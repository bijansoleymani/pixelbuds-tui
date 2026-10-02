//! Owns the Bluetooth side: finds the Pixel Buds, opens the Maestro RFCOMM
//! channel, and turns what the buds report into `LinkEvent`s for the UI.
//! Setting writes come back the other way as `SettingValue`s.
//!
//! The Maestro session resets whenever the buds hand processing off between
//! each other (pbpctrl documents this as `os error 104`), so every failure
//! just waits briefly and reconnects.
//!
//! Parts of this file are adapted from pbpctrl (cli/src/bt.rs) and
//! omarchy-pixelbuds (daemon/pixelbudsd/src/maestro_link.rs), both MIT;
//! see NOTICE.

use std::time::Duration;

use anyhow::{Context, Result};
use bluer::rfcomm::{Profile, ProfileHandle, ReqError, Role, Stream};
use bluer::{Adapter, Address, Device, Session};
use futures::StreamExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use maestro::protocol::codec::Codec;
use maestro::protocol::types::{RuntimeInfo, SoftwareInfo, settings_rsp};
use maestro::protocol::utils;
use maestro::pwrpc::client::Client;
use maestro::service::MaestroService;
use maestro::service::settings::{SettingId, SettingValue};

#[derive(Debug)]
pub enum LinkEvent {
    /// Free-form state for the header while there is no session.
    Status(String),
    Device { name: String, address: Address },
    Connected,
    Disconnected,
    Runtime(RuntimeInfo),
    Setting(SettingValue),
    Firmware(SoftwareInfo),
    Error(String),
}

const SETTINGS_TO_SEED: [SettingId; 10] = [
    SettingId::CurrentAncrState,
    SettingId::MultipointEnable,
    SettingId::OhdEnable,
    SettingId::SpeechDetection,
    SettingId::VolumeExposureNotifications,
    SettingId::VolumeEqEnable,
    SettingId::SumToMono,
    SettingId::GestureEnable,
    SettingId::VolumeAsymmetry,
    SettingId::CurrentUserEq,
];

pub async fn run(
    wanted: Option<Address>,
    events: UnboundedSender<LinkEvent>,
    mut commands: UnboundedReceiver<SettingValue>,
) {
    let session = match Session::new().await {
        Ok(s) => s,
        Err(e) => {
            let _ = events.send(LinkEvent::Status(format!("BlueZ unavailable: {e}")));
            return;
        }
    };

    loop {
        let dev = match find_device(&session, wanted).await {
            Ok(Some(dev)) => dev,
            Ok(None) => {
                let _ = events.send(LinkEvent::Status(
                    "Waiting for Pixel Buds to connect…".into(),
                ));
                drain(&mut commands);
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
            Err(e) => {
                let _ = events.send(LinkEvent::Status(format!("Bluetooth error: {e}")));
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        let name = dev.alias().await.unwrap_or_else(|_| dev.address().to_string());
        let _ = events.send(LinkEvent::Device { name, address: dev.address() });
        let _ = events.send(LinkEvent::Status("Connecting…".into()));

        if let Err(e) = run_session(&session, &dev, &events, &mut commands).await {
            let _ = events.send(LinkEvent::Status(format!("Reconnecting ({e:#})")));
        }
        let _ = events.send(LinkEvent::Disconnected);
        drain(&mut commands);
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
}

/// Writes queued while disconnected would land on whatever state the buds
/// have by the time we reconnect, so drop them instead.
fn drain(commands: &mut UnboundedReceiver<SettingValue>) {
    while commands.try_recv().is_ok() {}
}

/// The requested device if it is connected, otherwise the first connected
/// device that offers the Maestro service. Only connected devices count:
/// opening the TUI should not pull the buds away from a phone.
async fn find_device(session: &Session, wanted: Option<Address>) -> Result<Option<Device>> {
    let adapter: Adapter = session.default_adapter().await?;

    let candidates = match wanted {
        Some(addr) => vec![addr],
        None => adapter.device_addresses().await?,
    };

    for addr in candidates {
        let dev = adapter.device(addr)?;
        if !dev.is_connected().await.unwrap_or(false) {
            continue;
        }
        let uuids = dev.uuids().await.ok().flatten().unwrap_or_default();
        if uuids.contains(&maestro::UUID) {
            return Ok(Some(dev));
        }
    }
    Ok(None)
}

async fn run_session(
    session: &Session,
    dev: &Device,
    events: &UnboundedSender<LinkEvent>,
    commands: &mut UnboundedReceiver<SettingValue>,
) -> Result<()> {
    let stream = connect_maestro_rfcomm(session, dev).await?;

    let mut client = Client::new(Codec::new().wrap(stream));
    let handle = client.handle();
    let channel = utils::resolve_channel(&mut client)
        .await
        .context("resolving Maestro channel")?;
    let service = MaestroService::new(handle, channel);

    // `client.run()` is what moves requests and replies over the socket, so
    // it has to be polled alongside every RPC below.
    let client_task = async move {
        client.run().await?;
        anyhow::bail!("connection closed")
    };

    tokio::select! {
        res = client_task => res,
        res = serve(service, events, commands) => res,
    }
}

async fn serve(
    mut service: MaestroService,
    events: &UnboundedSender<LinkEvent>,
    commands: &mut UnboundedReceiver<SettingValue>,
) -> Result<()> {
    let _ = events.send(LinkEvent::Connected);

    // Subscriptions only report changes, so read every setting once first.
    for id in SETTINGS_TO_SEED {
        if let Ok(value) = service.read_setting_var(id).await {
            let _ = events.send(LinkEvent::Setting(value));
        }
    }
    if let Ok(info) = service.get_software_info().await {
        let _ = events.send(LinkEvent::Firmware(info));
    }

    let runtime = {
        let mut service = service.clone();
        async move {
            let mut call = service.subscribe_to_runtime_info()?;
            while let Some(msg) = call.stream().next().await {
                let _ = events.send(LinkEvent::Runtime(msg?));
            }
            anyhow::bail!("runtime info stream ended")
        }
    };

    let settings = {
        let mut service = service.clone();
        async move {
            let mut call = service.subscribe_to_settings_changes()?;
            while let Some(msg) = call.stream().next().await {
                let Some(settings_rsp::ValueOneof::Value(raw)) = msg?.value_oneof else {
                    continue;
                };
                if let Some(value) = raw.value_oneof {
                    let _ = events.send(LinkEvent::Setting(value.into()));
                }
            }
            anyhow::bail!("settings stream ended")
        }
    };

    let writes = async move {
        while let Some(value) = commands.recv().await {
            if let Err(e) = service.write_setting(value).await {
                let _ = events.send(LinkEvent::Error(format!("write failed: {e}")));
            }
        }
        Ok(())
    };

    tokio::select! {
        res = runtime => res,
        res = settings => res,
        res = writes => res,
    }
}

async fn connect_maestro_rfcomm(session: &Session, dev: &Device) -> Result<Stream> {
    let profile = Profile {
        uuid: maestro::UUID,
        role: Some(Role::Client),
        require_authentication: Some(false),
        require_authorization: Some(false),
        auto_connect: Some(false),
        ..Default::default()
    };
    let mut handle = session.register_profile(profile).await?;

    let (_, stream) = tokio::try_join!(
        try_connect_profile(dev),
        accept_profile_request(&mut handle, dev.address()),
    )?;
    Ok(stream)
}

async fn try_connect_profile(dev: &Device) -> Result<()> {
    const MAX_TRIES: u32 = 3;
    let mut attempt = 0;
    loop {
        match dev.connect_profile(&maestro::UUID).await {
            Ok(()) => return Ok(()),
            Err(_) if attempt < MAX_TRIES => {
                attempt += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn accept_profile_request(handle: &mut ProfileHandle, address: Address) -> Result<Stream> {
    while let Some(req) = handle.next().await {
        if req.device() == address {
            return Ok(req.accept()?);
        }
        req.reject(ReqError::Rejected);
    }
    anyhow::bail!("profile closed without a connection request")
}
