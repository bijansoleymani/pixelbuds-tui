//! Terminal UI for Google Pixel Buds Pro / Pro 2.
//!
//! Talks to the buds over Google's Maestro protocol (the one the Pixel Buds
//! app uses), via the `maestro` crate from qzed/pbpctrl.

use std::time::Duration;

use anyhow::Result;
use bluer::Address;
use clap::Parser;
use crossterm::event::{Event, EventStream};
use futures::StreamExt;
use tokio::sync::mpsc;

use pixelbuds_tui::app::App;
use pixelbuds_tui::{handle_key, link, ui};

#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Bluetooth address of the buds. Without it, the first connected device
    /// offering the Maestro service is used.
    #[arg(short, long)]
    device: Option<Address>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let mut app = App::new(cmd_tx);

    // MaestroService is not Send, so the link runs on this thread rather than
    // in a spawned task.
    let link = link::run(args.device, event_tx, cmd_rx);
    tokio::pin!(link);

    let mut terminal = ratatui::init();
    let mut keys = EventStream::new();
    // Redraws expire flash messages.
    let mut tick = tokio::time::interval(Duration::from_secs(1));

    let result: Result<()> = async {
        while !app.quit {
            terminal.draw(|f| ui::draw(f, &app))?;
            tokio::select! {
                _ = &mut link => {}
                Some(ev) = event_rx.recv() => {
                    app.apply(ev);
                    while let Ok(ev) = event_rx.try_recv() {
                        app.apply(ev);
                    }
                }
                Some(ev) = keys.next() => {
                    if let Event::Key(key) = ev? {
                        handle_key(&mut app, key);
                    }
                }
                _ = tick.tick() => {}
            }
        }
        Ok(())
    }
    .await;

    ratatui::restore();
    result
}
