//! Terminal UI for Google Pixel Buds Pro / Pro 2.
//!
//! Talks to the buds over Google's Maestro protocol (the one the Pixel Buds
//! app uses), via the `maestro` crate from qzed/pbpctrl.

mod app;
mod link;
mod ui;

use std::time::Duration;

use anyhow::Result;
use bluer::Address;
use clap::Parser;
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use futures::StreamExt;
use maestro::service::settings::AncState;
use tokio::sync::mpsc;

use app::App;

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
                Some(ev) = keys.next() => handle_event(&mut app, ev?),
                _ = tick.tick() => {}
            }
        }
        Ok(())
    }
    .await;

    ratatui::restore();
    result
}

fn handle_event(app: &mut App, event: Event) {
    let Event::Key(key) = event else { return };
    if key.kind != KeyEventKind::Press {
        return;
    }
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        app.quit = true;
        return;
    }
    if app.show_info {
        app.show_info = false;
        return;
    }
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => app.quit = true,
        KeyCode::Up | KeyCode::Char('k') => app.up(),
        KeyCode::Down | KeyCode::Char('j') | KeyCode::Tab => app.down(),
        KeyCode::Left | KeyCode::Char('h') => app.adjust(false),
        KeyCode::Right | KeyCode::Char('l') => app.adjust(true),
        KeyCode::Enter | KeyCode::Char(' ') => app.activate(),
        KeyCode::Char('r') => app.reset(),
        KeyCode::Char('i') => app.show_info = true,
        KeyCode::Char('1') => app.set_anc(AncState::Off),
        KeyCode::Char('2') => app.set_anc(AncState::Active),
        KeyCode::Char('3') => app.set_anc(AncState::Aware),
        KeyCode::Char('4') => app.set_anc(AncState::Adaptive),
        _ => {}
    }
}
