mod app;
mod client;
mod ui;

use std::time::Duration;

use clap::Parser;
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt;
use ratatui::{Terminal, backend::CrosstermBackend};

use app::App;
use client::ApiClient;

#[derive(Parser)]
#[command(name = "gears-ctl", about = "TUI controller for the gears workflow engine")]
struct Args {
    /// Base URL of the gears management API
    #[arg(long, default_value = "http://localhost:3000")]
    url: String,

    /// Auto-refresh interval in seconds
    #[arg(long, default_value_t = 2)]
    interval: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Set up terminal.
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run(&args, &mut terminal).await;

    // Always restore terminal even on error.
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn run(
    args: &Args,
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
) -> anyhow::Result<()> {
    let client = ApiClient::new(&args.url);
    let mut app = App::new(client);
    app.refresh().await;

    let mut events = EventStream::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(args.interval));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        terminal.draw(|f| ui::render(&app, f))?;

        tokio::select! {
            // Auto-refresh tick.
            _ = ticker.tick() => {
                app.refresh().await;
            }

            // Keyboard / terminal events.
            maybe_event = events.next() => {
                let Some(Ok(event)) = maybe_event else { break };

                if let Event::Key(key) = event {
                    if key.kind != KeyEventKind::Press {
                        continue;
                    }
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => break,
                        KeyCode::Tab => app.tab = app.tab.next(),
                        KeyCode::Up => app.scroll_up(),
                        KeyCode::Down => app.scroll_down(),
                        KeyCode::Char('r') => app.refresh().await,
                        KeyCode::Char('c') => {
                            app.cancel_selected().await;
                            app.refresh().await;
                        }
                        KeyCode::Char('p') => {
                            app.pause_resume_selected().await;
                        }
                        KeyCode::Char('d') => {
                            app.delete_selected_schedule().await;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(())
}
