mod app;
mod client;
mod ui;

use std::time::Duration;

use clap::Parser;
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt;
use ratatui::{Terminal, backend::CrosstermBackend};

use app::{App, Tab};
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

    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run(&args, &mut terminal).await;

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
            _ = ticker.tick() => {
                app.refresh().await;
            }

            maybe_event = events.next() => {
                let Some(Ok(event)) = maybe_event else { break };

                if let Event::Key(key) = event {
                    if key.kind != KeyEventKind::Press {
                        continue;
                    }

                    // ── Error popup (highest priority — modal) ────────────
                    if app.error_popup {
                        match key.code {
                            KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') => {
                                app.dismiss_error()
                            }
                            KeyCode::Up => app.error_scroll_up(),
                            KeyCode::Down => app.error_scroll_down(),
                            _ => {}
                        }
                        continue;
                    }

                    // ── Text-input modes (filter / workflow trigger) ──────
                    if app.filter_active {
                        match key.code {
                            KeyCode::Esc => app.stop_filter(),
                            KeyCode::Backspace => app.pop_filter_char(),
                            KeyCode::Char(c) => app.push_filter_char(c),
                            _ => {}
                        }
                        continue;
                    }

                    if app.input_mode {
                        match key.code {
                            KeyCode::Esc => app.stop_input(),
                            KeyCode::Enter => app.submit_run().await,
                            KeyCode::Backspace => { app.input_buffer.pop(); }
                            KeyCode::Char(c) => app.input_buffer.push(c),
                            _ => {}
                        }
                        continue;
                    }

                    // ── Normal mode ───────────────────────────────────────
                    match key.code {
                        KeyCode::Char('q') => break,

                        KeyCode::Esc => {
                            if app.detail_mode {
                                app.close_detail();
                            }
                            // Esc in normal mode with no detail → no-op (q to quit)
                        }

                        KeyCode::Enter => {
                            if app.tab == Tab::Runs && !app.detail_mode {
                                app.open_detail().await;
                            }
                        }

                        KeyCode::Tab => app.tab = app.tab.next(),
                        KeyCode::BackTab => {
                            // Shift+Tab cycles backwards.
                            app.tab = match app.tab {
                                Tab::Runs => Tab::Registered,
                                Tab::Schedules => Tab::Runs,
                                Tab::Registered => Tab::Schedules,
                            };
                        }

                        KeyCode::Up => app.scroll_up(),
                        KeyCode::Down => app.scroll_down(),

                        KeyCode::Char('r') => app.refresh().await,

                        // Runs tab actions
                        KeyCode::Char('c') if app.tab == Tab::Runs => {
                            app.cancel_selected().await;
                            app.refresh().await;
                        }
                        KeyCode::Char('/') if app.tab == Tab::Runs && !app.detail_mode => {
                            app.start_filter();
                        }
                        KeyCode::Char('f') if app.tab == Tab::Runs && !app.detail_mode => {
                            app.cycle_status_filter().await;
                        }
                        KeyCode::Char('y') if app.tab == Tab::Runs && !app.detail_mode => {
                            app.copy_run_id().await;
                        }

                        // Schedules tab actions
                        KeyCode::Char('p') if app.tab == Tab::Schedules => {
                            app.pause_resume_selected().await;
                        }
                        KeyCode::Char('d') if app.tab == Tab::Schedules => {
                            app.delete_selected_schedule().await;
                        }

                        // Registered tab actions
                        KeyCode::Char('n') if app.tab == Tab::Registered => {
                            app.start_input();
                        }

                        // Ctrl+C as universal quit
                        KeyCode::Char('c')
                            if key.modifiers.contains(KeyModifiers::CONTROL) =>
                        {
                            break;
                        }

                        _ => {}
                    }
                }
            }
        }
    }

    Ok(())
}
