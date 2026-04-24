use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Tabs},
};

use crate::app::{App, Tab};

pub fn render(app: &App, frame: &mut Frame) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // tabs header
            Constraint::Min(0),    // content
            Constraint::Length(1), // status / key hints
        ])
        .split(area);

    // ── Tab bar ───────────────────────────────────────────────────────────
    let tab_titles = ["Runs", "Schedules"];
    let selected = match app.tab {
        Tab::Runs => 0,
        Tab::Schedules => 1,
    };
    let tabs = Tabs::new(tab_titles)
        .block(Block::default().borders(Borders::ALL).title(" Gears Controller "))
        .select(selected)
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
    frame.render_widget(tabs, chunks[0]);

    // ── Content ───────────────────────────────────────────────────────────
    match app.tab {
        Tab::Runs => render_runs(app, frame, chunks[1]),
        Tab::Schedules => render_schedules(app, frame, chunks[1]),
    }

    // ── Footer ────────────────────────────────────────────────────────────
    let hints = match app.tab {
        Tab::Runs => " ↑↓ Navigate  c Cancel  r Refresh  Tab Switch  q Quit",
        Tab::Schedules => " ↑↓ Navigate  p Pause/Resume  d Delete  r Refresh  Tab Switch  q Quit",
    };
    let status = if app.status_msg.is_empty() {
        Span::raw(hints)
    } else {
        Span::styled(
            format!("{hints}  │  {}", app.status_msg),
            Style::default().fg(Color::Yellow),
        )
    };
    let footer = Paragraph::new(Line::from(status))
        .style(Style::default().bg(Color::DarkGray));
    frame.render_widget(footer, chunks[2]);
}

fn status_style(status: &str) -> Style {
    match status {
        "running" => Style::default().fg(Color::Yellow),
        "completed" => Style::default().fg(Color::Green),
        "failed" => Style::default().fg(Color::Red),
        "cancelled" => Style::default().fg(Color::DarkGray),
        _ => Style::default(),
    }
}

fn status_symbol(status: &str) -> &'static str {
    match status {
        "running" => "●",
        "completed" => "✓",
        "failed" => "✗",
        "cancelled" => "⊘",
        _ => "?",
    }
}

fn render_runs(app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let header_cells = ["Status", "Workflow", "Run ID", "Updated"]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD)));
    let header = Row::new(header_cells).height(1).bottom_margin(0);

    let rows: Vec<Row> = app
        .runs
        .iter()
        .map(|r| {
            let style = status_style(&r.status);
            let symbol = status_symbol(&r.status);
            let short_id = if r.run_id.to_string().len() >= 8 {
                r.run_id.to_string()[..8].to_string()
            } else {
                r.run_id.to_string()
            };
            Row::new([
                Cell::from(format!("{} {}", symbol, r.status)).style(style),
                Cell::from(r.workflow_name.as_str()),
                Cell::from(format!("{}…", short_id)),
                Cell::from(r.updated_at.as_str()),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(16),
        Constraint::Min(14),
        Constraint::Length(12),
        Constraint::Min(20),
    ];

    let mut state = TableState::default();
    if !app.runs.is_empty() {
        state.select(Some(app.run_cursor));
    }

    let title = if app.connected {
        format!(" Runs ({}) ", app.runs.len())
    } else {
        " Runs (disconnected) ".to_string()
    };

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(table, area, &mut state);
}

fn render_schedules(app: &App, frame: &mut Frame, area: ratatui::layout::Rect) {
    let header_cells = ["Name", "Cron", "Workflow", "Status", "Last Fired"]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD)));
    let header = Row::new(header_cells).height(1).bottom_margin(0);

    let rows: Vec<Row> = app
        .schedules
        .iter()
        .map(|s| {
            let status_style = match s.status.as_str() {
                "active" => Style::default().fg(Color::Green),
                "paused" => Style::default().fg(Color::Yellow),
                _ => Style::default(),
            };
            let last_fired = s.last_fired_at.as_deref().unwrap_or("never");
            Row::new([
                Cell::from(s.name.as_str()),
                Cell::from(s.cron_expression.as_str()),
                Cell::from(s.workflow_name.as_str()),
                Cell::from(s.status.as_str()).style(status_style),
                Cell::from(last_fired),
            ])
        })
        .collect();

    let widths = [
        Constraint::Min(16),
        Constraint::Length(20),
        Constraint::Min(14),
        Constraint::Length(8),
        Constraint::Min(20),
    ];

    let mut state = TableState::default();
    if !app.schedules.is_empty() {
        state.select(Some(app.schedule_cursor));
    }

    let title = format!(" Schedules ({}) ", app.schedules.len());
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(table, area, &mut state);
}
