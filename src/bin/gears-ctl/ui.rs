use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Cell, Clear, List, ListItem, ListState, Paragraph, Row, Table, TableState, Tabs, Wrap},
};

use crate::app::{App, Tab};

// ── Top-level render ──────────────────────────────────────────────────────

pub fn render(app: &App, frame: &mut Frame) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // tab bar
            Constraint::Min(0),    // content
            Constraint::Length(1), // footer
        ])
        .split(area);

    render_tab_bar(app, frame, chunks[0]);
    render_content(app, frame, chunks[1]);
    render_footer(app, frame, chunks[2]);

    if app.error_popup {
        render_error_popup(app, frame, area);
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn render_error_popup(app: &App, frame: &mut Frame, area: Rect) {
    let popup_area = centered_rect(70, 60, area);
    frame.render_widget(Clear, popup_area);

    let block = Block::default()
        .title(" Error  (↑↓ scroll · Esc dismiss) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Red));

    let para = Paragraph::new(app.error_text.as_str())
        .block(block)
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: false })
        .scroll((app.error_scroll, 0));

    frame.render_widget(para, popup_area);
}

fn render_tab_bar(app: &App, frame: &mut Frame, area: Rect) {
    let selected = match app.tab {
        Tab::Runs => 0,
        Tab::Schedules => 1,
        Tab::Registered => 2,
    };

    let runs_title = if let Some(sf) = app.status_filter {
        let filtered = app.filtered_runs().len();
        let total = app.runs.len();
        if !app.filter_input.is_empty() || app.status_filter.is_some() {
            format!("Runs ({filtered}/{total} · {sf})")
        } else {
            format!("Runs ({total})")
        }
    } else if !app.filter_input.is_empty() {
        let filtered = app.filtered_runs().len();
        let total = app.runs.len();
        format!("Runs ({filtered}/{total})")
    } else {
        let total = app.runs.len();
        format!("Runs ({total})")
    };

    let tab_titles = [
        runs_title.as_str(),
        "Schedules",
        "Registered",
    ];

    let tabs = Tabs::new(tab_titles)
        .block(Block::default().borders(Borders::ALL).title(" Gears Controller "))
        .select(selected)
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
    frame.render_widget(tabs, area);
}

fn render_content(app: &App, frame: &mut Frame, area: Rect) {
    match app.tab {
        Tab::Runs => {
            if app.detail_mode {
                render_run_detail(app, frame, area);
            } else {
                render_runs(app, frame, area);
            }
        }
        Tab::Schedules => render_schedules(app, frame, area),
        Tab::Registered => render_registered(app, frame, area),
    }
}

fn render_footer(app: &App, frame: &mut Frame, area: Rect) {
    let content = if app.input_mode {
        let selected = app
            .workflows_list
            .get(app.registered_cursor)
            .map(|wf| wf.name.as_str())
            .unwrap_or("?");
        Span::styled(
            format!(
                " Trigger '{selected}'  Input: {}▌   [Enter] Run  [Esc] Cancel",
                app.input_buffer
            ),
            Style::default().fg(Color::Cyan),
        )
    } else if app.filter_active {
        Span::styled(
            format!(
                " Filter: {}▌   [Esc] Done  [Backspace] Delete",
                app.filter_input
            ),
            Style::default().fg(Color::Cyan),
        )
    } else {
        let hints = build_hints(app);
        if app.status_msg.is_empty() {
            let refresh_suffix = refresh_indicator(app);
            Span::raw(format!("{hints}{refresh_suffix}"))
        } else {
            Span::styled(
                format!("{hints}  │  {}", app.status_msg),
                Style::default().fg(Color::Yellow),
            )
        }
    };

    let footer = Paragraph::new(Line::from(content)).style(Style::default().bg(Color::DarkGray));
    frame.render_widget(footer, area);
}

fn build_hints(app: &App) -> &'static str {
    if app.detail_mode {
        " ↑↓ Scroll  Esc Back  r Refresh  q Quit"
    } else {
        match app.tab {
            Tab::Runs => " ↑↓ Navigate  Enter Detail  c Cancel  / Filter  f Status  y Copy ID  P Prune  r Refresh  Tab Switch  q Quit",
            Tab::Schedules => " ↑↓ Navigate  p Pause/Resume  d Delete  r Refresh  Tab Switch  q Quit",
            Tab::Registered => " ↑↓ Navigate  n Run  r Refresh  Tab Switch  q Quit",
        }
    }
}

fn refresh_indicator(app: &App) -> String {
    if !app.connected {
        return String::new();
    }
    match &app.last_refreshed {
        None => String::new(),
        Some(t) => {
            let secs = t.elapsed().as_secs();
            if secs < 1 {
                "  ⟳ <1s ago".to_string()
            } else {
                format!("  ⟳ {secs}s ago")
            }
        }
    }
}

// ── Runs list ─────────────────────────────────────────────────────────────

fn render_runs(app: &App, frame: &mut Frame, area: Rect) {
    let filtered = app.filtered_runs();

    let header_cells = ["Status", "Workflow", "Run ID", "Duration", "Updated"]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD)));
    let header = Row::new(header_cells).height(1);

    let rows: Vec<Row> = filtered
        .iter()
        .map(|r| {
            let style = status_style(&r.status);
            let symbol = status_symbol(&r.status);
            let short_id = r.run_id.to_string()[..8].to_string();
            let dur = format_duration(&r.created_at, &r.updated_at, &r.status);
            Row::new([
                Cell::from(format!("{} {}", symbol, r.status)).style(style),
                Cell::from(r.workflow_name.as_str()),
                Cell::from(format!("{short_id}…")),
                Cell::from(dur),
                Cell::from(r.updated_at.get(..19).unwrap_or(&r.updated_at)),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(16),
        Constraint::Min(14),
        Constraint::Length(12),
        Constraint::Length(9),
        Constraint::Min(19),
    ];

    let mut state = TableState::default();
    if !filtered.is_empty() {
        state.select(Some(app.run_cursor));
    }

    let title = if app.connected {
        if filtered.len() != app.runs.len() {
            format!(" Runs ({}/{}) ", filtered.len(), app.runs.len())
        } else {
            format!(" Runs ({}) ", app.runs.len())
        }
    } else {
        " Runs (disconnected) ".to_string()
    };

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(table, area, &mut state);
}

// ── Run detail ────────────────────────────────────────────────────────────

pub fn render_run_detail(app: &App, frame: &mut Frame, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Min(0)])
        .split(area);

    // Info block at top.
    let run = app.runs.get(app.run_cursor);
    let info_text: Text = if let Some(r) = run {
        let dur = format_duration(&r.created_at, &r.updated_at, &r.status);
        let line1 = format!(
            " {} │ {} │ {} │ {} ",
            r.workflow_name,
            r.run_id,
            r.status.to_uppercase(),
            dur
        );
        let retention_line = format_retention_info(r.status.as_str(), &r.created_at, r.workflow_name.as_str(), &app.workflows_list);
        Text::from(vec![
            Line::from(line1),
            Line::from(vec![
                Span::raw(" "),
                Span::styled(retention_line, Style::default().fg(Color::DarkGray)),
            ]),
        ])
    } else {
        Text::default()
    };
    let info_para = Paragraph::new(info_text)
        .block(Block::default().borders(Borders::ALL).title(" Run Detail "))
        .style(Style::default().fg(Color::White));
    frame.render_widget(info_para, chunks[0]);

    // Event table.
    let header_cells = ["Seq", "Time", "Event", "Details"]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD)));
    let header = Row::new(header_cells).height(1);

    let rows: Vec<Row> = app
        .detail_events
        .iter()
        .map(|e| {
            let style = event_style(&e.event_type);
            Row::new([
                Cell::from(e.sequence.to_string()),
                Cell::from(e.occurred_at.as_str()),
                Cell::from(e.event_type.as_str()).style(style),
                Cell::from(e.detail.as_str()),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(5),
        Constraint::Length(10),
        Constraint::Length(14),
        Constraint::Min(20),
    ];

    let mut state = TableState::default();
    if !app.detail_events.is_empty() {
        state.select(Some(app.event_cursor));
    }

    let title = format!(" Events ({}) ", app.detail_events.len());
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(table, chunks[1], &mut state);
}

// ── Schedules ─────────────────────────────────────────────────────────────

fn render_schedules(app: &App, frame: &mut Frame, area: Rect) {
    let header_cells = ["Name", "Cron", "Workflow", "Status", "Last Fired"]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD)));
    let header = Row::new(header_cells).height(1);

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
                Cell::from(last_fired.get(..19).unwrap_or(last_fired)),
            ])
        })
        .collect();

    let widths = [
        Constraint::Min(16),
        Constraint::Length(20),
        Constraint::Min(14),
        Constraint::Length(8),
        Constraint::Min(19),
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

// ── Registered (workflows + activities) ──────────────────────────────────

fn render_registered(app: &App, frame: &mut Frame, area: Rect) {
    let halves = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(area);

    // Left: workflow list with retention label.
    let items: Vec<ListItem> = app
        .workflows_list
        .iter()
        .map(|wf| {
            let ret = match wf.retention_secs {
                Some(s) => format!("({})", format_retention_duration(s)),
                None => "(∞)".to_string(),
            };
            let line = Line::from(vec![
                Span::raw(wf.name.as_str()),
                Span::raw("  "),
                Span::styled(ret, Style::default().fg(Color::DarkGray)),
            ]);
            ListItem::new(line)
        })
        .collect();

    let mut list_state = ListState::default();
    if !app.workflows_list.is_empty() {
        list_state.select(Some(app.registered_cursor));
    }

    let wf_title = format!(" Workflows ({}) ", app.workflows_list.len());
    let wf_list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(wf_title))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .highlight_symbol("> ");

    frame.render_stateful_widget(wf_list, halves[0], &mut list_state);

    // Right: activity table.
    let header_cells = ["Name", "Attempts", "Base Delay", "Timeout"]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD)));
    let header = Row::new(header_cells).height(1);

    let rows: Vec<Row> = app
        .activities_list
        .iter()
        .map(|a| {
            let delay = format_ms(a.retry_base_delay_ms);
            let timeout = a
                .timeout_ms
                .map(format_ms)
                .unwrap_or_else(|| "none".to_string());
            Row::new([
                Cell::from(a.name.as_str()),
                Cell::from(a.max_attempts.to_string()),
                Cell::from(delay),
                Cell::from(timeout),
            ])
        })
        .collect();

    let widths = [
        Constraint::Min(16),
        Constraint::Length(9),
        Constraint::Length(11),
        Constraint::Length(10),
    ];

    let act_title = format!(" Activities ({}) ", app.activities_list.len());
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(act_title));

    frame.render_widget(table, halves[1]);
}

// ── Helpers ───────────────────────────────────────────────────────────────

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

fn event_style(event_type: &str) -> Style {
    if event_type.starts_with("✓") {
        Style::default().fg(Color::Green)
    } else if event_type.starts_with("✗") {
        Style::default().fg(Color::Red)
    } else if event_type.starts_with('!') {
        Style::default().fg(Color::Yellow)
    } else if event_type.starts_with('⏱') || event_type.starts_with('⏰') || event_type.starts_with('✓') {
        Style::default().fg(Color::Blue)
    } else if event_type.starts_with('⟲') {
        Style::default().fg(Color::Magenta)
    } else if event_type.starts_with('↩') {
        Style::default().fg(Color::Cyan)
    } else if event_type.starts_with('⊘') {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    }
}

fn format_duration(created_at: &str, updated_at: &str, status: &str) -> String {
    let parse = |s: &str| -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|t| t.with_timezone(&chrono::Utc))
    };

    let created = match parse(created_at) {
        Some(t) => t,
        None => return String::new(),
    };

    let end = if status == "running" {
        chrono::Utc::now()
    } else {
        match parse(updated_at) {
            Some(t) => t,
            None => return String::new(),
        }
    };

    let secs = (end - created).num_seconds().max(0) as u64;
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    }
}

fn format_retention_info(
    status: &str,
    created_at: &str,
    workflow_name: &str,
    workflows: &[crate::client::WorkflowInfo],
) -> String {
    if status == "running" {
        return String::new();
    }
    let retention_secs = workflows
        .iter()
        .find(|wf| wf.name == workflow_name)
        .and_then(|wf| wf.retention_secs);

    match retention_secs {
        None => "Kept forever (no retention policy)".to_string(),
        Some(secs) => {
            let parse = |s: &str| -> Option<chrono::DateTime<chrono::Utc>> {
                chrono::DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|t| t.with_timezone(&chrono::Utc))
            };
            if let Some(created) = parse(created_at) {
                let retention = chrono::Duration::seconds(secs as i64);
                let removal = created + retention;
                let now = chrono::Utc::now();
                let remaining = (removal - now).num_seconds().max(0) as u64;
                let time_left = if remaining < 3600 {
                    format!("{}m", remaining / 60)
                } else if remaining < 86_400 {
                    format!("{}h{}m", remaining / 3600, (remaining % 3600) / 60)
                } else {
                    format!("{}d{}h", remaining / 86_400, (remaining % 86_400) / 3600)
                };
                if removal > now {
                    format!(
                        "Removes: {} UTC  (in {})",
                        removal.format("%Y-%m-%d %H:%M"),
                        time_left
                    )
                } else {
                    format!(
                        "Removing soon (next pruning pass)  [was due {}]",
                        removal.format("%Y-%m-%d %H:%M UTC")
                    )
                }
            } else {
                String::new()
            }
        }
    }
}

fn format_retention_duration(secs: u64) -> String {
    if secs % 86_400 == 0 {
        format!("{}d", secs / 86_400)
    } else if secs % 3_600 == 0 {
        format!("{}h", secs / 3_600)
    } else if secs % 60 == 0 {
        format!("{}m", secs / 60)
    } else {
        format!("{}s", secs)
    }
}

fn format_ms(ms: u64) -> String {
    if ms < 1000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        format!("{}s", ms / 1000)
    } else {
        format!("{}m", ms / 60_000)
    }
}
