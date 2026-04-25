use std::time::Instant;

use crate::client::{ActivityInfo, ApiClient, RunSummary, ScheduleInfo, WorkflowEventRow};

// ── Tab ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Runs,
    Schedules,
    Registered,
}

impl Tab {
    pub fn next(self) -> Self {
        match self {
            Tab::Runs => Tab::Schedules,
            Tab::Schedules => Tab::Registered,
            Tab::Registered => Tab::Runs,
        }
    }
}

// ── App ───────────────────────────────────────────────────────────────────

pub struct App {
    pub tab: Tab,

    // Runs tab
    pub runs: Vec<RunSummary>,
    pub run_cursor: usize,

    // Schedules tab
    pub schedules: Vec<ScheduleInfo>,
    pub schedule_cursor: usize,

    // Registered tab
    pub workflows_list: Vec<String>,
    pub activities_list: Vec<ActivityInfo>,
    pub registered_cursor: usize,

    // Run detail view (drill-down on Enter)
    pub detail_mode: bool,
    pub detail_run_id: Option<String>,
    pub detail_events: Vec<WorkflowEventRow>,
    pub event_cursor: usize,

    // Name filter (/ key on Runs tab)
    pub filter_active: bool,
    pub filter_input: String,

    // Status filter (f key) — also used when fetching
    pub status_filter: Option<&'static str>,

    // Workflow trigger input (n key on Registered tab)
    pub input_mode: bool,
    pub input_buffer: String,

    // Misc UI state
    pub status_msg: String,
    pub connected: bool,
    pub last_refreshed: Option<Instant>,

    // Error popup
    pub error_popup: bool,
    pub error_text: String,
    pub error_scroll: u16,

    client: ApiClient,
}

impl App {
    pub fn new(client: ApiClient) -> Self {
        Self {
            tab: Tab::Runs,
            runs: Vec::new(),
            run_cursor: 0,
            schedules: Vec::new(),
            schedule_cursor: 0,
            workflows_list: Vec::new(),
            activities_list: Vec::new(),
            registered_cursor: 0,
            detail_mode: false,
            detail_run_id: None,
            detail_events: Vec::new(),
            event_cursor: 0,
            filter_active: false,
            filter_input: String::new(),
            status_filter: None,
            input_mode: false,
            input_buffer: String::new(),
            status_msg: "Connecting…".to_string(),
            connected: false,
            last_refreshed: None,
            error_popup: false,
            error_text: String::new(),
            error_scroll: 0,
            client,
        }
    }

    // ── Refresh ───────────────────────────────────────────────────────────

    pub async fn refresh(&mut self) {
        let (runs_res, schedules_res, workflows_res, activities_res) = tokio::join!(
            self.client.list_runs(self.status_filter),
            self.client.list_schedules(),
            self.client.list_workflows(),
            self.client.list_activities(),
        );

        match (runs_res, schedules_res, workflows_res, activities_res) {
            (Ok(runs), Ok(schedules), Ok(workflows), Ok(activities)) => {
                self.connected = true;
                self.run_cursor = self.run_cursor.min(runs.len().saturating_sub(1));
                self.schedule_cursor =
                    self.schedule_cursor.min(schedules.len().saturating_sub(1));
                self.registered_cursor =
                    self.registered_cursor.min(workflows.len().saturating_sub(1));
                self.runs = runs;
                self.schedules = schedules;
                self.workflows_list = workflows;
                self.activities_list = activities;
                self.status_msg = String::new();
                self.last_refreshed = Some(Instant::now());
            }
            (Err(e), _, _, _) | (_, Err(e), _, _) | (_, _, Err(e), _) | (_, _, _, Err(e)) => {
                self.connected = false;
                self.show_error(format!("Error: {e}"));
            }
        }
    }

    // ── Run detail ────────────────────────────────────────────────────────

    pub async fn open_detail(&mut self) {
        let run_id = match self.filtered_runs().get(self.run_cursor) {
            Some(run) => run.run_id.to_string(),
            None => return,
        };
        match self.client.get_run_events(&run_id).await {
            Ok(events) => {
                self.detail_run_id = Some(run_id);
                self.detail_events = events;
                self.event_cursor = 0;
                self.detail_mode = true;
                self.status_msg = String::new();
            }
            Err(e) => {
                self.show_error(format!("Failed to load events: {e}"));
            }
        }
    }

    pub fn show_error(&mut self, msg: String) {
        self.error_text = msg;
        self.error_popup = true;
        self.error_scroll = 0;
        self.status_msg = String::new();
    }

    pub fn dismiss_error(&mut self) {
        self.error_popup = false;
        self.error_text.clear();
        self.error_scroll = 0;
    }

    pub fn error_scroll_up(&mut self) {
        self.error_scroll = self.error_scroll.saturating_sub(1);
    }

    pub fn error_scroll_down(&mut self) {
        self.error_scroll = self.error_scroll.saturating_add(1);
    }

    pub fn close_detail(&mut self) {
        self.detail_mode = false;
        self.detail_run_id = None;
        self.detail_events.clear();
        self.event_cursor = 0;
    }

    pub fn event_scroll_up(&mut self) {
        if self.event_cursor > 0 {
            self.event_cursor -= 1;
        }
    }

    pub fn event_scroll_down(&mut self) {
        let max = self.detail_events.len().saturating_sub(1);
        if self.event_cursor < max {
            self.event_cursor += 1;
        }
    }

    // ── Name filter (/) ───────────────────────────────────────────────────

    pub fn start_filter(&mut self) {
        self.filter_active = true;
    }

    pub fn stop_filter(&mut self) {
        self.filter_active = false;
        self.filter_input.clear();
    }

    pub fn push_filter_char(&mut self, c: char) {
        self.filter_input.push(c);
        self.run_cursor = 0;
    }

    pub fn pop_filter_char(&mut self) {
        self.filter_input.pop();
        self.run_cursor = 0;
    }

    /// Returns runs matching the active name filter (case-insensitive substring).
    /// This is a view over `self.runs`; callers that need indices must re-index.
    pub fn filtered_runs(&self) -> Vec<&RunSummary> {
        let needle = self.filter_input.to_lowercase();
        self.runs
            .iter()
            .filter(|r| r.workflow_name.to_lowercase().contains(&needle))
            .collect()
    }

    // ── Status filter (f) ─────────────────────────────────────────────────

    pub async fn cycle_status_filter(&mut self) {
        self.status_filter = match self.status_filter {
            None => Some("running"),
            Some("running") => Some("completed"),
            Some("completed") => Some("failed"),
            Some("failed") => Some("cancelled"),
            _ => None,
        };
        self.run_cursor = 0;
        self.filter_input.clear();
        self.refresh().await;
    }

    // ── Workflow trigger (n) ──────────────────────────────────────────────

    pub fn start_input(&mut self) {
        self.input_mode = true;
        self.input_buffer.clear();
    }

    pub fn stop_input(&mut self) {
        self.input_mode = false;
        self.input_buffer.clear();
    }

    pub async fn submit_run(&mut self) {
        let Some(workflow_name) = self.workflows_list.get(self.registered_cursor).cloned() else {
            self.status_msg = "No workflow selected.".to_string();
            self.stop_input();
            return;
        };

        let input: serde_json::Value = if self.input_buffer.trim().is_empty() {
            serde_json::json!({})
        } else {
            match serde_json::from_str(&self.input_buffer) {
                Ok(v) => v,
                Err(e) => {
                    self.status_msg = format!("Invalid JSON: {e}");
                    return;
                }
            }
        };

        match self.client.start_run(&workflow_name, input).await {
            Ok(run_id) => {
                self.status_msg = format!("Started run {}", &run_id.to_string()[..8]);
                self.stop_input();
                self.tab = Tab::Runs;
                self.refresh().await;
            }
            Err(e) => {
                self.show_error(format!("Failed to start run: {e}"));
            }
        }
    }

    // ── Clipboard (y) ─────────────────────────────────────────────────────

    pub async fn copy_run_id(&mut self) {
        let Some(run) = self.runs.get(self.run_cursor) else {
            return;
        };
        let id = run.run_id.to_string();

        #[cfg(target_os = "macos")]
        {
            use tokio::process::Command;
            let result = Command::new("sh")
                .arg("-c")
                .arg(format!("printf '%s' '{}' | pbcopy", id))
                .status()
                .await;
            match result {
                Ok(s) if s.success() => {
                    self.status_msg = format!("Copied {id}");
                }
                _ => {
                    self.status_msg = "Clipboard copy failed.".to_string();
                }
            }
        }
        #[cfg(not(target_os = "macos"))]
        {
            self.status_msg = "Clipboard not supported on this platform.".to_string();
        }
    }

    // ── Run actions ───────────────────────────────────────────────────────

    pub async fn cancel_selected(&mut self) {
        let filtered = self.filtered_runs();
        if let Some(run) = filtered.get(self.run_cursor) {
            if run.status != "running" {
                self.status_msg = "Only running workflows can be cancelled.".to_string();
                return;
            }
            let id = run.run_id.to_string();
            match self.client.cancel_run(&id).await {
                Ok(()) => self.status_msg = format!("Cancellation requested for {}", &id[..8]),
                Err(e) => self.show_error(format!("Cancel failed: {e}")),
            }
        }
    }

    // ── Schedule actions ──────────────────────────────────────────────────

    pub async fn pause_resume_selected(&mut self) {
        if let Some(schedule) = self.schedules.get(self.schedule_cursor) {
            let name = schedule.name.clone();
            let is_active = schedule.status == "active";
            let result = if is_active {
                self.client.pause_schedule(&name).await
            } else {
                self.client.resume_schedule(&name).await
            };
            match result {
                Ok(()) => {
                    let action = if is_active { "paused" } else { "resumed" };
                    self.status_msg = format!("Schedule '{name}' {action}.");
                }
                Err(e) => self.show_error(format!("Failed: {e}")),
            }
            self.refresh().await;
        }
    }

    pub async fn delete_selected_schedule(&mut self) {
        if let Some(schedule) = self.schedules.get(self.schedule_cursor) {
            let name = schedule.name.clone();
            match self.client.delete_schedule(&name).await {
                Ok(()) => {
                    self.status_msg = format!("Schedule '{name}' deleted.");
                    self.schedule_cursor =
                        self.schedule_cursor.min(self.schedules.len().saturating_sub(2));
                }
                Err(e) => self.show_error(format!("Delete failed: {e}")),
            }
            self.refresh().await;
        }
    }

    // ── Navigation ────────────────────────────────────────────────────────

    pub fn scroll_up(&mut self) {
        match self.tab {
            Tab::Runs => {
                if self.detail_mode {
                    self.event_scroll_up();
                } else if self.run_cursor > 0 {
                    self.run_cursor -= 1;
                }
            }
            Tab::Schedules => {
                if self.schedule_cursor > 0 {
                    self.schedule_cursor -= 1;
                }
            }
            Tab::Registered => {
                if self.registered_cursor > 0 {
                    self.registered_cursor -= 1;
                }
            }
        }
    }

    pub fn scroll_down(&mut self) {
        match self.tab {
            Tab::Runs => {
                if self.detail_mode {
                    self.event_scroll_down();
                } else {
                    let max = self.filtered_runs().len().saturating_sub(1);
                    if self.run_cursor < max {
                        self.run_cursor += 1;
                    }
                }
            }
            Tab::Schedules => {
                let max = self.schedules.len().saturating_sub(1);
                if self.schedule_cursor < max {
                    self.schedule_cursor += 1;
                }
            }
            Tab::Registered => {
                let max = self.workflows_list.len().saturating_sub(1);
                if self.registered_cursor < max {
                    self.registered_cursor += 1;
                }
            }
        }
    }
}
