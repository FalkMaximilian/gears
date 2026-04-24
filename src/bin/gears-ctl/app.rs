use crate::client::{ApiClient, RunSummary, ScheduleInfo};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Runs,
    Schedules,
}

impl Tab {
    pub fn next(self) -> Self {
        match self {
            Tab::Runs => Tab::Schedules,
            Tab::Schedules => Tab::Runs,
        }
    }
}

pub struct App {
    pub tab: Tab,
    pub runs: Vec<RunSummary>,
    pub schedules: Vec<ScheduleInfo>,
    pub run_cursor: usize,
    pub schedule_cursor: usize,
    pub status_msg: String,
    pub connected: bool,
    client: ApiClient,
}

impl App {
    pub fn new(client: ApiClient) -> Self {
        Self {
            tab: Tab::Runs,
            runs: Vec::new(),
            schedules: Vec::new(),
            run_cursor: 0,
            schedule_cursor: 0,
            status_msg: "Connecting…".to_string(),
            connected: false,
            client,
        }
    }

    pub async fn refresh(&mut self) {
        let (runs_res, schedules_res) =
            tokio::join!(self.client.list_runs(None), self.client.list_schedules());

        match (runs_res, schedules_res) {
            (Ok(runs), Ok(schedules)) => {
                self.connected = true;
                self.run_cursor = self.run_cursor.min(runs.len().saturating_sub(1));
                self.schedule_cursor =
                    self.schedule_cursor.min(schedules.len().saturating_sub(1));
                self.runs = runs;
                self.schedules = schedules;
                self.status_msg = String::new();
            }
            (Err(e), _) | (_, Err(e)) => {
                self.connected = false;
                self.status_msg = format!("Error: {e}");
            }
        }
    }

    pub async fn cancel_selected(&mut self) {
        if let Some(run) = self.runs.get(self.run_cursor) {
            if run.status != "running" {
                self.status_msg = "Only running workflows can be cancelled.".to_string();
                return;
            }
            let id = run.run_id.to_string();
            match self.client.cancel_run(&id).await {
                Ok(()) => self.status_msg = format!("Cancellation requested for {}", &id[..8]),
                Err(e) => self.status_msg = format!("Cancel failed: {e}"),
            }
        }
    }

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
                Err(e) => self.status_msg = format!("Failed: {e}"),
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
                Err(e) => self.status_msg = format!("Delete failed: {e}"),
            }
            self.refresh().await;
        }
    }

    pub fn scroll_up(&mut self) {
        match self.tab {
            Tab::Runs => {
                if self.run_cursor > 0 {
                    self.run_cursor -= 1;
                }
            }
            Tab::Schedules => {
                if self.schedule_cursor > 0 {
                    self.schedule_cursor -= 1;
                }
            }
        }
    }

    pub fn scroll_down(&mut self) {
        match self.tab {
            Tab::Runs => {
                let max = self.runs.len().saturating_sub(1);
                if self.run_cursor < max {
                    self.run_cursor += 1;
                }
            }
            Tab::Schedules => {
                let max = self.schedules.len().saturating_sub(1);
                if self.schedule_cursor < max {
                    self.schedule_cursor += 1;
                }
            }
        }
    }
}
