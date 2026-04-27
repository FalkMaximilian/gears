use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

// ── Response types ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct RunSummary {
    pub run_id: Uuid,
    pub workflow_name: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScheduleInfo {
    pub name: String,
    pub cron_expression: String,
    pub workflow_name: String,
    pub input: serde_json::Value,
    pub status: String,
    pub created_at: String,
    pub last_fired_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ActivityInfo {
    pub name: String,
    pub max_attempts: u32,
    pub retry_base_delay_ms: u64,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowInfo {
    pub name: String,
    pub retention_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EngineInfo {
    pub max_concurrent_workflows: usize,
    pub global_retention_days: Option<u32>,
    pub registered_workflows: usize,
    pub registered_activities: usize,
}

/// A single event in a run's history, processed for display.
#[derive(Debug, Clone)]
pub struct WorkflowEventRow {
    pub sequence: u64,
    pub occurred_at: String,
    pub event_type: String,
    pub detail: String,
}

// ── Raw deserialization ───────────────────────────────────────────────────

#[derive(Deserialize)]
struct RawEvent {
    sequence: u64,
    occurred_at: String,
    payload: Value,
}

fn parse_event(raw: RawEvent) -> WorkflowEventRow {
    let p = &raw.payload;
    let kind = p["type"].as_str().unwrap_or("unknown");

    let (event_type, detail) = match kind {
        "workflow_started" => (
            "Started".to_string(),
            p["workflow_name"].as_str().unwrap_or("").to_string(),
        ),
        "activity_scheduled" => (
            "→ Schedule".to_string(),
            format!(
                "{} [seq {}]",
                p["activity_name"].as_str().unwrap_or("?"),
                p["sequence_id"].as_u64().unwrap_or(0)
            ),
        ),
        "activity_completed" => (
            "✓ Activity".to_string(),
            truncate_json(&p["output"], 60),
        ),
        "activity_attempt_failed" => (
            "! Attempt".to_string(),
            format!(
                "#{}: {}",
                p["attempt"].as_u64().unwrap_or(0),
                p["error"].as_str().unwrap_or("?")
            ),
        ),
        "activity_errored" => (
            "✗ Errored".to_string(),
            p["error"].as_str().unwrap_or("?").to_string(),
        ),
        "activity_attempt_timed_out" => (
            "⏱ Timeout".to_string(),
            format!(
                "#{} ({}ms)",
                p["attempt"].as_u64().unwrap_or(0),
                p["timeout_ms"].as_u64().unwrap_or(0)
            ),
        ),
        "timer_started" => (
            "⏰ Timer".to_string(),
            format!(
                "wake {}",
                p["wake_at"].as_str().unwrap_or("?").get(..19).unwrap_or("")
            ),
        ),
        "timer_fired" => (
            "✓ Timer".to_string(),
            format!("seq {}", p["sequence_id"].as_u64().unwrap_or(0)),
        ),
        "version_marker" => (
            "V Marker".to_string(),
            format!(
                "{} = {}",
                p["change_id"].as_str().unwrap_or("?"),
                p["version"].as_u64().unwrap_or(0)
            ),
        ),
        "cleanup_registered" => (
            "↩ Cleanup".to_string(),
            p["activity_name"].as_str().unwrap_or("?").to_string(),
        ),
        "cleanup_completed" => (
            "✓ Cleanup".to_string(),
            format!("seq {}", p["sequence_id"].as_u64().unwrap_or(0)),
        ),
        "cleanup_failed" => (
            "✗ Cleanup".to_string(),
            p["error"].as_str().unwrap_or("?").to_string(),
        ),
        "workflow_cancelled" => (
            "⊘ Cancelled".to_string(),
            p["reason"].as_str().unwrap_or("").to_string(),
        ),
        "workflow_completed" => (
            "✓ Completed".to_string(),
            truncate_json(&p["output"], 60),
        ),
        "workflow_failed" => (
            "✗ Failed".to_string(),
            p["error"].as_str().unwrap_or("?").to_string(),
        ),
        "concurrent_branches_started" => (
            "⟲ Branches".to_string(),
            format!(
                "{} branches (budget {})",
                p["num_branches"].as_u64().unwrap_or(0),
                p["branch_budget"].as_u64().unwrap_or(0)
            ),
        ),
        other => (other.to_string(), String::new()),
    };

    // Format timestamp as HH:MM:SS (strip date and sub-second parts).
    let time = raw
        .occurred_at
        .get(11..19)
        .unwrap_or(&raw.occurred_at)
        .to_string();

    WorkflowEventRow {
        sequence: raw.sequence,
        occurred_at: time,
        event_type,
        detail,
    }
}

fn truncate_json(v: &Value, max: usize) -> String {
    let s = v.to_string();
    if s.len() <= max {
        s
    } else {
        format!("{}…", &s[..max])
    }
}

// ── API client ────────────────────────────────────────────────────────────

pub struct ApiClient {
    base_url: String,
    client: reqwest::Client,
}

impl ApiClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn list_runs(&self, status: Option<&str>) -> anyhow::Result<Vec<RunSummary>> {
        let mut url = format!("{}/api/runs", self.base_url);
        if let Some(s) = status {
            url = format!("{}?status={}", url, s);
        }
        Ok(self.client.get(&url).send().await?.json().await?)
    }

    pub async fn cancel_run(&self, run_id: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/runs/{}/cancel", self.base_url, run_id);
        self.client.post(&url).send().await?;
        Ok(())
    }

    pub async fn get_run_events(&self, run_id: &str) -> anyhow::Result<Vec<WorkflowEventRow>> {
        let url = format!("{}/api/runs/{}/events", self.base_url, run_id);
        let raw: Vec<RawEvent> = self.client.get(&url).send().await?.json().await?;
        Ok(raw.into_iter().map(parse_event).collect())
    }

    pub async fn start_run(
        &self,
        workflow_name: &str,
        input: serde_json::Value,
    ) -> anyhow::Result<Uuid> {
        let url = format!("{}/api/runs", self.base_url);
        let body = serde_json::json!({ "workflow_name": workflow_name, "input": input });
        let resp: serde_json::Value = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        let id = resp["run_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing run_id in response"))?;
        Ok(id.parse()?)
    }

    pub async fn list_schedules(&self) -> anyhow::Result<Vec<ScheduleInfo>> {
        let url = format!("{}/api/schedules", self.base_url);
        Ok(self.client.get(&url).send().await?.json().await?)
    }

    pub async fn delete_schedule(&self, name: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/schedules/{}", self.base_url, name);
        self.client.delete(&url).send().await?;
        Ok(())
    }

    pub async fn pause_schedule(&self, name: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/schedules/{}/pause", self.base_url, name);
        self.client.post(&url).send().await?;
        Ok(())
    }

    pub async fn resume_schedule(&self, name: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/schedules/{}/resume", self.base_url, name);
        self.client.post(&url).send().await?;
        Ok(())
    }

    pub async fn list_workflows(&self) -> anyhow::Result<Vec<WorkflowInfo>> {
        let url = format!("{}/api/workflows", self.base_url);
        Ok(self.client.get(&url).send().await?.json().await?)
    }

    pub async fn list_activities(&self) -> anyhow::Result<Vec<ActivityInfo>> {
        let url = format!("{}/api/activities", self.base_url);
        Ok(self.client.get(&url).send().await?.json().await?)
    }

    pub async fn trigger_prune(&self) -> anyhow::Result<()> {
        let url = format!("{}/api/runs/prune", self.base_url);
        self.client.post(&url).send().await?;
        Ok(())
    }

    pub async fn trigger_schedule(&self, name: &str) -> anyhow::Result<Uuid> {
        let url = format!("{}/api/schedules/{}/trigger", self.base_url, name);
        let resp: serde_json::Value = self.client.post(&url).send().await?.json().await?;
        let id = resp["run_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing run_id in response"))?;
        Ok(id.parse()?)
    }

    pub async fn get_engine_info(&self) -> anyhow::Result<EngineInfo> {
        let url = format!("{}/api/engine/info", self.base_url);
        Ok(self.client.get(&url).send().await?.json().await?)
    }
}
