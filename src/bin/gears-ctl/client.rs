use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
pub struct RunSummary {
    pub run_id: Uuid,
    pub workflow_name: String,
    pub status: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScheduleInfo {
    pub name: String,
    pub cron_expression: String,
    pub workflow_name: String,
    pub status: String,
    pub last_fired_at: Option<String>,
}

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
}
