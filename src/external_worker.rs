/// Rust SDK for writing external workers that execute gears activities.
///
/// External workers poll the engine's task queue, execute activities in their
/// own process, and report results back over HTTP. Multiple workers can handle
/// the same activity type — the engine guarantees each task is delivered to
/// exactly one worker.
///
/// # Example
///
/// ```ignore
/// use gears::external_worker::{ExternalActivity, ExternalActivityContext, ExternalWorker};
/// use serde_json::Value;
/// use std::pin::Pin;
/// use std::future::Future;
///
/// struct SendEmail;
///
/// impl ExternalActivity for SendEmail {
///     fn name(&self) -> &'static str { "send-email" }
///
///     fn execute(
///         &self,
///         ctx: ExternalActivityContext,
///         input: Value,
///     ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'static>> {
///         Box::pin(async move {
///             // Periodically send heartbeats so the engine knows we are alive.
///             if ctx.heartbeat().await.unwrap_or(false) {
///                 return Err("run cancelled".into());
///             }
///             // ... do the actual work ...
///             Ok(serde_json::json!({"sent": true}))
///         })
///     }
/// }
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let worker = ExternalWorker::builder("http://localhost:3000/api", "my-worker-1")
///         .register_activity(SendEmail)
///         .build();
///     worker.run().await
/// }
/// ```
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use uuid::Uuid;

// ── Trait ─────────────────────────────────────────────────────────────────

/// Implement this trait to define an activity handled by an external worker.
pub trait ExternalActivity: Send + Sync + 'static {
    /// Stable name matching the one registered on the engine via
    /// [`WorkflowEngineBuilder::register_external_activity`].
    fn name(&self) -> &'static str;

    /// Execute the activity. Return `Ok(output)` on success or
    /// `Err(message)` on failure (the engine will retry if attempts remain).
    fn execute(
        &self,
        ctx: ExternalActivityContext,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'static>>;
}

// ── Context ───────────────────────────────────────────────────────────────

/// Passed to [`ExternalActivity::execute`]. Provides heartbeat support and
/// task metadata.
#[derive(Clone)]
pub struct ExternalActivityContext {
    pub task_token: Uuid,
    pub run_id: Uuid,
    pub attempt: u32,
    pub sequence_id: u32,
    api_base_url: String,
    client: reqwest::Client,
}

impl ExternalActivityContext {
    /// Send a heartbeat to the engine. Returns `true` if the workflow run
    /// has been cancelled and the worker should stop early.
    pub async fn heartbeat(&self) -> anyhow::Result<bool> {
        let url = format!(
            "{}/workers/tasks/{}/heartbeat",
            self.api_base_url, self.task_token
        );
        let resp: serde_json::Value = self
            .client
            .post(&url)
            .json(&serde_json::json!({}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(resp["run_cancelled"].as_bool().unwrap_or(false))
    }
}

// ── Builder ───────────────────────────────────────────────────────────────

pub struct ExternalWorkerBuilder {
    api_base_url: String,
    worker_id: String,
    activities: HashMap<String, Arc<dyn ExternalActivity>>,
    heartbeat_interval: Duration,
    poll_timeout_ms: u64,
}

impl ExternalWorkerBuilder {
    fn new(api_base_url: &str, worker_id: &str) -> Self {
        Self {
            api_base_url: api_base_url.trim_end_matches('/').to_string(),
            worker_id: worker_id.to_string(),
            activities: HashMap::new(),
            heartbeat_interval: Duration::from_secs(10),
            poll_timeout_ms: 30_000,
        }
    }

    pub fn register_activity(mut self, a: impl ExternalActivity) -> Self {
        self.activities.insert(a.name().to_string(), Arc::new(a));
        self
    }

    /// How often to send heartbeats while an activity is executing.
    /// Default: 10 seconds.
    pub fn heartbeat_interval(mut self, d: Duration) -> Self {
        self.heartbeat_interval = d;
        self
    }

    /// Maximum time to wait for a task when the queue is empty. Default: 30 000 ms.
    pub fn poll_timeout_ms(mut self, ms: u64) -> Self {
        self.poll_timeout_ms = ms;
        self
    }

    pub fn build(self) -> ExternalWorker {
        ExternalWorker {
            api_base_url: self.api_base_url,
            worker_id: self.worker_id,
            activities: self.activities,
            heartbeat_interval: self.heartbeat_interval,
            poll_timeout_ms: self.poll_timeout_ms,
            client: reqwest::Client::new(),
        }
    }
}

// ── ExternalWorker ────────────────────────────────────────────────────────

pub struct ExternalWorker {
    api_base_url: String,
    worker_id: String,
    activities: HashMap<String, Arc<dyn ExternalActivity>>,
    heartbeat_interval: Duration,
    poll_timeout_ms: u64,
    client: reqwest::Client,
}

impl ExternalWorker {
    pub fn builder(api_base_url: &str, worker_id: &str) -> ExternalWorkerBuilder {
        ExternalWorkerBuilder::new(api_base_url, worker_id)
    }

    /// Run the worker loop: poll → execute → report result, forever.
    ///
    /// Returns only on an unrecoverable error. Transient HTTP errors are
    /// logged and retried with a short backoff.
    pub async fn run(&self) -> anyhow::Result<()> {
        let activity_names: Vec<String> = self.activities.keys().cloned().collect();
        tracing::info!(
            worker_id = %self.worker_id,
            activities = ?activity_names,
            "external worker started"
        );

        loop {
            match self.poll(&activity_names).await {
                Ok(Some(assignment)) => {
                    self.handle_assignment(assignment).await;
                }
                Ok(None) => {
                    // Timeout with no task — re-poll immediately.
                }
                Err(e) => {
                    tracing::warn!(error = %e, "poll failed, retrying in 2s");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    async fn poll(
        &self,
        activity_names: &[String],
    ) -> anyhow::Result<Option<TaskAssignmentResp>> {
        let url = format!("{}/workers/poll", self.api_base_url);
        let body = serde_json::json!({
            "worker_id": self.worker_id,
            "activity_names": activity_names,
            "long_poll_timeout_ms": self.poll_timeout_ms,
        });
        let resp = self.client.post(&url).json(&body).send().await?;
        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(None);
        }
        resp.error_for_status_ref()?;
        let assignment: TaskAssignmentResp = resp.json().await?;
        Ok(Some(assignment))
    }

    async fn handle_assignment(&self, assignment: TaskAssignmentResp) {
        let token = assignment.task_token;
        let Some(activity) = self.activities.get(&assignment.activity_name) else {
            tracing::warn!(
                task_token = %token,
                activity = %assignment.activity_name,
                "received task for unregistered activity"
            );
            return;
        };

        let ctx = ExternalActivityContext {
            task_token: token,
            run_id: assignment.run_id,
            attempt: assignment.attempt,
            sequence_id: assignment.sequence_id,
            api_base_url: self.api_base_url.clone(),
            client: self.client.clone(),
        };

        let result = self
            .execute_with_heartbeat(activity.clone(), ctx, assignment.input)
            .await;

        match result {
            Ok(output) => {
                if let Err(e) = self.complete_task(token, output).await {
                    tracing::error!(task_token = %token, error = %e, "failed to report task completion");
                }
            }
            Err(error) => {
                if let Err(e) = self.fail_task(token, error).await {
                    tracing::error!(task_token = %token, error = %e, "failed to report task failure");
                }
            }
        }
    }

    async fn execute_with_heartbeat(
        &self,
        activity: Arc<dyn ExternalActivity>,
        ctx: ExternalActivityContext,
        input: Value,
    ) -> Result<Value, String> {
        let heartbeat_ctx = ctx.clone();
        let heartbeat_interval = self.heartbeat_interval;

        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.tick().await; // skip the immediate first tick
            loop {
                interval.tick().await;
                match heartbeat_ctx.heartbeat().await {
                    Ok(true) => {
                        tracing::info!(task_token = %heartbeat_ctx.task_token, "run cancelled");
                        return;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "heartbeat failed");
                    }
                }
            }
        });

        let result = activity.execute(ctx, input).await;
        heartbeat_handle.abort();
        result
    }

    async fn complete_task(&self, task_token: Uuid, output: Value) -> anyhow::Result<()> {
        let url = format!(
            "{}/workers/tasks/{}/complete",
            self.api_base_url, task_token
        );
        self.client
            .post(&url)
            .json(&serde_json::json!({"output": output}))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn fail_task(&self, task_token: Uuid, error: String) -> anyhow::Result<()> {
        let url = format!(
            "{}/workers/tasks/{}/fail",
            self.api_base_url, task_token
        );
        self.client
            .post(&url)
            .json(&serde_json::json!({"error": error}))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

// ── Internal response types ───────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct TaskAssignmentResp {
    task_token: Uuid,
    run_id: Uuid,
    activity_name: String,
    input: Value,
    attempt: u32,
    sequence_id: u32,
}
