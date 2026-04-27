use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot};
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

use crate::context::WorkflowContext;
use crate::error::{Result, GearsError};
use crate::metrics;
use crate::event::WorkflowEvent;
use crate::traits::{
    Activity, PendingTask, RunFilter, RunInfo, RunStatus, ScheduleRecord, ScheduleStatus,
    Storage, TaskResult, Workflow,
};
use crate::worker::{CleanupPolicy, WorkerTask};

// ── Public metadata types ─────────────────────────────────────────────────

/// Metadata about a registered activity, returned by [`WorkflowEngine::activity_info_list`].
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct ActivityInfo {
    pub name: String,
    pub max_attempts: u32,
    pub retry_base_delay_ms: u64,
    pub timeout_ms: Option<u64>,
}

/// Metadata about a registered workflow, returned by [`WorkflowEngine::workflow_info_list`].
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct WorkflowInfo {
    pub name: String,
    /// Effective retention in seconds. `None` means runs are kept forever.
    pub retention_secs: Option<u64>,
}

/// Runtime configuration snapshot returned by [`WorkflowEngine::engine_info`].
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct EngineInfo {
    /// Maximum number of concurrently executing workflow runs.
    pub max_concurrent_workflows: usize,
    /// Global retention in days. `None` means runs are kept forever (unless overridden per workflow).
    pub global_retention_days: Option<u32>,
    /// Number of registered workflow types.
    pub registered_workflows: usize,
    /// Number of registered activity types.
    pub registered_activities: usize,
}

/// Controls automatic deletion of terminal workflow runs.
///
/// Configure on the engine builder with [`WorkflowEngineBuilder::retention_days`].
/// Per-workflow overrides are set via [`Workflow::retention`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RetentionPolicy {
    /// Delete terminal runs of any workflow type older than this many days.
    /// `None` means keep forever (the default).
    pub global_days: Option<u32>,
}

impl RetentionPolicy {
    /// Compute the effective retention for a specific workflow type.
    ///
    /// - `Some(d)` from per-workflow `Workflow::retention()` always wins.
    /// - `None` from per-workflow falls back to `global_days`.
    /// - Both `None` → no pruning.
    pub fn effective_for(&self, wf_retention: Option<Duration>) -> Option<Duration> {
        match wf_retention {
            Some(d) => Some(d),
            None => self.global_days.map(|d| Duration::from_secs(d as u64 * 86_400)),
        }
    }
}

// ── External worker support ───────────────────────────────────────────────

/// Configuration for an activity type handled by external workers.
///
/// Register via [`WorkflowEngineBuilder::register_external_activity`].
#[derive(Debug, Clone)]
pub struct ExternalActivityConfig {
    /// Workers must send a heartbeat within this window or the task is
    /// reset to `pending` and re-queued for another worker.
    pub heartbeat_timeout: Duration,
    /// If set, a task that is not claimed within this window is treated
    /// as a failed attempt.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Maximum number of attempts before the workflow receives an error.
    /// Defaults to 3.
    pub max_attempts: u32,
    /// Base retry delay (doubles on each attempt). Defaults to 1 second.
    pub retry_base_delay: Duration,
}

impl Default for ExternalActivityConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(30),
            schedule_to_start_timeout: None,
            max_attempts: 3,
            retry_base_delay: Duration::from_secs(1),
        }
    }
}

/// In-memory notification layer for the external task queue.
///
/// A single shared [`tokio::sync::Notify`] wakes all long-polling workers
/// whenever any new pending task is created. Workers filter by activity name
/// on the actual claim attempt (second pass after the Notify fires).
pub struct ExternalTaskQueue {
    pub notify: Arc<tokio::sync::Notify>,
}

impl ExternalTaskQueue {
    fn new() -> Self {
        Self {
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

// ── Internal message types ────────────────────────────────────────────────

struct StartRequest {
    run_id: Uuid,
    workflow_name: String,
    input: Value,
}

/// Shared map of active workflow contexts, used for cancellation.
type CancelHandles = Arc<Mutex<HashMap<Uuid, WorkflowContext>>>;

// ── WorkflowEngineBuilder ─────────────────────────────────────────────────

pub struct WorkflowEngineBuilder {
    storage: Option<Arc<dyn Storage>>,
    workflows: HashMap<String, Arc<dyn Workflow>>,
    activities: HashMap<String, Arc<dyn Activity>>,
    external_activities: HashMap<String, ExternalActivityConfig>,
    max_concurrent: usize,
    cleanup_policy: CleanupPolicy,
    retention_policy: RetentionPolicy,
}

impl Default for WorkflowEngineBuilder {
    fn default() -> Self {
        WorkflowEngineBuilder {
            storage: None,
            workflows: HashMap::new(),
            activities: HashMap::new(),
            external_activities: HashMap::new(),
            max_concurrent: 100,
            cleanup_policy: CleanupPolicy::default(),
            retention_policy: RetentionPolicy::default(),
        }
    }
}

impl WorkflowEngineBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_storage(mut self, s: impl Storage) -> Self {
        self.storage = Some(Arc::new(s));
        self
    }

    pub fn register_workflow(mut self, w: impl Workflow) -> Self {
        self.workflows.insert(w.name().to_string(), Arc::new(w));
        self
    }

    pub fn register_activity(mut self, a: impl Activity) -> Self {
        self.activities.insert(a.name().to_string(), Arc::new(a));
        self
    }

    /// Register an activity type whose execution is delegated to external
    /// workers that poll the engine via the `/api/workers/poll` endpoint.
    pub fn register_external_activity(
        mut self,
        name: &str,
        config: ExternalActivityConfig,
    ) -> Self {
        self.external_activities.insert(name.to_string(), config);
        self
    }

    pub fn max_concurrent_workflows(mut self, n: usize) -> Self {
        self.max_concurrent = n;
        self
    }

    /// Set the cleanup policy for all workflows run by this engine.
    ///
    /// - [`CleanupPolicy::OnSuccessOrCancelled`] (default): cleanups run after
    ///   `Ok` or `Err(Cancelled)` only.
    /// - [`CleanupPolicy::Always`]: cleanups also run after `Err(other)`.
    pub fn cleanup_policy(mut self, policy: CleanupPolicy) -> Self {
        self.cleanup_policy = policy;
        self
    }

    /// Delete terminal runs (completed / failed / cancelled) older than `days`
    /// days. Individual workflows may override this via [`Workflow::retention`].
    /// If neither this nor a per-workflow retention is set, runs are kept forever.
    pub fn retention_days(mut self, days: u32) -> Self {
        self.retention_policy.global_days = Some(days);
        self
    }

    pub async fn build(self) -> Result<WorkflowEngine> {
        let storage = self
            .storage
            .ok_or_else(|| GearsError::Other("storage not configured".into()))?;

        let (start_tx, start_rx) = mpsc::channel::<StartRequest>(1024);

        let job_scheduler =
            Arc::new(Mutex::new(JobScheduler::new().await.map_err(|e| {
                GearsError::Other(format!("scheduler init failed: {e}"))
            })?));

        let task_queue = Arc::new(ExternalTaskQueue::new());
        let pending_completions = Arc::new(Mutex::new(HashMap::new()));

        Ok(WorkflowEngine {
            storage,
            workflows: Arc::new(self.workflows),
            activities: Arc::new(self.activities),
            external_activity_configs: Arc::new(self.external_activities),
            start_tx,
            start_rx: Some(start_rx),
            max_concurrent: self.max_concurrent,
            cancel_handles: Arc::new(Mutex::new(HashMap::new())),
            job_scheduler,
            job_handles: Arc::new(Mutex::new(HashMap::new())),
            cleanup_policy: self.cleanup_policy,
            retention_policy: self.retention_policy,
            task_queue,
            pending_completions,
        })
    }
}

// ── WorkflowEngine ────────────────────────────────────────────────────────

pub struct WorkflowEngine {
    storage: Arc<dyn Storage>,
    workflows: Arc<HashMap<String, Arc<dyn Workflow>>>,
    activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    external_activity_configs: Arc<HashMap<String, ExternalActivityConfig>>,
    start_tx: mpsc::Sender<StartRequest>,
    /// Held until `run()` is called and moves it into the dispatch task.
    start_rx: Option<mpsc::Receiver<StartRequest>>,
    max_concurrent: usize,
    cancel_handles: CancelHandles,
    job_scheduler: Arc<Mutex<JobScheduler>>,
    /// Maps schedule name → scheduler job UUID (for pause/remove).
    job_handles: Arc<Mutex<HashMap<String, Uuid>>>,
    cleanup_policy: CleanupPolicy,
    retention_policy: RetentionPolicy,
    pub(crate) task_queue: Arc<ExternalTaskQueue>,
    /// task_token → oneshot sender to unblock a waiting workflow context.
    pub(crate) pending_completions:
        Arc<Mutex<HashMap<Uuid, tokio::sync::oneshot::Sender<TaskResult>>>>,
}

impl WorkflowEngine {
    pub fn builder() -> WorkflowEngineBuilder {
        WorkflowEngineBuilder::new()
    }

    /// Start the engine: recover in-flight workflows, then start the
    /// dispatch loop.  Returns an `EngineHandle` for clean shutdown.
    pub async fn run(&mut self) -> Result<EngineHandle> {
        // Recover any runs that were `running` when the process last died.
        let running: Vec<crate::traits::RunRecord> = self.storage.list_running_workflows().await?;
        if !running.is_empty() {
            tracing::info!("recovering {} in-flight workflow(s)", running.len());
        }
        for record in &running {
            // Reset any claimed external tasks — the worker that held them is
            // gone. Pending tasks are fine as-is; workers will re-poll them.
            let tasks = self.storage.list_pending_tasks_by_run(record.run_id).await?;
            for task in tasks {
                if task.status == "claimed" {
                    let _ = self.storage.reset_pending_task(task.task_token).await;
                }
            }
        }
        // Wake any workers that may already be long-polling.
        if !running.is_empty() && !self.external_activity_configs.is_empty() {
            self.task_queue.notify.notify_waiters();
        }
        for record in running {
            self.start_tx
                .send(StartRequest {
                    run_id: record.run_id,
                    workflow_name: record.workflow_name,
                    input: record.input,
                })
                .await
                .map_err(|_| GearsError::EngineNotRunning)?;
        }

        let start_rx = self
            .start_rx
            .take()
            .ok_or_else(|| GearsError::Other("WorkflowEngine::run() called twice".into()))?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let storage = self.storage.clone();
        let workflows = self.workflows.clone();
        let activities = self.activities.clone();
        let external_activity_configs = self.external_activity_configs.clone();
        let semaphore = Arc::new(Semaphore::new(self.max_concurrent));
        let cancel_handles = self.cancel_handles.clone();
        let cleanup_policy = self.cleanup_policy;
        let task_queue = self.task_queue.clone();
        let pending_completions = self.pending_completions.clone();

        tokio::spawn(dispatch_loop(
            start_rx,
            shutdown_rx,
            storage,
            workflows,
            activities,
            external_activity_configs,
            semaphore,
            cancel_handles,
            cleanup_policy,
            task_queue,
            pending_completions,
        ));

        // Re-register all active schedules persisted from previous runs.
        let all_schedules = self.storage.list_schedules().await?;
        {
            let mut handles = self.job_handles.lock().await;
            for schedule in all_schedules {
                if schedule.status != ScheduleStatus::Active {
                    continue;
                }
                // Skip schedules already registered (e.g. via schedule_workflow
                // called before run()).
                if handles.contains_key(&schedule.name) {
                    continue;
                }
                let job =
                    build_scheduler_job(&schedule, self.start_tx.clone(), self.storage.clone())?;
                let job_uuid = self
                    .job_scheduler
                    .lock()
                    .await
                    .add(job)
                    .await
                    .map_err(|e| GearsError::Other(format!("failed to add job: {e}")))?;
                handles.insert(schedule.name.clone(), job_uuid);
                tracing::info!(schedule = %schedule.name, "schedule recovered");
            }
        }

        self.job_scheduler
            .lock()
            .await
            .start()
            .await
            .map_err(|e| GearsError::Other(format!("scheduler start failed: {e}")))?;

        let pruner_shutdown_tx = if self.retention_policy.global_days.is_some()
            || self.workflows.values().any(|w| w.retention().is_some())
        {
            let (pruner_tx, pruner_rx) = oneshot::channel::<()>();
            tokio::spawn(pruning_loop(
                pruner_rx,
                self.storage.clone(),
                self.workflows.clone(),
                self.retention_policy,
            ));
            tracing::info!("retention pruning task started");
            Some(pruner_tx)
        } else {
            None
        };

        let stale_monitor_shutdown_tx = if !self.external_activity_configs.is_empty() {
            let (stale_tx, stale_rx) = oneshot::channel::<()>();
            tokio::spawn(stale_task_monitor(
                stale_rx,
                self.storage.clone(),
                self.task_queue.clone(),
                self.external_activity_configs.clone(),
            ));
            tracing::info!("external task stale monitor started");
            Some(stale_tx)
        } else {
            None
        };

        Ok(EngineHandle {
            shutdown_tx,
            pruner_shutdown_tx,
            stale_monitor_shutdown_tx,
            job_scheduler: self.job_scheduler.clone(),
        })
    }

    /// Enqueue a new workflow run. Returns the run ID immediately;
    /// execution is asynchronous.
    pub async fn start_workflow(&self, workflow_name: &str, input: Value) -> Result<Uuid> {
        if !self.workflows.contains_key(workflow_name) {
            return Err(GearsError::WorkflowNotFound(workflow_name.to_string()));
        }

        let run_id = Uuid::new_v4();
        self.storage
            .create_run(run_id, workflow_name, &input)
            .await?;

        self.start_tx
            .send(StartRequest {
                run_id,
                workflow_name: workflow_name.to_string(),
                input,
            })
            .await
            .map_err(|_| GearsError::EngineNotRunning)?;

        tracing::info!(run_id = %run_id, workflow = workflow_name, "workflow enqueued");
        metrics::inc_workflow_started(workflow_name);
        Ok(run_id)
    }

    /// Poll the current status of a run.
    pub async fn get_run_status(&self, run_id: Uuid) -> Result<RunStatus> {
        self.storage.get_run_status(run_id).await
    }

    /// List runs matching the given filter criteria.
    pub async fn list_runs(&self, filter: &RunFilter) -> Result<Vec<RunInfo>> {
        self.storage.list_runs(filter).await
    }

    /// Cancel a running workflow. The workflow's context methods will
    /// return `GearsError::Cancelled` at the next yield point.
    pub async fn cancel_workflow(&self, run_id: Uuid) -> Result<()> {
        let handles = self.cancel_handles.lock().await;
        if let Some(ctx) = handles.get(&run_id) {
            ctx.cancel();
            tracing::info!(run_id = %run_id, "workflow cancellation requested");
            Ok(())
        } else {
            Err(GearsError::RunNotFound(run_id))
        }
    }

    // ── Typed convenience methods ────────────────────────────────────────

    /// Start a workflow with a typed input. The input is serialized to JSON.
    pub async fn start_workflow_typed<I: Serialize>(
        &self,
        workflow_name: &str,
        input: &I,
    ) -> Result<Uuid> {
        let value = serde_json::to_value(input)?;
        self.start_workflow(workflow_name, value).await
    }

    /// Retrieve the raw JSON result of a completed workflow run.
    /// Returns `Ok(None)` if the run has not completed or has no result.
    pub async fn get_run_result(&self, run_id: Uuid) -> Result<Option<Value>> {
        self.storage.get_run_result(run_id).await
    }

    /// Retrieve the result of a completed workflow run, deserialized into `T`.
    /// Returns `Ok(None)` if the run has not completed or has no result.
    pub async fn get_run_result_typed<T: DeserializeOwned>(
        &self,
        run_id: Uuid,
    ) -> Result<Option<T>> {
        match self.storage.get_run_result(run_id).await? {
            Some(value) => Ok(Some(serde_json::from_value(value)?)),
            None => Ok(None),
        }
    }

    /// Retrieve the full event log for a run, ordered by sequence.
    /// Returns `GearsError::RunNotFound` if the run does not exist.
    pub async fn get_run_events(&self, run_id: Uuid) -> Result<Vec<WorkflowEvent>> {
        let filter = RunFilter { ..Default::default() };
        let runs = self.storage.list_runs(&filter).await?;
        if runs.iter().all(|r| r.run_id != run_id) {
            return Err(GearsError::RunNotFound(run_id));
        }
        self.storage.load_events(run_id).await
    }

    // ── Scheduled workflows ───────────────────────────────────────────────

    /// Register (or update) a named cron schedule. `cron_expression` must be
    /// a standard 6-field expression: `sec min hour dom month dow`.
    /// Example: `"0 0 9 * * Mon-Fri"` — weekdays at 09:00.
    ///
    /// If a schedule with this name already exists it is replaced (the old
    /// scheduler job is removed before the new one is added).
    pub async fn schedule_workflow(
        &self,
        name: &str,
        cron_expression: &str,
        workflow_name: &str,
        input: Value,
    ) -> Result<()> {
        if !self.workflows.contains_key(workflow_name) {
            return Err(GearsError::WorkflowNotFound(workflow_name.to_string()));
        }

        // Remove any existing job for this schedule name.
        if let Some(old_uuid) = self.job_handles.lock().await.remove(name) {
            let _ = self.job_scheduler.lock().await.remove(&old_uuid).await;
        }

        let record = ScheduleRecord {
            name: name.to_string(),
            cron_expression: cron_expression.to_string(),
            workflow_name: workflow_name.to_string(),
            input: input.clone(),
            status: ScheduleStatus::Active,
            created_at: Utc::now(),
            last_fired_at: None,
        };

        let job = build_scheduler_job(&record, self.start_tx.clone(), self.storage.clone())?;
        let job_uuid = self
            .job_scheduler
            .lock()
            .await
            .add(job)
            .await
            .map_err(|e| GearsError::Other(format!("failed to add scheduled job: {e}")))?;

        self.job_handles
            .lock()
            .await
            .insert(name.to_string(), job_uuid);
        self.storage.upsert_schedule(&record).await?;

        tracing::info!(
            schedule = name,
            workflow = workflow_name,
            "schedule registered"
        );
        Ok(())
    }

    /// List all registered schedules.
    pub async fn list_schedules(&self) -> Result<Vec<ScheduleRecord>> {
        self.storage.list_schedules().await
    }

    /// Names of all registered workflows.
    pub fn workflow_names(&self) -> Vec<String> {
        self.workflows.keys().cloned().collect()
    }

    /// Names of all registered activities.
    pub fn activity_names(&self) -> Vec<String> {
        self.activities.keys().cloned().collect()
    }

    /// Metadata for all registered activities (name, retry config, timeout).
    pub fn activity_info_list(&self) -> Vec<ActivityInfo> {
        let mut list: Vec<ActivityInfo> = self
            .activities
            .values()
            .map(|a| ActivityInfo {
                name: a.name().to_string(),
                max_attempts: a.max_attempts(),
                retry_base_delay_ms: a.retry_base_delay().as_millis() as u64,
                timeout_ms: a.timeout().map(|d| d.as_millis() as u64),
            })
            .collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        list
    }

    /// Metadata for all registered workflows (name, effective retention).
    pub fn workflow_info_list(&self) -> Vec<WorkflowInfo> {
        let mut list: Vec<WorkflowInfo> = self
            .workflows
            .values()
            .map(|wf| {
                let effective = self.retention_policy.effective_for(wf.retention());
                let retention_secs = effective.map(|d| d.as_secs());
                WorkflowInfo { name: wf.name().to_string(), retention_secs }
            })
            .collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        list
    }

    /// Permanently remove a schedule. In-flight runs are not affected.
    pub async fn delete_schedule(&self, name: &str) -> Result<()> {
        if let Some(uuid) = self.job_handles.lock().await.remove(name) {
            let _ = self.job_scheduler.lock().await.remove(&uuid).await;
        }
        self.storage.delete_schedule(name).await?;
        tracing::info!(schedule = name, "schedule deleted");
        Ok(())
    }

    /// Suspend a schedule without deleting it. The next fire is skipped;
    /// call `resume_schedule` to re-enable from the next future cron slot.
    pub async fn pause_schedule(&self, name: &str) -> Result<()> {
        let uuid = self
            .job_handles
            .lock()
            .await
            .remove(name)
            .ok_or_else(|| GearsError::ScheduleNotFound(name.to_string()))?;
        let _ = self.job_scheduler.lock().await.remove(&uuid).await;
        self.storage
            .set_schedule_status(name, ScheduleStatus::Paused)
            .await?;
        tracing::info!(schedule = name, "schedule paused");
        Ok(())
    }

    /// Resume a paused schedule. The next tick after the next future cron
    /// slot will fire the workflow. Missed fires while paused are not
    /// replayed.
    pub async fn resume_schedule(&self, name: &str) -> Result<()> {
        // Idempotent: if already running, do nothing.
        if self.job_handles.lock().await.contains_key(name) {
            return Ok(());
        }

        let record = self
            .storage
            .get_schedule(name)
            .await?
            .ok_or_else(|| GearsError::ScheduleNotFound(name.to_string()))?;

        let job = build_scheduler_job(&record, self.start_tx.clone(), self.storage.clone())?;
        let job_uuid = self
            .job_scheduler
            .lock()
            .await
            .add(job)
            .await
            .map_err(|e| GearsError::Other(format!("failed to add scheduled job: {e}")))?;

        self.job_handles
            .lock()
            .await
            .insert(name.to_string(), job_uuid);
        self.storage
            .set_schedule_status(name, ScheduleStatus::Active)
            .await?;
        tracing::info!(schedule = name, "schedule resumed");
        Ok(())
    }

    /// Returns a snapshot of the engine's current runtime configuration.
    pub fn engine_info(&self) -> EngineInfo {
        EngineInfo {
            max_concurrent_workflows: self.max_concurrent,
            global_retention_days: self.retention_policy.global_days,
            registered_workflows: self.workflows.len(),
            registered_activities: self.activities.len(),
        }
    }

    /// Fetch a single schedule by name.
    /// Returns `GearsError::ScheduleNotFound` if it does not exist.
    pub async fn get_schedule(&self, name: &str) -> Result<ScheduleRecord> {
        self.storage
            .get_schedule(name)
            .await?
            .ok_or_else(|| GearsError::ScheduleNotFound(name.to_string()))
    }

    /// Fire a schedule immediately without waiting for its next cron tick.
    /// Starts a workflow run with the schedule's configured workflow name and input.
    /// Returns the new run ID. Does not update `last_fired_at`. Works on paused schedules.
    pub async fn trigger_schedule(&self, name: &str) -> Result<Uuid> {
        let record = self
            .storage
            .get_schedule(name)
            .await?
            .ok_or_else(|| GearsError::ScheduleNotFound(name.to_string()))?;
        let run_id = self.start_workflow(&record.workflow_name, record.input).await?;
        tracing::info!(schedule = name, %run_id, "schedule triggered manually");
        Ok(run_id)
    }

    /// Trigger a pruning pass immediately. Intended for testing.
    #[doc(hidden)]
    pub async fn prune_now(&self) {
        run_pruning_pass(&self.storage, &self.workflows, &self.retention_policy).await;
    }

    // ── External worker API ───────────────────────────────────────────────

    /// Long-poll for a pending task matching any of the given activity names.
    ///
    /// Returns immediately if a task is available, otherwise waits up to
    /// `timeout_ms` milliseconds. Returns `None` if the timeout expires with
    /// no task available.
    pub async fn poll_task(
        &self,
        activity_names: Vec<String>,
        worker_id: String,
        timeout_ms: u64,
    ) -> Result<Option<PendingTask>> {
        // First attempt: claim immediately if a task is already pending.
        if let Some(task) = self
            .storage
            .claim_pending_task(&activity_names, &worker_id)
            .await?
        {
            return Ok(Some(task));
        }

        // Long-poll: wait until a new task is signalled or timeout expires.
        tokio::select! {
            _ = self.task_queue.notify.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(timeout_ms)) => {}
        }

        // Second attempt after wakeup.
        self.storage
            .claim_pending_task(&activity_names, &worker_id)
            .await
    }

    /// Report that an external worker completed a task successfully.
    pub async fn complete_task(&self, task_token: Uuid, output: Value) -> Result<()> {
        self.storage
            .resolve_pending_task(task_token, TaskResult::Success { output: output.clone() })
            .await?;
        // Fast path: wake the waiting workflow context via in-memory channel.
        if let Some(tx) = self.pending_completions.lock().await.remove(&task_token) {
            let _ = tx.send(TaskResult::Success { output });
        }
        Ok(())
    }

    /// Report that an external worker failed a task.
    pub async fn fail_task(&self, task_token: Uuid, error: String) -> Result<()> {
        self.storage
            .resolve_pending_task(task_token, TaskResult::Failure { error: error.clone() })
            .await?;
        if let Some(tx) = self.pending_completions.lock().await.remove(&task_token) {
            let _ = tx.send(TaskResult::Failure { error });
        }
        Ok(())
    }

    /// Send a heartbeat for a claimed task. Returns `true` if the run has
    /// been cancelled (signal for the worker to stop).
    pub async fn heartbeat_task(&self, task_token: Uuid) -> Result<bool> {
        let updated = self.storage.heartbeat_pending_task(task_token).await?;
        if !updated {
            return Err(GearsError::HeartbeatMismatch(task_token));
        }
        // Check whether the owning run was cancelled.
        let task = self
            .storage
            .get_pending_task(task_token)
            .await?
            .ok_or(GearsError::TaskNotFound(task_token))?;
        let status = self.storage.get_run_status(task.run_id).await?;
        Ok(status == RunStatus::Cancelled)
    }

    /// Fetch a pending task by token. Returns `GearsError::TaskNotFound` if
    /// the token does not exist.
    pub async fn get_pending_task(&self, task_token: Uuid) -> Result<PendingTask> {
        self.storage
            .get_pending_task(task_token)
            .await?
            .ok_or(GearsError::TaskNotFound(task_token))
    }

    /// Names of all registered external activity types.
    pub fn external_activity_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.external_activity_configs.keys().cloned().collect();
        names.sort();
        names
    }
}

// ── Dispatch loop ─────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn dispatch_loop(
    mut start_rx: mpsc::Receiver<StartRequest>,
    mut shutdown_rx: oneshot::Receiver<()>,
    storage: Arc<dyn Storage>,
    workflows: Arc<HashMap<String, Arc<dyn Workflow>>>,
    activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    external_activity_configs: Arc<HashMap<String, ExternalActivityConfig>>,
    semaphore: Arc<Semaphore>,
    cancel_handles: CancelHandles,
    cleanup_policy: CleanupPolicy,
    task_queue: Arc<ExternalTaskQueue>,
    pending_completions: Arc<Mutex<HashMap<Uuid, tokio::sync::oneshot::Sender<TaskResult>>>>,
) {
    loop {
        tokio::select! {
            Some(req) = start_rx.recv() => {
                let workflow = match workflows.get(&req.workflow_name) {
                    Some(w) => w.clone(),
                    None => {
                        tracing::error!(
                            run_id = %req.run_id,
                            workflow = %req.workflow_name,
                            "unknown workflow — skipping"
                        );
                        continue;
                    }
                };

                // Load history (empty for brand-new runs).
                let history = match storage.load_events(req.run_id).await {
                    Ok(h) => h,
                    Err(e) => {
                        tracing::error!(run_id = %req.run_id, error = %e, "failed to load history");
                        continue;
                    }
                };

                let Ok(permit) = semaphore.clone().acquire_owned().await else {
                    tracing::error!("dispatch loop: semaphore closed, stopping");
                    break;
                };
                let task = WorkerTask {
                    run_id: req.run_id,
                    workflow,
                    activities: activities.clone(),
                    external_activity_configs: external_activity_configs.clone(),
                    storage: storage.clone(),
                    history,
                    input: req.input,
                    cleanup_policy,
                    task_queue: task_queue.clone(),
                    pending_completions: pending_completions.clone(),
                };

                let handles = cancel_handles.clone();
                tokio::spawn(async move {
                    task.run(&handles).await;
                    drop(permit);
                });
            }
            _ = &mut shutdown_rx => {
                tracing::info!("engine dispatch loop shutting down");
                break;
            }
        }
    }
}

// ── Scheduler job builder ─────────────────────────────────────────────────

/// Build a `tokio-cron-scheduler` job that fires `start_workflow` on each tick.
fn build_scheduler_job(
    schedule: &ScheduleRecord,
    start_tx: mpsc::Sender<StartRequest>,
    storage: Arc<dyn Storage>,
) -> Result<Job> {
    let name = schedule.name.clone();
    let workflow_name = schedule.workflow_name.clone();
    let input = schedule.input.clone();

    Job::new_async(schedule.cron_expression.as_str(), move |_uuid, _l| {
        let name = name.clone();
        let workflow_name = workflow_name.clone();
        let input = input.clone();
        let start_tx = start_tx.clone();
        let storage = storage.clone();

        Box::pin(async move {
            let run_id = Uuid::new_v4();
            if let Err(e) = storage.create_run(run_id, &workflow_name, &input).await {
                tracing::error!(
                    schedule = %name,
                    error = %e,
                    "failed to create scheduled run"
                );
                return;
            }
            let _ = storage.record_schedule_fired(&name, Utc::now()).await;
            if start_tx
                .send(StartRequest {
                    run_id,
                    workflow_name: workflow_name.clone(),
                    input: input.clone(),
                })
                .await
                .is_err()
            {
                tracing::error!(
                    schedule = %name,
                    "dispatch channel closed — run created but not dispatched"
                );
                return;
            }
            tracing::info!(schedule = %name, %run_id, workflow = %workflow_name, "schedule fired");
        })
    })
    .map_err(|e| GearsError::InvalidSchedule(e.to_string()))
}

// ── EngineHandle ──────────────────────────────────────────────────────────

// ── Stale task monitor ────────────────────────────────────────────────────

async fn stale_task_monitor(
    mut shutdown_rx: oneshot::Receiver<()>,
    storage: Arc<dyn Storage>,
    task_queue: Arc<ExternalTaskQueue>,
    configs: Arc<HashMap<String, ExternalActivityConfig>>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.tick().await; // skip the immediate first tick

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let min_timeout_ms = configs
                    .values()
                    .map(|c| c.heartbeat_timeout.as_millis() as u64)
                    .min()
                    .unwrap_or(30_000);
                match storage.list_stale_pending_tasks(min_timeout_ms).await {
                    Ok(stale) => {
                        for task in stale {
                            if storage.reset_pending_task(task.task_token).await.is_ok() {
                                tracing::warn!(
                                    task_token = %task.task_token,
                                    activity = %task.activity_name,
                                    "stale external task reset to pending"
                                );
                                task_queue.notify.notify_waiters();
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "stale task monitor query failed");
                    }
                }
            }
            _ = &mut shutdown_rx => {
                tracing::info!("stale task monitor shutting down");
                break;
            }
        }
    }
}

// ── EngineHandle ──────────────────────────────────────────────────────────

/// Returned by `WorkflowEngine::run()`. Drop it or call `shutdown()` to
/// stop the dispatch loop and the cron scheduler.
pub struct EngineHandle {
    shutdown_tx: oneshot::Sender<()>,
    pruner_shutdown_tx: Option<oneshot::Sender<()>>,
    stale_monitor_shutdown_tx: Option<oneshot::Sender<()>>,
    job_scheduler: Arc<Mutex<JobScheduler>>,
}

impl EngineHandle {
    pub async fn shutdown(self) {
        let _ = self.job_scheduler.lock().await.shutdown().await;
        if let Some(tx) = self.pruner_shutdown_tx {
            let _ = tx.send(());
        }
        if let Some(tx) = self.stale_monitor_shutdown_tx {
            let _ = tx.send(());
        }
        let _ = self.shutdown_tx.send(());
    }
}

// ── Retention pruning ─────────────────────────────────────────────────────

async fn pruning_loop(
    mut shutdown_rx: oneshot::Receiver<()>,
    storage: Arc<dyn Storage>,
    workflows: Arc<HashMap<String, Arc<dyn Workflow>>>,
    policy: RetentionPolicy,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(3_600));
    interval.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            _ = interval.tick() => {
                run_pruning_pass(&storage, &workflows, &policy).await;
            }
            _ = &mut shutdown_rx => {
                tracing::info!("retention pruning task shutting down");
                break;
            }
        }
    }
}

async fn run_pruning_pass(
    storage: &Arc<dyn Storage>,
    workflows: &Arc<HashMap<String, Arc<dyn Workflow>>>,
    policy: &RetentionPolicy,
) {
    let now = Utc::now();

    // Phase A: prune each registered workflow by its effective retention.
    for (name, wf) in workflows.iter() {
        let Some(duration) = policy.effective_for(wf.retention()) else {
            continue;
        };
        let cutoff = now
            - chrono::Duration::from_std(duration)
                .unwrap_or(chrono::Duration::try_days(i64::MAX / 86_400).unwrap_or_default());
        prune_terminal_runs(storage, Some(name.as_str()), cutoff).await;
    }

    // Phase B: apply global retention to orphaned workflow types (no longer registered).
    if let Some(days) = policy.global_days {
        let cutoff = now - chrono::Duration::days(days as i64);
        let registered: HashSet<&str> = workflows.keys().map(String::as_str).collect();
        for status in [RunStatus::Completed, RunStatus::Failed, RunStatus::Cancelled] {
            let filter = RunFilter {
                status: Some(status),
                created_before: Some(cutoff),
                limit: Some(1000),
                ..Default::default()
            };
            match storage.list_runs(&filter).await {
                Ok(runs) => {
                    for run in runs
                        .into_iter()
                        .filter(|r| !registered.contains(r.workflow_name.as_str()))
                    {
                        delete_run_logged(storage, run.run_id, &run.workflow_name).await;
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "pruning: list_runs failed for orphaned workflows");
                }
            }
        }
    }
}

async fn prune_terminal_runs(
    storage: &Arc<dyn Storage>,
    workflow_name: Option<&str>,
    cutoff: DateTime<Utc>,
) {
    for status in [RunStatus::Completed, RunStatus::Failed, RunStatus::Cancelled] {
        let filter = RunFilter {
            status: Some(status),
            workflow_name: workflow_name.map(String::from),
            created_before: Some(cutoff),
            limit: Some(1000),
            ..Default::default()
        };
        match storage.list_runs(&filter).await {
            Ok(runs) => {
                for run in runs {
                    delete_run_logged(storage, run.run_id, &run.workflow_name).await;
                }
            }
            Err(e) => {
                tracing::error!(
                    workflow = ?workflow_name,
                    error = %e,
                    "pruning: list_runs failed"
                );
            }
        }
    }
}

async fn delete_run_logged(storage: &Arc<dyn Storage>, run_id: Uuid, workflow_name: &str) {
    match storage.delete_run(run_id).await {
        Ok(()) => tracing::info!(%run_id, workflow = workflow_name, "pruned expired workflow run"),
        Err(e) => tracing::error!(%run_id, error = %e, "pruning: failed to delete run"),
    }
}
