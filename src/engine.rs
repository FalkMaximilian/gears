use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot};
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

use crate::context::WorkflowContext;
use crate::error::{Result, ZdflowError};
use crate::metrics;
use crate::traits::{
    Activity, RunFilter, RunInfo, RunStatus, ScheduleRecord, ScheduleStatus, Storage, Workflow,
};
use crate::worker::{CleanupPolicy, WorkerTask};

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
    max_concurrent: usize,
    cleanup_policy: CleanupPolicy,
}

impl Default for WorkflowEngineBuilder {
    fn default() -> Self {
        WorkflowEngineBuilder {
            storage: None,
            workflows: HashMap::new(),
            activities: HashMap::new(),
            max_concurrent: 100,
            cleanup_policy: CleanupPolicy::default(),
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

    pub async fn build(self) -> Result<WorkflowEngine> {
        let storage = self
            .storage
            .ok_or_else(|| ZdflowError::Other("storage not configured".into()))?;

        let (start_tx, start_rx) = mpsc::channel::<StartRequest>(1024);

        let job_scheduler =
            Arc::new(Mutex::new(JobScheduler::new().await.map_err(|e| {
                ZdflowError::Other(format!("scheduler init failed: {e}"))
            })?));

        Ok(WorkflowEngine {
            storage,
            workflows: Arc::new(self.workflows),
            activities: Arc::new(self.activities),
            start_tx,
            start_rx: Some(start_rx),
            max_concurrent: self.max_concurrent,
            cancel_handles: Arc::new(Mutex::new(HashMap::new())),
            job_scheduler,
            job_handles: Arc::new(Mutex::new(HashMap::new())),
            cleanup_policy: self.cleanup_policy,
        })
    }
}

// ── WorkflowEngine ────────────────────────────────────────────────────────

pub struct WorkflowEngine {
    storage: Arc<dyn Storage>,
    workflows: Arc<HashMap<String, Arc<dyn Workflow>>>,
    activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    start_tx: mpsc::Sender<StartRequest>,
    /// Held until `run()` is called and moves it into the dispatch task.
    start_rx: Option<mpsc::Receiver<StartRequest>>,
    max_concurrent: usize,
    cancel_handles: CancelHandles,
    job_scheduler: Arc<Mutex<JobScheduler>>,
    /// Maps schedule name → scheduler job UUID (for pause/remove).
    job_handles: Arc<Mutex<HashMap<String, Uuid>>>,
    cleanup_policy: CleanupPolicy,
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
        for record in running {
            self.start_tx
                .send(StartRequest {
                    run_id: record.run_id,
                    workflow_name: record.workflow_name,
                    input: record.input,
                })
                .await
                .map_err(|_| ZdflowError::EngineNotRunning)?;
        }

        let start_rx = self
            .start_rx
            .take()
            .ok_or_else(|| ZdflowError::Other("WorkflowEngine::run() called twice".into()))?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let storage = self.storage.clone();
        let workflows = self.workflows.clone();
        let activities = self.activities.clone();
        let semaphore = Arc::new(Semaphore::new(self.max_concurrent));
        let cancel_handles = self.cancel_handles.clone();
        let cleanup_policy = self.cleanup_policy;

        tokio::spawn(dispatch_loop(
            start_rx,
            shutdown_rx,
            storage,
            workflows,
            activities,
            semaphore,
            cancel_handles,
            cleanup_policy,
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
                    .map_err(|e| ZdflowError::Other(format!("failed to add job: {e}")))?;
                handles.insert(schedule.name.clone(), job_uuid);
                tracing::info!(schedule = %schedule.name, "schedule recovered");
            }
        }

        self.job_scheduler
            .lock()
            .await
            .start()
            .await
            .map_err(|e| ZdflowError::Other(format!("scheduler start failed: {e}")))?;

        Ok(EngineHandle {
            shutdown_tx,
            job_scheduler: self.job_scheduler.clone(),
        })
    }

    /// Enqueue a new workflow run. Returns the run ID immediately;
    /// execution is asynchronous.
    pub async fn start_workflow(&self, workflow_name: &str, input: Value) -> Result<Uuid> {
        if !self.workflows.contains_key(workflow_name) {
            return Err(ZdflowError::WorkflowNotFound(workflow_name.to_string()));
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
            .map_err(|_| ZdflowError::EngineNotRunning)?;

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
    /// return `ZdflowError::Cancelled` at the next yield point.
    pub async fn cancel_workflow(&self, run_id: Uuid) -> Result<()> {
        let handles = self.cancel_handles.lock().await;
        if let Some(ctx) = handles.get(&run_id) {
            ctx.cancel();
            tracing::info!(run_id = %run_id, "workflow cancellation requested");
            Ok(())
        } else {
            Err(ZdflowError::RunNotFound(run_id))
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
            return Err(ZdflowError::WorkflowNotFound(workflow_name.to_string()));
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
            .map_err(|e| ZdflowError::Other(format!("failed to add scheduled job: {e}")))?;

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
            .ok_or_else(|| ZdflowError::ScheduleNotFound(name.to_string()))?;
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
            .ok_or_else(|| ZdflowError::ScheduleNotFound(name.to_string()))?;

        let job = build_scheduler_job(&record, self.start_tx.clone(), self.storage.clone())?;
        let job_uuid = self
            .job_scheduler
            .lock()
            .await
            .add(job)
            .await
            .map_err(|e| ZdflowError::Other(format!("failed to add scheduled job: {e}")))?;

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
}

// ── Dispatch loop ─────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn dispatch_loop(
    mut start_rx: mpsc::Receiver<StartRequest>,
    mut shutdown_rx: oneshot::Receiver<()>,
    storage: Arc<dyn Storage>,
    workflows: Arc<HashMap<String, Arc<dyn Workflow>>>,
    activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    semaphore: Arc<Semaphore>,
    cancel_handles: CancelHandles,
    cleanup_policy: CleanupPolicy,
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
                    storage: storage.clone(),
                    history,
                    input: req.input,
                    cleanup_policy,
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
    .map_err(|e| ZdflowError::InvalidSchedule(e.to_string()))
}

// ── EngineHandle ──────────────────────────────────────────────────────────

/// Returned by `WorkflowEngine::run()`. Drop it or call `shutdown()` to
/// stop the dispatch loop and the cron scheduler.
pub struct EngineHandle {
    shutdown_tx: oneshot::Sender<()>,
    job_scheduler: Arc<Mutex<JobScheduler>>,
}

impl EngineHandle {
    pub async fn shutdown(self) {
        let _ = self.job_scheduler.lock().await.shutdown().await;
        let _ = self.shutdown_tx.send(());
    }
}
