use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot};
use uuid::Uuid;

use crate::context::WorkflowContext;
use crate::error::{Result, ZdflowError};
use crate::metrics;
use crate::traits::{Activity, RunFilter, RunInfo, RunStatus, Storage, Workflow};
use crate::worker::WorkerTask;

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
}

impl Default for WorkflowEngineBuilder {
    fn default() -> Self {
        WorkflowEngineBuilder {
            storage: None,
            workflows: HashMap::new(),
            activities: HashMap::new(),
            max_concurrent: 100,
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

    pub async fn build(self) -> Result<WorkflowEngine> {
        let storage = self
            .storage
            .ok_or_else(|| ZdflowError::Other("storage not configured".into()))?;

        let (start_tx, start_rx) = mpsc::channel::<StartRequest>(1024);

        Ok(WorkflowEngine {
            storage,
            workflows: Arc::new(self.workflows),
            activities: Arc::new(self.activities),
            start_tx,
            start_rx: Some(start_rx),
            max_concurrent: self.max_concurrent,
            cancel_handles: Arc::new(Mutex::new(HashMap::new())),
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
            .expect("WorkflowEngine::run called twice");

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let storage = self.storage.clone();
        let workflows = self.workflows.clone();
        let activities = self.activities.clone();
        let semaphore = Arc::new(Semaphore::new(self.max_concurrent));
        let cancel_handles = self.cancel_handles.clone();

        tokio::spawn(dispatch_loop(
            start_rx,
            shutdown_rx,
            storage,
            workflows,
            activities,
            semaphore,
            cancel_handles,
        ));

        Ok(EngineHandle { shutdown_tx })
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
            Err(ZdflowError::Other(format!(
                "no active workflow with run_id {run_id}"
            )))
        }
    }
}

// ── Dispatch loop ─────────────────────────────────────────────────────────

async fn dispatch_loop(
    mut start_rx: mpsc::Receiver<StartRequest>,
    mut shutdown_rx: oneshot::Receiver<()>,
    storage: Arc<dyn Storage>,
    workflows: Arc<HashMap<String, Arc<dyn Workflow>>>,
    activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    semaphore: Arc<Semaphore>,
    cancel_handles: CancelHandles,
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

                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let task = WorkerTask {
                    run_id: req.run_id,
                    workflow,
                    activities: activities.clone(),
                    storage: storage.clone(),
                    history,
                    input: req.input,
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

// ── EngineHandle ──────────────────────────────────────────────────────────

/// Returned by `WorkflowEngine::run()`. Drop it or call `shutdown()` to
/// stop the dispatch loop.
pub struct EngineHandle {
    shutdown_tx: oneshot::Sender<()>,
}

impl EngineHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}
