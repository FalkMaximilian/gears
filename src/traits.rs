use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::context::{ActivityContext, WorkflowContext};
use crate::error::Result;
use crate::event::WorkflowEvent;

/// Boxed future returned by workflow and activity methods.
pub type WorkflowFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;
pub type ActivityFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;
/// Boxed future used by the dyn-compatible Storage trait.
pub type StorageFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>;

// ── Workflow ──────────────────────────────────────────────────────────────

/// Implement this trait to define a workflow.
///
/// Workflow code must be **deterministic**: given the same sequence of
/// activity results and timer completions, it must always make the same
/// calls in the same order. Do not use `SystemTime::now()`, random
/// numbers, or external I/O directly inside a workflow — wrap those in
/// an Activity instead.
pub trait Workflow: Send + Sync + 'static {
    /// Stable identifier for this workflow type. Used to look it up in
    /// the registry; must be unique across all registered workflows.
    fn name(&self) -> &'static str;

    /// Entry point. `ctx` provides `execute_activity` and `sleep`.
    /// `input` is the JSON payload passed to `engine.start_workflow`.
    fn run(&self, ctx: WorkflowContext, input: Value) -> WorkflowFuture;
}

// ── Activity ──────────────────────────────────────────────────────────────

/// Implement this trait to define an activity.
///
/// Activities may perform arbitrary I/O. They are retried automatically
/// on failure up to `max_attempts` times with exponential backoff.
pub trait Activity: Send + Sync + 'static {
    /// Stable identifier for this activity type.
    fn name(&self) -> &'static str;

    /// Execute the activity. `ctx` provides metadata (run ID, attempt
    /// number). `input` is whatever the workflow passed.
    fn execute(&self, ctx: ActivityContext, input: Value) -> ActivityFuture;

    /// Maximum number of attempts before the engine gives up and
    /// propagates an error to the workflow. Defaults to 3.
    fn max_attempts(&self) -> u32 {
        3
    }

    /// Base delay before the first retry. Doubles on each subsequent
    /// attempt (exponential backoff). Defaults to 1 second.
    fn retry_base_delay(&self) -> Duration {
        Duration::from_secs(1)
    }

    /// Optional per-activity execution deadline. If the activity does not
    /// complete within this duration, the attempt is treated as failed and
    /// may be retried. Defaults to `None` (no timeout).
    fn timeout(&self) -> Option<Duration> {
        None
    }
}

// ── Storage ───────────────────────────────────────────────────────────────

/// A record returned by `list_running_workflows` used for crash recovery.
#[derive(Debug, Clone)]
pub struct RunRecord {
    pub run_id: Uuid,
    pub workflow_name: String,
    pub input: Value,
}

/// Run lifecycle status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
    NotFound,
}

/// Filter criteria for listing workflow runs.
#[derive(Debug, Clone, Default)]
pub struct RunFilter {
    pub status: Option<RunStatus>,
    pub workflow_name: Option<String>,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

/// Summary information about a workflow run.
#[derive(Debug, Clone)]
pub struct RunInfo {
    pub run_id: Uuid,
    pub workflow_name: String,
    pub status: RunStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Persistence backend. Implement this trait to use a different database.
///
/// All methods return boxed futures so the trait is dyn-compatible and can
/// be used as `Arc<dyn Storage>`.
///
/// The default implementation is `SqliteStorage`.
pub trait Storage: Send + Sync + 'static {
    /// Create a new run record before any events are appended.
    fn create_run(&self, run_id: Uuid, workflow_name: &str, input: &Value) -> StorageFuture<()>;

    /// Append a single event to the run's event log.
    fn append_event(&self, run_id: Uuid, event: &WorkflowEvent) -> StorageFuture<()>;

    /// Load the full ordered event history for a run.
    fn load_events(&self, run_id: Uuid) -> StorageFuture<Vec<WorkflowEvent>>;

    /// List every run whose status is still `Running`.
    /// Called on engine startup to recover in-flight workflows.
    fn list_running_workflows(&self) -> StorageFuture<Vec<RunRecord>>;

    /// Update the high-level status of a run.
    fn set_run_status(
        &self,
        run_id: Uuid,
        status: RunStatus,
        result: Option<Value>,
    ) -> StorageFuture<()>;

    /// Query the status of a run.
    fn get_run_status(&self, run_id: Uuid) -> StorageFuture<RunStatus>;

    /// List runs matching the given filter criteria.
    fn list_runs(&self, filter: &RunFilter) -> StorageFuture<Vec<RunInfo>>;
}
