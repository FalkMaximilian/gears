use std::time::Duration;

use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum GearsError {
    #[error("storage error: {0}")]
    Storage(#[from] tokio_rusqlite::Error),

    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("workflow not found: {0}")]
    WorkflowNotFound(String),

    #[error("activity not found: {0}")]
    ActivityNotFound(String),

    #[error("activity failed after all retries: {0}")]
    ActivityFailed(String),

    #[error("workflow failed: {0}")]
    WorkflowFailed(String),

    #[error("workflow cancelled")]
    Cancelled,

    #[error("engine not running")]
    EngineNotRunning,

    /// An activity attempt exceeded its configured per-attempt timeout.
    #[error("activity '{activity_name}' timed out after {timeout:?}")]
    ActivityTimedOut {
        activity_name: String,
        timeout: Duration,
    },

    /// The stored version marker for `change_id` exceeds the current `max_version`.
    /// This indicates a rollback — the running code is older than what was persisted.
    #[error("version incompatibility for '{change_id}': stored={stored} exceeds max={max}")]
    VersionConflict {
        change_id: String,
        stored: u32,
        max: u32,
    },

    /// A tokio task spawned for parallel activity execution panicked.
    #[error("parallel activity task panicked: {0}")]
    TaskPanicked(String),

    /// An invalid cron expression was passed to `schedule_workflow`.
    #[error("invalid cron expression: {0}")]
    InvalidSchedule(String),

    /// `cancel_workflow` was called for a run ID with no active handle.
    #[error("no active workflow run with id {0}")]
    RunNotFound(Uuid),

    /// `pause_schedule` or `resume_schedule` called for an unknown schedule.
    #[error("schedule not found: '{0}'")]
    ScheduleNotFound(String),

    /// A concurrent branch exhausted its pre-allocated sequence-ID budget.
    #[error("concurrent branch exceeded its sequence-id budget of {budget}")]
    BranchBudgetExceeded { budget: u32 },

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, GearsError>;
