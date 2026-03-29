use thiserror::Error;

#[derive(Debug, Error)]
pub enum ZdflowError {
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

    #[error("engine not running")]
    EngineNotRunning,

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, ZdflowError>;
