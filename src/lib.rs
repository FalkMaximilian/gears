pub mod context;
pub mod engine;
pub mod error;
pub mod event;
pub mod storage;
pub mod traits;
pub(crate) mod worker;

// Public re-exports for library users.
pub use context::{ActivityContext, WorkflowContext};
pub use engine::{EngineHandle, WorkflowEngine, WorkflowEngineBuilder};
pub use error::{Result, ZdflowError};
pub use event::{EventPayload, WorkflowEvent};
pub use storage::SqliteStorage;
pub use traits::{
    Activity, ActivityFuture, RunRecord, RunStatus, Storage, Workflow, WorkflowFuture,
};
