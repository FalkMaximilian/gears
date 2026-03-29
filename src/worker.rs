use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use uuid::Uuid;

use crate::context::WorkflowContext;
use crate::event::{EventPayload, WorkflowEvent};
use crate::traits::{Activity, RunStatus, Storage, Workflow};

/// Executes a single workflow run from start (or recovery point) to finish.
pub(crate) struct WorkerTask {
    pub run_id: Uuid,
    pub workflow: Arc<dyn Workflow>,
    pub activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    pub storage: Arc<dyn Storage>,
    pub history: Vec<WorkflowEvent>,
    pub input: Value,
}

impl WorkerTask {
    pub async fn run(self) {
        let run_id = self.run_id;
        tracing::info!(run_id = %run_id, workflow = self.workflow.name(), "workflow starting");

        // Determine the next global event sequence.
        // history is already loaded; WorkflowContext will track event_seq from max+1.

        // Record WorkflowStarted only if this is a brand-new run.
        let is_new = !self.history.iter().any(|e| {
            matches!(e.payload, EventPayload::WorkflowStarted { .. })
        });

        if is_new {
            let seq = 0u64;
            let event = WorkflowEvent {
                sequence: seq,
                occurred_at: chrono::Utc::now(),
                payload: EventPayload::WorkflowStarted {
                    workflow_name: self.workflow.name().to_string(),
                    input: self.input.clone(),
                },
            };
            if let Err(e) = self.storage.append_event(run_id, &event).await {
                tracing::error!(run_id = %run_id, error = %e, "failed to persist WorkflowStarted");
                return;
            }
        }

        // Build a context that wraps the activity registry so workflow code
        // can call `ctx.execute_activity`.
        let ctx = WorkflowContextWithRegistry::new(
            run_id,
            self.history,
            self.storage.clone(),
            self.activities,
        );

        let result = self.workflow.run(ctx.inner_ctx(), self.input).await;

        match result {
            Ok(output) => {
                tracing::info!(run_id = %run_id, "workflow completed");
                let _ = self
                    .storage
                    .set_run_status(run_id, RunStatus::Completed, Some(output))
                    .await;
            }
            Err(e) => {
                tracing::error!(run_id = %run_id, error = %e, "workflow failed");
                let _ = self
                    .storage
                    .set_run_status(run_id, RunStatus::Failed, None)
                    .await;
            }
        }
    }
}

/// Thin wrapper that holds the activity registry alongside the context.
/// The actual dispatch into activities happens inside `WorkflowContext::execute_activity`,
/// which takes `&dyn Activity` — so the workflow code must pass the activity object.
///
/// For a simpler API, we provide a `WorkflowContextWithRegistry` that exposes
/// a `WorkflowContext` already bound to the storage. The activities are passed
/// directly by the user's workflow code, so no registry lookup is needed here.
pub(crate) struct WorkflowContextWithRegistry {
    ctx: WorkflowContext,
}

impl WorkflowContextWithRegistry {
    fn new(
        run_id: Uuid,
        history: Vec<WorkflowEvent>,
        storage: Arc<dyn Storage>,
        _activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    ) -> Self {
        WorkflowContextWithRegistry {
            ctx: WorkflowContext::new(run_id, history, storage),
        }
    }

    pub fn inner_ctx(&self) -> WorkflowContext {
        self.ctx.clone()
    }
}
