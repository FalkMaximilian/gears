use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::context::WorkflowContext;
use crate::error::ZdflowError;
use crate::event::EventPayload;
use crate::metrics;
use crate::traits::{Activity, RunStatus, Storage, Workflow};

/// Shared map of active workflow contexts, used for cancellation.
type CancelHandles = Arc<Mutex<HashMap<Uuid, WorkflowContext>>>;

/// Executes a single workflow run from start (or recovery point) to finish.
pub(crate) struct WorkerTask {
    pub run_id: Uuid,
    pub workflow: Arc<dyn Workflow>,
    pub activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    pub storage: Arc<dyn Storage>,
    pub history: Vec<crate::event::WorkflowEvent>,
    pub input: Value,
}

impl WorkerTask {
    pub async fn run(self, cancel_handles: &CancelHandles) {
        let run_id = self.run_id;
        tracing::info!(run_id = %run_id, workflow = self.workflow.name(), "workflow starting");

        // Record WorkflowStarted only if this is a brand-new run.
        let is_new = !self
            .history
            .iter()
            .any(|e| matches!(e.payload, EventPayload::WorkflowStarted { .. }));

        if is_new {
            let event = crate::event::WorkflowEvent {
                sequence: 0u64,
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

        metrics::inc_workflow_active();

        // Build the context with the activity registry.
        let ctx = WorkflowContext::new(run_id, self.history, self.storage.clone(), self.activities);

        // Register cancel handle so the engine can cancel this workflow.
        {
            let mut handles = cancel_handles.lock().await;
            handles.insert(run_id, ctx.clone());
        }

        // Keep a clone for writing terminal events after the workflow returns.
        let worker_ctx = ctx.clone();
        let result = self.workflow.run(ctx, self.input).await;

        let workflow_name = self.workflow.name();
        match &result {
            Ok(output) => {
                tracing::info!(run_id = %run_id, "workflow completed");
                let _ = worker_ctx
                    .append_event(EventPayload::WorkflowCompleted {
                        output: output.clone(),
                    })
                    .await;
                let _ = self
                    .storage
                    .set_run_status(run_id, RunStatus::Completed, Some(output.clone()))
                    .await;
                metrics::inc_workflow_completed(workflow_name);
            }
            Err(ZdflowError::Cancelled) => {
                tracing::info!(run_id = %run_id, "workflow cancelled");
                let _ = worker_ctx
                    .append_event(EventPayload::WorkflowCancelled {
                        reason: "cancelled via engine.cancel_workflow()".to_string(),
                    })
                    .await;
                let _ = self
                    .storage
                    .set_run_status(run_id, RunStatus::Cancelled, None)
                    .await;
                metrics::inc_workflow_cancelled(workflow_name);
            }
            Err(e) => {
                tracing::error!(run_id = %run_id, error = %e, "workflow failed");
                let _ = worker_ctx
                    .append_event(EventPayload::WorkflowFailed {
                        error: e.to_string(),
                    })
                    .await;
                let _ = self
                    .storage
                    .set_run_status(run_id, RunStatus::Failed, None)
                    .await;
                metrics::inc_workflow_failed(workflow_name);
            }
        }
        metrics::dec_workflow_active();

        // Deregister cancel handle.
        {
            let mut handles = cancel_handles.lock().await;
            handles.remove(&run_id);
        }
    }
}
