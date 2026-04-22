use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::context::{ActivityContext, WorkflowContext};
use crate::error::GearsError;
use crate::event::EventPayload;
use crate::metrics;
use crate::traits::{Activity, RunStatus, Storage, Workflow};

/// Controls when registered cleanups are executed after a workflow run ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CleanupPolicy {
    /// Run cleanups only when the workflow returns `Ok` or `Err(Cancelled)`.
    /// Workflow failures (`Err(other)`) skip cleanups. This is the default.
    #[default]
    OnSuccessOrCancelled,
    /// Run cleanups after any workflow outcome, including `Err(other)`.
    Always,
}

/// Shared map of active workflow contexts, used for cancellation.
type CancelHandles = Arc<Mutex<HashMap<Uuid, WorkflowContext>>>;

/// Execute all registered cleanups that have not yet been completed or failed.
///
/// This function is called by [`WorkerTask::run`] immediately after
/// `Workflow::run` returns `Ok` or `Err(GearsError::Cancelled)`, and also
/// after `Err(other)` when [`CleanupPolicy::Always`] is configured. Under the
/// default [`CleanupPolicy::OnSuccessOrCancelled`], workflow failures skip
/// cleanups because they may be transient and recoverable.
///
/// ## Ordering
///
/// Cleanups are run in reverse registration order (LIFO). The assumption is
/// that the last resource acquired should be the first released.
///
/// ## Failure tolerance
///
/// If a cleanup activity returns `Err`, or if the named activity is not in the
/// registry, a [`EventPayload::CleanupFailed`] event is written and execution
/// continues with the next cleanup. A failing cleanup never propagates an error
/// or changes the workflow's final status.
///
/// ## Crash recovery / idempotency
///
/// This function runs **before** the terminal event is written. If the process
/// crashes here, the run remains in `Running` status and will be re-dispatched
/// on the next engine startup. To avoid running a cleanup twice, the function
/// checks history for a [`EventPayload::CleanupCompleted`] or
/// [`EventPayload::CleanupFailed`] event with a matching `sequence_id` before
/// executing each cleanup. Cleanups that already have a terminal cleanup event
/// are skipped.
///
/// ## Why events are reloaded from storage
///
/// The `WorkflowContext` was built with a history snapshot taken *before*
/// `Workflow::run` was called. Any `CleanupRegistered` events appended during
/// the live execution are not in that snapshot. Re-loading from storage here
/// ensures all registrations — including those written moments ago — are
/// visible.
async fn run_cleanups(
    run_id: Uuid,
    storage: &Arc<dyn Storage>,
    activities: &HashMap<String, Arc<dyn Activity>>,
    worker_ctx: &WorkflowContext,
) {
    // Reload events from storage: cleanups registered during the live execution
    // are not in the pre-loaded history snapshot that was passed to WorkflowContext.
    let all_events = match storage.load_events(run_id).await {
        Ok(events) => events,
        Err(e) => {
            tracing::error!(run_id = %run_id, error = %e, "failed to reload events for cleanup");
            return;
        }
    };

    // Collect CleanupRegistered entries in registration order, then reverse (LIFO).
    let mut registrations: Vec<(u32, String, serde_json::Value)> = all_events
        .iter()
        .filter_map(|e| {
            if let EventPayload::CleanupRegistered {
                sequence_id,
                activity_name,
                input,
            } = &e.payload
            {
                Some((*sequence_id, activity_name.clone(), input.clone()))
            } else {
                None
            }
        })
        .collect();
    registrations.reverse();

    for (seq_id, activity_name, input) in registrations {
        // Idempotency: skip cleanups that already have a terminal cleanup event.
        // This handles crash recovery where some cleanups ran before the crash.
        let already_done = all_events.iter().any(|e| {
            matches!(&e.payload, EventPayload::CleanupCompleted { sequence_id: s } if *s == seq_id)
                || matches!(&e.payload, EventPayload::CleanupFailed { sequence_id: s, .. } if *s == seq_id)
        });
        if already_done {
            tracing::debug!(
                run_id = %run_id,
                sequence_id = seq_id,
                activity = %activity_name,
                "cleanup already executed, skipping"
            );
            continue;
        }

        let activity = match activities.get(&activity_name) {
            Some(a) => a.clone(),
            None => {
                tracing::error!(
                    run_id = %run_id,
                    activity = %activity_name,
                    "cleanup activity not found in registry"
                );
                let _ = worker_ctx
                    .append_event(EventPayload::CleanupFailed {
                        sequence_id: seq_id,
                        error: format!("activity '{}' not registered", activity_name),
                    })
                    .await;
                continue;
            }
        };

        let cleanup_ctx = ActivityContext {
            run_id,
            activity_name: activity_name.clone(),
            attempt: 1,
            sequence_id: seq_id,
            shared_state: worker_ctx.shared_state_value(),
        };

        tracing::info!(
            run_id = %run_id,
            activity = %activity_name,
            sequence_id = seq_id,
            "running cleanup"
        );

        match activity.execute(cleanup_ctx, input).await {
            Ok(_) => {
                let _ = worker_ctx
                    .append_event(EventPayload::CleanupCompleted {
                        sequence_id: seq_id,
                    })
                    .await;
                tracing::info!(run_id = %run_id, activity = %activity_name, "cleanup completed");
            }
            Err(e) => {
                tracing::warn!(
                    run_id = %run_id,
                    activity = %activity_name,
                    error = %e,
                    "cleanup failed (tolerated)"
                );
                let _ = worker_ctx
                    .append_event(EventPayload::CleanupFailed {
                        sequence_id: seq_id,
                        error: e.to_string(),
                    })
                    .await;
            }
        }
    }
}

/// Executes a single workflow run from start (or recovery point) to finish.
pub(crate) struct WorkerTask {
    pub run_id: Uuid,
    pub workflow: Arc<dyn Workflow>,
    pub activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    pub storage: Arc<dyn Storage>,
    pub history: Vec<crate::event::WorkflowEvent>,
    pub input: Value,
    pub cleanup_policy: CleanupPolicy,
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
        let ctx = WorkflowContext::new(
            run_id,
            self.history,
            self.storage.clone(),
            self.activities.clone(),
        );

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
                tracing::info!(run_id = %run_id, "workflow completed — running cleanups");
                run_cleanups(run_id, &self.storage, &self.activities, &worker_ctx).await;
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
            Err(GearsError::Cancelled) => {
                tracing::info!(run_id = %run_id, "workflow cancelled — running cleanups");
                run_cleanups(run_id, &self.storage, &self.activities, &worker_ctx).await;
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
                if self.cleanup_policy == CleanupPolicy::Always {
                    tracing::info!(run_id = %run_id, "workflow failed — running cleanups (policy=Always)");
                    run_cleanups(run_id, &self.storage, &self.activities, &worker_ctx).await;
                }
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
