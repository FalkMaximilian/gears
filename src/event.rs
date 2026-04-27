use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One entry in the durable event log for a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct WorkflowEvent {
    /// Monotonically increasing position within this run's event log.
    pub sequence: u64,
    pub occurred_at: DateTime<Utc>,
    pub payload: EventPayload,
}

/// All possible state transitions for a workflow run.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventPayload {
    /// Run was created and its input was recorded.
    WorkflowStarted {
        workflow_name: String,
        #[schema(value_type = Object)]
        input: serde_json::Value,
    },

    /// The engine is about to execute an activity.
    ActivityScheduled {
        /// Logical call index — increments each time the workflow calls
        /// `execute_activity` or `sleep`. Used as the replay key.
        sequence_id: u32,
        activity_name: String,
        #[schema(value_type = Object)]
        input: serde_json::Value,
    },

    /// An external worker claimed this task from the queue. Written for
    /// observability only — replay correctness depends solely on
    /// `ActivityCompleted` / `ActivityErrored`.
    ActivityDispatched {
        sequence_id: u32,
        task_token: Uuid,
        attempt: u32,
    },

    /// Activity completed successfully.
    ActivityCompleted {
        sequence_id: u32,
        #[schema(value_type = Object)]
        output: serde_json::Value,
    },

    /// One attempt failed; may still be retried.
    ActivityAttemptFailed {
        sequence_id: u32,
        attempt: u32,
        error: String,
    },

    /// All retry attempts exhausted — workflow receives an error.
    ActivityErrored { sequence_id: u32, error: String },

    /// Workflow called `ctx.sleep_until(wake_at)`.
    TimerStarted {
        sequence_id: u32,
        wake_at: DateTime<Utc>,
    },

    /// Timer elapsed and workflow was resumed.
    TimerFired { sequence_id: u32 },

    /// One attempt timed out before completing.
    ActivityAttemptTimedOut {
        sequence_id: u32,
        attempt: u32,
        timeout_ms: u64,
    },

    /// Version marker for safe workflow code migration.
    VersionMarker { change_id: String, version: u32 },

    /// The workflow called `ctx.register_cleanup()`, registering an activity to
    /// be executed when the workflow completes successfully or is cancelled.
    ///
    /// `sequence_id` is allocated from the same call counter as
    /// `execute_activity` and `sleep`, so this event participates in
    /// deterministic replay: on recovery, the matching event in history is
    /// found and the call returns immediately without re-persisting.
    ///
    /// Multiple `CleanupRegistered` events may appear in a single run's log.
    /// The `WorkerTask` runs them in reverse registration order (LIFO) before
    /// writing the terminal event.
    CleanupRegistered {
        sequence_id: u32,
        activity_name: String,
        #[schema(value_type = Object)]
        input: serde_json::Value,
    },

    /// A registered cleanup activity ran and returned `Ok`.
    ///
    /// Written by `WorkerTask` after calling the cleanup activity. The
    /// `sequence_id` matches the corresponding `CleanupRegistered` event.
    /// On crash recovery, the presence of this event means the cleanup has
    /// already been executed and will be skipped.
    CleanupCompleted { sequence_id: u32 },

    /// A registered cleanup activity ran and returned `Err`, or the named
    /// activity was not in the registry.
    ///
    /// Cleanup failures are **tolerated**: this event is written and execution
    /// continues with the next cleanup. The workflow's terminal status
    /// (`Completed` or `Cancelled`) is not affected. As with `CleanupCompleted`,
    /// the presence of this event prevents the cleanup from being re-run on
    /// crash recovery.
    CleanupFailed { sequence_id: u32, error: String },

    /// Workflow was cancelled via `engine.cancel_workflow()`.
    WorkflowCancelled { reason: String },

    /// Workflow function returned successfully.
    WorkflowCompleted {
        #[schema(value_type = Object)]
        output: serde_json::Value,
    },

    /// Workflow function returned an error.
    WorkflowFailed { error: String },

    /// Marks the start of a concurrent-branch fork in history.
    ///
    /// Written by `WorkflowContext::concurrently` and friends when executing
    /// live (skipped on replay if already present). Records how many branches
    /// were spawned and the per-branch sequence-ID budget so that the fork
    /// point is observable in the event log.
    ConcurrentBranchesStarted {
        /// The sequence_id claimed for this fork point.
        sequence_id: u32,
        /// Number of parallel branches spawned.
        num_branches: u32,
        /// Sequence-ID budget reserved per branch.
        branch_budget: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(payload: EventPayload) {
        let event = WorkflowEvent {
            sequence: 1,
            occurred_at: Utc::now(),
            payload,
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: WorkflowEvent = serde_json::from_str(&json).unwrap();
        // verify round-trip via re-serialization
        let json2 = serde_json::to_string(&back).unwrap();
        assert_eq!(json, json2);
    }

    #[test]
    fn test_round_trip_all_variants() {
        round_trip(EventPayload::WorkflowStarted {
            workflow_name: "my_workflow".into(),
            input: json!({"key": "value"}),
        });
        round_trip(EventPayload::ActivityScheduled {
            sequence_id: 0,
            activity_name: "my_activity".into(),
            input: json!({"x": 1}),
        });
        round_trip(EventPayload::ActivityCompleted {
            sequence_id: 0,
            output: json!({"result": true}),
        });
        round_trip(EventPayload::ActivityAttemptFailed {
            sequence_id: 0,
            attempt: 1,
            error: "timeout".into(),
        });
        round_trip(EventPayload::ActivityErrored {
            sequence_id: 0,
            error: "all retries exhausted".into(),
        });
        round_trip(EventPayload::TimerStarted {
            sequence_id: 1,
            wake_at: Utc::now(),
        });
        round_trip(EventPayload::TimerFired { sequence_id: 1 });
        round_trip(EventPayload::ActivityAttemptTimedOut {
            sequence_id: 0,
            attempt: 2,
            timeout_ms: 5000,
        });
        round_trip(EventPayload::VersionMarker {
            change_id: "add_step_2".into(),
            version: 2,
        });
        round_trip(EventPayload::WorkflowCancelled {
            reason: "user requested".into(),
        });
        round_trip(EventPayload::WorkflowCompleted {
            output: json!({"done": true}),
        });
        round_trip(EventPayload::WorkflowFailed {
            error: "oops".into(),
        });
        round_trip(EventPayload::CleanupRegistered {
            sequence_id: 5,
            activity_name: "release_resource".into(),
            input: json!({"handle": "h-123"}),
        });
        round_trip(EventPayload::CleanupCompleted { sequence_id: 5 });
        round_trip(EventPayload::CleanupFailed {
            sequence_id: 5,
            error: "file not found".into(),
        });
        round_trip(EventPayload::ConcurrentBranchesStarted {
            sequence_id: 10,
            num_branches: 3,
            branch_budget: 1000,
        });
        round_trip(EventPayload::ActivityDispatched {
            sequence_id: 0,
            task_token: Uuid::new_v4(),
            attempt: 1,
        });
    }
}
