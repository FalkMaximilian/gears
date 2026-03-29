use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// One entry in the durable event log for a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowEvent {
    /// Monotonically increasing position within this run's event log.
    pub sequence: u64,
    pub occurred_at: DateTime<Utc>,
    pub payload: EventPayload,
}

/// All possible state transitions for a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventPayload {
    /// Run was created and its input was recorded.
    WorkflowStarted {
        workflow_name: String,
        input: serde_json::Value,
    },

    /// The engine is about to execute an activity.
    ActivityScheduled {
        /// Logical call index — increments each time the workflow calls
        /// `execute_activity` or `sleep`. Used as the replay key.
        sequence_id: u32,
        activity_name: String,
        input: serde_json::Value,
    },

    /// Activity completed successfully.
    ActivityCompleted {
        sequence_id: u32,
        output: serde_json::Value,
    },

    /// One attempt failed; may still be retried.
    ActivityAttemptFailed {
        sequence_id: u32,
        attempt: u32,
        error: String,
    },

    /// All retry attempts exhausted — workflow receives an error.
    ActivityErrored {
        sequence_id: u32,
        error: String,
    },

    /// Workflow called `ctx.sleep_until(wake_at)`.
    TimerStarted {
        sequence_id: u32,
        wake_at: DateTime<Utc>,
    },

    /// Timer elapsed and workflow was resumed.
    TimerFired {
        sequence_id: u32,
    },

    /// Workflow function returned successfully.
    WorkflowCompleted {
        output: serde_json::Value,
    },

    /// Workflow function returned an error.
    WorkflowFailed {
        error: String,
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
        round_trip(EventPayload::WorkflowCompleted {
            output: json!({"done": true}),
        });
        round_trip(EventPayload::WorkflowFailed {
            error: "oops".into(),
        });
    }
}
