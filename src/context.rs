use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{Result, ZdflowError};
use crate::event::{EventPayload, WorkflowEvent};
use crate::traits::{Activity, Storage};

// ── ActivityContext ───────────────────────────────────────────────────────

/// Passed to every activity execution. Contains metadata about the
/// current attempt; no back-channel to the engine.
#[derive(Debug, Clone)]
pub struct ActivityContext {
    pub run_id: Uuid,
    pub activity_name: String,
    /// 1-based attempt number (1 = first attempt).
    pub attempt: u32,
    /// Logical call index within this workflow run.
    pub sequence_id: u32,
}

// ── WorkflowContext ───────────────────────────────────────────────────────

/// Passed to every workflow execution. Provides `execute_activity` and
/// `sleep` / `sleep_until`.
///
/// Internally this type holds the pre-loaded event history and a
/// monotonically-increasing `call_counter`. Every call to
/// `execute_activity` or `sleep*` increments the counter and uses it as
/// the `sequence_id` to look up cached results in history.
#[derive(Clone)]
pub struct WorkflowContext {
    pub(crate) run_id: Uuid,
    inner: Arc<ContextInner>,
}

struct ContextInner {
    history: Vec<WorkflowEvent>,
    /// Number of logical calls made so far in this execution.
    call_counter: AtomicU32,
    /// Global event sequence counter for the run (separate from call_counter).
    event_seq: AtomicU64,
    storage: Arc<dyn Storage>,
}

impl WorkflowContext {
    pub(crate) fn new(
        run_id: Uuid,
        history: Vec<WorkflowEvent>,
        storage: Arc<dyn Storage>,
    ) -> Self {
        // Find the max sequence already in history so we assign new
        // event sequences after all existing ones.
        let max_seq = history.iter().map(|e| e.sequence).max().unwrap_or(0);
        WorkflowContext {
            run_id,
            inner: Arc::new(ContextInner {
                history,
                call_counter: AtomicU32::new(0),
                event_seq: AtomicU64::new(max_seq + 1),
                storage,
            }),
        }
    }

    /// The unique ID of this workflow run.
    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    // ── execute_activity ─────────────────────────────────────────────────

    /// Execute an activity, returning its output.
    ///
    /// If this `sequence_id` already has an `ActivityCompleted` event in
    /// the history, the cached result is returned immediately without
    /// calling the activity. This is the core replay mechanism.
    ///
    /// If no cached result exists, the activity is executed live with
    /// automatic retry (up to `activity.max_attempts()` times).
    pub async fn execute_activity(
        &self,
        activity: &dyn Activity,
        input: Value,
    ) -> Result<Value> {
        let sequence_id = self
            .inner
            .call_counter
            .fetch_add(1, Ordering::SeqCst);

        // ── Replay path ───────────────────────────────────────────────
        for event in &self.inner.history {
            if let EventPayload::ActivityCompleted {
                sequence_id: sid,
                output,
            } = &event.payload
            {
                if *sid == sequence_id {
                    tracing::debug!(
                        run_id = %self.run_id,
                        activity = activity.name(),
                        sequence_id,
                        "replaying activity result from history"
                    );
                    return Ok(output.clone());
                }
            }
            // If all retries previously exhausted, replay the error too.
            if let EventPayload::ActivityErrored {
                sequence_id: sid,
                error,
            } = &event.payload
            {
                if *sid == sequence_id {
                    return Err(ZdflowError::ActivityFailed(error.clone()));
                }
            }
        }

        // ── Live path ─────────────────────────────────────────────────
        // Persist the scheduling intent before executing.
        self.append_event(EventPayload::ActivityScheduled {
            sequence_id,
            activity_name: activity.name().to_string(),
            input: input.clone(),
        })
        .await?;

        let max_attempts = activity.max_attempts();
        let base_delay = activity.retry_base_delay();
        let mut last_error = String::new();

        for attempt in 1..=max_attempts {
            let ctx = ActivityContext {
                run_id: self.run_id,
                activity_name: activity.name().to_string(),
                attempt,
                sequence_id,
            };

            tracing::debug!(
                run_id = %self.run_id,
                activity = activity.name(),
                attempt,
                "executing activity"
            );

            match activity.execute(ctx, input.clone()).await {
                Ok(output) => {
                    self.append_event(EventPayload::ActivityCompleted {
                        sequence_id,
                        output: output.clone(),
                    })
                    .await?;
                    tracing::info!(
                        run_id = %self.run_id,
                        activity = activity.name(),
                        "activity completed"
                    );
                    return Ok(output);
                }
                Err(e) => {
                    last_error = e.to_string();
                    tracing::warn!(
                        run_id = %self.run_id,
                        activity = activity.name(),
                        attempt,
                        error = %last_error,
                        "activity attempt failed"
                    );
                    self.append_event(EventPayload::ActivityAttemptFailed {
                        sequence_id,
                        attempt,
                        error: last_error.clone(),
                    })
                    .await?;

                    if attempt < max_attempts {
                        let backoff = base_delay * 2u32.pow(attempt - 1);
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }

        // All retries exhausted.
        self.append_event(EventPayload::ActivityErrored {
            sequence_id,
            error: last_error.clone(),
        })
        .await?;
        tracing::error!(
            run_id = %self.run_id,
            activity = activity.name(),
            "activity exhausted all retries"
        );
        Err(ZdflowError::ActivityFailed(last_error))
    }

    // ── sleep / sleep_until ───────────────────────────────────────────────

    /// Durably suspend the workflow for at least `duration`.
    pub async fn sleep(&self, duration: Duration) -> Result<()> {
        let wake_at = Utc::now() + chrono::Duration::from_std(duration)
            .map_err(|e| ZdflowError::Other(e.to_string()))?;
        self.sleep_until(wake_at).await
    }

    /// Durably suspend the workflow until an absolute UTC timestamp.
    ///
    /// On replay (if `TimerFired` is already in history) this returns
    /// immediately. On crash-recovery (if `TimerStarted` exists but not
    /// `TimerFired`) the remaining duration is recalculated.
    pub async fn sleep_until(&self, wake_at: DateTime<Utc>) -> Result<()> {
        let sequence_id = self
            .inner
            .call_counter
            .fetch_add(1, Ordering::SeqCst);

        // ── Replay: timer already fired ───────────────────────────────
        for event in &self.inner.history {
            if let EventPayload::TimerFired { sequence_id: sid } = &event.payload {
                if *sid == sequence_id {
                    tracing::debug!(
                        run_id = %self.run_id,
                        sequence_id,
                        "replaying timer from history"
                    );
                    return Ok(());
                }
            }
        }

        // ── Crash recovery: timer was started but not fired ───────────
        let mut effective_wake_at = wake_at;
        for event in &self.inner.history {
            if let EventPayload::TimerStarted {
                sequence_id: sid,
                wake_at: recorded_wake_at,
            } = &event.payload
            {
                if *sid == sequence_id {
                    // Use the originally-recorded wake_at, not the one
                    // computed from `duration` (which would extend the sleep).
                    effective_wake_at = *recorded_wake_at;
                    break;
                }
            }
        }

        // Persist TimerStarted only if not already in history.
        let already_started = self.inner.history.iter().any(|e| {
            matches!(
                &e.payload,
                EventPayload::TimerStarted { sequence_id: sid, .. } if *sid == sequence_id
            )
        });
        if !already_started {
            self.append_event(EventPayload::TimerStarted {
                sequence_id,
                wake_at: effective_wake_at,
            })
            .await?;
        }

        // Sleep for remaining duration (may be zero if already past).
        let now = Utc::now();
        if effective_wake_at > now {
            let remaining = (effective_wake_at - now)
                .to_std()
                .unwrap_or(Duration::ZERO);
            tracing::debug!(
                run_id = %self.run_id,
                sequence_id,
                remaining_secs = remaining.as_secs_f64(),
                "sleeping"
            );
            tokio::time::sleep(remaining).await;
        }

        self.append_event(EventPayload::TimerFired { sequence_id })
            .await?;
        tracing::debug!(run_id = %self.run_id, sequence_id, "timer fired");
        Ok(())
    }

    // ── helpers ───────────────────────────────────────────────────────────

    async fn append_event(&self, payload: EventPayload) -> Result<()> {
        let sequence = self.inner.event_seq.fetch_add(1, Ordering::SeqCst);
        let event = WorkflowEvent {
            sequence,
            occurred_at: Utc::now(),
            payload,
        };
        self.inner.storage.append_event(self.run_id, &event).await
    }
}
