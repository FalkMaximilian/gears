use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{Result, ZdflowError};
use crate::event::{EventPayload, WorkflowEvent};
use crate::metrics;
use crate::traits::{Activity, Storage};

use std::collections::HashMap;

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
    activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    /// Cancellation flag.
    cancelled: AtomicBool,
    /// Notified when the workflow is cancelled.
    cancel_notify: tokio::sync::Notify,
}

impl WorkflowContext {
    pub(crate) fn new(
        run_id: Uuid,
        history: Vec<WorkflowEvent>,
        storage: Arc<dyn Storage>,
        activities: Arc<HashMap<String, Arc<dyn Activity>>>,
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
                activities,
                cancelled: AtomicBool::new(false),
                cancel_notify: tokio::sync::Notify::new(),
            }),
        }
    }

    /// The unique ID of this workflow run.
    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Returns `true` if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    /// Request cancellation. Context methods will return
    /// `ZdflowError::Cancelled` at the next yield point.
    pub(crate) fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::SeqCst);
        self.inner.cancel_notify.notify_waiters();
    }

    fn check_cancelled(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(ZdflowError::Cancelled)
        } else {
            Ok(())
        }
    }

    // ── execute_activity ─────────────────────────────────────────────────

    /// Execute an activity by name, returning its output.
    ///
    /// The activity must have been registered on the engine builder via
    /// `register_activity`. If this `sequence_id` already has an
    /// `ActivityCompleted` event in the history, the cached result is
    /// returned immediately (replay). Otherwise the activity is executed
    /// live with automatic retry.
    pub async fn execute_activity(&self, activity_name: &str, input: Value) -> Result<Value> {
        let sequence_id = self.inner.call_counter.fetch_add(1, Ordering::SeqCst);

        let activity = self
            .inner
            .activities
            .get(activity_name)
            .ok_or_else(|| ZdflowError::ActivityNotFound(activity_name.to_string()))?
            .clone();

        self.execute_activity_inner(&*activity, input, sequence_id)
            .await
    }

    /// Core activity execution logic with an explicit sequence_id.
    /// Used by both `execute_activity` (single) and
    /// `execute_activities_parallel` (fan-out).
    async fn execute_activity_inner(
        &self,
        activity: &dyn Activity,
        input: Value,
        sequence_id: u32,
    ) -> Result<Value> {
        self.check_cancelled()?;

        // ── Replay path ───────────────────────────────────────────────
        for event in &self.inner.history {
            if let EventPayload::ActivityCompleted {
                sequence_id: sid,
                output,
            } = &event.payload
                && *sid == sequence_id
            {
                tracing::debug!(
                    run_id = %self.run_id,
                    activity = activity.name(),
                    sequence_id,
                    "replaying activity result from history"
                );
                return Ok(output.clone());
            }
            // If all retries previously exhausted, replay the error too.
            if let EventPayload::ActivityErrored {
                sequence_id: sid,
                error,
            } = &event.payload
                && *sid == sequence_id
            {
                return Err(ZdflowError::ActivityFailed(error.clone()));
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
        let timeout = activity.timeout();
        let mut last_error = String::new();
        let activity_start = std::time::Instant::now();
        metrics::inc_activity_started(activity.name());

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

            let exec_future = activity.execute(ctx, input.clone());
            let cancel_notify = &self.inner.cancel_notify;

            let exec_result = match timeout {
                Some(duration) => {
                    tokio::select! {
                        biased;
                        _ = cancel_notify.notified(), if !self.is_cancelled() => {
                            return Err(ZdflowError::Cancelled);
                        }
                        result = tokio::time::timeout(duration, exec_future) => {
                            match result {
                                Ok(inner) => inner,
                                Err(_elapsed) => {
                                    let msg = format!(
                                        "activity '{}' timed out after {:?}",
                                        activity.name(),
                                        duration
                                    );
                                    self.append_event(EventPayload::ActivityAttemptTimedOut {
                                        sequence_id,
                                        attempt,
                                        timeout_ms: duration.as_millis() as u64,
                                    })
                                    .await?;
                                    Err(ZdflowError::Other(msg))
                                }
                            }
                        }
                    }
                }
                None => {
                    tokio::select! {
                        biased;
                        _ = cancel_notify.notified(), if !self.is_cancelled() => {
                            return Err(ZdflowError::Cancelled);
                        }
                        result = exec_future => result,
                    }
                }
            };

            match exec_result {
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
                    metrics::inc_activity_completed(activity.name());
                    metrics::record_activity_duration(
                        activity.name(),
                        activity_start.elapsed().as_secs_f64(),
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
                        metrics::inc_activity_retries(activity.name());
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

    /// Execute multiple activities in parallel, returning results in the
    /// same order as the input. Sequence IDs are pre-allocated to ensure
    /// deterministic replay.
    ///
    /// If any activity fails, remaining in-flight activities are cancelled
    /// and the first error is returned.
    pub async fn execute_activities_parallel(
        &self,
        activities: Vec<(&str, Value)>,
    ) -> Result<Vec<Value>> {
        let n = activities.len() as u32;
        let base = self.inner.call_counter.fetch_add(n, Ordering::SeqCst);

        let mut handles = Vec::with_capacity(activities.len());
        for (i, (activity_name, input)) in activities.into_iter().enumerate() {
            let sequence_id = base + i as u32;
            let activity = self
                .inner
                .activities
                .get(activity_name)
                .ok_or_else(|| ZdflowError::ActivityNotFound(activity_name.to_string()))?
                .clone();
            let ctx = self.clone();
            handles.push(tokio::spawn(async move {
                ctx.execute_activity_inner(&*activity, input, sequence_id)
                    .await
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            let result = handle
                .await
                .map_err(|e| ZdflowError::Other(format!("task join error: {e}")))?;
            results.push(result?);
        }
        Ok(results)
    }

    // ── sleep / sleep_until ───────────────────────────────────────────────

    /// Durably suspend the workflow for at least `duration`.
    pub async fn sleep(&self, duration: Duration) -> Result<()> {
        let wake_at = Utc::now()
            + chrono::Duration::from_std(duration)
                .map_err(|e| ZdflowError::Other(e.to_string()))?;
        self.sleep_until(wake_at).await
    }

    /// Durably suspend the workflow until an absolute UTC timestamp.
    ///
    /// On replay (if `TimerFired` is already in history) this returns
    /// immediately. On crash-recovery (if `TimerStarted` exists but not
    /// `TimerFired`) the remaining duration is recalculated.
    pub async fn sleep_until(&self, wake_at: DateTime<Utc>) -> Result<()> {
        self.check_cancelled()?;

        let sequence_id = self.inner.call_counter.fetch_add(1, Ordering::SeqCst);

        // ── Replay: timer already fired ───────────────────────────────
        for event in &self.inner.history {
            if let EventPayload::TimerFired { sequence_id: sid } = &event.payload
                && *sid == sequence_id
            {
                tracing::debug!(
                    run_id = %self.run_id,
                    sequence_id,
                    "replaying timer from history"
                );
                return Ok(());
            }
        }

        // ── Crash recovery: timer was started but not fired ───────────
        let mut effective_wake_at = wake_at;
        for event in &self.inner.history {
            if let EventPayload::TimerStarted {
                sequence_id: sid,
                wake_at: recorded_wake_at,
            } = &event.payload
                && *sid == sequence_id
            {
                // Use the originally-recorded wake_at, not the one
                // computed from `duration` (which would extend the sleep).
                effective_wake_at = *recorded_wake_at;
                break;
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
            let remaining = (effective_wake_at - now).to_std().unwrap_or(Duration::ZERO);
            tracing::debug!(
                run_id = %self.run_id,
                sequence_id,
                remaining_secs = remaining.as_secs_f64(),
                "sleeping"
            );
            tokio::select! {
                biased;
                _ = self.inner.cancel_notify.notified(), if !self.is_cancelled() => {
                    return Err(ZdflowError::Cancelled);
                }
                _ = tokio::time::sleep(remaining) => {}
            }
        }

        self.append_event(EventPayload::TimerFired { sequence_id })
            .await?;
        tracing::debug!(run_id = %self.run_id, sequence_id, "timer fired");
        Ok(())
    }

    // ── versioning ─────────────────────────────────────────────────────────

    /// Return a version number for the given `change_id`, enabling safe
    /// workflow code changes while runs are in-flight.
    ///
    /// On a fresh execution, `max_version` is stored and returned. On
    /// replay, the previously stored version is returned. If the stored
    /// version falls outside `[min_version, max_version]`, an error is
    /// returned (the code has drifted too far from the persisted history).
    ///
    /// Unlike `execute_activity` and `sleep`, this does **not** consume a
    /// sequence_id — the marker is keyed by `change_id` instead, so
    /// inserting a version check does not shift other calls.
    pub async fn get_version(
        &self,
        change_id: &str,
        min_version: u32,
        max_version: u32,
    ) -> Result<u32> {
        // Replay: look for an existing marker.
        for event in &self.inner.history {
            if let EventPayload::VersionMarker {
                change_id: cid,
                version,
            } = &event.payload
                && cid == change_id
            {
                if *version < min_version || *version > max_version {
                    return Err(ZdflowError::Other(format!(
                        "version incompatibility for '{}': stored={} range=[{}, {}]",
                        change_id, version, min_version, max_version
                    )));
                }
                return Ok(*version);
            }
        }

        // New execution: persist max_version.
        self.append_event(EventPayload::VersionMarker {
            change_id: change_id.to_string(),
            version: max_version,
        })
        .await?;
        Ok(max_version)
    }

    // ── helpers ───────────────────────────────────────────────────────────

    pub(crate) async fn append_event(&self, payload: EventPayload) -> Result<()> {
        let sequence = self.inner.event_seq.fetch_add(1, Ordering::SeqCst);
        let event = WorkflowEvent {
            sequence,
            occurred_at: Utc::now(),
            payload,
        };
        self.inner.storage.append_event(self.run_id, &event).await
    }
}
