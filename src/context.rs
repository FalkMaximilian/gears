use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use tokio::task::JoinSet;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use crate::error::{Result, ZdflowError};
use crate::event::{EventPayload, WorkflowEvent};
use crate::metrics;
use crate::traits::{Activity, Storage};

use std::collections::HashMap;

// ── Branch types ──────────────────────────────────────────────────────────

/// The maximum number of sequence IDs reserved per concurrent branch.
///
/// Each call to `concurrently` (and typed variants) reserves this many IDs for
/// each branch it spawns. A branch that performs more than `BRANCH_BUDGET`
/// total operations (activities, sleeps, nested `concurrently` forks) will
/// return [`ZdflowError::BranchBudgetExceeded`].
///
/// **Nested `concurrently` within a branch**: each inner fork claims
/// `1 + n × BRANCH_BUDGET` IDs from the outer branch's budget, so deep
/// nesting is impractical at the default value. Call `concurrently` from the
/// root workflow context when possible.
pub const BRANCH_BUDGET: u32 = 1000;

/// Pinned boxed future returned by a concurrent branch closure.
pub type BranchFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;

/// A boxed branch closure for use with [`WorkflowContext::concurrently`].
///
/// Construct one with the [`branch`] helper function.
pub type BranchFn = Box<dyn FnOnce(WorkflowContext) -> BranchFuture + Send + 'static>;

/// Box a branch closure for use with [`WorkflowContext::concurrently`].
///
/// # Example
///
/// ```rust,ignore
/// use zdflow::{branch, WorkflowContext};
///
/// let results = ctx.concurrently(vec![
///     branch(|ctx| async move {
///         let r1 = ctx.execute_activity("step_a", input_a).await?;
///         ctx.execute_activity("step_b", r1).await
///     }),
///     branch(|ctx| async move {
///         ctx.execute_activity("step_c", input_c).await
///     }),
/// ]).await?;
/// ```
pub fn branch<F, Fut>(f: F) -> BranchFn
where
    F: FnOnce(WorkflowContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Value>> + Send + 'static,
{
    Box::new(move |ctx| Box::pin(f(ctx)))
}

// ── ActivityContext ───────────────────────────────────────────────────────

/// Passed to every activity execution. Contains metadata about the
/// current attempt and optional shared workflow state.
#[derive(Debug, Clone)]
pub struct ActivityContext {
    pub run_id: Uuid,
    pub activity_name: String,
    /// 1-based attempt number (1 = first attempt).
    pub attempt: u32,
    /// Logical call index within this workflow run.
    pub sequence_id: u32,
    /// Shared state set by the workflow via
    /// [`WorkflowContext::set_shared_state`]. `None` if never set.
    pub(crate) shared_state: Option<Value>,
}

impl ActivityContext {
    /// Deserialize the shared workflow state into `T`.
    ///
    /// Returns an error if no shared state has been set or if
    /// deserialization fails.
    pub fn shared_state<T: DeserializeOwned>(&self) -> Result<T> {
        match &self.shared_state {
            Some(v) => Ok(serde_json::from_value(v.clone())?),
            None => Err(ZdflowError::Other(
                "no shared state has been set on this workflow".into(),
            )),
        }
    }
}

// ── WorkflowContext ───────────────────────────────────────────────────────

/// Passed to every workflow execution. Provides `execute_activity`,
/// `sleep` / `sleep_until`, and `concurrently` for parallel branches.
///
/// Internally this type holds the pre-loaded event history and a
/// monotonically-increasing `call_counter`. Every call to
/// `execute_activity`, `sleep*`, `register_cleanup`, or `concurrently`
/// increments the counter and uses it as the `sequence_id` to look up
/// cached results in history.
///
/// **Branch mode**: contexts created by `concurrently` have their own
/// per-branch counter (`branch_counter`) and a fixed `branch_base` offset.
/// All other fields are shared with the root context via `Arc`.
#[derive(Clone)]
pub struct WorkflowContext {
    pub(crate) run_id: Uuid,
    inner: Arc<ContextInner>,
    /// Present only in branch-mode contexts created by `concurrently`.
    /// `None` in the root workflow context (which uses `inner.call_counter`).
    branch_counter: Option<Arc<AtomicU32>>,
    /// Offset added to `branch_counter` to produce global sequence IDs.
    /// Zero in the root context.
    branch_base: u32,
}

struct ContextInner {
    history: Vec<WorkflowEvent>,
    /// Global call counter for the root context. Not used by branch contexts.
    call_counter: AtomicU32,
    /// Global event sequence counter for the run (separate from call_counter).
    event_seq: AtomicU64,
    storage: Arc<dyn Storage>,
    activities: Arc<HashMap<String, Arc<dyn Activity>>>,
    /// Cancellation flag.
    cancelled: AtomicBool,
    /// Notified when the workflow is cancelled.
    cancel_notify: tokio::sync::Notify,
    /// User-set shared state, accessible by all activities in this run.
    shared_state: Mutex<Option<Value>>,
    /// One past the highest sequence_id of any completed history entry
    /// (ActivityCompleted, ActivityErrored, TimerFired, CleanupRegistered).
    /// Used by `is_replaying()`.
    replay_depth: u32,
    /// Timestamp of the WorkflowStarted event, or context-creation time for
    /// brand-new runs. Used by `workflow_start_time()`.
    workflow_start_time: DateTime<Utc>,
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

        // Compute how far into history is already complete so is_replaying()
        // can return true while we're still serving cached results.
        let replay_depth = history
            .iter()
            .filter_map(|e| match &e.payload {
                EventPayload::ActivityCompleted { sequence_id, .. } => Some(sequence_id + 1),
                EventPayload::ActivityErrored { sequence_id, .. } => Some(sequence_id + 1),
                EventPayload::TimerFired { sequence_id } => Some(sequence_id + 1),
                EventPayload::CleanupRegistered { sequence_id, .. } => Some(sequence_id + 1),
                _ => None,
            })
            .max()
            .unwrap_or(0);

        // Use the persisted start time for recovered runs; fall back to now
        // for brand-new runs (WorkflowStarted hasn't been written yet).
        let workflow_start_time = history
            .iter()
            .find_map(|e| {
                if matches!(e.payload, EventPayload::WorkflowStarted { .. }) {
                    Some(e.occurred_at)
                } else {
                    None
                }
            })
            .unwrap_or_else(Utc::now);

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
                shared_state: Mutex::new(None),
                replay_depth,
                workflow_start_time,
            }),
            branch_counter: None,
            branch_base: 0,
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

    /// Returns `true` while the workflow is re-executing history to reconstruct
    /// its state (i.e., the current call will be served from the event cache
    /// without performing real I/O).
    ///
    /// Use this to skip side effects — logging, tracing, metrics — that should
    /// only fire on live execution, not on replay. Do **not** use it to gate
    /// calls to `execute_activity` or `sleep`; those are always safe to call
    /// regardless of replay state.
    pub fn is_replaying(&self) -> bool {
        let current_seq = match &self.branch_counter {
            Some(counter) => self.branch_base + counter.load(Ordering::SeqCst),
            None => self.inner.call_counter.load(Ordering::SeqCst),
        };
        current_seq < self.inner.replay_depth
    }

    /// Returns the UTC timestamp at which this workflow run started.
    ///
    /// On crash-recovery and replay this returns the original start time from
    /// the persisted `WorkflowStarted` event, making it safe to use inside
    /// workflow logic (e.g., computing deadlines relative to the start time).
    pub fn workflow_start_time(&self) -> DateTime<Utc> {
        self.inner.workflow_start_time
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

    // ── Sequence ID helpers ───────────────────────────────────────────────

    /// Returns the next sequence ID, advancing either the branch-local or
    /// global counter depending on whether this is a branch context.
    fn next_sequence_id(&self) -> Result<u32> {
        match &self.branch_counter {
            Some(counter) => {
                let local = counter.fetch_add(1, Ordering::SeqCst);
                if local >= BRANCH_BUDGET {
                    return Err(ZdflowError::BranchBudgetExceeded {
                        budget: BRANCH_BUDGET,
                    });
                }
                Ok(self.branch_base + local)
            }
            None => Ok(self.inner.call_counter.fetch_add(1, Ordering::SeqCst)),
        }
    }

    /// Atomically claims `n` consecutive sequence IDs and returns the base.
    /// Callers use `base + 0`, `base + 1`, ..., `base + n - 1`.
    fn next_n_sequence_ids(&self, n: u32) -> Result<u32> {
        match &self.branch_counter {
            Some(counter) => {
                let local = counter.fetch_add(n, Ordering::SeqCst);
                if local.checked_add(n).map(|sum| sum > BRANCH_BUDGET).unwrap_or(true) {
                    return Err(ZdflowError::BranchBudgetExceeded {
                        budget: BRANCH_BUDGET,
                    });
                }
                Ok(self.branch_base + local)
            }
            None => Ok(self.inner.call_counter.fetch_add(n, Ordering::SeqCst)),
        }
    }

    /// Create a branch-mode context with its own local counter starting at `branch_base`.
    /// Shares history, storage, cancellation, activities, and shared state with the parent.
    fn make_branch_ctx(&self, branch_base: u32) -> WorkflowContext {
        WorkflowContext {
            run_id: self.run_id,
            inner: self.inner.clone(),
            branch_counter: Some(Arc::new(AtomicU32::new(0))),
            branch_base,
        }
    }

    /// Allocate sequence IDs for a concurrent fork and persist the marker event
    /// if this is a live execution (not a replay). Returns `(fork_seq, block_base)`.
    async fn alloc_concurrent_fork(&self, num_branches: u32) -> Result<(u32, u32)> {
        let fork_seq = self.next_sequence_id()?;
        let block_base = self.next_n_sequence_ids(num_branches * BRANCH_BUDGET)?;

        let already_started = self.inner.history.iter().any(|e| {
            matches!(
                &e.payload,
                EventPayload::ConcurrentBranchesStarted { sequence_id: sid, .. } if *sid == fork_seq
            )
        });
        if !already_started {
            self.append_event(EventPayload::ConcurrentBranchesStarted {
                sequence_id: fork_seq,
                num_branches,
                branch_budget: BRANCH_BUDGET,
            })
            .await?;
        }

        Ok((fork_seq, block_base))
    }

    // ── shared state ─────────────────────────────────────────────────────

    /// Store a value as shared workflow state.
    ///
    /// Once set, every [`ActivityContext`] created for subsequent activity
    /// executions in this run will carry a copy of this state. Activities
    /// can retrieve it with [`ActivityContext::shared_state`].
    ///
    /// This is **not** persisted as an event — it lives only in memory.
    /// Because the workflow function is re-executed from the top on replay,
    /// the state is deterministically re-established before any activities
    /// run.
    ///
    /// Typical pattern: set shared state once at the beginning of a
    /// workflow so every activity has access to common data (trace IDs,
    /// user context, etc.) without threading it through every call.
    pub fn set_shared_state<T: Serialize>(&self, state: &T) -> Result<()> {
        let value = serde_json::to_value(state)?;
        *self.inner.shared_state.lock().unwrap() = Some(value);
        Ok(())
    }

    /// Retrieve the shared workflow state, deserialized into `T`.
    ///
    /// Returns an error if no shared state has been set.
    pub fn shared_state<T: DeserializeOwned>(&self) -> Result<T> {
        let guard = self.inner.shared_state.lock().unwrap();
        match &*guard {
            Some(v) => Ok(serde_json::from_value(v.clone())?),
            None => Err(ZdflowError::Other(
                "no shared state has been set on this workflow".into(),
            )),
        }
    }

    /// Read the current shared state as a raw `Value`.
    pub(crate) fn shared_state_value(&self) -> Option<Value> {
        self.inner.shared_state.lock().unwrap().clone()
    }

    // ── typed convenience methods ────────────────────────────────────────

    /// Execute an activity, automatically serializing the input and
    /// deserializing the output.
    ///
    /// This is a typed wrapper around [`execute_activity`](Self::execute_activity).
    pub async fn execute_activity_typed<I, O>(&self, activity_name: &str, input: &I) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let value = serde_json::to_value(input)?;
        let result = self.execute_activity(activity_name, value).await?;
        Ok(serde_json::from_value(result)?)
    }

    /// Execute multiple activities in parallel, deserializing all results
    /// into the same type `O`.
    ///
    /// This is a typed wrapper around
    /// [`execute_activities_parallel`](Self::execute_activities_parallel).
    pub async fn execute_activities_parallel_typed<O>(
        &self,
        activities: Vec<(&str, Value)>,
    ) -> Result<Vec<O>>
    where
        O: DeserializeOwned,
    {
        let results = self.execute_activities_parallel(activities).await?;
        results
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(Into::into))
            .collect()
    }

    /// Execute multiple activities in parallel and collect *all* results,
    /// including per-activity failures. Unlike
    /// [`execute_activities_parallel`](Self::execute_activities_parallel),
    /// this does not short-circuit on the first error — every activity runs
    /// to completion (or exhausted retries).
    ///
    /// Returns `Err` only for unexpected task panics (`TaskPanicked`).
    /// Per-activity failures are returned as `Err` items inside the `Vec`.
    pub async fn try_execute_activities_parallel(
        &self,
        activities: Vec<(&str, Value)>,
    ) -> Result<Vec<Result<Value>>> {
        let n = activities.len();
        let base = self.next_n_sequence_ids(n as u32)?;

        let mut set: JoinSet<(usize, Result<Value>)> = JoinSet::new();
        for (i, (activity_name, input)) in activities.into_iter().enumerate() {
            let sequence_id = base + i as u32;
            let activity = self
                .inner
                .activities
                .get(activity_name)
                .ok_or_else(|| ZdflowError::ActivityNotFound(activity_name.to_string()))?
                .clone();
            let ctx = self.clone();
            set.spawn(async move {
                let result = ctx.execute_activity_inner(&*activity, input, sequence_id).await;
                (i, result)
            });
        }

        let mut results: Vec<Option<Result<Value>>> = (0..n).map(|_| None).collect();
        while let Some(join_res) = set.join_next().await {
            match join_res {
                Ok((i, result)) => results[i] = Some(result),
                Err(e) => return Err(ZdflowError::TaskPanicked(e.to_string())),
            }
        }
        Ok(results.into_iter().map(|r| r.unwrap()).collect())
    }

    /// Execute multiple activities in parallel and collect all results,
    /// deserializing successes into `O`. Per-activity failures are returned
    /// as `Err` items inside the `Vec`.
    ///
    /// This is a typed wrapper around
    /// [`try_execute_activities_parallel`](Self::try_execute_activities_parallel).
    pub async fn try_execute_activities_parallel_typed<O: DeserializeOwned>(
        &self,
        activities: Vec<(&str, Value)>,
    ) -> Result<Vec<Result<O>>> {
        let results = self.try_execute_activities_parallel(activities).await?;
        Ok(results
            .into_iter()
            .map(|r| r.and_then(|v| serde_json::from_value(v).map_err(Into::into)))
            .collect())
    }

    /// Execute exactly 2 activities in parallel, returning a typed tuple.
    ///
    /// Sequence IDs are pre-allocated to ensure deterministic replay.
    /// If either activity fails, the other is aborted and the error is returned.
    pub async fn execute_activities_parallel_2<O1, O2>(
        &self,
        a1: (&str, Value),
        a2: (&str, Value),
    ) -> Result<(O1, O2)>
    where
        O1: DeserializeOwned,
        O2: DeserializeOwned,
    {
        let mut r = self.execute_activities_parallel(vec![a1, a2]).await?;
        let v2 = r.pop().unwrap();
        let v1 = r.pop().unwrap();
        Ok((serde_json::from_value(v1)?, serde_json::from_value(v2)?))
    }

    /// Execute exactly 3 activities in parallel, returning a typed tuple.
    ///
    /// Sequence IDs are pre-allocated to ensure deterministic replay.
    /// If any activity fails, the others are aborted and the error is returned.
    pub async fn execute_activities_parallel_3<O1, O2, O3>(
        &self,
        a1: (&str, Value),
        a2: (&str, Value),
        a3: (&str, Value),
    ) -> Result<(O1, O2, O3)>
    where
        O1: DeserializeOwned,
        O2: DeserializeOwned,
        O3: DeserializeOwned,
    {
        let mut r = self.execute_activities_parallel(vec![a1, a2, a3]).await?;
        let v3 = r.pop().unwrap();
        let v2 = r.pop().unwrap();
        let v1 = r.pop().unwrap();
        Ok((
            serde_json::from_value(v1)?,
            serde_json::from_value(v2)?,
            serde_json::from_value(v3)?,
        ))
    }

    /// Execute exactly 4 activities in parallel, returning a typed tuple.
    ///
    /// Sequence IDs are pre-allocated to ensure deterministic replay.
    /// If any activity fails, the others are aborted and the error is returned.
    pub async fn execute_activities_parallel_4<O1, O2, O3, O4>(
        &self,
        a1: (&str, Value),
        a2: (&str, Value),
        a3: (&str, Value),
        a4: (&str, Value),
    ) -> Result<(O1, O2, O3, O4)>
    where
        O1: DeserializeOwned,
        O2: DeserializeOwned,
        O3: DeserializeOwned,
        O4: DeserializeOwned,
    {
        let mut r = self.execute_activities_parallel(vec![a1, a2, a3, a4]).await?;
        let v4 = r.pop().unwrap();
        let v3 = r.pop().unwrap();
        let v2 = r.pop().unwrap();
        let v1 = r.pop().unwrap();
        Ok((
            serde_json::from_value(v1)?,
            serde_json::from_value(v2)?,
            serde_json::from_value(v3)?,
            serde_json::from_value(v4)?,
        ))
    }

    /// Register a cleanup activity with a typed input.
    ///
    /// This is a typed wrapper around [`register_cleanup`](Self::register_cleanup).
    pub async fn register_cleanup_typed<I: Serialize>(
        &self,
        activity_name: &str,
        input: &I,
    ) -> Result<()> {
        let value = serde_json::to_value(input)?;
        self.register_cleanup(activity_name, value).await
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
        let sequence_id = self.next_sequence_id()?;

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
                shared_state: self.shared_state_value(),
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
                                    self.append_event(EventPayload::ActivityAttemptTimedOut {
                                        sequence_id,
                                        attempt,
                                        timeout_ms: duration.as_millis() as u64,
                                    })
                                    .await?;
                                    Err(ZdflowError::ActivityTimedOut {
                                        activity_name: activity.name().to_string(),
                                        timeout: duration,
                                    })
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
    /// If any activity fails, the remaining in-flight activities are aborted
    /// and the first error is returned. Use
    /// [`try_execute_activities_parallel`](Self::try_execute_activities_parallel)
    /// to collect all results including failures.
    pub async fn execute_activities_parallel(
        &self,
        activities: Vec<(&str, Value)>,
    ) -> Result<Vec<Value>> {
        let n = activities.len();
        let base = self.next_n_sequence_ids(n as u32)?;

        let mut set: JoinSet<(usize, Result<Value>)> = JoinSet::new();
        for (i, (activity_name, input)) in activities.into_iter().enumerate() {
            let sequence_id = base + i as u32;
            let activity = self
                .inner
                .activities
                .get(activity_name)
                .ok_or_else(|| ZdflowError::ActivityNotFound(activity_name.to_string()))?
                .clone();
            let ctx = self.clone();
            set.spawn(async move {
                let result = ctx.execute_activity_inner(&*activity, input, sequence_id).await;
                (i, result)
            });
        }

        let mut results = vec![Value::Null; n];
        while let Some(join_res) = set.join_next().await {
            match join_res {
                Ok((i, Ok(v))) => results[i] = v,
                Ok((_, Err(e))) => {
                    set.abort_all();
                    return Err(e);
                }
                Err(e) => {
                    set.abort_all();
                    return Err(ZdflowError::TaskPanicked(e.to_string()));
                }
            }
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

        let sequence_id = self.next_sequence_id()?;

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
                    return Err(ZdflowError::VersionConflict {
                        change_id: change_id.to_string(),
                        stored: *version,
                        min: min_version,
                        max: max_version,
                    });
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

    // ── cleanup ───────────────────────────────────────────────────────────

    /// Register an activity to be executed as a cleanup (finalizer) for this
    /// workflow run.
    ///
    /// # When cleanups run
    ///
    /// Cleanups are executed by the `WorkerTask` after `Workflow::run` returns,
    /// **before** the terminal event is written to the log:
    ///
    /// | Workflow outcome | Cleanups run? |
    /// |---|---|
    /// | `Ok(output)` — completed successfully | **Yes** |
    /// | `Err(ZdflowError::Cancelled)` — cancelled | **Yes** |
    /// | `Err(other)` — failed | **No** |
    ///
    /// Workflow failures are excluded because they may be transient (e.g. an
    /// external API was temporarily unavailable). Cleanup code typically frees
    /// or cancels resources that were intentionally acquired, which is only
    /// appropriate when the workflow reached a terminal state on purpose.
    ///
    /// # Ordering
    ///
    /// Cleanups run in **reverse registration order** (LIFO), like a stack of
    /// deferred functions. This mirrors the typical resource ownership
    /// relationship: the last resource acquired should be the first released.
    ///
    /// # Failure tolerance
    ///
    /// A cleanup is allowed to fail. If the cleanup activity returns `Err`,
    /// or if the named activity is not in the registry, a `CleanupFailed` event
    /// is written to the log and execution continues with the next cleanup. The
    /// workflow's final status (`Completed` or `Cancelled`) is not affected.
    ///
    /// # Crash safety
    ///
    /// Because cleanups are run before the terminal event, a crash during
    /// cleanup leaves the run in `Running` status. On the next engine restart,
    /// the run is recovered and replayed. Cleanups that have already written a
    /// `CleanupCompleted` or `CleanupFailed` event are skipped; only the
    /// remaining ones are re-executed.
    ///
    /// # Replay
    ///
    /// `register_cleanup` consumes a `sequence_id` from the same call counter
    /// as `execute_activity` and `sleep`. On replay, the matching
    /// `CleanupRegistered` event is found in history at that counter position
    /// and this method returns immediately without re-persisting the event.
    /// The counter advances normally, keeping all subsequent calls aligned.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Allocate an external resource.
    /// let alloc = ctx.execute_activity("allocate_vm", input.clone()).await?;
    ///
    /// // Register cleanup immediately — runs even if the workflow is cancelled
    /// // or completes successfully.
    /// ctx.register_cleanup("terminate_vm", json!({ "id": alloc["vm_id"] })).await?;
    ///
    /// // Continue with the rest of the workflow. If this sleep is interrupted
    /// // by a cancellation, "terminate_vm" will still be called.
    /// ctx.sleep(Duration::from_secs(300)).await?;
    /// ```
    pub async fn register_cleanup(&self, activity_name: &str, input: Value) -> Result<()> {
        let sequence_id = self.next_sequence_id()?;

        // Replay path: if CleanupRegistered at this sequence_id is already in
        // history, return without persisting again.
        for event in &self.inner.history {
            if let EventPayload::CleanupRegistered {
                sequence_id: sid, ..
            } = &event.payload
                && *sid == sequence_id
            {
                return Ok(());
            }
        }

        // Live path: persist the registration.
        self.append_event(EventPayload::CleanupRegistered {
            sequence_id,
            activity_name: activity_name.to_string(),
            input,
        })
        .await
    }

    // ── concurrently ─────────────────────────────────────────────────────

    /// Run multiple workflow branches concurrently and return all results.
    ///
    /// Each branch is an async closure that receives its own
    /// [`WorkflowContext`] and can execute any workflow primitives:
    /// activities, sleeps, nested `concurrently` calls, etc. All branches
    /// start immediately and run in parallel via tokio tasks.
    ///
    /// **Fail-fast**: if any branch returns an error, all remaining branches
    /// are aborted and the error is propagated. Use
    /// [`try_concurrently`](Self::try_concurrently) to collect all results
    /// including per-branch failures.
    ///
    /// Use the [`branch`] helper to box closures for this method.
    ///
    /// # Deterministic replay
    ///
    /// Each branch is allocated a contiguous range of `BRANCH_BUDGET`
    /// sequence IDs. On replay, the same ranges are reconstructed from the
    /// same counter state, so every activity in every branch finds its
    /// cached result in history.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use zdflow::branch;
    ///
    /// // Copy a file, then in parallel: start a job AND make an API call.
    /// let copy_result = ctx.execute_activity("copy_file", input).await?;
    ///
    /// let results = ctx.concurrently(vec![
    ///     branch(|ctx| async move {
    ///         let job = ctx.execute_activity("start_job", copy_result.clone()).await?;
    ///         ctx.execute_activity("poll_job", job).await
    ///     }),
    ///     branch(|ctx| async move {
    ///         let resp = ctx.execute_activity("make_api_call", json!({})).await?;
    ///         ctx.execute_activity("process_response", resp).await
    ///     }),
    /// ]).await?;
    /// ```
    pub async fn concurrently(&self, branches: Vec<BranchFn>) -> Result<Vec<Value>> {
        let n = branches.len();
        if n == 0 {
            return Ok(vec![]);
        }
        self.check_cancelled()?;

        let (_fork_seq, block_base) = self.alloc_concurrent_fork(n as u32).await?;

        let mut set: JoinSet<(usize, Result<Value>)> = JoinSet::new();
        for (i, branch_fn) in branches.into_iter().enumerate() {
            let branch_ctx = self.make_branch_ctx(block_base + i as u32 * BRANCH_BUDGET);
            set.spawn(async move {
                let result = branch_fn(branch_ctx).await;
                (i, result)
            });
        }

        let mut results = vec![Value::Null; n];
        while let Some(join_res) = set.join_next().await {
            match join_res {
                Ok((i, Ok(v))) => results[i] = v,
                Ok((_, Err(e))) => {
                    set.abort_all();
                    return Err(e);
                }
                Err(e) => {
                    set.abort_all();
                    return Err(ZdflowError::TaskPanicked(e.to_string()));
                }
            }
        }
        Ok(results)
    }

    /// Run multiple workflow branches concurrently and collect *all* results,
    /// including per-branch failures.
    ///
    /// Unlike [`concurrently`](Self::concurrently), this does not
    /// short-circuit on the first error — every branch runs to completion.
    /// Returns `Err` only for unexpected task panics (`TaskPanicked`).
    /// Per-branch failures are `Err` items inside the returned `Vec`.
    ///
    /// Use the [`branch`] helper to box closures for this method.
    pub async fn try_concurrently(&self, branches: Vec<BranchFn>) -> Result<Vec<Result<Value>>> {
        let n = branches.len();
        if n == 0 {
            return Ok(vec![]);
        }
        self.check_cancelled()?;

        let (_fork_seq, block_base) = self.alloc_concurrent_fork(n as u32).await?;

        let mut set: JoinSet<(usize, Result<Value>)> = JoinSet::new();
        for (i, branch_fn) in branches.into_iter().enumerate() {
            let branch_ctx = self.make_branch_ctx(block_base + i as u32 * BRANCH_BUDGET);
            set.spawn(async move {
                let result = branch_fn(branch_ctx).await;
                (i, result)
            });
        }

        let mut results: Vec<Option<Result<Value>>> = (0..n).map(|_| None).collect();
        while let Some(join_res) = set.join_next().await {
            match join_res {
                Ok((i, result)) => results[i] = Some(result),
                Err(e) => return Err(ZdflowError::TaskPanicked(e.to_string())),
            }
        }
        Ok(results.into_iter().map(|r| r.unwrap()).collect())
    }

    /// Run exactly 2 concurrent branches, returning a typed tuple.
    ///
    /// Each branch closure receives its own [`WorkflowContext`] and may
    /// return a different output type. If either branch fails, the other is
    /// aborted and the error is returned.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let (job_result, api_result): (JobOutput, ApiOutput) = ctx
    ///     .concurrently_2(
    ///         |ctx| async move { ctx.execute_activity_typed("start_job", &input).await },
    ///         |ctx| async move { ctx.execute_activity_typed("make_api_call", &req).await },
    ///     )
    ///     .await?;
    /// ```
    pub async fn concurrently_2<A, B, FA, FB, FutA, FutB>(
        &self,
        branch_a: FA,
        branch_b: FB,
    ) -> Result<(A, B)>
    where
        FA: FnOnce(WorkflowContext) -> FutA + Send + 'static,
        FutA: Future<Output = Result<A>> + Send + 'static,
        FB: FnOnce(WorkflowContext) -> FutB + Send + 'static,
        FutB: Future<Output = Result<B>> + Send + 'static,
        A: Send + 'static,
        B: Send + 'static,
    {
        self.check_cancelled()?;

        let (_fork_seq, block_base) = self.alloc_concurrent_fork(2).await?;

        let ctx_a = self.make_branch_ctx(block_base);
        let ctx_b = self.make_branch_ctx(block_base + BRANCH_BUDGET);

        let handle_a = tokio::spawn(branch_a(ctx_a));
        let handle_b = tokio::spawn(branch_b(ctx_b));

        let res_a = match handle_a.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                handle_b.abort();
                return Err(e);
            }
            Err(e) => {
                handle_b.abort();
                return Err(ZdflowError::TaskPanicked(e.to_string()));
            }
        };
        let res_b = match handle_b.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(ZdflowError::TaskPanicked(e.to_string())),
        };

        Ok((res_a, res_b))
    }

    /// Run exactly 3 concurrent branches, returning a typed tuple.
    ///
    /// If any branch fails, the others are aborted and the error is returned.
    pub async fn concurrently_3<A, B, C, FA, FB, FC, FutA, FutB, FutC>(
        &self,
        branch_a: FA,
        branch_b: FB,
        branch_c: FC,
    ) -> Result<(A, B, C)>
    where
        FA: FnOnce(WorkflowContext) -> FutA + Send + 'static,
        FutA: Future<Output = Result<A>> + Send + 'static,
        FB: FnOnce(WorkflowContext) -> FutB + Send + 'static,
        FutB: Future<Output = Result<B>> + Send + 'static,
        FC: FnOnce(WorkflowContext) -> FutC + Send + 'static,
        FutC: Future<Output = Result<C>> + Send + 'static,
        A: Send + 'static,
        B: Send + 'static,
        C: Send + 'static,
    {
        self.check_cancelled()?;

        let (_fork_seq, block_base) = self.alloc_concurrent_fork(3).await?;

        let ctx_a = self.make_branch_ctx(block_base);
        let ctx_b = self.make_branch_ctx(block_base + BRANCH_BUDGET);
        let ctx_c = self.make_branch_ctx(block_base + 2 * BRANCH_BUDGET);

        let handle_a = tokio::spawn(branch_a(ctx_a));
        let handle_b = tokio::spawn(branch_b(ctx_b));
        let handle_c = tokio::spawn(branch_c(ctx_c));

        let res_a = match handle_a.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                handle_b.abort();
                handle_c.abort();
                return Err(e);
            }
            Err(e) => {
                handle_b.abort();
                handle_c.abort();
                return Err(ZdflowError::TaskPanicked(e.to_string()));
            }
        };
        let res_b = match handle_b.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                handle_c.abort();
                return Err(e);
            }
            Err(e) => {
                handle_c.abort();
                return Err(ZdflowError::TaskPanicked(e.to_string()));
            }
        };
        let res_c = match handle_c.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(ZdflowError::TaskPanicked(e.to_string())),
        };

        Ok((res_a, res_b, res_c))
    }

    /// Run exactly 4 concurrent branches, returning a typed tuple.
    ///
    /// If any branch fails, the others are aborted and the error is returned.
    pub async fn concurrently_4<A, B, C, D, FA, FB, FC, FD, FutA, FutB, FutC, FutD>(
        &self,
        branch_a: FA,
        branch_b: FB,
        branch_c: FC,
        branch_d: FD,
    ) -> Result<(A, B, C, D)>
    where
        FA: FnOnce(WorkflowContext) -> FutA + Send + 'static,
        FutA: Future<Output = Result<A>> + Send + 'static,
        FB: FnOnce(WorkflowContext) -> FutB + Send + 'static,
        FutB: Future<Output = Result<B>> + Send + 'static,
        FC: FnOnce(WorkflowContext) -> FutC + Send + 'static,
        FutC: Future<Output = Result<C>> + Send + 'static,
        FD: FnOnce(WorkflowContext) -> FutD + Send + 'static,
        FutD: Future<Output = Result<D>> + Send + 'static,
        A: Send + 'static,
        B: Send + 'static,
        C: Send + 'static,
        D: Send + 'static,
    {
        self.check_cancelled()?;

        let (_fork_seq, block_base) = self.alloc_concurrent_fork(4).await?;

        let ctx_a = self.make_branch_ctx(block_base);
        let ctx_b = self.make_branch_ctx(block_base + BRANCH_BUDGET);
        let ctx_c = self.make_branch_ctx(block_base + 2 * BRANCH_BUDGET);
        let ctx_d = self.make_branch_ctx(block_base + 3 * BRANCH_BUDGET);

        let handle_a = tokio::spawn(branch_a(ctx_a));
        let handle_b = tokio::spawn(branch_b(ctx_b));
        let handle_c = tokio::spawn(branch_c(ctx_c));
        let handle_d = tokio::spawn(branch_d(ctx_d));

        let res_a = match handle_a.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                handle_b.abort();
                handle_c.abort();
                handle_d.abort();
                return Err(e);
            }
            Err(e) => {
                handle_b.abort();
                handle_c.abort();
                handle_d.abort();
                return Err(ZdflowError::TaskPanicked(e.to_string()));
            }
        };
        let res_b = match handle_b.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                handle_c.abort();
                handle_d.abort();
                return Err(e);
            }
            Err(e) => {
                handle_c.abort();
                handle_d.abort();
                return Err(ZdflowError::TaskPanicked(e.to_string()));
            }
        };
        let res_c = match handle_c.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                handle_d.abort();
                return Err(e);
            }
            Err(e) => {
                handle_d.abort();
                return Err(ZdflowError::TaskPanicked(e.to_string()));
            }
        };
        let res_d = match handle_d.await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(ZdflowError::TaskPanicked(e.to_string())),
        };

        Ok((res_a, res_b, res_c, res_d))
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
