# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                        # Debug build
cargo build --release              # Release build
cargo test                         # Run all tests
cargo test <test_name>             # Run a single test
cargo run --bin gears-demo         # Run the demo HTTP server (port 3000)
cargo run --bin gears-ctl          # Run the TUI controller (connects to localhost:3000)
cargo run --bin gears-ctl -- --url http://host:3000  # Custom engine URL

# gears-demo endpoints
# http://localhost:3000/swagger-ui  — interactive OpenAPI browser (Swagger UI)
# http://localhost:3000/api/openapi.json  — raw OpenAPI 3.1 spec

# gears-demo configuration
# Settings are read from gears-demo.toml (optional) then overridden by GEARS_* env vars.
# Available settings and their defaults:
#   log_level="gears=debug,info"   GEARS_LOG_LEVEL=...
#   database_url="gears-demo.db"   GEARS_DATABASE_URL=...
#   port=3000                      GEARS_PORT=...
#   swagger_ui=true                GEARS_SWAGGER_UI=...
#   max_concurrent_workflows=50    GEARS_MAX_CONCURRENT_WORKFLOWS=...
#   retention_days=(unset)         GEARS_RETENTION_DAYS=...  # omit to keep runs forever
#
# Example: GEARS_PORT=8080 GEARS_SWAGGER_UI=false cargo run --bin gears-demo

cargo fmt                          # Format code
cargo clippy                       # Lint
```

## Architecture

gears is a **durable workflow execution engine** (similar to Temporal.io) built in Rust. The core design is event sourcing + deterministic replay: all workflow state transitions are persisted to SQLite, enabling crash recovery by replaying history.

### Execution Model

```
engine.start_workflow(name, input)
  → persists WorkflowStarted event
  → spawns WorkerTask (bounded by semaphore for concurrency limit)
    → calls Workflow::run(WorkflowContext, input)
      → ctx.execute_activity("activity_name", input)
          → on replay: returns cached result from event history
          → on new: persist ActivityScheduled → retry loop (with optional timeout) → persist ActivityCompleted/ActivityErrored
      → ctx.execute_activities_parallel(vec![("a", input), ("b", input)])
          → pre-allocates sequence_ids → runs all branches concurrently → each branch replays independently
      → ctx.concurrently(vec![branch(|ctx| async { ... }), branch(|ctx| async { ... })])
          → claims 1 fork-marker sequence_id + n × BRANCH_BUDGET (1000) IDs from main counter
          → persists ConcurrentBranchesStarted event (skipped on replay if already present)
          → creates n BranchContexts, each with its own local counter + fixed branch_base offset
          → runs all branches as concurrent tokio tasks (each receives its own BranchContext)
          → each branch uses branch_base + local_counter as sequence_id for its activities
          → fail-fast: first branch error aborts the rest; try_concurrently collects all results
          → concurrently_2/3/4 typed variants return tuples with heterogeneous output types
          → on replay: same counter state → same branch_base values → branches find cached history entries
      → ctx.sleep(duration)
          → on replay: returns immediately if TimerFired in history
          → on new: persist TimerStarted → sleep → persist TimerFired
          → on crash-recovery: recalculate remaining time from TimerStarted timestamp
      → ctx.get_version(change_id, max) / ctx.changed(change_id)
          → on replay: returns stored version from VersionMarker event
          → on new: persists max_version, returns it
      → ctx.register_cleanup("activity_name", input)
          → consumes a sequence_id from the same call counter as execute_activity/sleep
          → on replay: finds CleanupRegistered{sequence_id} in history → no-op
          → on new: persists CleanupRegistered event
      → ctx.execute_activity("external_activity_name", input)  [if name is in external_activity_configs]
          → on replay: same as local activity — returns ActivityCompleted/ActivityErrored from history
          → on new: creates PendingTask in storage → persists ActivityScheduled + ActivityDispatched
              → blocks waiting for worker result via oneshot channel (fast path)
                or periodic storage poll every 5s (crash-recovery fallback)
              → worker calls POST /api/workers/tasks/{token}/complete or /fail
              → engine fires the oneshot sender → context unblocks
              → on success: persist ActivityCompleted → return output
              → on failure with retries remaining: persist ActivityAttemptFailed → backoff → next attempt
              → on all retries exhausted: persist ActivityErrored → return error
  → WorkerTask: if result is Ok or Cancelled → run_cleanups() in LIFO order
      → for each CleanupRegistered not yet followed by CleanupCompleted/CleanupFailed:
          → look up activity by name → call activity.execute() (single attempt)
          → persist CleanupCompleted or CleanupFailed (failures tolerated, do not propagate)
      → if result is Err(other) and CleanupPolicy::Always → run_cleanups() same as above
      → if result is Err(other) and CleanupPolicy::OnSuccessOrCancelled (default) → skip cleanups
  → persist WorkflowCompleted/WorkflowCancelled (after cleanups) or WorkflowFailed + update run status
```

On engine startup, `list_running_workflows()` finds any in-progress runs and replays them from their stored event history — this is the crash-recovery path. For external worker tasks, any tasks in `claimed` status are reset to `pending` on startup (their worker is gone); tasks in `pending` status are left as-is for workers to re-poll.

### Key Modules

- **`traits.rs`** — `Workflow`, `Activity`, `Storage` traits. All user-defined logic implements these. Also defines `PendingTask` (external worker task record) and `TaskResult` (Success/Failure enum reported by workers); 8 `Storage` methods cover the external task queue (`create_pending_task`, `claim_pending_task`, `resolve_pending_task`, `heartbeat_pending_task`, `get_pending_task`, `list_pending_tasks_by_run`, `list_stale_pending_tasks`, `reset_pending_task`).
- **`typed.rs`** — `TypedWorkflow`, `TypedActivity` traits with associated `Input`/`Output` types. Blanket impls auto-generate the untyped `Workflow`/`Activity` implementations, handling serde at the boundary.
- **`event.rs`** — `WorkflowEvent`/`EventPayload` enum — the immutable event log schema. Includes `ActivityDispatched { sequence_id, task_token, attempt }` written when an external worker claims a task (audit-only; replay correctness still relies on `ActivityCompleted`/`ActivityErrored`). Both `EventPayload` and `GearsError` are `#[non_exhaustive]`.
- **`context.rs`** — `WorkflowContext` (passed to `Workflow::run`) provides `execute_activity()`, `execute_activities_parallel()`, `concurrently()` / `try_concurrently()` / `concurrently_2/3/4()`, `sleep()`/`sleep_until()`, `get_version(change_id, max_version)`, `changed(change_id)`, `register_cleanup()`, cancellation support, determinism helpers (`is_replaying()`, `workflow_start_time()`), and shared workflow state (`set_shared_state`/`shared_state`). `get_version` stores `max_version` on new executions and returns the stored version on replay (errors only if stored > max, indicating a rollback); `changed` is sugar for `get_version(id, 1) >= 1`. `ActivityContext` carries run metadata and optional shared state from the workflow. Maintains internal replay cache keyed by call sequence number. Activities are looked up by name from the registry. Typed convenience methods (`execute_activity_typed`, etc.) wrap the Value-based API with auto-serde. **Branch mode**: contexts spawned by `concurrently` carry a `branch_counter` + `branch_base` that replace the global `call_counter` for sequence ID allocation, keeping each branch's IDs in a fixed, non-overlapping range (`BRANCH_BUDGET = 1000` IDs per branch). The public `branch()` helper boxes closures for `concurrently`. **External activity dispatch**: `execute_activity()` routes to `execute_external_activity()` when the name is in `external_activity_configs`; that method manages `PendingTask` lifecycle, blocks via a `oneshot` channel (fast path) or 5-second storage polling loop (crash-recovery fallback), and handles retries identically to local activities.
- **`engine.rs`** — `WorkflowEngineBuilder` + `WorkflowEngine`. Manages workflow/activity registration, dispatch loop, recovery on startup, concurrency via semaphore, `cancel_workflow()`, `list_runs()`, `get_run_events()`, `get_schedule()`, `trigger_schedule()`, `workflow_names()`, `activity_names()`, `activity_info_list()`, `workflow_info_list()`, `engine_info()`, cleanup policy (`cleanup_policy(CleanupPolicy)`), and typed variants (`start_workflow_typed`, `get_run_result`, `get_run_result_typed`). `ActivityInfo` struct carries per-activity metadata (name, max_attempts, retry_base_delay_ms, timeout_ms). `WorkflowInfo` struct carries per-workflow metadata (name, effective retention_days); returned by `workflow_info_list()`. `EngineInfo` struct carries a runtime config snapshot (max_concurrent_workflows, global_retention_days, registered counts); returned by `engine_info()`. `RetentionPolicy` controls automatic deletion of terminal runs: global via `retention_days(u32)` on the builder; per-workflow via `Workflow::retention() -> Option<Duration>` (overrides global when `Some`). A background hourly Tokio task runs `run_pruning_pass` and deletes expired runs; it starts only when at least one retention period is configured and shuts down cleanly via `EngineHandle::shutdown()`. **External worker support**: `register_external_activity(name, ExternalActivityConfig)` on the builder; `ExternalActivityConfig` carries `heartbeat_timeout`, `schedule_to_start_timeout`, `max_attempts`, `retry_base_delay`. `ExternalTaskQueue` holds a shared `tokio::sync::Notify` that wakes long-polling workers. Public methods: `poll_task(activity_names, worker_id, timeout_ms)` (long-poll + atomic claim), `complete_task(token, output)`, `fail_task(token, error)`, `heartbeat_task(token)` (returns `run_cancelled: bool`), `get_pending_task(token)`. A background `stale_task_monitor` runs every 10 seconds and resets claimed tasks whose heartbeat has expired.
- **`api.rs`** — `management_router()` returns an Axum `Router<Arc<WorkflowEngine>>` with REST endpoints for all engine management operations. Mount it with `.nest("/api", management_router())`. Endpoints: `GET /runs` (supports `?status`, `?workflow_name`, `?limit`, `?offset`, `?created_after`, `?created_before` ISO 8601 filters), `POST /runs` (start a run; body: `{workflow_name, input}`; returns `{run_id}`), `GET /runs/{id}` (includes `error: string|null` field for failed runs), `GET /runs/{id}/events` (full ordered event log as JSON array), `POST /runs/{id}/cancel`, `GET /runs/stats` (aggregate counts: `{total, running, completed, failed, cancelled}`), `POST /runs/prune`, `GET /schedules` (supports `?workflow_name` filter), `POST /schedules`, `GET /schedules/{name}`, `PATCH /schedules/{name}` (update `cron_expression` and/or `input`; upsert semantics), `DELETE /schedules/{name}`, `POST /schedules/{name}/pause`, `POST /schedules/{name}/resume`, `POST /schedules/{name}/trigger` (fire immediately, returns `{run_id}`; works on paused schedules), `GET /workflows` (returns `WorkflowInfo` objects with name + effective `retention_secs`), `GET /activities` (returns `ActivityInfo` objects with retry config), `GET /engine/info` (returns `EngineInfo` with `max_concurrent_workflows`, `global_retention_days`, `registered_workflows`, `registered_activities`), `GET /openapi.json` (OpenAPI 3.1 spec). **External worker endpoints** (tag: `workers`): `POST /workers/poll` (body: `{worker_id, activity_names, long_poll_timeout_ms?}`; returns 200 `TaskAssignment` or 204), `GET /workers/tasks/{token}`, `POST /workers/tasks/{token}/complete` (body: `{output}`), `POST /workers/tasks/{token}/fail` (body: `{error}`), `POST /workers/tasks/{token}/heartbeat` (returns `{run_cancelled: bool}`). All request/response types carry `#[derive(utoipa::ToSchema)]`; the spec is generated at compile time via `GearsApiDoc` and served on each request. `openapi_spec() -> utoipa::openapi::OpenApi` is also exported from the library root for programmatic use.
- **`worker.rs`** — `WorkerTask` executes a single workflow run end-to-end. Runs registered cleanups (LIFO, failures tolerated) before writing `WorkflowCompleted` or `WorkflowCancelled`. With `CleanupPolicy::Always`, also runs cleanups before `WorkflowFailed`. Defines `CleanupPolicy` enum. Carries `external_activity_configs`, `task_queue`, and `pending_completions` and passes them to `WorkflowContext::new()`.
- **`storage/sqlite.rs`** — SQLite backend (WAL mode). Three tables: `workflow_runs` (metadata + status), `workflow_events` (append-only event log), and `pending_tasks` (external worker task queue, indexed on `(status, activity_name)` and `run_id`). `claim_pending_task` uses `BEGIN IMMEDIATE` to atomically SELECT + UPDATE a single pending task — safe because SQLite WAL allows only one writer at a time.
- **`external_worker.rs`** — Rust SDK for writing external workers. `ExternalWorker::builder(url, worker_id).register_activity(impl ExternalActivity).build().run()` runs a continuous poll → execute → complete/fail loop with automatic heartbeating. `ExternalActivityContext` exposes `heartbeat() -> bool` (returns `run_cancelled`). Workers are an optional convenience layer — any HTTP client can implement the protocol.
- **`metrics.rs`** — Optional metrics instrumentation behind the `metrics` Cargo feature.
- **`error.rs`** — `GearsError` enum (`#[non_exhaustive]`) covering storage, serialization, execution, cancellation, and engine lifecycle errors. Specific variants for `ActivityTimedOut`, `VersionConflict`, `TaskPanicked`, `InvalidSchedule`, `RunNotFound`, `ScheduleNotFound`, `BranchBudgetExceeded`, `TaskNotFound` (404 from worker endpoints), `TaskAlreadyResolved` (409 from worker complete/fail), `HeartbeatMismatch` (409 from heartbeat on non-claimed task); `Other(String)` is reserved for truly unexpected errors.
- **`src/bin/gears-ctl/`** — Standalone TUI controller binary (`gears-ctl`). Connects to the management API over HTTP. Four files: `main.rs` (event loop, terminal setup), `app.rs` (state + actions), `client.rs` (reqwest API client), `ui.rs` (ratatui rendering). Three tabs: **Runs** (list with status/workflow/id/duration/updated; title shows live ●running ✓completed ✗failed ⊘cancelled counts), **Schedules** (list with cron/status/last-fired; supports name filter), **Registered** (split view: workflows left with name + retention label, activities with retry config right; engine info panel at bottom). Key bindings:
  - `Tab` / `Shift+Tab` — cycle tabs
  - `↑↓` — navigate list or scroll event log in detail view
  - `Enter` — open run detail (event timeline) — Runs tab
  - `Esc` — close detail / exit filter / exit input mode
  - `c` — cancel selected run (Runs tab)
  - `t` — trigger selected schedule immediately, opens resulting run in Runs tab (Schedules tab)
  - `p` — pause/resume selected schedule (Schedules tab)
  - `d` — delete selected schedule (Schedules tab)
  - `/` — filter by name (live substring): Runs tab filters by workflow name, Schedules tab filters by schedule name
  - `f` — cycle status filter: all → running → completed → failed → cancelled (re-fetches from API, Runs tab)
  - `n` — trigger a new run with inline JSON input (Registered tab)
  - `y` — copy selected run ID to clipboard (macOS: pbcopy)
  - `P` — trigger an immediate pruning pass (Runs tab)
  - `r` — refresh
  - `q` — quit

### Deterministic Replay Invariant

Workflow functions must be **deterministic and side-effect free** — all I/O must go through `ctx.execute_activity()` or `ctx.sleep()`. The replay mechanism uses `sequence_id` (atomic counter) to match history entries to the current call position. Introducing non-determinism (e.g., random values, system time) inside a workflow function breaks replay correctness.

Two helpers address the most common non-determinism footguns:

- `ctx.is_replaying() -> bool` — returns `true` while the current call position still has a cached result in history. Use this to gate side effects (logging, tracing, metrics increments) that should only fire on live execution, not during replay.
- `ctx.workflow_start_time() -> DateTime<Utc>` — returns the original `WorkflowStarted` event timestamp, not `Utc::now()`. Use this instead of system time for any deadline or elapsed-time logic inside a workflow.

### Activity Retries

Activities retry with exponential backoff (default: 3 attempts, 1s base delay). Each failed attempt persists an `ActivityAttemptFailed` event (or `ActivityAttemptTimedOut` if a timeout was configured). After all retries are exhausted, `ActivityErrored` is written and the error propagates to the workflow. Activities are referenced by name string, not by passing a trait object.

### Test Coverage

`tests/integration.rs` covers (42 tests as of this writing):

| Feature | Tests |
|---------|-------|
| Basic workflow execution (success, failure) | ✓ |
| Activity execution, retries, timeout | ✓ |
| Parallel activities (`execute_activities_parallel`, fail-fast, try-parallel, heterogeneous types) | ✓ |
| Durable sleep + cancellation | ✓ |
| Versioning (`get_version`, `changed`) | ✓ |
| Typed workflows and activities | ✓ |
| Shared state | ✓ |
| Run listing and search | ✓ |
| Run result retrieval | ✓ |
| **Cleanup / finalizers** (success, cancel, fail, LIFO order, CleanupPolicy::Always, failing/unregistered cleanup tolerance) | ✓ |
| **Scheduled workflows** (cron validation, list/delete, fires on tick) | ✓ |
| **Crash recovery** (pre-populated history replayed; activity not re-executed) | ✓ |
| **Concurrent branches** (multi-step branches, sequential-then-parallel, fail-fast, try_concurrently partial results, typed_2, sleep within branch, crash recovery) | ✓ |
| **Retention pruning** (global retention, per-workflow priority, recent runs preserved, running workflows never pruned) | ✓ |
| **External workers** (round-trip poll→complete, failure+retry, all retries exhausted, multi-worker load balancing, double-complete returns TaskAlreadyResolved) | ✓ |
