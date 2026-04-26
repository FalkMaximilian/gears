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
      → ctx.get_version(change_id, min, max)
          → on replay: returns stored version from VersionMarker event
          → on new: persists max_version, returns it
      → ctx.register_cleanup("activity_name", input)
          → consumes a sequence_id from the same call counter as execute_activity/sleep
          → on replay: finds CleanupRegistered{sequence_id} in history → no-op
          → on new: persists CleanupRegistered event
  → WorkerTask: if result is Ok or Cancelled → run_cleanups() in LIFO order
      → for each CleanupRegistered not yet followed by CleanupCompleted/CleanupFailed:
          → look up activity by name → call activity.execute() (single attempt)
          → persist CleanupCompleted or CleanupFailed (failures tolerated, do not propagate)
      → if result is Err(other) and CleanupPolicy::Always → run_cleanups() same as above
      → if result is Err(other) and CleanupPolicy::OnSuccessOrCancelled (default) → skip cleanups
  → persist WorkflowCompleted/WorkflowCancelled (after cleanups) or WorkflowFailed + update run status
```

On engine startup, `list_running_workflows()` finds any in-progress runs and replays them from their stored event history — this is the crash-recovery path.

### Key Modules

- **`traits.rs`** — `Workflow`, `Activity`, `Storage` traits. All user-defined logic implements these.
- **`typed.rs`** — `TypedWorkflow`, `TypedActivity` traits with associated `Input`/`Output` types. Blanket impls auto-generate the untyped `Workflow`/`Activity` implementations, handling serde at the boundary.
- **`event.rs`** — `WorkflowEvent`/`EventPayload` enum — the immutable event log schema.
- **`context.rs`** — `WorkflowContext` (passed to `Workflow::run`) provides `execute_activity()`, `execute_activities_parallel()`, `concurrently()` / `try_concurrently()` / `concurrently_2/3/4()`, `sleep()`/`sleep_until()`, `get_version()`, `register_cleanup()`, cancellation support, determinism helpers (`is_replaying()`, `workflow_start_time()`), and shared workflow state (`set_shared_state`/`shared_state`). `ActivityContext` carries run metadata and optional shared state from the workflow. Maintains internal replay cache keyed by call sequence number. Activities are looked up by name from the registry. Typed convenience methods (`execute_activity_typed`, etc.) wrap the Value-based API with auto-serde. **Branch mode**: contexts spawned by `concurrently` carry a `branch_counter` + `branch_base` that replace the global `call_counter` for sequence ID allocation, keeping each branch's IDs in a fixed, non-overlapping range (`BRANCH_BUDGET = 1000` IDs per branch). The public `branch()` helper boxes closures for `concurrently`.
- **`engine.rs`** — `WorkflowEngineBuilder` + `WorkflowEngine`. Manages workflow/activity registration, dispatch loop, recovery on startup, concurrency via semaphore, `cancel_workflow()`, `list_runs()`, `get_run_events()`, `workflow_names()`, `activity_names()`, `activity_info_list()`, cleanup policy (`cleanup_policy(CleanupPolicy)`), and typed variants (`start_workflow_typed`, `get_run_result`, `get_run_result_typed`). `ActivityInfo` struct carries per-activity metadata (name, max_attempts, retry_base_delay_ms, timeout_ms). `RetentionPolicy` controls automatic deletion of terminal runs: global via `retention_days(u32)` on the builder; per-workflow via `Workflow::retention() -> Option<Duration>` (overrides global when `Some`). A background hourly Tokio task runs `run_pruning_pass` and deletes expired runs; it starts only when at least one retention period is configured and shuts down cleanly via `EngineHandle::shutdown()`.
- **`api.rs`** — `management_router()` returns an Axum `Router<Arc<WorkflowEngine>>` with REST endpoints for all engine management operations. Mount it with `.nest("/api", management_router())`. Endpoints: `GET /runs`, `POST /runs` (start a run; body: `{workflow_name, input}`; returns `{run_id}`), `GET /runs/{id}`, `GET /runs/{id}/events` (full ordered event log as JSON array), `POST /runs/{id}/cancel`, `GET /schedules`, `POST /schedules`, `DELETE /schedules/{name}`, `POST /schedules/{name}/pause`, `POST /schedules/{name}/resume`, `GET /workflows`, `GET /activities` (returns `ActivityInfo` objects, not just names), `GET /openapi.json` (OpenAPI 3.1 spec). All request/response types carry `#[derive(utoipa::ToSchema)]`; the spec is generated at compile time via `GearsApiDoc` and served on each request. `openapi_spec() -> utoipa::openapi::OpenApi` is also exported from the library root for programmatic use.
- **`worker.rs`** — `WorkerTask` executes a single workflow run end-to-end. Runs registered cleanups (LIFO, failures tolerated) before writing `WorkflowCompleted` or `WorkflowCancelled`. With `CleanupPolicy::Always`, also runs cleanups before `WorkflowFailed`. Defines `CleanupPolicy` enum.
- **`storage/sqlite.rs`** — SQLite backend (WAL mode). Two tables: `workflow_runs` (metadata + status) and `workflow_events` (append-only event log).
- **`metrics.rs`** — Optional metrics instrumentation behind the `metrics` Cargo feature.
- **`error.rs`** — `GearsError` enum covering storage, serialization, execution, cancellation, and engine lifecycle errors. Specific variants for `ActivityTimedOut`, `VersionConflict`, `TaskPanicked`, `InvalidSchedule`, `RunNotFound`, `ScheduleNotFound`, `BranchBudgetExceeded`; `Other(String)` is reserved for truly unexpected errors.
- **`src/bin/gears-ctl/`** — Standalone TUI controller binary (`gears-ctl`). Connects to the management API over HTTP. Four files: `main.rs` (event loop, terminal setup), `app.rs` (state + actions), `client.rs` (reqwest API client), `ui.rs` (ratatui rendering). Three tabs: **Runs** (list with status/workflow/id/duration/updated), **Schedules** (list with cron/status/last-fired), **Registered** (split view: workflows left, activities with retry config right). Key bindings:
  - `Tab` / `Shift+Tab` — cycle tabs
  - `↑↓` — navigate list or scroll event log in detail view
  - `Enter` — open run detail (event timeline) — Runs tab
  - `Esc` — close detail / exit filter / exit input mode
  - `c` — cancel selected run
  - `p` — pause/resume selected schedule
  - `d` — delete selected schedule
  - `/` — filter runs by workflow name (live substring, Runs tab)
  - `f` — cycle status filter: all → running → completed → failed → cancelled (re-fetches from API)
  - `n` — trigger a new run with inline JSON input (Registered tab)
  - `y` — copy selected run ID to clipboard (macOS: pbcopy)
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

`tests/integration.rs` covers (37 tests as of this writing):

| Feature | Tests |
|---------|-------|
| Basic workflow execution (success, failure) | ✓ |
| Activity execution, retries, timeout | ✓ |
| Parallel activities (`execute_activities_parallel`, fail-fast, try-parallel, heterogeneous types) | ✓ |
| Durable sleep + cancellation | ✓ |
| Versioning (`get_version`) | ✓ |
| Typed workflows and activities | ✓ |
| Shared state | ✓ |
| Run listing and search | ✓ |
| Run result retrieval | ✓ |
| **Cleanup / finalizers** (success, cancel, fail, LIFO order, CleanupPolicy::Always, failing/unregistered cleanup tolerance) | ✓ |
| **Scheduled workflows** (cron validation, list/delete, fires on tick) | ✓ |
| **Crash recovery** (pre-populated history replayed; activity not re-executed) | ✓ |
| **Concurrent branches** (multi-step branches, sequential-then-parallel, fail-fast, try_concurrently partial results, typed_2, sleep within branch, crash recovery) | ✓ |
| **Retention pruning** (global retention, per-workflow priority, recent runs preserved, running workflows never pruned) | ✓ |
