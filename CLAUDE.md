# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build                  # Debug build
cargo build --release        # Release build
cargo test                   # Run all tests
cargo test <test_name>       # Run a single test
cargo run                    # Run the demo HTTP server (port 3000)
cargo fmt                    # Format code
cargo clippy                 # Lint
```

## Architecture

zdflow is a **durable workflow execution engine** (similar to Temporal.io) built in Rust. The core design is event sourcing + deterministic replay: all workflow state transitions are persisted to SQLite, enabling crash recovery by replaying history.

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
      → if result is Err(other) → skip cleanups entirely
  → persist WorkflowCompleted/WorkflowCancelled (after cleanups) or WorkflowFailed + update run status
```

On engine startup, `list_running_workflows()` finds any in-progress runs and replays them from their stored event history — this is the crash-recovery path.

### Key Modules

- **`traits.rs`** — `Workflow`, `Activity`, `Storage` traits. All user-defined logic implements these.
- **`typed.rs`** — `TypedWorkflow`, `TypedActivity` traits with associated `Input`/`Output` types. Blanket impls auto-generate the untyped `Workflow`/`Activity` implementations, handling serde at the boundary.
- **`event.rs`** — `WorkflowEvent`/`EventPayload` enum — the immutable event log schema.
- **`context.rs`** — `WorkflowContext` (passed to `Workflow::run`) provides `execute_activity()`, `execute_activities_parallel()`, `sleep()`/`sleep_until()`, `get_version()`, `register_cleanup()`, cancellation support, and shared workflow state (`set_shared_state`/`shared_state`). `ActivityContext` carries run metadata and optional shared state from the workflow. Maintains internal replay cache keyed by call sequence number. Activities are looked up by name from the registry. Typed convenience methods (`execute_activity_typed`, etc.) wrap the Value-based API with auto-serde.
- **`engine.rs`** — `WorkflowEngineBuilder` + `WorkflowEngine`. Manages workflow/activity registration, dispatch loop, recovery on startup, concurrency via semaphore, `cancel_workflow()`, `list_runs()`, and typed variants (`start_workflow_typed`, `get_run_result`, `get_run_result_typed`).
- **`worker.rs`** — `WorkerTask` executes a single workflow run end-to-end. Runs registered cleanups (LIFO, failures tolerated) before writing `WorkflowCompleted` or `WorkflowCancelled`. Writes `WorkflowFailed` directly without cleanups.
- **`storage/sqlite.rs`** — SQLite backend (WAL mode). Two tables: `workflow_runs` (metadata + status) and `workflow_events` (append-only event log).
- **`metrics.rs`** — Optional metrics instrumentation behind the `metrics` Cargo feature.
- **`error.rs`** — `ZdflowError` enum covering storage, serialization, execution, cancellation, and engine lifecycle errors.

### Deterministic Replay Invariant

Workflow functions must be **deterministic and side-effect free** — all I/O must go through `ctx.execute_activity()` or `ctx.sleep()`. The replay mechanism uses `sequence_id` (atomic counter) to match history entries to the current call position. Introducing non-determinism (e.g., random values, system time) inside a workflow function breaks replay correctness.

### Activity Retries

Activities retry with exponential backoff (default: 3 attempts, 1s base delay). Each failed attempt persists an `ActivityAttemptFailed` event (or `ActivityAttemptTimedOut` if a timeout was configured). After all retries are exhausted, `ActivityErrored` is written and the error propagates to the workflow. Activities are referenced by name string, not by passing a trait object.
