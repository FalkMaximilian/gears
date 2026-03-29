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
      → ctx.execute_activity(activity, input)
          → on replay: returns cached result from event history
          → on new: persist ActivityScheduled → retry loop → persist ActivityCompleted/ActivityErrored
      → ctx.sleep(duration)
          → on replay: returns immediately if TimerFired in history
          → on new: persist TimerStarted → sleep → persist TimerFired
          → on crash-recovery: recalculate remaining time from TimerStarted timestamp
  → persist WorkflowCompleted or WorkflowFailed + update run status
```

On engine startup, `list_running_workflows()` finds any in-progress runs and replays them from their stored event history — this is the crash-recovery path.

### Key Modules

- **`traits.rs`** — `Workflow`, `Activity`, `Storage` traits. All user-defined logic implements these.
- **`event.rs`** — `WorkflowEvent`/`EventPayload` enum — the immutable event log schema.
- **`context.rs`** — `WorkflowContext` (passed to `Workflow::run`) provides `execute_activity()` and `sleep()`/`sleep_until()`. Maintains internal replay cache keyed by call sequence number.
- **`engine.rs`** — `WorkflowEngineBuilder` + `WorkflowEngine`. Manages workflow registration, dispatch loop, recovery on startup, and concurrency via semaphore.
- **`worker.rs`** — `WorkerTask` executes a single workflow run end-to-end.
- **`storage/sqlite.rs`** — SQLite backend (WAL mode). Two tables: `workflow_runs` (metadata + status) and `workflow_events` (append-only event log).
- **`error.rs`** — `ZdflowError` enum covering storage, serialization, execution, and engine lifecycle errors.

### Deterministic Replay Invariant

Workflow functions must be **deterministic and side-effect free** — all I/O must go through `ctx.execute_activity()` or `ctx.sleep()`. The replay mechanism uses `sequence_id` (atomic counter) to match history entries to the current call position. Introducing non-determinism (e.g., random values, system time) inside a workflow function breaks replay correctness.

### Activity Retries

Activities retry with exponential backoff (default: 3 attempts, 1s base delay). Each failed attempt persists an `ActivityAttemptFailed` event. After all retries are exhausted, `ActivityErrored` is written and the error propagates to the workflow.
