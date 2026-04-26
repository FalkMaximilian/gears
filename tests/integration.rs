use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use std::sync::Arc;

use gears::{
    Activity, ActivityContext, ActivityFuture, CleanupPolicy, EventPayload, RetentionPolicy,
    RunFilter, RunStatus, SqliteStorage, Storage, TypedActivity, TypedActivityFuture, TypedWorkflow,
    TypedWorkflowFuture, Workflow, WorkflowContext, WorkflowEngine, WorkflowEvent, WorkflowFuture,
    GearsError, branch,
};

// ── Test activities ──────────────────────────────────────────────────────

struct EchoActivity;

impl EchoActivity {
    pub const NAME: &'static str = "echo";
}

impl Activity for EchoActivity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn execute(&self, _ctx: ActivityContext, input: Value) -> ActivityFuture {
        Box::pin(async move { Ok(input) })
    }
}

struct FailActivity;

impl FailActivity {
    pub const NAME: &'static str = "fail";
}

impl Activity for FailActivity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn execute(&self, _ctx: ActivityContext, _input: Value) -> ActivityFuture {
        Box::pin(async move { Err(GearsError::Other("intentional failure".into())) })
    }

    fn max_attempts(&self) -> u32 {
        1
    }
}

struct SlowActivity;

impl SlowActivity {
    pub const NAME: &'static str = "slow";
}

impl Activity for SlowActivity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn execute(&self, _ctx: ActivityContext, _input: Value) -> ActivityFuture {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(json!("done"))
        })
    }

    fn timeout(&self) -> Option<Duration> {
        Some(Duration::from_millis(50))
    }

    fn max_attempts(&self) -> u32 {
        2
    }

    fn retry_base_delay(&self) -> Duration {
        Duration::from_millis(10)
    }
}

// ── Test workflows ───────────────────────────────────────────────────────

struct SimpleWorkflow;

impl SimpleWorkflow {
    pub const NAME: &'static str = "simple";
}

impl Workflow for SimpleWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, input: Value) -> WorkflowFuture {
        Box::pin(async move { ctx.execute_activity(EchoActivity::NAME, input).await })
    }
}

struct FailingWorkflow;

impl FailingWorkflow {
    pub const NAME: &'static str = "failing";
}

impl Workflow for FailingWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move { ctx.execute_activity(FailActivity::NAME, json!({})).await })
    }
}

struct TimeoutWorkflow;

impl TimeoutWorkflow {
    pub const NAME: &'static str = "timeout_wf";
}

impl Workflow for TimeoutWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move { ctx.execute_activity(SlowActivity::NAME, json!({})).await })
    }
}

struct SleepyWorkflow;

impl SleepyWorkflow {
    pub const NAME: &'static str = "sleepy";
}

impl Workflow for SleepyWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            ctx.sleep(Duration::from_secs(60)).await?;
            Ok(json!("woke up"))
        })
    }
}

struct ParallelWorkflow;

impl ParallelWorkflow {
    pub const NAME: &'static str = "parallel";
}

impl Workflow for ParallelWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let results = ctx
                .execute_activities_parallel(vec![
                    (EchoActivity::NAME, json!({"id": 1})),
                    (EchoActivity::NAME, json!({"id": 2})),
                    (EchoActivity::NAME, json!({"id": 3})),
                ])
                .await?;
            Ok(json!(results))
        })
    }
}

// Workflow that uses execute_activities_parallel with a failing activity.
// Used to verify fail-fast behaviour.
struct ParallelFailFastWorkflow;

impl ParallelFailFastWorkflow {
    pub const NAME: &'static str = "parallel_fail_fast";
}

impl Workflow for ParallelFailFastWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            ctx.execute_activities_parallel(vec![
                (EchoActivity::NAME, json!({"id": 1})),
                (FailActivity::NAME, json!({})),
                (EchoActivity::NAME, json!({"id": 3})),
            ])
            .await?;
            Ok(json!("done"))
        })
    }
}

// Workflow that uses try_execute_activities_parallel and reports partial results.
struct TryParallelWorkflow;

impl TryParallelWorkflow {
    pub const NAME: &'static str = "try_parallel";
}

impl Workflow for TryParallelWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let results = ctx
                .try_execute_activities_parallel(vec![
                    (EchoActivity::NAME, json!({"id": 1})),
                    (FailActivity::NAME, json!({})),
                    (EchoActivity::NAME, json!({"id": 3})),
                ])
                .await?;
            let successes = results.iter().filter(|r| r.is_ok()).count() as u32;
            let failures = results.iter().filter(|r| r.is_err()).count() as u32;
            Ok(json!({"successes": successes, "failures": failures}))
        })
    }
}

// Types and workflow for the heterogeneous typed-tuple test.
#[derive(Debug, Serialize, Deserialize)]
struct PersonData {
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ScoreData {
    score: u32,
}

struct HeterogeneousWorkflow;

impl HeterogeneousWorkflow {
    pub const NAME: &'static str = "heterogeneous";
}

impl Workflow for HeterogeneousWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let (person, score): (PersonData, ScoreData) = ctx
                .execute_activities_parallel_2(
                    (EchoActivity::NAME, json!({"name": "Alice"})),
                    (EchoActivity::NAME, json!({"score": 42u32})),
                )
                .await?;
            Ok(json!({"name": person.name, "score": score.score}))
        })
    }
}

struct VersionedWorkflow;

impl VersionedWorkflow {
    pub const NAME: &'static str = "versioned";
}

impl Workflow for VersionedWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let version = ctx.get_version("add_step_2", 2).await?;
            if version >= 2 {
                let _ = ctx
                    .execute_activity(EchoActivity::NAME, json!({"step": 2}))
                    .await?;
            }
            let result = ctx
                .execute_activity(EchoActivity::NAME, json!({"step": 1}))
                .await?;
            Ok(json!({"version": version, "result": result}))
        })
    }
}

struct ChangedWorkflow;

impl ChangedWorkflow {
    pub const NAME: &'static str = "changed_workflow";
}

impl Workflow for ChangedWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let mut steps = vec!["step_1"];
            if ctx.changed("add_step_2").await? {
                steps.push("step_2");
                let _ = ctx
                    .execute_activity(EchoActivity::NAME, json!({"step": 2}))
                    .await?;
            }
            let result = ctx
                .execute_activity(EchoActivity::NAME, json!({"step": 1}))
                .await?;
            Ok(json!({"steps": steps, "result": result}))
        })
    }
}

// ── Helper ───────────────────────────────────────────────────────────────

async fn build_engine() -> WorkflowEngine {
    let storage = SqliteStorage::open(":memory:").await.unwrap();
    WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(SimpleWorkflow)
        .register_workflow(FailingWorkflow)
        .register_workflow(TimeoutWorkflow)
        .register_workflow(SleepyWorkflow)
        .register_workflow(ParallelWorkflow)
        .register_workflow(ParallelFailFastWorkflow)
        .register_workflow(TryParallelWorkflow)
        .register_workflow(HeterogeneousWorkflow)
        .register_workflow(VersionedWorkflow)
        .register_workflow(ChangedWorkflow)
        .register_activity(EchoActivity)
        .register_activity(FailActivity)
        .register_activity(SlowActivity)
        .max_concurrent_workflows(10)
        .build()
        .await
        .unwrap()
}

async fn wait_for_status(engine: &WorkflowEngine, run_id: Uuid, expected: RunStatus) {
    for _ in 0..100 {
        let status = engine.get_run_status(run_id).await.unwrap();
        if status == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("workflow {} did not reach {:?}", run_id, expected);
}

// ── Tests ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_workflow_completed_event_written() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow("simple", json!({"hello": "world"}))
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn test_workflow_failed_event_written() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine.start_workflow("failing", json!({})).await.unwrap();

    wait_for_status(&engine, run_id, RunStatus::Failed).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn test_activity_not_found() {
    let storage = SqliteStorage::open(":memory:").await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(SimpleWorkflow)
        // Deliberately not registering EchoActivity
        .max_concurrent_workflows(10)
        .build()
        .await
        .unwrap();

    let handle = engine.run().await.unwrap();
    let run_id = engine.start_workflow("simple", json!({})).await.unwrap();

    wait_for_status(&engine, run_id, RunStatus::Failed).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn test_activity_timeout() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow("timeout_wf", json!({}))
        .await
        .unwrap();

    // SlowActivity has timeout=50ms but sleeps 10s, max_attempts=2
    // Should fail after 2 timed-out attempts
    wait_for_status(&engine, run_id, RunStatus::Failed).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn test_workflow_cancellation() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine.start_workflow("sleepy", json!({})).await.unwrap();

    // Give it a moment to start sleeping
    tokio::time::sleep(Duration::from_millis(100)).await;

    engine.cancel_workflow(run_id).await.unwrap();
    wait_for_status(&engine, run_id, RunStatus::Cancelled).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn test_parallel_activities() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine.start_workflow("parallel", json!({})).await.unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn test_versioning() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine.start_workflow("versioned", json!({})).await.unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn test_changed() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow("changed_workflow", json!({}))
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let result = engine.get_run_result(run_id).await.unwrap().unwrap();
    assert_eq!(result["steps"], json!(["step_1", "step_2"]));

    handle.shutdown().await;
}

#[tokio::test]
async fn test_list_runs() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let _run1 = engine.start_workflow("simple", json!({})).await.unwrap();
    let _run2 = engine.start_workflow("simple", json!({})).await.unwrap();

    // Wait for both to complete
    wait_for_status(&engine, _run1, RunStatus::Completed).await;
    wait_for_status(&engine, _run2, RunStatus::Completed).await;

    // List all completed runs
    let runs = engine
        .list_runs(&RunFilter {
            status: Some(RunStatus::Completed),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(runs.len() >= 2);

    // List with limit
    let runs = engine
        .list_runs(&RunFilter {
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(runs.len(), 1);

    handle.shutdown().await;
}

// ── Typed API test types ────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderVars {
    order_id: String,
    user_id: String,
    trace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderResult {
    validated: bool,
    echoed_order_id: String,
}

// A typed activity that reads shared state from the context.
struct ValidateOrderActivity;

impl TypedActivity for ValidateOrderActivity {
    type Input = ();
    type Output = bool;

    fn name(&self) -> &'static str {
        "validate_order"
    }

    fn execute(&self, ctx: ActivityContext, _input: ()) -> TypedActivityFuture<bool> {
        Box::pin(async move {
            let vars: OrderVars = ctx.shared_state()?;
            Ok(!vars.order_id.is_empty())
        })
    }

    fn max_attempts(&self) -> u32 {
        1
    }
}

// A typed activity that accepts the full workflow struct as input.
struct EchoOrderIdActivity;

impl TypedActivity for EchoOrderIdActivity {
    type Input = OrderVars;
    type Output = String;

    fn name(&self) -> &'static str {
        "echo_order_id"
    }

    fn execute(&self, _ctx: ActivityContext, input: OrderVars) -> TypedActivityFuture<String> {
        Box::pin(async move { Ok(input.order_id) })
    }

    fn max_attempts(&self) -> u32 {
        1
    }
}

// A typed workflow using shared state + typed context methods.
struct TypedOrderWorkflow;

impl TypedWorkflow for TypedOrderWorkflow {
    type Input = OrderVars;
    type Output = OrderResult;

    fn name(&self) -> &'static str {
        "typed_order"
    }

    fn run(&self, ctx: WorkflowContext, vars: OrderVars) -> TypedWorkflowFuture<OrderResult> {
        Box::pin(async move {
            ctx.set_shared_state(&vars)?;

            let validated: bool = ctx.execute_activity_typed("validate_order", &()).await?;

            let echoed_order_id: String =
                ctx.execute_activity_typed("echo_order_id", &vars).await?;

            Ok(OrderResult {
                validated,
                echoed_order_id,
            })
        })
    }
}

async fn build_typed_engine() -> WorkflowEngine {
    let storage = SqliteStorage::open(":memory:").await.unwrap();
    WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(TypedOrderWorkflow)
        .register_activity(ValidateOrderActivity)
        .register_activity(EchoOrderIdActivity)
        .register_activity(EchoActivity)
        .max_concurrent_workflows(10)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_typed_workflow_end_to_end() {
    let mut engine = build_typed_engine().await;
    let handle = engine.run().await.unwrap();

    let input = OrderVars {
        order_id: "ORD-42".into(),
        user_id: "USR-1".into(),
        trace_id: "trace-abc".into(),
    };

    let run_id = engine
        .start_workflow_typed("typed_order", &input)
        .await
        .unwrap();
    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let result: Option<OrderResult> = engine.get_run_result_typed(run_id).await.unwrap();
    let result = result.expect("should have a result");
    assert!(result.validated);
    assert_eq!(result.echoed_order_id, "ORD-42");

    handle.shutdown().await;
}

#[tokio::test]
async fn test_shared_state_validation_false() {
    let mut engine = build_typed_engine().await;
    let handle = engine.run().await.unwrap();

    let input = OrderVars {
        order_id: "".into(),
        user_id: "USR-1".into(),
        trace_id: "trace-xyz".into(),
    };

    let run_id = engine
        .start_workflow_typed("typed_order", &input)
        .await
        .unwrap();
    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let result: OrderResult = engine.get_run_result_typed(run_id).await.unwrap().unwrap();
    assert!(!result.validated);

    handle.shutdown().await;
}

#[tokio::test]
async fn test_get_run_result_none_for_nonexistent() {
    let mut engine = build_typed_engine().await;
    let handle = engine.run().await.unwrap();

    let result = engine.get_run_result(Uuid::new_v4()).await.unwrap();
    assert!(result.is_none());

    handle.shutdown().await;
}

#[tokio::test]
async fn test_get_run_result_raw_json() {
    let mut engine = build_typed_engine().await;
    let handle = engine.run().await.unwrap();

    let input = OrderVars {
        order_id: "ORD-99".into(),
        user_id: "USR-2".into(),
        trace_id: "trace-def".into(),
    };

    let run_id = engine
        .start_workflow_typed("typed_order", &input)
        .await
        .unwrap();
    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let raw: Value = engine.get_run_result(run_id).await.unwrap().unwrap();
    assert_eq!(raw["echoed_order_id"], "ORD-99");
    assert_eq!(raw["validated"], true);

    handle.shutdown().await;
}

// ── Cleanup infrastructure ────────────────────────────────────────────────

type CallLog = Arc<std::sync::Mutex<Vec<String>>>;

/// Activity that appends its name to a shared call log.
struct LoggingActivity {
    activity_name: &'static str,
    log: CallLog,
}

impl Activity for LoggingActivity {
    fn name(&self) -> &'static str {
        self.activity_name
    }

    fn execute(&self, _ctx: ActivityContext, input: Value) -> ActivityFuture {
        let log = self.log.clone();
        let name = self.activity_name;
        Box::pin(async move {
            log.lock().unwrap().push(name.to_string());
            Ok(input)
        })
    }

    fn max_attempts(&self) -> u32 {
        1
    }
}

/// Activity that always returns an error (used as a failing cleanup).
struct AlwaysFailCleanupActivity;

impl Activity for AlwaysFailCleanupActivity {
    fn name(&self) -> &'static str {
        "always_fail_cleanup"
    }

    fn execute(&self, _ctx: ActivityContext, _input: Value) -> ActivityFuture {
        Box::pin(async move {
            Err(GearsError::Other("cleanup failed intentionally".into()))
        })
    }

    fn max_attempts(&self) -> u32 {
        1
    }
}

// Registers one cleanup then completes successfully.
struct CleanupSuccessWorkflow;

impl Workflow for CleanupSuccessWorkflow {
    fn name(&self) -> &'static str {
        "cleanup_success_wf"
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            ctx.register_cleanup("cleanup_action", json!({})).await?;
            Ok(json!("success"))
        })
    }
}

// Registers one cleanup then fails (workflow-level error).
struct CleanupFailureWorkflow;

impl Workflow for CleanupFailureWorkflow {
    fn name(&self) -> &'static str {
        "cleanup_failure_wf"
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            ctx.register_cleanup("cleanup_action", json!({})).await?;
            Err(GearsError::Other("workflow failed intentionally".into()))
        })
    }
}

// Registers one cleanup then sleeps — used for cancellation tests.
struct CleanupCancelWorkflow;

impl Workflow for CleanupCancelWorkflow {
    fn name(&self) -> &'static str {
        "cleanup_cancel_wf"
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            ctx.register_cleanup("cleanup_action", json!({})).await?;
            ctx.sleep(Duration::from_secs(60)).await?;
            Ok(json!("done"))
        })
    }
}

// Registers two cleanups in order — used to verify LIFO execution.
struct MultiCleanupWorkflow;

impl Workflow for MultiCleanupWorkflow {
    fn name(&self) -> &'static str {
        "multi_cleanup_wf"
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            ctx.register_cleanup("cleanup_first", json!({"order": 1})).await?;
            ctx.register_cleanup("cleanup_second", json!({"order": 2})).await?;
            Ok(json!("done"))
        })
    }
}

// Registers a cleanup for an activity that always fails, then succeeds.
struct CleanupFailingActivityWorkflow;

impl Workflow for CleanupFailingActivityWorkflow {
    fn name(&self) -> &'static str {
        "cleanup_failing_activity_wf"
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            ctx.register_cleanup("always_fail_cleanup", json!({})).await?;
            Ok(json!("success"))
        })
    }
}

// Registers a cleanup for an activity that is not in the registry.
struct UnregisteredCleanupWorkflow;

impl Workflow for UnregisteredCleanupWorkflow {
    fn name(&self) -> &'static str {
        "unregistered_cleanup_wf"
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            ctx.register_cleanup("nonexistent_activity_xyz", json!({})).await?;
            Ok(json!("success"))
        })
    }
}

// Simple no-op workflow used by schedule and crash-recovery tests.
struct NullWorkflow;

impl Workflow for NullWorkflow {
    fn name(&self) -> &'static str {
        "null_wf"
    }

    fn run(&self, _ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move { Ok(json!("ok")) })
    }
}

// Two sequential echo steps — used for crash recovery test.
struct TwoStepWorkflow;

impl Workflow for TwoStepWorkflow {
    fn name(&self) -> &'static str {
        "two_step_wf"
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let a = ctx
                .execute_activity(EchoActivity::NAME, json!({"step": "a"}))
                .await?;
            let b = ctx
                .execute_activity(EchoActivity::NAME, json!({"step": "b"}))
                .await?;
            Ok(json!({"a": a, "b": b}))
        })
    }
}

// ── Cleanup tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cleanup_runs_on_success() {
    let log: CallLog = Arc::new(std::sync::Mutex::new(vec![]));

    let storage = SqliteStorage::open(":memory:").await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(CleanupSuccessWorkflow)
        .register_activity(LoggingActivity {
            activity_name: "cleanup_action",
            log: log.clone(),
        })
        .build()
        .await
        .unwrap();

    let handle = engine.run().await.unwrap();
    let run_id = engine
        .start_workflow("cleanup_success_wf", json!({}))
        .await
        .unwrap();
    wait_for_status(&engine, run_id, RunStatus::Completed).await;
    handle.shutdown().await;

    let calls = log.lock().unwrap().clone();
    assert_eq!(calls, vec!["cleanup_action"], "cleanup should run once on success");
}

#[tokio::test]
async fn test_cleanup_lifo_order() {
    let log: CallLog = Arc::new(std::sync::Mutex::new(vec![]));

    let storage = SqliteStorage::open(":memory:").await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(MultiCleanupWorkflow)
        .register_activity(LoggingActivity {
            activity_name: "cleanup_first",
            log: log.clone(),
        })
        .register_activity(LoggingActivity {
            activity_name: "cleanup_second",
            log: log.clone(),
        })
        .build()
        .await
        .unwrap();

    let handle = engine.run().await.unwrap();
    let run_id = engine
        .start_workflow("multi_cleanup_wf", json!({}))
        .await
        .unwrap();
    wait_for_status(&engine, run_id, RunStatus::Completed).await;
    handle.shutdown().await;

    let calls = log.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec!["cleanup_second", "cleanup_first"],
        "cleanups must run in LIFO order (last registered runs first)"
    );
}

#[tokio::test]
async fn test_cleanup_runs_on_cancellation() {
    let log: CallLog = Arc::new(std::sync::Mutex::new(vec![]));

    let storage = SqliteStorage::open(":memory:").await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(CleanupCancelWorkflow)
        .register_activity(LoggingActivity {
            activity_name: "cleanup_action",
            log: log.clone(),
        })
        .build()
        .await
        .unwrap();

    let handle = engine.run().await.unwrap();
    let run_id = engine
        .start_workflow("cleanup_cancel_wf", json!({}))
        .await
        .unwrap();

    // Give the workflow time to register the cleanup and enter the sleep.
    tokio::time::sleep(Duration::from_millis(100)).await;
    engine.cancel_workflow(run_id).await.unwrap();
    wait_for_status(&engine, run_id, RunStatus::Cancelled).await;
    handle.shutdown().await;

    let calls = log.lock().unwrap().clone();
    assert_eq!(calls, vec!["cleanup_action"], "cleanup should run on cancellation");
}

#[tokio::test]
async fn test_cleanup_skipped_on_failure_default_policy() {
    let log: CallLog = Arc::new(std::sync::Mutex::new(vec![]));

    let storage = SqliteStorage::open(":memory:").await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(CleanupFailureWorkflow)
        .register_activity(LoggingActivity {
            activity_name: "cleanup_action",
            log: log.clone(),
        })
        .build()
        .await
        .unwrap();

    let handle = engine.run().await.unwrap();
    let run_id = engine
        .start_workflow("cleanup_failure_wf", json!({}))
        .await
        .unwrap();
    wait_for_status(&engine, run_id, RunStatus::Failed).await;
    handle.shutdown().await;

    let calls = log.lock().unwrap().clone();
    assert!(
        calls.is_empty(),
        "cleanup must NOT run on workflow failure under default (OnSuccessOrCancelled) policy"
    );
}

#[tokio::test]
async fn test_cleanup_always_policy() {
    let log: CallLog = Arc::new(std::sync::Mutex::new(vec![]));

    let storage = SqliteStorage::open(":memory:").await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(CleanupFailureWorkflow)
        .register_activity(LoggingActivity {
            activity_name: "cleanup_action",
            log: log.clone(),
        })
        .cleanup_policy(CleanupPolicy::Always)
        .build()
        .await
        .unwrap();

    let handle = engine.run().await.unwrap();
    let run_id = engine
        .start_workflow("cleanup_failure_wf", json!({}))
        .await
        .unwrap();
    wait_for_status(&engine, run_id, RunStatus::Failed).await;
    handle.shutdown().await;

    let calls = log.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec!["cleanup_action"],
        "cleanup should run on failure when CleanupPolicy::Always is set"
    );
}

#[tokio::test]
async fn test_cleanup_failure_tolerated() {
    let storage = SqliteStorage::open(":memory:").await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(CleanupFailingActivityWorkflow)
        .register_activity(AlwaysFailCleanupActivity)
        .build()
        .await
        .unwrap();

    let handle = engine.run().await.unwrap();
    let run_id = engine
        .start_workflow("cleanup_failing_activity_wf", json!({}))
        .await
        .unwrap();
    // A failing cleanup must not change the workflow's final status.
    wait_for_status(&engine, run_id, RunStatus::Completed).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn test_cleanup_unregistered_activity_tolerated() {
    let storage = SqliteStorage::open(":memory:").await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(UnregisteredCleanupWorkflow)
        // Deliberately NOT registering "nonexistent_activity_xyz".
        .build()
        .await
        .unwrap();

    let handle = engine.run().await.unwrap();
    let run_id = engine
        .start_workflow("unregistered_cleanup_wf", json!({}))
        .await
        .unwrap();
    // A missing cleanup activity must not fail the workflow.
    wait_for_status(&engine, run_id, RunStatus::Completed).await;
    handle.shutdown().await;
}

// ── Schedule tests ────────────────────────────────────────────────────────

async fn build_schedule_engine() -> WorkflowEngine {
    let storage = SqliteStorage::open(":memory:").await.unwrap();
    WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(NullWorkflow)
        .register_workflow(SimpleWorkflow)
        .register_activity(EchoActivity)
        .build()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_invalid_cron_expression() {
    let mut engine = build_schedule_engine().await;
    let _handle = engine.run().await.unwrap();

    let err = engine
        .schedule_workflow("bad-sched", "not a cron expression", "null_wf", json!({}))
        .await
        .unwrap_err();
    assert!(
        matches!(err, GearsError::InvalidSchedule(_)),
        "expected InvalidSchedule error, got: {err:?}"
    );
}

#[tokio::test]
async fn test_schedule_list_and_delete() {
    let mut engine = build_schedule_engine().await;
    let handle = engine.run().await.unwrap();

    // Far-future schedules so they never fire during the test.
    engine
        .schedule_workflow("sched-alpha", "0 0 0 1 1 *", "null_wf", json!({}))
        .await
        .unwrap();
    engine
        .schedule_workflow("sched-beta", "0 0 0 1 1 *", "null_wf", json!({}))
        .await
        .unwrap();

    let schedules = engine.list_schedules().await.unwrap();
    assert_eq!(schedules.len(), 2);
    assert!(schedules.iter().any(|s| s.name == "sched-alpha"));
    assert!(schedules.iter().any(|s| s.name == "sched-beta"));

    engine.delete_schedule("sched-alpha").await.unwrap();
    let schedules = engine.list_schedules().await.unwrap();
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].name, "sched-beta");

    handle.shutdown().await;
}

#[tokio::test]
async fn test_schedule_fires() {
    let mut engine = build_schedule_engine().await;
    let handle = engine.run().await.unwrap();

    // `"* * * * * *"` fires every second on each second boundary.
    engine
        .schedule_workflow("tick", "* * * * * *", "null_wf", json!({}))
        .await
        .unwrap();

    // Wait long enough for at least one tick.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let runs = engine
        .list_runs(&RunFilter {
            workflow_name: Some("null_wf".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        !runs.is_empty(),
        "schedule should have triggered at least one workflow run"
    );

    handle.shutdown().await;
}

// ── Crash recovery test ───────────────────────────────────────────────────

/// Validates that the engine correctly replays a partially-executed run after
/// a simulated crash.  The test pre-populates a SQLite database with history
/// showing that step A of a two-step workflow completed, then starts a fresh
/// engine against the same file.  The engine must replay step A from history
/// (without re-executing the activity) and execute step B live.
#[tokio::test]
async fn test_crash_recovery() {
    let db_path = format!("/tmp/gears-crash-recovery-{}.db", Uuid::new_v4());
    let run_id = Uuid::new_v4();

    // Phase 1: Pre-populate the DB to simulate a crashed run where step A
    // completed but step B has not started yet.
    {
        let storage = SqliteStorage::open(&db_path).await.unwrap();
        storage
            .create_run(run_id, "two_step_wf", &json!({}))
            .await
            .unwrap();

        storage
            .append_event(
                run_id,
                &WorkflowEvent {
                    sequence: 0,
                    occurred_at: chrono::Utc::now(),
                    payload: EventPayload::WorkflowStarted {
                        workflow_name: "two_step_wf".to_string(),
                        input: json!({}),
                    },
                },
            )
            .await
            .unwrap();

        storage
            .append_event(
                run_id,
                &WorkflowEvent {
                    sequence: 1,
                    occurred_at: chrono::Utc::now(),
                    payload: EventPayload::ActivityScheduled {
                        sequence_id: 0,
                        activity_name: EchoActivity::NAME.to_string(),
                        input: json!({"step": "a"}),
                    },
                },
            )
            .await
            .unwrap();

        storage
            .append_event(
                run_id,
                &WorkflowEvent {
                    sequence: 2,
                    occurred_at: chrono::Utc::now(),
                    payload: EventPayload::ActivityCompleted {
                        sequence_id: 0,
                        output: json!({"step": "a"}),
                    },
                },
            )
            .await
            .unwrap();
        // Run status stays "running" — simulates the process dying mid-flight.
    }

    // Phase 2: Start a fresh engine against the same file.  Track how many
    // times the activity actually executes (replay must skip step A).
    let call_log: CallLog = Arc::new(std::sync::Mutex::new(vec![]));

    let storage = SqliteStorage::open(&db_path).await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(TwoStepWorkflow)
        .register_activity(LoggingActivity {
            activity_name: EchoActivity::NAME,
            log: call_log.clone(),
        })
        .build()
        .await
        .unwrap();

    let handle = engine.run().await.unwrap();
    wait_for_status(&engine, run_id, RunStatus::Completed).await;
    handle.shutdown().await;

    // Step A was in history: activity must NOT have been called for it.
    // Step B was not in history: activity must have been called exactly once.
    let calls = call_log.lock().unwrap().clone();
    assert_eq!(
        calls.len(),
        1,
        "EchoActivity should execute once (step B only); \
         step A was in history and must be replayed without calling the activity"
    );

    let result = engine.get_run_result(run_id).await.unwrap().unwrap();
    assert_eq!(result["a"], json!({"step": "a"}), "step A result must come from history");
    assert_eq!(result["b"], json!({"step": "b"}), "step B result must come from live execution");

    let _ = std::fs::remove_file(&db_path);
}

// ── Parallel activity improvement tests ───────────────────────────────────

/// When one activity in execute_activities_parallel fails, the whole call
/// should fail and the workflow should end with Failed status.
#[tokio::test]
async fn test_parallel_fail_fast() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow(ParallelFailFastWorkflow::NAME, json!({}))
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Failed).await;
    handle.shutdown().await;
}

/// try_execute_activities_parallel collects all results without short-circuiting.
/// The workflow itself completes successfully and can inspect per-activity outcomes.
#[tokio::test]
async fn test_try_parallel_partial_results() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow(TryParallelWorkflow::NAME, json!({}))
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let result = engine.get_run_result(run_id).await.unwrap().unwrap();
    assert_eq!(result["successes"], 2, "two echo activities should succeed");
    assert_eq!(result["failures"], 1, "one fail activity should fail");

    handle.shutdown().await;
}

/// execute_activities_parallel_2 returns a typed tuple with different output types.
#[tokio::test]
async fn test_parallel_heterogeneous_types() {
    let mut engine = build_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow(HeterogeneousWorkflow::NAME, json!({}))
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let result = engine.get_run_result(run_id).await.unwrap().unwrap();
    assert_eq!(result["name"], "Alice");
    assert_eq!(result["score"], 42);

    handle.shutdown().await;
}

// ── concurrently tests ────────────────────────────────────────────────────

// Workflow: two branches each with 2 sequential activities.
struct ConcurrentBranchesWorkflow;

impl ConcurrentBranchesWorkflow {
    pub const NAME: &'static str = "concurrent_branches";
}

impl Workflow for ConcurrentBranchesWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let results = ctx
                .concurrently(vec![
                    // Branch A: two sequential echos
                    branch(|ctx| async move {
                        let r1 = ctx
                            .execute_activity(EchoActivity::NAME, json!({"branch": "a", "step": 1}))
                            .await?;
                        let r2 = ctx
                            .execute_activity(EchoActivity::NAME, json!({"branch": "a", "step": 2}))
                            .await?;
                        Ok(json!({"a1": r1, "a2": r2}))
                    }),
                    // Branch B: two sequential echos
                    branch(|ctx| async move {
                        let r1 = ctx
                            .execute_activity(EchoActivity::NAME, json!({"branch": "b", "step": 1}))
                            .await?;
                        let r2 = ctx
                            .execute_activity(EchoActivity::NAME, json!({"branch": "b", "step": 2}))
                            .await?;
                        Ok(json!({"b1": r1, "b2": r2}))
                    }),
                ])
                .await?;
            Ok(json!(results))
        })
    }
}

// Workflow: sequential activity → two parallel branches (the user's example).
struct SequentialThenConcurrentWorkflow;

impl SequentialThenConcurrentWorkflow {
    pub const NAME: &'static str = "seq_then_concurrent";
}

impl Workflow for SequentialThenConcurrentWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, input: Value) -> WorkflowFuture {
        Box::pin(async move {
            // First: copy the file (sequential)
            let copied = ctx
                .execute_activity(EchoActivity::NAME, json!({"action": "copy", "file": input["file"]}))
                .await?;

            // Then: two parallel branches — start job AND make API call
            let (job_result, api_result): (Value, Value) = ctx
                .concurrently_2(
                    move |ctx| {
                        let copied = copied.clone();
                        async move {
                            ctx.execute_activity(EchoActivity::NAME, json!({"action": "start_job", "file": copied}))
                                .await
                        }
                    },
                    |ctx| async move {
                        ctx.execute_activity(EchoActivity::NAME, json!({"action": "api_call"}))
                            .await
                    },
                )
                .await?;

            Ok(json!({"job": job_result, "api": api_result}))
        })
    }
}

// Workflow: one branch fails; the other should be aborted.
struct ConcurrentFailFastWorkflow;

impl ConcurrentFailFastWorkflow {
    pub const NAME: &'static str = "concurrent_fail_fast";
}

impl Workflow for ConcurrentFailFastWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            ctx.concurrently(vec![
                branch(|ctx| async move {
                    ctx.execute_activity(EchoActivity::NAME, json!({"id": 1})).await
                }),
                branch(|ctx| async move {
                    ctx.execute_activity(FailActivity::NAME, json!({})).await
                }),
            ])
            .await?;
            Ok(json!("should not reach here"))
        })
    }
}

// Workflow: try_concurrently collects both success and failure.
struct TryConcurrentWorkflow;

impl TryConcurrentWorkflow {
    pub const NAME: &'static str = "try_concurrent";
}

impl Workflow for TryConcurrentWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let results = ctx
                .try_concurrently(vec![
                    branch(|ctx| async move {
                        ctx.execute_activity(EchoActivity::NAME, json!({"id": 1})).await
                    }),
                    branch(|ctx| async move {
                        ctx.execute_activity(FailActivity::NAME, json!({})).await
                    }),
                    branch(|ctx| async move {
                        ctx.execute_activity(EchoActivity::NAME, json!({"id": 3})).await
                    }),
                ])
                .await?;

            let successes = results.iter().filter(|r| r.is_ok()).count() as u32;
            let failures = results.iter().filter(|r| r.is_err()).count() as u32;
            Ok(json!({"successes": successes, "failures": failures}))
        })
    }
}

// Workflow: typed concurrently_2 with heterogeneous output types.
struct ConcurrentTyped2Workflow;

impl ConcurrentTyped2Workflow {
    pub const NAME: &'static str = "concurrent_typed_2";
}

impl Workflow for ConcurrentTyped2Workflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let (person, score): (PersonData, ScoreData) = ctx
                .concurrently_2(
                    |ctx| async move {
                        ctx.execute_activity_typed(EchoActivity::NAME, &json!({"name": "Bob"}))
                            .await
                    },
                    |ctx| async move {
                        ctx.execute_activity_typed(EchoActivity::NAME, &json!({"score": 99u32}))
                            .await
                    },
                )
                .await?;
            Ok(json!({"name": person.name, "score": score.score}))
        })
    }
}

// Workflow: branch with activity + short sleep + activity.
struct ConcurrentWithSleepWorkflow;

impl ConcurrentWithSleepWorkflow {
    pub const NAME: &'static str = "concurrent_with_sleep";
}

impl Workflow for ConcurrentWithSleepWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let results = ctx
                .concurrently(vec![
                    // Branch A: activity → tiny sleep → activity
                    branch(|ctx| async move {
                        let r1 = ctx
                            .execute_activity(EchoActivity::NAME, json!({"step": "before_sleep"}))
                            .await?;
                        ctx.sleep(Duration::from_millis(1)).await?;
                        let r2 = ctx
                            .execute_activity(EchoActivity::NAME, json!({"step": "after_sleep"}))
                            .await?;
                        Ok(json!({"r1": r1, "r2": r2}))
                    }),
                    // Branch B: single activity
                    branch(|ctx| async move {
                        ctx.execute_activity(EchoActivity::NAME, json!({"step": "fast"})).await
                    }),
                ])
                .await?;
            Ok(json!(results))
        })
    }
}

async fn build_concurrent_engine() -> WorkflowEngine {
    let storage = SqliteStorage::open(":memory:").await.unwrap();
    WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(ConcurrentBranchesWorkflow)
        .register_workflow(SequentialThenConcurrentWorkflow)
        .register_workflow(ConcurrentFailFastWorkflow)
        .register_workflow(TryConcurrentWorkflow)
        .register_workflow(ConcurrentTyped2Workflow)
        .register_workflow(ConcurrentWithSleepWorkflow)
        .register_activity(EchoActivity)
        .register_activity(FailActivity)
        .max_concurrent_workflows(10)
        .build()
        .await
        .unwrap()
}

/// Two branches each containing 2 sequential activities run concurrently
/// and both results are returned in order.
#[tokio::test]
async fn test_concurrent_branches_basic() {
    let mut engine = build_concurrent_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow(ConcurrentBranchesWorkflow::NAME, json!({}))
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let result = engine.get_run_result(run_id).await.unwrap().unwrap();
    // result is [[{...branch_a...}, {...branch_b...}]]
    assert_eq!(result[0]["a1"]["branch"], "a");
    assert_eq!(result[0]["a1"]["step"], 1);
    assert_eq!(result[0]["a2"]["branch"], "a");
    assert_eq!(result[0]["a2"]["step"], 2);
    assert_eq!(result[1]["b1"]["branch"], "b");
    assert_eq!(result[1]["b2"]["branch"], "b");

    handle.shutdown().await;
}

/// The user's example: copy file (sequential) → then start_job AND api_call (parallel).
#[tokio::test]
async fn test_concurrent_branches_sequential_then_parallel() {
    let mut engine = build_concurrent_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow(
            SequentialThenConcurrentWorkflow::NAME,
            json!({"file": "data.csv"}),
        )
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let result = engine.get_run_result(run_id).await.unwrap().unwrap();
    assert_eq!(result["job"]["action"], "start_job");
    assert_eq!(result["api"]["action"], "api_call");

    handle.shutdown().await;
}

/// If one branch fails, concurrently aborts the rest and returns the error.
#[tokio::test]
async fn test_concurrent_branches_fail_fast() {
    let mut engine = build_concurrent_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow(ConcurrentFailFastWorkflow::NAME, json!({}))
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Failed).await;
    handle.shutdown().await;
}

/// try_concurrently collects all results without short-circuiting.
#[tokio::test]
async fn test_try_concurrent_partial_results() {
    let mut engine = build_concurrent_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow(TryConcurrentWorkflow::NAME, json!({}))
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let result = engine.get_run_result(run_id).await.unwrap().unwrap();
    assert_eq!(result["successes"], 2, "two branches should succeed");
    assert_eq!(result["failures"], 1, "one branch should fail");

    handle.shutdown().await;
}

/// concurrently_2 returns a typed tuple with different output types per branch.
#[tokio::test]
async fn test_concurrent_branches_typed_2() {
    let mut engine = build_concurrent_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow(ConcurrentTyped2Workflow::NAME, json!({}))
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let result = engine.get_run_result(run_id).await.unwrap().unwrap();
    assert_eq!(result["name"], "Bob");
    assert_eq!(result["score"], 99);

    handle.shutdown().await;
}

/// A branch containing both an activity and a sleep completes correctly.
#[tokio::test]
async fn test_concurrent_branches_with_sleep() {
    let mut engine = build_concurrent_engine().await;
    let handle = engine.run().await.unwrap();

    let run_id = engine
        .start_workflow(ConcurrentWithSleepWorkflow::NAME, json!({}))
        .await
        .unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    let result = engine.get_run_result(run_id).await.unwrap().unwrap();
    assert_eq!(result[0]["r1"]["step"], "before_sleep");
    assert_eq!(result[0]["r2"]["step"], "after_sleep");
    assert_eq!(result[1]["step"], "fast");

    handle.shutdown().await;
}

/// Crash recovery: pre-populate history with a completed first branch activity
/// and verify the second branch activity is executed live while the first is replayed.
#[tokio::test]
async fn test_concurrent_branches_crash_recovery() {
    use gears::{BRANCH_BUDGET, EventPayload, WorkflowEvent};
    use chrono::Utc;

    let db_path = format!("/tmp/gears_concurrent_recovery_{}.db", Uuid::new_v4());
    let storage = SqliteStorage::open(&db_path).await.unwrap();

    // Pre-create a run for ConcurrentBranchesWorkflow.
    let run_id = Uuid::new_v4();
    storage
        .create_run(run_id, ConcurrentBranchesWorkflow::NAME, &json!({}))
        .await
        .unwrap();

    // The workflow calls concurrently with 2 branches (BRANCH_BUDGET IDs each).
    // Counter before fork: 0 → fork_seq = 0, block_base = 1.
    // Branch A base = 1, Branch B base = 1 + BRANCH_BUDGET.
    // Branch A step 1 → seq_id = 1 (branch_base=1, local=0)
    // Branch A step 2 → seq_id = 2 (branch_base=1, local=1)
    // Branch B step 1 → seq_id = 1+BRANCH_BUDGET
    // Branch B step 2 → seq_id = 2+BRANCH_BUDGET

    let branch_b_base = 1 + BRANCH_BUDGET;

    let mut seq: u64 = 0;
    let mut push = |payload: EventPayload| {
        seq += 1;
        WorkflowEvent {
            sequence: seq,
            occurred_at: Utc::now(),
            payload,
        }
    };

    // WorkflowStarted
    storage
        .append_event(
            run_id,
            &push(EventPayload::WorkflowStarted {
                workflow_name: ConcurrentBranchesWorkflow::NAME.to_string(),
                input: json!({}),
            }),
        )
        .await
        .unwrap();

    // Fork marker (fork_seq=0)
    storage
        .append_event(
            run_id,
            &push(EventPayload::ConcurrentBranchesStarted {
                sequence_id: 0,
                num_branches: 2,
                branch_budget: BRANCH_BUDGET,
            }),
        )
        .await
        .unwrap();

    // Branch A step 1 completed (seq_id = 1)
    storage
        .append_event(
            run_id,
            &push(EventPayload::ActivityScheduled {
                sequence_id: 1,
                activity_name: EchoActivity::NAME.to_string(),
                input: json!({"branch": "a", "step": 1}),
            }),
        )
        .await
        .unwrap();
    storage
        .append_event(
            run_id,
            &push(EventPayload::ActivityCompleted {
                sequence_id: 1,
                output: json!({"branch": "a", "step": 1}),
            }),
        )
        .await
        .unwrap();

    // Branch B step 1 completed (seq_id = branch_b_base + 0)
    storage
        .append_event(
            run_id,
            &push(EventPayload::ActivityScheduled {
                sequence_id: branch_b_base,
                activity_name: EchoActivity::NAME.to_string(),
                input: json!({"branch": "b", "step": 1}),
            }),
        )
        .await
        .unwrap();
    storage
        .append_event(
            run_id,
            &push(EventPayload::ActivityCompleted {
                sequence_id: branch_b_base,
                output: json!({"branch": "b", "step": 1}),
            }),
        )
        .await
        .unwrap();

    // Drop storage so the engine can open it
    drop(storage);

    // Boot the engine — it will recover the in-progress run and replay.
    let storage2 = SqliteStorage::open(&db_path).await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage2)
        .register_workflow(ConcurrentBranchesWorkflow)
        .register_activity(EchoActivity)
        .max_concurrent_workflows(10)
        .build()
        .await
        .unwrap();

    let handle = engine.run().await.unwrap();

    wait_for_status(&engine, run_id, RunStatus::Completed).await;
    handle.shutdown().await;

    // Verify result is correct (both branches completed).
    let storage3 = SqliteStorage::open(&db_path).await.unwrap();
    let result = storage3.get_run_result(run_id).await.unwrap().unwrap();
    assert_eq!(result[0]["a1"]["branch"], "a", "branch A step 1 from history");
    assert_eq!(result[1]["b1"]["branch"], "b", "branch B step 1 from history");

    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-shm", &db_path));
    let _ = std::fs::remove_file(format!("{}-wal", &db_path));
}

// ── Retention pruning ─────────────────────────────────────────────────────

#[tokio::test]
async fn retention_policy_effective_for_priority() {
    // Pure unit test — no engine needed.
    let policy = RetentionPolicy { global_days: Some(30) };

    // Per-workflow Some overrides global.
    let seven_days = Duration::from_secs(7 * 86_400);
    assert_eq!(policy.effective_for(Some(seven_days)), Some(seven_days));

    // Per-workflow None falls back to global.
    assert_eq!(
        policy.effective_for(None),
        Some(Duration::from_secs(30 * 86_400))
    );

    // Both None → no pruning.
    let no_policy = RetentionPolicy { global_days: None };
    assert_eq!(no_policy.effective_for(None), None);
}

#[tokio::test]
async fn retention_prunes_expired_completed_run() {
    let db_path = format!("/tmp/gears-ret-expired-{}.db", Uuid::new_v4());

    // Phase 1: create and complete a run using a real engine.
    let storage = SqliteStorage::open(&db_path).await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage.clone())
        .register_workflow(NullWorkflow)
        .register_activity(EchoActivity)
        .retention_days(1)
        .build()
        .await
        .unwrap();
    let handle = engine.run().await.unwrap();
    let run_id = engine.start_workflow("null_wf", json!({})).await.unwrap();
    wait_for_status(&engine, run_id, RunStatus::Completed).await;
    handle.shutdown().await;

    // Backdate the run so it appears 2 days old.
    storage.backdate_run(run_id, 2).await;

    // Phase 2: rebuild engine (same storage) and trigger pruning.
    let mut engine2 = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(NullWorkflow)
        .register_activity(EchoActivity)
        .retention_days(1)
        .build()
        .await
        .unwrap();
    let handle2 = engine2.run().await.unwrap();
    engine2.prune_now().await;

    assert_eq!(
        engine2.get_run_status(run_id).await.unwrap(),
        RunStatus::NotFound,
        "expired run should be deleted"
    );
    handle2.shutdown().await;

    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-shm", &db_path));
    let _ = std::fs::remove_file(format!("{}-wal", &db_path));
}

#[tokio::test]
async fn retention_does_not_prune_recent_run() {
    let storage = SqliteStorage::open(":memory:").await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(NullWorkflow)
        .register_activity(EchoActivity)
        .retention_days(30)
        .build()
        .await
        .unwrap();
    let handle = engine.run().await.unwrap();
    let run_id = engine.start_workflow("null_wf", json!({})).await.unwrap();
    wait_for_status(&engine, run_id, RunStatus::Completed).await;

    engine.prune_now().await;

    assert_eq!(
        engine.get_run_status(run_id).await.unwrap(),
        RunStatus::Completed,
        "recent run should not be pruned"
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn retention_does_not_prune_running_workflows() {
    let storage = SqliteStorage::open(":memory:").await.unwrap();
    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(SleepyWorkflow)
        .register_activity(EchoActivity)
        .retention_days(0)
        .build()
        .await
        .unwrap();
    let handle = engine.run().await.unwrap();

    // Start a workflow that will sleep (remain Running).
    let run_id = engine
        .start_workflow(SleepyWorkflow::NAME, json!({}))
        .await
        .unwrap();

    // Give it a moment to start and persist WorkflowStarted.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Prune with 0-day retention — only terminal runs are eligible.
    engine.prune_now().await;

    assert_eq!(
        engine.get_run_status(run_id).await.unwrap(),
        RunStatus::Running,
        "running workflow must never be pruned"
    );

    engine.cancel_workflow(run_id).await.unwrap();
    wait_for_status(&engine, run_id, RunStatus::Cancelled).await;
    handle.shutdown().await;
}
