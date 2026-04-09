use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use zdflow::{
    Activity, ActivityContext, ActivityFuture, RunFilter, RunStatus, SqliteStorage, TypedActivity,
    TypedActivityFuture, TypedWorkflow, TypedWorkflowFuture, Workflow, WorkflowContext,
    WorkflowEngine, WorkflowFuture, ZdflowError,
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
        Box::pin(async move { Err(ZdflowError::Other("intentional failure".into())) })
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
            let version = ctx.get_version("add_step_2", 1, 2).await?;
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
        .register_workflow(VersionedWorkflow)
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
