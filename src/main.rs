//! gears demo — start a workflow from an Axum HTTP endpoint.
//!
//! Run:  cargo run --bin gears-demo
//! Test: curl -X POST http://localhost:3000/greet \
//!            -H 'Content-Type: application/json' \
//!            -d '{"name": "Alice"}'
//!
//! Management API (via gears-ctl or curl):
//!   curl http://localhost:3000/api/runs
//!   curl http://localhost:3000/api/schedules
//!   curl http://localhost:3000/api/workflows
//!
//! OpenAPI:
//!   Swagger UI: http://localhost:3000/swagger-ui
//!   Raw spec:   http://localhost:3000/api/openapi.json

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::Json,
    routing::post,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use gears::{
    Activity, ActivityContext, ActivityFuture, SqliteStorage, TypedActivity, TypedActivityFuture,
    TypedWorkflow, TypedWorkflowFuture, Workflow, WorkflowContext, WorkflowEngine, WorkflowFuture,
    management_router, openapi_spec,
};
use utoipa_swagger_ui::SwaggerUi;

// ── Activity: send a greeting ─────────────────────────────────────────────

struct SendGreetingActivity;

impl SendGreetingActivity {
    pub const NAME: &'static str = "send_greeting";
}

impl Activity for SendGreetingActivity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn execute(&self, ctx: ActivityContext, input: Value) -> ActivityFuture {
        Box::pin(async move {
            let name = input["name"].as_str().unwrap_or("stranger");
            tokio::time::sleep(Duration::from_millis(50)).await;
            let message = format!("Hello, {}! (attempt #{})", name, ctx.attempt);
            println!("[activity] {}", message);
            Ok(json!({ "message": message }))
        })
    }
}

// ── Workflow: greet → sleep → greet again ────────────────────────────────

struct GreetingWorkflow;

impl GreetingWorkflow {
    pub const NAME: &'static str = "greeting";
}

impl Workflow for GreetingWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let result1 = ctx
                .execute_activity(SendGreetingActivity::NAME, input.clone())
                .await?;
            println!("[workflow] first greeting: {:?}", result1["message"]);

            ctx.sleep(Duration::from_secs(2)).await?;
            println!("[workflow] woke up after sleep");

            let result2 = ctx
                .execute_activity(SendGreetingActivity::NAME, input.clone())
                .await?;
            println!("[workflow] second greeting: {:?}", result2["message"]);

            Ok(json!({
                "first":  result1,
                "second": result2,
            }))
        })
    }
}

// ── Activity: allocate a resource ────────────────────────────────────────

struct AllocateResourceActivity;

impl AllocateResourceActivity {
    pub const NAME: &'static str = "allocate_resource";
}

impl Activity for AllocateResourceActivity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn execute(&self, _ctx: ActivityContext, input: Value) -> ActivityFuture {
        Box::pin(async move {
            let resource = input["resource"].as_str().unwrap_or("unnamed");
            tokio::time::sleep(Duration::from_millis(30)).await;
            let handle = format!("handle-{}", &uuid::Uuid::new_v4().to_string()[..8]);
            println!("[activity] allocated resource '{}' → {}", resource, handle);
            Ok(json!({ "resource": resource, "handle": handle }))
        })
    }
}

// ── Activity: release a resource (used as cleanup) ────────────────────────

struct ReleaseResourceActivity;

impl ReleaseResourceActivity {
    pub const NAME: &'static str = "release_resource";
}

impl Activity for ReleaseResourceActivity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn execute(&self, _ctx: ActivityContext, input: Value) -> ActivityFuture {
        Box::pin(async move {
            let handle = input["handle"].as_str().unwrap_or("unknown");
            tokio::time::sleep(Duration::from_millis(20)).await;
            println!("[cleanup] released resource handle: {}", handle);
            Ok(json!({ "released": handle }))
        })
    }
}

// ── Workflow: allocate resource with automatic cleanup ────────────────────

struct ResourceWorkflow;

impl ResourceWorkflow {
    pub const NAME: &'static str = "resource_workflow";
}

impl Workflow for ResourceWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, ctx: WorkflowContext, input: Value) -> WorkflowFuture {
        Box::pin(async move {
            let alloc = ctx
                .execute_activity(AllocateResourceActivity::NAME, input.clone())
                .await?;

            ctx.register_cleanup(
                ReleaseResourceActivity::NAME,
                json!({ "handle": alloc["handle"] }),
            )
            .await?;

            println!("[workflow] resource allocated, sleeping 3 s (try cancelling me)…");
            ctx.sleep(Duration::from_secs(3)).await?;
            println!("[workflow] work done");

            Ok(json!({ "result": "done", "allocation": alloc }))
        })
    }
}

// ── Workflow: heartbeat (used by the cron schedule) ───────────────────────

struct HeartbeatWorkflow;

impl HeartbeatWorkflow {
    pub const NAME: &'static str = "heartbeat";
}

impl Workflow for HeartbeatWorkflow {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn run(&self, _ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            println!("[heartbeat] tick at {}", chrono::Utc::now());
            Ok(json!({ "status": "ok" }))
        })
    }
}

// ── Typed workflow demo: order processing ────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderInput {
    order_id: String,
    customer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderOutput {
    order_id: String,
    confirmed: bool,
    message: String,
}

struct ConfirmOrderActivity;

impl TypedActivity for ConfirmOrderActivity {
    type Input = ();
    type Output = bool;

    fn name(&self) -> &'static str {
        "confirm_order"
    }

    fn execute(&self, ctx: ActivityContext, _input: ()) -> TypedActivityFuture<bool> {
        Box::pin(async move {
            let order: OrderInput = ctx.shared_state()?;
            tokio::time::sleep(Duration::from_millis(30)).await;
            println!(
                "[activity] confirmed order {} for {}",
                order.order_id, order.customer
            );
            Ok(true)
        })
    }

    fn max_attempts(&self) -> u32 {
        1
    }
}

struct NotifyCustomerActivity;

impl TypedActivity for NotifyCustomerActivity {
    type Input = OrderInput;
    type Output = String;

    fn name(&self) -> &'static str {
        "notify_customer"
    }

    fn execute(&self, _ctx: ActivityContext, input: OrderInput) -> TypedActivityFuture<String> {
        Box::pin(async move {
            let msg = format!(
                "Dear {}, order {} is confirmed!",
                input.customer, input.order_id
            );
            println!("[activity] {}", msg);
            Ok(msg)
        })
    }

    fn max_attempts(&self) -> u32 {
        1
    }
}

struct OrderWorkflow;

impl TypedWorkflow for OrderWorkflow {
    type Input = OrderInput;
    type Output = OrderOutput;

    fn name(&self) -> &'static str {
        "order"
    }

    fn run(&self, ctx: WorkflowContext, input: OrderInput) -> TypedWorkflowFuture<OrderOutput> {
        Box::pin(async move {
            ctx.set_shared_state(&input)?;
            let confirmed: bool = ctx.execute_activity_typed("confirm_order", &()).await?;
            let message: String = ctx
                .execute_activity_typed("notify_customer", &input)
                .await?;
            Ok(OrderOutput {
                order_id: input.order_id,
                confirmed,
                message,
            })
        })
    }
}

// ── Axum handlers ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct GreetRequest {
    name: String,
}

#[derive(Serialize)]
struct RunResponse {
    run_id: String,
}

async fn start_greeting(
    State(engine): State<Arc<WorkflowEngine>>,
    Json(payload): Json<GreetRequest>,
) -> std::result::Result<Json<RunResponse>, StatusCode> {
    let run_id = engine
        .start_workflow("greeting", json!({ "name": payload.name }))
        .await
        .map_err(|e| {
            tracing::error!("failed to start workflow: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(RunResponse {
        run_id: run_id.to_string(),
    }))
}

async fn start_resource_workflow(
    State(engine): State<Arc<WorkflowEngine>>,
    Json(payload): Json<serde_json::Value>,
) -> std::result::Result<Json<RunResponse>, StatusCode> {
    let resource = payload["resource"].as_str().unwrap_or("default-resource");
    let run_id = engine
        .start_workflow(ResourceWorkflow::NAME, json!({ "resource": resource }))
        .await
        .map_err(|e| {
            tracing::error!("failed to start resource workflow: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(RunResponse {
        run_id: run_id.to_string(),
    }))
}

async fn start_order(
    State(engine): State<Arc<WorkflowEngine>>,
    Json(payload): Json<OrderInput>,
) -> std::result::Result<Json<RunResponse>, StatusCode> {
    let run_id = engine
        .start_workflow_typed("order", &payload)
        .await
        .map_err(|e| {
            tracing::error!("failed to start order workflow: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(RunResponse {
        run_id: run_id.to_string(),
    }))
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("gears=debug,info")
        .init();

    let storage = SqliteStorage::open("gears-demo.db").await?;

    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(GreetingWorkflow)
        .register_workflow(HeartbeatWorkflow)
        .register_workflow(ResourceWorkflow)
        .register_workflow(OrderWorkflow)
        .register_activity(SendGreetingActivity)
        .register_activity(AllocateResourceActivity)
        .register_activity(ReleaseResourceActivity)
        .register_activity(ConfirmOrderActivity)
        .register_activity(NotifyCustomerActivity)
        .max_concurrent_workflows(50)
        .build()
        .await?;

    let engine_handle = engine.run().await?;

    engine
        .schedule_workflow(
            "heartbeat-30s",
            "*/30 * * * * *",
            HeartbeatWorkflow::NAME,
            json!({}),
        )
        .await?;
    println!("Scheduled heartbeat every 30 seconds.");

    let engine = Arc::new(engine);

    let app = Router::new()
        .route("/greet", post(start_greeting))
        .route("/resource", post(start_resource_workflow))
        .route("/order", post(start_order))
        .nest("/api", management_router())
        .with_state(engine.clone())
        .merge(SwaggerUi::new("/swagger-ui").url("/swagger-ui/openapi.json", openapi_spec()));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("Listening on http://localhost:3000");
    println!("Management API: http://localhost:3000/api/runs");
    println!("Swagger UI:     http://localhost:3000/swagger-ui");
    println!("TUI controller: cargo run --bin gears-ctl");

    tokio::select! {
        result = axum::serve(listener, app) => { result?; }
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down...");
            engine_handle.shutdown().await;
        }
    }

    Ok(())
}
