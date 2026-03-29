//! zdflow demo — start a workflow from an Axum HTTP endpoint.
//!
//! Run:  cargo run --bin zdflow-demo
//! Test: curl -X POST http://localhost:3000/greet \
//!            -H 'Content-Type: application/json' \
//!            -d '{"name": "Alice"}'

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use zdflow::{
    Activity, ActivityContext, ActivityFuture,
    SqliteStorage, Workflow, WorkflowContext, WorkflowFuture,
    WorkflowEngine,
};

// ── Activity: send a greeting ─────────────────────────────────────────────

struct SendGreetingActivity;

impl Activity for SendGreetingActivity {
    fn name(&self) -> &'static str {
        "send_greeting"
    }

    fn execute(&self, ctx: ActivityContext, input: Value) -> ActivityFuture {
        Box::pin(async move {
            let name = input["name"].as_str().unwrap_or("stranger");
            // Simulate some external I/O.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let message = format!("Hello, {}! (attempt #{})", name, ctx.attempt);
            println!("[activity] {}", message);
            Ok(json!({ "message": message }))
        })
    }
}

// ── Workflow: greet → sleep → greet again ────────────────────────────────

struct GreetingWorkflow;

impl Workflow for GreetingWorkflow {
    fn name(&self) -> &'static str {
        "greeting"
    }

    fn run(&self, ctx: WorkflowContext, input: Value) -> WorkflowFuture {
        Box::pin(async move {
            // First greeting.
            let result1 = ctx
                .execute_activity(&SendGreetingActivity, input.clone())
                .await?;
            println!("[workflow] first greeting: {:?}", result1["message"]);

            // Durable sleep — if the process crashes here and restarts,
            // the workflow resumes with only the remaining time left.
            ctx.sleep(Duration::from_secs(2)).await?;
            println!("[workflow] woke up after sleep");

            // Second greeting.
            let result2 = ctx
                .execute_activity(&SendGreetingActivity, input.clone())
                .await?;
            println!("[workflow] second greeting: {:?}", result2["message"]);

            Ok(json!({
                "first":  result1,
                "second": result2,
            }))
        })
    }
}

// ── Axum handler ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct GreetRequest {
    name: String,
}

#[derive(Serialize)]
struct GreetResponse {
    run_id: String,
}

async fn start_greeting(
    State(engine): State<Arc<WorkflowEngine>>,
    Json(payload): Json<GreetRequest>,
) -> std::result::Result<Json<GreetResponse>, StatusCode> {
    let run_id = engine
        .start_workflow("greeting", json!({ "name": payload.name }))
        .await
        .map_err(|e| {
            tracing::error!("failed to start workflow: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(GreetResponse {
        run_id: run_id.to_string(),
    }))
}

// ── Main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("zdflow=debug,info")
        .init();

    let storage = SqliteStorage::open("zdflow-demo.db").await?;

    let mut engine = WorkflowEngine::builder()
        .with_storage(storage)
        .register_workflow(GreetingWorkflow)
        .register_activity(SendGreetingActivity)
        .max_concurrent_workflows(50)
        .build()
        .await?;

    let engine_handle = engine.run().await?;
    let engine = Arc::new(engine);

    let app = Router::new()
        .route("/greet", post(start_greeting))
        .with_state(engine.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("Listening on http://localhost:3000");
    println!("Try: curl -X POST http://localhost:3000/greet -H 'Content-Type: application/json' -d '{{\"name\": \"Alice\"}}'");

    tokio::select! {
        result = axum::serve(listener, app) => { result?; }
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down...");
            engine_handle.shutdown().await;
        }
    }

    Ok(())
}
