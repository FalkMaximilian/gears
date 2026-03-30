//! zdflow demo — start a workflow from an Axum HTTP endpoint.
//!
//! Run:  cargo run --bin zdflow-demo
//! Test: curl -X POST http://localhost:3000/greet \
//!            -H 'Content-Type: application/json' \
//!            -d '{"name": "Alice"}'

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use zdflow::{
    Activity, ActivityContext, ActivityFuture, SqliteStorage, Workflow, WorkflowContext,
    WorkflowEngine, WorkflowFuture,
};

// ── Activity: send a greeting ─────────────────────────────────────────────

struct SendGreetingActivity;

impl SendGreetingActivity {
    pub const NAME: &'static str = "send_greeting";
}

impl Activity for SendGreetingActivity {
    fn name(&self) -> &'static str { Self::NAME }

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

impl GreetingWorkflow {
    pub const NAME: &'static str = "greeting";
}

impl Workflow for GreetingWorkflow {
    fn name(&self) -> &'static str { Self::NAME }

    fn run(&self, ctx: WorkflowContext, input: Value) -> WorkflowFuture {
        Box::pin(async move {
            // First greeting.
            let result1 = ctx.execute_activity(SendGreetingActivity::NAME, input.clone()).await?;
            println!("[workflow] first greeting: {:?}", result1["message"]);

            // Durable sleep — if the process crashes here and restarts,
            // the workflow resumes with only the remaining time left.
            ctx.sleep(Duration::from_secs(2)).await?;
            println!("[workflow] woke up after sleep");

            // Second greeting.
            let result2 = ctx.execute_activity(SendGreetingActivity::NAME, input.clone()).await?;
            println!("[workflow] second greeting: {:?}", result2["message"]);

            Ok(json!({
                "first":  result1,
                "second": result2,
            }))
        })
    }
}

// ── Workflow: heartbeat (used by the cron schedule) ───────────────────────

struct HeartbeatWorkflow;

impl HeartbeatWorkflow {
    pub const NAME: &'static str = "heartbeat";
}

impl Workflow for HeartbeatWorkflow {
    fn name(&self) -> &'static str { Self::NAME }

    fn run(&self, _ctx: WorkflowContext, _input: Value) -> WorkflowFuture {
        Box::pin(async move {
            println!("[heartbeat] tick at {}", chrono::Utc::now());
            Ok(json!({ "status": "ok" }))
        })
    }
}

// ── Axum handlers ─────────────────────────────────────────────────────────

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

// ── Schedule handlers ─────────────────────────────────────────────────────

#[derive(Serialize)]
struct ScheduleResponse {
    name: String,
    cron_expression: String,
    workflow_name: String,
    status: String,
    last_fired_at: Option<String>,
}

async fn list_schedules_handler(
    State(engine): State<Arc<WorkflowEngine>>,
) -> std::result::Result<Json<Vec<ScheduleResponse>>, StatusCode> {
    let schedules = engine.list_schedules().await.map_err(|e| {
        tracing::error!("failed to list schedules: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let resp = schedules
        .into_iter()
        .map(|s| ScheduleResponse {
            name: s.name,
            cron_expression: s.cron_expression,
            workflow_name: s.workflow_name,
            status: match s.status {
                zdflow::ScheduleStatus::Active => "active".into(),
                zdflow::ScheduleStatus::Paused => "paused".into(),
            },
            last_fired_at: s.last_fired_at.map(|t| t.to_rfc3339()),
        })
        .collect();
    Ok(Json(resp))
}

async fn pause_schedule_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(name): Path<String>,
) -> std::result::Result<StatusCode, StatusCode> {
    engine.pause_schedule(&name).await.map_err(|e| {
        tracing::error!(schedule = %name, "failed to pause schedule: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn resume_schedule_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(name): Path<String>,
) -> std::result::Result<StatusCode, StatusCode> {
    engine.resume_schedule(&name).await.map_err(|e| {
        tracing::error!(schedule = %name, "failed to resume schedule: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(StatusCode::NO_CONTENT)
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
        .register_workflow(HeartbeatWorkflow)
        .register_activity(SendGreetingActivity)
        .max_concurrent_workflows(50)
        .build()
        .await?;

    let engine_handle = engine.run().await?;

    // Register a heartbeat schedule that fires every 30 seconds.
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
        .route("/schedules", get(list_schedules_handler))
        .route("/schedules/{name}/pause", post(pause_schedule_handler))
        .route("/schedules/{name}/resume", post(resume_schedule_handler))
        .with_state(engine.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("Listening on http://localhost:3000");
    println!(
        "Try: curl -X POST http://localhost:3000/greet -H 'Content-Type: application/json' -d '{{\"name\": \"Alice\"}}'"
    );
    println!("     curl http://localhost:3000/schedules");

    tokio::select! {
        result = axum::serve(listener, app) => { result?; }
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down...");
            engine_handle.shutdown().await;
        }
    }

    Ok(())
}
