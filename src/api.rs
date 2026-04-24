use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    GearsError, WorkflowEngine,
    traits::{RunFilter, RunStatus, ScheduleRecord, ScheduleStatus},
};

// ── Response types ────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct RunSummary {
    pub run_id: Uuid,
    pub workflow_name: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize)]
pub struct RunDetail {
    pub run_id: Uuid,
    pub workflow_name: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    pub result: Option<Value>,
}

#[derive(Serialize)]
pub struct ScheduleResponse {
    pub name: String,
    pub cron_expression: String,
    pub workflow_name: String,
    pub input: Value,
    pub status: String,
    pub created_at: String,
    pub last_fired_at: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateScheduleRequest {
    pub name: String,
    pub cron_expression: String,
    pub workflow_name: String,
    #[serde(default)]
    pub input: Value,
}

#[derive(Deserialize, Default)]
pub struct RunListQuery {
    pub status: Option<String>,
    pub workflow_name: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

// ── Helpers ───────────────────────────────────────────────────────────────

fn status_str(s: &RunStatus) -> &'static str {
    match s {
        RunStatus::Running => "running",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
        RunStatus::NotFound => "not_found",
    }
}

fn parse_status(s: &str) -> Option<RunStatus> {
    match s {
        "running" => Some(RunStatus::Running),
        "completed" => Some(RunStatus::Completed),
        "failed" => Some(RunStatus::Failed),
        "cancelled" => Some(RunStatus::Cancelled),
        _ => None,
    }
}

fn to_schedule_response(r: ScheduleRecord) -> ScheduleResponse {
    ScheduleResponse {
        name: r.name,
        cron_expression: r.cron_expression,
        workflow_name: r.workflow_name,
        input: r.input,
        status: match r.status {
            ScheduleStatus::Active => "active".into(),
            ScheduleStatus::Paused => "paused".into(),
        },
        created_at: r.created_at.to_rfc3339(),
        last_fired_at: r.last_fired_at.map(|t| t.to_rfc3339()),
    }
}

fn engine_err(e: GearsError) -> (StatusCode, String) {
    match e {
        GearsError::RunNotFound(_) | GearsError::ScheduleNotFound(_) => {
            (StatusCode::NOT_FOUND, e.to_string())
        }
        GearsError::WorkflowNotFound(_) | GearsError::InvalidSchedule(_) => {
            (StatusCode::UNPROCESSABLE_ENTITY, e.to_string())
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

// ── Handlers ──────────────────────────────────────────────────────────────

async fn list_runs(
    State(engine): State<Arc<WorkflowEngine>>,
    Query(q): Query<RunListQuery>,
) -> impl IntoResponse {
    let filter = RunFilter {
        status: q.status.as_deref().and_then(parse_status),
        workflow_name: q.workflow_name,
        limit: q.limit,
        offset: q.offset,
        ..Default::default()
    };
    match engine.list_runs(&filter).await {
        Ok(runs) => {
            let body: Vec<RunSummary> = runs
                .into_iter()
                .map(|r| RunSummary {
                    run_id: r.run_id,
                    workflow_name: r.workflow_name,
                    status: status_str(&r.status).to_string(),
                    created_at: r.created_at.to_rfc3339(),
                    updated_at: r.updated_at.to_rfc3339(),
                })
                .collect();
            Json(body).into_response()
        }
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

async fn get_run(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    let filter = RunFilter::default();
    let (list_res, result_res) =
        tokio::join!(engine.list_runs(&filter), engine.get_run_result(id),);
    match (list_res, result_res) {
        (Ok(runs), Ok(result)) => match runs.into_iter().find(|r| r.run_id == id) {
            Some(run) => Json(RunDetail {
                run_id: run.run_id,
                workflow_name: run.workflow_name,
                status: status_str(&run.status).to_string(),
                created_at: run.created_at.to_rfc3339(),
                updated_at: run.updated_at.to_rfc3339(),
                result,
            })
            .into_response(),
            None => (StatusCode::NOT_FOUND, "run not found".to_string()).into_response(),
        },
        (Err(e), _) | (_, Err(e)) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

async fn cancel_run(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    match engine.cancel_workflow(id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

async fn list_schedules_handler(
    State(engine): State<Arc<WorkflowEngine>>,
) -> impl IntoResponse {
    match engine.list_schedules().await {
        Ok(schedules) => {
            Json(schedules.into_iter().map(to_schedule_response).collect::<Vec<_>>())
                .into_response()
        }
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

async fn create_schedule_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Json(req): Json<CreateScheduleRequest>,
) -> impl IntoResponse {
    match engine
        .schedule_workflow(&req.name, &req.cron_expression, &req.workflow_name, req.input)
        .await
    {
        Ok(()) => StatusCode::CREATED.into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

async fn delete_schedule_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match engine.delete_schedule(&name).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

async fn pause_schedule_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match engine.pause_schedule(&name).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

async fn resume_schedule_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match engine.resume_schedule(&name).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

async fn list_workflows_handler(State(engine): State<Arc<WorkflowEngine>>) -> impl IntoResponse {
    Json(engine.workflow_names()).into_response()
}

async fn list_activities_handler(State(engine): State<Arc<WorkflowEngine>>) -> impl IntoResponse {
    Json(engine.activity_names()).into_response()
}

// ── Router ────────────────────────────────────────────────────────────────

/// Returns an Axum router exposing all engine management endpoints.
/// Mount it under a prefix in your application:
///
/// ```ignore
/// let app = Router::new()
///     .nest("/api", gears::management_router())
///     .with_state(Arc::new(engine));
/// ```
pub fn management_router() -> Router<Arc<WorkflowEngine>> {
    Router::new()
        .route("/runs", get(list_runs))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/cancel", post(cancel_run))
        .route(
            "/schedules",
            get(list_schedules_handler).post(create_schedule_handler),
        )
        .route("/schedules/{name}", delete(delete_schedule_handler))
        .route("/schedules/{name}/pause", post(pause_schedule_handler))
        .route("/schedules/{name}/resume", post(resume_schedule_handler))
        .route("/workflows", get(list_workflows_handler))
        .route("/activities", get(list_activities_handler))
}
