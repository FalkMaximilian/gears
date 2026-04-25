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
use utoipa::{IntoParams, OpenApi, ToSchema};

use crate::{
    GearsError, WorkflowEngine,
    engine::ActivityInfo,
    event::{EventPayload, WorkflowEvent},
    traits::{RunFilter, RunStatus, ScheduleRecord, ScheduleStatus},
};

// ── Response types ────────────────────────────────────────────────────────

#[derive(Serialize, ToSchema)]
pub struct RunSummary {
    pub run_id: Uuid,
    pub workflow_name: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize, ToSchema)]
pub struct RunDetail {
    pub run_id: Uuid,
    pub workflow_name: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    #[schema(value_type = Object)]
    pub result: Option<Value>,
}

#[derive(Serialize, ToSchema)]
pub struct ScheduleResponse {
    pub name: String,
    pub cron_expression: String,
    pub workflow_name: String,
    #[schema(value_type = Object)]
    pub input: Value,
    pub status: String,
    pub created_at: String,
    pub last_fired_at: Option<String>,
}

#[derive(Deserialize, ToSchema)]
pub struct CreateScheduleRequest {
    pub name: String,
    pub cron_expression: String,
    pub workflow_name: String,
    #[serde(default)]
    #[schema(value_type = Object)]
    pub input: Value,
}

#[derive(Deserialize, ToSchema)]
pub struct StartRunRequest {
    pub workflow_name: String,
    #[serde(default)]
    #[schema(value_type = Object)]
    pub input: Value,
}

#[derive(Serialize, ToSchema)]
pub struct StartRunResponse {
    pub run_id: Uuid,
}

#[derive(Deserialize, Default, IntoParams)]
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

#[utoipa::path(
    get,
    path = "/runs",
    params(RunListQuery),
    responses(
        (status = 200, description = "List of workflow runs", body = Vec<RunSummary>),
        (status = 500, description = "Internal server error"),
    ),
    tag = "runs"
)]
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

#[utoipa::path(
    post,
    path = "/runs",
    request_body = StartRunRequest,
    responses(
        (status = 201, description = "Run created", body = StartRunResponse),
        (status = 422, description = "Workflow not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "runs"
)]
async fn start_run_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Json(req): Json<StartRunRequest>,
) -> impl IntoResponse {
    match engine.start_workflow(&req.workflow_name, req.input).await {
        Ok(run_id) => (StatusCode::CREATED, Json(StartRunResponse { run_id })).into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

#[utoipa::path(
    get,
    path = "/runs/{id}",
    params(
        ("id" = Uuid, Path, description = "Run UUID"),
    ),
    responses(
        (status = 200, description = "Run detail", body = RunDetail),
        (status = 404, description = "Run not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "runs"
)]
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

#[utoipa::path(
    post,
    path = "/runs/{id}/cancel",
    params(
        ("id" = Uuid, Path, description = "Run UUID"),
    ),
    responses(
        (status = 204, description = "Cancellation requested"),
        (status = 404, description = "Run not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "runs"
)]
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

#[utoipa::path(
    get,
    path = "/runs/{id}/events",
    params(
        ("id" = Uuid, Path, description = "Run UUID"),
    ),
    responses(
        (status = 200, description = "Ordered event log", body = Vec<WorkflowEvent>),
        (status = 404, description = "Run not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "runs"
)]
async fn get_run_events_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    match engine.get_run_events(id).await {
        Ok(events) => Json(events).into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

#[utoipa::path(
    get,
    path = "/schedules",
    responses(
        (status = 200, description = "List of schedules", body = Vec<ScheduleResponse>),
        (status = 500, description = "Internal server error"),
    ),
    tag = "schedules"
)]
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

#[utoipa::path(
    post,
    path = "/schedules",
    request_body = CreateScheduleRequest,
    responses(
        (status = 201, description = "Schedule created"),
        (status = 422, description = "Invalid cron expression or workflow not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "schedules"
)]
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

#[utoipa::path(
    delete,
    path = "/schedules/{name}",
    params(
        ("name" = String, Path, description = "Schedule name"),
    ),
    responses(
        (status = 204, description = "Schedule deleted"),
        (status = 404, description = "Schedule not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "schedules"
)]
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

#[utoipa::path(
    post,
    path = "/schedules/{name}/pause",
    params(
        ("name" = String, Path, description = "Schedule name"),
    ),
    responses(
        (status = 204, description = "Schedule paused"),
        (status = 404, description = "Schedule not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "schedules"
)]
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

#[utoipa::path(
    post,
    path = "/schedules/{name}/resume",
    params(
        ("name" = String, Path, description = "Schedule name"),
    ),
    responses(
        (status = 204, description = "Schedule resumed"),
        (status = 404, description = "Schedule not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "schedules"
)]
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

#[utoipa::path(
    get,
    path = "/workflows",
    responses(
        (status = 200, description = "Names of all registered workflows", body = Vec<String>),
    ),
    tag = "registered"
)]
async fn list_workflows_handler(State(engine): State<Arc<WorkflowEngine>>) -> impl IntoResponse {
    Json(engine.workflow_names()).into_response()
}

#[utoipa::path(
    get,
    path = "/activities",
    responses(
        (status = 200, description = "Metadata for all registered activities", body = Vec<ActivityInfo>),
    ),
    tag = "registered"
)]
async fn list_activities_handler(State(engine): State<Arc<WorkflowEngine>>) -> impl IntoResponse {
    let info: Vec<ActivityInfo> = engine.activity_info_list();
    Json(info).into_response()
}

// ── OpenAPI spec ──────────────────────────────────────────────────────────

#[derive(OpenApi)]
#[openapi(
    paths(
        list_runs,
        start_run_handler,
        get_run,
        cancel_run,
        get_run_events_handler,
        list_schedules_handler,
        create_schedule_handler,
        delete_schedule_handler,
        pause_schedule_handler,
        resume_schedule_handler,
        list_workflows_handler,
        list_activities_handler,
    ),
    components(schemas(
        RunSummary,
        RunDetail,
        StartRunRequest,
        StartRunResponse,
        CreateScheduleRequest,
        ScheduleResponse,
        ActivityInfo,
        WorkflowEvent,
        EventPayload,
    )),
    tags(
        (name = "runs", description = "Workflow run management"),
        (name = "schedules", description = "Cron schedule management"),
        (name = "registered", description = "Registered workflows and activities"),
    ),
    info(
        title = "Gears Workflow Engine API",
        version = "0.1.0",
        description = "Management API for the Gears durable workflow execution engine",
    ),
)]
struct GearsApiDoc;

/// Returns the generated OpenAPI 3.1 specification for the management API.
pub fn openapi_spec() -> utoipa::openapi::OpenApi {
    GearsApiDoc::openapi()
}

async fn openapi_json_handler() -> impl IntoResponse {
    Json(GearsApiDoc::openapi())
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
///
/// The OpenAPI spec is served at `GET /openapi.json` relative to the mount prefix.
pub fn management_router() -> Router<Arc<WorkflowEngine>> {
    Router::new()
        .route("/openapi.json", get(openapi_json_handler))
        .route("/runs", get(list_runs).post(start_run_handler))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/events", get(get_run_events_handler))
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
