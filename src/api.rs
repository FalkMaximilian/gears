use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;
use utoipa::{IntoParams, OpenApi, ToSchema};

use crate::{
    GearsError, WorkflowEngine,
    engine::{ActivityInfo, EngineInfo, WorkflowInfo},
    event::{EventPayload, WorkflowEvent},
    traits::{PendingTask, RunFilter, RunStatus, ScheduleRecord, ScheduleStatus},
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
    /// Error message when status is `"failed"`. `null` for all other statuses.
    pub error: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct RunStats {
    pub total: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
}

#[derive(Deserialize, ToSchema)]
pub struct PatchScheduleRequest {
    pub cron_expression: Option<String>,
    #[serde(default)]
    #[schema(value_type = Object)]
    pub input: Option<Value>,
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
    /// ISO 8601 datetime — only return runs created at or after this time.
    pub created_after: Option<String>,
    /// ISO 8601 datetime — only return runs created at or before this time.
    pub created_before: Option<String>,
}

#[derive(Deserialize, Default, IntoParams)]
pub struct ScheduleListQuery {
    /// Filter by workflow name (exact match).
    pub workflow_name: Option<String>,
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

fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s).ok().map(|dt| dt.with_timezone(&Utc))
}

fn extract_run_error(events: &[WorkflowEvent]) -> Option<String> {
    events.iter().rev().find_map(|e| {
        if let EventPayload::WorkflowFailed { error } = &e.payload {
            Some(error.clone())
        } else {
            None
        }
    })
}

fn engine_err(e: GearsError) -> (StatusCode, String) {
    match e {
        GearsError::RunNotFound(_)
        | GearsError::ScheduleNotFound(_)
        | GearsError::TaskNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
        GearsError::WorkflowNotFound(_) | GearsError::InvalidSchedule(_) => {
            (StatusCode::UNPROCESSABLE_ENTITY, e.to_string())
        }
        GearsError::TaskAlreadyResolved(_) | GearsError::HeartbeatMismatch(_) => {
            (StatusCode::CONFLICT, e.to_string())
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
        created_after: q.created_after.as_deref().and_then(parse_datetime),
        created_before: q.created_before.as_deref().and_then(parse_datetime),
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
    let (list_res, result_res, events_res) = tokio::join!(
        engine.list_runs(&filter),
        engine.get_run_result(id),
        engine.get_run_events(id),
    );
    match (list_res, result_res, events_res) {
        (Ok(runs), Ok(result), Ok(events)) => match runs.into_iter().find(|r| r.run_id == id) {
            Some(run) => {
                let error = extract_run_error(&events);
                Json(RunDetail {
                    run_id: run.run_id,
                    workflow_name: run.workflow_name,
                    status: status_str(&run.status).to_string(),
                    created_at: run.created_at.to_rfc3339(),
                    updated_at: run.updated_at.to_rfc3339(),
                    result,
                    error,
                })
                .into_response()
            }
            None => (StatusCode::NOT_FOUND, "run not found".to_string()).into_response(),
        },
        (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
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
    post,
    path = "/runs/prune",
    responses(
        (status = 204, description = "Pruning pass triggered"),
    ),
    tag = "runs"
)]
async fn prune_runs_handler(State(engine): State<Arc<WorkflowEngine>>) -> impl IntoResponse {
    engine.prune_now().await;
    StatusCode::NO_CONTENT.into_response()
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
    params(ScheduleListQuery),
    responses(
        (status = 200, description = "List of schedules", body = Vec<ScheduleResponse>),
        (status = 500, description = "Internal server error"),
    ),
    tag = "schedules"
)]
async fn list_schedules_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Query(q): Query<ScheduleListQuery>,
) -> impl IntoResponse {
    match engine.list_schedules().await {
        Ok(schedules) => {
            let filtered: Vec<_> = schedules
                .into_iter()
                .filter(|s| {
                    q.workflow_name
                        .as_deref()
                        .map_or(true, |wn| s.workflow_name == wn)
                })
                .map(to_schedule_response)
                .collect();
            Json(filtered).into_response()
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
    path = "/schedules/{name}",
    params(
        ("name" = String, Path, description = "Schedule name"),
    ),
    responses(
        (status = 200, description = "Schedule detail", body = ScheduleResponse),
        (status = 404, description = "Schedule not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "schedules"
)]
async fn get_schedule_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match engine.get_schedule(&name).await {
        Ok(record) => Json(to_schedule_response(record)).into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

#[utoipa::path(
    patch,
    path = "/schedules/{name}",
    params(
        ("name" = String, Path, description = "Schedule name"),
    ),
    request_body = PatchScheduleRequest,
    responses(
        (status = 200, description = "Schedule updated", body = ScheduleResponse),
        (status = 404, description = "Schedule not found"),
        (status = 422, description = "Invalid cron expression"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "schedules"
)]
async fn patch_schedule_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(name): Path<String>,
    Json(req): Json<PatchScheduleRequest>,
) -> impl IntoResponse {
    let existing = match engine.get_schedule(&name).await {
        Ok(r) => r,
        Err(e) => {
            let (code, msg) = engine_err(e);
            return (code, msg).into_response();
        }
    };
    let new_cron = req
        .cron_expression
        .as_deref()
        .unwrap_or(&existing.cron_expression);
    let new_input = req.input.unwrap_or(existing.input);
    match engine
        .schedule_workflow(&name, new_cron, &existing.workflow_name, new_input)
        .await
    {
        Ok(()) => match engine.get_schedule(&name).await {
            Ok(record) => Json(to_schedule_response(record)).into_response(),
            Err(e) => {
                let (code, msg) = engine_err(e);
                (code, msg).into_response()
            }
        },
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

#[utoipa::path(
    post,
    path = "/schedules/{name}/trigger",
    params(
        ("name" = String, Path, description = "Schedule name"),
    ),
    responses(
        (status = 201, description = "Run started", body = StartRunResponse),
        (status = 404, description = "Schedule not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "schedules"
)]
async fn trigger_schedule_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match engine.trigger_schedule(&name).await {
        Ok(run_id) => (StatusCode::CREATED, Json(StartRunResponse { run_id })).into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

#[utoipa::path(
    get,
    path = "/runs/stats",
    responses(
        (status = 200, description = "Aggregate run counts by status", body = RunStats),
        (status = 500, description = "Internal server error"),
    ),
    tag = "runs"
)]
async fn run_stats_handler(State(engine): State<Arc<WorkflowEngine>>) -> impl IntoResponse {
    let filter = RunFilter::default();
    match engine.list_runs(&filter).await {
        Ok(runs) => {
            let mut stats = RunStats {
                total: runs.len(),
                running: 0,
                completed: 0,
                failed: 0,
                cancelled: 0,
            };
            for run in &runs {
                match run.status {
                    RunStatus::Running => stats.running += 1,
                    RunStatus::Completed => stats.completed += 1,
                    RunStatus::Failed => stats.failed += 1,
                    RunStatus::Cancelled => stats.cancelled += 1,
                    RunStatus::NotFound => {}
                }
            }
            Json(stats).into_response()
        }
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

#[utoipa::path(
    get,
    path = "/engine/info",
    responses(
        (status = 200, description = "Engine runtime configuration snapshot", body = EngineInfo),
    ),
    tag = "engine"
)]
async fn engine_info_handler(State(engine): State<Arc<WorkflowEngine>>) -> impl IntoResponse {
    Json(engine.engine_info()).into_response()
}

#[utoipa::path(
    get,
    path = "/workflows",
    responses(
        (status = 200, description = "Metadata for all registered workflows", body = Vec<WorkflowInfo>),
    ),
    tag = "registered"
)]
async fn list_workflows_handler(State(engine): State<Arc<WorkflowEngine>>) -> impl IntoResponse {
    Json(engine.workflow_info_list()).into_response()
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

// ── Worker endpoints ──────────────────────────────────────────────────────

#[derive(Deserialize, ToSchema)]
pub struct PollTaskRequest {
    pub worker_id: String,
    pub activity_names: Vec<String>,
    /// How long to wait (ms) if no task is immediately available. Default: 30 000.
    pub long_poll_timeout_ms: Option<u64>,
}

#[derive(Serialize, ToSchema)]
pub struct TaskAssignment {
    pub task_token: Uuid,
    pub run_id: Uuid,
    pub activity_name: String,
    #[schema(value_type = Object)]
    pub input: Value,
    pub attempt: u32,
    pub sequence_id: u32,
}

#[derive(Deserialize, ToSchema)]
pub struct CompleteTaskRequest {
    #[serde(default)]
    #[schema(value_type = Object)]
    pub output: Value,
}

#[derive(Deserialize, ToSchema)]
pub struct FailTaskRequest {
    pub error: String,
}

#[derive(Serialize, ToSchema)]
pub struct HeartbeatResponse {
    /// `true` if the workflow run has been cancelled — the worker should stop.
    pub run_cancelled: bool,
}

fn to_task_assignment(t: PendingTask) -> TaskAssignment {
    TaskAssignment {
        task_token: t.task_token,
        run_id: t.run_id,
        activity_name: t.activity_name,
        input: t.input,
        attempt: t.attempt,
        sequence_id: t.sequence_id,
    }
}

#[utoipa::path(
    post,
    path = "/workers/poll",
    request_body = PollTaskRequest,
    responses(
        (status = 200, description = "Task assigned", body = TaskAssignment),
        (status = 204, description = "No task available within the long-poll window"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "workers"
)]
async fn poll_task_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Json(req): Json<PollTaskRequest>,
) -> impl IntoResponse {
    let timeout_ms = req.long_poll_timeout_ms.unwrap_or(30_000);
    match engine
        .poll_task(req.activity_names, req.worker_id, timeout_ms)
        .await
    {
        Ok(Some(task)) => (StatusCode::OK, Json(to_task_assignment(task))).into_response(),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

#[utoipa::path(
    get,
    path = "/workers/tasks/{task_token}",
    params(("task_token" = Uuid, Path, description = "Task token")),
    responses(
        (status = 200, description = "Task detail", body = PendingTask),
        (status = 404, description = "Task not found"),
    ),
    tag = "workers"
)]
async fn get_task_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(task_token): Path<Uuid>,
) -> impl IntoResponse {
    match engine.get_pending_task(task_token).await {
        Ok(task) => Json(task).into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

#[utoipa::path(
    post,
    path = "/workers/tasks/{task_token}/complete",
    params(("task_token" = Uuid, Path, description = "Task token")),
    request_body = CompleteTaskRequest,
    responses(
        (status = 200, description = "Task marked completed"),
        (status = 404, description = "Task not found"),
        (status = 409, description = "Task already resolved"),
    ),
    tag = "workers"
)]
async fn complete_task_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(task_token): Path<Uuid>,
    Json(req): Json<CompleteTaskRequest>,
) -> impl IntoResponse {
    match engine.complete_task(task_token, req.output).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

#[utoipa::path(
    post,
    path = "/workers/tasks/{task_token}/fail",
    params(("task_token" = Uuid, Path, description = "Task token")),
    request_body = FailTaskRequest,
    responses(
        (status = 200, description = "Task marked failed"),
        (status = 404, description = "Task not found"),
        (status = 409, description = "Task already resolved"),
    ),
    tag = "workers"
)]
async fn fail_task_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(task_token): Path<Uuid>,
    Json(req): Json<FailTaskRequest>,
) -> impl IntoResponse {
    match engine.fail_task(task_token, req.error).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

#[utoipa::path(
    post,
    path = "/workers/tasks/{task_token}/heartbeat",
    params(("task_token" = Uuid, Path, description = "Task token")),
    responses(
        (status = 200, description = "Heartbeat accepted", body = HeartbeatResponse),
        (status = 404, description = "Task not found"),
        (status = 409, description = "Task not in claimed status"),
    ),
    tag = "workers"
)]
async fn heartbeat_task_handler(
    State(engine): State<Arc<WorkflowEngine>>,
    Path(task_token): Path<Uuid>,
) -> impl IntoResponse {
    match engine.heartbeat_task(task_token).await {
        Ok(run_cancelled) => Json(HeartbeatResponse { run_cancelled }).into_response(),
        Err(e) => {
            let (code, msg) = engine_err(e);
            (code, msg).into_response()
        }
    }
}

// ── OpenAPI spec ──────────────────────────────────────────────────────────

#[derive(OpenApi)]
#[openapi(
    paths(
        list_runs,
        start_run_handler,
        get_run,
        cancel_run,
        prune_runs_handler,
        run_stats_handler,
        get_run_events_handler,
        list_schedules_handler,
        create_schedule_handler,
        get_schedule_handler,
        patch_schedule_handler,
        delete_schedule_handler,
        pause_schedule_handler,
        resume_schedule_handler,
        trigger_schedule_handler,
        list_workflows_handler,
        list_activities_handler,
        engine_info_handler,
        poll_task_handler,
        get_task_handler,
        complete_task_handler,
        fail_task_handler,
        heartbeat_task_handler,
    ),
    components(schemas(
        RunSummary,
        RunDetail,
        RunStats,
        StartRunRequest,
        StartRunResponse,
        CreateScheduleRequest,
        PatchScheduleRequest,
        ScheduleResponse,
        ActivityInfo,
        WorkflowInfo,
        EngineInfo,
        WorkflowEvent,
        EventPayload,
        PendingTask,
        PollTaskRequest,
        TaskAssignment,
        CompleteTaskRequest,
        FailTaskRequest,
        HeartbeatResponse,
    )),
    tags(
        (name = "runs", description = "Workflow run management"),
        (name = "schedules", description = "Cron schedule management"),
        (name = "registered", description = "Registered workflows and activities"),
        (name = "engine", description = "Engine health and configuration"),
        (name = "workers", description = "External worker task queue"),
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
        .route("/runs/stats", get(run_stats_handler))
        .route("/runs/prune", post(prune_runs_handler))
        .route("/runs/{id}", get(get_run))
        .route("/runs/{id}/events", get(get_run_events_handler))
        .route("/runs/{id}/cancel", post(cancel_run))
        .route(
            "/schedules",
            get(list_schedules_handler).post(create_schedule_handler),
        )
        .route(
            "/schedules/{name}",
            get(get_schedule_handler)
                .patch(patch_schedule_handler)
                .delete(delete_schedule_handler),
        )
        .route("/schedules/{name}/trigger", post(trigger_schedule_handler))
        .route("/schedules/{name}/pause", post(pause_schedule_handler))
        .route("/schedules/{name}/resume", post(resume_schedule_handler))
        .route("/workflows", get(list_workflows_handler))
        .route("/activities", get(list_activities_handler))
        .route("/engine/info", get(engine_info_handler))
        .route("/workers/poll", post(poll_task_handler))
        .route("/workers/tasks/{task_token}", get(get_task_handler))
        .route(
            "/workers/tasks/{task_token}/complete",
            post(complete_task_handler),
        )
        .route(
            "/workers/tasks/{task_token}/fail",
            post(fail_task_handler),
        )
        .route(
            "/workers/tasks/{task_token}/heartbeat",
            post(heartbeat_task_handler),
        )
}
