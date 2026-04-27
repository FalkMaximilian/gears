use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use tokio_rusqlite::Connection;
use uuid::Uuid;

use crate::error::{Result, GearsError};
use crate::event::{EventPayload, WorkflowEvent};
use crate::traits::{
    PendingTask, RunFilter, RunInfo, RunRecord, RunStatus, ScheduleRecord, ScheduleStatus,
    Storage, StorageFuture, TaskResult,
};

/// SQLite-backed durable storage for workflow events and run state.
///
/// Uses `tokio-rusqlite` to run rusqlite on a dedicated thread without
/// blocking the Tokio runtime.
#[derive(Clone)]
pub struct SqliteStorage {
    conn: Arc<Connection>,
}

impl SqliteStorage {
    /// Open (or create) a SQLite database at the given path.
    /// Use `":memory:"` for an in-memory database (useful in tests).
    pub async fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path).await.map_err(|e| GearsError::Storage(tokio_rusqlite::Error::Error(e)))?;
        let storage = SqliteStorage {
            conn: Arc::new(conn),
        };
        storage.init_schema().await?;
        Ok(storage)
    }

    async fn init_schema(&self) -> Result<()> {
        self.conn
            .call(|conn| {
                conn.execute_batch(
                    "
                    PRAGMA journal_mode=WAL;
                    PRAGMA foreign_keys=ON;

                    CREATE TABLE IF NOT EXISTS workflow_runs (
                        run_id      TEXT PRIMARY KEY,
                        name        TEXT NOT NULL,
                        input       TEXT NOT NULL,
                        status      TEXT NOT NULL,
                        result      TEXT,
                        created_at  INTEGER NOT NULL,
                        updated_at  INTEGER NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS workflow_events (
                        id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id      TEXT NOT NULL,
                        sequence    INTEGER NOT NULL,
                        occurred_at INTEGER NOT NULL,
                        payload     TEXT NOT NULL,
                        FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id)
                    );

                    CREATE INDEX IF NOT EXISTS idx_events_run
                        ON workflow_events(run_id, sequence);

                    CREATE TABLE IF NOT EXISTS workflow_schedules (
                        name             TEXT PRIMARY KEY,
                        cron_expression  TEXT NOT NULL,
                        workflow_name    TEXT NOT NULL,
                        input            TEXT NOT NULL,
                        status           TEXT NOT NULL,
                        created_at       INTEGER NOT NULL,
                        last_fired_at    INTEGER
                    );

                    CREATE TABLE IF NOT EXISTS pending_tasks (
                        task_token                   TEXT PRIMARY KEY,
                        run_id                       TEXT NOT NULL,
                        sequence_id                  INTEGER NOT NULL,
                        activity_name                TEXT NOT NULL,
                        input                        TEXT NOT NULL,
                        attempt                      INTEGER NOT NULL DEFAULT 1,
                        status                       TEXT NOT NULL DEFAULT 'pending',
                        worker_id                    TEXT,
                        heartbeat_at                 INTEGER,
                        schedule_to_start_timeout_ms INTEGER,
                        output                       TEXT,
                        error_message                TEXT,
                        created_at                   INTEGER NOT NULL,
                        updated_at                   INTEGER NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_pending_tasks_status_name
                        ON pending_tasks(status, activity_name);

                    CREATE INDEX IF NOT EXISTS idx_pending_tasks_run_id
                        ON pending_tasks(run_id);
                    ",
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }
}

impl Storage for SqliteStorage {
    fn create_run(&self, run_id: Uuid, workflow_name: &str, input: &Value) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let run_id_str = run_id.to_string();
        let name = workflow_name.to_string();
        let input_json = match serde_json::to_string(input) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(GearsError::Serialize(e)) }),
        };
        let now = Utc::now().timestamp_millis();

        Box::pin(async move {
            conn.call(move |conn| {
                conn.execute(
                    "INSERT INTO workflow_runs
                        (run_id, name, input, status, created_at, updated_at)
                     VALUES (?1, ?2, ?3, 'running', ?4, ?4)",
                    rusqlite::params![run_id_str, name, input_json, now],
                )?;
                Ok(())
            })
            .await?;
            Ok(())
        })
    }

    fn append_event(&self, run_id: Uuid, event: &WorkflowEvent) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let run_id_str = run_id.to_string();
        let payload_json = match serde_json::to_string(&event.payload) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(GearsError::Serialize(e)) }),
        };
        let occurred_at = event.occurred_at.timestamp_millis();
        let sequence = event.sequence;

        Box::pin(async move {
            conn.call(move |conn| {
                conn.execute(
                    "INSERT INTO workflow_events (run_id, sequence, occurred_at, payload)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![run_id_str, sequence, occurred_at, payload_json],
                )?;
                Ok(())
            })
            .await?;
            Ok(())
        })
    }

    fn load_events(&self, run_id: Uuid) -> StorageFuture<Vec<WorkflowEvent>> {
        let conn = self.conn.clone();
        let run_id_str = run_id.to_string();

        Box::pin(async move {
            let rows = conn
                .call(move |conn| {
                    let mut stmt = conn.prepare(
                        "SELECT sequence, occurred_at, payload
                         FROM workflow_events
                         WHERE run_id = ?1
                         ORDER BY sequence ASC",
                    )?;
                    let rows: Vec<(u64, i64, String)> = stmt
                        .query_map(rusqlite::params![run_id_str], |row| {
                            Ok((
                                row.get::<_, u64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        })?
                        .collect::<std::result::Result<_, _>>()?;
                    Ok(rows)
                })
                .await?;

            let mut events = Vec::with_capacity(rows.len());
            for (sequence, occurred_at_ms, payload_json) in rows {
                let occurred_at = Utc
                    .timestamp_millis_opt(occurred_at_ms)
                    .single()
                    .unwrap_or_else(Utc::now);
                let payload: EventPayload = serde_json::from_str(&payload_json)?;
                events.push(WorkflowEvent {
                    sequence,
                    occurred_at,
                    payload,
                });
            }
            Ok(events)
        })
    }

    fn list_running_workflows(&self) -> StorageFuture<Vec<RunRecord>> {
        let conn = self.conn.clone();

        Box::pin(async move {
            let rows: Vec<(String, String, String)> = conn
                .call(|conn| {
                    let mut stmt = conn.prepare(
                        "SELECT run_id, name, input
                         FROM workflow_runs
                         WHERE status = 'running'",
                    )?;
                    let rows = stmt
                        .query_map([], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                            ))
                        })?
                        .collect::<std::result::Result<_, _>>()?;
                    Ok(rows)
                })
                .await?;

            let mut result = Vec::with_capacity(rows.len());
            for (run_id_str, name, input_json) in rows {
                let run_id =
                    Uuid::parse_str(&run_id_str).map_err(|e| GearsError::Other(e.to_string()))?;
                let input: Value = serde_json::from_str(&input_json)?;
                result.push(RunRecord {
                    run_id,
                    workflow_name: name,
                    input,
                });
            }
            Ok(result)
        })
    }

    fn set_run_status(
        &self,
        run_id: Uuid,
        status: RunStatus,
        result: Option<Value>,
    ) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let run_id_str = run_id.to_string();
        let status_str = match status {
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
            RunStatus::NotFound => "not_found",
        }
        .to_string();
        let result_json = match result.map(|v| serde_json::to_string(&v)).transpose() {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(GearsError::Serialize(e)) }),
        };
        let now = Utc::now().timestamp_millis();

        Box::pin(async move {
            conn.call(move |conn| {
                conn.execute(
                    "UPDATE workflow_runs
                     SET status = ?1, result = ?2, updated_at = ?3
                     WHERE run_id = ?4",
                    rusqlite::params![status_str, result_json, now, run_id_str],
                )?;
                Ok(())
            })
            .await?;
            Ok(())
        })
    }

    fn get_run_status(&self, run_id: Uuid) -> StorageFuture<RunStatus> {
        let conn = self.conn.clone();
        let run_id_str = run_id.to_string();

        Box::pin(async move {
            let status_opt: Option<String> = conn
                .call(move |conn| {
                    let mut stmt =
                        conn.prepare("SELECT status FROM workflow_runs WHERE run_id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![run_id_str])?;
                    if let Some(row) = rows.next()? {
                        Ok(Some(row.get::<_, String>(0)?))
                    } else {
                        Ok(None)
                    }
                })
                .await?;

            Ok(match status_opt.as_deref() {
                Some("running") => RunStatus::Running,
                Some("completed") => RunStatus::Completed,
                Some("failed") => RunStatus::Failed,
                Some("cancelled") => RunStatus::Cancelled,
                _ => RunStatus::NotFound,
            })
        })
    }

    fn get_run_result(&self, run_id: Uuid) -> StorageFuture<Option<Value>> {
        let conn = self.conn.clone();
        let run_id_str = run_id.to_string();

        Box::pin(async move {
            let result_opt: Option<String> = conn
                .call(move |conn| {
                    let mut stmt =
                        conn.prepare("SELECT result FROM workflow_runs WHERE run_id = ?1")?;
                    let mut rows = stmt.query(rusqlite::params![run_id_str])?;
                    if let Some(row) = rows.next()? {
                        Ok(row.get::<_, Option<String>>(0)?)
                    } else {
                        Ok(None)
                    }
                })
                .await?;

            match result_opt {
                Some(json_str) => {
                    let value: Value = serde_json::from_str(&json_str)?;
                    Ok(Some(value))
                }
                None => Ok(None),
            }
        })
    }

    fn list_runs(&self, filter: &RunFilter) -> StorageFuture<Vec<RunInfo>> {
        let conn = self.conn.clone();

        // Build filter parameters as owned values.
        let status_str = filter.status.as_ref().map(|s| match s {
            RunStatus::Running => "running".to_string(),
            RunStatus::Completed => "completed".to_string(),
            RunStatus::Failed => "failed".to_string(),
            RunStatus::Cancelled => "cancelled".to_string(),
            RunStatus::NotFound => "not_found".to_string(),
        });
        let workflow_name = filter.workflow_name.clone();
        let created_after_ms = filter.created_after.map(|d| d.timestamp_millis());
        let created_before_ms = filter.created_before.map(|d| d.timestamp_millis());
        let limit = filter.limit;
        let offset = filter.offset;

        Box::pin(async move {
            let rows: Vec<(String, String, String, i64, i64)> = conn
                .call(move |conn| {
                    let mut sql =
                        "SELECT run_id, name, status, created_at, updated_at FROM workflow_runs WHERE 1=1"
                            .to_string();
                    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

                    if let Some(ref s) = status_str {
                        params.push(Box::new(s.clone()));
                        sql.push_str(&format!(" AND status = ?{}", params.len()));
                    }
                    if let Some(ref name) = workflow_name {
                        params.push(Box::new(name.clone()));
                        sql.push_str(&format!(" AND name = ?{}", params.len()));
                    }
                    if let Some(after) = created_after_ms {
                        params.push(Box::new(after));
                        sql.push_str(&format!(" AND created_at >= ?{}", params.len()));
                    }
                    if let Some(before) = created_before_ms {
                        params.push(Box::new(before));
                        sql.push_str(&format!(" AND created_at <= ?{}", params.len()));
                    }

                    sql.push_str(" ORDER BY created_at DESC");

                    if let Some(limit) = limit {
                        params.push(Box::new(limit as i64));
                        sql.push_str(&format!(" LIMIT ?{}", params.len()));
                    }
                    if let Some(offset) = offset {
                        params.push(Box::new(offset as i64));
                        sql.push_str(&format!(" OFFSET ?{}", params.len()));
                    }

                    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                        params.iter().map(|p| &**p).collect();

                    let mut stmt = conn.prepare(&sql)?;
                    let rows = stmt
                        .query_map(param_refs.as_slice(), |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, i64>(3)?,
                                row.get::<_, i64>(4)?,
                            ))
                        })?
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    Ok(rows)
                })
                .await?;

            let mut result = Vec::with_capacity(rows.len());
            for (run_id_str, name, status_str, created_at_ms, updated_at_ms) in rows {
                let run_id =
                    Uuid::parse_str(&run_id_str).map_err(|e| GearsError::Other(e.to_string()))?;
                let status = match status_str.as_str() {
                    "running" => RunStatus::Running,
                    "completed" => RunStatus::Completed,
                    "failed" => RunStatus::Failed,
                    "cancelled" => RunStatus::Cancelled,
                    _ => RunStatus::NotFound,
                };
                let created_at = Utc
                    .timestamp_millis_opt(created_at_ms)
                    .single()
                    .unwrap_or_else(Utc::now);
                let updated_at = Utc
                    .timestamp_millis_opt(updated_at_ms)
                    .single()
                    .unwrap_or_else(Utc::now);
                result.push(RunInfo {
                    run_id,
                    workflow_name: name,
                    status,
                    created_at,
                    updated_at,
                });
            }
            Ok(result)
        })
    }

    // ── Schedule persistence ──────────────────────────────────────────────

    fn upsert_schedule(&self, record: &ScheduleRecord) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let name = record.name.clone();
        let cron_expression = record.cron_expression.clone();
        let workflow_name = record.workflow_name.clone();
        let input_json = match serde_json::to_string(&record.input) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(GearsError::Serialize(e)) }),
        };
        let status_str = match record.status {
            ScheduleStatus::Active => "active",
            ScheduleStatus::Paused => "paused",
        }
        .to_string();
        let created_at = record.created_at.timestamp_millis();

        Box::pin(async move {
            conn.call(move |conn| {
                conn.execute(
                    "INSERT INTO workflow_schedules
                         (name, cron_expression, workflow_name, input, status, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(name) DO UPDATE SET
                         cron_expression = excluded.cron_expression,
                         workflow_name   = excluded.workflow_name,
                         input           = excluded.input,
                         status          = excluded.status",
                    rusqlite::params![
                        name,
                        cron_expression,
                        workflow_name,
                        input_json,
                        status_str,
                        created_at
                    ],
                )?;
                Ok(())
            })
            .await?;
            Ok(())
        })
    }

    fn get_schedule(&self, name: &str) -> StorageFuture<Option<ScheduleRecord>> {
        let conn = self.conn.clone();
        let name = name.to_string();

        Box::pin(async move {
            let row_opt: Option<ScheduleRow> = conn
                .call(move |conn| {
                    let mut stmt = conn.prepare(
                        "SELECT name, cron_expression, workflow_name, input, status,
                                created_at, last_fired_at
                         FROM workflow_schedules WHERE name = ?1",
                    )?;
                    let mut rows = stmt.query(rusqlite::params![name])?;
                    if let Some(row) = rows.next()? {
                        Ok(Some((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, Option<i64>>(6)?,
                        )))
                    } else {
                        Ok(None)
                    }
                })
                .await?;

            match row_opt {
                None => Ok(None),
                Some((name, cron_expr, wf_name, input_json, status_str, created_ms, fired_ms)) => {
                    Ok(Some(parse_schedule_row(
                        name, cron_expr, wf_name, input_json, status_str, created_ms, fired_ms,
                    )?))
                }
            }
        })
    }

    fn list_schedules(&self) -> StorageFuture<Vec<ScheduleRecord>> {
        let conn = self.conn.clone();

        Box::pin(async move {
            let rows: Vec<ScheduleRow> = conn
                .call(|conn| {
                    let mut stmt = conn.prepare(
                        "SELECT name, cron_expression, workflow_name, input, status,
                                created_at, last_fired_at
                         FROM workflow_schedules
                         ORDER BY created_at DESC",
                    )?;
                    let rows = stmt
                        .query_map([], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, String>(4)?,
                                row.get::<_, i64>(5)?,
                                row.get::<_, Option<i64>>(6)?,
                            ))
                        })?
                        .collect::<std::result::Result<_, _>>()?;
                    Ok(rows)
                })
                .await?;

            let mut result = Vec::with_capacity(rows.len());
            for (name, cron_expr, wf_name, input_json, status_str, created_ms, fired_ms) in rows {
                result.push(parse_schedule_row(
                    name, cron_expr, wf_name, input_json, status_str, created_ms, fired_ms,
                )?);
            }
            Ok(result)
        })
    }

    fn delete_schedule(&self, name: &str) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let name = name.to_string();

        Box::pin(async move {
            conn.call(move |conn| {
                conn.execute(
                    "DELETE FROM workflow_schedules WHERE name = ?1",
                    rusqlite::params![name],
                )?;
                Ok(())
            })
            .await?;
            Ok(())
        })
    }

    fn set_schedule_status(&self, name: &str, status: ScheduleStatus) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let name = name.to_string();
        let status_str = match status {
            ScheduleStatus::Active => "active",
            ScheduleStatus::Paused => "paused",
        }
        .to_string();

        Box::pin(async move {
            conn.call(move |conn| {
                conn.execute(
                    "UPDATE workflow_schedules SET status = ?1 WHERE name = ?2",
                    rusqlite::params![status_str, name],
                )?;
                Ok(())
            })
            .await?;
            Ok(())
        })
    }

    fn record_schedule_fired(&self, name: &str, fired_at: DateTime<Utc>) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let name = name.to_string();
        let fired_at_ms = fired_at.timestamp_millis();

        Box::pin(async move {
            conn.call(move |conn| {
                conn.execute(
                    "UPDATE workflow_schedules SET last_fired_at = ?1 WHERE name = ?2",
                    rusqlite::params![fired_at_ms, name],
                )?;
                Ok(())
            })
            .await?;
            Ok(())
        })
    }

    fn delete_run(&self, run_id: Uuid) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let id = run_id.to_string();

        Box::pin(async move {
            conn.call(move |conn| {
                let tx = conn.transaction()?;
                tx.execute(
                    "DELETE FROM workflow_events WHERE run_id = ?1",
                    rusqlite::params![id],
                )?;
                tx.execute(
                    "DELETE FROM workflow_runs WHERE run_id = ?1",
                    rusqlite::params![id],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await?;
            Ok(())
        })
    }

    // ── External worker task queue ────────────────────────────────────────

    fn create_pending_task(&self, task: PendingTask) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let token = task.task_token.to_string();
        let run_id = task.run_id.to_string();
        let input_json = match serde_json::to_string(&task.input) {
            Ok(s) => s,
            Err(e) => return Box::pin(async move { Err(GearsError::Serialize(e)) }),
        };
        let created_at = task.created_at.timestamp_millis();
        let updated_at = task.updated_at.timestamp_millis();
        let heartbeat_at = task.heartbeat_at.map(|t| t.timestamp_millis());
        let seq = task.sequence_id;
        let attempt = task.attempt;
        let name = task.activity_name.clone();
        let sts_timeout = task.schedule_to_start_timeout_ms.map(|v| v as i64);

        Box::pin(async move {
            conn.call(move |conn| {
                conn.execute(
                    "INSERT INTO pending_tasks
                        (task_token, run_id, sequence_id, activity_name, input,
                         attempt, status, worker_id, heartbeat_at,
                         schedule_to_start_timeout_ms, output, error_message,
                         created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', NULL, ?7, ?8, NULL, NULL, ?9, ?10)",
                    rusqlite::params![
                        token, run_id, seq, name, input_json,
                        attempt, heartbeat_at, sts_timeout, created_at, updated_at
                    ],
                )?;
                Ok(())
            })
            .await?;
            Ok(())
        })
    }

    fn claim_pending_task(
        &self,
        activity_names: &[String],
        worker_id: &str,
    ) -> StorageFuture<Option<PendingTask>> {
        let conn = self.conn.clone();
        let names: Vec<String> = activity_names.to_vec();
        let worker = worker_id.to_string();
        let now = Utc::now().timestamp_millis();

        Box::pin(async move {
            let row_opt = conn
                .call(move |conn| {
                    if names.is_empty() {
                        return Ok(None);
                    }
                    // Build IN clause: (?1, ?2, ...)
                    let placeholders: Vec<String> = (1..=names.len())
                        .map(|i| format!("?{i}"))
                        .collect();
                    let in_clause = placeholders.join(", ");

                    // Use a transaction to atomically SELECT + UPDATE.
                    let tx = conn.transaction_with_behavior(
                        rusqlite::TransactionBehavior::Immediate,
                    )?;

                    let token_opt: Option<String> = {
                        let params_vec: Vec<Box<dyn rusqlite::ToSql>> =
                            names.iter().map(|n| -> Box<dyn rusqlite::ToSql> {
                                Box::new(n.clone())
                            }).collect();
                        let sql = format!(
                            "SELECT task_token FROM pending_tasks
                             WHERE status = 'pending' AND activity_name IN ({in_clause})
                             ORDER BY created_at ASC LIMIT 1"
                        );
                        let mut stmt = tx.prepare(&sql)?;
                        let params_refs: Vec<&dyn rusqlite::ToSql> =
                            params_vec.iter().map(|b| b.as_ref()).collect();
                        let mut rows = stmt.query(params_refs.as_slice())?;
                        if let Some(row) = rows.next()? {
                            Some(row.get::<_, String>(0)?)
                        } else {
                            None
                        }
                    };

                    let Some(token) = token_opt else {
                        tx.commit()?;
                        return Ok(None);
                    };

                    tx.execute(
                        "UPDATE pending_tasks
                         SET status = 'claimed', worker_id = ?1,
                             heartbeat_at = ?2, updated_at = ?2
                         WHERE task_token = ?3",
                        rusqlite::params![worker, now, token],
                    )?;
                    tx.commit()?;
                    Ok(Some(token))
                })
                .await?;

            let Some(token_str) = row_opt else {
                return Ok(None);
            };
            let token = Uuid::parse_str(&token_str)
                .map_err(|e| GearsError::Other(e.to_string()))?;
            // Fetch the full row now that it is claimed.
            fetch_pending_task(&conn, token).await
        })
    }

    fn resolve_pending_task(&self, task_token: Uuid, result: TaskResult) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let token = task_token.to_string();
        let (new_status, output_json, error_msg) = match &result {
            TaskResult::Success { output } => {
                let json = match serde_json::to_string(output) {
                    Ok(s) => s,
                    Err(e) => return Box::pin(async move { Err(GearsError::Serialize(e)) }),
                };
                ("completed".to_string(), Some(json), None)
            }
            TaskResult::Failure { error } => {
                ("failed".to_string(), None, Some(error.clone()))
            }
        };
        let now = Utc::now().timestamp_millis();

        Box::pin(async move {
            let rows_changed = conn
                .call(move |conn| {
                    // Only transition from pending/claimed → completed/failed.
                    let changed = conn.execute(
                        "UPDATE pending_tasks
                         SET status = ?1, output = ?2, error_message = ?3, updated_at = ?4
                         WHERE task_token = ?5
                           AND status NOT IN ('completed', 'failed')",
                        rusqlite::params![new_status, output_json, error_msg, now, token],
                    )?;
                    Ok(changed)
                })
                .await?;

            if rows_changed == 0 {
                let exists = conn
                    .call(move |conn| {
                        let count: i64 = conn.query_row(
                            "SELECT COUNT(*) FROM pending_tasks WHERE task_token = ?1",
                            rusqlite::params![task_token.to_string()],
                            |row| row.get(0),
                        )?;
                        Ok(count)
                    })
                    .await?;
                if exists > 0 {
                    return Err(GearsError::TaskAlreadyResolved(task_token));
                } else {
                    return Err(GearsError::TaskNotFound(task_token));
                }
            }
            Ok(())
        })
    }

    fn heartbeat_pending_task(&self, task_token: Uuid) -> StorageFuture<bool> {
        let conn = self.conn.clone();
        let token = task_token.to_string();
        let now = Utc::now().timestamp_millis();

        Box::pin(async move {
            let changed = conn
                .call(move |conn| {
                    let n = conn.execute(
                        "UPDATE pending_tasks
                         SET heartbeat_at = ?1, updated_at = ?1
                         WHERE task_token = ?2 AND status = 'claimed'",
                        rusqlite::params![now, token],
                    )?;
                    Ok(n)
                })
                .await?;
            Ok(changed > 0)
        })
    }

    fn get_pending_task(&self, task_token: Uuid) -> StorageFuture<Option<PendingTask>> {
        let conn = self.conn.clone();
        Box::pin(async move { fetch_pending_task(&conn, task_token).await })
    }

    fn list_pending_tasks_by_run(&self, run_id: Uuid) -> StorageFuture<Vec<PendingTask>> {
        let conn = self.conn.clone();
        let run_id_str = run_id.to_string();

        Box::pin(async move {
            let rows = conn
                .call(move |conn| {
                    let mut stmt = conn.prepare(
                        "SELECT task_token, run_id, sequence_id, activity_name, input,
                                attempt, status, worker_id, heartbeat_at,
                                schedule_to_start_timeout_ms, output, error_message,
                                created_at, updated_at
                         FROM pending_tasks
                         WHERE run_id = ?1 AND status NOT IN ('completed', 'failed')
                         ORDER BY created_at ASC",
                    )?;
                    let rows = stmt
                        .query_map(rusqlite::params![run_id_str], parse_pending_task_row)?
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    Ok(rows)
                })
                .await?;

            let mut tasks = Vec::with_capacity(rows.len());
            for raw in rows {
                tasks.push(build_pending_task(raw)?);
            }
            Ok(tasks)
        })
    }

    fn list_stale_pending_tasks(&self, older_than_ms: u64) -> StorageFuture<Vec<PendingTask>> {
        let conn = self.conn.clone();
        let cutoff = Utc::now().timestamp_millis() - older_than_ms as i64;

        Box::pin(async move {
            let rows = conn
                .call(move |conn| {
                    let mut stmt = conn.prepare(
                        "SELECT task_token, run_id, sequence_id, activity_name, input,
                                attempt, status, worker_id, heartbeat_at,
                                schedule_to_start_timeout_ms, output, error_message,
                                created_at, updated_at
                         FROM pending_tasks
                         WHERE status = 'claimed'
                           AND heartbeat_at IS NOT NULL
                           AND heartbeat_at < ?1
                         ORDER BY created_at ASC",
                    )?;
                    let rows = stmt
                        .query_map(rusqlite::params![cutoff], parse_pending_task_row)?
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    Ok(rows)
                })
                .await?;

            let mut tasks = Vec::with_capacity(rows.len());
            for raw in rows {
                tasks.push(build_pending_task(raw)?);
            }
            Ok(tasks)
        })
    }

    fn reset_pending_task(&self, task_token: Uuid) -> StorageFuture<()> {
        let conn = self.conn.clone();
        let token = task_token.to_string();
        let now = Utc::now().timestamp_millis();

        Box::pin(async move {
            conn.call(move |conn| {
                conn.execute(
                    "UPDATE pending_tasks
                     SET status = 'pending', worker_id = NULL, heartbeat_at = NULL,
                         updated_at = ?1
                     WHERE task_token = ?2 AND status = 'claimed'",
                    rusqlite::params![now, token],
                )?;
                Ok(())
            })
            .await?;
            Ok(())
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

// ── Pending task helpers ──────────────────────────────────────────────────

type PendingTaskRow = (
    String,         // task_token
    String,         // run_id
    u32,            // sequence_id
    String,         // activity_name
    String,         // input JSON
    u32,            // attempt
    String,         // status
    Option<String>, // worker_id
    Option<i64>,    // heartbeat_at
    Option<i64>,    // schedule_to_start_timeout_ms
    Option<String>, // output JSON
    Option<String>, // error_message
    i64,            // created_at
    i64,            // updated_at
);

fn parse_pending_task_row(
    row: &rusqlite::Row<'_>,
) -> std::result::Result<PendingTaskRow, rusqlite::Error> {
    Ok((
        row.get::<_, String>(0)?,
        row.get::<_, String>(1)?,
        row.get::<_, u32>(2)?,
        row.get::<_, String>(3)?,
        row.get::<_, String>(4)?,
        row.get::<_, u32>(5)?,
        row.get::<_, String>(6)?,
        row.get::<_, Option<String>>(7)?,
        row.get::<_, Option<i64>>(8)?,
        row.get::<_, Option<i64>>(9)?,
        row.get::<_, Option<String>>(10)?,
        row.get::<_, Option<String>>(11)?,
        row.get::<_, i64>(12)?,
        row.get::<_, i64>(13)?,
    ))
}

fn build_pending_task(raw: PendingTaskRow) -> crate::error::Result<PendingTask> {
    let (
        token_str, run_id_str, sequence_id, activity_name, input_json,
        attempt, status, worker_id, heartbeat_at_ms, sts_timeout_ms,
        output_json, error_message,
        created_at_ms, updated_at_ms,
    ) = raw;

    let task_token = Uuid::parse_str(&token_str).map_err(|e| GearsError::Other(e.to_string()))?;
    let run_id = Uuid::parse_str(&run_id_str).map_err(|e| GearsError::Other(e.to_string()))?;
    let input: serde_json::Value = serde_json::from_str(&input_json)?;
    let output = output_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?;
    let heartbeat_at = heartbeat_at_ms.map(|ms| {
        Utc.timestamp_millis_opt(ms).single().unwrap_or_else(Utc::now)
    });
    let created_at = Utc.timestamp_millis_opt(created_at_ms).single().unwrap_or_else(Utc::now);
    let updated_at = Utc.timestamp_millis_opt(updated_at_ms).single().unwrap_or_else(Utc::now);

    Ok(PendingTask {
        task_token,
        run_id,
        sequence_id,
        activity_name,
        input,
        attempt,
        status,
        worker_id,
        heartbeat_at,
        schedule_to_start_timeout_ms: sts_timeout_ms.map(|v| v as u64),
        output,
        error_message,
        created_at,
        updated_at,
    })
}

async fn fetch_pending_task(
    conn: &tokio_rusqlite::Connection,
    task_token: Uuid,
) -> crate::error::Result<Option<PendingTask>> {
    let token_str = task_token.to_string();
    let row_opt = conn
        .call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT task_token, run_id, sequence_id, activity_name, input,
                        attempt, status, worker_id, heartbeat_at,
                        schedule_to_start_timeout_ms, output, error_message,
                        created_at, updated_at
                 FROM pending_tasks WHERE task_token = ?1",
            )?;
            let mut rows = stmt.query(rusqlite::params![token_str])?;
            if let Some(row) = rows.next()? {
                Ok(Some(parse_pending_task_row(row)?))
            } else {
                Ok(None)
            }
        })
        .await?;

    match row_opt {
        None => Ok(None),
        Some(raw) => Ok(Some(build_pending_task(raw)?)),
    }
}

/// Raw column tuple returned by schedule SELECT queries.
type ScheduleRow = (String, String, String, String, String, i64, Option<i64>);

fn parse_schedule_row(
    name: String,
    cron_expression: String,
    workflow_name: String,
    input_json: String,
    status_str: String,
    created_at_ms: i64,
    last_fired_ms: Option<i64>,
) -> crate::error::Result<ScheduleRecord> {
    use chrono::TimeZone;
    let input: Value = serde_json::from_str(&input_json)?;
    let status = match status_str.as_str() {
        "active" => ScheduleStatus::Active,
        _ => ScheduleStatus::Paused,
    };
    let created_at = Utc
        .timestamp_millis_opt(created_at_ms)
        .single()
        .unwrap_or_else(Utc::now);
    let last_fired_at = last_fired_ms.map(|ms| {
        Utc.timestamp_millis_opt(ms)
            .single()
            .unwrap_or_else(Utc::now)
    });
    Ok(ScheduleRecord {
        name,
        cron_expression,
        workflow_name,
        input,
        status,
        created_at,
        last_fired_at,
    })
}

impl SqliteStorage {
    /// Overwrite `created_at` / `updated_at` to simulate aged runs.
    /// Intended for testing only; not part of the stable API.
    #[doc(hidden)]
    pub async fn backdate_run(&self, run_id: Uuid, days_ago: i64) {
        let conn = self.conn.clone();
        let id = run_id.to_string();
        let ts = (Utc::now() - chrono::Duration::days(days_ago)).timestamp_millis();
        conn.call(move |c| -> rusqlite::Result<()> {
            c.execute(
                "UPDATE workflow_runs SET created_at = ?1, updated_at = ?1 WHERE run_id = ?2",
                rusqlite::params![ts, id],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventPayload;
    use serde_json::json;

    async fn mem_storage() -> SqliteStorage {
        SqliteStorage::open(":memory:").await.unwrap()
    }

    #[tokio::test]
    async fn test_create_and_load_events() {
        let storage = mem_storage().await;
        let run_id = Uuid::new_v4();
        let input = json!({"name": "test"});

        storage.create_run(run_id, "my_wf", &input).await.unwrap();

        let event = WorkflowEvent {
            sequence: 0,
            occurred_at: Utc::now(),
            payload: EventPayload::WorkflowStarted {
                workflow_name: "my_wf".into(),
                input: input.clone(),
            },
        };
        storage.append_event(run_id, &event).await.unwrap();

        let events = storage.load_events(run_id).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 0);
    }

    #[tokio::test]
    async fn test_list_running_and_complete() {
        let storage = mem_storage().await;
        let run_id = Uuid::new_v4();
        storage.create_run(run_id, "wf", &json!({})).await.unwrap();

        let running = storage.list_running_workflows().await.unwrap();
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].run_id, run_id);

        storage
            .set_run_status(run_id, RunStatus::Completed, Some(json!({})))
            .await
            .unwrap();

        let running = storage.list_running_workflows().await.unwrap();
        assert_eq!(running.len(), 0);
    }

    #[tokio::test]
    async fn test_get_run_status() {
        let storage = mem_storage().await;
        let run_id = Uuid::new_v4();
        storage.create_run(run_id, "wf", &json!({})).await.unwrap();

        assert_eq!(
            storage.get_run_status(run_id).await.unwrap(),
            RunStatus::Running
        );

        storage
            .set_run_status(run_id, RunStatus::Completed, None)
            .await
            .unwrap();
        assert_eq!(
            storage.get_run_status(run_id).await.unwrap(),
            RunStatus::Completed
        );
    }
}
