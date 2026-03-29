use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde_json::Value;
use tokio_rusqlite::Connection;
use uuid::Uuid;

use crate::error::{Result, ZdflowError};
use crate::event::{EventPayload, WorkflowEvent};
use crate::traits::{RunFilter, RunInfo, RunRecord, RunStatus, Storage, StorageFuture};

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
        let conn = Connection::open(path).await?;
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
            Err(e) => return Box::pin(async move { Err(ZdflowError::Serialize(e)) }),
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
            Err(e) => return Box::pin(async move { Err(ZdflowError::Serialize(e)) }),
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
                    Uuid::parse_str(&run_id_str).map_err(|e| ZdflowError::Other(e.to_string()))?;
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
            Err(e) => return Box::pin(async move { Err(ZdflowError::Serialize(e)) }),
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
                    Uuid::parse_str(&run_id_str).map_err(|e| ZdflowError::Other(e.to_string()))?;
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
