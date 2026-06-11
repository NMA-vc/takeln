//! SQLite-backed checkpointer using `rusqlite`.
//!
//! Feature-gated behind the `sqlite` feature flag.

use async_trait::async_trait;
use std::sync::Arc;

use crate::checkpoint::Checkpointer;
use crate::checkpoint_meta::{CheckpointMeta, CheckpointStatus, RetentionPolicy};
use crate::dag::DAG;
use crate::error::TakelnError;
use crate::graph::State;

/// SQLite-backed checkpointer for durable local persistence.
///
/// Requires the `sqlite` feature flag. Uses `rusqlite` with synchronous
/// operations offloaded to `spawn_blocking` for async compatibility.
///
/// The database file persists across process restarts, making this ideal for
/// single-process deployments that need durability without a database server.
pub struct SqliteCheckpointer<S: State> {
    conn: Arc<std::sync::Mutex<rusqlite::Connection>>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: State> SqliteCheckpointer<S> {
    /// Create a new SQLite checkpointer at the given path.
    ///
    /// Creates the database file and table if they don't exist.
    pub fn new(path: &str) -> Result<Self, TakelnError> {
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| TakelnError::CheckpointError(format!("Failed to open SQLite: {}", e)))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS takeln_checkpoints (
                id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                state TEXT NOT NULL,
                next_node TEXT NOT NULL,
                dag TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_takeln_thread_created ON takeln_checkpoints(thread_id, created_at);",
        )
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to create table: {}", e)))?;

        Ok(Self {
            conn: Arc::new(std::sync::Mutex::new(conn)),
            _marker: std::marker::PhantomData,
        })
    }

    /// Create a new in-memory SQLite checkpointer (useful for testing).
    pub fn in_memory() -> Result<Self, TakelnError> {
        Self::new(":memory:")
    }
}

#[async_trait]
impl<S: State> Checkpointer<S> for SqliteCheckpointer<S> {
    async fn save_state(
        &self,
        thread_id: String,
        state: S,
        next_node: String,
        dag: Option<&DAG>,
        status: CheckpointStatus,
    ) -> Result<String, TakelnError> {
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let state_json = serde_json::to_string(&state).map_err(|e| TakelnError::SerializationError(e.to_string()))?;
        let dag_json = dag
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| TakelnError::SerializationError(e.to_string()))?;
        let status_str = format!("{:?}", status);
        let created_at = chrono::Utc::now().to_rfc3339();

        let conn = self.conn.clone();
        let cp_id = checkpoint_id.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;

            conn.execute(
                "INSERT INTO takeln_checkpoints (id, thread_id, state, next_node, dag, status, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![cp_id, thread_id, state_json, next_node, dag_json, status_str, created_at],
            )
            .map_err(|e| TakelnError::CheckpointError(format!("Failed to save: {}", e)))?;

            Ok(cp_id)
        })
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("spawn_blocking join error: {}", e)))?
    }

    async fn load_state(&self, thread_id: String) -> Result<Option<(S, CheckpointMeta, Option<DAG>)>, TakelnError> {
        let conn = self.conn.clone();

        let row_opt = tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;

            let mut stmt = conn.prepare(
                "SELECT id, state, next_node, dag, status, created_at FROM takeln_checkpoints WHERE thread_id = ?1 ORDER BY created_at DESC LIMIT 1",
            ).map_err(|e| TakelnError::CheckpointError(format!("Failed to prepare: {}", e)))?;

            let result = stmt.query_row(rusqlite::params![thread_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    thread_id.clone(),
                ))
            });

            match result {
                Ok(row) => Ok(Some(row)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(TakelnError::CheckpointError(format!("Failed to load: {}", e))),
            }
        })
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("spawn_blocking join error: {}", e)))??;

        match row_opt {
            Some((id, state_str, next_node, dag_str, status_str, created_at_str, thread_id)) => {
                let state: S =
                    serde_json::from_str(&state_str).map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let dag: Option<DAG> = dag_str
                    .map(|s| serde_json::from_str(&s))
                    .transpose()
                    .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| chrono::Utc::now());
                let meta = CheckpointMeta {
                    checkpoint_id: id,
                    thread_id,
                    next_node,
                    graph_version: None,
                    state_schema_version: None,
                    status: parse_status(&status_str),
                    created_at,
                };
                Ok(Some((state, meta, dag)))
            }
            None => Ok(None),
        }
    }

    async fn load_version(
        &self,
        thread_id: String,
        checkpoint_id: String,
    ) -> Result<Option<(S, CheckpointMeta, Option<DAG>)>, TakelnError> {
        let conn = self.conn.clone();

        let row_opt = tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;

            let mut stmt = conn.prepare(
                "SELECT id, state, next_node, dag, status, created_at FROM takeln_checkpoints WHERE thread_id = ?1 AND id = ?2",
            ).map_err(|e| TakelnError::CheckpointError(format!("Failed to prepare: {}", e)))?;

            let result = stmt.query_row(rusqlite::params![thread_id, checkpoint_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    thread_id.clone(),
                ))
            });

            match result {
                Ok(row) => Ok(Some(row)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(TakelnError::CheckpointError(format!("Failed to load version: {}", e))),
            }
        })
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("spawn_blocking join error: {}", e)))??;

        match row_opt {
            Some((id, state_str, next_node, dag_str, status_str, created_at_str, thread_id)) => {
                let state: S =
                    serde_json::from_str(&state_str).map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let dag: Option<DAG> = dag_str
                    .map(|s| serde_json::from_str(&s))
                    .transpose()
                    .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| chrono::Utc::now());
                let meta = CheckpointMeta {
                    checkpoint_id: id,
                    thread_id,
                    next_node,
                    graph_version: None,
                    state_schema_version: None,
                    status: parse_status(&status_str),
                    created_at,
                };
                Ok(Some((state, meta, dag)))
            }
            None => Ok(None),
        }
    }

    async fn list_checkpoints(&self, thread_id: String) -> Result<Vec<CheckpointMeta>, TakelnError> {
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;

            let mut stmt = conn.prepare(
                "SELECT id, next_node, status, created_at FROM takeln_checkpoints WHERE thread_id = ?1 ORDER BY created_at ASC",
            ).map_err(|e| TakelnError::CheckpointError(format!("Failed to prepare: {}", e)))?;

            let rows = stmt
                .query_map(rusqlite::params![thread_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(|e| TakelnError::CheckpointError(format!("Failed to list: {}", e)))?;

            let mut result = Vec::new();
            for row in rows {
                let (id, next_node, status_str, created_at_str) =
                    row.map_err(|e| TakelnError::CheckpointError(format!("Row error: {}", e)))?;
                let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| chrono::Utc::now());
                result.push(CheckpointMeta {
                    checkpoint_id: id,
                    thread_id: thread_id.clone(),
                    next_node,
                    graph_version: None,
                    state_schema_version: None,
                    status: parse_status(&status_str),
                    created_at,
                });
            }
            Ok(result)
        })
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("spawn_blocking join error: {}", e)))?
    }

    async fn delete_checkpoints(&self, thread_id: String, policy: RetentionPolicy) -> Result<usize, TakelnError> {
        let conn = self.conn.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;

            match policy {
                RetentionPolicy::KeepAll => Ok(0),
                RetentionPolicy::KeepLast(n) => {
                    let deleted = conn.execute(
                        "DELETE FROM takeln_checkpoints WHERE thread_id = ?1 AND id NOT IN (SELECT id FROM takeln_checkpoints WHERE thread_id = ?1 ORDER BY created_at DESC LIMIT ?2)",
                        rusqlite::params![thread_id, n],
                    ).map_err(|e| TakelnError::CheckpointError(format!("Failed to delete: {}", e)))?;
                    Ok(deleted)
                }
                RetentionPolicy::OlderThan(duration) => {
                    let cutoff = chrono::Utc::now()
                        - chrono::Duration::from_std(duration).map_err(|e| TakelnError::CheckpointError(e.to_string()))?;
                    let cutoff_str = cutoff.to_rfc3339();
                    let deleted = conn
                        .execute(
                            "DELETE FROM takeln_checkpoints WHERE thread_id = ?1 AND created_at < ?2",
                            rusqlite::params![thread_id, cutoff_str],
                        )
                        .map_err(|e| TakelnError::CheckpointError(format!("Failed to delete: {}", e)))?;
                    Ok(deleted)
                }
            }
        })
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("spawn_blocking join error: {}", e)))?
    }
}

fn parse_status(s: &str) -> CheckpointStatus {
    match s {
        "Complete" => CheckpointStatus::Complete,
        "Running" => CheckpointStatus::Running,
        "Yielded" => CheckpointStatus::Yielded,
        "Failed" => CheckpointStatus::Failed,
        "Interrupted" => CheckpointStatus::Interrupted,
        _ => CheckpointStatus::Complete,
    }
}
