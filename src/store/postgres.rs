//! PostgreSQL-backed checkpointer using `sqlx-core` and `sqlx-postgres`.
//!
//! Feature-gated behind the `postgres` feature flag.

use async_trait::async_trait;
use sqlx_core::pool::Pool;
use sqlx_core::query::query;
use sqlx_core::query_as::query_as;
use sqlx_postgres::Postgres;

/// Type alias for the PostgreSQL connection pool.
pub type PgPool = Pool<Postgres>;

use crate::checkpoint::Checkpointer;
use crate::checkpoint_meta::{CheckpointMeta, CheckpointStatus, RetentionPolicy};
use crate::dag::DAG;
use crate::error::TakelnError;
use crate::graph::State;

/// PostgreSQL-backed checkpointer for durable persistence across restarts.
///
/// Requires the `postgres` feature flag and a running PostgreSQL instance.
/// The table `takeln_checkpoints` is automatically created on construction.
///
/// # Example
/// ```rust,no_run
/// use takeln::PostgresCheckpointer;
/// use takeln::PgPool;
///
/// #[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
/// struct MyState { value: String }
///
/// # async fn example() {
/// let pool = PgPool::connect("postgres://localhost/takeln").await.unwrap();
/// let cp = PostgresCheckpointer::<MyState>::new(pool).await.unwrap();
/// # }
/// ```
pub struct PostgresCheckpointer<S: State> {
    pool: PgPool,
    _marker: std::marker::PhantomData<S>,
}

impl<S: State> PostgresCheckpointer<S> {
    /// Create a new Postgres checkpointer, auto-creating the table if it doesn't exist.
    pub async fn new(pool: PgPool) -> Result<Self, TakelnError> {
        query(
            r#"
            CREATE TABLE IF NOT EXISTS takeln_checkpoints (
                id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                state JSONB NOT NULL,
                next_node TEXT NOT NULL,
                dag JSONB,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to create table: {}", e)))?;

        query("CREATE INDEX IF NOT EXISTS idx_takeln_thread_created ON takeln_checkpoints(thread_id, created_at)")
            .execute(&pool)
            .await
            .map_err(|e| TakelnError::CheckpointError(format!("Failed to create index: {}", e)))?;

        Ok(Self {
            pool,
            _marker: std::marker::PhantomData,
        })
    }
}

#[async_trait]
impl<S: State> Checkpointer<S> for PostgresCheckpointer<S> {
    async fn save_state(
        &self,
        thread_id: String,
        state: S,
        next_node: String,
        dag: Option<&DAG>,
        status: CheckpointStatus,
    ) -> Result<String, TakelnError> {
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let state_json = serde_json::to_value(&state).map_err(|e| TakelnError::SerializationError(e.to_string()))?;
        let dag_json = dag
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| TakelnError::SerializationError(e.to_string()))?;
        let status_str = format!("{:?}", status);

        query(
            "INSERT INTO takeln_checkpoints (id, thread_id, state, next_node, dag, status) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&checkpoint_id)
        .bind(&thread_id)
        .bind(&state_json)
        .bind(&next_node)
        .bind(&dag_json)
        .bind(&status_str)
        .execute(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to save: {}", e)))?;

        Ok(checkpoint_id)
    }

    async fn load_state(&self, thread_id: String) -> Result<Option<(S, CheckpointMeta, Option<DAG>)>, TakelnError> {
        let row: Option<(String, serde_json::Value, String, Option<serde_json::Value>, String, chrono::DateTime<chrono::Utc>)> = query_as(
            "SELECT id, state, next_node, dag, status, created_at FROM takeln_checkpoints WHERE thread_id = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(&thread_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to load: {}", e)))?;

        match row {
            Some((id, state_val, next_node, dag_val, status_str, created_at)) => {
                let state: S =
                    serde_json::from_value(state_val).map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let dag: Option<DAG> = dag_val
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let status = parse_checkpoint_status(&status_str);
                let meta = CheckpointMeta {
                    checkpoint_id: id,
                    thread_id,
                    next_node,
                    graph_version: None,
                    state_schema_version: None,
                    status,
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
        let row: Option<(String, serde_json::Value, String, Option<serde_json::Value>, String, chrono::DateTime<chrono::Utc>)> = query_as(
            "SELECT id, state, next_node, dag, status, created_at FROM takeln_checkpoints WHERE thread_id = $1 AND id = $2",
        )
        .bind(&thread_id)
        .bind(&checkpoint_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to load version: {}", e)))?;

        match row {
            Some((id, state_val, next_node, dag_val, status_str, created_at)) => {
                let state: S =
                    serde_json::from_value(state_val).map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let dag: Option<DAG> = dag_val
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let status = parse_checkpoint_status(&status_str);
                let meta = CheckpointMeta {
                    checkpoint_id: id,
                    thread_id,
                    next_node,
                    graph_version: None,
                    state_schema_version: None,
                    status,
                    created_at,
                };
                Ok(Some((state, meta, dag)))
            }
            None => Ok(None),
        }
    }

    async fn list_checkpoints(&self, thread_id: String) -> Result<Vec<CheckpointMeta>, TakelnError> {
        let rows: Vec<(String, String, String, chrono::DateTime<chrono::Utc>)> = query_as(
            "SELECT id, next_node, status, created_at FROM takeln_checkpoints WHERE thread_id = $1 ORDER BY created_at ASC",
        )
        .bind(&thread_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to list: {}", e)))?;

        Ok(rows
            .into_iter()
            .map(|(id, next_node, status_str, created_at)| CheckpointMeta {
                checkpoint_id: id,
                thread_id: thread_id.clone(),
                next_node,
                graph_version: None,
                state_schema_version: None,
                status: parse_checkpoint_status(&status_str),
                created_at,
            })
            .collect())
    }

    async fn delete_checkpoints(&self, thread_id: String, policy: RetentionPolicy) -> Result<usize, TakelnError> {
        match policy {
            RetentionPolicy::KeepAll => Ok(0),
            RetentionPolicy::KeepLast(n) => {
                let result = query(
                    "DELETE FROM takeln_checkpoints WHERE thread_id = $1 AND id NOT IN (SELECT id FROM takeln_checkpoints WHERE thread_id = $1 ORDER BY created_at DESC LIMIT $2)",
                )
                .bind(&thread_id)
                .bind(n as i64)
                .execute(&self.pool)
                .await
                .map_err(|e| TakelnError::CheckpointError(format!("Failed to delete: {}", e)))?;
                Ok(result.rows_affected() as usize)
            }
            RetentionPolicy::OlderThan(duration) => {
                let cutoff = chrono::Utc::now()
                    - chrono::Duration::from_std(duration).map_err(|e| TakelnError::CheckpointError(e.to_string()))?;
                let result = query("DELETE FROM takeln_checkpoints WHERE thread_id = $1 AND created_at < $2")
                    .bind(&thread_id)
                    .bind(cutoff)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| TakelnError::CheckpointError(format!("Failed to delete: {}", e)))?;
                Ok(result.rows_affected() as usize)
            }
        }
    }
}

fn parse_checkpoint_status(s: &str) -> CheckpointStatus {
    match s {
        "Complete" => CheckpointStatus::Complete,
        "Running" => CheckpointStatus::Running,
        "Yielded" => CheckpointStatus::Yielded,
        "Failed" => CheckpointStatus::Failed,
        "Interrupted" => CheckpointStatus::Interrupted,
        _ => CheckpointStatus::Complete,
    }
}
