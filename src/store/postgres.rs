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

use crate::checkpoint::{Checkpointer, ClaimResult};
use crate::checkpoint_meta::{CheckpointMeta, CheckpointStatus, RetentionPolicy};
use crate::dag::DAG;
use crate::error::TakelnError;
use crate::graph::State;
use crate::hitl::YieldRequest;

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
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                yield_request JSONB,
                claimed_interrupt TEXT,
                resolved_interrupt TEXT
            )
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to create table: {}", e)))?;

        // Migrate older tables if needed
        let _ = query("ALTER TABLE takeln_checkpoints ADD COLUMN IF NOT EXISTS yield_request JSONB")
            .execute(&pool)
            .await;
        let _ = query("ALTER TABLE takeln_checkpoints ADD COLUMN IF NOT EXISTS claimed_interrupt TEXT")
            .execute(&pool)
            .await;
        let _ = query("ALTER TABLE takeln_checkpoints ADD COLUMN IF NOT EXISTS resolved_interrupt TEXT")
            .execute(&pool)
            .await;

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
        yield_request: Option<YieldRequest>,
        claimed_interrupt: Option<String>,
        resolved_interrupt: Option<String>,
    ) -> Result<String, TakelnError> {
        let checkpoint_id = uuid::Uuid::new_v4().to_string();
        let state_json = serde_json::to_value(&state).map_err(|e| TakelnError::SerializationError(e.to_string()))?;
        let dag_json = dag
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| TakelnError::SerializationError(e.to_string()))?;
        let yield_request_json = yield_request
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| TakelnError::SerializationError(e.to_string()))?;
        let status_str = format!("{:?}", status);

        query(
            "INSERT INTO takeln_checkpoints (id, thread_id, state, next_node, dag, status, yield_request, claimed_interrupt, resolved_interrupt) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(&checkpoint_id)
        .bind(&thread_id)
        .bind(&state_json)
        .bind(&next_node)
        .bind(&dag_json)
        .bind(&status_str)
        .bind(&yield_request_json)
        .bind(&claimed_interrupt)
        .bind(&resolved_interrupt)
        .execute(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to save: {}", e)))?;

        Ok(checkpoint_id)
    }

    async fn load_state(&self, thread_id: String) -> Result<Option<(S, CheckpointMeta, Option<DAG>)>, TakelnError> {
        let row: Option<(String, serde_json::Value, String, Option<serde_json::Value>, String, chrono::DateTime<chrono::Utc>, Option<serde_json::Value>, Option<String>, Option<String>)> = query_as(
            "SELECT id, state, next_node, dag, status, created_at, yield_request, claimed_interrupt, resolved_interrupt FROM takeln_checkpoints WHERE thread_id = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(&thread_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to load: {}", e)))?;

        match row {
            Some((
                id,
                state_val,
                next_node,
                dag_val,
                status_str,
                created_at,
                yield_request_val,
                claimed_interrupt,
                resolved_interrupt,
            )) => {
                let state: S =
                    serde_json::from_value(state_val).map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let dag: Option<DAG> = dag_val
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let status = parse_checkpoint_status(&status_str);
                let yield_request = yield_request_val
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let meta = CheckpointMeta {
                    checkpoint_id: id,
                    thread_id,
                    next_node,
                    graph_version: None,
                    state_schema_version: None,
                    status,
                    created_at,
                    yield_request,
                    claimed_interrupt,
                    resolved_interrupt,
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
        let row: Option<(String, serde_json::Value, String, Option<serde_json::Value>, String, chrono::DateTime<chrono::Utc>, Option<serde_json::Value>, Option<String>, Option<String>)> = query_as(
            "SELECT id, state, next_node, dag, status, created_at, yield_request, claimed_interrupt, resolved_interrupt FROM takeln_checkpoints WHERE thread_id = $1 AND id = $2",
        )
        .bind(&thread_id)
        .bind(&checkpoint_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to load version: {}", e)))?;

        match row {
            Some((
                id,
                state_val,
                next_node,
                dag_val,
                status_str,
                created_at,
                yield_request_val,
                claimed_interrupt,
                resolved_interrupt,
            )) => {
                let state: S =
                    serde_json::from_value(state_val).map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let dag: Option<DAG> = dag_val
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let status = parse_checkpoint_status(&status_str);
                let yield_request = yield_request_val
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let meta = CheckpointMeta {
                    checkpoint_id: id,
                    thread_id,
                    next_node,
                    graph_version: None,
                    state_schema_version: None,
                    status,
                    created_at,
                    yield_request,
                    claimed_interrupt,
                    resolved_interrupt,
                };
                Ok(Some((state, meta, dag)))
            }
            None => Ok(None),
        }
    }

    async fn list_checkpoints(&self, thread_id: String) -> Result<Vec<CheckpointMeta>, TakelnError> {
        let rows: Vec<(String, String, String, chrono::DateTime<chrono::Utc>, Option<serde_json::Value>, Option<String>, Option<String>)> = query_as(
            "SELECT id, next_node, status, created_at, yield_request, claimed_interrupt, resolved_interrupt FROM takeln_checkpoints WHERE thread_id = $1 ORDER BY created_at ASC",
        )
        .bind(&thread_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to list: {}", e)))?;

        let mut metas = Vec::new();
        for (id, next_node, status_str, created_at, yield_request_val, claimed_interrupt, resolved_interrupt) in rows {
            let yield_request = yield_request_val
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
            metas.push(CheckpointMeta {
                checkpoint_id: id,
                thread_id: thread_id.clone(),
                next_node,
                graph_version: None,
                state_schema_version: None,
                status: parse_checkpoint_status(&status_str),
                created_at,
                yield_request,
                claimed_interrupt,
                resolved_interrupt,
            });
        }
        Ok(metas)
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

    async fn claim_interrupt(&self, thread_id: &str, interrupt_id: &str) -> Result<ClaimResult, TakelnError> {
        // First, check if it was already resolved
        let exists: Option<(bool,)> = query_as(
            "SELECT EXISTS(SELECT 1 FROM takeln_checkpoints WHERE thread_id = $1 AND resolved_interrupt = $2)",
        )
        .bind(thread_id)
        .bind(interrupt_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to check interrupt resolution: {}", e)))?;

        if let Some((true,)) = exists {
            return Ok(ClaimResult::AlreadyCompleted);
        }

        // Get latest checkpoint status, yield_request, and claimed_interrupt
        let latest: Option<(String, Option<serde_json::Value>, Option<String>)> = query_as(
            "SELECT status, yield_request, claimed_interrupt FROM takeln_checkpoints WHERE thread_id = $1 ORDER BY created_at DESC LIMIT 1"
        )
        .bind(thread_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to read latest checkpoint: {}", e)))?;

        let (status_str, yield_req_val, claimed_interrupt) = match latest {
            Some(triplet) => triplet,
            None => return Err(TakelnError::NothingToResume(thread_id.to_string())),
        };

        let status = parse_checkpoint_status(&status_str);
        if status == CheckpointStatus::Running && claimed_interrupt.as_deref() == Some(interrupt_id) {
            return Ok(ClaimResult::InProgress);
        }

        if status != CheckpointStatus::Yielded {
            if status == CheckpointStatus::Running {
                return Err(TakelnError::ExecutionError(format!(
                    "Resume claim failed: thread '{}' is already running with claimed_interrupt '{}'",
                    thread_id,
                    claimed_interrupt.as_deref().unwrap_or("none")
                )));
            } else {
                return Err(TakelnError::NothingToResume(format!(
                    "Thread '{}' latest checkpoint status is '{}', expected Yielded",
                    thread_id, status_str
                )));
            }
        }

        let yield_req: YieldRequest = match yield_req_val {
            Some(v) => serde_json::from_value(v).map_err(|e| TakelnError::DeserializationError(e.to_string()))?,
            None => {
                return Err(TakelnError::NothingToResume(format!(
                    "Thread '{}' has Yielded status but no yield_request metadata",
                    thread_id
                )));
            }
        };

        if yield_req.interrupt_id != interrupt_id {
            return Err(TakelnError::InvalidResume(format!(
                "Expected interrupt_id '{}', got '{}'",
                yield_req.interrupt_id, interrupt_id
            )));
        }

        // Try to update the latest checkpoint from Yielded to Running and set claimed_interrupt (NOT resolved_interrupt)
        let rows_affected = query(
            r#"
            UPDATE takeln_checkpoints
            SET status = 'Running', claimed_interrupt = $1
            WHERE id = (
                SELECT id FROM takeln_checkpoints
                WHERE thread_id = $2
                ORDER BY created_at DESC LIMIT 1
            ) AND status = 'Yielded'
            "#,
        )
        .bind(interrupt_id)
        .bind(thread_id)
        .execute(&self.pool)
        .await
        .map_err(|e| TakelnError::CheckpointError(format!("Failed to claim interrupt: {}", e)))?
        .rows_affected();

        if rows_affected == 1 {
            Ok(ClaimResult::Claimed)
        } else {
            Err(TakelnError::ExecutionError(format!(
                "Concurrent resume claim failed for thread '{}'",
                thread_id
            )))
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
