use crate::checkpoint_meta::{CheckpointMeta, CheckpointStatus, RetentionPolicy};
use crate::dag::DAG;
use crate::error::TakelnError;
use crate::graph::State;
use async_trait::async_trait;

/// Interface for persisting and retrieving execution states across process restarts.
///
/// Implementations save and load the graph state, the next node pointer, and
/// optionally a DAG snapshot. Built-in implementations are provided for
/// in-memory storage ([`InMemoryCheckpointer`](crate::InMemoryCheckpointer))
/// and PostgreSQL ([`PostgresCheckpointer`](crate::PostgresCheckpointer), behind the `postgres` feature).
///
/// # `async-trait` Note
///
/// This trait uses `#[async_trait]` because checkpointers are passed as
/// `&impl Checkpointer<S>` to the graph executor. Rust's native async fn
/// in traits (stabilized in 1.75) does not yet support `dyn` dispatch.
/// This dependency will be removed when that limitation is lifted.
#[async_trait]
pub trait Checkpointer<S: State>: Send + Sync {
    /// Saves the current graph state under a `thread_id`.
    ///
    /// `next_node` denotes the node where execution will resume.
    /// `status` indicates what the graph was doing when this checkpoint was taken.
    /// Returns a unique `checkpoint_id` representing this snapshot.
    async fn save_state(
        &self,
        thread_id: String,
        state: S,
        next_node: String,
        dag: Option<&DAG>,
        status: CheckpointStatus,
    ) -> Result<String, TakelnError>;

    /// Retrieves the most recent checkpoint for a given `thread_id`.
    ///
    /// Returns the state, checkpoint metadata, and optional DAG snapshot.
    async fn load_state(&self, thread_id: String) -> Result<Option<(S, CheckpointMeta, Option<DAG>)>, TakelnError>;

    /// Retrieves a specific historical checkpoint by its `checkpoint_id`.
    async fn load_version(
        &self,
        thread_id: String,
        checkpoint_id: String,
    ) -> Result<Option<(S, CheckpointMeta, Option<DAG>)>, TakelnError>;

    /// Lists all historical checkpoints for a thread.
    ///
    /// Returns checkpoint metadata entries ordered by creation time (ascending).
    async fn list_checkpoints(&self, thread_id: String) -> Result<Vec<CheckpointMeta>, TakelnError>;

    /// Deletes checkpoints according to the given retention policy.
    ///
    /// Returns the number of checkpoints deleted.
    async fn delete_checkpoints(&self, thread_id: String, policy: RetentionPolicy) -> Result<usize, TakelnError>;
}
