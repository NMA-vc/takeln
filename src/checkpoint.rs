use crate::checkpoint_meta::{CheckpointMeta, CheckpointStatus, RetentionPolicy};
use crate::dag::DAG;
use crate::error::TakelnError;
use crate::graph::State;
use async_trait::async_trait;

/// Result of claiming a thread resume interrupt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClaimResult {
    /// The interrupt has been successfully claimed by this process.
    Claimed,
    /// The interrupt claim is currently in progress (active but unresolved).
    InProgress,
    /// The interrupt has already been claimed and resolved previously.
    AlreadyCompleted,
}

/// Evidence and optimistic-concurrency precondition for a durable write.
/// Every durable snapshot carries the run authorization hash and the exact
/// connector-receipt chain state observed at the boundary. Before the first
/// connector receipt that state is explicitly `(0, None)`; no synthetic
/// receipt hash is permitted.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CheckpointProof {
    /// Canonical hash of the run's authorizing capability grant. Required for
    /// every durable record; an empty grant set is still hashed explicitly.
    pub authorization_hash: Option<String>,
    /// Verified connector-receipt chain state at this boundary. The genesis
    /// state is `(0, None)`, never a fabricated receipt hash.
    pub receipt_chain_count: u64,
    pub receipt_chain_head: Option<String>,
    pub expected_head: Option<String>,
}

/// Outcome of [`Checkpointer::save_state_bound`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointSave {
    Created { checkpoint_id: String },
    Advanced { checkpoint_id: String },
    IdempotentReplay { checkpoint_id: String },
    Conflict { actual_head: Option<String> },
}

impl CheckpointSave {
    pub fn checkpoint_id(&self) -> Option<&str> {
        match self {
            Self::Created { checkpoint_id }
            | Self::Advanced { checkpoint_id }
            | Self::IdempotentReplay { checkpoint_id } => Some(checkpoint_id),
            Self::Conflict { .. } => None,
        }
    }
}

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
    /// Return the immutable descriptor at the current head, when the store
    /// supports optimistic concurrency. Legacy stores have no head.
    async fn current_head(&self, _thread_id: String) -> Result<Option<String>, TakelnError> {
        Ok(None)
    }

    /// Receipt/CAS-aware boundary. It takes exactly the arguments of
    /// [`Checkpointer::save_state`] (including upstream's `yield_request`,
    /// `claimed_interrupt` and `resolved_interrupt`) plus a [`CheckpointProof`].
    /// Legacy stores inherit compatibility: the default forwards to
    /// `save_state` and ignores the proof, so a store without optimistic
    /// concurrency behaves as before. A store that supports it overrides this
    /// and performs the head comparison atomically with the write.
    #[allow(clippy::too_many_arguments)]
    async fn save_state_bound(
        &self,
        thread_id: String,
        state: S,
        next_node: String,
        dag: Option<&DAG>,
        status: CheckpointStatus,
        yield_request: Option<crate::hitl::YieldRequest>,
        claimed_interrupt: Option<String>,
        resolved_interrupt: Option<String>,
        _proof: CheckpointProof,
    ) -> Result<CheckpointSave, TakelnError> {
        let checkpoint_id = self
            .save_state(
                thread_id,
                state,
                next_node,
                dag,
                status,
                yield_request,
                claimed_interrupt,
                resolved_interrupt,
            )
            .await?;
        Ok(CheckpointSave::Created { checkpoint_id })
    }

    /// Saves the current graph state under a `thread_id`.
    ///
    /// `next_node` denotes the node where execution will resume.
    /// `status` indicates what the graph was doing when this checkpoint was taken.
    /// Returns a unique `checkpoint_id` representing this snapshot.
    #[allow(clippy::too_many_arguments)]
    async fn save_state(
        &self,
        thread_id: String,
        state: S,
        next_node: String,
        dag: Option<&DAG>,
        status: CheckpointStatus,
        yield_request: Option<crate::hitl::YieldRequest>,
        claimed_interrupt: Option<String>,
        resolved_interrupt: Option<String>,
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

    /// Atomically check and claim the interrupt for a thread resume operation.
    ///
    /// This is a critical compare-and-swap (CAS) operation to ensure single-winner
    /// semantics for concurrent resume calls.
    async fn claim_interrupt(&self, thread_id: &str, interrupt_id: &str) -> Result<ClaimResult, TakelnError>;
}
