use thiserror::Error;

/// Node-level execution flow signals.
///
/// Returned by [`Node::call`](crate::Node::call) to indicate execution outcomes that affect
/// the graph's control flow (retry, suspend, abort, budget).
#[derive(Debug, Error, Clone)]
#[non_exhaustive]
pub enum GraphError {
    /// A transient error that the retry policy may recover from.
    #[error("Retryable error: {0}")]
    Retryable(String),
    /// An unrecoverable error that should halt execution immediately.
    #[error("Fatal error: {0}")]
    Fatal(String),
    /// The node is suspending execution (e.g., awaiting human approval).
    #[error("Suspended/Yielded: {0}")]
    Yield(String),
    /// The node's cost would exceed the configured budget.
    #[error("Budget exceeded: spent {spent_eur:.4}€ of {limit_eur:.4}€ limit")]
    BudgetExceeded { spent_eur: f64, limit_eur: f64 },
}

/// Runner-level errors for the graph orchestrator.
///
/// These errors are returned by [`Graph::run`](crate::Graph::run), [`Graph::run_dag`](crate::Graph::run_dag),
/// [`Graph::resume`](crate::Graph::resume), and [`Graph::resume_dag`](crate::Graph::resume_dag).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TakelnError {
    /// A node referenced in the graph or DAG is not registered.
    #[error("Node '{0}' not found in graph registry")]
    NodeNotFound(String),
    /// The checkpointer encountered a persistence failure.
    #[error("Checkpoint failure: {0}")]
    CheckpointError(String),
    /// Cumulative node costs exceeded the configured budget.
    #[error("Budget exceeded: spent {spent_eur:.4}€ of {limit_eur:.4}€ limit")]
    BudgetExceeded { spent_eur: f64, limit_eur: f64 },
    /// The DAG has pending nodes whose dependencies can never be satisfied.
    #[error("DAG deadlock — pending nodes with no satisfied dependencies: {0}")]
    DAGDeadlock(String),
    /// A parallel task panicked inside the `JoinSet`.
    #[error("JoinSet panic: {0}")]
    JoinError(String),
    /// General execution error wrapping node-level failures.
    #[error("Execution error: {0}")]
    ExecutionError(String),
    /// State serialization failed during checkpointing.
    #[error("Serialization error: {0}")]
    SerializationError(String),
    /// State deserialization failed during checkpoint loading.
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    /// DAG execution exceeded the maximum allowed recursion depth.
    #[error("DAG recursion depth {depth} exceeds global cap {limit}")]
    RecursionLimitExceeded { depth: u8, limit: u8 },
    /// Some nodes in a parallel wave failed while others succeeded.
    /// Only returned when `WaveFailurePolicy::ContinueOnError` is set.
    #[error("Partial wave failure: {succeeded:?} succeeded, {failed:?} failed")]
    PartialWaveFailure {
        succeeded: Vec<String>,
        failed: Vec<(String, String)>,
    },
}
