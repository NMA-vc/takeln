//! Resource limits for graph execution.

/// Configurable resource boundaries for graph execution.
///
/// Provides hard limits on concurrency, memory usage, and cardinality
/// to prevent unbounded resource consumption in production.
///
/// # Defaults
///
/// All defaults are generous enough for typical workloads:
/// - 64 concurrent DAG nodes
/// - 10,000 execution history records
/// - 10 MB max checkpoint payload
/// - 10,000 max DAG nodes
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ResourceLimits {
    /// Maximum concurrent nodes in a DAG wave (default: 64).
    pub max_concurrent_nodes: usize,
    /// Maximum execution history records retained in memory (default: 10,000).
    pub max_execution_records: usize,
    /// Maximum checkpoint payload size in bytes (default: 10 MB).
    pub max_checkpoint_bytes: usize,
    /// Maximum DAG nodes allowed (default: 10,000).
    pub max_dag_nodes: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_concurrent_nodes: 64,
            max_execution_records: 10_000,
            max_checkpoint_bytes: 10 * 1024 * 1024,
            max_dag_nodes: 10_000,
        }
    }
}

impl ResourceLimits {
    /// Set the maximum concurrent nodes in a DAG wave.
    pub fn with_max_concurrent_nodes(mut self, n: usize) -> Self {
        self.max_concurrent_nodes = n;
        self
    }

    /// Set the maximum execution history records.
    pub fn with_max_execution_records(mut self, n: usize) -> Self {
        self.max_execution_records = n;
        self
    }

    /// Set the maximum checkpoint payload size in bytes.
    pub fn with_max_checkpoint_bytes(mut self, n: usize) -> Self {
        self.max_checkpoint_bytes = n;
        self
    }

    /// Set the maximum DAG nodes allowed.
    pub fn with_max_dag_nodes(mut self, n: usize) -> Self {
        self.max_dag_nodes = n;
        self
    }
}
