//! Execution context passed to every node invocation.

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Execution context provided to each [`Node::call`](crate::Node::call) invocation.
///
/// Contains runtime metadata for idempotency, observability, budget awareness,
/// and cancellation. Nodes that don't need context can ignore it with `_ctx`.
///
/// # Idempotency
///
/// Use [`execution_id`](Self::execution_id) as an idempotency key for side effects
/// (API calls, database writes, payments) to guard against duplicate execution
/// after crash recovery. Unlike `attempt_id`, `execution_id` is deterministic
/// and stable across retries — it only changes when the logical checkpoint changes.
///
/// # Example
/// ```rust,no_run
/// # use takeln::{Node, NodeContext, NodeOutput, GraphError};
/// # use async_trait::async_trait;
/// # #[derive(Clone, serde::Serialize, serde::Deserialize)] struct S { v: String }
/// struct MyNode;
///
/// #[async_trait]
/// impl Node<S> for MyNode {
///     async fn call(&self, ctx: NodeContext, mut state: S) -> Result<NodeOutput<S>, GraphError> {
///         println!("Node {} attempt {} (exec: {}, attempt: {})", ctx.node_name, ctx.attempt, ctx.execution_id, ctx.attempt_id);
///         Ok(NodeOutput::bare(state))
///     }
/// }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NodeContext {
    /// The thread/session ID for this execution.
    pub thread_id: String,
    /// The name of the currently executing node.
    pub node_name: String,
    /// Current retry attempt (0-indexed; 0 = first attempt).
    pub attempt: u8,
    /// Stable ID for this logical node execution.
    /// Deterministic: derived from thread_id + node_name + last_checkpoint_id.
    /// Use for external idempotency keys that must survive crash/resume.
    pub execution_id: String,
    /// Unique ID for this specific attempt (changes on every retry).
    /// Use for logging/tracing, NOT for idempotency.
    pub attempt_id: String,
    /// The checkpoint ID from the most recent save, if any.
    pub last_checkpoint_id: Option<String>,
    /// Remaining budget in EUR, if a budget is configured.
    pub budget_remaining_eur: Option<f64>,
    /// Cancellation token — check `ctx.cancellation.as_ref().map(|t| t.is_cancelled())`.
    pub cancellation: Option<CancellationToken>,
    /// Input provided when resuming from a yield (only set on re-entry).
    pub resumed_input: Option<serde_json::Value>,
}

impl NodeContext {
    /// Create a new NodeContext. Used internally by the executor.
    pub(crate) fn new(
        thread_id: String,
        node_name: String,
        attempt: u8,
        last_checkpoint_id: Option<String>,
        budget_remaining_eur: Option<f64>,
        cancellation: Option<CancellationToken>,
        resumed_input: Option<serde_json::Value>,
    ) -> Self {
        // Deterministic UUID v5 from namespace + "{thread_id}:{node_name}:{checkpoint_id}"
        let seed = format!(
            "{}:{}:{}",
            thread_id,
            node_name,
            last_checkpoint_id.as_deref().unwrap_or("initial")
        );
        let execution_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, seed.as_bytes()).to_string();

        Self {
            thread_id,
            node_name,
            attempt,
            execution_id,
            attempt_id: Uuid::new_v4().to_string(),
            last_checkpoint_id,
            budget_remaining_eur,
            cancellation,
            resumed_input,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_id_stable_across_attempts() {
        let ctx1 = NodeContext::new(
            "thread1".into(),
            "nodeA".into(),
            0,
            Some("cp1".into()),
            None,
            None,
            None,
        );
        let ctx2 = NodeContext::new(
            "thread1".into(),
            "nodeA".into(),
            1,
            Some("cp1".into()),
            None,
            None,
            None,
        );
        assert_eq!(ctx1.execution_id, ctx2.execution_id);
        assert_ne!(ctx1.attempt_id, ctx2.attempt_id);
    }

    #[test]
    fn execution_id_changes_with_checkpoint() {
        let ctx1 = NodeContext::new(
            "thread1".into(),
            "nodeA".into(),
            0,
            Some("cp1".into()),
            None,
            None,
            None,
        );
        let ctx2 = NodeContext::new(
            "thread1".into(),
            "nodeA".into(),
            0,
            Some("cp2".into()),
            None,
            None,
            None,
        );
        assert_ne!(ctx1.execution_id, ctx2.execution_id);
    }
}
