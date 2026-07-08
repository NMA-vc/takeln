//! Dynamic node execution primitives.
//!
//! Dynamic nodes can imperatively invoke child nodes at runtime,
//! enabling patterns like loops, fan-out over dynamic lists, and
//! conditional sub-workflows that aren't known at graph construction time.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::context::NodeContext;
use crate::error::GraphError;
use crate::graph::{Node, NodeOutput, State};

/// A handle that allows dynamic nodes to execute child nodes imperatively.
///
/// Provided by the graph executor during dynamic node execution.
/// Not constructible by user code.
///
/// # Examples
///
/// ```ignore
/// // Inside a dynamic node closure:
/// let result = runner.run_child("process_item", &ctx, item_state).await?;
/// ```
pub struct ChildRunner<S: State> {
    pub(crate) nodes: HashMap<String, Arc<dyn Node<S>>>,
}

impl<S: State> ChildRunner<S> {
    /// # HITL restriction
    ///
    /// Child nodes inside a dynamic node **cannot yield** for human-in-the-loop
    /// interaction. If a child returns [`GraphError::Yield`], it is converted to
    /// [`GraphError::YieldInDynamicNode`] because dynamic execution is atomic and
    /// does not support per-child checkpointing. Place HITL nodes at the top level
    /// of the graph instead.
    pub async fn run_child(&self, child_name: &str, ctx: &NodeContext, state: S) -> Result<NodeOutput<S>, GraphError> {
        let node = self
            .nodes
            .get(child_name)
            .ok_or_else(|| GraphError::Fatal(format!("Child node '{}' not found in graph", child_name)))?;
        // Create a child context inheriting the parent's identity
        let child_ctx = NodeContext::new(
            ctx.thread_id.clone(),
            child_name.to_string(),
            0, // first attempt
            ctx.last_checkpoint_id.clone(),
            ctx.budget_remaining_eur,
            ctx.cancellation.clone(),
            None, // no resumed_input for child nodes
        );
        match node.call(child_ctx, state).await {
            Err(GraphError::Yield(request)) => Err(GraphError::YieldInDynamicNode {
                interrupt_id: request.interrupt_id,
            }),
            other => other,
        }
    }

    /// Execute a named child node, returning only the transformed state.
    ///
    /// Convenience wrapper around [`run_child`](Self::run_child) that
    /// discards the `NodeMeta`.
    pub async fn run_child_state(&self, child_name: &str, ctx: &NodeContext, state: S) -> Result<S, GraphError> {
        self.run_child(child_name, ctx, state).await.map(|output| output.state)
    }
}

/// A node that can imperatively orchestrate child node execution.
///
/// Unlike regular [`Node`]s which are pure state transforms, dynamic nodes
/// receive a [`ChildRunner`] that lets them invoke other registered nodes
/// at runtime. This enables patterns like:
///
/// - Iterating over a list and processing each item with a child node
/// - Conditionally running different sub-workflows based on runtime data
/// - Accumulating results from multiple child invocations
///
/// # Checkpointing
///
/// Dynamic node execution is **atomic** — child node calls within a dynamic
/// node do not produce individual checkpoints. If the process crashes mid-way
/// through a dynamic node, the entire dynamic node re-executes on resume.
/// For per-step checkpointing, use a DAG with explicit nodes instead.
#[async_trait]
pub trait DynamicNode<S: State>: Send + Sync {
    /// Execute the dynamic node with access to a child runner.
    async fn call(&self, ctx: NodeContext, state: S, runner: &ChildRunner<S>) -> Result<NodeOutput<S>, GraphError>;
}

/// A closure-based dynamic node.
///
/// Created via [`Graph::add_dynamic_fn_node`](crate::Graph::add_dynamic_fn_node)
/// or [`GraphBuilder::dynamic_fn_node`](crate::GraphBuilder::dynamic_fn_node).
pub struct DynamicFnNode<F> {
    pub(crate) f: F,
}

#[async_trait]
impl<S, F, Fut> DynamicNode<S> for DynamicFnNode<F>
where
    S: State,
    F: Fn(NodeContext, S, &ChildRunner<S>) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<NodeOutput<S>, GraphError>> + Send,
{
    async fn call(&self, ctx: NodeContext, state: S, runner: &ChildRunner<S>) -> Result<NodeOutput<S>, GraphError> {
        (self.f)(ctx, state, runner).await
    }
}
