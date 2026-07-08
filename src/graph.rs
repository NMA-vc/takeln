use crate::checkpoint::Checkpointer;
use crate::checkpoint_meta::{CheckpointStatus, CrashRecoveryPolicy};
use crate::context::NodeContext;
use crate::dag::{NodeStatus, DAG};
use crate::dynamic::{ChildRunner, DynamicFnNode, DynamicNode};
use crate::emitter::{NoopEmitter, SpanContext, SpanEmitter, SpanStatus};
use crate::error::{GraphError, TakelnError};
use crate::history::ExecutionRecord;
use crate::hitl::{ResumeMode, YieldRequest};
use crate::merge::Merge;
use crate::metrics::MetricsHook;
use crate::resource_limits::ResourceLimits;
use async_trait::async_trait;
use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

/// Retry policy attached to the graph or overridden per-node.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first). Default: 3.
    pub max_attempts: u8,
    /// Base delay in milliseconds before the first retry. Default: 500ms.
    pub base_delay_ms: u64,
    /// Maximum delay cap in milliseconds. Default: 30_000ms.
    pub max_delay_ms: u64,
    /// Apply ±25% random jitter to each delay. Default: true.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 500,
            max_delay_ms: 30_000,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Compute the delay for a given attempt (0-indexed), with optional jitter.
    pub fn delay_for(&self, attempt: u8) -> std::time::Duration {
        let base = self.base_delay_ms.saturating_mul(1u64 << attempt.min(10));
        let capped = base.min(self.max_delay_ms);
        let ms = if self.jitter {
            let jitter_range = capped / 4;
            let jitter = if jitter_range > 0 {
                rand::random::<u64>() % jitter_range
            } else {
                0
            };
            capped.saturating_sub(jitter_range / 2).saturating_add(jitter)
        } else {
            capped
        };
        std::time::Duration::from_millis(ms)
    }
}

/// Per-node execution configuration that overrides graph-level defaults.
///
/// Any `None` field falls back to the graph-level setting.
#[derive(Debug, Clone, Default)]
pub struct NodeConfig {
    /// Override the graph-level retry policy for this node.
    pub retry_policy: Option<RetryPolicy>,
    /// Maximum execution duration for this node. If exceeded, the node
    /// is cancelled and treated as a fatal error.
    pub timeout: Option<std::time::Duration>,
    /// Per-node budget cap in EUR. Independent of the graph-level budget.
    pub budget_eur: Option<f64>,
}

/// Controls how a parallel wave handles individual node failures.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum WaveFailurePolicy {
    /// Abort the entire graph on the first node failure in a wave. This is the default.
    #[default]
    FailFast,
    /// Complete all remaining nodes in the wave, then report partial failure.
    /// Returns `TakelnError::PartialWaveFailure` with both succeeded and failed node names.
    ContinueOnError,
}

/// A generic bound for the payload passed between nodes.
/// Must be thread-safe for async executors and serializable for checkpointing.
pub trait State: Clone + Send + Sync + Serialize + DeserializeOwned + 'static {}

/// Blanket implementation applying `State` to any type that meets the criteria.
impl<T> State for T where T: Clone + Send + Sync + Serialize + DeserializeOwned + 'static {}

/// Metadata returned alongside the updated state by every node.
/// Used to populate `ExecutionSpan` without requiring a side-channel.
#[derive(Debug, Clone, Default)]
pub struct NodeMeta {
    /// Input tokens consumed by the LLM call (if any).
    pub tokens_in: Option<u32>,
    /// Output tokens produced by the LLM call (if any).
    pub tokens_out: Option<u32>,
    /// Estimated cost in EUR (if any).
    pub cost_eur: Option<f64>,
    /// LLM model name used (if any).
    pub model: Option<String>,
}

impl NodeMeta {
    pub fn llm(tokens_in: u32, tokens_out: u32, model: impl Into<String>) -> Self {
        let model_str = model.into();
        let cost = (tokens_in as f64 * 0.00000015) + (tokens_out as f64 * 0.0000006);
        Self {
            tokens_in: Some(tokens_in),
            tokens_out: Some(tokens_out),
            cost_eur: Some(cost),
            model: Some(model_str),
        }
    }
}

/// Wraps the updated state with execution metadata for observability.
pub struct NodeOutput<S: State> {
    /// The mutated state to pass to the next node.
    pub state: S,
    /// Optional event signal for ADK-style declarative routing.
    pub event: Option<String>,
    /// Optional metadata for span emission.
    pub meta: NodeMeta,
}

impl<S: State> NodeOutput<S> {
    /// Convenience: state only, no metadata.
    pub fn bare(state: S) -> Self {
        Self {
            state,
            event: None,
            meta: NodeMeta::default(),
        }
    }

    /// Convenience: state with full LLM metadata.
    pub fn with_llm(state: S, tokens_in: u32, tokens_out: u32, model: impl Into<String>) -> Self {
        Self {
            state,
            event: None,
            meta: NodeMeta::llm(tokens_in, tokens_out, model),
        }
    }

    /// Builder method to attach an ADK-style event for declarative routing.
    pub fn with_event(mut self, event: impl Into<String>) -> Self {
        self.event = Some(event.into());
        self
    }
}

/// A computational unit within the graph that transforms state.
///
/// Each node receives the current state, performs work (e.g., an LLM call,
/// database query, or computation), and returns the updated state along
/// with execution metadata.
///
/// # `async-trait` Note
///
/// This trait uses `#[async_trait]` because nodes are stored as `Arc<dyn Node<S>>`
/// internally, requiring dynamic dispatch. Rust's native async fn in traits
/// (stabilized in 1.75) does not yet support `dyn` dispatch. This dependency
/// will be removed when that limitation is lifted.
#[async_trait]
pub trait Node<S: State>: Send + Sync {
    /// Execute this node with the given context and state.
    async fn call(&self, ctx: NodeContext, state: S) -> Result<NodeOutput<S>, GraphError>;
}

/// A node implemented as an async closure, avoiding the need for a full struct + trait impl.
///
/// # Example
/// ```rust,no_run
/// # use takeln::{Graph, NodeContext, NodeOutput, GraphError};
/// # #[derive(Clone, serde::Serialize, serde::Deserialize)] struct S { v: String }
/// # let mut graph = Graph::new();
/// graph.add_fn_node("transform", |_ctx: NodeContext, mut state: S| async move {
///     state.v.push_str("_transformed");
///     Ok(NodeOutput::bare(state))
/// });
/// ```
pub struct FnNode<F> {
    f: F,
}

#[async_trait]
impl<S, F, Fut> Node<S> for FnNode<F>
where
    S: State,
    F: Fn(NodeContext, S) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<NodeOutput<S>, GraphError>> + Send,
{
    async fn call(&self, ctx: NodeContext, state: S) -> Result<NodeOutput<S>, GraphError> {
        (self.f)(ctx, state).await
    }
}

/// Represents mapping transitions
pub enum Edge<S: State> {
    Unconditional(String),
    Conditional(Box<dyn Fn(&S) -> String + Send + Sync>),
}

/// Events broadcast during graph execution.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub enum GraphEvent {
    NodeStarted {
        thread_id: String,
        node_name: String,
        started_at: chrono::DateTime<chrono::Utc>,
        sequence_number: u64,
    },
    NodeFinished {
        thread_id: String,
        node_name: String,
        duration_ms: u64,
        status: String,
        cost: Option<f64>,
        sequence_number: u64,
    },
    GraphFinished {
        thread_id: String,
        cost: f64,
        sequence_number: u64,
    },
}

/// The Orchestrator Graph holding the registry of named Nodes.
pub struct Graph<S: State> {
    nodes: HashMap<String, Arc<dyn Node<S>>>,
    pub(crate) dynamic_nodes: HashMap<String, Arc<dyn DynamicNode<S>>>,
    edges: HashMap<String, Edge<S>>,
    emitter: Arc<dyn SpanEmitter>,
    retry_policy: RetryPolicy,
    budget_eur: Option<f64>,
    interrupt_before: HashSet<String>,
    interrupt_after: HashSet<String>,
    crash_recovery_policy: CrashRecoveryPolicy,
    event_tx: tokio::sync::broadcast::Sender<GraphEvent>,
    node_configs: HashMap<String, NodeConfig>,
    wave_failure_policy: WaveFailurePolicy,
    event_seq: Arc<std::sync::atomic::AtomicU64>,
    metrics_hook: Arc<dyn MetricsHook>,
    execution_records: Arc<tokio::sync::Mutex<VecDeque<ExecutionRecord>>>,
    resource_limits: ResourceLimits,
}

impl<S: State> Graph<S> {
    /// Create a graph with the no-op span emitter (observability disabled).
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let (event_tx, _) = tokio::sync::broadcast::channel(128);
        Self {
            nodes: HashMap::new(),
            dynamic_nodes: HashMap::new(),
            edges: HashMap::new(),
            emitter: Arc::new(NoopEmitter),
            retry_policy: RetryPolicy::default(),
            budget_eur: None,
            interrupt_before: HashSet::new(),
            interrupt_after: HashSet::new(),
            crash_recovery_policy: CrashRecoveryPolicy::default(),
            event_tx,
            node_configs: HashMap::new(),
            wave_failure_policy: WaveFailurePolicy::default(),
            event_seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            metrics_hook: crate::metrics::default_metrics_hook(),
            execution_records: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            resource_limits: ResourceLimits::default(),
        }
    }

    /// Create a graph with a custom span emitter for full observability.
    pub fn with_emitter(emitter: Arc<dyn SpanEmitter>) -> Self {
        let (event_tx, _) = tokio::sync::broadcast::channel(128);
        Self {
            nodes: HashMap::new(),
            dynamic_nodes: HashMap::new(),
            edges: HashMap::new(),
            emitter,
            retry_policy: RetryPolicy::default(),
            budget_eur: None,
            interrupt_before: HashSet::new(),
            interrupt_after: HashSet::new(),
            crash_recovery_policy: CrashRecoveryPolicy::default(),
            event_tx,
            node_configs: HashMap::new(),
            wave_failure_policy: WaveFailurePolicy::default(),
            event_seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            metrics_hook: crate::metrics::default_metrics_hook(),
            execution_records: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            resource_limits: ResourceLimits::default(),
        }
    }

    /// Subscribe to execution events.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<GraphEvent> {
        self.event_tx.subscribe()
    }

    /// Add a node where execution will interrupt BEFORE calling it.
    pub fn add_interrupt_before(&mut self, node: &str) {
        self.interrupt_before.insert(node.to_string());
    }

    /// Add a node where execution will interrupt AFTER calling it.
    pub fn add_interrupt_after(&mut self, node: &str) {
        self.interrupt_after.insert(node.to_string());
    }

    /// Set the crash recovery policy for handling checkpoints with `Running` status.
    pub fn set_crash_recovery_policy(&mut self, policy: CrashRecoveryPolicy) {
        self.crash_recovery_policy = policy;
    }

    /// Set a global retry policy for all retryable nodes in this graph.
    pub fn set_retry_policy(&mut self, policy: RetryPolicy) {
        self.retry_policy = policy;
    }

    /// Set a per-thread budget cap in EUR. If cumulative node costs exceed this,
    /// execution is aborted with `TakelnError::BudgetExceeded`.
    pub fn set_budget_eur(&mut self, budget: f64) {
        self.budget_eur = Some(budget);
    }

    /// Register a concrete Node instance into the graph
    pub fn add_node<N: Node<S> + 'static>(&mut self, name: impl Into<String>, node: N) {
        self.nodes.insert(name.into(), Arc::new(node));
    }

    /// Register a node with per-node execution configuration.
    pub fn add_node_with_config<N: Node<S> + 'static>(&mut self, name: impl Into<String>, node: N, config: NodeConfig) {
        let name = name.into();
        self.nodes.insert(name.clone(), Arc::new(node));
        self.node_configs.insert(name, config);
    }

    /// Set the wave failure policy for parallel DAG execution.
    pub fn set_wave_failure_policy(&mut self, policy: WaveFailurePolicy) {
        self.wave_failure_policy = policy;
    }

    /// Appends a static edge transition out of the source node
    pub fn add_edge(&mut self, source: &str, target: &str) {
        self.edges
            .insert(source.to_string(), Edge::Unconditional(target.to_string()));
    }

    /// Executes arbitrary transition logic out of the source node dependent on State mutations
    pub fn add_conditional_edge<F>(&mut self, source: &str, condition_fn: F)
    where
        F: Fn(&S) -> String + Send + Sync + 'static,
    {
        self.edges
            .insert(source.to_string(), Edge::Conditional(Box::new(condition_fn)));
    }

    fn next_seq(&self) -> u64 {
        self.event_seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Set a custom metrics hook for collecting execution metrics.
    pub fn set_metrics_hook(&mut self, hook: Arc<dyn MetricsHook>) {
        self.metrics_hook = hook;
    }

    /// Retrieve the execution history recorded during this graph's lifetime.
    pub async fn execution_history(&self) -> Vec<ExecutionRecord> {
        self.execution_records.lock().await.iter().cloned().collect()
    }

    /// Register an async closure as a node, avoiding the need for a separate struct.
    pub fn add_fn_node<F, Fut>(&mut self, name: impl Into<String>, f: F)
    where
        F: Fn(NodeContext, S) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<NodeOutput<S>, GraphError>> + Send + 'static,
    {
        self.nodes.insert(name.into(), Arc::new(FnNode { f }));
    }

    /// Register an async closure as a node that doesn't need execution context.
    ///
    /// This is a convenience wrapper around [`add_fn_node`](Self::add_fn_node)
    /// for simple transformations that don't use `NodeContext`.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use takeln::{Graph, NodeOutput, GraphError};
    /// # #[derive(Clone, serde::Serialize, serde::Deserialize)] struct S { v: String }
    /// # let mut graph = Graph::<S>::new();
    /// graph.add_simple_fn_node("transform", |mut state: S| async move {
    ///     state.v.push_str("_transformed");
    ///     Ok(NodeOutput::bare(state))
    /// });
    /// ```
    pub fn add_simple_fn_node<F, Fut>(&mut self, name: impl Into<String>, f: F)
    where
        F: Fn(S) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<NodeOutput<S>, GraphError>> + Send + 'static,
    {
        // Wrap the simple closure to accept and ignore NodeContext
        struct SimpleFnNode<F> {
            f: F,
        }

        #[async_trait]
        impl<S, F, Fut> Node<S> for SimpleFnNode<F>
        where
            S: State,
            F: Fn(S) -> Fut + Send + Sync,
            Fut: std::future::Future<Output = Result<NodeOutput<S>, GraphError>> + Send,
        {
            async fn call(&self, _ctx: NodeContext, state: S) -> Result<NodeOutput<S>, GraphError> {
                (self.f)(state).await
            }
        }

        self.nodes.insert(name.into(), Arc::new(SimpleFnNode { f }));
    }

    /// Register a dynamic node that can invoke child nodes at runtime.
    pub fn add_dynamic_node(&mut self, name: impl Into<String>, node: impl DynamicNode<S> + 'static) {
        self.dynamic_nodes.insert(name.into(), Arc::new(node));
    }

    /// Register a closure-based dynamic node.
    pub fn add_dynamic_fn_node<F, Fut>(&mut self, name: impl Into<String>, f: F)
    where
        F: Fn(NodeContext, S, &ChildRunner<S>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<NodeOutput<S>, GraphError>> + Send,
    {
        self.dynamic_nodes.insert(name.into(), Arc::new(DynamicFnNode { f }));
    }

    /// Set resource limits for graph execution.
    pub fn set_resource_limits(&mut self, limits: ResourceLimits) {
        self.resource_limits = limits;
    }

    /// Create a builder for constructing a graph with a fluent API.
    pub fn builder() -> GraphBuilder<S> {
        GraphBuilder { graph: Graph::new() }
    }
}

/// Builder for constructing graphs with a fluent API.
pub struct GraphBuilder<S: State> {
    graph: Graph<S>,
}

impl<S: State> GraphBuilder<S> {
    /// Add a node to the graph.
    pub fn node<N: Node<S> + 'static>(mut self, name: &str, node: N) -> Self {
        self.graph.add_node(name, node);
        self
    }

    /// Add a node with per-node configuration.
    pub fn node_with_config<N: Node<S> + 'static>(mut self, name: &str, node: N, config: NodeConfig) -> Self {
        self.graph.add_node_with_config(name, node, config);
        self
    }

    /// Add a static edge between two nodes.
    pub fn edge(mut self, source: &str, target: &str) -> Self {
        self.graph.add_edge(source, target);
        self
    }

    /// Add a conditional edge.
    pub fn conditional_edge<F>(mut self, source: &str, condition_fn: F) -> Self
    where
        F: Fn(&S) -> String + Send + Sync + 'static,
    {
        self.graph.add_conditional_edge(source, condition_fn);
        self
    }

    /// Set the global retry policy.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.graph.set_retry_policy(policy);
        self
    }

    /// Set the global budget cap in EUR.
    pub fn budget_eur(mut self, budget: f64) -> Self {
        self.graph.set_budget_eur(budget);
        self
    }

    /// Set the crash recovery policy.
    pub fn crash_recovery_policy(mut self, policy: CrashRecoveryPolicy) -> Self {
        self.graph.set_crash_recovery_policy(policy);
        self
    }

    /// Set the wave failure policy.
    pub fn wave_failure_policy(mut self, policy: WaveFailurePolicy) -> Self {
        self.graph.set_wave_failure_policy(policy);
        self
    }

    /// Add an interrupt-before hook.
    pub fn interrupt_before(mut self, node: &str) -> Self {
        self.graph.add_interrupt_before(node);
        self
    }

    /// Add an interrupt-after hook.
    pub fn interrupt_after(mut self, node: &str) -> Self {
        self.graph.add_interrupt_after(node);
        self
    }

    /// Set a custom span emitter.
    pub fn emitter(mut self, emitter: Arc<dyn SpanEmitter>) -> Self {
        self.graph.emitter = emitter;
        self
    }

    /// Set a custom metrics hook.
    pub fn metrics_hook(mut self, hook: Arc<dyn MetricsHook>) -> Self {
        self.graph.set_metrics_hook(hook);
        self
    }

    /// Add a closure node that receives `NodeContext`.
    pub fn fn_node<F, Fut>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(NodeContext, S) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<NodeOutput<S>, GraphError>> + Send + 'static,
    {
        self.graph.add_fn_node(name, f);
        self
    }

    /// Add a simple closure node that doesn't need `NodeContext`.
    pub fn simple_fn_node<F, Fut>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(S) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<NodeOutput<S>, GraphError>> + Send + 'static,
    {
        self.graph.add_simple_fn_node(name, f);
        self
    }

    /// Register a dynamic node.
    pub fn dynamic_node(mut self, name: impl Into<String>, node: impl DynamicNode<S> + 'static) -> Self {
        self.graph.add_dynamic_node(name, node);
        self
    }

    /// Register a closure-based dynamic node.
    pub fn dynamic_fn_node<F, Fut>(mut self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(NodeContext, S, &ChildRunner<S>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<NodeOutput<S>, GraphError>> + Send,
    {
        self.graph.add_dynamic_fn_node(name, f);
        self
    }

    /// Set resource limits.
    pub fn resource_limits(mut self, limits: ResourceLimits) -> Self {
        self.graph.set_resource_limits(limits);
        self
    }

    /// Consume the builder and return the configured graph.
    pub fn build(self) -> Graph<S> {
        self.graph
    }
}

impl<S: State> Graph<S> {
    /// Check that the state doesn't exceed the max checkpoint payload size.
    fn check_checkpoint_size(&self, state: &S) -> Result<(), TakelnError> {
        let estimated_size = serde_json::to_string(state).map(|s| s.len()).unwrap_or(0);
        if estimated_size > self.resource_limits.max_checkpoint_bytes {
            Err(TakelnError::ExecutionError(format!(
                "Checkpoint payload {} bytes exceeds max_checkpoint_bytes limit of {}",
                estimated_size, self.resource_limits.max_checkpoint_bytes
            )))
        } else {
            Ok(())
        }
    }
}

impl<S: State + Merge> Graph<S> {
    /// Execute a DAG with parallel wave scheduling.
    pub async fn run_dag(
        &self,
        thread_id: &str,
        dag: &mut DAG,
        mut state: S,
        checkpointer: &impl Checkpointer<S>,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
        depth: u8,
    ) -> Result<S, TakelnError> {
        let max_depth_global: u8 = 8;
        if depth > max_depth_global {
            return Err(TakelnError::RecursionLimitExceeded {
                depth,
                limit: max_depth_global,
            });
        }

        // Enforce max_dag_nodes limit
        if dag.nodes.len() > self.resource_limits.max_dag_nodes {
            return Err(TakelnError::ExecutionError(format!(
                "DAG has {} nodes, exceeding max_dag_nodes limit of {}",
                dag.nodes.len(),
                self.resource_limits.max_dag_nodes
            )));
        }

        // Semaphore for max_concurrent_nodes (clamp to at least 1 to prevent deadlock)
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.resource_limits.max_concurrent_nodes.max(1),
        ));

        let mut running_cost_eur: f64 = 0.0;
        let mut completed: HashSet<uuid::Uuid> = dag
            .nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Done)
            .map(|n| n.id)
            .collect();

        let mut is_first_wave = true;

        loop {
            // Identify the next ready wave
            let ready: Vec<usize> = dag
                .nodes
                .iter()
                .enumerate()
                .filter(|(_, n)| {
                    n.status == NodeStatus::Pending && n.depends_on.iter().all(|dep| completed.contains(dep))
                })
                .map(|(i, _)| i)
                .collect();

            if ready.is_empty() {
                let still_pending = dag.nodes.iter().any(|n| n.status == NodeStatus::Pending);
                if still_pending {
                    let pending_names = dag
                        .nodes
                        .iter()
                        .filter(|n| n.status == NodeStatus::Pending)
                        .map(|n| n.step_type.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(TakelnError::DAGDeadlock(pending_names));
                }
                break; // All done
            }

            // Check declarative interrupt_before
            let has_interrupt = ready
                .iter()
                .any(|&idx| self.interrupt_before.contains(&dag.nodes[idx].step_type));

            if !is_first_wave && has_interrupt {
                info!("Thread {}: HITL Interrupt BEFORE parallel wave", thread_id);
                let next_pending = ready
                    .iter()
                    .map(|&idx| dag.nodes[idx].step_type.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                self.check_checkpoint_size(&state)?;
                checkpointer
                    .save_state(
                        thread_id.to_string(),
                        state.clone(),
                        next_pending,
                        Some(dag),
                        CheckpointStatus::Interrupted,
                        None,
                        None,
                        None,
                    )
                    .await?;
                break;
            }
            is_first_wave = false;

            info!(
                "Thread {}: Launching wave of {} node(s) in parallel",
                thread_id,
                ready.len()
            );

            // Mark all wave nodes as Running
            for &idx in &ready {
                dag.nodes[idx].status = NodeStatus::Running;
            }

            // Dispatch wave concurrently
            let mut join_set: JoinSet<(usize, Result<NodeOutput<S>, GraphError>)> = JoinSet::new();

            for &idx in &ready {
                let dag_node = &dag.nodes[idx];

                let node_name = dag_node.step_type.clone();

                let is_dynamic = self.dynamic_nodes.contains_key(&node_name);
                let node = if !is_dynamic {
                    self.nodes.get(&node_name).cloned()
                } else {
                    None
                };
                let dyn_node = if is_dynamic {
                    Some(self.dynamic_nodes.get(&node_name).unwrap().clone())
                } else {
                    None
                };

                if !is_dynamic && node.is_none() {
                    return Err(TakelnError::NodeNotFound(node_name));
                }

                // Resolve per-node config
                let node_config = self.node_configs.get(&node_name);
                let effective_retry = node_config
                    .and_then(|c| c.retry_policy.clone())
                    .unwrap_or_else(|| self.retry_policy.clone());
                let effective_timeout = node_config.and_then(|c| c.timeout);

                let state_clone = state.clone();
                let cancel_clone = cancellation_token.clone();
                let emitter_clone = self.emitter.clone();
                let thread_id_owned = thread_id.to_string();
                let node_name_owned = node_name.clone();
                let event_tx_clone = self.event_tx.clone();
                let event_seq_clone = self.event_seq.clone();
                let _metrics_hook_clone = self.metrics_hook.clone();
                let _execution_records_clone = self.execution_records.clone();
                let nodes_for_runner = if is_dynamic { self.nodes.clone() } else { HashMap::new() };

                let sem = semaphore.clone();
                join_set.spawn(async move {
                    let _permit = sem.acquire().await.unwrap();
                    let started_at = Utc::now();
                    let _ = event_tx_clone.send(GraphEvent::NodeStarted {
                        thread_id: thread_id_owned.clone(),
                        node_name: node_name_owned.clone(),
                        started_at,
                        sequence_number: event_seq_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    });

                    let mut attempt = 0u8;

                    let result = if let Some(dyn_node) = dyn_node {
                        // Dynamic node: call directly with ChildRunner
                        let runner = ChildRunner {
                            nodes: nodes_for_runner,
                        };
                        let ctx = NodeContext::new(
                            thread_id_owned.clone(),
                            node_name_owned.clone(),
                            0,
                            None,
                            None,
                            cancel_clone.clone(),
                            None,
                        );
                        dyn_node.call(ctx, state_clone.clone(), &runner).await
                    } else {
                        // Regular node: existing retry loop
                        let node = node.unwrap();
                        loop {
                            let ctx = NodeContext::new(
                                thread_id_owned.clone(),
                                node_name_owned.clone(),
                                attempt,
                                None, // last_checkpoint_id not tracked per-node in parallel waves
                                None, // budget tracked at graph level, not passed into parallel nodes
                                cancel_clone.clone(),
                                None, // resumed_input not supported in DAG parallel waves
                            );
                            let call_fut_inner = node.call(ctx, state_clone.clone());
                            let res = {
                                if let Some(timeout_dur) = effective_timeout {
                                    let timed = tokio::time::timeout(timeout_dur, call_fut_inner);
                                    if let Some(token) = &cancel_clone {
                                        tokio::select! {
                                            r = timed => r.unwrap_or_else(|_| Err(GraphError::Fatal(format!("Node '{}' timed out after {:?}", node_name_owned, timeout_dur)))),
                                            _ = token.cancelled() => Err(GraphError::Yield(YieldRequest::simple("Cancelled"))),
                                        }
                                    } else {
                                        timed.await.unwrap_or_else(|_| Err(GraphError::Fatal(format!("Node '{}' timed out after {:?}", node_name_owned, timeout_dur))))
                                    }
                                } else if let Some(token) = &cancel_clone {
                                    tokio::select! {
                                        r = call_fut_inner => r,
                                        _ = token.cancelled() => Err(GraphError::Yield(YieldRequest::simple("Cancelled"))),
                                    }
                                } else {
                                    call_fut_inner.await
                                }
                            };

                            match res {
                                Ok(out) => break Ok(out),
                                Err(GraphError::Retryable(msg)) => {
                                    attempt += 1;
                                    if attempt >= effective_retry.max_attempts {
                                        break Err(GraphError::Retryable(msg));
                                    }
                                    let delay = effective_retry.delay_for(attempt - 1);
                                    let default_meta = NodeMeta::default();
                                    emitter_clone
                                        .emit(&SpanContext {
                                            thread_id: &thread_id_owned,
                                            node_name: &node_name_owned,
                                            checkpoint_id: None,
                                            attempt,
                                            duration_ms: 0,
                                            cost_eur: None,
                                            status: SpanStatus::Retrying,
                                            dag_id: None,
                                            error: Some(&msg),
                                            meta: &default_meta,
                                        })
                                        .await;
                                    tokio::time::sleep(delay).await;
                                }
                                Err(other) => break Err(other),
                            }
                        }
                    };

                    let duration_ms = (Utc::now() - started_at).num_milliseconds().max(0) as u64;
                    let (status_str, cost) = match &result {
                        Ok(out) => ("success".to_string(), out.meta.cost_eur),
                        Err(GraphError::Yield(_)) => ("yielded".to_string(), None),
                        Err(_) => ("failed".to_string(), None),
                    };

                    let _ = event_tx_clone.send(GraphEvent::NodeFinished {
                        thread_id: thread_id_owned,
                        node_name: node_name_owned,
                        duration_ms,
                        status: status_str,
                        cost,
                        sequence_number: event_seq_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    });

                    (idx, result)
                });
            }

            // Collect wave results
            let mut wave_states: Vec<(usize, S)> = Vec::new();
            let mut wave_cost: f64 = 0.0;
            let mut has_yielded = false;
            let mut wave_succeeded: Vec<String> = Vec::new();
            let mut wave_failed: Vec<(String, String)> = Vec::new();
            let mut latest_yield_request = None;

            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok((
                        idx,
                        Ok(NodeOutput {
                            state: new_state,
                            event: _,
                            meta,
                        }),
                    )) => {
                        {
                            let node_name_ref = &dag.nodes[idx].step_type;
                            self.emitter
                                .emit(&SpanContext {
                                    thread_id,
                                    node_name: node_name_ref,
                                    checkpoint_id: None,
                                    attempt: 0,
                                    duration_ms: 0,
                                    cost_eur: meta.cost_eur,
                                    status: SpanStatus::Success,
                                    dag_id: None,
                                    error: None,
                                    meta: &meta,
                                })
                                .await;
                            self.metrics_hook
                                .on_node_complete(node_name_ref, 0, SpanStatus::Success);
                            {
                                let record = ExecutionRecord {
                                    node_name: node_name_ref.to_string(),
                                    started_at: Utc::now(),
                                    duration_ms: 0,
                                    status: "success".to_string(),
                                    cost_eur: meta.cost_eur,
                                    checkpoint_id: None,
                                    attempts: 0,
                                    actor: None,
                                    response_hash: None,
                                };
                                let mut records = self.execution_records.lock().await;
                                while records.len() >= self.resource_limits.max_execution_records {
                                    records.pop_front();
                                }
                                records.push_back(record);
                            }
                        }

                        // Per-node budget check
                        let node_config = self.node_configs.get(&dag.nodes[idx].step_type);
                        if let Some(node_budget) = node_config.and_then(|c| c.budget_eur) {
                            if meta.cost_eur.unwrap_or(0.0) > node_budget {
                                return Err(TakelnError::BudgetExceeded {
                                    spent_eur: meta.cost_eur.unwrap_or(0.0),
                                    limit_eur: node_budget,
                                });
                            }
                        }

                        wave_cost += meta.cost_eur.unwrap_or(0.0);
                        dag.nodes[idx].status = NodeStatus::Done;
                        completed.insert(dag.nodes[idx].id);
                        wave_succeeded.push(dag.nodes[idx].step_type.clone());
                        wave_states.push((idx, new_state));
                    }
                    Ok((idx, Err(GraphError::Yield(request)))) => {
                        info!(
                            "Thread {}: Node {} yielded: {}",
                            thread_id, dag.nodes[idx].step_type, request.message
                        );
                        dag.nodes[idx].status = NodeStatus::Yielded;
                        has_yielded = true;
                        latest_yield_request = Some(request.clone());
                    }
                    Ok((idx, Err(e))) => {
                        dag.nodes[idx].status = NodeStatus::Failed;
                        match &e {
                            GraphError::BudgetExceeded { spent_eur, limit_eur } => {
                                return Err(TakelnError::BudgetExceeded {
                                    spent_eur: *spent_eur,
                                    limit_eur: *limit_eur,
                                });
                            }
                            _ => match &self.wave_failure_policy {
                                WaveFailurePolicy::FailFast => {
                                    return Err(TakelnError::ExecutionError(format!(
                                        "Node {} failed: {}",
                                        dag.nodes[idx].step_type, e
                                    )));
                                }
                                WaveFailurePolicy::ContinueOnError => {
                                    wave_failed.push((dag.nodes[idx].step_type.clone(), e.to_string()));
                                }
                            },
                        }
                    }
                    Err(join_err) => {
                        return Err(TakelnError::JoinError(join_err.to_string()));
                    }
                }
            }

            // If wave had failures under ContinueOnError, report them
            if !wave_failed.is_empty() {
                return Err(TakelnError::PartialWaveFailure {
                    succeeded: wave_succeeded,
                    failed: wave_failed,
                });
            }

            if has_yielded {
                let next_pending = dag
                    .nodes
                    .iter()
                    .filter(|n| n.status == NodeStatus::Yielded || n.status == NodeStatus::Pending)
                    .map(|n| n.step_type.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                self.check_checkpoint_size(&state)?;
                checkpointer
                    .save_state(
                        thread_id.to_string(),
                        state.clone(),
                        next_pending,
                        Some(dag),
                        CheckpointStatus::Yielded,
                        latest_yield_request,
                        None,
                        None,
                    )
                    .await?;
                return Ok(state);
            }

            // Sort by node index in the DAG to ensure deterministic merge order
            wave_states.sort_by_key(|(idx, _)| *idx);

            // Merge wave results into running state
            for (_, wave_state) in wave_states {
                state.merge(wave_state);
            }

            // Budget enforcement
            running_cost_eur += wave_cost;
            if let Some(budget) = self.budget_eur {
                if running_cost_eur > budget {
                    return Err(TakelnError::BudgetExceeded {
                        spent_eur: running_cost_eur,
                        limit_eur: budget,
                    });
                }
            }

            // Wave-level checkpoint
            let next_pending = dag
                .nodes
                .iter()
                .find(|n| n.status == NodeStatus::Pending)
                .map(|n| n.step_type.clone())
                .unwrap_or_else(|| "__END__".to_string());

            self.check_checkpoint_size(&state)?;
            let cp_id = checkpointer
                .save_state(
                    thread_id.to_string(),
                    state.clone(),
                    next_pending,
                    Some(dag),
                    CheckpointStatus::Complete,
                    None,
                    None,
                    None,
                )
                .await?;
            let _ = cp_id; // DAG-level checkpoint id not tracked further

            // Check declarative interrupt_after for completed wave nodes
            let has_after_interrupt = ready
                .iter()
                .any(|&idx| self.interrupt_after.contains(&dag.nodes[idx].step_type));
            if has_after_interrupt {
                info!("Thread {}: HITL Interrupt AFTER parallel wave", thread_id);
                break;
            }

            debug!(
                "Thread {}: Wave complete. Running cost: {:.4}€",
                thread_id, running_cost_eur
            );
        }

        info!(
            "Thread {}: DAG execution complete. Total cost: {:.4}€",
            thread_id, running_cost_eur
        );
        let _ = self.event_tx.send(GraphEvent::GraphFinished {
            thread_id: thread_id.to_string(),
            cost: running_cost_eur,
            sequence_number: self.next_seq(),
        });
        Ok(state)
    }
}

impl<S: State> Graph<S> {
    /// Primary execution engine for the orchestrator (sequential, single-node graphs).
    pub async fn run(
        &self,
        thread_id: &str,
        state: S,
        start_node: &str,
        checkpointer: &impl Checkpointer<S>,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<S, TakelnError> {
        self.run_inner(
            thread_id,
            state,
            start_node,
            checkpointer,
            cancellation_token,
            None,
            None,
        )
        .await
    }

    /// Internal execution engine that supports an optional resumed input for HITL re-entry.
    #[allow(clippy::too_many_arguments)]
    async fn run_inner(
        &self,
        thread_id: &str,
        mut state: S,
        start_node: &str,
        checkpointer: &impl Checkpointer<S>,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
        resumed_input: Option<serde_json::Value>,
        resolved_interrupt: Option<String>,
    ) -> Result<S, TakelnError> {
        let mut current_node_name = start_node.to_string();
        let mut running_cost_eur: f64 = 0.0;
        let mut is_first_step = true;
        let mut last_checkpoint_id: Option<String> = None;
        let mut total_cost: f64 = 0.0;
        let mut step_count: usize = 0;
        let mut pending_resumed_input = resumed_input;

        loop {
            // Loop protection: prevent infinite cycles from conditional edges
            if step_count >= self.resource_limits.max_sequential_steps {
                return Err(TakelnError::StepLimitExceeded {
                    steps: step_count,
                    limit: self.resource_limits.max_sequential_steps,
                });
            }
            step_count += 1;

            if current_node_name == "__END__" {
                debug!("Thread {}: Reached __END__, exiting graph run loop.", thread_id);
                let total_duration_ms = 0u64; // sequential run doesn't track total duration separately
                self.metrics_hook
                    .on_graph_complete(thread_id, running_cost_eur, total_duration_ms);
                let _ = self.event_tx.send(GraphEvent::GraphFinished {
                    thread_id: thread_id.to_string(),
                    cost: running_cost_eur,
                    sequence_number: self.next_seq(),
                });
                break;
            }

            // Check declarative interrupt_before
            if !is_first_step && self.interrupt_before.contains(&current_node_name) {
                info!(
                    "Thread {}: HITL Interrupt BEFORE node '{}'",
                    thread_id, current_node_name
                );
                self.check_checkpoint_size(&state)?;
                checkpointer
                    .save_state(
                        thread_id.to_string(),
                        state.clone(),
                        current_node_name.clone(),
                        None,
                        CheckpointStatus::Interrupted,
                        None,
                        None,
                        resolved_interrupt.clone(),
                    )
                    .await?;
                break;
            }
            is_first_step = false;

            let is_dynamic = self.dynamic_nodes.contains_key(&current_node_name);
            let node = if !is_dynamic {
                self.nodes.get(&current_node_name).cloned()
            } else {
                None
            };

            if !is_dynamic && node.is_none() {
                return Err(TakelnError::NodeNotFound(current_node_name));
            }

            // Resolve per-node config
            let node_config = self.node_configs.get(&current_node_name);
            let effective_retry = node_config
                .and_then(|c| c.retry_policy.as_ref())
                .unwrap_or(&self.retry_policy);
            let effective_timeout = node_config.and_then(|c| c.timeout);
            let effective_node_budget = node_config.and_then(|c| c.budget_eur);

            info!("Thread {}: Executing node '{}'", thread_id, current_node_name);

            let started_at = Utc::now();
            let _ = self.event_tx.send(GraphEvent::NodeStarted {
                thread_id: thread_id.to_string(),
                node_name: current_node_name.clone(),
                started_at,
                sequence_number: self.next_seq(),
            });

            let mut attempt = 0u8;
            let call_result = if is_dynamic {
                // Dynamic nodes: call directly with ChildRunner, no retry/timeout wrapper
                let dyn_node = self.dynamic_nodes.get(&current_node_name).unwrap();
                let runner = ChildRunner {
                    nodes: self.nodes.clone(),
                };
                let ctx = NodeContext::new(
                    thread_id.to_string(),
                    current_node_name.clone(),
                    0,
                    last_checkpoint_id.clone(),
                    self.budget_eur.map(|b| b - total_cost),
                    cancellation_token.clone(),
                    pending_resumed_input.take(),
                );
                dyn_node.call(ctx, state.clone(), &runner).await
            } else {
                // Regular nodes: existing retry/timeout loop
                let node = node.unwrap();
                loop {
                    let res = {
                        let ctx = NodeContext::new(
                            thread_id.to_string(),
                            current_node_name.clone(),
                            attempt,
                            last_checkpoint_id.clone(),
                            self.budget_eur.map(|b| b - total_cost),
                            cancellation_token.clone(),
                            pending_resumed_input.take(),
                        );
                        let call_fut = node.call(ctx, state.clone());
                        if let Some(timeout_dur) = effective_timeout {
                            let timed = tokio::time::timeout(timeout_dur, call_fut);
                            if let Some(token) = &cancellation_token {
                                tokio::select! {
                                    r = timed => r.unwrap_or_else(|_| Err(GraphError::Fatal(format!("Node '{}' timed out after {:?}", current_node_name, timeout_dur)))),
                                    _ = token.cancelled() => Err(GraphError::Yield(YieldRequest::simple("Cancelled"))),
                                }
                            } else {
                                timed.await.unwrap_or_else(|_| {
                                    Err(GraphError::Fatal(format!(
                                        "Node '{}' timed out after {:?}",
                                        current_node_name, timeout_dur
                                    )))
                                })
                            }
                        } else if let Some(token) = &cancellation_token {
                            tokio::select! {
                                r = call_fut => r,
                                _ = token.cancelled() => Err(GraphError::Yield(YieldRequest::simple("Cancelled"))),
                            }
                        } else {
                            call_fut.await
                        }
                    };

                    match res {
                        Ok(out) => break Ok(out),
                        Err(GraphError::Retryable(msg)) => {
                            attempt += 1;
                            if attempt >= effective_retry.max_attempts {
                                break Err(GraphError::Retryable(msg));
                            }
                            let delay = effective_retry.delay_for(attempt - 1);
                            {
                                let default_meta = NodeMeta::default();
                                self.emitter
                                    .emit(&SpanContext {
                                        thread_id,
                                        node_name: &current_node_name,
                                        checkpoint_id: None,
                                        attempt,
                                        duration_ms: 0,
                                        cost_eur: None,
                                        status: SpanStatus::Retrying,
                                        dag_id: None,
                                        error: Some(&msg),
                                        meta: &default_meta,
                                    })
                                    .await;
                            }
                            tokio::time::sleep(delay).await;
                        }
                        Err(other) => break Err(other),
                    }
                }
            };

            match call_result {
                Ok(NodeOutput {
                    state: new_state,
                    event,
                    meta,
                }) => {
                    let duration_ms = (Utc::now() - started_at).num_milliseconds().max(0) as u64;
                    running_cost_eur += meta.cost_eur.unwrap_or(0.0);
                    total_cost = running_cost_eur;

                    // Check per-node budget enforcement
                    if let Some(node_budget) = effective_node_budget {
                        if meta.cost_eur.unwrap_or(0.0) > node_budget {
                            let _ = self.event_tx.send(GraphEvent::NodeFinished {
                                thread_id: thread_id.to_string(),
                                node_name: current_node_name.clone(),
                                duration_ms,
                                status: "failed".to_string(),
                                cost: meta.cost_eur,
                                sequence_number: self.next_seq(),
                            });
                            return Err(TakelnError::BudgetExceeded {
                                spent_eur: meta.cost_eur.unwrap_or(0.0),
                                limit_eur: node_budget,
                            });
                        }
                    }

                    // Check global budget enforcement
                    if let Some(budget) = self.budget_eur {
                        if running_cost_eur > budget {
                            let msg = format!(
                                "Budget exceeded: spent {:.4}€ of {:.4}€ limit",
                                running_cost_eur, budget
                            );
                            {
                                let default_meta = NodeMeta::default();
                                self.emitter
                                    .emit(&SpanContext {
                                        thread_id,
                                        node_name: &current_node_name,
                                        checkpoint_id: None,
                                        attempt,
                                        duration_ms,
                                        cost_eur: Some(running_cost_eur),
                                        status: SpanStatus::Error,
                                        dag_id: None,
                                        error: Some(&msg),
                                        meta: &default_meta,
                                    })
                                    .await;
                            }

                            let _ = self.event_tx.send(GraphEvent::NodeFinished {
                                thread_id: thread_id.to_string(),
                                node_name: current_node_name.clone(),
                                duration_ms,
                                status: "failed".to_string(),
                                cost: meta.cost_eur,
                                sequence_number: self.next_seq(),
                            });

                            error!(
                                "Thread {}: Fatal error (budget) at node '{}': {}",
                                thread_id, current_node_name, msg
                            );
                            return Err(TakelnError::BudgetExceeded {
                                spent_eur: running_cost_eur,
                                limit_eur: budget,
                            });
                        }
                    }

                    // Emit observability span
                    self.emitter
                        .emit(&SpanContext {
                            thread_id,
                            node_name: &current_node_name,
                            checkpoint_id: None,
                            attempt,
                            duration_ms,
                            cost_eur: meta.cost_eur,
                            status: SpanStatus::Success,
                            dag_id: None,
                            error: None,
                            meta: &meta,
                        })
                        .await;
                    self.metrics_hook
                        .on_node_complete(&current_node_name, duration_ms, SpanStatus::Success);
                    {
                        let record = ExecutionRecord {
                            node_name: current_node_name.clone(),
                            started_at,
                            duration_ms,
                            status: "success".to_string(),
                            cost_eur: meta.cost_eur,
                            checkpoint_id: None,
                            attempts: attempt,
                            actor: None,
                            response_hash: None,
                        };
                        let mut records = self.execution_records.lock().await;
                        while records.len() >= self.resource_limits.max_execution_records {
                            records.pop_front();
                        }
                        records.push_back(record);
                    }

                    // Emit broadcast event
                    let _ = self.event_tx.send(GraphEvent::NodeFinished {
                        thread_id: thread_id.to_string(),
                        node_name: current_node_name.clone(),
                        duration_ms,
                        status: "success".to_string(),
                        cost: meta.cost_eur,
                        sequence_number: self.next_seq(),
                    });

                    state = new_state;

                    // Determine next_node
                    let next_node = if let Some(target) = event {
                        target
                    } else {
                        match self.edges.get(&current_node_name) {
                            Some(Edge::Unconditional(target)) => target.to_string(),
                            Some(Edge::Conditional(cond_fn)) => cond_fn(&state),
                            None => "__END__".to_string(),
                        }
                    };

                    // Save the state and next_node to the checkpointer
                    self.check_checkpoint_size(&state)?;
                    let checkpoint_id = checkpointer
                        .save_state(
                            thread_id.to_string(),
                            state.clone(),
                            next_node.clone(),
                            None,
                            CheckpointStatus::Complete,
                            None,
                            None,
                            resolved_interrupt.clone(),
                        )
                        .await?;
                    last_checkpoint_id = Some(checkpoint_id.clone());
                    self.metrics_hook
                        .on_checkpoint_saved(thread_id, &checkpoint_id.to_string());

                    // Check declarative interrupt_after
                    if self.interrupt_after.contains(&current_node_name) {
                        info!(
                            "Thread {}: HITL Interrupt AFTER node '{}'",
                            thread_id, current_node_name
                        );
                        break;
                    }

                    current_node_name = next_node;
                }
                Err(GraphError::Yield(request)) => {
                    let duration_ms = (Utc::now() - started_at).num_milliseconds().max(0) as u64;
                    let msg = request.message.clone();
                    {
                        let default_meta = NodeMeta::default();
                        self.emitter
                            .emit(&SpanContext {
                                thread_id,
                                node_name: &current_node_name,
                                checkpoint_id: None,
                                attempt,
                                duration_ms,
                                cost_eur: None,
                                status: SpanStatus::Cancelled,
                                dag_id: None,
                                error: Some(&msg),
                                meta: &default_meta,
                            })
                            .await;
                    }

                    let _ = self.event_tx.send(GraphEvent::NodeFinished {
                        thread_id: thread_id.to_string(),
                        node_name: current_node_name.clone(),
                        duration_ms,
                        status: "yielded".to_string(),
                        cost: None,
                        sequence_number: self.next_seq(),
                    });

                    info!(
                        "Thread {}: Yielding at node '{}': {}",
                        thread_id, current_node_name, msg
                    );
                    self.check_checkpoint_size(&state)?;
                    checkpointer
                        .save_state(
                            thread_id.to_string(),
                            state.clone(),
                            current_node_name.clone(),
                            None,
                            CheckpointStatus::Yielded,
                            Some(request.clone()),
                            None,
                            None,
                        )
                        .await?;
                    break;
                }
                Err(GraphError::Retryable(msg)) => {
                    let duration_ms = (Utc::now() - started_at).num_milliseconds().max(0) as u64;
                    {
                        let default_meta = NodeMeta::default();
                        self.emitter
                            .emit(&SpanContext {
                                thread_id,
                                node_name: &current_node_name,
                                checkpoint_id: None,
                                attempt,
                                duration_ms,
                                cost_eur: None,
                                status: SpanStatus::Error,
                                dag_id: None,
                                error: Some(&msg),
                                meta: &default_meta,
                            })
                            .await;
                    }

                    let _ = self.event_tx.send(GraphEvent::NodeFinished {
                        thread_id: thread_id.to_string(),
                        node_name: current_node_name.clone(),
                        duration_ms,
                        status: "failed".to_string(),
                        cost: None,
                        sequence_number: self.next_seq(),
                    });

                    warn!(
                        "Thread {}: Retryable error at node '{}' after max attempts: {}",
                        thread_id, current_node_name, msg
                    );
                    return Err(TakelnError::ExecutionError(format!(
                        "Node {} failed (retryable) after max attempts: {}",
                        current_node_name, msg
                    )));
                }
                Err(GraphError::Fatal(msg)) => {
                    let duration_ms = (Utc::now() - started_at).num_milliseconds().max(0) as u64;
                    {
                        let default_meta = NodeMeta::default();
                        self.emitter
                            .emit(&SpanContext {
                                thread_id,
                                node_name: &current_node_name,
                                checkpoint_id: None,
                                attempt,
                                duration_ms,
                                cost_eur: None,
                                status: SpanStatus::Error,
                                dag_id: None,
                                error: Some(&msg),
                                meta: &default_meta,
                            })
                            .await;
                    }

                    let _ = self.event_tx.send(GraphEvent::NodeFinished {
                        thread_id: thread_id.to_string(),
                        node_name: current_node_name.clone(),
                        duration_ms,
                        status: "failed".to_string(),
                        cost: None,
                        sequence_number: self.next_seq(),
                    });

                    error!(
                        "Thread {}: Fatal error at node '{}': {}",
                        thread_id, current_node_name, msg
                    );
                    return Err(TakelnError::ExecutionError(format!(
                        "Node {} failed fatally: {}",
                        current_node_name, msg
                    )));
                }
                Err(GraphError::BudgetExceeded { spent_eur, limit_eur }) => {
                    let duration_ms = (Utc::now() - started_at).num_milliseconds().max(0) as u64;
                    let msg = format!("Budget exceeded: spent {:.4}€ of {:.4}€ limit", spent_eur, limit_eur);
                    {
                        let default_meta = NodeMeta::default();
                        self.emitter
                            .emit(&SpanContext {
                                thread_id,
                                node_name: &current_node_name,
                                checkpoint_id: None,
                                attempt: 0,
                                duration_ms,
                                cost_eur: Some(spent_eur),
                                status: SpanStatus::Error,
                                dag_id: None,
                                error: Some(&msg),
                                meta: &default_meta,
                            })
                            .await;
                    }

                    let _ = self.event_tx.send(GraphEvent::NodeFinished {
                        thread_id: thread_id.to_string(),
                        node_name: current_node_name.clone(),
                        duration_ms,
                        status: "failed".to_string(),
                        cost: None,
                        sequence_number: self.next_seq(),
                    });

                    error!(
                        "Thread {}: Fatal error (budget) at node '{}': {}",
                        thread_id, current_node_name, msg
                    );
                    return Err(TakelnError::BudgetExceeded { spent_eur, limit_eur });
                }
                Err(GraphError::YieldInDynamicNode { interrupt_id }) => {
                    error!(
                        "Thread {}: HITL yield inside dynamic node at '{}' (interrupt: '{}'). Move the yielding node to the top level.",
                        thread_id, current_node_name, interrupt_id
                    );
                    return Err(TakelnError::ExecutionError(format!(
                        "HITL yield inside dynamic node is not supported (node: '{}', interrupt: '{}'). Move the yielding node to a top-level graph node.",
                        current_node_name, interrupt_id
                    )));
                }
            }
        }

        Ok(state)
    }

    /// Resume sequential execution of a thread from its last saved checkpoint.
    pub async fn resume(
        &self,
        thread_id: &str,
        checkpointer: &impl Checkpointer<S>,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<Option<S>, TakelnError> {
        let loaded = checkpointer.load_state(thread_id.to_string()).await?;
        if let Some((state, meta, _dag)) = loaded {
            // Apply crash recovery policy if the checkpoint was taken mid-execution
            if meta.status == CheckpointStatus::Running {
                match &self.crash_recovery_policy {
                    CrashRecoveryPolicy::ResetToPending => {
                        tracing::warn!(
                            "Thread {}: Checkpoint status is Running (crash recovery). Resetting to re-execute node '{}'.",
                            thread_id, meta.next_node
                        );
                    }
                    CrashRecoveryPolicy::FailFast => {
                        return Err(TakelnError::ExecutionError(format!(
                            "Checkpoint for thread '{}' has Running status (crash detected). FailFast policy applied.",
                            thread_id
                        )));
                    }
                    CrashRecoveryPolicy::SkipAndContinue => {
                        tracing::warn!(
                            "Thread {}: Checkpoint status is Running (crash recovery). Skipping node '{}' (SkipAndContinue).",
                            thread_id, meta.next_node
                        );
                        // Find the next node after the current one by looking up its edge
                        if let Some(edge) = self.edges.get(&meta.next_node) {
                            let next = match edge {
                                Edge::Unconditional(target) => target.clone(),
                                Edge::Conditional(f) => f(&state),
                            };
                            let final_state = self
                                .run_inner(
                                    thread_id,
                                    state,
                                    &next,
                                    checkpointer,
                                    cancellation_token,
                                    None,
                                    meta.claimed_interrupt.clone(),
                                )
                                .await?;
                            return Ok(Some(final_state));
                        }
                    }
                }
            }
            let final_state = self
                .run_inner(
                    thread_id,
                    state,
                    &meta.next_node,
                    checkpointer,
                    cancellation_token,
                    None,
                    meta.claimed_interrupt.clone(),
                )
                .await?;
            Ok(Some(final_state))
        } else {
            Ok(None)
        }
    }

    /// Resume execution with structured input for a yielded HITL checkpoint.
    ///
    /// Validates the `interrupt_id` matches the pending yield, performs basic schema
    /// validation on the provided `input`, and resumes according to the yield's
    /// [`ResumeMode`]:
    /// - [`ReEntry`](ResumeMode::ReEntry): re-executes the yielded node with the input
    ///   available via [`NodeContext::resumed_input`].
    /// - [`Handoff`](ResumeMode::Handoff): skips the yielded node and proceeds to the
    ///   next node in the graph.
    ///
    /// # Idempotent Resume
    ///
    /// If the latest checkpoint has already been resolved with the same `interrupt_id`
    /// (i.e. `resolved_interrupt == Some(interrupt_id)`), the method short-circuits and
    /// returns `Ok(Some(current_state))` without re-executing anything. This makes it
    /// safe to retry resume calls across network failures or duplicate webhook deliveries.
    pub async fn resume_with_input(
        &self,
        thread_id: &str,
        interrupt_id: &str,
        input: serde_json::Value,
        context: crate::hitl::ResumeContext,
        checkpointer: &impl Checkpointer<S>,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<Option<S>, TakelnError> {
        // 1. Claim the interrupt atomically
        let claim = checkpointer.claim_interrupt(thread_id, interrupt_id).await?;

        // 2. Load the state
        let loaded = checkpointer.load_state(thread_id.to_string()).await?;
        let (state, meta, _dag) = match loaded {
            Some(t) => t,
            None => return Err(TakelnError::NothingToResume(thread_id.to_string())),
        };

        match claim {
            crate::checkpoint::ClaimResult::AlreadyCompleted => {
                info!(
                    "Thread {}: Idempotent resume for interrupt '{}', returning current state",
                    thread_id, interrupt_id
                );
                return Ok(Some(state));
            }
            crate::checkpoint::ClaimResult::InProgress => {
                return Err(TakelnError::ExecutionError(format!(
                    "Resume in progress for interrupt '{}' on thread '{}'",
                    interrupt_id, thread_id
                )));
            }
            crate::checkpoint::ClaimResult::Claimed => {}
        }

        let yield_request = meta.yield_request.as_ref().ok_or_else(|| {
            TakelnError::NothingToResume(format!("Thread '{}' has no yield_request metadata", thread_id))
        })?;

        // Verify interrupt_id matches
        if yield_request.interrupt_id != interrupt_id {
            return Err(TakelnError::InvalidResume(format!(
                "Expected interrupt_id '{}', got '{}'",
                yield_request.interrupt_id, interrupt_id
            )));
        }

        // Basic schema validation if a schema is provided
        if let Some(schema) = &yield_request.schema {
            Self::validate_input_against_schema(interrupt_id, &input, schema)?;
        }

        let response_hash = crate::hitl::compute_response_hash(&input);

        let result =
            match yield_request.resume_mode {
                ResumeMode::ReEntry => {
                    // Re-execute the yielded node with the input
                    let final_res = self
                        .run_inner(
                            thread_id,
                            state.clone(),
                            &meta.next_node,
                            checkpointer,
                            cancellation_token,
                            Some(input),
                            Some(interrupt_id.to_string()),
                        )
                        .await;
                    match final_res {
                        Ok(final_state) => Some(final_state),
                        Err(e) => {
                            // Revert checkpoint to Yielded
                            if let Err(save_err) = checkpointer
                                .save_state(
                                    thread_id.to_string(),
                                    state,
                                    meta.next_node.clone(),
                                    None,
                                    CheckpointStatus::Yielded,
                                    Some(yield_request.clone()),
                                    None,
                                    None,
                                )
                                .await
                            {
                                tracing::error!(
                                "Thread {}: Failed to revert checkpoint status to Yielded after execution failure: {}",
                                thread_id, save_err
                            );
                                return Err(TakelnError::ExecutionError(format!(
                                    "Execution failed: {}. Rollback save failed: {}",
                                    e, save_err
                                )));
                            }
                            return Err(e);
                        }
                    }
                }
                ResumeMode::Handoff => {
                    // Skip the yielded node, resolve its edge, continue from next
                    let next_node = match self.edges.get(&meta.next_node) {
                        Some(Edge::Unconditional(target)) => target.clone(),
                        Some(Edge::Conditional(f)) => f(&state),
                        None => "__END__".to_string(),
                    };
                    let final_res = self
                        .run_inner(
                            thread_id,
                            state.clone(),
                            &next_node,
                            checkpointer,
                            cancellation_token,
                            None,
                            Some(interrupt_id.to_string()),
                        )
                        .await;
                    match final_res {
                        Ok(final_state) => Some(final_state),
                        Err(e) => {
                            // Revert checkpoint to Yielded
                            if let Err(save_err) = checkpointer
                                .save_state(
                                    thread_id.to_string(),
                                    state,
                                    meta.next_node.clone(),
                                    None,
                                    CheckpointStatus::Yielded,
                                    Some(yield_request.clone()),
                                    None,
                                    None,
                                )
                                .await
                            {
                                tracing::error!(
                                "Thread {}: Failed to revert checkpoint status to Yielded after execution failure: {}",
                                thread_id, save_err
                            );
                                return Err(TakelnError::ExecutionError(format!(
                                    "Execution failed: {}. Rollback save failed: {}",
                                    e, save_err
                                )));
                            }
                            return Err(e);
                        }
                    }
                }
            };

        // Audit Logging (Issue #3)
        let record = crate::hitl::ResumeRecord {
            interrupt_id: interrupt_id.to_string(),
            thread_id: thread_id.to_string(),
            node_name: meta.next_node.clone(),
            actor: context.actor.clone(),
            resumed_at: Utc::now(),
            response_hash: response_hash.clone(),
        };

        // Fire metrics hook
        self.metrics_hook.on_resume(&record);

        // Record in execution_records history
        {
            let exec_record = ExecutionRecord {
                node_name: meta.next_node.clone(),
                started_at: record.resumed_at,
                duration_ms: 0,
                status: "resumed".to_string(),
                cost_eur: None,
                checkpoint_id: Some(meta.checkpoint_id.clone()),
                attempts: 0,
                actor: context.actor.clone(),
                response_hash: Some(response_hash),
            };
            let mut records = self.execution_records.lock().await;
            while records.len() >= self.resource_limits.max_execution_records {
                records.pop_front();
            }
            records.push_back(exec_record);
        }

        Ok(result)
    }

    /// Basic schema validation: checks JSON type and enum constraints.
    fn validate_input_against_schema(
        interrupt_id: &str,
        input: &serde_json::Value,
        schema: &serde_json::Value,
    ) -> Result<(), TakelnError> {
        // Type check
        if let Some(expected_type) = schema.get("type").and_then(|t| t.as_str()) {
            let actual_type = match input {
                serde_json::Value::Null => "null",
                serde_json::Value::Bool(_) => "boolean",
                serde_json::Value::Number(n) => {
                    if n.is_i64() || n.is_u64() {
                        "integer"
                    } else {
                        "number"
                    }
                }
                serde_json::Value::String(_) => "string",
                serde_json::Value::Array(_) => "array",
                serde_json::Value::Object(_) => "object",
            };

            // "number" matches both "number" and "integer"
            let type_ok = match expected_type {
                "number" => actual_type == "number" || actual_type == "integer",
                other => actual_type == other,
            };

            if !type_ok {
                return Err(TakelnError::SchemaValidationFailed {
                    interrupt_id: interrupt_id.to_string(),
                    reason: format!("Expected type '{}', got '{}'", expected_type, actual_type),
                });
            }
        }

        // Enum constraint
        if let Some(enum_values) = schema.get("enum").and_then(|e| e.as_array()) {
            if !enum_values.contains(input) {
                return Err(TakelnError::SchemaValidationFailed {
                    interrupt_id: interrupt_id.to_string(),
                    reason: format!("Value {:?} not in allowed enum values {:?}", input, enum_values),
                });
            }
        }

        Ok(())
    }
}

impl<S: State + Merge> Graph<S> {
    /// Resume parallel wave-based DAG execution of a thread from its last saved checkpoint.
    pub async fn resume_dag(
        &self,
        thread_id: &str,
        dag: &mut DAG,
        checkpointer: &impl Checkpointer<S>,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
        depth: u8,
    ) -> Result<Option<S>, TakelnError> {
        let loaded = checkpointer.load_state(thread_id.to_string()).await?;
        if let Some((state, meta, Some(saved_dag))) = loaded {
            // Apply crash recovery policy if needed
            if meta.status == CheckpointStatus::Running {
                match &self.crash_recovery_policy {
                    CrashRecoveryPolicy::ResetToPending => {
                        tracing::warn!(
                            "Thread {}: DAG checkpoint status is Running (crash recovery). Resetting Running nodes to Pending.",
                            thread_id
                        );
                    }
                    CrashRecoveryPolicy::FailFast => {
                        return Err(TakelnError::ExecutionError(format!(
                            "DAG checkpoint for thread '{}' has Running status (crash detected). FailFast policy applied.",
                            thread_id
                        )));
                    }
                    CrashRecoveryPolicy::SkipAndContinue => {
                        tracing::warn!(
                            "Thread {}: DAG checkpoint status is Running. SkipAndContinue: marking Running nodes as Done.",
                            thread_id
                        );
                    }
                }
            }

            dag.restore_statuses(&saved_dag);

            // Apply crash recovery to any Running nodes in the restored DAG
            if meta.status == CheckpointStatus::Running {
                for node in &mut dag.nodes {
                    if node.status == crate::dag::NodeStatus::Running {
                        match &self.crash_recovery_policy {
                            CrashRecoveryPolicy::ResetToPending => {
                                node.status = crate::dag::NodeStatus::Pending;
                            }
                            CrashRecoveryPolicy::SkipAndContinue => {
                                node.status = crate::dag::NodeStatus::Done;
                            }
                            CrashRecoveryPolicy::FailFast => {} // already handled above
                        }
                    }
                }
            }

            let final_state = self
                .run_dag(thread_id, dag, state, checkpointer, cancellation_token, depth)
                .await?;
            Ok(Some(final_state))
        } else {
            Ok(None)
        }
    }
}
