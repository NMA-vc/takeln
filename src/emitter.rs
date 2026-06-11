use crate::graph::NodeMeta;
use async_trait::async_trait;

/// Status of a node execution span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SpanStatus {
    Success,
    Error,
    Cancelled,
    Retrying,
}

/// Structured context passed to the span emitter for each node execution event.
#[derive(Debug, Clone)]
pub struct SpanContext<'a> {
    /// Thread/conversation ID for this execution.
    pub thread_id: &'a str,
    /// Name of the node being executed.
    pub node_name: &'a str,
    /// Checkpoint ID if a checkpoint was saved for this step.
    pub checkpoint_id: Option<&'a str>,
    /// Current retry attempt (0-indexed).
    pub attempt: u8,
    /// Execution duration in milliseconds.
    pub duration_ms: u64,
    /// Cost in EUR if available.
    pub cost_eur: Option<f64>,
    /// Execution outcome.
    pub status: SpanStatus,
    /// DAG ID if this is a parallel DAG execution.
    pub dag_id: Option<&'a str>,
    /// Error message if the node failed.
    pub error: Option<&'a str>,
    /// Node metadata (tokens, cost, model).
    pub meta: &'a NodeMeta,
}

/// Trait for emitting observability spans during graph execution.
///
/// # `async-trait` Note
///
/// This trait uses `#[async_trait]` because it is stored as `Arc<dyn SpanEmitter>`
/// internally, requiring dynamic dispatch.
#[async_trait]
pub trait SpanEmitter: Send + Sync {
    /// Called after each node execution attempt with structured context.
    async fn emit(&self, ctx: &SpanContext<'_>);
}

/// No-op emitter used when observability is not configured.
pub struct NoopEmitter;

#[async_trait]
impl SpanEmitter for NoopEmitter {
    async fn emit(&self, _ctx: &SpanContext<'_>) {}
}

/// Emitter that uses the `tracing` crate to emit structured spans.
///
/// Each node execution creates a span at INFO level with all context fields.
/// Retries and errors are logged at WARN level within the span.
pub struct TracingEmitter;

#[async_trait]
impl SpanEmitter for TracingEmitter {
    async fn emit(&self, ctx: &SpanContext<'_>) {
        match ctx.status {
            SpanStatus::Success => {
                tracing::info!(
                    thread_id = ctx.thread_id,
                    node = ctx.node_name,
                    duration_ms = ctx.duration_ms,
                    cost_eur = ?ctx.cost_eur,
                    tokens_in = ?ctx.meta.tokens_in,
                    tokens_out = ?ctx.meta.tokens_out,
                    model = ?ctx.meta.model,
                    attempt = ctx.attempt,
                    "Node completed successfully"
                );
            }
            SpanStatus::Retrying => {
                tracing::warn!(
                    thread_id = ctx.thread_id,
                    node = ctx.node_name,
                    attempt = ctx.attempt,
                    error = ?ctx.error,
                    "Node retrying"
                );
            }
            SpanStatus::Error => {
                tracing::error!(
                    thread_id = ctx.thread_id,
                    node = ctx.node_name,
                    duration_ms = ctx.duration_ms,
                    attempt = ctx.attempt,
                    error = ?ctx.error,
                    "Node failed"
                );
            }
            SpanStatus::Cancelled => {
                tracing::warn!(
                    thread_id = ctx.thread_id,
                    node = ctx.node_name,
                    duration_ms = ctx.duration_ms,
                    "Node cancelled"
                );
            }
        }
    }
}
