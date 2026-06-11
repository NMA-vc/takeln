use crate::emitter::SpanStatus;
use std::sync::Arc;

/// Hook for collecting metrics from graph execution.
///
/// Implement this trait to integrate with your preferred metrics system
/// (Prometheus, StatsD, OpenTelemetry Metrics, etc.).
pub trait MetricsHook: Send + Sync {
    /// Called after each node completes (success or failure).
    fn on_node_complete(&self, node_name: &str, duration_ms: u64, status: SpanStatus);
    /// Called when the entire graph execution finishes.
    fn on_graph_complete(&self, thread_id: &str, total_cost: f64, total_duration_ms: u64);
    /// Called after a checkpoint is successfully saved.
    fn on_checkpoint_saved(&self, thread_id: &str, checkpoint_id: &str);
}

/// Default no-op metrics hook that discards all metrics.
pub struct NoopMetricsHook;

impl MetricsHook for NoopMetricsHook {
    fn on_node_complete(&self, _: &str, _: u64, _: SpanStatus) {}
    fn on_graph_complete(&self, _: &str, _: f64, _: u64) {}
    fn on_checkpoint_saved(&self, _: &str, _: &str) {}
}

/// Returns the default no-op metrics hook.
pub(crate) fn default_metrics_hook() -> Arc<dyn MetricsHook> {
    Arc::new(NoopMetricsHook)
}
