//! # takeln
//!
//! > Typed Rust runtime for durable DAG-based agent workflows.
//!
//! `takeln` is a lightweight, production-grade execution engine for Directed Acyclic
//! Graphs (DAGs) in Rust. It provides built-in wave-based parallel scheduling,
//! robust state checkpointing, retry policies, budget enforcement, and LLM metadata
//! instrumentation.
//!
//! ## Feature Flags
//!
//! | Flag | Default | Description |
//! |------|---------|-------------|
//! | `postgres` | No | Enables [`PostgresCheckpointer`] backed by PostgreSQL via `sqlx` |
//! | `sqlite` | No | Enables [`SqliteCheckpointer`] backed by SQLite via `rusqlite` |
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use async_trait::async_trait;
//! use takeln::{Graph, Node, NodeContext, NodeOutput, GraphError, InMemoryCheckpointer};
//!
//! #[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
//! struct MyState { value: String }
//!
//! struct AppendNode { suffix: String }
//!
//! #[async_trait]
//! impl Node<MyState> for AppendNode {
//!     async fn call(&self, _ctx: NodeContext, mut state: MyState) -> Result<NodeOutput<MyState>, GraphError> {
//!         state.value.push_str(&self.suffix);
//!         Ok(NodeOutput::bare(state))
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut graph = Graph::new();
//!     graph.add_node("A", AppendNode { suffix: "Hello".to_string() });
//!     graph.add_node("B", AppendNode { suffix: " World".to_string() });
//!     graph.add_edge("A", "B");
//!     graph.add_edge("B", "__END__");
//!
//!     let checkpointer = InMemoryCheckpointer::new();
//!     let state = graph.run("thread_1", MyState::default(), "A", &checkpointer, None).await.unwrap();
//!     assert_eq!(state.value, "Hello World");
//! }
//! ```

pub mod checkpoint;
pub mod checkpoint_meta;
pub mod context;
pub mod dag;
pub mod emitter;
pub mod error;
pub mod graph;
pub mod history;
pub mod merge;
pub mod metrics;
pub mod resource_limits;
pub mod store;

pub use checkpoint::Checkpointer;
pub use checkpoint_meta::{CheckpointMeta, CheckpointStatus, CrashRecoveryPolicy, RetentionPolicy};
pub use context::NodeContext;
pub use dag::{DAGBuilder, DAGNode, NodeStatus, DAG};
pub use emitter::{NoopEmitter, SpanContext, SpanEmitter, SpanStatus, TracingEmitter};
pub use error::{GraphError, TakelnError};
pub use graph::{
    Edge, FnNode, Graph, GraphBuilder, GraphEvent, Node, NodeConfig, NodeMeta, NodeOutput, RetryPolicy, State,
    WaveFailurePolicy,
};
pub use history::ExecutionRecord;
pub use merge::Merge;
pub use metrics::{MetricsHook, NoopMetricsHook};
pub use resource_limits::ResourceLimits;

pub use store::memory::InMemoryCheckpointer;
#[cfg(feature = "postgres")]
pub use store::postgres::PgPool;
#[cfg(feature = "postgres")]
pub use store::postgres::PostgresCheckpointer;
#[cfg(feature = "sqlite")]
pub use store::sqlite::SqliteCheckpointer;
