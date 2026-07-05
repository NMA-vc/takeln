# takeln

[![CI](https://github.com/NMA-vc/takeln/actions/workflows/ci.yml/badge.svg)](https://github.com/NMA-vc/takeln/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/takeln.svg)](https://crates.io/crates/takeln)
[![docs.rs](https://img.shields.io/docsrs/takeln)](https://docs.rs/takeln)
[![License](https://img.shields.io/crates/l/takeln.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.75.0-blue.svg)](https://blog.rust-lang.org/2023/12/28/Rust-1.75.0.html)

> Typed Rust runtime for durable DAG-based agent workflows.

`takeln` (Plattdeutsch for rigging/tackle) is a lightweight, production-grade execution engine for Directed Acyclic Graphs (DAGs) in Rust. Designed by NMA (New Model Agents), it provides built-in wave-based parallel scheduling, robust state checkpointing, retry policies, budget enforcement, and LLM metadata instrumentation.

---

## Why takeln?

Agentic workflows require structured execution environments where state can be:
1. **Parallelised safely**: Tasks that do not depend on each other should execute concurrently.
2. **Checkpoint-resumable**: If a long-running LLM call fails, you should resume execution exactly from the last wave rather than re-running the entire flow.
3. **Audit-trailed**: Observability and token tracking must be first-class citizens.

`takeln` solves these challenges with a clean, feature-gated library API, allowing you to run agent tasks with minimal boilerplate.

---

## Installation

Add `takeln` to your `Cargo.toml`:
```toml
[dependencies]
takeln = "0.11.0"
```

With optional features:
```toml
[dependencies]
takeln = { version = "0.11.0", features = ["sqlite"] }
```

---

## Quick Start

Here is a simple 20-line example demonstrating sequential execution:

```rust
use async_trait::async_trait;
use takeln::{Graph, Node, NodeContext, NodeOutput, GraphError, InMemoryCheckpointer};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
struct MyState { value: String }

struct AppendNode { suffix: String }

#[async_trait]
impl Node<MyState> for AppendNode {
    async fn call(&self, _ctx: NodeContext, mut state: MyState) -> Result<NodeOutput<MyState>, GraphError> {
        state.value.push_str(&self.suffix);
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    let mut graph = Graph::new();
    graph.add_node("A", AppendNode { suffix: "A".to_string() });
    graph.add_node("B", AppendNode { suffix: "B".to_string() });
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");

    let checkpointer = InMemoryCheckpointer::new();
    let state = graph.run("thread_1", MyState::default(), "A", &checkpointer, None).await.unwrap();
    println!("Final State Value: {}", state.value); // Prints "AB"
}
```

---

## Core Concepts

### 1. Nodes
A `Node` represents a single computational task or agent invocation. It implements the `Node<S>` trait, taking a generic `State` type and returning a modified state wrapped with execution metadata.

### 2. Edges & Transitions
Edges represent transitions between nodes. They can be:
- **Unconditional**: A static transition from Node A to Node B.
- **Conditional**: A transition determined by a function matching on the current graph state.
- **Event-driven**: ADK-style routing where the node itself returns a specific transition event.

### 3. Checkpointer
A `Checkpointer` saves and restores execution states with rich metadata. Each checkpoint records its `CheckpointStatus` (`Complete`, `Running`, `Yielded`, `Failed`, `Interrupted`) and returns a `CheckpointMeta` on load. In-memory, SQLite, and PostgreSQL checkpointers are provided out-of-the-box.

### 4. First-Class Resumption
Resuming a failed or yielding workflow is extremely ergonomic. Instead of manually parsing the loaded checkpoint, you can call `resume` (for sequential flows) or `resume_dag` (for parallel DAG execution). These APIs automatically align the node statuses, apply crash recovery policies, and run the remaining workflow to completion:
```rust
// Resume sequential run automatically
let final_state = graph.resume("thread_1", &checkpointer, None).await.unwrap();

// Resume wave-based parallel DAG execution
let final_state = graph.resume_dag("thread_1", &mut dag, &checkpointer, None, 10).await.unwrap();
```

### 5. Crash Recovery
When a process crashes mid-execution, the last checkpoint may have `Running` status. `CrashRecoveryPolicy` controls what happens on resume:
- **`ResetToPending`** (default): Re-execute the interrupted node.
- **`FailFast`**: Return an error immediately.
- **`SkipAndContinue`**: Skip the interrupted node and continue.

### 6. Per-Node Execution Policies
Override graph-level settings on individual nodes using `NodeConfig`:
```rust
graph.add_node_with_config("expensive_llm", my_node, NodeConfig {
    retry_policy: Some(RetryPolicy { max_attempts: 5, ..Default::default() }),
    timeout: Some(Duration::from_secs(30)),
    budget_eur: Some(0.50),
});
```

### 7. Wave Failure Modes
Control how parallel DAG waves handle failures:
- **`FailFast`** (default): Abort the entire graph on the first node failure.
- **`ContinueOnError`**: Complete all remaining nodes, then return `TakelnError::PartialWaveFailure` with both succeeded and failed node lists.

### 8. Observability
Built-in support for structured tracing and custom metrics:

- **`TracingEmitter`** — plug-and-play emitter that logs structured spans via the `tracing` crate.
- **`MetricsHook`** — trait for integrating with Prometheus, StatsD, OpenTelemetry Metrics, etc. Callbacks fire on node completion, graph completion, and checkpoint saves.
- **`ExecutionRecord`** — timestamped execution history accessible via `graph.execution_history()` for replay and auditing.

```rust
// Use TracingEmitter for structured logging
let graph = Graph::<MyState>::with_emitter(Arc::new(TracingEmitter));

// Or add a custom metrics hook
graph.set_metrics_hook(my_prometheus_hook);

// After execution, inspect history
let records = graph.execution_history().await;
```

### 9. Typed Errors
`takeln` enforces typed error handling. Node executions return a `GraphError` representing workflow signals (e.g. `Yield`, `Retryable`, `Fatal`), while the runner returns a `TakelnError` (e.g., `NodeNotFound`, `BudgetExceeded`, `CheckpointError`, `PartialWaveFailure`, `StepLimitExceeded`), making it easy to programmatically decide whether to retry or escalate.

### 10. Sequential Loops
Conditional edges can create cycles in `Graph::run()`, enabling retry loops and iterative refinement patterns. The `max_sequential_steps` resource limit (default: 1,000) prevents infinite loops.

### 11. Structured Human-in-the-Loop
Beyond simple interrupt-before/after, nodes can yield with a `YieldRequest` containing a message, JSON schema, and `ResumeMode`. Resume with `graph.resume_with_input()` which validates input against the schema before continuing.

### 12. Dynamic Nodes
`DynamicNode<S>` and `ChildRunner<S>` enable imperative child node orchestration — dynamically invoke registered nodes at runtime rather than declaring static edges. Dynamic execution is atomic for checkpointing.

---

## Custom Checkpointers

Implementing a custom checkpointer allows you to bind state saving to external APIs, databases, or filesystem files. The `Checkpointer` trait is defined as follows:

```rust
#[async_trait]
pub trait Checkpointer<S: State>: Send + Sync {
    async fn save_state(
        &self,
        thread_id: String,
        state: S,
        next_node: String,
        dag: Option<&DAG>,
        status: CheckpointStatus,
    ) -> Result<String, TakelnError>;

    async fn load_state(&self, thread_id: String) -> Result<Option<(S, CheckpointMeta, Option<DAG>)>, TakelnError>;

    async fn load_version(&self, thread_id: String, checkpoint_id: String) -> Result<Option<(S, CheckpointMeta, Option<DAG>)>, TakelnError>;

    async fn list_checkpoints(&self, thread_id: String) -> Result<Vec<CheckpointMeta>, TakelnError>;

    async fn delete_checkpoints(&self, thread_id: String, policy: RetentionPolicy) -> Result<usize, TakelnError>;
}
```

---

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `postgres` | No | Enables `PostgresCheckpointer` backed by PostgreSQL via `sqlx` |
| `sqlite` | No | Enables `SqliteCheckpointer` backed by SQLite via `rusqlite` (bundled) |

---

## Documentation

- **[GUIDE.md](GUIDE.md)** — Comprehensive getting started guide
- **[API Docs](https://docs.rs/takeln)** — Full type reference
- **[examples/](examples/)** — 11 working examples covering sequential chains, parallel DAGs, conditional routing, human-in-the-loop, crash recovery, LLM metadata, builder APIs, loops, structured HITL, and dynamic nodes

---

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

---

## License

Copyright 2026 **NMA Venture Capital GmbH**, Hamburg.
Licensed under the [Apache License, Version 2.0](LICENSE).
