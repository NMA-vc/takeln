# Getting Started with takeln

> Typed Rust runtime for durable DAG-based agent workflows.

This guide walks you through building your first workflow with `takeln`, from a simple sequential chain to a parallel DAG with checkpointing, observability, and error handling.

---

## Table of Contents

1. [Installation](#installation)
2. [Your First Graph](#your-first-graph)
3. [Nodes](#nodes)
4. [Edges](#edges)
5. [Checkpointing](#checkpointing)
6. [Parallel DAG Execution](#parallel-dag-execution)
7. [Error Handling & Retries](#error-handling--retries)
8. [Per-Node Policies](#per-node-policies)
9. [Human-in-the-Loop](#human-in-the-loop)
10. [Observability](#observability)
11. [Builder APIs](#builder-apis)

---

## Installation

Add `takeln` to your `Cargo.toml`:

```toml
[dependencies]
takeln = "0.9.1"
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Optional feature flags:

| Flag | Description |
|------|-------------|
| `postgres` | Enables `PostgresCheckpointer` backed by PostgreSQL via `sqlx` |
| `sqlite` | Enables `SqliteCheckpointer` backed by SQLite via `rusqlite` |

---

## Your First Graph

A `Graph` is an orchestrator that routes state through a sequence of `Node`s connected by `Edge`s.

```rust
use async_trait::async_trait;
use takeln::{Graph, Node, NodeContext, NodeOutput, GraphError, InMemoryCheckpointer};

// 1. Define your state
#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
struct MyState {
    value: String,
}

// 2. Define a node
struct GreetNode;

#[async_trait]
impl Node<MyState> for GreetNode {
    async fn call(&self, _ctx: NodeContext, mut state: MyState) -> Result<NodeOutput<MyState>, GraphError> {
        state.value = "Hello, World!".to_string();
        Ok(NodeOutput::bare(state))
    }
}

// 3. Build and run
#[tokio::main]
async fn main() {
    let mut graph = Graph::new();
    graph.add_node("greet", GreetNode);
    graph.add_edge("greet", "__END__");

    let checkpointer = InMemoryCheckpointer::new();
    let result = graph.run("thread_1", MyState::default(), "greet", &checkpointer, None)
        .await.unwrap();

    assert_eq!(result.value, "Hello, World!");
}
```

Key concepts:
- **State** must implement `Clone + Send + Sync + Serialize + DeserializeOwned + 'static`
- **`__END__`** is a special node name that terminates execution
- Every `run` requires a **thread ID** (used for checkpoint isolation) and a **checkpointer**

---

## Nodes

Nodes are the computational units. Each node receives the current state, transforms it, and returns the updated state:

```rust
#[async_trait]
impl Node<MyState> for MyNode {
    async fn call(&self, _ctx: NodeContext, mut state: MyState) -> Result<NodeOutput<MyState>, GraphError> {
        // Do work: API calls, LLM inference, database queries, etc.
        state.value.push_str("_processed");
        Ok(NodeOutput::bare(state))
    }
}
```

### LLM Metadata

For LLM nodes, attach token counts and model info:

```rust
Ok(NodeOutput::with_llm(state, 150, 300, "gpt-4o"))
```

This automatically computes cost estimates and populates `NodeMeta` for observability.

### Closure Nodes (FnNode)

For simple transformations, skip the struct:

```rust
graph.add_simple_fn_node("transform", |mut state: MyState| async move {
    state.value.push_str("_transformed");
    Ok(NodeOutput::bare(state))
});
```

Or with full context access:

```rust
graph.add_fn_node("transform", |ctx: NodeContext, mut state: MyState| async move {
    println!("Attempt {} of node {}", ctx.attempt, ctx.node_name);
    state.value.push_str("_transformed");
    Ok(NodeOutput::bare(state))
});
```

---

## Edges

Three types of edges control the execution flow:

### Unconditional

```rust
graph.add_edge("A", "B");  // A always transitions to B
```

### Conditional

```rust
graph.add_conditional_edge("router", |state: &MyState| {
    if state.value.contains("error") {
        "error_handler".to_string()
    } else {
        "next_step".to_string()
    }
});
```

### Event-driven (ADK-style)

Nodes can return an event signal for routing:

```rust
Ok(NodeOutput::bare(state).with_event("needs_review"))
```

---

## Checkpointing

Every state transition is automatically checkpointed. This enables:
- **Resume from failure**: Pick up exactly where you left off
- **Time travel**: Load any previous checkpoint version
- **Audit trail**: Full execution history

### In-Memory Checkpointer

```rust
let cp = InMemoryCheckpointer::new();
```

### Resume from Checkpoint

```rust
// Resume a previously interrupted graph
let result = graph.resume("thread_1", &checkpointer, None).await?;
```

### Checkpoint Retention

```rust
use takeln::RetentionPolicy;
checkpointer.delete_checkpoints("thread_1".into(), RetentionPolicy::KeepLast(5)).await?;
```

---

## Parallel DAG Execution

For workflows with parallel branches, use a `DAG`:

```rust
use takeln::{DAG, Merge};

// State must implement Merge for parallel results
impl Merge for MyState {
    fn merge(&mut self, other: Self) {
        self.value.push_str(&other.value);
    }
}

// Build a DAG with the builder API
let mut dag = DAG::builder()
    .node("fetch", &[])              // root, no dependencies
    .node("parse", &["fetch"])       // depends on fetch
    .node("score", &["parse"])       // parallel with rank
    .node("rank", &["parse"])        // parallel with score
    .node("merge", &["score", "rank"])
    .build()?;

// Execute with wave-based parallel scheduling
let result = graph.run_dag("thread_1", &mut dag, state, &checkpointer, None, 0).await?;
```

The scheduler automatically groups nodes into waves based on dependency resolution.

---

## Error Handling & Retries

### Node-Level Errors

```rust
// Retryable: the retry policy will attempt recovery
Err(GraphError::Retryable("API timeout".into()))

// Fatal: execution stops immediately
Err(GraphError::Fatal("Invalid configuration".into()))

// Yield: suspend execution for human approval
Err(GraphError::Yield("Awaiting approval".into()))
```

### Retry Policy

```rust
graph.set_retry_policy(RetryPolicy {
    max_attempts: 5,
    base_delay_ms: 1000,
    max_delay_ms: 30_000,
    jitter: true,
});
```

### Budget Enforcement

```rust
graph.set_budget_eur(10.0);  // Abort if cumulative cost exceeds 10€
```

### Wave Failure Policy

```rust
use takeln::WaveFailurePolicy;

// Default: abort on first failure
graph.set_wave_failure_policy(WaveFailurePolicy::FailFast);

// Or: complete all nodes, report partial results
graph.set_wave_failure_policy(WaveFailurePolicy::ContinueOnError);
```

---

## Per-Node Policies

Override graph-level settings for individual nodes:

```rust
use takeln::NodeConfig;

graph.add_node_with_config("expensive_llm", MyLLMNode, NodeConfig {
    retry_policy: Some(RetryPolicy { max_attempts: 5, ..Default::default() }),
    timeout: Some(Duration::from_secs(30)),
    budget_eur: Some(0.50),
});
```

---

## Human-in-the-Loop

Interrupt execution before or after specific nodes:

```rust
graph.add_interrupt_before("approval_step");  // Pause before executing
graph.add_interrupt_after("draft_step");       // Pause after executing

// Run until interrupt
let state = graph.run("thread_1", state, "start", &cp, None).await?;

// ... human reviews and approves ...

// Resume from where we left off
let final_state = graph.resume("thread_1", &cp, None).await?.unwrap();
```

---

## Observability

### Structured Tracing

```rust
use takeln::TracingEmitter;

let graph = Graph::with_emitter(Arc::new(TracingEmitter));
```

### Custom Metrics

```rust
use takeln::{MetricsHook, SpanStatus};

struct MyMetrics;

impl MetricsHook for MyMetrics {
    fn on_node_complete(&self, name: &str, duration_ms: u64, status: SpanStatus) {
        // Push to Prometheus, StatsD, etc.
    }
    fn on_graph_complete(&self, thread_id: &str, cost: f64, duration_ms: u64) { }
    fn on_checkpoint_saved(&self, thread_id: &str, checkpoint_id: &str) { }
}

graph.set_metrics_hook(Arc::new(MyMetrics));
```

### Execution History

```rust
let records = graph.execution_history().await;
for record in &records {
    println!("{} — {}ms, cost: {:?}", record.node_name, record.duration_ms, record.cost_eur);
}
```

### Broadcast Events

```rust
let mut rx = graph.subscribe();
tokio::spawn(async move {
    while let Ok(event) = rx.recv().await {
        println!("{:?}", event);
    }
});
```

---

## Builder APIs

### Graph Builder

```rust
let graph = Graph::builder()
    .node("A", MyNodeA)
    .node("B", MyNodeB)
    .edge("A", "B")
    .edge("B", "__END__")
    .retry_policy(RetryPolicy { max_attempts: 3, ..Default::default() })
    .budget_eur(10.0)
    .build();
```

### DAG Builder

```rust
let dag = DAG::builder()
    .node("fetch", &[])
    .node("parse", &["fetch"])
    .node("score", &["parse"])
    .build()?;
```

The DAG builder validates that all dependencies exist and detects cycles at build time.

---

## Next Steps

- Browse the [examples/](examples/) directory for complete working programs
- Read the [ARCHITECTURE.md](ARCHITECTURE.md) for internal design details
- Read the [API documentation](https://docs.rs/takeln) for full type reference
- Check the [CHANGELOG.md](CHANGELOG.md) for version history
