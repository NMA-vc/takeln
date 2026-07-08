# Architecture

> Internal design of the `takeln` execution engine.

---

## Execution Model

`takeln` is a **state-machine graph executor**. The core abstraction is:

```
State → Node → State' → Edge → Next Node → ...
```

Each **Node** is an async function `S → Result<NodeOutput<S>, GraphError>` that transforms state. **Edges** connect nodes — either unconditionally (`A → B`) or conditionally (`A → f(state) → B | C`). Execution terminates when a node transitions to `__END__`.

---

## Graph Structure

```
┌──────────────────────────────────────────────────┐
│                    Graph<S>                       │
│                                                  │
│  ┌─────────────┐   ┌─────────────┐               │
│  │  nodes:      │   │  edges:      │              │
│  │  HashMap<    │   │  HashMap<    │              │
│  │    String,   │   │    String,   │              │
│  │    Arc<Node> │   │    Edge      │              │
│  │  >           │   │  >           │              │
│  └─────────────┘   └─────────────┘               │
│                                                  │
│  ┌─────────────┐   ┌─────────────┐               │
│  │  emitter     │   │  metrics    │               │
│  │  SpanEmitter │   │  MetricsHook│               │
│  └─────────────┘   └─────────────┘               │
│                                                  │
│  retry_policy, budget_eur, node_configs,          │
│  interrupt_before, interrupt_after,               │
│  crash_recovery_policy, wave_failure_policy       │
└──────────────────────────────────────────────────┘
```

---

## Sequential Execution (`Graph::run`)

```
1. Load checkpoint (if resuming)
2. Loop:
   a. Check interrupt_before → save checkpoint, return
   b. Execute node with retry policy
   c. Accumulate cost, check budget
   d. Save checkpoint
   e. Emit span (SpanEmitter)
   f. Fire metrics hook
   g. Record ExecutionRecord
   h. Check interrupt_after → save checkpoint, return
   i. Resolve edge → next node name
   j. If "__END__" → return state
   k. Increment step counter → check max_sequential_steps limit
```

Conditional edges may point back to earlier nodes, creating loops. The `max_sequential_steps` limit (default: 1,000) prevents infinite loops by raising `StepLimitExceeded` when the counter is exhausted.

---

## Wave-Parallel DAG Execution (`Graph::run_dag`)

The DAG scheduler uses **wave-based topological scheduling**:

```
Wave 0: [fetch]                    ← nodes with no dependencies
Wave 1: [parse]                    ← depends on fetch
Wave 2: [score, rank]              ← both depend on parse (parallel)
Wave 3: [merge]                    ← depends on score AND rank
```

### Algorithm

```
1. Load checkpoint (if resuming)
2. Restore node statuses from checkpointed DAG
3. Loop:
   a. Collect ready nodes: status=Pending AND all deps=Done
   b. If none ready AND pending exist → DAGDeadlock error
   c. If none ready AND none pending → complete
   d. Spawn all ready nodes in JoinSet (parallel)
   e. Collect results, merge states deterministically (by DAG index)
   f. Save checkpoint with DAG snapshot
   g. Apply WaveFailurePolicy (FailFast vs ContinueOnError)
   h. Repeat
```

### Merge Determinism

Parallel node results are merged in **DAG index order** (the order nodes appear in the `dag.nodes` vector), not arrival order. This ensures deterministic state regardless of execution timing.

---

## Dynamic Node Execution

Dynamic nodes implement the `DynamicNode<S>` trait and receive a `ChildRunner` handle that can invoke registered child nodes imperatively. Unlike edges, which define static transitions between nodes, dynamic nodes decide at runtime which child nodes to call and in what order.

Dynamic node execution is **atomic** — no per-child checkpoints are saved. The entire dynamic node re-runs on crash recovery. Register dynamic nodes via `Graph::add_dynamic_fn_node()` or `GraphBuilder::dynamic_fn_node()`.

---

## Checkpoint Persistence Model

```
┌──────────────────────────────────┐
│         CheckpointMeta           │
│                                  │
│  checkpoint_id: UUID             │
│  thread_id: String               │
│  next_node: String               │
│  status: CheckpointStatus        │
│  graph_version: Option<String>   │
│  state_schema_version: Option    │
│  yield_request: Option<YieldRequest> │
│  created_at: DateTime<Utc>       │
└──────────────────────────────────┘

CheckpointStatus:
  Complete    ← normal save after node execution
  Running     ← saved before node execution (crash recovery)
  Yielded     ← suspended for human-in-the-loop
  Failed      ← node failed
  Interrupted ← interrupted before/after hook
```

### Backends

| Backend | Feature | Storage | Use Case |
|---------|---------|---------|----------|
| `InMemoryCheckpointer` | default | `HashMap` | Testing, ephemeral |
| `SqliteCheckpointer` | `sqlite` | File-based SQLite | Single-process durability |
| `PostgresCheckpointer` | `postgres` | PostgreSQL JSONB | Multi-process production |

### Crash Recovery

When loading a checkpoint with `Running` status, the `CrashRecoveryPolicy` determines behavior:

| Policy | Behavior |
|--------|----------|
| `ResetToPending` | Re-execute the running node |
| `SkipAndContinue` | Skip to the next node |
| `FailFast` | Return an error immediately |

---

## Error Hierarchy

```
GraphError (node-level)          TakelnError (runner-level)
├── Retryable(String)            ├── NodeNotFound(String)
├── Fatal(String)                ├── CheckpointError(String)
├── Yield(YieldRequest)          ├── BudgetExceeded { .. }
└── BudgetExceeded { .. }        ├── DAGDeadlock(String)
                                 ├── JoinError(String)
                                 ├── ExecutionError(String)
                                 ├── SerializationError(String)
                                 ├── DeserializationError(String)
                                 ├── RecursionLimitExceeded { .. }
                                 ├── StepLimitExceeded { .. }
                                 └── PartialWaveFailure { .. }
```

- **`GraphError::Retryable`** → triggers retry policy (exponential backoff + jitter)
- **`GraphError::Fatal`** → immediate abort, no retry
- **`GraphError::Yield`** → save checkpoint with `Yielded` status, return control to caller. Contains a `YieldRequest` with `interrupt_id`, `message`, optional JSON schema, and `ResumeMode` (ReEntry or Handoff). Resume with `graph.resume_with_input()` which validates the response against the schema.
- **`TakelnError::StepLimitExceeded`** → sequential loop exceeded `max_sequential_steps`
- **`TakelnError::PartialWaveFailure`** → only with `WaveFailurePolicy::ContinueOnError`

---

## Observability Stack

```
Node Execution
    │
    ├── SpanEmitter::emit(SpanContext)     ← structured event
    │     └── TracingEmitter               ← tracing crate integration
    │
    ├── MetricsHook::on_node_complete()    ← metrics callback
    │
    ├── ExecutionRecord → history vec      ← audit trail
    │
    └── GraphEvent → broadcast channel     ← event streaming
```

---

## Retry Policy

```
attempt 0: immediate
attempt 1: base_delay_ms * 2^0 ± jitter
attempt 2: base_delay_ms * 2^1 ± jitter
attempt N: min(base_delay_ms * 2^N, max_delay_ms) ± jitter
```

Per-node overrides via `NodeConfig` take precedence over the graph-level policy.

---

## Execution Guarantees

`takeln` provides **at-least-once** execution semantics:

- A node may execute more than once if a crash occurs after execution but before the checkpoint is saved.
- Use `ctx.execution_id` as an **idempotency key** for external side effects (API calls, database writes, payments). It is deterministic (UUID v5) and stable across retries and crash/resume.
- `ctx.attempt_id` is a random UUID per attempt — use it for logging and tracing, **not** for idempotency.
- `ctx.attempt` tracks retry attempts (0 = first try).
- `ctx.last_checkpoint_id` lets nodes detect resume-after-crash scenarios.

**Important**: Nodes with external side effects (sending emails, charging payments, calling APIs) should always use `execution_id` to guard against duplicate execution.

---

## NodeContext

Every `Node::call` receives a `NodeContext` as its first argument:

```
NodeContext {
    thread_id: String,           // session/thread identifier
    node_name: String,           // name of the executing node
    attempt: u8,                 // retry attempt (0 = first try)
    execution_id: String,        // deterministic UUID v5 (stable idempotency key)
    attempt_id: String,          // random UUID v4 per attempt (logging/tracing)
    last_checkpoint_id: Option,  // most recent checkpoint ID
    budget_remaining_eur: Option, // remaining budget (sequential only)
    cancellation: Option,        // cancellation token
    resumed_input: Option<Value>, // HITL re-entry input (set after resume_with_input)
}
```

Nodes that don't need context can ignore it with `_ctx: NodeContext`.

---

## Resource Limits

```
ResourceLimits {
    max_concurrent_nodes: 64,       // DAG wave parallelism cap (semaphore-gated)
    max_execution_records: 10_000,  // in-memory audit trail cap (ring buffer)
    max_checkpoint_bytes: 10 MB,    // max serialized state size (checked before save)
    max_dag_nodes: 10_000,          // max nodes in a DAG (checked at run_dag entry)
    max_sequential_steps: 1_000,    // max loop iterations in sequential run (prevents infinite loops)
}
```

All limits have generous defaults and are **enforced at runtime**. Override via `Graph::set_resource_limits()` or `GraphBuilder::resource_limits()`.

---

## Security Model

### Checkpoint Data

Checkpoint payloads (state, DAG snapshots) are stored as **plaintext JSON**. There is no built-in encryption, signing, or redaction.

**Implications:**
- Do not store secrets (API keys, tokens, PII) directly in graph state without application-level encryption.
- Checkpoint integrity is not verified on load — a corrupted or tampered checkpoint will be deserialized as-is.
- For production deployments requiring encryption or signing, implement a wrapper around the `Checkpointer` trait or use `takeln-tectic` (planned) for signed/encrypted checkpoints.

### State Visibility

By default, full state is visible in:
- Checkpoint storage (all backends)
- Execution history records (in-memory)
- SpanEmitter events (if configured)
- GraphEvent broadcasts

Design your `State` type with this in mind. Consider implementing custom `Serialize` that redacts sensitive fields.

### Idempotency

`NodeContext` provides two IDs for different purposes:

| Field | Stability | Use Case |
|-------|-----------|----------|
| `execution_id` | **Stable** across retries — deterministic UUID v5 from `{thread_id, node_name, checkpoint_id}` | External idempotency keys (API calls, payments, DB writes) |
| `attempt_id` | **Unique** per attempt — random UUID v4 | Logging, tracing, debugging |

---

## Semver Policy

- All public enums are `#[non_exhaustive]` — new variants are non-breaking
- All public structs with `#[non_exhaustive]` (e.g., `NodeContext`, `ResourceLimits`) — new fields are non-breaking
- The `Checkpointer` trait is sealed by convention (adding methods is a minor bump)
- Feature flags are additive — enabling a new feature never breaks existing code
