# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.11.0] - 2026-07-05

### Added
- `claimed_interrupt` field to `CheckpointMeta` for separate lock-claiming semantics
- `ClaimResult` enum variants `Claimed`, `InProgress`, and `AlreadyCompleted` for safe CAS execution
- `payload_ref` field to `YieldRequest` for payload-by-reference handle storage to avoid inline PII
- Failure reversion: `resume_with_input` rolls back checkpoints to `Yielded` with active claims cleared on node execution errors
- New integration tests suite for concurrent resumes and crash-recovery simulation (`tests/hitl_concurrency_tests.rs`)

### Changed
- **Breaking**: `Checkpointer::save_state` signature updated to accept `claimed_interrupt` and `resolved_interrupt`
- `claim_interrupt` now returns `Result<ClaimResult, TakelnError>` and manages atomic updates of `claimed_interrupt`
- Successful resumption writes `resolved_interrupt` to database to prevent stale resumes and duplicates

## [0.10.0] - 2026-07-04

### Added
- Sequential loop support: conditional edges can create cycles in `Graph::run()`
- `max_sequential_steps` resource limit (default: 1,000) with `StepLimitExceeded` error
- Structured HITL: `YieldRequest` and `ResumeMode` types in new `hitl` module
- `Graph::resume_with_input()` for resuming with validated human input
- `NodeContext::resumed_input` field for HITL re-entry mode
- Schema validation (type + enum) for HITL resume input
- `DynamicNode<S>` trait for imperative child node orchestration
- `ChildRunner<S>` handle for invoking registered nodes from dynamic nodes
- `Graph::add_dynamic_fn_node()` and `GraphBuilder::dynamic_fn_node()` registration
- New examples: `loop_until_valid`, `hitl_approval`, `dynamic_orchestration`

### Changed
- **Breaking**: `GraphError::Yield(String)` → `GraphError::Yield(YieldRequest)`. Use `YieldRequest::simple("msg")` for migration.
- `CheckpointMeta` now includes optional `yield_request` field
- `NodeContext::new()` accepts additional `resumed_input` parameter (internal)
- `Graph::run()` now delegates to internal `run_inner()` method

### Migration from v0.9.x

```rust
// Before
Err(GraphError::Yield("message".into()))
// After
Err(GraphError::Yield(YieldRequest::simple("message")))
```

## [0.9.1] - 2026-06-05

### Security
- **Removed `surreal` feature** — `surrealdb 1.x` carries 3 known vulnerabilities (`ring`, `rsa`, `object_store`) with no upstream fix. Will return as a companion crate when SurrealDB v2+ ships clean.
- **Replaced `sqlx` umbrella with `sqlx-core` + `sqlx-postgres`** — eliminates `sqlx-mysql` transitive dependency and its vulnerable `rsa 0.9.10`. Dependency count reduced from 504 to 244.
- **`cargo audit` passes** — zero vulnerabilities, no ignores needed.

### Fixed
- **ResourceLimits now enforced** — all 4 limits are checked at runtime:
  - `max_concurrent_nodes` — semaphore-gated DAG wave dispatch
  - `max_dag_nodes` — checked at `run_dag` entry
  - `max_checkpoint_bytes` — checked before every `save_state` call
  - `max_execution_records` — ring-buffer eviction at cap
- **Stable idempotency key** — `NodeContext.execution_id` is now deterministic (UUID v5 from `{thread_id, node_name, checkpoint_id}`). Use for external deduplication. `attempt_id` remains random UUID v4 for tracing.
- **SQLite async safety** — `SqliteCheckpointer` now uses `spawn_blocking` for all rusqlite operations, preventing async runtime blocking.

### Added
- `NodeContext.execution_id` — stable deterministic UUID v5 idempotency key
- `PgPool` re-export — users no longer need to depend on `sqlx` directly
- `ResourceLimits` builder methods (`with_max_concurrent_nodes`, etc.)
- Security Model documentation in `ARCHITECTURE.md`
- 3 new integration tests: `test_max_dag_nodes_enforced`, `test_max_checkpoint_bytes_enforced`, `test_semaphore_limits_concurrency`
- 2 unit tests for `execution_id` stability

### Changed
- Execution record history uses `VecDeque` instead of `Vec` — O(1) eviction at cap instead of O(n)
- `max_concurrent_nodes` is clamped to minimum 1 to prevent deadlock with zero-permit semaphore

### Removed
- `SurrealCheckpointer` and `surreal` feature flag (audit remediation)
- `sqlx` umbrella dependency — replaced with `sqlx-core` + `sqlx-postgres`

## [0.9.0] - 2026-06-05

### ⚠️ Breaking Changes
- **`Node::call` signature changed**: `call(&self, state: S)` → `call(&self, ctx: NodeContext, state: S)`. All node implementations must add `ctx: NodeContext` (or `_ctx: NodeContext`) as the first parameter.
- **`FnNode` closure signature changed**: `Fn(S) -> Fut` → `Fn(NodeContext, S) -> Fut`. Use `add_simple_fn_node` for closures that don't need context.

### Added
- `NodeContext` — execution context with thread_id, node_name, attempt count, attempt_id (idempotency key), last_checkpoint_id, budget_remaining_eur, cancellation token.
- `ResourceLimits` — configurable bounds for max_concurrent_nodes, max_execution_records, max_checkpoint_bytes, max_dag_nodes.
- `Graph::add_simple_fn_node()` — convenience wrapper for closures that don't need `NodeContext`.
- `GraphBuilder::fn_node()`, `GraphBuilder::simple_fn_node()`, `GraphBuilder::resource_limits()`.
- `benches/large_state.rs` — clone/serialize/checkpoint benchmarks for 1KB–1MB payloads.
- At-least-once execution semantics documentation in `ARCHITECTURE.md`.

## [0.8.0] - 2026-06-05

### Added
- `#[non_exhaustive]` on all 9 public enums for forward-compatible API evolution.
- Property tests (`proptest`): checkpoint fidelity, DAG completion, budget enforcement.
- Load tests: 100-node DAG (10×10 waves), 10k checkpoint throughput, thread isolation.
- `ARCHITECTURE.md` — internal design documentation.
- Comprehensive CI: matrix (stable + MSRV 1.75), all feature combos, security audit, publish dry-run.
- `documentation` field in `Cargo.toml` for docs.rs integration.

### Changed
- `GraphEvent` match arms now require a wildcard (`_ =>`) due to `#[non_exhaustive]`.

## [0.7.0] - 2026-06-05

### Added
- `PostgresCheckpointer` — durable persistence via `sqlx` (feature: `postgres`). Auto-creates table and index.
- `SqliteCheckpointer` — durable local persistence via `rusqlite` with bundled SQLite (feature: `sqlite`). Supports file-based and in-memory modes.
- Criterion benchmarks: `sequential_throughput` (10-node chain) and `checkpoint_overhead` (save/load cycle).
- 7 SQLite integration tests covering save/load, versioning, listing, retention, DAG round-trip, and status round-trip.

## [0.6.0] - 2026-06-05

### Added
- `DAGBuilder` — fluent API for constructing DAGs with string-based dependency references and cycle detection.
- `DAG::builder()` constructor.
- `GraphBuilder` — fluent API for constructing graphs with chained `.node()`, `.edge()`, `.retry_policy()`, `.budget_eur()`, etc.
- `Graph::builder()` constructor.
- `FnNode` — async closure wrapper enabling `graph.add_fn_node("name", |state| async { ... })`.
- 5 new examples: `conditional_routing`, `dag_builder`, `hitl_approval`, `crash_resume`, `llm_call`.
- `GUIDE.md` — comprehensive getting started guide.
- 5 new tests: DAG builder, missing dep, cycle detection, graph builder, FnNode.

## [0.5.0] - 2026-06-05

### Added
- `SpanContext` struct for structured observability data passed to `SpanEmitter::emit()`.
- `TracingEmitter` — emits structured `tracing` spans (INFO/WARN/ERROR by status) with all node context fields.
- `MetricsHook` trait with `on_node_complete`, `on_graph_complete`, `on_checkpoint_saved` callbacks.
- `NoopMetricsHook` default implementation.
- `Graph::set_metrics_hook()` for registering custom metrics collectors.
- `ExecutionRecord` struct for execution replay and auditing.
- `Graph::execution_history()` returns ordered execution records from the graph's lifetime.
- 3 new tests: tracing emitter, metrics hook firing, execution history.

### Changed
- **BREAKING**: `SpanEmitter::emit()` now takes `&SpanContext<'_>` instead of 6 loose parameters.

## [0.4.0] - 2026-06-05

### Added
- `NodeConfig` struct for per-node execution overrides (retry policy, timeout, budget).
- `Graph::add_node_with_config()` to register nodes with per-node configuration.
- `WaveFailurePolicy` enum: `FailFast` (default) and `ContinueOnError`.
- `Graph::set_wave_failure_policy()` for configuring parallel wave error handling.
- `TakelnError::PartialWaveFailure` variant with `succeeded` and `failed` node lists.
- `sequence_number: u64` on all `GraphEvent` variants for deterministic event ordering.
- Per-node timeout via `tokio::time::timeout` wrapping node calls.
- Per-node budget enforcement independent of graph-level budget.
- 4 new tests: per-node retry override, per-node timeout, wave continue-on-error, event sequence numbers.

### Changed
- **BREAKING**: `GraphEvent` variants now include a `sequence_number: u64` field.
- `Graph` internally uses `Arc<AtomicU64>` for thread-safe event sequencing across spawned tasks.
- Retry loops in both `run()` and `run_dag()` now check per-node config before graph-level fallback.

## [0.3.0] - 2026-06-05

### Changed
- **BREAKING**: `Checkpointer::save_state` now requires a `status: CheckpointStatus` parameter.
- **BREAKING**: `Checkpointer::load_state` and `load_version` now return `(S, CheckpointMeta, Option<DAG>)` instead of `(S, String, Option<DAG>)`.
- **BREAKING**: `Checkpointer::list_checkpoints` now returns `Vec<CheckpointMeta>` instead of `Vec<(String, String, DateTime)>`.

### Added
- `CheckpointMeta` struct: versioned checkpoint metadata with `checkpoint_id`, `thread_id`, `next_node`, `graph_version`, `state_schema_version`, `status`, and `created_at`.
- `CheckpointStatus` enum: `Complete`, `Running`, `Yielded`, `Failed`, `Interrupted`.
- `CrashRecoveryPolicy` enum: `ResetToPending` (default), `FailFast`, `SkipAndContinue`.
- `RetentionPolicy` enum: `KeepAll`, `KeepLast(n)`, `OlderThan(duration)`.
- `Checkpointer::delete_checkpoints` trait method for checkpoint compaction.
- `Graph::set_crash_recovery_policy` for configuring crash recovery behavior.
- `resume()` and `resume_dag()` now automatically apply crash recovery policy when checkpoint status is `Running`.
- 13 new edge-case tests in `tests/checkpoint_edge_cases.rs`: retention, status metadata, all 3 crash recovery policies, concurrent saves, nonexistent threads.

## [0.2.0] - 2026-06-05

### Changed
- **BREAKING**: `DAGNode` stripped of tectic-specific fields (`task_intent_id`, `instruction`, `output`, `agent_id`, `max_depth`, `started_at`, `completed_at`, `token_usage`). Now contains only `id`, `step_type`, `depends_on`, `status`.
- **BREAKING**: `DAG` stripped of `task_intent_id` field.
- **BREAKING**: `surreal` feature is no longer enabled by default. Add `features = ["surreal"]` to opt in.
- **BREAKING**: `Merge` trait is now provided by `takeln::Merge` instead of the external `merge` crate.
- Replaced `Result<_, String>` with typed `TakelnError` in all public APIs (completed in 0.1.x patch series, now stabilized).
- Crate description updated to "Typed Rust runtime for durable DAG-based agent workflows".

### Added
- `DAG::new()` constructor and `DAG::add_node()` builder helper.
- Internal `takeln::Merge` trait replacing the external `merge` crate dependency.
- Crate-level rustdoc with quick-start example.
- Rustdoc on all public types, traits, methods, and enum variants.
- `async-trait` rationale documented on `Node` and `Checkpointer` traits.
- MSRV set to Rust 1.75.0.
- CI workflow (GitHub Actions): check, test, clippy, fmt, doc across stable + MSRV.
- `deny.toml` for license audit and advisory DB checks.
- `rustfmt.toml` and `clippy.toml` configuration.
- `[package.metadata.docs.rs]` for docs.rs feature flag configuration.
- Sequential budget enforcement in `Graph::run` (was only enforced in `run_dag`).

### Removed
- `specta` optional dependency (consumers can derive `specta::Type` externally).
- `merge` crate dependency (replaced by internal `takeln::Merge`).
- `futures-util` dependency (unused).
- `tokio` features trimmed from `full` to `rt-multi-thread, macros, sync, time`.
- `tokio-util` `codec` feature (only `CancellationToken` is needed).
- SCRATCHPAD.md section from README (spec not yet published separately).

## [0.1.0] - 2026-06-05

### Added
- Initial extraction of core Orchestrator `Graph<S>`, `Node<S>`, `Edge<S>`, and `Checkpointer<S>` traits from the `tectic` codebase.
- Standard wave-based parallel DAG scheduling algorithm `run_dag` in `Graph<S>`.
- Feature-gated `SurrealCheckpointer` mapping execution states to SurrealDB.
- `InMemoryCheckpointer` implementation for testing and light local execution.
- Documentation for integration with the `SCRATCHPAD.md` standard.
- Working examples for sequential, parallel, and checkpoint-resumed executions.
- Reference test suite.
- Apache 2.0 Licensing and NMA attribution.
