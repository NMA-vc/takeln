//! `CheckpointProof` / `save_state_bound` / `current_head`: the optimistic
//! concurrency boundary that the executor writes every durable snapshot through.
//!
//! A store without a head keeps working (the default `save_state_bound`
//! forwards to `save_state`); a store with one sees the head the executor
//! observed, advances it on every checkpoint, and can refuse a stale write.

use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use takeln::checkpoint::ClaimResult;
use takeln::{
    CheckpointMeta, CheckpointProof, CheckpointSave, CheckpointStatus, Checkpointer, Graph, GraphError,
    InMemoryCheckpointer, Node, NodeContext, NodeOutput, ResumeContext, RetentionPolicy, TakelnError, YieldRequest,
    DAG,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default, PartialEq)]
struct TestState {
    value: String,
}

impl takeln::Merge for TestState {
    fn merge(&mut self, other: Self) {
        self.value.push_str(&other.value);
    }
}

struct Append(&'static str);

#[async_trait]
impl Node<TestState> for Append {
    async fn call(&self, _ctx: NodeContext, mut state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        state.value.push_str(self.0);
        Ok(NodeOutput::bare(state))
    }
}

struct YieldOnce {
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl Node<TestState> for YieldOnce {
    async fn call(&self, _ctx: NodeContext, state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            return Err(GraphError::Yield(YieldRequest::new("gate", "approve?")));
        }
        Err(GraphError::Fatal(format!("failed after resume with {}", state.value)))
    }
}

/// One observed `save_state_bound` call.
#[derive(Debug, Clone)]
struct BoundCall {
    next_node: String,
    status: CheckpointStatus,
    yield_request: Option<YieldRequest>,
    resolved_interrupt: Option<String>,
    proof: CheckpointProof,
}

/// A store with a head: `current_head` is the id of the last write, and a write
/// whose `expected_head` is not the current head is a `Conflict`.
struct HeadStore {
    inner: InMemoryCheckpointer<TestState>,
    head: Mutex<Option<String>>,
    calls: Mutex<Vec<BoundCall>>,
    /// When set, every bound write reports this conflict instead of saving.
    force_conflict: Mutex<Option<Option<String>>>,
}

impl HeadStore {
    fn new() -> Self {
        Self {
            inner: InMemoryCheckpointer::new(),
            head: Mutex::new(None),
            calls: Mutex::new(Vec::new()),
            force_conflict: Mutex::new(None),
        }
    }
    fn calls(&self) -> Vec<BoundCall> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl Checkpointer<TestState> for HeadStore {
    async fn current_head(&self, _thread_id: String) -> Result<Option<String>, TakelnError> {
        Ok(self.head.lock().unwrap().clone())
    }

    #[allow(clippy::too_many_arguments)]
    async fn save_state_bound(
        &self,
        thread_id: String,
        state: TestState,
        next_node: String,
        dag: Option<&DAG>,
        status: CheckpointStatus,
        yield_request: Option<YieldRequest>,
        claimed_interrupt: Option<String>,
        resolved_interrupt: Option<String>,
        proof: CheckpointProof,
    ) -> Result<CheckpointSave, TakelnError> {
        self.calls.lock().unwrap().push(BoundCall {
            next_node: next_node.clone(),
            status: status.clone(),
            yield_request: yield_request.clone(),
            resolved_interrupt: resolved_interrupt.clone(),
            proof: proof.clone(),
        });
        if let Some(actual_head) = self.force_conflict.lock().unwrap().clone() {
            return Ok(CheckpointSave::Conflict { actual_head });
        }
        let actual = self.head.lock().unwrap().clone();
        if proof.expected_head != actual {
            return Ok(CheckpointSave::Conflict { actual_head: actual });
        }
        let id = self
            .inner
            .save_state(
                thread_id,
                state,
                next_node,
                dag,
                status,
                yield_request,
                claimed_interrupt,
                resolved_interrupt,
            )
            .await?;
        *self.head.lock().unwrap() = Some(id.clone());
        Ok(if actual.is_none() {
            CheckpointSave::Created { checkpoint_id: id }
        } else {
            CheckpointSave::Advanced { checkpoint_id: id }
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn save_state(
        &self,
        thread_id: String,
        state: TestState,
        next_node: String,
        dag: Option<&DAG>,
        status: CheckpointStatus,
        yield_request: Option<YieldRequest>,
        claimed_interrupt: Option<String>,
        resolved_interrupt: Option<String>,
    ) -> Result<String, TakelnError> {
        self.inner
            .save_state(
                thread_id,
                state,
                next_node,
                dag,
                status,
                yield_request,
                claimed_interrupt,
                resolved_interrupt,
            )
            .await
    }

    async fn load_state(
        &self,
        thread_id: String,
    ) -> Result<Option<(TestState, CheckpointMeta, Option<DAG>)>, TakelnError> {
        self.inner.load_state(thread_id).await
    }

    async fn load_version(
        &self,
        thread_id: String,
        checkpoint_id: String,
    ) -> Result<Option<(TestState, CheckpointMeta, Option<DAG>)>, TakelnError> {
        self.inner.load_version(thread_id, checkpoint_id).await
    }

    async fn list_checkpoints(&self, thread_id: String) -> Result<Vec<CheckpointMeta>, TakelnError> {
        self.inner.list_checkpoints(thread_id).await
    }

    async fn delete_checkpoints(&self, thread_id: String, policy: RetentionPolicy) -> Result<usize, TakelnError> {
        self.inner.delete_checkpoints(thread_id, policy).await
    }

    async fn claim_interrupt(&self, thread_id: &str, interrupt_id: &str) -> Result<ClaimResult, TakelnError> {
        self.inner.claim_interrupt(thread_id, interrupt_id).await
    }
}

fn chain_graph() -> Graph<TestState> {
    let mut graph = Graph::new();
    graph.add_node("A", Append("a"));
    graph.add_node("B", Append("b"));
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");
    graph
}

#[test]
fn checkpoint_proof_default_is_the_genesis_state() {
    // Before the first connector receipt the chain state is exactly (0, None):
    // no synthetic receipt hash is permitted.
    let proof = CheckpointProof::default();
    assert_eq!(proof.authorization_hash, None);
    assert_eq!(proof.receipt_chain_count, 0);
    assert_eq!(proof.receipt_chain_head, None);
    assert_eq!(proof.expected_head, None);
}

#[test]
fn checkpoint_save_exposes_the_id_unless_it_is_a_conflict() {
    for save in [
        CheckpointSave::Created {
            checkpoint_id: "c1".into(),
        },
        CheckpointSave::Advanced {
            checkpoint_id: "c1".into(),
        },
        CheckpointSave::IdempotentReplay {
            checkpoint_id: "c1".into(),
        },
    ] {
        assert_eq!(save.checkpoint_id(), Some("c1"));
    }
    assert_eq!(CheckpointSave::Conflict { actual_head: None }.checkpoint_id(), None);
}

#[tokio::test]
async fn a_store_without_a_head_keeps_working_through_the_default_bound_save() {
    let cp = InMemoryCheckpointer::<TestState>::new();
    assert_eq!(cp.current_head("t".into()).await.unwrap(), None);

    let save = cp
        .save_state_bound(
            "t".into(),
            TestState { value: "x".into() },
            "A".into(),
            None,
            CheckpointStatus::Complete,
            None,
            None,
            None,
            CheckpointProof::default(),
        )
        .await
        .unwrap();
    let id = save.checkpoint_id().expect("legacy stores always create").to_string();
    assert!(matches!(save, CheckpointSave::Created { .. }));

    let (state, meta, _) = cp.load_state("t".into()).await.unwrap().unwrap();
    assert_eq!(state.value, "x");
    assert_eq!(meta.checkpoint_id, id);

    // And the executor runs to completion on such a store.
    let state = chain_graph()
        .run("t2", TestState::default(), "A", &cp, None)
        .await
        .unwrap();
    assert_eq!(state.value, "ab");
}

#[tokio::test]
async fn the_executor_threads_the_head_through_every_sequential_checkpoint() {
    let store = HeadStore::new();
    let state = chain_graph()
        .run("t", TestState::default(), "A", &store, None)
        .await
        .unwrap();
    assert_eq!(state.value, "ab");

    let calls = store.calls();
    assert_eq!(calls.len(), 2, "one durable write per node: {calls:?}");
    // First write: the genesis head. Second write: the id the first write returned.
    assert_eq!(calls[0].proof.expected_head, None);
    let all = store.list_checkpoints("t".into()).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        calls[1].proof.expected_head.as_deref(),
        Some(all[0].checkpoint_id.as_str())
    );
    for call in &calls {
        assert_eq!(call.status, CheckpointStatus::Complete);
        assert_eq!(call.proof.receipt_chain_count, 0);
        assert_eq!(call.proof.receipt_chain_head, None);
    }
}

#[tokio::test]
async fn a_run_starts_from_the_head_the_store_reports() {
    let store = HeadStore::new();
    let graph = chain_graph();
    graph.run("t", TestState::default(), "A", &store, None).await.unwrap();
    let head_after_first_run = store.current_head("t".into()).await.unwrap();
    assert!(head_after_first_run.is_some());

    // A second run on the same store must read that head, not assume genesis.
    graph.run("t", TestState::default(), "A", &store, None).await.unwrap();
    let calls = store.calls();
    assert_eq!(calls[2].proof.expected_head, head_after_first_run);
}

#[tokio::test]
async fn a_head_conflict_stops_the_run_instead_of_being_ignored() {
    let store = HeadStore::new();
    *store.force_conflict.lock().unwrap() = Some(Some("somebody-else".to_string()));

    let err = chain_graph()
        .run("t", TestState::default(), "A", &store, None)
        .await
        .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("head conflict"), "{message}");
    assert!(message.contains("somebody-else"), "{message}");
    assert_eq!(store.calls().len(), 1, "no node runs past a refused write");
}

#[tokio::test]
async fn a_yield_reaches_the_bound_save_as_upstreams_yield_request() {
    let store = HeadStore::new();
    let mut graph = Graph::new();
    graph.add_node(
        "gate",
        YieldOnce {
            calls: Default::default(),
        },
    );
    graph.add_edge("gate", "__END__");

    graph
        .run("t", TestState::default(), "gate", &store, None)
        .await
        .unwrap();

    let calls = store.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, CheckpointStatus::Yielded);
    let request = calls[0].yield_request.as_ref().expect("the yield is persisted");
    assert_eq!(request.interrupt_id, "gate");
    assert_eq!(request.message, "approve?");
}

#[tokio::test]
async fn a_dag_yield_reaches_the_bound_save_with_its_request() {
    let store = HeadStore::new();
    let mut graph = Graph::new();
    graph.add_node(
        "gate",
        YieldOnce {
            calls: Default::default(),
        },
    );
    let mut dag = DAG::builder().node("gate", &[]).build().unwrap();
    let _ = graph
        .run_dag("t", &mut dag, TestState::default(), &store, None, 0)
        .await;

    let calls = store.calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(calls[0].status, CheckpointStatus::Yielded);
    assert_eq!(calls[0].yield_request.as_ref().unwrap().message, "approve?");
}

#[tokio::test]
async fn a_failed_resume_is_rolled_back_through_the_bound_save_on_the_current_head() {
    let store = HeadStore::new();
    let mut graph = Graph::new();
    graph.add_node(
        "gate",
        YieldOnce {
            calls: Default::default(),
        },
    );
    graph.add_edge("gate", "__END__");

    graph
        .run("t", TestState::default(), "gate", &store, None)
        .await
        .unwrap();
    let head_after_yield = store.current_head("t".into()).await.unwrap();

    let err = graph
        .resume_with_input(
            "t",
            "gate",
            serde_json::json!("yes"),
            ResumeContext::new("alice"),
            &store,
            None,
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed after resume"), "{err}");

    let calls = store.calls();
    let rollback = calls.last().unwrap();
    assert_eq!(rollback.status, CheckpointStatus::Yielded);
    assert_eq!(rollback.next_node, "gate");
    assert_eq!(rollback.yield_request.as_ref().unwrap().interrupt_id, "gate");
    assert_eq!(
        rollback.proof.expected_head, head_after_yield,
        "the rollback extends the chain at the head it found"
    );
    let (_, meta, _) = store.load_state("t".into()).await.unwrap().unwrap();
    assert_eq!(meta.status, CheckpointStatus::Yielded);
}

#[tokio::test]
async fn a_successful_resume_writes_the_resolved_interrupt_through_the_bound_save() {
    struct Approve;
    #[async_trait]
    impl Node<TestState> for Approve {
        async fn call(&self, ctx: NodeContext, mut state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
            if ctx.resumed_input.is_none() {
                return Err(GraphError::Yield(YieldRequest::new("gate", "approve?")));
            }
            state.value.push_str("approved");
            Ok(NodeOutput::bare(state))
        }
    }

    let store = HeadStore::new();
    let mut graph = Graph::new();
    graph.add_node("gate", Approve);
    graph.add_edge("gate", "__END__");

    graph
        .run("t", TestState::default(), "gate", &store, None)
        .await
        .unwrap();
    let state = graph
        .resume_with_input(
            "t",
            "gate",
            serde_json::json!(true),
            ResumeContext::new("alice"),
            &store,
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.value, "approved");

    let calls = store.calls();
    let last = calls.last().unwrap();
    assert_eq!(last.status, CheckpointStatus::Complete);
    assert_eq!(last.resolved_interrupt.as_deref(), Some("gate"));
    assert!(last.proof.expected_head.is_some());
}

#[tokio::test]
async fn a_shared_node_can_be_registered_behind_an_arc() {
    let shared = Arc::new(Append("s"));
    let mut graph = Graph::new();
    graph.add_node("A", shared.clone());
    graph.add_node("B", shared);
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");
    let state = graph
        .run("t", TestState::default(), "A", &InMemoryCheckpointer::new(), None)
        .await
        .unwrap();
    assert_eq!(state.value, "ss");
}
