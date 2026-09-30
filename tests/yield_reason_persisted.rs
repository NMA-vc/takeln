//! The yield reason is persisted with the checkpoint and can be read back from
//! storage.
//!
//! The Tectic fork kept the reason in a `CheckpointStatus::Yielded(Option<String>)`
//! payload plus a `CheckpointMeta::reason` field. Upstream persists the whole
//! [`YieldRequest`] in `CheckpointMeta::yield_request`, so the reason is
//! `yield_request.message` and there is one yield API, not two. These tests pin
//! the behaviour, read back from storage, on that API:
//!
//! 1. a node yields with "test_reason": the checkpoint is `Yielded` and
//!    `meta.yield_request.message == "test_reason"`, through `list_checkpoints`,
//!    `load_state` and `load_version`;
//! 2. a node completes normally: the checkpoint is `Complete` and carries no
//!    yield request;
//! 3. the same holds for a yield raised inside a parallel DAG wave;
//! 4. with the `sqlite` feature, the reason survives a round trip through the
//!    durable store, not only the in-memory one.

use async_trait::async_trait;
use takeln::{
    CheckpointStatus, Checkpointer, Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput,
    YieldRequest,
};

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct SimpleState {
    value: u32,
}

impl takeln::Merge for SimpleState {
    fn merge(&mut self, other: Self) {
        self.value += other.value;
    }
}

struct YieldingNode;

#[async_trait]
impl Node<SimpleState> for YieldingNode {
    async fn call(&self, _ctx: NodeContext, _state: SimpleState) -> Result<NodeOutput<SimpleState>, GraphError> {
        Err(GraphError::Yield(YieldRequest::simple("test_reason")))
    }
}

struct PassthroughNode;

#[async_trait]
impl Node<SimpleState> for PassthroughNode {
    async fn call(&self, _ctx: NodeContext, state: SimpleState) -> Result<NodeOutput<SimpleState>, GraphError> {
        Ok(NodeOutput::bare(state))
    }
}

/// Assert the yielded checkpoint of `thread` carries `reason`, read three ways.
async fn assert_yield_reason_persisted(cp: &impl Checkpointer<SimpleState>, thread: &str, reason: &str) {
    let checkpoints = cp.list_checkpoints(thread.into()).await.unwrap();
    let last = checkpoints.last().expect("must have at least one checkpoint");
    assert_eq!(last.status, CheckpointStatus::Yielded, "checkpoint must be Yielded");
    assert_eq!(
        last.yield_request.as_ref().map(|r| r.message.as_str()),
        Some(reason),
        "list_checkpoints must return the persisted reason"
    );

    let (_, meta, _) = cp
        .load_state(thread.into())
        .await
        .unwrap()
        .expect("checkpoint must exist");
    assert_eq!(
        meta.yield_request.as_ref().map(|r| r.message.as_str()),
        Some(reason),
        "load_state must also return the persisted reason"
    );

    let (_, meta_v, _) = cp
        .load_version(thread.into(), last.checkpoint_id.clone())
        .await
        .unwrap()
        .expect("version must exist");
    assert_eq!(
        meta_v.yield_request.as_ref().map(|r| r.message.as_str()),
        Some(reason),
        "load_version must also return the persisted reason"
    );
}

#[tokio::test]
async fn yield_persists_reason() {
    let mut graph: Graph<SimpleState> = Graph::new();
    graph.add_node("yielder", YieldingNode);
    graph.add_edge("yielder", "__END__");

    let cp = InMemoryCheckpointer::new();
    let _result = graph
        .run("reason_test", SimpleState::default(), "yielder", &cp, None)
        .await;

    assert_yield_reason_persisted(&cp, "reason_test", "test_reason").await;
}

#[tokio::test]
async fn non_yield_has_no_reason() {
    let mut graph: Graph<SimpleState> = Graph::new();
    graph.add_node("pass", PassthroughNode);
    graph.add_edge("pass", "__END__");

    let cp = InMemoryCheckpointer::new();
    let _result = graph
        .run("normal_test", SimpleState::default(), "pass", &cp, None)
        .await;

    let checkpoints = cp.list_checkpoints("normal_test".into()).await.unwrap();
    let last = checkpoints.last().expect("must have at least one checkpoint");
    assert_eq!(last.status, CheckpointStatus::Complete, "checkpoint must be Complete");
    assert!(
        last.yield_request.is_none(),
        "non-yield checkpoint must carry no yield request"
    );

    let (_, meta, _) = cp
        .load_state("normal_test".into())
        .await
        .unwrap()
        .expect("checkpoint must exist");
    assert!(meta.yield_request.is_none(), "load_state must carry no yield request");
}

#[tokio::test]
async fn dag_wave_yield_persists_reason() {
    let mut graph: Graph<SimpleState> = Graph::new();
    graph.add_node("yielder", YieldingNode);
    graph.add_node("pass", PassthroughNode);

    let mut dag = takeln::DAG::builder()
        .node("yielder", &[])
        .node("pass", &[])
        .build()
        .unwrap();

    let cp = InMemoryCheckpointer::new();
    let _ = graph
        .run_dag("dag_reason_test", &mut dag, SimpleState::default(), &cp, None, 0)
        .await;

    assert_yield_reason_persisted(&cp, "dag_reason_test", "test_reason").await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn yield_reason_survives_the_sqlite_store() {
    let mut graph: Graph<SimpleState> = Graph::new();
    graph.add_node("yielder", YieldingNode);
    graph.add_edge("yielder", "__END__");

    let cp = takeln::SqliteCheckpointer::<SimpleState>::in_memory().unwrap();
    let _ = graph
        .run("sqlite_reason_test", SimpleState::default(), "yielder", &cp, None)
        .await;

    assert_yield_reason_persisted(&cp, "sqlite_reason_test", "test_reason").await;
}
