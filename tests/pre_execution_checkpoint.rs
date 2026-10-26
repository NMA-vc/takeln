//! With `set_pre_execution_checkpoint(true)` a run that dies inside a node leaves a `Running`
//! checkpoint naming that node, so the crash recovery policy decides on resume. Without it the
//! last checkpoint is the previous `Complete`, and the node silently runs again.

use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use takeln::{
    CheckpointStatus, Checkpointer, CrashRecoveryPolicy, Graph, GraphError, InMemoryCheckpointer, Node, NodeContext,
    NodeOutput,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default, PartialEq)]
struct Trail(Vec<String>);

impl takeln::Merge for Trail {
    fn merge(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}

struct Step {
    name: &'static str,
    /// Calls that fail before the node succeeds, standing in for a crash mid-node.
    failures: usize,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Node<Trail> for Step {
    async fn call(&self, _ctx: NodeContext, mut state: Trail) -> Result<NodeOutput<Trail>, GraphError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.failures {
            return Err(GraphError::Fatal("worker died".into()));
        }
        state.0.push(self.name.to_string());
        Ok(NodeOutput::bare(state))
    }
}

fn graph(b_calls: &Arc<AtomicUsize>, pre_execution: bool) -> Graph<Trail> {
    let mut graph = Graph::new();
    let a = Step {
        name: "a",
        failures: 0,
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let b = Step {
        name: "b",
        failures: 1,
        calls: b_calls.clone(),
    };
    graph.add_node("a", a);
    graph.add_node("b", b);
    graph.add_edge("a", "b");
    graph.set_pre_execution_checkpoint(pre_execution);
    graph
}

#[tokio::test]
async fn a_node_that_dies_leaves_a_running_checkpoint_and_fail_fast_refuses_to_rerun_it() {
    let b_calls = Arc::new(AtomicUsize::new(0));
    let mut graph = graph(&b_calls, true);
    let store = InMemoryCheckpointer::<Trail>::new();

    assert!(graph.run("t", Trail::default(), "a", &store, None).await.is_err());
    let (saved, meta, _) = store.load_state("t".into()).await.unwrap().unwrap();
    assert_eq!(meta.status, CheckpointStatus::Running);
    assert_eq!(meta.next_node, "b");
    assert_eq!(saved.0, vec!["a"], "the checkpoint holds the state b was handed");

    graph.set_crash_recovery_policy(CrashRecoveryPolicy::FailFast);
    assert!(graph.resume("t", &store, None).await.is_err());
    assert_eq!(b_calls.load(Ordering::SeqCst), 1, "FailFast does not re-run b");

    graph.set_crash_recovery_policy(CrashRecoveryPolicy::ResetToPending);
    let done = graph.resume("t", &store, None).await.unwrap().unwrap();
    assert_eq!(done.0, vec!["a", "b"]);
    assert_eq!(b_calls.load(Ordering::SeqCst), 2);
    let (_, meta, _) = store.load_state("t".into()).await.unwrap().unwrap();
    assert_eq!(meta.status, CheckpointStatus::Complete);
}

#[tokio::test]
async fn without_the_option_the_last_checkpoint_is_the_previous_complete() {
    let b_calls = Arc::new(AtomicUsize::new(0));
    let graph = graph(&b_calls, false);
    let store = InMemoryCheckpointer::<Trail>::new();

    assert!(graph.run("t", Trail::default(), "a", &store, None).await.is_err());
    let (_, meta, _) = store.load_state("t".into()).await.unwrap().unwrap();
    assert_eq!(meta.status, CheckpointStatus::Complete);
    assert_eq!(meta.next_node, "b");
}
