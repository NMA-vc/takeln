//! A parallel wave in which one node yields: the siblings that finished in that wave keep their
//! output, and `resume_dag` re-enters the yielded node and completes the DAG without re-running
//! what was done. Before the fix the pre-merge state was saved (sibling output lost) and the
//! yielded node stayed `Yielded`, which `ready` never admits, so resume deadlocked.

use async_trait::async_trait;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use takeln::{
    CheckpointStatus, Checkpointer, Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput,
    YieldRequest, DAG,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default, PartialEq)]
struct Parts(BTreeSet<String>);

impl takeln::Merge for Parts {
    fn merge(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}

struct Add {
    part: &'static str,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Node<Parts> for Add {
    async fn call(&self, _ctx: NodeContext, mut state: Parts) -> Result<NodeOutput<Parts>, GraphError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        state.0.insert(self.part.to_string());
        Ok(NodeOutput::bare(state))
    }
}

/// Yields on its first call, completes on the next.
struct GateThenAdd {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Node<Parts> for GateThenAdd {
    async fn call(&self, _ctx: NodeContext, mut state: Parts) -> Result<NodeOutput<Parts>, GraphError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(GraphError::Yield(YieldRequest::new("gate", "approve?")));
        }
        state.0.insert("b".to_string());
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::test]
async fn a_sibling_of_a_yielded_node_keeps_its_output_and_resume_completes_the_dag() {
    let a_calls = Arc::new(AtomicUsize::new(0));
    let b_calls = Arc::new(AtomicUsize::new(0));
    let mut graph = Graph::new();
    graph.add_node(
        "a",
        Add {
            part: "a",
            calls: a_calls.clone(),
        },
    );
    graph.add_node("b", GateThenAdd { calls: b_calls.clone() });
    graph.add_node(
        "c",
        Add {
            part: "c",
            calls: Arc::new(AtomicUsize::new(0)),
        },
    );
    let mut dag = DAG::builder()
        .node("a", &[])
        .node("b", &[])
        .node("c", &["a", "b"])
        .build()
        .unwrap();
    let store = InMemoryCheckpointer::<Parts>::new();

    graph
        .run_dag("t", &mut dag, Parts::default(), &store, None, 0)
        .await
        .unwrap();
    let (saved, meta, _) = store.load_state("t".into()).await.unwrap().unwrap();
    assert_eq!(meta.status, CheckpointStatus::Yielded);
    assert!(
        saved.0.contains("a"),
        "the finished sibling's output was checkpointed: {saved:?}"
    );
    assert!(!saved.0.contains("c"));

    let done = graph
        .resume_dag("t", &mut dag, &store, None, 0)
        .await
        .unwrap()
        .expect("a checkpoint to resume");
    assert_eq!(done.0, ["a", "b", "c"].iter().map(|s| s.to_string()).collect());
    assert_eq!(a_calls.load(Ordering::SeqCst), 1, "a completed sibling is not re-run");
    assert_eq!(b_calls.load(Ordering::SeqCst), 2, "the yielded node is re-entered once");
}

#[tokio::test]
async fn resume_with_input_refuses_a_dag_checkpoint_and_leaves_the_interrupt_claimable() {
    let mut graph = Graph::new();
    graph.add_node(
        "a",
        Add {
            part: "a",
            calls: Arc::new(AtomicUsize::new(0)),
        },
    );
    graph.add_node(
        "b",
        GateThenAdd {
            calls: Arc::new(AtomicUsize::new(0)),
        },
    );
    let mut dag = DAG::builder().node("a", &[]).node("b", &[]).build().unwrap();
    let store = InMemoryCheckpointer::<Parts>::new();
    graph
        .run_dag("t", &mut dag, Parts::default(), &store, None, 0)
        .await
        .unwrap();

    let err = graph
        .resume_with_input(
            "t",
            "gate",
            serde_json::json!({ "decision": "approve" }),
            takeln::ResumeContext::default(),
            &store,
            None,
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("resume it with resume_dag"), "{err}");
    // The refusal came before the claim: the DAG still resumes normally.
    assert!(graph
        .resume_dag("t", &mut dag, &store, None, 0)
        .await
        .unwrap()
        .is_some());
}
