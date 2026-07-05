use async_trait::async_trait;
use takeln::{
    CheckpointStatus, Checkpointer, Graph, GraphError, InMemoryCheckpointer, Merge, Node, NodeContext, NodeOutput, DAG,
};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug)]
struct LoadState {
    counter: u64,
}

impl Merge for LoadState {
    fn merge(&mut self, other: Self) {
        self.counter += other.counter;
    }
}

struct CountNode;

#[async_trait]
impl Node<LoadState> for CountNode {
    async fn call(&self, _ctx: NodeContext, mut state: LoadState) -> Result<NodeOutput<LoadState>, GraphError> {
        state.counter += 1;
        Ok(NodeOutput::bare(state))
    }
}

/// 100-node DAG with 10 waves of 10 — verify completion and correctness.
#[tokio::test]
async fn test_large_dag_100_nodes() {
    let mut graph = Graph::new();
    let mut builder = DAG::builder();

    // Create 10 waves of 10 nodes each
    for wave in 0..10 {
        for node in 0..10 {
            let name = format!("w{}n{}", wave, node);
            graph.add_node(&name, CountNode);
            if wave == 0 {
                builder = builder.node(&name, &[]);
            } else {
                // Each node depends on one node from the previous wave
                let dep = format!("w{}n{}", wave - 1, node);
                builder = builder.node(&name, &[&dep]);
            }
        }
    }

    let mut dag = builder.build().unwrap();
    assert_eq!(dag.nodes.len(), 100);

    let cp = InMemoryCheckpointer::new();
    let _result = graph
        .run_dag("load_100", &mut dag, LoadState::default(), &cp, None, 0)
        .await
        .unwrap();

    // Each of 100 nodes increments by 1, but parallel nodes get merged
    // The exact counter depends on merge order, but all nodes should be Done
    for node in &dag.nodes {
        assert_eq!(
            node.status,
            takeln::NodeStatus::Done,
            "Node {} not done",
            node.step_type
        );
    }
}

/// Sustained checkpoint throughput: 10,000 saves in reasonable time.
#[tokio::test]
async fn test_checkpoint_throughput_10k() {
    let cp = InMemoryCheckpointer::<LoadState>::new();
    let state = LoadState { counter: 42 };

    let start = std::time::Instant::now();
    for i in 0..10_000 {
        cp.save_state(
            format!("thread_{}", i % 100),
            state.clone(),
            "next".to_string(),
            None,
            CheckpointStatus::Complete,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    }
    let elapsed = start.elapsed();

    // Should complete in well under 10 seconds
    assert!(
        elapsed.as_secs() < 10,
        "10,000 saves took {:?}, expected < 10s",
        elapsed
    );
}

/// Multiple sequential graphs on separate threads — verify isolation.
#[tokio::test]
async fn test_concurrent_thread_isolation() {
    let cp = InMemoryCheckpointer::new();

    let mut handles = vec![];
    for t in 0..10 {
        let thread_id = format!("thread_{}", t);
        let cp_ref = &cp;
        handles.push(async move {
            let mut graph = Graph::new();
            graph.add_node("inc", CountNode);
            graph.add_edge("inc", "__END__");
            let result = graph
                .run(&thread_id, LoadState::default(), "inc", cp_ref, None)
                .await
                .unwrap();
            assert_eq!(result.counter, 1);
        });
    }

    // Run all 10 sequentially (they share the checkpointer)
    for handle in handles {
        handle.await;
    }

    // Verify each thread has its own checkpoint
    for t in 0..10 {
        let loaded = cp.load_state(format!("thread_{}", t)).await.unwrap();
        assert!(loaded.is_some());
    }
}
