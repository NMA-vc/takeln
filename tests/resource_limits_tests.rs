use async_trait::async_trait;
use takeln::{Graph, GraphError, InMemoryCheckpointer, Merge, Node, NodeContext, NodeOutput, ResourceLimits, DAG};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug)]
struct TestState {
    counter: u64,
}

impl Merge for TestState {
    fn merge(&mut self, other: Self) {
        self.counter += other.counter;
    }
}

struct IncrNode;

#[async_trait]
impl Node<TestState> for IncrNode {
    async fn call(&self, _ctx: NodeContext, mut state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        state.counter += 1;
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::test]
async fn test_max_dag_nodes_enforced() {
    let mut graph = Graph::new();
    graph.set_resource_limits(ResourceLimits::default().with_max_dag_nodes(5));

    let mut builder = DAG::builder();
    for i in 0..10 {
        let name = format!("n{}", i);
        graph.add_node(&name, IncrNode);
        if i == 0 {
            builder = builder.node(&name, &[]);
        } else {
            builder = builder.node(&name, &[&format!("n{}", i - 1)]);
        }
    }
    let mut dag = builder.build().unwrap();
    let cp = InMemoryCheckpointer::new();
    let result = graph.run_dag("t1", &mut dag, TestState::default(), &cp, None, 0).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("max_dag_nodes"),
        "Expected max_dag_nodes error, got: {}",
        err
    );
}

#[tokio::test]
async fn test_max_checkpoint_bytes_enforced() {
    let mut graph = Graph::new();
    graph.set_resource_limits(ResourceLimits::default().with_max_checkpoint_bytes(10));

    graph.add_node("big", IncrNode);
    graph.add_edge("big", "__END__");

    let cp = InMemoryCheckpointer::new();
    let state = TestState { counter: 0 };
    let result = graph.run("t1", state, "big", &cp, None).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("max_checkpoint_bytes"),
        "Expected max_checkpoint_bytes error, got: {}",
        err
    );
}

#[tokio::test]
async fn test_semaphore_limits_concurrency() {
    // Verify that execution succeeds even with a tight concurrency limit
    let mut graph = Graph::new();
    graph.set_resource_limits(ResourceLimits::default().with_max_concurrent_nodes(2));

    for i in 0..6 {
        graph.add_node(format!("n{}", i), IncrNode);
    }

    // Build a 3-wave DAG: [n0, n1] -> [n2, n3] -> [n4, n5]
    let id0 = uuid::Uuid::new_v4();
    let id1 = uuid::Uuid::new_v4();
    let id2 = uuid::Uuid::new_v4();
    let id3 = uuid::Uuid::new_v4();
    let id4 = uuid::Uuid::new_v4();
    let id5 = uuid::Uuid::new_v4();

    let mut dag = DAG {
        id: uuid::Uuid::new_v4(),
        nodes: vec![
            takeln::DAGNode {
                id: id0,
                step_type: "n0".into(),
                depends_on: vec![],
                status: takeln::NodeStatus::Pending,
            },
            takeln::DAGNode {
                id: id1,
                step_type: "n1".into(),
                depends_on: vec![],
                status: takeln::NodeStatus::Pending,
            },
            takeln::DAGNode {
                id: id2,
                step_type: "n2".into(),
                depends_on: vec![id0, id1],
                status: takeln::NodeStatus::Pending,
            },
            takeln::DAGNode {
                id: id3,
                step_type: "n3".into(),
                depends_on: vec![id0, id1],
                status: takeln::NodeStatus::Pending,
            },
            takeln::DAGNode {
                id: id4,
                step_type: "n4".into(),
                depends_on: vec![id2, id3],
                status: takeln::NodeStatus::Pending,
            },
            takeln::DAGNode {
                id: id5,
                step_type: "n5".into(),
                depends_on: vec![id2, id3],
                status: takeln::NodeStatus::Pending,
            },
        ],
        created_at: chrono::Utc::now(),
    };

    let cp = InMemoryCheckpointer::new();
    let result = graph
        .run_dag("t_sem", &mut dag, TestState::default(), &cp, None, 0)
        .await;
    assert!(result.is_ok(), "DAG should succeed with semaphore: {:?}", result.err());
    let final_state = result.unwrap();
    // Verify all nodes ran (counter > 0 and all nodes marked Done)
    assert!(final_state.counter > 0, "Counter should be positive after execution");
    for node in &dag.nodes {
        assert_eq!(node.status, takeln::NodeStatus::Done, "All nodes should be Done");
    }
}
