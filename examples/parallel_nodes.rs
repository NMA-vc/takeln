use async_trait::async_trait;
use takeln::{DAGNode, Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput, NodeStatus, DAG};
use uuid::Uuid;

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug)]
struct ParallelState {
    executed: Vec<String>,
}

impl takeln::Merge for ParallelState {
    fn merge(&mut self, other: Self) {
        self.executed.extend(other.executed);
    }
}

struct LogNode {
    name: String,
}

#[async_trait]
impl Node<ParallelState> for LogNode {
    async fn call(&self, _ctx: NodeContext, mut state: ParallelState) -> Result<NodeOutput<ParallelState>, GraphError> {
        println!("Executing node: {}", self.name);
        state.executed.push(self.name.clone());
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    let mut graph = Graph::new();
    // Register step types to string identifiers in Graph
    graph.add_node(
        "Navigate",
        LogNode {
            name: "Navigate Node".to_string(),
        },
    );
    graph.add_node(
        "Extract",
        LogNode {
            name: "Extract Node".to_string(),
        },
    );

    let checkpointer = InMemoryCheckpointer::new();
    let initial_state = ParallelState::default();

    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();

    let mut dag = DAG {
        id: Uuid::new_v4(),
        nodes: vec![
            DAGNode {
                id: id_a,
                step_type: "Navigate".to_string(),
                depends_on: vec![],
                status: NodeStatus::Pending,
            },
            DAGNode {
                id: id_b,
                step_type: "Extract".to_string(),
                depends_on: vec![],
                status: NodeStatus::Pending,
            },
        ],
        created_at: chrono::Utc::now(),
    };

    println!("Executing parallel DAG wave...");
    let final_state = graph
        .run_dag("thread_parallel_ex", &mut dag, initial_state, &checkpointer, None, 0)
        .await
        .unwrap();

    println!(
        "Parallel execution complete. Executed nodes: {:?}",
        final_state.executed
    );
}
