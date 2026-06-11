use async_trait::async_trait;
use takeln::{Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug)]
struct ChainState {
    output: String,
}

struct StepNode {
    text: String,
}

#[async_trait]
impl Node<ChainState> for StepNode {
    async fn call(&self, _ctx: NodeContext, mut state: ChainState) -> Result<NodeOutput<ChainState>, GraphError> {
        state.output.push_str(&self.text);
        state.output.push(' ');
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    let mut graph = Graph::new();
    graph.add_node(
        "Node1",
        StepNode {
            text: "hello".to_string(),
        },
    );
    graph.add_node(
        "Node2",
        StepNode {
            text: "from".to_string(),
        },
    );
    graph.add_node(
        "Node3",
        StepNode {
            text: "takeln".to_string(),
        },
    );

    graph.add_edge("Node1", "Node2");
    graph.add_edge("Node2", "Node3");
    graph.add_edge("Node3", "__END__");

    let checkpointer = InMemoryCheckpointer::new();
    let initial_state = ChainState::default();

    println!("Starting simple chain execution...");
    let final_state = graph
        .run("thread_chain", initial_state, "Node1", &checkpointer, None)
        .await
        .unwrap();

    println!("Execution finished. Output: '{}'", final_state.output.trim());
}
