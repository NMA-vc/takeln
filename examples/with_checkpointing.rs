use async_trait::async_trait;
use takeln::{Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug)]
struct ResumableState {
    step_completed: Option<String>,
}

struct NodeA;

#[async_trait]
impl Node<ResumableState> for NodeA {
    async fn call(
        &self,
        _ctx: NodeContext,
        mut state: ResumableState,
    ) -> Result<NodeOutput<ResumableState>, GraphError> {
        println!("NodeA completed successfully.");
        state.step_completed = Some("NodeA".to_string());
        Ok(NodeOutput::bare(state))
    }
}

struct YieldingNodeB;

#[async_trait]
impl Node<ResumableState> for YieldingNodeB {
    async fn call(&self, _ctx: NodeContext, _state: ResumableState) -> Result<NodeOutput<ResumableState>, GraphError> {
        println!("NodeB encountered an interruption (e.g. API down, need manual intervention). Yielding...");
        Err(GraphError::Yield("Interrupted".to_string()))
    }
}

struct SuccessfulNodeB;

#[async_trait]
impl Node<ResumableState> for SuccessfulNodeB {
    async fn call(
        &self,
        _ctx: NodeContext,
        mut state: ResumableState,
    ) -> Result<NodeOutput<ResumableState>, GraphError> {
        println!("NodeB executed successfully after resumption!");
        state.step_completed = Some("NodeB".to_string());
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    // 1. Initial graph setup where Node B will yield/fail
    let mut initial_graph = Graph::new();
    initial_graph.add_node("NodeA", NodeA);
    initial_graph.add_node("NodeB", YieldingNodeB);
    initial_graph.add_edge("NodeA", "NodeB");
    initial_graph.add_edge("NodeB", "__END__");

    let checkpointer = InMemoryCheckpointer::new();

    println!("--- Initial Run (will yield at NodeB) ---");
    let state = ResumableState::default();
    initial_graph
        .run("thread_chk", state, "NodeA", &checkpointer, None)
        .await
        .unwrap();

    // 2. Resume run using a graph where Node B is now healthy/successful
    let mut resume_graph = Graph::new();
    resume_graph.add_node("NodeB", SuccessfulNodeB);
    resume_graph.add_edge("NodeB", "__END__");

    println!("\n--- Resuming Run (from NodeB) using first-class resume API ---");
    let final_state = resume_graph
        .resume("thread_chk", &checkpointer, None)
        .await
        .unwrap()
        .unwrap();
    println!("Final State: {:?}", final_state);
}
