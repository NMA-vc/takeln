//! Simulating crash and resuming from checkpoint.

use async_trait::async_trait;
use takeln::{CrashRecoveryPolicy, Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug)]
struct WorkState {
    progress: Vec<String>,
}

struct WorkNode {
    step: String,
}

#[async_trait]
impl Node<WorkState> for WorkNode {
    async fn call(&self, _ctx: NodeContext, mut state: WorkState) -> Result<NodeOutput<WorkState>, GraphError> {
        state.progress.push(self.step.clone());
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    let cp = InMemoryCheckpointer::new();

    // Build graph with 3 steps
    let mut graph = Graph::new();
    graph.add_node("step_1", WorkNode { step: "step_1".into() });
    graph.add_node("step_2", WorkNode { step: "step_2".into() });
    graph.add_node("step_3", WorkNode { step: "step_3".into() });
    graph.add_edge("step_1", "step_2");
    graph.add_edge("step_2", "step_3");
    graph.add_edge("step_3", "__END__");

    // Run and interrupt after step_1
    graph.add_interrupt_after("step_1");
    let state = graph
        .run("job_1", WorkState::default(), "step_1", &cp, None)
        .await
        .unwrap();
    println!("After step 1: {:?}", state.progress);

    // Simulate "crash" — create a new graph (as if process restarted)
    let mut graph2 = Graph::new();
    graph2.add_node("step_1", WorkNode { step: "step_1".into() });
    graph2.add_node("step_2", WorkNode { step: "step_2".into() });
    graph2.add_node("step_3", WorkNode { step: "step_3".into() });
    graph2.add_edge("step_1", "step_2");
    graph2.add_edge("step_2", "step_3");
    graph2.add_edge("step_3", "__END__");
    graph2.set_crash_recovery_policy(CrashRecoveryPolicy::ResetToPending);

    // Resume from checkpoint
    let final_state = graph2.resume("job_1", &cp, None).await.unwrap().unwrap();
    println!("Final state: {:?}", final_state.progress);
}
