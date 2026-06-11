//! Human-in-the-loop: interrupt before a node, then resume.

use async_trait::async_trait;
use takeln::{Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
struct ApprovalState {
    draft: String,
    approved: bool,
}

struct DraftNode;

#[async_trait]
impl Node<ApprovalState> for DraftNode {
    async fn call(&self, _ctx: NodeContext, mut state: ApprovalState) -> Result<NodeOutput<ApprovalState>, GraphError> {
        state.draft = "Important document content".to_string();
        Ok(NodeOutput::bare(state))
    }
}

struct PublishNode;

#[async_trait]
impl Node<ApprovalState> for PublishNode {
    async fn call(&self, _ctx: NodeContext, mut state: ApprovalState) -> Result<NodeOutput<ApprovalState>, GraphError> {
        state.approved = true;
        println!("Published: {}", state.draft);
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    let mut graph = Graph::new();
    graph.add_node("draft", DraftNode);
    graph.add_node("publish", PublishNode);
    graph.add_edge("draft", "publish");
    graph.add_edge("publish", "__END__");
    graph.add_interrupt_before("publish");

    let cp = InMemoryCheckpointer::new();

    // First run: stops before "publish"
    let state = graph
        .run("approval_1", ApprovalState::default(), "draft", &cp, None)
        .await
        .unwrap();
    println!("Draft ready: '{}', awaiting approval...", state.draft);

    // Simulate human approval, then resume
    let final_state = graph.resume("approval_1", &cp, None).await.unwrap().unwrap();
    println!("Published: {}", final_state.approved);
}
