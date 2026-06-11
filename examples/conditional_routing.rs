//! Conditional edge routing based on state.

use async_trait::async_trait;
use takeln::{Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
struct ReviewState {
    score: f64,
    decision: String,
}

struct ScoreNode;

#[async_trait]
impl Node<ReviewState> for ScoreNode {
    async fn call(&self, _ctx: NodeContext, mut state: ReviewState) -> Result<NodeOutput<ReviewState>, GraphError> {
        state.score = 0.85;
        Ok(NodeOutput::bare(state))
    }
}

struct ApproveNode;

#[async_trait]
impl Node<ReviewState> for ApproveNode {
    async fn call(&self, _ctx: NodeContext, mut state: ReviewState) -> Result<NodeOutput<ReviewState>, GraphError> {
        state.decision = "approved".to_string();
        Ok(NodeOutput::bare(state))
    }
}

struct RejectNode;

#[async_trait]
impl Node<ReviewState> for RejectNode {
    async fn call(&self, _ctx: NodeContext, mut state: ReviewState) -> Result<NodeOutput<ReviewState>, GraphError> {
        state.decision = "rejected".to_string();
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    let graph = Graph::builder()
        .node("score", ScoreNode)
        .node("approve", ApproveNode)
        .node("reject", RejectNode)
        .conditional_edge("score", |state: &ReviewState| {
            if state.score >= 0.7 {
                "approve".to_string()
            } else {
                "reject".to_string()
            }
        })
        .edge("approve", "__END__")
        .edge("reject", "__END__")
        .build();

    let cp = InMemoryCheckpointer::new();
    let state = graph
        .run("review_1", ReviewState::default(), "score", &cp, None)
        .await
        .unwrap();
    println!("Decision: {} (score: {:.2})", state.decision, state.score);
}
