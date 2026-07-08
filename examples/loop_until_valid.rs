//! Loop until valid: demonstrates sequential loops via conditional edges.

use async_trait::async_trait;
use takeln::{Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput};

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct LoopState {
    value: u32,
    attempt_count: u32,
    is_valid: bool,
}

struct GenerateNode;

#[async_trait]
impl Node<LoopState> for GenerateNode {
    async fn call(&self, _ctx: NodeContext, mut state: LoopState) -> Result<NodeOutput<LoopState>, GraphError> {
        state.attempt_count += 1;
        state.value = state.attempt_count;
        println!("[generate] attempt {}, value = {}", state.attempt_count, state.value);
        Ok(NodeOutput::bare(state))
    }
}

struct ValidateNode;

#[async_trait]
impl Node<LoopState> for ValidateNode {
    async fn call(&self, _ctx: NodeContext, mut state: LoopState) -> Result<NodeOutput<LoopState>, GraphError> {
        if state.value >= 3 {
            state.is_valid = true;
        }
        println!("[validate] value = {}, valid = {}", state.value, state.is_valid);
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    let graph = Graph::builder()
        .node("generate", GenerateNode)
        .node("validate", ValidateNode)
        .edge("generate", "validate")
        .conditional_edge("validate", |state: &LoopState| {
            if state.is_valid {
                "__END__".to_string()
            } else {
                "generate".to_string()
            }
        })
        .build();

    let cp = InMemoryCheckpointer::new();
    let state = graph
        .run("loop_1", LoopState::default(), "generate", &cp, None)
        .await
        .unwrap();
    println!("\nDone! Final state: {:?}", state);
}
