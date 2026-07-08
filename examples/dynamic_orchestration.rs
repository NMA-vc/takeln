//! Dynamic orchestration: iterate over items using a ChildRunner.

use async_trait::async_trait;
use takeln::{ChildRunner, DynamicNode, Graph, GraphError, InMemoryCheckpointer, NodeContext, NodeOutput};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BatchState {
    items: Vec<String>,
    current_item: String,
    results: Vec<String>,
}

struct ProcessItemNode;

#[async_trait]
impl takeln::Node<BatchState> for ProcessItemNode {
    async fn call(&self, _ctx: NodeContext, mut state: BatchState) -> Result<NodeOutput<BatchState>, GraphError> {
        let upper = state.current_item.to_uppercase();
        println!("[process] '{}' → '{}'", state.current_item, upper);
        state.results.push(upper);
        Ok(NodeOutput::bare(state))
    }
}

struct OrchestratorNode;

#[async_trait]
impl DynamicNode<BatchState> for OrchestratorNode {
    async fn call(
        &self,
        ctx: NodeContext,
        mut state: BatchState,
        runner: &ChildRunner<BatchState>,
    ) -> Result<NodeOutput<BatchState>, GraphError> {
        let items = state.items.clone();
        for item in items {
            state.current_item = item;
            state = runner.run_child_state("process_item", &ctx, state).await?;
        }
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    let graph = Graph::<BatchState>::builder()
        .node("process_item", ProcessItemNode)
        .dynamic_node("orchestrator", OrchestratorNode)
        .edge("orchestrator", "__END__")
        .build();

    let cp = InMemoryCheckpointer::new();
    let initial = BatchState {
        items: vec!["hello".into(), "world".into(), "takeln".into()],
        current_item: String::new(),
        results: Vec::new(),
    };

    let state = graph.run("batch_1", initial, "orchestrator", &cp, None).await.unwrap();

    println!("\nResults: {:?}", state.results);
}
