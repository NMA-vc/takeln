//! Wrapping an LLM API call with token/cost metadata.

use async_trait::async_trait;
use takeln::{Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
struct ChatState {
    prompt: String,
    response: String,
}

struct LLMCallNode;

#[async_trait]
impl Node<ChatState> for LLMCallNode {
    async fn call(&self, _ctx: NodeContext, mut state: ChatState) -> Result<NodeOutput<ChatState>, GraphError> {
        // Simulate an LLM call
        state.response = format!("Answer to: {}", state.prompt);

        Ok(NodeOutput::with_llm(state, 150, 300, "gpt-4o"))
    }
}

struct SummarizeNode;

#[async_trait]
impl Node<ChatState> for SummarizeNode {
    async fn call(&self, _ctx: NodeContext, mut state: ChatState) -> Result<NodeOutput<ChatState>, GraphError> {
        let end = 20.min(state.response.len());
        state.response = format!("Summary: {}", &state.response[..end]);
        Ok(NodeOutput::with_llm(state, 300, 100, "gpt-4o-mini"))
    }
}

#[tokio::main]
async fn main() {
    let graph = Graph::builder()
        .node("call_llm", LLMCallNode)
        .node("summarize", SummarizeNode)
        .edge("call_llm", "summarize")
        .edge("summarize", "__END__")
        .budget_eur(1.0)
        .build();

    let cp = InMemoryCheckpointer::new();
    let state = graph
        .run(
            "chat_1",
            ChatState {
                prompt: "What is Rust?".to_string(),
                response: String::new(),
            },
            "call_llm",
            &cp,
            None,
        )
        .await
        .unwrap();

    println!("Response: {}", state.response);

    // Check execution history for cost tracking
    let history = graph.execution_history().await;
    for record in &history {
        println!(
            "  {} — {}ms, cost: {:?}",
            record.node_name, record.duration_ms, record.cost_eur
        );
    }
}
