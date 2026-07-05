//! Structured HITL: yield with schema validation, then resume with input.

use async_trait::async_trait;
use serde_json::json;
use takeln::{Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput, ResumeMode, YieldRequest};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PaymentState {
    amount: f64,
    recipient: String,
    approved: bool,
}

impl Default for PaymentState {
    fn default() -> Self {
        Self {
            amount: 0.0,
            recipient: String::new(),
            approved: false,
        }
    }
}

struct PreparePaymentNode;

#[async_trait]
impl Node<PaymentState> for PreparePaymentNode {
    async fn call(&self, _ctx: NodeContext, mut state: PaymentState) -> Result<NodeOutput<PaymentState>, GraphError> {
        state.amount = 200.0;
        state.recipient = "Acme Corp".to_string();
        println!("[prepare] ${:.2} to {}", state.amount, state.recipient);
        Ok(NodeOutput::bare(state))
    }
}

struct RequestApprovalNode;

#[async_trait]
impl Node<PaymentState> for RequestApprovalNode {
    async fn call(&self, ctx: NodeContext, mut state: PaymentState) -> Result<NodeOutput<PaymentState>, GraphError> {
        if let Some(val) = &ctx.resumed_input {
            if val == &json!("yes") {
                state.approved = true;
            }
            println!("[approval] Received: {}, approved = {}", val, state.approved);
            Ok(NodeOutput::bare(state))
        } else {
            // The mandate data lives in the caller's store, not inline in the
            // checkpoint. `payload_ref` keeps the checkpoint free of PII.
            Err(GraphError::Yield(
                YieldRequest::new(
                    "approve_payment",
                    format!("Approve ${:.2} to {}?", state.amount, state.recipient),
                )
                .with_schema(json!({ "type": "string", "enum": ["yes", "no"] }))
                .with_resume_mode(ResumeMode::ReEntry)
                .with_payload_ref("mandate-ref-12345"),
            ))
        }
    }
}

struct ExecutePaymentNode;

#[async_trait]
impl Node<PaymentState> for ExecutePaymentNode {
    async fn call(&self, _ctx: NodeContext, state: PaymentState) -> Result<NodeOutput<PaymentState>, GraphError> {
        if state.approved {
            println!(
                "[execute] Payment of ${:.2} to {} executed!",
                state.amount, state.recipient
            );
        } else {
            println!("[execute] Payment rejected.");
        }
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    let graph = Graph::builder()
        .node("prepare", PreparePaymentNode)
        .node("request_approval", RequestApprovalNode)
        .node("execute_payment", ExecutePaymentNode)
        .edge("prepare", "request_approval")
        .edge("request_approval", "execute_payment")
        .edge("execute_payment", "__END__")
        .build();

    let cp = InMemoryCheckpointer::new();
    let state = graph
        .run("payment_1", PaymentState::default(), "prepare", &cp, None)
        .await
        .unwrap();

    println!(
        "\nGraph yielded — amount: ${:.2}, recipient: {}",
        state.amount, state.recipient
    );
    println!("Simulating human approval...\n");

    let final_state = graph
        .resume_with_input(
            "payment_1",
            "approve_payment",
            json!("yes"),
            takeln::ResumeContext::new("manager_bob"),
            &cp,
            None,
        )
        .await
        .unwrap()
        .unwrap();
    println!("\nFinal state: {:?}", final_state);
}
