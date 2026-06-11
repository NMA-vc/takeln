//! DAG builder API for a 5-node parallel workflow.

use async_trait::async_trait;
use takeln::{Graph, GraphError, InMemoryCheckpointer, Merge, Node, NodeContext, NodeOutput, DAG};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
struct PipelineState {
    steps: Vec<String>,
}

impl Merge for PipelineState {
    fn merge(&mut self, other: Self) {
        self.steps.extend(other.steps);
    }
}

struct StepNode {
    name: String,
}

#[async_trait]
impl Node<PipelineState> for StepNode {
    async fn call(&self, _ctx: NodeContext, mut state: PipelineState) -> Result<NodeOutput<PipelineState>, GraphError> {
        state.steps.push(self.name.clone());
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::main]
async fn main() {
    let mut graph = Graph::new();
    graph.add_node("fetch", StepNode { name: "fetch".into() });
    graph.add_node("parse", StepNode { name: "parse".into() });
    graph.add_node("score", StepNode { name: "score".into() });
    graph.add_node("rank", StepNode { name: "rank".into() });
    graph.add_node("merge", StepNode { name: "merge".into() });

    let mut dag = DAG::builder()
        .node("fetch", &[])
        .node("parse", &["fetch"])
        .node("score", &["parse"])
        .node("rank", &["parse"])
        .node("merge", &["score", "rank"])
        .build()
        .unwrap();

    let cp = InMemoryCheckpointer::new();
    let state = graph
        .run_dag("pipeline_1", &mut dag, PipelineState::default(), &cp, None, 0)
        .await
        .unwrap();
    println!("Pipeline completed with {} steps: {:?}", state.steps.len(), state.steps);
}
