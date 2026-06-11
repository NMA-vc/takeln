use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use takeln::{Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default)]
struct BenchState {
    counter: u64,
}

struct IncrementNode;

#[async_trait]
impl Node<BenchState> for IncrementNode {
    async fn call(&self, _ctx: NodeContext, mut state: BenchState) -> Result<NodeOutput<BenchState>, GraphError> {
        state.counter += 1;
        Ok(NodeOutput::bare(state))
    }
}

fn bench_sequential_chain(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("sequential_10_nodes", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut graph = Graph::new();
                let node_names: Vec<String> = (0..10).map(|i| format!("node_{}", i)).collect();

                for name in &node_names {
                    graph.add_node(name.as_str(), IncrementNode);
                }
                for i in 0..9 {
                    graph.add_edge(&node_names[i], &node_names[i + 1]);
                }
                graph.add_edge(&node_names[9], "__END__");

                let cp = InMemoryCheckpointer::new();
                graph
                    .run("bench", BenchState::default(), &node_names[0], &cp, None)
                    .await
                    .unwrap()
            })
        })
    });
}

criterion_group!(benches, bench_sequential_chain);
criterion_main!(benches);
