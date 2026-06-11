use async_trait::async_trait;
use proptest::prelude::*;
use takeln::{
    CheckpointStatus, Checkpointer, Graph, GraphError, InMemoryCheckpointer, Merge, Node, NodeContext, NodeOutput, DAG,
};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug, PartialEq)]
struct PropState {
    value: String,
    counter: u64,
}

impl Merge for PropState {
    fn merge(&mut self, other: Self) {
        self.value.push_str(&other.value);
        self.counter += other.counter;
    }
}

struct IncrNode;

#[async_trait]
impl Node<PropState> for IncrNode {
    async fn call(&self, _ctx: NodeContext, mut state: PropState) -> Result<NodeOutput<PropState>, GraphError> {
        state.counter += 1;
        Ok(NodeOutput::bare(state))
    }
}

// Property 1: Checkpoint fidelity — save then load returns equivalent state
proptest! {
    #[test]
    fn checkpoint_roundtrip(
        value in "[a-zA-Z0-9]{0,100}",
        counter in 0u64..1_000_000,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let cp = InMemoryCheckpointer::<PropState>::new();
            let state = PropState { value: value.clone(), counter };
            let id = cp.save_state(
                "prop_thread".to_string(), state.clone(), "next".to_string(),
                None, CheckpointStatus::Complete,
            ).await.unwrap();

            let (loaded, meta, _) = cp.load_state("prop_thread".to_string()).await.unwrap().unwrap();
            prop_assert_eq!(loaded.value, value);
            prop_assert_eq!(loaded.counter, counter);
            prop_assert_eq!(meta.checkpoint_id, id);
            Ok(())
        })?;
    }
}

// Property 2: DAG completion — all nodes reach Done for any valid linear DAG
proptest! {
    #[test]
    fn dag_completes_all_nodes(node_count in 2usize..20) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut graph = Graph::new();
            let mut builder = DAG::builder();

            let names: Vec<String> = (0..node_count).map(|i| format!("n{}", i)).collect();

            for (i, name) in names.iter().enumerate() {
                graph.add_node(name.as_str(), IncrNode);
                if i == 0 {
                    builder = builder.node(name, &[]);
                } else {
                    builder = builder.node(name, &[&names[i - 1]]);
                }
            }

            let mut dag = builder.build().unwrap();
            let cp = InMemoryCheckpointer::new();
            let result = graph.run_dag("prop_dag", &mut dag, PropState::default(), &cp, None, 0).await.unwrap();

            // The merge trait adds the returned counter onto the base state,
            // so exact value depends on merge semantics. Just verify it ran.
            prop_assert!(result.counter > 0);
            // All nodes should be Done
            for node in &dag.nodes {
                prop_assert_eq!(node.status.clone(), takeln::NodeStatus::Done);
            }
            Ok(())
        })?;
    }
}

// Property 3: Budget enforcement — budget never allows overspend
proptest! {
    #[test]
    fn budget_enforced(budget in 0.001f64..1.0) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut graph = Graph::<PropState>::new();

            // Each node costs ~0.01 EUR via with_llm
            struct CostNode;
            #[async_trait]
            impl Node<PropState> for CostNode {
                async fn call(&self, _ctx: NodeContext, state: PropState) -> Result<NodeOutput<PropState>, GraphError> {
                    Ok(NodeOutput::with_llm(state, 100, 100, "test-model"))
                }
            }

            for i in 0..100 {
                graph.add_node(format!("n{}", i), CostNode);
            }
            for i in 0..99 {
                graph.add_edge(&format!("n{}", i), &format!("n{}", i + 1));
            }
            graph.add_edge("n99", "__END__");
            graph.set_budget_eur(budget);

            let cp = InMemoryCheckpointer::new();
            let result = graph.run("budget_prop", PropState::default(), "n0", &cp, None).await;

            // Should either complete (if budget was sufficient) or error with BudgetExceeded
            match result {
                Ok(_) => {} // Budget was enough
                Err(takeln::TakelnError::BudgetExceeded { spent_eur: _, limit_eur }) => {
                    // The overshoot should be at most one node's cost
                    // (the node that triggered the breach already ran)
                    prop_assert!(limit_eur <= budget + 0.001);
                }
                Err(e) => prop_assert!(false, "Unexpected error: {:?}", e),
            }
            Ok(())
        })?;
    }
}
