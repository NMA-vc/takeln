use async_trait::async_trait;
use takeln::{
    CheckpointStatus, Checkpointer, DAGNode, Graph, GraphError, GraphEvent, InMemoryCheckpointer, Node, NodeConfig,
    NodeContext, NodeMeta, NodeOutput, NodeStatus, RetryPolicy, SpanStatus, TakelnError, WaveFailurePolicy, DAG,
};
use uuid::Uuid;

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug)]
struct TestState {
    value: String,
    logs: Vec<String>,
}

impl takeln::Merge for TestState {
    fn merge(&mut self, other: Self) {
        if !other.value.is_empty() {
            self.value = other.value;
        }
        self.logs.extend(other.logs);
    }
}

struct AppendNode {
    suffix: String,
}

#[async_trait]
impl Node<TestState> for AppendNode {
    async fn call(&self, _ctx: NodeContext, mut state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        state.value.push_str(&self.suffix);
        state.logs.push(self.suffix.clone());
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::test]
async fn test_in_memory_checkpointer() {
    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState {
        value: "init".to_string(),
        logs: vec![],
    };

    checkpointer
        .save_state(
            "thread_1".to_string(),
            state,
            "next_step".to_string(),
            None,
            CheckpointStatus::Complete,
        )
        .await
        .unwrap();

    let loaded = checkpointer.load_state("thread_1".to_string()).await.unwrap();
    assert!(loaded.is_some());
    let (s, meta, _) = loaded.unwrap();
    assert_eq!(s.value, "init");
    assert_eq!(meta.next_node, "next_step");
}

#[tokio::test]
async fn test_dag_linear_execution() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node(
        "B",
        AppendNode {
            suffix: "B".to_string(),
        },
    );
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let final_state = graph
        .run("thread_linear", state, "A", &checkpointer, None)
        .await
        .unwrap();

    assert_eq!(final_state.value, "AB");
    assert_eq!(final_state.logs, vec!["A", "B"]);
}

#[tokio::test]
async fn test_dag_parallel_execution() {
    let mut graph = Graph::new();
    graph.add_node(
        "Navigate",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node(
        "Extract",
        AppendNode {
            suffix: "B".to_string(),
        },
    );

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();

    let mut dag = DAG {
        id: Uuid::new_v4(),
        nodes: vec![
            DAGNode {
                id: id_a,
                step_type: "Navigate".to_string(),
                depends_on: vec![],
                status: NodeStatus::Pending,
            },
            DAGNode {
                id: id_b,
                step_type: "Extract".to_string(),
                depends_on: vec![],
                status: NodeStatus::Pending,
            },
        ],
        created_at: chrono::Utc::now(),
    };

    let final_state = graph
        .run_dag("thread_parallel", &mut dag, state, &checkpointer, None, 0)
        .await
        .unwrap();

    assert!(final_state.logs.contains(&"A".to_string()));
    assert!(final_state.logs.contains(&"B".to_string()));
    assert_eq!(final_state.logs.len(), 2);
}

struct YieldNode;

#[async_trait]
impl Node<TestState> for YieldNode {
    async fn call(&self, _ctx: NodeContext, _state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        Err(GraphError::Yield("interrupted".to_string()))
    }
}

#[tokio::test]
async fn test_checkpoint_save_and_resume() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node("B", YieldNode);
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    graph
        .run("thread_resume", state, "A", &checkpointer, None)
        .await
        .unwrap();

    let (loaded_state, meta, _) = checkpointer
        .load_state("thread_resume".to_string())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(loaded_state.value, "A");
    assert_eq!(meta.next_node, "B");

    let mut resume_graph = Graph::new();
    resume_graph.add_node(
        "B",
        AppendNode {
            suffix: "B".to_string(),
        },
    );
    resume_graph.add_edge("B", "__END__");

    let final_state = resume_graph
        .run("thread_resume", loaded_state, &meta.next_node, &checkpointer, None)
        .await
        .unwrap();

    assert_eq!(final_state.value, "AB");
}

#[tokio::test]
async fn test_time_travel_checkpointing() {
    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let thread_id = "thread_time_travel".to_string();

    let state1 = TestState {
        value: "step1".to_string(),
        logs: vec!["L1".to_string()],
    };
    let id1 = checkpointer
        .save_state(
            thread_id.clone(),
            state1,
            "NodeB".to_string(),
            None,
            CheckpointStatus::Complete,
        )
        .await
        .unwrap();

    let state2 = TestState {
        value: "step2".to_string(),
        logs: vec!["L1".to_string(), "L2".to_string()],
    };
    let id2 = checkpointer
        .save_state(
            thread_id.clone(),
            state2,
            "NodeC".to_string(),
            None,
            CheckpointStatus::Complete,
        )
        .await
        .unwrap();

    let checkpoints = checkpointer.list_checkpoints(thread_id.clone()).await.unwrap();
    assert_eq!(checkpoints.len(), 2);
    assert_eq!(checkpoints[0].checkpoint_id, id1);
    assert_eq!(checkpoints[0].next_node, "NodeB");
    assert_eq!(checkpoints[1].checkpoint_id, id2);
    assert_eq!(checkpoints[1].next_node, "NodeC");

    let loaded1 = checkpointer
        .load_version(thread_id.clone(), id1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded1.0.value, "step1");
    assert_eq!(loaded1.1.next_node, "NodeB");

    let loaded2 = checkpointer
        .load_version(thread_id.clone(), id2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded2.0.value, "step2");
    assert_eq!(loaded2.1.next_node, "NodeC");
}

#[tokio::test]
async fn test_declarative_interrupt_before() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node(
        "B",
        AppendNode {
            suffix: "B".to_string(),
        },
    );
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");

    graph.add_interrupt_before("B");

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let final_state = graph
        .run("thread_interrupt_before", state, "A", &checkpointer, None)
        .await
        .unwrap();

    assert_eq!(final_state.value, "A");
    assert_eq!(final_state.logs, vec!["A"]);

    let (saved_state, meta, _) = checkpointer
        .load_state("thread_interrupt_before".to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved_state.value, "A");
    assert_eq!(meta.next_node, "B");

    // Resuming from B should bypass interrupt on the first step
    let final_resumed_state = graph
        .run(
            "thread_interrupt_before",
            saved_state,
            &meta.next_node,
            &checkpointer,
            None,
        )
        .await
        .unwrap();
    assert_eq!(final_resumed_state.value, "AB");
    assert_eq!(final_resumed_state.logs, vec!["A", "B"]);
}

#[tokio::test]
async fn test_declarative_interrupt_after() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node(
        "B",
        AppendNode {
            suffix: "B".to_string(),
        },
    );
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");

    graph.add_interrupt_after("A");

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let final_state = graph
        .run("thread_interrupt_after", state, "A", &checkpointer, None)
        .await
        .unwrap();

    assert_eq!(final_state.value, "A");
    assert_eq!(final_state.logs, vec!["A"]);

    let (saved_state, meta, _) = checkpointer
        .load_state("thread_interrupt_after".to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved_state.value, "A");
    assert_eq!(meta.next_node, "B");
}

#[tokio::test]
async fn test_event_streaming() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node(
        "B",
        AppendNode {
            suffix: "B".to_string(),
        },
    );
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");

    let mut rx = graph.subscribe();
    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let _final_state = graph
        .run("thread_events", state, "A", &checkpointer, None)
        .await
        .unwrap();

    let mut events = vec![];
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }

    assert!(events.len() >= 3);

    let has_started_a = events
        .iter()
        .any(|e| matches!(e, GraphEvent::NodeStarted { node_name, .. } if node_name == "A"));
    assert!(has_started_a);

    let has_finished_a = events
        .iter()
        .any(|e| matches!(e, GraphEvent::NodeFinished { node_name, .. } if node_name == "A"));
    assert!(has_finished_a);

    let has_graph_finished = events.iter().any(|e| matches!(e, GraphEvent::GraphFinished { .. }));
    assert!(has_graph_finished);
}

#[tokio::test]
async fn test_durable_dag_restart_recovery() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node(
        "B",
        AppendNode {
            suffix: "B".to_string(),
        },
    );
    graph.add_node(
        "C",
        AppendNode {
            suffix: "C".to_string(),
        },
    );

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();
    let id_c = Uuid::new_v4();

    graph.add_interrupt_before("C");

    let mut dag = DAG {
        id: Uuid::new_v4(),
        nodes: vec![
            DAGNode {
                id: id_a,
                step_type: "A".to_string(),
                depends_on: vec![],
                status: NodeStatus::Pending,
            },
            DAGNode {
                id: id_b,
                step_type: "B".to_string(),
                depends_on: vec![id_a],
                status: NodeStatus::Pending,
            },
            DAGNode {
                id: id_c,
                step_type: "C".to_string(),
                depends_on: vec![id_b],
                status: NodeStatus::Pending,
            },
        ],
        created_at: chrono::Utc::now(),
    };

    let intermediate_state = graph
        .run_dag("thread_durable", &mut dag, state, &checkpointer, None, 0)
        .await
        .unwrap();

    assert_eq!(intermediate_state.value, "AB");
    assert_eq!(dag.nodes[0].status, NodeStatus::Done);
    assert_eq!(dag.nodes[1].status, NodeStatus::Done);
    assert_eq!(dag.nodes[2].status, NodeStatus::Pending);

    let mut fresh_dag = DAG {
        id: Uuid::new_v4(),
        nodes: vec![
            DAGNode {
                id: id_a,
                step_type: "A".to_string(),
                depends_on: vec![],
                status: NodeStatus::Pending,
            },
            DAGNode {
                id: id_b,
                step_type: "B".to_string(),
                depends_on: vec![id_a],
                status: NodeStatus::Pending,
            },
            DAGNode {
                id: id_c,
                step_type: "C".to_string(),
                depends_on: vec![id_b],
                status: NodeStatus::Pending,
            },
        ],
        created_at: chrono::Utc::now(),
    };

    let (loaded_state, meta, saved_dag_opt) = checkpointer
        .load_state("thread_durable".to_string())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(loaded_state.value, "AB");
    assert_eq!(meta.next_node, "C");
    assert!(saved_dag_opt.is_some());

    let saved_dag = saved_dag_opt.unwrap();
    fresh_dag.restore_statuses(&saved_dag);

    assert_eq!(fresh_dag.nodes[0].status, NodeStatus::Done);
    assert_eq!(fresh_dag.nodes[1].status, NodeStatus::Done);
    assert_eq!(fresh_dag.nodes[2].status, NodeStatus::Pending);

    let final_resumed_state = graph
        .run_dag("thread_durable", &mut fresh_dag, loaded_state, &checkpointer, None, 0)
        .await
        .unwrap();

    assert_eq!(final_resumed_state.value, "ABC");
    assert_eq!(fresh_dag.nodes[2].status, NodeStatus::Done);
}

// ==========================================
// FIRST-CLASS RESUMPTION & FAILURE MODE TESTS
// ==========================================

#[tokio::test]
async fn test_first_class_resumption_api() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node("B", YieldNode);
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    // 1. Initial sequential execution halts at B
    let res = graph
        .run("thread_resume_api", state, "A", &checkpointer, None)
        .await
        .unwrap();
    assert_eq!(res.value, "A");

    // 2. Resume using the first-class resume API, substituting B with a successful node
    let mut resume_graph = Graph::new();
    resume_graph.add_node(
        "B",
        AppendNode {
            suffix: "B".to_string(),
        },
    );
    resume_graph.add_edge("B", "__END__");

    let final_state = resume_graph
        .resume("thread_resume_api", &checkpointer, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_state.value, "AB");
}

struct RetryNode {
    attempts: std::sync::Arc<std::sync::atomic::AtomicU8>,
}

#[async_trait]
impl Node<TestState> for RetryNode {
    async fn call(&self, _ctx: NodeContext, mut state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        let prev = self.attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        state.logs.push(format!("attempt_{}", prev));
        if prev < 2 {
            Err(GraphError::Retryable("try again".to_string()))
        } else {
            state.value = "success".to_string();
            Ok(NodeOutput::bare(state))
        }
    }
}

#[tokio::test]
async fn test_sequential_retry_success() {
    let mut graph = Graph::new();
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));
    graph.add_node(
        "A",
        RetryNode {
            attempts: attempts.clone(),
        },
    );

    let policy = RetryPolicy {
        max_attempts: 3,
        base_delay_ms: 1,
        jitter: false,
        ..Default::default()
    };
    graph.set_retry_policy(policy);

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let final_state = graph
        .run("thread_retry_seq", state, "A", &checkpointer, None)
        .await
        .unwrap();
    assert_eq!(final_state.value, "success");
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 3);
}

#[tokio::test]
async fn test_sequential_retry_failure() {
    let mut graph = Graph::new();
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));
    graph.add_node(
        "A",
        RetryNode {
            attempts: attempts.clone(),
        },
    );

    let policy = RetryPolicy {
        max_attempts: 2,
        base_delay_ms: 1,
        jitter: false,
        ..Default::default()
    };
    graph.set_retry_policy(policy);

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let err = graph
        .run("thread_retry_fail", state, "A", &checkpointer, None)
        .await
        .unwrap_err();
    assert!(matches!(err, TakelnError::ExecutionError(_)));
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 2);
}

struct ExpensiveNode;

#[async_trait]
impl Node<TestState> for ExpensiveNode {
    async fn call(&self, _ctx: NodeContext, state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        Ok(NodeOutput {
            state,
            event: None,
            meta: NodeMeta {
                cost_eur: Some(10.0),
                ..Default::default()
            },
        })
    }
}

#[tokio::test]
async fn test_budget_exceeded() {
    let mut graph = Graph::new();
    graph.add_node("A", ExpensiveNode);
    graph.set_budget_eur(5.0);

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let err = graph
        .run("thread_budget", state, "A", &checkpointer, None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, TakelnError::BudgetExceeded { spent_eur, limit_eur } if spent_eur == 10.0 && limit_eur == 5.0)
    );
}

struct DelayedNode;

#[async_trait]
impl Node<TestState> for DelayedNode {
    async fn call(&self, _ctx: NodeContext, state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::test]
async fn test_cancellation_sequential() {
    let mut graph = Graph::new();
    graph.add_node("A", DelayedNode);

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();
    let token = tokio_util::sync::CancellationToken::new();

    let token_clone = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        token_clone.cancel();
    });

    let result = graph.run("thread_cancel", state, "A", &checkpointer, Some(token)).await;
    assert!(result.is_ok());
    let (_, meta, _) = checkpointer
        .load_state("thread_cancel".to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(meta.next_node, "A");
}

#[tokio::test]
async fn test_missing_node_error() {
    let graph = Graph::new();
    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let err = graph
        .run("thread_missing", state, "NonExistent", &checkpointer, None)
        .await
        .unwrap_err();
    assert!(matches!(err, TakelnError::NodeNotFound(name) if name == "NonExistent"));
}

struct OverrideNode {
    val: String,
}

#[async_trait]
impl Node<TestState> for OverrideNode {
    async fn call(&self, _ctx: NodeContext, mut state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        state.value = self.val.clone();
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::test]
async fn test_deterministic_merge_order() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        OverrideNode {
            val: "ValA".to_string(),
        },
    );
    graph.add_node(
        "B",
        OverrideNode {
            val: "ValB".to_string(),
        },
    );

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();

    let mut dag = DAG {
        id: Uuid::new_v4(),
        nodes: vec![
            DAGNode {
                id: id_a,
                step_type: "A".to_string(),
                depends_on: vec![],
                status: NodeStatus::Pending,
            },
            DAGNode {
                id: id_b,
                step_type: "B".to_string(),
                depends_on: vec![],
                status: NodeStatus::Pending,
            },
        ],
        created_at: chrono::Utc::now(),
    };

    let final_state = graph
        .run_dag("thread_merge_det", &mut dag, state, &checkpointer, None, 0)
        .await
        .unwrap();
    assert_eq!(final_state.value, "ValB");
}

// ==========================================
// v0.4.0 EXECUTION SEMANTICS TESTS
// ==========================================

#[tokio::test]
async fn test_per_node_retry_override() {
    // Graph has max_attempts: 1 (no retries at graph level)
    // Node "A" has per-node max_attempts: 3
    // RetryNode succeeds on attempt 3 -> per-node override wins
    let mut graph = Graph::new();
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));

    graph.add_node_with_config(
        "A",
        RetryNode {
            attempts: attempts.clone(),
        },
        NodeConfig {
            retry_policy: Some(RetryPolicy {
                max_attempts: 3,
                base_delay_ms: 1,
                jitter: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    graph.add_edge("A", "__END__");

    // Graph-level: no retries
    graph.set_retry_policy(RetryPolicy {
        max_attempts: 1,
        base_delay_ms: 1,
        jitter: false,
        ..Default::default()
    });

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let final_state = graph
        .run("thread_per_node_retry", state, "A", &checkpointer, None)
        .await
        .unwrap();
    assert_eq!(final_state.value, "success");
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 3);
}

struct SlowNode {
    delay_ms: u64,
}

#[async_trait]
impl Node<TestState> for SlowNode {
    async fn call(&self, _ctx: NodeContext, mut state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
        state.value.push_str("slow");
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::test]
async fn test_per_node_timeout() {
    let mut graph = Graph::new();
    graph.add_node_with_config(
        "A",
        SlowNode { delay_ms: 200 },
        NodeConfig {
            timeout: Some(std::time::Duration::from_millis(50)),
            ..Default::default()
        },
    );
    graph.add_edge("A", "__END__");

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let err = graph
        .run("thread_timeout", state, "A", &checkpointer, None)
        .await
        .unwrap_err();
    assert!(matches!(err, TakelnError::ExecutionError(msg) if msg.contains("timed out")));
}

struct FatalNode;

#[async_trait]
impl Node<TestState> for FatalNode {
    async fn call(&self, _ctx: NodeContext, _state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        Err(GraphError::Fatal("boom".to_string()))
    }
}

#[tokio::test]
async fn test_wave_continue_on_error() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node("B", FatalNode);
    graph.set_wave_failure_policy(WaveFailurePolicy::ContinueOnError);

    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let id_a = Uuid::new_v4();
    let id_b = Uuid::new_v4();

    let mut dag = DAG {
        id: Uuid::new_v4(),
        nodes: vec![
            DAGNode {
                id: id_a,
                step_type: "A".to_string(),
                depends_on: vec![],
                status: NodeStatus::Pending,
            },
            DAGNode {
                id: id_b,
                step_type: "B".to_string(),
                depends_on: vec![],
                status: NodeStatus::Pending,
            },
        ],
        created_at: chrono::Utc::now(),
    };

    let err = graph
        .run_dag("thread_continue", &mut dag, state, &checkpointer, None, 0)
        .await
        .unwrap_err();

    match err {
        TakelnError::PartialWaveFailure { succeeded, failed } => {
            assert!(succeeded.contains(&"A".to_string()));
            assert_eq!(failed.len(), 1);
            assert_eq!(failed[0].0, "B");
        }
        other => panic!("Expected PartialWaveFailure, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_event_sequence_numbers() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node(
        "B",
        AppendNode {
            suffix: "B".to_string(),
        },
    );
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");

    let mut rx = graph.subscribe();
    let checkpointer = InMemoryCheckpointer::<TestState>::new();
    let state = TestState::default();

    let _final_state = graph
        .run("thread_seq_nums", state, "A", &checkpointer, None)
        .await
        .unwrap();

    let mut events = vec![];
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }

    // Should have at least 5 events: start A, finish A, start B, finish B, graph finished
    assert!(events.len() >= 5);

    let seq_numbers: Vec<u64> = events
        .iter()
        .map(|e| match e {
            GraphEvent::NodeStarted { sequence_number, .. } => *sequence_number,
            GraphEvent::NodeFinished { sequence_number, .. } => *sequence_number,
            GraphEvent::GraphFinished { sequence_number, .. } => *sequence_number,
            _ => panic!("Unexpected event variant"),
        })
        .collect();

    // Verify strictly monotonically increasing
    for pair in seq_numbers.windows(2) {
        assert!(
            pair[0] < pair[1],
            "Sequence numbers must be strictly increasing: {} >= {}",
            pair[0],
            pair[1]
        );
    }
}

// ==========================================
// v0.5.0 OBSERVABILITY TESTS
// ==========================================

#[tokio::test]
async fn test_tracing_emitter_compiles() {
    // Verify TracingEmitter works as a SpanEmitter
    let mut graph = Graph::<TestState>::with_emitter(std::sync::Arc::new(takeln::TracingEmitter));
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_edge("A", "__END__");
    let cp = InMemoryCheckpointer::new();
    let state = graph
        .run("thread_tracing", TestState::default(), "A", &cp, None)
        .await
        .unwrap();
    assert_eq!(state.value, "A");
}

#[tokio::test]
async fn test_metrics_hook_fires() {
    use std::sync::atomic::{AtomicU32, Ordering};

    struct CountingHook {
        node_count: AtomicU32,
        graph_count: AtomicU32,
        checkpoint_count: AtomicU32,
    }

    impl takeln::MetricsHook for CountingHook {
        fn on_node_complete(&self, _: &str, _: u64, _: SpanStatus) {
            self.node_count.fetch_add(1, Ordering::Relaxed);
        }
        fn on_graph_complete(&self, _: &str, _: f64, _: u64) {
            self.graph_count.fetch_add(1, Ordering::Relaxed);
        }
        fn on_checkpoint_saved(&self, _: &str, _: &str) {
            self.checkpoint_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    let hook = std::sync::Arc::new(CountingHook {
        node_count: AtomicU32::new(0),
        graph_count: AtomicU32::new(0),
        checkpoint_count: AtomicU32::new(0),
    });

    let mut graph = Graph::new();
    graph.set_metrics_hook(hook.clone());
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node(
        "B",
        AppendNode {
            suffix: "B".to_string(),
        },
    );
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");

    let cp = InMemoryCheckpointer::new();
    let _ = graph
        .run("thread_metrics", TestState::default(), "A", &cp, None)
        .await
        .unwrap();

    assert_eq!(hook.node_count.load(Ordering::Relaxed), 2);
    assert_eq!(hook.graph_count.load(Ordering::Relaxed), 1);
    // A->B->END means 2 save_state calls (after A, after B)
    assert!(hook.checkpoint_count.load(Ordering::Relaxed) >= 2);
}

#[tokio::test]
async fn test_execution_history() {
    let mut graph = Graph::new();
    graph.add_node(
        "A",
        AppendNode {
            suffix: "A".to_string(),
        },
    );
    graph.add_node(
        "B",
        AppendNode {
            suffix: "B".to_string(),
        },
    );
    graph.add_edge("A", "B");
    graph.add_edge("B", "__END__");

    let cp = InMemoryCheckpointer::new();
    let _ = graph
        .run("thread_history", TestState::default(), "A", &cp, None)
        .await
        .unwrap();

    let history = graph.execution_history().await;
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].node_name, "A");
    assert_eq!(history[1].node_name, "B");
    assert_eq!(history[0].status, "success");
    assert_eq!(history[1].status, "success");
}

// ==========================================
// v0.6.0 ERGONOMICS TESTS
// ==========================================

#[tokio::test]
async fn test_dag_builder() {
    let dag = takeln::DAG::builder()
        .node("fetch", &[])
        .node("parse", &["fetch"])
        .node("score", &["parse"])
        .node("rank", &["parse"])
        .node("merge", &["score", "rank"])
        .build()
        .unwrap();
    assert_eq!(dag.nodes.len(), 5);
    // fetch has no deps
    assert!(dag.nodes[0].depends_on.is_empty());
    // parse depends on fetch
    assert_eq!(dag.nodes[1].depends_on.len(), 1);
    // merge depends on score and rank
    assert_eq!(dag.nodes[4].depends_on.len(), 2);
}

#[tokio::test]
async fn test_dag_builder_missing_dep() {
    let result = takeln::DAG::builder().node("A", &["nonexistent"]).build();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not found"));
}

#[tokio::test]
async fn test_dag_builder_cycle_detection() {
    let result = takeln::DAG::builder().node("A", &["B"]).node("B", &["A"]).build();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Cycle"));
}

#[tokio::test]
async fn test_graph_builder() {
    let graph = Graph::builder()
        .node(
            "A",
            AppendNode {
                suffix: "A".to_string(),
            },
        )
        .node(
            "B",
            AppendNode {
                suffix: "B".to_string(),
            },
        )
        .edge("A", "B")
        .edge("B", "__END__")
        .build();

    let cp = InMemoryCheckpointer::new();
    let state = graph
        .run("thread_builder", TestState::default(), "A", &cp, None)
        .await
        .unwrap();
    assert_eq!(state.value, "AB");
}

#[tokio::test]
async fn test_fn_node() {
    let mut graph = Graph::<TestState>::new();
    graph.add_simple_fn_node("transform", |mut state: TestState| async move {
        state.value.push_str("_fn");
        Ok(NodeOutput::bare(state))
    });
    graph.add_edge("transform", "__END__");

    let cp = InMemoryCheckpointer::new();
    let state = graph
        .run(
            "thread_fn",
            TestState {
                value: "start".to_string(),
                ..Default::default()
            },
            "transform",
            &cp,
            None,
        )
        .await
        .unwrap();
    assert_eq!(state.value, "start_fn");
}
