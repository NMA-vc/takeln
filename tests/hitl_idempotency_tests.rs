use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use takeln::{
    Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput, ResumeContext, ResumeMode, YieldRequest,
};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct TestState {
    value: String,
}

struct YieldOnceNode {
    interrupt_id: String,
    call_count: Arc<AtomicUsize>,
    execution_ids: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Node<TestState> for YieldOnceNode {
    async fn call(&self, ctx: NodeContext, mut state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        let count = self.call_count.fetch_add(1, Ordering::SeqCst);
        self.execution_ids.lock().unwrap().push(ctx.execution_id.clone());
        if count == 0 {
            return Err(GraphError::Yield(
                YieldRequest::new(self.interrupt_id.clone(), "Approve this".to_string())
                    .with_resume_mode(ResumeMode::ReEntry),
            ));
        }
        state.value.push_str("_resumed");
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::test]
async fn test_resume_double_tap_idempotent() {
    let mut graph = Graph::new();
    let call_count = Arc::new(AtomicUsize::new(0));
    let execution_ids = Arc::new(Mutex::new(Vec::new()));
    graph.add_node(
        "yield_node",
        YieldOnceNode {
            interrupt_id: "int_123".to_string(),
            call_count: call_count.clone(),
            execution_ids: execution_ids.clone(),
        },
    );
    graph.add_edge("yield_node", "__END__");

    let checkpointer = InMemoryCheckpointer::new();
    let state = TestState::default();

    let _ = graph.run("thread_1", state, "yield_node", &checkpointer, None).await;

    let state_after_resume = graph
        .resume_with_input(
            "thread_1",
            "int_123",
            json!("yes"),
            ResumeContext::new("alice"),
            &checkpointer,
            None,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(state_after_resume.value, "_resumed");
    assert_eq!(call_count.load(Ordering::SeqCst), 2);

    let state_after_second_resume = graph
        .resume_with_input(
            "thread_1",
            "int_123",
            json!("yes"),
            ResumeContext::new("alice"),
            &checkpointer,
            None,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(state_after_second_resume.value, "_resumed");
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_resume_stale_interrupt_rejected() {
    let mut graph = Graph::new();
    let call_count = Arc::new(AtomicUsize::new(0));
    let execution_ids = Arc::new(Mutex::new(Vec::new()));
    graph.add_node(
        "yield_node",
        YieldOnceNode {
            interrupt_id: "int_123".to_string(),
            call_count: call_count.clone(),
            execution_ids: execution_ids.clone(),
        },
    );
    graph.add_edge("yield_node", "__END__");

    let checkpointer = InMemoryCheckpointer::new();
    let state = TestState::default();

    let _ = graph.run("thread_2", state, "yield_node", &checkpointer, None).await;

    let err = graph
        .resume_with_input(
            "thread_2",
            "wrong_id",
            json!("yes"),
            ResumeContext::default(),
            &checkpointer,
            None,
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("Invalid resume"));

    let _ = graph
        .resume_with_input(
            "thread_2",
            "int_123",
            json!("yes"),
            ResumeContext::default(),
            &checkpointer,
            None,
        )
        .await
        .unwrap();

    let err2 = graph
        .resume_with_input(
            "thread_2",
            "wrong_id",
            json!("yes"),
            ResumeContext::default(),
            &checkpointer,
            None,
        )
        .await
        .unwrap_err();
    assert!(err2.to_string().contains("Nothing to resume") || err2.to_string().contains("Invalid resume"));
}

#[tokio::test]
async fn test_resume_concurrent_single_winner() {
    let mut graph = Graph::new();
    let call_count = Arc::new(AtomicUsize::new(0));
    let execution_ids = Arc::new(Mutex::new(Vec::new()));
    graph.add_node(
        "yield_node",
        YieldOnceNode {
            interrupt_id: "int_123".to_string(),
            call_count: call_count.clone(),
            execution_ids: execution_ids.clone(),
        },
    );
    graph.add_edge("yield_node", "__END__");

    let checkpointer = Arc::new(InMemoryCheckpointer::new());
    let state = TestState::default();

    let graph = Arc::new(graph);
    let _ = graph.run("thread_3", state, "yield_node", &*checkpointer, None).await;

    let checkpointer_clone = checkpointer.clone();
    let graph_clone = graph.clone();
    let handle1 = tokio::spawn(async move {
        graph_clone
            .resume_with_input(
                "thread_3",
                "int_123",
                json!("yes"),
                ResumeContext::new("bob"),
                &*checkpointer_clone,
                None,
            )
            .await
    });

    let checkpointer_clone2 = checkpointer.clone();
    let graph_clone2 = graph.clone();
    let handle2 = tokio::spawn(async move {
        graph_clone2
            .resume_with_input(
                "thread_3",
                "int_123",
                json!("yes"),
                ResumeContext::new("bob"),
                &*checkpointer_clone2,
                None,
            )
            .await
    });

    let res1 = handle1.await.unwrap().unwrap().unwrap();
    let res2 = handle2.await.unwrap().unwrap().unwrap();

    assert_eq!(res1.value, "_resumed");
    assert_eq!(res2.value, "_resumed");
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_execution_id_stable_across_reentry() {
    let mut graph = Graph::new();
    let call_count = Arc::new(AtomicUsize::new(0));
    let execution_ids = Arc::new(Mutex::new(Vec::new()));
    graph.add_node(
        "yield_node",
        YieldOnceNode {
            interrupt_id: "int_123".to_string(),
            call_count: call_count.clone(),
            execution_ids: execution_ids.clone(),
        },
    );
    graph.add_edge("yield_node", "__END__");

    let checkpointer = InMemoryCheckpointer::new();
    let state = TestState::default();

    let _ = graph.run("thread_4", state, "yield_node", &checkpointer, None).await;

    let _final_state = graph
        .resume_with_input(
            "thread_4",
            "int_123",
            json!("yes"),
            ResumeContext::new("charlie"),
            &checkpointer,
            None,
        )
        .await
        .unwrap()
        .unwrap();

    {
        let ids = execution_ids.lock().unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], ids[1]);
    }

    // Verify observability / execution history (Issue #3)
    let history = graph.execution_history().await;
    let resume_events: Vec<_> = history.iter().filter(|r| r.status == "resumed").collect();
    assert_eq!(resume_events.len(), 1);
    assert_eq!(resume_events[0].node_name, "yield_node");
    assert_eq!(resume_events[0].actor.as_deref(), Some("charlie"));
    assert_eq!(
        resume_events[0].response_hash.as_deref(),
        Some(takeln::hitl::compute_response_hash(&json!("yes"))).as_deref()
    );
}
