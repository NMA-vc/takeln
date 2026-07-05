use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use takeln::{
    CheckpointStatus, Checkpointer, Graph, GraphError, InMemoryCheckpointer, Node, NodeContext, NodeOutput,
    ResumeContext, ResumeMode, YieldRequest,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default, PartialEq)]
struct TestState {
    value: String,
}

struct ConcurrentTestNode {
    interrupt_id: String,
    call_count: Arc<AtomicUsize>,
    should_fail: Arc<AtomicBool>,
    delay_ms: u64,
}

#[async_trait]
impl Node<TestState> for ConcurrentTestNode {
    async fn call(&self, _ctx: NodeContext, mut state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        let count = self.call_count.fetch_add(1, Ordering::SeqCst);
        if count == 0 {
            return Err(GraphError::Yield(
                YieldRequest::new(self.interrupt_id.clone(), "Approve this".to_string())
                    .with_resume_mode(ResumeMode::ReEntry),
            ));
        }
        if self.should_fail.load(Ordering::SeqCst) {
            return Err(GraphError::Fatal("Simulated failure".to_string()));
        }
        if self.delay_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(self.delay_ms)).await;
        }
        state.value.push_str("_resumed");
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::test]
async fn test_resume_failure_recovery() {
    let mut graph = Graph::new();
    let call_count = Arc::new(AtomicUsize::new(0));
    let should_fail = Arc::new(AtomicBool::new(true));
    graph.add_node(
        "node",
        ConcurrentTestNode {
            interrupt_id: "int_123".to_string(),
            call_count: call_count.clone(),
            should_fail: should_fail.clone(),
            delay_ms: 0,
        },
    );
    graph.add_edge("node", "__END__");

    let checkpointer = InMemoryCheckpointer::new();
    let state = TestState::default();

    // 1. Run initially to yield
    let _ = graph.run("thread_1", state, "node", &checkpointer, None).await;

    // 2. Call resume_with_input, which should fail because should_fail is true
    let resume_res = graph
        .resume_with_input(
            "thread_1",
            "int_123",
            serde_json::json!("yes"),
            ResumeContext::new("alice"),
            &checkpointer,
            None,
        )
        .await;

    assert!(resume_res.is_err());

    // 3. Verify status reverted back to Yielded, and claimed_interrupt is None
    let loaded = checkpointer.load_state("thread_1".to_string()).await.unwrap().unwrap();
    assert_eq!(loaded.1.status, CheckpointStatus::Yielded);
    assert_eq!(loaded.1.claimed_interrupt, None);

    // 4. Reset failure flag and try again
    should_fail.store(false, Ordering::SeqCst);
    let resume_res2 = graph
        .resume_with_input(
            "thread_1",
            "int_123",
            serde_json::json!("yes"),
            ResumeContext::new("alice"),
            &checkpointer,
            None,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(resume_res2.value, "_resumed");
}

#[tokio::test]
async fn test_concurrent_resume_in_progress() {
    let mut graph = Graph::new();
    let call_count = Arc::new(AtomicUsize::new(0));
    let should_fail = Arc::new(AtomicBool::new(false));
    graph.add_node(
        "node",
        ConcurrentTestNode {
            interrupt_id: "int_123".to_string(),
            call_count: call_count.clone(),
            should_fail: should_fail.clone(),
            delay_ms: 200,
        },
    );
    graph.add_edge("node", "__END__");

    let checkpointer = InMemoryCheckpointer::new();
    let state = TestState::default();

    // 1. Run initially to yield
    let _ = graph.run("thread_1", state, "node", &checkpointer, None).await;

    // 2. Launch resume in the background
    let graph_arc = Arc::new(graph);
    let checkpointer_arc = Arc::new(checkpointer);
    let cp_clone = checkpointer_arc.clone();
    let g_clone = graph_arc.clone();
    let handle = tokio::spawn(async move {
        g_clone
            .resume_with_input(
                "thread_1",
                "int_123",
                serde_json::json!("yes"),
                ResumeContext::new("alice"),
                &*cp_clone,
                None,
            )
            .await
    });

    // Give the background task a moment to start and claim the interrupt
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // 3. Try to resume concurrently
    let concurrent_res = graph_arc
        .resume_with_input(
            "thread_1",
            "int_123",
            serde_json::json!("yes"),
            ResumeContext::new("bob"),
            &*checkpointer_arc,
            None,
        )
        .await;

    // 4. Verify it returned an ExecutionError indicating "Resume in progress"
    assert!(concurrent_res.is_err());
    let err = concurrent_res.unwrap_err();
    assert!(matches!(err, takeln::TakelnError::ExecutionError(msg) if msg.contains("Resume in progress")));

    // 5. Let the original resume finish
    let final_state = handle.await.unwrap().unwrap().unwrap();
    assert_eq!(final_state.value, "_resumed");
}

#[tokio::test]
async fn test_crashed_resume_recovery() {
    let mut graph = Graph::new();
    let call_count = Arc::new(AtomicUsize::new(1));
    let should_fail = Arc::new(AtomicBool::new(false));
    graph.add_node(
        "node",
        ConcurrentTestNode {
            interrupt_id: "int_123".to_string(),
            call_count: call_count.clone(),
            should_fail: should_fail.clone(),
            delay_ms: 0,
        },
    );
    graph.add_edge("node", "__END__");

    let checkpointer = InMemoryCheckpointer::new();

    // 1. Manually insert a checkpoint with Running status and claimed_interrupt = Some("int_123")
    // representing a crash mid-resume.
    let state = TestState::default();
    let yield_request =
        YieldRequest::new("int_123".to_string(), "Approve".to_string()).with_resume_mode(ResumeMode::ReEntry);

    // Save a checkpoint that looks like it's Running/claimed
    checkpointer
        .save_state(
            "thread_1".to_string(),
            state,
            "node".to_string(),
            None,
            CheckpointStatus::Running,
            Some(yield_request),
            Some("int_123".to_string()),
            None,
        )
        .await
        .unwrap();

    // 2. Call graph.resume() to recover
    // This should trigger the crash recovery policy (ResetToPending by default)
    // which re-executes "node" passing the claimed_interrupt.
    let recovered_state = graph.resume("thread_1", &checkpointer, None).await.unwrap().unwrap();

    // Verify it succeeded and correctly re-executed
    assert_eq!(recovered_state.value, "_resumed");

    // Verify history now records the resolved_interrupt
    let loaded = checkpointer.load_state("thread_1".to_string()).await.unwrap().unwrap();
    assert_eq!(loaded.1.status, CheckpointStatus::Complete);
    assert_eq!(loaded.1.resolved_interrupt, Some("int_123".to_string()));
}
