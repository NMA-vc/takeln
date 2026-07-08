use takeln::{CheckpointStatus, Checkpointer, CrashRecoveryPolicy, InMemoryCheckpointer, RetentionPolicy, TakelnError};

// ── Test State ──────────────────────────────────────────────────────────────

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug, PartialEq)]
struct TestState {
    value: String,
}

// ── Retention / Compaction ──────────────────────────────────────────────────

#[tokio::test]
async fn test_retention_keep_last_3() {
    let cp = InMemoryCheckpointer::<TestState>::new();
    let tid = "thread_retain".to_string();

    // Save 10 checkpoints
    for i in 0..10 {
        cp.save_state(
            tid.clone(),
            TestState {
                value: format!("v{}", i),
            },
            format!("Node{}", i),
            None,
            CheckpointStatus::Complete,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    }

    let all = cp.list_checkpoints(tid.clone()).await.unwrap();
    assert_eq!(all.len(), 10);

    // Retain only the last 3
    let deleted = cp
        .delete_checkpoints(tid.clone(), RetentionPolicy::KeepLast(3))
        .await
        .unwrap();
    assert_eq!(deleted, 7);

    let remaining = cp.list_checkpoints(tid.clone()).await.unwrap();
    assert_eq!(remaining.len(), 3);

    // The latest 3 should survive (Node7, Node8, Node9)
    assert_eq!(remaining[0].next_node, "Node7");
    assert_eq!(remaining[1].next_node, "Node8");
    assert_eq!(remaining[2].next_node, "Node9");
}

#[tokio::test]
async fn test_retention_keep_all() {
    let cp = InMemoryCheckpointer::<TestState>::new();
    let tid = "thread_keep_all".to_string();

    for i in 0..5 {
        cp.save_state(
            tid.clone(),
            TestState {
                value: format!("v{}", i),
            },
            format!("Node{}", i),
            None,
            CheckpointStatus::Complete,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    }

    let deleted = cp
        .delete_checkpoints(tid.clone(), RetentionPolicy::KeepAll)
        .await
        .unwrap();
    assert_eq!(deleted, 0);

    let remaining = cp.list_checkpoints(tid.clone()).await.unwrap();
    assert_eq!(remaining.len(), 5);
}

#[tokio::test]
async fn test_retention_keep_last_more_than_available() {
    let cp = InMemoryCheckpointer::<TestState>::new();
    let tid = "thread_retain_more".to_string();

    cp.save_state(
        tid.clone(),
        TestState {
            value: "v0".to_string(),
        },
        "Node0".to_string(),
        None,
        CheckpointStatus::Complete,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let deleted = cp
        .delete_checkpoints(tid.clone(), RetentionPolicy::KeepLast(10))
        .await
        .unwrap();
    assert_eq!(deleted, 0);

    let remaining = cp.list_checkpoints(tid.clone()).await.unwrap();
    assert_eq!(remaining.len(), 1);
}

#[tokio::test]
async fn test_delete_nonexistent_thread() {
    let cp = InMemoryCheckpointer::<TestState>::new();

    let deleted = cp
        .delete_checkpoints("no_such_thread".to_string(), RetentionPolicy::KeepLast(1))
        .await
        .unwrap();
    assert_eq!(deleted, 0);
}

// ── Checkpoint Status Metadata ─────────────────────────────────────────────

#[tokio::test]
async fn test_checkpoint_status_persists() {
    let cp = InMemoryCheckpointer::<TestState>::new();
    let tid = "thread_status".to_string();

    cp.save_state(
        tid.clone(),
        TestState {
            value: "running".to_string(),
        },
        "NodeA".to_string(),
        None,
        CheckpointStatus::Running,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let (_, meta, _) = cp.load_state(tid.clone()).await.unwrap().unwrap();
    assert_eq!(meta.status, CheckpointStatus::Running);
    assert_eq!(meta.thread_id, tid);
    assert_eq!(meta.next_node, "NodeA");

    // Save a yielded checkpoint
    cp.save_state(
        tid.clone(),
        TestState {
            value: "yielded".to_string(),
        },
        "NodeB".to_string(),
        None,
        CheckpointStatus::Yielded,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let (_, meta2, _) = cp.load_state(tid.clone()).await.unwrap().unwrap();
    assert_eq!(meta2.status, CheckpointStatus::Yielded);
}

#[tokio::test]
async fn test_checkpoint_meta_in_list() {
    let cp = InMemoryCheckpointer::<TestState>::new();
    let tid = "thread_meta_list".to_string();

    cp.save_state(
        tid.clone(),
        TestState::default(),
        "A".to_string(),
        None,
        CheckpointStatus::Complete,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    cp.save_state(
        tid.clone(),
        TestState::default(),
        "B".to_string(),
        None,
        CheckpointStatus::Interrupted,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let list = cp.list_checkpoints(tid.clone()).await.unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].status, CheckpointStatus::Complete);
    assert_eq!(list[1].status, CheckpointStatus::Interrupted);
}

// ── Crash Recovery (via Graph) ─────────────────────────────────────────────

use async_trait::async_trait;
use takeln::{Graph, GraphError, Node, NodeContext, NodeOutput};

struct AppendNode {
    suffix: String,
}

#[async_trait]
impl Node<TestState> for AppendNode {
    async fn call(&self, _ctx: NodeContext, mut state: TestState) -> Result<NodeOutput<TestState>, GraphError> {
        state.value.push_str(&self.suffix);
        Ok(NodeOutput::bare(state))
    }
}

#[tokio::test]
async fn test_crash_recovery_reset_to_pending() {
    let cp = InMemoryCheckpointer::<TestState>::new();
    let tid = "thread_crash_reset".to_string();

    // Simulate a crash: save checkpoint with Running status
    cp.save_state(
        tid.clone(),
        TestState {
            value: "before_crash".to_string(),
        },
        "B".to_string(),
        None,
        CheckpointStatus::Running,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    // Resume with ResetToPending policy (default)
    let mut graph = Graph::new();
    graph.add_node(
        "B",
        AppendNode {
            suffix: "_resumed".to_string(),
        },
    );
    graph.add_edge("B", "__END__");

    let result = graph.resume(&tid, &cp, None).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, "before_crash_resumed");
}

#[tokio::test]
async fn test_crash_recovery_fail_fast() {
    let cp = InMemoryCheckpointer::<TestState>::new();
    let tid = "thread_crash_fail".to_string();

    cp.save_state(
        tid.clone(),
        TestState {
            value: "before_crash".to_string(),
        },
        "B".to_string(),
        None,
        CheckpointStatus::Running,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let mut graph = Graph::new();
    graph.add_node(
        "B",
        AppendNode {
            suffix: "_resumed".to_string(),
        },
    );
    graph.add_edge("B", "__END__");
    graph.set_crash_recovery_policy(CrashRecoveryPolicy::FailFast);

    let result = graph.resume(&tid, &cp, None).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, TakelnError::ExecutionError(msg) if msg.contains("FailFast")));
}

#[tokio::test]
async fn test_crash_recovery_skip_and_continue() {
    let cp = InMemoryCheckpointer::<TestState>::new();
    let tid = "thread_crash_skip".to_string();

    cp.save_state(
        tid.clone(),
        TestState {
            value: "before_crash".to_string(),
        },
        "B".to_string(),
        None,
        CheckpointStatus::Running,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let mut graph = Graph::new();
    graph.add_node(
        "B",
        AppendNode {
            suffix: "_B".to_string(),
        },
    );
    graph.add_node(
        "C",
        AppendNode {
            suffix: "_C".to_string(),
        },
    );
    graph.add_edge("B", "C");
    graph.add_edge("C", "__END__");
    graph.set_crash_recovery_policy(CrashRecoveryPolicy::SkipAndContinue);

    let result = graph.resume(&tid, &cp, None).await.unwrap();
    assert!(result.is_some());
    // B was skipped, so only C appended
    assert_eq!(result.unwrap().value, "before_crash_C");
}

#[tokio::test]
async fn test_resume_complete_checkpoint_no_crash_recovery() {
    let cp = InMemoryCheckpointer::<TestState>::new();
    let tid = "thread_normal_resume".to_string();

    // Save a normal (Complete) checkpoint — crash recovery should NOT trigger
    cp.save_state(
        tid.clone(),
        TestState {
            value: "ok".to_string(),
        },
        "B".to_string(),
        None,
        CheckpointStatus::Complete,
        None,
        None,
        None,
    )
    .await
    .unwrap();

    let mut graph = Graph::new();
    graph.add_node(
        "B",
        AppendNode {
            suffix: "_done".to_string(),
        },
    );
    graph.add_edge("B", "__END__");
    // Even with FailFast, a Complete checkpoint should not trigger crash recovery
    graph.set_crash_recovery_policy(CrashRecoveryPolicy::FailFast);

    let result = graph.resume(&tid, &cp, None).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, "ok_done");
}

// ── Load from empty / nonexistent thread ───────────────────────────────────

#[tokio::test]
async fn test_load_nonexistent_thread() {
    let cp = InMemoryCheckpointer::<TestState>::new();

    let result = cp.load_state("nonexistent_thread".to_string()).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_load_version_nonexistent() {
    let cp = InMemoryCheckpointer::<TestState>::new();

    let result = cp
        .load_version("nonexistent_thread".to_string(), "nonexistent_id".to_string())
        .await
        .unwrap();
    assert!(result.is_none());
}

// ── Concurrent saves to same thread ────────────────────────────────────────

#[tokio::test]
async fn test_concurrent_saves_same_thread() {
    let cp = std::sync::Arc::new(InMemoryCheckpointer::<TestState>::new());
    let tid = "thread_concurrent".to_string();

    let mut handles = vec![];
    for i in 0..10 {
        let cp = cp.clone();
        let tid = tid.clone();
        handles.push(tokio::spawn(async move {
            cp.save_state(
                tid,
                TestState {
                    value: format!("v{}", i),
                },
                format!("Node{}", i),
                None,
                CheckpointStatus::Complete,
                None,
                None,
                None,
            )
            .await
            .unwrap()
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let all = cp.list_checkpoints(tid).await.unwrap();
    assert_eq!(all.len(), 10);
}
