#![cfg(feature = "sqlite")]

use takeln::{CheckpointStatus, Checkpointer, RetentionPolicy, SqliteCheckpointer, DAG};

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug, PartialEq)]
struct TestState {
    value: String,
}

#[tokio::test]
async fn test_sqlite_save_and_load() {
    let cp = SqliteCheckpointer::<TestState>::in_memory().unwrap();
    let state = TestState {
        value: "hello".to_string(),
    };

    let id = cp
        .save_state(
            "thread_1".to_string(),
            state.clone(),
            "NodeB".to_string(),
            None,
            CheckpointStatus::Complete,
        )
        .await
        .unwrap();

    let loaded = cp.load_state("thread_1".to_string()).await.unwrap().unwrap();
    assert_eq!(loaded.0.value, "hello");
    assert_eq!(loaded.1.next_node, "NodeB");
    assert_eq!(loaded.1.checkpoint_id, id);
}

#[tokio::test]
async fn test_sqlite_load_nonexistent() {
    let cp = SqliteCheckpointer::<TestState>::in_memory().unwrap();
    let result = cp.load_state("nonexistent".to_string()).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_sqlite_load_version() {
    let cp = SqliteCheckpointer::<TestState>::in_memory().unwrap();
    let state1 = TestState {
        value: "v1".to_string(),
    };
    let state2 = TestState {
        value: "v2".to_string(),
    };

    let id1 = cp
        .save_state(
            "t1".to_string(),
            state1,
            "A".to_string(),
            None,
            CheckpointStatus::Complete,
        )
        .await
        .unwrap();
    let _id2 = cp
        .save_state(
            "t1".to_string(),
            state2,
            "B".to_string(),
            None,
            CheckpointStatus::Complete,
        )
        .await
        .unwrap();

    // Load latest should be v2
    let latest = cp.load_state("t1".to_string()).await.unwrap().unwrap();
    assert_eq!(latest.0.value, "v2");

    // Load specific version should be v1
    let v1 = cp.load_version("t1".to_string(), id1).await.unwrap().unwrap();
    assert_eq!(v1.0.value, "v1");
}

#[tokio::test]
async fn test_sqlite_list_checkpoints() {
    let cp = SqliteCheckpointer::<TestState>::in_memory().unwrap();

    for i in 0..5 {
        cp.save_state(
            "t1".to_string(),
            TestState {
                value: format!("v{}", i),
            },
            "A".to_string(),
            None,
            CheckpointStatus::Complete,
        )
        .await
        .unwrap();
    }

    let list = cp.list_checkpoints("t1".to_string()).await.unwrap();
    assert_eq!(list.len(), 5);
}

#[tokio::test]
async fn test_sqlite_retention_keep_last() {
    let cp = SqliteCheckpointer::<TestState>::in_memory().unwrap();

    for i in 0..5 {
        cp.save_state(
            "t1".to_string(),
            TestState {
                value: format!("v{}", i),
            },
            "A".to_string(),
            None,
            CheckpointStatus::Complete,
        )
        .await
        .unwrap();
    }

    let deleted = cp
        .delete_checkpoints("t1".to_string(), RetentionPolicy::KeepLast(2))
        .await
        .unwrap();
    assert_eq!(deleted, 3);

    let remaining = cp.list_checkpoints("t1".to_string()).await.unwrap();
    assert_eq!(remaining.len(), 2);
}

#[tokio::test]
async fn test_sqlite_with_dag() {
    let cp = SqliteCheckpointer::<TestState>::in_memory().unwrap();
    let mut dag = DAG::new();
    let _id = dag.add_node("step_a", vec![]);

    let state = TestState {
        value: "with_dag".to_string(),
    };
    cp.save_state(
        "t1".to_string(),
        state,
        "step_a".to_string(),
        Some(&dag),
        CheckpointStatus::Running,
    )
    .await
    .unwrap();

    let loaded = cp.load_state("t1".to_string()).await.unwrap().unwrap();
    assert!(loaded.2.is_some());
    assert_eq!(loaded.2.unwrap().nodes.len(), 1);
    assert_eq!(loaded.1.status, CheckpointStatus::Running);
}

#[tokio::test]
async fn test_sqlite_status_roundtrip() {
    let cp = SqliteCheckpointer::<TestState>::in_memory().unwrap();

    let statuses = [
        CheckpointStatus::Complete,
        CheckpointStatus::Running,
        CheckpointStatus::Yielded,
        CheckpointStatus::Failed,
        CheckpointStatus::Interrupted,
    ];

    for (i, status) in statuses.iter().enumerate() {
        cp.save_state(
            "t1".to_string(),
            TestState {
                value: format!("v{}", i),
            },
            format!("Node{}", i),
            None,
            status.clone(),
        )
        .await
        .unwrap();
    }

    let list = cp.list_checkpoints("t1".to_string()).await.unwrap();
    assert_eq!(list.len(), 5);
    assert_eq!(list[0].status, CheckpointStatus::Complete);
    assert_eq!(list[1].status, CheckpointStatus::Running);
    assert_eq!(list[2].status, CheckpointStatus::Yielded);
    assert_eq!(list[3].status, CheckpointStatus::Failed);
    assert_eq!(list[4].status, CheckpointStatus::Interrupted);
}
