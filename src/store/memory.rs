use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use uuid::Uuid;

use crate::checkpoint::{Checkpointer, ClaimResult};
use crate::checkpoint_meta::{CheckpointMeta, CheckpointStatus, RetentionPolicy};
use crate::error::TakelnError;
use crate::graph::State;
use crate::hitl::YieldRequest;

struct MemoryCheckpoint {
    checkpoint_id: String,
    state: Value,
    next_node: String,
    dag: Option<crate::dag::DAG>,
    status: CheckpointStatus,
    created_at: chrono::DateTime<chrono::Utc>,
    yield_request: Option<YieldRequest>,
    claimed_interrupt: Option<String>,
    resolved_interrupt: Option<String>,
}

impl MemoryCheckpoint {
    fn to_meta(&self, thread_id: &str) -> CheckpointMeta {
        CheckpointMeta {
            checkpoint_id: self.checkpoint_id.clone(),
            thread_id: thread_id.to_string(),
            next_node: self.next_node.clone(),
            graph_version: None,
            state_schema_version: None,
            status: self.status.clone(),
            created_at: self.created_at,
            yield_request: self.yield_request.clone(),
            claimed_interrupt: self.claimed_interrupt.clone(),
            resolved_interrupt: self.resolved_interrupt.clone(),
        }
    }
}

/// Thread-safe in-memory checkpointer for testing and lightweight local execution.
///
/// Checkpoints are stored in a `HashMap<thread_id, Vec<checkpoint>>` and are
/// lost when the process exits. Use a persistent backend (e.g., SQLite, PostgreSQL)
/// for durable checkpointing across restarts.
pub struct InMemoryCheckpointer<S: State> {
    store: Mutex<HashMap<String, Vec<MemoryCheckpoint>>>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: State> Default for InMemoryCheckpointer<S> {
    fn default() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S: State> InMemoryCheckpointer<S> {
    /// Creates a new empty in-memory checkpointer.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl<S: State> Checkpointer<S> for InMemoryCheckpointer<S> {
    async fn save_state(
        &self,
        thread_id: String,
        state: S,
        next_node: String,
        dag: Option<&crate::dag::DAG>,
        status: CheckpointStatus,
        yield_request: Option<YieldRequest>,
        claimed_interrupt: Option<String>,
        resolved_interrupt: Option<String>,
    ) -> Result<String, TakelnError> {
        let val = serde_json::to_value(&state).map_err(|e| TakelnError::SerializationError(e.to_string()))?;

        let checkpoint_id = Uuid::new_v4().to_string();
        let checkpoint = MemoryCheckpoint {
            checkpoint_id: checkpoint_id.clone(),
            state: val,
            next_node,
            dag: dag.cloned(),
            status,
            created_at: chrono::Utc::now(),
            yield_request,
            claimed_interrupt,
            resolved_interrupt,
        };

        let mut map = self
            .store
            .lock()
            .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;
        map.entry(thread_id).or_default().push(checkpoint);
        Ok(checkpoint_id)
    }

    async fn load_state(
        &self,
        thread_id: String,
    ) -> Result<Option<(S, CheckpointMeta, Option<crate::dag::DAG>)>, TakelnError> {
        let map = self
            .store
            .lock()
            .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;
        if let Some(list) = map.get(&thread_id) {
            if let Some(checkpoint) = list.last() {
                let state: S = serde_json::from_value(checkpoint.state.clone())
                    .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let meta = checkpoint.to_meta(&thread_id);
                return Ok(Some((state, meta, checkpoint.dag.clone())));
            }
        }
        Ok(None)
    }

    async fn load_version(
        &self,
        thread_id: String,
        checkpoint_id: String,
    ) -> Result<Option<(S, CheckpointMeta, Option<crate::dag::DAG>)>, TakelnError> {
        let map = self
            .store
            .lock()
            .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;
        if let Some(list) = map.get(&thread_id) {
            if let Some(checkpoint) = list.iter().find(|c| c.checkpoint_id == checkpoint_id) {
                let state: S = serde_json::from_value(checkpoint.state.clone())
                    .map_err(|e| TakelnError::DeserializationError(e.to_string()))?;
                let meta = checkpoint.to_meta(&thread_id);
                return Ok(Some((state, meta, checkpoint.dag.clone())));
            }
        }
        Ok(None)
    }

    async fn list_checkpoints(&self, thread_id: String) -> Result<Vec<CheckpointMeta>, TakelnError> {
        let map = self
            .store
            .lock()
            .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;
        if let Some(list) = map.get(&thread_id) {
            let res = list.iter().map(|c| c.to_meta(&thread_id)).collect();
            Ok(res)
        } else {
            Ok(vec![])
        }
    }

    async fn delete_checkpoints(&self, thread_id: String, policy: RetentionPolicy) -> Result<usize, TakelnError> {
        let mut map = self
            .store
            .lock()
            .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;

        if let Some(list) = map.get_mut(&thread_id) {
            let original_len = list.len();
            match policy {
                RetentionPolicy::KeepAll => return Ok(0),
                RetentionPolicy::KeepLast(n) => {
                    if list.len() > n {
                        let drain_count = list.len() - n;
                        list.drain(..drain_count);
                        return Ok(drain_count);
                    }
                    return Ok(0);
                }
                RetentionPolicy::OlderThan(duration) => {
                    let cutoff = chrono::Utc::now()
                        - chrono::Duration::from_std(duration)
                            .map_err(|e| TakelnError::CheckpointError(e.to_string()))?;
                    list.retain(|c| c.created_at >= cutoff);
                    return Ok(original_len - list.len());
                }
            }
        }
        Ok(0)
    }

    async fn claim_interrupt(&self, thread_id: &str, interrupt_id: &str) -> Result<ClaimResult, TakelnError> {
        let mut map = self
            .store
            .lock()
            .map_err(|e| TakelnError::CheckpointError(format!("Mutex poisoning: {}", e)))?;
        let list = map
            .get_mut(thread_id)
            .ok_or_else(|| TakelnError::NothingToResume(thread_id.to_string()))?;

        // 1. Check history for any checkpoint with matching resolved_interrupt
        if list
            .iter()
            .any(|cp| cp.resolved_interrupt.as_deref() == Some(interrupt_id))
        {
            return Ok(ClaimResult::AlreadyCompleted);
        }

        // 2. Get the latest checkpoint
        let last = list
            .last_mut()
            .ok_or_else(|| TakelnError::NothingToResume(thread_id.to_string()))?;

        // 3. If latest is Running under this interrupt_id, return InProgress
        if last.status == CheckpointStatus::Running && last.claimed_interrupt.as_deref() == Some(interrupt_id) {
            return Ok(ClaimResult::InProgress);
        }

        match last.status {
            CheckpointStatus::Yielded => {
                let yield_req = last.yield_request.as_ref().ok_or_else(|| {
                    TakelnError::NothingToResume(format!(
                        "Thread '{}' has Yielded status but no yield_request metadata",
                        thread_id
                    ))
                })?;
                if yield_req.interrupt_id != interrupt_id {
                    return Err(TakelnError::InvalidResume(format!(
                        "Expected interrupt_id '{}', got '{}'",
                        yield_req.interrupt_id, interrupt_id
                    )));
                }

                last.status = CheckpointStatus::Running;
                last.claimed_interrupt = Some(interrupt_id.to_string());
                Ok(ClaimResult::Claimed)
            }
            CheckpointStatus::Running => Err(TakelnError::ExecutionError(format!(
                "Resume claim failed: thread '{}' is already running with claimed_interrupt '{}'",
                thread_id,
                last.claimed_interrupt.as_deref().unwrap_or("none")
            ))),
            _ => Err(TakelnError::NothingToResume(format!(
                "Thread '{}' latest checkpoint status is {:?}, expected Yielded",
                thread_id, last.status
            ))),
        }
    }
}
