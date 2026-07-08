use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::hitl::YieldRequest;

/// Status of a checkpoint, indicating what the graph was doing when the checkpoint was taken.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CheckpointStatus {
    /// The graph completed a node successfully and checkpointed before the next node.
    Complete,
    /// A node was mid-execution when the process was interrupted (crash recovery scenario).
    Running,
    /// A node yielded, suspending execution for external input (human-in-the-loop).
    Yielded,
    /// A node failed after exhausting retries.
    Failed,
    /// The graph was interrupted by a HITL `interrupt_before` or `interrupt_after` gate.
    Interrupted,
}

/// Metadata associated with a checkpoint snapshot.
///
/// Contains versioning information, timing, and status for crash recovery
/// and checkpoint management.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMeta {
    /// Unique identifier for this checkpoint.
    pub checkpoint_id: String,
    /// The thread this checkpoint belongs to.
    pub thread_id: String,
    /// The node that should execute next when resuming.
    pub next_node: String,
    /// Optional application-defined graph version string for compatibility checking.
    pub graph_version: Option<String>,
    /// Optional application-defined state schema version for migration detection.
    pub state_schema_version: Option<String>,
    /// What the graph was doing when this checkpoint was created.
    pub status: CheckpointStatus,
    /// When this checkpoint was created.
    pub created_at: DateTime<Utc>,
    /// The yield request that caused this checkpoint, if status is `Yielded`.
    pub yield_request: Option<YieldRequest>,
    /// The interrupt_id that was claimed for this execution, indicating
    /// that a resume has been started for this interrupt.
    pub claimed_interrupt: Option<String>,
    /// The interrupt_id that was last resolved via `resume_with_input`.
    /// Used for idempotent resume detection: if a caller retries with the
    /// same `interrupt_id`, the graph returns the current state instead of
    /// erroring.
    pub resolved_interrupt: Option<String>,
}

/// Policy for retaining or pruning old checkpoints.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RetentionPolicy {
    /// Keep all checkpoints (no pruning).
    KeepAll,
    /// Keep only the most recent N checkpoints per thread, deleting older ones.
    KeepLast(usize),
    /// Delete checkpoints older than the given duration.
    OlderThan(std::time::Duration),
}

/// Policy for handling checkpoints with `Running` status during resume (crash recovery).
///
/// When a process crashes mid-execution, the last checkpoint may have `status: Running`.
/// This policy determines how the graph executor handles that scenario.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum CrashRecoveryPolicy {
    /// Reset the interrupted node to `Pending` and re-execute it. This is the safest default.
    #[default]
    ResetToPending,
    /// Return an error immediately without attempting re-execution.
    FailFast,
    /// Skip the interrupted node (mark as `Done`) and continue with the next node.
    SkipAndContinue,
}
