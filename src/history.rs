use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A record of a single node execution for replay and auditing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    /// Name of the executed node.
    pub node_name: String,
    /// When the node started executing.
    pub started_at: DateTime<Utc>,
    /// Execution duration in milliseconds.
    pub duration_ms: u64,
    /// Execution outcome.
    pub status: String,
    /// Cost in EUR (if available).
    pub cost_eur: Option<f64>,
    /// Checkpoint ID saved after this execution (if any).
    pub checkpoint_id: Option<String>,
    /// Number of retry attempts.
    pub attempts: u8,
    /// Opaque actor identifier who performed the resume (only for resumes).
    pub actor: Option<String>,
    /// SHA-256 hash of the canonicalized resume input (only for resumes).
    pub response_hash: Option<String>,
}
