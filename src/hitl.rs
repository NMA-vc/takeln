//! Structured Human-in-the-Loop (HITL) yield requests.
//!
//! Provides [`YieldRequest`] for nodes that need to suspend execution and wait
//! for external input, and [`ResumeMode`] to control how execution resumes
//! after the human provides their response.
//!
//! # Security: No Inline PII
//!
//! The `message` field is intended for human-facing prompt text only and should
//! **not** contain personally identifiable information (PII) or sensitive data.
//! `YieldRequest` is persisted inside `CheckpointMeta`, which checkpoint backends
//! store as plaintext JSON.
//!
//! For sensitive payloads, use [`YieldRequest::with_payload_ref`] to store an opaque
//! reference handle (e.g., a record ID into your data store). Erasure of the
//! referenced record removes the sensitive data from a single source.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A structured request to suspend graph execution for external input.
///
/// Nodes return this via [`GraphError::Yield`](crate::GraphError::Yield) to
/// pause execution with schema-validated prompts and configurable resume behavior.
///
/// # Example
/// ```rust
/// use takeln::YieldRequest;
///
/// // Simple yield with just a message
/// let req = YieldRequest::simple("Please approve this action");
///
/// // Yield with explicit ID and JSON schema validation
/// let req = YieldRequest::new("approval_gate", "Please approve this action")
///     .with_schema(serde_json::json!({
///         "type": "object",
///         "properties": {
///             "approved": { "type": "boolean" }
///         },
///         "required": ["approved"]
///     }));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct YieldRequest {
    /// Unique identifier for this interrupt point, used to match resume calls.
    pub interrupt_id: String,
    /// Human-readable message describing what input is needed.
    pub message: String,
    /// Optional JSON Schema describing the expected input shape.
    pub schema: Option<serde_json::Value>,
    /// How execution should resume after input is provided.
    pub resume_mode: ResumeMode,
    /// Optional opaque reference to the caller's own data store (e.g., a record ID).
    /// Use this instead of embedding sensitive data in `message`.
    /// The checkpoint only retains this handle; erasure of the referenced record
    /// removes the sensitive data from a single source.
    pub payload_ref: Option<String>,
}

/// Controls how graph execution resumes after a yield is satisfied.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResumeMode {
    /// Re-execute the yielded node with the provided input available via
    /// [`NodeContext::resumed_input`](crate::NodeContext::resumed_input).
    #[default]
    ReEntry,
    /// Skip re-execution of the yielded node and proceed to the next node
    /// in the graph, discarding the input.
    Handoff,
}

impl YieldRequest {
    /// Create a simple yield request using the message as the interrupt ID.
    pub fn simple(message: impl Into<String>) -> Self {
        let msg = message.into();
        Self {
            interrupt_id: msg.clone(),
            message: msg,
            schema: None,
            resume_mode: ResumeMode::default(),
            payload_ref: None,
        }
    }

    /// Create a yield request with an explicit interrupt ID.
    pub fn new(interrupt_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            interrupt_id: interrupt_id.into(),
            message: message.into(),
            schema: None,
            resume_mode: ResumeMode::default(),
            payload_ref: None,
        }
    }

    /// Attach a JSON Schema to validate the resume input against.
    pub fn with_schema(mut self, schema: serde_json::Value) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the resume mode for this yield request.
    pub fn with_resume_mode(mut self, mode: ResumeMode) -> Self {
        self.resume_mode = mode;
        self
    }

    /// Set a payload reference handle (e.g., a record ID into tectic-memory).
    ///
    /// Use this to reference sensitive data by ID rather than embedding it
    /// inline in the yield request. This keeps the checkpoint free of PII.
    pub fn with_payload_ref(mut self, reference: impl Into<String>) -> Self {
        self.payload_ref = Some(reference.into());
        self
    }
}

impl fmt::Display for YieldRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.interrupt_id, self.message)
    }
}

/// Context for resuming a yielded graph execution thread, capturing audit metadata.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResumeContext {
    /// Opaque identifier for the actor performing the resume (e.g. user ID, system service).
    pub actor: Option<String>,
    /// Additional application-specific metadata.
    pub metadata: Option<serde_json::Value>,
}

impl ResumeContext {
    /// Create a new ResumeContext with a given actor name.
    pub fn new(actor: impl Into<String>) -> Self {
        Self {
            actor: Some(actor.into()),
            metadata: None,
        }
    }

    /// Attach metadata to the resume context.
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// Detailed record of a resume event, used for audit logging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeRecord {
    /// The unique ID of the interrupt point that was resolved.
    pub interrupt_id: String,
    /// The thread ID of the graph execution.
    pub thread_id: String,
    /// The name of the node where execution was suspended.
    pub node_name: String,
    /// Opaque actor identifier who performed the resume.
    pub actor: Option<String>,
    /// When the resume occurred.
    pub resumed_at: chrono::DateTime<chrono::Utc>,
    /// SHA-256 hash of the canonicalized resume input.
    pub response_hash: String,
}

/// Compute a canonical SHA-256 hash over the given JSON value.
///
/// Since `serde_json::to_string` writes keys sorted lexicographically
/// (using a `BTreeMap` internally), the resulting string is canonical.
pub fn compute_response_hash(input: &serde_json::Value) -> String {
    use sha2::{Digest, Sha256};
    let canonical = serde_json::to_string(input).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}
