//! One safe representation for node failures that may cross a persistence or
//! process-log boundary.
//!
//! Node errors frequently include provider, tool, or user supplied fragments.
//! Those fragments are neither telemetry capture nor logging authority.  Keep
//! only a bounded, deterministic fingerprint so operators can correlate the
//! same failure without persisting or logging its raw content.

use sha2::{Digest, Sha256};

/// A fingerprint is deliberately much smaller than any error payload and is
/// safe to write even if the source error contains credentials or PII.
pub const SAFE_ERROR_FINGERPRINT_MAX_BYTES: usize = 96;

/// Return the sole safe cross-boundary representation for a node error.
///
/// This is stronger than best-effort pattern redaction: no source characters
/// are retained, so a missed PII/token pattern cannot leak via a future error
/// type.  The stable digest and bounded byte length still let an operator
/// correlate repeats and detect unexpectedly large failures.
pub fn redacted_error_fingerprint(error: &str) -> String {
    let digest = Sha256::digest(error.as_bytes());
    let value = format!("sha256:{}(len={})", &hex::encode(digest)[..16], error.len());
    debug_assert!(value.len() <= SAFE_ERROR_FINGERPRINT_MAX_BYTES);
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_fingerprint_is_bounded_deterministic_and_never_contains_source() {
        let source = format!("provider rejected nico@nma.vc bearer sk-secret-{}", "x".repeat(8192));
        let value = redacted_error_fingerprint(&source);
        assert_eq!(value, redacted_error_fingerprint(&source));
        assert!(value.len() <= SAFE_ERROR_FINGERPRINT_MAX_BYTES);
        assert!(!value.contains("nico@nma.vc"));
        assert!(!value.contains("sk-secret"));
    }

    /// The digest format is a contract: operators correlate failures across
    /// processes and releases by this exact string, so it is pinned to known
    /// values (sha256 of the UTF-8 bytes, first 16 hex characters, byte length).
    /// These values were produced by the Tectic fork's implementation before the
    /// port and independently recomputed with `sha256sum`.
    #[test]
    fn error_fingerprint_digest_is_pinned() {
        assert_eq!(redacted_error_fingerprint(""), "sha256:e3b0c44298fc1c14(len=0)");
        assert_eq!(
            redacted_error_fingerprint("connection refused"),
            "sha256:25a84382140b11ac(len=18)"
        );
    }

    #[test]
    fn error_fingerprint_length_is_bytes_not_characters() {
        // "\u{e9}" is one char and two UTF-8 bytes.
        assert!(redacted_error_fingerprint("\u{e9}").ends_with("(len=2)"));
    }
}
