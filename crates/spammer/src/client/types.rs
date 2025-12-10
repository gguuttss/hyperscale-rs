//! Types for RPC client communication.

use serde::{Deserialize, Serialize};

/// Request to submit a transaction.
#[derive(Debug, Serialize)]
pub struct SubmitTransactionRequest {
    pub transaction_hex: String,
}

/// Response from transaction submission.
#[derive(Debug, Deserialize)]
pub struct SubmitTransactionResponse {
    pub accepted: bool,
    pub hash: String,
    pub error: Option<String>,
}

/// Result of a transaction submission.
#[derive(Debug)]
pub struct SubmissionResult {
    /// Whether the transaction was accepted.
    pub accepted: bool,
    /// The transaction hash.
    pub hash: String,
    /// Error message if rejected.
    pub error: Option<String>,
    /// HTTP status code.
    pub status_code: u16,
}

impl SubmissionResult {
    /// Check if the submission was successful.
    pub fn is_success(&self) -> bool {
        self.accepted && self.status_code >= 200 && self.status_code < 300
    }
}

/// Response from node status endpoint.
#[derive(Debug, Deserialize)]
pub struct NodeStatusResponse {
    pub validator_id: u32,
    pub shard: u64,
    #[serde(default)]
    pub num_shards: u64,
    #[serde(default)]
    pub block_height: u64,
    #[serde(default)]
    pub view: u64,
    #[serde(default)]
    pub connected_peers: usize,
    #[serde(default)]
    pub uptime_secs: u64,
    #[serde(default)]
    pub version: String,
}

/// Simplified node status.
#[derive(Debug)]
pub struct NodeStatus {
    pub validator_id: u32,
    pub shard: u64,
    pub block_height: u64,
    pub connected_peers: usize,
}

/// Response from transaction status endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct TransactionStatusResponse {
    /// Transaction hash (hex-encoded).
    pub hash: String,
    /// Current status of the transaction.
    /// Possible values: "pending", "committed", "executed", "completed", "blocked", "retried", "unknown", "error"
    pub status: String,
    /// Block height where committed (if committed).
    #[serde(default)]
    pub committed_height: Option<u64>,
    /// Final decision (if executed): "accept" or "reject".
    #[serde(default)]
    pub decision: Option<String>,
    /// Hash of the transaction blocking this one (if blocked).
    #[serde(default)]
    pub blocked_by: Option<String>,
    /// Hash of the retry transaction (if retried).
    #[serde(default)]
    pub retry_tx: Option<String>,
    /// Error message if status lookup failed.
    #[serde(default)]
    pub error: Option<String>,
}

impl TransactionStatusResponse {
    /// Check if the transaction has reached a terminal state.
    ///
    /// A transaction is truly terminal when:
    /// - `completed`: Certificate committed to a block, state locks released
    /// - `retried`: Superseded by a retry transaction
    /// - `error`: Processing error
    ///
    /// Note: `executed` is NOT terminal - the certificate still needs to be
    /// committed to a block to release state locks.
    pub fn is_terminal(&self) -> bool {
        matches!(self.status.as_str(), "completed" | "retried" | "error")
    }

    /// Check if the transaction completed successfully.
    pub fn is_success(&self) -> bool {
        self.status == "completed"
    }
}
