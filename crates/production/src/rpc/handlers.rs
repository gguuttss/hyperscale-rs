//! HTTP request handlers for the RPC API.

use super::types::*;
use crate::sync::SyncStatus;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use hyperscale_types::RoutableTransaction;
use prometheus::{Encoder, TextEncoder};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};

/// Shared state for RPC handlers.
#[derive(Clone)]
pub struct RpcState {
    /// Ready flag for readiness probe.
    pub ready: Arc<AtomicBool>,
    /// Sync status provider.
    pub sync_status: Arc<RwLock<SyncStatus>>,
    /// Node status provider.
    pub node_status: Arc<RwLock<NodeStatusState>>,
    /// Channel to submit transactions to the node.
    pub tx_sender: mpsc::Sender<RoutableTransaction>,
    /// Server start time for uptime calculation.
    pub start_time: Instant,
}

/// Mutable node status state updated by the runner.
#[derive(Debug, Clone, Default)]
pub struct NodeStatusState {
    pub validator_id: u64,
    pub shard: u64,
    pub num_shards: u64,
    pub block_height: u64,
    pub view: u64,
    pub connected_peers: usize,
}

// ═══════════════════════════════════════════════════════════════════════════
// Health & Readiness Handlers
// ═══════════════════════════════════════════════════════════════════════════

/// Handler for `GET /health` - liveness probe.
pub async fn health_handler() -> impl IntoResponse {
    Json(HealthResponse::default())
}

/// Handler for `GET /ready` - readiness probe.
pub async fn ready_handler(State(state): State<RpcState>) -> impl IntoResponse {
    if state.ready.load(Ordering::SeqCst) {
        (
            StatusCode::OK,
            Json(ReadyResponse {
                status: "ready".to_string(),
                ready: true,
            }),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyResponse {
                status: "not_ready".to_string(),
                ready: false,
            }),
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Metrics Handler
// ═══════════════════════════════════════════════════════════════════════════

/// Handler for `GET /metrics` - Prometheus metrics.
pub async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(error = ?e, "Failed to encode metrics");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to encode metrics".to_string(),
        )
            .into_response();
    }

    (
        [(
            axum::http::header::CONTENT_TYPE,
            encoder.format_type().to_string(),
        )],
        buffer,
    )
        .into_response()
}

// ═══════════════════════════════════════════════════════════════════════════
// Status Handlers
// ═══════════════════════════════════════════════════════════════════════════

/// Handler for `GET /api/v1/status` - node status.
pub async fn status_handler(State(state): State<RpcState>) -> impl IntoResponse {
    let node_status = state.node_status.read().await;
    let uptime = state.start_time.elapsed().as_secs();

    Json(NodeStatusResponse {
        validator_id: node_status.validator_id,
        shard: node_status.shard,
        num_shards: node_status.num_shards,
        block_height: node_status.block_height,
        view: node_status.view,
        connected_peers: node_status.connected_peers,
        uptime_secs: uptime,
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Handler for `GET /api/v1/sync` - sync status.
pub async fn sync_handler(State(state): State<RpcState>) -> impl IntoResponse {
    let sync_status = state.sync_status.read().await;

    Json(SyncStatusResponse {
        state: format!("{:?}", sync_status.state).to_lowercase(),
        current_height: sync_status.current_height,
        target_height: sync_status.target_height,
        blocks_behind: sync_status.blocks_behind,
        sync_peers: sync_status.sync_peers,
        pending_fetches: sync_status.pending_fetches,
        queued_heights: sync_status.queued_heights,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// Transaction Handlers
// ═══════════════════════════════════════════════════════════════════════════

/// Handler for `POST /api/v1/transactions` - submit transaction.
pub async fn submit_transaction_handler(
    State(state): State<RpcState>,
    Json(request): Json<SubmitTransactionRequest>,
) -> impl IntoResponse {
    // Decode hex
    let tx_bytes = match hex::decode(&request.transaction_hex) {
        Ok(bytes) => bytes,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(SubmitTransactionResponse {
                    accepted: false,
                    hash: String::new(),
                    error: Some(format!("Invalid hex encoding: {}", e)),
                }),
            );
        }
    };

    // Decode SBOR
    let transaction: RoutableTransaction = match sbor::prelude::basic_decode(&tx_bytes) {
        Ok(tx) => tx,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(SubmitTransactionResponse {
                    accepted: false,
                    hash: String::new(),
                    error: Some(format!("Invalid transaction format: {:?}", e)),
                }),
            );
        }
    };

    let hash = hex::encode(transaction.hash().as_bytes());

    // Send to node
    match state.tx_sender.try_send(transaction) {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(SubmitTransactionResponse {
                accepted: true,
                hash,
                error: None,
            }),
        ),
        Err(mpsc::error::TrySendError::Full(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(SubmitTransactionResponse {
                accepted: false,
                hash,
                error: Some("Transaction queue full, try again later".to_string()),
            }),
        ),
        Err(mpsc::error::TrySendError::Closed(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(SubmitTransactionResponse {
                accepted: false,
                hash,
                error: Some("Node is shutting down".to_string()),
            }),
        ),
    }
}

/// Handler for `GET /api/v1/transactions/:hash` - get transaction status.
///
/// Note: This is a placeholder. Full implementation requires access to the
/// mempool/execution state, which would need to be added to RpcState.
pub async fn get_transaction_handler(
    State(_state): State<RpcState>,
    Path(_hash): Path<String>,
) -> impl IntoResponse {
    // TODO: Look up transaction status from mempool
    // For now, return a placeholder response
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(ErrorResponse::new("Transaction lookup not yet implemented")),
    )
}

// ═══════════════════════════════════════════════════════════════════════════
// Mempool Handler
// ═══════════════════════════════════════════════════════════════════════════

/// Handler for `GET /api/v1/mempool` - mempool status.
///
/// Note: This is a placeholder. Full implementation requires access to the
/// mempool state.
pub async fn mempool_handler(State(_state): State<RpcState>) -> impl IntoResponse {
    // TODO: Get actual mempool stats
    Json(MempoolStatusResponse {
        pending_count: 0,
        executing_count: 0,
        total_count: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request, Router};
    use tower::ServiceExt;

    fn create_test_state() -> RpcState {
        let (tx_sender, _rx) = mpsc::channel(100);
        RpcState {
            ready: Arc::new(AtomicBool::new(false)),
            sync_status: Arc::new(RwLock::new(SyncStatus::default())),
            node_status: Arc::new(RwLock::new(NodeStatusState::default())),
            tx_sender,
            start_time: Instant::now(),
        }
    }

    #[tokio::test]
    async fn test_health_handler() {
        let app = Router::new()
            .route("/health", axum::routing::get(health_handler))
            .with_state(create_test_state());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_ready_handler_not_ready() {
        let state = create_test_state();
        let app = Router::new()
            .route("/ready", axum::routing::get(ready_handler))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_ready_handler_ready() {
        let state = create_test_state();
        state.ready.store(true, Ordering::SeqCst);
        let app = Router::new()
            .route("/ready", axum::routing::get(ready_handler))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
