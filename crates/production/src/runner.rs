//! Production runner implementation.

use crate::network::{InboundSyncRequest, Libp2pAdapter, Libp2pConfig, NetworkError};
use crate::storage::RocksDbStorage;
use crate::sync::{SyncConfig, SyncManager};
use crate::thread_pools::ThreadPoolManager;
use crate::timers::TimerManager;
use hyperscale_bft::{BftConfig, RecoveredState};
use hyperscale_core::TransactionStatus;
use hyperscale_engine::{NetworkDefinition, RadixExecutor};
use hyperscale_types::BlockHeight;

// Re-export NodeIndex from node crate (simulation-only concept).
// Production code will use ValidatorId from topology instead.
use hyperscale_core::{Action, Event, RequestId, StateMachine};
pub use hyperscale_node::NodeIndex;
use hyperscale_node::NodeStateMachine;
use hyperscale_types::{
    BlockVote, KeyPair, PublicKey, RoutableTransaction, ShardGroupId, Signature, StateVoteBlock,
    Topology, ViewChangeVote,
};
use libp2p::identity;
use parking_lot::RwLock;
use sbor::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::{instrument, span, Level, Span};

/// Errors from the production runner.
#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("Event channel closed")]
    ChannelClosed,
    #[error("Request dropped")]
    RequestDropped,
    #[error("Send error: {0}")]
    SendError(String),
    #[error("Network error: {0}")]
    NetworkError(#[from] NetworkError),
}

/// Pending signature verifications that can be batched.
///
/// Collects verification actions and processes them together using
/// batch verification for better performance (2-8x speedup for large batches).
#[derive(Default)]
struct PendingVerifications {
    /// Block votes waiting for verification (public key and signing message included).
    block_votes: Vec<(BlockVote, PublicKey, Vec<u8>)>,
    /// View change votes waiting for verification.
    view_change_votes: Vec<(ViewChangeVote, PublicKey, Vec<u8>)>,
    /// State votes waiting for verification (cross-shard execution).
    state_votes: Vec<(StateVoteBlock, PublicKey)>,
}

impl PendingVerifications {
    fn is_empty(&self) -> bool {
        self.block_votes.is_empty()
            && self.view_change_votes.is_empty()
            && self.state_votes.is_empty()
    }

    fn total_count(&self) -> usize {
        self.block_votes.len() + self.view_change_votes.len() + self.state_votes.len()
    }
}

/// Handle for shutting down a running ProductionRunner.
///
/// When dropped, signals the runner to exit gracefully.
#[derive(Debug)]
pub struct ShutdownHandle {
    tx: Option<oneshot::Sender<()>>,
}

impl ShutdownHandle {
    /// Trigger shutdown (consumes the handle).
    pub fn shutdown(mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for ShutdownHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Production runner with async I/O.
///
/// Uses the event aggregator pattern: a single task owns the state machine
/// and receives events via an mpsc channel.
///
/// # Thread Pool Configuration
///
/// The runner uses configurable thread pools for different workloads:
/// - **Crypto Pool**: BLS signature verification (CPU-bound)
/// - **Execution Pool**: Transaction execution via Radix Engine (CPU/memory)
/// - **I/O Pool**: Network, storage, timers (tokio runtime)
///
/// Use `ThreadPoolConfig` to customize core allocation, or use defaults
/// based on available CPU cores.
pub struct ProductionRunner {
    /// Receives events from all sources.
    event_rx: mpsc::Receiver<Event>,
    /// Clone this to send events from network, timers, callbacks.
    event_tx: mpsc::Sender<Event>,
    /// The state machine (owned, not shared).
    state: NodeStateMachine,
    /// Start time for calculating elapsed duration.
    start_time: Instant,
    /// Pending client requests awaiting response.
    pending_requests: HashMap<RequestId, oneshot::Sender<TransactionStatus>>,
    /// Next request ID.
    next_request_id: u64,
    /// Thread pool manager for crypto and execution workloads.
    thread_pools: Arc<ThreadPoolManager>,
    /// Timer manager for setting/cancelling timers.
    timer_manager: TimerManager,
    /// Network adapter (optional - None for testing without network).
    network: Option<Arc<Libp2pAdapter>>,
    /// Sync manager for fetching blocks from peers (optional - requires network).
    sync_manager: Option<SyncManager>,
    /// Local shard for network broadcasts.
    local_shard: ShardGroupId,
    /// Network topology (needed for cross-shard execution).
    topology: Arc<dyn Topology>,
    /// Block storage (optional - None for testing without persistence).
    /// Wrapped in RwLock for thread-safe mutable access from execution thread pool.
    storage: Option<Arc<RwLock<RocksDbStorage>>>,
    /// Transaction executor (optional - requires storage).
    executor: Option<Arc<RadixExecutor>>,
    /// Inbound sync request channel (from network adapter).
    sync_request_rx: Option<mpsc::Receiver<InboundSyncRequest>>,
    /// Shutdown signal receiver.
    shutdown_rx: oneshot::Receiver<()>,
    /// Shutdown handle sender (stored to return to caller).
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl ProductionRunner {
    /// Create a new production runner with default thread pool configuration.
    ///
    /// Uses `ThreadPoolConfig::auto()` to detect available cores and allocate
    /// threads using recommended ratios.
    pub fn new(
        node_index: NodeIndex,
        topology: Arc<dyn Topology>,
        signing_key: KeyPair,
        bft_config: BftConfig,
        channel_capacity: usize,
    ) -> Result<Self, RunnerError> {
        let thread_pools =
            ThreadPoolManager::auto().map_err(|e| RunnerError::SendError(e.to_string()))?;
        Self::with_thread_pools(
            node_index,
            topology,
            signing_key,
            bft_config,
            channel_capacity,
            Arc::new(thread_pools),
        )
    }

    /// Create a new production runner with custom thread pool configuration.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use hyperscale_production::{ProductionRunner, ThreadPoolConfig, ThreadPoolManager};
    /// use std::sync::Arc;
    ///
    /// let config = ThreadPoolConfig::builder()
    ///     .crypto_threads(4)
    ///     .execution_threads(8)
    ///     .io_threads(2)
    ///     .build()
    ///     .unwrap();
    ///
    /// let thread_pools = Arc::new(ThreadPoolManager::new(config).unwrap());
    ///
    /// // let runner = ProductionRunner::with_thread_pools(..., thread_pools);
    /// ```
    pub fn with_thread_pools(
        node_index: NodeIndex,
        topology: Arc<dyn Topology>,
        signing_key: KeyPair,
        bft_config: BftConfig,
        channel_capacity: usize,
        thread_pools: Arc<ThreadPoolManager>,
    ) -> Result<Self, RunnerError> {
        let (event_tx, event_rx) = mpsc::channel(channel_capacity);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let local_shard = topology.local_shard();
        // TODO: Load RecoveredState from storage for crash recovery.
        // For now, start fresh (unsafe for production restarts).
        let recovered = RecoveredState::default();
        let state = NodeStateMachine::new(
            node_index,
            topology.clone(),
            signing_key,
            bft_config,
            recovered,
        );
        let timer_manager = TimerManager::new(event_tx.clone());

        Ok(Self {
            event_rx,
            event_tx,
            state,
            start_time: Instant::now(),
            pending_requests: HashMap::new(),
            next_request_id: 0,
            thread_pools,
            timer_manager,
            network: None,
            sync_manager: None,
            local_shard,
            topology,
            storage: None,
            executor: None,
            sync_request_rx: None,
            shutdown_rx,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// Create a new production runner with network support.
    ///
    /// This is the full-featured constructor that includes libp2p networking.
    /// Use this for production deployments.
    ///
    /// # Arguments
    ///
    /// * `storage` - Optional RocksDB storage for block persistence and serving sync requests
    #[allow(clippy::too_many_arguments)]
    pub async fn with_network(
        node_index: NodeIndex,
        topology: Arc<dyn Topology>,
        signing_key: KeyPair,
        bft_config: BftConfig,
        channel_capacity: usize,
        thread_pools: Arc<ThreadPoolManager>,
        network_config: Libp2pConfig,
        ed25519_keypair: identity::Keypair,
        storage: Option<Arc<RwLock<RocksDbStorage>>>,
    ) -> Result<Self, RunnerError> {
        let (event_tx, event_rx) = mpsc::channel(channel_capacity);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let validator_id = topology.local_validator_id();
        let local_shard = topology.local_shard();

        // Load RecoveredState from storage for crash recovery
        // Falls back to default (fresh start) if no storage configured
        let recovered = match &storage {
            Some(s) => {
                let storage_guard = s.read();
                storage_guard.load_recovered_state()
            }
            None => RecoveredState::default(),
        };

        let state = NodeStateMachine::new(
            node_index,
            topology.clone(),
            signing_key,
            bft_config,
            recovered,
        );
        let timer_manager = TimerManager::new(event_tx.clone());

        // Create network adapter (returns both adapter and sync request receiver)
        let (network, sync_request_rx) = Libp2pAdapter::new(
            network_config,
            ed25519_keypair,
            validator_id,
            local_shard,
            event_tx.clone(),
        )
        .await?;

        // Subscribe to local shard topics
        network.subscribe_shard(local_shard).await?;

        // Create sync manager
        let sync_manager =
            SyncManager::new(SyncConfig::default(), network.clone(), event_tx.clone());

        // Create executor if storage is provided
        // Note: Using simulator network for now - production should use mainnet
        let executor = storage
            .as_ref()
            .map(|_| Arc::new(RadixExecutor::new(NetworkDefinition::simulator())));

        // Register all validators for peer validation
        // Note: In production, we'd derive PeerId from ed25519 public keys in topology
        // For now, this is a placeholder - validators need to be registered separately

        Ok(Self {
            event_rx,
            event_tx,
            state,
            start_time: Instant::now(),
            pending_requests: HashMap::new(),
            next_request_id: 0,
            thread_pools,
            timer_manager,
            network: Some(network),
            sync_manager: Some(sync_manager),
            local_shard,
            topology,
            storage,
            executor,
            sync_request_rx: Some(sync_request_rx),
            shutdown_rx,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// Get a reference to the thread pool manager.
    pub fn thread_pools(&self) -> &Arc<ThreadPoolManager> {
        &self.thread_pools
    }

    /// Get a reference to the network adapter (if configured).
    pub fn network(&self) -> Option<&Arc<Libp2pAdapter>> {
        self.network.as_ref()
    }

    /// Get the local shard ID.
    pub fn local_shard(&self) -> ShardGroupId {
        self.local_shard
    }

    /// Get a sender for submitting events.
    pub fn event_sender(&self) -> mpsc::Sender<Event> {
        self.event_tx.clone()
    }

    /// Take the shutdown handle.
    ///
    /// Returns a handle that when dropped triggers graceful shutdown.
    /// Can only be called once; subsequent calls return None.
    pub fn shutdown_handle(&mut self) -> Option<ShutdownHandle> {
        self.shutdown_tx
            .take()
            .map(|tx| ShutdownHandle { tx: Some(tx) })
    }

    /// Get a mutable reference to the sync manager (if configured).
    pub fn sync_manager_mut(&mut self) -> Option<&mut SyncManager> {
        self.sync_manager.as_mut()
    }

    /// Check if sync is in progress.
    pub fn is_syncing(&self) -> bool {
        self.sync_manager.as_ref().is_some_and(|s| s.is_syncing())
    }

    /// Run the main event loop.
    ///
    /// This should be spawned as a task. It runs until the event channel closes.
    ///
    /// The state machine runs on the current thread (the caller should ensure
    /// this is pinned to a dedicated core if desired). Crypto and execution
    /// work is dispatched to the configured thread pools.
    ///
    /// # Priority Handling
    ///
    /// Consensus events (from gossipsub) are prioritized over sync requests:
    /// - Uses `biased` select to always check consensus channel first
    /// - Drains up to `CONSENSUS_BATCH_SIZE` events before checking sync
    /// - Ensures consensus stays responsive under sync load
    pub async fn run(mut self) -> Result<(), RunnerError> {
        // Maximum consensus events to process before checking sync (10:1 ratio)
        const CONSENSUS_BATCH_SIZE: usize = 10;

        let config = self.thread_pools.config();
        tracing::info!(
            node_index = self.state.node_index(),
            shard = ?self.state.shard(),
            crypto_threads = config.crypto_threads,
            execution_threads = config.execution_threads,
            io_threads = config.io_threads,
            pin_cores = config.pin_cores,
            "Starting production runner"
        );

        // Sync tick interval (100ms)
        let mut sync_tick = tokio::time::interval(Duration::from_millis(100));
        sync_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Metrics tick interval (1 second)
        let mut metrics_tick = tokio::time::interval(Duration::from_secs(1));
        metrics_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track consecutive consensus events for batch limiting
        let mut consensus_batch_count: usize = 0;

        loop {
            // We need to handle the sync_request_rx conditionally since it's Option
            // Use a helper async block that returns None if the receiver is None
            let sync_request_future = async {
                if let Some(rx) = &mut self.sync_request_rx {
                    rx.recv().await
                } else {
                    // Never resolve if there's no receiver
                    std::future::pending::<Option<InboundSyncRequest>>().await
                }
            };

            // Use biased select for priority: consensus events are always checked first.
            // After CONSENSUS_BATCH_SIZE events, we yield to sync to prevent starvation.
            tokio::select! {
                biased;

                // SHUTDOWN: Always check shutdown first (highest priority)
                _ = &mut self.shutdown_rx => {
                    tracing::info!("Shutdown signal received");
                    break;
                }

                // HIGH PRIORITY: Handle incoming consensus events
                // Only check this branch if we haven't hit the batch limit
                event = self.event_rx.recv(), if consensus_batch_count < CONSENSUS_BATCH_SIZE => {
                    match event {
                        Some(event) => {
                            consensus_batch_count += 1;

                            // Create span for event handling
                            let event_type = event.type_name();
                            let event_span = span!(
                                Level::INFO,
                                "handle_event",
                                event.type = %event_type,
                                node = self.state.node_index(),
                                shard = ?self.state.shard(),
                                otel.kind = "INTERNAL",
                            );
                            let _event_guard = event_span.enter();

                            // Update time
                            let now = self.start_time.elapsed();
                            self.state.set_time(now);

                            // Check for SyncNeeded - handle specially
                            let sync_target = if let Event::SyncNeeded { target_height, target_hash } = &event {
                                Some((*target_height, *target_hash))
                            } else {
                                None
                            };

                            // Process event synchronously (fast)
                            let actions = {
                                let sm_span = span!(Level::DEBUG, "state_machine.handle");
                                let _sm_guard = sm_span.enter();
                                self.state.handle(event)
                            };

                            // Record action count
                            Span::current().record("actions.count", actions.len());

                            // Collect signature verifications for batching, process others immediately
                            let mut pending = PendingVerifications::default();

                            for action in actions {
                                match action {
                                    Action::VerifyVoteSignature { vote, public_key, signing_message } => {
                                        pending.block_votes.push((vote, public_key, signing_message));
                                    }
                                    Action::VerifyViewChangeVoteSignature { vote, public_key, signing_message } => {
                                        pending.view_change_votes.push((vote, public_key, signing_message));
                                    }
                                    Action::VerifyStateVoteSignature { vote, public_key } => {
                                        pending.state_votes.push((vote, public_key));
                                    }
                                    other => {
                                        if let Err(e) = self.process_action(other).await {
                                            tracing::error!(error = ?e, "Error processing action");
                                        }
                                    }
                                }
                            }

                            // Try to collect more verifications from queued events
                            // This drains any immediately available events to maximize batch size
                            while let Ok(more_event) = self.event_rx.try_recv() {
                                consensus_batch_count += 1;

                                // Update time for each event
                                let now = self.start_time.elapsed();
                                self.state.set_time(now);

                                let more_actions = self.state.handle(more_event);

                                for action in more_actions {
                                    match action {
                                        Action::VerifyVoteSignature { vote, public_key, signing_message } => {
                                            pending.block_votes.push((vote, public_key, signing_message));
                                        }
                                        Action::VerifyViewChangeVoteSignature { vote, public_key, signing_message } => {
                                            pending.view_change_votes.push((vote, public_key, signing_message));
                                        }
                                        Action::VerifyStateVoteSignature { vote, public_key } => {
                                            pending.state_votes.push((vote, public_key));
                                        }
                                        other => {
                                            if let Err(e) = self.process_action(other).await {
                                                tracing::error!(error = ?e, "Error processing action");
                                            }
                                        }
                                    }
                                }

                                // Respect batch limit
                                if consensus_batch_count >= CONSENSUS_BATCH_SIZE {
                                    break;
                                }
                            }

                            // Dispatch collected verifications as batches
                            self.dispatch_batched_verifications(pending);

                            // If this was a sync request, start the sync manager
                            if let Some((target_height, target_hash)) = sync_target {
                                if let Some(sync_mgr) = &mut self.sync_manager {
                                    sync_mgr.start_sync(target_height, target_hash);
                                }
                            }
                        }
                        None => {
                            // Channel closed, exit loop
                            break;
                        }
                    }
                }

                // LOW PRIORITY: Handle inbound sync requests from peers
                // This branch is checked when consensus is idle OR after batch limit hit
                Some(request) = sync_request_future => {
                    let sync_span = span!(
                        Level::DEBUG,
                        "handle_sync_request",
                        peer = %request.peer,
                        height = request.height,
                        channel_id = request.channel_id,
                    );
                    let _sync_guard = sync_span.enter();

                    self.handle_inbound_sync_request(request);
                    // Reset batch counter - we yielded to sync, now back to prioritizing consensus
                    consensus_batch_count = 0;
                }

                // MEDIUM PRIORITY: Periodic sync manager tick
                // This drives outbound sync fetches, so give it some priority
                _ = sync_tick.tick() => {
                    let tick_span = span!(Level::TRACE, "sync_tick");
                    let _tick_guard = tick_span.enter();

                    if let Some(sync_mgr) = &mut self.sync_manager {
                        sync_mgr.tick().await;
                    }
                    // Reset batch counter - we yielded, now back to prioritizing consensus
                    consensus_batch_count = 0;
                }

                // LOW PRIORITY: Periodic metrics update (1 second)
                _ = metrics_tick.tick() => {
                    // Update thread pool queue depths
                    crate::metrics::set_pool_queue_depths(
                        self.thread_pools.crypto_queue_depth(),
                        self.thread_pools.execution_queue_depth(),
                    );

                    // Update sync status
                    if let Some(sync_mgr) = &self.sync_manager {
                        crate::metrics::set_sync_status(
                            sync_mgr.blocks_behind(),
                            sync_mgr.is_syncing(),
                        );
                    }

                    // Update peer count
                    if let Some(network) = &self.network {
                        let peer_count = network.connected_peers().await.len();
                        crate::metrics::set_libp2p_peers(peer_count);
                    }
                }
            }
        }

        tracing::info!("Production runner stopped");
        Ok(())
    }

    /// Process an action.
    #[instrument(skip(self), fields(action.type = %action.type_name()))]
    async fn process_action(&mut self, action: Action) -> Result<(), RunnerError> {
        match action {
            // Network I/O - broadcast via gossipsub topics
            Action::BroadcastToShard { shard, mut message } => {
                // Inject trace context for cross-shard messages (no-op if feature disabled)
                message.inject_trace_context();

                if let Some(network) = &self.network {
                    network.broadcast_shard(shard, &message).await?;
                    tracing::debug!(?shard, msg_type = message.type_name(), "Broadcast to shard");
                } else {
                    tracing::debug!(
                        ?shard,
                        msg_type = message.type_name(),
                        "Would broadcast to shard (network not configured)"
                    );
                }
            }

            Action::BroadcastGlobal { mut message } => {
                // Inject trace context for cross-shard messages (no-op if feature disabled)
                message.inject_trace_context();

                if let Some(network) = &self.network {
                    network.broadcast_global(&message).await?;
                    tracing::debug!(msg_type = message.type_name(), "Broadcast globally");
                } else {
                    tracing::debug!(
                        msg_type = message.type_name(),
                        "Would broadcast globally (network not configured)"
                    );
                }
            }

            // Timers via timer manager
            Action::SetTimer { id, duration } => {
                self.timer_manager.set_timer(id, duration);
            }

            Action::CancelTimer { id } => {
                self.timer_manager.cancel_timer(id);
            }

            // Crypto verification on dedicated crypto thread pool
            Action::VerifyVoteSignature {
                vote,
                public_key,
                signing_message,
            } => {
                let event_tx = self.event_tx.clone();
                self.thread_pools.spawn_crypto(move || {
                    let start = std::time::Instant::now();
                    // Verify vote signature against domain-separated message
                    let valid = public_key.verify(&signing_message, &vote.signature);
                    crate::metrics::record_signature_verification_latency(
                        start.elapsed().as_secs_f64(),
                    );
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ = event_tx.blocking_send(Event::VoteSignatureVerified { vote, valid });
                });
            }

            Action::VerifyProvisionSignature {
                provision,
                public_key,
            } => {
                let event_tx = self.event_tx.clone();
                self.thread_pools.spawn_crypto(move || {
                    let start = std::time::Instant::now();
                    // Build signing message (must match ExecutionState::sign_provision)
                    let mut msg = Vec::new();
                    msg.extend_from_slice(b"STATE_PROVISION");
                    msg.extend_from_slice(provision.transaction_hash.as_bytes());
                    msg.extend_from_slice(&provision.target_shard.0.to_le_bytes());
                    msg.extend_from_slice(&provision.source_shard.0.to_le_bytes());
                    msg.extend_from_slice(&provision.block_height.0.to_le_bytes());
                    for entry in &provision.entries {
                        msg.extend_from_slice(entry.hash().as_bytes());
                    }

                    let valid = public_key.verify(&msg, &provision.signature);
                    crate::metrics::record_signature_verification_latency(
                        start.elapsed().as_secs_f64(),
                    );
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ = event_tx
                        .blocking_send(Event::ProvisionSignatureVerified { provision, valid });
                });
            }

            Action::VerifyStateVoteSignature { vote, public_key } => {
                let event_tx = self.event_tx.clone();
                self.thread_pools.spawn_crypto(move || {
                    let start = std::time::Instant::now();
                    // Build signing message (must match ExecutionState::create_vote)
                    let mut msg = Vec::new();
                    msg.extend_from_slice(b"EXEC_VOTE");
                    msg.extend_from_slice(vote.transaction_hash.as_bytes());
                    msg.extend_from_slice(vote.state_root.as_bytes());
                    msg.extend_from_slice(&vote.shard_group_id.0.to_le_bytes());
                    msg.push(if vote.success { 1 } else { 0 });

                    let valid = public_key.verify(&msg, &vote.signature);
                    crate::metrics::record_signature_verification_latency(
                        start.elapsed().as_secs_f64(),
                    );
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ =
                        event_tx.blocking_send(Event::StateVoteSignatureVerified { vote, valid });
                });
            }

            Action::VerifyStateCertificateSignature {
                certificate,
                public_keys,
            } => {
                let event_tx = self.event_tx.clone();
                self.thread_pools.spawn_crypto(move || {
                    let start = std::time::Instant::now();
                    // Build signing message
                    let mut msg = Vec::new();
                    msg.extend_from_slice(b"STATE_CERT");
                    msg.extend_from_slice(certificate.transaction_hash.as_bytes());
                    msg.extend_from_slice(certificate.outputs_merkle_root.as_bytes());
                    msg.extend_from_slice(&certificate.shard_group_id.0.to_le_bytes());
                    msg.push(if certificate.success { 1 } else { 0 });

                    // Get signer keys based on bitfield
                    let signer_keys: Vec<_> = public_keys
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| certificate.signers.is_set(*i))
                        .map(|(_, pk)| pk.clone())
                        .collect();

                    let valid = if signer_keys.is_empty() {
                        // No signers - valid only if zero signature (single-shard case)
                        certificate.aggregated_signature == hyperscale_types::Signature::zero()
                    } else {
                        // Verify aggregated BLS signature
                        match hyperscale_types::PublicKey::aggregate_bls(&signer_keys) {
                            Ok(aggregated_pk) => {
                                aggregated_pk.verify(&msg, &certificate.aggregated_signature)
                            }
                            Err(_) => false,
                        }
                    };

                    crate::metrics::record_signature_verification_latency(
                        start.elapsed().as_secs_f64(),
                    );
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ = event_tx.blocking_send(Event::StateCertificateSignatureVerified {
                        certificate,
                        valid,
                    });
                });
            }

            Action::VerifyQcSignature {
                qc,
                public_keys,
                block_hash,
                signing_message,
            } => {
                let event_tx = self.event_tx.clone();
                self.thread_pools.spawn_crypto(move || {
                    let start = std::time::Instant::now();
                    // Get signer keys based on QC's signer bitfield
                    let signer_keys: Vec<_> = public_keys
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| qc.signers.is_set(*i))
                        .map(|(_, pk)| pk.clone())
                        .collect();

                    let valid = if signer_keys.is_empty() {
                        // No signers - invalid QC (genesis is handled before action is emitted)
                        false
                    } else {
                        // Verify aggregated BLS signature against domain-separated message
                        match hyperscale_types::PublicKey::aggregate_bls(&signer_keys) {
                            Ok(aggregated_pk) => {
                                aggregated_pk.verify(&signing_message, &qc.aggregated_signature)
                            }
                            Err(_) => false,
                        }
                    };

                    crate::metrics::record_signature_verification_latency(
                        start.elapsed().as_secs_f64(),
                    );
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ =
                        event_tx.blocking_send(Event::QcSignatureVerified { block_hash, valid });
                });
            }

            Action::VerifyViewChangeVoteSignature {
                vote,
                public_key,
                signing_message,
            } => {
                let event_tx = self.event_tx.clone();
                self.thread_pools.spawn_crypto(move || {
                    let start = std::time::Instant::now();
                    let valid = public_key.verify(&signing_message, &vote.signature);
                    crate::metrics::record_signature_verification_latency(
                        start.elapsed().as_secs_f64(),
                    );
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ = event_tx
                        .blocking_send(Event::ViewChangeVoteSignatureVerified { vote, valid });
                });
            }

            Action::VerifyViewChangeHighestQc {
                vote,
                public_keys,
                signing_message,
            } => {
                let event_tx = self.event_tx.clone();
                self.thread_pools.spawn_crypto(move || {
                    let start = std::time::Instant::now();
                    // Get signer keys based on the highest_qc's signer bitfield
                    let signer_keys: Vec<_> = public_keys
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| vote.highest_qc.signers.is_set(*i))
                        .map(|(_, pk)| pk.clone())
                        .collect();

                    let valid = if signer_keys.is_empty() {
                        false
                    } else {
                        // Verify against domain-separated signing message
                        match hyperscale_types::PublicKey::aggregate_bls(&signer_keys) {
                            Ok(aggregated_pk) => aggregated_pk
                                .verify(&signing_message, &vote.highest_qc.aggregated_signature),
                            Err(_) => false,
                        }
                    };

                    crate::metrics::record_signature_verification_latency(
                        start.elapsed().as_secs_f64(),
                    );
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ =
                        event_tx.blocking_send(Event::ViewChangeHighestQcVerified { vote, valid });
                });
            }

            Action::VerifyViewChangeCertificateSignature {
                certificate,
                public_keys,
                signing_message,
            } => {
                let event_tx = self.event_tx.clone();
                self.thread_pools.spawn_crypto(move || {
                    let start = std::time::Instant::now();
                    // Verify aggregated BLS signature on the view change certificate
                    // The public_keys are pre-filtered by the state machine based on the signer bitfield
                    let valid = if public_keys.is_empty() {
                        false
                    } else {
                        match hyperscale_types::PublicKey::aggregate_bls(&public_keys) {
                            Ok(aggregated_pk) => aggregated_pk
                                .verify(&signing_message, &certificate.aggregated_signature),
                            Err(_) => false,
                        }
                    };

                    crate::metrics::record_signature_verification_latency(
                        start.elapsed().as_secs_f64(),
                    );
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ = event_tx.blocking_send(Event::ViewChangeCertificateSignatureVerified {
                        certificate,
                        valid,
                    });
                });
            }

            // Transaction execution on dedicated execution thread pool
            // NOTE: Execution is READ-ONLY. State writes are collected in the results
            // and committed later when TransactionCertificate is included in a block.
            Action::ExecuteTransactions {
                block_hash,
                transactions,
                state_root: _,
            } => {
                let event_tx = self.event_tx.clone();
                let storage = self.storage.clone();
                let executor = self.executor.clone();

                self.thread_pools.spawn_execution(move || {
                    let results = match (storage, executor) {
                        (Some(storage), Some(executor)) => {
                            // Execute transactions with READ lock - execution is read-only
                            let storage_guard = storage.read();
                            match executor.execute_single_shard(&*storage_guard, &transactions) {
                                Ok(output) => output
                                    .results()
                                    .iter()
                                    .map(|r| hyperscale_types::ExecutionResult {
                                        transaction_hash: r.tx_hash,
                                        success: r.success,
                                        state_root: r.outputs_merkle_root,
                                        writes: r.state_writes.clone(),
                                        error: r.error.clone(),
                                    })
                                    .collect(),
                                Err(e) => {
                                    tracing::warn!(?block_hash, error = %e, "Transaction execution failed");
                                    transactions
                                        .iter()
                                        .map(|tx| hyperscale_types::ExecutionResult {
                                            transaction_hash: tx.hash(),
                                            success: false,
                                            state_root: hyperscale_types::Hash::ZERO,
                                            writes: vec![],
                                            error: Some(format!("{}", e)),
                                        })
                                        .collect()
                                }
                            }
                        }
                        _ => {
                            tracing::warn!("No storage/executor configured, returning failed results");
                            transactions
                                .iter()
                                .map(|tx| hyperscale_types::ExecutionResult {
                                    transaction_hash: tx.hash(),
                                    success: false,
                                    state_root: hyperscale_types::Hash::ZERO,
                                    writes: vec![],
                                    error: Some("No executor configured".to_string()),
                                })
                                .collect()
                        }
                    };

                    let _ = event_tx.blocking_send(Event::TransactionsExecuted {
                        block_hash,
                        results,
                    });
                });
            }

            // Cross-shard transaction execution with provisions
            // NOTE: Execution is READ-ONLY. State writes are collected in the results
            // and committed later when TransactionCertificate is included in a block.
            Action::ExecuteCrossShardTransaction {
                tx_hash,
                transaction,
                provisions,
            } => {
                let event_tx = self.event_tx.clone();
                let storage = self.storage.clone();
                let executor = self.executor.clone();
                let topology = self.topology.clone();
                let local_shard = self.local_shard;

                self.thread_pools.spawn_execution(move || {
                    let result = match (storage, executor) {
                        (Some(storage), Some(executor)) => {
                            // Determine which nodes are local to this shard
                            let is_local_node = |node_id: &hyperscale_types::NodeId| -> bool {
                                topology.shard_for_node_id(node_id) == local_shard
                            };

                            // Execute with provisions - READ lock since execution is read-only
                            let storage_guard = storage.read();
                            match executor.execute_cross_shard(
                                &*storage_guard,
                                &[transaction],
                                &provisions,
                                is_local_node,
                            ) {
                                Ok(output) => {
                                    if let Some(r) = output.results().first() {
                                        hyperscale_types::ExecutionResult {
                                            transaction_hash: r.tx_hash,
                                            success: r.success,
                                            state_root: r.outputs_merkle_root,
                                            writes: r.state_writes.clone(),
                                            error: r.error.clone(),
                                        }
                                    } else {
                                        hyperscale_types::ExecutionResult {
                                            transaction_hash: tx_hash,
                                            success: false,
                                            state_root: hyperscale_types::Hash::ZERO,
                                            writes: vec![],
                                            error: Some("No execution result".to_string()),
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(?tx_hash, error = %e, "Cross-shard execution failed");
                                    hyperscale_types::ExecutionResult {
                                        transaction_hash: tx_hash,
                                        success: false,
                                        state_root: hyperscale_types::Hash::ZERO,
                                        writes: vec![],
                                        error: Some(format!("{}", e)),
                                    }
                                }
                            }
                        }
                        _ => {
                            tracing::warn!("No storage/executor configured for cross-shard execution");
                            hyperscale_types::ExecutionResult {
                                transaction_hash: tx_hash,
                                success: false,
                                state_root: hyperscale_types::Hash::ZERO,
                                writes: vec![],
                                error: Some("No executor configured".to_string()),
                            }
                        }
                    };

                    let _ = event_tx.blocking_send(Event::CrossShardTransactionExecuted {
                        tx_hash,
                        result,
                    });
                });
            }

            // Merkle computation on execution pool (can be parallelized internally)
            // Note: This action is currently not emitted by any state machine.
            Action::ComputeMerkleRoot { tx_hash, writes } => {
                let event_tx = self.event_tx.clone();

                self.thread_pools.spawn_execution(move || {
                    // Simple merkle root computation using hash chain
                    // A proper implementation would use a sparse Merkle tree
                    let root = if writes.is_empty() {
                        hyperscale_types::Hash::ZERO
                    } else {
                        // Sort writes for determinism
                        let mut sorted = writes;
                        sorted.sort_by(|a, b| a.0 .0.cmp(&b.0 .0));

                        // Hash chain
                        let mut data = Vec::new();
                        for (node_id, value) in &sorted {
                            data.extend_from_slice(&node_id.0);
                            data.extend_from_slice(value);
                        }
                        hyperscale_types::Hash::from_bytes(&data)
                    };
                    let _ = event_tx.blocking_send(Event::MerkleRootComputed { tx_hash, root });
                });
            }

            // Internal events go back through the channel
            Action::EnqueueInternal { event } => {
                self.event_tx
                    .send(event)
                    .await
                    .map_err(|e| RunnerError::SendError(e.to_string()))?;
            }

            // Client responses
            Action::EmitTransactionResult { request_id, result } => {
                if let Some(tx) = self.pending_requests.remove(&request_id) {
                    // Map TransactionDecision to TransactionStatus::Finalized
                    // Finalized means the decision (Accept/Reject) has been made
                    let _ = tx.send(TransactionStatus::Finalized(result));
                }
                tracing::debug!(?request_id, ?result, "Transaction result");
            }

            Action::EmitTransactionStatus {
                request_id,
                tx_hash,
                status,
            } => {
                tracing::debug!(?request_id, ?tx_hash, ?status, "Transaction status");
                // TODO: Route to status subscribers
            }

            Action::EmitCommittedBlock { block } => {
                tracing::info!(block_hash = ?block.hash(), "Block committed");
                // TODO: Notify subscribers
            }

            // ═══════════════════════════════════════════════════════════════════════
            // Storage writes
            // ═══════════════════════════════════════════════════════════════════════
            Action::PersistBlock { block, qc } => {
                // Fire-and-forget block persistence - not latency critical
                if let Some(storage) = &self.storage {
                    let storage = storage.clone();
                    let height = block.height();
                    tokio::spawn(async move {
                        let guard = storage.write();
                        guard.put_block(height, &block, &qc);
                        // Update chain metadata
                        guard.set_chain_metadata(height, None, None);
                        // Prune old votes - we no longer need votes at or below committed height
                        guard.prune_own_votes(height.0);
                    });
                }
            }

            Action::PersistTransactionCertificate { certificate } => {
                // Commit certificate + state writes atomically
                // This is durability-critical: we await completion
                if let Some(storage) = &self.storage {
                    let storage = storage.clone();
                    let local_shard = self.local_shard;

                    // Extract writes for local shard from the certificate's shard_proofs
                    let writes: Vec<_> = certificate
                        .shard_proofs
                        .get(&local_shard)
                        .map(|p| p.state_writes.clone())
                        .unwrap_or_default();

                    // Run on blocking thread since RocksDB write is sync
                    tokio::task::spawn_blocking(move || {
                        let mut guard = storage.write();
                        guard.commit_certificate_with_writes(&certificate, &writes);
                    })
                    .await
                    .ok();
                }
            }

            Action::PersistOwnVote {
                height,
                round,
                block_hash,
            } => {
                // **BFT Safety Critical**: Must persist before broadcasting vote
                // Prevents equivocation after crash/restart
                if let Some(storage) = &self.storage {
                    let storage = storage.clone();

                    // Use spawn_blocking since we need sync writes for BFT safety
                    // We await completion to ensure vote is persisted before returning
                    tokio::task::spawn_blocking(move || {
                        let guard = storage.read();
                        guard.put_own_vote(height.0, round, block_hash);
                    })
                    .await
                    .ok();
                }
            }

            // ═══════════════════════════════════════════════════════════════════════
            // Storage reads - delegate to async storage, send callback event
            // ═══════════════════════════════════════════════════════════════════════
            Action::FetchStateEntries { tx_hash, nodes } => {
                let event_tx = self.event_tx.clone();

                if let (Some(storage), Some(executor)) = (&self.storage, &self.executor) {
                    let storage = storage.clone();
                    let executor = executor.clone();

                    tokio::task::spawn_blocking(move || {
                        let guard = storage.read();
                        let entries = executor.fetch_state_entries(&*guard, &nodes);
                        let _ =
                            event_tx.blocking_send(Event::StateEntriesFetched { tx_hash, entries });
                    });
                } else {
                    // No storage configured - return empty
                    let _ = self
                        .event_tx
                        .send(Event::StateEntriesFetched {
                            tx_hash,
                            entries: vec![],
                        })
                        .await;
                }
            }

            Action::FetchBlock { height } => {
                let event_tx = self.event_tx.clone();

                if let Some(storage) = &self.storage {
                    let storage = storage.clone();

                    tokio::task::spawn_blocking(move || {
                        let guard = storage.read();
                        let block = guard.get_block(height).map(|(b, _qc)| b);
                        let _ = event_tx.blocking_send(Event::BlockFetched { height, block });
                    });
                } else {
                    let _ = self
                        .event_tx
                        .send(Event::BlockFetched {
                            height,
                            block: None,
                        })
                        .await;
                }
            }

            Action::FetchChainMetadata => {
                let event_tx = self.event_tx.clone();

                if let Some(storage) = &self.storage {
                    let storage = storage.clone();

                    tokio::task::spawn_blocking(move || {
                        let guard = storage.read();
                        let (height, hash, qc) = guard.get_chain_metadata();
                        let _ = event_tx.blocking_send(Event::ChainMetadataFetched {
                            height,
                            hash,
                            qc,
                        });
                    });
                } else {
                    let _ = self
                        .event_tx
                        .send(Event::ChainMetadataFetched {
                            height: hyperscale_types::BlockHeight(0),
                            hash: None,
                            qc: None,
                        })
                        .await;
                }
            }
        }

        Ok(())
    }

    /// Dispatch batched signature verifications to the crypto thread pool.
    ///
    /// This method takes collected verifications and processes them in a single
    /// crypto thread pool task, using batch verification when possible for better
    /// performance (2-8x speedup for batches of 8+ signatures).
    ///
    /// Results are sent back as individual events to maintain compatibility
    /// with the state machine's expectations.
    fn dispatch_batched_verifications(&self, pending: PendingVerifications) {
        if pending.is_empty() {
            return;
        }

        let event_tx = self.event_tx.clone();
        let batch_size = pending.total_count();

        self.thread_pools.spawn_crypto(move || {
            let start = std::time::Instant::now();

            // === BLOCK VOTES ===
            if !pending.block_votes.is_empty() {
                // Separate by key type for appropriate batch verification
                let mut ed25519_votes: Vec<(BlockVote, PublicKey, Vec<u8>)> = Vec::new();
                let mut bls_votes: Vec<(BlockVote, PublicKey, Vec<u8>)> = Vec::new();

                for (vote, pk, msg) in pending.block_votes {
                    match &pk {
                        PublicKey::Ed25519(_) => ed25519_votes.push((vote, pk, msg)),
                        PublicKey::Bls12381(_) => bls_votes.push((vote, pk, msg)),
                    }
                }

                // Process Ed25519 block votes using batch verification
                if !ed25519_votes.is_empty() {
                    let messages: Vec<&[u8]> =
                        ed25519_votes.iter().map(|(_, _, m)| m.as_slice()).collect();
                    let signatures: Vec<Signature> = ed25519_votes
                        .iter()
                        .map(|(v, _, _)| v.signature.clone())
                        .collect();
                    let pubkeys: Vec<PublicKey> =
                        ed25519_votes.iter().map(|(_, pk, _)| pk.clone()).collect();

                    let batch_valid =
                        PublicKey::batch_verify_ed25519(&messages, &signatures, &pubkeys);

                    if batch_valid {
                        for (vote, _, _) in ed25519_votes {
                            let _ = event_tx
                                .blocking_send(Event::VoteSignatureVerified { vote, valid: true });
                        }
                    } else {
                        // Fallback to individual verification to find which ones failed
                        for (vote, pk, msg) in ed25519_votes {
                            let valid = pk.verify(&msg, &vote.signature);
                            if !valid {
                                crate::metrics::record_signature_verification_failure();
                            }
                            let _ = event_tx
                                .blocking_send(Event::VoteSignatureVerified { vote, valid });
                        }
                    }
                }

                // Process BLS block votes individually (different messages)
                for (vote, pk, msg) in bls_votes {
                    let valid = pk.verify(&msg, &vote.signature);
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ = event_tx.blocking_send(Event::VoteSignatureVerified { vote, valid });
                }
            }

            // === VIEW CHANGE VOTES ===
            if !pending.view_change_votes.is_empty() {
                let mut ed25519_votes: Vec<(ViewChangeVote, PublicKey, Vec<u8>)> = Vec::new();
                let mut bls_votes: Vec<(ViewChangeVote, PublicKey, Vec<u8>)> = Vec::new();

                for (vote, pk, msg) in pending.view_change_votes {
                    match &pk {
                        PublicKey::Ed25519(_) => ed25519_votes.push((vote, pk, msg)),
                        PublicKey::Bls12381(_) => bls_votes.push((vote, pk, msg)),
                    }
                }

                // Process Ed25519 view change votes using batch verification
                if !ed25519_votes.is_empty() {
                    let messages: Vec<&[u8]> =
                        ed25519_votes.iter().map(|(_, _, m)| m.as_slice()).collect();
                    let signatures: Vec<Signature> = ed25519_votes
                        .iter()
                        .map(|(v, _, _)| v.signature.clone())
                        .collect();
                    let pubkeys: Vec<PublicKey> =
                        ed25519_votes.iter().map(|(_, pk, _)| pk.clone()).collect();

                    let batch_valid =
                        PublicKey::batch_verify_ed25519(&messages, &signatures, &pubkeys);

                    if batch_valid {
                        for (vote, _, _) in ed25519_votes {
                            let _ =
                                event_tx.blocking_send(Event::ViewChangeVoteSignatureVerified {
                                    vote,
                                    valid: true,
                                });
                        }
                    } else {
                        for (vote, pk, msg) in ed25519_votes {
                            let valid = pk.verify(&msg, &vote.signature);
                            if !valid {
                                crate::metrics::record_signature_verification_failure();
                            }
                            let _ =
                                event_tx.blocking_send(Event::ViewChangeVoteSignatureVerified {
                                    vote,
                                    valid,
                                });
                        }
                    }
                }

                // Process BLS view change votes individually
                for (vote, pk, msg) in bls_votes {
                    let valid = pk.verify(&msg, &vote.signature);
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ = event_tx
                        .blocking_send(Event::ViewChangeVoteSignatureVerified { vote, valid });
                }
            }

            // === STATE VOTES (cross-shard execution) ===
            if !pending.state_votes.is_empty() {
                // Build signing messages for state votes (must match ExecutionState::create_vote)
                let votes_with_msgs: Vec<(StateVoteBlock, PublicKey, Vec<u8>)> = pending
                    .state_votes
                    .into_iter()
                    .map(|(vote, pk)| {
                        let mut msg = Vec::new();
                        msg.extend_from_slice(b"EXEC_VOTE");
                        msg.extend_from_slice(vote.transaction_hash.as_bytes());
                        msg.extend_from_slice(vote.state_root.as_bytes());
                        msg.extend_from_slice(&vote.shard_group_id.0.to_le_bytes());
                        msg.push(if vote.success { 1 } else { 0 });
                        (vote, pk, msg)
                    })
                    .collect();

                let mut ed25519_votes: Vec<(StateVoteBlock, PublicKey, Vec<u8>)> = Vec::new();
                let mut bls_votes: Vec<(StateVoteBlock, PublicKey, Vec<u8>)> = Vec::new();

                for (vote, pk, msg) in votes_with_msgs {
                    match &pk {
                        PublicKey::Ed25519(_) => ed25519_votes.push((vote, pk, msg)),
                        PublicKey::Bls12381(_) => bls_votes.push((vote, pk, msg)),
                    }
                }

                // Process Ed25519 state votes using batch verification
                if !ed25519_votes.is_empty() {
                    let messages: Vec<&[u8]> =
                        ed25519_votes.iter().map(|(_, _, m)| m.as_slice()).collect();
                    let signatures: Vec<Signature> = ed25519_votes
                        .iter()
                        .map(|(v, _, _)| v.signature.clone())
                        .collect();
                    let pubkeys: Vec<PublicKey> =
                        ed25519_votes.iter().map(|(_, pk, _)| pk.clone()).collect();

                    let batch_valid =
                        PublicKey::batch_verify_ed25519(&messages, &signatures, &pubkeys);

                    if batch_valid {
                        for (vote, _, _) in ed25519_votes {
                            let _ = event_tx.blocking_send(Event::StateVoteSignatureVerified {
                                vote,
                                valid: true,
                            });
                        }
                    } else {
                        for (vote, pk, msg) in ed25519_votes {
                            let valid = pk.verify(&msg, &vote.signature);
                            if !valid {
                                crate::metrics::record_signature_verification_failure();
                            }
                            let _ = event_tx
                                .blocking_send(Event::StateVoteSignatureVerified { vote, valid });
                        }
                    }
                }

                // Process BLS state votes individually
                for (vote, pk, msg) in bls_votes {
                    let valid = pk.verify(&msg, &vote.signature);
                    if !valid {
                        crate::metrics::record_signature_verification_failure();
                    }
                    let _ =
                        event_tx.blocking_send(Event::StateVoteSignatureVerified { vote, valid });
                }
            }

            // Record batch metrics
            let elapsed = start.elapsed().as_secs_f64();
            crate::metrics::record_signature_verification_latency(elapsed);

            if batch_size > 1 {
                tracing::debug!(
                    batch_size,
                    elapsed_ms = elapsed * 1000.0,
                    "Batch verified signatures"
                );
            }
        });
    }

    /// Submit a transaction and wait for result.
    pub async fn submit_transaction(
        &mut self,
        tx: RoutableTransaction,
    ) -> Result<TransactionStatus, RunnerError> {
        let request_id = RequestId(self.next_request_id);
        self.next_request_id += 1;

        let (response_tx, response_rx) = oneshot::channel();
        self.pending_requests.insert(request_id, response_tx);

        // Send event to the state machine
        self.event_tx
            .send(Event::SubmitTransaction { tx, request_id })
            .await
            .map_err(|e| RunnerError::SendError(e.to_string()))?;

        // Wait for response
        response_rx.await.map_err(|_| RunnerError::RequestDropped)
    }

    /// Handle an inbound sync request from a peer.
    ///
    /// Looks up the requested block from storage and sends the response
    /// back via the network adapter.
    fn handle_inbound_sync_request(&self, request: InboundSyncRequest) {
        let height = BlockHeight(request.height);
        let channel_id = request.channel_id;

        tracing::debug!(
            peer = %request.peer,
            height = request.height,
            channel_id = channel_id,
            "Handling inbound sync request"
        );

        // Look up block from storage
        let response = if let Some(storage) = &self.storage {
            if let Some((block, qc)) = storage.read().get_block(height) {
                // Encode the response as SBOR: (Some(block), Some(qc))
                match sbor::basic_encode(&(Some(&block), Some(&qc))) {
                    Ok(data) => data,
                    Err(e) => {
                        tracing::warn!(height = request.height, error = ?e, "Failed to encode block response");
                        // Send empty response on encoding failure
                        sbor::basic_encode(&(None::<()>, None::<()>)).unwrap_or_default()
                    }
                }
            } else {
                tracing::trace!(height = request.height, "Block not found for sync request");
                // Send "not found" response
                sbor::basic_encode(&(None::<()>, None::<()>)).unwrap_or_default()
            }
        } else {
            tracing::warn!("Received sync request but no storage configured");
            // Send empty response
            sbor::basic_encode(&(None::<()>, None::<()>)).unwrap_or_default()
        };

        // Send response via network adapter
        if let Some(network) = &self.network {
            if let Err(e) = network.send_block_response(channel_id, response) {
                tracing::warn!(
                    height = request.height,
                    channel_id = channel_id,
                    error = ?e,
                    "Failed to send block response"
                );
            }
        }
    }
}
