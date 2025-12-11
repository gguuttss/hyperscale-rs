//! Parallel simulator orchestrator.
//!
//! Manages the lifecycle of a parallel simulation:
//! 1. Creates nodes with storage and state machines
//! 2. Initializes genesis on all nodes
//! 3. Spawns router and node tasks
//! 4. Provides interface for transaction submission
//! 5. Coordinates shutdown and collects final metrics

use crate::config::ParallelConfig;
use crate::metrics::{MetricsCollector, SharedMetrics, SimulationReport};
use crate::node_task::{NodeHandle, NodeTask, NodeTaskConfig};
use crate::router::{MessageRouter, RoutedMessage, RouterStatsHandle};
use hyperscale_bft::{BftConfig, RecoveredState};
use hyperscale_core::Event;
use hyperscale_engine::{NetworkDefinition, RadixExecutor};
use hyperscale_node::NodeStateMachine;
use hyperscale_production::ThreadPoolManager;
use hyperscale_simulation::SimStorage;
use hyperscale_types::{
    Block, BlockHeader, BlockHeight, Hash, KeyPair, KeyType, PublicKey, QuorumCertificate,
    RoutableTransaction, ShardGroupId, StaticTopology, Topology, ValidatorId, ValidatorInfo,
    ValidatorSet,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

/// Errors from parallel simulation.
#[derive(Debug, Error)]
pub enum ParallelError {
    #[error("Simulation not started")]
    NotStarted,
    #[error("Simulation already started")]
    AlreadyStarted,
    #[error("Node {0} not found")]
    NodeNotFound(u32),
    #[error("Failed to submit transaction: {0}")]
    SubmitFailed(String),
}

/// Parallel simulator orchestrator.
///
/// Manages all components of a parallel simulation and provides
/// the interface for running simulations and collecting results.
pub struct ParallelSimulator {
    config: ParallelConfig,
    /// Node handles for controlling each node.
    node_handles: Vec<NodeHandle>,
    /// Router statistics handle.
    router_stats: Option<RouterStatsHandle>,
    /// Metrics collector.
    metrics: Option<MetricsCollector>,
    /// Shared metrics for atomic counters.
    shared_metrics: Arc<SharedMetrics>,
    /// Router task handle.
    router_handle: Option<JoinHandle<()>>,
    /// Node task handles.
    node_task_handles: Vec<JoinHandle<()>>,
    /// Thread pool manager (shared across all nodes).
    thread_pools: Arc<ThreadPoolManager>,
    /// Sender for routing messages (cloned to each node, unbounded).
    router_tx: Option<mpsc::UnboundedSender<RoutedMessage>>,
    /// Whether simulation has been started.
    started: bool,
}

impl ParallelSimulator {
    /// Create a new parallel simulator with the given configuration.
    pub fn new(config: ParallelConfig) -> Self {
        let thread_pools = Arc::new(
            ThreadPoolManager::new(config.thread_pools.clone())
                .expect("Failed to create thread pools"),
        );
        let shared_metrics = SharedMetrics::new();

        Self {
            config,
            node_handles: Vec::new(),
            router_stats: None,
            metrics: None,
            shared_metrics,
            router_handle: None,
            node_task_handles: Vec::new(),
            thread_pools,
            router_tx: None,
            started: false,
        }
    }

    /// Start the simulation.
    ///
    /// Creates all nodes, initializes genesis, and spawns tasks.
    pub async fn start(&mut self) -> Result<(), ParallelError> {
        if self.started {
            return Err(ParallelError::AlreadyStarted);
        }

        let num_shards = self.config.num_shards;
        let validators_per_shard = self.config.validators_per_shard;
        let total_nodes = self.config.total_nodes();
        let seed = self.config.seed;

        info!(
            num_shards,
            validators_per_shard, total_nodes, seed, "Starting parallel simulation"
        );

        // Generate deterministic keys for all validators
        let keys: Vec<KeyPair> = (0..total_nodes)
            .map(|i| {
                let mut seed_bytes = [0u8; 32];
                let key_seed = seed.wrapping_add(i as u64).wrapping_mul(0x517cc1b727220a95);
                seed_bytes[..8].copy_from_slice(&key_seed.to_le_bytes());
                seed_bytes[8..16].copy_from_slice(&(i as u64).to_le_bytes());
                KeyPair::from_seed(KeyType::Bls12381, &seed_bytes)
            })
            .collect();
        let public_keys: Vec<PublicKey> = keys.iter().map(|k| k.public_key()).collect();

        // Build global validator set
        let global_validators: Vec<ValidatorInfo> = (0..total_nodes)
            .map(|i| ValidatorInfo {
                validator_id: ValidatorId(i as u64),
                public_key: public_keys[i].clone(),
                voting_power: 1,
            })
            .collect();
        let global_validator_set = ValidatorSet::new(global_validators);

        // Build per-shard committee mappings
        let mut shard_committees: HashMap<ShardGroupId, Vec<ValidatorId>> = HashMap::new();
        for shard_id in 0..num_shards {
            let shard = ShardGroupId(shard_id as u64);
            let shard_start = shard_id * validators_per_shard;
            let committee: Vec<ValidatorId> = (0..validators_per_shard)
                .map(|v| ValidatorId((shard_start + v) as u64))
                .collect();
            shard_committees.insert(shard, committee);
        }

        // Create router (unbounded channels for simulation)
        let (router, node_receivers) = MessageRouter::new(
            num_shards,
            validators_per_shard,
            self.config.network.clone(),
            seed,
        );
        self.router_stats = Some(router.stats_handle());

        // Create router channel (unbounded)
        let (router_tx, router_rx) = mpsc::unbounded_channel();
        self.router_tx = Some(router_tx.clone());

        // Create metrics channel
        let (metrics_tx, metrics_rx) = mpsc::channel(10_000);
        self.metrics = Some(MetricsCollector::new(
            Arc::clone(&self.shared_metrics),
            metrics_rx,
        ));

        // Create executor for genesis
        let executor = RadixExecutor::new(NetworkDefinition::simulator());

        // Create nodes
        let mut nodes = Vec::with_capacity(total_nodes);
        let mut storages = Vec::with_capacity(total_nodes);

        for shard_id in 0..num_shards {
            let shard = ShardGroupId(shard_id as u64);
            let shard_start = shard_id * validators_per_shard;

            for v in 0..validators_per_shard {
                let node_index = (shard_start + v) as u32;
                let validator_id = ValidatorId(node_index as u64);

                // Create topology for this node
                let topology: Arc<dyn Topology> = Arc::new(StaticTopology::with_shard_committees(
                    validator_id,
                    shard,
                    num_shards as u64,
                    &global_validator_set,
                    shard_committees.clone(),
                ));

                // Create state machine
                let state_machine = NodeStateMachine::new(
                    node_index,
                    topology,
                    keys[node_index as usize].clone(),
                    BftConfig::default(),
                    RecoveredState::default(),
                );

                // Create storage and run genesis
                let mut storage = SimStorage::new();
                if let Err(e) = executor.run_genesis(&mut storage) {
                    tracing::warn!(node = node_index, "Genesis failed: {:?}", e);
                }

                nodes.push(state_machine);
                storages.push(storage);
            }
        }

        info!(total_nodes, "Created nodes and ran genesis");

        // Initialize genesis blocks on each shard and collect initial actions
        let mut initial_actions: Vec<Vec<hyperscale_core::Action>> =
            Vec::with_capacity(total_nodes);

        for shard_id in 0..num_shards {
            let genesis_header = BlockHeader {
                height: BlockHeight(0),
                parent_hash: Hash::from_bytes(&[0u8; 32]),
                parent_qc: QuorumCertificate::genesis(),
                proposer: ValidatorId((shard_id * validators_per_shard) as u64),
                timestamp: 0,
                round: 0,
                is_fallback: false,
            };

            let genesis_block = Block {
                header: genesis_header,
                transactions: vec![],
                committed_certificates: vec![],
                deferred: vec![],
                aborted: vec![],
            };

            let shard_start = shard_id * validators_per_shard;
            let shard_end = shard_start + validators_per_shard;

            for node in nodes[shard_start..shard_end].iter_mut() {
                let actions = node.initialize_genesis(genesis_block.clone());
                initial_actions.push(actions);
            }
        }

        info!(num_shards, "Initialized genesis blocks");

        // Spawn router task
        let router_handle = tokio::spawn(async move {
            router.run(router_rx).await;
        });
        self.router_handle = Some(router_handle);

        // Spawn node tasks
        let mut node_handles = Vec::with_capacity(total_nodes);
        let mut task_handles = Vec::with_capacity(total_nodes);

        // Convert node_receivers HashMap to a more usable form
        let mut receivers: Vec<_> = node_receivers.into_iter().collect();
        receivers.sort_by_key(|(k, _)| *k);

        for (node_index, (_, message_rx)) in receivers.into_iter().enumerate() {
            let node = nodes.remove(0);
            let storage = storages.remove(0);
            let node_initial_actions = initial_actions.remove(0);

            let config = NodeTaskConfig {
                node_index: node_index as u32,
                state_machine: node,
                storage,
                message_rx,
                router_tx: router_tx.clone(),
                thread_pools: Arc::clone(&self.thread_pools),
                metrics_tx: metrics_tx.clone(),
                initial_actions: node_initial_actions,
            };

            let (task, handle) = NodeTask::new(config);
            node_handles.push(handle);

            let task_handle = tokio::spawn(async move {
                task.run().await;
            });
            task_handles.push(task_handle);
        }

        self.node_handles = node_handles;
        self.node_task_handles = task_handles;
        self.started = true;

        info!(total_nodes, "All node tasks spawned");

        Ok(())
    }

    /// Submit a transaction to a specific node.
    pub async fn submit_transaction(
        &mut self,
        node_index: u32,
        tx: RoutableTransaction,
    ) -> Result<(), ParallelError> {
        if !self.started {
            return Err(ParallelError::NotStarted);
        }

        let handle = self
            .node_handles
            .get(node_index as usize)
            .ok_or(ParallelError::NodeNotFound(node_index))?;

        // Record submission in metrics
        if let Some(metrics) = &mut self.metrics {
            metrics.record_submission(tx.hash());
        }

        // Submit to node
        handle
            .tx_submit
            .send(Event::SubmitTransaction { tx })
            .await
            .map_err(|e| ParallelError::SubmitFailed(e.to_string()))?;

        Ok(())
    }

    /// Submit transactions to random nodes (round-robin by shard).
    pub async fn submit_transactions(
        &mut self,
        transactions: Vec<RoutableTransaction>,
    ) -> Result<(), ParallelError> {
        let validators_per_shard = self.config.validators_per_shard;

        for (i, tx) in transactions.into_iter().enumerate() {
            // Route to first validator in each shard, round-robin
            let shard = i % self.config.num_shards;
            let node_index = (shard * validators_per_shard) as u32;
            self.submit_transaction(node_index, tx).await?;
        }

        Ok(())
    }

    /// Process pending metrics (call periodically during long simulations).
    pub fn process_metrics(&mut self) {
        if let Some(metrics) = &mut self.metrics {
            metrics.process_completions();
        }
    }

    /// Get current in-flight transaction count.
    pub fn in_flight_count(&self) -> usize {
        self.metrics
            .as_ref()
            .map(|m| m.in_flight_count())
            .unwrap_or(0)
    }

    /// Wait for drain duration and process remaining completions.
    pub async fn drain(&mut self) {
        let drain_duration = self.config.drain_duration;
        info!(
            duration_secs = drain_duration.as_secs_f64(),
            "Draining in-flight transactions"
        );

        // Process completions periodically during drain
        let interval = Duration::from_millis(100);
        let mut elapsed = Duration::ZERO;

        while elapsed < drain_duration {
            tokio::time::sleep(interval).await;
            self.process_metrics();
            elapsed += interval;

            // Early exit if all transactions completed
            if self.in_flight_count() == 0 {
                break;
            }
        }

        // Final processing
        self.process_metrics();
    }

    /// Shutdown the simulation and collect final report.
    pub async fn shutdown(mut self) -> SimulationReport {
        info!("Shutting down parallel simulation");

        // Signal all nodes to shutdown
        for handle in self.node_handles.drain(..) {
            let _ = handle.shutdown.send(());
        }

        // Wait for node tasks to complete
        for handle in self.node_task_handles.drain(..) {
            let _ = handle.await;
        }

        // Drop router sender to trigger router shutdown
        drop(self.router_tx.take());

        // Wait for router to complete
        if let Some(handle) = self.router_handle.take() {
            let _ = handle.await;
        }

        // Collect final metrics
        let router_stats = self
            .router_stats
            .take()
            .map(|h| h.snapshot())
            .unwrap_or_default();

        let metrics = self
            .metrics
            .take()
            .expect("Metrics should exist after start");

        let report = metrics.finalize(router_stats);

        info!(
            completed = report.completed,
            tps = format!("{:.2}", report.tps),
            "Simulation complete"
        );

        report
    }

    /// Run a complete simulation with the given transactions.
    ///
    /// Starts the simulation, submits all transactions, drains, and shuts down.
    pub async fn run(
        mut self,
        transactions: Vec<RoutableTransaction>,
    ) -> Result<SimulationReport, ParallelError> {
        self.start().await?;
        self.submit_transactions(transactions).await?;
        self.drain().await;
        Ok(self.shutdown().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simulator_creation() {
        let config = ParallelConfig::new(1, 4);
        let simulator = ParallelSimulator::new(config);
        assert!(!simulator.started);
    }

    #[tokio::test]
    async fn test_simulator_start_shutdown() {
        let config = ParallelConfig::new(1, 4).with_drain_duration(Duration::from_millis(100));

        let mut simulator = ParallelSimulator::new(config);
        simulator.start().await.unwrap();
        assert!(simulator.started);

        let report = simulator.shutdown().await;
        assert_eq!(report.submitted, 0);
        assert_eq!(report.completed, 0);
    }
}
