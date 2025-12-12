//! Message routing between nodes with network simulation.
//!
//! The router uses a `DelayQueue` for efficient delayed delivery instead of
//! spawning per-message tasks. Network simulation (latency, packet loss,
//! partitions) is applied using the same `NetworkConfig` as the deterministic
//! simulator for feature parity.

use hyperscale_core::OutboundMessage;
use hyperscale_simulation::{NetworkConfig, SimulatedNetwork};
use hyperscale_types::{ShardGroupId, ValidatorId};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Handle for sending messages to the router (unbounded for simulation).
pub type RouterTx = mpsc::UnboundedSender<RoutedMessage>;

/// Handle for receiving messages at a node (unbounded for simulation).
pub type NodeRx = mpsc::UnboundedReceiver<InboundMessage>;

/// Message with routing information, sent to the router.
pub struct RoutedMessage {
    /// Source node index.
    pub from: u32,
    /// Destination specification.
    pub destination: Destination,
    /// The message payload (Arc to avoid cloning during broadcast).
    pub message: Arc<OutboundMessage>,
}

/// Where to send a message.
#[derive(Debug, Clone)]
pub enum Destination {
    /// Broadcast to all nodes in a shard.
    Shard(ShardGroupId),
    /// Broadcast to all nodes globally.
    Global,
    /// Send to a specific validator.
    Validator(ValidatorId),
}

/// Inbound message delivered to a node.
#[derive(Clone)]
pub struct InboundMessage {
    /// Source node index.
    pub from: u32,
    /// The message payload (Arc to avoid cloning).
    pub message: Arc<OutboundMessage>,
}

/// Statistics from the router (accessed via Arc after router task completes).
#[derive(Debug, Clone, Default)]
pub struct RouterStats {
    pub dropped_loss: u64,
    pub dropped_partition: u64,
}

/// Routes messages between nodes with network simulation.
///
/// Messages are delivered immediately (no wall-clock delays) for maximum
/// simulation speed. Packet loss and partitions are still simulated.
///
/// # Design Notes
///
/// - Uses unbounded channels to avoid backpressure in simulation
/// - Messages are wrapped in `Arc` to avoid expensive clones during broadcast
/// - Partitions are managed through the underlying `SimulatedNetwork`
/// - Drop statistics are tracked via atomic counters exposed through `RouterStatsHandle`
pub struct MessageRouter {
    /// Sender handles for each node (node_index -> sender).
    node_senders: HashMap<u32, mpsc::UnboundedSender<InboundMessage>>,
    /// Simulated network for latency/loss/partition decisions.
    network: SimulatedNetwork,
    /// RNG for latency/loss sampling.
    rng: ChaCha8Rng,
    /// Count of dropped messages due to packet loss.
    messages_dropped_loss: Arc<AtomicU64>,
    /// Count of dropped messages due to partition.
    messages_dropped_partition: Arc<AtomicU64>,
}

impl MessageRouter {
    /// Create a new message router.
    ///
    /// Returns the router and receiver handles for each node.
    pub fn new(
        num_shards: usize,
        validators_per_shard: usize,
        network_config: NetworkConfig,
        seed: u64,
    ) -> (Self, HashMap<u32, NodeRx>) {
        let total_nodes = num_shards * validators_per_shard;
        let mut node_senders = HashMap::new();
        let mut node_receivers = HashMap::new();

        for node_idx in 0..total_nodes as u32 {
            let (tx, rx) = mpsc::unbounded_channel();
            node_senders.insert(node_idx, tx);
            node_receivers.insert(node_idx, rx);
        }

        let network = SimulatedNetwork::new(network_config);

        let router = Self {
            node_senders,
            network,
            rng: ChaCha8Rng::seed_from_u64(seed),
            messages_dropped_loss: Arc::new(AtomicU64::new(0)),
            messages_dropped_partition: Arc::new(AtomicU64::new(0)),
        };

        (router, node_receivers)
    }

    /// Get a handle for accessing router stats after the router task completes.
    pub fn stats_handle(&self) -> RouterStatsHandle {
        RouterStatsHandle {
            dropped_loss: Arc::clone(&self.messages_dropped_loss),
            dropped_partition: Arc::clone(&self.messages_dropped_partition),
        }
    }

    /// Queue a message for delivery to its destination(s).
    fn queue_message(&mut self, msg: RoutedMessage) {
        let targets = match msg.destination {
            Destination::Shard(shard) => self.network.peers_in_shard(shard),
            Destination::Global => self.network.all_nodes(),
            Destination::Validator(validator) => {
                vec![validator.0 as u32]
            }
        };

        for target in targets {
            // Skip sending to self
            if target == msg.from {
                continue;
            }

            // Use SimulatedNetwork's should_deliver which handles partition and packet loss
            let latency = match self.network.should_deliver(msg.from, target, &mut self.rng) {
                Some(latency) => latency,
                None => {
                    // Message was dropped - determine why for stats
                    if self.network.is_partitioned(msg.from, target) {
                        self.messages_dropped_partition
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        // Must be packet loss
                        self.messages_dropped_loss.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
            };

            let inbound = InboundMessage {
                from: msg.from,
                message: Arc::clone(&msg.message),
            };

            // Deliver immediately - we run as fast as possible, not wall-clock time
            // The latency value is computed but not used for delays (could be used for metrics)
            let _ = latency;
            self.deliver(target, inbound);
        }
    }

    /// Deliver a message to a target node (unbounded, never fails).
    fn deliver(&self, target: u32, message: InboundMessage) {
        if let Some(tx) = self.node_senders.get(&target) {
            // Unbounded send - ignoring error means receiver was dropped (shutdown)
            let _ = tx.send(message);
        }
    }

    /// Run the router's main loop.
    ///
    /// Processes incoming messages and delivers them immediately.
    /// Exits when the incoming channel is closed (all senders dropped).
    pub async fn run(mut self, mut incoming: mpsc::UnboundedReceiver<RoutedMessage>) {
        // Process messages as fast as possible - no wall-clock delays
        while let Some(msg) = incoming.recv().await {
            self.queue_message(msg);
        }
        tracing::debug!("Router shutdown complete");
    }

    /// Access the underlying network for partition management.
    ///
    /// This allows the orchestrator to create/heal partitions during simulation.
    pub fn network_mut(&mut self) -> &mut SimulatedNetwork {
        &mut self.network
    }
}

/// Handle for accessing router statistics after the router task is spawned.
///
/// The router is consumed by `run()`, so this handle provides access to
/// statistics via atomic counters that can be read from anywhere.
#[derive(Clone)]
pub struct RouterStatsHandle {
    dropped_loss: Arc<AtomicU64>,
    dropped_partition: Arc<AtomicU64>,
}

impl RouterStatsHandle {
    /// Get current count of messages dropped due to packet loss.
    pub fn dropped_loss(&self) -> u64 {
        self.dropped_loss.load(Ordering::Relaxed)
    }

    /// Get current count of messages dropped due to partition.
    pub fn dropped_partition(&self) -> u64 {
        self.dropped_partition.load(Ordering::Relaxed)
    }

    /// Get a snapshot of all router statistics.
    pub fn snapshot(&self) -> RouterStats {
        RouterStats {
            dropped_loss: self.dropped_loss(),
            dropped_partition: self.dropped_partition(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_router_creation() {
        let config = NetworkConfig {
            num_shards: 2,
            validators_per_shard: 4,
            ..Default::default()
        };

        let (router, receivers) = MessageRouter::new(2, 4, config, 42);

        assert_eq!(receivers.len(), 8);
        assert!(receivers.contains_key(&0));
        assert!(receivers.contains_key(&7));

        let stats = router.stats_handle();
        assert_eq!(stats.dropped_loss(), 0);
        assert_eq!(stats.dropped_partition(), 0);
    }

    #[tokio::test]
    async fn test_router_shutdown() {
        let config = NetworkConfig {
            num_shards: 1,
            validators_per_shard: 2,
            ..Default::default()
        };

        let (router, _receivers) = MessageRouter::new(1, 2, config, 42);
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn router
        let handle = tokio::spawn(async move {
            router.run(rx).await;
        });

        // Drop sender to trigger shutdown
        drop(tx);

        // Router should complete
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("Router should shutdown")
            .expect("Router task should complete");
    }
}
