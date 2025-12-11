//! Parallel simulation orchestrator.
//!
//! Wraps `ParallelSimulator` with workload generation and metrics collection
//! for multi-core performance testing.

use crate::config::SimulatorConfig;
use hyperscale_parallel::{ParallelConfig, ParallelError, ParallelSimulator, SimulationReport};
use hyperscale_spammer::{
    AccountPool, AccountPoolError, SelectionMode, TransferWorkload, WorkloadGenerator,
};
use hyperscale_types::{shard_for_node, RoutableTransaction, ShardGroupId};
use radix_common::network::NetworkDefinition;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::info;

/// Errors from parallel orchestration.
#[derive(Debug, Error)]
pub enum ParallelOrchestratorError {
    #[error("Account pool error: {0}")]
    AccountPool(#[from] AccountPoolError),
    #[error("Parallel simulator error: {0}")]
    Parallel(#[from] ParallelError),
}

/// Configuration for parallel orchestrator.
#[derive(Debug, Clone)]
pub struct ParallelOrchestratorConfig {
    /// Number of shards.
    pub num_shards: usize,
    /// Validators per shard.
    pub validators_per_shard: usize,
    /// Accounts per shard for workload generation.
    pub accounts_per_shard: usize,
    /// Target transactions per second.
    pub target_tps: u64,
    /// Duration of transaction submission.
    pub submission_duration: Duration,
    /// Drain duration after submission ends.
    pub drain_duration: Duration,
    /// Cross-shard transaction ratio (0.0 to 1.0).
    pub cross_shard_ratio: f64,
    /// Random seed.
    pub seed: u64,
    /// Account selection mode for workload generation.
    pub selection_mode: SelectionMode,
}

impl Default for ParallelOrchestratorConfig {
    fn default() -> Self {
        Self {
            num_shards: 2,
            validators_per_shard: 4,
            accounts_per_shard: 100,
            target_tps: 100,
            submission_duration: Duration::from_secs(10),
            drain_duration: Duration::from_secs(5),
            cross_shard_ratio: 0.1,
            seed: 42,
            selection_mode: SelectionMode::Random,
        }
    }
}

impl ParallelOrchestratorConfig {
    /// Create a new configuration.
    pub fn new(num_shards: usize, validators_per_shard: usize) -> Self {
        Self {
            num_shards,
            validators_per_shard,
            ..Default::default()
        }
    }

    /// Set the target TPS.
    pub fn with_target_tps(mut self, tps: u64) -> Self {
        self.target_tps = tps;
        self
    }

    /// Set the submission duration.
    pub fn with_submission_duration(mut self, duration: Duration) -> Self {
        self.submission_duration = duration;
        self
    }

    /// Set the drain duration.
    pub fn with_drain_duration(mut self, duration: Duration) -> Self {
        self.drain_duration = duration;
        self
    }

    /// Set the cross-shard ratio.
    pub fn with_cross_shard_ratio(mut self, ratio: f64) -> Self {
        self.cross_shard_ratio = ratio;
        self
    }

    /// Set the random seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Set accounts per shard.
    pub fn with_accounts_per_shard(mut self, accounts: usize) -> Self {
        self.accounts_per_shard = accounts;
        self
    }

    /// Set account selection mode.
    pub fn with_selection_mode(mut self, mode: SelectionMode) -> Self {
        self.selection_mode = mode;
        self
    }

    /// Use no-contention mode for zero account conflicts.
    pub fn with_no_contention(self) -> Self {
        self.with_selection_mode(SelectionMode::NoContention)
    }
}

impl From<&SimulatorConfig> for ParallelOrchestratorConfig {
    fn from(config: &SimulatorConfig) -> Self {
        Self {
            num_shards: config.num_shards as usize,
            validators_per_shard: config.validators_per_shard as usize,
            accounts_per_shard: config.accounts_per_shard,
            target_tps: 100, // Default
            submission_duration: Duration::from_secs(10),
            drain_duration: Duration::from_secs(5),
            cross_shard_ratio: config.workload.cross_shard_ratio,
            seed: config.seed,
            selection_mode: config.workload.selection_mode,
        }
    }
}

/// Orchestrates parallel simulation with workload generation.
///
/// Uses `ParallelSimulator` under the hood for multi-core execution,
/// combined with `AccountPool` and `TransferWorkload` for transaction
/// generation.
pub struct ParallelOrchestrator {
    /// The parallel simulator.
    simulator: ParallelSimulator,
    /// Account pool for transaction generation.
    accounts: AccountPool,
    /// Workload generator.
    workload: TransferWorkload,
    /// Configuration.
    config: ParallelOrchestratorConfig,
    /// RNG for workload generation.
    rng: ChaCha8Rng,
}

impl ParallelOrchestrator {
    /// Create a new parallel orchestrator.
    pub fn new(config: ParallelOrchestratorConfig) -> Result<Self, ParallelOrchestratorError> {
        // Create parallel simulator config
        let parallel_config = ParallelConfig::new(config.num_shards, config.validators_per_shard)
            .with_seed(config.seed)
            .with_drain_duration(config.drain_duration);

        let simulator = ParallelSimulator::new(parallel_config);

        // Generate accounts
        let accounts = AccountPool::generate(config.num_shards as u64, config.accounts_per_shard)?;

        // Create workload generator
        let workload = TransferWorkload::new(NetworkDefinition::simulator())
            .with_cross_shard_ratio(config.cross_shard_ratio)
            .with_selection_mode(config.selection_mode);

        let rng = ChaCha8Rng::seed_from_u64(config.seed.wrapping_add(1));

        info!(
            num_shards = config.num_shards,
            validators_per_shard = config.validators_per_shard,
            accounts_per_shard = config.accounts_per_shard,
            target_tps = config.target_tps,
            "ParallelOrchestrator created"
        );

        Ok(Self {
            simulator,
            accounts,
            workload,
            config,
            rng,
        })
    }

    /// Run the full parallel simulation.
    ///
    /// This will:
    /// 1. Start the simulator (spawns all node tasks)
    /// 2. Generate and submit transactions at the target TPS rate
    /// 3. Drain remaining in-flight transactions
    /// 4. Shutdown and return the final report
    pub async fn run(mut self) -> Result<SimulationReport, ParallelOrchestratorError> {
        // Start the simulator
        self.simulator.start().await?;

        info!(
            target_tps = self.config.target_tps,
            submission_duration_secs = self.config.submission_duration.as_secs_f64(),
            "Starting transaction submission"
        );

        // Calculate batch size and interval for target TPS
        // Submit in small batches to spread load evenly
        let batch_size = (self.config.target_tps / 10).max(1) as usize;
        let batch_interval = Duration::from_millis(100); // 10 batches per second

        let submission_end = Instant::now() + self.config.submission_duration;
        let mut total_submitted = 0u64;

        while Instant::now() < submission_end {
            // Generate a batch of transactions
            let transactions =
                self.workload
                    .generate_batch(&self.accounts, batch_size, &mut self.rng);

            // Submit transactions to the appropriate shard
            for tx in transactions {
                let target_shard = self.get_target_shard(&tx);
                // Submit to first validator in the target shard
                let node_index =
                    (target_shard.0 as usize * self.config.validators_per_shard) as u32;
                if let Err(e) = self.simulator.submit_transaction(node_index, tx).await {
                    tracing::warn!("Failed to submit transaction: {}", e);
                }
                total_submitted += 1;
            }

            // Process any pending completions
            self.simulator.process_metrics();

            // Wait before next batch
            tokio::time::sleep(batch_interval).await;
        }

        info!(
            total_submitted,
            in_flight = self.simulator.in_flight_count(),
            "Submission complete, draining"
        );

        // Drain in-flight transactions
        self.simulator.drain().await;

        // Shutdown and get report
        let report = self.simulator.shutdown().await;

        Ok(report)
    }

    /// Get access to the underlying simulator (for advanced control).
    pub fn simulator(&self) -> &ParallelSimulator {
        &self.simulator
    }

    /// Get mutable access to the underlying simulator.
    pub fn simulator_mut(&mut self) -> &mut ParallelSimulator {
        &mut self.simulator
    }

    /// Determine the target shard for a transaction based on its first declared write.
    fn get_target_shard(&self, tx: &RoutableTransaction) -> ShardGroupId {
        tx.declared_writes
            .first()
            .map(|node_id| shard_for_node(node_id, self.config.num_shards as u64))
            .unwrap_or(ShardGroupId(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = ParallelOrchestratorConfig::new(2, 4)
            .with_target_tps(500)
            .with_submission_duration(Duration::from_secs(30))
            .with_cross_shard_ratio(0.2);

        assert_eq!(config.num_shards, 2);
        assert_eq!(config.validators_per_shard, 4);
        assert_eq!(config.target_tps, 500);
        assert_eq!(config.submission_duration, Duration::from_secs(30));
        assert_eq!(config.cross_shard_ratio, 0.2);
    }
}
