//! Main simulator runner.
//!
//! Orchestrates workload generation, transaction submission, and metrics collection
//! using the deterministic simulation framework.

use crate::config::SimulatorConfig;
use crate::metrics::{MetricsCollector, SimulationReport};
use hyperscale_core::Event;
use hyperscale_engine::CommittableSubstateDatabase;
use hyperscale_mempool::LockContentionStats;
use hyperscale_simulation::NodeIndex;
use hyperscale_simulation::SimulationRunner;
use hyperscale_spammer::{AccountPool, AccountPoolError, SwapWorkload, TransferWorkload, WorkloadGenerator};
use hyperscale_types::{
    shard_for_node, Hash, ShardGroupId, TransactionDecision, TransactionStatus,
};
use radix_common::network::NetworkDefinition;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info};

/// Main simulator that orchestrates workload generation and metrics collection.
pub struct Simulator {
    /// Underlying deterministic simulation runner.
    runner: SimulationRunner,

    /// Account pool for transaction generation.
    accounts: AccountPool,

    /// Transfer workload generator.
    transfer_workload: TransferWorkload,

    /// Optional swap workload generator.
    swap_workload: Option<SwapWorkload>,

    /// Batch size for transaction generation.
    batch_size: usize,

    /// Metrics collector.
    metrics: MetricsCollector,

    /// Configuration.
    config: SimulatorConfig,

    /// RNG for workload generation.
    rng: ChaCha8Rng,

    /// Tracks in-flight transactions: hash -> (submit_time, target_shard).
    in_flight: HashMap<Hash, (Duration, ShardGroupId)>,

    /// Stores execution results for pool deployment transactions.
    /// Maps: tx_hash -> ExecutionResult
    _execution_results: HashMap<Hash, hyperscale_types::ExecutionResult>,
}

impl Simulator {
    /// Create a new simulator with the given configuration.
    pub fn new(config: SimulatorConfig) -> Result<Self, SimulatorError> {
        let network_config = config.to_network_config();
        let runner = SimulationRunner::new(network_config, config.seed);

        // Generate accounts
        let accounts = AccountPool::generate(config.num_shards as u64, config.accounts_per_shard)?;

        // Create transfer workload generator
        let transfer_workload = TransferWorkload::new(NetworkDefinition::simulator())
            .with_cross_shard_ratio(config.workload.cross_shard_ratio)
            .with_selection_mode(config.workload.selection_mode);

        // Swap workload is optional, will be added via with_swap_workload()
        let swap_workload = None;

        let batch_size = config.workload.batch_size;

        // RNG for workload (separate from simulation RNG for independence)
        let rng = ChaCha8Rng::seed_from_u64(config.seed.wrapping_add(1));

        // Metrics start at time zero (will be reset when we actually start)
        let metrics = MetricsCollector::new(Duration::ZERO);

        info!(
            num_shards = config.num_shards,
            validators_per_shard = config.validators_per_shard,
            accounts_per_shard = config.accounts_per_shard,
            "Simulator created"
        );

        Ok(Self {
            runner,
            accounts,
            transfer_workload,
            swap_workload,
            batch_size,
            metrics,
            config,
            rng,
            in_flight: HashMap::new(),
            _execution_results: HashMap::new(),
        })
    }

    /// Enable swap workload with custom tokens and pools.
    ///
    /// This must be called before `initialize()` to ensure custom tokens
    /// are created at genesis.
    ///
    /// # Arguments
    /// * `num_tokens` - Number of custom tokens to create (e.g., 2 for TOKEN_A, TOKEN_B)
    /// * `num_pools` - Number of AMM pools to create (default: 4)
    ///
    /// # Example
    /// ```ignore
    /// let mut sim = Simulator::new(config)?;
    /// sim.with_swap_workload(2, Some(4))?;
    /// sim.initialize(); // Will create tokens + deploy pools
    /// ```
    pub fn with_swap_workload(
        &mut self,
        num_tokens: usize,
        num_pools: Option<usize>,
    ) -> Result<(), SimulatorError> {
        let mut swap_workload = SwapWorkload::new(NetworkDefinition::simulator())
            .with_cross_shard_ratio(self.config.workload.cross_shard_ratio)
            .with_selection_mode(self.config.workload.selection_mode);

        // Prepare tokens and pools
        swap_workload.prepare_tokens(num_tokens);
        swap_workload
            .prepare_pools(self.config.num_shards as u64, num_pools)
            .map_err(|e| SimulatorError::Workload(e.to_string()))?;

        info!(
            num_tokens,
            num_pools = swap_workload.num_pools(),
            "Swap workload configured"
        );

        self.swap_workload = Some(swap_workload);
        Ok(())
    }

    /// Initialize the simulation (genesis, etc).
    ///
    /// This funds all generated accounts at genesis time with the configured
    /// initial balance, allowing transactions to execute without failing due
    /// to insufficient funds.
    ///
    /// Also runs a warmup period to let the consensus establish before
    /// transaction submission begins. This ensures cross-shard provisioning
    /// works correctly from the start.
    pub fn initialize(&mut self) {
        // Collect all account balances for genesis
        let balances: Vec<_> = self
            .accounts
            .shards()
            .flat_map(|shard| {
                self.accounts
                    .genesis_balances_for_shard(shard, self.config.initial_balance)
            })
            .collect();

        info!(
            num_accounts = balances.len(),
            initial_balance = %self.config.initial_balance,
            "Funding accounts at genesis"
        );

        self.runner.initialize_genesis_with_balances(balances);

        // Run a warmup period to let consensus establish.
        // This ensures at least a few blocks are committed before we start
        // submitting transactions. Without this, cross-shard transactions
        // submitted immediately after genesis may fail because provisioning
        // requires blocks to be committed first.
        //
        // Warmup time: 3 block intervals (default 300ms each = 900ms)
        // This allows:
        // - Block 1 to be proposed and committed
        // - Block 2 to be proposed and committed
        // - Consensus to stabilize across all shards
        let warmup_duration = Duration::from_millis(900);
        info!(
            warmup_ms = warmup_duration.as_millis(),
            "Running warmup period for consensus to establish"
        );
        self.runner.run_until(self.runner.now() + warmup_duration);

        // Deploy AMM pools if swap workload is enabled
        if self.swap_workload.is_some() {
            info!("Deploying AMM pools...");
            if let Err(e) = self.deploy_pools() {
                panic!("Failed to deploy pools: {}", e);
            }
            info!("AMM pools deployed successfully");
        }

        info!("Genesis initialized with funded accounts");
    }

    /// Deploy AMM pools after genesis.
    ///
    /// This is Step 1 of the 5-step deployment process.
    /// Currently implements package publishing.
    fn deploy_pools(&mut self) -> Result<(), SimulatorError> {
        // Step 1: Publish Radiswap package
        info!("Step 1: Publishing Radiswap package...");
        let package_address = self.publish_radiswap_package()?;
        info!(?package_address, "Radiswap package published");

        // Step 2: Create custom tokens
        if let Some(swap_workload) = &self.swap_workload {
            info!("Step 2: Creating custom tokens...");
            let token_count = swap_workload.custom_token_addresses().len();
            self.create_custom_tokens(token_count)?;
            info!(token_count, "Custom tokens created");
        }

        // Step 3: Instantiate pools
        let pool_addresses = if let Some(swap_workload) = &self.swap_workload {
            info!("Step 3: Instantiating AMM pools...");
            let pool_count = swap_workload.num_pools();
            let addresses = self.instantiate_pools(package_address, pool_count)?;
            info!(pool_count, "AMM pools instantiated");

            // Replicate pool components to all shards
            info!("Replicating {} pool components to all shards", addresses.len());
            self.replicate_pool_components(&addresses);

            addresses
        } else {
            Vec::new()
        };

        // Step 4: Add initial liquidity to pools (directly from deployer)
        if let Some(_swap_workload) = &self.swap_workload {
            info!("Step 4: Adding initial liquidity to pools...");
            self.add_liquidity_to_pools(&pool_addresses)?;
            info!("Initial liquidity added to all pools");
        }

        // Step 5: Distribute custom tokens to all simulation accounts
        if let Some(_swap_workload) = &self.swap_workload {
            info!("Step 5: Distributing custom tokens to simulation accounts...");
            self.distribute_tokens_to_accounts()?;
            info!("Custom tokens distributed to all accounts");
        }

        // Step 6: Update SwapWorkload with pool addresses
        if let Some(swap_workload) = &mut self.swap_workload {
            info!("Step 6: Updating SwapWorkload with pool addresses...");
            for (i, &address) in pool_addresses.iter().enumerate() {
                swap_workload.set_pool_address(i, address);
            }
            info!("SwapWorkload updated with {} pool addresses", pool_addresses.len());
        }

        Ok(())
    }

    /// Wait for a transaction to reach an accepted state with exponential backoff.
    ///
    /// Polls the transaction status with exponential backoff until it reaches
    /// Executed(Accept) or Completed(Accept), or fails/times out.
    ///
    /// Returns the final accepted status.
    fn wait_for_transaction_acceptance(
        &mut self,
        tx_hash: Hash,
        shard: ShardGroupId,
        description: &str,
    ) -> Result<TransactionStatus, SimulatorError> {
        const INITIAL_WAIT_MS: u64 = 500;   // Increased for cross-shard TXs
        const MAX_WAIT_MS: u64 = 10000;      // Increased to 10 seconds per iteration
        const MAX_ATTEMPTS: u32 = 50;        // More attempts with longer waits

        let mut wait_ms = INITIAL_WAIT_MS;

        for attempt in 0..MAX_ATTEMPTS {
            // Wait with current backoff
            let wait_duration = Duration::from_millis(wait_ms);
            let target_time = self.runner.now() + wait_duration;
            self.runner.run_until(target_time);

            // Check transaction status using tx_status cache (survives eviction from mempool)
            let node_idx = self.get_node_for_shard(shard);

            let status = self.runner.tx_status(node_idx, &tx_hash)
                .ok_or_else(|| SimulatorError::Workload(format!("{} transaction not found", description)))?
                .clone();

            // Log status on each attempt for debugging
            if attempt % 5 == 0 || attempt == MAX_ATTEMPTS - 1 {
                tracing::info!(
                    attempt,
                    ?status,
                    wait_ms,
                    "Transaction status: {}", description
                );
            }

            // Check if we've reached a terminal state where writes have been applied
            match status {
                TransactionStatus::Completed(TransactionDecision::Accept) => {
                    // State writes have been applied - safe to use results
                    debug!(
                        ?tx_hash,
                        ?status,
                        attempt,
                        total_wait_ms = wait_ms * attempt as u64,
                        "{} succeeded and state committed",
                        description
                    );
                    return Ok(status);
                }
                TransactionStatus::Completed(TransactionDecision::Reject)
                | TransactionStatus::Aborted { .. } => {
                    // Try to get detailed error from execution result
                    let error_detail = self.runner.execution_result(&tx_hash)
                        .and_then(|result| result.error.clone())
                        .unwrap_or_else(|| "No error details available".to_string());

                    return Err(SimulatorError::Workload(format!(
                        "{} failed with status: {:?}. Error: {}",
                        description, status, error_detail
                    )));
                }
                TransactionStatus::Pending
                | TransactionStatus::Committed(_)
                | TransactionStatus::Executed(_) => {
                    // Still processing (Executed means cert created but not yet in block)
                    debug!(
                        ?tx_hash,
                        ?status,
                        attempt,
                        wait_ms,
                        "{} still processing (waiting for state commit), retrying...",
                        description
                    );
                }
                _ => {
                    // Unexpected status (Blocked, Retried, etc.)
                    return Err(SimulatorError::Workload(format!(
                        "{} reached unexpected status: {:?}",
                        description, status
                    )));
                }
            }

            // Exponential backoff (double the wait time, up to max)
            wait_ms = (wait_ms * 2).min(MAX_WAIT_MS);
        }

        Err(SimulatorError::Workload(format!(
            "{} timed out after {} attempts",
            description, MAX_ATTEMPTS
        )))
    }

    /// Publish the Radiswap WASM package and return its address.
    fn publish_radiswap_package(&mut self) -> Result<radix_common::prelude::PackageAddress, SimulatorError> {
        use radix_common::prelude::*;
        use radix_engine_interface::blueprints::package::PackageDefinition;
        use radix_engine_interface::prelude::OwnerRole;
        use radix_transactions::prelude::ManifestBuilder;
        use hyperscale_types::sign_and_notarize;
        use std::collections::BTreeMap;

        // Read WASM bytes
        let wasm_path = std::path::Path::new("scrypto/radiswap/target/wasm32-unknown-unknown/release/radiswap.wasm");
        let wasm_bytes = std::fs::read(wasm_path).map_err(|e| {
            SimulatorError::Workload(format!(
                "Failed to read Radiswap WASM at {:?}: {}. Did you run `cd scrypto/radiswap && cargo build --target wasm32-unknown-unknown --release`?",
                wasm_path, e
            ))
        })?;

        info!(wasm_size = wasm_bytes.len(), "Radiswap WASM loaded");

        // Read RPD (Radix Package Definition)
        let rpd_path = std::path::Path::new("scrypto/radiswap/target/wasm32-unknown-unknown/release/radiswap.rpd");
        let rpd_bytes = std::fs::read(rpd_path).map_err(|e| {
            SimulatorError::Workload(format!(
                "Failed to read Radiswap RPD at {:?}: {}",
                rpd_path, e
            ))
        })?;

        let package_definition = manifest_decode::<PackageDefinition>(&rpd_bytes)
            .map_err(|e| SimulatorError::Workload(format!("Failed to decode RPD: {:?}", e)))?;

        info!("Package definition loaded");

        // Create a deployer account (use first account from first shard)
        let accounts = self.accounts
            .accounts_for_shard(hyperscale_types::ShardGroupId(0))
            .ok_or_else(|| SimulatorError::Workload("No accounts on shard 0".to_string()))?;
        let deployer = accounts.first()
            .ok_or_else(|| SimulatorError::Workload("No accounts available for deployment".to_string()))?;

        // Build publish manifest
        let manifest = ManifestBuilder::new()
            .lock_fee(deployer.address, Decimal::from(500u32))  // Higher fee for package publish
            .publish_package_advanced(
                None,  // owner_role: None means no owner
                wasm_bytes.into(),
                package_definition,
                BTreeMap::new(),  // metadata
                OwnerRole::None,
            )
            .build();

        // Sign and notarize
        let nonce = deployer.next_nonce();
        let network = NetworkDefinition::simulator();
        let notarized = sign_and_notarize(manifest, &network, nonce as u32, &deployer.keypair)
            .map_err(|e| SimulatorError::Workload(format!("Failed to sign publish transaction: {:?}", e)))?;

        let tx: hyperscale_types::RoutableTransaction = notarized.try_into()
            .map_err(|e| SimulatorError::Workload(format!("Failed to convert to RoutableTransaction: {:?}", e)))?;

        info!(tx_hash = ?tx.hash(), "Publish transaction built");

        // Submit transaction to all nodes in shard 0 (where deployer is)
        let tx_hash = tx.hash();
        let tx_arc = std::sync::Arc::new(tx);
        let target_shard = hyperscale_types::ShardGroupId(0);

        for node_idx in self.nodes_for_shard(target_shard) {
            self.runner.schedule_initial_event(
                node_idx,
                Duration::ZERO,
                Event::SubmitTransaction {
                    tx: std::sync::Arc::clone(&tx_arc),
                },
            );
        }

        // Wait for transaction to be accepted with exponential backoff
        info!("Waiting for publish transaction to commit...");
        self.wait_for_transaction_acceptance(tx_hash, target_shard, "Publish Radiswap package")?;
        info!("Publish transaction committed successfully");

        // Extract PackageAddress from execution result
        let execution_result = self.runner.execution_result(&tx_hash)
            .ok_or_else(|| SimulatorError::Workload(
                "No execution result for package publish transaction".to_string()
            ))?;

        let package_node_id = execution_result.new_packages.first()
            .ok_or_else(|| SimulatorError::Workload(
                "Package publish did not create a package address".to_string()
            ))?;

        let package_address = PackageAddress::try_from(package_node_id.0.as_slice())
            .map_err(|e| SimulatorError::Workload(format!(
                "Failed to convert NodeId to PackageAddress: {:?}", e
            )))?;

        Ok(package_address)
    }

    /// Create custom fungible token resources.
    ///
    /// Creates the specified number of custom tokens that will be used in AMM pools.
    /// Each token is created as a divisible fungible resource with 18 decimals.
    fn create_custom_tokens(&mut self, num_tokens: usize) -> Result<(), SimulatorError> {
        use radix_common::prelude::*;
        use radix_engine_interface::prelude::{OwnerRole, FungibleResourceRoles, dec, ModuleConfig, RoleAssignmentInit, MetadataValue};
        use radix_transactions::prelude::ManifestBuilder;
        use hyperscale_types::sign_and_notarize;
        use std::collections::BTreeMap;

        // Collect all created resource NodeIds for replication
        let mut created_resource_node_ids = Vec::new();

        // Create each token
        for i in 0..num_tokens {
            let token_name = format!("TOKEN_{}", (b'A' + i as u8) as char);
            let token_symbol = format!("TKN{}", (b'A' + i as u8) as char);

            info!(token_name = %token_name, "Creating custom token");

            // Build, sign and submit transaction in a scope to release the borrow
            let (tx_hash, target_shard) = {
                // Get deployer account (same as package deployer)
                let accounts = self.accounts
                    .accounts_for_shard(hyperscale_types::ShardGroupId(0))
                    .ok_or_else(|| SimulatorError::Workload("No accounts on shard 0".to_string()))?;
                let deployer = accounts.first()
                    .ok_or_else(|| SimulatorError::Workload("No accounts available for token creation".to_string()))?;

                // Build manifest to create fungible token
                let manifest = ManifestBuilder::new()
                    .lock_fee(deployer.address, Decimal::from(100u32))
                    .create_fungible_resource(
                        OwnerRole::None,
                        true,  // track_total_supply
                        18u8,  // divisibility (18 decimals like most tokens)
                        FungibleResourceRoles::default(),
                        ModuleConfig {
                            init: {
                                let mut metadata = BTreeMap::new();
                                metadata.insert("name".to_string(), MetadataValue::String(token_name.clone()));
                                metadata.insert("symbol".to_string(), MetadataValue::String(token_symbol));
                                metadata.into()
                            },
                            roles: RoleAssignmentInit::default(),
                        },
                        Some(dec!("1000000000")), // Initial supply: 1 billion tokens
                    )
                    .try_deposit_entire_worktop_or_abort(deployer.address, None)
                    .build();

                // Sign and notarize
                let nonce = deployer.next_nonce();
                let network = NetworkDefinition::simulator();
                let notarized = sign_and_notarize(manifest, &network, nonce as u32, &deployer.keypair)
                    .map_err(|e| SimulatorError::Workload(format!("Failed to sign token creation transaction: {:?}", e)))?;

                let tx: hyperscale_types::RoutableTransaction = notarized.try_into()
                    .map_err(|e| SimulatorError::Workload(format!("Failed to convert to RoutableTransaction: {:?}", e)))?;

                // Submit transaction
                let tx_hash = tx.hash();
                let tx_arc = std::sync::Arc::new(tx);
                let target_shard = hyperscale_types::ShardGroupId(0);

                for node_idx in self.nodes_for_shard(target_shard) {
                    self.runner.schedule_initial_event(
                        node_idx,
                        Duration::ZERO,
                        Event::SubmitTransaction {
                            tx: std::sync::Arc::clone(&tx_arc),
                        },
                    );
                }

                (tx_hash, target_shard)
            }; // End of scope - accounts borrow is dropped here

            // Wait for transaction acceptance with exponential backoff
            self.wait_for_transaction_acceptance(
                tx_hash,
                target_shard,
                &format!("Create token {}", token_name)
            )?;

            // Extract ResourceAddress from execution result
            let execution_result = self.runner.execution_result(&tx_hash)
                .ok_or_else(|| SimulatorError::Workload(
                    format!("No execution result for token {} creation", token_name)
                ))?;

            let resource_node_id = execution_result.new_resources.first()
                .ok_or_else(|| SimulatorError::Workload(
                    format!("Token {} creation did not create a resource address", token_name)
                ))?;

            let resource_address = radix_common::prelude::ResourceAddress::try_from(resource_node_id.0.as_slice())
                .map_err(|e| SimulatorError::Workload(format!(
                    "Failed to convert NodeId to ResourceAddress for token {}: {:?}", token_name, e
                )))?;

            // Update swap workload with the actual token address
            if let Some(swap_workload) = &mut self.swap_workload {
                swap_workload.set_custom_token_address(i, resource_address);
            }

            // Collect the resource node ID for replication
            created_resource_node_ids.push(*resource_node_id);

            info!(
                token_name = %token_name,
                ?resource_address,
                "Token created successfully"
            );
        }

        // Replicate all token resources to all shards
        if !created_resource_node_ids.is_empty() {
            info!("Replicating {} token resources to all shards", created_resource_node_ids.len());
            self.replicate_resource_state_to_all_shards(&created_resource_node_ids);
        }

        Ok(())
    }

    /// Distribute custom tokens to all simulation accounts.
    ///
    /// Transfers tokens from the deployer account to all simulation accounts so they can
    /// perform swap transactions.
    fn distribute_tokens_to_accounts(&mut self) -> Result<(), SimulatorError> {
        use radix_common::prelude::*;
        use radix_engine_interface::prelude::dec;
        use radix_transactions::prelude::ManifestBuilder;
        use hyperscale_types::sign_and_notarize;

        // Get swap workload and custom tokens
        let custom_tokens = {
            let swap_workload = self.swap_workload.as_ref()
                .ok_or_else(|| SimulatorError::Workload("Swap workload not configured".to_string()))?;
            swap_workload.custom_token_addresses().to_vec()
        };

        // Amount to send to each account (10,000 of each token for swapping)
        let transfer_amount = dec!("10000");

        // Only distribute to a subset of accounts to optimize setup time
        // We distribute to the first 30 accounts (15 per shard for 2 shards)
        // This is sufficient for swap testing while keeping setup fast
        let max_accounts_per_shard = 15;
        let total_accounts = self.accounts.total_accounts();
        info!(total_accounts, max_accounts_per_shard, "Distributing tokens to subset of accounts for fast setup");

        let mut accounts_funded = 0;
        let mut pending_txs = Vec::new();

        // Collect shards to avoid holding a borrow across the wait call
        let shards: Vec<_> = self.accounts.shards().collect();

        for shard in shards {
            if let Some(shard_accounts) = self.accounts.accounts_for_shard(shard) {
                // Clone account addresses to avoid borrowing self.accounts
                // Only take first N accounts per shard
                let account_addresses: Vec<_> = shard_accounts.iter()
                    .take(max_accounts_per_shard)
                    .map(|acc| acc.address)
                    .collect();

                for account_address in account_addresses {
                    // Build, sign and submit in a scope
                    let (tx_hash, target_shard) = {
                        // Get deployer account (holds all tokens)
                        let deployer_accounts = self.accounts
                            .accounts_for_shard(hyperscale_types::ShardGroupId(0))
                            .ok_or_else(|| SimulatorError::Workload("No accounts on shard 0".to_string()))?;
                        let deployer = deployer_accounts.first()
                            .ok_or_else(|| SimulatorError::Workload("No accounts available".to_string()))?;

                        // Build manifest to transfer all custom tokens to this account
                        let mut builder = ManifestBuilder::new()
                            .lock_fee(deployer.address, Decimal::from(50u32));

                        // Withdraw all custom tokens
                        for &token in &custom_tokens {
                            builder = builder.withdraw_from_account(deployer.address, token, transfer_amount);
                        }

                        // Deposit all to target account
                        builder = builder.try_deposit_entire_worktop_or_abort(account_address, None);

                        let manifest = builder.build();

                        // Sign and notarize
                        let nonce = deployer.next_nonce();
                        let network = NetworkDefinition::simulator();
                        let notarized = sign_and_notarize(manifest, &network, nonce as u32, &deployer.keypair)
                            .map_err(|e| SimulatorError::Workload(format!("Failed to sign token distribution: {:?}", e)))?;

                        let tx: hyperscale_types::RoutableTransaction = notarized.try_into()
                            .map_err(|e| SimulatorError::Workload(format!("Failed to convert to RoutableTransaction: {:?}", e)))?;

                        // Submit transaction
                        let tx_hash = tx.hash();
                        let tx_arc = std::sync::Arc::new(tx);
                        let target_shard = hyperscale_types::ShardGroupId(0);

                        for node_idx in self.nodes_for_shard(target_shard) {
                            self.runner.schedule_initial_event(
                                node_idx,
                                Duration::ZERO,
                                Event::SubmitTransaction {
                                    tx: std::sync::Arc::clone(&tx_arc),
                                },
                            );
                        }

                        (tx_hash, target_shard)
                    }; // End scope - accounts borrow dropped

                    // Collect transaction for batch processing
                    pending_txs.push((tx_hash, target_shard, account_address));
                    accounts_funded += 1;
                }
            }
        }

        // Run consensus for a bit to process all submitted transactions (30s for batch)
        info!(pending_count = pending_txs.len(), "Running consensus to process token distribution batch...");
        let batch_process_time = Duration::from_secs(30);
        self.runner.run_until(self.runner.now() + batch_process_time);

        // Wait for all token distribution transactions to complete
        info!("Checking token distribution transaction statuses...");
        for (tx_hash, target_shard, account_address) in pending_txs {
            self.wait_for_transaction_acceptance(
                tx_hash,
                target_shard,
                &format!("Distribute tokens to account {:?}", account_address)
            )?;
        }

        info!(accounts_funded, "Token distribution complete");
        Ok(())
    }

    /// Instantiate AMM pool components.
    ///
    /// Creates pool instances by calling Radiswap::new() for each pool configuration
    /// in the swap workload. Returns the list of pool component addresses.
    fn instantiate_pools(
        &mut self,
        radiswap_package: radix_common::prelude::PackageAddress,
        pool_count: usize,
    ) -> Result<Vec<radix_common::prelude::ComponentAddress>, SimulatorError> {
        use radix_common::prelude::*;
        use radix_engine_interface::prelude::OwnerRole;
        use radix_transactions::prelude::ManifestBuilder;
        use hyperscale_types::sign_and_notarize;

        let mut pool_addresses = Vec::new();

        // Instantiate each pool
        for i in 0..pool_count {
            // Get pool info
            let (token_a, token_b) = {
                let swap_workload = self.swap_workload.as_ref()
                    .ok_or_else(|| SimulatorError::Workload("Swap workload not configured".to_string()))?;
                let pool_info = swap_workload.pool_info(i)
                    .ok_or_else(|| SimulatorError::Workload(format!("Pool {} not found", i)))?;
                (pool_info.token_a, pool_info.token_b)
            };

            info!(
                pool_index = i,
                token_a = ?token_a,
                token_b = ?token_b,
                "Instantiating pool"
            );

            // Build, sign and submit in a scope
            let (tx_hash, target_shard) = {
                // Get deployer account
                let accounts = self.accounts
                    .accounts_for_shard(hyperscale_types::ShardGroupId(0))
                    .ok_or_else(|| SimulatorError::Workload("No accounts on shard 0".to_string()))?;
                let deployer = accounts.first()
                    .ok_or_else(|| SimulatorError::Workload("No accounts available for pool instantiation".to_string()))?;

                // Build manifest to instantiate Radiswap pool
                use radix_common::manifest_args;

                let manifest = ManifestBuilder::new()
                    .lock_fee(deployer.address, Decimal::from(100u32))
                    .call_function(
                        ManifestPackageAddress::Static(radiswap_package.into()),
                        "Radiswap",
                        "new",
                        manifest_args!(
                            OwnerRole::None,
                            token_a,
                            token_b
                        ),
                    )
                    .build();

                // Sign and notarize
                let nonce = deployer.next_nonce();
                let network = NetworkDefinition::simulator();
                let notarized = sign_and_notarize(manifest, &network, nonce as u32, &deployer.keypair)
                    .map_err(|e| SimulatorError::Workload(format!("Failed to sign pool instantiation: {:?}", e)))?;

                let tx: hyperscale_types::RoutableTransaction = notarized.try_into()
                    .map_err(|e| SimulatorError::Workload(format!("Failed to convert to RoutableTransaction: {:?}", e)))?;

                // Submit transaction
                let tx_hash = tx.hash();
                let tx_arc = std::sync::Arc::new(tx);
                let target_shard = hyperscale_types::ShardGroupId(0);

                for node_idx in self.nodes_for_shard(target_shard) {
                    self.runner.schedule_initial_event(
                        node_idx,
                        Duration::ZERO,
                        Event::SubmitTransaction {
                            tx: std::sync::Arc::clone(&tx_arc),
                        },
                    );
                }

                (tx_hash, target_shard)
            }; // End scope - accounts borrow dropped

            // Wait for transaction acceptance with exponential backoff
            self.wait_for_transaction_acceptance(
                tx_hash,
                target_shard,
                &format!("Instantiate pool {}", i)
            )?;

            // Extract ComponentAddress from execution result
            let execution_result = self.runner.execution_result(&tx_hash)
                .ok_or_else(|| SimulatorError::Workload(
                    format!("No execution result for pool {} instantiation", i)
                ))?;

            // Check if execution was successful
            if !execution_result.success {
                let error_msg = execution_result.error.as_ref()
                    .map(|e| e.as_str())
                    .unwrap_or("Unknown error");
                return Err(SimulatorError::Workload(format!(
                    "Pool {} instantiation failed: {}", i, error_msg
                )));
            }

            // Debug: log what entities were created
            debug!(
                pool = i,
                packages = execution_result.new_packages.len(),
                components = execution_result.new_components.len(),
                resources = execution_result.new_resources.len(),
                "Pool instantiation result"
            );

            let component_node_id = execution_result.new_components.first()
                .ok_or_else(|| SimulatorError::Workload(
                    format!("Pool {} instantiation did not create a component address. Found {} packages, {} resources",
                        i, execution_result.new_packages.len(), execution_result.new_resources.len())
                ))?;

            let component_address = ComponentAddress::try_from(component_node_id.0.as_slice())
                .map_err(|e| SimulatorError::Workload(format!(
                    "Failed to convert NodeId to ComponentAddress for pool {}: {:?}", i, e
                )))?;

            pool_addresses.push(component_address);
            info!(pool_index = i, ?component_address, "Pool instantiated successfully");
        }

        Ok(pool_addresses)
    }

    /// Add initial liquidity to all pools.
    ///
    /// For each pool, withdraws tokens from the deployer account and calls add_liquidity()
    /// to provide initial liquidity for trading.
    fn add_liquidity_to_pools(
        &mut self,
        pool_addresses: &[radix_common::prelude::ComponentAddress],
    ) -> Result<(), SimulatorError> {
        use radix_common::prelude::*;
        use radix_engine_interface::prelude::dec;
        use radix_transactions::prelude::ManifestBuilder;
        use hyperscale_types::sign_and_notarize;

        // Submit liquidity transactions sequentially (cross-shard TXs don't batch well)
        for (i, &pool_address) in pool_addresses.iter().enumerate() {
            // Get pool info
            let (token_a, token_b, _target_shard) = {
                let swap_workload = self.swap_workload.as_ref()
                    .ok_or_else(|| SimulatorError::Workload("Swap workload not configured".to_string()))?;
                let pool_info = swap_workload.pool_info(i)
                    .ok_or_else(|| SimulatorError::Workload(format!("Pool {} not found", i)))?;
                (pool_info.token_a, pool_info.token_b, pool_info.target_shard)
            };

            info!(
                pool_index = i,
                token_a = ?token_a,
                token_b = ?token_b,
                "Adding liquidity to pool"
            );

            // Build, sign and submit in a scope
            let tx_hash = {
                // Use different account for each pool to avoid nonce conflicts
                // Account 0 has all tokens initially, but we distributed to others
                let accounts = self.accounts
                    .accounts_for_shard(hyperscale_types::ShardGroupId(0))
                    .ok_or_else(|| SimulatorError::Workload("No accounts on shard 0".to_string()))?;
                // Use account index i % accounts.len() to cycle through accounts
                let account_idx = i.min(accounts.len() - 1);
                let deployer = &accounts[account_idx];

                // Amount of liquidity to add (100,000 of each token)
                let liquidity_amount = dec!("100000");

                // Build manifest to add liquidity
                use radix_common::manifest_args;
                let manifest = ManifestBuilder::new()
                    .lock_fee(deployer.address, Decimal::from(10u32))
                    .withdraw_from_account(deployer.address, token_a, liquidity_amount)
                    .withdraw_from_account(deployer.address, token_b, liquidity_amount)
                    .take_all_from_worktop(token_a, "token_a_bucket")
                    .take_all_from_worktop(token_b, "token_b_bucket")
                    .with_name_lookup(|builder, lookup| {
                        builder.call_method(
                            pool_address,
                            "add_liquidity",
                            manifest_args!(
                                lookup.bucket("token_a_bucket"),
                                lookup.bucket("token_b_bucket")
                            ),
                        )
                    })
                    .try_deposit_entire_worktop_or_abort(deployer.address, None)
                    .build();

                // Sign and notarize
                let nonce = deployer.next_nonce();
                let network = NetworkDefinition::simulator();
                let notarized = sign_and_notarize(manifest, &network, nonce as u32, &deployer.keypair)
                    .map_err(|e| SimulatorError::Workload(format!("Failed to sign add_liquidity transaction: {:?}", e)))?;

                let tx: hyperscale_types::RoutableTransaction = notarized.try_into()
                    .map_err(|e| SimulatorError::Workload(format!("Failed to convert to RoutableTransaction: {:?}", e)))?;

                // Submit transaction to SOURCE shard only (deployer account shard)
                // Cross-shard transactions are submitted to one shard, then gossiped/coordinated
                let tx_hash = tx.hash();
                let tx_arc = std::sync::Arc::new(tx);

                // Submit to shard 0 (deployer account's home shard)
                let source_shard = hyperscale_types::ShardGroupId(0);

                for node_idx in self.nodes_for_shard(source_shard) {
                    self.runner.schedule_initial_event(
                        node_idx,
                        Duration::ZERO,
                        Event::SubmitTransaction {
                            tx: std::sync::Arc::clone(&tx_arc),
                        },
                    );
                }

                tx_hash
            }; // End scope - accounts borrow dropped

            // Wait for this liquidity transaction to complete before submitting next
            // Check status on source_shard (shard 0) where the transaction was submitted
            self.wait_for_transaction_acceptance(
                tx_hash,
                hyperscale_types::ShardGroupId(0), // source_shard
                &format!("Add liquidity to pool {}", i)
            )?;
            info!(pool_index = i, "Liquidity added successfully");
        }

        Ok(())
    }

    /// Run the simulation for the specified duration.
    ///
    /// Returns a report with throughput and latency metrics.
    pub fn run_for(&mut self, duration: Duration) -> SimulationReport {
        let start_time = self.runner.now();
        self.metrics = MetricsCollector::new(start_time);

        let batch_interval = self.config.workload.batch_interval;
        let submission_end_time = start_time + duration;

        info!(
            duration_secs = duration.as_secs(),
            batch_interval_ms = batch_interval.as_millis(),
            batch_size = self.config.workload.batch_size,
            "Starting simulation"
        );

        // Main simulation loop
        let mut last_progress_time = start_time;
        let progress_interval = Duration::from_secs(5);

        while self.runner.now() < submission_end_time {
            // Generate and submit a batch of transactions
            let current_time = self.runner.now();

            // Generate mixed workload: swaps + transfers based on swap_ratio
            let mut batch: Vec<(hyperscale_types::RoutableTransaction, bool)> = Vec::new();
            let swap_ratio = self.config.workload.swap_ratio;

            for _ in 0..self.batch_size {
                // Decide whether to generate a swap or transfer
                let should_generate_swap = if let Some(ref _swap_workload) = self.swap_workload {
                    // Only generate swaps if swap workload is enabled and random check passes
                    rand::Rng::gen::<f64>(&mut self.rng) < swap_ratio
                } else {
                    false // No swap workload configured, generate transfers only
                };

                let tx = if should_generate_swap {
                    // Generate a swap transaction
                    let tx = self.swap_workload
                        .as_ref()
                        .unwrap()
                        .generate_one(&self.accounts, &mut self.rng)
                        .expect("Failed to generate swap transaction");
                    (tx, true) // true = is_swap
                } else {
                    // Generate a transfer transaction
                    let tx = self.transfer_workload
                        .generate_one(&self.accounts, &mut self.rng)
                        .expect("Failed to generate transfer transaction");
                    (tx, false) // false = is_transfer
                };

                batch.push(tx);
            }

            for (tx, is_swap) in batch {
                let hash = tx.hash();
                let target_shard = self.get_target_shard(&tx);
                let tx = std::sync::Arc::new(tx);

                // Submit to ALL validators in the shard to ensure the proposer has the tx.
                // This mirrors real-world behavior where clients submit to multiple validators.
                // Without this, transactions may miss the next block because gossip hasn't
                // propagated to the proposer yet when their proposal timer fires.
                let shard_nodes = self.nodes_for_shard(target_shard);
                for node_idx in shard_nodes {
                    self.runner.schedule_initial_event(
                        node_idx,
                        Duration::ZERO,
                        Event::SubmitTransaction {
                            tx: std::sync::Arc::clone(&tx),
                        },
                    );
                }

                self.in_flight.insert(hash, (current_time, target_shard));

                // Record submission type
                if is_swap {
                    self.metrics.record_swap_submission();
                } else {
                    self.metrics.record_transfer_submission();
                }
            }

            // First, run a tiny step to process the submitted transactions
            // This ensures they're in mempools before any proposal timers fire.
            // The event priority system processes Client events last, so we need
            // to advance time slightly to get them processed.
            self.runner
                .run_until(self.runner.now() + Duration::from_micros(1));

            // Advance simulation by one batch interval
            let next_time = self.runner.now() + batch_interval;
            self.runner.run_until(next_time);

            // Check for completed transactions
            self.check_completions();

            // Progress logging
            if self.runner.now() - last_progress_time >= progress_interval {
                self.log_progress(start_time, submission_end_time);
                last_progress_time = self.runner.now();
            }
        }

        // Simulation complete
        let end_time = self.runner.now();
        self.metrics.set_submission_end_time(end_time);
        self.metrics
            .set_in_flight_at_end(self.in_flight.len() as u64);
        info!(
            total_time_secs = (end_time - start_time).as_secs_f64(),
            "Simulation complete"
        );

        // Generate and return report
        let report = std::mem::replace(&mut self.metrics, MetricsCollector::new(Duration::ZERO))
            .finalize(end_time);

        report.print_summary();
        report
    }

    /// Check for completed transactions and record metrics.
    fn check_completions(&mut self) {
        let current_time = self.runner.now();

        // We need to check the transaction status cache for each in-flight transaction
        // Using tx_status() which captures all emitted statuses, even after eviction
        let hashes: Vec<Hash> = self.in_flight.keys().copied().collect();

        for hash in hashes {
            if let Some((submit_time, shard)) = self.in_flight.get(&hash).copied() {
                // Check status from the status cache (survives eviction from mempool)
                let node_idx = self.get_node_for_shard(shard);
                if let Some(status) = self.runner.tx_status(node_idx, &hash).cloned() {
                    // Only remove from in_flight when reaching a terminal state
                    if status.is_final() {
                        self.in_flight.remove(&hash);

                        match status {
                            TransactionStatus::Completed(TransactionDecision::Accept) => {
                                // Transaction fully executed - record completion and latency
                                let latency = current_time.saturating_sub(submit_time);
                                self.metrics.record_completion(latency);
                                debug!(
                                    ?hash,
                                    latency_ms = latency.as_millis(),
                                    "Transaction completed"
                                );
                            }
                            TransactionStatus::Completed(TransactionDecision::Reject)
                            | TransactionStatus::Aborted { .. } => {
                                // Check if this is a spurious rejection (retry rejected because original succeeded)
                                let is_spurious = if let Some(node) = self.runner.node(node_idx) {
                                    if let Some(tx) = node.mempool().get_transaction(&hash) {
                                        tx.retry_count() > 0
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                };

                                if is_spurious {
                                    self.metrics.record_spurious_rejection();
                                    debug!(?hash, %status, "Retry rejected (spurious - original succeeded)");
                                } else {
                                    self.metrics.record_rejection();
                                    debug!(?hash, %status, "Transaction rejected/aborted");
                                }
                            }
                            TransactionStatus::Retried { new_tx } => {
                                // Transaction was retried - track the new hash instead
                                self.in_flight.insert(new_tx, (submit_time, shard));
                                self.metrics.record_retry();
                                debug!(?hash, ?new_tx, "Transaction retried");
                            }
                            _ => unreachable!("Transaction status is not final: {:?}", status),
                        }
                    }
                }
            }
        }
    }

    /// Determine the target shard for a transaction.
    fn get_target_shard(&self, tx: &hyperscale_types::RoutableTransaction) -> ShardGroupId {
        tx.declared_writes
            .first()
            .map(|node_id| shard_for_node(node_id, self.config.num_shards as u64))
            .unwrap_or(ShardGroupId(0))
    }

    /// Get a node index for submitting to a shard (for status checks).
    fn get_node_for_shard(&self, shard: ShardGroupId) -> u32 {
        // Return the first validator in the shard
        shard.0 as u32 * self.config.validators_per_shard
    }

    /// Get all node indices in a shard.
    fn nodes_for_shard(&self, shard: ShardGroupId) -> Vec<NodeIndex> {
        let start = shard.0 as u32 * self.config.validators_per_shard;
        let end = start + self.config.validators_per_shard;
        (start..end).collect()
    }

    /// Replicate resource state to all shards.
    ///
    /// This is a workaround for cross-shard provisioning limitations.
    /// When a resource (token, pool, etc.) is created on one shard, we replicate
    /// its state to all other shards so that cross-shard transactions can access
    /// the resource definitions without needing recursive provisioning.
    ///
    /// This is called after resource creation transactions complete.
    fn replicate_resource_state_to_all_shards(&mut self, resource_node_ids: &[hyperscale_types::NodeId]) {
        if resource_node_ids.is_empty() {
            return;
        }

        // Get a reference node from shard 0 (where resources were created)
        let source_node_idx = self.nodes_for_shard(hyperscale_types::ShardGroupId(0))[0];
        let source_storage = self.runner.node_storage(source_node_idx)
            .expect("Source node storage should exist");
        let executor = self.runner.node_executor(source_node_idx);

        // Fetch state entries for the resources from shard 0
        let entries = executor.fetch_state_entries(source_storage, resource_node_ids);

        // Apply these entries to all nodes in other shards
        for shard_id in 0..self.config.num_shards {
            let shard = hyperscale_types::ShardGroupId(shard_id as u64);
            if shard.0 == 0 {
                // Skip shard 0 - it already has the state
                continue;
            }

            let nodes_in_shard = self.nodes_for_shard(shard);
            for node_idx in nodes_in_shard {
                let storage = self.runner.node_storage_mut(node_idx);

                // Convert entries to database updates and commit them
                let updates = hyperscale_engine::state_entries_to_database_updates(&entries);
                storage.commit(&updates);
            }
        }

    }

    /// Replicate pool component state to all shards.
    ///
    /// Similar to resource replication, this ensures all shards have the pool
    /// component definitions so cross-shard swap transactions can bootload them.
    /// The actual pool state (reserves, LP tokens) is still provisioned on-demand.
    fn replicate_pool_components(&mut self, pool_addresses: &[radix_common::prelude::ComponentAddress]) {
        if pool_addresses.is_empty() {
            return;
        }

        // Convert ComponentAddresses to NodeIds
        let pool_node_ids: Vec<hyperscale_types::NodeId> = pool_addresses
            .iter()
            .map(|addr| hyperscale_types::NodeId(addr.as_node_id().0))
            .collect();

        // Get source shard where pools were created (shard 0)
        let source_node_idx = self.nodes_for_shard(hyperscale_types::ShardGroupId(0))[0];
        let source_storage = self.runner.node_storage(source_node_idx)
            .expect("Source node storage should exist");
        let executor = self.runner.node_executor(source_node_idx);

        // Fetch state entries for the pool components from shard 0
        let entries = executor.fetch_state_entries(source_storage, &pool_node_ids);

        // Apply these entries to all nodes in other shards
        for shard_id in 0..self.config.num_shards {
            let shard = hyperscale_types::ShardGroupId(shard_id as u64);
            if shard.0 == 0 {
                // Skip shard 0 - it already has the state
                continue;
            }

            let nodes_in_shard = self.nodes_for_shard(shard);
            for node_idx in nodes_in_shard {
                let storage = self.runner.node_storage_mut(node_idx);

                // Convert entries to database updates and commit them
                let updates = hyperscale_engine::state_entries_to_database_updates(&entries);
                storage.commit(&updates);
            }
        }
    }

    /// Log progress during simulation.
    fn log_progress(&mut self, start_time: Duration, end_time: Duration) {
        let (submitted, completed, rejected) = self.metrics.current_stats();
        let elapsed = self.runner.now() - start_time;
        let remaining = end_time.saturating_sub(self.runner.now());

        let tps = if elapsed.as_secs_f64() > 0.0 {
            completed as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        // Aggregate lock contention stats from all shards
        let lock_stats = self.aggregate_lock_contention();

        // Take a sample
        self.metrics
            .sample(self.runner.now(), self.in_flight.len() as u64, lock_stats);

        info!(
            elapsed_secs = elapsed.as_secs(),
            remaining_secs = remaining.as_secs(),
            submitted,
            completed,
            rejected,
            in_flight = self.in_flight.len(),
            tps = format!("{:.2}", tps),
            "Simulation progress"
        );
    }

    /// Aggregate lock contention stats from all shards.
    ///
    /// Sums stats from the first validator of each shard (they all see the same mempool).
    fn aggregate_lock_contention(&self) -> LockContentionStats {
        let mut total = LockContentionStats::default();

        for shard_idx in 0..self.config.num_shards {
            let node_idx = shard_idx * self.config.validators_per_shard;
            if let Some(node) = self.runner.node(node_idx) {
                let stats = node.mempool().lock_contention_stats();
                total.locked_nodes += stats.locked_nodes;
                total.blocked_count += stats.blocked_count;
                total.pending_count += stats.pending_count;
                total.pending_blocked += stats.pending_blocked;
            }
        }

        total
    }

    /// Get the underlying simulation runner (for advanced use).
    pub fn runner(&self) -> &SimulationRunner {
        &self.runner
    }

    /// Get mutable access to the simulation runner.
    pub fn runner_mut(&mut self) -> &mut SimulationRunner {
        &mut self.runner
    }

    /// Get account usage statistics.
    pub fn account_usage_stats(&self) -> hyperscale_spammer::AccountUsageStats {
        self.accounts.usage_stats()
    }

    /// Analyze stuck transactions and potential livelocks.
    ///
    /// Returns a report of all incomplete transactions, grouped by status
    /// and shard, with potential cycle detection.
    pub fn analyze_livelocks(&self) -> crate::livelock::LivelockReport {
        let analyzer = crate::livelock::LivelockAnalyzer::from_runner(
            &self.runner,
            self.config.num_shards as u64,
            self.config.validators_per_shard,
        );
        analyzer.analyze()
    }

    /// Enable network traffic analysis for bandwidth estimation.
    ///
    /// Call this before `run_for()` to collect network traffic statistics.
    /// After the simulation, call `traffic_report()` to get the bandwidth report.
    pub fn enable_traffic_analysis(&mut self) {
        self.runner.enable_traffic_analysis();
    }

    /// Check if traffic analysis is enabled.
    pub fn has_traffic_analysis(&self) -> bool {
        self.runner.has_traffic_analysis()
    }

    /// Get a network traffic bandwidth report.
    ///
    /// Returns `None` if traffic analysis is not enabled.
    /// Call `enable_traffic_analysis()` before `run_for()` to collect data.
    pub fn traffic_report(&self) -> Option<hyperscale_simulation::BandwidthReport> {
        self.runner.traffic_report()
    }
}

/// Errors that can occur during simulation.
#[derive(Debug, thiserror::Error)]
pub enum SimulatorError {
    #[error("Account pool error: {0}")]
    AccountPool(#[from] AccountPoolError),

    #[error("Workload error: {0}")]
    Workload(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WorkloadConfig;

    #[test]
    fn test_simulator_creation() {
        let config = SimulatorConfig::new(1, 4)
            .with_accounts_per_shard(20)
            .with_workload(WorkloadConfig::transfers_only().with_batch_size(5));

        let simulator = Simulator::new(config);
        assert!(simulator.is_ok());
    }
}
