//! AMM swap workload generator using Radiswap pools.
//!
//! This workload:
//! - Creates multiple Radiswap pools across shards
//! - Pre-creates custom tokens at genesis
//! - Adds liquidity from dedicated LP accounts
//! - Generates swap transactions across multiple pools

use crate::accounts::{AccountPool, FundedAccount, SelectionMode};
use crate::workloads::WorkloadGenerator;
use hyperscale_types::{sign_and_notarize, RoutableTransaction, ShardGroupId};
use radix_common::manifest_args;
use radix_common::math::Decimal;
use radix_common::network::NetworkDefinition;
use radix_common::prelude::ManifestArgs;
use radix_common::types::{ComponentAddress, NodeId, ResourceAddress};
use radix_transactions::builder::ManifestBuilder;
use rand::{Rng, RngCore};
use std::path::Path;
use tracing::{info, warn};

/// Information about a pool's configuration (public view).
#[derive(Debug, Clone, Copy)]
pub struct PoolInfo {
    /// First token in the pool.
    pub token_a: ResourceAddress,
    /// Second token in the pool.
    pub token_b: ResourceAddress,
    /// Target shard for this pool.
    pub target_shard: ShardGroupId,
    /// Whether the pool has been initialized.
    pub is_initialized: bool,
}

/// Configuration for a single Radiswap pool.
#[derive(Clone)]
pub struct PoolConfig {
    /// Pool component address (set after deployment).
    pub component_address: Option<ComponentAddress>,

    /// First token in the pool.
    pub token_a: ResourceAddress,

    /// Second token in the pool.
    pub token_b: ResourceAddress,

    /// Target shard for this pool.
    pub target_shard: ShardGroupId,

    /// LP account that owns this pool.
    pub lp_account: FundedAccount,
}

/// Generates swap transactions across multiple Radiswap AMM pools.
pub struct SwapWorkload {
    /// All configured pools.
    pools: Vec<PoolConfig>,

    /// Custom token addresses (created at genesis).
    /// Index 0 = Token A, Index 1 = Token B, etc.
    custom_tokens: Vec<ResourceAddress>,

    /// Ratio of cross-shard swaps (0.0 to 1.0).
    /// Cross-shard = account on different shard than pool.
    cross_shard_ratio: f64,

    /// Account selection mode for traders.
    selection_mode: SelectionMode,

    /// Swap amount range (min, max).
    amount_range: (Decimal, Decimal),

    /// Network definition for transaction signing.
    network: NetworkDefinition,
}

impl SwapWorkload {
    /// Read the Radiswap WASM package from disk.
    ///
    /// Looks for the compiled WASM at:
    /// `scrypto/radiswap/target/wasm32-unknown-unknown/release/radiswap.wasm`
    #[allow(dead_code)]
    fn read_radiswap_wasm() -> Result<Vec<u8>, SwapWorkloadError> {
        let wasm_path = Path::new("scrypto/radiswap/target/wasm32-unknown-unknown/release/radiswap.wasm");

        std::fs::read(wasm_path).map_err(|e| {
            SwapWorkloadError::InitializationFailed(format!(
                "Failed to read Radiswap WASM at {:?}: {}. Did you run `cd scrypto/radiswap && cargo build --target wasm32-unknown-unknown --release`?",
                wasm_path, e
            ))
        })
    }

    /// Create a new swap workload generator.
    ///
    /// # Default Configuration
    /// - Cross-shard ratio: 30%
    /// - Account selection: Random
    /// - Swap amount range: 10-100 tokens
    ///
    /// # Note
    /// Pools and tokens are not created yet. You must call:
    /// 1. `prepare_tokens(num_tokens)` - Generate token addresses for genesis
    /// 2. `prepare_pools(num_shards, num_pools)` - Configure pool distribution
    /// 3. `initialize(radiswap_package)` - Deploy pools (after network starts)
    ///
    /// # Example
    /// ```ignore
    /// let workload = SwapWorkload::new(NetworkDefinition::simulator())
    ///     .with_cross_shard_ratio(0.5)
    ///     .with_amount_range(dec!("50"), dec!("500"))
    ///     .with_selection_mode(SelectionMode::NoContention);
    /// ```
    pub fn new(network: NetworkDefinition) -> Self {
        Self {
            pools: Vec::new(),
            custom_tokens: Vec::new(),
            cross_shard_ratio: 0.3,
            selection_mode: SelectionMode::default(),
            amount_range: (Decimal::from(10u32), Decimal::from(100u32)),
            network,
        }
    }

    /// Set the cross-shard swap ratio (0.0 to 1.0).
    ///
    /// # Arguments
    /// * `ratio` - Fraction of swaps that should be cross-shard (automatically clamped to 0.0-1.0)
    ///
    /// # Example
    /// ```ignore
    /// let workload = SwapWorkload::new(network)
    ///     .with_cross_shard_ratio(0.5); // 50% cross-shard swaps
    /// ```
    pub fn with_cross_shard_ratio(mut self, ratio: f64) -> Self {
        self.cross_shard_ratio = ratio.clamp(0.0, 1.0);
        self
    }

    /// Set the account selection mode for traders.
    ///
    /// # Arguments
    /// * `mode` - Selection strategy (Random, RoundRobin, NoContention, or Zipf)
    ///
    /// # Example
    /// ```ignore
    /// use hyperscale_spammer::SelectionMode;
    ///
    /// let workload = SwapWorkload::new(network)
    ///     .with_selection_mode(SelectionMode::NoContention); // Minimize contention
    /// ```
    pub fn with_selection_mode(mut self, mode: SelectionMode) -> Self {
        self.selection_mode = mode;
        self
    }

    /// Set the swap amount range.
    ///
    /// Swap amounts will be randomly chosen between min and max for each transaction.
    ///
    /// # Arguments
    /// * `min` - Minimum swap amount (inclusive)
    /// * `max` - Maximum swap amount (inclusive)
    ///
    /// # Example
    /// ```ignore
    /// use radix_common::math::Decimal;
    ///
    /// let workload = SwapWorkload::new(network)
    ///     .with_amount_range(Decimal::from(50u32), Decimal::from(500u32));
    /// ```
    pub fn with_amount_range(mut self, min: Decimal, max: Decimal) -> Self {
        self.amount_range = (min, max);
        self
    }

    /// Create a workload optimized for testing cross-shard swaps.
    ///
    /// Pre-configured with:
    /// - 100% cross-shard swaps
    /// - No contention account selection
    /// - Larger swap amounts (100-1000)
    ///
    /// # Example
    /// ```ignore
    /// let workload = SwapWorkload::cross_shard_test(NetworkDefinition::simulator());
    /// ```
    pub fn cross_shard_test(network: NetworkDefinition) -> Self {
        Self::new(network)
            .with_cross_shard_ratio(1.0)
            .with_selection_mode(SelectionMode::NoContention)
            .with_amount_range(Decimal::from(100u32), Decimal::from(1000u32))
    }

    /// Create a workload with high contention for stress testing.
    ///
    /// Pre-configured with:
    /// - 30% cross-shard swaps (default)
    /// - Zipf distribution (creates hot accounts)
    /// - Variable swap amounts (10-500)
    ///
    /// # Example
    /// ```ignore
    /// let workload = SwapWorkload::high_contention(NetworkDefinition::simulator());
    /// ```
    pub fn high_contention(network: NetworkDefinition) -> Self {
        Self::new(network)
            .with_selection_mode(SelectionMode::Zipf { exponent: 1.5 })
            .with_amount_range(Decimal::from(10u32), Decimal::from(500u32))
    }

    /// Generate custom token addresses for pools.
    ///
    /// Call this at genesis time to create tokens that will be funded.
    /// Returns token addresses that need initial balances in genesis.
    pub fn prepare_tokens(&mut self, num_tokens: usize) -> Vec<(ResourceAddress, String)> {
        self.custom_tokens.clear();

        let mut tokens = Vec::new();

        for i in 0..num_tokens {
            // Generate deterministic resource address from seed
            // Format: "TOKEN_" + index (e.g., TOKEN_A, TOKEN_B, TOKEN_C, ...)
            let token_name = format!("TOKEN_{}", (b'A' + i as u8) as char);

            // Create deterministic NodeId for this token
            // ResourceAddress requires EntityType::GlobalFungibleResourceManager (0x5d)
            // NodeId format: [entity_type (1 byte), hash (29 bytes)]
            let token_seed = 0xDEADBEEF00000000u64 + i as u64;
            let mut node_id_bytes = [0u8; 30];
            node_id_bytes[0] = 0x5d; // EntityType::GlobalFungibleResourceManager
            node_id_bytes[1..9].copy_from_slice(&token_seed.to_le_bytes());
            let node_id = NodeId::from(node_id_bytes);
            let resource_address = ResourceAddress::new_or_panic(node_id.0);

            self.custom_tokens.push(resource_address);
            tokens.push((resource_address, token_name));
        }

        tokens
    }

    /// Prepare pool configurations.
    ///
    /// Call this before initialization to set up which pools will be created.
    ///
    /// # Arguments
    /// * `num_shards` - Number of shards to distribute pools across
    /// * `num_pools` - Number of pools to create (default: 4)
    pub fn prepare_pools(
        &mut self,
        num_shards: u64,
        num_pools: Option<usize>,
    ) -> Result<(), SwapWorkloadError> {
        let num_pools = num_pools.unwrap_or(4);

        if num_pools < 1 {
            return Err(SwapWorkloadError::InvalidConfig(
                "Must have at least 1 pool".to_string(),
            ));
        }

        if self.custom_tokens.is_empty() {
            return Err(SwapWorkloadError::InvalidConfig(
                "Must call prepare_tokens() before prepare_pools()".to_string(),
            ));
        }

        self.pools.clear();

        // Strategy: Create pools with different token pairs
        // For simplicity, only create custom Token ↔ Token pools (skip XRD pools)
        // 1. TokenA ↔ TokenB
        // 2. TokenA ↔ TokenC
        // 3. TokenB ↔ TokenC
        // etc.
        //
        // Distribute pools evenly across shards

        let mut pool_index = 0;

        // Create Token ↔ Token pools
        for i in 0..self.custom_tokens.len() {
            for j in (i + 1)..self.custom_tokens.len() {
                if pool_index >= num_pools {
                    break;
                }

                let target_shard = ShardGroupId((pool_index % num_shards as usize) as u64);

                // Generate LP account for this pool
                let lp_seed = 0xFFFFFFFF00000000u64 + pool_index as u64;
                let lp_account = FundedAccount::from_seed(lp_seed, num_shards);

                self.pools.push(PoolConfig {
                    component_address: None,
                    token_a: self.custom_tokens[i],
                    token_b: self.custom_tokens[j],
                    target_shard,
                    lp_account,
                });

                pool_index += 1;
            }

            if pool_index >= num_pools {
                break;
            }
        }

        Ok(())
    }

    /// Initialize pools and add liquidity.
    ///
    /// This should be called after genesis, once the network is running.
    /// Returns transactions that:
    /// 1. Publish Radiswap package (if not already published)
    /// 2. Instantiate pool components
    /// 3. Add initial liquidity
    ///
    /// # Arguments
    /// * `_radiswap_package` - Address of deployed Radiswap package (None = needs deployment)
    pub fn initialize(
        &mut self,
        _radiswap_package: Option<ComponentAddress>,
    ) -> Result<Vec<RoutableTransaction>, SwapWorkloadError> {
        if self.pools.is_empty() {
            return Err(SwapWorkloadError::NoPools);
        }

        info!("Starting pool initialization for {} pools", self.pools.len());

        // TODO: Implementation plan:
        //
        // Step 1: Publish Radiswap package (if not provided)
        // - Read WASM bytes from disk
        // - Create publish_package transaction
        // - Calculate resulting PackageAddress
        //
        // Step 2: For each pool, create instantiation transaction
        // - Call Radiswap::new(OwnerRole::None, token_a, token_b)
        // - Parse transaction receipt to get ComponentAddress
        // - Store in pool.component_address
        //
        // Step 3: For each pool, create add_liquidity transaction
        // - Withdraw tokens from LP account
        // - Call pool.add_liquidity(bucket_a, bucket_b)
        // - Deposit pool units back to LP
        //
        // BLOCKER: This requires executing transactions and reading receipts
        // to get addresses, which can't be done in a pure transaction builder.
        //
        // SOLUTION: Return "initialization script" that caller must execute
        // sequentially, updating pool addresses as they go.

        Err(SwapWorkloadError::InitializationFailed(
            "Auto-deployment not yet implemented. \
             Pool deployment requires sequential transaction execution \
             with receipt parsing to obtain component addresses. \
             For now, please deploy pools manually and set pool.component_address.".to_string()
        ))
    }

    /// Get all LP accounts for genesis funding.
    ///
    /// Returns (account_address, balances_needed) for each LP.
    /// Each LP needs XRD + all custom tokens for adding liquidity.
    pub fn lp_genesis_balances(&self, lp_balance_per_token: Decimal) -> Vec<(ComponentAddress, Vec<(ResourceAddress, Decimal)>)> {
        let mut result = Vec::new();

        for pool in &self.pools {
            let mut balances = Vec::new();

            // LP needs both tokens in the pool
            balances.push((pool.token_a, lp_balance_per_token));
            balances.push((pool.token_b, lp_balance_per_token));

            result.push((pool.lp_account.address, balances));
        }

        result
    }

    /// Get list of all LP account addresses.
    pub fn lp_accounts(&self) -> Vec<&FundedAccount> {
        self.pools.iter().map(|p| &p.lp_account).collect()
    }

    /// Get list of all custom token addresses.
    pub fn custom_token_addresses(&self) -> &[ResourceAddress] {
        &self.custom_tokens
    }

    /// Update a custom token address.
    ///
    /// This is called after token deployment to store the actual resource address.
    /// Also updates all pools that use this token.
    pub fn set_custom_token_address(&mut self, index: usize, address: ResourceAddress) {
        if index < self.custom_tokens.len() {
            let old_address = self.custom_tokens[index];
            self.custom_tokens[index] = address;

            // Update all pools that use this token
            for pool in &mut self.pools {
                if pool.token_a == old_address {
                    pool.token_a = address;
                }
                if pool.token_b == old_address {
                    pool.token_b = address;
                }
            }
        }
    }

    /// Get the number of configured pools.
    pub fn num_pools(&self) -> usize {
        self.pools.len()
    }

    /// Get the number of custom tokens.
    pub fn num_tokens(&self) -> usize {
        self.custom_tokens.len()
    }

    /// Get the current cross-shard swap ratio.
    pub fn cross_shard_ratio(&self) -> f64 {
        self.cross_shard_ratio
    }

    /// Get the current swap amount range.
    pub fn amount_range(&self) -> (Decimal, Decimal) {
        self.amount_range
    }

    /// Check if pools have been initialized (have component addresses).
    pub fn is_initialized(&self) -> bool {
        !self.pools.is_empty() && self.pools.iter().all(|p| p.component_address.is_some())
    }

    /// Get pool information for a specific index.
    pub fn pool_info(&self, index: usize) -> Option<PoolInfo> {
        self.pools.get(index).map(|p| PoolInfo {
            token_a: p.token_a,
            token_b: p.token_b,
            target_shard: p.target_shard,
            is_initialized: p.component_address.is_some(),
        })
    }

    /// Update a pool's component address.
    ///
    /// This is called after pool deployment to store the actual component address.
    pub fn set_pool_address(&mut self, index: usize, address: ComponentAddress) {
        if let Some(pool) = self.pools.get_mut(index) {
            pool.component_address = Some(address);
        }
    }

    /// Generate a random swap amount within configured range.
    fn random_amount<R: Rng + ?Sized>(&self, rng: &mut R) -> Decimal {
        let (min, max) = self.amount_range;
        let min_u64 = min.to_string().parse::<u64>().unwrap_or(10);
        let max_u64 = max.to_string().parse::<u64>().unwrap_or(100);

        let amount = rng.gen_range(min_u64..=max_u64);
        Decimal::from(amount)
    }

    /// Select a random pool for swapping.
    fn select_pool<R: Rng + ?Sized>(&self, rng: &mut R) -> Option<&PoolConfig> {
        if self.pools.is_empty() {
            return None;
        }

        let idx = rng.gen_range(0..self.pools.len());
        Some(&self.pools[idx])
    }

    /// Build a swap transaction.
    ///
    /// # Arguments
    /// * `trader` - Account performing the swap
    /// * `pool` - Pool to swap in
    /// * `input_token` - Token being swapped from
    /// * `amount` - Amount to swap
    fn build_swap(
        &self,
        trader: &FundedAccount,
        pool: &PoolConfig,
        input_token: ResourceAddress,
        amount: Decimal,
    ) -> Option<RoutableTransaction> {
        // Verify pool has a component address (must be initialized first)
        let pool_address = match pool.component_address {
            Some(addr) => addr,
            None => {
                warn!("Pool not initialized, cannot swap");
                return None;
            }
        };

        // Build manifest:
        // 1. Lock fee from trader
        // 2. Withdraw input tokens
        // 3. Call swap method on pool component
        // 4. Deposit output tokens back to trader
        let manifest = ManifestBuilder::new()
            .lock_fee(trader.address, Decimal::from(10u32))
            .withdraw_from_account(trader.address, input_token, amount)
            .take_all_from_worktop(input_token, "input_bucket")
            .with_name_lookup(|builder, lookup| {
                builder.call_method(
                    pool_address,
                    "swap",
                    manifest_args!(lookup.bucket("input_bucket")),
                )
            })
            .try_deposit_entire_worktop_or_abort(trader.address, None)
            .build();

        // Get and increment nonce atomically
        let nonce = trader.next_nonce();

        // Sign and notarize
        let notarized = match sign_and_notarize(manifest, &self.network, nonce as u32, &trader.keypair) {
            Ok(n) => n,
            Err(e) => {
                warn!(error = ?e, "Failed to sign swap transaction");
                return None;
            }
        };

        // Convert to RoutableTransaction
        let tx: RoutableTransaction = match notarized.try_into() {
            Ok(t) => t,
            Err(e) => {
                warn!(error = ?e, "Failed to convert swap to RoutableTransaction");
                return None;
            }
        };

        Some(tx)
    }
}

impl WorkloadGenerator for SwapWorkload {
    fn generate_one(
        &self,
        accounts: &AccountPool,
        rng: &mut dyn RngCore,
    ) -> Option<RoutableTransaction> {
        // Select a pool
        let pool = self.select_pool(rng)?;

        // Determine if this should be cross-shard
        let cross_shard = rng.gen_bool(self.cross_shard_ratio);

        // Select trader account
        let trader = if cross_shard {
            // Pick account from different shard than pool
            self.select_cross_shard_account(accounts, pool.target_shard, rng)?
        } else {
            // Pick account from same shard as pool
            self.select_same_shard_account(accounts, pool.target_shard, rng)?
        };

        // Randomly choose which token to swap (A→B or B→A)
        let input_token = if rng.gen_bool(0.5) {
            pool.token_a
        } else {
            pool.token_b
        };

        let amount = self.random_amount(rng);

        self.build_swap(&trader, pool, input_token, amount)
    }

    fn generate_batch(
        &self,
        accounts: &AccountPool,
        count: usize,
        rng: &mut dyn RngCore,
    ) -> Vec<RoutableTransaction> {
        (0..count)
            .filter_map(|_| self.generate_one(accounts, rng))
            .collect()
    }
}

impl SwapWorkload {
    /// Select an account from the same shard as the pool.
    fn select_same_shard_account<R: Rng + ?Sized>(
        &self,
        accounts: &AccountPool,
        shard: ShardGroupId,
        rng: &mut R,
    ) -> Option<FundedAccount> {
        // Use AccountPool's same_shard_pair method which respects selection mode,
        // and just take the first account from the pair
        let (account, _) = accounts.same_shard_pair(rng, self.selection_mode)?;

        // Check if the selected account is in the target shard
        if account.shard == shard {
            Some(account.clone())
        } else {
            // If not in target shard, fall back to selecting from shard directly
            let shard_accounts = accounts.accounts_for_shard(shard)?;
            if shard_accounts.is_empty() {
                return None;
            }
            // For non-NoContention modes, use random/zipf selection
            let idx = self.select_account_index(shard_accounts.len(), rng);
            Some(shard_accounts[idx].clone())
        }
    }

    /// Select an account from a different shard than the pool.
    fn select_cross_shard_account<R: Rng + ?Sized>(
        &self,
        accounts: &AccountPool,
        pool_shard: ShardGroupId,
        rng: &mut R,
    ) -> Option<FundedAccount> {
        if accounts.num_shards() < 2 {
            // Fall back to same-shard if only one shard exists
            return self.select_same_shard_account(accounts, pool_shard, rng);
        }

        // Use AccountPool's cross_shard_pair which respects selection mode
        let (account1, account2) = accounts.cross_shard_pair(rng, self.selection_mode)?;

        // Pick the account that's NOT in the pool's shard
        if account1.shard != pool_shard {
            Some(account1.clone())
        } else if account2.shard != pool_shard {
            Some(account2.clone())
        } else {
            // Fallback: manually pick a different shard
            let mut shard = ShardGroupId(rng.gen_range(0..accounts.num_shards()));
            while shard == pool_shard && accounts.num_shards() > 1 {
                shard = ShardGroupId(rng.gen_range(0..accounts.num_shards()));
            }

            let shard_accounts = accounts.accounts_for_shard(shard)?;
            if shard_accounts.is_empty() {
                return None;
            }

            let idx = self.select_account_index(shard_accounts.len(), rng);
            Some(shard_accounts[idx].clone())
        }
    }

    /// Select an account index based on selection mode (for fallback cases).
    fn select_account_index<R: Rng + ?Sized>(&self, num_accounts: usize, rng: &mut R) -> usize {
        use crate::accounts::SelectionMode;

        match self.selection_mode {
            SelectionMode::Random | SelectionMode::RoundRobin | SelectionMode::NoContention => {
                // For stateful modes, ideally use AccountPool methods.
                // This is a fallback that uses random selection.
                rng.gen_range(0..num_accounts)
            }
            SelectionMode::Zipf { exponent } => {
                let exp = exponent.max(1.0);
                let u: f64 = rng.gen();
                let idx = ((num_accounts as f64).powf(1.0 - u)).powf(1.0 / exp) as usize;
                idx.min(num_accounts - 1)
            }
        }
    }
}

/// Initialization step for pool deployment.
///
/// Note: This enum is currently not used. Pool initialization
/// requires custom token creation which must be done via transactions
/// after genesis. This is a placeholder for future implementation.
#[derive(Clone)]
#[allow(dead_code)]
pub enum InitializationStep {
    /// Publish the Radiswap package.
    PublishPackage {
        wasm_bytes: Vec<u8>,
    },

    /// Instantiate a pool component.
    InstantiatePool {
        package_address: ComponentAddress,
        token_a: ResourceAddress,
        token_b: ResourceAddress,
        pool_index: usize,
    },

    /// Add initial liquidity to a pool.
    AddLiquidity {
        pool_address: ComponentAddress,
        token_a: ResourceAddress,
        token_b: ResourceAddress,
        amount_a: Decimal,
        amount_b: Decimal,
    },
}

/// Errors that can occur during swap workload generation.
#[derive(Debug, thiserror::Error)]
pub enum SwapWorkloadError {
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Pool initialization failed: {0}")]
    InitializationFailed(String),

    #[error("No pools available")]
    NoPools,

    #[error("Token creation failed: {0}")]
    TokenCreationFailed(String),
}
