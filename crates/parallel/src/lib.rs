//! Parallel (non-deterministic) simulation for multi-core performance testing.
//!
//! Unlike the deterministic `hyperscale-simulation` crate which runs all nodes
//! sequentially on a single thread, this crate runs each node as an independent
//! tokio task, enabling multi-core utilization for performance testing.
//!
//! # Goals
//!
//! 1. **Multi-core Performance**: Utilize all available CPU cores
//! 2. **Per-Node Parallelism**: Each node runs in its own tokio task
//! 3. **Realistic Concurrency**: Better model real-world asynchronous behavior
//! 4. **Reuse Production Infrastructure**: Leverage thread pools from production
//! 5. **Feature Parity**: Support same network simulation features (latency, loss)
//!
//! # Non-Goals
//!
//! - **Determinism**: Results may vary between runs due to scheduling
//! - **Replacing Deterministic Simulation**: Use `hyperscale-simulation` for correctness testing
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                          ParallelSimulator                                  │
//! │                     (orchestrator - main tokio task)                        │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                                                                             │
//! │   ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                   │
//! │   │   Node 0      │  │   Node 1      │  │   Node 2      │  ...              │
//! │   │   (task)      │  │   (task)      │  │   (task)      │                   │
//! │   │               │  │               │  │               │                   │
//! │   │  ┌─────────┐  │  │  ┌─────────┐  │  │  ┌─────────┐  │                   │
//! │   │  │ State   │  │  │  │ State   │  │  │  │ State   │  │                   │
//! │   │  │ Machine │  │  │  │ Machine │  │  │  │ Machine │  │                   │
//! │   │  └─────────┘  │  │  └─────────┘  │  │  └─────────┘  │                   │
//! │   │  ┌─────────┐  │  │  ┌─────────┐  │  │  ┌─────────┐  │                   │
//! │   │  │ Storage │  │  │  │ Storage │  │  │  │ Storage │  │                   │
//! │   │  │+Executor│  │  │  │+Executor│  │  │  │+Executor│  │                   │
//! │   │  └─────────┘  │  │  └─────────┘  │  │  └─────────┘  │                   │
//! │   └───────┬───────┘  └───────┬───────┘  └───────┬───────┘                   │
//! │           │                  │                  │                           │
//! │           └──────────────────┼──────────────────┘                           │
//! │                              │                                              │
//! │                    ┌─────────▼─────────┐                                    │
//! │                    │  MessageRouter    │                                    │
//! │                    │  (DelayQueue +    │                                    │
//! │                    │   NetworkConfig)  │                                    │
//! │                    └───────────────────┘                                    │
//! │                                                                             │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │                    Shared Crypto Thread Pool (rayon)                        │
//! │                    - BLS signature verification                             │
//! │                    - Aggregated signature verification                      │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Transaction execution happens inline within each NodeTask (synchronous).
//! The crypto thread pool is shared across all nodes for signature verification.

mod config;
mod metrics;
mod node_task;
mod router;
mod simulator;

pub use config::ParallelConfig;
pub use metrics::{
    MetricsCollector, MetricsEvent, MetricsRx, MetricsTx, SharedMetrics, SimulationReport,
};
pub use node_task::{NodeHandle, NodeTask, NodeTaskConfig};
pub use router::{
    Destination, InboundMessage, MessageRouter, NodeRx, RoutedMessage, RouterStats,
    RouterStatsHandle, RouterTx,
};
pub use simulator::{ParallelError, ParallelSimulator};
