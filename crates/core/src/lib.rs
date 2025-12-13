//! Core types for Hyperscale consensus.
//!
//! This crate provides the foundational types for the consensus architecture:
//!
//! - [`Event`]: All possible inputs to the state machine
//! - [`Action`]: All possible outputs from the state machine
//! - [`EventPriority`]: Ordering priority for events at the same timestamp
//! - [`StateMachine`]: The trait that all state machines implement
//!
//! # Architecture
//!
//! The core is built on a simple event-driven model:
//!
//! ```text
//! Events → StateMachine::handle() → Actions
//! ```
//!
//! The state machine is:
//! - **Synchronous**: No async, no .await
//! - **Deterministic**: Same state + event = same actions
//! - **Pure-ish**: Mutates self, but performs no I/O
//!
//! All I/O is handled by the runner (simulation or production) which:
//! 1. Delivers events to the state machine
//! 2. Executes the returned actions
//! 3. Converts action results back into events

mod action;
mod event;
mod message;
mod traits;

pub use action::{Action, TransactionStatus};
pub use event::{Event, EventPriority};
pub use message::OutboundMessage;
pub use traits::{StateMachine, SubStateMachine};

/// Type alias for timer identification.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimerId {
    /// Block proposal timer (also used for implicit round advancement)
    Proposal,
    /// Periodic cleanup timer
    Cleanup,
    /// Global consensus timer (epoch management)
    GlobalConsensus,
    /// Transaction fetch timer for a specific block.
    /// Fires after a timeout if pending block is still missing transactions.
    TransactionFetch {
        /// The block hash that needs transactions fetched.
        block_hash: hyperscale_types::Hash,
    },
    /// Certificate fetch timer for a specific block.
    /// Fires after a timeout if pending block is still missing certificates.
    CertificateFetch {
        /// The block hash that needs certificates fetched.
        block_hash: hyperscale_types::Hash,
    },
}
