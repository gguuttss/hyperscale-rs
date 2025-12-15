//! Transaction receipt utilities.
//!
//! Provides simplified receipt information extracted from Radix Engine receipts.

use hyperscale_types::NodeId;
use radix_common::types::{ComponentAddress, PackageAddress, ResourceAddress};
use radix_engine::transaction::TransactionReceipt;

/// Simplified receipt information for simulator use.
///
/// Extracts key information from a Radix Engine TransactionReceipt without
/// requiring the full receipt to be stored/serialized.
#[derive(Debug, Clone)]
pub struct ReceiptInfo {
    /// Whether the transaction committed successfully.
    pub success: bool,

    /// Newly created package addresses (in order of creation, as NodeId bytes).
    pub new_packages: Vec<NodeId>,

    /// Newly created component addresses (in order of creation, as NodeId bytes).
    pub new_components: Vec<NodeId>,

    /// Newly created resource addresses (in order of creation, as NodeId bytes).
    pub new_resources: Vec<NodeId>,

    /// Outcome description (for debugging).
    pub outcome: Option<String>,
}

impl ReceiptInfo {
    /// Extract receipt info from a Radix Engine TransactionReceipt.
    pub fn from_receipt(receipt: &TransactionReceipt) -> Self {
        use crate::execution::extract_state_updates;

        // Check if transaction was successful
        let success = matches!(&receipt.result, radix_engine::transaction::TransactionResult::Commit(_));

        // Extract new entity addresses from the receipt
        let mut new_packages = Vec::new();
        let mut new_components = Vec::new();
        let mut new_resources = Vec::new();

        // Extract from state updates (converted to database updates format)
        if let Some(updates) = extract_state_updates(receipt) {
            //  Iterate over node updates to find new entities
            for (db_node_key, _node_updates) in &updates.node_updates {
                // Extract NodeId from DbNodeKey (skip 20-byte hash prefix, take 30-byte NodeId)
                if db_node_key.len() >= 50 {
                    let node_id_bytes = &db_node_key[20..50];

                    if node_id_bytes.len() >= 30 {
                        let entity_type = node_id_bytes[0];

                        // Match entity types and create addresses
                        match entity_type {
                            0x0d => {
                                // GlobalPackage
                                let mut node_id = [0u8; 30];
                                node_id.copy_from_slice(node_id_bytes);
                                new_packages.push(NodeId(node_id));
                            }
                            0x60..=0x6F | 0xc0..=0xcF => {
                                // All component types:
                                // 0x60-0x6F: Global components
                                // 0xc0-0xcF: Internal components (created during instantiation)
                                let mut node_id = [0u8; 30];
                                node_id.copy_from_slice(node_id_bytes);
                                new_components.push(NodeId(node_id));
                            }
                            0x5d => {
                                // GlobalFungibleResourceManager
                                let mut node_id = [0u8; 30];
                                node_id.copy_from_slice(node_id_bytes);
                                new_resources.push(NodeId(node_id));
                            }
                            other => {
                                // Log unknown entity types to help debug
                                tracing::debug!(
                                    entity_type = other,
                                    "Skipping unknown entity type in receipt"
                                );
                            }
                        }
                    }
                }
            }
        }

        // Get outcome description
        let outcome = if let radix_engine::transaction::TransactionResult::Commit(commit) = &receipt.result {
            Some(format!("{:?}", commit.outcome))
        } else {
            None
        };

        Self {
            success,
            new_packages,
            new_components,
            new_resources,
            outcome,
        }
    }

    /// Get the first new package address (useful for single package deployments).
    pub fn first_package(&self) -> Option<PackageAddress> {
        self.new_packages
            .first()
            .and_then(|node_id| PackageAddress::try_from(node_id.0.as_slice()).ok())
    }

    /// Get the first new component address (useful for single component instantiations).
    pub fn first_component(&self) -> Option<ComponentAddress> {
        self.new_components
            .first()
            .and_then(|node_id| ComponentAddress::try_from(node_id.0.as_slice()).ok())
    }

    /// Get the first new resource address (useful for single resource creations).
    pub fn first_resource(&self) -> Option<ResourceAddress> {
        self.new_resources
            .first()
            .and_then(|node_id| ResourceAddress::try_from(node_id.0.as_slice()).ok())
    }
}
