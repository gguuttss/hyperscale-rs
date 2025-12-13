//! View change component for liveness.
//!
//! Handles timeout detection and coordinated round increments when progress stalls.
//!
//! # HotStuff-2 QC Forwarding
//!
//! With the 2-chain commit rule, view change votes include the validator's `highest_qc`.
//! This ensures the new proposer learns about the highest certified block and can build
//! on it, preserving safety. The `highest_qc` is attached as **unsigned data** to allow
//! BLS signature aggregation.
//!
//! # Exponential Backoff
//!
//! View change timeouts use exponential backoff to prevent rapid repeated view changes
//! during network partitions or slow periods. The timeout doubles with each consecutive
//! view change at the same height, up to a maximum cap.

use hyperscale_types::{
    BlockHeight, KeyPair, PublicKey, QuorumCertificate, ShardGroupId, Signature, SignerBitfield,
    Topology, ValidatorId, ViewChangeCertificate, VotePower,
};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use hyperscale_core::{Action, Event, OutboundMessage, TimerId};
use hyperscale_messages::{ViewChangeVote, ViewChangeVoteGossip};

use crate::state::BftState;

/// Marker for view change vote pending signature verification.
/// The vote itself is returned in the verification callback event.
#[derive(Debug, Clone, Copy)]
struct PendingVoteVerification;

/// Marker for view change vote pending highest_qc verification.
/// The vote itself is returned in the verification callback event.
#[derive(Debug, Clone, Copy)]
struct PendingHighestQcVerification;

/// Marker for view change certificate pending signature verification.
/// The certificate itself is returned in the verification callback event.
#[derive(Debug, Clone, Copy)]
struct PendingCertificateVerification;

/// Maximum multiplier for exponential backoff (2^6 = 64x base timeout).
const MAX_BACKOFF_EXPONENT: u32 = 6;

/// Maximum allowed gap between highest_qc height and view change height.
/// This prevents accepting votes with absurdly high QC heights.
const MAX_QC_HEIGHT_GAP: u64 = 10;

/// View change state for a deterministic BFT node.
///
/// Unlike the async version that uses DashMap and AtomicU64, this version
/// uses plain HashMap and u64 since it's single-threaded.
pub struct ViewChangeState {
    /// Shard group identifier for replay protection.
    shard_group: ShardGroupId,

    /// Signing key for votes.
    signing_key: KeyPair,

    /// Network topology (single source of truth for committee/shard info).
    topology: Arc<dyn Topology>,

    /// Base view change timeout duration.
    base_timeout: Duration,

    /// Time of last progress (block commit).
    last_progress_time: Duration,

    /// Current round number.
    current_round: u64,

    /// Current height being tracked.
    current_height: u64,

    /// The round at which the current height started (for backoff calculation).
    /// Reset to 0 when height advances.
    base_round_for_height: u64,

    /// The round we've broadcast a view change vote for (if any).
    /// None means no vote broadcast yet for current height.
    /// This tracks which round we voted for, so we can vote again for higher rounds.
    vote_broadcast_for_round: Option<u64>,

    /// Collects view change votes: (height, new_round) -> map of voter -> (signature, highest_qc).
    vote_collector: HashMap<(u64, u64), BTreeMap<ValidatorId, (Signature, QuorumCertificate)>>,

    /// Highest QC we've seen (HotStuff-2 QC forwarding).
    highest_qc: QuorumCertificate,

    /// Highest QC seen from view change votes: (height, new_round) -> highest QC.
    highest_qc_collector: HashMap<(u64, u64), QuorumCertificate>,

    /// Votes pending vote signature verification.
    /// Key: (height, new_round, voter)
    pending_vote_verifications: HashMap<(u64, u64, ValidatorId), PendingVoteVerification>,

    /// Votes pending highest_qc verification (vote signature already verified).
    /// Key: (height, new_round, voter)
    pending_qc_verifications: HashMap<(u64, u64, ValidatorId), PendingHighestQcVerification>,

    /// Certificates pending signature verification.
    /// Key: (height, new_round)
    pending_cert_verifications: HashMap<(u64, u64), PendingCertificateVerification>,

    /// Current simulation time.
    now: Duration,
}

impl ViewChangeState {
    /// Create a new view change state.
    pub fn new(
        shard_group: ShardGroupId,
        signing_key: KeyPair,
        topology: Arc<dyn Topology>,
        timeout: Duration,
    ) -> Self {
        Self {
            shard_group,
            signing_key,
            topology,
            base_timeout: timeout,
            last_progress_time: Duration::ZERO,
            current_round: 0,
            current_height: 0,
            base_round_for_height: 0,
            vote_broadcast_for_round: None,
            vote_collector: HashMap::new(),
            highest_qc: QuorumCertificate::genesis(),
            highest_qc_collector: HashMap::new(),
            pending_vote_verifications: HashMap::new(),
            pending_qc_verifications: HashMap::new(),
            pending_cert_verifications: HashMap::new(),
            now: Duration::ZERO,
        }
    }

    /// Calculate the current timeout with exponential backoff.
    ///
    /// The timeout doubles for each round increment at the same height,
    /// up to a maximum of 2^MAX_BACKOFF_EXPONENT times the base timeout.
    fn current_timeout(&self) -> Duration {
        let rounds_since_base = self
            .current_round
            .saturating_sub(self.base_round_for_height);
        let exponent = (rounds_since_base as u32).min(MAX_BACKOFF_EXPONENT);
        self.base_timeout * 2u32.pow(exponent)
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Topology Accessors
    // ═══════════════════════════════════════════════════════════════════════════

    /// Get the local validator ID.
    fn validator_id(&self) -> ValidatorId {
        self.topology.local_validator_id()
    }

    /// Get the local shard.
    fn local_shard(&self) -> ShardGroupId {
        self.topology.local_shard()
    }

    /// Get the local committee.
    fn committee(&self) -> Cow<'_, [ValidatorId]> {
        self.topology.local_committee()
    }

    /// Get the total voting power.
    fn total_voting_power(&self) -> u64 {
        self.topology.local_voting_power()
    }

    /// Get voting power for a validator.
    fn voting_power(&self, validator_id: ValidatorId) -> u64 {
        self.topology.voting_power(validator_id).unwrap_or(0)
    }

    /// Get public key for a validator.
    fn public_key(&self, validator_id: ValidatorId) -> Option<PublicKey> {
        self.topology.public_key(validator_id)
    }

    /// Check if we have quorum.
    #[allow(dead_code)]
    fn has_quorum(&self, voting_power: u64) -> bool {
        self.topology.local_has_quorum(voting_power)
    }

    /// Get committee index for a validator.
    fn committee_index(&self, validator_id: ValidatorId) -> Option<usize> {
        self.topology.local_committee_index(validator_id)
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Public API
    // ═══════════════════════════════════════════════════════════════════════════

    /// Set the current time.
    pub fn set_time(&mut self, now: Duration) {
        self.now = now;
    }

    /// Get the current round.
    pub fn current_round(&self) -> u64 {
        self.current_round
    }

    /// Get the current height.
    pub fn current_height(&self) -> u64 {
        self.current_height
    }

    /// Get the highest QC.
    pub fn highest_qc(&self) -> &QuorumCertificate {
        &self.highest_qc
    }

    /// Update the highest QC we've seen.
    pub fn update_highest_qc(&mut self, qc: QuorumCertificate) {
        if qc.height > self.highest_qc.height {
            debug!(
                old_height = self.highest_qc.height.0,
                new_height = qc.height.0,
                "Updated highest QC for view change"
            );
            self.highest_qc = qc;
        }
    }

    /// Check if a view change should occur.
    ///
    /// Returns true if timeout (with exponential backoff) has elapsed since last progress.
    pub fn should_change_view(&self) -> bool {
        // Don't trigger view change at genesis height
        if self.current_height == 0 {
            return false;
        }

        // Check if timeout elapsed (using exponential backoff)
        let elapsed = self.now.saturating_sub(self.last_progress_time);
        elapsed > self.current_timeout()
    }

    /// Reset timeout due to progress (block committed).
    pub fn reset_timeout(&mut self, height: u64) {
        let old_height = self.current_height;

        // Update height
        self.current_height = height;

        // If height increased, reset round and base_round to 0
        if height > old_height {
            self.current_round = 0;
            self.base_round_for_height = 0;
            debug!(height = height, "Progress made, reset to round 0");
        }

        // Update last progress time
        self.last_progress_time = self.now;

        // Reset view change state - clear any pending vote broadcast
        self.vote_broadcast_for_round = None;
        self.cleanup_old_votes(height);
    }

    /// Handle view change timer event.
    ///
    /// Returns actions to take (broadcast vote, set timer).
    pub fn on_view_change_timer(&mut self) -> Vec<Action> {
        let mut actions = vec![];

        // Always reschedule the timer with exponential backoff
        let timeout = self.current_timeout();
        actions.push(Action::SetTimer {
            id: TimerId::ViewChange,
            duration: timeout,
        });

        // Check if we should trigger view change
        if !self.should_change_view() {
            info!(
                current_height = self.current_height,
                now = ?self.now,
                last_progress_time = ?self.last_progress_time,
                timeout = ?timeout,
                "View change timer fired but should_change_view = false"
            );
            return actions;
        }

        info!(
            current_height = self.current_height,
            current_round = self.current_round,
            timeout = ?timeout,
            "View change timer fired, triggering view change"
        );

        // Create and broadcast view change vote
        if let Some(vote) = self.create_view_change_vote() {
            debug!(
                height = vote.height.0,
                current_round = vote.current_round,
                new_view = vote.new_view,
                "Broadcasting view change vote"
            );

            let gossip = ViewChangeVoteGossip { vote: vote.clone() };

            actions.push(Action::BroadcastToShard {
                shard: self.local_shard(),
                message: OutboundMessage::ViewChangeVote(gossip),
            });

            // Process our own vote directly (no verification needed - we just created it)
            // Skip the signature verification since we signed it ourselves
            actions.extend(self.finalize_vote(vote));
        }

        actions
    }

    /// Create a view change vote for broadcasting.
    ///
    /// Returns None if already broadcast a vote for this round or higher.
    pub fn create_view_change_vote(&mut self) -> Option<ViewChangeVote> {
        let height = BlockHeight(self.current_height);
        let current_round = self.current_round;
        let new_round = current_round + 1;

        // Only create vote if we haven't already voted for this round or higher
        if let Some(voted_round) = self.vote_broadcast_for_round {
            if voted_round >= new_round {
                return None;
            }
        }
        self.vote_broadcast_for_round = Some(new_round);

        // Sign the vote: (shard_group, height, new_round)
        let message = Self::view_change_message(self.shard_group, height, new_round);
        let signature = self.signing_key.sign(&message);

        Some(ViewChangeVote::new(
            height,
            current_round,
            new_round,
            self.validator_id(),
            self.highest_qc.clone(),
            signature,
        ))
    }

    /// Create the message bytes to sign for a view change vote.
    ///
    /// Uses the centralized `view_change_message` from `hyperscale_types::signing`.
    pub fn view_change_message(
        shard_group: ShardGroupId,
        height: BlockHeight,
        new_round: u64,
    ) -> Vec<u8> {
        hyperscale_types::view_change_message(shard_group, height, new_round)
    }

    /// Get the shard group ID (needed for signing message construction).
    pub fn shard_group(&self) -> ShardGroupId {
        self.shard_group
    }

    /// Handle a view change vote from another validator.
    ///
    /// Performs initial validation and delegates signature verification to the runner.
    /// Returns actions to verify the vote signature.
    pub fn on_view_change_vote(&mut self, vote: ViewChangeVote) -> Vec<Action> {
        let (height, new_round) = vote.vote_key();
        let vote_key = (height.0, new_round, vote.voter);

        // Ignore votes for old heights
        if height.0 < self.current_height {
            debug!(
                vote_height = height.0,
                current_height = self.current_height,
                "Ignoring view change vote for old height"
            );
            return vec![];
        }

        // Ignore votes for rounds we've already passed
        if height.0 == self.current_height && new_round <= self.current_round {
            debug!(
                new_round = new_round,
                current_round = self.current_round,
                "Ignoring view change vote for old/current round"
            );
            return vec![];
        }

        // Check if already pending verification
        if self.pending_vote_verifications.contains_key(&vote_key)
            || self.pending_qc_verifications.contains_key(&vote_key)
        {
            debug!(voter = ?vote.voter, "Vote already pending verification");
            return vec![];
        }

        // Check for duplicate (already accepted vote)
        let voters = self.vote_collector.get(&(height.0, new_round));
        if voters.is_some_and(|v| v.contains_key(&vote.voter)) {
            debug!(voter = ?vote.voter, "Ignoring duplicate view change vote");
            return vec![];
        }

        // Verify the voter is in the committee
        if self.committee_index(vote.voter).is_none() {
            warn!(voter = ?vote.voter, "View change vote from unknown validator");
            return vec![];
        }

        let public_key = match self.public_key(vote.voter) {
            Some(pk) => pk,
            None => {
                warn!(voter = ?vote.voter, "No public key for voter");
                return vec![];
            }
        };

        // Store pending verification marker
        self.pending_vote_verifications
            .insert(vote_key, PendingVoteVerification);

        // Delegate vote signature verification to runner
        let signing_message = Self::view_change_message(self.shard_group, height, new_round);
        vec![Action::VerifyViewChangeVoteSignature {
            vote,
            public_key,
            signing_message,
        }]
    }

    /// Handle vote signature verification result.
    ///
    /// If valid, proceeds to verify the highest_qc (if non-genesis).
    /// Returns actions to verify the QC signature, or finalizes the vote if genesis QC.
    pub fn on_vote_signature_verified(&mut self, vote: ViewChangeVote, valid: bool) -> Vec<Action> {
        let (height, new_round) = vote.vote_key();
        let vote_key = (height.0, new_round, vote.voter);

        // Remove from pending
        if self.pending_vote_verifications.remove(&vote_key).is_none() {
            warn!(voter = ?vote.voter, "Vote signature verified but not pending");
            return vec![];
        }

        if !valid {
            warn!(voter = ?vote.voter, "View change vote has invalid signature");
            return vec![];
        }

        // Check the attached highest_qc has quorum (structural check)
        let total_power = self.total_voting_power();
        if !vote.highest_qc.is_genesis() && !vote.highest_qc.has_quorum(total_power) {
            warn!(voter = ?vote.voter, "View change vote contains QC without quorum");
            return vec![];
        }

        // Bounds check: highest_qc.height should be reasonable relative to view change height.
        // The QC height should be at most equal to the view change height (can't have QC for future),
        // and should not be unreasonably far below (prevents accepting stale/malicious QCs).
        let (height, _new_round) = vote.vote_key();
        if !vote.highest_qc.is_genesis() {
            let qc_height = vote.highest_qc.height.0;
            // QC height must be <= view change height (can't certify future blocks)
            if qc_height > height.0 {
                warn!(
                    voter = ?vote.voter,
                    qc_height = qc_height,
                    view_change_height = height.0,
                    "View change vote contains QC for future height"
                );
                return vec![];
            }
            // QC height should be within MAX_QC_HEIGHT_GAP of the view change height
            // (prevents accepting ancient QCs that might cause issues)
            if height.0.saturating_sub(qc_height) > MAX_QC_HEIGHT_GAP {
                warn!(
                    voter = ?vote.voter,
                    qc_height = qc_height,
                    view_change_height = height.0,
                    max_gap = MAX_QC_HEIGHT_GAP,
                    "View change vote contains QC too far behind view change height"
                );
                return vec![];
            }
        }

        // If highest_qc is genesis, skip QC verification and finalize directly
        if vote.highest_qc.is_genesis() {
            return self.finalize_vote(vote);
        }

        // Get public keys for QC signers
        let signer_keys: Vec<PublicKey> = vote
            .highest_qc
            .signers
            .set_indices()
            .filter_map(|idx| {
                self.topology
                    .local_validator_at_index(idx)
                    .and_then(|v| self.public_key(v))
            })
            .collect();

        if signer_keys.is_empty() {
            warn!(voter = ?vote.voter, "No valid signer keys for highest_qc");
            return vec![];
        }

        // Store pending QC verification marker
        self.pending_qc_verifications
            .insert(vote_key, PendingHighestQcVerification);

        // Construct signing message for the highest_qc with domain separation
        let signing_message = BftState::block_vote_message(
            self.shard_group,
            vote.highest_qc.height.0,
            vote.highest_qc.round,
            &vote.highest_qc.block_hash,
        );

        // Delegate QC signature verification to runner
        vec![Action::VerifyViewChangeHighestQc {
            vote,
            public_keys: signer_keys,
            signing_message,
        }]
    }

    /// Handle highest_qc signature verification result.
    ///
    /// If valid, finalizes the vote and checks for quorum.
    pub fn on_highest_qc_verified(&mut self, vote: ViewChangeVote, valid: bool) -> Vec<Action> {
        let (height, new_round) = vote.vote_key();
        let vote_key = (height.0, new_round, vote.voter);

        // Remove from pending
        if self.pending_qc_verifications.remove(&vote_key).is_none() {
            warn!(voter = ?vote.voter, "Highest QC verified but not pending");
            return vec![];
        }

        if !valid {
            warn!(voter = ?vote.voter, "View change vote has invalid highest_qc signature");
            return vec![];
        }

        self.finalize_vote(vote)
    }

    /// Finalize a view change vote after all verifications pass.
    ///
    /// Adds the vote to the collector and checks for quorum.
    fn finalize_vote(&mut self, vote: ViewChangeVote) -> Vec<Action> {
        let (height, new_round) = vote.vote_key();
        let vote_key = (height.0, new_round);

        // Track the highest QC seen for this (height, new_round)
        self.highest_qc_collector
            .entry(vote_key)
            .and_modify(|existing| {
                if vote.highest_qc.height > existing.height {
                    *existing = vote.highest_qc.clone();
                }
            })
            .or_insert(vote.highest_qc.clone());

        // Add vote to collector
        let voters = self.vote_collector.entry(vote_key).or_default();

        // Double-check for duplicate (shouldn't happen, but be safe)
        if voters.contains_key(&vote.voter) {
            debug!(voter = ?vote.voter, "Ignoring duplicate view change vote in finalize");
            return vec![];
        }

        // Add new vote
        voters.insert(
            vote.voter,
            (vote.signature.clone(), vote.highest_qc.clone()),
        );

        // Calculate voting power
        let voter_ids: Vec<ValidatorId> = voters.keys().copied().collect();
        let vote_power = self.calculate_voting_power(&voter_ids);
        let total_power = self.total_voting_power();

        debug!(
            height = height.0,
            new_round = new_round,
            vote_power = vote_power,
            total_power = total_power,
            "View change vote added"
        );

        // Check for quorum
        if VotePower::has_quorum(vote_power, total_power) {
            debug!(
                height = height.0,
                new_round = new_round,
                "View change quorum reached"
            );

            // Emit internal signal to trigger apply_view_change.
            // Note: We emit ViewChangeQuorumReached (not ViewChangeCompleted) because
            // apply_view_change will emit ViewChangeCompleted with the correct highest_qc.
            // This prevents duplicate ViewChangeCompleted events.
            return vec![Action::EnqueueInternal {
                event: Event::ViewChangeQuorumReached {
                    height: height.0,
                    new_round,
                },
            }];
        }

        vec![]
    }

    /// Legacy method for backward compatibility with tests.
    /// Performs synchronous verification (NOT for production use).
    #[cfg(test)]
    pub fn add_view_change_vote(&mut self, vote: ViewChangeVote) -> Option<(u64, u64)> {
        let (height, new_round) = vote.vote_key();

        // Perform all the checks that on_view_change_vote does
        if height.0 < self.current_height {
            return None;
        }
        if height.0 == self.current_height && new_round <= self.current_round {
            return None;
        }
        self.committee_index(vote.voter)?;
        let public_key = self.public_key(vote.voter)?;

        // Synchronous signature verification (test only)
        let message = Self::view_change_message(self.shard_group, height, new_round);
        if !public_key.verify(&message, &vote.signature) {
            return None;
        }

        // Check QC quorum
        let total_power = self.total_voting_power();
        if !vote.highest_qc.is_genesis() && !vote.highest_qc.has_quorum(total_power) {
            return None;
        }

        // Finalize vote and check for quorum
        let actions = self.finalize_vote(vote);

        // Check if quorum was reached (ViewChangeQuorumReached emitted)
        for action in actions {
            if let Action::EnqueueInternal {
                event: Event::ViewChangeQuorumReached { height, new_round },
            } = action
            {
                return Some((height, new_round));
            }
        }

        None
    }

    /// Calculate total voting power for a set of voters.
    fn calculate_voting_power(&self, voters: &[ValidatorId]) -> u64 {
        voters.iter().map(|&v| self.voting_power(v)).sum()
    }

    /// Apply a view change after quorum reached.
    ///
    /// This is called when `add_view_change_vote` returns `Some((height, new_round))`,
    /// indicating quorum was reached. The caller must invoke this to actually apply
    /// the view change and get the resulting actions.
    pub fn apply_view_change(&mut self, height: u64, new_round: u64) -> Vec<Action> {
        // Verify this is a valid transition
        if height != self.current_height {
            warn!(
                height = height,
                current_height = self.current_height,
                "Cannot apply view change for different height"
            );
            return vec![];
        }

        if new_round <= self.current_round {
            debug!(
                new_round = new_round,
                current_round = self.current_round,
                "View change already applied"
            );
            return vec![];
        }

        // Update to new round
        self.current_round = new_round;
        self.last_progress_time = self.now;
        // Note: We do NOT reset vote_broadcast_for_round here because we want to
        // prevent voting for the same round again. The field tracks votes by round
        // number, so voting for a higher round is still allowed.

        info!(
            height = height,
            new_round = new_round,
            "Applied coordinated view change"
        );

        // Clean up old votes
        self.cleanup_old_votes(height);

        // Build and broadcast certificate
        let mut actions = vec![];
        let highest_qc = if let Some(cert) = self.build_certificate(BlockHeight(height), new_round)
        {
            let qc = cert.highest_qc.clone();
            let gossip = hyperscale_messages::ViewChangeCertificateGossip { certificate: cert };
            actions.push(Action::BroadcastToShard {
                shard: self.local_shard(),
                message: OutboundMessage::ViewChangeCertificate(gossip),
            });
            qc
        } else {
            // Fallback to our local highest QC
            self.highest_qc_collector
                .get(&(height, new_round))
                .cloned()
                .unwrap_or_else(QuorumCertificate::genesis)
        };

        // Emit internal event for BFT state to react to
        actions.push(Action::EnqueueInternal {
            event: Event::ViewChangeCompleted {
                height,
                new_round,
                highest_qc,
            },
        });

        actions
    }

    /// Build a ViewChangeCertificate from collected votes.
    pub fn build_certificate(
        &self,
        height: BlockHeight,
        new_round: u64,
    ) -> Option<ViewChangeCertificate> {
        let vote_key = (height.0, new_round);
        let voters = self.vote_collector.get(&vote_key)?;

        // Check quorum
        let vote_power: u64 = voters.keys().map(|&v| self.voting_power(v)).sum();
        let total_power = self.total_voting_power();

        if !VotePower::has_quorum(vote_power, total_power) {
            return None;
        }

        // Aggregate signatures
        let signatures: Vec<Signature> = voters.values().map(|(sig, _)| sig.clone()).collect();
        let aggregated_signature = match Signature::aggregate_bls(&signatures) {
            Ok(sig) => sig,
            Err(e) => {
                warn!(error = ?e, "Failed to aggregate view change signatures");
                return None;
            }
        };

        // Build signer bitfield
        let committee_size = self.committee().len();
        let mut signers = SignerBitfield::new(committee_size);
        for voter_id in voters.keys() {
            if let Some(idx) = self.committee_index(*voter_id) {
                signers.set(idx);
            }
        }

        // Get the highest QC collected
        let highest_qc = self
            .highest_qc_collector
            .get(&vote_key)
            .cloned()
            .unwrap_or_else(QuorumCertificate::genesis);
        let highest_qc_block_hash = highest_qc.block_hash;

        Some(ViewChangeCertificate {
            height,
            new_view: new_round,
            highest_qc,
            highest_qc_block_hash,
            aggregated_signature,
            signers,
            voting_power: VotePower(vote_power),
        })
    }

    /// Handle a received view change certificate.
    ///
    /// Performs initial validation and delegates signature verification to the runner.
    /// Returns actions to verify the certificate signature.
    pub fn on_view_change_certificate(&mut self, cert: ViewChangeCertificate) -> Vec<Action> {
        let cert_key = (cert.height.0, cert.new_round());

        // Verify certificate is for current height
        if cert.height.0 != self.current_height {
            debug!(
                cert_height = cert.height.0,
                current_height = self.current_height,
                "Ignoring view change certificate for different height"
            );
            return vec![];
        }

        // Verify certificate is for future round
        if cert.new_round() <= self.current_round {
            debug!(
                cert_round = cert.new_round(),
                current_round = self.current_round,
                "Ignoring view change certificate for old/current round"
            );
            return vec![];
        }

        // Check if already pending verification
        if self.pending_cert_verifications.contains_key(&cert_key) {
            debug!(
                height = cert.height.0,
                new_round = cert.new_round(),
                "Certificate already pending verification"
            );
            return vec![];
        }

        // Verify quorum
        let total_power = self.total_voting_power();
        if !cert.has_quorum(total_power) {
            warn!(
                voting_power = cert.voting_power.0,
                "View change certificate does not have quorum"
            );
            return vec![];
        }

        // Verify embedded highest_qc has quorum (structural check only)
        if !cert.highest_qc.is_genesis() && !cert.highest_qc.has_quorum(total_power) {
            warn!("View change certificate contains QC without quorum");
            return vec![];
        }

        // Get signer public keys from the bitfield
        let signer_keys: Vec<PublicKey> = cert
            .signers
            .set_indices()
            .filter_map(|idx| {
                self.topology
                    .local_validator_at_index(idx)
                    .and_then(|v| self.public_key(v))
            })
            .collect();

        if signer_keys.is_empty() {
            warn!("No signers in view change certificate");
            return vec![];
        }

        if signer_keys.len() != cert.signers.count() {
            warn!(
                expected = cert.signers.count(),
                found = signer_keys.len(),
                "Could not find public keys for all certificate signers"
            );
            return vec![];
        }

        // Store pending verification marker
        self.pending_cert_verifications
            .insert(cert_key, PendingCertificateVerification);

        // Construct the message that was signed
        let signing_message =
            Self::view_change_message(self.shard_group, cert.height, cert.new_round());

        // Delegate signature verification to runner
        vec![Action::VerifyViewChangeCertificateSignature {
            certificate: cert,
            public_keys: signer_keys,
            signing_message,
        }]
    }

    /// Handle certificate signature verification result.
    ///
    /// If valid, applies the view change.
    pub fn on_certificate_signature_verified(
        &mut self,
        cert: ViewChangeCertificate,
        valid: bool,
    ) -> Vec<Action> {
        let cert_key = (cert.height.0, cert.new_round());

        // Remove from pending
        if self.pending_cert_verifications.remove(&cert_key).is_none() {
            warn!(
                height = cert.height.0,
                new_round = cert.new_round(),
                "Certificate signature verified but not pending"
            );
            return vec![];
        }

        if !valid {
            warn!(
                height = cert.height.0,
                new_round = cert.new_round(),
                "View change certificate has invalid signature"
            );
            return vec![];
        }

        // Re-check round (may have advanced while waiting for verification)
        if cert.new_round() <= self.current_round {
            debug!(
                cert_round = cert.new_round(),
                current_round = self.current_round,
                "View change certificate for old/current round after verification"
            );
            return vec![];
        }

        // Apply the view change
        self.current_round = cert.new_round();
        self.last_progress_time = self.now;
        // Note: We do NOT reset vote_broadcast_for_round here - same reason as apply_view_change.

        info!(
            height = cert.height.0,
            new_round = cert.new_round(),
            "Applied view change from certificate"
        );

        self.cleanup_old_votes(cert.height.0);

        vec![Action::EnqueueInternal {
            event: Event::ViewChangeCompleted {
                height: cert.height.0,
                new_round: cert.new_round(),
                highest_qc: cert.highest_qc.clone(),
            },
        }]
    }

    /// Clean up view change votes for old heights and old rounds.
    ///
    /// Removes:
    /// - All votes for heights below current_height
    /// - All votes for old rounds at the current height (rounds <= current_round)
    fn cleanup_old_votes(&mut self, current_height: u64) {
        let current_round = self.current_round;

        // Remove votes for old heights AND old rounds at current height
        self.vote_collector.retain(|(height, round), _voters| {
            if *height < current_height {
                return false;
            }
            if *height == current_height && *round <= current_round {
                return false;
            }
            true
        });

        self.highest_qc_collector.retain(|(height, round), _qc| {
            if *height < current_height {
                return false;
            }
            if *height == current_height && *round <= current_round {
                return false;
            }
            true
        });

        // Also clean up pending verifications for old heights/rounds
        self.pending_vote_verifications
            .retain(|(height, round, _voter), _| {
                if *height < current_height {
                    return false;
                }
                if *height == current_height && *round <= current_round {
                    return false;
                }
                true
            });

        self.pending_qc_verifications
            .retain(|(height, round, _voter), _| {
                if *height < current_height {
                    return false;
                }
                if *height == current_height && *round <= current_round {
                    return false;
                }
                true
            });

        self.pending_cert_verifications
            .retain(|(height, round), _| {
                if *height < current_height {
                    return false;
                }
                if *height == current_height && *round <= current_round {
                    return false;
                }
                true
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperscale_types::{KeyPair, StaticTopology, ValidatorInfo, ValidatorSet};
    use tracing_test::traced_test;

    fn make_test_state() -> (ViewChangeState, Vec<KeyPair>) {
        let keys: Vec<KeyPair> = (0..4).map(|_| KeyPair::generate_bls()).collect();

        // Create validator set with ValidatorInfo
        let validators: Vec<ValidatorInfo> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| ValidatorInfo {
                validator_id: ValidatorId(i as u64),
                public_key: k.public_key(),
                voting_power: 1,
            })
            .collect();
        let validator_set = ValidatorSet::new(validators);

        // Create topology
        let topology = Arc::new(StaticTopology::new(ValidatorId(0), 1, validator_set));

        // Use the topology's shard group for consistency
        let shard_group = topology.local_shard();

        let state = ViewChangeState::new(
            shard_group,
            keys[0].clone(),
            topology,
            Duration::from_secs(5),
        );

        (state, keys)
    }

    #[traced_test]
    #[test]
    fn test_no_view_change_at_genesis() {
        let (state, _) = make_test_state();
        assert!(!state.should_change_view());
    }

    #[traced_test]
    #[test]
    fn test_view_change_after_timeout() {
        let (mut state, _) = make_test_state();

        // Set height and last progress
        state.current_height = 1;
        state.last_progress_time = Duration::ZERO;

        // Advance time past timeout
        state.now = Duration::from_secs(6);

        assert!(state.should_change_view());
    }

    #[traced_test]
    #[test]
    fn test_create_view_change_vote() {
        let (mut state, _) = make_test_state();
        state.current_height = 1;
        state.now = Duration::from_secs(1);

        // First vote should succeed
        let vote = state.create_view_change_vote();
        assert!(vote.is_some());
        let vote = vote.unwrap();
        assert_eq!(vote.height.0, 1);
        assert_eq!(vote.current_round, 0);
        assert_eq!(vote.new_view, 1);

        // Second vote should fail (already broadcast)
        let vote2 = state.create_view_change_vote();
        assert!(vote2.is_none());
    }

    #[traced_test]
    #[test]
    fn test_add_view_change_votes_quorum() {
        let (mut state, keys) = make_test_state();
        state.current_height = 1;

        // Use the state's shard_group for consistency
        let shard_group = state.shard_group();

        // Create votes from validators 1, 2, 3
        // Note: Using range loop intentionally because i is used both for indexing keys
        // and for constructing ValidatorId (which must match the key index).
        #[allow(clippy::needless_range_loop)]
        for i in 1..=3 {
            let message = hyperscale_types::view_change_message(shard_group, BlockHeight(1), 1);
            let signature = keys[i].sign(&message);
            let vote = ViewChangeVote::new(
                BlockHeight(1),
                0,
                1,
                ValidatorId(i as u64),
                QuorumCertificate::genesis(),
                signature,
            );

            let result = state.add_view_change_vote(vote);
            if i < 3 {
                assert!(result.is_none(), "Should not have quorum yet at vote {}", i);
            } else {
                assert_eq!(result, Some((1, 1)), "Should have quorum at vote 3");
            }
        }
    }

    #[traced_test]
    #[test]
    fn test_reset_timeout() {
        let (mut state, _) = make_test_state();
        state.current_height = 1;
        state.current_round = 2;
        state.now = Duration::from_secs(10);

        // Reset at same height - round should not reset
        state.reset_timeout(1);
        assert_eq!(state.current_round, 2);

        // Reset at new height - round should reset
        state.reset_timeout(2);
        assert_eq!(state.current_round, 0);
        assert_eq!(state.current_height, 2);
    }

    #[traced_test]
    #[test]
    fn test_exponential_backoff() {
        let (mut state, _) = make_test_state();
        let base_timeout = Duration::from_secs(5);

        state.current_height = 1;
        state.current_round = 0;
        state.base_round_for_height = 0;

        // Round 0: 1x base timeout
        assert_eq!(state.current_timeout(), base_timeout);

        // Round 1: 2x base timeout
        state.current_round = 1;
        assert_eq!(state.current_timeout(), base_timeout * 2);

        // Round 2: 4x base timeout
        state.current_round = 2;
        assert_eq!(state.current_timeout(), base_timeout * 4);

        // Round 3: 8x base timeout
        state.current_round = 3;
        assert_eq!(state.current_timeout(), base_timeout * 8);

        // Round 6: 64x base timeout (MAX_BACKOFF_EXPONENT = 6)
        state.current_round = 6;
        assert_eq!(state.current_timeout(), base_timeout * 64);

        // Round 10: still capped at 64x (MAX_BACKOFF_EXPONENT = 6)
        state.current_round = 10;
        assert_eq!(state.current_timeout(), base_timeout * 64);
    }

    #[traced_test]
    #[test]
    fn test_exponential_backoff_resets_on_height_change() {
        let (mut state, _) = make_test_state();
        let base_timeout = Duration::from_secs(5);

        state.current_height = 1;
        state.current_round = 5;
        state.base_round_for_height = 0;
        state.now = Duration::from_secs(100);

        // At round 5, timeout should be 32x base
        assert_eq!(state.current_timeout(), base_timeout * 32);

        // Reset to new height - backoff should reset
        state.reset_timeout(2);
        assert_eq!(state.current_round, 0);
        assert_eq!(state.base_round_for_height, 0);
        assert_eq!(state.current_timeout(), base_timeout);
    }

    #[traced_test]
    #[test]
    fn test_cleanup_old_rounds() {
        let (mut state, keys) = make_test_state();
        state.current_height = 1;
        state.current_round = 0;

        let shard_group = state.shard_group();

        // Add votes for round 1 at height 1
        let message = hyperscale_types::view_change_message(shard_group, BlockHeight(1), 1);
        let signature = keys[1].sign(&message);
        let vote = ViewChangeVote::new(
            BlockHeight(1),
            0,
            1,
            ValidatorId(1),
            QuorumCertificate::genesis(),
            signature,
        );
        state.add_view_change_vote(vote);

        // Add votes for round 2 at height 1
        let message2 = hyperscale_types::view_change_message(shard_group, BlockHeight(1), 2);
        let signature2 = keys[2].sign(&message2);
        let vote2 = ViewChangeVote::new(
            BlockHeight(1),
            1,
            2,
            ValidatorId(2),
            QuorumCertificate::genesis(),
            signature2,
        );
        state.add_view_change_vote(vote2);

        // Verify we have votes for both rounds
        assert!(state.vote_collector.contains_key(&(1, 1)));
        assert!(state.vote_collector.contains_key(&(1, 2)));

        // Advance to round 1 and cleanup
        state.current_round = 1;
        state.cleanup_old_votes(1);

        // Round 1 votes should be cleaned up (round <= current_round)
        assert!(!state.vote_collector.contains_key(&(1, 1)));
        // Round 2 votes should still be present
        assert!(state.vote_collector.contains_key(&(1, 2)));
    }

    #[traced_test]
    #[test]
    fn test_can_vote_for_higher_round_after_view_change() {
        // This tests the scenario where:
        // 1. View change to round 1 succeeds
        // 2. But no progress is made (no blocks committed)
        // 3. Timer fires again, validator should be able to vote for round 2
        let (mut state, _) = make_test_state();
        state.current_height = 1;
        state.current_round = 0;
        state.now = Duration::from_secs(10);
        state.last_progress_time = Duration::ZERO;

        // First vote should succeed (for round 1)
        let vote1 = state.create_view_change_vote();
        assert!(vote1.is_some());
        let vote1 = vote1.unwrap();
        assert_eq!(vote1.new_view, 1);
        assert_eq!(state.vote_broadcast_for_round, Some(1));

        // Second vote for same round should fail
        let vote1_again = state.create_view_change_vote();
        assert!(vote1_again.is_none());

        // Simulate view change completing to round 1
        state.current_round = 1;
        state.last_progress_time = state.now;
        // Note: vote_broadcast_for_round is NOT reset by apply_view_change

        // Advance time so another view change is triggered
        state.now = Duration::from_secs(20);

        // Now we should be able to vote for round 2
        let vote2 = state.create_view_change_vote();
        assert!(vote2.is_some());
        let vote2 = vote2.unwrap();
        assert_eq!(vote2.new_view, 2);
        assert_eq!(state.vote_broadcast_for_round, Some(2));

        // Third vote for round 2 should fail
        let vote2_again = state.create_view_change_vote();
        assert!(vote2_again.is_none());
    }

    #[traced_test]
    #[test]
    fn test_vote_broadcast_reset_on_height_change() {
        // When height changes (block committed), vote_broadcast_for_round should reset
        let (mut state, _) = make_test_state();
        state.current_height = 1;
        state.current_round = 0;
        state.now = Duration::from_secs(10);
        state.last_progress_time = Duration::ZERO;

        // Vote for round 1
        let vote1 = state.create_view_change_vote();
        assert!(vote1.is_some());
        assert_eq!(state.vote_broadcast_for_round, Some(1));

        // Simulate block commit at new height
        state.reset_timeout(2);
        assert_eq!(state.current_height, 2);
        assert_eq!(state.current_round, 0);
        assert_eq!(state.vote_broadcast_for_round, None);

        // Advance time
        state.now = Duration::from_secs(20);

        // Should be able to vote for round 1 at new height
        let vote_new_height = state.create_view_change_vote();
        assert!(vote_new_height.is_some());
        let vote_new_height = vote_new_height.unwrap();
        assert_eq!(vote_new_height.height.0, 2);
        assert_eq!(vote_new_height.new_view, 1);
    }
}
