//! Metrics collection for parallel simulation.
//!
//! Uses a hybrid approach:
//! - Atomic counters for fast TPS metrics
//! - Channel for latency samples (to avoid contention on latency histogram)
//!
//! Metrics events are sent from node tasks to the orchestrator via a channel.
//! The orchestrator deduplicates completion events (multiple validators may
//! report the same transaction completing) and computes final statistics.

use hyperscale_types::{Hash, TransactionDecision, TransactionStatus};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Sender for metrics events from nodes.
pub type MetricsTx = mpsc::Sender<MetricsEvent>;

/// Receiver for metrics events in orchestrator.
pub type MetricsRx = mpsc::Receiver<MetricsEvent>;

/// Events sent from nodes to the orchestrator for metrics collection.
#[derive(Debug, Clone)]
pub enum MetricsEvent {
    /// A transaction reached terminal status.
    TransactionCompleted {
        hash: Hash,
        status: TransactionStatus,
    },
}

/// Shared atomic counters for fast metrics.
///
/// These can be read from any thread without blocking.
#[derive(Debug, Default)]
pub struct SharedMetrics {
    /// Total transactions submitted.
    pub submitted: AtomicU64,
    /// Total transactions completed successfully.
    pub completed: AtomicU64,
    /// Total transactions rejected/aborted.
    pub rejected: AtomicU64,
    /// Total retries (transactions that entered Retried state).
    pub retries: AtomicU64,
}

impl SharedMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_submission(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_completion(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rejection(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of metrics.
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub submitted: u64,
    pub completed: u64,
    pub rejected: u64,
    pub retries: u64,
}

/// Collects metrics and computes final report.
///
/// Runs in the orchestrator task, receiving completion events from nodes.
pub struct MetricsCollector {
    shared: Arc<SharedMetrics>,
    metrics_rx: MetricsRx,
    /// In-flight transactions: hash -> submit_time
    in_flight: HashMap<Hash, Instant>,
    /// Latency samples in microseconds.
    latencies: Vec<u64>,
    start_time: Instant,
}

impl MetricsCollector {
    pub fn new(shared: Arc<SharedMetrics>, metrics_rx: MetricsRx) -> Self {
        Self {
            shared,
            metrics_rx,
            in_flight: HashMap::new(),
            latencies: Vec::new(),
            start_time: Instant::now(),
        }
    }

    /// Record a submission (called by orchestrator).
    pub fn record_submission(&mut self, hash: Hash) {
        self.shared.record_submission();
        self.in_flight.insert(hash, Instant::now());
    }

    /// Get the number of in-flight transactions.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Process pending completion events (call periodically).
    pub fn process_completions(&mut self) {
        while let Ok(event) = self.metrics_rx.try_recv() {
            match event {
                MetricsEvent::TransactionCompleted { hash, status } => {
                    // Only count first completion (dedup across validators)
                    if let Some(submit_time) = self.in_flight.remove(&hash) {
                        let latency = submit_time.elapsed().as_micros() as u64;
                        self.latencies.push(latency);

                        match status {
                            TransactionStatus::Completed(TransactionDecision::Accept) => {
                                self.shared.record_completion();
                            }
                            TransactionStatus::Completed(TransactionDecision::Reject)
                            | TransactionStatus::Aborted { .. } => {
                                self.shared.record_rejection();
                            }
                            TransactionStatus::Retried { new_tx } => {
                                // Track the new hash instead, preserving original submit time
                                self.in_flight.insert(new_tx, submit_time);
                                self.shared.record_retry();
                            }
                            _ => {} // Non-final status (shouldn't happen)
                        }
                    }
                }
            }
        }
    }

    /// Finalize and produce report.
    pub fn finalize(self, router_stats: crate::router::RouterStats) -> SimulationReport {
        let duration = self.start_time.elapsed();
        let snapshot = self.shared.snapshot();

        // Compute latency percentiles
        let mut latencies = self.latencies;
        latencies.sort_unstable();

        let p50 = percentile(&latencies, 0.50);
        let p90 = percentile(&latencies, 0.90);
        let p99 = percentile(&latencies, 0.99);
        let max = latencies.last().copied().unwrap_or(0);
        let avg = if latencies.is_empty() {
            0
        } else {
            latencies.iter().sum::<u64>() / latencies.len() as u64
        };

        let tps = if duration.as_secs_f64() > 0.0 {
            snapshot.completed as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        SimulationReport {
            duration,
            submitted: snapshot.submitted,
            completed: snapshot.completed,
            rejected: snapshot.rejected,
            retries: snapshot.retries,
            in_flight: self.in_flight.len() as u64,
            messages_dropped_loss: router_stats.dropped_loss,
            messages_dropped_partition: router_stats.dropped_partition,
            tps,
            latency_p50_us: p50,
            latency_p90_us: p90,
            latency_p99_us: p99,
            latency_max_us: max,
            latency_avg_us: avg,
        }
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Final simulation report.
#[derive(Debug, Clone)]
pub struct SimulationReport {
    pub duration: Duration,
    pub submitted: u64,
    pub completed: u64,
    pub rejected: u64,
    pub retries: u64,
    pub in_flight: u64,
    pub messages_dropped_loss: u64,
    pub messages_dropped_partition: u64,
    pub tps: f64,
    pub latency_p50_us: u64,
    pub latency_p90_us: u64,
    pub latency_p99_us: u64,
    pub latency_max_us: u64,
    pub latency_avg_us: u64,
}

impl SimulationReport {
    pub fn print_summary(&self) {
        println!("\n═══════════════════════════════════════════");
        println!("       PARALLEL SIMULATION REPORT           ");
        println!("═══════════════════════════════════════════");
        println!();
        println!("Transactions:");
        println!("  Submitted:  {}", self.submitted);
        println!("  Completed:  {}", self.completed);
        println!("  Rejected:   {}", self.rejected);
        println!("  Retries:    {}", self.retries);
        println!("  In-flight:  {} (at cutoff)", self.in_flight);
        println!();
        println!("Throughput:");
        println!("  Average TPS: {:.2}", self.tps);
        println!();
        println!("Latency (completed txs):");
        println!("  P50:  {:.3}ms", self.latency_p50_us as f64 / 1000.0);
        println!("  P90:  {:.3}ms", self.latency_p90_us as f64 / 1000.0);
        println!("  P99:  {:.3}ms", self.latency_p99_us as f64 / 1000.0);
        println!("  Max:  {:.3}ms", self.latency_max_us as f64 / 1000.0);
        println!("  Avg:  {:.3}ms", self.latency_avg_us as f64 / 1000.0);
        println!();
        println!("Message Drops:");
        println!("  Packet loss: {}", self.messages_dropped_loss);
        println!("  Partitions:  {}", self.messages_dropped_partition);
        println!();
        println!("Duration: {:.2}s", self.duration.as_secs_f64());
        println!("═══════════════════════════════════════════\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_metrics() {
        let metrics = SharedMetrics::new();

        metrics.record_submission();
        metrics.record_submission();
        metrics.record_completion();
        metrics.record_rejection();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.submitted, 2);
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.rejected, 1);
    }

    #[test]
    fn test_percentile() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        // For 10 elements: idx = floor(10 * p)
        // p=0.5 -> idx=5 -> value=6
        // p=0.9 -> idx=9 -> value=10
        // p=0.99 -> idx=9 (min of len-1) -> value=10
        assert_eq!(percentile(&data, 0.5), 6);
        assert_eq!(percentile(&data, 0.9), 10);
        assert_eq!(percentile(&data, 0.99), 10);

        let empty: Vec<u64> = vec![];
        assert_eq!(percentile(&empty, 0.5), 0);
    }
}
