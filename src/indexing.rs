//! Passive query observation for future adaptive indexing.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{
    helpers::u128_to_u64_saturating,
    query::{ExecutionStatistics, PhysicalAccess, PhysicalPlan},
    storage::CollectionId,
};

/// Default number of observations that may wait for the worker.
pub const DEFAULT_OBSERVATION_CAPACITY: usize = 256;

/// Stable identifier for one physical query shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct QueryFingerprint(u64);

impl QueryFingerprint {
    /// Computes a deterministic fingerprint from a physical plan.
    #[must_use]
    pub fn from_plan(plan: &PhysicalPlan) -> Self {
        let mut hasher = StableHasher::default();
        plan.source().collection().hash(&mut hasher);
        match plan.source().access() {
            PhysicalAccess::CollectionScan { options } => {
                0_u8.hash(&mut hasher);
                options.limit().hash(&mut hasher);
                options.direction().hash(&mut hasher);
            }
            PhysicalAccess::PrimaryKeyLookup { .. } => 1_u8.hash(&mut hasher),
        }
        plan.mode().hash(&mut hasher);
        for operator in plan.operators() {
            operator.kind().hash(&mut hasher);
        }
        Self(hasher.finish())
    }

    /// Returns the raw fingerprint value.
    #[must_use]
    pub const fn as_u64(self) -> u64 { self.0 }
}

/// Source strategy observed during execution, Full collection scan or Direct lookup through the master `_id` index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ObservedAccess {
    CollectionScan,
    PrimaryKeyLookup,
}

/// One successfully executed query observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryObservation {
    pub collection: CollectionId,
    pub fingerprint: QueryFingerprint,
    pub access: ObservedAccess,
    pub statistics: ExecutionStatistics,
    pub elapsed: Duration,
}

impl QueryObservation {
    /// Builds an observation from a plan and its successful output.
    #[must_use]
    pub fn from_execution( plan: &PhysicalPlan, statistics: ExecutionStatistics, elapsed: Duration, ) -> Self {
        let access = match plan.source().access() {
            PhysicalAccess::CollectionScan { .. } => ObservedAccess::CollectionScan,
            PhysicalAccess::PrimaryKeyLookup { .. } => ObservedAccess::PrimaryKeyLookup,
        };
        Self {
            collection: plan.source().collection().clone(),
            fingerprint: QueryFingerprint::from_plan(plan),
            access,
            statistics,
            elapsed,
        }
    }
}

/// Aggregated in-memory metrics for one query shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryAggregate {
    pub collection: CollectionId,
    pub access: ObservedAccess,
    pub executions: u64,
    pub scanned: u64,
    pub returned: u64,
    pub elapsed_micros: u64,
}

/// Point-in-time passive observer state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexingSnapshot {
    /// Aggregates keyed by query fingerprint.
    pub queries: HashMap<QueryFingerprint, QueryAggregate>,
    /// Observations discarded because the bounded queue was full.
    pub dropped_full: u64,
    /// Observations discarded because the worker was unavailable.
    pub dropped_disconnected: u64,
}

#[derive(Debug)]
enum IndexingEvent {
    QueryExecuted(QueryObservation),
    Flush(mpsc::Sender<()>),
    Shutdown,
}

/// Autonomous passive indexing observer.
#[derive(Debug)]
pub struct IndexingEngine {
    sender: SyncSender<IndexingEvent>,
    aggregates: Arc<Mutex<HashMap<QueryFingerprint, QueryAggregate>>>,
    dropped_full: Arc<AtomicU64>,
    dropped_disconnected: Arc<AtomicU64>,
    worker: Option<JoinHandle<()>>,
}

impl IndexingEngine {
    /// Starts a passive observer with the default bounded capacity.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_OBSERVATION_CAPACITY)
    }

    /// Starts a passive observer with an explicit bounded capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let aggregates: Arc<Mutex<HashMap<QueryFingerprint, QueryAggregate>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let worker_aggregates = Arc::clone(&aggregates);
        let worker = thread::Builder::new()
            .name("og-index-observer".to_owned())
            .spawn(move || {
                while let Ok(event) = receiver.recv() {
                    match event {
                        IndexingEvent::QueryExecuted(observation) => {
                            let mut state = worker_aggregates
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            let aggregate =
                                state.entry(observation.fingerprint).or_insert_with(|| {
                                    QueryAggregate {
                                        collection: observation.collection.clone(),
                                        access: observation.access,
                                        executions: 0,
                                        scanned: 0,
                                        returned: 0,
                                        elapsed_micros: 0,
                                    }
                                });
                            aggregate.executions = aggregate.executions.saturating_add(1);
                            aggregate.scanned = aggregate
                                .scanned
                                .saturating_add(observation.statistics.scanned());
                            aggregate.returned = aggregate
                                .returned
                                .saturating_add(observation.statistics.returned());
                            let micros = u128_to_u64_saturating(observation.elapsed.as_micros());
                            aggregate.elapsed_micros =
                                aggregate.elapsed_micros.saturating_add(micros);
                        }
                        IndexingEvent::Flush(acknowledge) => {
                            let _ = acknowledge.send(());
                        }
                        IndexingEvent::Shutdown => break,
                    }
                }
            })
            .expect("index observer thread must start");

        Self {
            sender,
            aggregates,
            dropped_full: Arc::new(AtomicU64::new(0)),
            dropped_disconnected: Arc::new(AtomicU64::new(0)),
            worker: Some(worker),
        }
    }

    /// Attempts to enqueue an observation without blocking the query path.
    pub fn observe(&self, observation: QueryObservation) {
        match self
            .sender
            .try_send(IndexingEvent::QueryExecuted(observation))
        {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped_full.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.dropped_disconnected.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Returns a point-in-time copy of all in-memory metrics.
    #[must_use]
    pub fn snapshot(&self) -> IndexingSnapshot {
        let (acknowledge, acknowledged) = mpsc::channel();
        if self.sender.send(IndexingEvent::Flush(acknowledge)).is_ok() {
            let _ = acknowledged.recv();
        }

        let queries = self
            .aggregates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        IndexingSnapshot {
            queries,
            dropped_full: self.dropped_full.load(Ordering::Relaxed),
            dropped_disconnected: self.dropped_disconnected.load(Ordering::Relaxed),
        }
    }
}

impl Default for IndexingEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for IndexingEngine {
    fn drop(&mut self) {
        let _ = self.sender.send(IndexingEvent::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Default)]
struct StableHasher(u64);

impl Hasher for StableHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.0 == 0 {
            0xcbf2_9ce4_8422_2325
        } else {
            self.0
        };
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.0 = hash;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ query::{PhysicalPlan, PhysicalSource}, storage::CollectionId, };
    use std::{thread, time::Duration};
    fn plan() -> PhysicalPlan { PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse("users").unwrap()), [], ) .unwrap() }
    #[test] fn fingerprints_are_stable_for_equal_plan_shapes() { let first = plan(); let second = plan(); assert_eq!( QueryFingerprint::from_plan(&first), QueryFingerprint::from_plan(&second) ); }
    #[test] fn worker_aggregates_observations() { let engine = IndexingEngine::with_capacity(4); let physical = plan(); engine.observe(QueryObservation::from_execution( &physical, ExecutionStatistics::default(), Duration::from_micros(7), )); for _ in 0..100 { if !engine.snapshot().queries.is_empty() { break; } thread::yield_now(); } let snapshot = engine.snapshot(); let aggregate = snapshot .queries .get(&QueryFingerprint::from_plan(&physical)) .expect("observation must be aggregated"); assert_eq!(aggregate.executions, 1); assert_eq!(aggregate.elapsed_micros, 7); }
    #[test] fn zero_capacity_observer_never_blocks_sender() { let engine = IndexingEngine::with_capacity(0); let physical = plan(); for _ in 0..1_000 { engine.observe(QueryObservation::from_execution( &physical, ExecutionStatistics::default(), Duration::ZERO, )); } let snapshot = engine.snapshot(); assert!(snapshot.dropped_full <= 1_000); }
}
