//! Global memory-budget primitives.
//! To not be confused with the memory backend.
//! It provides a shared governor, RAII reservations and diagnostics that later patches can wire into caches and physical operators.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{ cell::{Cell, RefCell}, collections::{HashMap, VecDeque}, hash::BuildHasher, fmt, sync::{ atomic::{AtomicU64, AtomicUsize, Ordering}, Arc, Mutex, OnceLock, }, time::{Duration, Instant}, };

pub use crate::error::{MemoryReservationError, ProcessMemoryPressureError, QueryAdmissionError};

/// Logical consumer of governed memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum MemoryClass {
    /// Revocable index cache.
    PageCache,
    /// Memory owned by query execution operators.
    Query,
    /// Import parsing and batching buffers.
    Import,
    /// Protocol and socket buffers.
    Network,
    /// Index observation, construction and maintenance.
    Indexing,
    /// Logical and physical planning caches.
    Planner,
    /// Rows of repeated Place reads (`engine/results.rs`).
    Results,
}

impl MemoryClass {
    /// Stable diagnostic name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PageCache => "page_cache",
            Self::Query => "query",
            Self::Import => "import",
            Self::Network => "network",
            Self::Indexing => "indexing",
            Self::Planner => "planner",
            Self::Results => "results",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::PageCache => 0,
            Self::Query => 1,
            Self::Import => 2,
            Self::Network => 3,
            Self::Indexing => 4,
            Self::Planner => 5,
            Self::Results => 6,
        }
    }
}

impl fmt::Display for MemoryClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

use crate::helpers::{lock_unpoisoned, u128_to_usize_saturating};

const MEMORY_CLASS_COUNT: usize = 7;
pub const DEFAULT_MEMORY_EVENT_CAPACITY: usize = 1_024;
pub const MEMORY_EVENT_MIN_BYTES: usize = 2 * 1024 * 1024;
const MEMORY_CLASSES: [MemoryClass; MEMORY_CLASS_COUNT] = [
    MemoryClass::PageCache,
    MemoryClass::Query,
    MemoryClass::Import,
    MemoryClass::Network,
    MemoryClass::Indexing,
    MemoryClass::Planner,
    MemoryClass::Results,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum MemoryEventKind { Reserved, Released, Rejected, }

impl MemoryEventKind {
    /// Stable diagnostic name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Released => "released",
            Self::Rejected => "rejected",
        }
    }
}

impl fmt::Display for MemoryEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One bounded diagnostic event emitted by the governor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryEvent {
    /// Monotone event sequence, local to this governor.
    pub sequence: u64,
    /// Reservation class involved in the event.
    pub class: MemoryClass,
    /// Event category.
    pub kind: MemoryEventKind,
    /// Bytes accepted, released or requested.
    pub bytes: usize,
    /// Global reserved bytes immediately after the event.
    pub current_bytes: usize,
    /// Configured hard limit, when present.
    pub limit_bytes: Option<usize>,
}

#[derive(Debug)]
struct MemoryEventLog {
    capacity: usize,
    dropped: u64,
    events: VecDeque<MemoryEvent>,
}

impl MemoryEventLog {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            dropped: 0,
            events: VecDeque::with_capacity(capacity),
        }
    }

    fn push(&mut self, event: MemoryEvent) {
        if self.capacity == 0 {
            self.dropped = self.dropped.saturating_add(1);
            return;
        }
        if self.events.len() == self.capacity {
            self.events.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
        self.events.push_back(event);
    }
}

/// Snapshot of the bounded memory-event journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryEventSnapshot {
    /// Oldest-to-newest retained events.
    pub events: Vec<MemoryEvent>,
    /// Events discarded because the journal was full or disabled.
    pub dropped_events: u64,
    /// Configured journal capacity.
    pub capacity: usize,
}

#[derive(Debug, Default)]
struct ClassCounters {
    current_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
    observed_bytes: AtomicUsize,
    active_reservations: AtomicUsize,
    failed_reservations: AtomicU64,
}

/// Immutable diagnostics for one memory class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryClassSnapshot {
    /// Memory class represented by this row.
    pub class: MemoryClass,
    /// Bytes currently reserved and governed.
    pub current_bytes: usize,
    /// Highest number of simultaneously reserved bytes.
    pub peak_bytes: usize,
    /// Best-effort resident bytes observed for this class but not governed.
    pub observed_bytes: usize,
    /// Number of live RAII reservations.
    pub active_reservations: usize,
    /// Number of rejected reservation attempts.
    pub failed_reservations: u64,
}

/// Point-in-time diagnostics for the complete governor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySnapshot {
    /// Current Linux process memory metrics, when available.
    pub process: Option<ProcessMemorySnapshot>,
    /// Configured hard limit. `None` means unlimited.
    pub limit_bytes: Option<usize>,
    /// Bytes currently reserved across all classes.
    pub current_bytes: usize,
    /// Highest global reservation total observed.
    pub peak_bytes: usize,
    /// Remaining bytes, or `None` for an unlimited governor.
    pub available_bytes: Option<usize>,
    /// Total number of live reservations.
    pub active_reservations: usize,
    /// Total number of rejected reservation attempts.
    pub failed_reservations: u64,
    /// Per-class diagnostics in stable order.
    pub classes: Vec<MemoryClassSnapshot>,
}

/// Linux process memory metrics derived from `/proc/self/smaps_rollup`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemorySnapshot {
    /// Resident set size in bytes.
    pub rss_bytes: usize,
    /// Anonymous resident memory in bytes.
    pub anonymous_bytes: usize,
    /// RSS not represented by active governor reservations.
    pub unmanaged_bytes: usize,
}

impl ProcessMemorySnapshot {
    /// Resident bytes that are not anonymous. Clean file-backed mmap pages
    /// are included here.
    #[must_use]
    pub const fn non_anonymous_rss_bytes(self) -> usize {
        self.rss_bytes.saturating_sub(self.anonymous_bytes)
    }

    /// Effective process pressure used for soft/hard-limit decisions.
    ///
    /// This deliberately excludes non-anonymous RSS that the kernel can reclaim,
    /// while never dropping below active governor reservations.
    #[must_use]
    pub const fn pressure_bytes(self, governed_bytes: usize) -> usize {
        if self.anonymous_bytes > governed_bytes {
            self.anonymous_bytes
        } else {
            governed_bytes
        }
    }
}

/// Automatically selected memory operating profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryProfile {
    Mib256,
    Gib1,
    Gib8,
    Gib16,
    Gib32,
    Custom,
    Unlimited,
}

impl MemoryProfile {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mib256 => "256m",
            Self::Gib1 => "1g",
            Self::Gib8 => "8g",
            Self::Gib16 => "16g",
            Self::Gib32 => "32g",
            Self::Custom => "custom",
            Self::Unlimited => "unlimited",
        }
    }

    #[must_use]
    pub const fn canonical_limit_bytes(self) -> Option<usize> {
        const MIB: usize = 1024 * 1024;
        const GIB: usize = 1024 * MIB;
        match self {
            Self::Mib256 => Some(256 * MIB),
            Self::Gib1 => Some(GIB),
            // These canonical limits cannot be represented by `usize` on a
            // 32-bit target. Returning `None` there avoids a compile-time
            // overflow while preserving the existing public API.
            #[cfg(target_pointer_width = "64")]
            Self::Gib8 => Some(8 * GIB),
            #[cfg(target_pointer_width = "64")]
            Self::Gib16 => Some(16 * GIB),
            #[cfg(target_pointer_width = "64")]
            Self::Gib32 => Some(32 * GIB),
            #[cfg(target_pointer_width = "32")]
            Self::Gib8 | Self::Gib16 | Self::Gib32 => None,
            Self::Custom | Self::Unlimited => None,
        }
    }
}

/// Calibrated budget split for one process-memory profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryProfileConfig {
    pub profile: MemoryProfile,
    pub process_limit_bytes: Option<usize>,
    pub runtime_reserve_bytes: usize,
    pub managed_budget_bytes: Option<usize>,
    pub planner_cache_bytes: usize,
    pub result_cache_bytes: usize,
    pub operation_budget_bytes: usize,
    pub query_budget_bytes: usize,
    pub import_budget_bytes: usize,
}

impl MemoryProfileConfig {
    #[must_use]
    pub fn for_limit(limit_bytes: usize) -> Self {
        const MIB: usize = 1024 * 1024;
        const MIB64: u64 = 1024 * 1024;
        const GIB64: u64 = 1024 * MIB64;

        // Profile templates describe logical byte budgets and may therefore be
        // larger than the address space of a 32-bit process. Keep them in u64
        // and only convert the scaled result back to usize. This lets armhf
        // compile while leaving the runtime accounting API in usize.
        #[derive(Clone, Copy)]
        struct Template {
            profile: MemoryProfile,
            limit: u64,
            runtime: u64,
            planner: u64,
            results: u64,
            operations: u64,
            query: u64,
            import: u64,
        }

        const TEMPLATES: [Template; 5] = [
            Template { profile: MemoryProfile::Mib256, limit: 256 * MIB64, runtime: 48 * MIB64, planner: 4 * MIB64, results: 8 * MIB64, operations: 112 * MIB64, query: 80 * MIB64, import: 32 * MIB64 },
            Template { profile: MemoryProfile::Gib1, limit: GIB64, runtime: 128 * MIB64, planner: 8 * MIB64, results: 32 * MIB64, operations: 512 * MIB64, query: 320 * MIB64, import: 192 * MIB64 },
            Template { profile: MemoryProfile::Gib8, limit: 8 * GIB64, runtime: 512 * MIB64, planner: 16 * MIB64, results: 128 * MIB64, operations: 4 * GIB64, query: 2 * GIB64, import: GIB64 },
            Template { profile: MemoryProfile::Gib16, limit: 16 * GIB64, runtime: GIB64, planner: 32 * MIB64, results: 256 * MIB64, operations: 8 * GIB64, query: 4 * GIB64, import: 2 * GIB64 },
            Template { profile: MemoryProfile::Gib32, limit: 32 * GIB64, runtime: 2 * GIB64, planner: 64 * MIB64, results: 512 * MIB64, operations: 16 * GIB64, query: 8 * GIB64, import: 4 * GIB64 },
        ];

        let limit_u64 = limit_bytes as u64;
        let template = TEMPLATES
            .into_iter()
            .min_by_key(|candidate| candidate.limit.abs_diff(limit_u64))
            .expect("memory profile templates cannot be empty");

        let scale = |value: u64| -> usize {
            let scaled = u128::from(value)
                .saturating_mul(limit_bytes as u128)
                .checked_div(u128::from(template.limit))
                .unwrap_or(0);
            u128_to_usize_saturating(scaled)
        };

        let runtime = scale(template.runtime).min(limit_bytes / 2);
        let managed = limit_bytes.saturating_sub(runtime);
        let operations = scale(template.operations).min(managed);

        Self {
            profile: template.profile,
            process_limit_bytes: Some(limit_bytes),
            runtime_reserve_bytes: runtime,
            managed_budget_bytes: Some(managed),
            planner_cache_bytes: scale(template.planner).min(managed / 8).max(MIB),
            result_cache_bytes: scale(template.results).min(managed / 16).max(MIB),
            operation_budget_bytes: operations,
            query_budget_bytes: scale(template.query).min(operations),
            import_budget_bytes: scale(template.import).min(operations),
        }
    }

    #[must_use]
    pub const fn is_scaled(self) -> bool {
        match (
            self.process_limit_bytes,
            self.profile.canonical_limit_bytes(),
        ) {
            (Some(effective), Some(base)) => effective != base,
            _ => false,
        }
    }

    #[must_use]
    pub fn effective_profile_label(self) -> String {
        if self.is_scaled() {
            format!("custom (base: {})", self.profile.as_str())
        } else {
            self.profile.as_str().to_owned()
        }
    }

    #[must_use]
    pub fn unlimited() -> Self {
        const MIB: usize = 1024 * 1024;
        // "unlimited" means there is no configured process hard limit, not that
        // blocking operators should be artificially restricted to the historical
        // 64 MiB fallback. Size the concurrency envelope from currently available
        // host memory while keeping a generous OS/page-cache reserve.
        let available = system_available_memory_bytes().unwrap_or(1024 * MIB);
        let operation_budget = available.saturating_mul(3) / 4;
        let query_budget = (available / 2).clamp(64 * MIB, operation_budget.max(64 * MIB));
        let import_budget = (available / 4).clamp(32 * MIB, query_budget);
        Self {
            profile: MemoryProfile::Unlimited,
            process_limit_bytes: None,
            runtime_reserve_bytes: 0,
            managed_budget_bytes: None,
            planner_cache_bytes: 8 * MIB,
            result_cache_bytes: 128 * MIB,
            operation_budget_bytes: operation_budget.max(query_budget),
            query_budget_bytes: query_budget,
            import_budget_bytes: import_budget,
        }
    }
}

/// Kind of concurrently admitted operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadClass {
    Streaming,
    Query,
    Import,
}
impl WorkloadClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Streaming => "streaming",
            Self::Query => "query",
            Self::Import => "import",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMemoryRecord {
    pub id: u64,
    pub class: WorkloadClass,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMemorySnapshot {
    pub base_profile: MemoryProfile,
    pub profile_scaled: bool,
    pub process_limit_bytes: Option<usize>,
    pub runtime_reserve_bytes: usize,
    pub managed_budget_bytes: Option<usize>,
    pub operation_budget_bytes: usize,
    pub active_heavy_operations: usize,
    pub rejected_operations: u64,
    pub running_operations: usize,
    pub waiting_operations: usize,
    pub query_slots: usize,
    pub records: Vec<QueryMemoryRecord>,
}

#[derive(Debug, Default)]
struct AdmissionState {
    next_id: u64,
    rejected: u64,
    /// Running heavy operations; light ones are only counted.
    records: Vec<QueryMemoryRecord>,
    light: usize,
    waiting: VecDeque<AdmissionWaiter>,
    /// Query bytes the running heavy operations needed on their past runs.
    committed: usize,
    /// Peak query bytes of each operation text, averaged over its runs
    /// (see [`QueryMemoryPermit`]).
    peaks: HashMap<u64, usize>,
}

/// Operation texts whose peak is remembered; the table restarts when full.
const PEAK_ENTRIES: usize = 4096;

/// Operation waiting for a slot: the id it is granted, 0 until then, and
/// the query bytes its past runs held.
#[derive(Debug)]
struct AdmissionWaiter { class: WorkloadClass, thread: std::thread::Thread, grant: Arc<AtomicU64>, bytes: usize }

const ADMISSION_RECHECK: Duration = Duration::from_millis(100);

/// Room the operation envelope keeps for a heavy operation to start in.
const ADMISSION_ROOM_BYTES: usize = 4 * 1024 * 1024;

thread_local! {
    /// Most query bytes held at once by the operation of this thread.
    static QUERY_PEAK: Cell<usize> = const { Cell::new(0) };
}

/// Grants free slots to the waiters in arrival order, skipping those whose
/// pool is full. Light (streaming) and heavy operations have their own
/// slots: a short operation never queues behind long ones. A heavy one also
/// needs room in the operation envelope, and the query bytes its past runs
/// held unless no heavy one runs; the first that lacks them holds back the
/// later heavy ones, which cannot starve it.
fn dispatch(inner: &MemoryGovernorInner, state: &mut AdmissionState) {
    let (slots, light_slots) = (inner.slots.load(Ordering::Relaxed), inner.light_slots.load(Ordering::Relaxed));
    let query_bytes = inner.classes[MemoryClass::Query.index()].current_bytes.load(Ordering::Acquire);
    let (mut index, mut heavy_waits) = (0, false);
    while index < state.waiting.len() {
        let (light, bytes) = (state.waiting[index].class == WorkloadClass::Streaming, state.waiting[index].bytes);
        let used = query_bytes.max(state.committed);
        if if light { state.light >= light_slots } else { heavy_waits || state.records.len() >= slots } {
            index += 1;
            continue;
        }
        if !light && (used.saturating_add(ADMISSION_ROOM_BYTES) > inner.profile.operation_budget_bytes || !state.records.is_empty() && used.saturating_add(bytes) > inner.profile.operation_budget_bytes) {
            heavy_waits = true;
            index += 1;
            continue;
        }
        let Some(waiter) = state.waiting.remove(index) else { break };
        let id = state.next_id;
        state.next_id = id.saturating_add(1);
        if light { state.light += 1 } else { state.records.push(QueryMemoryRecord { id, class: waiter.class }); state.committed = state.committed.saturating_add(bytes) }
        waiter.grant.store(id, Ordering::Release);
        waiter.thread.unpark();
    }
}

#[derive(Debug)]
struct MemoryGovernorInner {
    limit_bytes: Option<usize>,
    process_limit_bytes: Option<usize>,
    profile: MemoryProfileConfig,
    admission: Mutex<AdmissionState>,
    slots: AtomicUsize,
    light_slots: AtomicUsize,
    queue_wait_ms: AtomicU64,
    /// Stages waiting for memory to be released, see [`StageMemory::holds`].
    release_waiters: AtomicUsize,
    released: (Mutex<()>, std::sync::Condvar),
    current_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
    active_reservations: AtomicUsize,
    failed_reservations: AtomicU64,
    classes: [ClassCounters; MEMORY_CLASS_COUNT],
    next_event_sequence: AtomicU64,
    events: Mutex<MemoryEventLog>,
}

/// Shared hard memory budget with lock-free accounting.
#[derive(Debug, Clone)]
pub struct MemoryGovernor {
    inner: Arc<MemoryGovernorInner>,
}

impl MemoryGovernor {
    /// Creates a governor with no hard limit.
    #[must_use]
    pub fn unlimited() -> Self {
        Self::new_inner(None, DEFAULT_MEMORY_EVENT_CAPACITY)
    }

    /// Creates a governor with a strict byte limit.
    ///
    /// A zero-byte governor is valid and rejects every non-empty reservation.
    #[must_use]
    pub fn with_limit(limit_bytes: usize) -> Self {
        Self::new_inner(Some(limit_bytes), DEFAULT_MEMORY_EVENT_CAPACITY)
    }

    /// Creates a process governor with an automatically calibrated profile.
    #[must_use]
    pub fn with_process_limit(limit_bytes: usize) -> Self {
        Self::new_with_profile(
            MemoryProfileConfig::for_limit(limit_bytes),
            DEFAULT_MEMORY_EVENT_CAPACITY,
        )
    }

    /// Creates an unlimited governor with a custom event-journal capacity.
    #[must_use]
    pub fn unlimited_with_event_capacity(event_capacity: usize) -> Self {
        Self::new_inner(None, event_capacity)
    }

    /// Creates a limited governor with a custom event-journal capacity.
    #[must_use]
    pub fn with_limit_and_event_capacity(limit_bytes: usize, event_capacity: usize) -> Self {
        Self::new_inner(Some(limit_bytes), event_capacity)
    }

    fn new_inner(limit_bytes: Option<usize>, event_capacity: usize) -> Self {
        let profile = match limit_bytes {
            Some(limit) => MemoryProfileConfig {
                profile: MemoryProfile::Custom,
                process_limit_bytes: Some(limit),
                runtime_reserve_bytes: 0,
                managed_budget_bytes: Some(limit),
                planner_cache_bytes: 8 * 1024 * 1024,
                result_cache_bytes: 8 * 1024 * 1024,
                operation_budget_bytes: limit,
                query_budget_bytes: limit,
                import_budget_bytes: limit,
            },
            None => MemoryProfileConfig::unlimited(),
        };
        Self::new_with_profile(profile, event_capacity)
    }

    fn new_with_profile(profile: MemoryProfileConfig, event_capacity: usize) -> Self {
        Self {
            inner: Arc::new(MemoryGovernorInner {
                limit_bytes: profile.managed_budget_bytes,
                process_limit_bytes: profile.process_limit_bytes,
                profile,
                admission: Mutex::new(AdmissionState {
                    next_id: 1,
                    ..AdmissionState::default()
                }),
                slots: AtomicUsize::new(usize::MAX),
                light_slots: AtomicUsize::new(usize::MAX),
                queue_wait_ms: AtomicU64::new(0),
                release_waiters: AtomicUsize::new(0),
                released: (Mutex::new(()), std::sync::Condvar::new()),
                current_bytes: AtomicUsize::new(0),
                peak_bytes: AtomicUsize::new(0),
                active_reservations: AtomicUsize::new(0),
                failed_reservations: AtomicU64::new(0),
                classes: std::array::from_fn(|_| ClassCounters::default()),
                next_event_sequence: AtomicU64::new(1),
                events: Mutex::new(MemoryEventLog::new(event_capacity)),
            }),
        }
    }

    /// Configured hard limit, or `None` when unlimited.
    #[must_use]
    pub fn limit_bytes(&self) -> Option<usize> {
        self.inner.process_limit_bytes
    }

    #[must_use]
    pub fn profile(&self) -> MemoryProfileConfig {
        self.inner.profile
    }

    /// Bytes currently held by live reservations. Unlike [`Self::snapshot`],
    /// this never reads process metrics and is cheap enough to poll.
    #[must_use]
    pub fn reserved_bytes(&self) -> usize {
        self.inner.current_bytes.load(Ordering::Acquire)
    }

    /// Attempts to reserve bytes for one logical class.
    ///
    /// Successful reservations are released automatically when the returned
    /// [`MemoryReservation`] is dropped.
    pub fn reserve( &self, class: MemoryClass, bytes: usize, ) -> Result<MemoryReservation, MemoryReservationError> {
        if bytes == 0 {
            return Ok(MemoryReservation::empty(Arc::clone(&self.inner), class));
        }
        acquire(&self.inner, class, bytes, true)?;
        Ok(MemoryReservation {
            inner: Arc::clone(&self.inner),
            class,
            bytes,
            released: false,
        })
    }

    /// Reports best-effort resident bytes owned by a memory class but not
    /// governed by reservations. This is diagnostic-only accounting and does
    /// not affect admission or hard-limit decisions.
    pub fn set_observed_bytes(&self, class: MemoryClass, bytes: usize) {
        self.inner.classes[class.index()]
            .observed_bytes
            .store(bytes, Ordering::Release);
    }

    /// Returns current global and per-class counters.
    #[must_use]
    pub fn snapshot(&self) -> MemorySnapshot {
        let current_bytes = self.inner.current_bytes.load(Ordering::Acquire);
        let classes = MEMORY_CLASSES
            .into_iter()
            .map(|class| {
                let counters = &self.inner.classes[class.index()];
                MemoryClassSnapshot {
                    class,
                    current_bytes: counters.current_bytes.load(Ordering::Acquire),
                    peak_bytes: counters.peak_bytes.load(Ordering::Acquire),
                    observed_bytes: counters.observed_bytes.load(Ordering::Acquire),
                    active_reservations: counters.active_reservations.load(Ordering::Acquire),
                    failed_reservations: counters.failed_reservations.load(Ordering::Acquire),
                }
            })
            .collect();

        let process = process_memory_snapshot(current_bytes);
        MemorySnapshot {
            process,
            limit_bytes: self.inner.process_limit_bytes,
            current_bytes,
            peak_bytes: self.inner.peak_bytes.load(Ordering::Acquire),
            available_bytes: self
                .inner
                .limit_bytes
                .map(|limit| limit.saturating_sub(current_bytes)),
            active_reservations: self.inner.active_reservations.load(Ordering::Acquire),
            failed_reservations: self.inner.failed_reservations.load(Ordering::Acquire),
            classes,
        }
    }

    /// Bytes left under the governor limit (`None` without a limit), without
    /// reading process metrics: [`Self::snapshot`] reads `/proc`, whose cost
    /// grows with the pages mapped by the process.
    #[must_use]
    pub fn available_bytes(&self) -> Option<usize> {
        let current_bytes = self.inner.current_bytes.load(Ordering::Acquire);
        self.inner.limit_bytes.map(|limit| limit.saturating_sub(current_bytes))
    }

    /// Returns current Linux process memory metrics when `/proc` is available.
    #[must_use]
    pub fn process_memory_snapshot(&self) -> Option<ProcessMemorySnapshot> {
        process_memory_snapshot(self.inner.current_bytes.load(Ordering::Acquire))
    }

    /// Returns the current process-pressure state derived from the configured
    /// process limit. The soft threshold is 90% of the hard limit.
    ///
    /// Pressure is based on the larger of anonymous resident memory and active
    /// governor reservations. Non-anonymous RSS (notably clean read-only mmap
    /// pages) remains observable but does not by itself trigger memory pressure.
    #[must_use]
    pub fn process_pressure(&self) -> ProcessMemoryPressure {
        let Some(limit_bytes) = self.inner.process_limit_bytes else {
            return ProcessMemoryPressure::Unlimited;
        };
        let governed_bytes = self.inner.current_bytes.load(Ordering::Acquire);
        let classify = || process_memory_snapshot(governed_bytes).map_or(ProcessMemoryPressure::Unavailable { limit_bytes }, |process| classify_process_pressure(limit_bytes, process, governed_bytes));
        match classify() {
            ProcessMemoryPressure::Soft { .. } | ProcessMemoryPressure::Hard { .. } if release_free_memory() => classify(),
            pressure => pressure,
        }
    }

    /// Rejects an allocation-producing operation when effective process memory
    /// pressure is already above the hard limit. This check is deliberately
    /// separate from
    /// [`Self::reserve`]: zero-byte reservations and pure accounting operations
    /// must never fail because of process pressure.
    pub fn ensure_process_capacity( &self, class: MemoryClass, requested_bytes: usize, ) -> Result<(), ProcessMemoryPressureError> {
        match self.process_pressure() {
            ProcessMemoryPressure::Hard {
                rss_bytes,
                soft_limit_bytes,
                hard_limit_bytes,
            } => Err(ProcessMemoryPressureError {
                class,
                requested_bytes,
                rss_bytes,
                soft_limit_bytes,
                hard_limit_bytes,
            }),
            _ => Ok(()),
        }
    }

    /// Admits one operation, waiting at most the queue wait (none by
    /// default) for a slot of its pool, see [`dispatch`]; the permit that
    /// frees one hands it over, so a release wakes only the admitted waiters.
    /// The operation's `text` keys the peak query bytes learned from its runs,
    /// none when empty.
    pub fn admit(&self, class: WorkloadClass, text: &str) -> Result<QueryMemoryPermit, QueryAdmissionError> {
        let deadline = Instant::now() + Duration::from_millis(self.inner.queue_wait_ms.load(Ordering::Relaxed));
        let (grant, key) = (Arc::new(AtomicU64::new(0)), (!text.is_empty()).then(|| std::hash::BuildHasherDefault::<std::hash::DefaultHasher>::default().hash_one(text)));
        let mut state = lock_unpoisoned(&self.inner.admission);
        let bytes = key.and_then(|key| state.peaks.get(&key).copied()).unwrap_or_default();
        state.waiting.push_back(AdmissionWaiter { class, thread: std::thread::current(), grant: Arc::clone(&grant), bytes });
        dispatch(&self.inner, &mut state);
        loop {
            let id = grant.load(Ordering::Acquire);
            if id != 0 {
                QUERY_PEAK.set(0);
                return Ok(QueryMemoryPermit { inner: Arc::clone(&self.inner), id, class, key, bytes });
            }
            let now = Instant::now();
            if now >= deadline {
                state.waiting.retain(|waiter| !Arc::ptr_eq(&waiter.grant, &grant));
                state.rejected = state.rejected.saturating_add(1);
                return Err(QueryAdmissionError { class, running_operations: state.light + state.records.len(), query_slots: self.inner.slots.load(Ordering::Relaxed), active_heavy_operations: state.records.len() });
            }
            drop(state);
            // Memory released by query operators does not wake the queue: the
            // first waiter re-evaluates capacity at this interval.
            std::thread::park_timeout((deadline - now).min(ADMISSION_RECHECK));
            state = lock_unpoisoned(&self.inner.admission);
            if grant.load(Ordering::Acquire) == 0 && state.waiting.front().is_some_and(|waiter| Arc::ptr_eq(&waiter.grant, &grant)) {
                dispatch(&self.inner, &mut state);
            }
        }
    }

    /// Bounds the heavy and the light operations running at once, and the
    /// allocator arenas to them; the others queue for at most `wait`.
    pub fn set_query_slots(&self, slots: usize, light_slots: usize, wait: Duration) {
        self.inner.slots.store(slots.max(1), Ordering::Relaxed);
        self.inner.light_slots.store(light_slots.max(1), Ordering::Relaxed);
        if let Some(allocator) = ALLOCATOR.get() { (allocator.limit_arenas)(slots.max(1).saturating_add(light_slots.max(1))); }
        self.inner.queue_wait_ms.store(u64::try_from(wait.as_millis()).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    /// [`Self::reserve`] that waits for other operations to release memory
    /// while the governor lacks it, unless all of them wait as well.
    pub fn reserve_waiting(&self, class: MemoryClass, bytes: usize) -> Result<MemoryReservation, MemoryReservationError> {
        loop {
            match self.reserve(class, bytes) {
                Err(_) if self.wait_for_release() => {}
                result => return result,
            }
        }
    }

    /// Waits a little for memory to be released, unless every other running
    /// operation waits too, since none of them would release any: whether it
    /// waited.
    fn wait_for_release(&self) -> bool {
        let running = self.running_operations();
        let waiters = &self.inner.release_waiters;
        if waiters.fetch_add(1, Ordering::AcqRel) + 1 >= running {
            waiters.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        let released = lock_unpoisoned(&self.inner.released.0);
        drop(self.inner.released.1.wait_timeout(released, ADMISSION_RECHECK));
        waiters.fetch_sub(1, Ordering::AcqRel);
        true
    }

    /// Operations holding a query slot.
    #[must_use]
    pub fn running_operations(&self) -> usize {
        let state = lock_unpoisoned(&self.inner.admission);
        state.light + state.records.len()
    }

    #[must_use]
    pub fn query_memory_snapshot(&self) -> QueryMemorySnapshot {
        let state = lock_unpoisoned(&self.inner.admission);
        QueryMemorySnapshot {
            base_profile: self.inner.profile.profile,
            profile_scaled: self.inner.profile.is_scaled(),
            process_limit_bytes: self.inner.profile.process_limit_bytes,
            runtime_reserve_bytes: self.inner.profile.runtime_reserve_bytes,
            managed_budget_bytes: self.inner.profile.managed_budget_bytes,
            operation_budget_bytes: self.inner.profile.operation_budget_bytes,
            active_heavy_operations: state.records.len(),
            rejected_operations: state.rejected,
            running_operations: state.light + state.records.len(),
            waiting_operations: state.waiting.len(),
            query_slots: self.inner.slots.load(Ordering::Relaxed),
            records: state.records.clone(),
        }
    }

    /// Returns an oldest-to-newest snapshot of the bounded event journal.
    #[must_use]
    pub fn event_snapshot(&self) -> MemoryEventSnapshot {
        let events = lock_unpoisoned(&self.inner.events);
        MemoryEventSnapshot {
            events: events.events.iter().copied().collect(),
            dropped_events: events.dropped,
            capacity: events.capacity,
        }
    }
}

impl Default for MemoryGovernor {
    fn default() -> Self {
        Self::unlimited()
    }
}

/// Counts `bytes` more in `class`, or rejects them when they would exceed the
/// governor limit or, for query operators, the profile's operation envelope.
fn acquire( inner: &Arc<MemoryGovernorInner>, class: MemoryClass, bytes: usize, new_reservation: bool, ) -> Result<(), MemoryReservationError> {
    let counters = &inner.classes[class.index()];
    // Query operators reserve as their state grows: together they stay within
    // the envelope that admission used to split between them upfront.
    let class_cap = (class == MemoryClass::Query).then_some(inner.profile.operation_budget_bytes);
    let class_current = add_within(&counters.current_bytes, bytes, class_cap)
        .map_err(|_| reject(inner, class, bytes, inner.current_bytes.load(Ordering::Acquire)))?;
    let next = match add_within(&inner.current_bytes, bytes, inner.limit_bytes) {
        Ok(next) => next,
        Err(current) => {
            counters.current_bytes.fetch_sub(bytes, Ordering::AcqRel);
            return Err(reject(inner, class, bytes, current));
        }
    };
    crate::helpers::raise_peak_usize(&inner.peak_bytes, next);
    crate::helpers::raise_peak_usize(&counters.peak_bytes, class_current);
    if new_reservation {
        inner.active_reservations.fetch_add(1, Ordering::Relaxed);
        counters.active_reservations.fetch_add(1, Ordering::Relaxed);
    }
    record_event(inner, MemoryEventKind::Reserved, class, bytes, next);
    Ok(())
}

/// Adds `bytes` to `counter` unless the sum would exceed `limit`: the new
/// value, or the current one when refused.
fn add_within(counter: &AtomicUsize, bytes: usize, limit: Option<usize>) -> Result<usize, usize> {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let next = current.checked_add(bytes).filter(|next| limit.is_none_or(|limit| *next <= limit)).ok_or(current)?;
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(next),
            Err(observed) => current = observed,
        }
    }
}

fn reject( inner: &MemoryGovernorInner, class: MemoryClass, requested_bytes: usize, current_bytes: usize, ) -> MemoryReservationError {
    inner.failed_reservations.fetch_add(1, Ordering::Relaxed);
    inner.classes[class.index()].failed_reservations.fetch_add(1, Ordering::Relaxed);
    record_event(inner, MemoryEventKind::Rejected, class, requested_bytes, current_bytes);
    MemoryReservationError {
        class,
        requested_bytes,
        current_bytes,
        limit_bytes: inner.process_limit_bytes,
    }
}

fn record_event( inner: &MemoryGovernorInner, kind: MemoryEventKind, class: MemoryClass, bytes: usize, current_bytes: usize, ) {
    if kind != MemoryEventKind::Rejected && bytes < MEMORY_EVENT_MIN_BYTES {
        return;
    }
    let sequence = inner.next_event_sequence.fetch_add(1, Ordering::Relaxed);
    let event = MemoryEvent {
        sequence,
        class,
        kind,
        bytes,
        current_bytes,
        limit_bytes: inner.limit_bytes,
    };
    lock_unpoisoned(&inner.events)
        .push(event);
}

/// Current process-memory pressure relative to the configured limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessMemoryPressure {
    /// No process limit is configured.
    Unlimited,
    /// Linux process metrics are unavailable.
    Unavailable { limit_bytes: usize },
    /// Effective process pressure is below the soft threshold.
    Normal {
        rss_bytes: usize,
        soft_limit_bytes: usize,
        hard_limit_bytes: usize,
    },
    /// Effective process pressure is above the soft threshold but not the hard limit.
    Soft {
        rss_bytes: usize,
        soft_limit_bytes: usize,
        hard_limit_bytes: usize,
    },
    /// Effective process pressure is above the configured hard limit.
    Hard {
        rss_bytes: usize,
        soft_limit_bytes: usize,
        hard_limit_bytes: usize,
    },
}

#[derive(Debug)]
pub struct QueryMemoryPermit {
    inner: Arc<MemoryGovernorInner>,
    id: u64,
    class: WorkloadClass,
    key: Option<u64>,
    /// Query bytes committed for the operation at admission.
    bytes: usize,
}
impl QueryMemoryPermit {
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }
    #[must_use]
    pub const fn class(&self) -> WorkloadClass {
        self.class
    }
}
impl Drop for QueryMemoryPermit {
    fn drop(&mut self) {
        let peak = QUERY_PEAK.take();
        let mut state = lock_unpoisoned(&self.inner.admission);
        match state.records.iter().position(|r| r.id == self.id) {
            Some(index) => { state.records.swap_remove(index); state.committed = state.committed.saturating_sub(self.bytes); }
            None => state.light = state.light.saturating_sub(1),
        }
        if let Some(key) = self.key {
            if state.peaks.len() >= PEAK_ENTRIES && !state.peaks.contains_key(&key) { state.peaks.clear(); }
            let average = state.peaks.entry(key).or_insert(peak);
            *average = *average / 4 * 3 + peak / 4;
        }
        dispatch(&self.inner, &mut state);
    }
}

/// RAII token representing one successful memory reservation.
#[derive(Debug)]
pub struct MemoryReservation {
    inner: Arc<MemoryGovernorInner>,
    class: MemoryClass,
    bytes: usize,
    released: bool,
}

impl MemoryReservation {
    const fn empty(inner: Arc<MemoryGovernorInner>, class: MemoryClass) -> Self {
        Self {
            inner,
            class,
            bytes: 0,
            released: true,
        }
    }

    /// Reserved class.
    #[must_use]
    pub const fn class(&self) -> MemoryClass {
        self.class
    }

    /// Number of bytes represented by this reservation.
    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Reserves `bytes` more for the same class.
    ///
    /// # Errors
    ///
    /// Returns the rejection when the governor cannot grant them; the
    /// reservation then keeps its current size.
    pub fn try_grow(&mut self, bytes: usize) -> Result<(), MemoryReservationError> {
        if bytes == 0 {
            return Ok(());
        }
        acquire(&self.inner, self.class, bytes, self.released)?;
        self.bytes += bytes;
        self.released = false;
        Ok(())
    }

    /// Releases the reservation before the end of its lexical scope.
    pub fn release(mut self) {
        self.release_inner();
    }

    /// Returns part of this reservation to the governor while keeping the
    /// remainder live. This is primarily used by revocable caches.
    pub fn shrink_by(&mut self, bytes: usize) -> usize {
        if self.released || self.bytes == 0 || bytes == 0 {
            return 0;
        }
        let released = bytes.min(self.bytes);
        let previous = self
            .inner
            .current_bytes
            .fetch_sub(released, Ordering::AcqRel);
        let current = previous.saturating_sub(released);
        let counters = &self.inner.classes[self.class.index()];
        counters
            .current_bytes
            .fetch_sub(released, Ordering::AcqRel);
        self.bytes = self.bytes.saturating_sub(released);
        if self.inner.release_waiters.load(Ordering::Acquire) > 0 {
            self.inner.released.1.notify_all();
        }
        record_event(
            &self.inner,
            MemoryEventKind::Released,
            self.class,
            released,
            current,
        );
        if self.bytes == 0 {
            self.inner
                .active_reservations
                .fetch_sub(1, Ordering::Relaxed);
            counters.active_reservations.fetch_sub(1, Ordering::Relaxed);
            self.released = true;
        }
        released
    }

    fn release_inner(&mut self) {
        let remaining = self.bytes;
        let _ = self.shrink_by(remaining);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.release_inner();
    }
}

/// Memory of the blocking stages of one query.
///
/// Stages reserve what their state holds, as it grows, instead of the whole
/// query budget upfront: concurrent queries then share the governed memory
/// by what they use. Together, the stages of a query never hold more than
/// the profile's query budget.
#[derive(Debug)]
pub struct QueryMemory {
    governor: MemoryGovernor,
    limit: usize,
    held: Cell<usize>,
}

impl QueryMemory {
    #[must_use]
    pub fn new(governor: &MemoryGovernor) -> Self {
        Self::with_limit(governor, governor.profile().query_budget_bytes)
    }

    #[must_use]
    pub fn with_limit(governor: &MemoryGovernor, limit: usize) -> Self {
        Self { governor: governor.clone(), limit, held: Cell::new(0) }
    }

    /// Memory of one more stage, holding nothing yet.
    #[must_use]
    pub fn stage(&self) -> StageMemory<'_> {
        StageMemory { query: self, reservation: RefCell::new(MemoryReservation::empty(Arc::clone(&self.governor.inner), MemoryClass::Query)) }
    }

    /// Bytes reserved by the stages alive.
    #[must_use]
    pub fn held_bytes(&self) -> usize {
        self.held.get()
    }
}

/// Memory of one blocking stage of a query, see [`QueryMemory`].
#[derive(Debug)]
pub struct StageMemory<'q> {
    query: &'q QueryMemory,
    reservation: RefCell<MemoryReservation>,
}

impl StageMemory<'_> {
    /// Smallest growth step: a stage reserves at least this much more at once.
    const STEP_BYTES: usize = 256 * 1024;

    /// Whether the stage may hold `bytes` in all, reserving them when it holds
    /// less. Growth doubles the reservation (at least [`Self::STEP_BYTES`]),
    /// or takes only what is needed when the governor cannot grant more.
    pub fn fits(&self, bytes: usize) -> bool {
        let held = self.reservation.borrow().bytes();
        if bytes <= held {
            return true;
        }
        let others = self.query.held.get() - held;
        let room = self.query.limit.saturating_sub(others);
        if bytes > room {
            return false;
        }
        let wanted = bytes.max(held.saturating_mul(2)).max(Self::STEP_BYTES).min(room);
        let mut reservation = self.reservation.borrow_mut();
        let grown = reservation.try_grow(wanted - held).is_ok() || reservation.try_grow(bytes - held).is_ok();
        self.query.held.set(others + reservation.bytes());
        QUERY_PEAK.set(QUERY_PEAK.get().max(self.query.held.get()));
        grown
    }

    /// [`Self::fits`] for a stage that cannot spill: when the governor lacks
    /// the bytes, waits for other operations to release theirs instead of
    /// failing, unless all of them wait as well.
    pub fn holds(&self, bytes: usize) -> bool {
        while !self.fits(bytes) {
            if bytes > self.limit() || !self.query.governor.wait_for_release() {
                return false;
            }
        }
        true
    }

    /// Most the stage could hold, if the governor grants it.
    #[must_use]
    pub fn limit(&self) -> usize {
        let held = self.reservation.borrow().bytes();
        self.query.limit.saturating_sub(self.query.held.get() - held)
    }

    /// Bytes reserved by the stage.
    #[must_use]
    pub fn held_bytes(&self) -> usize {
        self.reservation.borrow().bytes()
    }

    /// Returns what the stage holds, once its state is gone (spilled).
    pub fn release(&self) {
        let mut reservation = self.reservation.borrow_mut();
        let held = reservation.bytes();
        reservation.shrink_by(held);
        self.query.held.set(self.query.held.get() - held);
    }
}

impl Drop for StageMemory<'_> {
    fn drop(&mut self) {
        let held = self.reservation.get_mut().bytes();
        self.query.held.set(self.query.held.get() - held);
    }
}

fn system_available_memory_bytes() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        let contents = std::fs::read_to_string("/proc/meminfo").ok()?;
        for line in contents.lines() {
            let mut parts = line.split_whitespace();
            if parts.next()? == "MemAvailable:" {
                return parts
                    .next()?
                    .parse::<usize>()
                    .ok()
                    .map(|kib| kib.saturating_mul(1024));
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

fn process_memory_snapshot(governed_bytes: usize) -> Option<ProcessMemorySnapshot> {
    #[cfg(target_os = "linux")]
    {
        let contents = std::fs::read_to_string("/proc/self/smaps_rollup").ok()?;
        let mut rss_kib = None;
        let mut anonymous_kib = None;
        for line in contents.lines() {
            let mut parts = line.split_whitespace();
            let Some(label) = parts.next() else {
                continue;
            };
            match label {
                "Rss:" => rss_kib = parts.next()?.parse::<usize>().ok(),
                "Anonymous:" => anonymous_kib = parts.next()?.parse::<usize>().ok(),
                _ => {}
            }
        }
        let rss_kib = rss_kib?;
        let rss_bytes = rss_kib.saturating_mul(1024);
        let anonymous_bytes = anonymous_kib.unwrap_or(rss_kib).saturating_mul(1024);
        Some(process_memory_snapshot_from_parts(
            rss_bytes,
            anonymous_bytes,
            governed_bytes,
        ))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = governed_bytes;
        None
    }
}

static ALLOCATOR: OnceLock<Allocator> = OnceLock::new();
static FREE_MEMORY_RELEASED_AT: Mutex<Option<Instant>> = Mutex::new(None);

/// How the binary steers its allocator (glibc keeps what each thread freed
/// in its arena: without this, the resident memory stays at its peak).
#[derive(Debug, Clone, Copy)]
pub struct Allocator {
    /// Returns the free memory to the system.
    pub release: fn(),
    /// Bounds the arenas, to the threads that run operations at once.
    pub limit_arenas: fn(usize),
}

/// Installs the allocator hooks; the query slots then bound the arenas.
pub fn set_allocator(allocator: Allocator) {
    let _ = ALLOCATOR.set(allocator);
}

/// Releases the allocator's free memory under process pressure, at most once
/// a second; whether it did.
fn release_free_memory() -> bool {
    let Some(allocator) = ALLOCATOR.get() else { return false };
    let mut released_at = lock_unpoisoned(&FREE_MEMORY_RELEASED_AT);
    if released_at.is_some_and(|at| at.elapsed() < Duration::from_secs(1)) {
        return false;
    }
    (allocator.release)();
    *released_at = Some(Instant::now());
    true
}

#[cfg(any(target_os = "linux", test))]
const fn process_memory_snapshot_from_parts( rss_bytes: usize, anonymous_bytes: usize, governed_bytes: usize, ) -> ProcessMemorySnapshot {
    ProcessMemorySnapshot {
        rss_bytes,
        anonymous_bytes,
        unmanaged_bytes: rss_bytes.saturating_sub(governed_bytes),
    }
}

const fn classify_process_pressure( limit_bytes: usize, process: ProcessMemorySnapshot, governed_bytes: usize, ) -> ProcessMemoryPressure {
    let soft_limit_bytes = limit_bytes.saturating_mul(9) / 10;
    let pressure_bytes = process.pressure_bytes(governed_bytes);
    if pressure_bytes > limit_bytes {
        ProcessMemoryPressure::Hard {
            rss_bytes: process.rss_bytes,
            soft_limit_bytes,
            hard_limit_bytes: limit_bytes,
        }
    } else if pressure_bytes > soft_limit_bytes {
        ProcessMemoryPressure::Soft {
            rss_bytes: process.rss_bytes,
            soft_limit_bytes,
            hard_limit_bytes: limit_bytes,
        }
    } else {
        ProcessMemoryPressure::Normal {
            rss_bytes: process.rss_bytes,
            soft_limit_bytes,
            hard_limit_bytes: limit_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    
    #[test] fn stage_memory_grows_on_demand_within_the_query_limit() { let mib = 1024 * 1024; let governor = MemoryGovernor::with_limit(64 * mib); let query = QueryMemory::with_limit(&governor, 10 * mib); let first = query.stage(); assert_eq!(governor.reserved_bytes(), 0); assert!(first.fits(1)); assert_eq!(first.held_bytes(), mib / 4); assert!(first.fits(9 * mib)); assert_eq!(first.held_bytes(), 9 * mib); assert!(!first.fits(11 * mib)); let second = query.stage(); assert!(!second.fits(2 * mib)); assert_eq!(query.held_bytes(), 9 * mib); drop(first); assert!(second.fits(2 * mib)); assert_eq!(governor.reserved_bytes(), 2 * mib); drop(second); assert_eq!((governor.reserved_bytes(), query.held_bytes()), (0, 0)); }

    #[test] fn stage_memory_takes_what_the_governor_has_left() { let mib = 1024 * 1024; let governor = MemoryGovernor::with_limit(6 * mib); let query = QueryMemory::with_limit(&governor, 100 * mib); let stage = query.stage(); assert!(stage.fits(5 * mib)); assert!(stage.fits(5 * mib + 1)); assert_eq!(stage.held_bytes(), 5 * mib + 1); assert!(!stage.fits(7 * mib)); assert_eq!(governor.reserved_bytes(), 5 * mib + 1); }

    #[test] fn query_reservations_stay_within_the_operation_envelope() { let mib = 1024 * 1024; let governor = MemoryGovernor::with_process_limit(256 * mib); let envelope = governor.profile().operation_budget_bytes; let _held = governor.reserve(MemoryClass::Query, envelope - mib).unwrap(); assert!(governor.reserve(MemoryClass::Query, 2 * mib).is_err()); assert!(governor.reserve(MemoryClass::Planner, 2 * mib).is_ok()); let mut grown = governor.reserve(MemoryClass::Query, mib / 2).unwrap(); assert!(grown.try_grow(mib).is_err()); assert_eq!(grown.bytes(), mib / 2); assert!(governor.admit(WorkloadClass::Query, "").is_err()); }

    #[test] fn reservation_is_released_on_drop() { let governor = MemoryGovernor::with_limit(128); { let reservation = governor.reserve(MemoryClass::Query, 64).unwrap(); assert_eq!(reservation.bytes(), 64); assert_eq!(governor.snapshot().current_bytes, 64); } let snapshot = governor.snapshot(); assert_eq!(snapshot.current_bytes, 0); assert_eq!(snapshot.active_reservations, 0); assert_eq!(snapshot.peak_bytes, 64); }
    #[test] fn hard_limit_rejects_without_changing_current_usage() { let governor = MemoryGovernor::with_limit(100); let _held = governor.reserve(MemoryClass::PageCache, 80).unwrap(); let error = governor .reserve(MemoryClass::Query, 21) .expect_err("reservation must exceed the limit"); assert_eq!(error.class, MemoryClass::Query); assert_eq!(error.requested_bytes, 21); let snapshot = governor.snapshot(); assert_eq!(snapshot.current_bytes, 80); assert_eq!(snapshot.failed_reservations, 1); }
    #[test] fn classes_are_accounted_independently() { let governor = MemoryGovernor::with_limit(1_000); let _cache = governor.reserve(MemoryClass::PageCache, 300).unwrap(); let _query = governor.reserve(MemoryClass::Query, 200).unwrap(); let snapshot = governor.snapshot(); assert_eq!(snapshot.current_bytes, 500); assert_eq!( snapshot.classes[MemoryClass::PageCache.index()].current_bytes, 300 ); assert_eq!( snapshot.classes[MemoryClass::Query.index()].current_bytes, 200 ); }
    #[test] fn observed_bytes_are_diagnostic_only() { let governor = MemoryGovernor::with_limit(128); governor.set_observed_bytes(MemoryClass::Indexing, 1_000_000); let snapshot = governor.snapshot(); assert_eq!(snapshot.current_bytes, 0); assert_eq!( snapshot.classes[MemoryClass::Indexing.index()].observed_bytes, 1_000_000 ); assert!(governor.reserve(MemoryClass::Query, 64).is_ok()); }
    #[test] fn zero_byte_reservation_has_no_accounting_effect() { let governor = MemoryGovernor::with_limit(0); let reservation = governor.reserve(MemoryClass::Network, 0).unwrap(); assert_eq!(reservation.bytes(), 0); assert_eq!(governor.snapshot().current_bytes, 0); }
    #[test] fn zero_byte_reservation_is_independent_from_process_pressure() { let governor = MemoryGovernor::with_limit(0); assert!(governor.reserve(MemoryClass::Network, 0).is_ok()); }
    #[test] fn process_pressure_error_uses_rss_wording_not_reserved_wording() { let error = ProcessMemoryPressureError { class: MemoryClass::Network, requested_bytes: 1024, rss_bytes: 300, soft_limit_bytes: 225, hard_limit_bytes: 250, }; let rendered = error.to_string(); assert!(rendered.contains("RSS is 300 bytes")); assert!(!rendered.contains("already reserved")); }
    #[test] fn file_backed_rss_does_not_trigger_process_pressure() { let process = process_memory_snapshot_from_parts(7000, 768, 512); assert_eq!(process.non_anonymous_rss_bytes(), 6232); assert_eq!(process.pressure_bytes(512), 768); assert!(matches!( classify_process_pressure(1024, process, 512), ProcessMemoryPressure::Normal { .. } )); }
    #[test] fn anonymous_rss_still_triggers_hard_process_pressure() { let process = process_memory_snapshot_from_parts(1200, 1100, 512); assert!(matches!( classify_process_pressure(1024, process, 512), ProcessMemoryPressure::Hard { .. } )); }
    #[test] fn governed_reservations_are_a_floor_for_process_pressure() { let process = process_memory_snapshot_from_parts(900, 400, 950); assert_eq!(process.pressure_bytes(950), 950); assert!(matches!( classify_process_pressure(1024, process, 950), ProcessMemoryPressure::Soft { .. } )); }
    #[test] fn cloned_governors_share_the_same_budget() { let governor = MemoryGovernor::with_limit(100); let clone = governor.clone(); let _held = governor.reserve(MemoryClass::Import, 70).unwrap(); assert!(clone.reserve(MemoryClass::Query, 31).is_err()); assert_eq!(clone.snapshot().current_bytes, 70); }
    #[test] fn event_log_records_reserve_release_and_reject() { let governor = MemoryGovernor::with_limit_and_event_capacity(5 * 1024 * 1024, 8); let reservation = governor .reserve(MemoryClass::Query, MEMORY_EVENT_MIN_BYTES) .unwrap(); assert!(governor .reserve(MemoryClass::Import, 4 * 1024 * 1024) .is_err()); drop(reservation); let snapshot = governor.event_snapshot(); assert_eq!(snapshot.events.len(), 3); assert_eq!(snapshot.events[0].kind, MemoryEventKind::Reserved); assert_eq!(snapshot.events[1].kind, MemoryEventKind::Rejected); assert_eq!(snapshot.events[2].kind, MemoryEventKind::Released); assert_eq!(snapshot.events[2].current_bytes, 0); }
    #[test] fn event_log_is_bounded_and_counts_dropped_events() { let governor = MemoryGovernor::unlimited_with_event_capacity(2); for _ in 0..2 { let reservation = governor .reserve(MemoryClass::Planner, MEMORY_EVENT_MIN_BYTES) .unwrap(); drop(reservation); } let snapshot = governor.event_snapshot(); assert_eq!(snapshot.capacity, 2); assert_eq!(snapshot.events.len(), 2); assert_eq!(snapshot.dropped_events, 2); assert!(snapshot.events[0].sequence < snapshot.events[1].sequence); }
    #[test] fn automatic_profiles_calibrate_known_limits() { let mib = 1024 * 1024; let profile = MemoryProfileConfig::for_limit(256 * mib); assert_eq!(profile.profile, MemoryProfile::Mib256); assert_eq!(profile.runtime_reserve_bytes, 48 * mib); assert!(profile.managed_budget_bytes.unwrap() < 256 * mib); assert_eq!( MemoryProfileConfig::for_limit(1024 * mib).profile, MemoryProfile::Gib1 ); #[cfg(target_pointer_width = "64")] { assert_eq!( MemoryProfileConfig::for_limit(8 * 1024 * mib).profile, MemoryProfile::Gib8 ); assert_eq!( MemoryProfileConfig::for_limit(16 * 1024 * mib).profile, MemoryProfile::Gib16 ); assert_eq!( MemoryProfileConfig::for_limit(32 * 1024 * mib).profile, MemoryProfile::Gib32 ); } }
    #[cfg(target_pointer_width = "32")] #[test] fn large_canonical_profiles_are_unrepresentable_on_32_bit() { assert_eq!(MemoryProfile::Gib8.canonical_limit_bytes(), None); assert_eq!(MemoryProfile::Gib16.canonical_limit_bytes(), None); assert_eq!(MemoryProfile::Gib32.canonical_limit_bytes(), None); }
    #[test] fn arbitrary_limits_use_the_nearest_profile_and_keep_the_exact_limit() { let mib = 1024 * 1024; let gib = 1024 * mib; let profile_512m = MemoryProfileConfig::for_limit(512 * mib); assert_eq!(profile_512m.profile, MemoryProfile::Mib256); assert_eq!(profile_512m.process_limit_bytes, Some(512 * mib)); assert_eq!(profile_512m.runtime_reserve_bytes, 96 * mib); let profile_850m = MemoryProfileConfig::for_limit(850 * mib); assert_eq!(profile_850m.profile, MemoryProfile::Gib1); assert_eq!(profile_850m.process_limit_bytes, Some(850 * mib)); let profile_1500m = MemoryProfileConfig::for_limit(1536 * mib); assert_eq!(profile_1500m.profile, MemoryProfile::Gib1); assert_eq!(profile_1500m.process_limit_bytes, Some(1536 * mib)); let profile_2g = MemoryProfileConfig::for_limit(2 * gib); assert_eq!(profile_2g.profile, MemoryProfile::Gib1); assert_eq!(profile_2g.process_limit_bytes, Some(2 * gib)); }
    #[test] fn a_heavy_operation_waits_for_the_bytes_its_past_runs_held() { let mib = 1024 * 1024; let governor = MemoryGovernor::with_process_limit(256 * mib); let query = QueryMemory::new(&governor); let run = |text| { let _permit = governor.admit(WorkloadClass::Query, text).unwrap(); let stage = query.stage(); assert!(stage.fits(60 * mib)); }; run("big"); let _held = governor.admit(WorkloadClass::Query, "other").unwrap(); let stage = query.stage(); assert!(stage.fits(60 * mib)); assert!(governor.admit(WorkloadClass::Query, "big").is_err()); assert!(governor.admit(WorkloadClass::Import, "").is_ok()); assert_eq!(governor.query_memory_snapshot().rejected_operations, 1); }
    #[test] fn a_reservation_may_wait_for_others_to_release() { let mib = 1024 * 1024; let governor = MemoryGovernor::with_limit(10 * mib); let _first = governor.admit(WorkloadClass::Streaming, "").unwrap(); let held = governor.reserve(MemoryClass::Network, 8 * mib).unwrap(); assert!(governor.reserve_waiting(MemoryClass::Network, 4 * mib).is_err()); let waiter = { let governor = governor.clone(); std::thread::spawn(move || { let _second = governor.admit(WorkloadClass::Streaming, "").unwrap(); governor.reserve_waiting(MemoryClass::Network, 4 * mib).map(|reservation| reservation.bytes()) }) }; std::thread::sleep(Duration::from_millis(50)); drop(held); assert_eq!(waiter.join().unwrap().unwrap(), 4 * mib); }
    #[test] fn a_stage_that_cannot_spill_waits_for_memory_others_release() { let mib = 1024 * 1024; let governor = MemoryGovernor::with_process_limit(256 * mib); let _first = governor.admit(WorkloadClass::Streaming, "").unwrap(); let query = QueryMemory::new(&governor); let stage = query.stage(); assert!(stage.fits(80 * mib)); let waiter = { let governor = governor.clone(); std::thread::spawn(move || { let _second = governor.admit(WorkloadClass::Streaming, "").unwrap(); let query = QueryMemory::new(&governor); let held = query.stage().holds(50 * mib); held }) }; std::thread::sleep(Duration::from_millis(50)); assert!(!query.stage().holds(mib)); drop(stage); assert!(waiter.join().unwrap()); }
    #[test] fn a_queued_operation_runs_when_a_slot_frees() { let governor = MemoryGovernor::unlimited(); governor.set_query_slots(1, 1, Duration::from_secs(5)); let held = governor.admit(WorkloadClass::Query, "").unwrap(); let waiter = { let governor = governor.clone(); std::thread::spawn(move || governor.admit(WorkloadClass::Query, "").is_ok()) }; std::thread::sleep(Duration::from_millis(20)); assert_eq!(governor.query_memory_snapshot().waiting_operations, 1); drop(held); assert!(waiter.join().unwrap()); assert_eq!(governor.query_memory_snapshot().running_operations, 0); }
    #[test] fn a_queued_operation_is_rejected_after_the_wait_and_leaves_the_queue() { let governor = MemoryGovernor::unlimited(); governor.set_query_slots(1, 1, Duration::from_millis(30)); let _held = governor.admit(WorkloadClass::Streaming, "").unwrap(); assert!(governor.admit(WorkloadClass::Streaming, "").is_err()); let snapshot = governor.query_memory_snapshot(); assert_eq!((snapshot.waiting_operations, snapshot.rejected_operations, snapshot.running_operations), (0, 1, 1)); }
    #[test] fn a_waiting_heavy_operation_does_not_hold_back_streaming_ones() { let governor = MemoryGovernor::with_process_limit(256 * 1024 * 1024); governor.set_query_slots(1, 4, Duration::from_secs(5)); let query = governor.admit(WorkloadClass::Query, "").unwrap(); let heavy = { let governor = governor.clone(); std::thread::spawn(move || governor.admit(WorkloadClass::Import, "").is_ok()) }; std::thread::sleep(Duration::from_millis(20)); assert!(governor.admit(WorkloadClass::Streaming, "").is_ok()); assert_eq!(governor.query_memory_snapshot().waiting_operations, 1); drop(query); assert!(heavy.join().unwrap()); }
    #[test] fn light_operations_have_their_own_slots() { let governor = MemoryGovernor::unlimited(); governor.set_query_slots(1, 1, Duration::from_millis(30)); let _heavy = governor.admit(WorkloadClass::Query, "").unwrap(); let light = governor.admit(WorkloadClass::Streaming, "").unwrap(); assert!(governor.admit(WorkloadClass::Query, "").is_err() && governor.admit(WorkloadClass::Streaming, "").is_err()); drop(light); assert!(governor.admit(WorkloadClass::Streaming, "").is_ok()); assert_eq!(governor.query_memory_snapshot().running_operations, 1); }
    #[test] fn admission_is_released_on_drop() { let governor = MemoryGovernor::with_process_limit(1024 * 1024 * 1024); { let _permit = governor.admit(WorkloadClass::Query, "").unwrap(); assert_eq!((governor.query_memory_snapshot().active_heavy_operations, governor.running_operations()), (1, 1)); } assert_eq!((governor.query_memory_snapshot().active_heavy_operations, governor.running_operations()), (0, 0)); }
    #[test] fn effective_profile_label_distinguishes_scaled_profiles() { const MIB: usize = 1024 * 1024; let scaled = MemoryProfileConfig::for_limit(2 * 1024 * MIB); assert_eq!(scaled.profile, MemoryProfile::Gib1); assert!(scaled.is_scaled()); assert_eq!(scaled.effective_profile_label(), "custom (base: 1g)"); let canonical = MemoryProfileConfig::for_limit(1024 * MIB); assert!(!canonical.is_scaled()); assert_eq!(canonical.effective_profile_label(), "1g"); }
}
