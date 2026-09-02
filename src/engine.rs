//! Query engine orchestration.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, VecDeque},
    error::Error as StdError,
    fmt, io,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
    time::Instant,
};

const DEFAULT_PLANNER_CACHE_BYTES: usize = 8 * 1024 * 1024;

// .29 diagnostics: external-group spill is intentionally instrumented at the
// execution layer so `_memory` can expose where bounded execution spends its
// disk and merge budget without coupling Storage to Query internals.
static EXTERNAL_GROUP_FLUSHES: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_PARTIALS_WRITTEN: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_FLUSH_US: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_PARTIALS_MERGED: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_MERGE_US: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_PEAK_GROUPS_PER_FLUSH: AtomicU64 = AtomicU64::new(0);
// .30 microscope: account for the CPU and memory phases surrounding spill.
static EXTERNAL_GROUP_SOURCE_US: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_ROWS_CONSUMED: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_CONSUME_SAMPLES: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_KEY_NS: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_LOOKUP_NS: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_ACCUMULATE_NS: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_GROUP_HITS: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_GROUP_MISSES: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_ACCUMULATOR_CREATE_US: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_FLUSH_TRIGGERS: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_ESTIMATED_PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_PARTIAL_FINISH_US: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_FLUSH_SORT_US: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_ENCODE_WRITE_US: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_MERGE_INIT_US: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_MERGE_SAMPLES: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_MERGE_SELECT_NS: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_MERGE_PARTIAL_NS: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_MERGE_READ_NS: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_MERGE_FINISH_NS: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_RSS_SAMPLES: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_RSS_PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
static EXTERNAL_GROUP_UNMANAGED_PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
fn update_atomic_peak(target: &AtomicU64, value: u64) {
    target.fetch_max(value, AtomicOrdering::Relaxed);
}

fn sample_external_group_process_memory(governor: &MemoryGovernor) {
    if let Some(process) = governor.process_memory_snapshot() {
        EXTERNAL_GROUP_RSS_SAMPLES.fetch_add(1, AtomicOrdering::Relaxed);
        update_atomic_peak(
            &EXTERNAL_GROUP_RSS_PEAK_BYTES,
            usize_to_u64_saturating(process.rss_bytes),
        );
        update_atomic_peak(
            &EXTERNAL_GROUP_UNMANAGED_PEAK_BYTES,
            usize_to_u64_saturating(process.unmanaged_bytes),
        );
    }
}

use crate::storage::StorageReadCapability;
use crate::{
    helpers::{elapsed_micros, elapsed_nanos, usize_to_u64_saturating},
    indexing::{IndexingEngine, IndexingSnapshot, ObservedAccess, QueryObservation},
    memory::{MemoryGovernor, MemorySnapshot, ProcessMemoryPressure},
    query::{
        decode_execution_row, encode_execution_row, execution_row_working_bytes,
        reserve_query_memory, stable_sort, BoundedProjectedTopN, BoundedTopN, ExecutionError,
        ExecutionOutput, ExecutionRow, ExecutionRuntime, ExecutionStatistics, ExecutionStrategies,
        ExecutionStrategy, Executor, LogicalPlan, LookupDocuments, Order, PhysicalOperator,
        PhysicalPlan, PhysicalPlanError, PhysicalPlanner, PhysicalSubPipeline, Planner,
        PlannerCache, PlannerCacheStats, PlannerError, PlannerPipeline, ProjectedValueLayout,
        ProjectedValuePipeline, SortKey, vcollections,
    },
    spill::{SpillEngine, SpillRun, SpillRunReader},
    storage::{
        CollectionId, DocumentId, DocumentVersion, MemoryStorage, StorageEngine, StorageError,
        StoredDocument,
    },
    Document, Value,
};

#[inline]
fn storage_engine_error(error: StorageError) -> EngineError {
    EngineError::execution(ExecutionError::storage(error))
}
#[inline]
fn backend_storage_error(error: impl fmt::Display) -> StorageError {
    StorageError::backend(error.to_string())
}
#[inline]
fn engine_spill_error(error: io::Error) -> EngineError {
    EngineError::execution(spill_engine_error(error))
}

#[inline]
fn group_state_estimate(key_len: usize, field_count: usize) -> usize {
    key_len
        .saturating_mul(2)
        .saturating_add(field_count.saturating_mul(64))
        .saturating_add(384)
}

type GroupAccumulator = Box<dyn crate::query::IncrementalGroupAccumulator>;

fn admit_bounded_hash_group(
    groups: &mut HashMap<Arc<[u8]>, GroupAccumulator>,
    frontier: &mut Option<Arc<[u8]>>,
    estimated_bytes: &mut usize,
    key: &[u8],
    state_estimate: usize,
    field_count: usize,
    budget: usize,
    limit: usize,
) -> Result<bool, StorageError> {
    if groups.len() >= limit {
        let largest = frontier.as_ref().expect("bounded group frontier exists");
        if key >= largest.as_ref() {
            return Ok(false);
        }
        let largest = frontier.take().expect("bounded group frontier exists");
        groups.remove(largest.as_ref());
        *estimated_bytes =
            estimated_bytes.saturating_sub(group_state_estimate(largest.len(), field_count));
    }
    if estimated_bytes.saturating_add(state_estimate) > budget {
        return Err(StorageError::backend(
            "bounded group limit exceeds the governed working set",
        ));
    }
    Ok(true)
}

fn is_bounded_secondary_row_operator(operator: &PhysicalOperator) -> bool {
    matches!(
        operator,
        PhysicalOperator::Filter { .. }
            | PhysicalOperator::Select { .. }
            | PhysicalOperator::Skip { .. }
            | PhysicalOperator::Limit { .. }
            | PhysicalOperator::Custom {
                writes: false,
                changes_cardinality: false,
                ..
            }
    )
}

/// Result returned by engine operations.
pub type EngineResult<T> = std::result::Result<T, EngineError>;

/// Converts a validated logical plan into an executable physical plan.
///
/// Keeping lowering behind an object-safe trait allows the initial scan-based
/// implementation to evolve into a cost-based optimizer without changing
/// [`Engine`] callers.
pub trait PlanLowerer: Send + Sync {
    /// Lowers one logical plan.
    fn lower(
        &self,
        logical: &LogicalPlan,
        physical_planner: &PhysicalPlanner,
    ) -> Result<PhysicalPlan, PhysicalPlanError>;
}

/// High-level OG database engine.
pub struct Engine {
    storage: Arc<dyn StorageEngine>,
    runtime: Arc<dyn ExecutionRuntime>,
    lowerer: Arc<dyn PlanLowerer>,
    planner: Planner,
    physical_planner: PhysicalPlanner,
    executor: Executor,
    planner_cache: PlannerCache,
    indexing: IndexingEngine,
    memory_governor: MemoryGovernor,
}

impl Engine {
    /// Creates an engine with default logical and physical planner options.
    #[must_use]
    #[inline]
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        runtime: Arc<dyn ExecutionRuntime>,
        lowerer: Arc<dyn PlanLowerer>,
    ) -> Self {
        let memory_governor = MemoryGovernor::unlimited();
        Self {
            storage,
            runtime,
            lowerer,
            planner: Planner::new(),
            physical_planner: PhysicalPlanner::new(),
            executor: Executor::new_governed(memory_governor.clone()),
            planner_cache: PlannerCache::new_governed(
                128,
                DEFAULT_PLANNER_CACHE_BYTES,
                memory_governor.clone(),
            ),
            indexing: IndexingEngine::new(),
            memory_governor,
        }
    }

    /// Creates an engine from explicitly configured components.
    #[must_use]
    pub fn with_components(
        storage: Arc<dyn StorageEngine>,
        runtime: Arc<dyn ExecutionRuntime>,
        lowerer: Arc<dyn PlanLowerer>,
        planner: Planner,
        physical_planner: PhysicalPlanner,
        executor: Executor,
    ) -> Self {
        let memory_governor = MemoryGovernor::unlimited();
        Self {
            storage,
            runtime,
            lowerer,
            planner,
            physical_planner,
            executor: executor.with_memory_governor(memory_governor.clone()),
            planner_cache: PlannerCache::new_governed(
                128,
                DEFAULT_PLANNER_CACHE_BYTES,
                memory_governor.clone(),
            ),
            indexing: IndexingEngine::new(),
            memory_governor,
        }
    }

    /// Returns the configured storage engine.
    #[must_use]
    pub fn storage(&self) -> &dyn StorageEngine {
        self.storage.as_ref()
    }

    /// Returns the configured execution runtime.
    #[must_use]
    pub fn runtime(&self) -> &dyn ExecutionRuntime {
        self.runtime.as_ref()
    }

    /// Returns the configured logical planner.
    #[must_use]
    pub const fn planner(&self) -> &Planner {
        &self.planner
    }

    /// Returns the configured physical planner.
    #[must_use]
    pub const fn physical_planner(&self) -> &PhysicalPlanner {
        &self.physical_planner
    }

    /// Returns the configured executor.
    #[must_use]
    pub const fn executor(&self) -> &Executor {
        &self.executor
    }

    /// Builds a validated logical plan from a normalized pipeline.
    pub fn plan_logical(&self, pipeline: &PlannerPipeline) -> EngineResult<LogicalPlan> {
        self.planner.plan(pipeline).map_err(EngineError::planning)
    }

    /// Lowers a validated logical plan to a physical plan.
    pub fn plan_physical(&self, logical: &LogicalPlan) -> EngineResult<PhysicalPlan> {
        self.lowerer
            .lower(logical, &self.physical_planner)
            .map_err(EngineError::physical_planning)
    }

    /// Plans a normalized pipeline through both planning layers.
    pub fn plan(&self, pipeline: &PlannerPipeline) -> EngineResult<PlannedQuery> {
        let logical = self.plan_logical(pipeline)?;
        let physical = self.plan_physical(&logical)?;

        Ok(PlannedQuery { logical, physical })
    }

    /// Plans with a bounded cache keyed by normalized request text.
    pub fn plan_cached(&self, key: &str, pipeline: &PlannerPipeline) -> EngineResult<PlannedQuery> {
        if let Some(planned) = self.planner_cache.get(key) {
            return Ok(planned);
        }
        let planned = self.plan(pipeline)?;
        self.planner_cache.insert(key.to_owned(), planned.clone());
        Ok(planned)
    }

    pub fn planner_cache_stats(&self) -> PlannerCacheStats {
        self.planner_cache.stats()
    }
    pub fn invalidate_planner_cache(&self) {
        self.planner_cache.invalidate_all();
    }

    /// Returns a point-in-time snapshot of passive indexing observations.
    #[must_use]
    pub fn indexing_snapshot(&self) -> IndexingSnapshot {
        self.indexing.snapshot()
    }

    /// Returns the shared memory governor.
    #[must_use]
    pub const fn memory_governor(&self) -> &MemoryGovernor {
        &self.memory_governor
    }

    /// Returns current memory-governor diagnostics.
    #[must_use]
    pub fn memory_snapshot(&self) -> MemorySnapshot {
        self.memory_governor.snapshot()
    }

    /// Replaces the governor used by this engine and its planner cache.
    ///
    /// Existing cached plans are intentionally discarded so every live cache
    /// reservation belongs to the newly configured governor.
    #[must_use]
    pub fn with_memory_governor(mut self, governor: MemoryGovernor) -> Self {
        let planner_cache_bytes = governor.profile().planner_cache_bytes;
        self.planner_cache = PlannerCache::new_governed(128, planner_cache_bytes, governor.clone());
        self.executor = Executor::new_governed(governor.clone());
        self.memory_governor = governor;
        self
    }

    /// Streams a read-only pipeline without materializing all rows.
    ///
    /// Returns `Ok(None)` when the physical plan contains an operator that
    /// requires set-level materialization or performs writes.
    pub fn stream_read_pipeline(
        &self,
        physical: &PhysicalPlan,
        visitor: &mut dyn FnMut(StoredDocument) -> EngineResult<()>,
    ) -> EngineResult<Option<ExecutionStatistics>> {
        if self.system_collection_storage(physical)?.is_some() || !physical.is_memory_streaming() {
            return Ok(None);
        }

        let started = Instant::now();
        let read = self.storage.read().map_err(storage_engine_error)?;
        let count_alias = physical
            .operators()
            .last()
            .and_then(PhysicalOperator::count_alias);
        let data_operator_len = physical
            .operators()
            .len()
            .saturating_sub(usize::from(count_alias.is_some()));

        if let Some(alias) = count_alias.filter(|_| data_operator_len == 0) {
            let count = read
                .count(physical.source().collection())
                .map_err(storage_engine_error)?;
            emit_streaming_count(visitor, alias, count)?;
            let statistics =
                ExecutionStatistics::streamed_pipeline(0, 0, 1, ExecutionStrategy::DirectCount);
            self.indexing.observe(QueryObservation::from_execution(
                physical,
                statistics,
                started.elapsed(),
            ));
            return Ok(Some(statistics));
        }

        // Standard ProjectedValues terminal count. Any compatible prefix
        // (currently Filter/Select) stays on physical values and only
        // cardinality reaches the terminal consumer.
        if let (Some(alias), crate::query::PhysicalAccess::CollectionScan { options }) =
            (count_alias, physical.source().access())
        {
            if data_operator_len > 0 {
                if let (true, Some(value_pipeline)) = (
                    read.support(StorageReadCapability::ProjectedValuesGatedUnordered)
                        .available(),
                    ProjectedValuePipeline::compile(
                        &physical.operators()[..data_operator_len],
                        std::iter::empty::<crate::query::ExpressionFieldPath>(),
                    )
                    .map_err(EngineError::execution)?,
                ) {
                    let mut projected_count = 0u64;
                    let mut projected_scanned = 0u64;
                    let mut projected_filtered = 0u64;
                    read.scan_projected_values_gated_unordered_each(
                        physical.source().collection(),
                        *options,
                        value_pipeline.layout().storage_fields(),
                        value_pipeline.gate_field_count(),
                        &mut |values| {
                            projected_scanned = projected_scanned.saturating_add(1);
                            let accepted = value_pipeline
                                .accepts_with(values, |expression, resolver| {
                                    self.runtime
                                        .evaluate_resolved_predicate(expression, resolver)
                                })
                                .map_err(backend_storage_error)?;
                            if !accepted {
                                projected_filtered = projected_filtered.saturating_add(1);
                            }
                            Ok(accepted)
                        },
                        &mut |_values| {
                            projected_count = projected_count.saturating_add(1);
                            Ok(true)
                        },
                    )
                    .map_err(storage_engine_error)?;

                    emit_streaming_count(visitor, alias, projected_count)?;
                    let statistics = ExecutionStatistics::streamed_with_strategies(
                        projected_scanned,
                        projected_filtered,
                        1,
                        ExecutionStrategies::default()
                            .with(ExecutionStrategy::CollectionScan)
                            .with(ExecutionStrategy::StreamingCount),
                    );
                    self.indexing.observe(QueryObservation::from_execution(
                        physical,
                        statistics,
                        started.elapsed(),
                    ));
                    return Ok(Some(statistics));
                }
            }
        }

        let mut streamed_count = 0_u64;
        let mut scanned = 0_u64;
        let mut filtered = 0_u64;
        let mut returned = 0_u64;
        let mut skip_remaining = physical
            .operators()
            .iter()
            .map(|operator| match operator {
                PhysicalOperator::Skip { count } => Some(*count),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut limit_remaining = physical
            .operators()
            .iter()
            .map(|operator| match operator {
                PhysicalOperator::Limit { count } => Some(*count),
                _ => None,
            })
            .collect::<Vec<_>>();

        if limit_remaining
            .iter()
            .any(|remaining| *remaining == Some(0))
        {
            let strategy = match physical.source().access() {
                crate::query::PhysicalAccess::CollectionScan { .. } => {
                    ExecutionStrategy::CollectionScan
                }
                crate::query::PhysicalAccess::PrimaryKeyLookup { .. } => {
                    ExecutionStrategy::PrimaryKeyLookup
                }
            };
            let statistics = if let Some(alias) = count_alias {
                emit_streaming_count(visitor, alias, 0)?;
                ExecutionStatistics::streamed_with_strategies(
                    0,
                    0,
                    1,
                    ExecutionStrategies::default()
                        .with(strategy)
                        .with(ExecutionStrategy::StreamingCount),
                )
            } else {
                ExecutionStatistics::streamed_pipeline(0, 0, 0, strategy)
            };
            self.indexing.observe(QueryObservation::from_execution(
                physical,
                statistics,
                started.elapsed(),
            ));
            return Ok(Some(statistics));
        }

        let strategy = match physical.source().access() {
            crate::query::PhysicalAccess::CollectionScan { options } => {
                // Push a simple terminal LIMIT into the storage scan. This is
                // semantically safe because there is no filter/skip/cardinality
                // changing operator before it, and it lets Glacier use its
                // sequential short-scan path instead of materializing/sorting a
                // multi-million-entry lazy primary index just to return N rows.
                let requested_window = match physical.operators().get(..data_operator_len) {
                    Some([PhysicalOperator::Limit { count }]) => Some(*count),
                    Some(
                        [PhysicalOperator::Skip { count: skip }, PhysicalOperator::Limit { count: limit }],
                    ) => {
                        let skip = *skip;
                        let limit = *limit;
                        Some(skip.saturating_add(limit))
                    }
                    _ => None,
                };
                let storage_options =
                    requested_window.map_or(*options, |requested| match options.limit() {
                        Some(existing) => (*options).with_limit(existing.min(requested)),
                        None => (*options).with_limit(requested),
                    });

                // Standard projected-value streaming path for pipelines made only
                // of Filter/Select stages and ending in Select. Glacier can read
                // the required top-level values directly from physical records,
                // evaluate filters without materializing full documents, and build
                // the selected output from those slots. Unsupported shapes keep
                // the established full-document streaming fallback below.
                let data_operators = &physical.operators()[..data_operator_len];
                let terminal_select =
                    data_operators
                        .iter()
                        .rev()
                        .find_map(|operator| match operator {
                            PhysicalOperator::Select { fields } => Some(fields),
                            _ => None,
                        });
                let projected_streaming_compatible = terminal_select.is_some()
                    && data_operators.iter().all(|operator| {
                        matches!(
                            operator,
                            PhysicalOperator::Filter { .. } | PhysicalOperator::Select { .. }
                        )
                    });

                if projected_streaming_compatible
                    && read.support(StorageReadCapability::ProjectedValuesGatedUnordered)
                        == crate::storage::StorageSupport::Native
                {
                    let selected_fields = terminal_select.expect("checked projected select");
                    if let Some(value_pipeline) = ProjectedValuePipeline::compile(
                        data_operators,
                        selected_fields.iter().cloned(),
                    )
                    .map_err(EngineError::execution)?
                    {
                        let selected_slots = value_pipeline
                            .layout()
                            .slots(selected_fields)
                            .map_err(EngineError::execution)?;

                        read.scan_projected_row_values_gated_unordered_each(
                            physical.source().collection(),
                            storage_options,
                            value_pipeline.layout().storage_fields(),
                            value_pipeline.gate_field_count(),
                            &mut |values| {
                                scanned = scanned.saturating_add(1);
                                let accepted = value_pipeline
                                    .accepts_with(values, |expression, resolver| {
                                        self.runtime
                                            .evaluate_resolved_predicate(expression, resolver)
                                    })
                                    .map_err(backend_storage_error)?;
                                if !accepted {
                                    filtered = filtered.saturating_add(1);
                                }
                                Ok(accepted)
                            },
                            &mut |id, version, values| {
                                let mut selected = Document::new();
                                for (field, slot) in selected_fields.iter().zip(&selected_slots) {
                                    if let Some(value) = values.get(*slot).and_then(Option::as_ref)
                                    {
                                        selected.insert(field.first(), value.clone());
                                    }
                                }

                                let output = StoredDocument::new(id, version, Arc::new(selected))?;
                                visitor(output).map_err(backend_storage_error)?;
                                returned = returned.saturating_add(1);
                                Ok(true)
                            },
                        )
                        .map_err(storage_engine_error)?;

                        let statistics = ExecutionStatistics::streamed_pipeline(
                            scanned,
                            filtered,
                            returned,
                            ExecutionStrategy::CollectionScan,
                        );
                        self.indexing.observe(QueryObservation::from_execution(
                            physical,
                            statistics,
                            started.elapsed(),
                        ));
                        return Ok(Some(statistics));
                    }
                }

                let leading_filter_len = data_operators
                    .iter()
                    .take_while(|operator| matches!(operator, PhysicalOperator::Filter { .. }))
                    .count();
                let leading_filter_pipeline = if leading_filter_len > 0
                    && read.support(StorageReadCapability::ProjectedValuesGatedUnordered)
                        == crate::storage::StorageSupport::Native
                {
                    ProjectedValuePipeline::compile(
                        &data_operators[..leading_filter_len],
                        std::iter::empty::<crate::query::ExpressionFieldPath>(),
                    )
                    .map_err(EngineError::execution)?
                } else {
                    None
                };
                let source_gated = leading_filter_pipeline.is_some();

                let mut process_stored = |stored: StoredDocument| {
                    if !source_gated {
                        scanned = scanned.saturating_add(1);
                    }
                    let (id, version, mut document) = stored.into_parts();
                    let mut keep = true;
                    let mut stop_after_row = false;

                    for (index, operator) in physical
                        .operators()
                        .iter()
                        .take(data_operator_len)
                        .enumerate()
                        .skip(if source_gated { leading_filter_len } else { 0 })
                    {
                        match operator {
                            PhysicalOperator::Filter { predicate } => {
                                let mut evaluation = document.as_ref().clone();
                                evaluation.insert("_id", Value::from(id.to_string()));
                                if !self
                                    .runtime
                                    .evaluate_predicate(predicate, &evaluation)
                                    .map_err(backend_storage_error)?
                                {
                                    keep = false;
                                    filtered = filtered.saturating_add(1);
                                    break;
                                }
                            }
                            PhysicalOperator::Select { fields } => {
                                let mut evaluation = document.as_ref().clone();
                                evaluation.insert("_id", Value::from(id.to_string()));
                                let selected = self
                                    .runtime
                                    .apply_select(fields, &evaluation)
                                    .map_err(backend_storage_error)?;
                                let mut selected = selected.as_ref().clone();
                                selected.remove("_id");
                                document = Arc::new(selected);
                            }
                            PhysicalOperator::Custom {
                                name,
                                arguments,
                                writes: false,
                                changes_cardinality: false,
                            } => {
                                let mut evaluation = document.as_ref().clone();
                                evaluation.insert("_id", Value::from(id.to_string()));
                                match self
                                    .runtime
                                    .apply_custom(name, arguments, false, &evaluation)
                                    .map_err(backend_storage_error)?
                                {
                                    crate::query::CustomOperatorResult::Keep => {}
                                    crate::query::CustomOperatorResult::Replace(replacement) => {
                                        let mut replacement = replacement.as_ref().clone();
                                        replacement.remove("_id");
                                        document = Arc::new(replacement);
                                    }
                                    crate::query::CustomOperatorResult::Discard => {
                                        keep = false;
                                        filtered = filtered.saturating_add(1);
                                        break;
                                    }
                                    crate::query::CustomOperatorResult::Expand(_) => {
                                        return Err(StorageError::backend(
                                            "streaming custom operator unexpectedly expanded rows",
                                        ));
                                    }
                                }
                            }
                            PhysicalOperator::Skip { .. } => {
                                let remaining = skip_remaining[index].as_mut().expect("skip state");
                                if *remaining > 0 {
                                    *remaining -= 1;
                                    keep = false;
                                    filtered = filtered.saturating_add(1);
                                    break;
                                }
                            }
                            PhysicalOperator::Limit { .. } => {
                                let remaining =
                                    limit_remaining[index].as_mut().expect("limit state");
                                if *remaining == 0 {
                                    return Ok(false);
                                }
                                *remaining -= 1;
                                stop_after_row |= *remaining == 0;
                            }
                            _ => unreachable!("validated streaming operator"),
                        }
                    }

                    if keep {
                        if count_alias.is_some() {
                            streamed_count = streamed_count.saturating_add(1);
                        } else {
                            let output = StoredDocument::new(id, version, document)?;
                            visitor(output).map_err(backend_storage_error)?;
                            returned = returned.saturating_add(1);
                        }
                    }
                    Ok(!stop_after_row)
                };

                if let Some(value_pipeline) = leading_filter_pipeline {
                    let mut gated_scanned = 0u64;
                    let mut gated_filtered = 0u64;
                    read.scan_projected_gated_each(
                        physical.source().collection(),
                        storage_options,
                        value_pipeline.layout().storage_fields(),
                        &mut |values| {
                            gated_scanned = gated_scanned.saturating_add(1);
                            let accepted = value_pipeline
                                .accepts_with(values, |expression, resolver| {
                                    self.runtime
                                        .evaluate_resolved_predicate(expression, resolver)
                                })
                                .map_err(backend_storage_error)?;
                            if !accepted {
                                gated_filtered = gated_filtered.saturating_add(1);
                            }
                            Ok(accepted)
                        },
                        &mut process_stored,
                    )
                    .map_err(storage_engine_error)?;
                    scanned = scanned.saturating_add(gated_scanned);
                    filtered = filtered.saturating_add(gated_filtered);
                } else {
                    read.scan_each(
                        physical.source().collection(),
                        storage_options,
                        &mut process_stored,
                    )
                    .map_err(storage_engine_error)?;
                }
                ExecutionStrategy::CollectionScan
            }
            crate::query::PhysicalAccess::PrimaryKeyLookup { id } => {
                if let Some(stored) = read
                    .get(physical.source().collection(), id)
                    .map_err(storage_engine_error)?
                {
                    scanned = 1;
                    let (row_id, version, mut document) = stored.into_parts();
                    let mut keep = true;
                    for (index, operator) in physical
                        .operators()
                        .iter()
                        .take(data_operator_len)
                        .enumerate()
                    {
                        match operator {
                            PhysicalOperator::Filter { predicate } => {
                                let mut evaluation = document.as_ref().clone();
                                evaluation.insert("_id", Value::from(row_id.to_string()));
                                if !self
                                    .runtime
                                    .evaluate_predicate(predicate, &evaluation)
                                    .map_err(EngineError::execution)?
                                {
                                    keep = false;
                                    filtered = filtered.saturating_add(1);
                                    break;
                                }
                            }
                            PhysicalOperator::Select { fields } => {
                                let mut evaluation = document.as_ref().clone();
                                evaluation.insert("_id", Value::from(row_id.to_string()));
                                let selected = self
                                    .runtime
                                    .apply_select(fields, &evaluation)
                                    .map_err(EngineError::execution)?;
                                let mut selected = selected.as_ref().clone();
                                selected.remove("_id");
                                document = Arc::new(selected);
                            }
                            PhysicalOperator::Custom {
                                name,
                                arguments,
                                writes: false,
                                changes_cardinality: false,
                            } => {
                                let mut evaluation = document.as_ref().clone();
                                evaluation.insert("_id", Value::from(row_id.to_string()));
                                match self
                                    .runtime
                                    .apply_custom(name, arguments, false, &evaluation)
                                    .map_err(EngineError::execution)?
                                {
                                    crate::query::CustomOperatorResult::Keep => {}
                                    crate::query::CustomOperatorResult::Replace(replacement) => {
                                        let mut replacement = replacement.as_ref().clone();
                                        replacement.remove("_id");
                                        document = Arc::new(replacement);
                                    }
                                    crate::query::CustomOperatorResult::Discard => {
                                        keep = false;
                                        filtered = filtered.saturating_add(1);
                                        break;
                                    }
                                    crate::query::CustomOperatorResult::Expand(_) => {
                                        return Err(EngineError::execution(
                                            ExecutionError::evaluation(
                                                "streaming custom operator unexpectedly expanded rows",
                                            ),
                                        ));
                                    }
                                }
                            }
                            PhysicalOperator::Skip { .. } => {
                                let remaining = skip_remaining[index].as_mut().expect("skip state");
                                if *remaining > 0 {
                                    *remaining -= 1;
                                    keep = false;
                                    filtered = filtered.saturating_add(1);
                                    break;
                                }
                            }
                            PhysicalOperator::Limit { .. } => {
                                let remaining =
                                    limit_remaining[index].as_mut().expect("limit state");
                                if *remaining == 0 {
                                    keep = false;
                                    break;
                                }
                                *remaining -= 1;
                            }
                            _ => unreachable!("validated streaming operator"),
                        }
                    }
                    if keep {
                        if count_alias.is_some() {
                            streamed_count = 1;
                        } else {
                            let output = StoredDocument::new(row_id, version, document)
                                .map_err(storage_engine_error)?;
                            visitor(output)?;
                            returned = 1;
                        }
                    }
                }
                ExecutionStrategy::PrimaryKeyLookup
            }
        };
        let statistics = if let Some(alias) = count_alias {
            emit_streaming_count(visitor, alias, streamed_count)?;
            ExecutionStatistics::streamed_with_strategies(
                scanned,
                filtered,
                1,
                ExecutionStrategies::default()
                    .with(strategy)
                    .with(ExecutionStrategy::StreamingCount),
            )
        } else {
            ExecutionStatistics::streamed_pipeline(scanned, filtered, returned, strategy)
        };
        self.indexing.observe(QueryObservation::from_execution(
            physical,
            statistics,
            started.elapsed(),
        ));
        Ok(Some(statistics))
    }

    /// Compatibility alias for source-only callers.
    pub fn stream_source_only(
        &self,
        physical: &PhysicalPlan,
        visitor: &mut dyn FnMut(StoredDocument) -> EngineResult<()>,
    ) -> EngineResult<Option<ExecutionStatistics>> {
        self.stream_read_pipeline(physical, visitor)
    }

    /// Executes an already validated physical plan.
    pub fn execute_physical(&self, physical: &PhysicalPlan) -> EngineResult<ExecutionOutput> {
        let started = Instant::now();
        let output = if let Some(storage) = self.system_collection_storage(physical)? {
            self.executor
                .execute(&storage, self.runtime.as_ref(), physical)
                .map_err(EngineError::execution)?
        } else {
            self.executor
                .execute(self.storage.as_ref(), self.runtime.as_ref(), physical)
                .map_err(EngineError::execution)?
        };
        self.indexing.observe(QueryObservation::from_execution(
            physical,
            output.statistics(),
            started.elapsed(),
        ));
        Ok(output)
    }

    /// Executes a physical plan under a trusted Place or AppInstance document scope.
    pub fn execute_physical_scoped(
        &self,
        physical: &PhysicalPlan,
        scope: &crate::query::DocumentScope,
    ) -> EngineResult<ExecutionOutput> {
        let started = Instant::now();
        if physical.source().collection().as_str().starts_with('_') {
            return Err(EngineError::execution(
                ExecutionError::unsupported_operator(
                    "scoped-system-collection",
                    "Place-scoped queries cannot target system collections",
                ),
            ));
        }
        let output = self
            .executor
            .execute_scoped(
                self.storage.as_ref(),
                self.runtime.as_ref(),
                physical,
                Some(scope),
            )
            .map_err(EngineError::execution)?;
        self.indexing.observe(QueryObservation::from_execution(
            physical,
            output.statistics(),
            started.elapsed(),
        ));
        Ok(output)
    }

    /// Streams one bounded read-only secondary branch used by UNION and LOOKUP.
    ///
    /// The branch keeps row-local stages streaming and shares one implementation
    /// for both compound operators.  LOOKUP may provide an outer document so
    /// nested predicates keep their alias-aware semantics without materializing
    /// the secondary collection.
    fn stream_bounded_secondary_pipeline(
        &self,
        collection: &CollectionId,
        pipeline: &PhysicalSubPipeline,
        lookup_context: Option<(&Document, Option<&str>)>,
        union_origin: bool,
        visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>,
    ) -> EngineResult<ExecutionStatistics> {
        let read = self.storage.read().map_err(storage_engine_error)?;
        let operators = pipeline.operators();
        if !operators.iter().all(is_bounded_secondary_row_operator) {
            return Err(EngineError::execution(ExecutionError::evaluation(
                "secondary pipeline contains an operator without a bounded streaming executor",
            )));
        }

        let mut skip_remaining = operators
            .iter()
            .map(|operator| match operator {
                PhysicalOperator::Skip { count } => Some(*count),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut limit_remaining = operators
            .iter()
            .map(|operator| match operator {
                PhysicalOperator::Limit { count } => Some(*count),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut scanned = 0u64;
        let mut filtered = 0u64;
        let mut returned = 0u64;

        if limit_remaining
            .iter()
            .any(|remaining| *remaining == Some(0))
        {
            return Ok(ExecutionStatistics::streamed_pipeline(
                0,
                0,
                0,
                ExecutionStrategy::CollectionScan,
            ));
        }

        // Preserve stage order while pushing only the two trivially safe bounded
        // windows into Storage. This matters for UNION's common skip+limit branch
        // and for LOOKUP prefixes such as `limit N | where ...`.
        let requested_window = match operators {
            [PhysicalOperator::Limit { count }] => Some(*count),
            [PhysicalOperator::Skip { count: skip }, PhysicalOperator::Limit { count: limit }] => {
                Some((*skip).saturating_add(*limit))
            }
            [PhysicalOperator::Limit { count }, ..] => Some(*count),
            _ => None,
        };
        let options = requested_window.map_or_else(crate::storage::ScanOptions::default, |count| {
            crate::storage::ScanOptions::default().with_limit(count)
        });

        read.scan_each(collection, options, &mut |stored| {
            scanned = scanned.saturating_add(1);
            let mut row = if union_origin {
                ExecutionRow::from_union(stored)
            } else {
                ExecutionRow::from_stored(stored)
            };
            let mut keep = true;
            let mut stop_after_row = false;

            for (index, operator) in operators.iter().enumerate() {
                match operator {
                    PhysicalOperator::Filter { predicate } => {
                        let evaluation = row.evaluation_document();
                        let accepted = match lookup_context {
                            Some((outer, alias)) => self.runtime.evaluate_lookup_predicate(
                                predicate,
                                outer,
                                alias,
                                &evaluation,
                            ),
                            None => self.runtime.evaluate_predicate(predicate, &evaluation),
                        }
                        .map_err(backend_storage_error)?;
                        if !accepted {
                            keep = false;
                            filtered = filtered.saturating_add(1);
                            break;
                        }
                    }
                    PhysicalOperator::Select { fields } => {
                        let evaluation = row.evaluation_document();
                        let selected = self
                            .runtime
                            .apply_select(fields, &evaluation)
                            .map_err(backend_storage_error)?;
                        row.replace_document(selected, false);
                    }
                    PhysicalOperator::Custom {
                        name,
                        arguments,
                        writes: false,
                        changes_cardinality: false,
                    } => {
                        let evaluation = row.evaluation_document();
                        match self
                            .runtime
                            .apply_custom(name, arguments, false, &evaluation)
                            .map_err(backend_storage_error)?
                        {
                            crate::query::CustomOperatorResult::Keep => {}
                            crate::query::CustomOperatorResult::Replace(document) => {
                                row.replace_document(document, false);
                            }
                            crate::query::CustomOperatorResult::Discard => {
                                keep = false;
                                filtered = filtered.saturating_add(1);
                                break;
                            }
                            crate::query::CustomOperatorResult::Expand(_) => {
                                return Err(StorageError::backend(
                                    "bounded secondary custom operator unexpectedly expanded rows",
                                ));
                            }
                        }
                    }
                    PhysicalOperator::Skip { .. } => {
                        let remaining = skip_remaining[index].as_mut().expect("skip state");
                        if *remaining > 0 {
                            *remaining -= 1;
                            keep = false;
                            filtered = filtered.saturating_add(1);
                            break;
                        }
                    }
                    PhysicalOperator::Limit { .. } => {
                        let remaining = limit_remaining[index].as_mut().expect("limit state");
                        if *remaining == 0 {
                            return Ok(false);
                        }
                        *remaining -= 1;
                        stop_after_row |= *remaining == 0;
                    }
                    _ => unreachable!("validated bounded secondary row operator"),
                }
            }

            if keep {
                visitor(row).map_err(backend_storage_error)?;
                returned = returned.saturating_add(1);
            }
            Ok(!stop_after_row)
        })
        .map_err(storage_engine_error)?;

        Ok(ExecutionStatistics::streamed_pipeline(
            scanned,
            filtered,
            returned,
            ExecutionStrategy::CollectionScan,
        ))
    }

    /// Returns whether the governed blocking streaming executor supports this plan shape.
    #[must_use]
    pub fn supports_governed_blocking_streaming(&self, physical: &PhysicalPlan) -> bool {
        if physical.source().collection().as_str().starts_with('_') {
            return false;
        }
        if physical.is_write() || physical.is_memory_streaming() {
            return false;
        }
        let Some(index) = physical.operators().iter().position(|operator| {
            matches!(
                operator.execution_properties().flow,
                crate::query::Flow::GovernedBlocking
            )
        }) else {
            return false;
        };
        if !physical.operators()[..index].iter().all(|operator| {
            matches!(
                operator.execution_properties().flow,
                crate::query::Flow::Streaming
            )
        }) {
            return false;
        }
        let suffix = &physical.operators()[index..];
        let bounded = suffix
            .last()
            .is_some_and(|operator| operator.execution_properties().linear_bound().is_some());
        let core = if bounded {
            &suffix[..suffix.len() - 1]
        } else {
            suffix
        };
        match core {
            [operator] => {
                matches!(operator.execution_properties().order, Order::Ordered(_))
                    || matches!(
                        operator,
                        PhysicalOperator::Distinct { .. } | PhysicalOperator::Group { .. }
                    )
                    || matches!(
                        operator,
                        PhysicalOperator::Lookup { pipeline, .. }
                            | PhysicalOperator::Union { pipeline, .. }
                            if suffix.len() == 1
                                && pipeline
                                    .operators()
                                    .iter()
                                    .all(is_bounded_secondary_row_operator)
                    )
            }
            [PhysicalOperator::Group { .. }, ordering] => {
                matches!(ordering.execution_properties().order, Order::Ordered(_))
            }
            [ordering, PhysicalOperator::Distinct { .. }] if bounded => {
                matches!(ordering.execution_properties().order, Order::Ordered(_))
            }
            _ => false,
        }
    }

    /// Executes Top-N from the shared projected-value access vector and
    /// hydrates only retained winners. Filter prefixes compose on the same
    /// borrowed scalar row, so combinations inherit late materialization
    /// without adding stage-specific execution paths.
    fn try_projected_top_n(
        &self,
        prefix: &PhysicalPlan,
        keys: &[crate::query::SortKey],
        limit: usize,
    ) -> EngineResult<Option<(Vec<ExecutionRow>, ExecutionStatistics)>> {
        if limit == 0
            || prefix
                .operators()
                .iter()
                .any(|operator| !matches!(operator, PhysicalOperator::Filter { .. }))
        {
            return Ok(None);
        }
        let crate::query::PhysicalAccess::CollectionScan { options } = prefix.source().access()
        else {
            return Ok(None);
        };

        let Some(projected) = ProjectedValuePipeline::compile(
            prefix.operators(),
            keys.iter().map(|key| key.field().clone()),
        )
        .map_err(EngineError::execution)?
        else {
            return Ok(None);
        };
        let sort_fields = keys
            .iter()
            .map(|key| key.field().clone())
            .collect::<Vec<_>>();
        let sort_slots = projected
            .layout()
            .slots(&sort_fields)
            .map_err(EngineError::execution)?;

        let read = self.storage.read().map_err(storage_engine_error)?;
        let mut top = BoundedProjectedTopN::new(limit);
        let mut scanned = 0u64;
        read.scan_projected_row_refs_unordered_each(
            prefix.source().collection(),
            *options,
            projected.layout().storage_fields(),
            &mut |id, version, values| {
                scanned = scanned.saturating_add(1);
                let accepted = projected
                    .accepts_refs_with(values, |expression, resolver| {
                        self.runtime
                            .evaluate_resolved_predicate(expression, resolver)
                    })
                    .map_err(backend_storage_error)?;
                if accepted {
                    top.push_refs(keys, &sort_slots, id, version, values)
                        .map_err(backend_storage_error)?;
                }
                Ok(true)
            },
        )
        .map_err(storage_engine_error)?;

        let projected = top
            .into_sorted_winners(keys)
            .map_err(EngineError::execution)?;
        let mut hydrated = Vec::with_capacity(projected.len());
        for winner in projected {
            let Some(stored) = read
                .get(prefix.source().collection(), winner.id())
                .map_err(storage_engine_error)?
            else {
                continue;
            };
            if stored.version() == winner.version() {
                hydrated.push(ExecutionRow::from_stored(stored));
            }
        }

        let returned = hydrated.len() as u64;
        Ok(Some((
            hydrated,
            ExecutionStatistics::streamed_with_strategies(
                scanned,
                scanned.saturating_sub(returned),
                returned,
                ExecutionStrategies::default()
                    .with(ExecutionStrategy::CollectionScan)
                    .with(ExecutionStrategy::TopN),
            ),
        )))
    }

    fn try_in_memory_distinct(
        &self,
        prefix: &PhysicalPlan,
        fields: &[crate::query::ExpressionFieldPath],
        budget: usize,
    ) -> EngineResult<Option<(Vec<ExecutionRow>, ExecutionStatistics)>> {
        let mut rows: BTreeMap<Arc<[u8]>, ExecutionRow> = BTreeMap::new();
        let mut estimated_bytes = 0usize;
        let exceeded = std::cell::Cell::new(false);
        let statistics = self
            .stream_read_pipeline(prefix, &mut |stored| {
                if exceeded.get() {
                    return Ok(());
                }
                let row = ExecutionRow::from_stored(stored);
                let key = self
                    .runtime
                    .distinct_key(fields, row.document())
                    .map_err(EngineError::execution)?;
                if rows.contains_key(&key) {
                    return Ok(());
                }
                let row_bytes = encode_execution_row(&row)
                    .map_err(EngineError::execution)?
                    .len();
                let estimate = key.len().saturating_add(row_bytes).saturating_add(96);
                if estimated_bytes.saturating_add(estimate) > budget {
                    exceeded.set(true);
                    rows.clear();
                    return Ok(());
                }
                estimated_bytes = estimated_bytes.saturating_add(estimate);
                rows.insert(key, row);
                Ok(())
            })?
            .expect("validated streaming prefix");
        if exceeded.get() {
            Ok(None)
        } else {
            Ok(Some((rows.into_values().collect(), statistics)))
        }
    }

    fn try_in_memory_incremental_group(
        &self,
        prefix: &PhysicalPlan,
        keys: &[crate::query::ExpressionFieldPath],
        budget: usize,
        group_limit: Option<usize>,
    ) -> EngineResult<Option<(Vec<ExecutionRow>, ExecutionStatistics)>> {
        if self
            .runtime
            .incremental_group_accumulator(keys)
            .map_err(EngineError::execution)?
            .is_none()
        {
            return Ok(None);
        }

        let (grouping_keys, required_input_fields) =
            crate::query::group_field_layout(keys).map_err(EngineError::execution)?;

        // Standard projected-value access vector: any compatible streaming
        // prefix can stay on physical values until the group consumer. `.25`
        // starts with Filter stages; unsupported prefixes fall back to the
        // established Document pipeline below.
        if let crate::query::PhysicalAccess::CollectionScan { options } = prefix.source().access() {
            if let Some(value_pipeline) =
                ProjectedValuePipeline::compile(prefix.operators(), required_input_fields.clone())
                    .map_err(EngineError::execution)?
            {
                let layout = value_pipeline.layout();
                let group_layout = ProjectedValueLayout::new(required_input_fields.clone())
                    .map_err(EngineError::execution)?;
                let group_source_slots = layout
                    .slots(group_layout.fields())
                    .map_err(EngineError::execution)?;
                let key_indexes = group_layout
                    .slots(&grouping_keys)
                    .map_err(EngineError::execution)?;

                let read = self.storage.read().map_err(storage_engine_error)?;
                if !read
                    .support(StorageReadCapability::ProjectedValuesGatedUnordered)
                    .available()
                {
                    return Ok(None);
                }
                // Projected group keys are already encoded into `key_buffer`. Use a
                // hash table so the overwhelmingly-common hit path performs one
                // borrowed `[u8]` lookup and never materializes an owned key.
                // Owned `Arc<[u8]>` keys are created only on misses.
                let mut groups: HashMap<
                    Arc<[u8]>,
                    Box<dyn crate::query::IncrementalGroupAccumulator>,
                > = HashMap::new();
                let mut estimated_bytes = 0usize;
                let mut bounded_frontier: Option<Arc<[u8]>> = None;
                let exceeded = std::cell::Cell::new(false);
                let mut scanned = 0u64;
                let mut filtered = 0u64;
                let key_materializer = crate::query::RuntimeMaterializer::new();
                let mut key_buffer = Vec::<u8>::with_capacity(64);
                let mut group_values = vec![None; group_source_slots.len()];
                let mut group_probe = crate::storage::GroupConsumerProbeSnapshot::default();

                if value_pipeline.gate_field_count() == 0 {
                    let storage_key_indexes = key_indexes
                        .iter()
                        .map(|index| group_source_slots[*index])
                        .collect::<Vec<_>>();
                    read.scan_projected_value_refs_unordered_each(
                        prefix.source().collection(),
                        *options,
                        layout.storage_fields(),
                        &mut |values| {
                            if exceeded.get() {
                                return Ok(false);
                            }
                            scanned = scanned.saturating_add(1);
                            let sampled = crate::debug::query_instrumentation_enabled() && scanned & 1023 == 0;
                            if sampled {
                                group_probe.samples = group_probe.samples.saturating_add(1);
                            }

                            let key_started = sampled.then(Instant::now);
                            key_materializer.write_projected_ref_distinct_key_indexes(
                                values,
                                &storage_key_indexes,
                                &mut key_buffer,
                            );
                            if let Some(started) = key_started {
                                group_probe.key_encode_ns = group_probe
                                    .key_encode_ns
                                    .saturating_add(elapsed_nanos(started));
                            }

                            let lookup_started = sampled.then(Instant::now);
                            if let Some(accumulator) = groups.get_mut(key_buffer.as_slice()) {
                                if let Some(started) = lookup_started {
                                    group_probe.lookup_ns = group_probe
                                        .lookup_ns
                                        .saturating_add(elapsed_nanos(started));
                                }
                                group_probe.lookup_hits = group_probe.lookup_hits.saturating_add(1);
                                let aggregate_started = sampled.then(Instant::now);
                                let accepted = accumulator
                                    .push_projected_value_refs(values, &group_source_slots)
                                    .map_err(backend_storage_error)?;
                                if let Some(started) = aggregate_started {
                                    group_probe.aggregate_ns = group_probe
                                        .aggregate_ns
                                        .saturating_add(elapsed_nanos(started));
                                }
                                if !accepted {
                                    return Err(StorageError::backend(
                                        "incremental group runtime rejected borrowed projected-value pipeline",
                                    ));
                                }
                                return Ok(true);
                            }
                            if let Some(started) = lookup_started {
                                group_probe.lookup_ns = group_probe
                                    .lookup_ns
                                    .saturating_add(elapsed_nanos(started));
                            }
                            group_probe.lookup_misses = group_probe.lookup_misses.saturating_add(1);

                            let insert_started = sampled.then(Instant::now);
                            let state_estimate = group_state_estimate(key_buffer.len(), keys.len());
                            if let Some(limit) = group_limit {
                                if !admit_bounded_hash_group(
                                    &mut groups,
                                    &mut bounded_frontier,
                                    &mut estimated_bytes,
                                    key_buffer.as_slice(),
                                    state_estimate,
                                    keys.len(),
                                    budget,
                                    limit,
                                )? {
                                    return Ok(true);
                                }
                            } else if estimated_bytes.saturating_add(state_estimate) > budget {
                                exceeded.set(true);
                                groups.clear();
                                return Ok(false);
                            }
                            let mut accumulator = self
                                .runtime
                                .incremental_group_accumulator(keys)
                                .map_err(backend_storage_error)?
                                .expect("incremental group capability was probed above");
                            estimated_bytes = estimated_bytes.saturating_add(state_estimate);
                            group_probe.key_materializations = group_probe
                                .key_materializations
                                .saturating_add(grouping_keys.len() as u64);
                            let aggregate_started = sampled.then(Instant::now);
                            let accepted = accumulator
                                .push_projected_value_refs(values, &group_source_slots)
                                .map_err(backend_storage_error)?;
                            if let Some(started) = aggregate_started {
                                group_probe.aggregate_ns = group_probe
                                    .aggregate_ns
                                    .saturating_add(elapsed_nanos(started));
                            }
                            if !accepted {
                                return Err(StorageError::backend(
                                    "incremental group runtime rejected borrowed projected-value pipeline",
                                ));
                            }
                            groups.insert(Arc::from(key_buffer.as_slice()), accumulator);
                            if group_limit.is_some_and(|limit| groups.len() >= limit) {
                                bounded_frontier = groups.keys().max().cloned();
                            }
                            if let Some(started) = insert_started {
                                group_probe.insert_ns = group_probe
                                    .insert_ns
                                    .saturating_add(elapsed_nanos(started));
                            }
                            Ok(true)
                        },
                    )
                    .map_err(storage_engine_error)?;
                    crate::storage::record_group_consumer_probe(group_probe);
                } else {
                    read.scan_projected_values_gated_unordered_each(
                        prefix.source().collection(),
                        *options,
                        layout.storage_fields(),
                        value_pipeline.gate_field_count(),
                        &mut |values| {
                            if exceeded.get() {
                                return Ok(false);
                            }
                            scanned = scanned.saturating_add(1);
                            let accepted = value_pipeline
                                .accepts_with(values, |expression, resolver| {
                                    self.runtime
                                        .evaluate_resolved_predicate(expression, resolver)
                                })
                                .map_err(backend_storage_error)?;
                            if !accepted {
                                filtered = filtered.saturating_add(1);
                            }
                            Ok(accepted)
                        },
                        &mut |values| {
                            if exceeded.get() {
                                return Ok(false);
                            }
                            for (target, source) in group_values
                                .iter_mut()
                                .zip(group_source_slots.iter().copied())
                            {
                                *target = values.get(source).cloned().flatten();
                            }

                            key_materializer.write_projected_distinct_key_indexes(
                                &group_values,
                                &key_indexes,
                                &mut key_buffer,
                            );
                            if !groups.contains_key(key_buffer.as_slice()) {
                                let state_estimate =
                                    group_state_estimate(key_buffer.len(), keys.len());
                                if let Some(limit) = group_limit {
                                    if !admit_bounded_hash_group(
                                        &mut groups,
                                        &mut bounded_frontier,
                                        &mut estimated_bytes,
                                        key_buffer.as_slice(),
                                        state_estimate,
                                        keys.len(),
                                        budget,
                                        limit,
                                    )? {
                                        return Ok(true);
                                    }
                                } else if estimated_bytes.saturating_add(state_estimate) > budget {
                                    exceeded.set(true);
                                    groups.clear();
                                    return Ok(false);
                                }
                                let accumulator = self
                                    .runtime
                                    .incremental_group_accumulator(keys)
                                    .map_err(backend_storage_error)?
                                    .expect("incremental group capability was probed above");
                                estimated_bytes = estimated_bytes.saturating_add(state_estimate);
                                groups.insert(Arc::from(key_buffer.as_slice()), accumulator);
                                if group_limit.is_some_and(|limit| groups.len() >= limit) {
                                    bounded_frontier = groups.keys().max().cloned();
                                }
                            }
                            let accepted = groups
                                .get_mut(key_buffer.as_slice())
                                .expect("group accumulator exists")
                                .push_projected_values(&group_values)
                                .map_err(backend_storage_error)?;
                            if !accepted {
                                return Err(StorageError::backend(
                                    "incremental group runtime rejected projected-value pipeline",
                                ));
                            }
                            Ok(true)
                        },
                    )
                    .map_err(storage_engine_error)?;
                }

                if exceeded.get() {
                    return Ok(None);
                }
                let mut ordered_groups = groups.into_iter().collect::<Vec<_>>();
                ordered_groups.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
                let mut rows = Vec::with_capacity(ordered_groups.len());
                for (index, (_, accumulator)) in ordered_groups.into_iter().enumerate() {
                    let ordinal = usize_to_u64_saturating(index).saturating_add(1);
                    rows.push(ExecutionRow::synthetic(
                        accumulator
                            .finish(ordinal)
                            .map_err(EngineError::execution)?,
                    ));
                }
                return Ok(Some((
                    rows,
                    ExecutionStatistics::streamed_pipeline(
                        scanned,
                        filtered,
                        scanned.saturating_sub(filtered),
                        ExecutionStrategy::CollectionScan,
                    ),
                )));
            }
        }

        // Bounded GROUP has a dedicated exact fallback in the blocking executor.
        // Do not probe an unbounded document group first and rescan the source.
        if group_limit.is_some() {
            return Ok(None);
        }

        // Established fallback for nested fields, lookups and transformed prefixes.
        let mut groups: BTreeMap<Arc<[u8]>, Box<dyn crate::query::IncrementalGroupAccumulator>> =
            BTreeMap::new();
        let mut estimated_bytes = 0usize;
        let exceeded = std::cell::Cell::new(false);
        let mut process = |stored: StoredDocument| -> EngineResult<()> {
            if exceeded.get() {
                return Ok(());
            }
            let row = ExecutionRow::from_stored(stored);
            let key = self
                .runtime
                .distinct_key(&grouping_keys, row.document())
                .map_err(EngineError::execution)?;
            if !groups.contains_key(&key) {
                let state_estimate = group_state_estimate(key.len(), keys.len());
                if estimated_bytes.saturating_add(state_estimate) > budget {
                    exceeded.set(true);
                    groups.clear();
                    return Ok(());
                }
                let accumulator = self
                    .runtime
                    .incremental_group_accumulator(keys)
                    .map_err(EngineError::execution)?
                    .expect("incremental group capability was probed above");
                estimated_bytes = estimated_bytes.saturating_add(state_estimate);
                groups.insert(key.clone(), accumulator);
            }
            groups
                .get_mut(&key)
                .expect("group accumulator exists")
                .push(row.document())
                .map_err(EngineError::execution)
        };

        let statistics = self
            .stream_read_pipeline(prefix, &mut |stored| process(stored))?
            .expect("validated streaming prefix");
        if exceeded.get() {
            return Ok(None);
        }

        let mut rows = Vec::with_capacity(groups.len());
        for (index, (_, accumulator)) in groups.into_iter().enumerate() {
            let ordinal = usize_to_u64_saturating(index).saturating_add(1);
            rows.push(ExecutionRow::synthetic(
                accumulator
                    .finish(ordinal)
                    .map_err(EngineError::execution)?,
            ));
        }
        Ok(Some((rows, statistics)))
    }

    /// Spills standard incremental GROUP states directly from the shared projected-value
    /// scanner. This is the external counterpart of `try_in_memory_incremental_group`:
    /// compatible collection/filter prefixes never fall back to full-document pointer scans.
    fn try_projected_external_group_runs(
        &self,
        prefix: &PhysicalPlan,
        keys: &[crate::query::ExpressionFieldPath],
        budget: usize,
        spill: &SpillEngine,
    ) -> EngineResult<Option<(Vec<SpillRun>, ExecutionStatistics)>> {
        if self
            .runtime
            .incremental_group_accumulator(keys)
            .map_err(EngineError::execution)?
            .is_none()
        {
            return Ok(None);
        }
        let crate::query::PhysicalAccess::CollectionScan { options } = prefix.source().access()
        else {
            return Ok(None);
        };
        let (grouping_keys, required_input_fields) =
            crate::query::group_field_layout(keys).map_err(EngineError::execution)?;
        let Some(value_pipeline) =
            ProjectedValuePipeline::compile(prefix.operators(), required_input_fields.clone())
                .map_err(EngineError::execution)?
        else {
            return Ok(None);
        };
        let layout = value_pipeline.layout();
        let group_layout =
            ProjectedValueLayout::new(required_input_fields).map_err(EngineError::execution)?;
        let group_source_slots = layout
            .slots(group_layout.fields())
            .map_err(EngineError::execution)?;
        let key_indexes = group_layout
            .slots(&grouping_keys)
            .map_err(EngineError::execution)?;
        let read = self.storage.read().map_err(storage_engine_error)?;
        if !read
            .support(StorageReadCapability::ProjectedValuesGatedUnordered)
            .available()
        {
            return Ok(None);
        }

        let mut groups: HashMap<Arc<[u8]>, GroupAccumulator> = HashMap::new();
        let mut bytes = 0usize;
        let mut runs = Vec::new();
        let mut scanned = 0u64;
        let mut filtered = 0u64;
        let mut key_buffer = Vec::with_capacity(64);
        let materializer = crate::query::RuntimeMaterializer::new();
        let mut group_values = vec![None; group_source_slots.len()];
        let started = Instant::now();

        if value_pipeline.gate_field_count() == 0 {
            let storage_key_indexes = key_indexes
                .iter()
                .map(|index| group_source_slots[*index])
                .collect::<Vec<_>>();
            read.scan_projected_value_refs_unordered_each(
                prefix.source().collection(),
                *options,
                layout.storage_fields(),
                &mut |values| {
                    scanned = scanned.saturating_add(1);
                    materializer.write_projected_ref_distinct_key_indexes(
                        values,
                        &storage_key_indexes,
                        &mut key_buffer,
                    );
                    if let Some(accumulator) = groups.get_mut(key_buffer.as_slice()) {
                        if !accumulator
                            .push_projected_value_refs(values, &group_source_slots)
                            .map_err(backend_storage_error)?
                        {
                            return Err(StorageError::backend(
                                "incremental group runtime rejected borrowed projected values",
                            ));
                        }
                        return Ok(true);
                    }
                    let estimate = group_state_estimate(key_buffer.len(), keys.len());
                    if !groups.is_empty() && bytes.saturating_add(estimate) > budget {
                        EXTERNAL_GROUP_FLUSH_TRIGGERS.fetch_add(1, AtomicOrdering::Relaxed);
                        flush_partial_group_run(
                            self.runtime.as_ref(),
                            keys,
                            spill,
                            &mut groups,
                            &mut runs,
                        )
                        .map_err(backend_storage_error)?;
                        bytes = 0;
                    }
                    let mut accumulator = self
                        .runtime
                        .incremental_group_accumulator(keys)
                        .map_err(backend_storage_error)?
                        .expect("incremental group capability was probed above");
                    if !accumulator
                        .push_projected_value_refs(values, &group_source_slots)
                        .map_err(backend_storage_error)?
                    {
                        return Err(StorageError::backend(
                            "incremental group runtime rejected borrowed projected values",
                        ));
                    }
                    bytes = bytes.saturating_add(estimate);
                    groups.insert(Arc::from(key_buffer.as_slice()), accumulator);
                    Ok(true)
                },
            )
            .map_err(storage_engine_error)?;
        } else {
            read.scan_projected_values_gated_unordered_each(
                prefix.source().collection(),
                *options,
                layout.storage_fields(),
                value_pipeline.gate_field_count(),
                &mut |values| {
                    scanned = scanned.saturating_add(1);
                    let accepted = value_pipeline
                        .accepts_with(values, |expression, resolver| {
                            self.runtime
                                .evaluate_resolved_predicate(expression, resolver)
                        })
                        .map_err(backend_storage_error)?;
                    if !accepted {
                        filtered = filtered.saturating_add(1);
                    }
                    Ok(accepted)
                },
                &mut |values| {
                    for (target, source) in group_values
                        .iter_mut()
                        .zip(group_source_slots.iter().copied())
                    {
                        *target = values.get(source).cloned().flatten();
                    }
                    materializer.write_projected_distinct_key_indexes(
                        &group_values,
                        &key_indexes,
                        &mut key_buffer,
                    );
                    if !groups.contains_key(key_buffer.as_slice()) {
                        let estimate = group_state_estimate(key_buffer.len(), keys.len());
                        if !groups.is_empty() && bytes.saturating_add(estimate) > budget {
                            EXTERNAL_GROUP_FLUSH_TRIGGERS.fetch_add(1, AtomicOrdering::Relaxed);
                            flush_partial_group_run(
                                self.runtime.as_ref(),
                                keys,
                                spill,
                                &mut groups,
                                &mut runs,
                            )
                            .map_err(backend_storage_error)?;
                            bytes = 0;
                        }
                        let accumulator = self
                            .runtime
                            .incremental_group_accumulator(keys)
                            .map_err(backend_storage_error)?
                            .expect("incremental group capability was probed above");
                        bytes = bytes.saturating_add(estimate);
                        groups.insert(Arc::from(key_buffer.as_slice()), accumulator);
                    }
                    if !groups
                        .get_mut(key_buffer.as_slice())
                        .expect("group accumulator exists")
                        .push_projected_values(&group_values)
                        .map_err(backend_storage_error)?
                    {
                        return Err(StorageError::backend(
                            "incremental group runtime rejected projected values",
                        ));
                    }
                    Ok(true)
                },
            )
            .map_err(storage_engine_error)?;
        }
        if !groups.is_empty() {
            flush_partial_group_run(self.runtime.as_ref(), keys, spill, &mut groups, &mut runs)?;
        }
        EXTERNAL_GROUP_SOURCE_US.fetch_add(elapsed_micros(started), AtomicOrdering::Relaxed);
        EXTERNAL_GROUP_ROWS_CONSUMED
            .fetch_add(scanned.saturating_sub(filtered), AtomicOrdering::Relaxed);
        Ok(Some((
            runs,
            ExecutionStatistics::streamed_pipeline(
                scanned,
                filtered,
                scanned.saturating_sub(filtered),
                ExecutionStrategy::CollectionScan,
            ),
        )))
    }

    /// Streams a governed blocking read plan without materializing its full input or output.
    ///
    /// Executes supported governed blocking shapes after a fully streaming prefix.
    ///
    /// A downstream linear bound is consumed generically; concrete sort, distinct and group
    /// algorithms remain local to the executor. External runs keep blocking work inside the
    /// governed memory budget.
    pub fn stream_governed_blocking_pipeline(
        &self,
        physical: &PhysicalPlan,
        visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>,
    ) -> EngineResult<Option<ExecutionStatistics>> {
        if !self.supports_governed_blocking_streaming(physical) {
            return Ok(None);
        }

        let blocking_index = physical
            .operators()
            .iter()
            .position(|operator| {
                matches!(
                    operator.execution_properties().flow,
                    crate::query::Flow::GovernedBlocking
                )
            })
            .expect("governed support check guarantees a blocking stage");

        let suffix = &physical.operators()[blocking_index + 1..];
        let chained_group_sort = matches!(
            &physical.operators()[blocking_index],
            PhysicalOperator::Group { .. }
        ) && match suffix {
            [ordering] => matches!(ordering.execution_properties().order, Order::Ordered(_)),
            [ordering, bound] => {
                matches!(ordering.execution_properties().order, Order::Ordered(_))
                    && bound.execution_properties().linear_bound().is_some()
            }
            _ => false,
        };
        let output_limit = match suffix {
            [] => None,
            [bound] => bound.execution_properties().linear_bound(),
            _ if chained_group_sort => None,
            _ => return Ok(None),
        };

        let prefix = PhysicalPlan::new(
            physical.source().clone(),
            physical.operators()[..blocking_index].iter().cloned(),
        )
        .map_err(EngineError::physical_planning)?
        .with_source_access_negotiation(
            physical.source_access_vector(),
            physical.projected_prefix_len().min(blocking_index),
        );
        let source_strategy = match physical.source().access() {
            crate::query::PhysicalAccess::CollectionScan { .. } => {
                ExecutionStrategy::CollectionScan
            }
            crate::query::PhysicalAccess::PrimaryKeyLookup { .. } => {
                ExecutionStrategy::PrimaryKeyLookup
            }
        };

        // Blocking operators use the query profile and currently available
        // managed memory; spill follows that resource contract.
        let snapshot = self.memory_governor.snapshot();
        let query_budget = self.memory_governor.profile().query_budget_bytes;
        let available = snapshot.available_bytes.unwrap_or(query_budget);
        let working_budget = available.min(query_budget).max(1 * 1024 * 1024);

        // UNION and LOOKUP are bounded at the compound-stage boundary. UNION
        // streams both branches directly; LOOKUP only retains the matches for
        // the current outer row, which is the smallest semantic unit that must
        // exist in memory because the runtime attaches it as one result value.
        if suffix.is_empty() {
            match &physical.operators()[blocking_index] {
                PhysicalOperator::Union {
                    collection,
                    pipeline,
                    ..
                } if pipeline
                    .operators()
                    .iter()
                    .all(is_bounded_secondary_row_operator) =>
                {
                    let mut returned = 0u64;
                    let prefix_stats = self
                        .stream_read_pipeline(&prefix, &mut |stored| {
                            visitor(ExecutionRow::from_stored(stored))?;
                            returned = returned.saturating_add(1);
                            Ok(())
                        })?
                        .expect("validated streaming prefix");
                    let branch_stats = self.stream_bounded_secondary_pipeline(
                        collection,
                        pipeline,
                        None,
                        true,
                        &mut |row| {
                            visitor(row)?;
                            returned = returned.saturating_add(1);
                            Ok(())
                        },
                    )?;
                    let strategies = ExecutionStrategies::default().with(source_strategy);
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        prefix_stats
                            .scanned()
                            .saturating_add(branch_stats.scanned()),
                        prefix_stats
                            .filtered()
                            .saturating_add(branch_stats.filtered()),
                        returned,
                        strategies,
                    )));
                }
                PhysicalOperator::Lookup {
                    collection,
                    alias,
                    into,
                    pipeline,
                } if pipeline
                    .operators()
                    .iter()
                    .all(is_bounded_secondary_row_operator) =>
                {
                    let reservation = reserve_query_memory(
                        &self.memory_governor,
                        "bounded lookup working set",
                        working_budget,
                    )
                    .map_err(EngineError::execution)?;
                    let mut inner_scanned = 0u64;
                    let mut inner_filtered = 0u64;
                    let mut returned = 0u64;
                    let prefix_stats = self
                        .stream_read_pipeline(&prefix, &mut |stored| {
                            let mut outer = ExecutionRow::from_stored(stored);
                            let mut matches = Vec::<Arc<Document>>::new();
                            let mut retained_bytes = 0usize;
                            let inner_stats = self.stream_bounded_secondary_pipeline(
                                collection,
                                pipeline,
                                Some((outer.document(), alias.as_deref())),
                                false,
                                &mut |row| {
                                    let estimated = execution_row_working_bytes(&row)
                                        .map_err(EngineError::execution)?;
                                    if estimated > working_budget
                                        || retained_bytes.saturating_add(estimated) > working_budget
                                    {
                                        return Err(EngineError::execution(
                                            ExecutionError::evaluation(
                                                "one lookup result exceeds the governed query working set",
                                            ),
                                        ));
                                    }
                                    retained_bytes = retained_bytes.saturating_add(estimated);
                                    matches.push(row.shared_document());
                                    Ok(())
                                },
                            )?;
                            inner_scanned = inner_scanned.saturating_add(inner_stats.scanned());
                            inner_filtered = inner_filtered.saturating_add(inner_stats.filtered());
                            let documents = LookupDocuments::new(matches);
                            let evaluation = outer.evaluation_document();
                            let document = self
                                .runtime
                                .apply_lookup(into, &evaluation, &documents)
                                .map_err(EngineError::execution)?;
                            outer.replace_document(document, false);
                            visitor(outer)?;
                            returned = returned.saturating_add(1);
                            Ok(())
                        })?
                        .expect("validated streaming prefix");
                    drop(reservation);
                    let strategies = ExecutionStrategies::default().with(source_strategy);
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        prefix_stats.scanned().saturating_add(inner_scanned),
                        prefix_stats.filtered().saturating_add(inner_filtered),
                        returned,
                        strategies,
                    )));
                }
                _ => {}
            }
        }

        if let [PhysicalOperator::Sort { keys: sort_keys }, PhysicalOperator::Distinct {
            fields: distinct_fields,
        }, bound] = &physical.operators()[blocking_index..]
        {
            let Some(limit) = bound.execution_properties().linear_bound() else {
                return Ok(None);
            };
            let reservation = reserve_query_memory(
                &self.memory_governor,
                "bounded sort-distinct-limit working set",
                working_budget,
            )
            .map_err(EngineError::execution)?;
            let spill = SpillEngine::default();
            let sort_input_budget = bounded_sort_input_budget(working_budget);
            let mut chunk = Vec::<ExecutionRow>::new();
            let mut chunk_bytes = 0usize;
            let mut runs = Vec::<SpillRun>::new();
            let prefix_stats = self
                .stream_read_pipeline(&prefix, &mut |stored| {
                    let row = ExecutionRow::from_stored(stored);
                    push_bounded_sort_row(
                        self.runtime.as_ref(),
                        sort_keys,
                        &spill,
                        row,
                        sort_input_budget,
                        &mut chunk,
                        &mut chunk_bytes,
                        &mut runs,
                        "one sort-distinct row exceeds the governed query working set",
                    )
                })?
                .expect("validated streaming prefix");

            let returned = if runs.is_empty() {
                stable_sort(self.runtime.as_ref(), sort_keys, &mut chunk)
                    .map_err(EngineError::execution)?;
                emit_distinct_limited_rows(
                    self.runtime.as_ref(),
                    distinct_fields,
                    limit,
                    chunk.into_iter(),
                    visitor,
                )?
            } else {
                if !chunk.is_empty() {
                    flush_sorted_run(
                        self.runtime.as_ref(),
                        sort_keys,
                        &spill,
                        &mut chunk,
                        &mut runs,
                    )
                    .map_err(EngineError::execution)?;
                }
                merge_sorted_distinct_limited_runs(
                    self.runtime.as_ref(),
                    sort_keys,
                    distinct_fields,
                    &runs,
                    limit,
                    visitor,
                )?
            };
            drop(reservation);
            let strategies = ExecutionStrategies::default()
                .with(source_strategy)
                .with(if runs.is_empty() {
                    ExecutionStrategy::InMemorySort
                } else {
                    ExecutionStrategy::ExternalSort
                })
                .with(ExecutionStrategy::ExternalDistinct);
            return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                prefix_stats.scanned(),
                prefix_stats
                    .filtered()
                    .saturating_add(prefix_stats.returned().saturating_sub(returned)),
                returned,
                strategies,
            )));
        }

        if let [PhysicalOperator::Group { keys: group_keys }, PhysicalOperator::Sort { keys: sort_keys }, tail @ ..] =
            &physical.operators()[blocking_index..]
        {
            let output_limit = match tail {
                [] => None,
                [bound] => bound.execution_properties().linear_bound(),
                _ => return Ok(None),
            };

            // Reuse the same governed in-memory group primitive used by GROUP
            // alone before choosing the external path. Chaining SORT/LIMIT must
            // not force spill when the grouped state already fits the contract.
            if let Some((grouped_rows, prefix_stats)) =
                self.try_in_memory_incremental_group(&prefix, group_keys, working_budget, None)?
            {
                if let Some(limit) = output_limit {
                    let mut top = BoundedTopN::new(limit);
                    for row in grouped_rows {
                        top.push_lazy(self.runtime.as_ref(), sort_keys, row, |candidate| {
                            execution_row_working_bytes(candidate)
                        })
                        .map_err(EngineError::execution)?;
                    }
                    let top = top
                        .into_sorted_rows(self.runtime.as_ref(), sort_keys)
                        .map_err(EngineError::execution)?;
                    let returned = top.len() as u64;
                    for row in top {
                        visitor(row)?;
                    }
                    let strategies = ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(ExecutionStrategy::InMemoryGroup)
                        .with(ExecutionStrategy::TopN);
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        prefix_stats.scanned(),
                        prefix_stats.filtered(),
                        returned,
                        strategies,
                    )));
                } else {
                    let mut grouped_rows = grouped_rows;
                    stable_sort(self.runtime.as_ref(), sort_keys, &mut grouped_rows)
                        .map_err(EngineError::execution)?;
                    let returned = grouped_rows.len() as u64;
                    for row in grouped_rows {
                        visitor(row)?;
                    }
                    let strategies = ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(ExecutionStrategy::InMemoryGroup)
                        .with(ExecutionStrategy::InMemorySort);
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        prefix_stats.scanned(),
                        prefix_stats.filtered(),
                        returned,
                        strategies,
                    )));
                }
            }

            let reservation = reserve_query_memory(
                &self.memory_governor,
                "external group-sort working set",
                working_budget,
            )
            .map_err(EngineError::execution)?;
            let stage_budget = working_budget.saturating_div(2).max(512 * 1024);

            if let Some((mut grouped, prefix_stats)) =
                self.try_in_memory_incremental_group(&prefix, group_keys, stage_budget, None)?
            {
                let sort_strategy = if let Some(limit) = output_limit {
                    stable_sort(self.runtime.as_ref(), sort_keys, &mut grouped)
                        .map_err(EngineError::execution)?;
                    grouped.truncate(limit);
                    ExecutionStrategy::TopN
                } else {
                    stable_sort(self.runtime.as_ref(), sort_keys, &mut grouped)
                        .map_err(EngineError::execution)?;
                    ExecutionStrategy::InMemorySort
                };
                let returned = grouped.len() as u64;
                for row in grouped {
                    visitor(row)?;
                }
                drop(reservation);
                let strategies = ExecutionStrategies::default()
                    .with(source_strategy)
                    .with(ExecutionStrategy::InMemoryGroup)
                    .with(sort_strategy);
                return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                    prefix_stats.scanned(),
                    prefix_stats.filtered(),
                    returned,
                    strategies,
                )));
            }

            let spill = SpillEngine::default();
            // Keep compatible GROUP pipelines on the physical projected scanner even
            // after the in-memory state crosses its budget. The old fallback rebuilt
            // full documents through `scan_each`, turning one analytical scan into a
            // second 16M-row pointer walk.
            if let Some((group_runs, prefix_stats)) =
                self.try_projected_external_group_runs(&prefix, group_keys, stage_budget, &spill)?
            {
                let (returned, sort_strategy) = if let Some(limit) = output_limit {
                    let mut top = BoundedTopN::new(limit);
                    merge_group_runs(
                        self.runtime.as_ref(),
                        group_keys,
                        stage_budget,
                        &group_runs,
                        None,
                        &mut |row| {
                            top.push_lazy(self.runtime.as_ref(), sort_keys, row, |candidate| {
                                execution_row_working_bytes(candidate)
                            })
                            .map_err(EngineError::execution)?;
                            if top.live_bytes() > stage_budget {
                                return Err(EngineError::execution(ExecutionError::evaluation(
                                    "group-sort Top-N candidates exceed the governed working set",
                                )));
                            }
                            Ok(())
                        },
                    )?;
                    let top = top
                        .into_sorted_rows(self.runtime.as_ref(), sort_keys)
                        .map_err(EngineError::execution)?;
                    let returned = top.len() as u64;
                    for row in top {
                        visitor(row)?;
                    }
                    (returned, ExecutionStrategy::TopN)
                } else {
                    let mut sort_chunk = Vec::<ExecutionRow>::new();
                    let mut sort_chunk_bytes = 0usize;
                    let mut sort_runs = Vec::<SpillRun>::new();
                    merge_group_runs(
                        self.runtime.as_ref(),
                        group_keys,
                        stage_budget,
                        &group_runs,
                        None,
                        &mut |row| {
                            push_bounded_sort_row(
                                self.runtime.as_ref(),
                                sort_keys,
                                &spill,
                                row,
                                stage_budget,
                                &mut sort_chunk,
                                &mut sort_chunk_bytes,
                                &mut sort_runs,
                                "one grouped row exceeds the governed sort working set",
                            )
                        },
                    )?;
                    if sort_runs.is_empty() {
                        stable_sort(self.runtime.as_ref(), sort_keys, &mut sort_chunk)
                            .map_err(EngineError::execution)?;
                        let returned = sort_chunk.len() as u64;
                        for row in sort_chunk {
                            visitor(row)?;
                        }
                        (returned, ExecutionStrategy::InMemorySort)
                    } else {
                        if !sort_chunk.is_empty() {
                            flush_sorted_run(
                                self.runtime.as_ref(),
                                sort_keys,
                                &spill,
                                &mut sort_chunk,
                                &mut sort_runs,
                            )
                            .map_err(EngineError::execution)?;
                        }
                        let returned = merge_sorted_runs(
                            self.runtime.as_ref(),
                            sort_keys,
                            &sort_runs,
                            None,
                            visitor,
                        )?;
                        (returned, ExecutionStrategy::ExternalSort)
                    }
                };
                drop(reservation);
                let strategies = ExecutionStrategies::default()
                    .with(source_strategy)
                    .with(ExecutionStrategy::ExternalGroup)
                    .with(sort_strategy);
                return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                    prefix_stats.scanned(),
                    prefix_stats.filtered(),
                    returned,
                    strategies,
                )));
            }
            // External group is aggregate-first: keep bounded mergeable states
            // in RAM and spill compact partials only when required.
            let mut partial_groups: HashMap<
                Arc<[u8]>,
                Box<dyn crate::query::IncrementalGroupAccumulator>,
            > = HashMap::new();
            let mut partial_bytes = 0usize;
            let mut group_runs = Vec::<SpillRun>::new();
            let (grouping_keys, _) =
                crate::query::group_field_layout(group_keys).map_err(EngineError::execution)?;
            let mut ext_rows = 0u64;
            let mut ext_samples = 0u64;
            let mut ext_key_ns = 0u64;
            let mut ext_lookup_ns = 0u64;
            let mut ext_accumulate_ns = 0u64;
            let mut ext_hits = 0u64;
            let mut ext_misses = 0u64;
            let source_started = Instant::now();
            let prefix_stats = self
                .stream_read_pipeline(&prefix, &mut |stored| {
                    ext_rows = ext_rows.saturating_add(1);
                    let sampled =
                        crate::debug::query_instrumentation_enabled() && ext_rows & 1023 == 0;
                    if sampled {
                        ext_samples = ext_samples.saturating_add(1);
                    }
                    if ext_rows & 65535 == 0 {
                        sample_external_group_process_memory(&self.memory_governor);
                    }
                    let row = ExecutionRow::from_stored(stored);
                    let key_started = sampled.then(Instant::now);
                    let key = self
                        .runtime
                        .distinct_key(&grouping_keys, row.document())
                        .map_err(EngineError::execution)?;
                    if let Some(started) = key_started {
                        ext_key_ns = ext_key_ns.saturating_add(elapsed_nanos(started));
                    }
                    let lookup_started = sampled.then(Instant::now);
                    let exists = partial_groups.contains_key(key.as_ref());
                    if let Some(started) = lookup_started {
                        ext_lookup_ns = ext_lookup_ns.saturating_add(elapsed_nanos(started));
                    }
                    if !exists {
                        ext_misses = ext_misses.saturating_add(1);
                        let estimate = group_state_estimate(key.len(), group_keys.len());
                        if !partial_groups.is_empty()
                            && partial_bytes.saturating_add(estimate) > stage_budget
                        {
                            EXTERNAL_GROUP_FLUSH_TRIGGERS.fetch_add(1, AtomicOrdering::Relaxed);
                            flush_partial_group_run(
                                self.runtime.as_ref(),
                                group_keys,
                                &spill,
                                &mut partial_groups,
                                &mut group_runs,
                            )?;
                            partial_bytes = 0;
                            sample_external_group_process_memory(&self.memory_governor);
                        }
                        let create_started = Instant::now();
                        let accumulator = self
                            .runtime
                            .incremental_group_accumulator(group_keys)
                            .map_err(EngineError::execution)?
                            .ok_or_else(|| {
                                EngineError::execution(ExecutionError::evaluation(
                                    "external group runtime has no incremental accumulator",
                                ))
                            })?;
                        EXTERNAL_GROUP_ACCUMULATOR_CREATE_US
                            .fetch_add(elapsed_micros(create_started), AtomicOrdering::Relaxed);
                        partial_bytes = partial_bytes.saturating_add(estimate);
                        update_atomic_peak(
                            &EXTERNAL_GROUP_ESTIMATED_PEAK_BYTES,
                            usize_to_u64_saturating(partial_bytes),
                        );
                        partial_groups.insert(key.clone(), accumulator);
                    } else {
                        ext_hits = ext_hits.saturating_add(1);
                    }
                    let accumulate_started = sampled.then(Instant::now);
                    let result = partial_groups
                        .get_mut(key.as_ref())
                        .expect("partial group accumulator exists")
                        .push(row.document())
                        .map_err(EngineError::execution);
                    if let Some(started) = accumulate_started {
                        ext_accumulate_ns =
                            ext_accumulate_ns.saturating_add(elapsed_nanos(started));
                    }
                    result
                })?
                .expect("validated streaming prefix");
            EXTERNAL_GROUP_SOURCE_US
                .fetch_add(elapsed_micros(source_started), AtomicOrdering::Relaxed);
            EXTERNAL_GROUP_ROWS_CONSUMED.fetch_add(ext_rows, AtomicOrdering::Relaxed);
            EXTERNAL_GROUP_CONSUME_SAMPLES.fetch_add(ext_samples, AtomicOrdering::Relaxed);
            EXTERNAL_GROUP_KEY_NS.fetch_add(ext_key_ns, AtomicOrdering::Relaxed);
            EXTERNAL_GROUP_LOOKUP_NS.fetch_add(ext_lookup_ns, AtomicOrdering::Relaxed);
            EXTERNAL_GROUP_ACCUMULATE_NS.fetch_add(ext_accumulate_ns, AtomicOrdering::Relaxed);
            EXTERNAL_GROUP_GROUP_HITS.fetch_add(ext_hits, AtomicOrdering::Relaxed);
            EXTERNAL_GROUP_GROUP_MISSES.fetch_add(ext_misses, AtomicOrdering::Relaxed);
            if !partial_groups.is_empty() {
                flush_partial_group_run(
                    self.runtime.as_ref(),
                    group_keys,
                    &spill,
                    &mut partial_groups,
                    &mut group_runs,
                )?;
            }

            let returned: u64;
            let sort_strategy;
            if let Some(limit) = output_limit {
                let mut top = BoundedTopN::new(limit);
                merge_group_runs(
                    self.runtime.as_ref(),
                    group_keys,
                    stage_budget,
                    &group_runs,
                    None,
                    &mut |row| {
                        top.push_lazy(self.runtime.as_ref(), sort_keys, row, |candidate| {
                            execution_row_working_bytes(candidate)
                        })
                        .map_err(EngineError::execution)?;
                        if top.live_bytes() > stage_budget {
                            return Err(EngineError::execution(ExecutionError::evaluation(
                                "group-sort Top-N candidates exceed the governed working set",
                            )));
                        }
                        Ok(())
                    },
                )?;
                let top = top
                    .into_sorted_rows(self.runtime.as_ref(), sort_keys)
                    .map_err(EngineError::execution)?;
                returned = top.len() as u64;
                for row in top {
                    visitor(row)?;
                }
                sort_strategy = ExecutionStrategy::TopN;
            } else {
                let mut sort_chunk = Vec::<ExecutionRow>::new();
                let mut sort_chunk_bytes = 0usize;
                let mut sort_runs = Vec::<SpillRun>::new();
                merge_group_runs(
                    self.runtime.as_ref(),
                    group_keys,
                    stage_budget,
                    &group_runs,
                    None,
                    &mut |row| {
                        push_bounded_sort_row(
                            self.runtime.as_ref(),
                            sort_keys,
                            &spill,
                            row,
                            stage_budget,
                            &mut sort_chunk,
                            &mut sort_chunk_bytes,
                            &mut sort_runs,
                            "one grouped row exceeds the governed sort working set",
                        )
                    },
                )?;
                if sort_runs.is_empty() {
                    stable_sort(self.runtime.as_ref(), sort_keys, &mut sort_chunk)
                        .map_err(EngineError::execution)?;
                    returned = sort_chunk.len() as u64;
                    for row in sort_chunk {
                        visitor(row)?;
                    }
                    sort_strategy = ExecutionStrategy::InMemorySort;
                } else {
                    if !sort_chunk.is_empty() {
                        flush_sorted_run(
                            self.runtime.as_ref(),
                            sort_keys,
                            &spill,
                            &mut sort_chunk,
                            &mut sort_runs,
                        )
                        .map_err(EngineError::execution)?;
                    }
                    returned = merge_sorted_runs(
                        self.runtime.as_ref(),
                        sort_keys,
                        &sort_runs,
                        None,
                        visitor,
                    )?;
                    sort_strategy = ExecutionStrategy::ExternalSort;
                }
            }
            drop(reservation);
            let strategies = ExecutionStrategies::default()
                .with(source_strategy)
                .with(ExecutionStrategy::ExternalGroup)
                .with(sort_strategy);
            return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                prefix_stats.scanned(),
                prefix_stats.filtered(),
                returned,
                strategies,
            )));
        }

        match &physical.operators()[blocking_index] {
            PhysicalOperator::Sort { keys } if output_limit.is_some() => {
                let limit = output_limit.expect("checked Top-N limit");
                if let Some((rows, statistics)) = self.try_projected_top_n(&prefix, keys, limit)? {
                    for row in rows {
                        visitor(row)?;
                    }
                    return Ok(Some(statistics));
                }
                let reservation =
                    reserve_query_memory(&self.memory_governor, "streaming top-n", working_budget)
                        .map_err(EngineError::execution)?;
                let mut top = BoundedTopN::new(limit);
                let prefix_stats = self
                    .stream_read_pipeline(&prefix, &mut |stored| {
                        let row = ExecutionRow::from_stored(stored);
                        top.push_lazy(self.runtime.as_ref(), keys, row, |candidate| {
                            execution_row_working_bytes(candidate)
                        })
                        .map_err(EngineError::execution)?;
                        if top.live_bytes() > working_budget {
                            return Err(EngineError::execution(ExecutionError::evaluation(
                                "top-n candidates exceed the governed query working set",
                            )));
                        }
                        Ok(())
                    })?
                    .expect("validated streaming prefix");
                drop(reservation);

                let top = top
                    .into_sorted_rows(self.runtime.as_ref(), keys)
                    .map_err(EngineError::execution)?;
                let returned = top.len() as u64;
                for row in top {
                    visitor(row)?;
                }
                let strategies = ExecutionStrategies::default()
                    .with(source_strategy)
                    .with(ExecutionStrategy::TopN);
                return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                    prefix_stats.scanned(),
                    prefix_stats
                        .filtered()
                        .saturating_add(prefix_stats.returned().saturating_sub(returned)),
                    returned,
                    strategies,
                )));
            }
            PhysicalOperator::Sort { keys } => {
                let reservation = reserve_query_memory(
                    &self.memory_governor,
                    "external sort working set",
                    working_budget,
                )
                .map_err(EngineError::execution)?;
                let spill = SpillEngine::default();
                let sort_input_budget = bounded_sort_input_budget(working_budget);
                let late_materialize = prefix
                    .operators()
                    .iter()
                    .all(|operator| matches!(operator, PhysicalOperator::Filter { .. }));
                let sort_fields = keys
                    .iter()
                    .map(|key| key.field().clone())
                    .collect::<Vec<_>>();
                let mut chunk = Vec::<ExecutionRow>::new();
                let mut chunk_bytes = 0usize;
                let mut runs = Vec::<SpillRun>::new();
                let prefix_stats = self
                    .stream_read_pipeline(&prefix, &mut |stored| {
                        let mut row = ExecutionRow::from_stored(stored);
                        if late_materialize {
                            let projected = self
                                .runtime
                                .apply_select(&sort_fields, row.document())
                                .map_err(EngineError::execution)?;
                            row.replace_document(projected, false);
                        }
                        push_bounded_sort_row(
                            self.runtime.as_ref(),
                            keys,
                            &spill,
                            row,
                            sort_input_budget,
                            &mut chunk,
                            &mut chunk_bytes,
                            &mut runs,
                            "one sort row exceeds the governed query working set",
                        )
                    })?
                    .expect("validated streaming prefix");

                let read = late_materialize
                    .then(|| self.storage.read())
                    .transpose()
                    .map_err(storage_engine_error)?;
                let collection = prefix.source().collection();
                let mut emit = |row: ExecutionRow| -> EngineResult<()> {
                    if let Some(read) = read.as_ref() {
                        let Some(stored) = read
                            .get(collection, row.id())
                            .map_err(storage_engine_error)?
                        else {
                            return Ok(());
                        };
                        if stored.version() != row.version() {
                            return Ok(());
                        }
                        visitor(ExecutionRow::from_stored(stored))
                    } else {
                        visitor(row)
                    }
                };

                let mut returned = 0u64;
                if runs.is_empty() {
                    stable_sort(self.runtime.as_ref(), keys, &mut chunk)
                        .map_err(EngineError::execution)?;
                    for row in chunk {
                        emit(row)?;
                        returned = returned.saturating_add(1);
                    }
                } else {
                    if !chunk.is_empty() {
                        flush_sorted_run(
                            self.runtime.as_ref(),
                            keys,
                            &spill,
                            &mut chunk,
                            &mut runs,
                        )
                        .map_err(EngineError::execution)?;
                    }
                    returned =
                        merge_sorted_runs(self.runtime.as_ref(), keys, &runs, None, &mut emit)?;
                }
                drop(reservation);
                let strategies =
                    ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(if runs.is_empty() {
                            ExecutionStrategy::InMemorySort
                        } else {
                            ExecutionStrategy::ExternalSort
                        });
                return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                    prefix_stats.scanned(),
                    prefix_stats.filtered(),
                    returned,
                    strategies,
                )));
            }
            PhysicalOperator::Distinct { fields } => {
                let reservation = reserve_query_memory(
                    &self.memory_governor,
                    "adaptive distinct working set",
                    working_budget,
                )
                .map_err(EngineError::execution)?;
                if let Some((rows, prefix_stats)) =
                    self.try_in_memory_distinct(&prefix, fields, working_budget)?
                {
                    let mut returned = 0u64;
                    for row in rows {
                        if output_limit.is_some_and(|limit| (returned as usize) >= limit) {
                            break;
                        }
                        visitor(row)?;
                        returned = returned.saturating_add(1);
                    }
                    drop(reservation);
                    let strategies = ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(ExecutionStrategy::InMemoryDistinct);
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        prefix_stats.scanned(),
                        prefix_stats.filtered(),
                        returned,
                        strategies,
                    )));
                }
                let spill = SpillEngine::default();
                let mut chunk = Vec::<KeyedRow>::new();
                let mut chunk_bytes = 0usize;
                let mut runs = Vec::<SpillRun>::new();
                let prefix_stats = self
                    .stream_read_pipeline(&prefix, &mut |stored| {
                        let row = ExecutionRow::from_stored(stored);
                        let key = self
                            .runtime
                            .distinct_key(fields, row.document())
                            .map_err(EngineError::execution)?;
                        let estimated = key
                            .len()
                            .saturating_add(
                                encode_execution_row(&row)
                                    .map_err(EngineError::execution)?
                                    .len(),
                            )
                            .saturating_add(72);
                        if estimated > working_budget {
                            return Err(EngineError::execution(ExecutionError::evaluation(
                                "one distinct row exceeds the governed query working set",
                            )));
                        }
                        if !chunk.is_empty()
                            && chunk_bytes.saturating_add(estimated) > working_budget
                        {
                            flush_keyed_run(&spill, &mut chunk, &mut runs)?;
                            chunk_bytes = 0;
                        }
                        chunk_bytes = chunk_bytes.saturating_add(estimated);
                        chunk.push(KeyedRow { key, row });
                        Ok(())
                    })?
                    .expect("validated streaming prefix");
                if !chunk.is_empty() {
                    flush_keyed_run(&spill, &mut chunk, &mut runs)?;
                }
                let returned = merge_distinct_runs(&runs, output_limit, visitor)?;
                drop(reservation);
                let strategies = ExecutionStrategies::default()
                    .with(source_strategy)
                    .with(ExecutionStrategy::ExternalDistinct);
                return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                    prefix_stats.scanned(),
                    prefix_stats
                        .filtered()
                        .saturating_add(prefix_stats.returned().saturating_sub(returned)),
                    returned,
                    strategies,
                )));
            }
            PhysicalOperator::Group { keys } => {
                let reservation = reserve_query_memory(
                    &self.memory_governor,
                    "external group working set",
                    working_budget,
                )
                .map_err(EngineError::execution)?;
                if output_limit == Some(0) {
                    drop(reservation);
                    let strategies = ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(ExecutionStrategy::InMemoryGroup);
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        0, 0, 0, strategies,
                    )));
                }
                if let Some((grouped, prefix_stats)) = self.try_in_memory_incremental_group(
                    &prefix,
                    keys,
                    working_budget,
                    output_limit,
                )? {
                    let mut returned = 0u64;
                    for row in grouped {
                        if output_limit.is_some_and(|limit| (returned as usize) >= limit) {
                            break;
                        }
                        visitor(row)?;
                        returned = returned.saturating_add(1);
                    }
                    drop(reservation);
                    let strategies = ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(ExecutionStrategy::InMemoryGroup);
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        prefix_stats.scanned(),
                        prefix_stats.filtered(),
                        returned,
                        strategies,
                    )));
                }

                // A terminal `group | limit N` has a stronger boundedness guarantee
                // than generic external grouping: group results are emitted in encoded
                // key order, so only the N smallest group keys can ever be observable.
                // Keep those groups exact in memory and discard keys above the moving
                // Nth-key frontier. The frontier only moves downward, therefore a key
                // once discarded can never become observable later. This preserves the
                // language semantics while preventing high-cardinality dimensions from
                // generating unbounded spill volume merely to throw almost everything
                // away at `limit`.
                if let Some(limit) = output_limit.filter(|limit| *limit > 0) {
                    let (grouping_keys, _) =
                        crate::query::group_field_layout(keys).map_err(EngineError::execution)?;
                    let mut groups: BTreeMap<
                        Arc<[u8]>,
                        (Box<dyn crate::query::IncrementalGroupAccumulator>, usize),
                    > = BTreeMap::new();
                    let mut estimated_bytes = 0usize;
                    let prefix_stats = self
                        .stream_read_pipeline(&prefix, &mut |stored| {
                            let row = ExecutionRow::from_stored(stored);
                            let key = self
                                .runtime
                                .distinct_key(&grouping_keys, row.document())
                                .map_err(EngineError::execution)?;

                            if let Some((accumulator, _)) = groups.get_mut(key.as_ref()) {
                                return accumulator
                                    .push(row.document())
                                    .map_err(EngineError::execution);
                            }

                            if groups.len() >= limit {
                                let keep = groups
                                    .last_key_value()
                                    .is_some_and(|(largest, _)| key.as_ref() < largest.as_ref());
                                if !keep {
                                    return Ok(());
                                }
                                if let Some((_, (_, bytes))) = groups.pop_last() {
                                    estimated_bytes = estimated_bytes.saturating_sub(bytes);
                                }
                            }

                            let estimate = group_state_estimate(key.len(), keys.len());
                            if estimated_bytes.saturating_add(estimate) > working_budget {
                                return Err(EngineError::execution(ExecutionError::evaluation(
                                    "bounded group limit exceeds the governed working set",
                                )));
                            }
                            let mut accumulator = self
                                .runtime
                                .incremental_group_accumulator(keys)
                                .map_err(EngineError::execution)?
                                .ok_or_else(|| {
                                    EngineError::execution(ExecutionError::evaluation(
                                        "group runtime has no incremental accumulator",
                                    ))
                                })?;
                            accumulator
                                .push(row.document())
                                .map_err(EngineError::execution)?;
                            estimated_bytes = estimated_bytes.saturating_add(estimate);
                            groups.insert(key, (accumulator, estimate));
                            Ok(())
                        })?
                        .expect("validated streaming prefix");

                    let mut returned = 0u64;
                    for (_, (accumulator, _)) in groups {
                        let ordinal = returned.saturating_add(1);
                        visitor(ExecutionRow::synthetic(
                            accumulator
                                .finish(ordinal)
                                .map_err(EngineError::execution)?,
                        ))?;
                        returned = returned.saturating_add(1);
                    }
                    drop(reservation);
                    let strategies = ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(ExecutionStrategy::InMemoryGroup);
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        prefix_stats.scanned(),
                        prefix_stats.filtered(),
                        returned,
                        strategies,
                    )));
                }

                let spill = SpillEngine::default();
                if let Some((runs, prefix_stats)) =
                    self.try_projected_external_group_runs(&prefix, keys, working_budget, &spill)?
                {
                    let returned = merge_group_runs(
                        self.runtime.as_ref(),
                        keys,
                        working_budget,
                        &runs,
                        None,
                        visitor,
                    )?;
                    drop(reservation);
                    let strategies = ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(ExecutionStrategy::ExternalGroup);
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        prefix_stats.scanned(),
                        prefix_stats.filtered(),
                        returned,
                        strategies,
                    )));
                }
                let mut partial_groups: HashMap<
                    Arc<[u8]>,
                    Box<dyn crate::query::IncrementalGroupAccumulator>,
                > = HashMap::new();
                let mut partial_bytes = 0usize;
                let mut runs = Vec::<SpillRun>::new();
                let (grouping_keys, _) =
                    crate::query::group_field_layout(keys).map_err(EngineError::execution)?;
                let mut ext_rows = 0u64;
                let mut ext_samples = 0u64;
                let mut ext_key_ns = 0u64;
                let mut ext_lookup_ns = 0u64;
                let mut ext_accumulate_ns = 0u64;
                let mut ext_hits = 0u64;
                let mut ext_misses = 0u64;
                let source_started = Instant::now();
                let prefix_stats = self
                    .stream_read_pipeline(&prefix, &mut |stored| {
                        ext_rows = ext_rows.saturating_add(1);
                        let sampled =
                            crate::debug::query_instrumentation_enabled() && ext_rows & 1023 == 0;
                        if sampled {
                            ext_samples = ext_samples.saturating_add(1);
                        }
                        if ext_rows & 65535 == 0 {
                            sample_external_group_process_memory(&self.memory_governor);
                        }
                        let row = ExecutionRow::from_stored(stored);
                        let key_started = sampled.then(Instant::now);
                        let key = self
                            .runtime
                            .distinct_key(&grouping_keys, row.document())
                            .map_err(EngineError::execution)?;
                        if let Some(started) = key_started {
                            ext_key_ns = ext_key_ns.saturating_add(elapsed_nanos(started));
                        }
                        let lookup_started = sampled.then(Instant::now);
                        let exists = partial_groups.contains_key(key.as_ref());
                        if let Some(started) = lookup_started {
                            ext_lookup_ns = ext_lookup_ns.saturating_add(elapsed_nanos(started));
                        }
                        if !exists {
                            ext_misses = ext_misses.saturating_add(1);
                            let estimate = group_state_estimate(key.len(), keys.len());
                            if !partial_groups.is_empty()
                                && partial_bytes.saturating_add(estimate) > working_budget
                            {
                                EXTERNAL_GROUP_FLUSH_TRIGGERS.fetch_add(1, AtomicOrdering::Relaxed);
                                flush_partial_group_run(
                                    self.runtime.as_ref(),
                                    keys,
                                    &spill,
                                    &mut partial_groups,
                                    &mut runs,
                                )?;
                                partial_bytes = 0;
                                sample_external_group_process_memory(&self.memory_governor);
                            }
                            let create_started = Instant::now();
                            let accumulator = self
                                .runtime
                                .incremental_group_accumulator(keys)
                                .map_err(EngineError::execution)?
                                .ok_or_else(|| {
                                    EngineError::execution(ExecutionError::evaluation(
                                        "external group runtime has no incremental accumulator",
                                    ))
                                })?;
                            EXTERNAL_GROUP_ACCUMULATOR_CREATE_US
                                .fetch_add(elapsed_micros(create_started), AtomicOrdering::Relaxed);
                            partial_bytes = partial_bytes.saturating_add(estimate);
                            update_atomic_peak(
                                &EXTERNAL_GROUP_ESTIMATED_PEAK_BYTES,
                                usize_to_u64_saturating(partial_bytes),
                            );
                            partial_groups.insert(key.clone(), accumulator);
                        } else {
                            ext_hits = ext_hits.saturating_add(1);
                        }
                        let accumulate_started = sampled.then(Instant::now);
                        let result = partial_groups
                            .get_mut(key.as_ref())
                            .expect("partial group accumulator exists")
                            .push(row.document())
                            .map_err(EngineError::execution);
                        if let Some(started) = accumulate_started {
                            ext_accumulate_ns =
                                ext_accumulate_ns.saturating_add(elapsed_nanos(started));
                        }
                        result
                    })?
                    .expect("validated streaming prefix");
                EXTERNAL_GROUP_SOURCE_US
                    .fetch_add(elapsed_micros(source_started), AtomicOrdering::Relaxed);
                EXTERNAL_GROUP_ROWS_CONSUMED.fetch_add(ext_rows, AtomicOrdering::Relaxed);
                EXTERNAL_GROUP_CONSUME_SAMPLES.fetch_add(ext_samples, AtomicOrdering::Relaxed);
                EXTERNAL_GROUP_KEY_NS.fetch_add(ext_key_ns, AtomicOrdering::Relaxed);
                EXTERNAL_GROUP_LOOKUP_NS.fetch_add(ext_lookup_ns, AtomicOrdering::Relaxed);
                EXTERNAL_GROUP_ACCUMULATE_NS.fetch_add(ext_accumulate_ns, AtomicOrdering::Relaxed);
                EXTERNAL_GROUP_GROUP_HITS.fetch_add(ext_hits, AtomicOrdering::Relaxed);
                EXTERNAL_GROUP_GROUP_MISSES.fetch_add(ext_misses, AtomicOrdering::Relaxed);
                if !partial_groups.is_empty() {
                    flush_partial_group_run(
                        self.runtime.as_ref(),
                        keys,
                        &spill,
                        &mut partial_groups,
                        &mut runs,
                    )?;
                }
                let returned = merge_group_runs(
                    self.runtime.as_ref(),
                    keys,
                    working_budget,
                    &runs,
                    output_limit,
                    visitor,
                )?;
                drop(reservation);
                let strategies = ExecutionStrategies::default()
                    .with(source_strategy)
                    .with(ExecutionStrategy::ExternalGroup);
                return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                    prefix_stats.scanned(),
                    prefix_stats.filtered(),
                    returned,
                    strategies,
                )));
            }
            _ => {}
        }

        Ok(None)
    }

    /// Executes a physical plan while suppressing streaming-load result rows.
    pub fn execute_physical_compact(
        &self,
        physical: &PhysicalPlan,
    ) -> EngineResult<ExecutionOutput> {
        let started = Instant::now();
        let output = if let Some(storage) = self.system_collection_storage(physical)? {
            self.executor
                .execute_compact(&storage, self.runtime.as_ref(), physical)
                .map_err(EngineError::execution)?
        } else {
            self.executor
                .execute_compact(self.storage.as_ref(), self.runtime.as_ref(), physical)
                .map_err(EngineError::execution)?
        };
        self.indexing.observe(QueryObservation::from_execution(
            physical,
            output.statistics(),
            started.elapsed(),
        ));
        Ok(output)
    }

    fn system_collection_storage(
        &self,
        physical: &PhysicalPlan,
    ) -> EngineResult<Option<MemoryStorage>> {
        let collection = physical.source().collection();
        let storage = match collection.as_str() {
            vcollections::INDEX_OBSERVATIONS => self.index_observations_storage()?,
            vcollections::MEMORY => self.memory_storage()?,
            vcollections::MEMORY_EVENTS => self.memory_events_storage()?,
            vcollections::QUERY_MEMORY => self.query_memory_storage()?,
            _ => return Ok(None),
        };

        if physical.is_write() {
            return Err(EngineError::execution(
                ExecutionError::unsupported_operator(
                    "system-collection-write",
                    format!("{} is read-only", collection.as_str()),
                ),
            ));
        }

        Ok(Some(storage))
    }

    fn memory_storage(&self) -> EngineResult<MemoryStorage> {
        let storage = MemoryStorage::new();
        let collection = CollectionId::parse(vcollections::MEMORY).map_err(storage_engine_error)?;
        let snapshot = self.memory_governor.snapshot();
        let observed_bytes = snapshot.classes.iter().fold(0usize, |total, class| {
            total.saturating_add(class.observed_bytes)
        });
        let pressure = self.memory_governor.process_pressure();
        let event_snapshot = self.memory_governor.event_snapshot();
        let spill = crate::spill::stats_snapshot();
        let process = snapshot.process;
        let (pressure_state, soft_limit_bytes, hard_limit_bytes) = match pressure {
            ProcessMemoryPressure::Unlimited => ("unlimited", None, None),
            ProcessMemoryPressure::Unavailable { limit_bytes } => {
                ("unavailable", None, Some(limit_bytes))
            }
            ProcessMemoryPressure::Normal {
                soft_limit_bytes,
                hard_limit_bytes,
                ..
            } => ("normal", Some(soft_limit_bytes), Some(hard_limit_bytes)),
            ProcessMemoryPressure::Soft {
                soft_limit_bytes,
                hard_limit_bytes,
                ..
            } => ("soft", Some(soft_limit_bytes), Some(hard_limit_bytes)),
            ProcessMemoryPressure::Hard {
                soft_limit_bytes,
                hard_limit_bytes,
                ..
            } => ("hard", Some(soft_limit_bytes), Some(hard_limit_bytes)),
        };
        let mut transaction = storage.begin().map_err(storage_engine_error)?;

        let global = Document::from_fields([
            ("scope", Value::from("global")),
            ("class", Value::from("all")),
            (
                "profile",
                Value::from(self.memory_governor.profile().effective_profile_label()),
            ),
            (
                "base_profile",
                Value::from(self.memory_governor.profile().profile.as_str()),
            ),
            (
                "profile_scaled",
                Value::from(self.memory_governor.profile().is_scaled()),
            ),
            (
                "process_limit_bytes",
                optional_usize_value(self.memory_governor.profile().process_limit_bytes)?,
            ),
            (
                "runtime_reserve_bytes",
                usize_value(self.memory_governor.profile().runtime_reserve_bytes)?,
            ),
            (
                "managed_budget_bytes",
                optional_usize_value(self.memory_governor.profile().managed_budget_bytes)?,
            ),
            ("limit_bytes", optional_usize_value(snapshot.limit_bytes)?),
            ("current_bytes", usize_value(snapshot.current_bytes)?),
            ("peak_bytes", usize_value(snapshot.peak_bytes)?),
            ("observed_bytes", usize_value(observed_bytes)?),
            (
                "available_bytes",
                optional_usize_value(snapshot.available_bytes)?,
            ),
            (
                "active_reservations",
                usize_value(snapshot.active_reservations)?,
            ),
            (
                "failed_reservations",
                Value::from(snapshot.failed_reservations),
            ),
            ("event_capacity", usize_value(event_snapshot.capacity)?),
            ("dropped_events", Value::from(event_snapshot.dropped_events)),
            ("pressure_state", Value::from(pressure_state)),
            ("soft_limit_bytes", optional_usize_value(soft_limit_bytes)?),
            ("hard_limit_bytes", optional_usize_value(hard_limit_bytes)?),
            (
                "rss_bytes",
                optional_usize_value(process.map(|p| p.rss_bytes))?,
            ),
            (
                "anonymous_bytes",
                optional_usize_value(process.map(|p| p.anonymous_bytes))?,
            ),
            (
                "unmanaged_bytes",
                optional_usize_value(process.map(|p| p.unmanaged_bytes))?,
            ),
            ("memory_enforcement", Value::from("managed")),
            ("rss_enforced", Value::from(false)),
            (
                "process_headroom_bytes",
                optional_usize_value(match (process, hard_limit_bytes) {
                    (Some(process), Some(limit)) => Some(limit.saturating_sub(process.rss_bytes)),
                    _ => None,
                })?,
            ),
            (
                "rss_over_limit_bytes",
                optional_usize_value(match (process, hard_limit_bytes) {
                    (Some(process), Some(limit)) => Some(process.rss_bytes.saturating_sub(limit)),
                    _ => None,
                })?,
            ),
            ("spill_runs_created", Value::from(spill.runs_created)),
            ("spill_runs_active", Value::from(spill.runs_active)),
            (
                "spill_runs_peak_active",
                Value::from(spill.runs_peak_active),
            ),
            ("spill_runs_deleted", Value::from(spill.runs_deleted)),
            ("spill_records_written", Value::from(spill.records_written)),
            (
                "spill_payload_bytes_written",
                Value::from(spill.payload_bytes_written),
            ),
            (
                "spill_framing_bytes_written",
                Value::from(spill.framing_bytes_written),
            ),
            (
                "spill_bytes_written",
                Value::from(
                    spill
                        .payload_bytes_written
                        .saturating_add(spill.framing_bytes_written),
                ),
            ),
            ("spill_records_read", Value::from(spill.records_read)),
            (
                "spill_payload_bytes_read",
                Value::from(spill.payload_bytes_read),
            ),
            ("spill_live_bytes", Value::from(spill.live_bytes)),
            ("spill_peak_live_bytes", Value::from(spill.peak_live_bytes)),
            ("spill_create_us", Value::from(spill.create_us)),
            ("spill_flush_us", Value::from(spill.flush_us)),
            ("spill_sync_us", Value::from(spill.sync_us)),
            (
                "external_group_flushes",
                Value::from(EXTERNAL_GROUP_FLUSHES.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_partials_written",
                Value::from(EXTERNAL_GROUP_PARTIALS_WRITTEN.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_bytes_written",
                Value::from(EXTERNAL_GROUP_BYTES_WRITTEN.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_flush_us",
                Value::from(EXTERNAL_GROUP_FLUSH_US.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_partials_merged",
                Value::from(EXTERNAL_GROUP_PARTIALS_MERGED.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_merge_us",
                Value::from(EXTERNAL_GROUP_MERGE_US.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_peak_groups_per_flush",
                Value::from(EXTERNAL_GROUP_PEAK_GROUPS_PER_FLUSH.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_source_us",
                Value::from(EXTERNAL_GROUP_SOURCE_US.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_rows_consumed",
                Value::from(EXTERNAL_GROUP_ROWS_CONSUMED.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_consume_samples",
                Value::from(EXTERNAL_GROUP_CONSUME_SAMPLES.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_key_ns",
                Value::from(EXTERNAL_GROUP_KEY_NS.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_lookup_ns",
                Value::from(EXTERNAL_GROUP_LOOKUP_NS.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_accumulate_ns",
                Value::from(EXTERNAL_GROUP_ACCUMULATE_NS.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_group_hits",
                Value::from(EXTERNAL_GROUP_GROUP_HITS.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_group_misses",
                Value::from(EXTERNAL_GROUP_GROUP_MISSES.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_accumulator_create_us",
                Value::from(EXTERNAL_GROUP_ACCUMULATOR_CREATE_US.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_flush_triggers",
                Value::from(EXTERNAL_GROUP_FLUSH_TRIGGERS.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_estimated_peak_bytes",
                Value::from(EXTERNAL_GROUP_ESTIMATED_PEAK_BYTES.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_partial_finish_us",
                Value::from(EXTERNAL_GROUP_PARTIAL_FINISH_US.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_flush_sort_us",
                Value::from(EXTERNAL_GROUP_FLUSH_SORT_US.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_encode_write_us",
                Value::from(EXTERNAL_GROUP_ENCODE_WRITE_US.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_merge_init_us",
                Value::from(EXTERNAL_GROUP_MERGE_INIT_US.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_merge_samples",
                Value::from(EXTERNAL_GROUP_MERGE_SAMPLES.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_merge_select_ns",
                Value::from(EXTERNAL_GROUP_MERGE_SELECT_NS.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_merge_partial_ns",
                Value::from(EXTERNAL_GROUP_MERGE_PARTIAL_NS.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_merge_read_ns",
                Value::from(EXTERNAL_GROUP_MERGE_READ_NS.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_merge_finish_ns",
                Value::from(EXTERNAL_GROUP_MERGE_FINISH_NS.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_rss_samples",
                Value::from(EXTERNAL_GROUP_RSS_SAMPLES.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_rss_peak_bytes",
                Value::from(EXTERNAL_GROUP_RSS_PEAK_BYTES.load(AtomicOrdering::Relaxed)),
            ),
            (
                "external_group_unmanaged_peak_bytes",
                Value::from(EXTERNAL_GROUP_UNMANAGED_PEAK_BYTES.load(AtomicOrdering::Relaxed)),
            ),
        ]);
        transaction
            .insert(
                &collection,
                DocumentId::synthetic(0x6d65_6d6f_7279, 0),
                Arc::new(global),
            )
            .map_err(storage_engine_error)?;

        for (ordinal, class) in snapshot.classes.into_iter().enumerate() {
            let document = Document::from_fields([
                ("scope", Value::from("class")),
                ("class", Value::from(class.class.as_str())),
                ("profile", Value::Null),
                ("base_profile", Value::Null),
                ("profile_scaled", Value::Null),
                ("process_limit_bytes", Value::Null),
                ("runtime_reserve_bytes", Value::Null),
                ("managed_budget_bytes", Value::Null),
                ("limit_bytes", Value::Null),
                ("current_bytes", usize_value(class.current_bytes)?),
                ("peak_bytes", usize_value(class.peak_bytes)?),
                ("observed_bytes", usize_value(class.observed_bytes)?),
                ("available_bytes", Value::Null),
                (
                    "active_reservations",
                    usize_value(class.active_reservations)?,
                ),
                (
                    "failed_reservations",
                    Value::from(class.failed_reservations),
                ),
                ("event_capacity", Value::Null),
                ("dropped_events", Value::Null),
                ("pressure_state", Value::Null),
                ("soft_limit_bytes", Value::Null),
                ("hard_limit_bytes", Value::Null),
                ("rss_bytes", Value::Null),
                ("anonymous_bytes", Value::Null),
                ("unmanaged_bytes", Value::Null),
                ("memory_enforcement", Value::Null),
                ("rss_enforced", Value::Null),
                ("process_headroom_bytes", Value::Null),
                ("rss_over_limit_bytes", Value::Null),
                ("spill_runs_created", Value::Null),
                ("spill_runs_active", Value::Null),
                ("spill_runs_peak_active", Value::Null),
                ("spill_runs_deleted", Value::Null),
                ("spill_records_written", Value::Null),
                ("spill_payload_bytes_written", Value::Null),
                ("spill_framing_bytes_written", Value::Null),
                ("spill_bytes_written", Value::Null),
                ("spill_records_read", Value::Null),
                ("spill_payload_bytes_read", Value::Null),
                ("spill_live_bytes", Value::Null),
                ("spill_peak_live_bytes", Value::Null),
                ("spill_create_us", Value::Null),
                ("spill_flush_us", Value::Null),
                ("spill_sync_us", Value::Null),
                ("external_group_flushes", Value::Null),
                ("external_group_partials_written", Value::Null),
                ("external_group_bytes_written", Value::Null),
                ("external_group_flush_us", Value::Null),
                ("external_group_partials_merged", Value::Null),
                ("external_group_merge_us", Value::Null),
                ("external_group_peak_groups_per_flush", Value::Null),
                ("external_group_source_us", Value::Null),
                ("external_group_rows_consumed", Value::Null),
                ("external_group_consume_samples", Value::Null),
                ("external_group_key_ns", Value::Null),
                ("external_group_lookup_ns", Value::Null),
                ("external_group_accumulate_ns", Value::Null),
                ("external_group_group_hits", Value::Null),
                ("external_group_group_misses", Value::Null),
                ("external_group_accumulator_create_us", Value::Null),
                ("external_group_flush_triggers", Value::Null),
                ("external_group_estimated_peak_bytes", Value::Null),
                ("external_group_partial_finish_us", Value::Null),
                ("external_group_flush_sort_us", Value::Null),
                ("external_group_encode_write_us", Value::Null),
                ("external_group_merge_init_us", Value::Null),
                ("external_group_merge_samples", Value::Null),
                ("external_group_merge_select_ns", Value::Null),
                ("external_group_merge_partial_ns", Value::Null),
                ("external_group_merge_read_ns", Value::Null),
                ("external_group_merge_finish_ns", Value::Null),
                ("external_group_rss_samples", Value::Null),
                ("external_group_rss_peak_bytes", Value::Null),
                ("external_group_unmanaged_peak_bytes", Value::Null),
            ]);
            transaction
                .insert(
                    &collection,
                    DocumentId::synthetic(
                        0x6d65_6d63_6c61,
                        u64::try_from(ordinal).map_err(|_| {
                            EngineError::execution(ExecutionError::evaluation(
                                "too many memory classes",
                            ))
                        })?,
                    ),
                    Arc::new(document),
                )
                .map_err(storage_engine_error)?;
        }

        transaction.commit().map_err(storage_engine_error)?;
        Ok(storage)
    }

    fn query_memory_storage(&self) -> EngineResult<MemoryStorage> {
        let storage = MemoryStorage::new();
        let collection = CollectionId::parse(vcollections::QUERY_MEMORY).map_err(storage_engine_error)?;
        let snapshot = self.memory_governor.query_memory_snapshot();
        let mut transaction = storage.begin().map_err(storage_engine_error)?;
        let global = Document::from_fields([
            ("scope", Value::from("global")),
            ("operation_id", Value::Null),
            ("class", Value::from("all")),
            (
                "profile",
                Value::from(if snapshot.profile_scaled {
                    format!("custom (base: {})", snapshot.base_profile.as_str())
                } else {
                    snapshot.base_profile.as_str().to_owned()
                }),
            ),
            ("base_profile", Value::from(snapshot.base_profile.as_str())),
            ("profile_scaled", Value::from(snapshot.profile_scaled)),
            (
                "process_limit_bytes",
                optional_usize_value(snapshot.process_limit_bytes)?,
            ),
            (
                "runtime_reserve_bytes",
                usize_value(snapshot.runtime_reserve_bytes)?,
            ),
            (
                "managed_budget_bytes",
                optional_usize_value(snapshot.managed_budget_bytes)?,
            ),
            (
                "operation_budget_bytes",
                usize_value(snapshot.operation_budget_bytes)?,
            ),
            (
                "active_operation_bytes",
                usize_value(snapshot.active_operation_bytes)?,
            ),
            (
                "peak_operation_bytes",
                usize_value(snapshot.peak_operation_bytes)?,
            ),
            (
                "active_heavy_operations",
                usize_value(snapshot.active_heavy_operations)?,
            ),
            (
                "rejected_operations",
                Value::from(snapshot.rejected_operations),
            ),
            ("budget_bytes", Value::Null),
        ]);
        transaction
            .insert(
                &collection,
                DocumentId::synthetic(0x7175_6572_796d, 0),
                Arc::new(global),
            )
            .map_err(storage_engine_error)?;
        for record in snapshot.records {
            let document = Document::from_fields([
                ("scope", Value::from("operation")),
                ("operation_id", Value::from(record.id)),
                ("class", Value::from(record.class.as_str())),
                ("profile", Value::Null),
                ("base_profile", Value::Null),
                ("profile_scaled", Value::Null),
                ("process_limit_bytes", Value::Null),
                ("runtime_reserve_bytes", Value::Null),
                ("managed_budget_bytes", Value::Null),
                ("operation_budget_bytes", Value::Null),
                ("active_operation_bytes", Value::Null),
                ("peak_operation_bytes", Value::Null),
                ("active_heavy_operations", Value::Null),
                ("rejected_operations", Value::Null),
                ("budget_bytes", usize_value(record.budget_bytes)?),
            ]);
            transaction
                .insert(
                    &collection,
                    DocumentId::synthetic(0x7175_6572_796f, record.id),
                    Arc::new(document),
                )
                .map_err(storage_engine_error)?;
        }
        transaction.commit().map_err(storage_engine_error)?;
        Ok(storage)
    }

    fn memory_events_storage(&self) -> EngineResult<MemoryStorage> {
        let storage = MemoryStorage::new();
        let collection = CollectionId::parse(vcollections::MEMORY_EVENTS).map_err(storage_engine_error)?;
        let snapshot = self.memory_governor.event_snapshot();
        let mut transaction = storage.begin().map_err(storage_engine_error)?;

        for event in snapshot.events {
            let document = Document::from_fields([
                ("sequence", Value::from(event.sequence)),
                ("kind", Value::from(event.kind.as_str())),
                ("class", Value::from(event.class.as_str())),
                ("bytes", usize_value(event.bytes)?),
                ("current_bytes", usize_value(event.current_bytes)?),
                ("limit_bytes", optional_usize_value(event.limit_bytes)?),
                ("event_capacity", usize_value(snapshot.capacity)?),
                ("dropped_events", Value::from(snapshot.dropped_events)),
            ]);
            transaction
                .insert(
                    &collection,
                    DocumentId::synthetic(0x6d65_6d65_766e, event.sequence),
                    Arc::new(document),
                )
                .map_err(storage_engine_error)?;
        }

        transaction.commit().map_err(storage_engine_error)?;
        Ok(storage)
    }

    fn index_observations_storage(&self) -> EngineResult<MemoryStorage> {
        let storage = MemoryStorage::new();
        let collection =
            CollectionId::parse(vcollections::INDEX_OBSERVATIONS).map_err(storage_engine_error)?;
        let snapshot = self.indexing.snapshot();
        let dropped_full = snapshot.dropped_full;
        let dropped_disconnected = snapshot.dropped_disconnected;
        let mut transaction = storage.begin().map_err(storage_engine_error)?;

        let mut observations = snapshot.queries.into_iter().collect::<Vec<_>>();
        observations.sort_by_key(|(fingerprint, _)| fingerprint.as_u64());

        for (ordinal, (fingerprint, aggregate)) in observations.into_iter().enumerate() {
            let document = Document::from_fields([
                (
                    "fingerprint",
                    Value::from(format!("{:016x}", fingerprint.as_u64())),
                ),
                ("collection", Value::from(aggregate.collection.as_str())),
                (
                    "access",
                    Value::from(match aggregate.access {
                        ObservedAccess::CollectionScan => "collection_scan",
                        ObservedAccess::PrimaryKeyLookup => "primary_key_lookup",
                    }),
                ),
                ("executions", Value::from(aggregate.executions)),
                ("scanned", Value::from(aggregate.scanned)),
                ("returned", Value::from(aggregate.returned)),
                ("elapsed_us", Value::from(aggregate.elapsed_micros)),
                (
                    "average_elapsed_us",
                    Value::from(if aggregate.executions == 0 {
                        0
                    } else {
                        aggregate.elapsed_micros / aggregate.executions
                    }),
                ),
                ("dropped_full", Value::from(dropped_full)),
                ("dropped_disconnected", Value::from(dropped_disconnected)),
            ]);
            let ordinal = u64::try_from(ordinal).map_err(|_| {
                EngineError::execution(ExecutionError::evaluation("too many indexing observations"))
            })?;
            transaction
                .insert(
                    &collection,
                    DocumentId::synthetic(fingerprint.as_u64(), ordinal),
                    Arc::new(document),
                )
                .map_err(storage_engine_error)?;
        }
        transaction.commit().map_err(storage_engine_error)?;
        Ok(storage)
    }

    /// Plans and executes a normalized query pipeline.
    pub fn execute(&self, pipeline: &PlannerPipeline) -> EngineResult<QueryOutput> {
        let planned = self.plan(pipeline)?;
        let output = self.execute_physical(planned.physical())?;

        Ok(QueryOutput { planned, output })
    }
}

fn usize_value(value: usize) -> EngineResult<Value> {
    u64::try_from(value).map(Value::from).map_err(|_| {
        EngineError::execution(ExecutionError::evaluation(
            "memory counter does not fit in the public numeric representation",
        ))
    })
}

fn optional_usize_value(value: Option<usize>) -> EngineResult<Value> {
    value.map_or(Ok(Value::Null), usize_value)
}

impl fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("planner", &self.planner)
            .field("physical_planner", &self.physical_planner)
            .field("executor", &self.executor)
            .finish_non_exhaustive()
    }
}

/// Logical and physical representations of one planned query.
#[derive(Clone, Debug)]
pub struct PlannedQuery {
    logical: LogicalPlan,
    physical: PhysicalPlan,
}

impl PlannedQuery {
    /// Creates a paired planned query.
    #[must_use]
    #[inline]
    pub const fn new(logical: LogicalPlan, physical: PhysicalPlan) -> Self {
        Self { logical, physical }
    }

    /// Returns the logical plan.
    #[must_use]
    pub const fn logical(&self) -> &LogicalPlan {
        &self.logical
    }

    /// Returns the physical plan.
    #[must_use]
    pub const fn physical(&self) -> &PhysicalPlan {
        &self.physical
    }

    /// Consumes the pair.
    #[must_use]
    pub fn into_parts(self) -> (LogicalPlan, PhysicalPlan) {
        (self.logical, self.physical)
    }
}

/// Full result of planning and executing a query.
#[derive(Clone, Debug)]
pub struct QueryOutput {
    planned: PlannedQuery,
    output: ExecutionOutput,
}

impl QueryOutput {
    /// Returns both query plans.
    #[must_use]
    pub const fn planned(&self) -> &PlannedQuery {
        &self.planned
    }

    /// Returns execution output.
    #[must_use]
    pub const fn output(&self) -> &ExecutionOutput {
        &self.output
    }

    /// Consumes the result and returns its parts.
    #[must_use]
    pub fn into_parts(self) -> (PlannedQuery, ExecutionOutput) {
        (self.planned, self.output)
    }
}

fn emit_streaming_count(
    visitor: &mut dyn FnMut(StoredDocument) -> EngineResult<()>,
    alias: &str,
    count: u64,
) -> EngineResult<()> {
    let document = Arc::new(Document::from_fields([(alias, Value::from(count))]));
    let stored = StoredDocument::new(
        DocumentId::synthetic(0x0063_6f75_6e74, 1),
        DocumentVersion::INITIAL,
        document,
    )
    .map_err(storage_engine_error)?;
    visitor(stored)
}

#[derive(Debug)]
struct KeyedRow {
    key: Arc<[u8]>,
    row: ExecutionRow,
}

enum GroupSpillPartial {
    Compact(Vec<u8>),
    Document(ExecutionRow),
}

struct GroupSpillRecord {
    key: Arc<[u8]>,
    partial: GroupSpillPartial,
}

#[inline]
fn bounded_sort_input_budget(working_budget: usize) -> usize {
    // Keep half of the governed working set available for the stable sort scratch
    // buffer. This makes every run self-contained inside the same memory contract.
    working_budget.saturating_div(2).max(512 * 1024)
}

fn push_bounded_sort_row(
    runtime: &dyn ExecutionRuntime,
    keys: &[SortKey],
    spill: &SpillEngine,
    row: ExecutionRow,
    budget: usize,
    chunk: &mut Vec<ExecutionRow>,
    chunk_bytes: &mut usize,
    runs: &mut Vec<SpillRun>,
    oversized_message: &'static str,
) -> EngineResult<()> {
    let estimated = execution_row_working_bytes(&row).map_err(EngineError::execution)?;
    if estimated > budget {
        return Err(EngineError::execution(ExecutionError::evaluation(
            oversized_message,
        )));
    }
    if !chunk.is_empty() && chunk_bytes.saturating_add(estimated) > budget {
        flush_sorted_run(runtime, keys, spill, chunk, runs).map_err(EngineError::execution)?;
        *chunk_bytes = 0;
    }
    *chunk_bytes = chunk_bytes.saturating_add(estimated);
    chunk.push(row);
    Ok(())
}

fn flush_sorted_run(
    runtime: &dyn ExecutionRuntime,
    keys: &[crate::query::SortKey],
    spill: &SpillEngine,
    chunk: &mut Vec<ExecutionRow>,
    runs: &mut Vec<SpillRun>,
) -> Result<(), ExecutionError> {
    stable_sort(runtime, keys, chunk)?;
    let mut writer = spill.create_run().map_err(spill_engine_error)?;
    for row in chunk.iter() {
        writer
            .append(&encode_execution_row(row)?)
            .map_err(spill_engine_error)?;
    }
    runs.push(writer.finish().map_err(spill_engine_error)?);
    chunk.clear();
    Ok(())
}

fn emit_distinct_limited_rows(
    runtime: &dyn ExecutionRuntime,
    fields: &[crate::query::ExpressionFieldPath],
    limit: usize,
    rows: impl IntoIterator<Item = ExecutionRow>,
    visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>,
) -> EngineResult<u64> {
    if limit == 0 {
        return Ok(0);
    }
    let mut seen = Vec::<Arc<[u8]>>::with_capacity(limit);
    let mut returned = 0u64;
    for row in rows {
        let key = runtime
            .distinct_key(fields, row.document())
            .map_err(EngineError::execution)?;
        if seen
            .iter()
            .any(|existing| existing.as_ref() == key.as_ref())
        {
            continue;
        }
        seen.push(key);
        visitor(row)?;
        returned = returned.saturating_add(1);
        if returned as usize >= limit {
            break;
        }
    }
    Ok(returned)
}

fn merge_sorted_distinct_limited_runs(
    runtime: &dyn ExecutionRuntime,
    sort_keys: &[crate::query::SortKey],
    distinct_fields: &[crate::query::ExpressionFieldPath],
    runs: &[SpillRun],
    limit: usize,
    visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>,
) -> EngineResult<u64> {
    if limit == 0 {
        return Ok(0);
    }
    let mut readers = runs
        .iter()
        .map(SpillRun::reader)
        .collect::<io::Result<Vec<_>>>()
        .map_err(engine_spill_error)?;
    let mut heads = readers
        .iter_mut()
        .map(read_spilled_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(EngineError::execution)?;
    let mut seen = Vec::<Arc<[u8]>>::with_capacity(limit);
    let mut returned = 0u64;
    loop {
        let mut best = None;
        for index in 0..heads.len() {
            let Some(candidate) = heads[index].as_ref() else {
                continue;
            };
            match best {
                None => best = Some(index),
                Some(current) => {
                    let selected = heads[current].as_ref().expect("selected spill head");
                    if runtime
                        .compare_documents(sort_keys, candidate.document(), selected.document())
                        .map_err(EngineError::execution)?
                        == Ordering::Less
                    {
                        best = Some(index);
                    }
                }
            }
        }
        let Some(index) = best else {
            break;
        };
        let row = heads[index].take().expect("selected spill row");
        heads[index] = read_spilled_row(&mut readers[index]).map_err(EngineError::execution)?;
        let key = runtime
            .distinct_key(distinct_fields, row.document())
            .map_err(EngineError::execution)?;
        if seen
            .iter()
            .any(|existing| existing.as_ref() == key.as_ref())
        {
            continue;
        }
        seen.push(key);
        visitor(row)?;
        returned = returned.saturating_add(1);
        if returned as usize >= limit {
            break;
        }
    }
    Ok(returned)
}

fn merge_sorted_runs(
    runtime: &dyn ExecutionRuntime,
    keys: &[crate::query::SortKey],
    runs: &[SpillRun],
    limit: Option<usize>,
    visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>,
) -> EngineResult<u64> {
    let mut readers = runs
        .iter()
        .map(SpillRun::reader)
        .collect::<io::Result<Vec<_>>>()
        .map_err(engine_spill_error)?;
    let mut heads = readers
        .iter_mut()
        .map(read_spilled_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(EngineError::execution)?;
    let mut returned = 0u64;
    loop {
        if limit.is_some_and(|limit| returned as usize >= limit) {
            break;
        }
        let mut best = None;
        for index in 0..heads.len() {
            let Some(candidate) = heads[index].as_ref() else {
                continue;
            };
            match best {
                None => best = Some(index),
                Some(current) => {
                    let selected = heads[current].as_ref().expect("selected spill head");
                    if runtime
                        .compare_documents(keys, candidate.document(), selected.document())
                        .map_err(EngineError::execution)?
                        == Ordering::Less
                    {
                        best = Some(index);
                    }
                }
            }
        }
        let Some(index) = best else { break };
        visitor(heads[index].take().expect("selected spill row"))?;
        returned = returned.saturating_add(1);
        heads[index] = read_spilled_row(&mut readers[index]).map_err(EngineError::execution)?;
    }
    Ok(returned)
}

fn read_spilled_row(reader: &mut SpillRunReader) -> Result<Option<ExecutionRow>, ExecutionError> {
    reader
        .next_record()
        .map_err(spill_engine_error)?
        .map(|bytes| decode_execution_row(&bytes))
        .transpose()
}

fn flush_partial_group_run(
    _runtime: &dyn ExecutionRuntime,
    _keys: &[crate::query::ExpressionFieldPath],
    spill: &SpillEngine,
    groups: &mut HashMap<Arc<[u8]>, Box<dyn crate::query::IncrementalGroupAccumulator>>,
    runs: &mut Vec<SpillRun>,
) -> EngineResult<()> {
    let started = Instant::now();
    let group_count = usize_to_u64_saturating(groups.len());
    update_atomic_peak(&EXTERNAL_GROUP_PEAK_GROUPS_PER_FLUSH, group_count);

    let finish_started = Instant::now();
    let mut chunk = Vec::<GroupSpillRecord>::with_capacity(groups.len());
    for (ordinal, (key, accumulator)) in groups.drain().enumerate() {
        let partial = if let Some(payload) = accumulator
            .compact_partial()
            .map_err(EngineError::execution)?
        {
            GroupSpillPartial::Compact(payload)
        } else {
            let partial = accumulator
                .finish_partial(usize_to_u64_saturating(ordinal).saturating_add(1))
                .map_err(EngineError::execution)?
                .ok_or_else(|| {
                    EngineError::execution(ExecutionError::evaluation(
                        "external group runtime cannot serialize mergeable partial state",
                    ))
                })?;
            GroupSpillPartial::Document(ExecutionRow::synthetic(partial))
        };
        chunk.push(GroupSpillRecord { key, partial });
    }
    EXTERNAL_GROUP_PARTIAL_FINISH_US
        .fetch_add(elapsed_micros(finish_started), AtomicOrdering::Relaxed);

    let sort_started = Instant::now();
    chunk.sort_by(|left, right| left.key.cmp(&right.key));
    EXTERNAL_GROUP_FLUSH_SORT_US.fetch_add(elapsed_micros(sort_started), AtomicOrdering::Relaxed);

    let encode_write_started = Instant::now();
    let mut writer = spill.create_run().map_err(engine_spill_error)?;
    // Group runs are block-framed and prefix-compressed. This amortizes the
    // generic spill engine's 8-byte framing across many groups and avoids
    // rewriting common key prefixes for high-cardinality sorted runs.
    let mut block = Vec::with_capacity(GROUP_SPILL_BLOCK_BYTES);
    let mut previous_key = Vec::<u8>::new();
    for record in chunk.iter() {
        append_group_spill_block_record(&mut block, &mut previous_key, record)
            .map_err(EngineError::execution)?;
        if block.len() >= GROUP_SPILL_BLOCK_BYTES {
            writer.append(&block).map_err(engine_spill_error)?;
            block.clear();
            previous_key.clear();
        }
    }
    if !block.is_empty() {
        writer.append(&block).map_err(engine_spill_error)?;
    }
    let run = writer.finish().map_err(engine_spill_error)?;
    let bytes = run.bytes();
    runs.push(run);
    chunk.clear();
    EXTERNAL_GROUP_ENCODE_WRITE_US.fetch_add(
        elapsed_micros(encode_write_started),
        AtomicOrdering::Relaxed,
    );

    EXTERNAL_GROUP_FLUSHES.fetch_add(1, AtomicOrdering::Relaxed);
    EXTERNAL_GROUP_PARTIALS_WRITTEN.fetch_add(group_count, AtomicOrdering::Relaxed);
    EXTERNAL_GROUP_BYTES_WRITTEN.fetch_add(bytes, AtomicOrdering::Relaxed);
    EXTERNAL_GROUP_FLUSH_US.fetch_add(elapsed_micros(started), AtomicOrdering::Relaxed);
    Ok(())
}

const GROUP_SPILL_BLOCK_TAG: u8 = 0xB2;
const GROUP_SPILL_BLOCK_BYTES: usize = 64 * 1024;

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(left, right)| left == right)
        .count()
}

fn put_group_varint(output: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn take_group_varint(bytes: &[u8], position: &mut usize) -> Result<usize, ExecutionError> {
    let mut value = 0usize;
    for shift in (0..usize::BITS).step_by(7) {
        let byte = *bytes
            .get(*position)
            .ok_or_else(|| ExecutionError::evaluation("truncated group spill varint"))?;
        *position += 1;
        value |= usize::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(ExecutionError::evaluation("group spill varint overflow"))
}

fn group_spill_partial_bytes(partial: &GroupSpillPartial) -> Result<(u8, Vec<u8>), ExecutionError> {
    Ok(match partial {
        GroupSpillPartial::Compact(payload) => (1, payload.clone()),
        GroupSpillPartial::Document(row) => (0, encode_execution_row(row)?),
    })
}

fn append_group_spill_block_record(
    block: &mut Vec<u8>,
    previous_key: &mut Vec<u8>,
    record: &GroupSpillRecord,
) -> Result<(), ExecutionError> {
    if block.is_empty() {
        block.push(GROUP_SPILL_BLOCK_TAG);
    }
    let common = common_prefix_len(previous_key, &record.key);
    let suffix = &record.key[common..];
    let (tag, payload) = group_spill_partial_bytes(&record.partial)?;
    put_group_varint(block, common);
    put_group_varint(block, suffix.len());
    block.push(tag);
    put_group_varint(block, payload.len());
    block.extend_from_slice(suffix);
    block.extend_from_slice(&payload);
    previous_key.clear();
    previous_key.extend_from_slice(&record.key);
    Ok(())
}

fn decode_group_spill_partial(
    tag: u8,
    payload: &[u8],
) -> Result<GroupSpillPartial, ExecutionError> {
    match tag {
        0 => Ok(GroupSpillPartial::Document(decode_execution_row(payload)?)),
        1 => Ok(GroupSpillPartial::Compact(payload.to_vec())),
        tag => Err(ExecutionError::evaluation(format!(
            "invalid group spill tag {tag}"
        ))),
    }
}

// Legacy one-record framing remains readable for compatibility with tests and
// custom spill producers; new engine runs use GROUP_SPILL_BLOCK_TAG blocks.
fn decode_group_spill_record(bytes: &[u8]) -> Result<GroupSpillRecord, ExecutionError> {
    if bytes.len() < 5 {
        return Err(ExecutionError::evaluation("truncated group spill row"));
    }
    let key_len = u32::from_le_bytes(bytes[1..5].try_into().expect("four bytes")) as usize;
    if bytes.len() < 5 + key_len {
        return Err(ExecutionError::evaluation("truncated group spill key"));
    }
    Ok(GroupSpillRecord {
        key: Arc::from(&bytes[5..5 + key_len]),
        partial: decode_group_spill_partial(bytes[0], &bytes[5 + key_len..])?,
    })
}

struct GroupSpillRunReader {
    reader: SpillRunReader,
    pending: VecDeque<GroupSpillRecord>,
}

impl GroupSpillRunReader {
    fn new(run: &SpillRun) -> io::Result<Self> {
        Ok(Self {
            reader: run.reader()?,
            pending: VecDeque::new(),
        })
    }

    fn next_record(&mut self) -> Result<Option<GroupSpillRecord>, ExecutionError> {
        if let Some(record) = self.pending.pop_front() {
            return Ok(Some(record));
        }
        let Some(bytes) = self.reader.next_record().map_err(spill_engine_error)? else {
            return Ok(None);
        };
        if bytes.first().copied() != Some(GROUP_SPILL_BLOCK_TAG) {
            return decode_group_spill_record(&bytes).map(Some);
        }
        let mut position = 1usize;
        let mut previous_key = Vec::<u8>::new();
        while position < bytes.len() {
            let common = take_group_varint(&bytes, &mut position)?;
            let suffix_len = take_group_varint(&bytes, &mut position)?;
            let tag = *bytes
                .get(position)
                .ok_or_else(|| ExecutionError::evaluation("truncated group spill block tag"))?;
            position += 1;
            let payload_len = take_group_varint(&bytes, &mut position)?;
            if common > previous_key.len() {
                return Err(ExecutionError::evaluation("invalid group spill key prefix"));
            }
            let suffix_end = position
                .checked_add(suffix_len)
                .ok_or_else(|| ExecutionError::evaluation("group spill suffix overflow"))?;
            let suffix = bytes
                .get(position..suffix_end)
                .ok_or_else(|| ExecutionError::evaluation("truncated group spill key suffix"))?;
            position = suffix_end;
            let payload_end = position
                .checked_add(payload_len)
                .ok_or_else(|| ExecutionError::evaluation("group spill payload overflow"))?;
            let payload = bytes
                .get(position..payload_end)
                .ok_or_else(|| ExecutionError::evaluation("truncated group spill payload"))?;
            position = payload_end;

            let mut key = Vec::with_capacity(common + suffix.len());
            key.extend_from_slice(&previous_key[..common]);
            key.extend_from_slice(suffix);
            previous_key.clear();
            previous_key.extend_from_slice(&key);
            self.pending.push_back(GroupSpillRecord {
                key: Arc::from(key),
                partial: decode_group_spill_partial(tag, payload)?,
            });
        }
        self.pending
            .pop_front()
            .map_or(Ok(None), |record| Ok(Some(record)))
    }
}

fn read_group_spill_record(
    reader: &mut GroupSpillRunReader,
) -> Result<Option<GroupSpillRecord>, ExecutionError> {
    reader.next_record()
}

fn group_merge_heads(
    runs: &[SpillRun],
) -> EngineResult<(Vec<GroupSpillRunReader>, Vec<Option<GroupSpillRecord>>)> {
    let mut readers = runs
        .iter()
        .map(GroupSpillRunReader::new)
        .collect::<io::Result<Vec<_>>>()
        .map_err(engine_spill_error)?;
    let heads = readers
        .iter_mut()
        .map(read_group_spill_record)
        .collect::<Result<Vec<_>, _>>()
        .map_err(EngineError::execution)?;
    Ok((readers, heads))
}

fn smallest_group_head(heads: &[Option<GroupSpillRecord>]) -> Option<usize> {
    heads
        .iter()
        .enumerate()
        .filter_map(|(index, head)| head.as_ref().map(|head| (index, &head.key)))
        .min_by(|left, right| left.1.cmp(right.1))
        .map(|(index, _)| index)
}

fn flush_keyed_run(
    spill: &SpillEngine,
    chunk: &mut Vec<KeyedRow>,
    runs: &mut Vec<SpillRun>,
) -> EngineResult<u64> {
    chunk.sort_by(|left, right| left.key.cmp(&right.key));
    let mut writer = spill.create_run().map_err(engine_spill_error)?;
    for keyed in chunk.iter() {
        writer
            .append(&encode_keyed_row(keyed).map_err(EngineError::execution)?)
            .map_err(engine_spill_error)?;
    }
    let run = writer.finish().map_err(engine_spill_error)?;
    let bytes = run.bytes();
    runs.push(run);
    chunk.clear();
    Ok(bytes)
}

fn encode_keyed_row(keyed: &KeyedRow) -> Result<Vec<u8>, ExecutionError> {
    let key_len = u32::try_from(keyed.key.len())
        .map_err(|_| ExecutionError::evaluation("blocking key exceeds u32"))?;
    let row = encode_execution_row(&keyed.row)?;
    let mut output = Vec::with_capacity(4 + keyed.key.len() + row.len());
    output.extend_from_slice(&key_len.to_le_bytes());
    output.extend_from_slice(&keyed.key);
    output.extend_from_slice(&row);
    Ok(output)
}

fn decode_keyed_row(bytes: &[u8]) -> Result<KeyedRow, ExecutionError> {
    if bytes.len() < 4 {
        return Err(ExecutionError::evaluation("truncated keyed spill row"));
    }
    let key_len = u32::from_le_bytes(bytes[..4].try_into().expect("four bytes")) as usize;
    if bytes.len() < 4 + key_len {
        return Err(ExecutionError::evaluation("truncated keyed spill key"));
    }
    Ok(KeyedRow {
        key: Arc::from(&bytes[4..4 + key_len]),
        row: decode_execution_row(&bytes[4 + key_len..])?,
    })
}

fn read_keyed_row(reader: &mut SpillRunReader) -> Result<Option<KeyedRow>, ExecutionError> {
    reader
        .next_record()
        .map_err(spill_engine_error)?
        .map(|bytes| decode_keyed_row(&bytes))
        .transpose()
}

fn keyed_merge_heads(
    runs: &[SpillRun],
) -> EngineResult<(Vec<SpillRunReader>, Vec<Option<KeyedRow>>)> {
    let mut readers = runs
        .iter()
        .map(SpillRun::reader)
        .collect::<io::Result<Vec<_>>>()
        .map_err(engine_spill_error)?;
    let heads = readers
        .iter_mut()
        .map(read_keyed_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(EngineError::execution)?;
    Ok((readers, heads))
}

fn smallest_keyed_head(heads: &[Option<KeyedRow>]) -> Option<usize> {
    heads
        .iter()
        .enumerate()
        .filter_map(|(index, head)| head.as_ref().map(|head| (index, &head.key)))
        .min_by(|left, right| left.1.cmp(right.1))
        .map(|(index, _)| index)
}

fn merge_distinct_runs(
    runs: &[SpillRun],
    limit: Option<usize>,
    visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>,
) -> EngineResult<u64> {
    let (mut readers, mut heads) = keyed_merge_heads(runs)?;
    let mut previous: Option<Arc<[u8]>> = None;
    let mut returned = 0u64;
    while let Some(index) = smallest_keyed_head(&heads) {
        if limit.is_some_and(|limit| returned as usize >= limit) {
            break;
        }
        let keyed = heads[index].take().expect("selected keyed head");
        if previous.as_deref() != Some(keyed.key.as_ref()) {
            previous = Some(keyed.key.clone());
            visitor(keyed.row)?;
            returned = returned.saturating_add(1);
        }
        heads[index] = read_keyed_row(&mut readers[index]).map_err(EngineError::execution)?;
    }
    Ok(returned)
}

fn merge_group_runs(
    runtime: &dyn ExecutionRuntime,
    keys: &[crate::query::ExpressionFieldPath],
    group_budget: usize,
    runs: &[SpillRun],
    limit: Option<usize>,
    visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>,
) -> EngineResult<u64> {
    // Capability-driven runtimes expose an incremental accumulator. In that
    // mode one group retains only its keys, counters and aggregate states;
    // source rows are never materialized regardless of group cardinality.
    if runtime
        .incremental_group_accumulator(keys)
        .map_err(EngineError::execution)?
        .is_some()
    {
        let merge_started = Instant::now();
        let mut merged_partials = 0u64;
        let init_started = Instant::now();
        let (mut readers, mut heads) = group_merge_heads(runs)?;
        EXTERNAL_GROUP_MERGE_INIT_US
            .fetch_add(elapsed_micros(init_started), AtomicOrdering::Relaxed);
        let mut current_key: Option<Arc<[u8]>> = None;
        let mut accumulator: Option<Box<dyn crate::query::IncrementalGroupAccumulator>> = None;
        let mut returned = 0u64;
        let mut merge_samples = 0u64;
        let mut merge_select_ns = 0u64;
        let mut merge_partial_ns = 0u64;
        let mut merge_read_ns = 0u64;
        let mut merge_finish_ns = 0u64;

        loop {
            let sampled =
                crate::debug::query_instrumentation_enabled() && merged_partials & 1023 == 0;
            let select_started = sampled.then(Instant::now);
            let selected = smallest_group_head(&heads);
            if let Some(started) = select_started {
                merge_select_ns = merge_select_ns.saturating_add(elapsed_nanos(started));
            }
            let Some(index) = selected else { break };
            if limit.is_some_and(|limit| returned as usize >= limit) {
                break;
            }
            if sampled {
                merge_samples = merge_samples.saturating_add(1);
            }

            let keyed = heads[index].take().expect("selected keyed head");
            merged_partials = merged_partials.saturating_add(1);
            let key_changed = current_key
                .as_deref()
                .is_some_and(|key| key != keyed.key.as_ref());

            if key_changed {
                if let Some(state) = accumulator.take() {
                    let finish_started = sampled.then(Instant::now);
                    let ordinal = returned.saturating_add(1);
                    let synthetic = state.finish(ordinal).map_err(EngineError::execution)?;
                    visitor(ExecutionRow::synthetic(synthetic))?;
                    if let Some(started) = finish_started {
                        merge_finish_ns = merge_finish_ns.saturating_add(elapsed_nanos(started));
                    }
                    returned = returned.saturating_add(1);
                }
            }

            if current_key.as_deref() != Some(keyed.key.as_ref()) {
                current_key = Some(keyed.key.clone());
                accumulator = runtime
                    .incremental_group_accumulator(keys)
                    .map_err(EngineError::execution)?;
                if let Some(state) = accumulator.as_mut() {
                    // Standard compact partials omit key values; seed them once
                    // from the canonical merge key. Legacy/custom runtimes may
                    // decline and continue using their payload-contained keys.
                    let _ = state
                        .seed_group_key(&keyed.key)
                        .map_err(EngineError::execution)?;
                }
            }

            let partial_started = sampled.then(Instant::now);
            let accumulator = accumulator
                .as_mut()
                .expect("incremental group capability was probed above");
            let merged = match &keyed.partial {
                GroupSpillPartial::Compact(payload) => accumulator
                    .merge_compact_partial(payload)
                    .map_err(EngineError::execution)?,
                GroupSpillPartial::Document(row) => accumulator
                    .merge_partial(row.document())
                    .map_err(EngineError::execution)?,
            };
            if !merged {
                if let GroupSpillPartial::Document(row) = &keyed.partial {
                    accumulator
                        .push(row.document())
                        .map_err(EngineError::execution)?;
                } else {
                    return Err(EngineError::execution(ExecutionError::evaluation(
                        "external group runtime cannot merge compact spilled state",
                    )));
                }
            }
            if let Some(started) = partial_started {
                merge_partial_ns = merge_partial_ns.saturating_add(elapsed_nanos(started));
            }

            let read_started = sampled.then(Instant::now);
            heads[index] =
                read_group_spill_record(&mut readers[index]).map_err(EngineError::execution)?;
            if let Some(started) = read_started {
                merge_read_ns = merge_read_ns.saturating_add(elapsed_nanos(started));
            }
        }

        if limit.map_or(true, |limit| (returned as usize) < limit) {
            if let Some(state) = accumulator.take() {
                let finish_started = Instant::now();
                let ordinal = returned.saturating_add(1);
                let synthetic = state.finish(ordinal).map_err(EngineError::execution)?;
                visitor(ExecutionRow::synthetic(synthetic))?;
                merge_finish_ns = merge_finish_ns.saturating_add(elapsed_nanos(finish_started));
                returned = returned.saturating_add(1);
            }
        }
        EXTERNAL_GROUP_MERGE_SAMPLES.fetch_add(merge_samples, AtomicOrdering::Relaxed);
        EXTERNAL_GROUP_MERGE_SELECT_NS.fetch_add(merge_select_ns, AtomicOrdering::Relaxed);
        EXTERNAL_GROUP_MERGE_PARTIAL_NS.fetch_add(merge_partial_ns, AtomicOrdering::Relaxed);
        EXTERNAL_GROUP_MERGE_READ_NS.fetch_add(merge_read_ns, AtomicOrdering::Relaxed);
        EXTERNAL_GROUP_MERGE_FINISH_NS.fetch_add(merge_finish_ns, AtomicOrdering::Relaxed);
        EXTERNAL_GROUP_PARTIALS_MERGED.fetch_add(merged_partials, AtomicOrdering::Relaxed);
        EXTERNAL_GROUP_MERGE_US.fetch_add(elapsed_micros(merge_started), AtomicOrdering::Relaxed);
        return Ok(returned);
    }

    // Compatibility fallback for custom runtimes that only implement the
    // legacy whole-group materializer.
    let (mut readers, mut heads) = group_merge_heads(runs)?;
    let mut current_key: Option<Arc<[u8]>> = None;
    let mut documents = Vec::<Arc<Document>>::new();
    let mut group_bytes = 0usize;
    let mut returned = 0u64;

    while let Some(index) = smallest_group_head(&heads) {
        let keyed = heads[index].take().expect("selected keyed head");
        if current_key
            .as_deref()
            .is_some_and(|key| key != keyed.key.as_ref())
        {
            emit_group_documents(runtime, keys, limit, &mut documents, &mut returned, visitor)?;
            group_bytes = 0;
            if limit.is_some_and(|limit| returned as usize >= limit) {
                break;
            }
        }
        current_key = Some(keyed.key.clone());
        let row = match keyed.partial {
            GroupSpillPartial::Document(row) => row,
            GroupSpillPartial::Compact(_) => {
                return Err(EngineError::execution(ExecutionError::evaluation(
                    "legacy group runtime cannot merge compact spilled state",
                )));
            }
        };
        let row_bytes = encode_execution_row(&row)
            .map_err(EngineError::execution)?
            .len();
        group_bytes = group_bytes.saturating_add(row_bytes);
        if group_bytes > group_budget {
            return Err(EngineError::execution(ExecutionError::evaluation(
                "one group exceeds the governed query working set; incremental group aggregation is required",
            )));
        }
        documents.push(Arc::new(row.document().clone()));
        heads[index] =
            read_group_spill_record(&mut readers[index]).map_err(EngineError::execution)?;
    }
    emit_group_documents(runtime, keys, limit, &mut documents, &mut returned, visitor)?;
    Ok(returned)
}

fn emit_group_documents(
    runtime: &dyn ExecutionRuntime,
    keys: &[crate::query::ExpressionFieldPath],
    limit: Option<usize>,
    documents: &mut Vec<Arc<Document>>,
    returned: &mut u64,
    visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>,
) -> EngineResult<()> {
    if documents.is_empty() || limit.is_some_and(|limit| *returned as usize >= limit) {
        documents.clear();
        return Ok(());
    }
    for synthetic in runtime
        .group_documents(keys, documents)
        .map_err(EngineError::execution)?
    {
        if limit.is_some_and(|limit| *returned as usize >= limit) {
            break;
        }
        visitor(ExecutionRow::synthetic(synthetic))?;
        *returned = returned.saturating_add(1);
    }
    documents.clear();
    Ok(())
}

fn spill_engine_error(error: io::Error) -> ExecutionError {
    ExecutionError::evaluation(format!("spill I/O failed: {error}"))
}

/// High-level engine error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineError {
    kind: EngineErrorKind,
}

impl EngineError {
    /// Creates an engine error.
    #[must_use]
    #[inline]
    pub const fn new(kind: EngineErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the detailed error category.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &EngineErrorKind {
        &self.kind
    }

    /// Wraps a semantic planning error.
    #[must_use]
    pub fn planning(error: PlannerError) -> Self {
        Self::new(EngineErrorKind::Planning(error))
    }

    /// Wraps a physical planning error.
    #[must_use]
    pub fn physical_planning(error: PhysicalPlanError) -> Self {
        Self::new(EngineErrorKind::PhysicalPlanning(error))
    }

    /// Wraps an execution error.
    #[must_use]
    pub fn execution(error: ExecutionError) -> Self {
        Self::new(EngineErrorKind::Execution(error))
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            EngineErrorKind::Planning(error) => {
                write!(formatter, "query planning failed: {error}")
            }
            EngineErrorKind::PhysicalPlanning(error) => {
                write!(formatter, "physical planning failed: {error}")
            }
            EngineErrorKind::Execution(error) => {
                write!(formatter, "query execution failed: {error}")
            }
        }
    }
}

impl StdError for EngineError {
    #[inline]
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.kind {
            EngineErrorKind::Planning(error) => Some(error),
            EngineErrorKind::PhysicalPlanning(error) => Some(error),
            EngineErrorKind::Execution(error) => Some(error),
        }
    }
}

/// Detailed engine error category.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EngineErrorKind {
    /// Semantic planning failed.
    Planning(PlannerError),

    /// Logical-to-physical lowering failed.
    PhysicalPlanning(PhysicalPlanError),

    /// Physical execution failed.
    Execution(ExecutionError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        query::{ExecutionResult, PhysicalSource},
        storage::{memory::MemoryStorage, CollectionId},
        Document,
    };

    #[derive(Debug, Default)]
    struct EmptyRuntime;

    impl ExecutionRuntime for EmptyRuntime {
        fn evaluate_predicate( &self, _expression: &crate::query::Expression, _document: &Document, ) -> ExecutionResult<bool> {
            panic!("runtime must not be called for an empty collection")
        }

        fn apply_set( &self, _assignments: &[crate::query::SetAssignment], _document: &Document, ) -> ExecutionResult<Arc<Document>> {
            panic!("runtime must not be called for an empty collection")
        }
    }

    #[derive(Debug, Default)]
    struct SourceOnlyLowerer;

    impl PlanLowerer for SourceOnlyLowerer {
        fn lower( &self, _logical: &LogicalPlan, physical_planner: &PhysicalPlanner, ) -> Result<PhysicalPlan, PhysicalPlanError> {
            let collection = CollectionId::parse("users").expect("test collection must be valid");
            physical_planner.plan_collection(collection).finish()
        }
    }

    #[test] fn engine_executes_an_existing_empty_physical_plan() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let runtime: Arc<dyn ExecutionRuntime> = Arc::new(EmptyRuntime); let lowerer: Arc<dyn PlanLowerer> = Arc::new(SourceOnlyLowerer); let engine = Engine::new(storage, runtime, lowerer); let physical = PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse("users").unwrap()), [], ) .unwrap(); let output = engine.execute_physical(&physical).unwrap(); assert!(output.is_empty()); assert!(!output.committed()); }
    #[test] fn governed_streaming_accepts_group_followed_by_sort_and_limit() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let runtime: Arc<dyn ExecutionRuntime> = Arc::new(EmptyRuntime); let lowerer: Arc<dyn PlanLowerer> = Arc::new(SourceOnlyLowerer); let engine = Engine::new(storage, runtime, lowerer); let group_field = crate::query::ExpressionFieldPath::new(["Article_Code"]).expect("valid group field"); let sort_field = crate::query::ExpressionFieldPath::new(["CA"]).expect("valid sort field"); let plan = PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse("data").unwrap()), [ PhysicalOperator::group([group_field]).unwrap(), PhysicalOperator::sort([crate::query::SortKey::descending(sort_field)]).unwrap(), PhysicalOperator::limit(10), ], ) .unwrap(); assert!(engine.supports_governed_blocking_streaming(&plan)); }
    #[test] fn engine_debug_does_not_require_trait_objects_to_be_debug() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let runtime: Arc<dyn ExecutionRuntime> = Arc::new(EmptyRuntime); let lowerer: Arc<dyn PlanLowerer> = Arc::new(SourceOnlyLowerer); let debug = format!("{:?}", Engine::new(storage, runtime, lowerer)); assert!(debug.starts_with("Engine")); }
    #[test] fn plan_cached_records_a_miss_then_a_hit() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let runtime: Arc<dyn ExecutionRuntime> = Arc::new(EmptyRuntime); let lowerer: Arc<dyn PlanLowerer> = Arc::new(SourceOnlyLowerer); let engine = Engine::new(storage, runtime, lowerer); let source = "from users"; let ast = crate::query::parse(source).expect("query parses"); let pipeline = PlannerPipeline::from_ast(source, &ast).expect("pipeline lowers"); engine.plan_cached(source, &pipeline).expect("first plan"); assert_eq!( engine.planner_cache_stats(), PlannerCacheStats { hits: 0, misses: 1, evictions: 0, } ); engine.plan_cached(source, &pipeline).expect("cached plan"); assert_eq!( engine.planner_cache_stats(), PlannerCacheStats { hits: 1, misses: 1, evictions: 0, } ); }
    #[test] fn invalidating_planner_cache_forces_replanning() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let runtime: Arc<dyn ExecutionRuntime> = Arc::new(EmptyRuntime); let lowerer: Arc<dyn PlanLowerer> = Arc::new(SourceOnlyLowerer); let engine = Engine::new(storage, runtime, lowerer); let source = "from users"; let ast = crate::query::parse(source).expect("query parses"); let pipeline = PlannerPipeline::from_ast(source, &ast).expect("pipeline lowers"); engine.plan_cached(source, &pipeline).expect("first plan"); engine.invalidate_planner_cache(); engine.plan_cached(source, &pipeline).expect("replanned"); let stats = engine.planner_cache_stats(); assert_eq!(stats.hits, 0); assert_eq!(stats.misses, 2); }
    #[test] fn engine_public_types_are_send_and_sync() { fn assert_send_and_sync<T: Send + Sync>() {} assert_send_and_sync::<Engine>(); assert_send_and_sync::<PlannedQuery>(); assert_send_and_sync::<QueryOutput>(); assert_send_and_sync::<EngineError>(); }
    #[test] fn memory_system_collection_exposes_global_and_class_rows() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let runtime: Arc<dyn ExecutionRuntime> = Arc::new(EmptyRuntime); let lowerer: Arc<dyn PlanLowerer> = Arc::new(SourceOnlyLowerer); let governor = MemoryGovernor::with_limit(1_024); let _held = governor .reserve(crate::memory::MemoryClass::Query, 128) .unwrap(); let engine = Engine::new(storage, runtime, lowerer).with_memory_governor(governor); let physical = PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse(vcollections::MEMORY).unwrap()), [], ) .unwrap(); let output = engine.execute_physical(&physical).unwrap(); assert_eq!(output.len(), 7); assert!(output.rows().iter().any(|row| { row.document().get("scope") == Some(&Value::from("global")) && row.document().get("current_bytes") == Some(&Value::from(128_u64)) })); assert!(output.rows().iter().any(|row| { row.document().get("class") == Some(&Value::from("query")) && row.document().get("current_bytes") == Some(&Value::from(128_u64)) })); }
    #[test] fn memory_events_system_collection_exposes_bounded_journal() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let runtime: Arc<dyn ExecutionRuntime> = Arc::new(EmptyRuntime); let lowerer: Arc<dyn PlanLowerer> = Arc::new(SourceOnlyLowerer); let governor = MemoryGovernor::with_limit_and_event_capacity(8 * 1024 * 1024, 8); let reservation = governor .reserve( crate::memory::MemoryClass::Import, crate::memory::MEMORY_EVENT_MIN_BYTES, ) .unwrap(); drop(reservation); let engine = Engine::new(storage, runtime, lowerer).with_memory_governor(governor); let physical = PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse(vcollections::MEMORY_EVENTS).unwrap()), [], ) .unwrap(); let output = engine.execute_physical(&physical).unwrap(); assert_eq!(output.len(), 2); let kinds = output .rows() .iter() .filter_map(|row| row.document().get("kind")) .cloned() .collect::<Vec<_>>(); assert!(kinds.contains(&Value::from("reserved"))); assert!(kinds.contains(&Value::from("released"))); }
    #[test] fn sort_distinct_limit_has_a_bounded_streaming_executor() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let runtime: Arc<dyn ExecutionRuntime> = Arc::new(EmptyRuntime); let lowerer: Arc<dyn PlanLowerer> = Arc::new(SourceOnlyLowerer); let engine = Engine::new(storage, runtime, lowerer); let operators = vec![ PhysicalOperator::sort([crate::query::SortKey::ascending( crate::query::ExpressionFieldPath::new(["value"]).unwrap(), )]) .unwrap(), PhysicalOperator::distinct([]).unwrap(), PhysicalOperator::limit(10), ]; let plan = PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse("data").unwrap()), operators, ) .unwrap(); assert!(engine.supports_governed_blocking_streaming(&plan)); }
}
