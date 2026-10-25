//! Plans run whole: a read collects its rows, a write (`set`, `delete`,
//! `load`, `into`) streams the rows of its read into one transaction.
//!
//! Both read through the streaming executors: there is no materialized
//! executor. A write reads with the stages before its first writing stage,
//! then runs the row-local stages from it on each row and writes the rows
//! they keep: `set` and `load` replace a changed stored row, `delete` deletes
//! a stored row, `into` inserts every row into another collection. What the
//! transaction and the returned rows hold is charged to the query's memory.
#![cfg_attr(rustfmt, rustfmt_skip)]
use super::*;
use super::changes;
use crate::query::{execute_insert, execute_streaming_load, insert_document, into_target, streaming_load_specification, DocumentScope};
use crate::storage::{CommitResult, UuidV7Generator, VersionPrecondition};

/// What a transaction keeps of each pending write besides a written document (its staged entry, its log entry,
/// its planned change): what a delete holds. A written document takes about twice its encoded size in memory.
/// Measured on an import of a million rows (ideas/data-import-rust.md): counted at its encoded size alone, a
/// write held four times its share of the query's memory and its transaction never moved out of it.
const PENDING_RECORD_BYTES: usize = 320;

/// Whether a write still sees the stored rows after `operator`: it keeps,
/// drops, reorders or rewrites them but neither merges nor expands them.
fn keeps_stored_rows(operator: &PhysicalOperator) -> bool {
    match operator {
        PhysicalOperator::Filter { .. } | PhysicalOperator::Sort { .. } | PhysicalOperator::Limit { .. } | PhysicalOperator::Skip { .. } | PhysicalOperator::Select { .. } | PhysicalOperator::Lookup { .. } => true,
        PhysicalOperator::Custom { .. } => !matches!(operator.execution_properties().cardinality, crate::query::CardinalityEffect::Expand),
        _ => false,
    }
}

/// The collection a terminal `into` of `physical` writes: a user
/// collection other than its source.
fn into_collection(physical: &PhysicalPlan, target: &str) -> EngineResult<CollectionId> {
    let target = CollectionId::parse(target).map_err(storage_engine_error)?;
    if target.as_str().starts_with('_') && !target.as_str().starts_with(crate::view::CACHE_PREFIX) {
        return Err(unsupported("into", "into cannot target a system collection"));
    }
    if &target == physical.source().collection() {
        return Err(unsupported("into", "into target must differ from the source collection"));
    }
    Ok(target)
}

fn unsupported(operator: impl Into<Arc<str>>, reason: impl Into<Arc<str>>) -> EngineError {
    EngineError::execution(ExecutionError::unsupported_operator(operator, reason))
}

impl Engine {
    /// Executes an already validated physical plan.
    pub fn execute_physical(&self, physical: &PhysicalPlan) -> EngineResult<ExecutionOutput> {
        self.run(physical, None, false)
    }

    /// Executes a physical plan under a trusted Place or `AppInstance` document scope.
    pub fn execute_physical_scoped(&self, physical: &PhysicalPlan, scope: &DocumentScope) -> EngineResult<ExecutionOutput> {
        let collection = physical.source().collection().as_str();
        // The rows a Place cached for materialized views are scoped like any
        // write: read in its scope, including to compute a materialized view
        // from another one (`on _cache.… | union … | into _cache.…`).
        if collection.starts_with(crate::view::CACHE_PREFIX) {
            return self.run(physical, Some(scope), false);
        }
        if collection.starts_with('_') {
            // A Place reads its own views, from any of its App instances
            // (they are written by `define view` only), and the constant rows
            // of `values […]`.
            return match scope.read_plan(physical, true).filter(|_| collection == crate::view::VIEWS_COLLECTION || collection.starts_with(crate::view::VALUES_PREFIX)) {
                Some(plan) => self.run(&plan, None, false),
                None => Err(unsupported("scoped-system-collection", "Place-scoped queries cannot target system collections")),
            };
        }
        self.run(physical, Some(scope), false)
    }

    /// Executes a physical plan while suppressing streaming-load result rows.
    pub fn execute_physical_compact(&self, physical: &PhysicalPlan) -> EngineResult<ExecutionOutput> {
        self.run(physical, None, true)
    }

    fn run(&self, physical: &PhysicalPlan, scope: Option<&DocumentScope>, compact: bool) -> EngineResult<ExecutionOutput> {
        if let Some(view) = self.temporal_view(physical)? {
            return view.run(physical, scope, compact);
        }
        if let Some(storage) = self.system_collection_storage(physical)? {
            return self.view(Arc::new(storage)).run(physical, scope, compact);
        }
        // `into` only reads its source: it may copy constant rows elsewhere.
        if physical.is_write() && into_target(physical).is_none() && physical.source().collection().as_str().starts_with(crate::view::VALUES_PREFIX) {
            return Err(unsupported("constant-collection", "the rows of `values […]` are constant"));
        }
        let started = Instant::now();
        let output = if physical.is_write() {
            // The write is attributed to its Place, for the caches of materialized views.
            let written = into_target(physical).unwrap_or(physical.source().collection().as_str()).to_owned();
            let before = changes::Changes::changed(self.storage.as_ref(), &written);
            let output = self.write(physical, scope, compact)?;
            self.changes.record(self.storage.as_ref(), &written, scope.map(DocumentScope::place_id), before);
            output
        } else { self.read(physical, scope)? };
        // A streaming read observes itself.
        if !physical.is_memory_streaming() {
            self.indexing.observe(QueryObservation::from_execution(physical, output.statistics(), started.elapsed()));
        }
        Ok(output)
    }

    /// Streams the rows of the read-only `physical`, restricted to `scope`,
    /// to `visitor`, whose own error comes back as it is.
    pub(super) fn stream_rows(&self, physical: &PhysicalPlan, scope: Option<&DocumentScope>, mut visitor: impl FnMut(ExecutionRow) -> EngineResult<()>) -> EngineResult<ExecutionStatistics> {
        let scoped = scope.map(|scope| scope.read_plan(physical, false).ok_or_else(|| unsupported("scope", "this plan cannot be restricted to a Place"))).transpose()?;
        let physical = scoped.as_ref().unwrap_or(physical);
        let mut failure = None;
        let mut visit = |row| visitor(row).map_err(|error| {
            let message = error.to_string();
            failure = Some(error);
            EngineError::execution(ExecutionError::evaluation(message))
        });
        let streamed = if physical.is_memory_streaming() {
            self.stream_read_pipeline(physical, &mut |stored| visit(ExecutionRow::from_stored(stored)))
        } else {
            self.stream_governed_blocking_pipeline(physical, &mut visit)
        };
        match (streamed, failure) {
            (Err(_), Some(error)) => Err(error),
            (streamed, _) => streamed?.ok_or_else(|| unsupported("pipeline", "query plan contains a blocking stage sequence without a bounded executor")),
        }
    }

    fn read(&self, physical: &PhysicalPlan, scope: Option<&DocumentScope>) -> EngineResult<ExecutionOutput> {
        let memory = QueryMemory::new(&self.memory_governor);
        let stage = memory.stage();
        let (mut rows, mut held) = (Vec::new(), 0usize);
        // System collections stay readable when memory runs short.
        let charged = !physical.source().collection().as_str().starts_with('_');
        let statistics = self.stream_rows(physical, scope, |row| {
            held = held.saturating_add(execution_row_working_bytes(&row).map_err(EngineError::execution)?);
            if charged && !stage.holds(held) {
                return Err(EngineError::execution(ExecutionError::evaluation("the result rows exceed the governed query working set")));
            }
            rows.push(row);
            Ok(())
        })?;
        Ok(ExecutionOutput::new(rows, statistics, None))
    }

    fn write(&self, physical: &PhysicalPlan, scope: Option<&DocumentScope>, compact: bool) -> EngineResult<ExecutionOutput> {
        if let Some(load) = streaming_load_specification(physical) {
            return execute_streaming_load(self.storage.as_ref(), &self.runtime, physical, load, &self.memory_governor, compact, scope).map_err(EngineError::execution);
        }
        if let Some(document) = insert_document(physical) {
            return execute_insert(self.storage.as_ref(), &self.runtime, physical, document, scope).map_err(EngineError::execution);
        }
        let memory = QueryMemory::new(&self.memory_governor);
        let mut transaction = self.storage.begin().map_err(storage_engine_error)?;
        let (rows, statistics, written) = self.write_in(transaction.as_mut(), &memory.stage(), &mut (0, 0), physical, scope)?;
        // An `into` that inserts nothing commits nothing.
        let commit = if into_target(physical).is_some() && written == 0 { None } else { Some(transaction.commit().map_err(storage_engine_error)?) };
        let statistics = statistics.with_commit(usize_to_u64_saturating(rows.len()), commit);
        Ok(ExecutionOutput::new(rows, statistics, commit))
    }

    /// Runs the writes `plans`, each in its scope, in one transaction: all
    /// of them are committed, or none. Each reads the stored rows as they
    /// were before the transaction. Rows given by value (`insert`) and
    /// past states are not accepted.
    pub fn execute_together(&self, plans: &[(PhysicalPlan, Option<DocumentScope>)]) -> EngineResult<CommitResult> {
        let memory = QueryMemory::new(&self.memory_governor);
        let (stage, mut held) = (memory.stage(), (0, 0));
        let mut transaction = self.storage.begin().map_err(storage_engine_error)?;
        let mut written = Vec::new();
        for (physical, scope) in plans {
            if !physical.is_write() || streaming_load_specification(physical).is_some() || insert_document(physical).is_some() || self.temporal_view(physical)?.is_some() || physical.source().collection().as_str().starts_with('_') {
                return Err(unsupported("together", "only writes reading stored user collections run together"));
            }
            let collection = into_target(physical).unwrap_or(physical.source().collection().as_str()).to_owned();
            written.push((changes::Changes::changed(self.storage.as_ref(), &collection), collection, scope.as_ref().map(DocumentScope::place_id)));
            self.write_in(transaction.as_mut(), &stage, &mut held, physical, scope.as_ref())?;
        }
        let commit = transaction.commit().map_err(storage_engine_error)?;
        for (before, collection, place) in written {
            self.changes.record(self.storage.as_ref(), &collection, place, before);
        }
        Ok(commit)
    }

    /// Streams the rows `physical` writes into `transaction`. `held` counts
    /// the bytes its pending writes and returned rows hold: past the query's
    /// memory, the transaction moves its pending writes out of memory, or
    /// the write fails.
    fn write_in(&self, transaction: &mut dyn StorageTransaction, stage: &StageMemory<'_>, held: &mut (usize, usize), physical: &PhysicalPlan, scope: Option<&DocumentScope>) -> EngineResult<(Vec<ExecutionRow>, ExecutionStatistics, u64)> {
        let operators = physical.operators();
        let into = into_target(physical).map(|target| into_collection(physical, target)).transpose()?;
        let first = if into.is_some() { operators.len() - 1 } else { operators.iter().position(|operator| operator.execution_properties().writes()).expect("a write plan writes") };
        let (prefix, suffix) = operators.split_at(first);
        let suffix = &suffix[usize::from(into.is_some())..];
        if let Some(operator) = suffix.iter().find(|operator| !is_row_operator(operator) && !matches!(operator, PhysicalOperator::Set { .. } | PhysicalOperator::Delete | PhysicalOperator::Load { .. })) {
            return Err(unsupported(operator.name(), "a write cannot be followed by this stage"));
        }
        if let (None, Some(operator)) = (&into, prefix.iter().find(|operator| !keeps_stored_rows(operator))) {
            return Err(unsupported(operator.name(), "a write cannot follow this stage: its rows are not stored documents"));
        }
        let read = PhysicalPlan::new(physical.source().clone(), prefix.iter().cloned()).map_err(|error| EngineError::execution(ExecutionError::evaluation(error.to_string())))?;
        if read.is_write() {
            return Err(unsupported("into", "into cannot follow a mutating stage"));
        }
        let delete = suffix.iter().any(|operator| matches!(operator, PhysicalOperator::Delete));
        let collection = into.as_ref().unwrap_or(physical.source().collection());
        let ids = UuidV7Generator::new();
        let mut chain = StreamingRowChain::new(suffix);
        let (mut rows, mut written, failure) = (Vec::new(), 0u64, std::cell::Cell::new(None));
        let name = if into.is_some() { "into" } else { operators[first].name() };
        let mut write = |mut row: ExecutionRow| -> Result<(), StorageError> {
            let bytes = if delete { PENDING_RECORD_BYTES } else { execution_row_encoded_len(&row).saturating_mul(2).saturating_add(PENDING_RECORD_BYTES) };
            held.0 = held.0.saturating_add(bytes);
            if !stage.fits(held.0 + held.1) && transaction.spill()? {
                held.0 = 0;
            }
            if !stage.holds(held.0 + held.1) {
                failure.set(Some(EngineError::execution(ExecutionError::evaluation(format!("{name}: the written rows exceed the governed query working set")))));
                return Err(StorageError::backend(name));
            }
            let precondition = VersionPrecondition::Exact(row.version());
            let enforce = |row: &ExecutionRow| scope.map_or_else(|| Ok(row.shared_document()), |scope| scope.enforce(collection.as_str(), row.document()).map_err(|message| {
                failure.set(Some(EngineError::execution(ExecutionError::evaluation(message))));
                StorageError::backend(name)
            }));
            if into.is_some() {
                let document = enforce(&row)?;
                transaction.insert(collection, ids.next_id(), document)?;
                written += 1;
                return Ok(());
            } else if delete {
                if row.is_stored() {
                    transaction.delete(collection, row.id(), precondition)?;
                }
                return Ok(());
            } else if row.changed() && row.is_stored() {
                let document = enforce(&row)?;
                row.committed(transaction.replace(collection, row.id(), document, precondition)?.into_stored());
            }
            // The returned rows stay in memory.
            held.1 += bytes;
            rows.push(row);
            Ok(())
        };
        let statistics = self.stream_rows(&read, scope, |row| {
            self.drive_streaming_row(&mut chain, 0, row, None, &mut write).map(drop).map_err(|error| failure.take().unwrap_or_else(|| storage_engine_error(error)))
        })?;
        drop(write);
        ensure_exact_rows(suffix, &chain.limit_remaining)?;
        let statistics = ExecutionStatistics::streamed_with_strategies(statistics.scanned(), statistics.filtered().saturating_add(chain.filtered), 0, statistics.strategies());
        Ok((rows, statistics, written))
    }
}
