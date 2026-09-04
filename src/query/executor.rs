//! Physical query plan execution.

use std::{cmp::Ordering, error::Error as StdError, fmt, io, mem::size_of, sync::Arc};

use crate::{
    compare,
    helpers::{document_scope_matches, enforce_document_scope, PLACE_SCOPE_FIELD},
    memory::{MemoryClass, MemoryGovernor, MemoryReservation, MemoryReservationError},
    model::{CoercionPolicy, Document, Number, Value},
    spill::{SpillEngine, SpillRun, SpillRunReader},
    storage::{
        CommitResult, DocumentId, DocumentVersion, ProjectedValueRef, ScanOptions, StorageEngine,
        StorageError, StorageMutation, StorageRead, StorageTransaction, StoredDocument,
        VersionPrecondition,
    },
};

use super::{
    Expression, ExpressionFieldPath, ExpressionFieldResolver, PhysicalAccess, PhysicalLoadMode,
    PhysicalOperator, PhysicalPlan, PhysicalSubPipeline, SetAssignment, SortKey, StageName,
};

use super::logical_plan::{InsertDocument as LogicalInsertDocument, PivotSpecification};

/// Trusted document scope applied below query syntax.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocumentScope {
    place_id: Arc<str>,
    app_instance_id: Option<Arc<str>>,
}

impl DocumentScope {
    /// Creates a Place + AppInstance scope.
    #[must_use]
    pub fn new(place_id: impl Into<Arc<str>>, app_instance_id: impl Into<Arc<str>>) -> Self {
        Self {
            place_id: place_id.into(),
            app_instance_id: Some(app_instance_id.into()),
        }
    }

    /// Creates a Place-wide scope without restricting documents to one AppInstance.
    #[must_use]
    pub fn for_place(place_id: impl Into<Arc<str>>) -> Self {
        Self {
            place_id: place_id.into(),
            app_instance_id: None,
        }
    }

    fn matches(&self, document: &Document) -> bool {
        match self.app_instance_id.as_deref() {
            Some(app_instance_id) => {
                document_scope_matches(document, &self.place_id, app_instance_id)
            }
            None => document.get(PLACE_SCOPE_FIELD) == Some(&Value::from(self.place_id.as_ref())),
        }
    }

    fn enforce(&self, document: &Document) -> Arc<Document> {
        match self.app_instance_id.as_deref() {
            Some(app_instance_id) => Arc::new(enforce_document_scope(
                document,
                &self.place_id,
                app_instance_id,
            )),
            None => {
                let mut scoped = document.clone();
                scoped.insert(PLACE_SCOPE_FIELD, self.place_id.as_ref());
                Arc::new(scoped)
            }
        }
    }
}

/// Result returned by execution operations.
pub type ExecutionResult<T> = std::result::Result<T, ExecutionError>;

/// Storage-ready document prepared by the runtime for an `insert` operator.
#[derive(Clone, Debug)]
pub struct PreparedInsertDocument {
    id: DocumentId,
    document: Arc<Document>,
}

impl PreparedInsertDocument {
    /// Creates a storage-ready insertion payload.
    #[must_use]
    #[inline]
    pub const fn new(id: DocumentId, document: Arc<Document>) -> Self {
        Self { id, document }
    }

    /// Returns the immutable identifier.
    #[must_use]
    pub const fn id(&self) -> &DocumentId {
        &self.id
    }

    /// Returns the document payload.
    #[must_use]
    pub fn document(&self) -> &Document {
        &self.document
    }

    /// Consumes the payload.
    #[must_use]
    pub fn into_parts(self) -> (DocumentId, Arc<Document>) {
        (self.id, self.document)
    }
}

/// One synthetic document produced by a set-level operator.
///
/// Synthetic rows are not tied to a committed source document. The supplied
/// identifier only needs to be deterministic within one execution output.
#[derive(Clone, Debug)]
pub struct SyntheticDocument {
    id: DocumentId,
    document: Arc<Document>,
}

impl SyntheticDocument {
    /// Creates a synthetic result document.
    #[must_use]
    #[inline]
    pub const fn new(id: DocumentId, document: Arc<Document>) -> Self {
        Self { id, document }
    }

    /// Returns its deterministic result identifier.
    #[must_use]
    pub const fn id(&self) -> &DocumentId {
        &self.id
    }

    /// Returns its document.
    #[must_use]
    pub fn document(&self) -> &Document {
        &self.document
    }

    /// Consumes the synthetic payload.
    #[must_use]
    pub fn into_parts(self) -> (DocumentId, Arc<Document>) {
        (self.id, self.document)
    }
}

/// One document mutation prepared for a streaming-load chunk.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum StreamingLoadMutation {
    /// Insert a new document.
    Insert {
        id: DocumentId,
        document: Arc<Document>,
    },

    /// Replace an existing document using an explicit optimistic-concurrency
    /// precondition.
    Replace {
        id: DocumentId,
        document: Arc<Document>,
        precondition: VersionPrecondition,
    },
}

impl StreamingLoadMutation {
    #[must_use]
    pub fn insert(id: DocumentId, document: Arc<Document>) -> Self {
        Self::Insert { id, document }
    }

    #[must_use]
    pub fn replace(
        id: DocumentId,
        document: Arc<Document>,
        precondition: VersionPrecondition,
    ) -> Self {
        Self::Replace {
            id,
            document,
            precondition,
        }
    }
}

/// Read-only documents produced by a lookup sub-pipeline.
#[derive(Clone, Debug)]
pub struct LookupDocuments {
    documents: Arc<[Arc<Document>]>,
}

impl LookupDocuments {
    #[must_use]
    pub fn new<I>(documents: I) -> Self
    where
        I: IntoIterator<Item = Arc<Document>>,
    {
        Self {
            documents: Arc::from(documents.into_iter().collect::<Vec<_>>()),
        }
    }

    #[must_use]
    pub fn documents(&self) -> &[Arc<Document>] {
        &self.documents
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.documents.len()
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }
}

/// Document-level and set-level runtime used by the executor.
///
/// Default implementations report unsupported operators so a minimal runtime
/// only needs to implement predicate evaluation and `set`.
/// Incremental state for one grouped key.
///
/// Implementations retain only aggregate state, never the complete source rows.
pub trait IncrementalGroupAccumulator: Send {
    /// Consumes one document belonging to this group.
    fn push(&mut self, document: &Document) -> ExecutionResult<()>;

    /// Consumes the physical input values resolved by `group_field_layout`.
    ///
    /// The default reports that direct value aggregation is unavailable so
    /// runtimes opt in explicitly without changing legacy group semantics.
    fn push_projected_values(&mut self, _values: &[Option<Value>]) -> ExecutionResult<bool> {
        Ok(false)
    }

    /// Consumes storage-borrowed scalar values when the runtime can aggregate
    /// without forcing per-row owned materialization.
    fn push_projected_value_refs(
        &mut self,
        _values: &[Option<crate::storage::ProjectedValueRef<'_>>],
        _source_slots: &[usize],
    ) -> ExecutionResult<bool> {
        Ok(false)
    }

    /// Materializes a compact mergeable partial state for external spill.
    ///
    /// Runtimes that implement this capability let the engine spill one bounded
    /// aggregate state per group instead of retaining complete source rows.
    fn finish_partial(
        self: Box<Self>,
        _ordinal: u64,
    ) -> ExecutionResult<Option<SyntheticDocument>> {
        Ok(None)
    }

    /// Serializes the aggregate state into a runtime-defined compact payload.
    ///
    /// External grouping prefers this representation over synthetic documents
    /// so high-cardinality groups do not repeat field names and row metadata in spill.
    fn compact_partial(&self) -> ExecutionResult<Option<Vec<u8>>> {
        Ok(None)
    }

    /// Seeds grouping-key values from the engine's canonical encoded group key.
    ///
    /// Compact external aggregation uses this to avoid serializing grouping keys
    /// twice (once in the merge key and again in the aggregate payload).
    fn seed_group_key(&mut self, _encoded_key: &[u8]) -> ExecutionResult<bool> {
        Ok(false)
    }

    /// Merges a payload produced by `compact_partial`.
    fn merge_compact_partial(&mut self, _payload: &[u8]) -> ExecutionResult<bool> {
        Ok(false)
    }

    /// Merges a compact state previously produced by `finish_partial`.
    fn merge_partial(&mut self, _document: &Document) -> ExecutionResult<bool> {
        Ok(false)
    }

    /// Materializes the final synthetic grouped row.
    fn finish(self: Box<Self>, ordinal: u64) -> ExecutionResult<SyntheticDocument>;
}

pub trait ExecutionRuntime: Send + Sync {
    /// Evaluates a filter predicate.
    fn evaluate_predicate(
        &self,
        expression: &Expression,
        document: &Document,
    ) -> ExecutionResult<bool>;

    /// Evaluates a predicate directly from a field resolver when supported.
    fn evaluate_resolved_predicate(
        &self,
        expression: &Expression,
        resolver: &dyn ExpressionFieldResolver<Value>,
    ) -> ExecutionResult<bool> {
        let _ = (expression, resolver);
        Err(ExecutionError::unsupported_operator(
            "filter",
            "resolved predicate evaluation runtime is not configured",
        ))
    }

    /// Applies one complete `set` operator.
    fn apply_set(
        &self,
        assignments: &[SetAssignment],
        document: &Document,
    ) -> ExecutionResult<Arc<Document>>;

    /// Evaluates a predicate inside a lookup sub-pipeline.
    ///
    /// `outer` is the document currently being enriched. `inner_alias` is the
    /// alias declared by the lookup header, when present. The default
    /// implementation evaluates the predicate against the inner document only.
    fn evaluate_lookup_predicate(
        &self,
        expression: &Expression,
        outer: &Document,
        inner_alias: Option<&str>,
        inner: &Document,
    ) -> ExecutionResult<bool> {
        let _ = (outer, inner_alias);
        self.evaluate_predicate(expression, inner)
    }

    /// Attaches the result of a lookup to the outer document.
    fn apply_lookup(
        &self,
        into: &str,
        outer: &Document,
        matches: &LookupDocuments,
    ) -> ExecutionResult<Arc<Document>> {
        let _ = (outer, matches);

        Err(ExecutionError::unsupported_operator(
            "lookup",
            format!("lookup target {into:?} has no runtime implementation"),
        ))
    }

    /// Converts ordered streaming-load chunks into concrete storage mutations.
    ///
    /// The runtime owns parsing, validation, merge semantics, and optimistic
    /// concurrency decisions. The executor only applies the returned mutations
    /// atomically inside the current transaction.
    fn prepare_streaming_load(
        &self,
        collection: &crate::storage::CollectionId,
        storage: &dyn StorageRead,
        mode: PhysicalLoadMode,
        chunks: &[Arc<str>],
    ) -> ExecutionResult<Vec<StreamingLoadMutation>> {
        let _ = (collection, storage, chunks);

        Err(ExecutionError::unsupported_operator(
            "streaming-load",
            format!("streaming load mode {mode} has no runtime implementation"),
        ))
    }

    /// Applies a load operator.
    fn apply_load(&self, target: &str, document: &Document) -> ExecutionResult<Arc<Document>> {
        let _ = document;

        Err(ExecutionError::unsupported_operator(
            "load",
            format!("load target {target:?} has no runtime implementation"),
        ))
    }

    /// Compares two documents using an ordered list of sort keys.
    fn compare_documents(
        &self,
        keys: &[SortKey],
        left: &Document,
        right: &Document,
    ) -> ExecutionResult<Ordering> {
        let _ = (keys, left, right);

        Err(ExecutionError::unsupported_operator(
            "sort",
            "document comparison has no runtime implementation",
        ))
    }

    /// Reports whether the runtime can compare rows represented only by sort-key values.
    ///
    /// This optional capability lets blocking sort operators remain on the standard
    /// projected-value access vector and hydrate documents only after ordering.
    fn supports_projected_sort(&self) -> bool {
        false
    }

    /// Compares two rows whose values are aligned one-for-one with `keys`.
    ///
    /// Returning `None` asks the engine to preserve the full-document path.
    fn compare_projected_values(
        &self,
        keys: &[SortKey],
        left: &[Option<Value>],
        right: &[Option<Value>],
    ) -> ExecutionResult<Option<Ordering>> {
        let _ = (keys, left, right);
        Ok(None)
    }

    /// Projects a document to the requested fields.
    fn apply_select(
        &self,
        fields: &[ExpressionFieldPath],
        document: &Document,
    ) -> ExecutionResult<Arc<Document>> {
        let _ = (fields, document);

        Err(ExecutionError::unsupported_operator(
            "select",
            "document projection has no runtime implementation",
        ))
    }

    /// Returns a deterministic equality key for `distinct`.
    ///
    /// An empty field list means the complete document.
    fn distinct_key(
        &self,
        fields: &[ExpressionFieldPath],
        document: &Document,
    ) -> ExecutionResult<Arc<[u8]>> {
        let _ = (fields, document);

        Err(ExecutionError::unsupported_operator(
            "distinct",
            "distinct-key extraction has no runtime implementation",
        ))
    }

    /// Reports whether the runtime can encode ordinary distinct keys into a
    /// reusable caller-owned buffer. This avoids one heap allocation per source
    /// row while preserving the runtime as owner of canonical key semantics.
    fn supports_buffered_distinct(&self) -> bool {
        false
    }

    /// Encodes the same key as `distinct_key` into `key` and returns `true` when
    /// the capability is available. Returning `false` preserves the Arc-returning
    /// compatibility path.
    fn write_distinct_key(
        &self,
        fields: &[ExpressionFieldPath],
        document: &Document,
        key: &mut Vec<u8>,
    ) -> ExecutionResult<bool> {
        let _ = (fields, document, key);
        Ok(false)
    }

    /// Reports whether the runtime can encode explicit-field distinct keys directly
    /// from the standard projected-value access vector.
    fn supports_projected_distinct(&self) -> bool {
        false
    }

    /// Encodes an explicit-field distinct key from storage-borrowed projected values.
    ///
    /// `indexes` maps `fields` into `values`. Implementations write into the reusable
    /// caller-owned `key` buffer and return `true`; returning `false` selects the
    /// established full-document fallback.
    fn write_projected_distinct_key(
        &self,
        fields: &[ExpressionFieldPath],
        values: &[Option<ProjectedValueRef<'_>>],
        indexes: &[usize],
        key: &mut Vec<u8>,
    ) -> ExecutionResult<bool> {
        let _ = (fields, values, indexes, key);
        Ok(false)
    }

    /// Creates the single result document emitted by `count`.
    fn count_document(&self, alias: &str, count: u64) -> ExecutionResult<Arc<Document>> {
        let _ = count;

        Err(ExecutionError::unsupported_operator(
            "count",
            format!("count result alias {alias:?} has no runtime implementation"),
        ))
    }

    /// Groups the complete row set by the requested fields.
    ///
    /// The runtime owns the representation of each grouped result document.
    /// Creates an incremental accumulator for one group when the runtime can
    /// lower the group semantics to bounded aggregate state.
    ///
    /// Returning `None` preserves compatibility with runtimes that only expose
    /// the legacy materializing group handler.
    fn incremental_group_accumulator(
        &self,
        keys: &[ExpressionFieldPath],
    ) -> ExecutionResult<Option<Box<dyn IncrementalGroupAccumulator>>> {
        let _ = keys;
        Ok(None)
    }

    fn group_documents(
        &self,
        keys: &[ExpressionFieldPath],
        documents: &[Arc<Document>],
    ) -> ExecutionResult<Vec<SyntheticDocument>> {
        let _ = (keys, documents);

        Err(ExecutionError::unsupported_operator(
            "group",
            "group aggregation has no runtime implementation",
        ))
    }

    /// Converts one validated logical insert document into the storage-specific
    /// document representation and chooses its immutable identifier.
    fn prepare_insert(
        &self,
        document: &LogicalInsertDocument,
    ) -> ExecutionResult<PreparedInsertDocument> {
        let _ = document;

        Err(ExecutionError::unsupported_operator(
            "insert",
            "typed insert document has no runtime implementation",
        ))
    }

    /// Applies a pivot to the complete intermediate document set.
    ///
    /// The runtime owns value extraction, aggregate semantics, column naming,
    /// and the representation of synthetic pivot result documents.
    fn pivot_documents(
        &self,
        specification: &PivotSpecification,
        documents: &[Arc<Document>],
    ) -> ExecutionResult<Vec<SyntheticDocument>> {
        let _ = (specification, documents);

        Err(ExecutionError::unsupported_operator(
            "pivot",
            "pivot aggregation has no runtime implementation",
        ))
    }

    /// Applies a custom operator.
    fn apply_custom(
        &self,
        stage: &StageName,
        arguments: &str,
        writes: bool,
        document: &Document,
    ) -> ExecutionResult<CustomOperatorResult> {
        let _ = (arguments, writes, document);

        Err(ExecutionError::unsupported_operator(
            stage.as_str(),
            "custom operator has no runtime implementation",
        ))
    }
}

/// Result produced by a custom row operator.
#[derive(Clone, Debug)]
pub enum CustomOperatorResult {
    /// Keep the current row unchanged.
    Keep,

    /// Remove the current row from the result stream.
    Discard,

    /// Replace the current row document.
    Replace(Arc<Document>),

    /// Replace the current row by zero or more documents.
    Expand(Vec<Arc<Document>>),
}

/// Physical plan executor governed by a shared memory budget.
#[derive(Clone, Debug)]
pub struct Executor {
    memory_governor: MemoryGovernor,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    /// Creates an executor with an unlimited governor.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::new_governed(MemoryGovernor::unlimited())
    }

    /// Creates an executor sharing the supplied memory governor.
    #[must_use]
    pub const fn new_governed(memory_governor: MemoryGovernor) -> Self {
        Self { memory_governor }
    }

    /// Returns the governor used by execution operators.
    #[must_use]
    pub const fn memory_governor(&self) -> &MemoryGovernor {
        &self.memory_governor
    }

    /// Rebinds this executor to another shared governor.
    #[must_use]
    pub fn with_memory_governor(mut self, memory_governor: MemoryGovernor) -> Self {
        self.memory_governor = memory_governor;
        self
    }

    /// Executes a physical plan and suppresses rows for streaming imports.
    ///
    /// This avoids constructing a result vector that callers immediately
    /// discard when only mutation counters are needed.
    pub fn execute_compact(
        &self,
        storage: &dyn StorageEngine,
        runtime: &dyn ExecutionRuntime,
        plan: &PhysicalPlan,
    ) -> ExecutionResult<ExecutionOutput> {
        if let Some((mode, chunks)) = streaming_load_specification(plan) {
            return execute_streaming_load_compact(
                storage,
                runtime,
                plan,
                mode,
                chunks,
                &self.memory_governor,
            );
        }

        self.execute(storage, runtime, plan)
    }

    /// Executes a physical plan.
    pub fn execute(
        &self,
        storage: &dyn StorageEngine,
        runtime: &dyn ExecutionRuntime,
        plan: &PhysicalPlan,
    ) -> ExecutionResult<ExecutionOutput> {
        self.execute_scoped(storage, runtime, plan, None)
    }

    /// Executes a physical plan under an optional trusted document scope.
    pub fn execute_scoped(
        &self,
        storage: &dyn StorageEngine,
        runtime: &dyn ExecutionRuntime,
        plan: &PhysicalPlan,
        scope: Option<&DocumentScope>,
    ) -> ExecutionResult<ExecutionOutput> {
        if scope.is_some() && plan.is_streaming_load() {
            return Err(ExecutionError::unsupported_operator(
                "scoped-streaming-load",
                "streaming load is not yet supported inside an App Instance scope",
            ));
        }
        if plan.is_write() {
            self.execute_write(storage, runtime, plan, scope)
        } else {
            self.execute_read(storage, runtime, plan, scope)
        }
    }

    fn execute_read(
        &self,
        storage: &dyn StorageEngine,
        runtime: &dyn ExecutionRuntime,
        plan: &PhysicalPlan,
        scope: Option<&DocumentScope>,
    ) -> ExecutionResult<ExecutionOutput> {
        let snapshot = storage.read().map_err(ExecutionError::storage)?;

        if scope.is_none() {
            if let Some(alias) = simple_count_alias(plan) {
                let count = snapshot
                    .count(plan.source().collection())
                    .map_err(ExecutionError::storage)?;
                let document = runtime.count_document(alias, count)?;
                let id = synthetic_id("_count")?;
                return Ok(ExecutionOutput {
                    rows: vec![ExecutionRow::synthetic(SyntheticDocument::new(
                        id, document,
                    ))],
                    statistics: ExecutionStatistics {
                        scanned: 0,
                        filtered: 0,
                        returned: 1,
                        strategies: ExecutionStrategies::default()
                            .with(ExecutionStrategy::DirectCount),
                        ..ExecutionStatistics::default()
                    },
                    commit: None,
                });
            }
        }

        let source_rows = scan_source(snapshot.as_ref(), plan, scope)?;
        let scanned = usize_to_u64(source_rows.len())?;

        let mut state = PipelineState::from_stored(source_rows);
        state.strategies = state.strategies.with(match plan.source().access() {
            PhysicalAccess::CollectionScan { .. } => ExecutionStrategy::CollectionScan,
            PhysicalAccess::PrimaryKeyLookup { .. } => ExecutionStrategy::PrimaryKeyLookup,
        });
        let state = execute_pipeline(
            snapshot.as_ref(),
            runtime,
            plan.operators(),
            state,
            None,
            &self.memory_governor,
            scope,
        )?;

        let returned = usize_to_u64(state.rows.len())?;
        let statistics = ExecutionStatistics {
            scanned,
            filtered: state.filtered,
            returned,
            strategies: state.strategies,
            ..ExecutionStatistics::default()
        };

        Ok(ExecutionOutput {
            rows: state.rows,
            statistics,
            commit: None,
        })
    }

    fn execute_write(
        &self,
        storage: &dyn StorageEngine,
        runtime: &dyn ExecutionRuntime,
        plan: &PhysicalPlan,
        scope: Option<&DocumentScope>,
    ) -> ExecutionResult<ExecutionOutput> {
        if let Some((mode, chunks)) = streaming_load_specification(plan) {
            return execute_streaming_load(
                storage,
                runtime,
                plan,
                mode,
                chunks,
                &self.memory_governor,
            );
        }

        let mut transaction = storage.begin().map_err(ExecutionError::storage)?;

        if let Some(document) = insert_document(plan) {
            return execute_insert(transaction, runtime, plan, document, scope);
        }

        let source_rows = scan_source(transaction.as_ref(), plan, scope)?;
        let scanned = usize_to_u64(source_rows.len())?;

        let mut state = PipelineState::from_stored(source_rows);
        state.strategies = state.strategies.with(match plan.source().access() {
            PhysicalAccess::CollectionScan { .. } => ExecutionStrategy::CollectionScan,
            PhysicalAccess::PrimaryKeyLookup { .. } => ExecutionStrategy::PrimaryKeyLookup,
        });
        let mut state = execute_pipeline(
            transaction.as_ref(),
            runtime,
            plan.operators(),
            state,
            None,
            &self.memory_governor,
            scope,
        )?;

        if contains_delete(plan) {
            delete_rows(transaction.as_mut(), plan, &state.rows)?;
        } else {
            replace_changed_rows(transaction.as_mut(), plan, &mut state.rows, scope)?;
        }

        let commit = transaction.commit().map_err(ExecutionError::storage)?;
        let returned = usize_to_u64(state.rows.len())?;

        let statistics = ExecutionStatistics {
            scanned,
            filtered: state.filtered,
            returned,
            inserted: commit.inserted(),
            replaced: commit.replaced(),
            deleted: commit.deleted(),
            strategies: state.strategies,
        };

        Ok(ExecutionOutput {
            rows: if contains_delete(plan) {
                Vec::new()
            } else {
                state.rows
            },
            statistics: ExecutionStatistics {
                returned: if contains_delete(plan) { 0 } else { returned },
                ..statistics
            },
            commit: Some(commit),
        })
    }
}

fn execute_insert(
    mut transaction: Box<dyn StorageTransaction + '_>,
    runtime: &dyn ExecutionRuntime,
    plan: &PhysicalPlan,
    document: &LogicalInsertDocument,
    scope: Option<&DocumentScope>,
) -> ExecutionResult<ExecutionOutput> {
    let prepared = runtime.prepare_insert(document)?;
    let (id, mut document) = prepared.into_parts();
    if let Some(scope) = scope {
        document = scope.enforce(document.as_ref());
    }

    let result = transaction
        .insert(plan.source().collection(), id, document)
        .map_err(ExecutionError::storage)?;

    let mut row = ExecutionRow::from_stored(result.into_stored());
    row.document = Arc::new(row.evaluation_document());
    let commit = transaction.commit().map_err(ExecutionError::storage)?;

    Ok(ExecutionOutput {
        rows: vec![row],
        statistics: ExecutionStatistics {
            scanned: 0,
            filtered: 0,
            returned: 1,
            inserted: commit.inserted(),
            replaced: commit.replaced(),
            deleted: commit.deleted(),
            strategies: ExecutionStrategies::default(),
        },
        commit: Some(commit),
    })
}

fn execute_streaming_load_compact(
    storage: &dyn StorageEngine,
    runtime: &dyn ExecutionRuntime,
    plan: &PhysicalPlan,
    mode: PhysicalLoadMode,
    chunks: &[Arc<str>],
    memory_governor: &MemoryGovernor,
) -> ExecutionResult<ExecutionOutput> {
    let chunk_bytes = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
    let _reservation = reserve_import_memory(memory_governor, chunk_bytes.saturating_mul(2))?;
    let read = storage.read().map_err(ExecutionError::storage)?;
    let prepared =
        runtime.prepare_streaming_load(plan.source().collection(), read.as_ref(), mode, chunks)?;
    let mut mutations = Vec::with_capacity(prepared.len());

    for mutation in prepared {
        mutations.push(match mutation {
            StreamingLoadMutation::Insert { id, document } => StorageMutation::insert(id, document),
            StreamingLoadMutation::Replace {
                id,
                document,
                precondition,
            } => StorageMutation::replace(id, document, precondition),
        });
    }

    let commit = storage
        .apply_batch_atomic_summary(plan.source().collection(), mutations)
        .map_err(ExecutionError::storage)?;

    Ok(ExecutionOutput {
        rows: Vec::new(),
        statistics: ExecutionStatistics {
            scanned: 0,
            filtered: 0,
            returned: 0,
            inserted: commit.inserted(),
            replaced: commit.replaced(),
            deleted: commit.deleted(),
            strategies: ExecutionStrategies::default(),
        },
        commit: Some(commit),
    })
}

fn execute_streaming_load(
    storage: &dyn StorageEngine,
    runtime: &dyn ExecutionRuntime,
    plan: &PhysicalPlan,
    mode: PhysicalLoadMode,
    chunks: &[Arc<str>],
    memory_governor: &MemoryGovernor,
) -> ExecutionResult<ExecutionOutput> {
    let chunk_bytes = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
    let _reservation = reserve_import_memory(memory_governor, chunk_bytes.saturating_mul(3))?;
    let read = storage.read().map_err(ExecutionError::storage)?;
    let prepared =
        runtime.prepare_streaming_load(plan.source().collection(), read.as_ref(), mode, chunks)?;
    let mut mutations = Vec::with_capacity(prepared.len());

    for mutation in prepared {
        mutations.push(match mutation {
            StreamingLoadMutation::Insert { id, document } => StorageMutation::insert(id, document),
            StreamingLoadMutation::Replace {
                id,
                document,
                precondition,
            } => StorageMutation::replace(id, document, precondition),
        });
    }

    let (stored, commit) = storage
        .apply_batch_atomic(plan.source().collection(), mutations)
        .map_err(ExecutionError::storage)?;
    let rows = stored
        .into_iter()
        .map(ExecutionRow::from_stored)
        .collect::<Vec<_>>();
    let returned = usize_to_u64(rows.len())?;

    Ok(ExecutionOutput {
        rows,
        statistics: ExecutionStatistics {
            scanned: 0,
            filtered: 0,
            returned,
            inserted: commit.inserted(),
            replaced: commit.replaced(),
            deleted: commit.deleted(),
            strategies: ExecutionStrategies::default(),
        },
        commit: Some(commit),
    })
}

/// Complete execution output.
#[derive(Clone, Debug)]
pub struct ExecutionOutput {
    rows: Vec<ExecutionRow>,
    statistics: ExecutionStatistics,
    commit: Option<CommitResult>,
}

impl ExecutionOutput {
    /// Returns emitted rows.
    #[must_use]
    pub fn rows(&self) -> &[ExecutionRow] {
        &self.rows
    }

    /// Consumes the output and returns emitted rows.
    #[must_use]
    pub fn into_rows(self) -> Vec<ExecutionRow> {
        self.rows
    }

    /// Returns execution statistics.
    #[must_use]
    pub const fn statistics(&self) -> ExecutionStatistics {
        self.statistics
    }

    /// Returns the commit summary for a write plan.
    #[must_use]
    pub const fn commit(&self) -> Option<CommitResult> {
        self.commit
    }

    /// Returns whether execution committed a transaction.
    #[must_use]
    pub const fn committed(&self) -> bool {
        self.commit.is_some()
    }

    /// Returns whether no row was emitted.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns the number of emitted rows.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

/// Origin of an execution row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExecutionRowOrigin {
    /// Row came from a committed storage scan.
    Stored,

    /// Row came from a secondary collection through `union`.
    Union,

    /// Row was synthesized by `count`, `group`, or `pivot`.
    Synthetic,
}

/// One row emitted by the executor.
#[derive(Clone, Debug)]
pub struct ExecutionRow {
    id: DocumentId,
    version: DocumentVersion,
    document: Arc<Document>,
    changed: bool,
    origin: ExecutionRowOrigin,
}

impl ExecutionRow {
    pub(crate) fn from_stored(stored: StoredDocument) -> Self {
        let (id, version, document) = stored.into_parts();

        Self {
            id,
            version,
            document,
            changed: false,
            origin: ExecutionRowOrigin::Stored,
        }
    }

    pub(crate) fn from_union(stored: StoredDocument) -> Self {
        let (id, version, document) = stored.into_parts();

        Self {
            id,
            version,
            document,
            changed: false,
            origin: ExecutionRowOrigin::Union,
        }
    }

    pub(crate) fn synthetic(synthetic: SyntheticDocument) -> Self {
        let (id, document) = synthetic.into_parts();

        Self {
            id,
            version: DocumentVersion::NONE,
            document,
            changed: false,
            origin: ExecutionRowOrigin::Synthetic,
        }
    }

    fn from_spill(
        id: DocumentId,
        version: DocumentVersion,
        document: Arc<Document>,
        changed: bool,
        origin: ExecutionRowOrigin,
    ) -> Self {
        Self {
            id,
            version,
            document,
            changed,
            origin,
        }
    }

    /// Returns the row identifier.
    #[must_use]
    pub const fn id(&self) -> &DocumentId {
        &self.id
    }

    /// Returns the source version, or [`DocumentVersion::NONE`] for synthetic rows.
    #[must_use]
    pub const fn version(&self) -> DocumentVersion {
        self.version
    }

    /// Returns the current result document.
    #[must_use]
    pub fn document(&self) -> &Document {
        &self.document
    }

    /// Returns a shared current result document.
    #[must_use]
    pub fn shared_document(&self) -> Arc<Document> {
        Arc::clone(&self.document)
    }

    /// Returns whether a row-local mutating operator replaced the document.
    #[must_use]
    pub const fn changed(&self) -> bool {
        self.changed
    }

    /// Returns the row origin.
    #[must_use]
    pub const fn origin(&self) -> ExecutionRowOrigin {
        self.origin
    }

    /// Returns whether this row corresponds to a stored source document.
    #[must_use]
    pub const fn is_stored(&self) -> bool {
        matches!(self.origin, ExecutionRowOrigin::Stored)
    }

    /// Returns whether this row originated from a union branch.
    #[must_use]
    pub const fn is_union(&self) -> bool {
        matches!(self.origin, ExecutionRowOrigin::Union)
    }

    /// Builds the semantic view exposed to query operators.
    ///
    /// `_id` is storage metadata and is deliberately absent from the physical
    /// document. Query expressions nevertheless treat it as a read-only,
    /// top-level virtual field.
    pub(crate) fn evaluation_document(&self) -> Document {
        let mut document = self.document.as_ref().clone();
        document.insert("_id", self.id.to_string());
        document
    }

    pub(crate) fn replace_document(&mut self, document: Arc<Document>, mark_changed: bool) {
        let mut document = document.as_ref().clone();
        document.remove("_id");
        self.document = Arc::new(document);
        self.changed |= mark_changed;
    }
}

/// Backend-independent locator retained while a row stays on the projected-value
/// access vector. It is intentionally sufficient to hydrate the committed row
/// later without retaining a `Document`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProjectedRowLocator {
    id: DocumentId,
    version: DocumentVersion,
}

impl ProjectedRowLocator {
    #[must_use]
    pub(crate) const fn new(id: DocumentId, version: DocumentVersion) -> Self {
        Self { id, version }
    }

    #[must_use]
    pub(crate) const fn id(self) -> DocumentId {
        self.id
    }

    #[must_use]
    pub(crate) const fn version(self) -> DocumentVersion {
        self.version
    }
}

/// Dense owned projected rows used by blocking operators. Values are stored in
/// one flat buffer rather than allocating one `Vec` per source row. Consumers
/// keep only the fields they actually need plus a locator for late hydration.
#[derive(Debug)]
pub(crate) struct ProjectedRowSet {
    width: usize,
    locators: Vec<ProjectedRowLocator>,
    values: Vec<Option<Value>>,
}

impl ProjectedRowSet {
    #[must_use]
    pub(crate) fn new(width: usize) -> Self {
        Self {
            width,
            locators: Vec::new(),
            values: Vec::new(),
        }
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.locators.len()
    }

    pub(crate) fn push_refs(
        &mut self,
        id: DocumentId,
        version: DocumentVersion,
        source: &[Option<ProjectedValueRef<'_>>],
        slots: &[usize],
    ) -> ExecutionResult<()> {
        if slots.len() != self.width || slots.iter().any(|slot| *slot >= source.len()) {
            return Err(ExecutionError::evaluation(
                "projected row slots do not match the blocking-consumer layout",
            ));
        }
        self.locators.push(ProjectedRowLocator::new(id, version));
        self.values.extend(slots.iter().map(|slot| {
            source
                .get(*slot)
                .and_then(Option::as_ref)
                .map(ProjectedValueRef::to_value)
        }));
        Ok(())
    }

    #[must_use]
    pub(crate) fn locator(&self, index: usize) -> Option<ProjectedRowLocator> {
        self.locators.get(index).copied()
    }

    #[must_use]
    pub(crate) fn row(&self, index: usize) -> Option<&[Option<Value>]> {
        let start = index.checked_mul(self.width)?;
        let end = start.checked_add(self.width)?;
        self.values.get(start..end)
    }
}

/// Conservative working-set estimate for one projected blocking row, including
/// locator storage, flattened value capacity and stable-sort index workspace.
/// It deliberately overcharges small scalar rows so the governor remains the
/// authority even when `Vec` capacities temporarily grow geometrically.
pub(crate) fn projected_row_working_bytes_refs(
    values: &[Option<ProjectedValueRef<'_>>],
    slots: &[usize],
) -> usize {
    let payload = slots.iter().fold(0usize, |bytes, slot| {
        let value = values.get(*slot).and_then(Option::as_ref);
        bytes.saturating_add(projected_ref_payload_bytes(value))
    });
    192usize
        .saturating_add(slots.len().saturating_mul(96))
        .saturating_add(payload.saturating_mul(2))
}

fn projected_ref_payload_bytes(value: Option<&ProjectedValueRef<'_>>) -> usize {
    match value {
        None
        | Some(ProjectedValueRef::Null)
        | Some(ProjectedValueRef::Bool(_))
        | Some(ProjectedValueRef::Signed(_))
        | Some(ProjectedValueRef::Unsigned(_))
        | Some(ProjectedValueRef::Float(_)) => 0,
        Some(ProjectedValueRef::String(value)) => value.len(),
        Some(ProjectedValueRef::Owned(value)) => spill_value_encoded_len(value),
    }
}

/// Builds the stable output permutation for projected sort rows. The runtime
/// remains the owner of value-comparison semantics; the identifier tie-break
/// reconstructs the deterministic collection-scan source order because the
/// storage cursor itself is allowed to be physically unordered.
pub(crate) fn stable_projected_order(
    runtime: &dyn ExecutionRuntime,
    keys: &[SortKey],
    rows: &ProjectedRowSet,
    direction: crate::storage::ScanDirection,
) -> ExecutionResult<Vec<usize>> {
    let mut order = (0..rows.len()).collect::<Vec<_>>();
    let mut failure = None;
    order.sort_by(|left_index, right_index| {
        if failure.is_some() {
            return Ordering::Equal;
        }
        let left = rows
            .row(*left_index)
            .expect("projected sort index must reference an existing row");
        let right = rows
            .row(*right_index)
            .expect("projected sort index must reference an existing row");
        let ordering = match runtime.compare_projected_values(keys, left, right) {
            Ok(Some(ordering)) => ordering,
            Ok(None) => {
                failure = Some(ExecutionError::unsupported_operator(
                    "sort",
                    "projected sort comparison runtime is not configured",
                ));
                Ordering::Equal
            }
            Err(error) => {
                failure = Some(error);
                Ordering::Equal
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }

        let left_id = rows
            .locator(*left_index)
            .expect("projected sort index must reference a locator")
            .id();
        let right_id = rows
            .locator(*right_index)
            .expect("projected sort index must reference a locator")
            .id();
        match direction {
            crate::storage::ScanDirection::Forward => left_id.cmp(&right_id),
            crate::storage::ScanDirection::Reverse => right_id.cmp(&left_id),
        }
    });
    failure.map_or(Ok(order), Err)
}

/// Physical execution strategy selected at runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExecutionStrategy {
    CollectionScan,
    PrimaryKeyLookup,
    DirectCount,
    StreamingCount,
    TopN,
    InMemorySort,
    ExternalSort,
    InMemoryDistinct,
    ExternalDistinct,
    InMemoryGroup,
    ExternalGroup,
}

impl ExecutionStrategy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CollectionScan => "collection_scan",
            Self::PrimaryKeyLookup => "primary_key_lookup",
            Self::DirectCount => "direct_count",
            Self::StreamingCount => "streaming_count",
            Self::TopN => "top_n",
            Self::InMemorySort => "in_memory_sort",
            Self::ExternalSort => "external_sort",
            Self::InMemoryDistinct => "in_memory_distinct",
            Self::ExternalDistinct => "external_distinct",
            Self::InMemoryGroup => "in_memory_group",
            Self::ExternalGroup => "external_group",
        }
    }

    const fn bit(self) -> u16 {
        match self {
            Self::CollectionScan => 1 << 0,
            Self::PrimaryKeyLookup => 1 << 1,
            Self::DirectCount => 1 << 2,
            Self::StreamingCount => 1 << 3,
            Self::TopN => 1 << 4,
            Self::InMemorySort => 1 << 5,
            Self::ExternalSort => 1 << 6,
            Self::InMemoryDistinct => 1 << 7,
            Self::ExternalDistinct => 1 << 8,
            Self::InMemoryGroup => 1 << 9,
            Self::ExternalGroup => 1 << 10,
        }
    }
}

/// Compact set of execution strategies used by one query.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecutionStrategies(u16);

impl ExecutionStrategies {
    #[must_use]
    pub const fn contains(self, strategy: ExecutionStrategy) -> bool {
        self.0 & strategy.bit() != 0
    }

    #[must_use]
    pub const fn with(self, strategy: ExecutionStrategy) -> Self {
        Self(self.0 | strategy.bit())
    }

    pub fn iter(self) -> impl Iterator<Item = ExecutionStrategy> {
        const ALL: [ExecutionStrategy; 11] = [
            ExecutionStrategy::CollectionScan,
            ExecutionStrategy::PrimaryKeyLookup,
            ExecutionStrategy::DirectCount,
            ExecutionStrategy::StreamingCount,
            ExecutionStrategy::TopN,
            ExecutionStrategy::InMemorySort,
            ExecutionStrategy::ExternalSort,
            ExecutionStrategy::InMemoryDistinct,
            ExecutionStrategy::ExternalDistinct,
            ExecutionStrategy::InMemoryGroup,
            ExecutionStrategy::ExternalGroup,
        ];
        ALL.into_iter()
            .filter(move |strategy| self.contains(*strategy))
    }
}

/// Deterministic execution counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExecutionStatistics {
    scanned: u64,
    filtered: u64,
    returned: u64,
    inserted: u64,
    replaced: u64,
    deleted: u64,
    strategies: ExecutionStrategies,
}

impl ExecutionStatistics {
    /// Builds statistics for a streamed read-only execution.
    #[must_use]
    pub fn streamed_read(scanned: u64, returned: u64, strategy: ExecutionStrategy) -> Self {
        Self::streamed_pipeline(scanned, 0, returned, strategy)
    }

    /// Builds statistics for a streamed read-only pipeline.
    #[must_use]
    pub fn streamed_pipeline(
        scanned: u64,
        filtered: u64,
        returned: u64,
        strategy: ExecutionStrategy,
    ) -> Self {
        Self {
            scanned,
            filtered,
            returned,
            inserted: 0,
            replaced: 0,
            deleted: 0,
            strategies: ExecutionStrategies::default().with(strategy),
        }
    }

    /// Builds statistics for a streamed execution using several physical strategies.
    #[must_use]
    pub fn streamed_with_strategies(
        scanned: u64,
        filtered: u64,
        returned: u64,
        strategies: ExecutionStrategies,
    ) -> Self {
        Self {
            scanned,
            filtered,
            returned,
            inserted: 0,
            replaced: 0,
            deleted: 0,
            strategies,
        }
    }

    /// Number of rows read from storage.
    #[must_use]
    pub const fn scanned(self) -> u64 {
        self.scanned
    }

    /// Number of rows removed from the result stream.
    #[must_use]
    pub const fn filtered(self) -> u64 {
        self.filtered
    }

    /// Number of rows emitted.
    #[must_use]
    pub const fn returned(self) -> u64 {
        self.returned
    }

    /// Number of storage insertions committed.
    #[must_use]
    pub const fn inserted(self) -> u64 {
        self.inserted
    }

    /// Number of storage replacements committed.
    #[must_use]
    pub const fn replaced(self) -> u64 {
        self.replaced
    }

    /// Number of storage deletions committed.
    #[must_use]
    pub const fn deleted(self) -> u64 {
        self.deleted
    }

    /// Total committed mutations.
    #[must_use]
    pub const fn mutated(self) -> u64 {
        self.inserted + self.replaced + self.deleted
    }

    /// Physical strategies used during execution.
    #[must_use]
    pub const fn strategies(self) -> ExecutionStrategies {
        self.strategies
    }
}

/// Execution error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionError {
    kind: ExecutionErrorKind,
}

impl ExecutionError {
    /// Creates an execution error.
    #[must_use]
    #[inline]
    pub const fn new(kind: ExecutionErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the detailed error category.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &ExecutionErrorKind {
        &self.kind
    }

    /// Wraps a storage error.
    #[must_use]
    pub fn storage(error: StorageError) -> Self {
        Self::new(ExecutionErrorKind::Storage(error))
    }

    /// Creates an evaluation error.
    #[must_use]
    pub fn evaluation(message: impl Into<Arc<str>>) -> Self {
        Self::new(ExecutionErrorKind::Evaluation {
            message: message.into(),
        })
    }

    /// Creates a mutation error.
    #[must_use]
    pub fn mutation(message: impl Into<Arc<str>>) -> Self {
        Self::new(ExecutionErrorKind::Mutation {
            message: message.into(),
        })
    }

    /// Creates an unsupported-operator error.
    #[must_use]
    pub fn unsupported_operator(
        operator: impl Into<Arc<str>>,
        reason: impl Into<Arc<str>>,
    ) -> Self {
        Self::new(ExecutionErrorKind::UnsupportedOperator {
            operator: operator.into(),
            reason: reason.into(),
        })
    }

    fn counter_overflow() -> Self {
        Self::new(ExecutionErrorKind::CounterOverflow)
    }

    fn memory_limit(operator: &str, error: MemoryReservationError) -> Self {
        Self::new(ExecutionErrorKind::MemoryLimitExceeded {
            operator: Arc::from(operator),
            requested_bytes: error.requested_bytes,
            current_bytes: error.current_bytes,
            limit_bytes: error.limit_bytes,
        })
    }
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ExecutionErrorKind::Storage(error) => {
                write!(formatter, "storage execution error: {error}")
            }
            ExecutionErrorKind::Evaluation { message } => {
                write!(formatter, "expression evaluation failed: {message}")
            }
            ExecutionErrorKind::Mutation { message } => {
                write!(formatter, "document mutation failed: {message}")
            }
            ExecutionErrorKind::UnsupportedOperator { operator, reason } => {
                write!(
                    formatter,
                    "physical operator {operator:?} is unsupported: {reason}",
                )
            }
            ExecutionErrorKind::InvalidSyntheticIdentifier { identifier, reason } => {
                write!(
                    formatter,
                    "invalid synthetic result identifier {identifier:?}: {reason}",
                )
            }
            ExecutionErrorKind::CounterOverflow => {
                formatter.write_str("execution statistics counter overflow")
            }
            ExecutionErrorKind::MemoryLimitExceeded {
                operator,
                requested_bytes,
                current_bytes,
                limit_bytes,
            } => {
                write!(
                    formatter,
                    "query memory limit exceeded for {operator}: requested {requested_bytes} bytes with {current_bytes} bytes already reserved"
                )?;
                if let Some(limit_bytes) = limit_bytes {
                    write!(formatter, "; limit is {limit_bytes} bytes")?;
                }
                Ok(())
            }
        }
    }
}

impl StdError for ExecutionError {
    #[inline]
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.kind {
            ExecutionErrorKind::Storage(error) => Some(error),
            _ => None,
        }
    }
}

/// Detailed execution error category.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutionErrorKind {
    /// Storage operation failed.
    Storage(StorageError),

    /// Expression evaluation failed.
    Evaluation { message: Arc<str> },

    /// Document mutation failed.
    Mutation { message: Arc<str> },

    /// No runtime implementation exists for an operator.
    UnsupportedOperator {
        operator: Arc<str>,
        reason: Arc<str>,
    },

    /// Runtime or executor generated an invalid synthetic identifier.
    InvalidSyntheticIdentifier {
        identifier: Arc<str>,
        reason: Arc<str>,
    },

    /// A statistics counter exceeded `u64`.
    CounterOverflow,

    /// A governed execution operator could not obtain its memory budget.
    MemoryLimitExceeded {
        operator: Arc<str>,
        requested_bytes: usize,
        current_bytes: usize,
        limit_bytes: Option<usize>,
    },
}

struct PipelineState {
    rows: Vec<ExecutionRow>,
    filtered: u64,
    strategies: ExecutionStrategies,
}

impl PipelineState {
    fn from_stored(rows: Vec<StoredDocument>) -> Self {
        Self {
            rows: rows.into_iter().map(ExecutionRow::from_stored).collect(),
            filtered: 0,
            strategies: ExecutionStrategies::default(),
        }
    }

    fn add_filtered(&mut self, count: usize) -> ExecutionResult<()> {
        self.filtered = self
            .filtered
            .checked_add(usize_to_u64(count)?)
            .ok_or_else(ExecutionError::counter_overflow)?;

        Ok(())
    }
}

fn simple_count_alias(plan: &PhysicalPlan) -> Option<&str> {
    if plan.source().access().scan_options() != Some(ScanOptions::default()) {
        return None;
    }

    match plan.operators() {
        [PhysicalOperator::Count { alias }] => Some(alias.as_ref()),
        _ => None,
    }
}

fn scan_source(
    storage: &dyn StorageRead,
    plan: &PhysicalPlan,
    scope: Option<&DocumentScope>,
) -> ExecutionResult<Vec<StoredDocument>> {
    let rows = match plan.source().access() {
        PhysicalAccess::CollectionScan { options } => storage
            .scan(plan.source().collection(), *options)
            .map_err(ExecutionError::storage)?,
        PhysicalAccess::PrimaryKeyLookup { id } => storage
            .get(plan.source().collection(), id)
            .map(|stored| stored.into_iter().collect())
            .map_err(ExecutionError::storage)?,
    };
    Ok(match scope {
        Some(scope) => rows
            .into_iter()
            .filter(|row| scope.matches(row.document()))
            .collect(),
        None => rows,
    })
}

fn execute_pipeline(
    storage: &dyn StorageRead,
    runtime: &dyn ExecutionRuntime,
    operators: &[PhysicalOperator],
    mut state: PipelineState,
    lookup_context: Option<LookupContext<'_>>,
    memory_governor: &MemoryGovernor,
    scope: Option<&DocumentScope>,
) -> ExecutionResult<PipelineState> {
    let mut cursor = 0usize;

    while cursor < operators.len() {
        if let (PhysicalOperator::Sort { keys }, Some(PhysicalOperator::Limit { count })) =
            (&operators[cursor], operators.get(cursor + 1))
        {
            let _reservation =
                reserve_query_memory(memory_governor, "top-n sort", estimated_top_n_bytes(*count))?;
            execute_top_n(runtime, keys, *count, &mut state)?;
            state.strategies = state.strategies.with(ExecutionStrategy::TopN);
            cursor += 2;
            continue;
        }

        if is_set_level_operator(&operators[cursor]) {
            execute_set_level_operator(
                storage,
                runtime,
                &operators[cursor],
                &mut state,
                lookup_context,
                memory_governor,
                scope,
            )?;
            cursor += 1;
            continue;
        }

        let segment_start = cursor;

        while cursor < operators.len() && !is_set_level_operator(&operators[cursor]) {
            cursor += 1;
        }

        execute_row_segment(
            storage,
            runtime,
            &operators[segment_start..cursor],
            &mut state,
            lookup_context,
            memory_governor,
            scope,
        )?;
    }

    Ok(state)
}

#[derive(Clone, Copy)]
struct LookupContext<'a> {
    outer: &'a Document,
    inner_alias: Option<&'a str>,
}

fn is_set_level_operator(operator: &PhysicalOperator) -> bool {
    matches!(
        operator.execution_properties().scope,
        super::execution_properties::Scope::Set
    )
}

fn execute_row_segment(
    storage: &dyn StorageRead,
    runtime: &dyn ExecutionRuntime,
    operators: &[PhysicalOperator],
    state: &mut PipelineState,
    lookup_context: Option<LookupContext<'_>>,
    memory_governor: &MemoryGovernor,
    scope: Option<&DocumentScope>,
) -> ExecutionResult<()> {
    let mut retained = Vec::with_capacity(state.rows.len());
    let mut removed = 0usize;

    for mut row in state.rows.drain(..) {
        let mut keep = true;

        for operator in operators {
            match operator {
                PhysicalOperator::Filter { predicate } => {
                    let evaluation_document = row.evaluation_document();
                    let accepted = match lookup_context {
                        Some(context) => runtime.evaluate_lookup_predicate(
                            predicate,
                            context.outer,
                            context.inner_alias,
                            &evaluation_document,
                        )?,
                        None => runtime.evaluate_predicate(predicate, &evaluation_document)?,
                    };

                    if !accepted {
                        keep = false;
                        break;
                    }
                }

                PhysicalOperator::Set { assignments } => {
                    let evaluation_document = row.evaluation_document();
                    let document = runtime.apply_set(assignments, &evaluation_document)?;
                    row.replace_document(document, true);
                }

                PhysicalOperator::Lookup {
                    collection,
                    alias,
                    into,
                    pipeline,
                } => {
                    let documents = execute_lookup(
                        storage,
                        runtime,
                        collection,
                        alias.as_deref(),
                        pipeline,
                        row.document(),
                        memory_governor,
                        scope,
                    )?;

                    let evaluation_document = row.evaluation_document();
                    let document = runtime.apply_lookup(into, &evaluation_document, &documents)?;
                    row.replace_document(document, false);
                }

                PhysicalOperator::Load { target } => {
                    let evaluation_document = row.evaluation_document();
                    let document = runtime.apply_load(target, &evaluation_document)?;
                    row.replace_document(document, true);
                }

                PhysicalOperator::StreamingLoad { .. } => {}

                PhysicalOperator::Select { fields } => {
                    let evaluation_document = row.evaluation_document();
                    let document = runtime.apply_select(fields, &evaluation_document)?;
                    row.replace_document(document, false);
                }

                PhysicalOperator::Delete | PhysicalOperator::Insert { .. } => {}

                PhysicalOperator::Custom {
                    name,
                    arguments,
                    writes,
                    ..
                } => {
                    let evaluation_document = row.evaluation_document();
                    match runtime.apply_custom(name, arguments, *writes, &evaluation_document)? {
                        CustomOperatorResult::Keep => {}
                        CustomOperatorResult::Discard => {
                            keep = false;
                            break;
                        }
                        CustomOperatorResult::Replace(document) => {
                            row.replace_document(document, *writes);
                        }
                        CustomOperatorResult::Expand(_) => {
                            return Err(ExecutionError::evaluation(
                                "row-local custom operator cannot expand rows",
                            ));
                        }
                    }
                }

                _ => unreachable!("set-level operator cannot appear in a row segment"),
            }
        }

        if keep {
            retained.push(row);
        } else {
            removed = removed
                .checked_add(1)
                .ok_or_else(ExecutionError::counter_overflow)?;
        }
    }

    state.rows = retained;
    state.add_filtered(removed)
}

fn execute_lookup(
    storage: &dyn StorageRead,
    runtime: &dyn ExecutionRuntime,
    collection: &crate::storage::CollectionId,
    alias: Option<&str>,
    pipeline: &PhysicalSubPipeline,
    outer: &Document,
    memory_governor: &MemoryGovernor,
    scope: Option<&DocumentScope>,
) -> ExecutionResult<LookupDocuments> {
    let rows = storage
        .scan(collection, ScanOptions::default())
        .map_err(ExecutionError::storage)?;
    let rows = match scope {
        Some(scope) => rows
            .into_iter()
            .filter(|row| scope.matches(row.document()))
            .collect(),
        None => rows,
    };
    let _reservation =
        reserve_query_memory(memory_governor, "lookup", estimated_rows_bytes(rows.len()))?;

    let state = PipelineState::from_stored(rows);
    let state = execute_pipeline(
        storage,
        runtime,
        pipeline.operators(),
        state,
        Some(LookupContext {
            outer,
            inner_alias: alias,
        }),
        memory_governor,
        scope,
    )?;

    Ok(LookupDocuments::new(
        state.rows.into_iter().map(|row| row.shared_document()),
    ))
}

fn execute_set_level_operator(
    storage: &dyn StorageRead,
    runtime: &dyn ExecutionRuntime,
    operator: &PhysicalOperator,
    state: &mut PipelineState,
    lookup_context: Option<LookupContext<'_>>,
    memory_governor: &MemoryGovernor,
    scope: Option<&DocumentScope>,
) -> ExecutionResult<()> {
    match operator {
        PhysicalOperator::Union {
            collection,
            alias,
            pipeline,
        } => {
            let source_rows = storage
                .scan(collection, ScanOptions::default())
                .map_err(ExecutionError::storage)?;
            let source_rows = match scope {
                Some(scope) => source_rows
                    .into_iter()
                    .filter(|row| scope.matches(row.document()))
                    .collect(),
                None => source_rows,
            };

            let branch = PipelineState {
                rows: source_rows
                    .into_iter()
                    .map(ExecutionRow::from_union)
                    .collect(),
                filtered: 0,
                strategies: ExecutionStrategies::default(),
            };

            let branch = execute_pipeline(
                storage,
                runtime,
                pipeline.operators(),
                branch,
                lookup_context.map(|context| LookupContext {
                    outer: context.outer,
                    inner_alias: alias.as_deref().or(context.inner_alias),
                }),
                memory_governor,
                scope,
            )?;

            state.filtered = state
                .filtered
                .checked_add(branch.filtered)
                .ok_or_else(ExecutionError::counter_overflow)?;
            state.rows.extend(branch.rows);
        }

        PhysicalOperator::Limit { count } => {
            if state.rows.len() > *count {
                let removed = state.rows.len() - *count;
                state.rows.truncate(*count);
                state.add_filtered(removed)?;
            }
        }

        PhysicalOperator::Skip { count } => {
            let removed = (*count).min(state.rows.len());
            state.rows.drain(..removed);
            state.add_filtered(removed)?;
        }

        PhysicalOperator::Sort { keys } => {
            // The rows and their Arc<Document> payloads are already resident here.
            // Sorting does not duplicate document bodies; govern only the temporary
            // stable-sort workspace instead of charging the full logical row set twice.
            let estimated = estimated_sort_workspace_bytes(state.rows.len());
            match reserve_query_memory(memory_governor, "sort", estimated) {
                Ok(_reservation) => {
                    stable_sort(runtime, keys, &mut state.rows)?;
                    state.strategies = state.strategies.with(ExecutionStrategy::InMemorySort);
                }
                Err(ExecutionError {
                    kind: ExecutionErrorKind::MemoryLimitExceeded { .. },
                }) => {
                    external_sort(runtime, keys, state, memory_governor)?;
                    state.strategies = state.strategies.with(ExecutionStrategy::ExternalSort);
                }
                Err(error) => return Err(error),
            }
        }

        PhysicalOperator::Distinct { fields } => {
            let _reservation = reserve_query_memory(
                memory_governor,
                "distinct",
                estimated_rows_bytes(state.rows.len()),
            )?;
            execute_distinct(runtime, fields, state)?;
        }

        PhysicalOperator::Count { alias } => {
            let count = usize_to_u64(state.rows.len())?;
            let document = runtime.count_document(alias, count)?;
            let id = synthetic_id("_count")?;

            state.rows = vec![ExecutionRow::synthetic(SyntheticDocument::new(
                id, document,
            ))];
        }

        PhysicalOperator::Group { keys } => {
            let _reservation = reserve_query_memory(
                memory_governor,
                "group",
                estimated_rows_bytes(state.rows.len()),
            )?;
            let documents = state
                .rows
                .iter()
                .map(ExecutionRow::shared_document)
                .collect::<Vec<_>>();

            state.rows = runtime
                .group_documents(keys, &documents)?
                .into_iter()
                .map(ExecutionRow::synthetic)
                .collect();
        }

        PhysicalOperator::Pivot { specification } => {
            let _reservation = reserve_query_memory(
                memory_governor,
                "pivot",
                estimated_rows_bytes(state.rows.len()),
            )?;
            let documents = state
                .rows
                .iter()
                .map(ExecutionRow::shared_document)
                .collect::<Vec<_>>();

            state.rows = runtime
                .pivot_documents(specification, &documents)?
                .into_iter()
                .map(ExecutionRow::synthetic)
                .collect();
        }

        PhysicalOperator::Custom {
            name,
            arguments,
            writes,
            ..
        } if matches!(name.as_str(), "unwind" | "first" | "single") => {
            if name.as_str() == "first" {
                if state.rows.len() > 1 {
                    let removed = state.rows.len() - 1;
                    state.rows.truncate(1);
                    state.add_filtered(removed)?;
                }
            } else if name.as_str() == "single" && state.rows.len() != 1 {
                return Err(ExecutionError::evaluation(format!(
                    "single expected exactly one row, found {}",
                    state.rows.len()
                )));
            }

            let mut expanded = Vec::new();
            for mut row in state.rows.drain(..) {
                match runtime.apply_custom(name, arguments, *writes, row.document())? {
                    CustomOperatorResult::Keep => expanded.push(row),
                    CustomOperatorResult::Discard => {}
                    CustomOperatorResult::Replace(document) => {
                        row.replace_document(document, *writes);
                        expanded.push(row);
                    }
                    CustomOperatorResult::Expand(documents) => {
                        expanded.extend(documents.into_iter().map(|document| {
                            ExecutionRow::synthetic(SyntheticDocument::new(
                                synthetic_id("_unwind").expect("static synthetic id"),
                                document,
                            ))
                        }));
                    }
                }
            }
            state.rows = expanded;
        }

        PhysicalOperator::Filter { .. }
        | PhysicalOperator::Set { .. }
        | PhysicalOperator::Lookup { .. }
        | PhysicalOperator::Load { .. }
        | PhysicalOperator::StreamingLoad { .. }
        | PhysicalOperator::Select { .. }
        | PhysicalOperator::Delete
        | PhysicalOperator::Insert { .. }
        | PhysicalOperator::Custom { .. } => {
            unreachable!("row-local operator cannot be executed as a set-level operator");
        }
    }

    Ok(())
}

fn execute_distinct(
    runtime: &dyn ExecutionRuntime,
    fields: &[ExpressionFieldPath],
    state: &mut PipelineState,
) -> ExecutionResult<()> {
    let mut keys = Vec::<Arc<[u8]>>::new();
    let mut retained = Vec::with_capacity(state.rows.len());
    let mut removed = 0usize;

    for row in state.rows.drain(..) {
        let key = runtime.distinct_key(fields, row.document())?;

        if keys
            .iter()
            .any(|existing| existing.as_ref() == key.as_ref())
        {
            removed = removed
                .checked_add(1)
                .ok_or_else(ExecutionError::counter_overflow)?;
        } else {
            keys.push(key);
            retained.push(row);
        }
    }

    state.rows = retained;
    state.add_filtered(removed)
}

const ESTIMATED_ROW_WORKING_BYTES: usize = 256;

pub(crate) fn estimated_rows_bytes(rows: usize) -> usize {
    rows.saturating_mul(ESTIMATED_ROW_WORKING_BYTES.max(size_of::<ExecutionRow>()))
}

#[inline]
fn estimated_sort_workspace_bytes(rows: usize) -> usize {
    // Rust's stable slice sort may allocate O(n) element scratch, but the elements
    // are ExecutionRow handles; Arc<Document> payloads stay shared and are not copied.
    rows.saturating_mul(size_of::<ExecutionRow>())
}

fn estimated_top_n_bytes(limit: usize) -> usize {
    limit.saturating_mul(ESTIMATED_ROW_WORKING_BYTES.max(size_of::<ExecutionRow>()))
}

pub(crate) fn reserve_query_memory(
    governor: &MemoryGovernor,
    operator: &'static str,
    bytes: usize,
) -> ExecutionResult<MemoryReservation> {
    governor
        .reserve(MemoryClass::Query, bytes)
        .map_err(|error| ExecutionError::memory_limit(operator, error))
}

fn reserve_import_memory(
    governor: &MemoryGovernor,
    bytes: usize,
) -> ExecutionResult<MemoryReservation> {
    governor
        .reserve(MemoryClass::Import, bytes)
        .map_err(|error| ExecutionError::memory_limit("streaming import", error))
}

#[derive(Debug)]
struct TopNEntry {
    sequence: u64,
    estimated_bytes: usize,
    row: ExecutionRow,
}

/// Shared bounded Top-N selector used by both materialized and streaming executors.
///
/// The selector keeps the current worst candidate cached. Once full, the common
/// path needs one comparison per incoming row; only a candidate that beats the
/// current worst triggers an O(N) rescan. This avoids the previous O(N) insertion
/// walk on every source row while keeping memory strictly bounded by `limit`.
#[derive(Debug)]
pub(crate) struct BoundedTopN {
    limit: usize,
    entries: Vec<TopNEntry>,
    worst_index: Option<usize>,
    next_sequence: u64,
    live_bytes: usize,
}

impl BoundedTopN {
    #[must_use]
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            entries: Vec::with_capacity(limit.min(4096)),
            worst_index: None,
            next_sequence: 0,
            live_bytes: 0,
        }
    }

    #[must_use]
    pub(crate) fn live_bytes(&self) -> usize {
        self.live_bytes
    }

    pub(crate) fn push(
        &mut self,
        runtime: &dyn ExecutionRuntime,
        keys: &[SortKey],
        row: ExecutionRow,
        estimated_bytes: usize,
    ) -> ExecutionResult<()> {
        self.push_lazy(runtime, keys, row, |_| Ok(estimated_bytes))
    }

    /// Pushes a candidate while deferring size estimation until the candidate is
    /// actually retained. For small Top-N limits this avoids serializing nearly
    /// every source row merely to account a handful of live candidates.
    pub(crate) fn push_lazy<F>(
        &mut self,
        runtime: &dyn ExecutionRuntime,
        keys: &[SortKey],
        row: ExecutionRow,
        estimate: F,
    ) -> ExecutionResult<()>
    where
        F: FnOnce(&ExecutionRow) -> ExecutionResult<usize>,
    {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.limit == 0 {
            return Ok(());
        }

        if self.entries.len() < self.limit {
            let estimated_bytes = estimate(&row)?;
            let index = self.entries.len();
            self.live_bytes = self.live_bytes.saturating_add(estimated_bytes);
            self.entries.push(TopNEntry {
                sequence,
                estimated_bytes,
                row,
            });
            match self.worst_index {
                None => self.worst_index = Some(index),
                Some(worst) => {
                    let ordering = runtime.compare_documents(
                        keys,
                        self.entries[worst].row.document(),
                        self.entries[index].row.document(),
                    )?;
                    if ordering == Ordering::Less
                        || (ordering == Ordering::Equal
                            && self.entries[index].sequence > self.entries[worst].sequence)
                    {
                        self.worst_index = Some(index);
                    }
                }
            }
            return Ok(());
        }

        let worst = self.worst_index.expect("full Top-N has a worst candidate");
        let ordering =
            runtime.compare_documents(keys, row.document(), self.entries[worst].row.document())?;
        // Equal candidates arrive later and therefore cannot improve a stable Top-N.
        if ordering != Ordering::Less {
            return Ok(());
        }

        let estimated_bytes = estimate(&row)?;
        self.live_bytes = self
            .live_bytes
            .saturating_sub(self.entries[worst].estimated_bytes)
            .saturating_add(estimated_bytes);
        self.entries[worst] = TopNEntry {
            sequence,
            estimated_bytes,
            row,
        };
        self.recompute_worst(runtime, keys)
    }

    fn recompute_worst(
        &mut self,
        runtime: &dyn ExecutionRuntime,
        keys: &[SortKey],
    ) -> ExecutionResult<()> {
        let mut worst = 0usize;
        for index in 1..self.entries.len() {
            let ordering = runtime.compare_documents(
                keys,
                self.entries[worst].row.document(),
                self.entries[index].row.document(),
            )?;
            if ordering == Ordering::Less
                || (ordering == Ordering::Equal
                    && self.entries[index].sequence > self.entries[worst].sequence)
            {
                worst = index;
            }
        }
        self.worst_index = Some(worst);
        Ok(())
    }

    pub(crate) fn into_sorted_rows(
        mut self,
        runtime: &dyn ExecutionRuntime,
        keys: &[SortKey],
    ) -> ExecutionResult<Vec<ExecutionRow>> {
        // Restore source order first so the stable sort preserves the original
        // order among equal keys even though replacements happen in-place.
        self.entries.sort_unstable_by_key(|entry| entry.sequence);
        let mut rows: Vec<_> = self.entries.into_iter().map(|entry| entry.row).collect();
        stable_sort(runtime, keys, &mut rows)?;
        Ok(rows)
    }
}

/// One retained projected row for late materialization.
#[derive(Debug)]
pub(crate) struct ProjectedTopNWinner {
    id: DocumentId,
    version: DocumentVersion,
    values: Vec<Option<Value>>,
}

impl ProjectedTopNWinner {
    #[must_use]
    pub(crate) fn id(&self) -> &DocumentId {
        &self.id
    }

    #[must_use]
    pub(crate) const fn version(&self) -> DocumentVersion {
        self.version
    }
}

#[derive(Debug)]
struct ProjectedTopNEntry {
    sequence: u64,
    winner: ProjectedTopNWinner,
}

/// Bounded ordering selector over a projected-value vector.
///
/// Unlike [`BoundedTopN`], this selector never constructs a temporary `Document`
/// for source rows. Borrowed storage values are compared directly against the
/// retained owned key vector; ownership is taken only when a row enters Top-N.
/// This makes late materialization a reusable query primitive rather than a
/// special case tied to one physical stage.
#[derive(Debug)]
pub(crate) struct BoundedProjectedTopN {
    limit: usize,
    entries: Vec<ProjectedTopNEntry>,
    worst_index: Option<usize>,
    next_sequence: u64,
}

impl BoundedProjectedTopN {
    #[must_use]
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            entries: Vec::with_capacity(limit.min(4096)),
            worst_index: None,
            next_sequence: 0,
        }
    }

    pub(crate) fn push_refs(
        &mut self,
        keys: &[SortKey],
        slots: &[usize],
        id: DocumentId,
        version: DocumentVersion,
        values: &[Option<ProjectedValueRef<'_>>],
    ) -> ExecutionResult<()> {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.limit == 0 {
            return Ok(());
        }
        if slots.len() != keys.len() || slots.iter().any(|slot| *slot >= values.len()) {
            return Err(ExecutionError::evaluation(
                "projected Top-N slots do not match its sort-key layout",
            ));
        }

        if self.entries.len() < self.limit {
            let index = self.entries.len();
            self.entries.push(ProjectedTopNEntry {
                sequence,
                winner: ProjectedTopNWinner {
                    id,
                    version,
                    values: materialize_projected_slots(values, slots),
                },
            });
            match self.worst_index {
                None => self.worst_index = Some(index),
                Some(worst) => {
                    let ordering = compare_owned_projected_rows(
                        keys,
                        &self.entries[worst].winner.values,
                        &self.entries[index].winner.values,
                    )?;
                    if ordering == Ordering::Less
                        || (ordering == Ordering::Equal
                            && self.entries[index].sequence > self.entries[worst].sequence)
                    {
                        self.worst_index = Some(index);
                    }
                }
            }
            return Ok(());
        }

        let worst = self
            .worst_index
            .expect("full projected Top-N has a worst candidate");
        let ordering = compare_projected_refs_to_owned(
            keys,
            slots,
            values,
            &self.entries[worst].winner.values,
        )?;
        if ordering != Ordering::Less {
            return Ok(());
        }

        self.entries[worst] = ProjectedTopNEntry {
            sequence,
            winner: ProjectedTopNWinner {
                id,
                version,
                values: materialize_projected_slots(values, slots),
            },
        };
        self.recompute_worst(keys)
    }

    fn recompute_worst(&mut self, keys: &[SortKey]) -> ExecutionResult<()> {
        let mut worst = 0usize;
        for index in 1..self.entries.len() {
            let ordering = compare_owned_projected_rows(
                keys,
                &self.entries[worst].winner.values,
                &self.entries[index].winner.values,
            )?;
            if ordering == Ordering::Less
                || (ordering == Ordering::Equal
                    && self.entries[index].sequence > self.entries[worst].sequence)
            {
                worst = index;
            }
        }
        self.worst_index = Some(worst);
        Ok(())
    }

    pub(crate) fn into_sorted_winners(
        mut self,
        keys: &[SortKey],
    ) -> ExecutionResult<Vec<ProjectedTopNWinner>> {
        // Restore source order first; the fallible insertion sort below is stable
        // on equal keys and N is deliberately bounded/small.
        self.entries.sort_unstable_by_key(|entry| entry.sequence);
        for index in 1..self.entries.len() {
            let mut cursor = index;
            while cursor > 0 {
                let ordering = compare_owned_projected_rows(
                    keys,
                    &self.entries[cursor - 1].winner.values,
                    &self.entries[cursor].winner.values,
                )?;
                if ordering != Ordering::Greater {
                    break;
                }
                self.entries.swap(cursor - 1, cursor);
                cursor -= 1;
            }
        }
        Ok(self.entries.into_iter().map(|entry| entry.winner).collect())
    }
}

fn materialize_projected_slots(
    values: &[Option<ProjectedValueRef<'_>>],
    slots: &[usize],
) -> Vec<Option<Value>> {
    slots
        .iter()
        .map(|slot| {
            values
                .get(*slot)
                .and_then(Option::as_ref)
                .map(ProjectedValueRef::to_value)
        })
        .collect()
}

fn compare_projected_refs_to_owned(
    keys: &[SortKey],
    slots: &[usize],
    left: &[Option<ProjectedValueRef<'_>>],
    right: &[Option<Value>],
) -> ExecutionResult<Ordering> {
    for (index, key) in keys.iter().enumerate() {
        let ordering = compare_projected_ref_value(
            slots
                .get(index)
                .and_then(|slot| left.get(*slot))
                .and_then(Option::as_ref),
            right.get(index).and_then(Option::as_ref),
            key,
        )?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_owned_projected_rows(
    keys: &[SortKey],
    left: &[Option<Value>],
    right: &[Option<Value>],
) -> ExecutionResult<Ordering> {
    for (index, key) in keys.iter().enumerate() {
        let ordering = compare_optional_values(
            left.get(index).and_then(Option::as_ref),
            right.get(index).and_then(Option::as_ref),
            key,
        )?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

fn compare_projected_ref_value(
    left: Option<&ProjectedValueRef<'_>>,
    right: Option<&Value>,
    key: &SortKey,
) -> ExecutionResult<Ordering> {
    let ordering = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(ProjectedValueRef::Null), Some(Value::Null)) => Ordering::Equal,
        (Some(ProjectedValueRef::Bool(left)), Some(Value::Bool(right))) => left.cmp(right),
        (Some(ProjectedValueRef::String(left)), Some(Value::String(right))) => {
            left.cmp(&right.as_ref())
        }
        (Some(ProjectedValueRef::Signed(left)), Some(Value::Number(Number::Signed(right)))) => {
            left.cmp(right)
        }
        (Some(ProjectedValueRef::Unsigned(left)), Some(Value::Number(Number::Unsigned(right)))) => {
            left.cmp(right)
        }
        (Some(ProjectedValueRef::Float(left)), Some(Value::Number(Number::Float(right)))) => {
            left.partial_cmp(right).ok_or_else(|| {
                ExecutionError::evaluation(format!(
                    "cannot sort field {}: incompatible values",
                    key.field()
                ))
            })?
        }
        (Some(ProjectedValueRef::Owned(left)), Some(right)) => {
            compare(left, right, CoercionPolicy::Numeric)
                .map_err(|error| {
                    ExecutionError::evaluation(format!(
                        "cannot sort field {}: {error}",
                        key.field()
                    ))
                })?
                .into_ordering()
        }
        (Some(left), Some(right)) => {
            // Mixed physical kinds are uncommon on the fast path. Materialize
            // only this comparison so normal numeric/string rows stay borrowed.
            let left = left.to_value();
            compare(&left, right, CoercionPolicy::Numeric)
                .map_err(|error| {
                    ExecutionError::evaluation(format!(
                        "cannot sort field {}: {error}",
                        key.field()
                    ))
                })?
                .into_ordering()
        }
    };
    Ok(match key.direction() {
        super::SortDirection::Ascending => ordering,
        super::SortDirection::Descending => ordering.reverse(),
    })
}

fn compare_optional_values(
    left: Option<&Value>,
    right: Option<&Value>,
    key: &SortKey,
) -> ExecutionResult<Ordering> {
    let ordering = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => compare(left, right, CoercionPolicy::Numeric)
            .map_err(|error| {
                ExecutionError::evaluation(format!("cannot sort field {}: {error}", key.field()))
            })?
            .into_ordering(),
    };
    Ok(match key.direction() {
        super::SortDirection::Ascending => ordering,
        super::SortDirection::Descending => ordering.reverse(),
    })
}

fn execute_top_n(
    runtime: &dyn ExecutionRuntime,
    keys: &[SortKey],
    limit: usize,
    state: &mut PipelineState,
) -> ExecutionResult<()> {
    if limit == 0 {
        let removed = state.rows.len();
        state.rows.clear();
        return state.add_filtered(removed);
    }

    let original_len = state.rows.len();
    let mut top = BoundedTopN::new(limit.min(original_len));
    for row in state.rows.drain(..) {
        top.push(runtime, keys, row, ESTIMATED_ROW_WORKING_BYTES)?;
    }
    state.rows = top.into_sorted_rows(runtime, keys)?;
    state.add_filtered(original_len.saturating_sub(state.rows.len()))
}

pub(crate) fn execution_row_encoded_len(row: &ExecutionRow) -> usize {
    const ROW_HEADER_BYTES: usize = 16 + 8 + 1 + 1;
    ROW_HEADER_BYTES.saturating_add(spill_document_encoded_len(row.document()))
}

pub(crate) fn execution_row_working_bytes(row: &ExecutionRow) -> ExecutionResult<usize> {
    Ok(execution_row_encoded_len(row).saturating_add(64))
}

fn spill_document_encoded_len(document: &Document) -> usize {
    document.iter().fold(8usize, |bytes, (name, value)| {
        bytes
            .saturating_add(8)
            .saturating_add(name.as_str().len())
            .saturating_add(spill_value_encoded_len(value))
    })
}

fn spill_value_encoded_len(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) => 1,
        Value::Number(_) => 1 + 8,
        Value::String(value) => 1usize.saturating_add(8).saturating_add(value.len()),
        Value::Array(values) => values
            .iter()
            .fold(1usize.saturating_add(8), |bytes, value| {
                bytes.saturating_add(spill_value_encoded_len(value))
            }),
        Value::Object(document) => 1usize.saturating_add(spill_document_encoded_len(document)),
    }
}

pub(crate) fn stable_sort(
    runtime: &dyn ExecutionRuntime,
    keys: &[SortKey],
    rows: &mut [ExecutionRow],
) -> ExecutionResult<()> {
    let mut failure = None;
    rows.sort_by(|left, right| {
        if failure.is_some() {
            return Ordering::Equal;
        }
        match runtime.compare_documents(keys, left.document(), right.document()) {
            Ok(ordering) => ordering,
            Err(error) => {
                failure = Some(error);
                Ordering::Equal
            }
        }
    });
    failure.map_or(Ok(()), Err)
}

fn external_sort(
    runtime: &dyn ExecutionRuntime,
    keys: &[SortKey],
    state: &mut PipelineState,
    governor: &MemoryGovernor,
) -> ExecutionResult<()> {
    let snapshot = governor.snapshot();
    let available = snapshot.available_bytes.unwrap_or(64 * 1024 * 1024);
    let chunk_budget = available
        .saturating_div(2)
        .clamp(1 * 1024 * 1024, 64 * 1024 * 1024);
    let rows_per_chunk = (chunk_budget / size_of::<ExecutionRow>().max(1)).max(1);
    let spill = SpillEngine::default();
    let mut runs = Vec::<SpillRun>::new();

    while !state.rows.is_empty() {
        let take = rows_per_chunk.min(state.rows.len());
        let mut chunk = state.rows.drain(..take).collect::<Vec<_>>();
        let _reservation = reserve_query_memory(
            governor,
            "external sort run",
            estimated_sort_workspace_bytes(chunk.len()),
        )?;
        stable_sort(runtime, keys, &mut chunk)?;
        let mut writer = spill.create_run().map_err(spill_execution_error)?;
        let mut encoded = Vec::new();
        for row in &chunk {
            encode_execution_row_into(row, &mut encoded)?;
            writer.append(&encoded).map_err(spill_execution_error)?;
        }
        runs.push(writer.finish().map_err(spill_execution_error)?);
    }

    let mut readers = runs
        .iter()
        .map(SpillRun::reader)
        .collect::<io::Result<Vec<_>>>()
        .map_err(spill_execution_error)?;
    let mut heads = readers
        .iter_mut()
        .map(read_next_spilled_row)
        .collect::<ExecutionResult<Vec<_>>>()?;
    let mut merged = Vec::new();
    loop {
        let mut best: Option<usize> = None;
        for index in 0..heads.len() {
            let Some(candidate) = heads[index].as_ref() else {
                continue;
            };
            match best {
                None => best = Some(index),
                Some(current) => {
                    let current_row = heads[current].as_ref().expect("selected spill head");
                    if runtime.compare_documents(
                        keys,
                        candidate.document(),
                        current_row.document(),
                    )? == Ordering::Less
                    {
                        best = Some(index);
                    }
                }
            }
        }
        let Some(index) = best else { break };
        merged.push(heads[index].take().expect("selected spill row"));
        heads[index] = read_next_spilled_row(&mut readers[index])?;
    }
    state.rows = merged;
    Ok(())
}

fn spill_execution_error(error: io::Error) -> ExecutionError {
    ExecutionError::evaluation(format!("spill I/O failed: {error}"))
}

fn read_next_spilled_row(reader: &mut SpillRunReader) -> ExecutionResult<Option<ExecutionRow>> {
    reader
        .next_record()
        .map_err(spill_execution_error)?
        .map(|bytes| decode_execution_row(&bytes))
        .transpose()
}

pub(crate) fn encode_execution_row_into(
    row: &ExecutionRow,
    output: &mut Vec<u8>,
) -> ExecutionResult<()> {
    output.clear();
    output.extend_from_slice(row.id().as_bytes());
    output.extend_from_slice(&row.version().get().to_le_bytes());
    output.push(u8::from(row.changed()));
    output.push(match row.origin() {
        ExecutionRowOrigin::Stored => 0,
        ExecutionRowOrigin::Union => 1,
        ExecutionRowOrigin::Synthetic => 2,
    });
    encode_spill_document(output, row.document())
}

pub(crate) fn decode_execution_row(bytes: &[u8]) -> ExecutionResult<ExecutionRow> {
    let mut input = SpillDecoder::new(bytes);
    let id = DocumentId::from_bytes(input.array_16()?);
    let version = DocumentVersion::new(input.u64()?);
    let changed = input.byte()? != 0;
    let origin = match input.byte()? {
        0 => ExecutionRowOrigin::Stored,
        1 => ExecutionRowOrigin::Union,
        2 => ExecutionRowOrigin::Synthetic,
        value => {
            return Err(ExecutionError::evaluation(format!(
                "invalid spill row origin {value}"
            )))
        }
    };
    let document = Arc::new(decode_spill_document(&mut input)?);
    input.finish()?;
    Ok(ExecutionRow::from_spill(
        id, version, document, changed, origin,
    ))
}

fn encode_spill_document(output: &mut Vec<u8>, document: &Document) -> ExecutionResult<()> {
    put_spill_len(output, document.len())?;
    for (name, value) in document.iter() {
        put_spill_string(output, name.as_str())?;
        encode_spill_value(output, value)?;
    }
    Ok(())
}
fn decode_spill_document(input: &mut SpillDecoder<'_>) -> ExecutionResult<Document> {
    let count = input.len()?;
    let mut document = Document::new();
    for _ in 0..count {
        let name = input.string()?.to_owned();
        let value = decode_spill_value(input)?;
        document.insert(name, value);
    }
    Ok(document)
}
fn encode_spill_value(output: &mut Vec<u8>, value: &Value) -> ExecutionResult<()> {
    match value {
        Value::Null => output.push(0),
        Value::Bool(false) => output.push(1),
        Value::Bool(true) => output.push(2),
        Value::Number(Number::Signed(value)) => {
            output.push(3);
            output.extend_from_slice(&value.to_le_bytes());
        }
        Value::Number(Number::Unsigned(value)) => {
            output.push(4);
            output.extend_from_slice(&value.to_le_bytes());
        }
        Value::Number(Number::Float(value)) => {
            output.push(5);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            output.push(6);
            put_spill_string(output, value)?;
        }
        Value::Array(values) => {
            output.push(7);
            put_spill_len(output, values.len())?;
            for value in values.iter() {
                encode_spill_value(output, value)?;
            }
        }
        Value::Object(document) => {
            output.push(8);
            encode_spill_document(output, document)?;
        }
    }
    Ok(())
}
fn decode_spill_value(input: &mut SpillDecoder<'_>) -> ExecutionResult<Value> {
    match input.byte()? {
        0 => Ok(Value::null()),
        1 => Ok(Value::bool(false)),
        2 => Ok(Value::bool(true)),
        3 => Ok(Value::signed(i64::from_le_bytes(input.array_8()?))),
        4 => Ok(Value::unsigned(input.u64()?)),
        5 => Value::float(f64::from_bits(input.u64()?))
            .map_err(|error| ExecutionError::evaluation(format!("invalid spilled float: {error}"))),
        6 => Ok(Value::string(input.string()?)),
        7 => {
            let count = input.len()?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_spill_value(input)?);
            }
            Ok(Value::array(values))
        }
        8 => Ok(Value::object(decode_spill_document(input)?)),
        tag => Err(ExecutionError::evaluation(format!(
            "invalid spill value tag {tag}"
        ))),
    }
}
fn put_spill_len(output: &mut Vec<u8>, value: usize) -> ExecutionResult<()> {
    let value =
        u64::try_from(value).map_err(|_| ExecutionError::evaluation("spill length overflow"))?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}
fn put_spill_string(output: &mut Vec<u8>, value: &str) -> ExecutionResult<()> {
    put_spill_len(output, value.len())?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}
struct SpillDecoder<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> SpillDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn take(&mut self, count: usize) -> ExecutionResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| ExecutionError::evaluation("spill decoder overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| ExecutionError::evaluation("truncated spill record"))?;
        self.position = end;
        Ok(value)
    }
    fn byte(&mut self) -> ExecutionResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn array_8(&mut self) -> ExecutionResult<[u8; 8]> {
        self.take(8)?
            .try_into()
            .map_err(|_| ExecutionError::evaluation("invalid spill integer"))
    }
    fn array_16(&mut self) -> ExecutionResult<[u8; 16]> {
        self.take(16)?
            .try_into()
            .map_err(|_| ExecutionError::evaluation("invalid spill identifier"))
    }
    fn u64(&mut self) -> ExecutionResult<u64> {
        Ok(u64::from_le_bytes(self.array_8()?))
    }
    fn len(&mut self) -> ExecutionResult<usize> {
        usize::try_from(self.u64()?)
            .map_err(|_| ExecutionError::evaluation("spill length does not fit usize"))
    }
    fn string(&mut self) -> ExecutionResult<&'a str> {
        let len = self.len()?;
        std::str::from_utf8(self.take(len)?)
            .map_err(|error| ExecutionError::evaluation(format!("invalid spill utf-8: {error}")))
    }
    fn finish(self) -> ExecutionResult<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(ExecutionError::evaluation("trailing spill bytes"))
        }
    }
}

fn replace_changed_rows(
    transaction: &mut dyn StorageTransaction,
    plan: &PhysicalPlan,
    rows: &mut [ExecutionRow],
    scope: Option<&DocumentScope>,
) -> ExecutionResult<()> {
    for row in rows {
        if !row.changed() || !row.is_stored() {
            continue;
        }

        let document = match scope {
            Some(scope) => scope.enforce(row.document()),
            None => row.shared_document(),
        };
        let result = transaction
            .replace(
                plan.source().collection(),
                row.id(),
                document,
                VersionPrecondition::Exact(row.version()),
            )
            .map_err(ExecutionError::storage)?;

        let stored = result.into_stored();
        let (_, version, document) = stored.into_parts();
        row.version = version;
        row.document = document;
        row.changed = false;
    }

    Ok(())
}

fn delete_rows(
    transaction: &mut dyn StorageTransaction,
    plan: &PhysicalPlan,
    rows: &[ExecutionRow],
) -> ExecutionResult<()> {
    for row in rows {
        if !row.is_stored() {
            continue;
        }

        transaction
            .delete(
                plan.source().collection(),
                row.id(),
                VersionPrecondition::Exact(row.version()),
            )
            .map_err(ExecutionError::storage)?;
    }

    Ok(())
}

fn streaming_load_specification(plan: &PhysicalPlan) -> Option<(PhysicalLoadMode, &[Arc<str>])> {
    plan.operators().iter().find_map(|operator| match operator {
        PhysicalOperator::StreamingLoad { mode, chunks } => Some((*mode, chunks.as_ref())),
        _ => None,
    })
}

fn insert_document(plan: &PhysicalPlan) -> Option<&LogicalInsertDocument> {
    plan.operators().iter().find_map(|operator| match operator {
        PhysicalOperator::Insert { document } => Some(document),
        _ => None,
    })
}

fn contains_delete(plan: &PhysicalPlan) -> bool {
    plan.operators()
        .iter()
        .any(|operator| matches!(operator, PhysicalOperator::Delete))
}

fn synthetic_id(value: &str) -> ExecutionResult<DocumentId> {
    // Synthetic execution rows are not persisted, but they still need a valid
    // binary UUID v7 identifier now that `DocumentId` no longer accepts
    // arbitrary strings such as `_count` or `_unwind`.
    //
    // Derive a stable namespace from the label and let `DocumentId::synthetic`
    // construct a standards-compliant UUID v7 value without touching the
    // persisted-document generator.
    let mut namespace = 0xcbf2_9ce4_8422_2325u64;
    for byte in value.bytes() {
        namespace ^= u64::from(byte);
        namespace = namespace.wrapping_mul(0x0000_0100_0000_01b3);
    }

    Ok(DocumentId::synthetic(namespace, 1))
}

fn usize_to_u64(value: usize) -> ExecutionResult<u64> {
    u64::try_from(value).map_err(|_| ExecutionError::counter_overflow())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        query::PhysicalSource,
        storage::{memory::MemoryStorage, CollectionId},
        Value,
    };

    #[test]
    fn execution_row_exposes_id_as_virtual_evaluation_field() {
        let id = DocumentId::parse("019fb7ae-9588-7057-830a-01bdb143b7ce").unwrap();
        let row = ExecutionRow {
            id,
            version: DocumentVersion::INITIAL,
            document: Arc::new(Document::from_fields([("name", Value::from("Alice"))])),
            changed: false,
            origin: ExecutionRowOrigin::Stored,
        };

        let evaluation = row.evaluation_document();

        assert_eq!(
            evaluation.get("_id"),
            Some(&Value::from("019fb7ae-9588-7057-830a-01bdb143b7ce")),
        );
        assert!(!row.document().contains_key("_id"));
    }

    #[test]
    fn replacing_a_row_never_persists_virtual_id_metadata() {
        let id = DocumentId::parse("019fb7ae-9588-7057-830a-01bdb143b7ce").unwrap();
        let mut row = ExecutionRow {
            id,
            version: DocumentVersion::INITIAL,
            document: Arc::new(Document::default()),
            changed: false,
            origin: ExecutionRowOrigin::Stored,
        };
        let mut replacement = row.evaluation_document();
        replacement.insert("active", true);

        row.replace_document(Arc::new(replacement), true);

        assert!(!row.document().contains_key("_id"));
        assert_eq!(row.document().get("active"), Some(&Value::from(true)));
    }

    #[derive(Debug, Default)]
    struct NeverCalledRuntime;

    impl ExecutionRuntime for NeverCalledRuntime {
        fn evaluate_predicate(
            &self,
            _expression: &Expression,
            _document: &Document,
        ) -> ExecutionResult<bool> {
            panic!("runtime must not be called for an empty collection")
        }

        fn apply_set(
            &self,
            _assignments: &[SetAssignment],
            _document: &Document,
        ) -> ExecutionResult<Arc<Document>> {
            panic!("runtime must not be called for an empty collection")
        }
    }

    #[inline]
    fn source() -> PhysicalSource {
        PhysicalSource::collection_scan(CollectionId::parse("users").unwrap())
    }

    #[test]
    fn executes_empty_read_scan() {
        let storage = MemoryStorage::new();
        let plan = PhysicalPlan::new(source(), []).unwrap();

        let output = Executor::new()
            .execute(&storage, &NeverCalledRuntime, &plan)
            .unwrap();

        assert!(output.is_empty());
        assert!(!output.committed());
        assert_eq!(output.statistics().scanned(), 0);
        assert_eq!(output.statistics().returned(), 0);
        assert!(output
            .statistics()
            .strategies()
            .contains(ExecutionStrategy::CollectionScan));
    }

    #[test]
    fn executes_empty_write_plan_and_commits() {
        let storage = MemoryStorage::new();
        let stage = StageName::parse("archive").unwrap();
        let operator = PhysicalOperator::custom(stage, "", true, false).unwrap();
        let plan = PhysicalPlan::new(source(), [operator]).unwrap();

        let output = Executor::new()
            .execute(&storage, &NeverCalledRuntime, &plan)
            .unwrap();

        assert!(output.is_empty());
        assert!(output.committed());
        assert_eq!(output.commit(), Some(CommitResult::default()));
        assert_eq!(output.statistics().mutated(), 0);
    }

    #[test]
    fn empty_delete_commits_no_mutations() {
        let storage = MemoryStorage::new();
        let plan = PhysicalPlan::new(source(), [PhysicalOperator::delete()]).unwrap();

        let output = Executor::new()
            .execute(&storage, &NeverCalledRuntime, &plan)
            .unwrap();

        assert!(output.is_empty());
        assert!(output.committed());
        assert_eq!(output.statistics().deleted(), 0);
    }

    #[test]
    fn empty_limit_and_skip_do_not_call_runtime() {
        let storage = MemoryStorage::new();
        let plan = PhysicalPlan::new(
            source(),
            [PhysicalOperator::skip(10), PhysicalOperator::limit(5)],
        )
        .unwrap();

        let output = Executor::new()
            .execute(&storage, &NeverCalledRuntime, &plan)
            .unwrap();

        assert!(output.is_empty());
        assert_eq!(output.statistics().filtered(), 0);
    }

    #[test]
    fn simple_count_uses_storage_count_without_scanning() {
        #[derive(Debug)]
        struct CountOnlyStorage;

        #[derive(Debug)]
        struct CountOnlyRead;

        impl StorageRead for CountOnlyRead {
            fn get(
                &self,
                _collection: &CollectionId,
                _id: &DocumentId,
            ) -> crate::storage::StorageResult<Option<StoredDocument>> {
                unreachable!()
            }

            fn scan(
                &self,
                _collection: &CollectionId,
                _options: ScanOptions,
            ) -> crate::storage::StorageResult<Vec<StoredDocument>> {
                panic!("simple count must not scan documents")
            }

            fn count(&self, _collection: &CollectionId) -> crate::storage::StorageResult<u64> {
                Ok(5_000_000)
            }

            fn collection_exists(
                &self,
                _collection: &CollectionId,
            ) -> crate::storage::StorageResult<bool> {
                Ok(true)
            }

            fn collections(&self) -> crate::storage::StorageResult<Vec<CollectionId>> {
                Ok(vec![CollectionId::parse("users").unwrap()])
            }
        }

        impl StorageEngine for CountOnlyStorage {
            fn read(&self) -> crate::storage::StorageResult<Box<dyn StorageRead + '_>> {
                Ok(Box::new(CountOnlyRead))
            }

            fn begin(&self) -> crate::storage::StorageResult<Box<dyn StorageTransaction + '_>> {
                panic!("read-only count must not begin a transaction")
            }
        }

        #[derive(Debug)]
        struct CountRuntime;

        impl ExecutionRuntime for CountRuntime {
            fn evaluate_predicate(
                &self,
                _expression: &Expression,
                _document: &Document,
            ) -> ExecutionResult<bool> {
                unreachable!("fast count must not evaluate predicates")
            }

            fn apply_set(
                &self,
                _assignments: &[SetAssignment],
                _document: &Document,
            ) -> ExecutionResult<Arc<Document>> {
                unreachable!("fast count must not apply transformations")
            }

            fn count_document(&self, alias: &str, count: u64) -> ExecutionResult<Arc<Document>> {
                assert_eq!(alias, "total");
                assert_eq!(count, 5_000_000);
                Ok(Arc::new(Document::from_fields([(
                    alias,
                    crate::Value::unsigned(count),
                )])))
            }
        }

        let plan =
            PhysicalPlan::new(source(), [PhysicalOperator::count("total").unwrap()]).unwrap();
        let output = Executor::new()
            .execute(&CountOnlyStorage, &CountRuntime, &plan)
            .unwrap();

        assert_eq!(output.statistics().scanned(), 0);
        assert_eq!(output.statistics().returned(), 1);
        assert_eq!(
            output.rows()[0].document().get("total"),
            Some(&crate::Value::unsigned(5_000_000))
        );
    }

    #[test]
    fn count_on_empty_collection_calls_count_runtime() {
        #[derive(Debug)]
        struct CountRuntime;

        impl ExecutionRuntime for CountRuntime {
            fn evaluate_predicate(
                &self,
                _expression: &Expression,
                _document: &Document,
            ) -> ExecutionResult<bool> {
                unreachable!()
            }

            fn apply_set(
                &self,
                _assignments: &[SetAssignment],
                _document: &Document,
            ) -> ExecutionResult<Arc<Document>> {
                unreachable!()
            }

            fn count_document(&self, alias: &str, count: u64) -> ExecutionResult<Arc<Document>> {
                assert_eq!(alias, "total");
                assert_eq!(count, 0);

                Err(ExecutionError::evaluation("test sentinel"))
            }
        }

        let storage = MemoryStorage::new();
        let plan =
            PhysicalPlan::new(source(), [PhysicalOperator::count("total").unwrap()]).unwrap();

        let error = Executor::new()
            .execute(&storage, &CountRuntime, &plan)
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            ExecutionErrorKind::Evaluation { .. }
        ));
    }

    #[test]
    fn union_on_empty_secondary_collection_is_a_noop() {
        let storage = MemoryStorage::new();
        let nested = PhysicalSubPipeline::empty();
        let union = PhysicalOperator::union(
            CollectionId::parse("archived_users").unwrap(),
            None::<&str>,
            nested,
        )
        .unwrap();

        let plan = PhysicalPlan::new(source(), [union]).unwrap();
        let output = Executor::new()
            .execute(&storage, &NeverCalledRuntime, &plan)
            .unwrap();

        assert!(output.is_empty());
        assert!(!output.committed());
    }

    #[test]
    fn lookup_on_empty_input_does_not_call_runtime() {
        let storage = MemoryStorage::new();
        let lookup = PhysicalOperator::lookup(
            CollectionId::parse("workspace").unwrap(),
            Some("w"),
            "public",
            PhysicalSubPipeline::empty(),
        )
        .unwrap();

        let plan = PhysicalPlan::new(source(), [lookup]).unwrap();
        let output = Executor::new()
            .execute(&storage, &NeverCalledRuntime, &plan)
            .unwrap();

        assert!(output.is_empty());
    }

    #[test]
    fn typed_insert_is_forwarded_to_runtime() {
        #[derive(Debug)]
        struct InsertRuntime;

        impl ExecutionRuntime for InsertRuntime {
            fn evaluate_predicate(
                &self,
                _expression: &Expression,
                _document: &Document,
            ) -> ExecutionResult<bool> {
                unreachable!()
            }

            fn apply_set(
                &self,
                _assignments: &[SetAssignment],
                _document: &Document,
            ) -> ExecutionResult<Arc<Document>> {
                unreachable!()
            }

            fn prepare_insert(
                &self,
                document: &LogicalInsertDocument,
            ) -> ExecutionResult<PreparedInsertDocument> {
                assert_eq!(document.object().fields().count(), 1);
                Err(ExecutionError::evaluation("insert test sentinel"))
            }
        }

        let storage = MemoryStorage::new();
        let document = LogicalInsertDocument::parse(r#"{name:"Alice"}"#).unwrap();
        let plan = PhysicalPlan::new(source(), [PhysicalOperator::insert(document)]).unwrap();

        let error = Executor::new()
            .execute(&storage, &InsertRuntime, &plan)
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            ExecutionErrorKind::Evaluation { .. }
        ));
    }

    #[test]
    fn streaming_load_calls_runtime_and_commits_empty_mutation_list() {
        #[derive(Debug)]
        struct StreamingRuntime;

        impl ExecutionRuntime for StreamingRuntime {
            fn evaluate_predicate(
                &self,
                _expression: &Expression,
                _document: &Document,
            ) -> ExecutionResult<bool> {
                unreachable!()
            }

            fn apply_set(
                &self,
                _assignments: &[SetAssignment],
                _document: &Document,
            ) -> ExecutionResult<Arc<Document>> {
                unreachable!()
            }

            fn prepare_streaming_load(
                &self,
                collection: &crate::storage::CollectionId,
                _storage: &dyn StorageRead,
                mode: PhysicalLoadMode,
                chunks: &[Arc<str>],
            ) -> ExecutionResult<Vec<StreamingLoadMutation>> {
                assert_eq!(collection.as_str(), "users");
                assert_eq!(mode, PhysicalLoadMode::Merge);
                assert_eq!(chunks.len(), 2);
                Ok(Vec::new())
            }
        }

        let storage = MemoryStorage::new();
        let operator =
            PhysicalOperator::streaming_load(PhysicalLoadMode::Merge, ["batch1", "batch2"])
                .unwrap();
        let plan = PhysicalPlan::new(source(), [operator]).unwrap();

        let output = Executor::new()
            .execute(&storage, &StreamingRuntime, &plan)
            .unwrap();

        assert!(output.committed());
        assert_eq!(output.statistics().mutated(), 0);
        assert_eq!(output.statistics().scanned(), 0);
    }

    #[test]
    fn pivot_calls_runtime_on_empty_collection() {
        use crate::query::logical_plan::{PivotAggregate, PivotSpecification, PivotValue};

        #[derive(Debug)]
        struct PivotRuntime;

        impl ExecutionRuntime for PivotRuntime {
            fn evaluate_predicate(
                &self,
                _expression: &Expression,
                _document: &Document,
            ) -> ExecutionResult<bool> {
                unreachable!()
            }

            fn apply_set(
                &self,
                _assignments: &[SetAssignment],
                _document: &Document,
            ) -> ExecutionResult<Arc<Document>> {
                unreachable!()
            }

            fn pivot_documents(
                &self,
                specification: &PivotSpecification,
                documents: &[Arc<Document>],
            ) -> ExecutionResult<Vec<SyntheticDocument>> {
                assert_eq!(specification.rows().len(), 1);
                assert_eq!(specification.columns().len(), 1);
                assert_eq!(specification.values().len(), 1);
                assert!(documents.is_empty());

                Err(ExecutionError::evaluation("pivot test sentinel"))
            }
        }

        let specification = PivotSpecification::new(
            [ExpressionFieldPath::new(["region"]).unwrap()],
            [ExpressionFieldPath::new(["month"]).unwrap()],
            [PivotValue::new(
                ExpressionFieldPath::new(["revenue"]).unwrap(),
                PivotAggregate::Sum,
                None::<&str>,
            )
            .unwrap()],
        )
        .unwrap();

        let storage = MemoryStorage::new();
        let plan = PhysicalPlan::new(source(), [PhysicalOperator::pivot(specification)]).unwrap();

        let error = Executor::new()
            .execute(&storage, &PivotRuntime, &plan)
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            ExecutionErrorKind::Evaluation { .. }
        ));
    }

    #[test]
    fn document_scope_matches_only_its_place_and_app_instance() {
        let scope = DocumentScope::new("place-a", "app-1");
        let matching = Document::from_fields([
            ("_place", Value::from("place-a")),
            ("_app_instance", Value::from("app-1")),
        ]);
        let other_instance = Document::from_fields([
            ("_place", Value::from("place-a")),
            ("_app_instance", Value::from("app-2")),
        ]);
        assert!(scope.matches(&matching));
        assert!(!scope.matches(&other_instance));
        assert!(!scope.matches(&Document::new()));
    }

    #[test]
    fn document_scope_overwrites_untrusted_scope_fields() {
        let scope = DocumentScope::new("place-a", "app-1");
        let document = Document::from_fields([
            ("_place", Value::from("place-evil")),
            ("_app_instance", Value::from("app-evil")),
            ("name", Value::from("item")),
        ]);
        let scoped = scope.enforce(&document);
        assert_eq!(scoped.get("_place"), Some(&Value::from("place-a")));
        assert_eq!(scoped.get("_app_instance"), Some(&Value::from("app-1")));
        assert_eq!(scoped.get("name"), Some(&Value::from("item")));
    }

    #[test]
    fn place_document_scope_matches_all_instances_in_the_place() {
        let scope = DocumentScope::for_place("place-a");
        let app_one = Document::from_fields([
            ("_place", Value::from("place-a")),
            ("_app_instance", Value::from("app-1")),
        ]);
        let app_two = Document::from_fields([
            ("_place", Value::from("place-a")),
            ("_app_instance", Value::from("app-2")),
        ]);
        let place_only = Document::from_fields([("_place", Value::from("place-a"))]);
        let other_place = Document::from_fields([
            ("_place", Value::from("place-b")),
            ("_app_instance", Value::from("app-1")),
        ]);

        assert!(scope.matches(&app_one));
        assert!(scope.matches(&app_two));
        assert!(scope.matches(&place_only));
        assert!(!scope.matches(&other_place));
    }

    #[test]
    fn place_document_scope_forces_place_without_overwriting_app_instance() {
        let scope = DocumentScope::for_place("place-a");
        let document = Document::from_fields([
            ("_place", Value::from("place-evil")),
            ("_app_instance", Value::from("app-2")),
            ("name", Value::from("item")),
        ]);
        let scoped = scope.enforce(&document);

        assert_eq!(scoped.get("_place"), Some(&Value::from("place-a")));
        assert_eq!(scoped.get("_app_instance"), Some(&Value::from("app-2")));
        assert_eq!(scoped.get("name"), Some(&Value::from("item")));
    }

    #[test]
    fn execution_public_types_are_send_and_sync() {
        fn assert_send_and_sync<T: Send + Sync>() {}

        assert_send_and_sync::<Executor>();
        assert_send_and_sync::<DocumentScope>();
        assert_send_and_sync::<ExecutionOutput>();
        assert_send_and_sync::<ExecutionRow>();
        assert_send_and_sync::<ExecutionRowOrigin>();
        assert_send_and_sync::<ExecutionError>();
        assert_send_and_sync::<ExecutionStatistics>();
        assert_send_and_sync::<PreparedInsertDocument>();
        assert_send_and_sync::<SyntheticDocument>();
        assert_send_and_sync::<LookupDocuments>();
        assert_send_and_sync::<StreamingLoadMutation>();
    }
}
