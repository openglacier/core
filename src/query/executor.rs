#![cfg_attr(rustfmt, rustfmt_skip)]
//! Execution rows, statistics and errors, and the bounded structures the
//! executors share (top-N selection, reservoir, spilled rows).

use std::{cmp::Ordering, error::Error as StdError, fmt, sync::Arc};

use crate::{
    helpers::{enforce_document_scope, unix_time_nanos, PLACE_SCOPE_FIELD},
    memory::{MemoryClass, MemoryGovernor, MemoryReservation, MemoryReservationError},
    model::{compare_for_sort, CoercionPolicy, Document, Number, Value},
    storage::{
        CommitResult, DocumentId, DocumentVersion, ProjectedValueRef, StorageEngine,
        StorageError, StorageMutation, StoredDocument, VersionPrecondition,
    },
};

use super::{ExpressionFieldPath, PhysicalLoadMode, PhysicalOperator, PhysicalPlan, SortKey, Stage};

use super::plan_values::InsertDocument;
use super::QueryRuntime;

/// Trusted document scope applied below query syntax.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentScope {
    place_id: Arc<str>,
    app_instance_id: Option<Arc<str>>,
    /// Values each field may hold in the rows read, when present, see
    /// [`Self::with_filters`].
    filters: Arc<[(Arc<str>, Arc<[Value]>)]>,
}

impl DocumentScope {
    /// Creates a Place + `AppInstance` scope.
    #[must_use]
    pub fn new(place_id: impl Into<Arc<str>>, app_instance_id: impl Into<Arc<str>>) -> Self {
        Self {
            place_id: place_id.into(),
            app_instance_id: Some(app_instance_id.into()),
            filters: Arc::new([]),
        }
    }

    /// Creates a Place-wide scope without restricting documents to one `AppInstance`.
    #[must_use]
    pub fn for_place(place_id: impl Into<Arc<str>>) -> Self {
        Self {
            place_id: place_id.into(),
            app_instance_id: None,
            filters: Arc::new([]),
        }
    }

    /// The scope reading only the rows whose `field`, when present, holds
    /// one of its values, for each filter: the restriction of a member of
    /// the Place to some entities, applied to every read.
    #[must_use]
    pub fn with_filters(mut self, filters: impl IntoIterator<Item = (String, Vec<Value>)>) -> Self {
        self.filters = filters.into_iter().map(|(field, values)| (Arc::from(field), Arc::from(values))).collect();
        self
    }

    /// The App instance it keeps, if any.
    #[must_use]
    pub fn app_instance_id(&self) -> Option<&str> {
        self.app_instance_id.as_deref()
    }

    /// The read-only `plan` reading only the rows of this scope (of its
    /// Place alone, with `place_only`), see [`super::scoped_read_plan`].
    /// The Place it keeps.
    #[must_use]
    pub fn place_id(&self) -> &str {
        &self.place_id
    }

    pub(crate) fn read_plan(&self, plan: &PhysicalPlan, place_only: bool) -> Option<PhysicalPlan> {
        super::scope::scoped_plan(plan, &self.place_id, self.app_instance_id.as_deref().filter(|_| !place_only), &self.filters)
    }

    /// `document`, written to `collection`, with the fields of this scope;
    /// refused when it leaves the scope's filters (a field it holds takes
    /// one of their values, a strict filter's field must be there), so a
    /// write cannot reach rows its writer does not read.
    pub(crate) fn enforce(&self, collection: &str, document: &Document) -> Result<Arc<Document>, String> {
        for (field, values, strict) in super::scope::filters_of(&self.filters, collection) {
            match document.get(field).filter(|value| !value.is_null()) {
                Some(value) if values.contains(value) => {}
                None if !strict => {}
                _ => return Err(format!("this row is outside your access in the Place: {field} must be one of {}", crate::helpers::value_to_json(&Value::array(values.iter().cloned())))),
            }
        }
        Ok(Arc::new(if let Some(app_instance_id) = self.app_instance_id.as_deref() { enforce_document_scope(document, &self.place_id, app_instance_id) } else {
            let mut scoped = document.clone();
            scoped.insert(PLACE_SCOPE_FIELD, self.place_id.as_ref());
            scoped
        }))
    }

    /// Its filters, keyed `<field>` or `<collection>:<field>` (strict).
    #[must_use]
    pub fn filters(&self) -> &[(Arc<str>, Arc<[Value]>)] {
        &self.filters
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
    pub const fn insert(id: DocumentId, document: Arc<Document>) -> Self {
        Self::Insert { id, document }
    }

    #[must_use]
    pub const fn replace(
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

/// Document-level and set-level runtime used by the executor.
///
/// Default implementations report unsupported operators so a minimal runtime
/// only needs to implement predicate evaluation and `set`.
/// Incremental state of one pivot.
///
/// Implementations retain only aggregate cells, one per (row key, column), never
/// the source rows.
pub trait IncrementalPivotAccumulator: Send {
    /// Folds one document into the pivot.
    fn push(&mut self, document: &Document) -> ExecutionResult<()>;

    /// Whether [`Self::push_slots`] is implemented.
    fn folds_slots(&self) -> bool {
        false
    }

    /// Folds one row held as values of top-level `fields`, as [`Self::push`]
    /// does for the document holding the present values. Only called when
    /// [`Self::folds_slots`] holds.
    fn push_slots(&mut self, fields: &[ExpressionFieldPath], values: &[Option<Value>]) -> ExecutionResult<()> {
        let _ = (fields, values);
        Err(ExecutionError::unsupported_operator("pivot", "accumulator folds documents only"))
    }

    /// Whether [`Self::push_refs`] is implemented.
    fn folds_refs(&self) -> bool {
        false
    }

    /// Folds one row given by the values of the specification's row,
    /// column and value paths, in that order, as [`Self::push`] does for a
    /// document holding them. Only called when [`Self::folds_refs`] holds.
    fn push_refs(
        &mut self,
        rows: &[Option<crate::storage::ProjectedValueRef<'_>>],
        columns: &[Option<crate::storage::ProjectedValueRef<'_>>],
        values: &[Option<crate::storage::ProjectedValueRef<'_>>],
    ) -> ExecutionResult<()> {
        let _ = (rows, columns, values);
        Err(ExecutionError::unsupported_operator("pivot", "accumulator folds documents only"))
    }

    /// Folds the rows of a batch at `selection`, the specification's row,
    /// column and value paths held by `rows`, `columns` and `values`
    /// (`None`: no row holds the path), as [`Self::push_refs`] does row by
    /// row. Returns `false` when not implemented: nothing was folded.
    fn push_batch(
        &mut self,
        rows: &[Option<&super::Column>],
        columns: &[Option<&super::Column>],
        values: &[Option<&super::Column>],
        selection: &[u32],
    ) -> ExecutionResult<bool> {
        let _ = (rows, columns, values, selection);
        Ok(false)
    }

    /// Estimated bytes currently retained by the aggregate state.
    fn retained_bytes(&self) -> usize;

    /// Moves the cells out, as records in row-key order for a run on disk.
    fn spill(&mut self) -> ExecutionResult<Vec<Vec<u8>>>;

    /// Emits the pivot result documents, merging the cells of the `runs`
    /// (records [`Self::spill`] wrote) with those in memory.
    fn finish(self: Box<Self>, runs: Vec<PivotRun<'_>>, emit: &mut dyn FnMut(SyntheticDocument) -> ExecutionResult<()>) -> ExecutionResult<()>;
}

/// Records of one spilled pivot run, in order.
pub type PivotRun<'r> = Box<dyn Iterator<Item = ExecutionResult<Vec<u8>>> + 'r>;

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

    /// The same group semantics as a [`super::DenseGroups`] holding every
    /// group of the stage, when the runtime provides one.
    fn into_dense(self: Box<Self>) -> Option<super::DenseGroups> {
        None
    }

    /// Materializes a compact mergeable partial state for external spill.
    ///
    /// Runtimes that implement this capability let the engine spill one bounded
    /// aggregate state per group instead of retaining complete source rows.
    fn finish_partial( self: Box<Self>, _ordinal: u64, ) -> ExecutionResult<Option<SyntheticDocument>> {
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

/// Inserts the document of an `insert` plan in one transaction.
pub(crate) fn execute_insert( storage: &dyn StorageEngine, runtime: &QueryRuntime, plan: &PhysicalPlan, document: &InsertDocument, scope: Option<&DocumentScope>, ) -> ExecutionResult<ExecutionOutput> {
    let (id, mut document) = runtime.prepare_insert(document)?.into_parts();
    if let Some(scope) = scope {
        document = scope.enforce(plan.source().collection().as_str(), document.as_ref()).map_err(ExecutionError::evaluation)?;
    }
    let mut transaction = storage.begin().map_err(ExecutionError::storage)?;
    let result = transaction.insert(plan.source().collection(), id, document).map_err(ExecutionError::storage)?;
    let mut row = ExecutionRow::from_stored(result.into_stored());
    row.document = Arc::new(row.evaluation_document());
    let commit = transaction.commit().map_err(ExecutionError::storage)?;
    Ok(ExecutionOutput { rows: vec![row], statistics: ExecutionStatistics::default().with_commit(1, Some(commit)), commit: Some(commit) })
}

/// Imports the chunks of a streaming `load` in one atomic batch; a `compact`
/// import returns no row.
/// Loads the rows of `chunks`; in a scope, each row gets its scope fields and none may replace a stored document.
pub(crate) fn execute_streaming_load( storage: &dyn StorageEngine, runtime: &QueryRuntime, plan: &PhysicalPlan, (mode, chunks): (PhysicalLoadMode, &[Arc<str>]), memory_governor: &MemoryGovernor, compact: bool, scope: Option<&DocumentScope>, ) -> ExecutionResult<ExecutionOutput> {
    let chunk_bytes = chunks.iter().map(|chunk| chunk.len()).sum::<usize>();
    let _reservation = reserve_import_memory(memory_governor, chunk_bytes.saturating_mul(if compact { 2 } else { 3 }))?;
    let read = storage.read().map_err(ExecutionError::storage)?;
    let collection = plan.source().collection();
    let mutations = runtime.prepare_streaming_load(collection, read.as_ref(), mode, chunks)?.into_iter().map(|mutation| match (mutation, scope) {
        (StreamingLoadMutation::Insert { id, document }, None) => Ok(StorageMutation::insert(id, document)),
        (StreamingLoadMutation::Insert { id, document }, Some(scope)) => Ok(StorageMutation::insert(id, scope.enforce(collection.as_str(), &document).map_err(ExecutionError::evaluation)?)),
        (StreamingLoadMutation::Replace { id, document, precondition }, None) => Ok(StorageMutation::replace(id, document, precondition)),
        (StreamingLoadMutation::Replace { .. }, Some(_)) => Err(ExecutionError::unsupported_operator("insert", "a scoped insert cannot replace a stored document")),
    }).collect::<ExecutionResult<Vec<_>>>()?.into_iter();
    let (rows, commit) = if compact {
        (Vec::new(), storage.apply_batch_atomic_summary(collection, mutations.collect()).map_err(ExecutionError::storage)?)
    } else {
        let (stored, commit) = storage.apply_batch_atomic(collection, mutations.collect()).map_err(ExecutionError::storage)?;
        (stored.into_iter().map(ExecutionRow::from_stored).collect::<Vec<_>>(), commit)
    };
    let statistics = ExecutionStatistics::default().with_commit(usize_to_u64(rows.len())?, Some(commit));
    Ok(ExecutionOutput { rows, statistics, commit: Some(commit) })
}

/// Complete execution output.
#[derive(Clone, Debug)]
pub struct ExecutionOutput {
    rows: Vec<ExecutionRow>,
    statistics: ExecutionStatistics,
    commit: Option<CommitResult>,
}

impl ExecutionOutput {
    pub(crate) const fn new(rows: Vec<ExecutionRow>, statistics: ExecutionStatistics, commit: Option<CommitResult>) -> Self {
        Self { rows, statistics, commit }
    }

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
    pub const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns the number of emitted rows.
    #[must_use]
    #[inline]
    pub const fn len(&self) -> usize {
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

    /// This row, read by a `union` branch: a stored row becomes a union row.
    #[must_use]
    pub(crate) fn into_union(mut self) -> Self {
        if matches!(self.origin, ExecutionRowOrigin::Stored) {
            self.origin = ExecutionRowOrigin::Union;
        }
        self
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

    const fn from_spill(
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

    /// The current result document to change in place (copied only when
    /// shared).
    pub(crate) fn document_mut(&mut self) -> &mut Document {
        Arc::make_mut(&mut self.document)
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

    /// The document a stage reads: the stored document itself, or a copy
    /// carrying the virtual `_id` field when the stage reads it.
    pub(crate) fn stage_input(&self, needs_id: bool) -> std::borrow::Cow<'_, Document> {
        if needs_id {
            std::borrow::Cow::Owned(self.evaluation_document())
        } else {
            std::borrow::Cow::Borrowed(self.document.as_ref())
        }
    }

    /// The row as `stored` by a write.
    pub(crate) fn committed(&mut self, stored: StoredDocument) {
        (_, self.version, self.document) = stored.into_parts();
        self.changed = false;
    }

    pub(crate) fn replace_document(&mut self, document: Arc<Document>, mark_changed: bool) {
        // Only a document carrying the virtual `_id` needs a copy without it.
        self.document = if document.contains_key("_id") {
            let mut document = document.as_ref().clone();
            document.remove("_id");
            Arc::new(document)
        } else {
            document
        };
        self.changed |= mark_changed;
    }
}

/// Backend-independent locator retained while a row stays on the projected-value
/// access vector. It is intentionally sufficient to hydrate the committed row
/// later without retaining a `Document`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProjectedRowLocator {
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

/// Conservative working-set estimate for one projected blocking row, including
/// locator storage, flattened value capacity and stable-sort index workspace.
/// It deliberately overcharges small scalar rows so the governor remains the
/// authority even when `Vec` capacities temporarily grow geometrically.
pub fn projected_row_working_bytes_refs( values: &[Option<ProjectedValueRef<'_>>], slots: &[usize], ) -> usize {
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
        None |
Some(ProjectedValueRef::Null | ProjectedValueRef::Bool(_) |
ProjectedValueRef::Signed(_) | ProjectedValueRef::Unsigned(_) |
ProjectedValueRef::Float(_)) => 0,
        Some(ProjectedValueRef::String(value)) => value.len(),
        Some(ProjectedValueRef::Owned(value)) => spill_value_encoded_len(value),
    }
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
    InMemoryPivot,
    /// Rows were read as partial documents holding only the fields the query reads.
    PartialDocuments,
    /// A lookup matched rows through an equality index built once.
    HashJoin,
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
            Self::InMemoryPivot => "in_memory_pivot",
            Self::PartialDocuments => "partial_documents",
            Self::HashJoin => "hash_join",
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
            Self::InMemoryPivot => 1 << 11,
            Self::PartialDocuments => 1 << 12,
            Self::HashJoin => 1 << 13,
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
        const ALL: [ExecutionStrategy; 14] = [
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
            ExecutionStrategy::InMemoryPivot,
            ExecutionStrategy::PartialDocuments,
            ExecutionStrategy::HashJoin,
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

    /// Builds statistics for a streamed read-only pipeline.
    #[must_use]
    pub fn streamed_pipeline( scanned: u64, filtered: u64, returned: u64, strategy: ExecutionStrategy, ) -> Self {
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
    pub const fn streamed_with_strategies( scanned: u64, filtered: u64, returned: u64, strategies: ExecutionStrategies, ) -> Self {
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

    /// These statistics with `returned` rows and the counts of `commit`.
    #[must_use]
    pub(crate) fn with_commit(self, returned: u64, commit: Option<CommitResult>) -> Self {
        let commit = commit.unwrap_or_default();
        Self { returned, inserted: commit.inserted(), replaced: commit.replaced(), deleted: commit.deleted(), ..self }
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
    pub const fn storage(error: StorageError) -> Self {
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
    pub fn unsupported_operator( operator: impl Into<Arc<str>>, reason: impl Into<Arc<str>>, ) -> Self {
        Self::new(ExecutionErrorKind::UnsupportedOperator {
            operator: operator.into(),
            reason: reason.into(),
        })
    }

    const fn counter_overflow() -> Self {
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

/// Uniform random sample of at most `capacity` items from a stream of unknown
/// length (reservoir sampling, algorithm L: once full, the number of items to
/// skip before the next replacement is drawn, so most items cost a counter
/// decrement). Memory stays bounded by `capacity` whatever the input size.
#[derive(Debug)]
pub struct Reservoir<T> {
    capacity: usize,
    seen: u64,
    state: u64,
    /// `W` of algorithm L.
    weight: f64,
    skip: u64,
    items: Vec<T>,
}

impl<T> Reservoir<T> {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self { capacity, seen: 0, state: sample_seed(), weight: 1.0, skip: 0, items: Vec::with_capacity(capacity.min(4096)) }
    }

    #[inline]
    pub fn push(&mut self, item: T) {
        self.seen += 1;
        if self.skip > 0 {
            self.skip -= 1;
            return;
        }
        if self.items.len() < self.capacity {
            self.items.push(item);
            if self.items.len() == self.capacity {
                self.weight = self.draw_weight();
                self.skip = self.draw_skip();
            }
            return;
        }
        if self.capacity == 0 {
            return;
        }
        let slot = (self.next() % self.capacity as u64) as usize;
        self.items[slot] = item;
        self.weight *= self.draw_weight();
        self.skip = self.draw_skip();
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15).rotate_left(27).wrapping_mul(0x94d0_49bb_1331_11eb);
        self.state ^ (self.state >> 31)
    }

    /// Uniform in (0, 1].
    fn uniform(&mut self) -> f64 {
        ((self.next() >> 11) + 1) as f64 * (1.0 / (1_u64 << 53) as f64)
    }

    fn draw_weight(&mut self) -> f64 {
        (self.uniform().ln() / self.capacity as f64).exp()
    }

    fn draw_skip(&mut self) -> u64 {
        let skip = (self.uniform().ln() / (1.0 - self.weight).ln()).floor();
        if skip.is_finite() && skip >= 0.0 { skip.min(u64::MAX as f64) as u64 } else { 0 }
    }

    /// Number of items offered so far.
    #[must_use]
    pub const fn seen(&self) -> u64 {
        self.seen
    }

    #[must_use]
    pub fn into_items(self) -> Vec<T> {
        self.items
    }
}

pub(crate) fn sample_seed() -> u64 {
    let nanos = unix_time_nanos();
    (nanos as u64) ^ ((nanos >> 64) as u64) ^ u64::from(std::process::id())
}

fn reserve_import_memory( governor: &MemoryGovernor, bytes: usize, ) -> ExecutionResult<MemoryReservation> {
    governor
        .reserve(MemoryClass::Import, bytes)
        .map_err(|error| ExecutionError::memory_limit("streaming import", error))
}

/// The first `limit` rows of a stream in sort order, in bounded memory.
///
/// The kept entries form a binary heap whose root is the worst: once full, a
/// row costs one comparison and only a row that beats the worst replaces the
/// root and sifts down, O(log N). Rows that compare equal keep their arrival
/// order.
#[derive(Debug)]
pub(crate) struct TopSelection<T> {
    limit: usize,
    entries: Vec<(u64, T)>,
    next_sequence: u64,
}

impl<T> TopSelection<T> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            entries: Vec::with_capacity(limit.min(4096)),
            next_sequence: 0,
        }
    }

    /// The worst kept entry once the selection is full.
    fn worst(&self) -> Option<&T> {
        if self.limit == 0 || self.entries.len() < self.limit {
            return None;
        }
        self.entries.first().map(|(_, entry)| entry)
    }

    /// Offers the next row. Once full, `beats_worst` decides whether it sorts
    /// before the worst kept entry; `keep` builds the entry of a kept row.
    /// Returns the entry the row evicted.
    pub(crate) fn offer<C>( &mut self, row: C, beats_worst: impl FnOnce(&C, &T) -> ExecutionResult<bool>, keep: impl FnOnce(C) -> ExecutionResult<T>, mut compare: impl FnMut(&T, &T) -> ExecutionResult<Ordering>, ) -> ExecutionResult<Option<T>> {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.limit == 0 {
            return Ok(None);
        }
        if self.entries.len() < self.limit {
            self.entries.push((sequence, keep(row)?));
            let mut index = self.entries.len() - 1;
            while index > 0 {
                let parent = (index - 1) / 2;
                if !self.sorts_after(index, parent, &mut compare)? {
                    break;
                }
                self.entries.swap(index, parent);
                index = parent;
            }
            return Ok(None);
        }
        // Equal candidates arrive later and therefore cannot improve a stable Top-N.
        if !beats_worst(&row, &self.entries[0].1)? {
            return Ok(None);
        }
        let (_, evicted) = std::mem::replace(&mut self.entries[0], (sequence, keep(row)?));
        let mut index = 0;
        loop {
            let left = 2 * index + 1;
            if left >= self.entries.len() {
                break;
            }
            let right = left + 1;
            let child = if right < self.entries.len() && self.sorts_after(right, left, &mut compare)? { right } else { left };
            if !self.sorts_after(child, index, &mut compare)? {
                break;
            }
            self.entries.swap(index, child);
            index = child;
        }
        Ok(Some(evicted))
    }

    /// Whether entry `index` sorts after entry `other`, arrival order breaking ties.
    fn sorts_after( &self, index: usize, other: usize, compare: &mut impl FnMut(&T, &T) -> ExecutionResult<Ordering>, ) -> ExecutionResult<bool> {
        let ordering = compare(&self.entries[other].1, &self.entries[index].1)?;
        Ok(ordering == Ordering::Less || (ordering == Ordering::Equal && self.entries[index].0 > self.entries[other].0))
    }

    /// The kept entries in arrival order: a stable sort then orders equal rows as they came.
    pub(crate) fn into_arrival_order(mut self) -> Vec<T> {
        self.entries.sort_unstable_by_key(|(sequence, _)| *sequence);
        self.entries.into_iter().map(|(_, entry)| entry).collect()
    }
}

/// Bounded Top-N selector over rows, shared by the materialized and streaming executors.
#[derive(Debug)]
pub struct BoundedTopN {
    selection: TopSelection<(usize, ExecutionRow)>,
    live_bytes: usize,
}

impl BoundedTopN {
    #[must_use]
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            selection: TopSelection::new(limit),
            live_bytes: 0,
        }
    }

    #[must_use]
    pub(crate) const fn live_bytes(&self) -> usize {
        self.live_bytes
    }

    /// Pushes a candidate while deferring size estimation until the candidate is
    /// actually retained. For small Top-N limits this avoids serializing nearly
    /// every source row merely to account a handful of live candidates.
    pub(crate) fn push_lazy<F>(
        &mut self,
        runtime: &QueryRuntime,
        keys: &[SortKey],
        row: ExecutionRow,
        estimate: F,
    ) -> ExecutionResult<()>
    where
        F: FnOnce(&ExecutionRow) -> ExecutionResult<usize>,
    {
        let mut kept_bytes = 0;
        let evicted = self.selection.offer(
            row,
            |row, (_, worst)| Ok(runtime.compare_documents(keys, row.document(), worst.document())? == Ordering::Less),
            |row| {
                kept_bytes = estimate(&row)?;
                Ok((kept_bytes, row))
            },
            |(_, left), (_, right)| runtime.compare_documents(keys, left.document(), right.document()),
        )?;
        let evicted_bytes = evicted.map_or(0, |(bytes, _)| bytes);
        self.live_bytes = self.live_bytes.saturating_sub(evicted_bytes).saturating_add(kept_bytes);
        Ok(())
    }

    pub(crate) fn into_sorted_rows( self, runtime: &QueryRuntime, keys: &[SortKey], ) -> ExecutionResult<Vec<ExecutionRow>> {
        let mut rows = self.selection.into_arrival_order().into_iter().map(|(_, row)| row).collect::<Vec<_>>();
        stable_sort(runtime, keys, &mut rows)?;
        Ok(rows)
    }
}

/// One retained projected row for late materialization.
#[derive(Debug)]
pub struct ProjectedTopNWinner {
    id: DocumentId,
    version: DocumentVersion,
    values: Vec<Option<Value>>,
}

impl ProjectedTopNWinner {
    #[must_use]
    pub(crate) const fn id(&self) -> &DocumentId {
        &self.id
    }

    #[must_use]
    pub(crate) const fn version(&self) -> DocumentVersion {
        self.version
    }

}

/// Bounded ordering selector over a projected-value vector.
///
/// Unlike [`BoundedTopN`], this selector never constructs a temporary `Document`
/// for source rows. Borrowed storage values are compared directly against the
/// retained owned key vector; ownership is taken only when a row enters Top-N.
/// This makes late materialization a reusable query primitive rather than a
/// special case tied to one physical stage.
#[derive(Debug)]
pub struct BoundedProjectedTopN {
    selection: TopSelection<ProjectedTopNWinner>,
}

impl BoundedProjectedTopN {
    #[must_use]
    pub(crate) fn new(limit: usize) -> Self {
        Self { selection: TopSelection::new(limit) }
    }

    pub(crate) fn push_refs( &mut self, keys: &[SortKey], slots: &[usize], id: DocumentId, version: DocumentVersion, values: &[Option<ProjectedValueRef<'_>>], ) -> ExecutionResult<()> {
        if self.selection.limit > 0 && (slots.len() != keys.len() || slots.iter().any(|slot| *slot >= values.len())) {
            return Err(ExecutionError::evaluation(
                "projected Top-N slots do not match its sort-key layout",
            ));
        }
        self.selection.offer(
            values,
            |values, worst| Ok(compare_projected_refs_to_owned(keys, slots, values, &worst.values)? == Ordering::Less),
            |values| Ok(ProjectedTopNWinner { id, version, values: materialize_projected_slots(values, slots) }),
            |left, right| compare_owned_projected_rows(keys, &left.values, &right.values),
        )?;
        Ok(())
    }

    /// Sort-key values of the worst kept row once the selection is full: a
    /// later row that does not sort strictly before them is not kept.
    #[must_use]
    pub(crate) fn boundary(&self) -> Option<&[Option<Value>]> {
        self.selection.worst().map(|worst| worst.values.as_slice())
    }

    pub(crate) fn into_sorted_winners( self, keys: &[SortKey], ) -> ExecutionResult<Vec<ProjectedTopNWinner>> {
        let mut winners = self.selection.into_arrival_order();
        let mut failure = None;
        winners.sort_by(|left, right| {
            if failure.is_some() {
                return Ordering::Equal;
            }
            compare_owned_projected_rows(keys, &left.values, &right.values).unwrap_or_else(|error| {
                failure = Some(error);
                Ordering::Equal
            })
        });
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(winners)
    }
}

fn materialize_projected_slots( values: &[Option<ProjectedValueRef<'_>>], slots: &[usize], ) -> Vec<Option<Value>> {
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

fn compare_projected_refs_to_owned( keys: &[SortKey], slots: &[usize], left: &[Option<ProjectedValueRef<'_>>], right: &[Option<Value>], ) -> ExecutionResult<Ordering> {
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

pub(crate) fn compare_owned_projected_rows( keys: &[SortKey], left: &[Option<Value>], right: &[Option<Value>], ) -> ExecutionResult<Ordering> {
    for (index, key) in keys.iter().enumerate() {
        let ordering = compare_sort_values(
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

fn compare_projected_ref_value( left: Option<&ProjectedValueRef<'_>>, right: Option<&Value>, key: &SortKey, ) -> ExecutionResult<Ordering> {
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
        (Some(ProjectedValueRef::Owned(left)), Some(right)) => sort_order(left, right, key)?,
        // Mixed physical kinds are uncommon on the fast path. Materialize
        // only this comparison so normal numeric/string rows stay borrowed.
        (Some(left), Some(right)) => sort_order(&left.to_value(), right, key)?,
    };
    Ok(directed(ordering, key))
}

/// Orders two values of a sort key: a missing value first, then OG's
/// numeric sort order, reversed for a descending key.
pub(crate) fn compare_sort_values( left: Option<&Value>, right: Option<&Value>, key: &SortKey, ) -> ExecutionResult<Ordering> {
    let ordering = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => sort_order(left, right, key)?,
    };
    Ok(directed(ordering, key))
}

fn sort_order(left: &Value, right: &Value, key: &SortKey) -> ExecutionResult<Ordering> {
    compare_for_sort(left, right, CoercionPolicy::Numeric)
        .map(|comparison| comparison.into_ordering())
        .map_err(|error| ExecutionError::evaluation(format!("cannot sort field {}: {error}", key.field())))
}

const fn directed(ordering: Ordering, key: &SortKey) -> Ordering {
    match key.direction() {
        super::SortDirection::Ascending => ordering,
        super::SortDirection::Descending => ordering.reverse(),
    }
}

pub fn execution_row_encoded_len(row: &ExecutionRow) -> usize {
    const ROW_HEADER_BYTES: usize = 16 + 8 + 1 + 1;
    ROW_HEADER_BYTES.saturating_add(spill_document_encoded_len(row.document()))
}

pub fn execution_row_working_bytes(row: &ExecutionRow) -> ExecutionResult<usize> {
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

/// Sorts `rows` stably as [`QueryRuntime::compare_documents`] orders their
/// documents, each row's sort values looked up once.
pub fn stable_sort( _runtime: &QueryRuntime, keys: &[SortKey], rows: &mut [ExecutionRow], ) -> ExecutionResult<()> {
    let mut failure = None;
    let values = rows.iter().flat_map(|row| keys.iter().map(|key| super::runtime_materializer::path_value(row.document(), key.field()))).collect::<Vec<_>>();
    let mut order = (0..rows.len()).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        let (left, right) = (&values[left * keys.len()..][..keys.len()], &values[right * keys.len()..][..keys.len()]);
        for ((left, right), key) in left.iter().zip(right).zip(keys) {
            match failure.is_none().then(|| compare_sort_values(*left, *right, key)) {
                Some(Ok(Ordering::Equal)) | None => {}
                Some(Ok(ordering)) => return ordering,
                Some(Err(error)) => failure = Some(error),
            }
        }
        Ordering::Equal
    });
    drop(values);
    if let Some(error) = failure {
        return Err(error);
    }
    let sorted = order.into_iter().map(|row| rows[row].clone()).collect::<Vec<_>>();
    rows.clone_from_slice(&sorted);
    Ok(())
}

pub fn encode_execution_row_into( row: &ExecutionRow, output: &mut Vec<u8>, ) -> ExecutionResult<()> {
    output.clear();
    output.extend_from_slice(row.id().as_bytes());
    output.extend_from_slice(&row.version().get().to_be_bytes());
    output.push(u8::from(row.changed()));
    output.push(row.origin() as u8);
    super::codec::encode_document(output, row.document());
    Ok(())
}

pub fn decode_execution_row(bytes: &[u8]) -> ExecutionResult<ExecutionRow> {
    let mut input = super::codec::Decoder::new(bytes);
    let id = DocumentId::from_bytes(input.array()?);
    let version = DocumentVersion::new(input.u64()?);
    let changed = input.byte()? != 0;
    let origin = match input.byte()? {
        0 => ExecutionRowOrigin::Stored,
        1 => ExecutionRowOrigin::Union,
        2 => ExecutionRowOrigin::Synthetic,
        value => return Err(ExecutionError::evaluation(format!("invalid spill row origin {value}"))),
    };
    let document = Arc::new(input.document()?);
    input.finish()?;
    Ok(ExecutionRow::from_spill(id, version, document, changed, origin))
}

pub(crate) fn streaming_load_specification(plan: &PhysicalPlan) -> Option<(PhysicalLoadMode, &[Arc<str>])> {
    plan.operators().iter().find_map(|operator| match operator {
        PhysicalOperator::StreamingLoad { mode, chunks } => Some((*mode, chunks.as_ref())),
        _ => None,
    })
}

/// Returns the target collection of a terminal `into` operator.
pub(crate) fn into_target(plan: &PhysicalPlan) -> Option<&str> {
    match plan.operators().last() {
        Some(PhysicalOperator::Custom { stage: Some(Stage::Into), arguments, .. }) => Some(arguments),
        _ => None,
    }
}

pub(crate) fn insert_document(plan: &PhysicalPlan) -> Option<&InsertDocument> {
    plan.operators().iter().find_map(|operator| match operator {
        PhysicalOperator::Insert { document } => Some(document),
        _ => None,
    })
}

fn usize_to_u64(value: usize) -> ExecutionResult<u64> {
    u64::try_from(value).map_err(|_| ExecutionError::counter_overflow())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{query::PhysicalSubPipeline, storage::{ScanOptions, StorageRead, StorageTransaction}};
    use crate::{
        query::PhysicalSource,
        storage::{memory::MemoryStorage, CollectionId},
        Value,
    };

    #[test] fn execution_row_exposes_id_as_virtual_evaluation_field() { let id = DocumentId::parse("019fb7ae-9588-7057-830a-01bdb143b7ce").unwrap(); let row = ExecutionRow { id, version: DocumentVersion::INITIAL, document: Arc::new(Document::from_fields([("name", Value::from("Alice"))])), changed: false, origin: ExecutionRowOrigin::Stored, }; let evaluation = row.evaluation_document(); assert_eq!( evaluation.get("_id"), Some(&Value::from("019fb7ae-9588-7057-830a-01bdb143b7ce")), ); assert!(!row.document().contains_key("_id")); }
    #[test] fn replacing_a_row_never_persists_virtual_id_metadata() { let id = DocumentId::parse("019fb7ae-9588-7057-830a-01bdb143b7ce").unwrap(); let mut row = ExecutionRow { id, version: DocumentVersion::INITIAL, document: Arc::new(Document::default()), changed: false, origin: ExecutionRowOrigin::Stored, }; let mut replacement = row.evaluation_document(); replacement.insert("active", true); row.replace_document(Arc::new(replacement), true); assert!(!row.document().contains_key("_id")); assert_eq!(row.document().get("active"), Some(&Value::from(true))); }

    #[inline]
    fn source() -> PhysicalSource {
        PhysicalSource::collection_scan(CollectionId::parse("users").unwrap())
    }

    #[test] fn executes_empty_read_scan() { let storage = MemoryStorage::new(); let plan = PhysicalPlan::new(source(), []).unwrap(); let output = crate::engine::Engine::new(Arc::new(storage)).execute_physical(&plan) .unwrap(); assert!(output.is_empty()); assert!(!output.committed()); assert_eq!(output.statistics().scanned(), 0); assert_eq!(output.statistics().returned(), 0); assert!(output .statistics() .strategies() .contains(ExecutionStrategy::CollectionScan)); }
    #[test] fn executes_empty_write_plan_and_commits() { let storage = MemoryStorage::new(); let plan = PhysicalPlan::new(source(), [PhysicalOperator::delete()]).unwrap(); let output = crate::engine::Engine::new(Arc::new(storage)).execute_physical(&plan) .unwrap(); assert!(output.is_empty()); assert!(output.committed()); assert_eq!(output.commit(), Some(CommitResult::default())); assert_eq!(output.statistics().mutated(), 0); }
    #[test] fn empty_delete_commits_no_mutations() { let storage = MemoryStorage::new(); let plan = PhysicalPlan::new(source(), [PhysicalOperator::delete()]).unwrap(); let output = crate::engine::Engine::new(Arc::new(storage)).execute_physical(&plan) .unwrap(); assert!(output.is_empty()); assert!(output.committed()); assert_eq!(output.statistics().deleted(), 0); }
    #[test] fn empty_limit_and_skip_do_not_call_runtime() { let storage = MemoryStorage::new(); let plan = PhysicalPlan::new( source(), [PhysicalOperator::skip(10), PhysicalOperator::limit(5)], ) .unwrap(); let output = crate::engine::Engine::new(Arc::new(storage)).execute_physical(&plan) .unwrap(); assert!(output.is_empty()); assert_eq!(output.statistics().filtered(), 0); }
    #[test] fn simple_count_uses_storage_count_without_scanning() { #[derive(Debug)] struct CountOnlyStorage; #[derive(Debug)] struct CountOnlyRead; impl StorageRead for CountOnlyRead { fn get( &self, _collection: &CollectionId, _id: &DocumentId, ) -> crate::storage::StorageResult<Option<StoredDocument>> { unreachable!() } fn scan( &self, _collection: &CollectionId, _options: ScanOptions, ) -> crate::storage::StorageResult<Vec<StoredDocument>> { panic!("simple count must not scan documents") } fn count(&self, _collection: &CollectionId) -> crate::storage::StorageResult<u64> { Ok(5_000_000) } fn collection_exists( &self, _collection: &CollectionId, ) -> crate::storage::StorageResult<bool> { Ok(true) } fn collections(&self) -> crate::storage::StorageResult<Vec<CollectionId>> { Ok(vec![CollectionId::parse("users").unwrap()]) } } impl StorageEngine for CountOnlyStorage { fn read(&self) -> crate::storage::StorageResult<Box<dyn StorageRead + '_>> { Ok(Box::new(CountOnlyRead)) } fn begin(&self) -> crate::storage::StorageResult<Box<dyn StorageTransaction + '_>> { panic!("read-only count must not begin a transaction") } } let plan = PhysicalPlan::new(source(), [PhysicalOperator::count("total").unwrap()]).unwrap(); let output = crate::engine::Engine::new(Arc::new(CountOnlyStorage)).execute_physical(&plan) .unwrap(); assert_eq!(output.statistics().scanned(), 0); assert_eq!(output.statistics().returned(), 1); assert_eq!( output.rows()[0].document().get("total"), Some(&crate::Value::unsigned(5_000_000)) ); }
    #[test] fn count_on_empty_collection_emits_zero() { let storage = MemoryStorage::new(); let plan = PhysicalPlan::new(source(), [PhysicalOperator::count("total").unwrap()]).unwrap(); let output = crate::engine::Engine::new(Arc::new(storage)).execute_physical(&plan) .unwrap(); assert_eq!(output.len(), 1); assert_eq!(output.rows()[0].document().get("total"), Some(&Value::from(0_u64))); }
    #[test] fn union_on_empty_secondary_collection_is_a_noop() { let storage = MemoryStorage::new(); let nested = PhysicalSubPipeline::empty(); let union = PhysicalOperator::union( CollectionId::parse("archived_users").unwrap(), None::<&str>, nested, ) .unwrap(); let plan = PhysicalPlan::new(source(), [union]).unwrap(); let output = crate::engine::Engine::new(Arc::new(storage)).execute_physical(&plan) .unwrap(); assert!(output.is_empty()); assert!(!output.committed()); }
    #[test] fn lookup_on_empty_input_does_not_call_runtime() { let storage = MemoryStorage::new(); let lookup = PhysicalOperator::lookup( CollectionId::parse("workspace").unwrap(), Some("w"), "public", PhysicalSubPipeline::empty(), ) .unwrap(); let plan = PhysicalPlan::new(source(), [lookup]).unwrap(); let output = crate::engine::Engine::new(Arc::new(storage)).execute_physical(&plan) .unwrap(); assert!(output.is_empty()); }
    #[test] fn typed_insert_commits_one_document() { let storage = MemoryStorage::new(); let document = InsertDocument::parse(r#"{name:"Alice"}"#).unwrap(); let plan = PhysicalPlan::new(source(), [PhysicalOperator::insert(document)]).unwrap(); let output = crate::engine::Engine::new(Arc::new(storage)).execute_physical(&plan) .unwrap(); assert!(output.committed()); assert_eq!(output.len(), 1); assert_eq!(output.rows()[0].document().get("name"), Some(&Value::from("Alice"))); }
    #[test] fn scoped_streaming_load_stamps_the_scope_and_never_replaces() { let engine = crate::engine::Engine::new(Arc::new(MemoryStorage::new())); let plan = |n: i64| PhysicalPlan::new(source(), [PhysicalOperator::streaming_load(PhysicalLoadMode::Merge, [format!(r#"[{{"_id":"019fb7ae-9588-7057-830a-01bdb143b7ce","n":{n}}}]"#)]).unwrap()]).unwrap(); let scope = DocumentScope::new("p", "i"); let output = engine.execute_physical_scoped(&plan(1), &scope).unwrap(); assert_eq!(crate::helpers::document_to_json(output.rows()[0].document())["_place"], "p"); assert!(engine.execute_physical_scoped(&plan(2), &scope).unwrap_err().to_string().contains("cannot replace a stored document")); }
    #[test] fn streaming_load_of_empty_chunks_commits_no_mutation() { let storage = MemoryStorage::new(); let operator = PhysicalOperator::streaming_load(PhysicalLoadMode::Merge, ["[]", "[]"]) .unwrap(); let plan = PhysicalPlan::new(source(), [operator]).unwrap(); let output = crate::engine::Engine::new(Arc::new(storage)).execute_physical(&plan) .unwrap(); assert!(output.committed()); assert_eq!(output.statistics().mutated(), 0); assert_eq!(output.statistics().scanned(), 0); }
    #[test] fn pivot_of_empty_collection_is_empty() { use crate::query::plan_values::{PivotAggregate, PivotSpecification, PivotValue}; let specification = PivotSpecification::new( [ExpressionFieldPath::new(["region"]).unwrap()], [ExpressionFieldPath::new(["month"]).unwrap()], [PivotValue::new( ExpressionFieldPath::new(["revenue"]).unwrap(), PivotAggregate::Sum, None::<&str>, ) .unwrap()], ) .unwrap(); let storage = MemoryStorage::new(); let plan = PhysicalPlan::new(source(), [PhysicalOperator::pivot(specification)]).unwrap(); let output = crate::engine::Engine::new(Arc::new(storage)).execute_physical(&plan) .unwrap(); assert!(output.is_empty()); }
    #[test] fn document_scope_overwrites_untrusted_scope_fields() { let scope = DocumentScope::new("place-a", "app-1"); let document = Document::from_fields([ ("_place", Value::from("place-evil")), ("_app_instance", Value::from("app-evil")), ("name", Value::from("item")), ]); let scoped = scope.enforce("c", &document).unwrap(); assert_eq!(scoped.get("_place"), Some(&Value::from("place-a"))); assert_eq!(scoped.get("_app_instance"), Some(&Value::from("app-1"))); assert_eq!(scoped.get("name"), Some(&Value::from("item"))); }
    #[test] fn place_document_scope_forces_place_without_overwriting_app_instance() { let scope = DocumentScope::for_place("place-a"); let document = Document::from_fields([ ("_place", Value::from("place-evil")), ("_app_instance", Value::from("app-2")), ("name", Value::from("item")), ]); let scoped = scope.enforce("c", &document).unwrap(); assert_eq!(scoped.get("_place"), Some(&Value::from("place-a"))); assert_eq!(scoped.get("_app_instance"), Some(&Value::from("app-2"))); assert_eq!(scoped.get("name"), Some(&Value::from("item"))); }
    #[test] fn execution_public_types_are_send_and_sync() { fn assert_send_and_sync<T: Send + Sync>() {} assert_send_and_sync::<DocumentScope>(); assert_send_and_sync::<ExecutionOutput>(); assert_send_and_sync::<ExecutionRow>(); assert_send_and_sync::<ExecutionRowOrigin>(); assert_send_and_sync::<ExecutionError>(); assert_send_and_sync::<ExecutionStatistics>(); assert_send_and_sync::<PreparedInsertDocument>(); assert_send_and_sync::<SyntheticDocument>(); assert_send_and_sync::<StreamingLoadMutation>(); }
    #[test] fn top_selection_keeps_the_stable_sorted_prefix() { let mut seed = 0x9e37_79b9_u64; let random: Vec<u32> = (0..500).map(|_| { seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1); (seed >> 40) as u32 % 13 }).collect(); for keys in [random, (0..300).collect(), (0..300).rev().collect()] { for limit in [0_usize, 1, 3, 7, 50, 200, 1000] { let mut selection = TopSelection::new(limit); for (position, key) in keys.iter().enumerate() { selection.offer((*key, position), |row, worst| Ok(row.0 < worst.0), Ok, |left, right| Ok(left.0.cmp(&right.0))).unwrap(); } let mut expected: Vec<(u32, usize)> = keys.iter().copied().zip(0..).collect(); expected.sort_by_key(|row| row.0); expected.truncate(limit); assert_eq!(selection.worst().copied(), (limit > 0 && limit <= keys.len()).then(|| *expected.last().unwrap())); let mut kept = selection.into_arrival_order(); kept.sort_by_key(|row| row.0); assert_eq!(kept, expected, "limit {limit}"); } } }
    #[test] fn reservoir_samples_every_position_uniformly() { let (capacity, length, rounds) = (4_usize, 40_usize, 20_000_usize); let mut hits = vec![0_u32; length]; for _ in 0..rounds { let mut reservoir = Reservoir::new(capacity); for value in 0..length { reservoir.push(value); } assert_eq!(reservoir.seen(), length as u64); for value in reservoir.into_items() { hits[value] += 1; } } let expected = (rounds * capacity / length) as f64; for (position, hit) in hits.iter().enumerate() { assert!((f64::from(*hit) - expected).abs() < expected * 0.1, "position {position}: {hit} hits, expected ~{expected}"); } }
    #[test] fn reservoir_keeps_a_bounded_sample_of_distinct_inputs() { let mut reservoir = Reservoir::new(5); for value in 0..1000 { reservoir.push(value); } assert_eq!(reservoir.seen(), 1000); let mut items = reservoir.into_items(); assert_eq!(items.len(), 5); items.sort_unstable(); items.dedup(); assert_eq!(items.len(), 5); let mut small = Reservoir::new(5); small.push(1); assert_eq!(small.into_items(), [1]); assert!(Reservoir::<u8>::new(0).into_items().is_empty()); }
}
