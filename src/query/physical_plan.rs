//! Physical query plan representation.

use std::{error::Error as StdError, fmt, sync::Arc};

use crate::storage::{CollectionId, DocumentId, ScanDirection, ScanOptions};

use super::execution_properties::{
    Bound, CardinalityEffect, Effect, ExecutionProperties, Fields, Flow, Materialization, Order,
    ProjectedAccess, ProjectionReuse, Shape,
};
use super::{AccessVector, Expression, ExpressionFieldPath, SetAssignment, SortKey, StageName};

use super::logical_plan::{InsertDocument, PivotSpecification};

/// Result returned by physical planning operations.
pub type PhysicalPlanResult<T> = std::result::Result<T, PhysicalPlanError>;

/// Executable physical query plan.
#[derive(Clone, Debug)]
pub struct PhysicalPlan {
    source: PhysicalSource,
    operators: Arc<[PhysicalOperator]>,
    mode: ExecutionMode,
    memory_mode: MemoryExecutionMode,
    changes_cardinality: bool,
    source_access_vector: AccessVector,
    projected_prefix_len: usize,
    source_projection_reuse: ProjectionReuse,
}

impl PhysicalPlan {
    /// Starts building a physical plan.
    #[must_use]
    pub fn builder(source: PhysicalSource) -> PhysicalPlanBuilder {
        PhysicalPlanBuilder::new(source)
    }

    /// Creates and validates a physical plan.
    ///
    /// # Errors
    ///
    /// Returns a [`PhysicalPlanError`] when the operator sequence is invalid.
    pub fn new<I>(source: PhysicalSource, operators: I) -> PhysicalPlanResult<Self>
    where
        I: IntoIterator<Item = PhysicalOperator>,
    {
        let operators = operators.into_iter().collect::<Vec<_>>();
        validate_plan(&source, &operators)?;

        let (source_access_vector, projected_prefix_len) =
            negotiate_source_access_vector(&operators);
        let source_projection_reuse = negotiate_source_projection_reuse(&operators);
        let (mode, memory_mode, changes_cardinality) = summarize_execution(&operators);
        Ok(Self {
            source,
            mode,
            memory_mode,
            changes_cardinality,
            operators: Arc::from(operators),
            source_access_vector,
            projected_prefix_len,
            source_projection_reuse,
        })
    }

    /// Returns the physical source access.
    #[must_use]
    #[inline]
    pub const fn source(&self) -> &PhysicalSource {
        &self.source
    }

    /// Returns all executable operators.
    #[must_use]
    #[inline]
    pub fn operators(&self) -> &[PhysicalOperator] {
        &self.operators
    }

    /// Returns the source row representation selected during physical planning.
    #[must_use]
    pub const fn source_access_vector(&self) -> AccessVector {
        self.source_access_vector
    }

    /// Number of source-facing operators negotiated for the projected-value vector.
    #[must_use]
    pub const fn projected_prefix_len(&self) -> usize {
        self.projected_prefix_len
    }

    /// Whether the negotiated projected source representation is immutable and
    /// reusable across executions. Storage remains free to decline caching.
    #[must_use]
    pub const fn source_projection_reuse(&self) -> ProjectionReuse {
        self.source_projection_reuse
    }

    /// Carries a source-vector negotiation from a parent physical plan into a
    /// source-facing subplan built by the executor.
    pub(crate) const fn with_source_access_negotiation(
        mut self,
        vector: AccessVector,
        projected_prefix_len: usize,
        projection_reuse: ProjectionReuse,
    ) -> Self {
        self.source_access_vector = vector;
        self.projected_prefix_len = projected_prefix_len;
        self.source_projection_reuse = projection_reuse;
        self
    }

    /// Returns the execution mode derived from the operators.
    #[must_use]
    pub const fn mode(&self) -> ExecutionMode {
        self.mode
    }

    /// Returns whether the plan performs mutations.
    #[must_use]
    pub const fn is_write(&self) -> bool {
        matches!(self.mode, ExecutionMode::ReadWrite)
    }

    /// Returns whether the plan contains the streaming import operator.
    #[must_use]
    pub fn is_streaming_load(&self) -> bool {
        self.operators
            .iter()
            .any(|operator| matches!(operator, PhysicalOperator::StreamingLoad { .. }))
    }

    /// Returns whether no operator follows the source scan.
    #[must_use]
    pub fn is_scan_only(&self) -> bool {
        self.operators.is_empty()
    }

    /// Returns the number of operators after the source.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.operators.len()
    }

    /// Returns whether the plan contains no operator after the source.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    /// Returns whether at least one operator can change output cardinality.
    #[must_use]
    pub const fn changes_cardinality(&self) -> bool {
        self.changes_cardinality
    }

    /// Returns the minimum storage capability needed by the executor.
    #[must_use]
    pub const fn required_storage_access(&self) -> StorageAccessMode {
        match self.mode {
            ExecutionMode::ReadOnly => StorageAccessMode::Snapshot,
            ExecutionMode::ReadWrite => StorageAccessMode::Transaction,
        }
    }

    /// Returns the memory execution contract for this plan.
    #[must_use]
    pub const fn memory_execution_mode(&self) -> MemoryExecutionMode {
        self.memory_mode
    }

    /// Returns whether this plan is guaranteed to stay on the streaming path.
    #[must_use]
    pub const fn is_memory_streaming(&self) -> bool {
        matches!(self.memory_mode, MemoryExecutionMode::Streaming)
    }
}

/// Memory execution contract selected for a physical plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MemoryExecutionMode {
    /// Every operator can consume and emit rows incrementally.
    Streaming,
    /// At least one operator must buffer state and therefore requires a governed budget.
    GovernedBlocking,
    /// The plan mutates storage and follows the transactional executor.
    Transactional,
}

impl MemoryExecutionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Streaming => "streaming",
            Self::GovernedBlocking => "governed_blocking",
            Self::Transactional => "transactional",
        }
    }
}

/// Physical source and storage access strategy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalSource {
    collection: CollectionId,
    access: PhysicalAccess,
}

impl PhysicalSource {
    /// Creates a forward full-collection scan.
    #[must_use]
    pub fn collection_scan(collection: CollectionId) -> Self {
        Self {
            collection,
            access: PhysicalAccess::CollectionScan {
                options: ScanOptions::default(),
            },
        }
    }

    /// Creates a direct primary-key lookup.
    #[must_use]
    pub const fn primary_key_lookup(collection: CollectionId, id: DocumentId) -> Self {
        Self {
            collection,
            access: PhysicalAccess::PrimaryKeyLookup { id },
        }
    }

    /// Creates a collection scan with explicit options.
    #[must_use]
    pub const fn scan(collection: CollectionId, options: ScanOptions) -> Self {
        Self {
            collection,
            access: PhysicalAccess::CollectionScan { options },
        }
    }

    /// Returns the source collection.
    #[must_use]
    pub const fn collection(&self) -> &CollectionId {
        &self.collection
    }

    /// Returns the selected access strategy.
    #[must_use]
    pub const fn access(&self) -> &PhysicalAccess {
        &self.access
    }

    /// Replaces the access strategy.
    #[must_use]
    pub const fn with_access(mut self, access: PhysicalAccess) -> Self {
        self.access = access;
        self
    }
}

/// Storage access selected for a physical source.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PhysicalAccess {
    /// Deterministic collection scan.
    CollectionScan { options: ScanOptions },

    /// Direct lookup through the collection master `_id` index.
    PrimaryKeyLookup { id: DocumentId },
}

impl PhysicalAccess {
    /// Returns scan options when this access strategy is a collection scan.
    #[must_use]
    pub const fn scan_options(&self) -> Option<ScanOptions> {
        match self {
            Self::CollectionScan { options } => Some(*options),
            Self::PrimaryKeyLookup { .. } => None,
        }
    }
}

/// Stable physical operator category.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PhysicalOperatorKind {
    Filter,
    Set,
    Lookup,
    Union,
    Load,
    StreamingLoad,
    Limit,
    Skip,
    Sort,
    Select,
    Distinct,
    Count,
    Delete,
    Insert,
    Group,
    Pivot,
    Custom,
}

impl fmt::Display for PhysicalOperatorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Filter => "filter",
            Self::Set => "set",
            Self::Lookup => "lookup",
            Self::Union => "union",
            Self::Load => "load",
            Self::StreamingLoad => "streaming-load",
            Self::Limit => "limit",
            Self::Skip => "skip",
            Self::Sort => "sort",
            Self::Select => "select",
            Self::Distinct => "distinct",
            Self::Count => "count",
            Self::Delete => "delete",
            Self::Insert => "insert",
            Self::Group => "group",
            Self::Pivot => "pivot",
            Self::Custom => "custom",
        })
    }
}

/// Physical load conflict mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PhysicalLoadMode {
    Replace,
    Update,
    Merge,
}

impl PhysicalLoadMode {
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Replace => "replace",
            Self::Update => "update",
            Self::Merge => "merge",
        }
    }
}

impl fmt::Display for PhysicalLoadMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Validated read-only operator sequence used by lookup and union.
#[derive(Clone, Debug)]
pub struct PhysicalSubPipeline {
    operators: Arc<[PhysicalOperator]>,
}

impl PhysicalSubPipeline {
    /// Creates and validates a nested read-only pipeline.
    pub fn new<I>(operators: I) -> PhysicalPlanResult<Self>
    where
        I: IntoIterator<Item = PhysicalOperator>,
    {
        let operators = operators.into_iter().collect::<Vec<_>>();
        validate_subpipeline(&operators)?;

        Ok(Self {
            operators: Arc::from(operators),
        })
    }

    /// Creates an empty nested pipeline.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            operators: Arc::from([]),
        }
    }

    #[must_use]
    #[inline]
    pub fn operators(&self) -> &[PhysicalOperator] {
        &self.operators
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.operators.len()
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    #[must_use]
    pub fn changes_cardinality(&self) -> bool {
        self.operators.iter().any(|operator| {
            !matches!(
                operator.execution_properties().cardinality,
                CardinalityEffect::Preserve
            )
        })
    }
}

/// One executable pipeline operator.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum PhysicalOperator {
    /// Retains documents whose predicate evaluates to true.
    Filter { predicate: Expression },

    /// Evaluates assignments and replaces each retained document.
    Set { assignments: Arc<[SetAssignment]> },

    /// Executes a read-only pipeline against another collection and stores the
    /// resulting value in `into`.
    Lookup {
        collection: CollectionId,
        alias: Option<Arc<str>>,
        into: Arc<str>,
        pipeline: PhysicalSubPipeline,
    },

    /// Executes a read-only pipeline against another collection and appends its
    /// resulting rows to the current stream.
    Union {
        collection: CollectionId,
        alias: Option<Arc<str>>,
        pipeline: PhysicalSubPipeline,
    },

    /// Loads or hydrates a named resource into each retained document.
    Load { target: Arc<str> },

    /// Applies ordered chunks using one explicit conflict mode.
    StreamingLoad {
        mode: PhysicalLoadMode,
        chunks: Arc<[Arc<str>]>,
    },

    /// Retains at most the first `count` rows.
    Limit { count: usize },

    /// Discards the first `count` rows.
    Skip { count: usize },

    /// Performs a stable multi-key sort.
    Sort { keys: Arc<[SortKey]> },

    /// Projects each row to the selected fields.
    Select { fields: Arc<[ExpressionFieldPath]> },

    /// Removes duplicate rows.
    ///
    /// An empty field list means full-document distinctness.
    Distinct { fields: Arc<[ExpressionFieldPath]> },

    /// Replaces the input stream with one count result document.
    Count { alias: Arc<str> },

    /// Deletes every document reaching this operator.
    Delete,

    /// Inserts one fully typed and validated document.
    Insert { document: InsertDocument },

    /// Groups rows by the supplied field keys.
    Group { keys: Arc<[ExpressionFieldPath]> },

    /// Reshapes rows according to one validated pivot specification.
    Pivot { specification: PivotSpecification },

    /// Extension operator preserved for a registered external executor.
    Custom {
        name: StageName,
        arguments: Arc<str>,
        writes: bool,
        changes_cardinality: bool,
    },
}

impl PhysicalOperator {
    /// Creates a filter operator.
    #[must_use]
    pub fn filter(predicate: Expression) -> Self {
        Self::Filter { predicate }
    }

    /// Creates a validated set operator.
    pub fn set<I>(assignments: I) -> PhysicalPlanResult<Self>
    where
        I: IntoIterator<Item = SetAssignment>,
    {
        let assignments = assignments.into_iter().collect::<Vec<_>>();
        validate_assignments(&assignments)?;

        Ok(Self::Set {
            assignments: Arc::from(assignments),
        })
    }

    /// Creates a validated lookup operator.
    pub fn lookup<A, T>(
        collection: CollectionId,
        alias: Option<A>,
        into: T,
        pipeline: PhysicalSubPipeline,
    ) -> PhysicalPlanResult<Self>
    where
        A: AsRef<str>,
        T: AsRef<str>,
    {
        let alias = normalize_optional_alias(alias)?;

        let into = validate_named_field(into.as_ref(), |target| {
            PhysicalPlanErrorKind::InvalidLookupTarget { target }
        })?;

        Ok(Self::Lookup {
            collection,
            alias,
            into: Arc::from(into),
            pipeline,
        })
    }

    /// Creates a validated union operator.
    pub fn union<A>(
        collection: CollectionId,
        alias: Option<A>,
        pipeline: PhysicalSubPipeline,
    ) -> PhysicalPlanResult<Self>
    where
        A: AsRef<str>,
    {
        let alias = normalize_optional_alias(alias)?;

        Ok(Self::Union {
            collection,
            alias,
            pipeline,
        })
    }

    /// Creates a validated streaming-load operator.
    pub fn streaming_load<I, S>(mode: PhysicalLoadMode, chunks: I) -> PhysicalPlanResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let chunks = chunks
            .into_iter()
            .map(|chunk| {
                non_empty_text(chunk.as_ref(), PhysicalPlanError::empty_load_chunk)
                    .map(Arc::<str>::from)
            })
            .collect::<PhysicalPlanResult<Vec<_>>>()?;

        if chunks.is_empty() {
            return Err(PhysicalPlanError::new(
                PhysicalPlanErrorKind::EmptyStreamingLoad,
            ));
        }

        Ok(Self::StreamingLoad {
            mode,
            chunks: Arc::from(chunks),
        })
    }

    /// Creates a validated load operator.
    pub fn load(target: impl AsRef<str>) -> PhysicalPlanResult<Self> {
        let target = non_empty_text(target.as_ref(), PhysicalPlanError::empty_load_target)?;

        Ok(Self::Load {
            target: Arc::from(target),
        })
    }

    /// Creates a limit operator.
    #[must_use]
    pub const fn limit(count: usize) -> Self {
        Self::Limit { count }
    }

    /// Creates a skip operator.
    #[must_use]
    pub const fn skip(count: usize) -> Self {
        Self::Skip { count }
    }

    /// Creates a validated sort operator.
    pub fn sort<I>(keys: I) -> PhysicalPlanResult<Self>
    where
        I: IntoIterator<Item = SortKey>,
    {
        let keys = keys.into_iter().collect::<Vec<_>>();
        validate_sort_keys(&keys)?;

        Ok(Self::Sort {
            keys: Arc::from(keys),
        })
    }

    /// Creates a validated projection operator.
    pub fn select<I>(fields: I) -> PhysicalPlanResult<Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        let fields = fields.into_iter().collect::<Vec<_>>();
        validate_fields(&fields, PhysicalFieldContext::Select, false)?;

        Ok(Self::Select {
            fields: Arc::from(fields),
        })
    }

    /// Creates a validated distinct operator.
    pub fn distinct<I>(fields: I) -> PhysicalPlanResult<Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        let fields = fields.into_iter().collect::<Vec<_>>();
        validate_fields(&fields, PhysicalFieldContext::Distinct, true)?;

        Ok(Self::Distinct {
            fields: Arc::from(fields),
        })
    }

    /// Creates a validated count operator.
    pub fn count(alias: impl AsRef<str>) -> PhysicalPlanResult<Self> {
        let alias = alias.as_ref().trim();
        let alias = if alias.is_empty() { "count" } else { alias };

        validate_identifier(alias)?;

        Ok(Self::Count {
            alias: Arc::from(alias),
        })
    }

    /// Creates a delete operator.
    #[must_use]
    pub const fn delete() -> Self {
        Self::Delete
    }

    /// Creates an insert operator from a document already validated by the
    /// semantic planner.
    #[must_use]
    pub const fn insert(document: InsertDocument) -> Self {
        Self::Insert { document }
    }

    /// Creates a validated group operator.
    pub fn group<I>(keys: I) -> PhysicalPlanResult<Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        let keys = keys.into_iter().collect::<Vec<_>>();
        validate_fields(&keys, PhysicalFieldContext::Group, false)?;

        Ok(Self::Group {
            keys: Arc::from(keys),
        })
    }

    /// Creates a pivot operator from a specification already validated by the
    /// semantic planner.
    #[must_use]
    pub const fn pivot(specification: PivotSpecification) -> Self {
        Self::Pivot { specification }
    }

    /// Creates an extension operator.
    pub fn custom(
        name: StageName,
        arguments: impl AsRef<str>,
        writes: bool,
        changes_cardinality: bool,
    ) -> PhysicalPlanResult<Self> {
        let arguments = arguments.as_ref();

        if arguments.chars().any(char::is_control) {
            return Err(PhysicalPlanError::new(
                PhysicalPlanErrorKind::InvalidCustomArguments {
                    stage: Arc::from(name.as_str()),
                },
            ));
        }

        Ok(Self::Custom {
            name,
            arguments: Arc::from(arguments),
            writes,
            changes_cardinality,
        })
    }

    /// Returns the resolved execution properties for this operator instance.
    #[must_use]
    pub fn execution_properties(&self) -> ExecutionProperties<'_> {
        use super::execution_properties::Scope::{Row, Set};
        use Bound::{AtMost, Exact, Unknown as U};
        use CardinalityEffect::{Expand, Preserve, Reduce, Unknown};
        use Effect::{ReadOnly as R, Write as W};
        use Fields::{Preserved as SameFields, Projected, Unknown as UnknownFields};
        use Flow::{GovernedBlocking as B, Specialized as X, Streaming as S};
        use Materialization::{Deferred as Defer, Required as Materialize};
        use Order::{Ordered, Preserved, Unknown as NoOrder};
        use ProjectedAccess::{Consumer as PvConsumer, None as NoPv, Stage as PvStage};
        use ProjectionReuse::{None as NoReuse, Reusable as Reuse};
        use Shape::{Linear as L, Matrix as M, Scalar as C};

        let (
            flow,
            cardinality,
            bound,
            order,
            fields,
            shape,
            scope,
            effect,
            projected_access,
            materialization,
            projection_reuse,
        ) = match self {
            Self::Filter { .. } => (
                S, Reduce, U, Preserved, SameFields, L, Row, R, PvStage, Defer, Reuse,
            ),
            Self::Set { .. } | Self::StreamingLoad { .. } => (
                X,
                Preserve,
                U,
                Preserved,
                UnknownFields,
                L,
                Row,
                W,
                NoPv,
                Materialize,
                NoReuse,
            ),
            Self::Lookup { .. } => (
                B,
                Preserve,
                U,
                Preserved,
                UnknownFields,
                L,
                Row,
                R,
                NoPv,
                Materialize,
                NoReuse,
            ),
            Self::Load { .. } => (
                B,
                Preserve,
                U,
                Preserved,
                UnknownFields,
                L,
                Row,
                W,
                NoPv,
                Materialize,
                NoReuse,
            ),
            Self::Union { .. } => (
                B,
                Expand,
                U,
                NoOrder,
                UnknownFields,
                L,
                Set,
                R,
                NoPv,
                Materialize,
                NoReuse,
            ),
            Self::Limit { count } => (
                S,
                Reduce,
                AtMost(*count),
                Preserved,
                SameFields,
                L,
                Set,
                R,
                NoPv,
                Defer,
                Reuse,
            ),
            Self::Skip { .. } => (
                S, Reduce, U, Preserved, SameFields, L, Set, R, NoPv, Defer, Reuse,
            ),
            Self::Distinct { fields } if !fields.is_empty() => (
                B,
                Reduce,
                U,
                NoOrder,
                UnknownFields,
                L,
                Set,
                R,
                PvConsumer,
                Defer,
                Reuse,
            ),
            Self::Distinct { .. } => (
                B,
                Reduce,
                U,
                NoOrder,
                UnknownFields,
                L,
                Set,
                R,
                NoPv,
                Materialize,
                NoReuse,
            ),
            Self::Group { .. } => (
                B,
                Reduce,
                U,
                NoOrder,
                UnknownFields,
                L,
                Set,
                R,
                PvConsumer,
                Defer,
                Reuse,
            ),
            Self::Sort { keys } => (
                B,
                Preserve,
                U,
                Ordered(keys),
                SameFields,
                L,
                Set,
                R,
                PvConsumer,
                Defer,
                Reuse,
            ),
            Self::Select { fields } => (
                S,
                Preserve,
                U,
                Preserved,
                Projected(fields),
                L,
                Row,
                R,
                PvStage,
                Defer,
                Reuse,
            ),
            Self::Count { .. } => (
                S,
                Unknown,
                Exact(1),
                NoOrder,
                UnknownFields,
                C,
                Set,
                R,
                PvConsumer,
                Defer,
                Reuse,
            ),
            Self::Delete => (
                X,
                Reduce,
                U,
                NoOrder,
                UnknownFields,
                L,
                Row,
                W,
                NoPv,
                Materialize,
                NoReuse,
            ),
            Self::Insert { .. } => (
                X,
                Expand,
                Exact(1),
                NoOrder,
                UnknownFields,
                L,
                Row,
                W,
                NoPv,
                Materialize,
                NoReuse,
            ),
            Self::Pivot { .. } => (
                B,
                Unknown,
                U,
                NoOrder,
                UnknownFields,
                M,
                Set,
                R,
                NoPv,
                Materialize,
                NoReuse,
            ),
            Self::Custom {
                writes,
                changes_cardinality,
                ..
            } => (
                if *writes {
                    X
                } else if *changes_cardinality {
                    B
                } else {
                    S
                },
                if *changes_cardinality {
                    Unknown
                } else {
                    Preserve
                },
                U,
                NoOrder,
                UnknownFields,
                L,
                if *changes_cardinality { Set } else { Row },
                if *writes { W } else { R },
                NoPv,
                Materialize,
                NoReuse,
            ),
        };
        ExecutionProperties {
            flow,
            cardinality,
            bound,
            order,
            fields,
            shape,
            scope,
            effect,
            projected_access,
            materialization,
            projection_reuse,
        }
    }

    /// Returns this operator's stable category.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> PhysicalOperatorKind {
        match self {
            Self::Filter { .. } => PhysicalOperatorKind::Filter,
            Self::Set { .. } => PhysicalOperatorKind::Set,
            Self::Lookup { .. } => PhysicalOperatorKind::Lookup,
            Self::Union { .. } => PhysicalOperatorKind::Union,
            Self::Load { .. } => PhysicalOperatorKind::Load,
            Self::StreamingLoad { .. } => PhysicalOperatorKind::StreamingLoad,
            Self::Limit { .. } => PhysicalOperatorKind::Limit,
            Self::Skip { .. } => PhysicalOperatorKind::Skip,
            Self::Sort { .. } => PhysicalOperatorKind::Sort,
            Self::Select { .. } => PhysicalOperatorKind::Select,
            Self::Distinct { .. } => PhysicalOperatorKind::Distinct,
            Self::Count { .. } => PhysicalOperatorKind::Count,
            Self::Delete => PhysicalOperatorKind::Delete,
            Self::Insert { .. } => PhysicalOperatorKind::Insert,
            Self::Group { .. } => PhysicalOperatorKind::Group,
            Self::Pivot { .. } => PhysicalOperatorKind::Pivot,
            Self::Custom { .. } => PhysicalOperatorKind::Custom,
        }
    }

    /// Returns a stable operator name for diagnostics and instrumentation.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        match self {
            Self::Filter { .. } => "filter",
            Self::Set { .. } => "set",
            Self::Lookup { .. } => "lookup",
            Self::Union { .. } => "union",
            Self::Load { .. } => "load",
            Self::StreamingLoad { .. } => "streaming-load",
            Self::Limit { .. } => "limit",
            Self::Skip { .. } => "skip",
            Self::Sort { .. } => "sort",
            Self::Select { .. } => "select",
            Self::Distinct { .. } => "distinct",
            Self::Count { .. } => "count",
            Self::Delete => "delete",
            Self::Insert { .. } => "insert",
            Self::Group { .. } => "group",
            Self::Pivot { .. } => "pivot",
            Self::Custom { name, .. } => name.as_str(),
        }
    }

    #[must_use]
    pub const fn predicate(&self) -> Option<&Expression> {
        match self {
            Self::Filter { predicate } => Some(predicate),
            _ => None,
        }
    }

    #[must_use]
    pub fn assignments(&self) -> Option<&[SetAssignment]> {
        match self {
            Self::Set { assignments } => Some(assignments),
            _ => None,
        }
    }

    #[must_use]
    pub const fn lookup_collection(&self) -> Option<&CollectionId> {
        match self {
            Self::Lookup { collection, .. } => Some(collection),
            _ => None,
        }
    }

    #[must_use]
    pub fn lookup_alias(&self) -> Option<&str> {
        match self {
            Self::Lookup { alias, .. } => alias.as_deref(),
            _ => None,
        }
    }

    #[must_use]
    pub fn lookup_target(&self) -> Option<&str> {
        match self {
            Self::Lookup { into, .. } => Some(into),
            _ => None,
        }
    }

    #[must_use]
    pub const fn union_collection(&self) -> Option<&CollectionId> {
        match self {
            Self::Union { collection, .. } => Some(collection),
            _ => None,
        }
    }

    #[must_use]
    pub fn union_alias(&self) -> Option<&str> {
        match self {
            Self::Union { alias, .. } => alias.as_deref(),
            _ => None,
        }
    }

    #[must_use]
    pub const fn nested_pipeline(&self) -> Option<&PhysicalSubPipeline> {
        match self {
            Self::Lookup { pipeline, .. } | Self::Union { pipeline, .. } => Some(pipeline),
            _ => None,
        }
    }

    #[must_use]
    pub const fn streaming_load_mode(&self) -> Option<PhysicalLoadMode> {
        match self {
            Self::StreamingLoad { mode, .. } => Some(*mode),
            _ => None,
        }
    }

    #[must_use]
    pub fn streaming_load_chunks(&self) -> Option<&[Arc<str>]> {
        match self {
            Self::StreamingLoad { chunks, .. } => Some(chunks),
            _ => None,
        }
    }

    #[must_use]
    pub fn load_target(&self) -> Option<&str> {
        match self {
            Self::Load { target } => Some(target),
            _ => None,
        }
    }

    #[must_use]
    pub const fn row_count(&self) -> Option<usize> {
        match self {
            Self::Limit { count } | Self::Skip { count } => Some(*count),
            _ => None,
        }
    }

    #[must_use]
    pub fn sort_keys(&self) -> Option<&[SortKey]> {
        match self {
            Self::Sort { keys } => Some(keys),
            _ => None,
        }
    }

    #[must_use]
    pub fn selected_fields(&self) -> Option<&[ExpressionFieldPath]> {
        match self {
            Self::Select { fields } => Some(fields),
            _ => None,
        }
    }

    #[must_use]
    pub fn distinct_fields(&self) -> Option<&[ExpressionFieldPath]> {
        match self {
            Self::Distinct { fields } => Some(fields),
            _ => None,
        }
    }

    #[must_use]
    pub fn count_alias(&self) -> Option<&str> {
        match self {
            Self::Count { alias } => Some(alias),
            _ => None,
        }
    }

    #[must_use]
    pub const fn insert_document(&self) -> Option<&InsertDocument> {
        match self {
            Self::Insert { document } => Some(document),
            _ => None,
        }
    }

    #[must_use]
    pub fn group_keys(&self) -> Option<&[ExpressionFieldPath]> {
        match self {
            Self::Group { keys } => Some(keys),
            _ => None,
        }
    }

    #[must_use]
    pub const fn pivot_specification(&self) -> Option<&PivotSpecification> {
        match self {
            Self::Pivot { specification } => Some(specification),
            _ => None,
        }
    }
}

/// Whether execution may mutate storage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ExecutionMode {
    /// A storage snapshot is sufficient.
    #[default]
    ReadOnly,

    /// Execution requires a storage transaction.
    ReadWrite,
}

/// Minimum storage handle required for a plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StorageAccessMode {
    Snapshot,
    Transaction,
}

/// Builder for validated physical plans.
#[derive(Clone, Debug)]
pub struct PhysicalPlanBuilder {
    source: PhysicalSource,
    operators: Vec<PhysicalOperator>,
}

impl PhysicalPlanBuilder {
    /// Replaces the source access selected by the planner.
    pub fn set_source_access(&mut self, access: PhysicalAccess) {
        self.source = self.source.clone().with_access(access);
    }

    /// Creates an empty builder for a source.
    #[must_use]
    #[inline]
    pub const fn new(source: PhysicalSource) -> Self {
        Self {
            source,
            operators: Vec::new(),
        }
    }

    #[must_use]
    #[inline]
    pub const fn source(&self) -> &PhysicalSource {
        &self.source
    }

    #[must_use]
    #[inline]
    pub fn operators(&self) -> &[PhysicalOperator] {
        &self.operators
    }

    pub fn push(&mut self, operator: PhysicalOperator) -> PhysicalPlanResult<&mut Self> {
        validate_next_operator(&self.operators, &operator)?;
        self.operators.push(operator);
        Ok(self)
    }

    pub fn filter(&mut self, predicate: Expression) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::filter(predicate))
    }

    pub fn set<I>(&mut self, assignments: I) -> PhysicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = SetAssignment>,
    {
        self.push(PhysicalOperator::set(assignments)?)
    }

    pub fn lookup(
        &mut self,
        collection: CollectionId,
        alias: Option<impl AsRef<str>>,
        into: impl AsRef<str>,
        pipeline: PhysicalSubPipeline,
    ) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::lookup(collection, alias, into, pipeline)?)
    }

    pub fn union(
        &mut self,
        collection: CollectionId,
        alias: Option<impl AsRef<str>>,
        pipeline: PhysicalSubPipeline,
    ) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::union(collection, alias, pipeline)?)
    }

    pub fn streaming_load<I, S>(
        &mut self,
        mode: PhysicalLoadMode,
        chunks: I,
    ) -> PhysicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.push(PhysicalOperator::streaming_load(mode, chunks)?)
    }

    pub fn load(&mut self, target: impl AsRef<str>) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::load(target)?)
    }

    pub fn limit(&mut self, count: usize) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::limit(count))
    }

    pub fn skip(&mut self, count: usize) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::skip(count))
    }

    pub fn sort<I>(&mut self, keys: I) -> PhysicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = SortKey>,
    {
        self.push(PhysicalOperator::sort(keys)?)
    }

    pub fn select<I>(&mut self, fields: I) -> PhysicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        self.push(PhysicalOperator::select(fields)?)
    }

    pub fn distinct<I>(&mut self, fields: I) -> PhysicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        self.push(PhysicalOperator::distinct(fields)?)
    }

    pub fn count(&mut self, alias: impl AsRef<str>) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::count(alias)?)
    }

    pub fn delete(&mut self) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::delete())
    }

    pub fn insert(&mut self, document: InsertDocument) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::insert(document))
    }

    pub fn group<I>(&mut self, keys: I) -> PhysicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        self.push(PhysicalOperator::group(keys)?)
    }

    pub fn pivot(&mut self, specification: PivotSpecification) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::pivot(specification))
    }

    pub fn custom(
        &mut self,
        name: StageName,
        arguments: impl AsRef<str>,
        writes: bool,
        changes_cardinality: bool,
    ) -> PhysicalPlanResult<&mut Self> {
        self.push(PhysicalOperator::custom(
            name,
            arguments,
            writes,
            changes_cardinality,
        )?)
    }

    pub fn build(&self) -> PhysicalPlanResult<PhysicalPlan> {
        PhysicalPlan::new(self.source.clone(), self.operators.clone())
    }

    pub fn finish(self) -> PhysicalPlanResult<PhysicalPlan> {
        PhysicalPlan::new(self.source, self.operators)
    }
}

/// Stateless physical planner.
#[derive(Clone, Copy, Debug, Default)]
pub struct PhysicalPlanner {
    options: PhysicalPlannerOptions,
}

impl PhysicalPlanner {
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            options: PhysicalPlannerOptions::new(),
        }
    }

    #[must_use]
    pub const fn with_options(options: PhysicalPlannerOptions) -> Self {
        Self { options }
    }

    #[must_use]
    pub const fn options(&self) -> PhysicalPlannerOptions {
        self.options
    }

    #[must_use]
    pub fn plan_collection(&self, collection: CollectionId) -> PhysicalPlanBuilder {
        let options = ScanOptions::new().with_direction(self.options.scan_direction);
        PhysicalPlan::builder(PhysicalSource::scan(collection, options))
    }
}

/// Physical planning behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalPlannerOptions {
    scan_direction: ScanDirection,
}

impl PhysicalPlannerOptions {
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            scan_direction: ScanDirection::Forward,
        }
    }

    #[must_use]
    pub const fn with_scan_direction(mut self, direction: ScanDirection) -> Self {
        self.scan_direction = direction;
        self
    }

    #[must_use]
    pub const fn scan_direction(self) -> ScanDirection {
        self.scan_direction
    }
}

impl Default for PhysicalPlannerOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Field-list context used in physical diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PhysicalFieldContext {
    Select,
    Distinct,
    Group,
}

impl fmt::Display for PhysicalFieldContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Select => "select",
            Self::Distinct => "distinct",
            Self::Group => "group",
        })
    }
}

/// Physical-plan validation error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalPlanError {
    kind: PhysicalPlanErrorKind,
}

impl PhysicalPlanError {
    #[must_use]
    #[inline]
    pub const fn new(kind: PhysicalPlanErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &PhysicalPlanErrorKind {
        &self.kind
    }

    fn empty_load_target() -> Self {
        Self::new(PhysicalPlanErrorKind::EmptyLoadTarget)
    }

    fn empty_load_chunk() -> Self {
        Self::new(PhysicalPlanErrorKind::EmptyLoadChunk)
    }
}

impl fmt::Display for PhysicalPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            PhysicalPlanErrorKind::EmptySetAssignments => {
                formatter.write_str("set operator requires at least one assignment")
            }
            PhysicalPlanErrorKind::DuplicateSetField { field } => {
                write!(
                    formatter,
                    "set operator assigns field {field:?} more than once"
                )
            }
            PhysicalPlanErrorKind::EmptyLoadTarget => {
                formatter.write_str("load operator target must not be empty")
            }
            PhysicalPlanErrorKind::EmptyStreamingLoad => {
                formatter.write_str("streaming load requires at least one chunk")
            }
            PhysicalPlanErrorKind::EmptyLoadChunk => {
                formatter.write_str("streaming load chunks must not be empty")
            }
            PhysicalPlanErrorKind::InvalidLookupTarget { target } => {
                write!(formatter, "invalid lookup target field {target:?}")
            }
            PhysicalPlanErrorKind::InvalidAlias { alias } => {
                write!(formatter, "invalid physical source alias {alias:?}")
            }
            PhysicalPlanErrorKind::InvalidNestedOperator {
                operator_index,
                operator,
            } => write!(
                formatter,
                "nested pipeline operator {operator:?} at index {operator_index} must be read-only and non-terminal",
            ),
            PhysicalPlanErrorKind::EmptySortKeys => {
                formatter.write_str("sort operator requires at least one key")
            }
            PhysicalPlanErrorKind::DuplicateSortField { field } => {
                write!(
                    formatter,
                    "sort operator uses field {field:?} more than once"
                )
            }
            PhysicalPlanErrorKind::EmptyFieldList { context } => {
                write!(formatter, "{context} operator requires at least one field")
            }
            PhysicalPlanErrorKind::DuplicateField { context, field } => {
                write!(
                    formatter,
                    "{context} operator uses field {field:?} more than once"
                )
            }
            PhysicalPlanErrorKind::InvalidCountAlias { alias } => {
                write!(formatter, "invalid count alias {alias:?}")
            }
            PhysicalPlanErrorKind::InvalidCustomArguments { stage } => {
                write!(
                    formatter,
                    "custom operator {stage:?} arguments must not contain control characters",
                )
            }
            PhysicalPlanErrorKind::LoadMustBeFirst { operator_index } => {
                write!(
                    formatter,
                    "load operator at index {operator_index} must be first",
                )
            }
            PhysicalPlanErrorKind::InsertMustBeOnlyOperator { operator_index } => {
                write!(
                    formatter,
                    "insert operator at index {operator_index} must be the only operator",
                )
            }
            PhysicalPlanErrorKind::DuplicateOperator {
                operator_index,
                operator,
            } => {
                write!(
                    formatter,
                    "{operator} operator is duplicated at index {operator_index}",
                )
            }
            PhysicalPlanErrorKind::OperatorAfterTerminal {
                operator_index,
                operator,
            } => {
                write!(
                    formatter,
                    "operator {operator:?} at index {operator_index} follows a terminal operator",
                )
            }
        }
    }
}

impl StdError for PhysicalPlanError {}

/// Detailed physical-plan validation category.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PhysicalPlanErrorKind {
    EmptySetAssignments,
    DuplicateSetField {
        field: Arc<str>,
    },
    EmptyLoadTarget,
    EmptyStreamingLoad,
    EmptyLoadChunk,
    InvalidLookupTarget {
        target: Arc<str>,
    },
    InvalidAlias {
        alias: Arc<str>,
    },
    InvalidNestedOperator {
        operator_index: usize,
        operator: Arc<str>,
    },
    EmptySortKeys,
    DuplicateSortField {
        field: Arc<str>,
    },
    EmptyFieldList {
        context: PhysicalFieldContext,
    },
    DuplicateField {
        context: PhysicalFieldContext,
        field: Arc<str>,
    },
    InvalidCountAlias {
        alias: Arc<str>,
    },
    InvalidCustomArguments {
        stage: Arc<str>,
    },
    LoadMustBeFirst {
        operator_index: usize,
    },
    InsertMustBeOnlyOperator {
        operator_index: usize,
    },
    DuplicateOperator {
        operator_index: usize,
        operator: PhysicalOperatorKind,
    },
    OperatorAfterTerminal {
        operator_index: usize,
        operator: Arc<str>,
    },
}

fn normalize_optional_alias<A>(alias: Option<A>) -> PhysicalPlanResult<Option<Arc<str>>>
where
    A: AsRef<str>,
{
    match alias {
        Some(value) => {
            let value = value.as_ref().trim();
            validate_optional_identifier(value)?;
            Ok(Some(Arc::from(value)))
        }
        None => Ok(None),
    }
}

fn validate_optional_identifier(identifier: &str) -> PhysicalPlanResult<&str> {
    let identifier = identifier.trim();
    validate_identifier(identifier).map_err(|_| {
        PhysicalPlanError::new(PhysicalPlanErrorKind::InvalidAlias {
            alias: Arc::from(identifier),
        })
    })?;
    Ok(identifier)
}

fn validate_named_field(
    field: &str,
    constructor: fn(Arc<str>) -> PhysicalPlanErrorKind,
) -> PhysicalPlanResult<&str> {
    let field = field.trim();
    validate_identifier(field)
        .map_err(|_| PhysicalPlanError::new(constructor(Arc::from(field))))?;
    Ok(field)
}

fn validate_subpipeline(operators: &[PhysicalOperator]) -> PhysicalPlanResult<()> {
    let mut previous = Vec::with_capacity(operators.len());

    for (operator_index, operator) in operators.iter().enumerate() {
        if operator.execution_properties().closes_linear_pipeline() {
            return Err(PhysicalPlanError::new(
                PhysicalPlanErrorKind::InvalidNestedOperator {
                    operator_index,
                    operator: Arc::from(operator.name()),
                },
            ));
        }

        validate_next_operator(&previous, operator)?;
        previous.push(operator.clone());
    }

    Ok(())
}

fn non_empty_text<'a>(
    text: &'a str,
    error: fn() -> PhysicalPlanError,
) -> PhysicalPlanResult<&'a str> {
    let text = text.trim();

    if text.is_empty() {
        return Err(error());
    }

    Ok(text)
}

fn validate_plan(
    _source: &PhysicalSource,
    operators: &[PhysicalOperator],
) -> PhysicalPlanResult<()> {
    let mut prefix = Vec::with_capacity(operators.len());

    for operator in operators {
        validate_next_operator(&prefix, operator)?;
        prefix.push(operator.clone());
    }

    Ok(())
}

fn validate_next_operator(
    previous: &[PhysicalOperator],
    next: &PhysicalOperator,
) -> PhysicalPlanResult<()> {
    let operator_index = previous.len();

    if matches!(
        next,
        PhysicalOperator::Load { .. } | PhysicalOperator::StreamingLoad { .. }
    ) && operator_index != 0
    {
        return Err(PhysicalPlanError::new(
            PhysicalPlanErrorKind::LoadMustBeFirst { operator_index },
        ));
    }

    if matches!(next, PhysicalOperator::Insert { .. }) && operator_index != 0 {
        return Err(PhysicalPlanError::new(
            PhysicalPlanErrorKind::InsertMustBeOnlyOperator { operator_index },
        ));
    }

    if previous
        .iter()
        .any(|operator| matches!(operator, PhysicalOperator::Insert { .. }))
    {
        return Err(PhysicalPlanError::new(
            PhysicalPlanErrorKind::InsertMustBeOnlyOperator { operator_index },
        ));
    }

    if previous
        .last()
        .is_some_and(|operator| operator.execution_properties().closes_linear_pipeline())
    {
        return Err(PhysicalPlanError::new(
            PhysicalPlanErrorKind::OperatorAfterTerminal {
                operator_index,
                operator: Arc::from(next.name()),
            },
        ));
    }

    let kind = next.kind();
    let unique = matches!(
        kind,
        PhysicalOperatorKind::Lookup
            | PhysicalOperatorKind::Union
            | PhysicalOperatorKind::Load
            | PhysicalOperatorKind::StreamingLoad
            | PhysicalOperatorKind::Limit
            | PhysicalOperatorKind::Skip
            | PhysicalOperatorKind::Sort
            | PhysicalOperatorKind::Select
            | PhysicalOperatorKind::Distinct
            | PhysicalOperatorKind::Count
            | PhysicalOperatorKind::Delete
            | PhysicalOperatorKind::Insert
            | PhysicalOperatorKind::Group
            | PhysicalOperatorKind::Pivot
    );

    if unique && previous.iter().any(|operator| operator.kind() == kind) {
        return Err(PhysicalPlanError::new(
            PhysicalPlanErrorKind::DuplicateOperator {
                operator_index,
                operator: kind,
            },
        ));
    }

    Ok(())
}

fn validate_assignments(assignments: &[SetAssignment]) -> PhysicalPlanResult<()> {
    if assignments.is_empty() {
        return Err(PhysicalPlanError::new(
            PhysicalPlanErrorKind::EmptySetAssignments,
        ));
    }

    for (index, assignment) in assignments.iter().enumerate() {
        let field = assignment.field();

        if assignments[..index]
            .iter()
            .any(|previous| previous.field() == field)
        {
            return Err(PhysicalPlanError::new(
                PhysicalPlanErrorKind::DuplicateSetField {
                    field: Arc::from(format!("{field:?}")),
                },
            ));
        }
    }

    Ok(())
}

fn validate_sort_keys(keys: &[SortKey]) -> PhysicalPlanResult<()> {
    if keys.is_empty() {
        return Err(PhysicalPlanError::new(PhysicalPlanErrorKind::EmptySortKeys));
    }

    for (index, key) in keys.iter().enumerate() {
        if keys[..index]
            .iter()
            .any(|previous| previous.field() == key.field())
        {
            return Err(PhysicalPlanError::new(
                PhysicalPlanErrorKind::DuplicateSortField {
                    field: Arc::from(format!("{:?}", key.field())),
                },
            ));
        }
    }

    Ok(())
}

fn validate_fields(
    fields: &[ExpressionFieldPath],
    context: PhysicalFieldContext,
    allow_empty: bool,
) -> PhysicalPlanResult<()> {
    if fields.is_empty() && !allow_empty {
        return Err(PhysicalPlanError::new(
            PhysicalPlanErrorKind::EmptyFieldList { context },
        ));
    }

    for (index, field) in fields.iter().enumerate() {
        if fields[..index].iter().any(|previous| previous == field) {
            return Err(PhysicalPlanError::new(
                PhysicalPlanErrorKind::DuplicateField {
                    context,
                    field: Arc::from(format!("{field:?}")),
                },
            ));
        }
    }

    Ok(())
}

fn validate_identifier(identifier: &str) -> PhysicalPlanResult<()> {
    let mut characters = identifier.chars();

    let Some(first) = characters.next() else {
        return Err(PhysicalPlanError::new(
            PhysicalPlanErrorKind::InvalidCountAlias {
                alias: Arc::from(identifier),
            },
        ));
    };

    if first != '_' && !first.is_alphabetic() {
        return Err(PhysicalPlanError::new(
            PhysicalPlanErrorKind::InvalidCountAlias {
                alias: Arc::from(identifier),
            },
        ));
    }

    if characters.any(|character| {
        character != '_' && !character.is_alphabetic() && !character.is_ascii_digit()
    }) {
        return Err(PhysicalPlanError::new(
            PhysicalPlanErrorKind::InvalidCountAlias {
                alias: Arc::from(identifier),
            },
        ));
    }

    Ok(())
}

fn negotiate_source_access_vector(operators: &[PhysicalOperator]) -> (AccessVector, usize) {
    let mut prefix_len = 0usize;
    for operator in operators {
        match operator.execution_properties().projected_access {
            ProjectedAccess::Stage => prefix_len = prefix_len.saturating_add(1),
            ProjectedAccess::Consumer => return (AccessVector::ProjectedValues, prefix_len),
            ProjectedAccess::None => return (AccessVector::Document, 0),
        }
    }

    // A projected prefix without a projected consumer remains on Documents
    // until a terminal projected row materializer exists.
    (AccessVector::Document, 0)
}

fn negotiate_source_projection_reuse(operators: &[PhysicalOperator]) -> ProjectionReuse {
    let mut saw_projected_stage = false;
    for operator in operators {
        let properties = operator.execution_properties();
        if !properties.reuses_projection() {
            return ProjectionReuse::None;
        }
        match properties.projected_access {
            ProjectedAccess::Stage => saw_projected_stage = true,
            ProjectedAccess::Consumer => return ProjectionReuse::Reusable,
            ProjectedAccess::None => return ProjectionReuse::None,
        }
    }
    if saw_projected_stage {
        ProjectionReuse::Reusable
    } else {
        ProjectionReuse::None
    }
}

fn summarize_execution(
    operators: &[PhysicalOperator],
) -> (ExecutionMode, MemoryExecutionMode, bool) {
    let (writes, blocking, changes) = operators.iter().fold((false, false, false), |state, op| {
        let p = op.execution_properties();
        (
            state.0 || p.writes(),
            state.1 || !matches!(p.flow, Flow::Streaming),
            state.2 || !matches!(p.cardinality, CardinalityEffect::Preserve),
        )
    });
    (
        if writes {
            ExecutionMode::ReadWrite
        } else {
            ExecutionMode::ReadOnly
        },
        if writes {
            MemoryExecutionMode::Transactional
        } else if blocking {
            MemoryExecutionMode::GovernedBlocking
        } else {
            MemoryExecutionMode::Streaming
        },
        changes,
    )
}

#[cfg(test)]
mod tests {

    #[test]
    fn negotiates_projected_values_for_filter_group_source_prefix() {
        let predicate = crate::query::parse_expression(r#"x == 1"#).unwrap();
        let field = ExpressionFieldPath::new(["x"]).unwrap();
        let operators = vec![
            PhysicalOperator::Filter { predicate },
            PhysicalOperator::Group {
                keys: Arc::from([field]),
            },
        ];
        assert_eq!(
            negotiate_source_access_vector(&operators),
            (AccessVector::ProjectedValues, 1)
        );
    }

    #[test]
    fn negotiates_projected_values_for_filter_sort_source_prefix() {
        let predicate = crate::query::parse_expression(r#"x == 1"#).unwrap();
        let field = ExpressionFieldPath::new(["score"]).unwrap();
        let operators = vec![
            PhysicalOperator::Filter { predicate },
            PhysicalOperator::Sort {
                keys: Arc::from([SortKey::ascending(field)]),
            },
        ];
        assert_eq!(
            negotiate_source_access_vector(&operators),
            (AccessVector::ProjectedValues, 1)
        );
    }

    #[test]
    fn explicit_distinct_negotiates_projected_values_but_document_distinct_does_not() {
        let field = ExpressionFieldPath::new(["name"]).unwrap();
        assert_eq!(
            negotiate_source_access_vector(&[PhysicalOperator::Distinct {
                fields: Arc::from([field]),
            }]),
            (AccessVector::ProjectedValues, 0)
        );
        assert_eq!(
            negotiate_source_access_vector(&[PhysicalOperator::Distinct {
                fields: Arc::from([]),
            }]),
            (AccessVector::Document, 0)
        );
    }

    #[test]
    fn reusable_projection_is_negotiated_from_stage_properties() {
        let predicate = crate::query::parse_expression(r#"active == true"#).unwrap();
        let field = ExpressionFieldPath::new(["score"]).unwrap();
        let operators = vec![
            PhysicalOperator::Filter { predicate },
            PhysicalOperator::Sort {
                keys: Arc::from([SortKey::ascending(field)]),
            },
        ];
        assert_eq!(
            negotiate_source_projection_reuse(&operators),
            ProjectionReuse::Reusable
        );
        assert!(operators
            .iter()
            .all(|operator| operator.execution_properties().defers_materialization()));
        assert!(operators
            .iter()
            .all(|operator| operator.execution_properties().reuses_projection()));
    }

    #[test]
    fn materializing_or_unsupported_stage_blocks_projection_reuse() {
        let field = ExpressionFieldPath::new(["score"]).unwrap();
        let with_skip = vec![
            PhysicalOperator::Skip { count: 1 },
            PhysicalOperator::Sort {
                keys: Arc::from([SortKey::ascending(field.clone())]),
            },
        ];
        assert_eq!(
            negotiate_source_projection_reuse(&with_skip),
            ProjectionReuse::None
        );

        let document_distinct = [PhysicalOperator::Distinct {
            fields: Arc::from([]),
        }];
        assert_eq!(
            negotiate_source_projection_reuse(&document_distinct),
            ProjectionReuse::None
        );
        assert!(!document_distinct[0]
            .execution_properties()
            .defers_materialization());
        assert!(!document_distinct[0]
            .execution_properties()
            .reuses_projection());
    }

    #[test]
    fn incompatible_source_stage_forces_document_vector() {
        let field = ExpressionFieldPath::new(["x"]).unwrap();
        let operators = vec![
            PhysicalOperator::Skip { count: 1 },
            PhysicalOperator::Group {
                keys: Arc::from([field]),
            },
        ];
        assert_eq!(
            negotiate_source_access_vector(&operators),
            (AccessVector::Document, 0)
        );
    }

    use super::*;

    use crate::query::{
        logical_plan::{InsertDocument, PivotAggregate, PivotSpecification, PivotValue},
        parse_expression, SortDirection,
    };

    #[inline]
    fn source() -> PhysicalSource {
        PhysicalSource::collection_scan(CollectionId::parse("users").unwrap())
    }

    #[inline]
    fn field(path: &[&str]) -> ExpressionFieldPath {
        ExpressionFieldPath::new(path.iter().copied()).unwrap()
    }

    fn insert_document() -> InsertDocument {
        InsertDocument::parse(
            r#"{
                name: "Alice",
                active: true,
                tags: ["rust", "database"],
                address: {city: "Paris"},
            }"#,
        )
        .unwrap()
    }

    fn pivot_specification() -> PivotSpecification {
        PivotSpecification::new(
            [field(&["region"])],
            [field(&["month"])],
            [
                PivotValue::new(
                    field(&["revenue"]),
                    PivotAggregate::Sum,
                    Some("total_revenue"),
                )
                .unwrap(),
                PivotValue::new(field(&["orders"]), PivotAggregate::Count, None::<&str>).unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn source_defaults_to_forward_collection_scan() {
        let collection = CollectionId::parse("users").unwrap();
        let source = PhysicalSource::collection_scan(collection.clone());

        assert_eq!(source.collection(), &collection);
        assert_eq!(source.access().scan_options(), Some(ScanOptions::default()));
    }

    #[test]
    fn planner_applies_scan_direction() {
        let planner = PhysicalPlanner::with_options(
            PhysicalPlannerOptions::new().with_scan_direction(ScanDirection::Reverse),
        );

        let plan = planner
            .plan_collection(CollectionId::parse("users").unwrap())
            .finish()
            .unwrap();

        assert_eq!(
            plan.source().access().scan_options().unwrap().direction(),
            ScanDirection::Reverse,
        );
    }

    #[test]
    fn supports_complete_read_pipeline() {
        let mut builder = PhysicalPlan::builder(source());

        builder
            .filter(parse_expression("active == true").unwrap())
            .unwrap()
            .sort([
                SortKey::new(field(&["age"]), SortDirection::Descending),
                SortKey::new(field(&["name"]), SortDirection::Ascending),
            ])
            .unwrap()
            .skip(10)
            .unwrap()
            .limit(20)
            .unwrap()
            .select([field(&["name"]), field(&["age"])])
            .unwrap()
            .distinct([field(&["name"])])
            .unwrap();

        let plan = builder.finish().unwrap();

        assert_eq!(plan.len(), 6);
        assert!(!plan.is_write());
        assert!(plan.changes_cardinality());
        assert_eq!(plan.mode(), ExecutionMode::ReadOnly);
    }

    #[test]
    fn load_requires_a_transaction_and_is_terminal() {
        let mut builder = PhysicalPlan::builder(source());
        builder.load("profile").unwrap();

        let error = builder.limit(1).unwrap_err();
        assert!(matches!(
            error.kind(),
            PhysicalPlanErrorKind::OperatorAfterTerminal { .. }
        ));

        let plan = builder.finish().unwrap();
        assert!(plan.is_write());
        assert_eq!(
            plan.required_storage_access(),
            StorageAccessMode::Transaction
        );
    }

    #[test]
    fn insert_preserves_typed_document() {
        let document = insert_document();
        let operator = PhysicalOperator::insert(document.clone());

        assert_eq!(operator.kind(), PhysicalOperatorKind::Insert);
        assert_eq!(operator.insert_document(), Some(&document));
        assert!(operator.execution_properties().writes());
        assert!(!matches!(
            operator.execution_properties().cardinality,
            CardinalityEffect::Preserve
        ));
        assert!(operator.execution_properties().closes_linear_pipeline());
    }

    #[test]
    fn insert_requires_transaction_and_must_be_alone() {
        let mut builder = PhysicalPlan::builder(source());
        builder.insert(insert_document()).unwrap();

        let plan = builder.finish().unwrap();
        assert!(plan.is_write());
        assert_eq!(
            plan.required_storage_access(),
            StorageAccessMode::Transaction
        );

        let mut invalid = PhysicalPlan::builder(source());
        invalid
            .filter(parse_expression("active == true").unwrap())
            .unwrap();

        assert!(matches!(
            invalid.insert(insert_document()).unwrap_err().kind(),
            PhysicalPlanErrorKind::InsertMustBeOnlyOperator { .. }
        ));
    }

    #[test]
    fn pivot_preserves_typed_specification() {
        let specification = pivot_specification();
        let operator = PhysicalOperator::pivot(specification.clone());

        assert_eq!(operator.kind(), PhysicalOperatorKind::Pivot);
        assert_eq!(operator.pivot_specification(), Some(&specification));
        assert!(!operator.execution_properties().writes());
        assert!(!matches!(
            operator.execution_properties().cardinality,
            CardinalityEffect::Preserve
        ));
        assert!(operator.execution_properties().closes_linear_pipeline());
    }

    #[test]
    fn builder_supports_pivot() {
        let specification = pivot_specification();
        let mut builder = PhysicalPlan::builder(source());

        builder.pivot(specification.clone()).unwrap();
        let plan = builder.finish().unwrap();

        assert_eq!(plan.mode(), ExecutionMode::ReadOnly);
        assert_eq!(
            plan.operators()[0].pivot_specification(),
            Some(&specification)
        );
    }

    #[test]
    fn pivot_rejects_following_operators() {
        let mut builder = PhysicalPlan::builder(source());
        builder.pivot(pivot_specification()).unwrap();

        assert!(matches!(
            builder.limit(1).unwrap_err().kind(),
            PhysicalPlanErrorKind::OperatorAfterTerminal { .. }
        ));
    }

    #[test]
    fn distinct_without_fields_means_complete_documents() {
        let operator = PhysicalOperator::distinct([]).unwrap();
        assert_eq!(operator.distinct_fields(), Some(&[][..]));
    }

    #[test]
    fn duplicate_unique_operator_is_rejected() {
        let mut builder = PhysicalPlan::builder(source());
        builder.limit(10).unwrap();

        assert!(matches!(
            builder.limit(20).unwrap_err().kind(),
            PhysicalPlanErrorKind::DuplicateOperator {
                operator: PhysicalOperatorKind::Limit,
                ..
            }
        ));
    }

    #[test]
    fn validates_native_operator_arguments() {
        assert!(matches!(
            PhysicalOperator::sort([]).unwrap_err().kind(),
            PhysicalPlanErrorKind::EmptySortKeys
        ));

        assert!(matches!(
            PhysicalOperator::select([]).unwrap_err().kind(),
            PhysicalPlanErrorKind::EmptyFieldList {
                context: PhysicalFieldContext::Select
            }
        ));

        assert!(matches!(
            PhysicalOperator::group([]).unwrap_err().kind(),
            PhysicalPlanErrorKind::EmptyFieldList {
                context: PhysicalFieldContext::Group
            }
        ));
    }

    #[test]
    fn supports_lookup_and_union_subpipelines() {
        let nested = PhysicalSubPipeline::new([PhysicalOperator::filter(
            parse_expression("active == true").unwrap(),
        )])
        .unwrap();

        let lookup = PhysicalOperator::lookup(
            CollectionId::parse("workspace").unwrap(),
            Some("w"),
            "public",
            nested.clone(),
        )
        .unwrap();

        assert_eq!(lookup.kind(), PhysicalOperatorKind::Lookup);
        assert_eq!(lookup.lookup_alias(), Some("w"));
        assert_eq!(lookup.lookup_target(), Some("public"));
        assert!(!lookup.execution_properties().writes());
        assert!(!!matches!(
            lookup.execution_properties().cardinality,
            CardinalityEffect::Preserve
        ));

        let union = PhysicalOperator::union(
            CollectionId::parse("archived_users").unwrap(),
            None::<&str>,
            nested,
        )
        .unwrap();

        assert_eq!(union.kind(), PhysicalOperatorKind::Union);
        assert!(!matches!(
            union.execution_properties().cardinality,
            CardinalityEffect::Preserve
        ));
        assert!(!union.execution_properties().writes());
    }

    #[test]
    fn nested_pipeline_rejects_mutations_and_terminal_operators() {
        assert!(matches!(
            PhysicalSubPipeline::new([PhysicalOperator::delete()])
                .unwrap_err()
                .kind(),
            PhysicalPlanErrorKind::InvalidNestedOperator { .. }
        ));

        assert!(matches!(
            PhysicalSubPipeline::new([PhysicalOperator::pivot(pivot_specification())])
                .unwrap_err()
                .kind(),
            PhysicalPlanErrorKind::InvalidNestedOperator { .. }
        ));
    }

    #[test]
    fn supports_streaming_load() {
        let operator =
            PhysicalOperator::streaming_load(PhysicalLoadMode::Replace, ["batch1", "batch2"])
                .unwrap();

        assert_eq!(operator.kind(), PhysicalOperatorKind::StreamingLoad);
        assert_eq!(
            operator.streaming_load_mode(),
            Some(PhysicalLoadMode::Replace)
        );
        assert_eq!(operator.streaming_load_chunks().unwrap().len(), 2);
        assert!(operator.execution_properties().writes());
        assert!(operator.execution_properties().closes_linear_pipeline());
    }

    #[test]
    fn row_local_read_only_custom_operator_is_streaming() {
        let operator = PhysicalOperator::custom(
            StageName::parse("select").unwrap(),
            "CAFacture - COGS as Marge",
            false,
            false,
        )
        .unwrap();

        assert!(matches!(
            operator.execution_properties().flow,
            Flow::Streaming
        ));
        assert!(!matches!(
            operator.execution_properties().flow,
            Flow::GovernedBlocking
        ));
    }

    #[test]
    fn cardinality_changing_custom_operator_is_set_level() {
        let operator =
            PhysicalOperator::custom(StageName::parse("sample").unwrap(), "3", false, true)
                .unwrap();

        assert!(matches!(
            operator.execution_properties().flow,
            Flow::GovernedBlocking
        ));
        assert!(matches!(
            operator.execution_properties().scope,
            super::super::execution_properties::Scope::Set
        ));
    }

    #[test]
    fn classifies_streaming_and_blocking_memory_contracts() {
        let collection = CollectionId::parse("users").unwrap();
        let streaming = PhysicalPlan::new(
            PhysicalSource::collection_scan(collection.clone()),
            [PhysicalOperator::limit(2)],
        )
        .unwrap();
        assert_eq!(
            streaming.memory_execution_mode(),
            MemoryExecutionMode::Streaming
        );
        assert!(streaming.is_memory_streaming());

        let blocking = PhysicalPlan::new(
            PhysicalSource::collection_scan(collection),
            [PhysicalOperator::sort([SortKey::ascending(field(&["name"]))]).unwrap()],
        )
        .unwrap();
        assert_eq!(
            blocking.memory_execution_mode(),
            MemoryExecutionMode::GovernedBlocking
        );
        assert!(!blocking.is_memory_streaming());

        let streaming_count = PhysicalPlan::new(
            PhysicalSource::collection_scan(CollectionId::parse("users").unwrap()),
            [
                PhysicalOperator::filter(parse_expression("active == true").unwrap()),
                PhysicalOperator::count("count").unwrap(),
            ],
        )
        .unwrap();
        assert_eq!(
            streaming_count.memory_execution_mode(),
            MemoryExecutionMode::Streaming
        );
        assert!(streaming_count.is_memory_streaming());
    }
}
