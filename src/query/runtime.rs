//! Runtime handlers for physical operators.

use std::{cmp::Ordering, fmt, sync::Arc};

pub use crate::error::QueryRuntimeBuildError;

use super::executor::PreparedInsertDocument;
use super::{
    CustomOperatorResult, ExecutionError, ExecutionResult, ExecutionRuntime, Expression,
    ExpressionFieldPath, ExpressionFieldResolver, IncrementalGroupAccumulator, LookupDocuments,
    PhysicalLoadMode, SetAssignment, SortKey, StageName, StreamingLoadMutation, SyntheticDocument,
};
use crate::{
    storage::{CollectionId, StorageRead},
    Document, Value,
};

use super::logical_plan::{InsertDocument as LogicalInsertDocument, PivotSpecification};

type PredicateHandler = dyn Fn(&Expression, &Document) -> ExecutionResult<bool> + Send + Sync;
type ResolvedPredicateHandler =
    dyn Fn(&Expression, &dyn ExpressionFieldResolver<Value>) -> ExecutionResult<bool> + Send + Sync;

type SetHandler =
    dyn Fn(&[SetAssignment], &Document) -> ExecutionResult<Arc<Document>> + Send + Sync;

type LookupPredicateHandler =
    dyn Fn(&Expression, &Document, Option<&str>, &Document) -> ExecutionResult<bool> + Send + Sync;

type LookupHandler =
    dyn Fn(&str, &Document, &LookupDocuments) -> ExecutionResult<Arc<Document>> + Send + Sync;

type StreamingLoadHandler = dyn Fn(
        &CollectionId,
        &dyn StorageRead,
        PhysicalLoadMode,
        &[Arc<str>],
    ) -> ExecutionResult<Vec<StreamingLoadMutation>>
    + Send
    + Sync;

type LoadHandler = dyn Fn(&str, &Document) -> ExecutionResult<Arc<Document>> + Send + Sync;

type CompareHandler =
    dyn Fn(&[SortKey], &Document, &Document) -> ExecutionResult<Ordering> + Send + Sync;

type SelectHandler =
    dyn Fn(&[ExpressionFieldPath], &Document) -> ExecutionResult<Arc<Document>> + Send + Sync;

type DistinctHandler =
    dyn Fn(&[ExpressionFieldPath], &Document) -> ExecutionResult<Arc<[u8]>> + Send + Sync;

type CountHandler = dyn Fn(&str, u64) -> ExecutionResult<Arc<Document>> + Send + Sync;

type GroupHandler = dyn Fn(&[ExpressionFieldPath], &[Arc<Document>]) -> ExecutionResult<Vec<SyntheticDocument>>
    + Send
    + Sync;

type IncrementalGroupHandler = dyn Fn(&[ExpressionFieldPath]) -> ExecutionResult<Box<dyn IncrementalGroupAccumulator>>
    + Send
    + Sync;

type InsertHandler =
    dyn Fn(&LogicalInsertDocument) -> ExecutionResult<PreparedInsertDocument> + Send + Sync;

type PivotHandler = dyn Fn(&PivotSpecification, &[Arc<Document>]) -> ExecutionResult<Vec<SyntheticDocument>>
    + Send
    + Sync;

type CustomHandler = dyn Fn(&StageName, &str, bool, &Document) -> ExecutionResult<CustomOperatorResult>
    + Send
    + Sync;

/// Configurable implementation of [`ExecutionRuntime`].
///
/// Predicate evaluation and `set` mutation remain mandatory because they are
/// the two foundational row-level semantics. Every other handler is optional
/// and reports an unsupported-operator error when absent.
#[derive(Clone)]
pub struct QueryRuntime {
    predicate: Arc<PredicateHandler>,
    resolved_predicate: Option<Arc<ResolvedPredicateHandler>>,
    lookup_predicate: Option<Arc<LookupPredicateHandler>>,
    set: Arc<SetHandler>,
    lookup: Option<Arc<LookupHandler>>,
    streaming_load: Option<Arc<StreamingLoadHandler>>,
    load: Option<Arc<LoadHandler>>,
    compare: Option<Arc<CompareHandler>>,
    select: Option<Arc<SelectHandler>>,
    distinct: Option<Arc<DistinctHandler>>,
    count: Option<Arc<CountHandler>>,
    group: Option<Arc<GroupHandler>>,
    incremental_group: Option<Arc<IncrementalGroupHandler>>,
    pivot: Option<Arc<PivotHandler>>,
    insert: Option<Arc<InsertHandler>>,
    custom: Option<Arc<CustomHandler>>,
}

impl QueryRuntime {
    /// Creates a runtime from its two mandatory operations.
    #[must_use]
    pub fn new<P, S>(predicate: P, set: S) -> Self
    where
        P: Fn(&Expression, &Document) -> ExecutionResult<bool> + Send + Sync + 'static,
        S: Fn(&[SetAssignment], &Document) -> ExecutionResult<Arc<Document>>
            + Send
            + Sync
            + 'static,
    {
        Self {
            predicate: Arc::new(predicate),
            resolved_predicate: None,
            lookup_predicate: None,
            set: Arc::new(set),
            lookup: None,
            streaming_load: None,
            load: None,
            compare: None,
            select: None,
            distinct: None,
            count: None,
            group: None,
            incremental_group: None,
            pivot: None,
            insert: None,
            custom: None,
        }
    }

    /// Creates a runtime that reports every operation as unsupported.
    ///
    /// This is useful for scan-only engines and incremental integration.
    #[must_use]
    pub fn unsupported() -> Self {
        Self::new(
            |_expression, _document| {
                Err(ExecutionError::unsupported_operator(
                    "filter",
                    "predicate evaluation runtime is not configured",
                ))
            },
            |_assignments, _document| {
                Err(ExecutionError::unsupported_operator(
                    "set",
                    "document mutation runtime is not configured",
                ))
            },
        )
    }

    /// Installs predicate evaluation over a generic field resolver.
    #[must_use]
    pub fn with_resolved_predicate<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&Expression, &dyn ExpressionFieldResolver<Value>) -> ExecutionResult<bool>
            + Send
            + Sync
            + 'static,
    {
        self.resolved_predicate = Some(Arc::new(predicate));
        self
    }

    /// Installs a lookup-aware predicate evaluator.
    ///
    /// When absent, lookup predicates fall back to the regular predicate
    /// handler and are evaluated against the inner document only.
    #[must_use]
    pub fn with_lookup_predicate<P>(mut self, predicate: P) -> Self
    where
        P: Fn(&Expression, &Document, Option<&str>, &Document) -> ExecutionResult<bool>
            + Send
            + Sync
            + 'static,
    {
        self.lookup_predicate = Some(Arc::new(predicate));
        self
    }

    /// Installs the lookup result assembler.
    #[must_use]
    pub fn with_lookup<L>(mut self, lookup: L) -> Self
    where
        L: Fn(&str, &Document, &LookupDocuments) -> ExecutionResult<Arc<Document>>
            + Send
            + Sync
            + 'static,
    {
        self.lookup = Some(Arc::new(lookup));
        self
    }

    /// Installs the streaming-load preparation handler.
    #[must_use]
    pub fn with_streaming_load<L>(mut self, load: L) -> Self
    where
        L: Fn(
                &CollectionId,
                &dyn StorageRead,
                PhysicalLoadMode,
                &[Arc<str>],
            ) -> ExecutionResult<Vec<StreamingLoadMutation>>
            + Send
            + Sync
            + 'static,
    {
        self.streaming_load = Some(Arc::new(load));
        self
    }

    /// Installs a relationship-load handler.
    #[must_use]
    pub fn with_load<L>(mut self, load: L) -> Self
    where
        L: Fn(&str, &Document) -> ExecutionResult<Arc<Document>> + Send + Sync + 'static,
    {
        self.load = Some(Arc::new(load));
        self
    }

    /// Installs the sort-comparison handler.
    #[must_use]
    pub fn with_compare<C>(mut self, compare: C) -> Self
    where
        C: Fn(&[SortKey], &Document, &Document) -> ExecutionResult<Ordering>
            + Send
            + Sync
            + 'static,
    {
        self.compare = Some(Arc::new(compare));
        self
    }

    /// Installs the projection handler.
    #[must_use]
    pub fn with_select<S>(mut self, select: S) -> Self
    where
        S: Fn(&[ExpressionFieldPath], &Document) -> ExecutionResult<Arc<Document>>
            + Send
            + Sync
            + 'static,
    {
        self.select = Some(Arc::new(select));
        self
    }

    /// Installs the deterministic distinct-key handler.
    #[must_use]
    pub fn with_distinct<D>(mut self, distinct: D) -> Self
    where
        D: Fn(&[ExpressionFieldPath], &Document) -> ExecutionResult<Arc<[u8]>>
            + Send
            + Sync
            + 'static,
    {
        self.distinct = Some(Arc::new(distinct));
        self
    }

    /// Installs the count-result handler.
    #[must_use]
    pub fn with_count<C>(mut self, count: C) -> Self
    where
        C: Fn(&str, u64) -> ExecutionResult<Arc<Document>> + Send + Sync + 'static,
    {
        self.count = Some(Arc::new(count));
        self
    }

    /// Installs the grouping handler.
    #[must_use]
    pub fn with_group<G>(mut self, group: G) -> Self
    where
        G: Fn(&[ExpressionFieldPath], &[Arc<Document>]) -> ExecutionResult<Vec<SyntheticDocument>>
            + Send
            + Sync
            + 'static,
    {
        self.group = Some(Arc::new(group));
        self
    }

    /// Installs capability-driven incremental group aggregation.
    #[must_use]
    pub fn with_incremental_group<G>(mut self, group: G) -> Self
    where
        G: Fn(&[ExpressionFieldPath]) -> ExecutionResult<Box<dyn IncrementalGroupAccumulator>>
            + Send
            + Sync
            + 'static,
    {
        self.incremental_group = Some(Arc::new(group));
        self
    }

    /// Installs the typed insert-materialization handler.
    #[must_use]
    pub fn with_insert<I>(mut self, insert: I) -> Self
    where
        I: Fn(&LogicalInsertDocument) -> ExecutionResult<PreparedInsertDocument>
            + Send
            + Sync
            + 'static,
    {
        self.insert = Some(Arc::new(insert));
        self
    }

    /// Installs the pivot handler.
    #[must_use]
    pub fn with_pivot<P>(mut self, pivot: P) -> Self
    where
        P: Fn(&PivotSpecification, &[Arc<Document>]) -> ExecutionResult<Vec<SyntheticDocument>>
            + Send
            + Sync
            + 'static,
    {
        self.pivot = Some(Arc::new(pivot));
        self
    }

    /// Installs a custom-stage handler.
    #[must_use]
    pub fn with_custom<C>(mut self, custom: C) -> Self
    where
        C: Fn(&StageName, &str, bool, &Document) -> ExecutionResult<CustomOperatorResult>
            + Send
            + Sync
            + 'static,
    {
        self.custom = Some(Arc::new(custom));
        self
    }

    /// Returns whether lookup-aware predicate evaluation is configured.
    #[must_use]
    pub const fn supports_lookup_predicate(&self) -> bool {
        self.lookup_predicate.is_some()
    }

    /// Returns whether lookup result assembly is configured.
    #[must_use]
    pub const fn supports_lookup(&self) -> bool {
        self.lookup.is_some()
    }

    /// Returns whether streaming-load preparation is configured.
    #[must_use]
    pub const fn supports_streaming_load(&self) -> bool {
        self.streaming_load.is_some()
    }

    /// Returns whether relationship loading is configured.
    #[must_use]
    pub const fn supports_load(&self) -> bool {
        self.load.is_some()
    }

    /// Returns whether sorting is configured.
    #[must_use]
    pub const fn supports_sort(&self) -> bool {
        self.compare.is_some()
    }

    /// Returns whether projection is configured.
    #[must_use]
    pub const fn supports_select(&self) -> bool {
        self.select.is_some()
    }

    /// Returns whether distinct-key extraction is configured.
    #[must_use]
    pub const fn supports_distinct(&self) -> bool {
        self.distinct.is_some()
    }

    /// Returns whether count result construction is configured.
    #[must_use]
    pub const fn supports_count(&self) -> bool {
        self.count.is_some()
    }

    /// Returns whether grouping is configured.
    #[must_use]
    pub const fn supports_group(&self) -> bool {
        self.group.is_some()
    }

    /// Returns whether insert materialization is configured.
    #[must_use]
    pub const fn supports_insert(&self) -> bool {
        self.insert.is_some()
    }

    /// Returns whether pivot aggregation is configured.
    #[must_use]
    pub const fn supports_pivot(&self) -> bool {
        self.pivot.is_some()
    }

    /// Returns whether custom operators are configured.
    #[must_use]
    pub const fn supports_custom(&self) -> bool {
        self.custom.is_some()
    }
}

impl Default for QueryRuntime {
    fn default() -> Self {
        Self::unsupported()
    }
}

impl fmt::Debug for QueryRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryRuntime")
            .field("predicate", &"<handler>")
            .field("lookup_predicate", &self.supports_lookup_predicate())
            .field("set", &"<handler>")
            .field("lookup", &self.supports_lookup())
            .field("streaming_load", &self.supports_streaming_load())
            .field("load", &self.supports_load())
            .field("sort", &self.supports_sort())
            .field("select", &self.supports_select())
            .field("distinct", &self.supports_distinct())
            .field("count", &self.supports_count())
            .field("group", &self.supports_group())
            .field("incremental_group", &self.incremental_group.is_some())
            .field("pivot", &self.supports_pivot())
            .field("insert", &self.supports_insert())
            .field("custom", &self.supports_custom())
            .finish()
    }
}

impl ExecutionRuntime for QueryRuntime {
    fn evaluate_predicate(
        &self,
        expression: &Expression,
        document: &Document,
    ) -> ExecutionResult<bool> {
        (self.predicate)(expression, document)
    }

    fn evaluate_resolved_predicate(
        &self,
        expression: &Expression,
        resolver: &dyn ExpressionFieldResolver<Value>,
    ) -> ExecutionResult<bool> {
        match &self.resolved_predicate {
            Some(predicate) => predicate(expression, resolver),
            None => Err(ExecutionError::unsupported_operator(
                "filter",
                "resolved predicate evaluation runtime is not configured",
            )),
        }
    }

    fn evaluate_lookup_predicate(
        &self,
        expression: &Expression,
        outer: &Document,
        inner_alias: Option<&str>,
        inner: &Document,
    ) -> ExecutionResult<bool> {
        match &self.lookup_predicate {
            Some(predicate) => predicate(expression, outer, inner_alias, inner),
            None => (self.predicate)(expression, inner),
        }
    }

    fn apply_set(
        &self,
        assignments: &[SetAssignment],
        document: &Document,
    ) -> ExecutionResult<Arc<Document>> {
        (self.set)(assignments, document)
    }

    fn apply_lookup(
        &self,
        into: &str,
        outer: &Document,
        matches: &LookupDocuments,
    ) -> ExecutionResult<Arc<Document>> {
        match &self.lookup {
            Some(lookup) => lookup(into, outer, matches),
            None => Err(ExecutionError::unsupported_operator(
                "lookup",
                format!("lookup target {into:?} has no configured runtime handler"),
            )),
        }
    }

    fn prepare_streaming_load(
        &self,
        collection: &CollectionId,
        storage: &dyn StorageRead,
        mode: PhysicalLoadMode,
        chunks: &[Arc<str>],
    ) -> ExecutionResult<Vec<StreamingLoadMutation>> {
        match &self.streaming_load {
            Some(load) => load(collection, storage, mode, chunks),
            None => Err(ExecutionError::unsupported_operator(
                "streaming-load",
                format!("streaming load mode {mode} has no configured runtime handler"),
            )),
        }
    }

    fn apply_load(&self, target: &str, document: &Document) -> ExecutionResult<Arc<Document>> {
        match &self.load {
            Some(load) => load(target, document),
            None => Err(ExecutionError::unsupported_operator(
                "load",
                format!("load target {target:?} has no configured runtime handler"),
            )),
        }
    }

    fn compare_documents(
        &self,
        keys: &[SortKey],
        left: &Document,
        right: &Document,
    ) -> ExecutionResult<Ordering> {
        match &self.compare {
            Some(compare) => compare(keys, left, right),
            None => Err(ExecutionError::unsupported_operator(
                "sort",
                "document comparison runtime is not configured",
            )),
        }
    }

    fn apply_select(
        &self,
        fields: &[ExpressionFieldPath],
        document: &Document,
    ) -> ExecutionResult<Arc<Document>> {
        match &self.select {
            Some(select) => select(fields, document),
            None => Err(ExecutionError::unsupported_operator(
                "select",
                "document projection runtime is not configured",
            )),
        }
    }

    fn distinct_key(
        &self,
        fields: &[ExpressionFieldPath],
        document: &Document,
    ) -> ExecutionResult<Arc<[u8]>> {
        match &self.distinct {
            Some(distinct) => distinct(fields, document),
            None => Err(ExecutionError::unsupported_operator(
                "distinct",
                "distinct-key runtime is not configured",
            )),
        }
    }

    fn count_document(&self, alias: &str, count: u64) -> ExecutionResult<Arc<Document>> {
        match &self.count {
            Some(handler) => handler(alias, count),
            None => Err(ExecutionError::unsupported_operator(
                "count",
                format!("count result alias {alias:?} has no configured runtime handler"),
            )),
        }
    }

    fn incremental_group_accumulator(
        &self,
        keys: &[ExpressionFieldPath],
    ) -> ExecutionResult<Option<Box<dyn IncrementalGroupAccumulator>>> {
        match &self.incremental_group {
            Some(group) => group(keys).map(Some),
            None => Ok(None),
        }
    }

    fn group_documents(
        &self,
        keys: &[ExpressionFieldPath],
        documents: &[Arc<Document>],
    ) -> ExecutionResult<Vec<SyntheticDocument>> {
        match &self.group {
            Some(group) => group(keys, documents),
            None => Err(ExecutionError::unsupported_operator(
                "group",
                "group aggregation runtime is not configured",
            )),
        }
    }

    fn prepare_insert(
        &self,
        document: &LogicalInsertDocument,
    ) -> ExecutionResult<PreparedInsertDocument> {
        match &self.insert {
            Some(insert) => insert(document),
            None => Err(ExecutionError::unsupported_operator(
                "insert",
                "typed insert document has no configured runtime handler",
            )),
        }
    }

    fn pivot_documents(
        &self,
        specification: &PivotSpecification,
        documents: &[Arc<Document>],
    ) -> ExecutionResult<Vec<SyntheticDocument>> {
        match &self.pivot {
            Some(pivot) => pivot(specification, documents),
            None => Err(ExecutionError::unsupported_operator(
                "pivot",
                "pivot aggregation runtime is not configured",
            )),
        }
    }

    fn apply_custom(
        &self,
        stage: &StageName,
        arguments: &str,
        writes: bool,
        document: &Document,
    ) -> ExecutionResult<CustomOperatorResult> {
        match &self.custom {
            Some(custom) => custom(stage, arguments, writes, document),
            None => Err(ExecutionError::unsupported_operator(
                stage.as_str(),
                "custom operator has no configured runtime handler",
            )),
        }
    }
}

/// Builder for [`QueryRuntime`].
///
/// Calling [`QueryRuntimeBuilder::build`] validates that the mandatory
/// predicate and `set` handlers were installed. Optional handlers may be added
/// incrementally as operators become available.
#[derive(Clone, Default)]
pub struct QueryRuntimeBuilder {
    predicate: Option<Arc<PredicateHandler>>,
    lookup_predicate: Option<Arc<LookupPredicateHandler>>,
    set: Option<Arc<SetHandler>>,
    lookup: Option<Arc<LookupHandler>>,
    streaming_load: Option<Arc<StreamingLoadHandler>>,
    load: Option<Arc<LoadHandler>>,
    compare: Option<Arc<CompareHandler>>,
    select: Option<Arc<SelectHandler>>,
    distinct: Option<Arc<DistinctHandler>>,
    count: Option<Arc<CountHandler>>,
    group: Option<Arc<GroupHandler>>,
    incremental_group: Option<Arc<IncrementalGroupHandler>>,
    pivot: Option<Arc<PivotHandler>>,
    insert: Option<Arc<InsertHandler>>,
    custom: Option<Arc<CustomHandler>>,
}

impl QueryRuntimeBuilder {
    /// Creates an empty runtime builder.
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            predicate: None,
            lookup_predicate: None,
            set: None,
            lookup: None,
            streaming_load: None,
            load: None,
            compare: None,
            select: None,
            distinct: None,
            count: None,
            group: None,
            incremental_group: None,
            pivot: None,
            insert: None,
            custom: None,
        }
    }

    /// Installs the predicate evaluator.
    #[must_use]
    pub fn predicate<P>(mut self, predicate: P) -> Self
    where
        P: Fn(&Expression, &Document) -> ExecutionResult<bool> + Send + Sync + 'static,
    {
        self.predicate = Some(Arc::new(predicate));
        self
    }

    /// Installs the `set` mutation handler.
    #[must_use]
    pub fn set<S>(mut self, set: S) -> Self
    where
        S: Fn(&[SetAssignment], &Document) -> ExecutionResult<Arc<Document>>
            + Send
            + Sync
            + 'static,
    {
        self.set = Some(Arc::new(set));
        self
    }

    /// Installs the lookup-aware predicate evaluator.
    #[must_use]
    pub fn lookup_predicate<P>(mut self, predicate: P) -> Self
    where
        P: Fn(&Expression, &Document, Option<&str>, &Document) -> ExecutionResult<bool>
            + Send
            + Sync
            + 'static,
    {
        self.lookup_predicate = Some(Arc::new(predicate));
        self
    }

    /// Installs the lookup result assembler.
    #[must_use]
    pub fn lookup<L>(mut self, lookup: L) -> Self
    where
        L: Fn(&str, &Document, &LookupDocuments) -> ExecutionResult<Arc<Document>>
            + Send
            + Sync
            + 'static,
    {
        self.lookup = Some(Arc::new(lookup));
        self
    }

    /// Installs the streaming-load preparation handler.
    #[must_use]
    pub fn streaming_load<L>(mut self, load: L) -> Self
    where
        L: Fn(
                &CollectionId,
                &dyn StorageRead,
                PhysicalLoadMode,
                &[Arc<str>],
            ) -> ExecutionResult<Vec<StreamingLoadMutation>>
            + Send
            + Sync
            + 'static,
    {
        self.streaming_load = Some(Arc::new(load));
        self
    }

    /// Installs the load handler.
    #[must_use]
    pub fn load<L>(mut self, load: L) -> Self
    where
        L: Fn(&str, &Document) -> ExecutionResult<Arc<Document>> + Send + Sync + 'static,
    {
        self.load = Some(Arc::new(load));
        self
    }

    /// Installs the sort-comparison handler.
    #[must_use]
    pub fn compare<C>(mut self, compare: C) -> Self
    where
        C: Fn(&[SortKey], &Document, &Document) -> ExecutionResult<Ordering>
            + Send
            + Sync
            + 'static,
    {
        self.compare = Some(Arc::new(compare));
        self
    }

    /// Installs the projection handler.
    #[must_use]
    pub fn select<S>(mut self, select: S) -> Self
    where
        S: Fn(&[ExpressionFieldPath], &Document) -> ExecutionResult<Arc<Document>>
            + Send
            + Sync
            + 'static,
    {
        self.select = Some(Arc::new(select));
        self
    }

    /// Installs the distinct-key handler.
    #[must_use]
    pub fn distinct<D>(mut self, distinct: D) -> Self
    where
        D: Fn(&[ExpressionFieldPath], &Document) -> ExecutionResult<Arc<[u8]>>
            + Send
            + Sync
            + 'static,
    {
        self.distinct = Some(Arc::new(distinct));
        self
    }

    /// Installs the count-result handler.
    #[must_use]
    pub fn count<C>(mut self, count: C) -> Self
    where
        C: Fn(&str, u64) -> ExecutionResult<Arc<Document>> + Send + Sync + 'static,
    {
        self.count = Some(Arc::new(count));
        self
    }

    /// Installs the group handler.
    #[must_use]
    pub fn group<G>(mut self, group: G) -> Self
    where
        G: Fn(&[ExpressionFieldPath], &[Arc<Document>]) -> ExecutionResult<Vec<SyntheticDocument>>
            + Send
            + Sync
            + 'static,
    {
        self.group = Some(Arc::new(group));
        self
    }

    /// Installs the typed insert-materialization handler.
    #[must_use]
    pub fn insert<I>(mut self, insert: I) -> Self
    where
        I: Fn(&LogicalInsertDocument) -> ExecutionResult<PreparedInsertDocument>
            + Send
            + Sync
            + 'static,
    {
        self.insert = Some(Arc::new(insert));
        self
    }

    /// Installs the pivot handler.
    #[must_use]
    pub fn pivot<P>(mut self, pivot: P) -> Self
    where
        P: Fn(&PivotSpecification, &[Arc<Document>]) -> ExecutionResult<Vec<SyntheticDocument>>
            + Send
            + Sync
            + 'static,
    {
        self.pivot = Some(Arc::new(pivot));
        self
    }

    /// Installs the custom-stage handler.
    #[must_use]
    pub fn custom<C>(mut self, custom: C) -> Self
    where
        C: Fn(&StageName, &str, bool, &Document) -> ExecutionResult<CustomOperatorResult>
            + Send
            + Sync
            + 'static,
    {
        self.custom = Some(Arc::new(custom));
        self
    }

    /// Builds and validates the runtime.
    pub fn build(self) -> Result<QueryRuntime, QueryRuntimeBuildError> {
        let predicate = self
            .predicate
            .ok_or(QueryRuntimeBuildError::MissingPredicateHandler)?;
        let set = self.set.ok_or(QueryRuntimeBuildError::MissingSetHandler)?;

        Ok(QueryRuntime {
            predicate,
            resolved_predicate: None,
            lookup_predicate: self.lookup_predicate,
            set,
            lookup: self.lookup,
            streaming_load: self.streaming_load,
            load: self.load,
            compare: self.compare,
            select: self.select,
            distinct: self.distinct,
            count: self.count,
            group: self.group,
            incremental_group: self.incremental_group,
            pivot: self.pivot,
            insert: self.insert,
            custom: self.custom,
        })
    }
}

impl fmt::Debug for QueryRuntimeBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryRuntimeBuilder")
            .field("predicate", &self.predicate.is_some())
            .field("lookup_predicate", &self.lookup_predicate.is_some())
            .field("set", &self.set.is_some())
            .field("lookup", &self.lookup.is_some())
            .field("streaming_load", &self.streaming_load.is_some())
            .field("load", &self.load.is_some())
            .field("sort", &self.compare.is_some())
            .field("select", &self.select.is_some())
            .field("distinct", &self.distinct.is_some())
            .field("count", &self.count.is_some())
            .field("group", &self.group.is_some())
            .field("pivot", &self.pivot.is_some())
            .field("insert", &self.insert.is_some())
            .field("custom", &self.custom.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageEngine;

    #[test]
    fn unsupported_runtime_has_no_optional_handlers() {
        let runtime = QueryRuntime::unsupported();

        assert!(!runtime.supports_lookup_predicate());
        assert!(!runtime.supports_lookup());
        assert!(!runtime.supports_streaming_load());
        assert!(!runtime.supports_load());
        assert!(!runtime.supports_sort());
        assert!(!runtime.supports_select());
        assert!(!runtime.supports_distinct());
        assert!(!runtime.supports_count());
        assert!(!runtime.supports_group());
        assert!(!runtime.supports_pivot());
        assert!(!runtime.supports_insert());
        assert!(!runtime.supports_custom());
    }

    #[test]
    fn builder_requires_predicate_handler_first() {
        let error = QueryRuntimeBuilder::new().build().unwrap_err();

        assert_eq!(error, QueryRuntimeBuildError::MissingPredicateHandler);
    }

    #[test]
    fn builder_requires_set_handler() {
        let error = QueryRuntimeBuilder::new()
            .predicate(|_, _| Ok(true))
            .build()
            .unwrap_err();

        assert_eq!(error, QueryRuntimeBuildError::MissingSetHandler);
    }

    #[test]
    fn builder_accepts_required_handlers() {
        let runtime = QueryRuntimeBuilder::new()
            .predicate(|_, _| Ok(true))
            .set(|_, document| Ok(Arc::new(document.clone())))
            .build()
            .unwrap();

        assert!(!runtime.supports_lookup_predicate());
        assert!(!runtime.supports_lookup());
        assert!(!runtime.supports_streaming_load());
        assert!(!runtime.supports_load());
        assert!(!runtime.supports_sort());
        assert!(!runtime.supports_select());
        assert!(!runtime.supports_distinct());
        assert!(!runtime.supports_count());
        assert!(!runtime.supports_group());
        assert!(!runtime.supports_pivot());
        assert!(!runtime.supports_insert());
        assert!(!runtime.supports_custom());
    }

    #[test]
    fn optional_handlers_are_reported() {
        let runtime = QueryRuntimeBuilder::new()
            .predicate(|_, _| Ok(true))
            .set(|_, document| Ok(Arc::new(document.clone())))
            .lookup_predicate(|_, _, _, _| Ok(true))
            .lookup(|_, document, _| Ok(Arc::new(document.clone())))
            .streaming_load(|_, _, _, _| Ok(Vec::new()))
            .load(|_, document| Ok(Arc::new(document.clone())))
            .compare(|_, _, _| Ok(Ordering::Equal))
            .select(|_, document| Ok(Arc::new(document.clone())))
            .distinct(|_, _| Ok(Arc::<[u8]>::from([])))
            .count(|_, _| Err(ExecutionError::evaluation("count test")))
            .group(|_, _| Ok(Vec::new()))
            .pivot(|_, _| Ok(Vec::new()))
            .insert(|_| Err(ExecutionError::mutation("insert test")))
            .custom(|_, _, _, _| Ok(CustomOperatorResult::Keep))
            .build()
            .unwrap();

        assert!(runtime.supports_lookup_predicate());
        assert!(runtime.supports_lookup());
        assert!(runtime.supports_streaming_load());
        assert!(runtime.supports_load());
        assert!(runtime.supports_sort());
        assert!(runtime.supports_select());
        assert!(runtime.supports_distinct());
        assert!(runtime.supports_count());
        assert!(runtime.supports_group());
        assert!(runtime.supports_pivot());
        assert!(runtime.supports_insert());
        assert!(runtime.supports_custom());
    }

    #[test]
    fn fluent_runtime_configuration_reports_handlers() {
        let runtime = QueryRuntime::new(
            |_, _| Ok(true),
            |_, document| Ok(Arc::new(document.clone())),
        )
        .with_lookup_predicate(|_, _, _, _| Ok(true))
        .with_lookup(|_, document, _| Ok(Arc::new(document.clone())))
        .with_streaming_load(|_, _, _, _| Ok(Vec::new()))
        .with_load(|_, document| Ok(Arc::new(document.clone())))
        .with_compare(|_, _, _| Ok(Ordering::Equal))
        .with_select(|_, document| Ok(Arc::new(document.clone())))
        .with_distinct(|_, _| Ok(Arc::<[u8]>::from([])))
        .with_count(|_, _| Err(ExecutionError::evaluation("count test")))
        .with_group(|_, _| Ok(Vec::new()))
        .with_pivot(|_, _| Ok(Vec::new()))
        .with_insert(|_| Err(ExecutionError::mutation("insert test")))
        .with_custom(|_, _, _, _| Ok(CustomOperatorResult::Keep));

        assert!(runtime.supports_lookup_predicate());
        assert!(runtime.supports_lookup());
        assert!(runtime.supports_streaming_load());
        assert!(runtime.supports_load());
        assert!(runtime.supports_sort());
        assert!(runtime.supports_select());
        assert!(runtime.supports_distinct());
        assert!(runtime.supports_count());
        assert!(runtime.supports_group());
        assert!(runtime.supports_pivot());
        assert!(runtime.supports_insert());
        assert!(runtime.supports_custom());
    }

    #[test]
    fn lookup_predicate_falls_back_to_regular_predicate() {
        let runtime = QueryRuntime::new(
            |_, _| Ok(true),
            |_, document| Ok(Arc::new(document.clone())),
        );

        assert!(!runtime.supports_lookup_predicate());
    }

    #[test]
    fn absent_lookup_handler_returns_structured_error() {
        let runtime = QueryRuntime::unsupported();

        assert!(!runtime.supports_lookup());
    }

    #[test]
    fn absent_streaming_load_handler_returns_structured_error() {
        let runtime = QueryRuntime::unsupported();
        let chunks = [Arc::<str>::from("batch")];
        let storage = crate::storage::MemoryStorage::new();
        let read = storage.read().unwrap();
        let collection = crate::storage::CollectionId::parse("data").unwrap();

        let error = runtime
            .prepare_streaming_load(
                &collection,
                read.as_ref(),
                PhysicalLoadMode::Replace,
                &chunks,
            )
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            super::super::ExecutionErrorKind::UnsupportedOperator { .. }
        ));
    }

    #[test]
    fn absent_pivot_handler_returns_structured_error() {
        use crate::query::logical_plan::{PivotAggregate, PivotSpecification, PivotValue};

        let runtime = QueryRuntime::unsupported();
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

        let error = runtime.pivot_documents(&specification, &[]).unwrap_err();

        assert!(matches!(
            error.kind(),
            super::super::ExecutionErrorKind::UnsupportedOperator { .. }
        ));
    }

    #[test]
    fn absent_optional_handler_returns_structured_error() {
        let runtime = QueryRuntime::unsupported();
        let error = runtime.count_document("total", 0).unwrap_err();

        assert!(matches!(
            error.kind(),
            super::super::ExecutionErrorKind::UnsupportedOperator { .. }
        ));
    }

    #[test]
    fn runtime_public_types_are_send_and_sync() {
        fn assert_send_and_sync<T: Send + Sync>() {}

        assert_send_and_sync::<QueryRuntime>();
        assert_send_and_sync::<QueryRuntimeBuilder>();
        assert_send_and_sync::<QueryRuntimeBuildError>();
    }
}
