#![cfg_attr(rustfmt, rustfmt_skip)]
//! Default document operator materialization.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::{
    helpers::json_to_value,
    model::Document,
    model::Number,
    model::Value,
    storage::{
        CollectionId, DocumentId, ProjectedValueRef, StorageRead, UuidV7Generator,
        VersionPrecondition,
    },
};

use super::{
    apply_assignments, evaluate_predicate, executor::{compare_owned_projected_rows, compare_sort_values, PreparedInsertDocument}, logical_plan::InsertDocument,
    parse_expression, projected_values::SlotRow, CustomOperatorResult, DocumentFields, ExecutionError, ExecutionResult,
    ExecutionRuntime, Expression, ExpressionFieldPath, ExpressionFieldResolver, IncrementalGroupAccumulator, IncrementalPivotAccumulator,
    InsertDocumentMaterializer, LookupDocuments, LookupFields, PhysicalLoadMode, PivotAggregate,
    PivotSpecification, SetAssignment, SortKey, StageName, StreamingLoadMutation,
    SyntheticDocument,
};

/// Parsed native `near` stage shared by the document fallback and projected Top-N path.
#[derive(Clone, Debug)]
pub struct NearSpec {
    field: ExpressionFieldPath,
    query: Arc<[f64]>,
    query_norm: f64,
}

impl NearSpec {
    pub(crate) fn parse(arguments: &str) -> ExecutionResult<Self> {
        let (field, vector) = arguments
            .split_once(',')
            .ok_or_else(|| ExecutionError::evaluation("near expects <field>, <vector>"))?;
        let field = ExpressionFieldPath::new(field.trim().split('.'))
            .map_err(|error| ExecutionError::evaluation(error.to_string()))?;
        let query = serde_json::from_str::<Vec<f64>>(vector.trim())
            .map_err(|error| ExecutionError::evaluation(format!("invalid near vector: {error}")))?;
        if query.is_empty() {
            return Err(ExecutionError::evaluation("near vector must not be empty"));
        }
        let query_norm = query.iter().map(|value| *value * *value).sum::<f64>();
        if !query_norm.is_finite() {
            return Err(ExecutionError::evaluation(
                "near vector norm must be finite",
            ));
        }
        Ok(Self {
            field,
            query: Arc::from(query),
            query_norm,
        })
    }

    #[must_use]
    pub(crate) const fn field(&self) -> &ExpressionFieldPath {
        &self.field
    }

    pub(crate) fn distance_value(&self, value: &Value) -> ExecutionResult<f64> {
        let values = value.as_array().ok_or_else(|| {
            ExecutionError::evaluation(format!(
                "near field {} must be an array of numbers",
                self.field
            ))
        })?;
        self.distance_values(values)
    }

    pub(crate) fn distance_projected(&self, value: &ProjectedValueRef<'_>) -> ExecutionResult<f64> {
        match value {
            ProjectedValueRef::Owned(value) => self.distance_value(value),
            _ => Err(ExecutionError::evaluation(format!(
                "near field {} must be an array of numbers",
                self.field
            ))),
        }
    }

    fn distance_values(&self, values: &[Value]) -> ExecutionResult<f64> {
        if values.len() != self.query.len() {
            return Err(ExecutionError::evaluation(format!(
                "near dimension mismatch for {}: document has {}, query has {}",
                self.field,
                values.len(),
                self.query.len()
            )));
        }

        let mut dot = 0.0_f64;
        let mut document_norm = 0.0_f64;
        for (value, query_value) in values.iter().zip(self.query.iter()) {
            let number = value.as_number().ok_or_else(|| {
                ExecutionError::evaluation(format!(
                    "near field {} must contain only numbers",
                    self.field
                ))
            })?;
            let value = match *number {
                Number::Signed(value) => value as f64,
                Number::Unsigned(value) => value as f64,
                Number::Float(value) => value,
            };
            dot = value.mul_add(*query_value, dot);
            document_norm = value.mul_add(value, document_norm);
        }
        if document_norm == 0.0 || self.query_norm == 0.0 {
            return Err(ExecutionError::evaluation(
                "near cosine distance requires non-zero vectors",
            ));
        }
        if !dot.is_finite() || !document_norm.is_finite() {
            return Err(ExecutionError::evaluation(
                "near cosine distance overflowed",
            ));
        }

        let similarity = (dot / (document_norm.sqrt() * self.query_norm.sqrt())).clamp(-1.0, 1.0);
        let distance = 1.0 - similarity;
        if !distance.is_finite() {
            return Err(ExecutionError::evaluation(
                "near cosine distance is not finite",
            ));
        }
        Ok(distance)
    }
}

/// Materializes lookup arrays and pivot result documents for the default daemon.
#[derive(Clone, Copy, Debug, Default)]
pub struct RuntimeMaterializer;

/// The standard runtime: `where` and `set` evaluate expressions over the
/// row's fields, and every other stage materializes documents.
#[derive(Clone, Debug, Default)]
pub struct QueryRuntime {
    materializer: RuntimeMaterializer,
    insert: InsertDocumentMaterializer,
    streaming_load: StreamingLoadMaterializer,
}

impl QueryRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl ExecutionRuntime for QueryRuntime {
    fn evaluate_predicate( &self, expression: &Expression, document: &Document, ) -> ExecutionResult<bool> {
        evaluate_predicate(expression, &DocumentFields(document))
    }

    fn evaluate_resolved_predicate( &self, expression: &Expression, resolver: &dyn ExpressionFieldResolver<Value>, ) -> ExecutionResult<bool> {
        evaluate_predicate(expression, resolver)
    }

    // The lookup scope is an ordinary field resolver: correlated predicates
    // need no dedicated evaluation.
    fn evaluate_lookup_predicate( &self, expression: &Expression, scope: &LookupFields<'_>, ) -> ExecutionResult<bool> {
        evaluate_predicate(expression, scope)
    }

    fn apply_set( &self, assignments: &[SetAssignment], document: &Document, ) -> ExecutionResult<Arc<Document>> {
        apply_assignments(assignments, document)
    }

    fn apply_lookup( &self, into: &str, outer: &Document, matches: &LookupDocuments, ) -> ExecutionResult<Arc<Document>> {
        self.materializer.materialize_lookup(into, outer, matches)
    }

    fn prepare_streaming_load( &self, collection: &CollectionId, storage: &dyn StorageRead, mode: PhysicalLoadMode, chunks: &[Arc<str>], ) -> ExecutionResult<Vec<StreamingLoadMutation>> {
        self.streaming_load.materialize(collection, storage, mode, chunks)
    }

    fn apply_load(&self, _target: &str, document: &Document) -> ExecutionResult<Arc<Document>> {
        Ok(Arc::new(document.clone()))
    }

    fn compare_documents( &self, keys: &[SortKey], left: &Document, right: &Document, ) -> ExecutionResult<Ordering> {
        self.materializer.materialize_sort_comparison(keys, left, right)
    }

    fn supports_projected_sort(&self) -> bool {
        true
    }

    fn compare_projected_values( &self, keys: &[SortKey], left: &[Option<Value>], right: &[Option<Value>], ) -> ExecutionResult<Option<Ordering>> {
        self.materializer.materialize_projected_sort_comparison(keys, left, right).map(Some)
    }

    fn apply_select( &self, fields: &[ExpressionFieldPath], document: &Document, ) -> ExecutionResult<Arc<Document>> {
        self.materializer.materialize_select(fields, document)
    }

    fn distinct_key( &self, fields: &[ExpressionFieldPath], document: &Document, ) -> ExecutionResult<Arc<[u8]>> {
        self.materializer.materialize_distinct_key(fields, document)
    }

    fn supports_buffered_distinct(&self) -> bool {
        true
    }

    fn write_distinct_key( &self, fields: &[ExpressionFieldPath], document: &Document, key: &mut Vec<u8>, ) -> ExecutionResult<bool> {
        self.materializer.write_distinct_key(fields, document, key).map(|()| true)
    }

    fn supports_projected_distinct(&self) -> bool {
        true
    }

    fn write_projected_distinct_key( &self, fields: &[ExpressionFieldPath], values: &[Option<ProjectedValueRef<'_>>], indexes: &[usize], key: &mut Vec<u8>, ) -> ExecutionResult<bool> {
        self.materializer.write_projected_ref_distinct_key(fields, values, indexes, key).map(|()| true)
    }

    fn write_projected_document_distinct_key( &self, fields: &[ExpressionFieldPath], values: &[Option<ProjectedValueRef<'_>>], indexes: &[usize], key: &mut Vec<u8>, ) -> ExecutionResult<bool> {
        self.materializer.write_projected_ref_document_key(fields, values, indexes, key).map(|()| true)
    }

    fn count_document(&self, alias: &str, count: u64) -> ExecutionResult<Arc<Document>> {
        self.materializer.materialize_count(alias, count)
    }

    fn incremental_group_accumulator( &self, keys: &[ExpressionFieldPath], ) -> ExecutionResult<Option<Box<dyn IncrementalGroupAccumulator>>> {
        self.materializer.incremental_group(keys).map(Some)
    }

    fn group_documents( &self, keys: &[ExpressionFieldPath], documents: &[Arc<Document>], ) -> ExecutionResult<Vec<SyntheticDocument>> {
        self.materializer.materialize_group(keys, documents)
    }

    fn prepare_insert( &self, document: &InsertDocument, ) -> ExecutionResult<PreparedInsertDocument> {
        self.insert.materialize(document)
    }

    fn incremental_pivot_accumulator( &self, specification: &PivotSpecification, ) -> ExecutionResult<Option<Box<dyn IncrementalPivotAccumulator>>> {
        Ok(Some(Box::new(self.materializer.incremental_pivot(specification))))
    }

    fn pivot_documents( &self, specification: &PivotSpecification, documents: &[Arc<Document>], ) -> ExecutionResult<Vec<SyntheticDocument>> {
        self.materializer.materialize_pivot(specification, documents)
    }

    fn apply_custom( &self, stage: &StageName, arguments: &str, _writes: bool, document: &Document, ) -> ExecutionResult<CustomOperatorResult> {
        self.materializer.materialize_custom(stage.as_str(), arguments, document)
    }
}

#[derive(Clone, Debug, Default)]
struct StreamingLoadMaterializer {
    id_generator: UuidV7Generator,
}

impl StreamingLoadMaterializer {
    fn materialize( &self, collection: &CollectionId, storage: &dyn StorageRead, mode: PhysicalLoadMode, chunks: &[Arc<str>], ) -> ExecutionResult<Vec<StreamingLoadMutation>> {
        let mut mutations = Vec::new();
        let mut seen = BTreeSet::new();

        for chunk in chunks {
            let values: serde_json::Value = serde_json::from_str(chunk).map_err(|error| {
                ExecutionError::mutation(format!("invalid streaming-load chunk JSON: {error}"))
            })?;
            let rows = values.as_array().ok_or_else(|| {
                ExecutionError::mutation("streaming-load chunk must be a JSON array")
            })?;

            mutations.reserve(rows.len());
            let generated_count = rows
                .iter()
                .filter(|row| {
                    row.as_object()
                        .is_some_and(|object| !object.contains_key("_id"))
                })
                .count();
            let mut generated_ids = if generated_count == 0 {
                None
            } else {
                Some(self.id_generator.reserve(generated_count))
            };
            for row in rows {
                let object = row.as_object().ok_or_else(|| {
                    ExecutionError::mutation("streaming-load rows must be JSON objects")
                })?;
                let generated_id = if object.contains_key("_id") {
                    None
                } else {
                    generated_ids.as_mut().and_then(Iterator::next)
                };
                let generated = generated_id.is_some();
                let (id, incoming) = self.materialize_row(object, generated_id)?;

                // IDs reserved by this materializer are fresh by construction. Avoid the
                // duplicate-set insertion and storage existence lookup on the hot path for
                // ordinary imports that do not provide `_id` values.
                if generated {
                    if mode == PhysicalLoadMode::Update {
                        return Err(ExecutionError::mutation(format!(
                            "streaming-load update target {:?} does not exist",
                            id.to_string()
                        )));
                    }
                    mutations.push(StreamingLoadMutation::insert(id, Arc::new(incoming)));
                    continue;
                }

                if !seen.insert(id) {
                    return Err(ExecutionError::mutation(format!(
                        "duplicate streaming-load identifier {:?} in one request",
                        id.to_string()
                    )));
                }

                let existing = storage
                    .get(collection, &id)
                    .map_err(ExecutionError::storage)?;

                let mutation = match (mode, existing) {
                    (PhysicalLoadMode::Replace, Some(stored)) => StreamingLoadMutation::replace(
                        id,
                        Arc::new(incoming),
                        VersionPrecondition::Exact(stored.version()),
                    ),
                    (PhysicalLoadMode::Replace, None) => {
                        StreamingLoadMutation::insert(id, Arc::new(incoming))
                    }
                    (PhysicalLoadMode::Update | PhysicalLoadMode::Merge, Some(stored)) => {
                        let mut merged = stored.document().clone();
                        for (name, value) in &incoming {
                            merged.insert(name.clone(), value.clone());
                        }
                        StreamingLoadMutation::replace(
                            id,
                            Arc::new(merged),
                            VersionPrecondition::Exact(stored.version()),
                        )
                    }
                    (PhysicalLoadMode::Merge, None) => {
                        StreamingLoadMutation::insert(id, Arc::new(incoming))
                    }
                    (PhysicalLoadMode::Update, None) => {
                        return Err(ExecutionError::mutation(format!(
                            "streaming-load update target {:?} does not exist",
                            id.to_string()
                        )));
                    }
                };
                mutations.push(mutation);
            }
        }

        Ok(mutations)
    }

    fn materialize_row( &self, object: &serde_json::Map<String, serde_json::Value>, generated_id: Option<DocumentId>, ) -> ExecutionResult<(DocumentId, Document)> {
        let id = match object.get("_id") {
            Some(serde_json::Value::String(value)) => {
                DocumentId::parse(value).map_err(|error| {
                    ExecutionError::mutation(format!("invalid streaming-load _id: {error}"))
                })?
            }
            Some(_) => {
                return Err(ExecutionError::mutation(
                    "streaming-load field \"_id\" must be a string",
                ));
            }
            None => generated_id
                .ok_or_else(|| ExecutionError::mutation("missing reserved document id"))?,
        };

        let mut document = Document::new();
        for (name, value) in object {
            if name != "_id" {
                document.insert(name.as_str(), json_to_value(value).map_err(ExecutionError::mutation)?);
            }
        }
        Ok((id, document))
    }
}

impl RuntimeMaterializer {
    /// Creates the default materializer.
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self
    }

    /// Applies the standard read-only document transformation stages.
    ///
    /// The stage is compiled from its text for this one call; plans hold a
    /// [`CompiledStage`] instead, so rows never re-parse their stage.
    pub fn materialize_custom( &self, name: &str, arguments: &str, document: &Document, ) -> ExecutionResult<CustomOperatorResult> {
        match CompiledStage::compile(name, arguments)? {
            Some(stage) => stage.apply(document),
            None => Ok(CustomOperatorResult::Keep),
        }
    }

    /// Compares two documents according to the ordered sort keys.
    ///
    /// Missing values sort before present values in ascending order and after
    /// present values in descending order. Present values use OG's numeric
    /// coercion policy, while incompatible physical kinds produce an explicit
    /// execution error.
    pub fn materialize_sort_comparison( &self, keys: &[SortKey], left: &Document, right: &Document, ) -> ExecutionResult<Ordering> {
        for key in keys {
            let ordering = compare_sort_values(path_value(left, key.field()), path_value(right, key.field()), key)?;
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }
        Ok(Ordering::Equal)
    }

    /// Compares values already aligned with the requested sort keys.
    ///
    /// This is semantically identical to `materialize_sort_comparison` but avoids
    /// building temporary Documents for projected blocking operators.
    pub fn materialize_projected_sort_comparison( &self, keys: &[SortKey], left: &[Option<Value>], right: &[Option<Value>], ) -> ExecutionResult<Ordering> {
        if left.len() != keys.len() || right.len() != keys.len() {
            return Err(ExecutionError::evaluation(
                "projected sort values do not match the sort-key layout",
            ));
        }
        compare_owned_projected_rows(keys, left, right)
    }

    /// Projects a document to the requested field paths.
    pub fn materialize_select( &self, fields: &[ExpressionFieldPath], document: &Document, ) -> ExecutionResult<Arc<Document>> {
        let mut result = Document::new();
        for field in fields {
            if let Some(value) = cloned_path_value(document, field) {
                let path = field.iter().collect::<Vec<_>>();
                insert_path(&mut result, &path, value);
            }
        }
        Ok(Arc::new(result))
    }

    /// Builds a deterministic, collision-safe equality key for `distinct`.
    ///
    /// An empty field list means full-document distinctness. For an explicit
    /// field list, missing values remain distinct from physical `null` values.
    pub fn materialize_distinct_key( &self, fields: &[ExpressionFieldPath], document: &Document, ) -> ExecutionResult<Arc<[u8]>> {
        let mut key = Vec::new();
        self.write_distinct_key(fields, document, &mut key)?;
        Ok(Arc::from(key))
    }

    /// Encodes the canonical distinct key into a reusable caller-owned buffer.
    /// This is byte-for-byte identical to `materialize_distinct_key`.
    pub fn write_distinct_key( &self, fields: &[ExpressionFieldPath], document: &Document, key: &mut Vec<u8>, ) -> ExecutionResult<()> {
        key.clear();
        if fields.is_empty() {
            key.push(0);
            encode_document(key, document);
        } else {
            key.push(1);
            encode_len(key, fields.len());
            for field in fields {
                match path_value(document, field) {
                    Some(value) => {
                        key.push(1);
                        encode_value(key, value);
                    }
                    None => key.push(0),
                }
            }
        }
        Ok(())
    }

    /// Encodes an explicit-field equality key directly from storage-borrowed
    /// projected values. Missing values remain distinct from physical nulls.
    pub fn write_projected_ref_distinct_key( &self, fields: &[ExpressionFieldPath], values: &[Option<crate::storage::ProjectedValueRef<'_>>], indexes: &[usize], key: &mut Vec<u8>, ) -> ExecutionResult<()> {
        if fields.is_empty() || fields.len() != indexes.len() {
            return Err(ExecutionError::evaluation(
                "projected distinct requires an explicit field layout",
            ));
        }
        if indexes.iter().any(|index| *index >= values.len()) {
            return Err(ExecutionError::evaluation(
                "projected distinct slots do not match the source layout",
            ));
        }

        self.write_projected_ref_distinct_key_indexes(values, indexes, key);
        Ok(())
    }

    /// Encodes the full-document equality key of the document holding the
    /// present values, byte-for-byte as [`Self::write_distinct_key`] does for
    /// it. `fields` are every top-level field in name order (the document's
    /// own order) and `indexes` maps them into `values`.
    pub fn write_projected_ref_document_key( &self, fields: &[ExpressionFieldPath], values: &[Option<crate::storage::ProjectedValueRef<'_>>], indexes: &[usize], key: &mut Vec<u8>, ) -> ExecutionResult<()> {
        if fields.len() != indexes.len() || indexes.iter().any(|index| *index >= values.len()) {
            return Err(ExecutionError::evaluation(
                "projected document distinct slots do not match the source layout",
            ));
        }
        key.clear();
        key.push(0);
        encode_len(key, indexes.iter().filter(|index| values[**index].is_some()).count());
        for (field, index) in fields.iter().zip(indexes) {
            if let Some(value) = &values[*index] {
                encode_bytes(key, field.first().as_bytes());
                encode_projected_ref(key, value);
            }
        }
        Ok(())
    }

    /// Encodes the explicit-field equality key directly into a reusable
    /// caller-owned buffer. This avoids one key allocation per source row.
    pub(crate) fn write_projected_distinct_key_indexes( &self, values: &[Option<Value>], indexes: &[usize], key: &mut Vec<u8>, ) {
        key.clear();
        key.push(1);
        encode_len(key, indexes.len());
        for index in indexes {
            match values.get(*index).and_then(Option::as_ref) {
                Some(value) => {
                    key.push(1);
                    encode_value(key, value);
                }
                None => key.push(0),
            }
        }
    }

    /// Encodes an equality key from storage-borrowed scalar values without
    /// materializing owned runtime strings for every source row.
    pub(crate) fn write_projected_ref_distinct_key_indexes( &self, values: &[Option<crate::storage::ProjectedValueRef<'_>>], indexes: &[usize], key: &mut Vec<u8>, ) {
        key.clear();
        key.push(1);
        encode_len(key, indexes.len());
        for index in indexes {
            match values.get(*index).and_then(Option::as_ref) {
                Some(value) => {
                    key.push(1);
                    encode_projected_ref(key, value);
                }
                None => key.push(0),
            }
        }
    }

    /// Creates the single result document emitted by `count`.
    pub fn materialize_count(&self, alias: &str, count: u64) -> ExecutionResult<Arc<Document>> {
        let mut result = Document::new();
        result.insert(alias, Value::unsigned(count));
        Ok(Arc::new(result))
    }

    /// Creates the bounded aggregate state used by the external group
    /// executor. Measures are folded immediately; no source document is retained.
    pub fn incremental_group( &self, fields: &[ExpressionFieldPath], ) -> ExecutionResult<Box<dyn IncrementalGroupAccumulator>> {
        Ok(Box::new(DefaultIncrementalGroupAccumulator::new(fields)?))
    }

    /// Groups documents by key paths and optional encoded measures.
    ///
    /// Plain paths are grouping keys. Internal marker paths generated by the
    /// planner are measures (`sum`, `avg`, `min`, `max`, `first`, `last`,
    /// `count`). A row whose measures produce no value carries `count` instead.
    pub fn materialize_group( &self, fields: &[ExpressionFieldPath], documents: &[Arc<Document>], ) -> ExecutionResult<Vec<SyntheticDocument>> {
        let template = DefaultIncrementalGroupAccumulator::new(fields)?;
        let mut groups: BTreeMap<Vec<u8>, DefaultIncrementalGroupAccumulator> = BTreeMap::new();

        for document in documents {
            let mut encoded = Vec::new();
            encode_len(&mut encoded, template.keys.len());
            for key in template.keys.iter() {
                match path_value(document, &key.source) {
                    Some(value) => {
                        encoded.push(1);
                        encode_value(&mut encoded, value);
                    }
                    None => encoded.push(0),
                }
            }
            groups
                .entry(encoded)
                .or_insert_with(|| template.clone())
                .push(document)?;
        }

        groups
            .into_values()
            .enumerate()
            .map(|(index, group)| Box::new(group).finish(index as u64 + 1))
            .collect()
    }

    /// Implements lookup's left-join result contract.
    ///
    /// The outer document is always retained. Matching inner documents are
    /// stored as an array of objects in `into`; no match therefore produces an
    /// empty array rather than dropping the outer row.
    pub fn materialize_lookup( &self, into: &str, outer: &Document, matches: &LookupDocuments, ) -> ExecutionResult<Arc<Document>> {
        let mut result = outer.clone();
        let values = matches
            .documents()
            .iter()
            .map(|document| Value::object((**document).clone()))
            .collect::<Vec<_>>();

        result.insert(into, Value::array(values));
        Ok(Arc::new(result))
    }

    /// Materializes a pivot result from a complete document set.
    pub fn materialize_pivot( &self, specification: &PivotSpecification, documents: &[Arc<Document>], ) -> ExecutionResult<Vec<SyntheticDocument>> {
        let mut accumulator = self.incremental_pivot(specification);
        for document in documents {
            accumulator.push(document)?;
        }
        Box::new(accumulator).finish()
    }

    /// Starts an incremental pivot that retains only its aggregate cells.
    #[must_use]
    pub fn incremental_pivot(&self, specification: &PivotSpecification) -> DefaultIncrementalPivotAccumulator {
        DefaultIncrementalPivotAccumulator {
            specification: specification.clone(),
            groups: Vec::new(),
            by_key: std::collections::HashMap::new(),
            by_values: std::collections::HashMap::new(),
            values_key: Vec::new(),
            column: String::new(),
            column_key: Vec::new(),
            by_column: std::collections::HashMap::new(),
            by_label: std::collections::HashMap::new(),
            labels: Vec::new(),
            last_group: None,
            last_column: None,
            bytes: 0,
        }
    }
}

/// Pivot state: one aggregate cell per (row key, column) pair.
///
/// Rows are ordered by their stable key, computed once per new row values;
/// every row finds its group by the binary encoding of those values, and its
/// column by the encoding of its column values. A column's label (and so
/// its cell names) is written once, when the column is first seen; columns
/// sharing a label share their cells. Cells are indexed by column and
/// measure and named only when the result is built.
#[derive(Debug)]
pub struct DefaultIncrementalPivotAccumulator {
    specification: PivotSpecification,
    groups: Vec<(String, PivotGroup)>,
    by_key: std::collections::HashMap<String, usize>,
    by_values: std::collections::HashMap<Box<[u8]>, usize>,
    values_key: Vec<u8>,
    column: String,
    column_key: Vec<u8>,
    by_column: std::collections::HashMap<Box<[u8]>, usize>,
    by_label: std::collections::HashMap<String, usize>,
    labels: Vec<String>,
    /// Encoded values and index of the previous row's group and column.
    last_group: Option<(Vec<u8>, usize)>,
    last_column: Option<(Vec<u8>, usize)>,
    bytes: usize,
}

impl IncrementalPivotAccumulator for DefaultIncrementalPivotAccumulator {
    fn push(&mut self, document: &Document) -> ExecutionResult<()> {
        self.push_with(|path| path_value(document, path))
    }

    fn folds_slots(&self) -> bool {
        true
    }

    fn push_slots(&mut self, fields: &[ExpressionFieldPath], values: &[Option<Value>]) -> ExecutionResult<()> {
        self.push_with(|path| {
            let mut segments = path.iter();
            let first = segments.next()?;
            let slot = fields.iter().position(|field| field.len() == 1 && field.first() == first)?;
            let mut value = values[slot].as_ref()?;
            for segment in segments {
                value = value.as_object()?.get(segment)?;
            }
            Some(value)
        })
    }

    fn folds_refs(&self) -> bool {
        true
    }

    fn push_refs( &mut self, rows: &[Option<ProjectedValueRef<'_>>], columns: &[Option<ProjectedValueRef<'_>>], values: &[Option<ProjectedValueRef<'_>>], ) -> ExecutionResult<()> {
        let group = self.group_of(rows);
        let column = self.column_of(columns);
        let measures = self.specification.values().len();
        let label = self.labels[column].len();
        let cells = &mut self.groups[group].1.cells;
        if cells.len() < (column + 1) * measures {
            cells.resize_with((column + 1) * measures, || None);
        }
        for (index, (measure, value)) in self.specification.values().iter().zip(values).enumerate() {
            let cell = match &mut cells[column * measures + index] {
                Some(cell) => cell,
                slot => {
                    let name = if measures == 1 { label } else { label + 1 + measure.output_name().len() };
                    self.bytes = self.bytes.saturating_add(PIVOT_ENTRY_BYTES + name * 2);
                    slot.insert(MeasureState::new(measure.aggregate()))
                }
            };
            let before = cell.retained_bytes();
            // Pivot cells skip missing values, except `count`, which counts rows.
            if measure.aggregate() == PivotAggregate::Count {
                cell.push(Some(&Value::Bool(true)))?;
            } else if value.is_some() {
                cell.push_ref(value.as_ref())?;
            }
            self.bytes = self.bytes.saturating_add(cell.retained_bytes()).saturating_sub(before);
        }
        Ok(())
    }

    fn retained_bytes(&self) -> usize {
        self.bytes
    }

    fn finish(self: Box<Self>) -> ExecutionResult<Vec<SyntheticDocument>> {
        let Self { specification, mut groups, labels, .. } = *self;
        groups.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let measures = specification.values().len();
        groups
            .into_iter()
            .enumerate()
            .map(|(index, (_, group))| {
                let mut cells = BTreeMap::new();
                for (position, cell) in group.cells.into_iter().enumerate() {
                    let Some(cell) = cell else { continue };
                    let (column, measure) = (position / measures, position % measures);
                    let mut name = labels[column].clone();
                    if measures != 1 {
                        name.push('.');
                        name.push_str(specification.values()[measure].output_name());
                    }
                    cells.insert(name, cell);
                }
                build_pivot_document(index, &specification, group.row_values, cells)
            })
            .collect()
    }
}

impl DefaultIncrementalPivotAccumulator {
    /// Folds one row whose fields `get` resolves.
    fn push_with<'a>(&mut self, get: impl Fn(&ExpressionFieldPath) -> Option<&'a Value>) -> ExecutionResult<()> {
        let refs = |paths: &[ExpressionFieldPath]| paths.iter().map(|path| get(path).map(ProjectedValueRef::borrowed)).collect::<Vec<_>>();
        let (rows, columns) = (refs(self.specification.rows()), refs(self.specification.columns()));
        let values = self.specification.values().iter().map(|value| get(value.field()).map(ProjectedValueRef::borrowed)).collect::<Vec<_>>();
        self.push_refs(&rows, &columns, &values)
    }

    /// Index of the group holding row values `rows`, created when new.
    fn group_of(&mut self, rows: &[Option<ProjectedValueRef<'_>>]) -> usize {
        encode_optional_refs(&mut self.values_key, rows);
        if let Some((key, index)) = &self.last_group {
            if *key == self.values_key {
                return *index;
            }
        }
        let index = match self.by_values.get(self.values_key.as_slice()) {
            Some(index) => *index,
            None => {
                let row_values = rows.iter().map(|value| value.as_ref().map(ProjectedValueRef::to_value)).collect::<Vec<_>>();
                let row_key = stable_values_key(&row_values);
                let index = match self.by_key.get(&row_key) {
                    Some(index) => *index,
                    None => {
                        self.bytes = self.bytes.saturating_add(
                            PIVOT_ENTRY_BYTES + row_key.len() * 2 + row_values.iter().map(optional_value_bytes).sum::<usize>(),
                        );
                        self.by_key.insert(row_key.clone(), self.groups.len());
                        self.groups.push((row_key, PivotGroup { row_values, cells: Vec::new() }));
                        self.groups.len() - 1
                    }
                };
                self.bytes = self.bytes.saturating_add(PIVOT_ENTRY_BYTES + self.values_key.len());
                self.by_values.insert(Box::from(self.values_key.as_slice()), index);
                index
            }
        };
        let last = self.last_group.get_or_insert_with(|| (Vec::new(), index));
        last.0.clear();
        last.0.extend_from_slice(&self.values_key);
        last.1 = index;
        index
    }

    /// Index of the column of values `columns`: columns sharing a label
    /// share an index.
    fn column_of(&mut self, columns: &[Option<ProjectedValueRef<'_>>]) -> usize {
        encode_optional_refs(&mut self.column_key, columns);
        if let Some((key, index)) = &self.last_column {
            if *key == self.column_key {
                return *index;
            }
        }
        let index = match self.by_column.get(self.column_key.as_slice()) {
            Some(index) => *index,
            None => {
                let values = columns.iter().map(|value| value.as_ref().map(ProjectedValueRef::to_value)).collect::<Vec<_>>();
                self.column.clear();
                write_column_label(&mut self.column, values.iter().map(Option::as_ref));
                let index = match self.by_label.get(&self.column) {
                    Some(index) => *index,
                    None => {
                        self.labels.push(self.column.clone());
                        self.by_label.insert(self.column.clone(), self.labels.len() - 1);
                        self.labels.len() - 1
                    }
                };
                self.by_column.insert(Box::from(self.column_key.as_slice()), index);
                index
            }
        };
        let last = self.last_column.get_or_insert_with(|| (Vec::new(), index));
        last.0.clear();
        last.0.extend_from_slice(&self.column_key);
        last.1 = index;
        index
    }
}

/// Encoding of optional values, as the pivot and group keys use it.
fn encode_optional_refs(output: &mut Vec<u8>, values: &[Option<ProjectedValueRef<'_>>]) {
    output.clear();
    for value in values {
        match value {
            Some(value) => {
                output.push(1);
                encode_projected_ref(output, value);
            }
            None => output.push(0),
        }
    }
}

/// Fixed overhead charged per retained map entry and aggregate state.
const PIVOT_ENTRY_BYTES: usize = 96;

fn optional_value_bytes(value: &Option<Value>) -> usize {
    value.as_ref().map_or(0, |value| match value {
        Value::String(text) => text.len(),
        Value::Number(_) | Value::Bool(_) | Value::Null => 0,
        other => format!("{other:?}").len(),
    })
}

#[derive(Debug)]
struct PivotGroup {
    row_values: Vec<Option<Value>>,
    /// Cell of each (column, measure), `None` until a row reaches it.
    cells: Vec<Option<MeasureState>>,
}

fn update_extreme( current: &mut Option<Value>, candidate: Option<Value>, minimum: bool, ) -> ExecutionResult<()> {
    let Some(candidate) = candidate else {
        return Ok(());
    };
    let replace = match current.as_ref() {
        None => true,
        Some(existing) => {
            let ordering = compare_extreme_values(&candidate, existing)?;
            if minimum { ordering.is_lt() } else { ordering.is_gt() }
        }
    };
    if replace {
        *current = Some(candidate);
    }
    Ok(())
}

fn compare_extreme_values(left: &Value, right: &Value) -> ExecutionResult<std::cmp::Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => number_as_f64(*left)
            .partial_cmp(&number_as_f64(*right))
            .ok_or_else(|| ExecutionError::evaluation("min/max cannot compare non-finite numbers")),
        (Value::String(left), Value::String(right)) => Ok(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        _ => Err(ExecutionError::evaluation(format!(
            "min/max requires comparable values of the same scalar type, found {left:?} and {right:?}"
        ))),
    }
}

fn build_pivot_document( index: usize, specification: &PivotSpecification, row_values: Vec<Option<Value>>, cells: BTreeMap<String, MeasureState>, ) -> ExecutionResult<SyntheticDocument> {
    let mut document = Document::new();

    for (path, value) in specification.rows().iter().zip(row_values) {
        let field = path.last();
        document.insert(field, value.unwrap_or_else(Value::null));
    }

    for (name, state) in cells {
        // A sum cell that never saw a number is 0 in a pivot table.
        document.insert(name, state.finish()?.unwrap_or_else(|| Value::signed(0)));
    }

    let id = DocumentId::synthetic(0x0070_6976_6f74, index as u64 + 1);

    Ok(SyntheticDocument::new(id, Arc::new(document)))
}

#[derive(Clone, Debug)]
struct IncrementalGroupKey {
    source: ExpressionFieldPath,
    output: ExpressionFieldPath,
}

/// Every aggregate, in marker-decoding order.
const AGGREGATES: [PivotAggregate; 7] = [
    PivotAggregate::Sum, PivotAggregate::Average, PivotAggregate::Minimum, PivotAggregate::Maximum,
    PivotAggregate::First, PivotAggregate::Last, PivotAggregate::Count,
];

#[derive(Clone, Debug)]
struct GroupMeasure {
    aggregate: PivotAggregate,
    /// Measured field; `None` for `count` without a field, which counts rows.
    source: Option<ExpressionFieldPath>,
    alias: String,
}

/// Running state of one `group` measure or `pivot` cell.
#[derive(Clone, Debug)]
enum MeasureState {
    /// `integer` stays exact while every term is an integer and the total fits in i64.
    Sum { seen: bool, integer: Option<i64>, total: f64 },
    Avg { count: u64, total: f64 },
    /// Smallest (`minimum`) or largest non-null value.
    Extreme { minimum: bool, value: Option<Value> },
    /// Value of the first row, or of the latest row when `last`.
    Pick { last: bool, value: Option<Value> },
    Count(u64),
}

impl MeasureState {
    const fn new(aggregate: PivotAggregate) -> Self {
        match aggregate {
            PivotAggregate::Sum => Self::Sum { seen: false, integer: Some(0), total: 0.0 },
            PivotAggregate::Average => Self::Avg { count: 0, total: 0.0 },
            PivotAggregate::Minimum => Self::Extreme { minimum: true, value: None },
            PivotAggregate::Maximum => Self::Extreme { minimum: false, value: None },
            PivotAggregate::First => Self::Pick { last: false, value: None },
            PivotAggregate::Last => Self::Pick { last: true, value: None },
            PivotAggregate::Count => Self::Count(0),
        }
    }

    /// Bytes retained beyond the fixed overhead of the state.
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Extreme { value, .. } | Self::Pick { value, .. } => optional_value_bytes(value),
            Self::Sum { .. } | Self::Avg { .. } | Self::Count(_) => 0,
        }
    }

    /// Folds one row's value; `None` means the field is missing on that row.
    /// Sums and averages ignore non-numeric values.
    fn push(&mut self, value: Option<&Value>) -> ExecutionResult<()> {
        let number = value.and_then(Value::as_number).copied();
        match self {
            Self::Sum { .. } | Self::Avg { .. } => {
                if let Some(number) = number {
                    self.push_number(number)?;
                }
            }
            Self::Extreme { minimum, value: current } => {
                update_extreme(current, value.filter(|value| !value.is_null()).cloned(), *minimum)?;
            }
            Self::Pick { last, value: current } => {
                if *last || current.is_none() {
                    *current = Some(value.cloned().unwrap_or_else(Value::null));
                }
            }
            Self::Count(count) => {
                if value.is_some_and(|value| !value.is_null()) {
                    *count = count.checked_add(1).ok_or_else(|| ExecutionError::evaluation("group count exceeds the supported u64 range"))?;
                }
            }
        }
        Ok(())
    }

    /// Borrowed twin of [`Self::push`]: sums, averages and counts read the
    /// storage value in place, so the per-row hot path never builds a [`Value`].
    fn push_ref(&mut self, value: Option<&ProjectedValueRef<'_>>) -> ExecutionResult<()> {
        let number = match value {
            Some(ProjectedValueRef::Owned(value)) => return self.push(Some(value)),
            Some(ProjectedValueRef::Signed(value)) => Some(Number::Signed(*value)),
            Some(ProjectedValueRef::Unsigned(value)) => Some(Number::Unsigned(*value)),
            Some(ProjectedValueRef::Float(value)) => Some(Number::Float(*value)),
            _ => None,
        };
        match self {
            Self::Sum { .. } | Self::Avg { .. } => {
                if let Some(number) = number {
                    self.push_number(number)?;
                }
            }
            Self::Count(count) => {
                if value.is_some_and(|value| !matches!(value, ProjectedValueRef::Null)) {
                    *count = count.checked_add(1).ok_or_else(|| ExecutionError::evaluation("group count exceeds the supported u64 range"))?;
                }
            }
            // The first value is kept: later rows change nothing.
            Self::Pick { last: false, value: Some(_) } => {}
            Self::Extreme { minimum, value: current } => {
                // Compared borrowed; a value is built only when it replaces
                // the current one. Other pairs take the common path.
                let ordering = match (value, current.as_ref()) {
                    (None | Some(ProjectedValueRef::Null), _) => return Ok(()),
                    (Some(ProjectedValueRef::String(text)), Some(Value::String(existing))) => Some((*text).cmp(existing.as_ref())),
                    (Some(_), Some(Value::Number(existing))) => match number {
                        Some(number) => Some(number_as_f64(number).partial_cmp(&number_as_f64(*existing)).ok_or_else(|| {
                            ExecutionError::evaluation("min/max cannot compare non-finite numbers")
                        })?),
                        None => None,
                    },
                    _ => None,
                };
                match ordering {
                    Some(ordering) => {
                        if (*minimum && ordering.is_lt()) || (!*minimum && ordering.is_gt()) {
                            *current = value.map(ProjectedValueRef::to_value);
                        }
                    }
                    None => self.push(value.map(ProjectedValueRef::to_value).as_ref())?,
                }
            }
            Self::Pick { .. } => {
                self.push(value.map(ProjectedValueRef::to_value).as_ref())?;
            }
        }
        Ok(())
    }

    /// Adds one numeric term to a sum or average.
    fn push_number(&mut self, number: Number) -> ExecutionResult<()> {
        match self {
            Self::Sum { seen, integer, total } => {
                *integer = integer.zip(exact_integer(number)).and_then(|(sum, term)| sum.checked_add(term));
                *total += number_as_f64(number);
                *seen = true;
            }
            Self::Avg { count, total } => {
                *total += number_as_f64(number);
                *count = count.checked_add(1).ok_or_else(|| {
                    ExecutionError::evaluation("group count exceeds the supported u64 range")
                })?;
            }
            _ => return Err(ExecutionError::evaluation("group measure is not numeric")),
        }
        Ok(())
    }

    /// Folds a partial state computed over later rows of the same group.
    fn merge(&mut self, other: Self) -> ExecutionResult<()> {
        let overflow = || ExecutionError::evaluation("group count exceeds the supported u64 range");
        match (self, other) {
            (Self::Sum { seen, integer, total }, Self::Sum { seen: other_seen, integer: other_integer, total: other_total }) => {
                *integer = integer.zip(other_integer).and_then(|(left, right)| left.checked_add(right));
                *total += other_total;
                *seen |= other_seen;
            }
            (Self::Avg { count, total }, Self::Avg { count: other_count, total: other_total }) => {
                *total += other_total;
                *count = count.checked_add(other_count).ok_or_else(overflow)?;
            }
            (Self::Extreme { minimum, value }, Self::Extreme { value: other, .. }) => update_extreme(value, other, *minimum)?,
            (Self::Pick { last, value }, Self::Pick { value: other, .. }) => {
                if value.is_none() || (*last && other.is_some()) {
                    *value = other;
                }
            }
            (Self::Count(count), Self::Count(other)) => *count = count.checked_add(other).ok_or_else(overflow)?,
            _ => return Err(ExecutionError::evaluation("group partial state does not match its measure")),
        }
        Ok(())
    }

    fn encode(&self, output: &mut Vec<u8>) {
        match self {
            Self::Sum { seen: false, .. } => output.push(0),
            Self::Sum { integer: Some(integer), .. } => {
                output.push(2);
                output.extend_from_slice(&(*integer as u64).to_be_bytes());
            }
            Self::Sum { total, .. } => {
                output.push(1);
                output.extend_from_slice(&total.to_bits().to_be_bytes());
            }
            Self::Avg { count, total } => {
                encode_var_u64(output, *count);
                output.extend_from_slice(&total.to_bits().to_be_bytes());
            }
            Self::Extreme { value, .. } | Self::Pick { value, .. } => match value {
                Some(value) => {
                    output.push(1);
                    encode_value(output, value);
                }
                None => output.push(0),
            },
            Self::Count(count) => encode_var_u64(output, *count),
        }
    }

    fn decode(aggregate: PivotAggregate, input: &mut CompactGroupDecoder<'_>) -> ExecutionResult<Self> {
        let mut state = Self::new(aggregate);
        match &mut state {
            Self::Sum { seen, integer, total } => match input.byte()? {
                0 => {}
                1 => (*seen, *integer, *total) = (true, None, f64::from_bits(input.u64()?)),
                2 => {
                    let value = input.u64()? as i64;
                    (*seen, *integer, *total) = (true, Some(value), value as f64);
                }
                _ => return Err(ExecutionError::evaluation("invalid compact group sum")),
            },
            Self::Avg { count, total } => (*count, *total) = (input.var_u64()?, f64::from_bits(input.u64()?)),
            Self::Extreme { value, .. } | Self::Pick { value, .. } => {
                *value = match input.byte()? {
                    0 => None,
                    1 => Some(input.value()?),
                    _ => return Err(ExecutionError::evaluation("invalid compact group measure")),
                };
            }
            Self::Count(count) => *count = input.var_u64()?,
        }
        Ok(state)
    }

    /// Final value, or `None` for a sum that never saw a number.
    fn finish(self) -> ExecutionResult<Option<Value>> {
        Ok(match self {
            Self::Sum { seen: false, .. } => None,
            Self::Sum { integer: Some(integer), .. } => Some(Value::signed(integer)),
            Self::Sum { total, .. } => Some(float_value(total)?),
            Self::Avg { count: 0, .. } => Some(Value::null()),
            Self::Avg { count, total } => Some(float_value(total / count as f64)?),
            Self::Extreme { value, .. } | Self::Pick { value, .. } => Some(value.unwrap_or_else(Value::null)),
            Self::Count(count) => Some(Value::unsigned(count)),
        })
    }
}

fn exact_integer(number: Number) -> Option<i64> {
    match number {
        Number::Signed(value) => Some(value),
        Number::Unsigned(value) => i64::try_from(value).ok(),
        Number::Float(_) => None,
    }
}

fn number_as_f64(number: Number) -> f64 {
    match number {
        Number::Signed(value) => value as f64,
        Number::Unsigned(value) => value as f64,
        Number::Float(value) => value,
    }
}

#[derive(Clone, Debug)]
struct DefaultIncrementalGroupAccumulator {
    keys: Arc<[IncrementalGroupKey]>,
    measures: Arc<[GroupMeasure]>,
    key_values: Option<Vec<Option<Value>>>,
    count: u64,
    states: Vec<MeasureState>,
    /// Position of each key and measure source in the required input fields.
    projected_key_indexes: Arc<[usize]>,
    projected_measure_indexes: Arc<[Option<usize>]>,
}

impl DefaultIncrementalGroupAccumulator {
    fn new(fields: &[ExpressionFieldPath]) -> ExecutionResult<Self> {
        let mut keys = Vec::new();
        let mut measures = Vec::new();
        for field in fields {
            if let Some(measure) = decode_group_measure_marker(field)? {
                measures.push(measure);
            } else if let Some((source, output)) = decode_group_key_marker(field)? {
                keys.push(IncrementalGroupKey { source, output });
            } else {
                keys.push(IncrementalGroupKey { source: field.clone(), output: field.clone() });
            }
        }
        let (_, required) = group_field_layout(fields)?;
        let position = |source: &ExpressionFieldPath| {
            required.iter().position(|field| field == source).expect("group input field is required")
        };
        let projected_key_indexes = keys.iter().map(|key| position(&key.source)).collect();
        let projected_measure_indexes = measures.iter().map(|measure| measure.source.as_ref().map(position)).collect();
        Ok(Self {
            states: measures.iter().map(|measure| MeasureState::new(measure.aggregate)).collect(),
            keys: Arc::from(keys),
            measures: Arc::from(measures),
            key_values: None,
            count: 0,
            projected_key_indexes,
            projected_measure_indexes,
        })
    }

    /// Captures the grouping-key values from the group's first row.
    fn init_keys(&mut self, values: impl FnOnce(&Self) -> Vec<Option<Value>>) {
        if self.key_values.is_none() {
            self.key_values = Some(values(self));
        }
    }

    fn count_row(&mut self) -> ExecutionResult<()> {
        self.count = self.count.checked_add(1).ok_or_else(|| {
            ExecutionError::evaluation("group count exceeds the supported u64 range")
        })?;
        Ok(())
    }

    /// Counts one row and folds it; `value(index)` returns measure `index`'s source value.
    fn push_row<'v>(&mut self, mut value: impl FnMut(usize) -> Option<&'v Value>) -> ExecutionResult<()> {
        self.count_row()?;
        let row = Value::bool(true);
        for (index, state) in self.states.iter_mut().enumerate() {
            let measured = if self.measures[index].source.is_some() { value(index) } else { Some(&row) };
            state.push(measured)?;
        }
        Ok(())
    }
}

impl IncrementalGroupAccumulator for DefaultIncrementalGroupAccumulator {
    fn push(&mut self, document: &Document) -> ExecutionResult<()> {
        self.init_keys(|this| this.keys.iter().map(|key| path_value(document, &key.source).cloned()).collect());
        let measures = Arc::clone(&self.measures);
        self.push_row(|index| measures[index].source.as_ref().and_then(|source| path_value(document, source)))
    }

    fn push_projected_values(&mut self, values: &[Option<Value>]) -> ExecutionResult<bool> {
        self.init_keys(|this| this.projected_key_indexes.iter().map(|index| values.get(*index).cloned().flatten()).collect());
        let indexes = Arc::clone(&self.projected_measure_indexes);
        self.push_row(|index| indexes[index].and_then(|slot| values.get(slot)).and_then(Option::as_ref))?;
        Ok(true)
    }

    fn push_projected_value_refs( &mut self, values: &[Option<crate::storage::ProjectedValueRef<'_>>], source_slots: &[usize], ) -> ExecutionResult<bool> {
        let resolve = |index: usize| {
            values
                .get(source_slots.get(index).copied().unwrap_or(usize::MAX))
                .and_then(Option::as_ref)
        };
        self.init_keys(|this| this.projected_key_indexes.iter().map(|index| resolve(*index).map(ProjectedValueRef::to_value)).collect());
        self.count_row()?;
        let row = Value::bool(true);
        for (state, slot) in self.states.iter_mut().zip(self.projected_measure_indexes.iter()) {
            match slot {
                Some(slot) => state.push_ref(resolve(*slot))?,
                None => state.push(Some(&row))?,
            }
        }
        Ok(true)
    }

    fn into_dense(self: Box<Self>) -> Option<DenseGroups> {
        Some(DenseGroups { template: *self, keys: Vec::new(), counts: Vec::new(), states: Vec::new() })
    }

    fn compact_partial(&self) -> ExecutionResult<Option<Vec<u8>>> {
        // Grouping-key values are omitted: the engine already writes the
        // canonical key once per spill record and seeds it again during merge.
        let mut out = Vec::with_capacity(9 + self.states.len() * 9);
        out.push(3);
        encode_var_u64(&mut out, self.count);
        for state in &self.states {
            state.encode(&mut out);
        }
        Ok(Some(out))
    }

    fn seed_group_key(&mut self, encoded_key: &[u8]) -> ExecutionResult<bool> {
        if self.key_values.is_some() {
            return Ok(true);
        }
        let Some(values) = decode_explicit_distinct_key_values(encoded_key)? else {
            return Ok(false);
        };
        if values.len() != self.keys.len() {
            return Ok(false);
        }
        self.key_values = Some(values);
        Ok(true)
    }

    fn merge_compact_partial(&mut self, payload: &[u8]) -> ExecutionResult<bool> {
        // Spill runs live only for one query, so only the current format (3) exists.
        let mut input = CompactGroupDecoder::new(payload);
        if input.byte()? != 3 {
            return Ok(false);
        }
        self.count = self.count.checked_add(input.var_u64()?).ok_or_else(|| {
            ExecutionError::evaluation("group count exceeds the supported u64 range")
        })?;
        for index in 0..self.measures.len() {
            let partial = MeasureState::decode(self.measures[index].aggregate, &mut input)?;
            self.states[index].merge(partial)?;
        }
        input.finish()?;
        Ok(true)
    }

    fn finish(self: Box<Self>, ordinal: u64) -> ExecutionResult<SyntheticDocument> {
        let mut document = Document::new();
        let key_values = self.key_values.unwrap_or_default();
        for (key, value) in self.keys.iter().zip(key_values) {
            let path = key.output.iter().collect::<Vec<_>>();
            insert_path(&mut document, &path, value.unwrap_or_else(Value::null));
        }

        let mut emitted = false;
        for (measure, state) in self.measures.iter().zip(self.states) {
            if let Some(value) = state.finish()? {
                document.insert(measure.alias.as_str(), value);
                emitted = true;
            }
        }
        if !emitted {
            document.insert("count", Value::unsigned(self.count));
        }

        Ok(SyntheticDocument::new(
            DocumentId::synthetic(GROUP_NAMESPACE, ordinal),
            Arc::new(document),
        ))
    }
}
/// Every group of one `group` stage, with the measure states of all groups
/// in one contiguous vector: the per-row fold touches one small slice
/// instead of a boxed accumulator per group. A group finishes through the
/// same accumulator as [`IncrementalGroupAccumulator::finish`].
#[derive(Debug)]
pub struct DenseGroups {
    template: DefaultIncrementalGroupAccumulator,
    keys: Vec<Option<Vec<Option<Value>>>>,
    counts: Vec<u64>,
    states: Vec<MeasureState>,
}

impl DenseGroups {
    /// Groups created so far (evicted ones included).
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Creates a group whose key values are read from `values` (the group
    /// input fields of its first row).
    pub fn add(&mut self, values: &[Option<ProjectedValueRef<'_>>]) -> u32 {
        let group = self.keys.len() as u32;
        self.keys.push(Some(self.template.projected_key_indexes.iter().map(|index| values.get(*index).and_then(Option::as_ref).map(ProjectedValueRef::to_value)).collect()));
        self.counts.push(0);
        self.states.extend_from_slice(&self.template.states);
        group
    }

    /// Folds one row of `group`, as `push_projected_value_refs` does.
    #[inline]
    pub fn push(&mut self, group: u32, values: &[Option<ProjectedValueRef<'_>>]) -> ExecutionResult<()> {
        let group = group as usize;
        let count = &mut self.counts[group];
        *count = count.checked_add(1).ok_or_else(|| ExecutionError::evaluation("group count exceeds the supported u64 range"))?;
        let measures = self.template.states.len();
        let states = &mut self.states[group * measures..(group + 1) * measures];
        for (state, slot) in states.iter_mut().zip(self.template.projected_measure_indexes.iter()) {
            match slot {
                Some(slot) => state.push_ref(values.get(*slot).and_then(Option::as_ref))?,
                None => state.push(Some(&Value::Bool(true)))?,
            }
        }
        Ok(())
    }

    /// Drops a group: it is never finished.
    pub fn evict(&mut self, group: u32) {
        self.keys[group as usize] = None;
    }

    /// Result row of `group`, numbered `ordinal`.
    pub fn finish(&mut self, group: u32, ordinal: u64) -> ExecutionResult<SyntheticDocument> {
        let group = group as usize;
        let measures = self.template.states.len();
        let mut accumulator = self.template.clone();
        accumulator.key_values = self.keys[group].take();
        accumulator.count = self.counts[group];
        accumulator.states = self.states[group * measures..(group + 1) * measures].to_vec();
        Box::new(accumulator).finish(ordinal)
    }
}

const GROUP_NAMESPACE: u64 = 0x0067_726f_7570;
const GROUP_MARKER_PREFIX: &str = "__og_group_";
const GROUP_KEY_MARKER_PREFIX: &str = "__og_group_key_";

/// Resolves the physical group layout.
///
/// Returns `(grouping_keys, required_input_fields)`: grouping keys exclude the
/// planner's internal measure markers, while required input fields replace each
/// marker with its source path. This is the shape storage projection must use.
pub fn group_field_layout( fields: &[ExpressionFieldPath], ) -> ExecutionResult<(Vec<ExpressionFieldPath>, Vec<ExpressionFieldPath>)> {
    let mut grouping_keys = Vec::new();
    let mut required = Vec::new();

    for field in fields {
        if let Some(measure) = decode_group_measure_marker(field)? {
            if let Some(source) = measure.source {
                if !required.contains(&source) {
                    required.push(source);
                }
            }
        } else if let Some((source, _output)) = decode_group_key_marker(field)? {
            grouping_keys.push(source.clone());
            if !required.contains(&source) {
                required.push(source);
            }
        } else {
            grouping_keys.push(field.clone());
            if !required.contains(field) {
                required.push(field.clone());
            }
        }
    }

    Ok((grouping_keys, required))
}

/// Renders group fields as written by the user (`region as zone, avg(qty) as mean`).
pub(crate) fn describe_group_fields(fields: &[ExpressionFieldPath]) -> String {
    fields
        .iter()
        .map(|field| {
            if let Ok(Some(measure)) = decode_group_measure_marker(field) {
                let source = measure.source.map(|source| source.to_string()).unwrap_or_default();
                format!("{}({source}) as {}", measure.aggregate.as_str(), measure.alias)
            } else if let Ok(Some((source, output))) = decode_group_key_marker(field) {
                format!("{source} as {output}")
            } else {
                field.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn decode_group_key_marker( field: &ExpressionFieldPath, ) -> ExecutionResult<Option<(ExpressionFieldPath, ExpressionFieldPath)>> {
    let Some((source, output)) = decode_group_path_marker(field, GROUP_KEY_MARKER_PREFIX, "key")?
    else {
        return Ok(None);
    };
    let Some(source) = source else {
        return Err(ExecutionError::evaluation("group key marker has no source field"));
    };
    let output = ExpressionFieldPath::new(output.split('.')).map_err(|error| {
        ExecutionError::evaluation(format!("invalid encoded group output path: {error}"))
    })?;
    Ok(Some((source, output)))
}

/// Decodes `__og_group_<prefix-rest>` into `(source, output)`. An empty source
/// (a row `count` measure) decodes as `None`.
fn decode_group_path_marker( field: &ExpressionFieldPath, prefix: &str, marker_kind: &str, ) -> ExecutionResult<Option<(Option<ExpressionFieldPath>, String)>> {
    let segments = field.iter().collect::<Vec<_>>();
    if segments.len() != 1 {
        return Ok(None);
    }

    let Some(encoded) = segments[0].strip_prefix(prefix) else {
        return Ok(None);
    };
    let Some((source_hex, output_hex)) = encoded.split_once('_') else {
        return Err(ExecutionError::evaluation(format!(
            "invalid encoded group {marker_kind} marker"
        )));
    };

    let output = decode_group_marker_text(output_hex)?;
    if source_hex.is_empty() {
        return Ok(Some((None, output)));
    }
    let source = decode_group_marker_text(source_hex)?;
    let source = ExpressionFieldPath::new(source.split('.')).map_err(|error| {
        ExecutionError::evaluation(format!("invalid encoded group source path: {error}"))
    })?;
    Ok(Some((Some(source), output)))
}

/// Decodes a measure marker `__og_group_<aggregate>_<source hex>_<alias hex>`.
fn decode_group_measure_marker( field: &ExpressionFieldPath, ) -> ExecutionResult<Option<GroupMeasure>> {
    let Some(rest) = field.first().strip_prefix(GROUP_MARKER_PREFIX) else {
        return Ok(None);
    };
    for aggregate in AGGREGATES {
        let name = aggregate.as_str();
        if !rest.strip_prefix(name).is_some_and(|rest| rest.starts_with('_')) {
            continue;
        }
        let prefix = format!("{GROUP_MARKER_PREFIX}{name}_");
        let Some((source, alias)) = decode_group_path_marker(field, &prefix, name)? else {
            return Ok(None);
        };
        if source.is_none() && aggregate != PivotAggregate::Count {
            return Err(ExecutionError::evaluation(format!("group {name} measure requires a field")));
        }
        return Ok(Some(GroupMeasure { aggregate, source, alias }));
    }
    Ok(None)
}

fn decode_group_marker_text(text: &str) -> ExecutionResult<String> {
    if !text.len().is_multiple_of(2) {
        return Err(ExecutionError::evaluation(
            "invalid hexadecimal group marker",
        ));
    }

    let mut bytes = Vec::with_capacity(text.len() / 2);
    for index in (0..text.len()).step_by(2) {
        let byte = u8::from_str_radix(&text[index..index + 2], 16)
            .map_err(|_| ExecutionError::evaluation("invalid hexadecimal group marker"))?;
        bytes.push(byte);
    }

    String::from_utf8(bytes)
        .map_err(|_| ExecutionError::evaluation("group marker is not valid UTF-8"))
}

/// How a compiled stage relates its output fields to its input fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StageOutput {
    /// Every input field passes through, except the written ones.
    Rewritten,
    /// Only the written fields are output.
    Projected,
    /// The row is replaced by a value of unknown shape (`root`).
    Replaced,
}

/// What an expanding stage makes of one slot row.
#[derive(Debug)]
pub enum SlotExpansion {
    /// The row passes through unchanged.
    Keep,
    /// The row is removed.
    Drop,
    /// The row passes through with these slots written.
    Set(Vec<(ExpressionFieldPath, Option<Value>)>),
    /// One row per item: `field` holds the item and `index` its position.
    Each { field: ExpressionFieldPath, index: Option<ExpressionFieldPath>, items: Arc<[Value]> },
}

/// A native row-local stage (`derive`, `select … as`, `rename`, `drop`,
/// `root`, `near`, `unwind`, `first`/`single`) compiled once per plan:
/// expressions are parsed and paths resolved before the first row.
///
/// It also declares the input fields it reads and the fields it writes, so
/// planning can reason about field usage without knowing the stage.
#[derive(Debug)]
pub struct CompiledStage {
    operation: CompiledOperation,
    reads: Arc<[ExpressionFieldPath]>,
    writes: Arc<[ExpressionFieldPath]>,
    output: StageOutput,
}

#[derive(Debug)]
enum CompiledOperation {
    Derive(Vec<(ExpressionFieldPath, Expression)>),
    Select(Vec<(ExpressionFieldPath, SelectSource)>),
    Rename { source: ExpressionFieldPath, target: ExpressionFieldPath },
    Drop(Vec<ExpressionFieldPath>),
    Near(NearSpec),
    Root(ExpressionFieldPath),
    Project(Option<ExpressionFieldPath>),
    Unwind { field: ExpressionFieldPath, index: Option<ExpressionFieldPath>, keep_empty: bool },
    /// A stage executed elsewhere (`sample`): compiled only for its properties.
    Passthrough,
}

#[derive(Debug)]
enum SelectSource {
    Field(ExpressionFieldPath),
    Expression(Expression),
}

/// What a stage that runs on slots does, for executors that compile it
/// their own way (see [`CompiledStage::slot_operation`]).
pub(crate) enum SlotOperation<'a> {
    /// Assignments in order; later ones read earlier results.
    Derive(&'a [(ExpressionFieldPath, Expression)]),
    /// `select`: each target reads a field (`Ok`) or an expression (`Err`).
    Select(Vec<(&'a ExpressionFieldPath, Result<&'a ExpressionFieldPath, &'a Expression>)>),
    Rename { source: &'a ExpressionFieldPath, target: &'a ExpressionFieldPath },
    Drop(&'a [ExpressionFieldPath]),
}

impl CompiledStage {
    /// Compiles a native stage from its planned arguments; `None` for a stage
    /// this runtime does not implement.
    pub fn compile(name: &str, arguments: &str) -> ExecutionResult<Option<Self>> {
        let mut reads = Vec::new();
        let mut writes = Vec::new();
        let mut output = StageOutput::Rewritten;
        let operation = match name {
            "derive" => {
                let mut assignments = Vec::new();
                for (field, source) in split_derive_assignments(arguments)? {
                    let expression = parse_expression(source).map_err(|error| {
                        ExecutionError::evaluation(format!("invalid derive expression {source:?}: {error}"))
                    })?;
                    read_expression_fields(&expression, &writes, &mut reads);
                    let target = runtime_path(field, "derive field")?;
                    writes.push(target.clone());
                    assignments.push((target, expression));
                }
                CompiledOperation::Derive(assignments)
            }
            "select" => {
                output = StageOutput::Projected;
                let mut items = Vec::new();
                for (item_index, item) in split_projection_items(arguments)?.into_iter().enumerate() {
                    let item = item.trim();
                    if item.is_empty() {
                        return Err(ExecutionError::evaluation(format!(
                            "invalid select projection at index {item_index}: projection is empty"
                        )));
                    }
                    let source = match item.rsplit_once(" as ") {
                        Some((source, _)) => {
                            let source = source.trim();
                            let expression = parse_expression(source).map_err(|error| {
                                ExecutionError::evaluation(format!("invalid select expression {source:?}: {error}"))
                            })?;
                            read_expression_fields(&expression, &writes, &mut reads);
                            SelectSource::Expression(expression)
                        }
                        None => {
                            let field = runtime_path(item, "select source")?;
                            push_unique(&mut reads, &field);
                            SelectSource::Field(field)
                        }
                    };
                    let target = runtime_path(item.rsplit_once(" as ").map_or(item, |(_, target)| target), "select target")?;
                    writes.push(target.clone());
                    items.push((target, source));
                }
                CompiledOperation::Select(items)
            }
            "rename" => {
                let (source, target) = arguments.split_once(" as ").ok_or_else(|| {
                    ExecutionError::evaluation("invalid rename runtime payload: expected `<source> as <target>`")
                })?;
                let source = runtime_path(source, "rename source")?;
                let target = runtime_path(target, "rename target")?;
                reads.push(source.clone());
                writes.extend([source.clone(), target.clone()]);
                CompiledOperation::Rename { source, target }
            }
            "drop" => {
                writes = arguments.split(',').map(|field| runtime_path(field, "drop field")).collect::<ExecutionResult<_>>()?;
                CompiledOperation::Drop(writes.clone())
            }
            "near" => {
                let spec = NearSpec::parse(arguments)?;
                reads.push(spec.field().clone());
                writes.push(runtime_path("_distance", "near output")?);
                CompiledOperation::Near(spec)
            }
            "root" => {
                output = StageOutput::Replaced;
                let path = runtime_path(arguments, "root field")?;
                reads.push(path.clone());
                CompiledOperation::Root(path)
            }
            "first" | "single" => {
                let field = arguments.trim();
                if field.is_empty() {
                    CompiledOperation::Project(None)
                } else {
                    output = StageOutput::Projected;
                    let path = runtime_path(field, "projection field")?;
                    reads.push(path.clone());
                    writes.push(path.clone());
                    CompiledOperation::Project(Some(path))
                }
            }
            "unwind" => {
                let spec = super::syntax::UnwindSpec::parse(arguments).map_err(ExecutionError::evaluation)?;
                let field = runtime_path(spec.field, "unwind field")?;
                let index = spec.index.map(|index| runtime_path(index, "unwind index")).transpose()?;
                reads.push(field.clone());
                writes.push(field.clone());
                writes.extend(index.clone());
                CompiledOperation::Unwind { field, index, keep_empty: spec.keep_empty }
            }
            // The reservoir runs in the executors; rows keep every field.
            "sample" => CompiledOperation::Passthrough,
            _ => return Ok(None),
        };
        Ok(Some(Self { operation, reads: Arc::from(reads), writes: Arc::from(writes), output }))
    }

    /// Input fields the stage reads.
    #[must_use]
    pub fn reads(&self) -> &Arc<[ExpressionFieldPath]> {
        &self.reads
    }

    /// Output fields relative to input fields, as an execution property.
    #[must_use]
    pub fn output_fields(&self) -> super::Fields<'_> {
        match self.output {
            StageOutput::Rewritten if self.writes.is_empty() => super::Fields::Preserved,
            StageOutput::Rewritten => super::Fields::Rewritten(&self.writes),
            StageOutput::Projected => super::Fields::Projected(&self.writes),
            StageOutput::Replaced => super::Fields::Computed,
        }
    }

    /// Whether the stage can run on a [`SlotRow`]: it turns one row into one
    /// row and every field it writes is top-level, so each write lands in a
    /// slot of its own. Reads may be nested; they walk the top-level slot.
    #[must_use]
    pub fn runs_on_slots(&self) -> bool {
        matches!(
            self.operation,
            CompiledOperation::Derive(_) | CompiledOperation::Select(_) | CompiledOperation::Rename { .. } | CompiledOperation::Drop(_)
        ) && self.writes.iter().all(|field| field.len() == 1)
    }

    /// The operation of a stage that runs on slots
    /// ([`Self::runs_on_slots`]); `None` otherwise.
    pub(crate) fn slot_operation(&self) -> Option<SlotOperation<'_>> {
        if !self.runs_on_slots() {
            return None;
        }
        Some(match &self.operation {
            CompiledOperation::Derive(assignments) => SlotOperation::Derive(assignments),
            CompiledOperation::Select(items) => SlotOperation::Select(items.iter().map(|(target, source)| (target, match source {
                SelectSource::Field(field) => Ok(field),
                SelectSource::Expression(expression) => Err(expression),
            })).collect()),
            CompiledOperation::Rename { source, target } => SlotOperation::Rename { source, target },
            CompiledOperation::Drop(paths) => SlotOperation::Drop(paths),
            _ => return None,
        })
    }

    /// Field, index field and `keep empty` of an `unwind` that runs on
    /// slots ([`Self::expands_on_slots`]); `None` otherwise.
    pub(crate) fn unwind_operation(&self) -> Option<(&ExpressionFieldPath, Option<&ExpressionFieldPath>, bool)> {
        match &self.operation {
            CompiledOperation::Unwind { field, index, keep_empty } if self.expands_on_slots() => Some((field, index.as_ref(), *keep_empty)),
            _ => None,
        }
    }

    /// Whether the stage can run on a [`SlotRow`] by turning it into any
    /// number of rows (`unwind`), each landing its writes in top-level slots.
    #[must_use]
    pub fn expands_on_slots(&self) -> bool {
        matches!(self.operation, CompiledOperation::Unwind { .. }) && self.writes.iter().all(|field| field.len() == 1)
    }

    /// What an expanding stage does to one slot row, with the same semantics
    /// as [`Self::apply`] on the document holding the row's present slots.
    /// Only valid when [`Self::expands_on_slots`] holds.
    pub fn expand_slots(&self, row: &SlotRow<'_>) -> ExecutionResult<SlotExpansion> {
        let CompiledOperation::Unwind { field, index, keep_empty } = &self.operation else {
            return Err(ExecutionError::unsupported_operator("stage", "does not expand slot rows"));
        };
        let null_index = || index.iter().map(|index| (index.clone(), Some(Value::null()))).collect::<Vec<_>>();
        Ok(match row.get(field) {
            Some(Value::Array(items)) if !items.is_empty() => SlotExpansion::Each {
                field: field.clone(),
                index: index.clone(),
                items: Arc::clone(items),
            },
            // An empty (or null) array drops the row unless `keep empty`
            // asks to keep it with the field set to null.
            Some(Value::Array(_) | Value::Null) if *keep_empty => {
                let mut writes = vec![(field.clone(), Some(Value::null()))];
                writes.extend(null_index());
                SlotExpansion::Set(writes)
            }
            Some(Value::Array(_)) => SlotExpansion::Drop,
            // A missing field or a scalar passes through unchanged (with a null index).
            Some(_) | None if index.is_some() => SlotExpansion::Set(null_index()),
            _ => SlotExpansion::Keep,
        })
    }

    /// When the stage cannot fail on any row whose fields have the kinds
    /// `field` gives (see [`super::total_kinds`]), the kinds it leaves in the
    /// fields it writes: `None` for a field it removes. `None` when the stage
    /// may fail, or is not a row-local rewrite of top-level fields.
    #[must_use]
    pub fn total_effects(&self, field: &dyn Fn(&ExpressionFieldPath) -> Option<super::Kinds>) -> Option<Vec<(ExpressionFieldPath, Option<super::Kinds>)>> {
        if !self.writes.iter().all(|path| path.len() == 1) {
            return None;
        }
        let mut effects: Vec<(ExpressionFieldPath, Option<super::Kinds>)> = Vec::new();
        let lookup = |effects: &[(ExpressionFieldPath, Option<super::Kinds>)], path: &ExpressionFieldPath| {
            match effects.iter().rev().find(|(written, _)| written.first() == path.first()) {
                Some((_, kinds)) if path.len() == 1 => *kinds,
                Some(_) => None,
                None => field(path),
            }
        };
        match &self.operation {
            CompiledOperation::Derive(assignments) => {
                for (target, expression) in assignments {
                    let kinds = super::total_kinds(expression, &|path| lookup(&effects, path))?;
                    effects.push((target.clone(), Some(kinds)));
                }
            }
            CompiledOperation::Rename { source, target } => {
                // A missing source leaves the row unchanged: the target keeps
                // what it held, so only a source every row holds is followed.
                let kinds = lookup(&effects, source)?;
                effects.push((source.clone(), None));
                effects.push((target.clone(), Some(kinds)));
            }
            CompiledOperation::Drop(paths) => effects.extend(paths.iter().map(|path| (path.clone(), None))),
            _ => return None,
        }
        Some(effects)
    }

    /// Fields the stage writes (or removes), in declaration order.
    #[must_use]
    pub fn writes(&self) -> &Arc<[ExpressionFieldPath]> {
        &self.writes
    }

    /// Applies the stage to one slot row, with the same semantics as
    /// [`Self::apply`] on the document holding the row's present slots.
    /// Only valid when [`Self::runs_on_slots`] holds.
    pub fn apply_slots(&self, row: &mut SlotRow<'_>) -> ExecutionResult<()> {
        match &self.operation {
            CompiledOperation::Derive(assignments) => {
                for (target, expression) in assignments {
                    let value = super::eval::evaluate(expression, &*row)?;
                    row.set(target, Some(value));
                }
            }
            CompiledOperation::Select(items) => {
                // Later items see the aliases written by earlier ones, as the
                // document path's scope does; only the targets are output.
                let mut produced = Vec::with_capacity(items.len());
                for (target, source) in items {
                    let value = match source {
                        SelectSource::Field(field) => row.get(field).cloned(),
                        SelectSource::Expression(expression) => Some(super::eval::evaluate(expression, &*row)?),
                    };
                    if let Some(value) = value {
                        if items.len() > 1 {
                            row.set(target, Some(value.clone()));
                        }
                        produced.push((target, value));
                    }
                }
                row.clear();
                for (target, value) in produced {
                    row.set(target, Some(value));
                }
            }
            CompiledOperation::Rename { source, target } => {
                if let Some(value) = row.take(source) {
                    row.set(target, Some(value));
                }
            }
            CompiledOperation::Drop(paths) => {
                for path in paths {
                    row.set(path, None);
                }
            }
            _ => return Err(ExecutionError::unsupported_operator("stage", "not runnable on slot rows")),
        }
        Ok(())
    }

    /// Applies the stage to one document.
    pub fn apply(&self, document: &Document) -> ExecutionResult<CustomOperatorResult> {
        let replace = |document: Document| Ok(CustomOperatorResult::Replace(Arc::new(document)));
        match &self.operation {
            CompiledOperation::Derive(assignments) => {
                let mut result = document.clone();
                for (target, expression) in assignments {
                    // Assignments apply in order, so later ones can use earlier results.
                    let value = super::eval::evaluate_document(expression, &result)?;
                    insert_path(&mut result, target.segments(), value);
                }
                replace(result)
            }
            CompiledOperation::Select(items) => {
                let mut result = Document::new();
                // Later items may use aliases introduced earlier; copy the row only then.
                let mut scope: Option<Document> = None;
                for (target, source) in items {
                    let value = match source {
                        SelectSource::Field(field) => path_value(scope.as_ref().unwrap_or(document), field).cloned(),
                        SelectSource::Expression(expression) => {
                            Some(super::eval::evaluate_document(expression, scope.as_ref().unwrap_or(document))?)
                        }
                    };
                    if let Some(value) = value {
                        if items.len() > 1 {
                            insert_path(scope.get_or_insert_with(|| document.clone()), target.segments(), value.clone());
                        }
                        insert_path(&mut result, target.segments(), value);
                    }
                }
                replace(result)
            }
            CompiledOperation::Rename { source, target } => {
                let mut result = document.clone();
                let Some(value) = remove_path(&mut result, source.segments()) else {
                    return Ok(CustomOperatorResult::Keep);
                };
                insert_path(&mut result, target.segments(), value);
                replace(result)
            }
            CompiledOperation::Drop(paths) => {
                let mut result = document.clone();
                let mut changed = false;
                for path in paths {
                    changed |= remove_path(&mut result, path.segments()).is_some();
                }
                if changed { replace(result) } else { Ok(CustomOperatorResult::Keep) }
            }
            CompiledOperation::Near(spec) => {
                let Some(value) = path_value(document, spec.field()) else {
                    return Ok(CustomOperatorResult::Discard);
                };
                let distance = spec.distance_value(value)?;
                let mut result = document.clone();
                result.insert("_distance", Value::float(distance).map_err(|error| {
                    ExecutionError::evaluation(format!("invalid near distance: {error}"))
                })?);
                replace(result)
            }
            CompiledOperation::Root(path) => {
                let value = path_value(document, path)
                    .ok_or_else(|| ExecutionError::evaluation(format!("root field {:?} is missing", path.to_string())))?;
                let Value::Object(root) = value else {
                    return Err(ExecutionError::evaluation(format!("root field {:?} must be an object", path.to_string())));
                };
                Ok(CustomOperatorResult::Replace(Arc::clone(root)))
            }
            CompiledOperation::Project(None) | CompiledOperation::Passthrough => Ok(CustomOperatorResult::Keep),
            CompiledOperation::Project(Some(path)) => {
                let mut result = Document::new();
                if let Some(value) = path_value(document, path) {
                    insert_path(&mut result, path.segments(), value.clone());
                }
                replace(result)
            }
            CompiledOperation::Unwind { field, index, keep_empty } => {
                let with_item = |value: Value, position: Option<usize>| {
                    let mut result = document.clone();
                    insert_path(&mut result, field.segments(), value);
                    if let Some(index) = index {
                        let position = position.map_or_else(Value::null, |position| Value::unsigned(position as u64));
                        insert_path(&mut result, index.segments(), position);
                    }
                    Arc::new(result)
                };
                match path_value(document, field) {
                    Some(Value::Array(values)) if !values.is_empty() => Ok(CustomOperatorResult::Expand(
                        values.iter().cloned().enumerate().map(|(position, value)| with_item(value, Some(position))).collect(),
                    )),
                    // An empty (or null) array drops the row unless `keep empty`
                    // asks to keep it with the field set to null.
                    Some(Value::Array(_) | Value::Null) if *keep_empty => Ok(CustomOperatorResult::Replace(with_item(Value::null(), None))),
                    Some(Value::Array(_)) => Ok(CustomOperatorResult::Expand(Vec::new())),
                    // A missing field or a scalar passes through unchanged (with a null index).
                    Some(value) if index.is_some() => Ok(CustomOperatorResult::Replace(with_item(value.clone(), None))),
                    None => match index {
                        Some(index) => {
                            let mut result = document.clone();
                            insert_path(&mut result, index.segments(), Value::null());
                            replace(result)
                        }
                        None => Ok(CustomOperatorResult::Keep),
                    },
                    Some(_) => Ok(CustomOperatorResult::Keep),
                }
            }
        }
    }
}

/// Parses a stage field path (`a.b.c`), rejecting empty segments.
fn runtime_path(text: &str, context: &str) -> ExecutionResult<ExpressionFieldPath> {
    let segments = parse_runtime_path(text, context)?;
    ExpressionFieldPath::new(segments).map_err(|error| ExecutionError::evaluation(error.to_string()))
}

/// Adds the fields an expression reads, except those the stage wrote earlier.
fn read_expression_fields(expression: &Expression, written: &[ExpressionFieldPath], reads: &mut Vec<ExpressionFieldPath>) {
    expression.for_each_field(&mut |field| {
        if !written.contains(field) {
            push_unique(reads, field);
        }
    });
}

fn push_unique(fields: &mut Vec<ExpressionFieldPath>, field: &ExpressionFieldPath) {
    if !fields.contains(field) {
        fields.push(field.clone());
    }
}

fn parse_runtime_path<'a>(text: &'a str, context: &str) -> ExecutionResult<Vec<&'a str>> {
    let text = text.trim();

    if text.is_empty() {
        return Err(ExecutionError::evaluation(format!(
            "{context} must not be empty"
        )));
    }

    let segments = text.split('.').map(str::trim).collect::<Vec<_>>();

    if segments.iter().any(|segment| segment.is_empty()) {
        return Err(ExecutionError::evaluation(format!(
            "{context} contains an empty path segment"
        )));
    }

    Ok(segments)
}

fn remove_path<S: AsRef<str>>(document: &mut Document, path: &[S]) -> Option<Value> {
    let (first, rest) = path.split_first()?;
    let first = first.as_ref();

    if rest.is_empty() {
        return document.remove(first);
    }

    let original = document.remove(first)?;
    let Some(parent) = original.as_object() else {
        document.insert(first, original);
        return None;
    };

    let mut nested = parent.clone();
    let removed = remove_path(&mut nested, rest);

    if let Some(value) = removed {
        if !nested.is_empty() {
            document.insert(first, Value::object(nested));
        }
        Some(value)
    } else {
        document.insert(first, original);
        None
    }
}

fn insert_path<S: AsRef<str>>(document: &mut Document, path: &[S], value: Value) {
    let Some((first, rest)) = path.split_first() else {
        return;
    };
    let first = first.as_ref();

    if rest.is_empty() {
        document.insert(first, value);
        return;
    }

    let mut nested = document
        .remove(first)
        .and_then(Value::into_object)
        .map(|document| document.as_ref().clone())
        .unwrap_or_default();

    insert_path(&mut nested, rest, value);
    document.insert(first, Value::object(nested));
}

fn path_value<'a>(document: &'a Document, path: &ExpressionFieldPath) -> Option<&'a Value> {
    let mut segments = path.iter();
    let first = segments.next()?;
    let mut value = document.get(first)?;

    for segment in segments {
        value = value.as_object()?.get(segment)?;
    }

    Some(value)
}

fn cloned_path_value(document: &Document, path: &ExpressionFieldPath) -> Option<Value> {
    path_value(document, path).cloned()
}

fn encode_len(output: &mut Vec<u8>, value: usize) {
    output.extend_from_slice(&(value as u64).to_be_bytes());
}

fn encode_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    encode_len(output, bytes.len());
    output.extend_from_slice(bytes);
}

fn encode_document(output: &mut Vec<u8>, document: &Document) {
    encode_len(output, document.len());

    for (name, value) in document {
        encode_bytes(output, name.as_str().as_bytes());
        encode_value(output, value);
    }
}

fn encode_projected_ref(output: &mut Vec<u8>, value: &crate::storage::ProjectedValueRef<'_>) {
    use crate::storage::ProjectedValueRef;
    match value {
        ProjectedValueRef::Null => output.push(0),
        ProjectedValueRef::Bool(false) => output.push(1),
        ProjectedValueRef::Bool(true) => output.push(2),
        ProjectedValueRef::Signed(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        ProjectedValueRef::Unsigned(value) => {
            output.push(4);
            output.extend_from_slice(&value.to_be_bytes());
        }
        ProjectedValueRef::Float(value) => {
            output.push(5);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        ProjectedValueRef::String(value) => {
            output.push(6);
            encode_bytes(output, value.as_bytes());
        }
        ProjectedValueRef::Owned(value) => encode_value(output, value),
    }
}

fn encode_value(output: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => output.push(0),
        Value::Bool(false) => output.push(1),
        Value::Bool(true) => output.push(2),
        Value::Number(Number::Signed(value)) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Value::Number(Number::Unsigned(value)) => {
            output.push(4);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Value::Number(Number::Float(value)) => {
            output.push(5);
            output.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Value::String(value) => {
            output.push(6);
            encode_bytes(output, value.as_bytes());
        }
        Value::Array(values) => {
            output.push(7);
            encode_len(output, values.len());
            for value in values.iter() {
                encode_value(output, value);
            }
        }
        Value::Object(document) => {
            output.push(8);
            encode_document(output, document);
        }
    }
}

fn encode_var_u64(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn decode_explicit_distinct_key_values( bytes: &[u8], ) -> ExecutionResult<Option<Vec<Option<Value>>>> {
    let mut input = CompactGroupDecoder::new(bytes);
    if input.byte()? != 1 {
        return Ok(None);
    }
    let count = input.len()?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(match input.byte()? {
            0 => None,
            1 => Some(input.value()?),
            _ => return Ok(None),
        });
    }
    input.finish()?;
    Ok(Some(values))
}

struct CompactGroupDecoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> CompactGroupDecoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, count: usize) -> ExecutionResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| ExecutionError::evaluation("compact group partial length overflow"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| ExecutionError::evaluation("truncated compact group partial"))?;
        self.position = end;
        Ok(bytes)
    }

    fn byte(&mut self) -> ExecutionResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> ExecutionResult<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn var_u64(&mut self) -> ExecutionResult<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(ExecutionError::evaluation("compact group varint overflow"))
    }

    fn len(&mut self) -> ExecutionResult<usize> {
        usize::try_from(self.u64()?)
            .map_err(|_| ExecutionError::evaluation("compact group partial length exceeds usize"))
    }

    fn value(&mut self) -> ExecutionResult<Value> {
        Ok(match self.byte()? {
            0 => Value::null(),
            1 => Value::bool(false),
            2 => Value::bool(true),
            3 => Value::signed(i64::from_be_bytes(
                self.take(8)?.try_into().expect("eight bytes"),
            )),
            4 => Value::unsigned(self.u64()?),
            5 => Value::float(f64::from_bits(self.u64()?)).map_err(|error| {
                ExecutionError::evaluation(format!("invalid compact group float: {error}"))
            })?,
            6 => {
                let len = self.len()?;
                let text = std::str::from_utf8(self.take(len)?).map_err(|error| {
                    ExecutionError::evaluation(format!("invalid compact group string: {error}"))
                })?;
                Value::string(text)
            }
            7 => {
                let count = self.len()?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.value()?);
                }
                Value::array(values)
            }
            8 => {
                let count = self.len()?;
                let mut document = Document::new();
                for _ in 0..count {
                    let name_len = self.len()?;
                    let name = std::str::from_utf8(self.take(name_len)?).map_err(|error| {
                        ExecutionError::evaluation(format!("invalid compact group field: {error}"))
                    })?;
                    document.insert(name, self.value()?);
                }
                Value::object(document)
            }
            tag => {
                return Err(ExecutionError::evaluation(format!(
                    "invalid compact group value tag {tag}"
                )))
            }
        })
    }

    fn finish(self) -> ExecutionResult<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(ExecutionError::evaluation(
                "trailing compact group partial bytes",
            ))
        }
    }
}

fn stable_values_key(values: &[Option<Value>]) -> String {
    values
        .iter()
        .map(|value| format!("{value:?}"))
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

/// Writes the label of a pivot column: its values joined by `.`, or
/// `value` when the pivot has no column field.
fn write_column_label<'a>(output: &mut String, values: impl ExactSizeIterator<Item = Option<&'a Value>>) {
    use std::fmt::Write as _;
    if values.len() == 0 {
        output.push_str("value");
        return;
    }
    for (index, value) in values.enumerate() {
        if index > 0 {
            output.push('.');
        }
        let _ = match value {
            Some(Value::String(value)) => { output.push_str(value); Ok(()) }
            Some(Value::Bool(value)) => write!(output, "{value}"),
            Some(Value::Number(value)) => { output.push_str(&format_number(*value)); Ok(()) }
            Some(Value::Null) | None => { output.push_str("null"); Ok(()) }
            Some(other) => write!(output, "{other:?}"),
        };
    }
}

fn float_value(value: f64) -> ExecutionResult<Value> {
    Value::float(value)
        .map_err(|error| ExecutionError::evaluation(format!("invalid pivot number: {error}")))
}

fn format_number(value: Number) -> String {
    match value {
        Number::Signed(value) => value.to_string(),
        Number::Unsigned(value) => value.to_string(),
        Number::Float(value) => value.to_string(),
    }
}

fn split_projection_items(source: &str) -> ExecutionResult<Vec<&str>> {
    super::syntax::split_top_level(source, ',')
        .map_err(|message| ExecutionError::evaluation(format!("invalid select projection list: {message}")))
}

fn split_derive_assignments(source: &str) -> ExecutionResult<Vec<(&str, &str)>> {
    let parts = super::syntax::split_top_level(source, ',')
        .map_err(|message| ExecutionError::evaluation(format!("invalid derive assignment list: {message}")))?;
    parts
        .into_iter()
        .enumerate()
        .map(|(index, part)| {
            let Some((field, expression)) = part.split_once('=') else {
                return Err(ExecutionError::evaluation(format!(
                    "invalid derive assignment at index {index}: expected `field=expression`"
                )));
            };
            let field = field.trim();
            let expression = expression.trim();
            if field.is_empty() || expression.is_empty() {
                return Err(ExecutionError::evaluation(format!(
                    "invalid derive assignment at index {index}: field and expression are required"
                )));
            }
            if field.split('.').any(str::is_empty) {
                return Err(ExecutionError::evaluation(format!(
                    "invalid derive field path {field:?}"
                )));
            }
            Ok((field, expression))
        })
        .collect()
}

/// `derive` and `select … as` evaluate through the common expression evaluator.
#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::storage::{ScanOptions, StorageEngine, StorageResult, StoredDocument};

    struct CountingRead<'a> {
        inner: Box<dyn StorageRead + 'a>,
        gets: Cell<usize>,
    }

    impl StorageRead for CountingRead<'_> {
        fn get(
            &self,
            collection: &CollectionId,
            id: &DocumentId,
        ) -> StorageResult<Option<StoredDocument>> {
            self.gets.set(self.gets.get() + 1);
            self.inner.get(collection, id)
        }

        fn scan(
            &self,
            collection: &CollectionId,
            options: ScanOptions,
        ) -> StorageResult<Vec<StoredDocument>> {
            self.inner.scan(collection, options)
        }

        fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> {
            self.inner.collection_exists(collection)
        }

        fn collections(&self) -> StorageResult<Vec<CollectionId>> {
            self.inner.collections()
        }
    }

    #[test] fn derive_subtracts_fields_and_preserves_the_source_document() { let mut document = Document::new(); document.insert("CAFacture", Value::unsigned(120)); document.insert("COGS", Value::unsigned(45)); let result = RuntimeMaterializer::new() .materialize_custom("derive", "Marge=CAFacture-COGS", &document) .unwrap(); let CustomOperatorResult::Replace(result) = result else { panic!("derive should replace the result document"); }; assert_eq!(result.get("Marge"), Some(&Value::signed(75))); assert_eq!(document.get("Marge"), None); assert_eq!(document.get("CAFacture"), Some(&Value::unsigned(120))); }
    #[test] fn pivot_columns_sharing_a_label_share_their_cells() { use crate::query::logical_plan::{PivotAggregate, PivotSpecification, PivotValue}; let field = |name: &str| ExpressionFieldPath::new([name]).unwrap(); let specification = PivotSpecification::new([field("region")], [field("month")], [PivotValue::new(field("revenue"), PivotAggregate::Sum, None::<&str>).unwrap(), PivotValue::new(field("revenue"), PivotAggregate::Count, Some("n")).unwrap()]).unwrap(); let row = |region: &str, month: Value, revenue: Value| Arc::new(Document::from_fields([("region", Value::string(region)), ("month", month), ("revenue", revenue)])); let documents = [row("north", Value::signed(1), Value::signed(2)), row("north", Value::string("1"), Value::float(0.5).unwrap()), row("south", Value::Null, Value::signed(7)), row("north", Value::signed(2), Value::string("x"))]; let result = RuntimeMaterializer::new().materialize_pivot(&specification, &documents).unwrap(); assert_eq!(result.len(), 2); let north = result[0].document(); assert_eq!(north.get("region"), Some(&Value::string("north"))); assert_eq!(north.get("1.revenue"), Some(&Value::float(2.5).unwrap())); assert_eq!(north.get("1.n"), Some(&Value::unsigned(2))); assert_eq!(north.get("2.revenue"), Some(&Value::signed(0))); assert_eq!(north.get("2.n"), Some(&Value::unsigned(1))); let south = result[1].document(); assert_eq!(south.get("null.revenue"), Some(&Value::signed(7))); assert_eq!(south.len(), 3); }
    #[test] fn derive_supports_unary_positive_without_colliding_with_pivot_numeric_conversion() { let mut document = Document::new(); document.insert("amount", Value::signed(-12)); let result = RuntimeMaterializer::new() .materialize_custom("derive", "copy=+amount", &document) .unwrap(); let CustomOperatorResult::Replace(result) = result else { panic!("derive should replace the result document"); }; assert_eq!(result.get("copy"), Some(&Value::signed(-12))); }
    #[test] fn derive_applies_multiple_assignments_to_nested_targets() { let mut document = Document::new(); document.insert("revenue", Value::unsigned(200)); document.insert("cost", Value::unsigned(50)); let result = RuntimeMaterializer::new() .materialize_custom("derive",  "metrics.margin=revenue-cost, metrics.ratio=(revenue-cost)/revenue", &document, ) .unwrap(); let CustomOperatorResult::Replace(result) = result else { panic!("derive should replace the result document"); }; let metrics = result .get("metrics") .and_then(Value::as_object) .expect("derive should create the nested object"); assert_eq!(metrics.get("margin"), Some(&Value::signed(150))); assert_eq!(metrics.get("ratio"), Some(&Value::float(0.75).unwrap())); }
    #[test] fn derive_rejects_division_by_zero() { let mut document = Document::new(); document.insert("revenue", Value::unsigned(200)); document.insert("zero", Value::unsigned(0)); let error = RuntimeMaterializer::new() .materialize_custom("derive", "ratio=revenue/zero", &document) .unwrap_err(); assert!(error.to_string().contains("division by zero")); }
    #[test] fn root_replaces_with_nested_object() { let mut nested = Document::new(); nested.insert("answer", Value::unsigned(42)); let mut wrapper = Document::new(); wrapper.insert("nested", Value::object(nested)); let mut document = Document::new(); document.insert("payload", Value::object(wrapper)); let result = RuntimeMaterializer::new() .materialize_custom("root", "payload.nested", &document) .unwrap(); let CustomOperatorResult::Replace(result) = result else { panic!("root should replace the result document"); }; assert_eq!(result.get("answer"), Some(&Value::unsigned(42))); }
    #[test] fn root_rejects_missing_or_non_object_fields() { let materializer = RuntimeMaterializer::new(); let missing = materializer .materialize_custom("root", "payload", &Document::new()) .unwrap_err(); assert!(missing.to_string().contains("is missing")); let mut document = Document::new(); document.insert("payload", "not an object"); let scalar = materializer .materialize_custom("root", "payload", &document) .unwrap_err(); assert!(scalar.to_string().contains("must be an object")); }
    #[test] fn near_adds_cosine_distance_without_mutating_the_source_document() { let mut document = Document::new(); document.insert( "embedding", Value::array([Value::float(1.0).unwrap(), Value::float(0.0).unwrap()]), ); let result = RuntimeMaterializer::new() .materialize_custom("near", "embedding, [1.0, 0.0]", &document) .unwrap(); let CustomOperatorResult::Replace(result) = result else { panic!("near should replace the result document"); }; assert_eq!(result.get("_distance"), Some(&Value::float(0.0).unwrap())); assert_eq!(document.get("_distance"), None); }
    #[test] fn near_discards_documents_without_the_vector_field() { let result = RuntimeMaterializer::new() .materialize_custom("near", "embedding, [1.0, 0.0]", &Document::new()) .unwrap(); assert!(matches!(result, CustomOperatorResult::Discard)); }
    #[test] fn near_rejects_dimension_mismatches() { let mut document = Document::new(); document.insert( "embedding", Value::array([Value::float(1.0).unwrap(), Value::float(0.0).unwrap()]), ); let error = RuntimeMaterializer::new() .materialize_custom("near", "embedding, [1.0]", &document) .unwrap_err(); assert!(error.to_string().contains("near dimension mismatch")); }
    #[test] fn select_and_count_recent_handlers_materialize_expected_documents() { let mut document = Document::new(); document.insert("a", Value::unsigned(1)); document.insert("b", Value::unsigned(2)); let field = ExpressionFieldPath::new(["a"]).unwrap(); let selected = RuntimeMaterializer::new() .materialize_select(&[field], &document) .unwrap(); assert_eq!(selected.get("a"), Some(&Value::unsigned(1))); assert_eq!(selected.get("b"), None); let counted = RuntimeMaterializer::new() .materialize_count("count", 7) .unwrap(); assert_eq!(counted.get("count"), Some(&Value::unsigned(7))); }
    #[test] fn select_aliases_may_be_mixed_with_plain_fields() { let mut document = Document::new(); document.insert("CAFacture", Value::unsigned(100)); document.insert("COGS", Value::unsigned(40)); let result = RuntimeMaterializer::new() .materialize_custom("select", "CAFacture as CA, COGS", &document) .unwrap(); let CustomOperatorResult::Replace(result) = result else { panic!("select should replace the result document"); }; assert_eq!(result.get("CA"), Some(&Value::unsigned(100))); assert_eq!(result.get("COGS"), Some(&Value::unsigned(40))); assert_eq!(result.get("CAFacture"), None); }
    #[test] fn select_expression_can_reference_an_alias_defined_earlier() { let mut document = Document::new(); document.insert("CAFacture", Value::unsigned(100)); document.insert("COGS", Value::unsigned(40)); let result = RuntimeMaterializer::new() .materialize_custom( "select", "CAFacture as CA, COGS, CA - COGS as Marge", &document, ) .unwrap(); let CustomOperatorResult::Replace(result) = result else { panic!("select should replace the result document"); }; assert_eq!(result.get("CA"), Some(&Value::unsigned(100))); assert_eq!(result.get("COGS"), Some(&Value::unsigned(40))); assert_eq!(result.get("Marge"), Some(&Value::signed(60))); }
    #[test] fn lookup_keeps_outer_and_writes_empty_array() { let mut outer = Document::new(); outer.insert("name", Value::string("Alice")); let result = RuntimeMaterializer::new() .materialize_lookup("workspaces", &outer, &LookupDocuments::new([])) .unwrap(); assert_eq!(result.get("name"), Some(&Value::string("Alice"))); assert_eq!( result.get("workspaces").and_then(Value::as_array), Some(&[][..]) ); }
    #[test] fn rename_moves_a_top_level_field() { let mut document = Document::new(); document.insert("name", Value::string("Alice")); document.insert("age", Value::unsigned(42)); let result = RuntimeMaterializer::new() .materialize_custom("rename", "name as display_name", &document) .unwrap(); let CustomOperatorResult::Replace(result) = result else { panic!("rename should replace the document"); }; assert_eq!(result.get("name"), None); assert_eq!(result.get("display_name"), Some(&Value::string("Alice"))); assert_eq!(result.get("age"), Some(&Value::unsigned(42))); }
    #[test] fn rename_moves_a_nested_field_and_prunes_empty_parent() { let mut profile = Document::new(); profile.insert("name", Value::string("Alice")); let mut document = Document::new(); document.insert("profile", Value::object(profile)); let result = RuntimeMaterializer::new() .materialize_custom("rename", "profile.name as display_name", &document) .unwrap(); let CustomOperatorResult::Replace(result) = result else { panic!("rename should replace the document"); }; assert_eq!(result.get("profile"), None); assert_eq!(result.get("display_name"), Some(&Value::string("Alice"))); }
    #[test] fn rename_keeps_document_when_source_is_missing() { let document = Document::new(); let result = RuntimeMaterializer::new() .materialize_custom("rename", "missing as present", &document) .unwrap(); assert!(matches!(result, CustomOperatorResult::Keep)); }
    #[test] fn drop_removes_multiple_fields_and_prunes_empty_parents() { let mut profile = Document::new(); profile.insert("secret", Value::string("hidden")); let mut document = Document::new(); document.insert("age", Value::unsigned(42)); document.insert("name", Value::string("Alice")); document.insert("profile", Value::object(profile)); let result = RuntimeMaterializer::new() .materialize_custom("drop", "age, profile.secret", &document) .unwrap(); let CustomOperatorResult::Replace(result) = result else { panic!("drop should replace the document"); }; assert_eq!(result.get("age"), None); assert_eq!(result.get("profile"), None); assert_eq!(result.get("name"), Some(&Value::string("Alice"))); }
    #[test] fn drop_keeps_document_when_no_field_exists() { let document = Document::new(); let result = RuntimeMaterializer::new() .materialize_custom("drop", "missing, profile.secret", &document) .unwrap(); assert!(matches!(result, CustomOperatorResult::Keep)); }
    #[test] fn distinct_without_fields_uses_the_complete_document() { let mut first = Document::new(); first.insert("a", Value::unsigned(1)); first.insert("b", Value::unsigned(2)); let mut same = Document::new(); same.insert("b", Value::unsigned(2)); same.insert("a", Value::unsigned(1)); let mut different = Document::new(); different.insert("a", Value::unsigned(1)); different.insert("b", Value::unsigned(3)); let materializer = RuntimeMaterializer::new(); let first_key = materializer.materialize_distinct_key(&[], &first).unwrap(); let same_key = materializer.materialize_distinct_key(&[], &same).unwrap(); let different_key = materializer .materialize_distinct_key(&[], &different) .unwrap(); assert_eq!(first_key, same_key); assert_ne!(first_key, different_key); }
    #[test] fn distinct_fields_ignore_unselected_fields() { let mut first = Document::new(); first.insert("a", Value::unsigned(1)); first.insert("b", Value::unsigned(2)); let mut second = Document::new(); second.insert("a", Value::unsigned(1)); second.insert("b", Value::unsigned(99)); let fields = [ExpressionFieldPath::new(["a"]).unwrap()]; let materializer = RuntimeMaterializer::new(); assert_eq!( materializer .materialize_distinct_key(&fields, &first) .unwrap(), materializer .materialize_distinct_key(&fields, &second) .unwrap() ); }
    #[test] fn distinct_keeps_missing_separate_from_null() { let missing = Document::new(); let mut null = Document::new(); null.insert("a", Value::null()); let fields = [ExpressionFieldPath::new(["a"]).unwrap()]; let materializer = RuntimeMaterializer::new(); assert_ne!( materializer .materialize_distinct_key(&fields, &missing) .unwrap(), materializer .materialize_distinct_key(&fields, &null) .unwrap() ); }
    #[test] fn buffered_distinct_matches_allocating_key() { let mut document = Document::new(); document.insert("a", Value::unsigned(1)); document.insert("b", Value::string("two")); let materializer = RuntimeMaterializer::new(); let expected = materializer .materialize_distinct_key(&[], &document) .unwrap(); let mut key = Vec::new(); materializer .write_distinct_key(&[], &document, &mut key) .unwrap(); assert_eq!(expected.as_ref(), key.as_slice()); }
    #[test] fn projected_distinct_matches_document_key_for_explicit_fields() { let field = ExpressionFieldPath::new(["a"]).unwrap(); let fields = [field]; let mut document = Document::new(); document.insert("a", Value::string("hello")); let materializer = RuntimeMaterializer::new(); let expected = materializer .materialize_distinct_key(&fields, &document) .unwrap(); let values = [Some(crate::storage::ProjectedValueRef::String("hello"))]; let mut key = Vec::new(); materializer .write_projected_ref_distinct_key(&fields, &values, &[0], &mut key) .unwrap(); assert_eq!(expected.as_ref(), key.as_slice()); }
    #[test] fn projected_sort_matches_document_sort_semantics() { let field = ExpressionFieldPath::new(["a"]).unwrap(); let keys = [SortKey::ascending(field)]; let mut one = Document::new(); one.insert("a", Value::unsigned(1)); let mut two = Document::new(); two.insert("a", Value::unsigned(2)); let materializer = RuntimeMaterializer::new(); assert_eq!( materializer .materialize_projected_sort_comparison( &keys, &[Some(Value::unsigned(1))], &[Some(Value::unsigned(2))], ) .unwrap(), materializer .materialize_sort_comparison(&keys, &one, &two) .unwrap(), ); }
    #[test] fn sort_compares_ascending_and_descending_values() { let mut one = Document::new(); one.insert("a", Value::unsigned(1)); let mut two = Document::new(); two.insert("a", Value::unsigned(2)); let field = ExpressionFieldPath::new(["a"]).unwrap(); let materializer = RuntimeMaterializer::new(); assert_eq!( materializer .materialize_sort_comparison(&[SortKey::ascending(field.clone())], &one, &two) .unwrap(), Ordering::Less ); assert_eq!( materializer .materialize_sort_comparison(&[SortKey::descending(field)], &one, &two) .unwrap(), Ordering::Greater ); }
    #[test] fn sort_uses_later_keys_when_values_are_equal() { let mut left = Document::new(); left.insert("a", Value::unsigned(1)); left.insert("b", Value::unsigned(2)); let mut right = Document::new(); right.insert("a", Value::unsigned(1)); right.insert("b", Value::unsigned(3)); let keys = [ SortKey::ascending(ExpressionFieldPath::new(["a"]).unwrap()), SortKey::ascending(ExpressionFieldPath::new(["b"]).unwrap()), ]; assert_eq!( RuntimeMaterializer::new() .materialize_sort_comparison(&keys, &left, &right) .unwrap(), Ordering::Less ); }
    #[test] fn sort_orders_null_values_like_missing_values() { let mut null = Document::new(); null.insert("a", Value::Null); let mut present = Document::new(); present.insert("a", Value::unsigned(1)); let field = ExpressionFieldPath::new(["a"]).unwrap(); assert_eq!( RuntimeMaterializer::new() .materialize_sort_comparison( &[SortKey::ascending(field.clone())], &null, &present, ) .unwrap(), Ordering::Less ); assert_eq!( RuntimeMaterializer::new() .materialize_projected_sort_comparison(&[SortKey::descending(field)], &[Some(Value::Null)], &[Some(Value::unsigned(1))],) .unwrap(), Ordering::Greater ); }
    #[test] fn sort_orders_missing_values_deterministically() { let missing = Document::new(); let mut present = Document::new(); present.insert("a", Value::unsigned(1)); let field = ExpressionFieldPath::new(["a"]).unwrap(); assert_eq!( RuntimeMaterializer::new() .materialize_sort_comparison( &[SortKey::ascending(field.clone())], &missing, &present, ) .unwrap(), Ordering::Less ); assert_eq!( RuntimeMaterializer::new() .materialize_sort_comparison(&[SortKey::descending(field)], &missing, &present,) .unwrap(), Ordering::Greater ); }
    #[test] fn borrowed_path_value_reads_nested_fields_without_materializing_a_clone() { let mut nested = Document::new(); nested.insert("value", Value::unsigned(42)); let mut document = Document::new(); document.insert("nested", Value::object(nested)); let path = ExpressionFieldPath::new(["nested", "value"]).unwrap(); let value = path_value(&document, &path).expect("path exists"); assert_eq!(value, &Value::unsigned(42)); }
    #[test] fn group_field_layout_resolves_aliased_keys_to_source_fields() { let fields = [ ExpressionFieldPath::new(["__og_group_key_61727469636c65_436c69656e74"]).unwrap(), ExpressionFieldPath::new(["__og_group_sum_6361_546f74616c"]).unwrap(), ]; let (grouping, required) = group_field_layout(&fields).unwrap(); assert_eq!( grouping, vec![ExpressionFieldPath::new(["article"]).unwrap()] ); assert_eq!( required, vec![ ExpressionFieldPath::new(["article"]).unwrap(), ExpressionFieldPath::new(["ca"]).unwrap(), ] ); }
    #[test] fn group_materializes_aliased_key_without_changing_group_identity() { let mut first = Document::new(); first.insert("article", Value::string("A")); let mut second = Document::new(); second.insert("article", Value::string("A")); let fields = [ExpressionFieldPath::new(["__og_group_key_61727469636c65_436c69656e74"]).unwrap()]; let documents = vec![Arc::new(first), Arc::new(second)]; let groups = RuntimeMaterializer::new() .materialize_group(&fields, &documents) .unwrap(); assert_eq!(groups.len(), 1); assert_eq!( groups[0].document().get("Client"), Some(&Value::string("A")) ); assert!(groups[0].document().get("article").is_none()); assert_eq!(groups[0].document().get("count"), Some(&Value::unsigned(2))); }
    #[test] fn incremental_group_materializes_aliased_key() { let fields = [ExpressionFieldPath::new(["__og_group_key_61727469636c65_436c69656e74"]).unwrap()]; let mut accumulator = RuntimeMaterializer::new() .incremental_group(&fields) .unwrap(); let mut document = Document::new(); document.insert("article", Value::string("A")); accumulator.push(&document).unwrap(); let grouped = accumulator.finish(1).unwrap(); assert_eq!(grouped.document().get("Client"), Some(&Value::string("A"))); assert!(grouped.document().get("article").is_none()); }
    #[test] fn group_field_layout_resolves_sum_markers_to_source_fields() { let fields = [ ExpressionFieldPath::new(["tPeriode"]).unwrap(), ExpressionFieldPath::new(["__og_group_sum_434146616374757265_434146616374757265"]) .unwrap(), ]; let (grouping, required) = group_field_layout(&fields).unwrap(); assert_eq!( grouping, vec![ExpressionFieldPath::new(["tPeriode"]).unwrap()] ); assert_eq!( required, vec![ ExpressionFieldPath::new(["tPeriode"]).unwrap(), ExpressionFieldPath::new(["CAFacture"]).unwrap(), ] ); }
    #[test] fn incremental_group_folds_summable_values_without_retaining_rows() { let fields = [ ExpressionFieldPath::new(["period"]).unwrap(), ExpressionFieldPath::new(["__og_group_sum_6361_6361"]).unwrap(), ]; let mut accumulator = RuntimeMaterializer::new() .incremental_group(&fields) .unwrap(); for _ in 0..100_000 { let mut document = Document::new(); document.insert("period", Value::string("2026-01")); document.insert("ca", Value::unsigned(2)); accumulator.push(&document).unwrap(); } let grouped = accumulator.finish(1).unwrap(); assert_eq!( grouped.document().get("period"), Some(&Value::string("2026-01")) ); assert_eq!( grouped.document().get("ca"), Some(&Value::signed(200_000)) ); assert!(grouped.document().get("count").is_none()); }
    #[test] fn group_counts_documents_by_one_key() { let mut first = Document::new(); first.insert("article", Value::string("A")); let mut second = Document::new(); second.insert("article", Value::string("A")); let mut third = Document::new(); third.insert("article", Value::string("B")); let documents = vec![Arc::new(first), Arc::new(second), Arc::new(third)]; let keys = [ExpressionFieldPath::new(["article"]).unwrap()]; let groups = RuntimeMaterializer::new() .materialize_group(&keys, &documents) .unwrap(); assert_eq!(groups.len(), 2); assert_eq!( groups[0].document().get("article"), Some(&Value::string("A")) ); assert_eq!(groups[0].document().get("count"), Some(&Value::unsigned(2))); assert_eq!( groups[1].document().get("article"), Some(&Value::string("B")) ); assert_eq!(groups[1].document().get("count"), Some(&Value::unsigned(1))); }
    #[test] fn group_preserves_nested_key_paths() { let mut address = Document::new(); address.insert("country", Value::string("FR")); let mut document = Document::new(); document.insert("address", Value::object(address)); let documents = vec![Arc::new(document)]; let keys = [ExpressionFieldPath::new(["address", "country"]).unwrap()]; let groups = RuntimeMaterializer::new() .materialize_group(&keys, &documents) .unwrap(); let address = groups[0] .document() .get("address") .and_then(Value::as_object) .expect("nested group key should be materialized as an object"); assert_eq!(address.get("country"), Some(&Value::string("FR"))); assert_eq!(groups[0].document().get("count"), Some(&Value::unsigned(1))); }
    #[test] fn group_keeps_missing_distinct_from_physical_null() { let missing = Document::new(); let mut null = Document::new(); null.insert("article", Value::null()); let documents = vec![Arc::new(missing), Arc::new(null)]; let keys = [ExpressionFieldPath::new(["article"]).unwrap()]; let groups = RuntimeMaterializer::new() .materialize_group(&keys, &documents) .unwrap(); assert_eq!(groups.len(), 2); assert!(groups.iter().all(|group| { group.document().get("article") == Some(&Value::null()) && group.document().get("count") == Some(&Value::unsigned(1)) })); }
    #[test] fn group_sums_runtime_summable_values() { let mut first = Document::new(); first.insert("article", Value::string("A")); first.insert("ca", Value::unsigned(10)); let mut second = Document::new(); second.insert("article", Value::string("A")); second.insert("ca", Value::signed(5)); let marker = ExpressionFieldPath::new(["__og_group_sum_6361_546f74616c5f4341"]).unwrap(); let fields = [ExpressionFieldPath::new(["article"]).unwrap(), marker]; let documents = vec![Arc::new(first), Arc::new(second)]; let groups = RuntimeMaterializer::new() .materialize_group(&fields, &documents) .unwrap(); assert_eq!(groups.len(), 1); assert_eq!( groups[0].document().get("article"), Some(&Value::string("A")) ); assert_eq!( groups[0].document().get("Total_CA"), Some(&Value::signed(15)) ); assert!(groups[0].document().get("count").is_none()); }
    fn measure_marker(aggregate: &str, source: &str, alias: &str) -> ExpressionFieldPath { let hex = |text: &str| text.bytes().map(|byte| format!("{byte:02x}")).collect::<String>(); ExpressionFieldPath::new([format!("__og_group_{aggregate}_{}_{}", hex(source), hex(alias))]).unwrap() }
    fn measured_rows() -> Vec<Arc<Document>> { [(Value::signed(4), "b"), (Value::float(1.5).unwrap(), "a"), (Value::null(), "c")].into_iter().map(|(qty, label)| { let mut document = Document::new(); document.insert("k", Value::string("K")); document.insert("qty", qty); document.insert("label", Value::string(label)); Arc::new(document) }).collect() }
    fn measure_fields() -> Vec<ExpressionFieldPath> { vec![ ExpressionFieldPath::new(["k"]).unwrap(), measure_marker("sum", "qty", "total"), measure_marker("avg", "qty", "mean"), measure_marker("min", "label", "low"), measure_marker("max", "qty", "high"), measure_marker("first", "label", "head"), measure_marker("last", "qty", "tail"), measure_marker("count", "qty", "known"), measure_marker("count", "", "rows") ] }
    #[test] fn projected_refs_fold_like_documents() { use crate::storage::ProjectedValueRef as R; let extra = [Value::signed(2), Value::unsigned(3)].map(|qty| { let mut document = Document::new(); document.insert("k", Value::string("K")); document.insert("qty", qty); document.insert("label", Value::string("d")); Arc::new(document) }); let documents = measured_rows().into_iter().chain(extra).collect::<Vec<_>>(); let refs = [(R::Signed(4), "b"), (R::Float(1.5), "a"), (R::Null, "c"), (R::Owned(Value::signed(2)), "d"), (R::Unsigned(3), "d")]; let fields = measure_fields(); let materializer = RuntimeMaterializer::new(); let mut borrowed = materializer.incremental_group(&fields).unwrap(); for (qty, label) in refs { assert!(borrowed.push_projected_value_refs(&[Some(R::String("K")), Some(qty), Some(R::String(label))], &[0, 1, 2]).unwrap()); } let mut owned = materializer.incremental_group(&fields).unwrap(); for document in &documents { owned.push(document).unwrap(); } assert_eq!(borrowed.finish(1).unwrap().document(), owned.finish(1).unwrap().document()); }
    #[test] fn group_computes_every_measure_kind() { let groups = RuntimeMaterializer::new().materialize_group(&measure_fields(), &measured_rows()).unwrap(); let row = groups[0].document(); assert_eq!(row.get("total"), Some(&Value::float(5.5).unwrap())); assert_eq!(row.get("mean"), Some(&Value::float(2.75).unwrap())); assert_eq!(row.get("low"), Some(&Value::string("a"))); assert_eq!(row.get("high"), Some(&Value::signed(4))); assert_eq!(row.get("head"), Some(&Value::string("b"))); assert_eq!(row.get("tail"), Some(&Value::null())); assert_eq!(row.get("known"), Some(&Value::unsigned(2))); assert_eq!(row.get("rows"), Some(&Value::unsigned(3))); assert!(row.get("count").is_none()); let (_, required) = group_field_layout(&measure_fields()).unwrap(); assert_eq!(required.len(), 3); }
    #[test] fn compact_partials_merge_every_measure_kind() { let rows = measured_rows(); let fields = measure_fields(); let materializer = RuntimeMaterializer::new(); let mut early = materializer.incremental_group(&fields).unwrap(); early.push(&rows[0]).unwrap(); let mut late = materializer.incremental_group(&fields).unwrap(); late.push(&rows[1]).unwrap(); late.push(&rows[2]).unwrap(); let mut merged = materializer.incremental_group(&fields).unwrap(); for partial in [early.compact_partial().unwrap().unwrap(), late.compact_partial().unwrap().unwrap()] { assert!(merged.merge_compact_partial(&partial).unwrap()); } merged.push(&rows[0]).unwrap(); let expected = materializer.materialize_group(&fields, &[Arc::clone(&rows[0]), Arc::clone(&rows[1]), Arc::clone(&rows[2]), Arc::clone(&rows[0])]).unwrap(); let mut actual = merged.finish(1).unwrap().document().clone(); actual.remove("k"); let mut wanted = expected[0].document().clone(); wanted.remove("k"); assert_eq!(actual, wanted); assert_eq!(wanted.get("tail"), Some(&Value::signed(4))); }
    #[test] fn integer_sums_fall_back_to_float_on_overflow() { let rows = [i64::MAX, 1].map(|value| { let mut document = Document::new(); document.insert("v", Value::signed(value)); Arc::new(document) }); let groups = RuntimeMaterializer::new().materialize_group(&[measure_marker("sum", "v", "v")], &rows).unwrap(); assert!(matches!(groups[0].document().get("v"), Some(Value::Number(Number::Float(_))))); }
    #[test] fn group_falls_back_to_count_when_measure_is_not_summable() { let mut first = Document::new(); first.insert("article", Value::string("A")); first.insert("label", Value::string("x")); let mut second = Document::new(); second.insert("article", Value::string("A")); second.insert("label", Value::string("y")); let marker = ExpressionFieldPath::new(["__og_group_sum_6c6162656c_4c6162656c"]).unwrap(); let fields = [ExpressionFieldPath::new(["article"]).unwrap(), marker]; let documents = vec![Arc::new(first), Arc::new(second)]; let groups = RuntimeMaterializer::new() .materialize_group(&fields, &documents) .unwrap(); assert_eq!(groups.len(), 1); assert_eq!(groups[0].document().get("count"), Some(&Value::unsigned(2))); assert!(groups[0].document().get("Label").is_none()); }
    #[test] fn group_can_sum_fields_created_by_previous_stages() { let mut first = Document::new(); first.insert("article", Value::string("A")); first.insert("calculated", Value::float(2.5).unwrap()); let mut second = Document::new(); second.insert("article", Value::string("A")); second.insert("calculated", Value::float(1.5).unwrap()); let marker = ExpressionFieldPath::new(["__og_group_sum_63616c63756c61746564_546f74616c"]).unwrap(); let fields = [ExpressionFieldPath::new(["article"]).unwrap(), marker]; let documents = vec![Arc::new(first), Arc::new(second)]; let groups = RuntimeMaterializer::new() .materialize_group(&fields, &documents) .unwrap(); assert_eq!( groups[0].document().get("Total"), Some(&Value::float(4.0).unwrap()) ); }
    #[test] fn group_without_keys_counts_the_complete_input() { let documents = vec![Arc::new(Document::new()), Arc::new(Document::new())]; let groups = RuntimeMaterializer::new() .materialize_group(&[], &documents) .unwrap(); assert_eq!(groups.len(), 1); assert_eq!(groups[0].document().get("count"), Some(&Value::unsigned(2))); }
    #[test] fn unwind_expands_array_values() { let mut document = Document::new(); document.insert("items", Value::array([Value::signed(1), Value::signed(2)])); let result = RuntimeMaterializer::new() .materialize_custom("unwind", "items", &document) .unwrap(); assert!(matches!(result, CustomOperatorResult::Expand(values) if values.len() == 2)); }
    #[test] fn first_projection_selects_requested_field() { let mut document = Document::new(); document.insert("Article_Code", Value::string("A")); document.insert("other", Value::signed(1)); let result = RuntimeMaterializer::new() .materialize_custom("first", "Article_Code", &document) .unwrap(); assert!( matches!(result, CustomOperatorResult::Replace(value) if value.get("Article_Code").is_some() && value.get("other").is_none()) ); }
    #[test] fn streaming_load_generated_ids_skip_existing_document_lookups() { let storage = crate::storage::MemoryStorage::new(); let read = CountingRead { inner: storage.read().unwrap(), gets: Cell::new(0), }; let collection = CollectionId::parse("data").unwrap(); let materializer = StreamingLoadMaterializer::default(); let chunks = [Arc::<str>::from(r#"[{"name":"Ada"},{"name":"Grace"}]"#)]; let mutations = materializer .materialize(&collection, &read, PhysicalLoadMode::Merge, &chunks) .unwrap(); assert_eq!(mutations.len(), 2); assert_eq!(read.gets.get(), 0); }
    #[test] fn streaming_load_mixed_explicit_and_generated_ids_preserves_reserved_ids() { let storage = crate::storage::MemoryStorage::new(); let read = storage.read().unwrap(); let collection = CollectionId::parse("data").unwrap(); let materializer = StreamingLoadMaterializer::default(); let chunks = [Arc::<str>::from( r#"[{"_id":"019fb7ae-9588-7057-830a-01bdb143b7ce","name":"Ada"},{"name":"Grace"}]"#, )]; let mutations = materializer .materialize(&collection, read.as_ref(), PhysicalLoadMode::Merge, &chunks) .unwrap(); assert_eq!(mutations.len(), 2); assert!(matches!(mutations[0], StreamingLoadMutation::Insert { .. })); assert!(matches!(mutations[1], StreamingLoadMutation::Insert { .. })); }
    #[test] fn streaming_load_merge_inserts_new_documents() { let storage = crate::storage::MemoryStorage::new(); let read = storage.read().unwrap(); let collection = CollectionId::parse("data").unwrap(); let materializer = StreamingLoadMaterializer::default(); let chunks = [Arc::<str>::from(r#"[{"name":"Ada","age":36}]"#)]; let mutations = materializer .materialize(&collection, read.as_ref(), PhysicalLoadMode::Merge, &chunks) .unwrap(); assert_eq!(mutations.len(), 1); assert!( matches!(&mutations[0], StreamingLoadMutation::Insert { document, .. } if document.get("name").and_then(Value::as_str) == Some("Ada")) ); }
    #[test] fn streaming_load_rejects_non_array_chunks() { let storage = crate::storage::MemoryStorage::new(); let read = storage.read().unwrap(); let collection = CollectionId::parse("data").unwrap(); let materializer = StreamingLoadMaterializer::default(); let chunks = [Arc::<str>::from(r#"{"name":"Ada"}"#)]; let error = materializer .materialize(&collection, read.as_ref(), PhysicalLoadMode::Merge, &chunks) .unwrap_err(); assert!(error.to_string().contains("must be a JSON array")); }
}
