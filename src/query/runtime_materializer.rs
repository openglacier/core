//! Default document operator materialization.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::{
    compare,
    model::parse_number_value,
    model::CoercionPolicy,
    model::Document,
    model::Number,
    model::Value,
    storage::{CollectionId, DocumentId, StorageRead, UuidV7Generator, VersionPrecondition},
    ValueCapabilities,
};

use super::{
    parse_expression, BinaryOperator, CustomOperatorResult, ExecutionError, ExecutionResult,
    Expression, ExpressionFieldPath, ExpressionView, IncrementalGroupAccumulator,
    InsertDocumentMaterializer, Literal, LookupDocuments, PhysicalLoadMode, PivotAggregate,
    PivotSpecification, PivotValue, QueryRuntime, SortDirection, SortKey, StreamingLoadMutation,
    SyntheticDocument, UnaryOperator,
};

/// Materializes lookup arrays and pivot result documents for the default daemon.
#[derive(Clone, Copy, Debug, Default)]
pub struct RuntimeMaterializer;

/// Shared composition of the default physical document operators.
///
/// This trait keeps daemon and embedding code independent from the concrete
/// materializer types. Predicate evaluation and `set` remain supplied by the
/// expression runtime; this extension installs only the document-level
/// operations shared by the standard in-memory engine.
pub trait QueryRuntimeMaterializationExt {
    /// Installs the standard lookup, load, pivot, insert and custom handlers.
    ///
    /// Generated insert identifiers use `insert_id_prefix`.
    #[must_use]
    fn with_default_materialization(self, insert_id_prefix: impl Into<Arc<str>>) -> QueryRuntime;
}

impl QueryRuntimeMaterializationExt for QueryRuntime {
    fn with_default_materialization(self, insert_id_prefix: impl Into<Arc<str>>) -> QueryRuntime {
        let materializer = RuntimeMaterializer::new();
        let lookup_materializer = materializer;
        let pivot_materializer = materializer;
        let group_materializer = materializer;
        let prefix = insert_id_prefix.into();
        let insert_materializer = InsertDocumentMaterializer::new(Arc::clone(&prefix));
        let streaming_materializer = StreamingLoadMaterializer::new(prefix);

        self.with_streaming_load(move |collection, storage, mode, chunks| {
            streaming_materializer.materialize(collection, storage, mode, chunks)
        })
        .with_load(|_expression, document| Ok(Arc::new(document.clone())))
        .with_compare(move |keys, left, right| {
            materializer.materialize_sort_comparison(keys, left, right)
        })
        .with_select(move |fields, document| materializer.materialize_select(fields, document))
        .with_distinct(move |fields, document| {
            materializer.materialize_distinct_key(fields, document)
        })
        .with_count(move |alias, count| materializer.materialize_count(alias, count))
        .with_group(move |keys, documents| group_materializer.materialize_group(keys, documents))
        .with_incremental_group(move |keys| group_materializer.incremental_group(keys))
        .with_lookup(move |into, outer, matches| {
            lookup_materializer.materialize_lookup(into, outer, matches)
        })
        .with_pivot(move |specification, documents| {
            pivot_materializer.materialize_pivot(specification, documents)
        })
        .with_insert(move |document| insert_materializer.materialize(document))
        .with_custom(move |name, arguments, _writes, document| {
            materializer.materialize_custom(name.as_str(), arguments, document)
        })
    }
}

#[derive(Clone, Debug)]
struct StreamingLoadMaterializer {
    id_generator: UuidV7Generator,
}

impl StreamingLoadMaterializer {
    fn new(generated_id_prefix: Arc<str>) -> Self {
        let _ = generated_id_prefix;
        Self {
            id_generator: UuidV7Generator::new(),
        }
    }

    fn materialize(
        &self,
        collection: &CollectionId,
        storage: &dyn StorageRead,
        mode: PhysicalLoadMode,
        chunks: &[Arc<str>],
    ) -> ExecutionResult<Vec<StreamingLoadMutation>> {
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
                            &id.to_string()
                        )));
                    }
                    mutations.push(StreamingLoadMutation::insert(id, Arc::new(incoming)));
                    continue;
                }

                if !seen.insert(id.clone()) {
                    return Err(ExecutionError::mutation(format!(
                        "duplicate streaming-load identifier {:?} in one request",
                        &id.to_string()
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
                        for (name, value) in incoming.iter() {
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
                            &id.to_string()
                        )));
                    }
                };
                mutations.push(mutation);
            }
        }

        Ok(mutations)
    }

    fn materialize_row(
        &self,
        object: &serde_json::Map<String, serde_json::Value>,
        generated_id: Option<DocumentId>,
    ) -> ExecutionResult<(DocumentId, Document)> {
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
                document.insert(name.as_str(), json_value(value)?);
            }
        }
        Ok((id, document))
    }
}

fn json_value(value: &serde_json::Value) -> ExecutionResult<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::null()),
        serde_json::Value::Bool(value) => Ok(Value::bool(*value)),
        serde_json::Value::String(value) => Ok(Value::string(value.as_str())),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::signed(value))
            } else if let Some(value) = value.as_u64() {
                Ok(Value::unsigned(value))
            } else if let Some(value) = value.as_f64() {
                Number::float(value)
                    .map(Number::into_value)
                    .map_err(|error| ExecutionError::mutation(error.to_string()))
            } else {
                Err(ExecutionError::mutation("unsupported JSON number"))
            }
        }
        serde_json::Value::Array(values) => values
            .iter()
            .map(json_value)
            .collect::<ExecutionResult<Vec<_>>>()
            .map(Value::array),
        serde_json::Value::Object(values) => {
            let mut document = Document::new();
            for (name, value) in values {
                document.insert(name.as_str(), json_value(value)?);
            }
            Ok(Value::object(document))
        }
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
    pub fn materialize_custom(
        &self,
        name: &str,
        arguments: &str,
        document: &Document,
    ) -> ExecutionResult<CustomOperatorResult> {
        match name {
            "select" => self.materialize_select_aliases(arguments, document),
            "rename" => self.materialize_rename(arguments, document),
            "drop" => self.materialize_drop(arguments, document),
            "derive" => self.materialize_derive(arguments, document),
            "first" | "single" => self.materialize_scalar_projection(arguments, document),
            "unwind" => self.materialize_unwind(arguments, document),
            _ => Ok(CustomOperatorResult::Keep),
        }
    }

    fn materialize_scalar_projection(
        &self,
        arguments: &str,
        document: &Document,
    ) -> ExecutionResult<CustomOperatorResult> {
        let field = arguments.trim();
        if field.is_empty() {
            return Ok(CustomOperatorResult::Keep);
        }
        let path = ExpressionFieldPath::new(field.split('.'))
            .map_err(|error| ExecutionError::evaluation(error.to_string()))?;
        Ok(CustomOperatorResult::Replace(
            self.materialize_select(&[path], document)?,
        ))
    }

    fn materialize_unwind(
        &self,
        arguments: &str,
        document: &Document,
    ) -> ExecutionResult<CustomOperatorResult> {
        let path = ExpressionFieldPath::new(arguments.trim().split('.'))
            .map_err(|error| ExecutionError::evaluation(error.to_string()))?;
        let Some(value) = cloned_path_value(document, &path) else {
            return Ok(CustomOperatorResult::Keep);
        };
        let Value::Array(values) = value else {
            return Ok(CustomOperatorResult::Keep);
        };
        let segments = path.iter().collect::<Vec<_>>();
        let documents = values
            .iter()
            .cloned()
            .map(|value| {
                let mut result = document.clone();
                insert_path(&mut result, &segments, value);
                Arc::new(result)
            })
            .collect();
        Ok(CustomOperatorResult::Expand(documents))
    }

    /// Derives one or more fields without mutating stored documents.
    pub fn materialize_derive(
        &self,
        arguments: &str,
        document: &Document,
    ) -> ExecutionResult<CustomOperatorResult> {
        let assignments = split_derive_assignments(arguments)?;
        let mut result = document.clone();

        for (field, expression_source) in assignments {
            let expression = parse_expression(expression_source).map_err(|error| {
                ExecutionError::evaluation(format!(
                    "invalid derive expression {expression_source:?}: {error}"
                ))
            })?;
            let value = evaluate_derive_expression(&expression, document)?;
            let path = field.split('.').collect::<Vec<_>>();
            insert_path(&mut result, &path, value);
        }

        Ok(CustomOperatorResult::Replace(Arc::new(result)))
    }

    /// Compares two documents according to the ordered sort keys.
    ///
    /// Missing values sort before present values in ascending order and after
    /// present values in descending order. Present values use OG's numeric
    /// coercion policy, while incompatible physical kinds produce an explicit
    /// execution error.
    pub fn materialize_sort_comparison(
        &self,
        keys: &[SortKey],
        left: &Document,
        right: &Document,
    ) -> ExecutionResult<Ordering> {
        for key in keys {
            let left_value = path_value(left, key.field());
            let right_value = path_value(right, key.field());

            let ordering = match (left_value, right_value) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(left), Some(right)) => compare(left, right, CoercionPolicy::Numeric)
                    .map_err(|error| {
                        ExecutionError::evaluation(format!(
                            "cannot sort field {}: {error}",
                            key.field()
                        ))
                    })?
                    .into_ordering(),
            };

            if ordering != Ordering::Equal {
                return Ok(match key.direction() {
                    SortDirection::Ascending => ordering,
                    SortDirection::Descending => ordering.reverse(),
                });
            }
        }

        Ok(Ordering::Equal)
    }

    /// Projects a document to the requested field paths.
    pub fn materialize_select(
        &self,
        fields: &[ExpressionFieldPath],
        document: &Document,
    ) -> ExecutionResult<Arc<Document>> {
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
    pub fn materialize_distinct_key(
        &self,
        fields: &[ExpressionFieldPath],
        document: &Document,
    ) -> ExecutionResult<Arc<[u8]>> {
        let mut key = Vec::new();

        if fields.is_empty() {
            key.push(0);
            encode_document(&mut key, document);
        } else {
            key.push(1);
            encode_len(&mut key, fields.len());

            for field in fields {
                match path_value(document, field) {
                    Some(value) => {
                        key.push(1);
                        encode_value(&mut key, value);
                    }
                    None => key.push(0),
                }
            }
        }

        Ok(Arc::from(key))
    }

    /// Encodes the explicit-field equality key directly into a reusable
    /// caller-owned buffer. This avoids one key allocation per source row.
    pub(crate) fn write_projected_distinct_key_indexes(
        &self,
        values: &[Option<Value>],
        indexes: &[usize],
        key: &mut Vec<u8>,
    ) {
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
    pub(crate) fn write_projected_ref_distinct_key_indexes(
        &self,
        values: &[Option<crate::storage::ProjectedValueRef<'_>>],
        indexes: &[usize],
        key: &mut Vec<u8>,
    ) {
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

    /// Creates the bounded, capability-driven aggregate state used by the
    /// external group executor. `Summable` values are folded immediately; no
    /// source document is retained.
    pub fn incremental_group(
        &self,
        fields: &[ExpressionFieldPath],
    ) -> ExecutionResult<Box<dyn IncrementalGroupAccumulator>> {
        Ok(Box::new(DefaultIncrementalGroupAccumulator::new(fields)?))
    }

    /// Groups documents by key paths and optional encoded sum measures.
    ///
    /// Plain paths are grouping keys. Internal marker paths generated by the
    /// planner represent sum candidates. A candidate is aggregated only when
    /// its runtime value exposes the `Summable` capability. If no candidate is
    /// summable for a group, the result falls back to the row `count`.
    pub fn materialize_group(
        &self,
        fields: &[ExpressionFieldPath],
        documents: &[Arc<Document>],
    ) -> ExecutionResult<Vec<SyntheticDocument>> {
        #[derive(Debug)]
        struct GroupKey {
            source: ExpressionFieldPath,
            output: ExpressionFieldPath,
        }

        #[derive(Debug)]
        struct GroupMeasure {
            source: ExpressionFieldPath,
            alias: String,
        }

        #[derive(Debug)]
        struct GroupBucket {
            values: Vec<Option<Value>>,
            count: u64,
            sums: Vec<f64>,
            seen_summable: Vec<bool>,
        }

        let mut keys = Vec::new();
        let mut measures = Vec::new();

        for field in fields {
            if let Some((source, alias)) = decode_group_sum_marker(field)? {
                measures.push(GroupMeasure { source, alias });
            } else if let Some((source, output)) = decode_group_key_marker(field)? {
                keys.push(GroupKey { source, output });
            } else {
                keys.push(GroupKey {
                    source: field.clone(),
                    output: field.clone(),
                });
            }
        }

        let mut groups: BTreeMap<Vec<u8>, GroupBucket> = BTreeMap::new();

        for document in documents {
            let values = keys
                .iter()
                .map(|key| cloned_path_value(document, &key.source))
                .collect::<Vec<_>>();

            let mut encoded = Vec::new();
            encode_len(&mut encoded, values.len());
            for value in &values {
                match value {
                    Some(value) => {
                        encoded.push(1);
                        encode_value(&mut encoded, value);
                    }
                    None => encoded.push(0),
                }
            }

            let bucket = groups.entry(encoded).or_insert_with(|| GroupBucket {
                values,
                count: 0,
                sums: vec![0.0; measures.len()],
                seen_summable: vec![false; measures.len()],
            });

            bucket.count = bucket.count.checked_add(1).ok_or_else(|| {
                ExecutionError::evaluation("group count exceeds the supported u64 range")
            })?;

            for (index, measure) in measures.iter().enumerate() {
                let Some(value) = cloned_path_value(document, &measure.source) else {
                    continue;
                };

                if value.is_summable() {
                    bucket.sums[index] += numeric_value(&value)?;
                    bucket.seen_summable[index] = true;
                }
            }
        }

        groups
            .into_values()
            .enumerate()
            .map(|(index, bucket)| {
                let mut document = Document::new();

                for (key, value) in keys.iter().zip(bucket.values) {
                    let path = key.output.iter().collect::<Vec<_>>();
                    insert_path(&mut document, &path, value.unwrap_or_else(Value::null));
                }

                let mut emitted_sum = false;
                for (measure_index, measure) in measures.iter().enumerate() {
                    if bucket.seen_summable[measure_index] {
                        let value = Value::float(bucket.sums[measure_index]).map_err(|error| {
                            ExecutionError::evaluation(format!(
                                "group sum cannot be represented as a finite number: {error}"
                            ))
                        })?;
                        document.insert(measure.alias.as_str(), value);
                        emitted_sum = true;
                    }
                }

                if !emitted_sum {
                    document.insert("count", Value::unsigned(bucket.count));
                }

                let id = DocumentId::synthetic(GROUP_NAMESPACE, index as u64 + 1);

                Ok(SyntheticDocument::new(id, Arc::new(document)))
            })
            .collect()
    }

    fn materialize_select_aliases(
        &self,
        arguments: &str,
        document: &Document,
    ) -> ExecutionResult<CustomOperatorResult> {
        let mut result = Document::new();
        let mut scope = document.clone();

        for (item_index, item) in split_projection_items(arguments)?.into_iter().enumerate() {
            let item = item.trim();
            if item.is_empty() {
                return Err(ExecutionError::evaluation(format!(
                    "invalid select projection at index {item_index}: projection is empty"
                )));
            }

            let (source_text, target_text, expression_projection) = match item.rsplit_once(" as ") {
                Some((source, target)) => (source.trim(), target.trim(), true),
                None => (item, item, false),
            };

            let target = parse_runtime_path(target_text, "select target")?;
            let value = if expression_projection {
                let expression = parse_expression(source_text).map_err(|error| {
                    ExecutionError::evaluation(format!(
                        "invalid select expression {source_text:?}: {error}"
                    ))
                })?;
                Some(evaluate_derive_expression(&expression, &scope)?)
            } else {
                let source = parse_runtime_path(source_text, "select source")?;
                cloned_runtime_path_value(&scope, &source)
            };

            if let Some(value) = value {
                insert_path(&mut result, &target, value.clone());
                // Later projections may refer to aliases introduced earlier in
                // the same select list.
                insert_path(&mut scope, &target, value);
            }
        }

        Ok(CustomOperatorResult::Replace(Arc::new(result)))
    }

    fn materialize_rename(
        &self,
        arguments: &str,
        document: &Document,
    ) -> ExecutionResult<CustomOperatorResult> {
        let (source, target) = arguments.split_once(" as ").ok_or_else(|| {
            ExecutionError::evaluation(
                "invalid rename runtime payload: expected `<source> as <target>`",
            )
        })?;

        let source = parse_runtime_path(source, "rename source")?;
        let target = parse_runtime_path(target, "rename target")?;

        let mut result = document.clone();
        let Some(value) = remove_path(&mut result, &source) else {
            return Ok(CustomOperatorResult::Keep);
        };

        insert_path(&mut result, &target, value);
        Ok(CustomOperatorResult::Replace(Arc::new(result)))
    }

    fn materialize_drop(
        &self,
        arguments: &str,
        document: &Document,
    ) -> ExecutionResult<CustomOperatorResult> {
        let mut paths = Vec::new();

        for field in arguments.split(',') {
            paths.push(parse_runtime_path(field, "drop field")?);
        }

        let mut result = document.clone();
        let mut changed = false;

        for path in paths {
            changed |= remove_path(&mut result, &path).is_some();
        }

        if changed {
            Ok(CustomOperatorResult::Replace(Arc::new(result)))
        } else {
            Ok(CustomOperatorResult::Keep)
        }
    }

    /// Implements lookup's left-join result contract.
    ///
    /// The outer document is always retained. Matching inner documents are
    /// stored as an array of objects in `into`; no match therefore produces an
    /// empty array rather than dropping the outer row.
    pub fn materialize_lookup(
        &self,
        into: &str,
        outer: &Document,
        matches: &LookupDocuments,
    ) -> ExecutionResult<Arc<Document>> {
        let mut result = outer.clone();
        let values = matches
            .documents()
            .iter()
            .map(|document| Value::object((**document).clone()))
            .collect::<Vec<_>>();

        result.insert(into, Value::array(values));
        Ok(Arc::new(result))
    }

    /// Materializes a pivot result.
    pub fn materialize_pivot(
        &self,
        specification: &PivotSpecification,
        documents: &[Arc<Document>],
    ) -> ExecutionResult<Vec<SyntheticDocument>> {
        let mut groups: BTreeMap<String, PivotGroup> = BTreeMap::new();

        for document in documents {
            let row_values = specification
                .rows()
                .iter()
                .map(|path| cloned_path_value(document, path))
                .collect::<Vec<_>>();
            let row_key = stable_values_key(&row_values);

            let column_values = specification
                .columns()
                .iter()
                .map(|path| cloned_path_value(document, path))
                .collect::<Vec<_>>();
            let column_name = column_label(&column_values);

            let group = groups.entry(row_key).or_insert_with(|| PivotGroup {
                row_values,
                cells: BTreeMap::new(),
            });

            for value in specification.values() {
                let cell_name = pivot_cell_name(&column_name, value, specification.values().len());
                let input = cloned_path_value(document, value.field());
                group
                    .cells
                    .entry(cell_name)
                    .or_insert_with(|| AggregateState::new(value.aggregate()))
                    .push(input)?;
            }
        }

        groups
            .into_values()
            .enumerate()
            .map(|(index, group)| build_pivot_document(index, specification, group))
            .collect()
    }
}

#[derive(Debug)]
struct PivotGroup {
    row_values: Vec<Option<Value>>,
    cells: BTreeMap<String, AggregateState>,
}

#[derive(Debug)]
enum AggregateState {
    Sum { total: f64, count: u64 },
    Average { total: f64, count: u64 },
    First(Option<Value>),
    Last(Option<Value>),
    Minimum(Option<Value>),
    Maximum(Option<Value>),
    Count(u64),
}

impl AggregateState {
    #[inline]
    fn new(aggregate: PivotAggregate) -> Self {
        match aggregate {
            PivotAggregate::Sum => Self::Sum {
                total: 0.0,
                count: 0,
            },
            PivotAggregate::Average => Self::Average {
                total: 0.0,
                count: 0,
            },
            PivotAggregate::First => Self::First(None),
            PivotAggregate::Last => Self::Last(None),
            PivotAggregate::Minimum => Self::Minimum(None),
            PivotAggregate::Maximum => Self::Maximum(None),
            PivotAggregate::Count => Self::Count(0),
        }
    }

    fn push(&mut self, value: Option<Value>) -> ExecutionResult<()> {
        match self {
            Self::Sum { total, count } | Self::Average { total, count } => {
                if let Some(value) = value {
                    *total += numeric_value(&value)?;
                    *count = count.checked_add(1).ok_or_else(|| {
                        ExecutionError::evaluation("pivot aggregate count overflow")
                    })?;
                }
            }
            Self::First(current) => {
                if current.is_none() {
                    *current = value;
                }
            }
            Self::Last(current) => {
                if value.is_some() {
                    *current = value;
                }
            }
            Self::Minimum(current) => update_extreme(current, value, true)?,
            Self::Maximum(current) => update_extreme(current, value, false)?,
            Self::Count(count) => {
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| ExecutionError::evaluation("pivot count overflow"))?;
            }
        }
        Ok(())
    }

    fn finish(self) -> ExecutionResult<Value> {
        match self {
            Self::Sum { total, .. } => float_value(total),
            Self::Average { total, count } => {
                if count == 0 {
                    Ok(Value::null())
                } else {
                    float_value(total / count as f64)
                }
            }
            Self::First(value)
            | Self::Last(value)
            | Self::Minimum(value)
            | Self::Maximum(value) => Ok(value.unwrap_or_else(Value::null)),
            Self::Count(count) => Ok(Value::unsigned(count)),
        }
    }
}

fn update_extreme(
    current: &mut Option<Value>,
    candidate: Option<Value>,
    minimum: bool,
) -> ExecutionResult<()> {
    let Some(candidate) = candidate else {
        return Ok(());
    };

    let Some(existing) = current.as_ref() else {
        *current = Some(candidate);
        return Ok(());
    };

    let ordering = compare_pivot_values(&candidate, existing)?;
    let replace = if minimum {
        ordering.is_lt()
    } else {
        ordering.is_gt()
    };

    if replace {
        *current = Some(candidate);
    }

    Ok(())
}

fn compare_pivot_values(left: &Value, right: &Value) -> ExecutionResult<std::cmp::Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => numeric_order(*left, *right),
        (Value::String(left), Value::String(right)) => Ok(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        _ => Err(ExecutionError::evaluation(format!(
            "pivot min/max requires comparable values of the same scalar type, found {left:?} and {right:?}"
        ))),
    }
}

fn numeric_order(left: Number, right: Number) -> ExecutionResult<std::cmp::Ordering> {
    let left = match left {
        Number::Signed(value) => value as f64,
        Number::Unsigned(value) => value as f64,
        Number::Float(value) => value,
    };
    let right = match right {
        Number::Signed(value) => value as f64,
        Number::Unsigned(value) => value as f64,
        Number::Float(value) => value,
    };

    left.partial_cmp(&right).ok_or_else(|| {
        ExecutionError::evaluation("pivot min/max cannot compare non-finite numbers")
    })
}

fn build_pivot_document(
    index: usize,
    specification: &PivotSpecification,
    group: PivotGroup,
) -> ExecutionResult<SyntheticDocument> {
    let mut document = Document::new();

    for (path, value) in specification.rows().iter().zip(group.row_values) {
        let field = path.last();
        document.insert(field, value.unwrap_or_else(Value::null));
    }

    for (name, state) in group.cells {
        document.insert(name, state.finish()?);
    }

    let id = DocumentId::synthetic(0x0070_6976_6f74, index as u64 + 1);

    Ok(SyntheticDocument::new(id, Arc::new(document)))
}

#[derive(Debug)]
struct IncrementalGroupKey {
    source: ExpressionFieldPath,
    output: ExpressionFieldPath,
}

#[derive(Debug)]
struct IncrementalGroupMeasure {
    source: ExpressionFieldPath,
    alias: String,
}

#[derive(Debug)]
struct DefaultIncrementalGroupAccumulator {
    keys: Vec<IncrementalGroupKey>,
    measures: Vec<IncrementalGroupMeasure>,
    key_values: Option<Vec<Option<Value>>>,
    count: u64,
    sums: Vec<f64>,
    seen_summable: Vec<bool>,
    projected_key_indexes: Vec<usize>,
    projected_measure_indexes: Vec<usize>,
}

impl DefaultIncrementalGroupAccumulator {
    fn new(fields: &[ExpressionFieldPath]) -> ExecutionResult<Self> {
        let mut keys = Vec::new();
        let mut measures = Vec::new();
        for field in fields {
            if let Some((source, alias)) = decode_group_sum_marker(field)? {
                measures.push(IncrementalGroupMeasure { source, alias });
            } else if let Some((source, output)) = decode_group_key_marker(field)? {
                keys.push(IncrementalGroupKey { source, output });
            } else {
                keys.push(IncrementalGroupKey {
                    source: field.clone(),
                    output: field.clone(),
                });
            }
        }
        let measure_count = measures.len();
        let (_, required) = group_field_layout(fields)?;
        let projected_key_indexes = keys
            .iter()
            .map(|key| {
                required
                    .iter()
                    .position(|field| field == &key.source)
                    .expect("group key is required")
            })
            .collect();
        let projected_measure_indexes = measures
            .iter()
            .map(|measure| {
                required
                    .iter()
                    .position(|field| field == &measure.source)
                    .expect("group measure is required")
            })
            .collect();
        Ok(Self {
            keys,
            measures,
            key_values: None,
            count: 0,
            sums: vec![0.0; measure_count],
            seen_summable: vec![false; measure_count],
            projected_key_indexes,
            projected_measure_indexes,
        })
    }
}

impl IncrementalGroupAccumulator for DefaultIncrementalGroupAccumulator {
    fn push(&mut self, document: &Document) -> ExecutionResult<()> {
        if self.key_values.is_none() {
            self.key_values = Some(
                self.keys
                    .iter()
                    .map(|key| path_value(document, &key.source).cloned())
                    .collect(),
            );
        }

        self.count = self.count.checked_add(1).ok_or_else(|| {
            ExecutionError::evaluation("group count exceeds the supported u64 range")
        })?;

        for (index, measure) in self.measures.iter().enumerate() {
            let Some(value) = path_value(document, &measure.source) else {
                continue;
            };
            if value.has_capability(crate::Capability::Summable) {
                self.sums[index] += numeric_value(value)?;
                self.seen_summable[index] = true;
            }
        }
        Ok(())
    }

    fn push_projected_values(&mut self, values: &[Option<Value>]) -> ExecutionResult<bool> {
        if self.key_values.is_none() {
            self.key_values = Some(
                self.projected_key_indexes
                    .iter()
                    .map(|index| values.get(*index).cloned().flatten())
                    .collect(),
            );
        }

        self.count = self.count.checked_add(1).ok_or_else(|| {
            ExecutionError::evaluation("group count exceeds the supported u64 range")
        })?;

        for (measure_index, value_index) in self.projected_measure_indexes.iter().enumerate() {
            let Some(Some(value)) = values.get(*value_index) else {
                continue;
            };
            if value.has_capability(crate::Capability::Summable) {
                self.sums[measure_index] += numeric_value(value)?;
                self.seen_summable[measure_index] = true;
            }
        }
        Ok(true)
    }

    fn push_projected_value_refs(
        &mut self,
        values: &[Option<crate::storage::ProjectedValueRef<'_>>],
        source_slots: &[usize],
    ) -> ExecutionResult<bool> {
        if self.key_values.is_none() {
            self.key_values = Some(
                self.projected_key_indexes
                    .iter()
                    .map(|index| {
                        values
                            .get(source_slots.get(*index).copied().unwrap_or(usize::MAX))
                            .and_then(Option::as_ref)
                            .map(crate::storage::ProjectedValueRef::to_value)
                    })
                    .collect(),
            );
        }

        self.count = self.count.checked_add(1).ok_or_else(|| {
            ExecutionError::evaluation("group count exceeds the supported u64 range")
        })?;

        for (measure_index, value_index) in self.projected_measure_indexes.iter().enumerate() {
            let Some(Some(value)) = values.get(
                source_slots
                    .get(*value_index)
                    .copied()
                    .unwrap_or(usize::MAX),
            ) else {
                continue;
            };
            if let Some(number) = value.as_f64() {
                self.sums[measure_index] += number;
                self.seen_summable[measure_index] = true;
            }
        }
        Ok(true)
    }

    fn compact_partial(&self) -> ExecutionResult<Option<Vec<u8>>> {
        // v2 deliberately omits grouping-key values. The engine already writes the
        // canonical key once per spill record and seeds it again during merge.
        let mut out = Vec::with_capacity(9 + self.measures.len() * 9);
        out.push(2);
        encode_var_u64(&mut out, self.count);
        for index in 0..self.measures.len() {
            out.push(u8::from(self.seen_summable[index]));
            if self.seen_summable[index] {
                out.extend_from_slice(&self.sums[index].to_bits().to_be_bytes());
            }
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
        let mut input = CompactGroupDecoder::new(payload);
        match input.byte()? {
            1 => {
                // Backward-compatible reader for v1 runs written by older engines.
                let key_count = input.len()?;
                if key_count != self.keys.len() {
                    return Ok(false);
                }
                let mut decoded_keys = Vec::with_capacity(key_count);
                for _ in 0..key_count {
                    decoded_keys.push(match input.byte()? {
                        0 => None,
                        1 => Some(input.value()?),
                        _ => return Ok(false),
                    });
                }
                if self.key_values.is_none() {
                    self.key_values = Some(decoded_keys);
                }
                self.count = self.count.checked_add(input.u64()?).ok_or_else(|| {
                    ExecutionError::evaluation("group count exceeds the supported u64 range")
                })?;
                let measure_count = input.len()?;
                if measure_count != self.measures.len() {
                    return Ok(false);
                }
                for index in 0..measure_count {
                    match input.byte()? {
                        0 => {}
                        1 => {
                            self.sums[index] += f64::from_bits(input.u64()?);
                            self.seen_summable[index] = true;
                        }
                        _ => return Ok(false),
                    }
                }
            }
            2 => {
                self.count = self.count.checked_add(input.var_u64()?).ok_or_else(|| {
                    ExecutionError::evaluation("group count exceeds the supported u64 range")
                })?;
                for index in 0..self.measures.len() {
                    match input.byte()? {
                        0 => {}
                        1 => {
                            self.sums[index] += f64::from_bits(input.u64()?);
                            self.seen_summable[index] = true;
                        }
                        _ => return Ok(false),
                    }
                }
            }
            _ => return Ok(false),
        }
        input.finish()?;
        Ok(true)
    }

    fn merge_partial(&mut self, document: &Document) -> ExecutionResult<bool> {
        if self.key_values.is_none() {
            self.key_values = Some(
                self.keys
                    .iter()
                    .map(|key| path_value(document, &key.source).cloned())
                    .collect(),
            );
        }
        let partial_count = match document.get("__og_internal_group_partial_count") {
            Some(Value::Number(Number::Unsigned(value))) => *value,
            Some(Value::Number(Number::Signed(value))) if *value >= 0 => *value as u64,
            _ => return Ok(false),
        };
        self.count = self.count.checked_add(partial_count).ok_or_else(|| {
            ExecutionError::evaluation("group count exceeds the supported u64 range")
        })?;
        for (index, measure) in self.measures.iter().enumerate() {
            let Some(value) = document.get(measure.alias.as_str()) else {
                continue;
            };
            if value.has_capability(crate::Capability::Summable) {
                self.sums[index] += numeric_value(value)?;
                self.seen_summable[index] = true;
            }
        }
        Ok(true)
    }

    fn finish(self: Box<Self>, ordinal: u64) -> ExecutionResult<SyntheticDocument> {
        let mut document = Document::new();
        let key_values = self.key_values.unwrap_or_default();
        for (key, value) in self.keys.iter().zip(key_values) {
            let path = key.output.iter().collect::<Vec<_>>();
            insert_path(&mut document, &path, value.unwrap_or_else(Value::null));
        }

        let mut emitted_sum = false;
        for (index, measure) in self.measures.iter().enumerate() {
            if self.seen_summable[index] {
                let value = Value::float(self.sums[index]).map_err(|error| {
                    ExecutionError::evaluation(format!(
                        "group sum cannot be represented as a finite number: {error}"
                    ))
                })?;
                document.insert(measure.alias.as_str(), value);
                emitted_sum = true;
            }
        }
        if !emitted_sum {
            document.insert("count", Value::unsigned(self.count));
        }

        Ok(SyntheticDocument::new(
            DocumentId::synthetic(GROUP_NAMESPACE, ordinal),
            Arc::new(document),
        ))
    }
}
const GROUP_NAMESPACE: u64 = 0x0067_726f_7570;
const GROUP_SUM_MARKER_PREFIX: &str = "__og_group_sum_";
const GROUP_KEY_MARKER_PREFIX: &str = "__og_group_key_";

/// Resolves the physical group layout.
///
/// Returns `(grouping_keys, required_input_fields)`: grouping keys exclude the
/// planner's internal sum markers, while required input fields replace each
/// marker with its source path. This is the shape storage projection must use.
pub(crate) fn group_field_layout(
    fields: &[ExpressionFieldPath],
) -> ExecutionResult<(Vec<ExpressionFieldPath>, Vec<ExpressionFieldPath>)> {
    let mut grouping_keys = Vec::new();
    let mut required = Vec::new();

    for field in fields {
        if let Some((source, _alias)) = decode_group_sum_marker(field)? {
            if !required.contains(&source) {
                required.push(source);
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

pub(crate) fn decode_group_key_marker(
    field: &ExpressionFieldPath,
) -> ExecutionResult<Option<(ExpressionFieldPath, ExpressionFieldPath)>> {
    let Some((source, output)) = decode_group_path_marker(field, GROUP_KEY_MARKER_PREFIX, "key")?
    else {
        return Ok(None);
    };
    let output = ExpressionFieldPath::new(output.split('.')).map_err(|error| {
        ExecutionError::evaluation(format!("invalid encoded group output path: {error}"))
    })?;
    Ok(Some((source, output)))
}

fn decode_group_path_marker(
    field: &ExpressionFieldPath,
    prefix: &str,
    marker_kind: &str,
) -> ExecutionResult<Option<(ExpressionFieldPath, String)>> {
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

    let source = decode_group_marker_text(source_hex)?;
    let output = decode_group_marker_text(output_hex)?;
    let source = ExpressionFieldPath::new(source.split('.')).map_err(|error| {
        ExecutionError::evaluation(format!("invalid encoded group source path: {error}"))
    })?;
    Ok(Some((source, output)))
}

pub(crate) fn decode_group_sum_marker(
    field: &ExpressionFieldPath,
) -> ExecutionResult<Option<(ExpressionFieldPath, String)>> {
    decode_group_path_marker(field, GROUP_SUM_MARKER_PREFIX, "sum")
}

fn decode_group_marker_text(text: &str) -> ExecutionResult<String> {
    if text.len() % 2 != 0 {
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

fn remove_path(document: &mut Document, path: &[&str]) -> Option<Value> {
    let (first, rest) = path.split_first()?;

    if rest.is_empty() {
        return document.remove(first);
    }

    let original = document.remove(first)?;
    let Some(parent) = original.as_object() else {
        document.insert(*first, original);
        return None;
    };

    let mut nested = parent.clone();
    let removed = remove_path(&mut nested, rest);

    match removed {
        Some(value) => {
            if !nested.is_empty() {
                document.insert(*first, Value::object(nested));
            }
            Some(value)
        }
        None => {
            document.insert(*first, original);
            None
        }
    }
}

fn insert_path(document: &mut Document, path: &[&str], value: Value) {
    let Some((first, rest)) = path.split_first() else {
        return;
    };

    if rest.is_empty() {
        document.insert(*first, value);
        return;
    }

    let mut nested = document
        .remove(first)
        .and_then(Value::into_object)
        .map(|document| document.as_ref().clone())
        .unwrap_or_else(Document::new);

    insert_path(&mut nested, rest, value);
    document.insert(*first, Value::object(nested));
}

fn cloned_runtime_path_value(document: &Document, path: &[&str]) -> Option<Value> {
    let (first, rest) = path.split_first()?;
    let mut value = document.get(first)?;
    for segment in rest {
        value = value.as_object()?.get(segment)?;
    }
    Some(value.clone())
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

    for (name, value) in document.iter() {
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

fn decode_explicit_distinct_key_values(
    bytes: &[u8],
) -> ExecutionResult<Option<Vec<Option<Value>>>> {
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
    fn new(bytes: &'a [u8]) -> Self {
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

fn column_label(values: &[Option<Value>]) -> String {
    if values.is_empty() {
        return "value".to_owned();
    }

    values
        .iter()
        .map(|value| match value {
            Some(Value::String(value)) => value.to_string(),
            Some(Value::Bool(value)) => value.to_string(),
            Some(Value::Number(value)) => format_number(*value),
            Some(Value::Null) | None => "null".to_owned(),
            Some(other) => format!("{other:?}"),
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn pivot_cell_name(column: &str, value: &PivotValue, value_count: usize) -> String {
    let measure = value.alias().unwrap_or_else(|| value.field().last());

    if value_count == 1 {
        column.to_owned()
    } else {
        format!("{column}.{measure}")
    }
}

fn numeric_value(value: &Value) -> ExecutionResult<f64> {
    let number = value.as_number().ok_or_else(|| {
        ExecutionError::evaluation(format!("pivot sum/avg requires a number, found {value:?}"))
    })?;

    Ok(match *number {
        Number::Signed(value) => value as f64,
        Number::Unsigned(value) => value as f64,
        Number::Float(value) => value,
    })
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
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;

    for (index, character) in source.char_indices() {
        if quoted {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                quoted = false;
            }
            continue;
        }

        match character {
            '"' => quoted = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&source[start..index]);
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }

    if quoted || depth != 0 {
        return Err(ExecutionError::evaluation(
            "invalid select projection list: unclosed quote or parenthesis",
        ));
    }

    parts.push(&source[start..]);
    Ok(parts)
}

fn split_derive_assignments(source: &str) -> ExecutionResult<Vec<(&str, &str)>> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut quoted = false;
    let mut escaped = false;

    for (index, character) in source.char_indices() {
        if quoted {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                quoted = false;
            }
            continue;
        }

        match character {
            '"' => quoted = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&source[start..index]);
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&source[start..]);

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
            if field.split('.').any(|segment| segment.is_empty()) {
                return Err(ExecutionError::evaluation(format!(
                    "invalid derive field path {field:?}"
                )));
            }
            Ok((field, expression))
        })
        .collect()
}

fn evaluate_derive_expression(
    expression: &Expression,
    document: &Document,
) -> ExecutionResult<Value> {
    match expression.ungrouped().view() {
        ExpressionView::Literal(literal) => match literal {
            Literal::Null => Ok(Value::null()),
            Literal::Bool(value) => Ok(Value::bool(*value)),
            Literal::String(value) => Ok(Value::string(Arc::clone(value))),
            Literal::Number(text) => parse_number_value(text)
                .map(Value::Number)
                .map_err(|error| ExecutionError::evaluation(error.to_string())),
            Literal::Json(text) => super::json_value::parse_json_literal(text),
        },
        ExpressionView::Field(path) => cloned_path_value(document, path)
            .ok_or_else(|| ExecutionError::evaluation(format!("derive field {path} is missing"))),
        ExpressionView::Group(inner) => evaluate_derive_expression(inner, document),
        ExpressionView::Unary { operator, operand } => {
            let value = evaluate_derive_expression(operand, document)?;
            match operator {
                UnaryOperator::Positive => derive_numeric_value(value, "+"),
                UnaryOperator::Negate => {
                    let number = numeric_as_f64(&value, "-")?;
                    Number::float(-number)
                        .map(Value::Number)
                        .map_err(|error| ExecutionError::evaluation(error.to_string()))
                }
                UnaryOperator::Not => value
                    .as_bool()
                    .map(|value| Value::bool(!value))
                    .ok_or_else(|| ExecutionError::evaluation("derive `!` expects a boolean")),
            }
        }
        ExpressionView::Binary {
            left,
            operator,
            right,
        } if operator.is_arithmetic() => {
            let left = evaluate_derive_expression(left, document)?;
            let right = evaluate_derive_expression(right, document)?;
            let left = numeric_as_f64(&left, operator.as_str())?;
            let right = numeric_as_f64(&right, operator.as_str())?;
            let result = match operator {
                BinaryOperator::Add => left + right,
                BinaryOperator::Subtract => left - right,
                BinaryOperator::Multiply => left * right,
                BinaryOperator::Divide if right == 0.0 => {
                    return Err(ExecutionError::evaluation("division by zero in derive"));
                }
                BinaryOperator::Divide => left / right,
                BinaryOperator::Remainder if right == 0.0 => {
                    return Err(ExecutionError::evaluation("remainder by zero in derive"));
                }
                BinaryOperator::Remainder => left % right,
                _ => unreachable!(),
            };
            Number::float(result)
                .map(Value::Number)
                .map_err(|error| ExecutionError::evaluation(error.to_string()))
        }
        ExpressionView::Binary { operator, .. } => Err(ExecutionError::evaluation(format!(
            "derive operator {operator} is not supported"
        ))),
    }
}

fn derive_numeric_value(value: Value, operator: &str) -> ExecutionResult<Value> {
    if value.as_number().is_some() {
        Ok(value)
    } else {
        Err(ExecutionError::evaluation(format!(
            "derive operator {operator} expects a number"
        )))
    }
}

fn numeric_as_f64(value: &Value, operator: &str) -> ExecutionResult<f64> {
    let number = value.as_number().ok_or_else(|| {
        ExecutionError::evaluation(format!("derive operator {operator} expects numbers"))
    })?;
    Ok(match *number {
        Number::Signed(value) => value as f64,
        Number::Unsigned(value) => value as f64,
        Number::Float(value) => value,
    })
}

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

    #[test]
    fn derive_subtracts_fields_and_preserves_the_source_document() {
        let mut document = Document::new();
        document.insert("CAFacture", Value::unsigned(120));
        document.insert("COGS", Value::unsigned(45));

        let result = RuntimeMaterializer::new()
            .materialize_derive("Marge=CAFacture-COGS", &document)
            .unwrap();

        let CustomOperatorResult::Replace(result) = result else {
            panic!("derive should replace the result document");
        };

        assert_eq!(result.get("Marge"), Some(&Value::float(75.0).unwrap()));
        assert_eq!(document.get("Marge"), None);
        assert_eq!(document.get("CAFacture"), Some(&Value::unsigned(120)));
    }

    #[test]
    fn derive_supports_unary_positive_without_colliding_with_pivot_numeric_conversion() {
        let mut document = Document::new();
        document.insert("amount", Value::signed(-12));

        let result = RuntimeMaterializer::new()
            .materialize_derive("copy=+amount", &document)
            .unwrap();

        let CustomOperatorResult::Replace(result) = result else {
            panic!("derive should replace the result document");
        };

        assert_eq!(result.get("copy"), Some(&Value::signed(-12)));
    }

    #[test]
    fn derive_applies_multiple_assignments_to_nested_targets() {
        let mut document = Document::new();
        document.insert("revenue", Value::unsigned(200));
        document.insert("cost", Value::unsigned(50));

        let result = RuntimeMaterializer::new()
            .materialize_derive(
                "metrics.margin=revenue-cost, metrics.ratio=(revenue-cost)/revenue",
                &document,
            )
            .unwrap();

        let CustomOperatorResult::Replace(result) = result else {
            panic!("derive should replace the result document");
        };
        let metrics = result
            .get("metrics")
            .and_then(Value::as_object)
            .expect("derive should create the nested object");

        assert_eq!(metrics.get("margin"), Some(&Value::float(150.0).unwrap()));
        assert_eq!(metrics.get("ratio"), Some(&Value::float(0.75).unwrap()));
    }

    #[test]
    fn derive_rejects_division_by_zero() {
        let mut document = Document::new();
        document.insert("revenue", Value::unsigned(200));
        document.insert("zero", Value::unsigned(0));

        let error = RuntimeMaterializer::new()
            .materialize_derive("ratio=revenue/zero", &document)
            .unwrap_err();

        assert!(error.to_string().contains("division by zero in derive"));
    }

    #[test]
    fn select_and_count_recent_handlers_materialize_expected_documents() {
        let mut document = Document::new();
        document.insert("a", Value::unsigned(1));
        document.insert("b", Value::unsigned(2));

        let field = ExpressionFieldPath::new(["a"]).unwrap();
        let selected = RuntimeMaterializer::new()
            .materialize_select(&[field], &document)
            .unwrap();
        assert_eq!(selected.get("a"), Some(&Value::unsigned(1)));
        assert_eq!(selected.get("b"), None);

        let counted = RuntimeMaterializer::new()
            .materialize_count("count", 7)
            .unwrap();
        assert_eq!(counted.get("count"), Some(&Value::unsigned(7)));
    }

    #[test]
    fn select_aliases_may_be_mixed_with_plain_fields() {
        let mut document = Document::new();
        document.insert("CAFacture", Value::unsigned(100));
        document.insert("COGS", Value::unsigned(40));

        let result = RuntimeMaterializer::new()
            .materialize_custom("select", "CAFacture as CA, COGS", &document)
            .unwrap();

        let CustomOperatorResult::Replace(result) = result else {
            panic!("select should replace the result document");
        };
        assert_eq!(result.get("CA"), Some(&Value::unsigned(100)));
        assert_eq!(result.get("COGS"), Some(&Value::unsigned(40)));
        assert_eq!(result.get("CAFacture"), None);
    }

    #[test]
    fn select_expression_can_reference_an_alias_defined_earlier() {
        let mut document = Document::new();
        document.insert("CAFacture", Value::unsigned(100));
        document.insert("COGS", Value::unsigned(40));

        let result = RuntimeMaterializer::new()
            .materialize_custom(
                "select",
                "CAFacture as CA, COGS, CA - COGS as Marge",
                &document,
            )
            .unwrap();

        let CustomOperatorResult::Replace(result) = result else {
            panic!("select should replace the result document");
        };
        assert_eq!(result.get("CA"), Some(&Value::unsigned(100)));
        assert_eq!(result.get("COGS"), Some(&Value::unsigned(40)));
        assert_eq!(result.get("Marge"), Some(&Value::float(60.0).unwrap()));
    }

    #[test]
    fn lookup_keeps_outer_and_writes_empty_array() {
        let mut outer = Document::new();
        outer.insert("name", Value::string("Alice"));

        let result = RuntimeMaterializer::new()
            .materialize_lookup("workspaces", &outer, &LookupDocuments::new([]))
            .unwrap();

        assert_eq!(result.get("name"), Some(&Value::string("Alice")));
        assert_eq!(
            result.get("workspaces").and_then(Value::as_array),
            Some(&[][..])
        );
    }

    #[test]
    fn rename_moves_a_top_level_field() {
        let mut document = Document::new();
        document.insert("name", Value::string("Alice"));
        document.insert("age", Value::unsigned(42));

        let result = RuntimeMaterializer::new()
            .materialize_custom("rename", "name as display_name", &document)
            .unwrap();

        let CustomOperatorResult::Replace(result) = result else {
            panic!("rename should replace the document");
        };

        assert_eq!(result.get("name"), None);
        assert_eq!(result.get("display_name"), Some(&Value::string("Alice")));
        assert_eq!(result.get("age"), Some(&Value::unsigned(42)));
    }

    #[test]
    fn rename_moves_a_nested_field_and_prunes_empty_parent() {
        let mut profile = Document::new();
        profile.insert("name", Value::string("Alice"));

        let mut document = Document::new();
        document.insert("profile", Value::object(profile));

        let result = RuntimeMaterializer::new()
            .materialize_custom("rename", "profile.name as display_name", &document)
            .unwrap();

        let CustomOperatorResult::Replace(result) = result else {
            panic!("rename should replace the document");
        };

        assert_eq!(result.get("profile"), None);
        assert_eq!(result.get("display_name"), Some(&Value::string("Alice")));
    }

    #[test]
    fn rename_keeps_document_when_source_is_missing() {
        let document = Document::new();

        let result = RuntimeMaterializer::new()
            .materialize_custom("rename", "missing as present", &document)
            .unwrap();

        assert!(matches!(result, CustomOperatorResult::Keep));
    }

    #[test]
    fn drop_removes_multiple_fields_and_prunes_empty_parents() {
        let mut profile = Document::new();
        profile.insert("secret", Value::string("hidden"));

        let mut document = Document::new();
        document.insert("age", Value::unsigned(42));
        document.insert("name", Value::string("Alice"));
        document.insert("profile", Value::object(profile));

        let result = RuntimeMaterializer::new()
            .materialize_custom("drop", "age, profile.secret", &document)
            .unwrap();

        let CustomOperatorResult::Replace(result) = result else {
            panic!("drop should replace the document");
        };

        assert_eq!(result.get("age"), None);
        assert_eq!(result.get("profile"), None);
        assert_eq!(result.get("name"), Some(&Value::string("Alice")));
    }

    #[test]
    fn drop_keeps_document_when_no_field_exists() {
        let document = Document::new();

        let result = RuntimeMaterializer::new()
            .materialize_custom("drop", "missing, profile.secret", &document)
            .unwrap();

        assert!(matches!(result, CustomOperatorResult::Keep));
    }

    #[test]
    fn distinct_without_fields_uses_the_complete_document() {
        let mut first = Document::new();
        first.insert("a", Value::unsigned(1));
        first.insert("b", Value::unsigned(2));

        let mut same = Document::new();
        same.insert("b", Value::unsigned(2));
        same.insert("a", Value::unsigned(1));

        let mut different = Document::new();
        different.insert("a", Value::unsigned(1));
        different.insert("b", Value::unsigned(3));

        let materializer = RuntimeMaterializer::new();
        let first_key = materializer.materialize_distinct_key(&[], &first).unwrap();
        let same_key = materializer.materialize_distinct_key(&[], &same).unwrap();
        let different_key = materializer
            .materialize_distinct_key(&[], &different)
            .unwrap();

        assert_eq!(first_key, same_key);
        assert_ne!(first_key, different_key);
    }

    #[test]
    fn distinct_fields_ignore_unselected_fields() {
        let mut first = Document::new();
        first.insert("a", Value::unsigned(1));
        first.insert("b", Value::unsigned(2));

        let mut second = Document::new();
        second.insert("a", Value::unsigned(1));
        second.insert("b", Value::unsigned(99));

        let fields = [ExpressionFieldPath::new(["a"]).unwrap()];
        let materializer = RuntimeMaterializer::new();

        assert_eq!(
            materializer
                .materialize_distinct_key(&fields, &first)
                .unwrap(),
            materializer
                .materialize_distinct_key(&fields, &second)
                .unwrap()
        );
    }

    #[test]
    fn distinct_keeps_missing_separate_from_null() {
        let missing = Document::new();
        let mut null = Document::new();
        null.insert("a", Value::null());

        let fields = [ExpressionFieldPath::new(["a"]).unwrap()];
        let materializer = RuntimeMaterializer::new();

        assert_ne!(
            materializer
                .materialize_distinct_key(&fields, &missing)
                .unwrap(),
            materializer
                .materialize_distinct_key(&fields, &null)
                .unwrap()
        );
    }

    #[test]
    fn sort_compares_ascending_and_descending_values() {
        let mut one = Document::new();
        one.insert("a", Value::unsigned(1));
        let mut two = Document::new();
        two.insert("a", Value::unsigned(2));

        let field = ExpressionFieldPath::new(["a"]).unwrap();
        let materializer = RuntimeMaterializer::new();

        assert_eq!(
            materializer
                .materialize_sort_comparison(&[SortKey::ascending(field.clone())], &one, &two)
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            materializer
                .materialize_sort_comparison(&[SortKey::descending(field)], &one, &two)
                .unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn sort_uses_later_keys_when_values_are_equal() {
        let mut left = Document::new();
        left.insert("a", Value::unsigned(1));
        left.insert("b", Value::unsigned(2));
        let mut right = Document::new();
        right.insert("a", Value::unsigned(1));
        right.insert("b", Value::unsigned(3));

        let keys = [
            SortKey::ascending(ExpressionFieldPath::new(["a"]).unwrap()),
            SortKey::ascending(ExpressionFieldPath::new(["b"]).unwrap()),
        ];

        assert_eq!(
            RuntimeMaterializer::new()
                .materialize_sort_comparison(&keys, &left, &right)
                .unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn sort_orders_missing_values_deterministically() {
        let missing = Document::new();
        let mut present = Document::new();
        present.insert("a", Value::unsigned(1));
        let field = ExpressionFieldPath::new(["a"]).unwrap();

        assert_eq!(
            RuntimeMaterializer::new()
                .materialize_sort_comparison(
                    &[SortKey::ascending(field.clone())],
                    &missing,
                    &present,
                )
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            RuntimeMaterializer::new()
                .materialize_sort_comparison(&[SortKey::descending(field)], &missing, &present,)
                .unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn borrowed_path_value_reads_nested_fields_without_materializing_a_clone() {
        let mut nested = Document::new();
        nested.insert("value", Value::unsigned(42));
        let mut document = Document::new();
        document.insert("nested", Value::object(nested));
        let path = ExpressionFieldPath::new(["nested", "value"]).unwrap();

        let value = path_value(&document, &path).expect("path exists");
        assert_eq!(value, &Value::unsigned(42));
    }

    #[test]
    fn group_field_layout_resolves_aliased_keys_to_source_fields() {
        let fields = [
            ExpressionFieldPath::new(["__og_group_key_61727469636c65_436c69656e74"]).unwrap(),
            ExpressionFieldPath::new(["__og_group_sum_6361_546f74616c"]).unwrap(),
        ];

        let (grouping, required) = group_field_layout(&fields).unwrap();
        assert_eq!(
            grouping,
            vec![ExpressionFieldPath::new(["article"]).unwrap()]
        );
        assert_eq!(
            required,
            vec![
                ExpressionFieldPath::new(["article"]).unwrap(),
                ExpressionFieldPath::new(["ca"]).unwrap(),
            ]
        );
    }

    #[test]
    fn group_materializes_aliased_key_without_changing_group_identity() {
        let mut first = Document::new();
        first.insert("article", Value::string("A"));
        let mut second = Document::new();
        second.insert("article", Value::string("A"));

        let fields =
            [ExpressionFieldPath::new(["__og_group_key_61727469636c65_436c69656e74"]).unwrap()];
        let documents = vec![Arc::new(first), Arc::new(second)];
        let groups = RuntimeMaterializer::new()
            .materialize_group(&fields, &documents)
            .unwrap();

        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].document().get("Client"),
            Some(&Value::string("A"))
        );
        assert!(groups[0].document().get("article").is_none());
        assert_eq!(groups[0].document().get("count"), Some(&Value::unsigned(2)));
    }

    #[test]
    fn incremental_group_materializes_aliased_key() {
        let fields =
            [ExpressionFieldPath::new(["__og_group_key_61727469636c65_436c69656e74"]).unwrap()];
        let mut accumulator = RuntimeMaterializer::new()
            .incremental_group(&fields)
            .unwrap();

        let mut document = Document::new();
        document.insert("article", Value::string("A"));
        accumulator.push(&document).unwrap();

        let grouped = accumulator.finish(1).unwrap();
        assert_eq!(grouped.document().get("Client"), Some(&Value::string("A")));
        assert!(grouped.document().get("article").is_none());
    }

    #[test]
    fn group_field_layout_resolves_sum_markers_to_source_fields() {
        let fields = [
            ExpressionFieldPath::new(["tPeriode"]).unwrap(),
            ExpressionFieldPath::new(["__og_group_sum_434146616374757265_434146616374757265"])
                .unwrap(),
        ];

        let (grouping, required) = group_field_layout(&fields).unwrap();
        assert_eq!(
            grouping,
            vec![ExpressionFieldPath::new(["tPeriode"]).unwrap()]
        );
        assert_eq!(
            required,
            vec![
                ExpressionFieldPath::new(["tPeriode"]).unwrap(),
                ExpressionFieldPath::new(["CAFacture"]).unwrap(),
            ]
        );
    }

    #[test]
    fn incremental_group_folds_summable_values_without_retaining_rows() {
        let fields = [
            ExpressionFieldPath::new(["period"]).unwrap(),
            ExpressionFieldPath::new(["__og_group_sum_6361_6361"]).unwrap(),
        ];
        let mut accumulator = RuntimeMaterializer::new()
            .incremental_group(&fields)
            .unwrap();

        for _ in 0..100_000 {
            let mut document = Document::new();
            document.insert("period", Value::string("2026-01"));
            document.insert("ca", Value::unsigned(2));
            accumulator.push(&document).unwrap();
        }

        let grouped = accumulator.finish(1).unwrap();
        assert_eq!(
            grouped.document().get("period"),
            Some(&Value::string("2026-01"))
        );
        assert_eq!(
            grouped.document().get("ca"),
            Some(&Value::float(200_000.0).unwrap())
        );
        assert!(grouped.document().get("count").is_none());
    }

    #[test]
    fn group_counts_documents_by_one_key() {
        let mut first = Document::new();
        first.insert("article", Value::string("A"));
        let mut second = Document::new();
        second.insert("article", Value::string("A"));
        let mut third = Document::new();
        third.insert("article", Value::string("B"));

        let documents = vec![Arc::new(first), Arc::new(second), Arc::new(third)];
        let keys = [ExpressionFieldPath::new(["article"]).unwrap()];
        let groups = RuntimeMaterializer::new()
            .materialize_group(&keys, &documents)
            .unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups[0].document().get("article"),
            Some(&Value::string("A"))
        );
        assert_eq!(groups[0].document().get("count"), Some(&Value::unsigned(2)));
        assert_eq!(
            groups[1].document().get("article"),
            Some(&Value::string("B"))
        );
        assert_eq!(groups[1].document().get("count"), Some(&Value::unsigned(1)));
    }

    #[test]
    fn group_preserves_nested_key_paths() {
        let mut address = Document::new();
        address.insert("country", Value::string("FR"));
        let mut document = Document::new();
        document.insert("address", Value::object(address));

        let documents = vec![Arc::new(document)];
        let keys = [ExpressionFieldPath::new(["address", "country"]).unwrap()];
        let groups = RuntimeMaterializer::new()
            .materialize_group(&keys, &documents)
            .unwrap();

        let address = groups[0]
            .document()
            .get("address")
            .and_then(Value::as_object)
            .expect("nested group key should be materialized as an object");
        assert_eq!(address.get("country"), Some(&Value::string("FR")));
        assert_eq!(groups[0].document().get("count"), Some(&Value::unsigned(1)));
    }

    #[test]
    fn group_keeps_missing_distinct_from_physical_null() {
        let missing = Document::new();
        let mut null = Document::new();
        null.insert("article", Value::null());

        let documents = vec![Arc::new(missing), Arc::new(null)];
        let keys = [ExpressionFieldPath::new(["article"]).unwrap()];
        let groups = RuntimeMaterializer::new()
            .materialize_group(&keys, &documents)
            .unwrap();

        assert_eq!(groups.len(), 2);
        assert!(groups.iter().all(|group| {
            group.document().get("article") == Some(&Value::null())
                && group.document().get("count") == Some(&Value::unsigned(1))
        }));
    }

    #[test]
    fn group_sums_runtime_summable_values() {
        let mut first = Document::new();
        first.insert("article", Value::string("A"));
        first.insert("ca", Value::unsigned(10));
        let mut second = Document::new();
        second.insert("article", Value::string("A"));
        second.insert("ca", Value::signed(5));

        let marker = ExpressionFieldPath::new(["__og_group_sum_6361_546f74616c5f4341"]).unwrap();
        let fields = [ExpressionFieldPath::new(["article"]).unwrap(), marker];
        let documents = vec![Arc::new(first), Arc::new(second)];

        let groups = RuntimeMaterializer::new()
            .materialize_group(&fields, &documents)
            .unwrap();

        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].document().get("article"),
            Some(&Value::string("A"))
        );
        assert_eq!(
            groups[0].document().get("Total_CA"),
            Some(&Value::float(15.0).unwrap())
        );
        assert!(groups[0].document().get("count").is_none());
    }

    #[test]
    fn group_falls_back_to_count_when_measure_is_not_summable() {
        let mut first = Document::new();
        first.insert("article", Value::string("A"));
        first.insert("label", Value::string("x"));
        let mut second = Document::new();
        second.insert("article", Value::string("A"));
        second.insert("label", Value::string("y"));

        let marker = ExpressionFieldPath::new(["__og_group_sum_6c6162656c_4c6162656c"]).unwrap();
        let fields = [ExpressionFieldPath::new(["article"]).unwrap(), marker];
        let documents = vec![Arc::new(first), Arc::new(second)];

        let groups = RuntimeMaterializer::new()
            .materialize_group(&fields, &documents)
            .unwrap();

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].document().get("count"), Some(&Value::unsigned(2)));
        assert!(groups[0].document().get("Label").is_none());
    }

    #[test]
    fn group_can_sum_fields_created_by_previous_stages() {
        let mut first = Document::new();
        first.insert("article", Value::string("A"));
        first.insert("calculated", Value::float(2.5).unwrap());
        let mut second = Document::new();
        second.insert("article", Value::string("A"));
        second.insert("calculated", Value::float(1.5).unwrap());

        let marker =
            ExpressionFieldPath::new(["__og_group_sum_63616c63756c61746564_546f74616c"]).unwrap();
        let fields = [ExpressionFieldPath::new(["article"]).unwrap(), marker];
        let documents = vec![Arc::new(first), Arc::new(second)];

        let groups = RuntimeMaterializer::new()
            .materialize_group(&fields, &documents)
            .unwrap();

        assert_eq!(
            groups[0].document().get("Total"),
            Some(&Value::float(4.0).unwrap())
        );
    }

    #[test]
    fn group_without_keys_counts_the_complete_input() {
        let documents = vec![Arc::new(Document::new()), Arc::new(Document::new())];
        let groups = RuntimeMaterializer::new()
            .materialize_group(&[], &documents)
            .unwrap();

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].document().get("count"), Some(&Value::unsigned(2)));
    }

    #[test]
    fn unwind_expands_array_values() {
        let mut document = Document::new();
        document.insert("items", Value::array([Value::signed(1), Value::signed(2)]));
        let result = RuntimeMaterializer::new()
            .materialize_unwind("items", &document)
            .unwrap();
        assert!(matches!(result, CustomOperatorResult::Expand(values) if values.len() == 2));
    }

    #[test]
    fn first_projection_selects_requested_field() {
        let mut document = Document::new();
        document.insert("Article_Code", Value::string("A"));
        document.insert("other", Value::signed(1));
        let result = RuntimeMaterializer::new()
            .materialize_scalar_projection("Article_Code", &document)
            .unwrap();
        assert!(
            matches!(result, CustomOperatorResult::Replace(value) if value.get("Article_Code").is_some() && value.get("other").is_none())
        );
    }

    #[test]
    fn streaming_load_generated_ids_skip_existing_document_lookups() {
        let storage = crate::storage::MemoryStorage::new();
        let read = CountingRead {
            inner: storage.read().unwrap(),
            gets: Cell::new(0),
        };
        let collection = CollectionId::parse("data").unwrap();
        let materializer = StreamingLoadMaterializer::new(Arc::from("ogd"));
        let chunks = [Arc::<str>::from(r#"[{"name":"Ada"},{"name":"Grace"}]"#)];

        let mutations = materializer
            .materialize(&collection, &read, PhysicalLoadMode::Merge, &chunks)
            .unwrap();

        assert_eq!(mutations.len(), 2);
        assert_eq!(read.gets.get(), 0);
    }

    #[test]
    fn streaming_load_mixed_explicit_and_generated_ids_preserves_reserved_ids() {
        let storage = crate::storage::MemoryStorage::new();
        let read = storage.read().unwrap();
        let collection = CollectionId::parse("data").unwrap();
        let materializer = StreamingLoadMaterializer::new(Arc::from("ogd"));
        let chunks = [Arc::<str>::from(
            r#"[{"_id":"019fb7ae-9588-7057-830a-01bdb143b7ce","name":"Ada"},{"name":"Grace"}]"#,
        )];

        let mutations = materializer
            .materialize(&collection, read.as_ref(), PhysicalLoadMode::Merge, &chunks)
            .unwrap();

        assert_eq!(mutations.len(), 2);
        assert!(matches!(mutations[0], StreamingLoadMutation::Insert { .. }));
        assert!(matches!(mutations[1], StreamingLoadMutation::Insert { .. }));
    }

    #[test]
    fn streaming_load_merge_inserts_new_documents() {
        let storage = crate::storage::MemoryStorage::new();
        let read = storage.read().unwrap();
        let collection = CollectionId::parse("data").unwrap();
        let materializer = StreamingLoadMaterializer::new(Arc::from("ogd"));
        let chunks = [Arc::<str>::from(r#"[{"name":"Ada","age":36}]"#)];

        let mutations = materializer
            .materialize(&collection, read.as_ref(), PhysicalLoadMode::Merge, &chunks)
            .unwrap();

        assert_eq!(mutations.len(), 1);
        assert!(
            matches!(&mutations[0], StreamingLoadMutation::Insert { document, .. } if document.get("name").and_then(Value::as_str) == Some("Ada"))
        );
    }

    #[test]
    fn streaming_load_rejects_non_array_chunks() {
        let storage = crate::storage::MemoryStorage::new();
        let read = storage.read().unwrap();
        let collection = CollectionId::parse("data").unwrap();
        let materializer = StreamingLoadMaterializer::new(Arc::from("ogd"));
        let chunks = [Arc::<str>::from(r#"{"name":"Ada"}"#)];

        let error = materializer
            .materialize(&collection, read.as_ref(), PhysicalLoadMode::Merge, &chunks)
            .unwrap_err();

        assert!(error.to_string().contains("must be a JSON array"));
    }
}
