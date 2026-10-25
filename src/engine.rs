//! Query engine orchestration.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, VecDeque},
    error::Error as StdError,
    fmt, io,
    sync::Arc,
    time::Instant,
};

const DEFAULT_PLANNER_CACHE_BYTES: usize = 8 * 1024 * 1024;

mod execute;
mod changes;
pub mod results;
use crate::query::codec::{encode_bytes, encode_varint, Decoder};
mod sinks;

use crate::storage::StorageReadCapability;
use crate::{
    helpers::usize_to_u64_saturating,
    indexing::{IndexingEngine, ObservedAccess, QueryObservation},
    memory::{MemoryGovernor, ProcessMemoryPressure, QueryMemory, StageMemory},
    query::{
        decode_execution_row, encode_execution_row_into, execution_row_encoded_len,
        execution_row_working_bytes,
        stable_sort, BoundedProjectedTopN, BoundedTopN,
        ExecutionError,
        ExecutionOutput, ExecutionRow, ExecutionStatistics, QueryRuntime, ExecutionStrategies,
        ExecutionStrategy, Order, PhysicalOperator,
        PhysicalPlan, PhysicalPlanError, PhysicalSubPipeline, Planner,
        PlannerCache, PlannerCacheStats, PlannerError, PlannerPipeline, ProjectedRowLocator,
        ProjectedValueLayout, ProjectedValuePipeline, SortKey, vcollections,
    },
    spill::{RunMerge, SpillEngine, SpillRun, SpillRunReader},
    storage::{
        CollectionId, DocumentId, DocumentVersion, MemoryStorage, ProjectedValueRef, ScanOptions,
        StorageEngine, StorageError, StorageRead, StorageTransaction, StoredDocument,
    },
    Document, Value,
};

/// Whether a stage keeps a subset of rows chosen by their order (`limit`,
/// `skip`, `first`, `single`): a set-scoped reducer.
fn keeps_rows_by_order(operator: &PhysicalOperator) -> bool {
    let properties = operator.execution_properties();
    matches!(properties.cardinality, crate::query::CardinalityEffect::Reduce)
        && matches!(properties.scope, crate::query::Scope::Set)
}

/// Whether a stage can be replayed on one hydrated row: read-only, streaming,
/// row-local and turning a row into at most one row.
fn replays_one_row(operator: &PhysicalOperator) -> bool {
    let properties = operator.execution_properties();
    matches!(properties.flow, crate::query::Flow::Streaming)
        && matches!(properties.scope, crate::query::Scope::Row)
        && matches!(
            properties.cardinality,
            crate::query::CardinalityEffect::Preserve | crate::query::CardinalityEffect::Reduce
        )
        && !properties.writes()
}

/// Prefix for a blocking core that retains only a few rows (Top-N, sample).
///
/// When the consumer needs whole rows and every prefix stage replays one row
/// at a time, the core can retain rows read as partial documents holding only
/// what the prefix and `core_reads` need (`true`: retained rows must then be
/// hydrated). Otherwise the prefix is used as it is.
fn retaining_prefix(prefix: &PhysicalPlan, core_reads: Vec<crate::query::ExpressionFieldPath>) -> (PhysicalPlan, bool) {
    if prefix.field_demand().is_none() && prefix.operators().iter().all(replays_one_row) {
        (prefix.clone().with_order_free_consumer(Some(core_reads)), true)
    } else {
        (prefix.clone(), false)
    }
}

/// Equality correlation of a lookup: the sub-pipeline starts with a `where`
/// whose leading conjuncts, after optional `exists(…)` guards, are
/// `joined == outer` equalities, or one `starts_with(outer, joined)`
/// (`prefix`). Rows of the joined collection whose key differs (is no prefix
/// of the outer value) cannot pass that `where`, so an index on the joined
/// fields finds every row that can. Several equalities make a composite key:
/// the array of their values.
struct LookupEquality {
    keys: Vec<LookupKey>,
    prefix: bool,
}

/// One equality of a lookup key; a side read optionally (`a?`) reads a missing field as `null`.
struct LookupKey {
    joined: crate::query::ExpressionFieldPath,
    outer: crate::query::ExpressionFieldPath,
    joined_optional: bool,
    outer_optional: bool,
}

impl LookupEquality {
    fn detect(pipeline: &PhysicalSubPipeline, outer_alias: Option<&str>) -> Option<Self> {
        // A leading scope filter never fails: rows it keeps or drops cannot
        // hide an error, and probes replay it on every candidate.
        let operators = pipeline.operators();
        let first = operators.iter().position(|operator| !matches!(operator, PhysicalOperator::Filter { predicate } if crate::query::is_scope_restriction(predicate)))?;
        let PhysicalOperator::Filter { predicate } = &operators[first] else {
            return None;
        };
        let outer_alias = outer_alias?;
        let is_outer = |field: &crate::query::ExpressionFieldPath| field.first() == outer_alias;
        let mut conjuncts = Vec::new();
        crate::helpers::collect_conjuncts(predicate, &mut conjuncts);
        let mut keys = Vec::new();
        for conjunct in conjuncts {
            match conjunct.view() {
                // `exists` never fails: skipping rows cannot hide an error.
                crate::query::ExpressionView::Call { function: "exists", .. } => {}
                crate::query::ExpressionView::Binary { left, operator: crate::query::BinaryOperator::Equal, right } => {
                    let (Some(left), Some(right)) = (left.ungrouped().as_read_field(), right.ungrouped().as_read_field()) else { break };
                    let ((joined, joined_optional), (outer, outer_optional)) = match (is_outer(left.0), is_outer(right.0)) {
                        (false, true) => (left, right),
                        (true, false) => (right, left),
                        _ => break,
                    };
                    keys.push(LookupKey { joined: joined.clone(), outer: outer.clone(), joined_optional, outer_optional });
                }
                crate::query::ExpressionView::Call { function: "starts_with", arguments: [text, prefix] } if keys.is_empty() => {
                    let (outer, joined) = (text.ungrouped().as_field()?, prefix.ungrouped().as_field()?);
                    return (outer.first() == outer_alias && joined.first() != outer_alias).then(|| Self { keys: vec![LookupKey { joined: joined.clone(), outer: outer.clone(), joined_optional: false, outer_optional: false }], prefix: true });
                }
                _ => break,
            }
        }
        (!keys.is_empty()).then_some(Self { keys, prefix: false })
    }

    fn outer(&self) -> &crate::query::ExpressionFieldPath {
        &self.keys[0].outer
    }

    fn composite(&self) -> bool {
        self.keys.len() > 1
    }

    /// The key of the joined row (`outer`: of the outer row) `fields` read: its value, or the array of the values of a
    /// composite key; `None` when one is missing.
    fn key_of(&self, fields: &crate::query::LookupFields<'_>, outer: bool) -> Option<Value> {
        let mut values = self.keys.iter().map(|key| {
            let (field, optional) = if outer { (&key.outer, key.outer_optional) } else { (&key.joined, key.joined_optional) };
            match crate::query::ExpressionFieldResolver::resolve_field(fields, field) {
                crate::query::SemanticValue::Present(value) => Some(value),
                crate::query::SemanticValue::Missing => optional.then_some(Value::Null),
            }
        }).collect::<Option<Vec<_>>>()?;
        if values.len() == 1 { values.pop() } else { Some(Value::Array(values.into())) }
    }
}

/// Kind of an equality key, for the kinds whose comparison raises an error.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    Number,
    String,
    Other,
}

/// Hash key grouping values that `==` may find equal. Numbers of any kind
/// share their numeric value; a bucket may hold a few extra rows, which the
/// `where` then rejects.
fn equality_key(value: &Value) -> (KeyKind, Vec<u8>) {
    match value {
        Value::Number(number) => {
            let mut key = vec![b'n'];
            key.extend_from_slice(&number_key_bits(*number).to_be_bytes());
            (KeyKind::Number, key)
        }
        Value::String(text) => {
            let mut key = vec![b's'];
            key.extend_from_slice(text.as_bytes());
            (KeyKind::String, key)
        }
        Value::Null => (KeyKind::Other, vec![b'z']),
        Value::Bool(flag) => (KeyKind::Other, vec![b'b', u8::from(*flag)]),
        // An array (a composite key): the keys of its items, each after its length.
        Value::Array(items) => {
            let mut key = vec![b'a'];
            for item in items.iter() {
                let item = equality_key(item).1;
                key.extend_from_slice(&u32::try_from(item.len()).unwrap_or(u32::MAX).to_be_bytes());
                key.extend_from_slice(&item);
            }
            (KeyKind::Other, key)
        }
        other => {
            let mut key = vec![b'j'];
            key.extend_from_slice(crate::helpers::value_to_json(other).to_string().as_bytes());
            (KeyKind::Other, key)
        }
    }
}

/// One indexed row: loaded, or known by locator until a probe needs it.
enum IndexedRow {
    Loaded(ExecutionRow),
    Pending(ProjectedRowLocator),
}

/// Joined rows indexed on their equality key, in collection order.
struct LookupIndex {
    /// Rows of the joined collection read to build the index.
    scanned: u64,
    rows: Vec<IndexedRow>,
    buckets: HashMap<Vec<u8>, Vec<u32>, std::hash::BuildHasherDefault<crate::query::keys::KeyHasher>>,
    /// Rows whose key is a number or a string: comparing those two kinds
    /// raises an error, so they stay candidates for keys of the other kind.
    numbers: Vec<u32>,
    strings: Vec<u32>,
    /// Rows without the key: the `where` decides (a guard, or an error).
    missing: Vec<u32>,
    /// Rows whose key is neither a number nor a string.
    others: Vec<u32>,
    /// Byte lengths of the string keys, for a prefix correlation.
    lengths: std::collections::BTreeSet<usize>,
    /// Rows keyed by an array (a composite key), by the kinds of its items:
    /// an item that is a number (string) where the outer key's is a string
    /// (number) raises, so those rows stay candidates.
    shapes: HashMap<Vec<u8>, Vec<u32>>,
}

/// The kinds of the items of an array key ([`equality_key`]): `n`, `s` or `o`.
fn item_kinds(key: &[u8]) -> Option<Vec<u8>> {
    let mut rest = key.strip_prefix(b"a")?;
    let mut kinds = Vec::new();
    while let Some((length, tail)) = rest.split_first_chunk::<4>() {
        let (item, tail) = tail.split_at(u32::from_be_bytes(*length) as usize);
        kinds.push(match item.first() { Some(b'n') => b'n', Some(b's') => b's', _ => b'o' });
        rest = tail;
    }
    Some(kinds)
}

impl LookupIndex {
    fn push(&mut self, row: IndexedRow, key: Option<(KeyKind, Vec<u8>)>) {
        let position = self.rows.len() as u32;
        self.rows.push(row);
        match key {
            None => self.missing.push(position),
            Some((kind, key)) => {
                match kind {
                    KeyKind::Number => self.numbers.push(position),
                    KeyKind::String => {
                        self.strings.push(position);
                        self.lengths.insert(key.len() - 1);
                    }
                    KeyKind::Other => self.others.push(position),
                }
                if let Some(kinds) = item_kinds(&key) {
                    self.shapes.entry(kinds).or_default().push(position);
                }
                self.buckets.entry(key).or_default().push(position);
            }
        }
    }

    /// Positions of every row that can pass the correlation for `outer_key`,
    /// in collection order. A prefix correlation probes the outer text's
    /// prefixes of every key length; rows keyed by a number or another
    /// kind stay candidates (the `where` decides, or raises its error).
    fn candidates(&self, outer_key: Option<&Value>, prefix: bool) -> Vec<u32> {
        let Some(outer_key) = outer_key else {
            return (0..self.rows.len() as u32).collect();
        };
        if prefix {
            let Value::String(text) = outer_key else {
                return (0..self.rows.len() as u32).collect();
            };
            let mut candidates = [&self.missing, &self.numbers, &self.others].into_iter().flatten().copied().collect::<Vec<_>>();
            let mut key = vec![b's'];
            for length in self.lengths.iter().copied().filter(|length| text.is_char_boundary(*length)) {
                key.truncate(1);
                key.extend_from_slice(&text.as_bytes()[..length]);
                candidates.extend(self.buckets.get(&key).into_iter().flatten());
            }
            candidates.sort_unstable();
            return candidates;
        }
        let (kind, key) = equality_key(outer_key);
        let mut candidates = self.missing.clone();
        candidates.extend(self.buckets.get(&key).into_iter().flatten());
        if let Some(outer) = item_kinds(&key) {
            let raises = |kinds: &Vec<u8>| kinds.len() == outer.len() && kinds.iter().zip(&outer).any(|pair| matches!(pair, (b'n', b's') | (b's', b'n')));
            candidates.extend(self.shapes.iter().filter(|(kinds, _)| raises(kinds)).flat_map(|(_, rows)| rows));
        }
        match kind {
            KeyKind::Number => candidates.extend(&self.strings),
            KeyKind::String => candidates.extend(&self.numbers),
            KeyKind::Other => {}
        }
        candidates.sort_unstable();
        candidates
    }
}

/// Outer rows retained by a lookup whose outer side is bounded
/// ([`prefix_row_bound`]): up to this many, their keys are collected first so
/// the index only holds joined rows that can match one of them.
const SEMI_JOIN_MAX_OUTER_ROWS: usize = 4096;

/// Keys of every outer row of a bounded lookup. A joined row is indexed when
/// it could be a candidate for one of them (see [`LookupIndex::candidates`]):
/// its key is missing, equal to an outer key, or a number (string) while an
/// outer key is a string (number), a pairing whose comparison raises.
#[derive(Default)]
struct LookupKeyFilter {
    numbers: KeySet<u64>,
    strings: KeySet<Arc<str>>,
    others: KeySet<Vec<u8>>,
}

type KeySet<K> = std::collections::HashSet<K, std::hash::BuildHasherDefault<crate::query::keys::KeyHasher>>;

impl LookupKeyFilter {
    fn insert(&mut self, value: &Value) {
        match value {
            Value::Number(number) => { self.numbers.insert(number_key_bits(*number)); }
            Value::String(text) => { self.strings.insert(Arc::clone(text)); }
            other => { self.others.insert(equality_key(other).1); }
        }
    }

    fn admits(&self, key: Option<&Value>) -> bool {
        match key {
            None => true,
            Some(Value::Number(number)) => self.admits_number(*number),
            Some(Value::String(text)) => self.admits_string(text),
            Some(other) => self.others.contains(&equality_key(other).1),
        }
    }

    /// [`Self::admits`] on a borrowed storage value.
    fn admits_ref(&self, key: Option<&crate::storage::ProjectedValueRef<'_>>) -> bool {
        use crate::storage::ProjectedValueRef;
        match key {
            None => true,
            Some(ProjectedValueRef::String(text)) => self.admits_string(text),
            Some(ProjectedValueRef::Float(value)) => self.admits_number(crate::Number::Float(*value)),
            Some(ProjectedValueRef::Signed(value)) => self.admits_number(crate::Number::Signed(*value)),
            Some(ProjectedValueRef::Unsigned(value)) => self.admits_number(crate::Number::Unsigned(*value)),
            Some(other) => self.admits(Some(&other.to_value())),
        }
    }

    fn admits_number(&self, number: crate::Number) -> bool {
        !self.strings.is_empty() || self.numbers.contains(&number_key_bits(number))
    }

    fn admits_string(&self, text: &str) -> bool {
        // A few keys compare faster than they hash.
        !self.numbers.is_empty()
            || if self.strings.len() <= 8 { self.strings.iter().any(|key| key.as_ref() == text) } else { self.strings.contains(text) }
    }
}

/// Numeric value of a key as [`equality_key`] hashes it.
fn number_key_bits(number: crate::Number) -> u64 {
    crate::helpers::normalize_zero(number.to_f64()).to_bits()
}

/// How many rows `prefix` emits at most: the smallest row bound of its
/// stages, when no later stage can emit more rows than it receives.
fn prefix_row_bound(prefix: &PhysicalPlan) -> Option<usize> {
    let mut bound = None;
    for operator in prefix.operators() {
        let properties = operator.execution_properties();
        if matches!(properties.cardinality, crate::query::CardinalityEffect::Expand | crate::query::CardinalityEffect::Unknown) {
            bound = None;
        }
        if let Some(count) = properties.stream_row_bound() {
            bound = Some(bound.map_or(count, |bound: usize| bound.min(count)));
        }
    }
    bound
}

thread_local! {
    /// Set when a scan on this thread built partial documents; blocking cores
    /// read it to report [`ExecutionStrategy::PartialDocuments`].
    static PARTIAL_DOCUMENTS_USED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Returns and clears whether partial documents were built on this thread.
fn take_partial_documents_used() -> bool {
    PARTIAL_DOCUMENTS_USED.with(|used| used.replace(false))
}

/// Stored fields to decode for a field demand, or `None` when the storage
/// cannot read chosen fields natively. A nested path is read on its own,
/// unless its top-level field is demanded too; `_id` comes from the row
/// identifier.
fn partial_source_fields(read: &dyn crate::storage::StorageRead, demand: &[crate::query::ExpressionFieldPath]) -> Option<Vec<crate::query::ExpressionFieldPath>> {
    if read.support(StorageReadCapability::ProjectedValuesGatedUnordered) != crate::storage::StorageSupport::Native {
        return None;
    }
    let mut fields = Vec::new();
    for field in demand.iter().filter(|field| field.first() != "_id") {
        let top_demanded = demand.iter().any(|other| other.len() == 1 && other.first() == field.first());
        let field = if top_demanded { crate::query::ExpressionFieldPath::new([field.first()]).ok()? } else { field.clone() };
        if !fields.contains(&field) {
            fields.push(field);
        }
    }
    Some(fields)
}

/// In-memory `group` fed from batches or rows: a row's key is found in a
/// [`crate::query::keys::KeyTable`] from its typed cells, and the encoded
/// key of a group is built once, when the group is created. Output order,
/// the `group | limit` frontier and the working-set estimate use that
/// encoded key, as the row-at-a-time consumer does. Measures fold column by
/// column ([`crate::query::DenseGroups::push_batch`]).
struct BatchGroups<'a> {
    keys: &'a [crate::query::ExpressionFieldPath],
    /// Position of each grouping key among the group input fields.
    key_indexes: &'a [usize],
    memory: &'a StageMemory<'a>,
    limit: Option<usize>,
    table: crate::query::keys::KeyTable,
    /// Group of each key of `table` ([`crate::query::DenseGroups::SKIP`]:
    /// left out of a bounded group, for good: its frontier only shrinks;
    /// [`UNSEEN_KEY`]: not looked up yet).
    key_groups: Vec<u32>,
    /// Encoded key of each group, `None` once evicted.
    groups: Vec<Option<Arc<[u8]>>>,
    /// Key of each group in `table` (`u32::MAX`: not seen since a reset).
    group_keys: Vec<u32>,
    dense: crate::query::DenseGroups,
    /// Bounded group: kept groups by encoded key.
    ordered: BTreeMap<Arc<[u8]>, u32>,
    estimated_bytes: usize,
    exceeded: bool,
    key_buffer: Vec<u8>,
    batch_keys: Vec<u32>,
    batch_groups: Vec<u32>,
    /// Bounded group, once full: per value of the first key field,
    /// [`ABOVE_FRONTIER`] when it sorts above the frontier's (then so does
    /// every key holding it, for good), else the frontier version it was
    /// found not to (0: never checked). One word, read once per row.
    first_frontier: Vec<u32>,
    frontier_version: u32,
    component: Vec<u8>,
    /// Batch positions not left out by their first key field.
    kept: Vec<u32>,
    /// Rows of a batch not left out by their first number ([`Self::narrow`]).
    narrowed: Vec<u32>,
}

/// Decides a block from its columns at the given positions
/// ([`Engine::stream_slot_batches_gated`]).
type BlockAdmit<'g> = &'g mut dyn FnMut(&crate::storage::ProjectedBlock<'_, '_>, &[usize]) -> EngineResult<bool>;

/// Fields and admission of a consumer's block gate
/// ([`Engine::stream_slot_batches_gated`]), whether it only refuses blocks
/// (their rows otherwise go on as usual), and whether the blocks it takes
/// need no prefix (it computes the prefix's fields itself).
type BlockGateOn<'g> = (&'g [crate::query::ExpressionFieldPath], BlockAdmit<'g>, bool, bool);

/// A `derive` that is the whole prefix of a block-folded `group`, every
/// field of which is a group measure of arithmetic over source fields
/// ([`crate::query::BlockKernel`]): the fold computes them on each block
/// instead of the batch runner. `sources` are the fields the block gate
/// reads; `inputs` gives each group input field from them.
struct FusedDerive { sources: Vec<crate::query::ExpressionFieldPath>, inputs: Vec<FusedInput> }

enum FusedInput { Source(usize), Kernel(crate::query::BlockKernel) }

fn fused_derive(prefix: &PhysicalPlan, fields: &[crate::query::ExpressionFieldPath], key_indexes: &[usize]) -> Option<FusedDerive> {
    let [PhysicalOperator::Custom { stage: Some(crate::query::Stage::Derive), arguments, .. }] = prefix.operators() else { return None };
    let assignments = crate::query::split_derive_assignments(arguments).ok()?;
    let targets = assignments.iter().map(|(target, _)| crate::query::ExpressionFieldPath::new([target.trim()]).ok()).collect::<Option<Vec<_>>>()?;
    // Every computed field feeds a measure, once: none is left to fail unseen.
    if (1..targets.len()).any(|at| targets[..at].contains(&targets[at])) || !targets.iter().all(|target| fields.iter().position(|field| field == target).is_some_and(|at| !key_indexes.contains(&at))) {
        return None;
    }
    let mut sources = Vec::new();
    let mut source = |field: &crate::query::ExpressionFieldPath| -> Option<usize> {
        if field.len() != 1 || targets.contains(field) {
            return None;
        }
        Some(sources.iter().position(|known| known == field).unwrap_or_else(|| { sources.push(field.clone()); sources.len() - 1 }))
    };
    let mut inputs = Vec::with_capacity(fields.len());
    for field in fields {
        inputs.push(match targets.iter().position(|target| target == field) {
            Some(at) => FusedInput::Kernel(crate::query::BlockKernel::compile(&crate::query::parse_expression(assignments[at].1.trim()).ok()?, &mut source)?),
            None => FusedInput::Source(source(field)?),
        });
    }
    Some(FusedDerive { sources, inputs })
}

/// [`Engine::resolve_compact_group`] for the group at `at`, with the
/// top-level source fields whose values are all numbers; `None` when an
/// item stays undecided.
fn resolve_group_items(physical: &PhysicalPlan, at: usize, numbers: &std::collections::BTreeSet<String>) -> Option<PhysicalPlan> {
    let source = |path: &crate::query::ExpressionFieldPath| (path.len() == 1).then(|| numbers.contains(path.first()));
    let mut derived = Vec::<(crate::query::ExpressionFieldPath, Option<bool>)>::new();
    let kind = |derived: &[(crate::query::ExpressionFieldPath, Option<bool>)], path: &crate::query::ExpressionFieldPath| derived.iter().rev().find(|(field, _)| field == path).map_or_else(|| source(path), |(_, kind)| *kind);
    for operator in &physical.operators()[..at] {
        match operator {
            PhysicalOperator::Filter { .. } | PhysicalOperator::Limit { .. } | PhysicalOperator::Skip { .. } | PhysicalOperator::Sort { .. } => {}
            PhysicalOperator::Custom { stage: Some(crate::query::Stage::Derive), arguments, .. } => {
                for (target, text) in crate::query::split_derive_assignments(arguments).ok()? {
                    let expression = crate::query::parse_expression(text.trim()).ok()?;
                    let summable = crate::query::summable(&expression, &|path| kind(&derived, path));
                    derived.push((crate::query::ExpressionFieldPath::new(target.trim().split('.')).ok()?, summable));
                }
            }
            _ => return None,
        }
    }
    let PhysicalOperator::Group { keys, .. } = &physical.operators()[at] else { return None };
    let mut fields = vec![keys.first()?.clone()];
    for field in &keys[1..] {
        let (path, alias, _) = crate::query::compact_group_item(field)?;
        let text = path.to_string();
        fields.push(match kind(&derived, &path)? {
            true if alias.contains('.') => return None,
            true => crate::query::group_measure_marker("sum", &text, &alias).ok()?,
            false if alias == text => path,
            false => crate::query::group_key_marker(&text, &alias).ok()?,
        });
    }
    physical.with_operator(at, PhysicalOperator::group(fields).ok()?).ok()
}

/// A sort right after a `group`, and its bound ([`BatchGroups::finish`]).
type GroupTop<'k> = Option<(&'k [crate::query::SortKey], Option<usize>)>;

/// A first key value above the frontier ([`BatchGroups::first_frontier`]).
const ABOVE_FRONTIER: u32 = u32::MAX;

/// A key of `BatchGroups::key_groups` not looked up yet.
const UNSEEN_KEY: u32 = u32::MAX - 1;

/// Bytes past which a bounded group's key table starts over (small in debug
/// builds, so tests start over too).
const BOUNDED_KEY_TABLE_BYTES: usize = if cfg!(debug_assertions) { 256 << 10 } else { 64 << 20 };

impl<'a> BatchGroups<'a> {
    fn new(dense: crate::query::DenseGroups, keys: &'a [crate::query::ExpressionFieldPath], key_indexes: &'a [usize], memory: &'a StageMemory<'a>, limit: Option<usize>) -> Self {
        Self {
            keys,
            key_indexes,
            memory,
            limit,
            table: crate::query::keys::KeyTable::new(key_indexes.len()),
            key_groups: Vec::new(),
            groups: Vec::new(),
            group_keys: Vec::new(),
            dense,
            ordered: BTreeMap::new(),
            estimated_bytes: 0,
            exceeded: false,
            key_buffer: Vec::with_capacity(64),
            batch_keys: Vec::with_capacity(crate::query::BATCH_ROWS),
            batch_groups: Vec::with_capacity(crate::query::BATCH_ROWS),
            first_frontier: Vec::new(),
            frontier_version: 1,
            component: Vec::with_capacity(32),
            kept: Vec::with_capacity(crate::query::BATCH_ROWS),
            narrowed: Vec::with_capacity(crate::query::BATCH_ROWS),
        }
    }

    /// Folds a whole block of an unbounded one-key group: `columns` holds the
    /// block column of each group input field. Every block folds here, so
    /// rows fold in scan order.
    fn push_block(&mut self, block: &crate::storage::ProjectedBlock<'_, '_>, columns: &[usize]) -> Result<(), StorageError> {
        let key = columns[self.key_indexes[0]];
        self.push_block_inputs(block, key, &columns.iter().map(|column| &block.columns[*column]).collect::<Vec<_>>())
    }

    /// [`Self::push_block`] with the key in the block's column `key` and
    /// the group input fields in `inputs`, some computed from the block.
    fn push_block_inputs(&mut self, block: &crate::storage::ProjectedBlock<'_, '_>, key: usize, inputs: &[&crate::storage::BlockColumn<'_, '_>]) -> Result<(), StorageError> {
        if self.exceeded {
            return Ok(());
        }
        let mut keys = std::mem::take(&mut self.batch_keys);
        self.table.block_keys(&block.columns[key], block.codes[key], &mut keys);
        self.create_new(&keys)?;
        if !self.exceeded {
            self.dense.fold_block(&keys, inputs).map_err(execution_storage_error)?;
        }
        self.batch_keys = keys;
        Ok(())
    }

    /// Creates the groups of the new keys among `keys`, under an unbounded
    /// group, whose keys are its groups ([`Self::group_of`]).
    fn create_new(&mut self, keys: &[u32]) -> Result<(), StorageError> {
        for &key in keys {
            if key as usize >= self.group_keys.len() && self.group_of(key).map(|_| self.exceeded)? {
                break;
            }
        }
        Ok(())
    }

    /// Folds the selected rows of a batch; `columns` holds the group input
    /// fields.
    fn push_batch(&mut self, columns: &[Option<&crate::query::Column>], selection: &[u32]) -> Result<(), StorageError> {
        if self.exceeded {
            return Ok(());
        }
        let key_columns = self.key_indexes.iter().map(|index| columns[*index]).collect::<Vec<_>>();
        let mut narrowed = std::mem::take(&mut self.narrowed);
        let selection = if self.narrow(key_columns[0], selection, &mut narrowed) { narrowed.as_slice() } else { selection };
        let mut batch_groups = std::mem::take(&mut self.batch_groups);
        batch_groups.clear();
        let mut batch_keys = std::mem::take(&mut self.batch_keys);
        if self.limit.is_some() {
            // A row whose first key field sorts above the frontier's is left
            // out without building its key.
            self.table.batch_field(0, key_columns[0], selection, None);
            let mut kept = std::mem::take(&mut self.kept);
            kept.clear();
            for position in 0..selection.len() {
                if !self.first_above_frontier(self.table.batch_value(0, position)) {
                    kept.push(position as u32);
                }
            }
            for (field, column) in key_columns.iter().enumerate().skip(1) {
                self.table.batch_field(field, *column, selection, Some(&kept));
            }
            self.kept = kept;
            for position in 0..selection.len() {
                // Checked again: a group created by an earlier row of the
                // batch may have moved the frontier.
                if self.first_above_frontier(self.table.batch_value(0, position)) {
                    batch_groups.push(crate::query::DenseGroups::SKIP);
                    continue;
                }
                let key = self.table.key_at(position);
                match self.group_of(key) {
                    Ok(group) => batch_groups.push(group),
                    Err(error) => {
                        self.batch_groups = batch_groups;
                        self.batch_keys = batch_keys;
                        return Err(error);
                    }
                }
            }
        } else {
            self.table.batch(&key_columns, selection, &mut batch_keys);
            self.create_new(&batch_keys)?;
        }
        // An unbounded group's keys are its groups ([`Self::group_of`]).
        let groups = if self.limit.is_some() { &batch_groups } else { &batch_keys };
        let folded = if self.exceeded { Ok(()) } else { self.dense.push_batch(groups, selection, columns).map_err(execution_storage_error) };
        self.batch_keys = batch_keys;
        self.batch_groups = batch_groups;
        self.narrowed = narrowed;
        folded?;
        self.trim();
        Ok(())
    }

    /// Keeps in `narrowed` the rows of `selection` a full bounded group may
    /// still keep by its first key field: a number or a text whose key
    /// component sorts above the frontier's first one is left out before any
    /// id is looked up. `false` (nothing decided) while the group is not full.
    fn narrow(&self, column: Option<&crate::query::Column>, selection: &[u32], narrowed: &mut Vec<u32>) -> bool {
        use crate::query::Tag;
        let (Some(limit), Some(column)) = (self.limit, column) else { return false };
        let Some((largest, _)) = self.ordered.last_key_value().filter(|_| self.ordered.len() >= limit) else { return false };
        // A component is its presence byte, its tag (3 signed, 4 unsigned,
        // 5 float), then a number's big-endian word: as these bytes, it
        // compares as (presence, tag, word), with no word but a number's.
        let at = crate::query::keys::KeyTable::KEY_HEADER;
        let frontier = match largest.get(at..) {
            Some([1, tag @ 3..=5, word @ ..]) if word.len() >= 8 => (1u8, *tag, u64::from_be_bytes(word[..8].try_into().expect("eight bytes"))),
            Some([1, tag @ 0..=2, ..]) => (1, *tag, 0),
            Some([1, 6, ..]) => return narrow_text(column, &largest[at..], selection, narrowed),
            Some([1, ..]) => return false,
            _ => (0, 0, 0),
        };
        narrowed.clear();
        narrowed.extend_from_slice(selection);
        crate::query::keep(narrowed, |row| {
            let tag = match column.tag(row) { Tag::Signed => 3, Tag::Unsigned => 4, Tag::Float => 5, _ => return true };
            (1, tag, column.bits(row)) <= frontier
        });
        true
    }

    /// Folds one row, its group input fields in `values`.
    fn push(&mut self, values: &[Option<ProjectedValueRef<'_>>]) -> Result<(), StorageError> {
        if self.exceeded {
            return Ok(());
        }
        let key_values = self.key_indexes.iter().map(|index| values[*index].clone()).collect::<Vec<_>>();
        let key = self.table.row(&key_values);
        let group = self.group_of(key)?;
        if self.exceeded || group == crate::query::DenseGroups::SKIP {
            return Ok(());
        }
        self.dense.push(group, values).map_err(execution_storage_error)?;
        self.trim();
        Ok(())
    }

    /// Whether value `id` of the first key field sorts above the first
    /// field of the largest kept key of a full bounded group: a key holding
    /// it is above that key and would be left out.
    #[inline]
    fn first_above_frontier(&mut self, id: u32) -> bool {
        let Some(limit) = self.limit else { return false };
        let index = id as usize;
        match self.first_frontier.get(index) {
            Some(&ABOVE_FRONTIER) => return true,
            Some(&version) if version == self.frontier_version => return false,
            _ if self.ordered.len() < limit => return false,
            Some(_) => {}
            None => self.first_frontier.resize(index + 1, 0),
        }
        self.component.clear();
        self.table.write_component(0, id, &mut self.component);
        let (largest, _) = self.ordered.last_key_value().expect("a full bounded group has a frontier");
        let start = crate::query::keys::KeyTable::KEY_HEADER;
        // Components are prefix-free: two different ones differ within the
        // shorter, so this byte range decides when it differs.
        let above = largest.get(start..start + self.component.len()).is_some_and(|frontier| self.component.as_slice() > frontier);
        self.first_frontier[index] = if above { ABOVE_FRONTIER } else { self.frontier_version };
        above
    }

    /// Group of `key`, created when the key is new.
    #[inline]
    fn group_of(&mut self, key: u32) -> Result<u32, StorageError> {
        // Unbounded, every key gets its group when first seen, in key order:
        // a key is its group.
        if self.limit.is_none() {
            return if (key as usize) < self.group_keys.len() { Ok(key) } else { self.create(key) };
        }
        if let Some(&group) = self.key_groups.get(key as usize) {
            if group != UNSEEN_KEY {
                return Ok(group);
            }
        }
        self.table.write_key(key, &mut self.key_buffer);
        let group = match self.ordered.get(self.key_buffer.as_slice()) {
            // A kept group whose key was forgotten by a reset.
            Some(group) => *group,
            None => self.create(key)?,
        };
        if group != crate::query::DenseGroups::SKIP {
            self.group_keys[group as usize] = key;
        }
        // Keys skip numbers when rows left out by the first key field
        // still numbered their value (single-field keys).
        if key as usize >= self.key_groups.len() {
            self.key_groups.resize(key as usize + 1, UNSEEN_KEY);
        }
        self.key_groups[key as usize] = group;
        Ok(group)
    }

    /// Creates the group whose encoded key is in `key_buffer`;
    /// [`crate::query::DenseGroups::SKIP`] when it is left out (bounded
    /// group) or the working set is exceeded.
    fn create(&mut self, key: u32) -> Result<u32, StorageError> {
        debug_assert!(self.limit.is_some() || key as usize == self.group_keys.len(), "unbounded keys are their groups");
        // An unbounded group's key values stay in `table`, which counts them.
        let state_estimate = group_state_estimate(if self.limit.is_some() { self.key_buffer.len() } else { 0 }, self.keys.len());
        if let Some(limit) = self.limit {
            if self.ordered.len() >= limit {
                let (largest, evicted) = self.ordered.last_key_value().expect("bounded group frontier exists");
                if self.key_buffer.as_slice() >= largest.as_ref() {
                    return Ok(crate::query::DenseGroups::SKIP);
                }
                let (largest_len, evicted) = (largest.len(), *evicted);
                self.ordered.pop_last();
                self.forget(evicted);
                self.estimated_bytes = self.estimated_bytes.saturating_sub(group_state_estimate(largest_len, self.keys.len()));
            }
            if !self.memory.holds(self.estimated_bytes.saturating_add(state_estimate)) {
                return Err(StorageError::backend("bounded group limit exceeds the governed working set"));
            }
        } else if !self.memory.fits(self.estimated_bytes.saturating_add(state_estimate).saturating_add(self.table.bytes())) {
            self.exceeded = true;
            self.groups.clear();
            return Ok(crate::query::DenseGroups::SKIP);
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(state_estimate);
        let group = self.dense.add_keys(if self.limit.is_some() { (0..self.key_indexes.len()).map(|field| self.table.value(key, field).cloned()).collect() } else { Vec::new() });
        // An unbounded group orders its keys at the end, by rank
        // ([`crate::query::keys::KeyTable::sort_by_key`]).
        if self.limit.is_some() {
            let encoded: Arc<[u8]> = Arc::from(self.key_buffer.as_slice());
            self.ordered.insert(Arc::clone(&encoded), group);
            self.frontier_version = self.frontier_version % (ABOVE_FRONTIER - 1) + 1;
            self.groups.push(Some(encoded));
        } else {
            self.groups.push(None);
        }
        self.group_keys.push(key);
        Ok(group)
    }

    /// Drops an evicted group: its key is left out from now on.
    fn forget(&mut self, group: u32) {
        self.groups[group as usize] = None;
        self.dense.evict(group);
        let key = self.group_keys[group as usize];
        if let Some(slot) = self.key_groups.get_mut(key as usize) {
            *slot = crate::query::DenseGroups::SKIP;
        }
    }

    /// A bounded group keeps few groups but may see many keys: past a size,
    /// the key table starts over, and keys seen again find their kept group
    /// by encoded key.
    fn trim(&mut self) {
        if self.limit.is_none() || self.table.bytes() <= BOUNDED_KEY_TABLE_BYTES {
            return;
        }
        self.table = crate::query::keys::KeyTable::new(self.key_indexes.len());
        self.key_groups.clear();
        self.group_keys.fill(u32::MAX);
        self.first_frontier.clear();
    }

    /// Groups in encoded-key order, as rows; `None` when the working set
    /// was exceeded. Under `top` (a sort right after, and its bound), the
    /// groups come sorted ([`GroupRows::sorted`]) when the sort reads only
    /// their fields: each group's sort values are read once, only the first
    /// `limit` kept (ties in key order).
    fn finish(self, top: GroupTop<'_>) -> EngineResult<Option<GroupRows>> {
        if self.exceeded {
            return Ok(None);
        }
        let mut dense = self.dense;
        let groups = if self.limit.is_some() {
            let mut groups = self.groups.into_iter().enumerate().filter_map(|(group, key)| Some((key?, group as u32))).collect::<Vec<_>>();
            groups.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            groups.into_iter().map(|(_, group)| group).collect::<Vec<_>>()
        } else {
            let mut groups = (0..self.group_keys.len() as u32).collect::<Vec<_>>();
            let group_keys = &self.group_keys;
            self.table.sort_by_key(&mut groups, |group| group_keys[*group as usize]);
            groups
        };
        let names = top.map(|(keys, _)| keys.iter().map(|key| key.field().first()).collect::<Vec<_>>()).filter(|names| !names.contains(&"_id"));
        let (Some((keys, limit)), Some(names), None) = (top, names, self.limit) else {
            let keys = self.limit.is_none().then_some((self.table, self.group_keys));
            return Ok(Some(GroupRows { dense, keys, groups: groups.into_iter().enumerate().collect::<Vec<_>>().into_iter(), sorted: false }));
        };
        type Keyed = (usize, Vec<Option<Value>>);
        let compare = |left: &Keyed, right: &Keyed| crate::query::compare_owned_projected_rows(keys, &left.1, &right.1);
        let mut selection = limit.map(crate::query::TopSelection::new);
        let mut keyed = Vec::new();
        for (index, group) in groups.iter().enumerate() {
            dense.set_keys(*group, (0..self.key_indexes.len()).map(|field| self.table.value(self.group_keys[*group as usize], field).cloned()).collect());
            let fields = dense.fields(*group, &names).map_err(EngineError::execution)?;
            let entry = (index, keys.iter().map(|key| crate::query::path_value(&fields, key.field()).cloned()).collect());
            match &mut selection {
                Some(selection) => drop(selection.offer(entry, |entry, worst| Ok(compare(entry, worst)?.is_lt()), Ok, compare).map_err(EngineError::execution)?),
                None => keyed.push(entry),
            }
        }
        keyed = selection.map_or(keyed, crate::query::TopSelection::into_arrival_order);
        let mut failed = None;
        keyed.sort_by(|left, right| compare(left, right).unwrap_or_else(|error| { failed.get_or_insert(error); std::cmp::Ordering::Equal }));
        match failed {
            Some(error) => Err(EngineError::execution(error)),
            None => Ok(Some(GroupRows { dense, keys: None, groups: keyed.into_iter().map(|(index, _)| (index, groups[index])).collect::<Vec<_>>().into_iter(), sorted: true })),
        }
    }
}

/// Finished groups, each built into its row when it is taken: the rows of
/// all groups never coexist.
struct GroupRows {
    dense: crate::query::DenseGroups,
    /// An unbounded group's keys and the key of each group, whose values
    /// the rows take as they are built.
    keys: Option<(crate::query::keys::KeyTable, Vec<u32>)>,
    groups: std::vec::IntoIter<(usize, u32)>,
    /// Already in the order of the sort after the group.
    sorted: bool,
}

impl Iterator for GroupRows {
    type Item = EngineResult<ExecutionRow>;

    fn next(&mut self) -> Option<Self::Item> {
        let (index, group) = self.groups.next()?;
        let ordinal = usize_to_u64_saturating(index).saturating_add(1);
        if let Some((table, keys)) = &self.keys {
            self.dense.set_keys(group, (0..table.width()).map(|field| table.value(keys[group as usize], field).cloned()).collect());
        }
        Some(self.dense.finish(group, ordinal).map(ExecutionRow::synthetic).map_err(EngineError::execution))
    }
}

/// Reads `source_fields` of `collection` in batches and runs `program` on
/// each ([`crate::query::BatchRunner`]). A batch that ran is handed to
/// `on_batch`; a batch that aborted is replayed row by row through
/// `on_row` (the row-at-a-time path), with its source values. Both run
/// inside the storage visitor, as the row-at-a-time path does, so an error
/// surfaces exactly as it would there. Returns the rows read. Under
/// [`ScanOptions::without_locators`], rows carry [`NO_LOCATOR`].
///
/// Leading filters that read only some fields see each block's columns of
/// those first ([`crate::query::BatchProgram::head`]): a block none of whose
/// rows passes them is not decoded further, its rows dropped by the filters;
/// they stop deciding first once they refuse fewer than one block in eight.
/// `consumer` may refuse blocks too, from its own columns, its rows then
/// only read.
#[allow(clippy::too_many_arguments)]
fn scan_batched(
    read: &dyn crate::storage::StorageRead,
    collection: &CollectionId,
    options: ScanOptions,
    storage_fields: &[crate::FieldPath],
    program: &Arc<crate::query::BatchProgram>,
    pruning: Option<&crate::query::GroupPruning>,
    mut consumer: Option<crate::storage::BlockGate<'_>>,
    on_batch: &mut dyn FnMut(&crate::query::BatchRunner) -> Result<bool, StorageError>,
    on_row: &mut dyn FnMut(DocumentId, DocumentVersion, &[Option<Value>]) -> Result<bool, StorageError>,
) -> Result<u64, StorageError> {
    let options = options.with_number_codes(program.number_codes());
    let mut runner = crate::query::BatchRunner::new(Arc::clone(program));
    let mut values = Vec::new();
    let scanned = std::cell::Cell::new(0u64);
    let mut flush = |runner: &mut crate::query::BatchRunner| -> Result<bool, StorageError> {
        scanned.set(scanned.get().saturating_add(runner.len() as u64));
        let more = if runner.run().is_ok() {
            on_batch(runner)?
        } else {
            let mut more = true;
            for row in 0..runner.len() {
                let (id, version) = if options.locators() { runner.id(row) } else { NO_LOCATOR };
                runner.source_values(row, &mut values);
                if !on_row(id, version, &values)? {
                    more = false;
                    break;
                }
            }
            more
        };
        runner.clear();
        Ok(more)
    };
    let mut stopped = false;
    // Rows of refused blocks: all of them, and those the filters dropped
    // not yet counted by a batch.
    let (refused, dropped) = (std::cell::Cell::new(0usize), std::cell::Cell::new(0usize));
    if read.supports_projected_blocks() {
        let head = program.head();
        let mut head_runner = crate::query::BatchRunner::new(Arc::clone(program));
        let (tried, refusals) = (std::cell::Cell::new(0usize), std::cell::Cell::new(0usize));
        let fields = (0..storage_fields.len()).map(|field| head.as_ref().is_some_and(|(_, reads)| reads[field]) || consumer.as_ref().is_some_and(|gate| gate.fields[field])).collect();
        let gated = head.is_some() || consumer.is_some();
        let mut admit = |block: &crate::storage::ProjectedBlock<'_, '_>| -> Result<bool, StorageError> {
            if let Some((stages, _)) = head.as_ref().filter(|_| tried.get() < 32 || refusals.get() * 8 >= tried.get()) {
                tried.set(tried.get() + 1);
                let mut passes = false;
                for from in (0..block.rows).step_by(crate::query::BATCH_ROWS) {
                    head_runner.push_rows(&[], block.columns, block.codes, from, crate::query::BATCH_ROWS.min(block.rows - from));
                    // An aborted batch is decided row by row, with its errors.
                    passes |= head_runner.run_stages(*stages).is_err() || !head_runner.selection().is_empty();
                    head_runner.clear();
                }
                if !passes {
                    refusals.set(refusals.get() + 1);
                    refused.set(refused.get() + block.rows);
                    dropped.set(dropped.get() + block.rows);
                    return Ok(false);
                }
            }
            let admitted = match consumer.as_mut() { Some(gate) => (gate.admit)(block)?, None => true };
            if !admitted {
                refused.set(refused.get() + block.rows);
            }
            Ok(admitted)
        };
        let gate = gated.then(|| crate::storage::BlockGate { fields, admit: &mut admit });
        // Row groups `pruning` proves the leading filters drop are left out
        // ([`crate::query::GroupPruning`]), never under a scan limit, which
        // counts rows before those filters.
        let pruning = pruning.filter(|_| options.limit().is_none());
        read.scan_projected_blocks_each(collection, options, storage_fields, &|facts| pruning.is_some_and(|pruning| pruning.skip(facts)), gate, &mut |block| {
            runner.skip(dropped.take());
            let (ids, columns) = (block.ids, block.columns);
            let mut from = 0;
            while from < block.rows {
                let count = (crate::query::BATCH_ROWS - runner.len()).min(block.rows - from);
                runner.push_rows(ids, columns, block.codes, from, count);
                from += count;
                if runner.is_full() && !flush(&mut runner)? {
                    stopped = true;
                    return Ok(false);
                }
            }
            Ok(true)
        })?;
    } else {
        read.scan_projected_row_refs_unordered_each(collection, options, storage_fields, &mut |id, version, row| {
            runner.push(id, version, row);
            if runner.is_full() && !flush(&mut runner)? {
                stopped = true;
                return Ok(false);
            }
            Ok(true)
        })?;
    }
    // The rows of pruned row groups count as refused, scanned and dropped,
    // as a store that prunes nothing counts them.
    if pruning.is_some() && !stopped && options.limit().is_none() && read.supports_projected_blocks() {
        let pruned = usize::try_from(read.count(collection)?).unwrap_or(usize::MAX).saturating_sub(scanned.get() as usize + runner.len() + refused.get());
        refused.set(refused.get() + pruned);
        dropped.set(dropped.get() + pruned);
    }
    let pending = dropped.take();
    runner.skip(pending);
    if !stopped && (!runner.is_empty() || pending > 0) {
        flush(&mut runner)?;
    }
    Ok(scanned.get().saturating_add(refused.get() as u64))
}

/// Locator handed to a visitor whose scan declined locators
/// ([`ScanOptions::without_locators`]).
const NO_LOCATOR: (DocumentId, DocumentVersion) = (DocumentId::from_bytes([0; 16]), DocumentVersion::new(0));

/// Kinds of each top-level field of `collection` held by every document,
/// from the storage's exact facts and bounds (`None`: no facts).
fn source_kinds(read: &dyn crate::storage::StorageRead, collection: &CollectionId) -> EngineResult<Option<impl Fn(&str) -> Option<crate::query::Kinds>>> {
    let Some(facts) = read.field_facts(collection).map_err(storage_engine_error)? else { return Ok(None) };
    let bounds = read.field_bounds(collection).map_err(storage_engine_error)?.unwrap_or_default();
    Ok(Some(move |name: &str| {
        facts.kinds_when_always_present(name).map(|kinds| {
            let kinds = crate::query::Kinds::from_kinds(kinds);
            bounds.get(name).map_or(kinds, |bounds| kinds.with_bounds(bounds))
        })
    }))
}

/// Whether every stage of `prefix` is a filter proven total from the
/// storage's exact field facts: it may drop rows, never fail.
fn only_total_filters(read: &dyn crate::storage::StorageRead, prefix: &PhysicalPlan) -> EngineResult<bool> {
    let operators = prefix.operators();
    if !operators.iter().all(|operator| matches!(operator, PhysicalOperator::Filter { .. })) {
        return Ok(false);
    }
    let Some(kinds) = source_kinds(read, prefix.source().collection())? else { return Ok(false) };
    let field = |path: &crate::query::ExpressionFieldPath| if path.len() == 1 { kinds(path.first()) } else { None };
    Ok(operators.iter().all(|operator| matches!(operator, PhysicalOperator::Filter { predicate } if crate::query::total_kinds(predicate, &field) == Some(crate::query::Kinds::of(crate::model::PhysicalKind::Bool)))))
}

/// First stage of `prefix` from which every later stage is proven total
/// ([`crate::query::CompiledStage::total_effects`]) from the storage's exact
/// field facts: those stages may run after a reservoir instead of before it.
/// Fields written by an earlier stage have unknown kinds.
fn sample_commuting_start(read: &dyn crate::storage::StorageRead, prefix: &PhysicalPlan) -> EngineResult<usize> {
    let operators = prefix.operators();
    let movable = |operator: &PhysicalOperator| matches!(operator, PhysicalOperator::Custom { writes: false, compiled: Some(_), .. }) && !operator.reads_document_id();
    let first = operators.iter().rposition(|operator| !movable(operator)).map_or(0, |position| position + 1);
    if first == operators.len() {
        return Ok(first);
    }
    let Some(source_kinds) = source_kinds(read, prefix.source().collection())? else {
        return Ok(operators.len());
    };
    'start: for start in first..operators.len() {
        let mut written = Vec::<(crate::query::ExpressionFieldPath, Option<crate::query::Kinds>)>::new();
        for operator in &operators[..start] {
            match operator.execution_properties().fields {
                crate::query::Fields::Preserved => {}
                crate::query::Fields::Rewritten(fields) => written.extend(fields.iter().map(|field| (field.clone(), None))),
                _ => continue 'start,
            }
        }
        for operator in &operators[start..] {
            let PhysicalOperator::Custom { compiled: Some(stage), .. } = operator else { continue 'start };
            let kinds = |path: &crate::query::ExpressionFieldPath| match written.iter().rev().find(|(field, _)| field.first() == path.first()) {
                Some((_, kinds)) => kinds.filter(|_| path.len() == 1),
                None if path.len() == 1 => source_kinds(path.first()),
                None => None,
            };
            let Some(effects) = stage.total_effects(&kinds) else { continue 'start };
            written.extend(effects);
        }
        return Ok(start);
    }
    Ok(operators.len())
}

/// Leading filters of `prefix` as a projected gate, and its other stages as
/// a slot program over the source fields they and `downstream` read; `None`
/// when a stage cannot run on slots or `downstream` is not top-level.
/// `expanding` admits stages that turn one row into several (`unwind`): the
/// caller then runs the program with [`crate::query::SlotProgram::run_each`].
fn slot_prefix(read: &dyn crate::storage::StorageRead, prefix: &PhysicalPlan, downstream: &[crate::query::ExpressionFieldPath], expanding: bool) -> EngineResult<Option<(ProjectedValuePipeline, crate::query::SlotProgram)>> {
    let operators = prefix.operators();
    let leading = operators.iter().take_while(|operator| matches!(operator, PhysicalOperator::Filter { .. })).count();
    let Some(fields) = crate::query::source_field_demand(operators, Some(downstream))
        .and_then(|demand| partial_source_fields(read, &demand))
    else {
        return Ok(None);
    };
    let Some(gate) = ProjectedValuePipeline::compile(&operators[..leading], fields).map_err(EngineError::execution)? else {
        return Ok(None);
    };
    let program = if expanding {
        crate::query::SlotProgram::compile_expanding(&operators[leading..], gate.layout().fields())
    } else {
        crate::query::SlotProgram::compile(&operators[leading..], gate.layout().fields())
    };
    let Some(program) = program else {
        return Ok(None);
    };
    Ok(program.slots(downstream).is_some().then_some((gate, program)))
}

/// Sort keys of the worst row a full Top-N keeps, for checking batch cells
/// against them without building values: only comparisons that cannot
/// fail and that the row path makes the same way (a number against a
/// number of its kind, text against text).
#[derive(Default)]
struct TopBound(Vec<KeyBound>);

enum KeyBound {
    Other,
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Text(Box<str>),
}

impl TopBound {
    fn of(top: &BoundedProjectedTopN) -> Self {
        let Some(values) = top.boundary() else { return Self::default() };
        Self(values.iter().map(|value| match value {
            Some(Value::Number(crate::model::Number::Signed(value))) => KeyBound::Signed(*value),
            Some(Value::Number(crate::model::Number::Unsigned(value))) => KeyBound::Unsigned(*value),
            Some(Value::Number(crate::model::Number::Float(value))) => KeyBound::Float(*value),
            Some(Value::String(text)) => KeyBound::Text(Box::from(text.as_ref())),
            _ => KeyBound::Other,
        }).collect())
    }

    /// Whether the row at `row` of the key `columns` cannot enter: its keys
    /// sort after the bound's, or equal them (a later equal row never
    /// displaces a kept one). `false` when it cannot tell.
    #[inline]
    fn rejects(&self, keys: &[crate::query::SortKey], columns: &[Option<&crate::query::Column>], row: usize) -> bool {
        use crate::query::Tag;
        self.rejects_cells(keys, |key| {
            let column = columns[key]?;
            Some(match column.tag(row) {
                Tag::Signed => ProjectedValueRef::Signed(column.bits(row) as i64),
                Tag::Unsigned => ProjectedValueRef::Unsigned(column.bits(row)),
                Tag::Float => ProjectedValueRef::Float(f64::from_bits(column.bits(row))),
                Tag::Str => ProjectedValueRef::String(column.str(row)),
                _ => return None,
            })
        })
    }

    /// Whether some row of a block can enter, told from the typed column of
    /// its first key alone (`None`: cannot tell so): a row whose first key
    /// sorts after the bound's cannot, nor one equal to it under one key.
    fn admits_block(&self, keys: &[crate::query::SortKey], column: &crate::storage::BlockColumn<'_, '_>) -> Option<bool> {
        use crate::storage::BlockColumn;
        let (descending, strict) = (matches!(keys.first()?.direction(), crate::query::SortDirection::Descending), keys.len() == 1);
        // Incomparable values (NaN) are left to the row path.
        let enters = |ordering: Option<std::cmp::Ordering>| ordering.is_none_or(|ordering| {
            let ordering = if descending { ordering.reverse() } else { ordering };
            if strict { ordering.is_lt() } else { ordering.is_le() }
        });
        Some(match (self.0.first()?, column) {
            (KeyBound::Float(bound), BlockColumn::Floats(values)) => values.iter().any(|value| enters(value.partial_cmp(bound))),
            (KeyBound::Signed(bound), BlockColumn::Signed(values)) => values.iter().any(|value| enters(Some(value.cmp(bound)))),
            (KeyBound::Unsigned(bound), BlockColumn::Unsigned(values)) => values.iter().any(|value| enters(Some(value.cmp(bound)))),
            (KeyBound::Text(bound), BlockColumn::Strings(values)) => values.iter().any(|value| enters(Some((*value).cmp(bound)))),
            _ => return None,
        })
    }

    /// [`Self::rejects`] for the key cells `cell` gives (`None`: cannot tell).
    #[inline]
    fn rejects_cells<'v>(&self, keys: &[crate::query::SortKey], cell: impl Fn(usize) -> Option<ProjectedValueRef<'v>>) -> bool {
        use std::cmp::Ordering;
        if self.0.is_empty() {
            return false;
        }
        for (index, (bound, key)) in self.0.iter().zip(keys).enumerate() {
            let ordering = match (bound, cell(index)) {
                (KeyBound::Signed(bound), Some(ProjectedValueRef::Signed(value))) => value.cmp(bound),
                (KeyBound::Unsigned(bound), Some(ProjectedValueRef::Unsigned(value))) => value.cmp(bound),
                (KeyBound::Float(bound), Some(ProjectedValueRef::Float(value))) => match value.partial_cmp(bound) {
                    Some(ordering) => ordering,
                    None => return false,
                },
                (KeyBound::Text(bound), Some(ProjectedValueRef::String(value))) => value.cmp(bound),
                _ => return false,
            };
            let ordering = if matches!(key.direction(), crate::query::SortDirection::Descending) { ordering.reverse() } else { ordering };
            match ordering {
                Ordering::Greater => return true,
                Ordering::Less => return false,
                Ordering::Equal => {}
            }
        }
        true
    }
}

/// One row of a streamed prefix held as slot values, with its identity.
#[derive(Clone, Copy)]
struct SlotRowRef<'a> {
    id: DocumentId,
    version: DocumentVersion,
    fields: &'a [crate::query::ExpressionFieldPath],
    values: &'a [Option<Value>],
}

/// Document holding the decoded top-level `fields` present in `values`.
/// The document of `fields`, read at `slots` of `values`.
fn selected_document(fields: &[crate::query::ExpressionFieldPath], slots: &[usize], values: &[Option<Value>]) -> Document {
    let picked = slots.iter().map(|slot| values.get(*slot).cloned().flatten()).collect::<Vec<_>>();
    crate::query::document_of_fields(fields, &picked)
}

fn partial_document(fields: &[crate::query::ExpressionFieldPath], values: &[Option<Value>]) -> Document {
    crate::query::document_of_fields(fields, values)
}

#[inline]
const fn reusable_projected_scan_options( options: ScanOptions, plan: &PhysicalPlan, projected: &ProjectedValuePipeline, ) -> ScanOptions {
    if matches!(
        (plan.source_projection_reuse(), projected.projection_reuse()),
        (
            crate::query::ProjectionReuse::Reusable,
            crate::query::ProjectionReuse::Reusable,
        )
    ) {
        options.with_reusable_projection()
    } else {
        options
    }
}

const fn storage_engine_error(error: StorageError) -> EngineError { EngineError::execution(ExecutionError::storage(error)) }
#[inline]
fn backend_storage_error(error: impl fmt::Display) -> StorageError { StorageError::backend(error.to_string()) }
/// A row evaluation failure raised inside a scan, worded as the document
/// path words it.
fn execution_storage_error(error: ExecutionError) -> StorageError { backend_storage_error(EngineError::execution(error)) }
#[inline]
fn engine_spill_error(error: io::Error) -> EngineError { EngineError::execution(spill_engine_error(error)) }

#[inline]
const fn group_state_estimate(key_len: usize, field_count: usize) -> usize {
    key_len
        .saturating_mul(2)
        .saturating_add(field_count.saturating_mul(64))
        .saturating_add(384)
}

/// Whether a stage runs row by row: on a `lookup`/`union` sub-pipeline,
/// or on the output of a blocking stage.
const fn is_row_operator(operator: &PhysicalOperator) -> bool {
    match operator {
        PhysicalOperator::Filter { .. } | PhysicalOperator::Select { .. } | PhysicalOperator::Skip { .. } | PhysicalOperator::Limit { .. } => true,
        PhysicalOperator::Custom { writes: false, stage, .. } => !matches!(stage, Some(crate::query::Stage::Sample | crate::query::Stage::Window | crate::query::Stage::Fill)),
        _ => false,
    }
}

/// How many rows a blocking core must produce for the streaming tail that
/// follows it, when the tail bounds its output: a leading `skip k | limit n`
/// needs `k + n` rows. Stages that turn each row into exactly one row
/// (`select`, `derive`, `rename`, `drop`, …) may sit in between.
fn tail_row_limit(tail: &[PhysicalOperator]) -> Option<usize> {
    let mut offset = 0usize;
    for operator in tail {
        let properties = operator.execution_properties();
        if let PhysicalOperator::Skip { count } = operator {
            offset = offset.saturating_add(*count);
        } else if let Some(bound) = properties.stream_row_bound() {
            // An exact-count stage must still see one row too many to fail.
            let extra = usize::from(properties.exact_rows().is_some());
            return Some(offset.saturating_add(bound).saturating_add(extra));
        } else if !preserves_each_row(&properties) {
            return None;
        }
    }
    None
}

/// Whether a stage turns every input row into exactly one output row.
fn preserves_each_row(properties: &crate::query::ExecutionProperties<'_>) -> bool {
    matches!(properties.flow, crate::query::Flow::Streaming)
        && matches!(properties.cardinality, crate::query::CardinalityEffect::Preserve)
        && matches!(properties.scope, crate::query::Scope::Row)
        && !properties.writes()
}

/// Decomposition of a governed read plan: a streaming prefix, a first
/// blocking core with a bounded executor, its row-local streaming tail, then
/// the later segments, each a blocking sink and its row-local tail
/// ([`sinks`]).
struct GovernedShape<'a> {
    blocking_index: usize,
    core_end: usize,
    tail: &'a [PhysicalOperator],
    /// The later segments, without the terminal `count`.
    rest: &'a [PhysicalOperator],
    count_alias: Option<&'a str>,
    output_limit: Option<usize>,
}

fn governed_shape(operators: &[PhysicalOperator]) -> Option<GovernedShape<'_>> {
    let blocking_index = operators.iter().position(|operator| {
        matches!(
            operator.execution_properties().flow,
            crate::query::Flow::GovernedBlocking
        )
    })?;
    if !operators[..blocking_index].iter().all(|operator| {
        matches!(
            operator.execution_properties().flow,
            crate::query::Flow::Streaming
        )
    }) {
        return None;
    }
    // A pair the core runs at once, when it applies; its stages run one after
    // the other otherwise.
    let pair = matches!(
        &operators[blocking_index..],
        [PhysicalOperator::Group { .. }, PhysicalOperator::Sort { .. }, ..] | [PhysicalOperator::Sort { .. }, PhysicalOperator::Distinct { .. }, ..]
    );
    if pair {
        if let Some(shape) = governed_shape_with_core(operators, blocking_index, 2) {
            return Some(shape);
        }
    }
    governed_shape_with_core(operators, blocking_index, 1)
}

fn governed_shape_with_core(operators: &[PhysicalOperator], blocking_index: usize, core_len: usize) -> Option<GovernedShape<'_>> {
    let core_end = blocking_index + core_len;
    let mut after = &operators[core_end..];
    let count_alias = after.last().and_then(PhysicalOperator::count_alias);
    if count_alias.is_some() {
        after = &after[..after.len() - 1];
    }
    let (tail, rest) = after.split_at(after.iter().position(|operator| !is_row_operator(operator)).unwrap_or(after.len()));
    sinks::later_segments(rest)?;
    let output_limit = tail_row_limit(tail);
    let core = &operators[blocking_index..core_end];
    let supported = match core {
        [operator] => {
            matches!(operator.execution_properties().order, Order::Ordered(_))
                || operator.execution_properties().retained_rows().is_some()
                || matches!(
                    operator,
                    PhysicalOperator::Distinct { .. } | PhysicalOperator::Group { .. } | PhysicalOperator::Pivot { .. }
                )
                || matches!(operator, PhysicalOperator::Union { .. })
                || matches!(
                    operator,
                    PhysicalOperator::Lookup { pipeline, .. } if sinks::runs_lookup_pipeline(pipeline)
                )
        }
        [PhysicalOperator::Group { .. }, PhysicalOperator::Sort { .. }] => true,
        [PhysicalOperator::Sort { .. }, PhysicalOperator::Distinct { .. }] => output_limit.is_some(),
        _ => false,
    };
    supported.then_some(GovernedShape {
        blocking_index,
        core_end,
        tail,
        rest,
        count_alias,
        output_limit,
    })
}

/// Per-scan state of a chain of streaming row-local stages: the skip and
/// declared row-bound budgets they have consumed so far.
struct StreamingRowChain<'a> {
    operators: &'a [PhysicalOperator],
    /// Per operator: whether it reads the virtual `_id` field.
    needs_id: Vec<bool>,
    skip_remaining: Vec<Option<usize>>,
    limit_remaining: Vec<Option<usize>>,
    filtered: u64,
}

impl<'a> StreamingRowChain<'a> {
    fn new(operators: &'a [PhysicalOperator]) -> Self {
        Self {
            operators,
            needs_id: operators.iter().map(PhysicalOperator::reads_document_id).collect(),
            skip_remaining: operators
                .iter()
                .map(|operator| match operator {
                    PhysicalOperator::Skip { count } => Some(*count),
                    _ => None,
                })
                .collect(),
            limit_remaining: operators
                .iter()
                .map(|operator| operator.execution_properties().stream_row_bound())
                .collect(),
            filtered: 0,
        }
    }

    /// Whether a stage lets no row through, so the source need not be read.
    fn has_empty_bound(&self) -> bool {
        self.limit_remaining.contains(&Some(0))
    }
}

/// Hands one streamed row to the visitor, or only counts it under a terminal `count`.
fn emit_streamed_row( visitor: &mut dyn FnMut(StoredDocument) -> EngineResult<()>, count_only: bool, returned: &mut u64, row: ExecutionRow, ) -> Result<(), StorageError> {
    if !count_only {
        let output = StoredDocument::new(*row.id(), row.version(), row.shared_document())?;
        visitor(output).map_err(backend_storage_error)?;
    }
    *returned = returned.saturating_add(1);
    Ok(())
}

/// Error raised when a stage declaring an exact row count (`single`) sees
/// another number of rows.
fn exact_rows_error(operator: &PhysicalOperator, expected: usize, found: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::evaluation(format!("{} expected exactly {expected} row(s), found {found}", operator.name()))
}

/// Fails when a stage declaring an exact row count saw fewer rows than required.
fn ensure_exact_rows(operators: &[PhysicalOperator], remaining: &[Option<usize>]) -> EngineResult<()> {
    for (operator, remaining) in operators.iter().zip(remaining) {
        if let (Some(expected), Some(remaining)) = (operator.execution_properties().exact_rows(), remaining) {
            if *remaining > 0 {
                return Err(EngineError::execution(exact_rows_error(operator, expected, expected - remaining)));
            }
        }
    }
    Ok(())
}

/// Result returned by engine operations.
pub type EngineResult<T> = std::result::Result<T, EngineError>;

/// High-level OG database engine.
pub struct Engine {
    storage: Arc<dyn StorageEngine>,
    runtime: Arc<QueryRuntime>,
    planner: Planner,
    planner_cache: Arc<PlannerCache>,
    indexing: Arc<IndexingEngine>,
    memory_governor: MemoryGovernor,
    changes: Arc<changes::Changes>,
    results: Arc<results::Results>,
    /// Set on the engine a temporal or system-collection query runs on (its
    /// storage is already that view).
    bound: bool,
}

impl Engine {
    /// Creates an engine with default logical and physical planner options.
    #[must_use]
    #[inline]
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        let memory_governor = MemoryGovernor::unlimited();
        Self {
            storage,
            runtime: Arc::new(QueryRuntime::new()),
            planner: Planner::new(),
            planner_cache: Arc::new(PlannerCache::new_governed(
                128,
                DEFAULT_PLANNER_CACHE_BYTES,
                memory_governor.clone(),
            )),
            indexing: Arc::new(IndexingEngine::new()),
            results: Arc::new(results::Results::new(memory_governor.profile().result_cache_bytes, memory_governor.clone())),
            memory_governor,
            changes: Arc::new(changes::Changes::new()),
            bound: false,
        }
    }

    /// What changes when `collection` changes for `place` (`None`: for any
    /// Place), to tell whether rows computed from it still hold; `None` when
    /// the storage cannot tell.
    #[must_use]
    pub fn change_token(&self, collection: &str, place: Option<&str>) -> Option<serde_json::Value> {
        self.changes.token(self.storage.as_ref(), collection, place)
    }

    /// The rows of the reads a Place repeats ([`results`]).
    #[must_use]
    pub fn results(&self) -> &results::Results {
        &self.results
    }

    #[must_use]
    pub fn storage(&self) -> &dyn StorageEngine {
        self.storage.as_ref()
    }

    /// The engine a temporal plan (`on c as of …`, `on c history`) runs on:
    /// the same components over the temporal view of the storage. `None`
    /// for an ordinary plan. Only temporal queries pay for it.
    fn temporal_view(&self, physical: &PhysicalPlan) -> EngineResult<Option<Self>> {
        let Some(temporal) = physical.source().temporal() else { return Ok(None) };
        if self.bound {
            return Ok(None);
        }
        if physical.source().collection().as_str().starts_with('_') {
            return Err(EngineError::execution(ExecutionError::unsupported_operator(
                "temporal-system-collection",
                "`as of` and `history` are not available on system collections",
            )));
        }
        if !temporal_plan_writes_allowed(physical) {
            return Err(EngineError::execution(ExecutionError::unsupported_operator(
                "temporal-write",
                "an `as of` or `history` source is read-only (only a terminal `into` may write)",
            )));
        }
        Ok(Some(self.view(Arc::new(crate::storage::history::TemporalStorage::new(Arc::clone(&self.storage), temporal.clone())))))
    }

    /// The same components over `storage`.
    fn view(&self, storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            runtime: Arc::clone(&self.runtime),
            planner: self.planner.clone(),
            planner_cache: Arc::clone(&self.planner_cache),
            indexing: Arc::clone(&self.indexing),
            memory_governor: self.memory_governor.clone(),
            changes: Arc::clone(&self.changes),
            results: Arc::clone(&self.results),
            bound: true,
        }
    }
    
    #[must_use]
    pub fn runtime(&self) -> &QueryRuntime {
        self.runtime.as_ref()
    }

    #[must_use]
    pub const fn planner(&self) -> &Planner {
        &self.planner
    }

    /// Plans a normalized pipeline.
    pub fn plan(&self, pipeline: &PlannerPipeline) -> EngineResult<PlannedQuery> {
        let planned = self.planner.plan(pipeline).map(PlannedQuery::new).map_err(EngineError::planning)?;
        self.resolve_compact_group(planned)
    }

    /// Decides the items of a compact `group` after the first: summed when
    /// every value they take is a number, a key otherwise
    /// ([`PhysicalOperator::Group`]'s `auto`), from the kinds of the source
    /// fields (the storage's facts, or a scan when it keeps none) through
    /// the stages before it. Left as written (the last item summed) when a
    /// kind cannot be known: a stage other than `where`, `limit`, `skip`,
    /// `sort` or `derive` before it, a nested field, a function of unknown
    /// result.
    fn resolve_compact_group(&self, planned: PlannedQuery) -> EngineResult<PlannedQuery> {
        let physical = planned.physical();
        let Some(at) = physical.operators().iter().position(|operator| matches!(operator, PhysicalOperator::Group { auto: true, .. })) else { return Ok(planned) };
        let collection = physical.source().collection();
        if self.is_virtual(collection) || collection.as_str().starts_with('_') {
            return Ok(planned);
        }
        let read = self.storage.read().map_err(storage_engine_error)?;
        let numbers = match read.field_facts(collection).map_err(storage_engine_error)? {
            Some(facts) => facts.number_fields(),
            None => {
                let mut kinds = HashMap::<String, (bool, bool)>::new();
                read.scan_each(collection, ScanOptions::default(), &mut |stored| {
                    for (name, value) in stored.document().iter() {
                        let (number, other) = kinds.entry(name.to_string()).or_default();
                        *number |= value.as_number().is_some();
                        *other |= !value.is_null() && value.as_number().is_none();
                    }
                    Ok(true)
                }).map_err(storage_engine_error)?;
                kinds.into_iter().filter(|(_, (number, other))| *number && !other).map(|(name, _)| name).collect()
            }
        };
        drop(read);
        Ok(resolve_group_items(physical, at, &numbers).map_or(planned, PlannedQuery::new))
    }

    /// The rows of a collection scan that the leading filters `gate` keep,
    /// run on batches over a block scan that leaves out the row groups
    /// they drop, as (identifier, version) in scan direction order, with the
    /// rows read. `None` when that cannot give exactly the rows and errors
    /// of the filters run row by row in that order: the storage reads no
    /// blocks, the filters do not run on batches, a filter fails on some row
    /// (the ordered scan raises it, or stops before it), or more rows pass
    /// than are kept in memory.
    fn gate_on_batches( &self, read: &dyn StorageRead, physical: &PhysicalPlan, options: ScanOptions, gate: &ProjectedValuePipeline, filters: &[PhysicalOperator], ) -> EngineResult<Option<(u64, Vec<(DocumentId, DocumentVersion)>)>> {
        const MAX_KEPT: usize = 1 << 20;
        if !read.supports_projected_blocks() {
            return Ok(None);
        }
        let Some(program) = crate::query::BatchProgram::compile(filters, gate.layout().fields(), &[]) else {
            return Ok(None);
        };
        let program = Arc::new(program);
        let (kept, abandoned) = (std::cell::RefCell::new(Vec::new()), std::cell::Cell::new(false));
        let scanned = scan_batched(
            read,
            physical.source().collection(),
            reusable_projected_scan_options(options, physical, gate),
            gate.layout().storage_fields(),
            &program,
            crate::query::GroupPruning::new(filters, gate.layout().storage_fields()).as_ref(),
            None,
            &mut |runner| {
                let mut kept = kept.borrow_mut();
                kept.extend(runner.selection().iter().map(|row| runner.id(*row as usize)));
                if kept.len() > MAX_KEPT {
                    abandoned.set(true);
                    return Ok(false);
                }
                Ok(true)
            },
            &mut |id, version, values| {
                match gate.accepts_with(values, |expression, resolver| self.runtime.evaluate_resolved_predicate(expression, resolver)) {
                    Ok(true) => kept.borrow_mut().push((id, version)),
                    Ok(false) => {}
                    Err(_) => {
                        abandoned.set(true);
                        return Ok(false);
                    }
                }
                Ok(kept.borrow().len() <= MAX_KEPT || { abandoned.set(true); false })
            },
        )
        .map_err(storage_engine_error)?;
        if abandoned.get() {
            return Ok(None);
        }
        let mut kept = kept.into_inner();
        kept.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        if options.direction() == crate::storage::ScanDirection::Reverse {
            kept.reverse();
        }
        Ok(Some((scanned, kept)))
    }

    /// Plans a read restricted to a Place (and an App instance): every
    /// source it reads keeps only the rows of that scope
    /// ([`crate::query::scoped_read_plan`]). `None` for a plan that writes.
    /// Never cached: the plan cache is keyed by request text alone.
    pub fn plan_scoped(&self, pipeline: &PlannerPipeline, scope: &crate::query::DocumentScope) -> EngineResult<Option<PlannedQuery>> {
        let planned = self.plan(pipeline)?;
        Ok(scope.read_plan(planned.physical(), false).map(PlannedQuery::new))
    }

    /// Plans with a bounded cache keyed by normalized request text.
    /// The cache keeps plans before [`Self::resolve_compact_group`]: the
    /// kinds it reads change with the data.
    pub fn plan_cached(&self, key: &str, pipeline: &PlannerPipeline) -> EngineResult<PlannedQuery> {
        let planned = match self.planner_cache.get(key) {
            Some(planned) => planned,
            None => {
                let planned = self.planner.plan(pipeline).map(PlannedQuery::new).map_err(EngineError::planning)?;
                self.planner_cache.insert(key.to_owned(), planned.clone());
                planned
            }
        };
        self.resolve_compact_group(planned)
    }

    /// Plans a pipeline without running it and describes the plan: source
    /// access, the executor the memory contract selects, and each operator.
    pub fn explain(&self, pipeline: &PlannerPipeline) -> EngineResult<Document> {
        let planned = self.plan(pipeline)?;
        let physical = planned.physical();
        let executor = if physical.is_write() {
            "transactional"
        } else if physical.is_memory_streaming() {
            "streaming"
        } else if self.supports_governed_blocking_streaming(physical) {
            "governed_blocking"
        } else {
            "rejected"
        };
        let access = match physical.source().access() {
            crate::query::PhysicalAccess::PrimaryKeyLookup { .. } => "primary_key_lookup",
            _ => "collection_scan",
        };
        let operators = physical.operators().iter().map(|operator| {
            Value::object(Document::from_fields([
                ("stage", Value::string(operator.name())),
                ("detail", Value::string(describe_operator(operator))),
            ]))
        });
        let mut document = Document::new();
        document.insert("collection", Value::string(physical.source().collection().as_str()));
        document.insert("access", Value::string(access));
        document.insert("read_only", Value::bool(!physical.is_write()));
        document.insert("mode", Value::string(physical.memory_execution_mode().as_str()));
        document.insert("executor", Value::string(executor));
        document.insert("operators", Value::array(operators));
        Ok(document)
    }

    pub fn planner_cache_stats(&self) -> PlannerCacheStats {
        self.planner_cache.stats()
    }
    pub fn invalidate_planner_cache(&self) {
        self.planner_cache.invalidate_all();
    }

    /// Returns the shared memory governor.
    #[must_use]
    pub const fn memory_governor(&self) -> &MemoryGovernor {
        &self.memory_governor
    }

    /// Replaces the governor used by this engine and its planner cache.
    ///
    /// Existing cached plans are intentionally discarded so every live cache
    /// reservation belongs to the newly configured governor.
    #[must_use]
    pub fn with_memory_governor(mut self, governor: MemoryGovernor) -> Self {
        let planner_cache_bytes = governor.profile().planner_cache_bytes;
        self.planner_cache = Arc::new(PlannerCache::new_governed(128, planner_cache_bytes, governor.clone()));
        self.results = Arc::new(results::Results::new(governor.profile().result_cache_bytes, governor.clone()));
        self.memory_governor = governor;
        self
    }

    /// Keeps no rows of repeated reads (`OGD_RESULT_CACHE=off`, for A/B).
    #[must_use]
    pub fn without_result_cache(mut self) -> Self {
        self.results = Arc::new(results::Results::new(0, self.memory_governor.clone()));
        self
    }

    /// Streams a read-only pipeline without materializing all rows.
    ///
    /// Returns `Ok(None)` when the physical plan contains an operator that
    /// requires set-level materialization or performs writes.
    pub fn stream_read_pipeline( &self, physical: &PhysicalPlan, visitor: &mut dyn FnMut(StoredDocument) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        if let Some(view) = self.temporal_view(physical)? {
            return view.stream_read_pipeline(physical, visitor);
        }
        if self.is_virtual(physical.source().collection()) || !physical.is_memory_streaming() {
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
                let value_pipeline = ProjectedValuePipeline::compile(&physical.operators()[..data_operator_len], std::iter::empty::<crate::query::ExpressionFieldPath>())
                    .map_err(EngineError::execution)?
                    .filter(|_| read.support(StorageReadCapability::ProjectedValuesGatedUnordered).available());
                let batch = value_pipeline.as_ref().and_then(|value_pipeline| crate::query::BatchProgram::compile(&physical.operators()[..data_operator_len], value_pipeline.layout().fields(), &[]).map(|program| (value_pipeline, Arc::new(program))));
                if let Some((value_pipeline, program)) = batch {
                    let counted = std::cell::Cell::new(0u64);
                    let projected_scanned = scan_batched(
                        read.as_ref(),
                        physical.source().collection(),
                        reusable_projected_scan_options(*options, physical, value_pipeline).without_locators(),
                        value_pipeline.layout().storage_fields(),
                        &program,
                        crate::query::GroupPruning::new(&physical.operators()[..data_operator_len], value_pipeline.layout().storage_fields()).as_ref(),
                        None,
                        &mut |runner| {
                            counted.set(counted.get() + runner.selection().len() as u64);
                            Ok(true)
                        },
                        &mut |_, _, values| {
                            let accepted = value_pipeline
                                .accepts_with(values, |expression, resolver| self.runtime.evaluate_resolved_predicate(expression, resolver))
                                .map_err(backend_storage_error)?;
                            counted.set(counted.get() + u64::from(accepted));
                            Ok(true)
                        },
                    )
                    .map_err(storage_engine_error)?;
                    let projected_count = counted.get();
                    let projected_filtered = projected_scanned.saturating_sub(projected_count);
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

        let mut scanned = 0_u64;
        let mut filtered = 0_u64;
        // Rows handed to the visitor, or only counted under a terminal `count`.
        let mut returned = 0_u64;
        let count_only = count_alias.is_some();
        let mut partial_used = false;
        let mut chain = StreamingRowChain::new(&physical.operators()[..data_operator_len]);

        if chain.has_empty_bound() {
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
                // changing operator before it, and it lets storage use its
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
                // of Filter/Select stages and ending in Select. Native storage can read
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
                        let select = |values: &[Option<Value>]| selected_document(selected_fields, &selected_slots, values);
                        // Filters and selects on batches, read by blocks in the
                        // same storage order; a batch that fails is replayed
                        // row by row, so a failure surfaces on the same row.
                        // The selects only pick fields from the source values.
                        if let Some(batch) = crate::query::BatchProgram::compile(data_operators, value_pipeline.layout().fields(), &[]) {
                            let mut values = Vec::new();
                            let (returned_cell, filtered_cell) = (std::cell::Cell::new(0u64), std::cell::Cell::new(0u64));
                            let visitor_cell = std::cell::RefCell::new(&mut *visitor);
                            let mut emit = |id: DocumentId, version: DocumentVersion, document: Document| -> Result<bool, StorageError> {
                                (visitor_cell.borrow_mut())(StoredDocument::new(id, version, Arc::new(document))?).map_err(backend_storage_error)?;
                                returned_cell.set(returned_cell.get() + 1);
                                Ok(true)
                            };
                            let emit = std::cell::RefCell::new(&mut emit);
                            scanned = scan_batched(
                                read.as_ref(),
                                physical.source().collection(),
                                reusable_projected_scan_options(storage_options, physical, &value_pipeline),
                                value_pipeline.layout().storage_fields(),
                                &Arc::new(batch),
                                crate::query::GroupPruning::new(data_operators, value_pipeline.layout().storage_fields()).as_ref(),
                                None,
                                &mut |runner| {
                                    filtered_cell.set(filtered_cell.get() + runner.dropped() as u64);
                                    for &row in runner.selection() {
                                        let (id, version) = runner.id(row as usize);
                                        runner.source_values(row as usize, &mut values);
                                        (emit.borrow_mut())(id, version, select(&values))?;
                                    }
                                    Ok(true)
                                },
                                &mut |id, version, values| {
                                    let accepted = value_pipeline
                                        .accepts_with(values, |expression, resolver| self.runtime.evaluate_resolved_predicate(expression, resolver))
                                        .map_err(backend_storage_error)?;
                                    if !accepted {
                                        filtered_cell.set(filtered_cell.get() + 1);
                                        return Ok(true);
                                    }
                                    (emit.borrow_mut())(id, version, select(values))
                                },
                            )
                            .map_err(storage_engine_error)?;
                            (filtered, returned) = (filtered_cell.get(), returned_cell.get());
                            let statistics = ExecutionStatistics::streamed_pipeline(scanned, filtered, returned, ExecutionStrategy::CollectionScan);
                            self.indexing.observe(QueryObservation::from_execution(physical, statistics, started.elapsed()));
                            return Ok(Some(statistics));
                        }
                    }
                }

                let leading_filter_len = data_operators
                    .iter()
                    .take_while(|operator| matches!(operator, PhysicalOperator::Filter { .. }))
                    .count();
                // Partial documents: when the consumer's field demand is known
                // (a terminal `count`, or an order-free blocking consumer), decode
                // only the fields the stages read, and evaluate the leading
                // filters on those values before building any document.
                // The partial cursor is unordered: a stage keeping a subset of
                // the rows that depends on their order (`limit`, `skip`,
                // `first`) keeps the ordered complete-document scan.
                let field_demand = ((physical.order_free() || count_only) && !data_operators.iter().any(keeps_rows_by_order))
                    .then(|| crate::query::source_field_demand(physical.operators(), physical.field_demand()))
                    .flatten();
                let partial_pipeline = match field_demand
                    .as_deref()
                    .and_then(|demand| partial_source_fields(read.as_ref(), demand))
                {
                    Some(fields) => ProjectedValuePipeline::compile(&data_operators[..leading_filter_len], fields)
                        .map_err(EngineError::execution)?,
                    None => None,
                };

                if let Some(value_pipeline) = partial_pipeline {
                    PARTIAL_DOCUMENTS_USED.with(|used| used.set(true));
                    partial_used = true;
                    let slot_fields = value_pipeline.layout().fields();
                    // Stages after the leading filters that run on slots keep
                    // the row as values; a document is built only for a
                    // consumer that receives rows.
                    let slot_program = crate::query::SlotProgram::compile_expanding(&data_operators[leading_filter_len..], slot_fields);
                    let mut slot_values = Vec::new();
                    let slot_filtered = std::cell::Cell::new(0u64);
                    let (scanned_cell, filtered_cell, returned_cell) = (std::cell::Cell::new(scanned), std::cell::Cell::new(filtered), std::cell::Cell::new(returned));
                    let visitor_cell = std::cell::RefCell::new(&mut *visitor);
                    let emit = |row: ExecutionRow, count_only: bool| -> Result<(), StorageError> {
                        let mut returned = returned_cell.get();
                        let emitted = emit_streamed_row(&mut **visitor_cell.borrow_mut(), count_only, &mut returned, row);
                        returned_cell.set(returned);
                        emitted
                    };
                    let gate = |values: &[Option<Value>]| -> Result<bool, StorageError> {
                        scanned_cell.set(scanned_cell.get().saturating_add(1));
                        let accepted = value_pipeline
                            .accepts_with(values, |expression, resolver| {
                                self.runtime.evaluate_resolved_predicate(expression, resolver)
                            })
                            .map_err(backend_storage_error)?;
                        if !accepted {
                            filtered_cell.set(filtered_cell.get().saturating_add(1));
                        }
                        Ok(accepted)
                    };
                    let mut row_visit = |id: DocumentId, version: DocumentVersion, values: &[Option<Value>]| -> Result<bool, StorageError> {
                        if let Some(program) = &slot_program {
                            slot_values.clear();
                            slot_values.extend_from_slice(values);
                            slot_values.resize(program.width(), None);
                            // A storage error raised while emitting is kept as is.
                            let mut emit_failure = None;
                            let emitted = program.run_each(
                                &mut slot_values,
                                |expression, resolver| self.runtime.evaluate_resolved_predicate(expression, resolver),
                                &mut |row_values| {
                                    if count_only {
                                        returned_cell.set(returned_cell.get().saturating_add(1));
                                        return Ok(());
                                    }
                                    let emitted = StoredDocument::new(id, version, Arc::new(program.document(row_values)))
                                        .and_then(|stored| emit(ExecutionRow::from_stored(stored), false));
                                    emitted.map_err(|failure| {
                                        emit_failure = Some(failure);
                                        ExecutionError::evaluation("slot row emission failed")
                                    })
                                },
                            );
                            if let Some(failure) = emit_failure {
                                return Err(failure);
                            }
                            if emitted.map_err(backend_storage_error)? == 0 {
                                slot_filtered.set(slot_filtered.get().saturating_add(1));
                            }
                            return Ok(true);
                        }
                        let stored = StoredDocument::new(id, version, Arc::new(partial_document(slot_fields, values)))?;
                        self.drive_streaming_row(
                            &mut chain,
                            leading_filter_len,
                            ExecutionRow::from_stored(stored),
                            None,
                            &mut |row| emit(row, count_only),
                        )
                    };
                    // Every stage runs on batches when it can; rows are then
                    // counted, or built only for the rows that pass. A batch
                    // that aborts is replayed through `gate` and `row_visit`.
                    let batch = slot_program
                        .as_ref()
                        .and_then(|program| crate::query::BatchProgram::compile(data_operators, slot_fields, program.fields()).map(|batch| (Arc::new(batch), program)));
                    if let Some((batch, program)) = batch {
                        let mut output = Vec::new();
                        let batch_options = reusable_projected_scan_options(storage_options, physical, &value_pipeline);
                        let batch_scanned = scan_batched(
                            read.as_ref(),
                            physical.source().collection(),
                            if count_only { batch_options.without_locators() } else { batch_options },
                            value_pipeline.layout().storage_fields(),
                            &batch,
                            crate::query::GroupPruning::new(data_operators, value_pipeline.layout().storage_fields()).as_ref(),
                            None,
                            &mut |runner| {
                                let passed = runner.selection().len() as u64;
                                filtered_cell.set(filtered_cell.get().saturating_add(runner.dropped() as u64));
                                if count_only {
                                    returned_cell.set(returned_cell.get().saturating_add(passed));
                                    return Ok(true);
                                }
                                for &row in runner.selection() {
                                    let (id, version) = runner.id(row as usize);
                                    runner.output_values(row as usize, &mut output);
                                    let stored = StoredDocument::new(id, version, Arc::new(program.document(&output)))?;
                                    emit(ExecutionRow::from_stored(stored), false)?;
                                }
                                Ok(true)
                            },
                            &mut |id, version, values| {
                                let accepted = gate(values)?;
                                // Counted once with the batch below.
                                scanned_cell.set(scanned_cell.get() - 1);
                                if accepted { row_visit(id, version, values) } else { Ok(true) }
                            },
                        )
                        .map_err(storage_engine_error)?;
                        scanned_cell.set(scanned_cell.get().saturating_add(batch_scanned));
                    } else {
                        read.scan_projected_row_values_gated_unordered_each(
                            physical.source().collection(),
                            reusable_projected_scan_options(storage_options, physical, &value_pipeline),
                            value_pipeline.layout().storage_fields(),
                            value_pipeline.gate_field_count(),
                            &mut |values| gate(values),
                            &mut row_visit,
                        )
                        .map_err(storage_engine_error)?;
                    }
                    let slot_filtered = slot_filtered.get();
                    (scanned, filtered, returned) = (scanned_cell.get(), filtered_cell.get(), returned_cell.get());
                    filtered = filtered.saturating_add(slot_filtered);
                } else {
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

                    let first_operator = if source_gated { leading_filter_len } else { 0 };
                    let mut process_stored = |stored: StoredDocument| {
                        if !source_gated {
                            scanned = scanned.saturating_add(1);
                        }
                        self.drive_streaming_row(
                            &mut chain,
                            first_operator,
                            ExecutionRow::from_stored(stored),
                            None,
                            &mut |row| emit_streamed_row(visitor, count_only, &mut returned, row),
                        )
                    };

                    // Leading filters on batches: the rows they keep, then
                    // those rows whole, by identifier.
                    let gated = match &leading_filter_pipeline {
                        Some(value_pipeline) if storage_options.limit().is_none() && !data_operators.iter().any(keeps_rows_by_order) => {
                            self.gate_on_batches(read.as_ref(), physical, storage_options, value_pipeline, &data_operators[..leading_filter_len])?
                        }
                        _ => None,
                    };
                    if let Some((gated_scanned, kept)) = gated {
                        let gated_filtered = gated_scanned.saturating_sub(kept.len() as u64);
                        // The same snapshot: every kept row is there, unchanged.
                        let ids = kept.iter().map(|(id, _)| *id).collect::<Vec<_>>();
                        read.get_each(physical.source().collection(), &ids, &mut |index, stored| match stored {
                            Some(stored) if stored.version() == kept[index].1 => process_stored(stored),
                            _ => Ok(true),
                        })
                        .map_err(storage_engine_error)?;
                        scanned = scanned.saturating_add(gated_scanned);
                        filtered = filtered.saturating_add(gated_filtered);
                    } else if let Some(value_pipeline) = leading_filter_pipeline {
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
                }
                ExecutionStrategy::CollectionScan
            }
            crate::query::PhysicalAccess::PrimaryKeyLookup { id } => {
                if let Some(stored) = read
                    .get(physical.source().collection(), id)
                    .map_err(storage_engine_error)?
                {
                    scanned = 1;
                    self.drive_streaming_row(
                        &mut chain,
                        0,
                        ExecutionRow::from_stored(stored),
                        None,
                        &mut |row| emit_streamed_row(visitor, count_only, &mut returned, row),
                    )
                    .map_err(storage_engine_error)?;
                }
                ExecutionStrategy::PrimaryKeyLookup
            }
        };
        ensure_exact_rows(chain.operators, &chain.limit_remaining)?;
        let filtered = filtered.saturating_add(chain.filtered);
        let mut strategies = ExecutionStrategies::default().with(strategy);
        if partial_used {
            strategies = strategies.with(ExecutionStrategy::PartialDocuments);
        }
        let statistics = if let Some(alias) = count_alias {
            emit_streaming_count(visitor, alias, returned)?;
            ExecutionStatistics::streamed_with_strategies(
                scanned,
                filtered,
                1,
                strategies.with(ExecutionStrategy::StreamingCount),
            )
        } else {
            ExecutionStatistics::streamed_with_strategies(scanned, filtered, returned, strategies)
        };
        self.indexing.observe(QueryObservation::from_execution(
            physical,
            statistics,
            started.elapsed(),
        ));
        Ok(Some(statistics))
    }

    /// Streams one bounded read-only secondary branch used by UNION and LOOKUP.
    ///
    /// The branch keeps row-local stages streaming and shares one implementation
    /// for both compound operators.  LOOKUP may provide an outer document so
    /// nested predicates keep their alias-aware semantics without materializing
    /// the secondary collection.
    /// Reads the joined collection once and indexes its rows on the joined
    /// side of `equality`, in collection order. `None` when the rows do not
    /// fit `budget`: the lookup then keeps its per-row scan.
    fn build_lookup_index( &self, collection: &CollectionId, equality: &LookupEquality, inner_alias: Option<&str>, outer_alias: Option<&str>, memory: &StageMemory<'_>, filter: Option<&LookupKeyFilter>, ) -> EngineResult<Option<LookupIndex>> {
        let read = self.storage.read().map_err(storage_engine_error)?;
        let no_outer = Document::new();
        let mut index = LookupIndex { scanned: 0, rows: Vec::new(), buckets: HashMap::default(), numbers: Vec::new(), strings: Vec::new(), missing: Vec::new(), others: Vec::new(), lengths: std::collections::BTreeSet::new(), shapes: HashMap::default() };
        let mut bytes = 0usize;
        let mut exceeded = false;
        // The key is read the way the `where` reads it; the copy carrying the
        // virtual `_id` is only built when a key is `_id`.
        let key_paths = equality.keys.iter().map(|key| match key.joined.segment(0) {
            Some(first) if Some(first) == inner_alias => key.joined.iter().skip(1).collect::<Vec<_>>(),
            _ => key.joined.iter().collect::<Vec<_>>(),
        }).collect::<Vec<_>>();
        let key_path = &key_paths[0];
        let key_is_id = key_paths.iter().any(|path| path.first() == Some(&"_id"));
        let key_value_of = |row: &ExecutionRow| {
            let input = row.stage_input(key_is_id);
            equality.key_of(&crate::query::LookupFields { inner: &input, inner_alias, outer: &no_outer, outer_alias }, false)
        };
        let admits = |key: Option<&Value>| filter.is_none_or(|filter| filter.admits(key));
        // Late materialization: index rows read as partial documents holding
        // only the key fields; probes load the complete candidate rows by `_id`.
        let mut tops = Vec::<&str>::new();
        let slots = key_paths.iter().map(|path| path.first().map(|top| tops.iter().position(|known| known == top).unwrap_or_else(|| { tops.push(top); tops.len() - 1 }))).collect::<Option<Vec<_>>>();
        let key_field = key_path.first().filter(|_| !key_is_id).and_then(|field| crate::query::ExpressionFieldPath::new([*field]).ok());
        let partial_fields = if key_is_id { Some(Vec::new()) } else { slots.as_ref().and_then(|_| tops.iter().map(|top| crate::query::ExpressionFieldPath::new([*top]).ok()).collect::<Option<Vec<_>>>()) };
        // A top-level key is read borrowed: rows a filter rejects cost no
        // allocation.
        if let (false, 1, false) = (key_is_id, key_path.len(), equality.composite()) {
            if let Some(field) = &key_field {
                if let Some(keyed) = self.lookup_keys_on_batches(collection, field, memory, filter)? {
                    let Some((scanned, keyed)) = keyed else {
                        return Ok(None);
                    };
                    index.scanned = scanned;
                    for (id, version, key) in keyed {
                        index.push(IndexedRow::Pending(ProjectedRowLocator::new(id, version)), key);
                    }
                    return Ok(Some(index));
                }
            }
            if let Some(fields) = partial_fields.as_ref().and_then(|fields| partial_source_fields(read.as_ref(), fields)) {
                let layout = crate::query::ProjectedValueLayout::new(fields).map_err(EngineError::execution)?;
                let mut keyed = Vec::new();
                read.scan_projected_row_refs_unordered_each(collection, ScanOptions::default(), layout.storage_fields(), &mut |id, version, values| {
                    index.scanned += 1;
                    let key = values.first().and_then(Option::as_ref);
                    if !filter.is_none_or(|filter| filter.admits_ref(key)) {
                        return Ok(true);
                    }
                    let key = key.map(|key| equality_key(&key.to_value()));
                    bytes = bytes.saturating_add(key.as_ref().map_or(0, |(_, key)| key.len()) + 96);
                    if !memory.fits(bytes) {
                        exceeded = true;
                        return Ok(false);
                    }
                    keyed.push((id, version, key));
                    Ok(true)
                })
                .map_err(storage_engine_error)?;
                if exceeded {
                    return Ok(None);
                }
                keyed.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                for (id, version, key) in keyed {
                    index.push(IndexedRow::Pending(ProjectedRowLocator::new(id, version)), key);
                }
                return Ok(Some(index));
            }
        }
        if let Some(fields) = partial_fields.and_then(|fields| partial_source_fields(read.as_ref(), &fields)) {
            let layout = crate::query::ProjectedValueLayout::new(fields).map_err(EngineError::execution)?;
            let mut keyed = Vec::new();
            read.scan_projected_row_values_gated_unordered_each(collection, ScanOptions::default(), layout.storage_fields(), 0, &mut |_| Ok(true), &mut |id, version, values| {
                // The key is read from its slot, walking nested segments, as
                // the `where` would read it from the row.
                index.scanned += 1;
                let key = if key_is_id {
                    let stored = StoredDocument::new(id, version, Arc::new(partial_document(layout.fields(), values)))?;
                    let value = key_value_of(&ExecutionRow::from_stored(stored));
                    if !admits(value.as_ref()) {
                        return Ok(true);
                    }
                    value.as_ref().map(equality_key)
                } else if let (true, Some(slots)) = (equality.composite(), &slots) {
                    let walk = |slot: usize, path: &[&str]| path[1..].iter().fold(values.get(slot).and_then(Option::as_ref), |value, segment| value.and_then(Value::as_object).and_then(|object| object.get(segment)));
                    let parts = equality.keys.iter().zip(&key_paths).zip(slots).map(|((key, path), slot)| walk(*slot, path).cloned().or_else(|| key.joined_optional.then_some(Value::Null))).collect::<Option<Vec<_>>>();
                    parts.map(|parts| equality_key(&Value::Array(parts.into())))
                } else {
                    let mut value = values.first().and_then(Option::as_ref);
                    for segment in &key_path[1..] {
                        value = value.and_then(Value::as_object).and_then(|object| object.get(segment));
                    }
                    if !admits(value) {
                        return Ok(true);
                    }
                    value.map(equality_key)
                };
                bytes = bytes.saturating_add(key.as_ref().map_or(0, |(_, key)| key.len()) + 96);
                if !memory.fits(bytes) {
                    exceeded = true;
                    return Ok(false);
                }
                keyed.push((id, version, key));
                Ok(true)
            })
            .map_err(storage_engine_error)?;
            if exceeded {
                return Ok(None);
            }
            // The cursor is unordered: restore collection (`_id`) order.
            keyed.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            for (id, version, key) in keyed {
                index.push(IndexedRow::Pending(ProjectedRowLocator::new(id, version)), key);
            }
            return Ok(Some(index));
        }
        read.scan_each(collection, ScanOptions::default(), &mut |stored| {
            let row = ExecutionRow::from_stored(stored);
            index.scanned += 1;
            let key = key_value_of(&row);
            if !admits(key.as_ref()) {
                return Ok(true);
            }
            let key = key.as_ref().map(equality_key);
            bytes = bytes
                .saturating_add(execution_row_working_bytes(&row).map_err(backend_storage_error)?)
                .saturating_add(key.as_ref().map_or(0, |(_, key)| key.len()) + 48);
            if !memory.fits(bytes) {
                exceeded = true;
                return Ok(false);
            }
            index.push(IndexedRow::Loaded(row), key);
            Ok(true)
        })
        .map_err(storage_engine_error)?;
        Ok((!exceeded).then_some(index))
    }

    /// Rows of a lookup index on a top-level key, read on batches: the
    /// locator and key of every row `filter` admits, in collection order,
    /// the filter decided once per dictionary code of a batch's key column.
    /// `None` when the collection cannot be read so; `Some(None)` when the
    /// rows exceed `memory`.
    #[allow(clippy::type_complexity)]
    fn lookup_keys_on_batches( &self, collection: &CollectionId, field: &crate::query::ExpressionFieldPath, memory: &StageMemory<'_>, filter: Option<&LookupKeyFilter>, ) -> EngineResult<Option<Option<(u64, Vec<(DocumentId, DocumentVersion, Option<(KeyKind, Vec<u8>)>)>)>>> {
        let Ok(prefix) = PhysicalPlan::new(crate::query::PhysicalSource::collection_scan(collection.clone()), []) else {
            return Ok(None);
        };
        let fields = std::slice::from_ref(field);
        // Kept rows, their bytes, and whether they exceeded `memory`.
        let kept = std::cell::RefCell::new((Vec::new(), 0usize, false));
        let keep = |id: DocumentId, version: DocumentVersion, key: Option<&crate::storage::ProjectedValueRef<'_>>| {
            let (keyed, bytes, exceeded) = &mut *kept.borrow_mut();
            let key = key.map(|key| equality_key(&key.to_value()));
            *bytes = bytes.saturating_add(key.as_ref().map_or(0, |(_, key)| key.len()) + 96);
            *exceeded |= !memory.fits(*bytes);
            if !*exceeded {
                keyed.push((id, version, key));
            }
        };
        let exceeded = || kept.borrow().2;
        // Admission of each code of the current dictionary: 0 unknown, 1 in, 2 out.
        let mut dictionary = None;
        let mut admitted = Vec::<u8>::new();
        let Some(statistics) = self.stream_slot_batches(
            &prefix,
            fields,
            fields,
            true,
            &mut |runner, _| {
                if exceeded() {
                    return Ok(());
                }
                let column = runner.output(0);
                for &row in runner.selection() {
                    let row = row as usize;
                    // The cell is read only to decide a new code or keep a row.
                    let key = || column.and_then(|column| column.value_ref(row));
                    let admits = match (filter, column.and_then(|column| column.code(row))) {
                        (None, _) => true,
                        (Some(filter), Some((words, code))) => {
                            if dictionary != Some(words) {
                                dictionary = Some(words);
                                admitted.clear();
                            }
                            let code = code as usize;
                            if admitted.len() <= code {
                                admitted.resize(code + 1, 0);
                            }
                            if admitted[code] == 0 {
                                admitted[code] = if filter.admits_ref(key().as_ref()) { 1 } else { 2 };
                            }
                            admitted[code] == 1
                        }
                        (Some(filter), None) => filter.admits_ref(key().as_ref()),
                    };
                    if admits {
                        let (id, version) = runner.id(row);
                        keep(id, version, key().as_ref());
                    }
                }
                Ok(())
            },
            &mut |row| {
                let key = row.values.first().and_then(Option::as_ref).map(crate::storage::ProjectedValueRef::borrowed);
                if !exceeded() && filter.is_none_or(|filter| filter.admits_ref(key.as_ref())) {
                    keep(row.id, row.version, key.as_ref());
                }
                Ok(())
            },
        )?
        else {
            return Ok(None);
        };
        let (mut keyed, _, exceeded) = kept.into_inner();
        if exceeded {
            return Ok(Some(None));
        }
        // The batches are unordered: restore collection (`_id`) order.
        keyed.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(Some(Some((statistics.scanned(), keyed))))
    }

    /// Runs the lookup sub-pipeline for one outer row over the indexed rows
    /// that can match it, in collection order, exactly as a full scan would.
    fn probe_lookup_index( &self, collection: &CollectionId, index: &LookupIndex, equality: &LookupEquality, pipeline: &PhysicalSubPipeline, lookup_context: Option<(&Document, Option<&str>, Option<&str>)>, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<ExecutionStatistics> {
        let (outer, inner_alias, outer_alias) = lookup_context.expect("a lookup probe has an outer row");
        let no_inner = Document::new();
        let fields = crate::query::LookupFields { inner: &no_inner, inner_alias, outer, outer_alias };
        let outer_key = equality.key_of(&fields, true);
        let read = self.storage.read().map_err(storage_engine_error)?;
        let mut chain = StreamingRowChain::new(pipeline.operators());
        let mut returned = 0u64;
        // Pending candidates are read a chunk at a time, in collection order.
        const CHUNK: usize = 64;
        let candidates = index.candidates(outer_key.as_ref(), equality.prefix);
        let mut ids = Vec::with_capacity(CHUNK);
        let mut loaded = Vec::<Option<ExecutionRow>>::with_capacity(CHUNK);
        'chunks: for chunk in candidates.chunks(CHUNK) {
            ids.clear();
            ids.extend(chunk.iter().filter_map(|position| match &index.rows[*position as usize] {
                IndexedRow::Pending(locator) => Some(locator.id()),
                IndexedRow::Loaded(_) => None,
            }));
            loaded.clear();
            read.get_each(collection, &ids, &mut |_, stored| {
                loaded.push(stored.map(ExecutionRow::from_stored));
                Ok(true)
            })
            .map_err(storage_engine_error)?;
            let mut pending = loaded.drain(..);
            for position in chunk {
                let row = match &index.rows[*position as usize] {
                    IndexedRow::Loaded(row) => row.clone(),
                    // A row changed or deleted since the index was built is skipped.
                    IndexedRow::Pending(locator) => match pending.next().flatten() {
                        Some(row) if row.version() == locator.version() => row,
                        _ => continue,
                    },
                };
                let more = self
                    .drive_streaming_row(&mut chain, 0, row, lookup_context, &mut |row| {
                        visitor(row).map_err(backend_storage_error)?;
                        returned = returned.saturating_add(1);
                        Ok(())
                    })
                    .map_err(storage_engine_error)?;
                if !more {
                    break 'chunks;
                }
            }
        }
        ensure_exact_rows(pipeline.operators(), &chain.limit_remaining)?;
        Ok(ExecutionStatistics::streamed_pipeline(0, chain.filtered, returned, ExecutionStrategy::CollectionScan))
    }

    /// An order-free union branch read as a main pipeline, on batches when
    /// its stages allow, its rows marked as union rows. `None` when it cannot
    /// run so (nothing was emitted).
    fn stream_union_batches( &self, collection: &CollectionId, pipeline: &PhysicalSubPipeline, consumer_demand: Option<&[crate::query::ExpressionFieldPath]>, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        let Ok(plan) = PhysicalPlan::new(crate::query::PhysicalSource::collection_scan(collection.clone()), pipeline.operators().iter().cloned()) else {
            return Ok(None);
        };
        let plan = plan.with_order_free_consumer(consumer_demand.map(<[_]>::to_vec));
        self.stream_read_pipeline(&plan, &mut |stored| visitor(ExecutionRow::from_union(stored)))
    }

    /// Streams a lookup or union sub-pipeline. `union_demand` is `Some` for a
    /// union branch (rows marked as union rows) and, when it lists fields,
    /// lets the branch decode only those fields from an unordered cursor.
    fn stream_bounded_secondary_pipeline( &self, collection: &CollectionId, pipeline: &PhysicalSubPipeline, lookup_context: Option<(&Document, Option<&str>, Option<&str>)>, union_demand: Option<&[crate::query::ExpressionFieldPath]>, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<ExecutionStatistics> {
        let union_origin = lookup_context.is_none();
        let read = self.storage.read().map_err(storage_engine_error)?;
        let operators = pipeline.operators();
        if !operators.iter().all(is_row_operator) {
            // A union branch with blocking stages runs as a query of its own, governed like any other.
            if union_origin {
                let branch = PhysicalPlan::new(crate::query::PhysicalSource::collection_scan(collection.clone()), operators.iter().cloned()).map_err(|error| EngineError::execution(ExecutionError::evaluation(error.to_string())))?;
                return self.stream_rows(&branch, None, |row| visitor(row.into_union()));
            }
            return Err(EngineError::execution(ExecutionError::evaluation(
                "secondary pipeline contains an operator without a bounded streaming executor",
            )));
        }

        let mut chain = StreamingRowChain::new(operators);
        let mut scanned = 0u64;
        let mut returned = 0u64;

        if chain.has_empty_bound() {
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

        let mut process = |stored: StoredDocument| {
            scanned = scanned.saturating_add(1);
            let row = if union_origin {
                ExecutionRow::from_union(stored)
            } else {
                ExecutionRow::from_stored(stored)
            };
            self.drive_streaming_row(&mut chain, 0, row, lookup_context, &mut |row| {
                visitor(row).map_err(backend_storage_error)?;
                returned = returned.saturating_add(1);
                Ok(())
            })
        };
        match union_demand.and_then(|demand| partial_source_fields(read.as_ref(), demand)) {
            Some(fields) => {
                PARTIAL_DOCUMENTS_USED.with(|used| used.set(true));
                let layout = crate::query::ProjectedValueLayout::new(fields).map_err(EngineError::execution)?;
                read.scan_projected_row_values_gated_unordered_each(
                    collection,
                    options,
                    layout.storage_fields(),
                    0,
                    &mut |_| Ok(true),
                    &mut |id, version, values| {
                        process(StoredDocument::new(id, version, Arc::new(partial_document(layout.fields(), values)))?)
                    },
                )
            }
            None => read.scan_each(collection, options, &mut process),
        }
        .map_err(storage_engine_error)?;
        ensure_exact_rows(operators, &chain.limit_remaining)?;

        Ok(ExecutionStatistics::streamed_pipeline(
            scanned,
            chain.filtered,
            returned,
            ExecutionStrategy::CollectionScan,
        ))
    }

    /// Drives one source row through `chain.operators[start..]`, handing
    /// every row that survives the chain to `emit`.
    ///
    /// Rows are processed one at a time from a depth-first work stack: when a
    /// stage emits several rows (`unwind`), they continue through the
    /// following stages in input order, so an expanding stage streams
    /// mid-chain like any row-local stage. Returns whether the scan should
    /// continue, `false` once a declared row bound is exhausted.
    fn drive_streaming_row( &self, chain: &mut StreamingRowChain<'_>, start: usize, row: ExecutionRow, lookup_context: Option<(&Document, Option<&str>, Option<&str>)>, emit: &mut dyn FnMut(ExecutionRow) -> Result<(), StorageError>, ) -> Result<bool, StorageError> {
        let mut stop_after_row = false;
        let mut pending = vec![(start, row)];

        'rows: while let Some((start, mut row)) = pending.pop() {
            for (index, operator) in chain.operators.iter().enumerate().skip(start) {
                // Declared row bounds (`limit`, `first`, …) are enforced
                // when a row reaches the stage, before it is evaluated.
                if let Some(remaining) = chain.limit_remaining[index].as_mut() {
                    let exact = operator.execution_properties().exact_rows();
                    if *remaining == 0 {
                        return match exact {
                            Some(expected) => Err(backend_storage_error(exact_rows_error(operator, expected, "more"))),
                            None => Ok(false),
                        };
                    }
                    *remaining -= 1;
                    stop_after_row |= *remaining == 0 && exact.is_none();
                }
                // Lookup aliases can name `_id` indirectly: keep it there.
                let needs_id = chain.needs_id[index] || lookup_context.is_some();
                match operator {
                    PhysicalOperator::Filter { predicate } => {
                        let evaluation = row.stage_input(needs_id);
                        let accepted = match lookup_context {
                            Some((outer, inner_alias, outer_alias)) => self.runtime.evaluate_lookup_predicate(
                                predicate,
                                &crate::query::LookupFields { inner: &evaluation, inner_alias, outer, outer_alias },
                            ),
                            None => self.runtime.evaluate_predicate(predicate, &evaluation),
                        }
                        .map_err(backend_storage_error)?;
                        if !accepted {
                            chain.filtered = chain.filtered.saturating_add(1);
                            continue 'rows;
                        }
                    }
                    PhysicalOperator::Select { fields } => {
                        let selected = self
                            .runtime
                            .apply_select(fields, &row.stage_input(needs_id))
                            .map_err(backend_storage_error)?;
                        row.replace_document(selected, false);
                    }
                    // A `derive` changes the row's own document: copied only when shared.
                    PhysicalOperator::Custom { writes: false, compiled: Some(compiled), .. } if !needs_id && compiled.derives() => {
                        let document = row.document_mut();
                        compiled.derive(document).expect("a derive").map_err(backend_storage_error)?;
                        document.remove("_id");
                    }
                    PhysicalOperator::Custom { writes: false, stage, .. } if !matches!(stage, Some(crate::query::Stage::Sample | crate::query::Stage::Window | crate::query::Stage::Fill)) => {
                        match operator
                            .apply_custom(self.runtime.as_ref(), &row.stage_input(needs_id), lookup_context)
                            .map_err(backend_storage_error)?
                        {
                            crate::query::CustomOperatorResult::Keep => {}
                            crate::query::CustomOperatorResult::Replace(document) => {
                                row.replace_document(document, false);
                            }
                            crate::query::CustomOperatorResult::Discard => {
                                chain.filtered = chain.filtered.saturating_add(1);
                                continue 'rows;
                            }
                            crate::query::CustomOperatorResult::Expand(documents) => {
                                if documents.is_empty() {
                                    chain.filtered = chain.filtered.saturating_add(1);
                                }
                                // Reversed onto the stack so the first item is
                                // processed first; each keeps the source row identity.
                                pending.extend(documents.into_iter().rev().map(|document| {
                                    let mut expanded = row.clone();
                                    expanded.replace_document(document, false);
                                    (index + 1, expanded)
                                }));
                                continue 'rows;
                            }
                        }
                    }
                    PhysicalOperator::Skip { .. } => {
                        let remaining = chain.skip_remaining[index].as_mut().expect("skip state");
                        if *remaining > 0 {
                            *remaining -= 1;
                            chain.filtered = chain.filtered.saturating_add(1);
                            continue 'rows;
                        }
                    }
                    PhysicalOperator::Limit { .. } | PhysicalOperator::Delete => {}
                    // Writing stages, run by a write (see `execute`).
                    PhysicalOperator::Set { assignments } => {
                        let document = self.runtime.apply_set(assignments, &row.evaluation_document()).map_err(execution_storage_error)?;
                        row.replace_document(document, true);
                    }
                    PhysicalOperator::Load { target } => {
                        let document = self.runtime.apply_load(target, &row.evaluation_document()).map_err(execution_storage_error)?;
                        row.replace_document(document, true);
                    }
                    _ => unreachable!("validated streaming row operator"),
                }
            }
            emit(row)?;
        }
        Ok(!stop_after_row)
    }

    /// Returns whether the governed blocking streaming executor supports this plan shape.
    #[must_use]
    pub fn supports_governed_blocking_streaming(&self, physical: &PhysicalPlan) -> bool {
        if self.is_virtual(physical.source().collection()) || physical.is_write() || physical.is_memory_streaming() {
            return false;
        }
        let Some(shape) = governed_shape(physical.operators()) else {
            return false;
        };
        // A pivot is bounded only when the runtime can aggregate it incrementally.
        physical.operators()[shape.blocking_index..shape.core_end].iter().chain(shape.rest).all(|operator| match operator {
            PhysicalOperator::Pivot { specification } => self.runtime.incremental_pivot_accumulator(specification).is_ok(),
            _ => true,
        })
    }

    /// Streams the rows of a collection-scan `prefix` as values of top-level
    /// fields to a consumer reading `downstream`: leading filters gate the
    /// projected scan, the other stages run on slots, and no document is
    /// built. `None` when a stage or field does not allow it.
    fn stream_slot_rows( &self, prefix: &PhysicalPlan, downstream: &[crate::query::ExpressionFieldPath], visitor: &mut dyn FnMut(SlotRowRef<'_>) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        let crate::query::PhysicalAccess::CollectionScan { options } = prefix.source().access() else {
            return self.stream_document_rows(prefix, downstream, visitor);
        };
        if self.is_virtual(prefix.source().collection()) {
            return Ok(None);
        }
        let read = self.storage.read().map_err(storage_engine_error)?;
        let Some((gate, program)) = slot_prefix(read.as_ref(), prefix, downstream, true)? else {
            drop(read);
            return self.stream_document_rows(prefix, downstream, visitor);
        };
        PARTIAL_DOCUMENTS_USED.with(|used| used.set(true));
        let (mut scanned, mut filtered, mut slot_filtered, mut returned) = (0u64, 0u64, 0u64, 0u64);
        let mut row = Vec::new();
        read.scan_projected_row_values_gated_unordered_each(
            prefix.source().collection(),
            reusable_projected_scan_options(*options, prefix, &gate),
            gate.layout().storage_fields(),
            gate.gate_field_count(),
            &mut |values| {
                scanned = scanned.saturating_add(1);
                let accepted = gate
                    .accepts_with(values, |expression, resolver| self.runtime.evaluate_resolved_predicate(expression, resolver))
                    .map_err(backend_storage_error)?;
                if !accepted {
                    filtered = filtered.saturating_add(1);
                }
                Ok(accepted)
            },
            &mut |id, version, values| {
                row.clear();
                row.extend_from_slice(values);
                row.resize(program.width(), None);
                let mut visit_failure = None;
                let emitted = program.run_each(
                    &mut row,
                    |expression, resolver| self.runtime.evaluate_resolved_predicate(expression, resolver),
                    &mut |values| {
                        visitor(SlotRowRef { id, version, fields: program.fields(), values }).map_err(|failure| {
                            visit_failure = Some(failure);
                            ExecutionError::evaluation("slot row visit failed")
                        })
                    },
                );
                if let Some(failure) = visit_failure {
                    return Err(backend_storage_error(failure));
                }
                match emitted.map_err(backend_storage_error)? {
                    0 => slot_filtered = slot_filtered.saturating_add(1),
                    rows => returned = returned.saturating_add(rows),
                }
                Ok(true)
            },
        )
        .map_err(storage_engine_error)?;
        Ok(Some(ExecutionStatistics::streamed_with_strategies(
            scanned,
            filtered.saturating_add(slot_filtered),
            returned,
            ExecutionStrategies::default().with(ExecutionStrategy::CollectionScan).with(ExecutionStrategy::PartialDocuments),
        )))
    }

    /// The output documents of any streaming `prefix`, handed as slot rows
    /// holding `downstream`: the path for prefixes that do not run on slots.
    fn stream_document_rows( &self, prefix: &PhysicalPlan, downstream: &[crate::query::ExpressionFieldPath], visitor: &mut dyn FnMut(SlotRowRef<'_>) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        let mut values = Vec::with_capacity(downstream.len());
        self.stream_read_pipeline(prefix, &mut |stored| {
            values.clear();
            values.extend(downstream.iter().map(|field| field.iter().skip(1).try_fold(stored.document().get(field.first())?, |value, segment| value.as_object()?.get(segment)).cloned()));
            visitor(SlotRowRef { id: *stored.id(), version: stored.version(), fields: downstream, values: &values })
        })
    }

    /// [`Self::stream_slot_rows`] for a consumer that reads batches: when
    /// every stage of `prefix` runs on batches, `on_batch` receives each
    /// batch that ran (its selection holds the rows that passed, its outputs
    /// follow the returned field order); rows of a batch that aborted, and
    /// every row when the stages cannot run on batches, go to `on_row`.
    /// The field order is that of [`SlotRowRef::fields`]. Without
    /// `locators`, rows may carry [`NO_LOCATOR`].
    fn stream_slot_batches( &self, prefix: &PhysicalPlan, downstream: &[crate::query::ExpressionFieldPath], keyed: &[crate::query::ExpressionFieldPath], locators: bool, on_batch: &mut dyn FnMut(&crate::query::BatchRunner, &[crate::query::ExpressionFieldPath]) -> EngineResult<()>, on_row: &mut dyn FnMut(SlotRowRef<'_>) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        self.stream_slot_batches_gated(prefix, downstream, keyed, locators, None, on_batch, on_row)
    }

    /// [`Self::stream_slot_batches`], where `gate` may refuse blocks from the
    /// source columns of its fields (their positions among the block's
    /// columns), when each is a source ([`scan_batched`]) and no stage runs
    /// before (whose errors and dropped rows a refused block would skip), or,
    /// for a gate that only refuses, only filters that cannot fail.
    fn stream_slot_batches_gated( &self, prefix: &PhysicalPlan, downstream: &[crate::query::ExpressionFieldPath], keyed: &[crate::query::ExpressionFieldPath], locators: bool, gate_on: Option<BlockGateOn<'_>>, on_batch: &mut dyn FnMut(&crate::query::BatchRunner, &[crate::query::ExpressionFieldPath]) -> EngineResult<()>, on_row: &mut dyn FnMut(SlotRowRef<'_>) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        let crate::query::PhysicalAccess::CollectionScan { options } = prefix.source().access() else {
            return self.stream_document_rows(prefix, downstream, on_row);
        };
        if self.is_virtual(prefix.source().collection()) {
            return Ok(None);
        }
        let read = self.storage.read().map_err(storage_engine_error)?;
        let Some((gate, program)) = slot_prefix(read.as_ref(), prefix, downstream, false)? else {
            drop(read);
            return self.stream_slot_rows(prefix, downstream, on_row);
        };
        let Some(mut batch) = crate::query::BatchProgram::compile(prefix.operators(), gate.layout().fields(), program.fields()) else {
            return self.stream_slot_rows(prefix, downstream, on_row);
        };
        batch.code_outputs(&keyed.iter().filter_map(|field| program.fields().iter().position(|output| output == field)).collect::<Vec<_>>());
        let batch = Arc::new(batch);
        PARTIAL_DOCUMENTS_USED.with(|used| used.set(true));
        let (filtered, returned) = (std::cell::Cell::new(0u64), std::cell::Cell::new(0u64));
        let on_row = std::cell::RefCell::new(on_row);
        let mut row = Vec::new();
        let sources = gate.layout().fields();
        // A gate that only refuses blocks may sit behind filters that cannot
        // fail: they would only drop rows.
        let behind = |refuses: bool| -> EngineResult<bool> { Ok(prefix.operators().is_empty() || (refuses && only_total_filters(read.as_ref(), prefix)?)) };
        let gate_on = match gate_on {
            Some(gate) if gate.3 || behind(gate.2)? => Some(gate),
            _ => None,
        };
        let mut consumer = gate_on.and_then(|(fields, admit, _, _)| {
            let columns = fields.iter().map(|field| sources.iter().position(|source| source == field)).collect::<Option<Vec<_>>>()?;
            let mask = (0..sources.len()).map(|source| columns.contains(&source)).collect::<Vec<_>>();
            Some((mask, move |block: &crate::storage::ProjectedBlock<'_, '_>| admit(block, &columns).map_err(backend_storage_error)))
        });
        let scanned = scan_batched(
            read.as_ref(),
            prefix.source().collection(),
            {
                let options = reusable_projected_scan_options(*options, prefix, &gate);
                if locators { options } else { options.without_locators() }
            },
            gate.layout().storage_fields(),
            &batch,
            crate::query::GroupPruning::new(prefix.operators(), gate.layout().storage_fields()).as_ref(),
            consumer.as_mut().map(|(fields, admit)| crate::storage::BlockGate { fields: fields.clone(), admit }),
            &mut |runner| {
                let passed = runner.selection().len() as u64;
                filtered.set(filtered.get() + runner.dropped() as u64);
                returned.set(returned.get() + passed);
                on_batch(runner, program.fields()).map_err(backend_storage_error)
                    .map(|()| true)
            },
            &mut |id, version, values| {
                let accepted = gate
                    .accepts_with(values, |expression, resolver| self.runtime.evaluate_resolved_predicate(expression, resolver))
                    .map_err(backend_storage_error)?;
                if !accepted {
                    filtered.set(filtered.get() + 1);
                    return Ok(true);
                }
                row.clear();
                row.extend_from_slice(values);
                row.resize(program.width(), None);
                let mut visit_failure = None;
                let kept = program
                    .run(&mut row, |expression, resolver| self.runtime.evaluate_resolved_predicate(expression, resolver))
                    .map_err(backend_storage_error)?;
                if !kept {
                    filtered.set(filtered.get() + 1);
                    return Ok(true);
                }
                if let Err(failure) = (on_row.borrow_mut())(SlotRowRef { id, version, fields: program.fields(), values: &row }) {
                    visit_failure = Some(failure);
                }
                if let Some(failure) = visit_failure {
                    return Err(backend_storage_error(failure));
                }
                returned.set(returned.get() + 1);
                Ok(true)
            },
        )
        .map_err(storage_engine_error)?;
        Ok(Some(ExecutionStatistics::streamed_with_strategies(
            scanned,
            filtered.get(),
            returned.get(),
            ExecutionStrategies::default().with(ExecutionStrategy::CollectionScan).with(ExecutionStrategy::PartialDocuments),
        )))
    }

    /// Hydrates one retained locator from the same storage snapshot and
    /// reapplies any pre-blocking Select stage that was deferred by the
    /// projected access vector.
    /// Replaces rows read as partial documents with their complete stored
    /// documents run through `prefix` again, so computed fields match the
    /// complete-document pipeline. Rows changed or deleted since are dropped.
    fn hydrate_replayed_rows( &self, prefix: &PhysicalPlan, rows: Vec<ExecutionRow>, ) -> EngineResult<Vec<ExecutionRow>> {
        self.hydrate_replayed(prefix, rows.iter().map(|row| (*row.id(), row.version())))
    }

    /// Loads each retained row by identity and replays the prefix on it, so a
    /// consumer that ranked or sampled partial rows returns complete ones.
    /// A row changed since it was read is dropped, as a scan would not see it.
    fn hydrate_replayed( &self, prefix: &PhysicalPlan, locators: impl IntoIterator<Item = (DocumentId, DocumentVersion)>, ) -> EngineResult<Vec<ExecutionRow>> {
        let read = self.storage.read().map_err(storage_engine_error)?;
        // Rows are read in identifier order, then replayed in the order given.
        let locators = locators.into_iter().collect::<Vec<_>>();
        let mut order = (0..locators.len()).collect::<Vec<_>>();
        order.sort_unstable_by_key(|position| locators[*position].0);
        let ids = order.iter().map(|position| locators[*position].0).collect::<Vec<_>>();
        let mut loaded = std::iter::repeat_with(|| None).take(locators.len()).collect::<Vec<Option<StoredDocument>>>();
        read.get_each(prefix.source().collection(), &ids, &mut |index, stored| {
            let position = order[index];
            loaded[position] = stored.filter(|stored| stored.version() == locators[position].1);
            Ok(true)
        })
        .map_err(storage_engine_error)?;
        let mut chain = StreamingRowChain::new(prefix.operators());
        let mut hydrated = Vec::new();
        for stored in loaded.into_iter().flatten() {
            self.drive_streaming_row(&mut chain, 0, ExecutionRow::from_stored(stored), None, &mut |row| {
                hydrated.push(row);
                Ok(())
            })
            .map_err(storage_engine_error)?;
        }
        Ok(hydrated)
    }

    /// Executes a complete blocking sort from projected values when the compact
    /// locator+key working set fits the governed budget. If it does not fit, the
    /// caller falls back to the established external Document sort.
    /// Top-N over a prefix of row-local stages read as slots (`derive`,
    /// `select … as`, `rename`, `drop`, `where`), such as the hidden `derive`
    /// of `sort <expression>`: only the key values of the current winners are
    /// kept; the winners are then loaded and the prefix replayed on them.
    /// Unbounded `sort` on batches (or slot rows): the sort keys of every
    /// row, ordered by key then identifier in scan direction, then the rows
    /// reloaded in that order. A consumer that declares the fields it reads
    /// gets them from the slots instead, as partial documents. `None` once
    /// the kept values exceed the stage's memory.
    fn try_slot_sort( &self, prefix: &PhysicalPlan, keys: &[crate::query::SortKey], memory: &StageMemory<'_>, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        let crate::query::PhysicalAccess::CollectionScan { options } = prefix.source().access() else {
            return Ok(None);
        };
        // Reloaded rows are complete, whatever the consumer reads of them.
        if keys.is_empty() || !prefix.operators().iter().all(replays_one_row) {
            return Ok(None);
        }
        let reverse = matches!(options.direction(), crate::storage::ScanDirection::Reverse);
        // The keys come first in the slots, then the other fields the
        // consumer reads.
        let mut key_fields = keys.iter().map(|key| key.field().clone()).collect::<Vec<_>>();
        let consumer = prefix.field_demand();
        for field in consumer.unwrap_or_default() {
            if !key_fields.contains(field) {
                key_fields.push(field.clone());
            }
        }
        let slots = (0..key_fields.len()).collect::<Vec<_>>();
        let rows = std::cell::RefCell::new(Vec::<(Vec<Option<Value>>, DocumentId, DocumentVersion)>::new());
        let bytes = std::cell::Cell::new(0usize);
        let keep = |values: &[Option<ProjectedValueRef<'_>>], id: DocumentId, version: DocumentVersion| -> EngineResult<()> {
            bytes.set(bytes.get().saturating_add(crate::query::projected_row_working_bytes_refs(values, &slots)));
            if memory.fits(bytes.get()) {
                rows.borrow_mut().push((values.iter().map(|value| value.as_ref().map(ProjectedValueRef::to_value)).collect(), id, version));
            }
            Ok(())
        };
        let positions_of = |fields: &[crate::query::ExpressionFieldPath]| -> Vec<usize> {
            key_fields.iter().map(|field| fields.iter().position(|slot| slot == field).expect("slot prefix holds every key")).collect()
        };
        let Some(prefix_stats) = self.stream_slot_batches(
            prefix,
            &key_fields,
            &[],
            true,
            &mut |runner, fields| {
                if !memory.fits(bytes.get()) {
                    return Ok(());
                }
                let columns = positions_of(fields).into_iter().map(|position| runner.output(position)).collect::<Vec<_>>();
                let mut values = Vec::with_capacity(columns.len());
                for &row in runner.selection() {
                    values.clear();
                    values.extend(columns.iter().map(|column| column.and_then(|column| column.value_ref(row as usize))));
                    let (id, version) = runner.id(row as usize);
                    keep(&values, id, version)?;
                }
                Ok(())
            },
            &mut |row| {
                let values = positions_of(row.fields).into_iter().map(|position| row.values[position].as_ref().map(ProjectedValueRef::borrowed)).collect::<Vec<_>>();
                keep(&values, row.id, row.version)
            },
        )?
        else {
            return Ok(None);
        };
        if !memory.fits(bytes.get()) {
            return Ok(None);
        }
        let mut rows = rows.into_inner();
        let mut failure = None;
        rows.sort_by(|left, right| {
            let ordering = crate::query::compare_owned_projected_rows(keys, &left.0, &right.0).unwrap_or_else(|error| {
                failure.get_or_insert(error);
                Ordering::Equal
            });
            let by_id = if reverse { right.1.cmp(&left.1) } else { left.1.cmp(&right.1) };
            ordering.then(by_id)
        });
        if let Some(error) = failure {
            return Err(EngineError::execution(error));
        }
        let mut returned = 0u64;
        if consumer.is_some() {
            PARTIAL_DOCUMENTS_USED.with(|used| used.set(true));
            for (values, id, version) in rows {
                let document = Arc::new(partial_document(&key_fields, &values));
                visitor(ExecutionRow::from_stored(StoredDocument::new(id, version, document).map_err(storage_engine_error)?))?;
                returned += 1;
            }
        } else {
            for row in self.hydrate_replayed(prefix, rows.into_iter().map(|(_, id, version)| (id, version)))? {
                visitor(row)?;
                returned += 1;
            }
        }
        Ok(Some(ExecutionStatistics::streamed_with_strategies(
            prefix_stats.scanned(),
            prefix_stats.filtered(),
            returned,
            ExecutionStrategies::default().with(ExecutionStrategy::CollectionScan).with(ExecutionStrategy::InMemorySort),
        )))
    }

    fn try_slot_top_n( &self, prefix: &PhysicalPlan, keys: &[crate::query::SortKey], limit: usize, ) -> EngineResult<Option<(Vec<ExecutionRow>, ExecutionStatistics)>> {
        const MAX_KEYS: usize = 4;
        const KEY_SLOTS: [usize; MAX_KEYS] = [0, 1, 2, 3];
        // Reloaded rows are complete, whatever the consumer reads of them.
        if limit == 0 || keys.len() > MAX_KEYS || !prefix.operators().iter().all(replays_one_row) {
            return Ok(None);
        }
        let key_fields = keys.iter().map(|key| key.field().clone()).collect::<Vec<_>>();
        let top = std::cell::RefCell::new(BoundedProjectedTopN::new(limit));
        let positions_of = |fields: &[crate::query::ExpressionFieldPath]| -> Vec<usize> {
            key_fields.iter().map(|field| fields.iter().position(|slot| slot == field).expect("slot prefix holds every key")).collect()
        };
        let mut positions: Option<Vec<usize>> = None;
        let mut batch_positions: Option<Vec<usize>> = None;
        // A block none of whose rows passes the bound is not decoded further.
        let mut admit = |block: &crate::storage::ProjectedBlock<'_, '_>, columns: &[usize]| -> EngineResult<bool> {
            let bound = TopBound::of(&top.borrow());
            Ok(bound.admits_block(keys, &block.columns[columns[0]]).unwrap_or_else(|| (0..block.rows).any(|row| !bound.rejects_cells(keys, |key| block.columns[columns[key]].get(row)))))
        };
        let Some(prefix_stats) = self.stream_slot_batches_gated(
            prefix,
            &key_fields,
            &[],
            true,
            Some((&key_fields[..], &mut admit, true, false)),
            &mut |runner, fields| {
                let positions = batch_positions.get_or_insert_with(|| positions_of(fields));
                let columns = positions.iter().map(|position| runner.output(*position)).collect::<Vec<_>>();
                let mut top = top.borrow_mut();
                let mut bound = TopBound::of(&top);
                for &row in runner.selection() {
                    let row = row as usize;
                    if bound.rejects(keys, &columns, row) {
                        continue;
                    }
                    let mut values: [Option<ProjectedValueRef<'_>>; MAX_KEYS] = [None, None, None, None];
                    for (value, column) in values.iter_mut().zip(&columns) {
                        *value = column.and_then(|column| column.value_ref(row));
                    }
                    let (id, version) = runner.id(row);
                    top.push_refs(keys, &KEY_SLOTS[..keys.len()], id, version, &values[..keys.len()]).map_err(EngineError::execution)?;
                    bound = TopBound::of(&top);
                }
                Ok(())
            },
            &mut |row| {
                let positions = positions.get_or_insert_with(|| positions_of(row.fields));
                let mut values: [Option<ProjectedValueRef<'_>>; MAX_KEYS] = [None, None, None, None];
                for (value, position) in values.iter_mut().zip(positions.iter()) {
                    *value = row.values[*position].as_ref().map(ProjectedValueRef::borrowed);
                }
                top.borrow_mut().push_refs(keys, &KEY_SLOTS[..keys.len()], row.id, row.version, &values[..keys.len()])
                    .map_err(EngineError::execution)
            },
        )?
        else {
            return Ok(None);
        };
        let top = top.into_inner();
        let winners = top.into_sorted_winners(keys).map_err(EngineError::execution)?;
        let rows = self.hydrate_replayed(prefix, winners.iter().map(|winner| (*winner.id(), winner.version())))?;
        let returned = rows.len() as u64;
        Ok(Some((
            rows,
            ExecutionStatistics::streamed_with_strategies(
                prefix_stats.scanned(),
                // Each read row is dropped or returned (blocks the gate
                // refused included).
                prefix_stats.scanned().saturating_sub(returned),
                returned,
                {
                    let strategies = ExecutionStrategies::default().with(ExecutionStrategy::CollectionScan).with(ExecutionStrategy::TopN);
                    if prefix_stats.strategies().contains(ExecutionStrategy::PartialDocuments) { strategies.with(ExecutionStrategy::PartialDocuments) } else { strategies }
                },
            ),
        )))
    }

    /// Executes explicit-field distinct directly from projected values. The
    /// canonical key remains owned by the runtime; the engine retains only one
    /// source locator per key and hydrates winners after deduplication. A trailing
    /// limit bounds the retained key frontier without changing key-order output.
    /// In-memory `distinct` over batches: rows find their key in a
    /// [`crate::query::keys::KeyTable`] from typed cells, and the encoded key
    /// that orders the output is built once per key. Explicit top-level
    /// fields, or the fields a `select` leaves (whole-document distinct).
    /// The first row of each key (lowest identifier in scan direction) is
    /// loaded and replayed once at the end, unless only a `count` follows
    /// (`rows_unused`). Unbounded output only: a bounded one keeps its
    /// frontier of the smallest keys. `None` when the plan does not fit or
    /// the keys exceed `budget`.
    /// `distinct | limit n` on batches, whatever the key cardinality: the
    /// `n` smallest keys, each with its earliest row, then those rows
    /// reloaded in key order. A key is encoded field by field and a row is
    /// dropped once its key sorts after the largest key kept.
    fn try_batch_distinct_limited( &self, prefix: &PhysicalPlan, fields: &[crate::query::ExpressionFieldPath], limit: usize, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        let crate::query::PhysicalAccess::CollectionScan { options } = prefix.source().access() else {
            return Ok(None);
        };
        // Rows are reloaded by identity: each must come from one document.
        if !prefix.operators().iter().all(replays_one_row) {
            return Ok(None);
        }
        let reverse = matches!(options.direction(), crate::storage::ScanDirection::Reverse);
        // Full-document distinct reads every top-level field the collection
        // holds, under filters that keep documents whole.
        let whole_document = fields.is_empty();
        let catalog;
        let fields = if whole_document {
            if !prefix.operators().iter().all(|operator| matches!(operator, PhysicalOperator::Filter { .. })) {
                return Ok(None);
            }
            let read = self.storage.read().map_err(storage_engine_error)?;
            let Some(names) = read.top_level_fields(prefix.source().collection()).map_err(storage_engine_error)? else {
                return Ok(None);
            };
            let Ok(names) = names.iter().map(|name| crate::query::ExpressionFieldPath::new([name.as_str()])).collect::<Result<Vec<_>, _>>() else {
                return Ok(None);
            };
            catalog = names;
            &catalog[..]
        } else {
            fields
        };
        if fields.iter().any(|field| field.first() == "_id") {
            return Ok(None);
        }
        let kept = std::cell::RefCell::new(BTreeMap::<Box<[u8]>, (DocumentId, DocumentVersion)>::new());
        let key = std::cell::RefCell::new(Vec::<u8>::with_capacity(128));
        // A first key field read as dictionary codes: a code whose component
        // passes the frontier rejects every row holding it (with as many
        // fields present) while its dictionary lasts, the frontier only
        // falling. Per code, the field count it was rejected with.
        let rejected = std::cell::RefCell::new((None::<u64>, Vec::<u8>::new()));
        #[allow(clippy::too_many_arguments)]
        fn rejects<'v>(runtime: &crate::query::QueryRuntime, fields: &[crate::query::ExpressionFieldPath], limit: usize, kept: &BTreeMap<Box<[u8]>, (DocumentId, DocumentVersion)>, key: &mut Vec<u8>, rejected: &mut (Option<u64>, Vec<u8>), (dictionary, code): (u64, u32), present: Option<usize>, value: impl FnOnce() -> Option<ProjectedValueRef<'v>>) -> bool {
            let Some(count) = present.map_or(Some(0), |present| u8::try_from(present).ok().filter(|count| *count < u8::MAX)) else { return false };
            if rejected.0 != Some(dictionary) {
                rejected.0 = Some(dictionary);
                rejected.1.clear();
            }
            let code = code as usize;
            if code >= rejected.1.len() {
                rejected.1.resize(code + 1, u8::MAX);
            }
            if rejected.1[code] == count {
                return true;
            }
            // Later fields written missing: a key passing the frontier so
            // passes it with any values there. The cell is read only here,
            // once per code.
            if kept.len() < limit {
                return false;
            }
            let value = value();
            let passes = !runtime.write_bounded_distinct_key(fields, present, |index| if index == 0 { value.clone() } else { None }, kept.last_key_value().map(|(largest, _)| largest.as_ref()), key);
            if passes {
                rejected.1[code] = count;
            }
            passes
        }
        #[allow(clippy::too_many_arguments)]
        fn consider<'v>(runtime: &crate::query::QueryRuntime, fields: &[crate::query::ExpressionFieldPath], limit: usize, reverse: bool, kept: &mut BTreeMap<Box<[u8]>, (DocumentId, DocumentVersion)>, key: &mut Vec<u8>, present: Option<usize>, value: impl Fn(usize) -> Option<ProjectedValueRef<'v>>, locator: (DocumentId, DocumentVersion)) {
            let frontier = if kept.len() >= limit { kept.last_key_value().map(|(largest, _)| largest.as_ref()) } else { None };
            if !runtime.write_bounded_distinct_key(fields, present, value, frontier, key) {
                return;
            }
            match kept.get_mut(key.as_slice()) {
                Some(retained) => {
                    if (locator.0 < retained.0) != reverse && locator.0 != retained.0 {
                        *retained = locator;
                    }
                }
                None => {
                    kept.insert(Box::from(key.as_slice()), locator);
                    if kept.len() > limit {
                        kept.pop_last();
                    }
                }
            }
        }
        let positions_of = |available: &[crate::query::ExpressionFieldPath]| -> Option<Vec<usize>> { fields.iter().map(|field| available.iter().position(|other| other == field)).collect() };
        // A block whose rows all hold rejected codes is not decoded further: a
        // whole document's field count must be known first.
        let mut admit = |block: &crate::storage::ProjectedBlock<'_, '_>, columns: &[usize]| -> EngineResult<bool> {
            let (Some(codes), true) = (block.codes[columns[0]], block.complete || !whole_document) else { return Ok(true) };
            let (kept, mut key, mut rejected) = (kept.borrow(), key.borrow_mut(), rejected.borrow_mut());
            let present = whole_document.then_some(fields.len());
            Ok(!codes.codes.iter().enumerate().all(|(row, code)| *code != crate::storage::BlockCodes::NONE && rejects(&self.runtime, fields, limit, &kept, &mut key, &mut rejected, (codes.dictionary, *code), present, || block.columns[columns[0]].get(row))))
        };
        let Some(statistics) = self.stream_slot_batches_gated(
            prefix,
            fields,
            fields,
            true,
            Some((fields, &mut admit, false, false)),
            &mut |runner, available| {
                let Some(positions) = positions_of(available) else {
                    return Err(EngineError::execution(ExecutionError::evaluation("distinct fields are absent from the batch outputs")));
                };
                let columns = positions.iter().map(|position| runner.output(*position)).collect::<Vec<_>>();
                let (mut kept, mut key, mut rejected) = (kept.borrow_mut(), key.borrow_mut(), rejected.borrow_mut());
                for &row in runner.selection() {
                    let row = row as usize;
                    let present = whole_document.then(|| columns.iter().filter(|column| column.is_some_and(|column| column.tag(row) != crate::query::Tag::Missing)).count());
                    let value = |index: usize| columns[index].and_then(|column| column.value_ref(row));
                    if columns[0].and_then(|column| column.code(row)).is_some_and(|code| rejects(&self.runtime, fields, limit, &kept, &mut key, &mut rejected, code, present, || value(0))) {
                        continue;
                    }
                    consider(&self.runtime, fields, limit, reverse, &mut kept, &mut key, present, value, runner.id(row));
                }
                Ok(())
            },
            &mut |row| {
                let Some(positions) = positions_of(row.fields) else {
                    return Err(EngineError::execution(ExecutionError::evaluation("distinct fields are absent from the slot row")));
                };
                let values = positions.iter().map(|position| row.values[*position].as_ref()).collect::<Vec<_>>();
                let present = whole_document.then(|| values.iter().filter(|value| value.is_some()).count());
                consider(&self.runtime, fields, limit, reverse, &mut kept.borrow_mut(), &mut key.borrow_mut(), present, |index| values[index].map(ProjectedValueRef::borrowed), (row.id, row.version));
                Ok(())
            },
        )?
        else {
            return Ok(None);
        };
        let mut returned = 0u64;
        for row in self.hydrate_replayed(prefix, kept.into_inner().into_values())? {
            visitor(row)?;
            returned += 1;
        }
        // Rows a distinct leaves out count as filtered, as on the row path.
        Ok(Some(ExecutionStatistics::streamed_with_strategies(
            statistics.scanned(),
            statistics.filtered().saturating_add(statistics.returned().saturating_sub(returned)),
            returned,
            ExecutionStrategies::default().with(ExecutionStrategy::CollectionScan).with(ExecutionStrategy::InMemoryDistinct),
        )))
    }

    fn try_batch_distinct( &self, prefix: &PhysicalPlan, fields: &[crate::query::ExpressionFieldPath], memory: &StageMemory<'_>, output_limit: Option<usize>, rows_unused: bool, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        let crate::query::PhysicalAccess::CollectionScan { options } = prefix.source().access() else {
            return Ok(None);
        };
        // Rows are reloaded by identity: each must come from one document.
        if !rows_unused && !prefix.operators().iter().all(replays_one_row) {
            return Ok(None);
        }
        let reverse = matches!(options.direction(), crate::storage::ScanDirection::Reverse);
        // Whole-document distinct after a final `select` of top-level fields:
        // the selected values are the key, read under the stages before it.
        let scan_prefix;
        let (scan, key_fields, names) = if fields.is_empty() {
            let operators = prefix.operators();
            let Some(PhysicalOperator::Select { fields: selected }) = operators.last() else { return Ok(None) };
            let mut names = selected.iter().map(|field| field.first()).collect::<Vec<_>>();
            names.sort_unstable();
            names.dedup();
            if names.len() != selected.len() || selected.iter().any(|field| field.len() != 1 || field.first() == "_id") {
                return Ok(None);
            }
            let kept = operators.len() - 1;
            scan_prefix = PhysicalPlan::new(prefix.source().clone(), operators[..kept].iter().cloned())
                .map_err(EngineError::physical_planning)?
                .with_source_access_negotiation(prefix.source_access_vector(), prefix.projected_prefix_len().min(kept), prefix.source_projection_reuse());
            let Ok(paths) = names.iter().map(|name| crate::query::ExpressionFieldPath::new([*name])).collect::<Result<Vec<_>, _>>() else { return Ok(None) };
            (&scan_prefix, paths, Some(names))
        } else {
            if fields.iter().any(|field| field.first() == "_id") {
                return Ok(None);
            }
            (prefix, fields.to_vec(), None)
        };
        let table = std::cell::RefCell::new(crate::query::keys::KeyTable::new(key_fields.len()));
        let locators = std::cell::RefCell::new(Vec::<(DocumentId, DocumentVersion)>::new());
        let exceeded = std::cell::Cell::new(false);
        let first = |locators: &mut Vec<(DocumentId, DocumentVersion)>, key: u32, locator: (DocumentId, DocumentVersion)| {
            match locators.get_mut(key as usize) {
                Some(kept) => {
                    if (locator.0 < kept.0) != reverse && locator.0 != kept.0 {
                        *kept = locator;
                    }
                }
                None => locators.push(locator),
            }
        };
        let over = |table: &crate::query::keys::KeyTable| !memory.fits(table.bytes().saturating_add(table.len().saturating_mul(32)));
        let positions_of = |fields: &[crate::query::ExpressionFieldPath]| -> Option<Vec<usize>> { key_fields.iter().map(|key| fields.iter().position(|field| field == key)).collect() };
        let mut batch_positions: Option<Option<Vec<usize>>> = None;
        let mut row_positions: Option<Option<Vec<usize>>> = None;
        let mut keys = Vec::new();
        let Some(statistics) = self.stream_slot_batches(
            scan,
            &key_fields,
            &key_fields,
            !rows_unused,
            &mut |runner, fields| {
                if exceeded.get() {
                    return Ok(());
                }
                let Some(positions) = batch_positions.get_or_insert_with(|| positions_of(fields)) else {
                    exceeded.set(true);
                    return Ok(());
                };
                let columns = positions.iter().map(|position| runner.output(*position)).collect::<Vec<_>>();
                let mut table = table.borrow_mut();
                table.batch(&columns, runner.selection(), &mut keys);
                if !rows_unused {
                    let mut locators = locators.borrow_mut();
                    for (key, row) in keys.iter().zip(runner.selection()) {
                        first(&mut locators, *key, runner.id(*row as usize));
                    }
                }
                exceeded.set(over(&table));
                Ok(())
            },
            &mut |row| {
                if exceeded.get() {
                    return Ok(());
                }
                let Some(positions) = row_positions.get_or_insert_with(|| positions_of(row.fields)) else {
                    exceeded.set(true);
                    return Ok(());
                };
                let values = positions.iter().map(|position| row.values[*position].as_ref().map(ProjectedValueRef::borrowed)).collect::<Vec<_>>();
                let mut table = table.borrow_mut();
                let key = table.row(&values);
                if !rows_unused {
                    first(&mut locators.borrow_mut(), key, (row.id, row.version));
                }
                exceeded.set(over(&table));
                Ok(())
            },
        )?
        else {
            return Ok(None);
        };
        if exceeded.get() {
            return Ok(None);
        }
        let table = table.into_inner();
        let mut order = (0..table.len() as u32).collect::<Vec<_>>();
        match &names {
            // Keys of selected fields are documents: their present fields, by name.
            Some(names) => {
                let mut buffer = Vec::with_capacity(64);
                let mut encoded = order.iter().map(|key| { table.write_document_key(*key, names, &mut buffer); (Box::<[u8]>::from(buffer.as_slice()), *key) }).collect::<Vec<_>>();
                encoded.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                order = encoded.into_iter().map(|(_, key)| key).collect();
            }
            None => table.sort_by_key(&mut order, |key| *key),
        }
        order.truncate(output_limit.unwrap_or(usize::MAX));
        let mut returned = 0u64;
        if rows_unused {
            for (ordinal, _) in order.iter().enumerate() {
                let id = DocumentId::synthetic(0x6469_7374, usize_to_u64_saturating(ordinal));
                visitor(ExecutionRow::synthetic(crate::query::SyntheticDocument::new(id, Arc::new(Document::new()))))?;
                returned += 1;
            }
        } else {
            let locators = locators.into_inner();
            for row in self.hydrate_replayed(prefix, order.iter().map(|key| locators[*key as usize]))? {
                visitor(row)?;
                returned += 1;
            }
        }
        Ok(Some(ExecutionStatistics::streamed_with_strategies(
            statistics.scanned(),
            statistics.filtered().saturating_add(statistics.returned().saturating_sub(returned)),
            returned,
            ExecutionStrategies::default().with(ExecutionStrategy::CollectionScan).with(ExecutionStrategy::InMemoryDistinct),
        )))
    }

    /// In-memory `group` over batches ([`BatchGroups`]): `None` when the
    /// prefix cannot run on batches, `Some(None)` when the groups exceed the
    /// working set.
    /// `group` on batches, slot rows or the prefix's output documents,
    /// whichever the prefix runs on; `Some(None)` when the groups exceed
    /// `budget`, `None` when the measures cannot fold densely.
    fn try_batch_group( &self, prefix: &PhysicalPlan, keys: &[crate::query::ExpressionFieldPath], grouping_keys: &[crate::query::ExpressionFieldPath], required: &[crate::query::ExpressionFieldPath], memory: &StageMemory<'_>, group_limit: Option<usize>, top: GroupTop<'_>, ) -> EngineResult<Option<Option<(GroupRows, ExecutionStatistics)>>> {
        let group_layout = ProjectedValueLayout::new(required.iter().cloned()).map_err(EngineError::execution)?;
        let key_indexes = group_layout.slots(grouping_keys).map_err(EngineError::execution)?;
        let Some(dense) = self.runtime.incremental_group_accumulator(keys).map_err(EngineError::execution)?.into_dense() else {
            return Ok(None);
        };
        let groups = std::cell::RefCell::new(BatchGroups::new(dense, keys, &key_indexes, memory, group_limit));
        let positions_of = |fields: &[crate::query::ExpressionFieldPath]| -> EngineResult<Vec<usize>> {
            group_layout.fields().iter().map(|field| fields.iter().position(|slot| slot == field))
                .collect::<Option<_>>()
                .ok_or_else(|| EngineError::execution(ExecutionError::evaluation("group input is absent from the rows")))
        };
        // A one-key group folds whole blocks; measures a lone `derive`
        // computes are computed on the block's typed columns.
        let fused = fused_derive(prefix, group_layout.fields(), &key_indexes);
        let mut fold_block = |block: &crate::storage::ProjectedBlock<'_, '_>, columns: &[usize]| -> EngineResult<bool> {
            let Some(fused) = &fused else {
                return groups.borrow_mut().push_block(block, columns).map_err(storage_engine_error).map(|()| false);
            };
            let sources = columns.iter().map(|column| &block.columns[*column]).collect::<Vec<_>>();
            let mut computed = Vec::with_capacity(fused.inputs.len());
            for input in &fused.inputs {
                if let FusedInput::Kernel(kernel) = input {
                    // A block the kernel cannot compute goes through the `derive`.
                    let Some(column) = kernel.run(block.rows, &sources) else { return Ok(true) };
                    computed.push(column);
                }
            }
            let computed = computed.iter().map(crate::query::KernelColumn::block).collect::<Vec<_>>();
            let mut next = computed.iter();
            let inputs = fused.inputs.iter().map(|input| match input {
                FusedInput::Source(source) => sources[*source],
                FusedInput::Kernel(_) => next.next().expect("one column per kernel"),
            }).collect::<Vec<_>>();
            let FusedInput::Source(key) = fused.inputs[key_indexes[0]] else { unreachable!("a fused key is a source field") };
            groups.borrow_mut().push_block_inputs(block, columns[key], &inputs).map_err(storage_engine_error).map(|()| false)
        };
        let gate_fields = fused.as_ref().map_or(group_layout.fields(), |fused| &fused.sources[..]);
        let gate = (key_indexes.len() == 1 && group_limit.is_none() && groups.borrow().dense.folds_blocks()).then_some((gate_fields, &mut fold_block as BlockAdmit<'_>, false, fused.is_some()));
        let Some(statistics) = self.stream_slot_batches_gated(
            prefix,
            group_layout.fields(),
            grouping_keys,
            false,
            gate,
            &mut |runner, fields| {
                let columns = positions_of(fields)?.into_iter().map(|position| runner.output(position)).collect::<Vec<_>>();
                groups.borrow_mut().push_batch(&columns, runner.selection()).map_err(storage_engine_error)
            },
            &mut |row| {
                let values = positions_of(row.fields)?.into_iter().map(|position| row.values[position].as_ref().map(ProjectedValueRef::borrowed)).collect::<Vec<_>>();
                groups.borrow_mut().push(&values).map_err(storage_engine_error)
            },
        )?
        else {
            return Ok(None);
        };
        let Some(rows) = groups.into_inner().finish(top)? else {
            return Ok(Some(None));
        };
        Ok(Some(Some((rows, statistics))))
    }

    fn try_in_memory_incremental_group( &self, prefix: &PhysicalPlan, keys: &[crate::query::ExpressionFieldPath], memory: &StageMemory<'_>, group_limit: Option<usize>, top: GroupTop<'_>, ) -> EngineResult<Option<(GroupRows, ExecutionStatistics)>> {
        // Rejects keys the group accumulator cannot hold before any scan.
        self.runtime.incremental_group_accumulator(keys).map_err(EngineError::execution)?;

        let (grouping_keys, required_input_fields) =
            crate::query::group_field_layout(keys).map_err(EngineError::execution)?;
        Ok(self.try_batch_group(prefix, keys, &grouping_keys, &required_input_fields, memory, group_limit, top)?.flatten())
    }


    /// Streams a governed blocking read plan without materializing its full input or output.
    ///
    /// Executes supported governed blocking shapes after a fully streaming prefix.
    ///
    /// A downstream linear bound is consumed generically; concrete sort, distinct and group
    /// algorithms remain local to the executor. External runs keep blocking work inside the
    /// governed memory budget.
    pub fn stream_governed_blocking_pipeline( &self, physical: &PhysicalPlan, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        if let Some(view) = self.temporal_view(physical)? {
            return view.stream_governed_blocking_pipeline(physical, visitor);
        }
        if !self.supports_governed_blocking_streaming(physical) {
            return Ok(None);
        }
        let shape = governed_shape(physical.operators()).expect("governed support check guarantees a shape");
        take_partial_documents_used();

        // The streaming tail (`select`, `skip`, `first`, `unwind`, a terminal
        // `count`, …) runs row by row on the core's output. A leading `skip`/
        // bound of the tail was already pushed into the core as a row limit.
        // Later blocking stages take the tail's rows, each in turn.
        let memory = QueryMemory::new(&self.memory_governor);
        let mut stages = self.later_stages(shape.rest, &memory)?;
        let mut chain = StreamingRowChain::new(shape.tail);
        let mut counted = 0u64;
        let mut delivered = 0u64;
        let mut open = !chain.has_empty_bound();
        let shared = if shape.tail.is_empty() && shape.rest.is_empty() && shape.count_alias.is_some() { self.try_shared_union_count(physical, &shape)? } else { None };
        let mut last = |row: ExecutionRow| -> EngineResult<()> {
            if shape.count_alias.is_some() {
                counted = counted.saturating_add(1);
                return Ok(());
            }
            visitor(row)?;
            delivered = delivered.saturating_add(1);
            Ok(())
        };
        let shared_count = shared.as_ref().map(|(count, _)| *count);
        let statistics = if let Some((_, statistics)) = shared {
            Some(statistics)
        } else {
            let mut tail_visitor = |row: ExecutionRow| -> EngineResult<()> {
                if !open {
                    return Ok(());
                }
                let mut failure = None;
                let driven = self.drive_streaming_row(&mut chain, 0, row, None, &mut |row| {
                    // An engine error comes back as it is, not reworded.
                    self.feed_later(&mut stages, row, None, &mut last).map_err(|error| {
                        let storage = backend_storage_error(&error);
                        failure = Some(error);
                        storage
                    })
                });
                open = match (driven, failure) {
                    (Ok(open), _) => open,
                    (Err(_), Some(error)) => return Err(error),
                    (Err(error), None) => return Err(storage_engine_error(error)),
                };
                Ok(())
            };
            let statistics = self.stream_governed_core(physical, &shape, &memory, &mut tail_visitor)?;
            if statistics.is_some() {
                self.finish_later(&mut stages, None, &mut last)?;
            }
            statistics
        };
        drop(last);
        counted = shared_count.unwrap_or(counted);
        let Some(mut statistics) = statistics else {
            return Ok(None);
        };
        if take_partial_documents_used() {
            statistics = ExecutionStatistics::streamed_with_strategies(
                statistics.scanned(),
                statistics.filtered(),
                statistics.returned(),
                statistics.strategies().with(ExecutionStrategy::PartialDocuments),
            );
        }
        ensure_exact_rows(shape.tail, &chain.limit_remaining)?;
        sinks::ensure_later_exact_rows(&stages)?;
        if let Some(alias) = shape.count_alias {
            let document = Arc::new(Document::from_fields([(alias, Value::from(counted))]));
            let stored = StoredDocument::new(DocumentId::synthetic(0x0063_6f75_6e74, 1), DocumentVersion::INITIAL, document)
                .map_err(storage_engine_error)?;
            visitor(ExecutionRow::from_stored(stored))?;
            delivered = 1;
        }
        if shape.tail.is_empty() && shape.rest.is_empty() && shape.count_alias.is_none() {
            return Ok(Some(statistics));
        }
        let (later_scanned, later_filtered, later_strategies) = sinks::later_statistics(&stages);
        Ok(Some(ExecutionStatistics::streamed_with_strategies(
            statistics.scanned().saturating_add(later_scanned),
            statistics.filtered().saturating_add(chain.filtered).saturating_add(later_filtered),
            delivered,
            later_strategies.iter().fold(statistics.strategies(), ExecutionStrategies::with),
        )))
    }

    /// `group` state spilled to sorted runs whenever it exceeds `budget`:
    /// from projected values when the prefix allows, from documents
    /// otherwise. External group is aggregate-first: bounded mergeable
    /// states stay in memory and compact partials are spilled.
    fn external_group_runs( &self, prefix: &PhysicalPlan, keys: &[crate::query::ExpressionFieldPath], memory: &StageMemory<'_>, spill: &SpillEngine, ) -> EngineResult<(Vec<SpillRun>, ExecutionStatistics)> {
        let mut partial_groups: HashMap<
            Arc<[u8]>,
            Box<dyn crate::query::IncrementalGroupAccumulator>,
        > = HashMap::new();
        let mut partial_bytes = 0usize;
        let mut runs = Vec::<SpillRun>::new();
        let (grouping_keys, _) =
            crate::query::group_field_layout(keys).map_err(EngineError::execution)?;
        let prefix_stats = self
            .stream_read_pipeline(prefix, &mut |stored| {
                let row = ExecutionRow::from_stored(stored);
                let key = self
                    .runtime
                    .distinct_key(&grouping_keys, row.document())
                    .map_err(EngineError::execution)?;
                if !partial_groups.contains_key(key.as_ref()) {
                    let estimate = group_state_estimate(key.len(), keys.len());
                    if !partial_groups.is_empty()
                        && !memory.fits(partial_bytes.saturating_add(estimate))
                    {
                        flush_partial_group_run(
                            self.runtime.as_ref(),
                            keys,
                            spill,
                            &mut partial_groups,
                            &mut runs,
                        )?;
                        partial_bytes = 0;
                    }
                    let accumulator = self
                        .runtime
                        .incremental_group_accumulator(keys)
                        .map_err(EngineError::execution)?;
                    partial_bytes = partial_bytes.saturating_add(estimate);
                    partial_groups.insert(key.clone(), accumulator);
                }
                partial_groups
                    .get_mut(key.as_ref())
                    .expect("partial group accumulator exists")
                    .push(row.document())
                    .map_err(EngineError::execution)
            })?
            .expect("validated streaming prefix");
        if !partial_groups.is_empty() {
            flush_partial_group_run(
                self.runtime.as_ref(),
                keys,
                spill,
                &mut partial_groups,
                &mut runs,
            )?;
        }
        Ok((runs, prefix_stats))
    }

    /// `… | union | on <same collection> | … | end | count` with row-local
    /// stages on both sides: one scan feeds both sides' batch programs.
    /// `None` when the shape does not apply or a batch aborts: the plan then
    /// runs as usual, scanning each side (nothing was emitted yet).
    fn try_shared_union_count( &self, physical: &PhysicalPlan, shape: &GovernedShape<'_>, ) -> EngineResult<Option<(u64, ExecutionStatistics)>> {
        let [PhysicalOperator::Union { collection, alias: None, pipeline }] = &physical.operators()[shape.blocking_index..shape.core_end] else {
            return Ok(None);
        };
        if !pipeline.operators().iter().all(is_row_operator) {
            return Ok(None);
        }
        let crate::query::PhysicalAccess::CollectionScan { options } = physical.source().access() else {
            return Ok(None);
        };
        if collection != physical.source().collection() || options.limit().is_some() || self.is_virtual(physical.source().collection()) {
            return Ok(None);
        }
        let read = self.storage.read().map_err(storage_engine_error)?;
        let sides = [&physical.operators()[..shape.blocking_index], pipeline.operators()];
        let mut fields = Vec::new();
        for operators in sides {
            let Some(demand) = crate::query::source_field_demand(operators, Some(&[])).and_then(|demand| partial_source_fields(read.as_ref(), &demand)) else {
                return Ok(None);
            };
            for field in demand {
                if !fields.contains(&field) {
                    fields.push(field);
                }
            }
        }
        let programs = sides.map(|operators| crate::query::BatchProgram::compile(operators, &fields, &[]).map(Arc::new));
        let [Some(main), Some(branch)] = programs else {
            return Ok(None);
        };
        let layout = ProjectedValueLayout::new(fields).map_err(EngineError::execution)?;
        let mut runners = [crate::query::BatchRunner::new(main), crate::query::BatchRunner::new(branch)];
        let (rows, passed, aborted) = (std::cell::Cell::new(0u64), [std::cell::Cell::new(0u64), std::cell::Cell::new(0u64)], std::cell::Cell::new(false));
        let flush = |runners: &mut [crate::query::BatchRunner; 2]| {
            rows.set(rows.get() + runners[0].len() as u64);
            for (runner, passed) in runners.iter_mut().zip(&passed) {
                if runner.run().is_err() {
                    aborted.set(true);
                }
                passed.set(passed.get() + runner.selection().len() as u64);
                runner.clear();
            }
            !aborted.get()
        };
        read.scan_projected_row_refs_unordered_each(collection, *options, layout.storage_fields(), &mut |id, version, values| {
            for runner in &mut runners {
                runner.push(id, version, values);
            }
            Ok(!runners[0].is_full() || flush(&mut runners))
        })
        .map_err(storage_engine_error)?;
        if !aborted.get() && !runners[0].is_empty() {
            flush(&mut runners);
        }
        if aborted.get() {
            return Ok(None);
        }
        let (rows, passed) = (rows.get(), [passed[0].get(), passed[1].get()]);
        PARTIAL_DOCUMENTS_USED.with(|used| used.set(true));
        let statistics = ExecutionStatistics::streamed_with_strategies(
            rows * 2,
            (rows - passed[0]) + (rows - passed[1]),
            passed[0] + passed[1],
            ExecutionStrategies::default().with(ExecutionStrategy::CollectionScan),
        );
        Ok(Some((passed[0] + passed[1], statistics)))
    }

    /// Runs the streaming prefix and the blocking core of a governed plan,
    /// emitting up to `shape.output_limit` rows to `visitor`.
    fn stream_governed_core( &self, physical: &PhysicalPlan, shape: &GovernedShape<'_>, memory: &QueryMemory, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<Option<ExecutionStatistics>> {
        let blocking_index = shape.blocking_index;
        let core = &physical.operators()[blocking_index..shape.core_end];
        let output_limit = shape.output_limit;

        let prefix = PhysicalPlan::new(
            physical.source().clone(),
            physical.operators()[..blocking_index].iter().cloned(),
        )
        .map_err(EngineError::physical_planning)?
        .with_source_access_negotiation(
            physical.source_access_vector(),
            physical.projected_prefix_len().min(blocking_index),
            physical.source_projection_reuse(),
        );
        // A core that does not keep its input order (sort, group, distinct,
        // pivot, sample, union), or a terminal count, lets the prefix read only
        // the fields the core and tail need, from an unordered cursor.
        let order_free = (shape.count_alias.is_some() && shape.rest.is_empty())
            || !matches!(core[0].execution_properties().order, crate::query::Order::Preserved);
        let consumer_demand = order_free
            .then(|| crate::query::source_field_demand(&physical.operators()[blocking_index..], None))
            .flatten();
        let prefix = if order_free { prefix.with_order_free_consumer(consumer_demand.clone()) } else { prefix };
        let source_strategy = match physical.source().access() {
            crate::query::PhysicalAccess::CollectionScan { .. } => {
                ExecutionStrategy::CollectionScan
            }
            crate::query::PhysicalAccess::PrimaryKeyLookup { .. } => {
                ExecutionStrategy::PrimaryKeyLookup
            }
        };

        // A stage that retains at most N rows (`sample N`) keeps a reservoir of
        // N rows while the prefix streams; memory is bounded by N.
        if let Some(capacity) = core[0].execution_properties().retained_rows() {
            let core_reads = core[0].field_reads().unwrap_or_default();
            let (sampling_prefix, late) = retaining_prefix(&prefix, core_reads.clone());
            // Rows read as slots: the reservoir keeps identities only, and the
            // retained rows are loaded and replayed through the prefix. The
            // reservoir keeps rows whatever their values, so the stages that
            // end the prefix and are proven total run on retained rows only.
            // A prefix whose rows already carry a field demand (a consumer
            // after the sample) replays as well: hydrated rows are complete.
            if late || prefix.operators().iter().all(replays_one_row) {
                let read = self.storage.read().map_err(storage_engine_error)?;
                let start = sample_commuting_start(read.as_ref(), &prefix)?;
                let plain_scan = matches!(prefix.source().access(), crate::query::PhysicalAccess::CollectionScan { options } if options.limit().is_none())
                    && !self.is_virtual(prefix.source().collection());
                if start == 0 && plain_scan {
                    // Nothing runs before the reservoir: draw by position when
                    // the storage can, from the primary index otherwise,
                    // reading only the retained rows.
                    let drawn = read.sample_row_ids(prefix.source().collection(), capacity, crate::query::sample_seed()).map_err(storage_engine_error)?;
                    let (seen, mut locators) = match drawn {
                        Some(drawn) => drawn,
                        None => {
                            let mut reservoir = crate::query::Reservoir::new(capacity);
                            read.visit_row_ids(prefix.source().collection(), &mut |id, version| {
                                reservoir.push((id, version));
                                Ok(true)
                            })
                            .map_err(storage_engine_error)?;
                            (reservoir.seen(), reservoir.into_items())
                        }
                    };
                    drop(read);
                    let dropped = seen.saturating_sub(capacity as u64);
                    if let Some(limit) = output_limit {
                        locators.truncate(limit);
                    }
                    let rows = self.hydrate_replayed(&prefix, locators)?;
                    let returned = visit_all(visitor, rows)?;
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        seen,
                        dropped,
                        returned,
                        ExecutionStrategies::default().with(source_strategy),
                    )));
                }
                drop(read);
                let sampled_prefix = if start < prefix.operators().len() {
                    PhysicalPlan::new(prefix.source().clone(), prefix.operators()[..start].iter().cloned())
                        .map_err(|error| EngineError::execution(ExecutionError::evaluation(error.to_string())))?
                } else {
                    prefix.clone()
                };
                let reservoir = std::cell::RefCell::new(crate::query::Reservoir::new(capacity));
                if let Some(prefix_stats) = self.stream_slot_batches(
                    &sampled_prefix,
                    &core_reads,
                    &[],
                    true,
                    &mut |runner, _| {
                        let mut reservoir = reservoir.borrow_mut();
                        for &row in runner.selection() {
                            reservoir.push(runner.id(row as usize));
                        }
                        Ok(())
                    },
                    &mut |row| {
                        reservoir.borrow_mut().push((row.id, row.version));
                        Ok(())
                    },
                )? {
                    let reservoir = reservoir.into_inner();
                    let dropped = reservoir.seen().saturating_sub(capacity as u64);
                    let mut locators = reservoir.into_items();
                    if let Some(limit) = output_limit {
                        locators.truncate(limit);
                    }
                    let rows = self.hydrate_replayed(&prefix, locators)?;
                    let returned = visit_all(visitor, rows)?;
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        prefix_stats.scanned(),
                        prefix_stats.filtered().saturating_add(dropped),
                        returned,
                        ExecutionStrategies::default().with(source_strategy),
                    )));
                }
            }
            let mut reservoir = crate::query::Reservoir::new(capacity);
            let prefix_stats = self
                .stream_read_pipeline(&sampling_prefix, &mut |stored| {
                    reservoir.push(ExecutionRow::from_stored(stored));
                    Ok(())
                })?
                .expect("validated streaming prefix");
            let dropped = reservoir.seen().saturating_sub(capacity as u64);
            let mut rows = reservoir.into_items();
            if let Some(limit) = output_limit {
                rows.truncate(limit);
            }
            if late && prefix_stats.strategies().contains(ExecutionStrategy::PartialDocuments) {
                rows = self.hydrate_replayed_rows(&prefix, rows)?;
            }
            let returned = visit_all(visitor, rows)?;
            return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                prefix_stats.scanned(),
                prefix_stats.filtered().saturating_add(dropped),
                returned,
                ExecutionStrategies::default().with(source_strategy),
            )));
        }

        // UNION and LOOKUP are bounded at the compound-stage boundary. UNION
        // streams both branches directly; LOOKUP only retains the matches for
        // the current outer row, which is the smallest semantic unit that must
        // exist in memory because the runtime attaches it as one result value.
        // PIVOT keeps one aggregate cell per (row key, column) while the
        // prefix streams; only that state, never the rows, is retained.
        if let [PhysicalOperator::Pivot { specification }] = core {
            let mut accumulator = self
                .runtime
                .incremental_pivot_accumulator(specification)
                .map_err(EngineError::execution)?;
            let (stage, runs) = (memory.stage(), std::cell::RefCell::new(Vec::new()));
            let over_budget = |accumulator: &mut dyn crate::query::IncrementalPivotAccumulator| spill_pivot(&stage, accumulator, &mut runs.borrow_mut());
            // Rows reach the accumulator as batches or slots when it folds
            // them; documents otherwise.
            let pivot_fields = specification
                .rows()
                .iter()
                .chain(specification.columns())
                .chain(specification.values().iter().map(|value| value.field()))
                .cloned()
                .collect::<Vec<_>>();
            let slot_stats = if accumulator.folds_refs() {
                // Batches: each pivot path is read from its output column.
                let accumulator = std::cell::RefCell::new(&mut accumulator);
                let groups = [specification.rows(), specification.columns()];
                let position = |fields: &[crate::query::ExpressionFieldPath], path: &crate::query::ExpressionFieldPath| fields.iter().position(|field| field == path);
                let keyed = specification.rows().iter().chain(specification.columns()).cloned().collect::<Vec<_>>();
                self.stream_slot_batches(
                    &prefix,
                    &pivot_fields,
                    &keyed,
                    false,
                    &mut |runner, fields| {
                        let columns = |paths: &[crate::query::ExpressionFieldPath]| paths.iter().map(|path| position(fields, path).and_then(|index| runner.output(index))).collect::<Vec<_>>();
                        let (rows, cols) = (columns(groups[0]), columns(groups[1]));
                        let values = specification.values().iter().map(|value| position(fields, value.field()).and_then(|index| runner.output(index))).collect::<Vec<_>>();
                        fn read<'c>(columns: &[Option<&'c crate::query::Column>], row: usize, out: &mut Vec<Option<ProjectedValueRef<'c>>>) {
                            out.clear();
                            out.extend(columns.iter().map(|column| column.and_then(|column| column.value_ref(row))));
                        }
                        let (mut row_refs, mut column_refs, mut value_refs) = (Vec::new(), Vec::new(), Vec::new());
                        let mut accumulator = accumulator.borrow_mut();
                        if accumulator.push_batch(&rows, &cols, &values, runner.selection()).map_err(EngineError::execution)? {
                            return over_budget(accumulator.as_mut());
                        }
                        for &row in runner.selection() {
                            let row = row as usize;
                            read(&rows, row, &mut row_refs);
                            read(&cols, row, &mut column_refs);
                            read(&values, row, &mut value_refs);
                            accumulator.push_refs(&row_refs, &column_refs, &value_refs).map_err(EngineError::execution)?;
                        }
                        over_budget(accumulator.as_mut())
                    },
                    &mut |row| {
                        let mut accumulator = accumulator.borrow_mut();
                        accumulator.push_slots(row.fields, row.values).map_err(EngineError::execution)?;
                        over_budget(accumulator.as_mut())
                    },
                )?
            } else if accumulator.folds_slots() {
                self.stream_slot_rows(&prefix, &pivot_fields, &mut |row| {
                    accumulator.push_slots(row.fields, row.values).map_err(EngineError::execution)?;
                    over_budget(accumulator.as_mut())
                })?
            } else {
                None
            };
            let prefix_stats = slot_stats.map_or_else(
                || {
                    self.stream_read_pipeline(&prefix, &mut |stored| {
                        accumulator.push(stored.document()).map_err(EngineError::execution)?;
                        over_budget(accumulator.as_mut())
                    })
                    .map(|statistics| statistics.expect("validated streaming prefix"))
                },
                Ok,
            )?;
            let runs = runs.into_inner();
            let returned = finish_pivot(accumulator, &runs, visitor)?;
            drop(stage);
            return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                prefix_stats.scanned(),
                prefix_stats.filtered(),
                returned,
                ExecutionStrategies::default()
                    .with(source_strategy)
                    .with(ExecutionStrategy::InMemoryPivot),
            )));
        }

        if let [core_operator] = core {
            match core_operator {
                PhysicalOperator::Union { collection, alias, pipeline } => {
                    let mut returned = 0u64;
                    let prefix_stats = self
                        .stream_read_pipeline(&prefix, &mut |stored| {
                            visitor(ExecutionRow::from_stored(stored))?;
                            returned = returned.saturating_add(1);
                            Ok(())
                        })?
                        .expect("validated streaming prefix");
                    // The branch rows meet the same consumer as the main rows. An
                    // aliased branch may name fields through the alias: read whole rows.
                    let branch_demand = (order_free && alias.is_none() && pipeline.operators().iter().all(is_row_operator))
                        .then(|| crate::query::source_field_demand(pipeline.operators(), consumer_demand.as_deref()))
                        .flatten();
                    let mut emit_branch = |row: ExecutionRow| {
                        visitor(row)?;
                        returned = returned.saturating_add(1);
                        Ok(())
                    };
                    let batched = match branch_demand {
                        Some(_) => self.stream_union_batches(collection, pipeline, consumer_demand.as_deref(), &mut emit_branch)?,
                        None => None,
                    };
                    let branch_stats = match batched {
                        Some(statistics) => statistics,
                        None => self.stream_bounded_secondary_pipeline(collection, pipeline, None, branch_demand.as_deref(), &mut emit_branch)?,
                    };
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
                PhysicalOperator::Lookup { pipeline, .. } if sinks::runs_lookup_pipeline(pipeline) => {
                    let mut join = sinks::LookupJoin::new(core_operator, memory).expect("a lookup operator");
                    let mut returned = 0u64;
                    let mut join_row = |join: &mut sinks::LookupJoin<'_, '_>, stored: StoredDocument, filter: Option<&LookupKeyFilter>| -> EngineResult<()> {
                        visitor(join.join(self, ExecutionRow::from_stored(stored), filter)?)?;
                        returned = returned.saturating_add(1);
                        Ok(())
                    };
                    // Semi-join: a bounded outer side is read first, and the
                    // index keeps only joined rows that can match its keys.
                    let semi_join = join.equality().is_some_and(|equality| !equality.prefix && !equality.composite()) && prefix_row_bound(&prefix).is_some_and(|bound| bound <= SEMI_JOIN_MAX_OUTER_ROWS);
                    let prefix_stats = if semi_join {
                        let mut outer_rows = Vec::new();
                        let prefix_stats = self
                            .stream_read_pipeline(&prefix, &mut |stored| {
                                outer_rows.push(stored);
                                Ok(())
                            })?;
                        let equality = join.equality().expect("semi-join has an equality");
                        let mut filter = Some(LookupKeyFilter::default());
                        let no_inner = Document::new();
                        for stored in &outer_rows {
                            let fields = crate::query::LookupFields { inner: &no_inner, inner_alias: join.alias(), outer: stored.document(), outer_alias: join.outer_alias() };
                            match crate::query::ExpressionFieldResolver::resolve_field(&fields, equality.outer()) {
                                crate::query::SemanticValue::Present(value) => {
                                    if let Some(filter) = &mut filter {
                                        filter.insert(&value);
                                    }
                                }
                                // Every joined row is a candidate for a missing key.
                                crate::query::SemanticValue::Missing => filter = None,
                            }
                        }
                        for stored in outer_rows {
                            join_row(&mut join, stored, filter.as_ref())?;
                        }
                        prefix_stats
                    } else {
                        self.stream_read_pipeline(&prefix, &mut |stored| join_row(&mut join, stored, None))?
                    }
                        .expect("validated streaming prefix");
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        prefix_stats.scanned().saturating_add(join.scanned),
                        prefix_stats.filtered().saturating_add(join.filtered),
                        returned,
                        join.strategies(ExecutionStrategies::default().with(source_strategy)),
                    )));
                }
                _ => {}
            }
        }

        if let [PhysicalOperator::Sort { keys: sort_keys }, PhysicalOperator::Distinct {
            fields: distinct_fields,
        }] = core
        {
            let Some(limit) = output_limit else {
                return Ok(None);
            };
            let stage = memory.stage();
            let spill = SpillEngine::default();
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
                        &stage,
                        &mut chunk,
                        &mut chunk_bytes,
                        &mut runs,
                        "one sort-distinct row exceeds the governed query working set",
                    )
                })?
                .expect("validated streaming prefix");

            // Emitted rows pass to the stages after the core, which count them.
            let returned = if runs.is_empty() {
                stable_sort(self.runtime.as_ref(), sort_keys, &mut chunk)
                    .map_err(EngineError::execution)?;
                stage.release();
                emit_distinct_limited_rows(
                    self.runtime.as_ref(),
                    distinct_fields,
                    limit,
                    chunk,
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
                stage.release();
                merge_sorted_distinct_limited_runs(
                    self.runtime.as_ref(),
                    sort_keys,
                    distinct_fields,
                    &runs,
                    limit,
                    visitor,
                )?
            };
            drop(stage);
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

        if let [PhysicalOperator::Group { keys: group_keys, .. }, PhysicalOperator::Sort { keys: sort_keys }] = core {
            // Reuse the same governed in-memory group primitive used by GROUP
            // alone before choosing the external path. Chaining SORT/LIMIT must
            // not force spill when the grouped state already fits the contract.
            let group_memory = memory.stage();
            if let Some((mut grouped_rows, prefix_stats)) = self.try_in_memory_incremental_group(&prefix, group_keys, &group_memory, None, Some((&sort_keys[..], output_limit)))? {
                // Groups come sorted, and bounded, when the sort reads only
                // their own fields; else their rows are sorted here.
                let rows = if grouped_rows.sorted {
                    None
                } else if let Some(limit) = output_limit {
                    let mut top = BoundedTopN::new(limit);
                    for row in grouped_rows.by_ref() {
                        top.push_lazy(self.runtime.as_ref(), sort_keys, row?, execution_row_working_bytes).map_err(EngineError::execution)?;
                    }
                    Some(top.into_sorted_rows(self.runtime.as_ref(), sort_keys).map_err(EngineError::execution)?)
                } else {
                    let mut rows = grouped_rows.by_ref().collect::<EngineResult<Vec<_>>>()?;
                    stable_sort(self.runtime.as_ref(), sort_keys, &mut rows).map_err(EngineError::execution)?;
                    Some(rows)
                };
                group_memory.release();
                let returned = match rows {
                    Some(rows) => visit_all(visitor, rows)?,
                    None => visit_each(visitor, grouped_rows)?,
                };
                let strategies = ExecutionStrategies::default().with(source_strategy).with(ExecutionStrategy::InMemoryGroup).with(if output_limit.is_some() { ExecutionStrategy::TopN } else { ExecutionStrategy::InMemorySort });
                return Ok(Some(streamed_after(&prefix_stats, returned, strategies)));
            }

            let spill = SpillEngine::default();
            let (group_runs, prefix_stats) = self.external_group_runs(&prefix, group_keys, &group_memory, &spill)?;
            // The partial groups are spilled: the sort takes their memory.
            drop(group_memory);
            let sort_memory = memory.stage();
            let (returned, sort_strategy) = if let Some(limit) = output_limit {
                let mut top = BoundedTopN::new(limit);
                merge_group_runs(
                    self.runtime.as_ref(),
                    group_keys,
                    &group_runs,
                    None,
                    &mut |row| {
                        top.push_lazy(self.runtime.as_ref(), sort_keys, row, |candidate| {
                            execution_row_working_bytes(candidate)
                        })
                        .map_err(EngineError::execution)?;
                        if !sort_memory.holds(top.live_bytes()) {
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
                sort_memory.release();
                let returned = visit_all(visitor, top)?;
                (returned, ExecutionStrategy::TopN)
            } else {
                let mut sort_chunk = Vec::<ExecutionRow>::new();
                let mut sort_chunk_bytes = 0usize;
                let mut sort_runs = Vec::<SpillRun>::new();
                merge_group_runs(
                    self.runtime.as_ref(),
                    group_keys,
                    &group_runs,
                    None,
                    &mut |row| {
                        push_bounded_sort_row(
                            self.runtime.as_ref(),
                            sort_keys,
                            &spill,
                            row,
                            &sort_memory,
                            &mut sort_chunk,
                            &mut sort_chunk_bytes,
                            &mut sort_runs,
                            "one grouped row exceeds the governed sort working set",
                        )
                    },
                )?;
                sort_memory.release();
                if sort_runs.is_empty() {
                    stable_sort(self.runtime.as_ref(), sort_keys, &mut sort_chunk)
                        .map_err(EngineError::execution)?;
                    let returned = visit_all(visitor, sort_chunk)?;
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
            drop(sort_memory);
            let strategies = ExecutionStrategies::default()
                .with(source_strategy)
                .with(ExecutionStrategy::ExternalGroup)
                .with(sort_strategy);
            return Ok(Some(streamed_after(&prefix_stats, returned, strategies)));
        }

        match &core[0] {
            PhysicalOperator::Sort { keys } if output_limit.is_some() => {
                let limit = output_limit.expect("checked Top-N limit");
                if let Some((rows, statistics)) = self.try_slot_top_n(&prefix, keys, limit)? {
                    for row in rows {
                        visitor(row)?;
                    }
                    return Ok(Some(statistics));
                }
                let stage = memory.stage();
                // Late materialization: when the prefix can be replayed on one
                // row, rank rows read as partial documents holding only what the
                // prefix and the sort keys read, then hydrate the winners.
                let (ranking_prefix, late) =
                    retaining_prefix(&prefix, keys.iter().map(|key| key.field().clone()).collect());
                let mut top = BoundedTopN::new(limit);
                let prefix_stats = self
                    .stream_read_pipeline(&ranking_prefix, &mut |stored| {
                        let row = ExecutionRow::from_stored(stored);
                        top.push_lazy(self.runtime.as_ref(), keys, row, |candidate| {
                            execution_row_working_bytes(candidate)
                        })
                        .map_err(EngineError::execution)?;
                        if !stage.holds(top.live_bytes()) {
                            return Err(EngineError::execution(ExecutionError::evaluation(
                                "top-n candidates exceed the governed query working set",
                            )));
                        }
                        Ok(())
                    })?
                    .expect("validated streaming prefix");
                drop(stage);

                let mut top = top
                    .into_sorted_rows(self.runtime.as_ref(), keys)
                    .map_err(EngineError::execution)?;
                if late && prefix_stats.strategies().contains(ExecutionStrategy::PartialDocuments) {
                    top = self.hydrate_replayed_rows(&prefix, top)?;
                }
                let returned = visit_all(visitor, top)?;
                let strategies = prefix_stats.strategies().with(source_strategy).with(ExecutionStrategy::TopN);
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
                let stage = memory.stage();
                // A consumer that declares the fields it reads gets rows
                // holding them, as the prefix reads them: never reloaded
                // whole.
                let consumer_reads = prefix.field_demand().is_some();
                if let Some(statistics) = self.try_slot_sort(&prefix, keys, &stage, visitor)? {
                    return Ok(Some(statistics));
                }

                let spill = SpillEngine::default();
                let late_materialize = !consumer_reads
                    && prefix
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
                            &stage,
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
                    // Emitted rows pass to the stages after the core, which
                    // count them.
                    stage.release();
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
                    stage.release();
                    returned =
                        merge_sorted_runs(self.runtime.as_ref(), keys, &runs, None, &mut emit)?;
                }
                drop(stage);
                let strategies =
                    ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(if runs.is_empty() {
                            ExecutionStrategy::InMemorySort
                        } else {
                            ExecutionStrategy::ExternalSort
                        });
                return Ok(Some(streamed_after(&prefix_stats, returned, strategies)));
            }
            PhysicalOperator::Distinct { fields } => {
                let stage = memory.stage();
                let rows_unused = shape.tail.is_empty() && shape.rest.is_empty() && shape.count_alias.is_some();
                // A bound keeps only its frontier of smallest keys: first for
                // a key of several fields, whose distinct tuples would
                // otherwise all be built; a one-field key finds its values
                // per dictionary code, faster.
                let limited = |visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>| output_limit.filter(|limit| *limit > 0).map(|limit| self.try_batch_distinct_limited(&prefix, fields, limit, visitor)).transpose().map(Option::flatten);
                if fields.len() > 1 {
                    if let Some(statistics) = limited(visitor)? {
                        return Ok(Some(statistics));
                    }
                }
                if let Some(statistics) = self.try_batch_distinct(&prefix, fields, &stage, output_limit, rows_unused, visitor)? {
                    return Ok(Some(statistics));
                }
                if let Some(statistics) = limited(visitor)? {
                    return Ok(Some(statistics));
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
                            .saturating_add(execution_row_encoded_len(&row))
                            .saturating_add(72);
                        if !stage.holds(estimated) {
                            return Err(EngineError::execution(ExecutionError::evaluation(
                                "one distinct row exceeds the governed query working set",
                            )));
                        }
                        if !chunk.is_empty()
                            && !stage.fits(chunk_bytes.saturating_add(estimated))
                        {
                            flush_keyed_run(&spill, &mut chunk, &mut runs)?;
                            chunk_bytes = 0;
                        }
                        chunk_bytes = chunk_bytes.saturating_add(estimated);
                        chunk.push(KeyedRow { key, row });
                        Ok(())
                    })?
                    .expect("validated streaming prefix");
                // Rows that fit the working set are deduplicated in memory,
                // exactly as one spilled run would be.
                let (returned, strategy) = if runs.is_empty() {
                    chunk.sort_by(|left, right| left.key.cmp(&right.key));
                    chunk.dedup_by(|later, earlier| later.key == earlier.key);
                    let rows = chunk.into_iter().take(output_limit.unwrap_or(usize::MAX)).map(|keyed| keyed.row).collect();
                    stage.release();
                    (visit_all(visitor, rows)?, ExecutionStrategy::InMemoryDistinct)
                } else {
                    if !chunk.is_empty() {
                        flush_keyed_run(&spill, &mut chunk, &mut runs)?;
                    }
                    stage.release();
                    (merge_distinct_runs(&runs, output_limit, visitor)?, ExecutionStrategy::ExternalDistinct)
                };
                drop(stage);
                let strategies = ExecutionStrategies::default()
                    .with(source_strategy)
                    .with(strategy);
                return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                    prefix_stats.scanned(),
                    prefix_stats
                        .filtered()
                        .saturating_add(prefix_stats.returned().saturating_sub(returned)),
                    returned,
                    strategies,
                )));
            }
            PhysicalOperator::Group { keys, .. } => {
                if output_limit == Some(0) {
                    let strategies = ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(ExecutionStrategy::InMemoryGroup);
                    return Ok(Some(ExecutionStatistics::streamed_with_strategies(
                        0, 0, 0, strategies,
                    )));
                }
                let stage = memory.stage();
                if let Some((grouped, prefix_stats)) = self.try_in_memory_incremental_group(
                    &prefix,
                    keys,
                    &stage,
                    output_limit,
                    None,
                )? {
                    // Emitted groups pass to the stages after the core, which
                    // count them.
                    stage.release();
                    let returned = visit_each(visitor, grouped.take(output_limit.unwrap_or(usize::MAX)))?;
                    let strategies = ExecutionStrategies::default()
                        .with(source_strategy)
                        .with(ExecutionStrategy::InMemoryGroup);
                    return Ok(Some(streamed_after(&prefix_stats, returned, strategies)));
                }

                let spill = SpillEngine::default();
                let (runs, prefix_stats) = self.external_group_runs(&prefix, keys, &stage, &spill)?;
                drop(stage);
                let returned = merge_group_runs(
                    self.runtime.as_ref(),
                    keys,
                    &runs,
                    output_limit,
                    visitor,
                )?;
                let strategies = ExecutionStrategies::default()
                    .with(source_strategy)
                    .with(ExecutionStrategy::ExternalGroup);
                return Ok(Some(streamed_after(&prefix_stats, returned, strategies)));
            }
            _ => {}
        }

        Ok(None)
    }

    /// Whether `collection` is a system collection built on demand (`_memory`, …).
    fn is_virtual(&self, collection: &CollectionId) -> bool {
        !self.bound && matches!(collection.as_str(), vcollections::INDEX_OBSERVATIONS | vcollections::MEMORY | vcollections::MEMORY_EVENTS | vcollections::QUERY_MEMORY)
    }

    /// The storage holding the system collection `physical` reads, built now.
    fn system_collection_storage( &self, physical: &PhysicalPlan, ) -> EngineResult<Option<MemoryStorage>> {
        let collection = physical.source().collection();
        if !self.is_virtual(collection) {
            return Ok(None);
        }
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
        let observed_bytes = snapshot.classes.iter().fold(0usize, |total, class| total.saturating_add(class.observed_bytes));
        let event_snapshot = self.memory_governor.event_snapshot();
        let spill = crate::spill::stats_snapshot();
        let process = snapshot.process;
        let profile = self.memory_governor.profile();
        let (pressure_state, soft_limit_bytes, hard_limit_bytes) = match self.memory_governor.process_pressure() {
            ProcessMemoryPressure::Unlimited => ("unlimited", None, None),
            ProcessMemoryPressure::Unavailable { limit_bytes } => ("unavailable", None, Some(limit_bytes)),
            ProcessMemoryPressure::Normal { soft_limit_bytes, hard_limit_bytes, .. } => ("normal", Some(soft_limit_bytes), Some(hard_limit_bytes)),
            ProcessMemoryPressure::Soft { soft_limit_bytes, hard_limit_bytes, .. } => ("soft", Some(soft_limit_bytes), Some(hard_limit_bytes)),
            ProcessMemoryPressure::Hard { soft_limit_bytes, hard_limit_bytes, .. } => ("hard", Some(soft_limit_bytes), Some(hard_limit_bytes)),
        };
        let pressure_bytes = process.map(|p| p.pressure_bytes(snapshot.current_bytes));
        let over_limit = |bytes: Option<usize>, limit: Option<usize>, headroom: bool| match (bytes, limit) {
            (Some(bytes), Some(limit)) => Some(if headroom { limit.saturating_sub(bytes) } else { bytes.saturating_sub(limit) }),
            _ => None,
        };
        let mut transaction = storage.begin().map_err(storage_engine_error)?;

        let global = Document::from_fields([
            ("scope", Value::from("global")),
            ("class", Value::from("all")),
            ("profile", Value::from(profile.effective_profile_label())),
            ("base_profile", Value::from(profile.profile.as_str())),
            ("profile_scaled", Value::from(profile.is_scaled())),
            ("process_limit_bytes", optional_usize_value(profile.process_limit_bytes)),
            ("runtime_reserve_bytes", usize_value(profile.runtime_reserve_bytes)),
            ("managed_budget_bytes", optional_usize_value(profile.managed_budget_bytes)),
            ("limit_bytes", optional_usize_value(snapshot.limit_bytes)),
            ("current_bytes", usize_value(snapshot.current_bytes)),
            ("peak_bytes", usize_value(snapshot.peak_bytes)),
            ("observed_bytes", usize_value(observed_bytes)),
            ("available_bytes", optional_usize_value(snapshot.available_bytes)),
            ("active_reservations", usize_value(snapshot.active_reservations)),
            ("failed_reservations", Value::from(snapshot.failed_reservations)),
            ("event_capacity", usize_value(event_snapshot.capacity)),
            ("dropped_events", Value::from(event_snapshot.dropped_events)),
            ("pressure_state", Value::from(pressure_state)),
            ("soft_limit_bytes", optional_usize_value(soft_limit_bytes)),
            ("hard_limit_bytes", optional_usize_value(hard_limit_bytes)),
            ("rss_bytes", optional_usize_value(process.map(|p| p.rss_bytes))),
            ("anonymous_bytes", optional_usize_value(process.map(|p| p.anonymous_bytes))),
            ("non_anonymous_rss_bytes", optional_usize_value(process.map(super::memory::ProcessMemorySnapshot::non_anonymous_rss_bytes))),
            ("process_pressure_bytes", optional_usize_value(pressure_bytes)),
            ("unmanaged_bytes", optional_usize_value(process.map(|p| p.unmanaged_bytes))),
            ("memory_enforcement", Value::from("managed")),
            ("rss_enforced", Value::from(false)),
            ("process_headroom_bytes", optional_usize_value(over_limit(pressure_bytes, hard_limit_bytes, true))),
            ("process_pressure_over_limit_bytes", optional_usize_value(over_limit(pressure_bytes, hard_limit_bytes, false))),
            ("rss_over_limit_bytes", optional_usize_value(over_limit(process.map(|p| p.rss_bytes), hard_limit_bytes, false))),
            ("spill_runs_created", Value::from(spill.runs_created)),
            ("spill_runs_active", Value::from(spill.runs_active)),
            ("spill_runs_peak_active", Value::from(spill.runs_peak_active)),
            ("spill_runs_deleted", Value::from(spill.runs_deleted)),
            ("spill_records_written", Value::from(spill.records_written)),
            ("spill_payload_bytes_written", Value::from(spill.payload_bytes_written)),
            ("spill_framing_bytes_written", Value::from(spill.framing_bytes_written)),
            ("spill_bytes_written", Value::from(spill.payload_bytes_written.saturating_add(spill.framing_bytes_written))),
            ("spill_records_read", Value::from(spill.records_read)),
            ("spill_payload_bytes_read", Value::from(spill.payload_bytes_read)),
            ("spill_live_bytes", Value::from(spill.live_bytes)),
            ("spill_peak_live_bytes", Value::from(spill.peak_live_bytes)),
            ("spill_create_us", Value::from(spill.create_us)),
            ("spill_flush_us", Value::from(spill.flush_us)),
            ("spill_sync_us", Value::from(spill.sync_us)),
        ]);
        // Class rows share the global schema; fields without a per-class meaning stay null.
        let class_template = Document::from_fields(global.keys().map(|name| (name.clone(), Value::Null)));
        insert_virtual_document(transaction.as_mut(), &collection, DocumentId::synthetic(0x6d65_6d6f_7279, 0), global)?;

        for (ordinal, class) in snapshot.classes.into_iter().enumerate() {
            let mut document = class_template.clone();
            document.extend([
                ("scope", Value::from("class")),
                ("class", Value::from(class.class.as_str())),
                ("current_bytes", usize_value(class.current_bytes)),
                ("peak_bytes", usize_value(class.peak_bytes)),
                ("observed_bytes", usize_value(class.observed_bytes)),
                ("active_reservations", usize_value(class.active_reservations)),
                ("failed_reservations", Value::from(class.failed_reservations)),
            ]);
            insert_virtual_document(transaction.as_mut(), &collection, DocumentId::synthetic(0x6d65_6d63_6c61, usize_to_u64_saturating(ordinal)), document)?;
        }

        transaction.commit().map_err(storage_engine_error)?;
        Ok(storage)
    }

    fn query_memory_storage(&self) -> EngineResult<MemoryStorage> {
        let storage = MemoryStorage::new();
        let collection = CollectionId::parse(vcollections::QUERY_MEMORY).map_err(storage_engine_error)?;
        let snapshot = self.memory_governor.query_memory_snapshot();
        let mut transaction = storage.begin().map_err(storage_engine_error)?;
        let profile = if snapshot.profile_scaled {
            format!("custom (base: {})", snapshot.base_profile.as_str())
        } else {
            snapshot.base_profile.as_str().to_owned()
        };
        let global = Document::from_fields([
            ("scope", Value::from("global")),
            ("operation_id", Value::Null),
            ("class", Value::from("all")),
            ("profile", Value::from(profile)),
            ("base_profile", Value::from(snapshot.base_profile.as_str())),
            ("profile_scaled", Value::from(snapshot.profile_scaled)),
            ("process_limit_bytes", optional_usize_value(snapshot.process_limit_bytes)),
            ("runtime_reserve_bytes", usize_value(snapshot.runtime_reserve_bytes)),
            ("managed_budget_bytes", optional_usize_value(snapshot.managed_budget_bytes)),
            ("operation_budget_bytes", usize_value(snapshot.operation_budget_bytes)),
            ("active_heavy_operations", usize_value(snapshot.active_heavy_operations)),
            ("rejected_operations", Value::from(snapshot.rejected_operations)),
            ("running_operations", usize_value(snapshot.running_operations)),
            ("waiting_operations", usize_value(snapshot.waiting_operations)),
            ("query_slots", optional_usize_value((snapshot.query_slots != usize::MAX).then_some(snapshot.query_slots))),
        ]);
        // Operation rows share the global schema; fields without a per-operation meaning stay null.
        let operation_template = Document::from_fields(global.keys().map(|name| (name.clone(), Value::Null)));
        insert_virtual_document(transaction.as_mut(), &collection, DocumentId::synthetic(0x7175_6572_796d, 0), global)?;
        for record in snapshot.records {
            let mut document = operation_template.clone();
            document.extend([
                ("scope", Value::from("operation")),
                ("operation_id", Value::from(record.id)),
                ("class", Value::from(record.class.as_str())),
            ]);
            insert_virtual_document(transaction.as_mut(), &collection, DocumentId::synthetic(0x7175_6572_796f, record.id), document)?;
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
                ("bytes", usize_value(event.bytes)),
                ("current_bytes", usize_value(event.current_bytes)),
                ("limit_bytes", optional_usize_value(event.limit_bytes)),
                ("event_capacity", usize_value(snapshot.capacity)),
                ("dropped_events", Value::from(snapshot.dropped_events)),
            ]);
            insert_virtual_document(transaction.as_mut(), &collection, DocumentId::synthetic(0x6d65_6d65_766e, event.sequence), document)?;
        }

        transaction.commit().map_err(storage_engine_error)?;
        Ok(storage)
    }

    fn index_observations_storage(&self) -> EngineResult<MemoryStorage> {
        let storage = MemoryStorage::new();
        let collection = CollectionId::parse(vcollections::INDEX_OBSERVATIONS).map_err(storage_engine_error)?;
        let snapshot = self.indexing.snapshot();
        let dropped_full = snapshot.dropped_full;
        let dropped_disconnected = snapshot.dropped_disconnected;
        let mut transaction = storage.begin().map_err(storage_engine_error)?;

        let mut observations = snapshot.queries.into_iter().collect::<Vec<_>>();
        observations.sort_by_key(|(fingerprint, _)| fingerprint.as_u64());

        for (ordinal, (fingerprint, aggregate)) in observations.into_iter().enumerate() {
            let access = match aggregate.access {
                ObservedAccess::CollectionScan => "collection_scan",
                ObservedAccess::PrimaryKeyLookup => "primary_key_lookup",
            };
            let average_elapsed_us = aggregate.elapsed_micros.checked_div(aggregate.executions).unwrap_or(0);
            let document = Document::from_fields([
                ("fingerprint", Value::from(format!("{:016x}", fingerprint.as_u64()))),
                ("collection", Value::from(aggregate.collection.as_str())),
                ("access", Value::from(access)),
                ("executions", Value::from(aggregate.executions)),
                ("scanned", Value::from(aggregate.scanned)),
                ("returned", Value::from(aggregate.returned)),
                ("elapsed_us", Value::from(aggregate.elapsed_micros)),
                ("average_elapsed_us", Value::from(average_elapsed_us)),
                ("dropped_full", Value::from(dropped_full)),
                ("dropped_disconnected", Value::from(dropped_disconnected)),
            ]);
            insert_virtual_document(transaction.as_mut(), &collection, DocumentId::synthetic(fingerprint.as_u64(), usize_to_u64_saturating(ordinal)), document)?;
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

/// A temporal plan may only write through a terminal `into` (a restore into
/// another collection); every other stage must read.
fn temporal_plan_writes_allowed(physical: &PhysicalPlan) -> bool {
    if !physical.is_write() {
        return true;
    }
    let operators = physical.operators();
    let Some((PhysicalOperator::Custom { stage: Some(crate::query::Stage::Into), .. }, prefix)) = operators.split_last() else {
        return false;
    };
    PhysicalPlan::new(physical.source().clone(), prefix.iter().cloned()).is_ok_and(|plan| !plan.is_write())
}

fn usize_value(value: usize) -> Value {
    Value::from(usize_to_u64_saturating(value))
}

fn optional_usize_value(value: Option<usize>) -> Value {
    value.map_or(Value::Null, usize_value)
}

fn insert_virtual_document( transaction: &mut dyn StorageTransaction, collection: &CollectionId, id: DocumentId, document: Document, ) -> EngineResult<()> {
    transaction.insert(collection, id, Arc::new(document)).map(drop).map_err(storage_engine_error)
}

impl fmt::Debug for Engine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Engine")
            .field("planner", &self.planner)
            .finish_non_exhaustive()
    }
}

/// One-line, human-readable summary of an operator's arguments for `explain`.
fn describe_operator(operator: &PhysicalOperator) -> String {
    let joined = |fields: &[crate::query::ExpressionFieldPath]| {
        fields.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
    };
    match operator {
        PhysicalOperator::Filter { predicate } => predicate.to_string(),
        PhysicalOperator::Set { assignments } => assignments
            .iter()
            .map(|assignment| format!("{} = {}", assignment.field(), assignment.value()))
            .collect::<Vec<_>>()
            .join(", "),
        PhysicalOperator::Lookup { collection, alias, into, pipeline, .. } => format!(
            "{}{} into {into} ({} nested stage(s))",
            collection.as_str(),
            alias.as_ref().map(|alias| format!(" as {alias}")).unwrap_or_default(),
            pipeline.operators().len(),
        ),
        PhysicalOperator::Union { collection, pipeline, .. } => {
            format!("{} ({} nested stage(s))", collection.as_str(), pipeline.operators().len())
        }
        PhysicalOperator::Load { target } => target.to_string(),
        PhysicalOperator::StreamingLoad { mode, chunks } => format!("{mode}, {} chunk(s)", chunks.len()),
        PhysicalOperator::Limit { count } | PhysicalOperator::Skip { count } => count.to_string(),
        PhysicalOperator::Sort { keys } => keys
            .iter()
            .map(|key| format!("{} {}", key.field(), key.direction().as_str()))
            .collect::<Vec<_>>()
            .join(", "),
        PhysicalOperator::Select { fields } | PhysicalOperator::Distinct { fields } => joined(fields),
        PhysicalOperator::Group { keys, .. } => crate::query::describe_group_fields(keys),
        PhysicalOperator::Count { alias } => alias.to_string(),
        PhysicalOperator::Custom { arguments, .. } => arguments.to_string(),
        PhysicalOperator::Delete | PhysicalOperator::Insert { .. } | PhysicalOperator::Pivot { .. } => String::new(),
    }
}

/// One planned query.
#[derive(Clone, Debug)]
pub struct PlannedQuery {
    physical: PhysicalPlan,
}

impl PlannedQuery {
    #[must_use]
    #[inline]
    pub const fn new(physical: PhysicalPlan) -> Self {
        Self { physical }
    }

    /// Returns the executable plan.
    #[must_use]
    pub const fn physical(&self) -> &PhysicalPlan {
        &self.physical
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

/// Hands every row to `visitor`; returns how many it received.
fn visit_all(visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, rows: Vec<ExecutionRow>) -> EngineResult<u64> {
    visit_each(visitor, rows.into_iter().map(Ok))
}

/// [`visit_all`] for rows built as they are taken.
fn visit_each(visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, rows: impl Iterator<Item = EngineResult<ExecutionRow>>) -> EngineResult<u64> {
    rows.map(|row| visitor(row?)).try_fold(0u64, |returned, visited| visited.map(|()| returned + 1))
}

/// Statistics of a stage that returned `returned` rows from what its
/// prefix scanned and filtered.
fn streamed_after(prefix: &ExecutionStatistics, returned: u64, strategies: ExecutionStrategies) -> ExecutionStatistics {
    ExecutionStatistics::streamed_with_strategies(prefix.scanned(), prefix.filtered(), returned, strategies)
}

fn emit_streaming_count( visitor: &mut dyn FnMut(StoredDocument) -> EngineResult<()>, alias: &str, count: u64, ) -> EngineResult<()> {
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
fn push_bounded_sort_row( runtime: &QueryRuntime, keys: &[SortKey], spill: &SpillEngine, row: ExecutionRow, memory: &StageMemory<'_>, chunk: &mut Vec<ExecutionRow>, chunk_bytes: &mut usize, runs: &mut Vec<SpillRun>, oversized_message: &'static str, ) -> EngineResult<()> {
    // As much again is kept for the scratch buffer of the stable sort: every
    // run is sorted within the stage's memory.
    let estimated = execution_row_working_bytes(&row).map_err(EngineError::execution)?;
    if !chunk.is_empty() && !memory.fits(chunk_bytes.saturating_add(estimated).saturating_mul(2)) {
        flush_sorted_run(runtime, keys, spill, chunk, runs).map_err(EngineError::execution)?;
        *chunk_bytes = 0;
    }
    if !memory.fits(estimated.saturating_mul(2)) {
        return Err(EngineError::execution(ExecutionError::evaluation(
            oversized_message,
        )));
    }
    *chunk_bytes = chunk_bytes.saturating_add(estimated);
    chunk.push(row);
    Ok(())
}

fn flush_sorted_run(
    runtime: &QueryRuntime,
    keys: &[crate::query::SortKey],
    spill: &SpillEngine,
    chunk: &mut Vec<ExecutionRow>,
    runs: &mut Vec<SpillRun>,
) -> Result<(), ExecutionError> {
    stable_sort(runtime, keys, chunk)?;
    let mut writer = spill.create_run().map_err(spill_engine_error)?;
    let mut encoded = Vec::new();
    for row in chunk.iter() {
        encode_execution_row_into(row, &mut encoded)?;
        writer.append(&encoded).map_err(spill_engine_error)?;
    }
    runs.push(writer.finish().map_err(spill_engine_error)?);
    chunk.clear();
    Ok(())
}

fn emit_distinct_limited_rows( runtime: &QueryRuntime, fields: &[crate::query::ExpressionFieldPath], limit: usize, rows: impl IntoIterator<Item = ExecutionRow>, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<u64> {
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

fn merge_sorted_distinct_limited_runs( runtime: &QueryRuntime, sort_keys: &[crate::query::SortKey], distinct_fields: &[crate::query::ExpressionFieldPath], runs: &[SpillRun], limit: usize, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<u64> {
    if limit == 0 {
        return Ok(0);
    }
    let mut merge = merge_runs(runs, SpillRun::reader, read_spilled_row)?;
    let mut seen = Vec::<Arc<[u8]>>::with_capacity(limit);
    let mut returned = 0u64;
    while let Some(row) = merge
        .pop_min(|left, right| row_before(runtime, sort_keys, left, right))
        .map_err(EngineError::execution)?
    {
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

fn merge_sorted_runs( runtime: &QueryRuntime, keys: &[crate::query::SortKey], runs: &[SpillRun], limit: Option<usize>, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<u64> {
    let mut merge = merge_runs(runs, SpillRun::reader, read_spilled_row)?;
    let mut returned = 0u64;
    while limit.is_none_or(|limit| (returned as usize) < limit) {
        let Some(row) = merge
            .pop_min(|left, right| row_before(runtime, keys, left, right))
            .map_err(EngineError::execution)?
        else {
            break;
        };
        visitor(row)?;
        returned = returned.saturating_add(1);
    }
    Ok(returned)
}

/// A merge of spill runs whose records are read with a plain function.
type SpillMerge<R, T> = RunMerge<R, T, fn(&mut R) -> Result<Option<T>, ExecutionError>>;

fn merge_runs<R, T>( runs: &[SpillRun], open: fn(&SpillRun) -> io::Result<R>, next: fn(&mut R) -> Result<Option<T>, ExecutionError>, ) -> EngineResult<SpillMerge<R, T>> {
    let readers = runs
        .iter()
        .map(open)
        .collect::<io::Result<Vec<_>>>()
        .map_err(engine_spill_error)?;
    RunMerge::new(readers, next).map_err(EngineError::execution)
}

/// Whether `left` sorts strictly before `right` on `keys`.
fn row_before( runtime: &QueryRuntime, keys: &[crate::query::SortKey], left: &ExecutionRow, right: &ExecutionRow, ) -> Result<bool, ExecutionError> {
    Ok(runtime.compare_documents(keys, left.document(), right.document())? == Ordering::Less)
}

fn read_spilled_row(reader: &mut SpillRunReader) -> Result<Option<ExecutionRow>, ExecutionError> {
    reader
        .next_record()
        .map_err(spill_engine_error)?
        .map(|bytes| decode_execution_row(&bytes))
        .transpose()
}

fn flush_partial_group_run( _runtime: &QueryRuntime, _keys: &[crate::query::ExpressionFieldPath], spill: &SpillEngine, groups: &mut HashMap<Arc<[u8]>, Box<dyn crate::query::IncrementalGroupAccumulator>>, runs: &mut Vec<SpillRun>, ) -> EngineResult<()> {
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
    chunk.sort_by(|left, right| left.key.cmp(&right.key));
    let mut writer = spill.create_run().map_err(engine_spill_error)?;
    // Group runs are block-framed and prefix-compressed. This amortizes the
    // generic spill engine's 8-byte framing across many groups and avoids
    // rewriting common key prefixes for high-cardinality sorted runs.
    let mut block = Vec::with_capacity(GROUP_SPILL_BLOCK_BYTES);
    let mut previous_key = Vec::<u8>::new();
    let mut payload_scratch = Vec::<u8>::new();
    for record in &chunk {
        append_group_spill_block_record(
            &mut block,
            &mut previous_key,
            &mut payload_scratch,
            record,
        )
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
    runs.push(writer.finish().map_err(engine_spill_error)?);
    Ok(())
}

const GROUP_SPILL_BLOCK_TAG: u8 = 0xB2;
const GROUP_SPILL_BLOCK_BYTES: usize = 64 * 1024;

fn append_group_spill_block_record( block: &mut Vec<u8>, previous_key: &mut Vec<u8>, payload_scratch: &mut Vec<u8>, record: &GroupSpillRecord, ) -> Result<(), ExecutionError> {
    if block.is_empty() {
        block.push(GROUP_SPILL_BLOCK_TAG);
    }
    let common = crate::helpers::common_prefix_len(previous_key, &record.key);
    let suffix = &record.key[common..];

    encode_varint(block, common as u64);
    encode_varint(block, suffix.len() as u64);
    match &record.partial {
        GroupSpillPartial::Compact(payload) => {
            block.push(1);
            encode_varint(block, payload.len() as u64);
            block.extend_from_slice(suffix);
            block.extend_from_slice(payload);
        }
        GroupSpillPartial::Document(row) => {
            encode_execution_row_into(row, payload_scratch)?;
            block.push(0);
            encode_varint(block, payload_scratch.len() as u64);
            block.extend_from_slice(suffix);
            block.extend_from_slice(payload_scratch);
        }
    }
    previous_key.clear();
    previous_key.extend_from_slice(&record.key);
    Ok(())
}

fn decode_group_spill_partial( tag: u8, payload: &[u8], ) -> Result<GroupSpillPartial, ExecutionError> {
    match tag {
        0 => Ok(GroupSpillPartial::Document(decode_execution_row(payload)?)),
        1 => Ok(GroupSpillPartial::Compact(payload.to_vec())),
        tag => Err(ExecutionError::evaluation(format!(
            "invalid group spill tag {tag}"
        ))),
    }
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
            return Err(ExecutionError::evaluation("invalid group spill block"));
        }
        let mut input = Decoder::new(&bytes[1..]);
        let mut previous_key = Vec::<u8>::new();
        while !input.is_empty() {
            let (common, suffix_len, tag, payload_len) = (input.size()?, input.size()?, input.byte()?, input.size()?);
            if common > previous_key.len() {
                return Err(ExecutionError::evaluation("invalid group spill key prefix"));
            }
            previous_key.truncate(common);
            previous_key.extend_from_slice(input.take(suffix_len)?);
            let partial = decode_group_spill_partial(tag, input.take(payload_len)?)?;
            self.pending.push_back(GroupSpillRecord { key: Arc::from(previous_key.as_slice()), partial });
        }
        self.pending
            .pop_front()
            .map_or(Ok(None), |record| Ok(Some(record)))
    }
}

fn read_group_spill_record( reader: &mut GroupSpillRunReader, ) -> Result<Option<GroupSpillRecord>, ExecutionError> {
    reader.next_record()
}

fn flush_keyed_run( spill: &SpillEngine, chunk: &mut Vec<KeyedRow>, runs: &mut Vec<SpillRun>, ) -> EngineResult<u64> {
    chunk.sort_by(|left, right| left.key.cmp(&right.key));
    let mut writer = spill.create_run().map_err(engine_spill_error)?;
    let mut encoded = Vec::new();
    let mut row_encoded = Vec::new();
    for keyed in chunk.iter() {
        encode_keyed_row_into(keyed, &mut encoded, &mut row_encoded)
            .map_err(EngineError::execution)?;
        writer.append(&encoded).map_err(engine_spill_error)?;
    }
    let run = writer.finish().map_err(engine_spill_error)?;
    let bytes = run.bytes();
    runs.push(run);
    chunk.clear();
    Ok(bytes)
}

fn encode_keyed_row_into( keyed: &KeyedRow, output: &mut Vec<u8>, row_output: &mut Vec<u8>, ) -> Result<(), ExecutionError> {
    encode_execution_row_into(&keyed.row, row_output)?;
    output.clear();
    encode_bytes(output, &keyed.key);
    output.extend_from_slice(row_output);
    Ok(())
}

fn decode_keyed_row(bytes: &[u8]) -> Result<KeyedRow, ExecutionError> {
    let mut input = Decoder::new(bytes);
    let len = input.len()?;
    let key = Arc::from(input.take(len)?);
    Ok(KeyedRow { key, row: decode_execution_row(input.rest())? })
}

fn read_keyed_row(reader: &mut SpillRunReader) -> Result<Option<KeyedRow>, ExecutionError> {
    reader
        .next_record()
        .map_err(spill_engine_error)?
        .map(|bytes| decode_keyed_row(&bytes))
        .transpose()
}

fn merge_distinct_runs( runs: &[SpillRun], limit: Option<usize>, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<u64> {
    let mut merge = merge_runs(runs, SpillRun::reader, read_keyed_row)?;
    let mut previous: Option<Arc<[u8]>> = None;
    let mut returned = 0u64;
    while limit.is_none_or(|limit| (returned as usize) < limit) {
        let Some(keyed) = merge
            .pop_min(|left, right| Ok(left.key < right.key))
            .map_err(EngineError::execution)?
        else {
            break;
        };
        if previous.as_deref() != Some(keyed.key.as_ref()) {
            previous = Some(keyed.key.clone());
            visitor(keyed.row)?;
            returned = returned.saturating_add(1);
        }
    }
    Ok(returned)
}

fn merge_group_runs( runtime: &QueryRuntime, keys: &[crate::query::ExpressionFieldPath], runs: &[SpillRun], limit: Option<usize>, visitor: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>, ) -> EngineResult<u64> {
    // One group retains only its keys, counters and aggregate states; source
    // rows are never materialized regardless of group cardinality.
    {
        let mut merge = merge_runs(runs, GroupSpillRunReader::new, read_group_spill_record)?;
        let mut current_key: Option<Arc<[u8]>> = None;
        let mut accumulator: Option<Box<dyn crate::query::IncrementalGroupAccumulator>> = None;
        let mut returned = 0u64;

        loop {
            let selected = merge
                .min_index(|left, right| Ok(left.key < right.key))
                .map_err(EngineError::execution)?;
            let Some(index) = selected else { break };
            if limit.is_some_and(|limit| returned as usize >= limit) {
                break;
            }
            let keyed = merge.take(index).map_err(EngineError::execution)?;
            let key_changed = current_key
                .as_deref()
                .is_some_and(|key| key != keyed.key.as_ref());

            if key_changed {
                if let Some(state) = accumulator.take() {
                    let ordinal = returned.saturating_add(1);
                    let synthetic = state.finish(ordinal).map_err(EngineError::execution)?;
                    visitor(ExecutionRow::synthetic(synthetic))?;
                    returned = returned.saturating_add(1);
                }
            }

            if current_key.as_deref() != Some(keyed.key.as_ref()) {
                current_key = Some(keyed.key.clone());
                accumulator = Some(runtime
                    .incremental_group_accumulator(keys)
                    .map_err(EngineError::execution)?);
                if let Some(state) = accumulator.as_mut() {
                    // Standard compact partials omit key values; seed them once
                    // from the canonical merge key. Legacy/custom runtimes may
                    // decline and continue using their payload-contained keys.
                    let _ = state
                        .seed_group_key(&keyed.key)
                        .map_err(EngineError::execution)?;
                }
            }

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
        }

        if limit.is_none_or(|limit| (returned as usize) < limit) {
            if let Some(state) = accumulator.take() {
                let ordinal = returned.saturating_add(1);
                let synthetic = state.finish(ordinal).map_err(EngineError::execution)?;
                visitor(ExecutionRow::synthetic(synthetic))?;
                returned = returned.saturating_add(1);
            }
        }
        Ok(returned)
    }
}

/// [`BatchGroups::narrow`] under a frontier whose first key is a text: its
/// component is its tag 6 then its length-prefixed bytes, compared as such
/// with each row's text; a row of another kind is kept, except a number,
/// which sorts before any text.
#[inline(never)]
fn narrow_text(column: &crate::query::Column, frontier: &[u8], selection: &[u32], narrowed: &mut Vec<u32>) -> bool {
    use crate::query::Tag;
    let mut text = Vec::new();
    narrowed.clear();
    narrowed.extend_from_slice(selection);
    crate::query::keep(narrowed, |row| match column.tag(row) {
        Tag::Str => {
            text.clear();
            text.extend_from_slice(&[1, 6]);
            crate::query::codec::encode_bytes(&mut text, column.str(row).as_bytes());
            text.as_slice() <= frontier
        }
        _ => true,
    });
    true
}

/// Moves the cells of a pivot to a run on disk once they outgrow `stage`.
fn spill_pivot(stage: &StageMemory<'_>, accumulator: &mut dyn crate::query::IncrementalPivotAccumulator, runs: &mut Vec<SpillRun>) -> EngineResult<()> {
    if stage.fits(accumulator.retained_bytes()) {
        return Ok(());
    }
    let mut writer = SpillEngine::default().create_run().map_err(engine_spill_error)?;
    for record in accumulator.spill().map_err(EngineError::execution)? {
        writer.append(&record).map_err(engine_spill_error)?;
    }
    runs.push(writer.finish().map_err(engine_spill_error)?);
    stage.release();
    Ok(())
}

/// Emits the rows of a pivot, its spilled `runs` merged back: the number
/// of rows.
fn finish_pivot(accumulator: Box<dyn crate::query::IncrementalPivotAccumulator>, runs: &[SpillRun], emit: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>) -> EngineResult<u64> {
    let readers = runs.iter().map(|run| {
        let mut reader = run.reader()?;
        Ok(Box::new(std::iter::from_fn(move || reader.next_record().map_err(spill_engine_error).transpose())) as crate::query::PivotRun<'_>)
    }).collect::<io::Result<Vec<_>>>().map_err(engine_spill_error)?;
    let (mut failure, mut returned) = (None, 0u64);
    let finished = accumulator.finish(readers, &mut |row| {
        returned += 1;
        emit(ExecutionRow::synthetic(row)).map_err(|error| ExecutionError::evaluation(failure.insert(error).to_string()))
    });
    match failure {
        Some(error) => Err(error),
        None => finished.map(|()| returned).map_err(EngineError::execution),
    }
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
    #[must_use]
    #[inline]
    pub const fn new(kind: EngineErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &EngineErrorKind {
        &self.kind
    }

    #[must_use]
    pub const fn planning(error: PlannerError) -> Self {
        Self::new(EngineErrorKind::Planning(error))
    }

    #[must_use]
    pub const fn physical_planning(error: PhysicalPlanError) -> Self {
        Self::new(EngineErrorKind::PhysicalPlanning(error))
    }

    #[must_use]
    pub const fn execution(error: ExecutionError) -> Self {
        Self::new(EngineErrorKind::Execution(error))
    }
}

impl From<ExecutionError> for EngineError {
    fn from(error: ExecutionError) -> Self {
        Self::execution(error)
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
        query::PhysicalSource,
        storage::{memory::MemoryStorage, CollectionId},
    };

    #[test] fn engine_executes_an_existing_empty_physical_plan() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let engine = Engine::new(storage); let physical = PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse("users").unwrap()), [], ) .unwrap(); let output = engine.execute_physical(&physical).unwrap(); assert!(output.is_empty()); assert!(!output.committed()); }
    #[test] fn governed_streaming_accepts_group_followed_by_sort_and_limit() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let engine = Engine::new(storage); let group_field = crate::query::ExpressionFieldPath::new(["Article_Code"]).expect("valid group field"); let sort_field = crate::query::ExpressionFieldPath::new(["CA"]).expect("valid sort field"); let plan = PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse("data").unwrap()), [ PhysicalOperator::group([group_field]).unwrap(), PhysicalOperator::sort([crate::query::SortKey::descending(sort_field)]).unwrap(), PhysicalOperator::limit(10), ], ) .unwrap(); assert!(engine.supports_governed_blocking_streaming(&plan)); }
    #[test] fn engine_debug_does_not_require_trait_objects_to_be_debug() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let debug = format!("{:?}", Engine::new(storage)); assert!(debug.starts_with("Engine")); }
    #[test] fn plan_cached_records_a_miss_then_a_hit() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let engine = Engine::new(storage); let source = "from users"; let ast = crate::query::parse(source).expect("query parses"); let pipeline = PlannerPipeline::from_ast(source, &ast).expect("pipeline lowers"); engine.plan_cached(source, &pipeline).expect("first plan"); assert_eq!( engine.planner_cache_stats(), PlannerCacheStats { hits: 0, misses: 1, evictions: 0, } ); engine.plan_cached(source, &pipeline).expect("cached plan"); assert_eq!( engine.planner_cache_stats(), PlannerCacheStats { hits: 1, misses: 1, evictions: 0, } ); }
    #[test] fn invalidating_planner_cache_forces_replanning() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let engine = Engine::new(storage); let source = "from users"; let ast = crate::query::parse(source).expect("query parses"); let pipeline = PlannerPipeline::from_ast(source, &ast).expect("pipeline lowers"); engine.plan_cached(source, &pipeline).expect("first plan"); engine.invalidate_planner_cache(); engine.plan_cached(source, &pipeline).expect("replanned"); let stats = engine.planner_cache_stats(); assert_eq!(stats.hits, 0); assert_eq!(stats.misses, 2); }
    #[test] fn engine_public_types_are_send_and_sync() { fn assert_send_and_sync<T: Send + Sync>() {} assert_send_and_sync::<Engine>(); assert_send_and_sync::<PlannedQuery>(); assert_send_and_sync::<QueryOutput>(); assert_send_and_sync::<EngineError>(); }
    #[test] fn memory_system_collection_exposes_global_and_class_rows() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let governor = MemoryGovernor::with_limit(1_024); let _held = governor .reserve(crate::memory::MemoryClass::Query, 128) .unwrap(); let engine = Engine::new(storage).with_memory_governor(governor); let physical = PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse(vcollections::MEMORY).unwrap()), [], ) .unwrap(); let output = engine.execute_physical(&physical).unwrap(); assert_eq!(output.len(), 8); assert!(output.rows().iter().any(|row| { row.document().get("scope") == Some(&Value::from("global")) && row.document().get("current_bytes") == Some(&Value::from(128_u64)) })); assert!(output.rows().iter().any(|row| { row.document().get("class") == Some(&Value::from("query")) && row.document().get("current_bytes") == Some(&Value::from(128_u64)) })); }
    #[test] fn memory_events_system_collection_exposes_bounded_journal() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let governor = MemoryGovernor::with_limit_and_event_capacity(8 * 1024 * 1024, 8); let reservation = governor .reserve( crate::memory::MemoryClass::Import, crate::memory::MEMORY_EVENT_MIN_BYTES, ) .unwrap(); drop(reservation); let engine = Engine::new(storage).with_memory_governor(governor); let physical = PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse(vcollections::MEMORY_EVENTS).unwrap()), [], ) .unwrap(); let output = engine.execute_physical(&physical).unwrap(); assert_eq!(output.len(), 2); let kinds = output .rows() .iter() .filter_map(|row| row.document().get("kind")) .cloned() .collect::<Vec<_>>(); assert!(kinds.contains(&Value::from("reserved"))); assert!(kinds.contains(&Value::from("released"))); }
    #[test] fn sort_distinct_limit_has_a_bounded_streaming_executor() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let engine = Engine::new(storage); let operators = vec![ PhysicalOperator::sort([crate::query::SortKey::ascending( crate::query::ExpressionFieldPath::new(["value"]).unwrap(), )]) .unwrap(), PhysicalOperator::distinct([]).unwrap(), PhysicalOperator::limit(10), ]; let plan = PhysicalPlan::new( PhysicalSource::collection_scan(CollectionId::parse("data").unwrap()), operators, ) .unwrap(); assert!(engine.supports_governed_blocking_streaming(&plan)); }
}
