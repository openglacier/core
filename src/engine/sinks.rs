//! Blocking stages after the first one of a governed plan.
//!
//! A governed plan is a streaming prefix, a first blocking core, then
//! segments: one blocking stage (a sink) followed by row-local stages. The
//! core's rows run through the row-local stages that follow it into the first
//! sink; once a sink has seen all its input, it emits its rows into the next
//! segment, and so on. Every sink holds its state within the query's memory
//! ([`QueryMemory`]) and spills sorted runs when it can.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::collections::HashSet;

use super::*;
use crate::query::{ExpressionFieldPath, IncrementalGroupAccumulator, IncrementalPivotAccumulator, Reservoir, FillSpec, Stage, WindowSpec};

/// Whether `operator` can be a sink after the first blocking core.
pub(super) fn is_later_sink(operator: &PhysicalOperator) -> bool {
    match operator {
        PhysicalOperator::Sort { .. } | PhysicalOperator::Distinct { .. } | PhysicalOperator::Group { .. } | PhysicalOperator::Pivot { .. } => true,
        PhysicalOperator::Lookup { pipeline, .. } => runs_lookup_pipeline(pipeline),
        PhysicalOperator::Union { .. } => true,
        PhysicalOperator::Custom { stage: Some(Stage::Window | Stage::Fill), .. } => true,
        _ => operator.execution_properties().retained_rows().is_some(),
    }
}

/// A lookup sub-pipeline: its row-local head (which may read the outer row),
/// then the segments that follow its first blocking stage.
fn split_lookup_pipeline(operators: &[PhysicalOperator]) -> (&[PhysicalOperator], &[PhysicalOperator]) {
    operators.split_at(operators.iter().position(|operator| !is_row_operator(operator)).unwrap_or(operators.len()))
}

/// Whether a `lookup` can run `pipeline` for each outer row: row-local
/// stages, then segments as after a governed core (on the joined rows).
pub(super) fn runs_lookup_pipeline(pipeline: &PhysicalSubPipeline) -> bool {
    later_segments(split_lookup_pipeline(pipeline.operators()).1).is_some()
}

/// Splits the stages after the first core's row-local tail into segments: a
/// sink, then the row-local stages up to the next sink. `None` when a stage
/// is neither.
pub(super) fn later_segments(operators: &[PhysicalOperator]) -> Option<Vec<&[PhysicalOperator]>> {
    let mut segments = Vec::new();
    let mut rest = operators;
    while let Some((sink, after)) = rest.split_first() {
        if !is_later_sink(sink) {
            return None;
        }
        let end = after.iter().position(|operator| !is_row_operator(operator)).unwrap_or(after.len());
        segments.push(&rest[..=end]);
        rest = &after[end..];
    }
    Some(segments)
}

/// One `lookup` joining rows one at a time. An equality correlation is
/// served by an index built at the first row, within the stage's memory.
pub(super) struct LookupJoin<'p, 'q> {
    collection: &'p CollectionId,
    alias: Option<&'p str>,
    outer_alias: Option<&'p str>,
    into: &'p str,
    /// The row-local head of the sub-pipeline, then its blocking segments,
    /// run on the joined rows of each outer row.
    pipeline: PhysicalSubPipeline,
    rest: &'p [PhysicalOperator],
    memory: &'q QueryMemory,
    equality: Option<LookupEquality>,
    /// `Some(None)`: the index did not fit, rows are joined by scan.
    index: Option<Option<LookupIndex>>,
    index_memory: StageMemory<'q>,
    matches_memory: StageMemory<'q>,
    memo: Option<LookupMemo<'q>>,
    pub(super) scanned: u64,
    pub(super) filtered: u64,
}

/// Results of a lookup's sub-pipeline by the outer values it reads, when it
/// reads nothing else of the outer row and is deterministic: rows sharing
/// those values share one run. It stops growing past its memory, and stops
/// when values rarely repeat (fewer than one join in eight served after
/// 1024 rows).
struct LookupMemo<'q> {
    outer: Vec<ExpressionFieldPath>,
    results: HashMap<Vec<u8>, Arc<[Arc<Document>]>>,
    memory: StageMemory<'q>,
    bytes: usize,
    joins: u64,
    hits: u64,
}

/// The outer fields `operators` read, `None` when one of them may read any
/// field or is not deterministic (`sample`, `now()`).
fn memo_fields<'a>(operators: impl IntoIterator<Item = &'a PhysicalOperator>, outer_alias: Option<&str>) -> Option<Vec<ExpressionFieldPath>> {
    let mut fields = Vec::new();
    for operator in operators {
        let volatile = match operator {
            PhysicalOperator::Filter { predicate } => predicate.to_string().contains("now("),
            PhysicalOperator::Custom { stage, arguments, .. } => matches!(stage, Some(Stage::Sample)) || arguments.contains("now("),
            _ => false,
        };
        if volatile {
            return None;
        }
        for field in operator.field_reads()? {
            if outer_alias.is_some_and(|alias| field.first() == alias) && !fields.contains(&field) {
                fields.push(field);
            }
        }
    }
    Some(fields)
}

impl<'p, 'q> LookupJoin<'p, 'q> {
    pub(super) fn new(operator: &'p PhysicalOperator, memory: &'q QueryMemory) -> Option<Self> {
        let PhysicalOperator::Lookup { collection, alias, outer_alias, into, pipeline } = operator else {
            return None;
        };
        let (head, rest) = split_lookup_pipeline(pipeline.operators());
        let pipeline = if rest.is_empty() { pipeline.clone() } else { PhysicalSubPipeline::new(head.iter().cloned()).ok()? };
        let memo = memo_fields(pipeline.operators().iter().chain(rest), outer_alias.as_deref()).map(|outer| LookupMemo { outer, results: HashMap::new(), memory: memory.stage(), bytes: 0, joins: 0, hits: 0 });
        Some(Self {
            collection,
            alias: alias.as_deref(),
            outer_alias: outer_alias.as_deref(),
            into,
            equality: LookupEquality::detect(&pipeline, outer_alias.as_deref()),
            pipeline,
            rest,
            memory,
            index: None,
            index_memory: memory.stage(),
            matches_memory: memory.stage(),
            memo,
            scanned: 0,
            filtered: 0,
        })
    }

    pub(super) const fn equality(&self) -> Option<&LookupEquality> {
        self.equality.as_ref()
    }

    pub(super) const fn alias(&self) -> Option<&'p str> {
        self.alias
    }

    pub(super) const fn outer_alias(&self) -> Option<&'p str> {
        self.outer_alias
    }

    pub(super) fn strategies(&self, strategies: ExecutionStrategies) -> ExecutionStrategies {
        if matches!(self.index, Some(Some(_))) { strategies.with(ExecutionStrategy::HashJoin) } else { strategies }
    }

    /// `outer` with its matches attached. `filter` bounds the index to the
    /// keys the outer rows can hold, when they are known upfront.
    pub(super) fn join(&mut self, engine: &Engine, mut outer: ExecutionRow, filter: Option<&LookupKeyFilter>) -> EngineResult<ExecutionRow> {
        let key = match &mut self.memo {
            Some(memo) => {
                let fields = crate::query::LookupFields { inner: &Document::new(), inner_alias: self.alias, outer: outer.document(), outer_alias: self.outer_alias };
                let mut key = Vec::new();
                for field in &memo.outer {
                    match crate::query::ExpressionFieldResolver::resolve_field(&fields, field) {
                        crate::query::SemanticValue::Present(value) => {
                            key.push(1);
                            crate::query::codec::encode_value(&mut key, &value);
                        }
                        crate::query::SemanticValue::Missing => key.push(0),
                    }
                }
                memo.joins += 1;
                if let Some(matches) = memo.results.get(&key) {
                    memo.hits += 1;
                    let matches = matches.iter().cloned().map(Value::Object);
                    outer.document_mut().insert(self.into, Value::array(matches));
                    return Ok(outer);
                }
                Some(key)
            }
            None => None,
        };
        let matches = self.run(engine, &outer, filter)?;
        if let (Some(key), Some(memo)) = (key, &mut self.memo) {
            if memo.joins >= 1024 && memo.hits * 8 < memo.joins {
                self.memo = None;
            } else {
                let bytes = memo.bytes.saturating_add(key.len() + 16 * matches.len() + 64);
                if memo.memory.fits(bytes) {
                    memo.bytes = bytes;
                    memo.results.insert(key, Arc::from(matches.as_slice()));
                }
            }
        }
        // The outer document gains the array in place (copied once when
        // shared); the joined documents are shared, not copied.
        outer.document_mut().insert(self.into, Value::array(matches.into_iter().map(Value::Object)));
        Ok(outer)
    }

    /// The joined documents of `outer`.
    fn run(&mut self, engine: &Engine, outer: &ExecutionRow, filter: Option<&LookupKeyFilter>) -> EngineResult<Vec<Arc<Document>>> {
        if let (Some(equality), None) = (&self.equality, &self.index) {
            let built = engine.build_lookup_index(self.collection, equality, self.alias, self.outer_alias, &self.index_memory, filter)?;
            if let Some(built) = &built {
                self.scanned = self.scanned.saturating_add(built.scanned);
            }
            self.index = Some(built);
        }
        let mut matches = Vec::<Arc<Document>>::new();
        let mut retained_bytes = 0usize;
        let matches_memory = &self.matches_memory;
        let mut collect = |row: ExecutionRow| -> EngineResult<()> {
            let estimated = execution_row_working_bytes(&row).map_err(EngineError::execution)?;
            if !matches_memory.fits(retained_bytes.saturating_add(estimated)) {
                return Err(EngineError::execution(ExecutionError::evaluation(
                    "one lookup result exceeds the governed query working set",
                )));
            }
            retained_bytes = retained_bytes.saturating_add(estimated);
            matches.push(row.shared_document());
            Ok(())
        };
        let lookup_context = Some((outer.document(), self.alias, self.outer_alias));
        // The joined rows run through the blocking segments, when there are.
        let mut stages = engine.later_stages(self.rest, self.memory)?;
        let mut head = |row: ExecutionRow| engine.feed_later(&mut stages, row, lookup_context, &mut collect);
        let inner_stats = if let (Some(equality), Some(Some(index))) = (&self.equality, &self.index) {
            engine.probe_lookup_index(self.collection, index, equality, &self.pipeline, lookup_context, &mut head)?
        } else {
            engine.stream_bounded_secondary_pipeline(self.collection, &self.pipeline, lookup_context, None, &mut head)?
        };
        engine.finish_later(&mut stages, lookup_context, &mut collect)?;
        ensure_later_exact_rows(&stages)?;
        let (later_scanned, later_filtered, _) = later_statistics(&stages);
        drop(stages);
        self.scanned = self.scanned.saturating_add(inner_stats.scanned()).saturating_add(later_scanned);
        self.filtered = self.filtered.saturating_add(inner_stats.filtered()).saturating_add(later_filtered);
        Ok(matches)
    }
}

/// State of one sink.
enum Sink<'p, 'q> {
    Sort { keys: &'p [SortKey], memory: StageMemory<'q>, spill: SpillEngine, chunk: Vec<ExecutionRow>, chunk_bytes: usize, runs: Vec<SpillRun> },
    TopN { keys: &'p [SortKey], memory: StageMemory<'q>, top: BoundedTopN },
    /// The first row of each key, emitted as it comes: only the keys are kept.
    Distinct { fields: &'p [ExpressionFieldPath], memory: StageMemory<'q>, seen: HashSet<Arc<[u8]>>, bytes: usize },
    Group { keys: &'p [ExpressionFieldPath], grouping_keys: Vec<ExpressionFieldPath>, limit: Option<usize>, memory: StageMemory<'q>, spill: SpillEngine, groups: HashMap<Arc<[u8]>, Box<dyn IncrementalGroupAccumulator>>, bytes: usize, runs: Vec<SpillRun> },
    Sample { reservoir: Reservoir<ExecutionRow>, limit: Option<usize> },
    Lookup(Box<LookupJoin<'p, 'q>>),
    Union { collection: &'p CollectionId, pipeline: &'p PhysicalSubPipeline, scanned: u64, filtered: u64 },
    Pivot { accumulator: Option<Box<dyn IncrementalPivotAccumulator>>, memory: StageMemory<'q> },
    /// The rows of the current partition (rows come sorted by partition).
    Window { spec: WindowSpec, memory: StageMemory<'q>, rows: Vec<ExecutionRow>, bytes: usize },
    /// The last row (rows come sorted by partition, then field) and the
    /// next grid position: rows stream, the grid adds the missing ones.
    Fill { spec: FillSpec, last: Option<ExecutionRow>, next: usize, generated: u64 },
}

/// The outer row of a lookup, its alias and the joined rows' alias, as row-local stages read them.
pub(super) type LookupContext<'a> = Option<(&'a Document, Option<&'a str>, Option<&'a str>)>;

/// One segment: a sink, then the row-local stages up to the next sink.
pub(super) struct LaterStage<'p, 'q> {
    sink: Sink<'p, 'q>,
    chain: StreamingRowChain<'p>,
    /// Whether the row-local stages still let rows through.
    open: bool,
    received: u64,
    emitted: u64,
    strategies: ExecutionStrategies,
}

impl Engine {
    /// The segments after the first core's tail, `None` when one of them
    /// cannot run here.
    pub(super) fn later_stages<'p, 'q>(&self, operators: &'p [PhysicalOperator], memory: &'q QueryMemory) -> EngineResult<Vec<LaterStage<'p, 'q>>> {
        let segments = later_segments(operators).expect("governed shape validated the later segments");
        let mut stages = Vec::with_capacity(segments.len());
        for segment in segments {
            let (operator, tail) = segment.split_first().expect("a segment starts with its sink");
            let limit = tail_row_limit(tail);
            let sink = match operator {
                PhysicalOperator::Sort { keys } => match limit {
                    Some(limit) => Sink::TopN { keys, memory: memory.stage(), top: BoundedTopN::new(limit) },
                    None => Sink::Sort { keys, memory: memory.stage(), spill: SpillEngine::default(), chunk: Vec::new(), chunk_bytes: 0, runs: Vec::new() },
                },
                PhysicalOperator::Distinct { fields } => Sink::Distinct { fields, memory: memory.stage(), seen: HashSet::new(), bytes: 0 },
                PhysicalOperator::Group { keys } => {
                    let (grouping_keys, _) = crate::query::group_field_layout(keys).map_err(EngineError::execution)?;
                    Sink::Group { keys, grouping_keys, limit, memory: memory.stage(), spill: SpillEngine::default(), groups: HashMap::new(), bytes: 0, runs: Vec::new() }
                }
                PhysicalOperator::Pivot { specification } => {
                    let accumulator = self.runtime.incremental_pivot_accumulator(specification).map_err(EngineError::execution)?;
                    Sink::Pivot { accumulator: Some(accumulator), memory: memory.stage() }
                }
                PhysicalOperator::Lookup { .. } => Sink::Lookup(Box::new(LookupJoin::new(operator, memory).expect("a lookup operator"))),
                PhysicalOperator::Union { collection, pipeline, .. } => Sink::Union { collection, pipeline, scanned: 0, filtered: 0 },
                PhysicalOperator::Custom { stage: Some(Stage::Fill), arguments, .. } => {
                    let spec = FillSpec::parse(arguments).map_err(|message| EngineError::execution(ExecutionError::evaluation(message)))?;
                    Sink::Fill { spec, last: None, next: 0, generated: 0 }
                }
                PhysicalOperator::Custom { stage: Some(Stage::Window), arguments, .. } => {
                    Sink::Window { spec: WindowSpec::parse(arguments).map_err(EngineError::execution)?, memory: memory.stage(), rows: Vec::new(), bytes: 0 }
                }
                _ => {
                    let capacity = operator.execution_properties().retained_rows().expect("a later sink retains rows");
                    Sink::Sample { reservoir: Reservoir::new(capacity), limit }
                }
            };
            let chain = StreamingRowChain::new(tail);
            let open = !chain.has_empty_bound();
            stages.push(LaterStage { sink, chain, open, received: 0, emitted: 0, strategies: ExecutionStrategies::default() });
        }
        Ok(stages)
    }

    /// Hands `row` to the first of `stages`, or to `last` when none is left.
    /// In a lookup, `context` is the outer row its row-local stages may read.
    pub(super) fn feed_later(&self, stages: &mut [LaterStage<'_, '_>], row: ExecutionRow, context: LookupContext<'_>, last: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>) -> EngineResult<()> {
        let Some((stage, rest)) = stages.split_first_mut() else {
            return last(row);
        };
        stage.received += 1;
        let LaterStage { sink, chain, open, emitted, .. } = stage;
        let mut emit = |row: ExecutionRow| self.emit_later(chain, open, emitted, rest, row, context, last);
        match sink {
            Sink::Sort { keys, memory, spill, chunk, chunk_bytes, runs } => {
                push_bounded_sort_row(self.runtime.as_ref(), keys, spill, row, memory, chunk, chunk_bytes, runs, "one sort row exceeds the governed query working set")
            }
            Sink::TopN { keys, memory, top } => {
                top.push_lazy(self.runtime.as_ref(), keys, row, execution_row_working_bytes).map_err(EngineError::execution)?;
                if !memory.fits(top.live_bytes()) {
                    return Err(EngineError::execution(ExecutionError::evaluation("top-n candidates exceed the governed query working set")));
                }
                Ok(())
            }
            Sink::Distinct { fields, memory, seen, bytes } => {
                let key = self.runtime.distinct_key(fields, row.document()).map_err(EngineError::execution)?;
                if seen.contains(&key) {
                    return Ok(());
                }
                *bytes = bytes.saturating_add(key.len()).saturating_add(48);
                if !memory.fits(*bytes) {
                    return Err(EngineError::execution(ExecutionError::evaluation("distinct keys exceed the governed query working set")));
                }
                seen.insert(key);
                emit(row)
            }
            Sink::Group { keys, grouping_keys, memory, spill, groups, bytes, runs, .. } => {
                let key = self.runtime.distinct_key(grouping_keys, row.document()).map_err(EngineError::execution)?;
                if !groups.contains_key(&key) {
                    let estimate = group_state_estimate(key.len(), keys.len());
                    if !groups.is_empty() && !memory.fits(bytes.saturating_add(estimate)) {
                        flush_partial_group_run(self.runtime.as_ref(), keys, spill, groups, runs)?;
                        *bytes = 0;
                    }
                    let accumulator = self.runtime.incremental_group_accumulator(keys).map_err(EngineError::execution)?;
                    *bytes = bytes.saturating_add(estimate);
                    groups.insert(Arc::clone(&key), accumulator);
                }
                groups.get_mut(&key).expect("group accumulator exists").push(row.document()).map_err(EngineError::execution)
            }
            Sink::Sample { reservoir, .. } => {
                reservoir.push(row);
                Ok(())
            }
            Sink::Lookup(join) => {
                let joined = join.join(self, row, None)?;
                emit(joined)
            }
            Sink::Union { .. } => emit(row),
            Sink::Window { spec, memory, rows, bytes } => {
                if rows.last().map(|last| spec.same_partition(last, &row)).transpose().map_err(EngineError::execution)? == Some(false) {
                    finish_window(spec, rows, &mut emit)?;
                    *bytes = 0;
                }
                *bytes = bytes.saturating_add(execution_row_working_bytes(&row).map_err(EngineError::execution)?);
                if !memory.fits(*bytes) {
                    return Err(EngineError::execution(ExecutionError::evaluation("a window partition exceeds the governed query working set")));
                }
                rows.push(row);
                Ok(())
            }
            Sink::Fill { spec, last, next, generated } => {
                if let Some(previous) = last.take() {
                    if spec.same_partition(&previous, &row)? {
                        *last = Some(previous);
                    } else {
                        spec.fill(&previous, None, *next, generated, &mut emit)?;
                        *next = 0;
                    }
                }
                *next = spec.fill(&row, Some(&row), *next, generated, &mut emit)?;
                *last = Some(row.clone());
                emit(row)
            }
            Sink::Pivot { accumulator, memory } => {
                let accumulator = accumulator.as_mut().expect("pivot accumulator until finished");
                accumulator.push(row.document()).map_err(EngineError::execution)?;
                if !memory.fits(accumulator.retained_bytes()) {
                    return Err(EngineError::execution(ExecutionError::evaluation(
                        "pivot cells exceed the governed query working set; reduce the row or column cardinality",
                    )));
                }
                Ok(())
            }
        }
    }

    /// Runs one row a sink emits through its row-local stages, into the
    /// stages after it.
    fn emit_later(&self, chain: &mut StreamingRowChain<'_>, open: &mut bool, emitted: &mut u64, rest: &mut [LaterStage<'_, '_>], row: ExecutionRow, context: LookupContext<'_>, last: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>) -> EngineResult<()> {
        if !*open {
            return Ok(());
        }
        *emitted += 1;
        let mut failure = None;
        let driven = self.drive_streaming_row(chain, 0, row, context, &mut |row| {
            // An engine error comes back as it is, not reworded by each segment.
            self.feed_later(rest, row, context, last).map_err(|error| {
                let storage = backend_storage_error(&error);
                failure = Some(error);
                storage
            })
        });
        *open = match (driven, failure) {
            (Ok(open), _) => open,
            (Err(_), Some(error)) => return Err(error),
            (Err(error), None) => return Err(storage_engine_error(error)),
        };
        Ok(())
    }

    /// Lets each sink, in order, emit its rows once its input is complete.
    pub(super) fn finish_later(&self, stages: &mut [LaterStage<'_, '_>], context: LookupContext<'_>, last: &mut dyn FnMut(ExecutionRow) -> EngineResult<()>) -> EngineResult<()> {
        for index in 0..stages.len() {
            let (stage, rest) = stages[index..].split_first_mut().expect("index within the stages");
            let LaterStage { sink, chain, open, emitted, strategies, .. } = stage;
            let mut emit = |row: ExecutionRow| self.emit_later(chain, open, emitted, rest, row, context, last);
            match sink {
                Sink::Sort { keys, memory, spill, chunk, runs, .. } => {
                    // Emitted rows pass to the next stages, which count them.
                    if runs.is_empty() {
                        stable_sort(self.runtime.as_ref(), keys, chunk).map_err(EngineError::execution)?;
                        memory.release();
                        for row in chunk.drain(..) {
                            emit(row)?;
                        }
                        *strategies = strategies.with(ExecutionStrategy::InMemorySort);
                    } else {
                        if !chunk.is_empty() {
                            flush_sorted_run(self.runtime.as_ref(), keys, spill, chunk, runs).map_err(EngineError::execution)?;
                        }
                        memory.release();
                        merge_sorted_runs(self.runtime.as_ref(), keys, runs, None, &mut emit)?;
                        *strategies = strategies.with(ExecutionStrategy::ExternalSort);
                    }
                }
                Sink::TopN { keys, top, .. } => {
                    let rows = std::mem::replace(top, BoundedTopN::new(0)).into_sorted_rows(self.runtime.as_ref(), keys).map_err(EngineError::execution)?;
                    for row in rows {
                        emit(row)?;
                    }
                    *strategies = strategies.with(ExecutionStrategy::TopN);
                }
                Sink::Distinct { .. } => *strategies = strategies.with(ExecutionStrategy::InMemoryDistinct),
                Sink::Group { keys, limit, memory, spill, groups, runs, .. } => {
                    if runs.is_empty() {
                        // Groups come in key order, as merged runs do.
                        let mut sorted = groups.drain().collect::<Vec<_>>();
                        sorted.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                        memory.release();
                        for (ordinal, (_, accumulator)) in sorted.into_iter().take(limit.unwrap_or(usize::MAX)).enumerate() {
                            let document = accumulator.finish(usize_to_u64_saturating(ordinal).saturating_add(1)).map_err(EngineError::execution)?;
                            emit(ExecutionRow::synthetic(document))?;
                        }
                        *strategies = strategies.with(ExecutionStrategy::InMemoryGroup);
                    } else {
                        if !groups.is_empty() {
                            flush_partial_group_run(self.runtime.as_ref(), keys, spill, groups, runs)?;
                        }
                        memory.release();
                        merge_group_runs(self.runtime.as_ref(), keys, runs, *limit, &mut emit)?;
                        *strategies = strategies.with(ExecutionStrategy::ExternalGroup);
                    }
                }
                Sink::Sample { reservoir, limit } => {
                    let mut rows = std::mem::replace(reservoir, Reservoir::new(0)).into_items();
                    if let Some(limit) = limit {
                        rows.truncate(*limit);
                    }
                    for row in rows {
                        emit(row)?;
                    }
                }
                Sink::Lookup(join) => *strategies = join.strategies(*strategies),
                Sink::Union { collection, pipeline, scanned, filtered } => {
                    let branch = self.stream_bounded_secondary_pipeline(collection, pipeline, None, None, &mut emit)?;
                    *scanned = branch.scanned();
                    *filtered = branch.filtered();
                }
                Sink::Window { spec, memory, rows, .. } => {
                    finish_window(spec, rows, &mut emit)?;
                    memory.release();
                }
                Sink::Fill { spec, last, next, generated } => {
                    if let Some(previous) = last.take() {
                        spec.fill(&previous, None, *next, generated, &mut emit)?;
                    }
                }
                Sink::Pivot { accumulator, memory } => {
                    let rows = accumulator.take().expect("pivot accumulator until finished").finish().map_err(EngineError::execution)?;
                    memory.release();
                    for row in rows {
                        emit(ExecutionRow::synthetic(row))?;
                    }
                    *strategies = strategies.with(ExecutionStrategy::InMemoryPivot);
                }
            }
        }
        Ok(())
    }
}

/// Emits the rows of one window partition, with the window's values.
fn finish_window(spec: &WindowSpec, rows: &mut Vec<ExecutionRow>, emit: &mut impl FnMut(ExecutionRow) -> EngineResult<()>) -> EngineResult<()> {
    spec.apply(rows).map_err(EngineError::execution)?;
    rows.drain(..).try_for_each(emit)
}

/// What the later stages add to the statistics of the first core: rows read
/// by `lookup`/`union` sub-pipelines, rows the stages removed, strategies.
pub(super) fn later_statistics(stages: &[LaterStage<'_, '_>]) -> (u64, u64, ExecutionStrategies) {
    let (mut scanned, mut filtered, mut strategies) = (0u64, 0u64, ExecutionStrategies::default());
    for stage in stages {
        let (inner_scanned, inner_filtered) = match &stage.sink {
            Sink::Lookup(join) => (join.scanned, join.filtered),
            Sink::Union { scanned, filtered, .. } => (*scanned, *filtered),
            _ => (0, 0),
        };
        scanned = scanned.saturating_add(inner_scanned);
        filtered = filtered
            .saturating_add(inner_filtered)
            .saturating_add(stage.received.saturating_sub(stage.emitted))
            .saturating_add(stage.chain.filtered);
        strategies = stage.strategies.iter().fold(strategies, ExecutionStrategies::with);
    }
    (scanned, filtered, strategies)
}

/// Fails when a later stage declaring an exact row count saw fewer rows.
pub(super) fn ensure_later_exact_rows(stages: &[LaterStage<'_, '_>]) -> EngineResult<()> {
    stages.iter().try_for_each(|stage| ensure_exact_rows(stage.chain.operators, &stage.chain.limit_remaining))
}
