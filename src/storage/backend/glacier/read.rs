#![cfg_attr(rustfmt, rustfmt_skip)]
//! Read snapshots: block cursors over segments and the delta, sequential for
//! unordered reads, merged by identifier for ordered ones.

use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use crate::storage::BlockGate;

use super::{
    codec::{decode_value, value_at_path, Reader},
    column::{cell_value, Cell, Scratch, StringCache, TypedBlock, BLOCK, K_FALSE, K_FLOAT, K_MISSING, K_NESTED, K_NULL, K_SIGNED, K_STRING, K_TRUE, K_UNSIGNED},
    id_of,
    segment::{self, Group, Segment},
    Collection, DeltaRow, SegmentPart, State,
};
use crate::{
    model::{Document, FieldName, FieldPath, FieldPathSegment, PhysicalKind, Value},
    storage::{
        project_document, BlockCodes, BlockColumn, CollectionId, DocumentId, DocumentVersion, FieldBounds, FieldFact, FieldFacts, GroupFacts, ProjectedBlock, ProjectedValueRef, ScanDirection, ScanOptions,
        StorageRead, StorageReadCapability, StorageResult, StorageSupport, StoredDocument,
    },
};

/// Which top-level fields a read decodes.
#[derive(Clone, Copy, Debug)]
enum Columns {
    /// Every field (full documents).
    All,
    /// The first segment of each requested path (see [`Layout`]).
    Paths,
}

/// Requested paths split into a top-level slot and a nested remainder.
#[derive(Debug)]
struct Layout<'f> {
    names: Vec<&'f str>,
    paths: Vec<(usize, &'f [FieldPathSegment])>,
}

impl<'f> Layout<'f> {
    fn new(fields: &'f [FieldPath]) -> Self {
        let mut names = Vec::<&str>::new();
        let paths = fields.iter().map(|path| {
            let segments = path.as_segments();
            let name = segments[0].as_str();
            let slot = names.iter().position(|known| *known == name).unwrap_or_else(|| { names.push(name); names.len() - 1 });
            (slot, &segments[1..])
        }).collect();
        Self { names, paths }
    }

    fn only_top_level(&self) -> bool { self.paths.iter().all(|(_, rest)| rest.is_empty()) }
}

/// One decoded block of a segment.
struct Block<'s> {
    group: Option<(usize, Group<'s>)>,
    readers: Vec<Option<super::column::ColumnReader<'s>>>,
    /// Block index in its group.
    index: usize,
    start: usize,
    len: usize,
    first_row: u64,
    ids: Vec<u128>,
    versions: Vec<u64>,
    cells: Vec<Vec<Cell<'s>>>,
    /// Whether slots sharing one kind over the block decode into `typed`
    /// instead of `cells`.
    typed_mode: bool,
    /// Per slot, decoded into `typed` (its `cells` are then empty).
    is_typed: Vec<bool>,
    /// Per slot, whether typed number cells keep their dictionary codes.
    number_codes: Vec<bool>,
    typed: Vec<TypedBlock<'s>>,
    scratch: Scratch,
    caches: Vec<StringCache>,
    /// Whether [`Self::load`] leaves the slots to [`Self::ready`], and the
    /// locators to [`Self::locate`].
    lazy: bool,
    /// Identifiers and versions still to decode.
    locators: (bool, bool),
    /// Per slot, decoded; then also retained to the live rows.
    decoded: Vec<bool>,
    ready: Vec<bool>,
}

impl<'s> Block<'s> {
    fn new(slots: usize, typed_mode: bool) -> Self {
        Self {
            group: None, readers: Vec::new(), index: 0, start: 0, len: 0, first_row: 0, ids: Vec::new(), versions: Vec::new(), cells: (0..slots).map(|_| Vec::with_capacity(BLOCK)).collect(),
            typed_mode, is_typed: vec![false; slots], number_codes: vec![false; slots], typed: (0..if typed_mode { slots } else { 0 }).map(|_| TypedBlock::default()).collect(), scratch: Scratch::default(), caches: (0..slots).map(|_| StringCache::default()).collect(),
            lazy: false, locators: (false, false), decoded: vec![false; slots], ready: vec![false; slots],
        }
    }

    fn load(&mut self, segment: &'s Segment, fields: &[Option<usize>], group_index: usize, block: usize, ids: bool, versions: bool) -> StorageResult<()> {
        if self.group.as_ref().is_none_or(|(index, _)| *index != group_index) {
            let group = segment.group(group_index)?;
            self.readers.clear();
            for field in fields {
                self.readers.push(match field { Some(field) => segment.column(group_index, *field)?, None => None });
            }
            for cache in &mut self.caches {
                cache.clear();
            }
            self.group = Some((group_index, group));
        }
        let Some((_, group)) = self.group else { return Ok(()) };
        self.index = block;
        self.start = block * BLOCK;
        self.len = BLOCK.min(group.rows - self.start);
        self.first_row = group.first_row + self.start as u64;
        self.locators = (ids, versions);
        if !self.lazy {
            self.locate();
        }
        self.decoded.fill(false);
        self.ready.fill(false);
        if !self.lazy {
            (0..self.readers.len()).try_for_each(|slot| self.decode(slot))?;
        }
        Ok(())
    }

    fn decode(&mut self, slot: usize) -> StorageResult<()> {
        let (reader, cells) = (&self.readers[slot], &mut self.cells[slot]);
        cells.clear();
        self.decoded[slot] = true;
        if self.typed_mode {
            self.is_typed[slot] = match reader { Some(reader) => reader.decode_typed(self.index, self.len, &mut self.typed[slot], &mut self.scratch, self.number_codes[slot])?, None => false };
            if self.is_typed[slot] {
                return Ok(());
            }
        }
        cells.resize(self.len, Cell::Missing);
        if let Some(reader) = reader {
            reader.decode_block(self.index, cells, &mut self.scratch)?;
        }
        Ok(())
    }

    /// Decodes `slot` if needed, and keeps only its `live` rows when typed
    /// (not `whole`).
    fn ready(&mut self, slot: usize, live: &[usize], whole: bool) -> StorageResult<()> {
        if !self.decoded[slot] {
            self.decode(slot)?;
        }
        if !std::mem::replace(&mut self.ready[slot], true) && self.is_typed[slot] && !whole {
            self.typed[slot].retain(live);
        }
        Ok(())
    }

    /// Decodes the identifiers and versions [`Self::load`] was asked for.
    fn locate(&mut self) {
        let (ids, versions) = std::mem::take(&mut self.locators);
        let Some((_, group)) = &self.group else { return };
        if ids {
            group.ids.decode_block(self.index, &mut self.ids, &mut self.scratch);
        }
        if versions {
            self.versions.resize(self.len, 0);
            group.versions.decode(self.start, &mut self.versions[..self.len]);
        }
    }

    /// Whether every row of the block holds a scalar in `slot`.
    fn uniform(&self, slot: usize) -> bool {
        self.readers[slot].as_ref().is_some_and(|reader| reader.uniform(self.index, self.len))
    }
}

/// Reads the `live` rows of `block` (all of them when `whole`) at the
/// `paths` that `wanted` selects into `columns`, with their dictionary codes
/// in `codes` under the identifier in `coded`.
#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn read_paths<'s>(block: &mut Block<'s>, part: &SegmentPart, fields: &[Option<usize>], paths: &[(usize, &[FieldPathSegment])], live: &[usize], whole: bool, wanted: impl Fn(usize) -> bool, columns: &mut [Vec<Option<ProjectedValueRef<'s>>>], codes: &mut [Vec<u32>], coded: &mut [Option<u64>]) -> StorageResult<()> {
    // Dictionary codes, under an identifier unique to this chunk's stream:
    // kind, segment, group and field.
    let group = block.group.as_ref().map_or(0, |(group, _)| *group);
    let identifier = |slot: usize, kind: u8| match fields[slot] {
        Some(field) if group < 1 << 16 && field < 1 << 16 && part.segment.number() < 1 << 24 => {
            Some(u64::from(if kind == K_STRING { 0 } else { kind }) << 56 | part.segment.number() << 32 | (group as u64) << 16 | field as u64)
        }
        _ => None,
    };
    for (path, (((column, (slot, rest)), codes), dictionary)) in columns.iter_mut().zip(paths).zip(codes).zip(coded).enumerate() {
        if !wanted(path) {
            continue;
        }
        block.ready(*slot, live, whole)?;
        column.clear();
        *dictionary = None;
        if !rest.is_empty() {
            // A nested path: read from each row's encoded value (a typed
            // slot holds scalars, where the path is missing).
            if block.is_typed[*slot] {
                column.resize(live.len(), None);
            } else {
                for offset in live {
                    column.push(match block.cells[*slot][*offset] { Cell::Nested(bytes) => value_at_path(bytes, rest)?, _ => None });
                }
            }
            continue;
        }
        if block.is_typed[*slot] {
            let typed = &block.typed[*slot];
            if !typed.codes.is_empty() {
                *dictionary = identifier(*slot, typed.kind);
            }
            continue;
        }
        let cells = &block.cells[*slot];
        if cells.iter().any(|cell| matches!(cell, Cell::Nested(_))) {
            for offset in live {
                column.push(top_level_ref(cells[*offset])?);
            }
        } else if whole {
            column.extend(cells[..block.len].iter().map(|cell| cell_ref(*cell, &[])));
        } else {
            column.extend(live.iter().map(|offset| cell_ref(cells[*offset], &[])));
        }
        if !cells.iter().any(|cell| matches!(cell, Cell::Str(_, code) if *code != u32::MAX)) {
            continue;
        }
        let Some(identifier) = identifier(*slot, K_STRING) else { continue };
        codes.clear();
        let code = |cell: &Cell<'_>| match cell { Cell::Str(_, code) => *code, _ => BlockCodes::NONE };
        if whole { codes.extend(cells[..block.len].iter().map(code)); } else { codes.extend(live.iter().map(|offset| code(&cells[*offset]))); }
        *dictionary = Some(identifier);
    }
    Ok(())
}

/// The columns [`read_paths`] read, as a block hands them out; the paths
/// `wanted` leaves out are empty.
fn project<'b, 's>(block: &'b Block<'s>, paths: &[(usize, &[FieldPathSegment])], columns: &'b [Vec<Option<ProjectedValueRef<'s>>>], codes: &'b [Vec<u32>], coded: &[Option<u64>], wanted: impl Fn(usize) -> bool) -> (Vec<BlockColumn<'b, 's>>, Vec<Option<BlockCodes<'b>>>) {
    let block_columns = columns.iter().zip(paths).enumerate().map(|(path, (column, (slot, rest)))| {
        if !wanted(path) {
            return BlockColumn::Values(&[]);
        }
        if !block.is_typed[*slot] || !rest.is_empty() {
            return BlockColumn::Values(column);
        }
        let typed = &block.typed[*slot];
        match typed.kind {
            K_SIGNED => BlockColumn::Signed(&typed.signed),
            K_UNSIGNED => BlockColumn::Unsigned(&typed.unsigned),
            K_FLOAT => BlockColumn::Floats(&typed.floats),
            _ => BlockColumn::Strings(&typed.strings),
        }
    }).collect();
    let block_codes = coded.iter().zip(codes).zip(paths).enumerate().map(|(path, ((dictionary, codes), (slot, _)))| {
        dictionary.filter(|_| wanted(path)).map(|dictionary| BlockCodes { dictionary, codes: if block.is_typed[*slot] { &block.typed[*slot].codes } else { codes } })
    }).collect();
    (block_columns, block_codes)
}

/// A sorted cursor over one part.
enum Cursor<'s> {
    Segment(Box<SegmentCursor<'s>>),
    Delta { rows: Vec<(u128, &'s DeltaRow)>, at: usize },
}

struct SegmentCursor<'s> {
    part: &'s SegmentPart,
    fields: Vec<Option<usize>>,
    reverse: bool,
    ids: bool,
    versions: bool,
    /// Next (group, block) to load, in emission order.
    position: Option<(usize, usize)>,
    /// Groups left out (empty: none).
    skipped: Vec<bool>,
    /// Per layout path, the slot of the leaf column serving it whole
    /// (`None`: read below its top-level slot). Empty unless pruned.
    leaves: Vec<Option<usize>>,
    block: Block<'s>,
    live: Vec<usize>,
    at: usize,
}

/// Facts of one chunk, from its statistics.
fn group_facts(stats: &super::column::ChunkStats) -> GroupFacts<'_> {
    let has = |kind: u8| stats.kinds & (1 << kind) != 0;
    let mut kinds = Vec::new();
    for (present, kind) in [
        (has(K_NULL), PhysicalKind::Null),
        (has(K_FALSE) || has(K_TRUE), PhysicalKind::Bool),
        (has(K_SIGNED) || has(K_UNSIGNED) || has(K_FLOAT), PhysicalKind::Number),
        (has(K_STRING), PhysicalKind::String),
        (has(K_NESTED), PhysicalKind::Array),
        (has(K_NESTED), PhysicalKind::Object),
    ] {
        if present {
            kinds.push(kind);
        }
    }
    GroupFacts { missing: has(K_MISSING), kinds, bounds: &stats.bounds }
}

fn blocks_in(segment: &Segment, group: usize) -> StorageResult<usize> { Ok(segment.group_rows(group).1.div_ceil(BLOCK)) }

impl<'s> SegmentCursor<'s> {
    /// `typed`: see [`Block::typed_mode`].
    fn new(part: &'s SegmentPart, columns: Columns, layout: Option<&Layout<'_>>, reverse: bool, ids: bool, versions: bool, typed: bool) -> StorageResult<Self> {
        let segment = &part.segment;
        let fields = match (columns, layout) {
            (Columns::Paths, Some(layout)) => layout.names.iter().map(|name| segment.field_index(name)).collect(),
            _ => (0..segment.fields().len()).map(Some).collect::<Vec<_>>(),
        };
        let groups = segment.group_count();
        let position = if groups == 0 { None } else if reverse { Some((groups - 1, blocks_in(segment, groups - 1)?.saturating_sub(1))) } else { Some((0, 0)) };
        let slots = fields.len();
        let mut cursor = Self { part, fields, reverse, ids, versions, position, skipped: Vec::new(), leaves: Vec::new(), block: Block::new(slots, typed), live: Vec::with_capacity(BLOCK), at: 0 };
        cursor.fill()?;
        Ok(cursor)
    }

    /// [`Self::new`] for a forward scan of `layout`'s paths that leaves out the
    /// groups for which `skip` holds on the facts of each path.
    /// A nested path the segment holds as a leaf reads that leaf's column,
    /// in a slot of its own; a top-level slot no path reads is not decoded.
    /// `number_codes`: [`ScanOptions::number_codes`] of `layout`'s paths.
    /// `lazy`: see [`Block::lazy`].
    fn pruned(part: &'s SegmentPart, layout: &Layout<'_>, ids: bool, number_codes: u64, lazy: bool, skip: &dyn Fn(&[Option<GroupFacts<'_>>]) -> bool) -> StorageResult<Self> {
        let segment = &part.segment;
        let mut fields = layout.names.iter().map(|name| segment.field_index(name)).collect::<Vec<_>>();
        let mut read = vec![false; fields.len()];
        let leaves = layout.paths.iter().map(|(slot, rest)| {
            let Some(column) = Some(rest).filter(|rest| !rest.is_empty()).and_then(|rest| segment.leaf_index(layout.names[*slot], rest)) else {
                read[*slot] = true;
                return None;
            };
            Some(fields.iter().skip(read.len()).position(|field| *field == Some(column)).map_or_else(|| { fields.push(Some(column)); fields.len() - 1 }, |at| read.len() + at))
        }).collect::<Vec<_>>();
        for (field, read) in fields.iter_mut().zip(read) {
            if !read {
                *field = None;
            }
        }
        let empty = FieldBounds::default();
        let mut facts = Vec::with_capacity(layout.paths.len());
        let skipped = (0..segment.group_count()).map(|group| {
            facts.clear();
            facts.extend(layout.paths.iter().zip(&leaves).map(|((slot, rest), leaf)| {
                // Statistics describe the whole value of a column: a leaf's
                // are its path's, a top-level field's are not a nested path's.
                let (field, rest) = match leaf { Some(leaf) => (fields[*leaf], &[][..]), None => (fields[*slot], *rest) };
                match field {
                    Some(field) if segment.has_column(group, field) => segment.stats(group, field).filter(|_| rest.is_empty()).map(group_facts),
                    _ => Some(GroupFacts { missing: true, kinds: Vec::new(), bounds: &empty }),
                }
            }));
            skip(&facts)
        }).collect::<Vec<_>>();
        let groups = segment.group_count();
        let slots = fields.len();
        let mut block = Block::new(slots, true);
        block.lazy = lazy;
        for (path, ((slot, rest), leaf)) in layout.paths.iter().zip(&leaves).enumerate() {
            if path < 64 && number_codes & 1 << path != 0 {
                match leaf {
                    Some(leaf) => block.number_codes[*leaf] = true,
                    None if rest.is_empty() => block.number_codes[*slot] = true,
                    None => {}
                }
            }
        }
        let mut cursor = Self { part, fields, reverse: false, ids, versions: ids, position: (groups > 0).then_some((0, 0)), skipped, leaves, block, live: Vec::with_capacity(BLOCK), at: 0 };
        cursor.fill()?;
        Ok(cursor)
    }

    /// Loads blocks until one has a live row; leaves `live` empty at the end.
    fn fill(&mut self) -> StorageResult<()> {
        self.live.clear();
        self.at = 0;
        let segment = &self.part.segment;
        while let Some((group, block)) = self.position {
            if self.skipped.get(group).copied().unwrap_or(false) {
                self.position = if self.reverse {
                    if group > 0 { Some((group - 1, blocks_in(segment, group - 1)?.saturating_sub(1))) } else { None }
                } else if group + 1 < segment.group_count() { Some((group + 1, 0)) } else { None };
                continue;
            }
            self.position = if self.reverse {
                if block > 0 { Some((group, block - 1)) } else if group > 0 { Some((group - 1, blocks_in(segment, group - 1)?.saturating_sub(1))) } else { None }
            } else if block + 1 < blocks_in(segment, group)? { Some((group, block + 1)) } else if group + 1 < segment.group_count() { Some((group + 1, 0)) } else { None };
            let (group_first, group_len) = segment.group_rows(group);
            let first_row = group_first + (block * BLOCK) as u64;
            let len = BLOCK.min(group_len - block * BLOCK);
            match self.part.dead.as_deref() {
                None => self.live.extend(0..len),
                Some(dead) => self.live.extend((0..len).filter(|offset| !dead.contains(first_row + *offset as u64))),
            }
            if self.live.is_empty() {
                continue;
            }
            if self.reverse {
                self.live.reverse();
            }
            self.block.load(segment, &self.fields, group, block, self.ids, self.versions)?;
            return Ok(());
        }
        Ok(())
    }
}

impl<'s> Cursor<'s> {
    fn key(&self) -> Option<u128> {
        match self {
            Self::Segment(cursor) => cursor.live.get(cursor.at).map(|offset| cursor.block.ids[*offset]),
            Self::Delta { rows, at } => rows.get(*at).map(|row| row.0),
        }
    }

    fn exhausted(&self) -> bool {
        match self {
            Self::Segment(cursor) => cursor.at >= cursor.live.len(),
            Self::Delta { rows, at } => *at >= rows.len(),
        }
    }

    fn advance(&mut self) -> StorageResult<()> {
        match self {
            Self::Segment(cursor) => {
                cursor.at += 1;
                if cursor.at >= cursor.live.len() {
                    cursor.fill()?;
                }
            }
            Self::Delta { at, .. } => *at += 1,
        }
        Ok(())
    }

    fn row(&mut self) -> Row<'s, '_> {
        match self {
            Self::Segment(cursor) => {
                let part: &'s SegmentPart = cursor.part;
                Row::Segment { segment: &part.segment, at: cursor.live[cursor.at], block: &mut cursor.block }
            }
            Self::Delta { rows, at } => Row::Delta { key: rows[*at].0, row: rows[*at].1 },
        }
    }
}

/// One visible row.
enum Row<'s, 'b> {
    Segment { segment: &'s Segment, block: &'b mut Block<'s>, at: usize },
    Delta { key: u128, row: &'s DeltaRow },
}

impl<'s> Row<'s, '_> {
    fn id(&self) -> DocumentId {
        match self {
            Self::Segment { block, at, .. } => id_of(block.ids[*at]),
            Self::Delta { key, .. } => id_of(*key),
        }
    }

    fn version(&self) -> DocumentVersion {
        DocumentVersion::new(match self {
            Self::Segment { block, at, .. } => block.versions[*at],
            Self::Delta { row, .. } => row.version,
        })
    }

    /// The row as a document: every field, or only `layout`'s paths.
    fn document(&mut self, layout: Option<&Layout<'_>>, fields: Option<&[FieldPath]>) -> StorageResult<Arc<Document>> {
        match self {
            Self::Delta { row, .. } => Ok(match fields {
                None => row.document.clone(),
                Some(fields) => Arc::new(project_document(&row.document, fields)),
            }),
            Self::Segment { segment, block, at } => {
                let mut document = Document::new();
                let names: Vec<FieldName> = match layout {
                    Some(layout) => layout.names.iter().map(|name| FieldName::from(*name)).collect(),
                    None => segment.fields().to_vec(),
                };
                for (slot, name) in names.into_iter().enumerate() {
                    let cell = block.cells[slot][*at];
                    if let Some(value) = cell_value(cell, Some(&mut block.caches[slot]))? {
                        document.insert(name, value);
                    }
                }
                Ok(Arc::new(match (layout, fields) {
                    (Some(layout), Some(fields)) if !layout.only_top_level() => project_document(&document, fields),
                    _ => document,
                }))
            }
        }
    }

    /// Owned value of each requested path, written into `out`.
    fn values(&mut self, layout: &Layout<'_>, fields: &[FieldPath], range: std::ops::Range<usize>, out: &mut [Option<Value>]) -> StorageResult<()> {
        match self {
            Self::Delta { row, .. } => {
                for index in range {
                    out[index] = fields[index].resolve_value(&row.document).cloned();
                }
            }
            Self::Segment { block, at, .. } => {
                for index in range {
                    let (slot, rest) = layout.paths[index];
                    let value = cell_value(block.cells[slot][*at], Some(&mut block.caches[slot]))?;
                    out[index] = if rest.is_empty() { value } else { value.and_then(|value| resolve(&value, rest).cloned()) };
                }
            }
        }
        Ok(())
    }

    /// Visits borrowed values of each requested path.
    /// Visits borrowed values of each requested path. `refs` is a buffer
    /// reused across rows; rows holding nested values use their own.
    fn with_refs<T>(&mut self, layout: &Layout<'_>, fields: &[FieldPath], refs: &mut Vec<Option<ProjectedValueRef<'s>>>, visit: impl for<'a> FnOnce(&[Option<ProjectedValueRef<'a>>]) -> StorageResult<T>) -> StorageResult<T> {
        refs.clear();
        match self {
            Self::Delta { row, .. } => {
                let document: &'s Document = &row.document;
                refs.extend(fields.iter().map(|path| path.resolve_value(document).map(value_ref)));
                visit(refs)
            }
            Self::Segment { block, at, .. } => {
                let at = *at;
                if layout.paths.iter().any(|(slot, _)| matches!(block.cells[*slot][at], Cell::Nested(_))) {
                    let mut nested = Vec::new();
                    for (slot, rest) in &layout.paths {
                        if let Cell::Nested(bytes) = block.cells[*slot][at] {
                            let value = decode_value(&mut Reader::new(bytes))?;
                            nested.push(if rest.is_empty() { Some(value) } else { resolve(&value, rest).cloned() });
                        }
                    }
                    let mut nested_values = nested.iter();
                    let values = layout.paths.iter().map(|(slot, rest)| match block.cells[*slot][at] {
                        Cell::Nested(_) => nested_values.next().and_then(Option::as_ref).map(value_ref),
                        cell => cell_ref(cell, rest),
                    }).collect::<Vec<_>>();
                    return visit(&values);
                }
                refs.extend(layout.paths.iter().map(|(slot, rest)| cell_ref(block.cells[*slot][at], rest)));
                visit(refs)
            }
        }
    }
}

/// Borrowed value of a whole top-level cell (nested values decoded, owned).
#[inline]
fn top_level_ref(cell: Cell<'_>) -> StorageResult<Option<ProjectedValueRef<'_>>> {
    Ok(match cell {
        Cell::Nested(bytes) => Some(ProjectedValueRef::Owned(decode_value(&mut Reader::new(bytes))?)),
        cell => cell_ref(cell, &[]),
    })
}

#[inline]
fn cell_ref<'s>(cell: Cell<'s>, rest: &[FieldPathSegment]) -> Option<ProjectedValueRef<'s>> {
    if !rest.is_empty() {
        return None;
    }
    match cell {
        Cell::Missing | Cell::Nested(_) => None,
        Cell::Null => Some(ProjectedValueRef::Null),
        Cell::Bool(value) => Some(ProjectedValueRef::Bool(value)),
        Cell::Signed(value) => Some(ProjectedValueRef::Signed(value)),
        Cell::Unsigned(value) => Some(ProjectedValueRef::Unsigned(value)),
        Cell::Float(value) => Some(ProjectedValueRef::Float(value)),
        Cell::Str(text, _) => Some(ProjectedValueRef::String(text)),
    }
}

fn resolve<'v>(value: &'v Value, rest: &[FieldPathSegment]) -> Option<&'v Value> {
    let mut current = value;
    for segment in rest {
        current = current.as_object()?.get(segment.as_str())?;
    }
    Some(current)
}

fn value_ref(value: &Value) -> ProjectedValueRef<'_> {
    match value {
        Value::Null => ProjectedValueRef::Null,
        Value::Bool(value) => ProjectedValueRef::Bool(*value),
        Value::Number(crate::Number::Signed(value)) => ProjectedValueRef::Signed(*value),
        Value::Number(crate::Number::Unsigned(value)) => ProjectedValueRef::Unsigned(*value),
        Value::Number(crate::Number::Float(value)) => ProjectedValueRef::Float(*value),
        Value::String(value) => ProjectedValueRef::String(value),
        Value::Array(_) | Value::Object(_) => ProjectedValueRef::Owned(value.clone()),
    }
}

/// How rows are visited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Order {
    Any,
    Id(ScanDirection),
}

/// Visits the visible rows of a collection; stops when `emit` returns `false`.
fn walk<'s>(collection: &'s Collection, columns: Columns, layout: Option<&Layout<'_>>, order: Order, ids: bool, versions: bool, emit: &mut dyn FnMut(&mut Row<'s, '_>) -> StorageResult<bool>) -> StorageResult<()> {
    let reverse = order == Order::Id(ScanDirection::Reverse);
    let ids = ids || order != Order::Any;
    // Sources: `Some(segment index)`, or `None` for the delta.
    let mut sources = (0..collection.segments.len()).map(|index| (collection.segments[index].segment.min_id(), collection.segments[index].segment.max_id(), Some(index))).collect::<Vec<_>>();
    if let (Some(first), Some(last)) = (collection.delta.keys().next(), collection.delta.keys().next_back()) {
        sources.push((*first, *last, None));
    }
    let open = |source: Option<usize>| -> StorageResult<Cursor<'s>> {
        Ok(match source {
            Some(index) => Cursor::Segment(Box::new(SegmentCursor::new(&collection.segments[index], columns, layout, reverse, ids, versions, false)?)),
            None => {
                let mut rows = collection.delta.iter().map(|(key, row)| (*key, row)).collect::<Vec<_>>();
                if reverse {
                    rows.reverse();
                }
                Cursor::Delta { rows, at: 0 }
            }
        })
    };
    if order != Order::Any {
        sources.sort_unstable();
    }
    let merge = order != Order::Any && sources.windows(2).any(|pair| pair[1].0 <= pair[0].1);
    if !merge {
        // One source after the other, each opened only when reached.
        if reverse {
            sources.reverse();
        }
        for (_, _, source) in sources {
            let mut cursor = open(source)?;
            while !cursor.exhausted() {
                if !emit(&mut cursor.row())? {
                    return Ok(());
                }
                cursor.advance()?;
            }
        }
        return Ok(());
    }
    let mut cursors = sources.into_iter().map(|(_, _, source)| open(source)).collect::<StorageResult<Vec<_>>>()?;
    loop {
        let next = cursors.iter().enumerate().filter_map(|(index, cursor)| cursor.key().map(|key| (key, index)));
        let next = if reverse { next.max() } else { next.min() };
        let Some((_, index)) = next else { return Ok(()) };
        if !emit(&mut cursors[index].row())? {
            return Ok(());
        }
        cursors[index].advance()?;
    }
}

/// Visits the live rows of the segment parts `parts` and of `delta` in identifier order, for compaction: one row
/// at a time, where reading each segment whole first held all of them in memory.
pub(super) fn visit_live(parts: Vec<SegmentPart>, delta: BTreeMap<u128, DeltaRow>, emit: &mut dyn FnMut(u128, u64, Arc<Document>) -> StorageResult<()>) -> StorageResult<()> {
    let collection = Collection { segments: parts, delta: Arc::new(delta), ..Collection::default() };
    walk(&collection, Columns::All, None, Order::Id(ScanDirection::Forward), true, true, &mut |row| {
        emit(super::key(&row.id()), row.version().get(), row.document(None, None)?)?;
        Ok(true)
    })
}

/// Every live row of a segment, with its document when `documents` (history).
pub(super) fn part_rows(part: &SegmentPart, documents: bool, emit: &mut dyn FnMut(DocumentId, DocumentVersion, Option<Arc<Document>>) -> StorageResult<bool>) -> StorageResult<()> {
    let collection = Collection { segments: vec![part.clone()], ..Collection::default() };
    let none: [FieldPath; 0] = [];
    let layout = Layout::new(&none);
    let (columns, layout) = if documents { (Columns::All, None) } else { (Columns::Paths, Some(&layout)) };
    walk(&collection, columns, layout, Order::Id(ScanDirection::Forward), true, true, &mut |row| {
        let document = if documents { Some(row.document(None, None)?) } else { None };
        emit(row.id(), row.version(), document)
    })
}

/// An immutable view of the store at one generation.
#[derive(Debug)]
pub(super) struct Snapshot {
    state: Arc<State>,
}

impl Snapshot {
    pub(super) const fn new(state: Arc<State>) -> Self { Self { state } }

    pub(super) fn generation(&self) -> u64 { self.state.generation }

    pub(super) fn state(&self) -> &State { &self.state }

    fn collection(&self, collection: &CollectionId) -> Option<&Collection> { self.state.collections.get(collection).map(AsRef::as_ref) }

    /// Version of the live document `id`, its document left unread.
    pub(super) fn version(&self, collection: &CollectionId, id: &DocumentId, scratch: &mut Scratch) -> StorageResult<Option<DocumentVersion>> {
        self.collection(collection).map_or(Ok(None), |collection| collection.version(id, scratch))
    }

    /// Unordered walk honouring `options.limit()`.
    fn walk_limited<'s>(&'s self, collection: &CollectionId, options: ScanOptions, columns: Columns, layout: Option<&Layout<'_>>, order: Order, ids: bool, versions: bool, emit: &mut dyn FnMut(&mut Row<'s, '_>) -> StorageResult<bool>) -> StorageResult<()> {
        let Some(collection) = self.collection(collection) else { return Ok(()) };
        let mut remaining = options.limit().unwrap_or(usize::MAX);
        if remaining == 0 {
            return Ok(());
        }
        walk(collection, columns, layout, order, ids, versions, &mut |row| {
            remaining -= 1;
            Ok(emit(row)? && remaining > 0)
        })
    }
}

fn physical_kinds(counts: &[u64; super::FACTS]) -> Vec<PhysicalKind> {
    [PhysicalKind::Null, PhysicalKind::Bool, PhysicalKind::Number, PhysicalKind::String, PhysicalKind::Array, PhysicalKind::Object]
        .into_iter().zip(counts).filter(|(_, count)| **count > 0).map(|(kind, _)| kind).collect()
}

impl StorageRead for Snapshot {
    fn changed(&self, collection: &CollectionId) -> StorageResult<Option<u64>> {
        Ok(Some(self.collection(collection).map_or(0, |collection| collection.changed)))
    }

    fn support(&self, capability: StorageReadCapability) -> StorageSupport {
        match capability {
            StorageReadCapability::ProjectedValuesGatedUnordered => StorageSupport::Native,
        }
    }

    fn top_level_fields(&self, collection: &CollectionId) -> StorageResult<Option<Vec<String>>> {
        Ok(Some(self.collection(collection).map(|collection| collection.facts.keys().map(|name| name.as_str().to_owned()).collect()).unwrap_or_default()))
    }

    fn field_facts(&self, collection: &CollectionId) -> StorageResult<Option<FieldFacts>> {
        let Some(collection) = self.collection(collection) else { return Ok(Some(FieldFacts::default())) };
        let fields = collection.facts.iter().map(|(name, counts)| (name.as_str().to_owned(), FieldFact { present: counts.iter().sum(), kinds: physical_kinds(counts) })).collect::<BTreeMap<_, _>>();
        Ok(Some(FieldFacts::new(collection.live, fields)))
    }

    fn field_bounds(&self, collection: &CollectionId) -> StorageResult<Option<BTreeMap<String, FieldBounds>>> {
        let mut bounds = BTreeMap::<String, FieldBounds>::new();
        let Some(collection) = self.collection(collection) else { return Ok(Some(bounds)) };
        let (mut unknown, mut strings_unknown) = (BTreeSet::new(), BTreeSet::new());
        for part in &collection.segments {
            let segment = &part.segment;
            for (field, name) in segment.fields().iter().enumerate() {
                for group in 0..segment.group_count() {
                    if !segment.has_column(group, field) {
                        continue;
                    }
                    match segment.stats(group, field) {
                        None => { unknown.insert(name.as_str().to_owned()); }
                        Some(stats) => {
                            if stats.kinds & (1 << K_STRING) != 0 && stats.bounds.string.is_none() {
                                strings_unknown.insert(name.as_str().to_owned());
                            }
                            bounds.entry(name.as_str().to_owned()).or_default().merge(&stats.bounds);
                        }
                    }
                }
            }
        }
        for row in collection.delta.values() {
            for (name, value) in row.document.iter() {
                bounds.entry(name.as_str().to_owned()).or_default().add(value);
            }
        }
        for name in strings_unknown {
            if let Some(bounds) = bounds.get_mut(&name) {
                bounds.string = None;
            }
        }
        bounds.retain(|name, _| !unknown.contains(name));
        Ok(Some(bounds))
    }

    fn sample_row_ids(&self, collection: &CollectionId, count: usize, seed: u64) -> StorageResult<Option<(u64, Vec<(DocumentId, DocumentVersion)>)>> {
        let Some(collection) = self.collection(collection) else { return Ok(None) };
        let stored = collection.segments.iter().map(|part| part.segment.rows()).sum::<u64>() + collection.delta.len() as u64;
        // Positions are drawn among stored rows and dead ones redrawn: only
        // while most rows are live.
        if count as u64 >= collection.live || collection.live * 2 < stored {
            return Ok(None);
        }
        let mut state = seed;
        let mut next = || {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        };
        let delta = collection.delta.iter().collect::<Vec<_>>();
        let (mut drawn, mut rows) = (BTreeSet::new(), Vec::with_capacity(count));
        let mut scratch = Scratch::default();
        let mut ids = Vec::new();
        while rows.len() < count {
            let mut position = next() % stored;
            if !drawn.insert(position) {
                continue;
            }
            let part = collection.segments.iter().find(|part| {
                let found = position < part.segment.rows();
                if !found {
                    position -= part.segment.rows();
                }
                found
            });
            let Some(part) = part else {
                let (key, row) = delta[position as usize];
                rows.push((id_of(*key), DocumentVersion::new(row.version)));
                continue;
            };
            if part.dead.as_ref().is_some_and(|dead| dead.contains(position)) {
                continue;
            }
            let index = (position / segment::GROUP_ROWS as u64) as usize;
            let group = part.segment.group(index)?;
            let Some(offset) = position.checked_sub(group.first_row).map(|offset| offset as usize).filter(|offset| *offset < group.rows) else {
                return Ok(None);
            };
            group.ids.decode_block(offset / BLOCK, &mut ids, &mut scratch);
            let mut version = [0];
            group.versions.decode(offset, &mut version);
            rows.push((id_of(ids[offset % BLOCK]), DocumentVersion::new(version[0])));
        }
        Ok(Some((collection.live, rows)))
    }

    fn visit_row_ids(&self, collection: &CollectionId, visitor: &mut dyn FnMut(DocumentId, DocumentVersion) -> StorageResult<bool>) -> StorageResult<()> {
        self.walk_limited(collection, ScanOptions::default(), Columns::Paths, Some(&Layout::new(&[])), Order::Any, true, true, &mut |row| visitor(row.id(), row.version()))
    }

    fn get(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> {
        match self.collection(collection) {
            None => Ok(None),
            Some(collection) => collection.get(id, &mut Scratch::default()),
        }
    }

    fn get_each(&self, collection: &CollectionId, ids: &[DocumentId], visitor: &mut dyn FnMut(usize, Option<StoredDocument>) -> StorageResult<bool>) -> StorageResult<()> {
        match self.collection(collection) {
            Some(collection) => collection.get_each(ids, visitor),
            None => {
                for index in 0..ids.len() {
                    if !visitor(index, None)? {
                        break;
                    }
                }
                Ok(())
            }
        }
    }

    fn scan(&self, collection: &CollectionId, options: ScanOptions) -> StorageResult<Vec<StoredDocument>> {
        crate::helpers::collect_scan(|visit| self.scan_each(collection, options, visit))
    }

    fn scan_each(&self, collection: &CollectionId, options: ScanOptions, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> {
        self.walk_limited(collection, options, Columns::All, None, Order::Id(options.direction()), true, true, &mut |row| {
            visitor(StoredDocument::new(row.id(), row.version(), row.document(None, None)?)?)
        })
    }

    fn scan_projected_row_refs_unordered_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], visitor: &mut dyn for<'a> FnMut(DocumentId, DocumentVersion, &[Option<ProjectedValueRef<'a>>]) -> StorageResult<bool>) -> StorageResult<()> {
        let layout = Layout::new(fields);
        let mut refs = Vec::with_capacity(fields.len());
        self.walk_limited(collection, options, Columns::Paths, Some(&layout), Order::Any, true, true, &mut |row| {
            let (id, version) = (row.id(), row.version());
            row.with_refs(&layout, fields, &mut refs, |values| visitor(id, version, values))
        })
    }

    fn supports_projected_blocks(&self) -> bool { true }

    fn scan_projected_blocks_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], skip: &dyn Fn(&[Option<GroupFacts<'_>>]) -> bool, gate: Option<BlockGate<'_>>, visitor: &mut dyn for<'a> FnMut(&ProjectedBlock<'_, 'a>) -> StorageResult<bool>) -> StorageResult<()> {
        let layout = Layout::new(fields);
        let Some(collection) = self.collection(collection) else { return Ok(()) };
        let mut remaining = options.limit().unwrap_or(usize::MAX);
        if remaining == 0 {
            return Ok(());
        }
        // A refused block would still count against a scan limit.
        let mut gate = gate.filter(|_| options.limit().is_none());
        let mut ids = Vec::with_capacity(BLOCK);
        let mut columns = (0..fields.len()).map(|_| Vec::with_capacity(BLOCK)).collect::<Vec<Vec<Option<ProjectedValueRef<'_>>>>>();
        let mut codes = (0..fields.len()).map(|_| Vec::<u32>::with_capacity(BLOCK)).collect::<Vec<_>>();
        let mut coded = vec![None::<u64>; fields.len()];
        let locators = options.locators();
        for part in &collection.segments {
            let mut cursor = SegmentCursor::pruned(part, &layout, locators, options.number_codes(), gate.is_some(), skip)?;
            let paths = layout.paths.iter().zip(&cursor.leaves).map(|(path, leaf)| leaf.map_or(*path, |slot| (slot, &[][..]))).collect::<Vec<_>>();
            while !cursor.live.is_empty() {
                let live = &cursor.live[..cursor.live.len().min(remaining)];
                // Every row of the block is live: read the cells in place.
                let whole = live.len() == cursor.block.len;
                // The gate's columns first; the others only for a block it admits.
                let gated = gate.as_ref().map(|gate| gate.fields.clone());
                if let Some(gate) = gate.as_mut() {
                    let admitted = |path: usize| gate.fields[path];
                    read_paths(&mut cursor.block, part, &cursor.fields, &paths, live, whole, admitted, &mut columns, &mut codes, &mut coded)?;
                    let complete = paths.iter().all(|(slot, rest)| rest.is_empty() && cursor.block.uniform(*slot));
                    let (block_columns, block_codes) = project(&cursor.block, &paths, &columns, &codes, &coded, admitted);
                    if !(gate.admit)(&ProjectedBlock { rows: live.len(), ids: &[], columns: &block_columns, codes: &block_codes, complete })? {
                        cursor.fill()?;
                        continue;
                    }
                }
                read_paths(&mut cursor.block, part, &cursor.fields, &paths, live, whole, |path| gated.as_ref().is_none_or(|gated| !gated[path]), &mut columns, &mut codes, &mut coded)?;
                cursor.block.locate();
                let block = &cursor.block;
                ids.clear();
                if locators && whole {
                    ids.extend(block.ids[..block.len].iter().zip(&block.versions[..block.len]).map(|(id, version)| (id_of(*id), DocumentVersion::new(*version))));
                } else if locators {
                    ids.extend(live.iter().map(|offset| (id_of(block.ids[*offset]), DocumentVersion::new(block.versions[*offset]))));
                }
                let (block_columns, block_codes) = project(block, &paths, &columns, &codes, &coded, |_| true);
                remaining -= live.len();
                if !visitor(&ProjectedBlock { rows: live.len(), ids: &ids, columns: &block_columns, codes: &block_codes, complete: false })? || remaining == 0 {
                    return Ok(());
                }
                cursor.fill()?;
            }
        }
        let rows = collection.delta.iter().collect::<Vec<_>>();
        for chunk in rows.chunks(BLOCK) {
            let chunk = &chunk[..chunk.len().min(remaining)];
            ids.clear();
            if locators {
                ids.extend(chunk.iter().map(|(key, row)| (id_of(**key), DocumentVersion::new(row.version))));
            }
            for (column, path) in columns.iter_mut().zip(fields) {
                column.clear();
                column.extend(chunk.iter().map(|(_, row)| path.resolve_value(&row.document).map(value_ref)));
            }
            remaining -= chunk.len();
            let no_codes = vec![None; columns.len()];
            let block_columns = columns.iter().map(|column| BlockColumn::Values(column)).collect::<Vec<_>>();
            if !crate::storage::visit_gated(&mut gate, &ProjectedBlock { rows: chunk.len(), ids: &ids, columns: &block_columns, codes: &no_codes, complete: false }, visitor)? || remaining == 0 {
                return Ok(());
            }
        }
        Ok(())
    }

    fn scan_projected_row_values_gated_unordered_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], gate_field_count: usize, gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>, visitor: &mut dyn FnMut(DocumentId, DocumentVersion, &[Option<Value>]) -> StorageResult<bool>) -> StorageResult<()> {
        let gate_field_count = gate_field_count.min(fields.len());
        let layout = Layout::new(fields);
        let mut values = vec![None; fields.len()];
        self.walk_limited(collection, options, Columns::Paths, Some(&layout), Order::Any, true, true, &mut |row| {
            row.values(&layout, fields, 0..gate_field_count, &mut values)?;
            for value in &mut values[gate_field_count..] {
                *value = None;
            }
            if !gate(&values)? {
                return Ok(true);
            }
            row.values(&layout, fields, gate_field_count..fields.len(), &mut values)?;
            visitor(row.id(), row.version(), &values)
        })
    }

    fn scan_projected_gated_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> {
        let layout = Layout::new(fields);
        let mut values = vec![None; fields.len()];
        let Some(collection) = self.collection(collection) else { return Ok(()) };
        let mut remaining = options.limit().unwrap_or(usize::MAX);
        if remaining == 0 {
            return Ok(());
        }
        // Gate on the projected fields, then read the accepted documents whole.
        walk(collection, Columns::All, None, Order::Id(options.direction()), true, true, &mut |row| {
            match row {
                Row::Delta { row: delta, .. } => {
                    for (value, path) in values.iter_mut().zip(fields) {
                        *value = path.resolve_value(&delta.document).cloned();
                    }
                }
                Row::Segment { segment, block, at } => {
                    for (index, (slot, rest)) in layout.paths.iter().enumerate() {
                        let column = segment.field_index(layout.names[*slot]);
                        let value = match column { Some(column) => cell_value(block.cells[column][*at], Some(&mut block.caches[column]))?, None => None };
                        values[index] = if rest.is_empty() { value } else { value.and_then(|value| resolve(&value, rest).cloned()) };
                    }
                }
            }
            if !gate(&values)? {
                return Ok(true);
            }
            remaining -= 1;
            Ok(visitor(StoredDocument::new(row.id(), row.version(), row.document(None, None)?)?)? && remaining > 0)
        })
    }

    fn count(&self, collection: &CollectionId) -> StorageResult<u64> { Ok(self.collection(collection).map_or(0, |collection| collection.live)) }

    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> { Ok(self.count(collection)? > 0) }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        Ok(self.state.collections.iter().filter(|(_, collection)| collection.live > 0).map(|(name, _)| name.clone()).collect())
    }
}
