#![cfg_attr(rustfmt, rustfmt_skip)]
//! Immutable column segment files.
//!
//! ```text
//! "OG6SEG01" | chunk ... chunk | footer | footer_len u32 | footer checksum u64 | "OG6SEGFT"
//! ```
//! Rows are sorted by identifier and split into row groups of up to
//! [`GROUP_ROWS`] rows. Each group stores one identifier chunk, one version
//! chunk and one chunk per field (absent when no row of the group holds it).
//! The footer lists field names once, per-group chunk locations with their
//! checksums, and exact per-field kind counts.
//!
//! Scalar leaves of object fields (`client.code`) also get a chunk of their
//! own, holding each row's value at that path, so a read of the path skips
//! the encoded objects. A leaf is listed from the first group holding it on;
//! a group without its chunk holds no value at that path.

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{atomic::{AtomicBool, Ordering}, Arc},
};

use super::{
    codec::{checksum, corrupt, put_bytes, put_u128, put_u32, put_u64, put_var, Reader},
    column::{fact_index, write_column, write_ids, ChunkStats, ColumnReader, IdsReader, Scratch, FACTS},
    encoding::{write_ints, Ints},
};
use crate::{model::{Document, FieldName, Value}, storage::{DocumentId, DocumentVersion, StorageError, StorageResult}};

const MAGIC: [u8; 8] = *b"OG6SEG01";
const TRAILER_MAGIC: [u8; 8] = *b"OG6SEGFT";
const TRAILER_BYTES: usize = 4 + 8 + 8;
/// Version 2 adds per-chunk statistics, version 3 leaf chunks; older
/// segments read without them.
const FOOTER_VERSION: u8 = 3;
pub(super) const GROUP_ROWS: usize = 65_536;
/// Leaves listed per segment, and their depth below the top-level field.
const MAX_LEAVES: usize = 256;
const MAX_LEAF_DEPTH: usize = 8;

pub(super) fn io_error<'a>(operation: &'static str, path: &'a Path) -> impl FnOnce(std::io::Error) -> StorageError + 'a {
    move |error| StorageError::backend(format!("Glacier {operation} {}: {error}", path.display()))
}

#[derive(Clone, Copy, Debug)]
struct ChunkRef {
    offset: u64,
    len: u32,
    checksum: u64,
}

#[derive(Debug)]
struct GroupMeta {
    rows: u32,
    first_row: u64,
    min_id: u128,
    max_id: u128,
    ids: ChunkRef,
    versions: ChunkRef,
    /// Field chunks, then (once read) leaf chunks.
    columns: Vec<Option<ChunkRef>>,
    /// Statistics of each column chunk (absent from version 1 footers).
    stats: Vec<Option<ChunkStats>>,
    /// Leaf chunks and their statistics, while writing.
    leaf_columns: Vec<Option<ChunkRef>>,
    leaf_stats: Vec<Option<ChunkStats>>,
}

/// A scalar leaf of an object field: the field, then the names below it.
#[derive(Debug)]
struct Leaf {
    field: usize,
    path: Vec<FieldName>,
}

/// Value at `path` below `value`, walking objects only (as
/// [`super::codec::value_at_path`] does).
fn value_below<'v>(value: &'v Value, path: &[FieldName]) -> Option<&'v Value> {
    path.iter().try_fold(value, |value, name| value.as_object()?.get(name.as_str()))
}

/// Lists in `leaves` each path below `value` that holds a scalar, up to the
/// limits.
fn discover_leaves(field: usize, value: &Value, prefix: &mut Vec<FieldName>, leaves: &mut Vec<Leaf>) {
    let Some(object) = value.as_object() else { return };
    for (name, value) in object.iter() {
        prefix.push(name.clone());
        match value {
            Value::Object(_) if prefix.len() < MAX_LEAF_DEPTH => discover_leaves(field, value, prefix, leaves),
            Value::Object(_) | Value::Array(_) => {}
            _ if leaves.len() < MAX_LEAVES && !leaves.iter().any(|leaf| leaf.field == field && leaf.path == *prefix) => leaves.push(Leaf { field, path: prefix.clone() }),
            _ => {}
        }
        prefix.pop();
    }
}

/// Writes rows, strictly increasing by identifier, into a new segment file.
#[derive(Debug)]
pub(super) struct SegmentWriter {
    path: PathBuf,
    file: BufWriter<File>,
    offset: u64,
    collection: String,
    fields: Vec<FieldName>,
    facts: Vec<[u64; FACTS]>,
    leaves: Vec<Leaf>,
    groups: Vec<GroupMeta>,
    pending: Vec<(u128, u64, Arc<Document>)>,
    rows: u64,
}

impl SegmentWriter {
    pub(super) fn create(path: &Path, collection: &str) -> StorageResult<Self> {
        let file = File::options().write(true).create_new(true).open(path).map_err(io_error("create segment", path))?;
        let mut file = BufWriter::with_capacity(1 << 20, file);
        file.write_all(&MAGIC).map_err(io_error("write segment", path))?;
        Ok(Self { path: path.to_owned(), file, offset: MAGIC.len() as u64, collection: collection.to_owned(), fields: Vec::new(), facts: Vec::new(), leaves: Vec::new(), groups: Vec::new(), pending: Vec::with_capacity(GROUP_ROWS), rows: 0 })
    }

    #[must_use]
    pub(super) const fn rows(&self) -> u64 { self.rows + self.pending.len() as u64 }

    pub(super) fn push(&mut self, id: DocumentId, version: DocumentVersion, document: Arc<Document>) -> StorageResult<()> {
        let id = u128::from_be_bytes(id.into_bytes());
        let last = self.pending.last().map(|row| row.0).or_else(|| self.groups.last().map(|group| group.max_id));
        if last.is_some_and(|last| last >= id) {
            return Err(StorageError::backend("Glacier segment rows must be strictly increasing by identifier"));
        }
        self.pending.push((id, version.get(), document));
        if self.pending.len() == GROUP_ROWS {
            self.flush_group()?;
        }
        Ok(())
    }

    fn write_chunk(&mut self, bytes: &[u8]) -> StorageResult<ChunkRef> {
        let chunk = ChunkRef { offset: self.offset, len: u32::try_from(bytes.len()).map_err(|_| StorageError::backend("Glacier chunk too large"))?, checksum: checksum(bytes) };
        self.file.write_all(bytes).map_err(io_error("write segment", &self.path))?;
        self.offset += bytes.len() as u64;
        Ok(chunk)
    }

    fn flush_group(&mut self) -> StorageResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let rows = std::mem::take(&mut self.pending);
        let mut bytes = Vec::new();
        write_ids(&rows.iter().map(|row| row.0).collect::<Vec<_>>(), &mut bytes);
        let ids = self.write_chunk(&bytes)?;
        bytes.clear();
        write_ints(&rows.iter().map(|row| row.1).collect::<Vec<_>>(), &mut bytes);
        let versions = self.write_chunk(&bytes)?;

        for (_, _, document) in &rows {
            for (name, _) in document.iter() {
                if !self.fields.iter().any(|field| field == name) {
                    self.fields.push(name.clone());
                    self.facts.push([0; FACTS]);
                }
            }
        }
        let mut columns = Vec::with_capacity(self.fields.len());
        let mut stats = Vec::with_capacity(self.fields.len());
        let mut cells = Vec::<Option<&Value>>::with_capacity(rows.len());
        for index in 0..self.fields.len() {
            let name = self.fields[index].as_str();
            cells.clear();
            cells.extend(rows.iter().map(|(_, _, document)| document.get(name)));
            if cells.iter().all(Option::is_none) {
                columns.push(None);
                stats.push(None);
                continue;
            }
            for value in cells.iter().flatten() {
                self.facts[index][fact_index(value)] += 1;
            }
            bytes.clear();
            stats.push(Some(write_column(&cells, &mut bytes)));
            columns.push(Some(self.write_chunk(&bytes)?));
            let mut prefix = Vec::new();
            for value in cells.iter().flatten() {
                discover_leaves(index, value, &mut prefix, &mut self.leaves);
            }
        }
        let mut leaf_columns = Vec::with_capacity(self.leaves.len());
        let mut leaf_stats = Vec::with_capacity(self.leaves.len());
        for index in 0..self.leaves.len() {
            let Leaf { field, path } = &self.leaves[index];
            let name = self.fields[*field].as_str();
            cells.clear();
            cells.extend(rows.iter().map(|(_, _, document)| document.get(name).and_then(|value| value_below(value, path))));
            if cells.iter().all(Option::is_none) {
                leaf_columns.push(None);
                leaf_stats.push(None);
                continue;
            }
            bytes.clear();
            leaf_stats.push(Some(write_column(&cells, &mut bytes)));
            leaf_columns.push(Some(self.write_chunk(&bytes)?));
        }
        let first_row = self.rows;
        self.rows += rows.len() as u64;
        self.groups.push(GroupMeta { rows: rows.len() as u32, first_row, min_id: rows[0].0, max_id: rows[rows.len() - 1].0, ids, versions, columns, stats, leaf_columns, leaf_stats });
        self.pending = rows;
        self.pending.clear();
        Ok(())
    }

    /// Writes the footer and syncs the file. Returns `None` for an empty segment
    /// (the file is removed).
    pub(super) fn finish(mut self) -> StorageResult<Option<u64>> {
        self.flush_group()?;
        if self.rows == 0 {
            drop(self.file);
            std::fs::remove_file(&self.path).map_err(io_error("remove empty segment", &self.path))?;
            return Ok(None);
        }
        let mut footer = Vec::new();
        footer.push(FOOTER_VERSION);
        put_bytes(&mut footer, self.collection.as_bytes());
        put_var(&mut footer, self.rows);
        put_var(&mut footer, self.fields.len() as u64);
        for (field, facts) in self.fields.iter().zip(&self.facts) {
            put_bytes(&mut footer, field.as_str().as_bytes());
            for count in facts {
                put_var(&mut footer, *count);
            }
        }
        put_var(&mut footer, self.leaves.len() as u64);
        for leaf in &self.leaves {
            put_var(&mut footer, leaf.field as u64);
            put_var(&mut footer, leaf.path.len() as u64);
            for name in &leaf.path {
                put_bytes(&mut footer, name.as_str().as_bytes());
            }
        }
        put_var(&mut footer, self.groups.len() as u64);
        for group in &self.groups {
            put_u32(&mut footer, group.rows);
            put_u128(&mut footer, group.min_id);
            put_u128(&mut footer, group.max_id);
            for chunk in [Some(group.ids), Some(group.versions)].iter().chain(group.columns.iter()) {
                match chunk {
                    Some(chunk) => { put_u64(&mut footer, chunk.offset); put_u32(&mut footer, chunk.len); put_u64(&mut footer, chunk.checksum); }
                    None => { put_u64(&mut footer, 0); put_u32(&mut footer, 0); put_u64(&mut footer, 0); }
                }
            }
            for _ in group.columns.len()..self.fields.len() {
                put_u64(&mut footer, 0); put_u32(&mut footer, 0); put_u64(&mut footer, 0);
            }
            for leaf in 0..self.leaves.len() {
                match group.leaf_columns.get(leaf).copied().flatten() {
                    Some(chunk) => { put_u64(&mut footer, chunk.offset); put_u32(&mut footer, chunk.len); put_u64(&mut footer, chunk.checksum); }
                    None => { put_u64(&mut footer, 0); put_u32(&mut footer, 0); put_u64(&mut footer, 0); }
                }
            }
            for stats in (0..self.fields.len()).map(|field| group.stats.get(field)).chain((0..self.leaves.len()).map(|leaf| group.leaf_stats.get(leaf))) {
                match stats.and_then(Option::as_ref) {
                    Some(stats) => { footer.push(1); stats.encode(&mut footer); }
                    None => footer.push(0),
                }
            }
        }
        let mut trailer = Vec::with_capacity(TRAILER_BYTES);
        put_u32(&mut trailer, footer.len() as u32);
        put_u64(&mut trailer, checksum(&footer));
        trailer.extend_from_slice(&TRAILER_MAGIC);
        self.file.write_all(&footer).map_err(io_error("write segment", &self.path))?;
        self.file.write_all(&trailer).map_err(io_error("write segment", &self.path))?;
        let file = self.file.into_inner().map_err(|error| StorageError::backend(format!("Glacier flush segment: {error}")))?;
        file.sync_all().map_err(io_error("sync segment", &self.path))?;
        Ok(Some(self.rows))
    }
}

#[derive(Debug)]
enum Bytes {
    #[cfg(all(target_pointer_width = "64", target_family = "unix"))]
    Mapped(mmap_guard::FileData),
    #[cfg_attr(all(target_pointer_width = "64", target_family = "unix"), allow(dead_code))]
    Owned(Vec<u8>),
}

impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            #[cfg(all(target_pointer_width = "64", target_family = "unix"))]
            Self::Mapped(data) => data,
            Self::Owned(data) => data,
        }
    }
}

/// An open, immutable segment.
#[derive(Debug)]
pub(super) struct Segment {
    number: u64,
    path: PathBuf,
    collection: String,
    bytes: Bytes,
    rows: u64,
    fields: Vec<FieldName>,
    /// Leaves; leaf `i` is column `fields.len() + i`.
    leaves: Vec<Leaf>,
    groups: Vec<GroupMeta>,
    verified: Vec<AtomicBool>,
}

/// Identifier, version and field readers of one row group.
#[derive(Clone, Copy, Debug)]
pub(super) struct Group<'a> {
    pub(super) rows: usize,
    pub(super) first_row: u64,
    pub(super) ids: IdsReader<'a>,
    pub(super) versions: Ints<'a>,
}

impl Segment {
    pub(super) fn open(path: &Path, number: u64) -> StorageResult<Arc<Self>> {
        #[cfg(all(target_pointer_width = "64", target_family = "unix"))]
        let bytes = Bytes::Mapped(mmap_guard::map_file(path).map_err(io_error("map segment", path))?);
        #[cfg(not(all(target_pointer_width = "64", target_family = "unix")))]
        let bytes = Bytes::Owned(std::fs::read(path).map_err(io_error("read segment", path))?);
        if bytes.len() < MAGIC.len() + TRAILER_BYTES || bytes[..8] != MAGIC || bytes[bytes.len() - 8..] != TRAILER_MAGIC {
            return Err(corrupt("segment magic"));
        }
        let mut trailer = Reader::new(&bytes[bytes.len() - TRAILER_BYTES..]);
        let footer_len = trailer.u32()? as usize;
        let footer_checksum = trailer.u64()?;
        let footer_start = (bytes.len() - TRAILER_BYTES).checked_sub(footer_len).filter(|start| *start >= MAGIC.len()).ok_or_else(|| corrupt("footer length"))?;
        let footer = &bytes[footer_start..bytes.len() - TRAILER_BYTES];
        if checksum(footer) != footer_checksum {
            return Err(corrupt("segment footer checksum"));
        }
        let mut reader = Reader::new(footer);
        let version = reader.u8()?;
        if !(1..=FOOTER_VERSION).contains(&version) {
            return Err(corrupt("segment footer version"));
        }
        let collection = reader.str()?.to_owned();
        let rows = reader.var()?;
        let field_count = reader.len()?;
        let mut fields = Vec::with_capacity(field_count.min(footer.len()));
        for _ in 0..field_count {
            fields.push(FieldName::from(reader.str()?));
            for _ in 0..FACTS {
                reader.var()?;
            }
        }
        let mut leaves = Vec::new();
        if version >= 3 {
            for _ in 0..reader.len()? {
                let field = reader.len()?;
                if field >= field_count {
                    return Err(corrupt("leaf field"));
                }
                let path = (0..reader.len()?).map(|_| reader.str().map(FieldName::from)).collect::<StorageResult<Vec<_>>>()?;
                leaves.push(Leaf { field, path });
            }
        }
        let column_count = field_count + leaves.len();
        let group_count = reader.len()?;
        let mut groups = Vec::with_capacity(group_count.min(footer.len()));
        let mut first_row = 0u64;
        let chunk = |reader: &mut Reader<'_>| -> StorageResult<Option<ChunkRef>> {
            let chunk = ChunkRef { offset: reader.u64()?, len: reader.u32()?, checksum: reader.u64()? };
            if chunk.len == 0 {
                return Ok(None);
            }
            let end = chunk.offset.checked_add(u64::from(chunk.len)).ok_or_else(|| corrupt("chunk range"))?;
            if chunk.offset < MAGIC.len() as u64 || end > footer_start as u64 {
                return Err(corrupt("chunk range"));
            }
            Ok(Some(chunk))
        };
        for _ in 0..group_count {
            let rows = reader.u32()?;
            let min_id = reader.u128()?;
            let max_id = reader.u128()?;
            let ids = chunk(&mut reader)?.ok_or_else(|| corrupt("identifier chunk"))?;
            let versions = chunk(&mut reader)?.ok_or_else(|| corrupt("version chunk"))?;
            let columns = (0..column_count).map(|_| chunk(&mut reader)).collect::<StorageResult<Vec<_>>>()?;
            let stats = if version >= 2 {
                (0..column_count).map(|_| if reader.u8()? == 0 { Ok(None) } else { ChunkStats::decode(&mut reader).map(Some) }).collect::<StorageResult<Vec<_>>>()?
            } else {
                vec![None; column_count]
            };
            groups.push(GroupMeta { rows, first_row, min_id, max_id, ids, versions, columns, stats, leaf_columns: Vec::new(), leaf_stats: Vec::new() });
            first_row += u64::from(rows);
        }
        if first_row != rows || groups.windows(2).any(|pair| pair[0].max_id >= pair[1].min_id) {
            return Err(corrupt("segment row groups"));
        }
        let verified = (0..group_count * (column_count + 2)).map(|_| AtomicBool::new(false)).collect();
        Ok(Arc::new(Self { number, path: path.to_owned(), collection, bytes, rows, fields, leaves, groups, verified }))
    }

    #[must_use] pub(super) const fn number(&self) -> u64 { self.number }
    #[must_use] pub(super) fn path(&self) -> &Path { &self.path }
    #[must_use] pub(super) fn collection(&self) -> &str { &self.collection }
    #[must_use] pub(super) const fn rows(&self) -> u64 { self.rows }
    #[must_use] pub(super) fn fields(&self) -> &[FieldName] { &self.fields }
    #[must_use] pub(super) fn group_count(&self) -> usize { self.groups.len() }
    #[must_use] pub(super) fn min_id(&self) -> u128 { self.groups.first().map_or(0, |group| group.min_id) }
    #[must_use] pub(super) fn max_id(&self) -> u128 { self.groups.last().map_or(0, |group| group.max_id) }
    #[must_use] pub(super) fn field_index(&self, name: &str) -> Option<usize> { self.fields.iter().position(|field| field.as_str() == name) }

    /// Column of the leaf at `rest` below field `name`, when the segment has it.
    #[must_use]
    pub(super) fn leaf_index(&self, name: &str, rest: &[crate::model::FieldPathSegment]) -> Option<usize> {
        let field = self.field_index(name)?;
        let leaf = self.leaves.iter().position(|leaf| leaf.field == field && leaf.path.len() == rest.len() && leaf.path.iter().zip(rest).all(|(name, segment)| name.as_str() == segment.as_str()))?;
        Some(self.fields.len() + leaf)
    }

    fn chunk(&self, group: usize, slot: usize, chunk: ChunkRef) -> StorageResult<&[u8]> {
        let bytes = &self.bytes[chunk.offset as usize..chunk.offset as usize + chunk.len as usize];
        let flag = &self.verified[group * (self.fields.len() + self.leaves.len() + 2) + slot];
        if !flag.load(Ordering::Relaxed) {
            if checksum(bytes) != chunk.checksum {
                return Err(StorageError::backend(format!("Glacier corrupt data: chunk checksum in {}", self.path.display())));
            }
            flag.store(true, Ordering::Relaxed);
        }
        Ok(bytes)
    }

    /// Checks every chunk against its checksum (a read checks a chunk only
    /// when it first needs it); returns the chunks and bytes checked.
    pub(super) fn verify(&self) -> StorageResult<(u64, u64)> {
        let (mut chunks, mut bytes) = (0u64, 0u64);
        for (index, meta) in self.groups.iter().enumerate() {
            let refs = [Some(meta.ids), Some(meta.versions)].into_iter().chain(meta.columns.iter().copied());
            for (slot, chunk) in refs.enumerate() {
                let Some(chunk) = chunk else { continue };
                self.chunk(index, slot, chunk)?;
                chunks += 1;
                bytes += u64::from(chunk.len);
            }
        }
        Ok((chunks, bytes))
    }

    pub(super) fn group(&self, index: usize) -> StorageResult<Group<'_>> {
        let meta = &self.groups[index];
        let ids = IdsReader::open(self.chunk(index, 0, meta.ids)?)?;
        let versions = Ints::read(&mut Reader::new(self.chunk(index, 1, meta.versions)?), meta.rows as usize)?;
        if ids.rows() != meta.rows as usize {
            return Err(corrupt("identifier chunk rows"));
        }
        Ok(Group { rows: meta.rows as usize, first_row: meta.first_row, ids, versions })
    }

    /// Reader of a field's chunk in a group; `None` when no row holds it.
    pub(super) fn column(&self, group: usize, field: usize) -> StorageResult<Option<ColumnReader<'_>>> {
        let meta = &self.groups[group];
        let Some(chunk) = meta.columns.get(field).copied().flatten() else {
            return Ok(None);
        };
        let column = ColumnReader::open(self.chunk(group, field + 2, chunk)?)?;
        if column.rows() != meta.rows as usize {
            return Err(corrupt("column chunk rows"));
        }
        Ok(Some(column))
    }

    /// Rows, first row and stored bytes (identifiers, versions and every chunk) of a group.
    #[must_use]
    pub(super) fn group_bytes(&self, group: usize) -> (usize, u64, u64) {
        let meta = &self.groups[group];
        let chunks: u64 = meta.columns.iter().flatten().map(|chunk| u64::from(chunk.len)).sum();
        (meta.rows as usize, meta.first_row, u64::from(meta.ids.len) + u64::from(meta.versions.len) + chunks)
    }

    /// Whether a field has a chunk in a group (some row of it holds the field).
    #[must_use]
    pub(super) fn has_column(&self, group: usize, field: usize) -> bool {
        self.groups[group].columns.get(field).is_some_and(Option::is_some)
    }

    /// Statistics of a field's chunk in a group; `None` when the chunk is
    /// absent or the segment predates statistics.
    #[must_use]
    pub(super) fn stats(&self, group: usize, field: usize) -> Option<&ChunkStats> {
        self.groups[group].stats.get(field).and_then(Option::as_ref)
    }

    /// Group and row (segment-wide) holding `id`.
    pub(super) fn find(&self, id: u128, scratch: &mut Scratch) -> StorageResult<Option<u64>> {
        let Some(index) = self.group_holding(id) else {
            return Ok(None);
        };
        Ok(self.group(index)?.ids.find(id, scratch).map(|row| self.groups[index].first_row + row as u64))
    }

    /// Group whose identifier range holds `id`, if any.
    #[must_use]
    pub(super) fn group_holding(&self, id: u128) -> Option<usize> {
        let index = self.groups.partition_point(|group| group.max_id < id);
        self.groups.get(index).filter(|group| group.min_id <= id).map(|_| index)
    }

    /// First segment-wide row and row count of a group.
    #[must_use]
    pub(super) fn group_rows(&self, index: usize) -> (u64, usize) {
        self.groups.get(index).map_or((0, 0), |group| (group.first_row, group.rows as usize))
    }

    /// Group index of a segment-wide row.
    #[must_use]
    pub(super) fn group_of(&self, row: u64) -> usize {
        self.groups.partition_point(|group| group.first_row + u64::from(group.rows) <= row)
    }

    /// Bytes of the file (for size accounting).
    #[must_use]
    pub(super) fn file_bytes(&self) -> u64 { self.bytes.len() as u64 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::backend::glacier::column::{cell_value, Cell, RankCursor, BLOCK};

    #[test] fn segment_round_trips_rows_and_fields() { let dir = std::env::temp_dir().join(format!("og-g6-seg-{}", std::process::id())); let _ = std::fs::remove_dir_all(&dir); std::fs::create_dir_all(&dir).unwrap(); let path = dir.join("1.g6"); let mut writer = SegmentWriter::create(&path, "ana").unwrap(); let total = GROUP_ROWS + 1500; let mut ids = Vec::new(); for row in 0..total { let id = DocumentId::synthetic(1, row as u64); ids.push(id); } ids.sort(); for (row, id) in ids.iter().enumerate() { let mut document = Document::new(); document.insert("n", Value::signed(row as i64)); if row > GROUP_ROWS { document.insert("late", Value::string("x")); } writer.push(*id, DocumentVersion::new(1 + (row % 3) as u64), Arc::new(document)).unwrap(); } assert_eq!(writer.finish().unwrap(), Some(total as u64)); let segment = Segment::open(&path, 1).unwrap(); assert_eq!(segment.rows(), total as u64); assert_eq!(segment.group_count(), 2); let late = segment.field_index("late").unwrap(); assert!(segment.column(0, late).unwrap().is_none()); let mut scratch = Scratch::default(); for (row, id) in ids.iter().enumerate().step_by(997) { let found = segment.find(u128::from_be_bytes(*id.as_bytes()), &mut scratch).unwrap().unwrap(); assert_eq!(found, row as u64); let group = segment.group_of(found); let local = (found - segment.group(group).unwrap().first_row) as usize; let column = segment.column(group, segment.field_index("n").unwrap()).unwrap().unwrap(); assert_eq!(cell_value(column.get_ranked(local, &mut RankCursor::default()).unwrap(), None).unwrap(), Some(Value::signed(row as i64))); assert_eq!(segment.group(group).unwrap().versions.get(local), 1 + (row % 3) as u64); } let column = segment.column(1, late).unwrap().unwrap(); let mut cells = vec![Cell::Missing; BLOCK]; column.decode_block(0, &mut cells, &mut scratch).unwrap(); assert_eq!(cells[0], Cell::Missing); assert!(matches!(cells[1], Cell::Str("x", _))); std::fs::remove_dir_all(&dir).unwrap(); }

    #[test] fn segment_rejects_unordered_rows() { let dir = std::env::temp_dir().join(format!("og-g6-seg-order-{}", std::process::id())); let _ = std::fs::remove_dir_all(&dir); std::fs::create_dir_all(&dir).unwrap(); let mut writer = SegmentWriter::create(&dir.join("1.g6"), "c").unwrap(); let id = DocumentId::synthetic(1, 2); writer.push(id, DocumentVersion::INITIAL, Arc::new(Document::new())).unwrap(); assert!(writer.push(id, DocumentVersion::INITIAL, Arc::new(Document::new())).is_err()); std::fs::remove_dir_all(&dir).unwrap(); }
}
