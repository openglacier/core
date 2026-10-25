#![cfg_attr(rustfmt, rustfmt_skip)]
//! Glacier: a column-segment storage backend.
//!
//! A store is a directory:
//! - `MANIFEST`: the live segments of every collection, their deleted rows and
//!   exact per-field facts, at one generation (see [`manifest`]);
//! - `<n>.g6` (`n` in 16 hexadecimal digits; `.seg` in older stores): immutable column segments, rows sorted by identifier, split in
//!   row groups of 65 536 rows, each field encoded on its own (see [`segment`]);
//! - `wal-<n>.log`: commits after the manifest generation (see [`wal`]);
//! - `LOCK`: held (`flock`, exclusive) by the one handle that has the store
//!   open.
//!
//! A store has a `MANIFEST` from its creation on: data files without one
//! are refused, never taken for a new store.
//!
//! Small commits are appended to the log, synced, and kept in an in-memory
//! delta. When the delta grows past 65 536 rows (or the log past 64 MiB), it is written as a segment and a new manifest is
//! installed; the log then restarts empty. A commit putting at least
//! 16 384 documents skips the log and is written as segments right
//! away. Replacing or deleting a segment row marks it dead; segments with
//! many dead rows, and runs of small segments, are rewritten on flush.
//!
//! Readers take an immutable snapshot of the whole state (`Arc<State>`); a
//! commit publishes a new state.

mod codec;
mod column;
mod encoding;
mod history;
mod manifest;
mod read;
mod segment;
mod tx;
mod usage;
mod wal;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock},
};

use self::{
    column::{cell_value, ColumnReader, RankCursor, Scratch, FACTS, fact_index},
    manifest::{sync_dir, DeadRows, Manifest, ManifestCollection, ManifestSegment, MANIFEST},
    segment::{io_error, Group, Segment, SegmentWriter},
    wal::{wal_path, LogEntry, Wal},
};
use super::{
    super::{
        common::{ensure_version, next_generation}, history::{temporal_read, CommitRecord, HistoryEvent, HistoryOp, HistorySource, HistoryWriter}, CollectionId, CommitResult, DocumentId, DocumentVersion, StorageError,
        StorageEngine, StorageMutation, StorageRead, StorageResult, StorageTransaction, StoredDocument, Temporal, TemporalCache,
    },
};
use crate::model::{Document, FieldName};

/// On-disk format version of Glacier stores.
pub const GLACIER_FORMAT_VERSION: u16 = 6;

/// Write-path thresholds.
#[derive(Clone, Copy, Debug)]
struct Limits {
    /// Delta rows that trigger a flush to a segment.
    flush_rows: usize,
    /// Log bytes that trigger a flush.
    flush_wal_bytes: u64,
    /// Puts in one commit from which the commit is written straight to segments.
    direct_rows: usize,
    /// Rows per segment written by a flush or a compaction.
    segment_rows: u64,
    /// Segments under this many rows are merged once `merge_small` of them pile up.
    small_segment_rows: u64,
    merge_small: usize,
}

const LIMITS: Limits = Limits { flush_rows: 65_536, flush_wal_bytes: 64 << 20, direct_rows: 16_384, segment_rows: 1 << 20, small_segment_rows: 1 << 18, merge_small: 8 };

#[inline]
fn key(id: &DocumentId) -> u128 { u128::from_be_bytes(*id.as_bytes()) }

#[inline]
fn id_of(key: u128) -> DocumentId { DocumentId::from_bytes(key.to_be_bytes()) }

/// A live document not yet written to a segment.
#[derive(Clone, Debug)]
pub(super) struct DeltaRow {
    version: u64,
    document: Arc<Document>,
}

/// A segment with its deleted rows at one generation.
#[derive(Clone, Debug)]
pub(super) struct SegmentPart {
    segment: Arc<Segment>,
    dead: Option<Arc<DeadRows>>,
}

impl SegmentPart {
    fn live(&self) -> u64 { self.segment.rows() - self.dead.as_ref().map_or(0, |dead| dead.count()) }
}

/// One collection at one generation.
#[derive(Clone, Debug, Default)]
pub(super) struct Collection {
    /// Sorted by smallest identifier.
    segments: Vec<SegmentPart>,
    /// Whether two segments' identifier ranges overlap.
    overlapping: bool,
    delta: Arc<BTreeMap<u128, DeltaRow>>,
    live: u64,
    facts: Arc<BTreeMap<FieldName, [u64; FACTS]>>,
    /// The generation of the last commit that changed it (since the store
    /// was opened: the opened generation for what was already there).
    changed: u64,
}

/// Where a live document is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Location {
    Delta,
    Segment { part: usize, row: u64 },
}

/// One row group of a segment opened for reads by identifier.
struct OpenGroup<'s> {
    segment: &'s Segment,
    index: usize,
    group: Group<'s>,
    columns: Vec<Option<ColumnReader<'s>>>,
    cursors: Vec<RankCursor>,
}

impl<'s> OpenGroup<'s> {
    /// `open`, reopened on group `index` of `segment` unless already there.
    fn at<'o>(open: &'o mut Option<Self>, segment: &'s Segment, index: usize) -> StorageResult<&'o mut Self> {
        if open.as_ref().is_none_or(|group| !std::ptr::eq(group.segment, segment) || group.index != index) {
            let columns = (0..segment.fields().len()).map(|field| segment.column(index, field)).collect::<StorageResult<Vec<_>>>()?;
            *open = Some(Self { segment, index, group: segment.group(index)?, cursors: vec![RankCursor::default(); columns.len()], columns });
        }
        Ok(open.as_mut().expect("group opened above"))
    }

    /// Version and document of a group row.
    fn read(&mut self, local: usize) -> StorageResult<(u64, Document)> {
        let mut document = Document::new();
        for ((column, cursor), name) in self.columns.iter().zip(&mut self.cursors).zip(self.segment.fields()) {
            if let Some(column) = column {
                if let Some(value) = cell_value(column.get_ranked(local, cursor)?, None)? {
                    document.insert(name.clone(), value);
                }
            }
        }
        Ok((self.group.versions.get(local), document))
    }

    /// Counts into `facts` the field and fact slot of each value of a group row.
    fn count_facts(&mut self, local: usize, facts: &mut BTreeMap<FieldName, [u64; FACTS]>) -> StorageResult<()> {
        for ((column, cursor), name) in self.columns.iter().zip(&mut self.cursors).zip(self.segment.fields()) {
            if let Some(fact) = column.as_ref().map(|column| column.fact(local, cursor)).transpose()?.flatten() {
                facts.entry(name.clone()).or_insert([0; FACTS])[fact] += 1;
            }
        }
        Ok(())
    }
}

impl Collection {
    fn locate(&self, id: u128, scratch: &mut Scratch) -> StorageResult<Option<Location>> {
        if self.delta.contains_key(&id) {
            return Ok(Some(Location::Delta));
        }
        let end = self.segments.partition_point(|part| part.segment.min_id() <= id);
        let candidates = if self.overlapping { 0..end } else { end.saturating_sub(1)..end };
        for part in candidates.rev() {
            let segment = &self.segments[part];
            if segment.segment.max_id() < id {
                continue;
            }
            if let Some(row) = segment.segment.find(id, scratch)? {
                if !segment.dead.as_ref().is_some_and(|dead| dead.contains(row)) {
                    return Ok(Some(Location::Segment { part, row }));
                }
            }
        }
        Ok(None)
    }

    /// Version of the live row of `id`, its document left unread.
    fn version(&self, id: &DocumentId, scratch: &mut Scratch) -> StorageResult<Option<DocumentVersion>> {
        let key = key(id);
        Ok(match self.locate(key, scratch)? {
            None => None,
            Some(Location::Delta) => Some(DocumentVersion::new(self.delta[&key].version)),
            Some(Location::Segment { part, row }) => {
                let segment = &self.segments[part].segment;
                let group = segment.group(segment.group_of(row))?;
                Some(DocumentVersion::new(group.versions.get((row - group.first_row) as usize)))
            }
        })
    }

    /// The group of a segment row, opened through `open` (ascending rows
    /// of one group share its columns), and the row's place in it.
    fn open_row<'s, 'o>(&'s self, part: usize, row: u64, open: &'o mut Option<OpenGroup<'s>>) -> StorageResult<(&'o mut OpenGroup<'s>, usize)> {
        let segment = &self.segments[part].segment;
        let group = OpenGroup::at(open, segment, segment.group_of(row))?;
        let local = (row - group.group.first_row) as usize;
        Ok((group, local))
    }

    /// Rows by identifier, visited in the order given with their position
    /// (`None` for an identifier without a live row): ascending
    /// identifiers read like a sparse scan.
    fn get_each(&self, ids: &[DocumentId], visitor: &mut dyn FnMut(usize, Option<StoredDocument>) -> StorageResult<bool>) -> StorageResult<()> {
        let mut scratch = Scratch::default();
        let mut open = None;
        for (index, id) in ids.iter().enumerate() {
            if !visitor(index, self.get_through(id, &mut open, &mut scratch)?)? {
                break;
            }
        }
        Ok(())
    }

    fn get(&self, id: &DocumentId, scratch: &mut Scratch) -> StorageResult<Option<StoredDocument>> {
        self.get_through(id, &mut None, scratch)
    }

    fn get_through<'s>(&'s self, id: &DocumentId, open: &mut Option<OpenGroup<'s>>, scratch: &mut Scratch) -> StorageResult<Option<StoredDocument>> {
        let key = key(id);
        let (version, document) = match self.locate(key, scratch)? {
            None => return Ok(None),
            Some(Location::Delta) => (self.delta[&key].version, self.delta[&key].document.clone()),
            Some(Location::Segment { part, row }) => {
                let (group, local) = self.open_row(part, row, open)?;
                group.read(local).map(|(version, document)| (version, Arc::new(document)))?
            }
        };
        Ok(Some(StoredDocument::new(*id, DocumentVersion::new(version), document)?))
    }

    fn max_id(&self) -> Option<u128> {
        let segments = self.segments.iter().map(|part| part.segment.max_id()).max();
        let delta = self.delta.keys().next_back().copied();
        segments.max(delta)
    }

    fn refresh_overlap(&mut self) {
        self.segments.sort_by_key(|part| part.segment.min_id());
        self.overlapping = self.segments.windows(2).any(|pair| pair[1].segment.min_id() <= pair[0].segment.max_id());
    }
}

/// Field and fact slot of each value of `document`.
fn facts_of(document: &Document) -> impl Iterator<Item = (FieldName, usize)> + '_ {
    document.iter().map(|(name, value)| (name.clone(), fact_index(value)))
}

fn add_facts(facts: &mut BTreeMap<FieldName, [u64; FACTS]>, values: impl IntoIterator<Item = (FieldName, usize)>) {
    for (name, fact) in values {
        facts.entry(name).or_insert([0; FACTS])[fact] += 1;
    }
}

/// The whole store at one generation.
#[derive(Clone, Debug, Default)]
pub(super) struct State {
    generation: u64,
    collections: BTreeMap<CollectionId, Arc<Collection>>,
}

/// A commit, prepared against generation `generation`.
#[derive(Debug)]
pub(super) struct Commit {
    generation: u64,
    entries: Vec<LogEntry>,
    result: CommitResult,
}

/// Effect of a commit on one document.
#[derive(Debug)]
struct Change {
    /// Where the removed version is.
    removed: Option<Location>,
    put: Option<DeltaRow>,
}

/// The changes of a commit to one collection, and the facts of the versions they remove, counted together: a
/// list per removed row would hold a commit of a million deletes in memory several times over.
#[derive(Debug, Default)]
struct Planned {
    changes: BTreeMap<u128, Change>,
    removed: BTreeMap<FieldName, [u64; FACTS]>,
}

/// The writes a transaction moved out of memory: its puts written to
/// segments of their own, what they remove marked. The commit installs them.
#[derive(Debug, Default)]
struct Spilled {
    collections: BTreeMap<CollectionId, SpilledCollection>,
    /// The deletes, for the history.
    deletes: Vec<LogEntry>,
}

#[derive(Debug, Default)]
struct SpilledCollection {
    segments: Vec<u64>,
    /// Rows removed, per segment part of the transaction's snapshot.
    dead: BTreeMap<usize, DeadRows>,
    delta: BTreeSet<u128>,
    facts: BTreeMap<FieldName, [i64; FACTS]>,
    live: i64,
}

impl SpilledCollection {
    fn count(&mut self, facts: impl IntoIterator<Item = (FieldName, usize)>, by: i64) {
        self.live += by;
        facts.into_iter().for_each(|(field, fact)| self.facts.entry(field).or_default()[fact] += by);
    }

    /// Takes away `rows` removed versions and their facts.
    fn uncount(&mut self, removed: &BTreeMap<FieldName, [u64; FACTS]>, rows: i64) {
        self.live -= rows;
        for (field, counts) in removed {
            let slots = self.facts.entry(field.clone()).or_default();
            slots.iter_mut().zip(counts).for_each(|(slot, count)| *slot -= i64::try_from(*count).unwrap_or(i64::MAX));
        }
    }
}

impl Spilled {
    /// Moves `entries`, planned against `state`, out of memory.
    fn add(&mut self, backend: &GlacierBackend, state: &State, entries: &[LogEntry], scratch: &mut Scratch) -> StorageResult<()> {
        let mut writer = backend.writer()?;
        let history = backend.inner.history.get().is_some();
        for (name, Planned { changes, removed }) in plan(state, entries, scratch)? {
            let spilled = self.collections.entry(name.clone()).or_default();
            let mut sink = SegmentSink::new(&backend.inner.dir, &name, backend.inner.limits.segment_rows);
            spilled.uncount(&removed, changes.values().filter(|change| change.removed.is_some()).count() as i64);
            for (key, change) in changes {
                if let Some(location) = change.removed {
                    let fresh = match location {
                        Location::Delta => spilled.delta.insert(key),
                        Location::Segment { part, row } => spilled.dead.entry(part).or_insert_with(|| DeadRows::with_rows(state.collections[&name].segments[part].segment.rows())).insert(row),
                    };
                    if !fresh {
                        return Err(StorageError::transaction_conflict("a transaction past its memory writes a document twice"));
                    }
                    if change.put.is_none() && history {
                        self.deletes.push(LogEntry::Delete { collection: name.clone(), id: id_of(key) });
                    }
                }
                if let Some(put) = change.put {
                    spilled.count(facts_of(&put.document), 1);
                    sink.push(&mut writer, key, put.version, put.document)?;
                }
            }
            spilled.segments.extend(sink.finish(&mut writer)?);
        }
        Ok(())
    }

    /// Removes the segments of a transaction that does not commit.
    fn discard(&self, backend: &GlacierBackend) {
        for number in self.collections.values().flat_map(|spilled| &spilled.segments) {
            let _ = std::fs::remove_file(segment_path(&backend.inner.dir, *number));
        }
    }
}

/// Cumulative write-path counters.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct GlacierWriteMetrics {
    pub commits: u64,
    pub wal_commits: u64,
    pub direct_commits: u64,
    pub flushes: u64,
    pub compactions: u64,
    pub segments_written: u64,
    pub rows_written: u64,
    pub segment_bytes_written: u64,
    pub flush_failures: u64,
}

#[derive(Debug)]
struct Writer {
    wal: Wal,
    wal_sequence: u64,
    /// Generation of the manifest the log follows: its commits come after it.
    wal_base: u64,
    next_file: u64,
    metrics: GlacierWriteMetrics,
}

#[derive(Debug)]
struct Inner {
    dir: PathBuf,
    /// The open `LOCK` file: its exclusive lock lasts as long as the store.
    _lock: std::fs::File,
    limits: Limits,
    state: RwLock<Arc<State>>,
    writer: Mutex<Writer>,
    /// History recording, once enabled (see [`history`]).
    history: OnceLock<history::History>,
    /// Stored bytes per key value, per segment (see [`usage`]).
    usage: Mutex<usage::UsageCache>,
}

/// Glacier column-segment backend. Cloning shares the same store.
#[derive(Clone, Debug)]
pub struct GlacierBackend {
    inner: Arc<Inner>,
}

/// What [`GlacierBackend::verify`] checked, and the segments found damaged.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct GlacierVerification {
    pub generation: u64,
    pub segments: u64,
    pub chunks: u64,
    pub bytes: u64,
    pub damaged: Vec<String>,
}

/// Size and shape of a Glacier store.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct GlacierStats {
    pub generation: u64,
    pub documents: u64,
    pub segments: u64,
    pub segment_rows: u64,
    pub dead_rows: u64,
    pub delta_rows: u64,
    pub segment_bytes: u64,
    pub wal_bytes: u64,
}

/// A Glacier store of format 5 (ogd 1.x) is one file starting with `OGGLACR\0`; format 6 is a directory. Said as such,
/// rather than as a directory that cannot be created.
fn refuse_single_file_store(path: &Path) -> StorageResult<()> {
    if !path.is_file() { return Ok(()); }
    let mut magic = [0u8; 8];
    let format_5 = std::fs::File::open(path).and_then(|mut file| std::io::Read::read_exact(&mut file, &mut magic)).is_ok() && magic == *b"OGGLACR\0";
    Err(StorageError::backend(if format_5 {
        format!("{} is a Glacier store of format 5 (ogd 1.x): this ogd reads format {GLACIER_FORMAT_VERSION} only", path.display())
    } else {
        format!("{} is a file: a Glacier store is a directory", path.display())
    }))
}

impl GlacierBackend {
    /// Opens or creates a store directory.
    pub fn open(dir: impl AsRef<Path>) -> StorageResult<Self> { Self::open_with(dir.as_ref(), LIMITS) }

    fn open_with(dir: &Path, limits: Limits) -> StorageResult<Self> {
        refuse_single_file_store(dir)?;
        let dir = dir.to_owned();
        std::fs::create_dir_all(&dir).map_err(io_error("create store directory", &dir))?;
        let lock = lock_store(&dir)?;
        let manifest = match Manifest::load(&dir)? {
            Some(manifest) => manifest,
            None => {
                // New (or created before the manifest came first, with only
                // its first log): the manifest is written now.
                refuse_files_without_manifest(&dir)?;
                let manifest = Manifest { generation: 0, next_file: 1, wal_sequence: 1, collections: Vec::new() };
                manifest.store(&dir)?;
                manifest
            }
        };
        let mut state = State { generation: manifest.generation, collections: BTreeMap::new() };
        let mut referenced = BTreeSet::new();
        for collection in &manifest.collections {
            let mut segments = Vec::with_capacity(collection.segments.len());
            for entry in &collection.segments {
                let segment = Segment::open(&segment_path(&dir, entry.number), entry.number)?;
                if entry.dead.as_ref().is_some_and(|dead| dead.count() > segment.rows()) {
                    return Err(StorageError::backend("Glacier corrupt data: dead rows exceed segment rows"));
                }
                referenced.insert(entry.number);
                segments.push(SegmentPart { segment, dead: entry.dead.clone() });
            }
            let live = segments.iter().map(SegmentPart::live).sum();
            let mut state_collection = Collection { segments, overlapping: false, delta: Arc::default(), live, facts: Arc::new(collection.facts.clone()), changed: manifest.generation };
            state_collection.refresh_overlap();
            state.collections.insert(collection.name.clone(), Arc::new(state_collection));
        }
        let recorded = history::History::load(&dir)?;
        remove_unreferenced(&dir, &referenced, manifest.wal_sequence, recorded.as_ref());

        let log = wal_path(&dir, manifest.wal_sequence);
        let (records, valid_len) = wal::replay_store_log(&log, manifest.generation)?;
        let mut scratch = Scratch::default();
        for (generation, entries) in records {
            if generation <= state.generation {
                continue;
            }
            if generation != state.generation + 1 {
                return Err(StorageError::backend("Glacier corrupt data: log generations are not consecutive"));
            }
            let changes = plan(&state, &entries, &mut scratch)?;
            apply(&mut state, changes, generation);
            state.generation = generation;
        }
        let created = !log.exists();
        let wal = Wal::open(&log, valid_len)?;
        if created {
            sync_dir(&dir)?;
        }
        let writer = Writer { wal, wal_sequence: manifest.wal_sequence, wal_base: manifest.generation, next_file: manifest.next_file, metrics: GlacierWriteMetrics::default() };
        let history = OnceLock::new();
        if let Some(recorded) = recorded {
            recorded.recover(state.generation);
            let _ = history.set(recorded);
        }
        Ok(Self { inner: Arc::new(Inner { dir, _lock: lock, limits, state: RwLock::new(Arc::new(state)), writer: Mutex::new(writer), history, usage: Mutex::default() }) })
    }

    fn state(&self) -> StorageResult<Arc<State>> {
        self.inner.state.read().map(|state| Arc::clone(&state)).map_err(|_| StorageError::backend("Glacier state lock poisoned"))
    }

    fn writer(&self) -> StorageResult<MutexGuard<'_, Writer>> {
        self.inner.writer.lock().map_err(|_| StorageError::backend("Glacier writer lock poisoned"))
    }

    /// Current committed generation.
    pub fn generation(&self) -> StorageResult<u64> { Ok(self.state()?.generation) }

    /// Reads every chunk of every live segment and checks it, so a damaged
    /// segment is found before a query needs it. The manifest and the log
    /// were checked when the store opened.
    pub fn verify(&self) -> StorageResult<GlacierVerification> {
        let state = self.state()?;
        let mut report = GlacierVerification { generation: state.generation, ..GlacierVerification::default() };
        for collection in state.collections.values() {
            for part in &collection.segments {
                match part.segment.verify() {
                    Ok((chunks, bytes)) => {
                        report.chunks += chunks;
                        report.bytes += bytes;
                    }
                    Err(error) => report.damaged.push(error.to_string()),
                }
                report.segments += 1;
            }
        }
        Ok(report)
    }

    /// Visible documents across all collections.
    pub fn document_count(&self) -> StorageResult<u64> { Ok(self.state()?.collections.values().map(|collection| collection.live).sum()) }

    /// The store directory.
    #[must_use]
    pub fn path(&self) -> &Path { &self.inner.dir }

    /// Write-path counters since open.
    pub fn write_metrics(&self) -> StorageResult<GlacierWriteMetrics> { Ok(self.writer()?.metrics) }

    /// Size and shape of the store.
    pub fn stats(&self) -> StorageResult<GlacierStats> {
        let state = self.state()?;
        let wal_bytes = self.writer()?.wal.bytes();
        let mut stats = GlacierStats { generation: state.generation, wal_bytes, ..GlacierStats::default() };
        for collection in state.collections.values() {
            stats.documents += collection.live;
            stats.delta_rows += collection.delta.len() as u64;
            for part in &collection.segments {
                stats.segments += 1;
                stats.segment_rows += part.segment.rows();
                stats.dead_rows += part.dead.as_ref().map_or(0, |dead| dead.count());
                stats.segment_bytes += part.segment.file_bytes();
            }
        }
        Ok(stats)
    }

    /// Writes every delta row to segments and installs a new manifest.
    pub fn checkpoint(&self) -> StorageResult<()> {
        let mut writer = self.writer()?;
        let state = self.state()?;
        if state.collections.values().all(|collection| collection.delta.is_empty()) && writer.wal.bytes() == 0 {
            return Ok(());
        }
        self.flush(&mut writer, &state, None)
    }

    /// Starts keeping history (see [`history`]): writes pending rows to
    /// segments, links the live segments as the baseline and records every
    /// later commit. Recording then stays on across restarts (remove `hist/`
    /// to stop it).
    ///
    /// # Errors
    ///
    /// Fails when the baseline cannot be written.
    pub fn record_history(&self) -> StorageResult<()> {
        if self.inner.history.get().is_some_and(history::History::journal) {
            return Err(StorageError::backend("Glacier: this store keeps a backup journal, not history; remove its hist/ directory to start history (the next backup is then a full one)"));
        }
        if self.inner.history.get().is_some() {
            return Ok(());
        }
        let mut writer = self.writer()?;
        if self.inner.history.get().is_some() {
            return Ok(());
        }
        let state = self.state()?;
        if state.collections.values().any(|collection| !collection.delta.is_empty()) || writer.wal.bytes() > 0 {
            self.flush(&mut writer, &state, None)?;
        }
        let state = self.state()?;
        let recorded = history::History::enable(&self.inner.dir, &state)?;
        let _ = self.inner.history.set(recorded);
        Ok(())
    }

    /// Starts a backup journal (see [`history`]): every later commit is kept
    /// like history, without a baseline nor commit times, until a backup
    /// releases it; `as of` stays unavailable. Ignored when history is kept.
    ///
    /// # Errors
    ///
    /// Fails when the journal cannot be started.
    pub fn record_journal(&self) -> StorageResult<()> {
        let _writer = self.writer()?;
        if self.inner.history.get().is_none() {
            let _ = self.inner.history.set(history::History::enable_journal(&self.inner.dir, self.state()?.generation)?);
        }
        Ok(())
    }

    /// Whether this store keeps history (a backup journal is not history).
    #[must_use]
    pub fn keeps_history(&self) -> bool { self.inner.history.get().is_some_and(|recorded| !recorded.journal()) }

    /// The current state with its history source (under the writer lock, so
    /// that no log is retired while the files are listed).
    fn history_source(&self) -> StorageResult<(Arc<State>, history::Source)> {
        let recorded = self.inner.history.get().ok_or_else(StorageError::history_unavailable)?;
        let writer = self.writer()?;
        let state = self.state()?;
        let live = wal::replay(&wal_path(&self.inner.dir, writer.wal_sequence))?.0.into_iter().filter(|record| record.0 <= state.generation).collect();
        let source = recorded.source(Arc::clone(&state), live)?;
        drop(writer);
        Ok((state, source))
    }

    /// Imports rows into an empty collection, straight into segments.
    /// `source` pushes documents, strictly increasing by identifier, into
    /// the sink it receives; versions are kept.
    pub fn import(&self, collection: &CollectionId, source: &mut dyn FnMut(&mut dyn FnMut(StoredDocument) -> StorageResult<()>) -> StorageResult<()>) -> StorageResult<u64> {
        let mut writer = self.writer()?;
        let state = self.state()?;
        if state.collections.get(collection).is_some_and(|existing| existing.live > 0) {
            return Err(StorageError::backend(format!("Glacier import target {collection} is not empty")));
        }
        let mut facts = BTreeMap::new();
        let mut sink = SegmentSink::new(&self.inner.dir, collection, self.inner.limits.segment_rows);
        let mut total = 0u64;
        source(&mut |row| {
            add_facts(&mut facts, facts_of(row.document()));
            let (id, version, document) = row.into_parts();
            sink.push(&mut writer, key(&id), version.get(), document)?;
            total += 1;
            Ok(())
        })?;
        let numbers = sink.finish(&mut writer)?;
        sync_dir(&self.inner.dir)?;
        let mut next = (*state).clone();
        next.generation = next_generation(state.generation)?;
        let mut target = Collection { facts: Arc::new(facts), live: total, changed: next.generation, ..Collection::default() };
        for number in numbers {
            target.segments.push(SegmentPart { segment: Segment::open(&segment_path(&self.inner.dir, number), number)?, dead: None });
        }
        target.refresh_overlap();
        next.collections.insert(collection.clone(), Arc::new(target));
        let generation = next.generation;
        if let Some(recorded) = self.inner.history.get() {
            for part in &next.collections[collection].segments {
                recorded.link_segment(&self.inner.dir, part.segment.number(), generation)?;
            }
            recorded.sync()?;
        }
        self.install(&mut writer, next, Vec::new())?;
        if let Some(recorded) = self.inner.history.get() {
            recorded.record(generation);
        }
        Ok(total)
    }

    /// Commits `entries` as generation `generation`, the next one or a later
    /// one: a gap is first made durable by a flush naming the generation before.
    fn commit_at(&self, generation: u64, entries: Vec<LogEntry>) -> StorageResult<()> {
        let current = self.generation()?;
        if generation <= current {
            return Err(StorageError::backend(format!("history: generation {generation} is not after {current}")));
        }
        if generation > current + 1 {
            self.set_generation(generation - 1)?;
        }
        self.commit(Commit { generation: generation - 1, entries, result: CommitResult::default() }).map(drop)
    }

    /// Moves the store to `generation` through a flush, whose manifest names it.
    fn set_generation(&self, generation: u64) -> StorageResult<()> {
        let mut writer = self.writer()?;
        let mut next = (*self.state()?).clone();
        next.generation = generation;
        for collection in next.collections.values_mut().filter(|collection| collection.changed > generation) {
            Arc::make_mut(collection).changed = generation;
        }
        self.flush(&mut writer, &Arc::new(next), None)
    }

    /// Commits prepared entries if the store is still at `commit.generation`.
    fn commit(&self, commit: Commit) -> StorageResult<CommitResult> {
        if commit.entries.is_empty() {
            return Ok(CommitResult::default());
        }
        let mut writer = self.writer()?;
        let current = self.state()?;
        if current.generation != commit.generation {
            return Err(StorageError::transaction_conflict("Glacier generation changed before commit"));
        }
        let generation = next_generation(commit.generation)?;
        let mut scratch = Scratch::default();
        let changes = plan(&current, &commit.entries, &mut scratch)?;
        writer.metrics.commits += 1;
        let puts = commit.entries.iter().filter(|entry| matches!(entry, LogEntry::Put { .. })).count();
        if puts >= self.inner.limits.direct_rows {
            writer.metrics.direct_commits += 1;
            self.flush(&mut writer, &current, Some(changes))?;
            if let Some(recorded) = self.inner.history.get() {
                recorded.record(generation);
            }
            return Ok(commit.result);
        }
        writer.wal.append(generation, &commit.entries)?;
        writer.metrics.wal_commits += 1;
        drop(current);
        let flush_due = {
            let mut guard = self.inner.state.write().map_err(|_| StorageError::backend("Glacier state lock poisoned"))?;
            let state = Arc::make_mut(&mut guard);
            apply(state, changes, generation);
            state.generation = generation;
            state.collections.values().map(|collection| collection.delta.len()).sum::<usize>() >= self.inner.limits.flush_rows || writer.wal.bytes() >= self.inner.limits.flush_wal_bytes
        };
        if let Some(recorded) = self.inner.history.get() {
            recorded.record(generation);
        }
        if flush_due {
            // The commit is durable in the log: a failed flush is retried by
            // the next commit rather than reported as a failed commit.
            let state = self.state()?;
            if self.flush(&mut writer, &state, None).is_err() {
                writer.metrics.flush_failures += 1;
            }
        }
        Ok(commit.result)
    }

    /// Installs what a transaction moved out of memory ([`Spilled`]) if the
    /// store is still at `generation`, with the delta rows (see [`Self::flush`]).
    fn commit_spilled(&self, generation: u64, spilled: Spilled, result: CommitResult) -> StorageResult<CommitResult> {
        let mut writer = self.writer()?;
        let current = self.state()?;
        if current.generation != generation {
            spilled.discard(self);
            return Err(StorageError::transaction_conflict("Glacier generation changed before commit"));
        }
        let mut next = (*current).clone();
        next.generation = next_generation(generation)?;
        let recorded = self.inner.history.get();
        if let Some(recorded) = recorded {
            recorded.forget(next.generation);
            recorded.write_deletes(next.generation, &spilled.deletes)?;
        }
        for (name, spill) in spilled.collections {
            let collection = Arc::make_mut(next.collections.entry(name).or_default());
            collection.changed = next.generation;
            collection.live = collection.live.saturating_add_signed(spill.live);
            let facts = Arc::make_mut(&mut collection.facts);
            for (field, counts) in spill.facts {
                let slots = facts.entry(field).or_insert([0; FACTS]);
                slots.iter_mut().zip(counts).for_each(|(slot, by)| *slot = slot.saturating_add_signed(by));
            }
            facts.retain(|_, counts| counts.iter().any(|count| *count > 0));
            // The parts as the transaction saw them, before new ones sort among them.
            for (part, dead) in spill.dead {
                let part = &mut collection.segments[part];
                Arc::make_mut(part.dead.get_or_insert_with(|| Arc::new(DeadRows::with_rows(part.segment.rows())))).add(&dead);
            }
            let delta = Arc::make_mut(&mut collection.delta);
            spill.delta.iter().for_each(|key| { delta.remove(key); });
            for number in spill.segments {
                if let Some(recorded) = recorded {
                    recorded.link_segment(&self.inner.dir, number, next.generation)?;
                }
                collection.segments.push(SegmentPart { segment: Segment::open(&segment_path(&self.inner.dir, number), number)?, dead: None });
            }
            collection.refresh_overlap();
        }
        if let Some(recorded) = recorded {
            recorded.sync()?;
        }
        next.collections.retain(|_, collection| collection.live > 0 || !collection.segments.is_empty());
        let generation = next.generation;
        self.flush(&mut writer, &Arc::new(next), None)?;
        if let Some(recorded) = recorded {
            recorded.record(generation);
        }
        Ok(result)
    }

    /// Writes delta rows (plus `changes`, applied at the next generation) to
    /// segments, compacts, installs the manifest and publishes the state.
    fn flush(&self, writer: &mut Writer, base: &Arc<State>, changes: Option<BTreeMap<CollectionId, Planned>>) -> StorageResult<()> {
        let mut next = (**base).clone();
        // With history, the puts of a direct commit go to their own segments
        // (kept in `hist/`), and its deletes to a log there.
        let recorded = self.inner.history.get().filter(|_| changes.is_some());
        let mut direct_puts = BTreeMap::<CollectionId, BTreeSet<u128>>::new();
        if let Some(changes) = changes {
            next.generation = next_generation(base.generation)?;
            if let Some(recorded) = recorded {
                recorded.forget(next.generation);
                let mut deletes = Vec::new();
                for (name, planned) in &changes {
                    for (key, change) in &planned.changes {
                        match &change.put {
                            Some(_) => { direct_puts.entry(name.clone()).or_default().insert(*key); }
                            None if change.removed.is_some() => deletes.push(LogEntry::Delete { collection: name.clone(), id: id_of(*key) }),
                            None => {}
                        }
                    }
                }
                recorded.write_deletes(next.generation, &deletes)?;
            }
            let generation = next.generation;
            apply(&mut next, changes, generation);
        }
        let mut history_segments = Vec::new();
        writer.metrics.flushes += 1;
        let mut obsolete = Vec::new();
        for (name, collection) in &mut next.collections {
            let limits = self.inner.limits;
            let small = collection.segments.iter().filter(|part| part.segment.rows() < limits.small_segment_rows).count();
            let victims = collection.segments.iter().enumerate().filter(|(_, part)| {
                let dead = part.dead.as_ref().map_or(0, |dead| dead.count());
                dead * 2 >= part.segment.rows() || (small >= limits.merge_small && part.segment.rows() < limits.small_segment_rows)
            }).map(|(index, _)| index).collect::<Vec<_>>();
            if collection.delta.is_empty() && victims.is_empty() {
                continue;
            }
            if !victims.is_empty() {
                writer.metrics.compactions += 1;
            }
            let commit_keys = direct_puts.get(name);
            let delta = collection.delta.iter().filter(|(key, _)| !commit_keys.is_some_and(|keys| keys.contains(key))).map(|(key, row)| (*key, row.clone())).collect();
            let mut sink = SegmentSink::new(&self.inner.dir, name, limits.segment_rows);
            read::visit_live(victims.iter().map(|index| collection.segments[*index].clone()).collect(), delta, &mut |key, version, document| sink.push(writer, key, version, document))?;
            let mut numbers = sink.finish(writer)?;
            if let Some(keys) = commit_keys {
                let mut sink = SegmentSink::new(&self.inner.dir, name, limits.segment_rows);
                for (key, row) in keys.iter().filter_map(|key| collection.delta.get(key).map(|row| (*key, row))) {
                    sink.push(writer, key, row.version, row.document.clone())?;
                }
                let own = sink.finish(writer)?;
                history_segments.extend(own.iter().copied());
                numbers.extend(own);
            }
            let collection = Arc::make_mut(collection);
            for index in victims.iter().rev() {
                obsolete.push(collection.segments.remove(*index).segment);
            }
            for number in numbers {
                collection.segments.push(SegmentPart { segment: Segment::open(&segment_path(&self.inner.dir, number), number)?, dead: None });
            }
            collection.delta = Arc::default();
            collection.refresh_overlap();
        }
        next.collections.retain(|_, collection| collection.live > 0 || !collection.segments.is_empty());
        sync_dir(&self.inner.dir)?;
        if let Some(recorded) = recorded {
            for number in history_segments {
                recorded.link_segment(&self.inner.dir, number, next.generation)?;
            }
            recorded.sync()?;
        }
        self.install(writer, next, obsolete)
    }

    /// Installs the manifest of `next` (no delta rows), restarts the log and
    /// publishes `next`.
    fn install(&self, writer: &mut Writer, next: State, obsolete: Vec<Arc<Segment>>) -> StorageResult<()> {
        debug_assert!(next.collections.values().all(|collection| collection.delta.is_empty()));
        let wal_sequence = writer.wal_sequence + 1;
        let manifest = Manifest {
            generation: next.generation,
            next_file: writer.next_file,
            wal_sequence,
            collections: next.collections.iter().map(|(name, collection)| ManifestCollection {
                name: name.clone(),
                segments: collection.segments.iter().map(|part| ManifestSegment { number: part.segment.number(), dead: part.dead.clone() }).collect(),
                facts: (*collection.facts).clone(),
            }).collect(),
        };
        // The next log exists, durably, before the manifest names it: a
        // failure before the manifest leaves the store as it was, writing to
        // the log the current manifest replays.
        let next_log = wal_path(&self.inner.dir, wal_sequence);
        let next_wal = Wal::open(&next_log, 0)?;
        if let Err(error) = sync_dir(&self.inner.dir).and_then(|()| manifest.store(&self.inner.dir)) {
            drop(next_wal);
            let _ = std::fs::remove_file(&next_log);
            return Err(error);
        }
        let old_log = wal_path(&self.inner.dir, writer.wal_sequence);
        let old_base = writer.wal_base;
        writer.wal = next_wal;
        writer.wal_sequence = wal_sequence;
        writer.wal_base = next.generation;
        match self.inner.history.get() {
            Some(recorded) => recorded.archive_wal(&old_log, old_base),
            None => { let _ = std::fs::remove_file(old_log); }
        }
        {
            let mut guard = self.inner.state.write().map_err(|_| StorageError::backend("Glacier state lock poisoned"))?;
            *guard = Arc::new(next);
        }
        for segment in obsolete {
            let _ = std::fs::remove_file(segment.path());
        }
        Ok(())
    }

    /// Prepares the mutations of one collection against `snapshot` (inserts
    /// must be new, replaces must match their precondition).
    fn prepare_batch(&self, state: &State, collection: &CollectionId, mutations: Vec<StorageMutation>, keep: bool) -> StorageResult<(Commit, Vec<StoredDocument>)> {
        let target = state.collections.get(collection);
        let max_id = target.and_then(|collection| collection.max_id());
        let mut scratch = Scratch::default();
        let mut staged = BTreeMap::<u128, StoredDocument>::new();
        let mut entries = Vec::with_capacity(mutations.len());
        let mut returned = Vec::with_capacity(if keep { mutations.len() } else { 0 });
        let (mut inserted, mut replaced) = (0u64, 0u64);
        for mutation in mutations {
            let stored = match mutation {
                StorageMutation::Insert { id, document } => {
                    let key = key(&id);
                    let exists = staged.contains_key(&key) || (max_id.is_some_and(|max| key <= max) && target.map(|target| target.locate(key, &mut scratch)).transpose()?.flatten().is_some());
                    if exists {
                        return Err(StorageError::document_already_exists(collection.clone(), id));
                    }
                    inserted += 1;
                    StoredDocument::new(id, DocumentVersion::INITIAL, document)?
                }
                StorageMutation::Replace { id, document, precondition } => {
                    let key = key(&id);
                    let current = match staged.get(&key) {
                        Some(current) => current.version(),
                        None => target.map(|target| target.get(&id, &mut scratch)).transpose()?.flatten().ok_or_else(|| StorageError::document_not_found(collection.clone(), id))?.version(),
                    };
                    ensure_version(collection, &id, current, precondition)?;
                    replaced += 1;
                    StoredDocument::new(id, current.next()?, document)?
                }
            };
            entries.push(LogEntry::Put { collection: collection.clone(), id: *stored.id(), version: stored.version(), document: stored.shared_document() });
            if keep {
                returned.push(stored.clone());
            }
            staged.insert(key(stored.id()), stored);
        }
        Ok((Commit { generation: state.generation, entries, result: CommitResult::new(inserted, replaced, 0) }, returned))
    }
}

/// `<number as 16 hex digits>.g6`. Older stores hold `<number as 16 hex digits>.seg`, or `<number as 8 decimal
/// digits>.seg` before the hexadecimal naming: those files are still found, and kept under their name.
fn segment_path(dir: &Path, number: u64) -> PathBuf {
    let path = dir.join(format!("{number:016x}.g6"));
    if path.exists() {
        return path;
    }
    [format!("{number:016x}.seg"), format!("{number:08}.seg")].into_iter().map(|name| dir.join(name)).find(|legacy| legacy.exists()).unwrap_or(path)
}

/// The segment number a file name stands for (`None` when it is not a segment file).
fn segment_number(name: &str) -> Option<u64> {
    let (stem, extension) = name.rsplit_once('.')?;
    match extension {
        "g6" if stem.len() == 16 => u64::from_str_radix(stem, 16).ok(),
        "seg" if stem.len() == 16 => u64::from_str_radix(stem, 16).ok(),
        "seg" => stem.parse().ok(),
        _ => None,
    }
}

/// Streams sorted rows into segments of up to `segment_rows` rows.
struct SegmentSink<'a> {
    dir: &'a Path,
    segment_rows: u64,
    collection: &'a CollectionId,
    current: Option<(u64, SegmentWriter)>,
    numbers: Vec<u64>,
}

impl<'a> SegmentSink<'a> {
    const fn new(dir: &'a Path, collection: &'a CollectionId, segment_rows: u64) -> Self { Self { dir, segment_rows, collection, current: None, numbers: Vec::new() } }

    fn push(&mut self, writer: &mut Writer, key: u128, version: u64, document: Arc<Document>) -> StorageResult<()> {
        if self.current.as_ref().is_some_and(|(_, segment)| segment.rows() >= self.segment_rows) {
            self.close(writer)?;
        }
        if self.current.is_none() {
            let number = writer.next_file;
            writer.next_file += 1;
            self.current = Some((number, SegmentWriter::create(&segment_path(self.dir, number), self.collection.as_str())?));
        }
        match self.current.as_mut() {
            Some((_, segment)) => segment.push(id_of(key), DocumentVersion::new(version), document),
            None => Ok(()),
        }
    }

    fn close(&mut self, writer: &mut Writer) -> StorageResult<()> {
        let Some((number, segment)) = self.current.take() else { return Ok(()) };
        if let Some(rows) = segment.finish()? {
            writer.metrics.segments_written += 1;
            writer.metrics.rows_written += rows;
            writer.metrics.segment_bytes_written += std::fs::metadata(segment_path(self.dir, number)).map_or(0, |meta| meta.len());
            self.numbers.push(number);
        }
        Ok(())
    }

    fn finish(mut self, writer: &mut Writer) -> StorageResult<Vec<u64>> {
        self.close(writer)?;
        Ok(self.numbers)
    }
}

/// Collapses log entries per document and locates the versions they remove.
fn plan(state: &State, entries: &[LogEntry], scratch: &mut Scratch) -> StorageResult<BTreeMap<CollectionId, Planned>> {
    let mut changes = BTreeMap::<CollectionId, Planned>::new();
    let mut open = None;
    for entry in entries {
        let (collection, id, put) = match entry {
            LogEntry::Put { collection, id, version, document } => (collection, id, Some(DeltaRow { version: version.get(), document: document.clone() })),
            LogEntry::Delete { collection, id } => (collection, id, None),
        };
        let Planned { changes: per_collection, removed: facts } = changes.entry(collection.clone()).or_default();
        let key = key(id);
        if let Some(change) = per_collection.get_mut(&key) {
            change.put = put;
            continue;
        }
        let removed = match state.collections.get(collection) {
            None => None,
            Some(existing) => match existing.locate(key, scratch)? {
                None => None,
                Some(Location::Delta) => existing.delta.get(&key).map(|row| {
                    add_facts(facts, facts_of(&row.document));
                    Location::Delta
                }),
                Some(location @ Location::Segment { part, row }) => {
                    let (group, local) = existing.open_row(part, row, &mut open)?;
                    group.count_facts(local, facts)?;
                    Some(location)
                }
            },
        };
        per_collection.insert(key, Change { removed, put });
    }
    Ok(changes)
}

/// Applies planned changes in place (copy-on-write of shared parts).
fn apply(state: &mut State, changes: BTreeMap<CollectionId, Planned>, generation: u64) {
    for (name, Planned { changes: per_collection, removed }) in changes {
        let collection = Arc::make_mut(state.collections.entry(name).or_default());
        collection.changed = generation;
        let facts = Arc::make_mut(&mut collection.facts);
        for (field, counts) in removed {
            let slots = facts.entry(field).or_insert([0; FACTS]);
            slots.iter_mut().zip(counts).for_each(|(slot, count)| *slot = slot.saturating_sub(count));
        }
        let mut delta_removals = Vec::new();
        for (key, change) in &per_collection {
            if let Some(location) = &change.removed {
                collection.live = collection.live.saturating_sub(1);
                match location {
                    Location::Delta => delta_removals.push(*key),
                    Location::Segment { part, row } => {
                        let part = &mut collection.segments[*part];
                        let rows = part.segment.rows();
                        Arc::make_mut(part.dead.get_or_insert_with(|| Arc::new(DeadRows::with_rows(rows)))).insert(*row);
                    }
                }
            }
            if let Some(put) = &change.put {
                add_facts(facts, facts_of(&put.document));
                collection.live += 1;
            }
        }
        facts.retain(|_, counts| counts.iter().any(|count| *count > 0));
        let delta = Arc::make_mut(&mut collection.delta);
        for key in delta_removals {
            delta.remove(&key);
        }
        for (key, change) in per_collection {
            if let Some(put) = change.put {
                delta.insert(key, put);
            }
        }
    }
    state.collections.retain(|_, collection| collection.live > 0 || !collection.segments.is_empty());
}

/// Takes the store's exclusive lock: a second handle, in this process or
/// another, would write commits the first one never sees.
fn lock_store(dir: &Path) -> StorageResult<std::fs::File> {
    let path = dir.join("LOCK");
    let file = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(&path).map_err(io_error("open lock", &path))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(StorageError::backend(format!("Glacier store {} is already open (by this process or another)", dir.display()))),
        Err(std::fs::TryLockError::Error(error)) => Err(io_error("lock", &path)(error)),
    }
}

/// Refuses a directory holding store data but no `MANIFEST`: opening it as a
/// new store would delete every segment and log it holds. Only a store that
/// never wrote past its first log (created before the manifest came first)
/// has no manifest.
fn refuse_files_without_manifest(dir: &Path) -> StorageResult<()> {
    let entries = std::fs::read_dir(dir).map_err(io_error("read store directory", dir))?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let first_log = name == wal::wal_file_name(1);
        let data = segment_number(name).is_some() || name == history::HIST || (name.starts_with("wal-") && name.ends_with(".log") && !first_log);
        if data {
            return Err(StorageError::backend(format!(
                "Glacier store {} holds {name} but no MANIFEST: refusing to open it as a new store (restore its MANIFEST)",
                dir.display()
            )));
        }
    }
    Ok(())
}

fn remove_unreferenced(dir: &Path, referenced: &BTreeSet<u64>, wal_sequence: u64, recorded: Option<&history::History>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let stale = if let Some(number) = segment_number(name) {
            !referenced.contains(&number)
        } else if let Some(sequence) = name.strip_prefix("wal-").and_then(|rest| rest.strip_suffix(".log")).and_then(|sequence| sequence.parse::<u64>().ok()) {
            sequence < wal_sequence
        } else {
            name.ends_with(".tmp") && name != MANIFEST
        };
        if stale {
            match recorded {
                Some(recorded) if name.starts_with("wal-") => recorded.archive_stale_wal(&entry.path()),
                _ => { let _ = std::fs::remove_file(entry.path()); }
            }
        }
    }
}

/// Writes recorded history into a new store (see [`HistoryWriter`]).
struct HistoryRestore<'a>(&'a GlacierBackend);

impl HistoryWriter for HistoryRestore<'_> {
    fn baseline(&mut self, collection: &CollectionId, rows: Vec<StoredDocument>) -> StorageResult<()> {
        let entries = rows.into_iter().map(|row| { let (id, version, document) = row.into_parts(); LogEntry::Put { collection: collection.clone(), id, version, document } }).collect();
        self.0.commit_at(self.0.generation()? + 1, entries)
    }

    fn start(&mut self, base: CommitRecord) -> StorageResult<()> {
        self.0.set_generation(base.generation)?;
        base.apply(|| self.0.record_history())
    }

    fn commit(&mut self, record: CommitRecord, events: Vec<(CollectionId, HistoryEvent)>) -> StorageResult<()> {
        if !self.0.keeps_history() {
            return Err(StorageError::backend("history: commits come after the start of history"));
        }
        let entries = events.into_iter().map(|(collection, event)| match (event.op, event.document) {
            (HistoryOp::Put, Some(document)) => Ok(LogEntry::Put { collection, id: event.id, version: event.version, document }),
            (HistoryOp::Delete, _) => Ok(LogEntry::Delete { collection, id: event.id }),
            (op, _) => Err(StorageError::backend(format!("history: cannot restore a {op:?} event"))),
        }).collect::<StorageResult<Vec<_>>>()?;
        let generation = record.generation;
        record.apply(|| self.0.commit_at(generation, entries))
    }
}

impl StorageEngine for GlacierBackend {
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>> {
        Ok(Box::new(read::Snapshot::new(self.state()?)))
    }

    fn read_temporal(&self, temporal: &Temporal, cache: &Arc<TemporalCache>) -> StorageResult<Box<dyn StorageRead + '_>> {
        if self.inner.history.get().is_some_and(history::History::journal) {
            return Err(StorageError::history_unavailable());
        }
        let (state, source) = self.history_source()?;
        temporal_read(Box::new(read::Snapshot::new(state)), Box::new(source), temporal, cache)
    }

    fn usage_by(&self, key: &str) -> StorageResult<crate::storage::KeyUsage> { self.usage_by_key(key) }

    fn history(&self) -> StorageResult<(Box<dyn StorageRead + '_>, Box<dyn HistorySource + '_>)> {
        let (state, source) = self.history_source()?;
        Ok((Box::new(read::Snapshot::new(state)), Box::new(source)))
    }

    fn history_writer(&self) -> StorageResult<Box<dyn HistoryWriter + '_>> {
        let state = self.state()?;
        if state.generation != 0 || !state.collections.is_empty() || self.inner.history.get().is_some() {
            return Err(StorageError::backend("history: restoring history needs a new, empty store directory"));
        }
        Ok(Box::new(HistoryRestore(self)))
    }

    fn release_history(&self, generation: u64) -> StorageResult<()> {
        let Some(recorded) = self.inner.history.get() else { return Ok(()) };
        let writer = self.writer()?;
        let live_first = wal::replay(&wal_path(&self.inner.dir, writer.wal_sequence))?.0.first().map(|record| record.0);
        recorded.release(generation, live_first)
    }

    fn begin(&self) -> StorageResult<Box<dyn StorageTransaction + '_>> {
        Ok(Box::new(tx::Transaction::new(self, self.state()?)))
    }

    fn apply_batch_atomic(&self, collection: &CollectionId, mutations: Vec<StorageMutation>) -> StorageResult<(Vec<StoredDocument>, CommitResult)> {
        if mutations.is_empty() {
            return Ok((Vec::new(), CommitResult::default()));
        }
        let state = self.state()?;
        let (commit, stored) = self.prepare_batch(&state, collection, mutations, true)?;
        drop(state);
        Ok((stored, self.commit(commit)?))
    }

    fn apply_batch_atomic_summary(&self, collection: &CollectionId, mutations: Vec<StorageMutation>) -> StorageResult<CommitResult> {
        if mutations.is_empty() {
            return Ok(CommitResult::default());
        }
        let state = self.state()?;
        let (commit, _) = self.prepare_batch(&state, collection, mutations, false)?;
        drop(state);
        self.commit(commit)
    }
}

#[cfg(test)]
mod tests;
