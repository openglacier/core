#![cfg_attr(rustfmt, rustfmt_skip)]
//! History of a Glacier store, kept once recording is enabled
//! ([`super::GlacierBackend::record_history`]).
//!
//! ```text
//! <store>/hist/
//!   BASE                         generation at which recording started (its presence turns recording on)
//!   JOURNAL                      instead of BASE, a backup journal: the generation after which commits are kept
//!   TIMELINE                     time and author of each commit (unsynced appends)
//!   b-<gen>-<n>.g6 [.dead]       baseline: the live segments at BASE (hard links) and their deleted rows
//!   w-<base>.log                 an archived log: the commits after generation <base> it held
//!   s-<gen>-<n>.g6               the rows put by the commit <gen> written straight to segments, or imported (hard links)
//!   d-<gen>.log                  the deletes of such a commit
//!   s-<gen>-<n>.dead             the rows of such a segment a purge removed ([`History::purge`]), as `b-….dead`
//! ```
//!
//! Nothing is copied: the store already writes every version once, to the
//! log (small commits) or to a segment (large commits, imports). Recording
//! keeps those files instead of deleting them: a log is renamed into `hist/`
//! when a flush retires it, a segment written by a large commit is hard
//! linked when written. The cost on the write path is one unsynced timeline
//! append per commit; large commits also keep their puts in their own
//! segments (not merged with the delta in the same flush). Disk use grows
//! with the history kept.
//!
//! A backup journal records the same files without a baseline nor a
//! timeline: enough for the events after its generation, which is all a
//! backup diff reads. A backup releases what it no longer needs, moving the
//! journal's generation forward.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{
    column::Scratch,
    manifest::{sync_dir, DeadRows},
    read::part_rows,
    segment::{io_error, Segment},
    wal::{self, LogEntry, Wal},
    Collection, DeltaRow, SegmentPart, State,
};
use crate::storage::{
    history::{CommitRecord, HistoryEvent, HistoryOp, HistorySource, Timeline, TimelineFile},
    CollectionId, DocumentId, DocumentVersion, StorageError, StorageRead, StorageResult,
};

pub(super) const HIST: &str = "hist";
const BASE: &str = "BASE";
const JOURNAL: &str = "JOURNAL";
const TIMELINE: &str = "TIMELINE";

/// Recording state of a store with history on.
#[derive(Debug)]
pub(super) struct History {
    dir: PathBuf,
    base: std::sync::atomic::AtomicU64,
    timeline: TimelineFile,
    journal: bool,
    past: Arc<PastStates>,
}

impl History {
    /// History of the store in `store`, when recording was enabled.
    pub(super) fn load(store: &Path) -> StorageResult<Option<Self>> {
        let dir = store.join(HIST);
        let read = |name: &str| match fs::read_to_string(dir.join(name)) {
            Ok(text) => text.trim().parse::<u64>().map(Some).map_err(|_| StorageError::backend(format!("Glacier corrupt data: hist/{name}"))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(io_error("read history base", &dir)(error)),
        };
        let (base, journal) = match read(BASE)? {
            Some(base) => (base, false),
            None => match read(JOURNAL)? { Some(base) => (base, true), None => return Ok(None) },
        };
        let timeline = TimelineFile::open(dir.join(TIMELINE))?;
        Ok(Some(Self { dir, base: base.into(), timeline, journal, past: Arc::default() }))
    }

    /// Starts recording at `state` (no delta rows, empty log): links its
    /// live segments as the baseline, then writes `BASE`.
    pub(super) fn enable(store: &Path, state: &State) -> StorageResult<Self> {
        let dir = store.join(HIST);
        fs::create_dir_all(&dir).map_err(io_error("create history directory", &dir))?;
        for collection in state.collections.values() {
            for part in &collection.segments {
                let number = part.segment.number();
                let link = dir.join(format!("b-{:020}-{number:08}.g6", state.generation));
                if !link.exists() {
                    fs::hard_link(part.segment.path(), &link).map_err(io_error("link baseline segment", &link))?;
                }
                if let Some(dead) = part.dead.as_ref().filter(|dead| dead.count() > 0) {
                    let path = dir.join(format!("b-{:020}-{number:08}.dead", state.generation));
                    fs::write(&path, dead.to_bytes()).map_err(io_error("write baseline deleted rows", &path))?;
                }
            }
        }
        let timeline = TimelineFile::open(dir.join(TIMELINE))?;
        timeline.append(&CommitRecord::now(state.generation));
        write_base(&dir, BASE, state.generation)?;
        sync_dir(store)?;
        Ok(Self { dir, base: state.generation.into(), timeline, journal: false, past: Arc::default() })
    }

    /// Starts a backup journal keeping the commits after `generation`.
    pub(super) fn enable_journal(store: &Path, generation: u64) -> StorageResult<Self> {
        let dir = store.join(HIST);
        fs::create_dir_all(&dir).map_err(io_error("create history directory", &dir))?;
        write_base(&dir, JOURNAL, generation)?;
        sync_dir(store)?;
        Ok(Self { timeline: TimelineFile::open(dir.join(TIMELINE))?, dir, base: generation.into(), journal: true, past: Arc::default() })
    }

    /// Whether this is a backup journal rather than history.
    pub(super) fn journal(&self) -> bool { self.journal }

    /// A journal drops the files holding only commits up to `generation`
    /// (a log ends below the next file's first generation, the last one
    /// below `live_first`, the live log's first commit) and keeps the later
    /// commits. History keeps everything.
    pub(super) fn release(&self, generation: u64, live_first: Option<u64>) -> StorageResult<()> {
        let base = self.base.load(std::sync::atomic::Ordering::Acquire);
        if !self.journal || generation <= base {
            return Ok(());
        }
        let mut files = fs::read_dir(&self.dir).map_err(io_error("list history", &self.dir))?.flatten().filter_map(|entry| Some((parse_name(entry.file_name().to_str()?)?, entry.path()))).collect::<Vec<_>>();
        files.sort_by_key(|(file, _)| (file.first(), file.kind.rank(), file.number));
        for index in 0..files.len() {
            let (file, path) = &files[index];
            let done = match file.kind {
                HistKind::Wal => files.get(index + 1).map(|(next, _)| next.first()).or(live_first).is_some_and(|end| end <= generation + 1),
                _ => file.generation <= generation,
            };
            if done {
                let _ = fs::remove_file(path);
            }
        }
        write_base(&self.dir, JOURNAL, generation)?;
        self.base.store(generation, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Records the time and author of commit `generation` (not in a journal).
    pub(super) fn record(&self, generation: u64) {
        if let Some(record) = self.commit_record(generation) {
            self.append(&record);
        }
    }

    /// The time and author of commit `generation`, made by this thread, to [`Self::append`] once it is durable
    /// (`None` in a journal).
    pub(super) fn commit_record(&self, generation: u64) -> Option<CommitRecord> { (!self.journal).then(|| CommitRecord::now(generation)) }

    pub(super) fn append(&self, record: &CommitRecord) { self.timeline.append(record); }

    /// Keeps a retired log holding the commits after `base` (removes it when empty).
    pub(super) fn archive_wal(&self, log: &Path, base: u64) {
        let empty = fs::metadata(log).map_or(true, |meta| meta.len() == 0);
        if empty {
            let _ = fs::remove_file(log);
            return;
        }
        let _ = fs::rename(log, self.dir.join(format!("w-{base:020}.log")));
    }

    /// Keeps a stale log found at open: its first record names its base.
    pub(super) fn archive_stale_wal(&self, log: &Path) {
        match wal::replay(log) {
            Ok((records, _)) if !records.is_empty() => self.archive_wal(log, records[0].0 - 1),
            _ => { let _ = fs::remove_file(log); }
        }
    }

    /// Links segment `number` of `store`, holding the puts of commit `generation`.
    pub(super) fn link_segment(&self, store: &Path, number: u64, generation: u64) -> StorageResult<()> {
        let link = self.dir.join(format!("s-{generation:020}-{number:08}.g6"));
        fs::hard_link(super::segment_path(store, number), &link).map_err(io_error("link history segment", &link))
    }

    /// Writes (synced) the deletes of commit `generation`.
    pub(super) fn write_deletes(&self, generation: u64, entries: &[LogEntry]) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut log = Wal::open(&self.dir.join(format!("d-{generation:020}.log")), 0)?;
        log.append(generation, entries)
    }

    pub(super) fn sync(&self) -> StorageResult<()> { sync_dir(&self.dir) }

    /// Removes the segment and delete files of `generation`: left by a commit
    /// that failed before its manifest, they must not join the retried one.
    pub(super) fn forget(&self, generation: u64) {
        let Ok(entries) = fs::read_dir(&self.dir) else { return };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(HistFile { generation: found, kind: HistKind::Segment | HistKind::Deletes, .. }) = parse_name(name) {
                if found == generation {
                    let _ = fs::remove_file(entry.path());
                    let _ = fs::remove_file(entry.path().with_extension("dead"));
                }
            }
        }
    }

    /// Removes the files of commits that never became durable (after `committed`).
    pub(super) fn recover(&self, committed: u64) {
        let Ok(entries) = fs::read_dir(&self.dir) else { return };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(HistFile { generation, kind: HistKind::Segment | HistKind::Deletes, .. }) = parse_name(name) {
                if generation > committed {
                    let _ = fs::remove_file(entry.path());
                    let _ = fs::remove_file(entry.path().with_extension("dead"));
                }
            }
        }
    }

    /// The history files and the live log's commits, for a source at `state`.
    pub(super) fn source(&self, state: Arc<State>, live: Vec<(u64, Vec<LogEntry>)>) -> StorageResult<Source> {
        let mut files = Vec::new();
        for entry in fs::read_dir(&self.dir).map_err(io_error("list history", &self.dir))?.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(file) = parse_name(name) else { continue };
            let opened = match file.kind {
                HistKind::Baseline | HistKind::Segment => {
                    let segment = Segment::open(&entry.path(), file.number)?;
                    // Rows deleted at the baseline, and rows a purge left out ([`History::purge`]).
                    let dead_path = self.dir.join(file.dead_name());
                    let dead = match fs::read(&dead_path) {
                        Ok(bytes) => Some(Arc::new(DeadRows::from_bytes(&bytes, segment.rows()))),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                        Err(error) => return Err(io_error("read history deleted rows", &dead_path)(error)),
                    };
                    let collection = CollectionId::parse(segment.collection()).map_err(|_| StorageError::backend("Glacier corrupt data: history segment collection"))?;
                    Opened::Rows { collection, part: SegmentPart { segment, dead } }
                }
                HistKind::Wal | HistKind::Deletes => Opened::Log(entry.path()),
            };
            files.push((file, opened));
        }
        files.sort_by_key(|(file, _)| (file.first(), file.kind.rank(), file.number));
        let live_first = live.first().map(|record| record.0);
        // Each archived log holds generations below the next file's first one.
        let mut sources = Vec::with_capacity(files.len());
        for index in 0..files.len() {
            let end = files.get(index + 1).map(|(file, _)| file.first()).or(live_first).unwrap_or(u64::MAX);
            let (file, opened) = &files[index];
            sources.push(SourceFile { file: *file, end, opened: opened.clone() });
        }
        let timeline = self.dir.join(TIMELINE);
        Ok(Source { generation: state.generation, state, past: Arc::clone(&self.past), base: self.base.load(std::sync::atomic::Ordering::Acquire), files: sources, live, timeline, journal: self.journal })
    }
}

/// Writes `name` holding `generation` (synced, replaced atomically).
fn write_base(dir: &Path, name: &str, generation: u64) -> StorageResult<()> {
    let temporary = dir.join(format!("{name}.tmp"));
    fs::write(&temporary, generation.to_string()).map_err(io_error("write history base", &temporary))?;
    fs::File::open(&temporary).and_then(|file| file.sync_all()).map_err(io_error("sync history base", &temporary))?;
    fs::rename(&temporary, dir.join(name)).map_err(io_error("install history base", dir))?;
    sync_dir(dir)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistKind {
    Baseline,
    Segment,
    Deletes,
    Wal,
}

impl HistKind {
    const fn rank(self) -> u8 {
        match self {
            Self::Baseline => 0,
            Self::Segment => 1,
            Self::Deletes => 2,
            Self::Wal => 3,
        }
    }
}

/// A history file named after its generation (a log: after its base).
#[derive(Clone, Copy, Debug)]
struct HistFile {
    kind: HistKind,
    generation: u64,
    number: u64,
}

impl HistFile {
    /// Name of the deleted rows kept beside a segment of rows.
    fn dead_name(&self) -> String { format!("{}-{:020}-{:08}.dead", if self.kind == HistKind::Baseline { "b" } else { "s" }, self.generation, self.number) }

    /// First generation the file may hold.
    const fn first(&self) -> u64 {
        match self.kind {
            HistKind::Wal => self.generation + 1,
            _ => self.generation,
        }
    }
}

fn parse_name(name: &str) -> Option<HistFile> {
    let (kind, rest) = name.split_once('-')?;
    let (stem, extension) = rest.rsplit_once('.')?;
    let kind = match (kind, extension) {
        ("b", "g6" | "seg") => HistKind::Baseline,
        ("s", "g6" | "seg") => HistKind::Segment,
        ("d", "log") => HistKind::Deletes,
        ("w", "log") => HistKind::Wal,
        _ => return None,
    };
    let (generation, number) = match stem.split_once('-') {
        Some((generation, number)) => (generation.parse().ok()?, number.parse().ok()?),
        None => (stem.parse().ok()?, 0),
    };
    Some(HistFile { kind, generation, number })
}

#[derive(Clone, Debug)]
enum Opened {
    Rows { collection: CollectionId, part: SegmentPart },
    Log(PathBuf),
}

#[derive(Debug)]
struct SourceFile {
    file: HistFile,
    /// Generations of the file are below this one.
    end: u64,
    opened: Opened,
}

/// History of a Glacier store at one generation.
#[derive(Debug)]
pub(super) struct Source {
    generation: u64,
    state: Arc<State>,
    past: Arc<PastStates>,
    base: u64,
    files: Vec<SourceFile>,
    /// Commits of the live log, up to `generation`.
    live: Vec<(u64, Vec<LogEntry>)>,
    timeline: PathBuf,
    journal: bool,
}

/// What [`History::purge`] removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HistoryPurge {
    /// Versions (and deletes) no longer kept.
    pub versions: u64,
    /// Files removed whole.
    pub files: u64,
    /// Bytes given back by removed and rewritten files.
    pub bytes: u64,
}

/// An event of a document seen by a purge: where it is, and the floor of its Place when its document was read.
struct Seen { generation: u64, file: usize, at: Option<(usize, usize)>, delete: bool, floor: Option<(u64, bool)> }

/// A segment of rows is written again without its purged rows once this share of them is purged (one in four).
const REWRITE_SHARE: u64 = 4;

impl History {
    /// Removes the versions no read of the past needs any more. `floor_of` gives, for the Place of a document
    /// (its `_place`, `None` without one), the time (Unix milliseconds) from which its past is read, `None` to
    /// keep it all: a version is removed once the next event of its document is at or before the last commit of
    /// that time, and a delete once it is the last event left of its document and as old. Every state from the
    /// floor on is rebuilt as before. `source` lists the files at a state whose log is empty (a checkpoint).
    ///
    /// Logs are rewritten without the removed events. A segment of rows (baseline, large commit) goes once every
    /// row is removed, is written again without its removed rows once [`REWRITE_SHARE`] of them are, or as soon
    /// as one belongs to a Place whose floor is `i64::MAX` (nothing of its past is kept: its bytes leave the
    /// disk); below that it keeps the list of its removed rows beside it. A segment written again takes a new
    /// `number` from `next_number`, so the list of the old one never applies to it. Each file is replaced at
    /// once and each removal stands alone: an interrupted purge leaves a valid history, the next one resumes
    /// (stopped between the new segment and the removal of the old one, the rows of both show until then).
    pub(super) fn purge(&self, source: &Source, floor_of: &dyn Fn(Option<&str>) -> Option<i64>, next_number: &dyn Fn() -> StorageResult<u64>) -> StorageResult<HistoryPurge> {
        // The past states kept were built from the files a purge rewrites.
        let _clear = Cleared(&self.past);
        // What a purge stopped while writing a segment left.
        for entry in fs::read_dir(&self.dir).map_err(io_error("list history", &self.dir))?.flatten() {
            if entry.file_name().to_str().is_some_and(|name| name.ends_with(".g6.tmp")) { let _ = fs::remove_file(entry.path()); }
        }
        let timeline = source.timeline()?;
        let mut floors = std::collections::HashMap::<Option<String>, (u64, bool)>::new();
        let mut floor = |document: &crate::model::Document| {
            let place = document.get(crate::helpers::PLACE_SCOPE_FIELD).and_then(crate::model::Value::as_str);
            *floors.entry(place.map(str::to_owned)).or_insert_with(|| floor_of(place).map_or((0, false), |millis| (timeline.entries().iter().take_while(|entry| entry.millis <= millis).last().map_or(0, |entry| entry.generation), millis == i64::MAX)))
        };
        // The events of each document after the baseline, in generation order (the order of the files).
        let mut documents = std::collections::HashMap::<(CollectionId, DocumentId), Vec<Seen>>::new();
        for (file, listed) in source.files.iter().enumerate() {
            match &listed.opened {
                Opened::Log(path) => for (record, (generation, entries)) in wal::replay(path)?.0.iter().enumerate() {
                    for (entry, logged) in entries.iter().enumerate() {
                        let (key, delete, floor) = match logged {
                            LogEntry::Put { collection, id, document, .. } => ((collection.clone(), *id), false, Some(floor(document))),
                            LogEntry::Delete { collection, id } => ((collection.clone(), *id), true, None),
                        };
                        documents.entry(key).or_default().push(Seen { generation: *generation, file, at: Some((record, entry)), delete, floor });
                    }
                },
                Opened::Rows { collection, part } if listed.file.kind == HistKind::Segment => part_rows(part, false, &mut |id, _, _| {
                    documents.entry((collection.clone(), id)).or_default().push(Seen { generation: listed.file.generation, file, at: None, delete: false, floor: None });
                    Ok(true)
                })?,
                Opened::Rows { .. } => {}
            }
        }
        let rows_of = |file: usize| match &source.files[file].opened { Opened::Rows { part, .. } => Collection { segments: vec![part.clone()], ..Collection::default() }, Opened::Log(_) => Collection::default() };
        let mut scratch = Scratch::default();
        let (mut entries, mut rows) = (std::collections::HashMap::<usize, HashSet<(usize, usize)>>::new(), std::collections::HashMap::<usize, Vec<DocumentId>>::new());
        // Segments of rows holding a removed row of a Place that keeps nothing.
        let mut erased = HashSet::<usize>::new();
        let mut purged = HistoryPurge::default();
        for ((collection, id), mut events) in documents {
            // The version of the baseline comes first, when the document was there.
            let baseline = source.files.iter().enumerate().find_map(|(file, listed)| match &listed.opened {
                Opened::Rows { collection: target, .. } if listed.file.kind == HistKind::Baseline && *target == collection => rows_of(file).get(&id, &mut scratch).transpose().map(|stored| stored.map(|stored| Seen { generation: source.base, file, at: None, delete: false, floor: Some(floor(stored.document())) })),
                _ => None,
            }).transpose()?;
            events.splice(0..0, baseline);
            let (mut kept, mut last_floor) = (false, (0, false));
            for index in 0..events.len() {
                let event = &events[index];
                // A delete follows the Place of the version it removed; a row is read for its Place only when judged.
                let floor = match (event.delete, event.floor, events.get(index + 1)) {
                    (true, ..) => last_floor,
                    (false, Some(floor), _) => floor,
                    (false, None, Some(_)) => rows_of(event.file).get(&id, &mut scratch)?.map_or((0, false), |stored| floor(stored.document())),
                    (false, None, None) => (0, false),
                };
                last_floor = floor;
                let removed = match events.get(index + 1) {
                    Some(next) => next.generation <= floor.0,
                    None => event.delete && !kept && event.generation <= floor.0,
                };
                if !removed { kept = true; continue; }
                purged.versions += 1;
                match event.at {
                    Some(at) => { entries.entry(event.file).or_default().insert(at); }
                    None => {
                        rows.entry(event.file).or_default().push(id);
                        if floor.1 { erased.insert(event.file); }
                    }
                }
            }
        }
        let size = |path: &Path| fs::metadata(path).map_or(0, |metadata| metadata.len());
        let replace = |path: &Path, bytes: &[u8]| -> StorageResult<()> {
            let temporary = path.with_extension("tmp");
            fs::write(&temporary, bytes).and_then(|()| fs::File::open(&temporary)?.sync_all()).and_then(|()| fs::rename(&temporary, path)).map_err(io_error("rewrite history file", path))
        };
        for (file, removed) in entries {
            let Opened::Log(path) = &source.files[file].opened else { continue };
            let mut bytes = Vec::new();
            for (record, (generation, logged)) in wal::replay(path)?.0.into_iter().enumerate() {
                let left = logged.into_iter().enumerate().filter(|(entry, _)| !removed.contains(&(record, *entry))).map(|(_, logged)| logged).collect::<Vec<_>>();
                if !left.is_empty() { bytes.extend(wal::encode_record(generation, &left)?); }
            }
            purged.bytes += size(path).saturating_sub(bytes.len() as u64);
            if bytes.is_empty() { fs::remove_file(path).map_err(io_error("remove history file", path))?; purged.files += 1; } else { replace(path, &bytes)?; }
        }
        for (file, ids) in rows {
            let Opened::Rows { part, collection } = &source.files[file].opened else { continue };
            let (single, mut dead) = (rows_of(file), part.dead.as_deref().cloned().unwrap_or_else(|| DeadRows::with_rows(part.segment.rows())));
            for id in ids {
                if let Some(super::Location::Segment { row, .. }) = single.locate(super::key(&id), &mut scratch)? { dead.insert(row); }
            }
            let (path, beside) = (part.segment.path().to_owned(), self.dir.join(source.files[file].file.dead_name()));
            if dead.count() >= part.segment.rows() {
                purged.bytes += size(&path);
                purged.files += 1;
                fs::remove_file(&path).map_err(io_error("remove history file", &path))?;
                let _ = fs::remove_file(&beside);
            } else if erased.contains(&file) || dead.count() * REWRITE_SHARE >= part.segment.rows() {
                // Written whole and synced under a temporary name, then named, then the old one goes.
                let listed = HistFile { number: next_number()?, ..source.files[file].file };
                let written = self.dir.join(listed.dead_name()).with_extension("g6");
                let temporary = written.with_extension("g6.tmp");
                let mut segment = super::segment::SegmentWriter::create(&temporary, collection.as_str())?;
                part_rows(&SegmentPart { segment: Arc::clone(&part.segment), dead: Some(Arc::new(dead)) }, true, &mut |id, version, document| {
                    segment.push(id, version, document.ok_or_else(|| StorageError::backend("Glacier: a history row without its document"))?)?;
                    Ok(true)
                })?;
                segment.finish()?;
                fs::File::open(&temporary).and_then(|file| file.sync_all()).and_then(|()| fs::rename(&temporary, &written)).map_err(io_error("write history segment", &written))?;
                sync_dir(&self.dir)?;
                purged.bytes += size(&path).saturating_sub(size(&written));
                fs::remove_file(&path).map_err(io_error("remove history file", &path))?;
                let _ = fs::remove_file(&beside);
            } else {
                replace(&beside, &dead.to_bytes())?;
            }
        }
        sync_dir(&self.dir)?;
        Ok(purged)
    }
}

fn log_events(generation: u64, entries: &[LogEntry], collection: &CollectionId, ids: Option<&HashSet<DocumentId>>, visitor: &mut dyn FnMut(HistoryEvent) -> StorageResult<bool>) -> StorageResult<bool> {
    for entry in entries {
        let event = match entry {
            LogEntry::Put { collection: target, id, version, document } if target == collection => HistoryEvent { generation, op: HistoryOp::Put, id: *id, version: *version, document: Some(Arc::clone(document)) },
            LogEntry::Delete { collection: target, id } if target == collection => HistoryEvent { generation, op: HistoryOp::Delete, id: *id, version: DocumentVersion::NONE, document: None },
            _ => continue,
        };
        if ids.is_some_and(|ids| !ids.contains(&event.id)) {
            continue;
        }
        if !visitor(event)? {
            return Ok(false);
        }
    }
    Ok(true)
}

impl Source {
    /// `collection` as it was at `generation`, read like a live one: its live segments without the rows of the
    /// documents an event touched since, the history segments with only the versions these documents had then,
    /// and the versions held by logs as its delta. Rows are matched by identifier, one pass over each segment
    /// holding some, and kept or left out by bitmap: no document is decoded. `None` when no event touched it since.
    pub(super) fn collection_at(&self, collection: &CollectionId, generation: u64) -> StorageResult<Option<Collection>> {
        let mut touched = Vec::new();
        self.events(collection, generation, self.generation, None, false, &mut |event| { touched.push(super::key(&event.id)); Ok(true) })?;
        if touched.is_empty() { return Ok(None); }
        touched.sort_unstable();
        touched.dedup();
        // The version of each touched document then: the last event up to `generation`, files in generation order.
        let mut then = vec![None; touched.len()];
        let logged = |entries: &[LogEntry], then: &mut [Option<Then>]| for entry in entries {
            let (target, id, version) = match entry {
                LogEntry::Put { collection: target, id, version, document } => (target, id, Some(Then::Logged(DeltaRow { version: version.get(), document: Arc::clone(document) }))),
                LogEntry::Delete { collection: target, id } => (target, id, None),
            };
            if target == collection { if let Ok(at) = touched.binary_search(&super::key(id)) { then[at] = version; } }
        };
        for (file, source) in self.files.iter().enumerate() {
            if source.file.first() > generation { break; }
            match &source.opened {
                Opened::Rows { collection: target, part } if target == collection && source.file.generation <= generation => part.segment.rows_among(&touched, &mut |row, at| {
                    if !part.dead.as_ref().is_some_and(|dead| dead.contains(row)) { then[at] = Some(Then::Row(file, row)); }
                })?,
                Opened::Rows { .. } => {}
                Opened::Log(path) => for (_, entries) in wal::replay(path)?.0.into_iter().filter(|(logged, _)| *logged <= generation) { logged(&entries, &mut then); },
            }
        }
        self.live.iter().filter(|(logged, _)| *logged <= generation).for_each(|(_, entries)| logged(entries, &mut then));
        let mut segments = Vec::new();
        let mut delta = std::collections::BTreeMap::new();
        if let Some(current) = self.state.collections.get(collection) {
            for part in &current.segments {
                let mut dead = part.dead.as_deref().cloned().unwrap_or_else(|| DeadRows::with_rows(part.segment.rows()));
                part.segment.rows_among(&touched, &mut |row, _| { dead.insert(row); })?;
                segments.push(SegmentPart { segment: Arc::clone(&part.segment), dead: Some(Arc::new(dead)) });
            }
            delta.extend(current.delta.iter().filter(|(key, _)| touched.binary_search(key).is_err()).map(|(key, row)| (*key, row.clone())));
        }
        let mut kept = std::collections::BTreeMap::<usize, DeadRows>::new();
        for (key, version) in touched.iter().zip(then) {
            match version {
                Some(Then::Row(file, row)) => { kept.entry(file).or_insert_with(|| DeadRows::with_rows(self.part(file).segment.rows())).insert(row); }
                Some(Then::Logged(row)) => { delta.insert(*key, row); }
                None => {}
            }
        }
        segments.extend(kept.into_iter().map(|(file, kept)| { let part = self.part(file); SegmentPart { segment: Arc::clone(&part.segment), dead: Some(Arc::new(kept.others(part.segment.rows()))) } }));
        let live = segments.iter().map(SegmentPart::live).sum::<u64>() + delta.len() as u64;
        let mut at = Collection { segments, delta: delta.into(), live, changed: generation, ..Collection::default() };
        at.refresh_overlap();
        Ok(Some(at))
    }

    fn part(&self, file: usize) -> &SegmentPart {
        match &self.files[file].opened { Opened::Rows { part, .. } => part, Opened::Log(_) => unreachable!("a row of a history segment") }
    }
}

/// Past states built for `as of` reads ([`Source::collection_at`]), kept across queries: reads at one date build
/// it once. A state is keyed by its collection, its generation and the last commit that changed the collection: a
/// commit elsewhere leaves it valid, one to the collection makes another key. A purge clears them all.
#[derive(Debug, Default)]
pub(super) struct PastStates(std::sync::Mutex<std::collections::VecDeque<(PastKey, Option<Arc<Collection>>, usize)>>);

type PastKey = (CollectionId, u64, u64);

/// States kept, and the bytes they may hold (bitmaps of rows, versions from logs).
const PAST_STATES: usize = 8;
const PAST_BYTES: usize = 256 << 20;

impl PastStates {
    fn get(&self, key: &PastKey) -> Option<Option<Arc<Collection>>> {
        let mut states = crate::helpers::lock_unpoisoned(&self.0);
        let at = states.iter().position(|(kept, ..)| kept == key)?;
        let state = states.remove(at)?;
        let found = state.1.clone();
        states.push_front(state);
        Some(found)
    }

    fn put(&self, key: PastKey, at: Option<Arc<Collection>>) {
        // Each segment part keeps a bitmap of its rows; a version from a log is counted at a typical document.
        let bytes = at.as_ref().map_or(0, |at| at.segments.iter().map(|part| part.segment.rows().div_ceil(8) as usize).sum::<usize>() + at.delta.len() * 512);
        if bytes > PAST_BYTES { return; }
        let mut states = crate::helpers::lock_unpoisoned(&self.0);
        states.push_front((key, at, bytes));
        while states.len() > PAST_STATES || states.iter().map(|state| state.2).sum::<usize>() > PAST_BYTES { states.pop_back(); }
    }

    fn clear(&self) { crate::helpers::lock_unpoisoned(&self.0).clear(); }
}

/// Clears the past states when dropped, whatever ends a purge.
struct Cleared<'a>(&'a PastStates);

impl Drop for Cleared<'_> {
    fn drop(&mut self) { self.0.clear(); }
}

/// Where the version of a document at a past generation is.
#[derive(Clone)]
enum Then {
    /// A row of a history segment (file, row).
    Row(usize, u64),
    Logged(DeltaRow),
}

impl HistorySource for Source {
    fn generation(&self) -> u64 { self.generation }

    fn floor(&self) -> u64 { self.base }

    fn keeps_baseline(&self) -> bool { !self.journal }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        let mut names = std::collections::BTreeSet::new();
        let add = |names: &mut std::collections::BTreeSet<CollectionId>, entries: &[LogEntry]| for entry in entries {
            let (LogEntry::Put { collection, .. } | LogEntry::Delete { collection, .. }) = entry;
            names.insert(collection.clone());
        };
        for source in &self.files {
            match &source.opened {
                Opened::Rows { collection, .. } => { names.insert(collection.clone()); }
                Opened::Log(path) => wal::replay(path)?.0.iter().for_each(|(_, entries)| add(&mut names, entries)),
            }
        }
        self.live.iter().for_each(|(_, entries)| add(&mut names, entries));
        Ok(names.into_iter().collect())
    }

    fn events(&self, collection: &CollectionId, after: u64, upto: u64, ids: Option<&HashSet<DocumentId>>, documents: bool, visitor: &mut dyn FnMut(HistoryEvent) -> StorageResult<bool>) -> StorageResult<()> {
        let upto = upto.min(self.generation);
        for source in &self.files {
            if source.file.first() > upto {
                break;
            }
            if source.end <= after + 1 && source.file.kind == HistKind::Wal {
                continue;
            }
            match &source.opened {
                Opened::Rows { collection: target, part } => {
                    let generation = source.file.generation;
                    if target != collection || generation <= after {
                        continue;
                    }
                    let op = if source.file.kind == HistKind::Baseline { HistoryOp::Baseline } else { HistoryOp::Put };
                    match ids {
                        Some(ids) => {
                            let single = Collection { segments: vec![part.clone()], ..Collection::default() };
                            let mut sorted = ids.iter().copied().collect::<Vec<_>>();
                            sorted.sort();
                            let mut scratch = Scratch::default();
                            for id in sorted {
                                if let Some(stored) = single.get(&id, &mut scratch)? {
                                    let event = HistoryEvent { generation, op, id, version: stored.version(), document: Some(stored.shared_document()) };
                                    if !visitor(event)? {
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        None => {
                            let mut more = true;
                            part_rows(part, documents, &mut |id, version, document| {
                                more = visitor(HistoryEvent { generation, op, id, version, document })?;
                                Ok(more)
                            })?;
                            if !more {
                                return Ok(());
                            }
                        }
                    }
                }
                Opened::Log(path) => {
                    for (generation, entries) in wal::replay(path)?.0 {
                        if generation <= after || generation > upto {
                            continue;
                        }
                        if !log_events(generation, &entries, collection, ids, visitor)? {
                            return Ok(());
                        }
                    }
                }
            }
        }
        for (generation, entries) in &self.live {
            if *generation <= after || *generation > upto {
                continue;
            }
            if !log_events(*generation, entries, collection, ids, visitor)? {
                return Ok(());
            }
        }
        Ok(())
    }

    fn read_at(&self, collection: &CollectionId, generation: u64) -> StorageResult<Option<Option<Arc<dyn StorageRead + Send + Sync>>>> {
        let key = (collection.clone(), generation, self.state.collections.get(collection).map_or(0, |current| current.changed));
        let at = match self.past.get(&key) {
            Some(at) => at,
            None => { let at = self.collection_at(collection, generation)?.map(Arc::new); self.past.put(key, at.clone()); at }
        };
        let Some(at) = at else { return Ok(Some(None)) };
        let state = State { generation, collections: std::iter::once((collection.clone(), at)).collect() };
        Ok(Some(Some(Arc::new(super::read::Snapshot::new(Arc::new(state))))))
    }

    fn timeline(&self) -> StorageResult<Timeline> {
        let timeline = TimelineFile::read(&self.timeline)?;
        Ok(timeline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn past_states_keep_the_last_used_within_their_count() { let states = PastStates::default(); let key = |generation| (CollectionId::parse("c").unwrap(), generation, 1); for generation in 0..=PAST_STATES as u64 { states.put(key(generation), None); } assert!(states.get(&key(0)).is_none()); assert!(matches!(states.get(&key(1)), Some(None))); states.put(key(99), None); assert!(states.get(&key(1)).is_some() && states.get(&key(2)).is_none()); states.clear(); assert!(states.get(&key(1)).is_none()); }

    #[test] fn history_file_names_parse() { let wal = parse_name("w-00000000000000000007.log").unwrap(); assert_eq!((wal.kind, wal.generation, wal.first()), (HistKind::Wal, 7, 8)); let segment = parse_name("s-00000000000000000009-00000012.seg").unwrap(); assert_eq!((segment.kind, segment.generation, segment.number), (HistKind::Segment, 9, 12)); assert_eq!(parse_name("b-00000000000000000003-00000001.g6").unwrap().kind, HistKind::Baseline); assert!(parse_name("b-00000000000000000003-00000001.dead").is_none()); assert!(parse_name("TIMELINE").is_none()); }
}
