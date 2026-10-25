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
    Collection, SegmentPart, State,
};
use crate::storage::{
    history::{CommitRecord, HistoryEvent, HistoryOp, HistorySource, Timeline, TimelineFile},
    CollectionId, DocumentId, DocumentVersion, StorageError, StorageResult,
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
        Ok(Some(Self { dir, base: base.into(), timeline, journal }))
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
        Ok(Self { dir, base: state.generation.into(), timeline, journal: false })
    }

    /// Starts a backup journal keeping the commits after `generation`.
    pub(super) fn enable_journal(store: &Path, generation: u64) -> StorageResult<Self> {
        let dir = store.join(HIST);
        fs::create_dir_all(&dir).map_err(io_error("create history directory", &dir))?;
        write_base(&dir, JOURNAL, generation)?;
        sync_dir(store)?;
        Ok(Self { timeline: TimelineFile::open(dir.join(TIMELINE))?, dir, base: generation.into(), journal: true })
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
        if !self.journal {
            self.timeline.append(&CommitRecord::now(generation));
        }
    }

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
                    let dead = if file.kind == HistKind::Baseline {
                        let dead_path = self.dir.join(format!("b-{:020}-{:08}.dead", file.generation, file.number));
                        match fs::read(&dead_path) {
                            Ok(bytes) => Some(Arc::new(DeadRows::from_bytes(&bytes, segment.rows()))),
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                            Err(error) => return Err(io_error("read baseline deleted rows", &dead_path)(error)),
                        }
                    } else {
                        None
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
        Ok(Source { generation: state.generation, base: self.base.load(std::sync::atomic::Ordering::Acquire), files: sources, live, timeline, journal: self.journal })
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
    base: u64,
    files: Vec<SourceFile>,
    /// Commits of the live log, up to `generation`.
    live: Vec<(u64, Vec<LogEntry>)>,
    timeline: PathBuf,
    journal: bool,
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

    fn timeline(&self) -> StorageResult<Timeline> {
        let timeline = TimelineFile::read(&self.timeline)?;
        Ok(timeline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn history_file_names_parse() { let wal = parse_name("w-00000000000000000007.log").unwrap(); assert_eq!((wal.kind, wal.generation, wal.first()), (HistKind::Wal, 7, 8)); let segment = parse_name("s-00000000000000000009-00000012.seg").unwrap(); assert_eq!((segment.kind, segment.generation, segment.number), (HistKind::Segment, 9, 12)); assert_eq!(parse_name("b-00000000000000000003-00000001.g6").unwrap().kind, HistKind::Baseline); assert!(parse_name("b-00000000000000000003-00000001.dead").is_none()); assert!(parse_name("TIMELINE").is_none()); }
}
