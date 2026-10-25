#![cfg_attr(rustfmt, rustfmt_skip)]
//! Temporal reads: `on c as of …` (a collection at a past generation) and
//! `on c history` (every stored version of a collection).
//!
//! A backend that keeps history exposes it as a [`HistorySource`]: the events
//! (puts and deletes) of a collection in generation order, plus the commit
//! times it recorded ([`Timeline`]). Two read views are built on it, the same
//! for every backend:
//!
//! - [`AsOfRead`]: the current snapshot minus the documents an event touched
//!   after the target generation, plus the versions those documents had then.
//!   A collection no event touched since is read straight from the current
//!   snapshot, with all its fast paths; the cost grows with the changes made
//!   since the target generation, not with the collection.
//! - [`HistoryRead`]: one row per event of the source collection, in
//!   generation order, with the document of the version and system fields
//!   (`_doc_id`, `_version`, `_op`, `_generation`, `_at`, `_by`,
//!   `_by_device`). Its `_id` is the
//!   event's own identifier (generation, position), so the rows have distinct,
//!   ordered identifiers.
//!
//! Neither view changes the current read or write paths: they are only built
//! for a temporal query. Recording history costs one small, unsynced append
//! per commit (its time and author, see [`TimelineFile`]); the documents
//! themselves are what the backend already keeps.
//!
//! The author of a commit is the [`CommitAuthor`] set on the committing thread
//! with [`with_commit_author`] (the daemon sets the connection principal around
//! each query); commits made elsewhere have no author.

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use super::{
    CollectionId, DocumentId, DocumentVersion, FieldBounds, FieldFacts, FieldPath, GroupFacts, ProjectedBlock, BlockGate,
    ProjectedValueRef, ScanDirection, ScanOptions, StorageError, StorageRead, StorageReadCapability, StorageResult, StorageSupport,
    StoredDocument,
};
use crate::model::{Document, Value};

/// Field holding the identifier of the document a history row is a version of.
pub const HISTORY_DOC_ID_FIELD: &str = "_doc_id";
/// Field holding the document version of a history row.
pub const HISTORY_VERSION_FIELD: &str = "_version";
/// Field holding the kind of a history row: `insert`, `replace`, `delete` or `baseline`.
pub const HISTORY_OP_FIELD: &str = "_op";
/// Field holding the commit generation of a history row.
pub const HISTORY_GENERATION_FIELD: &str = "_generation";
/// Field holding the commit time of a history row (ISO 8601), `null` when unknown.
pub const HISTORY_AT_FIELD: &str = "_at";
/// Field holding the identity that made the commit of a history row, `null` when unknown.
pub const HISTORY_BY_FIELD: &str = "_by";
/// Field holding the device that made the commit of a history row, `null` when unknown.
pub const HISTORY_BY_DEVICE_FIELD: &str = "_by_device";

/// Who made a commit.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CommitAuthor {
    pub identity: Arc<str>,
    pub device: Option<Arc<str>>,
}

thread_local! {
    static COMMIT_AUTHOR: RefCell<Option<CommitAuthor>> = const { RefCell::new(None) };
    static COMMIT_RECORD: RefCell<Option<CommitRecord>> = const { RefCell::new(None) };
}

/// Runs `body` with `author` as the author of the commits it makes on this
/// thread; the previous author is restored afterwards.
pub fn with_commit_author<R>(author: Option<CommitAuthor>, body: impl FnOnce() -> R) -> R {
    struct Restore(Option<CommitAuthor>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let previous = self.0.take();
            COMMIT_AUTHOR.with(|current| *current.borrow_mut() = previous);
        }
    }
    let _restore = Restore(COMMIT_AUTHOR.with(|current| std::mem::replace(&mut *current.borrow_mut(), author)));
    body()
}

/// Author of the commits made on this thread now.
#[must_use]
pub fn current_commit_author() -> Option<CommitAuthor> {
    COMMIT_AUTHOR.with(|current| current.borrow().clone())
}

/// Time and author of one commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitRecord {
    pub generation: u64,
    pub millis: i64,
    pub author: Option<CommitAuthor>,
}

impl CommitRecord {
    /// The record of a commit made now by the current thread's author.
    #[must_use]
    pub fn now(generation: u64) -> Self {
        match COMMIT_RECORD.with(|record| record.borrow().clone()) {
            Some(record) => Self { generation, ..record },
            None => Self { generation, millis: crate::helpers::now_millis(), author: current_commit_author() },
        }
    }

    /// Runs `body` with the time and author of `self` given to the commits it
    /// makes on this thread: a restored commit keeps its own.
    pub fn apply<R>(self, body: impl FnOnce() -> R) -> R {
        struct Restore;
        impl Drop for Restore {
            fn drop(&mut self) { COMMIT_RECORD.with(|current| *current.borrow_mut() = None); }
        }
        COMMIT_RECORD.with(|current| *current.borrow_mut() = Some(self));
        let _restore = Restore;
        body()
    }
}

/// Point in time of an `as of` read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AsOf {
    /// A commit generation.
    Generation(u64),
    /// Unix time in milliseconds: the last generation committed at or before it.
    Time(i64),
}

impl fmt::Display for AsOf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Generation(generation) => write!(formatter, "generation {generation}"),
            Self::Time(millis) => formatter.write_str(&crate::query::dates::to_iso(*millis)),
        }
    }
}

/// Temporal read requested by a query source.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Temporal {
    /// Every collection as it was at one point in time.
    AsOf(AsOf),
    /// Every stored version of one collection; other collections read current.
    History { collection: CollectionId },
}

/// Kind of a history event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HistoryOp {
    /// A version written by a commit.
    Put,
    /// A document removed by a commit.
    Delete,
    /// A version present when history recording started (at the source floor),
    /// written earlier.
    Baseline,
    /// Every collection emptied by a commit.
    Clear,
}

/// One event of a history source.
#[derive(Clone, Debug)]
pub struct HistoryEvent {
    pub generation: u64,
    pub op: HistoryOp,
    pub id: DocumentId,
    /// Version written by a put or a baseline; [`DocumentVersion::NONE`] otherwise.
    pub version: DocumentVersion,
    /// Document of a put or a baseline, when the caller asked for documents.
    pub document: Option<Arc<Document>>,
}

/// Commit times and authors recorded by a backend, by generation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Timeline {
    /// Sorted by generation.
    entries: Vec<CommitRecord>,
}

impl Timeline {
    /// Builds a timeline from records in any order; a generation recorded
    /// twice keeps its last record.
    #[must_use]
    pub fn new(mut entries: Vec<CommitRecord>) -> Self {
        entries.sort_by_key(|entry| entry.generation);
        let mut deduplicated: Vec<CommitRecord> = Vec::with_capacity(entries.len());
        for entry in entries {
            match deduplicated.last_mut() {
                Some(last) if last.generation == entry.generation => *last = entry,
                _ => deduplicated.push(entry),
            }
        }
        Self { entries: deduplicated }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// The records, by generation.
    #[must_use]
    pub fn entries(&self) -> &[CommitRecord] { &self.entries }

    fn record(&self, generation: u64) -> Option<&CommitRecord> {
        self.entries.binary_search_by_key(&generation, |entry| entry.generation).ok().map(|index| &self.entries[index])
    }

    /// Commit time of `generation`, when recorded.
    #[must_use]
    pub fn time_of(&self, generation: u64) -> Option<i64> { self.record(generation).map(|record| record.millis) }

    /// Author of `generation`, when recorded.
    #[must_use]
    pub fn author_of(&self, generation: u64) -> Option<&CommitAuthor> { self.record(generation).and_then(|record| record.author.as_ref()) }

    /// Last generation committed at or before `millis`.
    ///
    /// # Errors
    ///
    /// Fails when no time is recorded, or when `millis` precedes the first
    /// recorded commit and that commit is not the store's first one.
    pub fn generation_at(&self, millis: i64) -> StorageResult<u64> {
        let Some(first) = self.entries.first() else {
            return Err(StorageError::backend("history: commit times are not recorded for this store (enable OGD_HISTORY)"));
        };
        if millis < first.millis {
            if first.generation == 0 {
                return Ok(0);
            }
            return Err(StorageError::backend(format!(
                "history: commit times are recorded from {} (generation {}); {} is earlier",
                crate::query::dates::to_iso(first.millis),
                first.generation,
                crate::query::dates::to_iso(millis),
            )));
        }
        // Times may step back (clock changes): the answer is the last
        // generation whose own time is not after `millis`.
        Ok(self.entries.iter().filter(|entry| entry.millis <= millis).map(|entry| entry.generation).max().unwrap_or(first.generation))
    }
}

/// Append-only file of commit records, one per commit, written without
/// sync: a crash may lose the last records (their commits then have no time
/// or author), never the commits themselves.
///
/// ```text
/// record = generation u64 | millis i64 | identity len u16 | identity | device len u16 | device   (little endian)
/// ```
#[derive(Debug)]
pub struct TimelineFile {
    path: PathBuf,
    file: Mutex<File>,
}

impl TimelineFile {
    /// Opens (or creates) the file for appending.
    ///
    /// # Errors
    ///
    /// Fails when the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        let path = path.as_ref().to_owned();
        let file = OpenOptions::new().create(true).append(true).open(&path).map_err(|error| StorageError::backend(format!("history: open timeline {}: {error}", path.display())))?;
        Ok(Self { path, file: Mutex::new(file) })
    }

    #[must_use]
    pub fn path(&self) -> &Path { &self.path }

    /// Appends one record; errors are ignored (the commit is already durable).
    pub fn append(&self, record: &CommitRecord) {
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(&record.generation.to_le_bytes());
        bytes.extend_from_slice(&record.millis.to_le_bytes());
        for part in [record.author.as_ref().map(|author| author.identity.as_ref()), record.author.as_ref().and_then(|author| author.device.as_deref())] {
            let part = part.unwrap_or("").as_bytes();
            let part = &part[..part.len().min(usize::from(u16::MAX))];
            bytes.extend_from_slice(&(part.len() as u16).to_le_bytes());
            bytes.extend_from_slice(part);
        }
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(&bytes);
        }
    }

    /// Reads every complete record of a timeline file (none when it is missing).
    ///
    /// # Errors
    ///
    /// Fails when the file exists but cannot be read.
    pub fn read(path: impl AsRef<Path>) -> StorageResult<Timeline> {
        let path = path.as_ref();
        let mut bytes = Vec::new();
        match File::open(path) {
            Ok(mut file) => { file.read_to_end(&mut bytes).map_err(|error| StorageError::backend(format!("history: read timeline {}: {error}", path.display())))?; }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Timeline::default()),
            Err(error) => return Err(StorageError::backend(format!("history: open timeline {}: {error}", path.display()))),
        }
        let mut entries = Vec::new();
        let mut at = 0usize;
        let take = |at: &mut usize, len: usize| -> Option<&[u8]> {
            let slice = bytes.get(*at..*at + len)?;
            *at += len;
            Some(slice)
        };
        loop {
            let Some(generation) = take(&mut at, 8) else { break };
            let generation = u64::from_le_bytes(generation.try_into().expect("8 bytes"));
            let Some(millis) = take(&mut at, 8) else { break };
            let millis = i64::from_le_bytes(millis.try_into().expect("8 bytes"));
            let mut parts = [None, None];
            let mut complete = true;
            for part in &mut parts {
                let Some(len) = take(&mut at, 2) else { complete = false; break };
                let len = usize::from(u16::from_le_bytes(len.try_into().expect("2 bytes")));
                let Some(text) = take(&mut at, len) else { complete = false; break };
                *part = (!text.is_empty()).then(|| Arc::<str>::from(String::from_utf8_lossy(text).as_ref()));
            }
            if !complete {
                break;
            }
            let [identity, device] = parts;
            let author = identity.map(|identity| CommitAuthor { identity, device });
            entries.push(CommitRecord { generation, millis, author });
        }
        Ok(Timeline::new(entries))
    }
}

/// History of a store at one snapshot.
pub trait HistorySource {
    /// Generation of the snapshot the source describes.
    fn generation(&self) -> u64;

    /// Oldest generation whose state the source can rebuild.
    fn floor(&self) -> u64;

    /// Whether the source holds the state at its floor ([`HistoryOp::Baseline`]
    /// events) and the commit times: false for a backup journal.
    fn keeps_baseline(&self) -> bool { true }

    /// Every collection with events, including those no longer live.
    fn collections(&self) -> StorageResult<Vec<CollectionId>>;

    /// Visits the events of `collection` whose generation is in
    /// `(after, upto]`, in generation order (commit order within one
    /// generation); stops when `visitor` returns `false`. With `ids`, events
    /// of other documents may be left out. With `documents == false`, puts
    /// may come without their document.
    fn events(
        &self,
        collection: &CollectionId,
        after: u64,
        upto: u64,
        ids: Option<&HashSet<DocumentId>>,
        documents: bool,
        visitor: &mut dyn FnMut(HistoryEvent) -> StorageResult<bool>,
    ) -> StorageResult<()>;

    /// Commit times recorded so far.
    fn timeline(&self) -> StorageResult<Timeline>;
}

/// Writes recorded history into a store that has none yet (a backup
/// restored with its history): the documents of the baseline with their
/// versions, then the start of the history at its floor, then each commit
/// with its generation, time and author, in generation order.
pub trait HistoryWriter {
    /// Puts documents as they were when history started, versions kept.
    fn baseline(&mut self, collection: &CollectionId, rows: Vec<StoredDocument>) -> StorageResult<()>;

    /// Starts the history at `base.generation`: the baseline is the state then.
    fn start(&mut self, base: CommitRecord) -> StorageResult<()>;

    /// Commits the events of `record.generation`, above every earlier one.
    fn commit(&mut self, record: CommitRecord, events: Vec<(CollectionId, HistoryEvent)>) -> StorageResult<()>;
}

/// Per-query cache shared by the read views of one temporal query: the
/// resolved generation and the per-collection differences, so the several
/// reads of one query compute them once.
#[derive(Debug, Default)]
pub struct TemporalCache {
    resolved: Mutex<Option<u64>>,
    deltas: Mutex<HashMap<(CollectionId, u64), Arc<AsOfDelta>>>,
    timeline: Mutex<Option<Arc<Timeline>>>,
}

impl TemporalCache {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Generation an `as of` resolves to, against `source`; the first
    /// resolution is kept for the rest of the query.
    ///
    /// # Errors
    ///
    /// Fails when the point is after the current generation, before the
    /// source floor, or not datable.
    pub fn resolve(&self, as_of: AsOf, source: &dyn HistorySource) -> StorageResult<u64> {
        let mut resolved = self.resolved.lock().map_err(|_| StorageError::backend("history cache lock poisoned"))?;
        if let Some(generation) = *resolved {
            return Ok(generation);
        }
        let current = source.generation();
        let generation = match as_of {
            AsOf::Generation(generation) => {
                if generation > current {
                    return Err(StorageError::backend(format!("history: generation {generation} is after the current generation {current}")));
                }
                generation
            }
            AsOf::Time(millis) => self.timeline(source)?.generation_at(millis)?.min(current),
        };
        if generation < source.floor() {
            return Err(StorageError::backend(format!("history: this store keeps history from generation {}; {as_of} is earlier", source.floor())));
        }
        *resolved = Some(generation);
        Ok(generation)
    }

    fn timeline(&self, source: &dyn HistorySource) -> StorageResult<Arc<Timeline>> {
        let mut timeline = self.timeline.lock().map_err(|_| StorageError::backend("history cache lock poisoned"))?;
        if let Some(timeline) = timeline.as_ref() {
            return Ok(Arc::clone(timeline));
        }
        let loaded = Arc::new(source.timeline()?);
        *timeline = Some(Arc::clone(&loaded));
        Ok(loaded)
    }
}

/// How a collection at one generation differs from the current snapshot.
#[derive(Debug, Default)]
pub struct AsOfDelta {
    /// Documents an event touched after the generation: never read from
    /// the current snapshot.
    hidden: HashSet<DocumentId>,
    /// Hidden documents present in the current snapshot.
    hidden_present: u64,
    /// Versions the hidden documents had at the generation.
    rows: BTreeMap<DocumentId, StoredDocument>,
}

impl AsOfDelta {
    fn is_empty(&self) -> bool { self.hidden.is_empty() }

    fn build(source: &dyn HistorySource, collection: &CollectionId, generation: u64) -> StorageResult<Self> {
        let current = source.generation();
        if generation >= current {
            return Ok(Self::default());
        }
        // First and last event of each document touched after `generation`.
        let mut first = HashMap::<DocumentId, (HistoryOp, DocumentVersion)>::new();
        let mut present_now = HashMap::<DocumentId, bool>::new();
        source.events(collection, generation, current, None, false, &mut |event| {
            if event.op == HistoryOp::Clear {
                return Err(StorageError::backend(format!("history: the store was cleared after generation {generation}; an earlier state cannot be rebuilt")));
            }
            first.entry(event.id).or_insert((event.op, event.version));
            present_now.insert(event.id, event.op != HistoryOp::Delete);
            Ok(true)
        })?;
        // A document whose first later event inserts it did not exist then;
        // the others (replaced or deleted later) existed: find their version.
        let needed = first
            .iter()
            .filter(|(_, (op, version))| !(*op == HistoryOp::Put && *version == DocumentVersion::INITIAL))
            .map(|(id, _)| *id)
            .collect::<HashSet<_>>();
        let mut rows = BTreeMap::new();
        if !needed.is_empty() {
            let mut latest = HashMap::<DocumentId, Option<StoredDocument>>::with_capacity(needed.len());
            source.events(collection, 0, generation, Some(&needed), true, &mut |event| {
                match event.op {
                    HistoryOp::Clear => latest.clear(),
                    HistoryOp::Delete if needed.contains(&event.id) => { latest.insert(event.id, None); }
                    HistoryOp::Put | HistoryOp::Baseline if needed.contains(&event.id) => {
                        let document = event.document.ok_or_else(|| StorageError::backend("history source returned a put without its document"))?;
                        latest.insert(event.id, Some(StoredDocument::new(event.id, event.version, document)?));
                    }
                    _ => {}
                }
                Ok(true)
            })?;
            rows.extend(latest.into_iter().filter_map(|(id, stored)| stored.map(|stored| (id, stored))));
        }
        let hidden_present = present_now.values().filter(|present| **present).count() as u64;
        Ok(Self { hidden: first.into_keys().collect(), hidden_present, rows })
    }
}

/// A store as it was at one generation (see the module documentation).
pub struct AsOfRead<'a> {
    current: Box<dyn StorageRead + 'a>,
    source: Box<dyn HistorySource + 'a>,
    generation: u64,
    cache: Arc<TemporalCache>,
}

impl fmt::Debug for AsOfRead<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("AsOfRead").field("generation", &self.generation).finish_non_exhaustive()
    }
}

impl<'a> AsOfRead<'a> {
    /// Builds the view; `current` and `source` describe the same snapshot.
    ///
    /// # Errors
    ///
    /// Fails when `as_of` cannot be resolved (see [`TemporalCache::resolve`]).
    pub fn new(current: Box<dyn StorageRead + 'a>, source: Box<dyn HistorySource + 'a>, as_of: AsOf, cache: Arc<TemporalCache>) -> StorageResult<Self> {
        let generation = cache.resolve(as_of, source.as_ref())?;
        Ok(Self { current, source, generation, cache })
    }

    /// The generation read.
    #[must_use]
    pub const fn generation(&self) -> u64 { self.generation }

    fn delta(&self, collection: &CollectionId) -> StorageResult<Arc<AsOfDelta>> {
        let key = (collection.clone(), self.source.generation());
        if let Some(delta) = self.cache.deltas.lock().map_err(|_| StorageError::backend("history cache lock poisoned"))?.get(&key) {
            return Ok(Arc::clone(delta));
        }
        let delta = Arc::new(AsOfDelta::build(self.source.as_ref(), collection, self.generation)?);
        self.cache.deltas.lock().map_err(|_| StorageError::backend("history cache lock poisoned"))?.insert(key, Arc::clone(&delta));
        Ok(delta)
    }

    /// The delta when it is not empty: the collection then needs the merged path.
    fn changed(&self, collection: &CollectionId) -> StorageResult<Option<Arc<AsOfDelta>>> {
        let delta = self.delta(collection)?;
        Ok((!delta.is_empty()).then_some(delta))
    }
}

/// Merged reads of a changed collection: native ordered scan and lookups,
/// every other cursor from the trait defaults on top of them.
struct Merged<'v, 'a> {
    view: &'v AsOfRead<'a>,
    delta: Arc<AsOfDelta>,
}

impl StorageRead for Merged<'_, '_> {
    fn get(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> {
        if self.delta.hidden.contains(id) {
            return Ok(self.delta.rows.get(id).cloned());
        }
        self.view.current.get(collection, id)
    }

    fn scan(&self, collection: &CollectionId, options: ScanOptions) -> StorageResult<Vec<StoredDocument>> {
        crate::helpers::collect_scan(|visit| self.scan_each(collection, options, visit))
    }

    fn scan_each(&self, collection: &CollectionId, options: ScanOptions, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> {
        let limit = options.limit().unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(());
        }
        let forward = options.direction() == ScanDirection::Forward;
        let mut extra: Box<dyn Iterator<Item = &StoredDocument>> = if forward { Box::new(self.delta.rows.values()) } else { Box::new(self.delta.rows.values().rev()) };
        let mut pending = extra.next();
        let mut emitted = 0usize;
        let mut stopped = false;
        let inner_options = match options.limit() {
            Some(limit) => ScanOptions::new().with_direction(options.direction()).with_limit(limit.saturating_add(self.delta.hidden.len())),
            None => ScanOptions::new().with_direction(options.direction()),
        };
        self.view.current.scan_each(collection, inner_options, &mut |stored| {
            if self.delta.hidden.contains(stored.id()) {
                return Ok(true);
            }
            while let Some(row) = pending.filter(|row| if forward { row.id() < stored.id() } else { row.id() > stored.id() }) {
                if !visitor(row.clone())? {
                    stopped = true;
                    return Ok(false);
                }
                emitted += 1;
                if emitted >= limit {
                    stopped = true;
                    return Ok(false);
                }
                pending = extra.next();
            }
            if !visitor(stored)? {
                stopped = true;
                return Ok(false);
            }
            emitted += 1;
            if emitted >= limit {
                stopped = true;
                return Ok(false);
            }
            Ok(true)
        })?;
        while !stopped && emitted < limit {
            let Some(row) = pending else { break };
            if !visitor(row.clone())? {
                break;
            }
            emitted += 1;
            pending = extra.next();
        }
        Ok(())
    }

    fn count(&self, collection: &CollectionId) -> StorageResult<u64> {
        let current = self.view.current.count(collection)?;
        Ok(current.saturating_sub(self.delta.hidden_present) + self.delta.rows.len() as u64)
    }

    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> {
        Ok(self.count(collection)? > 0)
    }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        self.view.current.collections()
    }
}

/// Dispatches one read: to the current snapshot when the collection did not
/// change since the generation, to the merged reader otherwise.
macro_rules! as_of_dispatch {
    ($view:expr, $collection:expr, |$read:ident| $body:expr) => {{
        match $view.changed($collection)? {
            None => { let $read: &dyn StorageRead = $view.current.as_ref(); $body }
            Some(delta) => { let merged = Merged { view: $view, delta }; let $read: &dyn StorageRead = &merged; $body }
        }
    }};
}

impl StorageRead for AsOfRead<'_> {
    fn support(&self, capability: StorageReadCapability) -> StorageSupport { self.current.support(capability) }

    fn top_level_fields(&self, collection: &CollectionId) -> StorageResult<Option<Vec<String>>> {
        match self.changed(collection)? {
            None => self.current.top_level_fields(collection),
            Some(_) => Ok(None),
        }
    }

    fn field_facts(&self, collection: &CollectionId) -> StorageResult<Option<FieldFacts>> {
        match self.changed(collection)? {
            None => self.current.field_facts(collection),
            Some(_) => Ok(None),
        }
    }

    fn field_bounds(&self, collection: &CollectionId) -> StorageResult<Option<BTreeMap<String, FieldBounds>>> {
        match self.changed(collection)? {
            None => self.current.field_bounds(collection),
            Some(_) => Ok(None),
        }
    }

    fn visit_row_ids(&self, collection: &CollectionId, visitor: &mut dyn FnMut(DocumentId, DocumentVersion) -> StorageResult<bool>) -> StorageResult<()> {
        as_of_dispatch!(self, collection, |read| read.visit_row_ids(collection, visitor))
    }

    fn get(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> {
        as_of_dispatch!(self, collection, |read| read.get(collection, id))
    }

    fn scan(&self, collection: &CollectionId, options: ScanOptions) -> StorageResult<Vec<StoredDocument>> {
        as_of_dispatch!(self, collection, |read| read.scan(collection, options))
    }

    fn scan_each(&self, collection: &CollectionId, options: ScanOptions, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> {
        as_of_dispatch!(self, collection, |read| read.scan_each(collection, options, visitor))
    }

    fn scan_projected_row_refs_unordered_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], visitor: &mut dyn for<'r> FnMut(DocumentId, DocumentVersion, &[Option<ProjectedValueRef<'r>>]) -> StorageResult<bool>) -> StorageResult<()> {
        as_of_dispatch!(self, collection, |read| read.scan_projected_row_refs_unordered_each(collection, options, fields, visitor))
    }

    fn supports_projected_blocks(&self) -> bool { self.current.supports_projected_blocks() }

    fn sample_row_ids(&self, collection: &CollectionId, count: usize, seed: u64) -> StorageResult<Option<(u64, Vec<(DocumentId, DocumentVersion)>)>> {
        match self.changed(collection)? {
            None => self.current.sample_row_ids(collection, count, seed),
            Some(_) => Ok(None),
        }
    }

    fn scan_projected_blocks_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], skip: &dyn Fn(&[Option<GroupFacts<'_>>]) -> bool, gate: Option<BlockGate<'_>>, visitor: &mut dyn for<'r> FnMut(&ProjectedBlock<'_, 'r>) -> StorageResult<bool>) -> StorageResult<()> {
        match self.changed(collection)? {
            None => self.current.scan_projected_blocks_each(collection, options, fields, skip, gate, visitor),
            Some(delta) => Merged { view: self, delta }.scan_projected_blocks_each(collection, options, fields, skip, gate, visitor),
        }
    }

    fn scan_projected_row_values_gated_unordered_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], gate_field_count: usize, gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>, visitor: &mut dyn FnMut(DocumentId, DocumentVersion, &[Option<Value>]) -> StorageResult<bool>) -> StorageResult<()> {
        as_of_dispatch!(self, collection, |read| read.scan_projected_row_values_gated_unordered_each(collection, options, fields, gate_field_count, gate, visitor))
    }

    fn scan_projected_gated_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> {
        as_of_dispatch!(self, collection, |read| read.scan_projected_gated_each(collection, options, fields, gate, visitor))
    }

    fn count(&self, collection: &CollectionId) -> StorageResult<u64> {
        as_of_dispatch!(self, collection, |read| read.count(collection))
    }

    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> {
        as_of_dispatch!(self, collection, |read| read.collection_exists(collection))
    }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        self.current.collections()
    }
}

/// Identifier of the `position`-th event of `generation` in a history view.
#[must_use]
pub fn history_row_id(generation: u64, position: u64) -> DocumentId {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&generation.to_be_bytes());
    bytes[8..].copy_from_slice(&position.to_be_bytes());
    DocumentId::from_bytes(bytes)
}

fn history_row_position(id: &DocumentId) -> (u64, u64) {
    let bytes = id.as_bytes();
    (u64::from_be_bytes(bytes[..8].try_into().expect("8 bytes")), u64::from_be_bytes(bytes[8..].try_into().expect("8 bytes")))
}

/// Every stored version of one collection (see the module documentation);
/// other collections read from the current snapshot.
pub struct HistoryRead<'a> {
    current: Box<dyn StorageRead + 'a>,
    source: Box<dyn HistorySource + 'a>,
    collection: CollectionId,
    timeline: Timeline,
    /// Documents deleted at least once: their last version is kept while
    /// streaming so a delete row carries the document it removed.
    deleted: Mutex<Option<Arc<HashSet<DocumentId>>>>,
}

impl fmt::Debug for HistoryRead<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("HistoryRead").field("collection", &self.collection).finish_non_exhaustive()
    }
}

impl<'a> HistoryRead<'a> {
    /// Builds the view; `current` and `source` describe the same snapshot.
    ///
    /// # Errors
    ///
    /// Fails when the source cannot read its timeline.
    pub fn new(current: Box<dyn StorageRead + 'a>, source: Box<dyn HistorySource + 'a>, collection: CollectionId) -> StorageResult<Self> {
        let timeline = source.timeline()?;
        Ok(Self { current, source, collection, timeline, deleted: Mutex::new(None) })
    }

    fn deleted(&self) -> StorageResult<Arc<HashSet<DocumentId>>> {
        let mut deleted = self.deleted.lock().map_err(|_| StorageError::backend("history lock poisoned"))?;
        if let Some(deleted) = deleted.as_ref() {
            return Ok(Arc::clone(deleted));
        }
        let mut ids = HashSet::new();
        self.source.events(&self.collection, 0, self.source.generation(), None, false, &mut |event| {
            if event.op == HistoryOp::Delete {
                ids.insert(event.id);
            }
            Ok(true)
        })?;
        let ids = Arc::new(ids);
        *deleted = Some(Arc::clone(&ids));
        Ok(ids)
    }

    fn row(&self, event: &HistoryEvent, position: u64, removed: Option<&StoredDocument>) -> StorageResult<StoredDocument> {
        let (op, version, base) = match event.op {
            HistoryOp::Put => (if event.version == DocumentVersion::INITIAL { "insert" } else { "replace" }, Some(event.version), event.document.as_deref()),
            HistoryOp::Baseline => ("baseline", Some(event.version), event.document.as_deref()),
            HistoryOp::Delete => ("delete", removed.map(StoredDocument::version), removed.map(StoredDocument::document)),
            HistoryOp::Clear => ("clear", None, None),
        };
        let mut document = base.cloned().unwrap_or_default();
        document.insert(HISTORY_DOC_ID_FIELD, Value::string(event.id.to_string()));
        document.insert(HISTORY_VERSION_FIELD, version.map_or(Value::Null, |version| Value::unsigned(version.get())));
        document.insert(HISTORY_OP_FIELD, Value::string(op));
        document.insert(HISTORY_GENERATION_FIELD, Value::unsigned(event.generation));
        document.insert(HISTORY_AT_FIELD, self.timeline.time_of(event.generation).map_or(Value::Null, |millis| Value::string(crate::query::dates::to_iso(millis))));
        let author = self.timeline.author_of(event.generation);
        document.insert(HISTORY_BY_FIELD, author.map_or(Value::Null, |author| Value::String(Arc::clone(&author.identity))));
        document.insert(HISTORY_BY_DEVICE_FIELD, author.and_then(|author| author.device.clone()).map_or(Value::Null, Value::String));
        StoredDocument::new(history_row_id(event.generation, position), DocumentVersion::INITIAL, Arc::new(document))
    }

    /// Streams the rows of generations `(after, upto]` in generation order.
    fn stream(&self, after: u64, upto: u64, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> {
        let deleted = self.deleted()?;
        // Last version of each deleted document, as the stream reaches it.
        let mut last = HashMap::<DocumentId, StoredDocument>::new();
        if after > 0 && !deleted.is_empty() {
            self.source.events(&self.collection, 0, after, Some(&deleted), true, &mut |event| {
                remember(&mut last, &deleted, &event)?;
                Ok(true)
            })?;
        }
        let mut generation = u64::MAX;
        let mut position = 0u64;
        self.source.events(&self.collection, after, upto, None, true, &mut |event| {
            if event.generation == generation {
                position += 1;
            } else {
                generation = event.generation;
                position = 0;
            }
            let removed = if event.op == HistoryOp::Delete { last.remove(&event.id) } else { None };
            let row = self.row(&event, position, removed.as_ref())?;
            remember(&mut last, &deleted, &event)?;
            visitor(row)
        })
    }
}

fn remember(last: &mut HashMap<DocumentId, StoredDocument>, deleted: &HashSet<DocumentId>, event: &HistoryEvent) -> StorageResult<()> {
    match event.op {
        HistoryOp::Put | HistoryOp::Baseline if deleted.contains(&event.id) => {
            if let Some(document) = &event.document {
                last.insert(event.id, StoredDocument::new(event.id, event.version, Arc::clone(document))?);
            }
        }
        HistoryOp::Clear => last.clear(),
        _ => {}
    }
    Ok(())
}

impl StorageRead for HistoryRead<'_> {
    fn get(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> {
        if *collection != self.collection {
            return self.current.get(collection, id);
        }
        let (generation, position) = history_row_position(id);
        if generation == 0 || generation > self.source.generation() {
            return Ok(None);
        }
        let mut found = None;
        self.stream(generation - 1, generation, &mut |row| {
            if history_row_position(row.id()).1 == position {
                found = Some(row);
                return Ok(false);
            }
            Ok(true)
        })?;
        Ok(found)
    }

    fn scan(&self, collection: &CollectionId, options: ScanOptions) -> StorageResult<Vec<StoredDocument>> {
        crate::helpers::collect_scan(|visit| self.scan_each(collection, options, visit))
    }

    fn scan_each(&self, collection: &CollectionId, options: ScanOptions, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> {
        if *collection != self.collection {
            return self.current.scan_each(collection, options, visitor);
        }
        let limit = options.limit().unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(());
        }
        match options.direction() {
            ScanDirection::Forward => {
                let mut emitted = 0usize;
                self.stream(0, self.source.generation(), &mut |row| {
                    emitted += 1;
                    Ok(visitor(row)? && emitted < limit)
                })
            }
            ScanDirection::Reverse => {
                let mut rows = Vec::new();
                self.stream(0, self.source.generation(), &mut |row| {
                    rows.push(row);
                    Ok(true)
                })?;
                for row in rows.into_iter().rev().take(limit) {
                    if !visitor(row)? {
                        break;
                    }
                }
                Ok(())
            }
        }
    }

    fn count(&self, collection: &CollectionId) -> StorageResult<u64> {
        if *collection != self.collection {
            return self.current.count(collection);
        }
        let mut count = 0u64;
        self.source.events(&self.collection, 0, self.source.generation(), None, false, &mut |_| {
            count += 1;
            Ok(true)
        })?;
        Ok(count)
    }

    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> {
        if *collection != self.collection {
            return self.current.collection_exists(collection);
        }
        let mut any = false;
        self.source.events(&self.collection, 0, self.source.generation(), None, false, &mut |_| {
            any = true;
            Ok(false)
        })?;
        Ok(any)
    }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        self.current.collections()
    }
}

/// Storage seen by a temporal query: reads go to the temporal view, writes
/// (a terminal `into`) to the current store.
pub struct TemporalStorage {
    inner: Arc<dyn super::StorageEngine>,
    temporal: Temporal,
    cache: Arc<TemporalCache>,
}

impl TemporalStorage {
    #[must_use]
    pub fn new(inner: Arc<dyn super::StorageEngine>, temporal: Temporal) -> Self {
        Self { inner, temporal, cache: Arc::new(TemporalCache::new()) }
    }

    #[must_use]
    pub const fn temporal(&self) -> &Temporal { &self.temporal }
}

impl fmt::Debug for TemporalStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("TemporalStorage").field("temporal", &self.temporal).finish_non_exhaustive()
    }
}

impl super::StorageEngine for TemporalStorage {
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>> { self.inner.read_temporal(&self.temporal, &self.cache) }

    fn read_temporal(&self, temporal: &Temporal, cache: &Arc<TemporalCache>) -> StorageResult<Box<dyn StorageRead + '_>> { self.inner.read_temporal(temporal, cache) }

    fn begin(&self) -> StorageResult<Box<dyn super::StorageTransaction + '_>> { self.inner.begin() }

    fn apply_batch_atomic(&self, collection: &CollectionId, mutations: Vec<super::StorageMutation>) -> StorageResult<(Vec<StoredDocument>, super::CommitResult)> {
        self.inner.apply_batch_atomic(collection, mutations)
    }

    fn apply_batch_atomic_summary(&self, collection: &CollectionId, mutations: Vec<super::StorageMutation>) -> StorageResult<super::CommitResult> {
        self.inner.apply_batch_atomic_summary(collection, mutations)
    }
}

/// Builds the read view of a temporal query from a backend's current
/// snapshot and history source.
///
/// # Errors
///
/// Fails when the point in time cannot be resolved.
pub fn temporal_read<'a>(current: Box<dyn StorageRead + 'a>, source: Box<dyn HistorySource + 'a>, temporal: &Temporal, cache: &Arc<TemporalCache>) -> StorageResult<Box<dyn StorageRead + 'a>> {
    match temporal {
        Temporal::AsOf(as_of) => {
            let view = AsOfRead::new(current, source, *as_of, Arc::clone(cache))?;
            if view.generation == view.source.generation() {
                return Ok(view.current);
            }
            Ok(Box::new(view))
        }
        Temporal::History { collection } => Ok(Box::new(HistoryRead::new(current, source, collection.clone())?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(generation: u64, millis: i64) -> CommitRecord { CommitRecord { generation, millis, author: None } }

    #[test] fn timeline_resolves_the_last_generation_at_a_time() { let timeline = Timeline::new(vec![record(3, 300), record(1, 100), record(2, 200), record(2, 250)]); assert_eq!(timeline.time_of(2), Some(250)); assert_eq!(timeline.generation_at(260).unwrap(), 2); assert_eq!(timeline.generation_at(1_000).unwrap(), 3); assert!(timeline.generation_at(50).is_err()); assert_eq!(Timeline::new(vec![record(0, 10), record(1, 20)]).generation_at(5).unwrap(), 0); assert!(Timeline::default().generation_at(5).is_err()); }

    #[test] fn timeline_file_round_trips_records_and_ignores_a_torn_tail() { let path = std::env::temp_dir().join(format!("og-timeline-{}", std::process::id())); let _ = std::fs::remove_file(&path); let file = TimelineFile::open(&path).unwrap(); let author = CommitAuthor { identity: Arc::from("alice"), device: Some(Arc::from("laptop")) }; file.append(&CommitRecord { generation: 1, millis: 1_000, author: Some(author.clone()) }); file.append(&record(2, 2_000)); drop(file); let mut bytes = std::fs::read(&path).unwrap(); bytes.extend_from_slice(&[3, 0, 0]); std::fs::write(&path, bytes).unwrap(); let timeline = TimelineFile::read(&path).unwrap(); assert_eq!(timeline.author_of(1), Some(&author)); assert_eq!(timeline.time_of(2), Some(2_000)); assert_eq!(timeline.author_of(2), None); assert_eq!(timeline.time_of(3), None); std::fs::remove_file(&path).unwrap(); }

    #[test] fn commit_author_is_scoped_to_the_closure() { let author = CommitAuthor { identity: Arc::from("bob"), device: None }; assert_eq!(current_commit_author(), None); with_commit_author(Some(author.clone()), || assert_eq!(current_commit_author(), Some(author.clone()))); assert_eq!(current_commit_author(), None); }

    #[test] fn history_row_ids_order_by_generation_then_position() { assert!(history_row_id(2, 0) > history_row_id(1, 7)); assert!(history_row_id(2, 1) > history_row_id(2, 0)); assert_eq!(history_row_position(&history_row_id(9, 4)), (9, 4)); }
}
