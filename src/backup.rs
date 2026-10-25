#![cfg_attr(rustfmt, rustfmt_skip)]
//! Streaming, backend-agnostic OG backup/restore format.
//!
//! Version 2 (`OGB2`) writes each collection as blocks of up to
//! [`BLOCK_ROWS`] documents laid out by field: the identifiers as deltas, then
//! per field a tag per row and one value stream (integers as zigzag deltas,
//! strings through a dictionary of the block, nested values in the query
//! codec), the whole block compressed with zstd. Values come back with their
//! exact types. Version 1 (`OGB1`, one JSON document per record) is still read.
//!
//! A backup made from a store keeping history or a backup journal names its
//! generation and the store's lineage (a random id kept in `_backup`, renewed
//! by every restore). A diff names its parent backup and holds, for each
//! collection changed since the parent's generation, the documents as they
//! are (blocks) and the ones removed (identifier blocks), then the live
//! collections. Restoring a diff restores its chain: the full backup it
//! starts from, then each diff in order, all checked before the store is
//! touched.
//!
//! A full backup with history also holds, from a store keeping history, the
//! recorded events of every collection (the state when history started, then
//! each write and delete with its generation and version) and the time and
//! author of each commit. Restored into a new store ([`restore_history`]),
//! it gives back `as of` and `history` over the whole period, with the
//! original generations.

use crate::model::{Document, FieldName, Number, Value};
use crate::query::codec::{encode_value, encode_varint, Decoder};
use crate::storage::{
    history::{CommitAuthor, CommitRecord, HistoryEvent, HistoryOp, HistorySource}, CollectionId, DocumentId, DocumentVersion,
    ScanOptions, StorageEngine, StorageError, StorageMutation, StorageRead, StoredDocument, UuidV7Generator, VersionPrecondition,
};
use crate::{helpers::json_object_to_document, query::vcollections};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::{BTreeSet, HashMap},
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

const MAGIC: &[u8; 4] = b"OGB2";
const MAGIC_V1: &[u8; 4] = b"OGB1";
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_BLOCK_BYTES: u64 = 1 << 30;
const RESTORE_BATCH: usize = 256;
const BLOCK_ROWS: usize = 65_536;
/// Rows of version 2 written by one commit: large commits go straight to segments.
const RESTORE_ROWS: usize = 8 * BLOCK_ROWS;
const LEVEL: i32 = 3;
const MISSING: u8 = 0;
const NULL: u8 = 1;
const FALSE: u8 = 2;
const TRUE: u8 = 3;
const SIGNED: u8 = 4;
const UNSIGNED: u8 = 5;
const FLOAT: u8 = 6;
const STRING: u8 = 7;
const NESTED: u8 = 8;
/// Collection holding the store's lineage.
const LINEAGE: &str = "_backup";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSummary {
    pub collections: u64,
    pub documents: u64,
    /// Documents a diff removes.
    #[serde(default)]
    pub removed: u64,
    /// Recorded events a full backup with history holds.
    #[serde(default)]
    pub events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSource {
    pub instance_id: String,
    pub hostname: String,
    pub platform: String,
    pub arch: String,
    pub core_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupMetadata {
    pub created_at: u64,
    pub source: BackupSource,
}

/// Where a backup sits in a chain: its generation, the store's lineage, and for a diff its parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupChain {
    pub id: String,
    pub lineage: String,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<BackupLink>,
    /// A full backup with history: the generation its history starts at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupLink {
    pub id: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupInfo {
    pub format: String,
    pub version: u16,
    pub created_at: u64,
    pub size_bytes: u64,
    pub collections: u64,
    pub documents: u64,
    pub removed: u64,
    pub events: u64,
    pub source: BackupSource,
    pub chain: Option<BackupChain>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Record {
    Header {
        version: u16,
        created_at: u64,
        source: BackupSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        chain: Option<BackupChain>,
    },
    /// A collection; in a diff, `documents` changed since the parent.
    Collection {
        name: String,
        documents: u64,
    },
    /// Version 1: one document.
    Document {
        id: String,
        data: JsonValue,
    },
    /// Version 2: `rows` documents, followed by `bytes` bytes of zstd holding `raw` bytes of block.
    Block {
        rows: u64,
        raw: u64,
        bytes: u64,
    },
    /// A diff: `rows` documents removed, a block of identifiers laid out like [`Record::Block`].
    Removed {
        rows: u64,
        raw: u64,
        bytes: u64,
    },
    /// A diff: the collections at its generation; the others are dropped.
    Live {
        names: Vec<String>,
    },
    /// History: the events of a collection follow, in [`Record::Events`] blocks in generation order.
    History {
        name: String,
    },
    /// History: `rows` events, a block of their documents laid out like [`Record::Block`] then their operations,
    /// generations and versions.
    Events {
        rows: u64,
        raw: u64,
        bytes: u64,
    },
    /// History: the time and author of `rows` commits.
    Timeline {
        rows: u64,
        raw: u64,
        bytes: u64,
    },
    End {
        collections: u64,
        documents: u64,
        #[serde(default)]
        removed: u64,
        #[serde(default)]
        events: u64,
    },
}

pub use crate::error::BackupError;

/// A full backup of the current state.
pub fn create( storage: &dyn StorageEngine, path: &Path, metadata: BackupMetadata, ) -> Result<BackupSummary, BackupError> {
    full(storage, path, metadata, false)
}

/// A full backup of the current state and of the history the store keeps (`OGD_HISTORY=on`).
pub fn create_with_history( storage: &dyn StorageEngine, path: &Path, metadata: BackupMetadata, ) -> Result<BackupSummary, BackupError> {
    full(storage, path, metadata, true)
}

fn full( storage: &dyn StorageEngine, path: &Path, metadata: BackupMetadata, with_history: bool, ) -> Result<BackupSummary, BackupError> {
    if storage.history().is_ok() && lineage(&*storage.read()?)?.is_none() {
        renew_lineage(storage)?;
    }
    let (snapshot, source) = match storage.history() {
        Ok((snapshot, source)) => (snapshot, Some(source)),
        Err(_) => (storage.read()?, None),
    };
    if with_history && !source.as_ref().is_some_and(|source| source.keeps_baseline()) {
        return Err(BackupError::Invalid("a backup with history needs a store keeping history (OGD_HISTORY=on)".into()));
    }
    let chain = match (&source, lineage(&*snapshot)?) {
        (Some(source), Some(lineage)) => Some(BackupChain { id: new_id(), lineage, generation: source.generation(), parent: None, history: with_history.then(|| source.floor()) }),
        _ => None,
    };
    let collections = snapshot.collections()?.into_iter().filter(|c| !is_virtual(c.as_str())).collect::<Vec<_>>();
    let mut out = BufWriter::new(File::create(path)?);
    out.write_all(MAGIC)?;
    write_record(&mut out, &Record::Header { version: 2, created_at: metadata.created_at, source: metadata.source, chain: chain.clone() })?;
    let mut summary = BackupSummary::default();
    let mut rows = Vec::with_capacity(BLOCK_ROWS);
    for collection in collections {
        let count = snapshot.count(&collection)?;
        write_record(&mut out, &Record::Collection { name: collection.to_string(), documents: count })?;
        summary.collections += 1;
        let mut visitor = |stored: crate::storage::StoredDocument| {
            rows.push((*stored.id(), stored.shared_document()));
            if rows.len() == BLOCK_ROWS {
                write_block(&mut out, &rows, false).map_err(|error| StorageError::backend(error.to_string()))?;
                rows.clear();
            }
            Ok(true)
        };
        snapshot.scan_each(&collection, ScanOptions::default(), &mut visitor)?;
        if !rows.is_empty() {
            write_block(&mut out, &rows, false)?;
        }
        summary.documents += count;
        rows.clear();
    }
    if let Some(source) = source.as_deref().filter(|_| with_history) {
        summary.events = write_history(&mut out, source)?;
    }
    write_record(&mut out, &Record::End { collections: summary.collections, documents: summary.documents, removed: 0, events: summary.events })?;
    out.flush()?;
    out.into_inner().map_err(|error| error.into_error())?.sync_all()?;
    drop((snapshot, source));
    if let Some(chain) = chain {
        storage.release_history(chain.generation)?;
    }
    Ok(summary)
}

/// A diff from the backup at `parent`: what changed since its generation. Needs a store keeping history or a
/// backup journal, the parent's lineage, and the commits after the parent's generation.
pub fn create_diff( storage: &dyn StorageEngine, path: &Path, metadata: BackupMetadata, parent: &Path, ) -> Result<BackupSummary, BackupError> {
    let parent = header(parent)?.3.ok_or_else(|| BackupError::Invalid("the parent backup names no generation: it was made without history nor backup journal".into()))?;
    let (snapshot, source) = storage.history().map_err(|_| BackupError::Invalid("a diff needs history or a backup journal (OGD_HISTORY=on or OGD_HISTORY=journal)".into()))?;
    let lineage = lineage(&*snapshot)?.filter(|lineage| *lineage == parent.lineage)
        .ok_or_else(|| BackupError::Invalid("the store is not the one the parent backup was made from (restored since, or another store): make a full backup".into()))?;
    let generation = source.generation();
    if source.floor() > parent.generation || generation < parent.generation {
        return Err(BackupError::Invalid(format!("the store keeps the commits after generation {}, the parent backup is at generation {}: make a full backup", source.floor(), parent.generation)));
    }
    let collections = snapshot.collections()?.into_iter().filter(|c| !is_virtual(c.as_str())).collect::<Vec<_>>();
    let mut out = BufWriter::new(File::create(path)?);
    out.write_all(MAGIC)?;
    let chain = BackupChain { id: new_id(), lineage, generation, parent: Some(BackupLink { id: parent.id, generation: parent.generation }), history: None };
    write_record(&mut out, &Record::Header { version: 2, created_at: metadata.created_at, source: metadata.source, chain: Some(chain) })?;
    let mut summary = BackupSummary::default();
    let empty = Arc::new(Document::new());
    for collection in &collections {
        // The documents touched since the parent are as the snapshot holds them, or removed.
        let mut touched = BTreeSet::new();
        source.events(collection, parent.generation, generation, None, false, &mut |event| {
            if event.op == HistoryOp::Clear {
                return Err(StorageError::backend(format!("{collection} was cleared since the parent backup: make a full backup")));
            }
            touched.insert(event.id);
            Ok(true)
        })?;
        if touched.is_empty() {
            continue;
        }
        write_record(&mut out, &Record::Collection { name: collection.to_string(), documents: touched.len() as u64 })?;
        summary.collections += 1;
        let ids = touched.into_iter().collect::<Vec<_>>();
        for ids in ids.chunks(BLOCK_ROWS) {
            let (mut rows, mut removed) = (Vec::new(), Vec::new());
            snapshot.get_each(collection, ids, &mut |at, stored| {
                match stored { Some(stored) => rows.push((ids[at], stored.shared_document())), None => removed.push((ids[at], Arc::clone(&empty))) }
                Ok(true)
            })?;
            for (rows, removal) in [(&rows, false), (&removed, true)] {
                if !rows.is_empty() {
                    write_block(&mut out, rows, removal)?;
                }
            }
            summary.documents += rows.len() as u64;
            summary.removed += removed.len() as u64;
        }
    }
    write_record(&mut out, &Record::Live { names: collections.iter().map(ToString::to_string).collect() })?;
    write_record(&mut out, &Record::End { collections: summary.collections, documents: summary.documents, removed: summary.removed, events: 0 })?;
    out.flush()?;
    out.into_inner().map_err(|error| error.into_error())?.sync_all()?;
    Ok(summary)
}

pub fn inspect(path: &Path) -> Result<BackupInfo, BackupError> {
    let size_bytes = std::fs::metadata(path)?.len();
    let mut reader = open_reader(path)?;
    let (version, created_at, source, chain) = read_header(&mut reader)?;
    let mut summary = BackupSummary::default();
    loop {
        match read_record(&mut reader)? {
            Record::Header { .. } => return Err(BackupError::Invalid("duplicate header".into())),
            Record::Collection { .. } => summary.collections += 1,
            Record::Document { .. } => summary.documents += 1,
            Record::Block { rows, bytes, .. } => {
                summary.documents += rows;
                std::io::copy(&mut reader.by_ref().take(bytes), &mut std::io::sink())?;
            }
            Record::Removed { rows, bytes, .. } => {
                summary.removed += rows;
                std::io::copy(&mut reader.by_ref().take(bytes), &mut std::io::sink())?;
            }
            Record::Events { rows, bytes, .. } => {
                summary.events += rows;
                std::io::copy(&mut reader.by_ref().take(bytes), &mut std::io::sink())?;
            }
            Record::Timeline { bytes, .. } => { std::io::copy(&mut reader.by_ref().take(bytes), &mut std::io::sink())?; }
            Record::Live { .. } | Record::History { .. } => {}
            Record::End { collections, documents, removed, events } => {
                if summary.collections != collections || summary.documents != documents || summary.removed != removed || summary.events != events {
                    return Err(BackupError::Invalid("footer counters do not match stream".into()));
                }
                return Ok(BackupInfo { format: "ogb".to_owned(), version, created_at, size_bytes, collections, documents, removed, events, source, chain });
            }
        }
    }
}

/// Restores the backup at `path`: a full backup, or a diff with its chain (the full backup it starts from and the
/// diffs between, found next to it), checked before the store is touched. The store then gets a new lineage: the
/// next backup is a full one.
pub fn restore( storage: &dyn StorageEngine, path: &Path, replace: bool, ) -> Result<BackupSummary, BackupError> {
    let chain = chain(path)?;
    if replace {
        clear_persistent(storage)?;
    } else {
        let read = storage.read()?;
        for collection in read.collections()? {
            if !is_virtual(collection.as_str()) && read.count(&collection)? != 0 {
                return Err(BackupError::Invalid("restore target is not empty; use replace=true".into()));
            }
        }
    }
    let mut summary = BackupSummary::default();
    for (index, path) in chain.iter().enumerate() {
        let applied = replay(storage, path, index > 0)?;
        summary = BackupSummary { collections: summary.collections.max(applied.collections), documents: summary.documents + applied.documents, removed: summary.removed + applied.removed, events: 0 };
    }
    if storage.history().is_ok() || lineage(&*storage.read()?)?.is_some() {
        renew_lineage(storage)?;
    }
    Ok(summary)
}

/// The backups to restore for `path`, from the full one: each diff's parent is found by its id among the backups
/// of the same directory, at the generation and of the lineage the diff names, and every file reads to its end.
fn chain(path: &Path) -> Result<Vec<PathBuf>, BackupError> {
    let mut chain = vec![(path.to_owned(), header(path)?.3)];
    let mut found = None::<HashMap<String, (PathBuf, BackupChain)>>;
    while let Some((_, Some(BackupChain { lineage, parent: Some(link), .. }))) = chain.last().cloned() {
        let found = match &mut found {
            Some(found) => found,
            None => found.insert(std::fs::read_dir(path.parent().unwrap_or(Path::new(".")))?.flatten().map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|extension| extension == "ogb"))
                .filter_map(|path| Some((path.clone(), header(&path).ok()?.3?)))
                .map(|(path, chain)| (chain.id.clone(), (path, chain))).collect()),
        };
        let (parent, parent_chain) = found.get(&link.id).cloned().ok_or_else(|| BackupError::Invalid(format!("the chain is broken: parent backup {} not found", link.id)))?;
        if parent_chain.generation != link.generation || parent_chain.lineage != lineage || chain.len() > found.len() {
            return Err(BackupError::Invalid(format!("the chain is broken: {} is not the parent the diff names", parent.display())));
        }
        chain.push((parent, Some(parent_chain)));
    }
    chain.reverse();
    for (path, _) in &chain {
        inspect(path)?;
    }
    Ok(chain.into_iter().map(|(path, _)| path).collect())
}

/// Writes the records of one backup into the store: a full backup inserts, a diff writes each document as it is,
/// removes the others it names and drops the collections no longer live.
fn replay(storage: &dyn StorageEngine, path: &Path, diff: bool) -> Result<BackupSummary, BackupError> {
    let mut reader = open_reader(path)?;
    read_header(&mut reader)?;
    let mut current: Option<CollectionId> = None;
    let mut batch: Vec<StorageMutation> = Vec::with_capacity(RESTORE_BATCH);
    let mut summary = BackupSummary::default();
    loop {
        match read_record(&mut reader)? {
            Record::Header { .. } => return Err(BackupError::Invalid("duplicate header".into())),
            Record::Collection { name, .. } => {
                flush_batch(storage, current.as_ref(), &mut batch)?;
                current = Some(CollectionId::parse(name)?);
                summary.collections += 1;
            }
            Record::Document { id, data } => {
                let collection = current.as_ref().ok_or_else(|| BackupError::Invalid("document before collection".into()))?;
                let JsonValue::Object(data) = data else {
                    return Err(BackupError::Invalid("document payload is not an object".into()));
                };
                let document = json_object_to_document(&data).map_err(BackupError::Invalid)?;
                batch.push(StorageMutation::insert(DocumentId::parse(id)?, Arc::new(document)));
                summary.documents += 1;
                if batch.len() >= RESTORE_BATCH {
                    flush_batch(storage, Some(collection), &mut batch)?;
                }
            }
            record @ (Record::Block { .. } | Record::Removed { .. }) => {
                let (Record::Block { rows, raw, bytes } | Record::Removed { rows, raw, bytes }) = record else { unreachable!() };
                let removal = matches!(record, Record::Removed { .. });
                if raw > MAX_BLOCK_BYTES || bytes > MAX_BLOCK_BYTES {
                    return Err(BackupError::Invalid("block too large".into()));
                }
                let mut packed = vec![0u8; bytes as usize];
                reader.read_exact(&mut packed)?;
                let block = decode_block(&zstd::bulk::decompress(&packed, raw as usize)?)?;
                if block.len() as u64 != rows {
                    return Err(BackupError::Invalid("block row count does not match".into()));
                }
                let collection = current.as_ref().ok_or_else(|| BackupError::Invalid("block before collection".into()))?;
                if removal {
                    summary.removed += rows;
                    let mut tx = storage.begin()?;
                    for (id, _) in block {
                        if tx.get(collection, &id)?.is_some() {
                            tx.delete(collection, &id, VersionPrecondition::Any)?;
                        }
                    }
                    tx.commit()?;
                } else if diff {
                    summary.documents += rows;
                    let mut tx = storage.begin()?;
                    for (id, document) in block {
                        if tx.get(collection, &id)?.is_some() {
                            tx.replace(collection, &id, document, VersionPrecondition::Any)?;
                        } else {
                            tx.insert(collection, id, document)?;
                        }
                    }
                    tx.commit()?;
                } else {
                    summary.documents += rows;
                    batch.extend(block.into_iter().map(|(id, document)| StorageMutation::insert(id, document)));
                    if batch.len() >= RESTORE_ROWS {
                        flush_batch(storage, current.as_ref(), &mut batch)?;
                    }
                }
            }
            Record::Live { names } => {
                flush_batch(storage, current.as_ref(), &mut batch)?;
                let live = names.into_iter().collect::<BTreeSet<_>>();
                let dropped = storage.read()?.collections()?.into_iter().filter(|collection| !is_virtual(collection.as_str()) && !live.contains(collection.as_str()));
                for collection in dropped.collect::<Vec<_>>() {
                    clear_collection(storage, &collection)?;
                }
            }
            // The state restored, history is for restore_history.
            Record::History { .. } => flush_batch(storage, current.take().as_ref(), &mut batch)?,
            Record::Events { rows, bytes, .. } => {
                summary.events += rows;
                std::io::copy(&mut reader.by_ref().take(bytes), &mut std::io::sink())?;
            }
            Record::Timeline { bytes, .. } => { std::io::copy(&mut reader.by_ref().take(bytes), &mut std::io::sink())?; }
            Record::End { collections, documents, removed, events } => {
                flush_batch(storage, current.as_ref(), &mut batch)?;
                if summary.collections != collections || summary.documents != documents || summary.removed != removed || summary.events != events {
                    return Err(BackupError::Invalid("footer counters do not match stream".into()));
                }
                return Ok(summary);
            }
        }
    }
}

/// Restores the backup at `path` with its history into a new, empty store: the full backup with history its chain
/// starts from gives back every recorded commit with its generation, time and author (the baseline first, then the
/// commits of every collection merged by generation), then the diffs of the chain apply as new commits. The state
/// then matches the backup's, checked collection by collection when the chain is the full backup alone.
pub fn restore_history(storage: &dyn StorageEngine, path: &Path) -> Result<BackupSummary, BackupError> {
    let chain = chain(path)?;
    let (_, _, _, Some(BackupChain { history: Some(floor), .. })) = header(&chain[0])? else {
        return Err(BackupError::Invalid("the backup holds no history: make it with history (OGD_HISTORY=on)".into()));
    };
    let mut writer = storage.history_writer()?;
    // Where the events of each collection, the timeline and the expected counts are.
    let mut file = open_reader(&chain[0])?;
    read_header(&mut file)?;
    let (mut cursors, mut timeline, mut counts) = (Vec::<Cursor>::new(), None, Vec::new());
    loop {
        let record = read_record(&mut file)?;
        let at = file.stream_position()?;
        if let Record::Block { bytes, .. } | Record::Removed { bytes, .. } | Record::Events { bytes, .. } | Record::Timeline { bytes, .. } = record {
            file.seek(SeekFrom::Start(at + bytes))?;
        }
        match record {
            Record::Collection { name, documents } => counts.push((CollectionId::parse(name)?, documents)),
            Record::History { name } => cursors.push(Cursor { collection: CollectionId::parse(name)?, blocks: std::collections::VecDeque::new(), events: std::collections::VecDeque::new() }),
            Record::Events { raw, bytes, .. } => cursors.last_mut().ok_or_else(|| BackupError::Invalid("events before history".into()))?.blocks.push_back((at, raw, bytes)),
            Record::Timeline { raw, bytes, .. } => timeline = Some((at, raw, bytes)),
            Record::End { .. } => break,
            _ => {}
        }
    }
    let timeline = match timeline { Some(block) => decode_timeline(&read_block(&mut file, block)?)?, None => HashMap::new() };
    let record = |generation: u64| timeline.get(&generation).cloned().unwrap_or(CommitRecord { generation, millis: 0, author: None });
    let mut summary = BackupSummary::default();
    for cursor in &mut cursors {
        let mut rows = Vec::new();
        while cursor.peek(&mut file)?.is_some_and(|event| event.op == HistoryOp::Baseline) {
            let event = cursor.events.pop_front().expect("peeked");
            let document = event.document.ok_or_else(|| BackupError::Invalid("baseline event without document".into()))?;
            rows.push(StoredDocument::new(event.id, event.version, document)?);
            if rows.len() == BLOCK_ROWS {
                summary.events += rows.len() as u64;
                writer.baseline(&cursor.collection, std::mem::take(&mut rows))?;
            }
        }
        if !rows.is_empty() {
            summary.events += rows.len() as u64;
            writer.baseline(&cursor.collection, rows)?;
        }
    }
    writer.start(record(floor))?;
    loop {
        let mut next = None::<u64>;
        for cursor in &mut cursors {
            if let Some(event) = cursor.peek(&mut file)? {
                next = Some(next.map_or(event.generation, |next| next.min(event.generation)));
            }
        }
        let Some(generation) = next else { break };
        let mut events = Vec::new();
        for cursor in &mut cursors {
            while cursor.peek(&mut file)?.is_some_and(|event| event.generation == generation) {
                events.push((cursor.collection.clone(), cursor.events.pop_front().expect("peeked")));
            }
        }
        summary.events += events.len() as u64;
        writer.commit(record(generation), events)?;
    }
    drop(writer);
    if chain.len() == 1 {
        let read = storage.read()?;
        for (collection, documents) in &counts {
            if read.count(collection)? != *documents {
                return Err(BackupError::Invalid(format!("history restored, but {collection} holds {} documents instead of {documents}", read.count(collection)?)));
            }
        }
    }
    for path in &chain[1..] {
        let applied = replay(storage, path, true)?;
        summary.documents += applied.documents;
        summary.removed += applied.removed;
    }
    summary.collections = storage.read()?.collections()?.len() as u64;
    renew_lineage(storage)?;
    Ok(summary)
}

/// The events of one collection, read block by block from the backup.
struct Cursor {
    collection: CollectionId,
    blocks: std::collections::VecDeque<(u64, u64, u64)>,
    events: std::collections::VecDeque<HistoryEvent>,
}

impl Cursor {
    fn peek(&mut self, file: &mut BufReader<File>) -> Result<Option<&HistoryEvent>, BackupError> {
        while self.events.is_empty() {
            let Some(block) = self.blocks.pop_front() else { return Ok(None) };
            self.events = decode_events(&read_block(file, block)?)?.into();
        }
        Ok(self.events.front())
    }
}

/// The decompressed payload at (`offset`, `raw`, `bytes`).
fn read_block(file: &mut BufReader<File>, (offset, raw, bytes): (u64, u64, u64)) -> Result<Vec<u8>, BackupError> {
    if raw > MAX_BLOCK_BYTES || bytes > MAX_BLOCK_BYTES {
        return Err(BackupError::Invalid("block too large".into()));
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut packed = vec![0u8; bytes as usize];
    file.read_exact(&mut packed)?;
    Ok(zstd::bulk::decompress(&packed, raw as usize)?)
}

/// Writes the events of every collection the history names, then the timeline; returns the events written.
fn write_history(out: &mut impl Write, source: &dyn HistorySource) -> Result<u64, BackupError> {
    let generation = source.generation();
    let (mut events, mut written) = (Vec::with_capacity(BLOCK_ROWS), 0u64);
    for collection in source.collections()?.into_iter().filter(|c| !is_virtual(c.as_str())) {
        write_record(out, &Record::History { name: collection.to_string() })?;
        source.events(&collection, 0, generation, None, true, &mut |event| {
            events.push(event);
            if events.len() == BLOCK_ROWS {
                write_events(out, &events).map_err(|error| StorageError::backend(error.to_string()))?;
                written += events.len() as u64;
                events.clear();
            }
            Ok(true)
        })?;
        if !events.is_empty() {
            write_events(out, &events)?;
            written += events.len() as u64;
            events.clear();
        }
    }
    let records = source.timeline()?.entries().iter().filter(|record| record.generation <= generation)
        .map(|record| (record.generation, record.millis, record.author.as_ref().map(|author| author.identity.to_string()), record.author.as_ref().and_then(|author| author.device.as_deref().map(str::to_owned))))
        .collect::<Vec<_>>();
    let raw = rmp_serde::to_vec(&records).map_err(BackupError::Encode)?;
    let packed = zstd::bulk::compress(&raw, LEVEL)?;
    write_record(out, &Record::Timeline { rows: records.len() as u64, raw: raw.len() as u64, bytes: packed.len() as u64 })?;
    out.write_all(&packed)?;
    Ok(written)
}

fn decode_timeline(bytes: &[u8]) -> Result<HashMap<u64, CommitRecord>, BackupError> {
    let records: Vec<(u64, i64, Option<String>, Option<String>)> = rmp_serde::from_slice(bytes).map_err(BackupError::Decode)?;
    Ok(records.into_iter().map(|(generation, millis, identity, device)| (generation, CommitRecord { generation, millis, author: identity.map(|identity| CommitAuthor { identity: identity.into(), device: device.map(Into::into) }) })).collect())
}

/// The documents of the events as a block, then per event its operation, its generation (zigzag delta) and its version.
fn write_events(out: &mut impl Write, events: &[HistoryEvent]) -> Result<(), BackupError> {
    let empty = Arc::new(Document::new());
    let rows = events.iter().map(|event| (event.id, event.document.clone().unwrap_or_else(|| Arc::clone(&empty)))).collect::<Vec<_>>();
    let block = encode_block(&rows);
    let mut raw = Vec::with_capacity(block.len() + events.len() * 4);
    encode_varint(&mut raw, block.len() as u64);
    raw.extend(block);
    let mut last = 0u64;
    for event in events {
        raw.push(match event.op { HistoryOp::Put => 0, HistoryOp::Delete => 1, HistoryOp::Baseline => 2, HistoryOp::Clear => 3 });
        let delta = event.generation.wrapping_sub(last) as i64;
        last = event.generation;
        encode_varint(&mut raw, ((delta << 1) ^ (delta >> 63)) as u64);
        encode_varint(&mut raw, event.version.get());
    }
    let packed = zstd::bulk::compress(&raw, LEVEL)?;
    write_record(out, &Record::Events { rows: events.len() as u64, raw: raw.len() as u64, bytes: packed.len() as u64 })?;
    Ok(out.write_all(&packed)?)
}

fn decode_events(bytes: &[u8]) -> Result<Vec<HistoryEvent>, BackupError> {
    let bad = |error: crate::query::ExecutionError| BackupError::Invalid(error.to_string());
    let mut input = Decoder::new(bytes);
    let len = input.size().map_err(bad)?;
    let rows = decode_block(input.take(len).map_err(bad)?)?;
    let mut last = 0u64;
    let events = rows.into_iter().map(|(id, document)| {
        let op = match input.byte().map_err(bad)? { 0 => HistoryOp::Put, 1 => HistoryOp::Delete, 2 => HistoryOp::Baseline, 3 => HistoryOp::Clear, op => return Err(BackupError::Invalid(format!("event operation {op}"))) };
        let zigzag = input.varint().map_err(bad)?;
        last = last.wrapping_add(((zigzag >> 1) as i64 ^ -((zigzag & 1) as i64)) as u64);
        let version = DocumentVersion::new(input.varint().map_err(bad)?);
        let document = matches!(op, HistoryOp::Put | HistoryOp::Baseline).then_some(document);
        Ok(HistoryEvent { generation: last, op, id, version, document })
    }).collect::<Result<Vec<_>, _>>()?;
    input.finish().map_err(bad)?;
    Ok(events)
}

/// The store's lineage, when a backup gave it one.
fn lineage(read: &dyn StorageRead) -> Result<Option<String>, BackupError> {
    let stored = read.get(&CollectionId::parse(LINEAGE)?, &lineage_id())?;
    Ok(stored.and_then(|stored| stored.document().get("lineage").and_then(Value::as_str).map(str::to_owned)))
}

/// Gives the store a new lineage: backups made before cannot be the parent of a diff any more.
fn renew_lineage(storage: &dyn StorageEngine) -> Result<(), BackupError> {
    let (collection, id) = (CollectionId::parse(LINEAGE)?, lineage_id());
    let mut document = Document::new();
    document.insert("lineage", Value::string(new_id()));
    let mut tx = storage.begin()?;
    if tx.get(&collection, &id)?.is_some() {
        tx.replace(&collection, &id, Arc::new(document), VersionPrecondition::Any)?;
    } else {
        tx.insert(&collection, id, Arc::new(document))?;
    }
    tx.commit()?;
    Ok(())
}

fn lineage_id() -> DocumentId { DocumentId::from_bytes(1u128.to_be_bytes()) }

fn new_id() -> String { UuidV7Generator::new().next_id().as_str() }

/// The header of the backup at `path`.
fn header(path: &Path) -> Result<(u16, u64, BackupSource, Option<BackupChain>), BackupError> { read_header(&mut open_reader(path)?) }

fn read_header(reader: &mut impl Read) -> Result<(u16, u64, BackupSource, Option<BackupChain>), BackupError> {
    match read_record(reader)? {
        Record::Header { version: version @ (1 | 2), created_at, source, chain } => Ok((version, created_at, source, chain)),
        Record::Header { version, .. } => Err(BackupError::Invalid(format!("unsupported version {version}"))),
        _ => Err(BackupError::Invalid("missing header".into())),
    }
}

fn write_block(out: &mut impl Write, rows: &[(DocumentId, Arc<Document>)], removal: bool) -> Result<(), BackupError> {
    let raw = encode_block(rows);
    let packed = zstd::bulk::compress(&raw, LEVEL)?;
    let (rows, raw, bytes) = (rows.len() as u64, raw.len() as u64, packed.len() as u64);
    write_record(out, &if removal { Record::Removed { rows, raw, bytes } } else { Record::Block { rows, raw, bytes } })?;
    Ok(out.write_all(&packed)?)
}

/// Rows, identifiers (the first then deltas when increasing, else raw), then per field its name, its tags
/// (one repeated tag or one per row) and its value stream.
fn encode_block(rows: &[(DocumentId, Arc<Document>)]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_varint(&mut out, rows.len() as u64);
    let ids = rows.iter().map(|(id, _)| u128::from_be_bytes(id.into_bytes())).collect::<Vec<_>>();
    match ids.windows(2).map(|pair| pair[1].checked_sub(pair[0]).and_then(|delta| u64::try_from(delta).ok())).collect::<Option<Vec<_>>>() {
        Some(deltas) => { out.push(0); out.extend(ids[0].to_be_bytes()); deltas.into_iter().for_each(|delta| encode_varint(&mut out, delta)); }
        None => { out.push(1); ids.iter().for_each(|id| out.extend(id.to_be_bytes())); }
    }
    let names = rows.iter().flat_map(|(_, document)| document.iter().map(|(name, _)| name.as_str())).collect::<BTreeSet<_>>();
    encode_varint(&mut out, names.len() as u64);
    let (mut tags, mut values) = (Vec::with_capacity(rows.len()), Vec::new());
    for name in names {
        tags.clear();
        values.clear();
        let (mut last, mut strings) = (0u64, HashMap::<&str, u64>::new());
        let mut delta = |values: &mut Vec<u8>, value: u64| { let delta = value.wrapping_sub(last) as i64; last = value; encode_varint(values, ((delta << 1) ^ (delta >> 63)) as u64); };
        for (_, document) in rows {
            tags.push(match document.get(name) {
                None => MISSING,
                Some(Value::Null) => NULL,
                Some(Value::Bool(flag)) => FALSE + u8::from(*flag),
                Some(Value::Number(Number::Signed(value))) => { delta(&mut values, *value as u64); SIGNED }
                Some(Value::Number(Number::Unsigned(value))) => { delta(&mut values, *value); UNSIGNED }
                Some(Value::Number(Number::Float(value))) => { values.extend(value.to_bits().to_le_bytes()); FLOAT }
                Some(Value::String(text)) => {
                    let next = strings.len() as u64;
                    let index = *strings.entry(&**text).or_insert(next);
                    encode_varint(&mut values, index);
                    if index == next { encode_varint(&mut values, text.len() as u64); values.extend(text.as_bytes()); }
                    STRING
                }
                Some(value) => { encode_value(&mut values, value); NESTED }
            });
        }
        encode_varint(&mut out, name.len() as u64);
        out.extend(name.as_bytes());
        if tags.iter().all(|tag| *tag == tags[0]) { out.extend([0, tags[0]]) } else { out.push(1); out.extend(&tags) }
        encode_varint(&mut out, values.len() as u64);
        out.extend(&values);
    }
    out
}

fn decode_block(bytes: &[u8]) -> Result<Vec<(DocumentId, Arc<Document>)>, BackupError> {
    let bad = |error: crate::query::ExecutionError| BackupError::Invalid(error.to_string());
    let text = |bytes: &[u8]| std::str::from_utf8(bytes).map(Arc::<str>::from).map_err(|error| BackupError::Invalid(error.to_string()));
    let mut input = Decoder::new(bytes);
    let rows = input.size().map_err(bad)?;
    let ids = match (input.byte().map_err(bad)?, rows) {
        (_, 0) => Vec::new(),
        (0, _) => {
            let mut id = u128::from_be_bytes(input.array().map_err(bad)?);
            let mut ids = vec![id];
            for _ in 1..rows { id = id.checked_add(u128::from(input.varint().map_err(bad)?)).ok_or_else(|| BackupError::Invalid("identifier overflow".into()))?; ids.push(id); }
            ids
        }
        (1, _) => (0..rows).map(|_| input.array().map(u128::from_be_bytes)).collect::<Result<_, _>>().map_err(bad)?,
        (mode, _) => return Err(BackupError::Invalid(format!("identifier mode {mode}"))),
    };
    let mut documents = vec![Document::new(); rows];
    for _ in 0..input.size().map_err(bad)? {
        let len = input.size().map_err(bad)?;
        let name = FieldName::new(text(input.take(len).map_err(bad)?)?);
        let tags = match input.byte().map_err(bad)? { 0 => vec![input.byte().map_err(bad)?; rows], _ => input.take(rows).map_err(bad)?.to_vec() };
        let len = input.size().map_err(bad)?;
        let mut values = Decoder::new(input.take(len).map_err(bad)?);
        let (mut last, mut strings) = (0u64, Vec::<Arc<str>>::new());
        for (document, tag) in documents.iter_mut().zip(tags) {
            let mut delta = |values: &mut Decoder<'_>| -> Result<u64, BackupError> { let zigzag = values.varint().map_err(bad)?; last = last.wrapping_add(((zigzag >> 1) as i64 ^ -((zigzag & 1) as i64)) as u64); Ok(last) };
            let value = match tag {
                MISSING => continue,
                NULL => Value::Null,
                FALSE | TRUE => Value::bool(tag == TRUE),
                SIGNED => Value::signed(delta(&mut values)? as i64),
                UNSIGNED => Value::unsigned(delta(&mut values)?),
                FLOAT => Value::float(f64::from_bits(u64::from_le_bytes(values.array().map_err(bad)?))).map_err(|error| BackupError::Invalid(error.to_string()))?,
                STRING => {
                    let index = values.size().map_err(bad)?;
                    if index == strings.len() { let len = values.size().map_err(bad)?; strings.push(text(values.take(len).map_err(bad)?)?); }
                    Value::String(strings.get(index).cloned().ok_or_else(|| BackupError::Invalid("string index out of range".into()))?)
                }
                NESTED => values.value().map_err(bad)?,
                tag => return Err(BackupError::Invalid(format!("value tag {tag}"))),
            };
            document.insert(name.clone(), value);
        }
        values.finish().map_err(bad)?;
    }
    input.finish().map_err(bad)?;
    Ok(ids.into_iter().zip(documents).map(|(id, document)| (DocumentId::from_bytes(id.to_be_bytes()), Arc::new(document))).collect())
}

fn clear_persistent(storage: &dyn StorageEngine) -> Result<(), BackupError> {
    let collections = storage.read()?.collections()?.into_iter().filter(|c| !is_virtual(c.as_str())).collect::<Vec<_>>();
    for collection in collections {
        clear_collection(storage, &collection)?;
    }
    Ok(())
}
/// Removes every document of `collection`.
fn clear_collection(storage: &dyn StorageEngine, collection: &CollectionId) -> Result<(), BackupError> {
    loop {
        let docs = storage.read()?.scan(collection, ScanOptions::default().with_limit(RESTORE_ROWS))?;
        if docs.is_empty() {
            return Ok(());
        }
        let mut tx = storage.begin()?;
        for d in docs {
            tx.delete(collection, d.id(), VersionPrecondition::Any)?;
        }
        tx.commit()?;
    }
}
fn flush_batch( storage: &dyn StorageEngine, collection: Option<&CollectionId>, batch: &mut Vec<StorageMutation>, ) -> Result<(), BackupError> {
    if batch.is_empty() {
        return Ok(());
    }
    let c = collection.ok_or_else(|| BackupError::Invalid("missing collection".into()))?;
    storage.apply_batch_atomic_summary(c, std::mem::take(batch))?;
    Ok(())
}
fn is_virtual(name: &str) -> bool {
    vcollections::contains(name)
}
fn open_reader(path: &Path) -> Result<BufReader<File>, BackupError> {
    let mut r = BufReader::new(File::open(path)?);
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC && &magic != MAGIC_V1 {
        return Err(BackupError::Invalid("bad magic".into()));
    }
    Ok(r)
}
fn write_record(w: &mut impl Write, record: &Record) -> Result<(), BackupError> {
    let bytes = rmp_serde::to_vec_named(record).map_err(BackupError::Encode)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(BackupError::Invalid("record too large".into()));
    }
    w.write_all(&(bytes.len() as u32).to_be_bytes())?;
    w.write_all(&bytes)?;
    Ok(())
}
fn read_record(r: &mut impl Read) -> Result<Record, BackupError> {
    let mut h = [0u8; 4];
    r.read_exact(&mut h)?;
    let len = u32::from_be_bytes(h) as usize;
    if len == 0 || len > MAX_RECORD_BYTES {
        return Err(BackupError::Invalid(format!("invalid record length {len}")));
    }
    let mut b = vec![0u8; len];
    r.read_exact(&mut b)?;
    rmp_serde::from_slice(&b).map_err(BackupError::Decode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;
    #[test] fn round_trip() { let storage = MemoryStorage::new(); let c = CollectionId::parse("users").unwrap(); let mut tx = storage.begin().unwrap(); let mut d = Document::new(); d.insert("name", Value::string("Alice")); tx.insert(&c, DocumentId::from_test_label("a"), Arc::new(d)) .unwrap(); tx.commit().unwrap(); let p = std::env::temp_dir().join(format!("og-backup-{}.ogb", std::process::id())); let s = create( &storage, &p, BackupMetadata { created_at: 42, source: BackupSource { instance_id: "instance-test".into(), hostname: "test-host".into(), platform: "test-os".into(), arch: "test-arch".into(), core_version: "0.1.0".into(), }, }, ) .unwrap(); assert_eq!(s.documents, 1); let info = inspect(&p).unwrap(); assert_eq!(info.created_at, 42); assert_eq!(info.size_bytes, std::fs::metadata(&p).unwrap().len()); assert_eq!(info.source.instance_id, "instance-test"); let restored = MemoryStorage::new(); restore(&restored, &p, false).unwrap(); assert_eq!(restored.read().unwrap().count(&c).unwrap(), 1); let _ = std::fs::remove_file(p); }
    #[test] fn blocks_keep_types_missing_fields_and_unordered_ids() { let rows = (0..300u64).map(|row| { let mut d = Document::new(); d.insert("n", if row % 7 == 0 { Value::unsigned(u64::MAX - row) } else { Value::signed(-(row as i64) * 1000) }); if row % 3 != 0 { d.insert("s", Value::string(["a", "bé", ""][row as usize % 3])); } d.insert("f", if row % 5 == 0 { Value::Null } else { Value::float(row as f64 / 3.0).unwrap() }); d.insert("b", Value::bool(row % 2 == 0)); if row == 9 { let mut inner = Document::new(); inner.insert("k", Value::array([Value::signed(1), Value::string("x")])); d.insert("o", Value::object(inner)); } (DocumentId::from_bytes(((row * 7919) % 300 + 1).to_be_bytes().iter().chain(&[0u8; 8]).copied().collect::<Vec<_>>().try_into().unwrap()), Arc::new(d)) }).collect::<Vec<_>>(); for rows in [&rows[..], &rows[..1]] { assert_eq!(decode_block(&encode_block(rows)).unwrap(), rows.to_vec()); } }
    fn metadata() -> BackupMetadata { BackupMetadata { created_at: 1, source: BackupSource { instance_id: "i".into(), hostname: "h".into(), platform: "p".into(), arch: "a".into(), core_version: "0".into() } } }
    fn dump(storage: &dyn StorageEngine) -> Vec<(String, Vec<(DocumentId, Arc<Document>)>)> { let read = storage.read().unwrap(); read.collections().unwrap().into_iter().filter(|c| c.as_str() != LINEAGE).map(|c| (c.to_string(), read.scan(&c, ScanOptions::default()).unwrap().into_iter().map(|d| (*d.id(), d.shared_document())).collect::<Vec<_>>())).filter(|(_, rows)| !rows.is_empty()).collect() }
    fn write(storage: &dyn StorageEngine, collection: &str, label: &str, value: Option<i64>) { let (c, id) = (CollectionId::parse(collection).unwrap(), DocumentId::from_test_label(label)); let mut tx = storage.begin().unwrap(); let exists = tx.get(&c, &id).unwrap().is_some(); match value { Some(value) => { let mut d = Document::new(); d.insert("v", Value::signed(value)); if exists { tx.replace(&c, &id, Arc::new(d), VersionPrecondition::Any).unwrap(); } else { tx.insert(&c, id, Arc::new(d)).unwrap(); } } None => { tx.delete(&c, &id, VersionPrecondition::Any).unwrap(); } } tx.commit().unwrap(); }
    #[test] fn diff_chains_restore_the_state_of_their_last_backup() { let dir = std::env::temp_dir().join(format!("og-backup-chain-{}", std::process::id())); let _ = std::fs::remove_dir_all(&dir); std::fs::create_dir_all(&dir).unwrap(); let at = |name: &str| dir.join(format!("{name}.ogb")); let storage = MemoryStorage::with_history(); for (label, value) in [("a", 1), ("b", 2), ("c", 3)] { write(&storage, "t", label, Some(value)); } create(&storage, &at("full"), metadata()).unwrap(); write(&storage, "t", "a", Some(10)); write(&storage, "t", "b", None); write(&storage, "u", "x", Some(5)); let one = create_diff(&storage, &at("d1"), metadata(), &at("full")).unwrap(); assert_eq!((one.collections, one.documents, one.removed), (2, 2, 1)); let after_one = dump(&storage); write(&storage, "t", "d", Some(4)); write(&storage, "u", "x", None); let two = create_diff(&storage, &at("d2"), metadata(), &at("d1")).unwrap(); assert_eq!(two.documents, 1); let after_two = dump(&storage); assert_eq!(inspect(&at("d2")).unwrap().chain.unwrap().parent.unwrap().id, inspect(&at("d1")).unwrap().chain.unwrap().id); for (name, expected) in [("full", None), ("d1", Some(&after_one)), ("d2", Some(&after_two))] { let target = MemoryStorage::new(); restore(&target, &at(name), false).unwrap(); if let Some(expected) = expected { assert_eq!(&dump(&target), expected); } else { assert_eq!(dump(&target)[0].1.len(), 3); } } let target = MemoryStorage::with_history(); restore(&target, &at("d2"), false).unwrap(); create_diff(&target, &at("other"), metadata(), &at("d2")).unwrap_err(); create(&target, &at("again"), metadata()).unwrap(); write(&target, "t", "e", Some(7)); create_diff(&target, &at("next"), metadata(), &at("again")).unwrap(); storage.clear().unwrap(); create_diff(&storage, &at("cleared"), metadata(), &at("d2")).unwrap_err(); create_diff(&MemoryStorage::new(), &at("none"), metadata(), &at("full")).unwrap_err(); std::fs::remove_file(at("d1")).unwrap(); assert!(restore(&MemoryStorage::new(), &at("d2"), false).unwrap_err().to_string().contains("not found")); let _ = std::fs::remove_dir_all(&dir); }

}
