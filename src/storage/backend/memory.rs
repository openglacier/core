#![cfg_attr(rustfmt, rustfmt_skip)]
//! Physical in-memory storage backend.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::Document;

use super::super::{
    common::{ensure_version, increment_counter, next_generation},
    history::{temporal_read, CommitRecord, HistoryEvent, HistoryOp, HistorySource, Timeline},
    CollectionId, CommitResult, DeleteResult, DocumentId, DocumentVersion, InsertResult,
    ReplaceResult, ScanDirection, ScanOptions, StorageError, StorageMutation, StorageRead,
    StorageResult, StorageTransaction, StoredDocument, Temporal, TemporalCache, VersionPrecondition,
};
use crate::storage::StorageEngine;

/// Thread-safe in-memory storage engine.
///
/// Cloning this type creates another handle to the same underlying database.
#[derive(Clone, Debug, Default)]
pub struct MemoryBackend {
    inner: Arc<RwLock<MemoryState>>,
    /// Every version and deletion since creation, when history is kept.
    history: Option<Arc<RwLock<MemoryHistory>>>,
}

/// Events of every collection and the commit records, appended under the
/// state write lock so they follow the generation order.
#[derive(Debug, Default)]
struct MemoryHistory {
    events: BTreeMap<CollectionId, Vec<HistoryEvent>>,
    timeline: Vec<CommitRecord>,
}

impl MemoryHistory {
    fn put(&mut self, collection: &CollectionId, generation: u64, stored: &StoredDocument) {
        self.events.entry(collection.clone()).or_default().push(HistoryEvent { generation, op: HistoryOp::Put, id: *stored.id(), version: stored.version(), document: Some(stored.shared_document()) });
    }

    fn delete(&mut self, collection: &CollectionId, generation: u64, id: DocumentId) {
        self.events.entry(collection.clone()).or_default().push(HistoryEvent { generation, op: HistoryOp::Delete, id, version: DocumentVersion::NONE, document: None });
    }
}

impl MemoryBackend {
    /// Applies `mutations` as one commit, returning the stored documents when `keep` is set.
    fn apply_atomic(&self, collection: &CollectionId, mutations: Vec<StorageMutation>, keep: bool) -> StorageResult<(Vec<StoredDocument>, CommitResult)> {
        if mutations.is_empty() {
            return Ok((Vec::new(), CommitResult::default()));
        }
        let mut state = self.write_state()?;
        let target = state.collections.get(collection);
        let mut staged = BTreeMap::<DocumentId, StoredDocument>::new();
        let mut stored = Vec::with_capacity(if keep { mutations.len() } else { 0 });
        let (mut inserted, mut replaced) = (0u64, 0u64);
        for mutation in mutations {
            let document = match mutation {
                StorageMutation::Insert { id, document } => {
                    if staged.contains_key(&id) || target.is_some_and(|target| target.contains_key(&id)) {
                        return Err(StorageError::document_already_exists(collection.clone(), id));
                    }
                    inserted = increment_counter(inserted)?;
                    StoredDocument::new(id, DocumentVersion::INITIAL, document)?
                }
                StorageMutation::Replace { id, document, precondition } => {
                    let current = staged.get(&id).or_else(|| target.and_then(|target| target.get(&id))).ok_or_else(|| StorageError::document_not_found(collection.clone(), id))?;
                    ensure_version(collection, &id, current.version(), precondition)?;
                    replaced = increment_counter(replaced)?;
                    StoredDocument::new(id, current.version().next()?, document)?
                }
            };
            if keep { stored.push(document.clone()); }
            staged.insert(*document.id(), document);
        }
        state.generation = next_generation(state.generation)?;
        let generation = state.generation;
        state.changed.insert(collection.clone(), generation);
        self.record_history(|history| {
            for document in staged.values() {
                history.put(collection, generation, document);
            }
            history.timeline.push(CommitRecord::now(generation));
        })?;
        let target = Arc::make_mut(state.collections.entry(collection.clone()).or_default());
        target.extend(staged);
        Ok((stored, CommitResult::new(inserted, replaced, 0)))
    }

    /// Creates an empty in-memory storage engine.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty in-memory storage engine that keeps every version
    /// (for `as of` and `history` reads). Old versions stay in memory.
    #[must_use]
    pub fn with_history() -> Self {
        let history = MemoryHistory { timeline: vec![CommitRecord::now(0)], ..MemoryHistory::default() };
        Self { inner: Arc::default(), history: Some(Arc::new(RwLock::new(history))) }
    }

    /// Whether this engine keeps history.
    #[must_use]
    pub const fn keeps_history(&self) -> bool { self.history.is_some() }

    /// Runs `record` on the history, when kept.
    fn record_history(&self, record: impl FnOnce(&mut MemoryHistory)) -> StorageResult<()> {
        if let Some(history) = &self.history {
            let mut history = history.write().map_err(|_| StorageError::backend("in-memory history lock poisoned"))?;
            record(&mut history);
        }
        Ok(())
    }

    /// Returns the current committed generation.
    ///
    /// The generation starts at zero and increases after every non-empty
    /// successful commit.
    pub fn generation(&self) -> StorageResult<u64> {
        Ok(self.read_state()?.generation)
    }

    /// Returns the number of committed collections.
    pub fn collection_count(&self) -> StorageResult<usize> {
        Ok(self.read_state()?.collections.len())
    }

    /// Returns the number of committed documents across all collections.
    pub fn document_count(&self) -> StorageResult<usize> {
        let state = self.read_state()?;

        state
            .collections
            .values()
            .try_fold(0usize, |total, collection| {
                total
                    .checked_add(collection.len())
                    .ok_or_else(|| StorageError::backend("in-memory document count overflow"))
            })
    }

    /// Removes all committed data atomically.
    ///
    /// This administrative operation is primarily useful in tests. Clearing an
    /// already empty engine is a no-op and does not advance the generation.
    pub fn clear(&self) -> StorageResult<()> {
        let mut state = self.write_state()?;

        if state.collections.is_empty() {
            return Ok(());
        }

        let cleared = state.collections.keys().cloned().collect::<Vec<_>>();
        state.collections.clear();
        state.generation = next_generation(state.generation)?;
        let generation = state.generation;
        state.changed.extend(cleared.iter().map(|collection| (collection.clone(), generation)));
        self.record_history(|history| {
            for collection in &cleared {
                history.events.entry(collection.clone()).or_default().push(HistoryEvent { generation, op: HistoryOp::Clear, id: DocumentId::from_bytes([0; 16]), version: DocumentVersion::NONE, document: None });
            }
            history.timeline.push(CommitRecord::now(generation));
        })?;

        Ok(())
    }

    fn read_state(&self) -> StorageResult<RwLockReadGuard<'_, MemoryState>> {
        self.inner
            .read()
            .map_err(|_| StorageError::backend("in-memory storage read lock poisoned"))
    }

    fn write_state(&self) -> StorageResult<RwLockWriteGuard<'_, MemoryState>> {
        self.inner
            .write()
            .map_err(|_| StorageError::backend("in-memory storage write lock poisoned"))
    }
}

impl StorageEngine for MemoryBackend {
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>> {
        let snapshot = {
            let state = self.read_state()?;
            MemorySnapshot::from_state(&state)
        };

        Ok(Box::new(snapshot))
    }

    fn read_temporal(&self, temporal: &Temporal, cache: &Arc<TemporalCache>) -> StorageResult<Box<dyn StorageRead + '_>> {
        let Some(history) = &self.history else {
            return Err(StorageError::history_unavailable());
        };
        let snapshot = {
            let state = self.read_state()?;
            MemorySnapshot::from_state(&state)
        };
        let source = MemoryHistorySource { history: Arc::clone(history), generation: snapshot.generation };
        temporal_read(Box::new(snapshot), Box::new(source), temporal, cache)
    }

    fn history(&self) -> StorageResult<(Box<dyn StorageRead + '_>, Box<dyn HistorySource + '_>)> {
        let history = self.history.as_ref().ok_or_else(StorageError::history_unavailable)?;
        let snapshot = MemorySnapshot::from_state(&*self.read_state()?);
        let source = MemoryHistorySource { history: Arc::clone(history), generation: snapshot.generation };
        Ok((Box::new(snapshot), Box::new(source)))
    }

    fn begin(&self) -> StorageResult<Box<dyn StorageTransaction + '_>> {
        let transaction = {
            let state = self.read_state()?;
            MemoryTransaction::from_state(self, &state)
        };

        Ok(Box::new(transaction))
    }

    fn apply_batch_atomic( &self, collection: &CollectionId, mutations: Vec<StorageMutation>, ) -> StorageResult<(Vec<StoredDocument>, CommitResult)> { self.apply_atomic(collection, mutations, true) }

    fn apply_batch_atomic_summary( &self, collection: &CollectionId, mutations: Vec<StorageMutation>, ) -> StorageResult<CommitResult> { self.apply_atomic(collection, mutations, false).map(|(_, result)| result) }
}

#[derive(Clone, Debug, Default)]
pub(super) struct MemoryState {
    pub(super) generation: u64,
    pub(super) collections: Collections,
    /// The generation of the last commit that changed each collection.
    pub(super) changed: BTreeMap<CollectionId, u64>,
}

/// Each collection is shared by the snapshots and transactions that read it,
/// and copied only when one writes it.
pub(super) type Collections = BTreeMap<CollectionId, Arc<Collection>>;
pub(crate) type Collection = BTreeMap<DocumentId, StoredDocument>;

/// Immutable in-memory read snapshot.
#[derive(Clone, Debug)]
struct MemorySnapshot {
    generation: u64,
    collections: Collections,
    changed: BTreeMap<CollectionId, u64>,
}

impl MemorySnapshot {
    fn from_state(state: &MemoryState) -> Self {
        Self {
            generation: state.generation,
            collections: state.collections.clone(),
            changed: state.changed.clone(),
        }
    }

}

/// History of a [`MemoryBackend`] at one snapshot generation.
struct MemoryHistorySource {
    history: Arc<RwLock<MemoryHistory>>,
    generation: u64,
}

impl HistorySource for MemoryHistorySource {
    fn generation(&self) -> u64 { self.generation }

    fn floor(&self) -> u64 { 0 }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        Ok(self.history.read().map_err(|_| StorageError::backend("in-memory history lock poisoned"))?.events.keys().cloned().collect())
    }

    fn events(&self, collection: &CollectionId, after: u64, upto: u64, ids: Option<&std::collections::HashSet<DocumentId>>, _documents: bool, visitor: &mut dyn FnMut(HistoryEvent) -> StorageResult<bool>) -> StorageResult<()> {
        let upto = upto.min(self.generation);
        let history = self.history.read().map_err(|_| StorageError::backend("in-memory history lock poisoned"))?;
        let Some(events) = history.events.get(collection) else { return Ok(()) };
        let start = events.partition_point(|event| event.generation <= after);
        for event in &events[start..] {
            if event.generation > upto {
                break;
            }
            if ids.is_some_and(|ids| event.op != HistoryOp::Clear && !ids.contains(&event.id)) {
                continue;
            }
            if !visitor(event.clone())? {
                break;
            }
        }
        Ok(())
    }

    fn timeline(&self) -> StorageResult<Timeline> {
        let history = self.history.read().map_err(|_| StorageError::backend("in-memory history lock poisoned"))?;
        Ok(Timeline::new(history.timeline.iter().filter(|record| record.generation <= self.generation).cloned().collect()))
    }
}

/// The reads of the collections a snapshot or a transaction holds (`self.collections`).
macro_rules! read_collections { () => {
    fn get(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> { Ok(get_document(&self.collections, collection, id)) }
    fn scan(&self, collection: &CollectionId, options: ScanOptions) -> StorageResult<Vec<StoredDocument>> { Ok(scan_collection(&self.collections, collection, options)) }
    fn count(&self, collection: &CollectionId) -> StorageResult<u64> { Ok(self.collections.get(collection).map_or(0, |documents| documents.len() as u64)) }
    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> { Ok(self.collections.contains_key(collection)) }
    fn collections(&self) -> StorageResult<Vec<CollectionId>> { Ok(list_collections(&self.collections)) }
} }

impl StorageRead for MemorySnapshot {
    fn changed(&self, collection: &CollectionId) -> StorageResult<Option<u64>> {
        Ok(Some(self.changed.get(collection).copied().unwrap_or(0)))
    }

    read_collections!();

    fn scan_each( &self, collection: &CollectionId, options: ScanOptions, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>, ) -> StorageResult<()> {
        let Some(documents) = self.collections.get(collection) else {
            return Ok(());
        };
        let rows: Box<dyn Iterator<Item = &StoredDocument>> = match options.direction() { ScanDirection::Forward => Box::new(documents.values()), ScanDirection::Reverse => Box::new(documents.values().rev()) };
        for stored in rows.take(options.limit().unwrap_or(usize::MAX)) {
            if !visitor(stored.clone())? {
                break;
            }
        }
        Ok(())
    }
}

/// Isolated mutable snapshot used by [`MemoryBackend`] transactions.
struct MemoryTransaction<'storage> {
    storage: &'storage MemoryBackend,
    base_generation: u64,
    collections: Collections,
    inserted: u64,
    replaced: u64,
    deleted: u64,
    /// Puts (`Some`) and deletes (`None`) in order, when history is kept.
    changes: Option<Vec<(CollectionId, DocumentId, Option<StoredDocument>)>>,
    /// The collections it writes.
    touched: std::collections::BTreeSet<CollectionId>,
}

impl<'storage> MemoryTransaction<'storage> {
    fn from_state(storage: &'storage MemoryBackend, state: &MemoryState) -> Self {
        Self {
            storage,
            base_generation: state.generation,
            collections: state.collections.clone(),
            inserted: 0,
            replaced: 0,
            deleted: 0,
            changes: storage.history.is_some().then(Vec::new),
            touched: std::collections::BTreeSet::new(),
        }
    }

    fn record(&mut self, collection: &CollectionId, id: DocumentId, stored: Option<&StoredDocument>) {
        if !self.touched.contains(collection) {
            self.touched.insert(collection.clone());
        }
        if let Some(changes) = &mut self.changes {
            changes.push((collection.clone(), id, stored.cloned()));
        }
    }

    fn insert_batch( &mut self, collection: &CollectionId, mutations: Vec<StorageMutation>, ) -> StorageResult<Vec<StoredDocument>> {
        self.touched.insert(collection.clone());
        let target = self.collections.entry(collection.clone()).or_default();
        let target = Arc::make_mut(target);
        let mut pending = BTreeMap::new();
        let mut stored = Vec::with_capacity(mutations.len());

        for mutation in mutations {
            let StorageMutation::Insert { id, document } = mutation else {
                unreachable!("insert_batch receives insert-only mutations");
            };
            if target.contains_key(&id) || pending.contains_key(&id) {
                return Err(StorageError::document_already_exists(
                    collection.clone(),
                    id,
                ));
            }
            let document = StoredDocument::new(id, DocumentVersion::INITIAL, document)?;
            pending.insert(id, document.clone());
            stored.push(document);
        }

        self.inserted = self
            .inserted
            .checked_add(stored.len() as u64)
            .ok_or_else(|| StorageError::backend("in-memory mutation counter overflow"))?;
        target.append(&mut pending);
        if self.changes.is_some() {
            for document in &stored {
                self.record(collection, *document.id(), Some(document));
            }
        }
        Ok(stored)
    }
}

impl StorageRead for MemoryTransaction<'_> {
    read_collections!();
}

impl StorageTransaction for MemoryTransaction<'_> {
    fn insert( &mut self, collection: &CollectionId, id: DocumentId, document: Arc<Document>, ) -> StorageResult<InsertResult> {
        let target = self.collections.entry(collection.clone()).or_default();
        let target = Arc::make_mut(target);

        match target.entry(id) {
            Entry::Occupied(_) => Err(StorageError::document_already_exists(
                collection.clone(),
                id,
            )),
            Entry::Vacant(entry) => {
                let stored = StoredDocument::new(id, DocumentVersion::INITIAL, document)?;

                entry.insert(stored.clone());
                self.inserted = increment_counter(self.inserted)?;
                self.record(collection, id, Some(&stored));

                Ok(InsertResult::new(stored))
            }
        }
    }

    fn replace( &mut self, collection: &CollectionId, id: &DocumentId, document: Arc<Document>, precondition: VersionPrecondition, ) -> StorageResult<ReplaceResult> {
        let target = self
            .collections
            .get_mut(collection)
            .and_then(|documents| Arc::make_mut(documents).get_mut(id))
            .ok_or_else(|| StorageError::document_not_found(collection.clone(), *id))?;

        let previous_version = target.version();
        ensure_version(collection, id, previous_version, precondition)?;

        let next_version = previous_version.next()?;
        let stored = StoredDocument::new(*id, next_version, document)?;

        *target = stored.clone();
        self.replaced = increment_counter(self.replaced)?;
        self.record(collection, *id, Some(&stored));

        Ok(ReplaceResult::new(previous_version, stored))
    }

    fn delete( &mut self, collection: &CollectionId, id: &DocumentId, precondition: VersionPrecondition, ) -> StorageResult<DeleteResult> {
        let current_version = self
            .collections
            .get(collection)
            .and_then(|documents| documents.get(id))
            .map(StoredDocument::version)
            .ok_or_else(|| StorageError::document_not_found(collection.clone(), *id))?;

        ensure_version(collection, id, current_version, precondition)?;

        let remove_collection = {
            let documents = self
                .collections
                .get_mut(collection)
                .ok_or_else(|| StorageError::document_not_found(collection.clone(), *id))?;

            let documents = Arc::make_mut(documents);
            let removed = documents
                .remove(id)
                .ok_or_else(|| StorageError::document_not_found(collection.clone(), *id))?;

            debug_assert_eq!(removed.version(), current_version);
            documents.is_empty()
        };

        if remove_collection {
            self.collections.remove(collection);
        }

        self.deleted = increment_counter(self.deleted)?;
        self.record(collection, *id, None);

        Ok(DeleteResult::new(*id, current_version))
    }

    fn apply_batch( &mut self, collection: &CollectionId, mutations: Vec<StorageMutation>, ) -> StorageResult<Vec<StoredDocument>> {
        self.touched.insert(collection.clone());
        if mutations
            .iter()
            .all(|mutation| matches!(mutation, StorageMutation::Insert { .. }))
        {
            return self.insert_batch(collection, mutations);
        }

        let mut stored = Vec::with_capacity(mutations.len());
        let target = self.collections.entry(collection.clone()).or_default();
        let target = Arc::make_mut(target);
        let record = self.changes.is_some();

        for mutation in mutations {
            match mutation {
                StorageMutation::Insert { id, document } => match target.entry(id) {
                    Entry::Occupied(_) => {
                        return Err(StorageError::document_already_exists(
                            collection.clone(),
                            id,
                        ));
                    }
                    Entry::Vacant(entry) => {
                        let document = StoredDocument::new(id, DocumentVersion::INITIAL, document)?;
                        entry.insert(document.clone());
                        self.inserted = increment_counter(self.inserted)?;
                        stored.push(document);
                    }
                },
                StorageMutation::Replace {
                    id,
                    document,
                    precondition,
                } => {
                    let current = target.get_mut(&id).ok_or_else(|| {
                        StorageError::document_not_found(collection.clone(), id)
                    })?;
                    let previous_version = current.version();
                    ensure_version(collection, &id, previous_version, precondition)?;
                    let document = StoredDocument::new(id, previous_version.next()?, document)?;
                    *current = document.clone();
                    self.replaced = increment_counter(self.replaced)?;
                    stored.push(document);
                }
            }
        }

        if record {
            for document in &stored {
                self.record(collection, *document.id(), Some(document));
            }
        }
        Ok(stored)
    }

    fn commit(self: Box<Self>) -> StorageResult<CommitResult> {
        let Self {
            storage,
            base_generation,
            collections,
            inserted,
            replaced,
            deleted,
            changes,
            touched,
        } = *self;

        let mutation_count = inserted
            .checked_add(replaced)
            .and_then(|count| count.checked_add(deleted))
            .ok_or_else(|| StorageError::backend("in-memory mutation count overflow"))?;
        let result = CommitResult::new(inserted, replaced, deleted);

        if mutation_count == 0 {
            return Ok(result);
        }

        let mut state = storage.write_state()?;

        if state.generation != base_generation {
            return Err(StorageError::transaction_conflict(format!(
                "snapshot generation {base_generation} is stale; current generation is {}",
                state.generation
            )));
        }

        let next_generation = next_generation(state.generation)?;
        state.collections = collections;
        state.generation = next_generation;
        state.changed.extend(touched.into_iter().map(|collection| (collection, next_generation)));
        if let Some(changes) = changes {
            storage.record_history(|history| {
                for (collection, id, stored) in &changes {
                    match stored {
                        Some(stored) => history.put(collection, next_generation, stored),
                        None => history.delete(collection, next_generation, *id),
                    }
                }
                history.timeline.push(CommitRecord::now(next_generation));
            })?;
        }

        Ok(result)
    }

    fn rollback(self: Box<Self>) -> StorageResult<()> {
        Ok(())
    }
}

fn get_document( collections: &Collections, collection: &CollectionId, id: &DocumentId, ) -> Option<StoredDocument> {
    collections
        .get(collection)
        .and_then(|documents| documents.get(id))
        .cloned()
}

fn scan_collection( collections: &Collections, collection: &CollectionId, options: ScanOptions, ) -> Vec<StoredDocument> {
    let Some(documents) = collections.get(collection) else {
        return Vec::new();
    };

    let limit = options.limit().unwrap_or(usize::MAX);
    match options.direction() {
        ScanDirection::Forward => documents.values().take(limit).cloned().collect(),
        ScanDirection::Reverse => documents.values().rev().take(limit).cloned().collect(),
    }
}

fn list_collections(collections: &Collections) -> Vec<CollectionId> {
    collections.keys().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::super::super::StorageErrorKind;
    use super::*;

    fn collection(value: &str) -> CollectionId {
        CollectionId::parse(value).unwrap()
    }

    fn document_id(value: &str) -> DocumentId {
        DocumentId::from_test_label(value)
    }

    #[test] fn new_storage_is_empty() { let storage = MemoryBackend::new(); assert_eq!(storage.generation().unwrap(), 0); assert_eq!(storage.collection_count().unwrap(), 0); assert_eq!(storage.document_count().unwrap(), 0); }
    #[test] fn snapshots_share_each_collection_until_it_is_written() { let storage = MemoryBackend::new(); let write = |name: &str, id: &str| { let mut transaction = storage.begin().unwrap(); transaction.insert(&collection(name), document_id(id), Arc::new(Document::new())).unwrap(); transaction.commit().unwrap(); }; write("a", "1"); write("b", "2"); let before = MemorySnapshot::from_state(&storage.read_state().unwrap()); write("b", "3"); let after = MemorySnapshot::from_state(&storage.read_state().unwrap()); assert!(Arc::ptr_eq(&before.collections[&collection("a")], &after.collections[&collection("a")])); assert!(!Arc::ptr_eq(&before.collections[&collection("b")], &after.collections[&collection("b")])); assert_eq!((before.collections[&collection("b")].len(), after.collections[&collection("b")].len()), (1, 2)); assert_eq!((before.changed(&collection("a")).unwrap(), after.changed(&collection("b")).unwrap()), (Some(1), Some(3))); }
    #[test] fn empty_snapshot_has_no_collections() { let storage = MemoryBackend::new(); let snapshot = storage.read().unwrap(); assert!(snapshot.collections().unwrap().is_empty()); assert!(!snapshot.collection_exists(&collection("users")).unwrap()); }
    #[test] fn missing_document_returns_none() { let storage = MemoryBackend::new(); let snapshot = storage.read().unwrap(); assert!(snapshot .get(&collection("users"), &document_id("42")) .unwrap() .is_none()); }
    #[test] fn missing_collection_scan_is_empty() { let storage = MemoryBackend::new(); let snapshot = storage.read().unwrap(); assert!(snapshot .scan(&collection("users"), ScanOptions::default()) .unwrap() .is_empty()); }
    #[test] fn empty_commit_does_not_advance_generation() { let storage = MemoryBackend::new(); let transaction = storage.begin().unwrap(); let result = transaction.commit().unwrap(); assert!(result.is_empty()); assert_eq!(storage.generation().unwrap(), 0); }
    #[test] fn rollback_does_not_advance_generation() { let storage = MemoryBackend::new(); let transaction = storage.begin().unwrap(); transaction.rollback().unwrap(); assert_eq!(storage.generation().unwrap(), 0); }
    #[test] fn clear_on_empty_storage_is_a_no_op() { let storage = MemoryBackend::new(); storage.clear().unwrap(); assert_eq!(storage.generation().unwrap(), 0); }
    #[test] fn helper_rejects_incorrect_version() { let error = ensure_version( &collection("users"), &document_id("42"), DocumentVersion::new(3), VersionPrecondition::Exact(DocumentVersion::new(2)), ) .unwrap_err(); assert!(matches!( error.kind(), StorageErrorKind::VersionConflict { .. } )); }
    #[test] fn memory_storage_is_send_and_sync() { fn assert_send_and_sync<T: Send + Sync>() {} assert_send_and_sync::<MemoryBackend>(); }
    #[test] fn memory_backend_implements_storage_engine() { fn accept_backend(_: &dyn StorageEngine) {} let backend = MemoryBackend::new(); accept_backend(&backend); }
    #[test] fn atomic_batch_commits_one_generation() { let storage = MemoryBackend::new(); let users = collection("users"); let (stored, commit) = storage .apply_batch_atomic( &users, vec![ StorageMutation::insert(document_id("a"), Arc::new(Document::new())), StorageMutation::insert(document_id("b"), Arc::new(Document::new())), ], ) .unwrap(); assert_eq!(stored.len(), 2); assert_eq!(commit.inserted(), 2); assert_eq!(commit.replaced(), 0); assert_eq!(storage.generation().unwrap(), 1); assert_eq!(storage.document_count().unwrap(), 2); }
    #[test] fn atomic_batch_failure_leaves_storage_unchanged() { let storage = MemoryBackend::new(); let users = collection("users"); let duplicate = document_id("a"); let error = storage .apply_batch_atomic( &users, vec![ StorageMutation::insert(duplicate, Arc::new(Document::new())), StorageMutation::insert(duplicate, Arc::new(Document::new())), ], ) .unwrap_err(); assert!(matches!( error.kind(), StorageErrorKind::DocumentAlreadyExists { .. } )); assert_eq!(storage.generation().unwrap(), 0); assert_eq!(storage.collection_count().unwrap(), 0); assert_eq!(storage.document_count().unwrap(), 0); }
    #[test] fn atomic_batch_preserves_order_and_sequential_versions() { let storage = MemoryBackend::new(); let users = collection("users"); let id = document_id("a"); let (stored, commit) = storage .apply_batch_atomic( &users, vec![ StorageMutation::insert(id, Arc::new(Document::new())), StorageMutation::replace( id, Arc::new(Document::new()), VersionPrecondition::Exact(DocumentVersion::INITIAL), ), StorageMutation::replace( id, Arc::new(Document::new()), VersionPrecondition::Exact(DocumentVersion::new(2)), ), ], ) .unwrap(); assert_eq!( stored.iter().map(StoredDocument::id).collect::<Vec<_>>(), vec![&id, &id, &id] ); assert_eq!(stored[0].version(), DocumentVersion::INITIAL); assert_eq!(stored[1].version(), DocumentVersion::new(2)); assert_eq!(stored[2].version(), DocumentVersion::new(3)); assert_eq!(commit.inserted(), 1); assert_eq!(commit.replaced(), 2); }
    #[test] fn batch_applies_ordered_inserts_and_replacements() { let storage = MemoryBackend::new(); let users = collection("users"); let first = document_id("a"); let second = document_id("b"); let mut transaction = storage.begin().unwrap(); let stored = transaction .apply_batch( &users, vec![ StorageMutation::insert(first, Arc::new(Document::new())), StorageMutation::insert(second, Arc::new(Document::new())), StorageMutation::replace( first, Arc::new(Document::new()), VersionPrecondition::Exact(DocumentVersion::INITIAL), ), ], ) .unwrap(); assert_eq!(stored.len(), 3); assert_eq!(stored[0].id(), &first); assert_eq!(stored[1].id(), &second); assert_eq!(stored[2].id(), &first); assert_eq!(stored[2].version(), DocumentVersion::new(2)); let commit = transaction.commit().unwrap(); assert_eq!(commit.inserted(), 2); assert_eq!(commit.replaced(), 1); }
    #[test] fn compact_atomic_batch_returns_only_commit_summary() { let storage = MemoryBackend::new(); let users = collection("users"); let commit = storage .apply_batch_atomic_summary( &users, vec![ StorageMutation::insert(document_id("a"), Arc::new(Document::new())), StorageMutation::insert(document_id("b"), Arc::new(Document::new())), ], ) .unwrap(); assert_eq!(commit.inserted(), 2); assert_eq!(commit.replaced(), 0); assert_eq!(storage.generation().unwrap(), 1); assert_eq!(storage.document_count().unwrap(), 2); }
}
