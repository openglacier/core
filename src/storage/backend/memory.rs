//! Physical in-memory storage backend.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::Document;

use super::super::{
    common::{ensure_version, increment_counter, next_generation},
    CollectionId, CommitResult, DeleteResult, DocumentId, DocumentVersion, InsertResult,
    ReplaceResult, ScanDirection, ScanOptions, StorageError, StorageMutation, StorageRead,
    StorageResult, StorageTransaction, StoredDocument, VersionPrecondition,
};
use super::StorageBackend;

/// Thread-safe in-memory storage engine.
///
/// Cloning this type creates another handle to the same underlying database.
#[derive(Clone, Debug, Default)]
pub struct MemoryBackend {
    inner: Arc<RwLock<MemoryState>>,
}

impl MemoryBackend {
    /// Creates an empty in-memory storage engine.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
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

        state.collections.clear();
        state.generation = next_generation(state.generation)?;

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

impl StorageBackend for MemoryBackend {
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>> {
        let snapshot = {
            let state = self.read_state()?;
            MemorySnapshot::from_state(&state)
        };

        Ok(Box::new(snapshot))
    }

    fn begin(&self) -> StorageResult<Box<dyn StorageTransaction + '_>> {
        let transaction = {
            let state = self.read_state()?;
            MemoryTransaction::from_state(self, &state)
        };

        Ok(Box::new(transaction))
    }

    fn apply_batch_atomic(
        &self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<(Vec<StoredDocument>, CommitResult)> {
        if mutations.is_empty() {
            return Ok((Vec::new(), CommitResult::default()));
        }

        let mut state = self.write_state()?;
        let target = state.collections.get(collection);
        let mut staged = BTreeMap::<DocumentId, StoredDocument>::new();
        let mut stored = Vec::with_capacity(mutations.len());
        let mut inserted = 0u64;
        let mut replaced = 0u64;

        for mutation in mutations {
            match mutation {
                StorageMutation::Insert { id, document } => {
                    if staged.contains_key(&id)
                        || target.is_some_and(|target| target.contains_key(&id))
                    {
                        return Err(StorageError::document_already_exists(
                            collection.clone(),
                            id,
                        ));
                    }

                    let document =
                        StoredDocument::new(id.clone(), DocumentVersion::INITIAL, document)?;
                    staged.insert(id, document.clone());
                    stored.push(document);
                    inserted = increment_counter(inserted)?;
                }
                StorageMutation::Replace {
                    id,
                    document,
                    precondition,
                } => {
                    let current = staged
                        .get(&id)
                        .or_else(|| target.and_then(|target| target.get(&id)))
                        .ok_or_else(|| {
                            StorageError::document_not_found(collection.clone(), id.clone())
                        })?;
                    ensure_version(collection, &id, current.version(), precondition)?;
                    let document =
                        StoredDocument::new(id.clone(), current.version().next()?, document)?;
                    staged.insert(id, document.clone());
                    stored.push(document);
                    replaced = increment_counter(replaced)?;
                }
            }
        }

        let target = state.collections.entry(collection.clone()).or_default();
        for (id, document) in staged {
            target.insert(id, document);
        }
        state.generation = next_generation(state.generation)?;

        Ok((stored, CommitResult::new(inserted, replaced, 0)))
    }

    fn apply_batch_atomic_summary(
        &self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<CommitResult> {
        if mutations.is_empty() {
            return Ok(CommitResult::default());
        }

        let mut state = self.write_state()?;
        let target = state.collections.get(collection);
        let mut staged = BTreeMap::<DocumentId, StoredDocument>::new();
        let mut inserted = 0u64;
        let mut replaced = 0u64;

        for mutation in mutations {
            match mutation {
                StorageMutation::Insert { id, document } => {
                    if staged.contains_key(&id)
                        || target.is_some_and(|target| target.contains_key(&id))
                    {
                        return Err(StorageError::document_already_exists(
                            collection.clone(),
                            id,
                        ));
                    }

                    staged.insert(
                        id.clone(),
                        StoredDocument::new(id, DocumentVersion::INITIAL, document)?,
                    );
                    inserted = increment_counter(inserted)?;
                }
                StorageMutation::Replace {
                    id,
                    document,
                    precondition,
                } => {
                    let current = staged
                        .get(&id)
                        .or_else(|| target.and_then(|target| target.get(&id)))
                        .ok_or_else(|| {
                            StorageError::document_not_found(collection.clone(), id.clone())
                        })?;
                    ensure_version(collection, &id, current.version(), precondition)?;
                    staged.insert(
                        id.clone(),
                        StoredDocument::new(id, current.version().next()?, document)?,
                    );
                    replaced = increment_counter(replaced)?;
                }
            }
        }

        let target = state.collections.entry(collection.clone()).or_default();
        for (id, document) in staged {
            target.insert(id, document);
        }
        state.generation = next_generation(state.generation)?;

        Ok(CommitResult::new(inserted, replaced, 0))
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct MemoryState {
    pub(super) generation: u64,
    pub(super) collections: Collections,
}

pub(super) type Collections = BTreeMap<CollectionId, Collection>;
pub(crate) type Collection = BTreeMap<DocumentId, StoredDocument>;

/// Immutable in-memory read snapshot.
#[derive(Clone, Debug)]
struct MemorySnapshot {
    generation: u64,
    collections: Collections,
}

impl MemorySnapshot {
    fn from_state(state: &MemoryState) -> Self {
        Self {
            generation: state.generation,
            collections: state.collections.clone(),
        }
    }

    #[allow(dead_code)]
    const fn generation(&self) -> u64 {
        self.generation
    }
}

impl StorageRead for MemorySnapshot {
    fn get(
        &self,
        collection: &CollectionId,
        id: &DocumentId,
    ) -> StorageResult<Option<StoredDocument>> {
        Ok(get_document(&self.collections, collection, id))
    }

    fn scan(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
    ) -> StorageResult<Vec<StoredDocument>> {
        Ok(scan_collection(&self.collections, collection, options))
    }

    fn scan_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        let Some(documents) = self.collections.get(collection) else {
            return Ok(());
        };
        let limit = options.limit().unwrap_or(usize::MAX);
        match options.direction() {
            ScanDirection::Forward => {
                for stored in documents.values().take(limit) {
                    if !visitor(stored.clone())? {
                        break;
                    }
                }
            }
            ScanDirection::Reverse => {
                for stored in documents.values().rev().take(limit) {
                    if !visitor(stored.clone())? {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn count(&self, collection: &CollectionId) -> StorageResult<u64> {
        let count = self.collections.get(collection).map_or(0, BTreeMap::len);
        u64::try_from(count).map_err(|_| StorageError::backend("collection count overflow"))
    }

    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> {
        Ok(self.collections.contains_key(collection))
    }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        Ok(list_collections(&self.collections))
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
        }
    }

    fn insert_batch(
        &mut self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<Vec<StoredDocument>> {
        let target = self.collections.entry(collection.clone()).or_default();
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
            let document = StoredDocument::new(id.clone(), DocumentVersion::INITIAL, document)?;
            pending.insert(id, document.clone());
            stored.push(document);
        }

        self.inserted = self
            .inserted
            .checked_add(stored.len() as u64)
            .ok_or_else(|| StorageError::backend("in-memory mutation counter overflow"))?;
        target.append(&mut pending);
        Ok(stored)
    }
}

impl StorageRead for MemoryTransaction<'_> {
    fn get(
        &self,
        collection: &CollectionId,
        id: &DocumentId,
    ) -> StorageResult<Option<StoredDocument>> {
        Ok(get_document(&self.collections, collection, id))
    }

    fn scan(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
    ) -> StorageResult<Vec<StoredDocument>> {
        Ok(scan_collection(&self.collections, collection, options))
    }

    fn count(&self, collection: &CollectionId) -> StorageResult<u64> {
        let count = self.collections.get(collection).map_or(0, BTreeMap::len);
        u64::try_from(count).map_err(|_| StorageError::backend("collection count overflow"))
    }

    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> {
        Ok(self.collections.contains_key(collection))
    }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        Ok(list_collections(&self.collections))
    }
}

impl StorageTransaction for MemoryTransaction<'_> {
    fn insert(
        &mut self,
        collection: &CollectionId,
        id: DocumentId,
        document: Arc<Document>,
    ) -> StorageResult<InsertResult> {
        let target = self.collections.entry(collection.clone()).or_default();

        match target.entry(id.clone()) {
            Entry::Occupied(_) => Err(StorageError::document_already_exists(
                collection.clone(),
                id,
            )),
            Entry::Vacant(entry) => {
                let stored = StoredDocument::new(id, DocumentVersion::INITIAL, document)?;

                entry.insert(stored.clone());
                self.inserted = increment_counter(self.inserted)?;

                Ok(InsertResult::new(stored))
            }
        }
    }

    fn replace(
        &mut self,
        collection: &CollectionId,
        id: &DocumentId,
        document: Arc<Document>,
        precondition: VersionPrecondition,
    ) -> StorageResult<ReplaceResult> {
        let target = self
            .collections
            .get_mut(collection)
            .and_then(|documents| documents.get_mut(id))
            .ok_or_else(|| StorageError::document_not_found(collection.clone(), id.clone()))?;

        let previous_version = target.version();
        ensure_version(collection, id, previous_version, precondition)?;

        let next_version = previous_version.next()?;
        let stored = StoredDocument::new(id.clone(), next_version, document)?;

        *target = stored.clone();
        self.replaced = increment_counter(self.replaced)?;

        Ok(ReplaceResult::new(previous_version, stored))
    }

    fn delete(
        &mut self,
        collection: &CollectionId,
        id: &DocumentId,
        precondition: VersionPrecondition,
    ) -> StorageResult<DeleteResult> {
        let current_version = self
            .collections
            .get(collection)
            .and_then(|documents| documents.get(id))
            .map(StoredDocument::version)
            .ok_or_else(|| StorageError::document_not_found(collection.clone(), id.clone()))?;

        ensure_version(collection, id, current_version, precondition)?;

        let remove_collection = {
            let documents = self
                .collections
                .get_mut(collection)
                .ok_or_else(|| StorageError::document_not_found(collection.clone(), id.clone()))?;

            let removed = documents
                .remove(id)
                .ok_or_else(|| StorageError::document_not_found(collection.clone(), id.clone()))?;

            debug_assert_eq!(removed.version(), current_version);
            documents.is_empty()
        };

        if remove_collection {
            self.collections.remove(collection);
        }

        self.deleted = increment_counter(self.deleted)?;

        Ok(DeleteResult::new(id.clone(), current_version))
    }

    fn apply_batch(
        &mut self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<Vec<StoredDocument>> {
        if mutations
            .iter()
            .all(|mutation| matches!(mutation, StorageMutation::Insert { .. }))
        {
            return self.insert_batch(collection, mutations);
        }

        let mut stored = Vec::with_capacity(mutations.len());
        let target = self.collections.entry(collection.clone()).or_default();

        for mutation in mutations {
            match mutation {
                StorageMutation::Insert { id, document } => match target.entry(id.clone()) {
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
                        StorageError::document_not_found(collection.clone(), id.clone())
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

        Ok(result)
    }

    fn rollback(self: Box<Self>) -> StorageResult<()> {
        Ok(())
    }
}

fn get_document(
    collections: &Collections,
    collection: &CollectionId,
    id: &DocumentId,
) -> Option<StoredDocument> {
    collections
        .get(collection)
        .and_then(|documents| documents.get(id))
        .cloned()
}

fn scan_collection(
    collections: &Collections,
    collection: &CollectionId,
    options: ScanOptions,
) -> Vec<StoredDocument> {
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

    #[test]
    fn new_storage_is_empty() {
        let storage = MemoryBackend::new();

        assert_eq!(storage.generation().unwrap(), 0);
        assert_eq!(storage.collection_count().unwrap(), 0);
        assert_eq!(storage.document_count().unwrap(), 0);
    }

    #[test]
    fn empty_snapshot_has_no_collections() {
        let storage = MemoryBackend::new();
        let snapshot = storage.read().unwrap();

        assert!(snapshot.collections().unwrap().is_empty());
        assert!(!snapshot.collection_exists(&collection("users")).unwrap());
    }

    #[test]
    fn missing_document_returns_none() {
        let storage = MemoryBackend::new();
        let snapshot = storage.read().unwrap();

        assert!(snapshot
            .get(&collection("users"), &document_id("42"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn missing_collection_scan_is_empty() {
        let storage = MemoryBackend::new();
        let snapshot = storage.read().unwrap();

        assert!(snapshot
            .scan(&collection("users"), ScanOptions::default())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn empty_commit_does_not_advance_generation() {
        let storage = MemoryBackend::new();
        let transaction = storage.begin().unwrap();

        let result = transaction.commit().unwrap();

        assert!(result.is_empty());
        assert_eq!(storage.generation().unwrap(), 0);
    }

    #[test]
    fn rollback_does_not_advance_generation() {
        let storage = MemoryBackend::new();
        let transaction = storage.begin().unwrap();

        transaction.rollback().unwrap();

        assert_eq!(storage.generation().unwrap(), 0);
    }

    #[test]
    fn clear_on_empty_storage_is_a_no_op() {
        let storage = MemoryBackend::new();

        storage.clear().unwrap();

        assert_eq!(storage.generation().unwrap(), 0);
    }

    #[test]
    fn helper_rejects_incorrect_version() {
        let error = ensure_version(
            &collection("users"),
            &document_id("42"),
            DocumentVersion::new(3),
            VersionPrecondition::Exact(DocumentVersion::new(2)),
        )
        .unwrap_err();

        assert!(matches!(
            error.kind(),
            StorageErrorKind::VersionConflict { .. }
        ));
    }

    #[test]
    fn memory_storage_is_send_and_sync() {
        fn assert_send_and_sync<T: Send + Sync>() {}

        assert_send_and_sync::<MemoryBackend>();
    }

    #[test]
    fn memory_backend_implements_storage_backend() {
        fn accept_backend(_: &dyn StorageBackend) {}

        let backend = MemoryBackend::new();
        accept_backend(&backend);
    }
    #[test]
    fn atomic_batch_commits_one_generation() {
        let storage = MemoryBackend::new();
        let users = collection("users");

        let (stored, commit) = storage
            .apply_batch_atomic(
                &users,
                vec![
                    StorageMutation::insert(document_id("a"), Arc::new(Document::new())),
                    StorageMutation::insert(document_id("b"), Arc::new(Document::new())),
                ],
            )
            .unwrap();

        assert_eq!(stored.len(), 2);
        assert_eq!(commit.inserted(), 2);
        assert_eq!(commit.replaced(), 0);
        assert_eq!(storage.generation().unwrap(), 1);
        assert_eq!(storage.document_count().unwrap(), 2);
    }

    #[test]
    fn atomic_batch_failure_leaves_storage_unchanged() {
        let storage = MemoryBackend::new();
        let users = collection("users");
        let duplicate = document_id("a");

        let error = storage
            .apply_batch_atomic(
                &users,
                vec![
                    StorageMutation::insert(duplicate.clone(), Arc::new(Document::new())),
                    StorageMutation::insert(duplicate, Arc::new(Document::new())),
                ],
            )
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            StorageErrorKind::DocumentAlreadyExists { .. }
        ));
        assert_eq!(storage.generation().unwrap(), 0);
        assert_eq!(storage.collection_count().unwrap(), 0);
        assert_eq!(storage.document_count().unwrap(), 0);
    }

    #[test]
    fn atomic_batch_preserves_order_and_sequential_versions() {
        let storage = MemoryBackend::new();
        let users = collection("users");
        let id = document_id("a");

        let (stored, commit) = storage
            .apply_batch_atomic(
                &users,
                vec![
                    StorageMutation::insert(id.clone(), Arc::new(Document::new())),
                    StorageMutation::replace(
                        id.clone(),
                        Arc::new(Document::new()),
                        VersionPrecondition::Exact(DocumentVersion::INITIAL),
                    ),
                    StorageMutation::replace(
                        id.clone(),
                        Arc::new(Document::new()),
                        VersionPrecondition::Exact(DocumentVersion::new(2)),
                    ),
                ],
            )
            .unwrap();

        assert_eq!(
            stored.iter().map(StoredDocument::id).collect::<Vec<_>>(),
            vec![&id, &id, &id]
        );
        assert_eq!(stored[0].version(), DocumentVersion::INITIAL);
        assert_eq!(stored[1].version(), DocumentVersion::new(2));
        assert_eq!(stored[2].version(), DocumentVersion::new(3));
        assert_eq!(commit.inserted(), 1);
        assert_eq!(commit.replaced(), 2);
    }

    #[test]
    fn batch_applies_ordered_inserts_and_replacements() {
        let storage = MemoryBackend::new();
        let users = collection("users");
        let first = document_id("a");
        let second = document_id("b");
        let mut transaction = storage.begin().unwrap();

        let stored = transaction
            .apply_batch(
                &users,
                vec![
                    StorageMutation::insert(first.clone(), Arc::new(Document::new())),
                    StorageMutation::insert(second.clone(), Arc::new(Document::new())),
                    StorageMutation::replace(
                        first.clone(),
                        Arc::new(Document::new()),
                        VersionPrecondition::Exact(DocumentVersion::INITIAL),
                    ),
                ],
            )
            .unwrap();

        assert_eq!(stored.len(), 3);
        assert_eq!(stored[0].id(), &first);
        assert_eq!(stored[1].id(), &second);
        assert_eq!(stored[2].id(), &first);
        assert_eq!(stored[2].version(), DocumentVersion::new(2));

        let commit = transaction.commit().unwrap();
        assert_eq!(commit.inserted(), 2);
        assert_eq!(commit.replaced(), 1);
    }
    #[test]
    fn compact_atomic_batch_returns_only_commit_summary() {
        let storage = MemoryBackend::new();
        let users = collection("users");

        let commit = storage
            .apply_batch_atomic_summary(
                &users,
                vec![
                    StorageMutation::insert(document_id("a"), Arc::new(Document::new())),
                    StorageMutation::insert(document_id("b"), Arc::new(Document::new())),
                ],
            )
            .unwrap();

        assert_eq!(commit.inserted(), 2);
        assert_eq!(commit.replaced(), 0);
        assert_eq!(storage.generation().unwrap(), 1);
        assert_eq!(storage.document_count().unwrap(), 2);
    }
}
