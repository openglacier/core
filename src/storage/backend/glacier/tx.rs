#![cfg_attr(rustfmt, rustfmt_skip)]
//! Transactions: staged writes over a snapshot, committed only if no other
//! commit happened since the snapshot (optimistic).

use std::{collections::BTreeMap, sync::Arc};

use super::{column::Scratch, read::Snapshot, wal::LogEntry, Commit, GlacierBackend, Spilled, State};
use crate::{
    model::Document,
    storage::{
        common::ensure_version, CollectionId, CommitResult, DeleteResult, DocumentId, DocumentVersion, InsertResult, ReplaceResult,
        ScanDirection, ScanOptions, StorageError, StorageRead, StorageResult, StorageTransaction, StoredDocument, VersionPrecondition,
    },
};

pub(super) struct Transaction<'a> {
    backend: &'a GlacierBackend,
    snapshot: Snapshot,
    staged: BTreeMap<(CollectionId, DocumentId), Option<StoredDocument>>,
    entries: Vec<LogEntry>,
    counts: (u64, u64, u64),
    closed: bool,
    scratch: Scratch,
    /// Writes moved out of memory ([`StorageTransaction::spill`]).
    spilled: Option<Spilled>,
}

impl std::fmt::Debug for Transaction<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlacierTransaction").field("generation", &self.snapshot.generation()).field("entries", &self.entries.len()).finish()
    }
}

impl<'a> Transaction<'a> {
    pub(super) fn new(backend: &'a GlacierBackend, state: Arc<State>) -> Self {
        Self { backend, snapshot: Snapshot::new(state), staged: BTreeMap::new(), entries: Vec::new(), counts: (0, 0, 0), closed: false, scratch: Scratch::default(), spilled: None }
    }

    const fn ensure_open(&self) -> StorageResult<()> {
        if self.closed { Err(StorageError::transaction_closed()) } else { Ok(()) }
    }

    fn current(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> {
        match self.staged.get(&(collection.clone(), *id)) {
            Some(staged) => Ok(staged.clone()),
            None => self.snapshot.get(collection, id),
        }
    }

    /// Version of `id` in this transaction, its document left unread.
    fn current_version(&mut self, collection: &CollectionId, id: &DocumentId) -> StorageResult<DocumentVersion> {
        match self.staged.get(&(collection.clone(), *id)) {
            Some(staged) => staged.as_ref().map(StoredDocument::version),
            None => self.snapshot.version(collection, id, &mut self.scratch)?,
        }
        .ok_or_else(|| StorageError::document_not_found(collection.clone(), *id))
    }

    fn put(&mut self, collection: &CollectionId, stored: &StoredDocument) {
        self.staged.insert((collection.clone(), *stored.id()), Some(stored.clone()));
        self.entries.push(LogEntry::Put { collection: collection.clone(), id: *stored.id(), version: stored.version(), document: stored.shared_document() });
    }
}

impl StorageRead for Transaction<'_> {
    fn get(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> {
        self.ensure_open()?;
        self.current(collection, id)
    }

    fn scan(&self, collection: &CollectionId, options: ScanOptions) -> StorageResult<Vec<StoredDocument>> {
        self.ensure_open()?;
        let mut documents = self.snapshot.scan(collection, ScanOptions::default())?.into_iter().map(|stored| (*stored.id(), stored)).collect::<BTreeMap<_, _>>();
        for ((staged_collection, id), value) in &self.staged {
            if staged_collection != collection {
                continue;
            }
            match value {
                Some(stored) => { documents.insert(*id, stored.clone()); }
                None => { documents.remove(id); }
            }
        }
        let mut documents = documents.into_values().collect::<Vec<_>>();
        if options.direction() == ScanDirection::Reverse {
            documents.reverse();
        }
        if let Some(limit) = options.limit() {
            documents.truncate(limit);
        }
        Ok(documents)
    }

    fn scan_each(&self, collection: &CollectionId, options: ScanOptions, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> {
        for stored in self.scan(collection, options)? {
            if !visitor(stored)? {
                break;
            }
        }
        Ok(())
    }

    fn count(&self, collection: &CollectionId) -> StorageResult<u64> {
        self.ensure_open()?;
        let mut count = self.snapshot.count(collection)?;
        for ((staged_collection, id), value) in &self.staged {
            if staged_collection != collection {
                continue;
            }
            let before = self.snapshot.get(collection, id)?.is_some();
            match (before, value.is_some()) {
                (false, true) => count += 1,
                (true, false) => count = count.saturating_sub(1),
                _ => {}
            }
        }
        Ok(count)
    }

    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> { Ok(self.count(collection)? > 0) }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        let mut names = self.snapshot.collections()?;
        for (collection, _) in self.staged.keys() {
            if !names.contains(collection) {
                names.push(collection.clone());
            }
        }
        names.sort();
        names.dedup();
        let mut visible = Vec::with_capacity(names.len());
        for name in names {
            if self.count(&name)? > 0 {
                visible.push(name);
            }
        }
        Ok(visible)
    }
}

impl StorageTransaction for Transaction<'_> {
    fn insert(&mut self, collection: &CollectionId, id: DocumentId, document: Arc<Document>) -> StorageResult<InsertResult> {
        self.ensure_open()?;
        if self.current(collection, &id)?.is_some() {
            return Err(StorageError::document_already_exists(collection.clone(), id));
        }
        let stored = StoredDocument::new(id, DocumentVersion::INITIAL, document)?;
        self.put(collection, &stored);
        self.counts.0 += 1;
        Ok(InsertResult::new(stored))
    }

    fn replace(&mut self, collection: &CollectionId, id: &DocumentId, document: Arc<Document>, precondition: VersionPrecondition) -> StorageResult<ReplaceResult> {
        self.ensure_open()?;
        let current = self.current_version(collection, id)?;
        ensure_version(collection, id, current, precondition)?;
        let stored = StoredDocument::new(*id, current.next()?, document)?;
        self.put(collection, &stored);
        self.counts.1 += 1;
        Ok(ReplaceResult::new(current, stored))
    }

    fn delete(&mut self, collection: &CollectionId, id: &DocumentId, precondition: VersionPrecondition) -> StorageResult<DeleteResult> {
        self.ensure_open()?;
        let current = self.current_version(collection, id)?;
        ensure_version(collection, id, current, precondition)?;
        self.staged.insert((collection.clone(), *id), None);
        self.entries.push(LogEntry::Delete { collection: collection.clone(), id: *id });
        self.counts.2 += 1;
        Ok(DeleteResult::new(*id, current))
    }

    /// Writes the puts to segments of their own and marks what the writes
    /// remove; the staged map forgets them, so a later write of the same
    /// document fails (a conflict) or, for an insert, is not refused.
    fn spill(&mut self) -> StorageResult<bool> {
        self.ensure_open()?;
        self.spilled.get_or_insert_default().add(self.backend, self.snapshot.state(), &self.entries, &mut self.scratch)?;
        self.staged.clear();
        self.entries.clear();
        Ok(true)
    }

    fn commit(mut self: Box<Self>) -> StorageResult<CommitResult> {
        self.ensure_open()?;
        self.closed = true;
        let (inserted, replaced, deleted) = self.counts;
        let (generation, result) = (self.snapshot.generation(), CommitResult::new(inserted, replaced, deleted));
        if let Some(mut spilled) = self.spilled.take() {
            if let Err(error) = spilled.add(self.backend, self.snapshot.state(), &self.entries, &mut self.scratch) {
                spilled.discard(self.backend);
                return Err(error);
            }
            let backend = self.backend;
            drop(self);
            return backend.commit_spilled(generation, spilled, result);
        }
        let (backend, entries) = (self.backend, std::mem::take(&mut self.entries));
        // Release the snapshot first: the commit can then update the state in
        // place instead of copying the shared delta.
        drop(self);
        backend.commit(Commit { generation, entries, result })
    }

    fn rollback(mut self: Box<Self>) -> StorageResult<()> {
        self.ensure_open()?;
        self.closed = true;
        self.staged.clear();
        self.entries.clear();
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if let Some(spilled) = self.spilled.take() {
            spilled.discard(self.backend);
        }
    }
}
