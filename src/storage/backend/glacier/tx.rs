#![cfg_attr(rustfmt, rustfmt_skip)]
//! Transactions: staged writes over a snapshot, committed only if no other
//! commit since the snapshot changed a collection they read or write
//! (optimistic).

use std::{cell::RefCell, collections::{BTreeMap, BTreeSet, HashMap}, sync::Arc};

use super::{column::Scratch, read::Snapshot, wal::LogEntry, Commit, GlacierBackend, Spilled, State, Watch};
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
    /// The entry of each document written, its last write: the version and document live there only.
    staged: HashMap<(CollectionId, DocumentId), usize>,
    entries: Vec<LogEntry>,
    counts: (u64, u64, u64),
    closed: bool,
    scratch: Scratch,
    /// Writes moved out of memory ([`StorageTransaction::spill`]).
    spilled: Option<Spilled>,
    /// The collections read or written, `None` once every collection was listed.
    watched: RefCell<Option<BTreeSet<CollectionId>>>,
    /// The collections inserted into: those neither read nor written otherwise conflict only on their new documents.
    inserted: BTreeSet<CollectionId>,
}

impl std::fmt::Debug for Transaction<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlacierTransaction").field("generation", &self.snapshot.generation()).field("entries", &self.entries.len()).finish()
    }
}

impl<'a> Transaction<'a> {
    pub(super) fn new(backend: &'a GlacierBackend, state: Arc<State>) -> Self {
        Self { backend, snapshot: Snapshot::new(state), staged: HashMap::new(), entries: Vec::new(), counts: (0, 0, 0), closed: false, scratch: Scratch::default(), spilled: None, watched: RefCell::new(Some(BTreeSet::new())), inserted: BTreeSet::new() }
    }

    fn watch(&self, collection: &CollectionId) {
        if let Some(watched) = self.watched.borrow_mut().as_mut() {
            if !watched.contains(collection) { watched.insert(collection.clone()); }
        }
    }

    fn watched(&self) -> Watch {
        let watched = self.watched.borrow_mut().take();
        let (all, watched) = (watched.is_none(), watched.unwrap_or_default());
        let inserted = self.inserted.iter().filter(|name| !watched.contains(*name)).cloned().collect::<Vec<_>>();
        Watch::of(self.snapshot.state(), watched, inserted, all)
    }

    const fn ensure_open(&self) -> StorageResult<()> {
        if self.closed { Err(StorageError::transaction_closed()) } else { Ok(()) }
    }

    fn current(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> {
        match self.staged(collection, id)? {
            Some(staged) => Ok(staged),
            None => self.snapshot.get(collection, id),
        }
    }

    /// Version of `id` in this transaction, its document left unread.
    fn current_version(&mut self, collection: &CollectionId, id: &DocumentId) -> StorageResult<DocumentVersion> {
        match self.staged.get(&(collection.clone(), *id)) {
            Some(at) => match &self.entries[*at] { LogEntry::Put { version, .. } => Some(*version), LogEntry::Delete { .. } => None },
            None => self.snapshot.version(collection, id, &mut self.scratch)?,
        }
        .ok_or_else(|| StorageError::document_not_found(collection.clone(), *id))
    }

    /// The document `id` as this transaction last wrote it (`Some(None)`: deleted); `None` when it did not.
    fn staged(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<Option<StoredDocument>>> {
        self.staged.get(&(collection.clone(), *id)).map(|at| self.entry(*at)).transpose()
    }

    fn entry(&self, at: usize) -> StorageResult<Option<StoredDocument>> {
        match &self.entries[at] {
            LogEntry::Put { id, version, document, .. } => StoredDocument::new(*id, *version, Arc::clone(document)).map(Some),
            LogEntry::Delete { .. } => Ok(None),
        }
    }

    fn put(&mut self, collection: &CollectionId, stored: &StoredDocument) {
        self.staged.insert((collection.clone(), *stored.id()), self.entries.len());
        self.entries.push(LogEntry::Put { collection: collection.clone(), id: *stored.id(), version: stored.version(), document: stored.shared_document() });
    }
}

impl StorageRead for Transaction<'_> {
    fn get(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> {
        self.ensure_open()?;
        self.watch(collection);
        self.current(collection, id)
    }

    fn scan(&self, collection: &CollectionId, options: ScanOptions) -> StorageResult<Vec<StoredDocument>> {
        self.ensure_open()?;
        self.watch(collection);
        let mut documents = self.snapshot.scan(collection, ScanOptions::default())?.into_iter().map(|stored| (*stored.id(), stored)).collect::<BTreeMap<_, _>>();
        for ((staged_collection, id), at) in &self.staged {
            if staged_collection != collection {
                continue;
            }
            match self.entry(*at)? {
                Some(stored) => { documents.insert(*id, stored); }
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
        self.watch(collection);
        let mut count = self.snapshot.count(collection)?;
        for ((staged_collection, id), at) in &self.staged {
            if staged_collection != collection {
                continue;
            }
            let before = self.snapshot.get(collection, id)?.is_some();
            match (before, matches!(self.entries[*at], LogEntry::Put { .. })) {
                (false, true) => count += 1,
                (true, false) => count = count.saturating_sub(1),
                _ => {}
            }
        }
        Ok(count)
    }

    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> { Ok(self.count(collection)? > 0) }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        *self.watched.borrow_mut() = None;
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
        if !self.inserted.contains(collection) { self.inserted.insert(collection.clone()); }
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
        self.watch(collection);
        let current = self.current_version(collection, id)?;
        ensure_version(collection, id, current, precondition)?;
        let stored = StoredDocument::new(*id, current.next()?, document)?;
        self.put(collection, &stored);
        self.counts.1 += 1;
        Ok(ReplaceResult::new(current, stored))
    }

    fn delete(&mut self, collection: &CollectionId, id: &DocumentId, precondition: VersionPrecondition) -> StorageResult<DeleteResult> {
        self.ensure_open()?;
        self.watch(collection);
        let current = self.current_version(collection, id)?;
        ensure_version(collection, id, current, precondition)?;
        self.staged.insert((collection.clone(), *id), self.entries.len());
        self.entries.push(LogEntry::Delete { collection: collection.clone(), id: *id });
        self.counts.2 += 1;
        Ok(DeleteResult::new(*id, current))
    }

    fn watch_collection(&mut self, collection: &CollectionId) { self.watch(collection); }

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
        let (result, watch) = (CommitResult::new(inserted, replaced, deleted), self.watched());
        if let Some(mut spilled) = self.spilled.take() {
            if let Err(error) = spilled.add(self.backend, self.snapshot.state(), &self.entries, &mut self.scratch) {
                spilled.discard(self.backend);
                return Err(error);
            }
            // The snapshot stays: what the spilled writes remove names its segments.
            return self.backend.commit_spilled(self.snapshot.state(), &watch, spilled, result);
        }
        let (backend, entries) = (self.backend, std::mem::take(&mut self.entries));
        // Release the snapshot first: the commit can then update the state in
        // place instead of copying the shared delta.
        drop(self);
        backend.commit(Commit { entries, result, watch })
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
