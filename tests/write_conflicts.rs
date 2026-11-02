//! A write the storage refuses for a conflict runs again, whole: what another commit changed first is read anew.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};

use og_core::{
    engine::Engine,
    query::{parse, PlannerPipeline},
    storage::{CollectionId, CommitResult, DeleteResult, DocumentId, InsertResult, MemoryStorage, ReplaceResult, ScanOptions, StorageEngine, StorageError, StorageRead, StorageResult, StorageTransaction, StoredDocument, VersionPrecondition},
    Document, Value,
};

fn pipeline(text: &str) -> PlannerPipeline {
    let ast = parse(text).unwrap_or_else(|error| panic!("{text}: {error}"));
    PlannerPipeline::from_ast(text, &ast).unwrap_or_else(|error| panic!("{text}: {error}"))
}

/// A memory storage whose first `refusals` commits are refused for a conflict; `begun` counts the transactions.
#[derive(Debug)]
struct Refusing { inner: MemoryStorage, refusals: AtomicUsize, begun: AtomicUsize }

impl Refusing {
    fn new(refusals: usize) -> Arc<Self> { Arc::new(Self { inner: MemoryStorage::new(), refusals: AtomicUsize::new(refusals), begun: AtomicUsize::new(0) }) }
}

struct RefusingTransaction<'a> { inner: Box<dyn StorageTransaction + 'a>, refuse: bool }

impl StorageRead for RefusingTransaction<'_> {
    fn get(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> { self.inner.get(collection, id) }
    fn scan(&self, collection: &CollectionId, options: ScanOptions) -> StorageResult<Vec<StoredDocument>> { self.inner.scan(collection, options) }
    fn count(&self, collection: &CollectionId) -> StorageResult<u64> { self.inner.count(collection) }
    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> { self.inner.collection_exists(collection) }
    fn collections(&self) -> StorageResult<Vec<CollectionId>> { self.inner.collections() }
}

impl StorageTransaction for RefusingTransaction<'_> {
    fn insert(&mut self, collection: &CollectionId, id: DocumentId, document: Arc<Document>) -> StorageResult<InsertResult> { self.inner.insert(collection, id, document) }
    fn replace(&mut self, collection: &CollectionId, id: &DocumentId, document: Arc<Document>, precondition: VersionPrecondition) -> StorageResult<ReplaceResult> { self.inner.replace(collection, id, document, precondition) }
    fn delete(&mut self, collection: &CollectionId, id: &DocumentId, precondition: VersionPrecondition) -> StorageResult<DeleteResult> { self.inner.delete(collection, id, precondition) }
    fn commit(self: Box<Self>) -> StorageResult<CommitResult> { if self.refuse { self.inner.rollback()?; Err(StorageError::transaction_conflict("refused for the test")) } else { self.inner.commit() } }
    fn rollback(self: Box<Self>) -> StorageResult<()> { self.inner.rollback() }
}

impl StorageEngine for Refusing {
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>> { self.inner.read() }
    fn begin(&self) -> StorageResult<Box<dyn StorageTransaction + '_>> {
        self.begun.fetch_add(1, Ordering::SeqCst);
        let refuse = self.refusals.try_update(Ordering::SeqCst, Ordering::SeqCst, |left| left.checked_sub(1)).is_ok();
        Ok(Box::new(RefusingTransaction { inner: self.inner.begin()?, refuse }))
    }
}

fn seeded(refusals: usize) -> (Arc<Refusing>, Engine) {
    let storage = Refusing::new(0);
    let engine = Engine::new(storage.clone());
    for n in 1..=3 { engine.execute(&pipeline(&format!("on users | insert {{n: {n}}}"))).unwrap(); }
    storage.refusals.store(refusals, Ordering::SeqCst);
    storage.begun.store(0, Ordering::SeqCst);
    (storage, engine)
}

fn values(engine: &Engine, text: &str) -> Vec<Value> {
    let mut values = engine.execute(&pipeline(text)).unwrap().output().rows().iter().map(|row| row.document().get("n").cloned().unwrap_or(Value::Null)).collect::<Vec<_>>();
    values.sort_by_key(|value| format!("{value:?}"));
    values
}

#[test] fn a_set_refused_once_for_a_conflict_runs_again_and_commits_once() { let (storage, engine) = seeded(1); let output = engine.execute(&pipeline("on users | set n = n * 10")).unwrap(); assert_eq!(output.output().rows().len(), 3); assert_eq!(storage.begun.load(Ordering::SeqCst), 2); assert_eq!(values(&engine, "on users"), values(&engine, "on users | where n >= 10")); assert_eq!(values(&engine, "on users").len(), 3); }
#[test] fn an_insert_refused_twice_for_a_conflict_is_stored_once() { let (storage, engine) = seeded(2); engine.execute(&pipeline("on users | insert {n: 4}")).unwrap(); assert_eq!(storage.begun.load(Ordering::SeqCst), 3); assert_eq!(values(&engine, "on users | where n == 4").len(), 1); }
#[test] fn a_write_refused_five_times_returns_the_conflict_and_writes_nothing() { let (storage, engine) = seeded(5); let error = engine.execute(&pipeline("on users | set n = 0")).unwrap_err(); assert!(error.to_string().contains("refused for the test"), "{error}"); assert_eq!(storage.begun.load(Ordering::SeqCst), 5); assert!(values(&engine, "on users | where n == 0").is_empty()); }
#[test] fn a_write_refused_four_times_commits_at_the_fifth_attempt() { let (storage, engine) = seeded(4); engine.execute(&pipeline("on users | set n = 0")).unwrap(); assert_eq!(storage.begun.load(Ordering::SeqCst), 5); assert_eq!(values(&engine, "on users | where n == 0").len(), 3); }
#[test] fn a_write_failing_otherwise_is_not_run_again() { let (storage, engine) = seeded(0); assert!(engine.execute(&pipeline("on users | into _forbidden")).is_err()); assert!(storage.begun.load(Ordering::SeqCst) <= 1); }
#[test] fn concurrent_writes_to_one_collection_all_commit() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let engine = Arc::new(Engine::new(storage)); let threads = (0..4).map(|thread| { let engine = Arc::clone(&engine); std::thread::spawn(move || for n in 0..25 { engine.execute(&pipeline(&format!("on users | insert {{n: {}}}", thread * 100 + n))).unwrap(); }) }).collect::<Vec<_>>(); threads.into_iter().for_each(|thread| thread.join().unwrap()); assert_eq!(values(&engine, "on users").len(), 100); }
