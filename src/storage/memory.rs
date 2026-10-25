#![cfg_attr(rustfmt, rustfmt_skip)]
//! Public in-memory storage engine. Thread-safe in-memory storage engine.

/// Thread-safe in-memory storage.
pub type MemoryStorage = super::MemoryBackend;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageEngine;

    #[test] fn public_memory_storage_uses_memory_backend() { let storage = MemoryStorage::new(); assert_eq!(storage.generation().unwrap(), 0); assert_eq!(storage.collection_count().unwrap(), 0); assert_eq!(storage.document_count().unwrap(), 0); }
    #[test] fn reads_an_empty_store() { let storage = MemoryStorage::new(); assert!(StorageEngine::read(&storage) .unwrap() .collections() .unwrap() .is_empty()); }
}
