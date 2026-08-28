//! Public in-memory storage engine. Thread-safe in-memory storage engine.

use super::{backend::memory::MemoryBackend, BackendStorage, StorageResult};
pub type MemoryStorage = BackendStorage<MemoryBackend>;

impl BackendStorage<MemoryBackend> {
    /// Creates an empty in-memory storage engine.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::from_backend(MemoryBackend::new())
    }

    /// Returns the current committed generation.
    pub fn generation(&self) -> StorageResult<u64> {
        self.backend().generation()
    }

    /// Returns the number of committed collections.
    pub fn collection_count(&self) -> StorageResult<usize> {
        self.backend().collection_count()
    }

    /// Returns the number of committed documents across all collections.
    pub fn document_count(&self) -> StorageResult<usize> {
        self.backend().document_count()
    }

    /// Removes all committed data atomically.
    pub fn clear(&self) -> StorageResult<()> {
        self.backend().clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{StorageBackend, StorageEngine};

    #[test]
    fn public_memory_storage_uses_memory_backend() {
        let storage = MemoryStorage::new();

        assert_eq!(storage.generation().unwrap(), 0);
        assert_eq!(storage.collection_count().unwrap(), 0);
        assert_eq!(storage.document_count().unwrap(), 0);
    }

    #[test]
    fn facade_delegates_reads_to_backend() {
        let storage = MemoryStorage::new();

        assert!(StorageEngine::read(&storage)
            .unwrap()
            .collections()
            .unwrap()
            .is_empty());
        assert!(StorageBackend::read(storage.backend())
            .unwrap()
            .collections()
            .unwrap()
            .is_empty());
    }
}
