//! Public native Glacier storage engine.

use std::path::Path;

use super::{
    backend::glacier::{
        GlacierBackend, GlacierCollectionMetadata, GlacierFormatInfo, GlacierWriteMetricsSnapshot,
    },
    BackendStorage, StorageResult,
};
use crate::MemoryGovernor;

/// Native OpenGlacier storage façade.
pub type GlacierStorage = BackendStorage<GlacierBackend>;

impl BackendStorage<GlacierBackend> {
    /// Opens or initializes a native Glacier store.
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        GlacierBackend::open(path).map(Self::from_backend)
    }
    /// Opens or initializes a native Glacier store and reports resident
    /// storage memory to the shared governor as diagnostic observed bytes.
    pub fn open_governed(path: impl AsRef<Path>, governor: MemoryGovernor) -> StorageResult<Self> {
        GlacierBackend::open_governed(path, governor).map(Self::from_backend)
    }

    #[must_use]
    pub fn format_info(&self) -> GlacierFormatInfo {
        self.backend().format_info()
    }

    /// Writes a durable checkpoint and resets the Glacier WAL.
    pub fn checkpoint(&self) -> StorageResult<()> {
        self.backend().checkpoint()
    }

    /// Returns cumulative Glacier write-path instrumentation.
    #[must_use]
    pub fn write_metrics(&self) -> GlacierWriteMetricsSnapshot {
        self.backend().write_metrics()
    }

    /// Returns the current Glacier store file size.
    pub fn store_bytes(&self) -> StorageResult<u64> {
        self.backend().store_bytes()
    }

    /// Returns the current Glacier WAL file size.
    pub fn wal_bytes(&self) -> StorageResult<u64> {
        self.backend().wal_bytes()
    }

    /// Returns the current committed generation.
    pub fn generation(&self) -> StorageResult<u64> {
        self.backend().generation()
    }

    /// Returns the current persistent document count.
    pub fn document_count(&self) -> StorageResult<usize> {
        self.backend().document_count()
    }

    /// Returns capability-aware metadata for one persistent collection.
    pub fn collection_metadata(
        &self,
        collection: &super::CollectionId,
    ) -> StorageResult<Option<GlacierCollectionMetadata>> {
        self.backend().collection_metadata(collection)
    }

    /// Returns metadata for all persistent collections.
    pub fn metadata(&self) -> StorageResult<Vec<GlacierCollectionMetadata>> {
        self.backend().metadata()
    }

    /// Returns Glacier cold-start instrumentation.
    #[must_use]
    pub fn startup_metrics(&self) -> impl serde::Serialize + '_ {
        self.backend().startup_metrics()
    }

    /// Returns Glacier read-path instrumentation.
    #[must_use]
    pub fn read_metrics(&self) -> impl serde::Serialize + '_ {
        self.backend().read_metrics()
    }

    /// Returns best-effort resident Glacier state memory accounting.
    #[must_use]
    pub fn resident_memory(&self) -> impl serde::Serialize + '_ {
        self.backend().resident_memory()
    }
}
