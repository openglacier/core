//! Physical storage backend contracts and implementations.
//!
//! A backend owns the persistence mechanism and exposes consistent snapshots,
//! transactions, and optimized atomic batch operations. The public
//! [`StorageEngine`](crate::storage::StorageEngine) façade is implemented once
//! by [`BackendStorage`](crate::storage::BackendStorage), independently of the
//! selected backend.

pub mod glacier;
pub mod memory;

use super::{
    CollectionId, CommitResult, StorageMutation, StorageRead, StorageResult, StorageTransaction,
    StoredDocument,
};

/// Common contract implemented by physical storage backends.
///
/// Implementations may use memory, an embedded database, or a custom storage
/// engine. This interface deliberately mirrors only the physical operations
/// required by the public storage façade.
pub trait StorageBackend: Send + Sync {
    /// Opens a consistent backend read snapshot.
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>>;

    /// Begins a mutable multi-collection transaction.
    fn begin(&self) -> StorageResult<Box<dyn StorageTransaction + '_>>;

    /// Applies a complete mutation vector atomically.
    ///
    /// Backends should override this method when they can validate and commit a
    /// batch more efficiently than the generic transaction path.
    fn apply_batch_atomic(
        &self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<(Vec<StoredDocument>, CommitResult)> {
        let mut transaction = self.begin()?;
        let stored = transaction.apply_batch(collection, mutations)?;
        let commit = transaction.commit()?;
        Ok((stored, commit))
    }

    /// Applies a complete mutation vector atomically without returning rows.
    fn apply_batch_atomic_summary(
        &self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<CommitResult> {
        self.apply_batch_atomic(collection, mutations)
            .map(|(_, commit)| commit)
    }
}
