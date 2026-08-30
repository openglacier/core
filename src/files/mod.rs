//! File metadata and storage-provider contracts.
//!
//! `_files` is an App-scoped metadata collection. File contents are never
//! stored in Glacier documents; a [`FileStore`] owns the binary payload.

mod model;
mod native;
mod store;
mod sync;
mod version;

pub use model::{
    FileCapabilities, FileEntry, FileId, FileKind, FileMetadata, FileModelError, StoreId,
    FILES_COLLECTION,
};
pub use native::NativeFileStore;
pub use store::{
    FileRange, FileReader, FileResult, FileStore, FileStoreEntry, FileStoreError, FileWrite,
};
pub use sync::{
    FileSyncConfig, FileSyncEntryState, FileSyncIndex, FileSyncIndexEntry, FileSyncSelection,
    APP_FILES_DIRECTORY, FILE_SYNC_CONFIG_VERSION, PRIMARY_APPS_COLLISION_NAME,
};

pub use version::FileVersion;
