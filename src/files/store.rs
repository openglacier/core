//! Provider-neutral file-store contract.

use std::{error::Error as StdError, fmt, io::Read};

use super::{FileCapabilities, FileKind, FileMetadata, StoreId};

/// Reader returned by a [`FileStore`].
pub type FileReader = Box<dyn Read + Send>;
/// Result returned by file-store operations.
pub type FileResult<T> = Result<T, FileStoreError>;

/// Target and metadata hints for one streamed write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileWrite<'a> {
    /// Existing provider ID when replacing a file; `None` creates one.
    pub remote_id: Option<&'a str>,
    pub parent_remote_id: Option<&'a str>,
    pub name: &'a str,
    pub content_type: Option<&'a str>,
    pub size: Option<u64>,
}

/// Byte range requested from a provider.
///
/// `length == None` means "from offset to end". Suffix ranges are resolved by
/// the protocol adapter after `stat`, keeping provider implementations simple.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileRange {
    pub offset: u64,
    pub length: Option<u64>,
}

impl FileRange {
    #[must_use]
    pub const fn new(offset: u64, length: Option<u64>) -> Self {
        Self { offset, length }
    }
}

/// Provider-facing entry. It intentionally contains no Place/App scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStoreEntry {
    pub remote_id: String,
    pub parent_remote_id: Option<String>,
    pub name: String,
    pub kind: FileKind,
    pub metadata: FileMetadata,
}

/// Backend contract implemented by native, WebDAV, S3, Drive, OneDrive, etc.
///
/// Remote identifiers are opaque. Implementations must not assume they are
/// local filesystem paths.
pub trait FileStore: Send + Sync {
    /// Stable identifier used by `_files.storeId`.
    fn store_id(&self) -> &StoreId;

    /// Operations implemented by this provider.
    fn capabilities(&self) -> FileCapabilities;

    /// Loads one remote entry by opaque provider identifier.
    fn stat(&self, remote_id: &str) -> FileResult<FileStoreEntry>;

    /// Lists direct children. `None` addresses the provider root.
    fn list(&self, parent_remote_id: Option<&str>) -> FileResult<Vec<FileStoreEntry>>;

    /// Opens a streaming reader, optionally from a byte range.
    fn read(&self, remote_id: &str, range: Option<FileRange>) -> FileResult<FileReader>;

    /// Streams a new/replacement file to the provider.
    fn write(&self, target: FileWrite<'_>, source: &mut dyn Read) -> FileResult<FileStoreEntry>;

    /// Creates a directory under the provider root or another directory.
    fn mkdir(&self, parent_remote_id: Option<&str>, name: &str) -> FileResult<FileStoreEntry>;

    /// Moves/renames one entry without assuming path-based addressing.
    fn move_entry(
        &self,
        remote_id: &str,
        new_parent_remote_id: Option<&str>,
        new_name: &str,
    ) -> FileResult<FileStoreEntry>;

    /// Copies one entry without assuming server-side copy support.
    fn copy(
        &self,
        remote_id: &str,
        new_parent_remote_id: Option<&str>,
        new_name: &str,
    ) -> FileResult<FileStoreEntry>;

    /// Deletes one provider entry.
    fn delete(&self, remote_id: &str) -> FileResult<()>;
}

/// Provider-level failure independent from any concrete backend SDK.
#[derive(Debug)]
pub enum FileStoreError {
    NotFound,
    AlreadyExists,
    InvalidRemoteId,
    InvalidName,
    NotDirectory,
    NotFile,
    SizeMismatch { expected: u64, actual: u64 },
    Unsupported(&'static str),
    InvalidRange,
    Io(std::io::Error),
    Backend(String),
}

impl fmt::Display for FileStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("file entry not found"),
            Self::AlreadyExists => f.write_str("file entry already exists"),
            Self::InvalidRemoteId => f.write_str("invalid file-store remote ID"),
            Self::InvalidName => f.write_str("invalid file name"),
            Self::NotDirectory => f.write_str("file entry is not a directory"),
            Self::NotFile => f.write_str("file entry is not a regular file"),
            Self::SizeMismatch { expected, actual } => {
                write!(
                    f,
                    "file size mismatch: expected {expected} bytes, wrote {actual}"
                )
            }
            Self::Unsupported(operation) => {
                write!(f, "file-store operation {operation} is unsupported")
            }
            Self::InvalidRange => f.write_str("invalid file byte range"),
            Self::Io(error) => write!(f, "file-store I/O error: {error}"),
            Self::Backend(message) => write!(f, "file-store backend error: {message}"),
        }
    }
}

impl StdError for FileStoreError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for FileStoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ContractOnlyStore {
        id: StoreId,
    }
    impl FileStore for ContractOnlyStore {
        fn store_id(&self) -> &StoreId {
            &self.id
        }
        fn capabilities(&self) -> FileCapabilities {
            FileCapabilities::default()
        }
        fn stat(&self, _: &str) -> FileResult<FileStoreEntry> {
            Err(FileStoreError::NotFound)
        }
        fn list(&self, _: Option<&str>) -> FileResult<Vec<FileStoreEntry>> {
            Ok(Vec::new())
        }
        fn read(&self, _: &str, _: Option<FileRange>) -> FileResult<FileReader> {
            Err(FileStoreError::Unsupported("read"))
        }
        fn write(&self, _: FileWrite<'_>, _: &mut dyn Read) -> FileResult<FileStoreEntry> {
            Err(FileStoreError::Unsupported("write"))
        }
        fn mkdir(&self, _: Option<&str>, _: &str) -> FileResult<FileStoreEntry> {
            Err(FileStoreError::Unsupported("mkdir"))
        }
        fn move_entry(&self, _: &str, _: Option<&str>, _: &str) -> FileResult<FileStoreEntry> {
            Err(FileStoreError::Unsupported("move"))
        }
        fn copy(&self, _: &str, _: Option<&str>, _: &str) -> FileResult<FileStoreEntry> {
            Err(FileStoreError::Unsupported("copy"))
        }
        fn delete(&self, _: &str) -> FileResult<()> {
            Err(FileStoreError::Unsupported("delete"))
        }
    }

    #[test]
    fn contract_is_object_safe() {
        let store: Box<dyn FileStore> = Box::new(ContractOnlyStore {
            id: StoreId::from("test"),
        });
        assert_eq!(store.store_id().as_str(), "test");
        assert!(store.list(None).expect("list root").is_empty());
    }
}
