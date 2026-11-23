#![cfg_attr(rustfmt, rustfmt_skip)]
//! App-scoped file metadata persisted in `_files`.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Collection containing File App metadata.
pub const FILES_COLLECTION: &str = "_files";

/// Stable OG identifier for one logical file entry.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId(String);

impl FileId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for FileId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
impl From<String> for FileId {
    fn from(value: String) -> Self {
        Self(value)
    }
}
impl From<&str> for FileId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// Opaque identifier of one registered file-store provider.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StoreId(String);

impl StoreId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for StoreId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl fmt::Display for StoreId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
impl From<String> for StoreId {
    fn from(value: String) -> Self {
        Self(value)
    }
}
impl From<&str> for StoreId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

/// Logical kind exposed consistently by all providers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    File,
    Directory,
}

impl FileKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
        }
    }
}

/// Operations supported by a file store.
///
/// Capabilities are explicit because remote providers do not necessarily offer
/// filesystem-equivalent semantics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileCapabilities {
    pub read: bool,
    pub write: bool,
    pub mkdir: bool,
    pub move_entry: bool,
    pub copy: bool,
    pub delete: bool,
    pub range_read: bool,
}

/// Provider-neutral metadata cached in Glacier.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    pub size: Option<u64>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub created_at: Option<u64>,
    pub modified_at: Option<u64>,
}

/// One `_files` document.
///
/// `remote_id` is deliberately opaque: it may be a native blob identifier,
/// an S3 key, a Drive item ID, a `OneDrive` item ID, or another provider token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileEntry {
    pub file_id: FileId,
    pub store_id: StoreId,
    pub remote_id: String,
    pub parent_id: Option<FileId>,
    pub name: String,
    pub kind: FileKind,
    pub metadata: FileMetadata,
    pub place_id: String,
    pub app_instance_id: String,
}
