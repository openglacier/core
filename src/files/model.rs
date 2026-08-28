//! App-scoped file metadata persisted in `_files`.

use std::{error::Error as StdError, fmt};

use serde::{Deserialize, Serialize};

use crate::{
    helpers::{APP_INSTANCE_SCOPE_FIELD, PLACE_SCOPE_FIELD},
    Document, Number, Value,
};

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
/// an S3 key, a Drive item ID, a OneDrive item ID, or another provider token.
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

impl FileEntry {
    /// Converts the entry to the canonical `_files` document schema.
    #[must_use]
    pub fn to_document(&self) -> Document {
        let mut document = Document::new();
        document.insert("fileId", self.file_id.as_str());
        document.insert("storeId", self.store_id.as_str());
        document.insert("remoteId", self.remote_id.as_str());
        document.insert("name", self.name.as_str());
        document.insert("kind", self.kind.as_str());
        document.insert(PLACE_SCOPE_FIELD, self.place_id.as_str());
        document.insert(APP_INSTANCE_SCOPE_FIELD, self.app_instance_id.as_str());
        match &self.parent_id {
            Some(parent_id) => document.insert("parentId", parent_id.as_str()),
            None => document.insert("parentId", Value::Null),
        };
        if let Some(size) = self.metadata.size {
            document.insert("size", size);
        }
        if let Some(value) = &self.metadata.content_type {
            document.insert("contentType", value.as_str());
        }
        if let Some(value) = &self.metadata.etag {
            document.insert("etag", value.as_str());
        }
        if let Some(value) = self.metadata.created_at {
            document.insert("createdAt", value);
        }
        if let Some(value) = self.metadata.modified_at {
            document.insert("modifiedAt", value);
        }
        document
    }

    /// Decodes one canonical `_files` document.
    pub fn from_document(document: &Document) -> Result<Self, FileModelError> {
        Ok(Self {
            file_id: FileId::new(required_string(document, "fileId")?),
            store_id: StoreId::new(required_string(document, "storeId")?),
            remote_id: required_string(document, "remoteId")?,
            parent_id: optional_string(document, "parentId")?.map(FileId::new),
            name: required_string(document, "name")?,
            kind: match required_string(document, "kind")?.as_str() {
                "file" => FileKind::File,
                "directory" => FileKind::Directory,
                value => return Err(FileModelError::InvalidKind(value.to_owned())),
            },
            metadata: FileMetadata {
                size: optional_u64(document, "size")?,
                content_type: optional_string(document, "contentType")?,
                etag: optional_string(document, "etag")?,
                created_at: optional_u64(document, "createdAt")?,
                modified_at: optional_u64(document, "modifiedAt")?,
            },
            place_id: required_string(document, PLACE_SCOPE_FIELD)?,
            app_instance_id: required_string(document, APP_INSTANCE_SCOPE_FIELD)?,
        })
    }
}

/// Invalid persisted File App metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileModelError {
    MissingField(&'static str),
    InvalidField(&'static str),
    InvalidKind(String),
}

impl fmt::Display for FileModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField(field) => write!(f, "missing file metadata field {field}"),
            Self::InvalidField(field) => write!(f, "invalid file metadata field {field}"),
            Self::InvalidKind(kind) => write!(f, "invalid file kind {kind:?}"),
        }
    }
}
impl StdError for FileModelError {}

fn required_string(document: &Document, field: &'static str) -> Result<String, FileModelError> {
    optional_string(document, field)?.ok_or(FileModelError::MissingField(field))
}

fn optional_string(
    document: &Document,
    field: &'static str,
) -> Result<Option<String>, FileModelError> {
    match document.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.to_string())),
        Some(_) => Err(FileModelError::InvalidField(field)),
    }
}

fn optional_u64(document: &Document, field: &'static str) -> Result<Option<u64>, FileModelError> {
    match document.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(Number::Unsigned(value))) => Ok(Some(*value)),
        Some(Value::Number(Number::Signed(value))) if *value >= 0 => Ok(u64::try_from(*value).ok()),
        Some(_) => Err(FileModelError::InvalidField(field)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_entry_round_trips_through_app_scoped_document() {
        let entry = FileEntry {
            file_id: FileId::from("file-1"),
            store_id: StoreId::from("native-main"),
            remote_id: "opaque/provider/id".to_owned(),
            parent_id: Some(FileId::from("parent-1")),
            name: "report.pdf".to_owned(),
            kind: FileKind::File,
            metadata: FileMetadata {
                size: Some(42),
                content_type: Some("application/pdf".to_owned()),
                etag: Some("etag-1".to_owned()),
                created_at: Some(10),
                modified_at: Some(20),
            },
            place_id: "place-a".to_owned(),
            app_instance_id: "files-main".to_owned(),
        };

        let document = entry.to_document();
        assert_eq!(document.get("_place"), Some(&Value::from("place-a")));
        assert_eq!(
            document.get("_app_instance"),
            Some(&Value::from("files-main"))
        );
        assert_eq!(FileEntry::from_document(&document), Ok(entry));
    }
}
