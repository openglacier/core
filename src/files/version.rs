//! Persistent file-version metadata stored in `_file_versions`.

use super::{FileId, FileMetadata, StoreId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileVersion {
    pub version_id: String,
    pub file_id: FileId,
    pub store_id: StoreId,
    pub remote_id: String,
    pub name: String,
    pub metadata: FileMetadata,
    pub place_id: String,
    pub app_instance_id: String,
    pub created_at: u64,
}
