//! Local filesystem synchronization model.
//!
//! Sync is a local materialization of existing app-scoped Files trees. It does
//! not change the logical `_files` scope: each tree still belongs to one Place
//! and one App instance. The user-facing projection may flatten the primary
//! Files service at the Place root while keeping other app-owned trees below
//! [`APP_FILES_DIRECTORY`].

use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

/// Reserved user-facing namespace for files owned by non-primary Apps.
pub const APP_FILES_DIRECTORY: &str = "Apps";
/// Name used when the primary Files tree itself contains a root entry named
/// [`APP_FILES_DIRECTORY`]. This is a projection detail only; the logical name
/// remains unchanged in `_files`.
pub const PRIMARY_APPS_COLLISION_NAME: &str = "Apps (Files)";
/// Current on-disk sync configuration schema.
pub const FILE_SYNC_CONFIG_VERSION: u32 = 1;

/// Device-local Files synchronization configuration.
///
/// This state is deliberately local: the Place owns resource participation,
/// while the node owns where and which parts are materialized on its filesystem.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSyncConfig {
    pub version: u32,
    pub root: PathBuf,
    #[serde(default)]
    pub selections: Vec<FileSyncSelection>,
}

impl FileSyncConfig {
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self {
            version: FILE_SYNC_CONFIG_VERSION,
            root,
            selections: Vec::new(),
        }
    }

    #[must_use]
    pub fn selection(&self, place_id: &str, app_instance_id: &str) -> Option<&FileSyncSelection> {
        self.selections.iter().find(|selection| {
            selection.place_id == place_id && selection.app_instance_id == app_instance_id
        })
    }

    /// Replaces one app-instance selection. File IDs are canonicalized so the
    /// persisted state remains stable across UI ordering differences.
    pub fn set_selection(
        &mut self,
        place_id: impl Into<String>,
        app_instance_id: impl Into<String>,
        mut file_ids: Vec<String>,
    ) {
        let place_id = place_id.into();
        let app_instance_id = app_instance_id.into();
        file_ids.retain(|value| !value.is_empty());
        file_ids.sort();
        file_ids.dedup();
        self.selections.retain(|selection| {
            !(selection.place_id == place_id && selection.app_instance_id == app_instance_id)
        });
        self.selections.push(FileSyncSelection {
            place_id,
            app_instance_id,
            file_ids,
        });
        self.selections.sort_by(|left, right| {
            (&left.place_id, &left.app_instance_id).cmp(&(&right.place_id, &right.app_instance_id))
        });
    }

    pub fn remove_selection(&mut self, place_id: &str, app_instance_id: &str) {
        self.selections.retain(|selection| {
            selection.place_id != place_id || selection.app_instance_id != app_instance_id
        });
    }

    pub fn validate(&self) -> io::Result<()> {
        if self.version != FILE_SYNC_CONFIG_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported Files sync config version {}", self.version),
            ));
        }
        if self.root.as_os_str().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Files sync root must not be empty",
            ));
        }
        if self.selections.iter().any(|selection| {
            selection.place_id.is_empty()
                || selection.app_instance_id.is_empty()
                || selection.file_ids.iter().any(String::is_empty)
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Files sync selections contain an empty identifier",
            ));
        }
        Ok(())
    }

    pub fn load(path: &Path) -> io::Result<Option<Self>> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let config = serde_json::from_slice::<Self>(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        config.validate()?;
        Ok(Some(config))
    }

    /// Persists through a sibling temporary file and rename so `ogd` never
    /// observes a partially-written local sync configuration.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        self.validate()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        output.write_all(&bytes)?;
        output.flush()?;
        output.sync_all()?;
        fs::rename(temporary, path)
    }
}

/// Reconciliation state for one materialized logical file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileSyncEntryState {
    Synced,
    PendingUpload,
    PendingDownload,
    Conflict,
    Ignored,
    Error,
}

/// Device-local reconciliation record. The logical IDs remain canonical; the
/// materialized path is only the current local projection and may change after
/// a Place/App/Instance rename without changing file identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSyncIndexEntry {
    pub place_id: String,
    pub app_instance_id: String,
    pub file_id: String,
    pub materialized_path: PathBuf,
    #[serde(default)]
    pub remote_etag: Option<String>,
    #[serde(default)]
    pub remote_modified_at: Option<u64>,
    #[serde(default)]
    pub local_size: Option<u64>,
    #[serde(default)]
    pub local_modified_at: Option<u64>,
    #[serde(default)]
    pub local_fingerprint: Option<String>,
    pub state: FileSyncEntryState,
    #[serde(default)]
    pub last_error: Option<String>,
}

/// Persisted local reconciliation index. This is intentionally separate from
/// the user-facing sync tree and must live in ogd's private data directory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSyncIndex {
    pub version: u32,
    #[serde(default)]
    pub entries: Vec<FileSyncIndexEntry>,
}

impl Default for FileSyncIndex {
    fn default() -> Self {
        Self {
            version: FILE_SYNC_CONFIG_VERSION,
            entries: Vec::new(),
        }
    }
}

/// Selection of logical folders/files to materialize for one existing Files
/// scope. IDs are used instead of paths so renames do not invalidate selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSyncSelection {
    pub place_id: String,
    pub app_instance_id: String,
    #[serde(default)]
    pub file_ids: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selections_are_stable_and_id_based() {
        let mut config = FileSyncConfig::new(PathBuf::from("OpenGlacier"));
        config.set_selection(
            "place-1",
            "files-main",
            vec!["folder-b".into(), "folder-a".into(), "folder-a".into()],
        );
        let selection = config
            .selection("place-1", "files-main")
            .expect("selection");
        assert_eq!(selection.file_ids, ["folder-a", "folder-b"]);
    }

    #[test]
    fn reserved_projection_names_are_explicit() {
        assert_eq!(APP_FILES_DIRECTORY, "Apps");
        assert_eq!(PRIMARY_APPS_COLLISION_NAME, "Apps (Files)");
    }
}
