//! Local filesystem synchronization model.
//!
//! Sync is a device-local materialization of existing app-scoped Files trees.
//! It does not change the logical `_files` scope: each tree still belongs to
//! one Place and one App instance. The user-facing projection may flatten the
//! primary Files service at the Place root while keeping other app-owned trees
//! below [`APP_FILES_DIRECTORY`].

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
/// Current on-disk sync configuration/index schema.
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
    /// Safety latch used after changing the local materialization root. While
    /// true, the next reconciliation is remote -> local only, so an empty or
    /// partially moved destination can never be interpreted as mass deletion.
    #[serde(default)]
    pub local_baseline_required: bool,
}

impl FileSyncConfig {
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self {
            version: FILE_SYNC_CONFIG_VERSION,
            root,
            selections: Vec::new(),
            local_baseline_required: false,
        }
    }

    #[must_use]
    pub fn selection(&self, place_id: &str, app_instance_id: &str) -> Option<&FileSyncSelection> {
        self.selections.iter().find(|selection| {
            selection.place_id == place_id && selection.app_instance_id == app_instance_id
        })
    }

    /// Replaces one app-instance selection. Folder IDs are canonicalized so the
    /// persisted state remains stable across UI ordering differences.
    pub fn set_selection(
        &mut self,
        place_id: impl Into<String>,
        app_instance_id: impl Into<String>,
        mode: FileSyncSelectionMode,
        mut folder_ids: Vec<String>,
    ) {
        let place_id = place_id.into();
        let app_instance_id = app_instance_id.into();
        folder_ids.retain(|value| !value.is_empty());
        folder_ids.sort();
        folder_ids.dedup();
        if mode == FileSyncSelectionMode::All {
            folder_ids.clear();
        }

        self.selections.retain(|selection| {
            !(selection.place_id == place_id && selection.app_instance_id == app_instance_id)
        });
        self.selections.push(FileSyncSelection {
            place_id,
            app_instance_id,
            mode,
            folder_ids,
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
                || (selection.mode == FileSyncSelectionMode::Selected
                    && selection.folder_ids.is_empty())
                || selection.folder_ids.iter().any(String::is_empty)
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Files sync selections contain an invalid scope or folder identifier",
            ));
        }
        Ok(())
    }

    pub fn load(path: &Path) -> io::Result<Option<Self>> {
        let config = load_json::<Self>(path)?;
        if let Some(config) = config.as_ref() {
            config.validate()?;
        }
        Ok(config)
    }

    /// Persists through a sibling temporary file and rename so `ogd` never
    /// observes a partially-written local sync configuration.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        self.validate()?;
        save_json(path, self)
    }
}

/// Whether an app-scoped Files tree is fully materialized or limited to
/// explicitly selected logical folders.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileSyncSelectionMode {
    All,
    Selected,
}

/// Selection of logical folders to materialize for one existing Files scope.
/// IDs are used instead of paths so renames do not invalidate selection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSyncSelection {
    pub place_id: String,
    pub app_instance_id: String,
    pub mode: FileSyncSelectionMode,
    #[serde(default, alias = "fileIds")]
    pub folder_ids: Vec<String>,
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
/// materialized path is relative to the configured sync root and is only the current local projection and may change after
/// a Place/App/Instance rename without changing file identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileSyncIndexEntry {
    pub place_id: String,
    pub app_instance_id: String,
    pub file_id: String,
    pub materialized_path: PathBuf,
    #[serde(default)]
    pub kind: Option<String>,
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

impl FileSyncIndex {
    pub fn load(path: &Path) -> io::Result<Option<Self>> {
        let index = load_json::<Self>(path)?;
        if let Some(index) = index.as_ref() {
            if index.version != FILE_SYNC_CONFIG_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported Files sync index version {}", index.version),
                ));
            }
        }
        Ok(index)
    }

    pub fn save(&self, path: &Path) -> io::Result<()> {
        if self.version != FILE_SYNC_CONFIG_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported Files sync index version {}", self.version),
            ));
        }
        save_json(path, self)
    }
}

/// Returns a portable, deterministic filesystem component for a user-facing
/// OpenGlacier label. The same sanitization is used on every platform so a
/// Place/App/Instance keeps the same visible projection across devices.
#[must_use]
pub fn file_sync_projection_component(label: &str, fallback: &str) -> String {
    let mut value = String::with_capacity(label.len());
    for character in label.trim().chars() {
        let invalid = character.is_control()
            || matches!(
                character,
                '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*'
            );
        value.push(if invalid { '_' } else { character });
    }
    while value.ends_with(' ') || value.ends_with('.') {
        value.pop();
    }
    if value.is_empty() || matches!(value.as_str(), "." | "..") {
        value = fallback.to_owned();
    }

    let stem = value.split('.').next().unwrap_or_default();
    let upper = stem.to_ascii_uppercase();
    let windows_reserved = matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || upper.strip_prefix("COM").is_some_and(|suffix| {
            matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
        })
        || upper.strip_prefix("LPT").is_some_and(|suffix| {
            matches!(suffix, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9")
        });
    if windows_reserved {
        value.push('_');
    }
    value
}

/// Stable short suffix used only when two logical objects would otherwise
/// materialize to the same user-facing filesystem path.
#[must_use]
pub fn file_sync_projection_suffix(stable_id: &str) -> String {
    let compact: String = stable_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .take(8)
        .collect();
    if compact.is_empty() {
        "item".to_owned()
    } else {
        compact
    }
}

fn load_json<T>(path: &Path) -> io::Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    serde_json::from_slice::<T>(&bytes)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn save_json<T>(path: &Path, value: &T) -> io::Result<()>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("tmp");
    let bytes = serde_json::to_vec_pretty(value)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selections_are_stable_and_id_based() {
        let mut config = FileSyncConfig::new(PathBuf::from("OpenGlacier"));
        config.set_selection(
            "place-1",
            "files-main",
            FileSyncSelectionMode::Selected,
            vec!["folder-b".into(), "folder-a".into(), "folder-a".into()],
        );
        let selection = config
            .selection("place-1", "files-main")
            .expect("selection");
        assert_eq!(selection.mode, FileSyncSelectionMode::Selected);
        assert_eq!(selection.folder_ids, ["folder-a", "folder-b"]);
    }

    #[test]
    fn all_selection_does_not_persist_redundant_folder_ids() {
        let mut config = FileSyncConfig::new(PathBuf::from("OpenGlacier"));
        config.set_selection(
            "place-1",
            "files-main",
            FileSyncSelectionMode::All,
            vec!["folder-a".into()],
        );
        let selection = config
            .selection("place-1", "files-main")
            .expect("selection");
        assert_eq!(selection.mode, FileSyncSelectionMode::All);
        assert!(selection.folder_ids.is_empty());
    }

    #[test]
    fn reserved_projection_names_are_explicit() {
        assert_eq!(APP_FILES_DIRECTORY, "Apps");
        assert_eq!(PRIMARY_APPS_COLLISION_NAME, "Apps (Files)");
    }

    #[test]
    fn projection_components_are_portable() {
        assert_eq!(
            file_sync_projection_component(" Maison ", "Place"),
            "Maison"
        );
        assert_eq!(file_sync_projection_component("A/B:C*", "Place"), "A_B_C_");
        assert_eq!(file_sync_projection_component("CON", "Place"), "CON_");
        assert_eq!(file_sync_projection_component("..", "Place"), "Place");
    }
}
