//! Native filesystem-backed file store.
//!
//! The provider owns file payloads and filesystem hierarchy. `_files` remains
//! the canonical OG metadata/index; native `remote_id` values are provider-
//! opaque relative paths and must never be interpreted outside this module.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{helpers::u128_to_u64_saturating, storage::UuidV7Generator};

use super::{
    FileCapabilities, FileKind, FileMetadata, FileRange, FileReader, FileResult, FileStore,
    FileStoreEntry, FileStoreError, FileWrite, StoreId,
};

/// Native store rooted in one private filesystem directory.
#[derive(Clone, Debug)]
pub struct NativeFileStore {
    id: StoreId,
    root: PathBuf,
}

impl NativeFileStore {
    pub fn new(id: impl Into<StoreId>, root: impl Into<PathBuf>) -> FileResult<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self {
            id: id.into(),
            root,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn relative_path(remote_id: &str) -> FileResult<PathBuf> {
        let path = Path::new(remote_id);
        if remote_id.is_empty()
            || path.is_absolute()
            || path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(FileStoreError::InvalidRemoteId);
        }
        Ok(path.to_path_buf())
    }

    fn resolve_existing(&self, remote_id: &str) -> FileResult<PathBuf> {
        self.resolve_checked(remote_id, false)
    }

    fn resolve_new(&self, remote_id: &str) -> FileResult<PathBuf> {
        self.resolve_checked(remote_id, true)
    }

    fn resolve_checked(&self, remote_id: &str, allow_missing_final: bool) -> FileResult<PathBuf> {
        let relative = Self::relative_path(remote_id)?;
        let count = relative.components().count();
        let mut path = self.root.clone();
        for (index, component) in relative.components().enumerate() {
            let Component::Normal(component) = component else {
                return Err(FileStoreError::InvalidRemoteId);
            };
            path.push(component);
            match fs::symlink_metadata(&path) {
                Ok(metadata) => {
                    if metadata.file_type().is_symlink() {
                        return Err(FileStoreError::Unsupported("symlink"));
                    }
                    if index + 1 < count && !metadata.is_dir() {
                        return Err(FileStoreError::NotDirectory);
                    }
                }
                Err(error)
                    if allow_missing_final
                        && index + 1 == count
                        && error.kind() == std::io::ErrorKind::NotFound =>
                {
                    return Ok(path);
                }
                Err(error) => return Err(map_not_found(error)),
            }
        }
        Ok(path)
    }

    fn validate_name(name: &str) -> FileResult<()> {
        let mut components = Path::new(name).components();
        if name.is_empty()
            || !matches!(components.next(), Some(Component::Normal(_)))
            || components.next().is_some()
        {
            return Err(FileStoreError::InvalidName);
        }
        Ok(())
    }

    fn target_remote_id(parent: Option<&str>, name: &str) -> FileResult<String> {
        Self::validate_name(name)?;
        let mut relative = match parent {
            Some(parent) => Self::relative_path(parent)?,
            None => PathBuf::new(),
        };
        relative.push(name);
        Ok(remote_id_string(&relative))
    }

    fn entry(&self, remote_id: &str) -> FileResult<FileStoreEntry> {
        let path = self.resolve_existing(remote_id)?;
        let metadata = fs::symlink_metadata(&path).map_err(map_not_found)?;
        if metadata.file_type().is_symlink() {
            return Err(FileStoreError::Unsupported("symlink"));
        }
        let kind = if metadata.is_dir() {
            FileKind::Directory
        } else if metadata.is_file() {
            FileKind::File
        } else {
            return Err(FileStoreError::Unsupported("special file"));
        };
        let relative = Self::relative_path(remote_id)?;
        let name = relative
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or(FileStoreError::InvalidRemoteId)?
            .to_owned();
        let parent_remote_id = relative
            .parent()
            .and_then(|parent| (!parent.as_os_str().is_empty()).then(|| remote_id_string(parent)));
        Ok(FileStoreEntry {
            remote_id: remote_id.to_owned(),
            parent_remote_id,
            name,
            kind,
            metadata: metadata_from_fs(&metadata),
        })
    }

    fn ensure_directory(&self, remote_id: Option<&str>) -> FileResult<PathBuf> {
        let path = match remote_id {
            Some(remote_id) => self.resolve_existing(remote_id)?,
            None => self.root.clone(),
        };
        let metadata = fs::symlink_metadata(&path).map_err(map_not_found)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(FileStoreError::NotDirectory);
        }
        Ok(path)
    }

    fn ensure_destination_absent(path: &Path) -> FileResult<()> {
        match fs::symlink_metadata(path) {
            Ok(_) => Err(FileStoreError::AlreadyExists),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn copy_tree(source: &Path, destination: &Path) -> FileResult<()> {
        let metadata = fs::symlink_metadata(source).map_err(map_not_found)?;
        if metadata.file_type().is_symlink() {
            return Err(FileStoreError::Unsupported("symlink"));
        }
        if metadata.is_file() {
            fs::copy(source, destination)?;
            return Ok(());
        }
        if !metadata.is_dir() {
            return Err(FileStoreError::Unsupported("special file"));
        }
        fs::create_dir(destination)?;
        for child in fs::read_dir(source)? {
            let child = child?;
            Self::copy_tree(&child.path(), &destination.join(child.file_name()))?;
        }
        Ok(())
    }
}

impl FileStore for NativeFileStore {
    fn store_id(&self) -> &StoreId {
        &self.id
    }

    fn capabilities(&self) -> FileCapabilities {
        FileCapabilities {
            read: true,
            write: true,
            mkdir: true,
            move_entry: true,
            copy: true,
            delete: true,
            range_read: true,
        }
    }

    fn stat(&self, remote_id: &str) -> FileResult<FileStoreEntry> {
        self.entry(remote_id)
    }

    fn list(&self, parent_remote_id: Option<&str>) -> FileResult<Vec<FileStoreEntry>> {
        let directory = self.ensure_directory(parent_remote_id)?;
        let parent = parent_remote_id.map(Self::relative_path).transpose()?;
        let mut entries = Vec::new();
        for child in fs::read_dir(directory)? {
            let child = child?;
            let mut relative = parent.clone().unwrap_or_default();
            relative.push(child.file_name());
            let remote_id = remote_id_string(&relative);
            entries.push(self.entry(&remote_id)?);
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }

    fn read(&self, remote_id: &str, range: Option<FileRange>) -> FileResult<FileReader> {
        let path = self.resolve_existing(remote_id)?;
        let metadata = fs::symlink_metadata(&path).map_err(map_not_found)?;
        if metadata.file_type().is_symlink() {
            return Err(FileStoreError::Unsupported("symlink"));
        }
        if !metadata.is_file() {
            return Err(FileStoreError::NotFile);
        }

        let mut file = File::open(path)?;
        let Some(range) = range else {
            return Ok(Box::new(file));
        };
        if range.offset > metadata.len() {
            return Err(FileStoreError::InvalidRange);
        }
        file.seek(SeekFrom::Start(range.offset))?;
        let available = metadata.len() - range.offset;
        let length = range.length.unwrap_or(available);
        if length > available {
            return Err(FileStoreError::InvalidRange);
        }
        Ok(Box::new(file.take(length)))
    }

    fn write(&self, target: FileWrite<'_>, source: &mut dyn Read) -> FileResult<FileStoreEntry> {
        let remote_id = match target.remote_id {
            Some(remote_id) => remote_id.to_owned(),
            None => Self::target_remote_id(target.parent_remote_id, target.name)?,
        };
        let destination = if target.remote_id.is_some() {
            self.resolve_existing(&remote_id)?
        } else {
            self.resolve_new(&remote_id)?
        };

        if target.remote_id.is_none() {
            let parent = destination
                .parent()
                .ok_or(FileStoreError::InvalidRemoteId)?;
            let parent_metadata = fs::symlink_metadata(parent).map_err(map_not_found)?;
            if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
                return Err(FileStoreError::NotDirectory);
            }
            Self::ensure_destination_absent(&destination)?;
        } else if let Ok(metadata) = fs::symlink_metadata(&destination) {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(FileStoreError::NotFile);
            }
        }

        let parent = destination
            .parent()
            .ok_or(FileStoreError::InvalidRemoteId)?;
        let temporary = parent.join(format!(".og-upload-{}", UuidV7Generator::new().next_id()));
        let result = (|| -> FileResult<()> {
            let mut output = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            let written = std::io::copy(source, &mut output)?;
            if let Some(expected) = target.size {
                if written != expected {
                    return Err(FileStoreError::SizeMismatch {
                        expected,
                        actual: written,
                    });
                }
            }
            output.flush()?;
            output.sync_all()?;
            fs::rename(&temporary, &destination)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result?;
        self.entry(&remote_id)
    }

    fn mkdir(&self, parent_remote_id: Option<&str>, name: &str) -> FileResult<FileStoreEntry> {
        self.ensure_directory(parent_remote_id)?;
        let remote_id = Self::target_remote_id(parent_remote_id, name)?;
        let path = self.resolve_new(&remote_id)?;
        Self::ensure_destination_absent(&path)?;
        fs::create_dir(&path)?;
        self.entry(&remote_id)
    }

    fn move_entry(
        &self,
        remote_id: &str,
        new_parent_remote_id: Option<&str>,
        new_name: &str,
    ) -> FileResult<FileStoreEntry> {
        let source = self.resolve_existing(remote_id)?;
        fs::symlink_metadata(&source).map_err(map_not_found)?;
        self.ensure_directory(new_parent_remote_id)?;
        let new_remote_id = Self::target_remote_id(new_parent_remote_id, new_name)?;
        let destination = self.resolve_new(&new_remote_id)?;
        Self::ensure_destination_absent(&destination)?;
        fs::rename(source, destination)?;
        self.entry(&new_remote_id)
    }

    fn copy(
        &self,
        remote_id: &str,
        new_parent_remote_id: Option<&str>,
        new_name: &str,
    ) -> FileResult<FileStoreEntry> {
        let source = self.resolve_existing(remote_id)?;
        fs::symlink_metadata(&source).map_err(map_not_found)?;
        self.ensure_directory(new_parent_remote_id)?;
        let new_remote_id = Self::target_remote_id(new_parent_remote_id, new_name)?;
        let destination = self.resolve_new(&new_remote_id)?;
        Self::ensure_destination_absent(&destination)?;
        if let Err(error) = Self::copy_tree(&source, &destination) {
            let _ = if destination.is_dir() {
                fs::remove_dir_all(&destination)
            } else {
                fs::remove_file(&destination)
            };
            return Err(error);
        }
        self.entry(&new_remote_id)
    }

    fn delete(&self, remote_id: &str) -> FileResult<()> {
        let path = self.resolve_existing(remote_id)?;
        let metadata = fs::symlink_metadata(&path).map_err(map_not_found)?;
        if metadata.file_type().is_symlink() {
            return Err(FileStoreError::Unsupported("symlink"));
        }
        if metadata.is_dir() {
            fs::remove_dir_all(path)?;
        } else if metadata.is_file() {
            fs::remove_file(path)?;
        } else {
            return Err(FileStoreError::Unsupported("special file"));
        }
        Ok(())
    }
}

fn remote_id_string(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn metadata_from_fs(metadata: &fs::Metadata) -> FileMetadata {
    FileMetadata {
        size: metadata.is_file().then_some(metadata.len()),
        content_type: None,
        etag: Some(format!(
            "{:x}-{:x}",
            metadata.len(),
            system_time_nanos(metadata.modified().ok())
        )),
        created_at: system_time_millis(metadata.created().ok()),
        modified_at: system_time_millis(metadata.modified().ok()),
    }
}

fn system_time_millis(value: Option<SystemTime>) -> Option<u64> {
    value?
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| u128_to_u64_saturating(duration.as_millis()))
}

fn system_time_nanos(value: Option<SystemTime>) -> u64 {
    value
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| u128_to_u64_saturating(duration.as_nanos()))
        .unwrap_or_default()
}

fn map_not_found(error: std::io::Error) -> FileStoreError {
    if error.kind() == std::io::ErrorKind::NotFound {
        FileStoreError::NotFound
    } else {
        error.into()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    fn test_store() -> (NativeFileStore, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "og-native-files-{}",
            UuidV7Generator::new().next_id()
        ));
        (
            NativeFileStore::new("native-test", &root).expect("create native store"),
            root,
        )
    }

    #[test]
    fn native_store_supports_streaming_range_and_mutations() {
        let (store, root) = test_store();
        let directory = store.mkdir(None, "docs").expect("mkdir");
        let mut source: &[u8] = b"openglacier";
        let written = store
            .write(
                FileWrite {
                    remote_id: None,
                    parent_remote_id: Some(&directory.remote_id),
                    name: "hello.txt",
                    content_type: Some("text/plain"),
                    size: Some(11),
                },
                &mut source,
            )
            .expect("write");
        assert_eq!(written.metadata.size, Some(11));

        let mut range = store
            .read(&written.remote_id, Some(FileRange::new(4, Some(4))))
            .expect("range read");
        let mut bytes = Vec::new();
        range.read_to_end(&mut bytes).expect("read range");
        assert_eq!(bytes, b"glac");

        let copied = store
            .copy(&written.remote_id, Some(&directory.remote_id), "copy.txt")
            .expect("copy");
        let moved = store
            .move_entry(&copied.remote_id, None, "moved.txt")
            .expect("move");
        assert_eq!(moved.remote_id, "moved.txt");
        assert_eq!(store.list(None).expect("list root").len(), 2);

        store
            .delete(&directory.remote_id)
            .expect("delete directory");
        store.delete(&moved.remote_id).expect("delete moved");
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn traversal_and_invalid_ranges_are_rejected() {
        let (store, root) = test_store();
        let mut source: &[u8] = b"abc";
        let file = store
            .write(
                FileWrite {
                    remote_id: None,
                    parent_remote_id: None,
                    name: "a.txt",
                    content_type: None,
                    size: None,
                },
                &mut source,
            )
            .expect("write");
        assert!(matches!(
            store.stat("../outside"),
            Err(FileStoreError::InvalidRemoteId)
        ));
        assert!(matches!(
            store.read(&file.remote_id, Some(FileRange::new(4, None))),
            Err(FileStoreError::InvalidRange)
        ));
        store.delete(&file.remote_id).expect("delete");
        fs::remove_dir_all(root).expect("cleanup");
    }
}
