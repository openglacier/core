//! Native Glacier page/record-backed physical backend.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet}, fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write}, path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock, RwLock, Weak,
    },
    thread, time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{
    de::{DeserializeSeed, SeqAccess, Visitor}, Deserialize, Serialize,
};

use super::{glacier_mmap::GlacierReadOnlyMap, StorageBackend};
use crate::helpers::{elapsed_micros, elapsed_nanos, u64_to_usize_saturating, usize_to_u64_saturating};
use crate::model::{Document, Number, Value};
use crate::storage::{
    project_document, CollectionId, CommitResult, DeleteResult, DocumentId, DocumentVersion,
    FieldPath, InsertResult, ProjectedValueRef, ReplaceResult, ScanDirection, ScanOptions,
    StorageError, StorageMutation, StorageRead, StorageReadCapability, StorageResult,
    StorageSupport, StorageTransaction, StoredDocument, VersionPrecondition,
};
use crate::{
    capabilities_of, Capability, MemoryClass, MemoryGovernor, MemoryReclaimer, MemoryReservation,
};

pub const GLACIER_FORMAT_VERSION: u16 = 5;
pub const GLACIER_PAGE_SIZE: u32 = 16 * 1024;
pub const GLACIER_SUPERBLOCK_BYTES: usize = 64;

const MAGIC: [u8; 8] = *b"OGGLACR\0";
const ENDIAN_MARKER: u32 = 0x0102_0304;
const HEADER_BYTES: u16 = GLACIER_SUPERBLOCK_BYTES as u16;
const CHECKSUM_OFFSET: usize = 56;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GlacierFormatInfo {
    version: u16,
    page_size: u32,
    created_at_ms: u64,
    store_id: [u8; 16],
}
impl GlacierFormatInfo {
    #[must_use]
    pub const fn version(self) -> u16 { self.version }
    #[must_use]
    pub const fn page_size(self) -> u32 { self.page_size }
    #[must_use]
    pub const fn created_at_ms(self) -> u64 { self.created_at_ms }
    #[must_use]
    pub const fn store_id(self) -> [u8; 16] { self.store_id }
}

/// Persisted field metadata for one Glacier collection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlacierCollectionMetadata {
    name: String,
    documents: u64,
    fields: Vec<GlacierFieldMetadata>,
}

impl GlacierCollectionMetadata {
    #[must_use]
    pub fn name(&self) -> &str { &self.name }
    #[must_use]
    pub const fn documents(&self) -> u64 { self.documents }
    #[must_use]
    pub fn fields(&self) -> &[GlacierFieldMetadata] { &self.fields }
}

/// Aggregated physical observations for one field path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlacierFieldMetadata {
    path: String,
    present: u64,
    nulls: u64,
    kinds: Vec<String>,
    capabilities: Vec<String>,
}

impl GlacierFieldMetadata {
    #[must_use]
    pub fn path(&self) -> &str { &self.path }
    #[must_use]
    pub const fn present(&self) -> u64 { self.present }
    #[must_use]
    pub const fn nulls(&self) -> u64 { self.nulls }
    #[must_use]
    pub fn physical_kinds(&self) -> &[String] { &self.kinds }
    #[must_use]
    pub fn capabilities(&self) -> &[String] { &self.capabilities }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FieldCatalog {
    collections: BTreeMap<String, CollectionFieldStats>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CollectionFieldStats {
    documents: u64,
    fields: BTreeMap<String, FieldStats>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FieldStats {
    present: u64,
    nulls: u64,
    kinds: BTreeMap<String, u64>,
    capabilities: BTreeMap<String, u64>,
}

const SEGMENT_MAGIC: [u8; 8] = *b"OGSEG005";
const SEGMENT_HEADER_BYTES: usize = 48;
const MAX_SEGMENT_DIRECTORY_BYTES: usize = 128 * 1024 * 1024;
const MAX_SEGMENT_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_SEGMENT_BYTES: usize = 512 * 1024 * 1024;
const MAX_DATA_RECORD_BYTES: usize = 64 * 1024 * 1024;
const PHYSICAL_SET_MAGIC: [u8; 8] = *b"OGDOC001";
const PHYSICAL_SET_VERSION: u16 = 1;
const PHYSICAL_SET_HEADER_BYTES: usize = 64;
const PHYSICAL_FIELD_FIXED_BYTES: usize = 12;
const CHECKPOINT_MAGIC: [u8; 8] = *b"OGCKP001";
const CHECKPOINT_VERSION: u16 = 1;
const CHECKPOINT_HEADER_BYTES: usize = 32;
const MAX_CHECKPOINT_PAYLOAD_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const CHECKPOINT_WRITE_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const CHECKPOINT_READ_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const CHECKPOINT_FAILURE_BACKOFF_MULTIPLIER: u64 = 2;
const MIN_CHECKPOINT_INTERVAL_BYTES: u64 = 128 * 1024 * 1024;
const PRIMARY_INDEX_MAGIC: [u8; 8] = *b"OGPIDX01";
const PRIMARY_INDEX_HEADER_BYTES: u64 = 32;
const PRIMARY_INDEX_ENTRY_BYTES: u64 = 48;
const PRIMARY_CACHE_FRACTION_DENOMINATOR: usize = 4;

#[derive(Clone, Copy, Debug)]
struct RecordPointer { offset: u64, length: u32, }

#[derive(Clone, Copy, Debug)]
struct IndexVersion { generation: u64, version: DocumentVersion, pointer: Option<RecordPointer>, }

#[derive(Clone, Debug)]
enum InlineIndexVersions { One(IndexVersion), Many(Vec<IndexVersion>), }

impl InlineIndexVersions {
    fn new(version: IndexVersion) -> Self { Self::One(version) }

    fn push(&mut self, version: IndexVersion) {
        match self {
            Self::One(current) => {
                *self = Self::Many(vec![*current, version]);
            }
            Self::Many(versions) => versions.push(version),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Many(versions) => versions.len(),
        }
    }

    fn heap_capacity(&self) -> usize {
        match self {
            Self::One(_) => 0,
            Self::Many(versions) => versions.capacity(),
        }
    }

    fn iter_rev(&self) -> InlineIndexVersionsRev<'_> {
        match self {
            Self::One(version) => InlineIndexVersionsRev::One(Some(version)),
            Self::Many(versions) => InlineIndexVersionsRev::Many(versions.iter().rev()),
        }
    }

    fn is_single_visible_set(&self, generation: u64) -> bool {
        matches!(
            self,
            Self::One(version)
                if version.generation <= generation && version.pointer.is_some()
        )
    }
}

enum InlineIndexVersionsRev<'a> {
    One(Option<&'a IndexVersion>),
    Many(std::iter::Rev<std::slice::Iter<'a, IndexVersion>>),
}

impl<'a> Iterator for InlineIndexVersionsRev<'a> {
    type Item = &'a IndexVersion;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(version) => version.take(),
            Self::Many(iter) => iter.next(),
        }
    }
}

const PRIMARY_HEAD_CAPACITY: usize = BOUNDED_LIMIT_SELECTION_MAX;

#[derive(Clone, Copy, Debug)]
struct PrimaryHeadEntry {
    id: DocumentId,
    pointer: RecordPointer,
}

impl PartialEq for PrimaryHeadEntry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for PrimaryHeadEntry {}

impl PartialOrd for PrimaryHeadEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrimaryHeadEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Clone, Copy, Debug)]
struct CompactPrimaryEntry {
    id: DocumentId,
    generation: u64,
    version: DocumentVersion,
    pointer: RecordPointer,
}

impl CompactPrimaryEntry {
    fn from_index_version(id: DocumentId, version: IndexVersion) -> Option<Self> {
        Some(Self {
            id,
            generation: version.generation,
            version: version.version,
            pointer: version.pointer?,
        })
    }

    fn index_version(self) -> IndexVersion {
        IndexVersion {
            generation: self.generation,
            version: self.version,
            pointer: Some(self.pointer),
        }
    }
}

#[derive(Clone, Debug)]
struct DiskPrimaryIndex {
    path: PathBuf,
    count: u64,
    last_id: Option<DocumentId>,
}

fn primary_index_path(path: &Path, collection: &CollectionId) -> PathBuf {
    let hash = checksum64(collection.as_str().as_bytes());
    PathBuf::from(format!("{}.primary.{hash:016x}", path.display()))
}

fn encode_compact_primary_entry( entry: CompactPrimaryEntry, ) -> [u8; PRIMARY_INDEX_ENTRY_BYTES as usize] {
    let mut bytes = [0u8; PRIMARY_INDEX_ENTRY_BYTES as usize];
    bytes[0..16].copy_from_slice(&entry.id.into_bytes());
    bytes[16..24].copy_from_slice(&entry.generation.to_be_bytes());
    bytes[24..32].copy_from_slice(&entry.version.get().to_be_bytes());
    bytes[32..40].copy_from_slice(&entry.pointer.offset.to_be_bytes());
    bytes[40..44].copy_from_slice(&entry.pointer.length.to_be_bytes());
    bytes
}

fn decode_compact_primary_entry(
    bytes: &[u8; PRIMARY_INDEX_ENTRY_BYTES as usize],
) -> CompactPrimaryEntry {
    CompactPrimaryEntry {
        id: DocumentId::from_bytes(bytes[0..16].try_into().unwrap()),
        generation: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        version: DocumentVersion::new(u64::from_be_bytes(bytes[24..32].try_into().unwrap())),
        pointer: RecordPointer {
            offset: u64::from_be_bytes(bytes[32..40].try_into().unwrap()),
            length: u32::from_be_bytes(bytes[40..44].try_into().unwrap()),
        },
    }
}

#[cfg(test)]
fn rebuild_disk_primary_documents( path: &Path, collection: &CollectionId, generation: u64, documents: &[CheckpointDocument], ) -> StorageResult<DiskPrimaryIndex> {
    let target = primary_index_path(path, collection);
    let temporary = PathBuf::from(format!("{}.tmp", target.display()));
    let raw = File::create(&temporary).map_err(io_error("create primary index", &temporary))?;
    let mut file = BufWriter::with_capacity(1024 * 1024, raw);
    let mut header = [0u8; PRIMARY_INDEX_HEADER_BYTES as usize];
    header[0..8].copy_from_slice(&PRIMARY_INDEX_MAGIC);
    header[8..16].copy_from_slice(&generation.to_be_bytes());
    header[16..24].copy_from_slice(&(documents.len() as u64).to_be_bytes());
    file.write_all(&header)
        .map_err(io_error("write primary index header", &temporary))?;
    let mut last_id = None;
    for document in documents {
        let entry = CompactPrimaryEntry {
            id: DocumentId::from_bytes(document.id),
            generation,
            version: DocumentVersion::new(document.version),
            pointer: RecordPointer {
                offset: document.offset,
                length: document.length,
            },
        };
        file.write_all(&encode_compact_primary_entry(entry))
            .map_err(io_error("write primary index entry", &temporary))?;
        last_id = Some(entry.id);
    }
    file.flush()
        .map_err(io_error("flush primary index", &temporary))?;
    drop(file);
    fs::rename(&temporary, &target).map_err(io_error("install primary index", &target))?;
    Ok(DiskPrimaryIndex {
        path: target,
        count: documents.len() as u64,
        last_id,
    })
}

fn disk_primary_get( index: &DiskPrimaryIndex, id: &DocumentId, ) -> StorageResult<Option<IndexVersion>> {
    let mut file = File::open(&index.path).map_err(io_error("open primary index", &index.path))?;
    let mut low = 0u64;
    let mut high = index.count;
    let mut bytes = [0u8; PRIMARY_INDEX_ENTRY_BYTES as usize];
    while low < high {
        let mid = low + (high - low) / 2;
        let offset = PRIMARY_INDEX_HEADER_BYTES
            .checked_add(mid.saturating_mul(PRIMARY_INDEX_ENTRY_BYTES))
            .ok_or_else(|| StorageError::backend("GlacierStorage primary index offset overflow"))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(io_error("seek primary index", &index.path))?;
        file.read_exact(&mut bytes)
            .map_err(io_error("read primary index", &index.path))?;
        let entry = decode_compact_primary_entry(&bytes);
        match entry.id.cmp(id) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => return Ok(Some(entry.index_version())),
        }
    }
    Ok(None)
}

fn disk_primary_append( index: &mut DiskPrimaryIndex, entry: CompactPrimaryEntry, ) -> StorageResult<bool> {
    if index.last_id.is_some_and(|last| entry.id <= last) {
        return Ok(false);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&index.path)
        .map_err(io_error("open primary index append", &index.path))?;
    file.seek(SeekFrom::End(0))
        .map_err(io_error("seek primary index append", &index.path))?;
    file.write_all(&encode_compact_primary_entry(entry))
        .map_err(io_error("append primary index", &index.path))?;
    index.count = index.count.saturating_add(1);
    index.last_id = Some(entry.id);
    file.seek(SeekFrom::Start(16))
        .map_err(io_error("seek primary index count", &index.path))?;
    file.write_all(&index.count.to_be_bytes())
        .map_err(io_error("update primary index count", &index.path))?;
    Ok(true)
}

fn disk_primary_append_batch( index: &mut DiskPrimaryIndex, entries: &mut Vec<CompactPrimaryEntry>, ) -> StorageResult<bool> {
    if entries.is_empty() {
        return Ok(true);
    }
    entries.sort_unstable_by_key(|entry| entry.id);
    let mut previous = index.last_id;
    for entry in entries.iter() {
        if previous.is_some_and(|last| entry.id <= last) {
            return Ok(false);
        }
        previous = Some(entry.id);
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&index.path)
        .map_err(io_error("open primary index batch append", &index.path))?;
    file.seek(SeekFrom::End(0))
        .map_err(io_error("seek primary index batch append", &index.path))?;

    let mut buffer = Vec::with_capacity(
        entries
            .len()
            .saturating_mul(PRIMARY_INDEX_ENTRY_BYTES as usize),
    );
    for entry in entries.iter().copied() {
        buffer.extend_from_slice(&encode_compact_primary_entry(entry));
    }
    file.write_all(&buffer)
        .map_err(io_error("append primary index batch", &index.path))?;

    index.count = index
        .count
        .saturating_add(usize_to_u64_saturating(entries.len()));
    index.last_id = entries.last().map(|entry| entry.id);
    file.seek(SeekFrom::Start(16))
        .map_err(io_error("seek primary index batch count", &index.path))?;
    file.write_all(&index.count.to_be_bytes())
        .map_err(io_error("update primary index batch count", &index.path))?;
    Ok(true)
}

#[derive(Clone, Debug)]
struct CollectionIndex {
    primary: Vec<CompactPrimaryEntry>,
    /// Prefix of `primary` duplicated by `disk_primary` and therefore safely
    /// revocable under governor pressure. New writes are never counted here.
    reclaimable_primary: usize,
    disk_primary: Option<DiskPrimaryIndex>,
    exceptions: HashMap<DocumentId, InlineIndexVersions>,
    count_history: Vec<(u64, u64)>,
    primary_head: BinaryHeap<PrimaryHeadEntry>,
    primary_head_valid: bool,
}

impl Default for CollectionIndex {
    fn default() -> Self {
        Self {
            primary: Vec::new(),
            reclaimable_primary: 0,
            disk_primary: None,
            exceptions: HashMap::new(),
            count_history: Vec::new(),
            primary_head: BinaryHeap::new(),
            primary_head_valid: true,
        }
    }
}

impl CollectionIndex {
    fn primary_position(&self, id: &DocumentId) -> Result<usize, usize> {
        self.primary.binary_search_by_key(id, |entry| entry.id)
    }

    fn contains_id(&self, id: &DocumentId) -> bool {
        if self.exceptions.contains_key(id) || self.primary_position(id).is_ok() {
            return true;
        }
        let Some(disk) = self.disk_primary.as_ref() else {
            return false;
        };
        if disk.last_id.is_some_and(|last| *id > last) {
            return false;
        }
        disk_primary_get(disk, id).ok().flatten().is_some()
    }

    fn visible_version(
        &self,
        state: &GlacierState,
        generation: u64,
        id: &DocumentId,
    ) -> Option<IndexVersion> {
        if let Some(versions) = self.exceptions.get(id) {
            return visible_index_version(state, generation, versions);
        }
        if let Ok(position) = self.primary_position(id) {
            let version = self.primary[position].index_version();
            return version_visible_after_clear(state, generation, version).then_some(version);
        }
        let disk = self.disk_primary.as_ref()?;
        // The compact sidecar is ordered. UUIDv7/import IDs commonly advance
        // beyond its current tail, making absence provable without any I/O.
        if disk.last_id.is_some_and(|last| *id > last) {
            return None;
        }
        let version = disk_primary_get(disk, id).ok().flatten()?;
        version_visible_after_clear(state, generation, version).then_some(version)
    }

    fn insert_new(&mut self, id: DocumentId, version: IndexVersion) {
        if let Some(versions) = self.exceptions.get_mut(&id) {
            versions.push(version);
            return;
        }
        if let (Some(disk), Some(entry)) = (
            self.disk_primary.as_mut(),
            CompactPrimaryEntry::from_index_version(id, version),
        ) {
            if disk_primary_append(disk, entry).unwrap_or(false) {
                return;
            }
        }
        match self.primary_position(&id) {
            Ok(position) => {
                let current = self.primary[position].index_version();
                let mut versions = InlineIndexVersions::new(current);
                versions.push(version);
                self.exceptions.insert(id, versions);
            }
            Err(position) if position == self.primary.len() => {
                if let Some(entry) = CompactPrimaryEntry::from_index_version(id, version) {
                    self.primary.push(entry);
                } else {
                    self.exceptions
                        .insert(id, InlineIndexVersions::new(version));
                }
            }
            Err(_) => {
                self.exceptions
                    .insert(id, InlineIndexVersions::new(version));
            }
        }
    }

    fn push_existing(&mut self, id: DocumentId, version: IndexVersion) {
        if let Some(versions) = self.exceptions.get_mut(&id) {
            versions.push(version);
            return;
        }
        if let Ok(position) = self.primary_position(&id) {
            let current = self.primary[position].index_version();
            let mut versions = InlineIndexVersions::new(current);
            versions.push(version);
            self.exceptions.insert(id, versions);
        } else if let Some(current) = self
            .disk_primary
            .as_ref()
            .and_then(|disk| disk_primary_get(disk, &id).ok().flatten())
        {
            let mut versions = InlineIndexVersions::new(current);
            versions.push(version);
            self.exceptions.insert(id, versions);
        } else {
            self.insert_new(id, version);
        }
    }

    fn logical_document_count(&self) -> usize {
        let base = self
            .disk_primary
            .as_ref()
            .map(|disk| u64_to_usize_saturating(disk.count))
            .unwrap_or(self.primary.len());
        base.saturating_add(
            self.exceptions
                .keys()
                .filter(|id| {
                    self.disk_primary
                        .as_ref()
                        .and_then(|disk| disk_primary_get(disk, id).ok().flatten())
                        .is_none()
                })
                .count(),
        )
    }

    fn for_each_id_version(
        &self,
        state: &GlacierState,
        generation: u64,
        mut visitor: impl FnMut(DocumentId, IndexVersion),
    ) {
        if let Some(disk) = &self.disk_primary {
            if let Ok(file) = File::open(&disk.path) {
                let mut reader = BufReader::with_capacity(1024 * 1024, file);
                if reader
                    .seek(SeekFrom::Start(PRIMARY_INDEX_HEADER_BYTES))
                    .is_ok()
                {
                    let mut bytes = [0u8; PRIMARY_INDEX_ENTRY_BYTES as usize];
                    for _ in 0..disk.count {
                        if reader.read_exact(&mut bytes).is_err() {
                            break;
                        }
                        let entry = decode_compact_primary_entry(&bytes);
                        let version = if let Some(versions) = self.exceptions.get(&entry.id) {
                            visible_index_version(state, generation, versions)
                        } else {
                            let version = entry.index_version();
                            version_visible_after_clear(state, generation, version)
                                .then_some(version)
                        };
                        if let Some(version) = version {
                            visitor(entry.id, version);
                        }
                    }
                }
            }
            for (id, versions) in &self.exceptions {
                if disk_primary_get(disk, id).ok().flatten().is_some() {
                    continue;
                }
                if let Some(version) = visible_index_version(state, generation, versions) {
                    visitor(*id, version);
                }
            }
            return;
        }

        for entry in &self.primary {
            let version = if let Some(versions) = self.exceptions.get(&entry.id) {
                visible_index_version(state, generation, versions)
            } else {
                let version = entry.index_version();
                version_visible_after_clear(state, generation, version).then_some(version)
            };
            if let Some(version) = version {
                visitor(entry.id, version);
            }
        }
        for (id, versions) in &self.exceptions {
            if self.primary_position(id).is_ok() {
                continue;
            }
            if let Some(version) = visible_index_version(state, generation, versions) {
                visitor(*id, version);
            }
        }
    }
}

fn primary_head_insert(collection: &mut CollectionIndex, id: DocumentId, pointer: RecordPointer) {
    if !collection.primary_head_valid {
        return;
    }
    if collection.primary_head.len() < PRIMARY_HEAD_CAPACITY {
        collection
            .primary_head
            .push(PrimaryHeadEntry { id, pointer });
        return;
    }
    let should_replace = collection
        .primary_head
        .peek()
        .map(|largest| id < largest.id)
        .unwrap_or(true);
    if should_replace {
        collection.primary_head.pop();
        collection
            .primary_head
            .push(PrimaryHeadEntry { id, pointer });
    }
}

fn primary_head_entries( collection: &CollectionIndex, limit: usize, ) -> Option<Vec<(DocumentId, RecordPointer)>> {
    if !collection.primary_head_valid || limit > PRIMARY_HEAD_CAPACITY {
        return None;
    }
    let mut entries = collection
        .primary_head
        .clone()
        .into_sorted_vec()
        .into_iter()
        .map(|entry| (entry.id, entry.pointer))
        .collect::<Vec<_>>();
    entries.truncate(limit);
    Some(entries)
}

#[derive(Clone, Debug, Default)]
struct GlacierState {
    generation: u64,
    clear_generations: Vec<u64>,
    collections: BTreeMap<CollectionId, CollectionIndex>,
    metadata: FieldCatalog,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistentCheckpoint {
    format_version: u16,
    store_id: [u8; 16],
    generation: u64,
    data_len: u64,
    collections: Vec<CheckpointCollection>,
    metadata: FieldCatalog,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointCollection {
    name: String,
    count: u64,
    documents: Vec<CheckpointDocument>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckpointDocument {
    id: [u8; 16],
    version: u64,
    offset: u64,
    length: u32,
}

#[derive(Clone)]
pub struct GlacierBackend {
    inner: Arc<GlacierInner>,
}

macro_rules! define_atomic_metrics {
    (
        $metrics:ident => $snapshot:ident {
            $($field:ident),* $(,)?
        }
        $(prelude { $($prelude:tt)* })?
        $(extra { $($extra:ident : $value:expr),* $(,)? })?
    ) => {
        #[derive(Debug, Default)]
        struct $metrics { $( $field: AtomicU64, )* }

        #[derive(Clone, Copy, Debug, Default, Serialize)]
        pub struct $snapshot { $( pub $field: u64, )* $( $( pub $extra: u64, )* )? }

        impl $metrics {
            fn snapshot(&self) -> $snapshot {
                $( $($prelude)* )?
                $snapshot {
                    $( $field: self.$field.load(Ordering::Relaxed), )*
                    $( $( $extra: $value, )* )?
                }
            }
        }
    };
}

macro_rules! add_metrics {
    ($target:expr; $($field:ident $(=> $value:expr)?),* $(,)?) => {
        $( $target.$field.fetch_add(add_metrics!(@value $($value)?), Ordering::Relaxed); )*
    };
    (@value $value:expr) => { $value };
    (@value) => { 1 };
}

define_atomic_metrics! {
    GlacierWriteMetrics => GlacierWriteMetricsSnapshot {
        commits, mutations, wal_encode_us, wal_write_us, wal_sync_us,
        data_encode_us, data_write_us, data_sync_us, primary_index_us,
        primary_lookup_us, primary_lookup_records, metadata_us, commit_us,
        checkpoint_runs, checkpoint_failures, checkpoint_documents, checkpoint_bytes,
        checkpoint_build_us, checkpoint_encode_us, checkpoint_io_us,
        checkpoint_write_us, checkpoint_total_us, checkpoint_last_us,
        checkpoint_max_us, checkpoint_deferred
    }
    extra { checkpoint_next_offset: 0 }
}

define_atomic_metrics! {
    GlacierStartupMetrics => GlacierStartupMetricsSnapshot {
        total_us, checkpoint_loaded, checkpoint_generation, checkpoint_bytes,
        checkpoint_load_us, segments, records, directory_decode_us,
        index_rebuild_us, metadata_rebuild_us, segment_catalog_segments, segment_catalog_build_us
    }
}

define_atomic_metrics! {
    GlacierReadMetrics => GlacierReadMetricsSnapshot {
        scans, segments, records, projected_records, decoded_fields, io_us, decode_us,
        mmap_segments, mmap_bytes, mmap_us, mmap_map_creates, mmap_reuses, mmap_remaps, mmap_fallback_segments, mmap_bypass_segments,
        segment_catalog_hits, segment_catalog_refreshes, segment_catalog_rebuilds, segment_catalog_refresh_us, segment_catalog_skipped_segments,
        directory_decode_us, directory_bypass_segments, directory_fallback_segments,
        checksum_us, trusted_header_records, verified_header_records,
        projection_layout_hits, projection_layout_misses, visitor_us, record_loop_us,
        generic_scan_each_calls, generic_scan_each_rows, generic_scan_each_prepare_us,
        generic_scan_each_pointer_loop_us, pointer_loads, pointer_payload_bytes,
        pointer_open_us, pointer_seek_us, pointer_alloc_us, pointer_read_us,
        pointer_header_us, pointer_physical_decode_us, pointer_legacy_decode_us,
        pointer_build_us, pointer_total_us, pointer_physical_records, pointer_legacy_records,
        record_projection_profile_samples, record_projection_clear_ns,
        record_projection_prepare_ns, record_projection_cache_guard_ns,
        record_projection_meta_ns, record_projection_value_ns, record_projection_fallback_ns,
        projection_values, projection_null_values, projection_bool_values,
        projection_signed_values, projection_unsigned_values, projection_float_values,
        projection_string_values, projection_complex_values, projection_string_cache_hits,
        projection_string_cache_misses, projection_string_cache_replacements,
        borrowed_projected_values, borrowed_projected_strings,
        borrowed_projected_materializations, visibility_prepare_us, visibility_fast_scans,
        visibility_fallback_scans, visibility_checks, primary_head_scans,
        bounded_limit_scans, total_us
    }
    prelude { let group_probe = crate::storage::group_consumer_probe_snapshot(); }
    extra {
        group_consumer_samples: group_probe.samples,
        group_consumer_key_encode_ns: group_probe.key_encode_ns,
        group_consumer_lookup_ns: group_probe.lookup_ns,
        group_consumer_insert_ns: group_probe.insert_ns,
        group_consumer_aggregate_ns: group_probe.aggregate_ns,
        group_consumer_lookup_hits: group_probe.lookup_hits,
        group_consumer_lookup_misses: group_probe.lookup_misses,
        group_consumer_key_materializations: group_probe.key_materializations
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct GlacierResidentMemorySnapshot {
    pub collections: u64,
    pub primary_documents: u64,
    pub primary_versions: u64,
    pub primary_document_capacity: u64,
    pub primary_cache_entries: u64,
    pub disk_primary_documents: u64,
    pub disk_primary_bytes: u64,
    pub primary_exception_documents: u64,
    pub primary_version_capacity: u64,
    pub primary_head_entries: u64,
    pub count_history_entries: u64,
    pub metadata_collections: u64,
    pub metadata_fields: u64,
    pub hash_index_estimated_bytes: u64,
    pub compact_primary_estimated_bytes: u64,
    pub version_storage_estimated_bytes: u64,
    pub primary_head_estimated_bytes: u64,
    pub count_history_estimated_bytes: u64,
    pub metadata_estimated_bytes: u64,
    pub segment_catalog_entries: u64,
    pub segment_catalog_estimated_bytes: u64,
    pub state_estimated_bytes: u64,
}

fn estimate_btree_string_map_bytes<K, V>(map: &BTreeMap<K, V>) -> usize {
    map.len().saturating_mul(
        std::mem::size_of::<K>()
            .saturating_add(std::mem::size_of::<V>())
            .saturating_add(48),
    )
}

fn resident_memory_snapshot(state: &GlacierState) -> GlacierResidentMemorySnapshot {
    let mut snapshot = GlacierResidentMemorySnapshot::default();
    snapshot.collections = state.collections.len() as u64;

    let mut total = std::mem::size_of::<GlacierState>().saturating_add(
        state
            .clear_generations
            .capacity()
            .saturating_mul(std::mem::size_of::<u64>()),
    );

    for (collection_name, collection) in &state.collections {
        let logical_documents = collection.logical_document_count();
        snapshot.primary_documents = snapshot
            .primary_documents
            .saturating_add(logical_documents as u64);
        snapshot.primary_cache_entries = snapshot
            .primary_cache_entries
            .saturating_add(collection.primary.len() as u64);
        if let Some(disk) = collection.disk_primary.as_ref() {
            snapshot.disk_primary_documents =
                snapshot.disk_primary_documents.saturating_add(disk.count);
            snapshot.disk_primary_bytes = snapshot.disk_primary_bytes.saturating_add(
                PRIMARY_INDEX_HEADER_BYTES
                    .saturating_add(disk.count.saturating_mul(PRIMARY_INDEX_ENTRY_BYTES)),
            );
        }
        snapshot.primary_document_capacity = snapshot
            .primary_document_capacity
            .saturating_add(collection.primary.capacity() as u64)
            .saturating_add(collection.exceptions.capacity() as u64);
        snapshot.primary_exception_documents = snapshot
            .primary_exception_documents
            .saturating_add(collection.exceptions.len() as u64);
        snapshot.primary_head_entries = snapshot
            .primary_head_entries
            .saturating_add(collection.primary_head.len() as u64);
        snapshot.count_history_entries = snapshot
            .count_history_entries
            .saturating_add(collection.count_history.len() as u64);

        let compact_bytes = collection
            .primary
            .capacity()
            .saturating_mul(std::mem::size_of::<CompactPrimaryEntry>());
        snapshot.compact_primary_estimated_bytes = snapshot
            .compact_primary_estimated_bytes
            .saturating_add(compact_bytes as u64);
        total = total.saturating_add(compact_bytes);

        let hash_bytes = collection.exceptions.capacity().saturating_mul(
            std::mem::size_of::<DocumentId>()
                .saturating_add(std::mem::size_of::<InlineIndexVersions>())
                .saturating_add(8),
        );
        snapshot.hash_index_estimated_bytes = snapshot
            .hash_index_estimated_bytes
            .saturating_add(hash_bytes as u64);
        total = total.saturating_add(hash_bytes);

        snapshot.primary_versions = snapshot
            .primary_versions
            .saturating_add(collection.primary.len() as u64);
        let mut version_bytes = 0usize;
        let mut version_capacity = 0usize;
        for (id, versions) in &collection.exceptions {
            let overlays_primary = collection.primary_position(id).is_ok();
            let represented = versions.len().saturating_sub(usize::from(overlays_primary));
            snapshot.primary_versions =
                snapshot.primary_versions.saturating_add(represented as u64);
            let heap_capacity = versions.heap_capacity();
            version_capacity = version_capacity.saturating_add(heap_capacity);
            version_bytes = version_bytes.saturating_add(
                heap_capacity
                    .saturating_mul(std::mem::size_of::<IndexVersion>())
                    .saturating_add(usize::from(heap_capacity > 0).saturating_mul(16)),
            );
        }
        snapshot.primary_version_capacity = snapshot
            .primary_version_capacity
            .saturating_add(version_capacity as u64);
        snapshot.version_storage_estimated_bytes = snapshot
            .version_storage_estimated_bytes
            .saturating_add(version_bytes as u64);
        total = total.saturating_add(version_bytes);

        let head_bytes = collection
            .primary_head
            .capacity()
            .saturating_mul(std::mem::size_of::<PrimaryHeadEntry>());
        snapshot.primary_head_estimated_bytes = snapshot
            .primary_head_estimated_bytes
            .saturating_add(head_bytes as u64);
        total = total.saturating_add(head_bytes);

        let count_bytes = collection
            .count_history
            .capacity()
            .saturating_mul(std::mem::size_of::<(u64, u64)>());
        snapshot.count_history_estimated_bytes = snapshot
            .count_history_estimated_bytes
            .saturating_add(count_bytes as u64);
        total = total
            .saturating_add(count_bytes)
            .saturating_add(collection_name.as_str().len());
    }

    snapshot.metadata_collections = state.metadata.collections.len() as u64;
    let mut metadata_bytes = estimate_btree_string_map_bytes(&state.metadata.collections);
    for (collection_name, collection) in &state.metadata.collections {
        metadata_bytes = metadata_bytes.saturating_add(collection_name.capacity());
        snapshot.metadata_fields = snapshot
            .metadata_fields
            .saturating_add(collection.fields.len() as u64);
        metadata_bytes =
            metadata_bytes.saturating_add(estimate_btree_string_map_bytes(&collection.fields));
        for (field_name, field) in &collection.fields {
            metadata_bytes = metadata_bytes
                .saturating_add(field_name.capacity())
                .saturating_add(estimate_btree_string_map_bytes(&field.kinds))
                .saturating_add(estimate_btree_string_map_bytes(&field.capabilities))
                .saturating_add(
                    field
                        .kinds
                        .keys()
                        .map(|value| value.capacity())
                        .sum::<usize>(),
                )
                .saturating_add(
                    field
                        .capabilities
                        .keys()
                        .map(|value| value.capacity())
                        .sum::<usize>(),
                );
        }
    }
    snapshot.metadata_estimated_bytes = metadata_bytes as u64;
    snapshot.state_estimated_bytes = total.saturating_add(metadata_bytes) as u64;
    snapshot
}

fn add_segment_catalog_memory(
    snapshot: &mut GlacierResidentMemorySnapshot,
    catalog: &Mutex<Arc<SegmentCatalogSnapshot>>,
) {
    let guard = catalog
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entries = guard.segments.len();
    let bytes = std::mem::size_of::<SegmentCatalogSnapshot>()
        .saturating_add(entries.saturating_mul(std::mem::size_of::<SegmentCatalogEntry>()));
    snapshot.segment_catalog_entries = usize_to_u64_saturating(entries);
    snapshot.segment_catalog_estimated_bytes = usize_to_u64_saturating(bytes);
    snapshot.state_estimated_bytes = snapshot
        .state_estimated_bytes
        .saturating_add(usize_to_u64_saturating(bytes));
}

struct GlacierReadScanGuard<'a> {
    metrics: &'a GlacierReadMetrics,
    started: Instant,
    visitor_us: u64,
    decode_us: u64,
    segments: u64,
    records: u64,
    projected_records: u64,
    decoded_fields: u64,
    trusted_header_records: u64,
    verified_header_records: u64,
    projection_layout_hits: u64,
    projection_layout_misses: u64,
    projection_profile: PhysicalProjectionProfile,
    projection_counters: PhysicalProjectionCounters,
}

impl<'a> GlacierReadScanGuard<'a> {
    fn new(metrics: &'a GlacierReadMetrics) -> Self {
        Self {
            metrics,
            started: Instant::now(),
            visitor_us: 0,
            decode_us: 0,
            segments: 0,
            records: 0,
            projected_records: 0,
            decoded_fields: 0,
            trusted_header_records: 0,
            verified_header_records: 0,
            projection_layout_hits: 0,
            projection_layout_misses: 0,
            projection_profile: PhysicalProjectionProfile::default(),
            projection_counters: PhysicalProjectionCounters::default(),
        }
    }

    #[inline]
    fn sampled_timer(&self) -> Option<Instant> {
        (crate::debug::query_instrumentation_enabled() && self.records & 1023 == 0)
            .then(Instant::now)
    }

    #[inline]
    fn record_sampled_decode(&mut self, started: Option<Instant>) {
        if let Some(started) = started {
            self.decode_us = self
                .decode_us
                .saturating_add(elapsed_micros(started).saturating_mul(1024));
        }
    }

    #[inline]
    fn record_sampled_visitor(&mut self, started: Option<Instant>) {
        if let Some(started) = started {
            self.visitor_us = self
                .visitor_us
                .saturating_add(elapsed_micros(started).saturating_mul(1024));
        }
    }
}

impl Drop for GlacierReadScanGuard<'_> {
    fn drop(&mut self) {
        add_metrics!(self.metrics;
            scans, segments => self.segments, records => self.records,
            projected_records => self.projected_records, decoded_fields => self.decoded_fields,
            trusted_header_records => self.trusted_header_records,
            verified_header_records => self.verified_header_records,
            projection_layout_hits => self.projection_layout_hits,
            projection_layout_misses => self.projection_layout_misses,
            record_projection_profile_samples => self.projection_profile.samples,
            record_projection_clear_ns => self.projection_profile.clear_ns,
            record_projection_prepare_ns => self.projection_profile.prepare_ns,
            record_projection_cache_guard_ns => self.projection_profile.cache_guard_ns,
            record_projection_meta_ns => self.projection_profile.meta_ns,
            record_projection_value_ns => self.projection_profile.value_ns,
            record_projection_fallback_ns => self.projection_profile.fallback_ns,
            projection_values => self.projection_counters.values,
            projection_null_values => self.projection_counters.null_values,
            projection_bool_values => self.projection_counters.bool_values,
            projection_signed_values => self.projection_counters.signed_values,
            projection_unsigned_values => self.projection_counters.unsigned_values,
            projection_float_values => self.projection_counters.float_values,
            projection_string_values => self.projection_counters.string_values,
            projection_complex_values => self.projection_counters.complex_values,
            projection_string_cache_hits => self.projection_counters.string_cache_hits,
            projection_string_cache_misses => self.projection_counters.string_cache_misses,
            projection_string_cache_replacements => self.projection_counters.string_cache_replacements,
            decode_us => self.decode_us, visitor_us => self.visitor_us,
            total_us => elapsed_micros(self.started).saturating_sub(self.visitor_us)
        );
    }
}


struct GlacierPageCacheReclaimer {
    inner: Weak<GlacierInner>,
}

impl std::fmt::Debug for GlacierPageCacheReclaimer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GlacierPageCacheReclaimer")
    }
}

impl MemoryReclaimer for GlacierPageCacheReclaimer {
    fn reclaim(&self, target_bytes: usize) -> usize {
        if target_bytes == 0 {
            return 0;
        }
        let Some(inner) = self.inner.upgrade() else {
            return 0;
        };

        // `reclaimable_primary` tracks only checkpoint entries duplicated by
        // the ordered disk sidecar. Removing that prefix never touches entries
        // created by later writes and therefore cannot change write semantics.
        let entry_bytes = std::mem::size_of::<CompactPrimaryEntry>().max(1);
        let freed = {
            let mut state = inner
                .state
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut freed = 0usize;
            for collection in state.collections.values_mut() {
                let reclaimable = collection
                    .reclaimable_primary
                    .min(collection.primary.len());
                if reclaimable == 0 || collection.disk_primary.is_none() {
                    continue;
                }
                let remaining = target_bytes.saturating_sub(freed);
                let requested_entries = remaining
                    .saturating_add(entry_bytes.saturating_sub(1))
                    / entry_bytes;
                let requested_entries = requested_entries.max(1);
                let evict = reclaimable.min(requested_entries);
                collection.primary.drain(..evict);
                collection.primary.shrink_to_fit();
                collection.reclaimable_primary =
                    collection.reclaimable_primary.saturating_sub(evict);
                freed = freed.saturating_add(evict.saturating_mul(entry_bytes));
                if freed >= target_bytes {
                    break;
                }
            }
            freed
        };

        if freed == 0 {
            return 0;
        }
        let released = {
            let mut reservation = inner
                .page_cache_reservation
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            reservation
                .as_mut()
                .map(|reservation| reservation.shrink_by(freed))
                .unwrap_or(0)
        };
        if let Some(governor) = inner.memory_governor.as_ref() {
            let mut resident = inner
                .state
                .read()
                .map(|state| resident_memory_snapshot(&state))
                .unwrap_or_default();
            add_segment_catalog_memory(&mut resident, &inner.segment_catalog);
            governor.set_observed_bytes(
                MemoryClass::Indexing,
                u64_to_usize_saturating(resident.state_estimated_bytes),
            );
        }
        released
    }
}

struct GlacierInner {
    path: PathBuf,
    format: GlacierFormatInfo,
    state: RwLock<GlacierState>,
    persist_lock: Mutex<()>,
    write_metrics: GlacierWriteMetrics,
    startup_metrics: GlacierStartupMetrics,
    read_metrics: GlacierReadMetrics,
    read_mmap: Mutex<Option<(u64, Arc<GlacierReadOnlyMap>)>>,
    segment_catalog: Mutex<Arc<SegmentCatalogSnapshot>>,
    memory_governor: Option<MemoryGovernor>,
    page_cache_reservation: Mutex<Option<MemoryReservation>>,
    page_cache_reclaimer: Mutex<Option<Arc<dyn MemoryReclaimer>>>,
    checkpoint_offset: AtomicU64,
    next_checkpoint_offset: AtomicU64,
    checkpoint_scheduled: AtomicBool,
}

impl std::fmt::Debug for GlacierBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlacierBackend")
            .field("path", &self.inner.path)
            .field("format", &self.inner.format)
            .finish_non_exhaustive()
    }
}

impl GlacierBackend {
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::open_inner(path.as_ref(), None)
    }

    pub fn open_governed(path: impl AsRef<Path>, governor: MemoryGovernor) -> StorageResult<Self> {
        Self::open_inner(path.as_ref(), Some(governor))
    }

    fn open_inner(path: &Path, memory_governor: Option<MemoryGovernor>) -> StorageResult<Self> {
        let startup_started = Instant::now();
        let startup_metrics = GlacierStartupMetrics::default();
        let format = if path.exists() {
            read_superblock(path)?
        } else {
            initialize_file(path)?
        };

        let file_len = std::fs::metadata(path)
            .map_err(io_error("stat store", path))?
            .len();
        let checkpoint_started = Instant::now();
        let checkpoint = load_checkpoint(path, format, file_len, memory_governor.as_ref())?;
        startup_metrics
            .checkpoint_load_us
            .store(elapsed_micros(checkpoint_started), Ordering::Relaxed);
        let checkpoint_loaded = checkpoint.is_some();
        let (initial_state, replay_offset, page_cache_reservation) = match checkpoint {
            Some((state, data_len, checkpoint_bytes, cache_reservation)) => {
                startup_metrics
                    .checkpoint_loaded
                    .store(1, Ordering::Relaxed);
                startup_metrics
                    .checkpoint_generation
                    .store(state.generation, Ordering::Relaxed);
                startup_metrics
                    .checkpoint_bytes
                    .store(checkpoint_bytes, Ordering::Relaxed);
                (state, data_len, cache_reservation)
            }
            None => (
                GlacierState::default(),
                GLACIER_SUPERBLOCK_BYTES as u64,
                None,
            ),
        };

        let (state, mut replay_catalog) = scan_data_file(
            path,
            &startup_metrics,
            initial_state,
            replay_offset,
            memory_governor.as_ref(),
        )?;
        let catalog_started = Instant::now();
        let catalog_file_len = std::fs::metadata(path)
            .map_err(io_error("stat segment catalog", path))?
            .len();
        let mut catalog_entries = if replay_offset > GLACIER_SUPERBLOCK_BYTES as u64 {
            read_segment_catalog_headers(path, GLACIER_SUPERBLOCK_BYTES as u64, replay_offset, 1)?
        } else {
            Vec::new()
        };
        catalog_entries.append(&mut replay_catalog);
        if catalog_entries
            .last()
            .map(|entry| entry.generation)
            .unwrap_or(0)
            != state.generation
        {
            return Err(StorageError::backend(
                "GlacierStorage segment catalog generation disagrees with state",
            ));
        }
        if catalog_entries
            .last()
            .map(SegmentCatalogEntry::end)
            .transpose()?
            .unwrap_or(GLACIER_SUPERBLOCK_BYTES as u64)
            != catalog_file_len
        {
            return Err(StorageError::backend(
                "GlacierStorage segment catalog does not cover the data file",
            ));
        }
        startup_metrics.segment_catalog_segments.store(
            usize_to_u64_saturating(catalog_entries.len()),
            Ordering::Relaxed,
        );
        startup_metrics
            .segment_catalog_build_us
            .store(elapsed_micros(catalog_started), Ordering::Relaxed);
        let segment_catalog = Arc::new(SegmentCatalogSnapshot {
            file_len: catalog_file_len,
            segments: Arc::from(catalog_entries),
        });
        startup_metrics
            .total_us
            .store(elapsed_micros(startup_started), Ordering::Relaxed);

        let backend = Self {
            inner: Arc::new(GlacierInner {
                path: path.to_path_buf(),
                format,
                state: RwLock::new(state),
                persist_lock: Mutex::new(()),
                write_metrics: GlacierWriteMetrics::default(),
                startup_metrics,
                read_metrics: GlacierReadMetrics::default(),
                read_mmap: Mutex::new(None),
                segment_catalog: Mutex::new(segment_catalog),
                memory_governor,
                page_cache_reservation: Mutex::new(page_cache_reservation),
                page_cache_reclaimer: Mutex::new(None),
                checkpoint_offset: AtomicU64::new(replay_offset),
                // A checkpoint-less startup has just paid the full rebuild cost.  Historical
                // bytes must not make the very next tiny commit immediately serialize the
                // entire primary state again.  Start the automatic-checkpoint growth window
                // at the current end of the store in that case.  When a checkpoint was
                // loaded, keep measuring growth from the checkpoint boundary so a large
                // replay tail can still make maintenance due naturally.
                next_checkpoint_offset: AtomicU64::new(next_checkpoint_offset_after_open(
                    checkpoint_loaded,
                    replay_offset,
                    file_len,
                )),
                checkpoint_scheduled: AtomicBool::new(false),
            }),
        };
        if let Some(governor) = backend.inner.memory_governor.as_ref() {
            let reclaimer: Arc<dyn MemoryReclaimer> = Arc::new(GlacierPageCacheReclaimer {
                inner: Arc::downgrade(&backend.inner),
            });
            governor.register_reclaimer(MemoryClass::PageCache, &reclaimer);
            *backend
                .inner
                .page_cache_reclaimer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(reclaimer);
        }
        backend.refresh_resident_memory_observation();
        Ok(backend)
    }

    #[must_use]
    pub fn startup_metrics(&self) -> GlacierStartupMetricsSnapshot {
        self.inner.startup_metrics.snapshot()
    }

    #[must_use]
    pub fn read_metrics(&self) -> GlacierReadMetricsSnapshot {
        self.inner.read_metrics.snapshot()
    }
    #[must_use]
    pub fn resident_memory(&self) -> GlacierResidentMemorySnapshot {
        let mut resident = self
            .state_read()
            .map(|state| resident_memory_snapshot(&state))
            .unwrap_or_default();
        add_segment_catalog_memory(&mut resident, &self.inner.segment_catalog);
        if let Some(governor) = self.inner.memory_governor.as_ref() {
            governor.set_observed_bytes(
                MemoryClass::Indexing,
                u64_to_usize_saturating(resident.state_estimated_bytes),
            );
        }
        resident
    }

    fn refresh_resident_memory_observation(&self) {
        let _ = self.resident_memory();
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    #[must_use]
    pub fn format_info(&self) -> GlacierFormatInfo {
        self.inner.format
    }

    #[must_use]
    pub fn write_metrics(&self) -> GlacierWriteMetricsSnapshot {
        let mut snapshot = self.inner.write_metrics.snapshot();
        snapshot.checkpoint_next_offset = self.inner.next_checkpoint_offset.load(Ordering::Relaxed);
        snapshot
    }

    pub fn store_bytes(&self) -> StorageResult<u64> {
        std::fs::metadata(&self.inner.path)
            .map(|metadata| metadata.len())
            .map_err(io_error("stat store", &self.inner.path))
    }

    pub fn wal_bytes(&self) -> StorageResult<u64> {
        Ok(0)
    }

    pub fn generation(&self) -> StorageResult<u64> {
        Ok(self.state_read()?.generation)
    }

    pub fn collection_count(&self) -> StorageResult<usize> {
        let state = self.state_read()?;
        Ok(state
            .collections
            .iter()
            .filter(|(_, c)| visible_count(c, state.generation) > 0)
            .count())
    }

    pub fn document_count(&self) -> StorageResult<usize> {
        let state = self.state_read()?;
        state
            .collections
            .values()
            .try_fold(0usize, |total, collection| {
                total
                    .checked_add(visible_count(collection, state.generation) as usize)
                    .ok_or_else(|| StorageError::backend("GlacierStorage document count overflow"))
            })
    }

    pub fn collection_metadata(
        &self,
        collection: &CollectionId,
    ) -> StorageResult<Option<GlacierCollectionMetadata>> {
        let state = self.state_read()?;
        Ok(state
            .metadata
            .collections
            .get(collection.as_str())
            .map(|stats| public_collection_metadata(collection.as_str(), stats)))
    }

    pub fn metadata(&self) -> StorageResult<Vec<GlacierCollectionMetadata>> {
        let state = self.state_read()?;
        Ok(state
            .metadata
            .collections
            .iter()
            .map(|(name, stats)| public_collection_metadata(name, stats))
            .collect())
    }

    pub fn clear(&self) -> StorageResult<()> {
        let _guard = self.persistence_guard()?;
        let current = self.generation()?;
        if self.document_count()? == 0 {
            return Ok(());
        }
        let generation = next_generation(current)?;
        append_committed_data_records(
            &self.inner.path,
            generation,
            vec![DataRecord {
                generation,
                mutation: DataMutation::Clear,
            }],
            &SegmentMetadataDelta {
                clear: true,
                collections: BTreeMap::new(),
            },
            Some(&self.inner.write_metrics),
        )?;
        {
            let mut state = self.state_write()?;
            state.generation = generation;
            state.clear_generations.push(generation);
            state.metadata.collections.clear();
            for collection in state.collections.values_mut() {
                collection.count_history.push((generation, 0));
                collection.primary_head.clear();
                collection.primary_head_valid = true;
            }
        }
        drop(_guard);
        self.maybe_checkpoint_deferred(generation);
        Ok(())
    }

    pub fn checkpoint(&self) -> StorageResult<()> {
        let _guard = self.persistence_guard()?;
        self.write_checkpoint_locked()
    }

    fn state_read(&self) -> StorageResult<std::sync::RwLockReadGuard<'_, GlacierState>> {
        self.inner
            .state
            .read()
            .map_err(|_| StorageError::backend("GlacierStorage state read lock poisoned"))
    }

    fn state_write(&self) -> StorageResult<std::sync::RwLockWriteGuard<'_, GlacierState>> {
        self.inner
            .state
            .write()
            .map_err(|_| StorageError::backend("GlacierStorage state write lock poisoned"))
    }

    fn persistence_guard(&self) -> StorageResult<std::sync::MutexGuard<'_, ()>> {
        self.inner
            .persist_lock
            .lock()
            .map_err(|_| StorageError::backend("GlacierStorage persistence lock poisoned"))
    }

    fn maybe_checkpoint_deferred(&self, committed_generation: u64) {
        let Ok(data_len) = std::fs::metadata(&self.inner.path).map(|metadata| metadata.len())
        else {
            return;
        };
        if data_len < self.inner.next_checkpoint_offset.load(Ordering::Relaxed) {
            return;
        }
        if self
            .inner
            .checkpoint_scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        self.inner
            .write_metrics
            .checkpoint_deferred
            .fetch_add(1, Ordering::Relaxed);

        let backend = self.clone();
        thread::spawn(move || {
            // Automatic checkpoints are maintenance work, not part of commit
            // latency. Only run after a short write-idle window; a continuous
            // import keeps advancing generation and therefore never stalls on
            // an O(N) checkpoint.
            thread::sleep(Duration::from_millis(500));

            let idle = backend
                .generation()
                .is_ok_and(|generation| generation == committed_generation);
            if !idle {
                backend
                    .inner
                    .checkpoint_scheduled
                    .store(false, Ordering::Release);
                return;
            }

            let Ok(guard) = backend.inner.persist_lock.try_lock() else {
                backend
                    .inner
                    .checkpoint_scheduled
                    .store(false, Ordering::Release);
                return;
            };

            let still_idle = backend
                .generation()
                .is_ok_and(|generation| generation == committed_generation);
            let due = std::fs::metadata(&backend.inner.path)
                .map(|metadata| {
                    metadata.len() >= backend.inner.next_checkpoint_offset.load(Ordering::Relaxed)
                })
                .unwrap_or(false);

            if still_idle && due {
                if backend.write_checkpoint_locked().is_err() {
                    if let Ok(data_len) =
                        std::fs::metadata(&backend.inner.path).map(|metadata| metadata.len())
                    {
                        let checkpoint_offset =
                            backend.inner.checkpoint_offset.load(Ordering::Relaxed);
                        backend.inner.next_checkpoint_offset.store(
                            checkpoint_retry_offset_after_failure(data_len, checkpoint_offset),
                            Ordering::Relaxed,
                        );
                    }
                }
            }
            drop(guard);
            backend
                .inner
                .checkpoint_scheduled
                .store(false, Ordering::Release);
        });
    }

    fn write_checkpoint_locked(&self) -> StorageResult<()> {
        let checkpoint_started = Instant::now();
        let result = (|| {
            let data_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.inner.path)
                .map_err(io_error("open store for checkpoint", &self.inner.path))?;
            // Every committed Glacier segment is sync_data()'d before it is
            // published in the in-memory index. Under persist_lock there can be
            // no concurrent append here, so another sync_all() would only add a
            // foreground checkpoint stall without strengthening durability.
            let data_len = data_file
                .metadata()
                .map_err(io_error("stat store for checkpoint", &self.inner.path))?
                .len();

            let build_started = Instant::now();
            let checkpoint = {
                let state = self.state_read()?;
                build_checkpoint(&state, self.inner.format, data_len)?
            };
            self.inner
                .write_metrics
                .checkpoint_build_us
                .fetch_add(elapsed_micros(build_started), Ordering::Relaxed);
            let document_count = checkpoint
                .collections
                .iter()
                .fold(0u64, |total, collection| {
                    total.saturating_add(collection.count)
                });

            let write_started = Instant::now();
            let checkpoint_write = write_checkpoint(&self.inner.path, &checkpoint)?;
            self.inner
                .write_metrics
                .checkpoint_encode_us
                .fetch_add(checkpoint_write.encode_us, Ordering::Relaxed);
            self.inner
                .write_metrics
                .checkpoint_io_us
                .fetch_add(checkpoint_write.io_us, Ordering::Relaxed);
            self.inner
                .write_metrics
                .checkpoint_write_us
                .fetch_add(elapsed_micros(write_started), Ordering::Relaxed);
            let checkpoint_bytes = checkpoint_write.bytes;
            self.inner
                .write_metrics
                .checkpoint_documents
                .store(document_count, Ordering::Relaxed);
            self.inner
                .write_metrics
                .checkpoint_bytes
                .store(checkpoint_bytes, Ordering::Relaxed);
            self.inner
                .checkpoint_offset
                .store(data_len, Ordering::Relaxed);
            self.inner.next_checkpoint_offset.store(
                data_len.saturating_add(automatic_checkpoint_interval(data_len)),
                Ordering::Relaxed,
            );
            Ok(())
        })();

        let checkpoint_elapsed_us = elapsed_micros(checkpoint_started);
        self.inner
            .write_metrics
            .checkpoint_runs
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .write_metrics
            .checkpoint_total_us
            .fetch_add(checkpoint_elapsed_us, Ordering::Relaxed);
        self.inner
            .write_metrics
            .checkpoint_last_us
            .store(checkpoint_elapsed_us, Ordering::Relaxed);
        self.inner
            .write_metrics
            .checkpoint_max_us
            .fetch_max(checkpoint_elapsed_us, Ordering::Relaxed);
        if result.is_err() {
            self.inner
                .write_metrics
                .checkpoint_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn load_pointer(&self, pointer: RecordPointer) -> StorageResult<StoredDocument> {
        read_stored_document_profiled(&self.inner.path, pointer, &self.inner.read_metrics)
    }

    fn visible_pointer(
        &self,
        generation: u64,
        collection: &CollectionId,
        id: &DocumentId,
    ) -> StorageResult<Option<IndexVersion>> {
        let state = self.state_read()?;
        Ok(visible_version(&state, generation, collection, id))
    }

    fn current_stored(
        &self,
        collection: &CollectionId,
        id: &DocumentId,
    ) -> StorageResult<Option<StoredDocument>> {
        let generation = self.generation()?;
        match self.visible_pointer(generation, collection, id)? {
            Some(index) => match index.pointer {
                Some(pointer) => self.load_pointer(pointer).map(Some),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    fn commit_mutations(
        &self,
        expected_generation: u64,
        mutations: Vec<PreparedMutation>,
    ) -> StorageResult<CommitResult> {
        if mutations.is_empty() {
            return Ok(CommitResult::default());
        }

        let commit_started = Instant::now();
        let mutation_count = usize_to_u64_saturating(mutations.len());
        let _guard = self.persistence_guard()?;
        {
            let state = self.state_read()?;
            if state.generation != expected_generation {
                return Err(StorageError::transaction_conflict(
                    "GlacierStorage generation changed before commit",
                ));
            }
        }

        let generation = next_generation(expected_generation)?;
        let metadata_delta = metadata_delta_for_mutations(&mutations)?;
        let records = mutations
            .iter()
            .map(|mutation| {
                mutation.data_mutation().map(|mutation| DataRecord {
                    generation,
                    mutation,
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let pointers = append_committed_data_records(
            &self.inner.path,
            generation,
            records,
            &metadata_delta,
            Some(&self.inner.write_metrics),
        )?;

        let mut state = self.state_write()?;
        if state.generation != expected_generation {
            return Err(StorageError::transaction_conflict(
                "GlacierStorage generation changed during commit",
            ));
        }

        let mut inserted = 0u64;
        let mut replaced = 0u64;
        let mut deleted = 0u64;

        // One visible-count baseline per touched collection. All mutations in
        // this batch share `generation`, so count history only needs one final
        // value per collection/generation rather than one entry per mutation.
        let mut count_updates = BTreeMap::<CollectionId, (u64, i64)>::new();
        let mut primary_append_batches = BTreeMap::<CollectionId, Vec<CompactPrimaryEntry>>::new();
        for prepared in &mutations {
            let collection = match prepared {
                PreparedMutation::Set { collection, .. }
                | PreparedMutation::Delete { collection, .. } => collection,
            };
            count_updates.entry(collection.clone()).or_insert_with(|| {
                let count = state
                    .collections
                    .get(collection)
                    .map_or(0, |index| visible_count(index, expected_generation));
                (count, 0)
            });
        }

        for (prepared, pointer) in mutations.into_iter().zip(pointers) {
            match prepared {
                PreparedMutation::Set {
                    collection,
                    stored,
                    previous,
                } => {
                    if previous.is_some() {
                        replaced = replaced.saturating_add(1);
                    } else {
                        inserted = inserted.saturating_add(1);
                    }
                    let index_started = Instant::now();
                    let collection_index = state.collections.entry(collection.clone()).or_default();
                    if previous.is_some() {
                        collection_index.primary_head_valid = false;
                    } else {
                        primary_head_insert(collection_index, stored.id().clone(), pointer);
                    }
                    let index_version = IndexVersion {
                        generation,
                        version: stored.version(),
                        pointer: Some(pointer),
                    };
                    if previous.is_some() {
                        collection_index.push_existing(stored.id().clone(), index_version);
                    } else {
                        let batched = collection_index.disk_primary.as_ref().is_some_and(|disk| {
                            disk.last_id.map_or(true, |last| *stored.id() > last)
                        }) && CompactPrimaryEntry::from_index_version(
                            stored.id().clone(),
                            index_version,
                        )
                        .is_some();
                        if batched {
                            primary_append_batches
                                .entry(collection.clone())
                                .or_default()
                                .push(
                                    CompactPrimaryEntry::from_index_version(
                                        stored.id().clone(),
                                        index_version,
                                    )
                                    .expect("batch eligibility checked"),
                                );
                        } else {
                            collection_index.insert_new(stored.id().clone(), index_version);
                        }
                    }
                    if previous.is_none() {
                        if let Some((_, delta)) = count_updates.get_mut(&collection) {
                            *delta = delta.saturating_add(1);
                        }
                    }
                    self.inner
                        .write_metrics
                        .primary_index_us
                        .fetch_add(elapsed_micros(index_started), Ordering::Relaxed);
                }
                PreparedMutation::Delete {
                    collection,
                    id,
                    previous,
                } => {
                    let index_started = Instant::now();
                    let collection_index = state.collections.entry(collection.clone()).or_default();
                    collection_index.primary_head_valid = false;
                    let index_version = IndexVersion {
                        generation,
                        version: previous.version(),
                        pointer: None,
                    };
                    collection_index.push_existing(id, index_version);
                    if let Some((_, delta)) = count_updates.get_mut(&collection) {
                        *delta = delta.saturating_sub(1);
                    }
                    self.inner
                        .write_metrics
                        .primary_index_us
                        .fetch_add(elapsed_micros(index_started), Ordering::Relaxed);
                    deleted = deleted.saturating_add(1);
                }
            }
        }

        for (collection, mut entries) in primary_append_batches {
            let index_started = Instant::now();
            let collection_index = state.collections.entry(collection).or_default();
            let appended = if let Some(disk) = collection_index.disk_primary.as_mut() {
                disk_primary_append_batch(disk, &mut entries)?
            } else {
                false
            };
            if !appended {
                for entry in entries {
                    collection_index.insert_new(entry.id, entry.index_version());
                }
            }
            self.inner
                .write_metrics
                .primary_index_us
                .fetch_add(elapsed_micros(index_started), Ordering::Relaxed);
        }

        for (collection, (baseline, delta)) in count_updates {
            let final_count = if delta >= 0 {
                baseline.saturating_add(delta as u64)
            } else {
                baseline.saturating_sub(delta.unsigned_abs())
            };
            state
                .collections
                .entry(collection)
                .or_default()
                .count_history
                .push((generation, final_count));
        }

        let metadata_started = Instant::now();
        apply_metadata_delta(&mut state.metadata, &metadata_delta)?;
        self.inner
            .write_metrics
            .metadata_us
            .fetch_add(elapsed_micros(metadata_started), Ordering::Relaxed);

        state.generation = generation;
        drop(state);
        self.inner
            .write_metrics
            .commits
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .write_metrics
            .mutations
            .fetch_add(mutation_count, Ordering::Relaxed);
        self.inner
            .write_metrics
            .commit_us
            .fetch_add(elapsed_micros(commit_started), Ordering::Relaxed);
        drop(_guard);
        self.maybe_checkpoint_deferred(generation);
        Ok(CommitResult::new(inserted, replaced, deleted))
    }
}

impl StorageBackend for GlacierBackend {
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>> {
        Ok(Box::new(GlacierSnapshot {
            backend: self.clone(),
            generation: self.generation()?,
        }))
    }

    fn begin(&self) -> StorageResult<Box<dyn StorageTransaction + '_>> {
        Ok(Box::new(GlacierTransaction::new(self, self.generation()?)))
    }

    fn apply_batch_atomic(
        &self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<(Vec<StoredDocument>, CommitResult)> {
        if mutations.is_empty() {
            return Ok((Vec::new(), CommitResult::default()));
        }

        let generation = self.generation()?;

        // Validate insert IDs in one source-index snapshot instead of taking
        // the state lock and probing the page-backed primary independently
        // for every row during mutation materialization.
        let lookup_started = Instant::now();
        let mut insert_ids = HashSet::<DocumentId>::new();
        for mutation in &mutations {
            if let StorageMutation::Insert { id, .. } = mutation {
                if !insert_ids.insert(id.clone()) {
                    return Err(StorageError::document_already_exists(
                        collection.clone(),
                        id.clone(),
                    ));
                }
            }
        }
        if !insert_ids.is_empty() {
            let state = self.state_read()?;
            if state.generation != generation {
                return Err(StorageError::transaction_conflict(
                    "GlacierStorage generation changed during primary preflight",
                ));
            }
            if let Some(collection_index) = state.collections.get(collection) {
                for id in &insert_ids {
                    if collection_index
                        .visible_version(&state, generation, id)
                        .is_some()
                    {
                        return Err(StorageError::document_already_exists(
                            collection.clone(),
                            id.clone(),
                        ));
                    }
                }
            }
        }
        self.inner.write_metrics.primary_lookup_records.fetch_add(
            usize_to_u64_saturating(insert_ids.len()),
            Ordering::Relaxed,
        );
        self.inner
            .write_metrics
            .primary_lookup_us
            .fetch_add(elapsed_micros(lookup_started), Ordering::Relaxed);

        let mut staged = BTreeMap::<DocumentId, StoredDocument>::new();
        let mut prepared = Vec::with_capacity(mutations.len());
        let mut returned = Vec::with_capacity(mutations.len());

        for mutation in mutations {
            match mutation {
                StorageMutation::Insert { id, document } => {
                    if staged.contains_key(&id) {
                        return Err(StorageError::document_already_exists(
                            collection.clone(),
                            id,
                        ));
                    }
                    let stored =
                        StoredDocument::new(id.clone(), DocumentVersion::INITIAL, document)?;
                    staged.insert(id, stored.clone());
                    prepared.push(PreparedMutation::Set {
                        collection: collection.clone(),
                        stored: stored.clone(),
                        previous: None,
                    });
                    returned.push(stored);
                }
                StorageMutation::Replace {
                    id,
                    document,
                    precondition,
                } => {
                    let current = if let Some(current) = staged.get(&id) {
                        current.clone()
                    } else {
                        self.current_stored(collection, &id)?.ok_or_else(|| {
                            StorageError::document_not_found(collection.clone(), id.clone())
                        })?
                    };
                    ensure_precondition(collection, &id, current.version(), precondition)?;
                    let stored =
                        StoredDocument::new(id.clone(), current.version().next()?, document)?;
                    staged.insert(id, stored.clone());
                    prepared.push(PreparedMutation::Set {
                        collection: collection.clone(),
                        stored: stored.clone(),
                        previous: Some(current),
                    });
                    returned.push(stored);
                }
            }
        }

        let commit = self.commit_mutations(generation, prepared)?;
        Ok((returned, commit))
    }

    fn apply_batch_atomic_summary(
        &self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<CommitResult> {
        self.apply_batch_atomic(collection, mutations)
            .map(|(_, commit)| commit)
    }
}

const BOUNDED_LIMIT_SELECTION_MAX: usize = 4096;

fn select_visible_entries_bounded( state: &GlacierState, collection_index: &CollectionIndex, generation: u64, direction: ScanDirection, limit: usize, ) -> Vec<(DocumentId, RecordPointer)> {
    if limit == 0 {
        return Vec::new();
    }

    let mut selected: Vec<(DocumentId, RecordPointer)> = Vec::with_capacity(limit);
    collection_index.for_each_id_version(state, generation, |id, version| {
        let Some(pointer) = version.pointer else {
            return;
        };

        if selected.len() < limit {
            selected.push((id, pointer));
            return;
        }

        let mut worst_index = 0usize;
        for index in 1..selected.len() {
            let worse = match direction {
                ScanDirection::Forward => selected[index].0 > selected[worst_index].0,
                ScanDirection::Reverse => selected[index].0 < selected[worst_index].0,
            };
            if worse {
                worst_index = index;
            }
        }

        let improves = match direction {
            ScanDirection::Forward => id < selected[worst_index].0,
            ScanDirection::Reverse => id > selected[worst_index].0,
        };
        if improves {
            selected[worst_index] = (id, pointer);
        }
    });

    selected.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    if direction == ScanDirection::Reverse {
        selected.reverse();
    }
    selected
}

fn collect_visible_entries_ordered( state: &GlacierState, collection_index: &CollectionIndex, generation: u64, direction: ScanDirection, limit: Option<usize>, ) -> Vec<(DocumentId, RecordPointer)> {
    let mut entries = Vec::with_capacity(
        limit.unwrap_or_else(|| visible_count(collection_index, generation) as usize),
    );
    collection_index.for_each_id_version(state, generation, |id, version| {
        if let Some(pointer) = version.pointer {
            entries.push((id, pointer));
        }
    });
    entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    if direction == ScanDirection::Reverse {
        entries.reverse();
    }
    if let Some(limit) = limit {
        entries.truncate(limit);
    }
    entries
}

#[derive(Clone, Copy)]
struct VisibleExceptionEntry {
    id: DocumentId,
    pointer: Option<RecordPointer>,
}

/// Streams an ordered cold primary index without materializing one pointer per
/// document. Large Glacier collections are intentionally represented by the
/// disk primary sidecar; full scans must preserve that O(1) memory property.
fn scan_disk_primary_ordered_each( store_path: &Path, disk: &DiskPrimaryIndex, generation: u64, clear_generation: u64, mut exceptions: Vec<VisibleExceptionEntry>, direction: ScanDirection, metrics: &GlacierReadMetrics, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>, ) -> StorageResult<u64> {
    exceptions.sort_unstable_by_key(|entry| entry.id);
    let mut primary =
        File::open(&disk.path).map_err(io_error("open primary index scan", &disk.path))?;
    let mut data = File::open(store_path).map_err(io_error("open data", store_path))?;
    let mut bytes = [0u8; PRIMARY_INDEX_ENTRY_BYTES as usize];
    let mut rows = 0u64;

    let mut emit = |pointer: Option<RecordPointer>| -> StorageResult<bool> {
        let Some(pointer) = pointer else {
            return Ok(true);
        };
        rows = rows.saturating_add(1);
        visitor(read_stored_document_from_file_profiled(
            &mut data, store_path, pointer, metrics,
        )?)
    };

    match direction {
        ScanDirection::Forward => {
            primary
                .seek(SeekFrom::Start(PRIMARY_INDEX_HEADER_BYTES))
                .map_err(io_error("seek primary index scan", &disk.path))?;
            let mut exception_index = 0usize;
            for _ in 0..disk.count {
                primary
                    .read_exact(&mut bytes)
                    .map_err(io_error("read primary index scan", &disk.path))?;
                let entry = decode_compact_primary_entry(&bytes);
                while exception_index < exceptions.len()
                    && exceptions[exception_index].id < entry.id
                {
                    if !emit(exceptions[exception_index].pointer)? {
                        return Ok(rows);
                    }
                    exception_index += 1;
                }
                if exception_index < exceptions.len() && exceptions[exception_index].id == entry.id
                {
                    if !emit(exceptions[exception_index].pointer)? {
                        return Ok(rows);
                    }
                    exception_index += 1;
                } else if entry.generation <= generation && entry.generation > clear_generation {
                    if !emit(Some(entry.pointer))? {
                        return Ok(rows);
                    }
                }
            }
            while exception_index < exceptions.len() {
                if !emit(exceptions[exception_index].pointer)? {
                    break;
                }
                exception_index += 1;
            }
        }
        ScanDirection::Reverse => {
            let mut exception_index = exceptions.len();
            for index in (0..disk.count).rev() {
                let offset = PRIMARY_INDEX_HEADER_BYTES
                    .checked_add(index.saturating_mul(PRIMARY_INDEX_ENTRY_BYTES))
                    .ok_or_else(|| StorageError::backend("primary index scan offset overflow"))?;
                primary
                    .seek(SeekFrom::Start(offset))
                    .map_err(io_error("seek primary index reverse scan", &disk.path))?;
                primary
                    .read_exact(&mut bytes)
                    .map_err(io_error("read primary index reverse scan", &disk.path))?;
                let entry = decode_compact_primary_entry(&bytes);
                while exception_index > 0 && exceptions[exception_index - 1].id > entry.id {
                    exception_index -= 1;
                    if !emit(exceptions[exception_index].pointer)? {
                        return Ok(rows);
                    }
                }
                if exception_index > 0 && exceptions[exception_index - 1].id == entry.id {
                    exception_index -= 1;
                    if !emit(exceptions[exception_index].pointer)? {
                        return Ok(rows);
                    }
                } else if entry.generation <= generation && entry.generation > clear_generation {
                    if !emit(Some(entry.pointer))? {
                        return Ok(rows);
                    }
                }
            }
            while exception_index > 0 {
                exception_index -= 1;
                if !emit(exceptions[exception_index].pointer)? {
                    break;
                }
            }
        }
    }
    Ok(rows)
}

#[derive(Clone)]
struct GlacierSnapshot {
    backend: GlacierBackend,
    generation: u64,
}

#[inline]
fn glacier_value_at_field_path<'a>(document: &'a Document, path: &FieldPath) -> Option<&'a Value> {
    let first = path.get(0)?;
    let mut value = document.get(first.as_str())?;
    for index in 1..path.len() {
        let segment = path.get(index)?;
        let Value::Object(object) = value else {
            return None;
        };
        value = object.get(segment.as_str())?;
    }
    Some(value)
}

#[inline]
fn projected_value_ref(value: &Value) -> ProjectedValueRef<'_> {
    match value {
        Value::Null => ProjectedValueRef::Null,
        Value::Bool(value) => ProjectedValueRef::Bool(*value),
        Value::Number(Number::Signed(value)) => ProjectedValueRef::Signed(*value),
        Value::Number(Number::Unsigned(value)) => ProjectedValueRef::Unsigned(*value),
        Value::Number(Number::Float(value)) => ProjectedValueRef::Float(*value),
        Value::String(value) => ProjectedValueRef::String(value.as_ref()),
        Value::Array(_) | Value::Object(_) => ProjectedValueRef::Owned(value.clone()),
    }
}

impl StorageRead for GlacierSnapshot {
    fn support(&self, capability: StorageReadCapability) -> StorageSupport {
        match capability {
            StorageReadCapability::ProjectedValuesGatedUnordered => StorageSupport::Native,
        }
    }

    fn get(
        &self,
        collection: &CollectionId,
        id: &DocumentId,
    ) -> StorageResult<Option<StoredDocument>> {
        match self
            .backend
            .visible_pointer(self.generation, collection, id)?
        {
            Some(index) => match index.pointer {
                Some(pointer) => self.backend.load_pointer(pointer).map(Some),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    fn scan(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
    ) -> StorageResult<Vec<StoredDocument>> {
        let mut documents = Vec::new();
        self.scan_each(collection, options, &mut |document| {
            documents.push(document);
            Ok(true)
        })?;
        Ok(documents)
    }

    fn scan_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        let metrics = &self.backend.inner.read_metrics;
        metrics
            .generic_scan_each_calls
            .fetch_add(1, Ordering::Relaxed);
        let prepare_started = Instant::now();

        // A full scan over a cold primary index is already physically ordered.
        // Never expand it into an O(document_count) pointer vector: that used to
        // consume ~32 bytes per document (~500 MiB for 16.7M rows) per concurrent
        // query, completely bypassing the memory governor. Only the comparatively
        // small exception set is materialized for the ordered merge.
        if options.limit().is_none() {
            let cold_scan = {
                let state = self.backend.state_read()?;
                let Some(collection_index) = state.collections.get(collection) else {
                    return Ok(());
                };
                collection_index.disk_primary.as_ref().map(|disk| {
                    let clear = clear_generation_at(&state, self.generation);
                    let exceptions = collection_index
                        .exceptions
                        .iter()
                        .map(|(id, versions)| {
                            let pointer = versions
                                .iter_rev()
                                .copied()
                                .find(|version| {
                                    version.generation <= self.generation
                                        && version.generation > clear
                                })
                                .and_then(|version| version.pointer);
                            VisibleExceptionEntry { id: *id, pointer }
                        })
                        .collect::<Vec<_>>();
                    (disk.clone(), clear, exceptions)
                })
            };
            if let Some((disk, clear, exceptions)) = cold_scan {
                metrics
                    .generic_scan_each_prepare_us
                    .fetch_add(elapsed_micros(prepare_started), Ordering::Relaxed);
                let pointer_loop_started = Instant::now();
                let rows = scan_disk_primary_ordered_each(
                    &self.backend.inner.path,
                    &disk,
                    self.generation,
                    clear,
                    exceptions,
                    options.direction(),
                    metrics,
                    visitor,
                )?;
                metrics
                    .generic_scan_each_rows
                    .fetch_add(rows, Ordering::Relaxed);
                metrics
                    .generic_scan_each_pointer_loop_us
                    .fetch_add(elapsed_micros(pointer_loop_started), Ordering::Relaxed);
                return Ok(());
            }
        }

        let entries = {
            let state = self.backend.state_read()?;
            let Some(collection_index) = state.collections.get(collection) else {
                return Ok(());
            };

            if let Some(limit) = options.limit() {
                if options.direction() == ScanDirection::Forward
                    && self.generation == state.generation
                {
                    if let Some(entries) = primary_head_entries(collection_index, limit) {
                        self.backend
                            .inner
                            .read_metrics
                            .primary_head_scans
                            .fetch_add(1, Ordering::Relaxed);
                        entries
                    } else if limit <= BOUNDED_LIMIT_SELECTION_MAX {
                        self.backend
                            .inner
                            .read_metrics
                            .bounded_limit_scans
                            .fetch_add(1, Ordering::Relaxed);
                        select_visible_entries_bounded(
                            &state,
                            collection_index,
                            self.generation,
                            options.direction(),
                            limit,
                        )
                    } else {
                        collect_visible_entries_ordered(
                            &state,
                            collection_index,
                            self.generation,
                            options.direction(),
                            Some(limit),
                        )
                    }
                } else if limit <= BOUNDED_LIMIT_SELECTION_MAX {
                    self.backend
                        .inner
                        .read_metrics
                        .bounded_limit_scans
                        .fetch_add(1, Ordering::Relaxed);
                    select_visible_entries_bounded(
                        &state,
                        collection_index,
                        self.generation,
                        options.direction(),
                        limit,
                    )
                } else {
                    collect_visible_entries_ordered(
                        &state,
                        collection_index,
                        self.generation,
                        options.direction(),
                        Some(limit),
                    )
                }
            } else {
                collect_visible_entries_ordered(
                    &state,
                    collection_index,
                    self.generation,
                    options.direction(),
                    None,
                )
            }
        };
        metrics
            .generic_scan_each_prepare_us
            .fetch_add(elapsed_micros(prepare_started), Ordering::Relaxed);

        let pointer_loop_started = Instant::now();
        let mut rows = 0u64;
        // A generic scan owns one data-file handle for its lifetime; projected
        // scans remain preferred, while fallback scans avoid per-record opens.
        let open_started = Instant::now();
        let mut data_file = File::open(&self.backend.inner.path)
            .map_err(io_error("open data", &self.backend.inner.path))?;
        metrics
            .pointer_open_us
            .fetch_add(elapsed_micros(open_started), Ordering::Relaxed);
        for (_, pointer) in entries {
            rows = rows.saturating_add(1);
            if !visitor(read_stored_document_from_file_profiled(
                &mut data_file,
                &self.backend.inner.path,
                pointer,
                metrics,
            )?)? {
                break;
            }
        }
        metrics
            .generic_scan_each_rows
            .fetch_add(rows, Ordering::Relaxed);
        metrics
            .generic_scan_each_pointer_loop_us
            .fetch_add(elapsed_micros(pointer_loop_started), Ordering::Relaxed);
        Ok(())
    }

    fn scan_projected_unordered_each( &self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>, ) -> StorageResult<()> {
        if options.direction() != ScanDirection::Forward || options.limit().is_some() {
            return StorageRead::scan_projected_each(self, collection, options, fields, visitor);
        }

        let state = self.backend.state_read()?;
        let Some(collection_index) = state.collections.get(collection) else {
            return Ok(());
        };
        scan_collection_sequential(
            &self.backend.inner.path,
            &state,
            collection_index,
            collection,
            self.generation,
            fields,
            &self.backend.inner.segment_catalog,
            &self.backend.inner.read_mmap,
            &self.backend.inner.read_metrics,
            visitor,
        )
    }

    fn scan_projected_values_unordered_each( &self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], visitor: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>, ) -> StorageResult<()> {
        if options.direction() != ScanDirection::Forward || options.limit().is_some() {
            let mut projected = Vec::new();
            self.scan_projected_each(collection, options, fields, &mut |stored| {
                projected.clear();
                for path in fields {
                    projected.push(glacier_value_at_field_path(stored.document(), path).cloned());
                }
                visitor(&projected)
            })?;
            return Ok(());
        }
        let state = self.backend.state_read()?;
        let Some(collection_index) = state.collections.get(collection) else {
            return Ok(());
        };
        let mut gate = |_values: &[Option<Value>]| Ok(true);
        scan_collection_sequential_values(
            &self.backend.inner.path,
            &state,
            collection_index,
            collection,
            self.generation,
            fields,
            fields.len(),
            &self.backend.inner.segment_catalog,
            &self.backend.inner.read_mmap,
            &self.backend.inner.read_metrics,
            &mut gate,
            &mut |_id, _version, values| visitor(values),
            None,
        )
    }

    fn scan_projected_row_refs_unordered_each( &self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], visitor: &mut dyn for<'a> FnMut( DocumentId, DocumentVersion, &[Option<ProjectedValueRef<'a>>], ) -> StorageResult<bool>, ) -> StorageResult<()> {
        if options.direction() != ScanDirection::Forward || options.limit().is_some() {
            return self.scan_projected_unordered_each(
                collection,
                options,
                fields,
                &mut |stored| {
                    let id = stored.id().clone();
                    let version = stored.version();
                    let values = fields
                        .iter()
                        .map(|path| {
                            glacier_value_at_field_path(stored.document(), path)
                                .map(projected_value_ref)
                        })
                        .collect::<Vec<_>>();
                    visitor(id, version, &values)
                },
            );
        }
        let state = self.backend.state_read()?;
        let Some(collection_index) = state.collections.get(collection) else {
            return Ok(());
        };
        scan_collection_sequential_value_refs(
            &self.backend.inner.path,
            &state,
            collection_index,
            collection,
            self.generation,
            fields,
            &self.backend.inner.segment_catalog,
            &self.backend.inner.read_mmap,
            &self.backend.inner.read_metrics,
            visitor,
        )
    }

    fn scan_projected_value_refs_unordered_each( &self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], visitor: &mut dyn for<'a> FnMut(&[Option<ProjectedValueRef<'a>>]) -> StorageResult<bool>, ) -> StorageResult<()> {
        if options.direction() != ScanDirection::Forward || options.limit().is_some() {
            return self.scan_projected_values_unordered_each(
                collection,
                options,
                fields,
                &mut |values| {
                    let refs = values
                        .iter()
                        .map(|value| value.as_ref().map(projected_value_ref))
                        .collect::<Vec<_>>();
                    visitor(&refs)
                },
            );
        }
        let state = self.backend.state_read()?;
        let Some(collection_index) = state.collections.get(collection) else {
            return Ok(());
        };
        scan_collection_sequential_value_refs(
            &self.backend.inner.path,
            &state,
            collection_index,
            collection,
            self.generation,
            fields,
            &self.backend.inner.segment_catalog,
            &self.backend.inner.read_mmap,
            &self.backend.inner.read_metrics,
            &mut |_id, _version, values| visitor(values),
        )
    }

    fn scan_projected_values_gated_unordered_each( &self, collection: &CollectionId, options: ScanOptions, fields: &[FieldPath], gate_field_count: usize, gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>, visitor: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>, ) -> StorageResult<()> {
        if options.direction() != ScanDirection::Forward || options.limit().is_some() {
            return self.scan_projected_values_unordered_each(
                collection,
                options,
                fields,
                &mut |values| {
                    if gate(values)? {
                        visitor(values)
                    } else {
                        Ok(true)
                    }
                },
            );
        }
        let state = self.backend.state_read()?;
        let Some(collection_index) = state.collections.get(collection) else {
            return Ok(());
        };
        scan_collection_sequential_values(
            &self.backend.inner.path,
            &state,
            collection_index,
            collection,
            self.generation,
            fields,
            gate_field_count,
            &self.backend.inner.segment_catalog,
            &self.backend.inner.read_mmap,
            &self.backend.inner.read_metrics,
            gate,
            &mut |_id, _version, values| visitor(values),
            None,
        )
    }

    fn scan_projected_row_values_gated_unordered_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        fields: &[FieldPath],
        gate_field_count: usize,
        gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>,
        visitor: &mut dyn FnMut(
            DocumentId,
            DocumentVersion,
            &[Option<Value>],
        ) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        if options.direction() != ScanDirection::Forward || options.limit().is_some() {
            return StorageRead::scan_projected_row_values_gated_unordered_each(
                self,
                collection,
                options,
                fields,
                gate_field_count,
                gate,
                visitor,
            );
        }
        let state = self.backend.state_read()?;
        let Some(collection_index) = state.collections.get(collection) else {
            return Ok(());
        };
        scan_collection_sequential_values(
            &self.backend.inner.path,
            &state,
            collection_index,
            collection,
            self.generation,
            fields,
            gate_field_count,
            &self.backend.inner.segment_catalog,
            &self.backend.inner.read_mmap,
            &self.backend.inner.read_metrics,
            gate,
            visitor,
            None,
        )
    }

    fn scan_projected_gated_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        fields: &[FieldPath],
        gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>,
        visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        if options.direction() != ScanDirection::Forward || options.limit().is_some() {
            return StorageRead::scan_projected_gated_each(
                self, collection, options, fields, gate, visitor,
            );
        }
        let state = self.backend.state_read()?;
        let Some(collection_index) = state.collections.get(collection) else {
            return Ok(());
        };
        scan_collection_sequential_values(
            &self.backend.inner.path,
            &state,
            collection_index,
            collection,
            self.generation,
            fields,
            fields.len(),
            &self.backend.inner.segment_catalog,
            &self.backend.inner.read_mmap,
            &self.backend.inner.read_metrics,
            gate,
            &mut |_id, _version, _values| Ok(true),
            Some(visitor),
        )
    }

    fn count(&self, collection: &CollectionId) -> StorageResult<u64> {
        let state = self.backend.state_read()?;
        Ok(state
            .collections
            .get(collection)
            .map(|c| visible_count(c, self.generation))
            .unwrap_or(0))
    }

    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> {
        Ok(self.count(collection)? > 0)
    }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        let state = self.backend.state_read()?;
        Ok(state
            .collections
            .iter()
            .filter_map(|(name, c)| (visible_count(c, self.generation) > 0).then_some(name.clone()))
            .collect())
    }
}

enum TxValue {
    Set(StoredDocument),
    Delete,
}

struct GlacierTransaction<'a> {
    backend: &'a GlacierBackend,
    generation: u64,
    staged: BTreeMap<(CollectionId, DocumentId), TxValue>,
    prepared: Vec<PreparedMutation>,
    closed: bool,
}

impl<'a> GlacierTransaction<'a> {
    fn new(backend: &'a GlacierBackend, generation: u64) -> Self {
        Self {
            backend,
            generation,
            staged: BTreeMap::new(),
            prepared: Vec::new(),
            closed: false,
        }
    }

    fn ensure_open(&self) -> StorageResult<()> {
        if self.closed {
            Err(StorageError::transaction_closed())
        } else {
            Ok(())
        }
    }

    fn staged_get(
        &self,
        collection: &CollectionId,
        id: &DocumentId,
    ) -> Option<Option<StoredDocument>> {
        self.staged
            .get(&(collection.clone(), id.clone()))
            .map(|value| match value {
                TxValue::Set(stored) => Some(stored.clone()),
                TxValue::Delete => None,
            })
    }
}

impl StorageRead for GlacierTransaction<'_> {
    fn get( &self, collection: &CollectionId, id: &DocumentId, ) -> StorageResult<Option<StoredDocument>> {
        self.ensure_open()?;
        if let Some(staged) = self.staged_get(collection, id) {
            return Ok(staged);
        }
        GlacierSnapshot {
            backend: self.backend.clone(),
            generation: self.generation,
        }
        .get(collection, id)
    }

    fn scan( &self, collection: &CollectionId, options: ScanOptions, ) -> StorageResult<Vec<StoredDocument>> {
        let mut base = GlacierSnapshot {
            backend: self.backend.clone(),
            generation: self.generation,
        }
        .scan(collection, ScanOptions::default())?;

        let mut map = base
            .drain(..)
            .map(|stored| (stored.id().clone(), stored))
            .collect::<BTreeMap<_, _>>();

        for ((c, id), value) in &self.staged {
            if c != collection {
                continue;
            }
            match value {
                TxValue::Set(stored) => {
                    map.insert(id.clone(), stored.clone());
                }
                TxValue::Delete => {
                    map.remove(id);
                }
            }
        }

        let mut values = map.into_values().collect::<Vec<_>>();
        if options.direction() == ScanDirection::Reverse {
            values.reverse();
        }
        if let Some(limit) = options.limit() {
            values.truncate(limit);
        }
        Ok(values)
    }

    fn scan_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        for stored in self.scan(collection, options)? {
            if !visitor(stored)? {
                break;
            }
        }
        Ok(())
    }

    fn count(&self, collection: &CollectionId) -> StorageResult<u64> {
        u64::try_from(self.scan(collection, ScanOptions::default())?.len())
            .map_err(|_| StorageError::backend("GlacierStorage transaction count overflow"))
    }

    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> {
        Ok(self.count(collection)? > 0)
    }

    fn collections(&self) -> StorageResult<Vec<CollectionId>> {
        let mut names = GlacierSnapshot {
            backend: self.backend.clone(),
            generation: self.generation,
        }
        .collections()?;
        for ((collection, _), value) in &self.staged {
            if matches!(value, TxValue::Set(_)) && !names.contains(collection) {
                names.push(collection.clone());
            }
        }
        names.sort();
        names.retain(|collection| self.count(collection).unwrap_or(0) > 0);
        Ok(names)
    }
}

impl StorageTransaction for GlacierTransaction<'_> {
    fn insert( &mut self, collection: &CollectionId, id: DocumentId, document: Arc<Document>, ) -> StorageResult<InsertResult> {
        self.ensure_open()?;
        if self.get(collection, &id)?.is_some() {
            return Err(StorageError::document_already_exists(
                collection.clone(),
                id,
            ));
        }
        let stored = StoredDocument::new(id.clone(), DocumentVersion::INITIAL, document)?;
        self.staged
            .insert((collection.clone(), id), TxValue::Set(stored.clone()));
        self.prepared.push(PreparedMutation::Set {
            collection: collection.clone(),
            stored: stored.clone(),
            previous: None,
        });
        Ok(InsertResult::new(stored))
    }

    fn replace( &mut self, collection: &CollectionId, id: &DocumentId, document: Arc<Document>, precondition: VersionPrecondition, ) -> StorageResult<ReplaceResult> {
        self.ensure_open()?;
        let current = self
            .get(collection, id)?
            .ok_or_else(|| StorageError::document_not_found(collection.clone(), id.clone()))?;
        ensure_precondition(collection, id, current.version(), precondition)?;
        let previous_version = current.version();
        let stored = StoredDocument::new(id.clone(), previous_version.next()?, document)?;
        self.staged.insert(
            (collection.clone(), id.clone()),
            TxValue::Set(stored.clone()),
        );
        self.prepared.push(PreparedMutation::Set {
            collection: collection.clone(),
            stored: stored.clone(),
            previous: Some(current),
        });
        Ok(ReplaceResult::new(previous_version, stored))
    }

    fn delete( &mut self, collection: &CollectionId, id: &DocumentId, precondition: VersionPrecondition, ) -> StorageResult<DeleteResult> {
        self.ensure_open()?;
        let current = self
            .get(collection, id)?
            .ok_or_else(|| StorageError::document_not_found(collection.clone(), id.clone()))?;
        ensure_precondition(collection, id, current.version(), precondition)?;
        self.staged
            .insert((collection.clone(), id.clone()), TxValue::Delete);
        self.prepared.push(PreparedMutation::Delete {
            collection: collection.clone(),
            id: id.clone(),
            previous: current.clone(),
        });
        Ok(DeleteResult::new(id.clone(), current.version()))
    }

    fn commit(mut self: Box<Self>) -> StorageResult<CommitResult> {
        self.ensure_open()?;
        self.closed = true;
        self.backend
            .commit_mutations(self.generation, std::mem::take(&mut self.prepared))
    }

    fn rollback(mut self: Box<Self>) -> StorageResult<()> {
        self.ensure_open()?;
        self.closed = true;
        self.staged.clear();
        self.prepared.clear();
        Ok(())
    }
}

enum PreparedMutation {
    Set {
        collection: CollectionId,
        stored: StoredDocument,
        previous: Option<StoredDocument>,
    },
    Delete {
        collection: CollectionId,
        id: DocumentId,
        previous: StoredDocument,
    },
}

impl PreparedMutation {
    fn data_mutation(&self) -> StorageResult<DataMutation> {
        match self {
            Self::Set {
                collection, stored, ..
            } => Ok(DataMutation::Set {
                collection: collection.as_str().to_owned(),
                document: image_document(stored)?,
            }),
            Self::Delete { collection, id, .. } => Ok(DataMutation::Delete {
                collection: collection.as_str().to_owned(),
                id: *id.as_bytes(),
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum ImageValue {
    Null,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(String),
    Array(Vec<ImageValue>),
    Object(Vec<(String, ImageValue)>),
}

/// Borrowing scalar decoder used by projected analytical scans.
///
/// Physical field payloads are encoded as `ImageValue`. Most analytical
/// projections are scalar, and decoding a string through `ImageValue::String`
/// first allocates a temporary `String` before `Value::String` builds its
/// `Arc<str>`. Borrowing the string directly from the record removes that
/// transient allocation on the hot path. Complex values deliberately fall
/// back to the established owned decoder.
#[derive(Deserialize)]
enum BorrowedProjectedImageValue<'a> {
    Null,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(#[serde(borrow)] &'a str),
    Array(serde::de::IgnoredAny),
    Object(serde::de::IgnoredAny),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ImageField {
    name: String,
    #[serde(with = "byte_blob")]
    value: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ImageDocument {
    id: [u8; 16],
    version: u64,
    fields: Vec<ImageField>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DataRecord {
    generation: u64,
    mutation: DataMutation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum DataMutation {
    Set {
        collection: String,
        document: ImageDocument,
    },
    Delete {
        collection: String,
        id: [u8; 16],
    },
    Clear,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SegmentIndexEntry {
    collection: Option<String>,
    id: [u8; 16],
    version: u64,
    kind: u8,
    relative_offset: u32,
    length: u32,
}

const INDEX_KIND_SET: u8 = 1;
const INDEX_KIND_DELETE: u8 = 2;
const INDEX_KIND_CLEAR: u8 = 3;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SegmentMetadataDelta {
    clear: bool,
    collections: BTreeMap<String, CollectionMetadataDelta>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct CollectionMetadataDelta {
    documents: i64,
    fields: BTreeMap<String, FieldMetadataDelta>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct FieldMetadataDelta {
    present: i64,
    nulls: i64,
    kinds: BTreeMap<String, i64>,
    capabilities: BTreeMap<String, i64>,
}

mod byte_blob {
    use serde::{
        de::{Error as DeError, SeqAccess, Visitor},
        Deserializer, Serializer,
    };
    use std::fmt;

    pub fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(value)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a byte string")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                Ok(value.to_vec())
            }

            fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
            where
                E: DeError,
            {
                Ok(value)
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut value = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
                while let Some(byte) = sequence.next_element::<u8>()? {
                    value.push(byte);
                }
                Ok(value)
            }
        }

        deserializer.deserialize_bytes(BytesVisitor)
    }
}

fn image_document(stored: &StoredDocument) -> StorageResult<ImageDocument> {
    let mut fields = Vec::with_capacity(stored.document().len());
    for (name, value) in stored.document().iter() {
        let bytes = rmp_serde::to_vec(&value_to_image(value)).map_err(|error| {
            StorageError::backend(format!(
                "cannot encode GlacierStorage field {}: {error}",
                name.as_str()
            ))
        })?;
        fields.push(ImageField {
            name: name.as_str().to_owned(),
            value: bytes,
        });
    }
    Ok(ImageDocument {
        id: *stored.id().as_bytes(),
        version: stored.version().get(),
        fields,
    })
}

fn image_document_to_document(document: ImageDocument) -> StorageResult<Document> {
    let mut decoded = Document::new();
    for field in document.fields {
        let value: ImageValue = rmp_serde::from_slice(&field.value).map_err(|error| {
            StorageError::backend(format!(
                "cannot decode GlacierStorage field {}: {error}",
                field.name
            ))
        })?;
        decoded.insert(field.name, image_to_value(value)?);
    }
    Ok(decoded)
}

fn image_document_to_projected_prepared( document: ImageDocument, fields: &[FieldPath], requested_top_level: Option<&BTreeSet<&str>>, ) -> StorageResult<Document> {
    if fields.is_empty() {
        return image_document_to_document(document);
    }

    let requested = requested_top_level.ok_or_else(|| {
        StorageError::backend("GlacierStorage projected scan has no prepared field set")
    })?;
    let mut decoded = Document::new();
    for field in document.fields {
        if !requested.contains(field.name.as_str()) {
            continue;
        }
        let value: ImageValue = rmp_serde::from_slice(&field.value).map_err(|error| {
            StorageError::backend(format!(
                "cannot decode projected GlacierStorage field {}: {error}",
                field.name
            ))
        })?;
        decoded.insert(field.name, image_to_value(value)?);
    }

    Ok(project_document(&decoded, fields))
}

#[derive(Clone, Copy, Debug)]
struct PhysicalSetHeader {
    id: [u8; 16],
    version: u64,
    field_count: usize,
    directory_len: usize,
    payload_len: usize,
}

#[derive(Clone, Copy, Debug)]
struct PhysicalFieldEntry<'a> {
    name: &'a str,
    offset: usize,
    length: usize,
}

fn physical_kind_code(value: &ImageValue) -> u8 {
    match value {
        ImageValue::Null => 0,
        ImageValue::Bool(_) => 1,
        ImageValue::Signed(_) => 2,
        ImageValue::Unsigned(_) => 3,
        ImageValue::Float(_) => 4,
        ImageValue::String(_) => 5,
        ImageValue::Array(_) => 6,
        ImageValue::Object(_) => 7,
    }
}

fn physical_capability_bits(value: &ImageValue) -> StorageResult<u8> {
    let value = image_to_value(value.clone())?;
    let mut bits = 0u8;
    for capability in capabilities_of(&value) {
        bits |= match capability {
            Capability::Comparable => 1 << 0,
            Capability::Summable => 1 << 1,
            Capability::Temporal => 1 << 2,
            Capability::Searchable => 1 << 3,
        };
    }
    Ok(bits)
}

fn encode_physical_set_record(generation: u64, document: &ImageDocument) -> StorageResult<Vec<u8>> {
    let mut directory = Vec::new();
    let mut payloads = Vec::new();

    for field in &document.fields {
        let name = field.name.as_bytes();
        let name_len = u16::try_from(name.len())
            .map_err(|_| StorageError::backend("GlacierStorage field name too long"))?;
        let offset = u32::try_from(payloads.len())
            .map_err(|_| StorageError::backend("GlacierStorage field payload offset overflow"))?;
        let length = u32::try_from(field.value.len())
            .map_err(|_| StorageError::backend("GlacierStorage field payload too large"))?;
        let image: ImageValue = rmp_serde::from_slice(&field.value).map_err(|error| {
            StorageError::backend(format!(
                "cannot inspect GlacierStorage field {} while encoding physical directory: {error}",
                field.name
            ))
        })?;

        directory.extend_from_slice(&name_len.to_be_bytes());
        directory.push(physical_kind_code(&image));
        directory.push(physical_capability_bits(&image)?);
        directory.extend_from_slice(&offset.to_be_bytes());
        directory.extend_from_slice(&length.to_be_bytes());
        directory.extend_from_slice(name);
        payloads.extend_from_slice(&field.value);
    }

    let field_count = u32::try_from(document.fields.len())
        .map_err(|_| StorageError::backend("GlacierStorage document has too many fields"))?;
    let directory_len = u32::try_from(directory.len())
        .map_err(|_| StorageError::backend("GlacierStorage physical field directory too large"))?;
    let payload_len = u32::try_from(payloads.len())
        .map_err(|_| StorageError::backend("GlacierStorage physical field payload too large"))?;
    let checksum = checksum64_pair(&directory, &payloads);

    let total = PHYSICAL_SET_HEADER_BYTES
        .checked_add(directory.len())
        .and_then(|value| value.checked_add(payloads.len()))
        .ok_or_else(|| StorageError::backend("GlacierStorage physical SET record overflow"))?;
    if total > MAX_DATA_RECORD_BYTES {
        return Err(StorageError::backend(format!(
            "GlacierStorage physical SET record is {total} bytes"
        )));
    }

    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&PHYSICAL_SET_MAGIC);
    bytes.extend_from_slice(&PHYSICAL_SET_VERSION.to_be_bytes());
    bytes.extend_from_slice(&(PHYSICAL_SET_HEADER_BYTES as u16).to_be_bytes());
    bytes.extend_from_slice(&generation.to_be_bytes());
    bytes.extend_from_slice(&document.id);
    bytes.extend_from_slice(&document.version.to_be_bytes());
    bytes.extend_from_slice(&field_count.to_be_bytes());
    bytes.extend_from_slice(&directory_len.to_be_bytes());
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(&checksum.to_be_bytes());
    debug_assert_eq!(bytes.len(), PHYSICAL_SET_HEADER_BYTES);
    bytes.extend_from_slice(&directory);
    bytes.extend_from_slice(&payloads);
    Ok(bytes)
}

fn parse_physical_set_header_core( bytes: &[u8], verify_checksum: bool, ) -> StorageResult<Option<PhysicalSetHeader>> {
    if bytes.len() < 8 || bytes[0..8] != PHYSICAL_SET_MAGIC {
        return Ok(None);
    }
    if bytes.len() < PHYSICAL_SET_HEADER_BYTES {
        return Err(StorageError::backend(
            "truncated GlacierStorage physical SET header",
        ));
    }
    let version = u16::from_be_bytes(bytes[8..10].try_into().unwrap());
    let header_len = u16::from_be_bytes(bytes[10..12].try_into().unwrap()) as usize;
    if version != PHYSICAL_SET_VERSION || header_len != PHYSICAL_SET_HEADER_BYTES {
        return Err(StorageError::backend(format!(
            "unsupported GlacierStorage physical SET record version {version}"
        )));
    }
    let _generation = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
    let id = bytes[20..36].try_into().unwrap();
    let document_version = u64::from_be_bytes(bytes[36..44].try_into().unwrap());
    let field_count = u32::from_be_bytes(bytes[44..48].try_into().unwrap()) as usize;
    let directory_len = u32::from_be_bytes(bytes[48..52].try_into().unwrap()) as usize;
    let payload_len = u32::from_be_bytes(bytes[52..56].try_into().unwrap()) as usize;
    let expected = PHYSICAL_SET_HEADER_BYTES
        .checked_add(directory_len)
        .and_then(|value| value.checked_add(payload_len))
        .ok_or_else(|| StorageError::backend("GlacierStorage physical SET size overflow"))?;
    if expected != bytes.len() {
        return Err(StorageError::backend(format!(
            "GlacierStorage physical SET length mismatch: expected {expected}, got {}",
            bytes.len()
        )));
    }

    if verify_checksum {
        let checksum = u64::from_be_bytes(bytes[56..64].try_into().unwrap());
        let directory =
            &bytes[PHYSICAL_SET_HEADER_BYTES..PHYSICAL_SET_HEADER_BYTES + directory_len];
        let payloads = &bytes[PHYSICAL_SET_HEADER_BYTES + directory_len..];
        if checksum64_pair(directory, payloads) != checksum {
            return Err(StorageError::backend(
                "GlacierStorage physical SET checksum mismatch",
            ));
        }
    }

    Ok(Some(PhysicalSetHeader {
        id,
        version: document_version,
        field_count,
        directory_len,
        payload_len,
    }))
}

fn parse_physical_set_header(bytes: &[u8]) -> StorageResult<Option<PhysicalSetHeader>> { parse_physical_set_header_core(bytes, true) }
#[allow(dead_code)] fn parse_trusted_physical_set_header(bytes: &[u8]) -> StorageResult<Option<PhysicalSetHeader>> { parse_physical_set_header_core(bytes, false) }

fn physical_field_entries<'a>( bytes: &'a [u8], header: PhysicalSetHeader, ) -> StorageResult<Vec<PhysicalFieldEntry<'a>>> {
    let directory =
        &bytes[PHYSICAL_SET_HEADER_BYTES..PHYSICAL_SET_HEADER_BYTES + header.directory_len];
    let mut cursor = 0usize;
    let mut entries = Vec::with_capacity(header.field_count);
    while cursor < directory.len() {
        if directory.len().saturating_sub(cursor) < PHYSICAL_FIELD_FIXED_BYTES {
            return Err(StorageError::backend(
                "truncated GlacierStorage physical field directory",
            ));
        }
        let name_len =
            u16::from_be_bytes(directory[cursor..cursor + 2].try_into().unwrap()) as usize;
        let kind = directory[cursor + 2];
        let capabilities = directory[cursor + 3];
        if kind > 7 || capabilities & !0x0f != 0 {
            return Err(StorageError::backend(
                "invalid GlacierStorage physical field metadata",
            ));
        }
        let offset =
            u32::from_be_bytes(directory[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
        let length =
            u32::from_be_bytes(directory[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
        cursor += PHYSICAL_FIELD_FIXED_BYTES;
        let name_end = cursor
            .checked_add(name_len)
            .ok_or_else(|| StorageError::backend("GlacierStorage physical field name overflow"))?;
        if name_end > directory.len() {
            return Err(StorageError::backend(
                "GlacierStorage physical field name outside directory",
            ));
        }
        let name = std::str::from_utf8(&directory[cursor..name_end]).map_err(|_| {
            StorageError::backend("GlacierStorage physical field name is not UTF-8")
        })?;
        cursor = name_end;
        let payload_end = offset.checked_add(length).ok_or_else(|| {
            StorageError::backend("GlacierStorage physical field payload overflow")
        })?;
        if payload_end > header.payload_len {
            return Err(StorageError::backend(
                "GlacierStorage physical field outside payload area",
            ));
        }
        entries.push(PhysicalFieldEntry {
            name,
            offset,
            length,
        });
    }
    if entries.len() != header.field_count {
        return Err(StorageError::backend(
            "GlacierStorage physical field count mismatch",
        ));
    }
    Ok(entries)
}

fn decode_physical_set_document( bytes: &[u8], header: PhysicalSetHeader, ) -> StorageResult<Document> {
    let payload_base = PHYSICAL_SET_HEADER_BYTES + header.directory_len;
    let mut document = Document::new();
    for entry in physical_field_entries(bytes, header)? {
        let start = payload_base + entry.offset;
        let end = start + entry.length;
        let image: ImageValue = rmp_serde::from_slice(&bytes[start..end]).map_err(|error| {
            StorageError::backend(format!(
                "cannot decode GlacierStorage physical field {}: {error}",
                entry.name
            ))
        })?;
        document.insert(entry.name, image_to_value(image)?);
    }
    Ok(document)
}

fn decode_projected_physical_set( bytes: &[u8], header: PhysicalSetHeader, fields: &[FieldPath], requested_top_level: Option<&BTreeSet<&str>>, ) -> StorageResult<(Document, u64)> {
    if fields.is_empty() {
        let document = decode_physical_set_document(bytes, header)?;
        return Ok((document, header.field_count as u64));
    }
    let requested = requested_top_level.ok_or_else(|| {
        StorageError::backend("GlacierStorage projected scan has no prepared field set")
    })?;
    let payload_base = PHYSICAL_SET_HEADER_BYTES + header.directory_len;
    let mut decoded = Document::new();
    let mut decoded_fields = 0u64;
    for entry in physical_field_entries(bytes, header)? {
        if !requested.contains(entry.name) {
            continue;
        }
        let start = payload_base + entry.offset;
        let end = start + entry.length;
        let image: ImageValue = rmp_serde::from_slice(&bytes[start..end]).map_err(|error| {
            StorageError::backend(format!(
                "cannot decode projected GlacierStorage physical field {}: {error}",
                entry.name
            ))
        })?;
        decoded.insert(entry.name, image_to_value(image)?);
        decoded_fields = decoded_fields.saturating_add(1);
    }
    Ok((project_document(&decoded, fields), decoded_fields))
}

fn metadata_delta_for_mutations( mutations: &[PreparedMutation], ) -> StorageResult<SegmentMetadataDelta> {
    let mut delta = SegmentMetadataDelta::default();

    for mutation in mutations {
        match mutation {
            PreparedMutation::Set {
                collection,
                stored,
                previous,
            } => {
                let collection_delta = delta
                    .collections
                    .entry(collection.as_str().to_owned())
                    .or_default();
                if let Some(previous) = previous {
                    accumulate_document_delta(collection_delta, previous.document(), -1, false)?;
                } else {
                    collection_delta.documents =
                        collection_delta.documents.checked_add(1).ok_or_else(|| {
                            StorageError::backend("GlacierStorage metadata delta overflow")
                        })?;
                }
                accumulate_document_delta(collection_delta, stored.document(), 1, false)?;
            }
            PreparedMutation::Delete {
                collection,
                previous,
                ..
            } => {
                let collection_delta = delta
                    .collections
                    .entry(collection.as_str().to_owned())
                    .or_default();
                collection_delta.documents =
                    collection_delta.documents.checked_sub(1).ok_or_else(|| {
                        StorageError::backend("GlacierStorage metadata delta overflow")
                    })?;
                accumulate_document_delta(collection_delta, previous.document(), -1, false)?;
            }
        }
    }

    Ok(delta)
}

fn accumulate_document_delta( delta: &mut CollectionMetadataDelta, document: &Document, sign: i64, count_document: bool, ) -> StorageResult<()> {
    if count_document {
        delta.documents = delta
            .documents
            .checked_add(sign)
            .ok_or_else(|| StorageError::backend("GlacierStorage metadata delta overflow"))?;
    }
    accumulate_observation_delta(&mut delta.fields, "", document, sign)
}

fn accumulate_observation_delta( fields: &mut BTreeMap<String, FieldMetadataDelta>, prefix: &str, document: &Document, sign: i64, ) -> StorageResult<()> {
    for (name, value) in document.iter() {
        let path = if prefix.is_empty() {
            name.as_str().to_owned()
        } else {
            format!("{prefix}.{}", name.as_str())
        };
        let field = fields.entry(path.clone()).or_default();
        field.present = field
            .present
            .checked_add(sign)
            .ok_or_else(|| StorageError::backend("GlacierStorage metadata delta overflow"))?;
        if value.is_null() {
            field.nulls = field
                .nulls
                .checked_add(sign)
                .ok_or_else(|| StorageError::backend("GlacierStorage metadata delta overflow"))?;
        }
        add_i64_counter(
            &mut field.kinds,
            value.physical_kind().as_str().to_owned(),
            sign,
        )?;
        for capability in capabilities_of(value) {
            add_i64_counter(
                &mut field.capabilities,
                capability.as_str().to_owned(),
                sign,
            )?;
        }
        if let Value::Object(object) = value {
            accumulate_observation_delta(fields, &path, object, sign)?;
        }
    }
    Ok(())
}

fn add_i64_counter( counters: &mut BTreeMap<String, i64>, name: String, delta: i64, ) -> StorageResult<()> {
    let value = counters.entry(name.clone()).or_default();
    *value = (*value)
        .checked_add(delta)
        .ok_or_else(|| StorageError::backend("GlacierStorage metadata delta overflow"))?;
    if *value == 0 {
        counters.remove(&name);
    }
    Ok(())
}

fn apply_metadata_delta( catalog: &mut FieldCatalog, delta: &SegmentMetadataDelta, ) -> StorageResult<()> {
    if delta.clear {
        catalog.collections.clear();
        return Ok(());
    }

    for (collection_name, collection_delta) in &delta.collections {
        let stats = catalog
            .collections
            .entry(collection_name.clone())
            .or_default();

        stats.documents =
            apply_signed_u64(stats.documents, collection_delta.documents, "document")?;

        for (path, field_delta) in &collection_delta.fields {
            let field = stats.fields.entry(path.clone()).or_default();
            field.present = apply_signed_u64(field.present, field_delta.present, "field present")?;
            field.nulls = apply_signed_u64(field.nulls, field_delta.nulls, "field null")?;
            apply_signed_named_counters(&mut field.kinds, &field_delta.kinds)?;
            apply_signed_named_counters(&mut field.capabilities, &field_delta.capabilities)?;
        }

        stats.fields.retain(|_, field| field.present > 0);
    }

    catalog.collections.retain(|_, stats| stats.documents > 0);
    Ok(())
}

fn apply_signed_u64(value: u64, delta: i64, label: &str) -> StorageResult<u64> {
    if delta >= 0 {
        value.checked_add(delta as u64).ok_or_else(|| {
            StorageError::backend(format!("GlacierStorage metadata {label} overflow"))
        })
    } else {
        value.checked_sub(delta.unsigned_abs()).ok_or_else(|| {
            StorageError::backend(format!("GlacierStorage metadata {label} underflow"))
        })
    }
}

fn apply_signed_named_counters( counters: &mut BTreeMap<String, u64>, deltas: &BTreeMap<String, i64>, ) -> StorageResult<()> {
    for (name, delta) in deltas {
        let current = counters.get(name).copied().unwrap_or(0);
        let updated = apply_signed_u64(current, *delta, "capability/type")?;
        if updated == 0 {
            counters.remove(name);
        } else {
            counters.insert(name.clone(), updated);
        }
    }
    Ok(())
}

fn append_committed_data_records( path: &Path, generation: u64, records: Vec<DataRecord>, metadata_delta: &SegmentMetadataDelta, metrics: Option<&GlacierWriteMetrics>, ) -> StorageResult<Vec<RecordPointer>> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let encode_started = Instant::now();
    let mut encoded = Vec::with_capacity(records.len());
    let mut entries = Vec::with_capacity(records.len());
    let mut records_bytes = 0usize;

    for record in &records {
        let bytes = match &record.mutation {
            DataMutation::Set { document, .. } => {
                encode_physical_set_record(record.generation, document)?
            }
            DataMutation::Delete { .. } | DataMutation::Clear => rmp_serde::to_vec(record)
                .map_err(|error| {
                    StorageError::backend(format!(
                        "cannot encode GlacierStorage data record: {error}"
                    ))
                })?,
        };
        if bytes.is_empty() || bytes.len() > MAX_DATA_RECORD_BYTES {
            return Err(StorageError::backend(format!(
                "GlacierStorage data record is {} bytes",
                bytes.len()
            )));
        }

        let relative_offset = u32::try_from(records_bytes)
            .map_err(|_| StorageError::backend("GlacierStorage segment offset overflow"))?;
        let length = u32::try_from(bytes.len())
            .map_err(|_| StorageError::backend("GlacierStorage data record too large"))?;

        let entry = match &record.mutation {
            DataMutation::Set {
                collection,
                document,
            } => SegmentIndexEntry {
                collection: Some(collection.clone()),
                id: document.id,
                version: document.version,
                kind: INDEX_KIND_SET,
                relative_offset,
                length,
            },
            DataMutation::Delete { collection, id } => SegmentIndexEntry {
                collection: Some(collection.clone()),
                id: *id,
                version: 0,
                kind: INDEX_KIND_DELETE,
                relative_offset,
                length,
            },
            DataMutation::Clear => SegmentIndexEntry {
                collection: None,
                id: [0u8; 16],
                version: 0,
                kind: INDEX_KIND_CLEAR,
                relative_offset,
                length,
            },
        };

        records_bytes = records_bytes
            .checked_add(bytes.len())
            .ok_or_else(|| StorageError::backend("GlacierStorage segment payload overflow"))?;
        entries.push(entry);
        encoded.push(bytes);
    }

    let directory = rmp_serde::to_vec(&entries).map_err(|error| {
        StorageError::backend(format!(
            "cannot encode GlacierStorage segment directory: {error}"
        ))
    })?;
    let metadata = rmp_serde::to_vec(metadata_delta).map_err(|error| {
        StorageError::backend(format!(
            "cannot encode GlacierStorage segment metadata: {error}"
        ))
    })?;

    if directory.len() > MAX_SEGMENT_DIRECTORY_BYTES {
        return Err(StorageError::backend(
            "GlacierStorage segment directory is too large",
        ));
    }
    if metadata.len() > MAX_SEGMENT_METADATA_BYTES {
        return Err(StorageError::backend(
            "GlacierStorage segment metadata is too large",
        ));
    }

    let total_payload = directory
        .len()
        .checked_add(metadata.len())
        .and_then(|value| value.checked_add(records_bytes))
        .ok_or_else(|| StorageError::backend("GlacierStorage segment length overflow"))?;
    if total_payload == 0 || total_payload > MAX_SEGMENT_BYTES {
        return Err(StorageError::backend(format!(
            "GlacierStorage segment is {total_payload} bytes"
        )));
    }

    let record_count = u32::try_from(records.len())
        .map_err(|_| StorageError::backend("GlacierStorage segment has too many records"))?;
    let directory_len = u32::try_from(directory.len())
        .map_err(|_| StorageError::backend("GlacierStorage directory too large"))?;
    let metadata_len = u32::try_from(metadata.len())
        .map_err(|_| StorageError::backend("GlacierStorage metadata too large"))?;
    let records_len = u32::try_from(records_bytes)
        .map_err(|_| StorageError::backend("GlacierStorage records too large"))?;

    let directory_meta_checksum = checksum64_pair(&directory, &metadata);
    let records_checksum = encoded
        .iter()
        .fold(0xcbf2_9ce4_8422_2325u64, |hash, bytes| {
            checksum64_continue(hash, bytes)
        });

    let mut header = [0u8; SEGMENT_HEADER_BYTES];
    header[0..8].copy_from_slice(&SEGMENT_MAGIC);
    header[8..16].copy_from_slice(&generation.to_be_bytes());
    header[16..20].copy_from_slice(&record_count.to_be_bytes());
    header[20..24].copy_from_slice(&directory_len.to_be_bytes());
    header[24..28].copy_from_slice(&metadata_len.to_be_bytes());
    header[28..32].copy_from_slice(&records_len.to_be_bytes());
    header[32..40].copy_from_slice(&directory_meta_checksum.to_be_bytes());
    header[40..48].copy_from_slice(&records_checksum.to_be_bytes());

    let mut frame = Vec::with_capacity(SEGMENT_HEADER_BYTES + total_payload);
    frame.extend_from_slice(&header);
    frame.extend_from_slice(&directory);
    frame.extend_from_slice(&metadata);
    for bytes in &encoded {
        frame.extend_from_slice(bytes);
    }

    if let Some(metrics) = metrics {
        metrics
            .data_encode_us
            .fetch_add(elapsed_micros(encode_started), Ordering::Relaxed);
    }

    let write_started = Instant::now();
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(path)
        .map_err(io_error("append segment", path))?;
    let segment_start = file.metadata().map_err(io_error("stat data", path))?.len();

    file.write_all(&frame)
        .map_err(io_error("write Glacier segment", path))?;

    if let Some(metrics) = metrics {
        metrics
            .data_write_us
            .fetch_add(elapsed_micros(write_started), Ordering::Relaxed);
    }

    let sync_started = Instant::now();
    file.sync_data()
        .map_err(io_error("sync Glacier segment", path))?;
    if let Some(metrics) = metrics {
        metrics
            .data_sync_us
            .fetch_add(elapsed_micros(sync_started), Ordering::Relaxed);
    }

    let records_base = segment_start
        .checked_add(SEGMENT_HEADER_BYTES as u64)
        .and_then(|value| value.checked_add(directory.len() as u64))
        .and_then(|value| value.checked_add(metadata.len() as u64))
        .ok_or_else(|| StorageError::backend("GlacierStorage segment pointer overflow"))?;

    entries
        .iter()
        .map(|entry| {
            Ok(RecordPointer {
                offset: records_base
                    .checked_add(entry.relative_offset as u64)
                    .ok_or_else(|| {
                        StorageError::backend("GlacierStorage record pointer overflow")
                    })?,
                length: entry.length,
            })
        })
        .collect()
}

fn checksum64_pair(first: &[u8], second: &[u8]) -> u64 { checksum64_continue(checksum64_continue(0xcbf2_9ce4_8422_2325, first), second) }

#[derive(Debug)]
struct SegmentCatalogEntry {
    start: u64,
    generation: u64,
    record_count: u32,
    directory_len: u32,
    metadata_len: u32,
    records_len: u32,
    directory_checksum: u64,
    records_checksum: u64,
    directory_verified: AtomicBool,
    records_verified: AtomicBool,
    insert_collection: OnceLock<Option<Arc<str>>>,
    physical_sets_only: OnceLock<bool>,
}

impl SegmentCatalogEntry {
    fn new(
        start: u64,
        generation: u64,
        record_count: u32,
        directory_len: u32,
        metadata_len: u32,
        records_len: u32,
        directory_checksum: u64,
        records_checksum: u64,
        directory_verified: bool,
        insert_collection: Option<Option<Arc<str>>>,
    ) -> Self {
        let entry = Self {
            start,
            generation,
            record_count,
            directory_len,
            metadata_len,
            records_len,
            directory_checksum,
            records_checksum,
            directory_verified: AtomicBool::new(directory_verified),
            records_verified: AtomicBool::new(false),
            insert_collection: OnceLock::new(),
            physical_sets_only: OnceLock::new(),
        };
        if let Some(value) = insert_collection {
            let _ = entry.insert_collection.set(value);
        }
        entry
    }

    fn clone_cached(&self) -> Self {
        let entry = Self::new(
            self.start,
            self.generation,
            self.record_count,
            self.directory_len,
            self.metadata_len,
            self.records_len,
            self.directory_checksum,
            self.records_checksum,
            self.directory_verified.load(Ordering::Acquire),
            self.insert_collection.get().cloned(),
        );
        entry.records_verified.store(
            self.records_verified.load(Ordering::Acquire),
            Ordering::Release,
        );
        if let Some(value) = self.physical_sets_only.get().copied() {
            let _ = entry.physical_sets_only.set(value);
        }
        entry
    }

    #[inline]
    fn record_count(&self) -> usize { self.record_count as usize }
    #[inline]
    fn directory_len(&self) -> usize { self.directory_len as usize }
    #[inline]
    fn metadata_len(&self) -> usize { self.metadata_len as usize }
    #[inline]
    fn records_len(&self) -> usize { self.records_len as usize }

    fn end(&self) -> StorageResult<u64> {
        self.start
            .checked_add(SEGMENT_HEADER_BYTES as u64)
            .and_then(|value| value.checked_add(self.directory_len as u64))
            .and_then(|value| value.checked_add(self.metadata_len as u64))
            .and_then(|value| value.checked_add(self.records_len as u64))
            .ok_or_else(|| StorageError::backend("GlacierStorage catalog segment overflow"))
    }

    #[inline]
    fn known_insert_collection(&self) -> Option<&str> {
        self.insert_collection
            .get()
            .and_then(|value| value.as_deref())
    }

    fn proves_target_inserts(
        &self,
        metadata: &[u8],
        collection: &CollectionId,
    ) -> StorageResult<bool> {
        if self.insert_collection.get().is_none() {
            let delta: SegmentMetadataDelta = rmp_serde::from_slice(metadata).map_err(|error| {
                StorageError::backend(format!(
                    "cannot decode GlacierStorage segment metadata for catalog: {error}"
                ))
            })?;
            let proof = segment_insert_collection_from_delta(&delta, self.record_count())
                .map(Arc::<str>::from);
            let _ = self.insert_collection.set(proof);
        }
        Ok(self.known_insert_collection() == Some(collection.as_str()))
    }

    fn verify_directory_checksum(&self, directory: &[u8], metadata: &[u8]) -> StorageResult<bool> {
        if self.directory_verified.load(Ordering::Acquire) {
            return Ok(false);
        }
        if checksum64_pair(directory, metadata) != self.directory_checksum {
            return Err(StorageError::backend(
                "GlacierStorage segment directory checksum mismatch",
            ));
        }
        self.directory_verified.store(true, Ordering::Release);
        Ok(true)
    }

    fn verify_records_checksum(&self, records: &[u8]) -> StorageResult<bool> {
        if self.records_verified.load(Ordering::Acquire) {
            return Ok(false);
        }
        if checksum64(records) != self.records_checksum {
            return Err(StorageError::backend(
                "GlacierStorage segment records checksum mismatch",
            ));
        }
        self.records_verified.store(true, Ordering::Release);
        Ok(true)
    }

    fn proves_physical_sets(&self, records: &[u8]) -> StorageResult<bool> {
        if let Some(value) = self.physical_sets_only.get().copied() {
            return Ok(value);
        }
        let value = records_are_all_physical_sets(records, self.record_count())?;
        let _ = self.physical_sets_only.set(value);
        Ok(value)
    }
}

#[derive(Debug)]
struct SegmentCatalogSnapshot {
    file_len: u64,
    segments: Arc<[SegmentCatalogEntry]>,
}

fn segment_insert_collection_from_delta(
    delta: &SegmentMetadataDelta,
    record_count: usize,
) -> Option<&str> {
    if delta.clear || delta.collections.len() != 1 {
        return None;
    }
    let (collection, collection_delta) = delta.collections.iter().next()?;
    let expected = i64::try_from(record_count).ok()?;
    (collection_delta.documents == expected).then_some(collection.as_str())
}

fn catalog_entry_from_header(
    start: u64,
    header: &[u8; SEGMENT_HEADER_BYTES],
    directory_verified: bool,
    insert_collection: Option<Option<Arc<str>>>,
) -> StorageResult<SegmentCatalogEntry> {
    if header[0..8] != SEGMENT_MAGIC {
        return Err(StorageError::backend(
            "invalid GlacierStorage segment magic while building catalog",
        ));
    }
    let generation = u64::from_be_bytes(header[8..16].try_into().unwrap());
    let record_count = u32::from_be_bytes(header[16..20].try_into().unwrap());
    let directory_len = u32::from_be_bytes(header[20..24].try_into().unwrap());
    let metadata_len = u32::from_be_bytes(header[24..28].try_into().unwrap());
    let records_len = u32::from_be_bytes(header[28..32].try_into().unwrap());
    validate_segment_lengths(
        record_count as usize,
        directory_len as usize,
        metadata_len as usize,
        records_len as usize,
    )?;
    Ok(SegmentCatalogEntry::new(
        start,
        generation,
        record_count,
        directory_len,
        metadata_len,
        records_len,
        u64::from_be_bytes(header[32..40].try_into().unwrap()),
        u64::from_be_bytes(header[40..48].try_into().unwrap()),
        directory_verified,
        insert_collection,
    ))
}

fn read_segment_catalog_headers(
    path: &Path,
    start: u64,
    end: u64,
    mut expected_generation: u64,
) -> StorageResult<Vec<SegmentCatalogEntry>> {
    if start > end || start < GLACIER_SUPERBLOCK_BYTES as u64 {
        return Err(StorageError::backend(
            "GlacierStorage catalog range is outside the data file",
        ));
    }
    let mut file = File::open(path).map_err(io_error("open segment catalog", path))?;
    let mut cursor = start;
    let mut entries = Vec::new();
    while cursor < end {
        if end - cursor < SEGMENT_HEADER_BYTES as u64 {
            return Err(StorageError::backend(
                "GlacierStorage catalog encountered a truncated segment header",
            ));
        }
        file.seek(SeekFrom::Start(cursor))
            .map_err(io_error("seek segment catalog", path))?;
        let mut header = [0u8; SEGMENT_HEADER_BYTES];
        file.read_exact(&mut header)
            .map_err(io_error("read segment catalog header", path))?;
        let entry = catalog_entry_from_header(cursor, &header, false, None)?;
        if entry.generation != expected_generation {
            return Err(StorageError::backend(format!(
                "GlacierStorage catalog generation gap: expected {expected_generation}, got {}",
                entry.generation
            )));
        }
        expected_generation = expected_generation.saturating_add(1);
        let next = entry.end()?;
        if next > end {
            return Err(StorageError::backend(
                "GlacierStorage catalog segment extends beyond snapshot",
            ));
        }
        entries.push(entry);
        cursor = next;
    }
    if cursor != end {
        return Err(StorageError::backend(
            "GlacierStorage catalog snapshot is not segment aligned",
        ));
    }
    Ok(entries)
}

fn prepare_segment_catalog(
    path: &Path,
    file_len: u64,
    cache: &Mutex<Arc<SegmentCatalogSnapshot>>,
    metrics: &GlacierReadMetrics,
) -> StorageResult<Arc<SegmentCatalogSnapshot>> {
    let mut guard = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if guard.file_len == file_len {
        metrics.segment_catalog_hits.fetch_add(1, Ordering::Relaxed);
        return Ok(Arc::clone(&guard));
    }

    let started = Instant::now();
    let snapshot = if guard.file_len < file_len {
        let mut entries = guard
            .segments
            .iter()
            .map(SegmentCatalogEntry::clone_cached)
            .collect::<Vec<_>>();
        let expected_generation = guard
            .segments
            .last()
            .map(|entry| entry.generation.saturating_add(1))
            .unwrap_or(1);
        entries.extend(read_segment_catalog_headers(
            path,
            guard.file_len,
            file_len,
            expected_generation,
        )?);
        metrics
            .segment_catalog_refreshes
            .fetch_add(1, Ordering::Relaxed);
        Arc::new(SegmentCatalogSnapshot {
            file_len,
            segments: Arc::from(entries),
        })
    } else {
        let entries = read_segment_catalog_headers(
            path,
            GLACIER_SUPERBLOCK_BYTES as u64,
            file_len,
            1,
        )?;
        metrics
            .segment_catalog_rebuilds
            .fetch_add(1, Ordering::Relaxed);
        Arc::new(SegmentCatalogSnapshot {
            file_len,
            segments: Arc::from(entries),
        })
    };
    metrics
        .segment_catalog_refresh_us
        .fetch_add(elapsed_micros(started), Ordering::Relaxed);
    *guard = Arc::clone(&snapshot);
    Ok(snapshot)
}

fn checksum64_continue(mut hash: u64, bytes: &[u8]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    for byte in bytes {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(PRIME);
    }
    hash
}

// Tiny segments are cheaper to copy through the existing buffered path than to
// route through the mmap snapshot machinery. Larger immutable payloads avoid
// the directory/metadata/records Vec allocations and their page-cache-to-userspace
// copy. The threshold is deliberately architecture-neutral and easy to tune
// from read metrics after benchmarking.
const MIN_MMAP_SEGMENT_PAYLOAD_BYTES: usize = 64 * 1024;

enum SegmentPayload<'a> {
    Mapped {
        payload: &'a [u8],
        directory_len: usize,
        metadata_len: usize,
    },
    Buffered {
        directory: Vec<u8>,
        metadata: Vec<u8>,
        records: Vec<u8>,
    },
}

impl SegmentPayload<'_> {
    #[inline]
    fn directory(&self) -> &[u8] {
        match self {
            Self::Mapped {
                payload,
                directory_len,
                ..
            } => &payload[..*directory_len],
            Self::Buffered { directory, .. } => directory,
        }
    }

    #[inline]
    fn metadata(&self) -> &[u8] {
        match self {
            Self::Mapped {
                payload,
                directory_len,
                metadata_len,
            } => {
                let start = *directory_len;
                &payload[start..start + *metadata_len]
            }
            Self::Buffered { metadata, .. } => metadata,
        }
    }

    #[inline]
    fn records(&self) -> &[u8] {
        match self {
            Self::Mapped {
                payload,
                directory_len,
                metadata_len,
            } => &payload[*directory_len + *metadata_len..],
            Self::Buffered { records, .. } => records,
        }
    }
}

/// Returns the persistent read-only mapping for the current Glacier file snapshot.
///
/// Reusing the same mapping across scans is essential: recreating a VMA for each
/// analytical scan forces Linux to rebuild page-table entries and pay minor-fault
/// cost again even when the file is already hot in the kernel page cache. The
/// cache is refreshed lazily when append-only writes grow the file. In-flight
/// scans keep their previous `Arc`, so remapping never invalidates borrowed bytes.
///
/// Mapping remains disabled on non-64-bit targets. Mapping failure is non-fatal:
/// eligible segments transparently use the buffered path instead.
fn prepare_scan_mmap(
    path: &Path,
    file_len: u64,
    cache: &Mutex<Option<(u64, Arc<GlacierReadOnlyMap>)>>,
    metrics: &GlacierReadMetrics,
) -> Option<Arc<GlacierReadOnlyMap>> {
    if file_len < MIN_MMAP_SEGMENT_PAYLOAD_BYTES as u64 || !GlacierReadOnlyMap::supported() {
        return None;
    }

    if let Ok(guard) = cache.lock() {
        if let Some((cached_len, mapped)) = guard.as_ref() {
            if *cached_len == file_len {
                metrics.mmap_reuses.fetch_add(1, Ordering::Relaxed);
                return Some(Arc::clone(mapped));
            }
        }
    }

    let mmap_started = Instant::now();
    let mapped = GlacierReadOnlyMap::map(path, file_len).ok().map(Arc::new);
    metrics
        .mmap_us
        .fetch_add(elapsed_micros(mmap_started), Ordering::Relaxed);

    let Some(mapped) = mapped else {
        return None;
    };

    if let Ok(mut guard) = cache.lock() {
        if let Some((cached_len, cached)) = guard.as_ref() {
            if *cached_len == file_len {
                metrics.mmap_reuses.fetch_add(1, Ordering::Relaxed);
                return Some(Arc::clone(cached));
            }
        }

        if guard.is_some() {
            metrics.mmap_remaps.fetch_add(1, Ordering::Relaxed);
        } else {
            metrics.mmap_map_creates.fetch_add(1, Ordering::Relaxed);
        }
        *guard = Some((file_len, Arc::clone(&mapped)));
    } else {
        metrics.mmap_map_creates.fetch_add(1, Ordering::Relaxed);
    }

    Some(mapped)
}

fn read_segment_payload<'a>(
    file: &mut File,
    path: &Path,
    segment_start: u64,
    file_len: u64,
    directory_len: usize,
    metadata_len: usize,
    records_len: usize,
    mmap: Option<&'a GlacierReadOnlyMap>,
    metrics: &GlacierReadMetrics,
) -> StorageResult<SegmentPayload<'a>> {
    let payload_len = directory_len
        .checked_add(metadata_len)
        .and_then(|value| value.checked_add(records_len))
        .ok_or_else(|| StorageError::backend("GlacierStorage segment payload overflow"))?;
    let payload_offset = segment_start
        .checked_add(SEGMENT_HEADER_BYTES as u64)
        .ok_or_else(|| StorageError::backend("GlacierStorage segment payload offset overflow"))?;
    let payload_end = payload_offset
        .checked_add(payload_len as u64)
        .ok_or_else(|| StorageError::backend("GlacierStorage segment payload end overflow"))?;
    if payload_end > file_len {
        return Err(StorageError::backend(
            "GlacierStorage segment payload extends beyond the scan file snapshot",
        ));
    }

    if payload_len >= MIN_MMAP_SEGMENT_PAYLOAD_BYTES {
        if let Some(mmap) = mmap {
            if let Ok(payload) = mmap.slice(payload_offset, payload_len) {
                metrics.mmap_segments.fetch_add(1, Ordering::Relaxed);
                metrics
                    .mmap_bytes
                    .fetch_add(usize_to_u64_saturating(payload_len), Ordering::Relaxed);
                return Ok(SegmentPayload::Mapped {
                    payload,
                    directory_len,
                    metadata_len,
                });
            }
        }
        metrics
            .mmap_fallback_segments
            .fetch_add(1, Ordering::Relaxed);
    } else {
        metrics
            .mmap_bypass_segments
            .fetch_add(1, Ordering::Relaxed);
    }

    let mut directory = vec![0u8; directory_len];
    let mut metadata = vec![0u8; metadata_len];
    let mut records = vec![0u8; records_len];
    let io_started = Instant::now();
    file.seek(SeekFrom::Start(payload_offset))
        .map_err(io_error("seek Glacier segment payload", path))?;
    file.read_exact(&mut directory)
        .map_err(io_error("read Glacier segment directory", path))?;
    file.read_exact(&mut metadata)
        .map_err(io_error("read Glacier segment metadata", path))?;
    file.read_exact(&mut records)
        .map_err(io_error("read Glacier segment records", path))?;
    metrics
        .io_us
        .fetch_add(elapsed_micros(io_started), Ordering::Relaxed);

    Ok(SegmentPayload::Buffered {
        directory,
        metadata,
        records,
    })
}

fn read_stored_document_profiled( path: &Path, pointer: RecordPointer, metrics: &GlacierReadMetrics, ) -> StorageResult<StoredDocument> {
    let open_started = Instant::now();
    let mut file = File::open(path).map_err(io_error("open data", path))?;
    metrics
        .pointer_open_us
        .fetch_add(elapsed_micros(open_started), Ordering::Relaxed);
    read_stored_document_from_file_profiled(&mut file, path, pointer, metrics)
}

fn read_stored_document_from_file_profiled( file: &mut File, path: &Path, pointer: RecordPointer, metrics: &GlacierReadMetrics, ) -> StorageResult<StoredDocument> {
    let total_started = Instant::now();
    metrics.pointer_loads.fetch_add(1, Ordering::Relaxed);
    metrics
        .pointer_payload_bytes
        .fetch_add(pointer.length as u64, Ordering::Relaxed);

    let seek_started = Instant::now();
    file.seek(SeekFrom::Start(pointer.offset))
        .map_err(io_error("seek data", path))?;
    metrics
        .pointer_seek_us
        .fetch_add(elapsed_micros(seek_started), Ordering::Relaxed);

    let alloc_started = Instant::now();
    let mut payload = vec![0u8; pointer.length as usize];
    metrics
        .pointer_alloc_us
        .fetch_add(elapsed_micros(alloc_started), Ordering::Relaxed);
    let read_started = Instant::now();
    file.read_exact(&mut payload)
        .map_err(io_error("read data payload", path))?;
    metrics
        .pointer_read_us
        .fetch_add(elapsed_micros(read_started), Ordering::Relaxed);

    let header_started = Instant::now();
    let physical_header = parse_physical_set_header(&payload)?;
    metrics
        .pointer_header_us
        .fetch_add(elapsed_micros(header_started), Ordering::Relaxed);

    let result = if let Some(header) = physical_header {
        metrics
            .pointer_physical_records
            .fetch_add(1, Ordering::Relaxed);
        let decode_started = Instant::now();
        let id = DocumentId::from_bytes(header.id);
        let version = DocumentVersion::new(header.version);
        let document = decode_physical_set_document(&payload, header)?;
        metrics
            .pointer_physical_decode_us
            .fetch_add(elapsed_micros(decode_started), Ordering::Relaxed);
        let build_started = Instant::now();
        let stored = StoredDocument::new(id, version, Arc::new(document));
        metrics
            .pointer_build_us
            .fetch_add(elapsed_micros(build_started), Ordering::Relaxed);
        stored
    } else {
        metrics
            .pointer_legacy_records
            .fetch_add(1, Ordering::Relaxed);
        let decode_started = Instant::now();
        let record: DataRecord = rmp_serde::from_slice(&payload).map_err(|error| {
            StorageError::backend(format!("cannot decode GlacierStorage data record: {error}"))
        })?;
        let DataMutation::Set { document, .. } = record.mutation else {
            return Err(StorageError::backend(
                "GlacierStorage primary index points to a non-document record",
            ));
        };
        let id = DocumentId::from_bytes(document.id);
        let version = DocumentVersion::new(document.version);
        let document = image_document_to_document(document)?;
        metrics
            .pointer_legacy_decode_us
            .fetch_add(elapsed_micros(decode_started), Ordering::Relaxed);
        let build_started = Instant::now();
        let stored = StoredDocument::new(id, version, Arc::new(document));
        metrics
            .pointer_build_us
            .fetch_add(elapsed_micros(build_started), Ordering::Relaxed);
        stored
    };
    metrics
        .pointer_total_us
        .fetch_add(elapsed_micros(total_started), Ordering::Relaxed);
    result
}



fn scan_collection_sequential(
    path: &Path,
    state: &GlacierState,
    collection_index: &CollectionIndex,
    collection: &CollectionId,
    generation: u64,
    fields: &[FieldPath],
    catalog_cache: &Mutex<Arc<SegmentCatalogSnapshot>>,
    mmap_cache: &Mutex<Option<(u64, Arc<GlacierReadOnlyMap>)>>,
    metrics: &GlacierReadMetrics,
    visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>,
) -> StorageResult<()> {
    let mut scan_metrics = GlacierReadScanGuard::new(metrics);
    let expected = visible_count(collection_index, generation) as usize;
    if expected == 0 {
        return Ok(());
    }

    let requested_top_level = if fields.is_empty() {
        None
    } else {
        Some(
            fields
                .iter()
                .map(|path| path.first().as_str())
                .collect::<BTreeSet<_>>(),
        )
    };

    let io_started = Instant::now();
    let mut file = File::open(path).map_err(io_error("open sequential scan", path))?;
    let length = file
        .metadata()
        .map_err(io_error("stat sequential scan", path))?
        .len();
    metrics
        .io_us
        .fetch_add(elapsed_micros(io_started), Ordering::Relaxed);
    let mmap = prepare_scan_mmap(path, length, mmap_cache, metrics);
    let catalog = prepare_segment_catalog(path, length, catalog_cache, metrics)?;
    let mut emitted = 0usize;
    // Reused for every physical record. Only the Value payloads themselves are
    // replaced; the projection container allocation is paid once per scan.

    for segment in catalog.segments.iter() {
        if emitted >= expected || segment.generation > generation {
            break;
        }
        if let Some(other_collection) = segment.known_insert_collection() {
            if other_collection != collection.as_str() {
                metrics
                    .segment_catalog_skipped_segments
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
        }
        let segment_start = segment.start;
        let record_count = segment.record_count();
        let directory_len = segment.directory_len();
        let metadata_len = segment.metadata_len();
        let records_len = segment.records_len();
        scan_metrics.segments = scan_metrics.segments.saturating_add(1);

        let payload = read_segment_payload(
            &mut file,
            path,
            segment_start,
            length,
            directory_len,
            metadata_len,
            records_len,
            mmap.as_deref(),
            metrics,
        )?;
        let directory = payload.directory();
        let metadata = payload.metadata();
        let records = payload.records();

        let _ = segment.verify_directory_checksum(directory, metadata)?;
        let _ = segment.verify_records_checksum(records)?;

        let decode_started = Instant::now();
        let entries: Vec<SegmentIndexEntry> =
            rmp_serde::from_slice(directory).map_err(|error| {
                StorageError::backend(format!(
                    "cannot decode GlacierStorage segment directory: {error}"
                ))
            })?;
        let directory_decode_us = elapsed_micros(decode_started);
        metrics
            .decode_us
            .fetch_add(directory_decode_us, Ordering::Relaxed);
        metrics
            .directory_decode_us
            .fetch_add(directory_decode_us, Ordering::Relaxed);
        if entries.len() != record_count {
            return Err(StorageError::backend(
                "GlacierStorage segment directory count mismatch",
            ));
        }

        let records_base = segment_start
            .checked_add(SEGMENT_HEADER_BYTES as u64)
            .and_then(|value| value.checked_add(directory_len as u64))
            .and_then(|value| value.checked_add(metadata_len as u64))
            .ok_or_else(|| StorageError::backend("GlacierStorage scan pointer overflow"))?;

        for entry in entries {
            if entry.kind != INDEX_KIND_SET {
                continue;
            }
            if entry.collection.as_deref() != Some(collection.as_str()) {
                continue;
            }

            let id = DocumentId::from_bytes(entry.id);
            let Some(version) = collection_index.visible_version(state, generation, &id) else {
                continue;
            };
            let Some(pointer) = version.pointer else {
                continue;
            };
            let absolute = records_base
                .checked_add(entry.relative_offset as u64)
                .ok_or_else(|| StorageError::backend("GlacierStorage scan offset overflow"))?;
            if pointer.offset != absolute || pointer.length != entry.length {
                continue;
            }

            let start = entry.relative_offset as usize;
            let end = start
                .checked_add(entry.length as usize)
                .ok_or_else(|| StorageError::backend("GlacierStorage scan record overflow"))?;
            if entry.length == 0 || end > records.len() {
                return Err(StorageError::backend(
                    "GlacierStorage scan record outside segment",
                ));
            }

            scan_metrics.records = scan_metrics.records.saturating_add(1);
            let decode_started = scan_metrics.sampled_timer();
            let record_bytes = &records[start..end];

            let (decoded, decoded_fields) = if let Some(header) =
                parse_physical_set_header(record_bytes)?
            {
                if header.id != entry.id || header.version != entry.version {
                    return Err(StorageError::backend(
                        "GlacierStorage physical SET header disagrees with segment directory",
                    ));
                }
                decode_projected_physical_set(
                    record_bytes,
                    header,
                    fields,
                    requested_top_level.as_ref(),
                )?
            } else {
                let record: DataRecord = rmp_serde::from_slice(record_bytes).map_err(|error| {
                    StorageError::backend(format!(
                        "cannot decode GlacierStorage scan record: {error}"
                    ))
                })?;
                let DataMutation::Set { document, .. } = record.mutation else {
                    return Err(StorageError::backend(
                        "GlacierStorage set directory points to non-set record",
                    ));
                };
                let decoded_fields = if let Some(requested) = requested_top_level.as_ref() {
                    document
                        .fields
                        .iter()
                        .filter(|field| requested.contains(field.name.as_str()))
                        .count() as u64
                } else {
                    document.fields.len() as u64
                };
                let decoded = image_document_to_projected_prepared(
                    document,
                    fields,
                    requested_top_level.as_ref(),
                )?;
                (decoded, decoded_fields)
            };
            let stored = StoredDocument::new(id, version.version, Arc::new(decoded))?;
            scan_metrics.record_sampled_decode(decode_started);
            scan_metrics.decoded_fields =
                scan_metrics.decoded_fields.saturating_add(decoded_fields);
            scan_metrics.projected_records = scan_metrics.projected_records.saturating_add(1);
            emitted += 1;
            let visitor_started = scan_metrics.sampled_timer();
            let visitor_result = visitor(stored);
            scan_metrics.record_sampled_visitor(visitor_started);
            if !visitor_result? {
                return Ok(());
            }
        }

    }

    if emitted != expected {
        return Err(StorageError::backend(format!(
            "GlacierStorage sequential scan emitted {emitted} of {expected} visible documents"
        )));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct PhysicalProjectionProfile {
    samples: u64,
    clear_ns: u64,
    prepare_ns: u64,
    cache_guard_ns: u64,
    meta_ns: u64,
    value_ns: u64,
    fallback_ns: u64,
}

#[derive(Debug, Default)]
struct PhysicalProjectionCounters {
    values: u64,
    null_values: u64,
    bool_values: u64,
    signed_values: u64,
    unsigned_values: u64,
    float_values: u64,
    string_values: u64,
    complex_values: u64,
    string_cache_hits: u64,
    string_cache_misses: u64,
    string_cache_replacements: u64,
}

#[derive(Clone, Copy, Debug)]
enum ProjectedValueKind {
    Null,
    Bool,
    Signed,
    Unsigned,
    Float,
    StringHit,
    StringMiss,
    StringReplacement,
    StringBorrowed,
    Complex,
}

impl PhysicalProjectionCounters {
    fn record(&mut self, kind: ProjectedValueKind) {
        self.values = self.values.saturating_add(1);
        match kind {
            ProjectedValueKind::Null => self.null_values = self.null_values.saturating_add(1),
            ProjectedValueKind::Bool => self.bool_values = self.bool_values.saturating_add(1),
            ProjectedValueKind::Signed => self.signed_values = self.signed_values.saturating_add(1),
            ProjectedValueKind::Unsigned => {
                self.unsigned_values = self.unsigned_values.saturating_add(1)
            }
            ProjectedValueKind::Float => self.float_values = self.float_values.saturating_add(1),
            ProjectedValueKind::StringHit => {
                self.string_values = self.string_values.saturating_add(1);
                self.string_cache_hits = self.string_cache_hits.saturating_add(1);
            }
            ProjectedValueKind::StringMiss => {
                self.string_values = self.string_values.saturating_add(1);
                self.string_cache_misses = self.string_cache_misses.saturating_add(1);
            }
            ProjectedValueKind::StringReplacement => {
                self.string_values = self.string_values.saturating_add(1);
                self.string_cache_misses = self.string_cache_misses.saturating_add(1);
                self.string_cache_replacements = self.string_cache_replacements.saturating_add(1);
            }
            ProjectedValueKind::StringBorrowed => {
                self.string_values = self.string_values.saturating_add(1);
            }
            ProjectedValueKind::Complex => {
                self.complex_values = self.complex_values.saturating_add(1)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CompiledProjectionEntry {
    cursor: usize,
    name_start: usize,
    name_end: usize,
    kind: u8,
    capabilities: u8,
}

#[derive(Debug, Default)]
struct PhysicalProjectionLayout {
    directory_len: usize,
    field_count: usize,
    entry_offsets: Vec<Option<usize>>,
    compiled_entries: Vec<Option<CompiledProjectionEntry>>,
    field_names: Vec<Box<[u8]>>,
    string_cache: Vec<Option<Arc<str>>>,
}

impl PhysicalProjectionLayout {
    fn prepare(&mut self, fields: &[FieldPath]) {
        if self.entry_offsets.len() != fields.len() {
            self.entry_offsets.clear();
            self.entry_offsets.resize(fields.len(), None);
            self.compiled_entries.clear();
            self.compiled_entries.resize(fields.len(), None);
            self.field_names = fields
                .iter()
                .map(|field| {
                    field
                        .first()
                        .as_str()
                        .as_bytes()
                        .to_vec()
                        .into_boxed_slice()
                })
                .collect();
            self.string_cache.clear();
            self.string_cache.resize(fields.len(), None);
            self.directory_len = 0;
            self.field_count = 0;
        }
    }
}

fn decode_cached_physical_projection( bytes: &[u8], header: PhysicalSetHeader, fields: &[FieldPath], values: &mut [Option<Value>], layout: &mut PhysicalProjectionLayout, mut profile: Option<&mut PhysicalProjectionProfile>, mut counters: Option<&mut PhysicalProjectionCounters>, ) -> StorageResult<Option<u64>> {
    let guard_started = profile.as_ref().map(|_| Instant::now());
    if layout.directory_len != header.directory_len
        || layout.field_count != header.field_count
        || layout.entry_offsets.len() != fields.len()
    {
        if let (Some(started), Some(profile)) = (guard_started, profile.as_deref_mut()) {
            profile.cache_guard_ns = profile.cache_guard_ns.saturating_add(elapsed_nanos(started));
        }
        return Ok(None);
    }
    if let (Some(started), Some(profile)) = (guard_started, profile.as_deref_mut()) {
        profile.cache_guard_ns = profile.cache_guard_ns.saturating_add(elapsed_nanos(started));
    }
    let directory =
        &bytes[PHYSICAL_SET_HEADER_BYTES..PHYSICAL_SET_HEADER_BYTES + header.directory_len];
    let payload_base = PHYSICAL_SET_HEADER_BYTES + header.directory_len;
    let mut decoded_fields = 0u64;

    for field_index in 0..layout.entry_offsets.len() {
        let meta_started = profile.as_ref().map(|_| Instant::now());
        let Some(cursor) = layout.entry_offsets[field_index] else {
            continue;
        };
        if directory.len().saturating_sub(cursor) < PHYSICAL_FIELD_FIXED_BYTES {
            return Ok(None);
        }
        let name_len =
            u16::from_be_bytes(directory[cursor..cursor + 2].try_into().unwrap()) as usize;
        let kind = directory[cursor + 2];
        let capabilities = directory[cursor + 3];
        if kind > 7 || capabilities & !0x0f != 0 {
            return Ok(None);
        }
        let name_start = cursor + PHYSICAL_FIELD_FIXED_BYTES;
        let Some(name_end) = name_start.checked_add(name_len) else {
            return Ok(None);
        };
        if name_end > directory.len()
            || &directory[name_start..name_end] != fields[field_index].first().as_str().as_bytes()
        {
            return Ok(None);
        }
        let offset =
            u32::from_be_bytes(directory[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
        let length =
            u32::from_be_bytes(directory[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
        let Some(payload_end) = offset.checked_add(length) else {
            return Ok(None);
        };
        if payload_end > header.payload_len {
            return Ok(None);
        }
        let start = payload_base + offset;
        let end = start + length;
        if let (Some(started), Some(profile)) = (meta_started, profile.as_deref_mut()) {
            profile.meta_ns = profile.meta_ns.saturating_add(elapsed_nanos(started));
        }
        let value_started = profile.as_ref().map(|_| Instant::now());
        let (value, kind) = decode_projected_image_value_reusing_string(
            &bytes[start..end],
            &mut layout.string_cache[field_index],
        )?;
        values[field_index] = Some(value);
        if let Some(counters) = counters.as_deref_mut() {
            counters.record(kind);
        }
        if let (Some(started), Some(profile)) = (value_started, profile.as_deref_mut()) {
            profile.value_ns = profile.value_ns.saturating_add(elapsed_nanos(started));
        }
        decoded_fields = decoded_fields.saturating_add(1);
    }
    Ok(Some(decoded_fields))
}

fn decode_physical_projected_values_into( bytes: &[u8], header: PhysicalSetHeader, fields: &[FieldPath], values: &mut [Option<Value>], layout: &mut PhysicalProjectionLayout, mut profile: Option<&mut PhysicalProjectionProfile>, mut counters: Option<&mut PhysicalProjectionCounters>, ) -> StorageResult<(u64, bool)> {
    debug_assert_eq!(values.len(), fields.len());
    let clear_started = profile.as_ref().map(|_| Instant::now());
    for value in values.iter_mut() {
        *value = None;
    }
    if let (Some(started), Some(profile)) = (clear_started, profile.as_deref_mut()) {
        profile.clear_ns = profile.clear_ns.saturating_add(elapsed_nanos(started));
    }

    let prepare_started = profile.as_ref().map(|_| Instant::now());
    layout.prepare(fields);
    if let (Some(started), Some(profile)) = (prepare_started, profile.as_deref_mut()) {
        profile.prepare_ns = profile.prepare_ns.saturating_add(elapsed_nanos(started));
    }
    if let Some(decoded_fields) = decode_cached_physical_projection(
        bytes,
        header,
        fields,
        values,
        layout,
        profile.as_deref_mut(),
        counters.as_deref_mut(),
    )? {
        return Ok((decoded_fields, true));
    }

    let fallback_started = profile.as_ref().map(|_| Instant::now());
    let directory =
        &bytes[PHYSICAL_SET_HEADER_BYTES..PHYSICAL_SET_HEADER_BYTES + header.directory_len];
    let payload_base = PHYSICAL_SET_HEADER_BYTES + header.directory_len;
    let mut cursor = 0usize;
    let mut seen_entries = 0usize;
    let mut decoded_fields = 0u64;

    while cursor < directory.len() {
        let entry_start = cursor;
        if directory.len().saturating_sub(cursor) < PHYSICAL_FIELD_FIXED_BYTES {
            return Err(StorageError::backend(
                "truncated GlacierStorage physical field directory",
            ));
        }

        let name_len =
            u16::from_be_bytes(directory[cursor..cursor + 2].try_into().unwrap()) as usize;
        let kind = directory[cursor + 2];
        let capabilities = directory[cursor + 3];
        if kind > 7 || capabilities & !0x0f != 0 {
            return Err(StorageError::backend(
                "invalid GlacierStorage physical field metadata",
            ));
        }

        let offset =
            u32::from_be_bytes(directory[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
        let length =
            u32::from_be_bytes(directory[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
        cursor += PHYSICAL_FIELD_FIXED_BYTES;

        let name_end = cursor
            .checked_add(name_len)
            .ok_or_else(|| StorageError::backend("GlacierStorage physical field name overflow"))?;
        if name_end > directory.len() {
            return Err(StorageError::backend(
                "GlacierStorage physical field name outside directory",
            ));
        }
        let name = std::str::from_utf8(&directory[cursor..name_end]).map_err(|_| {
            StorageError::backend("GlacierStorage physical field name is not UTF-8")
        })?;
        cursor = name_end;
        seen_entries = seen_entries.saturating_add(1);

        let payload_end = offset.checked_add(length).ok_or_else(|| {
            StorageError::backend("GlacierStorage physical field payload overflow")
        })?;
        if payload_end > header.payload_len {
            return Err(StorageError::backend(
                "GlacierStorage physical field outside payload area",
            ));
        }

        // Directly match the physical directory entry against the small
        // requested field set. No PhysicalFieldEntry Vec is materialized.
        for (field_index, path) in fields.iter().enumerate() {
            if path.first().as_str() != name {
                continue;
            }
            let start = payload_base + offset;
            let end = start + length;
            let (value, kind) = decode_projected_image_value_reusing_string(
                &bytes[start..end],
                &mut layout.string_cache[field_index],
            )?;
            values[field_index] = Some(value);
            if let Some(counters) = counters.as_deref_mut() {
                counters.record(kind);
            }
            layout.entry_offsets[field_index] = Some(entry_start);
            decoded_fields = decoded_fields.saturating_add(1);
            break;
        }
    }

    if seen_entries != header.field_count {
        return Err(StorageError::backend(
            "GlacierStorage physical field count mismatch",
        ));
    }

    layout.directory_len = header.directory_len;
    layout.field_count = header.field_count;
    if let (Some(started), Some(profile)) = (fallback_started, profile.as_deref_mut()) {
        profile.fallback_ns = profile.fallback_ns.saturating_add(elapsed_nanos(started));
    }

    Ok((decoded_fields, false))
}

fn decode_projected_image_value_ref<'a>( bytes: &'a [u8], ) -> StorageResult<(ProjectedValueRef<'a>, ProjectedValueKind)> {
    let value: BorrowedProjectedImageValue<'a> = rmp_serde::from_slice(bytes).map_err(|error| {
        StorageError::backend(format!(
            "cannot decode GlacierStorage borrowed projected field: {error}"
        ))
    })?;

    match value {
        BorrowedProjectedImageValue::Null => {
            Ok((ProjectedValueRef::Null, ProjectedValueKind::Null))
        }
        BorrowedProjectedImageValue::Bool(value) => {
            Ok((ProjectedValueRef::Bool(value), ProjectedValueKind::Bool))
        }
        BorrowedProjectedImageValue::Signed(value) => {
            Ok((ProjectedValueRef::Signed(value), ProjectedValueKind::Signed))
        }
        BorrowedProjectedImageValue::Unsigned(value) => Ok((
            ProjectedValueRef::Unsigned(value),
            ProjectedValueKind::Unsigned,
        )),
        BorrowedProjectedImageValue::Float(value) => {
            if !value.is_finite() {
                return Err(StorageError::backend(
                    "stored projected float is not finite",
                ));
            }
            Ok((ProjectedValueRef::Float(value), ProjectedValueKind::Float))
        }
        BorrowedProjectedImageValue::String(value) => Ok((
            ProjectedValueRef::String(value),
            ProjectedValueKind::StringBorrowed,
        )),
        BorrowedProjectedImageValue::Array(_) | BorrowedProjectedImageValue::Object(_) => {
            let value: ImageValue = rmp_serde::from_slice(bytes).map_err(|error| {
                StorageError::backend(format!(
                    "cannot decode GlacierStorage projected complex field: {error}"
                ))
            })?;
            image_to_value(value)
                .map(|value| (ProjectedValueRef::Owned(value), ProjectedValueKind::Complex))
        }
    }
}

fn decode_cached_physical_projection_refs<'a>( bytes: &'a [u8], header: PhysicalSetHeader, fields: &[FieldPath], values: &mut [Option<ProjectedValueRef<'a>>], layout: &PhysicalProjectionLayout, counters: &mut PhysicalProjectionCounters, ) -> StorageResult<Option<u64>> {
    if layout.directory_len != header.directory_len
        || layout.field_count != header.field_count
        || layout.entry_offsets.len() != fields.len()
    {
        return Ok(None);
    }
    let directory =
        &bytes[PHYSICAL_SET_HEADER_BYTES..PHYSICAL_SET_HEADER_BYTES + header.directory_len];
    let payload_base = PHYSICAL_SET_HEADER_BYTES + header.directory_len;
    let mut decoded_fields = 0u64;

    for field_index in 0..layout.compiled_entries.len() {
        let Some(entry) = layout.compiled_entries[field_index] else {
            continue;
        };
        let cursor = entry.cursor;
        // The descriptor pre-resolves all layout-invariant metadata. We still
        // validate the field bytes and type on every record, preserving the
        // generic storage contract if an equal-sized but different layout
        // appears inside a segment.
        if entry.name_end > directory.len()
            || directory[cursor + 2] != entry.kind
            || directory[cursor + 3] != entry.capabilities
            || &directory[entry.name_start..entry.name_end]
                != layout.field_names[field_index].as_ref()
        {
            return Ok(None);
        }
        let offset =
            u32::from_be_bytes(directory[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
        let length =
            u32::from_be_bytes(directory[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
        let Some(payload_end) = offset.checked_add(length) else {
            return Ok(None);
        };
        if payload_end > header.payload_len {
            return Ok(None);
        }
        let start = payload_base + offset;
        let end = start + length;
        let (value, kind) = decode_projected_image_value_ref(&bytes[start..end])?;
        counters.record(kind);
        values[field_index] = Some(value);
        decoded_fields = decoded_fields.saturating_add(1);
    }
    Ok(Some(decoded_fields))
}

fn decode_physical_projected_refs_into<'a>( bytes: &'a [u8], header: PhysicalSetHeader, fields: &[FieldPath], values: &mut [Option<ProjectedValueRef<'a>>], layout: &mut PhysicalProjectionLayout, counters: &mut PhysicalProjectionCounters, ) -> StorageResult<(u64, bool)> {
    for value in values.iter_mut() {
        *value = None;
    }
    layout.prepare(fields);
    if let Some(decoded) =
        decode_cached_physical_projection_refs(bytes, header, fields, values, layout, counters)?
    {
        return Ok((decoded, true));
    }

    let directory =
        &bytes[PHYSICAL_SET_HEADER_BYTES..PHYSICAL_SET_HEADER_BYTES + header.directory_len];
    let payload_base = PHYSICAL_SET_HEADER_BYTES + header.directory_len;
    let mut cursor = 0usize;
    let mut seen_entries = 0usize;
    let mut decoded_fields = 0u64;

    while cursor < directory.len() {
        let entry_start = cursor;
        if directory.len().saturating_sub(cursor) < PHYSICAL_FIELD_FIXED_BYTES {
            return Err(StorageError::backend(
                "truncated GlacierStorage physical field directory",
            ));
        }
        let name_len =
            u16::from_be_bytes(directory[cursor..cursor + 2].try_into().unwrap()) as usize;
        let offset =
            u32::from_be_bytes(directory[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
        let length =
            u32::from_be_bytes(directory[cursor + 8..cursor + 12].try_into().unwrap()) as usize;
        cursor += PHYSICAL_FIELD_FIXED_BYTES;
        let name_end = cursor
            .checked_add(name_len)
            .ok_or_else(|| StorageError::backend("GlacierStorage physical field name overflow"))?;
        if name_end > directory.len() {
            return Err(StorageError::backend(
                "GlacierStorage physical field name outside directory",
            ));
        }
        let name = std::str::from_utf8(&directory[cursor..name_end]).map_err(|_| {
            StorageError::backend("GlacierStorage physical field name is not UTF-8")
        })?;
        cursor = name_end;
        seen_entries = seen_entries.saturating_add(1);
        let payload_end = offset.checked_add(length).ok_or_else(|| {
            StorageError::backend("GlacierStorage physical field payload overflow")
        })?;
        if payload_end > header.payload_len {
            return Err(StorageError::backend(
                "GlacierStorage physical field outside payload area",
            ));
        }
        for (field_index, path) in fields.iter().enumerate() {
            if path.first().as_str() != name {
                continue;
            }
            let start = payload_base + offset;
            let end = start + length;
            let (value, kind) = decode_projected_image_value_ref(&bytes[start..end])?;
            counters.record(kind);
            values[field_index] = Some(value);
            layout.entry_offsets[field_index] = Some(entry_start);
            layout.compiled_entries[field_index] = Some(CompiledProjectionEntry {
                cursor: entry_start,
                name_start: entry_start + PHYSICAL_FIELD_FIXED_BYTES,
                name_end,
                kind: directory[entry_start + 2],
                capabilities: directory[entry_start + 3],
            });
            decoded_fields = decoded_fields.saturating_add(1);
            break;
        }
    }
    if seen_entries != header.field_count {
        return Err(StorageError::backend(
            "GlacierStorage physical field count mismatch",
        ));
    }
    layout.directory_len = header.directory_len;
    layout.field_count = header.field_count;
    Ok((decoded_fields, false))
}

#[inline]
fn trusted_physical_set_prefix(bytes: &[u8]) -> StorageResult<Option<(PhysicalSetHeader, usize)>> {
    if bytes.len() < 8 || bytes[0..8] != PHYSICAL_SET_MAGIC {
        return Ok(None);
    }
    if bytes.len() < PHYSICAL_SET_HEADER_BYTES {
        return Err(StorageError::backend(
            "truncated GlacierStorage physical SET record",
        ));
    }
    let version = u16::from_be_bytes(bytes[8..10].try_into().unwrap());
    let header_len = u16::from_be_bytes(bytes[10..12].try_into().unwrap()) as usize;
    if version != PHYSICAL_SET_VERSION || header_len != PHYSICAL_SET_HEADER_BYTES {
        return Err(StorageError::backend(format!(
            "unsupported GlacierStorage physical SET record version {version}"
        )));
    }
    let directory_len = u32::from_be_bytes(bytes[48..52].try_into().unwrap()) as usize;
    let payload_len = u32::from_be_bytes(bytes[52..56].try_into().unwrap()) as usize;
    let total = PHYSICAL_SET_HEADER_BYTES
        .checked_add(directory_len)
        .and_then(|value| value.checked_add(payload_len))
        .ok_or_else(|| StorageError::backend("GlacierStorage physical SET size overflow"))?;
    if total > bytes.len() {
        return Err(StorageError::backend(
            "truncated GlacierStorage physical SET payload",
        ));
    }
    Ok(Some((
        PhysicalSetHeader {
            id: bytes[20..36].try_into().unwrap(),
            version: u64::from_be_bytes(bytes[36..44].try_into().unwrap()),
            field_count: u32::from_be_bytes(bytes[44..48].try_into().unwrap()) as usize,
            directory_len,
            payload_len,
        },
        total,
    )))
}

#[inline]
fn physical_set_record_len(bytes: &[u8]) -> StorageResult<Option<usize>> { trusted_physical_set_prefix(bytes).map(|record| record.map(|(_, len)| len)) }

fn records_are_all_physical_sets(records: &[u8], record_count: usize) -> StorageResult<bool> {
    let mut cursor = 0usize;
    for _ in 0..record_count {
        let Some(length) = physical_set_record_len(&records[cursor..])? else {
            return Ok(false);
        };
        cursor = cursor.checked_add(length).ok_or_else(|| {
            StorageError::backend("GlacierStorage physical record cursor overflow")
        })?;
        if cursor > records.len() {
            return Err(StorageError::backend(
                "GlacierStorage physical record cursor outside segment",
            ));
        }
    }
    Ok(cursor == records.len())
}

fn append_only_visibility_is_trivial( state: &GlacierState, collection_index: &CollectionIndex, generation: u64, ) -> bool {
    if generation != state.generation || !state.clear_generations.is_empty() {
        return false;
    }
    if visible_count(collection_index, generation) as usize
        != collection_index.logical_document_count()
    {
        return false;
    }

    collection_index
        .primary
        .iter()
        .all(|entry| entry.generation <= generation)
        && collection_index
            .exceptions
            .values()
            .all(|versions| versions.is_single_visible_set(generation))
}

fn scan_collection_sequential_value_refs(
    path: &Path,
    state: &GlacierState,
    collection_index: &CollectionIndex,
    collection: &CollectionId,
    generation: u64,
    fields: &[FieldPath],
    catalog_cache: &Mutex<Arc<SegmentCatalogSnapshot>>,
    mmap_cache: &Mutex<Option<(u64, Arc<GlacierReadOnlyMap>)>>,
    metrics: &GlacierReadMetrics,
    visitor: &mut dyn for<'a> FnMut(
        DocumentId,
        DocumentVersion,
        &[Option<ProjectedValueRef<'a>>],
    ) -> StorageResult<bool>,
) -> StorageResult<()> {
    if fields.iter().any(|field| field.len() != 1) {
        return Err(StorageError::backend(
            "GlacierStorage direct borrowed projected values require top-level fields",
        ));
    }
    let mut scan_metrics = GlacierReadScanGuard::new(metrics);
    let expected = visible_count(collection_index, generation) as usize;
    if expected == 0 {
        return Ok(());
    }

    let visibility_started = Instant::now();
    let append_only_visible =
        append_only_visibility_is_trivial(state, collection_index, generation);
    metrics
        .visibility_prepare_us
        .fetch_add(elapsed_micros(visibility_started), Ordering::Relaxed);
    if append_only_visible {
        metrics
            .visibility_fast_scans
            .fetch_add(1, Ordering::Relaxed);
    } else {
        metrics
            .visibility_fallback_scans
            .fetch_add(1, Ordering::Relaxed);
    }

    let io_started = Instant::now();
    let mut file = File::open(path).map_err(io_error("open borrowed projected scan", path))?;
    let length = file
        .metadata()
        .map_err(io_error("stat borrowed projected scan", path))?
        .len();
    metrics
        .io_us
        .fetch_add(elapsed_micros(io_started), Ordering::Relaxed);
    let mmap = prepare_scan_mmap(path, length, mmap_cache, metrics);
    let catalog = prepare_segment_catalog(path, length, catalog_cache, metrics)?;
    let mut emitted = 0usize;
    let mut borrowed_values = 0u64;
    let mut borrowed_strings = 0u64;
    let mut materialized_values = 0u64;
    // The physical projection descriptor is collection/layout state, not
    // segment state. Preserve it across compatible segments so the first
    // record of each segment does not pay a synthetic layout miss.
    let mut layout = PhysicalProjectionLayout::default();
    layout.prepare(fields);

    for segment in catalog.segments.iter() {
        if emitted >= expected || segment.generation > generation {
            break;
        }
        if let Some(other_collection) = segment.known_insert_collection() {
            if other_collection != collection.as_str() {
                metrics
                    .segment_catalog_skipped_segments
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
        }
        let segment_start = segment.start;
        let record_count = segment.record_count();
        let directory_len = segment.directory_len();
        let metadata_len = segment.metadata_len();
        let records_len = segment.records_len();
        scan_metrics.segments = scan_metrics.segments.saturating_add(1);

        let payload = read_segment_payload(
            &mut file,
            path,
            segment_start,
            length,
            directory_len,
            metadata_len,
            records_len,
            mmap.as_deref(),
            metrics,
        )?;
        let directory = payload.directory();
        let metadata = payload.metadata();
        let records = payload.records();
        if !segment.directory_verified.load(Ordering::Acquire) {
            let checksum_started = Instant::now();
            let _ = segment.verify_directory_checksum(directory, metadata)?;
            metrics
                .checksum_us
                .fetch_add(elapsed_micros(checksum_started), Ordering::Relaxed);
        }

        let bypass_directory = if append_only_visible {
            let metadata_decode_started = Instant::now();
            let metadata_matches = segment.proves_target_inserts(metadata, collection)?;
            metrics
                .decode_us
                .fetch_add(elapsed_micros(metadata_decode_started), Ordering::Relaxed);
            metadata_matches && segment.proves_physical_sets(records)?
        } else {
            false
        };

        let mut values: Vec<Option<ProjectedValueRef<'_>>> =
            (0..fields.len()).map(|_| None).collect();

        if bypass_directory {
            metrics
                .directory_bypass_segments
                .fetch_add(1, Ordering::Relaxed);
            let record_loop_started = Instant::now();
            let mut cursor = 0usize;
            for _ in 0..record_count {
                let (physical_header, length) = trusted_physical_set_prefix(&records[cursor..])?
                    .ok_or_else(|| {
                        StorageError::backend(
                            "GlacierStorage physical segment changed during borrowed projected scan",
                        )
                    })?;
                let end = cursor.checked_add(length).ok_or_else(|| {
                    StorageError::backend("GlacierStorage borrowed projected record end overflow")
                })?;
                let record_bytes = &records[cursor..end];
                scan_metrics.records = scan_metrics.records.saturating_add(1);
                scan_metrics.trusted_header_records =
                    scan_metrics.trusted_header_records.saturating_add(1);
                let decode_started = scan_metrics.sampled_timer();
                let before_strings = scan_metrics.projection_counters.string_values;
                let before_complex = scan_metrics.projection_counters.complex_values;
                let (decoded_fields, hit) = decode_physical_projected_refs_into(
                    record_bytes,
                    physical_header,
                    fields,
                    &mut values,
                    &mut layout,
                    &mut scan_metrics.projection_counters,
                )?;
                scan_metrics.record_sampled_decode(decode_started);
                if hit {
                    scan_metrics.projection_layout_hits =
                        scan_metrics.projection_layout_hits.saturating_add(1);
                } else {
                    scan_metrics.projection_layout_misses =
                        scan_metrics.projection_layout_misses.saturating_add(1);
                }
                borrowed_values = borrowed_values.saturating_add(decoded_fields);
                borrowed_strings = borrowed_strings.saturating_add(
                    scan_metrics
                        .projection_counters
                        .string_values
                        .saturating_sub(before_strings),
                );
                materialized_values = materialized_values.saturating_add(
                    scan_metrics
                        .projection_counters
                        .complex_values
                        .saturating_sub(before_complex),
                );
                scan_metrics.decoded_fields =
                    scan_metrics.decoded_fields.saturating_add(decoded_fields);
                scan_metrics.projected_records = scan_metrics.projected_records.saturating_add(1);
                emitted = emitted.saturating_add(1);
                let visitor_started = scan_metrics.sampled_timer();
                let result = visitor(
                    DocumentId::from_bytes(physical_header.id),
                    DocumentVersion::new(physical_header.version),
                    &values,
                );
                scan_metrics.record_sampled_visitor(visitor_started);
                if !result? {
                    metrics
                        .borrowed_projected_values
                        .fetch_add(borrowed_values, Ordering::Relaxed);
                    metrics
                        .borrowed_projected_strings
                        .fetch_add(borrowed_strings, Ordering::Relaxed);
                    metrics
                        .borrowed_projected_materializations
                        .fetch_add(materialized_values, Ordering::Relaxed);
                    return Ok(());
                }
                cursor = end;
            }
            metrics
                .record_loop_us
                .fetch_add(elapsed_micros(record_loop_started), Ordering::Relaxed);
        } else {
            metrics
                .directory_fallback_segments
                .fetch_add(1, Ordering::Relaxed);
            let decode_started = Instant::now();
            let entries: Vec<SegmentIndexEntry> =
                rmp_serde::from_slice(directory).map_err(|error| {
                    StorageError::backend(format!(
                        "cannot decode GlacierStorage segment directory: {error}"
                    ))
                })?;
            let directory_decode_us = elapsed_micros(decode_started);
            metrics
                .decode_us
                .fetch_add(directory_decode_us, Ordering::Relaxed);
            metrics
                .directory_decode_us
                .fetch_add(directory_decode_us, Ordering::Relaxed);
            let records_base = segment_start
                .checked_add(SEGMENT_HEADER_BYTES as u64)
                .and_then(|v| v.checked_add(directory_len as u64))
                .and_then(|v| v.checked_add(metadata_len as u64))
                .ok_or_else(|| StorageError::backend("GlacierStorage borrowed pointer overflow"))?;
            let record_loop_started = Instant::now();
            for entry in entries {
                if entry.kind != INDEX_KIND_SET
                    || entry.collection.as_deref() != Some(collection.as_str())
                {
                    continue;
                }
                if !append_only_visible {
                    metrics.visibility_checks.fetch_add(1, Ordering::Relaxed);
                    let id = DocumentId::from_bytes(entry.id);
                    let Some(version) = collection_index.visible_version(state, generation, &id)
                    else {
                        continue;
                    };
                    let Some(pointer) = version.pointer else {
                        continue;
                    };
                    let absolute = records_base
                        .checked_add(entry.relative_offset as u64)
                        .ok_or_else(|| {
                            StorageError::backend("GlacierStorage borrowed offset overflow")
                        })?;
                    if pointer.offset != absolute || pointer.length != entry.length {
                        continue;
                    }
                }
                let start = entry.relative_offset as usize;
                let end = start.checked_add(entry.length as usize).ok_or_else(|| {
                    StorageError::backend("GlacierStorage borrowed record overflow")
                })?;
                if entry.length == 0 || end > records.len() {
                    return Err(StorageError::backend(
                        "GlacierStorage borrowed record outside segment",
                    ));
                }
                let record_bytes = &records[start..end];
                scan_metrics.records = scan_metrics.records.saturating_add(1);
                if let Some(physical_header) = parse_physical_set_header(record_bytes)? {
                    scan_metrics.verified_header_records =
                        scan_metrics.verified_header_records.saturating_add(1);
                    let before_strings = scan_metrics.projection_counters.string_values;
                    let before_complex = scan_metrics.projection_counters.complex_values;
                    let (decoded_fields, hit) = decode_physical_projected_refs_into(
                        record_bytes,
                        physical_header,
                        fields,
                        &mut values,
                        &mut layout,
                        &mut scan_metrics.projection_counters,
                    )?;
                    if hit {
                        scan_metrics.projection_layout_hits =
                            scan_metrics.projection_layout_hits.saturating_add(1);
                    } else {
                        scan_metrics.projection_layout_misses =
                            scan_metrics.projection_layout_misses.saturating_add(1);
                    }
                    borrowed_values = borrowed_values.saturating_add(decoded_fields);
                    borrowed_strings = borrowed_strings.saturating_add(
                        scan_metrics
                            .projection_counters
                            .string_values
                            .saturating_sub(before_strings),
                    );
                    materialized_values = materialized_values.saturating_add(
                        scan_metrics
                            .projection_counters
                            .complex_values
                            .saturating_sub(before_complex),
                    );
                    scan_metrics.decoded_fields =
                        scan_metrics.decoded_fields.saturating_add(decoded_fields);
                } else {
                    let record: DataRecord =
                        rmp_serde::from_slice(record_bytes).map_err(|error| {
                            StorageError::backend(format!(
                                "cannot decode legacy GlacierStorage record: {error}"
                            ))
                        })?;
                    let DataMutation::Set { document, .. } = record.mutation else {
                        return Err(StorageError::backend(
                            "GlacierStorage set directory points to non-set record",
                        ));
                    };
                    let decoded = image_document_to_projected_prepared(document, fields, None)?;
                    for (index, path) in fields.iter().enumerate() {
                        values[index] = glacier_value_at_field_path(&decoded, path)
                            .cloned()
                            .map(ProjectedValueRef::Owned);
                    }
                    let count = values.iter().filter(|value| value.is_some()).count() as u64;
                    borrowed_values = borrowed_values.saturating_add(count);
                    materialized_values = materialized_values.saturating_add(count);
                    scan_metrics.decoded_fields = scan_metrics.decoded_fields.saturating_add(count);
                }
                scan_metrics.projected_records = scan_metrics.projected_records.saturating_add(1);
                emitted = emitted.saturating_add(1);
                let visitor_started = scan_metrics.sampled_timer();
                let result = visitor(
                    DocumentId::from_bytes(entry.id),
                    DocumentVersion::new(entry.version),
                    &values,
                );
                scan_metrics.record_sampled_visitor(visitor_started);
                if !result? {
                    break;
                }
            }
            metrics
                .record_loop_us
                .fetch_add(elapsed_micros(record_loop_started), Ordering::Relaxed);
        }
    }

    metrics
        .borrowed_projected_values
        .fetch_add(borrowed_values, Ordering::Relaxed);
    metrics
        .borrowed_projected_strings
        .fetch_add(borrowed_strings, Ordering::Relaxed);
    metrics
        .borrowed_projected_materializations
        .fetch_add(materialized_values, Ordering::Relaxed);
    Ok(())
}

fn decode_full_stored_record(
    record_bytes: &[u8],
    id: DocumentId,
    version: DocumentVersion,
) -> StorageResult<StoredDocument> {
    let document = if let Some(header) = parse_physical_set_header(record_bytes)? {
        let (document, _) = decode_projected_physical_set(record_bytes, header, &[], None)?;
        document
    } else {
        let record: DataRecord = rmp_serde::from_slice(record_bytes).map_err(|error| {
            StorageError::backend(format!("cannot decode GlacierStorage record: {error}"))
        })?;
        let DataMutation::Set { document, .. } = record.mutation else {
            return Err(StorageError::backend(
                "GlacierStorage set directory points to non-set record",
            ));
        };
        image_document_to_document(document)?
    };
    StoredDocument::new(id, version, Arc::new(document))
}

fn scan_collection_sequential_values(
    path: &Path,
    state: &GlacierState,
    collection_index: &CollectionIndex,
    collection: &CollectionId,
    generation: u64,
    fields: &[FieldPath],
    gate_field_count: usize,
    catalog_cache: &Mutex<Arc<SegmentCatalogSnapshot>>,
    mmap_cache: &Mutex<Option<(u64, Arc<GlacierReadOnlyMap>)>>,
    metrics: &GlacierReadMetrics,
    gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>,
    visitor: &mut dyn FnMut(
        DocumentId,
        DocumentVersion,
        &[Option<Value>],
    ) -> StorageResult<bool>,
    mut full_visitor: Option<&mut dyn FnMut(StoredDocument) -> StorageResult<bool>>,
) -> StorageResult<()> {
    if fields.iter().any(|field| field.len() != 1) {
        return Err(StorageError::backend(
            "GlacierStorage direct projected values require top-level fields",
        ));
    }
    let mut scan_metrics = GlacierReadScanGuard::new(metrics);
    let expected = visible_count(collection_index, generation) as usize;
    if expected == 0 {
        return Ok(());
    }

    let gate_field_count = gate_field_count.min(fields.len());
    let (gate_fields, trailing_fields) = fields.split_at(gate_field_count);
    let mut projected_values: Vec<Option<Value>> = vec![None; fields.len()];
    let mut gate_layout = PhysicalProjectionLayout::default();
    gate_layout.prepare(gate_fields);
    let mut trailing_layout = PhysicalProjectionLayout::default();
    trailing_layout.prepare(trailing_fields);

    let visibility_started = Instant::now();
    let append_only_visible =
        append_only_visibility_is_trivial(state, collection_index, generation);
    metrics
        .visibility_prepare_us
        .fetch_add(elapsed_micros(visibility_started), Ordering::Relaxed);
    if append_only_visible {
        metrics
            .visibility_fast_scans
            .fetch_add(1, Ordering::Relaxed);
    } else {
        metrics
            .visibility_fallback_scans
            .fetch_add(1, Ordering::Relaxed);
    }

    let io_started = Instant::now();
    let mut file = File::open(path).map_err(io_error("open direct projected scan", path))?;
    let length = file
        .metadata()
        .map_err(io_error("stat direct projected scan", path))?
        .len();
    metrics
        .io_us
        .fetch_add(elapsed_micros(io_started), Ordering::Relaxed);
    let mmap = prepare_scan_mmap(path, length, mmap_cache, metrics);
    let catalog = prepare_segment_catalog(path, length, catalog_cache, metrics)?;
    let mut emitted = 0usize;

    for segment in catalog.segments.iter() {
        if emitted >= expected || segment.generation > generation {
            break;
        }
        if let Some(other_collection) = segment.known_insert_collection() {
            if other_collection != collection.as_str() {
                metrics
                    .segment_catalog_skipped_segments
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
        }
        let segment_start = segment.start;
        let record_count = segment.record_count();
        let directory_len = segment.directory_len();
        let metadata_len = segment.metadata_len();
        let records_len = segment.records_len();
        scan_metrics.segments = scan_metrics.segments.saturating_add(1);

        let payload = read_segment_payload(
            &mut file,
            path,
            segment_start,
            length,
            directory_len,
            metadata_len,
            records_len,
            mmap.as_deref(),
            metrics,
        )?;
        let directory = payload.directory();
        let metadata = payload.metadata();
        let records = payload.records();
        // Analytical projected scans trust the physical OGDOC001 record checksum
        // validated by `parse_physical_set_header`. Re-hashing the complete
        // records area here would hash the entire store a second time on every
        // analytical query. Directory + metadata remain protected at segment level.
        if !segment.directory_verified.load(Ordering::Acquire) {
            let checksum_started = Instant::now();
            let _ = segment.verify_directory_checksum(directory, metadata)?;
            metrics
                .checksum_us
                .fetch_add(elapsed_micros(checksum_started), Ordering::Relaxed);
        }

        let bypass_directory = if append_only_visible {
            let metadata_decode_started = Instant::now();
            let metadata_matches = segment.proves_target_inserts(metadata, collection)?;
            let metadata_decode_us = elapsed_micros(metadata_decode_started);
            metrics
                .decode_us
                .fetch_add(metadata_decode_us, Ordering::Relaxed);
            metadata_matches && segment.proves_physical_sets(records)?
        } else {
            false
        };

        if bypass_directory {
            metrics
                .directory_bypass_segments
                .fetch_add(1, Ordering::Relaxed);
            let record_loop_started = Instant::now();
            let mut cursor = 0usize;
            for _ in 0..record_count {
                let (header, length) = trusted_physical_set_prefix(&records[cursor..])?
                    .ok_or_else(|| {
                        StorageError::backend(
                            "GlacierStorage physical segment changed during projected scan",
                        )
                    })?;
                let end = cursor.checked_add(length).ok_or_else(|| {
                    StorageError::backend("GlacierStorage physical record end overflow")
                })?;
                let record_bytes = &records[cursor..end];
                scan_metrics.records = scan_metrics.records.saturating_add(1);
                scan_metrics.trusted_header_records =
                    scan_metrics.trusted_header_records.saturating_add(1);
                let profile_projection = (scan_metrics.records & 1023) == 0;
                if profile_projection {
                    scan_metrics.projection_profile.samples =
                        scan_metrics.projection_profile.samples.saturating_add(1);
                }
                let decode_started = scan_metrics.sampled_timer();
                let (mut decoded_fields, gate_hit) = decode_physical_projected_values_into(
                    record_bytes,
                    header,
                    gate_fields,
                    &mut projected_values[..gate_field_count],
                    &mut gate_layout,
                    profile_projection.then_some(&mut scan_metrics.projection_profile),
                    Some(&mut scan_metrics.projection_counters),
                )?;
                if gate_hit {
                    scan_metrics.projection_layout_hits =
                        scan_metrics.projection_layout_hits.saturating_add(1);
                } else {
                    scan_metrics.projection_layout_misses =
                        scan_metrics.projection_layout_misses.saturating_add(1);
                }
                for target in &mut projected_values[gate_field_count..] {
                    *target = None;
                }

                let accepted = gate(&projected_values)?;
                if accepted && !trailing_fields.is_empty() {
                    let (trailing_decoded, trailing_hit) = decode_physical_projected_values_into(
                        record_bytes,
                        header,
                        trailing_fields,
                        &mut projected_values[gate_field_count..],
                        &mut trailing_layout,
                        profile_projection.then_some(&mut scan_metrics.projection_profile),
                        Some(&mut scan_metrics.projection_counters),
                    )?;
                    decoded_fields = decoded_fields.saturating_add(trailing_decoded);
                    if trailing_hit {
                        scan_metrics.projection_layout_hits =
                            scan_metrics.projection_layout_hits.saturating_add(1);
                    } else {
                        scan_metrics.projection_layout_misses =
                            scan_metrics.projection_layout_misses.saturating_add(1);
                    }
                }
                scan_metrics.record_sampled_decode(decode_started);
                scan_metrics.decoded_fields =
                    scan_metrics.decoded_fields.saturating_add(decoded_fields);
                scan_metrics.projected_records = scan_metrics.projected_records.saturating_add(1);
                emitted += 1;

                if accepted {
                    let visitor_started = scan_metrics.sampled_timer();
                    let id = DocumentId::from_bytes(header.id);
                    let version = DocumentVersion::new(header.version);
                    let result = if let Some(full_visitor) = full_visitor.as_deref_mut() {
                        full_visitor(decode_full_stored_record(record_bytes, id, version)?)
                    } else {
                        visitor(id, version, &projected_values)
                    };
                    scan_metrics.record_sampled_visitor(visitor_started);
                    if !result? {
                        return Ok(());
                    }
                }
                cursor = end;
            }
            metrics
                .record_loop_us
                .fetch_add(elapsed_micros(record_loop_started), Ordering::Relaxed);
            continue;
        }

        metrics
            .directory_fallback_segments
            .fetch_add(1, Ordering::Relaxed);
        let decode_started = Instant::now();
        let entries: Vec<SegmentIndexEntry> =
            rmp_serde::from_slice(directory).map_err(|error| {
                StorageError::backend(format!(
                    "cannot decode GlacierStorage segment directory: {error}"
                ))
            })?;
        let directory_decode_us = elapsed_micros(decode_started);
        metrics
            .decode_us
            .fetch_add(directory_decode_us, Ordering::Relaxed);
        metrics
            .directory_decode_us
            .fetch_add(directory_decode_us, Ordering::Relaxed);
        if entries.len() != record_count {
            return Err(StorageError::backend(
                "GlacierStorage segment directory count mismatch",
            ));
        }
        let records_base = segment_start
            .checked_add(SEGMENT_HEADER_BYTES as u64)
            .and_then(|v| v.checked_add(directory_len as u64))
            .and_then(|v| v.checked_add(metadata_len as u64))
            .ok_or_else(|| {
                StorageError::backend("GlacierStorage direct projected pointer overflow")
            })?;

        let record_loop_started = Instant::now();
        for entry in entries {
            if entry.kind != INDEX_KIND_SET
                || entry.collection.as_deref() != Some(collection.as_str())
            {
                continue;
            }
            if !append_only_visible {
                metrics.visibility_checks.fetch_add(1, Ordering::Relaxed);
                let id = DocumentId::from_bytes(entry.id);
                let Some(version) = collection_index.visible_version(state, generation, &id) else {
                    continue;
                };
                let Some(pointer) = version.pointer else {
                    continue;
                };
                let absolute = records_base
                    .checked_add(entry.relative_offset as u64)
                    .ok_or_else(|| {
                        StorageError::backend("GlacierStorage direct projected offset overflow")
                    })?;
                if pointer.offset != absolute || pointer.length != entry.length {
                    continue;
                }
            }
            let start = entry.relative_offset as usize;
            let end = start.checked_add(entry.length as usize).ok_or_else(|| {
                StorageError::backend("GlacierStorage direct projected record overflow")
            })?;
            if entry.length == 0 || end > records.len() {
                return Err(StorageError::backend(
                    "GlacierStorage direct projected record outside segment",
                ));
            }
            let decode_started = scan_metrics.sampled_timer();
            let record_bytes = &records[start..end];
            scan_metrics.records = scan_metrics.records.saturating_add(1);
            let physical_header = parse_physical_set_header(record_bytes)?;
            if physical_header.is_some() {
                scan_metrics.verified_header_records =
                    scan_metrics.verified_header_records.saturating_add(1);
            }
            let (decoded_fields, accepted) = if let Some(header) = physical_header {
                if header.id != entry.id || header.version != entry.version {
                    return Err(StorageError::backend(
                        "GlacierStorage physical SET header disagrees with segment directory",
                    ));
                }
                let (mut decoded_fields, gate_hit) = decode_physical_projected_values_into(
                    record_bytes,
                    header,
                    gate_fields,
                    &mut projected_values[..gate_field_count],
                    &mut gate_layout,
                    None,
                    Some(&mut scan_metrics.projection_counters),
                )?;
                if gate_hit {
                    scan_metrics.projection_layout_hits =
                        scan_metrics.projection_layout_hits.saturating_add(1);
                } else {
                    scan_metrics.projection_layout_misses =
                        scan_metrics.projection_layout_misses.saturating_add(1);
                }
                for target in &mut projected_values[gate_field_count..] {
                    *target = None;
                }
                let accepted = gate(&projected_values)?;
                if accepted && !trailing_fields.is_empty() {
                    let (trailing_decoded, trailing_hit) = decode_physical_projected_values_into(
                        record_bytes,
                        header,
                        trailing_fields,
                        &mut projected_values[gate_field_count..],
                        &mut trailing_layout,
                        None,
                        Some(&mut scan_metrics.projection_counters),
                    )?;
                    decoded_fields = decoded_fields.saturating_add(trailing_decoded);
                    if trailing_hit {
                        scan_metrics.projection_layout_hits =
                            scan_metrics.projection_layout_hits.saturating_add(1);
                    } else {
                        scan_metrics.projection_layout_misses =
                            scan_metrics.projection_layout_misses.saturating_add(1);
                    }
                }
                (decoded_fields, accepted)
            } else {
                // Legacy records keep correctness through the established full projection path.
                let record: DataRecord = rmp_serde::from_slice(record_bytes).map_err(|error| {
                    StorageError::backend(format!(
                        "cannot decode legacy GlacierStorage record: {error}"
                    ))
                })?;
                let DataMutation::Set { document, .. } = record.mutation else {
                    return Err(StorageError::backend(
                        "GlacierStorage set directory points to non-set record",
                    ));
                };
                let requested = fields
                    .iter()
                    .map(|path| path.first().as_str())
                    .collect::<BTreeSet<_>>();
                let decoded =
                    image_document_to_projected_prepared(document, fields, Some(&requested))?;
                for (index, path) in fields.iter().enumerate() {
                    projected_values[index] = glacier_value_at_field_path(&decoded, path).cloned();
                }
                let count = projected_values
                    .iter()
                    .filter(|value| value.is_some())
                    .count() as u64;
                let accepted = gate(&projected_values)?;
                (count, accepted)
            };
            scan_metrics.record_sampled_decode(decode_started);
            scan_metrics.decoded_fields =
                scan_metrics.decoded_fields.saturating_add(decoded_fields);
            scan_metrics.projected_records = scan_metrics.projected_records.saturating_add(1);
            emitted += 1;
            if accepted {
                let visitor_started = scan_metrics.sampled_timer();
                let id = DocumentId::from_bytes(entry.id);
                let version = DocumentVersion::new(entry.version);
                let result = if let Some(full_visitor) = full_visitor.as_deref_mut() {
                    full_visitor(decode_full_stored_record(record_bytes, id, version)?)
                } else {
                    visitor(id, version, &projected_values)
                };
                scan_metrics.record_sampled_visitor(visitor_started);
                if !result? {
                    return Ok(());
                }
            }
        }
        metrics
            .record_loop_us
            .fetch_add(elapsed_micros(record_loop_started), Ordering::Relaxed);
    }
    if emitted != expected {
        return Err(StorageError::backend(format!(
            "GlacierStorage direct projected scan emitted {emitted} of {expected} visible documents"
        )));
    }
    Ok(())
}

fn automatic_checkpoint_interval(checkpoint_offset: u64) -> u64 { checkpoint_offset.max(MIN_CHECKPOINT_INTERVAL_BYTES) }

fn next_checkpoint_offset_after_open( checkpoint_loaded: bool, replay_offset: u64, data_len: u64, ) -> u64 {
    let base = if checkpoint_loaded {
        replay_offset
    } else {
        data_len
    };
    base.saturating_add(automatic_checkpoint_interval(base))
}

fn checkpoint_retry_offset_after_failure(data_len: u64, checkpoint_offset: u64) -> u64 {
    data_len.saturating_add(
        automatic_checkpoint_interval(checkpoint_offset)
            .saturating_mul(CHECKPOINT_FAILURE_BACKOFF_MULTIPLIER),
    )
}

fn checkpoint_path(path: &Path) -> PathBuf { PathBuf::from(format!("{}.checkpoint", path.display())) }

fn checkpoint_temp_path(path: &Path) -> PathBuf { PathBuf::from(format!("{}.checkpoint.tmp", path.display())) }

fn build_checkpoint( state: &GlacierState, format: GlacierFormatInfo, data_len: u64, ) -> StorageResult<PersistentCheckpoint> {
    let mut collections = Vec::with_capacity(state.collections.len());
    for (collection_id, collection) in &state.collections {
        let count = visible_count(collection, state.generation);
        if count == 0 {
            continue;
        }
        let mut documents = Vec::with_capacity(count as usize);
        collection.for_each_id_version(state, state.generation, |id, index| {
            if let Some(pointer) = index.pointer {
                documents.push(CheckpointDocument {
                    id: id.into_bytes(),
                    version: index.version.get(),
                    offset: pointer.offset,
                    length: pointer.length,
                });
            }
        });
        documents.sort_unstable_by_key(|document| document.id);
        if documents.len() as u64 != count {
            return Err(StorageError::backend(format!(
                "GlacierStorage checkpoint count mismatch for {}: index={}, count={count}",
                collection_id.as_str(),
                documents.len()
            )));
        }
        collections.push(CheckpointCollection {
            name: collection_id.as_str().to_owned(),
            count,
            documents,
        });
    }
    Ok(PersistentCheckpoint {
        format_version: format.version(),
        store_id: format.store_id(),
        generation: state.generation,
        data_len,
        collections,
        metadata: state.metadata.clone(),
    })
}

#[derive(Clone, Copy, Debug, Default)]
struct CheckpointWriteTiming {
    bytes: u64,
    encode_us: u64,
    io_us: u64,
}

fn write_checkpoint( path: &Path, checkpoint: &PersistentCheckpoint, ) -> StorageResult<CheckpointWriteTiming> {
    struct ChecksumWriter<W> {
        inner: W,
        hash: u64,
        bytes: u64,
    }
    impl<W: Write> Write for ChecksumWriter<W> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let written = self.inner.write(buf)?;
            self.hash = checksum64_continue(self.hash, &buf[..written]);
            self.bytes = self.bytes.saturating_add(written as u64);
            Ok(written)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.inner.flush()
        }
    }

    let target = checkpoint_path(path);
    let temporary = checkpoint_temp_path(path);
    let io_started = Instant::now();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)
        .map_err(io_error("create checkpoint", &temporary))?;
    file.write_all(&[0u8; CHECKPOINT_HEADER_BYTES])
        .map_err(io_error("reserve checkpoint header", &temporary))?;

    let encode_started = Instant::now();
    let (payload_len, payload_checksum) = {
        // rmp-serde performs many small Write calls. Writing those directly to
        // File makes a large checkpoint syscall-bound and can block the commit
        // that triggered it long enough for clients to time out. Keep checksum
        // accounting on the serialized stream, but buffer physical writes.
        let buffered = BufWriter::with_capacity(CHECKPOINT_WRITE_BUFFER_BYTES, &mut file);
        let mut writer = ChecksumWriter {
            inner: buffered,
            hash: 0xcbf2_9ce4_8422_2325,
            bytes: 0,
        };
        checkpoint
            .serialize(&mut rmp_serde::Serializer::new(&mut writer))
            .map_err(|error| {
                StorageError::backend(format!("cannot encode GlacierStorage checkpoint: {error}"))
            })?;
        writer
            .flush()
            .map_err(io_error("flush checkpoint", &temporary))?;
        (writer.bytes, writer.hash)
    };
    let encode_us = elapsed_micros(encode_started);
    if payload_len == 0 || payload_len > MAX_CHECKPOINT_PAYLOAD_BYTES {
        let _ = fs::remove_file(&temporary);
        return Err(StorageError::backend(format!(
            "GlacierStorage checkpoint is {payload_len} bytes"
        )));
    }

    let mut header = [0u8; CHECKPOINT_HEADER_BYTES];
    header[0..8].copy_from_slice(&CHECKPOINT_MAGIC);
    header[8..10].copy_from_slice(&CHECKPOINT_VERSION.to_be_bytes());
    header[16..24].copy_from_slice(&payload_len.to_be_bytes());
    header[24..32].copy_from_slice(&payload_checksum.to_be_bytes());
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.write_all(&header))
        .map_err(io_error("write checkpoint header", &temporary))?;
    file.sync_all()
        .map_err(io_error("sync checkpoint", &temporary))?;
    drop(file);

    fs::rename(&temporary, &target).map_err(io_error("publish checkpoint", &target))?;
    if let Some(parent) = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        let directory =
            File::open(parent).map_err(io_error("open checkpoint directory", parent))?;
        directory
            .sync_all()
            .map_err(io_error("sync checkpoint directory", parent))?;
    }
    let bytes = (CHECKPOINT_HEADER_BYTES as u64)
        .checked_add(payload_len)
        .ok_or_else(|| StorageError::backend("GlacierStorage checkpoint size overflow"))?;
    Ok(CheckpointWriteTiming {
        bytes,
        encode_us,
        io_us: elapsed_micros(io_started).saturating_sub(encode_us),
    })
}

fn opportunistic_primary_cache_reservation( governor: Option<&MemoryGovernor>, ) -> (usize, Option<MemoryReservation>) {
    let entry_bytes = std::mem::size_of::<CompactPrimaryEntry>().max(1);
    let desired = governor
        .and_then(|governor| governor.profile().managed_budget_bytes)
        .map(|managed| managed / PRIMARY_CACHE_FRACTION_DENOMINATOR)
        .unwrap_or(256 * 1024 * 1024);

    let Some(governor) = governor else {
        return (desired / entry_bytes, None);
    };

    let mut candidate = desired;
    while candidate >= entry_bytes {
        match governor.reserve(MemoryClass::PageCache, candidate) {
            Ok(reservation) => return (candidate / entry_bytes, Some(reservation)),
            Err(_) => candidate /= 2,
        }
    }
    (0, None)
}

enum StreamingPrimaryTarget {
    Reuse(DiskPrimaryIndex),
    Build {
        target: PathBuf,
        temporary: PathBuf,
        writer: BufWriter<File>,
        last_id: Option<DocumentId>,
        count: u64,
    },
}

impl StreamingPrimaryTarget {
    fn prepare( store_path: &Path, collection: &CollectionId, generation: u64, expected_count: u64, ) -> StorageResult<Self> {
        let target = primary_index_path(store_path, collection);
        let expected_len = PRIMARY_INDEX_HEADER_BYTES
            .checked_add(expected_count.saturating_mul(PRIMARY_INDEX_ENTRY_BYTES))
            .ok_or_else(|| StorageError::backend("GlacierStorage primary index length overflow"))?;

        if let Ok(mut file) = OpenOptions::new().read(true).write(true).open(&target) {
            let mut header = [0u8; PRIMARY_INDEX_HEADER_BYTES as usize];
            if file.read_exact(&mut header).is_ok()
                && header[0..8] == PRIMARY_INDEX_MAGIC
                && u64::from_be_bytes(header[8..16].try_into().unwrap()) == generation
                && u64::from_be_bytes(header[16..24].try_into().unwrap()) >= expected_count
                && file
                    .metadata()
                    .map(|metadata| metadata.len() >= expected_len)
                    .unwrap_or(false)
            {
                file.set_len(expected_len)
                    .map_err(io_error("truncate primary index to checkpoint", &target))?;
                file.seek(SeekFrom::Start(16))
                    .map_err(io_error("seek primary checkpoint count", &target))?;
                file.write_all(&expected_count.to_be_bytes())
                    .map_err(io_error("write primary checkpoint count", &target))?;
                let last_id = if expected_count == 0 {
                    None
                } else {
                    let offset = PRIMARY_INDEX_HEADER_BYTES
                        + (expected_count - 1) * PRIMARY_INDEX_ENTRY_BYTES;
                    file.seek(SeekFrom::Start(offset))
                        .map_err(io_error("seek primary checkpoint tail", &target))?;
                    let mut bytes = [0u8; PRIMARY_INDEX_ENTRY_BYTES as usize];
                    file.read_exact(&mut bytes)
                        .map_err(io_error("read primary checkpoint tail", &target))?;
                    Some(decode_compact_primary_entry(&bytes).id)
                };
                return Ok(Self::Reuse(DiskPrimaryIndex {
                    path: target,
                    count: expected_count,
                    last_id,
                }));
            }
        }

        let temporary = PathBuf::from(format!("{}.tmp", target.display()));
        let raw = File::create(&temporary).map_err(io_error("create primary index", &temporary))?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, raw);
        let mut header = [0u8; PRIMARY_INDEX_HEADER_BYTES as usize];
        header[0..8].copy_from_slice(&PRIMARY_INDEX_MAGIC);
        header[8..16].copy_from_slice(&generation.to_be_bytes());
        header[16..24].copy_from_slice(&expected_count.to_be_bytes());
        writer
            .write_all(&header)
            .map_err(io_error("write primary index header", &temporary))?;
        Ok(Self::Build {
            target,
            temporary,
            writer,
            last_id: None,
            count: expected_count,
        })
    }

    fn observe(&mut self, entry: CompactPrimaryEntry) -> StorageResult<()> {
        if let Self::Build {
            writer,
            temporary,
            last_id,
            ..
        } = self
        {
            if last_id.as_ref().is_some_and(|last| entry.id <= *last) {
                return Err(StorageError::backend(
                    "GlacierStorage checkpoint primary ids are not strictly ordered",
                ));
            }
            writer
                .write_all(&encode_compact_primary_entry(entry))
                .map_err(io_error("stream primary index entry", temporary))?;
            *last_id = Some(entry.id);
        }
        Ok(())
    }

    fn finish(self) -> StorageResult<DiskPrimaryIndex> {
        match self {
            Self::Reuse(index) => Ok(index),
            Self::Build {
                target,
                temporary,
                mut writer,
                last_id,
                count,
            } => {
                writer
                    .flush()
                    .map_err(io_error("flush primary index", &temporary))?;
                drop(writer);
                fs::rename(&temporary, &target)
                    .map_err(io_error("install primary index", &target))?;
                Ok(DiskPrimaryIndex {
                    path: target,
                    count,
                    last_id,
                })
            }
        }
    }
}

struct CheckpointDocumentsSeed<'a> {
    store_path: &'a Path,
    collection: CollectionId,
    generation: u64,
    data_len: u64,
    expected_count: u64,
    cache_remaining: &'a std::cell::Cell<usize>,
}

impl<'de> DeserializeSeed<'de> for CheckpointDocumentsSeed<'_> {
    type Value = CollectionIndex;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DocumentsVisitor<'a> {
            seed: CheckpointDocumentsSeed<'a>,
        }

        impl<'de> Visitor<'de> for DocumentsVisitor<'_> {
            type Value = CollectionIndex;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("GlacierStorage checkpoint document sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let preload = self
                    .seed
                    .cache_remaining
                    .get()
                    .min(u64_to_usize_saturating(self.seed.expected_count));
                self.seed
                    .cache_remaining
                    .set(self.seed.cache_remaining.get().saturating_sub(preload));

                let mut collection = CollectionIndex {
                    primary: Vec::with_capacity(preload),
                    reclaimable_primary: preload,
                    disk_primary: None,
                    exceptions: HashMap::new(),
                    count_history: vec![(self.seed.generation, self.seed.expected_count)],
                    primary_head: BinaryHeap::new(),
                    primary_head_valid: true,
                };
                let mut target = StreamingPrimaryTarget::prepare(
                    self.seed.store_path,
                    &self.seed.collection,
                    self.seed.generation,
                    self.seed.expected_count,
                )
                .map_err(serde::de::Error::custom)?;

                let mut ordinal = 0u64;
                while let Some(document) = seq.next_element::<CheckpointDocument>()? {
                    if ordinal >= self.seed.expected_count {
                        return Err(serde::de::Error::custom(
                            "checkpoint contains more primary documents than declared",
                        ));
                    }
                    if document.length == 0
                        || document.offset < GLACIER_SUPERBLOCK_BYTES as u64
                        || document
                            .offset
                            .checked_add(document.length as u64)
                            .map_or(true, |end| end > self.seed.data_len)
                    {
                        return Err(serde::de::Error::custom(
                            "checkpoint contains an invalid record pointer",
                        ));
                    }

                    let id = DocumentId::from_bytes(document.id);
                    let entry = CompactPrimaryEntry {
                        id,
                        generation: self.seed.generation,
                        version: DocumentVersion::new(document.version),
                        pointer: RecordPointer {
                            offset: document.offset,
                            length: document.length,
                        },
                    };
                    target.observe(entry).map_err(serde::de::Error::custom)?;
                    primary_head_insert(&mut collection, id, entry.pointer);
                    if ordinal < preload as u64 {
                        collection.primary.push(entry);
                    }
                    ordinal += 1;
                }

                if ordinal != self.seed.expected_count {
                    return Err(serde::de::Error::custom(format!(
                        "checkpoint primary count mismatch: decoded {ordinal}, expected {}",
                        self.seed.expected_count
                    )));
                }
                collection.disk_primary = Some(target.finish().map_err(serde::de::Error::custom)?);
                Ok(collection)
            }
        }

        deserializer.deserialize_seq(DocumentsVisitor { seed: self })
    }
}

struct CheckpointCollectionSeed<'a> {
    store_path: &'a Path,
    generation: u64,
    data_len: u64,
    cache_remaining: &'a std::cell::Cell<usize>,
}

impl<'de> DeserializeSeed<'de> for CheckpointCollectionSeed<'_> {
    type Value = (CollectionId, CollectionIndex);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct CollectionVisitor<'a> {
            seed: CheckpointCollectionSeed<'a>,
        }

        impl<'de> Visitor<'de> for CollectionVisitor<'_> {
            type Value = (CollectionId, CollectionIndex);

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("GlacierStorage checkpoint collection")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let name: String = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing checkpoint collection name")
                })?;
                let count: u64 = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::custom("missing checkpoint collection count")
                })?;
                let collection = CollectionId::parse(name).map_err(serde::de::Error::custom)?;
                let index = seq
                    .next_element_seed(CheckpointDocumentsSeed {
                        store_path: self.seed.store_path,
                        collection: collection.clone(),
                        generation: self.seed.generation,
                        data_len: self.seed.data_len,
                        expected_count: count,
                        cache_remaining: self.seed.cache_remaining,
                    })?
                    .ok_or_else(|| {
                        serde::de::Error::custom("missing checkpoint collection documents")
                    })?;
                Ok((collection, index))
            }
        }

        deserializer.deserialize_seq(CollectionVisitor { seed: self })
    }
}

struct CheckpointCollectionsSeed<'a> {
    store_path: &'a Path,
    generation: u64,
    data_len: u64,
    cache_remaining: &'a std::cell::Cell<usize>,
}

impl<'de> DeserializeSeed<'de> for CheckpointCollectionsSeed<'_> {
    type Value = BTreeMap<CollectionId, CollectionIndex>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct CollectionsVisitor<'a> {
            seed: CheckpointCollectionsSeed<'a>,
        }

        impl<'de> Visitor<'de> for CollectionsVisitor<'_> {
            type Value = BTreeMap<CollectionId, CollectionIndex>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("GlacierStorage checkpoint collection sequence")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut collections = BTreeMap::new();
                while let Some((id, index)) = seq.next_element_seed(CheckpointCollectionSeed {
                    store_path: self.seed.store_path,
                    generation: self.seed.generation,
                    data_len: self.seed.data_len,
                    cache_remaining: self.seed.cache_remaining,
                })? {
                    collections.insert(id, index);
                }
                Ok(collections)
            }
        }

        deserializer.deserialize_seq(CollectionsVisitor { seed: self })
    }
}

struct StreamingCheckpointSeed<'a> {
    store_path: &'a Path,
    format: GlacierFormatInfo,
    data_file_len: u64,
    cache_entries: usize,
}

impl<'de> DeserializeSeed<'de> for StreamingCheckpointSeed<'_> {
    type Value = (GlacierState, u64, u64);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct CheckpointVisitor<'a> {
            seed: StreamingCheckpointSeed<'a>,
        }

        impl<'de> Visitor<'de> for CheckpointVisitor<'_> {
            type Value = (GlacierState, u64, u64);

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("GlacierStorage checkpoint")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let format_version: u16 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("missing checkpoint format version"))?;
                let store_id: [u8; 16] = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("missing checkpoint store id"))?;
                let generation: u64 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("missing checkpoint generation"))?;
                let data_len: u64 = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("missing checkpoint data length"))?;

                if format_version != self.seed.format.version()
                    || store_id != self.seed.format.store_id()
                    || data_len < GLACIER_SUPERBLOCK_BYTES as u64
                    || data_len > self.seed.data_file_len
                {
                    return Err(serde::de::Error::custom(
                        "checkpoint identity or data length mismatch",
                    ));
                }

                let cache_remaining = std::cell::Cell::new(self.seed.cache_entries);
                let collections = seq
                    .next_element_seed(CheckpointCollectionsSeed {
                        store_path: self.seed.store_path,
                        generation,
                        data_len,
                        cache_remaining: &cache_remaining,
                    })?
                    .ok_or_else(|| serde::de::Error::custom("missing checkpoint collections"))?;
                let metadata: FieldCatalog = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("missing checkpoint metadata"))?;

                Ok((
                    GlacierState {
                        generation,
                        clear_generations: Vec::new(),
                        collections,
                        metadata,
                    },
                    data_len,
                    generation,
                ))
            }
        }

        deserializer.deserialize_seq(CheckpointVisitor { seed: self })
    }
}

fn load_checkpoint( path: &Path, format: GlacierFormatInfo, data_file_len: u64, memory_governor: Option<&MemoryGovernor>, ) -> StorageResult<Option<(GlacierState, u64, u64, Option<MemoryReservation>)>> {
    let checkpoint_path = checkpoint_path(path);
    let mut file = match File::open(&checkpoint_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Ok(None),
    };
    let checkpoint_bytes = match file.metadata() {
        Ok(metadata) => metadata.len(),
        Err(_) => return Ok(None),
    };
    if checkpoint_bytes < CHECKPOINT_HEADER_BYTES as u64 {
        return Ok(None);
    }
    let mut header = [0u8; CHECKPOINT_HEADER_BYTES];
    if file.read_exact(&mut header).is_err() {
        return Ok(None);
    }
    if header[0..8] != CHECKPOINT_MAGIC {
        return Ok(None);
    }
    let version = u16::from_be_bytes(header[8..10].try_into().unwrap());
    if version != CHECKPOINT_VERSION {
        return Ok(None);
    }
    let payload_len = u64::from_be_bytes(header[16..24].try_into().unwrap());
    if payload_len == 0
        || payload_len > MAX_CHECKPOINT_PAYLOAD_BYTES
        || (CHECKPOINT_HEADER_BYTES as u64)
            .checked_add(payload_len)
            .map_or(true, |expected| expected != checkpoint_bytes)
    {
        return Ok(None);
    }

    let expected_checksum = u64::from_be_bytes(header[24..32].try_into().unwrap());
    let mut checksum = 0xcbf2_9ce4_8422_2325u64;
    let mut remaining = payload_len;
    let mut buffer = [0u8; 64 * 1024];
    while remaining > 0 {
        let chunk = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        if file.read_exact(&mut buffer[..chunk]).is_err() {
            return Ok(None);
        }
        checksum = checksum64_continue(checksum, &buffer[..chunk]);
        remaining -= chunk as u64;
    }
    if checksum != expected_checksum {
        return Ok(None);
    }
    if file
        .seek(SeekFrom::Start(CHECKPOINT_HEADER_BYTES as u64))
        .is_err()
    {
        return Ok(None);
    }

    let (cache_entries, mut cache_reservation) =
        opportunistic_primary_cache_reservation(memory_governor);
    let mut reader = BufReader::with_capacity(CHECKPOINT_READ_BUFFER_BYTES, file);
    let mut deserializer = rmp_serde::Deserializer::new(&mut reader);
    let (state, checkpoint_data_len, checkpoint_generation) = match (StreamingCheckpointSeed {
        store_path: path,
        format,
        data_file_len,
        cache_entries,
    })
    .deserialize(&mut deserializer)
    {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };

    // Reservation is a cache ceiling, not guaranteed resident usage. If the
    // checkpoint contains fewer primary entries than the opportunistic budget,
    // return the unused portion immediately so it cannot starve query memory.
    if let Some(reservation) = cache_reservation.as_mut() {
        let entry_bytes = std::mem::size_of::<CompactPrimaryEntry>().max(1);
        let resident_cache_bytes = state.collections.values().fold(0usize, |bytes, collection| {
            bytes.saturating_add(
                collection
                    .reclaimable_primary
                    .min(collection.primary.len())
                    .saturating_mul(entry_bytes),
            )
        });
        let unused = reservation.bytes().saturating_sub(resident_cache_bytes);
        let _ = reservation.shrink_by(unused);
    }

    if checkpoint_data_len < data_file_len {
        if data_file_len - checkpoint_data_len < SEGMENT_HEADER_BYTES as u64 {
            return Ok(None);
        }
        let mut data_file = match File::open(path) {
            Ok(file) => file,
            Err(_) => return Ok(None),
        };
        if data_file
            .seek(SeekFrom::Start(checkpoint_data_len))
            .is_err()
        {
            return Ok(None);
        }
        let mut segment_header = [0u8; SEGMENT_HEADER_BYTES];
        if data_file.read_exact(&mut segment_header).is_err()
            || segment_header[0..8] != SEGMENT_MAGIC
        {
            return Ok(None);
        }
        let tail_generation = u64::from_be_bytes(segment_header[8..16].try_into().unwrap());
        if tail_generation != checkpoint_generation.saturating_add(1) {
            return Ok(None);
        }
    }

    Ok(Some((
        state,
        checkpoint_data_len,
        checkpoint_bytes,
        cache_reservation,
    )))
}

fn install_disk_primary_from_entries( store_path: &Path, collection: &CollectionId, generation: u64, entries: &[CompactPrimaryEntry], ) -> StorageResult<DiskPrimaryIndex> {
    let target = primary_index_path(store_path, collection);
    let temporary = PathBuf::from(format!("{}.tmp", target.display()));
    let raw =
        File::create(&temporary).map_err(io_error("create cold primary index", &temporary))?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, raw);
    let mut header = [0u8; PRIMARY_INDEX_HEADER_BYTES as usize];
    header[0..8].copy_from_slice(&PRIMARY_INDEX_MAGIC);
    header[8..16].copy_from_slice(&generation.to_be_bytes());
    header[16..24].copy_from_slice(&(entries.len() as u64).to_be_bytes());
    writer
        .write_all(&header)
        .map_err(io_error("write cold primary header", &temporary))?;
    for entry in entries.iter().copied() {
        writer
            .write_all(&encode_compact_primary_entry(entry))
            .map_err(io_error("write cold primary entry", &temporary))?;
    }
    writer
        .flush()
        .map_err(io_error("flush cold primary index", &temporary))?;
    drop(writer);
    fs::rename(&temporary, &target).map_err(io_error("install cold primary index", &target))?;
    Ok(DiskPrimaryIndex {
        path: target,
        count: entries.len() as u64,
        last_id: entries.last().map(|entry| entry.id),
    })
}

/// During a checkpoint-less rebuild the compact primary used to grow without
/// reference to the configured process budget.  Once its resident footprint
/// reaches the same page-cache share used by checkpoint loading, move the
/// largest primary vector to the ordered disk sidecar.  Subsequent monotonic
/// ids append to that sidecar; updates/out-of-order ids remain exceptions.
fn enforce_cold_primary_budget( store_path: &Path, state: &mut GlacierState, generation: u64, governor: Option<&MemoryGovernor>, ) -> StorageResult<()> {
    let Some(governor) = governor else {
        return Ok(());
    };
    let Some(managed) = governor.profile().managed_budget_bytes else {
        return Ok(());
    };
    let entry_bytes = std::mem::size_of::<CompactPrimaryEntry>().max(1);
    let resident_budget = (managed / PRIMARY_CACHE_FRACTION_DENOMINATOR).max(entry_bytes);

    loop {
        let resident = state
            .collections
            .values()
            .fold(0usize, |total, collection| {
                total.saturating_add(collection.primary.capacity().saturating_mul(entry_bytes))
            });
        if resident <= resident_budget {
            return Ok(());
        }
        let Some(collection_id) = state
            .collections
            .iter()
            .filter(|(_, collection)| {
                collection.disk_primary.is_none() && !collection.primary.is_empty()
            })
            .max_by_key(|(_, collection)| collection.primary.capacity())
            .map(|(id, _)| id.clone())
        else {
            return Ok(());
        };
        let collection = state
            .collections
            .get_mut(&collection_id)
            .expect("collection exists");
        collection.primary.shrink_to_fit();
        let disk = install_disk_primary_from_entries(
            store_path,
            &collection_id,
            generation,
            &collection.primary,
        )?;
        collection.disk_primary = Some(disk);
        collection.primary = Vec::new();
        collection.reclaimable_primary = 0;
    }
}

fn scan_data_file( path: &Path, metrics: &GlacierStartupMetrics, mut state: GlacierState, replay_offset: u64, memory_governor: Option<&MemoryGovernor>, ) -> StorageResult<(GlacierState, Vec<SegmentCatalogEntry>)> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(io_error("open data", path))?;
    let length = file.metadata().map_err(io_error("stat data", path))?.len();
    if replay_offset < GLACIER_SUPERBLOCK_BYTES as u64 || replay_offset > length {
        return Err(StorageError::backend(
            "GlacierStorage checkpoint replay offset is outside the data file",
        ));
    }
    let mut segment_start = replay_offset;
    let mut replay_catalog = Vec::new();
    let mut catalog_collections = HashMap::<String, Arc<str>>::new();

    while segment_start < length {
        if length - segment_start < SEGMENT_HEADER_BYTES as u64 {
            file.set_len(segment_start)
                .map_err(io_error("truncate torn Glacier segment header", path))?;
            break;
        }

        file.seek(SeekFrom::Start(segment_start))
            .map_err(io_error("seek Glacier segment", path))?;
        let mut header = [0u8; SEGMENT_HEADER_BYTES];
        file.read_exact(&mut header)
            .map_err(io_error("read Glacier segment header", path))?;

        if header[0..8] != SEGMENT_MAGIC {
            return Err(StorageError::backend(
                "invalid GlacierStorage segment magic",
            ));
        }

        let generation = u64::from_be_bytes(header[8..16].try_into().unwrap());
        if generation != state.generation.saturating_add(1) {
            return Err(StorageError::backend(format!(
                "GlacierStorage segment generation gap: expected {}, got {generation}",
                state.generation.saturating_add(1)
            )));
        }

        let record_count = u32::from_be_bytes(header[16..20].try_into().unwrap()) as usize;
        let directory_len = u32::from_be_bytes(header[20..24].try_into().unwrap()) as usize;
        let metadata_len = u32::from_be_bytes(header[24..28].try_into().unwrap()) as usize;
        let records_len = u32::from_be_bytes(header[28..32].try_into().unwrap()) as usize;
        let expected_directory_checksum = u64::from_be_bytes(header[32..40].try_into().unwrap());

        validate_segment_lengths(record_count, directory_len, metadata_len, records_len)?;

        let segment_end = segment_start
            .checked_add(SEGMENT_HEADER_BYTES as u64)
            .and_then(|value| value.checked_add(directory_len as u64))
            .and_then(|value| value.checked_add(metadata_len as u64))
            .and_then(|value| value.checked_add(records_len as u64))
            .ok_or_else(|| StorageError::backend("GlacierStorage segment offset overflow"))?;

        if segment_end > length {
            file.set_len(segment_start)
                .map_err(io_error("truncate torn Glacier segment", path))?;
            break;
        }

        metrics.segments.fetch_add(1, Ordering::Relaxed);
        metrics
            .records
            .fetch_add(record_count as u64, Ordering::Relaxed);

        let mut directory = vec![0u8; directory_len];
        let mut metadata = vec![0u8; metadata_len];
        file.read_exact(&mut directory)
            .map_err(io_error("read Glacier startup directory", path))?;
        file.read_exact(&mut metadata)
            .map_err(io_error("read Glacier startup metadata", path))?;

        if checksum64_pair(&directory, &metadata) != expected_directory_checksum {
            return Err(StorageError::backend(
                "GlacierStorage segment directory checksum mismatch",
            ));
        }

        let directory_decode_started = Instant::now();
        let entries: Vec<SegmentIndexEntry> =
            rmp_serde::from_slice(&directory).map_err(|error| {
                StorageError::backend(format!(
                    "cannot decode GlacierStorage startup directory: {error}"
                ))
            })?;
        metrics
            .directory_decode_us
            .fetch_add(elapsed_micros(directory_decode_started), Ordering::Relaxed);
        if entries.len() != record_count {
            return Err(StorageError::backend(
                "GlacierStorage segment directory count mismatch",
            ));
        }

        let metadata_rebuild_started = Instant::now();
        let metadata_delta: SegmentMetadataDelta =
            rmp_serde::from_slice(&metadata).map_err(|error| {
                StorageError::backend(format!(
                    "cannot decode GlacierStorage startup metadata: {error}"
                ))
            })?;
        let insert_collection = segment_insert_collection_from_delta(&metadata_delta, record_count)
            .map(|name| {
                if let Some(existing) = catalog_collections.get(name) {
                    Arc::clone(existing)
                } else {
                    let interned = Arc::<str>::from(name);
                    catalog_collections.insert(name.to_owned(), Arc::clone(&interned));
                    interned
                }
            });
        replay_catalog.push(SegmentCatalogEntry::new(
            segment_start,
            generation,
            record_count as u32,
            directory_len as u32,
            metadata_len as u32,
            records_len as u32,
            expected_directory_checksum,
            u64::from_be_bytes(header[40..48].try_into().unwrap()),
            true,
            Some(insert_collection),
        ));
        reserve_index_from_metadata_delta(&mut state, &metadata_delta)?;
        metrics
            .metadata_rebuild_us
            .fetch_add(elapsed_micros(metadata_rebuild_started), Ordering::Relaxed);

        let records_base = segment_start
            .checked_add(SEGMENT_HEADER_BYTES as u64)
            .and_then(|value| value.checked_add(directory_len as u64))
            .and_then(|value| value.checked_add(metadata_len as u64))
            .ok_or_else(|| StorageError::backend("GlacierStorage startup pointer overflow"))?;

        let index_rebuild_started = Instant::now();
        let appended_primary =
            append_replay_primary_entries(&mut state, generation, records_base, &entries)?;
        for entry in entries {
            let appended = entry
                .collection
                .as_ref()
                .map(|collection| {
                    appended_primary
                        .contains(&(collection.clone(), DocumentId::from_bytes(entry.id)))
                })
                .unwrap_or(false);
            if !appended {
                apply_index_entry(&mut state, generation, records_base, entry)?;
            }
        }
        apply_replay_count_delta(&mut state, generation, &metadata_delta)?;
        metrics
            .index_rebuild_us
            .fetch_add(elapsed_micros(index_rebuild_started), Ordering::Relaxed);

        let metadata_rebuild_started = Instant::now();
        apply_metadata_delta(&mut state.metadata, &metadata_delta)?;
        metrics
            .metadata_rebuild_us
            .fetch_add(elapsed_micros(metadata_rebuild_started), Ordering::Relaxed);
        state.generation = generation;
        enforce_cold_primary_budget(path, &mut state, generation, memory_governor)?;

        segment_start = segment_end;
        file.seek(SeekFrom::Start(segment_start))
            .map_err(io_error("skip Glacier segment records", path))?;
    }

    Ok((state, replay_catalog))
}

fn validate_segment_lengths( record_count: usize, directory_len: usize, metadata_len: usize, records_len: usize, ) -> StorageResult<()> {
    if record_count == 0 {
        return Err(StorageError::backend(
            "GlacierStorage segment has no records",
        ));
    }
    if directory_len == 0 || directory_len > MAX_SEGMENT_DIRECTORY_BYTES {
        return Err(StorageError::backend(format!(
            "invalid GlacierStorage segment directory length {directory_len}"
        )));
    }
    if metadata_len == 0 || metadata_len > MAX_SEGMENT_METADATA_BYTES {
        return Err(StorageError::backend(format!(
            "invalid GlacierStorage segment metadata length {metadata_len}"
        )));
    }
    let total = directory_len
        .checked_add(metadata_len)
        .and_then(|value| value.checked_add(records_len))
        .ok_or_else(|| StorageError::backend("GlacierStorage segment length overflow"))?;
    if total > MAX_SEGMENT_BYTES {
        return Err(StorageError::backend(format!(
            "invalid GlacierStorage segment length {total}"
        )));
    }
    Ok(())
}

fn reserve_index_from_metadata_delta( state: &mut GlacierState, delta: &SegmentMetadataDelta, ) -> StorageResult<()> {
    if delta.clear {
        return Ok(());
    }
    for (collection_name, collection_delta) in &delta.collections {
        if collection_delta.documents <= 0 {
            continue;
        }
        let additional = usize::try_from(collection_delta.documents)
            .map_err(|_| StorageError::backend("GlacierStorage startup index capacity overflow"))?;
        if additional == 0 {
            continue;
        }
        let collection = CollectionId::parse(collection_name.clone())?;
        let collection_index = state.collections.entry(collection).or_default();
        if collection_index.disk_primary.is_none() {
            collection_index.primary.reserve(additional);
        }
    }
    Ok(())
}

fn append_replay_primary_entries( state: &mut GlacierState, generation: u64, records_base: u64, entries: &[SegmentIndexEntry], ) -> StorageResult<HashSet<(String, DocumentId)>> {
    let mut grouped = BTreeMap::<String, Vec<CompactPrimaryEntry>>::new();
    for entry in entries {
        if entry.kind != INDEX_KIND_SET {
            continue;
        }
        let Some(collection_name) = entry.collection.as_ref() else {
            continue;
        };
        let collection_id = CollectionId::parse(collection_name.clone())?;
        let Some(collection) = state.collections.get(&collection_id) else {
            continue;
        };
        let Some(disk) = collection.disk_primary.as_ref() else {
            continue;
        };
        let id = DocumentId::from_bytes(entry.id);
        if disk.last_id.is_some_and(|last| id <= last) || collection.exceptions.contains_key(&id) {
            continue;
        }
        let pointer = RecordPointer {
            offset: records_base
                .checked_add(entry.relative_offset as u64)
                .ok_or_else(|| {
                    StorageError::backend("GlacierStorage replay primary offset overflow")
                })?,
            length: entry.length,
        };
        grouped
            .entry(collection_name.clone())
            .or_default()
            .push(CompactPrimaryEntry {
                id,
                generation,
                version: DocumentVersion::new(entry.version),
                pointer,
            });
    }

    let mut appended = HashSet::new();
    for (collection_name, mut candidates) in grouped {
        candidates.sort_unstable_by_key(|entry| entry.id);
        let collection_id = CollectionId::parse(collection_name.clone())?;
        let collection = state.collections.get_mut(&collection_id).ok_or_else(|| {
            StorageError::backend("GlacierStorage replay primary collection disappeared")
        })?;
        let mut head_entries = Vec::new();
        {
            let Some(disk) = collection.disk_primary.as_mut() else {
                continue;
            };
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&disk.path)
                .map_err(io_error("open replay primary index", &disk.path))?;
            file.seek(SeekFrom::End(0))
                .map_err(io_error("seek replay primary index", &disk.path))?;
            let mut last = disk.last_id;
            let mut written = 0u64;
            for entry in candidates {
                if last.is_some_and(|previous| entry.id <= previous) {
                    continue;
                }
                file.write_all(&encode_compact_primary_entry(entry))
                    .map_err(io_error("append replay primary index", &disk.path))?;
                last = Some(entry.id);
                written = written.saturating_add(1);
                appended.insert((collection_name.clone(), entry.id));
                head_entries.push((entry.id, entry.pointer));
            }
            if written > 0 {
                disk.count = disk.count.saturating_add(written);
                disk.last_id = last;
                file.seek(SeekFrom::Start(16))
                    .map_err(io_error("seek replay primary count", &disk.path))?;
                file.write_all(&disk.count.to_be_bytes())
                    .map_err(io_error("update replay primary count", &disk.path))?;
            }
        }
        for (id, pointer) in head_entries {
            primary_head_insert(collection, id, pointer);
        }
    }
    Ok(appended)
}

fn apply_index_entry( state: &mut GlacierState, generation: u64, records_base: u64, entry: SegmentIndexEntry, ) -> StorageResult<()> {
    match entry.kind {
        INDEX_KIND_SET => {
            let collection_name = entry.collection.ok_or_else(|| {
                StorageError::backend("GlacierStorage set index entry has no collection")
            })?;
            let collection_id = CollectionId::parse(collection_name)?;
            let id = DocumentId::from_bytes(entry.id);
            let pointer = RecordPointer {
                offset: records_base
                    .checked_add(entry.relative_offset as u64)
                    .ok_or_else(|| {
                        StorageError::backend("GlacierStorage index pointer overflow")
                    })?,
                length: entry.length,
            };
            let collection = state.collections.entry(collection_id).or_default();
            let existed = collection.contains_id(&id);
            if existed {
                collection.primary_head_valid = false;
            } else {
                primary_head_insert(collection, id, pointer);
            }
            let index_version = IndexVersion {
                generation,
                version: DocumentVersion::new(entry.version),
                pointer: Some(pointer),
            };
            if existed {
                collection.push_existing(id, index_version);
            } else {
                collection.insert_new(id, index_version);
            }
        }
        INDEX_KIND_DELETE => {
            let collection_name = entry.collection.ok_or_else(|| {
                StorageError::backend("GlacierStorage delete index entry has no collection")
            })?;
            let collection_id = CollectionId::parse(collection_name)?;
            let id = DocumentId::from_bytes(entry.id);
            let collection = state.collections.entry(collection_id).or_default();
            collection.primary_head_valid = false;
            let index_version = IndexVersion {
                generation,
                version: DocumentVersion::INITIAL,
                pointer: None,
            };
            collection.push_existing(id, index_version);
        }
        INDEX_KIND_CLEAR => {
            state.clear_generations.push(generation);
            for collection in state.collections.values_mut() {
                collection.primary_head.clear();
                collection.primary_head_valid = true;
            }
        }
        other => {
            return Err(StorageError::backend(format!(
                "unknown GlacierStorage index entry kind {other}"
            )));
        }
    }
    Ok(())
}

/// Apply collection cardinality changes once per replayed segment.
///
/// The persisted metadata delta is the authoritative summary of inserts,
/// replacements and deletes for the segment. Using it here avoids deriving
/// counts from a primary index that is being mutated at the same generation,
/// which is especially important when replay starts from a compact checkpoint.
fn apply_replay_count_delta( state: &mut GlacierState, generation: u64, delta: &SegmentMetadataDelta, ) -> StorageResult<()> {
    if delta.clear {
        for collection in state.collections.values_mut() {
            collection.count_history.push((generation, 0));
        }
        return Ok(());
    }

    let base_generation = state.generation;
    for (collection_name, collection_delta) in &delta.collections {
        if collection_delta.documents == 0 {
            continue;
        }
        let collection_id = CollectionId::parse(collection_name.clone())?;
        let collection = state.collections.entry(collection_id).or_default();
        let current = visible_count(collection, base_generation);
        let updated = apply_signed_u64(
            current,
            collection_delta.documents,
            "replay collection document",
        )?;
        collection.count_history.push((generation, updated));
    }
    Ok(())
}

fn clear_generation_at(state: &GlacierState, generation: u64) -> u64 {
    state
        .clear_generations
        .iter()
        .copied()
        .filter(|clear| *clear <= generation)
        .max()
        .unwrap_or(0)
}

fn version_visible_after_clear( state: &GlacierState, generation: u64, version: IndexVersion, ) -> bool { version.generation <= generation && version.generation > clear_generation_at(state, generation) }

fn visible_index_version( state: &GlacierState, generation: u64, versions: &InlineIndexVersions, ) -> Option<IndexVersion> {
    let clear = clear_generation_at(state, generation);
    versions
        .iter_rev()
        .copied()
        .find(|version| version.generation <= generation && version.generation > clear)
}

fn visible_version( state: &GlacierState, generation: u64, collection: &CollectionId, id: &DocumentId, ) -> Option<IndexVersion> {
    state
        .collections
        .get(collection)
        .and_then(|collection| collection.visible_version(state, generation, id))
        .filter(|version| version.pointer.is_some())
}

fn visible_count(collection: &CollectionIndex, generation: u64) -> u64 {
    collection
        .count_history
        .iter()
        .rev()
        .find_map(|(g, count)| (*g <= generation).then_some(*count))
        .unwrap_or(0)
}

fn ensure_precondition( collection: &CollectionId, id: &DocumentId, actual: DocumentVersion, precondition: VersionPrecondition, ) -> StorageResult<()> {
    match precondition {
        VersionPrecondition::Any => Ok(()),
        VersionPrecondition::Exact(expected) if expected == actual => Ok(()),
        VersionPrecondition::Exact(expected) => Err(StorageError::version_conflict(
            collection.clone(),
            id.clone(),
            expected,
            actual,
        )),
    }
}

fn next_generation(current: u64) -> StorageResult<u64> {
    current
        .checked_add(1)
        .ok_or_else(|| StorageError::backend("GlacierStorage generation overflow"))
}

fn public_collection_metadata( name: &str, stats: &CollectionFieldStats, ) -> GlacierCollectionMetadata {
    GlacierCollectionMetadata {
        name: name.to_owned(),
        documents: stats.documents,
        fields: stats
            .fields
            .iter()
            .map(|(path, field)| {
                let non_null = field.present.saturating_sub(field.nulls);
                GlacierFieldMetadata {
                    path: path.clone(),
                    present: field.present,
                    nulls: field.nulls,
                    kinds: field.kinds.keys().cloned().collect(),
                    capabilities: if non_null == 0 {
                        Vec::new()
                    } else {
                        field
                            .capabilities
                            .iter()
                            .filter_map(|(name, count)| {
                                (*count == non_null).then_some(name.clone())
                            })
                            .collect()
                    },
                }
            })
            .collect(),
    }
}

fn initialize_file(path: &Path) -> StorageResult<GlacierFormatInfo> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent).map_err(io_error("create directory", parent))?;
    }
    let created_at_ms = now_millis()?;
    let format = GlacierFormatInfo {
        version: GLACIER_FORMAT_VERSION,
        page_size: GLACIER_PAGE_SIZE,
        created_at_ms,
        store_id: make_store_id(created_at_ms),
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io_error("create", path))?;
    file.write_all(&encode_superblock(format))
        .map_err(io_error("write superblock", path))?;
    file.sync_all().map_err(io_error("sync superblock", path))?;
    Ok(format)
}

fn read_superblock(path: &Path) -> StorageResult<GlacierFormatInfo> {
    let mut file = File::open(path).map_err(io_error("open", path))?;
    if file.metadata().map_err(io_error("stat", path))?.len() < GLACIER_SUPERBLOCK_BYTES as u64 {
        return Err(StorageError::backend("GlacierStorage file is truncated"));
    }
    let mut bytes = [0u8; GLACIER_SUPERBLOCK_BYTES];
    file.read_exact(&mut bytes)
        .map_err(io_error("read superblock", path))?;
    decode_superblock(&bytes)
}

fn encode_superblock(format: GlacierFormatInfo) -> [u8; GLACIER_SUPERBLOCK_BYTES] {
    let mut bytes = [0u8; GLACIER_SUPERBLOCK_BYTES];
    bytes[0..8].copy_from_slice(&MAGIC);
    bytes[8..10].copy_from_slice(&format.version.to_be_bytes());
    bytes[10..12].copy_from_slice(&HEADER_BYTES.to_be_bytes());
    bytes[12..16].copy_from_slice(&ENDIAN_MARKER.to_be_bytes());
    bytes[16..20].copy_from_slice(&format.page_size.to_be_bytes());
    bytes[24..32].copy_from_slice(&format.created_at_ms.to_be_bytes());
    bytes[32..48].copy_from_slice(&format.store_id);
    let checksum = checksum64(&bytes[..CHECKSUM_OFFSET]);
    bytes[CHECKSUM_OFFSET..64].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

fn decode_superblock(bytes: &[u8; GLACIER_SUPERBLOCK_BYTES]) -> StorageResult<GlacierFormatInfo> {
    if bytes[0..8] != MAGIC {
        return Err(StorageError::backend("invalid GlacierStorage magic"));
    }
    let version = u16::from_be_bytes([bytes[8], bytes[9]]);
    if version != GLACIER_FORMAT_VERSION {
        return Err(StorageError::backend(format!(
            "unsupported GlacierStorage format version {version}; 0.10.136 requires a fresh v5 store"
        )));
    }
    if u16::from_be_bytes([bytes[10], bytes[11]]) != HEADER_BYTES {
        return Err(StorageError::backend(
            "invalid GlacierStorage superblock length",
        ));
    }
    if u32::from_be_bytes(bytes[12..16].try_into().unwrap()) != ENDIAN_MARKER {
        return Err(StorageError::backend(
            "invalid GlacierStorage endian marker",
        ));
    }
    let page_size = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
    if page_size < 4096 || !page_size.is_power_of_two() {
        return Err(StorageError::backend(format!(
            "invalid GlacierStorage page size {page_size}"
        )));
    }
    let expected = u64::from_be_bytes(bytes[CHECKSUM_OFFSET..64].try_into().unwrap());
    if expected != checksum64(&bytes[..CHECKSUM_OFFSET]) {
        return Err(StorageError::backend(
            "GlacierStorage superblock checksum mismatch",
        ));
    }
    let created_at_ms = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
    let mut store_id = [0u8; 16];
    store_id.copy_from_slice(&bytes[32..48]);
    Ok(GlacierFormatInfo {
        version,
        page_size,
        created_at_ms,
        store_id,
    })
}

fn document_to_image(document: &Document) -> ImageValue {
    ImageValue::Object(
        document
            .iter()
            .map(|(name, value)| (name.as_str().to_owned(), value_to_image(value)))
            .collect(),
    )
}

fn value_to_image(value: &Value) -> ImageValue {
    match value {
        Value::Null => ImageValue::Null,
        Value::Bool(v) => ImageValue::Bool(*v),
        Value::String(v) => ImageValue::String(v.to_string()),
        Value::Array(v) => ImageValue::Array(v.iter().map(value_to_image).collect()),
        Value::Object(v) => document_to_image(v),
        Value::Number(Number::Signed(v)) => ImageValue::Signed(*v),
        Value::Number(Number::Unsigned(v)) => ImageValue::Unsigned(*v),
        Value::Number(Number::Float(v)) => ImageValue::Float(*v),
    }
}

fn decode_projected_image_value_reusing_string( bytes: &[u8], string_cache: &mut Option<Arc<str>>, ) -> StorageResult<(Value, ProjectedValueKind)> {
    let value: BorrowedProjectedImageValue<'_> = rmp_serde::from_slice(bytes).map_err(|error| {
        StorageError::backend(format!(
            "cannot decode GlacierStorage projected field: {error}"
        ))
    })?;

    match value {
        BorrowedProjectedImageValue::Null => Ok((Value::Null, ProjectedValueKind::Null)),
        BorrowedProjectedImageValue::Bool(value) => {
            Ok((Value::Bool(value), ProjectedValueKind::Bool))
        }
        BorrowedProjectedImageValue::Signed(value) => {
            Ok((Value::signed(value), ProjectedValueKind::Signed))
        }
        BorrowedProjectedImageValue::Unsigned(value) => {
            Ok((Value::unsigned(value), ProjectedValueKind::Unsigned))
        }
        BorrowedProjectedImageValue::Float(value) => Value::float(value)
            .map(|value| (value, ProjectedValueKind::Float))
            .map_err(|error| StorageError::backend(error.to_string())),
        BorrowedProjectedImageValue::String(value) => {
            if let Some(cached) = string_cache.as_ref() {
                if cached.as_ref() == value {
                    return Ok((
                        Value::String(Arc::clone(cached)),
                        ProjectedValueKind::StringHit,
                    ));
                }
                let owned = Arc::<str>::from(value);
                *string_cache = Some(Arc::clone(&owned));
                return Ok((Value::String(owned), ProjectedValueKind::StringReplacement));
            }
            let owned = Arc::<str>::from(value);
            *string_cache = Some(Arc::clone(&owned));
            Ok((Value::String(owned), ProjectedValueKind::StringMiss))
        }
        BorrowedProjectedImageValue::Array(_) | BorrowedProjectedImageValue::Object(_) => {
            let value: ImageValue = rmp_serde::from_slice(bytes).map_err(|error| {
                StorageError::backend(format!(
                    "cannot decode GlacierStorage projected complex field: {error}"
                ))
            })?;
            image_to_value(value).map(|value| (value, ProjectedValueKind::Complex))
        }
    }
}

fn image_to_value(value: ImageValue) -> StorageResult<Value> {
    Ok(match value {
        ImageValue::Null => Value::Null,
        ImageValue::Bool(v) => Value::Bool(v),
        ImageValue::Signed(v) => Value::signed(v),
        ImageValue::Unsigned(v) => Value::unsigned(v),
        ImageValue::Float(v) => {
            Value::float(v).map_err(|e| StorageError::backend(e.to_string()))?
        }
        ImageValue::String(v) => Value::string(v),
        ImageValue::Array(v) => Value::array(
            v.into_iter()
                .map(image_to_value)
                .collect::<StorageResult<Vec<_>>>()?,
        ),
        ImageValue::Object(fields) => {
            let mut document = Document::new();
            for (name, value) in fields {
                document.insert(name, image_to_value(value)?);
            }
            Value::object(document)
        }
    })
}

fn now_millis() -> StorageResult<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| StorageError::backend(format!("system clock before UNIX epoch: {e}")))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| StorageError::backend("GlacierStorage creation timestamp overflow"))
}
fn make_store_id(created_at_ms: u64) -> [u8; 16] {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let sequence =
        COUNTER.fetch_add(1, Ordering::Relaxed) ^ u64::from(std::process::id()).rotate_left(17);
    let mixed = created_at_ms.rotate_left(29) ^ sequence.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&created_at_ms.to_be_bytes());
    id[8..].copy_from_slice(&mixed.to_be_bytes());
    id
}
fn checksum64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}
fn io_error<'a>( operation: &'static str, path: &'a Path, ) -> impl FnOnce(std::io::Error) -> StorageError + 'a {
    move |error| {
        StorageError::backend(format!(
            "cannot {operation} GlacierStorage {}: {error}",
            path.display()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::UuidV7Generator;

    fn temp_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "og-glacier-v2-{label}-{}-{stamp}.glacier",
            std::process::id()
        ))
    }

    #[test] fn projected_scalar_string_reuses_owned_arc() { let bytes = rmp_serde::to_vec(&ImageValue::String("12-2025".to_owned())).unwrap(); let mut cache = None; let (first, first_kind) = decode_projected_image_value_reusing_string(&bytes, &mut cache).unwrap(); let (second, second_kind) = decode_projected_image_value_reusing_string(&bytes, &mut cache).unwrap(); let (Value::String(first), Value::String(second)) = (first, second) else { panic!("projected scalar decoder did not return strings"); }; assert!(matches!(first_kind, ProjectedValueKind::StringMiss)); assert!(matches!(second_kind, ProjectedValueKind::StringHit)); assert!(Arc::ptr_eq(&first, &second)); }
    #[test] fn creates_and_reopens_page_backed_store() { let path = temp_path("create"); let first = GlacierBackend::open(&path).unwrap(); assert_eq!(first.format_info().version(), 5); let second = GlacierBackend::open(&path).unwrap(); assert_eq!( second.format_info().store_id(), first.format_info().store_id() ); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn crud_persists_without_memory_documents() { let path = temp_path("crud"); let users = CollectionId::parse("users").unwrap(); let id = UuidV7Generator::new().next_id(); { let storage = GlacierBackend::open(&path).unwrap(); let mut doc = Document::new(); doc.insert("name", Value::string("Alice")); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(id.clone(), Arc::new(doc))], ) .unwrap(); assert_eq!(storage.document_count().unwrap(), 1); } { let storage = GlacierBackend::open(&path).unwrap(); let read = storage.read().unwrap(); let stored = read.get(&users, &id).unwrap().unwrap(); assert_eq!(stored.document().get("name"), Some(&Value::string("Alice"))); assert_eq!(read.count(&users).unwrap(), 1); } let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn snapshot_remains_generation_stable() { let path = temp_path("snapshot"); let users = CollectionId::parse("users").unwrap(); let storage = GlacierBackend::open(&path).unwrap(); let id1 = UuidV7Generator::new().next_id(); let id2 = UuidV7Generator::new().next_id(); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(id1, Arc::new(Document::new()))], ) .unwrap(); let snapshot = storage.read().unwrap(); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(id2, Arc::new(Document::new()))], ) .unwrap(); assert_eq!(snapshot.count(&users).unwrap(), 1); assert_eq!(storage.read().unwrap().count(&users).unwrap(), 2); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn batch_coalesces_count_history_per_generation() { let path = temp_path("count-history-coalesced"); let users = CollectionId::parse("users").unwrap(); let storage = GlacierBackend::open(&path).unwrap(); let generator = UuidV7Generator::new(); let mutations = (0..3) .map(|_| StorageMutation::insert(generator.next_id(), Arc::new(Document::new()))) .collect::<Vec<_>>(); storage .apply_batch_atomic_summary(&users, mutations) .unwrap(); let state = storage.state_read().unwrap(); let collection = state.collections.get(&users).unwrap(); assert_eq!(collection.count_history.len(), 1); assert_eq!(visible_count(collection, state.generation), 3); drop(state); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn startup_metrics_describe_rebuilt_segments_and_records() { let path = temp_path("startup-metrics"); let users = CollectionId::parse("users").unwrap(); let id = UuidV7Generator::new().next_id(); { let storage = GlacierBackend::open(&path).unwrap(); let mut doc = Document::new(); doc.insert("name", Value::string("Alice")); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(id, Arc::new(doc))], ) .unwrap(); } let reopened = GlacierBackend::open(&path).unwrap(); let metrics = reopened.startup_metrics(); assert_eq!(metrics.segments, 1); assert_eq!(metrics.records, 1); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn projected_scan_metrics_count_physical_read_work() { let path = temp_path("read-metrics"); let users = CollectionId::parse("users").unwrap(); let id = UuidV7Generator::new().next_id(); let storage = GlacierBackend::open(&path).unwrap(); let mut doc = Document::new(); doc.insert("name", Value::string("Alice")); doc.insert("city", Value::string("Paris")); storage .apply_batch_atomic_summary(&users, vec![StorageMutation::insert(id, Arc::new(doc))]) .unwrap(); let read = storage.read().unwrap(); let fields = [FieldPath::parse("name").unwrap()]; let mut visited = 0u64; read.scan_projected_unordered_each(&users, ScanOptions::default(), &fields, &mut |_| { visited += 1; Ok(true) }) .unwrap(); let metrics = storage.read_metrics(); assert_eq!(visited, 1); assert_eq!(metrics.scans, 1); assert_eq!(metrics.segments, 1); assert_eq!(metrics.records, 1); assert_eq!(metrics.projected_records, 1); assert_eq!(metrics.decoded_fields, 1); assert_eq!(metrics.mmap_segments, 0); assert_eq!(metrics.mmap_bypass_segments, 1); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[cfg(all(target_pointer_width = "64", target_family = "unix"))]
    #[test] fn projected_scan_reuses_mmap_and_refreshes_after_append() { let path = temp_path("read-mmap"); let users = CollectionId::parse("users").unwrap(); let storage = GlacierBackend::open(&path).unwrap(); let fields = [FieldPath::parse("name").unwrap()]; let first_id = UuidV7Generator::new().next_id(); let mut first = Document::new(); first.insert("name", Value::string("Alice")); first.insert( "payload", Value::string("x".repeat(MIN_MMAP_SEGMENT_PAYLOAD_BYTES * 2)), ); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(first_id, Arc::new(first))], ) .unwrap(); let read = storage.read().unwrap(); for _ in 0..2 { let mut visited = 0u64; read.scan_projected_unordered_each( &users, ScanOptions::default(), &fields, &mut |_| { visited += 1; Ok(true) }, ) .unwrap(); assert_eq!(visited, 1); } let metrics = storage.read_metrics(); assert_eq!(metrics.mmap_map_creates, 1); assert_eq!(metrics.mmap_reuses, 1); assert_eq!(metrics.mmap_remaps, 0); assert_eq!(metrics.segment_catalog_refreshes, 1); assert_eq!(metrics.segment_catalog_hits, 1); assert_eq!(metrics.segment_catalog_rebuilds, 0); assert_eq!(metrics.mmap_segments, 2); assert!(metrics.mmap_bytes >= (MIN_MMAP_SEGMENT_PAYLOAD_BYTES as u64) * 2); assert_eq!(metrics.mmap_fallback_segments, 0); let second_id = UuidV7Generator::new().next_id(); let mut second = Document::new(); second.insert("name", Value::string("Bob")); second.insert( "payload", Value::string("y".repeat(MIN_MMAP_SEGMENT_PAYLOAD_BYTES * 2)), ); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(second_id, Arc::new(second))], ) .unwrap(); let read = storage.read().unwrap(); let mut visited = 0u64; read.scan_projected_unordered_each( &users, ScanOptions::default(), &fields, &mut |_| { visited += 1; Ok(true) }, ) .unwrap(); assert_eq!(visited, 2); let metrics = storage.read_metrics(); assert_eq!(metrics.mmap_map_creates, 1); assert_eq!(metrics.mmap_reuses, 1); assert_eq!(metrics.mmap_remaps, 1); assert_eq!(metrics.segment_catalog_refreshes, 2); assert_eq!(metrics.segment_catalog_hits, 1); assert_eq!(metrics.segment_catalog_rebuilds, 0); assert_eq!(metrics.mmap_segments, 4); assert_eq!(metrics.mmap_fallback_segments, 0); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn startup_replay_seeds_segment_catalog_validation() { let path = temp_path("segment-catalog-startup"); let users = CollectionId::parse("users").unwrap(); let id = UuidV7Generator::new().next_id(); { let storage = GlacierBackend::open(&path).unwrap(); let mut doc = Document::new(); doc.insert("name", Value::string("Alice")); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(id, Arc::new(doc))], ) .unwrap(); } let reopened = GlacierBackend::open(&path).unwrap(); let startup = reopened.startup_metrics(); assert_eq!(startup.segment_catalog_segments, 1); let catalog = reopened .inner .segment_catalog .lock() .unwrap_or_else(|poisoned| poisoned.into_inner()); assert_eq!(catalog.segments.len(), 1); let entry = &catalog.segments[0]; assert!(entry.directory_verified.load(Ordering::Acquire)); assert_eq!(entry.known_insert_collection(), Some("users")); drop(catalog); let resident = reopened.resident_memory(); assert_eq!(resident.segment_catalog_entries, 1); assert!(resident.segment_catalog_estimated_bytes > 0); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn physical_projection_skips_unrequested_field_payload_decode() { let name = rmp_serde::to_vec(&ImageValue::String("Alice".to_owned())).unwrap(); let ignored = rmp_serde::to_vec(&ImageValue::String("Paris".to_owned())).unwrap(); let document = ImageDocument { id: [7u8; 16], version: 3, fields: vec![ ImageField { name: "name".to_owned(), value: name, }, ImageField { name: "city".to_owned(), value: ignored, }, ], }; let mut bytes = encode_physical_set_record(9, &document).unwrap(); assert_eq!(&bytes[..8], &PHYSICAL_SET_MAGIC); let header = parse_physical_set_header(&bytes).unwrap().unwrap(); let city = physical_field_entries(&bytes, header) .unwrap() .into_iter() .find(|entry| entry.name == "city") .map(|entry| (entry.offset, entry.length)) .unwrap(); let payload_base = PHYSICAL_SET_HEADER_BYTES + header.directory_len; bytes[payload_base + city.0] = 0xc1; let directory = &bytes[PHYSICAL_SET_HEADER_BYTES..payload_base]; let payloads = &bytes[payload_base..]; let checksum = checksum64_pair(directory, payloads); bytes[56..64].copy_from_slice(&checksum.to_be_bytes()); let fields = [FieldPath::parse("name").unwrap()]; let requested = fields .iter() .map(|path| path.first().as_str()) .collect::<BTreeSet<_>>(); let (projected, decoded_fields) = decode_projected_physical_set( &bytes, parse_physical_set_header(&bytes).unwrap().unwrap(), &fields, Some(&requested), ) .unwrap(); assert_eq!(decoded_fields, 1); assert!(projected.get("name").is_some()); assert!(decode_physical_set_document( &bytes, parse_physical_set_header(&bytes).unwrap().unwrap(), ) .is_err()); }
    #[test] fn projected_values_append_only_uses_trusted_compiled_path() { let path = temp_path("projected-values-trusted"); let users = CollectionId::parse("users").unwrap(); let storage = GlacierBackend::open(&path).unwrap(); let ids = UuidV7Generator::new().reserve(2); for (id, name) in ids.into_iter().zip(["Alice", "Bob"]) { let mut document = Document::new(); document.insert("name", Value::string(name)); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(id, Arc::new(document))], ) .unwrap(); } let fields = [FieldPath::parse("name").unwrap()]; let read = storage.read().unwrap(); let mut projected = Vec::new(); read.scan_projected_values_unordered_each( &users, ScanOptions::default(), &fields, &mut |values| { projected.push(values[0].clone()); Ok(true) }, ) .unwrap(); assert_eq!(projected.len(), 2); assert!(projected.contains(&Some(Value::string("Alice")))); assert!(projected.contains(&Some(Value::string("Bob")))); let metrics = storage.read_metrics(); assert_eq!(metrics.visibility_fast_scans, 1); assert_eq!(metrics.visibility_fallback_scans, 0); assert_eq!(metrics.trusted_header_records, 2); assert_eq!(metrics.verified_header_records, 0); assert_eq!(metrics.projection_layout_misses, 1); assert_eq!(metrics.projection_layout_hits, 1); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn projected_values_fallback_after_replace_preserves_result() { let path = temp_path("projected-values-replace-fallback"); let users = CollectionId::parse("users").unwrap(); let id = UuidV7Generator::new().next_id(); let storage = GlacierBackend::open(&path).unwrap(); let mut original = Document::new(); original.insert("name", Value::string("Alice")); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(id, Arc::new(original))], ) .unwrap(); let mut replacement = Document::new(); replacement.insert("name", Value::string("Bob")); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::replace( id, Arc::new(replacement), VersionPrecondition::Any, )], ) .unwrap(); let fields = [FieldPath::parse("name").unwrap()]; let read = storage.read().unwrap(); let mut projected = Vec::new(); read.scan_projected_values_unordered_each( &users, ScanOptions::default(), &fields, &mut |values| { projected.push(values[0].clone()); Ok(true) }, ) .unwrap(); assert_eq!(projected, vec![Some(Value::string("Bob"))]); let metrics = storage.read_metrics(); assert_eq!(metrics.visibility_fast_scans, 0); assert_eq!(metrics.visibility_fallback_scans, 1); assert!(metrics.visibility_checks > 0); assert!(metrics.verified_header_records > 0); assert_eq!(metrics.trusted_header_records, 0); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn compiled_projection_layout_relearns_when_field_order_changes() { let generator = UuidV7Generator::new(); let fields = [FieldPath::parse("name").unwrap()]; let mut values: Vec<Option<Value>> = vec![None]; let mut layout = PhysicalProjectionLayout::default(); let first = ImageDocument { id: *generator.next_id().as_bytes(), version: 1, fields: vec![ ImageField { name: "name".to_owned(), value: rmp_serde::to_vec(&ImageValue::String("Alice".to_owned())).unwrap(), }, ImageField { name: "city".to_owned(), value: rmp_serde::to_vec(&ImageValue::String("Paris".to_owned())).unwrap(), }, ], }; let first_bytes = encode_physical_set_record(1, &first).unwrap(); let first_header = parse_physical_set_header(&first_bytes).unwrap().unwrap(); assert_eq!( decode_physical_projected_values_into( &first_bytes, first_header, &fields, &mut values, &mut layout, None, None, ) .unwrap(), (1, false) ); assert_eq!(values[0], Some(Value::string("Alice"))); let second = ImageDocument { id: *generator.next_id().as_bytes(), version: 1, fields: vec![ ImageField { name: "city".to_owned(), value: rmp_serde::to_vec(&ImageValue::String("Lyon".to_owned())).unwrap(), }, ImageField { name: "name".to_owned(), value: rmp_serde::to_vec(&ImageValue::String("Bob".to_owned())).unwrap(), }, ], }; let second_bytes = encode_physical_set_record(2, &second).unwrap(); let second_header = parse_physical_set_header(&second_bytes).unwrap().unwrap(); assert_eq!(first_header.field_count, second_header.field_count); assert_eq!(first_header.directory_len, second_header.directory_len); assert_eq!( decode_physical_projected_values_into( &second_bytes, second_header, &fields, &mut values, &mut layout, None, None, ) .unwrap(), (1, false) ); assert_eq!(values[0], Some(Value::string("Bob"))); }
    #[test] fn trusted_physical_header_skips_only_record_checksum() { let id = UuidV7Generator::new().next_id(); let stored = StoredDocument::new( id, DocumentVersion::INITIAL, Arc::new({ let mut document = Document::new(); document.insert("name", Value::string("Alice")); document }), ) .unwrap(); let image = image_document(&stored).unwrap(); let mut bytes = encode_physical_set_record(7, &image).unwrap(); bytes[56] ^= 0x01; assert!(parse_physical_set_header(&bytes).is_err()); let trusted = parse_trusted_physical_set_header(&bytes) .unwrap() .expect("trusted analytical parser accepts valid framing"); assert_eq!(trusted.id, *id.as_bytes()); assert_eq!(trusted.version, DocumentVersion::INITIAL.get()); let truncated = &bytes[..bytes.len() - 1]; assert!(parse_trusted_physical_set_header(truncated).is_err()); }
    #[test] fn persisted_set_records_use_physical_field_directory() { let path = temp_path("physical-directory"); let users = CollectionId::parse("users").unwrap(); let id = UuidV7Generator::new().next_id(); let storage = GlacierBackend::open(&path).unwrap(); let mut doc = Document::new(); doc.insert("name", Value::string("Alice")); doc.insert("city", Value::string("Paris")); storage .apply_batch_atomic_summary(&users, vec![StorageMutation::insert(id, Arc::new(doc))]) .unwrap(); let pointer = { let state = storage.state_read().unwrap(); visible_version(&state, state.generation, &users, &id) .unwrap() .pointer .unwrap() }; let mut file = File::open(&path).unwrap(); file.seek(SeekFrom::Start(pointer.offset)).unwrap(); let mut magic = [0u8; 8]; file.read_exact(&mut magic).unwrap(); assert_eq!(magic, PHYSICAL_SET_MAGIC); let stored = storage.read().unwrap().get(&users, &id).unwrap().unwrap(); assert_eq!(stored.document().len(), 2); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn primary_head_tracks_smallest_ids_without_requiring_insert_order() { let generator = UuidV7Generator::new(); let mut ids = generator.reserve(6).collect::<Vec<_>>(); let expected = ids[..3].to_vec(); ids.reverse(); let mut collection = CollectionIndex::default(); for (ordinal, id) in ids.into_iter().enumerate() { primary_head_insert( &mut collection, id, RecordPointer { offset: 100 + ordinal as u64, length: 10, }, ); } let head = primary_head_entries(&collection, 3).unwrap(); assert_eq!(head.iter().map(|(id, _)| *id).collect::<Vec<_>>(), expected); }
    #[test] fn invalid_primary_head_forces_bounded_fallback() { let mut collection = CollectionIndex::default(); collection.primary_head_valid = false; assert!(primary_head_entries(&collection, 1).is_none()); }
    #[test] fn primary_batch_append_updates_tail_and_count_once() { let path = temp_path("primary-batch-append"); let collection = CollectionId::parse("users").unwrap(); let target = primary_index_path(&path, &collection); let mut file = File::create(&target).unwrap(); file.write_all(PRIMARY_INDEX_MAGIC.as_slice()).unwrap(); file.write_all(&GLACIER_FORMAT_VERSION.to_be_bytes()) .unwrap(); file.write_all(&0u16.to_be_bytes()).unwrap(); file.write_all(&0u32.to_be_bytes()).unwrap(); file.write_all(&0u64.to_be_bytes()).unwrap(); drop(file); let ids = UuidV7Generator::new().reserve(3).collect::<Vec<_>>(); let mut index = DiskPrimaryIndex { path: target.clone(), count: 0, last_id: None, }; let mut entries = ids .iter() .enumerate() .map(|(ordinal, id)| CompactPrimaryEntry { id: *id, generation: 1, version: DocumentVersion::INITIAL, pointer: RecordPointer { offset: 100 + ordinal as u64, length: 10, }, }) .collect::<Vec<_>>(); assert!(disk_primary_append_batch(&mut index, &mut entries).unwrap()); assert_eq!(index.count, 3); assert_eq!(index.last_id, ids.last().copied()); let _ = fs::remove_file(&target); let _ = fs::remove_file(&path); }
    #[test] fn page_backed_primary_rejects_ids_beyond_tail_without_disk_lookup() { let collection = CollectionId::parse("users").unwrap(); let ids = UuidV7Generator::new().reserve(2).collect::<Vec<_>>(); let mut index = CollectionIndex::default(); index.disk_primary = Some(DiskPrimaryIndex { path: PathBuf::from("/definitely/not/read"), count: 1, last_id: Some(ids[0]), }); let state = GlacierState { generation: 7, ..GlacierState::default() }; assert!(ids[1] > ids[0]); assert!(index.visible_version(&state, 7, &ids[1]).is_none()); let _ = collection; }
    #[test] fn page_backed_primary_lookup_works_without_resident_primary_entries() { let path = temp_path("primary-sidecar"); let collection = CollectionId::parse("users").unwrap(); let ids = UuidV7Generator::new().reserve(3).collect::<Vec<_>>(); let documents = ids .iter() .enumerate() .map(|(ordinal, id)| CheckpointDocument { id: id.into_bytes(), version: 1, offset: 100 + ordinal as u64 * 20, length: 10, }) .collect::<Vec<_>>(); let disk = rebuild_disk_primary_documents(&path, &collection, 7, &documents).unwrap(); let mut index = CollectionIndex::default(); index.disk_primary = Some(disk); assert!(index.primary.is_empty()); let mut state = GlacierState::default(); state.generation = 7; let found = index.visible_version(&state, 7, &ids[1]).unwrap(); assert_eq!(found.pointer.unwrap().offset, 120); if let Some(disk) = index.disk_primary { let _ = fs::remove_file(disk.path); } let _ = fs::remove_file(path); }
    #[test] fn streaming_checkpoint_loader_does_not_materialize_primary_documents() { let path = temp_path("streaming-checkpoint"); let format = initialize_file(&path).unwrap(); let data_len = GLACIER_SUPERBLOCK_BYTES as u64 + 1024; OpenOptions::new() .write(true) .open(&path) .unwrap() .set_len(data_len) .unwrap(); let ids = UuidV7Generator::new().reserve(3).collect::<Vec<_>>(); let checkpoint = PersistentCheckpoint { format_version: format.version(), store_id: format.store_id(), generation: 7, data_len, collections: vec![CheckpointCollection { name: "users".to_owned(), count: 3, documents: ids .iter() .enumerate() .map(|(ordinal, id)| CheckpointDocument { id: id.into_bytes(), version: 1, offset: GLACIER_SUPERBLOCK_BYTES as u64 + 100 + ordinal as u64 * 10, length: 5, }) .collect(), }], metadata: FieldCatalog::default(), }; let bytes = rmp_serde::to_vec(&checkpoint).unwrap(); let mut de = rmp_serde::Deserializer::new(bytes.as_slice()); let (state, decoded_data_len, generation) = StreamingCheckpointSeed { store_path: &path, format, data_file_len: data_len, cache_entries: 2, } .deserialize(&mut de) .unwrap(); assert_eq!(decoded_data_len, data_len); assert_eq!(generation, 7); let users = CollectionId::parse("users").unwrap(); let collection = state.collections.get(&users).unwrap(); assert_eq!(collection.primary.len(), 2); assert_eq!(collection.disk_primary.as_ref().unwrap().count, 3); let _ = fs::remove_file(&path); let _ = fs::remove_file(primary_index_path(&path, &users)); }
    #[test] fn primary_cache_reservation_is_opportunistic_and_bounded() { let governor = MemoryGovernor::with_process_limit(256 * 1024 * 1024); let (entries, reservation) = opportunistic_primary_cache_reservation(Some(&governor)); assert!(entries > 0); assert!(reservation.is_some()); assert!( governor .snapshot() .classes .iter() .find(|class| class.class == MemoryClass::PageCache) .unwrap() .current_bytes <= governor.profile().managed_budget_bytes.unwrap() / PRIMARY_CACHE_FRACTION_DENOMINATOR ); }
    #[test] fn compact_primary_keeps_ordered_ids_inline_and_spills_only_exceptions() { let generator = UuidV7Generator::new(); let ids = generator.reserve(4).collect::<Vec<_>>(); let mut index = CollectionIndex::default(); for (ordinal, id) in ids.iter().copied().enumerate() { index.insert_new( id, IndexVersion { generation: 1, version: DocumentVersion::INITIAL, pointer: Some(RecordPointer { offset: 100 + ordinal as u64, length: 10, }), }, ); } assert_eq!(index.primary.len(), 4); assert!(index.exceptions.is_empty()); let replacement = IndexVersion { generation: 2, version: DocumentVersion::new(2), pointer: Some(RecordPointer { offset: 999, length: 11, }), }; index.push_existing(ids[1], replacement); assert_eq!(index.primary.len(), 4); assert_eq!(index.exceptions.len(), 1); let mut state = GlacierState::default(); state.generation = 2; let visible = index.visible_version(&state, 2, &ids[1]).unwrap(); assert_eq!(visible.generation, 2); assert_eq!(visible.pointer.unwrap().offset, 999); }
    #[test] fn inline_index_versions_allocate_history_only_after_second_version() { let first = IndexVersion { generation: 1, version: DocumentVersion::INITIAL, pointer: Some(RecordPointer { offset: 100, length: 10, }), }; let second = IndexVersion { generation: 2, version: DocumentVersion::new(2), pointer: Some(RecordPointer { offset: 200, length: 11, }), }; let mut versions = InlineIndexVersions::new(first); assert_eq!(versions.len(), 1); assert_eq!(versions.heap_capacity(), 0); assert_eq!(versions.iter_rev().next().copied().unwrap().generation, 1); versions.push(second); assert_eq!(versions.len(), 2); assert!(versions.heap_capacity() >= 2); let replayed = versions .iter_rev() .map(|version| version.generation) .collect::<Vec<_>>(); assert_eq!(replayed, vec![2, 1]); }
    #[test] fn bounded_limit_selection_preserves_primary_id_order() { let mut state = GlacierState::default(); state.generation = 1; let collection = CollectionId::parse("users").unwrap(); let mut index = CollectionIndex::default(); let mut ids = UuidV7Generator::new().reserve(5).collect::<Vec<_>>(); ids.reverse(); for (ordinal, id) in ids.into_iter().enumerate() { index.insert_new( id, IndexVersion { generation: 1, version: DocumentVersion::INITIAL, pointer: Some(RecordPointer { offset: 100 + ordinal as u64, length: 10, }), }, ); } index.count_history.push((1, 5)); state.collections.insert(collection.clone(), index); let index = state.collections.get(&collection).unwrap(); let forward = select_visible_entries_bounded(&state, index, 1, ScanDirection::Forward, 2); assert_eq!(forward.len(), 2); assert!(forward[0].0 < forward[1].0); let reverse = select_visible_entries_bounded(&state, index, 1, ScanDirection::Reverse, 2); assert_eq!(reverse.len(), 2); assert!(reverse[0].0 > reverse[1].0); assert!(reverse[0].0 > forward[1].0); }
    #[test] fn checkpointless_open_does_not_make_historical_bytes_immediately_due() { let replay_offset = GLACIER_SUPERBLOCK_BYTES as u64; let data_len = 6 * 1024 * 1024 * 1024u64; let next = next_checkpoint_offset_after_open(false, replay_offset, data_len); assert_eq!(next, data_len.saturating_mul(2)); assert!(next > data_len.saturating_add(1)); }
    #[test] fn checkpointed_open_keeps_growth_relative_to_checkpoint_boundary() { let replay_offset = 512 * 1024 * 1024u64; let data_len = replay_offset + 64 * 1024 * 1024u64; let next = next_checkpoint_offset_after_open(true, replay_offset, data_len); assert_eq!(next, replay_offset.saturating_mul(2)); }
    #[test] fn automatic_checkpoint_interval_grows_geometrically() { assert_eq!( automatic_checkpoint_interval(0), MIN_CHECKPOINT_INTERVAL_BYTES ); assert_eq!( automatic_checkpoint_interval(MIN_CHECKPOINT_INTERVAL_BYTES / 2), MIN_CHECKPOINT_INTERVAL_BYTES ); assert_eq!( automatic_checkpoint_interval(MIN_CHECKPOINT_INTERVAL_BYTES), MIN_CHECKPOINT_INTERVAL_BYTES ); assert_eq!( automatic_checkpoint_interval(MIN_CHECKPOINT_INTERVAL_BYTES * 4), MIN_CHECKPOINT_INTERVAL_BYTES * 4 ); }
    #[test] fn failed_automatic_checkpoint_is_deferred_past_current_store_size() { let checkpoint_offset = MIN_CHECKPOINT_INTERVAL_BYTES * 4; let data_len = checkpoint_offset * 2; let next = checkpoint_retry_offset_after_failure(data_len, checkpoint_offset); assert_eq!( next, data_len + checkpoint_offset * CHECKPOINT_FAILURE_BACKOFF_MULTIPLIER ); assert!(next > data_len); }
    #[test] fn checkpoint_restores_index_and_replays_only_tail() { let path = temp_path("checkpoint-tail"); let users = CollectionId::parse("users").unwrap(); let generator = UuidV7Generator::new(); let mut ids = generator.reserve(2); let first_id = ids.next().unwrap(); let second_id = ids.next().unwrap(); { let storage = GlacierBackend::open(&path).unwrap(); let mut first = Document::new(); first.insert("name", Value::string("Alice")); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(first_id, Arc::new(first))], ) .unwrap(); assert_eq!( storage .collection_metadata(&users) .unwrap() .unwrap() .documents(), 1 ); storage.checkpoint().unwrap(); let checkpoint_metrics = storage.write_metrics(); assert_eq!(checkpoint_metrics.checkpoint_runs, 1); assert_eq!(checkpoint_metrics.checkpoint_failures, 0); assert_eq!(checkpoint_metrics.checkpoint_documents, 1); assert!(checkpoint_metrics.checkpoint_bytes > CHECKPOINT_HEADER_BYTES as u64); assert!( checkpoint_metrics.checkpoint_total_us >= checkpoint_metrics.checkpoint_build_us ); assert!( checkpoint_metrics.checkpoint_write_us >= checkpoint_metrics.checkpoint_encode_us ); let mut second = Document::new(); second.insert("name", Value::string("Bob")); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(second_id, Arc::new(second))], ) .unwrap(); } let reopened = GlacierBackend::open(&path).unwrap(); let metrics = reopened.startup_metrics(); assert_eq!(metrics.checkpoint_loaded, 1); assert_eq!(metrics.checkpoint_generation, 1); assert!(metrics.checkpoint_bytes > CHECKPOINT_HEADER_BYTES as u64); assert_eq!(metrics.segments, 1); assert_eq!(metrics.records, 1); assert_eq!(reopened.generation().unwrap(), 2); assert_eq!(reopened.document_count().unwrap(), 2); assert_eq!( reopened .collection_metadata(&users) .unwrap() .unwrap() .documents(), 2 ); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
    #[test] fn corrupt_checkpoint_falls_back_to_full_segment_replay() { let path = temp_path("checkpoint-corrupt"); let users = CollectionId::parse("users").unwrap(); let id = UuidV7Generator::new().next_id(); { let storage = GlacierBackend::open(&path).unwrap(); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(id, Arc::new(Document::new()))], ) .unwrap(); storage.checkpoint().unwrap(); } let checkpoint = checkpoint_path(&path); let mut bytes = fs::read(&checkpoint).unwrap(); bytes[0] ^= 0xff; fs::write(&checkpoint, bytes).unwrap(); let reopened = GlacierBackend::open(&path).unwrap(); let metrics = reopened.startup_metrics(); assert_eq!(metrics.checkpoint_loaded, 0); assert_eq!(metrics.segments, 1); assert_eq!(metrics.records, 1); assert_eq!(reopened.document_count().unwrap(), 1); let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint); }
    #[test] fn append_only_visibility_fast_path_requires_current_single_versions() { let path = temp_path("append-only-visibility"); let users = CollectionId::parse("users").unwrap(); let storage = GlacierBackend::open(&path).unwrap(); let ids = UuidV7Generator::new().reserve(2); for id in ids { storage .apply_batch_atomic_summary( &users, vec![StorageMutation::insert(id, Arc::new(Document::new()))], ) .unwrap(); } { let state = storage.state_read().unwrap(); let collection = state.collections.get(&users).unwrap(); assert!(append_only_visibility_is_trivial( &state, collection, state.generation )); assert!(!append_only_visibility_is_trivial( &state, collection, state.generation.saturating_sub(1) )); } let id = storage .read() .unwrap() .scan(&users, ScanOptions::default()) .unwrap()[0] .id() .clone(); storage .apply_batch_atomic_summary( &users, vec![StorageMutation::replace( id, Arc::new(Document::new()), VersionPrecondition::Any, )], ) .unwrap(); { let state = storage.state_read().unwrap(); let collection = state.collections.get(&users).unwrap(); assert!(!append_only_visibility_is_trivial( &state, collection, state.generation )); } { let mut transaction = storage.begin().unwrap(); transaction .delete(&users, &id, VersionPrecondition::Any) .unwrap(); transaction.commit().unwrap(); } { let state = storage.state_read().unwrap(); let collection = state.collections.get(&users).unwrap(); assert!(!append_only_visibility_is_trivial( &state, collection, state.generation )); } storage.clear().unwrap(); { let state = storage.state_read().unwrap(); let collection = state.collections.get(&users).unwrap(); assert!(!append_only_visibility_is_trivial( &state, collection, state.generation )); } let _ = fs::remove_file(&path); let _ = fs::remove_file(checkpoint_path(&path)); }
}
