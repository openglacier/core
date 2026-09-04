//! Query module exports.

pub mod backend;
mod common;
pub mod glacier;
pub mod memory;
pub use backend::{
    glacier::{
        GlacierBackend, GlacierCollectionMetadata, GlacierFieldMetadata, GlacierFormatInfo,
        GLACIER_FORMAT_VERSION, GLACIER_PAGE_SIZE,
    },
    memory::MemoryBackend,
    StorageBackend,
};
pub use glacier::GlacierStorage;
pub use memory::MemoryStorage;

use common::validate_collection_id;
use std::{
    error::Error as StdError,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::model::{Document, FieldPath, FieldPathSegment, Value};

/// Result returned by storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;

/// Diagnostic counters for the borrowed projected-value group consumer.
///
/// Timings are sampled by the execution engine and published in nanoseconds;
/// hit/miss/materialization counters are exact. Keeping these counters outside
/// the row loop avoids adding atomics to the analytical hot path.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct GroupConsumerProbeSnapshot {
    pub samples: u64,
    pub key_encode_ns: u64,
    pub lookup_ns: u64,
    pub insert_ns: u64,
    pub aggregate_ns: u64,
    pub lookup_hits: u64,
    pub lookup_misses: u64,
    pub key_materializations: u64,
}

static GROUP_CONSUMER_SAMPLES: AtomicU64 = AtomicU64::new(0);
static GROUP_CONSUMER_KEY_ENCODE_NS: AtomicU64 = AtomicU64::new(0);
static GROUP_CONSUMER_LOOKUP_NS: AtomicU64 = AtomicU64::new(0);
static GROUP_CONSUMER_INSERT_NS: AtomicU64 = AtomicU64::new(0);
static GROUP_CONSUMER_AGGREGATE_NS: AtomicU64 = AtomicU64::new(0);
static GROUP_CONSUMER_LOOKUP_HITS: AtomicU64 = AtomicU64::new(0);
static GROUP_CONSUMER_LOOKUP_MISSES: AtomicU64 = AtomicU64::new(0);
static GROUP_CONSUMER_KEY_MATERIALIZATIONS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_group_consumer_probe(delta: GroupConsumerProbeSnapshot) {
    GROUP_CONSUMER_SAMPLES.fetch_add(delta.samples, Ordering::Relaxed);
    GROUP_CONSUMER_KEY_ENCODE_NS.fetch_add(delta.key_encode_ns, Ordering::Relaxed);
    GROUP_CONSUMER_LOOKUP_NS.fetch_add(delta.lookup_ns, Ordering::Relaxed);
    GROUP_CONSUMER_INSERT_NS.fetch_add(delta.insert_ns, Ordering::Relaxed);
    GROUP_CONSUMER_AGGREGATE_NS.fetch_add(delta.aggregate_ns, Ordering::Relaxed);
    GROUP_CONSUMER_LOOKUP_HITS.fetch_add(delta.lookup_hits, Ordering::Relaxed);
    GROUP_CONSUMER_LOOKUP_MISSES.fetch_add(delta.lookup_misses, Ordering::Relaxed);
    GROUP_CONSUMER_KEY_MATERIALIZATIONS.fetch_add(delta.key_materializations, Ordering::Relaxed);
}

pub(crate) fn group_consumer_probe_snapshot() -> GroupConsumerProbeSnapshot {
    GroupConsumerProbeSnapshot {
        samples: GROUP_CONSUMER_SAMPLES.load(Ordering::Relaxed),
        key_encode_ns: GROUP_CONSUMER_KEY_ENCODE_NS.load(Ordering::Relaxed),
        lookup_ns: GROUP_CONSUMER_LOOKUP_NS.load(Ordering::Relaxed),
        insert_ns: GROUP_CONSUMER_INSERT_NS.load(Ordering::Relaxed),
        aggregate_ns: GROUP_CONSUMER_AGGREGATE_NS.load(Ordering::Relaxed),
        lookup_hits: GROUP_CONSUMER_LOOKUP_HITS.load(Ordering::Relaxed),
        lookup_misses: GROUP_CONSUMER_LOOKUP_MISSES.load(Ordering::Relaxed),
        key_materializations: GROUP_CONSUMER_KEY_MATERIALIZATIONS.load(Ordering::Relaxed),
    }
}

/// Scalar projected value borrowed directly from storage bytes when possible.
///
/// This representation lets analytical consumers inspect numeric/string values
/// without forcing an owned [`Value`] allocation for every source row. Complex
/// values retain an owned fallback to preserve the full storage contract.
#[derive(Clone, Debug)]
pub enum ProjectedValueRef<'a> {
    Null,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(&'a str),
    Owned(Value),
}

impl ProjectedValueRef<'_> {
    #[must_use]
    pub fn to_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(*value),
            Self::Signed(value) => Value::signed(*value),
            Self::Unsigned(value) => Value::unsigned(*value),
            Self::Float(value) => Value::float(*value).expect("stored finite float"),
            Self::String(value) => Value::string(*value),
            Self::Owned(value) => value.clone(),
        }
    }

    #[must_use]
    pub const fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Signed(value) => Some(*value as f64),
            Self::Unsigned(value) => Some(*value as f64),
            Self::Float(value) => Some(*value),
            _ => None,
        }
    }
}

/// Logical collection identifier.
///
/// Collection names are validated once at the storage boundary and then shared
/// cheaply through [`Arc`].
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CollectionId(Arc<str>);

impl CollectionId {
    /// Parses and validates a collection identifier.
    ///
    /// Qualified names such as `_og.events` are accepted.
    ///
    /// # Errors
    ///
    /// Returns [`StorageErrorKind::InvalidCollectionId`] when the identifier is
    /// empty or contains an invalid segment.
    pub fn parse(value: impl AsRef<str>) -> StorageResult<Self> {
        let value = value.as_ref();

        validate_collection_id(value)?;

        Ok(Self(Arc::from(value)))
    }

    /// Returns the collection identifier as a string slice.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this collection belongs to the `_og` namespace.
    #[must_use]
    pub fn is_system(&self) -> bool {
        self.as_str() == "_og" || self.as_str().starts_with("_og.")
    }

    /// Returns the number of qualified name segments.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.as_str().split('.').count()
    }

    /// Iterates over qualified name segments.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.as_str().split('.')
    }
}

impl fmt::Debug for CollectionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CollectionId")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for CollectionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for CollectionId {
    type Error = StorageError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for CollectionId {
    type Error = StorageError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

pub mod document_id;
pub use document_id::{DocumentId, DocumentIdGenerator, IdReservation, UuidV7Generator};

/// Monotonic version attached to a stored document snapshot.
///
/// Version zero is reserved for "no committed version". Concrete storage
/// engines should start committed documents at version one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DocumentVersion(u64);

impl DocumentVersion {
    /// No committed version.
    pub const NONE: Self = Self(0);

    /// First committed version.
    pub const INITIAL: Self = Self(1);

    /// Creates a version from its raw value.
    #[must_use]
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw version value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns whether this is the reserved zero version.
    #[must_use]
    pub const fn is_none(self) -> bool {
        self.0 == 0
    }

    /// Computes the next version.
    ///
    /// # Errors
    ///
    /// Returns [`StorageErrorKind::VersionOverflow`] at `u64::MAX`.
    pub fn next(self) -> StorageResult<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(StorageError::version_overflow)
    }
}

impl fmt::Display for DocumentVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Immutable committed document snapshot.
#[derive(Clone, Debug)]
pub struct StoredDocument {
    id: DocumentId,
    version: DocumentVersion,
    document: Arc<Document>,
}

impl StoredDocument {
    /// Creates a committed document snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when `version` is zero.
    #[inline]
    pub fn new(
        id: DocumentId,
        version: DocumentVersion,
        document: Arc<Document>,
    ) -> StorageResult<Self> {
        if version.is_none() {
            return Err(StorageError::invalid_committed_version());
        }

        Ok(Self {
            id,
            version,
            document,
        })
    }

    /// Returns the immutable document identifier.
    #[must_use]
    pub const fn id(&self) -> &DocumentId {
        &self.id
    }

    /// Returns the committed version.
    #[must_use]
    pub const fn version(&self) -> DocumentVersion {
        self.version
    }

    /// Returns the immutable document payload.
    #[must_use]
    pub fn document(&self) -> &Document {
        &self.document
    }

    /// Returns a shared document payload.
    #[must_use]
    pub fn shared_document(&self) -> Arc<Document> {
        Arc::clone(&self.document)
    }

    /// Consumes the snapshot.
    #[must_use]
    pub fn into_parts(self) -> (DocumentId, DocumentVersion, Arc<Document>) {
        (self.id, self.version, self.document)
    }
}

/// Version precondition for replace and delete operations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum VersionPrecondition {
    /// Accept any currently committed version.
    #[default]
    Any,

    /// Require the document to have the exact version.
    Exact(DocumentVersion),
}

impl VersionPrecondition {
    /// Returns whether a current version satisfies this precondition.
    #[must_use]
    pub const fn matches(self, current: DocumentVersion) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(expected) => expected.0 == current.0,
        }
    }
}

/// Options controlling a collection scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanOptions {
    limit: Option<usize>,
    direction: ScanDirection,
}

impl ScanOptions {
    /// Creates default scan options.
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            limit: None,
            direction: ScanDirection::Forward,
        }
    }

    /// Limits the number of returned documents.
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sets scan direction.
    #[must_use]
    pub const fn with_direction(mut self, direction: ScanDirection) -> Self {
        self.direction = direction;
        self
    }

    /// Returns the optional result limit.
    #[must_use]
    pub const fn limit(self) -> Option<usize> {
        self.limit
    }

    /// Returns scan direction.
    #[must_use]
    #[inline]
    pub const fn direction(self) -> ScanDirection {
        self.direction
    }
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic collection scan direction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ScanDirection {
    /// Ascending document identifier order.
    #[default]
    Forward,

    /// Descending document identifier order.
    Reverse,
}

/// Result of an insert operation.
#[derive(Clone, Debug)]
pub struct InsertResult {
    stored: StoredDocument,
}

impl InsertResult {
    /// Creates an insert result.
    #[must_use]
    #[inline]
    pub const fn new(stored: StoredDocument) -> Self {
        Self { stored }
    }

    /// Returns the committed document.
    #[must_use]
    pub const fn stored(&self) -> &StoredDocument {
        &self.stored
    }

    /// Consumes the result.
    #[must_use]
    pub fn into_stored(self) -> StoredDocument {
        self.stored
    }
}

/// Result of a replace operation.
#[derive(Clone, Debug)]
pub struct ReplaceResult {
    previous_version: DocumentVersion,
    stored: StoredDocument,
}

impl ReplaceResult {
    /// Creates a replace result.
    #[must_use]
    #[inline]
    pub const fn new(previous_version: DocumentVersion, stored: StoredDocument) -> Self {
        Self {
            previous_version,
            stored,
        }
    }

    /// Returns the version replaced by the operation.
    #[must_use]
    pub const fn previous_version(&self) -> DocumentVersion {
        self.previous_version
    }

    /// Returns the new committed document.
    #[must_use]
    pub const fn stored(&self) -> &StoredDocument {
        &self.stored
    }

    /// Consumes the result.
    #[must_use]
    pub fn into_stored(self) -> StoredDocument {
        self.stored
    }
}

/// Result of a delete operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteResult {
    id: DocumentId,
    deleted_version: DocumentVersion,
}

impl DeleteResult {
    /// Creates a delete result.
    #[must_use]
    #[inline]
    pub const fn new(id: DocumentId, deleted_version: DocumentVersion) -> Self {
        Self {
            id,
            deleted_version,
        }
    }

    /// Returns the deleted identifier.
    #[must_use]
    pub const fn id(&self) -> &DocumentId {
        &self.id
    }

    /// Returns the deleted committed version.
    #[must_use]
    pub const fn deleted_version(&self) -> DocumentVersion {
        self.deleted_version
    }
}

/// Degree to which a storage read capability is available.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageSupport {
    Unsupported,
    Supported,
    Native,
}

impl StorageSupport {
    #[must_use]
    pub const fn available(self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}

/// Optional physical read paths understood by the query/storage boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageReadCapability {
    ProjectedValuesGatedUnordered,
}

/// Read-only storage interface.
///
/// A read view represents one consistent storage snapshot. Implementations may
/// back it with locks, copy-on-write structures, MVCC snapshots, or immutable
/// generations.
pub trait StorageRead {
    /// Reports how this read view implements an optional physical access path.
    ///
    /// Default methods make every declared path `Supported`; backends override
    /// this only when they provide a native implementation.
    fn support(&self, _capability: StorageReadCapability) -> StorageSupport {
        StorageSupport::Supported
    }

    /// Returns a document by collection and identifier.
    ///
    /// Missing collections and missing documents both return `Ok(None)`.
    fn get(
        &self,
        collection: &CollectionId,
        id: &DocumentId,
    ) -> StorageResult<Option<StoredDocument>>;

    /// Scans a collection using deterministic identifier ordering.
    ///
    /// Missing collections return an empty vector.
    fn scan(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
    ) -> StorageResult<Vec<StoredDocument>>;

    /// Visits documents without materializing the complete collection.
    ///
    /// The visitor returns `true` to continue and `false` to stop early.
    /// Backends should override this method with a native cursor.
    fn scan_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        for document in self.scan(collection, options)? {
            if !visitor(document)? {
                break;
            }
        }
        Ok(())
    }

    /// Visits a collection while retaining only the requested field paths.
    ///
    /// The default implementation projects documents after a normal scan so
    /// existing backends remain compatible. Native stores may override this
    /// method to avoid materializing unrelated fields.
    fn scan_projected_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        fields: &[FieldPath],
        visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        if fields.is_empty() {
            return self.scan_each(collection, options, visitor);
        }

        self.scan_each(collection, options, &mut |stored| {
            let (id, version, document) = stored.into_parts();
            let projected = project_document(document.as_ref(), fields);
            let stored = StoredDocument::new(id, version, Arc::new(projected))?;
            visitor(stored)
        })
    }

    /// Visits a collection without requiring identifier order, retaining only requested fields.
    ///
    /// Analytical operators whose result is independent of source order should prefer this
    /// cursor. The default preserves compatibility by using the ordered projected scan. Native
    /// stores may override it with a physical/sequential scan.
    fn scan_projected_unordered_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        fields: &[FieldPath],
        visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        self.scan_projected_each(collection, options, fields, visitor)
    }

    /// Visits projected field values directly in the same order as `fields`.
    ///
    /// Analytical consumers can use this cursor to avoid constructing a
    /// temporary projected [`Document`] for every source row. Backends with a
    /// physical field directory should override this method.
    fn scan_projected_values_unordered_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        fields: &[FieldPath],
        visitor: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        self.scan_projected_unordered_each(collection, options, fields, &mut |stored| {
            let values = fields
                .iter()
                .map(|path| value_at_field_path(stored.document(), path).cloned())
                .collect::<Vec<_>>();
            visitor(&values)
        })
    }

    /// Visits projected scalar values without materializing owned runtime values.
    ///
    /// The default adapts the established owned projected-value cursor. Native
    /// stores can override this to borrow strings directly from storage bytes.
    ///
    /// Visits projected scalar values together with their source locator.
    ///
    /// This is the standard late-materialization cursor: consumers can rank,
    /// filter or deduplicate lightweight projected rows and hydrate only the
    /// retained documents. Backends should override it when identifiers and
    /// versions are available in the physical record header.
    fn scan_projected_row_refs_unordered_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        fields: &[FieldPath],
        visitor: &mut dyn for<'a> FnMut(
            DocumentId,
            DocumentVersion,
            &[Option<ProjectedValueRef<'a>>],
        ) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        self.scan_projected_unordered_each(collection, options, fields, &mut |stored| {
            let id = stored.id().clone();
            let version = stored.version();
            let values = fields
                .iter()
                .map(|path| {
                    value_at_field_path(stored.document(), path).map(|value| match value {
                        Value::Null => ProjectedValueRef::Null,
                        Value::Bool(value) => ProjectedValueRef::Bool(*value),
                        Value::Number(crate::Number::Signed(value)) => {
                            ProjectedValueRef::Signed(*value)
                        }
                        Value::Number(crate::Number::Unsigned(value)) => {
                            ProjectedValueRef::Unsigned(*value)
                        }
                        Value::Number(crate::Number::Float(value)) => {
                            ProjectedValueRef::Float(*value)
                        }
                        Value::String(value) => ProjectedValueRef::String(value.as_ref()),
                        Value::Array(_) | Value::Object(_) => {
                            ProjectedValueRef::Owned(value.clone())
                        }
                    })
                })
                .collect::<Vec<_>>();
            visitor(id, version, &values)
        })
    }

    fn scan_projected_value_refs_unordered_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        fields: &[FieldPath],
        visitor: &mut dyn for<'a> FnMut(&[Option<ProjectedValueRef<'a>>]) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        self.scan_projected_values_unordered_each(collection, options, fields, &mut |values| {
            let refs = values
                .iter()
                .map(|value| {
                    value.as_ref().map(|value| match value {
                        Value::Null => ProjectedValueRef::Null,
                        Value::Bool(value) => ProjectedValueRef::Bool(*value),
                        Value::Number(crate::Number::Signed(value)) => {
                            ProjectedValueRef::Signed(*value)
                        }
                        Value::Number(crate::Number::Unsigned(value)) => {
                            ProjectedValueRef::Unsigned(*value)
                        }
                        Value::Number(crate::Number::Float(value)) => {
                            ProjectedValueRef::Float(*value)
                        }
                        Value::String(value) => ProjectedValueRef::String(value.as_ref()),
                        Value::Array(_) | Value::Object(_) => {
                            ProjectedValueRef::Owned(value.clone())
                        }
                    })
                })
                .collect::<Vec<_>>();
            visitor(&refs)
        })
    }

    /// Visits projected values with a source-side gate. The first
    /// `gate_field_count` slots are sufficient for `gate` to decide whether
    /// downstream-only slots need to be materialized. Backends may override
    /// this to defer decoding those trailing slots until the gate accepts.
    fn scan_projected_values_gated_unordered_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        fields: &[FieldPath],
        gate_field_count: usize,
        gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>,
        visitor: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        let gate_field_count = gate_field_count.min(fields.len());
        self.scan_projected_values_unordered_each(collection, options, fields, &mut |values| {
            let _ = gate_field_count;
            if gate(values)? {
                visitor(values)
            } else {
                Ok(true)
            }
        })
    }

    /// Visits gated projected values together with their source locator.
    ///
    /// The default preserves correctness through the full-document projected
    /// cursor. Native stores can override this to decode gate fields first,
    /// defer downstream-only fields until acceptance, and still expose the
    /// document identifier/version needed by streaming consumers.
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
        let gate_field_count = gate_field_count.min(fields.len());
        let mut projected = Vec::with_capacity(fields.len());
        self.scan_projected_unordered_each(collection, options, fields, &mut |stored| {
            projected.clear();
            projected.extend(
                fields
                    .iter()
                    .map(|path| value_at_field_path(stored.document(), path).cloned()),
            );
            let _ = gate_field_count;
            if gate(&projected)? {
                visitor(stored.id().clone(), stored.version(), &projected)
            } else {
                Ok(true)
            }
        })
    }

    /// Visits full documents after evaluating a gate from projected scalar values.
    ///
    /// This is the generic late-materialization path for `where`: backends may
    /// decode only `fields` first, invoke `gate`, and materialize the complete
    /// document only when the gate accepts it.
    fn scan_projected_gated_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        fields: &[FieldPath],
        gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>,
        visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        let mut projected = Vec::with_capacity(fields.len());
        self.scan_each(collection, options, &mut |stored| {
            projected.clear();
            projected.extend(
                fields
                    .iter()
                    .map(|path| value_at_field_path(stored.document(), path).cloned()),
            );
            if gate(&projected)? {
                visitor(stored)
            } else {
                Ok(true)
            }
        })
    }

    /// Returns the number of committed documents in a collection.
    ///
    /// Backends with collection metadata should override this method so a
    /// simple unfiltered `count` can execute without scanning document values.
    fn count(&self, collection: &CollectionId) -> StorageResult<u64> {
        let documents = self.scan(collection, ScanOptions::default())?;
        u64::try_from(documents.len())
            .map_err(|_| StorageError::backend("collection count overflow"))
    }

    /// Returns whether a collection currently exists.
    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool>;

    /// Lists collections in deterministic lexicographic order.
    fn collections(&self) -> StorageResult<Vec<CollectionId>>;
}

fn project_document(document: &Document, fields: &[FieldPath]) -> Document {
    let mut projected = Document::new();

    for path in fields {
        if let Some(value) = path.resolve_value(document) {
            insert_projected_path(&mut projected, path.as_segments(), value.clone());
        }
    }

    projected
}

#[inline]
fn value_at_field_path<'a>(document: &'a Document, path: &FieldPath) -> Option<&'a Value> {
    path.resolve_value(document)
}

fn insert_projected_path(document: &mut Document, segments: &[FieldPathSegment], value: Value) {
    let Some((first, rest)) = segments.split_first() else {
        return;
    };

    if rest.is_empty() {
        document.insert(first.as_str(), value);
        return;
    }

    let mut object = match document.remove(first.as_str()) {
        Some(Value::Object(object)) => (*object).clone(),
        _ => Document::new(),
    };
    insert_projected_path(&mut object, rest, value);
    document.insert(first.as_str(), Value::Object(Arc::new(object)));
}

/// One storage mutation in an ordered batch.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum StorageMutation {
    /// Inserts a new document.
    Insert {
        id: DocumentId,
        document: Arc<Document>,
    },

    /// Replaces an existing document.
    Replace {
        id: DocumentId,
        document: Arc<Document>,
        precondition: VersionPrecondition,
    },
}

impl StorageMutation {
    #[must_use]
    #[inline]
    pub const fn insert(id: DocumentId, document: Arc<Document>) -> Self {
        Self::Insert { id, document }
    }

    #[must_use]
    #[inline]
    pub const fn replace(
        id: DocumentId,
        document: Arc<Document>,
        precondition: VersionPrecondition,
    ) -> Self {
        Self::Replace {
            id,
            document,
            precondition,
        }
    }
}

/// Mutable transaction interface.
///
/// Mutations become externally visible only after [`StorageTransaction::commit`]
/// succeeds. Dropping a transaction without committing must have the same
/// observable effect as rolling it back.
pub trait StorageTransaction: StorageRead {
    /// Inserts a new document.
    ///
    /// Collections are created automatically when necessary.
    fn insert(
        &mut self,
        collection: &CollectionId,
        id: DocumentId,
        document: Arc<Document>,
    ) -> StorageResult<InsertResult>;

    /// Replaces an existing document.
    fn replace(
        &mut self,
        collection: &CollectionId,
        id: &DocumentId,
        document: Arc<Document>,
        precondition: VersionPrecondition,
    ) -> StorageResult<ReplaceResult>;

    /// Deletes an existing document.
    fn delete(
        &mut self,
        collection: &CollectionId,
        id: &DocumentId,
        precondition: VersionPrecondition,
    ) -> StorageResult<DeleteResult>;

    /// Applies an ordered mutation batch.
    ///
    /// Backends may override this method to avoid repeated dynamic dispatch,
    /// collection lookup, validation setup, and allocation.
    fn apply_batch(
        &mut self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<Vec<StoredDocument>> {
        let mut stored = Vec::with_capacity(mutations.len());

        for mutation in mutations {
            let document = match mutation {
                StorageMutation::Insert { id, document } => {
                    self.insert(collection, id, document)?.into_stored()
                }
                StorageMutation::Replace {
                    id,
                    document,
                    precondition,
                } => self
                    .replace(collection, &id, document, precondition)?
                    .into_stored(),
            };
            stored.push(document);
        }

        Ok(stored)
    }

    /// Commits all transaction changes atomically.
    fn commit(self: Box<Self>) -> StorageResult<CommitResult>;

    /// Explicitly rolls the transaction back.
    ///
    /// Implementations must also roll back when the transaction is dropped.
    fn rollback(self: Box<Self>) -> StorageResult<()>;
}

/// Storage façade parameterized by a physical backend.
///
/// This type contains no document-specific storage logic. It delegates all
/// physical operations to `B`, allowing the runtime to use one stable
/// [`StorageEngine`] interface regardless of the selected backend.
#[derive(Clone, Debug, Default)]
pub struct BackendStorage<B> {
    backend: B,
}

impl<B> BackendStorage<B> {
    /// Creates a storage façade from a backend instance.
    #[must_use]
    #[inline]
    pub const fn from_backend(backend: B) -> Self {
        Self { backend }
    }

    /// Returns the configured backend.
    #[must_use]
    #[inline]
    pub const fn backend(&self) -> &B {
        &self.backend
    }

    /// Consumes the façade and returns its backend.
    #[must_use]
    #[inline]
    pub fn into_backend(self) -> B {
        self.backend
    }
}

impl<B: StorageBackend> StorageEngine for BackendStorage<B> {
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>> {
        self.backend.read()
    }

    fn begin(&self) -> StorageResult<Box<dyn StorageTransaction + '_>> {
        self.backend.begin()
    }

    fn apply_batch_atomic(
        &self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<(Vec<StoredDocument>, CommitResult)> {
        self.backend.apply_batch_atomic(collection, mutations)
    }

    fn apply_batch_atomic_summary(
        &self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<CommitResult> {
        self.backend
            .apply_batch_atomic_summary(collection, mutations)
    }
}

/// Root storage engine interface.
///
/// This trait is object-safe so the engine façade can hold
/// `Arc<dyn StorageEngine>`.
pub trait StorageEngine: Send + Sync {
    /// Opens a consistent read snapshot.
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>>;

    /// Begins a multi-collection transaction.
    fn begin(&self) -> StorageResult<Box<dyn StorageTransaction + '_>>;

    /// Applies a complete mutation vector atomically.
    ///
    /// Backends should override this method when they can validate and commit a
    /// batch without cloning a full transactional snapshot. The default keeps
    /// existing backends compatible.
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

    /// Applies a mutation vector atomically without materializing returned rows.
    ///
    /// Import paths should prefer this method when only commit counters are
    /// required. The default preserves backend compatibility.
    fn apply_batch_atomic_summary(
        &self,
        collection: &CollectionId,
        mutations: Vec<StorageMutation>,
    ) -> StorageResult<CommitResult> {
        self.apply_batch_atomic(collection, mutations)
            .map(|(_, commit)| commit)
    }
}

/// Summary returned after a successful transaction commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitResult {
    inserted: u64,
    replaced: u64,
    deleted: u64,
}

impl CommitResult {
    /// Creates a commit summary.
    #[must_use]
    #[inline]
    pub const fn new(inserted: u64, replaced: u64, deleted: u64) -> Self {
        Self {
            inserted,
            replaced,
            deleted,
        }
    }

    /// Returns the number of inserted documents.
    #[must_use]
    pub const fn inserted(self) -> u64 {
        self.inserted
    }

    /// Returns the number of replaced documents.
    #[must_use]
    pub const fn replaced(self) -> u64 {
        self.replaced
    }

    /// Returns the number of deleted documents.
    #[must_use]
    pub const fn deleted(self) -> u64 {
        self.deleted
    }

    /// Returns the total number of committed mutations.
    #[must_use]
    pub const fn total(self) -> u64 {
        self.inserted + self.replaced + self.deleted
    }

    /// Returns whether the transaction committed no mutations.
    #[must_use]
    #[inline]
    pub const fn is_empty(self) -> bool {
        self.total() == 0
    }
}

/// Storage-layer error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageError {
    kind: StorageErrorKind,
}

impl StorageError {
    /// Creates an error from its detailed kind.
    #[must_use]
    #[inline]
    pub const fn new(kind: StorageErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the detailed error kind.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &StorageErrorKind {
        &self.kind
    }

    /// Creates a backend-specific error without leaking implementation types.
    #[must_use]
    pub fn backend(message: impl Into<Arc<str>>) -> Self {
        Self::new(StorageErrorKind::Backend {
            message: message.into(),
        })
    }

    fn invalid_collection_id(value: impl Into<Arc<str>>, reason: impl Into<Arc<str>>) -> Self {
        Self::new(StorageErrorKind::InvalidCollectionId {
            value: value.into(),
            reason: reason.into(),
        })
    }

    fn invalid_document_id(value: impl Into<Arc<str>>, reason: impl Into<Arc<str>>) -> Self {
        Self::new(StorageErrorKind::InvalidDocumentId {
            value: value.into(),
            reason: reason.into(),
        })
    }

    fn invalid_committed_version() -> Self {
        Self::new(StorageErrorKind::InvalidCommittedVersion)
    }

    fn version_overflow() -> Self {
        Self::new(StorageErrorKind::VersionOverflow)
    }

    /// Creates a duplicate-document error.
    #[must_use]
    pub fn document_already_exists(collection: CollectionId, id: DocumentId) -> Self {
        Self::new(StorageErrorKind::DocumentAlreadyExists { collection, id })
    }

    /// Creates a missing-document error.
    #[must_use]
    pub fn document_not_found(collection: CollectionId, id: DocumentId) -> Self {
        Self::new(StorageErrorKind::DocumentNotFound { collection, id })
    }

    /// Creates an optimistic version conflict.
    #[must_use]
    pub fn version_conflict(
        collection: CollectionId,
        id: DocumentId,
        expected: DocumentVersion,
        actual: DocumentVersion,
    ) -> Self {
        Self::new(StorageErrorKind::VersionConflict {
            collection,
            id,
            expected,
            actual,
        })
    }

    /// Creates a transaction-closed error.
    #[must_use]
    pub const fn transaction_closed() -> Self {
        Self::new(StorageErrorKind::TransactionClosed)
    }

    /// Creates a transaction-conflict error.
    #[must_use]
    pub fn transaction_conflict(message: impl Into<Arc<str>>) -> Self {
        Self::new(StorageErrorKind::TransactionConflict {
            message: message.into(),
        })
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            StorageErrorKind::InvalidCollectionId { value, reason } => {
                write!(
                    formatter,
                    "invalid collection identifier {value:?}: {reason}"
                )
            }
            StorageErrorKind::InvalidDocumentId { value, reason } => {
                write!(formatter, "invalid document identifier {value:?}: {reason}")
            }
            StorageErrorKind::InvalidCommittedVersion => {
                formatter.write_str("committed document version must not be zero")
            }
            StorageErrorKind::VersionOverflow => formatter.write_str("document version overflow"),
            StorageErrorKind::DocumentAlreadyExists { collection, id } => {
                write!(
                    formatter,
                    "document {id} already exists in collection {collection}"
                )
            }
            StorageErrorKind::DocumentNotFound { collection, id } => {
                write!(
                    formatter,
                    "document {id} was not found in collection {collection}"
                )
            }
            StorageErrorKind::VersionConflict {
                collection,
                id,
                expected,
                actual,
            } => {
                write!(
                    formatter,
                    "version conflict for document {id} in collection {collection}: expected {expected}, actual {actual}"
                )
            }
            StorageErrorKind::TransactionClosed => {
                formatter.write_str("storage transaction is already closed")
            }
            StorageErrorKind::TransactionConflict { message } => {
                write!(formatter, "storage transaction conflict: {message}")
            }
            StorageErrorKind::Backend { message } => {
                write!(formatter, "storage backend error: {message}")
            }
        }
    }
}

impl StdError for StorageError {}

/// Detailed storage error category.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageErrorKind {
    /// Invalid logical collection identifier.
    InvalidCollectionId { value: Arc<str>, reason: Arc<str> },

    /// Invalid immutable document identifier.
    InvalidDocumentId { value: Arc<str>, reason: Arc<str> },

    /// Version zero was used for a committed snapshot.
    InvalidCommittedVersion,

    /// A document version could not be incremented.
    VersionOverflow,

    /// Insert attempted to reuse an existing identifier.
    DocumentAlreadyExists {
        collection: CollectionId,
        id: DocumentId,
    },

    /// Replace or delete targeted a missing document.
    DocumentNotFound {
        collection: CollectionId,
        id: DocumentId,
    },

    /// Optimistic version precondition failed.
    VersionConflict {
        collection: CollectionId,
        id: DocumentId,
        expected: DocumentVersion,
        actual: DocumentVersion,
    },

    /// Operation attempted on a completed transaction.
    TransactionClosed,

    /// Snapshot isolation detected a concurrent write conflict.
    TransactionConflict { message: Arc<str> },

    /// Concrete backend failure.
    Backend { message: Arc<str> },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_collection_identifier() {
        let id = CollectionId::parse("users").unwrap();

        assert_eq!(id.as_str(), "users");
        assert_eq!(id.segment_count(), 1);
        assert!(!id.is_system());
    }

    #[test]
    fn parses_system_collection_identifier() {
        let id = CollectionId::parse("_og.events").unwrap();

        assert_eq!(id.segments().collect::<Vec<_>>(), vec!["_og", "events"]);
        assert!(id.is_system());
    }

    #[test]
    fn rejects_empty_collection_identifier() {
        let error = CollectionId::parse("").unwrap_err();

        assert!(matches!(
            error.kind(),
            StorageErrorKind::InvalidCollectionId { .. }
        ));
    }

    #[test]
    fn rejects_empty_collection_segment() {
        let error = CollectionId::parse("_og..events").unwrap_err();

        assert!(matches!(
            error.kind(),
            StorageErrorKind::InvalidCollectionId { .. }
        ));
    }

    #[test]
    fn rejects_invalid_collection_start() {
        let error = CollectionId::parse("2users").unwrap_err();

        assert!(matches!(
            error.kind(),
            StorageErrorKind::InvalidCollectionId { .. }
        ));
    }

    #[test]
    fn rejects_invalid_collection_character() {
        let error = CollectionId::parse("user-data").unwrap_err();

        assert!(matches!(
            error.kind(),
            StorageErrorKind::InvalidCollectionId { .. }
        ));
    }

    #[test]
    fn parses_document_identifier() {
        let text = "01890f4c-0000-7000-8000-000000000001";
        let id = DocumentId::parse(text).unwrap();

        assert_eq!(id.to_string(), text);
    }

    #[test]
    fn rejects_empty_document_identifier() {
        let error = DocumentId::parse("").unwrap_err();

        assert!(matches!(
            error.kind(),
            StorageErrorKind::InvalidDocumentId { .. }
        ));
    }

    #[test]
    fn rejects_control_character_in_document_identifier() {
        let error = DocumentId::parse("user\n42").unwrap_err();

        assert!(matches!(
            error.kind(),
            StorageErrorKind::InvalidDocumentId { .. }
        ));
    }

    #[test]
    fn increments_document_version() {
        assert_eq!(
            DocumentVersion::INITIAL.next().unwrap(),
            DocumentVersion::new(2)
        );
    }

    #[test]
    fn detects_document_version_overflow() {
        let error = DocumentVersion::new(u64::MAX).next().unwrap_err();

        assert_eq!(error.kind(), &StorageErrorKind::VersionOverflow);
    }

    #[test]
    fn exact_version_precondition_matches_only_expected_version() {
        let precondition = VersionPrecondition::Exact(DocumentVersion::new(7));

        assert!(precondition.matches(DocumentVersion::new(7)));
        assert!(!precondition.matches(DocumentVersion::new(8)));
    }

    #[test]
    fn any_version_precondition_matches_every_version() {
        assert!(VersionPrecondition::Any.matches(DocumentVersion::INITIAL));
        assert!(VersionPrecondition::Any.matches(DocumentVersion::new(99)));
    }

    #[test]
    fn scan_options_default_to_forward_without_limit() {
        let options = ScanOptions::default();

        assert_eq!(options.limit(), None);
        assert_eq!(options.direction(), ScanDirection::Forward);
    }

    #[test]
    fn configures_scan_options() {
        let options = ScanOptions::new()
            .with_limit(25)
            .with_direction(ScanDirection::Reverse);

        assert_eq!(options.limit(), Some(25));
        assert_eq!(options.direction(), ScanDirection::Reverse);
    }

    #[test]
    fn commit_result_reports_totals() {
        let result = CommitResult::new(2, 3, 4);

        assert_eq!(result.inserted(), 2);
        assert_eq!(result.replaced(), 3);
        assert_eq!(result.deleted(), 4);
        assert_eq!(result.total(), 9);
        assert!(!result.is_empty());
    }

    #[test]
    fn empty_commit_result_is_empty() {
        assert!(CommitResult::default().is_empty());
    }

    #[test]
    fn storage_traits_are_object_safe() {
        fn accept_engine(_: &dyn StorageEngine) {}
        fn accept_read(_: &dyn StorageRead) {}
        fn accept_transaction(_: &mut dyn StorageTransaction) {}

        let _ = accept_engine;
        let _ = accept_read;
        let _ = accept_transaction;
    }

    #[test]
    fn public_storage_types_are_send_and_sync() {
        fn assert_send_and_sync<T: Send + Sync>() {}

        assert_send_and_sync::<CollectionId>();
        assert_send_and_sync::<DocumentId>();
        assert_send_and_sync::<DocumentVersion>();
        assert_send_and_sync::<StoredDocument>();
        assert_send_and_sync::<StorageError>();
        assert_send_and_sync::<CommitResult>();
        assert_send_and_sync::<ScanOptions>();
    }
}
