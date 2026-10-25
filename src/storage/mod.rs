#![cfg_attr(rustfmt, rustfmt_skip)]
//! Query module exports.

pub mod backend;
mod common;
pub mod glacier;
pub mod history;
pub mod memory;
pub use history::{AsOf, Temporal, TemporalCache};
pub use backend::{
    glacier::{GlacierBackend, GlacierStats, GlacierVerification, GlacierWriteMetrics, GLACIER_FORMAT_VERSION},
    memory::MemoryBackend,
};
pub use glacier::GlacierStorage;
pub use memory::MemoryStorage;

use common::validate_collection_id;
use std::{error::Error as StdError, fmt, sync::Arc};

use crate::model::{Document, FieldPath, FieldPathSegment, Value};

/// Result returned by storage operations.
pub type StorageResult<T> = std::result::Result<T, StorageError>;

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

impl<'a> ProjectedValueRef<'a> {
    /// Borrows a decoded value: scalars and strings without copying, arrays
    /// and objects as a shared (reference-counted) clone.
    #[must_use]
    pub fn borrowed(value: &'a Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Bool(*value),
            Value::Number(crate::Number::Signed(value)) => Self::Signed(*value),
            Value::Number(crate::Number::Unsigned(value)) => Self::Unsigned(*value),
            Value::Number(crate::Number::Float(value)) => Self::Float(*value),
            Value::String(value) => Self::String(value),
            other => Self::Owned(other.clone()),
        }
    }
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
    pub fn new( id: DocumentId, version: DocumentVersion, document: Arc<Document>, ) -> StorageResult<Self> {
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

/// Exact per-field facts of one collection at one storage snapshot
/// ([`StorageRead::field_facts`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FieldFacts {
    documents: u64,
    fields: std::collections::BTreeMap<String, FieldFact>,
}

/// How many documents hold one top-level field, and the physical kinds of
/// its values (`null` included).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldFact {
    pub present: u64,
    pub kinds: Vec<crate::model::PhysicalKind>,
}

impl FieldFacts {
    #[must_use]
    pub const fn new(documents: u64, fields: std::collections::BTreeMap<String, FieldFact>) -> Self {
        Self { documents, fields }
    }

    /// Visible documents of the collection.
    #[must_use]
    pub const fn documents(&self) -> u64 {
        self.documents
    }

    /// The top-level fields every value of which (null aside) is a number.
    #[must_use]
    pub fn number_fields(&self) -> std::collections::BTreeSet<String> {
        use crate::model::PhysicalKind;
        self.fields.iter().filter(|(_, fact)| fact.kinds.contains(&PhysicalKind::Number) && fact.kinds.iter().all(|kind| matches!(kind, PhysicalKind::Number | PhysicalKind::Null))).map(|(name, _)| name.clone()).collect()
    }

    /// Physical kinds a top-level field takes when every visible document
    /// holds it; `None` when some document lacks it or its kinds are unknown.
    #[must_use]
    pub fn kinds_when_always_present(&self, field: &str) -> Option<&[crate::model::PhysicalKind]> {
        if self.documents == 0 {
            return None;
        }
        self.fields
            .get(field)
            .filter(|fact| fact.present == self.documents)
            .map(|fact| fact.kinds.as_slice())
    }
}

/// Rows of one projected block, column by column: `columns[field][row]` is
/// the value of field `field` for the row whose locator is `ids[row]`.
#[derive(Debug)]
pub struct ProjectedBlock<'b, 'a> {
    pub rows: usize,
    /// `rows` locators, or none when the scan was
    /// [`ScanOptions::without_locators`].
    pub ids: &'b [(DocumentId, DocumentVersion)],
    pub columns: &'b [BlockColumn<'b, 'a>],
    /// Per column, when its strings come from a dictionary: their codes.
    pub codes: &'b [Option<BlockCodes<'b>>],
    /// Every row holds a value at every column's path, known before
    /// decoding (`false` claims nothing).
    pub complete: bool,
}

/// Decides from some columns of each block whether a block scan decodes the
/// others and visits it ([`StorageRead::scan_projected_blocks_each`]).
pub struct BlockGate<'g> {
    /// Per field, whether `admit` reads its column; the others are empty,
    /// and so are the locators.
    pub fields: Vec<bool>,
    pub admit: &'g mut dyn for<'a> FnMut(&ProjectedBlock<'_, 'a>) -> StorageResult<bool>,
}

impl std::fmt::Debug for BlockGate<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("BlockGate").field("fields", &self.fields).finish_non_exhaustive()
    }
}

/// Visits `block` unless `gate` refuses it.
pub fn visit_gated<'a>(gate: &mut Option<BlockGate<'_>>, block: &ProjectedBlock<'_, 'a>, visitor: &mut dyn for<'x> FnMut(&ProjectedBlock<'_, 'x>) -> StorageResult<bool>) -> StorageResult<bool> {
    if let Some(gate) = gate {
        if !(gate.admit)(block)? {
            return Ok(true);
        }
    }
    visitor(block)
}

/// One column of a [`ProjectedBlock`]: any values, or one slice of a single
/// kind when every row of the block holds that kind (no copy per cell).
#[derive(Clone, Copy, Debug)]
pub enum BlockColumn<'b, 'a> {
    /// `None`: the row lacks the field.
    Values(&'b [Option<ProjectedValueRef<'a>>]),
    Signed(&'b [i64]),
    Unsigned(&'b [u64]),
    Floats(&'b [f64]),
    Strings(&'b [&'a str]),
}

impl<'a> BlockColumn<'_, 'a> {
    #[must_use]
    pub const fn len(&self) -> usize {
        match self {
            Self::Values(values) => values.len(),
            Self::Signed(values) => values.len(),
            Self::Unsigned(values) => values.len(),
            Self::Floats(values) => values.len(),
            Self::Strings(values) => values.len(),
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool { self.len() == 0 }

    /// Value of `row`, as [`StorageRead::scan_projected_row_refs_unordered_each`] gives it.
    #[inline]
    #[must_use]
    pub fn get(&self, row: usize) -> Option<ProjectedValueRef<'a>> {
        match self {
            Self::Values(values) => values[row].clone(),
            Self::Signed(values) => Some(ProjectedValueRef::Signed(values[row])),
            Self::Unsigned(values) => Some(ProjectedValueRef::Unsigned(values[row])),
            Self::Floats(values) => Some(ProjectedValueRef::Float(values[row])),
            Self::Strings(values) => Some(ProjectedValueRef::String(values[row])),
        }
    }
}

/// Dictionary codes of a block column: `codes[row]` for a string cell,
/// [`BlockCodes::NONE`] for any other cell. Under one `dictionary`, equal
/// codes are equal strings and different codes different strings; distinct
/// dictionaries have distinct identifiers within one scan.
#[derive(Clone, Copy, Debug)]
pub struct BlockCodes<'b> {
    pub dictionary: u64,
    pub codes: &'b [u32],
}

impl BlockCodes<'_> {
    pub const NONE: u32 = u32::MAX;
}

/// What one row group of a stored collection holds for one top-level field
/// ([`StorageRead::scan_projected_blocks_each`]), deleted rows
/// included: never narrower than its visible rows.
#[derive(Clone, Debug)]
pub struct GroupFacts<'a> {
    /// Some row lacks the field.
    pub missing: bool,
    /// Kinds of the values present.
    pub kinds: Vec<crate::model::PhysicalKind>,
    pub bounds: &'a FieldBounds,
}

/// Bounds of the values one top-level field takes, per kind: every visible
/// value of that kind lies within them. Bounds may be wider than the visible
/// values (deleted rows), never narrower; `None` for a kind no value takes.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FieldBounds {
    pub signed: Option<(i64, i64)>,
    pub unsigned: Option<(u64, u64)>,
    pub float: Option<(f64, f64)>,
    /// Smallest and largest string; also `None` when strings exist but
    /// their bounds are unknown.
    pub string: Option<(Arc<str>, Arc<str>)>,
}

impl FieldBounds {
    /// Widens the bounds to hold `value`.
    pub fn add(&mut self, value: &Value) {
        fn widen<T: PartialOrd + Copy>(bounds: &mut Option<(T, T)>, value: T) {
            *bounds = Some(match *bounds {
                None => (value, value),
                Some((low, high)) => (if value < low { value } else { low }, if value > high { value } else { high }),
            });
        }
        match value {
            Value::Number(crate::Number::Signed(value)) => widen(&mut self.signed, *value),
            Value::Number(crate::Number::Unsigned(value)) => widen(&mut self.unsigned, *value),
            Value::Number(crate::Number::Float(value)) => widen(&mut self.float, *value),
            Value::String(value) => {
                self.string = Some(match self.string.take() {
                    None => (value.clone(), value.clone()),
                    Some((low, high)) => (if value < &low { value.clone() } else { low }, if value > &high { value.clone() } else { high }),
                });
            }
            _ => {}
        }
    }

    /// Widens the bounds to hold `other`'s.
    pub fn merge(&mut self, other: &Self) {
        for (low, high) in [other.signed].into_iter().flatten() {
            self.add(&Value::signed(low));
            self.add(&Value::signed(high));
        }
        for (low, high) in [other.unsigned].into_iter().flatten() {
            self.add(&Value::unsigned(low));
            self.add(&Value::unsigned(high));
        }
        for (low, high) in [other.float].into_iter().flatten() {
            self.add(&Value::Number(crate::Number::Float(low)));
            self.add(&Value::Number(crate::Number::Float(high)));
        }
        if let Some((low, high)) = &other.string {
            self.add(&Value::String(low.clone()));
            self.add(&Value::String(high.clone()));
        }
    }
}

/// Options controlling a collection scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanOptions {
    limit: Option<usize>,
    direction: ScanDirection,
    reusable_projection: bool,
    without_locators: bool,
    number_codes: u64,
}

impl ScanOptions {
    /// Creates default scan options.
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            limit: None,
            direction: ScanDirection::Forward,
            reusable_projection: false,
            without_locators: false,
            number_codes: 0,
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

    /// Marks an immutable projected source representation as reusable across
    /// executions. Storage backends may use this as an admission hint for
    /// governed caches, but must preserve identical semantics when ignored.
    #[must_use]
    pub const fn with_reusable_projection(mut self) -> Self {
        self.reusable_projection = true;
        self
    }

    /// Returns whether the caller proved the projected source representation
    /// reusable. This is a semantic hint, never a requirement to cache.
    #[must_use]
    #[inline]
    pub const fn reusable_projection(self) -> bool {
        self.reusable_projection
    }

    /// Tells a block scan the caller never reads identifiers or versions:
    /// [`ProjectedBlock::ids`] may then be empty.
    #[must_use]
    pub const fn without_locators(mut self) -> Self {
        self.without_locators = true;
        self
    }

    /// Returns whether the caller declined identifiers and versions.
    #[must_use]
    #[inline]
    pub const fn locators(self) -> bool {
        !self.without_locators
    }

    /// Asks a projected block scan for the dictionary codes of number cells
    /// of the fields at the set bits (bit `i`: `fields[i]`); string cells
    /// carry theirs anyway.
    #[must_use]
    #[inline]
    pub const fn with_number_codes(mut self, fields: u64) -> Self {
        self.number_codes = fields;
        self
    }

    /// Fields whose number cells should carry dictionary codes.
    #[must_use]
    #[inline]
    pub const fn number_codes(self) -> u64 {
        self.number_codes
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

    /// Every top-level field present in at least one document of the
    /// collection visible to this view, in name order; `None` when the view
    /// cannot tell exactly. May list fields no visible document holds, never
    /// omit one that some visible document holds.
    fn top_level_fields(&self, _collection: &CollectionId) -> StorageResult<Option<Vec<String>>> {
        Ok(None)
    }

    /// Exact facts about the top-level fields of the collection visible to
    /// this view: how many documents hold each field and which physical
    /// kinds its values take; `None` when the view cannot tell exactly.
    fn field_facts(&self, _collection: &CollectionId) -> StorageResult<Option<FieldFacts>> {
        Ok(None)
    }

    /// A generation no later than the last commit that changed `collection`
    /// and that changes whenever it does (`None`: the view cannot tell): what
    /// was computed from the collection holds while it stays the same.
    fn changed(&self, _collection: &CollectionId) -> StorageResult<Option<u64>> {
        Ok(None)
    }

    /// Value bounds of the top-level fields of the collection visible to this
    /// view (see [`FieldBounds`]); `None` when the view cannot tell. A field
    /// absent from the map has unknown bounds.
    fn field_bounds(&self, _collection: &CollectionId) -> StorageResult<Option<std::collections::BTreeMap<String, FieldBounds>>> {
        Ok(None)
    }

    /// Visits the identifier and version of every visible document, without
    /// reading documents when the backend indexes them; stops when `visitor`
    /// returns `false`. The order is unspecified.
    fn visit_row_ids(
        &self,
        collection: &CollectionId,
        visitor: &mut dyn FnMut(DocumentId, DocumentVersion) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        self.scan_projected_row_refs_unordered_each(collection, ScanOptions::default(), &[], &mut |id, version, _| {
            visitor(id, version)
        })
    }

    /// Returns a document by collection and identifier.
    ///
    /// Missing collections and missing documents both return `Ok(None)`.
    fn get(
        &self,
        collection: &CollectionId,
        id: &DocumentId,
    ) -> StorageResult<Option<StoredDocument>>;

    /// Rows by identifier, visited in the order given with their position
    /// (`None` for an identifier without a row); the visitor stops the
    /// visit by returning `false`. Ascending identifiers let a native store
    /// read them like a sparse scan.
    fn get_each(&self, collection: &CollectionId, ids: &[DocumentId], visitor: &mut dyn FnMut(usize, Option<StoredDocument>) -> StorageResult<bool>) -> StorageResult<()> {
        for (index, id) in ids.iter().enumerate() {
            if !visitor(index, self.get(collection, id)?)? {
                break;
            }
        }
        Ok(())
    }

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
        self.scan_each(collection, options, &mut |stored| {
            let values = fields
                .iter()
                .map(|path| value_at_field_path(stored.document(), path).map(ProjectedValueRef::borrowed))
                .collect::<Vec<_>>();
            visitor(*stored.id(), stored.version(), &values)
        })
    }

    /// Whether [`Self::scan_projected_blocks_each`] is native
    /// (several rows per block). Consumers fall back to row cursors otherwise.
    fn supports_projected_blocks(&self) -> bool {
        false
    }

    /// Draws `count` distinct visible documents uniformly at random (from
    /// `seed`), in no particular order, reading only those: the number of
    /// visible documents and the drawn locators. `None` when the backend
    /// cannot draw by position, or `count` is not below the visible count;
    /// callers then read every locator.
    fn sample_row_ids(&self, _collection: &CollectionId, _count: usize, _seed: u64) -> StorageResult<Option<(u64, Vec<(DocumentId, DocumentVersion)>)>> {
        Ok(None)
    }

    /// Visits projected values block by block (see [`ProjectedBlock`]).
    /// Values follow the conventions of
    /// [`Self::scan_projected_row_refs_unordered_each`]; `options.limit()`
    /// caps the rows. Before reading a row group, a backend that knows its
    /// facts may call `skip` with them, one per field of `fields` (`None`:
    /// unknown), and leaves out every row of the group when it returns
    /// `true`. A `gate` sees each block first (when the scan has no limit);
    /// a backend may decode only the columns it reads before it decides. The
    /// default skips nothing and emits one row per block, without codes.
    fn scan_projected_blocks_each(
        &self,
        collection: &CollectionId,
        options: ScanOptions,
        fields: &[FieldPath],
        _skip: &dyn Fn(&[Option<GroupFacts<'_>>]) -> bool,
        mut gate: Option<BlockGate<'_>>,
        visitor: &mut dyn for<'a> FnMut(&ProjectedBlock<'_, 'a>) -> StorageResult<bool>,
    ) -> StorageResult<()> {
        self.scan_projected_row_refs_unordered_each(collection, options, fields, &mut |id, version, row| {
            let columns = row.iter().map(std::slice::from_ref).map(BlockColumn::Values).collect::<Vec<_>>();
            let codes = vec![None; columns.len()];
            visit_gated(&mut gate, &ProjectedBlock { rows: 1, ids: &[(id, version)], columns: &columns, codes: &codes, complete: false }, visitor)
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
        let _ = gate_field_count;
        let mut projected = Vec::with_capacity(fields.len());
        self.scan_each(collection, options, &mut |stored| {
            projected.clear();
            projected.extend(
                fields
                    .iter()
                    .map(|path| value_at_field_path(stored.document(), path).cloned()),
            );
            if gate(&projected)? {
                visitor(*stored.id(), stored.version(), &projected)
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

    /// Moves the pending writes out of memory, to commit them later as
    /// they are; `false` when the backend keeps them in memory.
    fn spill(&mut self) -> StorageResult<bool> { Ok(false) }

    /// Commits all transaction changes atomically.
    fn commit(self: Box<Self>) -> StorageResult<CommitResult>;

    /// Explicitly rolls the transaction back.
    ///
    /// Implementations must also roll back when the transaction is dropped.
    fn rollback(self: Box<Self>) -> StorageResult<()>;
}

/// Root storage engine interface.
///
/// This trait is object-safe so the engine façade can hold
/// `Arc<dyn StorageEngine>`.
/// Stored bytes per value of a field, over every collection (see [`StorageEngine::usage_by`]).
pub type KeyUsage = std::collections::BTreeMap<String, u64>;

pub trait StorageEngine: Send + Sync {
    /// Bytes held by the live documents of every collection, shared per value of the top-level
    /// string field `key` (documents without it under ""), for usage accounting. The default
    /// sizes each document as JSON; a column store shares each row group's bytes by its rows.
    fn usage_by(&self, key: &str) -> StorageResult<KeyUsage> {
        let read = self.read()?;
        let mut usage = KeyUsage::new();
        for collection in read.collections()? {
            read.scan_each(&collection, ScanOptions::default(), &mut |stored| {
                let name = match stored.document().get(key) {
                    Some(crate::model::Value::String(text)) => text.to_string(),
                    _ => String::new(),
                };
                let bytes = serde_json::to_vec(&crate::helpers::document_to_json(stored.document())).map_or(0, |json| json.len() as u64);
                *usage.entry(name).or_default() += bytes;
                Ok(true)
            })?;
        }
        Ok(usage)
    }

    /// Opens a consistent read snapshot.
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>>;

    /// Opens a read view of a past state (`as of`) or of the stored versions
    /// of a collection (`history`); see [`history`]. `cache` is shared by
    /// the reads of one query. The default keeps no history.
    fn read_temporal(&self, _temporal: &Temporal, _cache: &Arc<TemporalCache>) -> StorageResult<Box<dyn StorageRead + '_>> {
        Err(StorageError::history_unavailable())
    }

    /// A read snapshot with the history kept since [`history::HistorySource::floor`],
    /// for backups: a diff holds the events after its parent's generation.
    /// The default keeps no history.
    fn history(&self) -> StorageResult<(Box<dyn StorageRead + '_>, Box<dyn history::HistorySource + '_>)> {
        Err(StorageError::history_unavailable())
    }

    /// No backup will ask again for the events up to `generation`: a store
    /// keeping a backup journal drops them. The default keeps everything.
    fn release_history(&self, _generation: u64) -> StorageResult<()> { Ok(()) }

    /// Writes recorded history into this store, which must have none and no
    /// commit yet. The default cannot.
    fn history_writer(&self) -> StorageResult<Box<dyn history::HistoryWriter + '_>> {
        Err(StorageError::backend("history: this store cannot restore history"))
    }

    /// Begins a multi-collection transaction.
    fn begin(&self) -> StorageResult<Box<dyn StorageTransaction + '_>>;

    /// Applies a complete mutation vector atomically.
    ///
    /// Backends should override this method when they can validate and commit a
    /// batch without cloning a full transactional snapshot. The default keeps
    /// existing backends compatible.
    fn apply_batch_atomic( &self, collection: &CollectionId, mutations: Vec<StorageMutation>, ) -> StorageResult<(Vec<StoredDocument>, CommitResult)> {
        let mut transaction = self.begin()?;
        let stored = transaction.apply_batch(collection, mutations)?;
        let commit = transaction.commit()?;
        Ok((stored, commit))
    }

    /// Applies a mutation vector atomically without materializing returned rows.
    ///
    /// Import paths should prefer this method when only commit counters are
    /// required. The default preserves backend compatibility.
    fn apply_batch_atomic_summary( &self, collection: &CollectionId, mutations: Vec<StorageMutation>, ) -> StorageResult<CommitResult> {
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

    const fn invalid_committed_version() -> Self {
        Self::new(StorageErrorKind::InvalidCommittedVersion)
    }

    const fn version_overflow() -> Self {
        Self::new(StorageErrorKind::VersionOverflow)
    }

    /// Creates a duplicate-document error.
    #[must_use]
    pub const fn document_already_exists(collection: CollectionId, id: DocumentId) -> Self {
        Self::new(StorageErrorKind::DocumentAlreadyExists { collection, id })
    }

    /// Creates a missing-document error.
    #[must_use]
    pub const fn document_not_found(collection: CollectionId, id: DocumentId) -> Self {
        Self::new(StorageErrorKind::DocumentNotFound { collection, id })
    }

    /// Creates an optimistic version conflict.
    #[must_use]
    pub const fn version_conflict(
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

    /// Creates the error of a temporal read on a store that keeps no history.
    #[must_use]
    pub fn history_unavailable() -> Self {
        Self::backend("history: this store does not keep history (`as of` and `history` need OGD_HISTORY=on, or the glacier backend)")
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

    #[test] fn parses_collection_identifier() { let id = CollectionId::parse("users").unwrap(); assert_eq!(id.as_str(), "users"); assert_eq!(id.segment_count(), 1); assert!(!id.is_system()); }
    #[test] fn parses_system_collection_identifier() { let id = CollectionId::parse("_og.events").unwrap(); assert_eq!(id.segments().collect::<Vec<_>>(), vec!["_og", "events"]); assert!(id.is_system()); }
    #[test] fn rejects_empty_collection_identifier() { let error = CollectionId::parse("").unwrap_err(); assert!(matches!( error.kind(), StorageErrorKind::InvalidCollectionId { .. } )); }
    #[test] fn rejects_empty_collection_segment() { let error = CollectionId::parse("_og..events").unwrap_err(); assert!(matches!( error.kind(), StorageErrorKind::InvalidCollectionId { .. } )); }
    #[test] fn rejects_invalid_collection_start() { let error = CollectionId::parse("2users").unwrap_err(); assert!(matches!( error.kind(), StorageErrorKind::InvalidCollectionId { .. } )); }
    #[test] fn rejects_invalid_collection_character() { let error = CollectionId::parse("user-data").unwrap_err(); assert!(matches!( error.kind(), StorageErrorKind::InvalidCollectionId { .. } )); }
    #[test] fn parses_document_identifier() { let text = "01890f4c-0000-7000-8000-000000000001"; let id = DocumentId::parse(text).unwrap(); assert_eq!(id.to_string(), text); }
    #[test] fn rejects_empty_document_identifier() { let error = DocumentId::parse("").unwrap_err(); assert!(matches!( error.kind(), StorageErrorKind::InvalidDocumentId { .. } )); }
    #[test] fn rejects_control_character_in_document_identifier() { let error = DocumentId::parse("user\n42").unwrap_err(); assert!(matches!( error.kind(), StorageErrorKind::InvalidDocumentId { .. } )); }
    #[test] fn increments_document_version() { assert_eq!( DocumentVersion::INITIAL.next().unwrap(), DocumentVersion::new(2) ); }
    #[test] fn detects_document_version_overflow() { let error = DocumentVersion::new(u64::MAX).next().unwrap_err(); assert_eq!(error.kind(), &StorageErrorKind::VersionOverflow); }
    #[test] fn exact_version_precondition_matches_only_expected_version() { let precondition = VersionPrecondition::Exact(DocumentVersion::new(7)); assert!(precondition.matches(DocumentVersion::new(7))); assert!(!precondition.matches(DocumentVersion::new(8))); }
    #[test] fn any_version_precondition_matches_every_version() { assert!(VersionPrecondition::Any.matches(DocumentVersion::INITIAL)); assert!(VersionPrecondition::Any.matches(DocumentVersion::new(99))); }
    #[test] fn scan_options_default_to_forward_without_limit() { let options = ScanOptions::default(); assert_eq!(options.limit(), None); assert_eq!(options.direction(), ScanDirection::Forward); }
    #[test] fn configures_scan_options() { let options = ScanOptions::new() .with_limit(25) .with_direction(ScanDirection::Reverse); assert_eq!(options.limit(), Some(25)); assert_eq!(options.direction(), ScanDirection::Reverse); }
    #[test] fn commit_result_reports_totals() { let result = CommitResult::new(2, 3, 4); assert_eq!(result.inserted(), 2); assert_eq!(result.replaced(), 3); assert_eq!(result.deleted(), 4); assert_eq!(result.total(), 9); assert!(!result.is_empty()); }
    #[test] fn empty_commit_result_is_empty() { assert!(CommitResult::default().is_empty()); }
    #[test] fn storage_traits_are_object_safe() { fn accept_engine(_: &dyn StorageEngine) {} fn accept_read(_: &dyn StorageRead) {} fn accept_transaction(_: &mut dyn StorageTransaction) {} let _ = accept_engine; let _ = accept_read; let _ = accept_transaction; }
    #[test] fn public_storage_types_are_send_and_sync() { fn assert_send_and_sync<T: Send + Sync>() {} assert_send_and_sync::<CollectionId>(); assert_send_and_sync::<DocumentId>(); assert_send_and_sync::<DocumentVersion>(); assert_send_and_sync::<StoredDocument>(); assert_send_and_sync::<StorageError>(); assert_send_and_sync::<CommitResult>(); assert_send_and_sync::<ScanOptions>(); }
}
