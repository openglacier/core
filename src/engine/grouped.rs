//! The groups of a finished in-memory `group`, read as a collection: the
//! stages after the `group` run on them as on stored rows (typed batches, a
//! direct `count`, …) instead of on one document built per group. Every other
//! collection reads through to the store.
#![cfg_attr(rustfmt, rustfmt_skip)]
use super::*;
use crate::storage::{BlockColumn, BlockGate, FieldBounds, FieldFacts, GroupFacts, ProjectedBlock, ScanDirection, StorageResult, StorageSupport};

/// The collection the groups are read as.
pub(super) const GROUPS: &str = "_groups";

/// Groups read per block.
const BLOCK_GROUPS: usize = 4096;

/// Finished groups, in the order the `group` gives them: the `n`-th has the
/// identifier [`crate::query::group_row_id`]`(n + 1)`.
pub(super) struct Groups {
    dense: crate::query::DenseGroups,
    table: crate::query::keys::KeyTable,
    /// The key of each group.
    keys: Vec<u32>,
    /// The groups in output order.
    order: Vec<u32>,
    /// The position of each row identifier, built when a row is asked for by it.
    positions: std::sync::OnceLock<HashMap<DocumentId, usize>>,
}

impl Groups {
    pub(super) fn new(dense: crate::query::DenseGroups, table: crate::query::keys::KeyTable, keys: Vec<u32>, order: Vec<u32>) -> Self {
        Self { dense, table, keys, order, positions: std::sync::OnceLock::new() }
    }

    pub(super) fn len(&self) -> usize { self.order.len() }

    /// The row at `position`, as the `group` gives it.
    fn stored(&self, position: usize) -> StorageResult<StoredDocument> {
        let group = self.order[position];
        let document = self.dense.row(group, &|field| self.table.value(self.keys[group as usize], field).cloned()).map_err(|error| StorageError::backend(error.to_string()))?;
        StoredDocument::new(crate::query::group_row_id(usize_to_u64_saturating(position) + 1), DocumentVersion::INITIAL, Arc::new(document))
    }

    /// The value at `path` of the row at `position`.
    fn cell(&self, position: usize, path: &crate::FieldPath) -> StorageResult<Option<Value>> {
        let group = self.order[position];
        let name = path.first().as_str();
        let cell = self.dense.cell(group, &|field| self.table.value(self.keys[group as usize], field).cloned(), name).map_err(|error| StorageError::backend(error.to_string()))?;
        Ok(match cell {
            Some(value) if path.len() > 1 => path.resolve_value(&Document::from_fields([(name, value)])).cloned(),
            cell => cell,
        })
    }

    /// The positions a scan under `options` reads, in its order.
    fn positions(&self, options: ScanOptions) -> Box<dyn Iterator<Item = usize>> {
        let (rows, limit) = (self.len(), options.limit().unwrap_or(usize::MAX));
        match options.direction() {
            ScanDirection::Forward => Box::new((0..rows).take(limit)),
            ScanDirection::Reverse => Box::new((0..rows).rev().take(limit)),
        }
    }

    /// The rows of a scan under `options`, `fields` of them, by blocks of [`BLOCK_GROUPS`], with their
    /// identifiers under `locators`.
    fn blocks(&self, options: ScanOptions, locators: bool, fields: &[crate::FieldPath], visitor: &mut dyn for<'a> FnMut(&ProjectedBlock<'_, 'a>) -> StorageResult<bool>) -> StorageResult<()> {
        let (mut positions, mut chunk) = (self.positions(options), Vec::with_capacity(BLOCK_GROUPS));
        let mut cells = vec![Vec::with_capacity(BLOCK_GROUPS); fields.len()];
        let codes = vec![None; fields.len()];
        loop {
            chunk.clear();
            chunk.extend(positions.by_ref().take(BLOCK_GROUPS));
            if chunk.is_empty() {
                return Ok(());
            }
            for (path, column) in fields.iter().zip(&mut cells) {
                column.clear();
                for position in &chunk {
                    column.push(self.cell(*position, path)?);
                }
            }
            let ids = if locators { chunk.iter().map(|position| (crate::query::group_row_id(usize_to_u64_saturating(*position) + 1), DocumentVersion::INITIAL)).collect() } else { Vec::new() };
            let refs = cells.iter().map(|column| column.iter().map(|cell| cell.as_ref().map(ProjectedValueRef::borrowed)).collect::<Vec<_>>()).collect::<Vec<_>>();
            let columns = refs.iter().map(|column| BlockColumn::Values(column)).collect::<Vec<_>>();
            if !visitor(&ProjectedBlock { rows: chunk.len(), ids: &ids, columns: &columns, codes: &codes, complete: false })? {
                return Ok(());
            }
        }
    }
}

/// The store with the groups as one more collection, [`GROUPS`].
pub(super) struct GroupedStorage {
    pub(super) store: Arc<dyn StorageEngine>,
    pub(super) groups: Groups,
}

impl StorageEngine for GroupedStorage {
    fn read(&self) -> StorageResult<Box<dyn StorageRead + '_>> {
        Ok(Box::new(GroupedRead { current: self.store.read()?, groups: GroupRead(&self.groups), collection: CollectionId::parse(GROUPS)? }))
    }
    fn read_temporal(&self, temporal: &crate::storage::Temporal, cache: &Arc<crate::storage::history::TemporalCache>) -> StorageResult<Box<dyn StorageRead + '_>> { self.store.read_temporal(temporal, cache) }
    fn begin(&self) -> StorageResult<Box<dyn StorageTransaction + '_>> { self.store.begin() }
}

/// The groups alone, as a store holding the one collection [`GROUPS`].
#[derive(Clone, Copy)]
struct GroupRead<'g>(&'g Groups);

impl fmt::Debug for GroupRead<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.debug_struct("GroupRead").field("groups", &self.0.len()).finish() }
}

impl StorageRead for GroupRead<'_> {
    fn get(&self, _collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> {
        let positions = self.0.positions.get_or_init(|| (0..self.0.len()).map(|position| (crate::query::group_row_id(usize_to_u64_saturating(position) + 1), position)).collect());
        positions.get(id).map(|position| self.0.stored(*position)).transpose()
    }
    fn scan(&self, collection: &CollectionId, options: ScanOptions) -> StorageResult<Vec<StoredDocument>> {
        crate::helpers::collect_scan(|visit| self.scan_each(collection, options, visit))
    }
    fn scan_each(&self, _collection: &CollectionId, options: ScanOptions, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> {
        for position in self.0.positions(options) {
            if !visitor(self.0.stored(position)?)? {
                break;
            }
        }
        Ok(())
    }
    fn scan_projected_row_refs_unordered_each(&self, _collection: &CollectionId, options: ScanOptions, fields: &[crate::FieldPath], visitor: &mut dyn for<'a> FnMut(DocumentId, DocumentVersion, &[Option<ProjectedValueRef<'a>>]) -> StorageResult<bool>) -> StorageResult<()> {
        self.0.blocks(options, true, fields, &mut |block| {
            let mut row = Vec::with_capacity(fields.len());
            for index in 0..block.rows {
                row.clear();
                row.extend(block.columns.iter().map(|column| column.get(index)));
                let (id, version) = block.ids[index];
                if !visitor(id, version, &row)? {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }
    fn supports_projected_blocks(&self) -> bool { true }
    fn scan_projected_blocks_each(&self, _collection: &CollectionId, options: ScanOptions, fields: &[crate::FieldPath], _skip: &dyn Fn(&[Option<GroupFacts<'_>>]) -> bool, mut gate: Option<BlockGate<'_>>, visitor: &mut dyn for<'a> FnMut(&ProjectedBlock<'_, 'a>) -> StorageResult<bool>) -> StorageResult<()> {
        self.0.blocks(options, options.locators(), fields, &mut |block| crate::storage::visit_gated(&mut gate, block, visitor))
    }
    fn count(&self, _collection: &CollectionId) -> StorageResult<u64> { Ok(usize_to_u64_saturating(self.0.len())) }
    fn collection_exists(&self, _collection: &CollectionId) -> StorageResult<bool> { Ok(true) }
    fn collections(&self) -> StorageResult<Vec<CollectionId>> { Ok(vec![CollectionId::parse(GROUPS)?]) }
}

/// The store's read with the groups as [`GROUPS`].
struct GroupedRead<'a> {
    current: Box<dyn StorageRead + 'a>,
    groups: GroupRead<'a>,
    collection: CollectionId,
}

impl fmt::Debug for GroupedRead<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.debug_struct("GroupedRead").field("groups", &self.groups).finish_non_exhaustive() }
}

impl GroupedRead<'_> {
    /// The read that holds `collection`.
    fn at(&self, collection: &CollectionId) -> &dyn StorageRead {
        if *collection == self.collection { &self.groups } else { self.current.as_ref() }
    }
}

impl StorageRead for GroupedRead<'_> {
    fn support(&self, capability: StorageReadCapability) -> StorageSupport { self.current.support(capability) }
    fn top_level_fields(&self, collection: &CollectionId) -> StorageResult<Option<Vec<String>>> { self.at(collection).top_level_fields(collection) }
    fn field_facts(&self, collection: &CollectionId) -> StorageResult<Option<FieldFacts>> { self.at(collection).field_facts(collection) }
    fn changed(&self, collection: &CollectionId) -> StorageResult<Option<u64>> { self.at(collection).changed(collection) }
    fn field_bounds(&self, collection: &CollectionId) -> StorageResult<Option<BTreeMap<String, FieldBounds>>> { self.at(collection).field_bounds(collection) }
    fn visit_row_ids(&self, collection: &CollectionId, visitor: &mut dyn FnMut(DocumentId, DocumentVersion) -> StorageResult<bool>) -> StorageResult<()> { self.at(collection).visit_row_ids(collection, visitor) }
    fn get(&self, collection: &CollectionId, id: &DocumentId) -> StorageResult<Option<StoredDocument>> { self.at(collection).get(collection, id) }
    fn get_each(&self, collection: &CollectionId, ids: &[DocumentId], visitor: &mut dyn FnMut(usize, Option<StoredDocument>) -> StorageResult<bool>) -> StorageResult<()> { self.at(collection).get_each(collection, ids, visitor) }
    fn scan(&self, collection: &CollectionId, options: ScanOptions) -> StorageResult<Vec<StoredDocument>> { self.at(collection).scan(collection, options) }
    fn scan_each(&self, collection: &CollectionId, options: ScanOptions, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> { self.at(collection).scan_each(collection, options, visitor) }
    fn scan_projected_row_refs_unordered_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[crate::FieldPath], visitor: &mut dyn for<'r> FnMut(DocumentId, DocumentVersion, &[Option<ProjectedValueRef<'r>>]) -> StorageResult<bool>) -> StorageResult<()> { self.at(collection).scan_projected_row_refs_unordered_each(collection, options, fields, visitor) }
    fn supports_projected_blocks(&self) -> bool { true }
    fn sample_row_ids(&self, collection: &CollectionId, count: usize, seed: u64) -> StorageResult<Option<(u64, Vec<(DocumentId, DocumentVersion)>)>> { self.at(collection).sample_row_ids(collection, count, seed) }
    fn scan_projected_blocks_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[crate::FieldPath], skip: &dyn Fn(&[Option<GroupFacts<'_>>]) -> bool, gate: Option<BlockGate<'_>>, visitor: &mut dyn for<'r> FnMut(&ProjectedBlock<'_, 'r>) -> StorageResult<bool>) -> StorageResult<()> { self.at(collection).scan_projected_blocks_each(collection, options, fields, skip, gate, visitor) }
    fn scan_projected_row_values_gated_unordered_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[crate::FieldPath], gate_field_count: usize, gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>, visitor: &mut dyn FnMut(DocumentId, DocumentVersion, &[Option<Value>]) -> StorageResult<bool>) -> StorageResult<()> { self.at(collection).scan_projected_row_values_gated_unordered_each(collection, options, fields, gate_field_count, gate, visitor) }
    fn scan_projected_gated_each(&self, collection: &CollectionId, options: ScanOptions, fields: &[crate::FieldPath], gate: &mut dyn FnMut(&[Option<Value>]) -> StorageResult<bool>, visitor: &mut dyn FnMut(StoredDocument) -> StorageResult<bool>) -> StorageResult<()> { self.at(collection).scan_projected_gated_each(collection, options, fields, gate, visitor) }
    fn count(&self, collection: &CollectionId) -> StorageResult<u64> { self.at(collection).count(collection) }
    fn collection_exists(&self, collection: &CollectionId) -> StorageResult<bool> { self.at(collection).collection_exists(collection) }
    fn collections(&self) -> StorageResult<Vec<CollectionId>> { self.current.collections() }
}
