//! Stored bytes per value of a field (method B of POLICIES in `wip/README.md`): each row group's bytes
//! (identifiers, versions and chunks) are shared by its rows, and the live rows are summed per
//! value of the key field, read through its dictionary codes. A segment never changes, so its
//! result is kept until its deleted rows change; delta rows are sized as JSON.

use std::collections::HashMap;
use std::sync::Arc;

use super::column::{Cell, Scratch, BLOCK};
use super::manifest::DeadRows;
use super::segment::Segment;
use super::{GlacierBackend, StorageError};
use crate::model::Value;
use crate::storage::{KeyUsage, StorageResult};

/// Results per (segment, key), with the deleted-row count they were computed at.
#[derive(Debug, Default)]
pub(super) struct UsageCache {
    segments: HashMap<(u64, String), (u64, Arc<Vec<(String, u64)>>)>,
}

/// Bytes of the live rows of a segment per value of `key` (missing or not a string: "").
fn segment_usage(segment: &Segment, key: &str, dead: Option<&DeadRows>) -> StorageResult<Vec<(String, u64)>> {
    let field = segment.field_index(key);
    let mut totals: Vec<f64> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    let mut index_of: HashMap<String, usize> = HashMap::new();
    let mut intern = |text: &str, names: &mut Vec<String>| -> usize {
        if let Some(index) = index_of.get(text) {
            return *index;
        }
        names.push(text.to_owned());
        index_of.insert(text.to_owned(), names.len() - 1);
        names.len() - 1
    };
    let missing = intern("", &mut names);
    let mut scratch = Scratch::default();
    let mut cells = vec![Cell::Missing; BLOCK];
    for group in 0..segment.group_count() {
        let (rows, first_row, bytes) = segment.group_bytes(group);
        if rows == 0 {
            continue;
        }
        let per_row = bytes as f64 / rows as f64;
        let live = |row: usize| dead.is_none_or(|dead| !dead.contains(first_row + row as u64));
        let column = match field {
            Some(field) => segment.column(group, field)?,
            None => None,
        };
        let mut counts: Vec<u64> = vec![0; names.len()];
        let Some(column) = column else {
            counts[missing] = (0..rows).filter(|row| live(*row)).count() as u64;
            add(&mut totals, &counts, per_row);
            continue;
        };
        // Dictionary code -> value index, for this chunk.
        let mut by_code: Vec<usize> = Vec::new();
        for block in 0..rows.div_ceil(BLOCK) {
            let len = BLOCK.min(rows - block * BLOCK);
            column.decode_block(block, &mut cells[..len], &mut scratch)?;
            for (offset, cell) in cells[..len].iter().enumerate() {
                if !live(block * BLOCK + offset) {
                    continue;
                }
                let index = match cell {
                    Cell::Str(text, code) => {
                        let cached = (*code != u32::MAX).then(|| by_code.get(*code as usize).copied()).flatten().filter(|index| *index != usize::MAX);
                        match cached {
                            Some(index) => index,
                            None => {
                                let index = intern(text, &mut names);
                                if *code != u32::MAX {
                                    let code = *code as usize;
                                    if by_code.len() <= code {
                                        by_code.resize(code + 1, usize::MAX);
                                    }
                                    by_code[code] = index;
                                }
                                index
                            }
                        }
                    }
                    _ => missing,
                };
                if counts.len() <= index {
                    counts.resize(index + 1, 0);
                }
                counts[index] += 1;
            }
        }
        add(&mut totals, &counts, per_row);
    }
    Ok(names.into_iter().zip(totals).filter(|(_, bytes)| *bytes > 0.0).map(|(name, bytes)| (name, bytes.round() as u64)).collect())
}

fn add(totals: &mut Vec<f64>, counts: &[u64], per_row: f64) {
    if totals.len() < counts.len() {
        totals.resize(counts.len(), 0.0);
    }
    for (total, count) in totals.iter_mut().zip(counts) {
        *total += *count as f64 * per_row;
    }
}

impl GlacierBackend {
    /// Stored bytes of the live rows of every collection per value of `key`.
    pub(super) fn usage_by_key(&self, key: &str) -> StorageResult<KeyUsage> {
        let state = self.state()?;
        let mut usage = KeyUsage::new();
        let mut cache = self.inner.usage.lock().map_err(|_| StorageError::backend("Glacier usage cache lock poisoned"))?;
        let mut kept = HashMap::new();
        for collection in state.collections.values() {
            for part in &collection.segments {
                let dead_count = part.dead.as_ref().map_or(0, |dead| dead.count());
                let entry = (part.segment.number(), key.to_owned());
                let result = match cache.segments.remove(&entry) {
                    Some((count, result)) if count == dead_count => result,
                    _ => Arc::new(segment_usage(&part.segment, key, part.dead.as_deref())?),
                };
                for (name, bytes) in result.iter() {
                    *usage.entry(name.clone()).or_default() += bytes;
                }
                kept.insert(entry, (dead_count, result));
            }
            for row in collection.delta.values() {
                let name = match row.document.get(key) {
                    Some(Value::String(text)) => text.to_string(),
                    _ => String::new(),
                };
                let bytes = serde_json::to_vec(&crate::helpers::document_to_json(&row.document)).map_or(0, |json| json.len() as u64);
                *usage.entry(name).or_default() += bytes;
            }
        }
        // Results of other keys stay; those of segments no longer live go.
        let live: std::collections::HashSet<u64> = kept.keys().map(|(number, _)| *number).collect();
        cache.segments.retain(|(number, other), _| other != key && live.contains(number));
        cache.segments.extend(kept);
        Ok(usage)
    }
}
