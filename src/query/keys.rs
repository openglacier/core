#![cfg_attr(rustfmt, rustfmt_skip)]
//! Dense identifiers for the distinct keys of rows, shared by `distinct`,
//! `group` and `pivot`.
//!
//! A key is one or more fields. Each field's values get dense identifiers of
//! their own ([`ValueIds`]), found from the typed cell of a batch column
//! (tag and bits, borrowed text, or the dictionary code of a source string)
//! without building a value or an encoded key; several fields combine their
//! value identifiers. Two rows get the same key exactly when their encoded
//! keys (`write_projected_ref_distinct_key_indexes`) are equal: a scalar
//! cell's tag and bits, and a string's text, are what its encoding holds.
//! The encoded key of a key is built only when a consumer asks for it, once.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use crate::model::{Number, Value};
use crate::storage::{BlockCodes, BlockColumn, ProjectedValueRef};

use super::batch::{Column, Tag};
use super::codec::{encode_bytes, encode_len, encode_value};

/// Multiply-rotate hasher with a murmur3 final mix: grouping codes stored
/// as floats have zero low mantissa bits, which a bare multiplicative hash
/// would send to the same buckets.
#[derive(Default)]
pub(crate) struct KeyHasher(u64);

impl std::hash::Hasher for KeyHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.write_u64(u64::from_le_bytes(word));
        }
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.write_u64(u64::from(value));
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.write_u64(u64::from(value));
    }

    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.write_u64(value as u64);
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.0 = (self.0.rotate_left(5) ^ value).wrapping_mul(0x517c_c1b7_2722_0a95);
    }

    #[inline]
    fn finish(&self) -> u64 {
        let mut hash = self.0;
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        hash ^ (hash >> 33)
    }
}

pub(crate) type KeyMap<K> = HashMap<K, u32, BuildHasherDefault<KeyHasher>>;

/// Bytes charged per identifier besides its encoding (map entries, value).
const ENTRY_BYTES: usize = 64;

/// One cell, as the identifiers see it.
enum Cell<'a> {
    Scalar(u8, u64),
    Text(&'a str),
    /// Array or object.
    Composite(Value),
}

impl Cell<'_> {
    #[inline]
    fn of_column(column: &Column, row: usize) -> Cell<'_> {
        match column.tag(row) {
            Tag::Missing => Cell::Scalar(0, 0),
            Tag::Null => Cell::Scalar(1, 0),
            Tag::Bool => Cell::Scalar(2, column.bits(row)),
            Tag::Signed => Cell::Scalar(3, column.bits(row)),
            Tag::Unsigned => Cell::Scalar(4, column.bits(row)),
            Tag::Float => Cell::Scalar(5, column.bits(row)),
            Tag::Str => Cell::Text(column.str(row)),
            Tag::Other | Tag::List => Cell::Composite(column.value(row).expect("present cell")),
        }
    }

    #[inline]
    fn of_ref<'a>(value: Option<&'a ProjectedValueRef<'a>>) -> Cell<'a> {
        match value {
            None => Cell::Scalar(0, 0),
            Some(ProjectedValueRef::Null) => Cell::Scalar(1, 0),
            Some(ProjectedValueRef::Bool(value)) => Cell::Scalar(2, u64::from(*value)),
            Some(ProjectedValueRef::Signed(value)) => Cell::Scalar(3, *value as u64),
            Some(ProjectedValueRef::Unsigned(value)) => Cell::Scalar(4, *value),
            Some(ProjectedValueRef::Float(value)) => Cell::Scalar(5, value.to_bits()),
            Some(ProjectedValueRef::String(text)) => Cell::Text(text),
            Some(ProjectedValueRef::Owned(value)) => Cell::of_value(value),
        }
    }

    fn of_value(value: &Value) -> Cell<'_> {
        match value {
            Value::Null => Cell::Scalar(1, 0),
            Value::Bool(value) => Cell::Scalar(2, u64::from(*value)),
            Value::Number(Number::Signed(value)) => Cell::Scalar(3, *value as u64),
            Value::Number(Number::Unsigned(value)) => Cell::Scalar(4, *value),
            Value::Number(Number::Float(value)) => Cell::Scalar(5, value.to_bits()),
            Value::String(text) => Cell::Text(text),
            other => Cell::Composite(other.clone()),
        }
    }

    /// The value (`None`: missing).
    fn value(&self) -> Option<Value> {
        Some(match *self {
            Cell::Scalar(0, _) => return None,
            Cell::Scalar(1, _) => Value::Null,
            Cell::Scalar(2, bits) => Value::Bool(bits != 0),
            Cell::Scalar(3, bits) => Value::Number(Number::Signed(bits as i64)),
            Cell::Scalar(4, bits) => Value::Number(Number::Unsigned(bits)),
            Cell::Scalar(_, bits) => Value::Number(Number::Float(f64::from_bits(bits))),
            Cell::Text(text) => Value::string(text),
            Cell::Composite(ref value) => value.clone(),
        })
    }
}

/// Dense identifiers of the values of one field, in order of first sight.
#[derive(Default)]
pub(crate) struct ValueIds {
    /// Scalars (word: their bits) and texts (word: their hash), with
    /// their cell tag (texts: `TEXT`).
    words: WordIndex,
    composites: KeyMap<Box<[u8]>>,
    /// Value of each identifier (`None`: missing).
    values: Vec<Option<Value>>,
    /// Dictionary of `by_code`, and the identifier of each of its codes
    /// (`u32::MAX`: not seen yet).
    dictionary: Option<u64>,
    by_code: Vec<u32>,
    last: Option<(u8, u64, u32)>,
    /// Text and identifier of the last string seen: equal keys often follow
    /// each other.
    last_text: Option<(String, u32)>,
    bytes: usize,
    scratch: Vec<u8>,
}

impl ValueIds {
    /// Identifier of cell `row` of `column`.
    #[inline(always)]
    pub(crate) fn of_column(&mut self, column: &Column, row: usize) -> u32 {
        match column.code(row) {
            Some((dictionary, code)) => self.of_code(dictionary, code, || Cell::of_column(column, row)),
            None => match column.tag(row) {
                // Numbers are looked up inline.
                Tag::Float => self.of_scalar(5, column.bits(row)),
                Tag::Signed => self.of_scalar(3, column.bits(row)),
                Tag::Unsigned => self.of_scalar(4, column.bits(row)),
                _ => self.of_cell(Cell::of_column(column, row)),
            },
        }
    }

    /// Identifier of a cell under `code` of `dictionary`, looked up once per
    /// code; `cell` gives the cell for a code not seen yet.
    #[inline(always)]
    fn of_code<'c>(&mut self, dictionary: u64, code: u32, cell: impl FnOnce() -> Cell<'c>) -> u32 {
        if self.dictionary != Some(dictionary) {
            self.dictionary = Some(dictionary);
            self.by_code.clear();
        }
        let code = code as usize;
        if let Some(&id) = self.by_code.get(code) {
            if id != u32::MAX {
                return id;
            }
        }
        let id = self.of_cell(cell());
        if code >= self.by_code.len() {
            self.by_code.resize(code + 1, u32::MAX);
        }
        self.by_code[code] = id;
        id
    }

    /// Identifier of a value read row by row (`None`: missing).
    pub(crate) fn of_ref(&mut self, value: Option<&ProjectedValueRef<'_>>) -> u32 {
        self.of_cell(Cell::of_ref(value))
    }

    #[inline]
    fn of_cell(&mut self, cell: Cell<'_>) -> u32 {
        match cell {
            Cell::Scalar(tag, bits) => self.of_scalar(tag, bits),
            Cell::Text(text) => {
                if let Some((last, id)) = &self.last_text {
                    if last.as_str() == text {
                        return *id;
                    }
                }
                let id = self.of_word(WordIndex::hash(text.as_bytes()), Self::TEXT, &cell, Some(text));
                let (last, last_id) = self.last_text.get_or_insert_with(|| (String::new(), id));
                last.clear();
                last.push_str(text);
                *last_id = id;
                id
            }
            Cell::Composite(ref value) => {
                self.scratch.clear();
                encode_value(&mut self.scratch, value);
                match self.composites.get(self.scratch.as_slice()) {
                    Some(id) => *id,
                    None => {
                        let id = self.add(&cell);
                        self.bytes += self.scratch.len() * 2;
                        self.composites.insert(Box::from(self.scratch.as_slice()), id);
                        id
                    }
                }
            }
        }
    }

    /// Identifier of a scalar: looked up inline, inserted out of line.
    #[inline(always)]
    fn of_scalar(&mut self, tag: u8, bits: u64) -> u32 {
        if let Some((last_tag, last_bits, id)) = self.last {
            if (last_tag, last_bits) == (tag, bits) {
                return id;
            }
        }
        let id = match self.words.find(bits, tag) {
            Some(id) => id,
            None => self.of_word(bits, tag, &Cell::Scalar(tag, bits), None),
        };
        self.last = Some((tag, bits, id));
        id
    }

    fn add(&mut self, cell: &Cell<'_>) -> u32 {
        let id = u32::try_from(self.values.len()).expect("fewer than 2^32 distinct values");
        self.values.push(cell.value());
        self.bytes += ENTRY_BYTES + if let Cell::Text(text) = cell { text.len() } else { 0 };
        id
    }

    /// Tag of the texts in `words`, beside the scalar tags of [`Cell`].
    const TEXT: u8 = 6;

    /// Identifier of a scalar, or of `text` (its word is a hash, checked
    /// against the candidate's value), found by its word and tag in `words`.
    #[inline(never)]
    fn of_word(&mut self, word: u64, tag: u8, cell: &Cell<'_>, text: Option<&str>) -> u32 {
        let next = u32::try_from(self.values.len()).expect("fewer than 2^32 distinct values");
        let id = self.words.find_or_insert(word, tag, next, |id| text.is_none_or(|text| self.values[id as usize].as_ref().and_then(Value::as_str) == Some(text)));
        if id == next {
            self.add(cell);
        }
        id
    }

    /// Identifiers of cells `rows` of `column` (`None`: no row holds the
    /// field), appended to `ids`. (Touching their slots first costs as
    /// much as it saves: the slots of 121 k values are L2/L3 hits.)
    pub(crate) fn of_rows(&mut self, column: Option<&Column>, rows: &[u32], ids: &mut Vec<u32>) {
        let Some(column) = column else {
            let missing = self.of_ref(None);
            ids.resize(ids.len() + rows.len(), missing);
            return;
        };
        ids.extend(rows.iter().map(|row| self.of_column(column, *row as usize)));
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    /// Value of identifier `id` (`None`: missing).
    #[must_use]
    pub(crate) fn value(&self, id: u32) -> Option<&Value> {
        self.values[id as usize].as_ref()
    }

    /// Bytes retained, estimated.
    #[must_use]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes + self.by_code.len() * 4
    }
}

/// Open-addressing index of words to dense keys, without an allocation per
/// key: a slot holds a word (a value itself, or its hash), a tag telling
/// apart equal words of different kinds, and the key; `same` checks a
/// candidate key when the word is a hash.
#[derive(Default)]
struct WordIndex {
    /// `(word, key, tag)`, `key == EMPTY` for a free slot; a power-of-two
    /// length.
    slots: Vec<(u64, u32, u8)>,
    used: usize,
}

impl WordIndex {
    const EMPTY: u32 = u32::MAX;

    fn hash(bytes: &[u8]) -> u64 {
        let mut hasher = KeyHasher::default();
        std::hash::Hasher::write(&mut hasher, bytes);
        std::hash::Hasher::finish(&hasher)
    }

    #[inline]
    fn slot_of(word: u64, mask: usize) -> usize {
        let mut hasher = KeyHasher::default();
        std::hash::Hasher::write_u64(&mut hasher, word);
        std::hash::Hasher::finish(&hasher) as usize & mask
    }

    /// Key of scalar `(word, tag)`, if present.
    #[inline(always)]
    fn find(&self, word: u64, tag: u8) -> Option<u32> {
        self.probe(word, tag, |_| true).ok()
    }

    /// Key of `(word, tag)`, or `next` once inserted.
    #[inline]
    fn find_or_insert(&mut self, word: u64, tag: u8, next: u32, same: impl Fn(u32) -> bool) -> u32 {
        if (self.used + 1) * 2 > self.slots.len() {
            self.grow();
        }
        self.probe(word, tag, same).unwrap_or_else(|at| {
            self.slots[at] = (word, next, tag);
            self.used += 1;
            next
        })
    }

    /// Key of `(word, tag)` that `same` accepts, else the free slot ending
    /// its probe (`usize::MAX`: no slot yet).
    #[inline(always)]
    fn probe(&self, word: u64, tag: u8, same: impl Fn(u32) -> bool) -> Result<u32, usize> {
        let mask = self.slots.len().checked_sub(1).ok_or(usize::MAX)?;
        let mut at = Self::slot_of(word, mask);
        loop {
            let (stored, key, stored_tag) = self.slots[at];
            if key == Self::EMPTY {
                return Err(at);
            }
            if stored == word && stored_tag == tag && same(key) {
                return Ok(key);
            }
            at = (at + 1) & mask;
        }
    }

    /// Reads the slots where `words` are looked up first, before the
    /// lookups: the loads are independent, so their cache misses overlap.
    fn touch(&self, words: impl Iterator<Item = u64>) {
        let mask = self.slots.len().wrapping_sub(1);
        if mask != usize::MAX {
            std::hint::black_box(words.fold(0u32, |sum, word| sum ^ self.slots[Self::slot_of(word, mask)].1));
        }
    }

    fn grow(&mut self) {
        let slots = (self.slots.len() * 2).max(64);
        let old = std::mem::replace(&mut self.slots, vec![(0, Self::EMPTY, 0); slots]);
        let mask = slots - 1;
        for slot in old.into_iter().filter(|slot| slot.1 != Self::EMPTY) {
            let mut at = Self::slot_of(slot.0, mask);
            while self.slots[at].1 != Self::EMPTY {
                at = (at + 1) & mask;
            }
            self.slots[at] = slot;
        }
    }
}

/// Dense identifiers of the distinct keys of rows, in order of first sight.
pub(crate) struct KeyTable {
    fields: Vec<ValueIds>,
    /// Value identifiers of each key of two fields or more, `width` per key.
    tuples: Vec<u32>,
    /// Tuples by their word: two fields packed in one, else their hash.
    by_tuple: WordIndex,
    keys: usize,
    /// Per value of the first field, the word and key of its latest tuple
    /// (`u32::MAX`: none): rows of one value often repeat its tuple, found
    /// here without a lookup in `by_tuple`.
    recent: Vec<(u64, u32)>,
    /// Whether `batch` uses `recent`: decided on one batch in 64, from how
    /// often it held the row's tuple, and the batches counted.
    recent_on: bool,
    batches: u32,
    /// Per field, the value identifiers of the current batch's rows.
    batch: Vec<Vec<u32>>,
    tuple: Vec<u32>,
}

impl KeyTable {
    #[must_use]
    pub(crate) fn new(width: usize) -> Self {
        assert!(width > 0, "a key has at least one field");
        Self { fields: (0..width).map(|_| ValueIds::default()).collect(), tuples: Vec::new(), by_tuple: WordIndex::default(), keys: 0, recent: Vec::new(), recent_on: true, batches: 0, batch: vec![Vec::new(); width], tuple: Vec::with_capacity(width) }
    }

    /// Keys of every row of a block's key column, for a one-field key, into
    /// `keys`.
    pub(crate) fn block_keys(&mut self, column: &BlockColumn<'_, '_>, codes: Option<BlockCodes<'_>>, keys: &mut Vec<u32>) {
        let [field] = self.fields.as_mut_slice() else { unreachable!("a one-field key") };
        keys.clear();
        match (column, codes) {
            (BlockColumn::Signed(values), None) => return keys.extend(values.iter().map(|value| field.of_scalar(3, *value as u64))),
            (BlockColumn::Unsigned(values), None) => return keys.extend(values.iter().map(|value| field.of_scalar(4, *value))),
            (BlockColumn::Floats(values), None) => return keys.extend(values.iter().map(|value| field.of_scalar(5, value.to_bits()))),
            _ => {}
        }
        let cell = |row: usize| match column {
            BlockColumn::Signed(values) => Cell::Scalar(3, values[row] as u64),
            BlockColumn::Unsigned(values) => Cell::Scalar(4, values[row]),
            BlockColumn::Floats(values) => Cell::Scalar(5, values[row].to_bits()),
            BlockColumn::Strings(texts) => Cell::Text(texts[row]),
            BlockColumn::Values(values) => Cell::of_ref(values[row].as_ref()),
        };
        match codes {
            Some(BlockCodes { dictionary, codes }) => keys.extend(codes.iter().enumerate().map(|(row, &code)| match code {
                BlockCodes::NONE => field.of_cell(cell(row)),
                code => field.of_code(dictionary, code, || cell(row)),
            })),
            None => keys.extend((0..column.len()).map(|row| field.of_cell(cell(row)))),
        }
    }

    /// Fields of a key.
    #[must_use]
    pub(crate) fn width(&self) -> usize {
        self.fields.len()
    }

    /// Keys seen so far.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        if self.fields.len() == 1 { self.fields[0].len() } else { self.keys }
    }

    /// Key of each row of `selection`, into `keys` (cleared first): `columns`
    /// holds the key fields of a batch, `None` for a field no row holds.
    pub(crate) fn batch(&mut self, columns: &[Option<&Column>], selection: &[u32], keys: &mut Vec<u32>) {
        keys.clear();
        if let [column] = columns {
            return self.fields[0].of_rows(*column, selection, keys);
        }
        self.batch_fields(columns, selection);
        // One batch in 64 decides whether `recent` serves the next ones.
        let sampled = self.batches % 64 == 0;
        self.batches = self.batches.wrapping_add(1);
        if self.recent_on || sampled {
            let hits = self.batch_recent(selection.len(), keys);
            if sampled {
                self.recent_on = hits * 4 >= selection.len();
            }
            return;
        }
        let batch = &self.batch;
        self.by_tuple.touch((0..selection.len()).map(|position| Self::word(batch.iter().map(|ids| ids[position]))));
        let mut tuple = std::mem::take(&mut self.tuple);
        for position in 0..selection.len() {
            tuple.clear();
            tuple.extend(self.batch.iter().map(|ids| ids[position]));
            let key = self.of_tuple(&tuple);
            keys.push(key);
        }
        self.tuple = tuple;
    }

    /// [`Self::batch`]'s keys of the `rows` rows of the last
    /// [`Self::batch_fields`], through `recent`: returns the rows found
    /// there. Out of [`Self::batch`]: its loop keeps its code.
    #[inline(never)]
    fn batch_recent(&mut self, rows: usize, keys: &mut Vec<u32>) -> usize {
        let (batch, recent) = (&self.batch, &self.recent);
        let word = |position: usize| Self::word(batch.iter().map(|ids| ids[position]));
        let latest = |position: usize| recent.get(batch[0][position] as usize).and_then(|&(word, key)| (key != u32::MAX).then_some((word, key)));
        self.by_tuple.touch((0..rows).filter(|position| latest(*position).is_none_or(|(latest, _)| latest != word(*position))).map(word));
        let (width, mut hits) = (self.batch.len(), 0);
        let mut tuple = std::mem::take(&mut self.tuple);
        for position in 0..rows {
            tuple.clear();
            tuple.extend(self.batch.iter().map(|ids| ids[position]));
            let (word, first) = (Self::word(tuple.iter().copied()), tuple[0] as usize);
            let found = self.recent.get(first).filter(|(latest, key)| *latest == word && *key != u32::MAX && (width == 2 || self.tuples[*key as usize * width..][..width] == *tuple)).map(|(_, key)| *key);
            hits += usize::from(found.is_some());
            let key = found.unwrap_or_else(|| self.of_tuple(&tuple));
            if first >= self.recent.len() {
                self.recent.resize(first + 1, (0, u32::MAX));
            }
            self.recent[first] = (word, key);
            keys.push(key);
        }
        self.tuple = tuple;
        hits
    }

    /// Finds the value identifiers of every key field of the rows of
    /// `selection` ([`Self::batch_value`]), without their keys yet
    /// ([`Self::key_at`]).
    pub(crate) fn batch_fields(&mut self, columns: &[Option<&Column>], selection: &[u32]) {
        for ((field, column), ids) in self.fields.iter_mut().zip(columns).zip(&mut self.batch) {
            ids.clear();
            field.of_rows(*column, selection, ids);
        }
    }

    /// Like [`Self::batch_fields`] for field `field` only, and only for the
    /// rows at `positions` of `selection` (all of them when `None`): the
    /// others keep no identifier for that field.
    pub(crate) fn batch_field(&mut self, field: usize, column: Option<&Column>, selection: &[u32], positions: Option<&[u32]>) {
        let (values, ids) = (&mut self.fields[field], &mut self.batch[field]);
        match positions {
            None => {
                ids.clear();
                values.of_rows(column, selection, ids);
            }
            Some(positions) => {
                ids.resize(selection.len(), u32::MAX);
                for &position in positions {
                    let position = position as usize;
                    ids[position] = match column {
                        Some(column) => values.of_column(column, selection[position] as usize),
                        None => values.of_ref(None),
                    };
                }
            }
        }
    }

    /// Value identifier of field `field` of the row at `position` of the
    /// last [`Self::batch_fields`].
    #[inline]
    #[must_use]
    pub(crate) fn batch_value(&self, field: usize, position: usize) -> u32 {
        self.batch[field][position]
    }

    /// Key of the row at `position` of the last [`Self::batch_fields`].
    #[inline]
    pub(crate) fn key_at(&mut self, position: usize) -> u32 {
        if self.fields.len() == 1 {
            return self.batch[0][position];
        }
        let mut tuple = std::mem::take(&mut self.tuple);
        tuple.clear();
        tuple.extend(self.batch.iter().map(|ids| ids[position]));
        let key = self.of_tuple(&tuple);
        self.tuple = tuple;
        key
    }

    /// Writes the encoded key component of value `id` of field `field`, as
    /// [`Self::write_key`] writes it.
    pub(crate) fn write_component(&self, field: usize, id: u32, output: &mut Vec<u8>) {
        match self.fields[field].value(id) {
            Some(value) => {
                output.push(1);
                encode_value(output, value);
            }
            None => output.push(0),
        }
    }

    /// Bytes before the first component in [`Self::write_key`].
    pub(crate) const KEY_HEADER: usize = 9;

    /// Key of one row read row by row: its key fields in `values`.
    pub(crate) fn row(&mut self, values: &[Option<ProjectedValueRef<'_>>]) -> u32 {
        if let ([field], [value]) = (self.fields.as_mut_slice(), values) {
            return field.of_ref(value.as_ref());
        }
        let mut tuple = std::mem::take(&mut self.tuple);
        tuple.clear();
        tuple.extend(self.fields.iter_mut().zip(values).map(|(field, value)| field.of_ref(value.as_ref())));
        let key = self.of_tuple(&tuple);
        self.tuple = tuple;
        key
    }

    /// Word of a tuple in `by_tuple`: two identifiers packed, else their hash.
    #[inline]
    fn word(mut ids: impl ExactSizeIterator<Item = u32>) -> u64 {
        if ids.len() == 2 {
            return u64::from(ids.next().unwrap_or(0)) << 32 | u64::from(ids.next().unwrap_or(0));
        }
        let mut hasher = KeyHasher::default();
        ids.for_each(|id| std::hash::Hasher::write_u32(&mut hasher, id));
        std::hash::Hasher::finish(&hasher)
    }

    #[inline]
    fn of_tuple(&mut self, tuple: &[u32]) -> u32 {
        let next = u32::try_from(self.keys).expect("fewer than 2^32 distinct keys");
        let (tuples, packed) = (&self.tuples, tuple.len() == 2);
        let key = self.by_tuple.find_or_insert(Self::word(tuple.iter().copied()), 0, next, |key| packed || tuples[key as usize * tuple.len()..][..tuple.len()] == *tuple);
        if key == next {
            self.tuples.extend_from_slice(tuple);
            self.keys += 1;
        }
        key
    }

    /// Value of field `field` of `key` (`None`: missing).
    #[must_use]
    pub(crate) fn value(&self, key: u32, field: usize) -> Option<&Value> {
        let id = if self.fields.len() == 1 { key } else { self.tuples[key as usize * self.fields.len() + field] };
        self.fields[field].value(id)
    }

    /// Writes the explicit-field distinct key of `key`, byte for byte as
    /// `write_projected_ref_distinct_key_indexes` does for its values.
    pub(crate) fn write_key(&self, key: u32, output: &mut Vec<u8>) {
        output.clear();
        output.push(1);
        encode_len(output, self.fields.len());
        for field in 0..self.fields.len() {
            match self.value(key, field) {
                Some(value) => {
                    output.push(1);
                    encode_value(output, value);
                }
                None => output.push(0),
            }
        }
    }

    /// Writes the whole-document distinct key of the document holding the
    /// present fields of `key`, named `names` (in name order), as
    /// `write_projected_ref_document_key` does.
    pub(crate) fn write_document_key(&self, key: u32, names: &[&str], output: &mut Vec<u8>) {
        output.clear();
        output.push(0);
        encode_len(output, (0..self.fields.len()).filter(|field| self.value(key, *field).is_some()).count());
        for (field, name) in names.iter().enumerate() {
            if let Some(value) = self.value(key, field) {
                encode_bytes(output, name.as_bytes());
                encode_value(output, value);
            }
        }
    }

    /// Sorts `items` in the order of their keys' [`Self::write_key`] bytes:
    /// each field's values are ranked once by their encoded component, then
    /// items by their keys' ranks (components are prefix-free, so the first
    /// field whose components differ decides, as it does for the bytes).
    /// Each item's ranks are read once, packed into one word when they fit.
    pub(crate) fn sort_by_key<T: Copy>(&self, items: &mut [T], key_of: impl Fn(&T) -> u32) {
        let width = self.fields.len();
        let ranks = (0..width).map(|field| self.field_ranks(field)).collect::<Vec<_>>();
        let id = |key: u32, field: usize| if width == 1 { key } else { self.tuples[key as usize * width + field] };
        let bits = ranks.iter().map(|ranks| usize::BITS - ranks.len().leading_zeros()).collect::<Vec<_>>();
        if bits.iter().sum::<u32>() <= 64 {
            let mut keyed = items.iter().map(|item| (ranks.iter().zip(&bits).enumerate().fold(0u64, |word, (field, (ranks, bits))| word << bits | u64::from(ranks[id(key_of(item), field) as usize])), *item)).collect::<Vec<_>>();
            keyed.sort_unstable_by_key(|(word, _)| *word);
            items.iter_mut().zip(keyed).for_each(|(item, (_, sorted))| *item = sorted);
            return;
        }
        items.sort_unstable_by(|left, right| {
            let (left, right) = (key_of(left), key_of(right));
            (0..width).map(|field| ranks[field][id(left, field) as usize].cmp(&ranks[field][id(right, field) as usize])).find(|ordering| ordering.is_ne()).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    /// Rank of each value of field `field` in the order of its encoded
    /// component ([`Self::write_component`]).
    fn field_ranks(&self, field: usize) -> Vec<u32> {
        let count = self.fields[field].len();
        let (mut encoded, mut ends) = (Vec::new(), Vec::with_capacity(count));
        for id in 0..count as u32 {
            self.write_component(field, id, &mut encoded);
            ends.push(encoded.len());
        }
        let component = |id: u32| &encoded[if id == 0 { 0 } else { ends[id as usize - 1] }..ends[id as usize]];
        let mut order = (0..count as u32).collect::<Vec<_>>();
        order.sort_unstable_by(|left, right| component(*left).cmp(component(*right)));
        let mut ranks = vec![0u32; count];
        for (rank, id) in order.into_iter().enumerate() {
            ranks[id as usize] = rank as u32;
        }
        ranks
    }

    /// Bytes retained, estimated.
    #[must_use]
    pub(crate) fn bytes(&self) -> usize {
        self.fields.iter().map(ValueIds::bytes).sum::<usize>() + self.keys * (self.fields.len() * 8 + ENTRY_BYTES) + self.recent.len() * 16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn block_keys_of_typed_numbers_match_their_cells() { let mut table = KeyTable::new(1); let mut keys = Vec::new(); table.block_keys(&BlockColumn::Floats(&[2.0, 1.5, 2.0, -0.0, 0.0]), None, &mut keys); assert_eq!(keys, [0, 1, 0, 2, 3]); table.block_keys(&BlockColumn::Signed(&[2, 2]), None, &mut keys); assert_eq!(keys, [4, 4]); assert_eq!(table.fields[0].value(1), Some(&Value::Number(Number::Float(1.5)))); }
}
