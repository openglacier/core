#![cfg_attr(rustfmt, rustfmt_skip)]
//! One column chunk (one top-level field over one row group), plus the
//! identifier and version chunks.
//!
//! A cell carries a kind tag (missing, null, false, true, signed, unsigned,
//! float, string, nested). Tags are stored as one constant, as a dominant
//! kind with sorted exceptions, or bit-packed. Each kind holding a payload
//! has its own dense stream; a per-block rank table gives each stream's
//! position at the start of every 1 024-row block, so any block decodes
//! independently.

use std::sync::Arc;

use super::{
    codec::{corrupt, encode_value, put_bytes, put_u128, put_u32, put_u64, put_u8, Reader},
    encoding::{pack, signed_from_key, signed_key, write_byte_strings, write_floats, write_ints, ByteStrings, Floats, Ints},
};
use crate::{model::{Number, Value}, storage::{FieldBounds, StorageResult}};

/// Rows per independently decodable block.
pub(super) const BLOCK: usize = 1024;

pub(super) const K_MISSING: u8 = 0;
pub(super) const K_NULL: u8 = 1;
pub(super) const K_FALSE: u8 = 2;
pub(super) const K_TRUE: u8 = 3;
pub(super) const K_SIGNED: u8 = 4;
pub(super) const K_UNSIGNED: u8 = 5;
pub(super) const K_FLOAT: u8 = 6;
pub(super) const K_STRING: u8 = 7;
pub(super) const K_NESTED: u8 = 8;
pub(super) const KINDS: usize = 9;
const STREAM_KINDS: [u8; 5] = [K_SIGNED, K_UNSIGNED, K_FLOAT, K_STRING, K_NESTED];

const TAGS_CONST: u8 = 0;
const TAGS_SPARSE: u8 = 1;
const TAGS_PACKED: u8 = 2;

/// Per-field fact slots, in [`crate::model::PhysicalKind`] order.
pub(super) const FACTS: usize = 6;

#[must_use]
pub(super) const fn fact_index(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

#[must_use]
pub(super) fn kind_of(value: Option<&Value>) -> u8 {
    match value {
        None => K_MISSING,
        Some(Value::Null) => K_NULL,
        Some(Value::Bool(false)) => K_FALSE,
        Some(Value::Bool(true)) => K_TRUE,
        Some(Value::Number(Number::Signed(_))) => K_SIGNED,
        Some(Value::Number(Number::Unsigned(_))) => K_UNSIGNED,
        Some(Value::Number(Number::Float(_))) => K_FLOAT,
        Some(Value::String(_)) => K_STRING,
        Some(Value::Array(_) | Value::Object(_)) => K_NESTED,
    }
}

/// Strings longer than this are not kept as chunk bounds.
const MAX_BOUND_BYTES: usize = 64;

/// What a reader can know of a chunk without decoding it.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct ChunkStats {
    /// Bit `k` set when some cell has kind `k` (`K_*`).
    pub(super) kinds: u16,
    pub(super) bounds: FieldBounds,
}

impl ChunkStats {
    pub(super) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.kinds.to_le_bytes());
        let bounds = &self.bounds;
        let flags = u8::from(bounds.signed.is_some()) | u8::from(bounds.unsigned.is_some()) << 1 | u8::from(bounds.float.is_some()) << 2 | u8::from(bounds.string.is_some()) << 3;
        put_u8(out, flags);
        if let Some((low, high)) = bounds.signed { put_u64(out, low as u64); put_u64(out, high as u64); }
        if let Some((low, high)) = bounds.unsigned { put_u64(out, low); put_u64(out, high); }
        if let Some((low, high)) = bounds.float { put_u64(out, low.to_bits()); put_u64(out, high.to_bits()); }
        if let Some((low, high)) = &bounds.string { put_bytes(out, low.as_bytes()); put_bytes(out, high.as_bytes()); }
    }

    pub(super) fn decode(reader: &mut Reader<'_>) -> StorageResult<Self> {
        let kinds = u16::from_le_bytes(reader.take(2)?.try_into().map_err(|_| corrupt("chunk kinds"))?);
        let flags = reader.u8()?;
        let mut bounds = FieldBounds::default();
        if flags & 1 != 0 { bounds.signed = Some((reader.u64()? as i64, reader.u64()? as i64)); }
        if flags & 2 != 0 { bounds.unsigned = Some((reader.u64()?, reader.u64()?)); }
        if flags & 4 != 0 { bounds.float = Some((f64::from_bits(reader.u64()?), f64::from_bits(reader.u64()?))); }
        if flags & 8 != 0 { bounds.string = Some((Arc::from(reader.str()?), Arc::from(reader.str()?))); }
        Ok(Self { kinds, bounds })
    }
}

/// Encodes one column chunk; `cells[row]` is the field value of that row.
pub(super) fn write_column(cells: &[Option<&Value>], out: &mut Vec<u8>) -> ChunkStats {
    let rows = cells.len();
    let tags = cells.iter().map(|cell| kind_of(*cell)).collect::<Vec<_>>();
    let mut counts = [0usize; KINDS];
    for tag in &tags {
        counts[*tag as usize] += 1;
    }
    let present = counts.iter().enumerate().filter(|(_, count)| **count > 0).fold(0u16, |mask, (kind, _)| mask | (1 << kind));
    let mut stats = ChunkStats { kinds: present, bounds: FieldBounds::default() };
    let mut long_string = false;
    for value in cells.iter().flatten() {
        match value {
            Value::String(text) if text.len() > MAX_BOUND_BYTES => long_string = true,
            value => stats.bounds.add(value),
        }
    }
    if long_string {
        stats.bounds.string = None;
    }
    let dominant = (0..KINDS).max_by_key(|kind| counts[*kind]).unwrap_or(0) as u8;
    let exceptions = rows - counts[dominant as usize];

    put_u32(out, rows as u32);
    if exceptions == 0 {
        put_u8(out, TAGS_CONST);
        put_u8(out, dominant);
    } else if exceptions * 10 < rows {
        put_u8(out, TAGS_SPARSE);
        put_u8(out, dominant);
        put_u32(out, exceptions as u32);
        for (row, tag) in tags.iter().enumerate().filter(|(_, tag)| **tag != dominant) {
            put_u32(out, row as u32);
            put_u8(out, *tag);
        }
    } else {
        put_u8(out, TAGS_PACKED);
        pack(tags.iter().map(|tag| u64::from(*tag)), rows, 4, out);
    }
    out.extend_from_slice(&present.to_le_bytes());

    let streams = STREAM_KINDS.iter().copied().filter(|kind| present & (1 << kind) != 0).collect::<Vec<_>>();
    for kind in &streams {
        put_u32(out, counts[*kind as usize] as u32);
    }
    if exceptions != 0 {
        let mut ranks = [0u32; KINDS];
        for (row, tag) in tags.iter().enumerate() {
            if row % BLOCK == 0 {
                for kind in &streams {
                    put_u32(out, ranks[*kind as usize]);
                }
            }
            ranks[*tag as usize] += 1;
        }
    }
    for kind in streams {
        let mut stream = Vec::new();
        let values = cells.iter().filter_map(|cell| cell.filter(|value| kind_of(Some(value)) == kind));
        match kind {
            K_SIGNED => write_ints(&values.filter_map(|v| v.as_number().and_then(|n| n.as_signed())).map(signed_key).collect::<Vec<_>>(), &mut stream),
            K_UNSIGNED => write_ints(&values.filter_map(|v| v.as_number().and_then(|n| n.as_unsigned())).collect::<Vec<_>>(), &mut stream),
            K_FLOAT => write_floats(&values.filter_map(|v| v.as_number().and_then(|n| n.as_float())).collect::<Vec<_>>(), &mut stream),
            K_STRING => write_byte_strings(&values.filter_map(|v| v.as_str()).map(str::as_bytes).collect::<Vec<_>>(), true, &mut stream),
            _ => {
                let encoded = values.map(|value| { let mut bytes = Vec::new(); encode_value(&mut bytes, value); bytes }).collect::<Vec<_>>();
                write_byte_strings(&encoded.iter().map(Vec::as_slice).collect::<Vec<_>>(), false, &mut stream);
            }
        }
        put_u32(out, stream.len() as u32);
        out.extend_from_slice(&stream);
    }
    stats
}

/// A decoded cell borrowing its string from the mapped segment.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Cell<'a> {
    Missing,
    Null,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    /// String with its dictionary code (`u32::MAX` when plain).
    Str(&'a str, u32),
    Nested(&'a [u8]),
}

#[derive(Clone, Copy, Debug)]
enum Tags<'a> {
    Const(u8),
    Sparse { dominant: u8, exceptions: &'a [u8] },
    Packed(&'a [u8]),
}

/// Kind counts of a block up to a row, kept between reads of one column
/// ([`ColumnReader::get_ranked`]).
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct RankCursor {
    block: Option<usize>,
    row: usize,
    ranks: [usize; KINDS],
}

/// Random-access reader of one column chunk.
#[derive(Clone, Debug)]
pub(super) struct ColumnReader<'a> {
    rows: usize,
    tags: Tags<'a>,
    present: u16,
    ranks: &'a [u8],
    stream_count: usize,
    signed: Option<Ints<'a>>,
    unsigned: Option<Ints<'a>>,
    floats: Option<Floats<'a>>,
    strings: Option<ByteStrings<'a>>,
    /// `strings`' bytes, validated as UTF-8 on the first block decode.
    text: std::cell::OnceCell<&'a str>,
    /// Decoded dictionary of `strings`, built on the first block decode.
    dictionary: std::cell::OnceCell<Vec<&'a str>>,
    nested: Option<ByteStrings<'a>>,
}

impl<'a> ColumnReader<'a> {
    pub(super) fn open(bytes: &'a [u8]) -> StorageResult<Self> {
        let mut reader = Reader::new(bytes);
        let rows = reader.u32()? as usize;
        let tags = match reader.u8()? {
            TAGS_CONST => Tags::Const(reader.u8()?),
            TAGS_SPARSE => {
                let dominant = reader.u8()?;
                let count = reader.u32()? as usize;
                Tags::Sparse { dominant, exceptions: reader.take(count.checked_mul(5).ok_or_else(|| corrupt("tag exceptions"))?)? }
            }
            TAGS_PACKED => Tags::Packed(reader.take((rows * 4).div_ceil(8) + 16)?),
            _ => return Err(corrupt("tag mode")),
        };
        let present = u16::from_le_bytes(reader.take(2)?.try_into().map_err(|_| corrupt("kinds"))?);
        let streams = STREAM_KINDS.iter().copied().filter(|kind| present & (1 << kind) != 0).collect::<Vec<_>>();
        let mut counts = [0usize; KINDS];
        for kind in &streams {
            counts[*kind as usize] = reader.u32()? as usize;
        }
        if counts.iter().sum::<usize>() > rows {
            return Err(corrupt("kind counts"));
        }
        let ranks = if matches!(tags, Tags::Const(_)) { &[][..] } else { reader.take(rows.div_ceil(BLOCK) * streams.len() * 4)? };
        let mut column = Self { rows, tags, present, ranks, stream_count: streams.len(), signed: None, unsigned: None, floats: None, strings: None, text: std::cell::OnceCell::new(), dictionary: std::cell::OnceCell::new(), nested: None };
        for kind in streams {
            let len = reader.u32()? as usize;
            let mut stream = Reader::new(reader.take(len)?);
            let count = counts[kind as usize];
            match kind {
                K_SIGNED => column.signed = Some(Ints::read(&mut stream, count)?),
                K_UNSIGNED => column.unsigned = Some(Ints::read(&mut stream, count)?),
                K_FLOAT => column.floats = Some(Floats::read(&mut stream, count)?),
                K_STRING => column.strings = Some(ByteStrings::read(&mut stream, count)?),
                _ => column.nested = Some(ByteStrings::read(&mut stream, count)?),
            }
        }
        Ok(column)
    }

    #[must_use]
    pub(super) const fn rows(&self) -> usize { self.rows }

    #[inline]
    fn tag(&self, row: usize) -> u8 {
        match self.tags {
            Tags::Const(kind) => kind,
            Tags::Sparse { dominant, exceptions } => {
                let (mut low, mut high) = (0usize, exceptions.len() / 5);
                while low < high {
                    let middle = (low + high) / 2;
                    let at = exception_row(exceptions, middle);
                    if at < row { low = middle + 1 } else if at > row { high = middle } else { return exceptions[middle * 5 + 4] }
                }
                dominant
            }
            Tags::Packed(bytes) => (bytes.get(row / 2).copied().unwrap_or(0) >> ((row % 2) * 4)) & 0x0F,
        }
    }

    fn ranks_at(&self, block: usize) -> [usize; KINDS] {
        let mut ranks = [0usize; KINDS];
        let mut at = block * self.stream_count * 4;
        for kind in STREAM_KINDS.iter().filter(|kind| self.present & (1 << **kind) != 0) {
            ranks[*kind as usize] = self.ranks.get(at..at + 4).map_or(0, |b| u32::from_le_bytes(b.try_into().unwrap_or([0; 4])) as usize);
            at += 4;
        }
        ranks
    }

    #[inline]
    fn cell(&self, kind: u8, rank: usize) -> StorageResult<Cell<'a>> {
        Ok(match kind {
            K_MISSING => Cell::Missing,
            K_NULL => Cell::Null,
            K_FALSE => Cell::Bool(false),
            K_TRUE => Cell::Bool(true),
            K_SIGNED => Cell::Signed(signed_from_key(self.signed.ok_or_else(|| corrupt("signed stream"))?.get(rank))),
            K_UNSIGNED => Cell::Unsigned(self.unsigned.ok_or_else(|| corrupt("unsigned stream"))?.get(rank)),
            K_FLOAT => Cell::Float(self.floats.ok_or_else(|| corrupt("float stream"))?.get(rank)),
            K_STRING => {
                let strings = self.strings.ok_or_else(|| corrupt("string stream"))?;
                let code = strings.code(rank);
                if let (Some(code), Some(dictionary)) = (code, self.dictionary.get()) {
                    return Ok(Cell::Str(dictionary.get(code).copied().ok_or_else(|| corrupt("dictionary code"))?, code as u32));
                }
                let (start, end) = strings.range(rank);
                let text = match self.text.get() {
                    Some(text) => text.get(start..end).ok_or_else(|| corrupt("string range"))?,
                    // A lone read validates only its own string.
                    None => std::str::from_utf8(strings.region().get(start..end).ok_or_else(|| corrupt("string range"))?).map_err(|_| corrupt("string utf-8"))?,
                };
                Cell::Str(text, code.map_or(u32::MAX, |code| code as u32))
            }
            K_NESTED => Cell::Nested(self.nested.ok_or_else(|| corrupt("nested stream"))?.get(rank)?),
            _ => return Err(corrupt("cell kind")),
        })
    }

    /// Decodes rows `[block * BLOCK, block * BLOCK + out.len())`.
    pub(super) fn decode_block(&self, block: usize, out: &mut [Cell<'a>], scratch: &mut Scratch) -> StorageResult<()> {
        let start = block * BLOCK;
        let len = out.len();
        if start + len > self.rows {
            return Err(corrupt("block range"));
        }
        if self.strings.is_some() {
            self.text()?;
        }
        if let Tags::Const(kind) = self.tags {
            return self.decode_const(kind, start, out, scratch);
        }
        let mut ranks = self.ranks_at(block);
        match self.tags {
            Tags::Sparse { dominant, exceptions } => {
                let mut next = lower_exception(exceptions, start);
                for (offset, slot) in out.iter_mut().enumerate() {
                    let row = start + offset;
                    let kind = if next * 5 < exceptions.len() && exception_row(exceptions, next) == row { next += 1; exceptions[next * 5 - 1] } else { dominant };
                    *slot = self.cell(kind, ranks[kind as usize % KINDS])?;
                    ranks[kind as usize % KINDS] += 1;
                }
            }
            _ => {
                for (offset, slot) in out.iter_mut().enumerate() {
                    let kind = self.tag(start + offset);
                    *slot = self.cell(kind, ranks[kind as usize % KINDS])?;
                    ranks[kind as usize % KINDS] += 1;
                }
            }
        }
        Ok(())
    }

    fn decode_const(&self, kind: u8, start: usize, out: &mut [Cell<'a>], scratch: &mut Scratch) -> StorageResult<()> {
        let len = out.len();
        match kind {
            K_SIGNED | K_UNSIGNED => {
                let stream = if kind == K_SIGNED { self.signed } else { self.unsigned }.ok_or_else(|| corrupt("integer stream"))?;
                let ints = &mut scratch.ints[..len];
                stream.decode(start, ints);
                for (slot, value) in out.iter_mut().zip(ints.iter()) {
                    *slot = if kind == K_SIGNED { Cell::Signed(signed_from_key(*value)) } else { Cell::Unsigned(*value) };
                }
            }
            K_FLOAT => {
                let stream = self.floats.ok_or_else(|| corrupt("float stream"))?;
                let (ints, floats) = (&mut scratch.ints[..len], &mut scratch.floats[..len]);
                stream.decode(start, ints, floats, None);
                for (slot, value) in out.iter_mut().zip(floats.iter()) {
                    *slot = Cell::Float(*value);
                }
            }
            K_STRING if self.strings.is_some_and(|strings| strings.dictionary_len() > 0) => {
                let dictionary = self.dictionary()?;
                let strings = self.strings.ok_or_else(|| corrupt("string stream"))?;
                let codes = &mut scratch.ints[..len];
                strings.codes(start, codes);
                for (slot, code) in out.iter_mut().zip(codes.iter()) {
                    *slot = Cell::Str(dictionary.get(*code as usize).copied().ok_or_else(|| corrupt("dictionary code"))?, *code as u32);
                }
            }
            K_STRING | K_NESTED => {
                for (offset, slot) in out.iter_mut().enumerate() {
                    *slot = self.cell(kind, start + offset)?;
                }
            }
            _ => out.fill(self.cell(kind, 0)?),
        }
        Ok(())
    }

    /// Kind of every cell of rows `[start, start + len)` when they share one
    /// that [`TypedBlock`] holds (signed, unsigned, float, string).
    fn uniform_kind(&self, start: usize, len: usize) -> Option<u8> {
        let kind = match self.tags {
            Tags::Const(kind) => kind,
            Tags::Sparse { dominant, exceptions } => {
                let next = lower_exception(exceptions, start);
                if next * 5 < exceptions.len() && exception_row(exceptions, next) < start + len {
                    return None;
                }
                dominant
            }
            Tags::Packed(_) => return None,
        };
        matches!(kind, K_SIGNED | K_UNSIGNED | K_FLOAT | K_STRING).then_some(kind)
    }

    /// Whether rows `[block * BLOCK, block * BLOCK + len)` share one scalar
    /// kind (none lacks the field).
    pub(super) fn uniform(&self, block: usize, len: usize) -> bool {
        self.uniform_kind(block * BLOCK, len).is_some()
    }

    /// Decodes rows `[block * BLOCK, block * BLOCK + len)` into `out` when
    /// they share one kind; returns `false` (and leaves `out` unspecified)
    /// otherwise.
    /// `number_codes`: also the dictionary codes of number cells.
    pub(super) fn decode_typed(&self, block: usize, len: usize, out: &mut TypedBlock<'a>, scratch: &mut Scratch, number_codes: bool) -> StorageResult<bool> {
        let start = block * BLOCK;
        if start + len > self.rows {
            return Err(corrupt("block range"));
        }
        let Some(kind) = self.uniform_kind(start, len) else { return Ok(false) };
        // Position in the kind's stream of the block's first row.
        let rank = if matches!(self.tags, Tags::Const(_)) { start } else { self.ranks_at(block)[kind as usize] };
        out.kind = kind;
        out.codes.clear();
        // Number streams under a dictionary give their codes, as strings do.
        match kind {
            K_SIGNED => {
                out.signed.resize(len, 0);
                self.signed.ok_or_else(|| corrupt("signed stream"))?.decode_map(rank, &mut scratch.ints, &mut out.signed, number_codes.then_some(&mut out.codes), signed_from_key);
            }
            K_UNSIGNED => {
                out.unsigned.resize(len, 0);
                self.unsigned.ok_or_else(|| corrupt("unsigned stream"))?.decode_map(rank, &mut scratch.ints, &mut out.unsigned, number_codes.then_some(&mut out.codes), |value| value);
            }
            K_FLOAT => {
                out.floats.resize(len, 0.0);
                self.floats.ok_or_else(|| corrupt("float stream"))?.decode(rank, &mut scratch.ints, &mut out.floats, number_codes.then_some(&mut out.codes));
            }
            _ => {
                let strings = self.strings.ok_or_else(|| corrupt("string stream"))?;
                out.strings.clear();
                if strings.dictionary_len() > 0 {
                    let dictionary = self.dictionary()?;
                    let codes = &mut scratch.ints[..len];
                    strings.codes(rank, codes);
                    if codes.iter().any(|code| *code as usize >= dictionary.len()) {
                        return Err(corrupt("dictionary code"));
                    }
                    out.strings.extend(codes.iter().map(|code| dictionary[*code as usize]));
                    out.codes.extend(codes.iter().map(|code| *code as u32));
                } else {
                    let text = self.text()?;
                    for position in rank..rank + len {
                        let (start, end) = strings.range(position);
                        out.strings.push(text.get(start..end).ok_or_else(|| corrupt("string range"))?);
                    }
                }
            }
        }
        Ok(true)
    }

    fn dictionary(&self) -> StorageResult<&[&'a str]> {
        if let Some(dictionary) = self.dictionary.get() {
            return Ok(dictionary);
        }
        let strings = self.strings.ok_or_else(|| corrupt("string stream"))?;
        let text = self.text()?;
        let dictionary = (0..strings.dictionary_len()).map(|code| { let (start, end) = strings.entry_range(code); text.get(start..end).ok_or_else(|| corrupt("dictionary range")) }).collect::<StorageResult<Vec<_>>>()?;
        Ok(self.dictionary.get_or_init(|| dictionary))
    }

    fn text(&self) -> StorageResult<&'a str> {
        if let Some(text) = self.text.get() {
            return Ok(text);
        }
        let strings = self.strings.ok_or_else(|| corrupt("string stream"))?;
        let text = std::str::from_utf8(strings.region()).map_err(|_| corrupt("string utf-8"))?;
        Ok(*self.text.get_or_init(|| text))
    }

    /// [`fact_index`] of a row's value (`None` when missing), decoding
    /// only a nested value.
    pub(super) fn fact(&self, row: usize, cursor: &mut RankCursor) -> StorageResult<Option<usize>> {
        Ok(match self.tag(row) {
            K_MISSING => None,
            K_NULL => Some(0),
            K_FALSE | K_TRUE => Some(1),
            K_STRING => Some(3),
            K_NESTED => cell_value(self.get_ranked(row, cursor)?, None)?.as_ref().map(fact_index),
            _ => Some(2),
        })
    }

    /// Decodes one row, counting the kinds before it from where `cursor`
    /// stopped when it is in the same block: ascending rows of a block cost
    /// one pass over its tags.
    pub(super) fn get_ranked(&self, row: usize, cursor: &mut RankCursor) -> StorageResult<Cell<'a>> {
        if row >= self.rows {
            return Err(corrupt("row out of range"));
        }
        let kind = self.tag(row);
        if let Tags::Const(_) = self.tags {
            return self.cell(kind, row);
        }
        let block = row / BLOCK;
        if cursor.block != Some(block) || cursor.row > row {
            *cursor = RankCursor { block: Some(block), row: block * BLOCK, ranks: self.ranks_at(block) };
        }
        for earlier in cursor.row..row {
            cursor.ranks[self.tag(earlier) as usize % KINDS] += 1;
        }
        cursor.row = row;
        self.cell(kind, cursor.ranks[kind as usize % KINDS])
    }
}

#[inline]
fn exception_row(exceptions: &[u8], at: usize) -> usize {
    u32::from_le_bytes(exceptions[at * 5..at * 5 + 4].try_into().unwrap_or([0; 4])) as usize
}

fn lower_exception(exceptions: &[u8], row: usize) -> usize { crate::helpers::partition_point(exceptions.len() / 5, |at| exception_row(exceptions, at) < row) }

/// One block of one column decoded into a slice of its single kind
/// ([`ColumnReader::decode_typed`]); only the slice of `kind` is valid.
#[derive(Debug, Default)]
pub(super) struct TypedBlock<'a> {
    pub(super) kind: u8,
    pub(super) signed: Vec<i64>,
    pub(super) unsigned: Vec<u64>,
    pub(super) floats: Vec<f64>,
    pub(super) strings: Vec<&'a str>,
    /// Dictionary codes of the values (empty without a dictionary).
    pub(super) codes: Vec<u32>,
}

impl TypedBlock<'_> {
    /// Keeps the rows at `live` (ascending offsets).
    pub(super) fn retain(&mut self, live: &[usize]) {
        fn keep<T: Copy>(values: &mut Vec<T>, live: &[usize]) {
            if values.is_empty() {
                return;
            }
            for (to, from) in live.iter().enumerate() {
                values[to] = values[*from];
            }
            values.truncate(live.len());
        }
        match self.kind {
            K_SIGNED => keep(&mut self.signed, live),
            K_UNSIGNED => keep(&mut self.unsigned, live),
            K_FLOAT => keep(&mut self.floats, live),
            _ => keep(&mut self.strings, live),
        }
        keep(&mut self.codes, live);
    }
}

/// Reusable decode buffers of one block, and the last identifier block
/// [`IdsReader::find`] decoded, keyed by where its reader's bytes live
/// (they outlive the scratch: both live under one state).
#[derive(Debug)]
pub(super) struct Scratch {
    ints: Vec<u64>,
    lows: Vec<u64>,
    floats: Vec<f64>,
    ids: Vec<u128>,
    ids_at: Option<(usize, usize)>,
}

impl Default for Scratch {
    fn default() -> Self { Self { ints: vec![0; BLOCK], lows: vec![0; BLOCK], floats: vec![0.0; BLOCK], ids: Vec::with_capacity(BLOCK), ids_at: None } }
}

const IDS_DELTA: u8 = 0;
const IDS_RAW: u8 = 1;
const IDS_SPLIT: u8 = 2;

/// Encodes strictly increasing identifiers: block anchors, then the deltas
/// when each fits 64 bits, else the high halves as deltas within each block
/// and the low halves as values (two integer streams).
pub(super) fn write_ids(ids: &[u128], out: &mut Vec<u8>) {
    let deltas = ids.iter().enumerate().map(|(row, id)| if row % BLOCK == 0 { Some(0) } else { u64::try_from(id - ids[row - 1]).ok() }).collect::<Option<Vec<_>>>();
    put_u8(out, if deltas.is_some() { IDS_DELTA } else { IDS_SPLIT });
    put_u32(out, ids.len() as u32);
    for id in ids.iter().step_by(BLOCK) {
        put_u128(out, *id);
    }
    match deltas {
        Some(deltas) => write_ints(&deltas, out),
        None => {
            let high = |id: u128| (id >> 64) as u64;
            write_ints(&ids.iter().enumerate().map(|(row, id)| if row % BLOCK == 0 { 0 } else { high(*id) - high(ids[row - 1]) }).collect::<Vec<_>>(), out);
            write_ints(&ids.iter().map(|id| *id as u64).collect::<Vec<_>>(), out);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum IdStreams<'a> {
    Delta(Ints<'a>),
    Split { high: Ints<'a>, low: Ints<'a> },
    Raw(&'a [u8]),
}

/// Reader of an identifier chunk.
#[derive(Clone, Copy, Debug)]
pub(super) struct IdsReader<'a> {
    rows: usize,
    anchors: &'a [u8],
    streams: IdStreams<'a>,
}

impl<'a> IdsReader<'a> {
    pub(super) fn open(bytes: &'a [u8]) -> StorageResult<Self> {
        let mut reader = Reader::new(bytes);
        let mode = reader.u8()?;
        let rows = reader.u32()? as usize;
        let anchors = reader.take(rows.div_ceil(BLOCK) * 16)?;
        let streams = match mode {
            IDS_DELTA => IdStreams::Delta(Ints::read(&mut reader, rows)?),
            IDS_SPLIT => IdStreams::Split { high: Ints::read(&mut reader, rows)?, low: Ints::read(&mut reader, rows)? },
            IDS_RAW => IdStreams::Raw(reader.take(rows * 16)?),
            _ => return Err(corrupt("identifier mode")),
        };
        Ok(Self { rows, anchors, streams })
    }

    #[must_use]
    pub(super) const fn rows(&self) -> usize { self.rows }

    #[inline]
    fn anchor(&self, block: usize) -> u128 {
        self.anchors.get(block * 16..block * 16 + 16).map_or(0, |b| u128::from_le_bytes(b.try_into().unwrap_or([0; 16])))
    }

    /// Decodes the identifiers of one block into `out` (cleared first).
    pub(super) fn decode_block(&self, block: usize, out: &mut Vec<u128>, scratch: &mut Scratch) {
        out.clear();
        let start = block * BLOCK;
        let len = BLOCK.min(self.rows.saturating_sub(start));
        match self.streams {
            IdStreams::Delta(deltas) => {
                let ints = &mut scratch.ints[..len];
                deltas.decode(start, ints);
                let mut id = self.anchor(block);
                for delta in ints.iter() {
                    id = id.wrapping_add(u128::from(*delta));
                    out.push(id);
                }
            }
            IdStreams::Split { high, low } => {
                let (highs, lows) = (&mut scratch.ints[..len], &mut scratch.lows[..len]);
                high.decode(start, highs);
                low.decode(start, lows);
                let mut at = (self.anchor(block) >> 64) as u64;
                out.extend(highs.iter().zip(lows.iter()).map(|(delta, low)| {
                    at = at.wrapping_add(*delta);
                    u128::from(at) << 64 | u128::from(*low)
                }));
            }
            IdStreams::Raw(raw) => out.extend(raw[start * 16..(start + len) * 16].chunks_exact(16).map(|b| u128::from_le_bytes(b.try_into().unwrap_or([0; 16])))),
        }
    }

    /// Block whose identifier range can hold `id`, if any.
    pub(super) fn block_of(&self, id: u128) -> Option<usize> {
        crate::helpers::partition_point(self.rows.div_ceil(BLOCK), |block| self.anchor(block) <= id).checked_sub(1)
    }

    /// Row holding `id`, if any: ascending identifiers decode each block once.
    pub(super) fn find(&self, id: u128, scratch: &mut Scratch) -> Option<usize> {
        let block = self.block_of(id)?;
        let at = Some((self.anchors.as_ptr() as usize, block));
        if scratch.ids_at != at {
            let mut ids = std::mem::take(&mut scratch.ids);
            self.decode_block(block, &mut ids, scratch);
            (scratch.ids, scratch.ids_at) = (ids, at);
        }
        scratch.ids.binary_search(&id).ok().map(|offset| block * BLOCK + offset)
    }
}

/// Owned conversion of a decoded cell (`None` for a missing field).
pub(super) fn cell_value(cell: Cell<'_>, strings: Option<&mut StringCache>) -> StorageResult<Option<Value>> {
    Ok(Some(match cell {
        Cell::Missing => return Ok(None),
        Cell::Null => Value::Null,
        Cell::Bool(value) => Value::Bool(value),
        Cell::Signed(value) => Value::Number(Number::Signed(value)),
        Cell::Unsigned(value) => Value::Number(Number::Unsigned(value)),
        Cell::Float(value) => Value::Number(Number::Float(value)),
        Cell::Str(text, code) => Value::String(match strings {
            Some(cache) if code != u32::MAX => cache.get(code, text),
            _ => Arc::from(text),
        }),
        Cell::Nested(bytes) => super::codec::decode_value(&mut Reader::new(bytes))?,
    }))
}

/// Shares one `Arc<str>` per dictionary code within a chunk.
#[derive(Debug, Default)]
pub(super) struct StringCache {
    entries: Vec<Option<Arc<str>>>,
}

impl StringCache {
    pub(super) fn clear(&mut self) { self.entries.clear(); }

    fn get(&mut self, code: u32, text: &str) -> Arc<str> {
        let code = code as usize;
        if code >= self.entries.len() {
            if code > 1 << 20 {
                return Arc::from(text);
            }
            self.entries.resize(code + 1, None);
        }
        self.entries[code].get_or_insert_with(|| Arc::from(text)).clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(cells: &[Option<Value>]) {
        let refs = cells.iter().map(Option::as_ref).collect::<Vec<_>>();
        let mut out = Vec::new();
        write_column(&refs, &mut out);
        let column = ColumnReader::open(&out).unwrap();
        let mut scratch = Scratch::default();
        for block in 0..cells.len().div_ceil(BLOCK) {
            let len = BLOCK.min(cells.len() - block * BLOCK);
            let mut decoded = vec![Cell::Missing; len];
            column.decode_block(block, &mut decoded, &mut scratch).unwrap();
            for (offset, cell) in decoded.into_iter().enumerate() {
                let row = block * BLOCK + offset;
                assert_eq!(cell_value(cell, None).unwrap(), cells[row], "row {row}");
                assert_eq!(cell_value(column.get_ranked(row, &mut RankCursor::default()).unwrap(), None).unwrap(), cells[row], "get {row}");
            }
        }
    }

    #[test] fn constant_kind_columns_round_trip() { round_trip(&(0..3000).map(|v| Some(Value::float(f64::from(v) / 4.0).unwrap())).collect::<Vec<_>>()); round_trip(&(0..2000).map(|v| Some(Value::string(format!("s{}", v % 7)))).collect::<Vec<_>>()); round_trip(&vec![None; 1500]); round_trip(&vec![Some(Value::Null); 10]); }
    #[test] fn sparse_kind_columns_round_trip() { round_trip(&(0..5000).map(|v| Some(if v % 97 == 0 { Value::signed(-v) } else if v % 1001 == 0 { Value::Null } else { Value::float(v as f64 + 0.5).unwrap() })).collect::<Vec<_>>()); }
    #[test] fn packed_kind_columns_round_trip() { round_trip(&(0..4100).map(|v| match v % 6 { 0 => None, 1 => Some(Value::bool(v % 4 == 1)), 2 => Some(Value::unsigned(v as u64 * 1_000_000_007)), 3 => Some(Value::string("x")), 4 => Some(Value::array([Value::signed(v)])), _ => Some(Value::signed(i64::MIN + v)) }).collect::<Vec<_>>()); }
    #[test] fn identifiers_round_trip_and_find() { for ids in [(0..3000u128).map(|v| (7u128 << 100) + v * 3).collect::<Vec<_>>(), (0..2100u128).map(|v| v << 80).collect::<Vec<_>>(), (0..2500u128).map(|v| ((0x01a0_0010_0058_7000 + (v / 12) * 0x1_0000) << 64) | (0xbb10_7cba_3899_2b20 + 2 * (v % 12))).collect::<Vec<_>>(), { let mut ids = (0..1500u128).map(|v| ((v / 3) << 64) | u128::from(v.wrapping_mul(0x9E37_79B9_7F4A_7C15) as u64)).collect::<Vec<_>>(); ids.sort_unstable(); ids.dedup(); ids }] { let mut out = Vec::new(); write_ids(&ids, &mut out); let reader = IdsReader::open(&out).unwrap(); let mut scratch = Scratch::default(); let mut block = Vec::new(); let mut all = Vec::new(); for b in 0..ids.len().div_ceil(BLOCK) { reader.decode_block(b, &mut block, &mut scratch); all.extend_from_slice(&block); } assert_eq!(all, ids); for (row, id) in ids.iter().enumerate().step_by(37) { assert_eq!(reader.find(*id, &mut scratch), Some(row)); } assert_eq!(reader.find(ids[5] + 1, &mut scratch), None); if ids[0] > 0 { assert_eq!(reader.find(ids[0] - 1, &mut scratch), None); } assert_eq!(reader.find(ids[ids.len() - 1] + 1, &mut scratch), None); } }
}
