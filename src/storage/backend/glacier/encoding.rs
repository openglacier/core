#![cfg_attr(rustfmt, rustfmt_skip)]
//! Column value streams of Glacier.
//!
//! Every stream is random-access by position, so a reader can decode any
//! 1 024-row block without touching the rest of the chunk:
//! - integers (`u64` domain): constant, frame of reference + bit packing, or
//!   a sorted dictionary with bit-packed codes, whichever is smallest;
//! - floats: scaled decimals (`m / 10^e`, bit-exact, with exceptions) stored
//!   as an integer stream, or their raw bit patterns as an integer stream;
//! - strings: dictionary with bit-packed codes, or plain offsets + bytes;
//! - blobs (nested values): plain offsets + value-codec bytes.

use std::collections::HashMap;

use super::codec::{corrupt, put_u32, put_u64, put_u8, Reader};
use crate::storage::StorageResult;

/// Zero bytes appended after packed data so that unaligned 16-byte loads
/// never run past the stream.
const PACK_PADDING: usize = 16;

#[inline]
#[must_use]
pub(super) const fn bit_width(max: u64) -> u32 { 64 - max.leading_zeros() }

#[inline]
const fn mask(width: u32) -> u64 { if width >= 64 { u64::MAX } else { (1u64 << width) - 1 } }

pub(super) fn pack(values: impl Iterator<Item = u64>, count: usize, width: u32, out: &mut Vec<u8>) {
    let start = out.len();
    let bytes = (count * width as usize).div_ceil(8);
    out.resize(start + bytes + PACK_PADDING, 0);
    if width == 0 {
        out.truncate(start);
        return;
    }
    let packed = &mut out[start..];
    let mut bit = 0usize;
    for value in values {
        let value = u128::from(value & mask(width)) << (bit & 7);
        let byte = bit >> 3;
        let mut word = u128::from_le_bytes(packed[byte..byte + 16].try_into().unwrap_or([0; 16]));
        word |= value;
        packed[byte..byte + 16].copy_from_slice(&word.to_le_bytes());
        bit += width as usize;
    }
    out.truncate(start + bytes + PACK_PADDING);
}

#[inline]
fn packed_len(count: usize, width: u32) -> usize {
    if width == 0 { 0 } else { (count * width as usize).div_ceil(8) + PACK_PADDING }
}

/// Bit-packed values of one width, read by position.
#[derive(Clone, Copy, Debug)]
pub(super) struct Packed<'a> {
    bytes: &'a [u8],
    width: u32,
}

impl<'a> Packed<'a> {
    fn read(reader: &mut Reader<'a>, count: usize, width: u32) -> StorageResult<Self> {
        if width > 64 {
            return Err(corrupt("bit width"));
        }
        Ok(Self { bytes: reader.take(packed_len(count, width))?, width })
    }

    #[inline]
    fn get(&self, index: usize) -> u64 {
        if self.width == 0 {
            return 0;
        }
        let bit = index * self.width as usize;
        let byte = bit >> 3;
        let shift = bit & 7;
        if self.width <= 56 {
            let word = self.bytes.get(byte..byte + 8).map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap_or([0; 8])));
            (word >> shift) & mask(self.width)
        } else {
            let word = self.bytes.get(byte..byte + 16).map_or(0, |b| u128::from_le_bytes(b.try_into().unwrap_or([0; 16])));
            ((word >> shift) as u64) & mask(self.width)
        }
    }

    #[inline]
    fn unpack(&self, start: usize, out: &mut [u64]) {
        if self.width == 0 {
            out.fill(0);
            return;
        }
        let bit = start * self.width as usize;
        let done = match self.bytes.get(bit >> 3..) {
            Some(bytes) => unpack_fixed(bytes, bit & 7, self.width, out),
            None => 0,
        };
        for (offset, slot) in out.iter_mut().enumerate().skip(done) {
            *slot = self.get(start + offset);
        }
    }
}

/// Unpacks whole runs of 8 values of `width` bits from `bytes`, the first at
/// bit `shift`; returns how many values it wrote (the rest is left to the
/// caller), 0 for a width it does not specialize.
#[inline]
fn unpack_fixed(bytes: &[u8], shift: usize, width: u32, out: &mut [u64]) -> usize {
    macro_rules! widths {
        ($($width:literal)*) => {
            match width { $($width => unpack_runs::<$width>(bytes, shift, out),)* _ => 0 }
        };
    }
    widths!(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32
        33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64)
}

/// Each run of 8 values spans `WIDTH` bytes from a byte boundary: one bounds
/// check per run, word loads at offsets known at compile time (16-byte
/// loads above 56 bits, within [`PACK_PADDING`]).
#[inline(always)]
fn unpack_runs<const WIDTH: usize>(bytes: &[u8], shift: usize, out: &mut [u64]) -> usize {
    // A block starting on a byte (the usual case) reads at constant offsets.
    if shift == 0 { unpack_shifted::<WIDTH>(bytes, 0, out) } else { unpack_shifted::<WIDTH>(bytes, shift & 7, out) }
}

#[inline(always)]
fn unpack_shifted<const WIDTH: usize>(bytes: &[u8], shift: usize, out: &mut [u64]) -> usize {
    let (mask, load) = (mask(WIDTH as u32), if WIDTH <= 56 { 8 } else { 16 });
    let mut done = 0;
    for (run, values) in out.chunks_exact_mut(8).enumerate() {
        // `shift + 7 * WIDTH` bits in, a word load ends before `WIDTH + load`.
        let Some(window) = bytes.get(run * WIDTH..run * WIDTH + WIDTH + load) else { break };
        for (index, value) in values.iter_mut().enumerate() {
            let bit = shift + index * WIDTH;
            *value = if WIDTH <= 56 {
                (u64::from_le_bytes(window[bit >> 3..(bit >> 3) + 8].try_into().unwrap_or([0; 8])) >> (bit & 7)) & mask
            } else {
                ((u128::from_le_bytes(window[bit >> 3..(bit >> 3) + 16].try_into().unwrap_or([0; 16])) >> (bit & 7)) as u64) & mask
            };
        }
        done += 8;
    }
    done
}

const INT_CONST: u8 = 0;
const INT_FOR: u8 = 1;
const INT_DICT: u8 = 2;
const MAX_INT_DICT: usize = 1 << 16;

/// Writes an integer stream with the cheapest of the three encodings.
pub(super) fn write_ints(values: &[u64], out: &mut Vec<u8>) {
    let Some(&first) = values.first() else {
        put_u8(out, INT_CONST);
        put_u64(out, 0);
        return;
    };
    let (mut min, mut max) = (first, first);
    for &value in values {
        min = min.min(value);
        max = max.max(value);
    }
    if min == max {
        put_u8(out, INT_CONST);
        put_u64(out, min);
        return;
    }
    let for_width = bit_width(max - min);
    let for_bytes = packed_len(values.len(), for_width) + 9;
    let dictionary = (for_width > 8).then(|| small_dictionary(values)).flatten();
    if let Some(dictionary) = dictionary {
        let code_width = bit_width(dictionary.len() as u64 - 1);
        let dict_bytes = packed_len(values.len(), code_width) + 5 + dictionary.len() * 8;
        if dict_bytes < for_bytes {
            let codes = dictionary.iter().enumerate().map(|(code, value)| (*value, code as u64)).collect::<HashMap<_, _>>();
            put_u8(out, INT_DICT);
            put_u32(out, dictionary.len() as u32);
            for value in &dictionary {
                put_u64(out, *value);
            }
            put_u8(out, code_width as u8);
            pack(values.iter().map(|value| codes[value]), values.len(), code_width, out);
            return;
        }
    }
    put_u8(out, INT_FOR);
    put_u64(out, min);
    put_u8(out, for_width as u8);
    pack(values.iter().map(|value| value - min), values.len(), for_width, out);
}

fn small_dictionary(values: &[u64]) -> Option<Vec<u64>> {
    let mut seen = HashMap::<u64, ()>::new();
    for &value in values {
        seen.insert(value, ());
        if seen.len() > MAX_INT_DICT || seen.len() * 4 > values.len() {
            return None;
        }
    }
    let mut dictionary = seen.into_keys().collect::<Vec<_>>();
    dictionary.sort_unstable();
    Some(dictionary)
}

/// Random-access reader of an integer stream.
#[derive(Clone, Copy, Debug)]
pub(super) enum Ints<'a> {
    Const(u64),
    For { base: u64, packed: Packed<'a> },
    Dict { values: &'a [u8], codes: Packed<'a> },
}

impl<'a> Ints<'a> {
    pub(super) fn read(reader: &mut Reader<'a>, count: usize) -> StorageResult<Self> {
        Ok(match reader.u8()? {
            INT_CONST => Self::Const(reader.u64()?),
            INT_FOR => {
                let base = reader.u64()?;
                let width = u32::from(reader.u8()?);
                Self::For { base, packed: Packed::read(reader, count, width)? }
            }
            INT_DICT => {
                let len = reader.u32()? as usize;
                let values = reader.take(len.checked_mul(8).ok_or_else(|| corrupt("dictionary length"))?)?;
                let width = u32::from(reader.u8()?);
                if len == 0 {
                    return Err(corrupt("empty dictionary"));
                }
                Self::Dict { values, codes: Packed::read(reader, count, width)? }
            }
            _ => return Err(corrupt("integer stream kind")),
        })
    }

    #[inline]
    pub(super) fn get(&self, index: usize) -> u64 {
        match self {
            Self::Const(value) => *value,
            Self::For { base, packed } => base.wrapping_add(packed.get(index)),
            Self::Dict { values, codes } => dict_value(values, codes.get(index)),
        }
    }

    /// Values from `start` into `out`, each mapped by `map` in the pass
    /// that adds the frame base (unpacked through `scratch`); a dictionary
    /// stream also appends to `codes` the codes of the values (under one
    /// stream, equal codes are equal values and different codes different
    /// values).
    #[inline]
    pub(super) fn decode_map<T>(&self, start: usize, scratch: &mut [u64], out: &mut [T], codes: Option<&mut Vec<u32>>, map: impl Fn(u64) -> T) {
        let scratch = &mut scratch[..out.len()];
        match self {
            Self::Const(value) => out.iter_mut().for_each(|slot| *slot = map(*value)),
            Self::For { base, packed } => {
                packed.unpack(start, scratch);
                out.iter_mut().zip(scratch.iter()).for_each(|(slot, raw)| *slot = map(base.wrapping_add(*raw)));
            }
            Self::Dict { values, codes: packed } => {
                packed.unpack(start, scratch);
                if let Some(codes) = codes {
                    codes.extend(scratch.iter().map(|code| *code as u32));
                }
                Self::lookup(values, scratch);
                out.iter_mut().zip(scratch.iter()).for_each(|(slot, value)| *slot = map(*value));
            }
        }
    }

    pub(super) fn decode(&self, start: usize, out: &mut [u64]) {
        match self {
            Self::Const(value) => out.fill(*value),
            Self::For { base, packed } => {
                packed.unpack(start, out);
                if *base != 0 {
                    for slot in out.iter_mut() {
                        *slot = base.wrapping_add(*slot);
                    }
                }
            }
            Self::Dict { values, codes } => {
                codes.unpack(start, out);
                Self::lookup(values, out);
            }
        }
    }

    /// Replaces dictionary codes by their values.
    fn lookup(values: &[u8], out: &mut [u64]) {
        // A small dictionary is decoded once into a table.
        let mut table = [0u64; 256];
        if values.len() <= table.len() * 8 {
            for (entry, bytes) in table.iter_mut().zip(values.chunks_exact(8)) {
                *entry = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
            }
            out.iter_mut().for_each(|slot| *slot = table.get(*slot as usize).copied().unwrap_or(0));
        } else {
            out.iter_mut().for_each(|slot| *slot = dict_value(values, *slot));
        }
    }
}

#[inline]
fn dict_value(values: &[u8], code: u64) -> u64 {
    let at = code as usize * 8;
    values.get(at..at + 8).map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap_or([0; 8])))
}

/// Order-preserving map of `i64` to `u64`.
#[inline]
#[must_use]
pub(super) const fn signed_key(value: i64) -> u64 { (value as u64) ^ (1 << 63) }

#[inline]
#[must_use]
pub(super) const fn signed_from_key(key: u64) -> i64 { (key ^ (1 << 63)) as i64 }

/// `signed_from_key(key) as f64` for a value within ±2^51: added to the bits
/// of 2^52 + 2^51, whose unit in the last place is 1 (no `i64` to `f64`
/// conversion, which AVX2 lacks).
#[inline(always)]
fn small_mantissa(key: u64) -> f64 {
    const MAGIC: f64 = 6_755_399_441_055_744.0;
    f64::from_bits(MAGIC.to_bits().wrapping_add(signed_from_key(key) as u64)) - MAGIC
}

const POW10: [f64; 7] = [1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0];
const MAX_EXACT_MANTISSA: f64 = 9_007_199_254_740_992.0;
const FLOAT_DECIMAL: u8 = 0;
const FLOAT_BITS: u8 = 1;

#[inline]
fn decimal_mantissa(value: f64, exponent: usize) -> Option<i64> {
    let scaled = (value * POW10[exponent]).round();
    if scaled.abs() >= MAX_EXACT_MANTISSA {
        return None;
    }
    let mantissa = scaled as i64;
    ((mantissa as f64 / POW10[exponent]).to_bits() == value.to_bits()).then_some(mantissa)
}

/// Writes a float stream: scaled decimals with exceptions when they win,
/// raw bit patterns otherwise.
pub(super) fn write_floats(values: &[f64], out: &mut Vec<u8>) {
    let mut best: Option<(usize, usize)> = None;
    let mut exceptions_at = [0usize; POW10.len()];
    for (exponent, slot) in exceptions_at.iter_mut().enumerate() {
        *slot = values.iter().filter(|value| decimal_mantissa(**value, exponent).is_none()).count();
        if *slot * 16 <= values.len() && best.is_none_or(|(_, exceptions)| *slot < exceptions) {
            best = Some((exponent, *slot));
        }
        if *slot == 0 {
            break;
        }
    }
    if let Some((exponent, _)) = best {
        let mut mantissas = Vec::with_capacity(values.len());
        let mut exceptions = Vec::new();
        let mut fill = signed_key(0);
        for (index, value) in values.iter().enumerate() {
            match decimal_mantissa(*value, exponent) {
                Some(mantissa) => {
                    fill = signed_key(mantissa);
                    mantissas.push(fill);
                }
                None => {
                    exceptions.push((index as u32, value.to_bits()));
                    mantissas.push(fill);
                }
            }
        }
        let mut decimal = Vec::new();
        put_u8(&mut decimal, FLOAT_DECIMAL);
        put_u8(&mut decimal, exponent as u8);
        put_u32(&mut decimal, exceptions.len() as u32);
        for (index, bits) in &exceptions {
            put_u32(&mut decimal, *index);
            put_u64(&mut decimal, *bits);
        }
        write_ints(&mantissas, &mut decimal);
        let mut raw = Vec::new();
        put_u8(&mut raw, FLOAT_BITS);
        write_ints(&values.iter().map(|value| value.to_bits()).collect::<Vec<_>>(), &mut raw);
        out.extend_from_slice(if decimal.len() <= raw.len() { &decimal } else { &raw });
        return;
    }
    put_u8(out, FLOAT_BITS);
    write_ints(&values.iter().map(|value| value.to_bits()).collect::<Vec<_>>(), out);
}

/// Random-access reader of a float stream.
#[derive(Clone, Copy, Debug)]
pub(super) enum Floats<'a> {
    Decimal { divisor: f64, exceptions: &'a [u8], mantissas: Ints<'a> },
    Bits(Ints<'a>),
}

impl<'a> Floats<'a> {
    pub(super) fn read(reader: &mut Reader<'a>, count: usize) -> StorageResult<Self> {
        Ok(match reader.u8()? {
            FLOAT_DECIMAL => {
                let exponent = reader.u8()? as usize;
                let divisor = *POW10.get(exponent).ok_or_else(|| corrupt("decimal exponent"))?;
                let exceptions = reader.u32()? as usize;
                let exceptions = reader.take(exceptions.checked_mul(12).ok_or_else(|| corrupt("exceptions"))?)?;
                Self::Decimal { divisor, exceptions, mantissas: Ints::read(reader, count)? }
            }
            FLOAT_BITS => Self::Bits(Ints::read(reader, count)?),
            _ => return Err(corrupt("float stream kind")),
        })
    }

    #[inline]
    pub(super) fn get(&self, index: usize) -> f64 {
        match self {
            Self::Decimal { divisor, exceptions, mantissas } => {
                if !exceptions.is_empty() {
                    if let Some(bits) = find_exception(exceptions, index as u32) {
                        return f64::from_bits(bits);
                    }
                }
                signed_from_key(mantissas.get(index)) as f64 / divisor
            }
            Self::Bits(bits) => f64::from_bits(bits.get(index)),
        }
    }

    /// Values from `start` into `out` (unpacked through `scratch`), mapped
    /// from their mantissas or bits in one pass; `codes` gets the dictionary
    /// codes when every value is its mantissa or bits under a dictionary.
    pub(super) fn decode(&self, start: usize, scratch: &mut [u64], out: &mut [f64], codes: Option<&mut Vec<u32>>) {
        match self {
            Self::Decimal { divisor, exceptions, mantissas } => {
                let codes = codes.filter(|_| exceptions.is_empty());
                // Mantissas known from the frame to lie within ±2^51 convert
                // by a vectorizable exact path; whole ones skip the division.
                let small = matches!(mantissas, Ints::For { base, packed } if base.checked_add(mask(packed.width)).is_some_and(|top| signed_from_key(*base) >= -(1 << 51) && signed_from_key(top) < 1 << 51));
                match (small, *divisor == 1.0) {
                    (true, true) => mantissas.decode_map(start, scratch, out, codes, small_mantissa),
                    (true, false) => mantissas.decode_map(start, scratch, out, codes, |key| small_mantissa(key) / divisor),
                    (false, true) => mantissas.decode_map(start, scratch, out, codes, |key| signed_from_key(key) as f64),
                    (false, false) => mantissas.decode_map(start, scratch, out, codes, |key| signed_from_key(key) as f64 / divisor),
                }
                if !exceptions.is_empty() {
                    let end = (start + out.len()) as u32;
                    let mut at = lower_exception(exceptions, start as u32);
                    while at * 12 < exceptions.len() {
                        let (index, bits) = exception_at(exceptions, at);
                        if index >= end {
                            break;
                        }
                        out[(index as usize) - start] = f64::from_bits(bits);
                        at += 1;
                    }
                }
            }
            Self::Bits(bits) => bits.decode_map(start, scratch, out, codes, f64::from_bits),
        }
    }
}

#[inline]
fn exception_at(exceptions: &[u8], at: usize) -> (u32, u64) {
    let entry = &exceptions[at * 12..at * 12 + 12];
    (u32::from_le_bytes(entry[..4].try_into().unwrap_or([0; 4])), u64::from_le_bytes(entry[4..].try_into().unwrap_or([0; 8])))
}

fn lower_exception(exceptions: &[u8], index: u32) -> usize { crate::helpers::partition_point(exceptions.len() / 12, |at| exception_at(exceptions, at).0 < index) }

fn find_exception(exceptions: &[u8], index: u32) -> Option<u64> {
    let at = lower_exception(exceptions, index);
    (at * 12 < exceptions.len()).then(|| exception_at(exceptions, at)).filter(|(found, _)| *found == index).map(|(_, bits)| bits)
}

const BYTES_PLAIN: u8 = 0;
const BYTES_DICT: u8 = 1;

/// Writes a byte-string stream (UTF-8 strings or value-codec blobs).
pub(super) fn write_byte_strings(values: &[&[u8]], allow_dictionary: bool, out: &mut Vec<u8>) {
    let plain_bytes = values.iter().map(|value| value.len()).sum::<usize>();
    if allow_dictionary {
        let mut codes = HashMap::<&[u8], u64>::new();
        let mut dictionary = Vec::<&[u8]>::new();
        let mut assigned = Vec::with_capacity(values.len());
        let mut dictionary_bytes = 0usize;
        for value in values {
            let code = *codes.entry(value).or_insert_with(|| {
                dictionary.push(value);
                dictionary_bytes += value.len();
                dictionary.len() as u64 - 1
            });
            assigned.push(code);
        }
        let code_width = bit_width(dictionary.len().saturating_sub(1) as u64);
        let dictionary_cost = dictionary_bytes + dictionary.len() * 4 + packed_len(values.len(), code_width);
        let plain_cost = plain_bytes + packed_len(values.len(), bit_width(plain_bytes as u64));
        if dictionary_cost < plain_cost {
            put_u8(out, BYTES_DICT);
            put_u32(out, dictionary.len() as u32);
            write_plain(&dictionary, out);
            put_u8(out, code_width as u8);
            pack(assigned.into_iter(), values.len(), code_width, out);
            return;
        }
    }
    put_u8(out, BYTES_PLAIN);
    write_plain(values, out);
}

fn write_plain(values: &[&[u8]], out: &mut Vec<u8>) {
    let mut end = 0u64;
    let ends = values.iter().map(|value| { end += value.len() as u64; end }).collect::<Vec<_>>();
    write_ints(&ends, out);
    put_u64(out, end);
    for value in values {
        out.extend_from_slice(value);
    }
}

/// Plain offsets + bytes, read by position.
#[derive(Clone, Copy, Debug)]
pub(super) struct Plain<'a> {
    ends: Ints<'a>,
    bytes: &'a [u8],
    len: usize,
}

impl<'a> Plain<'a> {
    fn read(reader: &mut Reader<'a>, count: usize) -> StorageResult<Self> {
        let ends = Ints::read(reader, count)?;
        let len = usize::try_from(reader.u64()?).map_err(|_| corrupt("byte length"))?;
        Ok(Self { ends, bytes: reader.take(len)?, len: count })
    }

    #[inline]
    fn range(&self, index: usize) -> (usize, usize) {
        let start = if index == 0 { 0 } else { self.ends.get(index - 1) as usize };
        (start, self.ends.get(index) as usize)
    }
}

/// Random-access reader of a byte-string stream.
#[derive(Clone, Copy, Debug)]
pub(super) enum ByteStrings<'a> {
    Plain(Plain<'a>),
    Dict { entries: Plain<'a>, codes: Packed<'a> },
}

impl<'a> ByteStrings<'a> {
    pub(super) fn read(reader: &mut Reader<'a>, count: usize) -> StorageResult<Self> {
        Ok(match reader.u8()? {
            BYTES_PLAIN => Self::Plain(Plain::read(reader, count)?),
            BYTES_DICT => {
                let entries = reader.u32()? as usize;
                let entries = Plain::read(reader, entries)?;
                let width = u32::from(reader.u8()?);
                Self::Dict { entries, codes: Packed::read(reader, count, width)? }
            }
            _ => return Err(corrupt("byte stream kind")),
        })
    }

    /// The bytes holding every value, validated once by string readers.
    pub(super) const fn region(&self) -> &'a [u8] {
        match self {
            Self::Plain(plain) => plain.bytes,
            Self::Dict { entries, .. } => entries.bytes,
        }
    }

    /// Dictionary code of a position, when the stream is a dictionary.
    #[inline]
    pub(super) fn code(&self, index: usize) -> Option<usize> {
        match self {
            Self::Plain(_) => None,
            Self::Dict { codes, .. } => Some(codes.get(index) as usize),
        }
    }

    /// Number of dictionary entries (0 for a plain stream).
    #[must_use]
    pub(super) fn dictionary_len(&self) -> usize {
        match self {
            Self::Plain(_) => 0,
            Self::Dict { entries, .. } => entries.len,
        }
    }

    /// Byte range of dictionary entry `code`.
    #[inline]
    pub(super) fn entry_range(&self, code: usize) -> (usize, usize) {
        match self {
            Self::Plain(plain) | Self::Dict { entries: plain, .. } => plain.range(code),
        }
    }

    /// Dictionary codes of `out.len()` positions from `start`; `false` for a
    /// plain stream.
    pub(super) fn codes(&self, start: usize, out: &mut [u64]) -> bool {
        match self {
            Self::Plain(_) => false,
            Self::Dict { codes, .. } => { codes.unpack(start, out); true }
        }
    }

    /// Byte range of a position inside [`Self::region`].
    #[inline]
    pub(super) fn range(&self, index: usize) -> (usize, usize) {
        match self {
            Self::Plain(plain) => plain.range(index),
            Self::Dict { entries, codes } => entries.range(codes.get(index) as usize),
        }
    }

    #[inline]
    pub(super) fn get(&self, index: usize) -> StorageResult<&'a [u8]> {
        let (start, end) = self.range(index);
        self.region().get(start..end).ok_or_else(|| corrupt("byte range"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints_round_trip(values: &[u64]) -> usize {
        let mut out = Vec::new();
        write_ints(values, &mut out);
        let stream = Ints::read(&mut Reader::new(&out), values.len()).unwrap();
        for (index, value) in values.iter().enumerate() {
            assert_eq!(stream.get(index), *value);
        }
        if values.len() > 3 {
            let mut decoded = vec![0; values.len() - 3];
            stream.decode(3, &mut decoded);
            assert_eq!(decoded, values[3..]);
        }
        out.len()
    }

    #[test] fn integer_streams_round_trip_every_encoding() { assert!(ints_round_trip(&[7; 100]) <= 9); ints_round_trip(&[0, u64::MAX, 3, 1 << 63]); ints_round_trip(&(0..5000u64).map(|v| 1_000_000 + v * 3).collect::<Vec<_>>()); let dict = (0..4096u64).map(|v| [17u64, 1 << 40, u64::MAX - 1][v as usize % 3]).collect::<Vec<_>>(); assert!(ints_round_trip(&dict) < 4096); ints_round_trip(&[]); }
    #[test] fn wide_packing_round_trips() { for width in [55u32, 57, 63, 64] { let values = (0..300u64).map(|v| v.wrapping_mul(0x9E37_79B9_7F4A_7C15) & mask(width)).collect::<Vec<_>>(); let mut out = Vec::new(); pack(values.iter().copied(), values.len(), width, &mut out); let packed = Packed::read(&mut Reader::new(&out), values.len(), width).unwrap(); for (index, value) in values.iter().enumerate() { assert_eq!(packed.get(index), *value, "width {width}"); } } }
    #[test] fn unpacking_runs_matches_single_reads() { for width in 1..=64u32 { let values = (0..700u64).map(|v| v.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(width) & mask(width)).collect::<Vec<_>>(); let mut out = Vec::new(); pack(values.iter().copied(), values.len(), width, &mut out); let packed = Packed::read(&mut Reader::new(&out), values.len(), width).unwrap(); for (start, len) in [(0, 700), (0, 1), (3, 64), (5, 13), (691, 9), (100, 600), (256, 0)] { let mut decoded = vec![u64::MAX; len]; packed.unpack(start, &mut decoded); assert_eq!(decoded, values[start..start + len], "width {width} start {start} len {len}"); } } }
    #[test] fn small_mantissas_convert_exactly() { for mantissa in [-(1i64 << 51), -123_456_789, -1, 0, 1, 987_654_321, (1 << 51) - 1] { assert_eq!(small_mantissa(signed_key(mantissa)).to_bits(), (mantissa as f64).to_bits()); } }
    #[test] fn float_streams_are_bit_exact() { let values = [12.34, 0.1, -7.5, 1e300, 5e-324, 123_456.78, 0.0, 3.0, f64::MAX, 0.30000000000000004]; for chunk in [&values[..], &[1.25; 64][..], &(0..1000).map(|v| f64::from(v) / 100.0).collect::<Vec<_>>()[..]] { let mut out = Vec::new(); write_floats(chunk, &mut out); let stream = Floats::read(&mut Reader::new(&out), chunk.len()).unwrap(); for (index, value) in chunk.iter().enumerate() { assert_eq!(stream.get(index).to_bits(), value.to_bits()); } let mut scratch = vec![0; chunk.len()]; let mut decoded = vec![0.0; chunk.len()]; stream.decode(0, &mut scratch, &mut decoded, None); assert!(decoded.iter().zip(chunk).all(|(a, b)| a.to_bits() == b.to_bits())); } }
    #[test] fn decimal_floats_use_few_bits() { let values = (0..4096).map(|v| f64::from(v % 997) / 100.0).collect::<Vec<_>>(); let mut out = Vec::new(); write_floats(&values, &mut out); assert!(out.len() < 4096 * 2, "{}", out.len()); }
    #[test] fn byte_strings_round_trip_plain_and_dictionary() { let low = (0..1000).map(|v| ["FR01", "DE02", ""][v % 3].as_bytes()).collect::<Vec<_>>(); let owned = (0..300).map(|v| format!("article-{v}")).collect::<Vec<_>>(); let high = owned.iter().map(|v| v.as_bytes()).collect::<Vec<_>>(); for values in [&low, &high] { let mut out = Vec::new(); write_byte_strings(values, true, &mut out); let stream = ByteStrings::read(&mut Reader::new(&out), values.len()).unwrap(); for (index, value) in values.iter().enumerate() { assert_eq!(stream.get(index).unwrap(), *value); } } let mut out = Vec::new(); write_byte_strings(&low, true, &mut out); assert!(matches!(ByteStrings::read(&mut Reader::new(&out), low.len()).unwrap(), ByteStrings::Dict { .. })); }
}
