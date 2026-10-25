#![cfg_attr(rustfmt, rustfmt_skip)]
//! Byte-level helpers shared by the Glacier files: little-endian cursor,
//! varints, a fast 64-bit checksum and the compact value codec used by the
//! write-ahead log and by nested (array/object) column cells.

use std::sync::Arc;

use crate::{model::{Document, FieldPathSegment, Number, Value}, storage::{ProjectedValueRef, StorageError, StorageResult}};

pub(super) fn corrupt(what: &str) -> StorageError {
    StorageError::backend(format!("Glacier corrupt data: {what}"))
}

/// Checksum over 8-byte words (multiply-rotate mix, final avalanche).
#[must_use]
pub(super) fn checksum(bytes: &[u8]) -> u64 {
    const K: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut h = 0x2545_F491_4F6C_DD1D ^ (bytes.len() as u64).wrapping_mul(K);
    let mut words = bytes.chunks_exact(8);
    for word in &mut words {
        let w = u64::from_le_bytes(word.try_into().unwrap_or([0; 8]));
        h = (h ^ w).wrapping_mul(K).rotate_left(29);
    }
    let mut tail = [0u8; 8];
    let rest = words.remainder();
    tail[..rest.len()].copy_from_slice(rest);
    h = (h ^ u64::from_le_bytes(tail)).wrapping_mul(K);
    h ^= h >> 31;
    h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h ^ (h >> 29)
}

pub(super) fn put_u8(out: &mut Vec<u8>, v: u8) { out.push(v); }
pub(super) fn put_u32(out: &mut Vec<u8>, v: u32) { out.extend_from_slice(&v.to_le_bytes()); }
pub(super) fn put_u64(out: &mut Vec<u8>, v: u64) { out.extend_from_slice(&v.to_le_bytes()); }
pub(super) fn put_u128(out: &mut Vec<u8>, v: u128) { out.extend_from_slice(&v.to_le_bytes()); }

pub(super) fn put_var(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub(super) fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_var(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// Bounds-checked little-endian reader over borrowed bytes.
#[derive(Clone, Copy, Debug)]
pub(super) struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(super) const fn new(bytes: &'a [u8]) -> Self { Self { bytes, pos: 0 } }
    pub(super) const fn remaining(&self) -> usize { self.bytes.len() - self.pos }

    pub(super) fn take(&mut self, len: usize) -> StorageResult<&'a [u8]> {
        let end = self.pos.checked_add(len).filter(|end| *end <= self.bytes.len()).ok_or_else(|| corrupt("truncated record"))?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    pub(super) fn u8(&mut self) -> StorageResult<u8> { Ok(self.take(1)?[0]) }
    pub(super) fn u32(&mut self) -> StorageResult<u32> { Ok(u32::from_le_bytes(self.take(4)?.try_into().map_err(|_| corrupt("u32"))?)) }
    #[inline]
    pub(super) fn u64(&mut self) -> StorageResult<u64> { Ok(u64::from_le_bytes(self.take(8)?.try_into().map_err(|_| corrupt("u64"))?)) }
    pub(super) fn u128(&mut self) -> StorageResult<u128> { Ok(u128::from_le_bytes(self.take(16)?.try_into().map_err(|_| corrupt("u128"))?)) }

    pub(super) fn var(&mut self) -> StorageResult<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7F) << shift;
            if byte < 0x80 {
                return Ok(value);
            }
        }
        Err(corrupt("varint overflow"))
    }

    pub(super) fn len(&mut self) -> StorageResult<usize> {
        usize::try_from(self.var()?).map_err(|_| corrupt("length"))
    }

    pub(super) fn bytes(&mut self) -> StorageResult<&'a [u8]> {
        let len = self.len()?;
        self.take(len)
    }

    pub(super) fn str(&mut self) -> StorageResult<&'a str> {
        std::str::from_utf8(self.bytes()?).map_err(|_| corrupt("utf-8"))
    }
}

const V_NULL: u8 = 0;
const V_FALSE: u8 = 1;
const V_TRUE: u8 = 2;
const V_SIGNED: u8 = 3;
const V_UNSIGNED: u8 = 4;
const V_FLOAT: u8 = 5;
const V_STRING: u8 = 6;
const V_ARRAY: u8 = 7;
const V_OBJECT: u8 = 8;

const fn zigzag(v: i64) -> u64 { ((v << 1) ^ (v >> 63)) as u64 }
const fn unzigzag(v: u64) -> i64 { ((v >> 1) as i64) ^ -((v & 1) as i64) }

pub(super) fn encode_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.push(V_NULL),
        Value::Bool(false) => out.push(V_FALSE),
        Value::Bool(true) => out.push(V_TRUE),
        Value::Number(Number::Signed(v)) => { out.push(V_SIGNED); put_var(out, zigzag(*v)); }
        Value::Number(Number::Unsigned(v)) => { out.push(V_UNSIGNED); put_var(out, *v); }
        Value::Number(Number::Float(v)) => { out.push(V_FLOAT); put_u64(out, v.to_bits()); }
        Value::String(v) => { out.push(V_STRING); put_bytes(out, v.as_bytes()); }
        Value::Array(items) => {
            out.push(V_ARRAY);
            put_var(out, items.len() as u64);
            for item in items.iter() {
                encode_value(out, item);
            }
        }
        Value::Object(document) => { out.push(V_OBJECT); encode_document(out, document); }
    }
}

pub(super) fn encode_document(out: &mut Vec<u8>, document: &Document) {
    put_var(out, document.len() as u64);
    for (name, value) in document.iter() {
        put_bytes(out, name.as_str().as_bytes());
        encode_value(out, value);
    }
}

const MAX_NESTING: usize = 256;

pub(super) fn decode_value(reader: &mut Reader<'_>) -> StorageResult<Value> { decode_value_at(reader, 0) }

fn decode_value_at(reader: &mut Reader<'_>, depth: usize) -> StorageResult<Value> {
    if depth > MAX_NESTING {
        return Err(corrupt("nesting too deep"));
    }
    Ok(match reader.u8()? {
        V_NULL => Value::Null,
        V_FALSE => Value::Bool(false),
        V_TRUE => Value::Bool(true),
        V_SIGNED => Value::Number(Number::Signed(unzigzag(reader.var()?))),
        V_UNSIGNED => Value::Number(Number::Unsigned(reader.var()?)),
        V_FLOAT => Value::Number(Number::float(f64::from_bits(reader.u64()?)).map_err(|_| corrupt("non-finite float"))?),
        V_STRING => Value::String(Arc::from(reader.str()?)),
        V_ARRAY => {
            let len = reader.len()?;
            let mut items = Vec::with_capacity(len.min(reader.remaining()));
            for _ in 0..len {
                items.push(decode_value_at(reader, depth + 1)?);
            }
            Value::Array(Arc::from(items))
        }
        V_OBJECT => Value::Object(Arc::new(decode_document_at(reader, depth + 1)?)),
        _ => return Err(corrupt("value tag")),
    })
}

pub(super) fn decode_document(reader: &mut Reader<'_>) -> StorageResult<Document> { decode_document_at(reader, 0) }

/// Moves `reader` past one encoded value.
fn skip_value(reader: &mut Reader<'_>, depth: usize) -> StorageResult<()> {
    if depth > MAX_NESTING {
        return Err(corrupt("nesting too deep"));
    }
    match reader.u8()? {
        V_NULL | V_FALSE | V_TRUE => {}
        V_SIGNED | V_UNSIGNED => { reader.var()?; }
        V_FLOAT => { reader.take(8)?; }
        V_STRING => { reader.bytes()?; }
        V_ARRAY => {
            for _ in 0..reader.len()? {
                skip_value(reader, depth + 1)?;
            }
        }
        V_OBJECT => {
            for _ in 0..reader.len()? {
                reader.bytes()?;
                skip_value(reader, depth + 1)?;
            }
        }
        _ => return Err(corrupt("value tag")),
    }
    Ok(())
}

/// The value at `path` inside the encoded value `bytes` (`None`: missing),
/// read without decoding the fields it does not cross; strings borrow
/// `bytes`.
#[inline(always)]
pub(super) fn value_at_path<'a>(bytes: &'a [u8], path: &[FieldPathSegment]) -> StorageResult<Option<ProjectedValueRef<'a>>> {
    let mut reader = Reader::new(bytes);
    'segments: for segment in path {
        if reader.u8()? != V_OBJECT {
            return Ok(None);
        }
        for _ in 0..reader.len()? {
            // Names compare as bytes: a valid UTF-8 name equals the segment
            // only if its bytes do.
            if reader.bytes()? == segment.as_str().as_bytes() {
                continue 'segments;
            }
            skip_value(&mut reader, 1)?;
        }
        return Ok(None);
    }
    let start = reader;
    Ok(Some(match reader.u8()? {
        V_NULL => ProjectedValueRef::Null,
        V_FALSE => ProjectedValueRef::Bool(false),
        V_TRUE => ProjectedValueRef::Bool(true),
        V_SIGNED => ProjectedValueRef::Signed(unzigzag(reader.var()?)),
        V_UNSIGNED => ProjectedValueRef::Unsigned(reader.var()?),
        V_FLOAT => ProjectedValueRef::Float(f64::from_bits(reader.u64()?)),
        V_STRING => ProjectedValueRef::String(reader.str()?),
        _ => ProjectedValueRef::Owned(decode_value(&mut { start })?),
    }))
}

fn decode_document_at(reader: &mut Reader<'_>, depth: usize) -> StorageResult<Document> {
    let len = reader.len()?;
    let mut document = Document::new();
    for _ in 0..len {
        let name = reader.str()?;
        let value = decode_value_at(reader, depth)?;
        document.insert(name, value);
    }
    Ok(document)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn value_codec_round_trips_nested_values() { let mut inner = Document::new(); inner.insert("x", Value::signed(-5)); let mut document = Document::new(); document.insert("a", Value::array([Value::Null, Value::bool(true), Value::unsigned(u64::MAX), Value::float(1.25).unwrap(), Value::string("é")])); document.insert("b", Value::object(inner)); let mut out = Vec::new(); encode_document(&mut out, &document); let decoded = decode_document(&mut Reader::new(&out)).unwrap(); assert_eq!(decoded, document); }
    #[test] fn checksum_depends_on_every_byte_and_length() { assert_ne!(checksum(b"abcdefgh1"), checksum(b"abcdefgh2")); assert_ne!(checksum(b""), checksum(b"\0")); }
    #[test] fn value_at_path_reads_one_nested_value() { let mut inner = Document::new(); inner.insert("skip", Value::array([Value::object(Document::new()), Value::string("x")])); inner.insert("name", Value::string("Ada")); inner.insert("deep", Value::object(Document::from_fields([("n", Value::signed(-3))]))); let mut out = Vec::new(); encode_value(&mut out, &Value::object(inner)); let path = |names: &[&str]| names.iter().map(|name| FieldPathSegment::new(*name)).collect::<Vec<_>>(); assert!(matches!(value_at_path(&out, &path(&["name"])).unwrap(), Some(ProjectedValueRef::String("Ada")))); assert!(matches!(value_at_path(&out, &path(&["deep", "n"])).unwrap(), Some(ProjectedValueRef::Signed(-3)))); assert!(matches!(value_at_path(&out, &path(&["deep"])).unwrap(), Some(ProjectedValueRef::Owned(Value::Object(_))))); assert!(value_at_path(&out, &path(&["absent"])).unwrap().is_none()); assert!(value_at_path(&out, &path(&["name", "more"])).unwrap().is_none()); }
    #[test] fn truncated_input_is_an_error() { let mut out = Vec::new(); encode_value(&mut out, &Value::string("hello")); assert!(decode_value(&mut Reader::new(&out[..3])).is_err()); }
}
