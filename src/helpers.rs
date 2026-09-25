#![cfg_attr(rustfmt, rustfmt_skip)]
//! Small helpers shared by the library and binaries.

pub use crate::error::Base64DecodeError;

use crate::{Document, Number, Value};
use serde_json::{Map, Value as JsonValue};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub const PLACE_SCOPE_FIELD: &str = "_place";
pub const APP_INSTANCE_SCOPE_FIELD: &str = "_app_instance";

#[must_use]
pub fn document_scope_matches(document: &Document, place_id: &str, app_instance_id: &str) -> bool {
    document.get(PLACE_SCOPE_FIELD) == Some(&Value::from(place_id))
        && document.get(APP_INSTANCE_SCOPE_FIELD) == Some(&Value::from(app_instance_id))
}

#[must_use]
pub fn enforce_document_scope( document: &Document, place_id: &str, app_instance_id: &str, ) -> Document {
    let mut scoped = document.clone();
    scoped.insert(PLACE_SCOPE_FIELD, place_id);
    scoped.insert(APP_INSTANCE_SCOPE_FIELD, app_instance_id);
    scoped
}

#[inline]
#[must_use]
pub fn elapsed_micros(started: Instant) -> u64 {
    u128_to_u64_saturating(started.elapsed().as_micros())
}
#[inline]
#[must_use]
pub fn elapsed_nanos(started: Instant) -> u64 {
    u128_to_u64_saturating(started.elapsed().as_nanos())
}
#[inline]
#[must_use]
pub fn elapsed_millis(started: Instant) -> u64 {
    u128_to_u64_saturating(started.elapsed().as_millis())
}
/// Milliseconds since the Unix epoch, or `None` for a time before it.
#[must_use]
pub fn system_time_millis(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH).ok().map(|duration| u128_to_u64_saturating(duration.as_millis()))
}
#[must_use]
pub fn unix_time_millis() -> u64 {
    system_time_millis(SystemTime::now()).unwrap_or_default()
}
#[must_use]
pub fn unix_time_nanos() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
}
#[inline]
#[must_use]
pub fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
#[inline]
#[must_use]
pub fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}
#[inline]
#[must_use]
pub fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
#[inline]
#[must_use]
pub fn u128_to_usize_saturating(value: u128) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

#[must_use]
pub fn document_to_json(document: &Document) -> JsonValue {
    JsonValue::Object(
        document
            .iter()
            .map(|(name, value)| (name.as_str().to_owned(), value_to_json(value)))
            .collect::<Map<_, _>>(),
    )
}
#[must_use]
pub fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(v) => JsonValue::Bool(*v),
        Value::Number(v) => number_to_json(*v),
        Value::String(v) => JsonValue::String(v.to_string()),
        Value::Array(v) => JsonValue::Array(v.iter().map(value_to_json).collect()),
        Value::Object(v) => document_to_json(v),
    }
}
/// Typed field accessors for JSON objects, e.g. `document.str_field("name")`.
pub trait JsonFields {
    fn field(&self, key: &str) -> Option<&JsonValue>;
    fn str_field(&self, key: &str) -> Option<&str> { self.field(key)?.as_str() }
    /// The string field, or `""` when it is missing or not a string.
    fn string_field(&self, key: &str) -> String { self.str_field(key).unwrap_or_default().to_owned() }
    fn u64_field(&self, key: &str) -> Option<u64> { self.field(key)?.as_u64() }
    fn i64_field(&self, key: &str) -> Option<i64> { self.field(key)?.as_i64() }
    fn bool_field(&self, key: &str) -> Option<bool> { self.field(key)?.as_bool() }
    fn array_field(&self, key: &str) -> Option<&Vec<JsonValue>> { self.field(key)?.as_array() }
    fn object_field(&self, key: &str) -> Option<&Map<String, JsonValue>> { self.field(key)?.as_object() }
}
impl JsonFields for JsonValue {
    fn field(&self, key: &str) -> Option<&JsonValue> { self.get(key) }
}
impl JsonFields for Map<String, JsonValue> {
    fn field(&self, key: &str) -> Option<&JsonValue> { self.get(key) }
}

/// Converts JSON into a runtime value. The error describes the unrepresentable number.
pub fn json_to_value(json: &JsonValue) -> Result<Value, String> {
    Ok(match json {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(v) => Value::Bool(*v),
        JsonValue::String(v) => Value::string(v.as_str()),
        JsonValue::Number(v) => {
            if let Some(v) = v.as_i64() {
                Value::signed(v)
            } else if let Some(v) = v.as_u64() {
                Value::unsigned(v)
            } else if let Some(v) = v.as_f64() {
                Value::float(v).map_err(|error| error.to_string())?
            } else {
                return Err("JSON number cannot be represented".to_owned());
            }
        }
        JsonValue::Array(v) => Value::array(v.iter().map(json_to_value).collect::<Result<Vec<_>, _>>()?),
        JsonValue::Object(v) => Value::object(json_object_to_document(v)?),
    })
}
pub fn json_object_to_document(object: &Map<String, JsonValue>) -> Result<Document, String> {
    let mut document = Document::new();
    for (name, value) in object {
        document.insert(name.as_str(), json_to_value(value)?);
    }
    Ok(document)
}
fn number_to_json(number: Number) -> JsonValue {
    match number {
        Number::Signed(v) => JsonValue::Number(v.into()),
        Number::Unsigned(v) => JsonValue::Number(v.into()),
        Number::Float(v) => serde_json::Number::from_f64(v)
            .map_or(JsonValue::Null, JsonValue::Number),
    }
}

/// Encodes bytes with the standard RFC 4648 Base64 alphabet and `=` padding.
#[must_use]
pub fn encode_base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        output.push(TABLE[(a >> 2) as usize] as char);
        output.push(TABLE[(((a & 0x03) << 4) | (b >> 4)) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[(((b & 0x0f) << 2) | (c >> 6)) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(c & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    output
}

/// Decodes standard RFC 4648 Base64, accepting ASCII whitespace.
pub fn decode_base64(input: &str) -> Result<Vec<u8>, Base64DecodeError> {
    const fn value(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }

    let clean: Vec<u8> = input
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect();
    if clean.is_empty() || !clean.len().is_multiple_of(4) {
        return Err(Base64DecodeError);
    }

    let chunk_count = clean.len() / 4;
    let mut output = Vec::with_capacity(chunk_count * 3);
    for (index, chunk) in clean.as_chunks::<4>().0.iter().enumerate() {
        let last = index + 1 == chunk_count;
        let padding = usize::from(chunk[3] == b'=') + usize::from(chunk[2] == b'=');
        if (!last && padding != 0) || (chunk[2] == b'=' && chunk[3] != b'=') || padding > 2 {
            return Err(Base64DecodeError);
        }
        let a = value(chunk[0]).ok_or(Base64DecodeError)?;
        let b = value(chunk[1]).ok_or(Base64DecodeError)?;
        let c = if chunk[2] == b'=' {
            0
        } else {
            value(chunk[2]).ok_or(Base64DecodeError)?
        };
        let d = if chunk[3] == b'=' {
            0
        } else {
            value(chunk[3]).ok_or(Base64DecodeError)?
        };
        // Reject non-zero unused bits, matching canonical padded Base64.
        if (padding == 2 && (b & 0x0f) != 0) || (padding == 1 && (c & 0x03) != 0) {
            return Err(Base64DecodeError);
        }
        output.push((a << 2) | (b >> 4));
        if padding < 2 {
            output.push((b << 4) | (c >> 2));
        }
        if padding == 0 {
            output.push((c << 6) | d);
        }
    }
    Ok(output)
}

#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

pub const FNV1A64_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Continues a FNV-1a 64-bit hash over `bytes`; start from [`FNV1A64_OFFSET`].
#[inline]
#[must_use]
pub fn fnv1a64_continue(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |hash, byte| (hash ^ u64::from(*byte)).wrapping_mul(FNV1A64_PRIME))
}
#[inline]
#[must_use]
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    fnv1a64_continue(FNV1A64_OFFSET, bytes)
}

/// Locks `mutex`, recovering the guard if a previous holder panicked.
#[inline]
pub fn lock_unpoisoned<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Replaces `path` atomically: `fill` writes `temporary`, which is then flushed,
/// synced and renamed over `path`. Missing parent directories are created.
pub fn write_file_atomic<T>( path: &Path, temporary: &Path, fill: impl FnOnce(&mut File) -> io::Result<T>, ) -> io::Result<T> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let mut output = OpenOptions::new().write(true).create(true).truncate(true).open(temporary)?;
    let result = fill(&mut output)?;
    output.flush()?;
    output.sync_all()?;
    drop(output);
    fs::rename(temporary, path)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn base64_round_trip_is_stable() { let input = b"openglacier authentication"; assert_eq!(decode_base64(&encode_base64(input)).unwrap(), input); }
    #[test] fn standard_vectors_are_stable() { assert_eq!(encode_base64(b""), ""); assert_eq!(encode_base64(b"f"), "Zg=="); assert_eq!(encode_base64(b"fo"), "Zm8="); assert_eq!(encode_base64(b"foo"), "Zm9v"); assert_eq!(decode_base64("Zm9v").unwrap(), b"foo"); }
    #[test] fn fnv1a64_matches_reference_vectors() { assert_eq!(fnv1a64(b""), FNV1A64_OFFSET); assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c); assert_eq!(fnv1a64_continue(fnv1a64(b"fo"), b"o"), fnv1a64(b"foo")); }
    #[test] fn json_round_trips_through_value() { let json = serde_json::json!({"a": [1, -2, 2.5, "x", null, true], "b": {"c": 18446744073709551615u64}}); assert_eq!(value_to_json(&json_to_value(&json).unwrap()), json); }
    #[test] fn json_fields_read_typed_values() { let json = serde_json::json!({"s": "x", "n": 3, "b": true}); assert_eq!(json.str_field("s"), Some("x")); assert_eq!(json.string_field("n"), ""); assert_eq!(json.u64_field("n"), Some(3)); assert_eq!(json.bool_field("b"), Some(true)); assert_eq!(json.as_object().unwrap().str_field("missing"), None); }
    #[test] fn malformed_padding_is_rejected() { assert!(decode_base64("Zg=a").is_err()); assert!(decode_base64("Zg==AAAA").is_err()); assert!(decode_base64("Zh==").is_err()); }
}
