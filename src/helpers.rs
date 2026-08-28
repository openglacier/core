//! Small helpers shared by the library and binaries.

pub use crate::error::Base64DecodeError;

use crate::{Document, Number, Value};
use serde_json::{Map, Value as JsonValue};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub const PLACE_SCOPE_FIELD: &str = "_place";
pub const APP_INSTANCE_SCOPE_FIELD: &str = "_app_instance";

#[must_use]
pub fn document_scope_matches(document: &Document, place_id: &str, app_instance_id: &str) -> bool {
    document.get(PLACE_SCOPE_FIELD) == Some(&Value::from(place_id))
        && document.get(APP_INSTANCE_SCOPE_FIELD) == Some(&Value::from(app_instance_id))
}

#[must_use]
pub fn enforce_document_scope(
    document: &Document,
    place_id: &str,
    app_instance_id: &str,
) -> Document {
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
#[must_use]
pub fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
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
fn number_to_json(number: Number) -> JsonValue {
    match number {
        Number::Signed(v) => JsonValue::Number(v.into()),
        Number::Unsigned(v) => JsonValue::Number(v.into()),
        Number::Float(v) => serde_json::Number::from_f64(v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
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
    fn value(byte: u8) -> Option<u8> {
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
    if clean.is_empty() || clean.len() % 4 != 0 {
        return Err(Base64DecodeError);
    }

    let chunk_count = clean.len() / 4;
    let mut output = Vec::with_capacity(chunk_count * 3);
    for (index, chunk) in clean.chunks_exact(4).enumerate() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trip_is_stable() {
        let input = b"openglacier authentication";
        assert_eq!(decode_base64(&encode_base64(input)).unwrap(), input);
    }

    #[test]
    fn standard_vectors_are_stable() {
        assert_eq!(encode_base64(b""), "");
        assert_eq!(encode_base64(b"f"), "Zg==");
        assert_eq!(encode_base64(b"fo"), "Zm8=");
        assert_eq!(encode_base64(b"foo"), "Zm9v");
        assert_eq!(decode_base64("Zm9v").unwrap(), b"foo");
    }

    #[test]
    fn malformed_padding_is_rejected() {
        assert!(decode_base64("Zg=a").is_err());
        assert!(decode_base64("Zg==AAAA").is_err());
        assert!(decode_base64("Zh==").is_err());
    }
}
