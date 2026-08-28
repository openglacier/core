//! Daemon request and response protocol.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::fmt::{self, Display, Formatter};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// Current wire-protocol version.
pub const PROTOCOL_VERSION: u16 = 1;

/// Maximum MessagePack payload size accepted on the TCP listener.
pub const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;

/// Maximum MessagePack payload size emitted on the TCP listener.
pub const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Size of the big-endian length prefix used by the TCP protocol.
pub const LENGTH_PREFIX_BYTES: usize = 4;

/// Largest integer exactly representable by a JavaScript `number`.
pub const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const JS_MIN_SAFE_INTEGER: i64 = -9_007_199_254_740_991;

/// Serializes an unsigned integer without ever producing a MessagePack uint64 for JS-safe values.
pub fn serialize_js_safe_u64<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if *value <= u64::from(u32::MAX) {
        return serializer.serialize_u64(*value);
    }
    if *value > JS_MAX_SAFE_INTEGER {
        return Err(serde::ser::Error::custom("integer exceeds JavaScript safe integer range"));
    }
    serializer.serialize_f64(*value as f64)
}

/// Serializes a signed integer without ever producing a MessagePack int64 for JS-safe values.
pub fn serialize_js_safe_i64<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if *value >= i64::from(i32::MIN) && *value <= i64::from(i32::MAX) {
        return serializer.serialize_i64(*value);
    }
    if *value < JS_MIN_SAFE_INTEGER || *value > JS_MAX_SAFE_INTEGER as i64 {
        return Err(serde::ser::Error::custom("integer exceeds JavaScript safe integer range"));
    }
    serializer.serialize_f64(*value as f64)
}


/// Maximum UTF-8 byte length accepted for a string request identifier.
pub const MAX_REQUEST_ID_BYTES: usize = 128;

/// Compact request identifier echoed exactly by the daemon.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestId {
    Number(u64),
    String { len: u8, bytes: [u8; MAX_REQUEST_ID_BYTES] },
}

impl RequestId {
    pub fn string(value: &str) -> Result<Self, &'static str> {
        let raw = value.as_bytes();
        if raw.is_empty() || raw.len() > MAX_REQUEST_ID_BYTES {
            return Err("request id string length is invalid");
        }
        let mut bytes = [0_u8; MAX_REQUEST_ID_BYTES];
        bytes[..raw.len()].copy_from_slice(raw);
        Ok(Self::String { len: raw.len() as u8, bytes })
    }

    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Number(_) => None,
            Self::String { len, bytes } => std::str::from_utf8(&bytes[..usize::from(*len)]).ok(),
        }
    }
}

impl fmt::Debug for RequestId { fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result { Display::fmt(self, formatter) } }

impl Serialize for RequestId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            Self::Number(value) => serialize_js_safe_u64(value, serializer),
            Self::String { .. } => serializer.serialize_str(self.as_str().expect("validated request id")),
        }
    }
}

impl<'de> Deserialize<'de> for RequestId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum WireId { Number(u64), Float(f64), String(String) }
        match WireId::deserialize(deserializer)? {
            WireId::Number(value) => Ok(Self::Number(value)),
            WireId::Float(value) => {
                if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value > JS_MAX_SAFE_INTEGER as f64 {
                    return Err(de::Error::custom("request id number is not a JavaScript-safe integer"));
                }
                Ok(Self::Number(value as u64))
            }
            WireId::String(value) => Self::string(&value).map_err(de::Error::custom),
        }
    }
}

impl From<u64> for RequestId { fn from(value: u64) -> Self { Self::Number(value) } }
impl PartialEq<u64> for RequestId { fn eq(&self, other: &u64) -> bool { matches!(self, Self::Number(value) if value == other) } }
impl PartialEq<RequestId> for u64 { fn eq(&self, other: &RequestId) -> bool { other == self } }
impl TryFrom<String> for RequestId {
    type Error = &'static str;
    fn try_from(value: String) -> Result<Self, Self::Error> { Self::string(&value) }
}
impl TryFrom<&str> for RequestId {
    type Error = &'static str;
    fn try_from(value: &str) -> Result<Self, Self::Error> { Self::string(value) }
}
impl Display for RequestId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(value) => Display::fmt(value, formatter),
            Self::String { .. } => formatter.write_str(self.as_str().expect("validated request id")),
        }
    }
}

/// One bounded MessagePack response in a streamed query result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum StreamResponse {
    Partial { kind: String, version: u16, id: RequestId, data: Value },
    Complete { kind: String, version: u16, id: RequestId, statistics: Option<Value> },
    Error { kind: String, version: u16, id: Option<RequestId>, error: WireError },
}

impl StreamResponse {
    #[must_use] pub fn partial(id: impl Into<RequestId>, data: Value) -> Self { Self::Partial { kind: "response".to_owned(), version: PROTOCOL_VERSION, id: id.into(), data } }
    #[must_use] pub fn complete(id: impl Into<RequestId>, statistics: Option<Value>) -> Self { Self::Complete { kind: "response".to_owned(), version: PROTOCOL_VERSION, id: id.into(), statistics } }
    #[must_use] pub fn error(id: Option<RequestId>, error: WireError) -> Self { Self::Error { kind: "response".to_owned(), version: PROTOCOL_VERSION, id, error } }
    #[must_use] pub fn kind(&self) -> &str { match self { Self::Partial { kind, .. } | Self::Complete { kind, .. } | Self::Error { kind, .. } => kind, } }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    pub version: u16,
    pub id: RequestId,
    pub query: String,
}

impl QueryRequest {
    /// Creates a request using the current protocol version.
    #[must_use]
    pub fn new(id: impl Into<RequestId>, query: impl Into<String>) -> Self { Self { version: PROTOCOL_VERSION, id: id.into(), query: query.into(), } }

    /// Validates protocol-level request invariants.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion {
                received: self.version,
                supported: PROTOCOL_VERSION,
            });
        }
        if self.query.trim().is_empty() {
            return Err(ProtocolError::EmptyQuery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum QueryResponse {
    /// The query completed successfully.
    Ok {
        version: u16,
        id: RequestId,
        documents: Vec<Value>,
        /// Optional engine statistics.
        statistics: Option<Value>,
    },

    /// The query or protocol operation failed.
    Error {
        /// Wire-protocol version used by the server.
        version: u16,
        id: Option<RequestId>,
        error: WireError,
    },
}

impl QueryResponse {
    /// Creates a successful response.
    #[must_use]
    pub fn success(id: impl Into<RequestId>, documents: Vec<Value>, statistics: Option<Value>) -> Self {
        Self::Ok {
            version: PROTOCOL_VERSION,
            id: id.into(),
            documents,
            statistics,
        }
    }

    /// Creates an error response associated with a known request.
    #[must_use]
    pub fn request_error(id: impl Into<RequestId>, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Error {
            version: PROTOCOL_VERSION,
            id: Some(id.into()),
            error: WireError::new(code, message),
        }
    }

    #[must_use]
    pub fn protocol_error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Error {
            version: PROTOCOL_VERSION,
            id: None,
            error: WireError::new(code, message),
        }
    }

    #[must_use]
    pub const fn id(&self) -> Option<RequestId> {
        match self {
            Self::Ok { id, .. } => Some(*id),
            Self::Error { id, .. } => *id,
        }
    }

    #[must_use] pub const fn is_ok(&self) -> bool { matches!(self, Self::Ok { .. }) }
}

/// Error information exposed on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireError {
    /// Stable machine-readable error code.
    pub code: String,

    /// Human-readable error message.
    pub message: String,
}

impl WireError {
    /// Creates wire error information.
    #[must_use]
    #[inline]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

/// Protocol errors and diagnostic message kinds are defined in `error.rs`
/// and re-exported here to preserve the existing public protocol API.
pub use crate::error::{MessageKind, ProtocolError};

/// Encodes a request as one length-prefixed MessagePack message.
pub fn encode_request(request: &QueryRequest) -> Result<Vec<u8>, ProtocolError> {
    request.validate()?;
    encode_message(request, MessageKind::Request, MAX_REQUEST_BYTES)
}

/// Decodes and validates one MessagePack request payload.
pub fn decode_request(payload: &[u8]) -> Result<QueryRequest, ProtocolError> {
    ensure_payload_size(MessageKind::Request, payload.len(), MAX_REQUEST_BYTES)?;
    let request: QueryRequest = rmp_serde::from_slice(payload)
        .map_err(ProtocolError::InvalidMessagePackDecode)?;
    request.validate()?;
    Ok(request)
}

/// Encodes one streamed response as length-prefixed MessagePack.
pub fn encode_stream_response(response: &StreamResponse) -> Result<Vec<u8>, ProtocolError> {
    encode_message(response, MessageKind::Response, MAX_RESPONSE_BYTES)
}

/// Decodes one streamed MessagePack response payload.
pub fn decode_stream_response(payload: &[u8]) -> Result<StreamResponse, ProtocolError> {
    ensure_payload_size(MessageKind::Response, payload.len(), MAX_RESPONSE_BYTES)?;
    let response: StreamResponse = rmp_serde::from_slice(payload)
        .map_err(ProtocolError::InvalidMessagePackDecode)?;
    let version = match &response {
        StreamResponse::Partial { version, .. }
        | StreamResponse::Complete { version, .. }
        | StreamResponse::Error { version, .. } => *version,
    };
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion {
            received: version,
            supported: PROTOCOL_VERSION,
        });
    }
    if response.kind() != "response" {
        return Err(ProtocolError::InvalidMessageKind {
            expected: "response",
            received: response.kind().to_owned(),
        });
    }
    Ok(response)
}

/// Encodes a complete response as length-prefixed MessagePack.
pub fn encode_response(response: &QueryResponse) -> Result<Vec<u8>, ProtocolError> {
    encode_message(response, MessageKind::Response, MAX_RESPONSE_BYTES)
}

/// Decodes one complete MessagePack response payload.
pub fn decode_response(payload: &[u8]) -> Result<QueryResponse, ProtocolError> {
    ensure_payload_size(MessageKind::Response, payload.len(), MAX_RESPONSE_BYTES)?;
    let response: QueryResponse = rmp_serde::from_slice(payload)
        .map_err(ProtocolError::InvalidMessagePackDecode)?;
    validate_response_version(&response)?;
    Ok(response)
}

/// Encodes any serializable value as a named MessagePack map with a 4-byte prefix.
pub fn encode_message<T>(
    value: &T,
    kind: MessageKind,
    maximum: usize,
) -> Result<Vec<u8>, ProtocolError>
where
    T: Serialize,
{
    let payload = if kind == MessageKind::Response {
        let mut wire = serde_json::to_value(value).map_err(ProtocolError::InvalidWireProjection)?;
        normalize_js_safe_numbers(&mut wire)?;
        rmp_serde::to_vec_named(&wire).map_err(ProtocolError::InvalidMessagePackEncode)?
    } else {
        rmp_serde::to_vec_named(value).map_err(ProtocolError::InvalidMessagePackEncode)?
    };
    ensure_payload_size(kind, payload.len(), maximum)?;
    let length = u32::try_from(payload.len())
        .map_err(|_| ProtocolError::MessageTooLarge {
            kind,
            actual: payload.len(),
            maximum,
        })?;
    let mut message = Vec::with_capacity(LENGTH_PREFIX_BYTES + payload.len());
    message.extend_from_slice(&length.to_be_bytes());
    message.extend_from_slice(&payload);
    Ok(message)
}

fn normalize_js_safe_numbers(value: &mut Value) -> Result<(), ProtocolError> {
    match value {
        Value::Number(number) => {
            if let Some(unsigned) = number.as_u64() {
                if unsigned > JS_MAX_SAFE_INTEGER {
                    return Err(ProtocolError::UnsafeJavaScriptInteger { value: unsigned.to_string() });
                }
                if unsigned > u64::from(u32::MAX) {
                    *number = serde_json::Number::from_f64(unsigned as f64)
                        .expect("JavaScript-safe integer is finite");
                }
            } else if let Some(signed) = number.as_i64() {
                if signed < JS_MIN_SAFE_INTEGER || signed > JS_MAX_SAFE_INTEGER as i64 {
                    return Err(ProtocolError::UnsafeJavaScriptInteger { value: signed.to_string() });
                }
                if signed < i64::from(i32::MIN) || signed > i64::from(i32::MAX) {
                    *number = serde_json::Number::from_f64(signed as f64)
                        .expect("JavaScript-safe integer is finite");
                }
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values { normalize_js_safe_numbers(value)?; }
            Ok(())
        }
        Value::Object(values) => {
            for value in values.values_mut() { normalize_js_safe_numbers(value)?; }
            Ok(())
        }
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(()),
    }
}

/// Validates a decoded payload length before allocation or deserialization.
pub fn ensure_payload_size(
    kind: MessageKind,
    actual: usize,
    maximum: usize,
) -> Result<(), ProtocolError> {
    if actual == 0 {
        return Err(ProtocolError::InvalidPayloadLength { length: actual });
    }
    if actual > maximum {
        return Err(ProtocolError::MessageTooLarge { kind, actual, maximum });
    }
    Ok(())
}

fn validate_response_version(response: &QueryResponse) -> Result<(), ProtocolError> {
    let version = match response {
        QueryResponse::Ok { version, .. } | QueryResponse::Error { version, .. } => *version,
    };
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion {
            received: version,
            supported: PROTOCOL_VERSION,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn request_round_trip() { let request = QueryRequest::new(42, "from users | where active == true"); let encoded = encode_request(&request).expect("request encodes"); let decoded = decode_request(&encoded[LENGTH_PREFIX_BYTES..]).expect("request decodes"); assert_eq!(decoded, request); }
    #[test] fn success_response_round_trip() { let response = QueryResponse::success( 7, vec![serde_json::json!({ "name": "Ada", "active": true })], Some(serde_json::json!({ "scanned": 1, "returned": 1 })), ); let encoded = encode_response(&response).expect("response encodes"); let decoded = decode_response(&encoded[LENGTH_PREFIX_BYTES..]).expect("response decodes"); assert_eq!(decoded, response); assert!(decoded.is_ok()); assert_eq!(decoded.id(), Some(RequestId::Number(7))); }
    #[test] fn error_response_round_trip() { let response = QueryResponse::request_error(9, "query.invalid", "invalid query"); let encoded = encode_response(&response).expect("response encodes"); let decoded = decode_response(&encoded[LENGTH_PREFIX_BYTES..]).expect("response decodes"); assert_eq!(decoded, response); assert!(!decoded.is_ok()); assert_eq!(decoded.id(), Some(RequestId::Number(9))); }
    #[test] fn rejects_empty_query() { let error = decode_request(&rmp_serde::to_vec_named(&QueryRequest::new(1, "   ")).unwrap()).expect_err("empty query must fail"); assert!(matches!(error, ProtocolError::EmptyQuery)); }
    #[test] fn rejects_unknown_request_fields() { let error = decode_request(&[0xc1]).expect_err("invalid MessagePack must fail"); assert!(matches!(error, ProtocolError::InvalidMessagePackDecode(_))); }
    #[test] fn rejects_unsupported_request_version() { let payload = rmp_serde::to_vec_named(&QueryRequest { version: 2, id: RequestId::Number(1), query: "from users".to_owned() }).unwrap(); let error = decode_request(&payload).expect_err("unsupported version must fail"); assert!(matches!( error, ProtocolError::UnsupportedVersion { received: 2, supported: PROTOCOL_VERSION } )); }
    #[test] fn rejects_empty_message_payload() { let error = decode_request(&[]).expect_err("empty payload must fail"); assert!(matches!(error, ProtocolError::InvalidPayloadLength { length: 0 })); }
    #[test] fn rejects_invalid_messagepack() { let error = decode_request(&[0xc1]).expect_err("MessagePack must fail"); assert!(matches!(error, ProtocolError::InvalidMessagePackDecode(_))); }
    #[test] fn rejects_oversized_request_before_messagepack_decode() { let oversized = vec![b'x'; MAX_REQUEST_BYTES + 1]; let error = decode_request(&oversized).expect_err("oversized request must fail"); assert!(matches!( error, ProtocolError::MessageTooLarge { kind: MessageKind::Request, actual, maximum: MAX_REQUEST_BYTES } if actual == MAX_REQUEST_BYTES + 1 )); }
    #[test] fn streamed_responses_are_named_messagepack_messages() { let response = StreamResponse::partial(7, serde_json::json!({"name": "Alice"})); let encoded = encode_stream_response(&response).expect("response encodes"); let value: serde_json::Value = rmp_serde::from_slice(&encoded[LENGTH_PREFIX_BYTES..]).expect("response decodes as map"); assert_eq!(value["kind"], "response"); assert_eq!(value["status"], "partial"); assert_eq!(value["version"], PROTOCOL_VERSION); assert_eq!(value["id"], 7); }
    #[test] fn response_numbers_that_require_int64_are_encoded_as_javascript_numbers() { let response = StreamResponse::partial( RequestId::string("query-1").unwrap(), serde_json::json!({ "created_at": 1_785_680_802_608_u64, "nested": { "ts": 1_785_680_802_609_u64 }, "array": [1_785_680_802_610_u64], "small": 42_u64 }), ); let encoded = encode_stream_response(&response).expect("response encodes"); let decoded: serde_json::Value = rmp_serde::from_slice(&encoded[LENGTH_PREFIX_BYTES..]).unwrap(); assert!(decoded["data"]["created_at"].is_f64()); assert!(decoded["data"]["nested"]["ts"].is_f64()); assert!(decoded["data"]["array"][0].is_f64()); assert!(decoded["data"]["small"].is_u64()); }
    #[test] fn response_rejects_integers_above_javascript_safe_range() { let response = StreamResponse::partial(1, serde_json::json!({ "too_large": JS_MAX_SAFE_INTEGER + 1 })); let error = encode_stream_response(&response).expect_err("unsafe integer must fail"); assert!(matches!(error, ProtocolError::UnsafeJavaScriptInteger { .. })); }
    #[test] fn numeric_request_id_can_round_trip_through_js_safe_response_projection() { let response = StreamResponse::partial(4_294_967_296_u64, serde_json::json!({"ok": true})); let encoded = encode_stream_response(&response).expect("response encodes"); let decoded = decode_stream_response(&encoded[LENGTH_PREFIX_BYTES..]).expect("response decodes"); assert!(matches!(decoded, StreamResponse::Partial { id: RequestId::Number(4_294_967_296), .. })); }
}
