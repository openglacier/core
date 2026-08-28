//! Operation requests and legacy query compatibility.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::catalog::QUERY_EXECUTE;

use crate::protocol::{
    MessageKind, ProtocolError, QueryRequest, RequestId, MAX_REQUEST_BYTES, PROTOCOL_VERSION,
};

/// One generic operation request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRequest {
    /// Wire protocol version.
    pub version: u16,
    /// Client-selected request identifier.
    pub id: RequestId,
    /// Stable operation name.
    pub op: String,
    /// Operation-specific data.
    #[serde(default)]
    pub data: Value,
}

impl OperationRequest {
    /// Creates an operation request using the current protocol version.
    #[must_use]
    pub fn new(id: impl Into<RequestId>, op: impl Into<String>, data: Value) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            id: id.into(),
            op: op.into(),
            data,
        }
    }

    /// Creates a `query.execute` request.
    #[must_use]
    pub fn query(id: impl Into<RequestId>, query: impl Into<String>) -> Self {
        Self::new(id, QUERY_EXECUTE, json!({ "query": query.into() }))
    }

    /// Validates protocol-level invariants.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion {
                received: self.version,
                supported: PROTOCOL_VERSION,
            });
        }
        if self.op.trim().is_empty() {
            return Err(ProtocolError::EmptyOperation);
        }
        Ok(())
    }
}

impl From<QueryRequest> for OperationRequest {
    fn from(request: QueryRequest) -> Self {
        Self::query(request.id, request.query)
    }
}

/// Accepted request forms during the protocol migration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum IncomingRequest {
    /// New generic operation request.
    Operation(OperationRequest),
    /// Legacy query request used by existing clients.
    Query(QueryRequest),
}

impl IncomingRequest {
    /// Normalizes both wire forms to one operation request.
    pub fn into_operation(self) -> Result<OperationRequest, ProtocolError> {
        let request = match self {
            Self::Operation(request) => request,
            Self::Query(request) => {
                request.validate()?;
                request.into()
            }
        };
        request.validate()?;
        Ok(request)
    }
}

/// Decodes either a generic operation request or a legacy query request.
pub fn decode_operation_request(bytes: &[u8]) -> Result<OperationRequest, ProtocolError> {
    if bytes.is_empty() {
        return Err(ProtocolError::InvalidPayloadLength { length: 0 });
    }
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(ProtocolError::MessageTooLarge {
            kind: MessageKind::Request,
            actual: bytes.len(),
            maximum: MAX_REQUEST_BYTES,
        });
    }
    let incoming = rmp_serde::from_slice::<IncomingRequest>(bytes)
        .map_err(ProtocolError::InvalidMessagePackDecode)?;
    incoming.into_operation()
}

#[cfg(test)]
mod tests {
    use super::super::catalog::AUTH_BEGIN;
    use super::*;

    #[test]
    fn legacy_query_is_normalized_to_query_execute() {
        let payload = rmp_serde::to_vec_named(&QueryRequest::new(9, "on users | limit 1")).unwrap();
        let request = decode_operation_request(&payload).unwrap();
        assert_eq!(request, OperationRequest::query(9, "on users | limit 1"));
    }

    #[test]
    fn generic_query_operation_is_decoded() {
        let payload = rmp_serde::to_vec_named(&OperationRequest::query(4, "on users")).unwrap();
        let request = decode_operation_request(&payload).unwrap();
        assert_eq!(request, OperationRequest::query(4, "on users"));
    }

    #[test]
    fn numeric_string_request_id_is_accepted() {
        #[derive(Serialize)]
        struct WireRequest<'a> {
            version: u16,
            id: &'a str,
            op: &'a str,
            data: Value,
        }
        let payload = rmp_serde::to_vec_named(&WireRequest {
            version: PROTOCOL_VERSION,
            id: "42",
            op: QUERY_EXECUTE,
            data: json!({"query": "on users"}),
        })
        .unwrap();
        let request = decode_operation_request(&payload).unwrap();
        assert_eq!(request.id, RequestId::string("42").unwrap());
    }

    #[test]
    fn arbitrary_string_request_id_is_preserved() {
        #[derive(Serialize)]
        struct WireRequest<'a> {
            version: u16,
            id: &'a str,
            op: &'a str,
            data: Value,
        }
        let payload = rmp_serde::to_vec_named(&WireRequest {
            version: PROTOCOL_VERSION,
            id: "1785677694881-1",
            op: AUTH_BEGIN,
            data: json!({"identityId": "identity-a", "deviceId": "device-a"}),
        })
        .unwrap();
        let request = decode_operation_request(&payload).unwrap();
        assert_eq!(request.id.as_str(), Some("1785677694881-1"));
    }

    #[test]
    fn empty_operation_name_is_rejected() {
        let payload = rmp_serde::to_vec_named(&OperationRequest::new(4, " ", json!({}))).unwrap();
        let error = decode_operation_request(&payload).unwrap_err();
        assert!(matches!(error, ProtocolError::EmptyOperation));
    }
}
