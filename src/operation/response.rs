//! Generic non-streaming operation response.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::{RequestId, PROTOCOL_VERSION};

/// Generic response for future non-query operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationResponse {
    pub kind: String,
    pub version: u16,
    pub id: RequestId,
    pub status: String,
    pub data: Value,
}

impl OperationResponse {
    #[must_use]
    pub fn new(id: impl Into<RequestId>, data: Value) -> Self {
        Self {
            kind: "response".to_owned(),
            version: PROTOCOL_VERSION,
            id: id.into(),
            status: "ok".to_owned(),
            data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_response_uses_the_common_response_kind() {
        let response = OperationResponse::new(1, serde_json::json!({"ok": true}));
        let bytes = rmp_serde::to_vec_named(&response).unwrap();
        let value: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(value["kind"], "response");
        assert_eq!(value["status"], "ok");
        assert_eq!(value["id"], 1);
    }
}
