//! Compact event values and audiences.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::PROTOCOL_VERSION;

/// Logical recipients selected by the core.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Audience {
    Global,
    Identity { identity_id: String },
    Identities { identity_ids: Vec<String> },
}

impl Audience {
    #[must_use]
    pub const fn global() -> Self {
        Self::Global
    }

    #[must_use]
    pub fn identities(identity_ids: impl IntoIterator<Item = String>) -> Self {
        let mut identity_ids: Vec<_> = identity_ids
            .into_iter()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .collect();
        identity_ids.sort();
        identity_ids.dedup();
        Self::Identities { identity_ids }
    }
}

/// One event emitted by the core.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub v: u16,
    pub kind: String,
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub audience: Audience,
    #[serde(serialize_with = "crate::protocol::serialize_js_safe_u64")]
    pub ts: u64,
    #[serde(default)]
    pub payload: Value,
}

impl Event {
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        event_type: impl Into<String>,
        audience: Audience,
        ts: u64,
        payload: Value,
    ) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            kind: "event".to_owned(),
            id: id.into(),
            event_type: event_type.into(),
            audience,
            ts,
            payload,
        }
    }

    #[must_use]
    pub fn global(
        id: impl Into<String>,
        event_type: impl Into<String>,
        ts: u64,
        payload: Value,
    ) -> Self {
        Self::new(id, event_type, Audience::Global, ts, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_wire_shape_is_compact_and_versioned() {
        let event = Event::global("event-1", "core.started", 42, json!({ "ok": true }));
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["v"], 1);
        assert_eq!(value["kind"], "event");
        assert_eq!(value["type"], "core.started");
        assert_eq!(value["audience"]["type"], "global");
    }

    #[test]
    fn event_timestamp_is_encoded_as_a_javascript_number() {
        let event = Event::global("event-1", "core.heartbeat", 1_785_680_802_608, json!({}));
        let encoded = rmp_serde::to_vec_named(&event).unwrap();
        let decoded: serde_json::Value = rmp_serde::from_slice(&encoded).unwrap();
        assert!(decoded["ts"].is_f64());
        assert_eq!(decoded["ts"].as_f64(), Some(1_785_680_802_608.0));
    }

    #[test]
    fn identities_audience_is_sorted_and_deduplicated() {
        let audience = Audience::identities([
            "identity-b".to_owned(),
            "identity-a".to_owned(),
            "identity-b".to_owned(),
        ]);
        assert_eq!(
            audience,
            Audience::Identities {
                identity_ids: vec!["identity-a".to_owned(), "identity-b".to_owned()],
            }
        );
    }
}
