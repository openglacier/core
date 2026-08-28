//! Lightweight developer diagnostics controlled by `OGD_DEBUG`.

use std::{
    collections::BTreeSet,
    env,
    fmt::{self, Display, Formatter},
    sync::OnceLock,
    time::Instant,
};

/// Stable debug categories understood by `OGD_DEBUG`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DebugTopic {
    Core,
    Network,
    Protocol,
    Router,
    Query,
    Planner,
    Executor,
    Storage,
    Memory,
    Events,
    Auth,
    Identity,
    Device,
    Sharing,
    Permission,
    Gateway,
}

impl DebugTopic {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::Network => "network",
            Self::Protocol => "protocol",
            Self::Router => "router",
            Self::Query => "query",
            Self::Planner => "planner",
            Self::Executor => "executor",
            Self::Storage => "storage",
            Self::Memory => "memory",
            Self::Events => "events",
            Self::Auth => "auth",
            Self::Identity => "identity",
            Self::Device => "device",
            Self::Sharing => "sharing",
            Self::Permission => "permission",
            Self::Gateway => "gateway",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "core" => Some(Self::Core),
            "network" => Some(Self::Network),
            "protocol" => Some(Self::Protocol),
            "router" => Some(Self::Router),
            "query" => Some(Self::Query),
            "planner" => Some(Self::Planner),
            "executor" => Some(Self::Executor),
            "storage" => Some(Self::Storage),
            "memory" => Some(Self::Memory),
            "events" | "event" => Some(Self::Events),
            "auth" => Some(Self::Auth),
            "identity" => Some(Self::Identity),
            "device" => Some(Self::Device),
            "sharing" => Some(Self::Sharing),
            "permission" | "permissions" => Some(Self::Permission),
            "gateway" => Some(Self::Gateway),
            _ => None,
        }
    }
}

impl Display for DebugTopic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug)]
struct DebugConfig {
    started: Instant,
    all: bool,
    topics: BTreeSet<DebugTopic>,
    protocol: bool,
    timing: bool,
    memory: bool,
    query_instrumentation: bool,
}

impl DebugConfig {
    fn from_environment() -> Self {
        let raw = env::var("OGD_DEBUG").unwrap_or_default();
        let normalized = raw.trim().to_ascii_lowercase();
        let all = matches!(normalized.as_str(), "1" | "true" | "yes" | "on" | "*");
        let topics = if all || normalized.is_empty() {
            BTreeSet::new()
        } else {
            normalized
                .split(',')
                .filter_map(DebugTopic::parse)
                .collect()
        };
        Self {
            started: Instant::now(),
            all,
            topics,
            protocol: env_flag("OGD_DEBUG_PROTOCOL")
                || all
                || normalized.split(',').any(|v| v.trim() == "protocol"),
            timing: env_flag("OGD_DEBUG_TIMING"),
            memory: env_flag("OGD_DEBUG_MEMORY")
                || normalized.split(',').any(|v| v.trim() == "memory"),
            query_instrumentation: env_flag("OGD_DEBUG_QUERY"),
        }
    }

    fn enabled(&self, topic: DebugTopic) -> bool {
        self.all || self.topics.contains(&topic)
    }
}

static CONFIG: OnceLock<DebugConfig> = OnceLock::new();

fn config() -> &'static DebugConfig {
    CONFIG.get_or_init(DebugConfig::from_environment)
}

fn env_flag(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

#[must_use]
pub fn enabled(topic: DebugTopic) -> bool {
    config().enabled(topic)
}

#[must_use]
pub fn protocol_enabled() -> bool {
    config().protocol
}

#[must_use]
pub fn timing_enabled() -> bool {
    config().timing
}

#[must_use]
pub fn memory_enabled() -> bool {
    config().memory
}

/// Enables expensive per-query profiling. Disabled by default so production
/// scans keep only low-cost aggregate counters.
#[must_use]
pub fn query_instrumentation_enabled() -> bool {
    config().query_instrumentation
}

pub fn log(topic: DebugTopic, connection_id: Option<u64>, message: impl AsRef<str>) {
    if !enabled(topic) {
        return;
    }
    let elapsed = config().started.elapsed().as_millis();
    match connection_id {
        Some(connection_id) => eprintln!(
            "+{elapsed:06}ms [conn:{connection_id}] [{topic}] {}",
            message.as_ref()
        ),
        None => eprintln!("+{elapsed:06}ms [{topic}] {}", message.as_ref()),
    }
}

/// Redacts values commonly carrying secrets before protocol logging.
#[must_use]
pub fn redact_json(mut value: serde_json::Value) -> serde_json::Value {
    redact_value(&mut value);
    value
}

fn redact_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                if is_secret_key(key) {
                    *value = serde_json::Value::String("<redacted>".to_owned());
                } else {
                    redact_value(value);
                }
            }
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(redact_value),
        _ => {}
    }
}

fn is_secret_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "privatekey"
            | "private_key"
            | "password"
            | "signature"
            | "challenge"
            | "token"
            | "enrollmenttoken"
            | "enrollment_token"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topics_have_stable_names() {
        assert_eq!(DebugTopic::Auth.as_str(), "auth");
        assert_eq!(DebugTopic::Events.as_str(), "events");
    }

    #[test]
    fn redaction_hides_nested_secrets() {
        let value = serde_json::json!({
            "data": {"signature": "secret", "identityId": "alice"},
            "token": "bootstrap"
        });
        let redacted = redact_json(value);
        assert_eq!(redacted["data"]["signature"], "<redacted>");
        assert_eq!(redacted["token"], "<redacted>");
        assert_eq!(redacted["data"]["identityId"], "alice");
    }
}
