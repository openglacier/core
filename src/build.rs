#![cfg_attr(rustfmt, rustfmt_skip)]
use crate::operation::ServiceCapabilities;

#[must_use]
pub const fn service_capabilities() -> ServiceCapabilities {
    ServiceCapabilities::compiled()
}

pub const FEATURE_NAMES: &[&str] = &[
    #[cfg(feature = "db-engine")]
    "db-engine",
    #[cfg(feature = "database")]
    "database",
    #[cfg(feature = "auth")]
    "auth",
    #[cfg(feature = "files")]
    "files",
    #[cfg(feature = "events")]
    "events",
    #[cfg(feature = "data-import")]
    "data-import",
    #[cfg(feature = "llm")]
    "llm",
    #[cfg(feature = "agent")]
    "agent",
    #[cfg(feature = "identity")]
    "identity",
    #[cfg(feature = "fabric")]
    "fabric",
    #[cfg(feature = "files-watch")]
    "files-watch",
    #[cfg(feature = "cli")]
    "cli",
    #[cfg(feature = "cli-tui")]
    "cli-tui",
];

#[must_use]
pub const fn feature_names() -> &'static [&'static str] {
    FEATURE_NAMES
}

#[must_use]
pub const fn has_db_engine() -> bool {
    cfg!(feature = "db-engine")
}

#[must_use]
pub const fn has_fabric() -> bool {
    cfg!(feature = "fabric")
}

#[must_use]
pub const fn has_identity() -> bool {
    cfg!(feature = "identity")
}

#[must_use]
pub const fn has_files_watch() -> bool {
    cfg!(feature = "files-watch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::ServiceCapability;
    #[test] fn compiled_service_capabilities_match_features() { let capabilities = service_capabilities(); assert_eq!( capabilities.contains(ServiceCapability::Database), cfg!(feature = "database"), ); assert_eq!( capabilities.contains(ServiceCapability::Auth), cfg!(feature = "auth"), ); assert_eq!( capabilities.contains(ServiceCapability::Files), cfg!(feature = "files"), ); assert_eq!( capabilities.contains(ServiceCapability::Events), cfg!(feature = "events"), ); assert_eq!( capabilities.contains(ServiceCapability::DataImport), cfg!(feature = "data-import"), ); assert_eq!( capabilities.contains(ServiceCapability::Llm), cfg!(feature = "llm"), ); assert_eq!( capabilities.contains(ServiceCapability::Agent), cfg!(feature = "agent"), ); }
    #[test] fn feature_names_do_not_duplicate_entries() { let names = feature_names(); for (index, name) in names.iter().enumerate() { assert!( !names[..index].contains(name), "duplicate feature name: {name}" ); } }
}
