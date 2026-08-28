//! Canonical built-in operation catalogue.
//!
//! An operation name, its stable kind and its coarse access contract are declared
//! exactly once here. Routing, authorization preflight, documentation and future
//! handler dispatch should consume this catalogue instead of maintaining mirrors.

use crate::access::authorization::AuthorizationAction;

use super::definition::operation_definitions;

/// Runtime service capability exposed by one ogd node.
///
/// These capabilities gate whole operation families. They are distinct from
/// value/model capabilities (`Comparable`, `Temporal`, ...).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ServiceCapability {
    Auth,
    Database,
    Files,
    Events,
    DataImport,
}

impl ServiceCapability {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::Database => "database",
            Self::Files => "files",
            Self::Events => "events",
            Self::DataImport => "data.import",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auth" => Some(Self::Auth),
            "database" => Some(Self::Database),
            "files" => Some(Self::Files),
            "events" => Some(Self::Events),
            "data.import" | "data-import" | "data_import" => Some(Self::DataImport),
            _ => None,
        }
    }
}

/// Compact set of service capabilities enabled for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ServiceCapabilities {
    bits: u8,
}

impl ServiceCapabilities {
    const AUTH: u8 = 1 << 0;
    const DATABASE: u8 = 1 << 1;
    const FILES: u8 = 1 << 2;
    const EVENTS: u8 = 1 << 3;
    const DATA_IMPORT: u8 = 1 << 4;

    pub const NONE: Self = Self { bits: 0 };
    pub const ALL: Self = Self {
        bits: Self::AUTH | Self::DATABASE | Self::FILES | Self::EVENTS | Self::DATA_IMPORT,
    };

    #[must_use]
    pub const fn contains(self, capability: ServiceCapability) -> bool {
        self.bits & service_capability_bit(capability) != 0
    }

    #[must_use]
    pub const fn contains_all(self, required: Self) -> bool {
        self.bits & required.bits == required.bits
    }

    #[must_use]
    pub const fn with(self, capability: ServiceCapability) -> Self {
        Self {
            bits: self.bits | service_capability_bit(capability),
        }
    }

    #[must_use]
    pub fn from_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<Self, String> {
        let mut result = Self::NONE;
        for name in names {
            let capability = ServiceCapability::parse(name)
                .ok_or_else(|| format!("unknown ogd service capability {name:?}"))?;
            result = result.with(capability);
        }
        Ok(result)
    }

    #[must_use]
    pub fn names(self) -> Vec<&'static str> {
        [
            ServiceCapability::Auth,
            ServiceCapability::Database,
            ServiceCapability::Files,
            ServiceCapability::Events,
            ServiceCapability::DataImport,
        ]
        .into_iter()
        .filter(|capability| self.contains(*capability))
        .map(ServiceCapability::as_str)
        .collect()
    }
}

const fn service_capability_bit(capability: ServiceCapability) -> u8 {
    match capability {
        ServiceCapability::Auth => ServiceCapabilities::AUTH,
        ServiceCapability::Database => ServiceCapabilities::DATABASE,
        ServiceCapability::Files => ServiceCapabilities::FILES,
        ServiceCapability::Events => ServiceCapabilities::EVENTS,
        ServiceCapability::DataImport => ServiceCapabilities::DATA_IMPORT,
    }
}

const AUTH: ServiceCapabilities = ServiceCapabilities {
    bits: ServiceCapabilities::AUTH,
};
const DATABASE: ServiceCapabilities = ServiceCapabilities {
    bits: ServiceCapabilities::DATABASE,
};
const FILES: ServiceCapabilities = ServiceCapabilities {
    bits: ServiceCapabilities::FILES,
};
const EVENTS: ServiceCapabilities = ServiceCapabilities {
    bits: ServiceCapabilities::EVENTS,
};
const AUTH_DATABASE: ServiceCapabilities = ServiceCapabilities {
    bits: ServiceCapabilities::AUTH | ServiceCapabilities::DATABASE,
};
const CAP_DATA_IMPORT: ServiceCapabilities = ServiceCapabilities {
    bits: ServiceCapabilities::DATA_IMPORT,
};

/// Transport/execution family for one operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Standard,
    Query,
    Authentication,
    Subscription,
    File,
}

/// Canonical handler domain for one operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerKind {
    Core,
    Query,
    Authentication,
    Subscription,
    File,
    Collections,
    Storage,
    Backup,
    Identity,
    Device,
    Permission,
    Sharing,
    Place,
    App,
}

/// Coarse authorization contract attached to one operation.
///
/// Dynamic/domain-specific policies keep resource extraction in the typed
/// authorization path; every operation still has an explicit coarse policy here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessPolicy {
    Public,
    Authenticated,
    Query,
    Permission {
        action: AuthorizationAction,
        resource: &'static str,
    },
    DynamicPermission(AuthorizationAction),
}

/// Static metadata for one built-in operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationDescriptor {
    pub kind: OperationKind,
    pub name: &'static str,
    pub access: AccessPolicy,
    pub execution: ExecutionMode,
    pub handler: HandlerKind,
}

macro_rules! define_operations {
    ($($constant:ident => $kind:ident, $name:literal, $access:expr, $execution:expr, $handler:expr, $payload:ty;)+) => {
        $(pub const $constant: &str = $name;)+

        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum OperationKind { $($kind,)+ }

        impl OperationKind {
            pub const ALL: &'static [Self] = &[$(Self::$kind,)+];

            #[must_use]
            pub const fn descriptor(self) -> &'static OperationDescriptor {
                match self { $(Self::$kind => &$kind,)+ }
            }

            #[must_use]
            pub const fn name(self) -> &'static str { self.descriptor().name }

            #[must_use]
            pub const fn access(self) -> AccessPolicy { self.descriptor().access }

            #[must_use]
            pub const fn execution_mode(self) -> ExecutionMode { self.descriptor().execution }

            #[must_use]
            pub const fn handler(self) -> HandlerKind { self.descriptor().handler }

            /// Service capabilities required before this operation can be routed.
            #[must_use]
            pub const fn required_capabilities(self) -> ServiceCapabilities {
                match self {
                    Self::CoreHealth | Self::Ping => ServiceCapabilities::NONE,
                    // data.analyze/data.import are control-plane operations: authorization,
                    // mappings and destination writes live on a database provider. The actual
                    // Python execution is isolated behind data.worker.run on a data.import node.
                    Self::DataAnalyze | Self::DataImport | Self::DataMappingSave => DATABASE,
                    Self::DataWorkerRun => CAP_DATA_IMPORT,
                    _ => match self.handler() {
                        HandlerKind::Core => ServiceCapabilities::NONE,
                        HandlerKind::Query => DATABASE,
                        HandlerKind::Authentication => AUTH,
                        HandlerKind::Subscription => EVENTS,
                        HandlerKind::File => FILES,
                        HandlerKind::Collections | HandlerKind::Storage | HandlerKind::Backup => DATABASE,
                        HandlerKind::Identity | HandlerKind::Device => AUTH_DATABASE,
                        HandlerKind::Permission | HandlerKind::Sharing | HandlerKind::Place | HandlerKind::App => DATABASE,
                    },
                }
            }
        }

        $(#[allow(non_upper_case_globals)]
        const $kind: OperationDescriptor = OperationDescriptor {
            kind: OperationKind::$kind, name: $name, access: $access, execution: $execution, handler: $handler,
        };)+

        pub const OPERATION_CATALOG: &[OperationDescriptor] = &[$($kind,)+];

        #[must_use]
        pub fn operation_by_name(name: &str) -> Option<&'static OperationDescriptor> {
            OPERATION_CATALOG.iter().find(|operation| operation.name == name)
        }
    };
}

operation_definitions!(define_operations);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn operation_names_are_unique_and_non_empty() {
        let mut names = BTreeSet::new();
        for operation in OPERATION_CATALOG {
            assert!(!operation.name.is_empty());
            assert!(
                names.insert(operation.name),
                "duplicate operation {}",
                operation.name
            );
        }
        assert_eq!(names.len(), OperationKind::ALL.len());
    }

    #[test]
    fn every_kind_round_trips_through_the_catalogue() {
        for &kind in OperationKind::ALL {
            let descriptor = kind.descriptor();
            assert_eq!(
                operation_by_name(descriptor.name).map(|entry| entry.kind),
                Some(kind)
            );
        }
    }
    #[test]
    fn operation_capabilities_follow_handler_domains() {
        assert_eq!(
            OperationKind::QueryExecute.required_capabilities(),
            DATABASE
        );
        assert_eq!(OperationKind::FileRead.required_capabilities(), FILES);
        assert_eq!(
            OperationKind::EventsSubscribe.required_capabilities(),
            EVENTS
        );
        assert_eq!(OperationKind::AuthBegin.required_capabilities(), AUTH);
        assert_eq!(
            OperationKind::DeviceList.required_capabilities(),
            AUTH_DATABASE
        );
        assert_eq!(OperationKind::PlaceList.required_capabilities(), DATABASE);
        assert_eq!(OperationKind::DataImport.required_capabilities(), DATABASE);
        assert_eq!(
            OperationKind::DataWorkerRun.required_capabilities(),
            CAP_DATA_IMPORT
        );
        assert_eq!(
            OperationKind::CoreHealth.required_capabilities(),
            ServiceCapabilities::NONE
        );
    }

    #[test]
    fn execution_modes_match_handler_domains() {
        for operation in OPERATION_CATALOG {
            let compatible = match operation.execution {
                ExecutionMode::Query => operation.handler == HandlerKind::Query,
                ExecutionMode::Authentication => operation.handler == HandlerKind::Authentication,
                ExecutionMode::Subscription => operation.handler == HandlerKind::Subscription,
                ExecutionMode::File => operation.handler == HandlerKind::File,
                ExecutionMode::Standard => !matches!(
                    operation.handler,
                    HandlerKind::Query
                        | HandlerKind::Authentication
                        | HandlerKind::Subscription
                        | HandlerKind::File
                ),
            };
            assert!(
                compatible,
                "incompatible execution/handler for {}",
                operation.name
            );
        }
    }
}
