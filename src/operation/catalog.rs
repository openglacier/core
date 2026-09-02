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

/// Wire payload shape declared by Core.
///
/// Gateway and other transports must select their forwarding primitive from
/// this value rather than special-casing operation names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    /// One framed request followed by one framed response.
    Message,
    /// One framed request followed by zero or more correlated framed responses.
    MessageStream,
    /// Framed request/header followed by a binary payload sent to Core.
    BinaryIn,
    /// Framed request/header followed by a binary payload emitted by Core.
    BinaryOut,
}

impl TransportKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::MessageStream => "message-stream",
            Self::BinaryIn => "binary-in",
            Self::BinaryOut => "binary-out",
        }
    }
}

/// Channel-use constraint declared by Core.
///
/// Core expresses the correctness constraint; Gateway remains free to realize
/// it using a direct channel, a pool, or multiplexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionKind {
    Shared,
    Exclusive,
    Persistent,
}

impl ConnectionKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Exclusive => "exclusive",
            Self::Persistent => "persistent",
        }
    }
}

/// Where the authoritative state for an operation lives.
///
/// `Authority` operations execute locally on a standalone/master Core and are
/// forwarded to the configured upstream authority when this Core is a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationScope {
    Local,
    Authority,
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
    pub transport: TransportKind,
    pub connection: ConnectionKind,
}

macro_rules! define_operations {
    ($($constant:ident => $kind:ident, $name:literal, $access:expr, $execution:expr, $handler:expr, $transport:expr, $connection:expr, $payload:ty;)+) => {
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

            #[must_use]
            pub const fn transport(self) -> TransportKind { self.descriptor().transport }

            #[must_use]
            pub const fn connection(self) -> ConnectionKind { self.descriptor().connection }

            /// Returns where the authoritative state for this operation lives.
            ///
            /// Keep this deliberately narrow: operations move here only when their
            /// trust/authority semantics are owned by Core rather than by a UI or
            /// Gateway adapter.
            #[must_use]
            pub const fn scope(self) -> OperationScope {
                match self {
                    Self::PlaceList
                    | Self::PlaceGet
                    | Self::PlaceAccessList
                    | Self::PlaceResourceList
                    | Self::PlaceResourceSet
                    | Self::PlaceResourceRemove
                    | Self::AppList
                    | Self::AppInstanceList => OperationScope::Authority,
                    _ => OperationScope::Local,
                }
            }

            /// Service capabilities required before this operation can be routed.
            #[must_use]
            pub const fn required_capabilities(self) -> ServiceCapabilities {
                if matches!(self.scope(), OperationScope::Authority) {
                    return ServiceCapabilities::NONE;
                }
                match self {
                    Self::CoreHealth | Self::Ping => ServiceCapabilities::NONE,
                    // data.analyze/data.import are control-plane operations: authorization,
                    // mappings and destination writes live on a database provider. The actual
                    // Python execution is isolated behind data.worker.run on a data.import node.
                    Self::DataAnalyze | Self::DataImport | Self::DataMappingSave | Self::DataMappingList | Self::DataMappingUpdate | Self::DataMappingDelete => DATABASE,
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
            kind: OperationKind::$kind,
            name: $name,
            access: $access,
            execution: $execution,
            handler: $handler,
            transport: $transport,
            connection: $connection,
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
        assert_eq!(
            OperationKind::PlaceList.required_capabilities(),
            ServiceCapabilities::NONE
        );
        assert_eq!(OperationKind::DataImport.required_capabilities(), DATABASE);
        assert_eq!(
            OperationKind::DataMappingList.required_capabilities(),
            DATABASE
        );
        assert_eq!(
            OperationKind::DataMappingUpdate.required_capabilities(),
            DATABASE
        );
        assert_eq!(
            OperationKind::DataMappingDelete.required_capabilities(),
            DATABASE
        );
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
    fn authority_scope_is_explicit_and_narrow() {
        assert_eq!(OperationKind::PlaceList.scope(), OperationScope::Authority);
        assert_eq!(
            OperationKind::PlaceResourceList.scope(),
            OperationScope::Authority
        );
        assert_eq!(
            OperationKind::AppInstanceList.scope(),
            OperationScope::Authority
        );
        assert_eq!(OperationKind::Ping.scope(), OperationScope::Local);
    }

    #[test]
    fn transport_contract_is_independent_from_handler_domain() {
        assert_eq!(OperationKind::FileSyncRun.handler(), HandlerKind::File);
        assert_eq!(
            OperationKind::FileSyncRun.transport(),
            TransportKind::Message
        );
        assert_eq!(OperationKind::FileRead.handler(), HandlerKind::File);
        assert_eq!(
            OperationKind::FileRead.transport(),
            TransportKind::BinaryOut
        );
        assert_eq!(
            OperationKind::FileWrite.transport(),
            TransportKind::BinaryIn
        );
        assert_eq!(
            OperationKind::FileVersionRead.transport(),
            TransportKind::BinaryOut
        );
        assert_eq!(
            OperationKind::DataWorkerRun.transport(),
            TransportKind::BinaryIn
        );
        assert_eq!(
            OperationKind::QueryExecute.transport(),
            TransportKind::MessageStream
        );
    }

    #[test]
    fn binary_transports_require_exclusive_channels() {
        for operation in OPERATION_CATALOG {
            if matches!(
                operation.transport,
                TransportKind::BinaryIn | TransportKind::BinaryOut
            ) {
                assert_eq!(
                    operation.connection,
                    ConnectionKind::Exclusive,
                    "binary operation {} must use an exclusive channel",
                    operation.name
                );
            }
        }
    }
}
