//! Canonical built-in operation catalogue.
//!
//! An operation name, its stable kind and its coarse access contract are declared
//! exactly once here. Routing, authorization preflight, documentation and future
//! handler dispatch should consume this catalogue instead of maintaining mirrors.
#![cfg_attr(rustfmt, rustfmt_skip)]
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
    Llm,
    Agent,
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
            Self::Llm => "llm",
            Self::Agent => "agent",
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
            "llm" => Some(Self::Llm),
            "agent" => Some(Self::Agent),
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
    const LLM: u8 = 1 << 5;
    const AGENT: u8 = 1 << 6;

    pub const NONE: Self = Self { bits: 0 };
    /// All capability kinds known by this Core version, regardless of the current build.
    pub const ALL: Self = Self {
        bits: Self::AUTH
            | Self::DATABASE
            | Self::FILES
            | Self::EVENTS
            | Self::DATA_IMPORT
            | Self::LLM
            | Self::AGENT,
    };

    /// Capability implementations compiled into this build.
    ///
    /// This is deliberately separate from the runtime-enabled capability set:
    /// Cargo features decide what code exists; `OGD_NODE_CAPABILITIES` decides which
    /// compiled services are enabled by one daemon instance.
    #[must_use]
    pub const fn compiled() -> Self {
        let mut result = Self::NONE;
        if cfg!(feature = "auth") {
            result = result.with(ServiceCapability::Auth);
        }
        if cfg!(feature = "database") {
            result = result.with(ServiceCapability::Database);
        }
        if cfg!(feature = "files") {
            result = result.with(ServiceCapability::Files);
        }
        if cfg!(feature = "events") {
            result = result.with(ServiceCapability::Events);
        }
        if cfg!(feature = "data-import") {
            result = result.with(ServiceCapability::DataImport);
        }
        if cfg!(feature = "llm") {
            result = result.with(ServiceCapability::Llm);
        }
        if cfg!(feature = "agent") {
            result = result.with(ServiceCapability::Agent);
        }
        result
    }

    /// Runtime defaults for this build.
    ///
    /// Optional extension services stay opt-in in an all-in-one build:
    /// `data.import`, `llm` and `agent` may be compiled by `full` without being
    /// published/enabled unless explicitly requested. A dedicated extension-only
    /// build enables its compiled services so it is useful without extra config.
    #[must_use]
    pub const fn default_enabled() -> Self {
        let compiled = Self::compiled();
        let mut result = Self::NONE;
        if compiled.contains(ServiceCapability::Auth) {
            result = result.with(ServiceCapability::Auth);
        }
        if compiled.contains(ServiceCapability::Database) {
            result = result.with(ServiceCapability::Database);
        }
        if compiled.contains(ServiceCapability::Files) {
            result = result.with(ServiceCapability::Files);
        }
        if compiled.contains(ServiceCapability::Events) {
            result = result.with(ServiceCapability::Events);
        }
        // A dedicated worker build should be useful without an extra environment variable,
        // while the historical full build keeps data.import opt-in.
        if result.is_empty() {
            if compiled.contains(ServiceCapability::DataImport) {
                result = result.with(ServiceCapability::DataImport);
            }
            if compiled.contains(ServiceCapability::Llm) {
                result = result.with(ServiceCapability::Llm);
            }
            if compiled.contains(ServiceCapability::Agent) {
                result = result.with(ServiceCapability::Agent);
            }
        }
        result
    }

    #[must_use]
    pub const fn contains(self, capability: ServiceCapability) -> bool {
        self.bits & service_capability_bit(capability) != 0
    }

    #[must_use]
    pub const fn contains_all(self, required: Self) -> bool {
        self.bits & required.bits == required.bits
    }

    /// Returns the subset present in `self` but absent from `available`.
    #[must_use]
    pub const fn missing_from(self, available: Self) -> Self {
        Self {
            bits: self.bits & !available.bits,
        }
    }

    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.bits == 0
    }

    #[must_use]
    pub const fn with(self, capability: ServiceCapability) -> Self {
        Self {
            bits: self.bits | service_capability_bit(capability),
        }
    }

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
            ServiceCapability::Llm,
            ServiceCapability::Agent,
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
        ServiceCapability::Llm => ServiceCapabilities::LLM,
        ServiceCapability::Agent => ServiceCapabilities::AGENT,
    }
}

const AUTH: ServiceCapabilities = ServiceCapabilities { bits: ServiceCapabilities::AUTH, };
const DATABASE: ServiceCapabilities = ServiceCapabilities { bits: ServiceCapabilities::DATABASE, };
const FILES: ServiceCapabilities = ServiceCapabilities { bits: ServiceCapabilities::FILES, };
const EVENTS: ServiceCapabilities = ServiceCapabilities { bits: ServiceCapabilities::EVENTS, };
const CAP_DATA_IMPORT: ServiceCapabilities = ServiceCapabilities { bits: ServiceCapabilities::DATA_IMPORT, };
const LLM: ServiceCapabilities = ServiceCapabilities { bits: ServiceCapabilities::LLM, };
const AGENT: ServiceCapabilities = ServiceCapabilities { bits: ServiceCapabilities::AGENT, };

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
/// `Authority` operations execute locally on an authority Core and are
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
    Llm,
    Agent,
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

impl AccessPolicy {
    /// Stable wire name, advertised with the operation contracts.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Authenticated => "authenticated",
            Self::Query => "query",
            Self::Permission { .. } => "permission",
            Self::DynamicPermission(_) => "dynamic_permission",
        }
    }
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
                    Self::QueryContextResolve
                    | Self::PlaceList
                    | Self::PlaceGet
                    | Self::PlaceAccessList
                    | Self::PlaceResourceList
                    | Self::PlaceResourceSet
                    | Self::PlaceResourceRemove
                    | Self::FabricResourceList
                    | Self::FabricResourceSet
                    | Self::FabricResourceRemove
                    | Self::AppList
                    | Self::AppInstanceList => OperationScope::Authority,
                    _ => OperationScope::Local,
                }
            }

            /// Public service capability that provides this operation on the fabric.
            ///
            /// This is not necessarily the complete set of local implementation dependencies.
            /// For example, Identity/Device operations are provided by `auth` while currently
            /// relying on database state internally.
            #[must_use]
            pub const fn provider_capability(self) -> Option<ServiceCapability> {
                match self {
                    Self::CoreHealth | Self::CoreOperations | Self::CoreDiscover | Self::NodeStatus | Self::Ping => None,
                    Self::FabricResourceList | Self::FabricResourceSet | Self::FabricResourceRemove => Some(ServiceCapability::Auth),
                    Self::DataWorkerRun => Some(ServiceCapability::DataImport),
                    Self::DataAnalyze | Self::DataImport | Self::DataMappingSave | Self::DataMappingList | Self::DataMappingUpdate | Self::DataMappingDelete => Some(ServiceCapability::Database),
                    _ => match self.handler() {
                        HandlerKind::Core => None,
                        HandlerKind::Query | HandlerKind::Collections | HandlerKind::Storage | HandlerKind::Backup
                        | HandlerKind::Permission | HandlerKind::Sharing | HandlerKind::Place | HandlerKind::App => Some(ServiceCapability::Database),
                        HandlerKind::Authentication | HandlerKind::Identity | HandlerKind::Device => Some(ServiceCapability::Auth),
                        HandlerKind::Subscription => Some(ServiceCapability::Events),
                        HandlerKind::File => Some(ServiceCapability::Files),
                        HandlerKind::Llm => Some(ServiceCapability::Llm),
                        HandlerKind::Agent => Some(ServiceCapability::Agent),
                    },
                }
            }

            /// Whether this operation should be advertised for the supplied published services.
            #[must_use]
            pub const fn is_published_by(self, published: ServiceCapabilities) -> bool {
                match self.provider_capability() {
                    Some(capability) => published.contains(capability),
                    None => true,
                }
            }

            /// Service implementation families needed for this operation to exist in a build.
            ///
            /// Unlike `required_capabilities`, this deliberately ignores Authority forwarding:
            /// a node may forward an Authority operation without enabling the provider service at
            /// runtime, but the operation still belongs to one compiled implementation family.
            #[must_use]
            pub const fn implementation_capabilities(self) -> ServiceCapabilities {
                match self {
                    Self::CoreHealth | Self::CoreOperations | Self::CoreDiscover | Self::NodeStatus | Self::Ping => ServiceCapabilities::NONE,
                    Self::FabricResourceList | Self::FabricResourceSet | Self::FabricResourceRemove => AUTH,
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
                        HandlerKind::Identity | HandlerKind::Device => AUTH,
                        HandlerKind::Permission | HandlerKind::Sharing | HandlerKind::Place | HandlerKind::App => DATABASE,
                        HandlerKind::Llm => LLM,
                        HandlerKind::Agent => AGENT,
                    },
                }
            }

            /// Whether this operation needs the internal database engine implementation.
            ///
            /// This is intentionally not the same thing as requiring or publishing the public
            /// `database` service capability. `auth`, `files` and `events` currently use the
            /// local database kernel for state while remaining independent providers.
            #[must_use]
            pub const fn requires_db_engine(self) -> bool {
                matches!(
                    self.provider_capability(),
                    Some(ServiceCapability::Database | ServiceCapability::Auth |
ServiceCapability::Files | ServiceCapability::Events)
                )
            }

            /// Whether this operation has an implementation in the current Cargo build.
            #[must_use]
            pub const fn is_compiled(self) -> bool {
                let services_present = ServiceCapabilities::compiled()
                    .contains_all(self.implementation_capabilities());
                services_present && (!self.requires_db_engine() || cfg!(feature = "db-engine"))
            }

            /// Service capabilities required before this operation can be routed locally.
            #[must_use]
            pub const fn required_capabilities(self) -> ServiceCapabilities {
                if matches!(self.scope(), OperationScope::Authority) {
                    ServiceCapabilities::NONE
                } else {
                    self.implementation_capabilities()
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

    #[test] fn compiled_capabilities_follow_cargo_features() { let compiled = ServiceCapabilities::compiled(); assert_eq!( compiled.contains(ServiceCapability::Database), cfg!(feature = "database") ); assert_eq!( compiled.contains(ServiceCapability::Auth), cfg!(feature = "auth") ); assert_eq!( compiled.contains(ServiceCapability::Files), cfg!(feature = "files") ); assert_eq!( compiled.contains(ServiceCapability::Events), cfg!(feature = "events") ); assert_eq!( compiled.contains(ServiceCapability::DataImport), cfg!(feature = "data-import") ); assert_eq!( compiled.contains(ServiceCapability::Llm), cfg!(feature = "llm") ); assert_eq!( compiled.contains(ServiceCapability::Agent), cfg!(feature = "agent") ); }
    #[test] fn runtime_defaults_are_a_subset_of_the_build() { let compiled = ServiceCapabilities::compiled(); let defaults = ServiceCapabilities::default_enabled(); assert!(compiled.contains_all(defaults)); let has_historical_default = [ ServiceCapability::Auth, ServiceCapability::Database, ServiceCapability::Files, ServiceCapability::Events, ] .into_iter() .any(|capability| compiled.contains(capability)); if has_historical_default { assert!(!defaults.contains(ServiceCapability::DataImport)); assert!(!defaults.contains(ServiceCapability::Llm)); assert!(!defaults.contains(ServiceCapability::Agent)); } }
    #[test] fn operation_build_availability_follows_implementation_capabilities() { assert_eq!( OperationKind::QueryExecute.is_compiled(), cfg!(feature = "database") ); assert_eq!( OperationKind::AuthBegin.is_compiled(), cfg!(feature = "auth") ); assert_eq!( OperationKind::FileList.is_compiled(), cfg!(feature = "files") ); assert_eq!( OperationKind::EventsSubscribe.is_compiled(), cfg!(feature = "events") ); assert_eq!( OperationKind::DataWorkerRun.is_compiled(), cfg!(feature = "data-import") ); assert_eq!( OperationKind::LlmStatus.is_compiled(), cfg!(feature = "llm") ); assert_eq!( OperationKind::LlmGenerate.is_compiled(), cfg!(feature = "llm") ); assert_eq!( OperationKind::LlmCancel.is_compiled(), cfg!(feature = "llm") ); assert_eq!( OperationKind::AgentStatus.is_compiled(), cfg!(feature = "agent") ); assert_eq!( OperationKind::AgentRun.is_compiled(), cfg!(feature = "agent") ); assert!(OperationKind::Ping.is_compiled()); }
    #[test] fn provider_capability_is_distinct_from_local_dependencies() { assert_eq!( OperationKind::IdentityRegister.provider_capability(), Some(ServiceCapability::Auth) ); assert_eq!( OperationKind::IdentityRegister.implementation_capabilities(), AUTH ); assert!(OperationKind::IdentityRegister.requires_db_engine()); assert!(OperationKind::FileRead.requires_db_engine()); assert!(!OperationKind::DataWorkerRun.requires_db_engine()); assert!(!OperationKind::LlmStatus.requires_db_engine()); assert!(!OperationKind::AgentStatus.requires_db_engine()); assert_eq!( OperationKind::QueryExecute.provider_capability(), Some(ServiceCapability::Database) ); assert_eq!( OperationKind::FileRead.provider_capability(), Some(ServiceCapability::Files) ); assert_eq!( OperationKind::DataWorkerRun.provider_capability(), Some(ServiceCapability::DataImport) ); assert_eq!( OperationKind::LlmStatus.provider_capability(), Some(ServiceCapability::Llm) ); assert_eq!( OperationKind::LlmGenerate.provider_capability(), Some(ServiceCapability::Llm) ); assert_eq!( OperationKind::LlmCancel.provider_capability(), Some(ServiceCapability::Llm) ); assert_eq!( OperationKind::AgentStatus.provider_capability(), Some(ServiceCapability::Agent) ); assert_eq!( OperationKind::AgentRun.provider_capability(), Some(ServiceCapability::Agent) ); assert_eq!(OperationKind::Ping.provider_capability(), None); }
    #[test] fn operation_names_are_unique_and_non_empty() { let mut names = BTreeSet::new(); for operation in OPERATION_CATALOG { assert!(!operation.name.is_empty()); assert!( names.insert(operation.name), "duplicate operation {}", operation.name ); } assert_eq!(names.len(), OperationKind::ALL.len()); }
    #[test] fn every_kind_round_trips_through_the_catalogue() { for &kind in OperationKind::ALL { let descriptor = kind.descriptor(); assert_eq!( operation_by_name(descriptor.name).map(|entry| entry.kind), Some(kind) ); } }
    #[test] fn operation_capabilities_follow_handler_domains() { assert_eq!( OperationKind::QueryExecute.required_capabilities(), DATABASE ); assert_eq!(OperationKind::FileRead.required_capabilities(), FILES); assert_eq!( OperationKind::EventsSubscribe.required_capabilities(), EVENTS ); assert_eq!(OperationKind::AuthBegin.required_capabilities(), AUTH); assert_eq!(OperationKind::DeviceList.required_capabilities(), AUTH); assert_eq!( OperationKind::PlaceList.required_capabilities(), ServiceCapabilities::NONE ); assert_eq!( OperationKind::QueryContextResolve.required_capabilities(), ServiceCapabilities::NONE ); assert_eq!(OperationKind::DataImport.required_capabilities(), DATABASE); assert_eq!( OperationKind::DataMappingList.required_capabilities(), DATABASE ); assert_eq!( OperationKind::DataMappingUpdate.required_capabilities(), DATABASE ); assert_eq!( OperationKind::DataMappingDelete.required_capabilities(), DATABASE ); assert_eq!( OperationKind::DataWorkerRun.required_capabilities(), CAP_DATA_IMPORT ); assert_eq!( OperationKind::LlmStatus.required_capabilities(), LLM ); assert_eq!( OperationKind::LlmGenerate.required_capabilities(), LLM ); assert_eq!( OperationKind::LlmCancel.required_capabilities(), LLM ); assert_eq!( OperationKind::AgentStatus.required_capabilities(), AGENT ); assert_eq!( OperationKind::AgentRun.required_capabilities(), AGENT ); assert_eq!( OperationKind::CoreHealth.required_capabilities(), ServiceCapabilities::NONE ); }
    #[test] fn authority_scope_is_explicit_and_narrow() { assert_eq!(OperationKind::PlaceList.scope(), OperationScope::Authority); assert_eq!( OperationKind::PlaceResourceList.scope(), OperationScope::Authority ); assert_eq!( OperationKind::FabricResourceList.scope(), OperationScope::Authority ); assert_eq!( OperationKind::FabricResourceSet.scope(), OperationScope::Authority ); assert_eq!( OperationKind::QueryContextResolve.scope(), OperationScope::Authority ); assert_eq!( OperationKind::AppInstanceList.scope(), OperationScope::Authority ); assert_eq!(OperationKind::Ping.scope(), OperationScope::Local); }
    #[test] fn transport_contract_is_independent_from_handler_domain() { assert_eq!(OperationKind::FileSyncRun.handler(), HandlerKind::File); assert_eq!( OperationKind::FileSyncRun.transport(), TransportKind::Message ); assert_eq!(OperationKind::FileRead.handler(), HandlerKind::File); assert_eq!( OperationKind::FileRead.transport(), TransportKind::BinaryOut ); assert_eq!( OperationKind::FileWrite.transport(), TransportKind::BinaryIn ); assert_eq!( OperationKind::FileVersionRead.transport(), TransportKind::BinaryOut ); assert_eq!( OperationKind::DataWorkerRun.transport(), TransportKind::BinaryIn ); assert_eq!( OperationKind::QueryExecute.transport(), TransportKind::MessageStream ); assert_eq!( OperationKind::LlmGenerate.transport(), TransportKind::MessageStream ); assert_eq!( OperationKind::AgentRun.transport(), TransportKind::MessageStream ); }
    #[test] fn binary_transports_require_exclusive_channels() { for operation in OPERATION_CATALOG { if matches!( operation.transport, TransportKind::BinaryIn | TransportKind::BinaryOut ) { assert_eq!( operation.connection, ConnectionKind::Exclusive, "binary operation {} must use an exclusive channel", operation.name ); } } }
}
