//! OG Core public API and module wiring.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![forbid(unsafe_code)]
#![deny(missing_debug_implementations, rust_2018_idioms, unused_must_use)]
#![warn(clippy::all, clippy::cargo, clippy::pedantic, clippy::nursery)]

pub mod access;
pub mod backup;
pub mod model;
pub mod debug;
pub mod error;
pub mod event_engine;
pub mod files;
pub mod indexing;
pub mod helpers;
pub mod memory;
pub mod operation;
pub mod query;
pub mod spill;
pub use model::capability::{ capabilities_of, capabilities_of_number, Capabilities, Capability, ValueCapabilities, };
pub use model::compare::{
    compare, compare_numbers, equals, greater_than, greater_than_or_equal, less_than,
    less_than_or_equal, not_equals, physically_equals, CompareFailure, CompareResult, Comparison,
};
pub use model::coercion::{
    coerce_number_pair, coerce_numbers, coerce_value_pair_to_numbers, coerce_value_to_number,
    is_integer_syntax, is_numeric_string, parse_number, parse_number_value, CoercedNumber,
    CoercedNumberPair, CoercionFailure, CoercionPolicy, CoercionResult,
};
pub use model::document::{Document, FieldName};
pub use debug::{DebugTopic, enabled as debug_enabled, log as debug_log, memory_enabled as debug_memory_enabled, protocol_enabled as debug_protocol_enabled, redact_json as redact_debug_json, timing_enabled as debug_timing_enabled};
pub use error::{Error, Result};
pub use model::field_path::{FieldPath, FieldPathSegment, ResolvedValue};
pub use indexing::{IndexingEngine, IndexingSnapshot, ObservedAccess, QueryAggregate, QueryFingerprint, QueryObservation, DEFAULT_OBSERVATION_CAPACITY};
pub use spill::{SpillEngine, SpillRun, SpillRunReader, SpillRunWriter};
pub use access::auth::{AuthChallenge, AuthError, ConnectionAuth, DeviceCredential, EnrollmentIdentity, Principal, DEFAULT_CHALLENGE_TTL};
pub use access::place::{parse_sharing_permission, sharing_permission, ExecutionContext, PlaceRole, PublicAccess, RequestedExecutionContext};
pub use operation::{decode_operation_request, Audience, Event as CoreEvent, IncomingRequest, OperationKind, OperationRequest, OperationResponse, OperationRouter, RoutedOperation, APP_CREATE, APP_DELETE, APP_GET, APP_INSTANCE_CREATE, APP_INSTANCE_LIST, APP_INSTANCE_REMOVE, APP_LIST, APP_UPDATE, AUTH_BEGIN, AUTH_COMPLETE, BACKUP_CREATE, BACKUP_INSPECT, BACKUP_RESTORE, COLLECTIONS_LIST, AUTH_ENROLL_BEGIN, AUTH_ENROLL_COMPLETE, DEVICE_REGISTER, DEVICE_REVOKE, EVENTS_SUBSCRIBE, DATA_ANALYZE, DATA_IMPORT, DATA_MAPPING_SAVE, IDENTITY_REGISTER, IDENTITY_RENEW, PLACE_CREATE, PLACE_DELETE, PLACE_GET, PLACE_LIST, PLACE_UPDATE, PLACE_PUBLIC_SET, QUERY_EXECUTE, SHARING_CREATE, SHARING_DELETE, SHARING_UPDATE};
pub use event_engine::{EventEngine, EventEngineSnapshot, EventSubscription, DEFAULT_EVENT_CAPACITY, DEFAULT_SUBSCRIBER_CAPACITY};
pub use files::{FileCapabilities, FileEntry, FileId, FileKind, FileMetadata, FileModelError, FileRange, FileReader, FileResult, FileStore, FileStoreEntry, FileStoreError, FileWrite, StoreId, FILES_COLLECTION};
pub use memory::{MemoryClass, MemoryClassSnapshot, MemoryEvent, MemoryEventKind, MemoryEventSnapshot, MemoryGovernor, MemoryProfile, MemoryProfileConfig, MemoryReclaimer, MemoryReservation, MemoryReservationError, MemorySnapshot, ProcessMemoryPressure, ProcessMemoryPressureError, ProcessMemorySnapshot, QueryAdmissionError, QueryMemoryPermit, QueryMemoryRecord, QueryMemorySnapshot, WorkloadClass, DEFAULT_MEMORY_EVENT_CAPACITY, MEMORY_EVENT_MIN_BYTES};
pub use model::value::{Number, NumberKind, PhysicalKind, Value};
pub const API_VERSION: u32 = 1;

#[must_use]
pub const fn api_version_string() -> &'static str { "1" }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_api_version_is_defined() { assert_eq!(API_VERSION, 1); }

    #[test]
    fn public_result_alias_is_available() {
        fn successful_operation() -> Result<()> {
            Ok(())
        }

        assert!(successful_operation().is_ok());
    }

    #[test]
    fn api_version_string_is_stable() {
        assert_eq!(api_version_string(), "1");
    }

    #[test]
    fn comparison_helpers_are_exported() {
        let _ = less_than;
        let _ = greater_than;
        let _ = physically_equals;
    }

    #[test]
    fn value_is_exported_from_the_crate_root() {
        let value = Value::Null;

        assert_eq!(value, Value::Null);
    }
}

pub mod engine;
pub mod storage;

pub use engine::{ Engine, EngineError, EngineErrorKind, EngineResult, PlanLowerer, PlannedQuery, QueryOutput, };
pub mod protocol;
