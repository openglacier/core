//! OG Core public API and module wiring.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![forbid(unsafe_code)]
#![deny(missing_debug_implementations, rust_2018_idioms, unused_must_use)]
// Lints de base
#![warn(clippy::all, clippy::cargo)]

// Groups optionnels — à activer progressivement
// #![warn(clippy::pedantic)]
// #![warn(clippy::nursery)]

#![allow( clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::missing_const_for_fn, clippy::result_large_err, clippy::too_many_lines, clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_possible_wrap, clippy::module_name_repetitions, clippy::struct_excessive_bools, clippy::option_option, clippy::type_complexity, clippy::too_many_arguments, clippy::multiple_crate_versions, clippy::option_if_let_else, clippy::significant_drop_tightening, clippy::unused_self, clippy::needless_pass_by_value, clippy::trivially_copy_pass_by_ref, clippy::match_same_arms, clippy::redundant_else, clippy::branches_sharing_code, clippy::verbose_bit_mask, clippy::unnecessary_wraps, clippy::while_let_loop, clippy::or_fun_call, clippy::manual_let_else, clippy::into_iter_without_iter, clippy::missing_fields_in_debug, clippy::struct_field_names, clippy::items_after_statements, clippy::assigning_clones, clippy::single_match_else, clippy::if_not_else, clippy::useless_let_if_seq, clippy::needless_continue, clippy::large_stack_arrays, clippy::redundant_closure_for_method_calls, clippy::explicit_iter_loop, clippy::unnecessary_trailing_comma, clippy::use_self, clippy::needless_lifetimes, clippy::elidable_lifetime_names, clippy::must_use_candidate, clippy::redundant_pub_crate, clippy::map_unwrap_or, clippy::wildcard_imports, clippy::double_must_use, clippy::used_underscore_binding, clippy::implicit_clone, clippy::derivable_impls, clippy::too_long_first_doc_paragraph, clippy::needless_raw_string_hashes, clippy::doc_markdown, clippy::derive_partial_eq_without_eq, clippy::unnested_or_patterns, clippy::should_implement_trait, clippy::misnamed_getters, clippy::question_mark, clippy::needless_range_loop, clippy::ptr_arg, clippy::iter_with_drain, clippy::redundant_clone, clippy::field_reassign_with_default, clippy::nonminimal_bool, clippy::literal_string_with_formatting_args, clippy::match_single_binding, clippy::format_in_format_args, clippy::drop_non_drop, clippy::manual_is_multiple_of, clippy::chunks_exact_to_as_chunks, clippy::manual_checked_ops, clippy::manual_contains, clippy::unnecessary_map_or, clippy::unnecessary_lazy_evaluations, clippy::useless_conversion, clippy::identity_op, clippy::needless_return, clippy::useless_borrows_in_formatting, clippy::unwrap_or_default, clippy::suboptimal_flops, clippy::collapsible_if, clippy::collapsible_match, clippy::semicolon_if_nothing_returned, clippy::borrow_as_ptr, clippy::checked_conversions, clippy::unnecessary_sort_by, clippy::needless_option_as_deref, )]

pub mod access;
pub mod backup;
pub mod build;
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
pub mod service;
pub mod daemon;
#[cfg(feature = "cli")]
pub mod cli;
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
pub use access::place::{parse_sharing_permission, sharing_permission, ExecutionContext, PlaceAccess, PlaceRole, PublicAccess, RequestedExecutionContext};
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
    #[test] fn public_api_version_is_defined() { assert_eq!(API_VERSION, 1); }
    #[test] fn public_result_alias_is_available() { fn successful_operation() -> Result<()> { Ok(()) } assert!(successful_operation().is_ok()); }
    #[test] fn api_version_string_is_stable() { assert_eq!(api_version_string(), "1"); }
    #[test] fn comparison_helpers_are_exported() { let _ = less_than; let _ = greater_than; let _ = physically_equals; }
    #[test] fn value_is_exported_from_the_crate_root() { let value = Value::Null; assert_eq!(value, Value::Null); }
}

pub mod engine;
pub mod storage;
pub use engine::{ Engine, EngineError, EngineErrorKind, EngineResult, PlanLowerer, PlannedQuery, QueryOutput, };
pub mod protocol;
