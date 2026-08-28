//! Core error types and conversions.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{error, fmt};
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Error {
    NonFiniteNumber { value: f64 },
    EmptyFieldPath,
    EmptyFieldPathSegment { index: usize },
    OperationNotFound { operation: String },
    OperationAlreadyRegistered { operation: String },
    InvalidOperationPayload { operation: String, reason: String },
    CapabilityUnavailable { operation: String, required: String },
}

impl Error {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::NonFiniteNumber { .. } => "validation.non_finite_number",
            Self::EmptyFieldPath => "validation.empty_field_path",
            Self::EmptyFieldPathSegment { .. } => "validation.empty_field_path_segment",
            Self::OperationNotFound { .. } => "operation.not_found",
            Self::OperationAlreadyRegistered { .. } => "operation.already_registered",
            Self::InvalidOperationPayload { .. } => "operation.invalid_payload",
            Self::CapabilityUnavailable { .. } => "capability.unavailable",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonFiniteNumber { value } => { write!(formatter, "numeric values must be finite; received {value}") },
            Self::EmptyFieldPath => formatter.write_str("field path must not be empty"),
            Self::EmptyFieldPathSegment { index } => { write!( formatter, "field path segment at index {index} must not be empty" ) },
            Self::OperationNotFound { operation } => { write!(formatter, "operation {operation:?} is not registered") },
            Self::OperationAlreadyRegistered { operation } => { write!(formatter, "operation {operation:?} is already registered") },
            Self::InvalidOperationPayload { operation, reason } => { write!( formatter, "invalid payload for operation {operation:?}: {reason}" ) },
            Self::CapabilityUnavailable { operation, required } => { write!(formatter, "operation {operation:?} requires unavailable capability {required}") }
        }
    }
}

impl error::Error for Error {}


/// Failures produced while validating, encoding, or decoding protocol messages.
#[derive(Debug)]
pub enum ProtocolError {
    /// The peer used an unsupported protocol version.
    UnsupportedVersion {
        /// Version received from the peer.
        received: u16,

        /// Version supported by this implementation.
        supported: u16,
    },

    /// The request contained only whitespace.
    EmptyQuery,

    /// The operation name contained only whitespace.
    EmptyOperation,

    /// A message exceeded its configured wire limit.
    MessageTooLarge {
        /// Kind of message being processed.
        kind: MessageKind,

        /// Actual encoded size.
        actual: usize,

        /// Maximum accepted encoded size.
        maximum: usize,
    },

    /// A server-to-client value could not be projected to the JS-safe wire representation.
    InvalidWireProjection(serde_json::Error),

    /// A server-to-client integer cannot be represented exactly by JavaScript.
    UnsafeJavaScriptInteger { value: String },

    /// A MessagePack payload could not be decoded.
    InvalidMessagePackDecode(rmp_serde::decode::Error),

    /// A value could not be encoded as MessagePack.
    InvalidMessagePackEncode(rmp_serde::encode::Error),

    /// A payload length was zero or otherwise invalid.
    InvalidPayloadLength { length: usize },

    /// A decoded message used the wrong top-level kind.
    InvalidMessageKind { expected: &'static str, received: String },
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion {
                received,
                supported,
            } => write!(
                formatter,
                "unsupported protocol version {received}; supported version is {supported}"
            ),
            Self::EmptyQuery => formatter.write_str("query must not be empty"),
            Self::EmptyOperation => formatter.write_str("operation must not be empty"),
            Self::MessageTooLarge {
                kind,
                actual,
                maximum,
            } => write!(
                formatter,
                "{kind} message is {actual} bytes; maximum is {maximum} bytes"
            ),
            Self::InvalidWireProjection(error) => {
                write!(formatter, "message cannot be projected to the wire representation: {error}")
            }
            Self::UnsafeJavaScriptInteger { value } => {
                write!(formatter, "integer {value} exceeds JavaScript safe integer range")
            }
            Self::InvalidMessagePackDecode(error) => {
                write!(formatter, "message is not valid MessagePack: {error}")
            }
            Self::InvalidMessagePackEncode(error) => {
                write!(formatter, "message cannot be encoded as MessagePack: {error}")
            }
            Self::InvalidPayloadLength { length } => {
                write!(formatter, "invalid payload length {length}")
            }
            Self::InvalidMessageKind { expected, received } => {
                write!(formatter, "invalid message kind {received:?}; expected {expected:?}")
            }
        }
    }
}

impl error::Error for ProtocolError {
    #[inline]
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::InvalidWireProjection(error) => Some(error),
            Self::InvalidMessagePackDecode(error) => Some(error),
            Self::InvalidMessagePackEncode(error) => Some(error),
            Self::UnsupportedVersion { .. }
            | Self::EmptyQuery
            | Self::EmptyOperation
            | Self::MessageTooLarge { .. }
            | Self::InvalidPayloadLength { .. }
            | Self::InvalidMessageKind { .. }
            | Self::UnsafeJavaScriptInteger { .. } => None,
        }
    }
}

/// Protocol message category used in diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    /// Client-to-server request.
    Request,
    /// Server-to-client response.
    Response,
}

impl fmt::Display for MessageKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request => formatter.write_str("request"),
            Self::Response => formatter.write_str("response"),
        }
    }
}


/// Error returned when standard RFC 4648 Base64 input is malformed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Base64DecodeError;
impl fmt::Display for Base64DecodeError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("invalid base64 data") } }
impl error::Error for Base64DecodeError {}

/// Authentication failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError { Random(String), NoPendingChallenge, ChallengeMismatch, ChallengeExpired, DeviceMismatch, UnsupportedAlgorithm(String), UnsupportedEncoding(String), InvalidBase64, InvalidPublicKey, InvalidSignature }
impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { match self {
        Self::Random(reason) => write!(f, "cannot generate authentication challenge: {reason}"),
        Self::NoPendingChallenge => f.write_str("no authentication challenge is pending"),
        Self::ChallengeMismatch => f.write_str("authentication challenge does not match"),
        Self::ChallengeExpired => f.write_str("authentication challenge has expired"),
        Self::DeviceMismatch => f.write_str("device credential does not match the pending challenge"),
        Self::UnsupportedAlgorithm(value) => write!(f, "unsupported public-key algorithm {value:?}"),
        Self::UnsupportedEncoding(value) => write!(f, "unsupported public-key encoding {value:?}"),
        Self::InvalidBase64 => f.write_str("invalid base64 authentication material"),
        Self::InvalidPublicKey => f.write_str("invalid Ed25519 public key"),
        Self::InvalidSignature => f.write_str("invalid Ed25519 signature"),
    } }
}
impl error::Error for AuthError {}

/// Backup and restore failure.
#[derive(Debug)]
pub enum BackupError { Io(std::io::Error), Storage(crate::storage::StorageError), Encode(rmp_serde::encode::Error), Decode(rmp_serde::decode::Error), Invalid(String) }
impl fmt::Display for BackupError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { match self { Self::Io(e) => write!(f, "I/O error: {e}"), Self::Storage(e) => write!(f, "storage error: {e}"), Self::Encode(e) => write!(f, "backup encode error: {e}"), Self::Decode(e) => write!(f, "backup decode error: {e}"), Self::Invalid(e) => write!(f, "invalid backup: {e}"), } } }
impl error::Error for BackupError {}
impl From<std::io::Error> for BackupError { fn from(value: std::io::Error) -> Self { Self::Io(value) } }
impl From<crate::storage::StorageError> for BackupError { fn from(value: crate::storage::StorageError) -> Self { Self::Storage(value) } }

/// Error returned when a real allocation-producing operation is attempted while process RSS is above the hard limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemoryPressureError { pub class: crate::memory::MemoryClass, pub requested_bytes: usize, pub rss_bytes: usize, pub soft_limit_bytes: usize, pub hard_limit_bytes: usize }
impl fmt::Display for ProcessMemoryPressureError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "process memory pressure for {}: RSS is {} bytes, requested growth is {} bytes, soft limit is {} bytes and hard limit is {} bytes", self.class, self.rss_bytes, self.requested_bytes, self.soft_limit_bytes, self.hard_limit_bytes) } }
impl error::Error for ProcessMemoryPressureError {}

/// Query memory admission failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryAdmissionError { pub class: crate::memory::WorkloadClass, pub requested_bytes: usize, pub active_bytes: usize, pub operation_budget_bytes: usize, pub active_heavy_operations: usize, pub max_concurrent_heavy: usize }
impl fmt::Display for QueryAdmissionError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "memory admission rejected for {}: requested {} bytes with {} operation bytes active; operation budget is {} bytes and {}/{} heavy operations are active", self.class.as_str(), self.requested_bytes, self.active_bytes, self.operation_budget_bytes, self.active_heavy_operations, self.max_concurrent_heavy) } }
impl error::Error for QueryAdmissionError {}

/// Error returned when the hard memory budget cannot satisfy a reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryReservationError { pub class: crate::memory::MemoryClass, pub requested_bytes: usize, pub current_bytes: usize, pub limit_bytes: Option<usize> }
impl fmt::Display for MemoryReservationError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { match self.limit_bytes { Some(limit) => write!(f, "memory reservation rejected for {}: requested {} bytes with {} bytes already reserved; limit is {} bytes", self.class, self.requested_bytes, self.current_bytes, limit), None => write!(f, "memory reservation rejected for {}: byte accounting overflow", self.class), } } }
impl error::Error for MemoryReservationError {}

/// Invalid evaluation-pipeline configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvaluationPipelineBuildError { MissingModel }
impl fmt::Display for EvaluationPipelineBuildError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { match self { Self::MissingModel => f.write_str("evaluation pipeline requires an expression model") } } }
impl error::Error for EvaluationPipelineBuildError {}

/// Invalid native evaluation limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeEvaluationLimitsError { ZeroDepth, ZeroSteps }
impl fmt::Display for NativeEvaluationLimitsError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { match self { Self::ZeroDepth => f.write_str("native evaluation maximum depth must be greater than zero"), Self::ZeroSteps => f.write_str("native evaluation maximum steps must be greater than zero"), } } }
impl error::Error for NativeEvaluationLimitsError {}

/// Invalid native semantic builder configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeSemanticBuildError { MissingPredicate, MissingAssignment }
impl fmt::Display for NativeSemanticBuildError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { match self { Self::MissingPredicate => f.write_str("native semantics require a predicate implementation"), Self::MissingAssignment => f.write_str("native semantics require an assignment implementation"), } } }
impl error::Error for NativeSemanticBuildError {}

/// Invalid query runtime builder configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryRuntimeBuildError { MissingPredicateHandler, MissingSetHandler }
impl fmt::Display for QueryRuntimeBuildError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { match self { Self::MissingPredicateHandler => f.write_str("query runtime requires a predicate handler"), Self::MissingSetHandler => f.write_str("query runtime requires a set handler"), } } }
impl error::Error for QueryRuntimeBuildError {}

/// Missing operation in a native expression-model configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeExpressionModelBuildError { MissingClassify, MissingLiteral, MissingField, MissingStrictBoolean, MissingBooleanValue, MissingCompare, MissingAssignmentExpression, MissingAssignmentField, MissingAssign }
impl NativeExpressionModelBuildError { #[must_use] pub const fn operation(self) -> &'static str { match self { Self::MissingClassify => "expression classification", Self::MissingLiteral => "literal extraction", Self::MissingField => "field resolution", Self::MissingStrictBoolean => "strict boolean conversion", Self::MissingBooleanValue => "boolean value construction", Self::MissingCompare => "comparison", Self::MissingAssignmentExpression => "assignment expression extraction", Self::MissingAssignmentField => "assignment diagnostic field formatting", Self::MissingAssign => "document assignment", } } }
impl fmt::Display for NativeExpressionModelBuildError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "native expression model requires {}", self.operation()) } }
impl error::Error for NativeExpressionModelBuildError {}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn result_alias_accepts_success() { let result: Result<u32> = Ok(42); assert_eq!(result, Ok(42)); }
    #[test] fn non_finite_number_preserves_value() { let error = Error::NonFiniteNumber { value: f64::INFINITY, }; assert_eq!( error, Error::NonFiniteNumber { value: f64::INFINITY, } ); }
    #[test] fn non_finite_number_has_expected_code() { let error = Error::NonFiniteNumber { value: f64::INFINITY, }; assert_eq!(error.code(), "validation.non_finite_number"); }
    #[test] fn non_finite_number_has_a_readable_message() { let error = Error::NonFiniteNumber { value: f64::NEG_INFINITY, }; assert_eq!( error.to_string(), "numeric values must be finite; received -inf" ); }
    #[test] fn empty_field_path_has_expected_code() { assert_eq!(Error::EmptyFieldPath.code(), "validation.empty_field_path"); }
    #[test] fn empty_field_path_has_a_readable_message() { let error = Error::EmptyFieldPath; assert_eq!(error.to_string(), "field path must not be empty"); }
    #[test] fn empty_field_path_segment_has_expected_code() { let error = Error::EmptyFieldPathSegment { index: 2 }; assert_eq!(error.code(), "validation.empty_field_path_segment"); }
    #[test] fn empty_field_path_segment_has_a_readable_message() { let error = Error::EmptyFieldPathSegment { index: 2 }; assert_eq!( error.to_string(), "field path segment at index 2 must not be empty" ); }
    #[test] fn operation_not_found_has_expected_code() { let error = Error::OperationNotFound { operation: "test".into(), }; assert_eq!(error.code(), "operation.not_found"); }
    #[test] fn operation_not_found_has_a_readable_message() { let error = Error::OperationNotFound { operation: "test".into(), }; assert_eq!(error.to_string(), r#"operation "test" is not registered"#); }
    #[test] fn operation_already_registered_has_expected_code() { let error = Error::OperationAlreadyRegistered { operation: "test".into(), }; assert_eq!(error.code(), "operation.already_registered"); }
    #[test] fn operation_already_registered_has_a_readable_message() { let error = Error::OperationAlreadyRegistered { operation: "test".into(), }; assert_eq!( error.to_string(), r#"operation "test" is already registered"# ); }
    #[test] fn invalid_operation_payload_has_expected_code() { let error = Error::InvalidOperationPayload { operation: "test".into(), reason: "missing field".into(), }; assert_eq!(error.code(), "operation.invalid_payload"); }
    #[test] fn invalid_operation_payload_has_a_readable_message() { let error = Error::InvalidOperationPayload { operation: "test".into(), reason: "missing field".into(), }; assert_eq!( error.to_string(), r#"invalid payload for operation "test": missing field"# ); }
    #[test] fn capability_unavailable_has_expected_code() { let error = Error::CapabilityUnavailable { operation: "file.list".into(), required: "files".into() }; assert_eq!(error.code(), "capability.unavailable"); }
    #[test] fn capability_unavailable_has_a_readable_message() { let error = Error::CapabilityUnavailable { operation: "file.list".into(), required: "files".into() }; assert_eq!(error.to_string(), r#"operation "file.list" requires unavailable capability files"#); }
    #[test] fn error_implements_standard_error() { fn assert_standard_error<T: std::error::Error>() {} assert_standard_error::<Error>(); }
    #[test] fn error_is_send_and_sync() { fn assert_send_and_sync<T: Send + Sync>() {} assert_send_and_sync::<Error>(); }
}
