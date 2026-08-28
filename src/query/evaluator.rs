//! Expression evaluator contracts.

use std::{error::Error as StdError, fmt, sync::Arc};

use crate::Document;

use super::{ExecutionError, Expression, QueryRuntime, QueryRuntimeBuilder, SetAssignment};

/// Result returned by evaluator operations.
pub type EvaluationResult<T> = std::result::Result<T, EvaluationError>;

/// Semantic implementation used by [`Evaluator`].
///
/// Implementations typically delegate to:
///
/// - `FieldPath` for document lookup;
/// - `compare` and `coercion` for comparison expressions;
/// - expression nodes for boolean composition;
/// - document mutation helpers for `set`.
pub trait EvaluationBackend: Send + Sync {
    /// Evaluates an expression to its strict boolean result.
    ///
    /// Implementations must not introduce ambiguous truthiness. Expressions
    /// that cannot produce a boolean should return an error.
    fn evaluate_predicate(
        &self,
        expression: &Expression,
        document: &Document,
        context: &EvaluationContext,
    ) -> EvaluationResult<bool>;

    /// Applies all assignments atomically to a document clone.
    ///
    /// The source document must remain unchanged when any assignment fails.
    fn apply_assignments(
        &self,
        assignments: &[SetAssignment],
        document: &Document,
        context: &EvaluationContext,
    ) -> EvaluationResult<Arc<Document>>;
}

/// Stable evaluator used by the query runtime.
#[derive(Clone)]
pub struct Evaluator {
    backend: Arc<dyn EvaluationBackend>,
    context: EvaluationContext,
}

impl Evaluator {
    /// Creates an evaluator with the default context.
    #[must_use]
    #[inline]
    pub fn new(backend: Arc<dyn EvaluationBackend>) -> Self {
        Self {
            backend,
            context: EvaluationContext::default(),
        }
    }

    /// Creates an evaluator with an explicit context.
    #[must_use]
    pub const fn with_context(
        backend: Arc<dyn EvaluationBackend>,
        context: EvaluationContext,
    ) -> Self {
        Self { backend, context }
    }

    /// Returns the evaluation context.
    #[must_use]
    pub const fn context(&self) -> &EvaluationContext {
        &self.context
    }

    /// Returns the semantic backend.
    #[must_use]
    pub fn backend(&self) -> &dyn EvaluationBackend {
        self.backend.as_ref()
    }

    /// Evaluates a strict predicate.
    pub fn evaluate_predicate(
        &self,
        expression: &Expression,
        document: &Document,
    ) -> EvaluationResult<bool> {
        self.backend
            .evaluate_predicate(expression, document, &self.context)
    }

    /// Applies one complete `set` operator.
    pub fn apply_assignments(
        &self,
        assignments: &[SetAssignment],
        document: &Document,
    ) -> EvaluationResult<Arc<Document>> {
        if assignments.is_empty() {
            return Err(EvaluationError::new(EvaluationErrorKind::EmptyAssignments));
        }

        self.backend
            .apply_assignments(assignments, document, &self.context)
    }

    /// Creates an execution runtime backed by this evaluator.
    ///
    /// `load` and custom stages remain unconfigured and can be added through
    /// [`QueryRuntime::with_load`] and [`QueryRuntime::with_custom`].
    #[must_use]
    pub fn into_runtime(self) -> QueryRuntime {
        let predicate_evaluator = self.clone();
        let set_evaluator = self;

        QueryRuntime::new(
            move |expression, document| {
                predicate_evaluator
                    .evaluate_predicate(expression, document)
                    .map_err(ExecutionError::from)
            },
            move |assignments, document| {
                set_evaluator
                    .apply_assignments(assignments, document)
                    .map_err(ExecutionError::from)
            },
        )
    }

    /// Installs this evaluator into a runtime builder.
    #[must_use]
    pub fn runtime_builder(self) -> QueryRuntimeBuilder {
        let predicate_evaluator = self.clone();
        let set_evaluator = self;

        QueryRuntimeBuilder::new()
            .predicate(move |expression, document| {
                predicate_evaluator
                    .evaluate_predicate(expression, document)
                    .map_err(ExecutionError::from)
            })
            .set(move |assignments, document| {
                set_evaluator
                    .apply_assignments(assignments, document)
                    .map_err(ExecutionError::from)
            })
    }
}

impl fmt::Debug for Evaluator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Evaluator")
            .field("backend", &"<evaluation backend>")
            .field("context", &self.context)
            .finish()
    }
}

/// Immutable evaluation configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EvaluationContext {
    missing_policy: MissingPolicy,
    boolean_policy: BooleanPolicy,
    assignment_policy: AssignmentPolicy,
}

impl EvaluationContext {
    /// Creates the default strict evaluation context.
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            missing_policy: MissingPolicy::Preserve,
            boolean_policy: BooleanPolicy::Strict,
            assignment_policy: AssignmentPolicy::Atomic,
        }
    }

    /// Sets missing-value behavior.
    #[must_use]
    pub const fn with_missing_policy(mut self, policy: MissingPolicy) -> Self {
        self.missing_policy = policy;
        self
    }

    /// Sets boolean evaluation behavior.
    #[must_use]
    pub const fn with_boolean_policy(mut self, policy: BooleanPolicy) -> Self {
        self.boolean_policy = policy;
        self
    }

    /// Sets assignment behavior.
    #[must_use]
    pub const fn with_assignment_policy(mut self, policy: AssignmentPolicy) -> Self {
        self.assignment_policy = policy;
        self
    }

    /// Returns missing-value behavior.
    #[must_use]
    pub const fn missing_policy(self) -> MissingPolicy {
        self.missing_policy
    }

    /// Returns boolean evaluation behavior.
    #[must_use]
    pub const fn boolean_policy(self) -> BooleanPolicy {
        self.boolean_policy
    }

    /// Returns assignment behavior.
    #[must_use]
    pub const fn assignment_policy(self) -> AssignmentPolicy {
        self.assignment_policy
    }
}

impl Default for EvaluationContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Behavior of unresolved field paths.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum MissingPolicy {
    /// Keep `missing` distinct from `null`.
    #[default]
    Preserve,

    /// Treat `missing` as `null` where comparison rules permit it.
    NullCompatible,

    /// Return an error as soon as a field path is missing.
    Error,
}

/// Boolean conversion behavior.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum BooleanPolicy {
    /// Only a boolean expression result is accepted.
    #[default]
    Strict,
}

/// Assignment application behavior.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum AssignmentPolicy {
    /// Apply every assignment to a clone and publish only on full success.
    #[default]
    Atomic,
}

/// Evaluation failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvaluationError {
    kind: EvaluationErrorKind,
}

impl EvaluationError {
    /// Creates an evaluation error.
    #[must_use]
    #[inline]
    pub const fn new(kind: EvaluationErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the detailed category.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &EvaluationErrorKind {
        &self.kind
    }

    /// Creates an invalid-result error.
    #[must_use]
    pub fn non_boolean(expression: impl Into<Arc<str>>) -> Self {
        Self::new(EvaluationErrorKind::NonBooleanPredicate {
            expression: expression.into(),
        })
    }

    /// Creates a missing-field error.
    #[must_use]
    pub fn missing_field(path: impl Into<Arc<str>>) -> Self {
        Self::new(EvaluationErrorKind::MissingField { path: path.into() })
    }

    /// Creates an incompatible-values error.
    #[must_use]
    pub fn incompatible_values(
        operation: impl Into<Arc<str>>,
        left: impl Into<Arc<str>>,
        right: impl Into<Arc<str>>,
    ) -> Self {
        Self::new(EvaluationErrorKind::IncompatibleValues {
            operation: operation.into(),
            left: left.into(),
            right: right.into(),
        })
    }

    /// Creates a backend error.
    #[must_use]
    pub fn backend(message: impl Into<Arc<str>>) -> Self {
        Self::new(EvaluationErrorKind::Backend {
            message: message.into(),
        })
    }
}

impl fmt::Display for EvaluationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            EvaluationErrorKind::EmptyAssignments => {
                formatter.write_str("set evaluation requires at least one assignment")
            }
            EvaluationErrorKind::NonBooleanPredicate { expression } => {
                write!(
                    formatter,
                    "predicate {expression:?} did not evaluate to a boolean"
                )
            }
            EvaluationErrorKind::MissingField { path } => {
                write!(formatter, "field path {path:?} is missing")
            }
            EvaluationErrorKind::IncompatibleValues {
                operation,
                left,
                right,
            } => {
                write!(
                    formatter,
                    "operation {operation:?} is incompatible with values \
                     {left:?} and {right:?}"
                )
            }
            EvaluationErrorKind::Assignment { field, message } => {
                write!(formatter, "assignment to field {field:?} failed: {message}")
            }
            EvaluationErrorKind::Backend { message } => {
                write!(formatter, "evaluation backend failed: {message}")
            }
        }
    }
}

impl StdError for EvaluationError {}

impl From<EvaluationError> for ExecutionError {
    fn from(error: EvaluationError) -> Self {
        match error.kind() {
            EvaluationErrorKind::EmptyAssignments | EvaluationErrorKind::Assignment { .. } => {
                Self::mutation(error.to_string())
            }
            _ => Self::evaluation(error.to_string()),
        }
    }
}

/// Detailed evaluation failure category.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EvaluationErrorKind {
    /// A mutation operator contained no assignments.
    EmptyAssignments,

    /// A predicate produced a non-boolean value.
    NonBooleanPredicate { expression: Arc<str> },

    /// A required field path was unresolved.
    MissingField { path: Arc<str> },

    /// Values do not support the requested operation.
    IncompatibleValues {
        operation: Arc<str>,
        left: Arc<str>,
        right: Arc<str>,
    },

    /// An assignment failed.
    Assignment { field: Arc<str>, message: Arc<str> },

    /// Backend-specific evaluation failure.
    Backend { message: Arc<str> },
}

/// Closure-backed evaluation backend.
///
/// This adapter is convenient while the native recursive evaluator is being
/// assembled from the already validated expression, comparison, and document
/// APIs.
#[derive(Clone)]
pub struct FunctionEvaluationBackend {
    predicate: Arc<PredicateFunction>,
    assignments: Arc<AssignmentFunction>,
}

type PredicateFunction =
    dyn Fn(&Expression, &Document, &EvaluationContext) -> EvaluationResult<bool> + Send + Sync;

type AssignmentFunction = dyn Fn(&[SetAssignment], &Document, &EvaluationContext) -> EvaluationResult<Arc<Document>>
    + Send
    + Sync;

impl FunctionEvaluationBackend {
    /// Creates a backend from two semantic functions.
    #[must_use]
    pub fn new<P, A>(predicate: P, assignments: A) -> Self
    where
        P: Fn(&Expression, &Document, &EvaluationContext) -> EvaluationResult<bool>
            + Send
            + Sync
            + 'static,
        A: Fn(&[SetAssignment], &Document, &EvaluationContext) -> EvaluationResult<Arc<Document>>
            + Send
            + Sync
            + 'static,
    {
        Self {
            predicate: Arc::new(predicate),
            assignments: Arc::new(assignments),
        }
    }
}

impl fmt::Debug for FunctionEvaluationBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FunctionEvaluationBackend")
            .field("predicate", &"<function>")
            .field("assignments", &"<function>")
            .finish()
    }
}

impl EvaluationBackend for FunctionEvaluationBackend {
    fn evaluate_predicate(
        &self,
        expression: &Expression,
        document: &Document,
        context: &EvaluationContext,
    ) -> EvaluationResult<bool> {
        (self.predicate)(expression, document, context)
    }

    fn apply_assignments(
        &self,
        assignments: &[SetAssignment],
        document: &Document,
        context: &EvaluationContext,
    ) -> EvaluationResult<Arc<Document>> {
        (self.assignments)(assignments, document, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_context_is_strict_and_atomic() {
        let context = EvaluationContext::default();

        assert_eq!(context.missing_policy(), MissingPolicy::Preserve);
        assert_eq!(context.boolean_policy(), BooleanPolicy::Strict);
        assert_eq!(context.assignment_policy(), AssignmentPolicy::Atomic);
    }

    #[test]
    fn error_is_converted_to_execution_category() {
        let evaluation = EvaluationError::missing_field("address.city");
        let execution = ExecutionError::from(evaluation);

        assert!(matches!(
            execution.kind(),
            super::super::ExecutionErrorKind::Evaluation { .. }
        ));

        let mutation =
            ExecutionError::from(EvaluationError::new(EvaluationErrorKind::EmptyAssignments));

        assert!(matches!(
            mutation.kind(),
            super::super::ExecutionErrorKind::Mutation { .. }
        ));
    }

    #[test]
    fn evaluator_public_types_are_send_and_sync() {
        fn assert_send_and_sync<T: Send + Sync>() {}

        assert_send_and_sync::<Evaluator>();
        assert_send_and_sync::<EvaluationContext>();
        assert_send_and_sync::<EvaluationError>();
        assert_send_and_sync::<FunctionEvaluationBackend>();
    }
}
