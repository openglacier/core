//! Default expression semantics.

use std::{
    fmt,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

pub use crate::error::NativeSemanticBuildError;

use crate::Document;

use super::{
    EvaluationError, EvaluationErrorKind, EvaluationResult, Evaluator, Expression,
    NativeEvaluationLimits, NativeEvaluationSession, NativeEvaluator, NativeSemantics,
    QueryRuntime, SetAssignment,
};

type PredicateFunction = dyn Fn(&Expression, &Document, &mut NativeEvaluationSession<'_>) -> EvaluationResult<bool>
    + Send
    + Sync;

type AssignmentFunction = dyn Fn(
        usize,
        &SetAssignment,
        &Document,
        &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<Document>
    + Send
    + Sync;

/// Closure-backed implementation of [`NativeSemantics`].
///
/// Assignments are evaluated sequentially against a private document clone.
/// The original document is never modified, and the final clone is returned
/// only when every assignment succeeds.
#[derive(Clone)]
pub struct NativeSemanticFunctions {
    predicate: Arc<PredicateFunction>,
    assignment: Arc<AssignmentFunction>,
    options: NativeSemanticOptions,
}

impl NativeSemanticFunctions {
    /// Creates a strict native semantics adapter.
    #[must_use]
    pub fn new<P, A>(predicate: P, assignment: A) -> Self
    where
        P: Fn(&Expression, &Document, &mut NativeEvaluationSession<'_>) -> EvaluationResult<bool>
            + Send
            + Sync
            + 'static,
        A: Fn(
                usize,
                &SetAssignment,
                &Document,
                &mut NativeEvaluationSession<'_>,
            ) -> EvaluationResult<Document>
            + Send
            + Sync
            + 'static,
    {
        Self {
            predicate: Arc::new(predicate),
            assignment: Arc::new(assignment),
            options: NativeSemanticOptions::default(),
        }
    }

    /// Creates an adapter with explicit options.
    #[must_use]
    pub fn with_options<P, A>(predicate: P, assignment: A, options: NativeSemanticOptions) -> Self
    where
        P: Fn(&Expression, &Document, &mut NativeEvaluationSession<'_>) -> EvaluationResult<bool>
            + Send
            + Sync
            + 'static,
        A: Fn(
                usize,
                &SetAssignment,
                &Document,
                &mut NativeEvaluationSession<'_>,
            ) -> EvaluationResult<Document>
            + Send
            + Sync
            + 'static,
    {
        Self {
            predicate: Arc::new(predicate),
            assignment: Arc::new(assignment),
            options,
        }
    }

    /// Returns active semantic options.
    #[must_use]
    pub const fn options(&self) -> NativeSemanticOptions {
        self.options
    }

    /// Wraps these semantic functions in the production native evaluator.
    #[must_use]
    pub fn into_native_evaluator(self) -> NativeEvaluator {
        NativeEvaluator::new(Arc::new(self))
    }

    /// Wraps these semantic functions in a native evaluator with explicit
    /// recursion and semantic-step limits.
    #[must_use]
    pub fn into_native_evaluator_with_limits(
        self,
        limits: NativeEvaluationLimits,
    ) -> NativeEvaluator {
        NativeEvaluator::with_limits(Arc::new(self), limits)
    }

    /// Builds the stable evaluator façade from these semantic functions.
    #[must_use]
    pub fn into_evaluator(self) -> Evaluator {
        self.into_native_evaluator().into_evaluator()
    }

    /// Builds the base query runtime backed by native predicate and assignment
    /// semantics.
    ///
    /// The returned runtime configures `filter` and `set`. Document-level
    /// operators such as `sort`, `select`, `distinct`, `count`, `group`, and
    /// `insert` remain optional runtime handlers and can be installed with the
    /// corresponding [`QueryRuntime::with_*`](QueryRuntime) methods.
    #[must_use]
    pub fn into_runtime(self) -> QueryRuntime {
        self.into_native_evaluator().into_runtime()
    }

    /// Builds the base query runtime with explicit native evaluation limits.
    #[must_use]
    pub fn into_runtime_with_limits(self, limits: NativeEvaluationLimits) -> QueryRuntime {
        self.into_native_evaluator_with_limits(limits)
            .into_runtime()
    }

    fn evaluate_predicate_inner(
        &self,
        expression: &Expression,
        document: &Document,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<bool> {
        session.charge()?;
        (self.predicate)(expression, document, session)
    }

    fn apply_assignment_inner(
        &self,
        index: usize,
        assignment: &SetAssignment,
        document: &Document,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<Document> {
        session.charge()?;

        (self.assignment)(index, assignment, document, session)
            .map_err(|error| contextualize_assignment_error(index, error))
    }

    fn handle_panic<T>(
        &self,
        operation: &'static str,
        result: std::thread::Result<EvaluationResult<T>>,
    ) -> EvaluationResult<T> {
        match result {
            Ok(result) => result,
            Err(payload) if self.options.catch_panics => Err(EvaluationError::backend(format!(
                "native {operation} panicked: {}",
                panic_message(payload.as_ref())
            ))),
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}

impl fmt::Debug for NativeSemanticFunctions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeSemanticFunctions")
            .field("predicate", &"<native predicate function>")
            .field("assignment", &"<native assignment function>")
            .field("options", &self.options)
            .finish()
    }
}

impl NativeSemantics for NativeSemanticFunctions {
    fn evaluate_predicate(
        &self,
        expression: &Expression,
        document: &Document,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<bool> {
        let mut operation = || self.evaluate_predicate_inner(expression, document, session);

        if self.options.catch_panics {
            self.handle_panic(
                "predicate evaluation",
                catch_unwind(AssertUnwindSafe(operation)),
            )
        } else {
            operation()
        }
    }

    fn apply_assignments(
        &self,
        assignments: &[SetAssignment],
        document: &Document,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<Document> {
        if assignments.is_empty() {
            return Err(EvaluationError::new(EvaluationErrorKind::EmptyAssignments));
        }

        if assignments.len() > self.options.max_assignments {
            return Err(EvaluationError::backend(format!(
                "set operator contains {} assignments, exceeding the maximum of {}",
                assignments.len(),
                self.options.max_assignments
            )));
        }

        let mut operation = || {
            let mut candidate = document.clone();

            for (index, assignment) in assignments.iter().enumerate() {
                candidate = self.apply_assignment_inner(index, assignment, &candidate, session)?;
            }

            Ok(candidate)
        };

        if self.options.catch_panics {
            self.handle_panic(
                "assignment evaluation",
                catch_unwind(AssertUnwindSafe(operation)),
            )
        } else {
            operation()
        }
    }
}

/// Native semantic adapter configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeSemanticOptions {
    /// Maximum assignments accepted in one `set` operator.
    max_assignments: usize,

    /// Convert semantic panics to deterministic evaluation errors.
    catch_panics: bool,
}

impl NativeSemanticOptions {
    /// Default maximum assignment count.
    pub const DEFAULT_MAX_ASSIGNMENTS: usize = 1_024;

    /// Creates default native semantic options.
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            max_assignments: Self::DEFAULT_MAX_ASSIGNMENTS,
            catch_panics: true,
        }
    }

    /// Sets the maximum assignments accepted by one mutation operator.
    ///
    /// A zero value is normalized to one so that a configured adapter always
    /// accepts at least one assignment.
    #[must_use]
    pub const fn with_max_assignments(mut self, max_assignments: usize) -> Self {
        self.max_assignments = if max_assignments == 0 {
            1
        } else {
            max_assignments
        };
        self
    }

    /// Enables or disables panic conversion.
    #[must_use]
    pub const fn with_panic_capture(mut self, catch_panics: bool) -> Self {
        self.catch_panics = catch_panics;
        self
    }

    /// Returns the assignment limit.
    #[must_use]
    pub const fn max_assignments(self) -> usize {
        self.max_assignments
    }

    /// Returns whether panics are converted to errors.
    #[must_use]
    pub const fn catches_panics(self) -> bool {
        self.catch_panics
    }
}

impl Default for NativeSemanticOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for [`NativeSemanticFunctions`].
#[derive(Clone, Default)]
pub struct NativeSemanticBuilder {
    predicate: Option<Arc<PredicateFunction>>,
    assignment: Option<Arc<AssignmentFunction>>,
    options: NativeSemanticOptions,
}

impl NativeSemanticBuilder {
    /// Creates an empty builder.
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            predicate: None,
            assignment: None,
            options: NativeSemanticOptions::new(),
        }
    }

    /// Installs the recursive predicate function.
    #[must_use]
    pub fn predicate<P>(mut self, predicate: P) -> Self
    where
        P: Fn(&Expression, &Document, &mut NativeEvaluationSession<'_>) -> EvaluationResult<bool>
            + Send
            + Sync
            + 'static,
    {
        self.predicate = Some(Arc::new(predicate));
        self
    }

    /// Installs the single-assignment function.
    #[must_use]
    pub fn assignment<A>(mut self, assignment: A) -> Self
    where
        A: Fn(
                usize,
                &SetAssignment,
                &Document,
                &mut NativeEvaluationSession<'_>,
            ) -> EvaluationResult<Document>
            + Send
            + Sync
            + 'static,
    {
        self.assignment = Some(Arc::new(assignment));
        self
    }

    /// Sets semantic adapter options.
    #[must_use]
    pub const fn options(mut self, options: NativeSemanticOptions) -> Self {
        self.options = options;
        self
    }

    /// Builds and validates the adapter.
    pub fn build(self) -> Result<NativeSemanticFunctions, NativeSemanticBuildError> {
        let predicate = self
            .predicate
            .ok_or(NativeSemanticBuildError::MissingPredicate)?;
        let assignment = self
            .assignment
            .ok_or(NativeSemanticBuildError::MissingAssignment)?;

        Ok(NativeSemanticFunctions {
            predicate,
            assignment,
            options: self.options,
        })
    }
}

impl fmt::Debug for NativeSemanticBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeSemanticBuilder")
            .field("predicate", &self.predicate.is_some())
            .field("assignment", &self.assignment.is_some())
            .field("options", &self.options)
            .finish()
    }
}

fn contextualize_assignment_error(index: usize, error: EvaluationError) -> EvaluationError {
    match error.kind() {
        EvaluationErrorKind::Assignment { .. } => error,
        _ => EvaluationError::new(EvaluationErrorKind::Assignment {
            field: Arc::from(format!("#{index}")),
            message: Arc::from(error.to_string()),
        }),
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else {
        "non-string panic payload"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_have_safe_defaults() {
        let options = NativeSemanticOptions::default();

        assert_eq!(
            options.max_assignments(),
            NativeSemanticOptions::DEFAULT_MAX_ASSIGNMENTS
        );
        assert!(options.catches_panics());
    }

    #[test]
    fn zero_assignment_limit_is_normalized() {
        let options = NativeSemanticOptions::new().with_max_assignments(0);

        assert_eq!(options.max_assignments(), 1);
    }

    #[test]
    fn builder_requires_predicate_first() {
        let error = NativeSemanticBuilder::new().build().unwrap_err();

        assert_eq!(error, NativeSemanticBuildError::MissingPredicate);
    }

    #[test]
    fn builder_requires_assignment() {
        let error = NativeSemanticBuilder::new()
            .predicate(|_, _, _| Ok(true))
            .build()
            .unwrap_err();

        assert_eq!(error, NativeSemanticBuildError::MissingAssignment);
    }

    #[test]
    fn adapter_builds_native_runtime() {
        let runtime = NativeSemanticFunctions::new(
            |_, _, _| Ok(true),
            |_, _, document, _| Ok(document.clone()),
        )
        .into_runtime();

        assert!(!runtime.supports_load());
        assert!(!runtime.supports_sort());
        assert!(!runtime.supports_select());
        assert!(!runtime.supports_distinct());
        assert!(!runtime.supports_count());
        assert!(!runtime.supports_group());
        assert!(!runtime.supports_insert());
        assert!(!runtime.supports_custom());
    }

    #[test]
    fn builder_is_cloneable() {
        let builder = NativeSemanticBuilder::new()
            .predicate(|_, _, _| Ok(true))
            .assignment(|_, _, document, _| Ok(document.clone()));

        let _clone = builder.clone();
    }
}
