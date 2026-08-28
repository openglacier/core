//! Default expression evaluator.

use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

pub use crate::error::NativeEvaluationLimitsError;

use crate::Document;

use super::{
    AssignmentPolicy, EvaluationBackend, EvaluationContext, EvaluationError, EvaluationErrorKind,
    EvaluationResult, Evaluator, Expression, MissingPolicy, QueryRuntime, SetAssignment,
};

/// Semantic primitives implemented by the native expression layer.
///
/// Keeping this boundary narrow lets `expression.rs` change its internal enum
/// layout without forcing changes in the executor or runtime.
pub trait NativeSemantics: Send + Sync {
    /// Evaluates one expression as a strict predicate.
    fn evaluate_predicate(
        &self,
        expression: &Expression,
        document: &Document,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<bool>;

    /// Applies all assignments to a private document value.
    ///
    /// The returned document is published only when this call succeeds.
    fn apply_assignments(
        &self,
        assignments: &[SetAssignment],
        document: &Document,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<Document>;
}

/// Production evaluation backend.
#[derive(Clone)]
pub struct NativeEvaluator {
    semantics: Arc<dyn NativeSemantics>,
    limits: NativeEvaluationLimits,
    statistics: Arc<NativeEvaluationStatistics>,
}

impl NativeEvaluator {
    /// Creates a native evaluator with default limits.
    #[must_use]
    #[inline]
    pub fn new(semantics: Arc<dyn NativeSemantics>) -> Self {
        Self::with_limits(semantics, NativeEvaluationLimits::default())
    }

    /// Creates a native evaluator with explicit safety limits.
    #[must_use]
    pub fn with_limits(
        semantics: Arc<dyn NativeSemantics>,
        limits: NativeEvaluationLimits,
    ) -> Self {
        Self {
            semantics,
            limits,
            statistics: Arc::new(NativeEvaluationStatistics::default()),
        }
    }

    /// Returns the configured safety limits.
    #[must_use]
    pub const fn limits(&self) -> NativeEvaluationLimits {
        self.limits
    }

    /// Returns a snapshot of runtime counters.
    #[must_use]
    pub fn statistics(&self) -> NativeEvaluationStatisticsSnapshot {
        self.statistics.snapshot()
    }

    /// Resets runtime counters.
    pub fn reset_statistics(&self) {
        self.statistics.reset();
    }

    /// Wraps this backend in the stable evaluator façade.
    #[must_use]
    pub fn into_evaluator(self) -> Evaluator {
        Evaluator::new(Arc::new(self))
    }

    /// Wraps this backend in an evaluator with an explicit context.
    #[must_use]
    pub fn into_evaluator_with_context(self, context: EvaluationContext) -> Evaluator {
        Evaluator::with_context(Arc::new(self), context)
    }

    /// Creates an execution runtime backed by this native evaluator.
    #[must_use]
    pub fn into_runtime(self) -> QueryRuntime {
        self.into_evaluator().into_runtime()
    }

    fn session<'a>(&'a self, context: &'a EvaluationContext) -> NativeEvaluationSession<'a> {
        NativeEvaluationSession {
            context,
            limits: self.limits,
            depth: 0,
            steps: 0,
        }
    }
}

impl fmt::Debug for NativeEvaluator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeEvaluator")
            .field("semantics", &"<native semantics>")
            .field("limits", &self.limits)
            .field("statistics", &self.statistics())
            .finish()
    }
}

impl EvaluationBackend for NativeEvaluator {
    fn evaluate_predicate(
        &self,
        expression: &Expression,
        document: &Document,
        context: &EvaluationContext,
    ) -> EvaluationResult<bool> {
        self.statistics
            .predicate_evaluations
            .fetch_add(1, Ordering::Relaxed);

        let mut session = self.session(context);
        let result = self
            .semantics
            .evaluate_predicate(expression, document, &mut session);

        self.statistics
            .steps
            .fetch_add(session.steps, Ordering::Relaxed);

        if result.is_err() {
            self.statistics.errors.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    fn apply_assignments(
        &self,
        assignments: &[SetAssignment],
        document: &Document,
        context: &EvaluationContext,
    ) -> EvaluationResult<Arc<Document>> {
        self.statistics
            .mutation_evaluations
            .fetch_add(1, Ordering::Relaxed);

        if assignments.is_empty() {
            self.statistics.errors.fetch_add(1, Ordering::Relaxed);
            return Err(EvaluationError::new(EvaluationErrorKind::EmptyAssignments));
        }

        if context.assignment_policy() != AssignmentPolicy::Atomic {
            self.statistics.errors.fetch_add(1, Ordering::Relaxed);
            return Err(EvaluationError::backend(
                "native evaluator only supports atomic assignments",
            ));
        }

        let mut session = self.session(context);
        let result = self
            .semantics
            .apply_assignments(assignments, document, &mut session)
            .map(Arc::new);

        self.statistics
            .steps
            .fetch_add(session.steps, Ordering::Relaxed);

        if result.is_err() {
            self.statistics.errors.fetch_add(1, Ordering::Relaxed);
        }

        result
    }
}

/// Mutable state scoped to one predicate or mutation evaluation.
///
/// Native expression recursion must call [`Self::enter`] before descending and
/// retain the returned guard until the child evaluation finishes.
pub struct NativeEvaluationSession<'a> {
    context: &'a EvaluationContext,
    limits: NativeEvaluationLimits,
    depth: usize,
    steps: u64,
}

impl<'a> NativeEvaluationSession<'a> {
    pub(crate) const fn new(
        context: &'a EvaluationContext,
        limits: NativeEvaluationLimits,
    ) -> Self {
        Self {
            context,
            limits,
            depth: 0,
            steps: 0,
        }
    }

    /// Returns the immutable semantic context.
    #[must_use]
    pub const fn context(&self) -> &'a EvaluationContext {
        self.context
    }

    /// Returns the active missing-value policy.
    #[must_use]
    pub const fn missing_policy(&self) -> MissingPolicy {
        self.context.missing_policy()
    }

    /// Returns the current recursive depth.
    #[must_use]
    pub const fn depth(&self) -> usize {
        self.depth
    }

    /// Returns the number of charged semantic steps.
    #[must_use]
    pub const fn steps(&self) -> u64 {
        self.steps
    }

    /// Charges one semantic operation.
    pub fn charge(&mut self) -> EvaluationResult<()> {
        self.charge_many(1)
    }

    /// Charges several semantic operations.
    pub fn charge_many(&mut self, amount: u64) -> EvaluationResult<()> {
        let next = self.steps.checked_add(amount).ok_or_else(|| {
            EvaluationError::new(EvaluationErrorKind::Backend {
                message: Arc::from("evaluation step counter overflowed"),
            })
        })?;

        if next > self.limits.max_steps {
            return Err(EvaluationError::new(EvaluationErrorKind::Backend {
                message: Arc::from(format!(
                    "evaluation exceeded the maximum of {} semantic steps",
                    self.limits.max_steps
                )),
            }));
        }

        self.steps = next;
        Ok(())
    }

    /// Enters one recursive expression level.
    pub fn enter(&mut self) -> EvaluationResult<NativeDepthGuard<'_, 'a>> {
        self.charge()?;

        if self.depth >= self.limits.max_depth {
            return Err(EvaluationError::new(EvaluationErrorKind::Backend {
                message: Arc::from(format!(
                    "expression nesting exceeds the maximum depth of {}",
                    self.limits.max_depth
                )),
            }));
        }

        self.depth += 1;

        Ok(NativeDepthGuard { session: self })
    }
}

impl fmt::Debug for NativeEvaluationSession<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeEvaluationSession")
            .field("context", self.context)
            .field("limits", &self.limits)
            .field("depth", &self.depth)
            .field("steps", &self.steps)
            .finish()
    }
}

/// RAII guard restoring recursive depth when a child evaluation finishes.
pub struct NativeDepthGuard<'session, 'context> {
    session: &'session mut NativeEvaluationSession<'context>,
}

impl<'session, 'context> NativeDepthGuard<'session, 'context> {
    /// Returns the guarded mutable session.
    #[must_use]
    pub fn session(&mut self) -> &mut NativeEvaluationSession<'context> {
        self.session
    }
}

impl Drop for NativeDepthGuard<'_, '_> {
    fn drop(&mut self) {
        debug_assert!(self.session.depth > 0);
        self.session.depth -= 1;
    }
}

impl fmt::Debug for NativeDepthGuard<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeDepthGuard")
            .field("depth", &self.session.depth)
            .finish()
    }
}

/// Evaluation safety limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeEvaluationLimits {
    /// Maximum nested expression depth.
    pub max_depth: usize,

    /// Maximum charged operations per document/operator evaluation.
    pub max_steps: u64,
}

impl NativeEvaluationLimits {
    /// Conservative production defaults.
    pub const DEFAULT_MAX_DEPTH: usize = 128;
    pub const DEFAULT_MAX_STEPS: u64 = 100_000;

    /// Creates validated limits.
    #[inline]
    pub fn new(max_depth: usize, max_steps: u64) -> Result<Self, NativeEvaluationLimitsError> {
        if max_depth == 0 {
            return Err(NativeEvaluationLimitsError::ZeroDepth);
        }

        if max_steps == 0 {
            return Err(NativeEvaluationLimitsError::ZeroSteps);
        }

        Ok(Self {
            max_depth,
            max_steps,
        })
    }
}

impl Default for NativeEvaluationLimits {
    fn default() -> Self {
        Self {
            max_depth: Self::DEFAULT_MAX_DEPTH,
            max_steps: Self::DEFAULT_MAX_STEPS,
        }
    }
}

/// Shared lock-free runtime counters.
#[derive(Debug, Default)]
struct NativeEvaluationStatistics {
    predicate_evaluations: AtomicU64,
    mutation_evaluations: AtomicU64,
    steps: AtomicU64,
    errors: AtomicU64,
}

impl NativeEvaluationStatistics {
    fn snapshot(&self) -> NativeEvaluationStatisticsSnapshot {
        NativeEvaluationStatisticsSnapshot {
            predicate_evaluations: self.predicate_evaluations.load(Ordering::Relaxed),
            mutation_evaluations: self.mutation_evaluations.load(Ordering::Relaxed),
            steps: self.steps.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.predicate_evaluations.store(0, Ordering::Relaxed);
        self.mutation_evaluations.store(0, Ordering::Relaxed);
        self.steps.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
    }
}

/// Immutable counter snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NativeEvaluationStatisticsSnapshot {
    pub predicate_evaluations: u64,
    pub mutation_evaluations: u64,
    pub steps: u64,
    pub errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_limits_are_non_zero() {
        let limits = NativeEvaluationLimits::default();

        assert!(limits.max_depth > 0);
        assert!(limits.max_steps > 0);
    }

    #[test]
    fn rejects_zero_limits() {
        assert_eq!(
            NativeEvaluationLimits::new(0, 1),
            Err(NativeEvaluationLimitsError::ZeroDepth)
        );
        assert_eq!(
            NativeEvaluationLimits::new(1, 0),
            Err(NativeEvaluationLimitsError::ZeroSteps)
        );
    }

    #[test]
    fn session_enforces_step_limit() {
        let context = EvaluationContext::default();
        let mut session = NativeEvaluationSession {
            context: &context,
            limits: NativeEvaluationLimits {
                max_depth: 2,
                max_steps: 1,
            },
            depth: 0,
            steps: 0,
        };

        assert!(session.charge().is_ok());
        assert!(session.charge().is_err());
    }

    #[test]
    fn depth_guard_restores_depth() {
        let context = EvaluationContext::default();
        let mut session = NativeEvaluationSession {
            context: &context,
            limits: NativeEvaluationLimits {
                max_depth: 2,
                max_steps: 10,
            },
            depth: 0,
            steps: 0,
        };

        {
            let mut guard = session.enter().unwrap();
            assert_eq!(guard.session().depth(), 1);
        }

        assert_eq!(session.depth(), 0);
    }

    #[test]
    fn public_types_are_send_and_sync() {
        fn assert_send_and_sync<T: Send + Sync>() {}

        assert_send_and_sync::<NativeEvaluator>();
        assert_send_and_sync::<NativeEvaluationLimits>();
        assert_send_and_sync::<NativeEvaluationStatisticsSnapshot>();
    }
}
