//! Expression semantic validation.

use std::{fmt, sync::Arc};

use crate::Document;

use super::{
    EvaluationError, EvaluationErrorKind, EvaluationResult, Evaluator, Expression, MissingPolicy,
    NativeEvaluationLimits, NativeEvaluationSession, NativeEvaluator, NativeSemanticFunctions,
    NativeSemanticOptions, QueryRuntime, SetAssignment,
};

/// Value produced while evaluating an expression.
///
/// `Missing` is deliberately distinct from a physical `null` value. The
/// concrete physical value type remains owned by the expression model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SemanticValue<V> {
    /// A concrete physical value.
    Present(V),

    /// An unresolved document field.
    Missing,
}

impl<V> SemanticValue<V> {
    /// Creates a present semantic value.
    #[rustfmt::skip]#[must_use]
    pub const fn present(value: V) -> Self { Self::Present(value)}

    #[rustfmt::skip]#[must_use]
    pub const fn is_present(&self) -> bool { matches!(self, Self::Present(_))}

    /// Returns whether this value is missing.
    #[rustfmt::skip]#[must_use]
    pub const fn is_missing(&self) -> bool { matches!(self, Self::Missing)}

    /// Returns a shared reference to the present value.
    #[must_use]
    pub const fn as_present(&self) -> Option<&V> {
        match self {
            Self::Present(value) => Some(value),
            Self::Missing => None,
        }
    }

    /// Maps a concrete value while preserving `Missing`.
    #[must_use]
    pub fn map<U, F>(self, map: F) -> SemanticValue<U>
    where
        F: FnOnce(V) -> U,
    {
        match self {
            Self::Present(value) => SemanticValue::Present(map(value)),
            Self::Missing => SemanticValue::Missing,
        }
    }

    /// Returns the contained physical value.
    pub fn into_present(self) -> EvaluationResult<V> {
        match self {
            Self::Present(value) => Ok(value),
            Self::Missing => Err(EvaluationError::backend(
                "expected a present semantic value",
            )),
        }
    }
}

/// Field-value source used by expression semantics without requiring a
/// materialized [`Document`].
pub trait ExpressionFieldResolver<V>: Send + Sync {
    /// Resolves one validated expression field path.
    fn resolve_field(&self, field: &super::ExpressionFieldPath) -> SemanticValue<V>;
}

/// Logical node exposed by [`ExpressionModel`].
#[derive(Clone, Copy, Debug)]
pub enum ExpressionNode<'a> {
    /// A literal expression.
    Literal,

    /// A document field-path expression.
    Field,

    /// Boolean negation.
    Not { operand: &'a Expression },

    /// Short-circuit conjunction.
    And {
        left: &'a Expression,
        right: &'a Expression,
    },

    /// Short-circuit disjunction.
    Or {
        left: &'a Expression,
        right: &'a Expression,
    },

    /// Comparison delegated to the model.
    Comparison {
        left: &'a Expression,
        right: &'a Expression,
    },
}

/// Concrete expression integration contract.
///
/// The model owns physical values, field-path resolution, comparison/coercion
/// and document mutation. `ExpressionSemantics` owns recursive orchestration.
pub trait ExpressionModel: Send + Sync + 'static {
    /// Physical value used by the query model.
    type Value: Clone + fmt::Debug + Send + Sync + 'static;

    /// Classifies one expression node.
    fn classify<'a>(&self, expression: &'a Expression) -> EvaluationResult<ExpressionNode<'a>>;

    /// Evaluates a literal node.
    fn literal(&self, expression: &Expression) -> EvaluationResult<Self::Value>;

    /// Resolves a field node against a document.
    fn field(
        &self,
        expression: &Expression,
        document: &Document,
    ) -> EvaluationResult<SemanticValue<Self::Value>>;

    /// Converts a physical value to a strict boolean.
    ///
    /// Implementations must reject non-boolean values rather than introduce
    /// truthiness.
    fn strict_boolean(&self, value: &Self::Value) -> EvaluationResult<bool>;

    /// Materializes a physical boolean value.
    fn boolean_value(&self, value: bool) -> Self::Value;

    /// Compares two semantic operands.
    ///
    /// This is where `compare.rs`, `coercion.rs` and missing/null policy are
    /// applied.
    fn compare(
        &self,
        expression: &Expression,
        left: SemanticValue<Self::Value>,
        right: SemanticValue<Self::Value>,
        missing_policy: MissingPolicy,
    ) -> EvaluationResult<bool>;

    /// Returns the value expression of one assignment.
    fn assignment_expression<'a>(
        &self,
        assignment: &'a SetAssignment,
    ) -> EvaluationResult<&'a Expression>;

    /// Returns a stable field name for diagnostics.
    fn assignment_field(&self, assignment: &SetAssignment) -> Arc<str>;

    /// Applies one already-evaluated assignment to a document clone.
    fn assign(
        &self,
        assignment: &SetAssignment,
        value: SemanticValue<Self::Value>,
        document: &Document,
    ) -> EvaluationResult<Document>;
}

/// Native recursive expression semantics.
pub struct ExpressionSemantics<M> {
    model: Arc<M>,
}

impl<M> Clone for ExpressionSemantics<M> {
    fn clone(&self) -> Self {
        Self {
            model: Arc::clone(&self.model),
        }
    }
}

impl<M> ExpressionSemantics<M>
where
    M: ExpressionModel,
{
    /// Creates expression semantics from a concrete model.
    #[must_use]
    #[inline]
    pub const fn new(model: Arc<M>) -> Self {
        Self { model }
    }

    /// Creates expression semantics from an owned concrete model.
    #[rustfmt::skip]#[must_use]
    pub fn from_model(model: M) -> Self { Self::new(Arc::new(model))}

    /// Returns the concrete expression model.
    #[rustfmt::skip]#[must_use]
    pub const fn model(&self) -> &Arc<M> { &self.model }

    /// Evaluates an expression to a semantic value.
    pub fn evaluate(
        &self,
        expression: &Expression,
        document: &Document,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<SemanticValue<M::Value>> {
        self.evaluate_source(expression, ExpressionSource::Document(document), session)
    }

    /// Evaluates an expression directly from a field resolver.
    pub fn evaluate_resolved(
        &self,
        expression: &Expression,
        resolver: &dyn ExpressionFieldResolver<M::Value>,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<SemanticValue<M::Value>> {
        self.evaluate_source(expression, ExpressionSource::Resolver(resolver), session)
    }

    /// Evaluates an expression as a strict predicate.
    pub fn evaluate_predicate(
        &self,
        expression: &Expression,
        document: &Document,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<bool> {
        self.evaluate_predicate_source(expression, ExpressionSource::Document(document), session)
    }

    /// Evaluates a strict predicate directly from a field resolver.
    pub fn evaluate_predicate_resolved(
        &self,
        expression: &Expression,
        resolver: &dyn ExpressionFieldResolver<M::Value>,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<bool> {
        self.evaluate_predicate_source(expression, ExpressionSource::Resolver(resolver), session)
    }

    fn evaluate_source(
        &self,
        expression: &Expression,
        source: ExpressionSource<'_, M::Value>,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<SemanticValue<M::Value>> {
        let mut guard = session.enter()?;
        self.evaluate_node_source(expression, source, guard.session())
    }

    fn evaluate_predicate_source(
        &self,
        expression: &Expression,
        source: ExpressionSource<'_, M::Value>,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<bool> {
        let value = self.evaluate_source(expression, source, session)?;

        match value {
            SemanticValue::Present(value) => self.model.strict_boolean(&value),
            SemanticValue::Missing => match session.missing_policy() {
                MissingPolicy::Error => Err(EvaluationError::missing_field("<predicate>")),
                MissingPolicy::Preserve | MissingPolicy::NullCompatible => {
                    Err(EvaluationError::non_boolean("<missing>"))
                }
            },
        }
    }

    /// Applies one assignment to a private document clone.
    pub fn apply_assignment(
        &self,
        index: usize,
        assignment: &SetAssignment,
        document: &Document,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<Document> {
        session.charge()?;

        let expression = self
            .model
            .assignment_expression(assignment)
            .map_err(|error| {
                assignment_error(self.model.assignment_field(assignment), index, error)
            })?;

        let value = self
            .evaluate(expression, document, session)
            .map_err(|error| {
                assignment_error(self.model.assignment_field(assignment), index, error)
            })?;

        self.model
            .assign(assignment, value, document)
            .map_err(|error| {
                assignment_error(self.model.assignment_field(assignment), index, error)
            })
    }

    /// Creates the closure-backed native semantics consumed by
    /// [`NativeEvaluator`].
    #[must_use]
    pub fn into_native_functions(self) -> NativeSemanticFunctions {
        self.into_native_functions_with_options(NativeSemanticOptions::default())
    }

    /// Creates closure-backed native semantics with explicit adapter options.
    #[must_use]
    pub fn into_native_functions_with_options(
        self,
        options: NativeSemanticOptions,
    ) -> NativeSemanticFunctions {
        let predicate_semantics = self.clone();
        let assignment_semantics = self;

        NativeSemanticFunctions::with_options(
            move |expression, document, session| {
                predicate_semantics.evaluate_predicate(expression, document, session)
            },
            move |index, assignment, document, session| {
                assignment_semantics.apply_assignment(index, assignment, document, session)
            },
            options,
        )
    }

    /// Builds the production native evaluator with default safety limits.
    #[rustfmt::skip]#[must_use]
    pub fn into_native_evaluator(self) -> NativeEvaluator { self.into_native_functions().into_native_evaluator() }

    /// Builds the production native evaluator with explicit safety limits.
    #[must_use]
    pub fn into_native_evaluator_with_limits(
        self,
        limits: NativeEvaluationLimits,
    ) -> NativeEvaluator {
        self.into_native_functions()
            .into_native_evaluator_with_limits(limits)
    }

    /// Builds the stable evaluator façade with default configuration.
    #[rustfmt::skip]#[must_use]
    pub fn into_evaluator(self) -> Evaluator { self.into_native_evaluator().into_evaluator() }

    /// Builds the base query runtime providing native `filter` and `set`
    /// semantics.
    ///
    /// Document-wide operators such as sorting, projection, grouping, counting,
    /// distinct extraction, and insertion remain configured directly on
    /// [`QueryRuntime`].
    #[rustfmt::skip]#[must_use]
    pub fn into_runtime(self) -> QueryRuntime { self.into_native_evaluator().into_runtime() }

    /// Builds the base query runtime with explicit native evaluation limits.
    #[rustfmt::skip]#[must_use]
    pub fn into_runtime_with_limits(self, limits: NativeEvaluationLimits) -> QueryRuntime { self.into_native_evaluator_with_limits(limits).into_runtime() }

    fn evaluate_node_source(
        &self,
        expression: &Expression,
        source: ExpressionSource<'_, M::Value>,
        session: &mut NativeEvaluationSession<'_>,
    ) -> EvaluationResult<SemanticValue<M::Value>> {
        session.charge()?;

        match self.model.classify(expression)? {
            ExpressionNode::Literal => self.model.literal(expression).map(SemanticValue::Present),

            ExpressionNode::Field => {
                let value = match source {
                    ExpressionSource::Document(document) => {
                        self.model.field(expression, document)?
                    }
                    ExpressionSource::Resolver(resolver) => {
                        let field = expression
                            .ungrouped()
                            .as_field()
                            .expect("field node must expose a field path");
                        resolver.resolve_field(field)
                    }
                };

                if value.is_missing() && session.missing_policy() == MissingPolicy::Error {
                    return Err(EvaluationError::missing_field("<expression field>"));
                }

                Ok(value)
            }

            ExpressionNode::Not { operand } => {
                let value = self.evaluate_predicate_source(operand, source, session)?;
                Ok(SemanticValue::Present(self.boolean_value(!value)?))
            }

            ExpressionNode::And { left, right } => {
                let left = self.evaluate_predicate_source(left, source, session)?;
                if !left {
                    return Ok(SemanticValue::Present(self.boolean_value(false)?));
                }
                let right = self.evaluate_predicate_source(right, source, session)?;
                Ok(SemanticValue::Present(self.boolean_value(right)?))
            }

            ExpressionNode::Or { left, right } => {
                let left = self.evaluate_predicate_source(left, source, session)?;
                if left {
                    return Ok(SemanticValue::Present(self.boolean_value(true)?));
                }
                let right = self.evaluate_predicate_source(right, source, session)?;
                Ok(SemanticValue::Present(self.boolean_value(right)?))
            }

            ExpressionNode::Comparison { left, right } => {
                let left = self.evaluate_source(left, source, session)?;
                let right = self.evaluate_source(right, source, session)?;
                let result =
                    self.model
                        .compare(expression, left, right, session.missing_policy())?;
                Ok(SemanticValue::Present(self.boolean_value(result)?))
            }
        }
    }

    fn boolean_value(&self, value: bool) -> EvaluationResult<M::Value> {
        Ok(self.model.boolean_value(value))
    }
}

enum ExpressionSource<'a, V> {
    Document(&'a Document),
    Resolver(&'a dyn ExpressionFieldResolver<V>),
}

impl<'a, V> Copy for ExpressionSource<'a, V> {}

impl<'a, V> Clone for ExpressionSource<'a, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M> fmt::Debug for ExpressionSemantics<M>
where
    M: ExpressionModel,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExpressionSemantics")
            .field("model", &"<expression model>")
            .finish()
    }
}

fn assignment_error(field: Arc<str>, index: usize, error: EvaluationError) -> EvaluationError {
    match error.kind() {
        EvaluationErrorKind::Assignment { .. } => error,
        _ => EvaluationError::new(EvaluationErrorKind::Assignment {
            field,
            message: Arc::from(format!("assignment #{index} failed: {error}")),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_value_presence_helpers_are_consistent() {
        let present = SemanticValue::Present(7_u64);
        let missing: SemanticValue<u64> = SemanticValue::Missing;

        assert!(present.is_present());
        assert!(!present.is_missing());
        assert_eq!(present.as_present(), Some(&7));

        assert!(!missing.is_present());
        assert!(missing.is_missing());
        assert_eq!(missing.as_present(), None);
    }

    #[test]
    fn semantic_value_preserves_missing_during_map() {
        let value: SemanticValue<u64> = SemanticValue::Missing;

        assert_eq!(value.map(|number| number + 1), SemanticValue::Missing);
    }

    #[test]
    fn semantic_value_maps_present_values() {
        let value = SemanticValue::Present(2_u64);

        assert_eq!(value.map(|number| number + 1), SemanticValue::Present(3),);
    }

    #[test]
    fn semantic_value_rejects_missing_as_present() {
        let value: SemanticValue<u64> = SemanticValue::Missing;

        assert!(value.into_present().is_err());
    }

    #[test]
    fn semantic_value_is_cloneable_and_debuggable() {
        fn assert_traits<T: Clone + fmt::Debug>() {}

        assert_traits::<SemanticValue<u64>>();
    }
}
