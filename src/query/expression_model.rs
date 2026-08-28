//! Expression model types.

use std::{fmt, sync::Arc};

pub use crate::error::NativeExpressionModelBuildError;

use crate::Document;

use super::{
    EvaluationError, EvaluationResult, Evaluator, Expression, ExpressionModel, ExpressionNode,
    ExpressionSemantics, MissingPolicy, NativeEvaluationLimits, NativeEvaluator,
    NativeSemanticFunctions, NativeSemanticOptions, QueryRuntime, SemanticValue, SetAssignment,
};

type ClassifyFunction =
    dyn for<'a> Fn(&'a Expression) -> EvaluationResult<ExpressionNode<'a>> + Send + Sync;

type LiteralFunction<V> = dyn Fn(&Expression) -> EvaluationResult<V> + Send + Sync;

type FieldFunction<V> =
    dyn Fn(&Expression, &Document) -> EvaluationResult<SemanticValue<V>> + Send + Sync;

type StrictBooleanFunction<V> = dyn Fn(&V) -> EvaluationResult<bool> + Send + Sync;

type BooleanValueFunction<V> = dyn Fn(bool) -> V + Send + Sync;

type CompareFunction<V> = dyn Fn(&Expression, SemanticValue<V>, SemanticValue<V>, MissingPolicy) -> EvaluationResult<bool>
    + Send
    + Sync;

type AssignmentExpressionFunction =
    dyn for<'a> Fn(&'a SetAssignment) -> EvaluationResult<&'a Expression> + Send + Sync;

type AssignmentFieldFunction = dyn Fn(&SetAssignment) -> Arc<str> + Send + Sync;

type AssignFunction<V> =
    dyn Fn(&SetAssignment, SemanticValue<V>, &Document) -> EvaluationResult<Document> + Send + Sync;

/// Function-backed production implementation of [`ExpressionModel`].
pub struct NativeExpressionModel<V>
where
    V: Clone + fmt::Debug + Send + Sync + 'static,
{
    classify: Arc<ClassifyFunction>,
    literal: Arc<LiteralFunction<V>>,
    field: Arc<FieldFunction<V>>,
    strict_boolean: Arc<StrictBooleanFunction<V>>,
    boolean_value: Arc<BooleanValueFunction<V>>,
    compare: Arc<CompareFunction<V>>,
    assignment_expression: Arc<AssignmentExpressionFunction>,
    assignment_field: Arc<AssignmentFieldFunction>,
    assign: Arc<AssignFunction<V>>,
}

impl<V> NativeExpressionModel<V>
where
    V: Clone + fmt::Debug + Send + Sync + 'static,
{
    /// Creates a builder for a native expression model.
    #[must_use]
    pub fn builder() -> NativeExpressionModelBuilder<V> {
        NativeExpressionModelBuilder::new()
    }

    /// Wraps this model in the recursive expression-semantic engine.
    #[must_use]
    pub fn into_expression_semantics(self) -> ExpressionSemantics<Self> {
        ExpressionSemantics::from_model(self)
    }

    /// Creates the closure-backed native semantic functions.
    #[must_use]
    pub fn into_native_functions(self) -> NativeSemanticFunctions {
        self.into_expression_semantics().into_native_functions()
    }

    /// Creates native semantic functions with explicit adapter options.
    #[must_use]
    pub fn into_native_functions_with_options(
        self,
        options: NativeSemanticOptions,
    ) -> NativeSemanticFunctions {
        self.into_expression_semantics()
            .into_native_functions_with_options(options)
    }

    /// Builds the native evaluator with default limits.
    #[must_use]
    pub fn into_native_evaluator(self) -> NativeEvaluator {
        self.into_expression_semantics().into_native_evaluator()
    }

    /// Builds the native evaluator with explicit limits.
    #[must_use]
    pub fn into_native_evaluator_with_limits(
        self,
        limits: NativeEvaluationLimits,
    ) -> NativeEvaluator {
        self.into_expression_semantics()
            .into_native_evaluator_with_limits(limits)
    }

    /// Builds the stable evaluator façade.
    #[must_use]
    pub fn into_evaluator(self) -> Evaluator {
        self.into_expression_semantics().into_evaluator()
    }

    /// Builds the base query runtime.
    ///
    /// The runtime receives predicate and assignment behavior from this model.
    /// Collection-wide operators remain configured directly on [`QueryRuntime`].
    #[must_use]
    pub fn into_runtime(self) -> QueryRuntime {
        self.into_expression_semantics().into_runtime()
    }

    /// Builds the base query runtime with explicit evaluation limits.
    #[must_use]
    pub fn into_runtime_with_limits(self, limits: NativeEvaluationLimits) -> QueryRuntime {
        self.into_expression_semantics()
            .into_runtime_with_limits(limits)
    }
}

impl<V> Clone for NativeExpressionModel<V>
where
    V: Clone + fmt::Debug + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            classify: Arc::clone(&self.classify),
            literal: Arc::clone(&self.literal),
            field: Arc::clone(&self.field),
            strict_boolean: Arc::clone(&self.strict_boolean),
            boolean_value: Arc::clone(&self.boolean_value),
            compare: Arc::clone(&self.compare),
            assignment_expression: Arc::clone(&self.assignment_expression),
            assignment_field: Arc::clone(&self.assignment_field),
            assign: Arc::clone(&self.assign),
        }
    }
}

impl<V> fmt::Debug for NativeExpressionModel<V>
where
    V: Clone + fmt::Debug + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeExpressionModel")
            .field("classify", &"<function>")
            .field("literal", &"<function>")
            .field("field", &"<function>")
            .field("strict_boolean", &"<function>")
            .field("boolean_value", &"<function>")
            .field("compare", &"<function>")
            .field("assignment_expression", &"<function>")
            .field("assignment_field", &"<function>")
            .field("assign", &"<function>")
            .finish()
    }
}

impl<V> ExpressionModel for NativeExpressionModel<V>
where
    V: Clone + fmt::Debug + Send + Sync + 'static,
{
    type Value = V;

    fn classify<'a>(&self, expression: &'a Expression) -> EvaluationResult<ExpressionNode<'a>> {
        (self.classify)(expression)
    }

    fn literal(&self, expression: &Expression) -> EvaluationResult<Self::Value> {
        (self.literal)(expression)
    }

    #[inline]
    fn field(
        &self,
        expression: &Expression,
        document: &Document,
    ) -> EvaluationResult<SemanticValue<Self::Value>> {
        (self.field)(expression, document)
    }

    fn strict_boolean(&self, value: &Self::Value) -> EvaluationResult<bool> {
        (self.strict_boolean)(value)
    }

    fn boolean_value(&self, value: bool) -> Self::Value {
        (self.boolean_value)(value)
    }

    fn compare(
        &self,
        expression: &Expression,
        left: SemanticValue<Self::Value>,
        right: SemanticValue<Self::Value>,
        missing_policy: MissingPolicy,
    ) -> EvaluationResult<bool> {
        (self.compare)(expression, left, right, missing_policy)
    }

    fn assignment_expression<'a>(
        &self,
        assignment: &'a SetAssignment,
    ) -> EvaluationResult<&'a Expression> {
        (self.assignment_expression)(assignment)
    }

    fn assignment_field(&self, assignment: &SetAssignment) -> Arc<str> {
        (self.assignment_field)(assignment)
    }

    fn assign(
        &self,
        assignment: &SetAssignment,
        value: SemanticValue<Self::Value>,
        document: &Document,
    ) -> EvaluationResult<Document> {
        (self.assign)(assignment, value, document)
    }
}

/// Builder for [`NativeExpressionModel`].
#[derive(Clone)]
pub struct NativeExpressionModelBuilder<V>
where
    V: Clone + fmt::Debug + Send + Sync + 'static,
{
    classify: Option<Arc<ClassifyFunction>>,
    literal: Option<Arc<LiteralFunction<V>>>,
    field: Option<Arc<FieldFunction<V>>>,
    strict_boolean: Option<Arc<StrictBooleanFunction<V>>>,
    boolean_value: Option<Arc<BooleanValueFunction<V>>>,
    compare: Option<Arc<CompareFunction<V>>>,
    assignment_expression: Option<Arc<AssignmentExpressionFunction>>,
    assignment_field: Option<Arc<AssignmentFieldFunction>>,
    assign: Option<Arc<AssignFunction<V>>>,
}

impl<V> NativeExpressionModelBuilder<V>
where
    V: Clone + fmt::Debug + Send + Sync + 'static,
{
    /// Creates an empty model builder.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            classify: None,
            literal: None,
            field: None,
            strict_boolean: None,
            boolean_value: None,
            compare: None,
            assignment_expression: None,
            assignment_field: None,
            assign: None,
        }
    }

    /// Installs expression classification.
    #[must_use]
    pub fn classify<F>(mut self, function: F) -> Self
    where
        F: for<'a> Fn(&'a Expression) -> EvaluationResult<ExpressionNode<'a>>
            + Send
            + Sync
            + 'static,
    {
        self.classify = Some(Arc::new(function));
        self
    }

    /// Installs literal extraction.
    #[must_use]
    pub fn literal<F>(mut self, function: F) -> Self
    where
        F: Fn(&Expression) -> EvaluationResult<V> + Send + Sync + 'static,
    {
        self.literal = Some(Arc::new(function));
        self
    }

    /// Installs field resolution.
    #[must_use]
    pub fn field<F>(mut self, function: F) -> Self
    where
        F: Fn(&Expression, &Document) -> EvaluationResult<SemanticValue<V>> + Send + Sync + 'static,
    {
        self.field = Some(Arc::new(function));
        self
    }

    /// Installs strict boolean conversion.
    #[must_use]
    pub fn strict_boolean<F>(mut self, function: F) -> Self
    where
        F: Fn(&V) -> EvaluationResult<bool> + Send + Sync + 'static,
    {
        self.strict_boolean = Some(Arc::new(function));
        self
    }

    /// Installs physical boolean construction.
    #[must_use]
    pub fn boolean_value<F>(mut self, function: F) -> Self
    where
        F: Fn(bool) -> V + Send + Sync + 'static,
    {
        self.boolean_value = Some(Arc::new(function));
        self
    }

    /// Installs comparison and coercion delegation.
    #[must_use]
    pub fn compare<F>(mut self, function: F) -> Self
    where
        F: Fn(
                &Expression,
                SemanticValue<V>,
                SemanticValue<V>,
                MissingPolicy,
            ) -> EvaluationResult<bool>
            + Send
            + Sync
            + 'static,
    {
        self.compare = Some(Arc::new(function));
        self
    }

    /// Installs assignment expression extraction.
    #[must_use]
    pub fn assignment_expression<F>(mut self, function: F) -> Self
    where
        F: for<'a> Fn(&'a SetAssignment) -> EvaluationResult<&'a Expression>
            + Send
            + Sync
            + 'static,
    {
        self.assignment_expression = Some(Arc::new(function));
        self
    }

    /// Installs assignment diagnostic field formatting.
    #[must_use]
    pub fn assignment_field<F>(mut self, function: F) -> Self
    where
        F: Fn(&SetAssignment) -> Arc<str> + Send + Sync + 'static,
    {
        self.assignment_field = Some(Arc::new(function));
        self
    }

    /// Installs assignment publication into a document clone.
    #[must_use]
    pub fn assign<F>(mut self, function: F) -> Self
    where
        F: Fn(&SetAssignment, SemanticValue<V>, &Document) -> EvaluationResult<Document>
            + Send
            + Sync
            + 'static,
    {
        self.assign = Some(Arc::new(function));
        self
    }

    /// Returns whether every required operation has been installed.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.first_missing_operation().is_none()
    }

    /// Returns the first operation still missing from this builder.
    #[must_use]
    pub fn first_missing_operation(&self) -> Option<NativeExpressionModelBuildError> {
        if self.classify.is_none() {
            Some(NativeExpressionModelBuildError::MissingClassify)
        } else if self.literal.is_none() {
            Some(NativeExpressionModelBuildError::MissingLiteral)
        } else if self.field.is_none() {
            Some(NativeExpressionModelBuildError::MissingField)
        } else if self.strict_boolean.is_none() {
            Some(NativeExpressionModelBuildError::MissingStrictBoolean)
        } else if self.boolean_value.is_none() {
            Some(NativeExpressionModelBuildError::MissingBooleanValue)
        } else if self.compare.is_none() {
            Some(NativeExpressionModelBuildError::MissingCompare)
        } else if self.assignment_expression.is_none() {
            Some(NativeExpressionModelBuildError::MissingAssignmentExpression)
        } else if self.assignment_field.is_none() {
            Some(NativeExpressionModelBuildError::MissingAssignmentField)
        } else if self.assign.is_none() {
            Some(NativeExpressionModelBuildError::MissingAssign)
        } else {
            None
        }
    }

    /// Builds and validates the model.
    pub fn build(self) -> Result<NativeExpressionModel<V>, NativeExpressionModelBuildError> {
        Ok(NativeExpressionModel {
            classify: required(
                self.classify,
                NativeExpressionModelBuildError::MissingClassify,
            )?,
            literal: required(
                self.literal,
                NativeExpressionModelBuildError::MissingLiteral,
            )?,
            field: required(self.field, NativeExpressionModelBuildError::MissingField)?,
            strict_boolean: required(
                self.strict_boolean,
                NativeExpressionModelBuildError::MissingStrictBoolean,
            )?,
            boolean_value: required(
                self.boolean_value,
                NativeExpressionModelBuildError::MissingBooleanValue,
            )?,
            compare: required(
                self.compare,
                NativeExpressionModelBuildError::MissingCompare,
            )?,
            assignment_expression: required(
                self.assignment_expression,
                NativeExpressionModelBuildError::MissingAssignmentExpression,
            )?,
            assignment_field: required(
                self.assignment_field,
                NativeExpressionModelBuildError::MissingAssignmentField,
            )?,
            assign: required(self.assign, NativeExpressionModelBuildError::MissingAssign)?,
        })
    }
}

impl<V> Default for NativeExpressionModelBuilder<V>
where
    V: Clone + fmt::Debug + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<V> fmt::Debug for NativeExpressionModelBuilder<V>
where
    V: Clone + fmt::Debug + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeExpressionModelBuilder")
            .field("classify", &self.classify.is_some())
            .field("literal", &self.literal.is_some())
            .field("field", &self.field.is_some())
            .field("strict_boolean", &self.strict_boolean.is_some())
            .field("boolean_value", &self.boolean_value.is_some())
            .field("compare", &self.compare.is_some())
            .field(
                "assignment_expression",
                &self.assignment_expression.is_some(),
            )
            .field("assignment_field", &self.assignment_field.is_some())
            .field("assign", &self.assign.is_some())
            .finish()
    }
}

fn required<T>(
    value: Option<T>,
    error: NativeExpressionModelBuildError,
) -> Result<T, NativeExpressionModelBuildError> {
    value.ok_or(error)
}

/// Converts a model integration failure to a backend evaluation error.
#[must_use]
pub fn model_backend_error(
    operation: impl fmt::Display,
    message: impl fmt::Display,
) -> EvaluationError {
    EvaluationError::backend(format!("expression model {operation} failed: {message}",))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_reports_completeness() {
        let builder = NativeExpressionModelBuilder::<bool>::new();

        assert!(!builder.is_complete());
        assert_eq!(
            builder.first_missing_operation(),
            Some(NativeExpressionModelBuildError::MissingClassify),
        );
    }

    #[test]
    fn builder_is_cloneable() {
        let builder = NativeExpressionModelBuilder::<bool>::new();
        let clone = builder.clone();

        assert_eq!(
            clone.first_missing_operation(),
            Some(NativeExpressionModelBuildError::MissingClassify),
        );
    }

    #[test]
    fn empty_builder_reports_first_missing_operation() {
        let error = NativeExpressionModelBuilder::<bool>::new()
            .build()
            .unwrap_err();

        assert_eq!(error, NativeExpressionModelBuildError::MissingClassify,);
    }

    #[test]
    fn build_error_has_actionable_message() {
        let error = NativeExpressionModelBuildError::MissingStrictBoolean;

        assert_eq!(error.operation(), "strict boolean conversion");
        assert!(error.to_string().contains(error.operation()));
    }

    #[test]
    fn public_types_are_send_and_sync() {
        fn assert_send_and_sync<T: Send + Sync>() {}

        assert_send_and_sync::<NativeExpressionModel<bool>>();
        assert_send_and_sync::<NativeExpressionModelBuilder<bool>>();
        assert_send_and_sync::<NativeExpressionModelBuildError>();
    }
}
