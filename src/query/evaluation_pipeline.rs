//! Expression evaluation pipeline composition.

use std::{fmt, sync::Arc};

pub use crate::error::EvaluationPipelineBuildError;

use super::{
    ExpressionModel, ExpressionSemantics, NativeEvaluationLimits, NativeEvaluator, QueryRuntime,
};

/// Fully assembled native evaluation pipeline.
///
/// The pipeline owns the concrete expression model and can create independent
/// runtimes that share the immutable model through `Arc`.
pub struct EvaluationPipeline<M>
where
    M: ExpressionModel,
{
    model: Arc<M>,
    limits: NativeEvaluationLimits,
}

impl<M> EvaluationPipeline<M>
where
    M: ExpressionModel,
{
    /// Creates a pipeline with default native evaluation limits.
    #[must_use]
    #[inline]
    pub fn new(model: Arc<M>) -> Self {
        Self {
            model,
            limits: NativeEvaluationLimits::default(),
        }
    }

    /// Creates a pipeline from an owned model.
    #[must_use]
    pub fn from_model(model: M) -> Self {
        Self::new(Arc::new(model))
    }

    /// Returns the shared concrete expression model.
    #[must_use]
    pub const fn model(&self) -> &Arc<M> {
        &self.model
    }

    /// Returns the active native evaluation limits.
    #[must_use]
    pub const fn limits(&self) -> NativeEvaluationLimits {
        self.limits
    }

    /// Replaces the native evaluation limits.
    #[must_use]
    pub const fn with_limits(mut self, limits: NativeEvaluationLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Creates a fresh query runtime.
    ///
    /// Every returned runtime receives its own evaluator state while sharing
    /// the immutable expression model.
    #[must_use]
    pub fn runtime(&self) -> QueryRuntime {
        let semantics = ExpressionSemantics::new(Arc::clone(&self.model));

        let native_semantics = semantics.into_native_functions();

        NativeEvaluator::with_limits(Arc::new(native_semantics), self.limits).into_runtime()
    }

    /// Consumes the pipeline and creates a query runtime.
    #[must_use]
    pub fn into_runtime(self) -> QueryRuntime {
        let semantics = ExpressionSemantics::new(self.model);
        let native_semantics = semantics.into_native_functions();

        NativeEvaluator::with_limits(Arc::new(native_semantics), self.limits).into_runtime()
    }
}

impl<M> Clone for EvaluationPipeline<M>
where
    M: ExpressionModel,
{
    fn clone(&self) -> Self {
        Self {
            model: Arc::clone(&self.model),
            limits: self.limits,
        }
    }
}

impl<M> fmt::Debug for EvaluationPipeline<M>
where
    M: ExpressionModel,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EvaluationPipeline")
            .field("model", &"<expression model>")
            .field("limits", &self.limits)
            .finish()
    }
}

/// Builder for [`EvaluationPipeline`].
pub struct EvaluationPipelineBuilder<M>
where
    M: ExpressionModel,
{
    model: Option<Arc<M>>,
    limits: NativeEvaluationLimits,
}

impl<M> EvaluationPipelineBuilder<M>
where
    M: ExpressionModel,
{
    /// Creates an empty pipeline builder.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            model: None,
            limits: NativeEvaluationLimits::default(),
        }
    }

    /// Installs an owned concrete expression model.
    #[must_use]
    pub fn model(mut self, model: M) -> Self {
        self.model = Some(Arc::new(model));
        self
    }

    /// Installs a shared concrete expression model.
    #[must_use]
    pub fn shared_model(mut self, model: Arc<M>) -> Self {
        self.model = Some(model);
        self
    }

    /// Configures native evaluation limits.
    #[must_use]
    pub const fn limits(mut self, limits: NativeEvaluationLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Builds the evaluation pipeline.
    pub fn build(self) -> Result<EvaluationPipeline<M>, EvaluationPipelineBuildError> {
        let model = self
            .model
            .ok_or(EvaluationPipelineBuildError::MissingModel)?;

        Ok(EvaluationPipeline {
            model,
            limits: self.limits,
        })
    }

    /// Builds the pipeline and immediately creates its runtime.
    pub fn build_runtime(self) -> Result<QueryRuntime, EvaluationPipelineBuildError> {
        self.build().map(EvaluationPipeline::into_runtime)
    }
}

impl<M> Default for EvaluationPipelineBuilder<M>
where
    M: ExpressionModel,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<M> fmt::Debug for EvaluationPipelineBuilder<M>
where
    M: ExpressionModel,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EvaluationPipelineBuilder")
            .field("model", &self.model.is_some())
            .field("limits", &self.limits)
            .finish()
    }
}

/// Convenience constructor using default limits.
#[must_use]
pub fn native_evaluation_runtime<M>(model: M) -> QueryRuntime
where
    M: ExpressionModel,
{
    EvaluationPipeline::from_model(model).into_runtime()
}

/// Convenience constructor using explicit limits.
#[must_use]
pub fn native_evaluation_runtime_with_limits<M>(
    model: M,
    limits: NativeEvaluationLimits,
) -> QueryRuntime
where
    M: ExpressionModel,
{
    EvaluationPipeline::from_model(model)
        .with_limits(limits)
        .into_runtime()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_builder_requires_model() {
        struct DummyModel;

        impl ExpressionModel for DummyModel {
            type Value = bool;

            fn classify<'a>(
                &self,
                _expression: &'a super::super::Expression,
            ) -> super::super::EvaluationResult<super::super::ExpressionNode<'a>> {
                unreachable!()
            }

            fn literal(
                &self,
                _expression: &super::super::Expression,
            ) -> super::super::EvaluationResult<Self::Value> {
                unreachable!()
            }

            #[inline]
            fn field(
                &self,
                _expression: &super::super::Expression,
                _document: &crate::Document,
            ) -> super::super::EvaluationResult<super::super::SemanticValue<Self::Value>>
            {
                unreachable!()
            }

            fn strict_boolean(&self, value: &Self::Value) -> super::super::EvaluationResult<bool> {
                Ok(*value)
            }

            fn boolean_value(&self, value: bool) -> Self::Value {
                value
            }

            fn compare(
                &self,
                _expression: &super::super::Expression,
                _left: super::super::SemanticValue<Self::Value>,
                _right: super::super::SemanticValue<Self::Value>,
                _missing_policy: super::super::MissingPolicy,
            ) -> super::super::EvaluationResult<bool> {
                unreachable!()
            }

            fn assignment_expression<'a>(
                &self,
                _assignment: &'a super::super::SetAssignment,
            ) -> super::super::EvaluationResult<&'a super::super::Expression> {
                unreachable!()
            }

            fn assignment_field(&self, _assignment: &super::super::SetAssignment) -> Arc<str> {
                Arc::from("<dummy>")
            }

            fn assign(
                &self,
                _assignment: &super::super::SetAssignment,
                _value: super::super::SemanticValue<Self::Value>,
                _document: &crate::Document,
            ) -> super::super::EvaluationResult<crate::Document> {
                unreachable!()
            }
        }

        let error = EvaluationPipelineBuilder::<DummyModel>::new()
            .build()
            .unwrap_err();

        assert_eq!(error, EvaluationPipelineBuildError::MissingModel,);
    }

    #[test]
    fn build_error_has_actionable_message() {
        assert!(EvaluationPipelineBuildError::MissingModel
            .to_string()
            .contains("expression model"),);
    }

    #[test]
    fn pipeline_types_are_send_and_sync() {
        fn assert_send_and_sync<T: Send + Sync>() {}

        struct DummyModel;

        impl ExpressionModel for DummyModel {
            type Value = bool;

            fn classify<'a>(
                &self,
                _expression: &'a super::super::Expression,
            ) -> super::super::EvaluationResult<super::super::ExpressionNode<'a>> {
                unreachable!()
            }

            fn literal(
                &self,
                _expression: &super::super::Expression,
            ) -> super::super::EvaluationResult<Self::Value> {
                unreachable!()
            }

            #[inline]
            fn field(
                &self,
                _expression: &super::super::Expression,
                _document: &crate::Document,
            ) -> super::super::EvaluationResult<super::super::SemanticValue<Self::Value>>
            {
                unreachable!()
            }

            fn strict_boolean(&self, value: &Self::Value) -> super::super::EvaluationResult<bool> {
                Ok(*value)
            }

            fn boolean_value(&self, value: bool) -> Self::Value {
                value
            }

            fn compare(
                &self,
                _expression: &super::super::Expression,
                _left: super::super::SemanticValue<Self::Value>,
                _right: super::super::SemanticValue<Self::Value>,
                _missing_policy: super::super::MissingPolicy,
            ) -> super::super::EvaluationResult<bool> {
                unreachable!()
            }

            fn assignment_expression<'a>(
                &self,
                _assignment: &'a super::super::SetAssignment,
            ) -> super::super::EvaluationResult<&'a super::super::Expression> {
                unreachable!()
            }

            fn assignment_field(&self, _assignment: &super::super::SetAssignment) -> Arc<str> {
                Arc::from("<dummy>")
            }

            fn assign(
                &self,
                _assignment: &super::super::SetAssignment,
                _value: super::super::SemanticValue<Self::Value>,
                _document: &crate::Document,
            ) -> super::super::EvaluationResult<crate::Document> {
                unreachable!()
            }
        }

        assert_send_and_sync::<EvaluationPipeline<DummyModel>>();
        assert_send_and_sync::<EvaluationPipelineBuilder<DummyModel>>();
    }
}
