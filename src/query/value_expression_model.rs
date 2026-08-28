//! Value expression model types.

use std::sync::Arc;

use crate::model::{
    equals, greater_than, greater_than_or_equal, less_than, less_than_or_equal, not_equals,
    parse_number_value, CoercionPolicy, Document, Value,
};

use super::{
    BinaryOperator, EvaluationContext, EvaluationError, EvaluationResult, ExecutionError,
    Expression, ExpressionNode, ExpressionView, Literal, MissingPolicy, NativeEvaluationLimits,
    NativeEvaluationSession, NativeExpressionModel, NativeExpressionModelBuildError, QueryRuntime,
    SemanticValue, SetAssignment, UnaryOperator,
};

/// Builds the production expression model for physical [`Value`]s.
pub fn value_expression_model(
) -> Result<NativeExpressionModel<Value>, NativeExpressionModelBuildError> {
    NativeExpressionModel::builder()
        .classify(classify)
        .literal(literal)
        .field(field)
        .strict_boolean(strict_boolean)
        .boolean_value(Value::bool)
        .compare(compare_expression)
        .assignment_expression(|assignment| Ok(assignment.value()))
        .assignment_field(|assignment| Arc::from(assignment.field().to_string()))
        .assign(assign)
        .build()
}

/// Builds the base native runtime providing real `where` and `set` semantics.
pub fn value_expression_runtime() -> EvaluationResult<QueryRuntime> {
    let model =
        value_expression_model().map_err(|error| EvaluationError::backend(error.to_string()))?;
    let semantics = model.into_expression_semantics();
    let resolved_semantics = semantics.clone();
    let runtime = semantics.into_runtime();
    let context = EvaluationContext::default();
    let limits = NativeEvaluationLimits::default();

    Ok(
        runtime.with_resolved_predicate(move |expression, resolver| {
            let mut session = NativeEvaluationSession::new(&context, limits);
            resolved_semantics
                .evaluate_predicate_resolved(expression, resolver, &mut session)
                .map_err(ExecutionError::from)
        }),
    )
}

fn classify(expression: &Expression) -> EvaluationResult<ExpressionNode<'_>> {
    let expression = expression.ungrouped();

    match expression.view() {
        ExpressionView::Literal(_) => Ok(ExpressionNode::Literal),
        ExpressionView::Field(_) => Ok(ExpressionNode::Field),
        ExpressionView::Group(inner) => classify(inner),
        ExpressionView::Unary {
            operator: UnaryOperator::Not,
            operand,
        } => Ok(ExpressionNode::Not { operand }),
        ExpressionView::Unary { operator, .. } => Err(EvaluationError::backend(format!(
            "unary operator {operator} is parsed but not supported by native evaluation"
        ))),
        ExpressionView::Binary {
            left,
            operator: BinaryOperator::And,
            right,
        } => Ok(ExpressionNode::And { left, right }),
        ExpressionView::Binary {
            left,
            operator: BinaryOperator::Or,
            right,
        } => Ok(ExpressionNode::Or { left, right }),
        ExpressionView::Binary {
            left,
            operator,
            right,
        } if operator.is_comparison() => Ok(ExpressionNode::Comparison { left, right }),
        ExpressionView::Binary { operator, .. } => Err(EvaluationError::backend(format!(
            "binary operator {operator} is parsed but not supported by native evaluation"
        ))),
    }
}

fn literal(expression: &Expression) -> EvaluationResult<Value> {
    let Some(literal) = expression.ungrouped().as_literal() else {
        return Err(EvaluationError::backend("expected a literal expression"));
    };

    match literal {
        Literal::Null => Ok(Value::null()),
        Literal::Bool(value) => Ok(Value::bool(*value)),
        Literal::String(value) => Ok(Value::string(Arc::clone(value))),
        Literal::Number(text) => parse_number_value(text)
            .map(Value::Number)
            .map_err(|error| {
                EvaluationError::backend(format!("invalid numeric literal {text:?}: {error}"))
            }),
        Literal::Json(text) => super::json_value::parse_json_literal(text)
            .map_err(|error| EvaluationError::backend(error.to_string())),
    }
}

#[inline]
fn field(expression: &Expression, document: &Document) -> EvaluationResult<SemanticValue<Value>> {
    let Some(path) = expression.ungrouped().as_field() else {
        return Err(EvaluationError::backend("expected a field expression"));
    };

    let mut segments = path.iter();
    let first = segments
        .next()
        .expect("validated expression paths are never empty");

    let Some(mut current) = document.get(first) else {
        return Ok(SemanticValue::Missing);
    };

    for segment in segments {
        let Some(object) = current.as_object() else {
            return Ok(SemanticValue::Missing);
        };
        let Some(value) = object.get(segment) else {
            return Ok(SemanticValue::Missing);
        };
        current = value;
    }

    Ok(SemanticValue::Present(current.clone()))
}

fn strict_boolean(value: &Value) -> EvaluationResult<bool> {
    value
        .as_bool()
        .ok_or_else(|| EvaluationError::non_boolean(format!("{value:?}")))
}

fn compare_expression(
    expression: &Expression,
    left: SemanticValue<Value>,
    right: SemanticValue<Value>,
    missing_policy: MissingPolicy,
) -> EvaluationResult<bool> {
    let operator = match expression.ungrouped().view() {
        ExpressionView::Binary { operator, .. } if operator.is_comparison() => operator,
        _ => return Err(EvaluationError::backend("expected a comparison expression")),
    };

    let (left, right) = normalize_missing(left, right, operator, missing_policy)?;
    let policy = CoercionPolicy::Numeric;

    let result = match operator {
        BinaryOperator::Equal => equals(&left, &right, policy),
        BinaryOperator::NotEqual => not_equals(&left, &right, policy),
        BinaryOperator::LessThan => less_than(&left, &right, policy),
        BinaryOperator::LessThanOrEqual => less_than_or_equal(&left, &right, policy),
        BinaryOperator::GreaterThan => greater_than(&left, &right, policy),
        BinaryOperator::GreaterThanOrEqual => greater_than_or_equal(&left, &right, policy),
        _ => unreachable!("comparison operator checked above"),
    };

    result.map_err(|error| {
        EvaluationError::incompatible_values(
            operator.as_str(),
            format!("{left:?}"),
            format!("{right:?}"),
        )
        .with_backend_context(error)
    })
}

fn normalize_missing(
    left: SemanticValue<Value>,
    right: SemanticValue<Value>,
    operator: BinaryOperator,
    policy: MissingPolicy,
) -> EvaluationResult<(Value, Value)> {
    match (left, right, policy) {
        (SemanticValue::Present(left), SemanticValue::Present(right), _) => Ok((left, right)),
        (SemanticValue::Missing, _, MissingPolicy::Error)
        | (_, SemanticValue::Missing, MissingPolicy::Error) => {
            Err(EvaluationError::missing_field("<comparison operand>"))
        }
        (left, right, MissingPolicy::NullCompatible) => Ok((
            left.into_present().unwrap_or_else(|_| Value::null()),
            right.into_present().unwrap_or_else(|_| Value::null()),
        )),
        (SemanticValue::Missing, SemanticValue::Missing, MissingPolicy::Preserve)
            if operator == BinaryOperator::Equal =>
        {
            Ok((Value::null(), Value::null()))
        }
        (SemanticValue::Missing, SemanticValue::Missing, MissingPolicy::Preserve)
            if operator == BinaryOperator::NotEqual =>
        {
            Ok((Value::bool(false), Value::bool(true)))
        }
        (_, _, MissingPolicy::Preserve) => {
            Err(EvaluationError::missing_field("<comparison operand>"))
        }
    }
}

fn assign(
    assignment: &SetAssignment,
    value: SemanticValue<Value>,
    document: &Document,
) -> EvaluationResult<Document> {
    let value = value.into_present()?;
    let segments = assignment.field().iter().collect::<Vec<_>>();
    let mut result = document.clone();
    assign_path(&mut result, &segments, value)?;
    Ok(result)
}

fn assign_path(document: &mut Document, segments: &[&str], value: Value) -> EvaluationResult<()> {
    let Some((first, rest)) = segments.split_first() else {
        return Err(EvaluationError::backend("assignment field path is empty"));
    };

    if rest.is_empty() {
        document.insert(*first, value);
        return Ok(());
    }

    let mut child = match document.get(first) {
        Some(Value::Object(object)) => object.as_ref().clone(),
        Some(other) => {
            return Err(EvaluationError::incompatible_values(
                "nested assignment",
                format!("{other:?}"),
                "object",
            ));
        }
        None => Document::new(),
    };

    assign_path(&mut child, rest, value)?;
    document.insert(*first, Value::object(child));
    Ok(())
}

trait EvaluationErrorContext {
    fn with_backend_context(self, source: impl std::fmt::Display) -> Self;
}

impl EvaluationErrorContext for EvaluationError {
    fn with_backend_context(self, source: impl std::fmt::Display) -> Self {
        EvaluationError::backend(format!("{self}: {source}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parse_expression;

    #[test]
    fn equality_predicate_filters_values() {
        let evaluator = value_expression_model().unwrap().into_evaluator();
        let expression = parse_expression("a == 2").unwrap();

        let mut matching = Document::new();
        matching.insert("a", Value::signed(2));
        let mut different = Document::new();
        different.insert("a", Value::signed(3));

        assert!(evaluator
            .evaluate_predicate(&expression, &matching)
            .unwrap());
        assert!(!evaluator
            .evaluate_predicate(&expression, &different)
            .unwrap());
    }

    #[test]
    fn nested_field_predicate_is_resolved() {
        let evaluator = value_expression_model().unwrap().into_evaluator();
        let expression = parse_expression("user.age >= 18").unwrap();
        let mut user = Document::new();
        user.insert("age", Value::signed(20));
        let mut document = Document::new();
        document.insert("user", Value::object(user));

        assert!(evaluator
            .evaluate_predicate(&expression, &document)
            .unwrap());
    }

    #[test]
    fn resolved_predicate_uses_native_semantics_without_document_materialization() {
        struct Resolver;

        impl super::super::ExpressionFieldResolver<Value> for Resolver {
            fn resolve_field(
                &self,
                field: &super::super::ExpressionFieldPath,
            ) -> SemanticValue<Value> {
                match field.to_string().as_str() {
                    "a" => SemanticValue::Present(Value::signed(2)),
                    "b" => SemanticValue::Present(Value::signed(3)),
                    _ => SemanticValue::Missing,
                }
            }
        }

        let runtime = value_expression_runtime().unwrap();
        let expression = parse_expression("a == 2 and b > 1").unwrap();
        assert!(super::super::ExecutionRuntime::evaluate_resolved_predicate(
            &runtime,
            &expression,
            &Resolver,
        )
        .unwrap());
    }

    #[test]
    fn non_boolean_predicate_is_rejected() {
        let evaluator = value_expression_model().unwrap().into_evaluator();
        let expression = parse_expression("a").unwrap();
        let mut document = Document::new();
        document.insert("a", Value::signed(2));

        assert!(evaluator
            .evaluate_predicate(&expression, &document)
            .is_err());
    }
}
