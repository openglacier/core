#![cfg_attr(rustfmt, rustfmt_skip)]
//! Common expression evaluator shared by every stage.
//!
//! `where`, `set`, `derive`, `select … as`, projected predicates and `lookup`
//! correlation all evaluate expressions through [`evaluate`], over any
//! [`ExpressionFieldResolver`]. Stages only differ by where field values come
//! from ([`DocumentFields`], [`LookupFields`], projected rows), never by which
//! operators or functions they understand.
//!
//! A field that does not exist is *missing*, which is distinct from `null`.
//! Only `exists` and `coalesce` accept missing operands; everywhere else a
//! missing field is an error naming the path.

use std::sync::Arc;

use crate::model::{
    compare, equals, greater_than, greater_than_or_equal, less_than, less_than_or_equal,
    not_equals, parse_number_value, CoercionPolicy, Comparison, Document, Number, Value,
};

use super::{
    BinaryOperator, ExecutionError, ExecutionResult, Expression, ExpressionFieldPath,
    ExpressionView, Literal, QueryRuntime, UnaryOperator,
};

/// Numbers compare across integer and float kinds; nothing else is coerced.
const POLICY: CoercionPolicy = CoercionPolicy::Numeric;

/// Evaluation result; `None` means the expression reads a missing field.
type Eval = ExecutionResult<Option<Value>>;

/// Value of a field reference: present, or missing (distinct from `null`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SemanticValue<V> {
    Present(V),
    Missing,
}

/// Source of field values for expression evaluation (a document, a projected
/// row, the scope of a `lookup`, ...).
pub trait ExpressionFieldResolver<V>: Send + Sync {
    /// Resolves one validated expression field path.
    fn resolve_field(&self, field: &ExpressionFieldPath) -> SemanticValue<V>;
}

/// Resolves fields from one materialized document.
#[derive(Clone, Copy, Debug)]
pub struct DocumentFields<'a>(pub &'a Document);

impl ExpressionFieldResolver<Value> for DocumentFields<'_> {
    fn resolve_field(&self, field: &ExpressionFieldPath) -> SemanticValue<Value> {
        resolve_path(self.0, field.iter())
    }
}

/// Field scope of a `lookup` sub-pipeline.
///
/// `<inner alias>.path` reads the joined document, `<outer alias>.path` reads
/// the document being enriched, and unqualified paths read the joined document.
#[derive(Clone, Copy, Debug)]
pub struct LookupFields<'a> {
    pub inner: &'a Document,
    pub inner_alias: Option<&'a str>,
    pub outer: &'a Document,
    pub outer_alias: Option<&'a str>,
}

impl ExpressionFieldResolver<Value> for LookupFields<'_> {
    fn resolve_field(&self, field: &ExpressionFieldPath) -> SemanticValue<Value> {
        let first = Some(field.first());
        if first == self.inner_alias {
            resolve_path(self.inner, field.iter().skip(1))
        } else if first == self.outer_alias {
            resolve_path(self.outer, field.iter().skip(1))
        } else {
            resolve_path(self.inner, field.iter())
        }
    }
}

fn resolve_path<'s>(document: &Document, mut segments: impl Iterator<Item = &'s str>) -> SemanticValue<Value> {
    let Some(first) = segments.next() else {
        return SemanticValue::Present(Value::object(document.clone()));
    };
    let Some(mut current) = document.get(first) else { return SemanticValue::Missing };
    for segment in segments {
        match current.as_object().and_then(|object| object.get(segment)) {
            Some(value) => current = value,
            None => return SemanticValue::Missing,
        }
    }
    SemanticValue::Present(current.clone())
}

/// Evaluates an expression to a value.
pub fn evaluate(expression: &Expression, fields: &dyn ExpressionFieldResolver<Value>) -> ExecutionResult<Value> {
    let value = eval(expression, fields)?;
    present(expression, value)
}

/// Evaluates an expression that must produce a boolean.
pub fn evaluate_predicate(expression: &Expression, fields: &dyn ExpressionFieldResolver<Value>) -> ExecutionResult<bool> {
    match evaluate(expression, fields)? {
        Value::Bool(value) => Ok(value),
        other => Err(error(format!("predicate {other:?} did not evaluate to a boolean"))),
    }
}

/// Evaluates an expression against one document.
pub fn evaluate_document(expression: &Expression, document: &Document) -> ExecutionResult<Value> {
    evaluate(expression, &DocumentFields(document))
}

/// Builds the production runtime: `where` and `set` evaluate through this
/// module, over documents or any other field resolver.
pub fn value_expression_runtime() -> ExecutionResult<QueryRuntime> {
    Ok(QueryRuntime::new(
        |expression, document| evaluate_predicate(expression, &DocumentFields(document)),
        |assignments, document| {
            let fields = DocumentFields(document);
            let mut result = document.clone();
            for assignment in assignments {
                let field = assignment.field();
                let value = evaluate(assignment.value(), &fields)
                    .map_err(|failure| error(format!("assignment to field \"{field}\" failed: {failure}")))?;
                assign_path(&mut result, &field.iter().collect::<Vec<_>>(), value)?;
            }
            Ok(Arc::new(result))
        },
    )
    .with_resolved_predicate(evaluate_predicate))
}

fn assign_path(document: &mut Document, segments: &[&str], value: Value) -> ExecutionResult<()> {
    let Some((first, rest)) = segments.split_first() else {
        return Err(error("assignment field path is empty"));
    };
    if rest.is_empty() {
        document.insert(*first, value);
        return Ok(());
    }
    let mut child = match document.get(first) {
        Some(Value::Object(object)) => object.as_ref().clone(),
        Some(other) => return Err(error(format!("nested assignment into {first:?} expects an object, found {other:?}"))),
        None => Document::new(),
    };
    assign_path(&mut child, rest, value)?;
    document.insert(*first, Value::object(child));
    Ok(())
}

fn error(message: impl Into<Arc<str>>) -> ExecutionError {
    ExecutionError::evaluation(message)
}

fn present(expression: &Expression, value: Option<Value>) -> ExecutionResult<Value> {
    value.ok_or_else(|| {
        let mut path = None;
        expression.for_each_field(&mut |field| { path.get_or_insert_with(|| field.to_string()); });
        error(format!("field path {:?} is missing", path.unwrap_or_else(|| "<expression>".to_owned())))
    })
}

fn eval(expression: &Expression, fields: &dyn ExpressionFieldResolver<Value>) -> Eval {
    let value = match expression.view() {
        ExpressionView::Literal(literal) => literal_value(literal)?,
        ExpressionView::Field(path) => return Ok(match fields.resolve_field(path) {
            SemanticValue::Present(value) => Some(value),
            SemanticValue::Missing => None,
        }),
        ExpressionView::Group(inner) => return eval(inner, fields),
        ExpressionView::Array(items) => Value::array(items.iter().map(|item| evaluate(item, fields)).collect::<ExecutionResult<Vec<_>>>()?),
        ExpressionView::Unary { operator, operand } => unary(operator, evaluate(operand, fields)?)?,
        ExpressionView::Binary { left, operator: operator @ (BinaryOperator::And | BinaryOperator::Or), right } => {
            let short_circuit = operator == BinaryOperator::Or;
            if boolean(operator.as_str(), &evaluate(left, fields)?)? == short_circuit {
                Value::bool(short_circuit)
            } else {
                Value::bool(boolean(operator.as_str(), &evaluate(right, fields)?)?)
            }
        }
        ExpressionView::Binary { left, operator, right } => binary(operator, &evaluate(left, fields)?, &evaluate(right, fields)?)?,
        ExpressionView::Call { function, arguments } => return call(function, arguments, fields),
    };
    Ok(Some(value))
}

fn literal_value(literal: &Literal) -> ExecutionResult<Value> {
    Ok(match literal {
        Literal::Null => Value::null(),
        Literal::Bool(value) => Value::bool(*value),
        Literal::String(value) => Value::string(Arc::clone(value)),
        Literal::Number(text) => parse_number_value(text)
            .map(Value::Number)
            .map_err(|failure| error(format!("invalid numeric literal {text:?}: {failure}")))?,
        Literal::Json(text) => super::json_value::parse_json_literal(text)?,
    })
}

fn boolean(context: &str, value: &Value) -> ExecutionResult<bool> {
    value.as_bool().ok_or_else(|| error(format!("`{context}` expects a boolean, found {value:?}")))
}

fn unary(operator: UnaryOperator, value: Value) -> ExecutionResult<Value> {
    match operator {
        UnaryOperator::Not => Ok(Value::bool(!boolean("not", &value)?)),
        UnaryOperator::Positive => number(operator.as_str(), &value).map(Value::Number),
        UnaryOperator::Negate => match number(operator.as_str(), &value)? {
            Number::Float(value) => float(-value),
            integer => match integer_of(integer).and_then(i64::checked_neg) {
                Some(value) => Ok(Value::signed(value)),
                None => float(-float_of(integer)),
            },
        },
    }
}

fn binary(operator: BinaryOperator, left: &Value, right: &Value) -> ExecutionResult<Value> {
    let compared = match operator {
        BinaryOperator::Equal => equals(left, right, POLICY),
        BinaryOperator::NotEqual => not_equals(left, right, POLICY),
        BinaryOperator::LessThan => less_than(left, right, POLICY),
        BinaryOperator::LessThanOrEqual => less_than_or_equal(left, right, POLICY),
        BinaryOperator::GreaterThan => greater_than(left, right, POLICY),
        BinaryOperator::GreaterThanOrEqual => greater_than_or_equal(left, right, POLICY),
        BinaryOperator::In | BinaryOperator::NotIn => {
            let items = right.as_array().ok_or_else(|| error(format!("`{operator}` expects an array on its right, found {right:?}")))?;
            let found = items.iter().any(|item| equals(left, item, POLICY).unwrap_or(false));
            return Ok(Value::bool(found == (operator == BinaryOperator::In)));
        }
        BinaryOperator::And | BinaryOperator::Or => unreachable!("boolean operators short-circuit in eval"),
        arithmetic => return arithmetic_value(arithmetic, left, right),
    };
    compared
        .map(Value::bool)
        .map_err(|failure| error(format!("operation {:?} is incompatible with values {left:?} and {right:?}: {failure}", operator.as_str())))
}

/// Integer arithmetic stays integral while it fits in i64; `/` and anything
/// involving a float produce a float.
fn arithmetic_value(operator: BinaryOperator, left: &Value, right: &Value) -> ExecutionResult<Value> {
    let (Some(a), Some(b)) = (left.as_number().copied(), right.as_number().copied()) else {
        return Err(error(format!("operator {operator} expects numbers, found {left:?} and {right:?}")));
    };
    if matches!(operator, BinaryOperator::Divide | BinaryOperator::Remainder) && float_of(b) == 0.0 {
        return Err(error("division by zero; use div(a, b, fallback) for a safe division"));
    }
    if let (Some(x), Some(y), false) = (integer_of(a), integer_of(b), operator == BinaryOperator::Divide) {
        let result = match operator {
            BinaryOperator::Add => x.checked_add(y),
            BinaryOperator::Subtract => x.checked_sub(y),
            BinaryOperator::Multiply => x.checked_mul(y),
            _ => x.checked_rem(y),
        };
        if let Some(result) = result {
            return Ok(Value::signed(result));
        }
    }
    let (x, y) = (float_of(a), float_of(b));
    float(match operator {
        BinaryOperator::Add => x + y,
        BinaryOperator::Subtract => x - y,
        BinaryOperator::Multiply => x * y,
        BinaryOperator::Divide => x / y,
        _ => x % y,
    })
}

fn number(context: &str, value: &Value) -> ExecutionResult<Number> {
    value.as_number().copied().ok_or_else(|| error(format!("`{context}` expects a number, found {value:?}")))
}

fn text<'a>(context: &str, value: &'a Value) -> ExecutionResult<&'a str> {
    value.as_str().ok_or_else(|| error(format!("`{context}` expects a string, found {value:?}")))
}

fn integer_of(number: Number) -> Option<i64> {
    match number {
        Number::Signed(value) => Some(value),
        Number::Unsigned(value) => i64::try_from(value).ok(),
        Number::Float(_) => None,
    }
}

fn float_of(number: Number) -> f64 {
    match number {
        Number::Signed(value) => value as f64,
        Number::Unsigned(value) => value as f64,
        Number::Float(value) => value,
    }
}

fn float(value: f64) -> ExecutionResult<Value> {
    Value::float(value).map_err(|failure| error(failure.to_string()))
}

/// Returns an integer when a rounded float fits exactly, otherwise the float.
fn integral(value: f64) -> ExecutionResult<Value> {
    if value.is_finite() && value.abs() < 9.0e15 {
        Ok(Value::signed(value as i64))
    } else {
        float(value)
    }
}

/// Built-in functions: name, minimum and maximum argument count (`None` = variadic).
const FUNCTIONS: &[(&str, usize, Option<usize>)] = &[
    ("abs", 1, Some(1)), ("sqrt", 1, Some(1)), ("floor", 1, Some(1)), ("ceil", 1, Some(1)),
    ("round", 1, Some(2)), ("pow", 2, Some(2)), ("min", 1, None), ("max", 1, None),
    ("div", 2, Some(3)), ("coalesce", 1, None), ("if", 3, Some(3)), ("exists", 1, Some(1)),
    ("len", 1, Some(1)), ("lower", 1, Some(1)), ("upper", 1, Some(1)), ("trim", 1, Some(1)),
    ("concat", 1, None), ("contains", 2, Some(2)), ("starts_with", 2, Some(2)),
    ("ends_with", 2, Some(2)), ("type", 1, Some(1)),
];

/// Validates a function name and argument count at parse time.
pub fn check_call(name: &str, arguments: usize) -> Result<(), String> {
    let Some((_, minimum, maximum)) = FUNCTIONS.iter().find(|(function, ..)| *function == name) else {
        let known = FUNCTIONS.iter().map(|(function, ..)| *function).collect::<Vec<_>>().join(", ");
        return Err(format!("unknown function `{name}`; available functions: {known}"));
    };
    if arguments < *minimum || maximum.is_some_and(|maximum| arguments > maximum) {
        let expected = match maximum {
            Some(maximum) if maximum == minimum => format!("{minimum}"),
            Some(maximum) => format!("{minimum} to {maximum}"),
            None => format!("at least {minimum}"),
        };
        return Err(format!("function `{name}` expects {expected} argument(s), got {arguments}"));
    }
    Ok(())
}

fn call(name: &str, arguments: &[Expression], fields: &dyn ExpressionFieldResolver<Value>) -> Eval {
    let argument = |index: usize| evaluate(&arguments[index], fields);
    let value = match name {
        // Functions that decide which arguments to evaluate, or accept missing ones.
        "coalesce" => {
            for expression in arguments {
                if let Some(value) = eval(expression, fields)?.filter(|value| !value.is_null()) {
                    return Ok(Some(value));
                }
            }
            Value::null()
        }
        "exists" => Value::bool(eval(&arguments[0], fields)?.is_some()),
        "if" => {
            let condition = boolean("if", &argument(0)?)?;
            return eval(&arguments[if condition { 1 } else { 2 }], fields);
        }
        "div" => {
            let (dividend, divisor) = (argument(0)?, argument(1)?);
            if number("div", &divisor).is_ok_and(|divisor| float_of(divisor) == 0.0) {
                return match arguments.get(2) {
                    Some(fallback) => eval(fallback, fields),
                    None => Ok(Some(Value::null())),
                };
            }
            arithmetic_value(BinaryOperator::Divide, &dividend, &divisor)?
        }
        // Numeric functions.
        "abs" => match number(name, &argument(0)?)? {
            Number::Float(value) => float(value.abs())?,
            integer => integer_of(integer).and_then(i64::checked_abs).map_or_else(|| float(float_of(integer).abs()), |value| Ok(Value::signed(value)))?,
        },
        "sqrt" => {
            let value = float_of(number(name, &argument(0)?)?);
            if value < 0.0 {
                return Err(error(format!("`sqrt` expects a non-negative number, found {value}")));
            }
            float(value.sqrt())?
        }
        "floor" | "ceil" | "round" => {
            let value = number(name, &argument(0)?)?;
            let digits = match arguments.get(1) {
                Some(expression) => integer_of(number(name, &evaluate(expression, fields)?)?)
                    .filter(|digits| (0..=15).contains(digits))
                    .ok_or_else(|| error("`round` digits must be an integer between 0 and 15"))?,
                None => 0,
            };
            if value.is_integer() {
                Value::Number(value)
            } else {
                let scale = 10_f64.powi(digits as i32);
                let scaled = float_of(value) * scale;
                let rounded = match name { "floor" => scaled.floor(), "ceil" => scaled.ceil(), _ => scaled.round() };
                if digits == 0 { integral(rounded)? } else { float(rounded / scale)? }
            }
        }
        "pow" => {
            let (base, exponent) = (number(name, &argument(0)?)?, number(name, &argument(1)?)?);
            match (integer_of(base), integer_of(exponent).and_then(|exponent| u32::try_from(exponent).ok())) {
                (Some(base), Some(exponent)) if base.checked_pow(exponent).is_some() => Value::signed(base.pow(exponent)),
                _ => float(float_of(base).powf(float_of(exponent)))?,
            }
        }
        "min" | "max" => {
            let values = arguments.iter().map(|expression| evaluate(expression, fields)).collect::<ExecutionResult<Vec<_>>>()?;
            // One array argument aggregates its elements: `max(scores)`.
            let values = match values.as_slice() {
                [Value::Array(items)] => items.to_vec(),
                _ => values,
            };
            let wanted = if name == "min" { Comparison::Less } else { Comparison::Greater };
            let mut best: Option<Value> = None;
            for value in values {
                let replace = match &best {
                    None => true,
                    Some(current) => compare(&value, current, POLICY)
                        .map_err(|failure| error(format!("`{name}` cannot compare {value:?} and {current:?}: {failure}")))?
                        == wanted,
                };
                if replace {
                    best = Some(value);
                }
            }
            best.unwrap_or_else(Value::null)
        }
        // Text and collection functions.
        "len" => {
            let value = argument(0)?;
            let length = match &value {
                Value::String(text) => text.chars().count(),
                Value::Array(items) => items.len(),
                Value::Object(object) => object.len(),
                other => return Err(error(format!("`len` expects a string, array or object, found {other:?}"))),
            };
            Value::unsigned(length as u64)
        }
        "lower" => Value::string(text(name, &argument(0)?)?.to_lowercase()),
        "upper" => Value::string(text(name, &argument(0)?)?.to_uppercase()),
        "trim" => Value::string(text(name, &argument(0)?)?.trim()),
        "concat" => {
            let values = arguments.iter().map(|expression| evaluate(expression, fields)).collect::<ExecutionResult<Vec<_>>>()?;
            if values.iter().all(Value::is_array) {
                Value::array(values.iter().flat_map(|value| value.as_array().unwrap_or_default().iter().cloned()))
            } else {
                let mut output = String::new();
                for value in &values {
                    match value {
                        Value::String(text) => output.push_str(text),
                        Value::Null => {}
                        Value::Bool(_) | Value::Number(_) => output.push_str(&crate::helpers::value_to_json(value).to_string()),
                        other => return Err(error(format!("`concat` cannot join {other:?} with text"))),
                    }
                }
                Value::string(output)
            }
        }
        "contains" => {
            let (haystack, needle) = (argument(0)?, argument(1)?);
            match &haystack {
                Value::Array(items) => Value::bool(items.iter().any(|item| equals(item, &needle, POLICY).unwrap_or(false))),
                _ => Value::bool(text(name, &haystack)?.contains(text(name, &needle)?)),
            }
        }
        "starts_with" => Value::bool(text(name, &argument(0)?)?.starts_with(text(name, &argument(1)?)?)),
        "ends_with" => Value::bool(text(name, &argument(0)?)?.ends_with(text(name, &argument(1)?)?)),
        "type" => Value::string(argument(0)?.physical_kind().as_str()),
        other => return Err(error(format!("unknown function `{other}`"))),
    };
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parse_expression;

    fn document() -> Document {
        Document::from_fields([
            ("price", Value::float(12.5).unwrap()), ("qty", Value::signed(3)), ("zero", Value::signed(0)),
            ("name", Value::string(" Ada ")), ("tags", Value::array([Value::string("a"), Value::string("b")])),
            ("scores", Value::array([Value::signed(4), Value::signed(9), Value::signed(2)])), ("nothing", Value::null()),
        ])
    }
    fn eval_text(source: &str) -> ExecutionResult<Value> { evaluate_document(&parse_expression(source).unwrap(), &document()) }
    fn value(source: &str) -> Value { eval_text(source).unwrap_or_else(|failure| panic!("{source}: {failure}")) }

    #[test] fn arithmetic_keeps_integers_and_divides_to_float() { assert_eq!(value("qty * 2 + 1"), Value::signed(7)); assert_eq!(value("qty / 2"), Value::float(1.5).unwrap()); assert_eq!(value("price * qty"), Value::float(37.5).unwrap()); assert_eq!(value("-qty % 2"), Value::signed(-1)); assert!(eval_text("qty / zero").is_err()); }
    #[test] fn comparisons_and_boolean_logic_share_one_evaluator() { assert_eq!(value("qty * 2 > 5 and not (price < 1)"), Value::bool(true)); assert_eq!(value("qty == 3.0 || missing_field"), Value::bool(true)); assert!(eval_text("qty == \"3\"").is_err()); }
    #[test] fn membership_accepts_literal_and_field_arrays() { assert_eq!(value("qty in [1, 2, 3]"), Value::bool(true)); assert_eq!(value("\"b\" in tags"), Value::bool(true)); assert_eq!(value("\"z\" not in tags"), Value::bool(true)); assert_eq!(value("qty + 1 in [qty, qty * 2]"), Value::bool(false)); }
    #[test] fn functions_are_chainable() { assert_eq!(value("round(price * qty / 7, 2)"), Value::float(5.36).unwrap()); assert_eq!(value("abs(-qty)"), Value::signed(3)); assert_eq!(value("sqrt(pow(qty, 2) + 16)"), Value::float(5.0).unwrap()); assert_eq!(value("max(scores)"), Value::signed(9)); assert_eq!(value("min(qty, 1, price)"), Value::signed(1)); assert_eq!(value("upper(trim(name))"), Value::string("ADA")); assert_eq!(value("len(concat(tags, [\"c\"]))"), Value::unsigned(3)); assert_eq!(value("if(qty > 2, \"many\", \"few\")"), Value::string("many")); assert_eq!(value("floor(price)"), Value::signed(12)); assert_eq!(value("concat(\"n=\", qty)"), Value::string("n=3")); }
    #[test] fn safe_division_and_missing_fields() { assert_eq!(value("div(qty, zero, 0)"), Value::signed(0)); assert_eq!(value("div(qty, zero)"), Value::null()); assert_eq!(value("div(qty, 2, 0)"), Value::float(1.5).unwrap()); assert_eq!(value("coalesce(missing, nothing, qty)"), Value::signed(3)); assert_eq!(value("exists(missing)"), Value::bool(false)); assert_eq!(value("if(exists(missing), missing, 0)"), Value::signed(0)); let failure = eval_text("missing + 1").unwrap_err(); assert!(failure.to_string().contains("\"missing\" is missing"), "{failure}"); }
    #[test] fn calls_are_validated_at_parse_time() { assert!(parse_expression("nope(1)").unwrap_err().to_string().contains("unknown function")); assert!(parse_expression("abs(1, 2)").unwrap_err().to_string().contains("expects 1 argument")); assert!(check_call("coalesce", 5).is_ok()); }
    #[test] fn lookup_scope_routes_aliases() { let inner = Document::from_fields([("user", Value::string("Ada"))]); let outer = Document::from_fields([("name", Value::string("Ada"))]); let fields = LookupFields { inner: &inner, inner_alias: Some("o"), outer: &outer, outer_alias: Some("u") }; assert!(evaluate_predicate(&parse_expression("o.user == u.name").unwrap(), &fields).unwrap()); assert!(evaluate_predicate(&parse_expression("user == u.name").unwrap(), &fields).unwrap()); }
}
