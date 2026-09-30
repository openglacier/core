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
//! Only `exists`, `coalesce` and `optional` (the `field?` syntax) accept missing
//! operands; everywhere else a missing field is an error naming the path.

use std::sync::Arc;

use crate::model::{
    equals, greater_than, greater_than_or_equal, less_than, less_than_or_equal,
    not_equals, parse_number_value, CoercionPolicy, Document, Number, Value,
};

use super::{
    BinaryOperator, ExecutionError, ExecutionResult, Expression, ExpressionFieldPath,
    ExpressionView, Literal, SetAssignment, UnaryOperator,
};

/// Numbers compare across integer and float kinds; nothing else is coerced.
pub(super) const POLICY: CoercionPolicy = CoercionPolicy::Numeric;

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
    match eval(expression, fields)? {
        Some(value) => Ok(value),
        None => Err(missing(expression)),
    }
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

/// Applies one `set` operator: every assignment evaluates against the
/// document as it was before the operator.
pub fn apply_assignments(assignments: &[SetAssignment], document: &Document) -> ExecutionResult<Arc<Document>> {
    let fields = DocumentFields(document);
    let mut result = document.clone();
    for assignment in assignments {
        let field = assignment.field();
        let value = evaluate(assignment.value(), &fields)
            .map_err(|failure| error(format!("assignment to field \"{field}\" failed: {failure}")))?;
        assign_path(&mut result, &field.iter().collect::<Vec<_>>(), value)?;
    }
    Ok(Arc::new(result))
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

pub(super) fn error(message: impl Into<Arc<str>>) -> ExecutionError {
    ExecutionError::evaluation(message)
}

#[cold]
fn missing(expression: &Expression) -> ExecutionError {
    let mut path = None;
    expression.for_each_field(&mut |field| { path.get_or_insert_with(|| field.to_string()); });
    error(format!("field path {:?} is missing", path.unwrap_or_else(|| "<expression>".to_owned())))
}

/// Whether a function's result changes between calls, so it is never precomputed.
pub(super) fn is_volatile(function: Function) -> bool {
    function == Function::Now
}

/// Resolver of a constant subtree: it reads no field.
struct NoFields;

impl ExpressionFieldResolver<Value> for NoFields {
    fn resolve_field(&self, _field: &ExpressionFieldPath) -> SemanticValue<Value> {
        SemanticValue::Missing
    }
}

/// Value of a subtree that reads no field, or `None` when evaluating it fails.
pub(super) fn evaluate_constant(expression: &Expression) -> Option<Value> {
    eval(expression, &NoFields).ok().flatten()
}

/// Evaluates an expression; `None` when it reads a missing field (only an
/// error for callers that need a value).
pub(super) fn evaluate_optional(expression: &Expression, fields: &dyn ExpressionFieldResolver<Value>) -> Eval {
    eval(expression, fields)
}

fn eval(expression: &Expression, fields: &dyn ExpressionFieldResolver<Value>) -> Eval {
    if let Some(value) = expression.constant() {
        return Ok(Some(value.clone()));
    }
    let value = match expression.view() {
        ExpressionView::Literal(literal) => literal_value(literal)?,
        ExpressionView::Field(path) => return Ok(match fields.resolve_field(path) {
            SemanticValue::Present(value) => Some(value),
            SemanticValue::Missing => None,
        }),
        ExpressionView::Group(inner) => return eval(inner, fields),
        ExpressionView::Array(items) => Value::array(items.iter().map(|item| evaluate(item, fields)).collect::<ExecutionResult<Vec<_>>>()?),
        ExpressionView::Unary { operator, operand } => unary(operator, &evaluate(operand, fields)?)?,
        ExpressionView::Binary { left, operator: operator @ (BinaryOperator::And | BinaryOperator::Or), right } => {
            let short_circuit = operator == BinaryOperator::Or;
            if boolean(operator.as_str(), &evaluate(left, fields)?)? == short_circuit {
                Value::bool(short_circuit)
            } else {
                Value::bool(boolean(operator.as_str(), &evaluate(right, fields)?)?)
            }
        }
        ExpressionView::Binary { left, operator, right } => binary(operator, &evaluate(left, fields)?, &evaluate(right, fields)?)?,
        ExpressionView::Call { arguments, .. } => {
            let function = expression.function().expect("a call node holds its resolved function");
            return call(function, arguments, fields);
        }
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

pub(super) fn boolean(context: &str, value: &Value) -> ExecutionResult<bool> {
    value.as_bool().ok_or_else(|| error(format!("`{context}` expects a boolean, found {value:?}")))
}

pub(super) fn unary(operator: UnaryOperator, value: &Value) -> ExecutionResult<Value> {
    match operator {
        UnaryOperator::Not => Ok(Value::bool(!boolean("not", value)?)),
        UnaryOperator::Positive => number(operator.as_str(), value).map(Value::Number),
        UnaryOperator::Negate => match number(operator.as_str(), value)? {
            Number::Float(value) => float(-value),
            integer => match integer_of(integer).and_then(i64::checked_neg) {
                Some(value) => Ok(Value::signed(value)),
                None => float(-float_of(integer)),
            },
        },
    }
}

pub(super) fn binary(operator: BinaryOperator, left: &Value, right: &Value) -> ExecutionResult<Value> {
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
pub(super) fn arithmetic_value(operator: BinaryOperator, left: &Value, right: &Value) -> ExecutionResult<Value> {
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

pub(super) fn number(context: &str, value: &Value) -> ExecutionResult<Number> {
    value.as_number().copied().ok_or_else(|| error(format!("`{context}` expects a number, found {value:?}")))
}

pub(super) fn text<'a>(context: &str, value: &'a Value) -> ExecutionResult<&'a str> {
    value.as_str().ok_or_else(|| error(format!("`{context}` expects a string, found {value:?}")))
}

pub(super) fn integer_of(number: Number) -> Option<i64> {
    match number {
        Number::Signed(value) => Some(value),
        Number::Unsigned(value) => i64::try_from(value).ok(),
        Number::Float(_) => None,
    }
}

pub(super) fn float_of(number: Number) -> f64 {
    match number {
        Number::Signed(value) => value as f64,
        Number::Unsigned(value) => value as f64,
        Number::Float(value) => value,
    }
}

pub(super) fn float(value: f64) -> ExecutionResult<Value> {
    Value::float(value).map_err(|failure| error(failure.to_string()))
}

/// Returns an integer when a rounded float fits exactly, otherwise the float.
pub(super) fn integral(value: f64) -> ExecutionResult<Value> {
    if value.is_finite() && value.abs() < 9.0e15 {
        Ok(Value::signed(value as i64))
    } else {
        float(value)
    }
}

/// Built-in functions: variant, name, minimum and maximum argument count
/// (`None` = variadic). The parser resolves a call to its [`Function`] once,
/// so evaluation dispatches on the variant, never on the name.
macro_rules! functions {
    ($($variant:ident = $name:literal, $minimum:literal, $maximum:expr;)*) => {
        /// A built-in function, resolved when the expression is parsed.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum Function { $($variant),* }

        impl Function {
            const ALL: &'static [(Self, &'static str, usize, Option<usize>)] = &[$((Self::$variant, $name, $minimum, $maximum)),*];

            /// The function's name in the query language.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $name),* }
            }
        }
    };
}

functions! {
    // Mathematics.
    Abs = "abs", 1, Some(1); Sqrt = "sqrt", 1, Some(1); Floor = "floor", 1, Some(1); Ceil = "ceil", 1, Some(1);
    Round = "round", 1, Some(2); Trunc = "trunc", 1, Some(2); Pow = "pow", 2, Some(2); Min = "min", 1, None; Max = "max", 1, None;
    Sum = "sum", 1, None; Avg = "avg", 1, None; Div = "div", 2, Some(3); Ln = "ln", 1, Some(1); Log10 = "log10", 1, Some(1);
    Log = "log", 2, Some(2); Exp = "exp", 1, Some(1); Sign = "sign", 1, Some(1);
    // Logic and missing values.
    Coalesce = "coalesce", 1, None; If = "if", 3, Some(3); Case = "case", 2, None; Exists = "exists", 1, Some(1);
    Optional = "optional", 1, Some(1); Nullif = "nullif", 2, Some(2); Between = "between", 3, Some(3); Bucket = "bucket", 2, Some(3);
    // Conversions.
    ToInt = "to_int", 1, Some(1); ToFloat = "to_float", 1, Some(1); ToString = "to_string", 1, Some(1); ToBool = "to_bool", 1, Some(1);
    Type = "type", 1, Some(1);
    // Text.
    Len = "len", 1, Some(1); Lower = "lower", 1, Some(1); Upper = "upper", 1, Some(1); Trim = "trim", 1, Some(1);
    Ltrim = "ltrim", 1, Some(1); Rtrim = "rtrim", 1, Some(1); Concat = "concat", 1, None; Contains = "contains", 2, Some(2);
    StartsWith = "starts_with", 2, Some(2); EndsWith = "ends_with", 2, Some(2); Substr = "substr", 2, Some(3); Split = "split", 2, Some(2);
    Replace = "replace", 3, Some(3); IndexOf = "index_of", 2, Some(2); PadLeft = "pad_left", 2, Some(3); PadRight = "pad_right", 2, Some(3);
    // Arrays.
    At = "at", 2, Some(2); Slice = "slice", 2, Some(3); Reverse = "reverse", 1, Some(1); SortArray = "sort_array", 1, Some(2);
    Unique = "unique", 1, Some(1); Flatten = "flatten", 1, Some(1); Range = "range", 2, Some(3);
    // Objects.
    Keys = "keys", 1, Some(1); Values = "values", 1, Some(1); Merge = "merge", 1, None; Get = "get", 2, Some(3); Has = "has", 2, Some(2);
    // Dates.
    Now = "now", 0, Some(0); ParseDate = "parse_date", 1, Some(1); DateTrunc = "date_trunc", 2, Some(2); DateAdd = "date_add", 3, Some(3);
    DateDiff = "date_diff", 3, Some(3); DatePart = "date_part", 2, Some(2); DateFormat = "date_format", 1, Some(2);
}

impl std::fmt::Display for Function {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Validates a function name and argument count at parse time.
pub fn check_call(name: &str, arguments: usize) -> Result<Function, String> {
    let Some((function, _, minimum, maximum)) = Function::ALL.iter().find(|(_, function, ..)| *function == name) else {
        let known = Function::ALL.iter().map(|(_, function, ..)| *function).collect::<Vec<_>>().join(", ");
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
    Ok(*function)
}

/// Calls a function. Only the functions that decide which arguments to
/// evaluate, or accept missing ones, run here; every other function gets its
/// arguments evaluated and runs in [`super::functions::call`], a leaf, so its
/// frame never stacks up across nested calls.
fn call(function: Function, arguments: &[Expression], fields: &dyn ExpressionFieldResolver<Value>) -> Eval {
    let value = match function {
        Function::Coalesce => {
            for expression in arguments {
                if let Some(value) = eval(expression, fields)?.filter(|value| !value.is_null()) {
                    return Ok(Some(value));
                }
            }
            Value::null()
        }
        Function::Exists => Value::bool(eval(&arguments[0], fields)?.is_some()),
        Function::Optional => eval(&arguments[0], fields)?.unwrap_or_else(Value::null),
        Function::Case => {
            // `case(c1, v1, c2, v2, …, [default])`: the first true condition wins.
            for pair in arguments.chunks(2) {
                match pair {
                    [condition, value] => {
                        if boolean("case", &evaluate(condition, fields)?)? {
                            return eval(value, fields);
                        }
                    }
                    [default] => return eval(default, fields),
                    _ => unreachable!("chunks of two"),
                }
            }
            Value::null()
        }
        Function::If => {
            let condition = boolean("if", &evaluate(&arguments[0], fields)?)?;
            return eval(&arguments[if condition { 1 } else { 2 }], fields);
        }
        Function::Div => {
            let (dividend, divisor) = (evaluate(&arguments[0], fields)?, evaluate(&arguments[1], fields)?);
            if number("div", &divisor).is_ok_and(|divisor| float_of(divisor) == 0.0) {
                return match arguments.get(2) {
                    Some(fallback) => eval(fallback, fields),
                    None => Ok(Some(Value::null())),
                };
            }
            arithmetic_value(BinaryOperator::Divide, &dividend, &divisor)?
        }
        eager => call_eager(eager, arguments, fields)?,
    };
    Ok(Some(value))
}

/// Evaluates every argument, then calls the function.
fn call_eager(function: Function, arguments: &[Expression], fields: &dyn ExpressionFieldResolver<Value>) -> ExecutionResult<Value> {
    // Up to four arguments stay on the stack.
    if arguments.len() <= 4 {
        let mut values = [Value::Null, Value::Null, Value::Null, Value::Null];
        for (slot, expression) in values.iter_mut().zip(arguments) {
            *slot = evaluate(expression, fields)?;
        }
        super::functions::call(function, &values[..arguments.len()])
    } else {
        let values = arguments.iter().map(|expression| evaluate(expression, fields)).collect::<ExecutionResult<Vec<_>>>()?;
        super::functions::call(function, &values)
    }
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
    #[test] fn conversions_are_explicit() { assert_eq!(value("to_int(\" 42 \")"), Value::signed(42)); assert_eq!(value("to_int(price)"), Value::signed(12)); assert_eq!(value("to_int(\"-3.9\")"), Value::signed(-3)); assert_eq!(value("to_float(qty)"), Value::float(3.0).unwrap()); assert_eq!(value("to_string(qty)"), Value::string("3")); assert_eq!(value("to_string(nothing)"), Value::null()); assert_eq!(value("to_bool(\"Yes\")"), Value::bool(true)); assert_eq!(value("to_bool(zero)"), Value::bool(false)); assert!(eval_text("to_int(\"abc\")").is_err()); assert!(eval_text("to_bool(\"maybe\")").is_err()); }
    #[test] fn text_functions_count_characters() { assert_eq!(value("substr(\"héllo\", 1, 3)"), Value::string("éll")); assert_eq!(value("substr(\"hello\", -3)"), Value::string("llo")); assert_eq!(value("split(\"a,b,,c\", \",\")"), value("[\"a\", \"b\", \"\", \"c\"]")); assert_eq!(value("replace(\"a-b-c\", \"-\", \"+\")"), Value::string("a+b+c")); assert_eq!(value("index_of(\"héllo\", \"l\")"), Value::signed(2)); assert_eq!(value("index_of(tags, \"z\")"), Value::signed(-1)); assert_eq!(value("pad_left(to_string(qty), 3, \"0\")"), Value::string("003")); assert_eq!(value("pad_right(\"ab\", 4)"), Value::string("ab  ")); assert_eq!(value("ltrim(name)"), Value::string("Ada ")); assert_eq!(value("rtrim(name)"), Value::string(" Ada")); assert!(eval_text("replace(\"a\", \"\", \"b\")").is_err()); }
    #[test] fn array_functions() { assert_eq!(value("at(scores, 0)"), Value::signed(4)); assert_eq!(value("at(scores, -1)"), Value::signed(2)); assert_eq!(value("at(scores, 9)"), Value::null()); assert_eq!(value("slice(scores, 1)"), value("[9, 2]")); assert_eq!(value("slice(scores, -2, 1)"), value("[9]")); assert_eq!(value("reverse(tags)"), value("[\"b\", \"a\"]")); assert_eq!(value("reverse(\"abc\")"), Value::string("cba")); assert_eq!(value("sort_array(scores)"), value("[2, 4, 9]")); assert_eq!(value("sort_array(scores, \"desc\")"), value("[9, 4, 2]")); assert_eq!(value("unique([1, 1.0, 2, 1])"), value("[1, 2]")); assert_eq!(value("flatten([[1, 2], 3, [[4]]])"), value("[1, 2, 3, [4]]")); assert_eq!(value("range(0, 10, 3)"), value("[0, 3, 6, 9]")); assert_eq!(value("range(3, 0, -1)"), value("[3, 2, 1]")); assert_eq!(value("sum(scores)"), Value::signed(15)); assert_eq!(value("sum(qty, price)"), Value::float(15.5).unwrap()); assert_eq!(value("avg(scores)"), Value::float(5.0).unwrap()); assert_eq!(value("avg([])"), Value::null()); assert!(eval_text("range(0, 10000000)").is_err()); assert!(eval_text("sort_array([1, \"a\"])").is_err()); }
    #[test] fn object_functions() { assert_eq!(value("keys({\"b\": 1, \"a\": 2})"), value("[\"a\", \"b\"]")); assert_eq!(value("values({\"a\": 1})"), value("[1]")); assert_eq!(value("merge({\"a\": 1, \"b\": 1}, nothing, {\"b\": 2})"), value("{\"a\": 1, \"b\": 2}")); assert_eq!(value("get({\"display name\": \"Zed\"}, \"display name\")"), Value::string("Zed")); assert_eq!(value("get({}, \"x\", 0)"), Value::signed(0)); assert_eq!(value("has({\"a\": null}, \"a\")"), Value::bool(true)); }
    #[test] fn math_and_logic_helpers() { assert_eq!(value("ln(exp(2))"), Value::float(2.0).unwrap()); assert_eq!(value("log10(1000)"), Value::float(3.0).unwrap()); assert_eq!(value("log(8, 2)"), Value::float(3.0).unwrap()); assert_eq!(value("trunc(-price)"), Value::signed(-12)); assert_eq!(value("trunc(1.2399, 2)"), Value::float(1.23).unwrap()); assert_eq!(value("sign(-qty)"), Value::signed(-1)); assert!(eval_text("ln(0)").is_err()); assert_eq!(value("case(qty > 5, \"big\", qty > 1, \"mid\", \"small\")"), Value::string("mid")); assert_eq!(value("case(qty > 5, \"big\")"), Value::null()); assert_eq!(value("case(false, missing, \"safe\")"), Value::string("safe")); assert_eq!(value("nullif(qty, 3)"), Value::null()); assert_eq!(value("nullif(qty, 4)"), Value::signed(3)); assert_eq!(value("qty between 1 and 3"), Value::bool(true)); assert_eq!(value("qty not between 1 and 2"), Value::bool(true)); assert_eq!(value("bucket(qty, [0, 3, 10])"), Value::signed(3)); assert_eq!(value("bucket(price, [0, 10])"), Value::null()); assert_eq!(value("bucket(price, [0, 10], \"other\")"), Value::string("other")); assert!(eval_text("bucket(qty, [3, 1])").is_err()); }
    #[test] fn bucket_finds_the_range_holding_the_value() { for (source, expected) in [("bucket(qty, [0, 3, 10])", Value::signed(3)), ("bucket(qty, [3, 10])", Value::signed(3)), ("bucket(qty, [0, 3])", Value::null()), ("bucket(qty, [4, 10], -1)", Value::signed(-1)), ("bucket(price, [0, 1.5, 12.5, 13])", Value::float(12.5).unwrap()), ("bucket(name, [\" A\", \"Z\"])", Value::string(" A")), ("bucket(name, [0, 10], \"x\")", Value::string("x"))] { assert_eq!(value(source), expected, "{source}"); } assert_eq!(value("bucket(price, [0, 10, 9007199254740993], \"none\")"), Value::string("none"), "a bound not exactly comparable closes no range"); assert_eq!(value("bucket(price, [0, 9007199254740993, 9007199254740995], \"none\")"), Value::string("none")); }
    #[test] fn optional_paths_read_missing_as_null() { assert_eq!(value("missing?"), Value::null()); assert_eq!(value("missing?.deep == \"x\""), Value::bool(false)); assert_eq!(value("coalesce(missing?, qty)"), Value::signed(3)); assert_eq!(value("qty?"), Value::signed(3)); assert!(eval_text("missing.deep == 1").is_err()); }
    #[test] fn date_functions_accept_epoch_millis_and_iso_strings() { assert_eq!(value("parse_date(\"2026-03-09T07:05:03Z\")"), Value::signed(1_773_039_903_000)); assert_eq!(value("date_format(1773039903042)"), Value::string("2026-03-09T07:05:03.042Z")); assert_eq!(value("date_format(date_trunc(\"2026-03-09T07:05:03Z\", \"month\"), \"%F\")"), Value::string("2026-03-01")); assert_eq!(value("date_part(\"2026-03-09\", \"day_of_week\")"), Value::signed(1)); assert_eq!(value("date_format(date_add(\"2026-01-31\", 1, \"month\"), \"%F\")"), Value::string("2026-02-28")); assert_eq!(value("date_diff(\"2026-01-01\", \"2026-03-15\", \"months\")"), Value::signed(2)); assert!(matches!(value("now()"), Value::Number(Number::Signed(millis)) if millis > 1_700_000_000_000)); assert!(eval_text("date_part(1, \"eon\")").is_err()); assert!(eval_text("parse_date(\"not a date\")").is_err()); }
    #[test] fn lookup_scope_routes_aliases() { let inner = Document::from_fields([("user", Value::string("Ada"))]); let outer = Document::from_fields([("name", Value::string("Ada"))]); let fields = LookupFields { inner: &inner, inner_alias: Some("o"), outer: &outer, outer_alias: Some("u") }; assert!(evaluate_predicate(&parse_expression("o.user == u.name").unwrap(), &fields).unwrap()); assert!(evaluate_predicate(&parse_expression("user == u.name").unwrap(), &fields).unwrap()); }
}
