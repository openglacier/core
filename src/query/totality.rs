#![cfg_attr(rustfmt, rustfmt_skip)]
//! Planning-time proof that a row-local stage cannot fail on any row.
//!
//! A stage proven total can be moved after a stage that only keeps some of
//! its input rows (`sample n`): no row it would have failed on can be
//! skipped, so errors stay exactly those of the written order.

use crate::model::{PhysicalKind, Value};

use super::{BinaryOperator, Expression, ExpressionFieldPath, ExpressionView, UnaryOperator};

/// Set of physical kinds a value may take, with what is known of its
/// numbers.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Kinds {
    bits: u8,
    number: Option<NumberRange>,
}

/// Every number a value may be: `|x| <= magnitude`; `integer` when one may
/// be an integer; `exact` when every integer it may be converts exactly to a
/// float (within ±2^53); `unsigned` when one may be an unsigned integer.
#[derive(Clone, Copy, Debug, PartialEq)]
struct NumberRange {
    magnitude: f64,
    integer: bool,
    exact: bool,
    unsigned: bool,
}

const EXACT_INTEGER: f64 = 9_007_199_254_740_992.0;

impl NumberRange {
    /// Range of a sum, difference or product, when its result stays finite
    /// with a margin (rounding included).
    fn arithmetic(left: Self, right: Self, operator: BinaryOperator) -> Option<Self> {
        let magnitude = match operator {
            BinaryOperator::Add | BinaryOperator::Subtract => left.magnitude + right.magnitude,
            BinaryOperator::Multiply => left.magnitude * right.magnitude,
            _ => return None,
        } * 2.0;
        // Integer operands give an integer while it fits, a float otherwise.
        let integer = left.integer && right.integer;
        (magnitude.is_finite() && magnitude < f64::MAX).then_some(Self { magnitude, integer, exact: !integer || magnitude <= EXACT_INTEGER, unsigned: false })
    }

    /// Whether comparing two such numbers can never fail.
    const fn comparable(left: Self, right: Self) -> bool {
        left.exact && right.exact && !left.unsigned && !right.unsigned
    }
}

impl Kinds {
    #[must_use]
    pub const fn of(kind: PhysicalKind) -> Self {
        Self {
            bits: match kind {
                PhysicalKind::Null => 1,
                PhysicalKind::Bool => 2,
                PhysicalKind::Number => 4,
                PhysicalKind::String => 8,
                PhysicalKind::Array => 16,
                PhysicalKind::Object => 32,
            },
            number: None,
        }
    }

    #[must_use]
    pub fn from_kinds(kinds: &[PhysicalKind]) -> Self {
        Self { bits: kinds.iter().fold(0, |bits, kind| bits | Self::of(*kind).bits), number: None }
    }

    /// The same kinds, numbers known to lie within `bounds` (see
    /// [`crate::storage::FieldBounds`]).
    #[must_use]
    pub fn with_bounds(self, bounds: &crate::storage::FieldBounds) -> Self {
        if !self.contains(PhysicalKind::Number) {
            return self;
        }
        let mut magnitude = 0.0_f64;
        let mut exact = true;
        if let Some((low, high)) = bounds.signed {
            let largest = low.unsigned_abs().max(high.unsigned_abs());
            magnitude = magnitude.max(largest as f64 * 2.0);
            exact &= largest as f64 <= EXACT_INTEGER && largest <= EXACT_INTEGER as u64;
        }
        if let Some((_, high)) = bounds.unsigned {
            magnitude = magnitude.max(high as f64 * 2.0);
            exact &= high <= EXACT_INTEGER as u64;
        }
        if let Some((low, high)) = bounds.float {
            magnitude = magnitude.max(low.abs()).max(high.abs());
        }
        if bounds.signed.is_none() && bounds.unsigned.is_none() && bounds.float.is_none() {
            return self;
        }
        let integer = bounds.signed.is_some() || bounds.unsigned.is_some();
        Self { number: Some(NumberRange { magnitude, integer, exact, unsigned: bounds.unsigned.is_some() }), ..self }
    }

    fn value(value: &Value) -> Self {
        let kinds = Self::of(value.physical_kind());
        let number = match value {
            Value::Number(crate::Number::Signed(value)) => Some(NumberRange { magnitude: value.unsigned_abs() as f64 * 2.0, integer: true, exact: value.unsigned_abs() <= EXACT_INTEGER as u64, unsigned: false }),
            Value::Number(crate::Number::Unsigned(value)) => Some(NumberRange { magnitude: *value as f64 * 2.0, integer: true, exact: *value <= EXACT_INTEGER as u64, unsigned: true }),
            Value::Number(crate::Number::Float(value)) => Some(NumberRange { magnitude: value.abs(), integer: false, exact: true, unsigned: false }),
            _ => None,
        };
        Self { number, ..kinds }
    }

    const fn contains(self, kind: PhysicalKind) -> bool {
        self.bits & Self::of(kind).bits != 0
    }

    const fn is(self, kind: PhysicalKind) -> bool {
        self.bits == Self::of(kind).bits
    }

    /// Range of the numbers when the value is always a number.
    fn numbers(self) -> Option<NumberRange> {
        self.number.filter(|_| self.is(PhysicalKind::Number))
    }
}

/// Kinds `expression` evaluates to on every row, or `None` when evaluating
/// it may fail on some row. `field` gives the kinds of a field every row
/// holds, `None` when a row may lack it or its kinds are unknown.
///
/// Deliberately narrow: arithmetic may overflow a float and comparing
/// numbers may lose precision, so both are proven total only from number
/// ranges (see [`Kinds::with_bounds`]): `+`, `-`, `*` when the result stays
/// finite, comparisons when every integer converts exactly and none may be
/// unsigned. Division may divide by zero and is never total.
pub fn total_kinds(expression: &Expression, field: &dyn Fn(&ExpressionFieldPath) -> Option<Kinds>) -> Option<Kinds> {
    if let Some(value) = expression.constant() {
        return Some(Kinds::value(value));
    }
    match expression.view() {
        ExpressionView::Literal(_) => None,
        ExpressionView::Field(path) => field(path),
        ExpressionView::Group(inner) => total_kinds(inner, field),
        ExpressionView::Array(items) => {
            for item in items {
                total_kinds(item, field)?;
            }
            Some(Kinds::of(PhysicalKind::Array))
        }
        ExpressionView::Unary { operator: UnaryOperator::Not, operand } => {
            total_kinds(operand, field)?.is(PhysicalKind::Bool).then_some(Kinds::of(PhysicalKind::Bool))
        }
        ExpressionView::Unary { .. } => None,
        ExpressionView::Binary { left, operator, right } => {
            let (left, right) = (total_kinds(left, field)?, total_kinds(right, field)?);
            let boolean = Some(Kinds::of(PhysicalKind::Bool));
            match operator {
                BinaryOperator::And | BinaryOperator::Or => {
                    (left.is(PhysicalKind::Bool) && right.is(PhysicalKind::Bool)).then_some(Kinds::of(PhysicalKind::Bool))
                }
                // `==` answers `false` across kinds, but a number against a
                // number or a string goes through a coercion that may fail.
                _ if operator.is_comparison() && left.numbers().zip(right.numbers()).is_some_and(|(left, right)| NumberRange::comparable(left, right)) => boolean,
                BinaryOperator::Equal | BinaryOperator::NotEqual => {
                    let numeric = |one: Kinds, other: Kinds| {
                        one.contains(PhysicalKind::Number)
                            && (other.contains(PhysicalKind::Number) || other.contains(PhysicalKind::String))
                    };
                    (!numeric(left, right) && !numeric(right, left)).then_some(Kinds::of(PhysicalKind::Bool))
                }
                BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual => {
                    let same = [PhysicalKind::Bool, PhysicalKind::String, PhysicalKind::Null]
                        .into_iter()
                        .any(|kind| left.is(kind) && right.is(kind));
                    if same { boolean } else { None }
                }
                BinaryOperator::In | BinaryOperator::NotIn => {
                    if right.is(PhysicalKind::Array) { boolean } else { None }
                }
                arithmetic => {
                    let range = NumberRange::arithmetic(left.numbers()?, right.numbers()?, arithmetic)?;
                    Some(Kinds { number: Some(range), ..Kinds::of(PhysicalKind::Number) })
                }
            }
        }
        ExpressionView::Call { function: "exists", .. } => Some(Kinds::of(PhysicalKind::Bool)),
        ExpressionView::Call { function: "between", arguments } if arguments.len() == 3 => {
            let ranges = arguments.iter().map(|argument| total_kinds(argument, field)?.numbers()).collect::<Option<Vec<_>>>()?;
            (NumberRange::comparable(ranges[0], ranges[1]) && NumberRange::comparable(ranges[0], ranges[2])).then_some(Kinds::of(PhysicalKind::Bool))
        }
        ExpressionView::Call { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(text: &str) -> Option<Kinds> {
        let expression = crate::query::parse_expression(text).unwrap();
        total_kinds(&expression, &|path| match path.to_string().as_str() {
            "code" => Some(Kinds::of(PhysicalKind::String)),
            "flag" => Some(Kinds::of(PhysicalKind::Bool)),
            "amount" => Some(Kinds::of(PhysicalKind::Number)),
            "maybe" => Some(Kinds::from_kinds(&[PhysicalKind::String, PhysicalKind::Null])),
            _ => None,
        })
    }

    #[test] fn fields_literals_and_string_predicates_are_total() { let boolean = Some(Kinds::of(PhysicalKind::Bool)); assert_eq!(kinds("code"), Some(Kinds::of(PhysicalKind::String))); assert_eq!(kinds("\"x\""), Some(Kinds::of(PhysicalKind::String))); assert_eq!(kinds("code == \"a\" and not flag"), boolean); assert_eq!(kinds("maybe != null or code < \"b\""), boolean); assert_eq!(kinds("code in [\"a\", \"b\"]"), boolean); assert_eq!(kinds("exists(other)"), boolean); assert_eq!(kinds("[code, flag]"), Some(Kinds::of(PhysicalKind::Array))); }
    fn bounded(text: &str, amount: crate::storage::FieldBounds) -> Option<Kinds> {
        let expression = crate::query::parse_expression(text).unwrap();
        let number = Kinds::of(PhysicalKind::Number);
        total_kinds(&expression, &|path| match path.to_string().as_str() {
            "amount" => Some(number.with_bounds(&amount)),
            "cost" => Some(number.with_bounds(&crate::storage::FieldBounds { float: Some((-1e6, 1e6)), ..Default::default() })),
            "code" => Some(Kinds::of(PhysicalKind::String)),
            _ => None,
        })
    }

    #[test] fn bounded_numbers_make_arithmetic_and_comparisons_total() { let floats = crate::storage::FieldBounds { float: Some((-5e3, 9e5)), signed: Some((-7, 1_701_051_036)), ..Default::default() }; let boolean = Some(Kinds::of(PhysicalKind::Bool)); assert_eq!(bounded("amount - cost > 10", floats.clone()), boolean); assert_eq!(bounded("amount * cost + 1 <= cost", floats.clone()), boolean); assert_eq!(bounded("amount between 10 and 20", floats.clone()), boolean); assert_eq!(bounded("amount == 3 and code == \"x\"", floats.clone()), boolean); assert!(bounded("amount - cost", floats.clone()).is_some_and(|kinds| kinds.is(PhysicalKind::Number))); assert_eq!(bounded("amount / cost", floats.clone()), None, "division by zero"); assert_eq!(bounded("amount == code", floats.clone()), None, "number against string"); let huge = crate::storage::FieldBounds { float: Some((-1e300, 1e308)), ..Default::default() }; assert_eq!(bounded("amount + amount", huge.clone()), None, "may overflow"); assert_eq!(bounded("amount > 1", huge), boolean, "floats compare exactly"); let wide = crate::storage::FieldBounds { signed: Some((0, 1 << 60)), ..Default::default() }; assert_eq!(bounded("amount > cost", wide), None, "integer beyond 2^53 against a float"); let unsigned = crate::storage::FieldBounds { unsigned: Some((0, 10)), ..Default::default() }; assert_eq!(bounded("amount > 1", unsigned), None, "unsigned against signed"); assert_eq!(bounded("amount > 1", crate::storage::FieldBounds::default()), None, "no bounds"); }

    #[test] fn what_may_fail_is_not_total() { assert_eq!(kinds("amount - 1"), None); assert_eq!(kinds("amount > 1"), None); assert_eq!(kinds("amount == code"), None); assert_eq!(kinds("amount == 1"), None); assert_eq!(kinds("maybe < \"b\""), None); assert_eq!(kinds("other"), None); assert_eq!(kinds("code and flag"), None); assert_eq!(kinds("upper(code)"), None); }
}
