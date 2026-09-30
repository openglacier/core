#![cfg_attr(rustfmt, rustfmt_skip)]
//! Planning-time proof that a row-local stage cannot fail on any row.
//!
//! A stage proven total can be moved after a stage that only keeps some of
//! its input rows (`sample n`): no row it would have failed on can be
//! skipped, so errors stay exactly those of the written order.

use crate::model::{PhysicalKind, Value};

use super::{BinaryOperator, Expression, ExpressionFieldPath, ExpressionView, UnaryOperator};

/// Set of physical kinds a value may take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Kinds(u8);

impl Kinds {
    #[must_use]
    pub const fn of(kind: PhysicalKind) -> Self {
        Self(match kind {
            PhysicalKind::Null => 1,
            PhysicalKind::Bool => 2,
            PhysicalKind::Number => 4,
            PhysicalKind::String => 8,
            PhysicalKind::Array => 16,
            PhysicalKind::Object => 32,
        })
    }

    #[must_use]
    pub fn from_kinds(kinds: &[PhysicalKind]) -> Self {
        Self(kinds.iter().fold(0, |bits, kind| bits | Self::of(*kind).0))
    }

    const fn value(value: &Value) -> Self {
        Self::of(value.physical_kind())
    }

    const fn contains(self, kind: PhysicalKind) -> bool {
        self.0 & Self::of(kind).0 != 0
    }

    const fn is(self, kind: PhysicalKind) -> bool {
        self.0 == Self::of(kind).0
    }
}

/// Kinds `expression` evaluates to on every row, or `None` when evaluating
/// it may fail on some row. `field` gives the kinds of a field every row
/// holds, `None` when a row may lack it or its kinds are unknown.
///
/// Deliberately narrow: arithmetic may overflow a float and comparing
/// numbers may lose precision, so neither is proven total.
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
                _ => None,
            }
        }
        ExpressionView::Call { function: "exists", .. } => Some(Kinds::of(PhysicalKind::Bool)),
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
    #[test] fn what_may_fail_is_not_total() { assert_eq!(kinds("amount - 1"), None); assert_eq!(kinds("amount > 1"), None); assert_eq!(kinds("amount == code"), None); assert_eq!(kinds("amount == 1"), None); assert_eq!(kinds("maybe < \"b\""), None); assert_eq!(kinds("other"), None); assert_eq!(kinds("code and flag"), None); assert_eq!(kinds("upper(code)"), None); }
}
