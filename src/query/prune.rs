#![cfg_attr(rustfmt, rustfmt_skip)]
//! Row groups a scan may leave out, from the facts the storage keeps per
//! group ([`crate::storage::GroupFacts`]).
//!
//! A group is left out when, for every row it may hold, the leading filters
//! of the plan drop the row without failing: the conjuncts before one that
//! is false on the whole group are proven total there ([`total_kinds`]),
//! and that one is total too. Rows left out are therefore rows the written
//! order would have dropped, with no error lost.

use std::sync::Arc;

use crate::model::{PhysicalKind, Value};
use crate::storage::GroupFacts;
use crate::FieldPath;

use super::eval::binary;
use super::totality::{total_kinds, Kinds};
use super::{BinaryOperator, Expression, ExpressionFieldPath, ExpressionView, PhysicalOperator};

/// The conjuncts of a plan's leading filters, over the fields a scan reads.
#[derive(Debug)]
pub struct GroupPruning {
    /// In evaluation order.
    conjuncts: Vec<Expression>,
    /// Segments of each scanned field.
    fields: Vec<Box<[Arc<str>]>>,
}

impl GroupPruning {
    /// `None` when no leading filter compares a field with a constant.
    #[must_use]
    pub fn new(operators: &[PhysicalOperator], fields: &[FieldPath]) -> Option<Self> {
        let mut conjuncts = Vec::new();
        for operator in operators {
            match operator {
                PhysicalOperator::Filter { predicate } => flatten(predicate, &mut conjuncts),
                _ => break,
            }
        }
        if !conjuncts.iter().any(|conjunct| comparison(conjunct).is_some()) {
            return None;
        }
        let fields = fields.iter().map(|field| field.as_segments().iter().map(|segment| Arc::from(segment.as_str())).collect()).collect();
        Some(Self { conjuncts, fields })
    }

    /// Whether every row of a group with these facts (one per scanned
    /// field, `None`: unknown) is dropped by the leading filters, none
    /// failing.
    #[must_use]
    pub fn skip(&self, facts: &[Option<GroupFacts<'_>>]) -> bool {
        let facts_of = |path: &ExpressionFieldPath| {
            let index = self.fields.iter().position(|field| field.len() == path.len() && field.iter().map(AsRef::as_ref).eq(path.iter()))?;
            facts.get(index)?.as_ref()
        };
        let kinds = |path: &ExpressionFieldPath| facts_of(path).filter(|facts| !facts.missing).map(|facts| Kinds::from_kinds(&facts.kinds).with_bounds(facts.bounds));
        for (index, conjunct) in self.conjuncts.iter().enumerate() {
            // A scope filter never fails ([`super::scope`]): its guard takes
            // any value, its equality only runs on strings. A group whose
            // field holds no string, or none that can equal the scope's
            // value, has no row in the scope.
            if super::scope::type_guard(conjunct).is_some() {
                continue;
            }
            if let Some((path, value)) = super::scope::optional_equality(conjunct) {
                if index > 0 && super::scope::type_guard(&self.conjuncts[index - 1]) == Some(path) {
                    if path.len() == 1 && facts_of(path).is_some_and(|facts| never_scoped(facts, value)) {
                        return true;
                    }
                    continue;
                }
            }
            if total_kinds(conjunct, &kinds) != Some(Kinds::of(PhysicalKind::Bool)) {
                return false;
            }
            if let Some((path, test)) = comparison(conjunct) {
                if facts_of(path).is_some_and(|facts| never_true(&test, facts)) {
                    return true;
                }
            }
        }
        false
    }
}

fn flatten(expression: &Expression, out: &mut Vec<Expression>) {
    match expression.view() {
        ExpressionView::Group(inner) => flatten(inner, out),
        ExpressionView::Binary { left, operator: BinaryOperator::And, right } => {
            flatten(left, out);
            flatten(right, out);
        }
        _ => out.push(expression.clone()),
    }
}

/// `field operator constant`, as read from the field's side.
#[derive(Debug)]
enum Test<'e> {
    Compare(BinaryOperator, &'e Value),
    Between(&'e Value, &'e Value),
}

fn field(expression: &Expression) -> Option<&ExpressionFieldPath> {
    match expression.view() {
        ExpressionView::Field(path) => Some(path),
        _ => None,
    }
}

fn comparison(expression: &Expression) -> Option<(&ExpressionFieldPath, Test<'_>)> {
    match expression.view() {
        ExpressionView::Group(inner) => comparison(inner),
        ExpressionView::Binary { left, operator, right } if operator.is_comparison() => {
            if let (Some(path), Some(value)) = (field(left), right.constant()) {
                return Some((path, Test::Compare(operator, value)));
            }
            let mirrored = match operator {
                BinaryOperator::LessThan => BinaryOperator::GreaterThan,
                BinaryOperator::LessThanOrEqual => BinaryOperator::GreaterThanOrEqual,
                BinaryOperator::GreaterThan => BinaryOperator::LessThan,
                BinaryOperator::GreaterThanOrEqual => BinaryOperator::LessThanOrEqual,
                other => other,
            };
            Some((field(right)?, Test::Compare(mirrored, left.constant()?)))
        }
        ExpressionView::Call { function: "between", arguments } if arguments.len() == 3 => {
            Some((field(&arguments[0])?, Test::Between(arguments[1].constant()?, arguments[2].constant()?)))
        }
        _ => None,
    }
}

/// Whether no row of a group with these facts holds the string `value`.
fn never_scoped(facts: &GroupFacts<'_>, value: &str) -> bool {
    if !facts.kinds.contains(&PhysicalKind::String) {
        return true;
    }
    facts.bounds.string.as_ref().is_some_and(|(low, high)| value < low.as_ref() || value > high.as_ref())
}

/// `a < b`, `None` when comparing fails.
fn less(a: &Value, b: &Value) -> Option<bool> {
    match binary(BinaryOperator::LessThan, a, b) {
        Ok(Value::Bool(less)) => Some(less),
        _ => None,
    }
}

/// Whether `test` is false for every value the facts allow. Assumes the
/// test is total on them (checked by the caller).
fn never_true(test: &Test<'_>, facts: &GroupFacts<'_>) -> bool {
    let constant = match test {
        Test::Compare(_, value) | Test::Between(value, _) => value,
    };
    let family = match constant {
        Value::Number(_) => PhysicalKind::Number,
        Value::String(_) => PhysicalKind::String,
        _ => return false,
    };
    // Equality is false across kinds; any other test needs one kind.
    let equality = matches!(test, Test::Compare(BinaryOperator::Equal, _));
    if !equality && facts.kinds.iter().any(|kind| *kind != family) {
        return false;
    }
    let bounds = facts.bounds;
    let mut ranges = Vec::new();
    if facts.kinds.contains(&family) {
        if family == PhysicalKind::Number {
            ranges.extend(bounds.signed.map(|(low, high)| (Value::signed(low), Value::signed(high))));
            ranges.extend(bounds.unsigned.map(|(low, high)| (Value::unsigned(low), Value::unsigned(high))));
            if let Some((low, high)) = bounds.float {
                match (Value::float(low), Value::float(high)) {
                    (Ok(low), Ok(high)) => ranges.push((low, high)),
                    _ => return false,
                }
            }
        } else {
            match &bounds.string {
                Some((low, high)) => ranges.push((Value::String(Arc::clone(low)), Value::String(Arc::clone(high)))),
                None => return false,
            }
        }
        if ranges.is_empty() {
            return false;
        }
    }
    ranges.iter().all(|(low, high)| outside(test, low, high) == Some(true))
}

/// Whether `test` is false for every value within `low..=high` (one kind).
fn outside(test: &Test<'_>, low: &Value, high: &Value) -> Option<bool> {
    match test {
        Test::Compare(BinaryOperator::Equal, value) => Some(less(value, low)? || less(high, value)?),
        Test::Compare(BinaryOperator::LessThan, value) => less(low, value).map(|less| !less),
        Test::Compare(BinaryOperator::LessThanOrEqual, value) => less(value, low),
        Test::Compare(BinaryOperator::GreaterThan, value) => less(value, high).map(|less| !less),
        Test::Compare(BinaryOperator::GreaterThanOrEqual, value) => less(high, value),
        Test::Between(from, to) => Some(less(high, from)? || less(to, low)?),
        Test::Compare(..) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parse_expression;
    use crate::storage::FieldBounds;

    fn pruning(predicates: &[&str]) -> GroupPruning {
        let operators = predicates.iter().map(|predicate| PhysicalOperator::Filter { predicate: parse_expression(predicate).unwrap() }).collect::<Vec<_>>();
        let fields = ["p", "x"].map(|name| FieldPath::parse(name).unwrap());
        GroupPruning::new(&operators, &fields).expect("a comparison")
    }

    fn strings(low: &str, high: &str) -> FieldBounds {
        FieldBounds { string: Some((Arc::from(low), Arc::from(high))), ..FieldBounds::default() }
    }

    fn facts<'a>(missing: bool, kinds: &[PhysicalKind], bounds: &'a FieldBounds) -> Option<GroupFacts<'a>> {
        Some(GroupFacts { missing, kinds: kinds.to_vec(), bounds })
    }

    #[test] fn groups_are_left_out_only_when_every_row_is_dropped_without_error() { use PhysicalKind::{Null, Number, String}; let (period, floats) = (strings("01-2025", "01-2025"), FieldBounds { float: Some((-5.0, 20.0)), ..FieldBounds::default() }); let big = FieldBounds { signed: Some((0, 9_007_199_254_740_993)), ..FieldBounds::default() }; let no_bounds = FieldBounds::default(); let p = |missing, kinds: &[PhysicalKind], bounds| [facts(missing, kinds, bounds), facts(false, &[Number], &floats)]; let cases: &[(&[&str], [Option<GroupFacts<'_>>; 2], bool)] = &[(&["p == \"12-2025\""], p(false, &[String], &period), true), (&["p == \"01-2025\""], p(false, &[String], &period), false), /* A row lacking the field fails the comparison. */ (&["p == \"12-2025\""], p(true, &[String], &period), false), /* `null` and booleans equal no string. */ (&["p == \"12-2025\""], p(false, &[String, Null], &period), true), /* A number against a string goes through a coercion that may fail. */ (&["p == \"12-2025\""], p(false, &[String, Number], &period), false), (&["\"12-2025\" == p"], p(false, &[String], &period), true), (&["p > \"06-2025\""], p(false, &[String], &period), true), (&["p < \"06-2025\""], p(false, &[String], &period), false), (&["p != \"12-2025\""], p(false, &[String], &period), false), /* Ordering needs one kind. */ (&["p > \"06-2025\""], p(false, &[String, Null], &period), false), /* Unknown string bounds. */ (&["p == \"12-2025\""], p(false, &[String], &no_bounds), false), (&["x > 100"], p(false, &[String], &period), true), (&["x >= 20"], p(false, &[String], &period), false), (&["x between 21 and 30"], p(false, &[String], &period), true), (&["x between 0 and 30"], p(false, &[String], &period), false), (&["100 < x"], p(false, &[String], &period), true), /* Conjuncts in order: one before must be total on the group. */ (&["p == \"01-2025\" and x > 100"], p(false, &[String], &period), true), (&["p == \"01-2025\"", "x > 100"], p(false, &[String], &period), true), (&["len(p) > 3 and x > 100"], p(false, &[String], &period), false), (&["upper(p) == \"X\"", "x > 100"], p(false, &[String], &period), false), (&["p > \"06-2025\" and x > 100"], p(false, &[String, Null], &period), false), /* Unknown facts end the proof. */ (&["x > 100"], [None, facts(false, &[Number], &floats)], true), (&["p == \"12-2025\""], [None, facts(false, &[Number], &floats)], false)]; for (predicates, facts, skip) in cases { assert_eq!(pruning(predicates).skip(facts), *skip, "{predicates:?}"); } /* Integers beyond 2^53 compared with a float may lose precision. */ assert!(!pruning(&["x > 0.5"]).skip(&[None, facts(false, &[Number], &big)])); let small = FieldBounds { signed: Some((0, 10)), ..FieldBounds::default() }; assert!(!pruning(&["x > 9007199254740993"]).skip(&[None, facts(false, &[Number], &small)])); assert!(pruning(&["x > 1000"]).skip(&[None, facts(false, &[Number], &small)])); /* Nothing to prove false: no pruning at all. */ assert!(GroupPruning::new(&[PhysicalOperator::Filter { predicate: parse_expression("len(p) > 3").unwrap() }], &[FieldPath::parse("p").unwrap()]).is_none()); }
}
