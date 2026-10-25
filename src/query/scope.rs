//! A Place/App scope written into a read plan, so scoped reads run on the
//! same executors as any read.
//!
//! The scope is a filter placed first in front of every source the plan
//! reads (the main one, and each `lookup` and `union` pipeline):
//! `type(_place?) == "string" and _place? == "<place>"` (and the same on
//! `_app_instance` for an App instance). It keeps exactly the rows
//! [`super::DocumentScope`] matches, and never fails: `type` takes any
//! value, a missing field reads as `null`, and the equality only runs on
//! strings. Being first, it drops the rows of other scopes before a user
//! stage can read them, or fail on them.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::{parse_expression, BinaryOperator, Expression, ExpressionView, Literal, PhysicalOperator, PhysicalPlan, PhysicalSubPipeline};
use crate::helpers::{APP_INSTANCE_SCOPE_FIELD, PLACE_SCOPE_FIELD};
use crate::Value;
use std::sync::Arc;

/// `plan` restricted to the rows of `place` (and of `app_instance`, when
/// given) in every source it reads; `None` for a plan that writes, which
/// keeps the transactional executor and its scope enforcement.
#[must_use]
pub fn scoped_read_plan(plan: &PhysicalPlan, place: &str, app_instance: Option<&str>) -> Option<PhysicalPlan> {
    scoped_plan(plan, place, app_instance, &[])
}

/// [`scoped_read_plan`] whose sources also keep only the rows whose field,
/// when present, holds one of the values of each of `filters`.
pub(crate) fn scoped_plan(plan: &PhysicalPlan, place: &str, app_instance: Option<&str>, filters: &[(Arc<str>, Arc<[Value]>)]) -> Option<PhysicalPlan> {
    if plan.is_write() {
        return None;
    }
    let scope = Scope { place, app_instance, filters };
    let operators = scoped_operators(plan.operators().to_vec(), None, plan.source().collection().as_str(), &scope)?;
    PhysicalPlan::new(plan.source().clone(), operators).ok()
}

/// The filters the rows of `collection` keep, each with whether a row must
/// hold its field: a filter keyed `<collection>:<field>` is strict on that
/// collection and replaces there the plain `<field>` one, under which a row
/// without the field passes.
pub(crate) fn filters_of<'s>(filters: &'s [(Arc<str>, Arc<[Value]>)], collection: &'s str) -> impl Iterator<Item = (&'s str, &'s [Value], bool)> {
    let strict = |field: &str| filters.iter().any(|(key, _)| key.split_once(':') == Some((collection, field)));
    filters.iter().filter_map(move |(key, values)| match key.split_once(':') {
        Some((name, field)) => (name == collection).then_some((field, &values[..], true)),
        None => (!strict(key)).then_some((&key[..], &values[..], false)),
    })
}

struct Scope<'s> {
    place: &'s str,
    app_instance: Option<&'s str>,
    filters: &'s [(Arc<str>, Arc<[Value]>)],
}

/// `operators` reading `collection`; the constant rows of `values […]`
/// belong to every scope and are not filtered.
fn scoped_operators(operators: Vec<PhysicalOperator>, alias: Option<&str>, collection: &str, scope: &Scope<'_>) -> Option<Vec<PhysicalOperator>> {
    let mut scoped = Vec::with_capacity(operators.len() + 2);
    let predicate = PhysicalOperator::filter(scope_predicate(alias, scope.place, scope.app_instance)?);
    let constant = collection.starts_with(crate::view::VALUES_PREFIX);
    let mut operators = operators.into_iter().peekable();
    // A leading `_id == "…"` stays first, so the source is still read by
    // identifier: it never fails, so the two filters commute.
    if let Some(PhysicalOperator::Filter { predicate }) = operators.peek() {
        if is_identifier_equality(predicate) {
            scoped.push(operators.next().expect("peeked"));
        }
    }
    if !constant {
        scoped.push(predicate);
        // `f? == null or f? in [...]` for each filter, `f? in [...]` for a strict one: it never fails either.
        let path = |field: &str| alias.map_or_else(|| format!("{field}?"), |alias| format!("{alias}.{field}?"));
        let text = filters_of(scope.filters, collection).map(|(field, values, strict)| {
            let values = crate::helpers::value_to_json(&Value::array(values.iter().cloned()));
            if strict { format!("{} in {values}", path(field)) } else { format!("({0} == null or {0} in {values})", path(field)) }
        }).collect::<Vec<_>>();
        if !text.is_empty() {
            scoped.push(PhysicalOperator::filter(parse_expression(&text.join(" and ")).ok()?));
        }
    }
    for operator in operators {
        scoped.push(match operator {
            PhysicalOperator::Lookup { collection, alias, outer_alias, into, pipeline } => {
                // Inside a lookup, a bare field names the joined row unless
                // it is the outer alias: the joined row's own alias, when it
                // has one, cannot be mistaken.
                let inner = alias.as_deref();
                let pipeline = PhysicalSubPipeline::new(scoped_operators(pipeline.operators().to_vec(), inner, collection.as_str(), scope)?).ok()?;
                PhysicalOperator::Lookup { collection, alias, outer_alias, into, pipeline }
            }
            PhysicalOperator::Union { collection, alias, pipeline } => {
                // A union branch reads its rows by bare field names.
                let pipeline = PhysicalSubPipeline::new(scoped_operators(pipeline.operators().to_vec(), None, collection.as_str(), scope)?).ok()?;
                PhysicalOperator::Union { collection, alias, pipeline }
            }
            other => other,
        });
    }
    Some(scoped)
}

/// The scope filter; `alias` qualifies the scope fields.
fn scope_predicate(alias: Option<&str>, place: &str, app_instance: Option<&str>) -> Option<Expression> {
    let mut text = String::new();
    let mut values = vec![(PLACE_SCOPE_FIELD, place)];
    values.extend(app_instance.map(|app_instance| (APP_INSTANCE_SCOPE_FIELD, app_instance)));
    for (field, value) in &values {
        let path = match alias {
            Some(alias) => format!("{alias}.{field}?"),
            None => format!("{field}?"),
        };
        if !text.is_empty() {
            text.push_str(" and ");
        }
        let literal = serde_json::to_string(value).ok()?;
        text.push_str(&format!("type({path}) == \"string\" and {path} == {literal}"));
    }
    let predicate = parse_expression(&text).ok()?;
    // The literals read back must be the scope's values, unchanged.
    let read = scope_restriction(&predicate)?;
    (read.len() == values.len() && read.iter().zip(&values).all(|((field, value), (expected, wanted))| field == expected && value == wanted)).then_some(predicate)
}

/// The (field, value) pairs of a scope filter, in order; `None` for any
/// other predicate.
#[must_use]
pub fn scope_restriction(predicate: &Expression) -> Option<Vec<(String, String)>> {
    let mut conjuncts = Vec::new();
    crate::helpers::collect_conjuncts(predicate, &mut conjuncts);
    if conjuncts.is_empty() || conjuncts.len() % 2 != 0 {
        return None;
    }
    conjuncts
        .chunks(2)
        .map(|pair| {
            let guarded = type_guard(pair[0])?;
            let (field, value) = optional_equality(pair[1])?;
            (guarded == field).then(|| (field.last().to_owned(), value.to_owned()))
        })
        .collect()
}

/// Whether `predicate` is a scope filter: it keeps rows without ever
/// failing, whatever they hold.
#[must_use]
pub fn is_scope_restriction(predicate: &Expression) -> bool {
    scope_restriction(predicate).is_some()
}

/// `f?` → `f`.
fn optional_field(expression: &Expression) -> Option<&super::ExpressionFieldPath> {
    match expression.ungrouped().view() {
        ExpressionView::Call { function: "optional", arguments: [field] } => field.as_field(),
        _ => None,
    }
}

/// `type(f?) == "string"` → `f`.
pub(crate) fn type_guard(expression: &Expression) -> Option<&super::ExpressionFieldPath> {
    let (left, operator, right) = expression.as_binary()?;
    if operator != BinaryOperator::Equal || !matches!(right.ungrouped().as_literal(), Some(Literal::String(kind)) if kind.as_ref() == "string") {
        return None;
    }
    match left.ungrouped().view() {
        ExpressionView::Call { function: "type", arguments: [argument] } => optional_field(argument),
        _ => None,
    }
}

/// `f? == "value"` → (`f`, value).
pub(crate) fn optional_equality(expression: &Expression) -> Option<(&super::ExpressionFieldPath, &str)> {
    let (left, operator, right) = expression.as_binary()?;
    if operator != BinaryOperator::Equal {
        return None;
    }
    let Some(Literal::String(value)) = right.ungrouped().as_literal() else { return None };
    Some((optional_field(left)?, value.as_ref()))
}

/// `_id == "…"`, either way round.
fn is_identifier_equality(predicate: &Expression) -> bool {
    let Some((left, BinaryOperator::Equal, right)) = predicate.ungrouped().as_binary() else { return false };
    let identifier = |field: &Expression, literal: &Expression| {
        field.ungrouped().as_field().is_some_and(|path| path.len() == 1 && path.first() == "_id") && matches!(literal.ungrouped().as_literal(), Some(Literal::String(_)))
    };
    identifier(left, right) || identifier(right, left)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn scope_filters_are_recognized_with_their_values() { let predicate = scope_predicate(None, "place \"a\"", Some("app-1")).unwrap(); assert_eq!(scope_restriction(&predicate).unwrap(), vec![("_place".to_owned(), "place \"a\"".to_owned()), ("_app_instance".to_owned(), "app-1".to_owned())]); let aliased = scope_predicate(Some("o"), "p", None).unwrap(); assert_eq!(scope_restriction(&aliased).unwrap(), vec![("_place".to_owned(), "p".to_owned())]); for other in ["_place == \"p\"", "type(_place?) == \"string\"", "type(a?) == \"string\" and b? == \"p\"", "_place? == \"p\" and type(_place?) == \"string\""] { assert!(!is_scope_restriction(&parse_expression(other).unwrap()), "{other}"); } }
}
