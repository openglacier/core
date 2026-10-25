#![cfg_attr(rustfmt, rustfmt_skip)]
//! Filters moved towards the source of a plan.
//!
//! A `where` passes the stage before it when that stage leaves the fields the
//! predicate reads and the set of rows alone: `derive`, `select`, `rename`,
//! `drop` and `unwind` that do not write those fields, `lookup` that does not
//! fill one of them, `sort`. It passes `group` and `pivot` when it reads only
//! their keys (a group is kept or dropped whole; the predicate then reads its
//! fields as optional, since a group key of a row without the field is
//! `null`), and `window` when it reads only partition fields. It stops at any
//! other stage, and after another filter (their order is kept); a total
//! guard of it then goes on alone, past filters too: it cannot fail and
//! drops only rows the filter drops without failing (`type(f?) != "string"
//! or f? == "x"` for each comparison with a constant). A filter passes a
//! `union` too, and is copied at the end of its branch (where it moves on):
//! the rows of both are filtered alike.
//! Each row the predicate now meets is a row it met
//! before, with the same values for the fields it reads; the stages it
//! passed see fewer rows, never other ones. Filters that reach the source
//! feed the storage pushdowns (row groups left out, typed batches).

use crate::Value;

use super::{parse_expression, BinaryOperator, Expression, ExpressionFieldPath, ExpressionView, PhysicalOperator, PhysicalSubPipeline, Stage};

/// `operators` with each filter moved as close to the source as it can go.
pub(super) fn push_filters(operators: Vec<PhysicalOperator>) -> Vec<PhysicalOperator> {
    // `where a and b` is `where a | where b`: each conjunct moves on its own;
    // filters that end side by side are joined (`and` short-circuits as the
    // second filter only meets the rows the first keeps), so that the
    // storage pushdowns and batch paths, which take one filter, see them all.
    let mut pushed: Vec<(PhysicalOperator, Option<usize>)> = Vec::with_capacity(operators.len());
    for (origin, operator) in operators.into_iter().enumerate() {
        let PhysicalOperator::Filter { predicate } = operator else {
            pushed.push((operator, None));
            continue;
        };
        for predicate in conjuncts(&predicate) {
            let (at, predicate, _, unions) = descend(&pushed, pushed.len(), predicate.clone(), None);
            for (index, copy) in unions {
                if let PhysicalOperator::Union { collection, alias, pipeline } = &pushed[index].0 {
                    let operators = pipeline.operators().iter().cloned().chain([PhysicalOperator::Filter { predicate: copy }]).collect();
                    if let Ok(pipeline) = PhysicalSubPipeline::new(push_filters(operators)) {
                        pushed[index].0 = PhysicalOperator::Union { collection: collection.clone(), alias: alias.clone(), pipeline };
                    }
                }
            }
            // Stopped by another filter: its total guard goes on alone, kept
            // when it passes a filter written apart from this one.
            let guard = matches!(at.checked_sub(1).map(|index| &pushed[index].0), Some(PhysicalOperator::Filter { .. })).then(|| guard(&predicate)).flatten();
            pushed.insert(at, (PhysicalOperator::Filter { predicate }, Some(origin)));
            if let Some((guard_at, guard, ..)) = guard.map(|guard| descend(&pushed, at, guard, Some(origin))).filter(|(_, _, apart, _)| *apart) {
                pushed.insert(guard_at, (PhysicalOperator::Filter { predicate: guard }, None));
            }
        }
    }
    let mut joined: Vec<(PhysicalOperator, Option<usize>)> = Vec::with_capacity(pushed.len());
    for (operator, origin) in pushed {
        match (joined.last_mut(), operator) {
            (Some((PhysicalOperator::Filter { predicate: before }, _)), PhysicalOperator::Filter { predicate }) => {
                let span = before.span();
                *before = Expression::binary(before.clone(), BinaryOperator::And, predicate, span);
            }
            (_, operator) => joined.push((operator, origin)),
        }
    }
    joined.into_iter().map(|(operator, _)| operator).collect()
}

/// Where `predicate`, placed at `at`, can move, what it reads as there,
/// whether it passed a filter of another origin than `guard`, and the
/// `union`s it passed with the predicate their branch takes. A guard (one
/// that cannot fail, of the filter written at `guard`) also passes filters.
fn descend(pushed: &[(PhysicalOperator, Option<usize>)], mut at: usize, mut predicate: Expression, guard: Option<usize>) -> (usize, Expression, bool, Vec<(usize, Expression)>) {
    let mut reads = Vec::new();
    predicate.for_each_field(&mut |field| reads.push(field.clone()));
    let (mut apart, mut unions) = (false, Vec::new());
    // A predicate that reads no field keeps its place: before a group of all
    // rows, it would decide whether the group exists.
    while at > 0 && !reads.is_empty() {
        let (operator, origin) = &pushed[at - 1];
        // A branch reads its fields under its own alias.
        if let PhysicalOperator::Union { alias, .. } = operator {
            if guard.is_some() || reads.iter().any(|read| alias.as_deref() == Some(read.first())) {
                break;
            }
            unions.push((at - 1, predicate.clone()));
            at -= 1;
            continue;
        }
        match passes(operator, &reads, guard.is_some()) {
            Some(optional) => {
                if optional {
                    predicate = predicate.with_optional_fields();
                }
                apart |= matches!(operator, PhysicalOperator::Filter { .. }) && *origin != guard;
                at -= 1;
            }
            None => break,
        }
    }
    (at, predicate, apart, unions)
}

/// A predicate that cannot fail and is false only where `predicate` is false
/// without failing, so it drops no row on which `predicate` would fail: for
/// each conjunct comparing a field with a constant, `type(f?) != "string" or
/// f? == "x"`; `None` when there is none.
fn guard(predicate: &Expression) -> Option<Expression> {
    let guards = conjuncts(predicate).into_iter().filter_map(|conjunct| conjunct.ungrouped().as_binary().and_then(|(left, operator, right)| comparison_guard(left, operator, right))).collect::<Vec<_>>();
    parse_expression(&guards.join(" and ")).ok().filter(|_| !guards.is_empty())
}

/// The conjuncts of `predicate` (`a and b and c`), in evaluation order.
fn conjuncts(predicate: &Expression) -> Vec<&Expression> {
    match predicate.ungrouped().as_binary() {
        Some((left, BinaryOperator::And, right)) => conjuncts(left).into_iter().chain(conjuncts(right)).collect(),
        _ => vec![predicate],
    }
}

fn comparison_guard(left: &Expression, operator: BinaryOperator, right: &Expression) -> Option<String> {
    use BinaryOperator::{Equal, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, NotEqual};
    let field = |expression: &Expression| match expression.ungrouped().view() {
        ExpressionView::Field(field) => Some(field.clone()),
        ExpressionView::Call { function: "optional", arguments } => match arguments.first().map(Expression::view) { Some(ExpressionView::Field(field)) => Some(field.clone()), _ => None },
        _ => None,
    };
    let kind = |value: &Value| match value { Value::String(_) => Some("string"), Value::Number(_) => Some("number"), _ => None };
    let (path, constant) = (field(left)?, right.constant()?);
    let kind = match operator {
        Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => kind(constant)?,
        In => {
            let kinds = constant.as_array()?.iter().map(kind).collect::<Option<Vec<_>>>()?;
            kinds.first().copied().filter(|first| kinds.iter().all(|kind| kind == first))?
        }
        _ => return None,
    };
    Some(format!("(type({path}?) != \"{kind}\" or {path}? {} {right})", operator.as_str()))
}

/// Whether a filter reading `reads` may run before `operator`: `Some(true)`
/// when it must then read them as optional.
fn passes(operator: &PhysicalOperator, reads: &[ExpressionFieldPath], total: bool) -> Option<bool> {
    let untouched = |writes: &[ExpressionFieldPath]| reads.iter().all(|read| writes.iter().all(|write| !overlaps(read, write)));
    let keys = |keys: &[ExpressionFieldPath]| reads.iter().all(|read| keys.contains(read));
    match operator {
        PhysicalOperator::Sort { .. } => Some(false),
        PhysicalOperator::Filter { .. } => total.then_some(false),
        PhysicalOperator::Lookup { into, .. } => ExpressionFieldPath::new([into.as_ref()]).ok().filter(|into| untouched(std::slice::from_ref(into))).map(|_| false),
        PhysicalOperator::Select { fields } => keys(fields).then_some(false),
        PhysicalOperator::Group { keys: fields, .. } => {
            let fields = fields.iter().filter(|field| !field.first().starts_with("__og_")).cloned().collect::<Vec<_>>();
            (!fields.is_empty() && keys(&fields)).then_some(true)
        }
        PhysicalOperator::Pivot { specification } => keys(specification.rows()).then_some(true),
        PhysicalOperator::Custom { stage: Some(Stage::Window), arguments, .. } => {
            let partition = arguments.split('|').next().unwrap_or_default().split(',').map(str::trim).filter(|field| !field.is_empty())
                .map(|field| ExpressionFieldPath::new(field.split('.'))).collect::<Result<Vec<_>, _>>().ok()?;
            (!partition.is_empty() && keys(&partition)).then_some(false)
        }
        PhysicalOperator::Custom { stage: Some(Stage::Derive | Stage::Select | Stage::Rename | Stage::Drop | Stage::Unwind), compiled: Some(compiled), .. } => match compiled.output_fields() {
            super::Fields::Preserved => Some(false),
            super::Fields::Rewritten(writes) => untouched(writes).then_some(false),
            // A `select` keeps a field the predicate reads only as itself.
            super::Fields::Projected(_) => compiled.slot_operation().and_then(|operation| match operation {
                super::runtime_materializer::SlotOperation::Select(items) => reads.iter().all(|read| items.iter().any(|(target, source)| *target == read && matches!(source, Ok(source) if *source == read))).then_some(false),
                _ => None,
            }),
            _ => None,
        },
        _ => None,
    }
}

/// Whether one path is a prefix of the other (`a` and `a.b`).
fn overlaps(left: &ExpressionFieldPath, right: &ExpressionFieldPath) -> bool {
    left.segments().iter().zip(right.segments()).all(|(left, right)| left == right)
}
