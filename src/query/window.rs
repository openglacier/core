//! Stages over sorted partitions, after the `sort` the planner puts before
//! them: `window` (values over the rows of each partition: `lag` and `lead`,
//! `cumsum`, `first` and `last`, `row_number`) and `fill` (the rows a grid
//! of values misses in each partition).
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::sync::Arc;

use super::executor::compare_sort_values;
use super::runtime_materializer::{insert_path, path_value, MeasureState};
use super::syntax::split_top_level_word;
use super::{dates, ExecutionError, ExecutionResult, ExecutionRow, ExpressionFieldPath, PivotAggregate, SortKey, SyntheticDocument};
use crate::model::{Document, Value};
use crate::storage::DocumentId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Lag,
    Lead,
    CumSum,
    First,
    Last,
    RowNumber,
}

#[derive(Clone, Debug)]
struct Function {
    kind: Kind,
    source: Option<ExpressionFieldPath>,
    offset: usize,
    alias: Vec<String>,
}

/// A `window` stage as the planner writes it: `<partition field>, … |
/// <function> [<field>] [<offset>] as <alias>, …`.
#[derive(Clone, Debug)]
pub struct WindowSpec {
    partition: Vec<SortKey>,
    functions: Vec<Function>,
}

impl WindowSpec {
    pub fn parse(arguments: &str) -> ExecutionResult<Self> {
        let invalid = || ExecutionError::evaluation(format!("invalid window arguments {arguments:?}"));
        let path = |text: &str| ExpressionFieldPath::new(text.split('.')).map_err(|_| invalid());
        let (partition, functions) = arguments.split_once('|').ok_or_else(invalid)?;
        let partition = partition.split(',').map(str::trim).filter(|field| !field.is_empty()).map(|field| path(field).map(SortKey::ascending)).collect::<ExecutionResult<_>>()?;
        let functions = functions.split(',').map(|text| {
            let words = text.split_whitespace().collect::<Vec<_>>();
            let [name, middle @ .., "as", alias] = words.as_slice() else { return Err(invalid()) };
            let kind = match *name {
                "lag" => Kind::Lag,
                "lead" => Kind::Lead,
                "cumsum" => Kind::CumSum,
                "first" => Kind::First,
                "last" => Kind::Last,
                "row_number" => Kind::RowNumber,
                _ => return Err(invalid()),
            };
            let (source, offset) = match middle {
                [] => (None, 1),
                [source] => (Some(path(source)?), 1),
                [source, offset] => (Some(path(source)?), offset.parse().map_err(|_| invalid())?),
                _ => return Err(invalid()),
            };
            Ok(Function { kind, source, offset, alias: alias.split('.').map(str::to_owned).collect() })
        }).collect::<ExecutionResult<_>>()?;
        Ok(Self { partition, functions })
    }

    pub(crate) fn same_partition(&self, left: &ExecutionRow, right: &ExecutionRow) -> ExecutionResult<bool> {
        same_partition(&self.partition, left, right)
    }

    /// Adds every function's value to the rows of one partition, in order;
    /// a function reads the fields the previous ones added.
    pub(crate) fn apply(&self, rows: &mut [ExecutionRow]) -> ExecutionResult<()> {
        let count = rows.len();
        for function in &self.functions {
            let value = |row: &ExecutionRow| function.source.as_ref().and_then(|source| path_value(row.document(), source)).cloned().unwrap_or_else(Value::null);
            let values = match function.kind {
                Kind::Lag => (0..count).map(|row| row.checked_sub(function.offset).map_or_else(Value::null, |at| value(&rows[at]))).collect(),
                Kind::Lead => (0..count).map(|row| rows.get(row + function.offset).map_or_else(Value::null, value)).collect(),
                Kind::First => vec![rows.first().map_or_else(Value::null, value); count],
                Kind::Last => vec![rows.last().map_or_else(Value::null, value); count],
                Kind::RowNumber => (1..=count).map(|number| Value::unsigned(number as u64)).collect(),
                // A running sum, as `group`'s: an integer while exact, `null`
                // before the first number.
                Kind::CumSum => {
                    let mut sum = MeasureState::new(PivotAggregate::Sum);
                    rows.iter().map(|row| {
                        sum.push(function.source.as_ref().and_then(|source| path_value(row.document(), source)))?;
                        Ok(sum.clone().finish()?.unwrap_or_else(Value::null))
                    }).collect::<ExecutionResult<Vec<_>>>()?
                }
            };
            for (row, value) in rows.iter_mut().zip(values) {
                insert_path(row.document_mut(), &function.alias, value);
            }
        }
        Ok(())
    }
}

/// Whether two rows are in one partition: their `keys` compare equal as the
/// `sort` before compares them (`1` and `1.0` too).
fn same_partition(keys: &[SortKey], left: &ExecutionRow, right: &ExecutionRow) -> ExecutionResult<bool> {
    for key in keys {
        if compare_sort_values(path_value(left.document(), key.field()), path_value(right.document(), key.field()), key)?.is_ne() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Values a `fill` grid holds at most.
const FILL_GRID_LIMIT: usize = 100_000;
const FILL_NAMESPACE: u64 = 0x0000_6669_6c6c;

/// `fill <field> from <start> to <end> step <step> [by <field>, …] [with
/// <field> = <value>, …]`: in each partition, a row for every grid value
/// `start`, `start + step`, … up to `end` that no row holds, made of the
/// partition fields, the grid value and the `with` values.
#[derive(Clone, Debug)]
pub struct FillSpec {
    field: SortKey,
    grid: Vec<Value>,
    partition: Vec<SortKey>,
    defaults: Vec<(Vec<String>, Value)>,
}

impl FillSpec {
    pub fn parse(arguments: &str) -> Result<Self, String> {
        let syntax = "expected `fill <field> from <start> to <end> step <step> [by <fields>] [with <field> = <value>, …]`";
        let path = |text: &str| ExpressionFieldPath::new(text.trim().split('.')).map_err(|error| format!("invalid field {text:?}: {error}"));
        let literal = |text: &str| super::plan_values::value_from_source(text.trim()).map_err(|error| format!("invalid value {text:?}: {error}"));
        let (field, rest) = split_top_level_word(arguments, "from").ok_or(syntax)?;
        let (start, rest) = split_top_level_word(rest, "to").ok_or(syntax)?;
        let (end, rest) = split_top_level_word(rest, "step").ok_or(syntax)?;
        let (rest, defaults) = split_top_level_word(rest, "with").map_or((rest, None), |(rest, defaults)| (rest, Some(defaults)));
        let (step, partition) = split_top_level_word(rest, "by").map_or((rest, None), |(step, partition)| (step, Some(partition)));
        let partition = partition.map_or(Ok(Vec::new()), |fields| fields.split(',').map(|field| path(field).map(SortKey::ascending)).collect())?;
        let defaults = defaults.map_or(Ok(Vec::new()), |defaults| defaults.split(',').map(|assignment| {
            let (field, value) = assignment.split_once('=').ok_or(syntax)?;
            Ok((path(field)?.iter().map(str::to_owned).collect(), literal(value)?))
        }).collect::<Result<_, String>>())?;
        let grid = grid(&literal(start)?, &literal(end)?, &literal(step)?)?;
        Ok(Self { field: SortKey::ascending(path(field)?), grid, partition, defaults })
    }

    /// The fields the `sort` before orders by: the partition, then the field.
    #[must_use]
    pub fn sort_fields(&self) -> Vec<&ExpressionFieldPath> {
        self.partition.iter().chain([&self.field]).map(SortKey::field).collect()
    }

    pub(crate) fn same_partition(&self, left: &ExecutionRow, right: &ExecutionRow) -> ExecutionResult<bool> {
        same_partition(&self.partition, left, right)
    }

    /// Emits the rows the grid misses from `next` up to `row`'s value (to
    /// the end without `row`), the partition's fields read from `template`;
    /// returns the next grid position.
    pub(crate) fn fill<E>(&self, template: &ExecutionRow, row: Option<&ExecutionRow>, mut next: usize, generated: &mut u64, emit: &mut impl FnMut(ExecutionRow) -> Result<(), E>) -> Result<usize, E> where E: From<ExecutionError> {
        let value = row.map(|row| path_value(row.document(), self.field.field()));
        while let Some(grid) = self.grid.get(next) {
            let ordering = value.map_or(Ok(std::cmp::Ordering::Less), |value| compare_sort_values(Some(grid), value, &self.field))?;
            if ordering.is_gt() {
                break;
            }
            next += 1;
            if ordering.is_eq() {
                break;
            }
            let mut document = Document::new();
            for key in &self.partition {
                if let Some(value) = path_value(template.document(), key.field()) {
                    insert_path(&mut document, &key.field().iter().collect::<Vec<_>>(), value.clone());
                }
            }
            insert_path(&mut document, &self.field.field().iter().collect::<Vec<_>>(), grid.clone());
            for (field, value) in &self.defaults {
                insert_path(&mut document, field, value.clone());
            }
            *generated += 1;
            emit(ExecutionRow::synthetic(SyntheticDocument::new(DocumentId::synthetic(FILL_NAMESPACE, *generated), Arc::new(document))))?;
        }
        Ok(next)
    }
}

/// `start`, `start + step`, … up to `end`: numbers by a number, dates by a
/// unit (`"month"`), written as `start` is (`"2026-03"`, a full date or
/// epoch milliseconds).
fn grid(start: &Value, end: &Value, step: &Value) -> Result<Vec<Value>, String> {
    let too_long = || format!("a fill grid holds at most {FILL_GRID_LIMIT} values");
    let mut grid = Vec::new();
    match (start, end, step) {
        (Value::Number(start), Value::Number(end), Value::Number(step)) => {
            let (start, end, step) = (start.to_f64(), end.to_f64(), step.to_f64());
            let integers = [start, step].iter().all(|value| value.fract() == 0.0);
            if step.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
                return Err("a fill step must be positive".to_owned());
            }
            for index in 0.. {
                let value = start + step * f64::from(index);
                if value > end {
                    break;
                }
                if grid.len() == FILL_GRID_LIMIT {
                    return Err(too_long());
                }
                grid.push(if integers { Value::signed(value as i64) } else { Value::float(value).map_err(|error| error.to_string())? });
            }
        }
        (_, _, Value::String(unit)) => {
            let unit = dates::DateUnit::parse(unit)?;
            let millis = |value: &Value| match value {
                Value::String(text) => dates::parse_iso(text),
                Value::Number(number) => Ok(number.to_f64() as i64),
                other => Err(format!("a fill bound is a date or a number, found {other:?}")),
            };
            let (first, last) = (millis(start)?, millis(end)?);
            let written = |millis: i64| -> Result<Value, String> {
                Ok(match start {
                    Value::String(text) if text.len() == 7 => Value::string(dates::format(millis, "%Y-%m")?),
                    Value::String(text) if text.len() == 10 => Value::string(dates::format(millis, "%F")?),
                    Value::String(_) => Value::string(dates::to_iso(millis)),
                    _ => Value::signed(millis),
                })
            };
            for index in 0.. {
                let millis = dates::add(first, index, unit)?;
                if millis > last {
                    break;
                }
                if grid.len() == FILL_GRID_LIMIT {
                    return Err(too_long());
                }
                grid.push(written(millis)?);
            }
        }
        _ => return Err("a fill step is a positive number, or a date unit (\"month\") for date bounds".to_owned()),
    }
    Ok(grid)
}
