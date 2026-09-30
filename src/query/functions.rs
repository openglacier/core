#![cfg_attr(rustfmt, rustfmt_skip)]
//! Built-in functions whose arguments are all evaluated before the call.
//!
//! [`super::eval`] handles the functions that choose which arguments to
//! evaluate or accept missing ones (`if`, `case`, `coalesce`, `exists`, …) and
//! delegates everything else here, with the arguments already computed.

use std::sync::Arc;

use crate::model::{compare, equals, Comparison, Document, Number, Value};

use super::dates::{self, DateUnit};
use super::eval::{error, float, float_of, integer_of, integral, number, text, POLICY};
use super::{ExecutionResult, Function, Function as F};

/// Largest array `range` may build, so a typo cannot allocate unbounded memory.
const MAX_RANGE_ITEMS: i64 = 1_000_000;

/// Calls one eager built-in function.
pub(super) fn call(function: Function, arguments: &[Value]) -> ExecutionResult<Value> {
    let name = function.as_str();
    let argument = |index: usize| &arguments[index];
    let optional = |index: usize| arguments.get(index);
    Ok(match function {
        // Numeric functions.
        F::Abs => match number(name, argument(0))? {
            Number::Float(value) => float(value.abs())?,
            integer => integer_of(integer).and_then(i64::checked_abs).map_or_else(|| float(float_of(integer).abs()), |value| Ok(Value::signed(value)))?,
        },
        F::Sqrt => {
            let value = float_of(number(name, argument(0))?);
            if value < 0.0 {
                return Err(error(format!("`sqrt` expects a non-negative number, found {value}")));
            }
            float(value.sqrt())?
        }
        F::Floor | F::Ceil | F::Round | F::Trunc => {
            let value = number(name, argument(0))?;
            let digits = match optional(1) {
                Some(digits) => integer_of(number(name, digits)?)
                    .filter(|digits| (0..=15).contains(digits))
                    .ok_or_else(|| error(format!("`{name}` digits must be an integer between 0 and 15")))?,
                None => 0,
            };
            if value.is_integer() {
                Value::Number(value)
            } else {
                let scale = 10_f64.powi(digits as i32);
                let scaled = float_of(value) * scale;
                let rounded = match function { F::Floor => scaled.floor(), F::Ceil => scaled.ceil(), F::Trunc => scaled.trunc(), _ => scaled.round() };
                if digits == 0 { integral(rounded)? } else { float(rounded / scale)? }
            }
        }
        F::Pow => {
            let (base, exponent) = (number(name, argument(0))?, number(name, argument(1))?);
            match (integer_of(base), integer_of(exponent).and_then(|exponent| u32::try_from(exponent).ok())) {
                (Some(base), Some(exponent)) if base.checked_pow(exponent).is_some() => Value::signed(base.pow(exponent)),
                _ => float(float_of(base).powf(float_of(exponent)))?,
            }
        }
        F::Min | F::Max => {
            // One array argument aggregates its elements: `max(scores)`.
            let values = match arguments {
                [Value::Array(items)] => items.as_ref(),
                _ => arguments,
            };
            let wanted = if function == F::Min { Comparison::Less } else { Comparison::Greater };
            let mut best: Option<&Value> = None;
            for value in values {
                let replace = match best {
                    None => true,
                    Some(current) => compare(value, current, POLICY)
                        .map_err(|failure| error(format!("`{name}` cannot compare {value:?} and {current:?}: {failure}")))?
                        == wanted,
                };
                if replace {
                    best = Some(value);
                }
            }
            best.cloned().unwrap_or_else(Value::null)
        }
        // Text and collection functions.
        F::Len => {
            let length = match argument(0) {
                Value::String(text) => text.chars().count(),
                Value::Array(items) => items.len(),
                Value::Object(object) => object.len(),
                other => return Err(error(format!("`len` expects a string, array or object, found {other:?}"))),
            };
            Value::unsigned(length as u64)
        }
        F::Lower => Value::string(text(name, argument(0))?.to_lowercase()),
        F::Upper => Value::string(text(name, argument(0))?.to_uppercase()),
        F::Trim => Value::string(text(name, argument(0))?.trim()),
        F::Concat => {
            if arguments.iter().all(Value::is_array) {
                Value::array(arguments.iter().flat_map(|value| value.as_array().unwrap_or_default().iter().cloned()))
            } else {
                let mut output = String::new();
                for value in arguments {
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
        F::Contains => {
            let (haystack, needle) = (argument(0), argument(1));
            match haystack {
                Value::Array(items) => Value::bool(items.iter().any(|item| equals(item, needle, POLICY).unwrap_or(false))),
                _ => Value::bool(text(name, haystack)?.contains(text(name, needle)?)),
            }
        }
        F::StartsWith => Value::bool(text(name, argument(0))?.starts_with(text(name, argument(1))?)),
        F::EndsWith => Value::bool(text(name, argument(0))?.ends_with(text(name, argument(1))?)),
        F::Type => Value::string(argument(0).physical_kind().as_str()),

        // Conversions.
        F::ToInt => to_int(argument(0))?,
        F::ToFloat => to_float(argument(0))?,
        F::ToString => match argument(0) {
            Value::Null => Value::null(),
            Value::String(value) => Value::String(Arc::clone(value)),
            other => Value::string(crate::helpers::value_to_json(other).to_string()),
        },
        F::ToBool => to_bool(argument(0))?,

        // Mathematics.
        F::Ln | F::Log10 | F::Log => {
            let value = float_of(number(name, argument(0))?);
            let base = match function {
                F::Ln => std::f64::consts::E,
                F::Log10 => 10.0,
                _ => float_of(number(name, argument(1))?),
            };
            if value <= 0.0 || base <= 0.0 || base == 1.0 {
                return Err(error(format!("`{name}` expects a positive number and a positive base other than 1, found {value} and {base}")));
            }
            // Dedicated routines are exact on powers of their base.
            #[allow(clippy::float_cmp)]
            let result = if function == F::Ln {
                value.ln()
            } else if base == 2.0 {
                value.log2()
            } else if base == 10.0 {
                value.log10()
            } else {
                value.log(base)
            };
            float(result)?
        }
        F::Exp => float(float_of(number(name, argument(0))?).exp())?,
        F::Sign => {
            let value = float_of(number(name, argument(0))?);
            Value::signed(if value > 0.0 { 1 } else if value < 0.0 { -1 } else { 0 })
        }
        F::Sum | F::Avg => {
            let values = numbers_or_array(arguments);
            let values = values.iter().filter(|value| !value.is_null()).collect::<Vec<_>>();
            if function == F::Avg {
                if values.is_empty() {
                    return Ok(Value::null());
                }
                let total = values.iter().map(|value| number(name, value).map(float_of)).sum::<ExecutionResult<f64>>()?;
                float(total / values.len() as f64)?
            } else {
                sum(values.into_iter())?
            }
        }

        // Text.
        F::Substr => {
            let value = text(name, argument(0))?;
            let (start, end) = window(name, value.chars().count(), argument(1), optional(2))?;
            let byte = |position: usize| value.char_indices().nth(position).map_or(value.len(), |(byte, _)| byte);
            Value::string(&value[byte(start)..byte(end)])
        }
        F::Split => {
            let (value, separator) = (text(name, argument(0))?, text(name, argument(1))?);
            if separator.is_empty() {
                Value::array(value.chars().map(|character| Value::string(character.to_string())))
            } else {
                Value::array(value.split(separator).map(Value::string))
            }
        }
        F::Replace => {
            let (value, from, to) = (text(name, argument(0))?, text(name, argument(1))?, text(name, argument(2))?);
            if from.is_empty() {
                return Err(error("`replace` expects a non-empty search string"));
            }
            Value::string(value.replace(from, to))
        }
        F::IndexOf => Value::signed(match argument(0) {
            Value::Array(items) => items.iter().position(|item| equals(item, argument(1), POLICY).unwrap_or(false)),
            value => {
                let (value, needle) = (text(name, value)?, text(name, argument(1))?);
                value.find(needle).map(|byte| value[..byte].chars().count())
            }
        }.map_or(-1, |index| index as i64)),
        F::PadLeft | F::PadRight => {
            let value = text(name, argument(0))?;
            let width = count(name, argument(1))?;
            let fill = match optional(2) {
                Some(fill) => {
                    let mut characters = text(name, fill)?.chars();
                    match (characters.next(), characters.next()) {
                        (Some(character), None) => character,
                        _ => return Err(error(format!("`{name}` fill must be exactly one character"))),
                    }
                }
                None => ' ',
            };
            let padding = fill.to_string().repeat(width.saturating_sub(value.chars().count()));
            Value::string(if function == F::PadLeft { padding + value } else { format!("{value}{padding}") })
        }
        F::Ltrim => Value::string(text(name, argument(0))?.trim_start()),
        F::Rtrim => Value::string(text(name, argument(0))?.trim_end()),

        // Arrays.
        F::At => {
            let items = array(name, argument(0))?;
            let index = integer(name, argument(1))?;
            let position = if index < 0 { items.len() as i64 + index } else { index };
            usize::try_from(position).ok().and_then(|position| items.get(position)).cloned().unwrap_or_else(Value::null)
        }
        F::Slice => {
            let items = array(name, argument(0))?;
            let (start, end) = window(name, items.len(), argument(1), optional(2))?;
            Value::array(items[start..end].iter().cloned())
        }
        F::Reverse => match argument(0) {
            Value::String(value) => Value::string(value.chars().rev().collect::<String>()),
            value => Value::array(array(name, value)?.iter().rev().cloned()),
        },
        F::SortArray => {
            let descending = match optional(1).map(|direction| text(name, direction)).transpose()? {
                None | Some("asc") => false,
                Some("desc") => true,
                Some(other) => return Err(error(format!("`sort_array` direction must be \"asc\" or \"desc\", found {other:?}"))),
            };
            let mut items = array(name, argument(0))?.to_vec();
            let mut failure = None;
            items.sort_by(|left, right| match compare(left, right, POLICY) {
                Ok(comparison) => comparison.into_ordering(),
                Err(_) => {
                    failure.get_or_insert_with(|| error(format!("`sort_array` cannot compare {left:?} and {right:?}")));
                    std::cmp::Ordering::Equal
                }
            });
            if let Some(failure) = failure {
                return Err(failure);
            }
            if descending {
                items.reverse();
            }
            Value::array(items)
        }
        F::Unique => {
            let mut kept: Vec<Value> = Vec::new();
            for item in array(name, argument(0))? {
                if !kept.iter().any(|existing| equals(existing, item, POLICY).unwrap_or(false)) {
                    kept.push(item.clone());
                }
            }
            Value::array(kept)
        }
        F::Flatten => Value::array(array(name, argument(0))?.iter().flat_map(|item| match item {
            Value::Array(inner) => inner.to_vec(),
            other => vec![other.clone()],
        })),
        F::Range => {
            let (start, end) = (integer(name, argument(0))?, integer(name, argument(1))?);
            let step = optional(2).map(|step| integer(name, step)).transpose()?.unwrap_or(1);
            if step == 0 {
                return Err(error("`range` step must not be 0"));
            }
            let length = if (step > 0 && end > start) || (step < 0 && end < start) {
                (i128::from(end) - i128::from(start)).unsigned_abs().div_ceil(u128::from(step.unsigned_abs()))
            } else {
                0
            };
            if length > MAX_RANGE_ITEMS as u128 {
                return Err(error(format!("`range` would build {length} items; the limit is {MAX_RANGE_ITEMS}")));
            }
            Value::array((0..length as i64).map(|index| Value::signed(start + index * step)))
        }

        // Objects.
        F::Keys => Value::array(object(name, argument(0))?.keys().map(|key| Value::string(key.as_arc().clone()))),
        F::Values => Value::array(object(name, argument(0))?.values().cloned()),
        F::Merge => {
            let mut merged = Document::new();
            for value in arguments.iter().filter(|value| !value.is_null()) {
                for (key, value) in object(name, value)?.iter() {
                    merged.insert(key.clone(), value.clone());
                }
            }
            Value::object(merged)
        }
        F::Get => {
            let key = text(name, argument(1))?;
            object(name, argument(0))?.get(key).cloned().or_else(|| optional(2).cloned()).unwrap_or_else(Value::null)
        }
        F::Has => Value::bool(object(name, argument(0))?.contains_key(text(name, argument(1))?)),

        // Comparison helpers.
        F::Nullif => if equals(argument(0), argument(1), POLICY).unwrap_or(false) { Value::null() } else { argument(0).clone() },
        F::Between => {
            let ordered = |low: &Value, high: &Value| -> ExecutionResult<bool> {
                let compared = |left: &Value, right: &Value| compare(left, right, POLICY)
                    .map_err(|failure| error(format!("`between` cannot compare {left:?} and {right:?}: {failure}")));
                Ok(compared(low, high)? != Comparison::Greater)
            };
            let (value, low, high) = (argument(0), argument(1), argument(2));
            Value::bool(ordered(low, value)? && ordered(value, high)?)
        }
        F::Bucket => {
            let bounds = array(name, argument(1))?;
            if bounds.len() < 2 {
                return Err(error("`bucket` expects at least two boundaries"));
            }
            for pair in bounds.windows(2) {
                let ascending = compare(&pair[0], &pair[1], POLICY).map_err(|failure| error(format!("`bucket` boundaries must be comparable: {failure}")))?;
                if ascending != Comparison::Less {
                    return Err(error("`bucket` boundaries must be strictly ascending"));
                }
            }
            // The value falls in the range [bounds[i], bounds[i + 1]) holding
            // it; a comparison that fails puts it in no range starting or
            // ending there. Bounds are ascending, so while every comparison
            // succeeds a binary search finds that range.
            let value = argument(0);
            let not_below = |bound: &Value| compare(value, bound, POLICY).map(|comparison| comparison != Comparison::Less);
            let failed = std::cell::Cell::new(false);
            let above = bounds.partition_point(|bound| not_below(bound).unwrap_or_else(|_| { failed.set(true); false }));
            let found = if failed.get() {
                bounds.windows(2).find(|pair| not_below(&pair[0]).unwrap_or(false) && not_below(&pair[1]).is_ok_and(|not_below| !not_below)).map(|pair| pair[0].clone())
            } else {
                (above > 0 && above < bounds.len()).then(|| bounds[above - 1].clone())
            };
            match (found, optional(2)) {
                (Some(bound), _) => bound,
                (None, Some(default)) => default.clone(),
                (None, None) => Value::null(),
            }
        }

        // Dates.
        F::Now => Value::signed(now_millis()),
        F::ParseDate => Value::signed(timestamp(name, argument(0))?),
        F::DateTrunc => Value::signed(dates::trunc(timestamp(name, argument(0))?, unit(name, argument(1))?)),
        F::DateAdd => Value::signed(dates::add(timestamp(name, argument(0))?, integer(name, argument(1))?, unit(name, argument(2))?).map_err(error)?),
        F::DateDiff => Value::signed(dates::diff(timestamp(name, argument(0))?, timestamp(name, argument(1))?, unit(name, argument(2))?)),
        F::DatePart => Value::signed(dates::part(timestamp(name, argument(0))?, text(name, argument(1))?).map_err(error)?),
        F::DateFormat => {
            let millis = timestamp(name, argument(0))?;
            Value::string(match optional(1) {
                Some(pattern) => dates::format(millis, text(name, pattern)?).map_err(error)?,
                None => dates::to_iso(millis),
            })
        }

        lazy => return Err(error(format!("function `{lazy}` evaluates its own arguments"))),
    })
}

fn to_int(value: &Value) -> ExecutionResult<Value> {
    let failure = || error(format!("`to_int` cannot convert {value:?}"));
    Ok(match value {
        Value::Null => Value::null(),
        Value::Bool(flag) => Value::signed(i64::from(*flag)),
        Value::Number(number) => match integer_of(*number) {
            Some(integer) => Value::signed(integer),
            None if number.is_integer() => Value::Number(*number),
            None => integral(float_of(*number).trunc()).map_err(|_| failure())?,
        },
        Value::String(text) => {
            let text = text.trim();
            match text.parse::<i64>() {
                Ok(integer) => Value::signed(integer),
                Err(_) => {
                    let parsed = text.parse::<f64>().ok().filter(|value| value.is_finite()).ok_or_else(failure)?;
                    integral(parsed.trunc())?
                }
            }
        }
        Value::Array(_) | Value::Object(_) => return Err(failure()),
    })
}

fn to_float(value: &Value) -> ExecutionResult<Value> {
    match value {
        Value::Null => Ok(Value::null()),
        Value::Bool(flag) => float(if *flag { 1.0 } else { 0.0 }),
        Value::Number(number) => float(float_of(*number)),
        Value::String(text) => match text.trim().parse::<f64>() {
            Ok(parsed) if parsed.is_finite() => float(parsed),
            _ => Err(error(format!("`to_float` cannot convert {text:?}"))),
        },
        other => Err(error(format!("`to_float` cannot convert {other:?}"))),
    }
}

fn to_bool(value: &Value) -> ExecutionResult<Value> {
    Ok(match value {
        Value::Null => Value::null(),
        Value::Bool(flag) => Value::bool(*flag),
        Value::Number(number) => Value::bool(float_of(*number) != 0.0),
        Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Value::bool(true),
            "false" | "0" | "no" => Value::bool(false),
            _ => return Err(error(format!("`to_bool` cannot convert {text:?}; expected true/false, 1/0 or yes/no"))),
        },
        other => return Err(error(format!("`to_bool` cannot convert {other:?}"))),
    })
}

/// Sums numbers, staying integral while every term is an integer and the total fits in i64.
pub(super) fn sum<'a>(values: impl Iterator<Item = &'a Value>) -> ExecutionResult<Value> {
    let mut integer_total = Some(0_i64);
    let mut float_total = 0.0;
    for value in values {
        let term = number("sum", value)?;
        integer_total = integer_total.zip(integer_of(term)).and_then(|(total, term)| total.checked_add(term));
        float_total += float_of(term);
    }
    match integer_total {
        Some(total) => Ok(Value::signed(total)),
        None => float(float_total),
    }
}

/// One array argument aggregates its elements; several arguments are the values.
fn numbers_or_array(arguments: &[Value]) -> Vec<Value> {
    match arguments {
        [Value::Array(items)] => items.to_vec(),
        _ => arguments.to_vec(),
    }
}

fn integer(name: &str, value: &Value) -> ExecutionResult<i64> {
    integer_of(number(name, value)?).ok_or_else(|| error(format!("`{name}` expects an integer, found {value:?}")))
}

fn count(name: &str, value: &Value) -> ExecutionResult<usize> {
    usize::try_from(integer(name, value)?).map_err(|_| error(format!("`{name}` expects a non-negative integer, found {value:?}")))
}

fn array<'a>(name: &str, value: &'a Value) -> ExecutionResult<&'a [Value]> {
    value.as_array().ok_or_else(|| error(format!("`{name}` expects an array, found {value:?}")))
}

fn object<'a>(name: &str, value: &'a Value) -> ExecutionResult<&'a Document> {
    value.as_object().ok_or_else(|| error(format!("`{name}` expects an object, found {value:?}")))
}

/// Resolves `start` (negative counts from the end) and an optional `length`
/// into a clamped `[start, end)` window over `len` items.
fn window(name: &str, len: usize, start: &Value, length: Option<&Value>) -> ExecutionResult<(usize, usize)> {
    let len = len as i64;
    let start = integer(name, start)?;
    let start = if start < 0 { (len + start).max(0) } else { start.min(len) };
    let end = match length {
        Some(length) => start.saturating_add(count(name, length)? as i64).min(len),
        None => len,
    };
    Ok((start as usize, end as usize))
}

/// Reads a timestamp: epoch milliseconds (a number) or an ISO-8601 string.
fn timestamp(name: &str, value: &Value) -> ExecutionResult<i64> {
    match value {
        Value::Number(number) => match integer_of(*number) {
            Some(millis) => Ok(millis),
            None => {
                let millis = float_of(*number).floor();
                if millis.is_finite() && millis.abs() < 9.0e15 {
                    Ok(millis as i64)
                } else {
                    Err(error(format!("`{name}` timestamp {millis} is out of range")))
                }
            }
        },
        Value::String(text) => dates::parse_iso(text).map_err(|message| error(format!("`{name}`: {message}"))),
        other => Err(error(format!("`{name}` expects epoch milliseconds or an ISO-8601 string, found {other:?}"))),
    }
}

fn unit(name: &str, value: &Value) -> ExecutionResult<DateUnit> {
    DateUnit::parse(text(name, value)?).map_err(|message| error(format!("`{name}`: {message}")))
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
}
