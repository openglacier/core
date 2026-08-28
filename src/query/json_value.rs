//! Shared JSON-to-runtime-value conversion for structured query literals.

use crate::{Document, Number, Value};

use super::{ExecutionError, ExecutionResult};

pub(crate) fn parse_json_literal(source: &str) -> ExecutionResult<Value> {
    let json: serde_json::Value = serde_json::from_str(source).map_err(|error| {
        ExecutionError::evaluation(format!("invalid structured JSON literal: {error}"))
    })?;
    json_to_value(&json)
}

fn json_to_value(value: &serde_json::Value) -> ExecutionResult<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::null()),
        serde_json::Value::Bool(value) => Ok(Value::bool(*value)),
        serde_json::Value::String(value) => Ok(Value::string(value.as_str())),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::signed(value))
            } else if let Some(value) = value.as_u64() {
                Ok(Value::unsigned(value))
            } else if let Some(value) = value.as_f64() {
                Number::float(value)
                    .map(Value::Number)
                    .map_err(|error| ExecutionError::evaluation(error.to_string()))
            } else {
                Err(ExecutionError::evaluation(
                    "JSON number cannot be represented",
                ))
            }
        }
        serde_json::Value::Array(values) => values
            .iter()
            .map(json_to_value)
            .collect::<ExecutionResult<Vec<_>>>()
            .map(Value::array),
        serde_json::Value::Object(values) => {
            let mut document = Document::new();
            for (key, value) in values {
                document.insert(key.as_str(), json_to_value(value)?);
            }
            Ok(Value::object(document))
        }
    }
}
