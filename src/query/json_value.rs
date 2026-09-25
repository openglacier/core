//! Shared JSON-to-runtime-value conversion for structured query literals.

use crate::helpers::json_to_value;
use crate::Value;

use super::{ExecutionError, ExecutionResult};

pub fn parse_json_literal(source: &str) -> ExecutionResult<Value> {
    let json: serde_json::Value = serde_json::from_str(source).map_err(|error| {
        ExecutionError::evaluation(format!("invalid structured JSON literal: {error}"))
    })?;
    json_to_value(&json).map_err(ExecutionError::evaluation)
}
