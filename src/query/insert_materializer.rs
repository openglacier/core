//! Insert document materialization.

use std::sync::Arc;

use crate::{
    storage::{DocumentId, UuidV7Generator},
    Document, Number, Value,
};

use super::{
    ExecutionError, ExecutionResult, InsertDocument, LogicalObject, LogicalValue,
    PreparedInsertDocument,
};

/// Converts validated logical insert documents into storage-ready payloads.
///
/// An explicit UUID v7 `_id` is treated as storage metadata and is not copied
/// into the physical document. When `_id` is absent, a backend-independent,
/// time-ordered UUID v7 is generated.
#[derive(Clone, Debug)]
pub struct InsertDocumentMaterializer {
    id_generator: UuidV7Generator,
}

impl InsertDocumentMaterializer {
    /// Creates a materializer. `prefix` is retained only for source compatibility;
    /// generated identifiers are UUID v7 values.
    #[must_use]
    #[inline]
    pub fn new(prefix: impl Into<Arc<str>>) -> Self {
        let _ = prefix.into();
        Self {
            id_generator: UuidV7Generator::new(),
        }
    }

    /// Converts one validated logical insert document into a storage payload.
    pub fn materialize(&self, source: &InsertDocument) -> ExecutionResult<PreparedInsertDocument> {
        let id = match source.object().get("_id") {
            Some(value) => explicit_document_id(value)?,
            None => self.generate_document_id()?,
        };

        let document = materialize_object(source.object(), true)?;

        Ok(PreparedInsertDocument::new(id, Arc::new(document)))
    }

    fn generate_document_id(&self) -> ExecutionResult<DocumentId> {
        Ok(self.id_generator.next_id())
    }
}

impl Default for InsertDocumentMaterializer {
    fn default() -> Self {
        Self::new("insert")
    }
}

fn explicit_document_id(value: &LogicalValue) -> ExecutionResult<DocumentId> {
    let text = match value {
        LogicalValue::String(value) | LogicalValue::Identifier(value) => value.as_ref(),
        _ => {
            return Err(ExecutionError::mutation(
                "insert field \"_id\" must be a string or identifier",
            ));
        }
    };

    DocumentId::parse(text).map_err(|error| {
        ExecutionError::mutation(format!("insert field \"_id\" is invalid: {error}"))
    })
}

fn materialize_object(source: &LogicalObject, omit_root_id: bool) -> ExecutionResult<Document> {
    let mut document = Document::new();

    for field in source.fields() {
        if omit_root_id && field.name() == "_id" {
            continue;
        }

        document.insert(field.name(), materialize_value(field.value())?);
    }

    Ok(document)
}

fn materialize_value(source: &LogicalValue) -> ExecutionResult<Value> {
    match source {
        LogicalValue::String(value) | LogicalValue::Identifier(value) => {
            Ok(Value::string(Arc::clone(value)))
        }
        LogicalValue::Number(value) => materialize_number(value),
        LogicalValue::Boolean(value) => Ok(Value::bool(*value)),
        LogicalValue::Null => Ok(Value::null()),
        LogicalValue::Array(values) => values
            .iter()
            .map(materialize_value)
            .collect::<ExecutionResult<Vec<_>>>()
            .map(Value::array),
        LogicalValue::Object(object) => materialize_object(object, false).map(Value::object),
    }
}

fn materialize_number(source: &str) -> ExecutionResult<Value> {
    if source.starts_with('-') {
        if let Ok(value) = source.parse::<i64>() {
            return Ok(Number::signed(value).into_value());
        }
    } else if let Ok(value) = source.parse::<u64>() {
        return Ok(Number::unsigned(value).into_value());
    }

    let value = source.parse::<f64>().map_err(|error| {
        ExecutionError::mutation(format!(
            "logical number {source:?} cannot be represented physically: {error}"
        ))
    })?;

    Number::float(value)
        .map(Number::into_value)
        .map_err(|error| {
            ExecutionError::mutation(format!(
                "logical number {source:?} cannot be represented physically: {error}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materializes_explicit_identifier_without_storing_id_field() {
        let source =
            InsertDocument::parse(r#"{"_id":"01890f4c-0000-7000-8000-000000000001","name":"Alice","active":true,"score":1.5}"#)
                .unwrap();

        let prepared = InsertDocumentMaterializer::default()
            .materialize(&source)
            .unwrap();

        assert_eq!(
            prepared.id().to_string(),
            "01890f4c-0000-7000-8000-000000000001"
        );
        assert!(!prepared.document().contains_key("_id"));
        assert_eq!(
            prepared.document().get("name").and_then(Value::as_str),
            Some("Alice")
        );
        assert_eq!(
            prepared.document().get("active").and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn generates_distinct_identifiers_when_id_is_absent() {
        let source = InsertDocument::parse(r#"{"name":"Alice"}"#).unwrap();
        let materializer = InsertDocumentMaterializer::new("ogd");

        let first = materializer.materialize(&source).unwrap();
        let second = materializer.materialize(&source).unwrap();

        assert_ne!(first.id(), second.id());
        assert!(first.id() < second.id());
    }

    #[test]
    fn rejects_non_string_identifier() {
        let source = InsertDocument::parse(r#"{"_id":42}"#).unwrap();

        let error = InsertDocumentMaterializer::default()
            .materialize(&source)
            .unwrap_err();

        assert!(error.to_string().contains("must be a string or identifier"));
    }

    #[test]
    fn materializes_nested_values() {
        let source =
            InsertDocument::parse(r#"{"items":[1,-2,3.5,null,{"enabled":false}]}"#).unwrap();

        let prepared = InsertDocumentMaterializer::default()
            .materialize(&source)
            .unwrap();

        let items = prepared
            .document()
            .get("items")
            .and_then(Value::as_array)
            .unwrap();

        assert_eq!(items.len(), 5);
        assert!(items[3].is_null());
        assert_eq!(
            items[4]
                .as_object()
                .and_then(|document| document.get("enabled"))
                .and_then(Value::as_bool),
            Some(false)
        );
    }
}
