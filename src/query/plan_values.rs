#![cfg_attr(rustfmt, rustfmt_skip)]
//! Values the planner validates and plans carry: collection names, sort
//! keys, insert documents, pivot specifications, set assignments.

use std::{fmt, sync::Arc};

use super::{Expression, ExpressionFieldPath};
use crate::{Document, Number, Value};

use crate::query::ast::{ObjectAst, ObjectKeyAst, ValueAst};
use crate::query::parser::parse_value_source;

/// Result of validating those values.
pub type PlanValueResult<T> = std::result::Result<T, PlanValueError>;

/// Validated collection name.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CollectionName {
    segments: Arc<[Arc<str>]>,
}

impl CollectionName {
    pub fn parse(name: impl AsRef<str>) -> PlanValueResult<Self> {
        let name = name.as_ref();

        if name.is_empty() {
            return Err(PlanValueError::empty_collection_name());
        }

        let mut segments = Vec::new();

        for (index, segment) in name.split('.').enumerate() {
            if segment.is_empty() {
                return Err(PlanValueError::empty_collection_segment(index));
            }

            validate_identifier(segment, IdentifierContext::CollectionSegment(index))?;
            segments.push(Arc::<str>::from(segment));
        }

        Ok(Self {
            segments: Arc::from(segments),
        })
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    #[must_use]
    pub fn segment(&self, index: usize) -> Option<&str> {
        self.segments.get(index).map(AsRef::as_ref)
    }

    #[must_use]
    pub fn first(&self) -> &str {
        self.segments
            .first()
            .expect("validated collection names are never empty")
            .as_ref()
    }

    #[must_use]
    pub fn last(&self) -> &str {
        self.segments
            .last()
            .expect("validated collection names are never empty")
            .as_ref()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &str> {
        self.segments.iter().map(AsRef::as_ref)
    }

    #[must_use]
    pub fn is_system(&self) -> bool {
        self.first() == "_og"
    }
}

impl fmt::Debug for CollectionName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CollectionName")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for CollectionName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, segment) in self.segments.iter().enumerate() {
            if index > 0 {
                formatter.write_str(".")?;
            }
            formatter.write_str(segment)?;
        }
        Ok(())
    }
}

impl TryFrom<&str> for CollectionName {
    type Error = PlanValueError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for CollectionName {
    type Error = PlanValueError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

/// Sort order for one key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SortDirection {
    #[default]
    Ascending,
    Descending,
}

impl SortDirection {
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ascending => "asc",
            Self::Descending => "desc",
        }
    }
}

/// One typed sort key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SortKey {
    field: ExpressionFieldPath,
    direction: SortDirection,
}

impl SortKey {
    #[must_use]
    #[inline]
    pub const fn new(field: ExpressionFieldPath, direction: SortDirection) -> Self {
        Self { field, direction }
    }

    #[must_use]
    pub const fn ascending(field: ExpressionFieldPath) -> Self {
        Self::new(field, SortDirection::Ascending)
    }

    #[must_use]
    pub const fn descending(field: ExpressionFieldPath) -> Self {
        Self::new(field, SortDirection::Descending)
    }

    #[must_use]
    #[inline]
    pub const fn field(&self) -> &ExpressionFieldPath {
        &self.field
    }

    #[must_use]
    #[inline]
    pub const fn direction(&self) -> SortDirection {
        self.direction
    }
}

/// Typed document accepted by `insert`, `_id` included when given.
#[derive(Clone, Debug, PartialEq)]
pub struct InsertDocument {
    document: Arc<Document>,
}

impl InsertDocument {
    pub fn parse(source: impl AsRef<str>) -> PlanValueResult<Self> {
        let source = crate::helpers::non_empty_text(
            source.as_ref(),
            PlanValueError::empty_insert_specification,
        )?;
        let value = parse_value_source(source)
            .map_err(|error| PlanValueError::invalid_insert_document(error.to_string()))?;
        let ValueAst::Object(object) = value else {
            return Err(PlanValueError::insert_document_not_object());
        };
        Ok(Self { document: Arc::new(document_from_ast(&object, source, insert_number)?) })
    }

    #[must_use]
    pub fn document(&self) -> &Document {
        &self.document
    }
}

/// The value a literal (`[…]`, `{…}` with bare or quoted keys, scalars)
/// denotes, numbers read as by a value coercion.
pub(crate) fn value_from_source(source: &str) -> PlanValueResult<Value> {
    let source = crate::helpers::non_empty_text(source, PlanValueError::empty_value_source)?;
    let value = parse_value_source(source).map_err(|error| PlanValueError::invalid_value(error.to_string()))?;
    value_from_ast(&value, source, |text| {
        crate::model::parse_number_value(text).map(Value::Number).map_err(|error| PlanValueError::invalid_value(format!("invalid number {text:?}: {error}")))
    })
}

/// An `insert` number: a non-negative integer is unsigned, a negative one
/// signed, anything else a float.
fn insert_number(text: &str) -> PlanValueResult<Value> {
    if text.starts_with('-') {
        if let Ok(value) = text.parse::<i64>() {
            return Ok(Number::signed(value).into_value());
        }
    } else if let Ok(value) = text.parse::<u64>() {
        return Ok(Number::unsigned(value).into_value());
    }
    let invalid = |error: String| PlanValueError::invalid_value(format!("number {text:?} cannot be represented: {error}"));
    let value = text.parse::<f64>().map_err(|error| invalid(error.to_string()))?;
    Number::float(value).map(Number::into_value).map_err(|error| invalid(error.to_string()))
}

/// Supported pivot aggregation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PivotAggregate {
    Sum,
    First,
    Last,
    Count,
    Average,
    Minimum,
    Maximum,
    /// `first … by <order>` of a compound `group`: the value of the row of
    /// smallest order.
    FirstBy,
    /// `last … by <order>`: the value of the row of largest order.
    LastBy,
    /// `list`: the array of the values of the group's rows, in input order.
    List,
}

impl PivotAggregate {
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::First => "first",
            Self::Last => "last",
            Self::Count => "count",
            Self::Average => "avg",
            Self::Minimum => "min",
            Self::Maximum => "max",
            Self::FirstBy => "firstby",
            Self::LastBy => "lastby",
            Self::List => "list",
        }
    }
}

/// One pivot measure.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PivotValue {
    field: ExpressionFieldPath,
    aggregate: PivotAggregate,
    alias: Option<Arc<str>>,
}

impl PivotValue {
    #[inline]
    pub fn new(
        field: ExpressionFieldPath,
        aggregate: PivotAggregate,
        alias: Option<impl AsRef<str>>,
    ) -> PlanValueResult<Self> {
        let alias = normalize_optional_identifier(alias, IdentifierContext::PivotAlias)?;
        Ok(Self {
            field,
            aggregate,
            alias,
        })
    }

    #[must_use]
    #[inline]
    pub const fn field(&self) -> &ExpressionFieldPath {
        &self.field
    }

    #[must_use]
    pub const fn aggregate(&self) -> PivotAggregate {
        self.aggregate
    }

    #[must_use]
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    /// Name of the cells this value fills: its alias, else its field's last segment.
    #[must_use]
    pub fn output_name(&self) -> &str {
        self.alias().unwrap_or_else(|| self.field.last())
    }
}

/// Fully resolved pivot contract.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PivotSpecification {
    rows: Arc<[ExpressionFieldPath]>,
    columns: Arc<[ExpressionFieldPath]>,
    values: Arc<[PivotValue]>,
}

impl PivotSpecification {
    pub fn new<R, C, V>(rows: R, columns: C, values: V) -> PlanValueResult<Self>
    where
        R: IntoIterator<Item = ExpressionFieldPath>,
        C: IntoIterator<Item = ExpressionFieldPath>,
        V: IntoIterator<Item = PivotValue>,
    {
        let rows = rows.into_iter().collect::<Vec<_>>();
        let columns = columns.into_iter().collect::<Vec<_>>();
        let values = values.into_iter().collect::<Vec<_>>();

        validate_unique_fields(&rows, FieldListContext::PivotRows)?;
        validate_unique_fields(&columns, FieldListContext::PivotColumns)?;
        validate_pivot_values(&values)?;
        validate_disjoint_fields(&rows, &columns)?;

        Ok(Self {
            rows: Arc::from(rows),
            columns: Arc::from(columns),
            values: Arc::from(values),
        })
    }

    #[must_use]
    pub fn rows(&self) -> &[ExpressionFieldPath] {
        self.rows.as_ref()
    }

    #[must_use]
    pub fn columns(&self) -> &[ExpressionFieldPath] {
        self.columns.as_ref()
    }

    #[must_use]
    pub fn values(&self) -> &[PivotValue] {
        self.values.as_ref()
    }
}

/// One field assignment.
#[derive(Clone, Debug, PartialEq)]
pub struct SetAssignment {
    field: ExpressionFieldPath,
    value: Expression,
}

impl SetAssignment {
    #[must_use]
    #[inline]
    pub const fn new(field: ExpressionFieldPath, value: Expression) -> Self {
        Self { field, value }
    }

    #[must_use]
    #[inline]
    pub const fn field(&self) -> &ExpressionFieldPath {
        &self.field
    }

    #[must_use]
    pub const fn value(&self) -> &Expression {
        &self.value
    }

    #[must_use]
    pub fn into_parts(self) -> (ExpressionFieldPath, Expression) {
        (self.field, self.value)
    }
}

/// Logical-plan validation error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanValueError {
    kind: PlanValueErrorKind,
}

impl PlanValueError {
    #[must_use]
    #[inline]
    pub const fn new(kind: PlanValueErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &PlanValueErrorKind {
        &self.kind
    }

    const fn empty_collection_name() -> Self {
        Self::new(PlanValueErrorKind::EmptyCollectionName)
    }

    const fn empty_collection_segment(index: usize) -> Self {
        Self::new(PlanValueErrorKind::EmptyCollectionSegment { index })
    }

    const fn invalid_identifier_start(context: IdentifierContext, character: char) -> Self {
        Self::new(PlanValueErrorKind::InvalidIdentifierStart { context, character })
    }

    const fn invalid_identifier_character(
        context: IdentifierContext,
        index: usize,
        character: char,
    ) -> Self {
        Self::new(PlanValueErrorKind::InvalidIdentifierCharacter {
            context,
            index,
            character,
        })
    }






    const fn empty_field_list(context: FieldListContext) -> Self {
        Self::new(PlanValueErrorKind::EmptyFieldList { context })
    }

    const fn duplicate_field(context: FieldListContext, field: ExpressionFieldPath) -> Self {
        Self::new(PlanValueErrorKind::DuplicateField { context, field })
    }

    fn invalid_count_alias(message: impl Into<Arc<str>>) -> Self {
        Self::new(PlanValueErrorKind::InvalidCountAlias {
            message: message.into(),
        })
    }

    const fn empty_insert_specification() -> Self {
        Self::new(PlanValueErrorKind::EmptyInsertSpecification)
    }

    fn invalid_insert_document(message: impl Into<Arc<str>>) -> Self {
        Self::new(PlanValueErrorKind::InvalidInsertDocument {
            message: message.into(),
        })
    }

    const fn insert_document_not_object() -> Self {
        Self::new(PlanValueErrorKind::InsertDocumentMustBeObject)
    }

    const fn empty_value_source() -> Self {
        Self::new(PlanValueErrorKind::EmptyValueSource)
    }

    fn invalid_value(message: impl Into<Arc<str>>) -> Self {
        Self::new(PlanValueErrorKind::InvalidValue {
            message: message.into(),
        })
    }

    const fn invalid_value_span() -> Self {
        Self::new(PlanValueErrorKind::InvalidValueSpan)
    }

    fn invalid_string_literal(message: impl Into<Arc<str>>) -> Self {
        Self::new(PlanValueErrorKind::InvalidStringLiteral {
            message: message.into(),
        })
    }

    fn duplicate_object_field(name: impl Into<Arc<str>>) -> Self {
        Self::new(PlanValueErrorKind::DuplicateObjectField { name: name.into() })
    }

    const fn duplicate_pivot_value(field: ExpressionFieldPath) -> Self {
        Self::new(PlanValueErrorKind::DuplicatePivotValue { field })
    }

    fn duplicate_pivot_alias(alias: impl Into<Arc<str>>) -> Self {
        Self::new(PlanValueErrorKind::DuplicatePivotAlias {
            alias: alias.into(),
        })
    }

    const fn overlapping_pivot_axis(field: ExpressionFieldPath) -> Self {
        Self::new(PlanValueErrorKind::OverlappingPivotAxis { field })
    }

}

impl fmt::Display for PlanValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            PlanValueErrorKind::EmptyCollectionName => {
                formatter.write_str("collection name must not be empty")
            }
            PlanValueErrorKind::EmptyCollectionSegment { index } => {
                write!(
                    formatter,
                    "collection-name segment {index} must not be empty"
                )
            }
            PlanValueErrorKind::InvalidIdentifierStart { context, character } => {
                write!(
                    formatter,
                    "{context} must start with an alphabetic character or '_', found {character:?}",
                )
            }
            PlanValueErrorKind::InvalidIdentifierCharacter {
                context,
                index,
                character,
            } => {
                write!(
                    formatter,
                    "invalid character {character:?} at byte index {index} in {context}",
                )
            }
            PlanValueErrorKind::EmptyFieldList { context } => {
                write!(
                    formatter,
                    "{context} operator requires at least one field"
                )
            }
            PlanValueErrorKind::DuplicateField { context, field } => {
                write!(
                    formatter,
                    "{context} operator uses field {field:?} more than once"
                )
            }
            PlanValueErrorKind::InvalidCountAlias { message } => {
                write!(formatter, "invalid count alias: {message}")
            }
            PlanValueErrorKind::EmptyInsertSpecification => {
                formatter.write_str("insert specification must not be empty")
            }
            PlanValueErrorKind::InvalidInsertDocument { message } => {
                write!(formatter, "invalid insert document: {message}")
            }
            PlanValueErrorKind::InsertDocumentMustBeObject => {
                formatter.write_str("insert document must be an object value")
            }
            PlanValueErrorKind::EmptyValueSource => {
                formatter.write_str("value source must not be empty")
            }
            PlanValueErrorKind::InvalidValue { message } => {
                write!(formatter, "invalid value: {message}")
            }
            PlanValueErrorKind::InvalidValueSpan => {
                formatter.write_str("value contains a span outside its source")
            }
            PlanValueErrorKind::InvalidStringLiteral { message } => {
                write!(formatter, "invalid string literal: {message}")
            }
            PlanValueErrorKind::DuplicateObjectField { name } => {
                write!(
                    formatter,
                    "object contains duplicate field {name:?}"
                )
            }
            PlanValueErrorKind::DuplicatePivotValue { field } => {
                write!(
                    formatter,
                    "pivot uses value field {field:?} more than once"
                )
            }
            PlanValueErrorKind::DuplicatePivotAlias { alias } => {
                write!(
                    formatter,
                    "pivot uses alias {alias:?} more than once"
                )
            }
            PlanValueErrorKind::OverlappingPivotAxis { field } => {
                write!(
                    formatter,
                    "pivot field {field:?} is used by both rows and columns"
                )
            }
        }
    }
}

impl std::error::Error for PlanValueError {}

/// Detailed category of a value error.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PlanValueErrorKind {
    EmptyCollectionName,
    EmptyCollectionSegment {
        index: usize,
    },
    InvalidIdentifierStart {
        context: IdentifierContext,
        character: char,
    },
    InvalidIdentifierCharacter {
        context: IdentifierContext,
        index: usize,
        character: char,
    },
    EmptyFieldList {
        context: FieldListContext,
    },
    DuplicateField {
        context: FieldListContext,
        field: ExpressionFieldPath,
    },
    InvalidCountAlias {
        message: Arc<str>,
    },
    EmptyInsertSpecification,
    InvalidInsertDocument {
        message: Arc<str>,
    },
    InsertDocumentMustBeObject,
    EmptyValueSource,
    InvalidValue {
        message: Arc<str>,
    },
    InvalidValueSpan,
    InvalidStringLiteral {
        message: Arc<str>,
    },
    DuplicateObjectField {
        name: Arc<str>,
    },
    DuplicatePivotValue {
        field: ExpressionFieldPath,
    },
    DuplicatePivotAlias {
        alias: Arc<str>,
    },
    OverlappingPivotAxis {
        field: ExpressionFieldPath,
    },
}

/// Identifier location used in diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IdentifierContext {
    CollectionSegment(usize),
    CountAlias,
    PivotAlias,
}

impl fmt::Display for IdentifierContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CollectionSegment(index) => write!(formatter, "collection-name segment {index}"),
            Self::CountAlias => formatter.write_str("count alias"),
            Self::PivotAlias => formatter.write_str("pivot alias"),
        }
    }
}

/// Context for a list of field paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FieldListContext {
    Select,
    Distinct,
    Group,
    PivotRows,
    PivotColumns,
    PivotValues,
}

impl fmt::Display for FieldListContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Select => formatter.write_str("select"),
            Self::Distinct => formatter.write_str("distinct"),
            Self::Group => formatter.write_str("group"),
            Self::PivotRows => formatter.write_str("pivot rows"),
            Self::PivotColumns => formatter.write_str("pivot columns"),
            Self::PivotValues => formatter.write_str("pivot values"),
        }
    }
}

fn validate_identifier(identifier: &str, context: IdentifierContext) -> PlanValueResult<()> {
    let mut characters = identifier.char_indices();
    let Some((_, first)) = characters.next() else {
        return match context {
            IdentifierContext::CountAlias => Err(PlanValueError::invalid_count_alias(
                "alias must not be empty",
            )),
            IdentifierContext::PivotAlias => {
                Err(PlanValueError::invalid_identifier_start(context, '\0'))
            }
            IdentifierContext::CollectionSegment(_) => {
                Err(PlanValueError::empty_collection_name())
            }
        };
    };

    if !crate::helpers::is_identifier_start(first) {
        return Err(PlanValueError::invalid_identifier_start(context, first));
    }

    for (index, character) in characters {
        if !crate::helpers::is_identifier_continue(character) {
            return Err(PlanValueError::invalid_identifier_character(
                context, index, character,
            ));
        }
    }

    Ok(())
}



fn validate_optional_unique_fields( fields: &[ExpressionFieldPath], context: FieldListContext, ) -> PlanValueResult<()> {
    for (index, field) in fields.iter().enumerate() {
        if fields[..index].iter().any(|previous| previous == field) {
            return Err(PlanValueError::duplicate_field(context, field.clone()));
        }
    }
    Ok(())
}

fn validate_unique_fields( fields: &[ExpressionFieldPath], context: FieldListContext, ) -> PlanValueResult<()> {
    if fields.is_empty() {
        return Err(PlanValueError::empty_field_list(context));
    }
    validate_optional_unique_fields(fields, context)
}

fn validate_pivot_values(values: &[PivotValue]) -> PlanValueResult<()> {
    if values.is_empty() {
        return Err(PlanValueError::empty_field_list(
            FieldListContext::PivotValues,
        ));
    }
    // Each value names its cells: two values with the same name would share them.
    for (index, value) in values.iter().enumerate() {
        let Some(previous) = values[..index]
            .iter()
            .find(|previous| previous.output_name() == value.output_name())
        else {
            continue;
        };
        return Err(match (previous.alias(), value.alias()) {
            (None, None) if previous.field() == value.field() => {
                PlanValueError::duplicate_pivot_value(value.field().clone())
            }
            _ => PlanValueError::duplicate_pivot_alias(value.output_name()),
        });
    }
    Ok(())
}

fn validate_disjoint_fields( rows: &[ExpressionFieldPath], columns: &[ExpressionFieldPath], ) -> PlanValueResult<()> {
    for field in rows {
        if columns.iter().any(|column| column == field) {
            return Err(PlanValueError::overlapping_pivot_axis(field.clone()));
        }
    }
    Ok(())
}

fn normalize_optional_identifier<A>(
    value: Option<A>,
    context: IdentifierContext,
) -> PlanValueResult<Option<Arc<str>>>
where
    A: AsRef<str>,
{
    match value {
        Some(value) => {
            let value = value.as_ref().trim();
            if value.is_empty() {
                return Ok(None);
            }
            validate_identifier(value, context)?;
            Ok(Some(Arc::from(value)))
        }
        None => Ok(None),
    }
}

fn document_from_ast(object: &ObjectAst, source: &str, number: fn(&str) -> PlanValueResult<Value>) -> PlanValueResult<Document> {
    let mut document = Document::new();
    for field in object.fields() {
        let name = object_key_text(field.key(), source)?;
        if document.insert(name.as_ref(), value_from_ast(field.value(), source, number)?).is_some() {
            return Err(PlanValueError::duplicate_object_field(name));
        }
    }
    Ok(document)
}

fn value_from_ast(value: &ValueAst, source: &str, number: fn(&str) -> PlanValueResult<Value>) -> PlanValueResult<Value> {
    match value {
        ValueAst::String(value) => Ok(Value::string(decode_string_literal(value.text(source).ok_or_else(PlanValueError::invalid_value_span)?)?)),
        ValueAst::Number(value) => number(value.text(source).ok_or_else(PlanValueError::invalid_value_span)?),
        ValueAst::Boolean(value) => Ok(Value::bool(value.value())),
        ValueAst::Null(_) => Ok(Value::null()),
        ValueAst::Identifier(value) => Ok(Value::string(value.text(source).ok_or_else(PlanValueError::invalid_value_span)?)),
        ValueAst::Array(value) => value
            .values()
            .iter()
            .map(|value| value_from_ast(value, source, number))
            .collect::<PlanValueResult<Vec<_>>>()
            .map(Value::array),
        ValueAst::Object(value) => document_from_ast(value, source, number).map(Value::object),
    }
}

fn object_key_text(key: ObjectKeyAst, source: &str) -> PlanValueResult<Arc<str>> {
    let text = key
        .text(source)
        .ok_or_else(PlanValueError::invalid_value_span)?;
    match key {
        ObjectKeyAst::Identifier(_) => Ok(Arc::from(text)),
        ObjectKeyAst::String(_) => Ok(Arc::from(decode_string_literal(text)?)),
    }
}

fn decode_string_literal(literal: &str) -> PlanValueResult<String> {
    let inner = literal
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .filter(|_| literal.len() >= 2)
        .ok_or_else(|| PlanValueError::invalid_string_literal("expected double-quoted string"))?;
    super::syntax::unescape_string(inner).map_err(|character| {
        PlanValueError::invalid_string_literal(format!("unsupported escape sequence \\{character}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn validates_nested_insert_object() { let document = InsertDocument::parse( "{\n                _id: \"u1\",\n                active: true,\n                tags: [\"rust\", \"database\"],\n                address: {city: \"Paris\"},\n            }", ) .unwrap(); assert_eq!(document.document().len(), 4); }
    #[test] fn rejects_non_object_insert_values() { assert!(matches!( InsertDocument::parse(r#""Alice""#).unwrap_err().kind(), PlanValueErrorKind::InsertDocumentMustBeObject )); assert!(matches!( InsertDocument::parse("[{name: \"Alice\"}]") .unwrap_err() .kind(), PlanValueErrorKind::InsertDocumentMustBeObject )); }
    #[test] fn rejects_malformed_insert_document() { assert!(matches!( InsertDocument::parse(r#"{name "Alice"}"#) .unwrap_err() .kind(), PlanValueErrorKind::InvalidInsertDocument { .. } )); }
}
