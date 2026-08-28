//! Logical query plan representation and validation.

use std::{fmt, sync::Arc};

use super::{
    BinaryOperator, Expression, ExpressionFieldPath, ExpressionKind, Literal,
    LogicalPlanFingerprint, StageName, UnaryOperator,
};

use crate::query::ast::{ObjectAst, ObjectKeyAst, ValueAst};
use crate::query::parser::parse_value_source;

/// Result returned by logical-plan operations.
pub type LogicalPlanResult<T> = std::result::Result<T, LogicalPlanError>;

/// Logical query plan.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPlan {
    source: LogicalSource,
    operators: Arc<[LogicalOperator]>,
}

impl LogicalPlan {
    /// Creates a validated logical plan.
    pub fn new<I>(source: LogicalSource, operators: I) -> LogicalPlanResult<Self>
    where
        I: IntoIterator<Item = LogicalOperator>,
    {
        let operators = operators.into_iter().collect::<Vec<_>>();
        validate_operators(&operators)?;

        Ok(Self {
            source,
            operators: Arc::from(operators),
        })
    }

    /// Starts building a plan from a collection.
    #[must_use]
    pub fn builder(source: LogicalSource) -> LogicalPlanBuilder {
        LogicalPlanBuilder::new(source)
    }

    /// Creates a plan containing only a collection scan.
    #[must_use]
    pub fn scan(source: LogicalSource) -> Self {
        Self {
            source,
            operators: Arc::from([]),
        }
    }

    #[must_use]
    #[inline]
    pub const fn source(&self) -> &LogicalSource {
        &self.source
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.operators.len()
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    #[must_use]
    #[inline]
    pub fn operator(&self, index: usize) -> Option<&LogicalOperator> {
        self.operators.get(index)
    }

    #[inline]
    pub fn operators(&self) -> impl ExactSizeIterator<Item = &LogicalOperator> {
        self.operators.iter()
    }

    #[must_use]
    pub fn is_mutating(&self) -> bool {
        self.operators.iter().any(LogicalOperator::is_mutating)
    }

    #[must_use]
    pub fn is_read_only(&self) -> bool {
        !self.is_mutating()
    }

    #[must_use]
    pub fn has_operator(&self, kind: LogicalOperatorKind) -> bool {
        self.operators
            .iter()
            .any(|operator| operator.kind() == kind)
    }

    #[must_use]
    pub fn has_filter(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Filter)
    }
    #[must_use]
    pub fn has_set(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Set)
    }
    #[must_use]
    pub fn has_load(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Load)
    }
    #[must_use]
    #[inline]
    pub fn has_limit(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Limit)
    }
    #[must_use]
    pub fn has_skip(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Skip)
    }
    #[must_use]
    #[inline]
    pub fn has_sort(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Sort)
    }
    #[must_use]
    #[inline]
    pub fn has_select(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Select)
    }
    #[must_use]
    pub fn has_distinct(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Distinct)
    }
    #[must_use]
    pub fn has_count(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Count)
    }
    #[must_use]
    pub fn has_delete(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Delete)
    }
    #[must_use]
    pub fn has_insert(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Insert)
    }
    #[must_use]
    #[inline]
    pub fn has_group(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Group)
    }
    #[must_use]
    pub fn has_pivot(&self) -> bool {
        self.has_operator(LogicalOperatorKind::Pivot)
    }

    /// Returns the canonical logical-plan representation.
    #[must_use]
    pub fn canonical_string(&self) -> String {
        let mut output = String::new();

        output.push_str("scan(");
        write_collection_name(&mut output, self.source.collection_name());
        output.push(')');

        for operator in self.operators.iter() {
            output.push(';');
            operator.write_canonical(&mut output);
        }

        output
    }

    #[must_use]
    pub fn fingerprint(&self) -> LogicalPlanFingerprint {
        LogicalPlanFingerprint::from_canonical_str(&self.canonical_string())
    }

    pub fn appended(&self, operator: LogicalOperator) -> LogicalPlanResult<Self> {
        let mut operators = self.operators.iter().cloned().collect::<Vec<_>>();
        operators.push(operator);
        Self::new(self.source.clone(), operators)
    }
}

/// Initial logical data source.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalSource {
    collection: CollectionName,
}

impl LogicalSource {
    #[must_use]
    #[inline]
    pub const fn new(collection: CollectionName) -> Self {
        Self { collection }
    }

    pub fn collection(name: impl AsRef<str>) -> LogicalPlanResult<Self> {
        Ok(Self::new(CollectionName::parse(name)?))
    }

    #[must_use]
    pub const fn collection_name(&self) -> &CollectionName {
        &self.collection
    }
}

/// Validated logical collection name.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CollectionName {
    segments: Arc<[Arc<str>]>,
}

impl CollectionName {
    pub fn parse(name: impl AsRef<str>) -> LogicalPlanResult<Self> {
        let name = name.as_ref();

        if name.is_empty() {
            return Err(LogicalPlanError::empty_collection_name());
        }

        let mut segments = Vec::new();

        for (index, segment) in name.split('.').enumerate() {
            if segment.is_empty() {
                return Err(LogicalPlanError::empty_collection_segment(index));
            }

            validate_identifier(segment, IdentifierContext::CollectionSegment(index))?;
            segments.push(Arc::<str>::from(segment));
        }

        Ok(Self {
            segments: Arc::from(segments),
        })
    }

    pub fn from_segments<I, S>(segments: I) -> LogicalPlanResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let segments = segments
            .into_iter()
            .map(|segment| segment.as_ref().to_owned())
            .collect::<Vec<_>>();

        if segments.is_empty() {
            return Err(LogicalPlanError::empty_collection_name());
        }

        Self::parse(segments.join("."))
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
    type Error = LogicalPlanError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for CollectionName {
    type Error = LogicalPlanError;

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

/// Owned logical value independent from parser spans and source text.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalValue {
    String(Arc<str>),
    Number(Arc<str>),
    Boolean(bool),
    Null,
    Identifier(Arc<str>),
    Array(Arc<[LogicalValue]>),
    Object(LogicalObject),
}

impl LogicalValue {
    pub fn from_source(source: impl AsRef<str>) -> LogicalPlanResult<Self> {
        let source = non_empty_text(source.as_ref(), LogicalPlanError::empty_value_source)?;
        let value = parse_value_source(source)
            .map_err(|error| LogicalPlanError::invalid_value(error.to_string()))?;
        logical_value_from_ast(&value, source)
    }

    #[must_use]
    pub const fn as_object(&self) -> Option<&LogicalObject> {
        match self {
            Self::Object(value) => Some(value),
            _ => None,
        }
    }

    fn write_canonical(&self, output: &mut String) {
        match self {
            Self::String(value) => {
                output.push_str("string(");
                write_string(output, value);
                output.push(')');
            }
            Self::Number(value) => {
                output.push_str("number(");
                write_string(output, value);
                output.push(')');
            }
            Self::Boolean(value) => {
                output.push_str(if *value { "bool(true)" } else { "bool(false)" })
            }
            Self::Null => output.push_str("null"),
            Self::Identifier(value) => {
                output.push_str("identifier(");
                write_string(output, value);
                output.push(')');
            }
            Self::Array(values) => {
                output.push_str("array(");
                write_joined(output, values, |output, value| {
                    value.write_canonical(output)
                });
                output.push(')');
            }
            Self::Object(value) => value.write_canonical(output),
        }
    }
}

/// One owned object field.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalObjectField {
    name: Arc<str>,
    value: LogicalValue,
}

impl LogicalObjectField {
    #[must_use]
    #[inline]
    pub fn new(name: impl Into<Arc<str>>, value: LogicalValue) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    #[must_use]
    pub const fn value(&self) -> &LogicalValue {
        &self.value
    }

    #[must_use]
    pub fn into_parts(self) -> (Arc<str>, LogicalValue) {
        (self.name, self.value)
    }
}

/// Owned object value with deterministic field order.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalObject {
    fields: Arc<[LogicalObjectField]>,
}

impl LogicalObject {
    pub fn new<I>(fields: I) -> LogicalPlanResult<Self>
    where
        I: IntoIterator<Item = LogicalObjectField>,
    {
        let fields = fields.into_iter().collect::<Vec<_>>();
        validate_unique_object_fields(&fields)?;
        Ok(Self {
            fields: Arc::from(fields),
        })
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    #[must_use]
    #[inline]
    pub fn field(&self, index: usize) -> Option<&LogicalObjectField> {
        self.fields.get(index)
    }

    pub fn fields(&self) -> impl ExactSizeIterator<Item = &LogicalObjectField> {
        self.fields.iter()
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&LogicalValue> {
        self.fields
            .iter()
            .find(|field| field.name() == name)
            .map(LogicalObjectField::value)
    }

    fn write_canonical(&self, output: &mut String) {
        output.push_str("object(");
        write_joined(output, self.fields.as_ref(), |output, field| {
            write_string(output, field.name());
            output.push('=');
            field.value().write_canonical(output);
        });
        output.push(')');
    }
}

/// Typed document accepted by `insert`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InsertDocument {
    object: LogicalObject,
}

impl InsertDocument {
    #[must_use]
    #[inline]
    pub const fn new(object: LogicalObject) -> Self {
        Self { object }
    }

    pub fn parse(source: impl AsRef<str>) -> LogicalPlanResult<Self> {
        let source = non_empty_text(
            source.as_ref(),
            LogicalPlanError::empty_insert_specification,
        )?;
        let value = parse_value_source(source)
            .map_err(|error| LogicalPlanError::invalid_insert_document(error.to_string()))?;
        let ValueAst::Object(object) = value else {
            return Err(LogicalPlanError::insert_document_not_object());
        };
        Ok(Self::new(logical_object_from_ast(&object, source)?))
    }

    #[must_use]
    pub const fn object(&self) -> &LogicalObject {
        &self.object
    }

    #[must_use]
    pub fn into_object(self) -> LogicalObject {
        self.object
    }
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
    ) -> LogicalPlanResult<Self> {
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
}

/// Fully resolved pivot contract.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PivotSpecification {
    rows: Arc<[ExpressionFieldPath]>,
    columns: Arc<[ExpressionFieldPath]>,
    values: Arc<[PivotValue]>,
}

impl PivotSpecification {
    pub fn new<R, C, V>(rows: R, columns: C, values: V) -> LogicalPlanResult<Self>
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

/// One logical pipeline operator.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum LogicalOperator {
    Filter {
        predicate: Expression,
    },
    Set {
        assignments: Arc<[SetAssignment]>,
    },
    Load {
        specification: Arc<str>,
    },
    Limit {
        count: usize,
    },
    Skip {
        count: usize,
    },
    Sort {
        keys: Arc<[SortKey]>,
    },
    Select {
        fields: Arc<[ExpressionFieldPath]>,
    },
    /// Empty fields mean distinct complete documents.
    Distinct {
        fields: Arc<[ExpressionFieldPath]>,
    },
    Count {
        alias: Arc<str>,
    },
    Delete,
    Insert {
        document: InsertDocument,
    },
    Group {
        keys: Arc<[ExpressionFieldPath]>,
    },
    Pivot {
        specification: PivotSpecification,
    },
    Custom {
        stage: StageName,
        arguments: Arc<str>,
        mutating: bool,
    },
}

impl LogicalOperator {
    #[must_use]
    pub fn filter(predicate: Expression) -> Self {
        Self::Filter { predicate }
    }

    pub fn set<I>(assignments: I) -> LogicalPlanResult<Self>
    where
        I: IntoIterator<Item = SetAssignment>,
    {
        let assignments = assignments.into_iter().collect::<Vec<_>>();
        validate_assignments(&assignments)?;

        Ok(Self::Set {
            assignments: Arc::from(assignments),
        })
    }

    pub fn load(specification: impl AsRef<str>) -> LogicalPlanResult<Self> {
        let specification = non_empty_text(
            specification.as_ref(),
            LogicalPlanError::empty_load_specification,
        )?;

        Ok(Self::Load {
            specification: Arc::from(specification),
        })
    }

    #[must_use]
    pub const fn limit(count: usize) -> Self {
        Self::Limit { count }
    }

    #[must_use]
    pub const fn skip(count: usize) -> Self {
        Self::Skip { count }
    }

    pub fn sort<I>(keys: I) -> LogicalPlanResult<Self>
    where
        I: IntoIterator<Item = SortKey>,
    {
        let keys = keys.into_iter().collect::<Vec<_>>();
        validate_unique_sort_keys(&keys)?;

        Ok(Self::Sort {
            keys: Arc::from(keys),
        })
    }

    pub fn select<I>(fields: I) -> LogicalPlanResult<Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        let fields = fields.into_iter().collect::<Vec<_>>();
        validate_unique_fields(&fields, FieldListContext::Select)?;

        Ok(Self::Select {
            fields: Arc::from(fields),
        })
    }

    pub fn distinct<I>(fields: I) -> LogicalPlanResult<Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        let fields = fields.into_iter().collect::<Vec<_>>();
        validate_optional_unique_fields(&fields, FieldListContext::Distinct)?;

        Ok(Self::Distinct {
            fields: Arc::from(fields),
        })
    }

    pub fn count(alias: impl AsRef<str>) -> LogicalPlanResult<Self> {
        let alias = alias.as_ref().trim();
        let alias = if alias.is_empty() { "count" } else { alias };

        validate_identifier(alias, IdentifierContext::CountAlias)?;

        Ok(Self::Count {
            alias: Arc::from(alias),
        })
    }

    #[must_use]
    pub const fn delete() -> Self {
        Self::Delete
    }

    pub fn insert(specification: impl AsRef<str>) -> LogicalPlanResult<Self> {
        Self::from_insert_document(InsertDocument::parse(specification)?)
    }

    pub fn from_insert_document(document: InsertDocument) -> LogicalPlanResult<Self> {
        Ok(Self::Insert { document })
    }

    pub fn group<I>(keys: I) -> LogicalPlanResult<Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        let keys = keys.into_iter().collect::<Vec<_>>();
        validate_unique_fields(&keys, FieldListContext::Group)?;

        Ok(Self::Group {
            keys: Arc::from(keys),
        })
    }

    pub fn pivot(specification: PivotSpecification) -> LogicalPlanResult<Self> {
        Ok(Self::Pivot { specification })
    }

    #[must_use]
    pub fn custom(stage: StageName, arguments: impl AsRef<str>, mutating: bool) -> Self {
        Self::Custom {
            stage,
            arguments: Arc::from(arguments.as_ref().trim()),
            mutating,
        }
    }

    #[must_use]
    pub const fn is_mutating(&self) -> bool {
        match self {
            Self::Set { .. } | Self::Load { .. } | Self::Delete | Self::Insert { .. } => true,
            Self::Custom { mutating, .. } => *mutating,
            Self::Filter { .. }
            | Self::Limit { .. }
            | Self::Skip { .. }
            | Self::Sort { .. }
            | Self::Select { .. }
            | Self::Distinct { .. }
            | Self::Count { .. }
            | Self::Group { .. }
            | Self::Pivot { .. } => false,
        }
    }

    #[must_use]
    pub const fn is_read_only(&self) -> bool {
        !self.is_mutating()
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Set { .. }
                | Self::Load { .. }
                | Self::Count { .. }
                | Self::Delete
                | Self::Insert { .. }
        ) || matches!(self, Self::Custom { mutating: true, .. })
    }

    #[must_use]
    pub const fn predicate(&self) -> Option<&Expression> {
        match self {
            Self::Filter { predicate } => Some(predicate),
            _ => None,
        }
    }

    #[must_use]
    pub fn assignments(&self) -> Option<&[SetAssignment]> {
        match self {
            Self::Set { assignments } => Some(assignments.as_ref()),
            _ => None,
        }
    }

    #[must_use]
    pub fn load_specification(&self) -> Option<&str> {
        match self {
            Self::Load { specification } => Some(specification.as_ref()),
            _ => None,
        }
    }

    #[must_use]
    pub const fn row_count(&self) -> Option<usize> {
        match self {
            Self::Limit { count } | Self::Skip { count } => Some(*count),
            _ => None,
        }
    }

    #[must_use]
    pub fn sort_keys(&self) -> Option<&[SortKey]> {
        match self {
            Self::Sort { keys } => Some(keys.as_ref()),
            _ => None,
        }
    }

    #[must_use]
    pub fn selected_fields(&self) -> Option<&[ExpressionFieldPath]> {
        match self {
            Self::Select { fields } => Some(fields.as_ref()),
            _ => None,
        }
    }

    #[must_use]
    pub fn distinct_fields(&self) -> Option<&[ExpressionFieldPath]> {
        match self {
            Self::Distinct { fields } => Some(fields.as_ref()),
            _ => None,
        }
    }

    #[must_use]
    pub fn count_alias(&self) -> Option<&str> {
        match self {
            Self::Count { alias } => Some(alias.as_ref()),
            _ => None,
        }
    }

    #[must_use]
    pub const fn insert_document(&self) -> Option<&InsertDocument> {
        match self {
            Self::Insert { document } => Some(document),
            _ => None,
        }
    }

    #[must_use]
    pub fn group_keys(&self) -> Option<&[ExpressionFieldPath]> {
        match self {
            Self::Group { keys } => Some(keys.as_ref()),
            _ => None,
        }
    }

    #[must_use]
    pub const fn pivot_specification(&self) -> Option<&PivotSpecification> {
        match self {
            Self::Pivot { specification } => Some(specification),
            _ => None,
        }
    }

    fn write_canonical(&self, output: &mut String) {
        match self {
            Self::Filter { predicate } => {
                output.push_str("filter(");
                write_expression(output, predicate);
                output.push(')');
            }
            Self::Set { assignments } => {
                output.push_str("set(");
                for (index, assignment) in assignments.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_field_path(output, assignment.field());
                    output.push('=');
                    write_expression(output, assignment.value());
                }
                output.push(')');
            }
            Self::Load { specification } => {
                output.push_str("load(");
                write_string(output, specification);
                output.push(')');
            }
            Self::Limit { count } => {
                output.push_str("limit(");
                output.push_str(&count.to_string());
                output.push(')');
            }
            Self::Skip { count } => {
                output.push_str("skip(");
                output.push_str(&count.to_string());
                output.push(')');
            }
            Self::Sort { keys } => {
                output.push_str("sort(");
                for (index, key) in keys.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_field_path(output, key.field());
                    output.push(':');
                    output.push_str(key.direction().as_str());
                }
                output.push(')');
            }
            Self::Select { fields } => {
                output.push_str("select(");
                write_field_list(output, fields);
                output.push(')');
            }
            Self::Distinct { fields } => {
                output.push_str("distinct(");
                if fields.is_empty() {
                    output.push_str("document");
                } else {
                    write_field_list(output, fields);
                }
                output.push(')');
            }
            Self::Count { alias } => {
                output.push_str("count(");
                write_string(output, alias);
                output.push(')');
            }
            Self::Delete => output.push_str("delete()"),
            Self::Insert { document } => {
                output.push_str("insert(");
                document.object().write_canonical(output);
                output.push(')');
            }
            Self::Group { keys } => {
                output.push_str("group(");
                write_field_list(output, keys);
                output.push(')');
            }
            Self::Pivot { specification } => {
                output.push_str("pivot(rows=");
                write_field_list(output, specification.rows());
                output.push_str(",columns=");
                write_field_list(output, specification.columns());
                output.push_str(",values=");
                write_joined(output, specification.values(), |output, value| {
                    write_field_path(output, value.field());
                    output.push(':');
                    output.push_str(value.aggregate().as_str());
                    if let Some(alias) = value.alias() {
                        output.push_str(":as=");
                        write_string(output, alias);
                    }
                });
                output.push(')');
            }
            Self::Custom {
                stage,
                arguments,
                mutating,
            } => {
                output.push_str("custom(");
                write_string(output, stage.as_str());
                output.push(',');
                output.push_str(if *mutating { "mutating" } else { "readonly" });
                output.push(',');
                write_string(output, arguments);
                output.push(')');
            }
        }
    }
}

/// One logical field assignment.
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

/// Incremental logical-plan builder.
#[derive(Clone, Debug)]
pub struct LogicalPlanBuilder {
    source: LogicalSource,
    operators: Vec<LogicalOperator>,
}

impl LogicalPlanBuilder {
    #[must_use]
    #[inline]
    pub const fn new(source: LogicalSource) -> Self {
        Self {
            source,
            operators: Vec::new(),
        }
    }

    #[must_use]
    #[inline]
    pub const fn source(&self) -> &LogicalSource {
        &self.source
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.operators.len()
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    #[must_use]
    #[inline]
    pub fn operator(&self, index: usize) -> Option<&LogicalOperator> {
        self.operators.get(index)
    }

    #[inline]
    pub fn operators(&self) -> impl ExactSizeIterator<Item = &LogicalOperator> {
        self.operators.iter()
    }

    pub fn push(&mut self, operator: LogicalOperator) -> LogicalPlanResult<&mut Self> {
        validate_next_operator(&self.operators, &operator)?;
        self.operators.push(operator);
        Ok(self)
    }

    pub fn filter(&mut self, predicate: Expression) -> LogicalPlanResult<&mut Self> {
        self.push(LogicalOperator::filter(predicate))
    }

    pub fn set<I>(&mut self, assignments: I) -> LogicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = SetAssignment>,
    {
        self.push(LogicalOperator::set(assignments)?)
    }

    pub fn load(&mut self, specification: impl AsRef<str>) -> LogicalPlanResult<&mut Self> {
        self.push(LogicalOperator::load(specification)?)
    }

    pub fn limit(&mut self, count: usize) -> LogicalPlanResult<&mut Self> {
        self.push(LogicalOperator::limit(count))
    }

    pub fn skip(&mut self, count: usize) -> LogicalPlanResult<&mut Self> {
        self.push(LogicalOperator::skip(count))
    }

    pub fn sort<I>(&mut self, keys: I) -> LogicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = SortKey>,
    {
        self.push(LogicalOperator::sort(keys)?)
    }

    pub fn select<I>(&mut self, fields: I) -> LogicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        self.push(LogicalOperator::select(fields)?)
    }

    pub fn distinct<I>(&mut self, fields: I) -> LogicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        self.push(LogicalOperator::distinct(fields)?)
    }

    pub fn count(&mut self, alias: impl AsRef<str>) -> LogicalPlanResult<&mut Self> {
        self.push(LogicalOperator::count(alias)?)
    }

    pub fn delete(&mut self) -> LogicalPlanResult<&mut Self> {
        self.push(LogicalOperator::delete())
    }

    pub fn insert(&mut self, specification: impl AsRef<str>) -> LogicalPlanResult<&mut Self> {
        self.push(LogicalOperator::insert(specification)?)
    }

    pub fn insert_document(&mut self, document: InsertDocument) -> LogicalPlanResult<&mut Self> {
        self.push(LogicalOperator::from_insert_document(document)?)
    }

    pub fn group<I>(&mut self, keys: I) -> LogicalPlanResult<&mut Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        self.push(LogicalOperator::group(keys)?)
    }

    pub fn pivot(&mut self, specification: PivotSpecification) -> LogicalPlanResult<&mut Self> {
        self.push(LogicalOperator::pivot(specification)?)
    }

    pub fn custom(
        &mut self,
        stage: StageName,
        arguments: impl AsRef<str>,
        mutating: bool,
    ) -> LogicalPlanResult<&mut Self> {
        self.push(LogicalOperator::custom(stage, arguments, mutating))
    }

    pub fn build(&self) -> LogicalPlanResult<LogicalPlan> {
        LogicalPlan::new(self.source.clone(), self.operators.clone())
    }

    pub fn finish(&self) -> LogicalPlanResult<LogicalPlan> {
        self.build()
    }
}

/// Logical-plan validation error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalPlanError {
    kind: LogicalPlanErrorKind,
}

impl LogicalPlanError {
    #[must_use]
    #[inline]
    pub const fn new(kind: LogicalPlanErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &LogicalPlanErrorKind {
        &self.kind
    }

    fn empty_collection_name() -> Self {
        Self::new(LogicalPlanErrorKind::EmptyCollectionName)
    }

    fn empty_collection_segment(index: usize) -> Self {
        Self::new(LogicalPlanErrorKind::EmptyCollectionSegment { index })
    }

    fn invalid_identifier_start(context: IdentifierContext, character: char) -> Self {
        Self::new(LogicalPlanErrorKind::InvalidIdentifierStart { context, character })
    }

    fn invalid_identifier_character(
        context: IdentifierContext,
        index: usize,
        character: char,
    ) -> Self {
        Self::new(LogicalPlanErrorKind::InvalidIdentifierCharacter {
            context,
            index,
            character,
        })
    }

    fn empty_set_assignments() -> Self {
        Self::new(LogicalPlanErrorKind::EmptySetAssignments)
    }

    fn duplicate_set_assignment(field: ExpressionFieldPath) -> Self {
        Self::new(LogicalPlanErrorKind::DuplicateSetAssignment { field })
    }

    fn empty_load_specification() -> Self {
        Self::new(LogicalPlanErrorKind::EmptyLoadSpecification)
    }

    fn empty_sort_keys() -> Self {
        Self::new(LogicalPlanErrorKind::EmptySortKeys)
    }

    fn duplicate_sort_key(field: ExpressionFieldPath) -> Self {
        Self::new(LogicalPlanErrorKind::DuplicateSortKey { field })
    }

    fn empty_field_list(context: FieldListContext) -> Self {
        Self::new(LogicalPlanErrorKind::EmptyFieldList { context })
    }

    fn duplicate_field(context: FieldListContext, field: ExpressionFieldPath) -> Self {
        Self::new(LogicalPlanErrorKind::DuplicateField { context, field })
    }

    fn invalid_count_alias(message: impl Into<Arc<str>>) -> Self {
        Self::new(LogicalPlanErrorKind::InvalidCountAlias {
            message: message.into(),
        })
    }

    fn empty_insert_specification() -> Self {
        Self::new(LogicalPlanErrorKind::EmptyInsertSpecification)
    }

    fn invalid_insert_document(message: impl Into<Arc<str>>) -> Self {
        Self::new(LogicalPlanErrorKind::InvalidInsertDocument {
            message: message.into(),
        })
    }

    fn insert_document_not_object() -> Self {
        Self::new(LogicalPlanErrorKind::InsertDocumentMustBeObject)
    }

    fn empty_value_source() -> Self {
        Self::new(LogicalPlanErrorKind::EmptyValueSource)
    }

    fn invalid_value(message: impl Into<Arc<str>>) -> Self {
        Self::new(LogicalPlanErrorKind::InvalidValue {
            message: message.into(),
        })
    }

    fn invalid_value_span() -> Self {
        Self::new(LogicalPlanErrorKind::InvalidValueSpan)
    }

    fn invalid_string_literal(message: impl Into<Arc<str>>) -> Self {
        Self::new(LogicalPlanErrorKind::InvalidStringLiteral {
            message: message.into(),
        })
    }

    fn duplicate_object_field(name: impl Into<Arc<str>>) -> Self {
        Self::new(LogicalPlanErrorKind::DuplicateObjectField { name: name.into() })
    }

    fn duplicate_pivot_value(field: ExpressionFieldPath) -> Self {
        Self::new(LogicalPlanErrorKind::DuplicatePivotValue { field })
    }

    fn duplicate_pivot_alias(alias: impl Into<Arc<str>>) -> Self {
        Self::new(LogicalPlanErrorKind::DuplicatePivotAlias {
            alias: alias.into(),
        })
    }

    fn overlapping_pivot_axis(field: ExpressionFieldPath) -> Self {
        Self::new(LogicalPlanErrorKind::OverlappingPivotAxis { field })
    }

    fn load_not_first(index: usize) -> Self {
        Self::new(LogicalPlanErrorKind::LoadMustBeFirst { index })
    }

    fn insert_not_only(index: usize) -> Self {
        Self::new(LogicalPlanErrorKind::InsertMustBeOnlyOperator { index })
    }

    fn duplicate_operator(kind: LogicalOperatorKind, index: usize) -> Self {
        Self::new(LogicalPlanErrorKind::DuplicateOperator { kind, index })
    }

    fn operator_after_terminal(index: usize) -> Self {
        Self::new(LogicalPlanErrorKind::OperatorAfterTerminal { index })
    }
}

impl fmt::Display for LogicalPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            LogicalPlanErrorKind::EmptyCollectionName => {
                formatter.write_str("logical collection name must not be empty")
            }
            LogicalPlanErrorKind::EmptyCollectionSegment { index } => {
                write!(
                    formatter,
                    "logical collection-name segment {index} must not be empty"
                )
            }
            LogicalPlanErrorKind::InvalidIdentifierStart { context, character } => {
                write!(
                    formatter,
                    "{context} must start with an alphabetic character or '_', found {character:?}",
                )
            }
            LogicalPlanErrorKind::InvalidIdentifierCharacter {
                context,
                index,
                character,
            } => {
                write!(
                    formatter,
                    "invalid character {character:?} at byte index {index} in {context}",
                )
            }
            LogicalPlanErrorKind::EmptySetAssignments => {
                formatter.write_str("logical set operator requires at least one assignment")
            }
            LogicalPlanErrorKind::DuplicateSetAssignment { field } => {
                write!(
                    formatter,
                    "logical set operator assigns field {field:?} more than once"
                )
            }
            LogicalPlanErrorKind::EmptyLoadSpecification => {
                formatter.write_str("logical load specification must not be empty")
            }
            LogicalPlanErrorKind::EmptySortKeys => {
                formatter.write_str("logical sort operator requires at least one key")
            }
            LogicalPlanErrorKind::DuplicateSortKey { field } => {
                write!(
                    formatter,
                    "logical sort operator uses field {field:?} more than once"
                )
            }
            LogicalPlanErrorKind::EmptyFieldList { context } => {
                write!(
                    formatter,
                    "logical {context} operator requires at least one field"
                )
            }
            LogicalPlanErrorKind::DuplicateField { context, field } => {
                write!(
                    formatter,
                    "logical {context} operator uses field {field:?} more than once"
                )
            }
            LogicalPlanErrorKind::InvalidCountAlias { message } => {
                write!(formatter, "invalid logical count alias: {message}")
            }
            LogicalPlanErrorKind::EmptyInsertSpecification => {
                formatter.write_str("logical insert specification must not be empty")
            }
            LogicalPlanErrorKind::InvalidInsertDocument { message } => {
                write!(formatter, "invalid logical insert document: {message}")
            }
            LogicalPlanErrorKind::InsertDocumentMustBeObject => {
                formatter.write_str("logical insert document must be an object value")
            }
            LogicalPlanErrorKind::EmptyValueSource => {
                formatter.write_str("logical value source must not be empty")
            }
            LogicalPlanErrorKind::InvalidValue { message } => {
                write!(formatter, "invalid logical value: {message}")
            }
            LogicalPlanErrorKind::InvalidValueSpan => {
                formatter.write_str("logical value contains a span outside its source")
            }
            LogicalPlanErrorKind::InvalidStringLiteral { message } => {
                write!(formatter, "invalid logical string literal: {message}")
            }
            LogicalPlanErrorKind::DuplicateObjectField { name } => {
                write!(
                    formatter,
                    "logical object contains duplicate field {name:?}"
                )
            }
            LogicalPlanErrorKind::DuplicatePivotValue { field } => {
                write!(
                    formatter,
                    "logical pivot uses value field {field:?} more than once"
                )
            }
            LogicalPlanErrorKind::DuplicatePivotAlias { alias } => {
                write!(
                    formatter,
                    "logical pivot uses alias {alias:?} more than once"
                )
            }
            LogicalPlanErrorKind::OverlappingPivotAxis { field } => {
                write!(
                    formatter,
                    "logical pivot field {field:?} is used by both rows and columns"
                )
            }
            LogicalPlanErrorKind::LoadMustBeFirst { index } => {
                write!(
                    formatter,
                    "logical load operator at index {index} must be first"
                )
            }
            LogicalPlanErrorKind::InsertMustBeOnlyOperator { index } => {
                write!(
                    formatter,
                    "logical insert operator at index {index} must be the only operator",
                )
            }
            LogicalPlanErrorKind::DuplicateOperator { kind, index } => {
                write!(
                    formatter,
                    "logical {kind} operator is duplicated at index {index}"
                )
            }
            LogicalPlanErrorKind::OperatorAfterTerminal { index } => {
                write!(
                    formatter,
                    "logical operator at index {index} appears after a terminal operator"
                )
            }
        }
    }
}

impl std::error::Error for LogicalPlanError {}

/// Detailed logical-plan error category.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LogicalPlanErrorKind {
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
    EmptySetAssignments,
    DuplicateSetAssignment {
        field: ExpressionFieldPath,
    },
    EmptyLoadSpecification,
    EmptySortKeys,
    DuplicateSortKey {
        field: ExpressionFieldPath,
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
    LoadMustBeFirst {
        index: usize,
    },
    InsertMustBeOnlyOperator {
        index: usize,
    },
    DuplicateOperator {
        kind: LogicalOperatorKind,
        index: usize,
    },
    OperatorAfterTerminal {
        index: usize,
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

/// Stable operator category used by diagnostics and optimizers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LogicalOperatorKind {
    Filter,
    Set,
    Load,
    Limit,
    Skip,
    Sort,
    Select,
    Distinct,
    Count,
    Delete,
    Insert,
    Group,
    Pivot,
    Custom,
}

impl fmt::Display for LogicalOperatorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Filter => "filter",
            Self::Set => "set",
            Self::Load => "load",
            Self::Limit => "limit",
            Self::Skip => "skip",
            Self::Sort => "sort",
            Self::Select => "select",
            Self::Distinct => "distinct",
            Self::Count => "count",
            Self::Delete => "delete",
            Self::Insert => "insert",
            Self::Group => "group",
            Self::Pivot => "pivot",
            Self::Custom => "custom",
        })
    }
}

impl LogicalOperator {
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> LogicalOperatorKind {
        match self {
            Self::Filter { .. } => LogicalOperatorKind::Filter,
            Self::Set { .. } => LogicalOperatorKind::Set,
            Self::Load { .. } => LogicalOperatorKind::Load,
            Self::Limit { .. } => LogicalOperatorKind::Limit,
            Self::Skip { .. } => LogicalOperatorKind::Skip,
            Self::Sort { .. } => LogicalOperatorKind::Sort,
            Self::Select { .. } => LogicalOperatorKind::Select,
            Self::Distinct { .. } => LogicalOperatorKind::Distinct,
            Self::Count { .. } => LogicalOperatorKind::Count,
            Self::Delete => LogicalOperatorKind::Delete,
            Self::Insert { .. } => LogicalOperatorKind::Insert,
            Self::Group { .. } => LogicalOperatorKind::Group,
            Self::Pivot { .. } => LogicalOperatorKind::Pivot,
            Self::Custom { .. } => LogicalOperatorKind::Custom,
        }
    }
}

fn non_empty_text<'a>(
    text: &'a str,
    error: fn() -> LogicalPlanError,
) -> LogicalPlanResult<&'a str> {
    let text = text.trim();
    if text.is_empty() {
        return Err(error());
    }
    Ok(text)
}

fn validate_identifier(identifier: &str, context: IdentifierContext) -> LogicalPlanResult<()> {
    let mut characters = identifier.char_indices();

    let Some((_, first)) = characters.next() else {
        return match context {
            IdentifierContext::CountAlias => Err(LogicalPlanError::invalid_count_alias(
                "alias must not be empty",
            )),
            IdentifierContext::PivotAlias => {
                Err(LogicalPlanError::invalid_identifier_start(context, '\0'))
            }
            IdentifierContext::CollectionSegment(_) => {
                Err(LogicalPlanError::empty_collection_name())
            }
        };
    };

    if !is_identifier_start(first) {
        return Err(LogicalPlanError::invalid_identifier_start(context, first));
    }

    for (index, character) in characters {
        if !is_identifier_continue(character) {
            return Err(LogicalPlanError::invalid_identifier_character(
                context, index, character,
            ));
        }
    }

    Ok(())
}

fn is_identifier_start(character: char) -> bool {
    character == '_' || character.is_alphabetic()
}

fn is_identifier_continue(character: char) -> bool {
    character == '_' || character.is_alphabetic() || character.is_ascii_digit()
}

fn validate_assignments(assignments: &[SetAssignment]) -> LogicalPlanResult<()> {
    if assignments.is_empty() {
        return Err(LogicalPlanError::empty_set_assignments());
    }

    for (index, assignment) in assignments.iter().enumerate() {
        if assignments[..index]
            .iter()
            .any(|previous| previous.field() == assignment.field())
        {
            return Err(LogicalPlanError::duplicate_set_assignment(
                assignment.field().clone(),
            ));
        }
    }

    Ok(())
}

fn validate_unique_sort_keys(keys: &[SortKey]) -> LogicalPlanResult<()> {
    if keys.is_empty() {
        return Err(LogicalPlanError::empty_sort_keys());
    }

    for (index, key) in keys.iter().enumerate() {
        if keys[..index]
            .iter()
            .any(|previous| previous.field() == key.field())
        {
            return Err(LogicalPlanError::duplicate_sort_key(key.field().clone()));
        }
    }

    Ok(())
}

fn validate_optional_unique_fields(
    fields: &[ExpressionFieldPath],
    context: FieldListContext,
) -> LogicalPlanResult<()> {
    for (index, field) in fields.iter().enumerate() {
        if fields[..index].iter().any(|previous| previous == field) {
            return Err(LogicalPlanError::duplicate_field(context, field.clone()));
        }
    }
    Ok(())
}

fn validate_unique_fields(
    fields: &[ExpressionFieldPath],
    context: FieldListContext,
) -> LogicalPlanResult<()> {
    if fields.is_empty() {
        return Err(LogicalPlanError::empty_field_list(context));
    }
    validate_optional_unique_fields(fields, context)
}

fn validate_unique_object_fields(fields: &[LogicalObjectField]) -> LogicalPlanResult<()> {
    for (index, field) in fields.iter().enumerate() {
        if fields[..index]
            .iter()
            .any(|previous| previous.name() == field.name())
        {
            return Err(LogicalPlanError::duplicate_object_field(field.name()));
        }
    }
    Ok(())
}

fn validate_pivot_values(values: &[PivotValue]) -> LogicalPlanResult<()> {
    if values.is_empty() {
        return Err(LogicalPlanError::empty_field_list(
            FieldListContext::PivotValues,
        ));
    }
    for (index, value) in values.iter().enumerate() {
        if values[..index]
            .iter()
            .any(|previous| previous.field() == value.field())
        {
            return Err(LogicalPlanError::duplicate_pivot_value(
                value.field().clone(),
            ));
        }
        if let Some(alias) = value.alias() {
            if values[..index]
                .iter()
                .filter_map(PivotValue::alias)
                .any(|previous| previous == alias)
            {
                return Err(LogicalPlanError::duplicate_pivot_alias(alias));
            }
        }
    }
    Ok(())
}

fn validate_disjoint_fields(
    rows: &[ExpressionFieldPath],
    columns: &[ExpressionFieldPath],
) -> LogicalPlanResult<()> {
    for field in rows {
        if columns.iter().any(|column| column == field) {
            return Err(LogicalPlanError::overlapping_pivot_axis(field.clone()));
        }
    }
    Ok(())
}

fn normalize_optional_identifier<A>(
    value: Option<A>,
    context: IdentifierContext,
) -> LogicalPlanResult<Option<Arc<str>>>
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

fn logical_object_from_ast(object: &ObjectAst, source: &str) -> LogicalPlanResult<LogicalObject> {
    let fields = object
        .fields()
        .iter()
        .map(|field| {
            let name = object_key_text(field.key(), source)?;
            let value = logical_value_from_ast(field.value(), source)?;
            Ok(LogicalObjectField::new(name, value))
        })
        .collect::<LogicalPlanResult<Vec<_>>>()?;
    LogicalObject::new(fields)
}

fn logical_value_from_ast(value: &ValueAst, source: &str) -> LogicalPlanResult<LogicalValue> {
    match value {
        ValueAst::String(value) => Ok(LogicalValue::String(Arc::from(decode_string_literal(
            value
                .text(source)
                .ok_or_else(LogicalPlanError::invalid_value_span)?,
        )?))),
        ValueAst::Number(value) => Ok(LogicalValue::Number(Arc::from(
            value
                .text(source)
                .ok_or_else(LogicalPlanError::invalid_value_span)?,
        ))),
        ValueAst::Boolean(value) => Ok(LogicalValue::Boolean(value.value())),
        ValueAst::Null(_) => Ok(LogicalValue::Null),
        ValueAst::Identifier(value) => Ok(LogicalValue::Identifier(Arc::from(
            value
                .text(source)
                .ok_or_else(LogicalPlanError::invalid_value_span)?,
        ))),
        ValueAst::Array(value) => {
            let values = value
                .values()
                .iter()
                .map(|value| logical_value_from_ast(value, source))
                .collect::<LogicalPlanResult<Vec<_>>>()?;
            Ok(LogicalValue::Array(Arc::from(values)))
        }
        ValueAst::Object(value) => Ok(LogicalValue::Object(logical_object_from_ast(
            value, source,
        )?)),
    }
}

fn object_key_text(key: ObjectKeyAst, source: &str) -> LogicalPlanResult<Arc<str>> {
    let text = key
        .text(source)
        .ok_or_else(LogicalPlanError::invalid_value_span)?;
    match key {
        ObjectKeyAst::Identifier(_) => Ok(Arc::from(text)),
        ObjectKeyAst::String(_) => Ok(Arc::from(decode_string_literal(text)?)),
    }
}

fn decode_string_literal(literal: &str) -> LogicalPlanResult<String> {
    let bytes = literal.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'"' || bytes[bytes.len() - 1] != b'"' {
        return Err(LogicalPlanError::invalid_string_literal(
            "expected double-quoted string",
        ));
    }

    let mut output = String::with_capacity(literal.len().saturating_sub(2));
    let mut characters = literal[1..literal.len() - 1].chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            output.push(character);
            continue;
        }
        let escaped = characters.next().ok_or_else(|| {
            LogicalPlanError::invalid_string_literal("unterminated escape sequence")
        })?;
        output.push(match escaped {
            '"' => '"',
            '\\' => '\\',
            '/' => '/',
            'b' => '\u{0008}',
            'f' => '\u{000c}',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            other => {
                return Err(LogicalPlanError::invalid_string_literal(format!(
                    "unsupported escape sequence \\\\{other}"
                )))
            }
        });
    }
    Ok(output)
}

fn validate_operators(operators: &[LogicalOperator]) -> LogicalPlanResult<()> {
    for (index, operator) in operators.iter().enumerate() {
        validate_next_operator(&operators[..index], operator)?;
    }
    Ok(())
}

fn validate_next_operator(
    existing: &[LogicalOperator],
    next: &LogicalOperator,
) -> LogicalPlanResult<()> {
    let next_index = existing.len();

    if matches!(next, LogicalOperator::Load { .. }) && next_index != 0 {
        return Err(LogicalPlanError::load_not_first(next_index));
    }

    if matches!(next, LogicalOperator::Insert { .. }) && next_index != 0 {
        return Err(LogicalPlanError::insert_not_only(next_index));
    }

    if existing
        .iter()
        .any(|operator| matches!(operator, LogicalOperator::Insert { .. }))
    {
        return Err(LogicalPlanError::insert_not_only(next_index));
    }

    if let Some(previous) = existing.last() {
        if previous.is_terminal() {
            return Err(LogicalPlanError::operator_after_terminal(next_index));
        }
    }

    let next_kind = next.kind();
    let unique = matches!(
        next_kind,
        LogicalOperatorKind::Load
            | LogicalOperatorKind::Limit
            | LogicalOperatorKind::Skip
            | LogicalOperatorKind::Sort
            | LogicalOperatorKind::Select
            | LogicalOperatorKind::Distinct
            | LogicalOperatorKind::Count
            | LogicalOperatorKind::Delete
            | LogicalOperatorKind::Insert
            | LogicalOperatorKind::Group
            | LogicalOperatorKind::Pivot
    );

    if unique && existing.iter().any(|operator| operator.kind() == next_kind) {
        return Err(LogicalPlanError::duplicate_operator(next_kind, next_index));
    }

    Ok(())
}

fn write_joined<T>(output: &mut String, values: &[T], mut write: impl FnMut(&mut String, &T)) {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write(output, value);
    }
}

fn write_collection_name(output: &mut String, collection: &CollectionName) {
    write_string(output, &collection.to_string());
}

fn write_field_list(output: &mut String, fields: &[ExpressionFieldPath]) {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_field_path(output, field);
    }
}

fn write_field_path(output: &mut String, path: &ExpressionFieldPath) {
    output.push_str("field(");
    for (index, segment) in path.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_string(output, segment);
    }
    output.push(')');
}

fn write_expression(output: &mut String, expression: &Expression) {
    match expression.kind() {
        ExpressionKind::Literal(literal) => write_literal(output, literal),
        ExpressionKind::Field(path) => write_field_path(output, path),
        ExpressionKind::Unary { operator, operand } => {
            output.push_str("unary(");
            output.push_str(canonical_unary_operator(*operator));
            output.push(',');
            write_expression(output, operand);
            output.push(')');
        }
        ExpressionKind::Binary {
            left,
            operator,
            right,
        } => {
            output.push_str("binary(");
            output.push_str(canonical_binary_operator(*operator));
            output.push(',');
            write_expression(output, left);
            output.push(',');
            write_expression(output, right);
            output.push(')');
        }
        ExpressionKind::Group(inner) => write_expression(output, inner),
    }
}

fn write_literal(output: &mut String, literal: &Literal) {
    match literal {
        Literal::Null => output.push_str("null"),
        Literal::Bool(value) => {
            output.push_str(if *value { "bool(true)" } else { "bool(false)" });
        }
        Literal::Number(value) => {
            output.push_str("number(");
            write_string(output, value);
            output.push(')');
        }
        Literal::String(value) => {
            output.push_str("string(");
            write_string(output, value);
            output.push(')');
        }
        Literal::Json(value) => {
            output.push_str("json(");
            write_string(output, value);
            output.push(')');
        }
    }
}

fn canonical_unary_operator(operator: UnaryOperator) -> &'static str {
    match operator {
        UnaryOperator::Not => "not",
        UnaryOperator::Negate => "negate",
        UnaryOperator::Positive => "positive",
    }
}

fn canonical_binary_operator(operator: BinaryOperator) -> &'static str {
    match operator {
        BinaryOperator::Or => "or",
        BinaryOperator::And => "and",
        BinaryOperator::Equal => "equal",
        BinaryOperator::NotEqual => "not-equal",
        BinaryOperator::LessThan => "less-than",
        BinaryOperator::LessThanOrEqual => "less-than-or-equal",
        BinaryOperator::GreaterThan => "greater-than",
        BinaryOperator::GreaterThanOrEqual => "greater-than-or-equal",
        BinaryOperator::Add => "add",
        BinaryOperator::Subtract => "subtract",
        BinaryOperator::Multiply => "multiply",
        BinaryOperator::Divide => "divide",
        BinaryOperator::Remainder => "remainder",
    }
}

fn write_string(output: &mut String, value: &str) {
    output.push_str(&value.len().to_string());
    output.push(':');
    output.push_str(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{parse_expression, ExpressionFieldPath};

    fn users_source() -> LogicalSource {
        LogicalSource::collection("users").unwrap()
    }

    #[inline]
    fn field(path: &[&str]) -> ExpressionFieldPath {
        ExpressionFieldPath::new(path.iter().copied()).unwrap()
    }

    fn assignment(path: &[&str], value: &str) -> SetAssignment {
        SetAssignment::new(field(path), parse_expression(value).unwrap())
    }

    #[test]
    fn supports_complete_native_stage_set() {
        let mut plan = LogicalPlan::builder(users_source());
        plan.filter(parse_expression("active == true").unwrap())
            .unwrap();
        plan.sort([
            SortKey::descending(field(&["age"])),
            SortKey::ascending(field(&["name"])),
        ])
        .unwrap();
        plan.skip(10).unwrap();
        plan.limit(20).unwrap();
        plan.select([field(&["name"]), field(&["age"])]).unwrap();
        plan.distinct([field(&["name"])]).unwrap();

        let plan = plan.finish().unwrap();
        assert_eq!(plan.len(), 6);
        assert!(plan.has_filter());
        assert!(plan.has_sort());
        assert!(plan.has_skip());
        assert!(plan.has_limit());
        assert!(plan.has_select());
        assert!(plan.has_distinct());
        assert!(plan.is_read_only());
    }

    #[test]
    fn supports_group_as_chainable_and_count_as_terminal() {
        let group = LogicalPlan::builder(users_source())
            .group([field(&["country"])])
            .unwrap()
            .finish()
            .unwrap();
        assert!(group.has_group());
        assert!(!group.operator(0).unwrap().is_terminal());

        let chained = LogicalPlan::builder(users_source())
            .group([field(&["country"])])
            .unwrap()
            .sort([SortKey::descending(field(&["count"]))])
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(chained.len(), 2);
        assert!(chained.has_group());
        assert!(chained.has_sort());

        let count = LogicalPlan::builder(users_source())
            .count("total")
            .unwrap()
            .finish()
            .unwrap();
        assert!(count.has_count());
        assert_eq!(count.operator(0).unwrap().count_alias(), Some("total"));
    }

    #[test]
    fn supports_delete_and_insert_mutations() {
        let delete = LogicalPlan::builder(users_source())
            .delete()
            .unwrap()
            .finish()
            .unwrap();
        assert!(delete.has_delete());
        assert!(delete.is_mutating());

        let insert = LogicalPlan::builder(users_source())
            .insert("{name:\"Alice\"}")
            .unwrap()
            .finish()
            .unwrap();
        assert!(insert.has_insert());
        assert!(insert.is_mutating());
    }

    #[test]
    fn retains_existing_filter_set_and_load_apis() {
        let filter = LogicalPlan::builder(users_source())
            .filter(parse_expression("age >= 18").unwrap())
            .unwrap()
            .finish()
            .unwrap();
        assert!(filter.has_filter());

        let set = LogicalPlan::builder(users_source())
            .set([assignment(&["enabled"], "true")])
            .unwrap()
            .finish()
            .unwrap();
        assert!(set.has_set());

        let load = LogicalPlan::builder(users_source())
            .load("profile")
            .unwrap()
            .finish()
            .unwrap();
        assert!(load.has_load());
    }

    #[test]
    fn validates_operator_arguments() {
        assert!(matches!(
            LogicalOperator::sort([]).unwrap_err().kind(),
            LogicalPlanErrorKind::EmptySortKeys
        ));
        assert!(matches!(
            LogicalOperator::select([]).unwrap_err().kind(),
            LogicalPlanErrorKind::EmptyFieldList {
                context: FieldListContext::Select
            }
        ));
        assert!(matches!(
            LogicalOperator::group([]).unwrap_err().kind(),
            LogicalPlanErrorKind::EmptyFieldList {
                context: FieldListContext::Group
            }
        ));
        assert!(matches!(
            LogicalOperator::insert(" ").unwrap_err().kind(),
            LogicalPlanErrorKind::EmptyInsertSpecification
        ));
    }

    #[test]
    fn validates_nested_insert_object() {
        let operator = LogicalOperator::insert(
            r#"{
                _id: "u1",
                active: true,
                tags: ["rust", "database"],
                address: {city: "Paris"},
            }"#,
        )
        .unwrap();

        assert!(operator.insert_document().is_some());
    }

    #[test]
    fn rejects_non_object_insert_values() {
        assert!(matches!(
            LogicalOperator::insert(r#""Alice""#).unwrap_err().kind(),
            LogicalPlanErrorKind::InsertDocumentMustBeObject
        ));

        assert!(matches!(
            LogicalOperator::insert("[{name: \"Alice\"}]")
                .unwrap_err()
                .kind(),
            LogicalPlanErrorKind::InsertDocumentMustBeObject
        ));
    }

    #[test]
    fn rejects_malformed_insert_document() {
        assert!(matches!(
            LogicalOperator::insert(r#"{name "Alice"}"#)
                .unwrap_err()
                .kind(),
            LogicalPlanErrorKind::InvalidInsertDocument { .. }
        ));
    }

    #[test]
    fn rejects_duplicate_field_arguments() {
        let repeated = field(&["country"]);

        assert!(matches!(
            LogicalOperator::sort([
                SortKey::ascending(repeated.clone()),
                SortKey::descending(repeated.clone()),
            ])
            .unwrap_err()
            .kind(),
            LogicalPlanErrorKind::DuplicateSortKey { .. }
        ));

        assert!(matches!(
            LogicalOperator::select([repeated.clone(), repeated])
                .unwrap_err()
                .kind(),
            LogicalPlanErrorKind::DuplicateField {
                context: FieldListContext::Select,
                ..
            }
        ));
    }

    #[test]
    fn enforces_terminal_and_unique_operators() {
        let mut builder = LogicalPlan::builder(users_source());
        builder.limit(10).unwrap();

        assert!(matches!(
            builder.limit(20).unwrap_err().kind(),
            LogicalPlanErrorKind::DuplicateOperator {
                kind: LogicalOperatorKind::Limit,
                ..
            }
        ));

        let mut builder = LogicalPlan::builder(users_source());
        builder.count("count").unwrap();

        assert!(matches!(
            builder.limit(1).unwrap_err().kind(),
            LogicalPlanErrorKind::OperatorAfterTerminal { .. }
        ));
    }

    #[test]
    fn supports_typed_pivot() {
        let specification = PivotSpecification::new(
            [field(&["region"])],
            [field(&["month"])],
            [PivotValue::new(field(&["revenue"]), PivotAggregate::Sum, None::<&str>).unwrap()],
        )
        .unwrap();

        let plan = LogicalPlan::builder(users_source())
            .pivot(specification)
            .unwrap()
            .finish()
            .unwrap();

        assert!(plan.has_pivot());
        assert!(plan.is_read_only());
        assert_eq!(
            plan.operator(0)
                .unwrap()
                .pivot_specification()
                .unwrap()
                .values()
                .len(),
            1
        );
    }

    #[test]
    fn canonical_plan_covers_all_native_operators() {
        let plan = LogicalPlan::builder(users_source())
            .sort([SortKey::descending(field(&["age"]))])
            .unwrap()
            .skip(2)
            .unwrap()
            .limit(5)
            .unwrap()
            .select([field(&["name"])])
            .unwrap()
            .distinct([])
            .unwrap()
            .finish()
            .unwrap();

        assert_eq!(
            plan.canonical_string(),
            "scan(5:users);sort(field(3:age):desc);skip(2);limit(5);select(field(4:name));distinct(document)",
        );
    }
}
