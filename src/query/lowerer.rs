//! Logical-to-physical plan lowering.

use std::{error::Error as StdError, fmt, sync::Arc};

use crate::{
    engine::PlanLowerer,
    storage::{CollectionId, DocumentId},
};

use super::{
    parse_expression, BinaryOperator, Expression, ExpressionFieldPath, Literal, PhysicalAccess,
    PhysicalLoadMode, PhysicalOperator, PhysicalPlan, PhysicalPlanError, PhysicalPlanErrorKind,
    PhysicalPlanner, PhysicalSubPipeline, SortKey, StageName,
};

use super::logical_plan::{LogicalOperator, LogicalPlan};
use super::syntax::{parse_sort_item, split_top_level};

/// Result returned by logical-to-physical lowering.
pub type LoweringResult<T> = std::result::Result<T, LoweringError>;

/// Maximum supported nesting depth for compound stages.
///
/// The semantic planner's diagnostic path currently retains eight levels, so
/// the lowerer uses the same practical bound.
const MAX_COMPOUND_DEPTH: usize = 8;

/// Default logical-to-physical lowerer.
///
/// `ScanPlanLowerer` preserves logical operator order and delegates source
/// access selection to [`PhysicalPlanner`].
#[derive(Clone, Copy, Debug, Default)]
pub struct ScanPlanLowerer;

impl ScanPlanLowerer {
    /// Creates the default lowerer.
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self
    }

    /// Lowers a logical plan using the supplied physical planner.
    ///
    /// # Errors
    ///
    /// Returns a lowering diagnostic when a logical payload is malformed, or a
    /// physical-plan diagnostic when the resulting operator sequence is invalid.
    pub fn lower_plan_detailed(
        &self,
        logical: &LogicalPlan,
        physical_planner: &PhysicalPlanner,
    ) -> LoweringResult<PhysicalPlan> {
        let collection = collection_id(logical)?;
        let mut builder = physical_planner.plan_collection(collection);

        // `_id` is the mandatory master index for every collection. Select a
        // direct lookup when the leading filter is a UUID-v7 equality. The
        // filter remains in the operator stream as a residual semantic check.
        if let Some(id) = leading_primary_key(logical) {
            builder.set_source_access(PhysicalAccess::PrimaryKeyLookup { id });
        }

        for (operator_index, operator) in logical.operators().enumerate() {
            let physical = lower_operator(operator, 0)
                .map_err(|error| error.with_operator_index(operator_index))?;

            builder
                .push(physical)
                .map_err(|error| LoweringError::physical(Some(operator_index), error))?;
        }

        builder
            .finish()
            .map_err(|error| LoweringError::physical(None, error))
    }

    /// Compatibility API used by the engine trait.
    ///
    /// Detailed payload diagnostics are converted into
    /// [`PhysicalPlanErrorKind::InvalidLoweringPayload`].
    pub fn lower_plan(
        &self,
        logical: &LogicalPlan,
        physical_planner: &PhysicalPlanner,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        self.lower_plan_detailed(logical, physical_planner)
            .map_err(Into::into)
    }
}

impl PlanLowerer for ScanPlanLowerer {
    fn lower(
        &self,
        logical: &LogicalPlan,
        physical_planner: &PhysicalPlanner,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        self.lower_plan(logical, physical_planner)
    }
}

fn leading_primary_key(logical: &LogicalPlan) -> Option<DocumentId> {
    let LogicalOperator::Filter { predicate } = logical.operator(0)? else {
        return None;
    };
    primary_key_equality(predicate)
}

fn primary_key_equality(expression: &Expression) -> Option<DocumentId> {
    let (left, operator, right) = expression.ungrouped().as_binary()?;
    if operator != BinaryOperator::Equal {
        return None;
    }
    primary_key_pair(left.ungrouped(), right.ungrouped())
        .or_else(|| primary_key_pair(right.ungrouped(), left.ungrouped()))
}

fn primary_key_pair(field: &Expression, literal: &Expression) -> Option<DocumentId> {
    let path = field.as_field()?;
    if path.len() != 1 || path.first() != "_id" {
        return None;
    }
    let Literal::String(value) = literal.as_literal()? else {
        return None;
    };
    DocumentId::parse(value.as_ref()).ok()
}

/// Logical-to-physical lowering diagnostic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoweringError {
    kind: LoweringErrorKind,
    operator_index: Option<usize>,
}

impl LoweringError {
    #[must_use]
    #[inline]
    pub const fn new(kind: LoweringErrorKind) -> Self {
        Self {
            kind,
            operator_index: None,
        }
    }

    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &LoweringErrorKind {
        &self.kind
    }

    #[must_use]
    pub const fn operator_index(&self) -> Option<usize> {
        self.operator_index
    }

    fn with_operator_index(mut self, operator_index: usize) -> Self {
        if self.operator_index.is_none() {
            self.operator_index = Some(operator_index);
        }
        self
    }

    fn physical(operator_index: Option<usize>, error: PhysicalPlanError) -> Self {
        Self {
            kind: LoweringErrorKind::PhysicalPlan {
                message: Arc::from(error.to_string()),
            },
            operator_index,
        }
    }

    fn invalid_payload(stage: &str, message: impl Into<Arc<str>>) -> Self {
        Self::new(LoweringErrorKind::InvalidNativePayload {
            stage: Arc::from(stage),
            message: message.into(),
        })
    }

    fn invalid_nested_stage(stage: &str, message: impl Into<Arc<str>>) -> Self {
        Self::new(LoweringErrorKind::InvalidNestedStage {
            stage: Arc::from(stage),
            message: message.into(),
        })
    }
}

impl fmt::Display for LoweringError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(index) = self.operator_index {
            write!(formatter, "logical operator {index}: ")?;
        }

        match &self.kind {
            LoweringErrorKind::InvalidCollection { collection, message } => write!(
                formatter,
                "validated logical collection {collection:?} cannot be represented by storage: {message}",
            ),

            LoweringErrorKind::InvalidNativePayload { stage, message } => {
                write!(formatter, "invalid native {stage:?} payload: {message}")
            }

            LoweringErrorKind::InvalidNestedStage { stage, message } => {
                write!(formatter, "cannot lower nested stage {stage:?}: {message}")
            }

            LoweringErrorKind::NestingLimitExceeded { limit } => write!(
                formatter,
                "compound-stage nesting exceeds the supported limit of {limit}",
            ),

            LoweringErrorKind::PhysicalPlan { message } => {
                write!(formatter, "physical-plan validation failed: {message}")
            }
        }
    }
}

impl StdError for LoweringError {}

/// Detailed lowering diagnostic category.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LoweringErrorKind {
    InvalidCollection {
        collection: Arc<str>,
        message: Arc<str>,
    },
    InvalidNativePayload {
        stage: Arc<str>,
        message: Arc<str>,
    },
    InvalidNestedStage {
        stage: Arc<str>,
        message: Arc<str>,
    },
    NestingLimitExceeded {
        limit: usize,
    },
    PhysicalPlan {
        message: Arc<str>,
    },
}

impl From<LoweringError> for PhysicalPlanError {
    fn from(error: LoweringError) -> Self {
        PhysicalPlanError::new(PhysicalPlanErrorKind::InvalidCustomArguments {
            stage: Arc::from(format!("lowering: {error}")),
        })
    }
}

fn collection_id(logical: &LogicalPlan) -> LoweringResult<CollectionId> {
    let collection = logical.source().collection_name().to_string();

    CollectionId::parse(&collection).map_err(|error| {
        LoweringError::new(LoweringErrorKind::InvalidCollection {
            collection: Arc::from(collection),
            message: Arc::from(error.to_string()),
        })
    })
}

fn lower_operator(operator: &LogicalOperator, depth: usize) -> LoweringResult<PhysicalOperator> {
    if depth > MAX_COMPOUND_DEPTH {
        return Err(LoweringError::new(
            LoweringErrorKind::NestingLimitExceeded {
                limit: MAX_COMPOUND_DEPTH,
            },
        ));
    }

    match operator {
        LogicalOperator::Filter { predicate } => Ok(PhysicalOperator::filter(predicate.clone())),

        LogicalOperator::Set { assignments } => PhysicalOperator::set(assignments.iter().cloned())
            .map_err(|error| LoweringError::physical(None, error)),

        LogicalOperator::Load { specification } => lower_load(specification.as_ref()),

        LogicalOperator::Limit { count } => Ok(PhysicalOperator::limit(*count)),

        LogicalOperator::Skip { count } => Ok(PhysicalOperator::skip(*count)),

        LogicalOperator::Sort { keys } => PhysicalOperator::sort(keys.iter().cloned())
            .map_err(|error| LoweringError::physical(None, error)),

        LogicalOperator::Select { fields } => PhysicalOperator::select(fields.iter().cloned())
            .map_err(|error| LoweringError::physical(None, error)),

        LogicalOperator::Distinct { fields } => PhysicalOperator::distinct(fields.iter().cloned())
            .map_err(|error| LoweringError::physical(None, error)),

        LogicalOperator::Count { alias } => PhysicalOperator::count(alias.as_ref())
            .map_err(|error| LoweringError::physical(None, error)),

        LogicalOperator::Delete => Ok(PhysicalOperator::delete()),

        LogicalOperator::Insert { document } => Ok(PhysicalOperator::insert(document.clone())),

        LogicalOperator::Group { keys } => PhysicalOperator::group(keys.iter().cloned())
            .map_err(|error| LoweringError::physical(None, error)),

        LogicalOperator::Pivot { specification } => {
            Ok(PhysicalOperator::pivot(specification.clone()))
        }

        LogicalOperator::Custom {
            stage,
            arguments,
            mutating,
        } => match stage.as_str() {
            "lookup" => lower_lookup(arguments.as_ref(), depth + 1),
            "union" => lower_union(arguments.as_ref(), depth + 1),
            "sample" => PhysicalOperator::custom(stage.clone(), arguments.as_ref(), false, true)
                .map_err(|error| LoweringError::physical(None, error)),

            _ => PhysicalOperator::custom(stage.clone(), arguments.as_ref(), *mutating, false)
                .map_err(|error| LoweringError::physical(None, error)),
        },
    }
}

fn lower_load(specification: &str) -> LoweringResult<PhysicalOperator> {
    if !specification.starts_with("streaming;") {
        return PhysicalOperator::load(specification)
            .map_err(|error| LoweringError::physical(None, error));
    }

    let payload = parse_streaming_load_payload(specification)?;
    PhysicalOperator::streaming_load(payload.mode, payload.chunks)
        .map_err(|error| LoweringError::physical(None, error))
}

fn lower_lookup(payload: &str, depth: usize) -> LoweringResult<PhysicalOperator> {
    if depth > MAX_COMPOUND_DEPTH {
        return Err(LoweringError::new(
            LoweringErrorKind::NestingLimitExceeded {
                limit: MAX_COMPOUND_DEPTH,
            },
        ));
    }

    let payload = parse_lookup_payload(payload)?;
    let collection = parse_collection_id("lookup", payload.source)?;
    let pipeline = lower_encoded_subpipeline("lookup", &payload.stages, depth)?;

    PhysicalOperator::lookup(collection, payload.alias, payload.into, pipeline)
        .map_err(|error| LoweringError::physical(None, error))
}

fn lower_union(payload: &str, depth: usize) -> LoweringResult<PhysicalOperator> {
    if depth > MAX_COMPOUND_DEPTH {
        return Err(LoweringError::new(
            LoweringErrorKind::NestingLimitExceeded {
                limit: MAX_COMPOUND_DEPTH,
            },
        ));
    }

    let payload = parse_union_payload(payload)?;
    let collection = parse_collection_id("union", payload.source)?;
    let pipeline = lower_encoded_subpipeline("union", &payload.stages, depth)?;

    PhysicalOperator::union(collection, payload.alias, pipeline)
        .map_err(|error| LoweringError::physical(None, error))
}

fn parse_collection_id(stage: &str, source: &str) -> LoweringResult<CollectionId> {
    CollectionId::parse(source).map_err(|error| {
        LoweringError::invalid_payload(stage, format!("invalid collection {source:?}: {error}"))
    })
}

fn lower_encoded_subpipeline(
    parent: &str,
    stages: &[EncodedStage<'_>],
    depth: usize,
) -> LoweringResult<PhysicalSubPipeline> {
    let mut operators = Vec::with_capacity(stages.len());

    for stage in stages {
        operators.push(lower_encoded_stage(parent, stage, depth)?);
    }

    PhysicalSubPipeline::new(operators).map_err(|error| LoweringError::physical(None, error))
}

fn lower_encoded_stage(
    parent: &str,
    stage: &EncodedStage<'_>,
    depth: usize,
) -> LoweringResult<PhysicalOperator> {
    match stage.name {
        "where" => {
            let expression = required_nested_arguments(stage)?;
            let predicate = parse_expression(expression).map_err(|error| {
                LoweringError::invalid_nested_stage(
                    stage.name,
                    format!("invalid expression {expression:?}: {error}"),
                )
            })?;

            Ok(PhysicalOperator::filter(predicate))
        }

        "limit" => {
            let count = parse_nested_usize(stage)?;
            Ok(PhysicalOperator::limit(count))
        }

        "skip" => {
            let count = parse_nested_usize(stage)?;
            Ok(PhysicalOperator::skip(count))
        }

        "sort" => {
            let keys = parse_nested_sort(stage)?;
            PhysicalOperator::sort(keys).map_err(|error| LoweringError::physical(None, error))
        }

        "select" => {
            let fields = parse_nested_fields(stage, false)?;
            PhysicalOperator::select(fields).map_err(|error| LoweringError::physical(None, error))
        }

        "distinct" => {
            let fields = parse_nested_fields(stage, true)?;
            PhysicalOperator::distinct(fields).map_err(|error| LoweringError::physical(None, error))
        }

        "lookup" => lower_lookup(stage.arguments, depth + 1),

        "union" => lower_union(stage.arguments, depth + 1),

        other => {
            let name = StageName::parse(other).map_err(|error| {
                LoweringError::invalid_nested_stage(other, format!("invalid stage name: {error}"))
            })?;

            PhysicalOperator::custom(name, stage.arguments, false, false)
                .map_err(|error| LoweringError::physical(None, error))
        }
    }
    .map_err(|error| match error.kind() {
        LoweringErrorKind::InvalidNestedStage { .. } => error,
        _ => LoweringError::invalid_nested_stage(stage.name, format!("inside {parent}: {error}")),
    })
}

fn required_nested_arguments<'a>(stage: &'a EncodedStage<'a>) -> LoweringResult<&'a str> {
    let arguments = stage.arguments.trim();

    if arguments.is_empty() {
        return Err(LoweringError::invalid_nested_stage(
            stage.name,
            "stage requires arguments",
        ));
    }

    Ok(arguments)
}

fn parse_nested_usize(stage: &EncodedStage<'_>) -> LoweringResult<usize> {
    let arguments = required_nested_arguments(stage)?;

    if arguments.split_whitespace().count() != 1 {
        return Err(LoweringError::invalid_nested_stage(
            stage.name,
            "expected exactly one non-negative integer",
        ));
    }

    arguments.parse::<usize>().map_err(|_| {
        LoweringError::invalid_nested_stage(
            stage.name,
            "expected a non-negative integer representable as usize",
        )
    })
}

fn parse_nested_sort(stage: &EncodedStage<'_>) -> LoweringResult<Vec<SortKey>> {
    let arguments = required_nested_arguments(stage)?;
    let items = split_top_level(arguments, ',')
        .map_err(|message| LoweringError::invalid_nested_stage(stage.name, message))?;

    let mut keys = Vec::with_capacity(items.len());

    for item in items {
        let (field, direction) = parse_sort_item(item)
            .map_err(|message| LoweringError::invalid_nested_stage(stage.name, message))?;

        let field = parse_field_path(field)
            .map_err(|message| LoweringError::invalid_nested_stage(stage.name, message))?;

        keys.push(SortKey::new(field, direction));
    }

    Ok(keys)
}

fn parse_nested_fields(
    stage: &EncodedStage<'_>,
    allow_empty: bool,
) -> LoweringResult<Vec<ExpressionFieldPath>> {
    let arguments = stage.arguments.trim();

    if arguments.is_empty() {
        if allow_empty {
            return Ok(Vec::new());
        }

        return Err(LoweringError::invalid_nested_stage(
            stage.name,
            "stage requires at least one field",
        ));
    }

    split_top_level(arguments, ',')
        .map_err(|message| LoweringError::invalid_nested_stage(stage.name, message))?
        .into_iter()
        .map(|field| {
            parse_field_path(field)
                .map_err(|message| LoweringError::invalid_nested_stage(stage.name, message))
        })
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LookupPayload<'a> {
    source: &'a str,
    alias: Option<&'a str>,
    into: &'a str,
    stages: Vec<EncodedStage<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UnionPayload<'a> {
    source: &'a str,
    alias: Option<&'a str>,
    stages: Vec<EncodedStage<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StreamingLoadPayload<'a> {
    mode: PhysicalLoadMode,
    chunks: Vec<&'a str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EncodedStage<'a> {
    name: &'a str,
    arguments: &'a str,
}

fn parse_lookup_payload(payload: &str) -> LoweringResult<LookupPayload<'_>> {
    let mut cursor = Cursor::new(payload);

    cursor.expect_literal("source=", "lookup")?;
    let source = cursor.read_length_prefixed("lookup source")?;

    cursor.expect_literal(";alias=", "lookup")?;
    let alias = cursor.read_optional_length_prefixed("lookup alias")?;

    cursor.expect_literal(";into=", "lookup")?;
    let into = cursor.read_length_prefixed("lookup target")?;

    cursor.expect_literal(";pipeline=", "lookup")?;
    let stages = cursor.read_stage_list("lookup")?;

    cursor.expect_end("lookup")?;

    Ok(LookupPayload {
        source,
        alias,
        into,
        stages,
    })
}

fn parse_union_payload(payload: &str) -> LoweringResult<UnionPayload<'_>> {
    let mut cursor = Cursor::new(payload);

    cursor.expect_literal("source=", "union")?;
    let source = cursor.read_length_prefixed("union source")?;

    cursor.expect_literal(";alias=", "union")?;
    let alias = cursor.read_optional_length_prefixed("union alias")?;

    cursor.expect_literal(";pipeline=", "union")?;
    let stages = cursor.read_stage_list("union")?;

    cursor.expect_end("union")?;

    Ok(UnionPayload {
        source,
        alias,
        stages,
    })
}

fn parse_streaming_load_payload(payload: &str) -> LoweringResult<StreamingLoadPayload<'_>> {
    let mut cursor = Cursor::new(payload);

    cursor.expect_literal("streaming;mode=", "load")?;
    let mode_text = cursor.read_until(';', "load mode")?;
    let mode = match mode_text {
        "replace" => PhysicalLoadMode::Replace,
        "update" => PhysicalLoadMode::Update,
        "merge" => PhysicalLoadMode::Merge,
        _ => {
            return Err(LoweringError::invalid_payload(
                "load",
                format!("unknown streaming mode {mode_text:?}; expected replace, update, or merge"),
            ))
        }
    };

    cursor.expect_literal("chunks=", "load")?;

    let mut chunks = Vec::new();
    while !cursor.is_end() {
        chunks.push(cursor.read_length_prefixed("load chunk")?);
    }

    if chunks.is_empty() {
        return Err(LoweringError::invalid_payload(
            "load",
            "streaming load contains no chunks",
        ));
    }

    Ok(StreamingLoadPayload { mode, chunks })
}

#[derive(Clone, Copy, Debug)]
struct Cursor<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> Cursor<'a> {
    #[inline]
    const fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn remaining(self) -> &'a str {
        &self.input[self.offset..]
    }

    fn is_end(self) -> bool {
        self.offset == self.input.len()
    }

    fn expect_literal(&mut self, literal: &str, stage: &str) -> LoweringResult<()> {
        if !self.remaining().starts_with(literal) {
            return Err(LoweringError::invalid_payload(
                stage,
                format!(
                    "expected {literal:?} at byte {}, found {:?}",
                    self.offset,
                    preview(self.remaining()),
                ),
            ));
        }

        self.offset += literal.len();
        Ok(())
    }

    fn expect_byte(&mut self, expected: u8, context: &str) -> LoweringResult<()> {
        let Some(actual) = self.input.as_bytes().get(self.offset).copied() else {
            return Err(LoweringError::invalid_payload(
                context,
                format!("expected byte {:?} at end of payload", char::from(expected)),
            ));
        };

        if actual != expected {
            return Err(LoweringError::invalid_payload(
                context,
                format!(
                    "expected {:?} at byte {}, found {:?}",
                    char::from(expected),
                    self.offset,
                    char::from(actual),
                ),
            ));
        }

        self.offset += 1;
        Ok(())
    }

    fn read_length_prefixed(&mut self, context: &str) -> LoweringResult<&'a str> {
        let length_start = self.offset;

        while let Some(byte) = self.input.as_bytes().get(self.offset).copied() {
            if byte == b':' {
                break;
            }

            if !byte.is_ascii_digit() {
                return Err(LoweringError::invalid_payload(
                    context,
                    format!(
                        "expected decimal length at byte {}, found {:?}",
                        self.offset,
                        char::from(byte),
                    ),
                ));
            }

            self.offset += 1;
        }

        if self.offset == length_start {
            return Err(LoweringError::invalid_payload(
                context,
                format!("missing decimal length at byte {}", self.offset),
            ));
        }

        if self.input.as_bytes().get(self.offset) != Some(&b':') {
            return Err(LoweringError::invalid_payload(
                context,
                "unterminated decimal length",
            ));
        }

        let length = self.input[length_start..self.offset]
            .parse::<usize>()
            .map_err(|_| {
                LoweringError::invalid_payload(context, "length is not representable as usize")
            })?;

        self.offset += 1;
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| LoweringError::invalid_payload(context, "length overflow"))?;

        if end > self.input.len() {
            return Err(LoweringError::invalid_payload(
                context,
                format!(
                    "declared length {length} exceeds remaining payload size {}",
                    self.input.len().saturating_sub(self.offset),
                ),
            ));
        }

        if !self.input.is_char_boundary(end) {
            return Err(LoweringError::invalid_payload(
                context,
                "declared length ends inside a UTF-8 code point",
            ));
        }

        let value = &self.input[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn read_optional_length_prefixed(&mut self, context: &str) -> LoweringResult<Option<&'a str>> {
        if self.remaining().starts_with('-') {
            self.offset += 1;
            return Ok(None);
        }

        self.read_length_prefixed(context).map(Some)
    }

    fn read_until(&mut self, delimiter: char, context: &str) -> LoweringResult<&'a str> {
        let remaining = self.remaining();
        let Some(relative) = remaining.find(delimiter) else {
            return Err(LoweringError::invalid_payload(
                context,
                format!("missing delimiter {delimiter:?}"),
            ));
        };

        let start = self.offset;
        let end = start + relative;
        self.offset = end + delimiter.len_utf8();
        Ok(&self.input[start..end])
    }

    fn read_stage_list(&mut self, context: &str) -> LoweringResult<Vec<EncodedStage<'a>>> {
        self.expect_byte(b'[', context)?;

        let mut stages = Vec::new();

        while self.input.as_bytes().get(self.offset) != Some(&b']') {
            self.expect_byte(b'{', context)?;
            let name = self.read_length_prefixed(context)?;
            self.expect_byte(b',', context)?;
            let arguments = self.read_length_prefixed(context)?;
            self.expect_byte(b'}', context)?;

            stages.push(EncodedStage { name, arguments });

            if self.is_end() {
                return Err(LoweringError::invalid_payload(
                    context,
                    "unterminated pipeline stage list",
                ));
            }
        }

        self.expect_byte(b']', context)?;
        Ok(stages)
    }

    fn expect_end(self, context: &str) -> LoweringResult<()> {
        if self.is_end() {
            return Ok(());
        }

        Err(LoweringError::invalid_payload(
            context,
            format!(
                "unexpected trailing payload at byte {}: {:?}",
                self.offset,
                preview(self.remaining()),
            ),
        ))
    }
}

fn preview(text: &str) -> String {
    text.chars().take(24).collect()
}

fn parse_field_path(text: &str) -> Result<ExpressionFieldPath, String> {
    let text = text.trim();

    if text.is_empty() {
        return Err("field path must not be empty".to_owned());
    }

    let segments = text.split('.').map(str::trim).collect::<Vec<_>>();

    for (index, segment) in segments.iter().enumerate() {
        if segment.is_empty() {
            return Err(format!("field-path segment {index} must not be empty"));
        }

        let mut characters = segment.char_indices();

        let Some((_, first)) = characters.next() else {
            return Err(format!("field-path segment {index} must not be empty"));
        };

        if first != '_' && !first.is_alphabetic() {
            return Err(format!(
                "field-path segment {index} must start with an alphabetic character or '_'",
            ));
        }

        if let Some((byte_index, character)) = characters.find(|(_, character)| {
            *character != '_' && !character.is_alphabetic() && !character.is_ascii_digit()
        }) {
            return Err(format!(
                "field-path segment {index} contains invalid character {character:?} \
                 at byte index {byte_index}",
            ));
        }
    }

    ExpressionFieldPath::new(segments).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::query::{
        parse_expression, ExpressionFieldPath, LogicalSource, SetAssignment, SortDirection,
        SortKey, StageName,
    };

    #[inline]
    fn source() -> LogicalSource {
        LogicalSource::collection("users").unwrap()
    }

    #[inline]
    fn field(path: &[&str]) -> ExpressionFieldPath {
        ExpressionFieldPath::new(path.iter().copied()).unwrap()
    }

    fn assignment(path: &[&str], expression: &str) -> SetAssignment {
        SetAssignment::new(field(path), parse_expression(expression).unwrap())
    }

    fn lower(logical: &LogicalPlan) -> PhysicalPlan {
        ScanPlanLowerer::new()
            .lower_plan_detailed(logical, &PhysicalPlanner::new())
            .unwrap()
    }

    fn native_custom(stage: &str, arguments: &str) -> LogicalOperator {
        LogicalOperator::custom(StageName::parse(stage).unwrap(), arguments, false)
    }

    fn write_length_prefixed(output: &mut String, value: &str) {
        output.push_str(&value.len().to_string());
        output.push(':');
        output.push_str(value);
    }

    fn write_optional(output: &mut String, value: Option<&str>) {
        match value {
            Some(value) => write_length_prefixed(output, value),
            None => output.push('-'),
        }
    }

    fn write_stages(output: &mut String, stages: &[(&str, &str)]) {
        output.push('[');

        for (name, arguments) in stages {
            output.push('{');
            write_length_prefixed(output, name);
            output.push(',');
            write_length_prefixed(output, arguments);
            output.push('}');
        }

        output.push(']');
    }

    fn lookup_payload(
        collection: &str,
        alias: Option<&str>,
        into: &str,
        stages: &[(&str, &str)],
    ) -> String {
        let mut output = String::from("source=");
        write_length_prefixed(&mut output, collection);
        output.push_str(";alias=");
        write_optional(&mut output, alias);
        output.push_str(";into=");
        write_length_prefixed(&mut output, into);
        output.push_str(";pipeline=");
        write_stages(&mut output, stages);
        output
    }

    fn union_payload(collection: &str, alias: Option<&str>, stages: &[(&str, &str)]) -> String {
        let mut output = String::from("source=");
        write_length_prefixed(&mut output, collection);
        output.push_str(";alias=");
        write_optional(&mut output, alias);
        output.push_str(";pipeline=");
        write_stages(&mut output, stages);
        output
    }

    fn streaming_payload(mode: &str, chunks: &[&str]) -> String {
        let mut output = format!("streaming;mode={mode};chunks=");

        for chunk in chunks {
            write_length_prefixed(&mut output, chunk);
        }

        output
    }

    #[test]
    fn lowers_scan_only_plan() {
        let logical = LogicalPlan::scan(source());
        let physical = lower(&logical);

        assert!(physical.is_scan_only());
        assert_eq!(physical.source().collection().as_str(), "users");
        assert!(!physical.is_write());
    }

    #[test]
    fn lowers_complete_read_pipeline_in_order() {
        let logical = LogicalPlan::builder(source())
            .filter(parse_expression("active == true").unwrap())
            .unwrap()
            .sort([
                SortKey::new(field(&["age"]), SortDirection::Descending),
                SortKey::new(field(&["name"]), SortDirection::Ascending),
            ])
            .unwrap()
            .skip(10)
            .unwrap()
            .limit(20)
            .unwrap()
            .select([field(&["name"]), field(&["age"])])
            .unwrap()
            .distinct([field(&["name"])])
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);
        let names = physical
            .operators()
            .iter()
            .map(PhysicalOperator::name)
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            ["filter", "sort", "skip", "limit", "select", "distinct"],
        );
        assert!(!physical.is_write());
        assert!(physical.changes_cardinality());
    }

    #[test]
    fn preserves_filter_payload() {
        let logical = LogicalPlan::builder(source())
            .filter(parse_expression("age >= 18").unwrap())
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);

        assert_eq!(
            physical.operators()[0].predicate(),
            logical.operators().next().unwrap().predicate(),
        );
    }

    #[test]
    fn preserves_set_assignments_and_write_mode() {
        let logical = LogicalPlan::builder(source())
            .set([
                assignment(&["enabled"], "true"),
                assignment(&["profile", "level"], "level + 1"),
            ])
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);

        assert_eq!(physical.operators()[0].name(), "set");
        assert_eq!(physical.operators()[0].assignments().unwrap().len(), 2);
        assert!(physical.is_write());
    }

    #[test]
    fn preserves_compact_load_target() {
        let logical = LogicalPlan::builder(source())
            .load("profile")
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);

        assert_eq!(physical.operators()[0].load_target(), Some("profile"));
        assert!(physical.is_write());
    }

    #[test]
    fn lowers_streaming_load_payload() {
        let specification = streaming_payload("replace", &["batch1", "batch2"]);

        let logical = LogicalPlan::builder(source())
            .load(specification)
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);
        let operator = &physical.operators()[0];

        assert_eq!(operator.name(), "streaming-load");
        assert_eq!(
            operator.streaming_load_mode(),
            Some(PhysicalLoadMode::Replace)
        );
        assert_eq!(operator.streaming_load_chunks().unwrap().len(), 2);
        assert!(physical.is_write());
    }

    #[test]
    fn lowers_lookup_payload() {
        let payload = lookup_payload(
            "workspace",
            Some("w"),
            "public",
            &[("where", "w.public == true"), ("limit", "5")],
        );

        let logical = LogicalPlan::new(source(), [native_custom("lookup", &payload)]).unwrap();

        let physical = lower(&logical);
        let operator = &physical.operators()[0];

        assert_eq!(operator.name(), "lookup");
        assert_eq!(operator.lookup_collection().unwrap().as_str(), "workspace");
        assert_eq!(operator.lookup_alias(), Some("w"));
        assert_eq!(operator.lookup_target(), Some("public"));
        assert_eq!(operator.nested_pipeline().unwrap().len(), 2);
        assert!(!operator.execution_properties().writes());
    }

    #[test]
    fn lowers_union_payload() {
        let payload = union_payload(
            "archived_users",
            None,
            &[("where", "active == true"), ("select", "name, age")],
        );

        let logical = LogicalPlan::new(source(), [native_custom("union", &payload)]).unwrap();

        let physical = lower(&logical);
        let operator = &physical.operators()[0];

        assert_eq!(operator.name(), "union");
        assert_eq!(
            operator.union_collection().unwrap().as_str(),
            "archived_users"
        );
        assert_eq!(operator.union_alias(), None);
        assert_eq!(operator.nested_pipeline().unwrap().len(), 2);
        assert!(!matches!(
            operator.execution_properties().cardinality,
            crate::query::CardinalityEffect::Preserve
        ));
    }

    #[test]
    fn lowers_nested_lookup_inside_union() {
        let nested_lookup = lookup_payload(
            "workspace",
            Some("w"),
            "spaces",
            &[("where", "w.public == true")],
        );

        let union = union_payload("archived_users", Some("a"), &[("lookup", &nested_lookup)]);

        let logical = LogicalPlan::new(source(), [native_custom("union", &union)]).unwrap();

        let physical = lower(&logical);
        let nested = physical.operators()[0]
            .nested_pipeline()
            .unwrap()
            .operators();

        assert_eq!(nested.len(), 1);
        assert_eq!(nested[0].name(), "lookup");
        assert_eq!(nested[0].lookup_alias(), Some("w"));
    }

    #[test]
    fn malformed_lookup_payload_is_rejected() {
        let logical =
            LogicalPlan::new(source(), [native_custom("lookup", "source=9:workspace")]).unwrap();

        let error = ScanPlanLowerer::new()
            .lower_plan_detailed(&logical, &PhysicalPlanner::new())
            .unwrap_err();

        assert_eq!(error.operator_index(), Some(0));
        assert!(matches!(
            error.kind(),
            LoweringErrorKind::InvalidNativePayload { stage, .. }
                if stage.as_ref() == "lookup"
        ));
    }

    #[test]
    fn malformed_streaming_load_is_rejected() {
        let logical = LogicalPlan::builder(source())
            .load("streaming;mode=replace;chunks=")
            .unwrap()
            .finish()
            .unwrap();

        let error = ScanPlanLowerer::new()
            .lower_plan_detailed(&logical, &PhysicalPlanner::new())
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            LoweringErrorKind::InvalidNativePayload { stage, .. }
                if stage.as_ref() == "load"
        ));
    }

    #[test]
    fn preserves_limit_and_skip_counts() {
        let logical = LogicalPlan::builder(source())
            .skip(7)
            .unwrap()
            .limit(11)
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);

        assert_eq!(physical.operators()[0].row_count(), Some(7));
        assert_eq!(physical.operators()[1].row_count(), Some(11));
    }

    #[test]
    fn preserves_sort_keys() {
        let logical = LogicalPlan::builder(source())
            .sort([
                SortKey::descending(field(&["age"])),
                SortKey::ascending(field(&["name"])),
            ])
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);
        let keys = physical.operators()[0].sort_keys().unwrap();

        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].field(), &field(&["age"]));
        assert_eq!(keys[0].direction(), SortDirection::Descending);
        assert_eq!(keys[1].field(), &field(&["name"]));
        assert_eq!(keys[1].direction(), SortDirection::Ascending);
    }

    #[test]
    fn preserves_select_and_distinct_fields() {
        let logical = LogicalPlan::builder(source())
            .select([field(&["name"]), field(&["profile", "country"])])
            .unwrap()
            .distinct([field(&["name"])])
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);

        assert_eq!(
            physical.operators()[0].selected_fields().unwrap(),
            &[field(&["name"]), field(&["profile", "country"])],
        );
        assert_eq!(
            physical.operators()[1].distinct_fields().unwrap(),
            &[field(&["name"])],
        );
    }

    #[test]
    fn preserves_count_alias() {
        let logical = LogicalPlan::builder(source())
            .count("total")
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);

        assert_eq!(physical.operators()[0].count_alias(), Some("total"));
        assert!(!physical.is_write());
    }

    #[test]
    fn preserves_custom_operator_metadata() {
        let stage = StageName::parse("inspect").unwrap();

        let logical = LogicalPlan::builder(source())
            .custom(stage, "verbose", false)
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);

        assert_eq!(physical.operators()[0].name(), "inspect");
        assert!(!physical.is_write());
    }

    #[test]
    fn length_prefixed_decoder_supports_utf8() {
        let payload = lookup_payload(
            "workspace",
            Some("équipe"),
            "résultat",
            &[("where", "actif == true")],
        );

        let parsed = parse_lookup_payload(&payload).unwrap();

        assert_eq!(parsed.alias, Some("équipe"));
        assert_eq!(parsed.into, "résultat");
    }

    #[test]
    fn trailing_payload_is_rejected() {
        let mut payload = union_payload("archived_users", None, &[("where", "true")]);
        payload.push('x');

        assert!(parse_union_payload(&payload).is_err());
    }

    #[test]
    fn preserves_typed_insert_document() {
        let logical = LogicalPlan::builder(source())
            .insert(r#"{name:"Alice",active:true,tags:["rust"],profile:{level:2}}"#)
            .unwrap()
            .finish()
            .unwrap();

        let expected = logical
            .operators()
            .next()
            .and_then(LogicalOperator::insert_document)
            .expect("logical insert document")
            .clone();

        let physical = lower(&logical);

        assert_eq!(physical.operators()[0].insert_document(), Some(&expected),);
        assert!(physical.is_write());
    }

    #[test]
    fn preserves_typed_pivot_specification() {
        use crate::query::logical_plan::{PivotAggregate, PivotSpecification, PivotValue};

        let specification = PivotSpecification::new(
            [field(&["region"])],
            [field(&["month"])],
            [PivotValue::new(
                field(&["revenue"]),
                PivotAggregate::Sum,
                Option::<&str>::None,
            )
            .unwrap()],
        )
        .unwrap();

        let logical = LogicalPlan::builder(source())
            .pivot(specification.clone())
            .unwrap()
            .finish()
            .unwrap();

        let physical = lower(&logical);
        let operator = &physical.operators()[0];

        assert_eq!(operator.name(), "pivot");
        assert_eq!(operator.pivot_specification(), Some(&specification));
        assert!(!operator.execution_properties().writes());
        assert!(!matches!(
            operator.execution_properties().cardinality,
            crate::query::CardinalityEffect::Preserve
        ));
    }
}
