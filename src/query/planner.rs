//! Pipeline-to-logical-plan translation.

use std::{fmt, sync::Arc};

use super::logical_plan::{
    CollectionName, InsertDocument, LogicalOperator, LogicalPlan, LogicalPlanError, LogicalSource,
    PivotAggregate, PivotSpecification, PivotValue, SetAssignment, SortKey,
};
use super::syntax::{parse_sort_item, split_top_level, validate_balanced_text, ScanState};
use super::{parse_expression, ExpressionFieldPath, PipelineAst, Span, StageAst, StageName};

use crate::query::ast::Spanned;

/// Result returned by planning operations.
pub type PlanningResult<T> = std::result::Result<T, PlannerError>;

/// Normalized source accepted by the semantic planner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannerSource {
    collection: Arc<str>,
    alias: Option<Arc<str>>,
}

impl PlannerSource {
    /// Creates a normalized source without an alias.
    #[must_use]
    #[inline]
    pub fn new(collection: impl AsRef<str>) -> Self {
        Self {
            collection: Arc::from(collection.as_ref()),
            alias: None,
        }
    }

    /// Creates a normalized source with an alias.
    #[must_use]
    pub fn with_alias(collection: impl AsRef<str>, alias: impl AsRef<str>) -> Self {
        Self {
            collection: Arc::from(collection.as_ref()),
            alias: Some(Arc::from(alias.as_ref())),
        }
    }

    #[must_use]
    pub fn collection(&self) -> &str {
        &self.collection
    }

    #[must_use]
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    #[must_use]
    pub const fn has_alias(&self) -> bool {
        self.alias.is_some()
    }
}

/// Normalized pipeline accepted by the semantic planner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannerPipeline {
    source: PlannerSource,
    stages: Arc<[PlannerStage]>,
}

impl PlannerPipeline {
    /// Creates a normalized pipeline without a source alias.
    #[must_use]
    pub fn new<I>(source: impl AsRef<str>, stages: I) -> Self
    where
        I: IntoIterator<Item = PlannerStage>,
    {
        Self::with_source(PlannerSource::new(source), stages)
    }

    /// Creates a normalized pipeline from a complete source.
    #[must_use]
    pub fn with_source<I>(source: PlannerSource, stages: I) -> Self
    where
        I: IntoIterator<Item = PlannerStage>,
    {
        Self {
            source,
            stages: Arc::from(stages.into_iter().collect::<Vec<_>>()),
        }
    }

    /// Converts a parsed AST into the normalized planner representation.
    pub fn from_ast(source_text: &str, pipeline: &PipelineAst) -> PlanningResult<Self> {
        let collection = pipeline
            .source()
            .collection_name(source_text)
            .ok_or_else(|| {
                PlannerError::invalid_ast(
                    "source collection span does not belong to the supplied source text",
                    Some(pipeline.source().span()),
                )
            })?;

        let source = match pipeline.source().alias_name(source_text) {
            Some(alias) => PlannerSource::with_alias(collection, alias),
            None => PlannerSource::new(collection),
        };

        let mut stages = Vec::with_capacity(pipeline.stage_count());

        for index in 0..pipeline.stage_count() {
            let stage = pipeline.stage(index).ok_or_else(|| {
                PlannerError::invalid_ast(
                    format!("pipeline reports stage {index} but does not expose it"),
                    Some(pipeline.span()),
                )
            })?;

            stages.push(PlannerStage::from_ast(source_text, stage)?);
        }

        Ok(Self::with_source(source, stages))
    }

    #[must_use]
    pub const fn source_descriptor(&self) -> &PlannerSource {
        &self.source
    }

    /// Preserved compatibility accessor.
    #[must_use]
    #[inline]
    pub fn source(&self) -> &str {
        self.source.collection()
    }

    #[must_use]
    pub fn source_alias(&self) -> Option<&str> {
        self.source.alias()
    }

    #[must_use]
    pub fn stages(&self) -> &[PlannerStage] {
        &self.stages
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.stages.len()
    }
}

/// One normalized pipeline stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannerStage {
    name: StageName,
    arguments: Arc<str>,
    subpipeline: Option<PlannerSubPipeline>,
    span: Span,
}

impl PlannerStage {
    /// Creates a simple normalized stage.
    #[must_use]
    #[inline]
    pub fn new(name: StageName, arguments: impl AsRef<str>, span: Span) -> Self {
        Self {
            name,
            arguments: Arc::from(arguments.as_ref()),
            subpipeline: None,
            span,
        }
    }

    /// Creates a compound normalized stage.
    #[must_use]
    pub fn with_subpipeline(
        name: StageName,
        arguments: impl AsRef<str>,
        subpipeline: PlannerSubPipeline,
        span: Span,
    ) -> Self {
        Self {
            name,
            arguments: Arc::from(arguments.as_ref()),
            subpipeline: Some(subpipeline),
            span,
        }
    }

    /// Converts one parsed AST stage recursively.
    pub fn from_ast(source_text: &str, stage: &StageAst) -> PlanningResult<Self> {
        let name_text = stage.name_text(source_text).ok_or_else(|| {
            PlannerError::invalid_ast(
                "stage-name span does not belong to the supplied source text",
                Some(stage.span()),
            )
        })?;

        let name = StageName::parse(name_text).map_err(|error| {
            PlannerError::invalid_ast(
                format!("invalid AST stage name {name_text:?}: {error}"),
                Some(stage.span()),
            )
        })?;

        let arguments = stage.arguments_text(source_text).ok_or_else(|| {
            PlannerError::invalid_ast(
                format!("arguments span of stage {name_text:?} is invalid"),
                Some(stage.span()),
            )
        })?;

        match stage.subpipeline() {
            Some(subpipeline) => {
                let mut stages = Vec::with_capacity(subpipeline.stage_count());

                for index in 0..subpipeline.stage_count() {
                    let child = subpipeline.stage(index).ok_or_else(|| {
                        PlannerError::invalid_ast(
                            format!(
                                "sub-pipeline of {name_text:?} reports stage {index} \
                                 but does not expose it"
                            ),
                            Some(stage.span()),
                        )
                    })?;

                    stages.push(Self::from_ast(source_text, child)?);
                }

                Ok(Self::with_subpipeline(
                    name,
                    arguments,
                    PlannerSubPipeline::new(stages, subpipeline.span()),
                    stage.span(),
                ))
            }

            None => Ok(Self::new(name, arguments, stage.span())),
        }
    }

    #[must_use]
    #[inline]
    pub const fn name(&self) -> &StageName {
        &self.name
    }

    #[must_use]
    #[inline]
    pub fn arguments(&self) -> &str {
        &self.arguments
    }

    #[must_use]
    pub const fn subpipeline(&self) -> Option<&PlannerSubPipeline> {
        self.subpipeline.as_ref()
    }

    #[must_use]
    pub const fn is_compound(&self) -> bool {
        self.subpipeline.is_some()
    }

    #[must_use]
    #[inline]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// Normalized body of a compound stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannerSubPipeline {
    stages: Arc<[PlannerStage]>,
    span: Span,
}

impl PlannerSubPipeline {
    #[must_use]
    pub fn new<I>(stages: I, span: Span) -> Self
    where
        I: IntoIterator<Item = PlannerStage>,
    {
        Self {
            stages: Arc::from(stages.into_iter().collect::<Vec<_>>()),
            span,
        }
    }

    #[must_use]
    pub fn stages(&self) -> &[PlannerStage] {
        &self.stages
    }

    #[must_use]
    pub fn stage(&self, index: usize) -> Option<&PlannerStage> {
        self.stages.get(index)
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.stages.len()
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    #[must_use]
    #[inline]
    pub const fn span(&self) -> Span {
        self.span
    }
}

/// Semantic query planner.
#[derive(Clone, Debug, Default)]
pub struct Planner {
    options: PlannerOptions,
}

impl Planner {
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn with_options(options: PlannerOptions) -> Self {
        Self { options }
    }

    #[must_use]
    pub const fn options(&self) -> &PlannerOptions {
        &self.options
    }

    /// Converts a parsed pipeline AST directly into a logical plan.
    pub fn plan_ast(
        &self,
        source_text: &str,
        pipeline: &PipelineAst,
    ) -> PlanningResult<LogicalPlan> {
        let normalized = PlannerPipeline::from_ast(source_text, pipeline)?;
        self.plan(&normalized)
    }

    /// Converts a normalized pipeline into a logical plan.
    pub fn plan(&self, pipeline: &PlannerPipeline) -> PlanningResult<LogicalPlan> {
        self.validate_source_alias(pipeline.source_descriptor())?;

        let collection = CollectionName::parse(pipeline.source()).map_err(|error| {
            PlannerError::new(
                PlannerErrorKind::InvalidSource {
                    source: Arc::from(pipeline.source()),
                    message: Arc::from(error.to_string()),
                },
                None,
            )
        })?;

        let source = LogicalSource::new(collection);
        let mut builder = LogicalPlan::builder(source);

        for (index, stage) in pipeline.stages().iter().enumerate() {
            let operator = self.compile_stage(StageLocation::root(index), stage)?;

            builder.push(operator).map_err(|error| {
                self.logical_plan_error(StageLocation::root(index), stage, error)
            })?;
        }

        builder.finish().map_err(|error| {
            PlannerError::new(
                PlannerErrorKind::LogicalPlan {
                    location: None,
                    message: Arc::from(error.to_string()),
                },
                None,
            )
        })
    }

    fn compile_stage(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        match stage.name().as_str() {
            "where" => self.compile_where(location, stage),
            "set" => self.compile_set(location, stage),
            "derive" => self.compile_derive(location, stage),
            "lookup" | "join" => self.compile_lookup(location, stage),
            "union" => self.compile_union(location, stage),
            "load" => self.compile_load(location, stage),
            "limit" => self.compile_limit(location, stage),
            "skip" | "offset" => self.compile_skip(location, stage),
            "first" => self.compile_first(location, stage),
            "single" => self.compile_single(location, stage),
            "unwind" => self.compile_unwind(location, stage),
            "sort" => self.compile_sort(location, stage),
            "select" => self.compile_select(location, stage),
            "rename" => self.compile_rename(location, stage),
            "drop" => self.compile_drop(location, stage),
            "distinct" => self.compile_distinct(location, stage),
            "count" => self.compile_count(location, stage),
            "delete" => self.compile_delete(location, stage),
            "insert" => self.compile_insert(location, stage),
            "group" => self.compile_group(location, stage),
            "pivot" => self.compile_pivot(location, stage),

            _ if stage.is_compound() => Err(PlannerError::new(
                PlannerErrorKind::CompoundCustomStage {
                    location,
                    name: Arc::from(stage.name().as_str()),
                },
                Some(stage.span()),
            )),

            _ if self.options.allow_custom_stages => Ok(LogicalOperator::custom(
                stage.name().clone(),
                stage.arguments(),
                false,
            )),

            _ => Err(PlannerError::new(
                PlannerErrorKind::UnknownStage {
                    location,
                    name: Arc::from(stage.name().as_str()),
                },
                Some(stage.span()),
            )),
        }
    }

    fn compile_where(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let arguments = self.required_arguments(location, stage)?;
        let predicate = self.parse_stage_expression(location, stage, arguments)?;
        Ok(LogicalOperator::filter(predicate))
    }

    fn compile_derive(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let arguments = self.required_arguments(location, stage)?;
        let name =
            StageName::parse("derive").expect("native stage name 'derive' must always be valid");
        Ok(LogicalOperator::custom(name, arguments, false))
    }

    fn compile_set(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let arguments = self.required_arguments(location, stage)?;

        let assignments = split_top_level(arguments, ',')
            .map_err(|message| self.invalid_syntax(location, stage, message))?;

        let mut compiled = Vec::with_capacity(assignments.len());

        for (assignment_index, assignment) in assignments.into_iter().enumerate() {
            let (field, expression) = split_assignment(assignment).map_err(|message| {
                PlannerError::new(
                    PlannerErrorKind::InvalidSetAssignment {
                        location,
                        assignment_index,
                        assignment: Arc::from(assignment.trim()),
                        message: Arc::from(message),
                    },
                    Some(stage.span()),
                )
            })?;

            let field = parse_field_path(field).map_err(|message| {
                PlannerError::new(
                    PlannerErrorKind::InvalidSetField {
                        location,
                        assignment_index,
                        field: Arc::from(field.trim()),
                        message: Arc::from(message),
                    },
                    Some(stage.span()),
                )
            })?;

            let value = self.parse_stage_expression(location, stage, expression.trim())?;
            compiled.push(SetAssignment::new(field, value));
        }

        LogicalOperator::set(compiled)
            .map_err(|error| self.logical_plan_error(location, stage, error))
    }

    fn compile_lookup(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        let arguments = self.required_arguments(location, stage)?;
        let subpipeline = self.required_subpipeline(location, stage)?;

        let header = parse_lookup_header(arguments)
            .map_err(|message| self.invalid_syntax(location, stage, message))?;

        let compiled = self.compile_lookup_body(location, stage, subpipeline)?;

        let mut payload = String::new();
        payload.push_str("source=");
        write_length_prefixed(&mut payload, header.collection);
        payload.push_str(";alias=");
        write_optional_length_prefixed(&mut payload, header.alias);
        payload.push_str(";into=");
        write_length_prefixed(&mut payload, compiled.into);
        payload.push_str(";pipeline=");
        write_compiled_stages(&mut payload, &compiled.stages);

        let name =
            StageName::parse("lookup").expect("native stage name 'lookup' must always be valid");

        Ok(LogicalOperator::custom(name, payload, false))
    }

    fn compile_union(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_no_arguments(location, stage)?;
        let subpipeline = self.required_subpipeline(location, stage)?;

        let compiled = self.compile_union_body(location, stage, subpipeline)?;

        let mut payload = String::new();
        payload.push_str("source=");
        write_length_prefixed(&mut payload, compiled.source);
        payload.push_str(";alias=");
        write_optional_length_prefixed(&mut payload, compiled.alias);
        payload.push_str(";pipeline=");
        write_compiled_stages(&mut payload, &compiled.stages);

        let name =
            StageName::parse("union").expect("native stage name 'union' must always be valid");

        Ok(LogicalOperator::custom(name, payload, false))
    }

    fn compile_load(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        match stage.subpipeline() {
            None => {
                let arguments = self.required_arguments(location, stage)?;

                LogicalOperator::load(arguments)
                    .map_err(|error| self.logical_plan_error(location, stage, error))
            }

            Some(subpipeline) => {
                self.require_no_arguments(location, stage)?;
                let specification =
                    self.compile_streaming_load_body(location, stage, subpipeline)?;

                LogicalOperator::load(specification)
                    .map_err(|error| self.logical_plan_error(location, stage, error))
            }
        }
    }

    fn compile_limit(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let count = self.parse_non_negative_integer(location, stage)?;
        Ok(LogicalOperator::limit(count))
    }

    fn compile_skip(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let count = self.parse_non_negative_integer(location, stage)?;
        Ok(LogicalOperator::skip(count))
    }

    fn compile_first(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let arguments = stage.arguments().trim();
        if !arguments.is_empty() {
            parse_field_path(arguments)
                .map_err(|message| self.invalid_syntax(location, stage, message))?;
        }
        Ok(LogicalOperator::custom(
            StageName::parse("first").unwrap(),
            arguments,
            false,
        ))
    }

    fn compile_single(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let arguments = stage.arguments().trim();
        if !arguments.is_empty() {
            parse_field_path(arguments)
                .map_err(|message| self.invalid_syntax(location, stage, message))?;
        }
        Ok(LogicalOperator::custom(
            StageName::parse("single").unwrap(),
            arguments,
            false,
        ))
    }

    fn compile_unwind(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let arguments = self.required_arguments(location, stage)?;
        parse_field_path(arguments)
            .map_err(|message| self.invalid_syntax(location, stage, message))?;
        Ok(LogicalOperator::custom(
            StageName::parse("unwind").unwrap(),
            arguments,
            false,
        ))
    }

    fn compile_sort(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let arguments = self.required_arguments(location, stage)?;

        let items = split_top_level(arguments, ',')
            .map_err(|message| self.invalid_syntax(location, stage, message))?;

        let mut keys = Vec::with_capacity(items.len());

        for (item_index, item) in items.into_iter().enumerate() {
            let (field_text, direction) = parse_sort_item(item).map_err(|message| {
                self.invalid_list_item(location, stage, item_index, item, message)
            })?;

            let field = parse_field_path(field_text).map_err(|message| {
                self.invalid_list_item(location, stage, item_index, item, message)
            })?;

            keys.push(SortKey::new(field, direction));
        }

        LogicalOperator::sort(keys).map_err(|error| self.logical_plan_error(location, stage, error))
    }

    fn compile_select(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let arguments = self.required_arguments(location, stage)?;
        let items = split_top_level(arguments, ',')
            .map_err(|message| self.invalid_syntax(location, stage, message))?;

        let mut normalized = Vec::with_capacity(items.len());
        let mut plain_fields = Vec::with_capacity(items.len());
        let mut requires_custom_projection = false;

        for (item_index, item) in items.into_iter().enumerate() {
            let item = item.trim();
            if item.is_empty() {
                return Err(self.invalid_list_item(
                    location,
                    stage,
                    item_index,
                    item,
                    select_projection_syntax(),
                ));
            }

            match item.rsplit_once(" as ") {
                Some((source_text, target_text)) => {
                    let source_text = source_text.trim();
                    let target_text = target_text.trim();
                    if source_text.is_empty() || target_text.is_empty() {
                        return Err(self.invalid_list_item(
                            location,
                            stage,
                            item_index,
                            item,
                            select_projection_syntax(),
                        ));
                    }

                    parse_expression(source_text).map_err(|error| {
                        self.invalid_list_item(
                            location,
                            stage,
                            item_index,
                            item,
                            format!("invalid projection expression {source_text:?}: {error}"),
                        )
                    })?;
                    let target = parse_field_path(target_text).map_err(|message| {
                        self.invalid_list_item(location, stage, item_index, item, message)
                    })?;

                    normalized.push(format!("{} as {}", source_text, target));
                    requires_custom_projection = true;
                }
                None => {
                    let field = parse_field_path(item).map_err(|_| {
                        self.invalid_list_item(
                            location,
                            stage,
                            item_index,
                            item,
                            "an expression projection requires an alias, for example `price * quantity as total`"
                                .to_owned(),
                        )
                    })?;
                    normalized.push(field.to_string());
                    plain_fields.push(field);
                }
            }
        }

        if requires_custom_projection {
            return Ok(LogicalOperator::custom(
                StageName::parse("select").expect("native stage name is valid"),
                normalized.join(", "),
                false,
            ));
        }

        LogicalOperator::select(plain_fields)
            .map_err(|error| self.logical_plan_error(location, stage, error))
    }

    fn compile_rename(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let arguments = self.required_arguments(location, stage)?;
        let (source, target) = parse_rename_arguments(arguments)
            .map_err(|message| self.invalid_syntax(location, stage, message))?;
        let source = parse_field_path(source)
            .map_err(|message| self.invalid_syntax(location, stage, message))?;
        let target = parse_field_path(target)
            .map_err(|message| self.invalid_syntax(location, stage, message))?;

        if source == target {
            return Err(self.invalid_syntax(
                location,
                stage,
                "rename source and target must be different".to_owned(),
            ));
        }

        Ok(LogicalOperator::custom(
            StageName::parse("rename").expect("native stage name is valid"),
            format!("{} as {}", source, target),
            false,
        ))
    }

    fn compile_drop(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let fields = self.parse_required_field_list(location, stage)?;
        let arguments = fields
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");

        Ok(LogicalOperator::custom(
            StageName::parse("drop").expect("native stage name is valid"),
            arguments,
            false,
        ))
    }

    fn compile_distinct(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;

        let arguments = stage.arguments().trim();
        let fields = if arguments.is_empty() {
            Vec::new()
        } else {
            self.parse_field_list(location, stage, arguments)?
        };

        LogicalOperator::distinct(fields)
            .map_err(|error| self.logical_plan_error(location, stage, error))
    }

    fn compile_count(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;

        let arguments = stage.arguments().trim();
        let alias = if arguments.is_empty() {
            "count"
        } else {
            parse_count_alias(arguments)
                .map_err(|message| self.invalid_syntax(location, stage, message))?
        };

        LogicalOperator::count(alias)
            .map_err(|error| self.logical_plan_error(location, stage, error))
    }

    fn compile_delete(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        self.require_no_arguments(location, stage)?;
        Ok(LogicalOperator::delete())
    }

    fn compile_insert(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_simple(location, stage)?;
        let arguments = self.required_arguments(location, stage)?;

        let document = InsertDocument::parse(arguments)
            .map_err(|error| self.logical_plan_error(location, stage, error))?;

        LogicalOperator::from_insert_document(document)
            .map_err(|error| self.logical_plan_error(location, stage, error))
    }

    fn compile_group(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        let mut fields = Vec::new();

        match stage.subpipeline() {
            None => {
                let arguments = self.required_arguments(location, stage)?;
                let items = split_top_level(arguments, ',')
                    .map_err(|message| self.invalid_syntax(location, stage, message))?;

                if items.is_empty() {
                    return Err(self.invalid_syntax(
                        location,
                        stage,
                        "group requires at least one grouping field",
                    ));
                }

                // Compact group syntax follows the relational shape people read from
                // left to right: every item except the last is a grouping dimension,
                // and the last item is the value to sum. A single item remains a pure
                // grouping key (with the implicit `count` produced by the runtime).
                //
                // This makes `group x, y, z` mean "group by x and y, sum z" instead
                // of silently treating y as another measure. Multiple measures remain
                // available through the explicit `group | by ... | sum ...` form.
                let key_count = if items.len() == 1 { 1 } else { items.len() - 1 };

                for (index, item) in items[..key_count].iter().enumerate() {
                    let (source, alias) = parse_group_key(item).map_err(|message| {
                        self.invalid_list_item(location, stage, index, item, message)
                    })?;
                    fields.push(match alias {
                        Some(alias) => group_key_marker(source, alias).map_err(|message| {
                            self.invalid_list_item(location, stage, index, item, message)
                        })?,
                        None => parse_field_path(source).map_err(|message| {
                            self.invalid_list_item(location, stage, index, item, message)
                        })?,
                    });
                }

                if items.len() > 1 {
                    let index = items.len() - 1;
                    let item = items[index];
                    let (source, alias) = parse_group_measure(item).map_err(|message| {
                        self.invalid_list_item(location, stage, index, item, message)
                    })?;
                    fields.push(group_sum_marker(source, alias).map_err(|message| {
                        self.invalid_list_item(location, stage, index, item, message)
                    })?);
                }
            }

            Some(subpipeline) => {
                self.require_no_arguments(location, stage)?;
                let mut by_seen = false;

                for (index, child) in subpipeline.stages().iter().enumerate() {
                    let child_location = location.child(index);
                    self.require_simple(child_location, child)?;

                    match child.name().as_str() {
                        "by" => {
                            if by_seen {
                                return Err(self.duplicate_directive(child_location, stage, child));
                            }
                            by_seen = true;
                            let arguments = self.required_arguments(child_location, child)?;
                            let items = split_top_level(arguments, ',').map_err(|message| {
                                self.invalid_syntax(child_location, child, message)
                            })?;
                            for (item_index, item) in items.into_iter().enumerate() {
                                let (source, alias) = parse_group_key(item).map_err(|message| {
                                    self.invalid_list_item(
                                        child_location,
                                        child,
                                        item_index,
                                        item,
                                        message,
                                    )
                                })?;
                                fields.push(match alias {
                                    Some(alias) => {
                                        group_key_marker(source, alias).map_err(|message| {
                                            self.invalid_list_item(
                                                child_location,
                                                child,
                                                item_index,
                                                item,
                                                message,
                                            )
                                        })?
                                    }
                                    None => parse_field_path(source).map_err(|message| {
                                        self.invalid_list_item(
                                            child_location,
                                            child,
                                            item_index,
                                            item,
                                            message,
                                        )
                                    })?,
                                });
                            }
                        }
                        "sum" => {
                            let arguments = self.required_arguments(child_location, child)?;
                            let (source, alias) =
                                parse_group_measure(arguments).map_err(|message| {
                                    self.invalid_syntax(child_location, child, message)
                                })?;
                            fields.push(group_sum_marker(source, alias).map_err(|message| {
                                self.invalid_syntax(child_location, child, message)
                            })?);
                        }
                        _ => {
                            return Err(self.invalid_child_stage(
                                child_location,
                                stage,
                                child,
                                "group accepts one `by <fields>` directive and zero or more `sum <field> [as <alias>]` directives",
                            ));
                        }
                    }
                }

                if !by_seen {
                    return Err(self.missing_directive(location, stage, "by"));
                }
            }
        }

        LogicalOperator::group(fields)
            .map_err(|error| self.logical_plan_error(location, stage, error))
    }

    fn compile_pivot(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<LogicalOperator> {
        self.require_no_arguments(location, stage)?;
        let subpipeline = self.required_subpipeline(location, stage)?;
        let specification = self.compile_pivot_body(location, stage, subpipeline)?;

        LogicalOperator::pivot(specification)
            .map_err(|error| self.logical_plan_error(location, stage, error))
    }

    fn compile_pivot_body(
        &self,
        parent_location: StageLocation,
        parent_stage: &PlannerStage,
        subpipeline: &PlannerSubPipeline,
    ) -> PlanningResult<PivotSpecification> {
        let mut rows = None;
        let mut columns = None;
        let mut value_fields = None;
        let mut aggregate = None;

        for (index, child) in subpipeline.stages().iter().enumerate() {
            let location = parent_location.child(index);
            self.require_simple(location, child)?;

            match child.name().as_str() {
                "rows" => {
                    set_once(
                        &mut rows,
                        self.parse_required_field_list(location, child)?,
                        || self.duplicate_directive(location, parent_stage, child),
                    )?;
                }
                "columns" => {
                    set_once(
                        &mut columns,
                        self.parse_required_field_list(location, child)?,
                        || self.duplicate_directive(location, parent_stage, child),
                    )?;
                }
                "values" => {
                    set_once(
                        &mut value_fields,
                        self.parse_required_field_list(location, child)?,
                        || self.duplicate_directive(location, parent_stage, child),
                    )?;
                }
                "aggregate" => {
                    let arguments = self.required_arguments(location, child)?;
                    let parsed = parse_pivot_aggregate(arguments)
                        .map_err(|message| self.invalid_syntax(location, child, message))?;

                    set_once(&mut aggregate, parsed, || {
                        self.duplicate_directive(location, parent_stage, child)
                    })?;
                }
                _ => {
                    return Err(self.invalid_child_stage(
                        location,
                        parent_stage,
                        child,
                        "pivot accepts exactly one `rows`, `columns`, `values`, and `aggregate` directive",
                    ));
                }
            }
        }

        let rows =
            rows.ok_or_else(|| self.missing_directive(parent_location, parent_stage, "rows"))?;
        let columns = columns
            .ok_or_else(|| self.missing_directive(parent_location, parent_stage, "columns"))?;
        let value_fields = value_fields
            .ok_or_else(|| self.missing_directive(parent_location, parent_stage, "values"))?;
        let aggregate = aggregate
            .ok_or_else(|| self.missing_directive(parent_location, parent_stage, "aggregate"))?;

        let values = value_fields
            .into_iter()
            .map(|field| {
                PivotValue::new(field, aggregate, None::<&str>)
                    .map_err(|error| self.logical_plan_error(parent_location, parent_stage, error))
            })
            .collect::<PlanningResult<Vec<_>>>()?;

        PivotSpecification::new(rows, columns, values)
            .map_err(|error| self.logical_plan_error(parent_location, parent_stage, error))
    }

    fn compile_lookup_body<'a>(
        &self,
        parent_location: StageLocation,
        parent_stage: &PlannerStage,
        subpipeline: &'a PlannerSubPipeline,
    ) -> PlanningResult<CompiledLookup<'a>> {
        let mut into = None;
        let mut compiled = Vec::new();

        for (index, child) in subpipeline.stages().iter().enumerate() {
            let location = parent_location.child(index);

            match child.name().as_str() {
                "into" => {
                    self.require_simple(location, child)?;
                    let arguments = self.required_arguments(location, child)?;

                    if into.is_some() {
                        return Err(PlannerError::new(
                            PlannerErrorKind::DuplicateCompoundDirective {
                                location,
                                parent: Arc::from(parent_stage.name().as_str()),
                                directive: Arc::from("into"),
                            },
                            Some(child.span()),
                        ));
                    }

                    let name = parse_single_identifier(arguments)
                        .map_err(|message| self.invalid_syntax(location, child, message))?;

                    into = Some(name);
                }

                "on" | "from" | "with" | "chunk" => {
                    return Err(self.invalid_child_stage(
                        location,
                        parent_stage,
                        child,
                        "lookup accepts query stages and exactly one `into <name>` directive",
                    ));
                }

                _ => {
                    compiled.push(self.compile_nested_read_stage(location, parent_stage, child)?);
                }
            }
        }

        let into = into.ok_or_else(|| {
            PlannerError::new(
                PlannerErrorKind::MissingCompoundDirective {
                    location: parent_location,
                    parent: Arc::from(parent_stage.name().as_str()),
                    directive: Arc::from("into"),
                },
                Some(parent_stage.span()),
            )
        })?;

        Ok(CompiledLookup {
            into,
            stages: compiled,
        })
    }

    fn compile_union_body<'a>(
        &self,
        parent_location: StageLocation,
        parent_stage: &PlannerStage,
        subpipeline: &'a PlannerSubPipeline,
    ) -> PlanningResult<CompiledUnion<'a>> {
        let Some(source_stage) = subpipeline.stage(0) else {
            return Err(PlannerError::new(
                PlannerErrorKind::MissingUnionSource {
                    location: parent_location,
                },
                Some(parent_stage.span()),
            ));
        };

        let source_location = parent_location.child(0);

        if !matches!(source_stage.name().as_str(), "on" | "from") {
            return Err(PlannerError::new(
                PlannerErrorKind::InvalidUnionSource {
                    location: source_location,
                    found: Arc::from(source_stage.name().as_str()),
                },
                Some(source_stage.span()),
            ));
        }

        self.require_simple(source_location, source_stage)?;

        let source_arguments = self.required_arguments(source_location, source_stage)?;
        let source = parse_source_directive(source_arguments)
            .map_err(|message| self.invalid_syntax(source_location, source_stage, message))?;

        let mut compiled = Vec::new();

        for (index, child) in subpipeline.stages().iter().enumerate().skip(1) {
            let location = parent_location.child(index);

            if matches!(
                child.name().as_str(),
                "on" | "from" | "into" | "with" | "chunk"
            ) {
                return Err(self.invalid_child_stage(
                    location,
                    parent_stage,
                    child,
                    "union accepts one leading source directive followed by query stages",
                ));
            }

            compiled.push(self.compile_nested_read_stage(location, parent_stage, child)?);
        }

        Ok(CompiledUnion {
            source: source.collection,
            alias: source.alias,
            stages: compiled,
        })
    }

    fn compile_streaming_load_body(
        &self,
        parent_location: StageLocation,
        parent_stage: &PlannerStage,
        subpipeline: &PlannerSubPipeline,
    ) -> PlanningResult<String> {
        let mut mode = None;
        let mut chunks = Vec::new();

        for (index, child) in subpipeline.stages().iter().enumerate() {
            let location = parent_location.child(index);

            match child.name().as_str() {
                "with" => {
                    self.require_simple(location, child)?;
                    let arguments = self.required_arguments(location, child)?;

                    if mode.is_some() {
                        return Err(PlannerError::new(
                            PlannerErrorKind::DuplicateCompoundDirective {
                                location,
                                parent: Arc::from(parent_stage.name().as_str()),
                                directive: Arc::from("with"),
                            },
                            Some(child.span()),
                        ));
                    }

                    mode = Some(
                        parse_load_mode(arguments)
                            .map_err(|message| self.invalid_syntax(location, child, message))?,
                    );
                }

                "chunk" => {
                    self.require_simple(location, child)?;
                    let arguments = self.required_arguments(location, child)?;

                    validate_balanced_text(arguments)
                        .map_err(|message| self.invalid_syntax(location, child, message))?;

                    chunks.push(arguments);
                }

                _ => {
                    return Err(self.invalid_child_stage(
                        location,
                        parent_stage,
                        child,
                        "streaming load accepts only `with <mode>` and `chunk <data>`",
                    ));
                }
            }
        }

        let mode = mode.ok_or_else(|| {
            PlannerError::new(
                PlannerErrorKind::MissingCompoundDirective {
                    location: parent_location,
                    parent: Arc::from(parent_stage.name().as_str()),
                    directive: Arc::from("with"),
                },
                Some(parent_stage.span()),
            )
        })?;

        if chunks.is_empty() {
            return Err(PlannerError::new(
                PlannerErrorKind::MissingLoadChunk {
                    location: parent_location,
                },
                Some(parent_stage.span()),
            ));
        }

        let mut output = String::new();
        output.push_str("streaming;mode=");
        output.push_str(mode);
        output.push_str(";chunks=");

        for chunk in chunks {
            write_length_prefixed(&mut output, chunk);
        }

        Ok(output)
    }

    fn compile_nested_read_stage(
        &self,
        location: StageLocation,
        parent_stage: &PlannerStage,
        stage: &PlannerStage,
    ) -> PlanningResult<CompiledStage> {
        let operator = self.compile_stage(location, stage)?;

        if operator.is_mutating() || operator.is_terminal() {
            return Err(PlannerError::new(
                PlannerErrorKind::InvalidCompoundChild {
                    location,
                    parent: Arc::from(parent_stage.name().as_str()),
                    child: Arc::from(stage.name().as_str()),
                    message: Arc::from(
                        "nested lookup/union pipelines must contain read-only non-terminal stages",
                    ),
                },
                Some(stage.span()),
            ));
        }

        Ok(CompiledStage::from_operator(stage, operator))
    }

    fn validate_source_alias(&self, source: &PlannerSource) -> PlanningResult<()> {
        let Some(alias) = source.alias() else {
            return Ok(());
        };

        parse_single_identifier(alias).map_err(|message| {
            PlannerError::new(
                PlannerErrorKind::InvalidSourceAlias {
                    alias: Arc::from(alias),
                    message: Arc::from(message),
                },
                None,
            )
        })?;

        Ok(())
    }

    fn require_simple(&self, location: StageLocation, stage: &PlannerStage) -> PlanningResult<()> {
        if !stage.is_compound() {
            return Ok(());
        }

        Err(PlannerError::new(
            PlannerErrorKind::UnexpectedSubPipeline {
                location,
                stage: Arc::from(stage.name().as_str()),
            },
            Some(stage.span()),
        ))
    }

    fn required_subpipeline<'a>(
        &self,
        location: StageLocation,
        stage: &'a PlannerStage,
    ) -> PlanningResult<&'a PlannerSubPipeline> {
        stage.subpipeline().ok_or_else(|| {
            PlannerError::new(
                PlannerErrorKind::MissingSubPipeline {
                    location,
                    stage: Arc::from(stage.name().as_str()),
                },
                Some(stage.span()),
            )
        })
    }

    fn required_arguments<'a>(
        &self,
        location: StageLocation,
        stage: &'a PlannerStage,
    ) -> PlanningResult<&'a str> {
        let arguments = stage.arguments().trim();

        if arguments.is_empty() {
            return Err(PlannerError::new(
                PlannerErrorKind::MissingStageArguments {
                    location,
                    stage: Arc::from(stage.name().as_str()),
                },
                Some(stage.span()),
            ));
        }

        Ok(arguments)
    }

    fn require_no_arguments(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<()> {
        let arguments = stage.arguments().trim();

        if arguments.is_empty() {
            return Ok(());
        }

        Err(PlannerError::new(
            PlannerErrorKind::UnexpectedStageArguments {
                location,
                stage: Arc::from(stage.name().as_str()),
                arguments: Arc::from(arguments),
            },
            Some(stage.span()),
        ))
    }

    fn parse_non_negative_integer(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<usize> {
        let arguments = self.required_arguments(location, stage)?;

        if arguments.split_whitespace().count() != 1 {
            return Err(self.invalid_syntax(
                location,
                stage,
                "expected exactly one non-negative integer",
            ));
        }

        arguments.parse::<usize>().map_err(|_| {
            self.invalid_syntax(
                location,
                stage,
                "expected a non-negative integer representable as usize",
            )
        })
    }

    fn parse_required_field_list(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
    ) -> PlanningResult<Vec<ExpressionFieldPath>> {
        let arguments = self.required_arguments(location, stage)?;
        self.parse_field_list(location, stage, arguments)
    }

    fn parse_field_list(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
        arguments: &str,
    ) -> PlanningResult<Vec<ExpressionFieldPath>> {
        let items = split_top_level(arguments, ',')
            .map_err(|message| self.invalid_syntax(location, stage, message))?;

        let mut fields = Vec::with_capacity(items.len());

        for (item_index, item) in items.into_iter().enumerate() {
            let field = parse_field_path(item).map_err(|message| {
                self.invalid_list_item(location, stage, item_index, item, message)
            })?;

            fields.push(field);
        }

        Ok(fields)
    }

    fn parse_stage_expression(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
        expression: &str,
    ) -> PlanningResult<super::Expression> {
        parse_expression(expression).map_err(|error| {
            PlannerError::new(
                PlannerErrorKind::InvalidExpression {
                    location,
                    stage: Arc::from(stage.name().as_str()),
                    expression: Arc::from(expression),
                    message: Arc::from(error.to_string()),
                },
                Some(stage.span()),
            )
        })
    }

    fn invalid_syntax(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
        message: impl Into<Arc<str>>,
    ) -> PlannerError {
        PlannerError::new(
            PlannerErrorKind::InvalidStageSyntax {
                location,
                stage: Arc::from(stage.name().as_str()),
                message: message.into(),
            },
            Some(stage.span()),
        )
    }

    fn invalid_list_item(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
        item_index: usize,
        item: &str,
        message: impl Into<Arc<str>>,
    ) -> PlannerError {
        PlannerError::new(
            PlannerErrorKind::InvalidStageItem {
                location,
                stage: Arc::from(stage.name().as_str()),
                item_index,
                item: Arc::from(item.trim()),
                message: message.into(),
            },
            Some(stage.span()),
        )
    }

    fn invalid_child_stage(
        &self,
        location: StageLocation,
        parent: &PlannerStage,
        child: &PlannerStage,
        message: impl Into<Arc<str>>,
    ) -> PlannerError {
        PlannerError::new(
            PlannerErrorKind::InvalidCompoundChild {
                location,
                parent: Arc::from(parent.name().as_str()),
                child: Arc::from(child.name().as_str()),
                message: message.into(),
            },
            Some(child.span()),
        )
    }

    fn duplicate_directive(
        &self,
        location: StageLocation,
        parent: &PlannerStage,
        directive: &PlannerStage,
    ) -> PlannerError {
        PlannerError::new(
            PlannerErrorKind::DuplicateCompoundDirective {
                location,
                parent: Arc::from(parent.name().as_str()),
                directive: Arc::from(directive.name().as_str()),
            },
            Some(directive.span()),
        )
    }

    fn missing_directive(
        &self,
        location: StageLocation,
        parent: &PlannerStage,
        directive: &'static str,
    ) -> PlannerError {
        PlannerError::new(
            PlannerErrorKind::MissingCompoundDirective {
                location,
                parent: Arc::from(parent.name().as_str()),
                directive: Arc::from(directive),
            },
            Some(parent.span()),
        )
    }

    fn logical_plan_error(
        &self,
        location: StageLocation,
        stage: &PlannerStage,
        error: LogicalPlanError,
    ) -> PlannerError {
        PlannerError::new(
            PlannerErrorKind::LogicalPlan {
                location: Some(location),
                message: Arc::from(error.to_string()),
            },
            Some(stage.span()),
        )
    }
}

/// Planner behavior options.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlannerOptions {
    /// Preserve unknown simple stages as read-only custom logical operators.
    pub allow_custom_stages: bool,
}

impl PlannerOptions {
    #[must_use]
    #[inline]
    pub const fn new(allow_custom_stages: bool) -> Self {
        Self {
            allow_custom_stages,
        }
    }

    #[must_use]
    pub const fn with_custom_stages(mut self, allow: bool) -> Self {
        self.allow_custom_stages = allow;
        self
    }
}

impl Default for PlannerOptions {
    fn default() -> Self {
        Self::new(false)
    }
}

/// Recursive stage location used by diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StageLocation {
    path: [usize; 8],
    depth: u8,
}

impl StageLocation {
    #[must_use]
    pub const fn root(index: usize) -> Self {
        let mut path = [0; 8];
        path[0] = index;

        Self { path, depth: 1 }
    }

    #[must_use]
    pub fn child(self, index: usize) -> Self {
        let mut next = self;

        if usize::from(next.depth) < next.path.len() {
            next.path[usize::from(next.depth)] = index;
            next.depth += 1;
        }

        next
    }

    #[must_use]
    pub const fn depth(self) -> usize {
        self.depth as usize
    }

    #[must_use]
    pub fn index(self, depth: usize) -> Option<usize> {
        if depth < self.depth() {
            Some(self.path[depth])
        } else {
            None
        }
    }
}

impl fmt::Display for StageLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for depth in 0..self.depth() {
            if depth > 0 {
                formatter.write_str(".")?;
            }

            write!(formatter, "{}", self.path[depth])?;
        }

        Ok(())
    }
}

/// Planner diagnostic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannerError {
    kind: PlannerErrorKind,
    span: Option<Span>,
}

impl PlannerError {
    #[must_use]
    #[inline]
    pub const fn new(kind: PlannerErrorKind, span: Option<Span>) -> Self {
        Self { kind, span }
    }

    fn invalid_ast(message: impl Into<Arc<str>>, span: Option<Span>) -> Self {
        Self::new(
            PlannerErrorKind::InvalidAst {
                message: message.into(),
            },
            span,
        )
    }

    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &PlannerErrorKind {
        &self.kind
    }

    #[must_use]
    #[inline]
    pub const fn span(&self) -> Option<Span> {
        self.span
    }
}

impl fmt::Display for PlannerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            PlannerErrorKind::InvalidAst { message } => {
                write!(formatter, "invalid pipeline AST: {message}")
            }

            PlannerErrorKind::InvalidSource { source, message } => {
                write!(formatter, "invalid source collection {source:?}: {message}")
            }

            PlannerErrorKind::InvalidSourceAlias { alias, message } => {
                write!(formatter, "invalid source alias {alias:?}: {message}")
            }

            PlannerErrorKind::UnknownStage { location, name } => {
                write!(formatter, "unknown stage {name:?} at pipeline location {location}")
            }

            PlannerErrorKind::CompoundCustomStage { location, name } => write!(
                formatter,
                "custom compound stage {name:?} at pipeline location {location} is not supported",
            ),

            PlannerErrorKind::MissingStageArguments { location, stage } => write!(
                formatter,
                "stage {stage:?} at pipeline location {location} requires arguments",
            ),

            PlannerErrorKind::UnexpectedStageArguments {
                location,
                stage,
                arguments,
            } => write!(
                formatter,
                "stage {stage:?} at pipeline location {location} does not accept arguments, found {arguments:?}",
            ),

            PlannerErrorKind::MissingSubPipeline { location, stage } => write!(
                formatter,
                "compound stage {stage:?} at pipeline location {location} requires a sub-pipeline",
            ),

            PlannerErrorKind::UnexpectedSubPipeline { location, stage } => write!(
                formatter,
                "simple stage {stage:?} at pipeline location {location} does not accept a sub-pipeline",
            ),

            PlannerErrorKind::InvalidExpression {
                location,
                stage,
                expression,
                message,
            } => write!(
                formatter,
                "invalid expression {expression:?} for stage {stage:?} at pipeline location {location}: {message}",
            ),

            PlannerErrorKind::InvalidSetSyntax { location, message } => write!(
                formatter,
                "invalid set syntax at pipeline location {location}: {message}",
            ),

            PlannerErrorKind::InvalidSetAssignment {
                location,
                assignment_index,
                assignment,
                message,
            } => write!(
                formatter,
                "invalid set assignment {assignment:?} at pipeline location {location}, assignment index {assignment_index}: {message}",
            ),

            PlannerErrorKind::InvalidSetField {
                location,
                assignment_index,
                field,
                message,
            } => write!(
                formatter,
                "invalid set field {field:?} at pipeline location {location}, assignment index {assignment_index}: {message}",
            ),

            PlannerErrorKind::InvalidStageSyntax {
                location,
                stage,
                message,
            } => write!(
                formatter,
                "invalid {stage} syntax at pipeline location {location}: {message}",
            ),

            PlannerErrorKind::InvalidStageItem {
                location,
                stage,
                item_index,
                item,
                message,
            } => write!(
                formatter,
                "invalid {stage} item {item:?} at pipeline location {location}, item index {item_index}: {message}",
            ),

            PlannerErrorKind::InvalidCompoundChild {
                location,
                parent,
                child,
                message,
            } => write!(
                formatter,
                "invalid child stage {child:?} in {parent:?} at pipeline location {location}: {message}",
            ),

            PlannerErrorKind::MissingCompoundDirective {
                location,
                parent,
                directive,
            } => write!(
                formatter,
                "compound stage {parent:?} at pipeline location {location} requires directive {directive:?}",
            ),

            PlannerErrorKind::DuplicateCompoundDirective {
                location,
                parent,
                directive,
            } => write!(
                formatter,
                "compound stage {parent:?} contains duplicate directive {directive:?} at pipeline location {location}",
            ),

            PlannerErrorKind::MissingUnionSource { location } => write!(
                formatter,
                "union stage at pipeline location {location} requires a leading `on` or `from` stage",
            ),

            PlannerErrorKind::InvalidUnionSource { location, found } => write!(
                formatter,
                "union stage requires a leading `on` or `from` stage, found {found:?} at pipeline location {location}",
            ),

            PlannerErrorKind::MissingLoadChunk { location } => write!(
                formatter,
                "streaming load at pipeline location {location} requires at least one `chunk` stage",
            ),

            PlannerErrorKind::LogicalPlan { location, message } => match location {
                Some(location) => write!(
                    formatter,
                    "logical-plan validation failed at pipeline location {location}: {message}",
                ),
                None => write!(formatter, "logical-plan validation failed: {message}"),
            },
        }
    }
}

impl std::error::Error for PlannerError {}

/// Detailed planner diagnostic category.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PlannerErrorKind {
    InvalidAst {
        message: Arc<str>,
    },
    InvalidSource {
        source: Arc<str>,
        message: Arc<str>,
    },
    InvalidSourceAlias {
        alias: Arc<str>,
        message: Arc<str>,
    },
    UnknownStage {
        location: StageLocation,
        name: Arc<str>,
    },
    CompoundCustomStage {
        location: StageLocation,
        name: Arc<str>,
    },
    MissingStageArguments {
        location: StageLocation,
        stage: Arc<str>,
    },
    UnexpectedStageArguments {
        location: StageLocation,
        stage: Arc<str>,
        arguments: Arc<str>,
    },
    MissingSubPipeline {
        location: StageLocation,
        stage: Arc<str>,
    },
    UnexpectedSubPipeline {
        location: StageLocation,
        stage: Arc<str>,
    },
    InvalidExpression {
        location: StageLocation,
        stage: Arc<str>,
        expression: Arc<str>,
        message: Arc<str>,
    },
    /// Preserved for source compatibility with previous callers.
    InvalidSetSyntax {
        location: StageLocation,
        message: Arc<str>,
    },
    InvalidSetAssignment {
        location: StageLocation,
        assignment_index: usize,
        assignment: Arc<str>,
        message: Arc<str>,
    },
    InvalidSetField {
        location: StageLocation,
        assignment_index: usize,
        field: Arc<str>,
        message: Arc<str>,
    },
    InvalidStageSyntax {
        location: StageLocation,
        stage: Arc<str>,
        message: Arc<str>,
    },
    InvalidStageItem {
        location: StageLocation,
        stage: Arc<str>,
        item_index: usize,
        item: Arc<str>,
        message: Arc<str>,
    },
    InvalidCompoundChild {
        location: StageLocation,
        parent: Arc<str>,
        child: Arc<str>,
        message: Arc<str>,
    },
    MissingCompoundDirective {
        location: StageLocation,
        parent: Arc<str>,
        directive: Arc<str>,
    },
    DuplicateCompoundDirective {
        location: StageLocation,
        parent: Arc<str>,
        directive: Arc<str>,
    },
    MissingUnionSource {
        location: StageLocation,
    },
    InvalidUnionSource {
        location: StageLocation,
        found: Arc<str>,
    },
    MissingLoadChunk {
        location: StageLocation,
    },
    LogicalPlan {
        location: Option<StageLocation>,
        message: Arc<str>,
    },
}

struct LookupHeader<'a> {
    collection: &'a str,
    alias: Option<&'a str>,
}

struct SourceDirective<'a> {
    collection: &'a str,
    alias: Option<&'a str>,
}

struct CompiledLookup<'a> {
    into: &'a str,
    stages: Vec<CompiledStage>,
}

struct CompiledUnion<'a> {
    source: &'a str,
    alias: Option<&'a str>,
    stages: Vec<CompiledStage>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompiledStage {
    name: Arc<str>,
    arguments: Arc<str>,
}

impl CompiledStage {
    fn from_operator(stage: &PlannerStage, operator: LogicalOperator) -> Self {
        let arguments = match operator {
            LogicalOperator::Custom { arguments, .. } => arguments,
            _ => Arc::from(stage.arguments().trim()),
        };

        Self {
            name: Arc::from(stage.name().as_str()),
            arguments,
        }
    }
}

fn set_once<T, E>(
    slot: &mut Option<T>,
    value: T,
    duplicate_error: impl FnOnce() -> E,
) -> Result<(), E> {
    if slot.is_some() {
        return Err(duplicate_error());
    }

    *slot = Some(value);
    Ok(())
}

const GROUP_SUM_MARKER_PREFIX: &str = "__og_group_sum_";
const GROUP_KEY_MARKER_PREFIX: &str = "__og_group_key_";

fn select_projection_syntax() -> String {
    "expected `field`, `field as alias`, or `expression as alias`; examples: `select name, age`, `select name as customer`, `select price * quantity as total`".to_owned()
}

fn parse_group_key(text: &str) -> Result<(&str, Option<&str>), String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("expected `<field>` or `<field> as <alias>`".to_owned());
    }

    let (source, alias) = match text.rsplit_once(" as ") {
        Some((source, alias)) => {
            let source = source.trim();
            let alias = alias.trim();
            if source.is_empty() || alias.is_empty() {
                return Err("expected `<field>` or `<field> as <alias>`".to_owned());
            }
            (source, Some(alias))
        }
        None => (text, None),
    };

    parse_field_path(source)?;
    if let Some(alias) = alias {
        parse_field_path(alias)?;
    }
    Ok((source, alias))
}

fn group_key_marker(source: &str, alias: &str) -> Result<ExpressionFieldPath, String> {
    let marker = format!(
        "{GROUP_KEY_MARKER_PREFIX}{}_{}",
        encode_group_marker_text(source),
        encode_group_marker_text(alias),
    );
    parse_field_path(&marker)
}

fn parse_group_measure(text: &str) -> Result<(&str, &str), String> {
    let words = text.split_whitespace().collect::<Vec<_>>();

    let (source, alias) = match words.as_slice() {
        [source] => (*source, source.rsplit('.').next().unwrap_or(source)),
        [source, "as", alias] => (*source, *alias),
        _ => return Err("expected `<field>` or `<field> as <alias>`".to_owned()),
    };

    parse_field_path(source)?;
    parse_single_identifier(alias)?;
    Ok((source, alias))
}

fn group_sum_marker(source: &str, alias: &str) -> Result<ExpressionFieldPath, String> {
    let marker = format!(
        "{GROUP_SUM_MARKER_PREFIX}{}_{}",
        encode_group_marker_text(source),
        encode_group_marker_text(alias),
    );
    parse_field_path(&marker)
}

fn encode_group_marker_text(text: &str) -> String {
    let mut encoded = String::with_capacity(text.len() * 2);
    for byte in text.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn parse_pivot_aggregate(text: &str) -> Result<PivotAggregate, String> {
    let words = text.split_whitespace().collect::<Vec<_>>();

    match words.as_slice() {
        ["sum"] => Ok(PivotAggregate::Sum),
        ["count"] => Ok(PivotAggregate::Count),
        ["avg" | "average"] => Ok(PivotAggregate::Average),
        ["min" | "minimum"] => Ok(PivotAggregate::Minimum),
        ["max" | "maximum"] => Ok(PivotAggregate::Maximum),
        ["first"] => Ok(PivotAggregate::First),
        ["last"] => Ok(PivotAggregate::Last),
        _ => Err(
            "expected one pivot aggregate: `sum`, `count`, `avg`, `average`, `min`, `minimum`, `max`, `maximum`, `first`, or `last`"
                .to_owned(),
        ),
    }
}

fn parse_lookup_header(text: &str) -> Result<LookupHeader<'_>, String> {
    let source = parse_source_directive(text)?;

    Ok(LookupHeader {
        collection: source.collection,
        alias: source.alias,
    })
}

fn parse_source_directive(text: &str) -> Result<SourceDirective<'_>, String> {
    let words = text.split_whitespace().collect::<Vec<_>>();

    match words.as_slice() {
        [collection] => {
            validate_collection_name_text(collection)?;

            Ok(SourceDirective {
                collection,
                alias: None,
            })
        }

        [collection, "as", alias] => {
            validate_collection_name_text(collection)?;
            parse_single_identifier(alias)?;

            Ok(SourceDirective {
                collection,
                alias: Some(alias),
            })
        }

        _ => Err("expected `<collection>` or `<collection> as <alias>`".to_owned()),
    }
}

fn validate_collection_name_text(text: &str) -> Result<(), String> {
    CollectionName::parse(text)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn parse_load_mode(text: &str) -> Result<&str, String> {
    let words = text.split_whitespace().collect::<Vec<_>>();

    match words.as_slice() {
        ["replace"] => Ok("replace"),
        ["update"] => Ok("update"),
        ["merge"] => Ok("merge"),
        _ => Err("expected one load mode: `replace`, `update`, or `merge`".to_owned()),
    }
}

fn parse_single_identifier(text: &str) -> Result<&str, String> {
    let text = text.trim();

    if text.is_empty() {
        return Err("identifier must not be empty".to_owned());
    }

    if text.split_whitespace().count() != 1 {
        return Err("expected exactly one identifier".to_owned());
    }

    let mut characters = text.char_indices();

    let Some((_, first)) = characters.next() else {
        return Err("identifier must not be empty".to_owned());
    };

    if first != '_' && !first.is_alphabetic() {
        return Err("identifier must start with an alphabetic character or '_'".to_owned());
    }

    for (index, character) in characters {
        if character != '_' && !character.is_alphabetic() && !character.is_ascii_digit() {
            return Err(format!(
                "identifier contains invalid character {character:?} at byte index {index}",
            ));
        }
    }

    Ok(text)
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

        parse_single_identifier(segment)
            .map_err(|message| format!("invalid field-path segment {index}: {message}"))?;
    }

    ExpressionFieldPath::new(segments).map_err(|error| error.to_string())
}

fn parse_rename_arguments(text: &str) -> Result<(&str, &str), String> {
    let words = text.split_whitespace().collect::<Vec<_>>();

    match words.as_slice() {
        [source, "as", target] => Ok((source, target)),
        _ => Err("expected `<source> as <target>`".to_owned()),
    }
}

fn parse_count_alias(text: &str) -> Result<&str, String> {
    let words = text.split_whitespace().collect::<Vec<_>>();

    match words.as_slice() {
        ["as", alias] => parse_single_identifier(alias),
        _ => Err("expected no arguments or `as <alias>`".to_owned()),
    }
}

fn split_assignment(text: &str) -> Result<(&str, &str), String> {
    let mut state = ScanState::default();

    for (index, character) in text.char_indices() {
        state.consume(character)?;

        if character == '=' && state.is_top_level() {
            let previous = text[..index].chars().next_back();
            let next = text[index + character.len_utf8()..].chars().next();

            if previous == Some('=') || next == Some('=') {
                continue;
            }

            let field = text[..index].trim();
            let expression = text[index + 1..].trim();

            if field.is_empty() {
                return Err("assignment field must not be empty".to_owned());
            }

            if expression.is_empty() {
                return Err("assignment expression must not be empty".to_owned());
            }

            return Ok((field, expression));
        }
    }

    state.finish()?;
    Err("assignment must contain a top-level `=`".to_owned())
}

fn write_length_prefixed(output: &mut String, value: &str) {
    output.push_str(&value.len().to_string());
    output.push(':');
    output.push_str(value);
}

fn write_optional_length_prefixed(output: &mut String, value: Option<&str>) {
    match value {
        Some(value) => write_length_prefixed(output, value),
        None => output.push('-'),
    }
}

fn write_compiled_stages(output: &mut String, stages: &[CompiledStage]) {
    output.push('[');

    for stage in stages {
        output.push('{');
        write_length_prefixed(output, &stage.name);
        output.push(',');
        write_length_prefixed(output, &stage.arguments);
        output.push('}');
    }

    output.push(']');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{parse, parse_expression, Literal};

    #[inline]
    fn span() -> Span {
        Span::new(0, 1)
    }

    fn stage(name: &str, arguments: &str) -> PlannerStage {
        PlannerStage::new(StageName::parse(name).unwrap(), arguments, span())
    }

    fn compound(name: &str, arguments: &str, stages: Vec<PlannerStage>) -> PlannerStage {
        PlannerStage::with_subpipeline(
            StageName::parse(name).unwrap(),
            arguments,
            PlannerSubPipeline::new(stages, span()),
            span(),
        )
    }

    fn plan_one(name: &str, arguments: &str) -> LogicalPlan {
        Planner::new()
            .plan(&PlannerPipeline::new("users", [stage(name, arguments)]))
            .unwrap()
    }

    #[test]
    fn plans_all_simple_native_stages() {
        assert!(plan_one("where", "age >= 18").has_filter());
        assert!(plan_one("set", "enabled=true").has_set());
        assert!(plan_one("load", "profile").has_load());
        assert!(plan_one("limit", "10").has_limit());
        assert!(plan_one("skip", "20").has_skip());
        assert!(plan_one("sort", "age desc, name asc").has_sort());
        assert!(plan_one("select", "name, profile.country").has_select());
        let rename = plan_one("rename", "name as display_name");
        let rename = rename
            .operator(0)
            .expect("rename should produce one logical operator");
        assert!(matches!(
            rename,
            LogicalOperator::Custom {
                stage,
                arguments,
                mutating: false,
            } if stage.as_str() == "rename" && arguments.as_ref() == "name as display_name"
        ));

        let drop = plan_one("drop", "age, profile.secret");
        let drop = drop
            .operator(0)
            .expect("drop should produce one logical operator");
        assert!(matches!(
            drop,
            LogicalOperator::Custom {
                stage,
                arguments,
                mutating: false,
            } if stage.as_str() == "drop" && arguments.as_ref() == "age, profile.secret"
        ));
        assert!(plan_one("distinct", "email").has_distinct());
        assert!(plan_one("distinct", "").has_distinct());
        assert!(plan_one("count", "as total").has_count());
        assert!(plan_one("delete", "").has_delete());
        assert!(plan_one("insert", "{ name: \"Alice\" }").has_insert());
        assert!(plan_one("group", "country, city").has_group());
    }

    #[test]
    fn plans_set_with_structured_json_literals() {
        let plan = plan_one(
            "set",
            r#"definition = {"views":{"tile":[{"type":"metric","value":42}]}}, permissions = ["read","write"]"#,
        );
        let assignments = plan
            .operator(0)
            .and_then(LogicalOperator::assignments)
            .expect("set should expose assignments");
        assert_eq!(assignments.len(), 2);
        assert!(matches!(
            assignments[0].value().as_literal(),
            Some(Literal::Json(value)) if value.starts_with('{')
        ));
        assert!(matches!(
            assignments[1].value().as_literal(),
            Some(Literal::Json(value)) if value.starts_with('[')
        ));
    }

    #[test]
    fn plans_compact_group_with_alias_on_grouping_key_and_measures() {
        let plan = plan_one(
            "group",
            "Receptionnaire_Code as Client, Article_Code as Produit, CAFacture as CA",
        );
        let operator = plan.operator(0).expect("group operator");
        let fields = operator.group_keys().expect("group fields");

        assert_eq!(fields.len(), 3);
        assert!(fields[0].to_string().starts_with(GROUP_KEY_MARKER_PREFIX));
        assert!(fields[1].to_string().starts_with(GROUP_KEY_MARKER_PREFIX));
        assert!(fields[2].to_string().starts_with(GROUP_SUM_MARKER_PREFIX));
    }

    #[test]
    fn plans_compound_group_with_aliases_on_by_keys() {
        let group = compound(
            "group",
            "",
            vec![
                stage("by", "customer.country as Country, customer.city as City"),
                stage("sum", "CAFacture as Total_CA"),
            ],
        );
        let plan = Planner::new()
            .plan(&PlannerPipeline::new("data", [group]))
            .unwrap();
        let fields = plan
            .operator(0)
            .and_then(LogicalOperator::group_keys)
            .expect("group fields");

        assert_eq!(fields.len(), 3);
        assert!(fields[0].to_string().starts_with(GROUP_KEY_MARKER_PREFIX));
        assert!(fields[1].to_string().starts_with(GROUP_KEY_MARKER_PREFIX));
        assert!(fields[2].to_string().starts_with(GROUP_SUM_MARKER_PREFIX));
    }

    #[test]
    fn compact_group_uses_all_but_last_item_as_dimensions() {
        let plan = plan_one("group", "x, y, z");
        let fields = plan
            .operator(0)
            .and_then(LogicalOperator::group_keys)
            .expect("group fields");

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].to_string(), "x");
        assert_eq!(fields[1].to_string(), "y");
        assert!(fields[2].to_string().starts_with(GROUP_SUM_MARKER_PREFIX));
    }

    #[test]
    fn compact_group_with_one_item_remains_a_pure_dimension() {
        let plan = plan_one("group", "x");
        let fields = plan
            .operator(0)
            .and_then(LogicalOperator::group_keys)
            .expect("group fields");

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].to_string(), "x");
    }

    #[test]
    fn plans_compact_group_with_automatic_sum_measure() {
        let plan = plan_one("group", "Article_Code, CAFacture as CA");
        let operator = plan.operator(0).expect("group operator");
        let fields = operator.group_keys().expect("group fields");

        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].to_string(), "Article_Code");
        assert!(fields[1].to_string().starts_with(GROUP_SUM_MARKER_PREFIX));
    }

    #[test]
    fn plans_compound_group_with_explicit_sum() {
        let group = compound(
            "group",
            "",
            vec![
                stage("by", "Article_Code"),
                stage("sum", "CAFacture as Total_CA"),
            ],
        );
        let plan = Planner::new()
            .plan(&PlannerPipeline::new("data", [group]))
            .unwrap();
        let fields = plan
            .operator(0)
            .and_then(LogicalOperator::group_keys)
            .expect("group fields");

        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].to_string(), "Article_Code");
        assert!(fields[1].to_string().starts_with(GROUP_SUM_MARKER_PREFIX));
    }

    #[test]
    fn compound_group_requires_by_directive() {
        let group = compound("group", "", vec![stage("sum", "CAFacture as Total_CA")]);
        let error = Planner::new()
            .plan(&PlannerPipeline::new("data", [group]))
            .unwrap_err();

        assert!(error.to_string().contains("requires directive \"by\""));
    }

    #[test]
    fn group_can_be_followed_by_sort() {
        let plan = Planner::new()
            .plan(&PlannerPipeline::new(
                "data",
                [stage("group", "Article_Code"), stage("sort", "count desc")],
            ))
            .unwrap();

        assert_eq!(plan.len(), 2);
        assert!(plan.has_group());
        assert!(plan.has_sort());
    }

    #[test]
    fn aggregated_group_can_be_followed_by_sort() {
        let plan = Planner::new()
            .plan(&PlannerPipeline::new(
                "data",
                [
                    stage("group", "Article_Code, CAFacture as CA"),
                    stage("sort", "CA desc"),
                ],
            ))
            .unwrap();

        assert_eq!(plan.len(), 2);
        assert!(plan.has_group());
        assert!(plan.has_sort());
    }

    #[test]
    fn group_can_be_followed_by_select_and_limit() {
        let plan = Planner::new()
            .plan(&PlannerPipeline::new(
                "data",
                [
                    stage("group", "Article_Code"),
                    stage("select", "Article_Code, count"),
                    stage("limit", "10"),
                ],
            ))
            .unwrap();

        assert_eq!(plan.len(), 3);
        assert!(plan.has_group());
        assert!(plan.has_select());
        assert!(plan.has_limit());
    }

    #[test]
    fn plans_offset_as_skip() {
        let plan = Planner::new()
            .plan(&PlannerPipeline::new("data", [stage("offset", "2")]))
            .unwrap();
        assert!(matches!(
            plan.operator(0),
            Some(LogicalOperator::Skip { count: 2 })
        ));
    }

    #[test]
    fn plans_first_single_and_unwind() {
        for (name, args) in [
            ("first", ""),
            ("first", "Article_Code"),
            ("single", ""),
            ("single", "Article_Code"),
            ("unwind", "data"),
        ] {
            let plan = Planner::new()
                .plan(&PlannerPipeline::new("data", [stage(name, args)]))
                .unwrap();
            assert!(
                matches!(plan.operator(0), Some(LogicalOperator::Custom { stage, .. }) if stage.as_str() == name)
            );
        }
    }

    #[test]
    fn validates_rename_syntax() {
        let error = Planner::new()
            .plan(&PlannerPipeline::new("users", [stage("rename", "name")]))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("expected `<source> as <target>`"));

        let error = Planner::new()
            .plan(&PlannerPipeline::new(
                "users",
                [stage("rename", "name as name")],
            ))
            .unwrap_err();
        assert!(error.to_string().contains("must be different"));
    }

    #[test]
    fn validates_drop_field_list() {
        let error = Planner::new()
            .plan(&PlannerPipeline::new("users", [stage("drop", "")]))
            .unwrap_err();
        assert!(error.to_string().contains("requires arguments"));
    }

    #[test]
    fn plans_insert_with_nested_object_value() {
        let specification = r#"{
            _id: "u1",
            name: "John",
            active: true,
            tags: ["rust", "database"],
            address: {
                city: "Paris",
            },
        }"#;

        let plan = plan_one("insert", specification);

        assert!(plan.has_insert());
        assert!(!plan.is_read_only());
    }

    #[test]
    fn plans_insert_ast_directly() {
        let source = r#"from users
        | insert {
            _id: "u1",
            name: "John",
            tags: ["rust", "database"],
        }"#;

        let ast = parse(source).unwrap();
        let plan = Planner::new().plan_ast(source, &ast).unwrap();

        assert!(plan.has_insert());
    }

    #[test]
    fn rejects_insert_scalar_value() {
        let error = Planner::new()
            .plan(&PlannerPipeline::new(
                "users",
                [stage("insert", r#""John""#)],
            ))
            .unwrap_err();

        assert!(matches!(error.kind(), PlannerErrorKind::LogicalPlan { .. }));
        assert!(error
            .to_string()
            .contains("insert document must be an object"));
    }

    #[test]
    fn rejects_insert_array_value() {
        let error = Planner::new()
            .plan(&PlannerPipeline::new(
                "users",
                [stage("insert", r#"[{name: "John"}]"#)],
            ))
            .unwrap_err();

        assert!(matches!(error.kind(), PlannerErrorKind::LogicalPlan { .. }));
        assert!(error
            .to_string()
            .contains("insert document must be an object"));
    }

    #[test]
    fn rejects_malformed_insert_object() {
        let error = Planner::new()
            .plan(&PlannerPipeline::new(
                "users",
                [stage("insert", r#"{name "John"}"#)],
            ))
            .unwrap_err();

        assert!(matches!(error.kind(), PlannerErrorKind::LogicalPlan { .. }));
        assert!(error
            .to_string()
            .contains("invalid logical insert document"));
        assert!(error.to_string().contains("expected `:` after object key"));
    }

    #[test]
    fn rejects_multiple_insert_values() {
        let error = Planner::new()
            .plan(&PlannerPipeline::new(
                "users",
                [stage("insert", r#"{name: "John"} {name: "Jane"}"#)],
            ))
            .unwrap_err();

        assert!(matches!(error.kind(), PlannerErrorKind::LogicalPlan { .. }));
        assert!(error.to_string().contains("unexpected token"));
    }

    #[test]
    fn plans_typed_pivot() {
        let pivot = compound(
            "pivot",
            "",
            vec![
                stage("rows", "region"),
                stage("columns", "month"),
                stage("values", "revenue"),
                stage("aggregate", "sum"),
            ],
        );

        let plan = Planner::new()
            .plan(&PlannerPipeline::new("sales", [pivot]))
            .unwrap();

        assert!(plan.has_pivot());

        let specification = plan
            .operator(0)
            .and_then(LogicalOperator::pivot_specification)
            .expect("typed pivot specification");

        assert_eq!(specification.rows().len(), 1);
        assert_eq!(specification.columns().len(), 1);
        assert_eq!(specification.values().len(), 1);
        assert_eq!(specification.values()[0].aggregate(), PivotAggregate::Sum);
    }

    #[test]
    fn rejects_incomplete_or_duplicate_pivot_directives() {
        let incomplete = compound(
            "pivot",
            "",
            vec![stage("rows", "region"), stage("columns", "month")],
        );

        let error = Planner::new()
            .plan(&PlannerPipeline::new("sales", [incomplete]))
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            PlannerErrorKind::MissingCompoundDirective { directive, .. }
                if directive.as_ref() == "values"
        ));

        let duplicate = compound(
            "pivot",
            "",
            vec![
                stage("rows", "region"),
                stage("rows", "country"),
                stage("columns", "month"),
                stage("values", "revenue"),
                stage("aggregate", "sum"),
            ],
        );

        let error = Planner::new()
            .plan(&PlannerPipeline::new("sales", [duplicate]))
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            PlannerErrorKind::DuplicateCompoundDirective { directive, .. }
                if directive.as_ref() == "rows"
        ));
    }

    #[test]
    fn plans_pivot_ast_directly() {
        let source = r#"on sales
        | pivot
            | rows region
            | columns month
            | values revenue
            | aggregate sum
        | end"#;

        let ast = parse(source).unwrap();
        let plan = Planner::new().plan_ast(source, &ast).unwrap();

        assert!(plan.has_pivot());
    }

    #[test]
    fn normalizes_ast_with_source_alias() {
        let source = "on users as u | where active == true";
        let ast = parse(source).unwrap();
        let normalized = PlannerPipeline::from_ast(source, &ast).unwrap();

        assert_eq!(normalized.source(), "users");
        assert_eq!(normalized.source_alias(), Some("u"));
        assert_eq!(normalized.len(), 1);
        assert_eq!(normalized.stages()[0].name().as_str(), "where");
    }

    #[test]
    fn plans_ast_directly() {
        let source = "on users | where active == true";
        let ast = parse(source).unwrap();

        let plan = Planner::new().plan_ast(source, &ast).unwrap();

        assert!(plan.has_filter());
    }

    #[test]
    fn plans_lookup_compound_stage() {
        let lookup = compound(
            "lookup",
            "workspace as w",
            vec![stage("where", "w.public == true"), stage("into", "public")],
        );

        let plan = Planner::new()
            .plan(&PlannerPipeline::new("users", [lookup]))
            .unwrap();

        let operator = plan.operator(0).unwrap();

        assert!(matches!(
            operator,
            LogicalOperator::Custom { stage, mutating: false, .. }
                if stage.as_str() == "lookup"
        ));
    }

    #[test]
    fn plans_join_as_normalized_lookup() {
        let join = compound(
            "join",
            "workspace as w",
            vec![stage("where", "w.public == true"), stage("into", "public")],
        );

        let plan = Planner::new()
            .plan(&PlannerPipeline::new("users", [join]))
            .unwrap();

        assert!(matches!(
            plan.operator(0).unwrap(),
            LogicalOperator::Custom { stage, mutating: false, .. }
                if stage.as_str() == "lookup"
        ));
    }

    #[test]
    fn plans_union_compound_stage() {
        let union = compound(
            "union",
            "",
            vec![
                stage("on", "archived_users"),
                stage("where", "active == true"),
            ],
        );

        let plan = Planner::new()
            .plan(&PlannerPipeline::new("users", [union]))
            .unwrap();

        assert!(matches!(
            plan.operator(0).unwrap(),
            LogicalOperator::Custom { stage, mutating: false, .. }
                if stage.as_str() == "union"
        ));
    }

    #[test]
    fn plans_streaming_load() {
        let load = compound(
            "load",
            "",
            vec![
                stage("with", "replace"),
                stage("chunk", "batch1"),
                stage("chunk", "batch2"),
            ],
        );

        let plan = Planner::new()
            .plan(&PlannerPipeline::new("users", [load]))
            .unwrap();

        assert!(plan.has_load());
        assert!(plan.is_mutating());

        let specification = plan
            .operator(0)
            .unwrap()
            .load_specification()
            .expect("load specification");

        assert!(specification.starts_with("streaming;mode=replace;chunks="));
    }

    #[test]
    fn validates_lookup_directives() {
        let lookup = compound(
            "lookup",
            "workspace",
            vec![stage("where", "public == true")],
        );

        let error = Planner::new()
            .plan(&PlannerPipeline::new("users", [lookup]))
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            PlannerErrorKind::MissingCompoundDirective { directive, .. }
                if directive.as_ref() == "into"
        ));
    }

    #[test]
    fn validates_union_source_is_first() {
        let union = compound(
            "union",
            "",
            vec![
                stage("where", "active == true"),
                stage("on", "archived_users"),
            ],
        );

        let error = Planner::new()
            .plan(&PlannerPipeline::new("users", [union]))
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            PlannerErrorKind::InvalidUnionSource { .. }
        ));
    }

    #[test]
    fn validates_streaming_load_directives() {
        let load = compound("load", "", vec![stage("with", "replace")]);

        let error = Planner::new()
            .plan(&PlannerPipeline::new("users", [load]))
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            PlannerErrorKind::MissingLoadChunk { .. }
        ));
    }

    #[test]
    fn rejects_mutating_stage_inside_lookup() {
        let lookup = compound(
            "lookup",
            "workspace",
            vec![stage("delete", ""), stage("into", "public")],
        );

        let error = Planner::new()
            .plan(&PlannerPipeline::new("users", [lookup]))
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            PlannerErrorKind::InvalidCompoundChild { .. }
        ));
    }

    #[test]
    fn plans_combined_read_pipeline() {
        let pipeline = PlannerPipeline::new(
            "users",
            [
                stage("where", "active == true"),
                stage("sort", "age desc, name"),
                stage("skip", "10"),
                stage("limit", "20"),
                stage("select", "name, age"),
                stage("distinct", "name"),
            ],
        );

        let plan = Planner::new().plan(&pipeline).unwrap();

        assert_eq!(plan.len(), 6);
        assert!(plan.is_read_only());
    }

    #[test]
    fn select_accepts_mixed_plain_and_aliased_fields() {
        let plan = plan_one("select", "CAFacture as CA, COGS");
        let operator = plan
            .operator(0)
            .expect("select should produce one operator");

        assert!(matches!(
            operator,
            LogicalOperator::Custom {
                stage,
                arguments,
                mutating: false,
            } if stage.as_str() == "select" && arguments.as_ref() == "CAFacture as CA, COGS"
        ));
    }

    #[test]
    fn select_accepts_expression_projection_with_alias() {
        let plan = plan_one("select", "CAFacture as CA, COGS, CA - COGS as Marge");
        let operator = plan
            .operator(0)
            .expect("select should produce one operator");

        assert!(matches!(
            operator,
            LogicalOperator::Custom {
                stage,
                arguments,
                mutating: false,
            } if stage.as_str() == "select"
                && arguments.as_ref() == "CAFacture as CA, COGS, CA - COGS as Marge"
        ));
    }

    #[test]
    fn select_requires_alias_for_expression_projection() {
        let error = Planner::new()
            .plan(&PlannerPipeline::new(
                "users",
                [stage("select", "price * quantity")],
            ))
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("expression projection requires an alias"));
    }

    #[test]
    fn validates_native_stage_arguments() {
        for name in [
            "where", "set", "load", "limit", "skip", "sort", "select", "insert", "group",
        ] {
            let error = Planner::new()
                .plan(&PlannerPipeline::new("users", [stage(name, "")]))
                .unwrap_err();

            assert!(matches!(
                error.kind(),
                PlannerErrorKind::MissingStageArguments { .. }
            ));
        }

        assert!(matches!(
            Planner::new()
                .plan(&PlannerPipeline::new("users", [stage("delete", "now")]))
                .unwrap_err()
                .kind(),
            PlannerErrorKind::UnexpectedStageArguments { .. }
        ));
    }

    #[test]
    fn validates_limit_skip_sort_and_count_syntax() {
        for (name, arguments) in [
            ("limit", "-1"),
            ("limit", "1 2"),
            ("skip", "one"),
            ("sort", "age sideways"),
            ("count", "total"),
            ("count", "as total extra"),
        ] {
            assert!(matches!(
                Planner::new()
                    .plan(&PlannerPipeline::new("users", [stage(name, arguments)]))
                    .unwrap_err()
                    .kind(),
                PlannerErrorKind::InvalidStageSyntax { .. }
                    | PlannerErrorKind::InvalidStageItem { .. }
            ));
        }
    }

    #[test]
    fn preserves_commas_and_equals_inside_nested_text() {
        let values = split_top_level("label=\"a,b\", enabled=true", ',').unwrap();

        assert_eq!(values, vec!["label=\"a,b\"", "enabled=true"]);

        let (field, expression) = split_assignment("enabled=status == true").unwrap();

        assert_eq!(field, "enabled");
        assert_eq!(expression, "status == true");
    }

    #[test]
    fn rejects_unknown_stage_by_default() {
        let error = Planner::new()
            .plan(&PlannerPipeline::new(
                "users",
                [stage("inspect", "verbose")],
            ))
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            PlannerErrorKind::UnknownStage { .. }
        ));
    }

    #[test]
    fn optionally_preserves_simple_custom_stage() {
        let planner = Planner::with_options(PlannerOptions::new(true));

        let plan = planner
            .plan(&PlannerPipeline::new(
                "users",
                [stage("inspect", "verbose")],
            ))
            .unwrap();

        assert_eq!(plan.len(), 1);
    }

    #[test]
    fn rejects_custom_compound_stage() {
        let planner = Planner::with_options(PlannerOptions::new(true));

        let error = planner
            .plan(&PlannerPipeline::new(
                "users",
                [compound("inspect", "", vec![stage("where", "true")])],
            ))
            .unwrap_err();

        assert!(matches!(
            error.kind(),
            PlannerErrorKind::CompoundCustomStage { .. }
        ));
    }

    #[test]
    fn stage_locations_are_recursive() {
        let location = StageLocation::root(2).child(4).child(1);

        assert_eq!(location.to_string(), "2.4.1");
        assert_eq!(location.depth(), 3);
        assert_eq!(location.index(0), Some(2));
        assert_eq!(location.index(2), Some(1));
        assert_eq!(location.index(3), None);
    }

    #[test]
    fn expression_parser_is_still_used_for_where() {
        let expression = parse_expression("age >= 18").unwrap();
        assert_eq!(
            plan_one("where", "age >= 18")
                .operator(0)
                .unwrap()
                .predicate(),
            Some(&expression),
        );
    }
}
