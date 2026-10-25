//! Pipeline-to-plan translation.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{fmt, sync::Arc};

use super::plan_values::{
    CollectionName, InsertDocument, PivotAggregate, PivotSpecification,
    PivotValue, SetAssignment, SortKey,
};
use super::{BinaryOperator, Expression, Literal, PhysicalAccess, PhysicalOperator, PhysicalPlan, PhysicalPlanner, PhysicalSubPipeline};
use crate::storage::{CollectionId, DocumentId};
use super::syntax::{
    parse_sort_item, split_assignment, split_sort_direction, split_top_level,
    split_top_level_word, validate_balanced_text, UnwindSpec,
};
use super::{parse_expression, Arguments, ExpressionFieldPath, PhysicalLoadMode, PipelineAst, Span, Stage, StageAst, StageName};

use crate::query::ast::Spanned;

/// Result returned by planning operations.
pub type PlanningResult<T> = std::result::Result<T, PlannerError>;

/// Normalized source accepted by the semantic planner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannerSource {
    collection: Arc<str>,
    alias: Option<Arc<str>>,
    temporal: Option<crate::storage::Temporal>,
}

impl PlannerSource {
    /// Creates a normalized source without an alias.
    #[must_use]
    #[inline]
    pub fn new(collection: impl AsRef<str>) -> Self {
        Self {
            collection: Arc::from(collection.as_ref()),
            alias: None,
            temporal: None,
        }
    }

    /// Creates a normalized source with an alias.
    #[must_use]
    pub fn with_alias(collection: impl AsRef<str>, alias: impl AsRef<str>) -> Self {
        Self {
            collection: Arc::from(collection.as_ref()),
            alias: Some(Arc::from(alias.as_ref())),
            temporal: None,
        }
    }

    /// Reads the source at a past state or as its history.
    #[must_use]
    pub fn with_temporal(mut self, temporal: crate::storage::Temporal) -> Self {
        self.temporal = Some(temporal);
        self
    }

    /// The temporal read of the source (`as of`, `history`), if any.
    #[must_use]
    pub const fn temporal(&self) -> Option<&crate::storage::Temporal> {
        self.temporal.as_ref()
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

        let mut source = match pipeline.source().alias_name(source_text) {
            Some(alias) => PlannerSource::with_alias(collection, alias),
            None => PlannerSource::new(collection),
        };
        if let Some(temporal) = pipeline.source().temporal() {
            source = source.with_temporal(source_temporal(source_text, collection, temporal)?);
        }

        let mut stages = Vec::with_capacity(pipeline.stage_count());

        for index in 0..pipeline.stage_count() {
            let stage = pipeline.stage(index).ok_or_else(|| {
                PlannerError::invalid_ast(format!("pipeline reports stage {index} but does not expose it"), Some(pipeline.span()))
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
    pub fn with_subpipeline( name: StageName, arguments: impl AsRef<str>, subpipeline: PlannerSubPipeline, span: Span, ) -> Self {
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
            PlannerError::invalid_ast("stage-name span does not belong to the supplied source text", Some(stage.span()))
        })?;

        let canonical_name = match name_text {
            "project" => "select",
            other => other,
        };

        let name = StageName::parse(canonical_name).map_err(|error| {
            PlannerError::invalid_ast(format!("invalid AST stage name {name_text:?}: {error}"), Some(stage.span()))
        })?;

        let arguments = stage.arguments_text(source_text).ok_or_else(|| {
            PlannerError::invalid_ast(format!("arguments span of stage {name_text:?} is invalid"), Some(stage.span()))
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

/// Rewrites a top-level stage so `<source alias>.field` reads `field`.
///
/// At the root the source document is the only one in scope, so the alias is a
/// pure qualifier. `lookup`, `union`, `load`, `insert`, `into` and `out` are left untouched:
/// inside a sub-pipeline the alias names the outer document and is resolved at
/// run time.
fn strip_source_alias(stage: &PlannerStage, alias: &str) -> PlannerStage {
    if matches!(stage.name().as_str(), "lookup" | "join" | "union" | "load" | "insert" | "into" | "out") {
        return stage.clone();
    }
    let arguments = strip_alias_prefix(stage.arguments(), alias);
    match stage.subpipeline() {
        Some(body) => PlannerStage::with_subpipeline(
            stage.name().clone(),
            arguments,
            PlannerSubPipeline::new(
                body.stages().iter().map(|child| strip_source_alias(child, alias)),
                body.span(),
            ),
            stage.span(),
        ),
        None => PlannerStage::new(stage.name().clone(), arguments, stage.span()),
    }
}

/// Removes `alias.` before a field name, outside string literals and
/// without touching longer identifiers or already-qualified paths.
fn strip_alias_prefix(text: &str, alias: &str) -> String {
    let is_word = |c: char| c.is_alphanumeric() || c == '_';
    let chars = text.chars().collect::<Vec<_>>();
    let alias_chars = alias.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    let mut quote: Option<char> = None;
    while index < chars.len() {
        let c = chars[index];
        if let Some(open) = quote {
            out.push(c);
            if c == '\\' && index + 1 < chars.len() {
                out.push(chars[index + 1]);
                index += 1;
            } else if c == open {
                quote = None;
            }
            index += 1;
            continue;
        }
        if c == '"' || c == '\'' {
            quote = Some(c);
        } else if index == 0 || !(is_word(chars[index - 1]) || chars[index - 1] == '.') {
            let end = index + alias_chars.len();
            if chars[index..].starts_with(&alias_chars)
                && chars.get(end) == Some(&'.')
                && chars.get(end + 1).is_some_and(|next| next.is_alphabetic() || *next == '_')
            {
                index = end + 1;
                continue;
            }
        }
        out.push(c);
        index += 1;
    }
    out
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

    /// Converts a parsed pipeline AST directly into a plan.
    pub fn plan_ast( &self, source_text: &str, pipeline: &PipelineAst, ) -> PlanningResult<PhysicalPlan> {
        let normalized = PlannerPipeline::from_ast(source_text, pipeline)?;
        self.plan(&normalized)
    }

    /// Converts a normalized pipeline into an executable plan.
    pub fn plan(&self, pipeline: &PlannerPipeline) -> PlanningResult<PhysicalPlan> {
        self.validate_source_alias(pipeline.source_descriptor())?;

        let invalid_source = |message: String| PlannerError::new(
            PlannerErrorKind::InvalidSource { source: Arc::from(pipeline.source()), message: Arc::from(message) },
            None,
        );
        let collection = CollectionName::parse(pipeline.source()).map_err(|error| invalid_source(error.to_string()))?;
        let collection = CollectionId::parse(collection.to_string()).map_err(|error| invalid_source(error.to_string()))?;
        let mut builder = PhysicalPlanner::new().plan_collection(collection);
        builder.set_source_temporal(pipeline.source_descriptor().temporal().cloned());

        for (index, stage) in pipeline.stages().iter().enumerate() {
            let location = StageLocation::root(index);
            let unqualified;
            let stage = match pipeline.source_alias() {
                Some(alias) => {
                    unqualified = strip_source_alias(stage, alias);
                    &unqualified
                }
                None => stage,
            };
            for (stage, scope_alias) in self.desugar_stage(location, stage, pipeline.source_alias())? {
                let scope_alias = scope_alias.as_deref().or(pipeline.source_alias());
                let operator = self.compile_stage(location, &stage, scope_alias)?;

                builder.push(operator).map_err(|error| self.plan_error(location, &stage, error))?;
            }
        }
        if let Some(depth) = builder.operators().iter().map(nesting_depth).max().filter(|depth| *depth > MAX_COMPOUND_DEPTH) {
            return Err(PlannerError::new(
                PlannerErrorKind::Plan {
                    location: None,
                    message: Arc::from(format!("compound-stage nesting of {depth} exceeds the supported limit of {MAX_COMPOUND_DEPTH}")),
                },
                None,
            ));
        }
        builder.push_filters();
        // `_id` is the master index of every collection: a leading UUIDv7
        // equality becomes a direct lookup, the filter staying as a check.
        if let Some(id) = builder.operators().first().and_then(PhysicalOperator::predicate).and_then(primary_key_equality) {
            builder.set_source_access(PhysicalAccess::PrimaryKeyLookup { id });
        }
        builder.finish().map_err(|error| {
            PlannerError::new(
                PlannerErrorKind::Plan {
                    location: None,
                    message: Arc::from(error.to_string()),
                },
                None,
            )
        })
    }

    fn collection_id( &self, location: StageLocation, stage: &PlannerStage, collection: &str, ) -> PlanningResult<CollectionId> {
        CollectionId::parse(collection).map_err(|error| self.plan_error(location, stage, format!("invalid collection {collection:?}: {error}")))
    }

    /// Rewrites a root stage written in a shorthand form into the stages it
    /// stands for. Each stage comes with an optional scope alias override
    /// (used when the shorthand needs to name the outer document).
    fn desugar_stage( &self, location: StageLocation, stage: &PlannerStage, source_alias: Option<&str>, ) -> PlanningResult<Vec<(PlannerStage, Option<Arc<str>>)>> {
        let unchanged = || Ok(vec![(stage.clone(), None)]);
        let native = Stage::parse(stage.name().as_str());
        if stage.is_compound() {
            return match native {
                Some(Stage::Pivot) => self.desugar_pivot(location, stage),
                Some(Stage::Group) => self.desugar_group(location, stage),
                Some(Stage::Window) => self.desugar_window(location, stage),
                _ => unchanged(),
            };
        }
        match native {
            Some(Stage::Sort) => self.check_form(location, stage, Stage::Sort).and_then(|()| self.desugar_sort(location, stage)),
            Some(Stage::Bucket) => self.check_form(location, stage, Stage::Bucket).and_then(|()| self.desugar_bucket(location, stage)),
            Some(Stage::Group) if !stage.arguments().trim().is_empty() => self.desugar_compact_group(location, stage),
            Some(Stage::Fill) => {
                self.check_form(location, stage, Stage::Fill)?;
                let spec = super::FillSpec::parse(stage.arguments()).map_err(|message| self.invalid_syntax(location, stage, message))?;
                let keys = spec.sort_fields().iter().map(ToString::to_string).collect::<Vec<_>>().join(", ");
                Ok(vec![(synthetic_stage("sort", &keys, stage), None), (stage.clone(), None)])
            }
            Some(Stage::Lookup) if split_top_level_word(stage.arguments(), "into").is_some() => {
                self.desugar_lookup(location, stage, source_alias)
            }
            _ => unchanged(),
        }
    }

    /// `sort <expression> [asc|desc], …`: every key that is not a plain field
    /// is computed by a `derive` into a hidden field, sorted on, then dropped.
    fn desugar_sort( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<Vec<(PlannerStage, Option<Arc<str>>)>> {
        let arguments = stage.arguments().trim();
        let items = split_top_level(arguments, ',').map_err(|message| self.invalid_syntax(location, stage, message))?;
        let mut keys = Vec::with_capacity(items.len());
        let mut derived = Vec::new();
        for (item_index, item) in items.into_iter().enumerate() {
            let (key, direction) = split_sort_direction(item);
            let direction = match direction {
                super::SortDirection::Ascending => "asc",
                super::SortDirection::Descending => "desc",
            };
            if parse_field_path(key).is_ok() {
                keys.push(format!("{key} {direction}"));
                continue;
            }
            parse_expression(key).map_err(|error| {
                self.invalid_list_item(location, stage, item_index, item, format!("invalid sort key {key:?}: {error}"))
            })?;
            let hidden = format!("{HIDDEN_SORT_FIELD_PREFIX}{item_index}");
            derived.push((hidden.clone(), key));
            keys.push(format!("{hidden} {direction}"));
        }
        if derived.is_empty() {
            return Ok(vec![(stage.clone(), None)]);
        }
        let assignments = derived.iter().map(|(field, key)| format!("{field} = {key}")).collect::<Vec<_>>().join(", ");
        let hidden_fields = derived.iter().map(|(field, _)| field.as_str()).collect::<Vec<_>>().join(", ");
        Ok(vec![
            (synthetic_stage("derive", &assignments, stage), None),
            (synthetic_stage("sort", &keys.join(", "), stage), None),
            (synthetic_stage("drop", &hidden_fields, stage), None),
        ])
    }

    /// `pivot | rows … | columns … | values … | end`: every value, row or
    /// column that is not a plain field is computed by a `derive` into a hidden
    /// field; values are measured under their alias, rows renamed after.
    fn desugar_pivot( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<Vec<(PlannerStage, Option<Arc<str>>)>> {
        let Some(body) = stage.subpipeline() else {
            return Ok(vec![(stage.clone(), None)]);
        };
        let (mut derived, mut renamed) = (Vec::new(), Vec::new());
        let mut children = Vec::with_capacity(body.stages().len());
        for (index, child) in body.stages().iter().enumerate() {
            if matches!(child.name().as_str(), "rows" | "columns") && !child.is_compound() {
                // A column's values name the cells: its alias is not kept.
                let mut unused = Vec::new();
                let sink = if child.name().as_str() == "rows" { &mut renamed } else { &mut unused };
                let keys = self.hide_expression_items(location.child(index), child, HIDDEN_PIVOT_FIELD_PREFIX, &mut derived, Some(sink))?;
                children.push(synthetic_stage(child.name().as_str(), &keys, child));
                continue;
            }
            if child.name().as_str() != "values" || child.is_compound() {
                children.push(child.clone());
                continue;
            }
            let child_location = location.child(index);
            let arguments = self.required_arguments(child_location, child)?;
            let items = split_top_level(arguments, ',').map_err(|message| self.invalid_syntax(child_location, child, message))?;
            let mut values = Vec::with_capacity(items.len());
            for (item_index, item) in items.into_iter().enumerate() {
                let item = item.trim();
                let Some((source, alias)) = item.rsplit_once(" as ") else {
                    values.push(item.to_owned());
                    continue;
                };
                let (source, alias) = (source.trim(), alias.trim());
                if parse_field_path(source).is_ok() {
                    values.push(item.to_owned());
                    continue;
                }
                parse_expression(source).map_err(|error| {
                    self.invalid_list_item(child_location, child, item_index, item, format!("invalid pivot value {source:?}: {error}"))
                })?;
                let hidden = format!("{HIDDEN_PIVOT_FIELD_PREFIX}{}", derived.len());
                values.push(format!("{hidden} as {alias}"));
                derived.push(format!("{hidden} = {source}"));
            }
            children.push(synthetic_stage("values", &values.join(", "), child));
        }
        let mut stages = with_derived(stage, &derived, children);
        stages.extend(renamed.iter().map(|(hidden, alias)| (synthetic_stage("rename", &format!("{hidden} as {alias}"), stage), None)));
        Ok(stages)
    }

    /// `group | … | <aggregate> <value> [by <order>] [where <predicate>] [as <alias>] | … | end`:
    /// an expression, or the value where a predicate fails (`null`), is a
    /// hidden field computed by a `derive`. `first`/`last … by` measure two
    /// fields, the value and the order (`null` where the predicate fails).
    /// `median`, `percentile <value> <fraction>` and `stddev` are a `list` of
    /// the values that a `derive` after the group reduces with the function
    /// of the same name.
    fn desugar_group( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<Vec<(PlannerStage, Option<Arc<str>>)>> {
        let Some(body) = stage.subpipeline() else {
            return Ok(vec![(stage.clone(), None)]);
        };
        let (mut derived, mut reduced) = (Vec::new(), Vec::new());
        let mut children = Vec::with_capacity(body.stages().len());
        for (index, child) in body.stages().iter().enumerate() {
            let aggregate = child.name().as_str();
            if aggregate == "by" && !child.is_compound() {
                let keys = self.hide_expression_items(location.child(index), child, HIDDEN_GROUP_FIELD_PREFIX, &mut derived, None)?;
                children.push(synthetic_stage("by", &keys, child));
                continue;
            }
            if child.is_compound() || !matches!(aggregate, "sum" | "avg" | "min" | "max" | "first" | "last" | "count" | "list" | "median" | "percentile" | "stddev") {
                children.push(child.clone());
                continue;
            }
            let invalid = |message: String| self.invalid_syntax(location.child(index), child, message);
            let (rest, alias) = split_top_level_word(child.arguments(), "as").map_or((child.arguments().trim(), None), |(rest, alias)| (rest, Some(alias)));
            let (rest, predicate) = split_top_level_word(rest, "where").map_or((rest, None), |(rest, predicate)| (rest, Some(predicate)));
            let (rest, fraction) = match rest.rsplit_once(char::is_whitespace) {
                Some((value, fraction)) if aggregate == "percentile" && fraction.parse::<f64>().is_ok() => (value.trim(), Some(fraction)),
                _ if aggregate == "percentile" => return Err(invalid("expected `percentile <value> <fraction>`, a fraction between 0 and 1".to_owned())),
                _ => (rest, None),
            };
            let reduce = matches!(aggregate, "median" | "percentile" | "stddev");
            let (value, order) = split_top_level_word(rest, "by").map_or((rest, None), |(value, order)| (value, Some(order)));
            let picks = matches!(aggregate, "first" | "last");
            if order.is_some() && !picks {
                return Err(invalid(format!("`{aggregate}` takes no `by <order>`: only `first` and `last` do")));
            }
            if picks && predicate.is_some() && order.is_none() {
                return Err(invalid(format!("`{aggregate} … where` needs `by <order>`")));
            }
            let field = |text: &str| parse_field_path(text).is_ok();
            if predicate.is_none() && order.is_none() && !reduce && (value.is_empty() || field(value)) {
                children.push(child.clone());
                continue;
            }
            let alias = match alias {
                Some(alias) => parse_single_identifier(alias).map_err(invalid)?.to_owned(),
                None => parse_group_measure_directive(aggregate, value).map_err(|_| invalid(format!("an expression measure needs `as <alias>`: `{aggregate} <expression> as <alias>`")))?.1,
            };
            let value = if value.is_empty() { "1" } else { value };
            for text in [Some(value), order, predicate].into_iter().flatten() {
                parse_expression(text).map_err(|error| invalid(format!("invalid expression {text:?}: {error}")))?;
            }
            let mut hide = |text: &str, kept: bool| {
                let expression = match predicate.filter(|_| kept) {
                    Some(predicate) => format!("if({predicate}, {text}, null)"),
                    None if field(text) => return text.to_owned(),
                    None => text.to_owned(),
                };
                derived.push(format!("{HIDDEN_GROUP_FIELD_PREFIX}{} = {expression}", derived.len()));
                format!("{HIDDEN_GROUP_FIELD_PREFIX}{}", derived.len() - 1)
            };
            children.push(match order {
                Some(order) => {
                    let value = hide(value, false);
                    synthetic_stage(&format!("{aggregate}by"), &format!("{value} {} as {alias}", hide(order, true)), child)
                }
                None if reduce => {
                    reduced.push(format!("{alias} = {aggregate}({alias}{})", fraction.map_or_else(String::new, |fraction| format!(", {fraction}"))));
                    synthetic_stage("list", &format!("{} as {alias}", hide(value, true)), child)
                }
                None => synthetic_stage(aggregate, &format!("{} as {alias}", hide(value, true)), child),
            });
        }
        let mut stages = with_derived(stage, &derived, children);
        stages.extend((!reduced.is_empty()).then(|| (synthetic_stage("derive", &reduced.join(", "), stage), None)));
        Ok(stages)
    }

    /// Items of a `group`, `by`, `rows` or `columns` list: each `<expression>
    /// as <alias>` that is not a field becomes a hidden field computed by a
    /// `derive` (pushed on `derived`), written `<hidden> as <alias>`, or bare
    /// `<hidden>` with its alias in `renamed` when the stage takes no `as`.
    fn hide_expression_items( &self, location: StageLocation, stage: &PlannerStage, prefix: &str, derived: &mut Vec<String>, mut renamed: Option<&mut Vec<(String, String)>>, ) -> PlanningResult<String> {
        let items = split_top_level(stage.arguments().trim(), ',').map_err(|message| self.invalid_syntax(location, stage, message))?;
        let mut out = Vec::with_capacity(items.len());
        for (item_index, item) in items.into_iter().enumerate() {
            let item = item.trim();
            let (source, alias) = split_top_level_word(item, "as").map_or((item, None), |(source, alias)| (source, Some(alias)));
            if parse_field_path(source).is_ok() {
                out.push(item.to_owned());
                continue;
            }
            let invalid = |message: String| self.invalid_list_item(location, stage, item_index, item, message);
            parse_expression(source).map_err(|error| invalid(format!("invalid expression {source:?}: {error}")))?;
            let alias = parse_single_identifier(alias.ok_or_else(|| invalid(format!("an expression needs `as <alias>`: `{source} as <alias>`")))?).map_err(invalid)?;
            let hidden = format!("{prefix}{}", derived.len());
            derived.push(format!("{hidden} = {source}"));
            out.push(match renamed.as_deref_mut() { Some(renamed) => { renamed.push((hidden.clone(), alias.to_owned())); hidden } None => format!("{hidden} as {alias}") });
        }
        Ok(out.join(", "))
    }

    /// Compact `group <key>, …, <measure>` whose items may be expressions.
    fn desugar_compact_group( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<Vec<(PlannerStage, Option<Arc<str>>)>> {
        let mut derived = Vec::new();
        let items = self.hide_expression_items(location, stage, HIDDEN_GROUP_FIELD_PREFIX, &mut derived, None)?;
        if derived.is_empty() { return Ok(vec![(stage.clone(), None)]) }
        Ok(vec![(synthetic_stage("derive", &derived.join(", "), stage), None), (synthetic_stage("group", &items, stage), None)])
    }

    /// `window | partition by <fields> | order by <keys> | <functions> | end`:
    /// a `sort` by the partition then the order, and the window without its
    /// `order by`, which runs on the sorted rows.
    fn desugar_window( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<Vec<(PlannerStage, Option<Arc<str>>)>> {
        let body = self.required_subpipeline(location, stage)?;
        let mut keys = Vec::new();
        let mut order = None;
        for (index, child) in body.stages().iter().enumerate() {
            let child_location = location.child(index);
            let by = || child.arguments().trim().strip_prefix("by").filter(|rest| rest.starts_with(char::is_whitespace)).map(str::trim).ok_or_else(|| self.invalid_syntax(child_location, child, format!("expected `{} by <fields>`", child.name().as_str())));
            match child.name().as_str() {
                "partition" => keys.extend(split_top_level(by()?, ',').map_err(|message| self.invalid_syntax(child_location, child, message))?.into_iter().map(str::to_owned)),
                "order" if order.is_some() => return Err(self.duplicate_directive(child_location, stage, child)),
                "order" => order = Some(by()?.to_owned()),
                _ => {}
            }
        }
        let order = order.ok_or_else(|| self.missing_directive(location, stage, "order"))?;
        keys.push(order);
        let children = body.stages().iter().filter(|child| child.name().as_str() != "order").cloned().collect();
        let mut stages = vec![(synthetic_stage("sort", &keys.join(", "), stage), None)];
        stages.extend(with_derived(stage, &[], children));
        Ok(stages)
    }

    /// `window` after [`Self::desugar_window`]: `partition by <fields>` and
    /// `lag|lead <field> [<n>]`, `cumsum|first|last <field>`, `row_number`,
    /// each `[as <alias>]`, written as the runtime reads them
    /// ([`super::WindowSpec`]).
    fn compile_window( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let body = self.required_subpipeline(location, stage)?;
        let (mut partition, mut functions) = (Vec::new(), Vec::new());
        for (index, child) in body.stages().iter().enumerate() {
            let child_location = location.child(index);
            self.require_simple(child_location, child)?;
            let name = child.name().as_str();
            let invalid = |message: String| self.invalid_syntax(child_location, child, message);
            let field = |text: &str| parse_field_path(text).map(|_| text.to_owned()).map_err(invalid);
            let arguments = child.arguments().trim();
            match name {
                "partition" => {
                    let fields = arguments.strip_prefix("by").map(str::trim).unwrap_or_default();
                    for item in split_top_level(fields, ',').map_err(invalid)? {
                        partition.push(field(item.trim())?);
                    }
                }
                "order" => return Err(invalid("`window` sorts its rows only at the top level of a pipeline".to_owned())),
                "lag" | "lead" | "cumsum" | "first" | "last" | "row_number" => {
                    let (rest, alias) = split_top_level_word(arguments, "as").map_or((arguments, None), |(rest, alias)| (rest, Some(alias)));
                    let words = rest.split_whitespace().collect::<Vec<_>>();
                    let expected = || invalid(match name {
                        "lag" | "lead" => format!("expected `{name} <field> [<rows>] [as <alias>]`"),
                        "row_number" => "expected `row_number [as <alias>]`".to_owned(),
                        _ => format!("expected `{name} <field> [as <alias>]`"),
                    });
                    let (source, offset) = match (name, words.as_slice()) {
                        ("row_number", []) => (None, None),
                        ("lag" | "lead", [source, rows]) => (Some(field(source)?), Some(rows.parse::<usize>().ok().filter(|rows| *rows > 0).ok_or_else(expected)?)),
                        (_, [source]) if name != "row_number" => (Some(field(source)?), None),
                        _ => return Err(expected()),
                    };
                    let alias = match (alias, &source) {
                        (Some(alias), _) => field(alias)?,
                        (None, Some(source)) => format!("{name}_{}", source.rsplit('.').next().unwrap_or(source)),
                        (None, None) => name.to_owned(),
                    };
                    functions.push([Some(name.to_owned()), source, offset.map(|rows| rows.to_string()), Some(format!("as {alias}"))].into_iter().flatten().collect::<Vec<_>>().join(" "));
                }
                _ => return Err(self.invalid_child_stage(child_location, stage, child, "window accepts `partition by <fields>`, `order by <keys>` and `lag|lead <field> [<rows>]`, `cumsum|first|last <field>`, `row_number`, each `[as <alias>]`")),
            }
        }
        if functions.is_empty() {
            return Err(self.invalid_syntax(location, stage, "`window` needs at least one function: `lag`, `lead`, `cumsum`, `first`, `last` or `row_number`".to_owned()));
        }
        native_stage(Stage::Window, format!("{} | {}", partition.join(", "), functions.join(", ")))
    }

    /// `bucket <expression> by [b0, b1, …] [default <value>] [as <alias>]`:
    /// counts rows per range `[b(i), b(i+1))`, like `derive` + `group`.
    fn desugar_bucket( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<Vec<(PlannerStage, Option<Arc<str>>)>> {
        let syntax = "expected `bucket <expression> by [<boundaries>] [default <value>] [as <alias>]`";
        let arguments = stage.arguments().trim();
        let (expression, rest) = split_top_level_word(arguments, "by")
            .ok_or_else(|| self.invalid_syntax(location, stage, syntax))?;
        let (rest, alias) = split_top_level_word(rest, "as").unwrap_or((rest, "bucket"));
        let (boundaries, default) = match split_top_level_word(rest, "default") {
            Some((boundaries, default)) => (boundaries, Some(default)),
            None => (rest, None),
        };
        if expression.is_empty() || !boundaries.starts_with('[') {
            return Err(self.invalid_syntax(location, stage, syntax));
        }
        let alias = parse_single_identifier(alias).map_err(|message| self.invalid_syntax(location, stage, message))?;
        let call = match default {
            Some(default) => format!("bucket({expression}, {boundaries}, {default})"),
            None => format!("bucket({expression}, {boundaries})"),
        };
        self.parse_stage_expression(location, stage, &call)?;
        Ok(vec![
            (synthetic_stage("derive", &format!("{alias} = {call}"), stage), None),
            (synthetic_stage("group", alias, stage), None),
        ])
    }

    /// `lookup <collection> [as <alias>] on <joined field> == <outer field> into <field>`:
    /// the one-line equality join, rewritten into the compound `lookup` form.
    fn desugar_lookup( &self, location: StageLocation, stage: &PlannerStage, source_alias: Option<&str>, ) -> PlanningResult<Vec<(PlannerStage, Option<Arc<str>>)>> {
        let syntax = "expected `lookup <collection> [as <alias>] on <joined field> == <outer field> into <field>`";
        let invalid = |message: &str| self.invalid_syntax(location, stage, message.to_owned());
        let (source, rest) = split_top_level_word(stage.arguments(), "on").ok_or_else(|| invalid(syntax))?;
        let (condition, into) = split_top_level_word(rest, "into").ok_or_else(|| invalid(syntax))?;
        let source = parse_source_directive(source).map_err(|message| invalid(&message))?;
        let into = parse_single_identifier(into).map_err(|message| invalid(&message))?;
        let (joined, outer) = condition.split_once("==").ok_or_else(|| invalid(syntax))?;
        let inner_alias = source.alias.unwrap_or(LOOKUP_INNER_ALIAS);
        let outer_alias = source_alias.unwrap_or(LOOKUP_OUTER_ALIAS);
        if inner_alias == outer_alias {
            return Err(invalid("the joined collection alias must differ from the source alias"));
        }
        let qualify = |path: &str, alias: &str| -> PlanningResult<String> {
            let path = path.trim();
            let qualified = match path.strip_prefix(alias).and_then(|rest| rest.strip_prefix('.')) {
                Some(_) => path.to_owned(),
                None => format!("{alias}.{path}"),
            };
            parse_field_path(&qualified).map_err(|message| invalid(&message))?;
            Ok(qualified)
        };
        // A row missing either field simply does not match.
        let (joined, outer) = (qualify(joined, inner_alias)?, qualify(outer, outer_alias)?);
        let predicate = format!("exists({joined}) and exists({outer}) and {joined} == {outer}");
        let body = PlannerSubPipeline::new(
            [synthetic_stage("where", &predicate, stage), synthetic_stage("into", into, stage)],
            stage.span(),
        );
        let compound = PlannerStage::with_subpipeline(
            stage.name().clone(),
            format!("{} as {inner_alias}", source.collection),
            body,
            stage.span(),
        );
        Ok(vec![(compound, Some(Arc::from(outer_alias)))])
    }

    /// `scope_alias` names the document the stage runs on (the source alias at
    /// the top level, the enclosing lookup/union alias inside sub-pipelines);
    /// nested lookups use it to resolve correlated fields.
    fn compile_stage( &self, location: StageLocation, stage: &PlannerStage, scope_alias: Option<&str>, ) -> PlanningResult<PhysicalOperator> {
        // `bucket` exists only as a root shorthand (`desugar_stage`).
        let Some(native) = Stage::parse(stage.name().as_str()).filter(|native| *native != Stage::Bucket) else {
            return if stage.is_compound() {
                Err(PlannerError::new(PlannerErrorKind::CompoundCustomStage { location, name: Arc::from(stage.name().as_str()) }, Some(stage.span())))
            } else if self.options.allow_custom_stages {
                custom_stage(stage.name().clone(), stage.arguments())
            } else {
                Err(PlannerError::new(PlannerErrorKind::UnknownStage { location, name: Arc::from(stage.name().as_str()) }, Some(stage.span())))
            };
        };
        self.check_form(location, stage, native)?;
        match native {
            Stage::Where => self.compile_where(location, stage),
            Stage::Near | Stage::Derive => native_stage(native, stage.arguments().trim()),
            Stage::Root => self.compile_root(location, stage),
            Stage::Sample => self.compile_sample(location, stage),
            Stage::Window => self.compile_window(location, stage),
            Stage::Fill if location.depth() > 1 => Err(self.invalid_syntax(location, stage, "`fill` sorts its rows only at the top level of a pipeline".to_owned())),
            Stage::Fill => native_stage(Stage::Fill, stage.arguments().trim()),
            Stage::Set => self.compile_set(location, stage),
            Stage::Lookup => self.compile_lookup(location, stage, scope_alias),
            Stage::Union => self.compile_union(location, stage),
            Stage::Load => self.compile_load(location, stage),
            Stage::Limit => self.compile_limit(location, stage),
            Stage::Skip => self.compile_skip(location, stage),
            Stage::First | Stage::Single => self.compile_first(location, stage, native),
            Stage::Unwind => self.compile_unwind(location, stage),
            Stage::Sort => self.compile_sort(location, stage),
            Stage::Select => self.compile_select(location, stage),
            Stage::Rename => self.compile_rename(location, stage),
            Stage::Drop => self.compile_drop(location, stage),
            Stage::Distinct => self.compile_distinct(location, stage),
            Stage::Count => self.compile_count(location, stage),
            Stage::Delete => Ok(PhysicalOperator::delete()),
            Stage::Insert => self.compile_insert(location, stage),
            Stage::Into => self.compile_into(location, stage),
            Stage::Group => self.compile_group(location, stage),
            Stage::Pivot => self.compile_pivot(location, stage),
            Stage::Bucket => unreachable!("filtered above"),
        }
    }

    /// Checks the stage form (simple or compound) and its arguments against
    /// the catalogue entry of `native`.
    fn check_form( &self, location: StageLocation, stage: &PlannerStage, native: Stage ) -> PlanningResult<()> {
        let entry = native.entry();
        let arguments = match (stage.is_compound(), entry.simple, entry.compound) {
            (false, Some(arguments), _) | (true, _, Some(arguments)) => arguments,
            (true, Some(_), None) => return self.require_simple(location, stage),
            (false, None, Some(arguments)) => {
                self.check_arguments(location, stage, arguments)?;
                return self.required_subpipeline(location, stage).map(drop);
            }
            (_, None, None) => unreachable!("every catalogue stage has a form"),
        };
        self.check_arguments(location, stage, arguments)
    }

    fn check_arguments( &self, location: StageLocation, stage: &PlannerStage, arguments: Arguments ) -> PlanningResult<()> {
        match arguments {
            Arguments::Required => self.required_arguments(location, stage).map(drop),
            Arguments::Forbidden => self.require_no_arguments(location, stage),
            Arguments::Optional => Ok(()),
        }
    }

    fn compile_where( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        let predicate = self.parse_stage_expression(location, stage, arguments)?;
        Ok(PhysicalOperator::filter(predicate))
    }


    fn compile_root( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        parse_field_path(arguments).map_err(|message| self.invalid_syntax(location, stage, message))?;
        native_stage(Stage::Root, arguments)
    }

    fn compile_sample( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let count = self.parse_non_negative_integer(location, stage)?;
        native_stage(Stage::Sample, count.to_string())
    }


    fn compile_set( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        let assignments = split_top_level(arguments, ',').map_err(|message| self.invalid_syntax(location, stage, message))?;
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

        PhysicalOperator::set(compiled).map_err(|error| self.plan_error(location, stage, error))
    }

    fn compile_lookup( &self, location: StageLocation, stage: &PlannerStage, outer_alias: Option<&str>, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        let subpipeline = self.required_subpipeline(location, stage)?;

        let header = parse_lookup_header(arguments).map_err(|message| self.invalid_syntax(location, stage, message))?;

        let compiled = self.compile_lookup_body(location, stage, subpipeline, header.alias)?;

        let pipeline = PhysicalSubPipeline::new(compiled.stages).map_err(|error| self.plan_error(location, stage, error))?;
        PhysicalOperator::lookup(self.collection_id(location, stage, header.collection)?, header.alias, compiled.into, pipeline)
            .map(|operator| operator.with_outer_alias(outer_alias))
            .map_err(|error| self.plan_error(location, stage, error))
    }

    fn compile_union( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let subpipeline = self.required_subpipeline(location, stage)?;

        let compiled = self.compile_union_body(location, stage, subpipeline)?;

        let pipeline = PhysicalSubPipeline::new(compiled.stages).map_err(|error| self.plan_error(location, stage, error))?;
        PhysicalOperator::union(self.collection_id(location, stage, compiled.source)?, compiled.alias, pipeline)
            .map_err(|error| self.plan_error(location, stage, error))
    }

    fn compile_load( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        match stage.subpipeline() {
            None => {
                let arguments = stage.arguments().trim();

                PhysicalOperator::load(arguments).map_err(|error| self.plan_error(location, stage, error))
            }

            Some(subpipeline) => {
                        self.compile_streaming_load_body(location, stage, subpipeline)
            }
        }
    }

    fn compile_limit( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let count = self.parse_non_negative_integer(location, stage)?;
        Ok(PhysicalOperator::limit(count))
    }

    fn compile_skip( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let count = self.parse_non_negative_integer(location, stage)?;
        Ok(PhysicalOperator::skip(count))
    }

    fn compile_first( &self, location: StageLocation, stage: &PlannerStage, native: Stage ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        if !arguments.is_empty() {
            parse_field_path(arguments).map_err(|message| self.invalid_syntax(location, stage, message))?;
        }
        native_stage(native, arguments)
    }


    fn compile_unwind( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        let spec = UnwindSpec::parse(arguments).map_err(|message| self.invalid_syntax(location, stage, message))?;
        for path in std::iter::once(spec.field).chain(spec.index) {
            parse_field_path(path).map_err(|message| self.invalid_syntax(location, stage, message))?;
        }
        native_stage(Stage::Unwind, arguments)
    }

    fn compile_sort( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        let items = split_top_level(arguments, ',').map_err(|message| self.invalid_syntax(location, stage, message))?;
        let mut keys = Vec::with_capacity(items.len());
        for (item_index, item) in items.into_iter().enumerate() {
            let (field_text, direction) = parse_sort_item(item).map_err(|message| {
                self.invalid_list_item(location, stage, item_index, item, message)
            })?;

            let field = parse_field_path(field_text).map_err(|message| self.invalid_list_item(location, stage, item_index, item, message))?;

            keys.push(SortKey::new(field, direction));
        }
        PhysicalOperator::sort(keys).map_err(|error| self.plan_error(location, stage, error))
    }

    fn compile_select( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        let items = split_top_level(arguments, ',').map_err(|message| self.invalid_syntax(location, stage, message))?;

        let mut normalized = Vec::with_capacity(items.len());
        let mut plain_fields = Vec::with_capacity(items.len());
        let mut requires_custom_projection = false;

        for (item_index, item) in items.into_iter().enumerate() {
            let item = item.trim();
            if item.is_empty() {
                return Err(self.invalid_list_item(location, stage, item_index, item, select_projection_syntax()));
            }

            if let Some((source_text, target_text)) = item.rsplit_once(" as ") {
                let source_text = source_text.trim();
                let target_text = target_text.trim();
                if source_text.is_empty() || target_text.is_empty() {
                    return Err(self.invalid_list_item(location, stage, item_index, item, select_projection_syntax()));
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

                normalized.push(format!("{source_text} as {target}"));
                requires_custom_projection = true;
            } else {
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

        if requires_custom_projection {
            return native_stage(Stage::Select, normalized.join(", "));
        }

        PhysicalOperator::select(plain_fields).map_err(|error| self.plan_error(location, stage, error))
    }

    fn compile_rename( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        let (source, target) = parse_rename_arguments(arguments).map_err(|message| self.invalid_syntax(location, stage, message))?;
        let source = parse_field_path(source).map_err(|message| self.invalid_syntax(location, stage, message))?;
        let target = parse_field_path(target).map_err(|message| self.invalid_syntax(location, stage, message))?;

        if source == target {
            return Err(self.invalid_syntax(location, stage, "rename source and target must be different".to_owned()));
        }

        native_stage(Stage::Rename, format!("{source} as {target}"))
    }

    fn compile_drop( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let fields = self.parse_required_field_list(location, stage)?;
        let arguments = fields
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");

        native_stage(Stage::Drop, arguments)
    }

    fn compile_distinct( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        let fields = if arguments.is_empty() {
            Vec::new()
        } else {
            self.parse_field_list(location, stage, arguments)?
        };
        PhysicalOperator::distinct(fields).map_err(|error| self.plan_error(location, stage, error))
    }

    fn compile_count( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        let alias = if arguments.is_empty() {
            "count"
        } else {
            parse_count_alias(arguments).map_err(|message| self.invalid_syntax(location, stage, message))?
        };
        PhysicalOperator::count(alias).map_err(|error| self.plan_error(location, stage, error))
    }


    fn compile_insert( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        // `insert [{…}, {…}]` inserts a batch: one atomic streaming load in merge mode.
        if arguments.starts_with('[') {
            let chunk = insert_batch_json(arguments).map_err(|message| self.invalid_syntax(location, stage, message))?;
            return PhysicalOperator::streaming_load(PhysicalLoadMode::Merge, [chunk]).map_err(|error| self.plan_error(location, stage, error));
        }
        let document = InsertDocument::parse(arguments).map_err(|error| self.plan_error(location, stage, error))?;
        Ok(PhysicalOperator::insert(document))
    }

    /// `into <collection>` (alias `out`): terminal stage that inserts the pipeline result
    /// into another collection. Lowered as a mutating extension operator: its
    /// rows are streamed into one transaction (see `engine::execute`).
    fn compile_into( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let arguments = stage.arguments().trim();
        let collection = CollectionName::parse(arguments).map_err(|error| self.plan_error(location, stage, error))?
            .to_string();
        // Materialized views refresh their cache collections with `into`.
        if collection.starts_with('_') && !collection.starts_with(crate::view::CACHE_PREFIX) {
            return Err(self.invalid_syntax(location, stage, "into cannot target a system collection"));
        }
        native_stage(Stage::Into, collection)
    }

    fn compile_group( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let mut fields = Vec::new();
        let mut compact = false;
        match stage.subpipeline() {
            None => {
                let arguments = stage.arguments().trim();
                let items = split_top_level(arguments, ',').map_err(|message| self.invalid_syntax(location, stage, message))?;

                // Compact group syntax follows the relational shape people read from
                // left to right: every item except the last is a grouping dimension,
                // and the last item is the value to sum. A single item remains a pure
                // grouping key (with the implicit `count` produced by the runtime).
                //
                // This makes `group x, y, z` mean "group by x and y, sum z" instead
                // of silently treating y as another measure. Other measures are
                // available through the explicit `group | by ... | avg ... | end` form.
                compact = items.len() > 1;
                let (keys, measure) = match items.split_last() {
                    Some((last, keys)) if !keys.is_empty() => (keys, Some(*last)),
                    _ => (items.as_slice(), None),
                };
                self.push_group_keys(location, stage, keys, &mut fields)?;
                if let Some(item) = measure {
                    let invalid = |message: String| self.invalid_list_item(location, stage, keys.len(), item, message);
                    let (source, alias) = parse_group_measure_directive("sum", item).map_err(invalid)?;
                    fields.push(group_measure_marker("sum", source, &alias).map_err(invalid)?);
                }
            }

            Some(subpipeline) => {
                        let mut by_seen = false;

                for (index, child) in subpipeline.stages().iter().enumerate() {
                    let child_location = location.child(index);
                    self.require_simple(child_location, child)?;

                    match child.name().as_str() {
                        "by" => {
                            if std::mem::replace(&mut by_seen, true) {
                                return Err(self.duplicate_directive(child_location, stage, child));
                            }
                            let arguments = self.required_arguments(child_location, child)?;
                            let items = split_top_level(arguments, ',')
                                .map_err(|message| self.invalid_syntax(child_location, child, message))?;
                            self.push_group_keys(child_location, child, &items, &mut fields)?;
                        }
                        aggregate @ ("firstby" | "lastby") => {
                            let invalid = || self.invalid_syntax(child_location, child, format!("expected `{aggregate} <field> <order> as <alias>`"));
                            let [value, order, "as", alias] = child.arguments().split_whitespace().collect::<Vec<_>>()[..] else { return Err(invalid()) };
                            for text in [value, order, alias] {
                                parse_field_path(text).map_err(|_| invalid())?;
                            }
                            fields.push(group_measure_marker(aggregate, &format!("{value} {order}"), alias).map_err(|message| self.invalid_syntax(child_location, child, message))?);
                        }
                        aggregate @ ("sum" | "avg" | "min" | "max" | "first" | "last" | "count" | "list") => {
                            let invalid = |message: String| self.invalid_syntax(child_location, child, message);
                            let (source, alias) = parse_group_measure_directive(aggregate, child.arguments()).map_err(invalid)?;
                            fields.push(group_measure_marker(aggregate, source, &alias).map_err(invalid)?);
                        }
                        _ => {
                            return Err(self.invalid_child_stage(
                                child_location,
                                stage,
                                child,
                                "group accepts one `by <fields>` directive and measures `sum|avg|min|max|first|last|list|median|stddev <value> [by <order>] [where <predicate>] [as <alias>]`, `percentile <value> <fraction>` or `count [<value>] [where <predicate>] [as <alias>]`",
                            ));
                        }
                    }
                }

                if !by_seen {
                    return Err(self.missing_directive(location, stage, "by"));
                }
            }
        }

        let group = PhysicalOperator::group(fields).map_err(|error| self.plan_error(location, stage, error))?;
        Ok(if compact { group.with_auto_measures() } else { group })
    }

    /// Appends grouping keys (`field` or `field as alias`) to `fields`.
    fn push_group_keys( &self, location: StageLocation, stage: &PlannerStage, items: &[&str], fields: &mut Vec<ExpressionFieldPath>, ) -> PlanningResult<()> {
        for (index, item) in items.iter().enumerate() {
            let invalid = |message: String| self.invalid_list_item(location, stage, index, item, message);
            let (source, alias) = parse_group_key(item).map_err(invalid)?;
            let field = match alias {
                Some(alias) => group_key_marker(source, alias),
                None => parse_field_path(source),
            };
            fields.push(field.map_err(invalid)?);
        }
        Ok(())
    }

    fn compile_pivot( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<PhysicalOperator> {
        let subpipeline = self.required_subpipeline(location, stage)?;
        let specification = self.compile_pivot_body(location, stage, subpipeline)?;

        Ok(PhysicalOperator::pivot(specification))
    }

    fn compile_pivot_body( &self, parent_location: StageLocation, parent_stage: &PlannerStage, subpipeline: &PlannerSubPipeline, ) -> PlanningResult<PivotSpecification> {
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
                        self.parse_pivot_values(location, child)?,
                        || self.duplicate_directive(location, parent_stage, child),
                    )?;
                }
                "aggregate" => {
                    let arguments = self.required_arguments(location, child)?;
                    let parsed = parse_pivot_aggregate(arguments).map_err(|message| self.invalid_syntax(location, child, message))?;

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
            .map(|(field, alias)| {
                PivotValue::new(field, aggregate, alias)
                    .map_err(|error| self.plan_error(parent_location, parent_stage, error))
            })
            .collect::<PlanningResult<Vec<_>>>()?;

        PivotSpecification::new(rows, columns, values).map_err(|error| self.plan_error(parent_location, parent_stage, error))
    }

    /// `values <field> [as <alias>], …`; expressions were already computed
    /// into hidden fields by [`Self::desugar_pivot`].
    fn parse_pivot_values( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<Vec<(ExpressionFieldPath, Option<String>)>> {
        let arguments = stage.arguments().trim();
        let items = split_top_level(arguments, ',').map_err(|message| self.invalid_syntax(location, stage, message))?;
        items
            .into_iter()
            .enumerate()
            .map(|(item_index, item)| {
                let item = item.trim();
                let (source, alias) = match item.rsplit_once(" as ") {
                    Some((source, alias)) => (source.trim(), Some(alias.trim().to_owned())),
                    None => (item, None),
                };
                let field = parse_field_path(source).map_err(|_| {
                    self.invalid_list_item(
                        location,
                        stage,
                        item_index,
                        item,
                        "an expression pivot value requires an alias, for example `CAFacture - COGS as marge`".to_owned(),
                    )
                })?;
                Ok((field, alias))
            })
            .collect()
    }

    fn compile_lookup_body<'a>( &self, parent_location: StageLocation, parent_stage: &PlannerStage, subpipeline: &'a PlannerSubPipeline, scope_alias: Option<&str>, ) -> PlanningResult<CompiledLookup<'a>> {
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

                    let name = parse_single_identifier(arguments).map_err(|message| self.invalid_syntax(location, child, message))?;

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
                    compiled.push(self.compile_nested_read_stage(location, parent_stage, child, scope_alias)?);
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

    fn compile_union_body<'a>( &self, parent_location: StageLocation, parent_stage: &PlannerStage, subpipeline: &'a PlannerSubPipeline, ) -> PlanningResult<CompiledUnion<'a>> {
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

            // A branch with blocking stages runs as a query of its own: its
            // stages are written as at the top level, `fill` included.
            for (stage, scope_alias) in self.desugar_stage(location, child, source.alias)? {
                compiled.push(match Stage::parse(stage.name().as_str()) {
                    Some(Stage::Fill) => native_stage(Stage::Fill, stage.arguments().trim()).map_err(|error| self.plan_error(location, &stage, error))?,
                    _ => self.compile_nested_read_stage(location, parent_stage, &stage, scope_alias.as_deref().or(source.alias))?,
                });
            }
        }

        Ok(CompiledUnion {
            source: source.collection,
            alias: source.alias,
            stages: compiled,
        })
    }

    fn compile_streaming_load_body( &self, parent_location: StageLocation, parent_stage: &PlannerStage, subpipeline: &PlannerSubPipeline, ) -> PlanningResult<PhysicalOperator> {
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
                        parse_load_mode(arguments).map_err(|message| self.invalid_syntax(location, child, message))?,
                    );
                }

                "chunk" => {
                    self.require_simple(location, child)?;
                    let arguments = self.required_arguments(location, child)?;

                    validate_balanced_text(arguments).map_err(|message| self.invalid_syntax(location, child, message))?;

                    chunks.push(Arc::from(arguments));
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

        PhysicalOperator::streaming_load(mode, chunks.iter().map(|chunk: &Arc<str>| chunk.as_ref())).map_err(|error| self.plan_error(parent_location, parent_stage, error))
    }

    fn compile_nested_read_stage( &self, location: StageLocation, parent_stage: &PlannerStage, stage: &PlannerStage, scope_alias: Option<&str>, ) -> PlanningResult<PhysicalOperator> {
        let operator = self.compile_stage(location, stage, scope_alias)?;
        if operator.execution_properties().writes() || operator.count_alias().is_some() {
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
        Ok(operator)
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

    fn required_subpipeline<'a>( &self, location: StageLocation, stage: &'a PlannerStage, ) -> PlanningResult<&'a PlannerSubPipeline> {
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

    fn required_arguments<'a>( &self, location: StageLocation, stage: &'a PlannerStage, ) -> PlanningResult<&'a str> {
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

    fn require_no_arguments( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<()> {
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

    fn parse_non_negative_integer( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<usize> {
        let arguments = stage.arguments().trim();
        if arguments.split_whitespace().count() != 1 {
            return Err(self.invalid_syntax(location, stage, "expected exactly one non-negative integer"));
        }

        arguments.parse::<usize>().map_err(|_| {
            self.invalid_syntax(location, stage, "expected a non-negative integer representable as usize")
        })
    }

    fn parse_required_field_list( &self, location: StageLocation, stage: &PlannerStage, ) -> PlanningResult<Vec<ExpressionFieldPath>> {
        let arguments = stage.arguments().trim();
        self.parse_field_list(location, stage, arguments)
    }

    fn parse_field_list( &self, location: StageLocation, stage: &PlannerStage, arguments: &str, ) -> PlanningResult<Vec<ExpressionFieldPath>> {
        let items = split_top_level(arguments, ',').map_err(|message| self.invalid_syntax(location, stage, message))?;

        let mut fields = Vec::with_capacity(items.len());
        for (item_index, item) in items.into_iter().enumerate() {
            let field = parse_field_path(item).map_err(|message| self.invalid_list_item(location, stage, item_index, item, message))?;

            fields.push(field);
        }
        Ok(fields)
    }

    fn parse_stage_expression( &self, location: StageLocation, stage: &PlannerStage, expression: &str, ) -> PlanningResult<super::Expression> {
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

    fn invalid_syntax( &self, location: StageLocation, stage: &PlannerStage, message: impl Into<Arc<str>>, ) -> PlannerError {
        PlannerError::new(
            PlannerErrorKind::InvalidStageSyntax {
                location,
                stage: Arc::from(stage.name().as_str()),
                message: message.into(),
            },
            Some(stage.span()),
        )
    }

    fn invalid_list_item( &self, location: StageLocation, stage: &PlannerStage, item_index: usize, item: &str, message: impl Into<Arc<str>>, ) -> PlannerError {
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

    fn invalid_child_stage( &self, location: StageLocation, parent: &PlannerStage, child: &PlannerStage, message: impl Into<Arc<str>>, ) -> PlannerError {
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

    fn duplicate_directive( &self, location: StageLocation, parent: &PlannerStage, directive: &PlannerStage, ) -> PlannerError {
        PlannerError::new(
            PlannerErrorKind::DuplicateCompoundDirective {
                location,
                parent: Arc::from(parent.name().as_str()),
                directive: Arc::from(directive.name().as_str()),
            },
            Some(directive.span()),
        )
    }

    fn missing_directive( &self, location: StageLocation, parent: &PlannerStage, directive: &'static str, ) -> PlannerError {
        PlannerError::new(
            PlannerErrorKind::MissingCompoundDirective {
                location,
                parent: Arc::from(parent.name().as_str()),
                directive: Arc::from(directive),
            },
            Some(parent.span()),
        )
    }

    fn plan_error( &self, location: StageLocation, stage: &PlannerStage, error: impl fmt::Display, ) -> PlannerError {
        PlannerError::new(
            PlannerErrorKind::Plan {
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
    /// Preserve unknown simple stages as read-only custom operators.
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
    pub const fn index(self, depth: usize) -> Option<usize> {
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

            PlannerErrorKind::Plan { location, message } => match location {
                Some(location) => write!(
                    formatter,
                    "plan validation failed at pipeline location {location}: {message}",
                ),
                None => write!(formatter, "plan validation failed: {message}"),
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
    Plan {
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
    stages: Vec<PhysicalOperator>,
}

struct CompiledUnion<'a> {
    source: &'a str,
    alias: Option<&'a str>,
    stages: Vec<PhysicalOperator>,
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

const GROUP_MEASURE_MARKER_PREFIX: &str = "__og_group_";
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

pub(crate) fn group_key_marker(source: &str, alias: &str) -> Result<ExpressionFieldPath, String> {
    let marker = format!(
        "{GROUP_KEY_MARKER_PREFIX}{}_{}",
        encode_group_marker_text(source),
        encode_group_marker_text(alias),
    );
    parse_field_path(&marker)
}

/// Parses the arguments of a compound `group` measure directive.
///
/// `sum` keeps its historical default alias (the field's last segment); the
/// other measures default to `<aggregate>_<last segment>`, and a bare `count`
/// counts rows under the alias `count`.
fn parse_group_measure_directive<'a>(aggregate: &str, text: &'a str) -> Result<(&'a str, String), String> {
    let words = text.split_whitespace().collect::<Vec<_>>();
    if aggregate == "count" {
        match words.as_slice() {
            [] => return Ok(("", "count".to_owned())),
            ["as", alias] => return parse_single_identifier(alias).map(|alias| ("", alias.to_owned())),
            _ => {}
        }
    }
    let (source, alias) = match words.as_slice() {
        [source] => {
            let last = source.rsplit('.').next().unwrap_or(source);
            let alias = if aggregate == "sum" { last.to_owned() } else { format!("{aggregate}_{last}") };
            (*source, alias)
        }
        [source, "as", alias] => (*source, (*alias).to_owned()),
        _ => return Err(format!("expected `{aggregate} <field>` or `{aggregate} <field> as <alias>`")),
    };
    parse_field_path(source)?;
    parse_single_identifier(&alias)?;
    Ok((source, alias))
}

/// Encodes a measure as `__og_group_<aggregate>_<source hex>_<alias hex>`; an
/// empty source (row `count`) encodes as an empty hex run.
pub(crate) fn group_measure_marker(aggregate: &str, source: &str, alias: &str) -> Result<ExpressionFieldPath, String> {
    let marker = format!(
        "{GROUP_MEASURE_MARKER_PREFIX}{aggregate}_{}_{}",
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
        ["list"] => Ok(PivotAggregate::List),
        _ => Err(
            "expected one pivot aggregate: `sum`, `count`, `avg`, `average`, `min`, `minimum`, `max`, `maximum`, `first`, `last`, or `list`"
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
        .map(|_| ()).map_err(|error| error.to_string())
}

fn parse_load_mode(text: &str) -> Result<PhysicalLoadMode, String> {
    let words = text.split_whitespace().collect::<Vec<_>>();

    match words.as_slice() {
        ["replace"] => Ok(PhysicalLoadMode::Replace),
        ["update"] => Ok(PhysicalLoadMode::Update),
        ["merge"] => Ok(PhysicalLoadMode::Merge),
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

    if !crate::helpers::is_identifier_start(first) {
        return Err("identifier must start with an alphabetic character or '_'".to_owned());
    }

    for (index, character) in characters {
        if !crate::helpers::is_identifier_continue(character) {
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

        parse_single_identifier(segment).map_err(|message| format!("invalid field-path segment {index}: {message}"))?;
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


/// Prefix of the hidden fields computed for expression sort keys.
const HIDDEN_SORT_FIELD_PREFIX: &str = "__og_sort_";
/// Prefix of the hidden fields computed for expression pivot values.
const HIDDEN_PIVOT_FIELD_PREFIX: &str = "__og_pivot_";
/// Prefix of the hidden fields computed for `group` measures.
const HIDDEN_GROUP_FIELD_PREFIX: &str = "__og_measure_";
/// Aliases used by the one-line `lookup` when the query does not name them.
const LOOKUP_INNER_ALIAS: &str = "__og_joined";
const LOOKUP_OUTER_ALIAS: &str = "__og_outer";

/// `stage` with `children` as its body, after a `derive` of `derived` if any.
fn with_derived(stage: &PlannerStage, derived: &[String], children: Vec<PlannerStage>) -> Vec<(PlannerStage, Option<Arc<str>>)> {
    let Some(body) = stage.subpipeline() else {
        return vec![(stage.clone(), None)];
    };
    let rebuilt = PlannerStage::with_subpipeline(stage.name().clone(), stage.arguments(), PlannerSubPipeline::new(children, body.span()), stage.span());
    let derive = (!derived.is_empty()).then(|| (synthetic_stage("derive", &derived.join(", "), stage), None));
    derive.into_iter().chain([(rebuilt, None)]).collect()
}

/// A stage generated by a shorthand, reported at the shorthand's span.
fn synthetic_stage(name: &str, arguments: &str, origin: &PlannerStage) -> PlannerStage {
    let name = StageName::parse(name).expect("synthetic stage names are valid");
    PlannerStage::new(name, arguments, origin.span())
}

/// Converts `insert [{…}, …]` (keys may be unquoted) into a canonical JSON array.
fn insert_batch_json(text: &str) -> Result<String, String> {
    let crate::Value::Array(items) = super::plan_values::value_from_source(text).map_err(|error| error.to_string())? else {
        return Err("a batch insert expects an array of objects".to_owned());
    };
    if items.is_empty() {
        return Err("a batch insert needs at least one document".to_owned());
    }
    if !items.iter().all(|item| matches!(item, crate::Value::Object(_))) {
        return Err("a batch insert expects an array of objects".to_owned());
    }
    Ok(crate::helpers::value_to_json(&crate::Value::Array(items)).to_string())
}

/// Converts `as of <value>` / `history` into the storage temporal read.
fn source_temporal(source_text: &str, collection: &str, temporal: crate::query::ast::SourceTemporalAst) -> PlanningResult<crate::storage::Temporal> {
    use crate::query::ast::SourceTemporalAst;
    use crate::storage::{AsOf, CollectionId, Temporal};
    match temporal {
        SourceTemporalAst::History { span } => CollectionId::parse(collection)
            .map(|collection| Temporal::History { collection })
            .map_err(|error| PlannerError::invalid_ast(error.to_string(), Some(span))),
        SourceTemporalAst::AsOf { value, number, span } => {
            let text = value.slice(source_text).ok_or_else(|| PlannerError::invalid_ast("`as of` value span does not belong to the source text", Some(span)))?;
            if number {
                return text
                    .parse::<u64>()
                    .map(|generation| Temporal::AsOf(AsOf::Generation(generation)))
                    .map_err(|_| PlannerError::invalid_ast(format!("`as of` generation must be a non-negative integer, got {text}"), Some(value)));
            }
            let inner = text.get(1..text.len().saturating_sub(1)).unwrap_or("");
            let decoded = crate::query::syntax::unescape_string(inner).map_err(|escape| PlannerError::invalid_ast(format!("invalid escape \\{escape} in `as of` date"), Some(value)))?;
            crate::query::dates::parse_iso(&decoded)
                .map(|millis| Temporal::AsOf(AsOf::Time(millis)))
                .map_err(|message| PlannerError::invalid_ast(format!("`as of` {message}"), Some(value)))
        }
    }
}

/// Deepest lookup/union nesting below `operator` (1 for a flat stage).
fn nesting_depth(operator: &PhysicalOperator) -> usize {
    1 + operator.nested_pipeline().map_or(0, |pipeline| pipeline.operators().iter().map(nesting_depth).max().unwrap_or(0))
}

/// Maximum nesting of compound stages (lookup and union inside each other).
const MAX_COMPOUND_DEPTH: usize = 8;

/// The document identifier a predicate `_id == "<uuid>"` selects.
fn primary_key_equality(expression: &Expression) -> Option<DocumentId> {
    let (left, operator, right) = expression.ungrouped().as_binary()?;
    if operator != BinaryOperator::Equal {
        return None;
    }
    let pair = |field: &Expression, literal: &Expression| {
        let path = field.as_field()?;
        if path.len() != 1 || path.first() != "_id" {
            return None;
        }
        let Literal::String(value) = literal.as_literal()? else { return None };
        DocumentId::parse(value.as_ref()).ok()
    };
    pair(left.ungrouped(), right.ungrouped()).or_else(|| pair(right.ungrouped(), left.ungrouped()))
}

/// An extension stage with its execution properties: the one place where a
/// custom stage name maps to physical properties, for top-level and nested
/// pipelines alike.
fn custom_stage( name: StageName, arguments: impl AsRef<str> ) -> PlanningResult<PhysicalOperator> {
    PhysicalOperator::custom(name, arguments)
        .map_err(|error| PlannerError::new(PlannerErrorKind::Plan { location: None, message: Arc::from(error.to_string()) }, None))
}

/// The operator of a built-in stage run through its compiled form.
fn native_stage( stage: Stage, arguments: impl AsRef<str> ) -> PlanningResult<PhysicalOperator> {
    custom_stage(StageName::parse(stage.name()).expect("catalogue stage names are valid"), arguments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{parse, parse_expression, PhysicalOperatorKind};

    /// The plan questions the tests ask, by operator kind.
    trait PlanView {
        fn operator(&self, index: usize) -> Option<&PhysicalOperator>;
        fn has(&self, kind: PhysicalOperatorKind) -> bool;
        fn has_filter(&self) -> bool { self.has(PhysicalOperatorKind::Filter) }
        fn has_set(&self) -> bool { self.has(PhysicalOperatorKind::Set) }
        fn has_load(&self) -> bool { self.has(PhysicalOperatorKind::Load) || self.has(PhysicalOperatorKind::StreamingLoad) }
        fn has_limit(&self) -> bool { self.has(PhysicalOperatorKind::Limit) }
        fn has_skip(&self) -> bool { self.has(PhysicalOperatorKind::Skip) }
        fn has_sort(&self) -> bool { self.has(PhysicalOperatorKind::Sort) }
        fn has_select(&self) -> bool { self.has(PhysicalOperatorKind::Select) }
        fn has_distinct(&self) -> bool { self.has(PhysicalOperatorKind::Distinct) }
        fn has_count(&self) -> bool { self.has(PhysicalOperatorKind::Count) }
        fn has_delete(&self) -> bool { self.has(PhysicalOperatorKind::Delete) }
        fn has_insert(&self) -> bool { self.has(PhysicalOperatorKind::Insert) }
        fn has_group(&self) -> bool { self.has(PhysicalOperatorKind::Group) }
        fn has_pivot(&self) -> bool { self.has(PhysicalOperatorKind::Pivot) }
        fn is_mutating(&self) -> bool;
        fn is_read_only(&self) -> bool { !self.is_mutating() }
    }

    impl PlanView for PhysicalPlan {
        fn operator(&self, index: usize) -> Option<&PhysicalOperator> { self.operators().get(index) }
        fn has(&self, kind: PhysicalOperatorKind) -> bool { self.operators().iter().any(|operator| operator.kind() == kind) }
        fn is_mutating(&self) -> bool { self.is_write() }
    }

    const GROUP_SUM_MARKER_PREFIX: &str = "__og_group_sum_";

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

    fn plan_one(name: &str, arguments: &str) -> PhysicalPlan {
        Planner::new()
            .plan(&PlannerPipeline::new("users", [stage(name, arguments)]))
            .unwrap()
    }

    #[test] fn plans_all_simple_native_stages() { assert!(plan_one("where", "age >= 18").has_filter()); let near = plan_one("near", "embedding, [1.0, 0.0]"); assert!(matches!( near.operator(0), Some(PhysicalOperator::Custom { name: stage, arguments, writes: false, .. }) if stage.as_str() == "near" && arguments.as_ref() == "embedding, [1.0, 0.0]" )); let root = plan_one("root", "payload"); assert!(matches!( root.operator(0), Some(PhysicalOperator::Custom { name: stage, arguments, writes: false, .. }) if stage.as_str() == "root" && arguments.as_ref() == "payload" )); let sample = plan_one("sample", "5"); assert!(matches!( sample.operator(0), Some(PhysicalOperator::Custom { name: stage, arguments, writes: false, .. }) if stage.as_str() == "sample" && arguments.as_ref() == "5" )); assert!(plan_one("set", "enabled=true").has_set()); assert!(plan_one("load", "profile").has_load()); assert!(plan_one("limit", "10").has_limit()); assert!(plan_one("skip", "20").has_skip()); assert!(plan_one("sort", "age desc, name asc").has_sort()); assert!(plan_one("select", "name, profile.country").has_select()); let rename = plan_one("rename", "name as display_name"); let rename = rename .operator(0) .expect("rename should produce one logical operator"); assert!(matches!( rename, PhysicalOperator::Custom { name: stage, arguments, writes: false, .. } if stage.as_str() == "rename" && arguments.as_ref() == "name as display_name" )); let drop = plan_one("drop", "age, profile.secret"); let drop = drop .operator(0) .expect("drop should produce one logical operator"); assert!(matches!( drop, PhysicalOperator::Custom { name: stage, arguments, writes: false, .. } if stage.as_str() == "drop" && arguments.as_ref() == "age, profile.secret" )); assert!(plan_one("distinct", "email").has_distinct()); assert!(plan_one("distinct", "").has_distinct()); assert!(plan_one("count", "as total").has_count()); assert!(plan_one("delete", "").has_delete()); assert!(plan_one("insert", "{ name: \"Alice\" }").has_insert()); assert!(plan_one("group", "country, city").has_group()); }
    #[test] fn plans_set_with_structured_json_literals() { let plan = plan_one( "set", r#"definition = {"views":{"tile":[{"type":"metric","value":42}]}}, permissions = ["read","write"]"#, ); let assignments = plan .operator(0) .and_then(PhysicalOperator::assignments) .expect("set should expose assignments"); assert_eq!(assignments.len(), 2); assert!(matches!( assignments[0].value().as_literal(), Some(Literal::Json(value)) if value.starts_with('{') )); assert!(matches!( assignments[1].value().as_literal(), Some(Literal::Json(value)) if value.starts_with('[') )); }
    #[test] fn plans_compact_group_with_alias_on_grouping_key_and_measures() { let plan = plan_one( "group", "Receptionnaire_Code as Client, Article_Code as Produit, CAFacture as CA", ); let operator = plan.operator(0).expect("group operator"); let fields = operator.group_keys().expect("group fields"); assert_eq!(fields.len(), 3); assert!(fields[0].to_string().starts_with(GROUP_KEY_MARKER_PREFIX)); assert!(fields[1].to_string().starts_with(GROUP_KEY_MARKER_PREFIX)); assert!(fields[2].to_string().starts_with(GROUP_SUM_MARKER_PREFIX)); }
    #[test] fn plans_compound_group_with_aliases_on_by_keys() { let group = compound( "group", "", vec![ stage("by", "customer.country as Country, customer.city as City"), stage("sum", "CAFacture as Total_CA"), ], ); let plan = Planner::new() .plan(&PlannerPipeline::new("data", [group])) .unwrap(); let fields = plan .operator(0) .and_then(PhysicalOperator::group_keys) .expect("group fields"); assert_eq!(fields.len(), 3); assert!(fields[0].to_string().starts_with(GROUP_KEY_MARKER_PREFIX)); assert!(fields[1].to_string().starts_with(GROUP_KEY_MARKER_PREFIX)); assert!(fields[2].to_string().starts_with(GROUP_SUM_MARKER_PREFIX)); }
    #[test] fn compact_group_uses_all_but_last_item_as_dimensions() { let plan = plan_one("group", "x, y, z"); let fields = plan .operator(0) .and_then(PhysicalOperator::group_keys) .expect("group fields"); assert_eq!(fields.len(), 3); assert_eq!(fields[0].to_string(), "x"); assert_eq!(fields[1].to_string(), "y"); assert!(fields[2].to_string().starts_with(GROUP_SUM_MARKER_PREFIX)); }
    #[test] fn compact_group_with_one_item_remains_a_pure_dimension() { let plan = plan_one("group", "x"); let fields = plan .operator(0) .and_then(PhysicalOperator::group_keys) .expect("group fields"); assert_eq!(fields.len(), 1); assert_eq!(fields[0].to_string(), "x"); }
    #[test] fn plans_compact_group_with_automatic_sum_measure() { let plan = plan_one("group", "Article_Code, CAFacture as CA"); let operator = plan.operator(0).expect("group operator"); let fields = operator.group_keys().expect("group fields"); assert_eq!(fields.len(), 2); assert_eq!(fields[0].to_string(), "Article_Code"); assert!(fields[1].to_string().starts_with(GROUP_SUM_MARKER_PREFIX)); }
    #[test] fn plans_compound_group_with_explicit_sum() { let group = compound( "group", "", vec![ stage("by", "Article_Code"), stage("sum", "CAFacture as Total_CA"), ], ); let plan = Planner::new() .plan(&PlannerPipeline::new("data", [group])) .unwrap(); let fields = plan .operator(0) .and_then(PhysicalOperator::group_keys) .expect("group fields"); assert_eq!(fields.len(), 2); assert_eq!(fields[0].to_string(), "Article_Code"); assert!(fields[1].to_string().starts_with(GROUP_SUM_MARKER_PREFIX)); }
    #[test] fn compound_group_requires_by_directive() { let group = compound("group", "", vec![stage("sum", "CAFacture as Total_CA")]); let error = Planner::new() .plan(&PlannerPipeline::new("data", [group])) .unwrap_err(); assert!(error.to_string().contains("requires directive \"by\"")); }
    #[test] fn group_can_be_followed_by_sort() { let plan = Planner::new() .plan(&PlannerPipeline::new( "data", [stage("group", "Article_Code"), stage("sort", "count desc")], )) .unwrap(); assert_eq!(plan.len(), 2); assert!(plan.has_group()); assert!(plan.has_sort()); }
    #[test] fn aggregated_group_can_be_followed_by_sort() { let plan = Planner::new() .plan(&PlannerPipeline::new( "data", [ stage("group", "Article_Code, CAFacture as CA"), stage("sort", "CA desc"), ], )) .unwrap(); assert_eq!(plan.len(), 2); assert!(plan.has_group()); assert!(plan.has_sort()); }
    #[test] fn group_can_be_followed_by_select_and_limit() { let plan = Planner::new() .plan(&PlannerPipeline::new( "data", [ stage("group", "Article_Code"), stage("select", "Article_Code, count"), stage("limit", "10"), ], )) .unwrap(); assert_eq!(plan.len(), 3); assert!(plan.has_group()); assert!(plan.has_select()); assert!(plan.has_limit()); }
    #[test] fn plans_offset_as_skip() { let plan = Planner::new() .plan(&PlannerPipeline::new("data", [stage("offset", "2")])) .unwrap(); assert!(matches!( plan.operator(0), Some(PhysicalOperator::Skip { count: 2 }) )); }
    #[test] fn plans_first_single_and_unwind() { for (name, args) in [ ("first", ""), ("first", "Article_Code"), ("single", ""), ("single", "Article_Code"), ("unwind", "data"), ] { let plan = Planner::new() .plan(&PlannerPipeline::new("data", [stage(name, args)])) .unwrap(); assert!( matches!(plan.operator(0), Some(PhysicalOperator::Custom { name: stage, .. }) if stage.as_str() == name) ); } }
    #[test] fn validates_rename_syntax() { let error = Planner::new() .plan(&PlannerPipeline::new("users", [stage("rename", "name")])) .unwrap_err(); assert!(error .to_string() .contains("expected `<source> as <target>`")); let error = Planner::new() .plan(&PlannerPipeline::new( "users", [stage("rename", "name as name")], )) .unwrap_err(); assert!(error.to_string().contains("must be different")); }
    #[test] fn validates_drop_field_list() { let error = Planner::new() .plan(&PlannerPipeline::new("users", [stage("drop", "")])) .unwrap_err(); assert!(error.to_string().contains("requires arguments")); }
    #[test] fn plans_insert_with_nested_object_value() { let specification = r#"{ _id: "u1", name: "John", active: true, tags: ["rust", "database"], address: { city: "Paris", }, }"#; let plan = plan_one("insert", specification); assert!(plan.has_insert()); assert!(!plan.is_read_only()); }
    #[test] fn plans_insert_ast_directly() { let source = r#"from users | insert { _id: "u1", name: "John", tags: ["rust", "database"], }"#; let ast = parse(source).unwrap(); let plan = Planner::new().plan_ast(source, &ast).unwrap(); assert!(plan.has_insert()); }
    #[test] fn rejects_insert_scalar_value() { let error = Planner::new() .plan(&PlannerPipeline::new( "users", [stage("insert", r#""John""#)], )) .unwrap_err(); assert!(matches!(error.kind(), PlannerErrorKind::Plan { .. })); assert!(error .to_string() .contains("insert document must be an object")); }
    #[test] fn plans_insert_array_as_a_batch_load() { let plan = Planner::new() .plan(&PlannerPipeline::new( "users", [stage("insert", r#"[{name: "John", n: -2}]"#)], )) .unwrap(); assert!(matches!(plan.operator(0), Some(PhysicalOperator::StreamingLoad { mode: PhysicalLoadMode::Merge, chunks }) if chunks.len() == 1 && chunks[0].as_ref() == r#"[{"n":-2,"name":"John"}]"#)); let error = Planner::new() .plan(&PlannerPipeline::new("users", [stage("insert", "[1]")])) .unwrap_err(); assert!(error.to_string().contains("array of objects"), "{error}"); }
    #[test] fn rejects_malformed_insert_object() { let error = Planner::new() .plan(&PlannerPipeline::new( "users", [stage("insert", r#"{name "John"}"#)], )) .unwrap_err(); assert!(matches!(error.kind(), PlannerErrorKind::Plan { .. })); assert!(error .to_string() .contains("invalid insert document")); assert!(error.to_string().contains("expected `:` after object key")); }
    #[test] fn rejects_multiple_insert_values() { let error = Planner::new() .plan(&PlannerPipeline::new( "users", [stage("insert", r#"{name: "John"} {name: "Jane"}"#)], )) .unwrap_err(); assert!(matches!(error.kind(), PlannerErrorKind::Plan { .. })); assert!(error.to_string().contains("unexpected token")); }
    #[test] fn plans_typed_pivot() { let pivot = compound( "pivot", "", vec![ stage("rows", "region"), stage("columns", "month"), stage("values", "revenue"), stage("aggregate", "sum"), ], ); let plan = Planner::new() .plan(&PlannerPipeline::new("sales", [pivot])) .unwrap(); assert!(plan.has_pivot()); let specification = plan .operator(0) .and_then(PhysicalOperator::pivot_specification) .expect("typed pivot specification"); assert_eq!(specification.rows().len(), 1); assert_eq!(specification.columns().len(), 1); assert_eq!(specification.values().len(), 1); assert_eq!(specification.values()[0].aggregate(), PivotAggregate::Sum); }
    #[test] fn rejects_incomplete_or_duplicate_pivot_directives() { let incomplete = compound( "pivot", "", vec![stage("rows", "region"), stage("columns", "month")], ); let error = Planner::new() .plan(&PlannerPipeline::new("sales", [incomplete])) .unwrap_err(); assert!(matches!( error.kind(), PlannerErrorKind::MissingCompoundDirective { directive, .. } if directive.as_ref() == "values" )); let duplicate = compound( "pivot", "", vec![ stage("rows", "region"), stage("rows", "country"), stage("columns", "month"), stage("values", "revenue"), stage("aggregate", "sum"), ], ); let error = Planner::new() .plan(&PlannerPipeline::new("sales", [duplicate])) .unwrap_err(); assert!(matches!( error.kind(), PlannerErrorKind::DuplicateCompoundDirective { directive, .. } if directive.as_ref() == "rows" )); }
    #[test] fn plans_pivot_ast_directly() { let source = r#"on sales | pivot | rows region | columns month | values revenue | aggregate sum | end"#; let ast = parse(source).unwrap(); let plan = Planner::new().plan_ast(source, &ast).unwrap(); assert!(plan.has_pivot()); }
    #[test] fn normalizes_ast_with_source_alias() { let source = "on users as u | where active == true"; let ast = parse(source).unwrap(); let normalized = PlannerPipeline::from_ast(source, &ast).unwrap(); assert_eq!(normalized.source(), "users"); assert_eq!(normalized.source_alias(), Some("u")); assert_eq!(normalized.len(), 1); assert_eq!(normalized.stages()[0].name().as_str(), "where"); }
    #[test] fn normalizes_project_alias_to_select() { let source = "on users | project name, profile.country"; let ast = parse(source).unwrap(); let normalized = PlannerPipeline::from_ast(source, &ast).unwrap(); assert_eq!(normalized.len(), 1); assert_eq!(normalized.stages()[0].name().as_str(), "select"); assert_eq!(normalized.stages()[0].arguments(), "name, profile.country"); let plan = Planner::new().plan(&normalized).unwrap(); assert!(plan.has_select()); }
    #[test] fn plans_ast_directly() { let source = "on users | where active == true"; let ast = parse(source).unwrap(); let plan = Planner::new().plan_ast(source, &ast).unwrap(); assert!(plan.has_filter()); }
    #[test] fn plans_lookup_compound_stage() { let lookup = compound( "lookup", "workspace as w", vec![stage("where", "w.public == true"), stage("into", "public")], ); let plan = Planner::new() .plan(&PlannerPipeline::new("users", [lookup])) .unwrap(); let operator = plan.operator(0).unwrap(); assert!(matches!( operator, PhysicalOperator::Lookup { collection, alias: Some(alias), into, pipeline, .. } if collection.as_str() == "workspace" && alias.as_ref() == "w" && into.as_ref() == "public" && matches!(pipeline.operators(), [PhysicalOperator::Filter { .. }]) )); }
    #[test] fn plans_join_as_normalized_lookup() { let join = compound( "join", "workspace as w", vec![stage("where", "w.public == true"), stage("into", "public")], ); let plan = Planner::new() .plan(&PlannerPipeline::new("users", [join])) .unwrap(); assert!(matches!( plan.operator(0).unwrap(), PhysicalOperator::Lookup { .. } )); }
    #[test] fn plans_union_compound_stage() { let union = compound( "union", "", vec![ stage("on", "archived_users"), stage("where", "active == true"), ], ); let plan = Planner::new() .plan(&PlannerPipeline::new("users", [union])) .unwrap(); assert!(matches!( plan.operator(0).unwrap(), PhysicalOperator::Union { collection, alias: None, pipeline } if collection.as_str() == "archived_users" && pipeline.len() == 1 )); }
    #[test] fn plans_streaming_load() { let load = compound( "load", "", vec![ stage("with", "replace"), stage("chunk", "batch1"), stage("chunk", "batch2"), ], ); let plan = Planner::new() .plan(&PlannerPipeline::new("users", [load])) .unwrap(); assert!(plan.has_load()); assert!(plan.is_mutating()); assert!(matches!( plan.operator(0).unwrap(), PhysicalOperator::StreamingLoad { mode: PhysicalLoadMode::Replace, chunks } if chunks.len() == 2 )); }
    #[test] fn validates_lookup_directives() { let lookup = compound( "lookup", "workspace", vec![stage("where", "public == true")], ); let error = Planner::new() .plan(&PlannerPipeline::new("users", [lookup])) .unwrap_err(); assert!(matches!( error.kind(), PlannerErrorKind::MissingCompoundDirective { directive, .. } if directive.as_ref() == "into" )); }
    #[test] fn validates_union_source_is_first() { let union = compound( "union", "", vec![ stage("where", "active == true"), stage("on", "archived_users"), ], ); let error = Planner::new() .plan(&PlannerPipeline::new("users", [union])) .unwrap_err(); assert!(matches!( error.kind(), PlannerErrorKind::InvalidUnionSource { .. } )); }
    #[test] fn validates_streaming_load_directives() { let load = compound("load", "", vec![stage("with", "replace")]); let error = Planner::new() .plan(&PlannerPipeline::new("users", [load])) .unwrap_err(); assert!(matches!( error.kind(), PlannerErrorKind::MissingLoadChunk { .. } )); }
    #[test] fn rejects_mutating_stage_inside_lookup() { let lookup = compound( "lookup", "workspace", vec![stage("delete", ""), stage("into", "public")], ); let error = Planner::new() .plan(&PlannerPipeline::new("users", [lookup])) .unwrap_err(); assert!(matches!( error.kind(), PlannerErrorKind::InvalidCompoundChild { .. } )); }
    #[test] fn plans_combined_read_pipeline() { let pipeline = PlannerPipeline::new( "users", [ stage("where", "active == true"), stage("sort", "age desc, name"), stage("skip", "10"), stage("limit", "20"), stage("select", "name, age"), stage("distinct", "name"), ], ); let plan = Planner::new().plan(&pipeline).unwrap(); assert_eq!(plan.len(), 6); assert!(plan.is_read_only()); }
    #[test] fn select_accepts_mixed_plain_and_aliased_fields() { let plan = plan_one("select", "CAFacture as CA, COGS"); let operator = plan .operator(0) .expect("select should produce one operator"); assert!(matches!( operator, PhysicalOperator::Custom { name: stage, arguments, writes: false, .. } if stage.as_str() == "select" && arguments.as_ref() == "CAFacture as CA, COGS" )); }
    #[test] fn select_accepts_expression_projection_with_alias() { let plan = plan_one("select", "CAFacture as CA, COGS, CA - COGS as Marge"); let operator = plan .operator(0) .expect("select should produce one operator"); assert!(matches!( operator, PhysicalOperator::Custom { name: stage, arguments, writes: false, .. } if stage.as_str() == "select" && arguments.as_ref() == "CAFacture as CA, COGS, CA - COGS as Marge" )); }
    #[test] fn select_requires_alias_for_expression_projection() { let error = Planner::new() .plan(&PlannerPipeline::new( "users", [stage("select", "price * quantity")], )) .unwrap_err(); assert!(error .to_string() .contains("expression projection requires an alias")); }
    #[test] fn validates_native_stage_arguments() { for name in [ "where", "near", "root", "sample", "set", "load", "limit", "skip", "sort", "select", "insert", "group", ] { let error = Planner::new() .plan(&PlannerPipeline::new("users", [stage(name, "")])) .unwrap_err(); assert!(matches!( error.kind(), PlannerErrorKind::MissingStageArguments { .. } )); } assert!(matches!( Planner::new() .plan(&PlannerPipeline::new("users", [stage("delete", "now")])) .unwrap_err() .kind(), PlannerErrorKind::UnexpectedStageArguments { .. } )); }
    #[test] fn validates_limit_skip_sort_and_count_syntax() { for (name, arguments) in [ ("limit", "-1"), ("limit", "1 2"), ("skip", "one"), ("sample", "-1"), ("sample", "one"), ("sort", "age sideways"), ("count", "total"), ("count", "as total extra"), ] { assert!(matches!( Planner::new() .plan(&PlannerPipeline::new("users", [stage(name, arguments)])) .unwrap_err() .kind(), PlannerErrorKind::InvalidStageSyntax { .. } | PlannerErrorKind::InvalidStageItem { .. } )); } }
    #[test] fn preserves_commas_and_equals_inside_nested_text() { let values = split_top_level("label=\"a,b\", enabled=true", ',').unwrap(); assert_eq!(values, vec!["label=\"a,b\"", "enabled=true"]); let (field, expression) = split_assignment("enabled=status == true").unwrap(); assert_eq!(field, "enabled"); assert_eq!(expression, "status == true"); }
    #[test] fn rejects_unknown_stage_by_default() { let error = Planner::new() .plan(&PlannerPipeline::new( "users", [stage("inspect", "verbose")], )) .unwrap_err(); assert!(matches!( error.kind(), PlannerErrorKind::UnknownStage { .. } )); }
    #[test] fn optionally_preserves_simple_custom_stage() { let planner = Planner::with_options(PlannerOptions::new(true)); let plan = planner .plan(&PlannerPipeline::new( "users", [stage("inspect", "verbose")], )) .unwrap(); assert_eq!(plan.len(), 1); }
    #[test] fn rejects_custom_compound_stage() { let planner = Planner::with_options(PlannerOptions::new(true)); let error = planner .plan(&PlannerPipeline::new( "users", [compound("inspect", "", vec![stage("where", "true")])], )) .unwrap_err(); assert!(matches!( error.kind(), PlannerErrorKind::CompoundCustomStage { .. } )); }
    #[test] fn stage_locations_are_recursive() { let location = StageLocation::root(2).child(4).child(1); assert_eq!(location.to_string(), "2.4.1"); assert_eq!(location.depth(), 3); assert_eq!(location.index(0), Some(2)); assert_eq!(location.index(2), Some(1)); assert_eq!(location.index(3), None); }
    #[test] fn expression_parser_is_still_used_for_where() { let expression = parse_expression("age >= 18").unwrap(); assert_eq!( plan_one("where", "age >= 18") .operator(0) .unwrap() .predicate(), Some(&expression), ); }
    #[test] fn compound_group_plans_keys_and_several_measures() { let source = "on orders | group | by region as zone, month | sum qty as units | sum price | end"; let ast = crate::query::parse(source).unwrap(); let plan = Planner::new().plan_ast(source, &ast).unwrap(); assert_eq!(plan.operator(0).and_then(PhysicalOperator::group_keys).map(<[ExpressionFieldPath]>::len), Some(4)); }
}
