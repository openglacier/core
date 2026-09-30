#![cfg_attr(rustfmt, rustfmt_skip)]
//! Logical-to-physical plan lowering.

use std::{error::Error as StdError, fmt, sync::Arc};

use crate::{
    engine::PlanLowerer,
    storage::{CollectionId, DocumentId},
};

use super::{
    BinaryOperator, Expression, Literal, PhysicalAccess, PhysicalOperator, PhysicalPlan,
    PhysicalPlanError, PhysicalPlanErrorKind, PhysicalPlanner, PhysicalSubPipeline, StageName,
};

use super::logical_plan::{LogicalOperator, LogicalPlan};

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
    pub fn lower_plan_detailed( &self, logical: &LogicalPlan, physical_planner: &PhysicalPlanner, ) -> LoweringResult<PhysicalPlan> {
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
    pub fn lower_plan( &self, logical: &LogicalPlan, physical_planner: &PhysicalPlanner, ) -> Result<PhysicalPlan, PhysicalPlanError> {
        self.lower_plan_detailed(logical, physical_planner)
            .map_err(Into::into)
    }
}

impl PlanLowerer for ScanPlanLowerer {
    fn lower( &self, logical: &LogicalPlan, physical_planner: &PhysicalPlanner, ) -> Result<PhysicalPlan, PhysicalPlanError> {
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

    const fn with_operator_index(mut self, operator_index: usize) -> Self {
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
    NestingLimitExceeded {
        limit: usize,
    },
    PhysicalPlan {
        message: Arc<str>,
    },
}

impl From<LoweringError> for PhysicalPlanError {
    fn from(error: LoweringError) -> Self {
        Self::new(PhysicalPlanErrorKind::InvalidCustomArguments {
            stage: Arc::from(format!("lowering: {error}")),
        })
    }
}

fn collection_id(logical: &LogicalPlan) -> LoweringResult<CollectionId> {
    parse_collection_id(&logical.source().collection_name().to_string())
}

fn lower_operator(operator: &LogicalOperator, depth: usize) -> LoweringResult<PhysicalOperator> {
    match operator {
        LogicalOperator::Filter { predicate } => Ok(PhysicalOperator::filter(predicate.clone())),

        LogicalOperator::Set { assignments } => PhysicalOperator::set(assignments.iter().cloned())
            .map_err(|error| LoweringError::physical(None, error)),

        LogicalOperator::Load { specification } => PhysicalOperator::load(specification.as_ref())
            .map_err(|error| LoweringError::physical(None, error)),

        LogicalOperator::StreamingLoad { mode, chunks } => PhysicalOperator::streaming_load(*mode, chunks.iter())
            .map_err(|error| LoweringError::physical(None, error)),

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

        LogicalOperator::Lookup { collection, alias, outer_alias, into, pipeline } => {
            let pipeline = lower_subpipeline(pipeline, depth + 1)?;
            PhysicalOperator::lookup(parse_collection_id(collection)?, alias.as_deref(), into.as_ref(), pipeline)
                .map(|operator| operator.with_outer_alias(outer_alias.as_deref()))
                .map_err(|error| LoweringError::physical(None, error))
        }

        LogicalOperator::Union { collection, alias, pipeline } => {
            let pipeline = lower_subpipeline(pipeline, depth + 1)?;
            PhysicalOperator::union(parse_collection_id(collection)?, alias.as_deref(), pipeline)
                .map_err(|error| LoweringError::physical(None, error))
        }

        LogicalOperator::Custom {
            stage,
            arguments,
            mutating,
        } => lower_custom_stage(stage.clone(), arguments.as_ref(), *mutating),
    }
}

/// Lowers an extension stage and declares its execution properties.
///
/// This is the single place where a custom stage name is mapped to physical
/// properties; top-level and nested pipelines share it, and executors only
/// consume the resulting properties.
fn lower_custom_stage( name: StageName, arguments: &str, mutating: bool, ) -> LoweringResult<PhysicalOperator> {
    let operator = match name.as_str() {
        // A random subset needs the whole input before emitting.
        // A uniform random subset: blocking, but retains at most `n` rows.
        "sample" => PhysicalOperator::custom(name, arguments, false, true)
            .map(|operator| match arguments.trim().parse::<usize>() {
                Ok(count) => operator.with_retained_rows(count),
                Err(_) => operator,
            }),
        // At most one row, in input order; without a projection path the row is unchanged.
        "first" => {
            let keeps_fields = arguments.trim().is_empty();
            PhysicalOperator::custom(name, arguments, false, false)
                .map(|operator| operator.with_row_bound(1, keeps_fields))
        }
        // Exactly one row: the same bound as `first`, declared exact.
        "single" => {
            let keeps_fields = arguments.trim().is_empty();
            PhysicalOperator::custom(name, arguments, false, false)
                .map(|operator| operator.with_exact_rows(1, keeps_fields))
        }
        // One row per array item, in order: streams mid-chain.
        "unwind" => PhysicalOperator::custom(name, arguments, false, false)
            .map(PhysicalOperator::with_row_expansion),
        // Drops rows without the vector field: a streaming filter.
        "near" => PhysicalOperator::custom(name, arguments, false, false)
            .map(PhysicalOperator::with_row_filter),
        _ => PhysicalOperator::custom(name, arguments, mutating, false),
    };
    operator.map_err(|error| LoweringError::physical(None, error))
}

/// Lowers the read-only pipeline of a lookup or union, one nesting level deeper.
fn lower_subpipeline(operators: &[LogicalOperator], depth: usize) -> LoweringResult<PhysicalSubPipeline> {
    if depth > MAX_COMPOUND_DEPTH {
        return Err(LoweringError::new(
            LoweringErrorKind::NestingLimitExceeded {
                limit: MAX_COMPOUND_DEPTH,
            },
        ));
    }
    let operators = operators
        .iter()
        .map(|operator| lower_operator(operator, depth))
        .collect::<LoweringResult<Vec<_>>>()?;
    PhysicalSubPipeline::new(operators).map_err(|error| LoweringError::physical(None, error))
}

fn parse_collection_id(collection: &str) -> LoweringResult<CollectionId> {
    CollectionId::parse(collection).map_err(|error| {
        LoweringError::new(LoweringErrorKind::InvalidCollection {
            collection: Arc::from(collection),
            message: Arc::from(error.to_string()),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::query::{
        parse_expression, ExpressionFieldPath, LogicalSource, PhysicalLoadMode, SetAssignment, SortDirection,
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

    fn lookup(collection: &str, alias: Option<&str>, into: &str, pipeline: Vec<LogicalOperator>) -> LogicalOperator {
        LogicalOperator::Lookup { collection: Arc::from(collection), alias: alias.map(Arc::from), outer_alias: None, into: Arc::from(into), pipeline: Arc::from(pipeline) }
    }

    fn union(collection: &str, alias: Option<&str>, pipeline: Vec<LogicalOperator>) -> LogicalOperator {
        LogicalOperator::Union { collection: Arc::from(collection), alias: alias.map(Arc::from), pipeline: Arc::from(pipeline) }
    }

    fn streaming_load(chunks: &[&str]) -> LogicalOperator {
        LogicalOperator::StreamingLoad { mode: PhysicalLoadMode::Replace, chunks: chunks.iter().map(|chunk| Arc::from(*chunk)).collect() }
    }

    #[test] fn lowers_scan_only_plan() { let logical = LogicalPlan::scan(source()); let physical = lower(&logical); assert!(physical.is_scan_only()); assert_eq!(physical.source().collection().as_str(), "users"); assert!(!physical.is_write()); }
    #[test] fn lowers_complete_read_pipeline_in_order() { let logical = LogicalPlan::builder(source()) .filter(parse_expression("active == true").unwrap()) .unwrap() .sort([ SortKey::new(field(&["age"]), SortDirection::Descending), SortKey::new(field(&["name"]), SortDirection::Ascending), ]) .unwrap() .skip(10) .unwrap() .limit(20) .unwrap() .select([field(&["name"]), field(&["age"])]) .unwrap() .distinct([field(&["name"])]) .unwrap() .finish() .unwrap(); let physical = lower(&logical); let names = physical .operators() .iter() .map(PhysicalOperator::name) .collect::<Vec<_>>(); assert_eq!( names, ["filter", "sort", "skip", "limit", "select", "distinct"], ); assert!(!physical.is_write()); assert!(physical.changes_cardinality()); }
    #[test] fn preserves_filter_payload() { let logical = LogicalPlan::builder(source()) .filter(parse_expression("age >= 18").unwrap()) .unwrap() .finish() .unwrap(); let physical = lower(&logical); assert_eq!( physical.operators()[0].predicate(), logical.operators().next().unwrap().predicate(), ); }
    #[test] fn preserves_set_assignments_and_write_mode() { let logical = LogicalPlan::builder(source()) .set([ assignment(&["enabled"], "true"), assignment(&["profile", "level"], "level + 1"), ]) .unwrap() .finish() .unwrap(); let physical = lower(&logical); assert_eq!(physical.operators()[0].name(), "set"); assert_eq!(physical.operators()[0].assignments().unwrap().len(), 2); assert!(physical.is_write()); }
    #[test] fn preserves_compact_load_target() { let logical = LogicalPlan::builder(source()) .load("profile") .unwrap() .finish() .unwrap(); let physical = lower(&logical); assert_eq!(physical.operators()[0].load_target(), Some("profile")); assert!(physical.is_write()); }
    #[test] fn lowers_streaming_load() { let logical = LogicalPlan::new(source(), [streaming_load(&["batch1", "batch2"])]).unwrap(); let physical = lower(&logical); let operator = &physical.operators()[0]; assert_eq!(operator.name(), "streaming-load"); assert_eq!( operator.streaming_load_mode(), Some(PhysicalLoadMode::Replace) ); assert_eq!(operator.streaming_load_chunks().unwrap().len(), 2); assert!(physical.is_write()); }
    #[test] fn lowers_lookup() { let logical = LogicalPlan::new(source(), [lookup("workspace", Some("w"), "public", vec![LogicalOperator::filter(parse_expression("w.public == true").unwrap()), LogicalOperator::limit(5)])]).unwrap(); let physical = lower(&logical); let operator = &physical.operators()[0]; assert_eq!(operator.name(), "lookup"); assert_eq!(operator.lookup_collection().unwrap().as_str(), "workspace"); assert_eq!(operator.lookup_alias(), Some("w")); assert_eq!(operator.lookup_target(), Some("public")); assert_eq!(operator.nested_pipeline().unwrap().len(), 2); assert!(!operator.execution_properties().writes()); }
    #[test] fn lowers_union() { let logical = LogicalPlan::new(source(), [union("archived_users", None, vec![LogicalOperator::filter(parse_expression("active == true").unwrap()), LogicalOperator::select([field(&["name"]), field(&["age"])]).unwrap()])]).unwrap(); let physical = lower(&logical); let operator = &physical.operators()[0]; assert_eq!(operator.name(), "union"); assert_eq!( operator.union_collection().unwrap().as_str(), "archived_users" ); assert_eq!(operator.union_alias(), None); assert_eq!(operator.nested_pipeline().unwrap().len(), 2); assert!(!matches!( operator.execution_properties().cardinality, crate::query::CardinalityEffect::Preserve )); }
    #[test] fn lowers_nested_lookup_inside_union() { let nested_lookup = lookup("workspace", Some("w"), "spaces", vec![LogicalOperator::filter(parse_expression("w.public == true").unwrap())]); let logical = LogicalPlan::new(source(), [union("archived_users", Some("a"), vec![nested_lookup])]).unwrap(); let physical = lower(&logical); let nested = physical.operators()[0] .nested_pipeline() .unwrap() .operators(); assert_eq!(nested.len(), 1); assert_eq!(nested[0].name(), "lookup"); assert_eq!(nested[0].lookup_alias(), Some("w")); }
    #[test] fn empty_streaming_load_is_rejected() { let logical = LogicalPlan::new(source(), [streaming_load(&[])]).unwrap(); let error = ScanPlanLowerer::new() .lower_plan_detailed(&logical, &PhysicalPlanner::new()) .unwrap_err(); assert_eq!(error.operator_index(), Some(0)); assert!(matches!(error.kind(), LoweringErrorKind::PhysicalPlan { .. })); }
    #[test] fn lookups_nested_beyond_the_limit_are_rejected() { let mut operator = lookup("workspace", None, "w", Vec::new()); for _ in 0..MAX_COMPOUND_DEPTH { operator = lookup("workspace", None, "w", vec![operator]); } let logical = LogicalPlan::new(source(), [operator]).unwrap(); let error = ScanPlanLowerer::new() .lower_plan_detailed(&logical, &PhysicalPlanner::new()) .unwrap_err(); assert!(matches!(error.kind(), LoweringErrorKind::NestingLimitExceeded { .. })); }
    #[test] fn preserves_limit_and_skip_counts() { let logical = LogicalPlan::builder(source()) .skip(7) .unwrap() .limit(11) .unwrap() .finish() .unwrap(); let physical = lower(&logical); assert_eq!(physical.operators()[0].row_count(), Some(7)); assert_eq!(physical.operators()[1].row_count(), Some(11)); }
    #[test] fn preserves_sort_keys() { let logical = LogicalPlan::builder(source()) .sort([ SortKey::descending(field(&["age"])), SortKey::ascending(field(&["name"])), ]) .unwrap() .finish() .unwrap(); let physical = lower(&logical); let keys = physical.operators()[0].sort_keys().unwrap(); assert_eq!(keys.len(), 2); assert_eq!(keys[0].field(), &field(&["age"])); assert_eq!(keys[0].direction(), SortDirection::Descending); assert_eq!(keys[1].field(), &field(&["name"])); assert_eq!(keys[1].direction(), SortDirection::Ascending); }
    #[test] fn preserves_select_and_distinct_fields() { let logical = LogicalPlan::builder(source()) .select([field(&["name"]), field(&["profile", "country"])]) .unwrap() .distinct([field(&["name"])]) .unwrap() .finish() .unwrap(); let physical = lower(&logical); assert_eq!( physical.operators()[0].selected_fields().unwrap(), &[field(&["name"]), field(&["profile", "country"])], ); assert_eq!( physical.operators()[1].distinct_fields().unwrap(), &[field(&["name"])], ); }
    #[test] fn preserves_count_alias() { let logical = LogicalPlan::builder(source()) .count("total") .unwrap() .finish() .unwrap(); let physical = lower(&logical); assert_eq!(physical.operators()[0].count_alias(), Some("total")); assert!(!physical.is_write()); }
    #[test] fn preserves_custom_operator_metadata() { let stage = StageName::parse("inspect").unwrap(); let logical = LogicalPlan::builder(source()) .custom(stage, "verbose", false) .unwrap() .finish() .unwrap(); let physical = lower(&logical); assert_eq!(physical.operators()[0].name(), "inspect"); assert!(!physical.is_write()); }
    #[test] fn preserves_typed_insert_document() { let logical = LogicalPlan::builder(source()) .insert(r#"{name:"Alice",active:true,tags:["rust"],profile:{level:2}}"#) .unwrap() .finish() .unwrap(); let expected = logical .operators() .next() .and_then(LogicalOperator::insert_document) .expect("logical insert document") .clone(); let physical = lower(&logical); assert_eq!(physical.operators()[0].insert_document(), Some(&expected),); assert!(physical.is_write()); }
    #[test] fn preserves_typed_pivot_specification() { use crate::query::logical_plan::{PivotAggregate, PivotSpecification, PivotValue}; let specification = PivotSpecification::new( [field(&["region"])], [field(&["month"])], [PivotValue::new( field(&["revenue"]), PivotAggregate::Sum, Option::<&str>::None, ) .unwrap()], ) .unwrap(); let logical = LogicalPlan::builder(source()) .pivot(specification.clone()) .unwrap() .finish() .unwrap(); let physical = lower(&logical); let operator = &physical.operators()[0]; assert_eq!(operator.name(), "pivot"); assert_eq!(operator.pivot_specification(), Some(&specification)); assert!(!operator.execution_properties().writes()); assert!(!matches!( operator.execution_properties().cardinality, crate::query::CardinalityEffect::Preserve )); }
}
