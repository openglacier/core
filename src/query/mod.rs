#![cfg_attr(rustfmt, rustfmt_skip)]
//! Query module exports.

mod ast;
mod lexer;
mod parser;
mod span;
mod token;

pub use ast::{
    EndAst, NameAst, PipelineAst, SourceAliasAst, SourceAst, SourceKeyword, SourceTemporalAst,
    Spanned, StageAst, SubPipelineAst,
};

pub use lexer::{lex, LexError, LexErrorKind, LexResult, Lexer, TokenStream};

pub use parser::{
    parse, parse_tokens, ParseError, ParseErrorKind, ParseResult, Parser, QueryParseError,
    QueryParseResult, SubPipelinePolicy,
};

pub use span::Span;
pub use token::{Token, TokenKind};

mod stage;

pub use stage::{
    Arguments, Placement, Stage, StageEntry, StageName, StageNameError, STAGE_CATALOG,
};

pub(crate) mod dates;
mod eval;
mod expression;
mod functions;
mod json_value;
mod params;
mod separators;

pub use eval::{
    apply_assignments, check_call, evaluate, evaluate_document, evaluate_predicate, DocumentFields,
    ExpressionFieldResolver, Function, LookupFields, LookupScope, SemanticValue,
};

pub use params::{
    bind_parameters, prepare_query, prepare_view, reads_place, split_explain, Sources,
};
pub(crate) use params::{group_length, stage_ranges};
pub use separators::{normalize, separated_by_lines};
pub(crate) use plan_values::value_from_source;

pub use expression::{
    parse_expression, BinaryOperator, Expression, ExpressionError, ExpressionErrorKind,
    ExpressionFieldPath, ExpressionKind, ExpressionParser, ExpressionResult, ExpressionView,
    Literal, UnaryOperator,
};

mod plan_values;
mod scope;

pub use scope::{is_scope_restriction, scope_restriction, scoped_read_plan};

pub use plan_values::{
    CollectionName, FieldListContext, IdentifierContext, InsertDocument, PivotAggregate,
    PivotSpecification, PivotValue, PlanValueError, PlanValueErrorKind, PlanValueResult,
    SetAssignment, SortDirection, SortKey,
};

mod syntax;
#[doc(hidden)]
pub mod vcollections;

mod planner;

pub use planner::{
    Planner, PlannerError, PlannerErrorKind, PlannerOptions, PlannerPipeline, PlannerStage,
    PlanningResult,
};

mod execution_properties;

pub use execution_properties::{
    Bound, CardinalityEffect, Effect, ExecutionProperties, Fields, Flow, Materialization, Order,
    ProjectedAccess, ProjectionReuse, Reads, Scope, Shape,
};

mod physical_plan;

pub use physical_plan::source_field_demand;
pub use physical_plan::{
    ExecutionMode, MemoryExecutionMode, PhysicalAccess, PhysicalFieldContext, PhysicalLoadMode,
    PhysicalOperator, PhysicalOperatorKind, PhysicalPlan, PhysicalPlanBuilder, PhysicalPlanError,
    PhysicalPlanErrorKind, PhysicalPlanResult, PhysicalPlanner, PhysicalPlannerOptions,
    PhysicalSource, PhysicalSubPipeline, StorageAccessMode,
};

pub(crate) mod codec;
mod executor;

pub use executor::{
    CustomOperatorResult, DocumentScope, ExecutionError, ExecutionErrorKind, ExecutionOutput,
    ExecutionResult, ExecutionRow, ExecutionRowOrigin, ExecutionStatistics, ExecutionStrategies,
    ExecutionStrategy, IncrementalGroupAccumulator, IncrementalPivotAccumulator, PivotRun,
    PreparedInsertDocument, Reservoir, StreamingLoadMutation, SyntheticDocument,
};

pub(crate) use executor::{
    decode_execution_row, encode_execution_row_into, execute_insert, execute_streaming_load,
    execution_row_encoded_len, execution_row_working_bytes, insert_document, into_target,
    projected_row_working_bytes_refs, sample_seed, stable_sort, streaming_load_specification,
    BoundedProjectedTopN, BoundedTopN, ProjectedRowLocator,
};

mod insert_materializer;

pub use insert_materializer::InsertDocumentMaterializer;

mod projected_values;

mod prune;
mod pushdown;
mod totality;

mod batch;
pub(crate) mod keys;

pub(crate) use batch::keep;
pub use batch::{Abort, BatchProgram, BatchRunner, BlockKernel, Column, KernelColumn, Tag, BATCH_ROWS};

pub(crate) use executor::{compare_owned_projected_rows, TopSelection};
pub use prune::GroupPruning;
pub use totality::{summable, total_kinds, Kinds};

pub use projected_values::{
    document_of_fields, AccessVector, ProjectedPredicate, ProjectedValueLayout,
    ProjectedValuePipeline, ProjectedValueRow, SlotProgram, SlotRow,
};

mod runtime_materializer;
mod window;
pub use window::{FillSpec, WindowSpec};

pub(crate) use runtime_materializer::{compact_group_item, describe_group_fields, group_field_layout, path_value, split_derive_assignments};
pub(crate) use planner::{group_key_marker, group_measure_marker};
pub use runtime_materializer::{
    CompiledStage, DenseGroups, QueryRuntime, RuntimeMaterializer, SlotExpansion,
};

pub mod planner_cache;
pub use planner_cache::{PlannerCache, PlannerCacheStats};
