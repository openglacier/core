//! Query module exports.

mod ast;
mod fingerprint;
mod lexer;
mod parser;
mod span;
mod token;

pub use ast::{
    EndAst, NameAst, PipelineAst, SourceAliasAst, SourceAst, SourceKeyword, Spanned, StageAst,
    SubPipelineAst,
};

pub use fingerprint::{
    fingerprint_query_shape, fingerprint_query_text, fingerprint_syntax, LogicalPlanFingerprint,
    QueryShapeFingerprint, QueryTextFingerprint, SyntaxFingerprint,
};

pub use lexer::{lex, LexError, LexErrorKind, LexResult, Lexer, TokenStream};

pub use parser::{
    parse, parse_tokens, ParseError, ParseErrorKind, ParseResult, Parser, QueryParseError,
    QueryParseResult,
};

pub use span::Span;
pub use token::{Token, TokenKind};

mod stage;

pub use stage::{
    ResolvedStage, StageArgumentPolicy, StageDefinition, StageError, StageErrorKind, StageKind,
    StageName, StageRegistry, StageResult,
};

mod expression;
mod json_value;

pub use expression::{
    parse_expression, BinaryOperator, Expression, ExpressionError, ExpressionErrorKind,
    ExpressionFieldPath, ExpressionKind, ExpressionParser, ExpressionResult, ExpressionView,
    Literal, UnaryOperator,
};

mod logical_plan;

pub use logical_plan::{
    CollectionName, FieldListContext, IdentifierContext, InsertDocument, LogicalObject,
    LogicalObjectField, LogicalOperator, LogicalOperatorKind, LogicalPlan, LogicalPlanBuilder,
    LogicalPlanError, LogicalPlanErrorKind, LogicalPlanResult, LogicalSource, LogicalValue,
    PivotAggregate, PivotSpecification, PivotValue, SetAssignment, SortDirection, SortKey,
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
    ProjectedAccess, ProjectionReuse, Scope, Shape,
};

mod physical_plan;

pub use physical_plan::{
    ExecutionMode, MemoryExecutionMode, PhysicalAccess, PhysicalFieldContext, PhysicalLoadMode,
    PhysicalOperator, PhysicalOperatorKind, PhysicalPlan, PhysicalPlanBuilder, PhysicalPlanError,
    PhysicalPlanErrorKind, PhysicalPlanResult, PhysicalPlanner, PhysicalPlannerOptions,
    PhysicalSource, PhysicalSubPipeline, StorageAccessMode,
};

mod executor;

pub use executor::{
    CustomOperatorResult, DocumentScope, ExecutionError, ExecutionErrorKind, ExecutionOutput,
    ExecutionResult, ExecutionRow, ExecutionRowOrigin, ExecutionRuntime, ExecutionStatistics,
    ExecutionStrategies, ExecutionStrategy, Executor, IncrementalGroupAccumulator, LookupDocuments,
    PreparedInsertDocument, StreamingLoadMutation, SyntheticDocument,
};

pub(crate) use executor::{
    decode_execution_row, encode_execution_row_into, execution_row_encoded_len,
    execution_row_working_bytes, projected_row_working_bytes_refs, reserve_query_memory,
    stable_projected_order, stable_sort, BoundedProjectedTopN, BoundedTopN, ProjectedRowLocator,
    ProjectedRowSet,
};

mod lowerer;

pub use lowerer::ScanPlanLowerer;

mod runtime;

pub use runtime::{QueryRuntime, QueryRuntimeBuildError, QueryRuntimeBuilder};

mod insert_materializer;

pub use insert_materializer::InsertDocumentMaterializer;

mod projected_values;

pub use projected_values::{
    AccessVector, ProjectedPredicate, ProjectedValueLayout, ProjectedValuePipeline,
    ProjectedValueRow,
};

mod runtime_materializer;

pub(crate) use runtime_materializer::{group_field_layout, NearSpec};
pub use runtime_materializer::{QueryRuntimeMaterializationExt, RuntimeMaterializer};

mod evaluator;

pub use evaluator::{
    AssignmentPolicy, BooleanPolicy, EvaluationBackend, EvaluationContext, EvaluationError,
    EvaluationErrorKind, EvaluationResult, Evaluator, FunctionEvaluationBackend, MissingPolicy,
};

mod native_evaluator;

pub use native_evaluator::{
    NativeDepthGuard, NativeEvaluationLimits, NativeEvaluationLimitsError, NativeEvaluationSession,
    NativeEvaluationStatisticsSnapshot, NativeEvaluator, NativeSemantics,
};

mod native_semantics;

pub use native_semantics::{
    NativeSemanticBuildError, NativeSemanticBuilder, NativeSemanticFunctions, NativeSemanticOptions,
};

mod expression_semantics;

pub use expression_semantics::{
    ExpressionFieldResolver, ExpressionModel, ExpressionNode, ExpressionSemantics, SemanticValue,
};

mod expression_model;

pub use expression_model::{
    model_backend_error, NativeExpressionModel, NativeExpressionModelBuildError,
    NativeExpressionModelBuilder,
};

mod evaluation_pipeline;

pub use evaluation_pipeline::{
    native_evaluation_runtime, native_evaluation_runtime_with_limits, EvaluationPipeline,
    EvaluationPipelineBuildError, EvaluationPipelineBuilder,
};

mod value_expression_model;

pub use value_expression_model::{value_expression_model, value_expression_runtime};

pub mod planner_cache;
pub use planner_cache::{PlannerCache, PlannerCacheStats};
