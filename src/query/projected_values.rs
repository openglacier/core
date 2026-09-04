//! Standard projected-value access vector shared by query stages.

use std::sync::Arc;

use crate::{
    equals, greater_than, greater_than_or_equal, less_than, less_than_or_equal, model::not_equals,
    model::parse_number_value, model::CoercionPolicy, model::FieldPath, model::Value,
    storage::ProjectedValueRef,
};

use super::Fields;
use super::{
    BinaryOperator, ExecutionError, ExecutionResult, Expression, ExpressionFieldPath,
    ExpressionFieldResolver, ExpressionView, Literal, PhysicalOperator, SemanticValue,
    UnaryOperator,
};

/// Row representation used by stages that can consume projected field values
/// without materializing a full document.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AccessVector {
    /// Full document materialization.
    Document,
    /// Values aligned to a precomputed [`ProjectedValueLayout`].
    ProjectedValues,
}

/// Stable mapping between query field paths and storage projection slots.
///
/// The layout is prepared once per pipeline and reused for every source row.
/// Fields are deduplicated while preserving first-use order.
#[derive(Clone, Debug)]
pub struct ProjectedValueLayout {
    expression_fields: Arc<[ExpressionFieldPath]>,
    storage_fields: Arc<[FieldPath]>,
}

impl ProjectedValueLayout {
    /// Builds one projected-value layout from the union of fields required by
    /// compatible stages.
    pub fn new<I>(fields: I) -> ExecutionResult<Self>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        let mut expression_fields = Vec::new();
        let mut storage_fields = Vec::new();

        for field in fields {
            if expression_fields.contains(&field) {
                continue;
            }
            let storage = FieldPath::parse(&field.to_string())
                .map_err(|error| ExecutionError::evaluation(error.to_string()))?;
            expression_fields.push(field);
            storage_fields.push(storage);
        }

        Ok(Self {
            expression_fields: Arc::from(expression_fields),
            storage_fields: Arc::from(storage_fields),
        })
    }

    /// Query-level fields in slot order.
    #[must_use]
    pub fn fields(&self) -> &[ExpressionFieldPath] {
        &self.expression_fields
    }

    /// Storage-level fields in the same slot order.
    #[must_use]
    pub fn storage_fields(&self) -> &[FieldPath] {
        &self.storage_fields
    }

    /// Returns whether every requested field can use Glacier's current native
    /// top-level projected-value cursor.
    #[must_use]
    pub fn is_top_level(&self) -> bool {
        self.storage_fields.iter().all(|field| field.len() == 1)
    }

    /// Returns the slot occupied by one query field.
    #[must_use]
    pub fn slot(&self, field: &ExpressionFieldPath) -> Option<usize> {
        self.expression_fields
            .iter()
            .position(|candidate| candidate == field)
    }

    /// Resolves several fields to slots, preserving requested order.
    pub fn slots(&self, fields: &[ExpressionFieldPath]) -> ExecutionResult<Vec<usize>> {
        fields
            .iter()
            .map(|field| {
                self.slot(field).ok_or_else(|| {
                    ExecutionError::evaluation(format!(
                        "projected-value field `{field}` is absent from the pipeline layout"
                    ))
                })
            })
            .collect()
    }
}

/// One compiled projected-value stage.
///
/// The enum is intentionally small: new query stages opt into the standard
/// access vector by adding one representation here rather than creating
/// combination-specific fast paths in the engine.
#[derive(Clone, Debug)]
enum ProjectedValueStage {
    Filter(ProjectedPredicate),
    Select,
}

/// A sequence of physical stages that can consume the standard
/// [`AccessVector::ProjectedValues`] representation.
///
/// Filter and Select stages compose ahead of blocking projected consumers such
/// as group, sort and explicit-field distinct. Unsupported operators return
/// `None`, preserving the established Document pipeline as the correctness
/// fallback.
#[derive(Clone, Debug)]
pub struct ProjectedValuePipeline {
    layout: ProjectedValueLayout,
    stages: Arc<[ProjectedValueStage]>,
    gate_field_count: usize,
    hydration_projection: Option<Arc<[ExpressionFieldPath]>>,
}

impl ProjectedValuePipeline {
    /// Compiles one stage prefix against the fields required by its downstream
    /// consumer. Returns `None` when any prefix stage cannot consume projected
    /// values.
    pub fn compile<I>(
        operators: &[PhysicalOperator],
        downstream_fields: I,
    ) -> ExecutionResult<Option<Self>>
    where
        I: IntoIterator<Item = ExpressionFieldPath>,
    {
        let downstream_fields = downstream_fields.into_iter().collect::<Vec<_>>();
        let mut required = downstream_fields.clone();

        // Walk backwards to validate Select visibility without turning Select
        // into a demand for every field it exposes. Only fields actually used
        // downstream remain required.
        for operator in operators.iter().rev() {
            match operator {
                PhysicalOperator::Filter { predicate } => {
                    collect_expression_fields(predicate, &mut required);
                }
                _ => match operator.execution_properties().fields {
                    Fields::Projected(fields) => {
                        if required.iter().any(|field| !fields.contains(field)) {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                },
            }
        }

        // Gate fields are the dependencies needed to decide whether a row
        // continues through the projected prefix. Put them first so storage
        // can decode them before downstream-only values.
        let mut gate_fields = Vec::new();
        for operator in operators {
            if let PhysicalOperator::Filter { predicate } = operator {
                collect_expression_fields(predicate, &mut gate_fields);
            }
        }
        gate_fields.dedup();

        let mut source_fields = gate_fields.clone();
        for field in required {
            if !source_fields.contains(&field) {
                source_fields.push(field);
            }
        }
        let gate_field_count = gate_fields.len();

        if source_fields.iter().any(|field| field.to_string() == "_id") {
            return Ok(None);
        }

        let layout = ProjectedValueLayout::new(source_fields)?;
        if !layout.is_top_level() {
            return Ok(None);
        }

        let mut stages = Vec::with_capacity(operators.len());
        let mut hydration_projection = None;
        for operator in operators {
            match operator {
                PhysicalOperator::Filter { predicate } => {
                    stages.push(ProjectedValueStage::Filter(ProjectedPredicate::compile(
                        predicate, &layout,
                    )?));
                }
                PhysicalOperator::Select { fields } => {
                    // Projection constrains visibility during compilation while the
                    // source scan still requests only fields needed downstream. Keep
                    // the user-visible projection so late hydration can reproduce the
                    // normal Document pipeline exactly.
                    hydration_projection = Some(Arc::clone(fields));
                    stages.push(ProjectedValueStage::Select);
                }
                _ => unreachable!("projected-value compatibility was checked above"),
            }
        }

        Ok(Some(Self {
            layout,
            stages: Arc::from(stages),
            gate_field_count,
            hydration_projection,
        }))
    }

    /// Shared slot layout requested from storage.
    #[must_use]
    pub const fn layout(&self) -> &ProjectedValueLayout {
        &self.layout
    }

    /// Number of leading slots required to decide whether a row continues.
    #[must_use]
    pub const fn gate_field_count(&self) -> usize {
        self.gate_field_count
    }

    /// Projection that must be re-applied after late hydration, when a projected
    /// `select` appeared before the blocking consumer.
    #[must_use]
    pub fn hydration_projection(&self) -> Option<&[ExpressionFieldPath]> {
        self.hydration_projection.as_deref()
    }

    /// Applies every compatible stage to one projected row.
    ///
    /// The callback retains ownership of expression semantics; this pipeline
    /// only handles access-vector composition.
    pub fn accepts_with<F>(
        &self,
        values: &[Option<crate::Value>],
        evaluate: F,
    ) -> ExecutionResult<bool>
    where
        F: FnMut(&Expression, &dyn ExpressionFieldResolver<crate::Value>) -> ExecutionResult<bool>,
    {
        let row = ProjectedValueRow::new(&self.layout, values)?;
        self.accepts_resolved(&row, evaluate)
    }

    /// Applies the same projected stage prefix to borrowed storage values.
    ///
    /// Scalar ownership is acquired only when the language evaluator resolves a
    /// field, keeping access-vector composition independent from materialization.
    pub fn accepts_refs_with<F>(
        &self,
        values: &[Option<ProjectedValueRef<'_>>],
        mut evaluate: F,
    ) -> ExecutionResult<bool>
    where
        F: FnMut(&Expression, &dyn ExpressionFieldResolver<crate::Value>) -> ExecutionResult<bool>,
    {
        if values.len() != self.layout.fields().len() {
            return Err(ExecutionError::evaluation(
                "borrowed projected-value row does not match its field layout",
            ));
        }
        let mut fallback_row = None;
        for stage in self.stages.iter() {
            if let ProjectedValueStage::Filter(predicate) = stage {
                let accepted = match predicate.accepts_refs(values) {
                    Some(accepted) => accepted,
                    None => {
                        let row = fallback_row.get_or_insert_with(|| {
                            ProjectedValueRefRow::new(&self.layout, values)
                                .expect("projected row length checked above")
                        });
                        evaluate(predicate.expression(), row)?
                    }
                };
                if !accepted {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn accepts_resolved<F>(
        &self,
        row: &dyn ExpressionFieldResolver<crate::Value>,
        mut evaluate: F,
    ) -> ExecutionResult<bool>
    where
        F: FnMut(&Expression, &dyn ExpressionFieldResolver<crate::Value>) -> ExecutionResult<bool>,
    {
        for stage in self.stages.iter() {
            if let ProjectedValueStage::Filter(predicate) = stage {
                if !evaluate(predicate.expression(), row)? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

/// One predicate compiled against a [`ProjectedValueLayout`].
///
/// Expression semantics remain owned by the normal query runtime. This type
/// only resolves field references to stable projected-value slots and can
/// materialize the minimal evaluation document required by that predicate.
#[derive(Clone, Debug)]
pub struct ProjectedPredicate {
    expression: Expression,
    fast: Option<CompiledPredicate>,
}

#[derive(Clone, Debug)]
enum CompiledPredicate {
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
    Compare {
        left: CompiledScalar,
        operator: BinaryOperator,
        right: CompiledScalar,
    },
}

#[derive(Clone, Debug)]
enum CompiledScalar {
    Slot(usize),
    Literal(Value),
}

impl ProjectedPredicate {
    /// Compiles a predicate against an existing projected-value layout.
    ///
    /// Every referenced field must already be present in the layout. This is
    /// intentionally independent of the expression evaluator so projected
    /// execution cannot drift from the language's normal predicate semantics.
    pub fn compile(
        expression: &Expression,
        layout: &ProjectedValueLayout,
    ) -> ExecutionResult<Self> {
        let mut required = Vec::<ExpressionFieldPath>::new();
        collect_expression_fields(expression, &mut required);

        for field in required {
            if layout.slot(&field).is_none() {
                return Err(ExecutionError::evaluation(format!(
                    "projected predicate field `{field}` is absent from the pipeline layout"
                )));
            }
        }

        Ok(Self {
            expression: expression.clone(),
            fast: CompiledPredicate::compile(expression, layout),
        })
    }

    /// Original expression evaluated by the normal query runtime.
    #[must_use]
    pub const fn expression(&self) -> &Expression {
        &self.expression
    }

    fn accepts_refs(&self, values: &[Option<ProjectedValueRef<'_>>]) -> Option<bool> {
        self.fast.as_ref()?.evaluate(values)
    }
}

impl CompiledPredicate {
    fn compile(expression: &Expression, layout: &ProjectedValueLayout) -> Option<Self> {
        let expression = expression.ungrouped();
        match expression.view() {
            ExpressionView::Binary {
                left,
                operator: BinaryOperator::And,
                right,
            } => Some(Self::And(
                Box::new(Self::compile(left, layout)?),
                Box::new(Self::compile(right, layout)?),
            )),
            ExpressionView::Binary {
                left,
                operator: BinaryOperator::Or,
                right,
            } => Some(Self::Or(
                Box::new(Self::compile(left, layout)?),
                Box::new(Self::compile(right, layout)?),
            )),
            ExpressionView::Unary {
                operator: UnaryOperator::Not,
                operand,
            } => Some(Self::Not(Box::new(Self::compile(operand, layout)?))),
            ExpressionView::Binary {
                left,
                operator,
                right,
            } if operator.is_comparison() => Some(Self::Compare {
                left: CompiledScalar::compile(left, layout)?,
                operator,
                right: CompiledScalar::compile(right, layout)?,
            }),
            _ => None,
        }
    }

    fn evaluate(&self, values: &[Option<ProjectedValueRef<'_>>]) -> Option<bool> {
        match self {
            Self::And(left, right) => {
                let left = left.evaluate(values)?;
                if !left {
                    Some(false)
                } else {
                    right.evaluate(values)
                }
            }
            Self::Or(left, right) => {
                let left = left.evaluate(values)?;
                if left {
                    Some(true)
                } else {
                    right.evaluate(values)
                }
            }
            Self::Not(inner) => inner.evaluate(values).map(|value| !value),
            Self::Compare {
                left,
                operator,
                right,
            } => compare_fast_scalars(left.resolve(values)?, right.resolve(values)?, *operator),
        }
    }
}

impl CompiledScalar {
    fn compile(expression: &Expression, layout: &ProjectedValueLayout) -> Option<Self> {
        let expression = expression.ungrouped();
        if let Some(field) = expression.as_field() {
            return layout.slot(field).map(Self::Slot);
        }
        let value = match expression.as_literal()? {
            Literal::Null => Value::null(),
            Literal::Bool(value) => Value::bool(*value),
            Literal::String(value) => Value::string(Arc::clone(value)),
            Literal::Number(text) => Value::Number(parse_number_value(text).ok()?),
            Literal::Json(_) => return None,
        };
        Some(Self::Literal(value))
    }

    fn resolve<'a>(
        &'a self,
        values: &'a [Option<ProjectedValueRef<'a>>],
    ) -> Option<FastScalar<'a>> {
        match self {
            Self::Slot(slot) => match values.get(*slot)?.as_ref()? {
                ProjectedValueRef::Null => Some(FastScalar::Null),
                ProjectedValueRef::Bool(value) => Some(FastScalar::Bool(*value)),
                ProjectedValueRef::Signed(value) => Some(FastScalar::Signed(*value)),
                ProjectedValueRef::Unsigned(value) => Some(FastScalar::Unsigned(*value)),
                ProjectedValueRef::Float(value) => Some(FastScalar::Float(*value)),
                ProjectedValueRef::String(value) => Some(FastScalar::String(value)),
                ProjectedValueRef::Owned(_) => None,
            },
            Self::Literal(value) => Some(FastScalar::Literal(value)),
        }
    }
}

#[derive(Clone, Copy)]
enum FastScalar<'a> {
    Null,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(&'a str),
    Literal(&'a Value),
}

fn compare_fast_scalars(
    left: FastScalar<'_>,
    right: FastScalar<'_>,
    operator: BinaryOperator,
) -> Option<bool> {
    if let (Some(left), Some(right)) = (fast_str(left), fast_str(right)) {
        return Some(match operator {
            BinaryOperator::Equal => left == right,
            BinaryOperator::NotEqual => left != right,
            BinaryOperator::LessThan => left < right,
            BinaryOperator::LessThanOrEqual => left <= right,
            BinaryOperator::GreaterThan => left > right,
            BinaryOperator::GreaterThanOrEqual => left >= right,
            _ => return None,
        });
    }
    let (left, right) = (fast_value(left)?, fast_value(right)?);
    let policy = CoercionPolicy::Numeric;
    match operator {
        BinaryOperator::Equal => equals(&left, &right, policy).ok(),
        BinaryOperator::NotEqual => not_equals(&left, &right, policy).ok(),
        BinaryOperator::LessThan => less_than(&left, &right, policy).ok(),
        BinaryOperator::LessThanOrEqual => less_than_or_equal(&left, &right, policy).ok(),
        BinaryOperator::GreaterThan => greater_than(&left, &right, policy).ok(),
        BinaryOperator::GreaterThanOrEqual => greater_than_or_equal(&left, &right, policy).ok(),
        _ => None,
    }
}

fn fast_str(value: FastScalar<'_>) -> Option<&str> {
    match value {
        FastScalar::String(value) => Some(value),
        FastScalar::Literal(value) => value.as_str(),
        _ => None,
    }
}

fn fast_value(value: FastScalar<'_>) -> Option<Value> {
    match value {
        FastScalar::Null => Some(Value::null()),
        FastScalar::Bool(value) => Some(Value::bool(value)),
        FastScalar::Signed(value) => Some(Value::signed(value)),
        FastScalar::Unsigned(value) => Some(Value::unsigned(value)),
        FastScalar::Float(value) => Value::float(value).ok(),
        FastScalar::Literal(value) => Some(value.clone()),
        FastScalar::String(_) => None,
    }
}

fn collect_expression_fields(expression: &Expression, fields: &mut Vec<ExpressionFieldPath>) {
    match expression.view() {
        ExpressionView::Field(field) => fields.push(field.clone()),
        ExpressionView::Unary { operand, .. } => collect_expression_fields(operand, fields),
        ExpressionView::Binary { left, right, .. } => {
            collect_expression_fields(left, fields);
            collect_expression_fields(right, fields);
        }
        ExpressionView::Group(inner) => collect_expression_fields(inner, fields),
        ExpressionView::Literal(_) => {}
    }
}

/// Borrowed projected row aligned to one [`ProjectedValueLayout`].
#[derive(Clone, Copy, Debug)]
pub struct ProjectedValueRow<'a> {
    layout: &'a ProjectedValueLayout,
    values: &'a [Option<crate::Value>],
}

impl<'a> ProjectedValueRow<'a> {
    /// Creates a row when the storage cursor emitted the expected slot count.
    pub fn new(
        layout: &'a ProjectedValueLayout,
        values: &'a [Option<crate::Value>],
    ) -> ExecutionResult<Self> {
        if values.len() != layout.fields().len() {
            return Err(ExecutionError::evaluation(
                "projected-value row does not match its field layout",
            ));
        }
        Ok(Self { layout, values })
    }

    /// Raw values in stable slot order.
    #[must_use]
    pub fn values(&self) -> &'a [Option<crate::Value>] {
        self.values
    }

    /// Reads one slot directly.
    #[must_use]
    pub fn slot(&self, index: usize) -> Option<&'a crate::Value> {
        self.values.get(index).and_then(Option::as_ref)
    }

    /// Reads one field without materializing a document.
    #[must_use]
    pub fn get(&self, field: &ExpressionFieldPath) -> Option<&'a crate::Value> {
        self.layout
            .slot(field)
            .and_then(|index| self.values.get(index))
            .and_then(Option::as_ref)
    }
}

impl ExpressionFieldResolver<crate::Value> for ProjectedValueRow<'_> {
    fn resolve_field(&self, field: &ExpressionFieldPath) -> SemanticValue<crate::Value> {
        self.get(field)
            .cloned()
            .map_or(SemanticValue::Missing, SemanticValue::Present)
    }
}

/// Borrowed storage scalar row using the same stable projected-value layout.
#[derive(Clone, Copy, Debug)]
struct ProjectedValueRefRow<'layout, 'row, 'value> {
    layout: &'layout ProjectedValueLayout,
    values: &'row [Option<ProjectedValueRef<'value>>],
}

impl<'layout, 'row, 'value> ProjectedValueRefRow<'layout, 'row, 'value> {
    fn new(
        layout: &'layout ProjectedValueLayout,
        values: &'row [Option<ProjectedValueRef<'value>>],
    ) -> ExecutionResult<Self> {
        if values.len() != layout.fields().len() {
            return Err(ExecutionError::evaluation(
                "borrowed projected-value row does not match its field layout",
            ));
        }
        Ok(Self { layout, values })
    }
}

impl ExpressionFieldResolver<crate::Value> for ProjectedValueRefRow<'_, '_, '_> {
    fn resolve_field(&self, field: &ExpressionFieldPath) -> SemanticValue<crate::Value> {
        self.layout
            .slot(field)
            .and_then(|slot| self.values.get(slot))
            .and_then(Option::as_ref)
            .map(ProjectedValueRef::to_value)
            .map_or(SemanticValue::Missing, SemanticValue::Present)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_deduplicates_fields_and_resolves_slots() {
        let period = ExpressionFieldPath::new(["tPeriode"]).unwrap();
        let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap();
        let layout =
            ProjectedValueLayout::new([period.clone(), revenue.clone(), period.clone()]).unwrap();

        assert_eq!(layout.fields(), &[period.clone(), revenue.clone()]);
        assert_eq!(layout.slot(&period), Some(0));
        assert_eq!(layout.slot(&revenue), Some(1));
        assert!(layout.is_top_level());
    }

    #[test]
    fn projected_row_reads_values_by_field() {
        let period = ExpressionFieldPath::new(["tPeriode"]).unwrap();
        let layout = ProjectedValueLayout::new([period.clone()]).unwrap();
        let values = [Some(crate::Value::string("12-2025"))];
        let row = ProjectedValueRow::new(&layout, &values).unwrap();

        assert_eq!(row.get(&period), Some(&crate::Value::string("12-2025")));
    }
    #[test]
    fn projected_pipeline_unions_filter_and_downstream_fields() {
        let period = ExpressionFieldPath::new(["tPeriode"]).unwrap();
        let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap();
        let predicate = crate::query::parse_expression(r#"tPeriode == "12-2025""#).unwrap();
        let operators = [PhysicalOperator::Filter { predicate }];

        let pipeline = ProjectedValuePipeline::compile(&operators, [revenue.clone()])
            .unwrap()
            .unwrap();

        // Gate fields are deliberately placed first so storage can decode
        // predicate dependencies before downstream-only values.
        assert_eq!(pipeline.layout().slot(&period), Some(0));
        assert_eq!(pipeline.layout().slot(&revenue), Some(1));
    }

    #[test]
    fn projected_pipeline_accepts_select_as_slot_visibility_boundary() {
        let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap();
        let operators = [PhysicalOperator::Select {
            fields: Arc::from([revenue.clone()]),
        }];

        let pipeline = ProjectedValuePipeline::compile(&operators, [revenue.clone()])
            .unwrap()
            .unwrap();
        assert_eq!(pipeline.layout().slot(&revenue), Some(0));
    }

    #[test]
    fn projected_pipeline_composes_filter_before_select() {
        let period = ExpressionFieldPath::new(["tPeriode"]).unwrap();
        let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap();
        let predicate = crate::query::parse_expression(r#"tPeriode == "12-2025""#).unwrap();
        let operators = [
            PhysicalOperator::Filter { predicate },
            PhysicalOperator::Select {
                fields: Arc::from([revenue.clone()]),
            },
        ];

        let pipeline = ProjectedValuePipeline::compile(&operators, [revenue.clone()])
            .unwrap()
            .unwrap();
        assert!(pipeline.layout().slot(&period).is_some());
        assert!(pipeline.layout().slot(&revenue).is_some());
    }

    #[test]
    fn projected_pipeline_select_does_not_force_unused_fields() {
        let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap();
        let operators = [PhysicalOperator::Select {
            fields: Arc::from([revenue]),
        }];

        let pipeline =
            ProjectedValuePipeline::compile(&operators, std::iter::empty::<ExpressionFieldPath>())
                .unwrap()
                .unwrap();

        assert!(pipeline.layout().fields().is_empty());
        assert_eq!(pipeline.gate_field_count(), 0);
    }

    #[test]
    fn projected_pipeline_rejects_downstream_field_removed_by_select() {
        let period = ExpressionFieldPath::new(["tPeriode"]).unwrap();
        let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap();
        let operators = [PhysicalOperator::Select {
            fields: Arc::from([period]),
        }];

        assert!(ProjectedValuePipeline::compile(&operators, [revenue])
            .unwrap()
            .is_none());
    }

    #[test]
    fn predicate_compiles_against_projected_layout() {
        let period = ExpressionFieldPath::new(["tPeriode"]).unwrap();
        let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap();
        let layout = ProjectedValueLayout::new([period.clone(), revenue.clone()]).unwrap();
        let values = [
            Some(crate::Value::string("12-2025")),
            Some(crate::Value::float(12.5).unwrap()),
        ];
        let row = ProjectedValueRow::new(&layout, &values).unwrap();
        assert_eq!(
            row.resolve_field(&period),
            SemanticValue::Present(crate::Value::string("12-2025"))
        );
    }
}
