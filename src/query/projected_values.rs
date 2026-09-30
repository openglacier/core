#![cfg_attr(rustfmt, rustfmt_skip)]
//! Standard projected-value access vector shared by query stages.

use std::sync::Arc;

use crate::{
    equals, greater_than, greater_than_or_equal, less_than, less_than_or_equal, model::not_equals,
    model::parse_number_value, model::CoercionPolicy, model::FieldPath, model::Value,
    storage::ProjectedValueRef,
};

use super::{
    BinaryOperator, ExecutionError, ExecutionResult, Expression, ExpressionFieldPath,
    ExpressionFieldResolver, ExpressionView, Literal, PhysicalOperator, SemanticValue,
    UnaryOperator, Fields, ProjectionReuse, SlotExpansion
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

    /// Returns whether every requested field is compatible with the storage
    /// top-level projected-value cursor contract.
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
    projection_reuse: ProjectionReuse,
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
        let mut required = downstream_fields;
        let projection_reuse = if operators
            .iter()
            .all(|operator| operator.execution_properties().reuses_projection())
        {
            ProjectionReuse::Reusable
        } else {
            ProjectionReuse::None
        };

        // Walk backwards to validate Select visibility without turning Select
        // into a demand for every field it exposes. Only fields actually used
        // downstream remain required.
        for operator in operators.iter().rev() {
            match operator {
                PhysicalOperator::Filter { predicate } => {
                    collect_expression_fields(predicate, &mut required);
                }
                // Only the native Select is a pure visibility boundary here;
                // `select … as` computes its outputs and has no stage below.
                PhysicalOperator::Select { .. } => match operator.execution_properties().fields {
                    Fields::Projected(fields) => {
                        if required.iter().any(|field| !fields.contains(field)) {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                },
                _ => return Ok(None),
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
            projection_reuse,
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

    /// Whether every source-facing stage declares immutable source
    /// projections reusable across executions.
    #[must_use]
    pub const fn projection_reuse(&self) -> ProjectionReuse {
        self.projection_reuse
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
        let mut evaluate = evaluate;
        for stage in self.stages.iter() {
            if let ProjectedValueStage::Filter(predicate) = stage {
                if !evaluate(predicate.expression(), &row)? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
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
                let accepted = if let Some(accepted) = predicate.accepts_refs(values) { accepted } else {
                    let row = fallback_row.get_or_insert_with(|| {
                        ProjectedValueRefRow::new(&self.layout, values)
                            .expect("projected row length checked above")
                    });
                    evaluate(predicate.expression(), row)?
                };
                if !accepted {
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
    pub fn compile( expression: &Expression, layout: &ProjectedValueLayout, ) -> ExecutionResult<Self> {
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
                if left {
                    right.evaluate(values)
                } else {
                    Some(false)
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

fn compare_fast_scalars( left: FastScalar<'_>, right: FastScalar<'_>, operator: BinaryOperator, ) -> Option<bool> {
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
    expression.for_each_field(&mut |field| fields.push(field.clone()));
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
    pub const fn values(&self) -> &'a [Option<crate::Value>] {
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

/// One row held as values by top-level field, without a document.
///
/// Row-local stages that declare top-level writes run on it (see
/// [`super::CompiledStage::apply_slots`]); an absent field is `None`.
#[derive(Debug)]
pub struct SlotRow<'a> {
    fields: &'a [ExpressionFieldPath],
    values: &'a mut [Option<Value>],
}

impl<'a> SlotRow<'a> {
    fn new(fields: &'a [ExpressionFieldPath], values: &'a mut [Option<Value>]) -> Self {
        debug_assert_eq!(fields.len(), values.len());
        Self { fields, values }
    }

    fn slot(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.first() == name)
    }

    /// Value at a (possibly nested) path.
    #[must_use]
    pub fn get(&self, field: &ExpressionFieldPath) -> Option<&Value> {
        let mut segments = field.iter();
        let mut current = self.values[self.slot(segments.next()?)?].as_ref()?;
        for segment in segments {
            current = current.as_object()?.get(segment)?;
        }
        Some(current)
    }

    /// Writes (or with `None` removes) a top-level field of the program layout.
    pub fn set(&mut self, field: &ExpressionFieldPath, value: Option<Value>) {
        let slot = self.slot(field.first()).expect("slot program layout holds every written field");
        self.values[slot] = value;
    }

    /// Removes a top-level field and returns its value.
    pub fn take(&mut self, field: &ExpressionFieldPath) -> Option<Value> {
        let slot = self.slot(field.first())?;
        self.values[slot].take()
    }

    /// Removes every field.
    pub fn clear(&mut self) {
        self.values.iter_mut().for_each(|value| *value = None);
    }
}

impl ExpressionFieldResolver<Value> for SlotRow<'_> {
    fn resolve_field(&self, field: &ExpressionFieldPath) -> SemanticValue<Value> {
        self.get(field).cloned().map_or(SemanticValue::Missing, SemanticValue::Present)
    }
}

#[derive(Clone, Debug)]
enum SlotStage {
    Filter(Expression),
    Stage(Arc<super::CompiledStage>),
}

/// Row-local stages (`where` and compiled stages that run on slots) applied
/// to a [`SlotRow`], so a row read as projected values never becomes a
/// document until a consumer needs one.
///
/// Which stages qualify is decided from what they declare: the fields they
/// read, and top-level fields they write, one row in and one row out.
#[derive(Clone, Debug)]
pub struct SlotProgram {
    fields: Arc<[ExpressionFieldPath]>,
    stages: Arc<[SlotStage]>,
}

impl SlotProgram {
    /// Compiles `operators` over rows holding `source_fields` (top-level).
    /// Returns `None` when a stage cannot run on slots or reads `_id`.
    /// Every stage turns one row into at most one: see [`Self::run`].
    #[must_use]
    pub fn compile(operators: &[PhysicalOperator], source_fields: &[ExpressionFieldPath]) -> Option<Self> {
        Self::compile_with(operators, source_fields, false)
    }

    /// Like [`Self::compile`], also accepting stages that turn one row into
    /// several (`unwind`); such a program runs through [`Self::run_each`].
    #[must_use]
    pub fn compile_expanding(operators: &[PhysicalOperator], source_fields: &[ExpressionFieldPath]) -> Option<Self> {
        Self::compile_with(operators, source_fields, true)
    }

    fn compile_with(operators: &[PhysicalOperator], source_fields: &[ExpressionFieldPath], expanding: bool) -> Option<Self> {
        if source_fields.iter().any(|field| field.len() != 1 || field.first() == "_id") {
            return None;
        }
        let mut fields = source_fields.to_vec();
        let mut stages = Vec::with_capacity(operators.len());
        for operator in operators {
            if operator.reads_document_id() {
                return None;
            }
            match operator {
                PhysicalOperator::Filter { predicate } => stages.push(SlotStage::Filter(predicate.clone())),
                PhysicalOperator::Custom { writes: false, compiled: Some(stage), .. } if stage.runs_on_slots() || (expanding && stage.expands_on_slots()) => {
                    for field in stage.writes().iter() {
                        if field.first() == "_id" {
                            return None;
                        }
                        if !fields.contains(field) {
                            fields.push(field.clone());
                        }
                    }
                    stages.push(SlotStage::Stage(Arc::clone(stage)));
                }
                _ => return None,
            }
        }
        Some(Self { fields: Arc::from(fields), stages: Arc::from(stages) })
    }

    /// Fields held by the slots of a row, in slot order.
    #[must_use]
    pub fn fields(&self) -> &[ExpressionFieldPath] {
        &self.fields
    }

    /// Slot count of a row: the source fields first, then written fields.
    #[must_use]
    pub fn width(&self) -> usize {
        self.fields.len()
    }

    /// Slots holding `fields` after the stages ran, or `None` when one of
    /// them is not a top-level field of the program.
    #[must_use]
    pub fn slots(&self, fields: &[ExpressionFieldPath]) -> Option<Vec<usize>> {
        fields.iter().map(|field| self.fields.iter().position(|slot| slot == field)).collect()
    }

    /// Runs the stages on one row whose leading slots hold the source values
    /// and the rest `None`. Returns whether the row passed every filter.
    /// Only for a program from [`Self::compile`].
    pub fn run<F>(&self, values: &mut [Option<Value>], mut evaluate: F) -> ExecutionResult<bool>
    where
        F: FnMut(&Expression, &dyn ExpressionFieldResolver<Value>) -> ExecutionResult<bool>,
    {
        let mut row = SlotRow::new(&self.fields, values);
        for stage in self.stages.iter() {
            match stage {
                SlotStage::Filter(predicate) => {
                    if !evaluate(predicate, &row)? {
                        return Ok(false);
                    }
                }
                SlotStage::Stage(stage) => stage.apply_slots(&mut row)?,
            }
        }
        Ok(true)
    }

    /// Runs the stages on one row and calls `emit` for every output row, in
    /// order: none when a filter drops it, several when a stage expands it.
    /// Returns how many rows were emitted.
    pub fn run_each<F, E>(&self, values: &mut [Option<Value>], mut evaluate: F, emit: &mut E) -> ExecutionResult<u64>
    where
        F: FnMut(&Expression, &dyn ExpressionFieldResolver<Value>) -> ExecutionResult<bool>,
        E: FnMut(&[Option<Value>]) -> ExecutionResult<()>,
    {
        self.run_from(0, values, &mut evaluate, emit)
    }

    fn run_from<F, E>(&self, start: usize, values: &mut [Option<Value>], evaluate: &mut F, emit: &mut E) -> ExecutionResult<u64>
    where
        F: FnMut(&Expression, &dyn ExpressionFieldResolver<Value>) -> ExecutionResult<bool>,
        E: FnMut(&[Option<Value>]) -> ExecutionResult<()>,
    {
        let mut row = SlotRow::new(&self.fields, values);
        for (position, stage) in self.stages.iter().enumerate().skip(start) {
            match stage {
                SlotStage::Filter(predicate) => {
                    if !evaluate(predicate, &row)? {
                        return Ok(0);
                    }
                }
                SlotStage::Stage(stage) if stage.expands_on_slots() => match stage.expand_slots(&row)? {
                    SlotExpansion::Keep => {}
                    SlotExpansion::Drop => return Ok(0),
                    SlotExpansion::Set(writes) => {
                        for (field, value) in writes {
                            row.set(&field, value);
                        }
                    }
                    SlotExpansion::Each { field, index, items } => {
                        let mut emitted = 0;
                        let mut copy = Vec::with_capacity(row.values.len());
                        for (item_position, item) in items.iter().enumerate() {
                            copy.clear();
                            copy.extend_from_slice(row.values);
                            let mut item_row = SlotRow::new(&self.fields, &mut copy);
                            item_row.set(&field, Some(item.clone()));
                            if let Some(index) = &index {
                                item_row.set(index, Some(Value::unsigned(item_position as u64)));
                            }
                            emitted += self.run_from(position + 1, &mut copy, evaluate, emit)?;
                        }
                        return Ok(emitted);
                    }
                },
                SlotStage::Stage(stage) => stage.apply_slots(&mut row)?,
            }
        }
        emit(row.values)?;
        Ok(1)
    }

    /// Document holding the present slots of a row.
    #[must_use]
    pub fn document(&self, values: &[Option<Value>]) -> crate::Document {
        let mut document = crate::Document::new();
        for (field, value) in self.fields.iter().zip(values) {
            if let Some(value) = value {
                document.insert(field.first(), value.clone());
            }
        }
        document
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

    #[test] fn layout_deduplicates_fields_and_resolves_slots() { let period = ExpressionFieldPath::new(["tPeriode"]).unwrap(); let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap(); let layout = ProjectedValueLayout::new([period.clone(), revenue.clone(), period.clone()]).unwrap(); assert_eq!(layout.fields(), &[period.clone(), revenue.clone()]); assert_eq!(layout.slot(&period), Some(0)); assert_eq!(layout.slot(&revenue), Some(1)); assert!(layout.is_top_level()); }
    #[test] fn projected_row_reads_values_by_field() { let period = ExpressionFieldPath::new(["tPeriode"]).unwrap(); let layout = ProjectedValueLayout::new([period.clone()]).unwrap(); let values = [Some(crate::Value::string("12-2025"))]; let row = ProjectedValueRow::new(&layout, &values).unwrap(); assert_eq!(row.get(&period), Some(&crate::Value::string("12-2025"))); }
    #[test] fn projected_pipeline_unions_filter_and_downstream_fields() { let period = ExpressionFieldPath::new(["tPeriode"]).unwrap(); let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap(); let predicate = crate::query::parse_expression(r#"tPeriode == "12-2025""#).unwrap(); let operators = [PhysicalOperator::Filter { predicate }]; let pipeline = ProjectedValuePipeline::compile(&operators, [revenue.clone()]) .unwrap() .unwrap(); assert_eq!(pipeline.layout().slot(&period), Some(0)); assert_eq!(pipeline.layout().slot(&revenue), Some(1)); assert_eq!(pipeline.projection_reuse(), ProjectionReuse::Reusable); }
    #[test] fn projected_pipeline_accepts_select_as_slot_visibility_boundary() { let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap(); let operators = [PhysicalOperator::Select { fields: Arc::from([revenue.clone()]), }]; let pipeline = ProjectedValuePipeline::compile(&operators, [revenue.clone()]) .unwrap() .unwrap(); assert_eq!(pipeline.layout().slot(&revenue), Some(0)); }
    #[test] fn projected_pipeline_composes_filter_before_select() { let period = ExpressionFieldPath::new(["tPeriode"]).unwrap(); let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap(); let predicate = crate::query::parse_expression(r#"tPeriode == "12-2025""#).unwrap(); let operators = [ PhysicalOperator::Filter { predicate }, PhysicalOperator::Select { fields: Arc::from([revenue.clone()]), }, ]; let pipeline = ProjectedValuePipeline::compile(&operators, [revenue.clone()]) .unwrap() .unwrap(); assert!(pipeline.layout().slot(&period).is_some()); assert!(pipeline.layout().slot(&revenue).is_some()); }
    #[test] fn projected_pipeline_select_does_not_force_unused_fields() { let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap(); let operators = [PhysicalOperator::Select { fields: Arc::from([revenue]), }]; let pipeline = ProjectedValuePipeline::compile(&operators, std::iter::empty::<ExpressionFieldPath>()) .unwrap() .unwrap(); assert!(pipeline.layout().fields().is_empty()); assert_eq!(pipeline.gate_field_count(), 0); }
    #[test] fn projected_pipeline_rejects_downstream_field_removed_by_select() { let period = ExpressionFieldPath::new(["tPeriode"]).unwrap(); let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap(); let operators = [PhysicalOperator::Select { fields: Arc::from([period]), }]; assert!(ProjectedValuePipeline::compile(&operators, [revenue]) .unwrap() .is_none()); }
    #[test] fn predicate_compiles_against_projected_layout() { let period = ExpressionFieldPath::new(["tPeriode"]).unwrap(); let revenue = ExpressionFieldPath::new(["CAFacture"]).unwrap(); let layout = ProjectedValueLayout::new([period.clone(), revenue.clone()]).unwrap(); let values = [ Some(crate::Value::string("12-2025")), Some(crate::Value::float(12.5).unwrap()), ]; let row = ProjectedValueRow::new(&layout, &values).unwrap(); assert_eq!( row.resolve_field(&period), SemanticValue::Present(crate::Value::string("12-2025")) ); }
}
