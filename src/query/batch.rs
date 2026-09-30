#![cfg_attr(rustfmt, rustfmt_skip)]
//! Batch execution of row-local stages over typed columns.
//!
//! A batch holds up to [`BATCH_ROWS`] rows as columns, one per slot. Each
//! cell carries its own physical tag, so no type has to be known before the
//! scan. Stages run one after the other on the whole batch: `where` narrows
//! a selection of row positions, and every later node only computes the
//! selected rows.
//!
//! Semantics stay those of [`super::eval`]:
//! - every (row, node) pair the row-at-a-time evaluator computes is computed
//!   here too, and no other: `and`, `or`, `case`, `if`, `coalesce` and `div`
//!   evaluate their later operands only on the rows that need them;
//! - fast kernels cover the common operand kinds; any other cell goes through
//!   the evaluator's own primitives for that row;
//! - a batch never reports an error itself: [`BatchRunner::run`] returns
//!   [`Abort`], and the caller replays that batch row by row through the
//!   row-at-a-time path, which fails on the same row with the same message.

use std::fmt::Write as _;
use std::sync::Arc;

use crate::model::{Number, Value};
use crate::storage::{DocumentId, DocumentVersion, ProjectedValueRef};

use super::eval::{arithmetic_value, binary, evaluate_optional, float_of, integer_of, unary};
use super::runtime_materializer::SlotOperation;
use super::{
    BinaryOperator, Expression, ExpressionFieldPath, ExpressionFieldResolver, ExpressionView, Function,
    PhysicalOperator, SemanticValue, UnaryOperator,
};

/// Rows per batch.
pub const BATCH_ROWS: usize = 1024;

/// A batch hit a row the row-at-a-time evaluator would fail on (or that the
/// batch cannot decide): replay the batch row by row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Abort;

type Step = Result<(), Abort>;

// ------------------------------------------------------------------ columns

/// Physical kind of one cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Tag {
    /// The field is absent (distinct from `null`).
    Missing,
    Null,
    /// `bits` is 0 or 1.
    Bool,
    /// `bits` holds the `i64`.
    Signed,
    Unsigned,
    /// `bits` holds the `f64` bits (finite, zero normalized).
    Float,
    /// `bits` holds the text span: offset << 32 | length.
    Str,
    /// Array or object: `bits` indexes `other`.
    Other,
    /// Array of strings (from `split`): `bits` indexes `lists`, a run of
    /// `items`, each a text span.
    List,
}

/// One slot of a batch, dense by row position: only selected rows are
/// meaningful.
#[derive(Clone, Debug, Default)]
pub struct Column {
    tag: Vec<Tag>,
    bits: Vec<u64>,
    text: String,
    other: Vec<Value>,
    lists: Vec<(u32, u32)>,
    items: Vec<(u32, u32)>,
}

impl Column {
    fn sized() -> Self {
        Self { tag: vec![Tag::Missing; BATCH_ROWS], bits: vec![0; BATCH_ROWS], text: String::new(), other: Vec::new(), lists: Vec::new(), items: Vec::new() }
    }

    /// Every row holds `value` (`None`: missing).
    fn broadcast(value: Option<&Value>) -> Self {
        let mut column = Self::sized();
        if let Some(value) = value {
            column.set_value(0, value.clone());
            let (tag, bits) = (column.tag[0], column.bits[0]);
            column.tag.fill(tag);
            column.bits.fill(bits);
        }
        column
    }

    fn clear_payloads(&mut self) {
        self.text.clear();
        self.other.clear();
        self.lists.clear();
        self.items.clear();
    }

    /// Starts a list cell; add items with [`Self::push_item`].
    #[inline]
    fn begin_list(&mut self, row: usize) {
        self.lists.push((self.items.len() as u32, 0));
        self.set(row, Tag::List, (self.lists.len() - 1) as u64);
    }

    #[inline]
    fn push_item(&mut self, item: &str) {
        let start = self.text.len();
        self.text.push_str(item);
        self.items.push((start as u32, item.len() as u32));
        self.lists.last_mut().expect("a list was begun").1 += 1;
    }

    /// Items of a list cell.
    #[inline]
    fn list(&self, row: usize) -> impl ExactSizeIterator<Item = &str> + '_ {
        let (start, count) = self.lists[self.bits[row] as usize];
        self.items[start as usize..(start + count) as usize].iter().map(|(offset, length)| &self.text[*offset as usize..(*offset + *length) as usize])
    }

    /// Makes room for `rows` rows.
    #[inline]
    fn ensure(&mut self, rows: usize) {
        if self.tag.len() < rows {
            self.tag.resize(rows, Tag::Missing);
            self.bits.resize(rows, 0);
        }
    }

    /// A constant column covering `rows` rows.
    fn extend_constant(&mut self, rows: usize) {
        if self.tag.len() < rows {
            let (tag, bits) = (self.tag[0], self.bits[0]);
            self.tag.resize(rows, tag);
            self.bits.resize(rows, bits);
        }
    }

    #[inline]
    #[must_use]
    pub fn tag(&self, row: usize) -> Tag {
        self.tag[row]
    }

    #[inline]
    fn set(&mut self, row: usize, tag: Tag, bits: u64) {
        self.tag[row] = tag;
        self.bits[row] = bits;
    }

    #[inline]
    fn set_missing(&mut self, row: usize) {
        self.tag[row] = Tag::Missing;
    }

    #[inline]
    fn set_null(&mut self, row: usize) {
        self.tag[row] = Tag::Null;
    }

    #[inline]
    fn set_bool(&mut self, row: usize, value: bool) {
        self.set(row, Tag::Bool, u64::from(value));
    }

    #[inline]
    fn set_number(&mut self, row: usize, number: Number) {
        match number {
            Number::Signed(value) => self.set(row, Tag::Signed, value as u64),
            Number::Unsigned(value) => self.set(row, Tag::Unsigned, value),
            Number::Float(value) => self.set(row, Tag::Float, value.to_bits()),
        }
    }

    /// A float result: `Abort` when not finite, as `Value::float` fails.
    #[inline]
    fn set_float(&mut self, row: usize, value: f64) -> Step {
        if !value.is_finite() {
            return Err(Abort);
        }
        self.set(row, Tag::Float, if value == 0.0 { 0 } else { value.to_bits() });
        Ok(())
    }

    #[inline]
    fn set_str(&mut self, row: usize, value: &str) {
        let start = self.text.len();
        self.text.push_str(value);
        self.set(row, Tag::Str, ((start as u64) << 32) | value.len() as u64);
    }

    /// Marks the text written since `start` as the string of `row`.
    #[inline]
    fn end_str(&mut self, row: usize, start: usize) {
        let length = self.text.len() - start;
        self.set(row, Tag::Str, ((start as u64) << 32) | length as u64);
    }

    fn set_value(&mut self, row: usize, value: Value) {
        match value {
            Value::Null => self.set_null(row),
            Value::Bool(value) => self.set_bool(row, value),
            Value::Number(number) => self.set_number(row, number),
            Value::String(text) => self.set_str(row, &text),
            other => {
                self.other.push(other);
                self.set(row, Tag::Other, (self.other.len() - 1) as u64);
            }
        }
    }

    fn set_optional(&mut self, row: usize, value: Option<Value>) {
        match value {
            Some(value) => self.set_value(row, value),
            None => self.set_missing(row),
        }
    }

    /// Copies the cell `row` of `source`.
    #[inline]
    fn copy_cell(&mut self, row: usize, source: &Self) {
        self.copy_from(row, source, row);
    }

    /// Copies cell `source_row` of `source` into `row`.
    #[inline]
    fn copy_from(&mut self, row: usize, source: &Self, source_row: usize) {
        match source.tag[source_row] {
            Tag::Str => self.set_str(row, source.str(source_row)),
            Tag::List => {
                self.begin_list(row);
                for item in source.list(source_row) {
                    self.push_item(item);
                }
            }
            Tag::Other => self.set_value(row, source.other[source.bits[source_row] as usize].clone()),
            tag => self.set(row, tag, source.bits[source_row]),
        }
    }

    /// Loads one value read from storage.
    #[inline]
    fn load(&mut self, row: usize, value: Option<&ProjectedValueRef<'_>>) {
        match value {
            None => self.set_missing(row),
            Some(ProjectedValueRef::Null) => self.set_null(row),
            Some(ProjectedValueRef::Bool(value)) => self.set_bool(row, *value),
            Some(ProjectedValueRef::Signed(value)) => self.set(row, Tag::Signed, *value as u64),
            Some(ProjectedValueRef::Unsigned(value)) => self.set(row, Tag::Unsigned, *value),
            Some(ProjectedValueRef::Float(value)) => self.set(row, Tag::Float, value.to_bits()),
            Some(ProjectedValueRef::String(value)) => self.set_str(row, value),
            Some(ProjectedValueRef::Owned(value)) => self.set_value(row, value.clone()),
        }
    }

    #[inline]
    #[must_use]
    pub fn str(&self, row: usize) -> &str {
        let bits = self.bits[row];
        let start = (bits >> 32) as usize;
        &self.text[start..start + (bits & 0xffff_ffff) as usize]
    }

    #[inline]
    #[must_use]
    pub fn number(&self, row: usize) -> Option<Number> {
        match self.tag[row] {
            Tag::Signed => Some(Number::Signed(self.bits[row] as i64)),
            Tag::Unsigned => Some(Number::Unsigned(self.bits[row])),
            Tag::Float => Some(Number::Float(f64::from_bits(self.bits[row]))),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    pub fn bool(&self, row: usize) -> Option<bool> {
        (self.tag[row] == Tag::Bool).then_some(self.bits[row] != 0)
    }

    /// Order of two numeric cells as `compare` finds it, `None` when it
    /// needs a coercion that may fail (a signed against an unsigned, an
    /// integer that does not convert exactly to a float) or a cell is not a
    /// number.
    #[inline]
    fn number_order(&self, row: usize, other: &Self, other_row: usize) -> Option<std::cmp::Ordering> {
        let (x, y) = (self.bits[row], other.bits[other_row]);
        let exact_signed = |value: u64| { let value = value as i64; let converted = value as f64; (converted as i64 == value).then_some(converted) };
        let exact_unsigned = |value: u64| { let converted = value as f64; (converted as u64 == value).then_some(converted) };
        match (self.tag[row], other.tag[other_row]) {
            (Tag::Float, Tag::Float) => f64::from_bits(x).partial_cmp(&f64::from_bits(y)),
            (Tag::Signed, Tag::Signed) => Some((x as i64).cmp(&(y as i64))),
            (Tag::Unsigned, Tag::Unsigned) => Some(x.cmp(&y)),
            (Tag::Float, Tag::Signed) => f64::from_bits(x).partial_cmp(&exact_signed(y)?),
            (Tag::Signed, Tag::Float) => exact_signed(x)?.partial_cmp(&f64::from_bits(y)),
            (Tag::Float, Tag::Unsigned) => f64::from_bits(x).partial_cmp(&exact_unsigned(y)?),
            (Tag::Unsigned, Tag::Float) => exact_unsigned(x)?.partial_cmp(&f64::from_bits(y)),
            _ => None,
        }
    }

    /// The cell as a value; `None` when missing.
    #[must_use]
    pub fn value(&self, row: usize) -> Option<Value> {
        Some(match self.tag[row] {
            Tag::Missing => return None,
            Tag::Null => Value::Null,
            Tag::Bool => Value::Bool(self.bits[row] != 0),
            Tag::Signed | Tag::Unsigned | Tag::Float => Value::Number(self.number(row).expect("numeric tag")),
            Tag::Str => Value::string(self.str(row)),
            Tag::Other => self.other[self.bits[row] as usize].clone(),
            Tag::List => Value::array(self.list(row).map(Value::string)),
        })
    }

    /// The cell borrowed as storage hands values out; `None` when missing.
    #[inline]
    #[must_use]
    pub fn value_ref(&self, row: usize) -> Option<ProjectedValueRef<'_>> {
        Some(match self.tag[row] {
            Tag::Missing => return None,
            Tag::Null => ProjectedValueRef::Null,
            Tag::Bool => ProjectedValueRef::Bool(self.bits[row] != 0),
            Tag::Signed => ProjectedValueRef::Signed(self.bits[row] as i64),
            Tag::Unsigned => ProjectedValueRef::Unsigned(self.bits[row]),
            Tag::Float => ProjectedValueRef::Float(f64::from_bits(self.bits[row])),
            Tag::Str => ProjectedValueRef::String(self.str(row)),
            Tag::Other => ProjectedValueRef::Owned(self.other[self.bits[row] as usize].clone()),
            Tag::List => ProjectedValueRef::Owned(self.value(row).expect("present")),
        })
    }

    /// The array held by a cell.
    fn array(&self, row: usize) -> Option<&[Value]> {
        match self.tag[row] {
            Tag::Other => self.other[self.bits[row] as usize].as_array(),
            _ => None,
        }
    }
}

// ------------------------------------------------------------------ program

#[derive(Debug)]
enum Op {
    /// Reads the column `out` (a slot or a constant): nothing to compute.
    Column,
    Unary(UnaryOperator),
    Arith(BinaryOperator),
    Compare(BinaryOperator),
    And,
    Or,
    /// `in` (`false`) or `not in` (`true`).
    In(bool),
    Eager(Function),
    Case,
    If,
    Coalesce,
    Exists,
    Optional,
    Div,
    /// Evaluated row by row by the common evaluator, fields read from the
    /// slots visible where the expression appears.
    Row(Expression, Arc<[(Arc<str>, usize)]>),
}

#[derive(Debug)]
struct Node {
    op: Op,
    args: Vec<Node>,
    out: usize,
}

#[derive(Debug)]
enum Stage {
    Filter(Node),
    /// Values that must be present (derive, `select … as` expressions).
    Assign(Vec<Node>),
    /// `unwind`: each row becomes one row per item of `field`, visible
    /// columns copied to new columns (`carried`).
    Unwind { field: usize, item: usize, index: Option<usize>, keep_empty: bool, carried: Vec<(usize, usize)> },
}

/// Row-local stages compiled for batches ([`BatchRunner`]). Fields a stage
/// reads are resolved to columns while compiling.
#[derive(Debug)]
pub struct BatchProgram {
    sources: usize,
    columns: usize,
    constants: Vec<(usize, Column)>,
    stages: Vec<Stage>,
    /// Column of each output field, `None` when no row holds it.
    outputs: Vec<Option<usize>>,
}

struct Compiler {
    columns: usize,
    constants: Vec<(usize, Column)>,
    missing: usize,
    /// Visible top-level fields and their columns.
    scope: Vec<(Arc<str>, usize)>,
}

impl Compiler {
    fn column(&mut self) -> usize {
        self.columns += 1;
        self.columns - 1
    }

    fn constant(&mut self, value: &Value) -> usize {
        let column = self.column();
        self.constants.push((column, Column::broadcast(Some(value))));
        column
    }

    fn lookup(&self, name: &str) -> usize {
        self.scope.iter().rev().find(|(field, _)| field.as_ref() == name).map_or(self.missing, |(_, column)| *column)
    }

    fn bind(&mut self, name: &str, column: usize) {
        self.scope.retain(|(field, _)| field.as_ref() != name);
        self.scope.push((Arc::from(name), column));
    }

    fn leaf(&self, out: usize) -> Node {
        Node { op: Op::Column, args: Vec::new(), out }
    }

    fn row(&mut self, expression: &Expression) -> Node {
        let scope = Arc::from(self.scope.as_slice());
        Node { op: Op::Row(expression.clone(), scope), args: Vec::new(), out: self.column() }
    }

    fn computed(&mut self, op: Op, args: Vec<Node>) -> Node {
        Node { op, args, out: self.column() }
    }

    fn node(&mut self, expression: &Expression) -> Node {
        if let Some(value) = expression.constant() {
            let out = self.constant(value);
            return self.leaf(out);
        }
        match expression.view() {
            ExpressionView::Field(path) if path.len() == 1 => {
                let out = self.lookup(path.first());
                self.leaf(out)
            }
            ExpressionView::Group(inner) => self.node(inner),
            ExpressionView::Unary { operator, operand } => {
                let operand = self.node(operand);
                self.computed(Op::Unary(operator), vec![operand])
            }
            ExpressionView::Binary { left, operator, right } => {
                let op = match operator {
                    BinaryOperator::And => Op::And,
                    BinaryOperator::Or => Op::Or,
                    BinaryOperator::In => Op::In(false),
                    BinaryOperator::NotIn => Op::In(true),
                    operator if operator.is_comparison() => Op::Compare(operator),
                    operator => Op::Arith(operator),
                };
                let args = vec![self.node(left), self.node(right)];
                self.computed(op, args)
            }
            ExpressionView::Call { arguments, .. } => {
                let Some(function) = expression.function() else { return self.row(expression) };
                let op = match function {
                    Function::Case => Op::Case,
                    Function::If => Op::If,
                    Function::Coalesce => Op::Coalesce,
                    Function::Exists => Op::Exists,
                    Function::Optional => Op::Optional,
                    Function::Div => Op::Div,
                    // A volatile value is read row by row, as it is today.
                    Function::Now => return self.row(expression),
                    eager => Op::Eager(eager),
                };
                let args = arguments.iter().map(|argument| self.node(argument)).collect();
                self.computed(op, args)
            }
            // Nested paths, non-constant literals and arrays.
            _ => self.row(expression),
        }
    }
}

impl BatchProgram {
    /// Compiles `operators` over rows holding `source_fields` (top-level, in
    /// slot order), producing `output_fields` (the slot order of the matching
    /// [`super::SlotProgram`]). `None` when a stage cannot run on batches.
    #[must_use]
    pub fn compile(operators: &[PhysicalOperator], source_fields: &[ExpressionFieldPath], output_fields: &[ExpressionFieldPath]) -> Option<Self> {
        if source_fields.iter().any(|field| field.len() != 1 || field.first() == "_id") {
            return None;
        }
        let sources = source_fields.len();
        let mut compiler = Compiler { columns: sources + 1, constants: Vec::new(), missing: sources, scope: Vec::new() };
        compiler.constants.push((sources, Column::broadcast(None)));
        for (column, field) in source_fields.iter().enumerate() {
            compiler.bind(field.first(), column);
        }
        let mut stages = Vec::with_capacity(operators.len());
        for operator in operators {
            if operator.reads_document_id() {
                return None;
            }
            match operator {
                PhysicalOperator::Filter { predicate } => stages.push(Stage::Filter(compiler.node(predicate))),
                PhysicalOperator::Custom { writes: false, compiled: Some(stage), .. } if stage.unwind_operation().is_some() => {
                    let (field, index, keep_empty) = stage.unwind_operation().expect("checked above");
                    let source = compiler.lookup(field.first());
                    let mut carried = Vec::new();
                    for (name, column) in compiler.scope.clone() {
                        if column != compiler.missing && name.as_ref() != field.first() {
                            let copy = compiler.column();
                            carried.push((column, copy));
                            compiler.bind(&name, copy);
                        }
                    }
                    let item = compiler.column();
                    compiler.bind(field.first(), item);
                    let index = index.map(|index| {
                        let column = compiler.column();
                        compiler.bind(index.first(), column);
                        column
                    });
                    stages.push(Stage::Unwind { field: source, item, index, keep_empty, carried });
                }
                PhysicalOperator::Custom { writes: false, compiled: Some(stage), .. } => match stage.slot_operation()? {
                    SlotOperation::Derive(assignments) => {
                        let mut nodes = Vec::with_capacity(assignments.len());
                        for (target, expression) in assignments {
                            let node = compiler.node(expression);
                            compiler.bind(target.first(), node.out);
                            nodes.push(node);
                        }
                        stages.push(Stage::Assign(nodes));
                    }
                    SlotOperation::Select(items) => {
                        // A missing source field leaves an alias as it was:
                        // only an alias naming no visible field (or its own
                        // source) reads the same through a column binding.
                        let mut nodes = Vec::new();
                        let mut produced = Vec::with_capacity(items.len());
                        for (target, source) in items {
                            let column = match source {
                                Ok(field) => {
                                    if field.len() != 1 || (target.first() != field.first() && compiler.lookup(target.first()) != compiler.missing) {
                                        return None;
                                    }
                                    compiler.lookup(field.first())
                                }
                                Err(expression) => {
                                    let node = compiler.node(expression);
                                    let column = node.out;
                                    nodes.push(node);
                                    column
                                }
                            };
                            compiler.bind(target.first(), column);
                            produced.push((target.first(), column));
                        }
                        compiler.scope.clear();
                        for (target, column) in produced {
                            compiler.bind(target, column);
                        }
                        stages.push(Stage::Assign(nodes));
                    }
                    SlotOperation::Rename { source, target } => {
                        // A missing source leaves the target as it was.
                        if source.len() != 1 || (target.first() != source.first() && compiler.lookup(target.first()) != compiler.missing) {
                            return None;
                        }
                        let column = compiler.lookup(source.first());
                        let missing = compiler.missing;
                        compiler.bind(source.first(), missing);
                        compiler.bind(target.first(), column);
                    }
                    SlotOperation::Drop(paths) => {
                        let missing = compiler.missing;
                        for path in paths {
                            compiler.bind(path.first(), missing);
                        }
                    }
                },
                _ => return None,
            }
        }
        let outputs = output_fields
            .iter()
            .map(|field| Some(compiler.lookup(field.first())).filter(|column| *column != compiler.missing))
            .collect();
        Some(Self { sources, columns: compiler.columns, constants: compiler.constants, stages, outputs })
    }

    /// Output fields held by a column (`None`: no row holds the field).
    #[must_use]
    pub fn output_columns(&self) -> &[Option<usize>] {
        &self.outputs
    }
}

// ------------------------------------------------------------------ runner

/// Rows of one batch and the columns computed on them.
#[derive(Debug)]
pub struct BatchRunner {
    program: Arc<BatchProgram>,
    columns: Vec<Column>,
    ids: Vec<(DocumentId, DocumentVersion)>,
    rows: usize,
    selection: Vec<u32>,
    /// Loaded row of each row once an `unwind` ran (empty before).
    origin: Vec<u32>,
    arguments: Vec<Value>,
}

impl BatchRunner {
    #[must_use]
    pub fn new(program: Arc<BatchProgram>) -> Self {
        let mut columns = (0..program.columns).map(|_| Column::sized()).collect::<Vec<_>>();
        for (column, constant) in &program.constants {
            columns[*column] = constant.clone();
        }
        Self { program, columns, ids: Vec::with_capacity(BATCH_ROWS), rows: 0, selection: Vec::with_capacity(BATCH_ROWS), origin: Vec::new(), arguments: Vec::new() }
    }

    /// Rows loaded in the current batch.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows == 0
    }

    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.rows == BATCH_ROWS
    }

    /// Loads one source row; the batch must not be full.
    #[inline]
    pub fn push(&mut self, id: DocumentId, version: DocumentVersion, values: &[Option<ProjectedValueRef<'_>>]) {
        let row = self.rows;
        for (column, value) in self.columns[..self.program.sources].iter_mut().zip(values) {
            column.load(row, value.as_ref());
        }
        self.ids.push((id, version));
        self.rows += 1;
    }

    /// Empties the batch.
    pub fn clear(&mut self) {
        self.rows = 0;
        self.ids.clear();
        for column in &mut self.columns[..self.program.sources] {
            column.clear_payloads();
        }
    }

    /// Runs every stage; afterwards [`Self::selection`] holds the rows that
    /// passed every filter, in load order.
    pub fn run(&mut self) -> Step {
        self.selection.clear();
        self.selection.extend(0..self.rows as u32);
        self.origin.clear();
        let program = Arc::clone(&self.program);
        for stage in &program.stages {
            if self.selection.is_empty() {
                break;
            }
            match stage {
                Stage::Filter(node) => {
                    let selection = std::mem::take(&mut self.selection);
                    let evaluated = eval(node, &mut self.columns, &selection, &mut self.arguments);
                    self.selection = selection;
                    evaluated?;
                    let column = &self.columns[node.out];
                    let mut kept = 0;
                    for position in 0..self.selection.len() {
                        let row = self.selection[position] as usize;
                        match column.bool(row) {
                            Some(true) => {
                                self.selection[kept] = row as u32;
                                kept += 1;
                            }
                            Some(false) => {}
                            None => return Err(Abort),
                        }
                    }
                    self.selection.truncate(kept);
                }
                Stage::Assign(nodes) => {
                    for node in nodes {
                        eval(node, &mut self.columns, &self.selection, &mut self.arguments)?;
                        let column = &self.columns[node.out];
                        if self.selection.iter().any(|row| column.tag(*row as usize) == Tag::Missing) {
                            return Err(Abort);
                        }
                    }
                }
                Stage::Unwind { field, item, index, keep_empty, carried } => self.unwind(*field, *item, *index, *keep_empty, carried),
            }
        }
        Ok(())
    }

    /// One row per item of `field` for each selected row, with the same
    /// outcome as [`super::CompiledStage::expand_slots`]: an empty array (or
    /// `null` under `keep empty`) keeps the row with the field and index
    /// `null` under `keep empty`, drops it otherwise; any other value keeps
    /// the row, its index `null`.
    fn unwind(&mut self, field: usize, item: usize, index: Option<usize>, keep_empty: bool, carried: &[(usize, usize)]) {
        let mut parents = Vec::with_capacity(self.selection.len());
        let mut items = std::mem::take(&mut self.columns[item]);
        let mut positions = index.map(|index| std::mem::take(&mut self.columns[index]));
        items.clear_payloads();
        if let Some(positions) = &mut positions {
            positions.clear_payloads();
        }
        {
            let source = &self.columns[field];
            for &row in &self.selection {
                let row = row as usize;
                if source.tag(row) == Tag::List && source.list(row).len() > 0 {
                    for (position, value) in source.list(row).enumerate() {
                        let target = parents.len();
                        parents.push(row as u32);
                        items.ensure(target + 1);
                        items.set_str(target, value);
                        if let Some(positions) = &mut positions {
                            positions.ensure(target + 1);
                            positions.set(target, Tag::Unsigned, position as u64);
                        }
                    }
                    continue;
                }
                let empty_list = source.tag(row) == Tag::List;
                let array = if empty_list { Some(&[][..]) } else { source.array(row) };
                match array {
                    Some(values) if !values.is_empty() => {
                        for (position, value) in values.iter().enumerate() {
                            let target = parents.len();
                            parents.push(row as u32);
                            items.ensure(target + 1);
                            items.set_value(target, value.clone());
                            if let Some(positions) = &mut positions {
                                positions.ensure(target + 1);
                                positions.set(target, Tag::Unsigned, position as u64);
                            }
                        }
                    }
                    Some(_) if !keep_empty => {}
                    _ => {
                        let target = parents.len();
                        parents.push(row as u32);
                        items.ensure(target + 1);
                        let empty = keep_empty && (array.is_some() || source.tag(row) == Tag::Null);
                        if empty { items.set_null(target) } else { items.copy_from(target, source, row) }
                        if let Some(positions) = &mut positions {
                            positions.ensure(target + 1);
                            positions.set_null(target);
                        }
                    }
                }
            }
        }
        self.columns[item] = items;
        if let (Some(index), Some(positions)) = (index, positions) {
            self.columns[index] = positions;
        }
        for &(from, to) in carried {
            let mut copy = std::mem::take(&mut self.columns[to]);
            copy.clear_payloads();
            copy.ensure(parents.len());
            for (target, &parent) in parents.iter().enumerate() {
                copy.copy_from(target, &self.columns[from], parent as usize);
            }
            self.columns[to] = copy;
        }
        let rows = parents.len();
        for (column, _) in &self.program.constants {
            self.columns[*column].extend_constant(rows);
        }
        self.origin = if self.origin.is_empty() { parents } else { parents.iter().map(|parent| self.origin[*parent as usize]).collect() };
        self.selection.clear();
        self.selection.extend(0..rows as u32);
    }

    /// Loaded rows none of whose rows passed.
    #[must_use]
    pub fn dropped(&self) -> usize {
        if self.origin.is_empty() {
            return self.rows - self.selection.len();
        }
        let mut kept = vec![false; self.rows];
        for &row in &self.selection {
            kept[self.origin[row as usize] as usize] = true;
        }
        kept.iter().filter(|kept| !**kept).count()
    }

    /// Rows that passed every filter.
    #[must_use]
    pub fn selection(&self) -> &[u32] {
        &self.selection
    }

    /// Identifier and version of the loaded row a row comes from.
    #[must_use]
    pub fn id(&self, row: usize) -> (DocumentId, DocumentVersion) {
        self.ids[if self.origin.is_empty() { row } else { self.origin[row] as usize }]
    }

    /// Column holding output field `index`, `None` when no row holds it.
    #[must_use]
    pub fn output(&self, index: usize) -> Option<&Column> {
        self.program.outputs[index].map(|column| &self.columns[column])
    }

    /// Output fields of `row`, in output order.
    pub fn output_values(&self, row: usize, values: &mut Vec<Option<Value>>) {
        values.clear();
        values.extend(self.program.outputs.iter().map(|column| column.and_then(|column| self.columns[column].value(row))));
    }

    /// Source fields of loaded row `row` as read from storage, in source order.
    pub fn source_values(&self, row: usize, values: &mut Vec<Option<Value>>) {
        values.clear();
        values.extend(self.columns[..self.program.sources].iter().map(|column| column.value(row)));
    }
}

// ------------------------------------------------------------------ evaluation

/// Fields of one batch row, for expressions evaluated row by row.
struct RowFields<'a> {
    columns: &'a [Column],
    scope: &'a [(Arc<str>, usize)],
    row: usize,
}

impl ExpressionFieldResolver<Value> for RowFields<'_> {
    fn resolve_field(&self, field: &ExpressionFieldPath) -> SemanticValue<Value> {
        let Some((_, column)) = self.scope.iter().rev().find(|(name, _)| name.as_ref() == field.first()) else {
            return SemanticValue::Missing;
        };
        let Some(mut value) = self.columns[*column].value(self.row) else {
            return SemanticValue::Missing;
        };
        for segment in field.iter().skip(1) {
            match value.as_object().and_then(|object| object.get(segment)) {
                Some(inner) => value = inner.clone(),
                None => return SemanticValue::Missing,
            }
        }
        SemanticValue::Present(value)
    }
}

/// Writes a result computed by the common evaluator.
#[inline]
fn reference(out: &mut Column, row: usize, result: super::ExecutionResult<Value>) -> Step {
    out.set_value(row, result.map_err(|_| Abort)?);
    Ok(())
}

/// The value of a cell an operator needs: a missing operand fails.
#[inline]
fn present(column: &Column, row: usize) -> Result<Value, Abort> {
    column.value(row).ok_or(Abort)
}

fn eval(node: &Node, columns: &mut [Column], selection: &[u32], arguments: &mut Vec<Value>) -> Step {
    match &node.op {
        Op::Column => return Ok(()),
        Op::And | Op::Or | Op::Case | Op::If | Op::Coalesce | Op::Div => {}
        _ => {
            for argument in &node.args {
                eval(argument, columns, selection, arguments)?;
            }
        }
    }
    let mut out = std::mem::take(&mut columns[node.out]);
    out.ensure(selection.last().map_or(0, |row| *row as usize + 1));
    out.clear_payloads();
    let result = compute(node, &mut out, columns, selection, arguments);
    columns[node.out] = out;
    result
}

fn compute(node: &Node, out: &mut Column, columns: &mut [Column], selection: &[u32], arguments: &mut Vec<Value>) -> Step {
    let arg = |index: usize| node.args[index].out;
    match &node.op {
        Op::Column => unreachable!("columns are read in place"),
        Op::Unary(operator) => {
            let a = &columns[arg(0)];
            for &row in selection {
                let row = row as usize;
                match (operator, a.tag(row)) {
                    (UnaryOperator::Not, Tag::Bool) => out.set_bool(row, a.bits[row] == 0),
                    (UnaryOperator::Negate, Tag::Float) => out.set_float(row, -f64::from_bits(a.bits[row]))?,
                    _ => reference(out, row, unary(*operator, &present(a, row)?))?,
                }
            }
        }
        Op::Arith(operator) => {
            let (a, b) = (&columns[arg(0)], &columns[arg(1)]);
            if let (Some(x), Some(y)) = (all_floats(a, selection), all_floats(b, selection)) {
                // Float operands only: one tight loop.
                return float_arith(*operator, x, y, out, selection);
            }
            for &row in selection {
                let row = row as usize;
                match (a.number(row), b.number(row)) {
                    (Some(x), Some(y)) => out.set_number(row, arith(*operator, x, y).ok_or(Abort)?),
                    _ => reference(out, row, arithmetic_value(*operator, &present(a, row)?, &present(b, row)?))?,
                }
            }
        }
        Op::Compare(operator) => {
            let (a, b) = (&columns[arg(0)], &columns[arg(1)]);
            for &row in selection {
                let row = row as usize;
                let ordering = match (a.tag(row), b.tag(row)) {
                    (Tag::Str, Tag::Str) => Some(a.str(row).cmp(b.str(row))),
                    _ => a.number_order(row, b, row),
                };
                match ordering {
                    Some(ordering) => out.set_bool(row, ordered(*operator, ordering)),
                    None => reference(out, row, binary(*operator, &present(a, row)?, &present(b, row)?))?,
                }
            }
        }
        Op::And | Op::Or => {
            let short_circuit = matches!(node.op, Op::Or);
            eval(&node.args[0], columns, selection, arguments)?;
            let mut rest = Vec::with_capacity(selection.len());
            {
                let left = &columns[arg(0)];
                for &row in selection {
                    let value = left.bool(row as usize).ok_or(Abort)?;
                    if value == short_circuit {
                        out.set_bool(row as usize, short_circuit);
                    } else {
                        rest.push(row);
                    }
                }
            }
            if !rest.is_empty() {
                eval(&node.args[1], columns, &rest, arguments)?;
                let right = &columns[arg(1)];
                for &row in &rest {
                    out.set_bool(row as usize, right.bool(row as usize).ok_or(Abort)?);
                }
            }
        }
        Op::In(negated) => {
            let (a, b) = (&columns[arg(0)], &columns[arg(1)]);
            for &row in selection {
                let row = row as usize;
                match (a.tag(row), b.array(row)) {
                    (Tag::Str, Some(items)) => {
                        let text = a.str(row);
                        let found = items.iter().any(|item| item.as_str() == Some(text));
                        out.set_bool(row, found != *negated);
                    }
                    _ => {
                        let operator = if *negated { BinaryOperator::NotIn } else { BinaryOperator::In };
                        reference(out, row, binary(operator, &present(a, row)?, &present(b, row)?))?;
                    }
                }
            }
        }
        Op::Exists => {
            let a = &columns[arg(0)];
            for &row in selection {
                out.set_bool(row as usize, a.tag(row as usize) != Tag::Missing);
            }
        }
        Op::Optional => {
            let a = &columns[arg(0)];
            for &row in selection {
                let row = row as usize;
                if a.tag(row) == Tag::Missing { out.set_null(row) } else { out.copy_cell(row, a) }
            }
        }
        Op::Coalesce => {
            let mut pending = selection.to_vec();
            for argument in &node.args {
                if pending.is_empty() {
                    break;
                }
                eval(argument, columns, &pending, arguments)?;
                let column = &columns[argument.out];
                pending.retain(|&row| {
                    let row = row as usize;
                    if matches!(column.tag(row), Tag::Missing | Tag::Null) {
                        true
                    } else {
                        out.copy_cell(row, column);
                        false
                    }
                });
            }
            for row in pending {
                out.set_null(row as usize);
            }
        }
        Op::Case => {
            let mut pending = selection.to_vec();
            let mut chosen = Vec::new();
            for pair in node.args.chunks(2) {
                if pending.is_empty() {
                    break;
                }
                match pair {
                    [condition, value] => {
                        eval(condition, columns, &pending, arguments)?;
                        chosen.clear();
                        let mut kept = Vec::with_capacity(pending.len());
                        for &row in &pending {
                            if columns[condition.out].bool(row as usize).ok_or(Abort)? { chosen.push(row) } else { kept.push(row) }
                        }
                        pending = kept;
                        if !chosen.is_empty() {
                            eval(value, columns, &chosen, arguments)?;
                            for &row in &chosen {
                                out.copy_cell(row as usize, &columns[value.out]);
                            }
                        }
                    }
                    [default] => {
                        eval(default, columns, &pending, arguments)?;
                        for &row in &pending {
                            out.copy_cell(row as usize, &columns[default.out]);
                        }
                        pending.clear();
                    }
                    _ => unreachable!("chunks of two"),
                }
            }
            for row in pending {
                out.set_null(row as usize);
            }
        }
        Op::If => {
            let condition = &node.args[0];
            eval(condition, columns, selection, arguments)?;
            let (mut yes, mut no) = (Vec::new(), Vec::new());
            for &row in selection {
                if columns[condition.out].bool(row as usize).ok_or(Abort)? { yes.push(row) } else { no.push(row) }
            }
            for (branch, rows) in [(&node.args[1], yes), (&node.args[2], no)] {
                if !rows.is_empty() {
                    eval(branch, columns, &rows, arguments)?;
                    for &row in &rows {
                        out.copy_cell(row as usize, &columns[branch.out]);
                    }
                }
            }
        }
        Op::Div => {
            eval(&node.args[0], columns, selection, arguments)?;
            eval(&node.args[1], columns, selection, arguments)?;
            let mut zero = Vec::new();
            {
                let (a, b) = (&columns[arg(0)], &columns[arg(1)]);
                for &row in selection {
                    let row = row as usize;
                    if a.tag(row) == Tag::Missing || b.tag(row) == Tag::Missing {
                        return Err(Abort);
                    }
                    match (a.number(row), b.number(row)) {
                        (_, Some(divisor)) if float_of(divisor) == 0.0 => zero.push(row as u32),
                        (Some(x), Some(y)) => out.set_number(row, arith(BinaryOperator::Divide, x, y).ok_or(Abort)?),
                        _ => reference(out, row, arithmetic_value(BinaryOperator::Divide, &present(a, row)?, &present(b, row)?))?,
                    }
                }
            }
            if !zero.is_empty() {
                match node.args.get(2) {
                    Some(fallback) => {
                        eval(fallback, columns, &zero, arguments)?;
                        for &row in &zero {
                            out.copy_cell(row as usize, &columns[fallback.out]);
                        }
                    }
                    None => zero.iter().for_each(|&row| out.set_null(row as usize)),
                }
            }
        }
        Op::Eager(function) => {
            for &row in selection {
                let row = row as usize;
                if !eager_fast(*function, node, columns, out, row)? {
                    arguments.clear();
                    for argument in &node.args {
                        arguments.push(present(&columns[argument.out], row)?);
                    }
                    reference(out, row, super::functions::call(*function, arguments))?;
                }
            }
        }
        Op::Row(expression, scope) => {
            for &row in selection {
                let row = row as usize;
                let fields = RowFields { columns, scope, row };
                out.set_optional(row, evaluate_optional(expression, &fields).map_err(|_| Abort)?);
            }
        }
    }
    Ok(())
}

#[inline]
fn ordered(operator: BinaryOperator, ordering: std::cmp::Ordering) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    match operator {
        BinaryOperator::Equal => ordering == Equal,
        BinaryOperator::NotEqual => ordering != Equal,
        BinaryOperator::LessThan => ordering == Less,
        BinaryOperator::LessThanOrEqual => ordering != Greater,
        BinaryOperator::GreaterThan => ordering == Greater,
        _ => ordering != Less,
    }
}

/// Float values of the selected rows when every one is a float.
#[inline]
fn all_floats<'a>(column: &'a Column, selection: &[u32]) -> Option<&'a [u64]> {
    selection.iter().all(|&row| column.tag[row as usize] == Tag::Float).then_some(&column.bits)
}

fn float_arith(operator: BinaryOperator, x: &[u64], y: &[u64], out: &mut Column, selection: &[u32]) -> Step {
    for &row in selection {
        let row = row as usize;
        let (x, y) = (f64::from_bits(x[row]), f64::from_bits(y[row]));
        let value = match operator {
            BinaryOperator::Add => x + y,
            BinaryOperator::Subtract => x - y,
            BinaryOperator::Multiply => x * y,
            _ => {
                if y == 0.0 {
                    return Err(Abort);
                }
                if operator == BinaryOperator::Divide { x / y } else { x % y }
            }
        };
        out.set_float(row, value)?;
    }
    Ok(())
}

/// [`arithmetic_value`] on two numbers; `None` where it fails.
#[inline]
fn arith(operator: BinaryOperator, a: Number, b: Number) -> Option<Number> {
    if matches!(operator, BinaryOperator::Divide | BinaryOperator::Remainder) && float_of(b) == 0.0 {
        return None;
    }
    if let (Some(x), Some(y), false) = (integer_of(a), integer_of(b), operator == BinaryOperator::Divide) {
        let result = match operator {
            BinaryOperator::Add => x.checked_add(y),
            BinaryOperator::Subtract => x.checked_sub(y),
            BinaryOperator::Multiply => x.checked_mul(y),
            _ => x.checked_rem(y),
        };
        if let Some(result) = result {
            return Some(Number::Signed(result));
        }
    }
    let (x, y) = (float_of(a), float_of(b));
    let value = match operator {
        BinaryOperator::Add => x + y,
        BinaryOperator::Subtract => x - y,
        BinaryOperator::Multiply => x * y,
        BinaryOperator::Divide => x / y,
        _ => x % y,
    };
    value.is_finite().then(|| Number::Float(if value == 0.0 { 0.0 } else { value }))
}

/// `integral` of the evaluator: an integer when the value fits exactly.
#[inline]
fn integral(out: &mut Column, row: usize, value: f64) -> Step {
    if value.is_finite() && value.abs() < 9.0e15 {
        out.set(row, Tag::Signed, (value as i64) as u64);
        Ok(())
    } else {
        out.set_float(row, value)
    }
}

/// A non-negative integer argument (`count` of the functions module).
#[inline]
fn count_of(column: &Column, row: usize) -> Option<usize> {
    column.number(row).and_then(integer_of).and_then(|value| usize::try_from(value).ok())
}

/// Characters of a string (bytes when ASCII).
#[inline]
fn char_len(text: &str) -> usize {
    if text.is_ascii() { text.len() } else { text.chars().count() }
}

/// Byte offset of character `position` (the end when past it).
#[inline]
fn char_byte(text: &str, position: usize) -> usize {
    if text.is_ascii() { position.min(text.len()) } else { text.char_indices().nth(position).map_or(text.len(), |(byte, _)| byte) }
}

/// JSON text of a scalar, as `value_to_json(…).to_string()` writes it.
fn write_json_scalar(out: &mut String, column: &Column, row: usize) -> bool {
    match column.tag(row) {
        Tag::Bool => out.push_str(if column.bits[row] != 0 { "true" } else { "false" }),
        Tag::Signed => { let _ = write!(out, "{}", column.bits[row] as i64); }
        Tag::Unsigned => { let _ = write!(out, "{}", column.bits[row]); }
        Tag::Float => match serde_json::Number::from_f64(f64::from_bits(column.bits[row])) {
            Some(number) => { let _ = write!(out, "{number}"); }
            None => out.push_str("null"),
        },
        _ => return false,
    }
    true
}

/// Epoch milliseconds of a timestamp cell, as the functions module reads
/// it; `None` for a kind only the functions module handles.
#[inline]
fn timestamp(column: &Column, row: usize) -> Option<Result<i64, Abort>> {
    Some(match column.tag(row) {
        Tag::Signed | Tag::Unsigned | Tag::Float => match column.number(row).expect("numeric tag") {
            number => match integer_of(number) {
                Some(millis) => Ok(millis),
                None => {
                    let millis = float_of(number).floor();
                    if millis.is_finite() && millis.abs() < 9.0e15 { Ok(millis as i64) } else { Err(Abort) }
                }
            },
        },
        Tag::Str => super::dates::parse_iso(column.str(row)).map_err(|_| Abort),
        _ => return None,
    })
}

/// `bucket` of the functions module on borrowed bounds.
fn bucket<'a>(value: &Value, bounds: &'a [Value]) -> Result<Option<&'a Value>, Abort> {
    use crate::model::{compare, Comparison};
    if bounds.len() < 2 {
        return Err(Abort);
    }
    for pair in bounds.windows(2) {
        if compare(&pair[0], &pair[1], super::eval::POLICY).map_err(|_| Abort)? != Comparison::Less {
            return Err(Abort);
        }
    }
    let not_below = |bound: &Value| compare(value, bound, super::eval::POLICY).map(|comparison| comparison != Comparison::Less);
    let failed = std::cell::Cell::new(false);
    let above = bounds.partition_point(|bound| not_below(bound).unwrap_or_else(|_| { failed.set(true); false }));
    Ok(if failed.get() {
        bounds.windows(2).find(|pair| not_below(&pair[0]).unwrap_or(false) && not_below(&pair[1]).is_ok_and(|not_below| !not_below)).map(|pair| &pair[0])
    } else {
        (above > 0 && above < bounds.len()).then(|| &bounds[above - 1])
    })
}

/// Fast kernels of eager functions for the common operand kinds; `false`
/// when the row must go through [`super::functions::call`].
#[allow(clippy::too_many_lines)]
fn eager_fast(function: Function, node: &Node, columns: &[Column], out: &mut Column, row: usize) -> Result<bool, Abort> {
    let column = |index: usize| node.args.get(index).map(|argument| &columns[argument.out]);
    let a = column(0).expect("an eager function has an argument");
    match function {
        Function::Between => {
            let (low, high) = (column(1).expect("between bounds"), column(2).expect("between bounds"));
            // `low <= value` decides first; `value <= high` only when it holds.
            if let Some(below) = low.number_order(row, a, row) {
                if below == std::cmp::Ordering::Greater {
                    out.set_bool(row, false);
                } else {
                    let Some(above) = a.number_order(row, high, row) else { return Ok(false) };
                    out.set_bool(row, above != std::cmp::Ordering::Greater);
                }
            } else if (a.tag(row), low.tag(row), high.tag(row)) == (Tag::Str, Tag::Str, Tag::Str) {
                let value = a.str(row);
                out.set_bool(row, low.str(row) <= value && value <= high.str(row));
            } else {
                return Ok(false);
            }
        }
        Function::Round | Function::Trunc | Function::Floor | Function::Ceil => {
            let digits = match column(1) {
                None => 0,
                Some(digits) => match digits.number(row).and_then(integer_of) {
                    Some(digits) if (0..=15).contains(&digits) => digits,
                    _ => return Ok(false),
                },
            };
            match a.tag(row) {
                Tag::Signed | Tag::Unsigned => out.set(row, a.tag(row), a.bits[row]),
                Tag::Float => {
                    let scale = 10_f64.powi(digits as i32);
                    let scaled = f64::from_bits(a.bits[row]) * scale;
                    let rounded = match function {
                        Function::Floor => scaled.floor(),
                        Function::Ceil => scaled.ceil(),
                        Function::Trunc => scaled.trunc(),
                        _ => scaled.round(),
                    };
                    if digits == 0 { integral(out, row, rounded)? } else { out.set_float(row, rounded / scale)? }
                }
                _ => return Ok(false),
            }
        }
        Function::Sign => {
            let Some(number) = a.number(row) else { return Ok(false) };
            let value = float_of(number);
            out.set(row, Tag::Signed, (if value > 0.0 { 1_i64 } else if value < 0.0 { -1 } else { 0 }) as u64);
        }
        Function::Abs => match a.tag(row) {
            Tag::Float => out.set_float(row, f64::from_bits(a.bits[row]).abs())?,
            Tag::Signed => match (a.bits[row] as i64).checked_abs() {
                Some(value) => out.set(row, Tag::Signed, value as u64),
                None => return Ok(false),
            },
            _ => return Ok(false),
        },
        Function::ToInt => match a.tag(row) {
            Tag::Null => out.set_null(row),
            Tag::Bool => out.set(row, Tag::Signed, a.bits[row]),
            Tag::Signed => out.set(row, Tag::Signed, a.bits[row]),
            Tag::Float => integral(out, row, f64::from_bits(a.bits[row]).trunc())?,
            Tag::Str => {
                let text = a.str(row).trim();
                match text.parse::<i64>() {
                    Ok(value) => out.set(row, Tag::Signed, value as u64),
                    Err(_) => {
                        let parsed = text.parse::<f64>().ok().filter(|value| value.is_finite()).ok_or(Abort)?;
                        integral(out, row, parsed.trunc())?;
                    }
                }
            }
            _ => return Ok(false),
        },
        Function::ToFloat => match a.tag(row) {
            Tag::Null => out.set_null(row),
            Tag::Signed | Tag::Unsigned | Tag::Float => out.set_float(row, float_of(a.number(row).expect("numeric tag")))?,
            Tag::Str => match a.str(row).trim().parse::<f64>() {
                Ok(parsed) if parsed.is_finite() => out.set_float(row, parsed)?,
                _ => return Err(Abort),
            },
            _ => return Ok(false),
        },
        Function::ToString => match a.tag(row) {
            Tag::Null => out.set_null(row),
            Tag::Str => out.set_str(row, a.str(row)),
            _ => {
                let start = out.text.len();
                if !write_json_scalar(&mut out.text, a, row) {
                    out.text.truncate(start);
                    return Ok(false);
                }
                out.end_str(row, start);
            }
        },
        Function::Len if a.tag(row) == Tag::Str => out.set(row, Tag::Unsigned, char_len(a.str(row)) as u64),
        Function::Len if a.tag(row) == Tag::List => out.set(row, Tag::Unsigned, a.list(row).len() as u64),
        Function::At if a.tag(row) == Tag::List => {
            let Some(index) = column(1).expect("at index").number(row).and_then(integer_of) else { return Ok(false) };
            let length = a.list(row).len();
            let position = if index < 0 { length as i64 + index } else { index };
            match usize::try_from(position).ok().filter(|position| *position < length) {
                Some(position) => {
                    let item = a.list(row).nth(position).expect("in range");
                    out.set_str(row, item);
                }
                None => out.set_null(row),
            }
        }
        Function::Contains if a.tag(row) == Tag::List => {
            let needle = column(1).expect("contains needle");
            if needle.tag(row) != Tag::Str {
                return Ok(false);
            }
            let needle = needle.str(row);
            out.set_bool(row, a.list(row).any(|item| item == needle));
        }
        Function::Upper | Function::Lower => {
            if a.tag(row) != Tag::Str {
                return Ok(false);
            }
            let text = a.str(row);
            let start = out.text.len();
            if text.is_ascii() {
                out.text.push_str(text);
                let written = &mut out.text[start..];
                if function == Function::Upper { written.make_ascii_uppercase() } else { written.make_ascii_lowercase() }
            } else if function == Function::Upper {
                out.text.push_str(&text.to_uppercase());
            } else {
                out.text.push_str(&text.to_lowercase());
            }
            out.end_str(row, start);
        }
        Function::Trim | Function::Ltrim | Function::Rtrim => {
            if a.tag(row) != Tag::Str {
                return Ok(false);
            }
            let text = a.str(row);
            let trimmed = match function { Function::Trim => text.trim(), Function::Ltrim => text.trim_start(), _ => text.trim_end() };
            out.set_str(row, trimmed);
        }
        Function::Substr => {
            let start = column(1).expect("substr start");
            let (Tag::Str, Some(first)) = (a.tag(row), start.number(row).and_then(integer_of)) else { return Ok(false) };
            let text = a.str(row);
            let len = char_len(text) as i64;
            let first = if first < 0 { (len + first).max(0) } else { first.min(len) };
            let last = match column(2) {
                Some(length) => match count_of(length, row) {
                    Some(length) => first.saturating_add(length as i64).min(len),
                    None => return Ok(false),
                },
                None => len,
            };
            let (from, to) = (char_byte(text, first as usize), char_byte(text, last as usize));
            out.set_str(row, &text[from..to]);
        }
        Function::Replace => {
            let (from, to) = (column(1).expect("replace search"), column(2).expect("replace replacement"));
            if (a.tag(row), from.tag(row), to.tag(row)) != (Tag::Str, Tag::Str, Tag::Str) || from.str(row).is_empty() {
                return Ok(false);
            }
            let (text, from, to) = (a.str(row), from.str(row), to.str(row));
            let start = out.text.len();
            let mut last = 0;
            let mut push = |position: usize, out: &mut Column| {
                out.text.push_str(&text[last..position]);
                out.text.push_str(to);
                last = position + from.len();
            };
            // One character searches faster as a `char` pattern.
            let mut characters = from.chars();
            match (characters.next(), characters.next()) {
                (Some(character), None) => text.match_indices(character).for_each(|(position, _)| push(position, out)),
                _ => text.match_indices(from).for_each(|(position, _)| push(position, out)),
            }
            out.text.push_str(&text[last..]);
            out.end_str(row, start);
        }
        Function::PadLeft | Function::PadRight => {
            let width = column(1).expect("pad width");
            let (Tag::Str, Some(width)) = (a.tag(row), count_of(width, row)) else { return Ok(false) };
            let fill = match column(2) {
                None => ' ',
                Some(fill) => {
                    if fill.tag(row) != Tag::Str {
                        return Ok(false);
                    }
                    let mut characters = fill.str(row).chars();
                    match (characters.next(), characters.next()) {
                        (Some(character), None) => character,
                        _ => return Ok(false),
                    }
                }
            };
            let text = a.str(row);
            let padding = width.saturating_sub(char_len(text));
            let start = out.text.len();
            if function == Function::PadRight {
                out.text.push_str(text);
            }
            for _ in 0..padding {
                out.text.push(fill);
            }
            if function == Function::PadLeft {
                out.text.push_str(text);
            }
            out.end_str(row, start);
        }
        Function::Contains | Function::StartsWith | Function::EndsWith => {
            let needle = column(1).expect("text needle");
            if (a.tag(row), needle.tag(row)) != (Tag::Str, Tag::Str) {
                return Ok(false);
            }
            let (text, needle) = (a.str(row), needle.str(row));
            out.set_bool(row, match function {
                Function::Contains => text.contains(needle),
                Function::StartsWith => text.starts_with(needle),
                _ => text.ends_with(needle),
            });
        }
        Function::Concat => {
            let start = out.text.len();
            for argument in &node.args {
                let column = &columns[argument.out];
                match column.tag(row) {
                    Tag::Str => out.text.push_str(column.str(row)),
                    Tag::Null => {}
                    Tag::Missing => return Err(Abort),
                    Tag::Other | Tag::List => {
                        out.text.truncate(start);
                        return Ok(false);
                    }
                    _ => {
                        write_json_scalar(&mut out.text, column, row);
                    }
                }
            }
            out.end_str(row, start);
        }
        Function::Len if a.tag(row) == Tag::Other => {
            let value = &a.other[a.bits[row] as usize];
            let length = match value {
                Value::Array(items) => items.len(),
                Value::Object(object) => object.len(),
                _ => return Ok(false),
            };
            out.set(row, Tag::Unsigned, length as u64);
        }
        Function::At => {
            let (Some(items), Some(index)) = (a.array(row), column(1).expect("at index").number(row).and_then(integer_of)) else { return Ok(false) };
            let position = if index < 0 { items.len() as i64 + index } else { index };
            match usize::try_from(position).ok().and_then(|position| items.get(position)) {
                Some(item) => out.set_value(row, item.clone()),
                None => out.set_null(row),
            }
        }
        Function::Split => {
            let separator = column(1).expect("split separator");
            if (a.tag(row), separator.tag(row)) != (Tag::Str, Tag::Str) {
                return Ok(false);
            }
            let (text, separator) = (a.str(row), separator.str(row));
            out.begin_list(row);
            if separator.is_empty() {
                let mut buffer = [0u8; 4];
                for character in text.chars() {
                    out.push_item(character.encode_utf8(&mut buffer));
                }
            } else {
                for item in text.split(separator) {
                    out.push_item(item);
                }
            }
        }
        Function::Has => {
            let key = column(1).expect("has key");
            let (Tag::Other, Tag::Str) = (a.tag(row), key.tag(row)) else { return Ok(false) };
            let Some(object) = a.other[a.bits[row] as usize].as_object() else { return Ok(false) };
            out.set_bool(row, object.contains_key(key.str(row)));
        }
        Function::Bucket => {
            let Some(bounds) = column(1).expect("bucket bounds").array(row) else { return Ok(false) };
            if !matches!(a.tag(row), Tag::Null | Tag::Bool | Tag::Signed | Tag::Unsigned | Tag::Float) {
                return Ok(false);
            }
            let value = a.value(row).expect("present");
            match bucket(&value, bounds)? {
                Some(bound) => out.set_value(row, bound.clone()),
                None => match column(2) {
                    Some(default) if default.tag(row) != Tag::Missing => out.copy_cell(row, default),
                    Some(_) => return Err(Abort),
                    None => out.set_null(row),
                },
            }
        }
        Function::ParseDate => {
            let Some(millis) = timestamp(a, row) else { return Ok(false) };
            out.set(row, Tag::Signed, millis? as u64);
        }
        Function::DateTrunc | Function::DatePart | Function::DateFormat => {
            let Some(millis) = timestamp(a, row) else { return Ok(false) };
            let millis = millis?;
            match (function, column(1)) {
                (Function::DateFormat, None) => out.set_str(row, &super::dates::to_iso(millis)),
                (_, Some(text)) if text.tag(row) == Tag::Str => {
                    let text = text.str(row);
                    match function {
                        Function::DateTrunc => {
                            let unit = super::dates::DateUnit::parse(text).map_err(|_| Abort)?;
                            out.set(row, Tag::Signed, super::dates::trunc(millis, unit) as u64);
                        }
                        Function::DatePart => out.set(row, Tag::Signed, super::dates::part(millis, text).map_err(|_| Abort)? as u64),
                        _ => {
                            let formatted = super::dates::format(millis, text).map_err(|_| Abort)?;
                            out.set_str(row, &formatted);
                        }
                    }
                }
                _ => return Ok(false),
            }
        }
        Function::DateAdd | Function::DateDiff => {
            let (second, unit) = (column(1).expect("date operand"), column(2).expect("date unit"));
            let Some(millis) = timestamp(a, row) else { return Ok(false) };
            if unit.tag(row) != Tag::Str {
                return Ok(false);
            }
            let millis = millis?;
            if function == Function::DateAdd {
                let Some(amount) = second.number(row).and_then(integer_of) else { return Ok(false) };
                let unit = super::dates::DateUnit::parse(unit.str(row)).map_err(|_| Abort)?;
                out.set(row, Tag::Signed, super::dates::add(millis, amount, unit).map_err(|_| Abort)? as u64);
            } else {
                let Some(end) = timestamp(second, row) else { return Ok(false) };
                let end = end?;
                let unit = super::dates::DateUnit::parse(unit.str(row)).map_err(|_| Abort)?;
                out.set(row, Tag::Signed, super::dates::diff(millis, end, unit) as u64);
            }
        }
        Function::Type => {
            let kind = match a.tag(row) {
                Tag::Null => "null",
                Tag::Bool => "bool",
                Tag::Signed | Tag::Unsigned | Tag::Float => "number",
                Tag::Str => "string",
                Tag::List => "array",
                _ => return Ok(false),
            };
            out.set_str(row, kind);
        }
        _ => return Ok(false),
    }
    // Operands that must be present were: a missing one never reaches a
    // fast kernel with a result.
    if node.args.iter().any(|argument| columns[argument.out].tag(row) == Tag::Missing) {
        return Err(Abort);
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{parse_expression, SlotProgram, StageName};

    /// Deterministic generator (splitmix64).
    struct Random(u64);

    impl Random {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        fn pick<T: Clone>(&mut self, items: &[T]) -> T {
            items[(self.next() % items.len() as u64) as usize].clone()
        }
    }

    const FIELDS: [&str; 7] = ["a", "b", "s", "t", "f", "arr", "o"];

    /// 0: floats and strings only; 1: every number kind; 2: anything.
    fn number(random: &mut Random, profile: u8) -> Option<Value> {
        let floats = [
            Value::float(12.5).unwrap(), Value::float(-3.25).unwrap(), Value::float(0.0).unwrap(), Value::float(7.0).unwrap(),
            Value::float(1234.5678).unwrap(), Value::float(0.1).unwrap(), Value::float(-0.75).unwrap(), Value::float(99.999).unwrap(),
        ];
        let numbers = [
            Value::float(1e300).unwrap(), Value::float(-1e300).unwrap(), Value::signed(3), Value::signed(-7), Value::signed(0),
            Value::signed(i64::MAX), Value::signed(i64::MIN), Value::signed(9_007_199_254_740_993), Value::unsigned(5), Value::unsigned(u64::MAX),
        ];
        let others = [Value::Null, Value::string("12"), Value::Bool(true)];
        Some(match (profile, random.next() % 3) {
            (0, _) | (_, 0) => random.pick(&floats),
            (1, _) | (_, 1) => random.pick(&numbers),
            _ => random.pick(&others),
        })
    }

    fn row(random: &mut Random, profile: u8) -> Vec<Option<Value>> {
        let text = |random: &mut Random| Some(random.pick(&[
            Value::string("1.598-900.0"), Value::string("héllo wörld"), Value::string(""), Value::string(" 42 "),
            Value::string("3.9"), Value::string("abc"), Value::string("12-2025"), Value::string("a.b.c"), Value::string("ÉTÉ"),
        ]));
        let missing = |random: &mut Random, value: Option<Value>| if profile == 2 && random.next() % 23 == 0 { None } else { value };
        let (first, second, label) = (number(random, profile), number(random, profile), text(random));
        let other = match (profile, random.next() % 4) { (2, 0) => None, (_, 1) => Some(Value::Null), _ => text(random) };
        vec![
            missing(random, first),
            second,
            missing(random, label),
            other,
            Some(random.pick(if profile == 2 { &[Value::Bool(true), Value::Bool(false), Value::Null][..] } else { &[Value::Bool(true), Value::Bool(false)][..] })),
            Some(Value::array([Value::string("x"), Value::signed(2)])),
            Some(Value::object(crate::Document::from_fields([("k", Value::signed(1))]))),
        ]
    }

    fn reference(value: &Value) -> ProjectedValueRef<'_> {
        match value {
            Value::Null => ProjectedValueRef::Null,
            Value::Bool(value) => ProjectedValueRef::Bool(*value),
            Value::Number(Number::Signed(value)) => ProjectedValueRef::Signed(*value),
            Value::Number(Number::Unsigned(value)) => ProjectedValueRef::Unsigned(*value),
            Value::Number(Number::Float(value)) => ProjectedValueRef::Float(*value),
            Value::String(value) => ProjectedValueRef::String(value),
            other => ProjectedValueRef::Owned(other.clone()),
        }
    }

    fn operators(stages: &[&str]) -> Vec<PhysicalOperator> {
        stages
            .iter()
            .map(|stage| match stage.split_once(' ') {
                Some(("where", predicate)) => PhysicalOperator::Filter { predicate: parse_expression(predicate).unwrap() },
                Some((name, arguments)) => PhysicalOperator::custom(StageName::parse(name).unwrap(), arguments, false, false).unwrap(),
                None => panic!("{stage}"),
            })
            .collect()
    }

    /// Runs `stages` on batches and row by row over the same rows.
    fn check(stages: &[&str], rows: &[Vec<Option<Value>>]) -> (usize, usize) {
        let sources = FIELDS.iter().map(|name| ExpressionFieldPath::new([*name]).unwrap()).collect::<Vec<_>>();
        let operators = operators(stages);
        let slots = SlotProgram::compile_expanding(&operators, &sources).unwrap_or_else(|| panic!("{stages:?}: no slot program"));
        let program = Arc::new(BatchProgram::compile(&operators, &sources, slots.fields()).unwrap_or_else(|| panic!("{stages:?}: no batch program")));
        let (mut aborted, mut passed) = (0, 0);
        for chunk in rows.chunks(BATCH_ROWS) {
            let mut runner = BatchRunner::new(Arc::clone(&program));
            for (index, values) in chunk.iter().enumerate() {
                let refs = values.iter().map(|value| value.as_ref().map(reference)).collect::<Vec<_>>();
                runner.push(DocumentId::from_bytes([index as u8; 16]), DocumentVersion::INITIAL, &refs);
            }
            // Every output row of each loaded row, in order.
            let expected = chunk
                .iter()
                .map(|values| {
                    let mut slot_values = values.clone();
                    slot_values.resize(slots.width(), None);
                    let mut rows = Vec::new();
                    slots.run_each(&mut slot_values, |expression, resolver| crate::query::evaluate_predicate(expression, resolver), &mut |row: &[Option<Value>]| { rows.push(row.to_vec()); Ok(()) }).map(|_| rows)
                })
                .collect::<Vec<_>>();
            let failed = expected.iter().position(Result::is_err);
            match runner.run() {
                Err(Abort) => {
                    assert!(failed.is_some(), "{stages:?}: the batch aborted, no row fails");
                    aborted += 1;
                }
                Ok(()) => {
                    assert_eq!(failed, None, "{stages:?}: row {failed:?} fails, the batch ran: {:?}", failed.map(|row| (&chunk[row], &expected[row])));
                    let expected = expected.into_iter().enumerate().flat_map(|(row, rows)| rows.unwrap().into_iter().map(move |values| (row, values))).collect::<Vec<_>>();
                    assert_eq!(runner.selection().len(), expected.len(), "{stages:?}");
                    let mut output = Vec::new();
                    for (&row, (loaded, values)) in runner.selection().iter().zip(&expected) {
                        runner.output_values(row as usize, &mut output);
                        assert_eq!(&output, values, "{stages:?} on {:?}", chunk[*loaded]);
                        assert_eq!(runner.id(row as usize).0, DocumentId::from_bytes([*loaded as u8; 16]), "{stages:?}");
                    }
                    let emitted = expected.iter().map(|(row, _)| *row).collect::<std::collections::BTreeSet<_>>();
                    assert_eq!(runner.dropped(), chunk.len() - emitted.len(), "{stages:?}");
                    passed += expected.len();
                }
            }
        }
        (aborted, passed)
    }

    fn rows(seed: u64, count: usize, profile: u8) -> Vec<Vec<Option<Value>>> {
        let mut random = Random(seed);
        (0..count).map(|_| row(&mut random, profile)).collect()
    }

    #[test]
    fn batches_match_row_at_a_time_evaluation() {
        // Floats and strings, where most batches run; every number kind;
        // anything, where most batches abort.
        let (clean, numeric, mixed) = (rows(7, 2600, 0), rows(9, 2600, 1), rows(11, 2600, 2));
        let expressions = [
            "a + b", "a - b * 2", "a / 4", "a % 3", "-a", "+b", "a * a", "b / a", "a > b", "a <= 10", "a == b", "a != 12.5",
            "a between 0 and 100", "b not between -10 and 10", "s between \"a\" and \"m\"",
            "s == \"abc\"", "s < t", "s != t", "t == null", "s in [\"abc\", \"ÉTÉ\"]", "a in [3, 12.5]", "s not in [\"x\"]",
            "f and a > 0", "f or s == \"\"", "not f", "a > 0 && s != \"\" || f",
            "round(a, 2)", "round(a)", "trunc(b, 1)", "floor(a)", "ceil(b)", "sign(a)", "abs(a)", "abs(b)",
            "div(a, b, 0)", "div(a, b)", "div(a, 0, s)",
            "to_int(a)", "to_int(s)", "to_float(a)", "to_float(s)", "to_string(a)", "to_string(s)", "to_string(f)",
            "len(s)", "upper(s)", "lower(s)", "trim(s)", "ltrim(s)", "rtrim(s)", "substr(s, 1, 3)", "substr(s, -3)", "substr(s, 0, 0)",
            "replace(s, \".\", \"\")", "replace(s, \"l\", \"LL\")", "pad_left(s, 12, \"0\")", "pad_right(s, 4)", "pad_left(to_string(to_int(b)), 4, \"0\")",
            "contains(s, \"l\")", "starts_with(s, \"1.5\")", "ends_with(s, \"0\")", "concat(s, \"@\", a, t)", "concat(s, f)", "type(a)", "type(t)",
            "case(a > 100, \"big\", a > 0, \"small\", \"none\")", "case(a > 1, s)", "if(f, a, s)", "coalesce(t, s, 0)", "coalesce(missing, t)",
            "exists(t)", "t?", "missing?", "bucket(a, [0, 10, 100], \"out\")", "split(s, \".\")", "at(split(s, \".\"), -1)",
            "o.k", "o.k + 1", "arr", "len(arr)", "contains(arr, \"x\")", "merge(o, {\"z\": 2})", "has(o, \"k\")", "max(a, b)",
            "date_part(parse_date(\"2026-03-09\"), \"quarter\")", "concat(arr, [1])", "sqrt(abs(b))",
            "len(split(s, \".\"))", "at(split(s, \"\"), 1)", "at(split(s, \".\"), -1)", "at(split(s, \".\"), 7)", "contains(split(s, \".\"), \"b\")",
            "contains(split(s, \".\"), 1)", "type(split(s, \"-\"))", "split(s, \".\") == [\"a\", \"b\", \"c\"]", "\"a\" in split(s, \".\")", "concat(split(s, \".\"), arr)",
            "coalesce(split(t, \"l\"), 1)", "case(f, split(s, \"\"), s)", "at(arr, b)", "has(o, s)", "len(o)", "bucket(b, [0, 10, 100])", "bucket(a, [\"a\", \"b\"], s)",
            "bucket(a, [3, 1])", "parse_date(s)", "parse_date(a)", "date_part(a, \"quarter\")", "date_trunc(a, \"month\")", "date_format(a)",
            "date_format(a, \"%F\")", "date_add(a, 1, \"month\")", "date_add(parse_date(concat(\"20\", \"26-01-31\")), b, \"day\")",
            "date_diff(a, b, \"days\")", "date_part(a, s)",
        ];
        let mut ran = 0;
        for expression in expressions {
            for rows in [&clean, &numeric, &mixed] {
                for stages in [
                    vec![format!("derive x = {expression}")],
                    vec![format!("derive x = {expression}"), "where exists(x)".to_owned()],
                    vec!["where b > 0".to_owned(), format!("derive x = {expression}, y = x"), "drop b".to_owned()],
                    vec![format!("where exists(t) or {expression} != null")],
                ] {
                    let stages = stages.iter().map(String::as_str).collect::<Vec<_>>();
                    let (_, passed) = check(&stages, rows);
                    ran += passed;
                }
            }
        }
        assert!(ran > 100_000, "{ran}");
        // Stages around the expressions.
        for stages in [
            &["where a > 0", "rename s as label", "derive l = len(label)", "where l > 2"][..],
            &["select a, b as bb, a + bb as c", "where c > 0"],
            &["derive a = a * 2, b = a", "select b"],
            &["drop a", "derive a = b"],
            &["where f", "where not f"],
            &["derive parts = split(s, \".\")", "unwind parts with index i", "where i >= 1", "derive p = len(parts) + i"],
            &["unwind arr with index i", "derive x = type(arr)"],
            &["derive e = if(f, [], [a, s])", "unwind e keep empty", "derive y = e"],
            &["derive e = if(f, [], null)", "unwind e with index k"],
            &["unwind s with index i", "where i == null"],
            &["unwind t keep empty"],
            &["derive parts = split(s, \"\")", "unwind parts", "unwind arr with index j", "where parts != \"x\""],
        ] {
            let (aborted, _) = check(stages, &clean);
            assert!(aborted < clean.len().div_ceil(BATCH_ROWS), "{stages:?}: no batch ran on clean rows");
            for rows in [&numeric, &mixed] {
                check(stages, rows);
            }
        }
    }

    #[test]
    fn a_failing_row_aborts_its_batch_only() {
        let mut values = rows(3, 3000, 0);
        for row in &mut values {
            row[0] = Some(Value::float(2.0).unwrap());
        }
        values[1500][0] = Some(Value::string("x"));
        let (aborted, passed) = check(&["derive x = a * 2", "where x > 1"], &values);
        assert_eq!(aborted, 1);
        assert_eq!(passed, 3000 - BATCH_ROWS);
    }
}
