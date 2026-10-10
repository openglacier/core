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
use crate::storage::{BlockCodes, BlockColumn, DocumentId, DocumentVersion, ProjectedValueRef};

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
    /// Source columns read by a memoized node ([`Memo`]): per loaded row,
    /// the dictionary code of its string (index into `dictionaries` << 32 |
    /// code), [`NO_CODE`] when the cell has none; empty while no row has one.
    codes: Vec<u64>,
    dictionaries: Vec<u64>,
}

/// A cell without a dictionary code.
const NO_CODE: u64 = u64::MAX;

impl Column {
    fn sized() -> Self {
        Self { tag: vec![Tag::Missing; BATCH_ROWS], bits: vec![0; BATCH_ROWS], ..Self::default() }
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
        self.codes.clear();
        self.dictionaries.clear();
    }

    /// Records the codes of rows `from..from + count` of a block, loaded at
    /// `row` (`None`: the block has none for this column).
    fn load_codes(&mut self, row: usize, codes: Option<&BlockCodes<'_>>, from: usize, count: usize) {
        let Some(codes) = codes else {
            if !self.codes.is_empty() {
                self.codes.resize(row + count, NO_CODE);
            }
            return;
        };
        self.codes.resize(row, NO_CODE);
        let slot = match self.dictionaries.iter().position(|dictionary| *dictionary == codes.dictionary) {
            Some(slot) => slot,
            None => {
                self.dictionaries.push(codes.dictionary);
                self.dictionaries.len() - 1
            }
        } as u64;
        self.codes.extend(codes.codes[from..from + count].iter().map(|code| if *code == BlockCodes::NONE { NO_CODE } else { slot << 32 | u64::from(*code) }));
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

    /// Raw payload of a cell (see [`Tag`]).
    #[inline]
    #[must_use]
    pub fn bits(&self, row: usize) -> u64 {
        self.bits[row]
    }

    /// Dictionary and code of a source string cell, when the batch loaded
    /// codes for this column ([`BatchProgram::code_outputs`]).
    #[inline]
    #[must_use]
    pub fn code(&self, row: usize) -> Option<(u64, u32)> {
        let code = *self.codes.get(row)?;
        (code != NO_CODE).then(|| (self.dictionaries[(code >> 32) as usize], code as u32))
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

    /// Loads rows `from..from + count` of a storage block at `row`.
    fn load_block(&mut self, row: usize, values: &BlockColumn<'_, '_>, from: usize, count: usize) {
        let (tag, bits) = (&mut self.tag[row..row + count], &mut self.bits[row..row + count]);
        match *values {
            BlockColumn::Values(values) => {
                for (offset, value) in values[from..from + count].iter().enumerate() {
                    self.load(row + offset, value.as_ref());
                }
            }
            BlockColumn::Signed(values) => {
                tag.fill(Tag::Signed);
                for (slot, value) in bits.iter_mut().zip(&values[from..from + count]) {
                    *slot = *value as u64;
                }
            }
            BlockColumn::Unsigned(values) => {
                tag.fill(Tag::Unsigned);
                bits.copy_from_slice(&values[from..from + count]);
            }
            BlockColumn::Floats(values) => {
                tag.fill(Tag::Float);
                for (slot, value) in bits.iter_mut().zip(&values[from..from + count]) {
                    *slot = value.to_bits();
                }
            }
            BlockColumn::Strings(values) => {
                for (offset, value) in values[from..from + count].iter().enumerate() {
                    self.set_str(row + offset, value);
                }
            }
        }
    }

    /// Loads strings whose codes were just loaded ([`Self::load_codes`]):
    /// rows sharing a code share one copy of its text. A block where few
    /// rows share one (many distinct values) is copied as it comes.
    fn load_coded_strings(&mut self, row: usize, values: &[&str], from: usize, count: usize) {
        const PROBE: usize = 128;
        let mut copied = [(NO_CODE, 0_u64); 64];
        let probe = count.min(PROBE);
        let shared = self.share_strings(row, &values[from..from + probe], &mut copied);
        let rest = &values[from + probe..from + count];
        if shared >= PROBE / 4 {
            self.share_strings(row + probe, rest, &mut copied);
        } else {
            for (offset, value) in rest.iter().enumerate() {
                self.set_str(row + probe + offset, value);
            }
        }
    }

    /// Loads `values` at `row`, one copy of the text per code in `copied`;
    /// returns the rows that found theirs there.
    #[inline]
    fn share_strings(&mut self, row: usize, values: &[&str], copied: &mut [(u64, u64); 64]) -> usize {
        let mut shared = 0;
        for (offset, value) in values.iter().enumerate() {
            let (row, code) = (row + offset, self.codes[row + offset]);
            let slot = &mut copied[(code ^ (code >> 32)) as usize & 63];
            if code != NO_CODE && slot.0 == code {
                self.set(row, Tag::Str, slot.1);
                shared += 1;
            } else {
                self.set_str(row, value);
                if code != NO_CODE {
                    *slot = (code, self.bits[row]);
                }
            }
        }
        shared
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
    lineage: Lineage,
    /// Memoized by dictionary code of source column `.1`: index `.0` of
    /// the runner's memos.
    memo: Option<(usize, usize)>,
}

impl Node {
    /// Marks the source columns (of `sources`) this node reads.
    fn reads(&self, sources: &mut [bool]) {
        let mut read = |column: usize| if let Some(source) = sources.get_mut(column) { *source = true };
        match &self.op {
            Op::Column => read(self.out),
            Op::Row(_, scope) => scope.iter().for_each(|(_, column)| read(*column)),
            _ => {}
        }
        if let Some((_, source)) = self.memo {
            read(source);
        }
        self.args.iter().for_each(|arg| arg.reads(sources));
    }
}

/// What a column's values depend on, per row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lineage {
    /// No field.
    Constant,
    /// The value of one source column, deterministically: rows with one
    /// dictionary code of that column hold one value.
    Source(usize),
    Other,
}

impl Lineage {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Constant, lineage) | (lineage, Self::Constant) => lineage,
            (Self::Source(a), Self::Source(b)) if a == b => self,
            _ => Self::Other,
        }
    }
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
    /// Per source column: whether a memoized node reads its codes.
    coded: Vec<bool>,
    /// Per source column: whether a consumer keys rows by its codes
    /// ([`Self::code_outputs`]) or a costly memo reads them; only such a
    /// column loads number codes, which a cheap memo (a comparison) would
    /// read slower than it computes.
    keyed: Vec<bool>,
    /// Per memo, whether it is cheap.
    memos: Vec<bool>,
    columns: usize,
    constants: Vec<(usize, Column)>,
    stages: Vec<Stage>,
    /// Column of each output field, `None` when no row holds it.
    outputs: Vec<Option<usize>>,
}

struct Compiler {
    columns: usize,
    /// Per column allocated after the sources and the missing column.
    lineage: Vec<Lineage>,
    memos: Vec<bool>,
    constants: Vec<(usize, Column)>,
    missing: usize,
    /// Visible top-level fields and their columns.
    scope: Vec<(Arc<str>, usize)>,
}

/// Scope key of a field path: its segments joined by a separator no field
/// name holds, so `a.b` (a nested path) never meets a field named "a.b".
fn path_key(path: &ExpressionFieldPath) -> std::borrow::Cow<'_, str> {
    if path.len() == 1 {
        return std::borrow::Cow::Borrowed(path.first());
    }
    std::borrow::Cow::Owned(path.iter().collect::<Vec<_>>().join("\x1f"))
}

/// Whether scope key `key` names `path`.
fn names_path(key: &str, path: &ExpressionFieldPath) -> bool {
    key.split('\x1f').eq(path.iter())
}

impl Compiler {
    fn column(&mut self) -> usize {
        self.columns += 1;
        self.lineage.push(Lineage::Other);
        self.columns - 1
    }

    fn constant(&mut self, value: &Value) -> usize {
        let column = self.column();
        self.lineage[column] = Lineage::Constant;
        self.constants.push((column, Column::broadcast(Some(value))));
        column
    }

    fn lookup(&self, name: &str) -> usize {
        self.scope.iter().rev().find(|(field, _)| field.as_ref() == name).map_or(self.missing, |(_, column)| *column)
    }

    /// Binds top-level field `name`; nested source paths under it no
    /// longer name its new value.
    fn bind(&mut self, name: &str, column: usize) {
        self.scope.retain(|(field, _)| field.as_ref() != name && field.split('\x1f').next() != Some(name));
        self.scope.push((Arc::from(name), column));
    }

    /// Whether a nested source path under top-level field `name` is bound.
    fn binds_under(&self, name: &str) -> bool {
        self.scope.iter().any(|(field, _)| field.contains('\x1f') && field.split('\x1f').next() == Some(name))
    }

    fn leaf(&self, out: usize) -> Node {
        Node { op: Op::Column, args: Vec::new(), out, lineage: self.lineage[out], memo: None }
    }

    fn row(&mut self, expression: &Expression) -> Node {
        let scope = Arc::from(self.scope.as_slice());
        Node { op: Op::Row(expression.clone(), scope), args: Vec::new(), out: self.column(), lineage: Lineage::Other, memo: None }
    }

    fn computed(&mut self, op: Op, args: Vec<Node>) -> Node {
        let lineage = args.iter().fold(Lineage::Constant, |lineage, arg| lineage.and(arg.lineage));
        let out = self.column();
        self.lineage[out] = lineage;
        Node { op, args, out, lineage, memo: None }
    }

    /// Compiles a stage's expression; its largest computed subtrees that
    /// read one source column are memoized by dictionary code.
    fn root(&mut self, expression: &Expression) -> Node {
        let mut node = self.node(expression);
        self.memoize(&mut node);
        node
    }

    /// A filter's top-level conjuncts, one filter stage each: a row one
    /// drops never reaches the next, as `and` skips its right operand.
    /// Adjacent conjuncts reading the same one source column stay one node,
    /// memoized once per dictionary code.
    fn conjuncts(&mut self, predicate: &Expression) -> Vec<Node> {
        fn collect<'a>(expression: &'a Expression, output: &mut Vec<&'a Expression>) {
            match expression.view() {
                ExpressionView::Group(inner) => collect(inner, output),
                ExpressionView::Binary { left, operator: BinaryOperator::And, right } => {
                    collect(left, output);
                    collect(right, output);
                }
                _ => output.push(expression),
            }
        }
        let mut expressions = Vec::new();
        collect(predicate, &mut expressions);
        let mut nodes: Vec<Node> = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let node = self.node(expression);
            match nodes.pop() {
                Some(last) if matches!(last.lineage, Lineage::Source(_)) && last.lineage == node.lineage => {
                    let joined = self.computed(Op::And, vec![last, node]);
                    nodes.push(joined);
                }
                Some(last) => nodes.extend([last, node]),
                None => nodes.push(node),
            }
        }
        for node in &mut nodes {
            self.memoize(node);
        }
        nodes
    }

    /// A memo of a subtree that calls no function nor the row evaluator is
    /// cheap: comparing or testing cells costs about as much as looking up a
    /// code's result, so it stays on only while codes repeat
    /// ([`Memo::cheap`]).
    fn memoize(&mut self, node: &mut Node) {
        fn costly(node: &Node) -> bool {
            matches!(node.op, Op::Eager(_) | Op::Row(..)) || node.args.iter().any(costly)
        }
        match (&node.op, node.lineage) {
            (Op::Column, _) => {}
            (_, Lineage::Source(source)) => {
                node.memo = Some((self.memos.len(), source));
                self.memos.push(!costly(node));
            }
            _ => node.args.iter_mut().for_each(|arg| self.memoize(arg)),
        }
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
            // A nested path read as its own source column.
            ExpressionView::Field(path) if self.lookup(&path_key(path)) != self.missing => {
                let out = self.lookup(&path_key(path));
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
    /// The leading filter stages, and the sources they read, when there are
    /// some and they leave a source unread: they can decide on those
    /// columns alone ([`BatchRunner::run_stages`]).
    #[must_use]
    pub fn head(&self) -> Option<(usize, Vec<bool>)> {
        let mut reads = vec![false; self.sources];
        let stages = self.stages.iter().map_while(|stage| if let Stage::Filter(node) = stage { node.reads(&mut reads); Some(()) } else { None }).count();
        (stages > 0 && reads.contains(&false)).then_some((stages, reads))
    }

    /// Compiles `operators` over rows holding `source_fields` (top-level, in
    /// slot order), producing `output_fields` (the slot order of the matching
    /// [`super::SlotProgram`]). `None` when a stage cannot run on batches.
    #[must_use]
    pub fn compile(operators: &[PhysicalOperator], source_fields: &[ExpressionFieldPath], output_fields: &[ExpressionFieldPath]) -> Option<Self> {
        if source_fields.iter().any(|field| field.first() == "_id") {
            return None;
        }
        let sources = source_fields.len();
        let mut lineage = (0..sources).map(Lineage::Source).collect::<Vec<_>>();
        lineage.push(Lineage::Constant);
        let mut compiler = Compiler { columns: sources + 1, lineage, memos: Vec::new(), constants: Vec::new(), missing: sources, scope: Vec::new() };
        compiler.constants.push((sources, Column::broadcast(None)));
        for (column, field) in source_fields.iter().enumerate() {
            compiler.scope.push((Arc::from(path_key(field).as_ref()), column));
        }
        let mut stages = Vec::with_capacity(operators.len());
        for operator in operators {
            if operator.reads_document_id() {
                return None;
            }
            match operator {
                PhysicalOperator::Filter { predicate } => stages.extend(compiler.conjuncts(predicate).into_iter().map(Stage::Filter)),
                // Keeps only the listed fields: a nested one must be a column.
                PhysicalOperator::Select { fields } => {
                    let mut kept = Vec::with_capacity(fields.len());
                    for field in fields.iter() {
                        let key = path_key(field);
                        let column = compiler.lookup(&key);
                        if field.len() > 1 && column == compiler.missing {
                            return None;
                        }
                        kept.push((Arc::<str>::from(key.as_ref()), column));
                    }
                    compiler.scope = kept;
                }
                PhysicalOperator::Custom { writes: false, compiled: Some(stage), .. } if stage.unwind_operation().is_some() => {
                    let (field, index, keep_empty) = stage.unwind_operation().expect("checked above");
                    if field.len() != 1 || index.is_some_and(|index| index.len() != 1) {
                        return None;
                    }
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
                            let node = compiler.root(expression);
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
                                    let node = compiler.root(expression);
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
                        // A missing source leaves the target as it was; the
                        // renamed value of a field read only by nested paths
                        // is not a column.
                        if source.len() != 1 || compiler.binds_under(source.first()) || (target.first() != source.first() && compiler.lookup(target.first()) != compiler.missing) {
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
        // A nested output is a source column, or not known here.
        if output_fields.iter().any(|field| field.len() > 1 && compiler.lookup(&path_key(field)) == compiler.missing) {
            return None;
        }
        let outputs = output_fields
            .iter()
            .map(|field| Some(compiler.lookup(&path_key(field))).filter(|column| *column != compiler.missing))
            .collect();
        let mut coded = vec![false; sources];
        let mut keyed = vec![false; sources];
        // A costly memo also reads number codes ([`Self::keyed`]).
        let mark = |node: &Node, coded: &mut [bool], keyed: &mut [bool]| {
            fn walk(node: &Node, memos: &[bool], coded: &mut [bool], keyed: &mut [bool]) {
                match node.memo {
                    Some((memo, source)) => {
                        coded[source] = true;
                        keyed[source] |= !memos[memo];
                    }
                    None => node.args.iter().for_each(|arg| walk(arg, memos, coded, keyed)),
                }
            }
            walk(node, &compiler.memos, coded, keyed);
        };
        for stage in &stages {
            match stage {
                Stage::Filter(node) => mark(node, &mut coded, &mut keyed),
                Stage::Assign(nodes) => nodes.iter().for_each(|node| mark(node, &mut coded, &mut keyed)),
                Stage::Unwind { .. } => {}
            }
        }
        Some(Self { sources, keyed, coded, memos: compiler.memos, columns: compiler.columns, constants: compiler.constants, stages, outputs })
    }

    /// Source columns that read number codes ([`Self::keyed`]), as bits
    /// for [`crate::storage::ScanOptions::with_number_codes`].
    #[must_use]
    pub fn number_codes(&self) -> u64 {
        self.keyed.iter().take(64).enumerate().filter(|(_, keyed)| **keyed).fold(0, |bits, (index, _)| bits | 1 << index)
    }

    /// Loads the dictionary codes of the output fields at `outputs` that are
    /// source columns, for a consumer that keys rows by them
    /// ([`Column::code`]).
    pub fn code_outputs(&mut self, outputs: &[usize]) {
        for output in outputs {
            if let Some(Some(column)) = self.outputs.get(*output) {
                if *column < self.sources {
                    self.coded[*column] = true;
                    self.keyed[*column] = true;
                }
            }
        }
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
    scratch: Scratch,
    /// Rows the filters dropped before loading ([`Self::skip`]).
    skipped: usize,
}

impl BatchRunner {
    #[must_use]
    pub fn new(program: Arc<BatchProgram>) -> Self {
        let mut columns = (0..program.columns).map(|_| Column::sized()).collect::<Vec<_>>();
        for (column, constant) in &program.constants {
            columns[*column] = constant.clone();
        }
        let scratch = Scratch { arguments: Vec::new(), memos: program.memos.iter().map(|cheap| Memo { cheap: *cheap, ..Memo::default() }).collect(), kept: Vec::new() };
        Self { program, columns, ids: Vec::with_capacity(BATCH_ROWS), rows: 0, selection: Vec::with_capacity(BATCH_ROWS), origin: Vec::new(), scratch, skipped: 0 }
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
        for ((column, value), coded) in self.columns[..self.program.sources].iter_mut().zip(values).zip(&self.program.coded) {
            column.load(row, value.as_ref());
            if *coded && !column.codes.is_empty() {
                column.codes.push(NO_CODE);
            }
        }
        self.ids.push((id, version));
        self.rows += 1;
    }

    /// Loads `count` source rows from `from` in column-major blocks
    /// (`columns[source][row]`); the batch must have room for them.
    /// `codes`: per column, the dictionary codes of its strings, if any.
    pub fn push_rows(&mut self, ids: &[(DocumentId, DocumentVersion)], columns: &[BlockColumn<'_, '_>], codes: &[Option<BlockCodes<'_>>], from: usize, count: usize) {
        let row = self.rows;
        for (index, (column, values)) in self.columns[..self.program.sources].iter_mut().zip(columns).enumerate() {
            // A column a gate did not decode ([`BatchProgram::head`]).
            if values.len() < from + count {
                continue;
            }
            if !self.program.coded[index] {
                column.load_block(row, values, from, count);
                continue;
            }
            let block_codes = codes.get(index).and_then(Option::as_ref);
            column.load_codes(row, block_codes, from, count);
            match (values, block_codes) {
                (BlockColumn::Strings(strings), Some(_)) => column.load_coded_strings(row, strings, from, count),
                _ => column.load_block(row, values, from, count),
            }
        }
        if !ids.is_empty() {
            self.ids.extend_from_slice(&ids[from..from + count]);
        }
        self.rows += count;
    }

    /// Empties the batch.
    pub fn clear(&mut self) {
        self.rows = 0;
        self.skipped = 0;
        self.ids.clear();
        for column in &mut self.columns[..self.program.sources] {
            column.clear_payloads();
        }
    }

    /// Runs every stage; afterwards [`Self::selection`] holds the rows that
    /// passed every filter, in load order.
    pub fn run(&mut self) -> Step {
        self.run_stages(usize::MAX)
    }

    /// [`Self::run`] for the first `stages` stages only.
    pub fn run_stages(&mut self, stages: usize) -> Step {
        self.selection.clear();
        self.selection.extend(0..self.rows as u32);
        self.origin.clear();
        let program = Arc::clone(&self.program);
        for stage in program.stages.iter().take(stages) {
            if self.selection.is_empty() {
                break;
            }
            match stage {
                Stage::Filter(node) => self.filter(node)?,
                Stage::Assign(nodes) => {
                    for node in nodes {
                        eval(node, &mut self.columns, &self.selection, &mut self.scratch)?;
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

    /// Keeps the selected rows on which `node` (one conjunct) is true: a
    /// float field against exact constants in one pass; anything else is
    /// evaluated, then its true rows kept ([`Abort`] on a value that is not
    /// a boolean).
    fn filter(&mut self, node: &Node) -> Step {
        if matches!(node.args.first(), Some(Node { op: Op::Column, .. })) {
            if let Some((bits, test)) = FloatTest::of(node, &self.columns, &self.selection) {
                keep(&mut self.selection, |row| test.holds(f64::from_bits(bits[row])));
                return Ok(());
            }
        }
        if let Some((memo, source)) = node.memo {
            if self.scratch.memos[memo].filter(&self.columns[source], &self.selection, &mut self.scratch.kept)? {
                std::mem::swap(&mut self.selection, &mut self.scratch.kept);
                return Ok(());
            }
        }
        let selection = std::mem::take(&mut self.selection);
        let evaluated = eval(node, &mut self.columns, &selection, &mut self.scratch);
        self.selection = selection;
        evaluated?;
        let (column, mut other) = (&self.columns[node.out], false);
        keep(&mut self.selection, |row| {
            other |= column.tag(row) != Tag::Bool;
            column.bits[row] != 0
        });
        if other { Err(Abort) } else { Ok(()) }
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
            return self.rows - self.selection.len() + self.skipped;
        }
        let mut kept = vec![false; self.rows];
        for &row in &self.selection {
            kept[self.origin[row as usize] as usize] = true;
        }
        kept.iter().filter(|kept| !**kept).count() + self.skipped
    }

    /// Counts `rows` the filters dropped without loading them in
    /// [`Self::dropped`], until [`Self::clear`].
    pub fn skip(&mut self, rows: usize) {
        self.skipped += rows;
    }

    /// Rows that passed every filter.
    #[must_use]
    pub fn selection(&self) -> &[u32] {
        &self.selection
    }

    /// Identifier and version of the loaded row a row comes from; the scan
    /// must not have declined locators.
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
        if field.len() > 1 {
            if let Some((_, column)) = self.scope.iter().rev().find(|(name, _)| names_path(name, field)) {
                return self.columns[*column].value(self.row).map_or(SemanticValue::Missing, SemanticValue::Present);
            }
        }
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

/// Per-runner state of the evaluation: argument buffer and memos.
#[derive(Debug)]
struct Scratch {
    arguments: Vec<Value>,
    memos: Vec<Memo>,
    /// Rows a memoized filter keeps ([`Memo::filter`]).
    kept: Vec<u32>,
}

/// Results of one memoized node per dictionary code of its source column,
/// filled at the first row holding each code. Only results are kept: a
/// failure aborts the batch, replayed row by row as without memo.
#[derive(Debug, Default)]
struct Memo {
    /// Per dictionary: the cell of each code ([`Memo::NONE`]: not known).
    dictionaries: Vec<(u64, Vec<u32>)>,
    cells: Vec<(Tag, u64)>,
    /// Text of the string cells (spans as in [`Column`]).
    text: String,
    /// A result that cannot be kept (array, object, text past
    /// [`Memo::TEXT_BYTES`]) turned it off.
    off: bool,
    /// A cheap memo turns off once codes repeat too little: past
    /// [`Memo::TRIAL_ROWS`] rows, more than one in [`Memo::REUSE`] holding a
    /// code not seen before.
    cheap: bool,
    rows: usize,
    new_codes: usize,
}

impl Memo {
    const NONE: u32 = u32::MAX;
    /// First seen in the current batch: computed with its row.
    const PENDING: u32 = u32::MAX - 1;
    /// Dictionaries kept at once: reading is ordered by row group.
    const DICTIONARIES: usize = 4;
    /// Text kept for string results: past it the memo turns off, and every
    /// row is computed (the memo is not charged to the memory governor).
    const TEXT_BYTES: usize = 4 << 20;
    const TRIAL_ROWS: usize = 8 * BATCH_ROWS;
    const REUSE: usize = 16;

    /// Index in `dictionaries` of each of `wanted`.
    fn slots(&mut self, wanted: &[u64]) -> Vec<usize> {
        if wanted.iter().filter(|dictionary| !self.dictionaries.iter().any(|(known, _)| known == *dictionary)).count() + self.dictionaries.len() > Self::DICTIONARIES {
            self.dictionaries.clear();
            self.cells.clear();
            self.text.clear();
        }
        wanted.iter().map(|dictionary| match self.dictionaries.iter().position(|(known, _)| known == dictionary) {
            Some(slot) => slot,
            None => {
                self.dictionaries.push((*dictionary, Vec::new()));
                self.dictionaries.len() - 1
            }
        }).collect()
    }

    /// A memoized filter over `selection` when every row's code has its
    /// result: the rows it keeps, in order, into `kept`. `Ok(false)` when a
    /// row needs computing (the caller evaluates the batch, which fills the
    /// memo); `Abort` on a result that is not a boolean, as the filter would.
    fn filter(&self, column: &Column, selection: &[u32], kept: &mut Vec<u32>) -> Result<bool, Abort> {
        if self.off || column.dictionaries.is_empty() {
            return Ok(false);
        }
        let mut tables: [&[u32]; Self::DICTIONARIES] = [&[]; Self::DICTIONARIES];
        if column.dictionaries.len() > Self::DICTIONARIES {
            return Ok(false);
        }
        for (table, dictionary) in tables.iter_mut().zip(&column.dictionaries) {
            match self.dictionaries.iter().find(|(known, _)| known == dictionary) {
                Some((_, codes)) => *table = codes,
                None => return Ok(false),
            }
        }
        kept.clear();
        for &row in selection {
            let code = column.codes[row as usize];
            if code == NO_CODE {
                return Ok(false);
            }
            let cell = match tables[(code >> 32) as usize].get(code as u32 as usize) {
                Some(&cell) if cell < Self::PENDING => cell,
                _ => return Ok(false),
            };
            match self.cells[cell as usize] {
                (Tag::Bool, bits) => {
                    if bits != 0 {
                        kept.push(row);
                    }
                }
                _ => return Err(Abort),
            }
        }
        Ok(true)
    }

    fn entry(&mut self, slot: usize, code: u32) -> &mut u32 {
        let codes = &mut self.dictionaries[slot].1;
        if codes.len() <= code as usize {
            codes.resize(code as usize + 1, Self::NONE);
        }
        &mut codes[code as usize]
    }
}

fn eval(node: &Node, columns: &mut [Column], selection: &[u32], scratch: &mut Scratch) -> Step {
    match node.memo {
        Some((memo, source)) if !scratch.memos[memo].off && !columns[source].dictionaries.is_empty() => eval_memo(node, memo, source, columns, selection, scratch),
        _ => eval_node(node, columns, selection, scratch),
    }
}

/// [`eval_node`] once per dictionary code of `source` (and for each row
/// without one); the other rows copy the result of their code.
fn eval_memo(node: &Node, index: usize, source: usize, columns: &mut [Column], selection: &[u32], scratch: &mut Scratch) -> Step {
    let mut memo = std::mem::take(&mut scratch.memos[index]);
    let slots = memo.slots(&columns[source].dictionaries);
    let (mut todo, mut first, mut known) = (Vec::with_capacity(selection.len()), Vec::new(), Vec::with_capacity(selection.len()));
    for &row in selection {
        let code = columns[source].codes[row as usize];
        if code == NO_CODE {
            todo.push(row);
            continue;
        }
        let (slot, code) = (slots[(code >> 32) as usize], code as u32);
        let entry = memo.entry(slot, code);
        if *entry == Memo::NONE {
            *entry = Memo::PENDING;
            todo.push(row);
            first.push((row, slot, code));
        } else {
            known.push((row, slot, code));
        }
    }
    let evaluated = if todo.is_empty() { Ok(()) } else { eval_node(node, columns, &todo, scratch) };
    let mut keep = evaluated.is_ok();
    let mut out = std::mem::take(&mut columns[node.out]);
    for &(row, slot, code) in &first {
        let row = row as usize;
        let cell = match (keep, out.tag(row)) {
            (true, Tag::Str) if memo.text.len() + out.str(row).len() > Memo::TEXT_BYTES => {
                keep = false;
                Memo::NONE
            }
            (true, Tag::Str) => {
                let start = memo.text.len();
                memo.text.push_str(out.str(row));
                memo.cells.push((Tag::Str, ((start as u64) << 32) | (memo.text.len() - start) as u64));
                (memo.cells.len() - 1) as u32
            }
            (true, Tag::Other | Tag::List) => {
                keep = false;
                Memo::NONE
            }
            (true, tag) => {
                memo.cells.push((tag, out.bits[row]));
                (memo.cells.len() - 1) as u32
            }
            (false, _) => Memo::NONE,
        };
        *memo.entry(slot, code) = cell;
    }
    memo.rows += selection.len();
    memo.new_codes += first.len();
    if memo.cheap && memo.rows >= Memo::TRIAL_ROWS && memo.new_codes * Memo::REUSE > memo.rows {
        keep = false;
    }
    if evaluated.is_ok() && !keep {
        // Not kept: forget this batch's codes, then compute every row.
        for &(_, slot, code) in &first {
            *memo.entry(slot, code) = Memo::NONE;
        }
        memo.off = true;
    }
    if keep {
        if todo.is_empty() {
            out.ensure(selection.last().map_or(0, |row| *row as usize + 1));
            out.clear_payloads();
        } else {
            out.ensure(selection.last().map_or(0, |row| *row as usize + 1));
        }
        for &(row, slot, code) in &known {
            let cell = *memo.entry(slot, code);
            let (tag, bits) = memo.cells[cell as usize];
            match tag {
                Tag::Str => {
                    let start = (bits >> 32) as usize;
                    out.set_str(row as usize, &memo.text[start..start + (bits & 0xffff_ffff) as usize]);
                }
                tag => out.set(row as usize, tag, bits),
            }
        }
    }
    columns[node.out] = out;
    scratch.memos[index] = memo;
    evaluated?;
    if keep { Ok(()) } else { eval_node(node, columns, selection, scratch) }
}

fn eval_node(node: &Node, columns: &mut [Column], selection: &[u32], scratch: &mut Scratch) -> Step {
    match &node.op {
        Op::Column => return Ok(()),
        Op::And | Op::Or | Op::Case | Op::If | Op::Coalesce | Op::Div => {}
        _ => {
            for argument in &node.args {
                eval(argument, columns, selection, scratch)?;
            }
        }
    }
    let mut out = std::mem::take(&mut columns[node.out]);
    out.ensure(selection.last().map_or(0, |row| *row as usize + 1));
    out.clear_payloads();
    let result = compute(node, &mut out, columns, selection, scratch);
    columns[node.out] = out;
    result
}

fn compute(node: &Node, out: &mut Column, columns: &mut [Column], selection: &[u32], scratch: &mut Scratch) -> Step {
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
            if let Some((x, test)) = FloatTest::of(node, columns, selection) {
                selection.iter().for_each(|&row| out.set_bool(row as usize, test.holds(f64::from_bits(x[row as usize]))));
                return Ok(());
            }
            let (a, b) = (&columns[arg(0)], &columns[arg(1)]);
            let (mut x, mut y) = (Vec::with_capacity(selection.len()), Vec::with_capacity(selection.len()));
            if exact_floats(a, selection, &mut x) && exact_floats(b, selection, &mut y) {
                // Numbers only, each exact as a float: one tight loop.
                for ((&row, x), y) in selection.iter().zip(&x).zip(&y) {
                    out.set_bool(row as usize, compare_floats(*operator, *x, *y));
                }
                return Ok(());
            }
            let equality = matches!(operator, BinaryOperator::Equal | BinaryOperator::NotEqual);
            for &row in selection {
                let row = row as usize;
                let ordering = match (a.tag(row), b.tag(row)) {
                    (Tag::Str, Tag::Str) => Some(a.str(row).cmp(b.str(row))),
                    (Tag::Bool, Tag::Bool) => Some(a.bits[row].cmp(&b.bits[row])),
                    (Tag::Null, Tag::Null) if equality => Some(std::cmp::Ordering::Equal),
                    // `null` equals nothing else (`equals` on incompatible kinds).
                    (Tag::Null, other) | (other, Tag::Null) if equality && other != Tag::Missing => Some(std::cmp::Ordering::Less),
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
            eval(&node.args[0], columns, selection, scratch)?;
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
                eval(&node.args[1], columns, &rest, scratch)?;
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
                eval(argument, columns, &pending, scratch)?;
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
                        eval(condition, columns, &pending, scratch)?;
                        chosen.clear();
                        let mut kept = Vec::with_capacity(pending.len());
                        for &row in &pending {
                            if columns[condition.out].bool(row as usize).ok_or(Abort)? { chosen.push(row) } else { kept.push(row) }
                        }
                        pending = kept;
                        if !chosen.is_empty() {
                            eval(value, columns, &chosen, scratch)?;
                            for &row in &chosen {
                                out.copy_cell(row as usize, &columns[value.out]);
                            }
                        }
                    }
                    [default] => {
                        eval(default, columns, &pending, scratch)?;
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
            eval(condition, columns, selection, scratch)?;
            let (mut yes, mut no) = (Vec::new(), Vec::new());
            for &row in selection {
                if columns[condition.out].bool(row as usize).ok_or(Abort)? { yes.push(row) } else { no.push(row) }
            }
            for (branch, rows) in [(&node.args[1], yes), (&node.args[2], no)] {
                if !rows.is_empty() {
                    eval(branch, columns, &rows, scratch)?;
                    for &row in &rows {
                        out.copy_cell(row as usize, &columns[branch.out]);
                    }
                }
            }
        }
        Op::Div => {
            eval(&node.args[0], columns, selection, scratch)?;
            eval(&node.args[1], columns, selection, scratch)?;
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
                        eval(fallback, columns, &zero, scratch)?;
                        for &row in &zero {
                            out.copy_cell(row as usize, &columns[fallback.out]);
                        }
                    }
                    None => zero.iter().for_each(|&row| out.set_null(row as usize)),
                }
            }
        }
        Op::Eager(Function::Between) if node.args.len() == 3 => {
            if let Some((x, test)) = FloatTest::of(node, columns, selection) {
                selection.iter().for_each(|&row| out.set_bool(row as usize, test.holds(f64::from_bits(x[row as usize]))));
                return Ok(());
            }
            let mut values = [Vec::with_capacity(selection.len()), Vec::with_capacity(selection.len()), Vec::with_capacity(selection.len())];
            if node.args.iter().zip(&mut values).all(|(argument, values)| exact_floats(&columns[argument.out], selection, values)) {
                let [value, low, high] = &values;
                for (index, &row) in selection.iter().enumerate() {
                    out.set_bool(row as usize, low[index] <= value[index] && value[index] <= high[index]);
                }
                return Ok(());
            }
            for &row in selection {
                let row = row as usize;
                if !eager_fast(Function::Between, node, columns, out, row)? {
                    scratch.arguments.clear();
                    for argument in &node.args {
                        scratch.arguments.push(present(&columns[argument.out], row)?);
                    }
                    reference(out, row, super::functions::call(Function::Between, &scratch.arguments))?;
                }
            }
        }
        Op::Eager(Function::Bucket) if node.args.len() >= 2 && node.args[1].lineage == Lineage::Constant => {
            let bounds = selection.first().and_then(|&first| float_bounds(&columns[arg(1)], first as usize));
            let Some(bounds) = bounds else {
                return selection.iter().try_for_each(|&row| eager_row(Function::Bucket, node, columns, out, row as usize, scratch));
            };
            // Numbers that convert exactly, against bounds validated once:
            // the comparisons `bucket` makes, on floats.
            let value = &columns[arg(0)];
            let default = node.args.get(2).map(|default| &columns[default.out]);
            for &row in selection {
                let row = row as usize;
                let bits = value.bits[row];
                let x = match value.tag(row) {
                    Tag::Float => Some(f64::from_bits(bits)),
                    Tag::Signed => Some(bits as i64 as f64).filter(|x| *x as i64 == bits as i64),
                    Tag::Unsigned => Some(bits as f64).filter(|x| *x as u64 == bits),
                    _ => None,
                };
                let Some(x) = x else {
                    eager_row(Function::Bucket, node, columns, out, row, scratch)?;
                    continue;
                };
                if default.is_some_and(|default| default.tag(row) == Tag::Missing) {
                    return Err(Abort);
                }
                let above = bounds.partition_point(|(bound, _)| x >= *bound);
                match default {
                    _ if above > 0 && above < bounds.len() => out.set_number(row, bounds[above - 1].1),
                    Some(default) => out.copy_cell(row, default),
                    None => out.set_null(row),
                }
            }
        }
        Op::Eager(function) => {
            for &row in selection {
                eager_row(*function, node, columns, out, row as usize, scratch)?;
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

/// One row of an eager function: its fast kernel, else the evaluator's.
#[inline]
fn eager_row(function: Function, node: &Node, columns: &[Column], out: &mut Column, row: usize, scratch: &mut Scratch) -> Step {
    if !eager_fast(function, node, columns, out, row)? {
        scratch.arguments.clear();
        for argument in &node.args {
            scratch.arguments.push(present(&columns[argument.out], row)?);
        }
        reference(out, row, super::functions::call(function, &scratch.arguments))?;
    }
    Ok(())
}

/// The bounds of a `bucket` as exact floats with their numbers, when they
/// are numbers in strictly increasing order (else `bucket` fails on them).
fn float_bounds(column: &Column, row: usize) -> Option<Vec<(f64, Number)>> {
    let bounds = column.array(row)?;
    if bounds.len() < 2 {
        return None;
    }
    let mut floats = Vec::with_capacity(bounds.len());
    for bound in bounds {
        let Value::Number(number) = bound else { return None };
        let x = match *number {
            Number::Float(value) => value,
            Number::Signed(value) => Some(value as f64).filter(|x| *x as i64 == value)?,
            Number::Unsigned(value) => Some(value as f64).filter(|x| *x as u64 == value)?,
        };
        if floats.last().is_some_and(|(last, _)| *last >= x) {
            return None;
        }
        floats.push((x, *number));
    }
    Some(floats)
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

/// Writes the value of each selected row, in selection order, as a float,
/// when every one is a number that converts exactly (the condition under
/// which [`Column::number_order`] compares through floats); `false`
/// otherwise.
fn exact_floats(column: &Column, selection: &[u32], out: &mut Vec<f64>) -> bool {
    out.clear();
    if let Some(bits) = all_floats(column, selection) {
        out.extend(selection.iter().map(|&row| f64::from_bits(bits[row as usize])));
        return true;
    }
    for &row in selection {
        let row = row as usize;
        let bits = column.bits[row];
        out.push(match column.tag[row] {
            Tag::Float => f64::from_bits(bits),
            Tag::Signed => {
                let converted = bits as i64 as f64;
                if converted as i64 != bits as i64 {
                    return false;
                }
                converted
            }
            Tag::Unsigned => {
                let converted = bits as f64;
                if converted as u64 != bits {
                    return false;
                }
                converted
            }
            _ => return false,
        });
    }
    true
}

/// `x operator y` for finite floats.
#[inline]
fn compare_floats(operator: BinaryOperator, x: f64, y: f64) -> bool {
    match operator {
        BinaryOperator::Equal => x == y,
        BinaryOperator::NotEqual => x != y,
        BinaryOperator::LessThan => x < y,
        BinaryOperator::LessThanOrEqual => x <= y,
        BinaryOperator::GreaterThan => x > y,
        _ => x >= y,
    }
}

/// Keeps the rows of `selection` that pass `test`, without a branch per row.
#[inline]
pub(crate) fn keep(selection: &mut Vec<u32>, mut test: impl FnMut(usize) -> bool) {
    let mut kept = 0;
    for position in 0..selection.len() {
        let row = selection[position];
        selection[kept] = row;
        kept += usize::from(test(row as usize));
    }
    selection.truncate(kept);
}

/// A test of floats against exact constants: `x operator y`, or
/// `low <= x <= high`.
#[derive(Clone, Copy)]
enum FloatTest {
    Compare(BinaryOperator, f64),
    Between(f64, f64),
}

impl FloatTest {
    #[inline]
    fn holds(self, x: f64) -> bool {
        match self {
            Self::Compare(operator, y) => compare_floats(operator, x, y),
            Self::Between(low, high) => low <= x && x <= high,
        }
    }

    /// The test `node` makes on the float bits of its first argument, when
    /// every selected row holds a float there and its other arguments are
    /// exact constants.
    fn of<'c>(node: &Node, columns: &'c [Column], selection: &[u32]) -> Option<(&'c [u64], Self)> {
        let first = *selection.first()?;
        let constant = |argument: &Node| {
            let mut value = Vec::with_capacity(1);
            (argument.lineage == Lineage::Constant && exact_floats(&columns[argument.out], &[first], &mut value)).then(|| value[0])
        };
        let test = match (&node.op, &node.args[..]) {
            (Op::Compare(operator), [_, y]) => Self::Compare(*operator, constant(y)?),
            (Op::Eager(Function::Between), [_, low, high]) => Self::Between(constant(low)?, constant(high)?),
            _ => return None,
        };
        Some((all_floats(&columns[node.args[0].out], selection)?, test))
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
/// `value` in decimal, as `Display` writes it.
fn write_decimal(out: &mut String, mut value: u64) {
    let mut digits = [0u8; 20];
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    out.push_str(std::str::from_utf8(&digits[start..]).expect("ASCII digits"));
}

fn write_json_scalar(out: &mut String, column: &Column, row: usize) -> bool {
    match column.tag(row) {
        Tag::Bool => out.push_str(if column.bits[row] != 0 { "true" } else { "false" }),
        Tag::Signed => {
            let value = column.bits[row] as i64;
            if value < 0 {
                out.push('-');
            }
            write_decimal(out, value.unsigned_abs());
        }
        Tag::Unsigned => write_decimal(out, column.bits[row]),
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
                            out.set(row, Tag::Signed, super::dates::trunc(millis, unit).map_err(|_| Abort)? as u64);
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
        Function::FiscalYear | Function::FiscalMonth | Function::FiscalStart | Function::FiscalEnd => {
            let Some(millis) = timestamp(a, row) else { return Ok(false) };
            let Some(closing) = column(1).and_then(|closing| timestamp(closing, row)) else { return Ok(false) };
            let fields = super::dates::fiscal(millis?, closing?).map_err(|_| Abort)?;
            out.set(row, Tag::Signed, fields[function.fiscal_field().expect("a fiscal function")] as u64);
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
                out.set(row, Tag::Signed, super::dates::diff(millis, end, unit).map_err(|_| Abort)? as u64);
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
                Some((name, arguments)) => PhysicalOperator::custom(StageName::parse(name).unwrap(), arguments).unwrap(),
                None => panic!("{stage}"),
            })
            .collect()
    }

    /// Runs `stages` on batches and row by row over the same rows.
    fn check(stages: &[&str], rows: &[Vec<Option<Value>>]) -> (usize, usize) {
        let plain = check_loaded(stages, rows, None);
        assert_eq!(check_loaded(stages, rows, Some(u64::MAX)), plain, "{stages:?}: with dictionary codes");
        assert_eq!(check_loaded(stages, rows, Some(3)), plain, "{stages:?}: with dictionaries met again");
        plain
    }

    /// `coded`: rows loaded as storage blocks of 100 rows, strings with
    /// dictionary codes, into one runner; one dictionary per 300 rows,
    /// `cycle` of them in turn (later batches meet them again).
    fn check_loaded(stages: &[&str], rows: &[Vec<Option<Value>>], coded: Option<u64>) -> (usize, usize) {
        let cycle = coded.unwrap_or(u64::MAX);
        let coded = coded.is_some();
        let sources = FIELDS.iter().map(|name| ExpressionFieldPath::new([*name]).unwrap()).collect::<Vec<_>>();
        let operators = operators(stages);
        let slots = SlotProgram::compile_expanding(&operators, &sources).unwrap_or_else(|| panic!("{stages:?}: no slot program"));
        let program = Arc::new(BatchProgram::compile(&operators, &sources, slots.fields()).unwrap_or_else(|| panic!("{stages:?}: no batch program")));
        let (mut aborted, mut passed) = (0, 0);
        let mut dictionaries = std::collections::HashMap::<(u64, String), u32>::new();
        let mut runner = BatchRunner::new(Arc::clone(&program));
        for (number, chunk) in rows.chunks(BATCH_ROWS).enumerate() {
            if !coded {
                runner = BatchRunner::new(Arc::clone(&program));
            }
            runner.clear();
            if coded {
                for (block, block_rows) in chunk.chunks(100).enumerate() {
                    let first = number * BATCH_ROWS + block * 100;
                    let ids = (0..block_rows.len()).map(|offset| (DocumentId::from_bytes([(block * 100 + offset) as u8; 16]), DocumentVersion::INITIAL)).collect::<Vec<_>>();
                    let cells = (0..FIELDS.len()).map(|field| block_rows.iter().map(|values| values[field].as_ref().map(reference)).collect::<Vec<_>>()).collect::<Vec<_>>();
                    let codes = (0..FIELDS.len()).map(|field| {
                        block_rows.iter().enumerate().map(|(offset, values)| match &values[field] {
                            Some(Value::String(text)) => {
                                let key = (((first + offset) / 300) as u64 % cycle, text.to_string());
                                let next = dictionaries.len() as u32;
                                *dictionaries.entry(key).or_insert(next)
                            }
                            _ => BlockCodes::NONE,
                        }).collect::<Vec<_>>()
                    }).collect::<Vec<_>>();
                    // A field holding a string in every row of the block comes
                    // as text, as Glacier hands its string chunks.
                    let strings = (0..FIELDS.len()).map(|field| block_rows.iter().map(|values| match &values[field] { Some(Value::String(text)) => Some(text.as_ref()), _ => None }).collect::<Option<Vec<&str>>>()).collect::<Vec<_>>();
                    let columns = cells.iter().zip(&strings).map(|(cells, strings)| strings.as_ref().map_or(BlockColumn::Values(cells), |strings| BlockColumn::Strings(strings))).collect::<Vec<_>>();
                    // Each 100-row block holds rows of one or two dictionaries:
                    // split it there.
                    let mut from = 0;
                    while from < block_rows.len() {
                        let dictionary = ((first + from) / 300) as u64 % cycle;
                        let count = (300 - (first + from) % 300).min(block_rows.len() - from);
                        let block_codes = codes.iter().map(|codes| Some(BlockCodes { dictionary, codes })).collect::<Vec<_>>();
                        runner.push_rows(&ids, &columns, &block_codes, from, count);
                        from += count;
                    }
                }
            } else {
                for (index, values) in chunk.iter().enumerate() {
                    let refs = values.iter().map(|value| value.as_ref().map(reference)).collect::<Vec<_>>();
                    runner.push(DocumentId::from_bytes([index as u8; 16]), DocumentVersion::INITIAL, &refs);
                }
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

    #[test] fn batches_match_row_at_a_time_evaluation() { /* Floats and strings, where most batches run; every number kind; */ /* anything, where most batches abort. */ let (clean, numeric, mixed) = (rows(7, 2600, 0), rows(9, 2600, 1), rows(11, 2600, 2)); let expressions = ["a + b", "a - b * 2", "a / 4", "a % 3", "-a", "+b", "a * a", "b / a", "a > b", "a <= 10", "a == b", "a != 12.5", "a between 0 and 100", "b not between -10 and 10", "s between \"a\" and \"m\"", "s == \"abc\"", "s < t", "s != t", "t == null", "s in [\"abc\", \"ÉTÉ\"]", "a in [3, 12.5]", "s not in [\"x\"]", "f and a > 0", "f or s == \"\"", "not f", "a > 0 && s != \"\" || f", "round(a, 2)", "round(a)", "trunc(b, 1)", "floor(a)", "ceil(b)", "sign(a)", "abs(a)", "abs(b)", "div(a, b, 0)", "div(a, b)", "div(a, 0, s)", "to_int(a)", "to_int(s)", "to_float(a)", "to_float(s)", "to_string(a)", "to_string(s)", "to_string(f)", "len(s)", "upper(s)", "lower(s)", "trim(s)", "ltrim(s)", "rtrim(s)", "substr(s, 1, 3)", "substr(s, -3)", "substr(s, 0, 0)", "replace(s, \".\", \"\")", "replace(s, \"l\", \"LL\")", "pad_left(s, 12, \"0\")", "pad_right(s, 4)", "pad_left(to_string(to_int(b)), 4, \"0\")", "contains(s, \"l\")", "starts_with(s, \"1.5\")", "ends_with(s, \"0\")", "concat(s, \"@\", a, t)", "concat(s, f)", "type(a)", "type(t)", "case(a > 100, \"big\", a > 0, \"small\", \"none\")", "case(a > 1, s)", "if(f, a, s)", "coalesce(t, s, 0)", "coalesce(missing, t)", "exists(t)", "t?", "missing?", "bucket(a, [0, 10, 100], \"out\")", "split(s, \".\")", "at(split(s, \".\"), -1)", "o.k", "o.k + 1", "arr", "len(arr)", "contains(arr, \"x\")", "merge(o, {\"z\": 2})", "has(o, \"k\")", "max(a, b)", "date_part(parse_date(\"2026-03-09\"), \"quarter\")", "concat(arr, [1])", "sqrt(abs(b))", "len(split(s, \".\"))", "at(split(s, \"\"), 1)", "at(split(s, \".\"), -1)", "at(split(s, \".\"), 7)", "contains(split(s, \".\"), \"b\")", "contains(split(s, \".\"), 1)", "type(split(s, \"-\"))", "split(s, \".\") == [\"a\", \"b\", \"c\"]", "\"a\" in split(s, \".\")", "concat(split(s, \".\"), arr)", "coalesce(split(t, \"l\"), 1)", "case(f, split(s, \"\"), s)", "at(arr, b)", "has(o, s)", "len(o)", "bucket(b, [0, 10, 100])", "bucket(a, [\"a\", \"b\"], s)", "bucket(a, [3, 1])", "to_string(to_int(a) - 5000)", "to_string(to_int(b))", "split(s, \"É\")", "split(s, \"bc\")", "bucket(a, [0, 10.5, 100])", "bucket(b, [-5, 0, 9007199254740993])", "bucket(a, [0, 10, 100], t)", "parse_date(s)", "parse_date(a)", "date_part(a, \"quarter\")", "date_trunc(a, \"month\")", "date_format(a)", "date_format(a, \"%F\")", "date_add(a, 1, \"month\")", "date_add(parse_date(concat(\"20\", \"26-01-31\")), b, \"day\")", "date_diff(a, b, \"days\")", "date_part(a, s)", "t != null", "null == t", "a == null", "f == true", "f != t", "f < true", "a > 3", "b >= 9007199254740993", "a between 3 and 100", "b between -7 and 5", "a between b and 100"]; let mut ran = 0; for expression in expressions { for rows in [&clean, &numeric, &mixed] { for stages in [vec![format!("derive x = {expression}")], vec![format!("derive x = {expression}"), "where exists(x)".to_owned()], vec!["where b > 0".to_owned(), format!("derive x = {expression}, y = x"), "drop b".to_owned()], vec![format!("where exists(t) or {expression} != null")]] { let stages = stages.iter().map(String::as_str).collect::<Vec<_>>(); let (_, passed) = check(&stages, rows); ran += passed; } } } assert!(ran > 100_000, "{ran}"); /* Stages around the expressions. */ for stages in [&["where a > 0", "rename s as label", "derive l = len(label)", "where l > 2"][..], &["select a, b as bb, a + bb as c", "where c > 0"], &["derive a = a * 2, b = a", "select b"], &["drop a", "derive a = b"], &["where f", "where not f"], /* Conjuncts: one filter stage each, those on one column memoized together. */ &["where type(s?) == \"string\" and s? == \"abc\" and type(t?) == \"string\" and t? == \"abc\""], &["where (s != \"\" and s < \"m\") and a > 0 and t != null and t > \"a\""], &["where s == \"abc\" and t == \"abc\"", "derive n = len(s)"], &["where a > 0 and (f or s == \"x\") and s != t"], &["derive parts = split(s, \".\")", "unwind parts with index i", "where i >= 1", "derive p = len(parts) + i"], &["unwind arr with index i", "derive x = type(arr)"], &["derive e = if(f, [], [a, s])", "unwind e keep empty", "derive y = e"], &["derive e = if(f, [], null)", "unwind e with index k"], &["unwind s with index i", "where i == null"], &["unwind t keep empty"], &["derive parts = split(s, \"\")", "unwind parts", "unwind arr with index j", "where parts != \"x\""]] { let (aborted, _) = check(stages, &clean); assert!(aborted < clean.len().div_ceil(BATCH_ROWS), "{stages:?}: no batch ran on clean rows"); for rows in [&numeric, &mixed] { check(stages, rows); } } /* A memoized conjunct that is not a boolean aborts every batch. */ for stages in [&["where upper(s) == s and s", "select s"][..], &["where s and a > 0"]] { for rows in [&clean, &numeric, &mixed] { check(stages, rows); } } }

    #[test] fn keep_compacts_the_passing_rows_in_order() { let mut selection = vec![1, 4, 6, 9, 10]; keep(&mut selection, |row| row % 2 == 0); assert_eq!(selection, [4, 6, 10]); }
    #[test] fn a_failing_row_aborts_its_batch_only() { let mut values = rows(3, 3000, 0); for row in &mut values { row[0] = Some(Value::float(2.0).unwrap()); } values[1500][0] = Some(Value::string("x")); let (aborted, passed) = check(&["derive x = a * 2", "where x > 1"], &values); assert_eq!(aborted, 1); assert_eq!(passed, 3000 - BATCH_ROWS); }
}
