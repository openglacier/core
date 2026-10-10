#![cfg_attr(rustfmt, rustfmt_skip)]
//! Executable query expressions.

use std::{fmt, sync::Arc};

use super::Span;

/// Result returned by expression parsing.
pub type ExpressionResult<T> = std::result::Result<T, ExpressionError>;

/// Parsed expression node.
#[derive(Clone, Debug, PartialEq)]
pub struct Expression {
    kind: ExpressionKind,
    span: Span,
    /// Value of the node when it depends on no field and no volatile
    /// function, computed once when the expression is parsed.
    constant: Option<crate::Value>,
}

impl Expression {
    /// Creates an expression.
    #[must_use]
    #[inline]
    pub const fn new(kind: ExpressionKind, span: Span) -> Self {
        Self { kind, span, constant: None }
    }

    /// The resolved function of a call node.
    #[must_use]
    #[inline]
    pub const fn function(&self) -> Option<super::Function> {
        match &self.kind {
            ExpressionKind::Call { function, .. } => Some(*function),
            _ => None,
        }
    }

    /// The node's precomputed value, when it is constant.
    #[must_use]
    #[inline]
    pub const fn constant(&self) -> Option<&crate::Value> {
        self.constant.as_ref()
    }

    /// Precomputes the value of every constant subtree: literals, arrays and
    /// objects of constants, operators and deterministic functions over
    /// constants. A subtree whose evaluation fails stays as it is, so the
    /// error still surfaces when rows are evaluated. Returns whether this
    /// node is constant.
    fn fold_constants(&mut self) -> bool {
        // Every item is folded, even after a non-constant one.
        let fold_all = |items: &mut [Self]| {
            let mut all = true;
            for item in items {
                all &= item.fold_constants();
            }
            all
        };
        let children_constant = match &mut self.kind {
            ExpressionKind::Literal(_) => true,
            ExpressionKind::Field(_) => false,
            ExpressionKind::Unary { operand, .. } | ExpressionKind::Group(operand) => operand.fold_constants(),
            ExpressionKind::Binary { left, right, .. } => {
                let left = left.fold_constants();
                right.fold_constants() && left
            }
            ExpressionKind::Array(items) => fold_all(items),
            ExpressionKind::Call { function, arguments } => {
                let volatile = super::eval::is_volatile(*function);
                fold_all(arguments) && !volatile
            }
        };
        if children_constant {
            self.constant = super::eval::evaluate_constant(self);
        } else if let Some(chosen) = self.chosen_branch() {
            *self = chosen;
        }
        self.constant.is_some()
    }

    /// The part of a conditional its constant conditions leave: `if(false, a, b)`
    /// is `b`, `true or x` is `true`, `coalesce(null, x)` is `x`, `case(false, a, c, b)`
    /// is `case(c, b)`. Folded at planning, so that `derive f = if($p, null, f)`
    /// becomes `f = f` once `$p` is bound.
    fn chosen_branch(&self) -> Option<Self> {
        use super::Function::{Case, Coalesce, If};
        let flag = |item: &Self| item.constant.as_ref().and_then(crate::Value::as_bool);
        let call = |function, rest: &[Self]| Self::new(ExpressionKind::Call { function, arguments: rest.to_vec() }, self.span);
        match &self.kind {
            ExpressionKind::Binary { left, operator: operator @ (BinaryOperator::And | BinaryOperator::Or), .. } => (flag(left)? == (*operator == BinaryOperator::Or)).then(|| left.as_ref().clone()),
            ExpressionKind::Call { function: If, arguments } => Some(arguments[if flag(&arguments[0])? { 1 } else { 2 }].clone()),
            ExpressionKind::Call { function: Case, arguments } => {
                let skip = arguments.chunks(2).take_while(|pair| pair.len() == 2 && flag(&pair[0]) == Some(false)).count();
                match &arguments[2 * skip..] {
                    [default] => Some(default.clone()),
                    [condition, value, ..] if flag(condition) == Some(true) => Some(value.clone()),
                    rest if skip > 0 && !rest.is_empty() => Some(call(Case, rest)),
                    _ => None,
                }
            }
            ExpressionKind::Call { function: Coalesce, arguments } => {
                let skip = arguments.iter().take_while(|item| item.constant.as_ref().is_some_and(crate::Value::is_null)).count();
                match &arguments[skip..] {
                    [first, ..] if first.constant.is_some() => Some(first.clone()),
                    [only] => Some(only.clone()),
                    rest if skip > 0 && !rest.is_empty() => Some(call(Coalesce, rest)),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// This expression with every field it reads made optional (`a` reads
    /// as `a?`): a missing field reads as `null` instead of failing.
    #[must_use]
    pub(crate) fn with_optional_fields(&self) -> Self {
        let mut copy = self.clone();
        copy.make_fields_optional();
        copy
    }

    fn make_fields_optional(&mut self) {
        match &mut self.kind {
            ExpressionKind::Field(_) => {
                let field = std::mem::replace(self, Self::new(ExpressionKind::Array(Vec::new()), self.span));
                *self = Self::new(ExpressionKind::Call { function: super::Function::Optional, arguments: vec![field] }, self.span);
            }
            ExpressionKind::Call { function: super::Function::Optional, .. } | ExpressionKind::Literal(_) => {}
            ExpressionKind::Unary { operand, .. } | ExpressionKind::Group(operand) => operand.make_fields_optional(),
            ExpressionKind::Binary { left, right, .. } => {
                left.make_fields_optional();
                right.make_fields_optional();
            }
            ExpressionKind::Array(items) | ExpressionKind::Call { arguments: items, .. } => items.iter_mut().for_each(Self::make_fields_optional),
        }
    }

    /// Creates a literal expression.
    #[must_use]
    pub const fn literal(literal: Literal, span: Span) -> Self {
        Self::new(ExpressionKind::Literal(literal), span)
    }

    /// Creates a field expression.
    #[must_use]
    #[inline]
    pub const fn field(path: ExpressionFieldPath, span: Span) -> Self {
        Self::new(ExpressionKind::Field(path), span)
    }

    /// Creates a unary expression.
    #[must_use]
    pub fn unary(operator: UnaryOperator, operand: Self, span: Span) -> Self {
        Self::new(
            ExpressionKind::Unary {
                operator,
                operand: Box::new(operand),
            },
            span,
        )
    }

    /// Creates a binary expression.
    #[must_use]
    pub fn binary(
        left: Self,
        operator: BinaryOperator,
        right: Self,
        span: Span,
    ) -> Self {
        Self::new(
            ExpressionKind::Binary {
                left: Box::new(left),
                operator,
                right: Box::new(right),
            },
            span,
        )
    }

    /// Creates an explicit parenthesized group.
    #[must_use]
    pub fn group(expression: Self, span: Span) -> Self {
        Self::new(ExpressionKind::Group(Box::new(expression)), span)
    }

    /// Returns the expression kind.
    #[must_use]
    pub const fn kind(&self) -> &ExpressionKind {
        &self.kind
    }

    /// Returns the expression source span.
    ///
    /// The span is relative to the source passed to [`parse_expression`].
    #[must_use]
    #[inline]
    pub const fn span(&self) -> Span {
        self.span
    }

    /// Returns the expression as a literal when applicable.
    #[must_use]
    pub const fn as_literal(&self) -> Option<&Literal> {
        match &self.kind {
            ExpressionKind::Literal(literal) => Some(literal),
            _ => None,
        }
    }

    /// Returns the expression as a field reference when applicable.
    #[must_use]
    pub const fn as_field(&self) -> Option<&ExpressionFieldPath> {
        match &self.kind {
            ExpressionKind::Field(path) => Some(path),
            _ => None,
        }
    }

    /// The field this node reads, and whether it reads it optionally (`a?`: missing reads as `null`).
    #[must_use]
    pub fn as_read_field(&self) -> Option<(&ExpressionFieldPath, bool)> {
        match &self.kind {
            ExpressionKind::Field(path) => Some((path, false)),
            ExpressionKind::Call { function: super::Function::Optional, arguments } if arguments.len() == 1 => arguments[0].as_field().map(|path| (path, true)),
            _ => None,
        }
    }

    /// Returns whether this node is a literal.
    #[must_use]
    pub const fn is_literal(&self) -> bool {
        matches!(self.kind, ExpressionKind::Literal(_))
    }

    /// Returns whether this node is a field reference.
    #[must_use]
    pub const fn is_field(&self) -> bool {
        matches!(self.kind, ExpressionKind::Field(_))
    }

    /// Returns whether this node is a unary expression.
    #[must_use]
    pub const fn is_unary(&self) -> bool {
        matches!(self.kind, ExpressionKind::Unary { .. })
    }

    /// Returns whether this node is a binary expression.
    #[must_use]
    pub const fn is_binary(&self) -> bool {
        matches!(self.kind, ExpressionKind::Binary { .. })
    }

    /// Returns whether this node is an explicit parenthesized group.
    #[must_use]
    pub const fn is_group(&self) -> bool {
        matches!(self.kind, ExpressionKind::Group(_))
    }

    /// Returns the unary operator and operand when applicable.
    #[must_use]
    pub fn as_unary(&self) -> Option<(UnaryOperator, &Self)> {
        match &self.kind {
            ExpressionKind::Unary { operator, operand } => Some((*operator, operand.as_ref())),
            _ => None,
        }
    }

    /// Returns the binary operands and operator when applicable.
    #[must_use]
    pub fn as_binary(&self) -> Option<(&Self, BinaryOperator, &Self)> {
        match &self.kind {
            ExpressionKind::Binary {
                left,
                operator,
                right,
            } => Some((left.as_ref(), *operator, right.as_ref())),
            _ => None,
        }
    }

    /// Returns the inner expression of an explicit group.
    #[must_use]
    pub fn as_group(&self) -> Option<&Self> {
        match &self.kind {
            ExpressionKind::Group(expression) => Some(expression.as_ref()),
            _ => None,
        }
    }

    /// Removes all transparent parenthesized wrappers.
    ///
    /// Group nodes remain preserved in the AST for diagnostics, while semantic
    /// integration can use this accessor when parentheses must not affect
    /// evaluation.
    #[must_use]
    pub fn ungrouped(&self) -> &Self {
        let mut expression = self;

        while let ExpressionKind::Group(inner) = expression.kind() {
            expression = inner.as_ref();
        }

        expression
    }

    /// Returns a stable structural view of this expression.
    ///
    /// This view covers every syntax form accepted by the parser.
    #[must_use]
    pub fn view(&self) -> ExpressionView<'_> {
        match &self.kind {
            ExpressionKind::Literal(literal) => ExpressionView::Literal(literal),
            ExpressionKind::Field(path) => ExpressionView::Field(path),
            ExpressionKind::Unary { operator, operand } => ExpressionView::Unary {
                operator: *operator,
                operand: operand.as_ref(),
            },
            ExpressionKind::Binary {
                left,
                operator,
                right,
            } => ExpressionView::Binary {
                left: left.as_ref(),
                operator: *operator,
                right: right.as_ref(),
            },
            ExpressionKind::Group(expression) => ExpressionView::Group(expression.as_ref()),
            ExpressionKind::Array(items) => ExpressionView::Array(items),
            ExpressionKind::Call { function, arguments } => ExpressionView::Call {
                function: function.as_str(),
                arguments,
            },
        }
    }

    /// Visits the direct sub-expressions, in source order.
    pub fn for_each_child<'a>(&'a self, visit: &mut dyn FnMut(&'a Self)) {
        match &self.kind {
            ExpressionKind::Literal(_) | ExpressionKind::Field(_) => {}
            ExpressionKind::Unary { operand, .. } | ExpressionKind::Group(operand) => visit(operand),
            ExpressionKind::Binary { left, right, .. } => {
                visit(left);
                visit(right);
            }
            ExpressionKind::Array(items) | ExpressionKind::Call { arguments: items, .. } => {
                items.iter().for_each(visit);
            }
        }
    }

    /// Visits every field path referenced anywhere in the expression.
    pub fn for_each_field<'a>(&'a self, visit: &mut dyn FnMut(&'a ExpressionFieldPath)) {
        if let ExpressionKind::Field(path) = &self.kind {
            visit(path);
        }
        self.for_each_child(&mut |child| child.for_each_field(visit));
    }
}

/// Renders the expression back to query syntax (used by `explain`).
impl fmt::Display for Expression {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let list = |formatter: &mut fmt::Formatter<'_>, items: &[Self]| -> fmt::Result {
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    formatter.write_str(", ")?;
                }
                write!(formatter, "{item}")?;
            }
            Ok(())
        };
        match &self.kind {
            ExpressionKind::Literal(Literal::Null) => formatter.write_str("null"),
            ExpressionKind::Literal(Literal::Bool(value)) => write!(formatter, "{value}"),
            ExpressionKind::Literal(Literal::Number(text) | Literal::Json(text)) => formatter.write_str(text),
            ExpressionKind::Literal(Literal::String(text)) => write!(formatter, "{}", serde_json::Value::from(text.as_ref())),
            ExpressionKind::Field(path) => write!(formatter, "{path}"),
            ExpressionKind::Unary { operator: UnaryOperator::Not, operand } => write!(formatter, "not {operand}"),
            ExpressionKind::Unary { operator, operand } if operand.is_binary() => write!(formatter, "{operator}({operand})"),
            ExpressionKind::Unary { operator, operand } => write!(formatter, "{operator}{operand}"),
            ExpressionKind::Binary { left, operator, right } => {
                let spelling = match operator {
                    BinaryOperator::And => "and",
                    BinaryOperator::Or => "or",
                    other => other.as_str(),
                };
                write!(formatter, "{left} {spelling} {right}")
            }
            ExpressionKind::Group(inner) => write!(formatter, "({inner})"),
            ExpressionKind::Array(items) => {
                formatter.write_str("[")?;
                list(formatter, items)?;
                formatter.write_str("]")
            }
            ExpressionKind::Call { function: super::Function::Optional, arguments } if arguments.len() == 1 && arguments[0].is_field() => {
                write!(formatter, "{}?", arguments[0])
            }
            ExpressionKind::Call { function, arguments } => {
                write!(formatter, "{function}(")?;
                list(formatter, arguments)?;
                formatter.write_str(")")
            }
        }
    }
}

/// Expression node category.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ExpressionKind {
    /// Literal value.
    Literal(Literal),

    /// Document field reference.
    Field(ExpressionFieldPath),

    /// Prefix unary expression.
    Unary {
        operator: UnaryOperator,
        operand: Box<Expression>,
    },

    /// Infix binary expression.
    Binary {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },

    /// Explicit parenthesized expression.
    ///
    /// The group is preserved in the syntax tree so diagnostics and future
    /// syntax-sensitive tooling can distinguish explicit parentheses.
    Group(Box<Expression>),

    /// Array built from element expressions: `[a, b + 1, "x"]`.
    Array(Vec<Expression>),

    /// Function call: `round(price * qty, 2)`.
    Call {
        function: super::Function,
        arguments: Vec<Expression>,
    },
}

/// Borrowed structural view of an expression.
///
/// This is the stable integration surface for semantic adapters. It exposes the
/// complete parsed language without giving callers mutable access to the AST.
#[derive(Clone, Copy, Debug)]
pub enum ExpressionView<'a> {
    Literal(&'a Literal),
    Field(&'a ExpressionFieldPath),
    Unary {
        operator: UnaryOperator,
        operand: &'a Expression,
    },
    Binary {
        left: &'a Expression,
        operator: BinaryOperator,
        right: &'a Expression,
    },
    Group(&'a Expression),
    Array(&'a [Expression]),
    Call {
        function: &'a str,
        arguments: &'a [Expression],
    },
}

impl ExpressionView<'_> {
    /// Returns whether this view represents a literal.
    #[must_use]
    pub const fn is_literal(self) -> bool {
        matches!(self, Self::Literal(_))
    }

    /// Returns whether this view represents a field.
    #[must_use]
    pub const fn is_field(self) -> bool {
        matches!(self, Self::Field(_))
    }

    /// Returns whether this view represents a unary expression.
    #[must_use]
    pub const fn is_unary(self) -> bool {
        matches!(self, Self::Unary { .. })
    }

    /// Returns whether this view represents a binary expression.
    #[must_use]
    pub const fn is_binary(self) -> bool {
        matches!(self, Self::Binary { .. })
    }

    /// Returns whether this view represents an explicit group.
    #[must_use]
    pub const fn is_group(self) -> bool {
        matches!(self, Self::Group(_))
    }
}

/// Expression literal.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Literal {
    Null,
    Bool(bool),

    /// Numeric literal preserving its exact source spelling.
    ///
    /// Semantic numeric conversion belongs to a later compilation layer.
    Number(Arc<str>),

    /// Decoded string value.
    String(Arc<str>),

    /// Canonical JSON array or object literal.
    Json(Arc<str>),
}

impl Literal {
    /// Returns the exact numeric spelling.
    #[must_use]
    pub fn as_number_text(&self) -> Option<&str> {
        match self {
            Self::Number(value) => Some(value.as_ref()),
            Self::Null | Self::Bool(_) | Self::String(_) | Self::Json(_) => None,
        }
    }

    /// Returns the decoded string value.
    #[must_use]
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_ref()),
            Self::Null | Self::Bool(_) | Self::Number(_) | Self::Json(_) => None,
        }
    }

    /// Returns the boolean value.
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            Self::Null | Self::Number(_) | Self::String(_) | Self::Json(_) => None,
        }
    }

    /// Returns whether this literal is null.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

/// Field path used inside expressions.
///
/// This type is intentionally independent from the storage-level `FieldPath`.
/// Expression compilation can later validate and lower it into the kernel
/// field-path representation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExpressionFieldPath {
    segments: Arc<[Arc<str>]>,
}

impl ExpressionFieldPath {
    /// Creates a validated expression field path.
    ///
    /// # Errors
    ///
    /// Returns an error when no segment is supplied or a segment is empty.
    pub fn new<I, S>(segments: I) -> ExpressionResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let segments = segments
            .into_iter()
            .enumerate()
            .map(|(index, segment)| {
                let segment = segment.as_ref();

                if segment.is_empty() {
                    return Err(ExpressionError::empty_field_segment(index));
                }

                Ok(Arc::<str>::from(segment))
            })
            .collect::<ExpressionResult<Vec<_>>>()?;

        if segments.is_empty() {
            return Err(ExpressionError::empty_field_path());
        }

        Ok(Self {
            segments: Arc::from(segments),
        })
    }

    /// Returns the number of segments.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Returns whether the path is empty.
    ///
    /// A valid expression field path is never empty.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Returns one segment.
    #[must_use]
    pub fn segment(&self, index: usize) -> Option<&str> {
        self.segments.get(index).map(AsRef::as_ref)
    }

    /// Returns the first segment.
    #[must_use]
    pub fn first(&self) -> &str {
        self.segments
            .first()
            .expect("validated field paths are never empty")
            .as_ref()
    }

    /// Returns the final segment.
    #[must_use]
    pub fn last(&self) -> &str {
        self.segments
            .last()
            .expect("validated field paths are never empty")
            .as_ref()
    }

    /// Returns the segments.
    #[must_use]
    pub fn segments(&self) -> &[Arc<str>] {
        &self.segments
    }

    /// Iterates over segments.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &str> {
        self.segments.iter().map(AsRef::as_ref)
    }
}

impl fmt::Display for ExpressionFieldPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, segment) in self.segments.iter().enumerate() {
            if index > 0 {
                formatter.write_str(".")?;
            }

            formatter.write_str(segment)?;
        }

        Ok(())
    }
}

/// Prefix unary operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
    Not,
    Negate,
    Positive,
}

impl UnaryOperator {
    /// Returns the source spelling.
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Not => "!",
            Self::Negate => "-",
            Self::Positive => "+",
        }
    }

    /// Returns whether this operator produces a boolean result.
    #[must_use]
    pub const fn is_boolean(self) -> bool {
        matches!(self, Self::Not)
    }

    /// Returns whether this operator is numeric.
    #[must_use]
    pub const fn is_numeric(self) -> bool {
        matches!(self, Self::Negate | Self::Positive)
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Infix binary operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    Or,
    And,

    Equal,
    NotEqual,

    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,

    /// Membership: `value in array`.
    In,
    /// Negated membership: `value not in array`.
    NotIn,
}

impl BinaryOperator {
    /// Returns the source spelling.
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Or => "||",
            Self::And => "&&",
            Self::Equal => "==",
            Self::NotEqual => "!=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Remainder => "%",
            Self::In => "in",
            Self::NotIn => "not in",
        }
    }

    /// Returns the operator precedence.
    #[must_use]
    pub const fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            Self::Equal | Self::NotEqual => 3,
            Self::LessThan
            | Self::LessThanOrEqual
            | Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::In
            | Self::NotIn => 4,
            Self::Add | Self::Subtract => 5,
            Self::Multiply | Self::Divide | Self::Remainder => 6,
        }
    }

    /// Returns whether this is a boolean operator.
    #[must_use]
    pub const fn is_boolean(self) -> bool {
        matches!(self, Self::Or | Self::And)
    }

    /// Returns whether this is a comparison operator.
    #[must_use]
    pub const fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Equal
                | Self::NotEqual
                | Self::LessThan
                | Self::LessThanOrEqual
                | Self::GreaterThan
                | Self::GreaterThanOrEqual
        )
    }

    /// Returns whether this is an arithmetic operator.
    #[must_use]
    pub const fn is_arithmetic(self) -> bool {
        matches!(
            self,
            Self::Add | Self::Subtract | Self::Multiply | Self::Divide | Self::Remainder
        )
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Parses one complete expression.
///
/// Leading and trailing whitespace is ignored.
///
/// # Errors
///
/// Returns an error when the expression is empty, malformed, or followed by an
/// unexpected token.
pub fn parse_expression(source: &str) -> ExpressionResult<Expression> {
    // A whole-expression JSON array stays one literal; objects are lexed as `Object` tokens.
    let trimmed = source.trim();
    if trimmed.starts_with('[') {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if value.is_array() {
                let canonical =
                    serde_json::to_string(&value).expect("parsed JSON value must serialize");
                let mut expression = Expression::literal(
                    Literal::Json(Arc::from(canonical)),
                    Span::new(0, source.len()),
                );
                expression.fold_constants();
                return Ok(expression);
            }
        }
    }
    let mut expression = ExpressionParser::new(source).parse()?;
    expression.fold_constants();
    Ok(expression)
}

/// Expression parser.
#[derive(Clone, Debug)]
pub struct ExpressionParser<'source> {
    source: &'source str,
    tokens: Vec<ExpressionToken>,
    /// Index of the token after `current`.
    next: usize,
    current: ExpressionToken,
}

impl<'source> ExpressionParser<'source> {
    /// Creates an expression parser.
    #[must_use]
    #[inline]
    pub const fn new(source: &'source str) -> Self {
        Self {
            source,
            tokens: Vec::new(),
            next: 0,
            current: ExpressionToken::end(0),
        }
    }

    /// Parses one complete expression.
    ///
    /// # Errors
    ///
    /// Returns an expression syntax error.
    pub fn parse(mut self) -> ExpressionResult<Expression> {
        self.tokens = expression_tokens(self.source)?;
        self.advance()?;

        if self.current.kind == ExpressionTokenKind::End {
            return Err(ExpressionError::empty_expression());
        }

        let expression = self.parse_precedence(1)?;

        if self.current.kind != ExpressionTokenKind::End {
            return Err(ExpressionError::unexpected_token(
                self.current.span,
                self.current.text(self.source),
                "end of expression",
            ));
        }

        Ok(expression)
    }

    fn parse_precedence(&mut self, minimum_precedence: u8) -> ExpressionResult<Expression> {
        let mut left = self.parse_unary()?;

        loop {
            // `x [not] between low and high` sits at the relational level.
            let negated_between = self.current.kind == ExpressionTokenKind::Bang && self.next_is_word("between");
            if (negated_between || self.current_is_word("between")) && minimum_precedence <= RELATIONAL_PRECEDENCE {
                left = self.parse_between(left, negated_between)?;
                continue;
            }

            let operator = match binary_operator(self.current.kind) {
                Some(operator) => operator,
                // `not in` and `not between` are the only infix uses of `not`.
                None if self.current.kind == ExpressionTokenKind::Bang && self.next_is(ExpressionTokenKind::In) => {
                    BinaryOperator::NotIn
                }
                None => break,
            };

            let precedence = operator.precedence();

            if precedence < minimum_precedence {
                break;
            }

            if operator == BinaryOperator::NotIn {
                self.advance()?;
            }
            self.advance()?;

            let right = self.parse_precedence(precedence + 1)?;
            let span = Span::new(left.span().start(), right.span().end());

            left = Expression::new(
                ExpressionKind::Binary {
                    left: Box::new(left),
                    operator,
                    right: Box::new(right),
                },
                span,
            );
        }

        Ok(left)
    }

    fn parse_unary(&mut self) -> ExpressionResult<Expression> {
        let operator = match self.current.kind {
            ExpressionTokenKind::Bang => Some(UnaryOperator::Not),

            ExpressionTokenKind::Minus => Some(UnaryOperator::Negate),

            ExpressionTokenKind::Plus => Some(UnaryOperator::Positive),

            _ => None,
        };

        let Some(operator) = operator else {
            return self.parse_primary();
        };

        let start = self.current.span.start();

        self.advance()?;

        // A sign directly applied to a numeric literal is part of the literal:
        // `-5` is the number -5, not a negation evaluated at runtime. Every
        // evaluator therefore accepts signed constants, including predicates.
        if matches!(operator, UnaryOperator::Negate | UnaryOperator::Positive)
            && self.current.kind == ExpressionTokenKind::Number
        {
            let span = Span::new(start, self.current.span.end());
            let digits = self.current.text(self.source);
            let text = if operator == UnaryOperator::Negate { format!("-{digits}") } else { digits.to_owned() };
            self.advance()?;
            return Ok(Expression::new(ExpressionKind::Literal(Literal::Number(Arc::from(text))), span));
        }

        // `not` binds looser than comparisons, like in SQL and Python:
        // `not city == "Berlin"` is `not (city == "Berlin")`, while
        // `not a and b` stays `(not a) and b`.
        let operand = if operator == UnaryOperator::Not {
            self.parse_precedence(BinaryOperator::Equal.precedence())?
        } else {
            self.parse_unary()?
        };
        let span = Span::new(start, operand.span().end());

        Ok(Expression::new(
            ExpressionKind::Unary {
                operator,
                operand: Box::new(operand),
            },
            span,
        ))
    }

    /// Parses `between low and high` after `value`, with the current token on
    /// `between` (or on `not` when `negated`). Desugars to `between(value, low, high)`.
    fn parse_between(&mut self, value: Expression, negated: bool) -> ExpressionResult<Expression> {
        if negated {
            self.advance()?;
        }
        self.advance()?;
        let low = self.parse_precedence(RELATIONAL_PRECEDENCE + 1)?;
        if self.current.kind != ExpressionTokenKind::AndAnd {
            return Err(match self.current.kind {
                ExpressionTokenKind::End => ExpressionError::unexpected_end(self.current.span, "`and` in `between … and …`"),
                _ => ExpressionError::unexpected_token(self.current.span, self.current.text(self.source), "`and` in `between … and …`"),
            });
        }
        self.advance()?;
        let high = self.parse_precedence(RELATIONAL_PRECEDENCE + 1)?;
        let span = Span::new(value.span().start(), high.span().end());
        let call = Expression::new(
            ExpressionKind::Call { function: super::Function::Between, arguments: vec![value, low, high] },
            span,
        );
        Ok(if negated { Expression::unary(UnaryOperator::Not, call, span) } else { call })
    }

    fn parse_primary(&mut self) -> ExpressionResult<Expression> {
        match self.current.kind {
            ExpressionTokenKind::Null => {
                let span = self.current.span;

                self.advance()?;

                Ok(Expression::new(
                    ExpressionKind::Literal(Literal::Null),
                    span,
                ))
            }

            ExpressionTokenKind::True => {
                let span = self.current.span;

                self.advance()?;

                Ok(Expression::new(
                    ExpressionKind::Literal(Literal::Bool(true)),
                    span,
                ))
            }

            ExpressionTokenKind::False => {
                let span = self.current.span;

                self.advance()?;

                Ok(Expression::new(
                    ExpressionKind::Literal(Literal::Bool(false)),
                    span,
                ))
            }

            ExpressionTokenKind::Number => {
                let span = self.current.span;
                let text = self.current.text(self.source);

                self.advance()?;

                Ok(Expression::new(
                    ExpressionKind::Literal(Literal::Number(Arc::from(text))),
                    span,
                ))
            }

            ExpressionTokenKind::String => {
                let span = self.current.span;
                let value = decode_string_literal(self.current.text(self.source), span)?;

                self.advance()?;

                Ok(Expression::new(
                    ExpressionKind::Literal(Literal::String(Arc::from(value))),
                    span,
                ))
            }

            ExpressionTokenKind::Identifier => self.parse_field(),

            ExpressionTokenKind::LeftParen => self.parse_group(),

            ExpressionTokenKind::LeftBracket => self.parse_array(),

            ExpressionTokenKind::Object => {
                let span = self.current.span;
                let text = self.current.text(self.source);
                let canonical = serde_json::from_str::<serde_json::Value>(text)
                    .ok()
                    .filter(serde_json::Value::is_object)
                    .map(|value| serde_json::to_string(&value).expect("parsed JSON value must serialize"))
                    .ok_or_else(|| ExpressionError::unexpected_token(span, text, "a JSON object with quoted keys"))?;
                self.advance()?;
                Ok(Expression::literal(Literal::Json(Arc::from(canonical)), span))
            }

            ExpressionTokenKind::End => Err(ExpressionError::unexpected_end(
                self.current.span,
                "expression",
            )),

            _ => Err(ExpressionError::unexpected_token(
                self.current.span,
                self.current.text(self.source),
                "expression",
            )),
        }
    }

    fn parse_field(&mut self) -> ExpressionResult<Expression> {
        let start = self.current.span.start();
        let mut end = self.current.span.end();
        let mut segments = Vec::new();

        segments.push(Arc::<str>::from(self.current.text(self.source)));

        self.advance()?;

        if self.current.kind == ExpressionTokenKind::LeftParen {
            return self.parse_call(Arc::clone(&segments[0]), start);
        }

        // A `?` after any segment (`nick?`, `profile?.country`) makes the whole
        // path optional: a missing field reads as `null` instead of failing.
        let mut optional = false;
        loop {
            if self.current.kind == ExpressionTokenKind::Question {
                optional = true;
                end = self.current.span.end();
                self.advance()?;
            }

            if self.current.kind != ExpressionTokenKind::Dot {
                break;
            }

            self.advance()?;

            if self.current.kind != ExpressionTokenKind::Identifier {
                return Err(ExpressionError::unexpected_token(
                    self.current.span,
                    self.current.text(self.source),
                    "field-path segment",
                ));
            }

            end = self.current.span.end();

            segments.push(Arc::<str>::from(self.current.text(self.source)));

            self.advance()?;
        }

        let path = ExpressionFieldPath {
            segments: Arc::from(segments),
        };
        let span = Span::new(start, end);
        let field = Expression::new(ExpressionKind::Field(path), span);

        Ok(if optional {
            Expression::new(ExpressionKind::Call { function: super::Function::Optional, arguments: vec![field] }, span)
        } else {
            field
        })
    }

    fn parse_group(&mut self) -> ExpressionResult<Expression> {
        let start = self.current.span.start();

        self.advance()?;

        if self.current.kind == ExpressionTokenKind::RightParen {
            return Err(ExpressionError::empty_group(Span::new(
                start,
                self.current.span.end(),
            )));
        }

        let expression = self.parse_precedence(1)?;

        if self.current.kind != ExpressionTokenKind::RightParen {
            return Err(ExpressionError::unexpected_token(
                self.current.span,
                self.current.text(self.source),
                "')'",
            ));
        }

        let end = self.current.span.end();

        self.advance()?;

        Ok(Expression::new(
            ExpressionKind::Group(Box::new(expression)),
            Span::new(start, end),
        ))
    }

    /// Parses `name(arg, ...)`; the current token is the opening parenthesis.
    fn parse_call(&mut self, function: Arc<str>, start: usize) -> ExpressionResult<Expression> {
        self.advance()?;
        let (arguments, end) = self.parse_list(ExpressionTokenKind::RightParen, "',' or ')'")?;
        let function = super::eval::check_call(&function, arguments.len())
            .map_err(|message| ExpressionError::new(ExpressionErrorKind::InvalidCall { message: Arc::from(message) }, Span::new(start, end)))?;
        Ok(Expression::new(ExpressionKind::Call { function, arguments }, Span::new(start, end)))
    }

    /// Parses `[item, ...]`; the current token is the opening bracket.
    fn parse_array(&mut self) -> ExpressionResult<Expression> {
        let start = self.current.span.start();
        self.advance()?;
        let (items, end) = self.parse_list(ExpressionTokenKind::RightBracket, "',' or ']'")?;
        Ok(Expression::new(ExpressionKind::Array(items), Span::new(start, end)))
    }

    /// Parses comma-separated expressions up to `close`, which is consumed.
    fn parse_list(&mut self, close: ExpressionTokenKind, expected: &'static str) -> ExpressionResult<(Vec<Expression>, usize)> {
        let mut items = Vec::new();
        if self.current.kind != close {
            loop {
                items.push(self.parse_precedence(1)?);
                if self.current.kind != ExpressionTokenKind::Comma {
                    break;
                }
                self.advance()?;
            }
        }
        if self.current.kind != close {
            return Err(match self.current.kind {
                ExpressionTokenKind::End => ExpressionError::unexpected_end(self.current.span, expected),
                _ => ExpressionError::unexpected_token(self.current.span, self.current.text(self.source), expected),
            });
        }
        let end = self.current.span.end();
        self.advance()?;
        Ok((items, end))
    }

    fn next_is(&self, kind: ExpressionTokenKind) -> bool {
        self.tokens.get(self.next).is_some_and(|token| token.kind == kind)
    }

    fn current_is_word(&self, word: &str) -> bool {
        self.current.kind == ExpressionTokenKind::Identifier && self.current.text(self.source) == word
    }

    fn next_is_word(&self, word: &str) -> bool {
        self.tokens.get(self.next).is_some_and(|token| {
            token.kind == ExpressionTokenKind::Identifier && token.text(self.source) == word
        })
    }

    fn advance(&mut self) -> ExpressionResult<()> {
        self.current = self.tokens.get(self.next).copied().unwrap_or_else(|| ExpressionToken::end(self.source.len()));
        self.next += 1;
        Ok(())
    }
}

/// Precedence of the relational level (`<`, `in`, `between`, …).
const RELATIONAL_PRECEDENCE: u8 = 4;

const fn binary_operator(kind: ExpressionTokenKind) -> Option<BinaryOperator> {
    match kind {
        ExpressionTokenKind::OrOr => Some(BinaryOperator::Or),

        ExpressionTokenKind::AndAnd => Some(BinaryOperator::And),

        ExpressionTokenKind::EqualEqual => Some(BinaryOperator::Equal),

        ExpressionTokenKind::BangEqual => Some(BinaryOperator::NotEqual),

        ExpressionTokenKind::Less => Some(BinaryOperator::LessThan),

        ExpressionTokenKind::LessEqual => Some(BinaryOperator::LessThanOrEqual),

        ExpressionTokenKind::Greater => Some(BinaryOperator::GreaterThan),

        ExpressionTokenKind::GreaterEqual => Some(BinaryOperator::GreaterThanOrEqual),

        ExpressionTokenKind::Plus => Some(BinaryOperator::Add),

        ExpressionTokenKind::Minus => Some(BinaryOperator::Subtract),

        ExpressionTokenKind::Star => Some(BinaryOperator::Multiply),

        ExpressionTokenKind::Slash => Some(BinaryOperator::Divide),

        ExpressionTokenKind::Percent => Some(BinaryOperator::Remainder),

        ExpressionTokenKind::In => Some(BinaryOperator::In),

        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpressionTokenKind {
    Identifier,
    Number,
    String,

    True,
    False,
    Null,

    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    Comma,
    Dot,
    In,

    Plus,
    Minus,
    Star,
    Slash,
    Percent,

    Bang,

    EqualEqual,
    BangEqual,

    Less,
    LessEqual,
    Greater,
    GreaterEqual,

    AndAnd,
    OrOr,

    /// Postfix `?` marking an optional field path.
    Question,

    /// Complete JSON object literal `{…}`, lexed as one token.
    Object,

    End,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExpressionToken {
    kind: ExpressionTokenKind,
    span: Span,
}

impl ExpressionToken {
    #[inline]
    const fn new(kind: ExpressionTokenKind, span: Span) -> Self {
        Self { kind, span }
    }

    const fn end(offset: usize) -> Self {
        Self::new(ExpressionTokenKind::End, Span::at(offset))
    }

    fn text(self, source: &str) -> &str {
        source.get(self.span.start()..self.span.end()).unwrap_or("")
    }
}

/// The tokens of an expression, read by the query lexer: signs and leading
/// dots of numbers, word operators and `{…}` objects are recombined into
/// the expression's own kinds.
fn expression_tokens(source: &str) -> ExpressionResult<Vec<ExpressionToken>> {
    use super::TokenKind as T;
    use ExpressionTokenKind as E;
    let stream = super::lex(source).map_err(|error| {
        let span = error.span();
        match *error.kind() {
            super::LexErrorKind::UnexpectedCharacter { character: '&' } => ExpressionError::single_ampersand(span),
            super::LexErrorKind::UnexpectedCharacter { character } => ExpressionError::invalid_character(character, span),
            super::LexErrorKind::UnterminatedString | super::LexErrorKind::UnterminatedEscape => ExpressionError::unterminated_string(span),
            super::LexErrorKind::InvalidEscape { character } => ExpressionError::invalid_escape(character, span),
            super::LexErrorKind::ExpectedEqualsAfterBang => ExpressionError::unexpected_end(Span::at(source.len()), "an operand"),
            _ => ExpressionError::invalid_number(span),
        }
    })?;
    let tokens = stream.tokens();
    let mut out = Vec::with_capacity(tokens.len());
    let mut index = 0;
    while let Some(&token) = tokens.get(index) {
        index += 1;
        let span = token.span();
        let text = span.slice(source).unwrap_or("");
        let kind = match token.kind() {
            T::End => E::End,
            T::String => E::String,
            T::True => E::True,
            T::False => E::False,
            T::Null => E::Null,
            T::And => E::AndAnd,
            T::Or => E::OrOr,
            T::Not => E::Bang,
            T::Identifier if text == "in" => E::In,
            kind if kind == T::Identifier || kind.is_keyword() => E::Identifier,
            T::Number => {
                if let Some(sign) = text.strip_prefix(['+', '-']).map(|_| if text.starts_with('-') { E::Minus } else { E::Plus }) {
                    out.push(ExpressionToken::new(sign, Span::new(span.start(), span.start() + 1)));
                    out.push(ExpressionToken::new(E::Number, Span::new(span.start() + 1, span.end())));
                    continue;
                }
                E::Number
            }
            // `.5`: a dot right before digits starts a number.
            T::Dot => match tokens.get(index) {
                Some(number) if number.kind() == T::Number && number.span().start() == span.end() && span.slice(source).is_some() && !number.span().slice(source).unwrap_or("").starts_with(['+', '-']) => {
                    index += 1;
                    out.push(ExpressionToken::new(E::Number, Span::new(span.start(), number.span().end())));
                    continue;
                }
                _ => E::Dot,
            },
            T::Comma => E::Comma,
            T::LeftParen => E::LeftParen,
            T::RightParen => E::RightParen,
            T::LeftBracket => E::LeftBracket,
            T::RightBracket => E::RightBracket,
            // A balanced `{…}` is one object literal.
            T::LeftBrace => {
                let mut depth = 1_usize;
                let end = loop {
                    let Some(&inner) = tokens.get(index) else {
                        return Err(ExpressionError::unexpected_end(Span::new(span.start(), source.len()), "'}'"));
                    };
                    index += 1;
                    match inner.kind() {
                        T::LeftBrace => depth += 1,
                        T::RightBrace => {
                            depth -= 1;
                            if depth == 0 {
                                break inner.span().end();
                            }
                        }
                        T::End => return Err(ExpressionError::unexpected_end(Span::new(span.start(), source.len()), "'}'")),
                        _ => {}
                    }
                };
                out.push(ExpressionToken::new(E::Object, Span::new(span.start(), end)));
                continue;
            }
            T::RightBrace => return Err(ExpressionError::invalid_character('}', span)),
            T::Colon => return Err(ExpressionError::invalid_character(':', span)),
            T::Equal => return Err(ExpressionError::single_equal(span)),
            T::Pipe => return Err(ExpressionError::single_pipe(span)),
            T::EqualEqual => E::EqualEqual,
            T::NotEqual => E::BangEqual,
            T::Less => E::Less,
            T::LessEqual => E::LessEqual,
            T::Greater => E::Greater,
            T::GreaterEqual => E::GreaterEqual,
            T::Operator => match text {
                "+" => E::Plus,
                "-" => E::Minus,
                "*" => E::Star,
                "/" => E::Slash,
                "%" => E::Percent,
                "?" => E::Question,
                "&&" => E::AndAnd,
                "||" => E::OrOr,
                "!" => E::Bang,
                _ => return Err(ExpressionError::invalid_character(text.chars().next().unwrap_or(' '), span)),
            },
            _ => return Err(ExpressionError::invalid_character(text.chars().next().unwrap_or(' '), span)),
        };
        out.push(ExpressionToken::new(kind, span));
    }
    Ok(out)
}

fn decode_string_literal(source: &str, span: Span) -> ExpressionResult<String> {
    let inner = source
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .ok_or_else(|| ExpressionError::unterminated_string(span))?;
    super::syntax::unescape_string(inner).map_err(|character| ExpressionError::invalid_escape(character, span))
}

/// Expression parsing error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpressionError {
    kind: ExpressionErrorKind,
    span: Span,
}

impl ExpressionError {
    /// Creates an expression error.
    #[must_use]
    #[inline]
    pub const fn new(kind: ExpressionErrorKind, span: Span) -> Self {
        Self { kind, span }
    }

    /// Returns the error category.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &ExpressionErrorKind {
        &self.kind
    }

    /// Returns the error span.
    #[must_use]
    #[inline]
    pub const fn span(&self) -> Span {
        self.span
    }

    const fn empty_expression() -> Self {
        Self::new(ExpressionErrorKind::EmptyExpression, Span::at(0))
    }

    const fn empty_group(span: Span) -> Self {
        Self::new(ExpressionErrorKind::EmptyGroup, span)
    }

    const fn empty_field_path() -> Self {
        Self::new(ExpressionErrorKind::EmptyFieldPath, Span::at(0))
    }

    const fn empty_field_segment(index: usize) -> Self {
        Self::new(
            ExpressionErrorKind::EmptyFieldSegment { index },
            Span::at(0),
        )
    }

    const fn unexpected_end(span: Span, expected: &'static str) -> Self {
        Self::new(ExpressionErrorKind::UnexpectedEnd { expected }, span)
    }

    fn unexpected_token(span: Span, found: &str, expected: &'static str) -> Self {
        Self::new(
            ExpressionErrorKind::UnexpectedToken {
                found: Arc::from(found),
                expected,
            },
            span,
        )
    }

    const fn invalid_character(character: char, span: Span) -> Self {
        Self::new(ExpressionErrorKind::InvalidCharacter { character }, span)
    }

    const fn invalid_number(span: Span) -> Self {
        Self::new(ExpressionErrorKind::InvalidNumber, span)
    }

    const fn unterminated_string(span: Span) -> Self {
        Self::new(ExpressionErrorKind::UnterminatedString, span)
    }

    const fn invalid_escape(character: char, span: Span) -> Self {
        Self::new(ExpressionErrorKind::InvalidStringEscape { character }, span)
    }

    const fn single_equal(span: Span) -> Self {
        Self::new(ExpressionErrorKind::SingleEqual, span)
    }

    const fn single_ampersand(span: Span) -> Self {
        Self::new(ExpressionErrorKind::SingleAmpersand, span)
    }

    const fn single_pipe(span: Span) -> Self {
        Self::new(ExpressionErrorKind::SinglePipe, span)
    }
}

impl fmt::Display for ExpressionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ExpressionErrorKind::EmptyExpression => {
                formatter.write_str("expression must not be empty")
            }

            ExpressionErrorKind::EmptyGroup => {
                formatter.write_str("parenthesized expression must not be empty")
            }

            ExpressionErrorKind::EmptyFieldPath => {
                formatter.write_str("expression field path must not be empty")
            }

            ExpressionErrorKind::EmptyFieldSegment { index } => {
                write!(
                    formatter,
                    "expression field-path segment {index} must not be empty",
                )
            }

            ExpressionErrorKind::UnexpectedEnd { expected } => {
                write!(
                    formatter,
                    "unexpected end of expression; expected {expected}",
                )
            }

            ExpressionErrorKind::UnexpectedToken { found, expected } => {
                write!(formatter, "unexpected token {found:?}; expected {expected}")
            }

            ExpressionErrorKind::InvalidCharacter { character } => {
                write!(formatter, "invalid expression character {character:?}")
            }

            ExpressionErrorKind::InvalidNumber => formatter.write_str("invalid numeric literal"),

            ExpressionErrorKind::UnterminatedString => {
                formatter.write_str("unterminated string literal")
            }

            ExpressionErrorKind::InvalidStringEscape { character } => {
                write!(formatter, "invalid string escape \\{character}")
            }

            ExpressionErrorKind::SingleEqual => formatter
                .write_str("single '=' is not an expression operator; use '==' for equality"),

            ExpressionErrorKind::SingleAmpersand => {
                formatter.write_str("single '&' is not supported; use '&&'")
            }

            ExpressionErrorKind::SinglePipe => {
                formatter.write_str("single '|' is not supported inside an expression; use '||'")
            }

            ExpressionErrorKind::InvalidCall { message } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ExpressionError {}

/// Detailed expression error category.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExpressionErrorKind {
    EmptyExpression,
    EmptyGroup,

    EmptyFieldPath,

    EmptyFieldSegment {
        index: usize,
    },

    UnexpectedEnd {
        expected: &'static str,
    },

    UnexpectedToken {
        found: Arc<str>,
        expected: &'static str,
    },

    InvalidCharacter {
        character: char,
    },

    InvalidNumber,
    UnterminatedString,

    InvalidStringEscape {
        character: char,
    },

    SingleEqual,
    SingleAmpersand,
    SinglePipe,

    /// Unknown function or wrong number of arguments.
    InvalidCall {
        message: Arc<str>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn parses_null_literal() { let expression = parse_expression("null").unwrap(); assert_eq!(expression.kind(), &ExpressionKind::Literal(Literal::Null),); assert_eq!(expression.span(), Span::new(0, 4)); }

    #[test] fn parses_boolean_literals() { let true_expression = parse_expression("true").unwrap(); let false_expression = parse_expression("false").unwrap(); assert_eq!(true_expression.as_literal().unwrap().as_bool(), Some(true),); assert_eq!( false_expression.as_literal().unwrap().as_bool(), Some(false), ); }

    #[test] fn parses_integer_literal() { let expression = parse_expression("123").unwrap(); assert_eq!( expression.as_literal().unwrap().as_number_text(), Some("123"), ); }

    #[test] fn parses_decimal_literal() { let expression = parse_expression("12.50").unwrap(); assert_eq!( expression.as_literal().unwrap().as_number_text(), Some("12.50"), ); }

    #[test] fn parses_leading_decimal_point() { let expression = parse_expression(".75").unwrap(); assert_eq!( expression.as_literal().unwrap().as_number_text(), Some(".75"), ); }

    #[test] fn parses_exponent_literal() { let expression = parse_expression("1.5e-10").unwrap(); assert_eq!( expression.as_literal().unwrap().as_number_text(), Some("1.5e-10"), ); }

    #[test] fn preserves_numeric_literal_spelling() { let first = parse_expression("1").unwrap(); let second = parse_expression("1.0").unwrap(); assert_ne!(first.as_literal(), second.as_literal(),); }

    #[test] fn parses_string_literal() { let expression = parse_expression(r#""hello""#).unwrap(); assert_eq!(expression.as_literal().unwrap().as_string(), Some("hello"),); }

    #[test] fn decodes_string_escapes() { let expression = parse_expression(r#""hello\n\"OG\"""#).unwrap(); assert_eq!( expression.as_literal().unwrap().as_string(), Some("hello\n\"OG\""), ); }

    #[test] fn parses_field_reference() { let expression = parse_expression("country").unwrap(); let field = expression.as_field().unwrap(); assert_eq!(field.len(), 1); assert_eq!(field.first(), "country"); assert_eq!(field.last(), "country"); }

    #[test] fn parses_nested_field_reference() { let expression = parse_expression("user.address.city").unwrap(); let field = expression.as_field().unwrap(); assert_eq!(field.len(), 3); assert_eq!( field.iter().collect::<Vec<_>>(), vec!["user", "address", "city"], ); assert_eq!(field.to_string(), "user.address.city",); }

    #[test] fn parses_unicode_field_reference() { let expression = parse_expression("utilisateur.adresse.ville").unwrap(); let field = expression.as_field().unwrap(); assert_eq!(field.len(), 3); }

    #[test] fn parses_unary_not() { let expression = parse_expression("!active").unwrap(); let ExpressionKind::Unary { operator, operand } = expression.kind() else { panic!("expected unary expression"); }; assert_eq!(*operator, UnaryOperator::Not); assert_eq!(operand.as_field().unwrap().first(), "active",); }

    #[test] fn parses_unary_negation() { let expression = parse_expression("-age").unwrap(); let ExpressionKind::Unary { operator, operand } = expression.kind() else { panic!("expected unary expression"); }; assert_eq!(*operator, UnaryOperator::Negate); assert_eq!(operand.as_field().unwrap().first(), "age"); }
    #[test] fn folds_signed_numeric_literals() { let negative = parse_expression("-18").unwrap(); assert_eq!(negative.as_literal().unwrap().as_number_text(), Some("-18")); assert_eq!(negative.span(), Span::new(0, 3)); let positive = parse_expression("+2.5").unwrap(); assert_eq!(positive.as_literal().unwrap().as_number_text(), Some("2.5")); let ExpressionKind::Binary { right, .. } = parse_expression("age > -1").unwrap().kind().clone() else { panic!("expected comparison"); }; assert_eq!(right.as_literal().unwrap().as_number_text(), Some("-1")); let ExpressionKind::Binary { operator, .. } = parse_expression("a - 1").unwrap().kind().clone() else { panic!("expected subtraction"); }; assert_eq!(operator, BinaryOperator::Subtract); }

    #[test] fn parses_comparison() { let expression = parse_expression("age >= 18").unwrap(); let ExpressionKind::Binary { left, operator, right, } = expression.kind() else { panic!("expected binary expression"); }; assert_eq!(*operator, BinaryOperator::GreaterThanOrEqual,); assert_eq!(left.as_field().unwrap().first(), "age",); assert_eq!(right.as_literal().unwrap().as_number_text(), Some("18"),); }

    #[test] fn multiplication_has_higher_precedence_than_addition() { let expression = parse_expression("1 + 2 * 3").unwrap(); let ExpressionKind::Binary { left, operator, right, } = expression.kind() else { panic!("expected addition"); }; assert_eq!(*operator, BinaryOperator::Add); assert!(left.is_literal()); let ExpressionKind::Binary { operator, .. } = right.kind() else { panic!("expected multiplication"); }; assert_eq!(*operator, BinaryOperator::Multiply,); }

    #[test] fn comparison_has_higher_precedence_than_and() { let expression = parse_expression("age >= 18 && active == true").unwrap(); let ExpressionKind::Binary { left, operator, right, } = expression.kind() else { panic!("expected boolean expression"); }; assert_eq!(*operator, BinaryOperator::And); assert!(matches!( left.kind(), ExpressionKind::Binary { operator: BinaryOperator::GreaterThanOrEqual, .. }, )); assert!(matches!( right.kind(), ExpressionKind::Binary { operator: BinaryOperator::Equal, .. }, )); }

    #[test] fn and_has_higher_precedence_than_or() { let expression = parse_expression("a || b && c").unwrap(); let ExpressionKind::Binary { operator, right, .. } = expression.kind() else { panic!("expected binary expression"); }; assert_eq!(*operator, BinaryOperator::Or); assert!(matches!( right.kind(), ExpressionKind::Binary { operator: BinaryOperator::And, .. }, )); }

    #[test] fn subtraction_is_left_associative() { let expression = parse_expression("10 - 5 - 2").unwrap(); let ExpressionKind::Binary { left, operator, .. } = expression.kind() else { panic!("expected subtraction"); }; assert_eq!(*operator, BinaryOperator::Subtract,); assert!(matches!( left.kind(), ExpressionKind::Binary { operator: BinaryOperator::Subtract, .. }, )); }

    #[test] fn parentheses_override_precedence() { let expression = parse_expression("(1 + 2) * 3").unwrap(); let ExpressionKind::Binary { left, operator, .. } = expression.kind() else { panic!("expected multiplication"); }; assert_eq!(*operator, BinaryOperator::Multiply,); assert!(matches!(left.kind(), ExpressionKind::Group(_),)); }

    #[test] fn preserves_group_span() { let expression = parse_expression("(age >= 18)").unwrap(); assert_eq!(expression.span(), Span::new(0, 11),); assert!(matches!(expression.kind(), ExpressionKind::Group(_),)); }

    #[test] fn ignores_outer_whitespace() { let expression = parse_expression("  age >= 18  ").unwrap(); assert_eq!(expression.span(), Span::new(2, 11),); }

    #[test] fn rejects_empty_expression() { let error = parse_expression("   ").unwrap_err(); assert_eq!(error.kind(), &ExpressionErrorKind::EmptyExpression,); }

    #[test] fn rejects_empty_group() { let error = parse_expression("()").unwrap_err(); assert_eq!(error.kind(), &ExpressionErrorKind::EmptyGroup,); }

    #[test] fn rejects_missing_right_operand() { let error = parse_expression("age >").unwrap_err(); assert_eq!( error.kind(), &ExpressionErrorKind::UnexpectedEnd { expected: "expression", }, ); }

    #[test] fn rejects_missing_closing_parenthesis() { let error = parse_expression("(age > 18").unwrap_err(); assert_eq!( error.kind(), &ExpressionErrorKind::UnexpectedToken { found: Arc::from(""), expected: "')'", }, ); }

    #[test] fn rejects_single_equal() { let error = parse_expression("age = 18").unwrap_err(); assert_eq!(error.kind(), &ExpressionErrorKind::SingleEqual,); }

    #[test] fn rejects_single_ampersand() { let error = parse_expression("a & b").unwrap_err(); assert_eq!(error.kind(), &ExpressionErrorKind::SingleAmpersand,); }

    #[test] fn rejects_pipeline_pipe_inside_expression() { let error = parse_expression("a | b").unwrap_err(); assert_eq!(error.kind(), &ExpressionErrorKind::SinglePipe,); }

    #[test] fn rejects_unterminated_string() { let error = parse_expression(r#""hello"#).unwrap_err(); assert_eq!(error.kind(), &ExpressionErrorKind::UnterminatedString,); }

    #[test] fn decodes_unicode_escapes() { assert_eq!(parse_expression(r#""a\u0001b\ud83d\ude00""#).unwrap().as_literal().unwrap().as_string(), Some("a\u{1}b😀")); assert!(parse_expression(r#""\u12""#).is_err()); }
    #[test] fn rejects_invalid_string_escape() { let error = parse_expression(r#""\q""#).unwrap_err(); assert_eq!( error.kind(), &ExpressionErrorKind::InvalidStringEscape { character: 'q' }, ); }

    #[test] fn rejects_incomplete_exponent() { let error = parse_expression("1e+").unwrap_err(); assert_eq!(error.kind(), &ExpressionErrorKind::InvalidNumber,); }

    #[test] fn rejects_trailing_expression() { let error = parse_expression("age 18").unwrap_err(); assert_eq!( error.kind(), &ExpressionErrorKind::UnexpectedToken { found: Arc::from("18"), expected: "end of expression", }, ); }

    #[test] fn creates_field_path_directly() { let path = ExpressionFieldPath::new(["user", "address", "city"]).unwrap(); assert_eq!(path.to_string(), "user.address.city",); }

    #[test] fn rejects_empty_direct_field_path() { let segments: [&str; 0] = []; let error = ExpressionFieldPath::new(segments).unwrap_err(); assert_eq!(error.kind(), &ExpressionErrorKind::EmptyFieldPath,); }

    #[test] fn rejects_empty_direct_field_segment() { let error = ExpressionFieldPath::new(["user", "", "name"]).unwrap_err(); assert_eq!( error.kind(), &ExpressionErrorKind::EmptyFieldSegment { index: 1 }, ); }

    #[test] fn binary_operator_categories_are_correct() { assert!(BinaryOperator::And.is_boolean()); assert!(BinaryOperator::Equal.is_comparison()); assert!(BinaryOperator::Add.is_arithmetic()); assert!(!BinaryOperator::And.is_arithmetic()); assert!(!BinaryOperator::Equal.is_boolean()); assert!(!BinaryOperator::Add.is_comparison()); }

    #[test] fn exposes_complete_structural_view() { let expression = parse_expression("(age + 1) >= 18").unwrap(); let ExpressionView::Binary { left, operator, right, } = expression.view() else { panic!("expected binary view"); }; assert_eq!(operator, BinaryOperator::GreaterThanOrEqual); assert!(left.is_group()); assert!(right.is_literal()); let ExpressionView::Binary { operator, .. } = left.ungrouped().view() else { panic!("expected grouped addition"); }; assert_eq!(operator, BinaryOperator::Add); }

    #[test] fn unary_and_binary_accessors_are_consistent() { let unary = parse_expression("!active").unwrap(); let (operator, operand) = unary.as_unary().unwrap(); assert_eq!(operator, UnaryOperator::Not); assert_eq!(operand.as_field().unwrap().first(), "active"); let binary = parse_expression("age >= 18").unwrap(); let (left, operator, right) = binary.as_binary().unwrap(); assert_eq!(operator, BinaryOperator::GreaterThanOrEqual); assert!(left.is_field()); assert!(right.is_literal()); }

    #[test] fn group_accessors_preserve_and_remove_parentheses() { let expression = parse_expression("((active))").unwrap(); assert!(expression.is_group()); assert!(expression.as_group().is_some()); assert!(expression.ungrouped().is_field()); }

    #[test] fn operator_categories_cover_unary_semantics() { assert!(UnaryOperator::Not.is_boolean()); assert!(!UnaryOperator::Not.is_numeric()); assert!(UnaryOperator::Negate.is_numeric()); assert!(UnaryOperator::Positive.is_numeric()); }

    #[test] fn word_boolean_operators_follow_standard_precedence() { let expression = parse_expression("a == 1 or b == 2 and not disabled").unwrap(); let ExpressionView::Binary { operator, right, .. } = expression.view() else { panic!("expected top-level binary expression"); }; assert_eq!(operator, BinaryOperator::Or); let ExpressionView::Binary { operator, right: and_right, .. } = right.view() else { panic!("expected right-hand conjunction"); }; assert_eq!(operator, BinaryOperator::And); assert!(matches!( and_right.view(), ExpressionView::Unary { operator: UnaryOperator::Not, .. } )); }

    fn assert_same_expression_structure(left: &Expression, right: &Expression) {
        match (left.view(), right.view()) {
            (ExpressionView::Literal(left), ExpressionView::Literal(right)) => {
                assert_eq!(left, right);
            }
            (ExpressionView::Field(left), ExpressionView::Field(right)) => {
                assert_eq!(left, right);
            }
            (
                ExpressionView::Unary {
                    operator: left_operator,
                    operand: left_operand,
                },
                ExpressionView::Unary {
                    operator: right_operator,
                    operand: right_operand,
                },
            ) => {
                assert_eq!(left_operator, right_operator);
                assert_same_expression_structure(left_operand, right_operand);
            }
            (
                ExpressionView::Binary {
                    left: left_left,
                    operator: left_operator,
                    right: left_right,
                },
                ExpressionView::Binary {
                    left: right_left,
                    operator: right_operator,
                    right: right_right,
                },
            ) => {
                assert_eq!(left_operator, right_operator);
                assert_same_expression_structure(left_left, right_left);
                assert_same_expression_structure(left_right, right_right);
            }
            (ExpressionView::Group(left), ExpressionView::Group(right)) => {
                assert_same_expression_structure(left, right);
            }
            (left, right) => {
                panic!("expression structures differ: left={left:?}, right={right:?}");
            }
        }
    }

    #[test] fn symbolic_and_word_boolean_operators_build_equivalent_trees() { let words = parse_expression("a == 1 and (b == 2 or not disabled)").unwrap(); let symbols = parse_expression("a == 1 && (b == 2 || !disabled)").unwrap(); assert_same_expression_structure(&words, &symbols); }
    #[test] fn not_binds_looser_than_comparisons() { let expression = parse_expression("not city == \"Berlin\" and active").unwrap(); let ExpressionView::Binary { left, operator: BinaryOperator::And, .. } = expression.view() else { panic!("expected conjunction"); }; assert!(matches!(left.view(), ExpressionView::Unary { operator: UnaryOperator::Not, operand } if operand.is_binary())); assert!(matches!(parse_expression("!x in tags").unwrap().view(), ExpressionView::Unary { operand, .. } if operand.is_binary())); }
    #[test] fn between_desugars_to_a_call() { let expression = parse_expression("age between 18 and 30 + 1 and active").unwrap(); let ExpressionView::Binary { left, operator: BinaryOperator::And, .. } = expression.view() else { panic!("expected conjunction"); }; assert!(matches!(left.view(), ExpressionView::Call { function: "between", arguments } if arguments.len() == 3 && arguments[2].is_binary())); assert!(matches!(parse_expression("age not between 1 and 2").unwrap().view(), ExpressionView::Unary { operator: UnaryOperator::Not, .. })); assert!(parse_expression("age between 1").is_err()); assert!(parse_expression("between == 1").unwrap().is_binary()); }
    #[test] fn question_mark_makes_a_path_optional() { for source in ["nick?", "profile?.country", "profile.country?"] { let expression = parse_expression(source).unwrap(); assert!(matches!(expression.view(), ExpressionView::Call { function: "optional", arguments } if arguments[0].is_field()), "{source}"); } assert_eq!(parse_expression("profile?.country").unwrap().to_string(), "profile.country?"); assert!(parse_expression("f(x)?").is_err()); }
    #[test] fn constant_conditions_choose_their_branch() { for (text, folded) in [("if(false, null, f)", "f"), ("if(1 > 0, f, g)", "f"), ("true or f", "true"), ("false and f", "false"), ("true and f", "true and f"), ("coalesce(null, f, 1)", "coalesce(f, 1)"), ("coalesce(null, 2, f)", "2"), ("case(false, 1, g, 2, 3)", "case(g, 2, 3)"), ("case(false, 1, true, f, 3)", "f"), ("if(g, 1, 2)", "if(g, 1, 2)")] { assert_eq!(parse_expression(text).unwrap().to_string(), parse_expression(folded).unwrap().to_string(), "{text}"); } }
    #[test] fn constant_subtrees_are_computed_once() { let expression = parse_expression("price * (2 + 3) > len([1, 2]) and now() > 0").unwrap(); assert!(expression.constant().is_none()); let ExpressionView::Binary { left, right, .. } = expression.view() else { panic!("expected and") }; let ExpressionView::Binary { left: product, right: length, .. } = left.view() else { panic!("expected comparison") }; assert_eq!(length.constant(), Some(&crate::Value::unsigned(2))); let ExpressionView::Binary { right: sum, .. } = product.view() else { panic!("expected product") }; assert_eq!(sum.constant(), Some(&crate::Value::signed(5))); assert!(right.constant().is_none(), "now() is volatile"); assert!(parse_expression("1 / 0").unwrap().constant().is_none(), "failing constants stay lazy"); assert_eq!(parse_expression("{\"a\": [1]}").unwrap().constant().and_then(crate::Value::as_object).map(crate::Document::len), Some(1)); }
    #[test] fn display_round_trips_through_the_parser() { for source in ["a.b >= -2 and not (c or d)", "round(price * qty, 2) not in [1, \"x\"]", "not x == 1", "-(a + b)", "nick? == \"Z\""] { let expression = parse_expression(source).unwrap(); let reparsed = parse_expression(&expression.to_string()).unwrap(); assert_eq!(reparsed.to_string(), expression.to_string(), "{source}"); } }
    #[test] fn parses_calls_arrays_and_membership() { let expression = parse_expression("round(price * qty, 2) not in [1, a.b]").unwrap(); let ExpressionKind::Binary { left, operator, right } = expression.kind() else { panic!("expected membership"); }; assert_eq!(*operator, BinaryOperator::NotIn); assert!(matches!(left.view(), ExpressionView::Call { function: "round", arguments } if arguments.len() == 2)); assert!(matches!(right.view(), ExpressionView::Array(items) if items.len() == 2)); let mut fields = Vec::new(); expression.for_each_field(&mut |field| fields.push(field.to_string())); assert_eq!(fields, ["price", "qty", "a.b"]); assert!(matches!(parse_expression("x in tags").unwrap().kind(), ExpressionKind::Binary { operator: BinaryOperator::In, .. })); assert!(parse_expression("f(").is_err()); assert!(parse_expression("[1, ").is_err()); }
}

