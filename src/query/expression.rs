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
}

impl Expression {
    /// Creates an expression.
    #[must_use]
    #[inline]
    pub const fn new(kind: ExpressionKind, span: Span) -> Self {
        Self { kind, span }
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
    pub fn unary(operator: UnaryOperator, operand: Expression, span: Span) -> Self {
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
        left: Expression,
        operator: BinaryOperator,
        right: Expression,
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
    pub fn group(expression: Expression, span: Span) -> Self {
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

    /// Consumes the expression and returns its kind.
    #[must_use]
    pub fn into_kind(self) -> ExpressionKind {
        self.kind
    }

    /// Returns the expression as a literal when applicable.
    #[must_use]
    pub const fn as_literal(&self) -> Option<&Literal> {
        match &self.kind {
            ExpressionKind::Literal(literal) => Some(literal),
            ExpressionKind::Field(_)
            | ExpressionKind::Unary { .. }
            | ExpressionKind::Binary { .. }
            | ExpressionKind::Group(_) => None,
        }
    }

    /// Returns the expression as a field reference when applicable.
    #[must_use]
    pub const fn as_field(&self) -> Option<&ExpressionFieldPath> {
        match &self.kind {
            ExpressionKind::Field(path) => Some(path),
            ExpressionKind::Literal(_)
            | ExpressionKind::Unary { .. }
            | ExpressionKind::Binary { .. }
            | ExpressionKind::Group(_) => None,
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
    pub fn as_unary(&self) -> Option<(UnaryOperator, &Expression)> {
        match &self.kind {
            ExpressionKind::Unary { operator, operand } => Some((*operator, operand.as_ref())),
            ExpressionKind::Literal(_)
            | ExpressionKind::Field(_)
            | ExpressionKind::Binary { .. }
            | ExpressionKind::Group(_) => None,
        }
    }

    /// Returns the binary operands and operator when applicable.
    #[must_use]
    pub fn as_binary(&self) -> Option<(&Expression, BinaryOperator, &Expression)> {
        match &self.kind {
            ExpressionKind::Binary {
                left,
                operator,
                right,
            } => Some((left.as_ref(), *operator, right.as_ref())),
            ExpressionKind::Literal(_)
            | ExpressionKind::Field(_)
            | ExpressionKind::Unary { .. }
            | ExpressionKind::Group(_) => None,
        }
    }

    /// Returns the inner expression of an explicit group.
    #[must_use]
    pub fn as_group(&self) -> Option<&Expression> {
        match &self.kind {
            ExpressionKind::Group(expression) => Some(expression.as_ref()),
            ExpressionKind::Literal(_)
            | ExpressionKind::Field(_)
            | ExpressionKind::Unary { .. }
            | ExpressionKind::Binary { .. } => None,
        }
    }

    /// Removes all transparent parenthesized wrappers.
    ///
    /// Group nodes remain preserved in the AST for diagnostics, while semantic
    /// integration can use this accessor when parentheses must not affect
    /// evaluation.
    #[must_use]
    pub fn ungrouped(&self) -> &Expression {
        let mut expression = self;

        while let ExpressionKind::Group(inner) = expression.kind() {
            expression = inner.as_ref();
        }

        expression
    }

    /// Returns a stable structural view of this expression.
    ///
    /// Unlike the evaluation-specific `ExpressionNode`, this view covers every
    /// syntax form accepted by the parser, including arithmetic operators and
    /// explicit groups.
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
            | Self::GreaterThanOrEqual => 4,
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
    let trimmed = source.trim();
    if matches!(trimmed.as_bytes().first(), Some(b'{') | Some(b'[')) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if value.is_object() || value.is_array() {
                let canonical =
                    serde_json::to_string(&value).expect("parsed JSON value must serialize");
                return Ok(Expression::literal(
                    Literal::Json(Arc::from(canonical)),
                    Span::new(0, source.len()),
                ));
            }
        }
    }
    ExpressionParser::new(source).parse()
}

/// Expression parser.
#[derive(Clone, Debug)]
pub struct ExpressionParser<'source> {
    source: &'source str,
    lexer: ExpressionLexer<'source>,
    current: ExpressionToken,
}

impl<'source> ExpressionParser<'source> {
    /// Creates an expression parser.
    #[must_use]
    #[inline]
    pub fn new(source: &'source str) -> Self {
        Self {
            source,
            lexer: ExpressionLexer::new(source),
            current: ExpressionToken::end(0),
        }
    }

    /// Parses one complete expression.
    ///
    /// # Errors
    ///
    /// Returns an expression syntax error.
    pub fn parse(mut self) -> ExpressionResult<Expression> {
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
            let Some(operator) = binary_operator(self.current.kind) else {
                break;
            };

            let precedence = operator.precedence();

            if precedence < minimum_precedence {
                break;
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

            ExpressionTokenKind::Identifier
            | ExpressionTokenKind::Number
            | ExpressionTokenKind::String
            | ExpressionTokenKind::True
            | ExpressionTokenKind::False
            | ExpressionTokenKind::Null
            | ExpressionTokenKind::LeftParen
            | ExpressionTokenKind::RightParen
            | ExpressionTokenKind::Dot
            | ExpressionTokenKind::Star
            | ExpressionTokenKind::Slash
            | ExpressionTokenKind::Percent
            | ExpressionTokenKind::EqualEqual
            | ExpressionTokenKind::BangEqual
            | ExpressionTokenKind::Less
            | ExpressionTokenKind::LessEqual
            | ExpressionTokenKind::Greater
            | ExpressionTokenKind::GreaterEqual
            | ExpressionTokenKind::AndAnd
            | ExpressionTokenKind::OrOr
            | ExpressionTokenKind::End => None,
        };

        let Some(operator) = operator else {
            return self.parse_primary();
        };

        let start = self.current.span.start();

        self.advance()?;

        let operand = self.parse_unary()?;
        let span = Span::new(start, operand.span().end());

        Ok(Expression::new(
            ExpressionKind::Unary {
                operator,
                operand: Box::new(operand),
            },
            span,
        ))
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

            ExpressionTokenKind::End => Err(ExpressionError::unexpected_end(
                self.current.span,
                "expression",
            )),

            ExpressionTokenKind::RightParen
            | ExpressionTokenKind::Dot
            | ExpressionTokenKind::Plus
            | ExpressionTokenKind::Minus
            | ExpressionTokenKind::Star
            | ExpressionTokenKind::Slash
            | ExpressionTokenKind::Percent
            | ExpressionTokenKind::Bang
            | ExpressionTokenKind::EqualEqual
            | ExpressionTokenKind::BangEqual
            | ExpressionTokenKind::Less
            | ExpressionTokenKind::LessEqual
            | ExpressionTokenKind::Greater
            | ExpressionTokenKind::GreaterEqual
            | ExpressionTokenKind::AndAnd
            | ExpressionTokenKind::OrOr => Err(ExpressionError::unexpected_token(
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

        while self.current.kind == ExpressionTokenKind::Dot {
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

        Ok(Expression::new(
            ExpressionKind::Field(path),
            Span::new(start, end),
        ))
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

    fn advance(&mut self) -> ExpressionResult<()> {
        self.current = self.lexer.next_token()?;
        Ok(())
    }
}

fn binary_operator(kind: ExpressionTokenKind) -> Option<BinaryOperator> {
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

        ExpressionTokenKind::Identifier
        | ExpressionTokenKind::Number
        | ExpressionTokenKind::String
        | ExpressionTokenKind::True
        | ExpressionTokenKind::False
        | ExpressionTokenKind::Null
        | ExpressionTokenKind::LeftParen
        | ExpressionTokenKind::RightParen
        | ExpressionTokenKind::Dot
        | ExpressionTokenKind::Bang
        | ExpressionTokenKind::End => None,
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
    Dot,

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

    fn text<'source>(self, source: &'source str) -> &'source str {
        source.get(self.span.start()..self.span.end()).unwrap_or("")
    }
}

#[derive(Clone, Debug)]
struct ExpressionLexer<'source> {
    source: &'source str,
    offset: usize,
}

impl<'source> ExpressionLexer<'source> {
    #[inline]
    const fn new(source: &'source str) -> Self {
        Self { source, offset: 0 }
    }

    fn next_token(&mut self) -> ExpressionResult<ExpressionToken> {
        self.skip_whitespace();

        let start = self.offset;

        let Some(character) = self.peek() else {
            return Ok(ExpressionToken::end(self.offset));
        };

        if is_identifier_start(character) {
            return Ok(self.lex_identifier());
        }

        if character.is_ascii_digit()
            || (character == '.' && self.peek_next().is_some_and(|next| next.is_ascii_digit()))
        {
            return self.lex_number();
        }

        match character {
            '"' => self.lex_string(),

            '(' => {
                self.advance();

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::LeftParen,
                    Span::new(start, self.offset),
                ))
            }

            ')' => {
                self.advance();

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::RightParen,
                    Span::new(start, self.offset),
                ))
            }

            '.' => {
                self.advance();

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::Dot,
                    Span::new(start, self.offset),
                ))
            }

            '+' => {
                self.advance();

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::Plus,
                    Span::new(start, self.offset),
                ))
            }

            '-' => {
                self.advance();

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::Minus,
                    Span::new(start, self.offset),
                ))
            }

            '*' => {
                self.advance();

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::Star,
                    Span::new(start, self.offset),
                ))
            }

            '/' => {
                self.advance();

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::Slash,
                    Span::new(start, self.offset),
                ))
            }

            '%' => {
                self.advance();

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::Percent,
                    Span::new(start, self.offset),
                ))
            }

            '!' => {
                self.advance();

                let kind = if self.consume('=') {
                    ExpressionTokenKind::BangEqual
                } else {
                    ExpressionTokenKind::Bang
                };

                Ok(ExpressionToken::new(kind, Span::new(start, self.offset)))
            }

            '=' => {
                self.advance();

                if !self.consume('=') {
                    return Err(ExpressionError::single_equal(Span::new(start, self.offset)));
                }

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::EqualEqual,
                    Span::new(start, self.offset),
                ))
            }

            '<' => {
                self.advance();

                let kind = if self.consume('=') {
                    ExpressionTokenKind::LessEqual
                } else {
                    ExpressionTokenKind::Less
                };

                Ok(ExpressionToken::new(kind, Span::new(start, self.offset)))
            }

            '>' => {
                self.advance();

                let kind = if self.consume('=') {
                    ExpressionTokenKind::GreaterEqual
                } else {
                    ExpressionTokenKind::Greater
                };

                Ok(ExpressionToken::new(kind, Span::new(start, self.offset)))
            }

            '&' => {
                self.advance();

                if !self.consume('&') {
                    return Err(ExpressionError::single_ampersand(Span::new(
                        start,
                        self.offset,
                    )));
                }

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::AndAnd,
                    Span::new(start, self.offset),
                ))
            }

            '|' => {
                self.advance();

                if !self.consume('|') {
                    return Err(ExpressionError::single_pipe(Span::new(start, self.offset)));
                }

                Ok(ExpressionToken::new(
                    ExpressionTokenKind::OrOr,
                    Span::new(start, self.offset),
                ))
            }

            _ => {
                self.advance();

                Err(ExpressionError::invalid_character(
                    character,
                    Span::new(start, self.offset),
                ))
            }
        }
    }

    fn lex_identifier(&mut self) -> ExpressionToken {
        let start = self.offset;

        self.advance();

        while self.peek().is_some_and(is_identifier_continue) {
            self.advance();
        }

        let span = Span::new(start, self.offset);
        let text = self
            .source
            .get(start..self.offset)
            .expect("lexer offsets must remain valid");

        let kind = match text {
            "true" => ExpressionTokenKind::True,
            "false" => ExpressionTokenKind::False,
            "null" => ExpressionTokenKind::Null,
            "and" => ExpressionTokenKind::AndAnd,
            "or" => ExpressionTokenKind::OrOr,
            "not" => ExpressionTokenKind::Bang,
            _ => ExpressionTokenKind::Identifier,
        };

        ExpressionToken::new(kind, span)
    }

    fn lex_number(&mut self) -> ExpressionResult<ExpressionToken> {
        let start = self.offset;
        let mut saw_digit = false;

        while self.peek().is_some_and(|value| value.is_ascii_digit()) {
            saw_digit = true;
            self.advance();
        }

        if self.peek() == Some('.') {
            self.advance();

            while self.peek().is_some_and(|value| value.is_ascii_digit()) {
                saw_digit = true;
                self.advance();
            }
        }

        if !saw_digit {
            return Err(ExpressionError::invalid_number(Span::new(
                start,
                self.offset,
            )));
        }

        if matches!(self.peek(), Some('e' | 'E')) {
            let exponent_start = self.offset;

            self.advance();

            if matches!(self.peek(), Some('+' | '-')) {
                self.advance();
            }

            let digits_start = self.offset;

            while self.peek().is_some_and(|value| value.is_ascii_digit()) {
                self.advance();
            }

            if self.offset == digits_start {
                return Err(ExpressionError::invalid_number(Span::new(
                    exponent_start,
                    self.offset,
                )));
            }
        }

        Ok(ExpressionToken::new(
            ExpressionTokenKind::Number,
            Span::new(start, self.offset),
        ))
    }

    fn lex_string(&mut self) -> ExpressionResult<ExpressionToken> {
        let start = self.offset;

        self.advance();

        while let Some(character) = self.peek() {
            match character {
                '"' => {
                    self.advance();

                    return Ok(ExpressionToken::new(
                        ExpressionTokenKind::String,
                        Span::new(start, self.offset),
                    ));
                }

                '\\' => {
                    self.advance();

                    if self.peek().is_none() {
                        break;
                    }

                    self.advance();
                }

                _ => {
                    self.advance();
                }
            }
        }

        Err(ExpressionError::unterminated_string(Span::new(
            start,
            self.offset,
        )))
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.advance();
        }
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() != Some(expected) {
            return false;
        }

        self.advance();
        true
    }

    fn peek(&self) -> Option<char> {
        self.source
            .get(self.offset..)
            .and_then(|remaining| remaining.chars().next())
    }

    fn peek_next(&self) -> Option<char> {
        let mut characters = self.source.get(self.offset..)?.chars();

        characters.next()?;
        characters.next()
    }

    fn advance(&mut self) -> Option<char> {
        let character = self.peek()?;

        self.offset += character.len_utf8();

        Some(character)
    }
}

fn decode_string_literal(source: &str, span: Span) -> ExpressionResult<String> {
    let Some(inner) = source
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    else {
        return Err(ExpressionError::unterminated_string(span));
    };

    let mut decoded = String::with_capacity(inner.len());
    let mut characters = inner.chars();

    while let Some(character) = characters.next() {
        if character != '\\' {
            decoded.push(character);
            continue;
        }

        let Some(escaped) = characters.next() else {
            return Err(ExpressionError::invalid_escape('\\', span));
        };

        match escaped {
            '"' => decoded.push('"'),
            '\\' => decoded.push('\\'),
            '/' => decoded.push('/'),
            'n' => decoded.push('\n'),
            'r' => decoded.push('\r'),
            't' => decoded.push('\t'),
            'b' => decoded.push('\u{0008}'),
            'f' => decoded.push('\u{000c}'),

            other => {
                return Err(ExpressionError::invalid_escape(other, span));
            }
        }
    }

    Ok(decoded)
}

fn is_identifier_start(character: char) -> bool {
    character == '_' || character.is_alphabetic()
}

fn is_identifier_continue(character: char) -> bool {
    character == '_' || character.is_alphabetic() || character.is_ascii_digit()
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

    fn empty_expression() -> Self {
        Self::new(ExpressionErrorKind::EmptyExpression, Span::at(0))
    }

    fn empty_group(span: Span) -> Self {
        Self::new(ExpressionErrorKind::EmptyGroup, span)
    }

    fn empty_field_path() -> Self {
        Self::new(ExpressionErrorKind::EmptyFieldPath, Span::at(0))
    }

    fn empty_field_segment(index: usize) -> Self {
        Self::new(
            ExpressionErrorKind::EmptyFieldSegment { index },
            Span::at(0),
        )
    }

    fn unexpected_end(span: Span, expected: &'static str) -> Self {
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

    fn invalid_character(character: char, span: Span) -> Self {
        Self::new(ExpressionErrorKind::InvalidCharacter { character }, span)
    }

    fn invalid_number(span: Span) -> Self {
        Self::new(ExpressionErrorKind::InvalidNumber, span)
    }

    fn unterminated_string(span: Span) -> Self {
        Self::new(ExpressionErrorKind::UnterminatedString, span)
    }

    fn invalid_escape(character: char, span: Span) -> Self {
        Self::new(ExpressionErrorKind::InvalidStringEscape { character }, span)
    }

    fn single_equal(span: Span) -> Self {
        Self::new(ExpressionErrorKind::SingleEqual, span)
    }

    fn single_ampersand(span: Span) -> Self {
        Self::new(ExpressionErrorKind::SingleAmpersand, span)
    }

    fn single_pipe(span: Span) -> Self {
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
                write!(formatter, "unexpected token {found:?}; expected {expected}",)
            }

            ExpressionErrorKind::InvalidCharacter { character } => {
                write!(formatter, "invalid expression character {character:?}",)
            }

            ExpressionErrorKind::InvalidNumber => formatter.write_str("invalid numeric literal"),

            ExpressionErrorKind::UnterminatedString => {
                formatter.write_str("unterminated string literal")
            }

            ExpressionErrorKind::InvalidStringEscape { character } => {
                write!(formatter, "invalid string escape \\{character}",)
            }

            ExpressionErrorKind::SingleEqual => formatter
                .write_str("single '=' is not an expression operator; use '==' for equality"),

            ExpressionErrorKind::SingleAmpersand => {
                formatter.write_str("single '&' is not supported; use '&&'")
            }

            ExpressionErrorKind::SinglePipe => {
                formatter.write_str("single '|' is not supported inside an expression; use '||'")
            }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_null_literal() {
        let expression = parse_expression("null").unwrap();

        assert_eq!(expression.kind(), &ExpressionKind::Literal(Literal::Null),);

        assert_eq!(expression.span(), Span::new(0, 4));
    }

    #[test]
    fn parses_boolean_literals() {
        let true_expression = parse_expression("true").unwrap();

        let false_expression = parse_expression("false").unwrap();

        assert_eq!(true_expression.as_literal().unwrap().as_bool(), Some(true),);

        assert_eq!(
            false_expression.as_literal().unwrap().as_bool(),
            Some(false),
        );
    }

    #[test]
    fn parses_integer_literal() {
        let expression = parse_expression("123").unwrap();

        assert_eq!(
            expression.as_literal().unwrap().as_number_text(),
            Some("123"),
        );
    }

    #[test]
    fn parses_decimal_literal() {
        let expression = parse_expression("12.50").unwrap();

        assert_eq!(
            expression.as_literal().unwrap().as_number_text(),
            Some("12.50"),
        );
    }

    #[test]
    fn parses_leading_decimal_point() {
        let expression = parse_expression(".75").unwrap();

        assert_eq!(
            expression.as_literal().unwrap().as_number_text(),
            Some(".75"),
        );
    }

    #[test]
    fn parses_exponent_literal() {
        let expression = parse_expression("1.5e-10").unwrap();

        assert_eq!(
            expression.as_literal().unwrap().as_number_text(),
            Some("1.5e-10"),
        );
    }

    #[test]
    fn preserves_numeric_literal_spelling() {
        let first = parse_expression("1").unwrap();
        let second = parse_expression("1.0").unwrap();

        assert_ne!(first.as_literal(), second.as_literal(),);
    }

    #[test]
    fn parses_string_literal() {
        let expression = parse_expression(r#""hello""#).unwrap();

        assert_eq!(expression.as_literal().unwrap().as_string(), Some("hello"),);
    }

    #[test]
    fn decodes_string_escapes() {
        let expression = parse_expression(r#""hello\n\"OG\"""#).unwrap();

        assert_eq!(
            expression.as_literal().unwrap().as_string(),
            Some("hello\n\"OG\""),
        );
    }

    #[test]
    fn parses_field_reference() {
        let expression = parse_expression("country").unwrap();

        let field = expression.as_field().unwrap();

        assert_eq!(field.len(), 1);
        assert_eq!(field.first(), "country");
        assert_eq!(field.last(), "country");
    }

    #[test]
    fn parses_nested_field_reference() {
        let expression = parse_expression("user.address.city").unwrap();

        let field = expression.as_field().unwrap();

        assert_eq!(field.len(), 3);
        assert_eq!(
            field.iter().collect::<Vec<_>>(),
            vec!["user", "address", "city"],
        );

        assert_eq!(field.to_string(), "user.address.city",);
    }

    #[test]
    fn parses_unicode_field_reference() {
        let expression = parse_expression("utilisateur.adresse.ville").unwrap();

        let field = expression.as_field().unwrap();

        assert_eq!(field.len(), 3);
    }

    #[test]
    fn parses_unary_not() {
        let expression = parse_expression("!active").unwrap();

        let ExpressionKind::Unary { operator, operand } = expression.kind() else {
            panic!("expected unary expression");
        };

        assert_eq!(*operator, UnaryOperator::Not);
        assert_eq!(operand.as_field().unwrap().first(), "active",);
    }

    #[test]
    fn parses_unary_negation() {
        let expression = parse_expression("-18").unwrap();

        let ExpressionKind::Unary { operator, operand } = expression.kind() else {
            panic!("expected unary expression");
        };

        assert_eq!(*operator, UnaryOperator::Negate);

        assert_eq!(operand.as_literal().unwrap().as_number_text(), Some("18"),);
    }

    #[test]
    fn parses_comparison() {
        let expression = parse_expression("age >= 18").unwrap();

        let ExpressionKind::Binary {
            left,
            operator,
            right,
        } = expression.kind()
        else {
            panic!("expected binary expression");
        };

        assert_eq!(*operator, BinaryOperator::GreaterThanOrEqual,);

        assert_eq!(left.as_field().unwrap().first(), "age",);

        assert_eq!(right.as_literal().unwrap().as_number_text(), Some("18"),);
    }

    #[test]
    fn multiplication_has_higher_precedence_than_addition() {
        let expression = parse_expression("1 + 2 * 3").unwrap();

        let ExpressionKind::Binary {
            left,
            operator,
            right,
        } = expression.kind()
        else {
            panic!("expected addition");
        };

        assert_eq!(*operator, BinaryOperator::Add);
        assert!(left.is_literal());

        let ExpressionKind::Binary { operator, .. } = right.kind() else {
            panic!("expected multiplication");
        };

        assert_eq!(*operator, BinaryOperator::Multiply,);
    }

    #[test]
    fn comparison_has_higher_precedence_than_and() {
        let expression = parse_expression("age >= 18 && active == true").unwrap();

        let ExpressionKind::Binary {
            left,
            operator,
            right,
        } = expression.kind()
        else {
            panic!("expected boolean expression");
        };

        assert_eq!(*operator, BinaryOperator::And);

        assert!(matches!(
            left.kind(),
            ExpressionKind::Binary {
                operator: BinaryOperator::GreaterThanOrEqual,
                ..
            },
        ));

        assert!(matches!(
            right.kind(),
            ExpressionKind::Binary {
                operator: BinaryOperator::Equal,
                ..
            },
        ));
    }

    #[test]
    fn and_has_higher_precedence_than_or() {
        let expression = parse_expression("a || b && c").unwrap();

        let ExpressionKind::Binary {
            operator, right, ..
        } = expression.kind()
        else {
            panic!("expected binary expression");
        };

        assert_eq!(*operator, BinaryOperator::Or);

        assert!(matches!(
            right.kind(),
            ExpressionKind::Binary {
                operator: BinaryOperator::And,
                ..
            },
        ));
    }

    #[test]
    fn subtraction_is_left_associative() {
        let expression = parse_expression("10 - 5 - 2").unwrap();

        let ExpressionKind::Binary { left, operator, .. } = expression.kind() else {
            panic!("expected subtraction");
        };

        assert_eq!(*operator, BinaryOperator::Subtract,);

        assert!(matches!(
            left.kind(),
            ExpressionKind::Binary {
                operator: BinaryOperator::Subtract,
                ..
            },
        ));
    }

    #[test]
    fn parentheses_override_precedence() {
        let expression = parse_expression("(1 + 2) * 3").unwrap();

        let ExpressionKind::Binary { left, operator, .. } = expression.kind() else {
            panic!("expected multiplication");
        };

        assert_eq!(*operator, BinaryOperator::Multiply,);

        assert!(matches!(left.kind(), ExpressionKind::Group(_),));
    }

    #[test]
    fn preserves_group_span() {
        let expression = parse_expression("(age >= 18)").unwrap();

        assert_eq!(expression.span(), Span::new(0, 11),);

        assert!(matches!(expression.kind(), ExpressionKind::Group(_),));
    }

    #[test]
    fn ignores_outer_whitespace() {
        let expression = parse_expression("  age >= 18  ").unwrap();

        assert_eq!(expression.span(), Span::new(2, 11),);
    }

    #[test]
    fn rejects_empty_expression() {
        let error = parse_expression("   ").unwrap_err();

        assert_eq!(error.kind(), &ExpressionErrorKind::EmptyExpression,);
    }

    #[test]
    fn rejects_empty_group() {
        let error = parse_expression("()").unwrap_err();

        assert_eq!(error.kind(), &ExpressionErrorKind::EmptyGroup,);
    }

    #[test]
    fn rejects_missing_right_operand() {
        let error = parse_expression("age >").unwrap_err();

        assert_eq!(
            error.kind(),
            &ExpressionErrorKind::UnexpectedEnd {
                expected: "expression",
            },
        );
    }

    #[test]
    fn rejects_missing_closing_parenthesis() {
        let error = parse_expression("(age > 18").unwrap_err();

        assert_eq!(
            error.kind(),
            &ExpressionErrorKind::UnexpectedToken {
                found: Arc::from(""),
                expected: "')'",
            },
        );
    }

    #[test]
    fn rejects_single_equal() {
        let error = parse_expression("age = 18").unwrap_err();

        assert_eq!(error.kind(), &ExpressionErrorKind::SingleEqual,);
    }

    #[test]
    fn rejects_single_ampersand() {
        let error = parse_expression("a & b").unwrap_err();

        assert_eq!(error.kind(), &ExpressionErrorKind::SingleAmpersand,);
    }

    #[test]
    fn rejects_pipeline_pipe_inside_expression() {
        let error = parse_expression("a | b").unwrap_err();

        assert_eq!(error.kind(), &ExpressionErrorKind::SinglePipe,);
    }

    #[test]
    fn rejects_unterminated_string() {
        let error = parse_expression(r#""hello"#).unwrap_err();

        assert_eq!(error.kind(), &ExpressionErrorKind::UnterminatedString,);
    }

    #[test]
    fn rejects_invalid_string_escape() {
        let error = parse_expression(r#""\q""#).unwrap_err();

        assert_eq!(
            error.kind(),
            &ExpressionErrorKind::InvalidStringEscape { character: 'q' },
        );
    }

    #[test]
    fn rejects_incomplete_exponent() {
        let error = parse_expression("1e+").unwrap_err();

        assert_eq!(error.kind(), &ExpressionErrorKind::InvalidNumber,);
    }

    #[test]
    fn rejects_trailing_expression() {
        let error = parse_expression("age 18").unwrap_err();

        assert_eq!(
            error.kind(),
            &ExpressionErrorKind::UnexpectedToken {
                found: Arc::from("18"),
                expected: "end of expression",
            },
        );
    }

    #[test]
    fn creates_field_path_directly() {
        let path = ExpressionFieldPath::new(["user", "address", "city"]).unwrap();

        assert_eq!(path.to_string(), "user.address.city",);
    }

    #[test]
    fn rejects_empty_direct_field_path() {
        let segments: [&str; 0] = [];

        let error = ExpressionFieldPath::new(segments).unwrap_err();

        assert_eq!(error.kind(), &ExpressionErrorKind::EmptyFieldPath,);
    }

    #[test]
    fn rejects_empty_direct_field_segment() {
        let error = ExpressionFieldPath::new(["user", "", "name"]).unwrap_err();

        assert_eq!(
            error.kind(),
            &ExpressionErrorKind::EmptyFieldSegment { index: 1 },
        );
    }

    #[test]
    fn binary_operator_categories_are_correct() {
        assert!(BinaryOperator::And.is_boolean());
        assert!(BinaryOperator::Equal.is_comparison());
        assert!(BinaryOperator::Add.is_arithmetic());

        assert!(!BinaryOperator::And.is_arithmetic());
        assert!(!BinaryOperator::Equal.is_boolean());
        assert!(!BinaryOperator::Add.is_comparison());
    }

    #[test]
    fn exposes_complete_structural_view() {
        let expression = parse_expression("(age + 1) >= 18").unwrap();

        let ExpressionView::Binary {
            left,
            operator,
            right,
        } = expression.view()
        else {
            panic!("expected binary view");
        };

        assert_eq!(operator, BinaryOperator::GreaterThanOrEqual);
        assert!(left.is_group());
        assert!(right.is_literal());

        let ExpressionView::Binary { operator, .. } = left.ungrouped().view() else {
            panic!("expected grouped addition");
        };

        assert_eq!(operator, BinaryOperator::Add);
    }

    #[test]
    fn unary_and_binary_accessors_are_consistent() {
        let unary = parse_expression("!active").unwrap();
        let (operator, operand) = unary.as_unary().unwrap();

        assert_eq!(operator, UnaryOperator::Not);
        assert_eq!(operand.as_field().unwrap().first(), "active");

        let binary = parse_expression("age >= 18").unwrap();
        let (left, operator, right) = binary.as_binary().unwrap();

        assert_eq!(operator, BinaryOperator::GreaterThanOrEqual);
        assert!(left.is_field());
        assert!(right.is_literal());
    }

    #[test]
    fn group_accessors_preserve_and_remove_parentheses() {
        let expression = parse_expression("((active))").unwrap();

        assert!(expression.is_group());
        assert!(expression.as_group().is_some());
        assert!(expression.ungrouped().is_field());
    }

    #[test]
    fn operator_categories_cover_unary_semantics() {
        assert!(UnaryOperator::Not.is_boolean());
        assert!(!UnaryOperator::Not.is_numeric());
        assert!(UnaryOperator::Negate.is_numeric());
        assert!(UnaryOperator::Positive.is_numeric());
    }

    #[test]
    fn word_boolean_operators_follow_standard_precedence() {
        let expression = parse_expression("a == 1 or b == 2 and not disabled").unwrap();
        let ExpressionView::Binary {
            operator, right, ..
        } = expression.view()
        else {
            panic!("expected top-level binary expression");
        };
        assert_eq!(operator, BinaryOperator::Or);
        let ExpressionView::Binary {
            operator,
            right: and_right,
            ..
        } = right.view()
        else {
            panic!("expected right-hand conjunction");
        };
        assert_eq!(operator, BinaryOperator::And);
        assert!(matches!(
            and_right.view(),
            ExpressionView::Unary {
                operator: UnaryOperator::Not,
                ..
            }
        ));
    }

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

    #[test]
    fn symbolic_and_word_boolean_operators_build_equivalent_trees() {
        let words = parse_expression("a == 1 and (b == 2 or not disabled)").unwrap();
        let symbols = parse_expression("a == 1 && (b == 2 || !disabled)").unwrap();

        assert_same_expression_structure(&words, &symbols);
    }
}
