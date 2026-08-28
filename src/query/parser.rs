//! Query parser.

use std::fmt;

use super::{
    lex, EndAst, LexError, NameAst, PipelineAst, SourceAliasAst, SourceAst, SourceKeyword, Span,
    Spanned, StageAst, SubPipelineAst, Token, TokenKind, TokenStream,
};

use super::ast::{
    ArrayAst, BooleanAst, NullAst, NumberAst, ObjectAst, ObjectFieldAst, ObjectKeyAst, StringAst,
    ValueAst,
};

/// Résultat d'une opération effectuée sur un flux déjà tokenisé.
pub type ParseResult<T> = Result<T, ParseError>;

/// Résultat du pipeline complet lexer + parser.
pub type QueryParseResult<T> = Result<T, QueryParseError>;

/// Erreur produite pendant l'analyse syntaxique.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ParseError {
    kind: ParseErrorKind,
    span: Span,
}

/// Catégorie d'erreur syntaxique.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseErrorKind {
    /// Une requête doit commencer par `from` ou `on`.
    ExpectedSourceKeyword { found: TokenKind },

    /// Le nom de collection est absent ou invalide.
    ExpectedCollectionName { found: TokenKind },

    /// Le mot-clé `as` doit être suivi d'un identifiant.
    ExpectedAliasName { found: TokenKind },

    /// Un point d'un nom qualifié doit être suivi d'un identifiant.
    ExpectedNameAfterDot { found: TokenKind },

    /// Un nouveau stage doit commencer par `|`.
    ExpectedPipe { found: TokenKind },

    /// Le nom d'un stage est absent ou invalide.
    ExpectedStageName { found: TokenKind },

    /// `end` a été rencontré hors d'un sous-pipeline.
    UnexpectedEndKeyword,

    /// Un stage composé a atteint la fin de l'entrée sans `| end`.
    UnclosedSubPipeline { stage_span: Span },

    /// Parenthèse fermante rencontrée sans parenthèse ouvrante correspondante.
    UnexpectedRightParenthesis,

    /// Parenthèse ouvrante non refermée.
    UnclosedParenthesis { opening_span: Span },

    /// Accolade fermante rencontrée sans accolade ouvrante correspondante.
    UnexpectedRightBrace,

    /// Accolade ouvrante non refermée dans les arguments d'un stage.
    UnclosedBrace { opening_span: Span },

    /// Crochet fermant rencontré sans crochet ouvrant correspondant.
    UnexpectedRightBracket,

    /// Crochet ouvrant non refermé dans les arguments d'un stage.
    UnclosedBracket { opening_span: Span },

    /// Une valeur était attendue.
    ExpectedValue { found: TokenKind },

    /// Une clé d'objet doit être un identifiant ou une chaîne.
    ExpectedObjectKey { found: TokenKind },

    /// Une clé d'objet doit être suivie de `:`.
    ExpectedColonAfterObjectKey { found: TokenKind },

    /// Un élément de tableau doit être suivi de `,` ou `]`.
    ExpectedCommaOrArrayEnd { found: TokenKind },

    /// Un champ d'objet doit être suivi de `,` ou `}`.
    ExpectedCommaOrObjectEnd { found: TokenKind },

    /// Tableau commencé mais non refermé.
    UnclosedArray { opening_span: Span },

    /// Objet commencé mais non refermé.
    UnclosedObject { opening_span: Span },

    /// Des tokens sont présents après la fin attendue de la requête.
    UnexpectedToken { found: TokenKind },
}

impl ParseError {
    /// Construit une erreur syntaxique.
    #[must_use]
    #[inline]
    pub const fn new(kind: ParseErrorKind, span: Span) -> Self {
        Self { kind, span }
    }

    /// Retourne la catégorie de l'erreur.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &ParseErrorKind {
        &self.kind
    }

    /// Retourne le span associé à l'erreur.
    #[must_use]
    #[inline]
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedSourceKeyword { found } => {
                write!(formatter, "expected `from` or `on`, found {found}")
            }

            Self::ExpectedCollectionName { found } => {
                write!(formatter, "expected collection name, found {found}")
            }

            Self::ExpectedAliasName { found } => {
                write!(formatter, "expected alias name after `as`, found {found}")
            }

            Self::ExpectedNameAfterDot { found } => {
                write!(formatter, "expected identifier after `.`, found {found}")
            }

            Self::ExpectedPipe { found } => {
                write!(formatter, "expected `|` before next stage, found {found}")
            }

            Self::ExpectedStageName { found } => {
                write!(formatter, "expected stage name, found {found}")
            }

            Self::UnexpectedEndKeyword => {
                formatter.write_str("unexpected `end` outside a sub-pipeline")
            }

            Self::UnclosedSubPipeline { stage_span } => {
                write!(
                    formatter,
                    "sub-pipeline opened by stage at {stage_span} is missing `| end`"
                )
            }

            Self::UnexpectedRightParenthesis => formatter.write_str("unexpected `)`"),

            Self::UnclosedParenthesis { opening_span } => {
                write!(formatter, "unclosed parenthesis opened at {opening_span}")
            }

            Self::UnexpectedRightBrace => formatter.write_str("unexpected `}`"),

            Self::UnclosedBrace { opening_span } => {
                write!(formatter, "unclosed brace opened at {opening_span}")
            }

            Self::UnexpectedRightBracket => formatter.write_str("unexpected `]`"),

            Self::UnclosedBracket { opening_span } => {
                write!(formatter, "unclosed bracket opened at {opening_span}")
            }

            Self::ExpectedValue { found } => {
                write!(formatter, "expected value, found {found}")
            }

            Self::ExpectedObjectKey { found } => {
                write!(formatter, "expected object key, found {found}")
            }

            Self::ExpectedColonAfterObjectKey { found } => {
                write!(formatter, "expected `:` after object key, found {found}")
            }

            Self::ExpectedCommaOrArrayEnd { found } => {
                write!(
                    formatter,
                    "expected `,` or `]` after array value, found {found}"
                )
            }

            Self::ExpectedCommaOrObjectEnd { found } => {
                write!(
                    formatter,
                    "expected `,` or `}}` after object field, found {found}"
                )
            }

            Self::UnclosedArray { opening_span } => {
                write!(formatter, "array opened at {opening_span} is missing `]`")
            }

            Self::UnclosedObject { opening_span } => {
                write!(formatter, "object opened at {opening_span} is missing `}}`")
            }

            Self::UnexpectedToken { found } => {
                write!(formatter, "unexpected token {found}")
            }
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} at {}", self.kind, self.span)
    }
}

impl std::error::Error for ParseError {}

/// Erreur du pipeline complet de lecture d'une requête.
///
/// Elle distingue volontairement :
///
/// - les erreurs lexicales ;
/// - les erreurs syntaxiques.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum QueryParseError {
    /// Erreur produite par le lexer.
    Lex(LexError),

    /// Erreur produite par le parser.
    Parse(ParseError),
}

impl QueryParseError {
    /// Retourne l'erreur lexicale lorsqu'elle existe.
    #[must_use]
    pub const fn as_lex_error(&self) -> Option<&LexError> {
        match self {
            Self::Lex(error) => Some(error),
            Self::Parse(_) => None,
        }
    }

    /// Retourne l'erreur syntaxique lorsqu'elle existe.
    #[must_use]
    pub const fn as_parse_error(&self) -> Option<&ParseError> {
        match self {
            Self::Lex(_) => None,
            Self::Parse(error) => Some(error),
        }
    }

    /// Retourne le span de l'erreur.
    #[must_use]
    #[inline]
    pub const fn span(&self) -> Span {
        match self {
            Self::Lex(error) => error.span(),
            Self::Parse(error) => error.span(),
        }
    }
}

impl fmt::Display for QueryParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lex(error) => write!(formatter, "lex error: {error}"),
            Self::Parse(error) => write!(formatter, "parse error: {error}"),
        }
    }
}

impl std::error::Error for QueryParseError {
    #[inline]
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Lex(error) => Some(error),
            Self::Parse(error) => Some(error),
        }
    }
}

impl From<LexError> for QueryParseError {
    fn from(error: LexError) -> Self {
        Self::Lex(error)
    }
}

impl From<ParseError> for QueryParseError {
    fn from(error: ParseError) -> Self {
        Self::Parse(error)
    }
}

/// Parser d'un flux de tokens OG.
///
/// Le parser emprunte le [`TokenStream`]. L'AST résultant ne conserve aucune
/// référence vers ce flux : il contient uniquement des spans et des types
/// syntaxiques possédés.
#[derive(Debug)]
pub struct Parser<'stream, 'source> {
    stream: &'stream TokenStream<'source>,
    cursor: usize,
}

impl<'stream, 'source> Parser<'stream, 'source> {
    /// Construit un parser au début d'un flux.
    #[must_use]
    #[inline]
    pub const fn new(stream: &'stream TokenStream<'source>) -> Self {
        Self { stream, cursor: 0 }
    }

    /// Retourne le flux analysé.
    #[must_use]
    pub const fn stream(&self) -> &'stream TokenStream<'source> {
        self.stream
    }

    /// Retourne l'index du token courant.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.cursor
    }

    /// Analyse un pipeline racine complet.
    pub fn parse_pipeline(mut self) -> ParseResult<PipelineAst> {
        let source = self.parse_source()?;
        let mut stages = Vec::new();

        while !self.is_at_end() {
            if !self.check(TokenKind::Pipe) {
                return Err(self.error_current(ParseErrorKind::ExpectedPipe {
                    found: self.current().kind(),
                }));
            }

            if self.check_next(TokenKind::EndKeyword) {
                return Err(ParseError::new(
                    ParseErrorKind::UnexpectedEndKeyword,
                    self.next_token().span(),
                ));
            }

            stages.push(self.parse_stage()?);
        }

        let span = stages
            .last()
            .map_or_else(|| source.span(), |stage| source.span().join(stage.span()));

        Ok(PipelineAst::new(source, stages, span))
    }

    /// Analyse une valeur complète et exige la fin du flux juste après elle.
    ///
    /// Cette entrée est indépendante du parsing de pipeline et permet aux
    /// couches spécialisées d'analyser le contenu d'un span d'arguments.
    pub fn parse_value(mut self) -> ParseResult<ValueAst> {
        let value = self.parse_value_node()?;

        if !self.is_at_end() {
            return Err(self.error_current(ParseErrorKind::UnexpectedToken {
                found: self.current().kind(),
            }));
        }

        Ok(value)
    }

    /// Analyse une valeur récursive à la position courante.
    fn parse_value_node(&mut self) -> ParseResult<ValueAst> {
        let token = self.current();

        match token.kind() {
            TokenKind::String => {
                self.advance();
                Ok(ValueAst::String(StringAst::new(token.span())))
            }

            TokenKind::Number => {
                self.advance();
                Ok(ValueAst::Number(NumberAst::new(token.span())))
            }

            TokenKind::True => {
                self.advance();
                Ok(ValueAst::Boolean(BooleanAst::new(true, token.span())))
            }

            TokenKind::False => {
                self.advance();
                Ok(ValueAst::Boolean(BooleanAst::new(false, token.span())))
            }

            TokenKind::Null => {
                self.advance();
                Ok(ValueAst::Null(NullAst::new(token.span())))
            }

            TokenKind::Identifier => {
                self.advance();
                Ok(ValueAst::Identifier(NameAst::new(token.span())))
            }

            TokenKind::LeftBracket => self.parse_array(),
            TokenKind::LeftBrace => self.parse_object(),

            found => Err(self.error_current(ParseErrorKind::ExpectedValue { found })),
        }
    }

    /// Analyse un tableau, avec virgule finale facultative.
    fn parse_array(&mut self) -> ParseResult<ValueAst> {
        let opening = self.consume(TokenKind::LeftBracket)?;
        let mut values = Vec::new();

        if self.check(TokenKind::RightBracket) {
            let closing = self.advance();
            let span = opening.span().join(closing.span());

            return Ok(ValueAst::Array(ArrayAst::new(
                opening.span(),
                values,
                closing.span(),
                span,
            )));
        }

        loop {
            if self.is_at_end() {
                return Err(ParseError::new(
                    ParseErrorKind::UnclosedArray {
                        opening_span: opening.span(),
                    },
                    self.current().span(),
                ));
            }

            values.push(self.parse_value_node()?);

            if self.check(TokenKind::Comma) {
                self.advance();

                if self.check(TokenKind::RightBracket) {
                    let closing = self.advance();
                    let span = opening.span().join(closing.span());

                    return Ok(ValueAst::Array(ArrayAst::new(
                        opening.span(),
                        values,
                        closing.span(),
                        span,
                    )));
                }

                if self.is_at_end() {
                    return Err(ParseError::new(
                        ParseErrorKind::UnclosedArray {
                            opening_span: opening.span(),
                        },
                        self.current().span(),
                    ));
                }

                continue;
            }

            if self.check(TokenKind::RightBracket) {
                let closing = self.advance();
                let span = opening.span().join(closing.span());

                return Ok(ValueAst::Array(ArrayAst::new(
                    opening.span(),
                    values,
                    closing.span(),
                    span,
                )));
            }

            if self.is_at_end() {
                return Err(ParseError::new(
                    ParseErrorKind::UnclosedArray {
                        opening_span: opening.span(),
                    },
                    self.current().span(),
                ));
            }

            return Err(self.error_current(ParseErrorKind::ExpectedCommaOrArrayEnd {
                found: self.current().kind(),
            }));
        }
    }

    /// Analyse un objet, avec clés identifiantes ou chaînes et virgule finale
    /// facultative.
    fn parse_object(&mut self) -> ParseResult<ValueAst> {
        let opening = self.consume(TokenKind::LeftBrace)?;
        let mut fields = Vec::new();

        if self.check(TokenKind::RightBrace) {
            let closing = self.advance();
            let span = opening.span().join(closing.span());

            return Ok(ValueAst::Object(ObjectAst::new(
                opening.span(),
                fields,
                closing.span(),
                span,
            )));
        }

        loop {
            if self.is_at_end() {
                return Err(ParseError::new(
                    ParseErrorKind::UnclosedObject {
                        opening_span: opening.span(),
                    },
                    self.current().span(),
                ));
            }

            let key_token = self.current();
            let key = match key_token.kind() {
                TokenKind::Identifier => {
                    self.advance();
                    ObjectKeyAst::Identifier(NameAst::new(key_token.span()))
                }

                TokenKind::String => {
                    self.advance();
                    ObjectKeyAst::String(StringAst::new(key_token.span()))
                }

                found => {
                    return Err(self.error_current(ParseErrorKind::ExpectedObjectKey { found }));
                }
            };

            if !self.check(TokenKind::Colon) {
                return Err(
                    self.error_current(ParseErrorKind::ExpectedColonAfterObjectKey {
                        found: self.current().kind(),
                    }),
                );
            }

            let colon = self.advance();
            let value = self.parse_value_node()?;
            let field_span = key.span().join(value.span());

            fields.push(ObjectFieldAst::new(key, colon.span(), value, field_span));

            if self.check(TokenKind::Comma) {
                self.advance();

                if self.check(TokenKind::RightBrace) {
                    let closing = self.advance();
                    let span = opening.span().join(closing.span());

                    return Ok(ValueAst::Object(ObjectAst::new(
                        opening.span(),
                        fields,
                        closing.span(),
                        span,
                    )));
                }

                if self.is_at_end() {
                    return Err(ParseError::new(
                        ParseErrorKind::UnclosedObject {
                            opening_span: opening.span(),
                        },
                        self.current().span(),
                    ));
                }

                continue;
            }

            if self.check(TokenKind::RightBrace) {
                let closing = self.advance();
                let span = opening.span().join(closing.span());

                return Ok(ValueAst::Object(ObjectAst::new(
                    opening.span(),
                    fields,
                    closing.span(),
                    span,
                )));
            }

            if self.is_at_end() {
                return Err(ParseError::new(
                    ParseErrorKind::UnclosedObject {
                        opening_span: opening.span(),
                    },
                    self.current().span(),
                ));
            }

            return Err(
                self.error_current(ParseErrorKind::ExpectedCommaOrObjectEnd {
                    found: self.current().kind(),
                }),
            );
        }
    }

    /// Analyse la source initiale du pipeline.
    fn parse_source(&mut self) -> ParseResult<SourceAst> {
        let keyword_token = self.current();

        let keyword = match keyword_token.kind() {
            TokenKind::From => SourceKeyword::From,
            TokenKind::On => SourceKeyword::On,

            found => {
                return Err(self.error_current(ParseErrorKind::ExpectedSourceKeyword { found }));
            }
        };

        self.advance();

        let collection = self.parse_qualified_collection_name()?;

        if self.check(TokenKind::As) {
            let as_token = self.advance();
            let alias_token = self.current();

            if alias_token.kind() != TokenKind::Identifier {
                return Err(self.error_current(ParseErrorKind::ExpectedAliasName {
                    found: alias_token.kind(),
                }));
            }

            self.advance();

            let alias_name = NameAst::new(alias_token.span());
            let alias_span = as_token.span().join(alias_token.span());
            let alias = SourceAliasAst::new(as_token.span(), alias_name, alias_span);
            let source_span = keyword_token.span().join(alias_span);

            Ok(SourceAst::with_alias(
                keyword,
                keyword_token.span(),
                collection,
                alias,
                source_span,
            ))
        } else {
            let span = keyword_token.span().join(collection.span());

            Ok(SourceAst::new(
                keyword,
                keyword_token.span(),
                collection,
                span,
            ))
        }
    }

    /// Analyse un nom de collection éventuellement qualifié.
    ///
    /// Exemples :
    ///
    /// ```text
    /// users
    /// _og.operations
    /// namespace.users.archive
    /// ```
    ///
    /// L'AST conserve le nom qualifié comme un unique [`NameAst`] couvrant
    /// l'ensemble du texte.
    fn parse_qualified_collection_name(&mut self) -> ParseResult<NameAst> {
        let first = self.current();

        if first.kind() != TokenKind::Identifier {
            return Err(self.error_current(ParseErrorKind::ExpectedCollectionName {
                found: first.kind(),
            }));
        }

        self.advance();

        let mut span = first.span();

        while self.check(TokenKind::Dot) {
            self.advance();

            let segment = self.current();

            if segment.kind() != TokenKind::Identifier {
                return Err(self.error_current(ParseErrorKind::ExpectedNameAfterDot {
                    found: segment.kind(),
                }));
            }

            span = span.join(segment.span());
            self.advance();
        }

        Ok(NameAst::new(span))
    }

    /// Analyse un stage simple ou composé.
    fn parse_stage(&mut self) -> ParseResult<StageAst> {
        let pipe = self.consume(TokenKind::Pipe)?;

        let name_token = self.current();

        if name_token.kind() == TokenKind::EndKeyword {
            return Err(ParseError::new(
                ParseErrorKind::UnexpectedEndKeyword,
                name_token.span(),
            ));
        }

        if !is_stage_name(name_token.kind()) {
            return Err(self.error_current(ParseErrorKind::ExpectedStageName {
                found: name_token.kind(),
            }));
        }

        self.advance();

        let name = NameAst::new(name_token.span());
        let (arguments_span, last_argument_span) =
            self.parse_stage_arguments(name_token.span().end())?;

        let header_end = last_argument_span.map_or(name_token.span().end(), Span::end);
        let header_span = Span::new(pipe.span().start(), header_end);

        if self.stage_opens_subpipeline(name_token.kind(), arguments_span) {
            let subpipeline = self.parse_subpipeline(header_span)?;
            let stage_span = Span::new(pipe.span().start(), subpipeline.span().end());

            Ok(StageAst::with_subpipeline(
                pipe.span(),
                name,
                arguments_span,
                header_span,
                subpipeline,
                stage_span,
            ))
        } else {
            Ok(StageAst::new(
                pipe.span(),
                name,
                arguments_span,
                header_span,
            ))
        }
    }

    /// Analyse le corps d'un stage composé jusqu'au `| end` correspondant.
    ///
    /// Les stages composés peuvent être imbriqués sans limite syntaxique fixe.
    fn parse_subpipeline(&mut self, opening_stage_span: Span) -> ParseResult<SubPipelineAst> {
        let mut stages = Vec::new();

        loop {
            if self.is_at_end() {
                return Err(ParseError::new(
                    ParseErrorKind::UnclosedSubPipeline {
                        stage_span: opening_stage_span,
                    },
                    self.current().span(),
                ));
            }

            if !self.check(TokenKind::Pipe) {
                return Err(self.error_current(ParseErrorKind::ExpectedPipe {
                    found: self.current().kind(),
                }));
            }

            if self.check_next(TokenKind::EndKeyword) {
                let end = self.parse_end_marker()?;

                let span = stages.first().map_or_else(
                    || end.span(),
                    |first: &StageAst| first.span().join(end.span()),
                );

                return Ok(SubPipelineAst::new(stages, end, span));
            }

            stages.push(self.parse_stage()?);
        }
    }

    /// Analyse une fermeture canonique `| end`.
    fn parse_end_marker(&mut self) -> ParseResult<EndAst> {
        let pipe = self.consume(TokenKind::Pipe)?;
        let keyword = self.consume(TokenKind::EndKeyword)?;
        let span = pipe.span().join(keyword.span());

        Ok(EndAst::new(pipe.span(), keyword.span(), span))
    }

    /// Trouve les limites des arguments d'un stage.
    ///
    /// Un pipe termine le stage uniquement lorsqu'il apparaît au niveau zéro
    /// des parenthèses :
    ///
    /// ```text
    /// | where active and (country == "FR" | custom)
    ///                                       ^ argument interne
    /// ```
    fn parse_stage_arguments(
        &mut self,
        empty_position: usize,
    ) -> ParseResult<(Span, Option<Span>)> {
        let mut opening_parentheses = Vec::new();
        let mut opening_braces = Vec::new();
        let mut opening_brackets = Vec::new();
        let mut first_span = None;
        let mut last_span = None;

        while !self.is_at_end() {
            let token = self.current();

            if token.kind() == TokenKind::Pipe
                && opening_parentheses.is_empty()
                && opening_braces.is_empty()
                && opening_brackets.is_empty()
            {
                break;
            }

            match token.kind() {
                TokenKind::LeftParen => {
                    opening_parentheses.push(token.span());
                }

                TokenKind::RightParen => {
                    if opening_parentheses.pop().is_none() {
                        return Err(ParseError::new(
                            ParseErrorKind::UnexpectedRightParenthesis,
                            token.span(),
                        ));
                    }
                }

                TokenKind::LeftBrace => {
                    opening_braces.push(token.span());
                }

                TokenKind::RightBrace => {
                    if opening_braces.pop().is_none() {
                        return Err(ParseError::new(
                            ParseErrorKind::UnexpectedRightBrace,
                            token.span(),
                        ));
                    }
                }

                TokenKind::LeftBracket => {
                    opening_brackets.push(token.span());
                }

                TokenKind::RightBracket => {
                    if opening_brackets.pop().is_none() {
                        return Err(ParseError::new(
                            ParseErrorKind::UnexpectedRightBracket,
                            token.span(),
                        ));
                    }
                }

                _ => {}
            }

            first_span.get_or_insert(token.span());
            last_span = Some(token.span());

            self.advance();
        }

        if let Some(opening_span) = opening_parentheses.last().copied() {
            return Err(ParseError::new(
                ParseErrorKind::UnclosedParenthesis { opening_span },
                self.current().span(),
            ));
        }

        if let Some(opening_span) = opening_braces.last().copied() {
            return Err(ParseError::new(
                ParseErrorKind::UnclosedBrace { opening_span },
                self.current().span(),
            ));
        }

        if let Some(opening_span) = opening_brackets.last().copied() {
            return Err(ParseError::new(
                ParseErrorKind::UnclosedBracket { opening_span },
                self.current().span(),
            ));
        }

        let arguments_span = match (first_span, last_span) {
            (Some(first), Some(last)) => first.join(last),
            (None, None) => Span::at(empty_position),

            _ => {
                unreachable!("first and last argument spans must be set together")
            }
        };

        Ok((arguments_span, last_span))
    }

    /// Indique si le stage courant ouvre un sous-pipeline.
    ///
    /// `lookup`, `union` et `pivot` sont toujours composés. `load` n'est composé
    /// que lorsque son en-tête est vide ; une forme compacte conserve ses
    /// arguments dans le stage simple.
    fn stage_opens_subpipeline(&self, kind: TokenKind, arguments_span: Span) -> bool {
        match kind {
            TokenKind::Lookup | TokenKind::Join | TokenKind::Union | TokenKind::Pivot => true,

            TokenKind::Load => arguments_span.is_empty(),
            _ => false,
        }
    }

    /// Consomme un token d'une catégorie précise.
    fn consume(&mut self, expected: TokenKind) -> ParseResult<Token> {
        let token = self.current();

        if token.kind() == expected {
            self.advance();
            Ok(token)
        } else {
            Err(self.error_current(ParseErrorKind::UnexpectedToken {
                found: token.kind(),
            }))
        }
    }

    /// Retourne le token courant.
    ///
    /// Un [`TokenStream`] valide contient toujours un token terminal. La valeur
    /// de secours protège néanmoins le parser contre un flux incorrect.
    fn current(&self) -> Token {
        self.stream
            .get(self.cursor)
            .unwrap_or_else(|| Token::end(self.stream.source().len()))
    }

    /// Retourne le token suivant sans avancer.
    fn next_token(&self) -> Token {
        self.stream
            .get(self.cursor.saturating_add(1))
            .unwrap_or_else(|| Token::end(self.stream.source().len()))
    }

    /// Vérifie la catégorie du token courant.
    fn check(&self, kind: TokenKind) -> bool {
        self.current().kind() == kind
    }

    /// Vérifie la catégorie du token suivant.
    fn check_next(&self, kind: TokenKind) -> bool {
        self.next_token().kind() == kind
    }

    /// Indique si le parser se trouve sur le token terminal.
    fn is_at_end(&self) -> bool {
        self.check(TokenKind::End)
    }

    /// Avance d'un token sans dépasser la fin.
    fn advance(&mut self) -> Token {
        let token = self.current();

        if !token.is_end() {
            self.cursor += 1;
        }

        token
    }

    /// Construit une erreur sur le token courant.
    fn error_current(&self, kind: ParseErrorKind) -> ParseError {
        ParseError::new(kind, self.current().span())
    }
}

/// Analyse un flux déjà tokenisé.
pub fn parse_tokens(stream: &TokenStream<'_>) -> ParseResult<PipelineAst> {
    Parser::new(stream).parse_pipeline()
}

/// Tokenise puis analyse une requête OG.
pub fn parse(source: &str) -> QueryParseResult<PipelineAst> {
    let stream = lex(source)?;
    let pipeline = parse_tokens(&stream)?;

    Ok(pipeline)
}

/// Analyse une valeur depuis un flux déjà tokenisé.
pub fn parse_value_tokens(stream: &TokenStream<'_>) -> ParseResult<ValueAst> {
    Parser::new(stream).parse_value()
}

/// Tokenise puis analyse une valeur autonome.
pub fn parse_value_source(source: &str) -> QueryParseResult<ValueAst> {
    let stream = lex(source)?;
    let value = parse_value_tokens(&stream)?;

    Ok(value)
}

/// Indique si une catégorie peut être utilisée comme nom de stage.
///
/// Les stages natifs ont leur propre token réservé. Les stages externes et
/// futurs restent des identifiants ordinaires. `from` et `on` sont admis comme
/// noms de stages internes afin que `union` puisse porter sa propre source.
const fn is_stage_name(kind: TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::Identifier
            | TokenKind::Where
            | TokenKind::Set
            | TokenKind::Lookup
            | TokenKind::Join
            | TokenKind::Union
            | TokenKind::Load
            | TokenKind::Pivot
            | TokenKind::Into
            | TokenKind::With
            | TokenKind::Chunk
            | TokenKind::Replace
            | TokenKind::Update
            | TokenKind::Merge
            | TokenKind::On
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(source: &str) -> PipelineAst {
        parse(source).unwrap()
    }

    fn parse_error(source: &str) -> ParseError {
        let error = parse(source).unwrap_err();

        let QueryParseError::Parse(error) = error else {
            panic!("expected parse error");
        };

        error
    }

    fn parse_value_ok(source: &str) -> ValueAst {
        parse_value_source(source).unwrap()
    }

    fn parse_value_error(source: &str) -> ParseError {
        let error = parse_value_source(source).unwrap_err();

        let QueryParseError::Parse(error) = error else {
            panic!("expected parse error");
        };

        error
    }

    #[test]
    fn parses_scalar_values() {
        assert!(matches!(parse_value_ok(r#""John""#), ValueAst::String(_)));
        assert!(matches!(parse_value_ok("42"), ValueAst::Number(_)));
        assert!(matches!(
            parse_value_ok("true"),
            ValueAst::Boolean(value) if value.value()
        ));
        assert!(matches!(
            parse_value_ok("false"),
            ValueAst::Boolean(value) if !value.value()
        ));
        assert!(matches!(parse_value_ok("null"), ValueAst::Null(_)));
        assert!(matches!(
            parse_value_ok("workspace"),
            ValueAst::Identifier(_)
        ));
    }

    #[test]
    fn parses_empty_structured_values() {
        let array = parse_value_ok("[]");
        let object = parse_value_ok("{}");

        assert!(matches!(array, ValueAst::Array(ref value) if value.is_empty()));
        assert!(matches!(object, ValueAst::Object(ref value) if value.is_empty()));
    }

    #[test]
    fn parses_array_with_trailing_comma() {
        let source = r#"["rust", 42, true, null,]"#;
        let ValueAst::Array(array) = parse_value_ok(source) else {
            panic!("expected array");
        };

        assert_eq!(array.len(), 4);
        assert_eq!(array.span().slice(source), Some(source));
        assert!(matches!(array.value(0), Some(ValueAst::String(_))));
        assert!(matches!(array.value(1), Some(ValueAst::Number(_))));
        assert!(matches!(array.value(2), Some(ValueAst::Boolean(_))));
        assert!(matches!(array.value(3), Some(ValueAst::Null(_))));
    }

    #[test]
    fn parses_object_with_identifier_and_string_keys() {
        let source = r#"{name: "John", "display-name": "Johnny",}"#;
        let ValueAst::Object(object) = parse_value_ok(source) else {
            panic!("expected object");
        };

        assert_eq!(object.len(), 2);
        assert_eq!(
            object.field(0).and_then(|field| field.key_text(source)),
            Some("name"),
        );
        assert_eq!(
            object.field(1).and_then(|field| field.key_text(source)),
            Some(r#""display-name""#),
        );
    }

    #[test]
    fn parses_nested_object_and_array() {
        let source = r#"{
            user: {
                name: "John",
                tags: ["rust", "database"],
                address: {city: "Paris"},
            },
            active: true,
        }"#;

        let value = parse_value_ok(source);

        assert!(value.is_object());
        assert_eq!(value.text(source), Some(source));
    }

    #[test]
    fn rejects_missing_object_colon() {
        let error = parse_value_error(r#"{name "John"}"#);

        assert!(matches!(
            error.kind(),
            ParseErrorKind::ExpectedColonAfterObjectKey {
                found: TokenKind::String
            }
        ));
    }

    #[test]
    fn rejects_missing_array_comma() {
        let error = parse_value_error("[1 2]");

        assert!(matches!(
            error.kind(),
            ParseErrorKind::ExpectedCommaOrArrayEnd {
                found: TokenKind::Number
            }
        ));
    }

    #[test]
    fn rejects_missing_object_comma() {
        let error = parse_value_error("{a: 1 b: 2}");

        assert!(matches!(
            error.kind(),
            ParseErrorKind::ExpectedCommaOrObjectEnd {
                found: TokenKind::Identifier
            }
        ));
    }

    #[test]
    fn rejects_unclosed_array_value() {
        let error = parse_value_error("[1, 2");

        assert!(matches!(error.kind(), ParseErrorKind::UnclosedArray { .. }));
    }

    #[test]
    fn rejects_unclosed_object_value() {
        let error = parse_value_error("{name: \"John\"");

        assert!(matches!(
            error.kind(),
            ParseErrorKind::UnclosedObject { .. }
        ));
    }

    #[test]
    fn rejects_trailing_tokens_after_value() {
        let error = parse_value_error("42 true");

        assert!(matches!(
            error.kind(),
            ParseErrorKind::UnexpectedToken {
                found: TokenKind::True
            }
        ));
    }

    #[test]
    fn parses_minimal_from_query() {
        let source = "from users";
        let pipeline = parse_ok(source);

        assert_eq!(pipeline.source().keyword(), SourceKeyword::From);
        assert_eq!(pipeline.source().collection_name(source), Some("users"));
        assert_eq!(pipeline.stage_count(), 0);
        assert!(pipeline.is_source_only());
        assert_eq!(pipeline.span(), Span::new(0, 10));
        assert_eq!(pipeline.text(source), Some(source));
    }

    #[test]
    fn parses_minimal_on_query() {
        let source = "on users";
        let pipeline = parse_ok(source);

        assert_eq!(pipeline.source().keyword(), SourceKeyword::On);
        assert_eq!(pipeline.source().collection_name(source), Some("users"));
    }

    #[test]
    fn parses_source_alias() {
        let source = "on users as u";
        let pipeline = parse_ok(source);

        assert!(pipeline.source().has_alias());
        assert_eq!(pipeline.source().alias_name(source), Some("u"));
        assert_eq!(pipeline.source().span(), Span::new(0, 13));
    }

    #[test]
    fn preserves_source_keyword_in_ast() {
        let from = parse_ok("from users");
        let on = parse_ok("on users");

        assert_ne!(from.source().keyword(), on.source().keyword());
    }

    #[test]
    fn parses_system_collection() {
        let source = "from _og.operations";
        let pipeline = parse_ok(source);

        assert_eq!(
            pipeline.source().collection_name(source),
            Some("_og.operations"),
        );

        assert_eq!(pipeline.source().collection().span(), Span::new(5, 19));
    }

    #[test]
    fn parses_deeply_qualified_collection() {
        let source = "from tenant.analytics.events";
        let pipeline = parse_ok(source);

        assert_eq!(
            pipeline.source().collection_name(source),
            Some("tenant.analytics.events"),
        );
    }

    #[test]
    fn parses_where_stage() {
        let source = "from users | where age > 18";
        let pipeline = parse_ok(source);

        assert_eq!(pipeline.stage_count(), 1);

        let stage = pipeline.stage(0).unwrap();

        assert_eq!(stage.name_text(source), Some("where"));
        assert_eq!(stage.arguments_text(source), Some("age > 18"));
        assert_eq!(stage.pipe_span(), Span::new(11, 12));
        assert_eq!(stage.span(), Span::new(11, 27));
        assert!(!stage.is_composite());
    }

    #[test]
    fn parses_set_stage() {
        let source = "from users | set active = true";
        let pipeline = parse_ok(source);
        let stage = pipeline.stage(0).unwrap();

        assert_eq!(stage.name_text(source), Some("set"));
        assert_eq!(stage.arguments_text(source), Some("active = true"));
    }

    #[test]
    fn parses_compact_load_stage() {
        let source = "from users | load profile with replace";
        let pipeline = parse_ok(source);
        let stage = pipeline.stage(0).unwrap();

        assert_eq!(stage.name_text(source), Some("load"));
        assert_eq!(stage.arguments_text(source), Some("profile with replace"),);
        assert!(!stage.is_composite());
    }

    #[test]
    fn parses_custom_stage() {
        let source = "from users | inspect verbose";
        let pipeline = parse_ok(source);
        let stage = pipeline.stage(0).unwrap();

        assert_eq!(stage.name_text(source), Some("inspect"));
        assert_eq!(stage.arguments_text(source), Some("verbose"));
    }

    #[test]
    fn parses_stage_without_arguments() {
        let source = "from users | inspect";
        let pipeline = parse_ok(source);
        let stage = pipeline.stage(0).unwrap();

        assert_eq!(stage.name_text(source), Some("inspect"));
        assert_eq!(stage.arguments_text(source), Some(""));
        assert!(!stage.has_arguments());
        assert_eq!(stage.arguments_span(), Span::at(source.len()));
    }

    #[test]
    fn parses_multiple_stages() {
        let source = "from users | where age > 18 | set active = true";
        let pipeline = parse_ok(source);

        assert_eq!(pipeline.stage_count(), 2);

        let where_stage = pipeline.stage(0).unwrap();
        let set_stage = pipeline.stage(1).unwrap();

        assert_eq!(where_stage.name_text(source), Some("where"));
        assert_eq!(where_stage.arguments_text(source), Some("age > 18"));
        assert_eq!(set_stage.name_text(source), Some("set"));
        assert_eq!(set_stage.arguments_text(source), Some("active = true"));
    }

    #[test]
    fn parses_lookup_subpipeline() {
        let source = concat!(
            "on users as u\n",
            "| lookup workspace as w\n",
            "    | where u._id in w.share\n",
            "    | where w.public == true\n",
            "    | into public\n",
            "| end",
        );

        let pipeline = parse_ok(source);
        let lookup = pipeline.stage(0).expect("lookup stage");

        assert_eq!(lookup.name_text(source), Some("lookup"));
        assert_eq!(lookup.arguments_text(source), Some("workspace as w"));
        assert!(lookup.is_composite());
        assert_eq!(lookup.header_span(), Span::new(14, 37));
        assert_eq!(lookup.span(), Span::new(14, source.len()));

        let body = lookup.subpipeline().expect("lookup body");

        assert_eq!(body.stage_count(), 3);
        assert_eq!(body.stage(0).unwrap().name_text(source), Some("where"));
        assert_eq!(body.stage(1).unwrap().name_text(source), Some("where"));
        assert_eq!(body.stage(2).unwrap().name_text(source), Some("into"));
        assert_eq!(
            body.stage(2).unwrap().arguments_text(source),
            Some("public"),
        );
        assert_eq!(body.end().text(source), Some("| end"));
    }

    #[test]
    fn parses_union_subpipeline_with_internal_source() {
        let source = concat!(
            "on users\n",
            "| union\n",
            "    | on archived_users\n",
            "    | where active == true\n",
            "| end",
        );

        let pipeline = parse_ok(source);
        let union = pipeline.stage(0).expect("union stage");

        assert_eq!(union.name_text(source), Some("union"));
        assert!(!union.has_arguments());
        assert!(union.is_composite());

        let body = union.subpipeline().expect("union body");

        assert_eq!(body.stage_count(), 2);
        assert_eq!(body.stage(0).unwrap().name_text(source), Some("on"));
        assert_eq!(
            body.stage(0).unwrap().arguments_text(source),
            Some("archived_users"),
        );
        assert_eq!(body.stage(1).unwrap().name_text(source), Some("where"));
    }

    #[test]
    fn parses_streaming_load_subpipeline() {
        let source = concat!(
            "on users\n",
            "| load\n",
            "    | with replace\n",
            "    | chunk batch1\n",
            "    | chunk batch2\n",
            "| end",
        );

        let pipeline = parse_ok(source);
        let load = pipeline.stage(0).expect("load stage");

        assert_eq!(load.name_text(source), Some("load"));
        assert!(!load.has_arguments());
        assert!(load.is_composite());

        let body = load.subpipeline().expect("load body");

        assert_eq!(body.stage_count(), 3);
        assert_eq!(body.stage(0).unwrap().name_text(source), Some("with"));
        assert_eq!(
            body.stage(0).unwrap().arguments_text(source),
            Some("replace"),
        );
        assert_eq!(body.stage(1).unwrap().name_text(source), Some("chunk"));
        assert_eq!(body.stage(2).unwrap().name_text(source), Some("chunk"));
    }

    #[test]
    fn parses_empty_union_subpipeline() {
        let source = "on users | union | end";
        let pipeline = parse_ok(source);
        let union = pipeline.stage(0).expect("union");

        let body = union.subpipeline().expect("union body");

        assert!(body.is_empty());
        assert_eq!(body.stage_count(), 0);
        assert_eq!(body.end().text(source), Some("| end"));
    }

    #[test]
    fn parses_nested_compound_stages() {
        let source = concat!(
            "on users\n",
            "| union\n",
            "    | on archived_users\n",
            "    | lookup workspace\n",
            "        | into workspaces\n",
            "    | end\n",
            "| end",
        );

        let pipeline = parse_ok(source);
        let union = pipeline.stage(0).expect("union");
        let union_body = union.subpipeline().expect("union body");
        let lookup = union_body.stage(1).expect("lookup");

        assert!(lookup.is_composite());
        assert_eq!(lookup.name_text(source), Some("lookup"));

        let lookup_body = lookup.subpipeline().expect("lookup body");

        assert_eq!(lookup_body.stage_count(), 1);
        assert_eq!(
            lookup_body.stage(0).unwrap().name_text(source),
            Some("into"),
        );
    }

    #[test]
    fn stage_after_compound_stage_remains_at_root() {
        let source = concat!(
            "on users\n",
            "| lookup workspace\n",
            "    | into workspaces\n",
            "| end\n",
            "| where active == true",
        );

        let pipeline = parse_ok(source);

        assert_eq!(pipeline.stage_count(), 2);
        assert!(pipeline.stage(0).unwrap().is_composite());
        assert_eq!(pipeline.stage(1).unwrap().name_text(source), Some("where"),);
    }

    #[test]
    fn ignores_whitespace_between_pipeline_elements() {
        let source = " \n from   users \n |   where   age > 18 \n";
        let pipeline = parse_ok(source);

        assert_eq!(pipeline.source().collection_name(source), Some("users"));

        let stage = pipeline.stage(0).unwrap();

        assert_eq!(stage.name_text(source), Some("where"));
        assert_eq!(stage.arguments_text(source), Some("age > 18"));

        assert_eq!(
            pipeline.text(source),
            Some("from   users \n |   where   age > 18"),
        );
    }

    #[test]
    fn stage_arguments_end_before_next_pipe() {
        let source = "from users | first alpha beta | second gamma";
        let pipeline = parse_ok(source);

        assert_eq!(pipeline.stage_count(), 2);
        assert_eq!(
            pipeline.stage(0).unwrap().arguments_text(source),
            Some("alpha beta"),
        );
        assert_eq!(
            pipeline.stage(1).unwrap().arguments_text(source),
            Some("gamma"),
        );
    }

    #[test]
    fn pipe_inside_parentheses_does_not_end_stage() {
        let source = "from users | custom (left | right) | inspect";
        let pipeline = parse_ok(source);

        assert_eq!(pipeline.stage_count(), 2);
        assert_eq!(
            pipeline.stage(0).unwrap().arguments_text(source),
            Some("(left | right)"),
        );
        assert_eq!(
            pipeline.stage(1).unwrap().name_text(source),
            Some("inspect"),
        );
    }

    #[test]
    fn handles_nested_parentheses() {
        let source = "from users | where (active and (age > 18))";
        let pipeline = parse_ok(source);
        let stage = pipeline.stage(0).unwrap();

        assert_eq!(
            stage.arguments_text(source),
            Some("(active and (age > 18))"),
        );
    }

    #[test]
    fn pipe_in_string_does_not_end_stage() {
        let source = r#"from users | where value == "left | right" | inspect"#;
        let pipeline = parse_ok(source);

        assert_eq!(pipeline.stage_count(), 2);
        assert_eq!(
            pipeline.stage(0).unwrap().arguments_text(source),
            Some(r#"value == "left | right""#),
        );
    }

    #[test]
    fn parses_insert_document_as_complete_stage_arguments() {
        let source = r#"from users | insert {
            _id: "u1",
            name: "John",
            tags: ["rust", "database"],
        } | inspect"#;

        let pipeline = parse_ok(source);

        assert_eq!(pipeline.stage_count(), 2);
        assert_eq!(pipeline.stage(0).unwrap().name_text(source), Some("insert"));
        assert_eq!(
            pipeline.stage(0).unwrap().arguments_text(source),
            Some(
                r#"{
            _id: "u1",
            name: "John",
            tags: ["rust", "database"],
        }"#,
            ),
        );
        assert_eq!(
            pipeline.stage(1).unwrap().name_text(source),
            Some("inspect")
        );
    }

    #[test]
    fn balances_braces_and_brackets_in_stage_arguments() {
        let source = r#"from users | custom {items: [{value: 1}, {value: 2}]} | inspect"#;
        let pipeline = parse_ok(source);

        assert_eq!(pipeline.stage_count(), 2);
        assert_eq!(
            pipeline.stage(0).unwrap().arguments_text(source),
            Some("{items: [{value: 1}, {value: 2}]}"),
        );
    }

    #[test]
    fn rejects_unclosed_brace_in_stage_arguments() {
        let source = "from users | insert {name: \"John\"";
        let error = parse_error(source);

        assert!(matches!(error.kind(), ParseErrorKind::UnclosedBrace { .. }));
    }

    #[test]
    fn rejects_unclosed_bracket_in_stage_arguments() {
        let source = "from users | custom [1, 2";
        let error = parse_error(source);

        assert!(matches!(
            error.kind(),
            ParseErrorKind::UnclosedBracket { .. }
        ));
    }

    #[test]
    fn preserves_exact_literal_arguments() {
        let first_source = "from users | where age > 18";
        let second_source = "from users | where age > 42";

        let first = parse_ok(first_source);
        let second = parse_ok(second_source);

        assert_eq!(
            first.stage(0).unwrap().name_text(first_source),
            second.stage(0).unwrap().name_text(second_source),
        );

        assert_ne!(
            first.stage(0).unwrap().arguments_text(first_source),
            second.stage(0).unwrap().arguments_text(second_source),
        );
    }

    #[test]
    fn rejects_empty_query() {
        let error = parse_error("");

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedSourceKeyword {
                found: TokenKind::End,
            },
        );
        assert_eq!(error.span(), Span::at(0));
    }

    #[test]
    fn rejects_query_without_source_keyword() {
        let error = parse_error("users");

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedSourceKeyword {
                found: TokenKind::Identifier,
            },
        );
        assert_eq!(error.span(), Span::new(0, 5));
    }

    #[test]
    fn rejects_missing_collection() {
        let error = parse_error("from");

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedCollectionName {
                found: TokenKind::End,
            },
        );
        assert_eq!(error.span(), Span::at(4));
    }

    #[test]
    fn rejects_missing_alias_name() {
        let source = "on users as";
        let error = parse_error(source);

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedAliasName {
                found: TokenKind::End,
            },
        );
        assert_eq!(error.span(), Span::at(source.len()));
    }

    #[test]
    fn rejects_keyword_as_alias_name() {
        let error = parse_error("on users as where");

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedAliasName {
                found: TokenKind::Where,
            },
        );
    }

    #[test]
    fn rejects_literal_collection_name() {
        let error = parse_error("from \"users\"");

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedCollectionName {
                found: TokenKind::String,
            },
        );
    }

    #[test]
    fn rejects_collection_ending_with_dot() {
        let error = parse_error("from _og.");

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedNameAfterDot {
                found: TokenKind::End,
            },
        );
    }

    #[test]
    fn rejects_empty_collection_segment() {
        let error = parse_error("from _og..operations");

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedNameAfterDot {
                found: TokenKind::Dot,
            },
        );
    }

    #[test]
    fn rejects_tokens_between_source_and_stage() {
        let error = parse_error("from users where age > 18");

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedPipe {
                found: TokenKind::Where,
            },
        );
    }

    #[test]
    fn rejects_trailing_pipe() {
        let error = parse_error("from users |");

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedStageName {
                found: TokenKind::End,
            },
        );
    }

    #[test]
    fn rejects_pipe_followed_by_literal() {
        let error = parse_error("from users | 18");

        assert_eq!(
            error.kind(),
            &ParseErrorKind::ExpectedStageName {
                found: TokenKind::Number,
            },
        );
    }

    #[test]
    fn rejects_end_at_root() {
        let error = parse_error("from users | end");

        assert_eq!(error.kind(), &ParseErrorKind::UnexpectedEndKeyword);
        assert_eq!(error.span(), Span::new(13, 16));
    }

    #[test]
    fn parses_join_as_lookup_alias() {
        let source = concat!(
            "on users as u\n",
            "| join workspace as w\n",
            "    | where u._id in w.share\n",
            "    | into public\n",
            "| end",
        );

        let pipeline = parse_ok(source);
        let join = pipeline.stage(0).expect("join stage");

        assert_eq!(join.name_text(source), Some("join"));
        assert_eq!(join.arguments_text(source), Some("workspace as w"));
        assert!(join.is_composite());

        let body = join.subpipeline().expect("join body");
        assert_eq!(body.stage_count(), 2);
        assert_eq!(body.stage(0).unwrap().name_text(source), Some("where"));
        assert_eq!(body.stage(1).unwrap().name_text(source), Some("into"));
    }

    #[test]
    fn rejects_unclosed_lookup_subpipeline() {
        let source = "on users | lookup workspace | into public";
        let error = parse_error(source);

        assert_eq!(
            error.kind(),
            &ParseErrorKind::UnclosedSubPipeline {
                stage_span: Span::new(9, 27),
            },
        );
        assert_eq!(error.span(), Span::at(source.len()));
    }

    #[test]
    fn rejects_unclosed_nested_subpipeline() {
        let source = concat!(
            "on users\n",
            "| union\n",
            "    | lookup workspace\n",
            "        | into public\n",
            "| end",
        );

        let error = parse_error(source);

        assert!(matches!(
            error.kind(),
            ParseErrorKind::UnclosedSubPipeline { .. },
        ));
    }

    #[test]
    fn rejects_unexpected_right_parenthesis() {
        let error = parse_error("from users | where age > 18)");

        assert_eq!(error.kind(), &ParseErrorKind::UnexpectedRightParenthesis);
        assert_eq!(error.span(), Span::new(27, 28));
    }

    #[test]
    fn rejects_unclosed_parenthesis() {
        let source = "from users | where (age > 18";
        let error = parse_error(source);

        assert_eq!(
            error.kind(),
            &ParseErrorKind::UnclosedParenthesis {
                opening_span: Span::new(19, 20),
            },
        );
        assert_eq!(error.span(), Span::at(source.len()));
    }

    #[test]
    fn reports_lexical_errors_separately() {
        let error = parse("from users @").unwrap_err();

        assert!(error.as_lex_error().is_some());
        assert!(error.as_parse_error().is_none());
        assert!(matches!(error, QueryParseError::Lex(_)));
    }

    #[test]
    fn reports_parse_errors_separately() {
        let error = parse("users").unwrap_err();

        assert!(error.as_lex_error().is_none());
        assert!(error.as_parse_error().is_some());
        assert!(matches!(error, QueryParseError::Parse(_)));
    }

    #[test]
    fn parses_previously_lexed_stream() {
        let source = "from users | where active == true";
        let stream = lex(source).unwrap();
        let pipeline = parse_tokens(&stream).unwrap();

        assert_eq!(pipeline.source().collection_name(source), Some("users"));
        assert_eq!(
            pipeline.stage(0).unwrap().arguments_text(source),
            Some("active == true"),
        );
    }

    #[test]
    fn parser_position_starts_at_zero() {
        let stream = lex("from users").unwrap();
        let parser = Parser::new(&stream);

        assert_eq!(parser.position(), 0);
        assert_eq!(parser.stream().source(), "from users");
    }

    #[test]
    fn parse_error_display_is_compact() {
        let error = ParseError::new(
            ParseErrorKind::ExpectedPipe {
                found: TokenKind::Where,
            },
            Span::new(11, 16),
        );

        assert_eq!(
            error.to_string(),
            "expected `|` before next stage, found `where` at 11..16",
        );
    }

    #[test]
    fn unclosed_subpipeline_error_mentions_opening_stage() {
        let error = ParseError::new(
            ParseErrorKind::UnclosedSubPipeline {
                stage_span: Span::new(9, 27),
            },
            Span::at(42),
        );

        assert_eq!(
            error.to_string(),
            "sub-pipeline opened by stage at 9..27 is missing `| end` at 42..42",
        );
    }

    #[test]
    fn query_parse_error_display_distinguishes_layer() {
        let error = parse("users").unwrap_err();

        assert_eq!(
            error.to_string(),
            "parse error: expected `from` or `on`, found identifier at 0..5",
        );
    }

    #[test]
    fn parses_pivot_subpipeline() {
        let source = r#"on sales
| pivot
    | rows region
    | columns month
    | values revenue
    | aggregate sum
| end"#;

        let pipeline = parse(source).unwrap();
        let pivot = pipeline.stage(0).expect("pivot stage");

        assert_eq!(pivot.name_text(source), Some("pivot"));
        assert!(pivot.is_composite());

        let subpipeline = pivot.subpipeline().expect("pivot sub-pipeline");

        assert_eq!(subpipeline.stage_count(), 4);
        assert_eq!(
            subpipeline
                .stage(0)
                .and_then(|stage| stage.name_text(source)),
            Some("rows"),
        );
        assert_eq!(
            subpipeline
                .stage(1)
                .and_then(|stage| stage.name_text(source)),
            Some("columns"),
        );
        assert_eq!(
            subpipeline
                .stage(2)
                .and_then(|stage| stage.name_text(source)),
            Some("values"),
        );
        assert_eq!(
            subpipeline
                .stage(3)
                .and_then(|stage| stage.name_text(source)),
            Some("aggregate"),
        );
    }

    #[test]
    fn rejects_unclosed_pivot_subpipeline() {
        let source = r#"on sales
| pivot
    | rows region
    | columns month
    | values revenue
    | aggregate sum"#;

        let error = parse(source).unwrap_err();

        assert!(matches!(
            error.as_parse_error().map(ParseError::kind),
            Some(ParseErrorKind::UnclosedSubPipeline { .. }),
        ));
    }
}
