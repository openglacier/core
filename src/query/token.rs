//! Query token types.

use std::fmt;

use super::Span;

/// Catégorie syntaxique d'un token OG.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TokenKind {
    Identifier,
    String,
    Number,
    True,
    False,
    Null,
    From,
    On,
    As,
    Where,
    Set,
    Lookup,
    Join,
    Union,
    Load,
    Pivot,
    Into,
    With,
    Chunk,
    Replace,
    Update,
    Merge,
    EndKeyword,
    And,
    Or,
    Not,
    Pipe,
    Dot,
    Colon,
    Comma,
    LeftParen,
    RightParen,
    LeftBrace,
    RightBrace,
    LeftBracket,
    RightBracket,
    Equal,
    EqualEqual,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    End,
}

impl TokenKind {
    #[must_use]
    pub fn from_keyword(value: &str) -> Option<Self> {
        match value {
            "true" => Some(Self::True),
            "false" => Some(Self::False),
            "null" => Some(Self::Null),
            "from" => Some(Self::From),
            "on" => Some(Self::On),
            "as" => Some(Self::As),
            "where" => Some(Self::Where),
            "set" => Some(Self::Set),
            "lookup" => Some(Self::Lookup),
            "join" => Some(Self::Join),
            "union" => Some(Self::Union),
            "load" => Some(Self::Load),
            "pivot" => Some(Self::Pivot),
            "into" => Some(Self::Into),
            "with" => Some(Self::With),
            "chunk" => Some(Self::Chunk),
            "replace" => Some(Self::Replace),
            "update" => Some(Self::Update),
            "merge" => Some(Self::Merge),
            "end" => Some(Self::EndKeyword),
            "and" => Some(Self::And),
            "or" => Some(Self::Or),
            "not" => Some(Self::Not),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_keyword(self) -> bool {
        matches!(
            self,
            Self::True
                | Self::False
                | Self::Null
                | Self::From
                | Self::On
                | Self::As
                | Self::Where
                | Self::Set
                | Self::Lookup
                | Self::Join
                | Self::Union
                | Self::Load
                | Self::Into
                | Self::With
                | Self::Chunk
                | Self::Replace
                | Self::Update
                | Self::Merge
                | Self::EndKeyword
                | Self::And
                | Self::Or
                | Self::Not
        )
    }

    /// Indique si le token introduit un stage composé.
    ///
    /// `lookup`, `union` et `load` partagent la même mécanique syntaxique :
    /// leur corps est un sous-pipeline fermé par le mot-clé `end`.
    #[must_use]
    pub const fn is_compound_stage_keyword(self) -> bool {
        matches!(self, Self::Lookup | Self::Join | Self::Union | Self::Load)
    }

    /// Indique si le token représente un mode de chargement.
    #[must_use]
    pub const fn is_load_mode_keyword(self) -> bool {
        matches!(self, Self::Replace | Self::Update | Self::Merge)
    }

    /// Indique si le token est le mot-clé syntaxique `end`.
    ///
    /// Cette catégorie est distincte de [`TokenKind::End`], qui représente la
    /// fin du flux de tokens.
    #[must_use]
    pub const fn is_end_keyword(self) -> bool {
        matches!(self, Self::EndKeyword)
    }

    #[must_use]
    pub const fn is_literal(self) -> bool {
        matches!(
            self,
            Self::String | Self::Number | Self::True | Self::False | Self::Null
        )
    }

    #[must_use]
    pub const fn can_start_value(self) -> bool {
        matches!(
            self,
            Self::Identifier
                | Self::String
                | Self::Number
                | Self::True
                | Self::False
                | Self::Null
                | Self::LeftParen
                | Self::LeftBrace
                | Self::LeftBracket
        )
    }

    #[must_use]
    pub const fn is_comparison_operator(self) -> bool {
        matches!(
            self,
            Self::EqualEqual
                | Self::NotEqual
                | Self::Less
                | Self::LessEqual
                | Self::Greater
                | Self::GreaterEqual
        )
    }

    #[must_use]
    pub const fn is_logical_operator(self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Not)
    }

    #[must_use]
    pub const fn is_punctuation(self) -> bool {
        matches!(
            self,
            Self::Pipe
                | Self::Dot
                | Self::Colon
                | Self::Comma
                | Self::LeftParen
                | Self::RightParen
                | Self::LeftBrace
                | Self::RightBrace
                | Self::LeftBracket
                | Self::RightBracket
        )
    }

    #[must_use]
    pub const fn display_name(self) -> &'static str {
        match self {
            Self::Identifier => "identifier",
            Self::String => "string",
            Self::Number => "number",
            Self::True => "`true`",
            Self::False => "`false`",
            Self::Null => "`null`",
            Self::From => "`from`",
            Self::On => "`on`",
            Self::As => "`as`",
            Self::Where => "`where`",
            Self::Set => "`set`",
            Self::Lookup => "`lookup`",
            Self::Join => "`join`",
            Self::Union => "`union`",
            Self::Load => "`load`",
            Self::Pivot => "`pivot`",
            Self::Into => "`into`",
            Self::With => "`with`",
            Self::Chunk => "`chunk`",
            Self::Replace => "`replace`",
            Self::Update => "`update`",
            Self::Merge => "`merge`",
            Self::EndKeyword => "`end`",
            Self::And => "`and`",
            Self::Or => "`or`",
            Self::Not => "`not`",
            Self::Pipe => "`|`",
            Self::Dot => "`.`",
            Self::Colon => "`:`",
            Self::Comma => "`,`",
            Self::LeftParen => "`(`",
            Self::RightParen => "`)`",
            Self::LeftBrace => "`{`",
            Self::RightBrace => "`}`",
            Self::LeftBracket => "`[`",
            Self::RightBracket => "`]`",
            Self::Equal => "`=`",
            Self::EqualEqual => "`==`",
            Self::NotEqual => "`!=`",
            Self::Less => "`<`",
            Self::LessEqual => "`<=`",
            Self::Greater => "`>`",
            Self::GreaterEqual => "`>=`",
            Self::End => "end of input",
        }
    }
}

impl fmt::Display for TokenKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.display_name())
    }
}

/// Token lexical OG.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Token {
    kind: TokenKind,
    span: Span,
}

impl Token {
    #[must_use]
    #[inline]
    pub const fn new(kind: TokenKind, span: Span) -> Self {
        Self { kind, span }
    }

    #[must_use]
    pub const fn end(offset: usize) -> Self {
        Self::new(TokenKind::End, Span::at(offset))
    }

    #[must_use]
    #[inline]
    pub const fn kind(self) -> TokenKind {
        self.kind
    }

    #[must_use]
    #[inline]
    pub const fn span(self) -> Span {
        self.span
    }

    #[must_use]
    pub fn is(self, kind: TokenKind) -> bool {
        self.kind == kind
    }

    #[must_use]
    pub fn is_end(self) -> bool {
        self.kind == TokenKind::End
    }

    /// Indique si ce token est le mot-clé syntaxique `end`.
    #[must_use]
    pub fn is_end_keyword(self) -> bool {
        self.kind == TokenKind::EndKeyword
    }

    #[must_use]
    pub fn lexeme<'source>(self, source: &'source str) -> Option<&'source str> {
        self.span.slice(source)
    }

    #[must_use]
    pub fn identifier<'source>(self, source: &'source str) -> Option<&'source str> {
        if self.kind == TokenKind::Identifier {
            self.lexeme(source)
        } else {
            None
        }
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}", self.kind, self.span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_keywords() {
        assert_eq!(TokenKind::from_keyword("from"), Some(TokenKind::From));
        assert_eq!(TokenKind::from_keyword("where"), Some(TokenKind::Where));
        assert_eq!(TokenKind::from_keyword("as"), Some(TokenKind::As));
        assert_eq!(TokenKind::from_keyword("lookup"), Some(TokenKind::Lookup));
        assert_eq!(TokenKind::from_keyword("join"), Some(TokenKind::Join));
        assert_eq!(TokenKind::from_keyword("union"), Some(TokenKind::Union));
        assert_eq!(TokenKind::from_keyword("load"), Some(TokenKind::Load));
        assert_eq!(TokenKind::from_keyword("into"), Some(TokenKind::Into));
        assert_eq!(TokenKind::from_keyword("with"), Some(TokenKind::With));
        assert_eq!(TokenKind::from_keyword("chunk"), Some(TokenKind::Chunk));
        assert_eq!(TokenKind::from_keyword("replace"), Some(TokenKind::Replace),);
        assert_eq!(TokenKind::from_keyword("update"), Some(TokenKind::Update));
        assert_eq!(TokenKind::from_keyword("merge"), Some(TokenKind::Merge));
        assert_eq!(TokenKind::from_keyword("end"), Some(TokenKind::EndKeyword),);
        assert_eq!(TokenKind::from_keyword("true"), Some(TokenKind::True));
        assert_eq!(TokenKind::from_keyword("false"), Some(TokenKind::False));
        assert_eq!(TokenKind::from_keyword("null"), Some(TokenKind::Null));
        assert_eq!(TokenKind::from_keyword("and"), Some(TokenKind::And));
    }

    #[test]
    fn keywords_are_case_sensitive() {
        assert_eq!(TokenKind::from_keyword("From"), None);
        assert_eq!(TokenKind::from_keyword("WHERE"), None);
        assert_eq!(TokenKind::from_keyword("True"), None);
    }

    #[test]
    fn unknown_word_is_not_keyword() {
        assert_eq!(TokenKind::from_keyword("users"), None);
        assert_eq!(TokenKind::from_keyword("country"), None);
        assert_eq!(TokenKind::from_keyword("enabled"), None);
    }

    #[test]
    fn classifies_keywords() {
        assert!(TokenKind::From.is_keyword());
        assert!(TokenKind::Where.is_keyword());
        assert!(TokenKind::Lookup.is_keyword());
        assert!(TokenKind::Join.is_keyword());
        assert!(TokenKind::Union.is_keyword());
        assert!(TokenKind::Load.is_keyword());
        assert!(TokenKind::EndKeyword.is_keyword());
        assert!(TokenKind::True.is_keyword());
        assert!(TokenKind::And.is_keyword());
        assert!(!TokenKind::Identifier.is_keyword());
        assert!(!TokenKind::Number.is_keyword());
        assert!(!TokenKind::EqualEqual.is_keyword());
    }

    #[test]
    fn classifies_compound_stage_keywords() {
        assert!(TokenKind::Lookup.is_compound_stage_keyword());
        assert!(TokenKind::Join.is_compound_stage_keyword());
        assert!(TokenKind::Union.is_compound_stage_keyword());
        assert!(TokenKind::Load.is_compound_stage_keyword());

        assert!(!TokenKind::Where.is_compound_stage_keyword());
        assert!(!TokenKind::EndKeyword.is_compound_stage_keyword());
        assert!(!TokenKind::End.is_compound_stage_keyword());
    }

    #[test]
    fn classifies_load_mode_keywords() {
        assert!(TokenKind::Replace.is_load_mode_keyword());
        assert!(TokenKind::Update.is_load_mode_keyword());
        assert!(TokenKind::Merge.is_load_mode_keyword());

        assert!(!TokenKind::Load.is_load_mode_keyword());
        assert!(!TokenKind::With.is_load_mode_keyword());
    }

    #[test]
    fn distinguishes_end_keyword_from_end_of_input() {
        assert!(TokenKind::EndKeyword.is_end_keyword());
        assert!(!TokenKind::End.is_end_keyword());

        assert_eq!(TokenKind::EndKeyword.display_name(), "`end`");
        assert_eq!(TokenKind::End.display_name(), "end of input");
    }

    #[test]
    fn classifies_literals() {
        assert!(TokenKind::String.is_literal());
        assert!(TokenKind::Number.is_literal());
        assert!(TokenKind::True.is_literal());
        assert!(TokenKind::False.is_literal());
        assert!(TokenKind::Null.is_literal());
        assert!(!TokenKind::Identifier.is_literal());
        assert!(!TokenKind::From.is_literal());
    }

    #[test]
    fn classifies_value_starters() {
        assert!(TokenKind::Identifier.can_start_value());
        assert!(TokenKind::String.can_start_value());
        assert!(TokenKind::Number.can_start_value());
        assert!(TokenKind::Null.can_start_value());
        assert!(TokenKind::LeftParen.can_start_value());
        assert!(TokenKind::LeftBrace.can_start_value());
        assert!(TokenKind::LeftBracket.can_start_value());
        assert!(!TokenKind::Pipe.can_start_value());
        assert!(!TokenKind::RightParen.can_start_value());
        assert!(!TokenKind::End.can_start_value());
    }

    #[test]
    fn classifies_comparison_operators() {
        assert!(TokenKind::EqualEqual.is_comparison_operator());
        assert!(TokenKind::NotEqual.is_comparison_operator());
        assert!(TokenKind::Less.is_comparison_operator());
        assert!(TokenKind::LessEqual.is_comparison_operator());
        assert!(TokenKind::Greater.is_comparison_operator());
        assert!(TokenKind::GreaterEqual.is_comparison_operator());
        assert!(!TokenKind::Equal.is_comparison_operator());
        assert!(!TokenKind::And.is_comparison_operator());
    }

    #[test]
    fn classifies_logical_operators() {
        assert!(TokenKind::And.is_logical_operator());
        assert!(TokenKind::Or.is_logical_operator());
        assert!(TokenKind::Not.is_logical_operator());
        assert!(!TokenKind::EqualEqual.is_logical_operator());
        assert!(!TokenKind::Identifier.is_logical_operator());
    }

    #[test]
    fn classifies_punctuation() {
        assert!(TokenKind::Pipe.is_punctuation());
        assert!(TokenKind::Dot.is_punctuation());
        assert!(TokenKind::Colon.is_punctuation());
        assert!(TokenKind::Comma.is_punctuation());
        assert!(TokenKind::LeftParen.is_punctuation());
        assert!(TokenKind::RightParen.is_punctuation());
        assert!(TokenKind::LeftBrace.is_punctuation());
        assert!(TokenKind::RightBrace.is_punctuation());
        assert!(TokenKind::LeftBracket.is_punctuation());
        assert!(TokenKind::RightBracket.is_punctuation());
        assert!(!TokenKind::Equal.is_punctuation());
        assert!(!TokenKind::Identifier.is_punctuation());
    }

    #[test]
    fn exposes_display_names() {
        assert_eq!(TokenKind::Identifier.display_name(), "identifier");
        assert_eq!(TokenKind::String.display_name(), "string");
        assert_eq!(TokenKind::From.display_name(), "`from`");
        assert_eq!(TokenKind::Lookup.display_name(), "`lookup`");
        assert_eq!(TokenKind::Join.display_name(), "`join`");
        assert_eq!(TokenKind::Union.display_name(), "`union`");
        assert_eq!(TokenKind::Load.display_name(), "`load`");
        assert_eq!(TokenKind::EndKeyword.display_name(), "`end`");
        assert_eq!(TokenKind::Pipe.display_name(), "`|`");
        assert_eq!(TokenKind::Colon.display_name(), "`:`");
        assert_eq!(TokenKind::LeftBrace.display_name(), "`{`");
        assert_eq!(TokenKind::RightBrace.display_name(), "`}`");
        assert_eq!(TokenKind::LeftBracket.display_name(), "`[`");
        assert_eq!(TokenKind::RightBracket.display_name(), "`]`");
        assert_eq!(TokenKind::EqualEqual.display_name(), "`==`");
        assert_eq!(TokenKind::End.display_name(), "end of input");
    }

    #[test]
    fn structured_value_delimiters_are_not_literals() {
        assert!(!TokenKind::LeftBrace.is_literal());
        assert!(!TokenKind::RightBrace.is_literal());
        assert!(!TokenKind::LeftBracket.is_literal());
        assert!(!TokenKind::RightBracket.is_literal());
        assert!(!TokenKind::Colon.is_literal());
    }

    #[test]
    fn structured_value_openers_can_start_values() {
        assert!(TokenKind::LeftBrace.can_start_value());
        assert!(TokenKind::LeftBracket.can_start_value());
        assert!(!TokenKind::RightBrace.can_start_value());
        assert!(!TokenKind::RightBracket.can_start_value());
        assert!(!TokenKind::Colon.can_start_value());
    }

    #[test]
    fn displays_token_kind() {
        assert_eq!(TokenKind::Identifier.to_string(), "identifier");
        assert_eq!(TokenKind::Where.to_string(), "`where`");
        assert_eq!(TokenKind::GreaterEqual.to_string(), "`>=`");
    }

    #[test]
    fn creates_token() {
        let token = Token::new(TokenKind::Identifier, Span::new(5, 10));
        assert_eq!(token.kind(), TokenKind::Identifier);
        assert_eq!(token.span(), Span::new(5, 10));
        assert!(token.is(TokenKind::Identifier));
        assert!(!token.is(TokenKind::String));
        assert!(!token.is_end());
    }

    #[test]
    fn creates_end_token() {
        let token = Token::end(10);
        assert_eq!(token.kind(), TokenKind::End);
        assert_eq!(token.span(), Span::at(10));
        assert!(token.is_end());
        assert!(!token.is_end_keyword());
    }

    #[test]
    fn creates_end_keyword_token() {
        let token = Token::new(TokenKind::EndKeyword, Span::new(2, 5));

        assert_eq!(token.kind(), TokenKind::EndKeyword);
        assert!(!token.is_end());
        assert!(token.is_end_keyword());
        assert_eq!(token.lexeme("| end"), Some("end"));
    }

    #[test]
    fn retrieves_lexeme() {
        let source = "from users";
        let token = Token::new(TokenKind::Identifier, Span::new(5, 10));
        assert_eq!(token.lexeme(source), Some("users"));
    }

    #[test]
    fn retrieves_utf8_lexeme() {
        let source = "from employés";
        let start = source.find("employés").unwrap();
        let end = start + "employés".len();
        let token = Token::new(TokenKind::Identifier, Span::new(start, end));
        assert_eq!(token.lexeme(source), Some("employés"));
    }

    #[test]
    fn invalid_span_returns_no_lexeme() {
        let token = Token::new(TokenKind::Identifier, Span::new(0, 20));
        assert_eq!(token.lexeme("users"), None);
    }

    #[test]
    fn retrieves_identifier_only_for_identifier_token() {
        let source = "users \"Paris\"";
        let identifier = Token::new(TokenKind::Identifier, Span::new(0, 5));
        let string = Token::new(TokenKind::String, Span::new(6, 13));
        assert_eq!(identifier.identifier(source), Some("users"));
        assert_eq!(string.identifier(source), None);
    }

    #[test]
    fn displays_token_compactly() {
        let token = Token::new(TokenKind::Identifier, Span::new(5, 10));
        assert_eq!(token.to_string(), "identifier at 5..10");
    }

    #[test]
    fn token_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<Token>();
        assert_copy::<TokenKind>();
    }

    #[test]
    fn token_remains_compact() {
        assert!(std::mem::size_of::<Token>() <= 3 * std::mem::size_of::<usize>());
    }
}
