//! Query tokenization.

use std::fmt;
use std::iter::FusedIterator;

use super::{Span, Token, TokenKind};

/// Résultat d'une opération de lexing.
pub type LexResult<T> = Result<T, LexError>;

/// Erreur produite pendant l'analyse lexicale.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct LexError {
    kind: LexErrorKind,
    span: Span,
}

/// Catégorie d'erreur lexicale.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LexErrorKind {
    /// Caractère inconnu du langage.
    UnexpectedCharacter { character: char },

    /// `!` doit être suivi de `=`.
    ExpectedEqualsAfterBang,

    /// Chaîne non terminée avant la fin du texte.
    UnterminatedString,

    /// Séquence d'échappement non reconnue.
    InvalidEscape { character: char },

    /// Séquence d'échappement interrompue par la fin du texte.
    UnterminatedEscape,

    /// Nombre syntaxiquement invalide.
    InvalidNumber,

    /// Exposant numérique sans chiffre.
    MissingExponentDigits,
}

impl LexError {
    /// Construit une erreur lexicale.
    #[must_use]
    #[inline]
    pub const fn new(kind: LexErrorKind, span: Span) -> Self {
        Self { kind, span }
    }

    /// Retourne la catégorie de l'erreur.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &LexErrorKind {
        &self.kind
    }

    /// Retourne la position de l'erreur.
    #[must_use]
    #[inline]
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl fmt::Display for LexErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedCharacter { character } => {
                write!(formatter, "unexpected character `{character}`")
            }

            Self::ExpectedEqualsAfterBang => formatter.write_str("expected `=` after `!`"),

            Self::UnterminatedString => formatter.write_str("unterminated string"),

            Self::InvalidEscape { character } => {
                write!(formatter, "invalid escape sequence `\\{character}`")
            }

            Self::UnterminatedEscape => formatter.write_str("unterminated escape sequence"),

            Self::InvalidNumber => formatter.write_str("invalid number"),

            Self::MissingExponentDigits => {
                formatter.write_str("expected at least one digit after exponent marker")
            }
        }
    }
}

impl fmt::Display for LexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} at {}", self.kind, self.span)
    }
}

impl std::error::Error for LexError {}

/// Séquence lexicale complète d'une requête.
///
/// Le flux possède le texte source afin que les lexèmes puissent être relus
/// sans allocation depuis les spans des tokens.
///
/// Le dernier token est toujours [`TokenKind::End`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenStream<'source> {
    source: &'source str,
    tokens: Vec<Token>,
}

impl<'source> TokenStream<'source> {
    /// Construit un flux déjà validé.
    ///
    /// Le constructeur reste privé afin de garantir la présence du token
    /// terminal.
    #[inline]
    fn new(source: &'source str, tokens: Vec<Token>) -> Self {
        debug_assert!(
            tokens.last().is_some_and(|token| token.is_end()),
            "token stream must end with an End token",
        );

        Self { source, tokens }
    }

    /// Retourne le texte source complet.
    #[must_use]
    #[inline]
    pub const fn source(&self) -> &'source str {
        self.source
    }

    /// Retourne tous les tokens, token terminal inclus.
    #[must_use]
    pub fn tokens(&self) -> &[Token] {
        &self.tokens
    }

    /// Consomme le flux et retourne les tokens.
    #[must_use]
    pub fn into_tokens(self) -> Vec<Token> {
        self.tokens
    }

    /// Retourne le nombre total de tokens, token terminal inclus.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// Indique si le flux ne contient que le token terminal.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tokens.len() == 1
    }

    /// Retourne un token à une position donnée.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<Token> {
        self.tokens.get(index).copied()
    }

    /// Retourne le lexème d'un token.
    ///
    /// Le token doit provenir de ce flux pour que le résultat soit
    /// sémantiquement pertinent.
    #[must_use]
    pub fn lexeme(&self, token: Token) -> Option<&'source str> {
        token.lexeme(self.source)
    }

    /// Itère sur les tokens, token terminal inclus.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Token> + '_ {
        self.tokens.iter().copied()
    }

    /// Itère sur les tokens significatifs, sans le token terminal.
    pub fn significant_tokens(
        &self,
    ) -> impl DoubleEndedIterator<Item = Token> + ExactSizeIterator + '_ {
        self.tokens[..self.tokens.len().saturating_sub(1)]
            .iter()
            .copied()
    }

    /// Retourne la séquence des catégories de tokens.
    ///
    /// Cette séquence constitue une première représentation de la forme
    /// lexicale de la requête. Elle ne doit pas être utilisée comme identité
    /// logique définitive : deux requêtes ayant les mêmes catégories peuvent
    /// désigner des champs ou stages différents.
    pub fn kinds(&self) -> impl ExactSizeIterator<Item = TokenKind> + '_ {
        self.tokens.iter().map(|token| token.kind())
    }
}

impl<'source> IntoIterator for &'source TokenStream<'source> {
    type Item = Token;
    type IntoIter = std::iter::Copied<std::slice::Iter<'source, Token>>;

    fn into_iter(self) -> Self::IntoIter {
        self.tokens.iter().copied()
    }
}

/// Lexer incrémental d'une requête OG.
///
/// Les positions sont des offsets d'octets UTF-8.
#[derive(Clone, Debug)]
pub struct Lexer<'source> {
    source: &'source str,
    cursor: usize,
    finished: bool,
}

impl<'source> Lexer<'source> {
    /// Construit un lexer.
    #[must_use]
    #[inline]
    pub const fn new(source: &'source str) -> Self {
        Self {
            source,
            cursor: 0,
            finished: false,
        }
    }

    /// Retourne le texte source.
    #[must_use]
    #[inline]
    pub const fn source(&self) -> &'source str {
        self.source
    }

    /// Retourne la position actuelle en octets.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.cursor
    }

    /// Tokenise toute la requête.
    pub fn tokenize(mut self) -> LexResult<TokenStream<'source>> {
        let mut tokens = Vec::new();

        while let Some(result) = self.next() {
            let token = result?;
            let is_end = token.is_end();

            tokens.push(token);

            if is_end {
                break;
            }
        }

        Ok(TokenStream::new(self.source, tokens))
    }

    fn next_token(&mut self) -> LexResult<Token> {
        self.skip_whitespace();

        let start = self.cursor;

        let Some(character) = self.peek_char() else {
            self.finished = true;
            return Ok(Token::end(self.source.len()));
        };

        match character {
            '|' => {
                self.advance_char();
                Ok(self.token(TokenKind::Pipe, start))
            }

            '.' => {
                self.advance_char();
                Ok(self.token(TokenKind::Dot, start))
            }

            ':' => {
                self.advance_char();
                Ok(self.token(TokenKind::Colon, start))
            }

            ',' => {
                self.advance_char();
                Ok(self.token(TokenKind::Comma, start))
            }

            '(' => {
                self.advance_char();
                Ok(self.token(TokenKind::LeftParen, start))
            }

            ')' => {
                self.advance_char();
                Ok(self.token(TokenKind::RightParen, start))
            }

            '{' => {
                self.advance_char();
                Ok(self.token(TokenKind::LeftBrace, start))
            }

            '}' => {
                self.advance_char();
                Ok(self.token(TokenKind::RightBrace, start))
            }

            '[' => {
                self.advance_char();
                Ok(self.token(TokenKind::LeftBracket, start))
            }

            ']' => {
                self.advance_char();
                Ok(self.token(TokenKind::RightBracket, start))
            }

            '=' => self.lex_equal(start),
            '!' => self.lex_bang(start),
            '<' => self.lex_less(start),
            '>' => self.lex_greater(start),

            '"' => self.lex_string(start),

            '+' | '-' if self.next_char_is_ascii_digit() => self.lex_number(start),

            '+' | '-' if self.source.trim().len() == character.len_utf8() => {
                self.advance_char();

                Err(LexError::new(
                    LexErrorKind::UnexpectedCharacter { character },
                    Span::new(start, self.cursor),
                ))
            }

            '+' | '-' | '*' | '/' | '%' => {
                self.advance_char();
                Ok(self.token(TokenKind::Identifier, start))
            }

            character if character.is_ascii_digit() => self.lex_number(start),

            character if is_identifier_start(character) => {
                Ok(self.lex_identifier_or_keyword(start))
            }

            character => {
                self.advance_char();

                Err(LexError::new(
                    LexErrorKind::UnexpectedCharacter { character },
                    Span::new(start, self.cursor),
                ))
            }
        }
    }

    fn lex_equal(&mut self, start: usize) -> LexResult<Token> {
        self.advance_char();

        let kind = if self.consume_char('=') {
            TokenKind::EqualEqual
        } else {
            TokenKind::Equal
        };

        Ok(self.token(kind, start))
    }

    fn lex_bang(&mut self, start: usize) -> LexResult<Token> {
        self.advance_char();

        if self.consume_char('=') {
            Ok(self.token(TokenKind::NotEqual, start))
        } else {
            Err(LexError::new(
                LexErrorKind::ExpectedEqualsAfterBang,
                Span::new(start, self.cursor),
            ))
        }
    }

    fn lex_less(&mut self, start: usize) -> LexResult<Token> {
        self.advance_char();

        let kind = if self.consume_char('=') {
            TokenKind::LessEqual
        } else {
            TokenKind::Less
        };

        Ok(self.token(kind, start))
    }

    fn lex_greater(&mut self, start: usize) -> LexResult<Token> {
        self.advance_char();

        let kind = if self.consume_char('=') {
            TokenKind::GreaterEqual
        } else {
            TokenKind::Greater
        };

        Ok(self.token(kind, start))
    }

    fn lex_identifier_or_keyword(&mut self, start: usize) -> Token {
        self.advance_char();

        while self.peek_char().is_some_and(is_identifier_continue) {
            self.advance_char();
        }

        let span = Span::new(start, self.cursor);

        let lexeme = span
            .slice(self.source)
            .expect("lexer must produce valid UTF-8 spans");

        let kind = TokenKind::from_keyword(lexeme).unwrap_or(TokenKind::Identifier);

        Token::new(kind, span)
    }

    fn lex_string(&mut self, start: usize) -> LexResult<Token> {
        debug_assert_eq!(self.peek_char(), Some('"'));

        self.advance_char();

        loop {
            let Some(character) = self.peek_char() else {
                return Err(LexError::new(
                    LexErrorKind::UnterminatedString,
                    Span::new(start, self.cursor),
                ));
            };

            match character {
                '"' => {
                    self.advance_char();

                    return Ok(self.token(TokenKind::String, start));
                }

                '\\' => {
                    self.lex_escape()?;
                }

                '\n' | '\r' => {
                    return Err(LexError::new(
                        LexErrorKind::UnterminatedString,
                        Span::new(start, self.cursor),
                    ));
                }

                _ => {
                    self.advance_char();
                }
            }
        }
    }

    fn lex_escape(&mut self) -> LexResult<()> {
        let start = self.cursor;

        debug_assert_eq!(self.peek_char(), Some('\\'));

        self.advance_char();

        let Some(character) = self.peek_char() else {
            return Err(LexError::new(
                LexErrorKind::UnterminatedEscape,
                Span::new(start, self.cursor),
            ));
        };

        if matches!(character, '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't') {
            self.advance_char();
            return Ok(());
        }

        Err(LexError::new(
            LexErrorKind::InvalidEscape { character },
            Span::new(start, self.cursor + character.len_utf8()),
        ))
    }

    fn lex_number(&mut self, start: usize) -> LexResult<Token> {
        self.consume_sign();

        let integer_digits = self.consume_ascii_digits();

        if integer_digits == 0 {
            return Err(LexError::new(
                LexErrorKind::InvalidNumber,
                Span::new(start, self.cursor),
            ));
        }

        if self.peek_char() == Some('.')
            && self
                .peek_next_char()
                .is_some_and(|character| character.is_ascii_digit())
        {
            self.advance_char();
            self.consume_ascii_digits();
        }

        if matches!(self.peek_char(), Some('e' | 'E')) {
            self.advance_char();
            self.consume_sign();

            if self.consume_ascii_digits() == 0 {
                return Err(LexError::new(
                    LexErrorKind::MissingExponentDigits,
                    Span::new(start, self.cursor),
                ));
            }
        }

        if self.peek_char().is_some_and(is_identifier_start) {
            while self.peek_char().is_some_and(is_identifier_continue) {
                self.advance_char();
            }

            return Err(LexError::new(
                LexErrorKind::InvalidNumber,
                Span::new(start, self.cursor),
            ));
        }

        Ok(self.token(TokenKind::Number, start))
    }

    fn consume_sign(&mut self) {
        if matches!(self.peek_char(), Some('+' | '-')) {
            self.advance_char();
        }
    }

    fn consume_ascii_digits(&mut self) -> usize {
        let start = self.cursor;

        while self
            .peek_char()
            .is_some_and(|character| character.is_ascii_digit())
        {
            self.advance_char();
        }

        self.cursor - start
    }

    fn skip_whitespace(&mut self) {
        while self.peek_char().is_some_and(char::is_whitespace) {
            self.advance_char();
        }
    }

    fn token(&self, kind: TokenKind, start: usize) -> Token {
        Token::new(kind, Span::new(start, self.cursor))
    }

    fn peek_char(&self) -> Option<char> {
        self.source[self.cursor..].chars().next()
    }

    fn peek_next_char(&self) -> Option<char> {
        let mut characters = self.source[self.cursor..].chars();

        characters.next()?;
        characters.next()
    }

    fn next_char_is_ascii_digit(&self) -> bool {
        self.peek_next_char()
            .is_some_and(|character| character.is_ascii_digit())
    }

    fn consume_char(&mut self, expected: char) -> bool {
        if self.peek_char() == Some(expected) {
            self.advance_char();
            true
        } else {
            false
        }
    }

    fn advance_char(&mut self) -> Option<char> {
        let character = self.peek_char()?;
        self.cursor += character.len_utf8();

        Some(character)
    }
}

impl Iterator for Lexer<'_> {
    type Item = LexResult<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        Some(self.next_token())
    }
}

impl FusedIterator for Lexer<'_> {}

/// Tokenise entièrement une requête.
pub fn lex(source: &str) -> LexResult<TokenStream<'_>> {
    Lexer::new(source).tokenize()
}

fn is_identifier_start(character: char) -> bool {
    character == '_' || character.is_alphabetic()
}

fn is_identifier_continue(character: char) -> bool {
    character == '_' || character.is_alphabetic() || character.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(source: &str) -> Vec<TokenKind> {
        lex(source).unwrap().kinds().collect()
    }

    fn lexemes(source: &str) -> Vec<&str> {
        let stream = lex(source).unwrap();

        stream
            .significant_tokens()
            .map(|token| stream.lexeme(token).unwrap())
            .collect()
    }

    #[test]
    fn lexes_empty_source() {
        let stream = lex("").unwrap();

        assert!(stream.is_empty());
        assert_eq!(stream.len(), 1);
        assert_eq!(stream.tokens(), &[Token::end(0)]);
    }

    #[test]
    fn lexes_whitespace_only_source() {
        let stream = lex(" \n\t\r").unwrap();

        assert!(stream.is_empty());
        assert_eq!(stream.tokens(), &[Token::end(" \n\t\r".len())],);
    }

    #[test]
    fn lexes_minimal_from_query() {
        let source = "from users";
        let stream = lex(source).unwrap();

        assert_eq!(
            stream.tokens(),
            &[
                Token::new(TokenKind::From, Span::new(0, 4)),
                Token::new(TokenKind::Identifier, Span::new(5, 10)),
                Token::end(10),
            ],
        );

        assert_eq!(
            stream
                .significant_tokens()
                .map(|token| stream.lexeme(token).unwrap())
                .collect::<Vec<_>>(),
            vec!["from", "users"],
        );
    }

    #[test]
    fn lexes_filter_pipeline() {
        assert_eq!(
            kinds("from users | where age > 18"),
            vec![
                TokenKind::From,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Where,
                TokenKind::Identifier,
                TokenKind::Greater,
                TokenKind::Number,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_set_pipeline() {
        assert_eq!(
            kinds("from users | set active = true"),
            vec![
                TokenKind::From,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Set,
                TokenKind::Identifier,
                TokenKind::Equal,
                TokenKind::True,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_load_pipeline() {
        assert_eq!(
            kinds("from users | load profile"),
            vec![
                TokenKind::From,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Load,
                TokenKind::Identifier,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_join_as_compound_stage_keyword() {
        assert_eq!(
            kinds("on users | join workspace | into public | end"),
            vec![
                TokenKind::On,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Join,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Into,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::EndKeyword,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_lookup_pipeline() {
        assert_eq!(
            kinds(
                "on users as u \
                 | lookup workspace as w \
                 | where w.public == true \
                 | into public \
                 | end"
            ),
            vec![
                TokenKind::On,
                TokenKind::Identifier,
                TokenKind::As,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Lookup,
                TokenKind::Identifier,
                TokenKind::As,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Where,
                TokenKind::Identifier,
                TokenKind::Dot,
                TokenKind::Identifier,
                TokenKind::EqualEqual,
                TokenKind::True,
                TokenKind::Pipe,
                TokenKind::Into,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::EndKeyword,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_union_pipeline() {
        assert_eq!(
            kinds(
                "on users \
                 | union \
                 | on archived_users \
                 | where active == true \
                 | end"
            ),
            vec![
                TokenKind::On,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Union,
                TokenKind::Pipe,
                TokenKind::On,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Where,
                TokenKind::Identifier,
                TokenKind::EqualEqual,
                TokenKind::True,
                TokenKind::Pipe,
                TokenKind::EndKeyword,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_streaming_load_pipeline() {
        assert_eq!(
            kinds(
                "on users \
                 | load \
                 | with replace \
                 | chunk batch1 \
                 | chunk batch2 \
                 | end"
            ),
            vec![
                TokenKind::On,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Load,
                TokenKind::Pipe,
                TokenKind::With,
                TokenKind::Replace,
                TokenKind::Pipe,
                TokenKind::Chunk,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Chunk,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::EndKeyword,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_compact_load_modes() {
        assert_eq!(
            kinds("load x with replace load y with update load z with merge"),
            vec![
                TokenKind::Load,
                TokenKind::Identifier,
                TokenKind::With,
                TokenKind::Replace,
                TokenKind::Load,
                TokenKind::Identifier,
                TokenKind::With,
                TokenKind::Update,
                TokenKind::Load,
                TokenKind::Identifier,
                TokenKind::With,
                TokenKind::Merge,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn distinguishes_end_keyword_from_end_of_input() {
        let stream = lex("| end").unwrap();

        assert_eq!(
            stream.tokens(),
            &[
                Token::new(TokenKind::Pipe, Span::new(0, 1)),
                Token::new(TokenKind::EndKeyword, Span::new(2, 5)),
                Token::end(5),
            ],
        );

        assert!(stream.get(1).is_some_and(Token::is_end_keyword));
        assert!(stream.get(2).is_some_and(Token::is_end));
    }

    #[test]
    fn lexes_on_source() {
        assert_eq!(
            kinds("on users"),
            vec![TokenKind::On, TokenKind::Identifier, TokenKind::End],
        );
    }

    #[test]
    fn lexes_source_alias() {
        assert_eq!(
            kinds("on users as u"),
            vec![
                TokenKind::On,
                TokenKind::Identifier,
                TokenKind::As,
                TokenKind::Identifier,
                TokenKind::End,
            ],
        );

        assert_eq!(lexemes("on users as u"), vec!["on", "users", "as", "u"]);
    }

    #[test]
    fn lexes_all_reserved_words() {
        assert_eq!(
            kinds(
                "from on as where set lookup join union load into with chunk \
                 replace update merge end true false null and or not"
            ),
            vec![
                TokenKind::From,
                TokenKind::On,
                TokenKind::As,
                TokenKind::Where,
                TokenKind::Set,
                TokenKind::Lookup,
                TokenKind::Join,
                TokenKind::Union,
                TokenKind::Load,
                TokenKind::Into,
                TokenKind::With,
                TokenKind::Chunk,
                TokenKind::Replace,
                TokenKind::Update,
                TokenKind::Merge,
                TokenKind::EndKeyword,
                TokenKind::True,
                TokenKind::False,
                TokenKind::Null,
                TokenKind::And,
                TokenKind::Or,
                TokenKind::Not,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn keywords_are_case_sensitive() {
        assert_eq!(
            kinds("FROM From from"),
            vec![
                TokenKind::Identifier,
                TokenKind::Identifier,
                TokenKind::From,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_ascii_identifiers() {
        assert_eq!(
            lexemes("users _private user2 first_name"),
            vec!["users", "_private", "user2", "first_name",],
        );
    }

    #[test]
    fn lexes_unicode_identifiers() {
        assert_eq!(
            lexemes("employés âge résumé"),
            vec!["employés", "âge", "résumé"],
        );
    }

    #[test]
    fn lexes_field_paths() {
        assert_eq!(
            kinds("profile.address.city"),
            vec![
                TokenKind::Identifier,
                TokenKind::Dot,
                TokenKind::Identifier,
                TokenKind::Dot,
                TokenKind::Identifier,
                TokenKind::End,
            ],
        );

        assert_eq!(
            lexemes("profile.address.city"),
            vec!["profile", ".", "address", ".", "city"],
        );
    }

    #[test]
    fn lexes_punctuation() {
        assert_eq!(
            kinds("| . : , ( ) { } [ ]"),
            vec![
                TokenKind::Pipe,
                TokenKind::Dot,
                TokenKind::Colon,
                TokenKind::Comma,
                TokenKind::LeftParen,
                TokenKind::RightParen,
                TokenKind::LeftBrace,
                TokenKind::RightBrace,
                TokenKind::LeftBracket,
                TokenKind::RightBracket,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_empty_object_and_array() {
        assert_eq!(
            kinds("{} []"),
            vec![
                TokenKind::LeftBrace,
                TokenKind::RightBrace,
                TokenKind::LeftBracket,
                TokenKind::RightBracket,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_object_literal_shape() {
        let source = r#"{ _id: "u1", name: "John", age: 42, active: true }"#;

        assert_eq!(
            kinds(source),
            vec![
                TokenKind::LeftBrace,
                TokenKind::Identifier,
                TokenKind::Colon,
                TokenKind::String,
                TokenKind::Comma,
                TokenKind::Identifier,
                TokenKind::Colon,
                TokenKind::String,
                TokenKind::Comma,
                TokenKind::Identifier,
                TokenKind::Colon,
                TokenKind::Number,
                TokenKind::Comma,
                TokenKind::Identifier,
                TokenKind::Colon,
                TokenKind::True,
                TokenKind::RightBrace,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_quoted_object_keys() {
        let source = r#"{ "_id": "u1", "display-name": "John" }"#;

        assert_eq!(
            kinds(source),
            vec![
                TokenKind::LeftBrace,
                TokenKind::String,
                TokenKind::Colon,
                TokenKind::String,
                TokenKind::Comma,
                TokenKind::String,
                TokenKind::Colon,
                TokenKind::String,
                TokenKind::RightBrace,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_array_literal_shape() {
        let source = r#"["rust", "database", 42, true, false, null]"#;

        assert_eq!(
            kinds(source),
            vec![
                TokenKind::LeftBracket,
                TokenKind::String,
                TokenKind::Comma,
                TokenKind::String,
                TokenKind::Comma,
                TokenKind::Number,
                TokenKind::Comma,
                TokenKind::True,
                TokenKind::Comma,
                TokenKind::False,
                TokenKind::Comma,
                TokenKind::Null,
                TokenKind::RightBracket,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_nested_structured_values() {
        let source = r#"
            {
                user: {
                    name: "John",
                    address: {
                        city: "Paris"
                    }
                },
                tags: ["rust", "database"],
                scores: [1, 2.5, -3e2]
            }
        "#;

        let stream = lex(source).unwrap();

        assert_eq!(
            stream.kinds().collect::<Vec<_>>(),
            vec![
                TokenKind::LeftBrace,
                TokenKind::Identifier,
                TokenKind::Colon,
                TokenKind::LeftBrace,
                TokenKind::Identifier,
                TokenKind::Colon,
                TokenKind::String,
                TokenKind::Comma,
                TokenKind::Identifier,
                TokenKind::Colon,
                TokenKind::LeftBrace,
                TokenKind::Identifier,
                TokenKind::Colon,
                TokenKind::String,
                TokenKind::RightBrace,
                TokenKind::RightBrace,
                TokenKind::Comma,
                TokenKind::Identifier,
                TokenKind::Colon,
                TokenKind::LeftBracket,
                TokenKind::String,
                TokenKind::Comma,
                TokenKind::String,
                TokenKind::RightBracket,
                TokenKind::Comma,
                TokenKind::Identifier,
                TokenKind::Colon,
                TokenKind::LeftBracket,
                TokenKind::Number,
                TokenKind::Comma,
                TokenKind::Number,
                TokenKind::Comma,
                TokenKind::Number,
                TokenKind::RightBracket,
                TokenKind::RightBrace,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_insert_with_document_literal() {
        let source = r#"
            from users
            | insert {
                _id: "u1",
                name: "John",
                age: 42
            }
        "#;

        let stream = lex(source).unwrap();
        let kinds = stream.kinds().collect::<Vec<_>>();

        assert_eq!(kinds[0], TokenKind::From);
        assert_eq!(kinds[1], TokenKind::Identifier);
        assert_eq!(kinds[2], TokenKind::Pipe);
        assert_eq!(kinds[3], TokenKind::Identifier);
        assert_eq!(kinds[4], TokenKind::LeftBrace);
        assert_eq!(kinds.last(), Some(&TokenKind::End));

        assert!(kinds.contains(&TokenKind::Colon));
        assert!(kinds.contains(&TokenKind::RightBrace));
    }

    #[test]
    fn lexes_assignment_and_comparison_operators() {
        assert_eq!(
            kinds("= == != < <= > >="),
            vec![
                TokenKind::Equal,
                TokenKind::EqualEqual,
                TokenKind::NotEqual,
                TokenKind::Less,
                TokenKind::LessEqual,
                TokenKind::Greater,
                TokenKind::GreaterEqual,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_integer_numbers() {
        assert_eq!(
            lexemes("0 18 +18 -18 0018"),
            vec!["0", "18", "+18", "-18", "0018"],
        );
    }

    #[test]
    fn lexes_decimal_numbers() {
        assert_eq!(lexemes("18.5 -0.25 +42.0"), vec!["18.5", "-0.25", "+42.0"],);
    }

    #[test]
    fn lexes_exponent_numbers() {
        assert_eq!(
            lexemes("1e3 1E3 -2.5e-4 +6E+8"),
            vec!["1e3", "1E3", "-2.5e-4", "+6E+8"],
        );
    }

    #[test]
    fn dot_after_integer_is_separate_token_without_fraction_digits() {
        assert_eq!(
            kinds("18.field"),
            vec![
                TokenKind::Number,
                TokenKind::Dot,
                TokenKind::Identifier,
                TokenKind::End,
            ],
        );

        assert_eq!(lexemes("18.field"), vec!["18", ".", "field"],);
    }

    #[test]
    fn rejects_number_followed_by_identifier_characters() {
        let error = lex("18abc").unwrap_err();

        assert_eq!(error.kind(), &LexErrorKind::InvalidNumber);
        assert_eq!(error.span(), Span::new(0, 5));
    }

    #[test]
    fn rejects_exponent_without_digits() {
        let error = lex("1e").unwrap_err();

        assert_eq!(error.kind(), &LexErrorKind::MissingExponentDigits,);
        assert_eq!(error.span(), Span::new(0, 2));
    }

    #[test]
    fn rejects_signed_exponent_without_digits() {
        let error = lex("1e+").unwrap_err();

        assert_eq!(error.kind(), &LexErrorKind::MissingExponentDigits,);
        assert_eq!(error.span(), Span::new(0, 3));
    }

    #[test]
    fn standalone_plus_is_rejected() {
        let error = lex("+").unwrap_err();

        assert_eq!(
            error.kind(),
            &LexErrorKind::UnexpectedCharacter { character: '+' },
        );
    }

    #[test]
    fn standalone_minus_is_rejected() {
        let error = lex("-").unwrap_err();

        assert_eq!(
            error.kind(),
            &LexErrorKind::UnexpectedCharacter { character: '-' },
        );
    }

    #[test]
    fn lexes_strings() {
        let source = r#""hello" "hello world" "a \"quote\"""#;

        let stream = lex(source).unwrap();

        assert_eq!(
            stream.kinds().collect::<Vec<_>>(),
            vec![
                TokenKind::String,
                TokenKind::String,
                TokenKind::String,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_escaped_strings() {
        let source = r#""line\nnext" "quote\"" "slash\\""#;

        assert_eq!(
            kinds(source),
            vec![
                TokenKind::String,
                TokenKind::String,
                TokenKind::String,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn lexes_unicode_strings() {
        let source = r#""Paris" "été" "東京""#;

        assert_eq!(lexemes(source), vec![r#""Paris""#, r#""été""#, r#""東京""#],);
    }

    #[test]
    fn rejects_unterminated_string() {
        let error = lex(r#""Paris"#).unwrap_err();

        assert_eq!(error.kind(), &LexErrorKind::UnterminatedString,);
        assert_eq!(error.span(), Span::new(0, r#""Paris"#.len()),);
    }

    #[test]
    fn rejects_multiline_string() {
        let error = lex("\"first\nsecond\"").unwrap_err();

        assert_eq!(error.kind(), &LexErrorKind::UnterminatedString,);
        assert_eq!(error.span(), Span::new(0, 6));
    }

    #[test]
    fn rejects_invalid_escape() {
        let error = lex(r#""invalid\q""#).unwrap_err();

        assert_eq!(
            error.kind(),
            &LexErrorKind::InvalidEscape { character: 'q' },
        );
        assert_eq!(error.span(), Span::new(8, 10));
    }

    #[test]
    fn rejects_unterminated_escape() {
        let error = lex("\"value\\").unwrap_err();

        assert_eq!(error.kind(), &LexErrorKind::UnterminatedEscape,);
        assert_eq!(error.span(), Span::new(6, 7));
    }

    #[test]
    fn rejects_bang_without_equals() {
        let error = lex("!").unwrap_err();

        assert_eq!(error.kind(), &LexErrorKind::ExpectedEqualsAfterBang,);
        assert_eq!(error.span(), Span::new(0, 1));
    }

    #[test]
    fn rejects_unknown_character() {
        let error = lex("@").unwrap_err();

        assert_eq!(
            error.kind(),
            &LexErrorKind::UnexpectedCharacter { character: '@' },
        );
        assert_eq!(error.span(), Span::new(0, 1));
    }

    #[test]
    fn skips_all_standard_whitespace() {
        assert_eq!(
            kinds("from\tusers\n|\r\nwhere age > 18"),
            vec![
                TokenKind::From,
                TokenKind::Identifier,
                TokenKind::Pipe,
                TokenKind::Where,
                TokenKind::Identifier,
                TokenKind::Greater,
                TokenKind::Number,
                TokenKind::End,
            ],
        );
    }

    #[test]
    fn preserves_exact_spans_across_whitespace() {
        let source = "  from   users  ";
        let stream = lex(source).unwrap();

        assert_eq!(
            stream.tokens(),
            &[
                Token::new(TokenKind::From, Span::new(2, 6)),
                Token::new(TokenKind::Identifier, Span::new(9, 14),),
                Token::end(16),
            ],
        );
    }

    #[test]
    fn iterator_emits_end_once() {
        let mut lexer = Lexer::new("from");

        assert_eq!(
            lexer.next(),
            Some(Ok(Token::new(TokenKind::From, Span::new(0, 4),))),
        );

        assert_eq!(lexer.next(), Some(Ok(Token::end(4))));
        assert_eq!(lexer.next(), None);
        assert_eq!(lexer.next(), None);
    }

    #[test]
    fn token_stream_exposes_source_and_tokens() {
        let source = "from users";
        let stream = lex(source).unwrap();

        assert_eq!(stream.source(), source);
        assert_eq!(stream.len(), 3);
        assert!(!stream.is_empty());

        assert_eq!(
            stream.get(0),
            Some(Token::new(TokenKind::From, Span::new(0, 4),)),
        );

        assert_eq!(stream.get(99), None);
    }

    #[test]
    fn token_stream_iteration_is_stable() {
        let stream = lex("from users").unwrap();

        let first = stream.iter().collect::<Vec<_>>();
        let second = stream.iter().collect::<Vec<_>>();

        assert_eq!(first, second);
    }

    #[test]
    fn identical_queries_produce_identical_tokens() {
        let left = lex("from users | where age >= 18").unwrap();

        let right = lex("from users | where age >= 18").unwrap();

        assert_eq!(left, right);
    }

    #[test]
    fn whitespace_changes_spans_but_not_token_kinds() {
        let compact = lex("from users|where age>=18").unwrap();

        let spaced = lex("from users | where age >= 18").unwrap();

        assert_ne!(compact.tokens(), spaced.tokens());

        assert_eq!(
            compact.kinds().collect::<Vec<_>>(),
            spaced.kinds().collect::<Vec<_>>(),
        );
    }

    #[test]
    fn different_literals_keep_same_lexical_shape() {
        let first = lex("from users | where age >= 18").unwrap();

        let second = lex("from users | where age >= 42").unwrap();

        assert_eq!(
            first.kinds().collect::<Vec<_>>(),
            second.kinds().collect::<Vec<_>>(),
        );

        assert_ne!(
            first
                .significant_tokens()
                .map(|token| first.lexeme(token).unwrap())
                .collect::<Vec<_>>(),
            second
                .significant_tokens()
                .map(|token| second.lexeme(token).unwrap())
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn different_identifiers_are_not_hidden_by_token_stream() {
        let users = lex("from users | where age > 18").unwrap();

        let orders = lex("from orders | where total > 18").unwrap();

        assert_eq!(
            users.kinds().collect::<Vec<_>>(),
            orders.kinds().collect::<Vec<_>>(),
        );

        assert_ne!(
            users
                .significant_tokens()
                .map(|token| users.lexeme(token).unwrap())
                .collect::<Vec<_>>(),
            orders
                .significant_tokens()
                .map(|token| orders.lexeme(token).unwrap())
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn errors_are_displayed_with_spans() {
        let error = lex("@").unwrap_err();

        assert_eq!(error.to_string(), "unexpected character `@` at 0..1",);
    }
}
