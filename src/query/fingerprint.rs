//! Stable query fingerprints.

use std::fmt;

use super::{lex, LexResult, TokenKind, TokenStream};

/// Current fingerprint serialization format.
///
/// Increment this value whenever the canonical encoding of one or more
/// fingerprint types changes.
///
/// Versioning prevents an old persisted cache entry from silently matching a
/// fingerprint produced with different encoding rules.
const FINGERPRINT_FORMAT_VERSION: u64 = 1;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Fingerprint of the exact query source text.
///
/// Every byte contributes to this fingerprint, including:
///
/// - whitespace;
/// - comments, once comments are supported;
/// - literal spelling;
/// - keyword spelling;
/// - source aliases such as `from` and `on`.
///
/// Therefore:
///
/// ```text
/// from users|where age>=18
/// ```
///
/// and:
///
/// ```text
/// from users | where age >= 18
/// ```
///
/// have different text fingerprints.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryTextFingerprint(u64);

/// Fingerprint of the significant token sequence.
///
/// Insignificant whitespace does not contribute to this fingerprint. Exact
/// token lexemes do contribute, so different literal values and identifiers
/// remain distinguishable.
///
/// Consequently:
///
/// ```text
/// from users|where age>=18
/// ```
///
/// and:
///
/// ```text
/// from users | where age >= 18
/// ```
///
/// have the same syntax fingerprint, while `age >= 18` and `age >= 42` do not.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SyntaxFingerprint(u64);

/// Fingerprint of the reusable query shape.
///
/// Literal values are abstracted while their token kinds remain visible.
/// Identifiers, keywords, operators, punctuation, and stage names continue to
/// contribute to the fingerprint.
///
/// Therefore:
///
/// ```text
/// from users | where age >= 18
/// from users | where age >= 42
/// ```
///
/// have the same shape fingerprint.
///
/// However:
///
/// ```text
/// from users  | where age   >= 18
/// from orders | where total >= 18
/// ```
///
/// have different shape fingerprints because collection and field identifiers
/// differ.
///
/// Literal categories are intentionally preserved:
///
/// ```text
/// age == 18
/// age == "18"
/// ```
///
/// do not have the same shape because `Number` and `String` are distinct token
/// kinds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryShapeFingerprint(u64);

/// Fingerprint of a canonical logical plan.
///
/// The logical-plan layer is responsible for producing a deterministic
/// canonical byte representation. This type hashes that representation and
/// provides the common fingerprint API.
///
/// The query module must not hash a debug representation of a logical plan.
/// Debug formatting is not a stable serialization format.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalPlanFingerprint(u64);

impl QueryTextFingerprint {
    /// Computes a fingerprint from the exact UTF-8 source bytes.
    #[must_use]
    #[inline]
    pub fn new(source: &str) -> Self {
        let mut hasher = StableHasher::for_domain(b"og.query.text");

        hasher.write_u64(FINGERPRINT_FORMAT_VERSION);
        hasher.write_bytes(source.as_bytes());

        Self(hasher.finish())
    }

    /// Returns the raw 64-bit fingerprint.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns the fingerprint as big-endian bytes.
    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Returns the fingerprint as little-endian bytes.
    #[must_use]
    pub const fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    /// Reconstructs a fingerprint from its raw value.
    #[must_use]
    pub const fn from_value(value: u64) -> Self {
        Self(value)
    }
}

impl SyntaxFingerprint {
    /// Lexes a source query and computes its syntax fingerprint.
    ///
    /// # Errors
    ///
    /// Returns the lexical error produced by the query lexer.
    pub fn from_source(source: &str) -> LexResult<Self> {
        let stream = lex(source)?;
        Ok(Self::from_token_stream(&stream))
    }

    /// Computes a syntax fingerprint from an already validated token stream.
    #[must_use]
    pub fn from_token_stream(stream: &TokenStream<'_>) -> Self {
        let mut hasher = StableHasher::for_domain(b"og.query.syntax");

        hasher.write_u64(FINGERPRINT_FORMAT_VERSION);

        for token in stream.significant_tokens() {
            let kind = token.kind();

            write_token_kind(&mut hasher, kind);

            let lexeme = token
                .lexeme(stream.source())
                .expect("lexer-produced token spans must reference their source");

            hasher.write_str(lexeme);
        }

        Self(hasher.finish())
    }

    /// Returns the raw 64-bit fingerprint.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns the fingerprint as big-endian bytes.
    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Returns the fingerprint as little-endian bytes.
    #[must_use]
    pub const fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    /// Reconstructs a fingerprint from its raw value.
    #[must_use]
    pub const fn from_value(value: u64) -> Self {
        Self(value)
    }
}

impl QueryShapeFingerprint {
    /// Lexes a source query and computes its reusable query-shape fingerprint.
    ///
    /// # Errors
    ///
    /// Returns the lexical error produced by the query lexer.
    pub fn from_source(source: &str) -> LexResult<Self> {
        let stream = lex(source)?;
        Ok(Self::from_token_stream(&stream))
    }

    /// Computes a query-shape fingerprint from a validated token stream.
    #[must_use]
    pub fn from_token_stream(stream: &TokenStream<'_>) -> Self {
        let mut hasher = StableHasher::for_domain(b"og.query.shape");

        hasher.write_u64(FINGERPRINT_FORMAT_VERSION);

        for token in stream.significant_tokens() {
            let kind = token.kind();

            write_token_kind(&mut hasher, kind);

            if token_lexeme_contributes_to_shape(kind) {
                let lexeme = token
                    .lexeme(stream.source())
                    .expect("lexer-produced token spans must reference their source");

                hasher.write_str(lexeme);
            }
        }

        Self(hasher.finish())
    }

    /// Returns the raw 64-bit fingerprint.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns the fingerprint as big-endian bytes.
    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Returns the fingerprint as little-endian bytes.
    #[must_use]
    pub const fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    /// Reconstructs a fingerprint from its raw value.
    #[must_use]
    pub const fn from_value(value: u64) -> Self {
        Self(value)
    }
}

impl LogicalPlanFingerprint {
    /// Computes a fingerprint from a canonical logical-plan byte sequence.
    ///
    /// The caller must guarantee that semantically equivalent plans produce
    /// identical canonical bytes.
    #[must_use]
    pub fn from_canonical_bytes(bytes: &[u8]) -> Self {
        let mut hasher = StableHasher::for_domain(b"og.query.logical-plan");

        hasher.write_u64(FINGERPRINT_FORMAT_VERSION);
        hasher.write_bytes(bytes);

        Self(hasher.finish())
    }

    /// Computes a fingerprint from a canonical UTF-8 plan representation.
    ///
    /// This helper is suitable for a deliberately designed canonical format,
    /// but not for `Debug` output.
    #[must_use]
    pub fn from_canonical_str(canonical: &str) -> Self {
        Self::from_canonical_bytes(canonical.as_bytes())
    }

    /// Returns the raw 64-bit fingerprint.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns the fingerprint as big-endian bytes.
    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Returns the fingerprint as little-endian bytes.
    #[must_use]
    pub const fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    /// Reconstructs a fingerprint from its raw value.
    #[must_use]
    pub const fn from_value(value: u64) -> Self {
        Self(value)
    }
}

/// Computes the exact source-text fingerprint.
#[must_use]
pub fn fingerprint_query_text(source: &str) -> QueryTextFingerprint {
    QueryTextFingerprint::new(source)
}

/// Computes the significant-syntax fingerprint.
///
/// # Errors
///
/// Returns a lexical error when `source` is not a valid token sequence.
pub fn fingerprint_syntax(source: &str) -> LexResult<SyntaxFingerprint> {
    SyntaxFingerprint::from_source(source)
}

/// Computes the literal-independent query-shape fingerprint.
///
/// # Errors
///
/// Returns a lexical error when `source` is not a valid token sequence.
pub fn fingerprint_query_shape(source: &str) -> LexResult<QueryShapeFingerprint> {
    QueryShapeFingerprint::from_source(source)
}

/// Determines whether the exact lexeme of a token contributes to a query shape.
///
/// Literal token values are deliberately excluded. Their token kind has already
/// been written to the hash, preserving the literal category.
///
/// `End` is normally removed by [`TokenStream::significant_tokens`], but is
/// excluded here as a defensive measure.
#[must_use]
const fn token_lexeme_contributes_to_shape(kind: TokenKind) -> bool {
    !matches!(
        kind,
        TokenKind::String
            | TokenKind::Number
            | TokenKind::True
            | TokenKind::False
            | TokenKind::Null
            | TokenKind::End
    )
}

/// Writes a stable token-kind identifier.
///
/// We deliberately use the public static token name instead of casting the enum
/// to an integer. Enum discriminants can change when variants are reordered or
/// inserted.
fn write_token_kind(hasher: &mut StableHasher, kind: TokenKind) {
    hasher.write_str(kind.display_name());
}

/// Minimal deterministic FNV-1a hasher.
///
/// Values are length-prefixed where necessary to avoid ambiguous concatenation.
/// For example, the sequences `["ab", "c"]` and `["a", "bc"]` must not share
/// the same canonical byte stream.
#[derive(Clone)]
struct StableHasher {
    state: u64,
}

impl StableHasher {
    fn for_domain(domain: &[u8]) -> Self {
        let mut hasher = Self {
            state: FNV_OFFSET_BASIS,
        };

        hasher.write_bytes(domain);
        hasher
    }

    fn write_u64(&mut self, value: u64) {
        self.write_raw(&value.to_le_bytes());
    }

    fn write_str(&mut self, value: &str) {
        self.write_bytes(value.as_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        let length =
            u64::try_from(bytes.len()).expect("fingerprint input length must fit into u64");

        self.write_u64(length);
        self.write_raw(bytes);
    }

    fn write_raw(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(FNV_PRIME);
        }
    }

    const fn finish(&self) -> u64 {
        self.state
    }
}

macro_rules! impl_fingerprint_formatting {
    ($fingerprint:ty, $name:literal) => {
        impl fmt::Debug for $fingerprint {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple($name)
                    .field(&format_args!("{:016x}", self.0))
                    .finish()
            }
        }

        impl fmt::Display for $fingerprint {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{:016x}", self.0)
            }
        }
    };
}

impl_fingerprint_formatting!(QueryTextFingerprint, "QueryTextFingerprint");
impl_fingerprint_formatting!(SyntaxFingerprint, "SyntaxFingerprint");
impl_fingerprint_formatting!(QueryShapeFingerprint, "QueryShapeFingerprint");
impl_fingerprint_formatting!(LogicalPlanFingerprint, "LogicalPlanFingerprint");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_fingerprint_is_deterministic() {
        let source = "from users | where age >= 18";

        assert_eq!(
            QueryTextFingerprint::new(source),
            QueryTextFingerprint::new(source),
        );
    }

    #[test]
    fn text_fingerprint_preserves_whitespace() {
        let compact = QueryTextFingerprint::new("from users|where age>=18");
        let formatted = QueryTextFingerprint::new("from users | where age >= 18");

        assert_ne!(compact, formatted);
    }

    #[test]
    fn text_fingerprint_preserves_literal_spelling() {
        let first = QueryTextFingerprint::new("from users | where age >= 18");
        let second = QueryTextFingerprint::new("from users | where age >= 018");

        assert_ne!(first, second);
    }

    #[test]
    fn syntax_fingerprint_ignores_whitespace() {
        let compact = SyntaxFingerprint::from_source("from users|where age>=18").unwrap();

        let formatted = SyntaxFingerprint::from_source("from users | where age >= 18").unwrap();

        assert_eq!(compact, formatted);
    }

    #[test]
    fn syntax_fingerprint_preserves_literal_values() {
        let first = SyntaxFingerprint::from_source("from users | where age >= 18").unwrap();

        let second = SyntaxFingerprint::from_source("from users | where age >= 42").unwrap();

        assert_ne!(first, second);
    }

    #[test]
    fn syntax_fingerprint_preserves_identifiers() {
        let first = SyntaxFingerprint::from_source("from users | where age >= 18").unwrap();

        let second = SyntaxFingerprint::from_source("from users | where score >= 18").unwrap();

        assert_ne!(first, second);
    }

    #[test]
    fn syntax_fingerprint_preserves_source_alias() {
        let from = SyntaxFingerprint::from_source("from users | where age >= 18").unwrap();

        let on = SyntaxFingerprint::from_source("on users | where age >= 18").unwrap();

        assert_ne!(from, on);
    }

    #[test]
    fn shape_fingerprint_ignores_numeric_literal_values() {
        let first = QueryShapeFingerprint::from_source("from users | where age >= 18").unwrap();

        let second = QueryShapeFingerprint::from_source("from users | where age >= 42").unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn shape_fingerprint_ignores_string_literal_values() {
        let first =
            QueryShapeFingerprint::from_source(r#"from users | where country == "FR""#).unwrap();

        let second =
            QueryShapeFingerprint::from_source(r#"from users | where country == "DE""#).unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn shape_fingerprint_ignores_boolean_literal_values() {
        let true_query =
            QueryShapeFingerprint::from_source("from users | where active == true").unwrap();

        let false_query =
            QueryShapeFingerprint::from_source("from users | where active == false").unwrap();

        assert_ne!(true_query, false_query);
    }

    #[test]
    fn shape_fingerprint_preserves_literal_categories() {
        let number = QueryShapeFingerprint::from_source("from users | where age == 18").unwrap();

        let string =
            QueryShapeFingerprint::from_source(r#"from users | where age == "18""#).unwrap();

        assert_ne!(number, string);
    }

    #[test]
    fn shape_fingerprint_preserves_collection_names() {
        let users = QueryShapeFingerprint::from_source("from users | where id == 18").unwrap();

        let orders = QueryShapeFingerprint::from_source("from orders | where id == 18").unwrap();

        assert_ne!(users, orders);
    }

    #[test]
    fn shape_fingerprint_preserves_field_names() {
        let age = QueryShapeFingerprint::from_source("from users | where age >= 18").unwrap();

        let score = QueryShapeFingerprint::from_source("from users | where score >= 18").unwrap();

        assert_ne!(age, score);
    }

    #[test]
    fn shape_fingerprint_preserves_operators() {
        let greater = QueryShapeFingerprint::from_source("from users | where age > 18").unwrap();

        let greater_equal =
            QueryShapeFingerprint::from_source("from users | where age >= 18").unwrap();

        assert_ne!(greater, greater_equal);
    }

    #[test]
    fn shape_fingerprint_preserves_stage_names() {
        let filter = QueryShapeFingerprint::from_source("from users | where age >= 18").unwrap();

        let custom = QueryShapeFingerprint::from_source("from users | inspect age >= 18").unwrap();

        assert_ne!(filter, custom);
    }

    #[test]
    fn fingerprint_helpers_match_associated_functions() {
        let source = "from users | where age >= 18";

        assert_eq!(
            fingerprint_query_text(source),
            QueryTextFingerprint::new(source),
        );

        assert_eq!(
            fingerprint_syntax(source).unwrap(),
            SyntaxFingerprint::from_source(source).unwrap(),
        );

        assert_eq!(
            fingerprint_query_shape(source).unwrap(),
            QueryShapeFingerprint::from_source(source).unwrap(),
        );
    }

    #[test]
    fn logical_plan_fingerprint_is_deterministic() {
        let canonical = b"scan(users);filter(gte(field(age),parameter(number)))";

        assert_eq!(
            LogicalPlanFingerprint::from_canonical_bytes(canonical),
            LogicalPlanFingerprint::from_canonical_bytes(canonical),
        );
    }

    #[test]
    fn logical_plan_fingerprint_changes_with_canonical_plan() {
        let users = LogicalPlanFingerprint::from_canonical_str(
            "scan(users);filter(gte(field(age),parameter(number)))",
        );

        let orders = LogicalPlanFingerprint::from_canonical_str(
            "scan(orders);filter(gte(field(age),parameter(number)))",
        );

        assert_ne!(users, orders);
    }

    #[test]
    fn fingerprints_use_separate_domains() {
        let text = QueryTextFingerprint::new("from users").value();

        let syntax = SyntaxFingerprint::from_source("from users")
            .unwrap()
            .value();

        let shape = QueryShapeFingerprint::from_source("from users")
            .unwrap()
            .value();

        let logical = LogicalPlanFingerprint::from_canonical_str("from users").value();

        assert_ne!(text, syntax);
        assert_ne!(text, shape);
        assert_ne!(text, logical);
        assert_ne!(syntax, shape);
        assert_ne!(syntax, logical);
        assert_ne!(shape, logical);
    }

    #[test]
    fn display_is_fixed_width_lowercase_hexadecimal() {
        let fingerprint = QueryTextFingerprint::from_value(0x00ab_cdef);

        assert_eq!(fingerprint.to_string(), "0000000000abcdef");
    }

    #[test]
    fn debug_includes_fingerprint_type() {
        let fingerprint = QueryTextFingerprint::from_value(0x1234);

        assert_eq!(
            format!("{fingerprint:?}"),
            "QueryTextFingerprint(0000000000001234)",
        );
    }

    #[test]
    fn raw_value_round_trip() {
        let original = QueryShapeFingerprint::from_value(0x1234_5678_90ab_cdef);

        assert_eq!(
            QueryShapeFingerprint::from_value(original.value()),
            original,
        );

        assert_eq!(u64::from_be_bytes(original.to_be_bytes()), original.value(),);

        assert_eq!(u64::from_le_bytes(original.to_le_bytes()), original.value(),);
    }

    #[test]
    fn fingerprint_types_are_compact() {
        assert_eq!(
            std::mem::size_of::<QueryTextFingerprint>(),
            std::mem::size_of::<u64>(),
        );

        assert_eq!(
            std::mem::size_of::<SyntaxFingerprint>(),
            std::mem::size_of::<u64>(),
        );

        assert_eq!(
            std::mem::size_of::<QueryShapeFingerprint>(),
            std::mem::size_of::<u64>(),
        );

        assert_eq!(
            std::mem::size_of::<LogicalPlanFingerprint>(),
            std::mem::size_of::<u64>(),
        );
    }

    #[test]
    fn invalid_source_returns_lexer_error() {
        assert!(SyntaxFingerprint::from_source("from users @").is_err());
        assert!(QueryShapeFingerprint::from_source("from users @").is_err());
    }
}
