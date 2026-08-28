//! Query abstract syntax tree.

use std::fmt;
use std::slice;

use super::Span;

/// Élément syntaxique possédant une position dans le texte source.
pub trait Spanned {
    /// Retourne le span complet de l'élément.
    fn span(&self) -> Span;
}

/// Mot-clé introduisant la source d'un pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SourceKeyword {
    /// Source introduite avec `from`.
    From,

    /// Alias de `from`, introduit avec `on`.
    On,
}

impl SourceKeyword {
    /// Retourne la représentation canonique du mot-clé.
    ///
    /// Cette représentation est syntaxique. La normalisation logique pourra
    /// considérer `from` et `on` comme équivalents.
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::From => "from",
            Self::On => "on",
        }
    }
}

impl fmt::Display for SourceKeyword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

macro_rules! span_ast {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident;
        panic = $panic:literal;
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        $vis struct $name {
            span: Span,
        }

        impl $name {
            /// Construit le nœud à partir de son span.
            ///
            /// # Panics
            ///
            /// Panique lorsque le span est vide.
            #[must_use]
            #[inline]
            pub const fn new(span: Span) -> Self {
                assert!(!span.is_empty(), $panic);
                Self { span }
            }

            /// Retourne le texte couvert par le nœud dans la source.
            #[must_use]
            pub fn text<'source>(self, source: &'source str) -> Option<&'source str> {
                self.span.slice(source)
            }
        }

        impl Spanned for $name {
            #[inline]
            fn span(&self) -> Span {
                self.span
            }
        }
    };
}

span_ast! {
    /// Nom syntaxique présent dans une requête.
    ///
    /// Un nom peut représenter, selon son contexte :
    ///
    /// - une collection ;
    /// - un alias ;
    /// - un stage ;
    /// - un champ ;
    /// - un symbole futur.
    ///
    /// Le contenu reste dans le texte source et est accessible avec [`NameAst::text`].
    pub struct NameAst;
    panic = "name span must not be empty";
}

impl fmt::Display for NameAst {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "name at {}", self.span)
    }
}

span_ast! {
    /// Chaîne littérale présente dans la source.
    ///
    /// Le span contient le littéral complet, guillemets inclus.
    pub struct StringAst;
    panic = "string span must not be empty";
}

span_ast! {
    /// Nombre littéral présent dans la source.
    ///
    /// L'AST ne choisit volontairement aucun type numérique. Le texte exact est
    /// conservé afin que la normalisation décide ultérieurement entre entier,
    /// décimal ou autre représentation.
    pub struct NumberAst;
    panic = "number span must not be empty";
}

/// Booléen littéral.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BooleanAst {
    value: bool,
    span: Span,
}

impl BooleanAst {
    #[must_use]
    #[inline]
    pub const fn new(value: bool, span: Span) -> Self {
        assert!(!span.is_empty(), "boolean span must not be empty");
        Self { value, span }
    }

    #[must_use]
    pub const fn value(self) -> bool {
        self.value
    }

    #[must_use]
    pub fn text<'source>(self, source: &'source str) -> Option<&'source str> {
        self.span.slice(source)
    }
}

impl Spanned for BooleanAst {
    #[inline]
    fn span(&self) -> Span {
        self.span
    }
}

span_ast! {
    /// Littéral `null`.
    pub struct NullAst;
    panic = "null span must not be empty";
}

/// Clé syntaxique d'un champ d'objet.
///
/// Les clés simples peuvent être écrites comme des identifiants. Les clés
/// contenant des caractères spéciaux restent accessibles sous forme de chaînes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ObjectKeyAst {
    Identifier(NameAst),
    String(StringAst),
}

impl ObjectKeyAst {
    #[must_use]
    pub const fn is_identifier(self) -> bool {
        matches!(self, Self::Identifier(_))
    }

    #[must_use]
    pub const fn is_string(self) -> bool {
        matches!(self, Self::String(_))
    }

    #[must_use]
    pub fn text<'source>(self, source: &'source str) -> Option<&'source str> {
        match self {
            Self::Identifier(name) => name.text(source),
            Self::String(string) => string.text(source),
        }
    }
}

impl Spanned for ObjectKeyAst {
    #[inline]
    fn span(&self) -> Span {
        match self {
            Self::Identifier(name) => name.span(),
            Self::String(string) => string.span(),
        }
    }
}

/// Valeur syntaxique générique.
///
/// Les objets et tableaux sont récursifs. Les identifiants restent distincts
/// des chaînes afin que les couches suivantes puissent les interpréter comme
/// références, symboles ou valeurs selon leur contexte.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ValueAst {
    String(StringAst),
    Number(NumberAst),
    Boolean(BooleanAst),
    Null(NullAst),
    Identifier(NameAst),
    Array(ArrayAst),
    Object(ObjectAst),
}

impl ValueAst {
    #[must_use]
    pub const fn is_scalar(&self) -> bool {
        matches!(
            self,
            Self::String(_)
                | Self::Number(_)
                | Self::Boolean(_)
                | Self::Null(_)
                | Self::Identifier(_)
        )
    }

    #[must_use]
    pub const fn is_array(&self) -> bool {
        matches!(self, Self::Array(_))
    }

    #[must_use]
    pub const fn is_object(&self) -> bool {
        matches!(self, Self::Object(_))
    }

    #[must_use]
    pub fn text<'source>(&self, source: &'source str) -> Option<&'source str> {
        self.span().slice(source)
    }
}

impl Spanned for ValueAst {
    #[inline]
    fn span(&self) -> Span {
        match self {
            Self::String(value) => value.span(),
            Self::Number(value) => value.span(),
            Self::Boolean(value) => value.span(),
            Self::Null(value) => value.span(),
            Self::Identifier(value) => value.span(),
            Self::Array(value) => value.span(),
            Self::Object(value) => value.span(),
        }
    }
}

/// Tableau syntaxique récursif.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ArrayAst {
    open_span: Span,
    values: Vec<ValueAst>,
    close_span: Span,
    span: Span,
}

impl ArrayAst {
    /// Construit un tableau.
    ///
    /// Les virgules ne sont pas matérialisées comme nœuds : leur présence reste
    /// conservée dans le texte couvert par `span`.
    #[must_use]
    #[inline]
    pub fn new(open_span: Span, values: Vec<ValueAst>, close_span: Span, span: Span) -> Self {
        assert!(
            !open_span.is_empty(),
            "array opening span must not be empty"
        );
        assert!(
            !close_span.is_empty(),
            "array closing span must not be empty"
        );
        assert!(
            span.contains_span(open_span),
            "array span must contain opening bracket",
        );
        assert!(
            span.contains_span(close_span),
            "array span must contain closing bracket",
        );
        assert!(
            open_span.end() <= close_span.start(),
            "array opening bracket must precede closing bracket",
        );

        let mut previous_end = open_span.end();

        for value in &values {
            assert!(
                span.contains_span(value.span()),
                "array span must contain every value",
            );
            assert!(
                previous_end <= value.span().start(),
                "array values must be ordered",
            );
            assert!(
                value.span().end() <= close_span.start(),
                "array value must precede closing bracket",
            );
            previous_end = value.span().end();
        }

        Self {
            open_span,
            values,
            close_span,
            span,
        }
    }

    #[must_use]
    pub const fn open_span(&self) -> Span {
        self.open_span
    }

    #[must_use]
    pub const fn close_span(&self) -> Span {
        self.close_span
    }

    #[must_use]
    pub fn values(&self) -> &[ValueAst] {
        &self.values
    }

    #[must_use]
    pub fn value(&self, index: usize) -> Option<&ValueAst> {
        self.values.get(index)
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn iter(&self) -> slice::Iter<'_, ValueAst> {
        self.values.iter()
    }

    #[must_use]
    pub fn into_values(self) -> Vec<ValueAst> {
        self.values
    }
}

impl Spanned for ArrayAst {
    #[inline]
    fn span(&self) -> Span {
        self.span
    }
}

impl<'ast> IntoIterator for &'ast ArrayAst {
    type Item = &'ast ValueAst;
    type IntoIter = slice::Iter<'ast, ValueAst>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

/// Champ syntaxique d'un objet.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectFieldAst {
    key: ObjectKeyAst,
    colon_span: Span,
    value: ValueAst,
    span: Span,
}

impl ObjectFieldAst {
    #[must_use]
    #[inline]
    pub fn new(key: ObjectKeyAst, colon_span: Span, value: ValueAst, span: Span) -> Self {
        assert!(
            !colon_span.is_empty(),
            "object field colon span must not be empty"
        );
        assert!(
            span.contains_span(key.span()),
            "object field span must contain key",
        );
        assert!(
            span.contains_span(colon_span),
            "object field span must contain colon",
        );
        assert!(
            span.contains_span(value.span()),
            "object field span must contain value",
        );
        assert!(
            key.span().end() <= colon_span.start(),
            "object field key must precede colon",
        );
        assert!(
            colon_span.end() <= value.span().start(),
            "object field colon must precede value",
        );

        Self {
            key,
            colon_span,
            value,
            span,
        }
    }

    #[must_use]
    pub const fn key(&self) -> ObjectKeyAst {
        self.key
    }

    #[must_use]
    pub const fn colon_span(&self) -> Span {
        self.colon_span
    }

    #[must_use]
    pub const fn value(&self) -> &ValueAst {
        &self.value
    }

    #[must_use]
    pub fn key_text<'source>(&self, source: &'source str) -> Option<&'source str> {
        self.key.text(source)
    }

    #[must_use]
    pub fn into_parts(self) -> (ObjectKeyAst, ValueAst) {
        (self.key, self.value)
    }
}

impl Spanned for ObjectFieldAst {
    #[inline]
    fn span(&self) -> Span {
        self.span
    }
}

/// Objet syntaxique récursif.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectAst {
    open_span: Span,
    fields: Vec<ObjectFieldAst>,
    close_span: Span,
    span: Span,
}

impl ObjectAst {
    /// Construit un objet.
    ///
    /// Les virgules restent représentées indirectement dans le texte couvert par
    /// le span complet. Cela autorise notamment une virgule finale sans alourdir
    /// la représentation sémantique.
    #[must_use]
    #[inline]
    pub fn new(open_span: Span, fields: Vec<ObjectFieldAst>, close_span: Span, span: Span) -> Self {
        assert!(
            !open_span.is_empty(),
            "object opening span must not be empty"
        );
        assert!(
            !close_span.is_empty(),
            "object closing span must not be empty"
        );
        assert!(
            span.contains_span(open_span),
            "object span must contain opening brace",
        );
        assert!(
            span.contains_span(close_span),
            "object span must contain closing brace",
        );
        assert!(
            open_span.end() <= close_span.start(),
            "object opening brace must precede closing brace",
        );

        let mut previous_end = open_span.end();

        for field in &fields {
            assert!(
                span.contains_span(field.span()),
                "object span must contain every field",
            );
            assert!(
                previous_end <= field.span().start(),
                "object fields must be ordered",
            );
            assert!(
                field.span().end() <= close_span.start(),
                "object field must precede closing brace",
            );
            previous_end = field.span().end();
        }

        Self {
            open_span,
            fields,
            close_span,
            span,
        }
    }

    #[must_use]
    pub const fn open_span(&self) -> Span {
        self.open_span
    }

    #[must_use]
    pub const fn close_span(&self) -> Span {
        self.close_span
    }

    #[must_use]
    pub fn fields(&self) -> &[ObjectFieldAst] {
        &self.fields
    }

    #[must_use]
    #[inline]
    pub fn field(&self, index: usize) -> Option<&ObjectFieldAst> {
        self.fields.get(index)
    }

    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn iter(&self) -> slice::Iter<'_, ObjectFieldAst> {
        self.fields.iter()
    }

    #[must_use]
    pub fn into_fields(self) -> Vec<ObjectFieldAst> {
        self.fields
    }
}

impl Spanned for ObjectAst {
    #[inline]
    fn span(&self) -> Span {
        self.span
    }
}

impl<'ast> IntoIterator for &'ast ObjectAst {
    type Item = &'ast ObjectFieldAst;
    type IntoIter = slice::Iter<'ast, ObjectFieldAst>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.iter()
    }
}

/// Alias syntaxique d'une source.
///
/// Exemple :
///
/// ```text
/// on users as u
///          ^^^^
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SourceAliasAst {
    as_span: Span,
    name: NameAst,
    span: Span,
}

impl SourceAliasAst {
    /// Construit un alias de source.
    ///
    /// `span` doit couvrir le mot-clé `as` et le nom de l'alias.
    ///
    /// # Panics
    ///
    /// Panique lorsque les spans sont incohérents.
    #[must_use]
    #[inline]
    pub const fn new(as_span: Span, name: NameAst, span: Span) -> Self {
        assert!(
            !as_span.is_empty(),
            "source alias `as` span must not be empty",
        );
        assert!(
            span.contains_span(as_span),
            "source alias span must contain `as` span",
        );
        assert!(
            span.contains_span(name.span),
            "source alias span must contain alias name",
        );
        assert!(
            as_span.end() <= name.span.start(),
            "source alias `as` keyword must precede alias name",
        );

        Self {
            as_span,
            name,
            span,
        }
    }

    #[must_use]
    pub const fn as_span(self) -> Span {
        self.as_span
    }

    #[must_use]
    #[inline]
    pub const fn name(self) -> NameAst {
        self.name
    }

    #[must_use]
    pub fn name_text<'source>(self, source: &'source str) -> Option<&'source str> {
        self.name.text(source)
    }
}

impl Spanned for SourceAliasAst {
    #[inline]
    fn span(&self) -> Span {
        self.span
    }
}

/// Source syntaxique d'un pipeline.
///
/// Exemples :
///
/// ```text
/// from users
/// on orders
/// on users as u
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SourceAst {
    keyword: SourceKeyword,
    keyword_span: Span,
    collection: NameAst,
    alias: Option<SourceAliasAst>,
    span: Span,
}

impl SourceAst {
    /// Construit une source sans alias.
    #[must_use]
    #[inline]
    pub const fn new(
        keyword: SourceKeyword,
        keyword_span: Span,
        collection: NameAst,
        span: Span,
    ) -> Self {
        Self::build(keyword, keyword_span, collection, None, span)
    }

    /// Construit une source avec alias.
    #[must_use]
    pub const fn with_alias(
        keyword: SourceKeyword,
        keyword_span: Span,
        collection: NameAst,
        alias: SourceAliasAst,
        span: Span,
    ) -> Self {
        Self::build(keyword, keyword_span, collection, Some(alias), span)
    }

    const fn build(
        keyword: SourceKeyword,
        keyword_span: Span,
        collection: NameAst,
        alias: Option<SourceAliasAst>,
        span: Span,
    ) -> Self {
        assert!(
            !keyword_span.is_empty(),
            "source keyword span must not be empty",
        );
        assert!(
            span.contains_span(keyword_span),
            "source span must contain keyword span",
        );
        assert!(
            span.contains_span(collection.span),
            "source span must contain collection span",
        );
        assert!(
            keyword_span.end() <= collection.span.start(),
            "source keyword must precede collection",
        );

        if let Some(alias) = alias {
            assert!(
                span.contains_span(alias.span),
                "source span must contain alias span",
            );
            assert!(
                collection.span.end() <= alias.span.start(),
                "source collection must precede alias",
            );
        }

        Self {
            keyword,
            keyword_span,
            collection,
            alias,
            span,
        }
    }

    #[must_use]
    pub const fn keyword(self) -> SourceKeyword {
        self.keyword
    }

    #[must_use]
    pub const fn keyword_span(self) -> Span {
        self.keyword_span
    }

    #[must_use]
    pub const fn collection(self) -> NameAst {
        self.collection
    }

    #[must_use]
    pub fn collection_name<'source>(self, source: &'source str) -> Option<&'source str> {
        self.collection.text(source)
    }

    #[must_use]
    pub const fn alias(self) -> Option<SourceAliasAst> {
        self.alias
    }

    #[must_use]
    pub fn alias_name<'source>(self, source: &'source str) -> Option<&'source str> {
        self.alias.and_then(|alias| alias.name_text(source))
    }

    #[must_use]
    pub const fn has_alias(self) -> bool {
        self.alias.is_some()
    }
}

impl Spanned for SourceAst {
    #[inline]
    fn span(&self) -> Span {
        self.span
    }
}

/// Fermeture syntaxique d'un sous-pipeline.
///
/// La forme canonique est `| end`. Ce nœud ferme le sous-pipeline courant mais
/// n'est pas lui-même un stage métier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EndAst {
    pipe_span: Span,
    keyword_span: Span,
    span: Span,
}

impl EndAst {
    #[must_use]
    #[inline]
    pub const fn new(pipe_span: Span, keyword_span: Span, span: Span) -> Self {
        assert!(!pipe_span.is_empty(), "end pipe span must not be empty");
        assert!(
            !keyword_span.is_empty(),
            "end keyword span must not be empty",
        );
        assert!(
            span.contains_span(pipe_span),
            "end span must contain pipe span",
        );
        assert!(
            span.contains_span(keyword_span),
            "end span must contain keyword span",
        );
        assert!(
            pipe_span.end() <= keyword_span.start(),
            "end pipe must precede end keyword",
        );

        Self {
            pipe_span,
            keyword_span,
            span,
        }
    }

    #[must_use]
    pub const fn pipe_span(self) -> Span {
        self.pipe_span
    }

    #[must_use]
    pub const fn keyword_span(self) -> Span {
        self.keyword_span
    }

    #[must_use]
    pub fn text<'source>(self, source: &'source str) -> Option<&'source str> {
        self.span.slice(source)
    }
}

impl Spanned for EndAst {
    #[inline]
    fn span(&self) -> Span {
        self.span
    }
}

/// Sous-pipeline syntaxique contenu par un stage composé.
///
/// Le sous-pipeline contient zéro ou plusieurs stages et se termine toujours par
/// un [`EndAst`]. Il ne possède pas de source structurelle propre : une forme
/// telle que `| on archived_users` dans un `union` reste un stage générique que
/// la normalisation spécialisée interprétera ensuite.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubPipelineAst {
    stages: Vec<StageAst>,
    end: EndAst,
    span: Span,
}

impl SubPipelineAst {
    #[must_use]
    #[inline]
    pub fn new(stages: Vec<StageAst>, end: EndAst, span: Span) -> Self {
        let mut previous_end = span.start();

        for stage in &stages {
            assert!(
                span.contains_span(stage.span()),
                "sub-pipeline span must contain every stage",
            );
            assert!(
                previous_end <= stage.span().start(),
                "sub-pipeline stages must be ordered",
            );
            previous_end = stage.span().end();
        }

        assert!(
            span.contains_span(end.span()),
            "sub-pipeline span must contain end",
        );
        assert!(
            previous_end <= end.span().start(),
            "sub-pipeline end must follow its stages",
        );

        Self { stages, end, span }
    }

    #[must_use]
    pub fn empty(end: EndAst) -> Self {
        Self {
            stages: Vec::new(),
            span: end.span(),
            end,
        }
    }

    #[must_use]
    pub fn stages(&self) -> &[StageAst] {
        &self.stages
    }

    #[must_use]
    pub fn stage(&self, index: usize) -> Option<&StageAst> {
        self.stages.get(index)
    }

    #[must_use]
    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    #[must_use]
    pub const fn end(&self) -> EndAst {
        self.end
    }

    pub fn iter_stages(&self) -> slice::Iter<'_, StageAst> {
        self.stages.iter()
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StageAst>, EndAst) {
        (self.stages, self.end)
    }

    #[must_use]
    pub fn text<'source>(&self, source: &'source str) -> Option<&'source str> {
        self.span.slice(source)
    }
}

impl Spanned for SubPipelineAst {
    #[inline]
    fn span(&self) -> Span {
        self.span
    }
}

impl<'ast> IntoIterator for &'ast SubPipelineAst {
    type Item = &'ast StageAst;
    type IntoIter = slice::Iter<'ast, StageAst>;

    fn into_iter(self) -> Self::IntoIter {
        self.stages.iter()
    }
}

/// Stage syntaxique générique.
///
/// L'AST conserve le séparateur `|`, le nom, la zone brute des arguments,
/// l'en-tête, l'éventuel sous-pipeline et le span complet.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StageAst {
    pipe_span: Span,
    name: NameAst,
    arguments_span: Span,
    header_span: Span,
    subpipeline: Option<SubPipelineAst>,
    span: Span,
}

impl StageAst {
    /// Construit un stage simple sans sous-pipeline.
    #[must_use]
    #[inline]
    pub fn new(pipe_span: Span, name: NameAst, arguments_span: Span, span: Span) -> Self {
        Self::validate_header(pipe_span, name, arguments_span, span);

        Self {
            pipe_span,
            name,
            arguments_span,
            header_span: span,
            subpipeline: None,
            span,
        }
    }

    /// Construit un stage composé possédant un sous-pipeline.
    #[must_use]
    pub fn with_subpipeline(
        pipe_span: Span,
        name: NameAst,
        arguments_span: Span,
        header_span: Span,
        subpipeline: SubPipelineAst,
        span: Span,
    ) -> Self {
        Self::validate_header(pipe_span, name, arguments_span, header_span);

        assert!(
            span.contains_span(header_span),
            "composite stage span must contain header span",
        );
        assert!(
            span.contains_span(subpipeline.span()),
            "composite stage span must contain sub-pipeline span",
        );
        assert!(
            header_span.end() <= subpipeline.span().start(),
            "composite stage header must precede sub-pipeline",
        );

        Self {
            pipe_span,
            name,
            arguments_span,
            header_span,
            subpipeline: Some(subpipeline),
            span,
        }
    }

    fn validate_header(pipe_span: Span, name: NameAst, arguments_span: Span, header_span: Span) {
        assert!(!pipe_span.is_empty(), "stage pipe span must not be empty");
        assert!(
            header_span.contains_span(pipe_span),
            "stage header span must contain pipe span",
        );
        assert!(
            header_span.contains_span(name.span),
            "stage header span must contain name span",
        );
        assert!(
            header_span.contains_span(arguments_span),
            "stage header span must contain arguments span",
        );
        assert!(
            pipe_span.end() <= name.span.start(),
            "stage pipe must precede stage name",
        );
        assert!(
            name.span.end() <= arguments_span.start(),
            "stage name must precede stage arguments",
        );
    }

    #[must_use]
    pub const fn pipe_span(&self) -> Span {
        self.pipe_span
    }

    #[must_use]
    #[inline]
    pub const fn name(&self) -> NameAst {
        self.name
    }

    #[must_use]
    pub fn name_text<'source>(&self, source: &'source str) -> Option<&'source str> {
        self.name.text(source)
    }

    #[must_use]
    pub const fn arguments_span(&self) -> Span {
        self.arguments_span
    }

    #[must_use]
    pub fn arguments_text<'source>(&self, source: &'source str) -> Option<&'source str> {
        self.arguments_span.slice(source)
    }

    #[must_use]
    pub const fn has_arguments(&self) -> bool {
        !self.arguments_span.is_empty()
    }

    #[must_use]
    pub const fn header_span(&self) -> Span {
        self.header_span
    }

    #[must_use]
    pub const fn subpipeline(&self) -> Option<&SubPipelineAst> {
        self.subpipeline.as_ref()
    }

    #[must_use]
    pub const fn is_composite(&self) -> bool {
        self.subpipeline.is_some()
    }

    #[must_use]
    pub fn into_parts(self) -> (NameAst, Span, Option<SubPipelineAst>) {
        (self.name, self.arguments_span, self.subpipeline)
    }
}

impl Spanned for StageAst {
    #[inline]
    fn span(&self) -> Span {
        self.span
    }
}

/// Pipeline syntaxique complet.
///
/// Un pipeline racine possède exactement une source et zéro ou plusieurs
/// stages. Les sous-pipelines sont portés récursivement par les stages composés.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PipelineAst {
    source: SourceAst,
    stages: Vec<StageAst>,
    span: Span,
}

impl PipelineAst {
    #[must_use]
    #[inline]
    pub fn new(source: SourceAst, stages: Vec<StageAst>, span: Span) -> Self {
        assert!(
            span.contains_span(source.span()),
            "pipeline span must contain source span",
        );

        let mut previous_end = source.span().end();

        for stage in &stages {
            assert!(
                span.contains_span(stage.span()),
                "pipeline span must contain every stage",
            );
            assert!(
                previous_end <= stage.span().start(),
                "pipeline stages must be ordered",
            );
            previous_end = stage.span().end();
        }

        Self {
            source,
            stages,
            span,
        }
    }

    #[must_use]
    pub fn source_only(source: SourceAst) -> Self {
        Self {
            span: source.span(),
            source,
            stages: Vec::new(),
        }
    }

    #[must_use]
    #[inline]
    pub const fn source(&self) -> SourceAst {
        self.source
    }

    #[must_use]
    pub fn stages(&self) -> &[StageAst] {
        &self.stages
    }

    #[must_use]
    pub fn stage(&self, index: usize) -> Option<&StageAst> {
        self.stages.get(index)
    }

    #[must_use]
    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    #[must_use]
    pub fn is_source_only(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn iter_stages(&self) -> slice::Iter<'_, StageAst> {
        self.stages.iter()
    }

    #[must_use]
    pub fn into_parts(self) -> (SourceAst, Vec<StageAst>) {
        (self.source, self.stages)
    }

    #[must_use]
    pub fn text<'source>(&self, source: &'source str) -> Option<&'source str> {
        self.span.slice(source)
    }
}

impl Spanned for PipelineAst {
    #[inline]
    fn span(&self) -> Span {
        self.span
    }
}

impl<'ast> IntoIterator for &'ast PipelineAst {
    type Item = &'ast StageAst;
    type IntoIter = slice::Iter<'ast, StageAst>;

    fn into_iter(self) -> Self::IntoIter {
        self.stages.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[inline]
    fn name(start: usize, end: usize) -> NameAst {
        NameAst::new(Span::new(start, end))
    }

    fn users_source() -> SourceAst {
        SourceAst::new(
            SourceKeyword::From,
            Span::new(0, 4),
            name(5, 10),
            Span::new(0, 10),
        )
    }

    #[test]
    fn source_keyword_has_stable_text() {
        assert_eq!(SourceKeyword::From.as_str(), "from");
        assert_eq!(SourceKeyword::On.as_str(), "on");
        assert_eq!(SourceKeyword::From.to_string(), "from");
        assert_eq!(SourceKeyword::On.to_string(), "on");
    }

    #[test]
    fn creates_name() {
        let name = NameAst::new(Span::new(5, 10));
        assert_eq!(name.span(), Span::new(5, 10));
        assert_eq!(name.text("from users"), Some("users"));
    }

    #[test]
    #[should_panic(expected = "name span must not be empty")]
    fn rejects_empty_name() {
        let _ = NameAst::new(Span::at(5));
    }

    #[test]
    fn displays_name_compactly() {
        let name = NameAst::new(Span::new(5, 10));
        assert_eq!(name.to_string(), "name at 5..10");
    }

    #[test]
    fn creates_scalar_values() {
        let string = ValueAst::String(StringAst::new(Span::new(0, 6)));
        let number = ValueAst::Number(NumberAst::new(Span::new(7, 9)));
        let boolean = ValueAst::Boolean(BooleanAst::new(true, Span::new(10, 14)));
        let null = ValueAst::Null(NullAst::new(Span::new(15, 19)));
        let identifier = ValueAst::Identifier(name(20, 23));

        assert!(string.is_scalar());
        assert!(number.is_scalar());
        assert!(boolean.is_scalar());
        assert!(null.is_scalar());
        assert!(identifier.is_scalar());

        assert_eq!(string.text(r#""John" 42 true null foo"#), Some(r#""John""#));
        assert_eq!(number.text(r#""John" 42 true null foo"#), Some("42"));
        assert_eq!(boolean.text(r#""John" 42 true null foo"#), Some("true"));
        assert_eq!(null.text(r#""John" 42 true null foo"#), Some("null"));
        assert_eq!(identifier.text(r#""John" 42 true null foo"#), Some("foo"));
    }

    #[test]
    fn creates_array_value() {
        let source = r#"["rust", 42, true, null]"#;
        let array = ArrayAst::new(
            Span::new(0, 1),
            vec![
                ValueAst::String(StringAst::new(Span::new(1, 7))),
                ValueAst::Number(NumberAst::new(Span::new(9, 11))),
                ValueAst::Boolean(BooleanAst::new(true, Span::new(13, 17))),
                ValueAst::Null(NullAst::new(Span::new(19, 23))),
            ],
            Span::new(23, 24),
            Span::new(0, 24),
        );

        assert_eq!(array.len(), 4);
        assert!(!array.is_empty());
        assert_eq!(
            array.value(0).and_then(|value| value.text(source)),
            Some(r#""rust""#)
        );
        assert_eq!(
            array.value(3).and_then(|value| value.text(source)),
            Some("null")
        );

        let value = ValueAst::Array(array);
        assert!(value.is_array());
        assert!(!value.is_scalar());
        assert_eq!(value.text(source), Some(source));
    }

    #[test]
    fn creates_object_value() {
        let source = r#"{name: "John", age: 42}"#;

        let name_field = ObjectFieldAst::new(
            ObjectKeyAst::Identifier(name(1, 5)),
            Span::new(5, 6),
            ValueAst::String(StringAst::new(Span::new(7, 13))),
            Span::new(1, 13),
        );
        let age_field = ObjectFieldAst::new(
            ObjectKeyAst::Identifier(name(15, 18)),
            Span::new(18, 19),
            ValueAst::Number(NumberAst::new(Span::new(20, 22))),
            Span::new(15, 22),
        );
        let object = ObjectAst::new(
            Span::new(0, 1),
            vec![name_field, age_field],
            Span::new(22, 23),
            Span::new(0, 23),
        );

        assert_eq!(object.len(), 2);
        assert_eq!(
            object.field(0).and_then(|field| field.key_text(source)),
            Some("name")
        );
        assert_eq!(
            object.field(1).and_then(|field| field.key_text(source)),
            Some("age")
        );

        let value = ValueAst::Object(object);
        assert!(value.is_object());
        assert_eq!(value.text(source), Some(source));
    }

    #[test]
    fn supports_quoted_object_keys() {
        let source = r#"{"display-name": "John"}"#;
        let key = ObjectKeyAst::String(StringAst::new(Span::new(1, 15)));
        let field = ObjectFieldAst::new(
            key,
            Span::new(15, 16),
            ValueAst::String(StringAst::new(Span::new(17, 23))),
            Span::new(1, 23),
        );
        let object = ObjectAst::new(
            Span::new(0, 1),
            vec![field],
            Span::new(23, 24),
            Span::new(0, 24),
        );

        assert!(key.is_string());
        assert_eq!(
            object.field(0).and_then(|field| field.key_text(source)),
            Some(r#""display-name""#)
        );
    }

    #[test]
    fn supports_nested_values() {
        let source = r#"{tags: ["rust", "db"]}"#;

        let array = ArrayAst::new(
            Span::new(7, 8),
            vec![
                ValueAst::String(StringAst::new(Span::new(8, 14))),
                ValueAst::String(StringAst::new(Span::new(16, 20))),
            ],
            Span::new(20, 21),
            Span::new(7, 21),
        );
        let field = ObjectFieldAst::new(
            ObjectKeyAst::Identifier(name(1, 5)),
            Span::new(5, 6),
            ValueAst::Array(array),
            Span::new(1, 21),
        );
        let object = ObjectAst::new(
            Span::new(0, 1),
            vec![field],
            Span::new(21, 22),
            Span::new(0, 22),
        );

        let value = ValueAst::Object(object);

        assert_eq!(value.text(source), Some(source));
        assert!(value.is_object());
    }

    #[test]
    fn allows_empty_array_and_object() {
        let array = ArrayAst::new(
            Span::new(0, 1),
            Vec::new(),
            Span::new(1, 2),
            Span::new(0, 2),
        );
        let object = ObjectAst::new(
            Span::new(3, 4),
            Vec::new(),
            Span::new(4, 5),
            Span::new(3, 5),
        );

        assert!(array.is_empty());
        assert!(object.is_empty());
    }

    #[test]
    fn creates_from_source() {
        let source = users_source();

        assert_eq!(source.keyword(), SourceKeyword::From);
        assert_eq!(source.keyword_span(), Span::new(0, 4));
        assert_eq!(source.collection(), name(5, 10));
        assert_eq!(source.span(), Span::new(0, 10));
        assert_eq!(source.collection_name("from users"), Some("users"));
        assert!(!source.has_alias());
        assert_eq!(source.alias(), None);
    }

    #[test]
    fn creates_on_source() {
        let source = SourceAst::new(
            SourceKeyword::On,
            Span::new(0, 2),
            name(3, 8),
            Span::new(0, 8),
        );

        assert_eq!(source.keyword(), SourceKeyword::On);
        assert_eq!(source.collection_name("on users"), Some("users"));
    }

    #[test]
    fn creates_source_with_alias() {
        let source_text = "on users as u";
        let alias = SourceAliasAst::new(Span::new(9, 11), name(12, 13), Span::new(9, 13));
        let source = SourceAst::with_alias(
            SourceKeyword::On,
            Span::new(0, 2),
            name(3, 8),
            alias,
            Span::new(0, 13),
        );

        assert!(source.has_alias());
        assert_eq!(source.alias(), Some(alias));
        assert_eq!(source.alias_name(source_text), Some("u"));
    }

    #[test]
    #[should_panic(expected = "source keyword span must not be empty")]
    fn rejects_empty_source_keyword() {
        let _ = SourceAst::new(
            SourceKeyword::From,
            Span::at(0),
            name(1, 6),
            Span::new(0, 6),
        );
    }

    #[test]
    #[should_panic(expected = "source span must contain collection span")]
    fn rejects_source_not_containing_collection() {
        let _ = SourceAst::new(
            SourceKeyword::From,
            Span::new(0, 4),
            name(5, 10),
            Span::new(0, 8),
        );
    }

    #[test]
    fn creates_stage_with_arguments() {
        let source = "from users | where age > 18";
        let stage = StageAst::new(
            Span::new(11, 12),
            name(13, 18),
            Span::new(19, 27),
            Span::new(11, 27),
        );

        assert_eq!(stage.pipe_span(), Span::new(11, 12));
        assert_eq!(stage.name_text(source), Some("where"));
        assert_eq!(stage.arguments_text(source), Some("age > 18"));
        assert_eq!(stage.header_span(), Span::new(11, 27));
        assert_eq!(stage.span(), Span::new(11, 27));
        assert!(stage.has_arguments());
        assert!(!stage.is_composite());
        assert_eq!(stage.subpipeline(), None);
    }

    #[test]
    fn creates_stage_without_arguments() {
        let source = "from users | inspect";
        let stage = StageAst::new(
            Span::new(11, 12),
            name(13, 20),
            Span::at(20),
            Span::new(11, 20),
        );

        assert_eq!(stage.name_text(source), Some("inspect"));
        assert_eq!(stage.arguments_text(source), Some(""));
        assert!(!stage.has_arguments());
    }

    #[test]
    fn creates_end_marker() {
        let source = "| end";
        let end = EndAst::new(Span::new(0, 1), Span::new(2, 5), Span::new(0, 5));

        assert_eq!(end.pipe_span(), Span::new(0, 1));
        assert_eq!(end.keyword_span(), Span::new(2, 5));
        assert_eq!(end.text(source), Some(source));
    }

    #[test]
    fn creates_composite_load_stage() {
        let source = "on users | load | with replace | chunk x | end";

        let with_stage = StageAst::new(
            Span::new(16, 17),
            name(18, 22),
            Span::new(23, 30),
            Span::new(16, 30),
        );
        let chunk_stage = StageAst::new(
            Span::new(31, 32),
            name(33, 38),
            Span::new(39, 40),
            Span::new(31, 40),
        );
        let end = EndAst::new(Span::new(41, 42), Span::new(43, 46), Span::new(41, 46));
        let subpipeline = SubPipelineAst::new(
            vec![with_stage.clone(), chunk_stage.clone()],
            end,
            Span::new(16, 46),
        );
        let load = StageAst::with_subpipeline(
            Span::new(9, 10),
            name(11, 15),
            Span::at(15),
            Span::new(9, 15),
            subpipeline,
            Span::new(9, 46),
        );

        assert!(load.is_composite());
        assert_eq!(load.name_text(source), Some("load"));

        let body = load.subpipeline().expect("load body");
        assert_eq!(body.stage_count(), 2);
        assert_eq!(body.stage(0), Some(&with_stage));
        assert_eq!(body.stage(1), Some(&chunk_stage));
        assert_eq!(body.end(), end);
    }

    #[test]
    fn supports_nested_composite_stages() {
        let source = "on a | union | lookup b | into x | end | end";

        let into_stage = StageAst::new(
            Span::new(24, 25),
            name(26, 30),
            Span::new(31, 32),
            Span::new(24, 32),
        );
        let lookup_end = EndAst::new(Span::new(33, 34), Span::new(35, 38), Span::new(33, 38));
        let lookup_body = SubPipelineAst::new(vec![into_stage], lookup_end, Span::new(24, 38));
        let lookup_stage = StageAst::with_subpipeline(
            Span::new(13, 14),
            name(15, 21),
            Span::new(22, 23),
            Span::new(13, 23),
            lookup_body,
            Span::new(13, 38),
        );
        let union_end = EndAst::new(Span::new(39, 40), Span::new(41, 44), Span::new(39, 44));
        let union_body = SubPipelineAst::new(vec![lookup_stage], union_end, Span::new(13, 44));
        let union_stage = StageAst::with_subpipeline(
            Span::new(5, 6),
            name(7, 12),
            Span::at(12),
            Span::new(5, 12),
            union_body,
            Span::new(5, 44),
        );

        let nested_lookup = union_stage
            .subpipeline()
            .and_then(|body| body.stage(0))
            .expect("nested lookup");

        assert_eq!(nested_lookup.name_text(source), Some("lookup"));
        assert!(nested_lookup.is_composite());
    }

    #[test]
    fn creates_empty_subpipeline() {
        let end = EndAst::new(Span::new(9, 10), Span::new(11, 14), Span::new(9, 14));
        let subpipeline = SubPipelineAst::empty(end);

        assert!(subpipeline.is_empty());
        assert_eq!(subpipeline.stage_count(), 0);
        assert_eq!(subpipeline.end(), end);
    }

    #[test]
    fn creates_source_only_pipeline() {
        let pipeline = PipelineAst::source_only(users_source());

        assert_eq!(pipeline.source(), users_source());
        assert_eq!(pipeline.stage_count(), 0);
        assert!(pipeline.is_source_only());
        assert_eq!(pipeline.span(), Span::new(0, 10));
        assert_eq!(pipeline.text("from users"), Some("from users"));
    }

    #[test]
    fn creates_pipeline_with_stages() {
        let source_text = "from users | where age > 18 | set active = true";
        let where_stage = StageAst::new(
            Span::new(11, 12),
            name(13, 18),
            Span::new(19, 27),
            Span::new(11, 27),
        );
        let set_stage = StageAst::new(
            Span::new(28, 29),
            name(30, 33),
            Span::new(34, 47),
            Span::new(28, 47),
        );
        let pipeline = PipelineAst::new(
            users_source(),
            vec![where_stage.clone(), set_stage.clone()],
            Span::new(0, 47),
        );

        assert_eq!(pipeline.stage_count(), 2);
        assert_eq!(pipeline.stage(0), Some(&where_stage));
        assert_eq!(pipeline.stage(1), Some(&set_stage));
        assert_eq!(pipeline.stage(2), None);
        assert_eq!(pipeline.text(source_text), Some(source_text));
    }

    #[test]
    fn iterates_over_stages_in_source_order() {
        let first = StageAst::new(
            Span::new(11, 12),
            name(13, 18),
            Span::new(19, 27),
            Span::new(11, 27),
        );
        let second = StageAst::new(
            Span::new(28, 29),
            name(30, 33),
            Span::new(34, 47),
            Span::new(28, 47),
        );
        let pipeline = PipelineAst::new(
            users_source(),
            vec![first.clone(), second.clone()],
            Span::new(0, 47),
        );

        assert_eq!(
            pipeline.iter_stages().cloned().collect::<Vec<_>>(),
            vec![first.clone(), second.clone()],
        );
        assert_eq!(
            (&pipeline).into_iter().cloned().collect::<Vec<_>>(),
            vec![first, second],
        );
    }

    #[test]
    fn consumes_pipeline_into_parts() {
        let stage = StageAst::new(
            Span::new(11, 12),
            name(13, 18),
            Span::new(19, 27),
            Span::new(11, 27),
        );
        let pipeline = PipelineAst::new(users_source(), vec![stage.clone()], Span::new(0, 27));
        let (source, stages) = pipeline.into_parts();

        assert_eq!(source, users_source());
        assert_eq!(stages, vec![stage]);
    }

    #[test]
    #[should_panic(expected = "pipeline stages must be ordered")]
    fn rejects_unordered_stages() {
        let first = StageAst::new(
            Span::new(20, 21),
            name(22, 27),
            Span::at(27),
            Span::new(20, 27),
        );
        let second = StageAst::new(
            Span::new(11, 12),
            name(13, 18),
            Span::at(18),
            Span::new(11, 18),
        );

        let _ = PipelineAst::new(users_source(), vec![first, second], Span::new(0, 27));
    }

    #[test]
    fn syntax_aliases_remain_distinguishable() {
        let from = SourceAst::new(
            SourceKeyword::From,
            Span::new(0, 4),
            name(5, 10),
            Span::new(0, 10),
        );
        let on = SourceAst::new(
            SourceKeyword::On,
            Span::new(0, 2),
            name(3, 8),
            Span::new(0, 8),
        );

        assert_ne!(from.keyword(), on.keyword());
    }

    #[test]
    fn different_literal_arguments_remain_distinguishable() {
        let first_source = "from users | where age > 18";
        let second_source = "from users | where age > 42";
        let first = StageAst::new(
            Span::new(11, 12),
            name(13, 18),
            Span::new(19, 27),
            Span::new(11, 27),
        );
        let second = StageAst::new(
            Span::new(11, 12),
            name(13, 18),
            Span::new(19, 27),
            Span::new(11, 27),
        );

        assert_eq!(first.arguments_text(first_source), Some("age > 18"));
        assert_eq!(second.arguments_text(second_source), Some("age > 42"));
    }

    #[test]
    fn leaf_ast_nodes_remain_compact() {
        assert!(std::mem::size_of::<NameAst>() <= 2 * std::mem::size_of::<usize>(),);
        assert!(std::mem::size_of::<StringAst>() <= 2 * std::mem::size_of::<usize>(),);
        assert!(std::mem::size_of::<NumberAst>() <= 2 * std::mem::size_of::<usize>(),);
        assert!(std::mem::size_of::<BooleanAst>() <= 3 * std::mem::size_of::<usize>(),);
        assert!(std::mem::size_of::<NullAst>() <= 2 * std::mem::size_of::<usize>(),);
        assert!(std::mem::size_of::<SourceAliasAst>() <= 6 * std::mem::size_of::<usize>(),);
        assert!(std::mem::size_of::<EndAst>() <= 6 * std::mem::size_of::<usize>(),);
    }
}
