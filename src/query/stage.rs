//! Pipeline stage model.

use std::{borrow::Borrow, collections::BTreeMap, fmt, sync::Arc};

use super::StageAst;

/// Result returned by stage-registry operations.
pub type StageResult<T> = std::result::Result<T, StageError>;

/// Validated stage name.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StageName(Arc<str>);

impl StageName {
    /// Creates a validated stage name.
    ///
    /// # Errors
    ///
    /// Returns an error when the name is empty or invalid.
    pub fn parse(name: impl AsRef<str>) -> StageResult<Self> {
        let name = name.as_ref();

        validate_stage_name(name)?;

        Ok(Self(Arc::from(name)))
    }

    fn from_static(name: &'static str) -> Self {
        debug_assert!(validate_stage_name(name).is_ok());

        Self(Arc::from(name))
    }

    /// Returns the stage name.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    /// Returns the UTF-8 byte length.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the name is empty.
    ///
    /// A valid stage name is never empty.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<str> for StageName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for StageName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for StageName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StageName")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for StageName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for StageName {
    type Error = StageError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<String> for StageName {
    type Error = StageError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

/// Semantic category of a registered stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StageKind {
    /// Filters documents according to a predicate.
    Where,

    /// Mutates fields on matching documents.
    Set,

    /// Loads or ingests data.
    Load,

    /// Extension stage.
    Custom,
}

impl StageKind {
    /// Returns whether this kind may mutate stored data.
    #[must_use]
    pub const fn is_mutating(self) -> bool {
        matches!(self, Self::Set | Self::Load)
    }

    /// Returns whether this kind is read-only.
    #[must_use]
    pub const fn is_read_only(self) -> bool {
        !self.is_mutating()
    }

    /// Returns a stable textual representation.
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Where => "where",
            Self::Set => "set",
            Self::Load => "load",
            Self::Custom => "custom",
        }
    }
}

impl fmt::Display for StageKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Argument-presence policy for a stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StageArgumentPolicy {
    /// No arguments are accepted.
    Forbidden,

    /// Arguments are accepted but optional.
    Optional,

    /// A non-empty argument region is required.
    Required,
}

impl StageArgumentPolicy {
    /// Returns whether the supplied argument presence satisfies the policy.
    #[must_use]
    pub const fn accepts(self, has_arguments: bool) -> bool {
        match self {
            Self::Forbidden => !has_arguments,
            Self::Optional => true,
            Self::Required => has_arguments,
        }
    }

    /// Returns whether arguments are required.
    #[must_use]
    pub const fn requires_arguments(self) -> bool {
        matches!(self, Self::Required)
    }

    /// Returns whether arguments are forbidden.
    #[must_use]
    pub const fn forbids_arguments(self) -> bool {
        matches!(self, Self::Forbidden)
    }
}

/// Metadata associated with a registered stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageDefinition {
    name: StageName,
    kind: StageKind,
    argument_policy: StageArgumentPolicy,
}

impl StageDefinition {
    /// Creates a stage definition.
    ///
    /// # Errors
    ///
    /// Returns an error when the stage name is invalid.
    #[inline]
    pub fn new(
        name: impl AsRef<str>,
        kind: StageKind,
        argument_policy: StageArgumentPolicy,
    ) -> StageResult<Self> {
        Ok(Self {
            name: StageName::parse(name)?,
            kind,
            argument_policy,
        })
    }

    /// Creates the native `where` stage definition.
    #[must_use]
    pub fn native_where() -> Self {
        Self {
            name: StageName::from_static("where"),
            kind: StageKind::Where,
            argument_policy: StageArgumentPolicy::Required,
        }
    }

    /// Creates the native `set` stage definition.
    #[must_use]
    pub fn native_set() -> Self {
        Self {
            name: StageName::from_static("set"),
            kind: StageKind::Set,
            argument_policy: StageArgumentPolicy::Required,
        }
    }

    /// Creates the native `load` stage definition.
    #[must_use]
    pub fn native_load() -> Self {
        Self {
            name: StageName::from_static("load"),
            kind: StageKind::Load,
            argument_policy: StageArgumentPolicy::Required,
        }
    }

    /// Returns the registered name.
    #[must_use]
    #[inline]
    pub const fn name(&self) -> &StageName {
        &self.name
    }

    /// Returns the semantic kind.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> StageKind {
        self.kind
    }

    /// Returns the argument policy.
    #[must_use]
    pub const fn argument_policy(&self) -> StageArgumentPolicy {
        self.argument_policy
    }

    /// Returns whether the stage may mutate data.
    #[must_use]
    pub const fn is_mutating(&self) -> bool {
        self.kind.is_mutating()
    }

    /// Returns whether the stage is read-only.
    #[must_use]
    pub const fn is_read_only(&self) -> bool {
        self.kind.is_read_only()
    }
}

/// Registry of known query stages.
///
/// A `BTreeMap` preserves deterministic iteration order.
#[derive(Clone, Debug, Default)]
pub struct StageRegistry {
    definitions: BTreeMap<StageName, StageDefinition>,
}

impl StageRegistry {
    /// Creates an empty registry.
    #[must_use]
    #[inline]
    pub const fn new() -> Self {
        Self {
            definitions: BTreeMap::new(),
        }
    }

    /// Creates a registry containing native OG stages.
    #[must_use]
    pub fn with_builtins() -> Self {
        let mut registry = Self::new();

        registry.insert_builtin(StageDefinition::native_where());
        registry.insert_builtin(StageDefinition::native_set());
        registry.insert_builtin(StageDefinition::native_load());

        registry
    }

    /// Returns the number of registered stages.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    /// Returns whether the registry is empty.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }

    /// Registers a new stage definition.
    ///
    /// # Errors
    ///
    /// Returns an error when the name is already registered.
    pub fn register(&mut self, definition: StageDefinition) -> StageResult<()> {
        let name = definition.name().clone();

        if self.definitions.contains_key(name.as_str()) {
            return Err(StageError::duplicate_stage(name));
        }

        self.definitions.insert(name, definition);

        Ok(())
    }

    /// Registers a custom stage.
    ///
    /// # Errors
    ///
    /// Returns an error when the name is invalid or duplicated.
    pub fn register_custom(
        &mut self,
        name: impl AsRef<str>,
        argument_policy: StageArgumentPolicy,
    ) -> StageResult<()> {
        let definition = StageDefinition::new(name, StageKind::Custom, argument_policy)?;

        self.register(definition)
    }

    /// Inserts or replaces a definition.
    pub fn replace(&mut self, definition: StageDefinition) -> Option<StageDefinition> {
        let name = definition.name().clone();

        self.definitions.insert(name, definition)
    }

    /// Removes a stage by name.
    pub fn remove(&mut self, name: &str) -> Option<StageDefinition> {
        self.definitions.remove(name)
    }

    /// Returns a definition by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&StageDefinition> {
        self.definitions.get(name)
    }

    /// Returns whether a stage is registered.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.definitions.contains_key(name)
    }

    /// Iterates over definitions in deterministic name order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &StageDefinition> {
        self.definitions.values()
    }

    /// Resolves and structurally validates a parsed stage.
    ///
    /// This validates:
    ///
    /// - AST source spans;
    /// - stage registration;
    /// - argument presence policy.
    ///
    /// It does not parse stage-specific arguments.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid span, unknown stage, missing arguments,
    /// or forbidden arguments.
    pub fn resolve<'registry, 'ast, 'source>(
        &'registry self,
        stage: &'ast StageAst,
        source: &'source str,
    ) -> StageResult<ResolvedStage<'registry, 'ast, 'source>> {
        let name = stage
            .name_text(source)
            .ok_or_else(StageError::invalid_source_span)?;

        let definition = self
            .get(name)
            .ok_or_else(|| StageError::unknown_stage(name))?;

        let arguments = stage
            .arguments_text(source)
            .ok_or_else(StageError::invalid_source_span)?;

        let has_arguments = !arguments.is_empty();

        match definition.argument_policy() {
            StageArgumentPolicy::Required if !has_arguments => {
                return Err(StageError::missing_arguments(definition.name().clone()));
            }

            StageArgumentPolicy::Forbidden if has_arguments => {
                return Err(StageError::unexpected_arguments(definition.name().clone()));
            }

            StageArgumentPolicy::Forbidden
            | StageArgumentPolicy::Optional
            | StageArgumentPolicy::Required => {}
        }

        Ok(ResolvedStage {
            definition,
            ast: stage,
            source,
            arguments,
        })
    }

    fn insert_builtin(&mut self, definition: StageDefinition) {
        let previous = self.replace(definition);

        debug_assert!(previous.is_none(), "native stage names must be unique",);
    }
}

impl FromIterator<StageDefinition> for StageResult<StageRegistry> {
    fn from_iter<T: IntoIterator<Item = StageDefinition>>(definitions: T) -> Self {
        let mut registry = StageRegistry::new();

        for definition in definitions {
            registry.register(definition)?;
        }

        Ok(registry)
    }
}

/// Parsed stage associated with its registry definition.
#[derive(Clone, Copy, Debug)]
pub struct ResolvedStage<'registry, 'ast, 'source> {
    definition: &'registry StageDefinition,
    ast: &'ast StageAst,
    source: &'source str,
    arguments: &'source str,
}

impl<'registry, 'ast, 'source> ResolvedStage<'registry, 'ast, 'source> {
    /// Returns the registered definition.
    #[must_use]
    pub const fn definition(&self) -> &'registry StageDefinition {
        self.definition
    }

    /// Returns the original AST node.
    #[must_use]
    pub const fn ast(&self) -> &'ast StageAst {
        self.ast
    }

    /// Returns the original source.
    #[must_use]
    #[inline]
    pub const fn source(&self) -> &'source str {
        self.source
    }

    /// Returns the validated stage name.
    #[must_use]
    #[inline]
    pub const fn name(&self) -> &StageName {
        self.definition.name()
    }

    /// Returns the stage kind.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> StageKind {
        self.definition.kind()
    }

    /// Returns the exact raw argument text.
    #[must_use]
    #[inline]
    pub const fn arguments(&self) -> &'source str {
        self.arguments
    }

    /// Returns whether arguments are present.
    #[must_use]
    pub const fn has_arguments(&self) -> bool {
        !self.arguments.is_empty()
    }

    /// Returns whether the stage may mutate data.
    #[must_use]
    pub const fn is_mutating(&self) -> bool {
        self.definition.is_mutating()
    }

    /// Returns whether the stage is read-only.
    #[must_use]
    pub const fn is_read_only(&self) -> bool {
        self.definition.is_read_only()
    }
}

/// Stage registration or resolution failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageError {
    kind: StageErrorKind,
}

impl StageError {
    /// Creates an error from its kind.
    #[must_use]
    #[inline]
    pub const fn new(kind: StageErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the error kind.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> &StageErrorKind {
        &self.kind
    }

    fn empty_name() -> Self {
        Self::new(StageErrorKind::EmptyName)
    }

    fn invalid_start(character: char) -> Self {
        Self::new(StageErrorKind::InvalidNameStart { character })
    }

    fn invalid_character(index: usize, character: char) -> Self {
        Self::new(StageErrorKind::InvalidNameCharacter { index, character })
    }

    fn duplicate_stage(name: StageName) -> Self {
        Self::new(StageErrorKind::DuplicateStage { name })
    }

    fn unknown_stage(name: &str) -> Self {
        Self::new(StageErrorKind::UnknownStage {
            name: Arc::from(name),
        })
    }

    fn missing_arguments(name: StageName) -> Self {
        Self::new(StageErrorKind::MissingArguments { name })
    }

    fn unexpected_arguments(name: StageName) -> Self {
        Self::new(StageErrorKind::UnexpectedArguments { name })
    }

    fn invalid_source_span() -> Self {
        Self::new(StageErrorKind::InvalidSourceSpan)
    }
}

impl fmt::Display for StageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            StageErrorKind::EmptyName => formatter.write_str("stage name must not be empty"),

            StageErrorKind::InvalidNameStart { character } => {
                write!(
                    formatter,
                    "stage name must start with an alphabetic character or '_', found {character:?}",
                )
            }

            StageErrorKind::InvalidNameCharacter { index, character } => {
                write!(
                    formatter,
                    "invalid character {character:?} at byte index {index} in stage name",
                )
            }

            StageErrorKind::DuplicateStage { name } => {
                write!(formatter, "stage {name:?} is already registered",)
            }

            StageErrorKind::UnknownStage { name } => {
                write!(formatter, "unknown query stage {name:?}",)
            }

            StageErrorKind::MissingArguments { name } => {
                write!(formatter, "stage {name:?} requires arguments",)
            }

            StageErrorKind::UnexpectedArguments { name } => {
                write!(formatter, "stage {name:?} does not accept arguments",)
            }

            StageErrorKind::InvalidSourceSpan => {
                formatter.write_str("stage AST contains a span outside the query source")
            }
        }
    }
}

impl std::error::Error for StageError {}

/// Detailed stage error category.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StageErrorKind {
    /// Empty stage name.
    EmptyName,

    /// Invalid first character.
    InvalidNameStart { character: char },

    /// Invalid later character.
    InvalidNameCharacter { index: usize, character: char },

    /// Duplicate registered name.
    DuplicateStage { name: StageName },

    /// Unknown stage.
    UnknownStage { name: Arc<str> },

    /// Required arguments are missing.
    MissingArguments { name: StageName },

    /// Arguments were supplied to a stage that forbids them.
    UnexpectedArguments { name: StageName },

    /// AST span is outside the query source.
    InvalidSourceSpan,
}

fn validate_stage_name(name: &str) -> StageResult<()> {
    let mut characters = name.char_indices();

    let Some((_, first)) = characters.next() else {
        return Err(StageError::empty_name());
    };

    if !is_stage_name_start(first) {
        return Err(StageError::invalid_start(first));
    }

    for (index, character) in characters {
        if !is_stage_name_continue(character) {
            return Err(StageError::invalid_character(index, character));
        }
    }

    Ok(())
}

fn is_stage_name_start(character: char) -> bool {
    character == '_' || character.is_alphabetic()
}

fn is_stage_name_continue(character: char) -> bool {
    character == '_' || character.is_alphabetic() || character.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parse;

    #[test]
    fn validates_stage_names() {
        assert!(StageName::parse("where").is_ok());
        assert!(StageName::parse("_internal").is_ok());
        assert!(StageName::parse("stage2").is_ok());
        assert!(StageName::parse("étape").is_ok());
    }

    #[test]
    fn rejects_empty_stage_name() {
        let error = StageName::parse("").expect_err("empty name must fail");

        assert_eq!(error.kind(), &StageErrorKind::EmptyName);
    }

    #[test]
    fn rejects_invalid_stage_name_start() {
        let error = StageName::parse("2stage").expect_err("name must fail");

        assert_eq!(
            error.kind(),
            &StageErrorKind::InvalidNameStart { character: '2' },
        );
    }

    #[test]
    fn rejects_invalid_stage_name_character() {
        let error = StageName::parse("my-stage").expect_err("name must fail");

        assert_eq!(
            error.kind(),
            &StageErrorKind::InvalidNameCharacter {
                index: 2,
                character: '-',
            },
        );
    }

    #[test]
    fn stage_name_is_case_sensitive() {
        assert_ne!(
            StageName::parse("where").unwrap(),
            StageName::parse("Where").unwrap(),
        );
    }

    #[test]
    fn creates_builtin_registry() {
        let registry = StageRegistry::with_builtins();

        assert_eq!(registry.len(), 3);
        assert!(registry.contains("where"));
        assert!(registry.contains("set"));
        assert!(registry.contains("load"));
    }

    #[test]
    fn builtin_registry_is_case_sensitive() {
        let registry = StageRegistry::with_builtins();

        assert!(registry.contains("where"));
        assert!(!registry.contains("Where"));
    }

    #[test]
    fn registers_custom_stage() {
        let mut registry = StageRegistry::new();

        registry
            .register_custom("inspect", StageArgumentPolicy::Optional)
            .unwrap();

        let definition = registry.get("inspect").expect("stage must be registered");

        assert_eq!(definition.kind(), StageKind::Custom);
        assert_eq!(definition.argument_policy(), StageArgumentPolicy::Optional,);
    }

    #[test]
    fn rejects_duplicate_stage() {
        let mut registry = StageRegistry::new();

        registry
            .register_custom("inspect", StageArgumentPolicy::Optional)
            .unwrap();

        let error = registry
            .register_custom("inspect", StageArgumentPolicy::Required)
            .expect_err("duplicate stage must fail");

        assert_eq!(
            error.kind(),
            &StageErrorKind::DuplicateStage {
                name: StageName::parse("inspect").unwrap(),
            },
        );
    }

    #[test]
    fn replaces_stage_definition() {
        let mut registry = StageRegistry::new();

        registry
            .register_custom("inspect", StageArgumentPolicy::Optional)
            .unwrap();

        let replacement =
            StageDefinition::new("inspect", StageKind::Custom, StageArgumentPolicy::Required)
                .unwrap();

        let previous = registry
            .replace(replacement)
            .expect("old definition must be returned");

        assert_eq!(previous.argument_policy(), StageArgumentPolicy::Optional,);

        assert_eq!(
            registry.get("inspect").unwrap().argument_policy(),
            StageArgumentPolicy::Required,
        );
    }

    #[test]
    fn removes_stage_definition() {
        let mut registry = StageRegistry::with_builtins();

        let removed = registry.remove("load").expect("load must exist");

        assert_eq!(removed.kind(), StageKind::Load);
        assert!(!registry.contains("load"));
    }

    #[test]
    fn registry_iteration_is_deterministic() {
        let mut registry = StageRegistry::new();

        registry
            .register_custom("zeta", StageArgumentPolicy::Optional)
            .unwrap();

        registry
            .register_custom("alpha", StageArgumentPolicy::Optional)
            .unwrap();

        registry
            .register_custom("middle", StageArgumentPolicy::Optional)
            .unwrap();

        let names = registry
            .iter()
            .map(|definition| definition.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["alpha", "middle", "zeta"]);
    }

    #[test]
    fn resolves_where_stage() {
        let source = "from users | where age >= 18";

        let pipeline = parse(source).unwrap();
        let stage = pipeline.stage(0).unwrap();
        let registry = StageRegistry::with_builtins();

        let resolved = registry
            .resolve(&stage, source)
            .expect("where must resolve");

        assert_eq!(resolved.name().as_str(), "where");
        assert_eq!(resolved.kind(), StageKind::Where);
        assert_eq!(resolved.arguments(), "age >= 18");
        assert!(resolved.is_read_only());
        assert!(!resolved.is_mutating());
    }

    #[test]
    fn resolves_set_stage() {
        let source = "from users | set enabled = true";

        let pipeline = parse(source).unwrap();
        let stage = pipeline.stage(0).unwrap();
        let registry = StageRegistry::with_builtins();

        let resolved = registry.resolve(&stage, source).expect("set must resolve");

        assert_eq!(resolved.kind(), StageKind::Set);
        assert_eq!(resolved.arguments(), "enabled = true");
        assert!(resolved.is_mutating());
        assert!(!resolved.is_read_only());
    }

    #[test]
    fn resolves_custom_stage() {
        let source = "from users | inspect verbose";

        let pipeline = parse(source).unwrap();
        let stage = pipeline.stage(0).unwrap();

        let mut registry = StageRegistry::with_builtins();

        registry
            .register_custom("inspect", StageArgumentPolicy::Optional)
            .unwrap();

        let resolved = registry
            .resolve(&stage, source)
            .expect("custom stage must resolve");

        assert_eq!(resolved.kind(), StageKind::Custom);
        assert_eq!(resolved.arguments(), "verbose");
    }

    #[test]
    fn rejects_unknown_stage() {
        let source = "from users | inspect verbose";

        let pipeline = parse(source).unwrap();
        let stage = pipeline.stage(0).unwrap();
        let registry = StageRegistry::with_builtins();

        let error = registry
            .resolve(&stage, source)
            .expect_err("unknown stage must fail");

        assert_eq!(
            error.kind(),
            &StageErrorKind::UnknownStage {
                name: Arc::from("inspect"),
            },
        );
    }

    #[test]
    fn rejects_missing_required_arguments() {
        let source = "from users | where";

        let pipeline = parse(source).unwrap();
        let stage = pipeline.stage(0).unwrap();
        let registry = StageRegistry::with_builtins();

        let error = registry
            .resolve(&stage, source)
            .expect_err("missing arguments must fail");

        assert_eq!(
            error.kind(),
            &StageErrorKind::MissingArguments {
                name: StageName::parse("where").unwrap(),
            },
        );
    }

    #[test]
    fn accepts_optional_arguments_when_absent() {
        let source = "from users | inspect";

        let pipeline = parse(source).unwrap();
        let stage = pipeline.stage(0).unwrap();

        let mut registry = StageRegistry::new();

        registry
            .register_custom("inspect", StageArgumentPolicy::Optional)
            .unwrap();

        let resolved = registry
            .resolve(&stage, source)
            .expect("optional arguments may be absent");

        assert_eq!(resolved.arguments(), "");
        assert!(!resolved.has_arguments());
    }

    #[test]
    fn rejects_arguments_when_forbidden() {
        let source = "from users | commit now";

        let pipeline = parse(source).unwrap();
        let stage = pipeline.stage(0).unwrap();

        let mut registry = StageRegistry::new();

        registry
            .register_custom("commit", StageArgumentPolicy::Forbidden)
            .unwrap();

        let error = registry
            .resolve(&stage, source)
            .expect_err("arguments must be rejected");

        assert_eq!(
            error.kind(),
            &StageErrorKind::UnexpectedArguments {
                name: StageName::parse("commit").unwrap(),
            },
        );
    }

    #[test]
    fn accepts_forbidden_policy_without_arguments() {
        let source = "from users | commit";

        let pipeline = parse(source).unwrap();
        let stage = pipeline.stage(0).unwrap();

        let mut registry = StageRegistry::new();

        registry
            .register_custom("commit", StageArgumentPolicy::Forbidden)
            .unwrap();

        let resolved = registry
            .resolve(&stage, source)
            .expect("argumentless stage must resolve");

        assert_eq!(resolved.arguments(), "");
    }

    #[test]
    fn native_stage_metadata_is_correct() {
        let where_stage = StageDefinition::native_where();
        let set_stage = StageDefinition::native_set();
        let load_stage = StageDefinition::native_load();

        assert!(where_stage.is_read_only());
        assert!(!where_stage.is_mutating());

        assert!(set_stage.is_mutating());
        assert!(load_stage.is_mutating());
    }

    #[test]
    fn argument_policy_accepts_expected_values() {
        assert!(StageArgumentPolicy::Forbidden.accepts(false),);

        assert!(!StageArgumentPolicy::Forbidden.accepts(true),);

        assert!(StageArgumentPolicy::Optional.accepts(false),);

        assert!(StageArgumentPolicy::Optional.accepts(true),);

        assert!(!StageArgumentPolicy::Required.accepts(false),);

        assert!(StageArgumentPolicy::Required.accepts(true),);
    }

    #[test]
    fn collects_registry_from_definitions() {
        let definitions = vec![
            StageDefinition::new("first", StageKind::Custom, StageArgumentPolicy::Optional)
                .unwrap(),
            StageDefinition::new("second", StageKind::Custom, StageArgumentPolicy::Required)
                .unwrap(),
        ];

        let registry: StageResult<StageRegistry> = definitions.into_iter().collect();

        let registry = registry.unwrap();

        assert!(registry.contains("first"));
        assert!(registry.contains("second"));
    }

    #[test]
    fn collecting_duplicate_definitions_fails() {
        let definitions = vec![
            StageDefinition::new("inspect", StageKind::Custom, StageArgumentPolicy::Optional)
                .unwrap(),
            StageDefinition::new("inspect", StageKind::Custom, StageArgumentPolicy::Required)
                .unwrap(),
        ];

        let registry: StageResult<StageRegistry> = definitions.into_iter().collect();

        assert!(matches!(
            registry.unwrap_err().kind(),
            StageErrorKind::DuplicateStage { .. },
        ));
    }
}
