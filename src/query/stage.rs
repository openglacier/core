//! Stage names and the canonical built-in stage catalogue.
//!
//! A stage's name, aliases, accepted forms and coarse execution contract are
//! declared exactly once here, like operations in `operation/definition.rs`.
//! The planner dispatches on [`Stage`], plan validation reads placement
//! from the catalogue, physical operators derive their properties
//! from it, and `query.stages` publishes it.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{borrow::Borrow, fmt, sync::Arc};

use super::execution_properties::{CardinalityEffect, Effect, Flow, Shape};

/// What a stage form accepts after its name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Arguments {
    Forbidden,
    Optional,
    Required,
}

impl Arguments {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Forbidden => "forbidden",
            Self::Optional => "optional",
            Self::Required => "required",
        }
    }
}

/// Where a stage may appear in its pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Placement {
    Anywhere,
    /// First stage of the pipeline (`load`).
    First,
    /// The only stage of the pipeline (`insert`).
    Only,
}

impl Placement {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Anywhere => "anywhere",
            Self::First => "first",
            Self::Only => "only",
        }
    }
}

/// One catalogue entry.
#[derive(Clone, Copy, Debug)]
pub struct StageEntry {
    pub stage: Stage,
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    /// Arguments of the simple form (`stage args`); `None`: no simple form.
    pub simple: Option<Arguments>,
    /// Arguments of the compound form (`stage args | … | end`); `None`: no compound form.
    pub compound: Option<Arguments>,
    pub placement: Placement,
    pub effect: Effect,
    pub flow: Flow,
    pub cardinality: CardinalityEffect,
    pub shape: Shape,
    pub summary: &'static str,
}

impl StageEntry {
    /// Nothing may follow the stage: it writes, or its output is not a row stream.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self.effect, Effect::Write) || !matches!(self.shape, Shape::Linear)
    }
}

macro_rules! stage_definitions {
    ($( $stage:ident => $name:literal [$($alias:literal),*], simple $simple:ident, compound $compound:ident, $placement:ident, $effect:ident, $flow:ident, $cardinality:ident, $shape:ident, $summary:literal; )*) => {
        /// A built-in stage.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum Stage { $($stage,)* }

        /// Every built-in stage, in documentation order.
        pub static STAGE_CATALOG: &[StageEntry] = &[$(
            StageEntry {
                stage: Stage::$stage,
                name: $name,
                aliases: &[$($alias),*],
                simple: stage_definitions!(@arguments $simple),
                compound: stage_definitions!(@arguments $compound),
                placement: Placement::$placement,
                effect: Effect::$effect,
                flow: Flow::$flow,
                cardinality: CardinalityEffect::$cardinality,
                shape: Shape::$shape,
                summary: $summary,
            },
        )*];
    };
    (@arguments None) => { None };
    (@arguments $arguments:ident) => { Some(Arguments::$arguments) };
}

stage_definitions! {
    Where => "where" [], simple Required, compound None, Anywhere, ReadOnly, Streaming, Reduce, Linear, "Keeps the rows whose predicate is true.";
    Select => "select" ["project"], simple Required, compound None, Anywhere, ReadOnly, Streaming, Preserve, Linear, "Keeps the listed fields and named expressions.";
    Derive => "derive" [], simple Required, compound None, Anywhere, ReadOnly, Streaming, Preserve, Linear, "Adds or replaces computed fields.";
    Rename => "rename" [], simple Required, compound None, Anywhere, ReadOnly, Streaming, Preserve, Linear, "Renames one field.";
    Drop => "drop" [], simple Required, compound None, Anywhere, ReadOnly, Streaming, Preserve, Linear, "Removes fields.";
    Root => "root" [], simple Required, compound None, Anywhere, ReadOnly, Streaming, Preserve, Linear, "Replaces each row by one of its object fields.";
    Near => "near" [], simple Required, compound None, Anywhere, ReadOnly, Streaming, Reduce, Linear, "Ranks rows by vector similarity.";
    Sort => "sort" [], simple Required, compound None, Anywhere, ReadOnly, GovernedBlocking, Preserve, Linear, "Orders rows by keys.";
    Limit => "limit" [], simple Required, compound None, Anywhere, ReadOnly, Streaming, Reduce, Linear, "Keeps the first n rows.";
    Skip => "skip" ["offset"], simple Required, compound None, Anywhere, ReadOnly, Streaming, Reduce, Linear, "Drops the first n rows.";
    First => "first" [], simple Optional, compound None, Anywhere, ReadOnly, Streaming, Reduce, Linear, "Keeps the first row.";
    Single => "single" [], simple Optional, compound None, Anywhere, ReadOnly, Streaming, Reduce, Linear, "Requires exactly one row.";
    Sample => "sample" [], simple Required, compound None, Anywhere, ReadOnly, GovernedBlocking, Reduce, Linear, "Keeps n rows drawn uniformly.";
    Unwind => "unwind" [], simple Required, compound None, Anywhere, ReadOnly, Streaming, Expand, Linear, "Emits one row per array item.";
    Distinct => "distinct" [], simple Optional, compound None, Anywhere, ReadOnly, GovernedBlocking, Reduce, Linear, "Keeps one row per distinct key.";
    Count => "count" [], simple Optional, compound None, Anywhere, ReadOnly, Streaming, Unknown, Scalar, "Returns the number of rows.";
    Group => "group" [], simple Required, compound Optional, Anywhere, ReadOnly, GovernedBlocking, Reduce, Linear, "Aggregates rows by keys.";
    Bucket => "bucket" [], simple Required, compound None, Anywhere, ReadOnly, GovernedBlocking, Reduce, Linear, "Counts rows per value range.";
    Fill => "fill" [], simple Required, compound None, Anywhere, ReadOnly, GovernedBlocking, Expand, Linear, "Adds the rows a grid of values misses in each partition.";
    Window => "window" [], simple None, compound Forbidden, Anywhere, ReadOnly, GovernedBlocking, Preserve, Linear, "Adds values over ordered partitions (lag, lead, running sum…).";
    Pivot => "pivot" [], simple None, compound Forbidden, Anywhere, ReadOnly, GovernedBlocking, Unknown, Linear, "Cross-tabulates rows by row and column keys.";
    Lookup => "lookup" ["join"], simple None, compound Required, Anywhere, ReadOnly, GovernedBlocking, Preserve, Linear, "Attaches the matching rows of another collection.";
    Union => "union" [], simple None, compound Forbidden, Anywhere, ReadOnly, GovernedBlocking, Expand, Linear, "Appends the rows of another pipeline.";
    Insert => "insert" [], simple Required, compound None, Only, Write, Specialized, Expand, Linear, "Inserts documents.";
    Set => "set" [], simple Required, compound None, Anywhere, Write, Specialized, Preserve, Linear, "Updates fields of the matching documents.";
    Delete => "delete" [], simple Forbidden, compound None, Anywhere, Write, Specialized, Reduce, Linear, "Deletes the matching documents.";
    Into => "into" ["out"], simple Required, compound None, Anywhere, Write, Specialized, Preserve, Linear, "Writes the rows into a collection.";
    Load => "load" [], simple Required, compound Forbidden, First, Write, Specialized, Preserve, Linear, "Imports or upserts documents.";
}

impl Stage {
    /// The stage named `name` or one of its aliases.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        STAGE_CATALOG
            .iter()
            .find(|entry| entry.name == name || entry.aliases.contains(&name))
            .map(|entry| entry.stage)
    }

    #[must_use]
    pub fn entry(self) -> &'static StageEntry {
        &STAGE_CATALOG[self as usize]
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        self.entry().name
    }
}

/// Validated stage name.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StageName(Arc<str>);

impl StageName {
    /// Creates a validated stage name.
    ///
    /// # Errors
    ///
    /// Returns an error when the name is empty or invalid.
    pub fn parse(name: impl AsRef<str>) -> Result<Self, StageNameError> {
        let name = name.as_ref();

        validate_stage_name(name)?;

        Ok(Self(Arc::from(name)))
    }

    /// Returns the stage name.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
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


/// Why a stage name is invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageNameError {
    EmptyName,
    InvalidNameStart { character: char },
    InvalidNameCharacter { index: usize, character: char },
}

impl fmt::Display for StageNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName => formatter.write_str("stage name must not be empty"),
            Self::InvalidNameStart { character } => write!(formatter, "stage name must start with an alphabetic character or '_', found {character:?}"),
            Self::InvalidNameCharacter { index, character } => write!(formatter, "invalid character {character:?} at byte index {index} in stage name"),
        }
    }
}

impl std::error::Error for StageNameError {}

fn validate_stage_name(name: &str) -> Result<(), StageNameError> {
    let mut characters = name.char_indices();

    let Some((_, first)) = characters.next() else {
        return Err(StageNameError::EmptyName);
    };

    if !is_stage_name_start(first) {
        return Err(StageNameError::InvalidNameStart { character: first });
    }

    for (index, character) in characters {
        if !is_stage_name_continue(character) {
            return Err(StageNameError::InvalidNameCharacter { index, character });
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

    #[test] fn validates_stage_names() { assert!(StageName::parse("where").is_ok()); assert!(StageName::parse("_internal").is_ok()); assert!(StageName::parse("stage2").is_ok()); assert!(StageName::parse("étape").is_ok()); }
    #[test] fn rejects_empty_stage_name() { let error = StageName::parse("").expect_err("empty name must fail"); assert_eq!(error, StageNameError::EmptyName); }
    #[test] fn rejects_invalid_stage_name_start() { let error = StageName::parse("2stage").expect_err("name must fail"); assert_eq!(error, StageNameError::InvalidNameStart { character: '2' }); }
    #[test] fn rejects_invalid_stage_name_character() { let error = StageName::parse("my-stage").expect_err("name must fail"); assert_eq!(error, StageNameError::InvalidNameCharacter { index: 2, character: '-' }); }
    #[test] fn stage_name_is_case_sensitive() { assert_ne!( StageName::parse("where").unwrap(), StageName::parse("Where").unwrap(), ); }

    #[test] fn catalogue_is_indexed_by_stage_and_names_are_unique() { let mut names = Vec::new(); for (index, entry) in STAGE_CATALOG.iter().enumerate() { assert_eq!(entry.stage as usize, index); assert!(entry.simple.is_some() || entry.compound.is_some(), "{}", entry.name); names.push(entry.name); names.extend(entry.aliases); } let count = names.len(); names.sort_unstable(); names.dedup(); assert_eq!(names.len(), count); assert_eq!(Stage::parse("project"), Some(Stage::Select)); assert_eq!(Stage::parse("frobnicate"), None); }

    #[test] fn every_stage_is_documented_in_the_language_reference() { let headings = include_str!("README.md").lines().filter(|line| line.starts_with("#### ")).collect::<Vec<_>>(); for name in STAGE_CATALOG.iter().flat_map(|entry| std::iter::once(&entry.name).chain(entry.aliases)) { assert!(headings.iter().any(|heading| heading.contains(&format!("`{name}`"))), "stage {name} has no heading in the language reference"); } }
}
