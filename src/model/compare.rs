//! Capability-aware value comparison.

use super::{
    coerce_value_pair_to_numbers, CoercedNumber, CoercionFailure, CoercionPolicy, Number, Value,
};
use std::cmp::Ordering;
use std::fmt;

/// Résultat d'une comparaison entre deux valeurs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Comparison {
    Less,
    Equal,
    Greater,
}

impl Comparison {
    /// Construit un résultat depuis [`Ordering`].
    #[must_use]
    pub const fn from_ordering(ordering: Ordering) -> Self {
        match ordering {
            Ordering::Less => Self::Less,
            Ordering::Equal => Self::Equal,
            Ordering::Greater => Self::Greater,
        }
    }

    /// Convertit le résultat en [`Ordering`].
    #[must_use]
    pub const fn into_ordering(self) -> Ordering {
        match self {
            Self::Less => Ordering::Less,
            Self::Equal => Ordering::Equal,
            Self::Greater => Ordering::Greater,
        }
    }

    /// Indique si la valeur gauche est strictement inférieure.
    #[must_use]
    pub const fn is_less(self) -> bool {
        matches!(self, Self::Less)
    }

    /// Indique si les valeurs sont égales.
    #[must_use]
    pub const fn is_equal(self) -> bool {
        matches!(self, Self::Equal)
    }

    /// Indique si la valeur gauche est strictement supérieure.
    #[must_use]
    pub const fn is_greater(self) -> bool {
        matches!(self, Self::Greater)
    }

    /// Indique si la valeur gauche est inférieure ou égale.
    #[must_use]
    pub const fn is_less_or_equal(self) -> bool {
        matches!(self, Self::Less | Self::Equal)
    }

    /// Indique si la valeur gauche est supérieure ou égale.
    #[must_use]
    pub const fn is_greater_or_equal(self) -> bool {
        matches!(self, Self::Equal | Self::Greater)
    }

    /// Inverse le sens de la comparaison.
    #[must_use]
    pub const fn reverse(self) -> Self {
        match self {
            Self::Less => Self::Greater,
            Self::Equal => Self::Equal,
            Self::Greater => Self::Less,
        }
    }
}

impl fmt::Display for Comparison {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Less => "less",
            Self::Equal => "equal",
            Self::Greater => "greater",
        })
    }
}

/// Échec d'une comparaison opérationnelle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CompareFailure {
    /// Les valeurs ne possèdent pas de relation d'ordre commune.
    IncompatibleValues,

    /// Une coercition nécessaire a échoué.
    Coercion(CoercionFailure),
}

impl CompareFailure {
    /// Indique si l'échec vient de valeurs sans relation d'ordre commune.
    #[must_use]
    pub const fn is_incompatible_values(self) -> bool {
        matches!(self, Self::IncompatibleValues)
    }

    /// Retourne l'échec de coercition sous-jacent, lorsqu'il existe.
    #[must_use]
    pub const fn coercion_failure(self) -> Option<CoercionFailure> {
        match self {
            Self::Coercion(failure) => Some(failure),
            Self::IncompatibleValues => None,
        }
    }
}

impl fmt::Display for CompareFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompatibleValues => formatter.write_str("values are not comparable"),
            Self::Coercion(failure) => {
                write!(formatter, "value coercion failed: {failure}")
            }
        }
    }
}

impl std::error::Error for CompareFailure {}

impl From<CoercionFailure> for CompareFailure {
    fn from(failure: CoercionFailure) -> Self {
        Self::Coercion(failure)
    }
}

/// Résultat d'une comparaison.
pub type CompareResult<T> = std::result::Result<T, CompareFailure>;

/// Compare deux valeurs selon une politique de coercition.
///
/// Les règles initiales sont :
///
/// - `null` est comparable uniquement à `null` ;
/// - les booléens sont ordonnés `false < true` ;
/// - les chaînes sont comparées lexicographiquement ;
/// - les nombres utilisent la couche de coercition ;
/// - une chaîne peut rejoindre une comparaison numérique uniquement avec
///   [`CoercionPolicy::Implicit`] ;
/// - les tableaux et objets ne possèdent pas encore d'ordre.
pub fn compare(left: &Value, right: &Value, policy: CoercionPolicy) -> CompareResult<Comparison> {
    match (left, right) {
        (Value::Null, Value::Null) => Ok(Comparison::Equal),

        (Value::Bool(left), Value::Bool(right)) => Ok(Comparison::from_ordering(left.cmp(right))),

        (Value::String(left), Value::String(right)) => {
            Ok(Comparison::from_ordering(left.cmp(right)))
        }

        (Value::Number(_), Value::Number(_) | Value::String(_))
        | (Value::String(_), Value::Number(_)) => compare_numeric_values(left, right, policy),

        _ => Err(CompareFailure::IncompatibleValues),
    }
}

/// Teste l'égalité opérationnelle.
///
/// Contrairement à [`Value::eq`], cette fonction peut appliquer une coercition.
pub fn equals(left: &Value, right: &Value, policy: CoercionPolicy) -> CompareResult<bool> {
    match compare(left, right, policy) {
        Ok(comparison) => Ok(comparison.is_equal()),

        Err(CompareFailure::IncompatibleValues) => Ok(left == right),

        Err(failure) => Err(failure),
    }
}

/// Teste l'inégalité opérationnelle.
pub fn not_equals(left: &Value, right: &Value, policy: CoercionPolicy) -> CompareResult<bool> {
    equals(left, right, policy).map(|equal| !equal)
}

/// Teste l'égalité physique sans coercition.
///
/// Cette fonction rend explicite la différence entre l'identité de représentation
/// de [`Value`] et l'égalité opérationnelle fournie par [`equals`].
#[must_use]
pub fn physically_equals(left: &Value, right: &Value) -> bool {
    left == right
}

/// Teste si la valeur gauche est strictement inférieure.
pub fn less_than(left: &Value, right: &Value, policy: CoercionPolicy) -> CompareResult<bool> {
    compare(left, right, policy).map(Comparison::is_less)
}

/// Teste si la valeur gauche est inférieure ou égale.
pub fn less_than_or_equal(
    left: &Value,
    right: &Value,
    policy: CoercionPolicy,
) -> CompareResult<bool> {
    compare(left, right, policy).map(Comparison::is_less_or_equal)
}

/// Teste si la valeur gauche est strictement supérieure.
pub fn greater_than(left: &Value, right: &Value, policy: CoercionPolicy) -> CompareResult<bool> {
    compare(left, right, policy).map(Comparison::is_greater)
}

/// Teste si la valeur gauche est supérieure ou égale.
pub fn greater_than_or_equal(
    left: &Value,
    right: &Value,
    policy: CoercionPolicy,
) -> CompareResult<bool> {
    compare(left, right, policy).map(Comparison::is_greater_or_equal)
}

/// Compare deux nombres physiques.
pub fn compare_numbers(
    left: Number,
    right: Number,
    policy: CoercionPolicy,
) -> CompareResult<Comparison> {
    let left = Value::Number(left);
    let right = Value::Number(right);

    compare_numeric_values(&left, &right, policy)
}

fn compare_numeric_values(
    left: &Value,
    right: &Value,
    policy: CoercionPolicy,
) -> CompareResult<Comparison> {
    let pair = coerce_value_pair_to_numbers(left, right, policy)?;

    compare_coerced_numbers(pair.left(), pair.right())
}

fn compare_coerced_numbers(left: CoercedNumber, right: CoercedNumber) -> CompareResult<Comparison> {
    let ordering = match (left, right) {
        (CoercedNumber::Signed(left), CoercedNumber::Signed(right)) => left.cmp(&right),

        (CoercedNumber::Unsigned(left), CoercedNumber::Unsigned(right)) => left.cmp(&right),

        (CoercedNumber::Float(left), CoercedNumber::Float(right)) => left
            .partial_cmp(&right)
            .ok_or(CompareFailure::IncompatibleValues)?,

        _ => return Err(CompareFailure::IncompatibleValues),
    };

    Ok(Comparison::from_ordering(ordering))
}

#[cfg(test)]
mod tests {
    use crate::{Document, Number};

    use super::*;

    #[test]
    fn comparison_converts_from_and_to_ordering() {
        assert_eq!(Comparison::from_ordering(Ordering::Less), Comparison::Less);

        assert_eq!(Comparison::Equal.into_ordering(), Ordering::Equal);
    }

    #[test]
    fn null_equals_null() {
        assert_eq!(
            compare(&Value::Null, &Value::Null, CoercionPolicy::Strict,),
            Ok(Comparison::Equal)
        );
    }

    #[test]
    fn null_is_not_comparable_to_other_values() {
        assert_eq!(
            compare(&Value::Null, &Value::from(false), CoercionPolicy::Implicit,),
            Err(CompareFailure::IncompatibleValues)
        );
    }

    #[test]
    fn null_is_not_equal_to_non_null_values() {
        assert_eq!(
            equals(
                &Value::Null,
                &Value::from("1.598-900.0"),
                CoercionPolicy::Implicit,
            ),
            Ok(false)
        );
        assert_eq!(
            equals(&Value::Null, &Value::from(false), CoercionPolicy::Strict,),
            Ok(false)
        );
    }

    #[test]
    fn null_is_different_from_non_null_values() {
        assert_eq!(
            not_equals(
                &Value::Null,
                &Value::from("1.598-900.0"),
                CoercionPolicy::Implicit,
            ),
            Ok(true)
        );
    }

    #[test]
    fn equality_is_total_across_incompatible_physical_kinds() {
        assert_eq!(
            equals(
                &Value::from(true),
                &Value::from("true"),
                CoercionPolicy::Implicit,
            ),
            Ok(false)
        );
    }

    #[test]
    fn booleans_are_ordered_false_before_true() {
        assert_eq!(
            compare(
                &Value::from(false),
                &Value::from(true),
                CoercionPolicy::Strict,
            ),
            Ok(Comparison::Less)
        );
    }

    #[test]
    fn strings_are_compared_lexicographically() {
        assert_eq!(
            compare(
                &Value::from("alice"),
                &Value::from("bob"),
                CoercionPolicy::Strict,
            ),
            Ok(Comparison::Less)
        );
    }

    #[test]
    fn identical_signed_numbers_are_equal() {
        assert_eq!(
            compare_numbers(
                Number::Signed(18),
                Number::Signed(18),
                CoercionPolicy::Strict,
            ),
            Ok(Comparison::Equal)
        );
    }

    #[test]
    fn signed_numbers_are_ordered() {
        assert_eq!(
            compare_numbers(
                Number::Signed(-1),
                Number::Signed(18),
                CoercionPolicy::Strict,
            ),
            Ok(Comparison::Less)
        );
    }

    #[test]
    fn unsigned_numbers_are_ordered() {
        assert_eq!(
            compare_numbers(
                Number::Unsigned(20),
                Number::Unsigned(18),
                CoercionPolicy::Strict,
            ),
            Ok(Comparison::Greater)
        );
    }

    #[test]
    fn floats_are_ordered() {
        assert_eq!(
            compare_numbers(
                Number::Float(18.5),
                Number::Float(18.0),
                CoercionPolicy::Strict,
            ),
            Ok(Comparison::Greater)
        );
    }

    #[test]
    fn strict_policy_preserves_numeric_representations() {
        assert_eq!(
            compare_numbers(
                Number::Signed(18),
                Number::Unsigned(18),
                CoercionPolicy::Strict,
            ),
            Err(CompareFailure::Coercion(CoercionFailure::ForbiddenByPolicy))
        );
    }

    #[test]
    fn numeric_policy_compares_signed_and_unsigned_values() {
        assert_eq!(
            compare_numbers(
                Number::Signed(18),
                Number::Unsigned(18),
                CoercionPolicy::Numeric,
            ),
            Ok(Comparison::Equal)
        );
    }

    #[test]
    fn numeric_policy_compares_integer_and_float() {
        assert_eq!(
            compare_numbers(
                Number::Signed(18),
                Number::Float(18.5),
                CoercionPolicy::Numeric,
            ),
            Ok(Comparison::Less)
        );
    }

    #[test]
    fn precision_loss_is_propagated() {
        assert_eq!(
            compare_numbers(
                Number::Signed(9_007_199_254_740_993),
                Number::Float(1.0),
                CoercionPolicy::Numeric,
            ),
            Err(CompareFailure::Coercion(CoercionFailure::PrecisionLoss))
        );
    }

    #[test]
    fn implicit_policy_compares_number_and_numeric_string() {
        assert_eq!(
            compare(
                &Value::from(18_i64),
                &Value::from("18"),
                CoercionPolicy::Implicit,
            ),
            Ok(Comparison::Equal)
        );
    }

    #[test]
    fn numeric_string_with_leading_zero_is_equal_to_number() {
        assert_eq!(
            equals(
                &Value::from("018"),
                &Value::from(18_i64),
                CoercionPolicy::Implicit,
            ),
            Ok(true)
        );
    }

    #[test]
    fn numeric_policy_rejects_string_to_number() {
        assert_eq!(
            compare(
                &Value::from(18_i64),
                &Value::from("18"),
                CoercionPolicy::Numeric,
            ),
            Err(CompareFailure::Coercion(CoercionFailure::ForbiddenByPolicy))
        );
    }

    #[test]
    fn two_strings_remain_lexical_under_implicit_policy() {
        assert_eq!(
            compare(
                &Value::from("18"),
                &Value::from("2"),
                CoercionPolicy::Implicit,
            ),
            Ok(Comparison::Less)
        );
    }

    #[test]
    fn non_numeric_string_is_incompatible_with_number() {
        assert_eq!(
            compare(
                &Value::from(18_i64),
                &Value::from("eighteen"),
                CoercionPolicy::Implicit,
            ),
            Err(CompareFailure::Coercion(CoercionFailure::IncompatibleValue))
        );
    }

    #[test]
    fn physically_equal_arrays_are_equal() {
        let left = Value::array([Value::from(1_i64), Value::from(2_i64)]);

        let right = left.clone();

        assert_eq!(equals(&left, &right, CoercionPolicy::Strict), Ok(true));
    }

    #[test]
    fn distinct_arrays_are_not_equal_without_requiring_an_order() {
        let left = Value::array([Value::from(1_i64)]);
        let right = Value::array([Value::from(2_i64)]);

        assert_eq!(equals(&left, &right, CoercionPolicy::Strict), Ok(false));
    }

    #[test]
    fn physically_equal_objects_are_equal() {
        let left = Value::from(Document::from_fields([("name", Value::from("Tom"))]));

        let right = left.clone();

        assert_eq!(equals(&left, &right, CoercionPolicy::Strict), Ok(true));
    }

    #[test]
    fn incompatible_physical_kinds_are_rejected() {
        assert_eq!(
            compare(
                &Value::from(true),
                &Value::from("true"),
                CoercionPolicy::Implicit,
            ),
            Err(CompareFailure::IncompatibleValues)
        );
    }

    #[test]
    fn not_equals_negates_equality() {
        assert_eq!(
            not_equals(
                &Value::from(18_i64),
                &Value::from("18"),
                CoercionPolicy::Implicit,
            ),
            Ok(false)
        );
    }

    #[test]
    fn compare_failure_has_readable_messages() {
        assert_eq!(
            CompareFailure::IncompatibleValues.to_string(),
            "values are not comparable"
        );

        assert_eq!(
            CompareFailure::Coercion(CoercionFailure::PrecisionLoss).to_string(),
            "value coercion failed: precision_loss"
        );
    }

    #[test]
    fn comparison_predicates_cover_every_relation() {
        assert!(Comparison::Less.is_less());
        assert!(Comparison::Less.is_less_or_equal());
        assert!(!Comparison::Less.is_equal());

        assert!(Comparison::Equal.is_equal());
        assert!(Comparison::Equal.is_less_or_equal());
        assert!(Comparison::Equal.is_greater_or_equal());

        assert!(Comparison::Greater.is_greater());
        assert!(Comparison::Greater.is_greater_or_equal());
        assert!(!Comparison::Greater.is_equal());
    }

    #[test]
    fn comparison_can_be_reversed() {
        assert_eq!(Comparison::Less.reverse(), Comparison::Greater);
        assert_eq!(Comparison::Equal.reverse(), Comparison::Equal);
        assert_eq!(Comparison::Greater.reverse(), Comparison::Less);
    }

    #[test]
    fn relational_helpers_delegate_to_operational_comparison() {
        let left = Value::from(18_i64);
        let right = Value::from("20");

        assert_eq!(less_than(&left, &right, CoercionPolicy::Implicit), Ok(true),);
        assert_eq!(
            less_than_or_equal(&left, &right, CoercionPolicy::Implicit),
            Ok(true),
        );
        assert_eq!(
            greater_than(&left, &right, CoercionPolicy::Implicit),
            Ok(false),
        );
        assert_eq!(
            greater_than_or_equal(&left, &right, CoercionPolicy::Implicit),
            Ok(false),
        );
    }

    #[test]
    fn physical_equality_never_applies_coercion() {
        assert!(!physically_equals(&Value::from(18_i64), &Value::from("18"),));

        assert_eq!(
            equals(
                &Value::from(18_i64),
                &Value::from("18"),
                CoercionPolicy::Implicit,
            ),
            Ok(true),
        );
    }

    #[test]
    fn compare_failure_exposes_its_category() {
        let incompatible = CompareFailure::IncompatibleValues;
        assert!(incompatible.is_incompatible_values());
        assert_eq!(incompatible.coercion_failure(), None);

        let coercion = CompareFailure::Coercion(CoercionFailure::PrecisionLoss);
        assert!(!coercion.is_incompatible_values());
        assert_eq!(
            coercion.coercion_failure(),
            Some(CoercionFailure::PrecisionLoss),
        );
    }
}
