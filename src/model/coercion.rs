//! Value coercion rules used by query evaluation.

use std::fmt;
use std::num::{ParseFloatError, ParseIntError};

use crate::{Number, NumberKind, Value};

/// Politique utilisée lors d'une tentative de coercition.
///
/// La politique permet aux futures opérations de choisir explicitement le
/// niveau de conversion accepté.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CoercionPolicy {
    /// Aucune conversion entre représentations distinctes.
    ///
    /// Deux valeurs doivent déjà partager la représentation attendue.
    Strict,

    /// Autorise les conversions entre représentations numériques.
    ///
    /// Une chaîne ne sera pas interprétée comme un nombre.
    Numeric,

    /// Autorise les conversions numériques ainsi que l'interprétation stricte
    /// des chaînes numériques.
    Implicit,
}

impl CoercionPolicy {
    /// Retourne le nom stable de la politique.
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Numeric => "numeric",
            Self::Implicit => "implicit",
        }
    }

    /// Indique si la politique autorise le rapprochement de représentations
    /// numériques différentes.
    #[must_use]
    pub const fn allows_numeric_conversion(self) -> bool {
        matches!(self, Self::Numeric | Self::Implicit)
    }

    /// Indique si la politique autorise l'interprétation d'une chaîne comme
    /// un nombre.
    #[must_use]
    pub const fn allows_string_to_number(self) -> bool {
        matches!(self, Self::Implicit)
    }

    /// Indique si cette politique est totalement stricte.
    #[must_use]
    pub const fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

impl fmt::Display for CoercionPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Motif d'échec d'une coercition.
///
/// Un échec de coercition constitue un résultat opérationnel normal. Il n'est
/// donc pas représenté par l'erreur générale du crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CoercionFailure {
    /// La politique active interdit la conversion nécessaire.
    ForbiddenByPolicy,

    /// La valeur ne peut pas être interprétée dans la représentation demandée.
    IncompatibleValue,

    /// Une conversion numérique exacte dépasserait la représentation cible.
    OutOfRange,

    /// Une conversion vers un flottant entraînerait une perte de précision
    /// interdite par le contrat de coercition.
    PrecisionLoss,
}

impl CoercionFailure {
    /// Retourne le nom stable de l'échec.
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ForbiddenByPolicy => "forbidden_by_policy",
            Self::IncompatibleValue => "incompatible_value",
            Self::OutOfRange => "out_of_range",
            Self::PrecisionLoss => "precision_loss",
        }
    }

    /// Indique si la politique active a refusé une conversion autrement valide.
    #[must_use]
    pub const fn is_forbidden_by_policy(self) -> bool {
        matches!(self, Self::ForbiddenByPolicy)
    }

    /// Indique si la valeur ne possède aucune représentation compatible.
    #[must_use]
    pub const fn is_incompatible_value(self) -> bool {
        matches!(self, Self::IncompatibleValue)
    }

    /// Indique si la valeur dépasse la représentation cible.
    #[must_use]
    pub const fn is_out_of_range(self) -> bool {
        matches!(self, Self::OutOfRange)
    }

    /// Indique si une conversion exacte entraînerait une perte de précision.
    #[must_use]
    pub const fn is_precision_loss(self) -> bool {
        matches!(self, Self::PrecisionLoss)
    }
}

impl fmt::Display for CoercionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Résultat d'une tentative de coercition.
pub type CoercionResult<T> = std::result::Result<T, CoercionFailure>;

/// Représentation numérique commune utilisable par une opération.
///
/// Cette énumération est distincte de [`Number`] : elle représente le résultat
/// temporaire d'une coercition entre deux opérandes.
///
/// Les valeurs signées et non signées restent distinctes tant qu'une opération
/// ne nécessite pas leur rapprochement.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum CoercedNumber {
    /// Entier signé.
    Signed(i64),

    /// Entier non signé.
    Unsigned(u64),

    /// Nombre flottant fini.
    Float(f64),
}

impl CoercedNumber {
    /// Retourne la représentation numérique obtenue.
    #[must_use]
    #[inline]
    pub const fn kind(self) -> NumberKind {
        match self {
            Self::Signed(_) => NumberKind::Signed,
            Self::Unsigned(_) => NumberKind::Unsigned,
            Self::Float(_) => NumberKind::Float,
        }
    }

    /// Retourne la valeur signée lorsqu'elle possède cette représentation.
    #[must_use]
    pub const fn as_signed(self) -> Option<i64> {
        match self {
            Self::Signed(value) => Some(value),
            _ => None,
        }
    }

    /// Retourne la valeur non signée lorsqu'elle possède cette représentation.
    #[must_use]
    pub const fn as_unsigned(self) -> Option<u64> {
        match self {
            Self::Unsigned(value) => Some(value),
            _ => None,
        }
    }

    /// Retourne la valeur flottante lorsqu'elle possède cette représentation.
    #[must_use]
    pub const fn as_float(self) -> Option<f64> {
        match self {
            Self::Float(value) => Some(value),
            _ => None,
        }
    }

    /// Convertit la représentation temporaire vers un [`Number`].
    #[must_use]
    pub const fn into_number(self) -> Number {
        match self {
            Self::Signed(value) => Number::Signed(value),
            Self::Unsigned(value) => Number::Unsigned(value),
            Self::Float(value) => Number::Float(value),
        }
    }

    /// Indique si cette représentation est entière.
    #[must_use]
    pub const fn is_integer(self) -> bool {
        matches!(self, Self::Signed(_) | Self::Unsigned(_))
    }

    /// Indique si cette représentation est flottante.
    #[must_use]
    pub const fn is_float(self) -> bool {
        matches!(self, Self::Float(_))
    }

    /// Indique si la valeur numérique est finie.
    ///
    /// Les entiers sont toujours finis. Cette méthode est surtout utile pour
    /// valider les valeurs [`CoercedNumber::Float`] construites directement.
    #[must_use]
    pub fn is_finite(self) -> bool {
        match self {
            Self::Signed(_) | Self::Unsigned(_) => true,
            Self::Float(value) => value.is_finite(),
        }
    }
}

impl From<Number> for CoercedNumber {
    fn from(number: Number) -> Self {
        match number {
            Number::Signed(value) => Self::Signed(value),
            Number::Unsigned(value) => Self::Unsigned(value),
            Number::Float(value) => Self::Float(value),

            #[allow(unreachable_patterns)]
            _ => unreachable!("all current Number variants are handled"),
        }
    }
}

impl From<CoercedNumber> for Number {
    fn from(number: CoercedNumber) -> Self {
        number.into_number()
    }
}

/// Paire de nombres rapprochés vers une représentation commune.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CoercedNumberPair {
    left: CoercedNumber,
    right: CoercedNumber,
}

impl CoercedNumberPair {
    /// Construit une paire déjà coercée.
    #[must_use]
    #[inline]
    pub const fn new(left: CoercedNumber, right: CoercedNumber) -> Self {
        Self { left, right }
    }

    /// Retourne l'opérande gauche.
    #[must_use]
    pub const fn left(self) -> CoercedNumber {
        self.left
    }

    /// Retourne l'opérande droit.
    #[must_use]
    pub const fn right(self) -> CoercedNumber {
        self.right
    }

    /// Retourne les deux opérandes.
    #[must_use]
    pub const fn into_tuple(self) -> (CoercedNumber, CoercedNumber) {
        (self.left, self.right)
    }

    /// Retourne la représentation commune.
    ///
    /// Une paire construite par [`coerce_number_pair`] possède toujours une
    /// représentation commune.
    #[must_use]
    #[inline]
    pub const fn kind(self) -> NumberKind {
        self.left.kind()
    }

    /// Indique si les deux opérandes utilisent la même représentation.
    #[must_use]
    pub const fn has_common_kind(self) -> bool {
        matches!(
            (self.left, self.right),
            (CoercedNumber::Signed(_), CoercedNumber::Signed(_))
                | (CoercedNumber::Unsigned(_), CoercedNumber::Unsigned(_))
                | (CoercedNumber::Float(_), CoercedNumber::Float(_))
        )
    }

    /// Convertit les deux opérandes vers leurs représentations physiques.
    #[must_use]
    pub const fn into_numbers(self) -> (Number, Number) {
        (self.left.into_number(), self.right.into_number())
    }
}

/// Tente d'interpréter une valeur comme un nombre.
///
/// # Règles
///
/// - une [`Value::Number`] est toujours acceptée ;
/// - une [`Value::String`] est acceptée uniquement avec
///   [`CoercionPolicy::Implicit`] ;
/// - les autres valeurs sont incompatibles.
///
/// Les espaces entourant une chaîne numérique sont refusés. Cela évite que la
/// couche de coercition introduise silencieusement une normalisation textuelle.
///
/// # Examples
///
/// ```
/// use og_core::{
///     coerce_value_to_number,
///     CoercedNumber,
///     CoercionPolicy,
///     Value,
/// };
///
/// let value = Value::from("18");
///
/// assert_eq!(
///     coerce_value_to_number(&value, CoercionPolicy::Implicit),
///     Ok(CoercedNumber::Signed(18)),
/// );
/// ```
pub fn coerce_value_to_number(
    value: &Value,
    policy: CoercionPolicy,
) -> CoercionResult<CoercedNumber> {
    match value {
        Value::Number(number) => Ok((*number).into()),

        Value::String(text) => {
            if policy.allows_string_to_number() {
                parse_number(text)
            } else {
                Err(CoercionFailure::ForbiddenByPolicy)
            }
        }

        _ => Err(CoercionFailure::IncompatibleValue),
    }
}

/// Tente de rapprocher deux valeurs vers une représentation numérique commune.
///
/// Cette fonction combine :
///
/// 1. l'interprétation éventuelle des valeurs comme nombres ;
/// 2. le choix d'une représentation commune exacte.
///
/// Elle n'effectue aucune comparaison.
///
/// # Examples
///
/// ```
/// use og_core::{
///     coerce_value_pair_to_numbers,
///     CoercedNumber,
///     CoercionPolicy,
///     Value,
/// };
///
/// let pair = coerce_value_pair_to_numbers(
///     &Value::from(18_i64),
///     &Value::from("18"),
///     CoercionPolicy::Implicit,
/// )?;
///
/// assert_eq!(pair.left(), CoercedNumber::Signed(18));
/// assert_eq!(pair.right(), CoercedNumber::Signed(18));
///
/// # Ok::<(), og_core::CoercionFailure>(())
/// ```
pub fn coerce_value_pair_to_numbers(
    left: &Value,
    right: &Value,
    policy: CoercionPolicy,
) -> CoercionResult<CoercedNumberPair> {
    let left = coerce_value_to_number(left, policy)?;
    let right = coerce_value_to_number(right, policy)?;

    coerce_number_pair(left, right, policy)
}

/// Rapproche deux représentations numériques vers une représentation commune.
///
/// La conversion est exacte : cette fonction refuse les conversions qui
/// nécessiteraient une perte de précision.
///
/// # Représentation choisie
///
/// - mêmes représentations : aucune conversion ;
/// - signé + non signé : signé si la valeur non signée tient dans `i64`,
///   sinon non signé si la valeur signée est positive, sinon échec ;
/// - entier + flottant : flottant uniquement si l'entier est représentable
///   exactement en `f64`.
pub fn coerce_number_pair(
    left: CoercedNumber,
    right: CoercedNumber,
    policy: CoercionPolicy,
) -> CoercionResult<CoercedNumberPair> {
    use CoercedNumber::{Float, Signed, Unsigned};

    match (left, right) {
        (Signed(left), Signed(right)) => Ok(CoercedNumberPair::new(Signed(left), Signed(right))),

        (Unsigned(left), Unsigned(right)) => {
            Ok(CoercedNumberPair::new(Unsigned(left), Unsigned(right)))
        }

        (Float(left), Float(right)) => Ok(CoercedNumberPair::new(Float(left), Float(right))),

        (_, _) if !policy.allows_numeric_conversion() => Err(CoercionFailure::ForbiddenByPolicy),

        (Signed(left), Unsigned(right)) => coerce_signed_unsigned(left, right),

        (Unsigned(left), Signed(right)) => {
            let pair = coerce_signed_unsigned(right, left)?;

            Ok(CoercedNumberPair::new(pair.right(), pair.left()))
        }

        (Signed(left), Float(right)) => {
            let left = signed_to_exact_float(left)?;

            Ok(CoercedNumberPair::new(Float(left), Float(right)))
        }

        (Float(left), Signed(right)) => {
            let right = signed_to_exact_float(right)?;

            Ok(CoercedNumberPair::new(Float(left), Float(right)))
        }

        (Unsigned(left), Float(right)) => {
            let left = unsigned_to_exact_float(left)?;

            Ok(CoercedNumberPair::new(Float(left), Float(right)))
        }

        (Float(left), Unsigned(right)) => {
            let right = unsigned_to_exact_float(right)?;

            Ok(CoercedNumberPair::new(Float(left), Float(right)))
        }
    }
}

/// Rapproche deux [`Number`] vers une représentation numérique commune.
pub fn coerce_numbers(
    left: Number,
    right: Number,
    policy: CoercionPolicy,
) -> CoercionResult<CoercedNumberPair> {
    coerce_number_pair(left.into(), right.into(), policy)
}

/// Analyse une chaîne et retourne directement sa représentation physique.
pub fn parse_number_value(text: &str) -> CoercionResult<Number> {
    parse_number(text).map(CoercedNumber::into_number)
}

/// Analyse une chaîne numérique selon la syntaxe fondamentale d'OG.
///
/// Ordre d'analyse :
///
/// 1. entier signé lorsqu'un signe `-` est présent ;
/// 2. entier non signé pour les entiers positifs dépassant `i64::MAX` ;
/// 3. flottant fini lorsqu'un séparateur décimal ou un exposant est présent.
///
/// Sont refusés :
///
/// - les espaces ;
/// - les chaînes vides ;
/// - `NaN` ;
/// - les infinis ;
/// - les séparateurs autres que `.` ;
/// - les suffixes et préfixes de représentation Rust.
pub fn parse_number(text: &str) -> CoercionResult<CoercedNumber> {
    if text.is_empty() || text.trim() != text {
        return Err(CoercionFailure::IncompatibleValue);
    }

    if is_integer_syntax(text) {
        return parse_integer(text);
    }

    parse_float(text)
}

/// Indique si une chaîne suit la syntaxe fondamentale d'un entier.
///
/// Les formats `+18` et `-18` sont acceptés. Les underscores et les préfixes
/// de base comme `0x10` ne le sont pas.
#[must_use]
pub fn is_integer_syntax(text: &str) -> bool {
    let digits = text
        .strip_prefix('-')
        .or_else(|| text.strip_prefix('+'))
        .unwrap_or(text);

    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

/// Indique si une chaîne peut être interprétée comme un nombre par OG.
#[must_use]
pub fn is_numeric_string(text: &str) -> bool {
    parse_number(text).is_ok()
}

fn parse_integer(text: &str) -> CoercionResult<CoercedNumber> {
    if text.starts_with('-') {
        return text
            .parse::<i64>()
            .map(CoercedNumber::Signed)
            .map_err(|error| classify_signed_parse_error(&error));
    }

    if let Ok(value) = text.parse::<i64>() {
        return Ok(CoercedNumber::Signed(value));
    }

    text.parse::<u64>()
        .map(CoercedNumber::Unsigned)
        .map_err(|error| classify_unsigned_parse_error(&error))
}

fn parse_float(text: &str) -> CoercionResult<CoercedNumber> {
    let value = text.parse::<f64>().map_err(classify_float_parse_error)?;

    if !value.is_finite() {
        return Err(CoercionFailure::OutOfRange);
    }

    Ok(CoercedNumber::Float(normalize_zero(value)))
}

fn coerce_signed_unsigned(signed: i64, unsigned: u64) -> CoercionResult<CoercedNumberPair> {
    if let Ok(unsigned_as_signed) = i64::try_from(unsigned) {
        return Ok(CoercedNumberPair::new(
            CoercedNumber::Signed(signed),
            CoercedNumber::Signed(unsigned_as_signed),
        ));
    }

    let signed_as_unsigned = u64::try_from(signed).map_err(|_| CoercionFailure::OutOfRange)?;

    Ok(CoercedNumberPair::new(
        CoercedNumber::Unsigned(signed_as_unsigned),
        CoercedNumber::Unsigned(unsigned),
    ))
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the non-negative round-trip cast intentionally verifies exact representability"
)]
fn signed_to_exact_float(value: i64) -> CoercionResult<f64> {
    let converted = value as f64;

    if converted as i64 == value {
        Ok(converted)
    } else {
        Err(CoercionFailure::PrecisionLoss)
    }
}

fn unsigned_to_exact_float(value: u64) -> CoercionResult<f64> {
    let converted = value as f64;

    if converted as u64 == value {
        Ok(converted)
    } else {
        Err(CoercionFailure::PrecisionLoss)
    }
}

fn normalize_zero(value: f64) -> f64 {
    if value == 0.0 {
        0.0
    } else {
        value
    }
}

fn classify_signed_parse_error(error: &ParseIntError) -> CoercionFailure {
    classify_int_error(error)
}

fn classify_unsigned_parse_error(error: &ParseIntError) -> CoercionFailure {
    classify_int_error(error)
}

fn classify_int_error(error: &ParseIntError) -> CoercionFailure {
    use std::num::IntErrorKind;

    match error.kind() {
        IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => CoercionFailure::OutOfRange,

        _ => CoercionFailure::IncompatibleValue,
    }
}

fn classify_float_parse_error(_error: ParseFloatError) -> CoercionFailure {
    CoercionFailure::IncompatibleValue
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_names_are_stable() {
        assert_eq!(CoercionPolicy::Strict.as_str(), "strict");
        assert_eq!(CoercionPolicy::Numeric.as_str(), "numeric");
        assert_eq!(CoercionPolicy::Implicit.as_str(), "implicit");
    }

    #[test]
    fn strict_policy_allows_no_conversion() {
        assert!(!CoercionPolicy::Strict.allows_numeric_conversion());

        assert!(!CoercionPolicy::Strict.allows_string_to_number());
    }

    #[test]
    fn numeric_policy_allows_numeric_conversion_only() {
        assert!(CoercionPolicy::Numeric.allows_numeric_conversion());

        assert!(!CoercionPolicy::Numeric.allows_string_to_number());
    }

    #[test]
    fn implicit_policy_allows_all_current_coercions() {
        assert!(CoercionPolicy::Implicit.allows_numeric_conversion());

        assert!(CoercionPolicy::Implicit.allows_string_to_number());
    }

    #[test]
    fn failure_names_are_stable() {
        assert_eq!(
            CoercionFailure::ForbiddenByPolicy.as_str(),
            "forbidden_by_policy"
        );

        assert_eq!(
            CoercionFailure::IncompatibleValue.as_str(),
            "incompatible_value"
        );

        assert_eq!(CoercionFailure::OutOfRange.as_str(), "out_of_range");

        assert_eq!(CoercionFailure::PrecisionLoss.as_str(), "precision_loss");
    }

    #[test]
    fn numeric_value_requires_no_coercion() {
        let value = Value::from(18_i64);

        assert_eq!(
            coerce_value_to_number(&value, CoercionPolicy::Strict,),
            Ok(CoercedNumber::Signed(18))
        );
    }

    #[test]
    fn numeric_string_is_accepted_implicitly() {
        let value = Value::from("18");

        assert_eq!(
            coerce_value_to_number(&value, CoercionPolicy::Implicit,),
            Ok(CoercedNumber::Signed(18))
        );
    }

    #[test]
    fn numeric_string_is_rejected_by_numeric_policy() {
        let value = Value::from("18");

        assert_eq!(
            coerce_value_to_number(&value, CoercionPolicy::Numeric,),
            Err(CoercionFailure::ForbiddenByPolicy)
        );
    }

    #[test]
    fn numeric_string_is_rejected_by_strict_policy() {
        let value = Value::from("18");

        assert_eq!(
            coerce_value_to_number(&value, CoercionPolicy::Strict,),
            Err(CoercionFailure::ForbiddenByPolicy)
        );
    }

    #[test]
    fn non_numeric_values_are_incompatible() {
        let values = [Value::Null, Value::from(true), Value::array([])];

        for value in values {
            assert_eq!(
                coerce_value_to_number(&value, CoercionPolicy::Implicit,),
                Err(CoercionFailure::IncompatibleValue)
            );
        }
    }

    #[test]
    fn positive_integer_string_becomes_signed_when_possible() {
        assert_eq!(parse_number("18"), Ok(CoercedNumber::Signed(18)));
    }

    #[test]
    fn explicitly_positive_integer_is_accepted() {
        assert_eq!(parse_number("+18"), Ok(CoercedNumber::Signed(18)));
    }

    #[test]
    fn negative_integer_string_becomes_signed() {
        assert_eq!(parse_number("-18"), Ok(CoercedNumber::Signed(-18)));
    }

    #[test]
    fn large_positive_integer_becomes_unsigned() {
        let text = u64::MAX.to_string();

        assert_eq!(parse_number(&text), Ok(CoercedNumber::Unsigned(u64::MAX)));
    }

    #[test]
    fn integer_above_u64_is_out_of_range() {
        assert_eq!(
            parse_number("18446744073709551616"),
            Err(CoercionFailure::OutOfRange)
        );
    }

    #[test]
    fn integer_below_i64_is_out_of_range() {
        assert_eq!(
            parse_number("-9223372036854775809"),
            Err(CoercionFailure::OutOfRange)
        );
    }

    #[test]
    fn decimal_string_becomes_float() {
        assert_eq!(parse_number("18.5"), Ok(CoercedNumber::Float(18.5)));
    }

    #[test]
    fn exponent_string_becomes_float() {
        assert_eq!(parse_number("1e3"), Ok(CoercedNumber::Float(1000.0)));
    }

    #[test]
    fn negative_zero_is_normalized() {
        let number = parse_number("-0.0").expect("-0.0 must be accepted");

        let CoercedNumber::Float(value) = number else {
            panic!("expected a floating-point value");
        };

        assert_eq!(value.to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn nan_is_rejected() {
        assert_eq!(parse_number("NaN"), Err(CoercionFailure::OutOfRange));
    }

    #[test]
    fn positive_infinity_is_rejected() {
        assert_eq!(parse_number("inf"), Err(CoercionFailure::OutOfRange));
    }

    #[test]
    fn negative_infinity_is_rejected() {
        assert_eq!(parse_number("-inf"), Err(CoercionFailure::OutOfRange));
    }

    #[test]
    fn surrounding_spaces_are_rejected() {
        assert_eq!(parse_number(" 18"), Err(CoercionFailure::IncompatibleValue));

        assert_eq!(parse_number("18 "), Err(CoercionFailure::IncompatibleValue));
    }

    #[test]
    fn empty_string_is_rejected() {
        assert_eq!(parse_number(""), Err(CoercionFailure::IncompatibleValue));
    }

    #[test]
    fn underscores_are_rejected() {
        assert_eq!(
            parse_number("1_000"),
            Err(CoercionFailure::IncompatibleValue)
        );
    }

    #[test]
    fn hexadecimal_syntax_is_rejected() {
        assert_eq!(
            parse_number("0x10"),
            Err(CoercionFailure::IncompatibleValue)
        );
    }

    #[test]
    fn arbitrary_text_is_rejected() {
        assert_eq!(
            parse_number("eighteen"),
            Err(CoercionFailure::IncompatibleValue)
        );
    }

    #[test]
    fn integer_syntax_detection_is_strict() {
        assert!(is_integer_syntax("18"));
        assert!(is_integer_syntax("+18"));
        assert!(is_integer_syntax("-18"));
        assert!(is_integer_syntax("018"));

        assert!(!is_integer_syntax(""));
        assert!(!is_integer_syntax("+"));
        assert!(!is_integer_syntax("-"));
        assert!(!is_integer_syntax("18.0"));
        assert!(!is_integer_syntax("1e3"));
        assert!(!is_integer_syntax("1_000"));
    }

    #[test]
    fn numeric_string_detection_uses_the_full_parser() {
        assert!(is_numeric_string("18"));
        assert!(is_numeric_string("-18"));
        assert!(is_numeric_string("18.5"));
        assert!(is_numeric_string("1e3"));

        assert!(!is_numeric_string(""));
        assert!(!is_numeric_string(" 18"));
        assert!(!is_numeric_string("NaN"));
        assert!(!is_numeric_string("unknown"));
    }

    #[test]
    fn equal_signed_numbers_require_no_conversion() {
        let pair = coerce_number_pair(
            CoercedNumber::Signed(18),
            CoercedNumber::Signed(19),
            CoercionPolicy::Strict,
        )
        .expect("matching representations must be accepted");

        assert_eq!(
            pair.into_tuple(),
            (CoercedNumber::Signed(18), CoercedNumber::Signed(19),)
        );
    }

    #[test]
    fn strict_policy_rejects_distinct_numeric_representations() {
        assert_eq!(
            coerce_number_pair(
                CoercedNumber::Signed(18),
                CoercedNumber::Unsigned(18),
                CoercionPolicy::Strict,
            ),
            Err(CoercionFailure::ForbiddenByPolicy)
        );
    }

    #[test]
    fn small_unsigned_value_can_join_signed_representation() {
        let pair = coerce_number_pair(
            CoercedNumber::Signed(-1),
            CoercedNumber::Unsigned(18),
            CoercionPolicy::Numeric,
        )
        .expect("18 fits in i64");

        assert_eq!(
            pair.into_tuple(),
            (CoercedNumber::Signed(-1), CoercedNumber::Signed(18),)
        );
    }

    #[test]
    fn large_unsigned_value_can_join_unsigned_positive_signed_value() {
        let pair = coerce_number_pair(
            CoercedNumber::Signed(18),
            CoercedNumber::Unsigned(u64::MAX),
            CoercionPolicy::Numeric,
        )
        .expect("positive signed value fits in u64");

        assert_eq!(
            pair.into_tuple(),
            (
                CoercedNumber::Unsigned(18),
                CoercedNumber::Unsigned(u64::MAX),
            )
        );
    }

    #[test]
    fn negative_signed_and_large_unsigned_are_not_coercible() {
        assert_eq!(
            coerce_number_pair(
                CoercedNumber::Signed(-1),
                CoercedNumber::Unsigned(u64::MAX),
                CoercionPolicy::Numeric,
            ),
            Err(CoercionFailure::OutOfRange)
        );
    }

    #[test]
    fn exactly_representable_signed_integer_can_join_float() {
        let pair = coerce_number_pair(
            CoercedNumber::Signed(18),
            CoercedNumber::Float(18.5),
            CoercionPolicy::Numeric,
        )
        .expect("18 is exactly representable as f64");

        assert_eq!(
            pair.into_tuple(),
            (CoercedNumber::Float(18.0), CoercedNumber::Float(18.5),)
        );
    }

    #[test]
    fn imprecise_signed_integer_cannot_join_float() {
        let value = 9_007_199_254_740_993_i64;

        assert_eq!(
            coerce_number_pair(
                CoercedNumber::Signed(value),
                CoercedNumber::Float(1.0),
                CoercionPolicy::Numeric,
            ),
            Err(CoercionFailure::PrecisionLoss)
        );
    }

    #[test]
    fn imprecise_unsigned_integer_cannot_join_float() {
        let value = 9_007_199_254_740_993_u64;

        assert_eq!(
            coerce_number_pair(
                CoercedNumber::Unsigned(value),
                CoercedNumber::Float(1.0),
                CoercionPolicy::Numeric,
            ),
            Err(CoercionFailure::PrecisionLoss)
        );
    }

    #[test]
    fn value_pair_supports_implicit_string_to_number() {
        let left = Value::from(18_i64);
        let right = Value::from("18");

        let pair = coerce_value_pair_to_numbers(&left, &right, CoercionPolicy::Implicit)
            .expect("the string must be interpreted as a number");

        assert_eq!(
            pair.into_tuple(),
            (CoercedNumber::Signed(18), CoercedNumber::Signed(18),)
        );
    }

    #[test]
    fn value_pair_rejects_string_under_numeric_policy() {
        let left = Value::from(18_i64);
        let right = Value::from("18");

        assert_eq!(
            coerce_value_pair_to_numbers(&left, &right, CoercionPolicy::Numeric,),
            Err(CoercionFailure::ForbiddenByPolicy)
        );
    }

    #[test]
    fn coerced_number_converts_back_to_number() {
        assert_eq!(CoercedNumber::Signed(-1).into_number(), Number::Signed(-1));

        assert_eq!(
            CoercedNumber::Unsigned(1).into_number(),
            Number::Unsigned(1)
        );

        assert_eq!(CoercedNumber::Float(1.5).into_number(), Number::Float(1.5));
    }

    #[test]
    fn policy_and_failure_predicates_are_consistent() {
        assert!(CoercionPolicy::Strict.is_strict());
        assert!(!CoercionPolicy::Numeric.is_strict());
        assert!(!CoercionPolicy::Implicit.is_strict());

        assert!(CoercionFailure::ForbiddenByPolicy.is_forbidden_by_policy());
        assert!(CoercionFailure::IncompatibleValue.is_incompatible_value());
        assert!(CoercionFailure::OutOfRange.is_out_of_range());
        assert!(CoercionFailure::PrecisionLoss.is_precision_loss());
    }

    #[test]
    fn coerced_number_reports_its_category_and_finiteness() {
        assert!(CoercedNumber::Signed(-1).is_integer());
        assert!(CoercedNumber::Unsigned(1).is_integer());
        assert!(!CoercedNumber::Float(1.5).is_integer());

        assert!(CoercedNumber::Float(1.5).is_float());
        assert!(CoercedNumber::Float(1.5).is_finite());
        assert!(!CoercedNumber::Float(f64::INFINITY).is_finite());
    }

    #[test]
    fn coerced_pair_exposes_common_kind_and_physical_numbers() {
        let pair = CoercedNumberPair::new(CoercedNumber::Signed(18), CoercedNumber::Signed(20));

        assert!(pair.has_common_kind());
        assert_eq!(
            pair.into_numbers(),
            (Number::Signed(18), Number::Signed(20)),
        );

        let heterogeneous =
            CoercedNumberPair::new(CoercedNumber::Signed(18), CoercedNumber::Float(18.0));
        assert!(!heterogeneous.has_common_kind());
    }

    #[test]
    fn number_convenience_helpers_delegate_to_core_coercion() {
        let pair = coerce_numbers(
            Number::Signed(18),
            Number::Unsigned(20),
            CoercionPolicy::Numeric,
        )
        .expect("the numeric policy must reconcile compatible integers");

        assert_eq!(
            pair.into_tuple(),
            (CoercedNumber::Signed(18), CoercedNumber::Signed(20)),
        );

        assert_eq!(parse_number_value("18.5"), Ok(Number::Float(18.5)),);
    }
}
