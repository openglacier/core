#![cfg_attr(rustfmt, rustfmt_skip)]
//! Value coercion rules used by query evaluation.

use std::fmt;
use std::num::{ParseFloatError, ParseIntError};

use crate::{Number, NumberKind, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CoercionPolicy {
    /// No conversion.
    Strict,
    /// Only allow numeric conversion. Don't convert numeric to string.
    Numeric,
    /// Allow string and numeric to be converted
    Implicit,
}

impl CoercionPolicy {
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Numeric => "numeric",
            Self::Implicit => "implicit",
        }
    }

    #[must_use]
    pub const fn allows_numeric_conversion(self) -> bool {
        matches!(self, Self::Numeric | Self::Implicit)
    }

    #[must_use]
    pub const fn allows_string_to_number(self) -> bool {
        matches!(self, Self::Implicit)
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CoercionFailure {
    ForbiddenByPolicy,
    IncompatibleValue,
    OutOfRange,
    PrecisionLoss,
}

impl CoercionFailure {
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

    #[must_use]
    pub const fn is_forbidden_by_policy(self) -> bool {
        matches!(self, Self::ForbiddenByPolicy)
    }

    #[must_use]
    pub const fn is_incompatible_value(self) -> bool {
        matches!(self, Self::IncompatibleValue)
    }

    #[must_use]
    pub const fn is_out_of_range(self) -> bool {
        matches!(self, Self::OutOfRange)
    }

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

pub type CoercionResult<T> = std::result::Result<T, CoercionFailure>;

#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum CoercedNumber {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

impl CoercedNumber {
    #[must_use]
    #[inline]
    pub const fn kind(self) -> NumberKind {
        match self {
            Self::Signed(_) => NumberKind::Signed,
            Self::Unsigned(_) => NumberKind::Unsigned,
            Self::Float(_) => NumberKind::Float,
        }
    }

    #[must_use]
    pub const fn as_signed(self) -> Option<i64> {
        match self {
            Self::Signed(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_unsigned(self) -> Option<u64> {
        match self {
            Self::Unsigned(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_float(self) -> Option<f64> {
        match self {
            Self::Float(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub const fn into_number(self) -> Number {
        match self {
            Self::Signed(value) => Number::Signed(value),
            Self::Unsigned(value) => Number::Unsigned(value),
            Self::Float(value) => Number::Float(value),
        }
    }

    #[must_use]
    pub const fn is_integer(self) -> bool {
        matches!(self, Self::Signed(_) | Self::Unsigned(_))
    }

    #[must_use]
    pub const fn is_float(self) -> bool {
        matches!(self, Self::Float(_))
    }

    #[must_use]
    pub const fn is_finite(self) -> bool {
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CoercedNumberPair {
    left: CoercedNumber,
    right: CoercedNumber,
}

impl CoercedNumberPair {
    #[must_use]
    #[inline]
    pub const fn new(left: CoercedNumber, right: CoercedNumber) -> Self {
        Self { left, right }
    }

    #[must_use]
    pub const fn left(self) -> CoercedNumber {
        self.left
    }

    #[must_use]
    pub const fn right(self) -> CoercedNumber {
        self.right
    }

    #[must_use]
    pub const fn into_tuple(self) -> (CoercedNumber, CoercedNumber) {
        (self.left, self.right)
    }

    #[must_use]
    #[inline]
    pub const fn kind(self) -> NumberKind {
        self.left.kind()
    }

    #[must_use]
    pub const fn has_common_kind(self) -> bool {
        matches!(
            (self.left, self.right),
            (CoercedNumber::Signed(_), CoercedNumber::Signed(_))
                | (CoercedNumber::Unsigned(_), CoercedNumber::Unsigned(_))
                | (CoercedNumber::Float(_), CoercedNumber::Float(_))
        )
    }

    #[must_use]
    pub const fn into_numbers(self) -> (Number, Number) {
        (self.left.into_number(), self.right.into_number())
    }
}

pub fn coerce_value_to_number( value: &Value, policy: CoercionPolicy, ) -> CoercionResult<CoercedNumber> {
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

pub fn coerce_value_pair_to_numbers( left: &Value, right: &Value, policy: CoercionPolicy, ) -> CoercionResult<CoercedNumberPair> {
    let left = coerce_value_to_number(left, policy)?;
    let right = coerce_value_to_number(right, policy)?;

    coerce_number_pair(left, right, policy)
}

pub fn coerce_number_pair( left: CoercedNumber, right: CoercedNumber, policy: CoercionPolicy, ) -> CoercionResult<CoercedNumberPair> {
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

pub fn coerce_numbers( left: Number, right: Number, policy: CoercionPolicy, ) -> CoercionResult<CoercedNumberPair> {
    coerce_number_pair(left.into(), right.into(), policy)
}

pub fn parse_number_value(text: &str) -> CoercionResult<Number> {
    parse_number(text).map(CoercedNumber::into_number)
}

pub fn parse_number(text: &str) -> CoercionResult<CoercedNumber> {
    if text.is_empty() || text.trim() != text {
        return Err(CoercionFailure::IncompatibleValue);
    }
    if is_integer_syntax(text) {
        return parse_integer(text);
    }
    parse_float(text)
}

#[must_use]
pub fn is_integer_syntax(text: &str) -> bool {
    let digits = text
        .strip_prefix('-')
        .or_else(|| text.strip_prefix('+'))
        .unwrap_or(text);

    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

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
const fn signed_to_exact_float(value: i64) -> CoercionResult<f64> {
    let converted = value as f64;

    if converted as i64 == value {
        Ok(converted)
    } else {
        Err(CoercionFailure::PrecisionLoss)
    }
}

const fn unsigned_to_exact_float(value: u64) -> CoercionResult<f64> {
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

const fn classify_signed_parse_error(error: &ParseIntError) -> CoercionFailure {
    classify_int_error(error)
}

const fn classify_unsigned_parse_error(error: &ParseIntError) -> CoercionFailure {
    classify_int_error(error)
}

const fn classify_int_error(error: &ParseIntError) -> CoercionFailure {
    use std::num::IntErrorKind;
    match error.kind() {
        IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => CoercionFailure::OutOfRange,

        _ => CoercionFailure::IncompatibleValue,
    }
}

const fn classify_float_parse_error(_error: ParseFloatError) -> CoercionFailure {
    CoercionFailure::IncompatibleValue
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn policy_names_are_stable() { assert_eq!(CoercionPolicy::Strict.as_str(), "strict"); assert_eq!(CoercionPolicy::Numeric.as_str(), "numeric"); assert_eq!(CoercionPolicy::Implicit.as_str(), "implicit"); }
    #[test] fn strict_policy_allows_no_conversion() { assert!(!CoercionPolicy::Strict.allows_numeric_conversion()); assert!(!CoercionPolicy::Strict.allows_string_to_number()); }
    #[test] fn numeric_policy_allows_numeric_conversion_only() { assert!(CoercionPolicy::Numeric.allows_numeric_conversion()); assert!(!CoercionPolicy::Numeric.allows_string_to_number()); }
    #[test] fn implicit_policy_allows_all_current_coercions() { assert!(CoercionPolicy::Implicit.allows_numeric_conversion()); assert!(CoercionPolicy::Implicit.allows_string_to_number()); }
    #[test] fn failure_names_are_stable() { assert_eq!( CoercionFailure::ForbiddenByPolicy.as_str(), "forbidden_by_policy" ); assert_eq!( CoercionFailure::IncompatibleValue.as_str(), "incompatible_value" ); assert_eq!(CoercionFailure::OutOfRange.as_str(), "out_of_range"); assert_eq!(CoercionFailure::PrecisionLoss.as_str(), "precision_loss"); }
    #[test] fn numeric_value_requires_no_coercion() { let value = Value::from(18_i64); assert_eq!( coerce_value_to_number(&value, CoercionPolicy::Strict,), Ok(CoercedNumber::Signed(18)) ); }
    #[test] fn numeric_string_is_accepted_implicitly() { let value = Value::from("18"); assert_eq!( coerce_value_to_number(&value, CoercionPolicy::Implicit,), Ok(CoercedNumber::Signed(18)) ); }
    #[test] fn numeric_string_is_rejected_by_numeric_policy() { let value = Value::from("18"); assert_eq!( coerce_value_to_number(&value, CoercionPolicy::Numeric,), Err(CoercionFailure::ForbiddenByPolicy) ); }
    #[test] fn numeric_string_is_rejected_by_strict_policy() { let value = Value::from("18"); assert_eq!( coerce_value_to_number(&value, CoercionPolicy::Strict,), Err(CoercionFailure::ForbiddenByPolicy) ); }
    #[test] fn non_numeric_values_are_incompatible() { let values = [Value::Null, Value::from(true), Value::array([])]; for value in values { assert_eq!( coerce_value_to_number(&value, CoercionPolicy::Implicit,), Err(CoercionFailure::IncompatibleValue) ); } }
    #[test] fn positive_integer_string_becomes_signed_when_possible() { assert_eq!(parse_number("18"), Ok(CoercedNumber::Signed(18))); }
    #[test] fn explicitly_positive_integer_is_accepted() { assert_eq!(parse_number("+18"), Ok(CoercedNumber::Signed(18))); }
    #[test] fn negative_integer_string_becomes_signed() { assert_eq!(parse_number("-18"), Ok(CoercedNumber::Signed(-18))); }
    #[test] fn large_positive_integer_becomes_unsigned() { let text = u64::MAX.to_string(); assert_eq!(parse_number(&text), Ok(CoercedNumber::Unsigned(u64::MAX))); }
    #[test] fn integer_above_u64_is_out_of_range() { assert_eq!( parse_number("18446744073709551616"), Err(CoercionFailure::OutOfRange) ); }
    #[test] fn integer_below_i64_is_out_of_range() { assert_eq!( parse_number("-9223372036854775809"), Err(CoercionFailure::OutOfRange) ); }
    #[test] fn decimal_string_becomes_float() { assert_eq!(parse_number("18.5"), Ok(CoercedNumber::Float(18.5))); }
    #[test] fn exponent_string_becomes_float() { assert_eq!(parse_number("1e3"), Ok(CoercedNumber::Float(1000.0))); }
    #[test] fn negative_zero_is_normalized() { let number = parse_number("-0.0").expect("-0.0 must be accepted"); let CoercedNumber::Float(value) = number else { panic!("expected a floating-point value"); }; assert_eq!(value.to_bits(), 0.0_f64.to_bits()); }
    #[test] fn nan_is_rejected() { assert_eq!(parse_number("NaN"), Err(CoercionFailure::OutOfRange)); }
    #[test] fn positive_infinity_is_rejected() { assert_eq!(parse_number("inf"), Err(CoercionFailure::OutOfRange)); }
    #[test] fn negative_infinity_is_rejected() { assert_eq!(parse_number("-inf"), Err(CoercionFailure::OutOfRange)); }
    #[test] fn surrounding_spaces_are_rejected() { assert_eq!(parse_number(" 18"), Err(CoercionFailure::IncompatibleValue)); assert_eq!(parse_number("18 "), Err(CoercionFailure::IncompatibleValue)); }
    #[test] fn empty_string_is_rejected() { assert_eq!(parse_number(""), Err(CoercionFailure::IncompatibleValue)); }
    #[test] fn underscores_are_rejected() { assert_eq!( parse_number("1_000"), Err(CoercionFailure::IncompatibleValue) ); }
    #[test] fn hexadecimal_syntax_is_rejected() { assert_eq!( parse_number("0x10"), Err(CoercionFailure::IncompatibleValue) ); }
    #[test] fn arbitrary_text_is_rejected() { assert_eq!( parse_number("eighteen"), Err(CoercionFailure::IncompatibleValue) ); }
    #[test] fn integer_syntax_detection_is_strict() { assert!(is_integer_syntax("18")); assert!(is_integer_syntax("+18")); assert!(is_integer_syntax("-18")); assert!(is_integer_syntax("018")); assert!(!is_integer_syntax("")); assert!(!is_integer_syntax("+")); assert!(!is_integer_syntax("-")); assert!(!is_integer_syntax("18.0")); assert!(!is_integer_syntax("1e3")); assert!(!is_integer_syntax("1_000")); }
    #[test] fn numeric_string_detection_uses_the_full_parser() { assert!(is_numeric_string("18")); assert!(is_numeric_string("-18")); assert!(is_numeric_string("18.5")); assert!(is_numeric_string("1e3")); assert!(!is_numeric_string("")); assert!(!is_numeric_string(" 18")); assert!(!is_numeric_string("NaN")); assert!(!is_numeric_string("unknown")); }
    #[test] fn equal_signed_numbers_require_no_conversion() { let pair = coerce_number_pair( CoercedNumber::Signed(18), CoercedNumber::Signed(19), CoercionPolicy::Strict, ) .expect("matching representations must be accepted"); assert_eq!( pair.into_tuple(), (CoercedNumber::Signed(18), CoercedNumber::Signed(19),) ); }
    #[test] fn strict_policy_rejects_distinct_numeric_representations() { assert_eq!( coerce_number_pair( CoercedNumber::Signed(18), CoercedNumber::Unsigned(18), CoercionPolicy::Strict, ), Err(CoercionFailure::ForbiddenByPolicy) ); }
    #[test] fn small_unsigned_value_can_join_signed_representation() { let pair = coerce_number_pair( CoercedNumber::Signed(-1), CoercedNumber::Unsigned(18), CoercionPolicy::Numeric, ) .expect("18 fits in i64"); assert_eq!( pair.into_tuple(), (CoercedNumber::Signed(-1), CoercedNumber::Signed(18),) ); }
    #[test] fn large_unsigned_value_can_join_unsigned_positive_signed_value() { let pair = coerce_number_pair( CoercedNumber::Signed(18), CoercedNumber::Unsigned(u64::MAX), CoercionPolicy::Numeric, ) .expect("positive signed value fits in u64"); assert_eq!( pair.into_tuple(), ( CoercedNumber::Unsigned(18), CoercedNumber::Unsigned(u64::MAX), ) ); }
    #[test] fn negative_signed_and_large_unsigned_are_not_coercible() { assert_eq!( coerce_number_pair( CoercedNumber::Signed(-1), CoercedNumber::Unsigned(u64::MAX), CoercionPolicy::Numeric, ), Err(CoercionFailure::OutOfRange) ); }
    #[test] fn exactly_representable_signed_integer_can_join_float() { let pair = coerce_number_pair( CoercedNumber::Signed(18), CoercedNumber::Float(18.5), CoercionPolicy::Numeric, ) .expect("18 is exactly representable as f64"); assert_eq!( pair.into_tuple(), (CoercedNumber::Float(18.0), CoercedNumber::Float(18.5),) ); }
    #[test] fn imprecise_signed_integer_cannot_join_float() { let value = 9_007_199_254_740_993_i64; assert_eq!( coerce_number_pair( CoercedNumber::Signed(value), CoercedNumber::Float(1.0), CoercionPolicy::Numeric, ), Err(CoercionFailure::PrecisionLoss) ); }
    #[test] fn imprecise_unsigned_integer_cannot_join_float() { let value = 9_007_199_254_740_993_u64; assert_eq!( coerce_number_pair( CoercedNumber::Unsigned(value), CoercedNumber::Float(1.0), CoercionPolicy::Numeric, ), Err(CoercionFailure::PrecisionLoss) ); }
    #[test] fn value_pair_supports_implicit_string_to_number() { let left = Value::from(18_i64); let right = Value::from("18"); let pair = coerce_value_pair_to_numbers(&left, &right, CoercionPolicy::Implicit) .expect("the string must be interpreted as a number"); assert_eq!( pair.into_tuple(), (CoercedNumber::Signed(18), CoercedNumber::Signed(18),) ); }
    #[test] fn value_pair_rejects_string_under_numeric_policy() { let left = Value::from(18_i64); let right = Value::from("18"); assert_eq!( coerce_value_pair_to_numbers(&left, &right, CoercionPolicy::Numeric,), Err(CoercionFailure::ForbiddenByPolicy) ); }
    #[test] fn coerced_number_converts_back_to_number() { assert_eq!(CoercedNumber::Signed(-1).into_number(), Number::Signed(-1)); assert_eq!( CoercedNumber::Unsigned(1).into_number(), Number::Unsigned(1) ); assert_eq!(CoercedNumber::Float(1.5).into_number(), Number::Float(1.5)); }
    #[test] fn policy_and_failure_predicates_are_consistent() { assert!(CoercionPolicy::Strict.is_strict()); assert!(!CoercionPolicy::Numeric.is_strict()); assert!(!CoercionPolicy::Implicit.is_strict()); assert!(CoercionFailure::ForbiddenByPolicy.is_forbidden_by_policy()); assert!(CoercionFailure::IncompatibleValue.is_incompatible_value()); assert!(CoercionFailure::OutOfRange.is_out_of_range()); assert!(CoercionFailure::PrecisionLoss.is_precision_loss()); }
    #[test] fn coerced_number_reports_its_category_and_finiteness() { assert!(CoercedNumber::Signed(-1).is_integer()); assert!(CoercedNumber::Unsigned(1).is_integer()); assert!(!CoercedNumber::Float(1.5).is_integer()); assert!(CoercedNumber::Float(1.5).is_float()); assert!(CoercedNumber::Float(1.5).is_finite()); assert!(!CoercedNumber::Float(f64::INFINITY).is_finite()); }
    #[test] fn coerced_pair_exposes_common_kind_and_physical_numbers() { let pair = CoercedNumberPair::new(CoercedNumber::Signed(18), CoercedNumber::Signed(20)); assert!(pair.has_common_kind()); assert_eq!( pair.into_numbers(), (Number::Signed(18), Number::Signed(20)), ); let heterogeneous = CoercedNumberPair::new(CoercedNumber::Signed(18), CoercedNumber::Float(18.0)); assert!(!heterogeneous.has_common_kind()); }
    #[test] fn number_convenience_helpers_delegate_to_core_coercion() { let pair = coerce_numbers( Number::Signed(18), Number::Unsigned(20), CoercionPolicy::Numeric, ) .expect("the numeric policy must reconcile compatible integers"); assert_eq!( pair.into_tuple(), (CoercedNumber::Signed(18), CoercedNumber::Signed(20)), ); assert_eq!(parse_number_value("18.5"), Ok(Number::Float(18.5)),); }
}
