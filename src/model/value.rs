//! Runtime value types and numeric representation.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::fmt;
use std::sync::Arc;

use crate::model::Document;
use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Number(Number),
    String(Arc<str>),
    Array(Arc<[Value]>),
    Object(Arc<Document>),
}

impl Value {
    #[must_use]
    pub const fn null() -> Self { Self::Null }

    #[must_use]
    pub const fn bool(value: bool) -> Self { Self::Bool(value) }

    #[must_use]
    pub const fn signed(value: i64) -> Self { Self::Number(Number::Signed(value)) }

    #[must_use]
    pub const fn unsigned(value: u64) -> Self { Self::Number(Number::Unsigned(value)) }

    pub fn float(value: f64) -> Result<Self> { Number::float(value).map(Self::Number) }

    #[must_use]
    pub fn string(value: impl Into<Arc<str>>) -> Self { Self::String(value.into()) }

    #[must_use]
    pub fn array(values: impl IntoIterator<Item = Value>) -> Self {
        Self::Array(Arc::from(values.into_iter().collect::<Vec<_>>()))
    }

    #[must_use]
    pub const fn is_null(&self) -> bool { matches!(self, Self::Null) }

    #[must_use]
    pub const fn is_bool(&self) -> bool { matches!(self, Self::Bool(_)) }

    #[must_use]
    pub const fn is_number(&self) -> bool { matches!(self, Self::Number(_)) }

    #[must_use]
    pub const fn is_string(&self) -> bool { matches!(self, Self::String(_)) }

    #[must_use]
    pub const fn is_array(&self) -> bool { matches!(self, Self::Array(_)) }

    #[must_use]
    pub const fn is_object(&self) -> bool { matches!(self, Self::Object(_)) }

    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> { match self { Self::Bool(v) => Some(*v), _ => None } }

    #[must_use]
    pub const fn as_number(&self) -> Option<&Number> { match self { Self::Number(v) => Some(v), _ => None } }

    #[must_use]
    pub fn as_str(&self) -> Option<&str> { match self { Self::String(v) => Some(v), _ => None } }

    #[must_use]
    pub fn as_string_arc(&self) -> Option<&Arc<str>> { match self { Self::String(v) => Some(v), _ => None } }

    #[must_use]
    pub fn into_string(self) -> Option<Arc<str>> { match self { Self::String(v) => Some(v), _ => None } }

    #[must_use]
    pub fn as_array(&self) -> Option<&[Value]> { match self { Self::Array(v) => Some(v), _ => None } }

    #[must_use]
    pub fn as_array_arc(&self) -> Option<&Arc<[Value]>> { match self { Self::Array(v) => Some(v), _ => None } }

    #[must_use]
    pub fn into_array(self) -> Option<Arc<[Value]>> { match self { Self::Array(v) => Some(v), _ => None } }

    #[must_use]
    pub fn object(document: Document) -> Self { Self::Object(Arc::new(document)) }

    #[must_use]
    pub fn as_object(&self) -> Option<&Document> { match self { Self::Object(v) => Some(v.as_ref()), _ => None } }

    #[must_use]
    pub fn as_object_arc(&self) -> Option<&Arc<Document>> { match self { Self::Object(v) => Some(v), _ => None } }

    #[must_use]
    pub fn into_object(self) -> Option<Arc<Document>> { match self { Self::Object(v) => Some(v), _ => None } }

    #[must_use]
    pub fn into_number(self) -> Option<Number> { match self { Self::Number(v) => Some(v), _ => None } }

    #[must_use]
    pub const fn physical_kind(&self) -> PhysicalKind {
        match self {
            Self::Null => PhysicalKind::Null,
            Self::Bool(_) => PhysicalKind::Bool,
            Self::Number(_) => PhysicalKind::Number,
            Self::String(_) => PhysicalKind::String,
            Self::Array(_) => PhysicalKind::Array,
            Self::Object(_) => PhysicalKind::Object,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum Number {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

impl Number {
    #[must_use]
    pub const fn signed(value: i64) -> Self { Self::Signed(value) }

    #[must_use]
    pub const fn unsigned(value: u64) -> Self { Self::Unsigned(value) }

    pub fn float(value: f64) -> Result<Self> {
        if value.is_finite() { Ok(Self::Float(normalize_zero(value))) }
        else { Err(Error::NonFiniteNumber { value }) }
    }

    #[must_use]
    pub const fn as_signed(self) -> Option<i64> { match self { Self::Signed(v) => Some(v), _ => None } }

    #[must_use]
    pub const fn as_unsigned(self) -> Option<u64> { match self { Self::Unsigned(v) => Some(v), _ => None } }

    #[must_use]
    pub const fn as_float(self) -> Option<f64> { match self { Self::Float(v) => Some(v), _ => None } }

    #[must_use]
    pub const fn physical_kind(self) -> NumberKind {
        match self {
            Self::Signed(_) => NumberKind::Signed,
            Self::Unsigned(_) => NumberKind::Unsigned,
            Self::Float(_) => NumberKind::Float,
        }
    }

    #[must_use]
    pub const fn is_integer(self) -> bool { matches!(self, Self::Signed(_) | Self::Unsigned(_)) }

    #[must_use]
    pub const fn is_signed(self) -> bool { matches!(self, Self::Signed(_)) }

    #[must_use]
    pub const fn is_unsigned(self) -> bool { matches!(self, Self::Unsigned(_)) }

    #[must_use]
    pub const fn is_float(self) -> bool { matches!(self, Self::Float(_)) }

    #[must_use]
    pub const fn into_value(self) -> Value { Value::Number(self) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PhysicalKind {
    Null, Bool, Number, String, Array, Object,
}

impl PhysicalKind {
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Null => "null", Self::Bool => "bool", Self::Number => "number",
            Self::String => "string", Self::Array => "array", Self::Object => "object",
        }
    }

    #[must_use]
    pub const fn is_scalar(self) -> bool { matches!(self, Self::Null | Self::Bool | Self::Number | Self::String) }

    #[must_use]
    pub const fn is_composite(self) -> bool { matches!(self, Self::Array | Self::Object) }
}

impl fmt::Display for PhysicalKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum NumberKind {
    Signed, Unsigned, Float,
}

impl NumberKind {
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Signed => "signed", Self::Unsigned => "unsigned", Self::Float => "float",
        }
    }

    #[must_use]
    pub const fn is_integer(self) -> bool { matches!(self, Self::Signed | Self::Unsigned) }

    #[must_use]
    pub const fn is_float(self) -> bool { matches!(self, Self::Float) }
}

impl fmt::Display for NumberKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
}

fn normalize_zero(value: f64) -> f64 { if value == 0.0 { 0.0 } else { value } }

impl From<i8> for Number { fn from(v: i8) -> Self { Self::Signed(i64::from(v)) } }
impl From<i16> for Number { fn from(v: i16) -> Self { Self::Signed(i64::from(v)) } }
impl From<i32> for Number { fn from(v: i32) -> Self { Self::Signed(i64::from(v)) } }
impl From<i64> for Number { fn from(v: i64) -> Self { Self::Signed(v) } }
impl From<u8> for Number { fn from(v: u8) -> Self { Self::Unsigned(u64::from(v)) } }
impl From<u16> for Number { fn from(v: u16) -> Self { Self::Unsigned(u64::from(v)) } }
impl From<u32> for Number { fn from(v: u32) -> Self { Self::Unsigned(u64::from(v)) } }
impl From<u64> for Number { fn from(v: u64) -> Self { Self::Unsigned(v) } }
impl TryFrom<f32> for Number { type Error = Error; fn try_from(v: f32) -> Result<Self> { Self::float(f64::from(v)) } }
impl TryFrom<f64> for Number { type Error = Error; fn try_from(v: f64) -> Result<Self> { Self::float(v) } }

impl From<&Document> for Value { fn from(d: &Document) -> Self { Self::object(d.clone()) } }
impl From<Document> for Value { fn from(d: Document) -> Self { Self::object(d) } }
impl From<Arc<Document>> for Value { fn from(d: Arc<Document>) -> Self { Self::Object(d) } }
impl From<()> for Value { fn from((): ()) -> Self { Self::Null } }
impl From<bool> for Value { fn from(v: bool) -> Self { Self::Bool(v) } }
impl From<Number> for Value { fn from(v: Number) -> Self { Self::Number(v) } }
impl From<i8> for Value { fn from(v: i8) -> Self { Self::Number(Number::from(v)) } }
impl From<i16> for Value { fn from(v: i16) -> Self { Self::Number(Number::from(v)) } }
impl From<i32> for Value { fn from(v: i32) -> Self { Self::Number(Number::from(v)) } }
impl From<i64> for Value { fn from(v: i64) -> Self { Self::Number(Number::from(v)) } }
impl From<u8> for Value { fn from(v: u8) -> Self { Self::Number(Number::from(v)) } }
impl From<u16> for Value { fn from(v: u16) -> Self { Self::Number(Number::from(v)) } }
impl From<u32> for Value { fn from(v: u32) -> Self { Self::Number(Number::from(v)) } }
impl From<u64> for Value { fn from(v: u64) -> Self { Self::Number(Number::from(v)) } }
impl TryFrom<f32> for Value { type Error = Error; fn try_from(v: f32) -> Result<Self> { Number::try_from(v).map(Self::Number) } }
impl TryFrom<f64> for Value { type Error = Error; fn try_from(v: f64) -> Result<Self> { Number::try_from(v).map(Self::Number) } }
impl From<&str> for Value { fn from(v: &str) -> Self { Self::string(v) } }
impl From<String> for Value { fn from(v: String) -> Self { Self::string(v) } }
impl From<Arc<str>> for Value { fn from(v: Arc<str>) -> Self { Self::String(v) } }
impl From<Vec<Value>> for Value { fn from(v: Vec<Value>) -> Self { Self::Array(Arc::from(v)) } }
impl From<Box<[Value]>> for Value { fn from(v: Box<[Value]>) -> Self { Self::Array(Arc::from(v)) } }
impl From<Arc<[Value]>> for Value { fn from(v: Arc<[Value]>) -> Self { Self::Array(v) } }
impl<const LENGTH: usize> From<[Value; LENGTH]> for Value { fn from(v: [Value; LENGTH]) -> Self { Self::Array(Arc::from(v)) } }

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test] fn null_constructor_creates_null() { assert_eq!(Value::null(), Value::Null); assert!(Value::null().is_null()); }
    #[test] fn unit_converts_to_null() { assert_eq!(Value::from(()), Value::Null); }
    #[test] fn bool_constructor_preserves_value() { assert_eq!(Value::bool(true), Value::Bool(true)); assert_eq!(Value::bool(false), Value::Bool(false)); }
    #[test] fn bool_accessor_only_accepts_bool_values() { assert_eq!(Value::Bool(true).as_bool(), Some(true)); assert_eq!(Value::Null.as_bool(), None); }
    #[test] fn signed_integer_preserves_its_physical_representation() { let value = Value::from(-42_i64); assert_eq!(value, Value::Number(Number::Signed(-42))); assert_eq!(value.as_number(), Some(&Number::Signed(-42))); }
    #[test] fn unsigned_integer_preserves_its_physical_representation() { assert_eq!(Value::from(u64::MAX), Value::Number(Number::Unsigned(u64::MAX))); }
    #[test] fn signed_and_unsigned_numbers_are_physically_distinct() { assert_ne!(Number::Signed(1), Number::Unsigned(1)); assert_ne!(Value::from(1_i64), Value::from(1_u64)); }
    #[test] fn integer_and_float_numbers_are_physically_distinct() { let float = Value::try_from(1.0_f64).expect("1.0 must be finite"); assert_ne!(Value::from(1_i64), float); }
    #[test] fn finite_float_is_accepted() { let value = Value::float(42.5).expect("42.5 must be finite"); assert_eq!(value, Value::Number(Number::Float(42.5))); }
    #[test] fn positive_infinity_is_rejected() { let error = Value::float(f64::INFINITY).expect_err("positive infinity must be rejected"); assert!(matches!(error, Error::NonFiniteNumber { value } if value == f64::INFINITY)); }
    #[test] fn negative_infinity_is_rejected() { let error = Value::float(f64::NEG_INFINITY).expect_err("negative infinity must be rejected"); assert!(matches!(error, Error::NonFiniteNumber { value } if value == f64::NEG_INFINITY)); }
    #[test] fn nan_is_rejected() { let error = Value::float(f64::NAN).expect_err("NaN must be rejected"); assert!(matches!(error, Error::NonFiniteNumber { value } if value.is_nan())); }
    #[test] fn negative_zero_is_normalized() { let value = Number::float(-0.0).expect("-0.0 must be finite"); assert_eq!(value, Number::Float(0.0)); let Number::Float(value) = value else { panic!("expected a floating-point number") }; assert_eq!(value.to_bits(), 0.0_f64.to_bits()); }
    #[test] fn string_constructor_preserves_text() { let value = Value::string("OG"); assert_eq!(value.as_str(), Some("OG")); assert_eq!(value.physical_kind(), PhysicalKind::String); }
    #[test] fn owned_string_converts_to_value() { let source = String::from("OG"); let value = Value::from(source); assert_eq!(value.as_str(), Some("OG")); }
    #[test] fn arc_string_is_reused() { let source: Arc<str> = Arc::from("shared"); let value = Value::from(source.clone()); let Value::String(stored) = value else { panic!("expected a string value") }; assert!(Arc::ptr_eq(&source, &stored)); }
    #[test] fn array_constructor_preserves_values() { let value = Value::array([Value::from(1_i64), Value::from("two"), Value::Null]); assert_eq!(value.as_array(), Some([Value::from(1_i64), Value::from("two"), Value::Null,].as_slice())); }
    #[test] fn vector_converts_to_array() { let value = Value::from(vec![Value::from(true), Value::from(false)]); assert_eq!(value.as_array(), Some([Value::Bool(true), Value::Bool(false)].as_slice())); }
    #[test] fn cloned_string_values_share_their_allocation() { let original = Value::string("shared"); let cloned = original.clone(); let (Value::String(original), Value::String(cloned)) = (original, cloned) else { panic!("expected string values") }; assert!(Arc::ptr_eq(&original, &cloned)); }
    #[test] fn cloned_array_values_share_their_allocation() { let original = Value::array([Value::from(1_i64), Value::from(2_i64)]); let cloned = original.clone(); let (Value::Array(original), Value::Array(cloned)) = (original, cloned) else { panic!("expected array values") }; assert!(Arc::ptr_eq(&original, &cloned)); }
    #[test] fn value_predicates_match_physical_variants() { assert!(Value::Null.is_null()); assert!(Value::Bool(true).is_bool()); assert!(Value::from(1_i64).is_number()); assert!(Value::from("text").is_string()); assert!(Value::array([]).is_array()); assert!(Value::from(Document::new()).is_object()); assert!(!Value::Null.is_object()); assert!(!Value::from("text").is_number()); }
    #[test] fn owned_accessors_preserve_shared_allocations() { let string: Arc<str> = Arc::from("shared"); let value = Value::from(string.clone()); let extracted = value.into_string().expect("expected string"); assert!(Arc::ptr_eq(&string, &extracted)); let array: Arc<[Value]> = Arc::from([Value::from(1_i64)]); let value = Value::from(array.clone()); let extracted = value.into_array().expect("expected array"); assert!(Arc::ptr_eq(&array, &extracted)); let object = Arc::new(Document::from_fields([("a", Value::from(1_i64))])); let value = Value::from(object.clone()); let extracted = value.into_object().expect("expected object"); assert!(Arc::ptr_eq(&object, &extracted)); }
    #[test] fn number_helpers_preserve_physical_representation() { assert!(Number::Signed(-1).is_integer()); assert!(Number::Signed(-1).is_signed()); assert!(Number::Unsigned(1).is_unsigned()); assert!(Number::Float(1.5).is_float()); assert_eq!(Number::Unsigned(1).into_value(), Value::Number(Number::Unsigned(1))); assert_eq!(Value::Number(Number::Signed(-1)).into_number(), Some(Number::Signed(-1))); }
    #[test] fn kind_helpers_distinguish_scalar_and_composite_values() { assert!(PhysicalKind::Null.is_scalar()); assert!(PhysicalKind::String.is_scalar()); assert!(PhysicalKind::Array.is_composite()); assert!(PhysicalKind::Object.is_composite()); assert!(NumberKind::Signed.is_integer()); assert!(NumberKind::Unsigned.is_integer()); assert!(NumberKind::Float.is_float()); }
    #[test] fn borrowed_document_converts_by_cloning() { let document = Document::from_fields([("name", Value::from("Tom"))]); let value = Value::from(&document); assert_eq!(value.as_object(), Some(&document)); }
    #[test] fn physical_kind_names_are_stable() { assert_eq!(PhysicalKind::Null.as_str(), "null"); assert_eq!(PhysicalKind::Bool.as_str(), "bool"); assert_eq!(PhysicalKind::Number.as_str(), "number"); assert_eq!(PhysicalKind::String.as_str(), "string"); assert_eq!(PhysicalKind::Array.as_str(), "array"); }
    #[test] fn number_kind_names_are_stable() { assert_eq!(NumberKind::Signed.as_str(), "signed"); assert_eq!(NumberKind::Unsigned.as_str(), "unsigned"); assert_eq!(NumberKind::Float.as_str(), "float"); }
    #[test] fn number_accessors_do_not_perform_coercion() { assert_eq!(Number::Signed(-1).as_signed(), Some(-1)); assert_eq!(Number::Signed(-1).as_unsigned(), None); assert_eq!(Number::Signed(-1).as_float(), None); assert_eq!(Number::Unsigned(1).as_signed(), None); assert_eq!(Number::Unsigned(1).as_unsigned(), Some(1)); assert_eq!(Number::Unsigned(1).as_float(), None); assert_eq!(Number::Float(1.5).as_signed(), None); assert_eq!(Number::Float(1.5).as_unsigned(), None); assert_eq!(Number::Float(1.5).as_float(), Some(1.5)); }
}