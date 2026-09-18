#![cfg_attr(rustfmt, rustfmt_skip)]
//! Value capability definitions and capability checks.
//! A capability represents a family of operations potentially applicable to a value.

use std::fmt;
use std::iter::FusedIterator;

use crate::{Number, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Capability {
    Comparable,
    Summable,
    Temporal,
    Searchable,
}

impl Capability {
    /// Retourne le nom stable de la capacité.
    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Comparable => "comparable",
            Self::Summable => "summable",
            Self::Temporal => "temporal",
            Self::Searchable => "searchable",
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Capabilities {
    bits: u8,
}

impl Capabilities {
    const COMPARABLE_BIT: u8 = 1 << 0;
    const SUMMABLE_BIT: u8 = 1 << 1;
    const TEMPORAL_BIT: u8 = 1 << 2;
    const SEARCHABLE_BIT: u8 = 1 << 3;

    const KNOWN_BITS: u8 =
        Self::COMPARABLE_BIT | Self::SUMMABLE_BIT | Self::TEMPORAL_BIT | Self::SEARCHABLE_BIT;

    pub const NONE: Self = Self { bits: 0 };

    pub const ALL: Self = Self {
        bits: Self::KNOWN_BITS,
    };

    pub const COMPARABLE: Self = Self {
        bits: Self::COMPARABLE_BIT,
    };

    pub const SUMMABLE: Self = Self {
        bits: Self::SUMMABLE_BIT,
    };

    pub const TEMPORAL: Self = Self {
        bits: Self::TEMPORAL_BIT,
    };

    pub const SEARCHABLE: Self = Self {
        bits: Self::SEARCHABLE_BIT,
    };

    #[must_use]
    pub const fn empty() -> Self {
        Self::NONE
    }

    #[must_use]
    pub const fn from_capability(capability: Capability) -> Self {
        Self {
            bits: bit_for(capability),
        }
    }

    #[must_use]
    pub fn from_iter(capabilities: impl IntoIterator<Item = Capability>) -> Self {
        capabilities.into_iter().fold(Self::empty(), Self::with)
    }

    #[must_use]
    #[inline]
    pub const fn is_empty(self) -> bool {
        self.bits == 0
    }

    #[must_use]
    #[inline]
    pub const fn len(self) -> usize {
        self.bits.count_ones() as usize
    }

    #[must_use]
    pub const fn contains(self, capability: Capability) -> bool {
        self.bits & bit_for(capability) != 0
    }

    #[must_use]
    pub const fn contains_all(self, required: Self) -> bool {
        self.bits & required.bits == required.bits
    }

    #[must_use]
    pub const fn intersects(self, other: Self) -> bool {
        self.bits & other.bits != 0
    }

    #[must_use]
    pub const fn with(self, capability: Capability) -> Self {
        Self {
            bits: self.bits | bit_for(capability),
        }
    }

    #[must_use]
    pub const fn without(self, capability: Capability) -> Self {
        Self {
            bits: self.bits & !bit_for(capability),
        }
    }

    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self {
            bits: self.bits | other.bits,
        }
    }

    #[must_use]
    pub const fn intersection(self, other: Self) -> Self {
        Self {
            bits: self.bits & other.bits,
        }
    }

    #[must_use]
    pub const fn difference(self, other: Self) -> Self {
        Self {
            bits: self.bits & !other.bits,
        }
    }

    /// Return an iteractor in a fixed order :
    /// 1. [`Capability::Comparable`]
    /// 2. [`Capability::Summable`]
    /// 3. [`Capability::Temporal`]
    /// 4. [`Capability::Searchable`]
    #[must_use]
    pub const fn iter(self) -> Iter {
        Iter {
            capabilities: self,
            index: 0,
        }
    }

    #[must_use]
    pub const fn bits(self) -> u8 {
        self.bits
    }
}

impl fmt::Debug for Capabilities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_set().entries(self.iter()).finish()
    }
}

impl fmt::Display for Capabilities {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut capabilities = self.iter();

        if let Some(first) = capabilities.next() {
            write!(formatter, "{first}")?;
        }

        for capability in capabilities {
            write!(formatter, ", {capability}")?;
        }

        Ok(())
    }
}

impl IntoIterator for Capabilities {
    type Item = Capability;
    type IntoIter = Iter;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl From<Capability> for Capabilities {
    fn from(capability: Capability) -> Self {
        Self::from_capability(capability)
    }
}

impl FromIterator<Capability> for Capabilities {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Capability>,
    {
        Self::from_iter(iter)
    }
}

#[derive(Debug, Clone)]
pub struct Iter {
    capabilities: Capabilities,
    index: u8,
}

impl Iterator for Iter {
    type Item = Capability;

    fn next(&mut self) -> Option<Self::Item> {
        const ORDERED_CAPABILITIES: [Capability; 4] = [
            Capability::Comparable,
            Capability::Summable,
            Capability::Temporal,
            Capability::Searchable,
        ];

        while usize::from(self.index) < ORDERED_CAPABILITIES.len() {
            let capability = ORDERED_CAPABILITIES[usize::from(self.index)];
            self.index += 1;

            if self.capabilities.contains(capability) {
                return Some(capability);
            }
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.clone().count();

        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for Iter {
    #[inline]
    fn len(&self) -> usize {
        self.clone().count()
    }
}

impl FusedIterator for Iter {}

#[must_use]
pub const fn capabilities_of(value: &Value) -> Capabilities {
    match value {
        Value::Null => Capabilities::NONE,
        Value::Bool(_) | Value::String(_) => {
            Capabilities::COMPARABLE.union(Capabilities::SEARCHABLE)
        }
        Value::Number(number) => capabilities_of_number(*number),
        Value::Array(_) | Value::Object(_) => Capabilities::SEARCHABLE,
    }
}

#[must_use]
pub const fn capabilities_of_number(_number: Number) -> Capabilities {
    Capabilities::COMPARABLE
        .union(Capabilities::SUMMABLE)
        .union(Capabilities::SEARCHABLE)
}

pub trait ValueCapabilities {
	
    #[must_use]
    fn capabilities(&self) -> Capabilities;

    #[must_use]
    fn has_capability(&self, capability: Capability) -> bool {
        self.capabilities().contains(capability)
    }

    #[must_use]
    fn is_comparable(&self) -> bool {
        self.has_capability(Capability::Comparable)
    }

    #[must_use]
    fn is_summable(&self) -> bool {
        self.has_capability(Capability::Summable)
    }

    #[must_use]
    fn is_temporal(&self) -> bool {
        self.has_capability(Capability::Temporal)
    }

    #[must_use]
    fn is_searchable(&self) -> bool {
        self.has_capability(Capability::Searchable)
    }
}

impl ValueCapabilities for Value {
    fn capabilities(&self) -> Capabilities {
        capabilities_of(self)
    }
}

const fn bit_for(capability: Capability) -> u8 {
    match capability {
        Capability::Comparable => Capabilities::COMPARABLE_BIT,
        Capability::Summable => Capabilities::SUMMABLE_BIT,
        Capability::Temporal => Capabilities::TEMPORAL_BIT,
        Capability::Searchable => Capabilities::SEARCHABLE_BIT,
    }
}

#[cfg(test)]
mod tests {
    use crate::Document;

    use super::*;

    #[test] fn capability_names_are_stable() { assert_eq!(Capability::Comparable.as_str(), "comparable"); assert_eq!(Capability::Summable.as_str(), "summable"); assert_eq!(Capability::Temporal.as_str(), "temporal"); assert_eq!(Capability::Searchable.as_str(), "searchable"); }
    #[test] fn empty_capability_set_contains_nothing() { let capabilities = Capabilities::empty(); assert!(capabilities.is_empty()); assert_eq!(capabilities.len(), 0); assert!(!capabilities.contains(Capability::Comparable)); assert!(!capabilities.contains(Capability::Summable)); assert!(!capabilities.contains(Capability::Temporal)); assert!(!capabilities.contains(Capability::Searchable)); }
    #[test] fn capability_can_be_added() { let capabilities = Capabilities::empty() .with(Capability::Comparable) .with(Capability::Searchable); assert_eq!(capabilities.len(), 2); assert!(capabilities.contains(Capability::Comparable)); assert!(capabilities.contains(Capability::Searchable)); assert!(!capabilities.contains(Capability::Summable)); }
    #[test] fn adding_a_capability_is_idempotent() { let capabilities = Capabilities::empty() .with(Capability::Comparable) .with(Capability::Comparable); assert_eq!(capabilities.len(), 1); }
    #[test] fn capability_can_be_removed() { let capabilities = Capabilities::ALL.without(Capability::Temporal); assert!(!capabilities.contains(Capability::Temporal)); assert!(capabilities.contains(Capability::Comparable)); assert!(capabilities.contains(Capability::Summable)); assert!(capabilities.contains(Capability::Searchable)); }
    #[test] fn union_combines_capabilities() { let left = Capabilities::COMPARABLE; let right = Capabilities::SEARCHABLE; let union = left.union(right); assert!(union.contains(Capability::Comparable)); assert!(union.contains(Capability::Searchable)); assert_eq!(union.len(), 2); }
    #[test] fn intersection_keeps_shared_capabilities() { let left = Capabilities::COMPARABLE.union(Capabilities::SUMMABLE); let right = Capabilities::SUMMABLE.union(Capabilities::SEARCHABLE); let intersection = left.intersection(right); assert_eq!(intersection, Capabilities::SUMMABLE); }
    #[test] fn difference_removes_shared_capabilities() { let left = Capabilities::COMPARABLE .union(Capabilities::SUMMABLE) .union(Capabilities::SEARCHABLE); let difference = left.difference(Capabilities::SUMMABLE); assert!(difference.contains(Capability::Comparable)); assert!(!difference.contains(Capability::Summable)); assert!(difference.contains(Capability::Searchable)); }
    #[test] fn contains_all_checks_a_required_set() { let capabilities = Capabilities::COMPARABLE.union(Capabilities::SEARCHABLE); assert!(capabilities.contains_all(Capabilities::COMPARABLE)); assert!(capabilities.contains_all(Capabilities::COMPARABLE.union(Capabilities::SEARCHABLE))); assert!(!capabilities.contains_all(Capabilities::SUMMABLE)); }
    #[test] fn intersects_detects_any_shared_capability() { let capabilities = Capabilities::COMPARABLE.union(Capabilities::SEARCHABLE); assert!(capabilities.intersects(Capabilities::SEARCHABLE)); assert!(!capabilities.intersects(Capabilities::SUMMABLE.union(Capabilities::TEMPORAL))); }
    #[test] fn iteration_order_is_stable() { let capabilities = Capabilities::ALL; let values = capabilities.into_iter().collect::<Vec<_>>(); assert_eq!( values, vec![ Capability::Comparable, Capability::Summable, Capability::Temporal, Capability::Searchable, ] ); }
    #[test] fn debug_format_lists_capabilities() { let capabilities = Capabilities::COMPARABLE.union(Capabilities::SEARCHABLE); assert_eq!(format!("{capabilities:?}"), "{Comparable, Searchable}"); }
    #[test] fn display_format_lists_stable_names() { let capabilities = Capabilities::COMPARABLE.union(Capabilities::SEARCHABLE); assert_eq!(capabilities.to_string(), "comparable, searchable"); }
    #[test] fn null_exposes_no_capability() { let value = Value::Null; assert_eq!(value.capabilities(), Capabilities::NONE); assert!(!value.is_comparable()); assert!(!value.is_summable()); assert!(!value.is_temporal()); assert!(!value.is_searchable()); }
    #[test] fn bool_is_comparable_and_searchable() { let value = Value::from(true); assert_eq!( value.capabilities(), Capabilities::COMPARABLE.union(Capabilities::SEARCHABLE) ); assert!(value.is_comparable()); assert!(value.is_searchable()); assert!(!value.is_summable()); assert!(!value.is_temporal()); }
    #[test] fn signed_number_is_comparable_summable_and_searchable() { let value = Value::from(-42_i64); assert_eq!( value.capabilities(), Capabilities::COMPARABLE .union(Capabilities::SUMMABLE) .union(Capabilities::SEARCHABLE) ); }
    #[test] fn unsigned_number_is_comparable_summable_and_searchable() { let value = Value::from(42_u64); assert_eq!( value.capabilities(), Capabilities::COMPARABLE .union(Capabilities::SUMMABLE) .union(Capabilities::SEARCHABLE) ); }
    #[test] fn float_is_comparable_summable_and_searchable() { let value = Value::float(42.5).expect("42.5 must be finite"); assert_eq!( value.capabilities(), Capabilities::COMPARABLE .union(Capabilities::SUMMABLE) .union(Capabilities::SEARCHABLE) ); }
    #[test] fn string_is_comparable_and_searchable() { let value = Value::from("OG"); assert_eq!( value.capabilities(), Capabilities::COMPARABLE.union(Capabilities::SEARCHABLE) ); }
    #[test] fn array_is_searchable_only() { let value = Value::array([Value::from(1_i64), Value::from(2_i64)]); assert_eq!(value.capabilities(), Capabilities::SEARCHABLE); assert!(!value.is_comparable()); assert!(!value.is_summable()); assert!(!value.is_temporal()); assert!(value.is_searchable()); }
    #[test] fn object_is_searchable_only() { let value = Value::from(Document::from_fields([("name", Value::from("Tom"))])); assert_eq!(value.capabilities(), Capabilities::SEARCHABLE); }
    #[test] fn no_current_value_is_temporal() { let values = [ Value::Null, Value::from(true), Value::from(42_i64), Value::from("2026-07-27"), Value::array([]), Value::from(Document::new()), ]; assert!(values.iter().all(|value| !value.is_temporal())); }
    #[test] fn capability_set_can_be_collected() { let capabilities = [Capability::Comparable, Capability::Searchable] .into_iter() .collect::<Capabilities>(); assert_eq!( capabilities, Capabilities::COMPARABLE.union(Capabilities::SEARCHABLE) ); }
}
