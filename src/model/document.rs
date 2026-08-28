//! Document storage and field access primitives.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::borrow::Borrow;
use std::collections::{btree_map, BTreeMap};
use std::fmt;
use std::iter::FromIterator;
use std::sync::Arc;
use crate::Value;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldName(Arc<str>);

impl FieldName {
    #[must_use] pub fn new(value: impl Into<Arc<str>>) -> Self { Self(value.into()) }
    #[must_use] pub fn as_str(&self) -> &str { &self.0 }
    #[must_use] pub fn as_arc(&self) -> &Arc<str> { &self.0 }
    #[must_use] pub fn into_arc(self) -> Arc<str> { self.0 }
    #[must_use] pub fn to_owned_string(&self) -> String { self.0.to_string() }
    #[must_use] pub fn is_empty(&self) -> bool { self.0.is_empty() }
}

impl fmt::Debug for FieldName { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_tuple("FieldName").field(&self.0).finish() } }
impl fmt::Display for FieldName { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) } }
impl AsRef<str> for FieldName { fn as_ref(&self) -> &str { self.as_str() } }
impl Borrow<str> for FieldName { fn borrow(&self) -> &str { self.as_str() } }
impl From<&str> for FieldName { fn from(value: &str) -> Self { Self::new(value) } }
impl From<String> for FieldName { fn from(value: String) -> Self { Self::new(value) } }
impl From<Arc<str>> for FieldName { fn from(value: Arc<str>) -> Self { Self(value) } }
impl From<FieldName> for Arc<str> { fn from(value: FieldName) -> Self { value.into_arc() } }

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Document { fields: BTreeMap<FieldName, Value>, }

impl Document {
    #[must_use] #[inline] pub const fn new() -> Self { Self { fields: BTreeMap::new() } }

    #[must_use]
    pub fn from_fields<I, K>(fields: I) -> Self
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<FieldName>,
    {
        let mut document = Self::new();
        document.extend(fields);
        document
    }

    #[must_use] #[inline] pub fn len(&self) -> usize { self.fields.len() }
    #[must_use] #[inline] pub fn is_empty(&self) -> bool { self.fields.is_empty() }
    pub fn contains_key(&self, name: &str) -> bool { self.fields.contains_key(name) }
    pub fn get(&self, name: &str) -> Option<&Value> { self.fields.get(name) }
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Value> { self.fields.get_mut(name) }
    pub fn entry(&mut self, name: impl Into<FieldName>) -> btree_map::Entry<'_, FieldName, Value> { self.fields.entry(name.into()) }
    pub fn iter_mut(&mut self) -> IterMut<'_> { IterMut { inner: self.fields.iter_mut() }}
    pub fn insert(&mut self, name: impl Into<FieldName>, value: impl Into<Value>) -> Option<Value> { self.fields.insert(name.into(), value.into()) }
    pub fn remove(&mut self, name: &str) -> Option<Value> { self.fields.remove(name) }
    pub fn clear(&mut self) { self.fields.clear(); }
    pub fn retain(&mut self, mut predicate: impl FnMut(&FieldName, &mut Value) -> bool) { self.fields.retain(|name, value| predicate(name, value)); }
    pub fn append(&mut self, other: &mut Document) { self.fields.append(&mut other.fields); }
    pub fn iter(&self) -> Iter<'_> { Iter { inner: self.fields.iter() } }
    pub fn keys(&self) -> Keys<'_> { Keys { inner: self.fields.keys() } }
    pub fn values(&self) -> Values<'_> { Values { inner: self.fields.values() } }
    pub fn values_mut(&mut self) -> ValuesMut<'_> { ValuesMut { inner: self.fields.values_mut() } }
    pub fn into_iter_fields(self) -> IntoIter { IntoIter { inner: self.fields.into_iter() } }
    #[must_use] pub fn as_map(&self) -> &BTreeMap<FieldName, Value> { &self.fields }
    #[must_use] pub fn into_map(self) -> BTreeMap<FieldName, Value> { self.fields }

    pub fn extend<I, K>(&mut self, fields: I)
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<FieldName>,
    {
        self.fields.extend(fields.into_iter().map(|(name, value)| (name.into(), value)));
    }
}

#[derive(Debug)]
pub struct Iter<'a> { inner: btree_map::Iter<'a, FieldName, Value>, }

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a FieldName, &'a Value);
    fn next(&mut self) -> Option<Self::Item> { self.inner.next() }
    fn size_hint(&self) -> (usize, Option<usize>) { self.inner.size_hint() }
}

impl DoubleEndedIterator for Iter<'_> { fn next_back(&mut self) -> Option<Self::Item> { self.inner.next_back() } }
impl ExactSizeIterator for Iter<'_> { fn len(&self) -> usize { self.inner.len() } }
impl std::iter::FusedIterator for Iter<'_> {}

#[derive(Debug)]
pub struct IterMut<'a> { inner: btree_map::IterMut<'a, FieldName, Value>, }

impl<'a> Iterator for IterMut<'a> {
    type Item = (&'a FieldName, &'a mut Value);
    fn next(&mut self) -> Option<Self::Item> { self.inner.next() }
    fn size_hint(&self) -> (usize, Option<usize>) { self.inner.size_hint() }
}

impl DoubleEndedIterator for IterMut<'_> {
    fn next_back(&mut self) -> Option<Self::Item> { self.inner.next_back() }
}

impl ExactSizeIterator for IterMut<'_> {
    fn len(&self) -> usize { self.inner.len() }
}

impl std::iter::FusedIterator for IterMut<'_> {}

#[derive(Debug)]
pub struct Keys<'a> { inner: btree_map::Keys<'a, FieldName, Value>, }

impl<'a> Iterator for Keys<'a> {
    type Item = &'a FieldName;
    fn next(&mut self) -> Option<Self::Item> { self.inner.next() }
    fn size_hint(&self) -> (usize, Option<usize>) { self.inner.size_hint() }
}

impl DoubleEndedIterator for Keys<'_> { fn next_back(&mut self) -> Option<Self::Item> { self.inner.next_back() } }
impl ExactSizeIterator for Keys<'_> { fn len(&self) -> usize { self.inner.len() } }
impl std::iter::FusedIterator for Keys<'_> {}

#[derive(Debug)]
pub struct Values<'a> { inner: btree_map::Values<'a, FieldName, Value>, }

impl<'a> Iterator for Values<'a> {
    type Item = &'a Value;
    fn next(&mut self) -> Option<Self::Item> { self.inner.next() }
    fn size_hint(&self) -> (usize, Option<usize>) { self.inner.size_hint() }
}

impl DoubleEndedIterator for Values<'_> {
    fn next_back(&mut self) -> Option<Self::Item> { self.inner.next_back() }
}

impl ExactSizeIterator for Values<'_> {
    fn len(&self) -> usize { self.inner.len() }
}

impl std::iter::FusedIterator for Values<'_> {}

#[derive(Debug)]
pub struct ValuesMut<'a> { inner: btree_map::ValuesMut<'a, FieldName, Value>, }

impl<'a> Iterator for ValuesMut<'a> {
    type Item = &'a mut Value;
    fn next(&mut self) -> Option<Self::Item> { self.inner.next() }
    fn size_hint(&self) -> (usize, Option<usize>) {self.inner.size_hint() }
}

impl DoubleEndedIterator for ValuesMut<'_> {
    fn next_back(&mut self) -> Option<Self::Item> { self.inner.next_back() }
}

impl ExactSizeIterator for ValuesMut<'_> {
    fn len(&self) -> usize { self.inner.len() }
}

impl std::iter::FusedIterator for ValuesMut<'_> {}

#[derive(Debug)]
pub struct IntoIter { inner: btree_map::IntoIter<FieldName, Value>, }

impl Iterator for IntoIter {
    type Item = (FieldName, Value);
    fn next(&mut self) -> Option<Self::Item> { self.inner.next() }
    fn size_hint(&self) -> (usize, Option<usize>) { self.inner.size_hint() }
}

impl DoubleEndedIterator for IntoIter {
    fn next_back(&mut self) -> Option<Self::Item> { self.inner.next_back() }
}

impl ExactSizeIterator for IntoIter {
    fn len(&self) -> usize { self.inner.len() }
}

impl std::iter::FusedIterator for IntoIter {}

impl<K> FromIterator<(K, Value)> for Document
where
    K: Into<FieldName>,
{
    fn from_iter<I>(fields: I) -> Self
    where
        I: IntoIterator<Item = (K, Value)>,
    {
        Self::from_fields(fields)
    }
}

impl<K> Extend<(K, Value)> for Document
where
    K: Into<FieldName>,
{
    fn extend<I>(&mut self, fields: I)
    where
        I: IntoIterator<Item = (K, Value)>,
    {
        Document::extend(self, fields);
    }
}

impl IntoIterator for Document {
    type Item = (FieldName, Value);
    type IntoIter = IntoIter;
    fn into_iter(self) -> Self::IntoIter { self.into_iter_fields() }
}

impl<'a> IntoIterator for &'a Document {
    type Item = (&'a FieldName, &'a Value);
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter { self.iter() }
}

impl<'a> IntoIterator for &'a mut Document {
    type Item = (&'a FieldName, &'a mut Value);
    type IntoIter = IterMut<'a>;
    fn into_iter(self) -> Self::IntoIter { self.iter_mut() }
}

impl From<BTreeMap<FieldName, Value>> for Document { fn from(fields: BTreeMap<FieldName, Value>) -> Self { Self { fields } } }
impl From<Document> for BTreeMap<FieldName, Value> { fn from(document: Document) -> Self { document.into_map() } }

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::*;

    #[test] fn field_name_preserves_text() { let name = FieldName::new("name"); assert_eq!(name.as_str(), "name"); assert_eq!(name.to_string(), "name"); }
    #[test] fn field_name_can_be_empty() { let name = FieldName::new(""); assert_eq!(name.as_str(), ""); }
    #[test] fn cloned_field_names_share_their_allocation() { let original = FieldName::new("shared"); let cloned = original.clone(); assert!(Arc::ptr_eq(original.as_arc(), cloned.as_arc())); }
    #[test] fn arc_field_name_is_reused() { let source: Arc<str> = Arc::from("shared"); let name = FieldName::from(source.clone()); assert!(Arc::ptr_eq(&source, name.as_arc())); }
    #[test] fn new_document_is_empty() { let document = Document::new(); assert!(document.is_empty()); assert_eq!(document.len(), 0); }
    #[test] fn default_document_is_empty() { assert_eq!(Document::default(), Document::new()); }
    #[test] fn insert_adds_a_field() { let mut document = Document::new(); let previous = document.insert("name", "Tom"); assert_eq!(previous, None); assert_eq!(document.len(), 1); assert_eq!(document.get("name"), Some(&Value::from("Tom"))); }
    #[test] fn insert_replaces_an_existing_field() { let mut document = Document::new(); document.insert("age", 18_i64); let previous = document.insert("age", 19_i64); assert_eq!(previous, Some(Value::from(18_i64))); assert_eq!(document.get("age"), Some(&Value::from(19_i64))); assert_eq!(document.len(), 1); }
    #[test] fn get_distinguishes_missing_from_null() { let mut document = Document::new(); document.insert("value", Value::Null); assert_eq!(document.get("value"), Some(&Value::Null)); assert_eq!(document.get("missing"), None); }
    #[test] fn contains_key_detects_existing_fields() { let document = Document::from_fields([("active", Value::from(true))]); assert!(document.contains_key("active")); assert!(!document.contains_key("missing")); }
    #[test] fn get_mut_updates_an_existing_value() { let mut document = Document::from_fields([("active", Value::from(false))]); let value = document.get_mut("active").expect("the field must exist"); *value = Value::from(true); assert_eq!(document.get("active"), Some(&Value::from(true))); }
    #[test] fn remove_deletes_and_returns_a_field() { let mut document = Document::from_fields([("name", Value::from("Tom"))]); let removed = document.remove("name"); assert_eq!(removed, Some(Value::from("Tom"))); assert!(document.is_empty()); }
    #[test] fn removing_a_missing_field_returns_none() { let mut document = Document::new(); assert_eq!(document.remove("missing"), None); }
    #[test] fn clear_removes_all_fields() { let mut document = Document::from_fields([ ("name", Value::from("Tom")), ("age", Value::from(18_i64)), ]); document.clear(); assert!(document.is_empty()); }
    #[test] fn from_fields_builds_a_document() { let document = Document::from_fields([ ("name", Value::from("Tom")), ("age", Value::from(18_i64)), ]); assert_eq!(document.len(), 2); assert_eq!(document.get("name"), Some(&Value::from("Tom"))); assert_eq!(document.get("age"), Some(&Value::from(18_i64))); }
    #[test] fn duplicate_fields_keep_the_last_value() { let document = Document::from_fields([ ("value", Value::from(1_i64)), ("value", Value::from(2_i64)), ]); assert_eq!(document.len(), 1); assert_eq!(document.get("value"), Some(&Value::from(2_i64))); }
    #[test] fn iteration_is_lexicographically_ordered() { let document = Document::from_fields([ ("z", Value::Null), ("a", Value::Null), ("m", Value::Null), ]); let names = document.keys().map(FieldName::as_str).collect::<Vec<_>>(); assert_eq!(names, vec!["a", "m", "z"]); }
    #[test] fn equality_is_independent_of_insertion_order() { let first = Document::from_fields([ ("name", Value::from("Tom")), ("age", Value::from(18_i64)), ]); let second = Document::from_fields([ ("age", Value::from(18_i64)), ("name", Value::from("Tom")), ]); assert_eq!(first, second); }
    #[test] fn extend_adds_and_replaces_fields() { let mut document = Document::from_fields([("age", Value::from(18_i64))]); document.extend([("age", Value::from(19_i64)), ("name", Value::from("Tom"))]); assert_eq!(document.get("age"), Some(&Value::from(19_i64))); assert_eq!(document.get("name"), Some(&Value::from("Tom"))); }
    #[test] fn document_can_be_collected_from_an_iterator() { let document = [("name", Value::from("Tom")), ("active", Value::from(true))] .into_iter() .collect::<Document>(); assert_eq!(document.get("name"), Some(&Value::from("Tom"))); assert_eq!(document.get("active"), Some(&Value::from(true))); }
    #[test] fn owned_iteration_returns_all_fields() { let document = Document::from_fields([ ("name", Value::from("Tom")), ("age", Value::from(18_i64)), ]); let fields = document.into_iter().collect::<Vec<_>>(); assert_eq!(fields.len(), 2); assert_eq!(fields[0].0.as_str(), "age"); assert_eq!(fields[1].0.as_str(), "name"); }
    #[test] fn entry_initializes_and_updates_without_duplicate_lookup() { let mut document = Document::new(); document.entry("count").or_insert_with(|| Value::from(0_i64)); assert_eq!(document.get("count"), Some(&Value::from(0_i64))); document.entry("count").and_modify(|value| *value = Value::from(1_i64)); assert_eq!(document.get("count"), Some(&Value::from(1_i64))); }
    #[test] fn iter_mut_can_update_values_with_their_names() { let mut document = Document::from_fields([ ("first", Value::from(false)), ("second", Value::from(false)), ]); for (name, value) in document.iter_mut() { *value = Value::from(name.as_str() == "second"); } assert_eq!(document.get("first"), Some(&Value::from(false))); assert_eq!(document.get("second"), Some(&Value::from(true))); }
    #[test] fn retain_filters_and_can_mutate_kept_values() { let mut document = Document::from_fields([ ("a", Value::from(1_i64)), ("b", Value::from(2_i64)), ]); document.retain(|name, value| { if name.as_str() == "b" { *value = Value::from(20_i64); true } else { false } }); assert_eq!(document.len(), 1); assert_eq!(document.get("b"), Some(&Value::from(20_i64))); }
    #[test] fn append_moves_fields_and_uses_other_values_on_conflict() { let mut left = Document::from_fields([ ("shared", Value::from(1_i64)), ("left", Value::from(true)), ]); let mut right = Document::from_fields([ ("shared", Value::from(2_i64)), ("right", Value::from(true)), ]); left.append(&mut right); assert!(right.is_empty()); assert_eq!(left.get("shared"), Some(&Value::from(2_i64))); assert_eq!(left.len(), 3); }
    #[test] fn mutable_reference_iteration_is_supported() { let mut document = Document::from_fields([ ("first", Value::from(false)), ("second", Value::from(false)), ]); for (_, value) in &mut document { *value = Value::from(true); } assert!(document.values().all(|value| value == &Value::from(true))); }
    #[test] fn field_name_helpers_are_consistent() { let empty = FieldName::new(""); let name = FieldName::new("name"); assert!(empty.is_empty()); assert!(!name.is_empty()); assert_eq!(name.to_owned_string(), "name"); }
    #[test] fn values_mut_can_update_every_field() { let mut document = Document::from_fields([ ("first", Value::from(false)), ("second", Value::from(false)), ]); for value in document.values_mut() { *value = Value::from(true); } assert!(document.values().all(|value| value == &Value::from(true))); }
}
