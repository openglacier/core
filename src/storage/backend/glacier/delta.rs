#![cfg_attr(rustfmt, rustfmt_skip)]
//! The live documents of a collection not yet written to a segment, by key.
//!
//! Snapshots share the delta, and a commit changes it in place only when no snapshot holds it: with readers or
//! other writers at work every commit copied it whole. It is therefore kept in two maps of distinct keys: commits
//! write to `recent`, at most [`RECENT`] rows, and only that one is copied; it joins `settled` once full.

use std::{collections::BTreeMap, sync::Arc};

use super::DeltaRow;

const RECENT: usize = 1024;

type Rows = BTreeMap<u128, DeltaRow>;

#[derive(Clone, Debug, Default)]
pub(super) struct Delta { settled: Arc<Rows>, recent: Arc<Rows> }

impl Delta {
    pub(super) fn len(&self) -> usize { self.settled.len() + self.recent.len() }
    pub(super) fn is_empty(&self) -> bool { self.len() == 0 }
    pub(super) fn get(&self, key: &u128) -> Option<&DeltaRow> { self.recent.get(key).or_else(|| self.settled.get(key)) }
    pub(super) fn contains_key(&self, key: &u128) -> bool { self.get(key).is_some() }
    pub(super) fn first_key(&self) -> Option<u128> { self.settled.keys().next().into_iter().chain(self.recent.keys().next()).min().copied() }
    pub(super) fn last_key(&self) -> Option<u128> { self.settled.keys().next_back().into_iter().chain(self.recent.keys().next_back()).max().copied() }
    /// In no particular order.
    pub(super) fn values(&self) -> impl Iterator<Item = &DeltaRow> { self.settled.values().chain(self.recent.values()) }

    /// In key order.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&u128, &DeltaRow)> {
        let (mut settled, mut recent) = (self.settled.iter().peekable(), self.recent.iter().peekable());
        std::iter::from_fn(move || match (settled.peek(), recent.peek()) {
            (Some(old), Some(new)) if old.0 < new.0 => settled.next(),
            (_, Some(_)) => recent.next(),
            _ => settled.next(),
        })
    }

    pub(super) fn insert(&mut self, key: u128, row: DeltaRow) {
        if self.settled.contains_key(&key) {
            Arc::make_mut(&mut self.settled).insert(key, row);
            return;
        }
        Arc::make_mut(&mut self.recent).insert(key, row);
        if self.recent.len() > RECENT {
            let recent = std::mem::take(&mut self.recent);
            Arc::make_mut(&mut self.settled).extend(Arc::try_unwrap(recent).unwrap_or_else(|shared| (*shared).clone()));
        }
    }

    pub(super) fn remove(&mut self, key: &u128) {
        if self.recent.contains_key(key) { Arc::make_mut(&mut self.recent).remove(key); } else if self.settled.contains_key(key) { Arc::make_mut(&mut self.settled).remove(key); }
    }
}

impl From<Rows> for Delta {
    fn from(rows: Rows) -> Self { Self { settled: Arc::new(rows), recent: Arc::default() } }
}

impl std::ops::Index<&u128> for Delta {
    type Output = DeltaRow;
    fn index(&self, key: &u128) -> &DeltaRow { self.get(key).expect("a delta row") }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Document;

    #[test] fn a_delta_reads_as_one_map_whatever_snapshots_hold_and_copies_only_its_recent_rows() { let row = |version| DeltaRow { version, document: Arc::new(Document::new()) }; let (mut delta, mut expected, mut snapshots) = (Delta::default(), BTreeMap::new(), Vec::new()); for step in 0..5 * RECENT as u128 { let key = step * 7919 % 4001; if step % 5 == 4 { delta.remove(&key); expected.remove(&key); } else { delta.insert(key, row(step as u64)); expected.insert(key, step as u64); }; if step % 97 == 0 { snapshots.push((delta.clone(), expected.clone())); } } snapshots.push((delta, expected)); for (delta, expected) in &snapshots { assert_eq!(delta.iter().map(|(key, row)| (*key, row.version)).collect::<BTreeMap<_, _>>(), *expected); assert!(delta.iter().map(|(key, _)| key).is_sorted()); assert_eq!((delta.len(), delta.first_key(), delta.last_key()), (expected.len(), expected.keys().next().copied(), expected.keys().next_back().copied())); assert_eq!(delta.values().count(), expected.len()); assert!(expected.iter().all(|(key, version)| delta[key].version == *version && delta.contains_key(key)) && !delta.contains_key(&4001)); assert!(delta.recent.len() <= RECENT); } }
}
