//! Which Place changed a collection. What a materialized view computed for a
//! Place holds while neither a write of that Place nor a write og-core cannot
//! attribute to one Place (an unscoped write, a load, a direct storage write)
//! changed the collections it reads: an import in one Place leaves the
//! caches of the other Places alone.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::{collections::HashMap, sync::Mutex};

use serde_json::{json, Value as JsonValue};

use crate::storage::{CollectionId, StorageEngine};

#[derive(Debug)]
pub(crate) struct Changes {
    /// Tells this process from the one before: what the engine attributed is not kept.
    boot: i64,
    collections: Mutex<HashMap<String, Collection>>,
}

/// The storage generations of one collection's changes.
#[derive(Debug, Default)]
struct Collection {
    /// The last one this engine accounted for.
    known: u64,
    /// The last one it could not attribute to one Place.
    shared: u64,
    /// The last one of each Place.
    places: HashMap<String, u64>,
}

impl Changes {
    pub(crate) fn new() -> Self {
        Self { boot: crate::helpers::now_millis(), collections: Mutex::default() }
    }

    /// The storage generation of `collection`'s last change (`None`: the storage cannot tell).
    pub(crate) fn changed(storage: &dyn StorageEngine, collection: &str) -> Option<u64> {
        let collection = CollectionId::parse(collection).ok()?;
        storage.read().ok()?.changed(&collection).ok().flatten()
    }

    /// Attributes to `place` (`None`: to every Place) the write that moved
    /// `collection` from generation `before`.
    pub(crate) fn record(&self, storage: &dyn StorageEngine, collection: &str, place: Option<&str>, before: Option<u64>) {
        let (Some(before), Some(after)) = (before, Self::changed(storage, collection)) else { return };
        if after == before {
            return;
        }
        let mut collections = crate::helpers::lock_unpoisoned(&self.collections);
        let entry = collections.entry(collection.to_owned()).or_default();
        // A change between the last one accounted for and this write is nobody's.
        if entry.known != before {
            entry.shared = before;
        }
        match place {
            Some(place) => { entry.places.insert(place.to_owned(), after); }
            None => entry.shared = after,
        }
        entry.known = after;
    }

    /// What changes when `collection` changes for `place` (`None`: for any
    /// Place); `None` when the storage cannot tell.
    pub(crate) fn token(&self, storage: &dyn StorageEngine, collection: &str, place: Option<&str>) -> Option<JsonValue> {
        let now = Self::changed(storage, collection)?;
        let Some(place) = place else { return Some(json!([self.boot, now])) };
        let mut collections = crate::helpers::lock_unpoisoned(&self.collections);
        let entry = collections.entry(collection.to_owned()).or_default();
        if entry.known != now {
            entry.shared = now;
            entry.known = now;
        }
        Some(json!([self.boot, entry.shared, entry.places.get(place).copied().unwrap_or(0)]))
    }
}
