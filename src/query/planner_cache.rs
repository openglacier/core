//! Bounded, invalidatable LRU cache for compiled plans.
use crate::{
    engine::PlannedQuery,
    memory::{MemoryClass, MemoryGovernor, MemoryReservation},
};
use std::{
    collections::{HashMap, VecDeque},
    mem::{size_of, size_of_val},
    sync::Mutex,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlannerCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}
impl PlannerCacheStats {
    pub fn hit_rate(self) -> f64 {
        let n = self.hits.saturating_add(self.misses);
        if n == 0 {
            0.0
        } else {
            self.hits as f64 / n as f64
        }
    }
}

#[derive(Debug)]
struct CacheEntry {
    value: PlannedQuery,
    _reservation: MemoryReservation,
    bytes: usize,
}

#[derive(Debug)]
struct State {
    map: HashMap<String, CacheEntry>,
    lru: VecDeque<String>,
    stats: PlannerCacheStats,
    current_bytes: usize,
}

#[derive(Debug)]
pub struct PlannerCache {
    capacity: usize,
    max_bytes: usize,
    governor: MemoryGovernor,
    state: Mutex<State>,
}
impl PlannerCache {
    pub fn new(capacity: usize) -> Self {
        Self::new_governed(capacity, usize::MAX, MemoryGovernor::unlimited())
    }

    pub fn new_governed(capacity: usize, max_bytes: usize, governor: MemoryGovernor) -> Self {
        Self {
            capacity: capacity.max(1),
            max_bytes: max_bytes.max(1),
            governor,
            state: Mutex::new(State {
                map: HashMap::new(),
                lru: VecDeque::new(),
                stats: PlannerCacheStats::default(),
                current_bytes: 0,
            }),
        }
    }

    pub fn get(&self, key: &str) -> Option<PlannedQuery> {
        let mut s = self.state.lock().expect("planner cache lock poisoned");
        let value = s.map.get(key).map(|entry| entry.value.clone());
        if value.is_some() {
            s.stats.hits = s.stats.hits.saturating_add(1);
            s.lru.retain(|cached| cached != key);
            s.lru.push_back(key.to_owned());
        } else {
            s.stats.misses = s.stats.misses.saturating_add(1);
        }
        value
    }

    pub fn insert(&self, key: String, value: PlannedQuery) {
        let bytes = estimated_entry_bytes(&key, &value);
        if bytes > self.max_bytes {
            return;
        }

        let mut s = self.state.lock().expect("planner cache lock poisoned");
        if let Some(previous) = s.map.remove(&key) {
            s.current_bytes = s.current_bytes.saturating_sub(previous.bytes);
            s.lru.retain(|cached| cached != &key);
        }

        while s.map.len() >= self.capacity || s.current_bytes.saturating_add(bytes) > self.max_bytes
        {
            if !evict_one(&mut s) {
                break;
            }
        }

        let reservation = loop {
            match self.governor.reserve(MemoryClass::Planner, bytes) {
                Ok(reservation) => break Some(reservation),
                Err(_) if evict_one(&mut s) => continue,
                Err(_) => break None,
            }
        };

        let Some(reservation) = reservation else {
            return;
        };

        s.current_bytes = s.current_bytes.saturating_add(bytes);
        s.map.insert(
            key.clone(),
            CacheEntry {
                value,
                _reservation: reservation,
                bytes,
            },
        );
        s.lru.push_back(key);
    }

    pub fn invalidate_all(&self) {
        let mut s = self.state.lock().expect("planner cache lock poisoned");
        s.map.clear();
        s.lru.clear();
        s.current_bytes = 0;
    }

    pub fn stats(&self) -> PlannerCacheStats {
        self.state
            .lock()
            .expect("planner cache lock poisoned")
            .stats
    }

    pub fn current_bytes(&self) -> usize {
        self.state
            .lock()
            .expect("planner cache lock poisoned")
            .current_bytes
    }

    pub const fn max_bytes(&self) -> usize {
        self.max_bytes
    }
}

fn estimated_entry_bytes(key: &str, value: &PlannedQuery) -> usize {
    key.len()
        .saturating_add(size_of_val(value))
        .saturating_add(size_of::<CacheEntry>())
        .max(1)
}

fn evict_one(state: &mut State) -> bool {
    while let Some(key) = state.lru.pop_front() {
        if let Some(entry) = state.map.remove(&key) {
            state.current_bytes = state.current_bytes.saturating_sub(entry.bytes);
            state.stats.evictions = state.stats.evictions.saturating_add(1);
            return true;
        }
    }
    false
}
