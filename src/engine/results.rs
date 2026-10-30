//! The rows of the reads a Place repeats: the hub asks the same views again
//! at every App opening, and every open client after an import
//! (CACHE_RESULTATS in wip/README.md). A read's rows are kept under a key its caller
//! builds from the prepared text, the scope and the change tokens of the
//! collections it reads ([`super::changes`]): a write of the Place makes a new
//! key, so kept rows are never stale, and rows nobody asks again leave after a
//! minute. A read arriving while the same one runs waits for its rows.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::{collections::HashMap, sync::{Arc, Condvar, Mutex}, time::{Duration, Instant}};

use crate::{helpers::lock_unpoisoned, memory::{MemoryClass, MemoryGovernor, MemoryReservation}, storage::DocumentId, Document, Value};

/// Below this cost a read runs again rather than being kept.
const MIN_COST: Duration = Duration::from_millis(20);
/// Rows nobody asked again for this long leave.
const IDLE: Duration = Duration::from_secs(60);

/// The rows of one read, as they were sent.
#[derive(Debug, Default)]
pub struct Rows {
    /// The rows a streamed read sent, with their storage id unless it counted.
    pub rows: Vec<(Option<DocumentId>, Document)>,
    /// The rows of a whole response.
    pub values: Vec<serde_json::Value>,
    pub streamed: bool,
    pub statistics: Option<serde_json::Value>,
    bytes: usize,
}

impl Rows {
    #[must_use]
    pub fn new(streamed: bool) -> Self {
        Self { streamed, ..Self::default() }
    }

    #[must_use]
    pub fn with_statistics(self, statistics: Option<serde_json::Value>) -> Self {
        Self { statistics, ..self }
    }

    /// Adds a streamed row unless the rows would pass `limit` bytes (`false`: too big to keep).
    pub fn push(&mut self, id: Option<DocumentId>, document: Document, limit: usize) -> bool {
        self.bytes = self.bytes.saturating_add(64 + document_bytes(&document));
        self.bytes <= limit && { self.rows.push((id, document)); true }
    }

    /// Adds a row of a whole response, as [`Self::push`].
    pub fn push_value(&mut self, value: serde_json::Value, limit: usize) -> bool {
        self.bytes = self.bytes.saturating_add(json_bytes(&value));
        self.bytes <= limit && { self.values.push(value); true }
    }
}

fn json_bytes(value: &serde_json::Value) -> usize {
    32 + match value {
        serde_json::Value::String(text) => text.len(),
        serde_json::Value::Array(items) => items.iter().map(json_bytes).sum(),
        serde_json::Value::Object(fields) => fields.iter().map(|(name, value)| 16 + name.len() + json_bytes(value)).sum(),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
    }
}

fn document_bytes(document: &Document) -> usize {
    document.iter().map(|(name, value)| 48 + name.as_str().len() + value_bytes(value)).sum()
}

fn value_bytes(value: &Value) -> usize {
    match value {
        Value::String(text) => text.len(),
        Value::Array(items) => items.iter().map(|item| 32 + value_bytes(item)).sum(),
        Value::Object(document) => document_bytes(document),
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResultStats {
    pub hits: u64,
    /// Reads served by the same read running at the time.
    pub shared: u64,
    pub misses: u64,
    pub kept: usize,
    pub bytes: usize,
}

#[derive(Debug)]
struct Kept { rows: Arc<Rows>, used: Instant, _reservation: MemoryReservation }

/// A read running for its first reader: the others wait for its rows
/// (`Some(None)`: none to share, they run it).
#[derive(Debug, Default)]
struct Flight { rows: Mutex<Option<Option<Arc<Rows>>>>, done: Condvar }

#[derive(Debug)]
enum Slot { Running(Arc<Flight>), Kept(Kept) }

#[derive(Debug, Default)]
struct State { slots: HashMap<String, Slot>, stats: ResultStats }

#[derive(Debug)]
pub struct Results {
    max_bytes: usize,
    governor: MemoryGovernor,
    state: Mutex<State>,
}

/// What a reader does with its read.
#[derive(Debug)]
pub enum Lookup<'a> {
    /// Sends these rows.
    Hit(Arc<Rows>),
    /// Runs the read and gives its rows to [`Lead::finish`].
    Lead(Lead<'a>),
    /// Runs the read alone.
    Run,
}

/// The first reader of a read; dropped unfinished, it lets the others run it.
#[derive(Debug)]
pub struct Lead<'a> { results: &'a Results, key: Option<String>, flight: Arc<Flight>, started: Instant }

impl Results {
    /// Rows within `max_bytes` (0: nothing is kept nor shared).
    #[must_use]
    pub fn new(max_bytes: usize, governor: MemoryGovernor) -> Self {
        Self { max_bytes, governor, state: Mutex::default() }
    }

    /// The largest rows one read keeps.
    #[must_use]
    pub fn entry_limit(&self) -> usize {
        self.max_bytes / 4
    }

    pub fn begin(&self, key: String) -> Lookup<'_> {
        if self.max_bytes == 0 {
            return Lookup::Run;
        }
        let flight = {
            let mut state = lock_unpoisoned(&self.state);
            match state.slots.get_mut(&key) {
                Some(Slot::Kept(kept)) => {
                    kept.used = Instant::now();
                    let rows = Arc::clone(&kept.rows);
                    state.stats.hits += 1;
                    return Lookup::Hit(rows);
                }
                Some(Slot::Running(flight)) => Arc::clone(flight),
                None => {
                    state.stats.misses += 1;
                    let flight = Arc::<Flight>::default();
                    state.slots.insert(key.clone(), Slot::Running(Arc::clone(&flight)));
                    return Lookup::Lead(Lead { results: self, key: Some(key), flight, started: Instant::now() });
                }
            }
        };
        let mut rows = lock_unpoisoned(&flight.rows);
        while rows.is_none() {
            rows = flight.done.wait(rows).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        match rows.clone().flatten() {
            Some(rows) => { lock_unpoisoned(&self.state).stats.shared += 1; Lookup::Hit(rows) }
            None => Lookup::Run,
        }
    }

    #[must_use]
    pub fn stats(&self) -> ResultStats {
        let state = lock_unpoisoned(&self.state);
        let kept = state.slots.values().filter_map(|slot| match slot { Slot::Kept(kept) => Some(kept.rows.bytes), Slot::Running(_) => None });
        let (kept, bytes) = kept.fold((0, 0), |(count, bytes), size| (count + 1, bytes + size));
        ResultStats { kept, bytes, ..state.stats }
    }

    /// Keeps `rows` under `key`, making room: idle rows first, then the least recently used.
    fn keep(&self, state: &mut State, key: String, rows: Arc<Rows>) {
        let now = Instant::now();
        let kept = |state: &State| state.slots.values().filter_map(|slot| match slot { Slot::Kept(kept) => Some(kept.rows.bytes), Slot::Running(_) => None }).sum::<usize>();
        state.slots.retain(|_, slot| !matches!(slot, Slot::Kept(kept) if now.duration_since(kept.used) > IDLE));
        let mut held = kept(state);
        loop {
            if held + rows.bytes <= self.max_bytes {
                if let Ok(reservation) = self.governor.reserve(MemoryClass::Results, rows.bytes) {
                    state.slots.insert(key, Slot::Kept(Kept { rows, used: now, _reservation: reservation }));
                    return;
                }
            }
            let oldest = state.slots.iter().filter_map(|(key, slot)| match slot { Slot::Kept(kept) => Some((kept.used, key)), Slot::Running(_) => None }).min().map(|(_, key)| key.clone());
            let Some(oldest) = oldest else { return };
            state.slots.remove(&oldest);
            held = kept(state);
        }
    }
}

impl Lead<'_> {
    /// Hands the read's rows to the readers waiting for them, and keeps them
    /// when the read cost enough (`None`: the read failed or was too big).
    pub fn finish(mut self, rows: Option<Rows>) {
        self.publish(rows.map(Arc::new));
    }

    fn publish(&mut self, rows: Option<Arc<Rows>>) {
        let Some(key) = self.key.take() else { return };
        let mut state = lock_unpoisoned(&self.results.state);
        state.slots.remove(&key);
        if let Some(rows) = rows.as_ref().filter(|rows| self.started.elapsed() >= MIN_COST && rows.bytes <= self.results.entry_limit()) {
            self.results.keep(&mut state, key, Arc::clone(rows));
        }
        drop(state);
        *lock_unpoisoned(&self.flight.rows) = Some(rows);
        self.flight.done.notify_all();
    }
}

impl Drop for Lead<'_> {
    fn drop(&mut self) {
        self.publish(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    fn rows(n: usize) -> Rows { let mut rows = Rows::new(false); for i in 0..n { rows.push(None, Document::from_iter([("n", Value::unsigned(i as u64))]), usize::MAX); } rows }
    fn lead<'a>(results: &'a Results, key: &str) -> Lead<'a> { match results.begin(key.to_owned()) { Lookup::Lead(lead) => lead, _ => panic!("expected to lead {key}") } }

    #[test] fn a_costly_read_is_kept_and_a_cheap_one_runs_again() { let results = Results::new(1 << 20, MemoryGovernor::unlimited()); let first = lead(&results, "a"); thread::sleep(MIN_COST); first.finish(Some(rows(3))); assert!(matches!(results.begin("a".into()), Lookup::Hit(rows) if rows.rows.len() == 3)); lead(&results, "b").finish(Some(rows(1))); lead(&results, "b").finish(None); lead(&results, "b"); assert_eq!(results.stats(), ResultStats { hits: 1, shared: 0, misses: 4, kept: 1, bytes: results.stats().bytes }); }
    #[test] fn readers_of_a_running_read_wait_for_its_rows_or_run_it_when_it_fails() { let results = Arc::new(Results::new(1 << 20, MemoryGovernor::unlimited())); for (rows_given, served) in [(Some(2), true), (None, false)] { let key = format!("k{served}"); let lead = lead(&results, &key); let waiters: Vec<_> = (0..3).map(|_| { let (results, key) = (Arc::clone(&results), key.clone()); thread::spawn(move || matches!(results.begin(key), Lookup::Hit(_))) }).collect(); thread::sleep(Duration::from_millis(30)); lead.finish(rows_given.map(rows)); assert!(waiters.into_iter().all(|waiter| waiter.join().unwrap() == served)); } assert_eq!(results.stats().shared, 3); }
    #[test] fn kept_rows_stay_within_their_budget() { let one = rows(10).bytes; let results = Results::new(one * 4 + one / 2, MemoryGovernor::unlimited()); for key in ["a", "b", "c", "d", "e"] { let lead = lead(&results, key); thread::sleep(MIN_COST); lead.finish(Some(rows(10))); } let stats = results.stats(); assert!(stats.kept == 4 && stats.bytes <= one * 4, "{stats:?}"); assert!(matches!(results.begin("a".into()), Lookup::Lead(_))); assert!(matches!(results.begin("e".into()), Lookup::Hit(_))); let big = lead(&results, "big"); thread::sleep(MIN_COST); big.finish(Some(rows(20))); assert!(matches!(results.begin("big".into()), Lookup::Lead(_))); assert!(matches!(Results::new(0, MemoryGovernor::unlimited()).begin("a".into()), Lookup::Run)); }
}
