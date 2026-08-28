#![cfg_attr(rustfmt, rustfmt_skip)]
pub const MEMORY: &str = "_memory";
pub const QUERY_MEMORY: &str = "_query_memory";
pub const MEMORY_EVENTS: &str = "_memory_events";
pub const INDEX_OBSERVATIONS: &str = "_index_observations";
pub const ALL: [&str; 4] = [MEMORY, QUERY_MEMORY, MEMORY_EVENTS, INDEX_OBSERVATIONS];
#[inline] pub fn contains(name: &str) -> bool { ALL.contains(&name) }