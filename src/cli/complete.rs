//! Completion for the REPL and the TUI.
//!
//! What is completed depends on the word under the cursor:
//! - the first word: client commands, `on`/`from`, and `.operations`;
//! - after `on`, `from`, `into`, `out`, `lookup`, `join`, `union`: collections;
//! - right after `|`: stage names and aliases;
//! - elsewhere in a query: fields of the `on`/`from` collection, then keywords;
//! - `import FILE COLLECTION`, `format MODE`: paths, collections, modes.
//!
//! Stages and operations come from this build's catalogues. Collections and
//! fields are asked to ogd (`collections.list`, a 200-row sample) and cached.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{collections::HashMap, sync::Mutex, time::{Duration, Instant}};

use serde_json::{json, Value};

use super::{Client, Config};
use crate::{operation::OPERATION_CATALOG, query::STAGE_CATALOG};

pub(super) const COMMANDS: &[&str] = &["help", "import", "format", "reconnect", "quit", "exit"];
pub(super) const FORMATS: &[&str] = &["table", "pretty", "compact", "quiet"];
const KEYWORDS: &[&str] = &["and", "or", "not", "in", "as", "asc", "desc", "by", "between", "true", "false", "null", "end"];
const COLLECTION_AFTER: &[&str] = &["on", "from", "into", "out", "lookup", "join", "union"];
const STALE: Duration = Duration::from_secs(15);

pub(super) struct Completer { config: Config, cache: Mutex<Cache> }

#[derive(Default)]
struct Cache { collections: Option<(Instant, Vec<String>)>, fields: HashMap<String, (Instant, Vec<String>)> }

impl Completer {
    pub fn new(config: Config) -> Self { Self { config, cache: Mutex::default() } }

    /// Drops the cached collections and fields (after an import, a reconnect).
    pub fn invalidate(&self) { if let Ok(mut cache) = self.cache.lock() { *cache = Cache::default() } }

    /// The byte offset where the replaced word starts, and the candidates for it.
    pub fn complete(&self, line: &str, pos: usize) -> (usize, Vec<String>) {
        let before = &line[..pos];
        let words: Vec<&str> = before.split_whitespace().collect();
        let at_word_start = before.is_empty() || before.ends_with(char::is_whitespace);
        let index = if at_word_start { words.len() } else { words.len().saturating_sub(1) };
        let first = words.first().copied().unwrap_or("");
        // Paths: the whole blank-separated word.
        if first == "import" && index == 1 {
            let start = before.rfind(char::is_whitespace).map_or(0, |i| i + 1);
            return (start, complete_path(&before[start..]));
        }
        let start = before.rfind(|c: char| !(crate::helpers::is_identifier_continue(c) || c == '.')).map_or(0, |i| i + before[i..].chars().next().map_or(1, char::len_utf8));
        let word = &before[start..];
        let candidates: Vec<String> = if index == 0 {
            if word.starts_with('.') {
                OPERATION_CATALOG.iter().map(|op| format!(".{}", op.name)).collect()
            } else {
                COMMANDS.iter().chain(&["on", "from"]).map(|s| s.to_string()).collect()
            }
        } else if first == "import" {
            if index == 2 { self.collections() } else { vec!["--sep".into(), "--text".into()] }
        } else if first == "format" {
            FORMATS.iter().map(|s| s.to_string()).collect()
        } else if before[..start].trim_end_matches([' ', '\t']).ends_with(['|', ';', '\n']) {
            // A stage starts after `|`, after `;` and at the start of a line.
            STAGE_CATALOG.iter().flat_map(|entry| std::iter::once(entry.name).chain(entry.aliases.iter().copied())).map(str::to_owned).collect()
        } else if words.get(index.wrapping_sub(1)).is_some_and(|w| COLLECTION_AFTER.contains(w)) {
            self.collections()
        } else {
            let collection = words.windows(2).find(|w| w[0] == "on" || w[0] == "from").map(|w| w[1].trim_end_matches(['|', ';']));
            let mut fields = collection.map(|c| self.fields(c)).unwrap_or_default();
            fields.extend(KEYWORDS.iter().map(|s| s.to_string()));
            fields
        };
        let mut matches: Vec<String> = candidates.into_iter().filter(|c| c.starts_with(word) && c != word).collect();
        matches.sort();
        matches.dedup();
        // A single candidate ends the word, so a blank follows (not after a directory).
        if let [only] = &mut matches[..] { if !only.ends_with('/') { only.push(' ') } }
        (start, matches)
    }

    fn collections(&self) -> Vec<String> {
        let mut cache = match self.cache.lock() { Ok(cache) => cache, Err(_) => return Vec::new() };
        if let Some((at, names)) = &cache.collections { if at.elapsed() < STALE { return names.clone() } }
        let names = self.ask(|client| client.operation("collections.list", json!({})))
            .and_then(|data| data.get("collections").and_then(Value::as_array).cloned())
            .map(|list| list.iter().filter_map(|c| c.get("name").and_then(Value::as_str).map(str::to_owned)).collect())
            .unwrap_or_default();
        cache.collections = Some((Instant::now(), names));
        cache.collections.as_ref().map(|c| c.1.clone()).unwrap_or_default()
    }

    /// Top-level fields seen in the first 200 rows of `collection`.
    fn fields(&self, collection: &str) -> Vec<String> {
        let mut cache = match self.cache.lock() { Ok(cache) => cache, Err(_) => return Vec::new() };
        if let Some((at, names)) = cache.fields.get(collection) { if at.elapsed() < STALE * 4 { return names.clone() } }
        if !crate::helpers::is_identifier(collection) { return Vec::new() }
        let mut names: Vec<String> = Vec::new();
        let _ = self.ask(|client| client.query_rows(&format!("on {collection} | limit 200"), &serde_json::Map::new(), |row| {
            if let Some(object) = row.as_object() { for key in object.keys() { if !names.contains(key) { names.push(key.clone()) } } }
            Ok(())
        }));
        cache.fields.insert(collection.to_owned(), (Instant::now(), names.clone()));
        names
    }

    /// One request on a short-lived connection; `None` on any failure, so
    /// completion never blocks the prompt with an error.
    fn ask<T>(&self, f: impl FnOnce(&mut Client) -> super::Result<T>) -> Option<T> {
        let mut config = self.config.clone();
        config.read_timeout = Some(Duration::from_secs(3));
        Client::connect(&config).and_then(|mut client| f(&mut client)).ok()
    }
}

/// Entries of the directory in `word` starting with its last component;
/// directories end with `/`, hidden entries only when asked for.
fn complete_path(word: &str) -> Vec<String> {
    let expanded = super::import::expand_home(word);
    let (dir, prefix) = match expanded.rfind('/') { Some(i) => (&expanded[..=i], &expanded[i + 1..]), None => ("", expanded.as_str()) };
    let shown = &word[..word.rfind('/').map_or(0, |i| i + 1)];
    let Ok(entries) = std::fs::read_dir(if dir.is_empty() { "." } else { dir }) else { return Vec::new() };
    let mut out: Vec<String> = entries.flatten().filter_map(|entry| {
        let name = entry.file_name().into_string().ok()?;
        if !name.starts_with(prefix) || (name.starts_with('.') && !prefix.starts_with('.')) { return None }
        let slash = if entry.path().is_dir() { "/" } else { "" };
        Some(format!("{shown}{name}{slash}"))
    }).collect();
    out.sort();
    out
}

/// The longest prefix shared by every candidate.
#[cfg(any(test, feature = "cli-tui"))]
pub(super) fn common_prefix(candidates: &[String]) -> &str {
    let Some(first) = candidates.first() else { return "" };
    let mut end = first.len();
    for other in &candidates[1..] {
        end = first.char_indices().zip(other.chars()).find(|((_, a), b)| a != b).map_or(end.min(other.len()), |((i, _), _)| i.min(end));
    }
    &first[..end]
}

#[cfg(test)]
mod tests {
    use super::*;
    fn completer() -> Completer { let (mut config, _) = super::super::Args::parse(Vec::<String>::new()).unwrap(); config.address = "127.0.0.1:1".into(); config.connect_timeout = Duration::from_millis(50); config.identity = None; Completer::new(config) }
    #[test] fn completes_commands_stages_and_operations() { let c = completer(); assert_eq!(c.complete("imp", 3), (0, vec!["import ".into()])); assert_eq!(c.complete("on users | whe", 14).1, ["where "]); assert_eq!(c.complete("on users | whe", 14).0, 11); assert_eq!(c.complete(".collections.l", 14).1, [".collections.list "]); assert_eq!(c.complete("format ta", 9).1, ["table "]); assert!(c.complete("on users | where a", 18).1.contains(&"and".to_owned())); assert_eq!(c.complete("on users; whe", 13).1, ["where "]); assert_eq!(c.complete("on users\nwhe", 12).1, ["where "]); }
    #[test] fn shares_prefixes() { assert_eq!(common_prefix(&["where".into(), "window".into()]), "w"); assert_eq!(common_prefix(&["sort".into()]), "sort"); }
}
