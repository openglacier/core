#![cfg_attr(rustfmt, rustfmt_skip)]
//! The index by words. A Place's index is made of parts, one per source of content (a collection, the files, the
//! Apps), so that a write rebuilds only the part it touched; a search reads all the parts at once. A word of the request
//! finds the words of the entries equal to it, starting with it (four letters or more), or, when no entry holds it as
//! it is, one typo away (`near`);
//! each word found counts for its rarity across all the parts (and a little for the times the entry repeats it), an entry
//! scores the sum over the words of the request.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value as JsonValue;

use super::{counted, near, words};

/// Where entries come from: what they refer to (merged into each hit) and the scope keys they fall under (`app:<instance>`,
/// `collection:<name>`, `field:<collection>.<field>`, `folder:<fileId>` …).
#[derive(Debug, Clone, PartialEq)]
pub struct Origin { pub reference: JsonValue, pub scope: Vec<String> }

/// One thing to find: its kind (`value`, `file`, `folder`, `app`, `table` …), its text, how many times it occurs (the
/// rows holding a value), its origin, and what is its own (a file's id and path).
#[derive(Debug, Clone, PartialEq)]
pub struct Entry { pub kind: &'static str, pub text: String, pub count: u64, pub origin: u32, pub own: Option<JsonValue> }

/// The entries of one source and their words.
#[derive(Debug, Default)]
pub struct Part { origins: Vec<Origin>, entries: Vec<Entry>, words: BTreeMap<String, Vec<(u32, u32)>> }

impl Part {
    #[must_use]
    pub fn new(origins: Vec<Origin>, entries: Vec<Entry>) -> Self {
        let mut index: BTreeMap<String, Vec<(u32, u32)>> = BTreeMap::new();
        for (at, entry) in entries.iter().enumerate() {
            for (word, times) in counted(&entry.text) { index.entry(word).or_default().push((at as u32, times)); }
        }
        Self { origins, entries, words: index }
    }
    #[must_use]
    pub fn len(&self) -> usize { self.entries.len() }
    #[must_use]
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// The words of this part matching `word`, with the weight of the match; typos only when `typos`.
    fn matching(&self, word: &str, typos: bool) -> Vec<(&str, f32)> {
        let mut out = Vec::new();
        let first = word.chars().next().map_or(String::new(), String::from);
        for (known, _) in self.words.range(first..) {
            if !known.starts_with(word.chars().next().unwrap_or(' ')) { break; }
            if known == word { out.push((known.as_str(), 1.0)); }
            else if word.len() >= 4 && known.starts_with(word) { out.push((known.as_str(), 0.8)); }
            else if typos && near(word, known) { out.push((known.as_str(), 0.6)); }
        }
        out
    }
}

/// One entry found, with its score; a long text (a passage) comes with the words around what was found.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit<'a> { pub entry: &'a Entry, pub origin: &'a Origin, pub score: f32, pub excerpt: Option<String> }

/// Texts longer than this are answered by an excerpt.
const LONG: usize = 240;

/// About [`LONG`] characters of `text` around the first of its words that starts with one of `found` (normalised).
fn excerpt(text: &str, found: &[String]) -> String {
    let spans: Vec<(usize, &str)> = text.split_whitespace().map(|word| (word.as_ptr() as usize - text.as_ptr() as usize, word)).collect();
    let at = spans.iter().position(|(_, word)| words(word).iter().any(|word| found.iter().any(|found| word.starts_with(found.as_str()) || found.starts_with(word.as_str())))).unwrap_or(0);
    let (mut first, mut last, mut length) = (at, at, spans.get(at).map_or(0, |(_, word)| word.len()));
    while length < LONG && (first > 0 || last + 1 < spans.len()) {
        if last + 1 < spans.len() { last += 1; length += spans[last].1.len() + 1; }
        if first > 0 && ((at - first) * 2 < last - at || last + 1 >= spans.len()) { first -= 1; length += spans[first].1.len() + 1; }
    }
    let Some((start, _)) = spans.get(first) else { return String::new() };
    let (end, word) = spans[last];
    format!("{}{}{}", if first > 0 { "… " } else { "" }, &text[*start..end + word.len()], if last + 1 < spans.len() { " …" } else { "" })
}

impl Hit<'_> {
    /// The hit as a search answers it: kind, text (its excerpt), count, score, then its reference and what is its own.
    #[must_use]
    pub fn to_json(&self) -> JsonValue {
        let mut out = serde_json::json!({"kind": self.entry.kind, "text": self.excerpt.as_deref().unwrap_or(&self.entry.text), "count": self.entry.count, "score": (self.score * 100.0).round() / 100.0});
        for extra in [Some(&self.origin.reference), self.entry.own.as_ref()].into_iter().flatten() {
            if let (Some(out), Some(extra)) = (out.as_object_mut(), extra.as_object()) { out.extend(extra.iter().map(|(key, value)| (key.clone(), value.clone()))); }
        }
        out
    }
}

/// The entries of `parts` matching `text`, under every key of `scope` and seen by `visible`, the best first (then the
/// most frequent); those scoring under half the best are left out.
pub fn search<'a>(parts: &[&'a Part], text: &str, scope: &[String], visible: &dyn Fn(&Origin) -> bool, limit: usize) -> Vec<Hit<'a>> {
    let total = parts.iter().map(|part| part.len()).sum::<usize>().max(1) as f32;
    let mut scores: HashMap<(usize, u32), f32> = HashMap::new();
    let asked = words(text);
    for word in asked.iter() {
        // A word some entry holds as it is was meant: no typo is read into it.
        let typos = !parts.iter().any(|part| part.words.contains_key(word));
        let matches: Vec<Vec<(&str, f32)>> = parts.iter().map(|part| part.matching(word, typos)).collect();
        let mut frequency: HashMap<&str, usize> = HashMap::new();
        for (part, found) in parts.iter().zip(&matches) {
            for (known, _) in found { *frequency.entry(known).or_default() += part.words[*known].len(); }
        }
        let mut best: HashMap<(usize, u32), f32> = HashMap::new();
        for (at, (part, found)) in parts.iter().zip(&matches).enumerate() {
            for &(known, weight) in found {
                let rarity = (1.0 + total / frequency[known] as f32).ln();
                // A word an entry repeats counts a little more (a page about it, not one mention).
                for &(entry, times) in &part.words[known] {
                    let score = best.entry((at, entry)).or_default();
                    *score = score.max(weight * rarity * (1.0 + 0.25 * (times as f32).ln()));
                }
            }
        }
        for (key, score) in best { *scores.entry(key).or_default() += score; }
    }
    let mut hits: Vec<Hit<'a>> = scores.into_iter().filter_map(|((at, entry), score)| {
        let part = parts[at];
        let entry = &part.entries[entry as usize];
        let origin = &part.origins[entry.origin as usize];
        (scope.iter().all(|key| origin.scope.contains(key)) && visible(origin)).then_some(Hit { entry, origin, score, excerpt: None })
    }).collect();
    hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(b.entry.count.cmp(&a.entry.count)).then(a.entry.text.len().cmp(&b.entry.text.len())).then(a.entry.text.cmp(&b.entry.text)));
    let floor = hits.first().map_or(0.0, |hit| hit.score / 2.0);
    hits.retain(|hit| hit.score >= floor);
    hits.truncate(limit);
    for hit in &mut hits { if hit.entry.text.len() > LONG { hit.excerpt = Some(excerpt(&hit.entry.text, &asked)); } }
    hits
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part() -> Part {
        let origins = vec![Origin { reference: serde_json::json!({"collection": "balance", "field": "label"}), scope: vec!["app:finance".into(), "collection:balance".into()] }, Origin { reference: serde_json::json!({"collection": "ventes", "field": "client"}), scope: vec!["app:ventes".into(), "collection:ventes".into()] }];
        let entry = |text: &str, count: u64, origin: u32| Entry { kind: "value", text: text.into(), count, origin, own: None };
        Part::new(origins, vec![entry("T.V.A. collectée 20 %", 12, 0), entry("T.V.A. déductible sur immobilisations", 3, 0), entry("Taxe foncière", 1, 0), entry("Salaires et appointements", 40, 0), entry("CASTORAMA FRANCE", 14_240, 1), entry("CASTORAMA FRANCE S.A.", 12, 1), entry("Collecte des déchets", 2, 1)])
    }
    fn texts(hits: &[Hit<'_>]) -> Vec<String> { hits.iter().map(|hit| hit.entry.text.clone()).collect() }

    #[test] fn words_rarity_typos_and_scopes_rank_the_entries() { let part = part(); let all = |_: &Origin| true; assert_eq!(texts(&search(&[&part], "C'est quoi le montant total de la TVA collectée de 2022 ?", &[], &all, 10))[0], "T.V.A. collectée 20 %"); assert_eq!(texts(&search(&[&part], "castorma", &[], &all, 10)), ["CASTORAMA FRANCE", "CASTORAMA FRANCE S.A."]); assert_eq!(texts(&search(&[&part], "salaire", &[], &all, 10)), ["Salaires et appointements"]); assert_eq!(texts(&search(&[&part], "tva", &["app:ventes".into()], &all, 10)), Vec::<String>::new()); assert!(search(&[&part], "castorama", &[], &|origin: &Origin| !origin.scope.contains(&"app:ventes".to_owned()), 10).is_empty()); let hit = &search(&[&part], "taxe fonciere", &[], &all, 1)[0]; assert_eq!(hit.to_json()["collection"], "balance"); }
}
