#![cfg_attr(rustfmt, rustfmt_skip)]
//! Index of the values of a Place: the words of a request to the codes of its data (wip/APP_MODELE_SEMANTIQUE.md,
//! step 5). « l'agence de Lyon » → `entity = LYO01`, « le client Martin » → `client = MARTIN SAS`.
//!
//! The values come from `place.schema` (`value_queries`): the referentials (`provides`), the axes that name where their
//! values live (`values: {source, code, label}`), and the distinct values of the other axes. Matching is lexical:
//! normalised text (lower case, no accents, no punctuation), then, from the strongest, the whole label or code as
//! words of the request, every distinctive word of the label, one rare word of it, or one typo away. Legal forms and
//! linking words never make a match on their own, nor do the words of the catalog (a client « VENTES PLUS » is not
//! found by « les ventes »); a numeric code that reads as a year needs its axis named just before it.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::Value as JsonValue;

pub use crate::search::normalize;
use crate::search::near;

/// Words that never identify a value on their own: legal forms and linking words, French and English.
const STOP: &[&str] = &["sa", "sas", "sasu", "sarl", "eurl", "snc", "sci", "ste", "societe", "et", "de", "du", "des", "la", "le", "les", "l", "d", "a", "au", "aux", "en", "par", "pour", "sur", "the", "of", "and", "inc", "ltd", "llc", "gmbh", "co", "cie", "group", "groupe",
    // Common words of requests, French and English: they never identify a value on their own.
    "plus", "moins", "top", "mois", "an", "ans", "annee", "annees", "jour", "jours", "trimestre", "semaine", "depuis", "entre", "avec", "sans", "tout", "tous", "toute", "toutes", "dernier", "derniere", "derniers",
    "premier", "prochain", "cette", "ce", "cet", "ces", "nos", "notre", "mon", "ma", "mes", "leur", "leurs", "qui", "que", "quoi", "quel", "quelle", "quels", "quelles", "comment", "pourquoi", "combien", "est", "sont",
    "fait", "font", "faire", "vs", "versus", "contre", "compare", "comparer", "affiche", "montre", "donne", "evolution", "total", "moyen", "moyenne", "fort", "forte", "grand", "grande", "petit", "petite", "nord", "sud",
    "est", "ouest", "centre", "pro", "new", "nouveau", "nouvelle", "is", "are", "was", "be", "by", "for", "on", "in", "at", "to", "from", "with", "without", "since", "over", "last", "next", "this", "that", "these", "our",
    "my", "their", "what", "which", "who", "how", "why", "much", "many", "more", "less", "most", "least", "show", "give", "compare", "trend", "per", "each", "all", "month", "months", "year", "years", "quarter", "week", "day"];

#[derive(Debug, Clone, PartialEq)]
pub struct ValueMatch {
    pub axis: String,
    pub code: String,
    pub label: String,
    pub score: f32,
    /// The normalised words of the request that made the match.
    pub matched: String,
    /// Found by meaning (embeddings), not by text: an interpretation to show, never applied silently.
    pub by_meaning: bool,
}

/// Semantic fallback (`llm-bench/values_semantic.py`, 2026-10-15): the best label of the axis needs this cosine with the
/// words after the axis name and this margin over the second label. Calibrated on 13 cases (6 of 10 found, none wrong,
/// 1 of 3 negatives let through): its matches score under the text ones and stay interpretations.
pub const MEANING_FLOOR: f32 = 0.49;
pub const MEANING_MARGIN: f32 = 0.05;
pub const MEANING_SCORE: f32 = 0.45;

#[derive(Debug)]
struct Entry { axis: String, code: String, label: String, label_words: Vec<String>, code_words: Vec<String>, distinctive: Vec<String> }

#[derive(Debug)]
pub struct ValueIndex {
    entries: Vec<Entry>,
    /// Distinctive words → entries (rarity, typos); every word of a label or code → entries (candidates).
    by_word: HashMap<String, Vec<usize>>,
    by_any: HashMap<String, Vec<usize>>,
    /// The words of the catalog: no typo is read into them.
    common: BTreeSet<String>,
    /// Names of each axis (its label, field name and vocabulary) as phrases, for the hint « client Martin ».
    axis_words: BTreeMap<String, Vec<Vec<String>>>,
}

fn words(text: &str) -> Vec<String> { normalize(text).split(' ').filter(|word| !word.is_empty()).map(str::to_owned).collect() }

fn year_like(word: &str) -> bool { word.len() == 4 && (word.starts_with("19") || word.starts_with("20")) && word.bytes().all(|b| b.is_ascii_digit()) }

/// Whether `needle` occurs in `haystack` as whole words; the position of its first word.
fn find_words(haystack: &[String], needle: &[String]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() { return None; }
    (0..=haystack.len() - needle.len()).find(|&at| haystack[at..at + needle.len()] == *needle)
}

impl ValueIndex {
    /// `values`: (axis, code, label); `axes`: (axis, words naming it); `common`: the words of the catalog (measure labels
    /// and vocabulary), which never identify a value on their own.
    pub fn new(values: impl IntoIterator<Item = (String, String, String)>, axes: impl IntoIterator<Item = (String, Vec<String>)>, common: &[String]) -> Self {
        let common: BTreeSet<String> = common.iter().flat_map(|word| words(word)).collect();
        let axis_words: BTreeMap<String, Vec<Vec<String>>> = axes.into_iter().map(|(axis, names)| (axis, names.iter().map(|name| words(name)).filter(|phrase| !phrase.is_empty()).collect())).collect();
        let mut seen = BTreeSet::new();
        let mut entries = Vec::new();
        for (axis, code, label) in values {
            if code.trim().is_empty() || !seen.insert((axis.clone(), code.clone())) { continue; }
            let label = if label.trim().is_empty() { code.clone() } else { label };
            let (label_words, code_words) = (words(&label), words(&code));
            // A number, a word naming the axis (« famille », « agence ») or a word of the catalog is no evidence alone.
            let naming: BTreeSet<&String> = axis_words.get(&axis).into_iter().flatten().flatten().collect();
            let distinctive = label_words.iter().filter(|word| word.len() >= 3 && !word.bytes().all(|b| b.is_ascii_digit()) && !STOP.contains(&word.as_str()) && !common.contains(*word) && !naming.contains(word))
                .cloned().collect::<BTreeSet<_>>().into_iter().collect();
            entries.push(Entry { axis, code, label, label_words, code_words, distinctive });
        }
        let (mut by_word, mut by_any): (HashMap<String, Vec<usize>>, HashMap<String, Vec<usize>>) = (HashMap::new(), HashMap::new());
        for (index, entry) in entries.iter().enumerate() {
            for word in &entry.distinctive { by_word.entry(word.clone()).or_default().push(index); }
            for word in entry.label_words.iter().chain(&entry.code_words).collect::<BTreeSet<_>>() { if !STOP.contains(&word.as_str()) { by_any.entry(word.clone()).or_default().push(index); } }
        }
        Self { entries, by_word, by_any, common, axis_words }
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// Whether a whole name of the axis of `entry` (singular or plural) stands at most two words before the value at
    /// `at` (« le client Martin », « la famille 30 ») or right after it (« the Lyon branch »).
    fn hinted(&self, entry: &Entry, request: &[String], at: usize, len: usize) -> bool {
        let singular: Vec<String> = request.iter().map(|word| word.trim_end_matches('s').to_owned()).collect();
        self.axis_words.get(&entry.axis).into_iter().flatten().any(|name| {
            let name: Vec<String> = name.iter().map(|word| word.trim_end_matches('s').to_owned()).collect();
            let at_start = |start: usize| start + name.len() <= singular.len() && singular[start..start + name.len()] == name[..];
            (0..at).any(|start| start + name.len() <= at && at - (start + name.len()) <= 2 && at_start(start)) || (at + len..(at + len + 2).min(singular.len())).any(at_start)
        })
    }

    /// The values named by `text`, the best first, one per (axis, code), above `floor`.
    pub fn find(&self, text: &str, floor: f32) -> Vec<ValueMatch> {
        let request = words(text);
        let mut best: BTreeMap<(String, String), ValueMatch> = BTreeMap::new();
        let mut keep = |entry: &Entry, score: f32, matched: String| {
            let slot = best.entry((entry.axis.clone(), entry.code.clone())).or_insert_with(|| ValueMatch { axis: entry.axis.clone(), code: entry.code.clone(), label: entry.label.clone(), score: 0.0, matched: String::new(), by_meaning: false });
            if score > slot.score { slot.score = score; slot.matched = matched; }
        };
        let mut candidates = BTreeSet::new();
        for word in &request {
            if let Some(list) = self.by_any.get(word) { candidates.extend(list.iter().copied()); }
        }
        for index in candidates {
            let entry = &self.entries[index];
            // The whole label, then the whole code, as words of the request; without a distinctive word in it (« Famille
            // 30 », « 4110 »), only when the axis is named just before it or holds the phrase (« famille 30 »).
            for (phrase, base) in [(&entry.label_words, 1.0), (&entry.code_words, 0.95)] {
                if phrase.is_empty() { continue; }
                if let Some(at) = find_words(&request, phrase) {
                    let holds_name = self.axis_words.get(&entry.axis).into_iter().flatten().any(|name| name.len() == 1 && phrase.contains(&name[0]));
                    let hint = self.hinted(entry, &request, at, phrase.len()) || holds_name;
                    if phrase.iter().all(|word| !entry.distinctive.contains(word)) && !hint { continue; }
                    if phrase.len() == 1 && year_like(&phrase[0]) && !self.hinted(entry, &request, at, 1) { continue; }
                    keep(entry, if hint { 1.0 } else { base }, phrase.join(" "));
                }
            }
            if entry.distinctive.is_empty() { continue; }
            let present: Vec<&String> = entry.distinctive.iter().filter(|word| request.contains(word)).collect();
            let at = present.first().and_then(|word| request.iter().position(|w| w == *word)).unwrap_or(0);
            let hint = if self.hinted(entry, &request, at, 1) { 0.1 } else { 0.0 };
            if present.len() == entry.distinctive.len() {
                keep(entry, 0.85 + hint, present.iter().map(|word| word.as_str()).collect::<Vec<_>>().join(" "));
            } else if let Some((rare, shared)) = present.iter().filter(|word| word.len() >= 4).filter_map(|word| self.by_word.get(*word).map(|list| (*word, list.len()))).filter(|(_, shared)| *shared <= 8).min_by_key(|(_, shared)| *shared) {
                keep(entry, if shared <= 3 { 0.6 } else { 0.55 } + hint, rare.clone());
            }
        }
        // One typo away from a distinctive word of eight entries at most, when the word matched nothing exactly, is no
        // word of the catalog and the axis is named next to it (« la famille peintre »).
        for (at, word) in request.iter().enumerate() {
            if word.len() < 5 || self.by_any.contains_key(word) || STOP.contains(&word.as_str()) || self.common.contains(word) { continue; }
            for (known, list) in &self.by_word {
                if list.len() > 8 || !near(word, known) { continue; }
                for &index in list {
                    let entry = &self.entries[index];
                    if self.hinted(entry, &request, at, 1) { keep(entry, 0.6, word.clone()); }
                }
            }
        }
        let mut found: Vec<ValueMatch> = best.into_values().filter(|value| value.score >= floor).collect();
        // At equal score, the label with the fewest words is the closest to the request (« Agence Lyon » before « Agence
        // Lyon Est »).
        let length = |value: &ValueMatch| words(&value.label).len();
        found.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| length(a).cmp(&length(b))).then_with(|| a.axis.cmp(&b.axis)).then_with(|| a.code.cmp(&b.code)));
        found
    }
}

impl ValueIndex {
    /// The words of a request that name something rather than ask for it: linking words, the common words of requests
    /// and of the catalog, and years left out (what `place.search` looks for).
    pub fn subject(&self, text: &str) -> String {
        let common = |word: &String| [word.clone(), format!("{word}s"), format!("{word}x")].iter().any(|form| STOP.contains(&form.as_str()) || self.common.contains(form));
        // A date (31/12/2022, 2022-12-31) names when, not what.
        let text: String = text.split_whitespace().filter(|token| !(token.contains(['/', '-']) && token.split(['/', '-']).all(|part| !part.is_empty() && part.trim_end_matches(['?', '.', ',']).bytes().all(|byte| byte.is_ascii_digit())))).collect::<Vec<_>>().join(" ");
        crate::search::words(&text).into_iter().filter(|word| !common(word) && !year_like(word)).collect::<Vec<_>>().join(" ")
    }

    /// The words to embed for the semantic fallback: for each axis named in the request without a value of it found by
    /// text, the three words at most after its name (else before it), linking and common words left out.
    pub fn meaning_spans(&self, text: &str, found: &[ValueMatch]) -> Vec<(String, String)> {
        let request = words(text);
        let mut out = Vec::new();
        for (axis, names) in &self.axis_words {
            if found.iter().any(|value| &value.axis == axis) || out.iter().any(|(known, _): &(String, String)| known == axis) { continue; }
            let keep = |word: &&String| !STOP.contains(&word.as_str()) && !self.common.contains(*word);
            for name in names {
                let Some(at) = find_words(&request, name) else { continue };
                let after: Vec<&String> = request[at + name.len()..].iter().filter(keep).take(3).collect();
                let span = if after.is_empty() { request[at.saturating_sub(3)..at].iter().filter(keep).cloned().collect::<Vec<_>>().join(" ") } else { after.into_iter().cloned().collect::<Vec<_>>().join(" ") };
                if !span.is_empty() { out.push((axis.clone(), span)); break; }
            }
        }
        out
    }

    /// The labels of an axis worth embedding (codes alone carry no meaning), with their position in the index.
    pub fn meaning_labels(&self, axis: &str) -> Vec<(usize, &str)> {
        self.entries.iter().enumerate().filter(|(_, entry)| entry.axis == axis && entry.label_words.iter().any(|word| word.bytes().any(|b| b.is_ascii_alphabetic()) && word.len() >= 3)).map(|(index, entry)| (index, entry.label.as_str())).collect()
    }

    /// The label nearest to `query` among `labels` (unit vectors, as `meaning_labels` lists them), when it clears
    /// `MEANING_FLOOR` and leads the second by `MEANING_MARGIN`.
    pub fn find_by_meaning(&self, span: &str, query: &[f32], labels: &[(usize, Vec<f32>)]) -> Option<ValueMatch> {
        let mut ranked: Vec<(f32, usize)> = labels.iter().map(|(index, vector)| (query.iter().zip(vector).map(|(a, b)| a * b).sum::<f32>(), *index)).collect();
        ranked.sort_by(|a, b| b.0.total_cmp(&a.0));
        let (best, index) = *ranked.first()?;
        let second = ranked.get(1).map_or(0.0, |(score, _)| *score);
        let entry = self.entries.get(index)?;
        (best >= MEANING_FLOOR && best - second >= MEANING_MARGIN).then(|| ValueMatch { axis: entry.axis.clone(), code: entry.code.clone(), label: entry.label.clone(), score: MEANING_SCORE, matched: span.to_owned(), by_meaning: true })
    }
}

/// The queries reading the values of a Place from its `place.schema` answer: (axis, query giving `code` and `label`).
/// A table that provides a referential gives its codes and labels; an axis naming its `values` reads them there; any
/// other axis reads its distinct values. An axis tied to a referential (`dimension`) reads the referential instead,
/// and also its own distinct values, so that codes without a node are found too.
pub fn value_queries(schema: &JsonValue) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut referentials = BTreeMap::new();
    for referential in schema["referentials"].as_array().into_iter().flatten() {
        let (name, collection) = (referential["name"].as_str().unwrap_or_default(), referential["collection"].as_str().unwrap_or_default());
        if !name.is_empty() && !collection.is_empty() { referentials.insert(name.to_owned(), collection.to_owned()); }
    }
    for (name, collection) in &referentials {
        out.push((name.clone(), format!("on {collection} | where exists(code) | select code, coalesce(label, code) as label")));
    }
    for collection in schema["collections"].as_array().into_iter().flatten() {
        let name = collection["name"].as_str().unwrap_or_default();
        if name.is_empty() || referentials.values().any(|provided| provided == name) { continue; }
        for field in collection["fields"].as_array().into_iter().flatten() {
            if field["role"] != "axis" { continue; }
            let Some(field_name) = field["name"].as_str() else { continue };
            let axis = format!("{name}.{field_name}");
            let values = &field["values"];
            if let (Some(source), Some(code)) = (values["source"].as_str(), values["code"].as_str()) {
                let label = values["label"].as_str().unwrap_or(code);
                out.push((axis.clone(), format!("on {source} | where exists({code}) | group | by {code} as code | first {label} as label | end")));
            }
            out.push((axis, format!("on {name} | where exists({field_name}) and {field_name} != null and {field_name} != \"\" | group | by {field_name} as code | end | derive label = code")));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> ValueIndex {
        let values = [
            ("ventes.client", "MARTIN SAS", "MARTIN SAS"), ("ventes.client", "MARTINEZ", "Martinez Frères"), ("ventes.client", "DUBOIS DISTRIBUTION", "DUBOIS DISTRIBUTION"),
            ("ventes.client", "LEROY BTP", "LEROY BTP"), ("ventes.client", "VENTES PLUS", "VENTES PLUS SARL"), ("ventes.client", "C0042", "Boulangerie du Centre"),
            ("ventes.family", "Famille 30", "Famille 30"), ("ventes.family", "Famille 3", "Famille 3"), ("ventes.family", "PEINT", "Peinture et accessoires"),
            ("ventes.client_type", "4110", "4110"), ("ventes.client_type", "2025", "2025"), ("org", "LYA02", "Agence Lyon Est"), ("org", "LYO01", "Agence Lyon"), ("org", "PAR01", "Agence Paris Nord"), ("org", "S1", "Glacier Lyon SAS"),
        ].map(|(a, c, l)| (a.to_owned(), c.to_owned(), l.to_owned()));
        let axes = [("ventes.client", vec!["client", "customer"]), ("ventes.family", vec!["famille", "family", "gamme"]), ("ventes.client_type", vec!["type de client", "client type"]), ("org", vec!["agence", "société", "site", "branch"])]
            .map(|(a, w)| (a.to_owned(), w.into_iter().map(str::to_owned).collect()));
        ValueIndex::new(values, axes, &["chiffre d'affaires", "ventes", "marge", "budget", "trésorerie"].map(str::to_owned))
    }
    fn top(text: &str) -> Vec<(String, String)> { index().find(text, 0.5).into_iter().map(|found| (found.axis, found.code)).collect() }
    fn pair(axis: &str, code: &str) -> (String, String) { (axis.to_owned(), code.to_owned()) }

    #[test] fn meaning_fallback_embeds_the_words_after_an_axis_without_value_and_keeps_a_clear_winner_only() { let index = index(); let found = index.find("ventes de la gamme jardinage cette année", 0.5); assert!(found.is_empty()); assert_eq!(index.meaning_spans("ventes de la gamme jardinage cette année", &found), [("ventes.family".to_owned(), "jardinage".to_owned())]); assert!(index.meaning_spans("ventes de la famille 30", &index.find("ventes de la famille 30", 0.5)).is_empty()); let labels = index.meaning_labels("ventes.family"); assert_eq!(labels.iter().map(|(_, label)| *label).collect::<Vec<_>>(), ["Famille 30", "Famille 3", "Peinture et accessoires"]); let peinture = labels[2].0; let unit = |x: f32, y: f32| { let n = (x * x + y * y).sqrt(); vec![x / n, y / n] }; let vectors = vec![(labels[0].0, unit(0.0, 1.0)), (labels[1].0, unit(0.1, 1.0)), (peinture, unit(1.0, 0.2))]; let hit = index.find_by_meaning("peintures", &unit(1.0, 0.1), &vectors).unwrap(); assert_eq!((hit.code.as_str(), hit.by_meaning, hit.score), ("PEINT", true, MEANING_SCORE)); assert!(index.find_by_meaning("ambigu", &unit(0.6717, 0.7407), &vectors).is_none()); }

    #[test] fn normalize_folds_case_accents_and_punctuation() { assert_eq!(normalize("Évolution du CA — l'Agence  Lyon !"), "evolution du ca l agence lyon"); assert!(near("peinture", "peintur")); assert!(!near("lyon", "lyons")); }
    #[test] fn values_are_found_by_label_code_distinctive_or_rare_words_and_typos() { assert_eq!(top("combien on a vendu au client MARTIN SAS cette année ?"), [pair("ventes.client", "MARTIN SAS")]); assert_eq!(top("ventes de la famille 30 par mois"), [pair("ventes.family", "Famille 30")]); assert_eq!(top("comment se porte l'agence de Lyon ?").first(), Some(&pair("org", "LYO01"))); assert_eq!(top("Revenue for client type 4110 by month"), [pair("ventes.client_type", "4110")]); assert_eq!(top("ventes de la famille peinture cette année"), [pair("ventes.family", "PEINT")]); assert_eq!(top("ventes de la famille peintre"), [pair("ventes.family", "PEINT")]); assert_eq!(top("CA du client Dubois en 2025"), [pair("ventes.client", "DUBOIS DISTRIBUTION")]); assert_eq!(top("monthly sales of LEROY BTP"), [pair("ventes.client", "LEROY BTP")]); }
    #[test] fn common_words_years_and_legal_forms_find_nothing() { for text in ["les ventes de 2025 par famille", "chiffre d'affaires et marge des sociétés SAS", "top 10 des clients par CA en 2025", "budget de trésorerie"] { assert_eq!(top(text), Vec::<(String, String)>::new(), "{text}"); } }
    #[test] fn value_queries_read_referentials_declared_values_and_distinct_axes() { let schema = serde_json::json!({"referentials": [{"name": "org", "collection": "org"}], "collections": [{"name": "org", "fields": [{"name": "code", "role": "axis"}]}, {"name": "ventes", "fields": [{"name": "revenue", "role": "measure"}, {"name": "client", "role": "axis", "values": {"source": "ventes_clients", "code": "client", "label": "name"}}, {"name": "entity", "role": "axis", "dimension": "org"}]}]}); let queries = value_queries(&schema); assert_eq!(queries.iter().map(|(axis, _)| axis.as_str()).collect::<Vec<_>>(), ["org", "ventes.client", "ventes.client", "ventes.entity"]); assert!(queries[1].1.starts_with("on ventes_clients | where exists(client) | group | by client as code | first name as label")); }
}
