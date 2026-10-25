#![cfg_attr(rustfmt, rustfmt_skip)]
//! Generation of an App from a request (wip/APP_MODELE_SEMANTIQUE.md, step 9): what the bench measured, in og-core.
//!
//! - **The Place** (`Place::new`): its catalog written from `place.schema` and the indicator definitions as
//!   `llm-bench/catalog_from_schema.py` writes it, the system messages of the routing and of the intent, the axes each
//!   catalog line names. The prompts and grammars are those of the bench, byte for byte (test `prompts_match_the_bench`
//!   against `llm-bench/results/prompt-fixture.json`): its scores are the scores of this code.
//! - **One request** (`run`, with the agent): the values it names (`values.rs`), absent data (`route.rs`, no model), the
//!   action the model proposes under the routing grammar, then for an App the intent under a grammar holding only the
//!   identifiers of the Place, the axes the request names and the values found, compiled into an App (`compile.rs`).
//!   A follow-up sends the previous request and intent and gets a diff. Each step is sent as it ends.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Value as JsonValue};

use crate::helpers::JsonFields;
use crate::service::compile::{self, Catalog, Intent, Lang};
use crate::service::route::{self, Concept};
use crate::service::values::{normalize, ValueIndex};

const RULES: &str = include_str!("intent_rules.txt");
const EXAMPLES: &str = include_str!("intent_examples.json");
const ROUTING: &str = include_str!("route_actions.json");
/// The examples of the intent prompt (`run.py`, prompt « commun »: 70 % against 68 % with all of them on the 35B-A3B).
const PICKED: &[&str] = &["E1", "E2", "E4", "E5", "E6", "E7", "E9"];
/// « par activité », « l'activité la plus … » name the business unit (`run.py`, ACTIVITY).
const ACTIVITY: &[&[&str]] = &[&["par", "activite"], &["activite", "la", "plu"], &["activite", "le", "plu"]];
const TAIL: &str = r#"
tail ::= ("\n" periode)? ("\n" gran)? ("\n" cmp)? ("\n" blocs)? ("\ntop: " [0-9]{1,3})? ("\n" tri)? ("\n" note)?
butv ::= "synthese" | "tendance" | "comparer" | "decomposer" | "classer" | "detail" | "expliquer"
periode ::= "periode: " ([0-9]{4} ("-T" [1-4] | "-" [0-1] [0-9])? | "M" [0-1] [0-9] | "dernier-T" | "dernier" | "ytd" | [0-9]{1,2} "m")
gran ::= "gran: " ("mois" | "trimestre" | "annee" | "ytd")
cmp ::= "cmp: " ("n1" | "n2" | "budget" | "forecast" | "aucun")
blocs ::= "blocs: " bloc (", " bloc){0,5}
note ::= "note: " [^\n]{1,100}
"#;

// ---------------------------------------------------------------- catalog text (catalog_from_schema.py)

fn strings(value: &JsonValue) -> Vec<String> { value.as_array().map_or(&[][..], Vec::as_slice).iter().filter_map(JsonValue::as_str).map(str::to_owned).collect() }
fn items(value: &JsonValue) -> &[JsonValue] { value.as_array().map_or(&[][..], Vec::as_slice) }
fn dedupe(words: impl IntoIterator<Item = String>) -> Vec<String> { let mut seen = BTreeSet::new(); words.into_iter().filter(|word| seen.insert(word.clone())).collect() }

/// The French then English label of a field: an object, one string, or none.
fn labels(value: &JsonValue) -> Vec<String> {
    match value { JsonValue::Object(map) => ["fr", "en"].iter().filter_map(|lang| map.str_field(lang).filter(|text| !text.is_empty()).map(str::to_owned)).collect(), JsonValue::String(text) if !text.is_empty() => vec![text.clone()], _ => vec![] }
}
/// « label / label ; aussi : words » with the words of the users that are not a label.
fn described(vocabulary: &JsonValue, labels: &[String]) -> String {
    let lowered: Vec<String> = labels.iter().map(|label| label.to_lowercase()).collect();
    let extra = dedupe(strings(&vocabulary["fr"]).into_iter().chain(strings(&vocabulary["en"])).filter(|word| !lowered.contains(&word.to_lowercase())));
    labels.join(" / ") + &if extra.is_empty() { String::new() } else { format!(" ; aussi : {}", extra.join(", ")) }
}
/// The vocabulary of a field, with the words of the referential its axis is tied to.
fn with_referential(vocabulary: &JsonValue, dimension: Option<&str>, referentials: &BTreeMap<String, JsonValue>) -> JsonValue {
    let Some(words) = dimension.and_then(|name| referentials.get(name)).filter(|words| words.as_object().is_some_and(|map| !map.is_empty())) else { return vocabulary.clone() };
    let langs: BTreeSet<&String> = vocabulary.as_object().into_iter().flatten().chain(words.as_object().into_iter().flatten()).map(|(lang, _)| lang).collect();
    JsonValue::Object(langs.into_iter().map(|lang| (lang.clone(), json!(dedupe(strings(&vocabulary[lang]).into_iter().chain(strings(&words[lang])))))).collect())
}
fn has_vocabulary(row: &JsonValue) -> bool { row["vocabulary"].as_object().is_some_and(|map| !map.is_empty()) }

/// The catalog of the Place as the model reads it.
pub fn catalog_text(schema: &JsonValue, definitions: &[JsonValue], last: &str) -> String {
    let mut rows: Vec<&JsonValue> = vec![];
    for row in definitions { let Some(id) = row.str_field("id") else { continue }; match rows.iter().position(|seen| seen.str_field("id") == Some(id)) { Some(at) => rows[at] = row, None => rows.push(row) } }
    let mut shown: Vec<String> = vec![];
    for instance in items(&schema["instances"]) { for name in strings(&instance["shows"]) { if !shown.contains(&name) { shown.push(name); } } }
    let rank = |id: &str| shown.iter().position(|name| name == id).unwrap_or(shown.len());
    let mut indicators: Vec<&JsonValue> = rows.into_iter().filter(|row| has_vocabulary(row)).collect();
    indicators.sort_by_key(|row| rank(row.str_field("id").unwrap_or_default()));
    let mut referentials = BTreeMap::new();
    for table in items(&schema["tables"]) {
        if let Some(name) = table.str_field("provides").filter(|name| !name.is_empty()) {
            let code = items(&table["fields"]).iter().find(|field| field.str_field("name") == Some("code"));
            referentials.insert(name.to_owned(), code.map_or(JsonValue::Null, |field| field["vocabulary"].clone()));
        }
    }
    let mut axis_labels: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for collection in items(&schema["collections"]) { for field in items(&collection["fields"]) { if field.str_field("role") == Some("axis") && !labels(&field["label"]).is_empty() { axis_labels.entry(field.string_field("name")).or_insert_with(|| labels(&field["label"])); } } }

    let mut out = vec!["CATALOGUE DE LA PLACE".to_owned(), format!("Données jusqu'à : {last}. Exercice ouvert en janvier."), String::new()];
    for view in items(&schema["views"]) {
        if view["measures"].str_field("from").is_none() { continue; }
        let axes: Vec<&JsonValue> = items(&view["fields"]).iter().filter(|field| field.str_field("role") == Some("axis")).collect();
        let scenarios = axes.iter().any(|field| field.str_field("dimension") == Some("scenario"));
        let dims: Vec<String> = axes.iter().filter(|field| field.str_field("dimension") == Some("org")).map(|field| field.string_field("name")).collect();
        out.push(format!("Table finance (indicateurs comptables mensuels, scénarios : {} ; dimension : {})", if scenarios { "réel, budget, prévision" } else { "réel uniquement" }, dims.join(", ")));
        for row in &indicators { let id = row.str_field("id").unwrap_or_default(); out.push(format!("{id} : {}", described(&row["vocabulary"], &[row.str_field("label_fr").unwrap_or(id).to_owned(), row.str_field("label_en").unwrap_or(id).to_owned()]))); }
        for name in &dims { out.push(format!("{name} : {} [axe]", described(&with_referential(&JsonValue::Null, Some("org"), &referentials), axis_labels.get(name).map_or(&[name.clone()][..], Vec::as_slice)))); }
        out.push(String::new());
    }
    // An App publishing its measures through a view keeps its raw tables out: the view is what one reads.
    let through_views: BTreeSet<&str> = items(&schema["views"]).iter().filter(|view| view["measures"].str_field("from").is_some()).filter_map(|view| view.str_field("appId")).collect();
    let app_of: BTreeMap<&str, &str> = items(&schema["instances"]).iter().filter_map(|instance| Some((instance.str_field("instanceId")?, instance.str_field("appId")?))).collect();
    for collection in items(&schema["collections"]) {
        let name = collection.str_field("name").unwrap_or_default();
        let owner = collection["owner"].as_object().filter(|owner| !owner.is_empty());
        if owner.is_some_and(|owner| owner.str_field("instanceId").and_then(|id| app_of.get(id)).is_some_and(|app| through_views.contains(app)))
            || items(&schema["tables"]).iter().any(|table| table.str_field("provides").is_some_and(|provides| !provides.is_empty()) && table.str_field("collection") == Some(name)) { continue; }
        let fields = items(&collection["fields"]);
        let measures: Vec<&JsonValue> = fields.iter().filter(|field| field.str_field("role") == Some("measure") || field.str_field("type") == Some("number") && field["role"].is_null()).collect();
        if measures.is_empty() { continue; }
        let ratios = fields.iter().filter(|field| field.str_field("type") == Some("ratio"));
        let axes = fields.iter().filter(|field| field.str_field("role") == Some("axis") && !matches!(field.str_field("name"), Some("family2" | "family3")));
        let scenarios = fields.iter().any(|field| field.str_field("dimension") == Some("scenario"));
        let table = owner.and_then(|owner| items(&schema["tables"]).iter().find(|table| table.str_field("instanceId") == owner.str_field("instanceId") && table.str_field("table") == owner.str_field("table")));
        let table_label = table.map(|table| labels(&table["label"])).and_then(|labels| labels.into_iter().next()).unwrap_or_else(|| name.to_owned()).to_lowercase();
        let named = |field: &JsonValue| { let own = labels(&field["label"]); if own.is_empty() { vec![field.string_field("name")] } else { own } };
        let qualify = |field: &JsonValue| named(field).into_iter().map(|text| format!("{text} ({table_label})")).collect::<Vec<_>>();
        out.push(format!("Table {name} ({})", if scenarios { "scénarios : réel, budget, prévision" } else { "réel uniquement, PAS de budget" }));
        for field in &measures { out.push(format!("{name}.{} : {}", field.string_field("name"), described(&field["vocabulary"], &qualify(field)))); }
        for field in ratios { out.push(format!("{name}.{} : {} ({} / {})", field.string_field("name"), described(&field["vocabulary"], &qualify(field)), field.string_field("num"), field.string_field("den"))); }
        for field in axes { out.push(format!("{name}.{} : {} [axe]", field.string_field("name"), described(&with_referential(&field["vocabulary"], field.str_field("dimension"), &referentials), &named(field)))); }
        out.push(String::new());
    }
    if !items(&schema["templates"]).is_empty() {
        out.push("Blocs modèles des Apps installées".into());
        for template in items(&schema["templates"]) {
            let fields = strings(&template["block"]["fields"]);
            let mut names = labels(&template["label"]);
            if let Some(first) = names.first_mut() { if !fields.is_empty() { *first = format!("{first} ({})", fields.join(", ")); } }
            out.push(format!("{} : {}", template.string_field("name"), described(&template["vocabulary"], &names)));
        }
        out.push(String::new());
    }
    out.push(format!("Importance des indicateurs (Apps installées) : {}", indicators.iter().take(10).filter_map(|row| row.str_field("id")).collect::<Vec<_>>().join(", ")));
    out.join("\n") + "\n"
}

// ---------------------------------------------------------------- the Place: prompts, grounding, grammar (run.py)

/// Folded words, a trailing s dropped (`run.py`, `singular`).
fn singular(text: &str) -> Vec<String> { normalize(text).split(' ').filter(|word| !word.is_empty()).map(|word| word.trim_end_matches('s').to_owned()).collect() }

/// One typo apart, on words of 7 letters or more (`run.py`, `near`): shorter names are words a typo turns into others.
fn typo(a: &str, b: &str) -> bool {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    if a.len().min(b.len()) < 7 || a.len().abs_diff(b.len()) > 1 { return false; }
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut current = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() { current[j + 1] = (previous[j] + usize::from(ca != cb)).min(previous[j + 1] + 1).min(current[j] + 1); }
        previous = current;
    }
    previous[b.len()] <= 1
}

fn contains(words: &[String], phrase: &[String], same: impl Fn(&str, &str) -> bool) -> bool {
    phrase.len() <= words.len() && (0..=words.len() - phrase.len()).any(|at| phrase.iter().enumerate().all(|(k, part)| same(&words[at + k], part)))
}
fn alternatives<S: AsRef<str>>(names: &[S]) -> String {
    let mut sorted: Vec<&str> = names.iter().map(AsRef::as_ref).collect();
    sorted.sort_by_key(|name| std::cmp::Reverse(name.chars().count()));
    sorted.iter().map(|name| format!("\"{name}\"")).collect::<Vec<_>>().join(" | ")
}
fn literal(text: &str) -> String { text.replace('\\', "\\\\").replace('"', "\\\"") }

#[derive(Debug)]
pub struct Place {
    pub catalog: String,
    pub compile: Catalog,
    /// Measures and axes of each table, in the order of the catalog (`finance` holds the indicators).
    tables: Vec<(String, Vec<String>, Vec<String>)>,
    models: Vec<String>,
    /// The words naming each axis: its labels and the words of its users.
    phrases: Vec<(Vec<String>, String)>,
    intent_system: String,
    router_system: String,
}

impl Place {
    pub fn new(schema: &JsonValue, definitions: &[JsonValue], last: &str) -> Result<Place, String> {
        let catalog = catalog_text(schema, definitions, last);
        let line = |line: &str| -> Option<(String, String)> {
            let (id, rest) = line.split_once(" : ")?;
            // Identifiers of the catalog: lowercase, digits after the first letter (a column « CA 2024 » imported as ca_2024).
            let valid = |part: &str| part.bytes().next().is_some_and(|b| b.is_ascii_lowercase() || b == b'_') && part.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
            (match id.split_once('.') { Some((table, name)) => valid(table) && valid(name), None => valid(id) }).then(|| (id.to_owned(), rest.to_owned()))
        };
        let models: Vec<String> = catalog.split_once("Blocs modèles").map_or(vec![], |(_, rest)| rest.lines().filter_map(line).map(|(id, _)| id).filter(|id| id.contains('.')).collect());
        let mut tables: Vec<(String, Vec<String>, Vec<String>)> = vec![];
        let mut phrases = BTreeSet::new();
        for (id, rest) in catalog.lines().filter_map(line).filter(|(id, _)| !models.contains(id)) {
            let table = id.split_once('.').map_or("finance", |(table, _)| table).to_owned();
            let at = tables.iter().position(|(name, _, _)| *name == table).unwrap_or_else(|| { tables.push((table, vec![], vec![])); tables.len() - 1 });
            if rest.trim_end().ends_with("[axe]") {
                for part in rest.replace("[axe]", "").replace(" ; aussi : ", ",").split(['/', ',']) { let words = singular(part); if !words.is_empty() { phrases.insert((words, id.clone())); } }
                tables[at].2.push(id);
            } else { tables[at].1.push(id); }
        }
        let examples: Vec<JsonValue> = serde_json::from_str(EXAMPLES).map_err(|error| error.to_string())?;
        let example = |item: &JsonValue| match item.str_field("prev_q") {
            Some(previous) => format!("Demande : {previous}\nIntention :\n{}\nDemande suivante : {}\nDiff :\n{}", item.string_field("prev_a"), item.string_field("q"), item.string_field("a")),
            None => format!("Demande : {}{}\nIntention :\n{}", item.string_field("q"), item.str_field("values").filter(|values| !values.is_empty()).map_or(String::new(), |values| format!("\nValeurs reconnues : {values}")), item.string_field("a")),
        };
        let picked: Vec<String> = examples.iter().filter(|item| PICKED.contains(&item.str_field("id").unwrap_or_default())).map(example).collect();
        let intent_system = format!("{RULES}\n\nEXEMPLES\n\n{}\n\n{catalog}", picked.join("\n\n"));
        let routing: JsonValue = serde_json::from_str(ROUTING).map_err(|error| error.to_string())?;
        let actions: Vec<String> = items(&routing["actions"]).iter().map(|action| format!("- {} : {}", action.string_field("name"), action.string_field("text"))).collect();
        let router_system = format!("{}\n{}\n{}\n\n{catalog}", routing.string_field("intro"), actions.join("\n"), routing.string_field("outro"));
        let compile = Catalog::new(schema, definitions, last)?;
        Ok(Place { catalog, compile, tables, models, phrases: phrases.into_iter().collect(), intent_system, router_system })
    }

    pub fn intent_system(&self) -> &str { &self.intent_system }
    pub fn router_system(&self) -> &str { &self.router_system }

    /// The routing grammar: the action, then after a previous request whether this one goes on from it (`router.py`).
    pub fn router_grammar(follow: bool) -> String {
        let routing: JsonValue = serde_json::from_str(ROUTING).expect("route_actions.json");
        let actions = alternatives(&items(&routing["actions"]).iter().map(|action| action.string_field("name")).collect::<Vec<_>>());
        if follow { format!("root ::= \"action: \" ({actions}) \"\\nsuite: \" (\"oui\" | \"non\")") } else { format!("root ::= \"action: \" ({actions})") }
    }
    /// The action and, after a previous request, whether the request goes on from it, read from the routing answer.
    pub fn router_answer(text: &str) -> (String, bool) {
        let field = |key: &str| text.lines().find_map(|line| line.trim().strip_prefix(key)).map(|rest| rest.trim().to_owned()).unwrap_or_default();
        (field("action:"), field("suite:") == "oui")
    }
    /// The routing user message (`router.py`).
    pub fn router_user(request: &str, previous: Option<&str>) -> String { previous.map_or(String::new(), |previous| format!("Demande précédente : {previous}\n")) + &format!("Demande : {request}") }

    /// The axes the request names (labels and vocabulary, plural-tolerant, one typo apart), minus those a recognised
    /// value already fixes.
    pub fn ground(&self, request: &str, values: &[(String, String)]) -> Vec<String> {
        let words = singular(request);
        let mut found: BTreeSet<String> = self.phrases.iter().filter(|(phrase, _)| contains(&words, phrase, |word, part| word == part || typo(word, part))).map(|(_, axis)| axis.clone()).collect();
        if ACTIVITY.iter().any(|pattern| contains(&words, &pattern.iter().map(|word| (*word).to_owned()).collect::<Vec<_>>(), |word, part| word == part)) {
            let unit = singular("business unit");
            found.extend(self.phrases.iter().filter(|(phrase, _)| *phrase == unit).map(|(_, axis)| axis.clone()));
        }
        found.into_iter().filter(|axis| !values.iter().any(|(fixed, _)| fixed == axis)).collect()
    }

    /// The intent user message (`run.py`).
    pub fn intent_user(request: &str, values: &[(String, String)], found: &[String]) -> String {
        let mut text = format!("Demande : {request}");
        if !values.is_empty() { text += &format!("\nValeurs reconnues : {}", values.iter().map(|(axis, code)| format!("{axis}={code}")).collect::<Vec<_>>().join(", ")); }
        if !found.is_empty() { text += &format!("\nDimensions mentionnées : {}", found.join(", ")); }
        text
    }

    /// The grammar of the intent: one branch per table with its measures, the axes the request names, the values found;
    /// a diff after a previous intent (`run.py`, `make_grammar`).
    pub fn grammar(&self, found: &[String], values: &[(String, String)], follow: bool) -> String {
        let quoted = |names: &[&String]| names.iter().map(|name| format!("\"{name}\"")).collect::<Vec<_>>().join(" | ");
        let (mut rules, mut branches) = (vec![], vec![]);
        for (name, measures, axes) in &self.tables {
            if measures.is_empty() { continue; }
            let rule = format!("t-{}", name.replace('_', "-"));
            let dims: Vec<&String> = axes.iter().filter(|axis| found.contains(axis)).collect();
            let dims_rule = if dims.is_empty() { String::new() } else { format!("(\"\\ndims: \" ({0}) (\", \" ({0}))?)?", quoted(&dims)) };
            let held: Vec<String> = values.iter().filter(|(axis, _)| axes.contains(axis)).map(|(axis, code)| literal(&format!("{axis}={code}"))).collect();
            let filters = if held.is_empty() { String::new() } else { format!("(\"\\nfiltres: {}\")?", held.join(", ")) };
            rules.push(format!("{rule}-m ::= {}", alternatives(measures)));
            rules.push(format!("{rule} ::= \"\\nmesures: \" {rule}-x (\", \" {rule}-x){{0,9}} {dims_rule} {filters} tail"));
            rules.push(format!("{rule}-x ::= {rule}-m | \"ratio(\" {rule}-m \", \" {rule}-m \")\""));
            branches.push(rule);
        }
        let measure = format!("({})", branches.iter().map(|branch| format!("{branch}-x")).collect::<Vec<_>>().join(" | "));
        let dims = if found.is_empty() { String::new() } else { format!(" | \"dims: \" ({})", quoted(&found.iter().collect::<Vec<_>>())) };
        let root = if follow { "diff".to_owned() } else { format!("\"but: hors\" | \"but: \" butv ({})", branches.join(" | ")) };
        let all: Vec<&String> = self.tables.iter().flat_map(|(_, measures, _)| measures).collect();
        let mut lines = vec![
            format!("root ::= {root}"),
            "diff ::= dl (\"\\n\" dl){0,5}".to_owned(),
            format!("dl ::= (\"+\" | \"-\" | \"=\") (\"mesures: \" {measure} (\", \" {measure}){{0,5}}{dims} | blocs) | \"=\" (periode | gran | cmp | \"top: \" [0-9]{{1,3}} | tri)"),
            format!("tri ::= \"tri: \" ({} | \"ecart\") \" \" (\"asc\" | \"desc\")", alternatives(&all)),
            format!("bloc ::= \"kpi\" | \"courbe\" | \"barres\" | \"tableau\" | \"classement\" | \"repartition\" | \"ecarts\" | \"objectif\" | \"cascade\" | \"commentaire\" | \"modele:\" ({})", alternatives(&self.models)),
        ];
        lines.extend(rules);
        lines.push(TAIL.to_owned());
        lines.join("\n")
    }

    /// The words of the catalog: a value never matches one of them alone (« VENTES PLUS » is not « les ventes »).
    fn common_words(&self) -> Vec<String> {
        self.catalog.lines().filter_map(|line| line.split_once(" : ").map(|(_, rest)| rest)).flat_map(|rest| rest.split(['/', ',', ';']).map(|part| part.replace("aussi :", "").replace("[axe]", "").trim().to_owned()).collect::<Vec<_>>()).filter(|part| !part.is_empty()).collect()
    }
}

// ---------------------------------------------------------------- values of the Place

/// The names of each axis whose values are indexed (`value_queries`): a referential is named by the axes tied to it and
/// by the words of its code field, an axis of a table by its labels, name and vocabulary.
fn axis_names(schema: &JsonValue) -> BTreeMap<String, Vec<String>> {
    let mut names: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut referential_words: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for table in items(&schema["tables"]) {
        let Some(name) = table.str_field("provides").filter(|name| !name.is_empty()) else { continue };
        let code = items(&table["fields"]).iter().find(|field| field.str_field("name") == Some("code"));
        referential_words.entry(name.to_owned()).or_default().extend(code.into_iter().flat_map(|field| strings(&field["vocabulary"]["fr"]).into_iter().chain(strings(&field["vocabulary"]["en"]))));
    }
    for collection in items(&schema["collections"]) {
        for field in items(&collection["fields"]).iter().filter(|field| field.str_field("role") == Some("axis")) {
            let own: Vec<String> = labels(&field["label"]).into_iter().chain([field.string_field("name")]).chain(strings(&field["vocabulary"]["fr"])).chain(strings(&field["vocabulary"]["en"])).collect();
            if let Some(referential) = field.str_field("dimension").filter(|name| referential_words.contains_key(*name)) { names.entry(referential.to_owned()).or_default().extend(own.clone()); }
            names.entry(format!("{}.{}", collection.string_field("name"), field.string_field("name"))).or_default().extend(own);
        }
    }
    for (name, words) in referential_words { names.entry(name).or_default().extend(words); }
    names.into_iter().map(|(axis, words)| (axis, dedupe(words))).collect()
}

/// The identifier of the intent for an indexed axis: a referential is the axis of the indicators tied to it
/// (`org` → `entity`); an axis of a table is its catalog identifier.
fn intent_axis(schema: &JsonValue, axis: &str) -> Option<String> {
    if axis.contains('.') { return Some(axis.to_owned()); }
    items(&schema["views"]).iter().filter(|view| view["measures"].str_field("from").is_some()).flat_map(|view| items(&view["fields"])).find(|field| field.str_field("dimension") == Some(axis)).map(|field| field.string_field("name"))
}

// ---------------------------------------------------------------- one request

#[derive(Debug, Clone, PartialEq)]
pub struct Previous { pub request: String, pub action: String, pub intent: Option<String> }

/// The Place as the generation reads it, built once and kept a few minutes (`run`).
#[derive(Debug)]
pub struct PlaceModel { pub place: Place, pub schema: JsonValue, index: ValueIndex, concepts: Vec<Concept>, pub last: String }

impl PlaceModel {
    /// From `place.schema`, the indicator definitions, the last month of the data and the rows of `value_queries`
    /// (axis, code, label).
    pub fn new(schema: JsonValue, definitions: &[JsonValue], last: &str, values: Vec<(String, String, String)>) -> Result<PlaceModel, String> {
        let place = Place::new(&schema, definitions, last)?;
        let index = ValueIndex::new(values, axis_names(&schema), &place.common_words());
        Ok(PlaceModel { place, schema, index, concepts: route::concepts(), last: last.to_owned() })
    }

    /// What a search looks for in the request (`ValueIndex::subject`).
    pub fn subject(&self, request: &str) -> String { self.index.subject(request) }

    /// The values the request names, as filters of the intent: the best one of each axis, found by text, each word of
    /// the request naming one value at most (« Glacier Lyon SAS » is not also the client « LYON FROID »).
    pub fn values(&self, request: &str) -> Vec<(String, String)> {
        let (mut out, mut used): (Vec<(String, String)>, BTreeSet<String>) = (vec![], BTreeSet::new());
        for found in self.index.find(request, 0.5) {
            let Some(axis) = intent_axis(&self.schema, &found.axis) else { continue };
            let words: Vec<String> = found.matched.split(' ').map(str::to_owned).collect();
            if found.by_meaning || words.iter().any(|word| used.contains(word)) || out.iter().any(|(seen, code)| *seen == axis || *code == found.code) { continue; }
            used.extend(words);
            out.push((axis, found.code));
        }
        out
    }

    /// The concepts the request names that the Place lacks (`route.rs`).
    pub fn absent(&self, request: &str) -> Vec<&Concept> { route::absent_concepts(request, &self.place.catalog, &self.concepts) }
}

/// The text of « absent » data: what the Place lacks, and what to import.
pub fn absent_text(concepts: &[&Concept], lang: Lang) -> String {
    let names: Vec<&str> = concepts.iter().map(|concept| if lang == Lang::Fr { concept.label_fr.as_str() } else { concept.label_en.as_str() }).collect();
    if names.is_empty() {
        return match lang {
            Lang::Fr => "Les données de cette Place ne permettent pas de répondre à cette demande.".into(),
            Lang::En => "The data of this Place cannot answer this request.".into(),
        };
    }
    match lang {
        Lang::Fr => format!("Cette Place ne contient pas de données sur : {}. Importez-les dans une App (ou créez une table) pour pouvoir les analyser.", names.join(", ")),
        Lang::En => format!("This Place holds no data on: {}. Import them into an App (or create a table) to analyse them.", names.join(", ")),
    }
}

/// The text of what `place.search` found (`results`), after `intro`: one line per hit, what it is and where it lies.
pub fn found_text(intro: &str, results: &[JsonValue], lang: Lang) -> String {
    let fr = lang == Lang::Fr;
    let lines: Vec<String> = results.iter().map(|hit| {
        let text = hit.str_field("text").unwrap_or_default();
        match hit.str_field("kind") {
            Some("value") => {
                // A label is a text or one per language.
                let label = |key: &str| hit[key].as_str().or_else(|| hit[key][if fr { "fr" } else { "en" }].as_str()).filter(|label| !label.is_empty());
                let field = label("label").or_else(|| hit.str_field("field")).unwrap_or_default();
                let table = label("table").or_else(|| hit.str_field("collection")).unwrap_or_default();
                let count = hit.u64_field("count").unwrap_or(0);
                let rows = match (fr, count) { (true, 1) => "1 ligne".to_owned(), (true, n) => format!("{n} lignes"), (false, 1) => "1 row".to_owned(), (false, n) => format!("{n} rows") };
                format!("- « {text} » : {table} · {field} ({rows})")
            }
            Some("file") => format!("- {} {}", if fr { "Fichier" } else { "File" }, hit.str_field("path").unwrap_or(text)),
            Some("passage") => format!("- {} {} : « {text} »", if fr { "Fichier" } else { "File" }, hit.str_field("path").unwrap_or_default()),
            Some("folder") => format!("- {} {}", if fr { "Dossier" } else { "Folder" }, hit.str_field("path").unwrap_or(text)),
            Some("app") => format!("- App {}", hit.str_field("name").unwrap_or(text)),
            Some("table") => format!("- Table {text}"),
            _ => format!("- {text}"),
        }
    }).collect();
    format!("{intro}\n{}", lines.join("\n"))
}

/// Rows a table of an exploration shows at most.
pub const EXPLORE_ROWS: usize = 50;

/// What Ask shows of what `place.search` found, beyond the list: the strongest hits (nine tenths of the best score at
/// least) explored. Values become a table per collection and field, its rows holding one of them (the most recent first,
/// the declared fields as columns): `query` and `count` are what to read, `rows` and `total` what they gave. Files,
/// folders and passages become one list of files.
pub fn explore(found: &[JsonValue], schema: &JsonValue, lang: Lang) -> Vec<JsonValue> {
    let best = found.iter().filter_map(|hit| hit["score"].as_f64()).fold(0.0, f64::max);
    let strong: Vec<&JsonValue> = found.iter().filter(|hit| hit["score"].as_f64().is_some_and(|score| score >= best * 0.9)).collect();
    let label = |value: &JsonValue| value.as_str().or_else(|| value[if lang == Lang::Fr { "fr" } else { "en" }].as_str()).map(str::to_owned);
    let mut out: Vec<JsonValue> = Vec::new();
    for hit in strong.iter().filter(|hit| hit.str_field("kind") == Some("value")) {
        let (Some(collection), Some(field), Some(text)) = (hit.str_field("collection"), hit.str_field("field"), hit.str_field("text")) else { continue };
        if let Some(table) = out.iter_mut().find(|table| table["collection"] == collection && table["field"] == field) {
            if let Some(values) = table["values"].as_array_mut().filter(|values| values.len() < 8) { values.push(text.into()); }
            continue;
        }
        if out.len() >= 2 { continue; }
        let declared = schema["collections"].as_array().into_iter().flatten().find(|declared| declared.str_field("name") == Some(collection));
        let fields: Vec<&JsonValue> = declared.and_then(|declared| declared["fields"].as_array()).into_iter().flatten().filter(|declared| declared.str_field("name").is_some_and(|name| crate::view::is_name(name) && !name.starts_with('_'))).collect();
        let mut columns: Vec<JsonValue> = fields.iter().take(10).map(|declared| json!({"field": declared["name"], "label": label(&declared["label"]).unwrap_or_else(|| declared.string_field("name")), "type": declared["type"]})).collect();
        if !columns.iter().any(|column| column["field"] == field) { columns.insert(0, json!({"field": field, "label": label(&hit["label"]).unwrap_or_else(|| field.to_owned())})); }
        let title = format!("{} · {}", label(&hit["table"]).unwrap_or_else(|| collection.to_owned()), label(&hit["label"]).unwrap_or_else(|| field.to_owned()));
        out.push(json!({"kind": "table", "title": title, "collection": collection, "field": field, "values": [text], "columns": columns, "dated": fields.iter().any(|declared| declared["name"] == "date")}));
    }
    for table in &mut out {
        let (collection, field) = (table.string_field("collection"), table.string_field("field"));
        let filter = format!("on {collection} | where {field} in {}", serde_json::to_string(&table["values"]).unwrap_or_default());
        let order = if table["dated"] == true { " | sort date desc" } else { "" };
        table["query"] = format!("{filter}{order} | limit {EXPLORE_ROWS}").into();
        table["count"] = format!("{filter} | count").into();
        if let Some(table) = table.as_object_mut() { table.remove("dated"); }
    }
    let mut files: Vec<JsonValue> = Vec::new();
    for hit in strong.iter().filter(|hit| matches!(hit.str_field("kind"), Some("file" | "folder" | "passage"))) {
        if files.iter().any(|file| file["fileId"] == hit["fileId"]) { continue; }
        files.push(json!({"kind": hit["kind"], "fileId": hit["fileId"], "instanceId": hit["instanceId"], "path": hit.get("path").cloned().unwrap_or_else(|| hit["text"].clone()), "excerpt": if hit["kind"] == "passage" { hit["text"].clone() } else { JsonValue::Null }}));
    }
    if !files.is_empty() { out.push(json!({"kind": "files", "items": files})); }
    out
}

/// The intent read from the model's answer, the previous one applied for a diff.
pub fn read_intent(text: &str, previous: Option<&str>) -> Intent { Intent::parse(text, previous.map(|text| Intent::parse(text, None)).as_ref()) }

/// The intent written back as the model writes it: the previous turn of a follow-up.
pub fn intent_text(intent: &Intent) -> String {
    let mut lines = vec![format!("but: {}", intent.but)];
    let list = |key: &str, values: &[String]| (!values.is_empty()).then(|| format!("{key}: {}", values.join(", ")));
    lines.extend(list("mesures", &intent.mesures));
    lines.extend(list("dims", &intent.dims));
    lines.extend(list("filtres", &intent.filtres.iter().map(|(axis, code)| format!("{axis}={code}")).collect::<Vec<_>>()));
    if intent.periode != "dernier" { lines.push(format!("periode: {}", intent.periode)); }
    if intent.gran != "ytd" { lines.push(format!("gran: {}", intent.gran)); }
    if intent.cmp != "n1" { lines.push(format!("cmp: {}", intent.cmp)); }
    lines.extend(list("blocs", &intent.blocs));
    if let Some(top) = intent.top { lines.push(format!("top: {top}")); }
    if let Some(tri) = &intent.tri { lines.push(format!("tri: {tri}")); }
    if let Some(note) = &intent.note { lines.push(format!("note: {note}")); }
    lines.join("\n")
}

/// Compiles an intent of the Place; `hors` and the refusals of the compiler come back as errors.
pub fn compile_intent(model: &PlaceModel, intent: &Intent, lang: Lang) -> Result<compile::Compiled, String> { compile::compile(intent, &model.place.compile, lang) }

// ---------------------------------------------------------------- the operation (agent.app)

#[cfg(feature = "agent")]
pub use running::run;

#[cfg(feature = "agent")]
mod running {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant};

    use serde_json::{json, Map, Value as JsonValue};

    use super::{absent_text, compile_intent, explore, found_text, intent_text, read_intent, Lang, Place, PlaceModel};
    use crate::helpers::{fnv1a64, JsonFields};
    use crate::operation::{AgentAppInput, AgentAppPrevious};
    use crate::service::agent::{response_documents, AgentCapabilityInvoker, AgentError};
    use crate::service::values::value_queries;

    /// How long the model of a Place (schema, catalog, values) is kept: a conversation reads it once.
    const KEEP: Duration = Duration::from_secs(300);
    const ROUTE_TOKENS: u32 = 20;
    const INTENT_TOKENS: u32 = 160;
    /// The hits of `place.search` an answer lists.
    const SEARCH_HITS: u32 = 8;

    fn models() -> &'static Mutex<HashMap<String, (Instant, Arc<PlaceModel>)>> { static MODELS: OnceLock<Mutex<HashMap<String, (Instant, Arc<PlaceModel>)>>> = OnceLock::new(); MODELS.get_or_init(Default::default) }

    fn rows(invoker: &mut dyn AgentCapabilityInvoker, query: String) -> Result<Vec<JsonValue>, AgentError> {
        Ok(response_documents(&invoker.invoke("query.execute", json!({"query": query, "readOnly": true}))?).into_iter().cloned().collect())
    }

    /// The month of a Unix time in milliseconds, `YYYY-MM` (civil calendar).
    fn month_of_millis(millis: u128) -> String {
        let days = (millis / 86_400_000) as i64 + 719_468;
        let (era, day) = (days.div_euclid(146_097), days.rem_euclid(146_097));
        let year_of_era = (day - day / 1460 + day / 36_524 - day / 146_096) / 365;
        let day_of_year = day - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month = (5 * day_of_year + 2) / 153;
        let month = if month < 10 { month + 3 } else { month - 9 };
        format!("{}-{month:02}", year_of_era + era * 400 + i64::from(month <= 2))
    }

    /// Reads the Place: its schema, the definitions of the indicators its views publish, the last month of its data
    /// (actual rows where a scenario is held) and the values of its axes.
    fn load(invoker: &mut dyn AgentCapabilityInvoker, place_id: &str) -> Result<PlaceModel, AgentError> {
        let schema = invoker.invoke("place.schema", json!({"placeId": place_id}))?.data.unwrap_or_default();
        let mut definitions = vec![];
        for view in schema["views"].as_array().into_iter().flatten() {
            if let Some(from) = view["measures"].str_field("from") { definitions.extend(rows(invoker, format!("on view {from}"))?); }
        }
        let mut last: Option<String> = None;
        for collection in schema["collections"].as_array().into_iter().flatten() {
            let fields = collection["fields"].as_array().map_or(&[][..], Vec::as_slice);
            let has = |name: &str| fields.iter().any(|field| field.str_field("name") == Some(name));
            let Some(name) = collection.str_field("name").filter(|_| has("date")) else { continue };
            let actual = if has("scenario") { " | where scenario == \"actual\"" } else { "" };
            for row in rows(invoker, format!("on {name}{actual} | where exists(date) | sort date desc | limit 1 | select date"))? {
                if let Some(month) = row.str_field("date").and_then(|date| date.get(..7)).filter(|month| month.as_bytes().get(4) == Some(&b'-')) {
                    if last.as_deref().is_none_or(|seen| month > seen) { last = Some(month.to_owned()); }
                }
            }
        }
        let last = last.unwrap_or_else(|| month_of_millis(crate::helpers::unix_time_millis() as u128));
        let mut values = vec![];
        for (axis, query) in value_queries(&schema) {
            for row in rows(invoker, query)? {
                let Some(code) = row.get("code").and_then(|code| code.as_str().map(str::to_owned).or_else(|| code.is_number().then(|| code.to_string()))) else { continue };
                values.push((axis.clone(), code.clone(), row.str_field("label").map_or(code, str::to_owned)));
            }
        }
        PlaceModel::new(schema, &definitions, &last, values).map_err(|error| AgentError::capability("place.schema", error))
    }

    fn place_model(invoker: &mut dyn AgentCapabilityInvoker, key: &str, place_id: &str) -> Result<Arc<PlaceModel>, AgentError> {
        if let Some((at, model)) = models().lock().map_err(|_| AgentError::Configuration("place models lock".into()))?.get(key) { if at.elapsed() < KEEP { return Ok(model.clone()); } }
        let model = Arc::new(load(invoker, place_id)?);
        let mut cache = models().lock().map_err(|_| AgentError::Configuration("place models lock".into()))?;
        cache.retain(|_, (at, _)| at.elapsed() < KEEP);
        cache.insert(key.to_owned(), (Instant::now(), model.clone()));
        Ok(model)
    }

    /// One answer of the model under grammar, greedy, without reasoning, the prefix of its system message kept in cache.
    fn complete(invoker: &mut dyn AgentCapabilityInvoker, messages: JsonValue, grammar: String, max_tokens: u32, cache: &str, tokens: &mut Map<String, JsonValue>) -> Result<String, AgentError> {
        let response = invoker.invoke("llm.generate", json!({"messages": messages, "grammar": grammar, "greedy": true, "thinking": false, "maxTokens": max_tokens, "cacheKey": cache, "keepCache": true}))?;
        if let Some(statistics) = response.statistics.as_ref() {
            for key in ["promptTokens", "cachedPromptTokens", "completionTokens"] { let sum = tokens.get(key).and_then(JsonValue::as_u64).unwrap_or(0) + statistics[key].as_u64().unwrap_or(0); tokens.insert(key.into(), sum.into()); }
        }
        Ok(response.partials.iter().filter(|partial| partial.str_field("type") == Some("token")).filter_map(|partial| partial.str_field("text")).collect())
    }

    /// Answers one request: the values it names, the action, and for an App its intent and definition. Each step is
    /// sent as it ends (`values`, `route`, `intent`, `app`); the result holds the action, the intent to send back with a
    /// follow-up, and the time of each step. `key` names the reader of the Place (its model is kept per reader).
    pub fn run(input: &AgentAppInput, key: &str, invoker: &mut dyn AgentCapabilityInvoker, mut emit: impl FnMut(JsonValue) -> bool) -> Result<JsonValue, AgentError> {
        let mut send = |event: JsonValue| if emit(event) { Ok(()) } else { Err(AgentError::Cancelled) };
        let mut work = Work::start(input, key, invoker)?;
        send(json!({"type": "values", "values": work.values.iter().map(|(axis, code)| json!({"axis": axis, "code": code})).collect::<Vec<_>>()}))?;
        let (action, previous, event) = work.route(input, invoker)?;
        send(event)?;
        // A search finding nothing asked for an analysis (« les créances échues de plus de 90 jours »): its App.
        let action = if action == "rechercher" && !work.result["search"].as_array().is_some_and(|hits| !hits.is_empty()) {
            work.result["searched"] = true.into(); work.result["action"] = "app".into();
            if let Some(result) = work.result.as_object_mut() { result.remove("text"); }
            "app".to_owned()
        } else { action };
        if action == "app" { work.app(input, previous, invoker, &mut send)?; }
        Ok(work.finish())
    }


    /// One request under way: the model of its Place, the values it names, the result being written.
    struct Work { model: Arc<PlaceModel>, lang: Lang, values: Vec<(String, String)>, result: JsonValue, timings: Map<String, JsonValue>, tokens: Map<String, JsonValue>, clock: Instant }

    impl Work {
        fn start(input: &AgentAppInput, key: &str, invoker: &mut dyn AgentCapabilityInvoker) -> Result<Work, AgentError> {
            let clock = Instant::now();
            let model = place_model(invoker, &format!("{key}|{}", input.context.place_id), &input.context.place_id)?;
            let values = model.values(input.request.trim());
            let result = json!({"values": values.iter().map(|(axis, code)| format!("{axis}={code}")).collect::<Vec<_>>(), "last": model.last});
            let mut work = Work { model, lang: if input.lang.as_deref() == Some("en") { Lang::En } else { Lang::Fr }, values, result, timings: Map::new(), tokens: Map::new(), clock };
            work.lap("place");
            Ok(work)
        }
        /// Absent data first; else the model routes the request and, after a previous one, says whether it goes on from
        /// it: only then does the previous turn count (its action for the routing rules, its intent for a diff). Returns
        /// the action, the previous turn kept and the `route` event.
        fn route<'a>(&mut self, input: &'a AgentAppInput, invoker: &mut dyn AgentCapabilityInvoker) -> Result<(String, Option<&'a AgentAppPrevious>, JsonValue), AgentError> {
            let request = input.request.trim();
            let model = self.model.clone();
            let absent = model.absent(request);
            let asked = input.previous.as_ref();
            let (proposed, action, previous) = if absent.is_empty() {
                let system = model.place.router_system();
                let cache = format!("route:{:016x}", fnv1a64(system.as_bytes()));
                let messages = json!([{"role": "system", "content": system}, {"role": "user", "content": Place::router_user(request, asked.map(|previous| previous.request.as_str()))}]);
                let text = complete(invoker, messages, Place::router_grammar(asked.is_some()), ROUTE_TOKENS, &cache, &mut self.tokens)?;
                let (proposed, follows) = Place::router_answer(&text);
                let previous = asked.filter(|_| follows);
                let action = crate::service::route::route(request, previous.map(|previous| previous.action.as_str()), &proposed, &model.place.catalog, &[]);
                (Some(proposed), action, previous)
            } else { (None, "absent".to_owned(), None) };
            self.lap("route");
            let follow_up = previous.is_some_and(|previous| previous.action == "app");
            self.result["follows"] = previous.is_some().into();
            self.result["followUp"] = follow_up.into();
            self.result["action"] = action.as_str().into();
            self.result["proposed"] = proposed.clone().into();
            // Data the catalog lacks, or rows to find: what the Place holds that the request names (`place.search`).
            let mut found = Vec::new();
            let subject = model.subject(request);
            if matches!(action.as_str(), "absent" | "rechercher") && !subject.is_empty() {
                match invoker.invoke("place.search", json!({"placeId": input.context.place_id, "text": subject, "limit": SEARCH_HITS})) {
                    Ok(response) => found = response.data.as_ref().and_then(|data| data["results"].as_array().cloned()).unwrap_or_default(),
                    Err(error) => self.result["searchError"] = error.to_string().into(),
                }
                self.lap("search");
                self.result["search"] = found.clone().into();
                // The strongest hits explored: the rows of their values, the files.
                let mut explored = explore(&found, &model.schema, self.lang);
                for table in explored.iter_mut().filter(|item| item["kind"] == "table") {
                    let (query, count) = (table.string_field("query"), table.string_field("count"));
                    match (rows(invoker, query), rows(invoker, count)) {
                        (Ok(rows), Ok(count)) => { table["total"] = count.first().map_or(JsonValue::Null, |row| row["count"].clone()); table["rows"] = rows.into(); }
                        (Err(error), _) | (_, Err(error)) => table["error"] = error.to_string().into(),
                    }
                }
                if !explored.is_empty() { self.lap("explore"); self.result["explore"] = explored.into(); }
            }
            let fr = self.lang == Lang::Fr;
            match action.as_str() {
                "absent" if found.is_empty() => self.result["text"] = absent_text(&absent, self.lang).into(),
                "absent" if absent.is_empty() => self.result["text"] = found_text(if fr { "Le catalogue d'analyse ne couvre pas cette demande, mais la Place contient :" } else { "The analysis catalog does not cover this request, but the Place holds:" }, &found, self.lang).into(),
                "absent" => self.result["text"] = format!("{}\n\n{}", absent_text(&absent, self.lang), found_text(if fr { "La Place contient toutefois :" } else { "The Place does hold:" }, &found, self.lang)).into(),
                "rechercher" if found.is_empty() => self.result["text"] = (if fr { "Rien de tel n'a été trouvé dans la Place." } else { "Nothing like this was found in the Place." }).into(),
                "rechercher" => self.result["text"] = found_text(if fr { "Trouvé dans la Place :" } else { "Found in the Place:" }, &found, self.lang).into(),
                _ => {}
            }
            let event = json!({"type": "route", "action": action, "proposed": proposed, "followUp": follow_up, "absent": absent.iter().map(|concept| concept.id.as_str()).collect::<Vec<_>>(), "search": found});
            Ok((action, previous, event))
        }
        fn lap(&mut self, step: &str) { self.timings.insert(step.into(), (self.clock.elapsed().as_millis() as u64).into()); self.clock = Instant::now(); }
        fn finish(mut self) -> JsonValue { self.result["timings"] = std::mem::take(&mut self.timings).into(); self.result["tokens"] = std::mem::take(&mut self.tokens).into(); self.result }

        /// The intent under the grammar of the Place, a diff after an App, compiled.
        fn app(&mut self, input: &AgentAppInput, previous: Option<&AgentAppPrevious>, invoker: &mut dyn AgentCapabilityInvoker, send: &mut dyn FnMut(JsonValue) -> Result<(), AgentError>) -> Result<(), AgentError> {
            let request = input.request.trim();
            let mut before = previous.filter(|previous| previous.action == "app").and_then(|previous| previous.intent.as_deref().map(|intent| (previous.request.as_str(), intent)));
            let found = self.model.place.ground(request, &self.values);
            let system = self.model.place.intent_system();
            let cache = format!("intent:{:016x}", fnv1a64(system.as_bytes()));
            let (answer, intent) = loop {
                let mut messages = vec![json!({"role": "system", "content": system})];
                if let Some((request, intent)) = before { messages.extend([json!({"role": "user", "content": format!("Demande : {request}")}), json!({"role": "assistant", "content": intent})]); }
                messages.push(json!({"role": "user", "content": Place::intent_user(request, &self.values, &found)}));
                let grammar = self.model.place.grammar(&found, &self.values, before.is_some());
                let answer = complete(invoker, JsonValue::Array(messages), grammar, INTENT_TOKENS, &cache, &mut self.tokens)?;
                let intent = read_intent(&answer, before.map(|(_, intent)| intent));
                // A diff keeping none of the previous measures asks about something else: a new request after all.
                let Some((_, text)) = before else { break (answer, intent) };
                let subject = self.model.place.compile.subject(&read_intent(text, None));
                if subject.is_empty() || !subject.is_disjoint(&self.model.place.compile.subject(&intent)) { break (answer, intent); }
                before = None;
                self.result["follows"] = false.into();
                self.result["followUp"] = false.into();
                self.result["diffDropped"] = answer.into();
            };
            self.lap("intent");
            send(json!({"type": "intent", "answer": answer, "intent": intent_text(&intent), "followUp": before.is_some()}))?;
            self.result["answer"] = answer.into();
            self.result["intent"] = intent_text(&intent).into();
            if intent.but == "hors" {
                self.result["action"] = "hors".into();
                return Ok(());
            }
            match compile_intent(&self.model, &intent, self.lang) {
                Ok(compiled) => { self.result["gaps"] = json!(compiled.gaps); send(json!({"type": "app", "definition": compiled.definition, "gaps": compiled.gaps}))?; }
                Err(error) => self.result["error"] = error.into(),
            }
            self.lap("compile");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn the_strongest_hits_are_explored_as_tables_of_their_rows_and_a_list_of_files() { let schema = json!({"collections": [{"name": "finance", "fields": [{"name": "date"}, {"name": "account_label", "label": {"fr": "Libellé", "en": "Label"}}, {"name": "amount", "label": "Montant"}, {"name": "_place"}]}]}); let hit = |kind: &str, text: &str, score: f64, more: JsonValue| { let mut hit = json!({"kind": kind, "text": text, "score": score}); hit.as_object_mut().unwrap().extend(more.as_object().unwrap().clone()); hit }; let found = [hit("value", "T.V.A. non récupérable", 10.0, json!({"collection": "finance", "field": "account_label", "table": "Finance", "label": {"fr": "Libellé"}})), hit("value", "T.V.A. A PAYER", 9.5, json!({"collection": "finance", "field": "account_label"})), hit("passage", "… TVA …", 9.2, json!({"fileId": "x", "path": "Compta/notes.md"})), hit("value", "RURAL 31", 4.0, json!({"collection": "creances", "field": "name"}))]; let explored = explore(&found, &schema, Lang::Fr); assert_eq!(explored.len(), 2); assert_eq!((explored[0]["title"].as_str(), explored[0]["query"].as_str(), explored[0]["count"].as_str()), (Some("Finance · Libellé"), Some("on finance | where account_label in [\"T.V.A. non récupérable\",\"T.V.A. A PAYER\"] | sort date desc | limit 50"), Some("on finance | where account_label in [\"T.V.A. non récupérable\",\"T.V.A. A PAYER\"] | count"))); assert_eq!(explored[0]["columns"], json!([{"field": "date", "label": "date", "type": null}, {"field": "account_label", "label": "Libellé", "type": null}, {"field": "amount", "label": "Montant", "type": null}])); assert_eq!(explored[1], json!({"kind": "files", "items": [{"kind": "passage", "fileId": "x", "instanceId": null, "path": "Compta/notes.md", "excerpt": "… TVA …"}]})); }
    #[test] fn found_hits_are_listed_and_absent_without_concepts_says_so() { let hits = [json!({"kind": "value", "text": "T.V.A. collectée", "count": 3, "collection": "comptes", "table": {"fr": "Comptes", "en": "Accounts"}, "field": "label", "label": "Libellé"}), json!({"kind": "file", "text": "TVA.pdf", "path": "Compta/TVA.pdf"}), json!({"kind": "passage", "text": "… la TVA collectée ligne 08 …", "path": "Compta/notes.md"})]; assert_eq!(found_text("Trouvé :", &hits, Lang::Fr), "Trouvé :\n- « T.V.A. collectée » : Comptes · Libellé (3 lignes)\n- Fichier Compta/TVA.pdf\n- Fichier Compta/notes.md : « … la TVA collectée ligne 08 … »"); assert!(!absent_text(&[], Lang::Fr).contains(": .")); }

    #[test] fn intents_write_back_as_the_model_writes_them() {
        let text = "but: classer\nmesures: ventes.revenue\ndims: ventes.client\nfiltres: ventes.family=F08\nperiode: 12m\ngran: mois\nblocs: classement\ntop: 5\ntri: ventes.revenue desc";
        assert_eq!(intent_text(&read_intent(text, None)), text);
        assert_eq!(intent_text(&read_intent("+dims: entity\n=cmp: budget", Some(text))), text.replace("dims: ventes.client", "dims: ventes.client, entity").replace("gran: mois", "gran: mois\ncmp: budget"));
    }
}
