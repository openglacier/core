#![cfg_attr(rustfmt, rustfmt_skip)]
//! Compiler of an intent into an App (wip/APP_MODELE_SEMANTIQUE.md, step 7). The language model writes a short intent,
//! one key per line (`src/service/intent_rules.txt`); this code turns it into an App definition og-hub renders as it renders the
//! Builder's: report blocks over the sources of the Place, a filter bar carrying the period, granularity and comparison.
//!
//! - Finance indicators read the view `finance.indicators` (sources `indicators`, `history`, `history_month`, as the
//!   finance Apps name them); a table of the Place (`ventes`, `creances`…) reads one grouped query by month and by the
//!   axes the intent names, whose fields carry the units, directions and aggregations of `place.schema`. A measure
//!   averaged over rows (`aggregate: avg`) is summed with the number of rows and read as their ratio.
//! - Template blocks (`modele:creances.clientaging`) copy the block an installed App offers (`template` on the block,
//!   listed by `place.schema`) with the sources it reads.
//! - A period of N months (`12m`) is a rolling window of the analysis bar (`r12`); finance indicators by entity read the
//!   view with one row per entity (`byEntity`); a ranking by `tri: … asc` or `ecart asc|desc` sets the order of its block.
//! - What the runtime cannot show yet is compiled to the closest it can and listed in `gaps`, never dropped silently
//!   (a ratio of two indicators, a template the Place lacks).

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value as JsonValue};

use crate::helpers::{fnv1a64, JsonFields};

#[derive(Debug, Clone, PartialEq)]
pub struct Intent {
    pub but: String, pub mesures: Vec<String>, pub dims: Vec<String>, pub filtres: Vec<(String, String)>, pub periode: String,
    pub gran: String, pub cmp: String, pub blocs: Vec<String>, pub top: Option<u32>, pub tri: Option<String>, pub note: Option<String>,
}

impl Default for Intent {
    fn default() -> Self { Intent { but: String::new(), mesures: vec![], dims: vec![], filtres: vec![], periode: "dernier".into(), gran: "ytd".into(), cmp: "n1".into(), blocs: vec![], top: None, tri: None, note: None } }
}

/// Values of a list key, split on the commas outside parentheses (`ratio(a, b)` stays one value, written without spaces).
fn split_values(text: &str) -> Vec<String> {
    let (mut out, mut depth, mut current) = (vec![], 0i32, String::new());
    for character in text.chars() {
        match character { '(' => depth += 1, ')' => depth -= 1, ',' if depth == 0 => { out.push(std::mem::take(&mut current)); continue; } _ => {} }
        current.push(character);
    }
    out.push(current);
    out.into_iter().map(|value| { let value = value.trim(); if value.starts_with("ratio(") { value.split_whitespace().collect() } else { value.to_owned() } }).filter(|value: &String| !value.is_empty()).collect()
}

fn filter_pair(value: &str) -> Option<(String, String)> { let (axis, code) = value.split_once('=')?; Some((axis.trim().to_owned(), code.trim().to_owned())) }

impl Intent {
    /// An intent, or the previous one with a diff applied: `+key: values` adds, `-key: values` removes, `=key: value`
    /// replaces. A key with one value (period, granularity, comparison, top, sort) is always replaced, whatever the sign.
    pub fn parse(text: &str, previous: Option<&Intent>) -> Intent {
        let mut intent = previous.cloned().unwrap_or_default();
        for line in text.lines() {
            let line = line.trim();
            let (sign, rest) = match line.chars().next() { Some(sign @ ('+' | '-' | '=')) => (sign, line[1..].trim_start()), _ => ('=', line) };
            let Some((key, value)) = rest.split_once(':') else { continue };
            let (key, value) = (key.trim(), value.trim());
            let list = match key {
                "mesures" => &mut intent.mesures, "dims" => &mut intent.dims, "blocs" => &mut intent.blocs,
                "filtres" => {
                    let pairs: Vec<_> = split_values(value).iter().filter_map(|value| filter_pair(value)).collect();
                    match (sign, previous.is_some()) {
                        ('-', _) => intent.filtres.retain(|pair| !pairs.contains(pair)),
                        ('+', _) => for pair in pairs { if !intent.filtres.contains(&pair) { intent.filtres.push(pair); } },
                        _ => intent.filtres = pairs,
                    }
                    continue;
                }
                "but" => { intent.but = value.into(); continue; }
                "periode" => { intent.periode = value.into(); continue; }
                "gran" => { intent.gran = value.into(); continue; }
                "cmp" => { intent.cmp = value.into(); continue; }
                "top" => { intent.top = value.parse().ok(); continue; }
                "tri" => { intent.tri = Some(value.into()).filter(|value: &String| !value.is_empty()); continue; }
                "note" => { intent.note = Some(value.into()).filter(|value: &String| !value.is_empty()); continue; }
                _ => continue,
            };
            let values = split_values(value);
            match sign {
                '-' => list.retain(|item| !values.contains(item)),
                '+' => for value in values { if !list.contains(&value) { list.push(value); } },
                _ => *list = values,
            }
        }
        intent
    }

    /// The period equivalences of the bench (`run.py`, `normp`): the latest month is `dernier`, `M03` the last March the
    /// data hold, the current year `ytd`, and a year to date shown by year is a year to date.
    pub fn normalize(&mut self, last: (i32, u32)) {
        let (year, month) = last;
        if let Some(wanted) = self.periode.strip_prefix('M').and_then(|text| text.parse::<u32>().ok()).filter(|_| self.periode.len() == 3) {
            self.periode = format!("{}-{wanted:02}", if wanted > month { year - 1 } else { year });
        }
        if self.periode == format!("{year}-{month:02}") { self.periode = "dernier".into(); }
        if self.periode == year.to_string() { self.periode = "ytd".into(); }
        if self.periode == "ytd" && self.gran == "annee" { self.gran = "ytd".into(); }
    }
}

// ---------------------------------------------------------------- catalog of the Place

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Lang { Fr, En }

#[derive(Debug, Clone)]
struct Label { fr: String, en: String }
impl Label {
    fn of(value: Option<&JsonValue>, fallback: &str) -> Label {
        match value {
            Some(JsonValue::Object(map)) => { let fr = map.str_field("fr").unwrap_or(fallback).to_owned(); Label { en: map.str_field("en").map_or_else(|| fr.clone(), str::to_owned), fr } }
            Some(JsonValue::String(text)) => Label { fr: text.clone(), en: text.clone() },
            _ => Label { fr: fallback.into(), en: fallback.into() },
        }
    }
    fn get(&self, lang: Lang) -> &str { if lang == Lang::Fr { &self.fr } else { &self.en } }
}

#[derive(Debug, Clone)]
struct Field { name: String, label: Label, unit: String, good: String, aggregate: String, ratio: Option<(String, String)>, axis: bool, dimension: Option<String>,
    /// For an axis, its referential (`values`): the collection, its code field and its label field.
    values: Option<(String, String, String)>,
    /// For a measure, the finance indicator it details (`ventes.revenue` details `ca`).
    indicator: Option<String> }

#[derive(Debug, Clone)]
struct Table { collection: String, fields: Vec<Field>, columns: Vec<String> }
impl Table { fn field(&self, name: &str) -> Option<&Field> { self.fields.iter().find(|field| field.name == name) } }

#[derive(Debug, Clone)]
pub struct Catalog { last: (i32, u32), opening: u32, indicators: BTreeMap<String, Field>, tables: BTreeMap<String, Table>, templates: BTreeMap<String, (JsonValue, Map<String, JsonValue>)> }

fn month_of(text: &str) -> Option<(i32, u32)> { let (year, month) = text.split_once('-')?; Some((year.parse().ok()?, month.get(..2)?.parse().ok().filter(|month| (1..=12).contains(month))?)) }

impl Catalog {
    /// From the answer of `place.schema` (its template blocks included), the rows of `finance.indicators.definitions` and
    /// the last month of the data (`2026-09`).
    pub fn new(schema: &JsonValue, definitions: &[JsonValue], last: &str) -> Result<Catalog, String> {
        let last = month_of(last).ok_or_else(|| format!("invalid last month {last:?}"))?;
        let indicators = definitions.iter().filter_map(|row| {
            let id = row.str_field("id")?;
            Some((id.to_owned(), Field { name: id.into(), label: Label { fr: row.str_field("label_fr").unwrap_or(id).into(), en: row.str_field("label_en").unwrap_or(id).into() },
                unit: row.str_field("unit").unwrap_or("amount").into(), good: row.str_field("good").unwrap_or("up").into(), aggregate: "last".into(), ratio: None, axis: false, dimension: None, values: None, indicator: None }))
        }).collect();
        let mut tables = BTreeMap::new();
        for collection in schema.array_field("collections").map_or(&[][..], Vec::as_slice) {
            let fields: Vec<Field> = collection.array_field("fields").map_or(&[][..], Vec::as_slice).iter().filter_map(|field| {
                let name = field.str_field("name")?;
                let ratio = (field.str_field("type") == Some("ratio")).then(|| (field.string_field("num"), field.string_field("den")));
                let measure = field.str_field("role") == Some("measure") || field.str_field("type") == Some("number") && field.get("role").is_none();
                let axis = field.str_field("role") == Some("axis");
                (measure || axis || ratio.is_some()).then(|| Field { name: name.into(), label: Label::of(field.get("label"), name), unit: field.str_field("unit").unwrap_or("number").into(),
                    good: field.str_field("good").unwrap_or("up").into(), aggregate: field.str_field("aggregate").unwrap_or("sum").into(), ratio, axis, dimension: field.str_field("dimension").map(str::to_owned),
                    values: field["values"].str_field("source").zip(field["values"].str_field("code")).filter(|_| axis)
                        .map(|(source, code)| (source.to_owned(), code.to_owned(), field["values"].str_field("label").unwrap_or(code).to_owned())),
                    indicator: field.str_field("indicator").filter(|_| !axis).map(str::to_owned) })
            }).collect();
            let columns = collection.array_field("fields").map_or(&[][..], Vec::as_slice).iter().filter_map(|field| field.str_field("name")).map(str::to_owned).collect();
            if let Some(name) = collection.str_field("name") { tables.insert(name.to_owned(), Table { collection: name.into(), fields, columns }); }
        }
        let templates = schema.array_field("templates").map_or(&[][..], Vec::as_slice).iter()
            .map(|template| (template.string_field("name"), (template["block"].clone(), template.object_field("sources").cloned().unwrap_or_default()))).collect();
        Ok(Catalog { last, opening: 1, indicators, tables, templates })
    }

    /// What an intent is about: its measures, a table's measure named by the indicator it details, a ratio by both terms.
    pub fn subject(&self, intent: &Intent) -> BTreeSet<String> {
        let name = |id: &str| id.split_once('.').and_then(|(table, name)| self.tables.get(table)?.field(name)?.indicator.clone()).unwrap_or_else(|| id.to_owned());
        intent.mesures.iter().flat_map(|id| id.strip_prefix("ratio(").and_then(|rest| rest.strip_suffix(')')).map_or(vec![id.as_str()], |inner| inner.split(',').collect())).map(|id| name(id.trim())).collect()
    }
}

// ---------------------------------------------------------------- compilation

#[derive(Debug)]
pub struct Compiled { pub definition: JsonValue, pub gaps: Vec<String> }

/// A source of the App: the finance indicators, or one table of the Place.
#[derive(Debug, Default)]
struct Group { measures: Vec<Field>, axes: Vec<Field> }

const FINANCE: &str = "finance";

pub fn compile(intent: &Intent, catalog: &Catalog, lang: Lang) -> Result<Compiled, String> {
    if intent.but == "hors" { return Err("hors".into()); }
    let mut intent = intent.clone();
    intent.normalize(catalog.last);
    let mut gaps = vec![];
    // Measures and axes by source, in the order of the intent.
    let mut groups: Vec<(String, Group)> = vec![];
    fn group<'a>(groups: &'a mut Vec<(String, Group)>, name: &str) -> &'a mut Group {
        if let Some(index) = groups.iter().position(|(key, _)| key == name) { return &mut groups[index].1; }
        groups.push((name.to_owned(), Group::default()));
        &mut groups.last_mut().unwrap().1
    }
    let resolve = |id: &str| -> Result<(String, Field), String> {
        if let Some((table, name)) = id.split_once('.') {
            let field = catalog.tables.get(table).and_then(|t| t.field(name)).ok_or_else(|| format!("unknown identifier {id:?}"))?;
            return Ok((table.to_owned(), field.clone()));
        }
        if let Some(field) = catalog.indicators.get(id) { return Ok((FINANCE.into(), field.clone())); }
        if id == "entity" { return Ok((FINANCE.into(), Field { name: "entity".into(), label: Label { fr: "Département".into(), en: "Department".into() }, unit: String::new(), good: String::new(), aggregate: String::new(), ratio: None, axis: true, dimension: Some("org".into()), values: None, indicator: None })); }
        Err(format!("unknown identifier {id:?}"))
    };
    for id in &intent.mesures {
        if let Some(inner) = id.strip_prefix("ratio(").and_then(|rest| rest.strip_suffix(')')) {
            let (num, den) = inner.split_once(',').ok_or_else(|| format!("invalid ratio {id:?}"))?;
            let ((source, num), (other, den)) = (resolve(num)?, resolve(den)?);
            if source != other { return Err(format!("{id}: a ratio reads one table")); }
            if source == FINANCE { gaps.push(format!("ratio:{id}")); continue; }
            let label = Label { fr: format!("{} / {}", num.label.fr, den.label.fr), en: format!("{} / {}", num.label.en, den.label.en) };
            let entry = group(&mut groups, &source);
            for part in [&num, &den] { if !entry.measures.iter().any(|field| field.name == part.name) { entry.measures.push(Field { label: Label { fr: String::new(), en: String::new() }, ..part.clone() }); } }
            entry.measures.push(Field { name: format!("{}_{}", num.name, den.name), label, unit: if num.unit == den.unit { "percent".into() } else { num.unit.clone() }, good: num.good.clone(), aggregate: "ratio".into(), ratio: Some((num.name, den.name)), axis: false, dimension: None, values: None, indicator: None });
            continue;
        }
        let (source, field) = resolve(id)?;
        if field.axis { return Err(format!("{id} is an axis, not a measure")); }
        let entry = group(&mut groups, &source);
        if !entry.measures.iter().any(|item| item.name == field.name) { entry.measures.push(field); }
    }
    if groups.is_empty() { return Err("no measure".into()); }
    let mut dims = vec![];
    for id in &intent.dims {
        let (source, field) = resolve(id)?;
        if !field.axis { return Err(format!("{id} is not an axis")); }
        // An axis of the organisation is the same referential in every source: each one reads its own.
        if field.dimension.as_deref() == Some("org") {
            for (key, entry) in groups.iter_mut() {
                let own = if key == FINANCE { Some(field.clone()) } else { catalog.tables[key.as_str()].fields.iter().find(|item| item.axis && item.dimension.as_deref() == Some("org")).cloned() };
                let Some(own) = own else { continue };
                if key != FINANCE { entry.axes.push(own.clone()); }
                dims.push((key.clone(), own));
            }
            continue;
        }
        // An axis of a table the intent reads no measure of: the measure of that table declared as detailing one it reads (the
        // revenue of the sales details the revenue of the accounts) takes its place.
        if !groups.iter().any(|(key, _)| *key == source) {
            let details = |field: &Field, measure: &Field| !field.axis && field.indicator.as_ref().is_some_and(|indicator| *indicator == measure.name || measure.indicator.as_ref() == Some(indicator));
            let found = groups.iter().enumerate().find_map(|(at, (_, entry))| entry.measures.iter().position(|measure| !measure.label.fr.is_empty()
                && catalog.tables[source.as_str()].fields.iter().any(|field| details(field, measure))).map(|index| (at, index)));
            let Some((at, index)) = found else { gaps.push(format!("dims:{id}:sans-mesure")); continue };
            let replaced = groups[at].1.measures.remove(index);
            let equivalent = catalog.tables[source.as_str()].fields.iter().find(|field| details(field, &replaced)).unwrap().clone();
            if groups[at].1.measures.is_empty() && !dims.iter().any(|(key, _)| *key == groups[at].0) { groups.remove(at); }
            group(&mut groups, &source).measures.push(equivalent);
        }
        if source != FINANCE { group(&mut groups, &source).axes.push(field.clone()); }
        dims.push((source, field));
    }
    let mut filters = vec![];
    for (id, code) in &intent.filtres {
        let (source, field) = resolve(id)?;
        if source != FINANCE && !groups.iter().any(|(key, _)| *key == source) { gaps.push(format!("filtres:{id}:sans-mesure")); continue; }
        let entry = group(&mut groups, &source);
        if source != FINANCE && !entry.axes.iter().any(|axis| axis.name == field.name) { entry.axes.push(field.clone()); }
        filters.push((source, field, code.clone()));
    }

    // Period, granularity and comparison: the defaults of the main filter bar.
    let (year, month) = catalog.last;
    let fiscal_end = (catalog.opening + 10) % 12 + 1;
    let gran = match intent.gran.as_str() { "mois" => "month", "trimestre" => "quarter", "annee" => "year", _ => "ytd" };
    // The window of the analysis bar; the intent's granularity stays the grain of series and tables (`mois` over `12m`).
    let (period, gran): (String, String) = match intent.periode.as_str() {
        "dernier" => ("latest".into(), gran.into()),
        "ytd" => ("latest".into(), "ytd".into()),
        "dernier-T" => { let end = (year * 12 + month as i32 - 1) - (month as i32 % 3); (format!("{}-{:02}", end.div_euclid(12), end.rem_euclid(12) + 1), "quarter".into()) }
        text if text.ends_with('m') && text[..text.len() - 1].parse::<u32>().is_ok_and(|months| (1..=60).contains(&months)) => ("latest".into(), format!("r{}", &text[..text.len() - 1])),
        text => match (text.len(), text.split_once("-T")) {
            (4, _) => { let y: i32 = text.parse().map_err(|_| format!("invalid period {text:?}"))?; (format!("{y}-{fiscal_end:02}"), "year".into()) }
            (_, Some((y, q))) => { let q: u32 = q.parse().ok().filter(|q| (1..=4).contains(q)).ok_or_else(|| format!("invalid period {text:?}"))?; (format!("{y}-{:02}", q * 3), "quarter".into()) }
            _ => { month_of(text).ok_or_else(|| format!("invalid period {text:?}"))?; (text.to_owned(), gran.into()) }
        },
    };
    let grain = match intent.gran.as_str() { "mois" => "month", "trimestre" => "quarter", _ => gran.as_str() };
    let finance = groups.iter().any(|(key, _)| key == FINANCE);
    let compare = match intent.cmp.as_str() {
        "aucun" => "none",
        "budget" | "forecast" if !finance => { gaps.push(format!("cmp:{}:sans-budget", intent.cmp)); "n1" }
        other @ ("n1" | "n2" | "m1" | "budget" | "forecast") => other,
        other => { gaps.push(format!("cmp:{other}")); "n1" }
    };
    // A ranking runs the largest first; `asc` the smallest (the most unfavourable gaps for `ecart asc`).
    let order = intent.tri.as_deref().map(|tri| if tri.ends_with(" asc") { "asc" } else { "desc" });
    let by_gap = intent.tri.as_deref().is_some_and(|tri| tri.starts_with("ecart"));

    // Sources.
    let mut data = Map::new();
    let text = |fr: &str, en: &str| if lang == Lang::Fr { fr.to_owned() } else { en.to_owned() };
    let months_of = |collection: &str| json!({"type": "query", "scope": "place", "query": format!("on {collection} | group | by date | count as lines | end | sort date desc")});
    for (source, entry) in &groups {
        if source == FINANCE {
            data.insert("indicators".into(), json!({"type": "view", "view": "finance.indicators"}));
            data.insert("history".into(), json!({"type": "view", "view": "finance.indicators", "series": true}));
            data.insert("history_month".into(), json!({"type": "view", "view": "finance.indicators", "series": true, "gran": "month"}));
            data.insert("months".into(), months_of("finance").as_object().map(|query| { let mut query = query.clone(); query.insert("query".into(), json!("on finance | where scenario == \"actual\" | group | by date | count as lines | end | sort date desc")); JsonValue::Object(query) }).unwrap());
            data.insert("entities".into(), json!({"type": "query", "scope": "place", "query": "on finance | group | by entity | count as lines | end | sort entity"}));
            // Detailed by entity: one row per entity, not their consolidation.
            if dims.iter().any(|(key, _)| key == FINANCE) {
                data.insert("indicators_entity".into(), json!({"type": "view", "view": "finance.indicators", "byEntity": true}));
                data.insert("history_month_entity".into(), json!({"type": "view", "view": "finance.indicators", "series": true, "gran": "month", "byEntity": true}));
            }
            continue;
        }
        let table = &catalog.tables[source];
        let mut fields = vec![json!({"name": "date", "type": "month"})];
        let mut axes: Vec<&Field> = entry.axes.iter().collect();
        if let Some(entity) = table.fields.iter().find(|field| field.axis && field.dimension.as_deref() == Some("org")) { if !axes.iter().any(|axis| axis.name == entity.name) { axes.push(entity); } }
        // Each axis is read on the row, completed by its referentials (`values` of the axes): the label of an axis from its
        // own referential (then the field of that name on the row, then the code), an axis the row may lack from the
        // referential of another axis declaring it (the family of the article). Rows are grouped by the axes and the keys of
        // those referentials, joined to them, then grouped again by the axes when a key was only there to join.
        let referential = |axis: &Field| axis.values.as_ref().filter(|(source, _, _)| *source != table.collection).and_then(|(source, code, label)| Some((catalog.tables.get(source)?, code.clone(), label.clone())));
        let provider = |axis: &Field| table.fields.iter().filter(|key| key.axis && key.name != axis.name).find_map(|key| referential(key).filter(|(other, _, _)| other.columns.contains(&axis.name)).map(|(other, code, _)| (key.name.clone(), other.collection.clone(), code)));
        let mut joins: Vec<(String, String, String)> = vec![];
        let mut join = |key: &str, collection: &str, code: &str| -> String {
            let at = joins.iter().position(|(k, c, _)| k == key && c == collection).unwrap_or_else(|| { joins.push((key.to_owned(), collection.to_owned(), code.to_owned())); joins.len() - 1 });
            format!("_r{at}")
        };
        let (mut derives, mut keys, mut labels): (Vec<String>, Vec<String>, Vec<String>) = (vec![], vec![], vec![]);
        let mut sums: Vec<String> = vec![];
        let mut by = vec!["date".to_owned()];
        for axis in &axes {
            by.push(format!("{0}? as {0}", axis.name));
            let mut field = json!({"name": axis.name, "type": "text", "role": "axis", "label": {"fr": axis.label.fr, "en": axis.label.en}});
            if let Some(dimension) = &axis.dimension { field["dimension"] = json!(dimension); }
            fields.push(field);
            if let Some((key, collection, code)) = provider(axis) {
                if !axes.iter().any(|other| other.name == key) && !keys.contains(&key) { keys.push(key.clone()); }
                let alias = join(&key, &collection, &code);
                derives.push(format!("{0} = coalesce({0}, get(coalesce(at({alias}, 0), {{}}), \"{0}\"))", axis.name));
            }
            if axis.values.is_some() {
                let name = format!("{}_label", axis.name);
                let (_, _, label) = axis.values.clone().unwrap();
                let own = table.columns.contains(&label).then(|| { sums.push(format!("first {label}? as {name}")); name.clone() });
                let joined = referential(axis).map(|(other, code, label)| format!("get(coalesce(at({}, 0), {{}}), \"{label}\")", join(&axis.name, &other.collection, &code)));
                derives.push(format!("{name} = coalesce({})", joined.into_iter().chain(own).chain([axis.name.clone()]).collect::<Vec<_>>().join(", ")));
                labels.push(name.clone());
                fields.push(json!({"name": name, "type": "text", "label": {"fr": axis.label.fr, "en": axis.label.en}}));
            }
        }
        by.extend(keys.iter().map(|key| format!("{key}? as {key}")));
        let measure = |field: &Field, fields: &mut Vec<JsonValue>, sums: &mut Vec<String>| {
            let mut out = json!({"name": field.name, "role": "measure", "label": {"fr": field.label.fr, "en": field.label.en}, "unit": field.unit, "good": field.good});
            if field.aggregate == "avg" {
                sums.push(format!("sum {0} as {0}_sum", field.name));
                fields.push(json!({"name": format!("{}_sum", field.name), "type": "number", "aggregate": "sum"}));
                out["type"] = json!("ratio"); out["num"] = json!(format!("{}_sum", field.name)); out["den"] = json!("lines");
            } else { sums.push(format!("sum {0}? as {0}", field.name)); out["type"] = json!("number"); out["aggregate"] = json!(field.aggregate); }
            fields.push(out);
        };
        for field in &entry.measures {
            if let Some((num, den)) = &field.ratio {
                for part in [num, den] {
                    if !fields.iter().any(|item| item["name"] == json!(part)) && !entry.measures.iter().any(|item| &item.name == part) {
                        let part = table.field(part).ok_or_else(|| format!("ratio {} reads unknown field {part:?}", field.name))?;
                        measure(&Field { label: Label { fr: String::new(), en: String::new() }, ..part.clone() }, &mut fields, &mut sums);
                    }
                }
                continue;
            }
            measure(field, &mut fields, &mut sums);
        }
        for field in entry.measures.iter().filter(|field| field.ratio.is_some()) {
            let (num, den) = field.ratio.clone().unwrap();
            fields.push(json!({"name": field.name, "type": "ratio", "num": num, "den": den, "role": "measure", "label": {"fr": field.label.fr, "en": field.label.en}, "unit": field.unit, "good": field.good}));
        }
        fields.push(json!({"name": "lines", "type": "number", "aggregate": "sum"}));
        let mut query = format!("on {} | group | by {} | {} | count as lines | end", table.collection, by.join(", "), sums.join(" | "));
        for (at, (key, collection, code)) in joins.iter().enumerate() { query += &format!(" | lookup {collection} as _r{at} on _r{at}.{code} == {key} into _r{at}"); }
        if !derives.is_empty() { query += &format!(" | derive {}", derives.join(", ")); }
        if !keys.is_empty() {
            let outs: Vec<&str> = sums.iter().filter(|sum| sum.starts_with("sum ")).filter_map(|sum| sum.rsplit(" as ").next()).chain(["lines"]).collect();
            let regroup: Vec<String> = ["date".to_owned()].into_iter().chain(axes.iter().map(|axis| axis.name.clone())).chain(labels.iter().cloned()).collect();
            query += &format!(" | group | by {} | {} | end", regroup.join(", "), outs.iter().map(|out| format!("sum {out}")).collect::<Vec<_>>().join(" | "));
        }
        data.insert(source.clone(), json!({"type": "query", "scope": "place", "query": query, "fields": fields, "analytics": {"time": "date", "opening": catalog.opening}}));
        if !finance && !data.contains_key("months") { data.insert("months".into(), months_of(&table.collection)); }
    }

    // Filter bar: the period, the organisation, the values the request names, granularity and comparison.
    let first = groups[0].0.clone();
    let mut bar = vec![json!({"name": "period", "label": text("Données au", "Data at"), "kind": "period", "field": "date", "source": "months", "table": "months", "default": period, "multiple": false})];
    let org_code = filters.iter().find(|(_, field, _)| field.dimension.as_deref() == Some("org")).map(|(_, _, code)| code.clone()).unwrap_or_default();
    if finance { bar.push(json!({"name": "entity", "label": text("Département", "Department"), "kind": "choice", "field": "entity", "source": "entities", "table": "entities", "default": org_code, "multiple": false})); }
    else if catalog.tables[&first].fields.iter().any(|field| field.axis && field.name == "entity") { bar.push(json!({"name": "entity", "label": text("Département", "Department"), "kind": "choice", "field": "entity", "source": first, "table": first, "default": org_code, "multiple": false})); }
    for (source, field, code) in &filters {
        if field.dimension.as_deref() == Some("org") { continue; }
        bar.push(json!({"name": format!("{source}.{}", field.name), "label": field.label.get(lang), "kind": "choice", "field": field.name, "source": source, "table": source, "default": code, "multiple": false}));
    }
    let mut grans = vec!["month", "quarter", "ytd", "year"];
    if gran.starts_with('r') { grans.push(gran.as_str()); }
    bar.push(json!({"name": "gran", "label": text("Période", "Period"), "kind": "granularity", "default": gran, "options": grans}));
    let mut options: Vec<&str> = if finance { vec!["n1", "n2", "budget", "forecast", "landing", "none"] } else { vec!["n1", "n2", "none"] };
    if compare == "m1" { options.insert(2, "m1"); }
    bar.push(json!({"name": "compare", "label": text("Comparaison", "Comparison"), "kind": "compare", "default": compare, "options": options}));
    let mut views = vec![json!({"type": "filterbar", "w": 12, "access": "read", "formatControl": true, "filters": bar})];

    // Blocks: each keyword of the intent, on each source.
    let blocs: Vec<String> = if intent.blocs.is_empty() {
        match intent.but.as_str() { "tendance" => vec!["courbe"], "comparer" => vec!["kpi", "ecarts"], "decomposer" => vec!["repartition"], "classer" => vec!["classement"], "detail" => vec!["tableau"], "expliquer" => vec!["cascade", "ecarts"], _ => vec!["kpi", "courbe"] }.into_iter().map(str::to_owned).collect()
    } else { intent.blocs.clone() };
    let cmp = compare != "none";
    let top = intent.top;
    let by_label = |field: &Field| format!("{} {}", text("par", "by"), field.label.get(lang).to_lowercase());
    let title_of = |fields: &[&Field]| fields.iter().map(|field| field.label.get(lang)).collect::<Vec<_>>().join(", ");
    // The values a block shows for an axis: its labels, when the rows hold them.
    let category = |axis: &Field| if axis.values.is_some() { format!("{}_label", axis.name) } else { axis.name.clone() };
    for bloc in &blocs {
        if let Some(name) = bloc.strip_prefix("modele:") {
            let Some((block, sources)) = catalog.templates.get(name) else { gaps.push(format!("modele:{name}")); continue };
            for (key, value) in sources { data.entry(key.clone()).or_insert_with(|| value.clone()); }
            if let Some(source) = block.str_field("source") { if data.get(source) != sources.get(source) { gaps.push(format!("modele:{name}:source")); } }
            views.push(block.clone());
            continue;
        }
        for (source, entry) in &groups {
            let measures: Vec<&Field> = entry.measures.iter().filter(|field| !field.label.fr.is_empty()).collect();
            if measures.is_empty() { continue; }
            let names: Vec<&str> = measures.iter().map(|field| field.name.as_str()).collect();
            let finance = source == FINANCE;
            let axis = dims.iter().find(|(key, _)| key == source).map(|(_, field)| field);
            let axis2 = dims.iter().filter(|(key, _)| key == source).nth(1).map(|(_, field)| field);
            let (series, monthly, flat) = match (finance, axis.is_some()) { (true, false) => ("history", "history_month", "indicators"), (true, true) => ("history", "history_month_entity", "indicators_entity"), _ => (source.as_str(), source.as_str(), source.as_str()) };
            let with_order = |mut opts: JsonValue| { if let Some(order) = order { opts["order"] = json!(order); } opts };
            let with_top = |mut opts: JsonValue, default: u32| { opts["top"] = json!(top.unwrap_or(default)); opts };
            let titled = |fields: &[&Field]| match axis { Some(axis) => format!("{} {}", title_of(fields), by_label(axis)), None => title_of(fields) };
            let mut add = |block: JsonValue| views.push(block);
            match bloc.as_str() {
                "kpi" if measures.len() > 4 => add(json!({"type": "sparks", "w": 12, "source": series, "fields": names})),
                "kpi" | "objectif" if axis.is_none() || bloc == "kpi" => {
                    let w = match measures.len() { 1 => 12, 2 => 6, 3 => 4, _ => 3 };
                    for field in &measures { add(json!({"type": "kpi", "w": w, "source": series, "fields": [field.name], "title": field.label.get(lang), "opts": {"trend": true}})); }
                }
                "objectif" => add(json!({"type": "bullet", "w": 6, "source": flat, "fields": [names[0]], "category": axis.unwrap().name, "title": titled(&measures[..1]), "opts": with_top(json!({}), 6)})),
                "courbe" => match axis {
                    Some(axis) => add(json!({"type": "line", "w": 6, "h": "M", "source": monthly, "fields": [names[0]], "category": category(axis), "title": titled(&measures[..1]), "opts": with_top(json!({"area": false, "cmp": false}), 5)})),
                    None => add(json!({"type": "line", "w": 6, "h": "M", "source": monthly, "fields": names.iter().take(3).collect::<Vec<_>>(), "title": title_of(&measures[..measures.len().min(3)]), "opts": {"area": measures.len() == 1, "cmp": cmp}})),
                },
                "barres" => match axis {
                    Some(axis) => add(json!({"type": "bars", "w": 6, "h": "M", "source": flat, "fields": [names[0]], "category": category(axis), "title": titled(&measures[..1]), "opts": with_top(json!({"cmp": cmp}), 12)})),
                    None => add(json!({"type": "bars", "w": 6, "h": "M", "source": monthly, "fields": [names[0]], "title": title_of(&measures[..1]), "opts": {"cmp": cmp, "quarters": grain == "quarter"}})),
                },
                "classement" => match axis {
                    Some(axis) => for field in measures.iter().take(2) {
                        if by_gap { add(json!({"type": "variance", "w": 6, "source": flat, "fields": [field.name], "category": category(axis), "title": titled(&[field]), "opts": with_order(with_top(json!({}), 10))})); }
                        else { add(json!({"type": "bars", "w": 6, "h": "M", "source": flat, "fields": [field.name], "category": category(axis), "title": titled(&[field]), "opts": with_order(with_top(json!({"cmp": cmp}), 10))})); }
                    },
                    None => gaps.push(format!("classement:{source}:sans-axe")),
                },
                "repartition" => match axis {
                    Some(axis) => add(json!({"type": "donut", "w": 6, "source": flat, "fields": [names[0]], "category": category(axis), "title": titled(&measures[..1]), "opts": with_top(json!({}), 8)})),
                    None => gaps.push(format!("repartition:{source}:sans-axe")),
                },
                "tableau" => match axis {
                    Some(axis) => for field in measures.iter().take(3) {
                        let mut block = json!({"type": "matrix", "w": 12, "source": flat, "fields": [field.name], "category": category(axis), "title": titled(&[field]), "opts": {"rows": "category", "cols": if axis2.is_some() { "category" } else { "scenarios" }}});
                        if let Some(axis2) = axis2 { block["category2"] = json!(category(axis2)); }
                        add(block);
                    },
                    None => add(json!({"type": "matrix", "w": 12, "source": if grain == "month" { monthly } else { flat }, "fields": names, "title": title_of(&measures), "opts": {"rows": "measures", "cols": match grain { "month" => "months", "quarter" => "quarters", _ => "scenarios" }}})),
                },
                "ecarts" => {
                    if !cmp { gaps.push(format!("ecarts:{source}:sans-comparaison")); }
                    match axis {
                        Some(axis) => add(json!({"type": "variance", "w": 6, "source": flat, "fields": [names[0]], "category": category(axis), "title": titled(&measures[..1]), "opts": if by_gap { with_order(with_top(json!({}), 12)) } else { with_top(json!({}), 12) }})),
                        None => add(json!({"type": "variance", "w": 6, "source": flat, "fields": names, "title": title_of(&measures), "opts": with_top(json!({}), 12)})),
                    }
                }
                "cascade" => {
                    let mut block = json!({"type": "waterfall", "w": 6, "h": "M", "source": flat, "fields": names, "title": titled(&measures), "opts": with_top(json!({}), 8)});
                    if let Some(axis) = axis { block["category"] = json!(category(axis)); block["fields"] = json!([names[0]]); }
                    add(block);
                }
                "commentaire" => add(json!({"type": "narrative", "w": 12, "source": flat, "fields": names, "title": text("Commentaire", "Commentary")})),
                other => gaps.push(format!("bloc:{other}")),
            }
        }
    }
    // A chart alone on its row takes the whole width.
    let mut index = 1;
    while index < views.len() {
        let half = |node: &JsonValue| node["w"] == json!(6);
        let start = index;
        while index < views.len() && half(&views[index]) { index += 1; }
        if (index - start) % 2 == 1 { views[index - 1]["w"] = json!(12); }
        if index == start { index += 1; }
    }

    let measures_title = groups.iter().flat_map(|(_, entry)| entry.measures.iter()).filter(|field| !field.label.fr.is_empty()).take(3).map(|field| field.label.get(lang).to_owned()).collect::<Vec<_>>().join(", ");
    let title = match dims.first() { Some((_, axis)) => format!("{measures_title} {}", by_label(axis)), None => measures_title };
    let mut tile: Vec<JsonValue> = views.iter().skip(1).take(2).cloned().collect();
    for node in &mut tile { if node["w"] == json!(12) { node["w"] = json!(6); } }
    let source_text = format!("{intent:?}");
    let definition = json!({
        "id": format!("generated.{:08x}", fnv1a64(source_text.as_bytes()) as u32), "name": title, "version": "0.1.0", "tone": "glacier",
        "description": intent.note.clone().unwrap_or_else(|| title.clone()),
        "settings": {"analysis": {"gran": gran, "compare": compare}},
        "data": data,
        "views": {"tile": tile, "full": views},
        "presentation": {"mark": "✦", "tile": {"eyebrow": text("Généré", "Generated")}, "report": {"format": "app", "title": title, "subtitle": intent.note.clone().unwrap_or_default()}},
        "generated": {"gaps": gaps},
    });
    Ok(Compiled { definition, gaps })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> Catalog {
        let schema: JsonValue = serde_json::from_str(include_str!("../../llm-bench/results/place-schema-finance.json")).unwrap();
        let definitions: Vec<JsonValue> = include_str!("../../llm-bench/results/finance-indicator-definitions.jsonl").lines().filter_map(|line| serde_json::from_str(line).ok()).collect();
        Catalog::new(&schema, &definitions, "2026-09").unwrap()
    }
    /// One row in each collection of the Place, every declared field filled, to run the generated queries on.
    fn sample_place() -> crate::engine::Engine {
        let engine = crate::engine::Engine::new(std::sync::Arc::new(crate::storage::MemoryStorage::new()));
        let schema: JsonValue = serde_json::from_str(include_str!("../../llm-bench/results/place-schema-finance.json")).unwrap();
        for collection in schema["collections"].as_array().unwrap() {
            let mut row = Map::new();
            for field in collection["fields"].as_array().unwrap() { if field["type"] != "ratio" { row.insert(field["name"].as_str().unwrap().into(), if matches!(field["type"].as_str(), Some("number")) { json!(1) } else if field["name"] == "date" { json!("2026-09") } else { json!("A") }); } }
            if collection["name"] == "finance" { row.insert("scenario".into(), json!("actual")); row.insert("entity".into(), json!("E1")); }
            crate::view::execute(&engine, None, &format!("on {} | insert {}", collection["name"].as_str().unwrap(), JsonValue::Object(row)), false).unwrap();
        }
        engine
    }
    fn app(text: &str) -> Compiled { compile(&Intent::parse(text, None), &catalog(), Lang::Fr).unwrap() }
    fn blocks(compiled: &Compiled) -> Vec<String> { compiled.definition["views"]["full"].as_array().unwrap().iter().skip(1).map(|node| format!("{}:{}", node["type"].as_str().unwrap(), node["source"].as_str().unwrap_or(""))).collect() }

    /// The intents of the reference (`llm-bench/test2.json`, first accepted answer of each field) as the model writes them.
    fn reference() -> Vec<(String, String)> {
        let gold: Vec<JsonValue> = serde_json::from_str(include_str!("../../llm-bench/test2.json")).unwrap();
        let first = |item: &JsonValue, key: &str| item.get(key).and_then(|value| value.as_array()).and_then(|values| values.first()).cloned();
        let list = |value: Option<JsonValue>| value.and_then(|value| value.as_array().map(|items| items.iter().filter_map(|item| item.as_str().map(str::to_owned)).collect::<Vec<_>>())).unwrap_or_default();
        gold.iter().filter(|item| item.get("diff").is_none()).map(|item| {
            let mut lines = vec![format!("but: {}", first(item, "but").unwrap().as_str().unwrap())];
            let measures = list(first(item, "mes")).into_iter().chain(list(item.get("soft").and_then(|soft| soft.get("ref")).cloned()).into_iter().take(if item.get("mes").is_some() { 0 } else { 3 })).collect::<Vec<_>>();
            if item["but"][0] != "hors" {
                lines.push(format!("mesures: {}", measures.join(", ")));
                let dims = list(first(item, "dims"));
                if !dims.is_empty() { lines.push(format!("dims: {}", dims.join(", "))); }
                let filters = list(item.get("filtres").cloned());
                if !filters.is_empty() { lines.push(format!("filtres: {}", filters.join(", "))); }
                for key in ["periode", "gran", "cmp"] { if let Some(value) = first(item, key) { lines.push(format!("{key}: {}", value.as_str().unwrap())); } }
                lines.push(format!("blocs: {}", list(item.get("blocs").cloned()).join(", ")));
                if let Some(top) = item.get("top").and_then(|top| top.as_str()) { lines.push(format!("top: {top}")); }
                if let Some(tri) = first(item, "tri") { lines.push(format!("tri: {}", tri.as_str().unwrap())); }
            }
            (item["id"].as_str().unwrap().to_owned(), lines.join("\n"))
        }).collect()
    }

    #[test] fn intents_parse_lists_ratios_filters_and_apply_diffs() { let intent = Intent::parse("but: comparer\nmesures: ventes.revenue, ratio(ventes.margin,  ventes.revenue)\ndims: ventes.client\nfiltres: ventes.family=F08\nperiode: 2025\ncmp: aucun\nblocs: kpi, tableau\ntop: 10\ntri: ventes.revenue desc", None); assert_eq!(intent.mesures, ["ventes.revenue", "ratio(ventes.margin,ventes.revenue)"]); assert_eq!(intent.filtres, [("ventes.family".to_owned(), "F08".to_owned())]); assert_eq!((intent.top, intent.tri.as_deref()), (Some(10), Some("ventes.revenue desc"))); let next = Intent::parse("+mesures: ventes.margin\n-blocs: kpi\n+gran: mois\n=cmp: n1", Some(&intent)); assert_eq!(next.mesures.len(), 3); assert_eq!(next.blocs, ["tableau"]); assert_eq!((next.gran.as_str(), next.cmp.as_str(), next.dims.len()), ("mois", "n1", 1)); }
    #[test] fn periods_normalize_as_the_bench_scores_them() { let mut intent = Intent { periode: "M03".into(), ..Intent::default() }; intent.normalize((2026, 9)); assert_eq!(intent.periode, "2026-03"); intent.periode = "M11".into(); intent.normalize((2026, 9)); assert_eq!(intent.periode, "2025-11"); intent.periode = "2026-09".into(); intent.normalize((2026, 9)); assert_eq!(intent.periode, "dernier"); intent.periode = "2026".into(); intent.gran = "annee".into(); intent.normalize((2026, 9)); assert_eq!((intent.periode.as_str(), intent.gran.as_str()), ("ytd", "ytd")); }
    #[test] fn finance_intents_read_the_indicator_views_and_the_filter_bar_carries_the_analysis() { let compiled = app("but: comparer\nmesures: ca, marge\nperiode: 2025-T2\ncmp: budget\nblocs: kpi, courbe, ecarts"); assert_eq!(blocks(&compiled), ["kpi:history", "kpi:history", "line:history_month", "variance:indicators"]); let bar = &compiled.definition["views"]["full"][0]["filters"]; assert_eq!((bar[0]["default"].as_str(), bar[2]["default"].as_str(), bar[3]["default"].as_str()), (Some("2025-06"), Some("quarter"), Some("budget"))); assert!(compiled.gaps.is_empty()); }
    #[test] fn table_intents_group_their_rows_by_month_and_the_axes_named() { let compiled = app("but: classer\nmesures: ventes.revenue, ventes.tm\ndims: ventes.client\nfiltres: ventes.family=F08\nblocs: classement\ntop: 5"); let query = compiled.definition["data"]["ventes"]["query"].as_str().unwrap(); assert_eq!(query, "on ventes | group | by date, client? as client, family? as family, entity? as entity, article? as article | sum revenue? as revenue | sum margin? as margin | count as lines | end | lookup ventes_clients as _r0 on _r0.client == client into _r0 | lookup ventes_articles as _r1 on _r1.article == article into _r1 | derive client_label = coalesce(get(coalesce(at(_r0, 0), {}), \"name\"), client), family = coalesce(family, get(coalesce(at(_r1, 0), {}), \"family\")) | group | by date, client, family, entity, client_label | sum revenue | sum margin | sum lines | end"); assert_eq!(blocks(&compiled), ["bars:ventes", "bars:ventes"]); assert_eq!(compiled.definition["views"]["full"][1]["category"], "client_label"); assert_eq!(compiled.definition["views"]["full"][1]["opts"]["top"], 5); let fields = compiled.definition["data"]["ventes"]["fields"].as_array().unwrap(); assert!(fields.iter().any(|field| field["name"] == "tm" && field["num"] == "margin")); assert!(compiled.definition["views"]["full"][0]["filters"].as_array().unwrap().iter().any(|filter| filter["field"] == "family" && filter["default"] == "F08")); }
    #[test] fn an_axis_of_a_table_without_measure_takes_the_measure_of_that_table_declared_as_detailing_the_one_read() { let compiled = app("but: comparer\nmesures: ca\ndims: ventes.family\nblocs: courbe"); assert_eq!(blocks(&compiled), ["line:ventes"]); assert!(compiled.gaps.is_empty(), "{:?}", compiled.gaps); assert!(compiled.definition["data"]["ventes"]["query"].as_str().unwrap().contains("sum revenue? as revenue")); assert_eq!(app("but: comparer\nmesures: rt\ndims: ventes.family").gaps, ["dims:ventes.family:sans-mesure"]); }
    #[test] fn axes_read_their_labels_and_missing_attributes_in_their_referentials() {
        // The label of the client: its referential, then the name on the row, then the code.
        let compiled = app("but: classer\nmesures: creances.value\ndims: creances.client\nblocs: classement");
        assert_eq!(compiled.definition["data"]["creances"]["query"], "on creances | group | by date, client? as client, entity? as entity | first name? as client_label | sum value? as value | count as lines | end | lookup ventes_clients as _r0 on _r0.client == client into _r0 | derive client_label = coalesce(get(coalesce(at(_r0, 0), {}), \"name\"), client_label, client)");
        assert_eq!(compiled.definition["views"]["full"][1]["category"], "client_label");
        // The family of the sales: on the row, else in the referential of the article, joined by article then grouped again.
        let compiled = app("but: classer\nmesures: ventes.revenue\ndims: ventes.family\nblocs: classement");
        assert_eq!(compiled.definition["data"]["ventes"]["query"], "on ventes | group | by date, family? as family, entity? as entity, article? as article | sum revenue? as revenue | count as lines | end | lookup ventes_articles as _r0 on _r0.article == article into _r0 | derive family = coalesce(family, get(coalesce(at(_r0, 0), {}), \"family\")) | group | by date, family, entity | sum revenue | sum lines | end");
        assert_eq!(compiled.definition["views"]["full"][1]["category"], "family");
    }
    #[test] fn averaged_measures_are_read_as_a_sum_over_the_rows() { let compiled = app("but: classer\nmesures: creances.age_days, creances.value\ndims: creances.client\nblocs: classement"); let fields = compiled.definition["data"]["creances"]["fields"].as_array().unwrap(); assert!(fields.iter().any(|field| field["name"] == "age_days" && field["type"] == "ratio" && field["den"] == "lines")); assert!(compiled.definition["data"]["creances"]["query"].as_str().unwrap().contains("sum age_days as age_days_sum")); }
    #[test] fn rolling_periods_entity_detail_ascending_rankings_and_prior_month_reach_the_runtime() { let compiled = app("but: classer\nmesures: ca\ndims: entity\nperiode: 12m\ngran: mois\ncmp: m1\nblocs: classement, courbe\ntri: ca asc"); assert!(compiled.gaps.is_empty(), "{:?}", compiled.gaps); let bar = &compiled.definition["views"]["full"][0]["filters"]; assert_eq!((bar[2]["default"].as_str(), bar[3]["default"].as_str()), (Some("r12"), Some("m1"))); assert!(bar[2]["options"].as_array().unwrap().contains(&json!("r12"))); assert_eq!(blocks(&compiled), ["bars:indicators_entity", "line:history_month_entity"]); let full = &compiled.definition["views"]["full"]; assert_eq!((full[1]["category"].as_str(), full[1]["opts"]["order"].as_str(), full[2]["category"].as_str()), (Some("entity"), Some("asc"), Some("entity"))); assert_eq!(compiled.definition["data"]["indicators_entity"]["byEntity"], true); let gaps = app("but: comparer\nmesures: ca, ratio(ca, rt)\nblocs: kpi").gaps; assert_eq!(gaps, ["ratio:ratio(ca,rt)"]); assert_eq!(compile(&Intent::parse("but: hors", None), &catalog(), Lang::Fr).unwrap_err(), "hors"); assert!(compile(&Intent::parse("but: synthese\nmesures: ventes.nothing", None), &catalog(), Lang::Fr).unwrap_err().contains("unknown identifier")); }
    #[test] fn template_blocks_copy_the_installed_block_with_its_sources() { let compiled = app("but: synthese\nmesures: clientsbrut\nblocs: kpi, modele:creances.clientaging"); assert_eq!(blocks(&compiled), ["kpi:history", "clientaging:indicators"]); assert!(compiled.gaps.is_empty()); }
    /// Writes the template blocks of the installed Apps into the place.schema fixture of the bench, as og-core lists them
    /// (`OG_SCHEMA_FIXTURE=llm-bench/results/place-schema-finance.json`).
    #[test] #[ignore] fn refresh_schema_fixture_templates() { let Ok(path) = std::env::var("OG_SCHEMA_FIXTURE") else { return }; let mut schema: JsonValue = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap(); let apps: Vec<JsonValue> = serde_json::from_str(include_str!("../../apps.json")).unwrap(); let installed: Vec<(JsonValue, JsonValue)> = schema["instances"].as_array().unwrap().iter().filter_map(|instance| Some((instance.clone(), apps.iter().find(|app| app["appId"] == instance["appId"])?.clone()))).collect(); schema["templates"] = crate::daemon::place_schema(&installed)["templates"].clone(); std::fs::write(path, serde_json::to_string_pretty(&schema).unwrap() + "\n").unwrap(); }
    /// The previous intent of a follow-up of the bench that names none (`run.py`, `gold19`).
    const GOLD19: &str = "but: tendance\nmesures: ca, marge\nperiode: 12m\ngran: mois\nblocs: kpi, courbe";
    /// The answers of a model on the bench (`OG_COMPILE_OUTPUTS=out/t2b-<model>-commun-g1.json`), compiled and dumped like
    /// the reference: what the compiler refuses in what the models really write.
    #[test] #[ignore] fn bench_outputs_compile() { let Ok(path) = std::env::var("OG_COMPILE_OUTPUTS") else { return }; let catalog = catalog(); let bench: JsonValue = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap(); let gold: Vec<JsonValue> = serde_json::from_str(include_str!("../../llm-bench/test2.json")).unwrap(); let previous = |id: &str| { let item = gold.iter().find(|item| item["id"] == id)?; item["prev_a"].as_str().map(|text| Intent::parse(text, None)).or_else(|| item.get("prev").map(|_| Intent::parse(GOLD19, None))) }; let (mut ok, mut refused) = (0, vec![]); for result in bench["results"].as_array().unwrap() { let text = result["out"].as_str().unwrap(); match compile(&Intent::parse(text, previous(result["id"].as_str().unwrap()).as_ref()), &catalog, Lang::Fr) { Ok(compiled) => { ok += 1; if let Ok(dump) = std::env::var("OG_COMPILE_DUMP") { let mut file = std::fs::OpenOptions::new().create(true).append(true).open(dump).unwrap(); use std::io::Write; writeln!(file, "{}", json!({"id": result["id"], "intent": text, "app": compiled.definition})).unwrap(); } } Err(error) if error == "hors" => {} Err(error) => refused.push(format!("{} {error}", result["id"].as_str().unwrap())) } } eprintln!("compiled {ok}, refused {refused:?}"); }
    #[test] fn every_reference_intent_compiles_to_a_valid_app_whose_queries_run() { let catalog = catalog(); let engine = sample_place(); let mut gaps = BTreeMap::new(); for (id, text) in reference() { match compile(&Intent::parse(&text, None), &catalog, Lang::Fr) { Ok(compiled) => { crate::daemon::validate_app_definition_model(&compiled.definition).unwrap_or_else(|error| panic!("{id}: {error}")); for (name, source) in compiled.definition["data"].as_object().unwrap() { if let Some(query) = source["query"].as_str() { let rows = crate::view::execute(&engine, None, query, true).unwrap_or_else(|error| panic!("{id} {name}: {error}\n{query}")); assert!(!rows.is_empty(), "{id} {name}: no row\n{query}"); } } for gap in compiled.gaps { *gaps.entry(gap.split(':').next().unwrap().to_owned()).or_insert(0) += 1; } if let Ok(path) = std::env::var("OG_COMPILE_DUMP") { let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap(); use std::io::Write; writeln!(file, "{}", json!({"id": id, "intent": text, "app": compiled.definition})).unwrap(); } } Err(error) => assert_eq!(error, "hors", "{id}: {error}\n{text}") } } eprintln!("gaps by kind: {gaps:?}"); }
}
