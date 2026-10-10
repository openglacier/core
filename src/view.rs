//! Views: named read queries kept per Place in `_views`, written by the
//! language itself.
//!
//! ```text
//! define view finance.cube | description "…" | exec | on ledger | where month <= $to | end | end
//! define view finance.cube | version 2 | end        -- restores version 2 as a new version
//! define view finance.all | materialized | exec | on ledger | group | by month | sum amount | end | end | end
//! undefine view finance.cube
//! ```
//!
//! Each `define` writes a new version and supersedes the active one; the
//! body is kept as written, its free `$name`s are the view's parameters.
//! A body is checked when defined: planned with its parameters read as `1`,
//! and refused when it writes. A `materialized` view (without parameters)
//! keeps its rows per scope in a cache collection, computed again when a
//! collection it reads changed.
#![cfg_attr(rustfmt, rustfmt_skip)]

use serde_json::{json, Value as JsonValue};

use crate::{engine::Engine, helpers::{document_to_json, fnv1a64, insert_query, now_millis, value_to_json, PLACE_SCOPE_FIELD}, query::{group_length, parse, prepare_view, stage_ranges, value_from_source, DocumentScope, PhysicalOperator, PlannerPipeline, Sources}, storage::{CollectionId, DocumentId}};

/// The collection holding every version of every view.
pub const VIEWS_COLLECTION: &str = "_views";

/// The prefix of the constant collections holding the rows of `values […]`.
pub const VALUES_PREFIX: &str = "_values.";

/// The collection telling which cache collection holds the rows of a
/// materialized view in a scope, and the reads they were computed from.
pub const CACHE_COLLECTION: &str = "_cache";

/// The prefix of the cache collections of materialized views.
pub const CACHE_PREFIX: &str = "_cache.";

/// A `define view` or `undefine view` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Statement {
    Define { name: String, description: Option<String>, version: Option<i64>, body: Option<String>, materialized: bool },
    Undefine { name: String },
}

impl Statement {
    /// True when `query` starts with `define view` or `undefine view`.
    #[must_use]
    pub fn is_statement(query: &str) -> bool {
        head(query).is_some()
    }

    /// The statement `query` holds, `None` when it is not a view statement.
    #[must_use]
    pub fn parse(query: &str) -> Option<Result<Self, String>> {
        let (define, _) = head(query)?;
        Some(parse_statement(query.trim(), define))
    }

    /// Runs the statement for `place` (`None`: a standard view) and returns
    /// the written version; `dry` checks it without writing (`explain`).
    pub fn run(&self, engine: &Engine, place: Option<&str>, author: &str, dry: bool) -> Result<JsonValue, String> {
        let (Self::Define { name, .. } | Self::Undefine { name }) = self;
        let scope = format!("name == {} and {PLACE_SCOPE_FIELD}? == {}", json!(name), json!(place));
        let versions = rows(engine, &format!("on {VIEWS_COLLECTION} | where {scope} | sort version desc"))?;
        let active = versions.iter().find(|row| row["state"] == "active");
        let now = now_millis();
        match self {
            Self::Undefine { .. } => {
                let active = active.ok_or_else(|| format!("view {name} is not defined"))?;
                if !dry {
                    rows(engine, &format!("on {VIEWS_COLLECTION} | where {scope} and state == \"active\" | set state = \"deleted\", deletedBy = {}, deletedAt = {now}", json!(author)))?;
                }
                Ok(json!({"name": name, "version": active["version"], "state": "deleted"}))
            }
            Self::Define { description, version, body, materialized, .. } => {
                let source = match (body, version) {
                    (Some(_), _) => active,
                    (None, Some(version)) => Some(versions.iter().find(|row| row["version"] == *version).ok_or_else(|| format!("view {name} has no version {version}"))?),
                    (None, None) => Some(active.ok_or_else(|| format!("view {name} is not defined"))?),
                };
                let (query, parameters, reads) = match body {
                    Some(body) => check(engine, place, name, body).map(|(parameters, reads)| (json!(body), json!(parameters), json!(reads)))?,
                    None => source.map(|row| (row["query"].clone(), row["parameters"].clone(), row["reads"].clone())).expect("a version to copy"),
                };
                let description = description.as_deref().map_or_else(|| source.map_or(JsonValue::Null, |row| row["description"].clone()), |text| json!(text));
                let materialized = *materialized || (body.is_none() && source.is_some_and(|row| row["materialized"] == true));
                if materialized && parameters.as_array().is_some_and(|parameters| !parameters.is_empty()) {
                    return Err(format!("view {name}: a materialized view takes no parameter, its body binds them with let"));
                }
                let next = versions.first().and_then(|row| row["version"].as_i64()).unwrap_or(0) + 1;
                let mut view = json!({"name": name, "version": next, "state": "active", "description": description, "query": query, "parameters": parameters, "reads": reads, "materialized": materialized, "createdBy": author, "createdAt": now});
                if !dry {
                    if let Some(place) = place {
                        view[PLACE_SCOPE_FIELD] = json!(place);
                    }
                    rows(engine, &insert_query(VIEWS_COLLECTION, &view))?;
                    rows(engine, &format!("on {VIEWS_COLLECTION} | where {scope} and state == \"active\" and version < {next} | set state = \"superseded\""))?;
                    view.as_object_mut().expect("an object").remove(PLACE_SCOPE_FIELD);
                }
                Ok(view)
            }
        }
    }
}

/// Delivers the standard views of App `app` (its `definition.queryViews`,
/// `[{name, description?, query, materialized?}]`, in order: a view may read the views
/// before it): a view whose query or description changed gets a new
/// version, a view the App no longer delivers is undefined.
pub fn deliver(engine: &Engine, app: &str, views: &[JsonValue]) -> Result<(), String> {
    let author = format!("app:{app}");
    let standard = rows(engine, &format!("on {VIEWS_COLLECTION} | where {PLACE_SCOPE_FIELD}? == null and state == \"active\""))?;
    for view in views {
        let field = |key: &str| view.get(key).and_then(JsonValue::as_str);
        let (Some(name), Some(query)) = (field("name"), field("query")) else { return Err(format!("App {app}: a view needs a name and a query")) };
        let description = field("description").map(str::to_owned);
        let materialized = view.get("materialized") == Some(&JsonValue::Bool(true));
        if !standard.iter().any(|row| row["name"] == name && row["query"] == query && row["description"].as_str() == description.as_deref() && (row["materialized"] == true) == materialized) {
            Statement::Define { name: name.to_owned(), description, version: None, body: Some(query.to_owned()), materialized }.run(engine, None, &author, false).map_err(|message| format!("App {app}, view {name}: {message}"))?;
        }
    }
    for row in standard.iter().filter(|row| row["createdBy"] == author.as_str() && !views.iter().any(|view| view.get("name") == Some(&row["name"]))) {
        Statement::Undefine { name: row["name"].as_str().unwrap_or_default().to_owned() }.run(engine, None, &author, false)?;
    }
    Ok(())
}

/// Rewrites each literal source `values [{…}, …]` (after `on`, `from` or
/// `lookup`) into the constant collection holding its rows,
/// `_values.<hash of the rows>`: written once, then read like any
/// collection (indexes of `lookup`, streaming), by every Place.
pub fn literals(engine: &Engine, query: &str) -> Result<String, String> {
    if !query.contains("values") {
        return Ok(query.to_owned());
    }
    let offset = |part: &str| part.as_ptr() as usize - query.as_ptr() as usize;
    let (mut rewritten, mut copied) = (String::with_capacity(query.len()), 0);
    for range in stage_ranges(query)? {
        let stage = query[range].trim_start();
        let Some(keyword) = ["on ", "from ", "lookup "].iter().find_map(|keyword| stage.strip_prefix(keyword)).map(str::trim_start).filter(|rest| rest.starts_with("values")) else { continue };
        let array = keyword["values".len()..].trim_start();
        if !array.starts_with('[') {
            continue;
        }
        let end = offset(array) + group_length(array)?;
        rewritten.push_str(&query[copied..offset(keyword)]);
        rewritten.push_str(&constant_collection(engine, &query[offset(array)..end])?);
        copied = end;
    }
    rewritten.push_str(&query[copied..]);
    Ok(rewritten)
}

/// The constant collection holding the objects of the array literal `array`.
fn constant_collection(engine: &Engine, array: &str) -> Result<String, String> {
    static WRITING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let value = value_from_source(array).map_err(|error| error.to_string())?;
    let rows = value.as_array().unwrap_or_default().iter().map(|row| row.as_object().cloned()).collect::<Option<Vec<_>>>().filter(|rows| !rows.is_empty()).ok_or("values takes a non-empty array of objects, as in values [{a: 1}, {a: 2}]")?;
    let name = format!("{VALUES_PREFIX}v{:016x}", fnv1a64(value_to_json(&value).to_string().as_bytes()));
    let collection = CollectionId::parse(&name).map_err(|error| error.to_string())?;
    let _writing = crate::helpers::lock_unpoisoned(&WRITING);
    let storage = engine.storage();
    if !storage.read().and_then(|read| read.collection_exists(&collection)).map_err(|error| error.to_string())? {
        let mut transaction = storage.begin().map_err(|error| error.to_string())?;
        for (index, row) in rows.into_iter().enumerate() {
            let id = DocumentId::parse(format!("01900000-0000-7000-8000-{:012x}", index + 1)).map_err(|error| error.to_string())?;
            transaction.insert(&collection, id, std::sync::Arc::new(row)).map_err(|error| error.to_string())?;
        }
        transaction.commit().map_err(|error| error.to_string())?;
    }
    Ok(name)
}

/// `(define, rest)` after `define view` / `undefine view`.
fn head(query: &str) -> Option<(bool, &str)> {
    let query = query.trim_start();
    let (define, rest) = keyword(query, "define").map(|rest| (true, rest)).or_else(|| keyword(query, "undefine").map(|rest| (false, rest)))?;
    keyword(rest.trim_start(), "view").map(|rest| (define, rest))
}

fn keyword<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    text.strip_prefix(word).filter(|rest| rest.starts_with(char::is_whitespace))
}

/// A view name: identifiers joined by dots, such as `finance.cube`.
pub(crate) fn is_name(name: &str) -> bool {
    !name.is_empty() && name.split('.').all(|part| part.chars().next().is_some_and(|first| first.is_alphabetic() || first == '_') && part.chars().all(|character| character.is_alphanumeric() || character == '_'))
}

/// Most rows a `let $x = (on …)` query may return.
const LET_ROWS: usize = 1000;

/// Prepares `query` for `place` and `app_instance` ([`crate::query::prepare_query`]):
/// its `on view` sources are read from `_views`, its `let` queries run in that scope.
pub fn prepare(engine: &Engine, place: Option<&str>, app_instance: Option<&str>, query: &str, parameters: &serde_json::Map<String, JsonValue>) -> Result<String, String> {
    let scope = place.map(|place| scope_of(place, app_instance));
    let rows = |query: &str| {
        let rows = execute(engine, scope.as_ref(), query, true)?;
        if rows.len() > LET_ROWS {
            return Err(format!("a query in `let` returns at most {LET_ROWS} rows"));
        }
        Ok(rows)
    };
    let view = |name: &str, version| {
        let view = definition(engine, place, name, version)?;
        match materialized(engine, place, app_instance, &view)? {
            Some(cache) => Ok(format!("on {cache}")),
            None => query_of(&view),
        }
    };
    prepare_view(query, parameters, Some(Sources { view: &view, rows: &rows }), Vec::new(), false).map(|(prepared, _)| prepared)
}

/// The version of view `name` read by `place`: the Place's own, else the
/// standard one; its active version, or `version`.
fn definition(engine: &Engine, place: Option<&str>, name: &str, version: Option<i64>) -> Result<JsonValue, String> {
    for layer in [place, None].into_iter().take(if place.is_some() { 2 } else { 1 }) {
        let versions = rows(engine, &format!("on {VIEWS_COLLECTION} | where name == {} and {PLACE_SCOPE_FIELD}? == {}", json!(name), json!(layer)))?;
        if let Some(view) = versions.into_iter().find(|row| version.map_or(row["state"] == "active", |version| row["version"] == version)) {
            return Ok(view);
        }
    }
    Err(match version { Some(version) => format!("view {name} has no version {version}"), None => format!("view {name} is not defined") })
}

fn query_of(view: &JsonValue) -> Result<String, String> {
    view["query"].as_str().map(str::to_owned).ok_or_else(|| format!("view {} has no query", view["name"]))
}

/// The cache collection holding the rows of `view` read in this scope, when
/// it is materialized: computed again (into the other of its two
/// collections, so a reader of the previous rows is not cut short) when a
/// collection it reads changed since. `None`: the view is read inline.
fn materialized(engine: &Engine, place: Option<&str>, app_instance: Option<&str>, view: &JsonValue) -> Result<Option<String>, String> {
    static REFRESHING: std::sync::Mutex<()> = std::sync::Mutex::new(());
    if view["materialized"] != true {
        return Ok(None);
    }
    // Only the changes of this Place (and those og-core cannot attribute) count;
    // a storage that cannot tell computes it at every read.
    let reads = view["reads"].as_array().map(Vec::as_slice).unwrap_or_default();
    let changed = reads.iter().map(|name| engine.change_token(name.as_str().unwrap_or_default(), place)).collect::<Option<Vec<_>>>().map_or_else(|| json!(now_millis()), |changed| json!(changed));
    let key = format!("{:016x}", fnv1a64(json!([view["name"], view["version"], view[PLACE_SCOPE_FIELD], place, app_instance]).to_string().as_bytes()));
    let current = || rows(engine, &format!("on {CACHE_COLLECTION} | where key == {}", json!(key))).map(|rows| rows.into_iter().next());
    let fresh = |entry: &Option<JsonValue>| entry.as_ref().filter(|entry| entry["changed"] == changed).map(|entry| entry["collection"].as_str().unwrap_or_default().to_owned());
    if let Some(cache) = fresh(&current()?) {
        return Ok(Some(cache));
    }
    // Prepared before taking the lock: the views it reads refresh their own caches.
    let body = prepare(engine, place, app_instance, &query_of(view)?, &serde_json::Map::new())?;
    let _refreshing = crate::helpers::lock_unpoisoned(&REFRESHING);
    let entry = current()?;
    if let Some(cache) = fresh(&entry) {
        return Ok(Some(cache));
    }
    let slot = entry.as_ref().map_or(0, |entry| 1 - entry["slot"].as_i64().unwrap_or(1));
    let cache = format!("{CACHE_PREFIX}v{key}s{slot}");
    rows(engine, &format!("on {cache} | delete"))?;
    execute(engine, place.map(|place| scope_of(place, app_instance)).as_ref(), &format!("{body} | into {cache}"), false).map_err(|error| format!("view {}: {error}", view["name"]))?;
    let record = json!({"key": key, "view": view["name"], "version": view["version"], "place": place, "appInstance": app_instance, "slot": slot, "collection": cache, "changed": changed, "computedAt": now_millis()});
    rows(engine, &format!("on {CACHE_COLLECTION} | where key == {} | delete", json!(key)))?;
    rows(engine, &insert_query(CACHE_COLLECTION, &record))?;
    Ok(Some(cache))
}

fn parse_statement(query: &str, define: bool) -> Result<Statement, String> {
    let ranges = stage_ranges(query)?;
    let stage = |index: usize| query[ranges[index].clone()].trim();
    let name = head(stage(0)).map(|(_, rest)| rest.trim()).unwrap_or_default();
    if !is_name(name) {
        return Err(format!("invalid view name {name:?}: expected names joined by dots, such as finance.cube"));
    }
    let name = name.to_owned();
    if !define {
        return if ranges.len() == 1 { Ok(Statement::Undefine { name }) } else { Err("undefine view takes no clause".to_owned()) };
    }
    let last = ranges.len() - 1;
    if last == 0 || stage(last) != "end" {
        return Err(format!("define view {name} must end with `| end`"));
    }
    let (mut description, mut version, mut body, mut materialized) = (None, None, None, false);
    let mut index = 1;
    while index < last {
        let clause = stage(index);
        let (word, rest) = clause.split_once(char::is_whitespace).map_or((clause, ""), |(word, rest)| (word, rest.trim()));
        let duplicate = || format!("define view {name}: `{word}` is given twice");
        match word {
            "description" if description.is_none() => description = Some(serde_json::from_str::<String>(rest).map_err(|_| format!("define view {name}: description takes a string, as in description \"…\""))?),
            "version" if version.is_none() => version = Some(rest.parse::<i64>().ok().filter(|version| *version > 0).ok_or_else(|| format!("define view {name}: version takes a positive integer"))?),
            "exec" if rest.is_empty() => {
                if last < index + 3 || stage(last - 1) != "end" {
                    return Err(format!("define view {name}: exec takes a query closed by `| end`, before the final `| end`"));
                }
                body = Some(query[ranges[index + 1].start..ranges[last - 2].end].trim().to_owned());
                index = last - 1;
            }
            "materialized" if rest.is_empty() && !materialized => materialized = true,
            "description" | "version" | "materialized" => return Err(duplicate()),
            _ => return Err(format!("define view {name}: unknown clause `{clause}`; expected description, version, materialized or exec")),
        }
        index += 1;
    }
    if body.is_some() && version.is_some() {
        return Err(format!("define view {name}: give either exec (a new body) or version (a body to restore)"));
    }
    Ok(Statement::Define { name, description, version, body, materialized })
}

/// Plans `body` with its parameters read as `1` (and the views it reads,
/// refusing cycles): its parameters and the collections it reads, or why it
/// cannot be a view.
fn check(engine: &Engine, place: Option<&str>, name: &str, body: &str) -> Result<(Vec<String>, Vec<String>), String> {
    // A materialized view is read from its cache: its reads are the collections it reads.
    let cached = std::cell::RefCell::new(Vec::new());
    let view = |name: &str, version| definition(engine, place, name, version).and_then(|view| if view["materialized"] == true {
        cached.borrow_mut().extend(view["reads"].as_array().into_iter().flatten().filter_map(|read| read.as_str().map(str::to_owned)));
        Ok(format!("on {CACHE_PREFIX}check"))
    } else { query_of(&view) });
    // The collections a `let` query reads are read by the view too.
    let rows = |query: &str| {
        let planned = engine.plan(&pipeline(&literals(engine, query)?)?).map_err(|error| error.to_string())?;
        let mut reads = cached.borrow_mut();
        reads.push(planned.physical().source().collection().to_string());
        collect_reads(planned.physical().operators(), &mut reads);
        Ok(Vec::new())
    };
    let (prepared, parameters) = prepare_view(body, &serde_json::Map::new(), Some(Sources { view: &view, rows: &rows }), vec![name.to_owned()], true)?;
    let planned = engine.plan(&pipeline(&literals(engine, &prepared)?)?).map_err(|error| error.to_string())?;
    if planned.physical().is_write() {
        return Err("a view only reads: its query cannot insert, set, delete or load".to_owned());
    }
    let mut reads = vec![planned.physical().source().collection().to_string()];
    collect_reads(planned.physical().operators(), &mut reads);
    reads.extend(cached.take());
    reads.retain(|collection| !collection.starts_with(VALUES_PREFIX) && !collection.starts_with(CACHE_PREFIX));
    reads.sort();
    reads.dedup();
    Ok((parameters, reads))
}

fn collect_reads(operators: &[PhysicalOperator], reads: &mut Vec<String>) {
    for operator in operators {
        if let PhysicalOperator::Lookup { collection, pipeline, .. } | PhysicalOperator::Union { collection, pipeline, .. } = operator {
            reads.push(collection.to_string());
            collect_reads(pipeline.operators(), reads);
        }
    }
}

fn pipeline(text: &str) -> Result<PlannerPipeline, String> {
    let ast = parse(text).map_err(|error| error.to_string())?;
    PlannerPipeline::from_ast(text, &ast).map_err(|error| error.to_string())
}

fn rows(engine: &Engine, text: &str) -> Result<Vec<JsonValue>, String> {
    execute(engine, None, text, false)
}

/// The scope of `place` (all its App instances), or of one of its App instances.
pub(crate) fn scope_of(place: &str, app_instance: Option<&str>) -> DocumentScope {
    app_instance.map_or_else(|| DocumentScope::for_place(place), |app_instance| DocumentScope::new(place, app_instance))
}

/// Runs `text` in `scope` (og-core's own, without one), returning its rows; `read_only` refuses a write.
pub(crate) fn execute(engine: &Engine, scope: Option<&DocumentScope>, text: &str, read_only: bool) -> Result<Vec<JsonValue>, String> {
    let planned = engine.plan(&pipeline(&literals(engine, text)?)?).map_err(|error| error.to_string())?;
    if read_only && planned.physical().is_write() {
        return Err("a query in `let` only reads".to_owned());
    }
    let output = match scope {
        Some(scope) => engine.execute_physical_scoped(planned.physical(), scope),
        None => engine.execute_physical(planned.physical()),
    }.map_err(|error| error.to_string())?;
    Ok(output.rows().iter().map(|row| document_to_json(row.document())).collect())
}

/// Runs `query` with `parameters` as a request of `place` and `app_instance` ([`prepare`]), returning its rows.
pub fn query(engine: &Engine, place: &str, app_instance: Option<&str>, query: &str, parameters: &serde_json::Map<String, JsonValue>) -> Result<Vec<JsonValue>, String> {
    execute(engine, Some(&scope_of(place, app_instance)), &prepare(engine, Some(place), app_instance, query, parameters)?, false)
}

/// Runs the writes `texts`, each [`prepare`]d as a request of `place` and `app_instance`, in one transaction
/// ([`Engine::execute_together`]).
pub fn write_together(engine: &Engine, place: &str, app_instance: Option<&str>, texts: &[String], parameters: &serde_json::Map<String, JsonValue>) -> Result<(), String> {
    let scope = scope_of(place, app_instance);
    let plans = texts.iter().map(|text| {
        let planned = engine.plan(&pipeline(&literals(engine, &prepare(engine, Some(place), app_instance, text, parameters)?)?)?).map_err(|error| error.to_string())?;
        Ok((planned.physical().clone(), Some(scope.clone())))
    }).collect::<Result<Vec<_>, String>>()?;
    engine.execute_together(&plans).map(drop).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;

    fn engine() -> Engine { let engine = Engine::new(std::sync::Arc::new(MemoryStorage::new())); rows(&engine, r#"on ledger | insert {account: "601", amount: 10, scenario: "actual"}"#).unwrap(); rows(&engine, r#"on accounts | insert {account: "601"}"#).unwrap(); engine }
    fn run(engine: &Engine, place: Option<&str>, text: &str) -> Result<JsonValue, String> { Statement::parse(text).expect("a view statement")?.run(engine, place, "ann", false) }
    fn states(engine: &Engine) -> Vec<(String, i64, String)> { rows(engine, "on _views | sort _place, name, version").unwrap().iter().map(|row| (format!("{}:{}", row[PLACE_SCOPE_FIELD].as_str().unwrap_or("-"), row["name"].as_str().unwrap()), row["version"].as_i64().unwrap(), row["state"].as_str().unwrap().to_owned())).collect() }

    #[test] fn parses_view_statements() { assert_eq!(Statement::parse("on views"), None); assert_eq!(Statement::parse("undefine view a.b"), Some(Ok(Statement::Undefine { name: "a.b".into() }))); assert_eq!(Statement::parse("define view a | description \"x | y\" | exec | on t | group | by k | sum v | end | where v > 1 || v < 0 | end | end"), Some(Ok(Statement::Define { name: "a".into(), description: Some("x | y".into()), version: None, body: Some("on t | group | by k | sum v | end | where v > 1 || v < 0".into()), materialized: false }))); assert_eq!(Statement::parse("define view a | version 2 | end"), Some(Ok(Statement::Define { name: "a".into(), description: None, version: Some(2), body: None, materialized: false }))); for (text, error) in [("define view a.", "invalid view name"), ("define view a | exec | on t | end", "exec takes a query"), ("define view a | where x | end", "unknown clause"), ("define view a | exec | on t", "must end with"), ("define view a | version 0 | end", "positive integer"), ("define view a | version 1 | version 2 | end", "twice"), ("define view a | description x | end", "takes a string"), ("define view a | version 1 | exec | on t | end | end", "either exec"), ("undefine view a | end", "no clause")] { assert!(Statement::parse(text).unwrap().unwrap_err().contains(error), "{text}"); } }
    #[test] fn define_checks_the_body_and_lists_its_parameters_and_reads() { let engine = engine(); let view = run(&engine, Some("p"), "define view finance.cube | description \"cube\" | exec | let $from = $to - 1 | on ledger | where scenario == $scenario and amount >= $from | lookup accounts as a | where a.account == account | into accounts | end | end | end").unwrap(); assert_eq!((view["version"].clone(), view["parameters"].clone(), view["reads"].clone(), view["description"].clone()), (json!(1), json!(["to", "scenario"]), json!(["accounts", "ledger"]), json!("cube"))); assert!(view["query"].as_str().unwrap().starts_with("let $from")); for (text, error) in [("define view w | exec | on ledger | set amount = 0 | end | end", "only reads"), ("define view w | exec | on ledger | where | end | end", "where"), ("define view w | exec | let $x = amount | on ledger | end | end", "cannot depend on documents"), ("define view w | end", "not defined"), ("define view w | version 3 | end", "no version 3"), ("undefine view w", "not defined")] { assert!(run(&engine, Some("p"), text).unwrap_err().contains(error), "{text}"); } assert_eq!(states(&engine).len(), 1); }
    #[test] fn versions_supersede_restore_and_delete_per_place() { let engine = engine(); for (place, text) in [(Some("p"), "define view v | description \"one\" | exec | on ledger | end | end"), (Some("p"), "define view v | exec | on ledger | where amount > $min | end | end"), (Some("q"), "define view v | exec | on accounts | end | end"), (None, "define view v | exec | on ledger | end | end"), (Some("p"), "undefine view v")] { run(&engine, place, text).unwrap(); } let restored = run(&engine, Some("p"), "define view v | version 1 | description \"back\" | end").unwrap(); assert_eq!((restored["version"].clone(), restored["query"].clone(), restored["parameters"].clone(), restored["description"].clone()), (json!(3), json!("on ledger"), json!([]), json!("back"))); assert_eq!(run(&engine, Some("p"), "define view v | end").unwrap()["description"], "back"); assert_eq!(states(&engine), [("-:v".into(), 1, "active".into()), ("p:v".into(), 1, "superseded".into()), ("p:v".into(), 2, "deleted".into()), ("p:v".into(), 3, "superseded".into()), ("p:v".into(), 4, "active".into()), ("q:v".into(), 1, "active".into())]); }
    fn read(engine: &Engine, place: Option<&str>, text: &str, parameters: JsonValue) -> Result<Vec<JsonValue>, String> { rows(engine, &prepare(engine, place, None, text, parameters.as_object().unwrap())?) }
    #[test] fn on_view_reads_the_place_view_else_the_standard_one_with_the_caller_variables() { let engine = engine(); rows(&engine, r#"on ledger | insert {account: "701", amount: 30, scenario: "budget"}"#).unwrap(); for (place, text) in [(None, "define view v | exec | on ledger | where scenario == $scenario | end | end"), (Some("p"), "define view v | exec | let $min = 20 | on ledger | where amount >= $min and scenario == $scenario | end | end"), (Some("p"), "define view v | exec | on ledger | end | end"), (Some("p"), "undefine view v")] { run(&engine, place, text).unwrap(); } let count = |place, text: &str, parameters| read(&engine, place, text, parameters).map(|rows| rows.len()); assert_eq!(count(Some("q"), "on view v", json!({"scenario": "actual"})), Ok(1)); assert_eq!(count(Some("p"), "let $scenario = \"budget\" | on view v | where account == \"701\"", json!({})), Ok(1)); assert_eq!(count(Some("p"), "on view v version 1 | count as n", json!({"scenario": "actual", "min": 0})), Ok(1)); assert_eq!(read(&engine, Some("p"), "on view v version 1", json!({"scenario": "budget", "min": 0})).unwrap()[0]["account"], "701"); assert_eq!(count(Some("p"), "on view v version 2 | union | on view v version 1 | end", json!({"scenario": "budget"})), Ok(3)); for (text, error) in [("on view v", "$scenario is not bound"), ("on view w", "view w is not defined"), ("on view v version 9", "no version 9"), ("on view v as x y", "takes only `version <n>` and `as <alias>`"), ("let $scenario = \"actual\" | on view v version 1 | where amount >= $min", "$min is not bound")] { assert!(read(&engine, Some("p"), text, json!({})).unwrap_err().contains(error), "{text}"); } assert!(crate::query::prepare_query("on view v", &serde_json::Map::new()).unwrap_err().contains("query.execute")); }
    #[test] fn views_read_views_and_lookups_without_cycles() { let engine = engine(); run(&engine, None, "define view a | exec | on accounts | where account == $account | end | end").unwrap(); let b = run(&engine, None, "define view b | exec | on ledger | lookup view a as x | where x.account == account | into accounts | end | end | end").unwrap(); assert_eq!((b["parameters"].clone(), b["reads"].clone()), (json!(["account"]), json!(["accounts", "ledger"]))); assert_eq!(read(&engine, None, "on view b | select account, accounts", json!({"account": "601"})).unwrap(), [json!({"account": "601", "accounts": [{"account": "601"}]})]); for text in ["define view a | exec | on view b | end | end", "define view c | exec | on view c | end | end"] { assert!(run(&engine, None, text).unwrap_err().contains("cycle"), "{text}"); } assert!(crate::query::reads_place("explain on t | union | on view a | end") && !crate::query::reads_place("define view x | exec | on view a | end | end") && !crate::query::reads_place("on view | where a == 1")); }
    #[test] fn apps_deliver_standard_views_once_and_withdraw_them() { let engine = engine(); let views = |list: JsonValue| list.as_array().unwrap().clone(); deliver(&engine, "system.f", &views(json!([{"name": "f.a", "query": "on ledger"}, {"name": "f.b", "description": "b", "query": "on view f.a | where amount > $min"}]))).unwrap(); deliver(&engine, "system.f", &views(json!([{"name": "f.a", "query": "on ledger"}, {"name": "f.b", "description": "b", "query": "on view f.a | where amount > $min"}]))).unwrap(); run(&engine, Some("p"), "define view f.a | exec | on accounts | end | end").unwrap(); deliver(&engine, "system.f", &views(json!([{"name": "f.a", "query": "on ledger | where amount > 1"}]))).unwrap(); assert_eq!(states(&engine), [("-:f.a".into(), 1, "superseded".into()), ("-:f.a".into(), 2, "active".into()), ("-:f.b".into(), 1, "deleted".into()), ("p:f.a".into(), 1, "active".into())]); assert!(deliver(&engine, "system.f", &views(json!([{"name": "f.c", "query": "on ledger | delete"}]))).unwrap_err().contains("App system.f, view f.c: a view only reads")); }
    #[test] fn values_are_constant_collections_read_by_sources_lookups_and_views() { let engine = engine(); let rules = r#"values [{prefix: "60", category: "Achats"}, {prefix: "601", category: "Matieres"}, {prefix: "7", category: "Ventes"}]"#; assert_eq!(rows(&engine, &format!("on {rules} | where len(prefix) > 1 | count as n")).unwrap(), [json!({"n": 2})]); let rewritten = literals(&engine, &format!("on ledger | union | on {rules} | end | lookup {rules} as r | where starts_with(account, r.prefix) | end")).unwrap(); assert_eq!(rewritten.matches(VALUES_PREFIX).count(), 2); assert_eq!(engine.storage().read().unwrap().collections().unwrap().iter().filter(|collection| collection.as_str().starts_with(VALUES_PREFIX)).count(), 1); run(&engine, None, &format!("define view ref.rules | exec | on {rules} | end | end")).unwrap(); assert_eq!(read(&engine, Some("p"), "on ledger as g | lookup view ref.rules as r | where starts_with(g.account, r.prefix) | derive n = len(r.prefix) | sort n desc | first | into rule | end | select account, rule", json!({})).unwrap(), [json!({"account": "601", "rule": [{"category": "Matieres", "n": 3, "prefix": "601"}]})]); let name = rewritten.split_whitespace().find(|word| word.starts_with(VALUES_PREFIX)).unwrap(); let scoped = |text: &str| engine.execute_physical_scoped(engine.plan(&pipeline(text).unwrap()).unwrap().physical(), &DocumentScope::new("p", "app")).map(|output| output.rows().len()); assert_eq!(scoped(&format!("on {name}")).unwrap(), 3); assert!(scoped(&format!("on {name} | delete")).is_err() && rows(&engine, &format!("on {name} | delete")).unwrap_err().contains("constant")); rows(&engine, &format!("on {name} | into copied")).unwrap(); assert_eq!(rows(&engine, "on copied | count as n").unwrap(), [json!({"n": 3})]); for (text, error) in [("on values []", "non-empty array of objects"), ("on values [1]", "non-empty array of objects"), ("on values [{a: 1}", "unclosed")] { assert!(rows(&engine, text).unwrap_err().contains(error), "{text}"); } assert_eq!(literals(&engine, "on values | where values == 1").unwrap(), "on values | where values == 1"); assert_eq!(rows(&engine, r#"on ledger | lookup values [{code: "601", label: "x"}] as v on code == account into hit | select hit"#).unwrap(), [json!({"hit": [{"code": "601", "label": "x"}]})]); }
    #[test] fn let_reads_the_rows_of_a_query_in_the_place() { let engine = engine(); rows(&engine, r#"on settings | insert {_place: "p", opening: 7}"#).unwrap(); rows(&engine, r#"on settings | insert {_place: "q", opening: 1}"#).unwrap(); let opening = |place| read(&engine, place, "let $s = (on settings | limit 1) | let $month = get(at($s, 0), \"opening\") | on ledger | derive m = $month | select m", json!({})).map(|rows| rows[0]["m"].clone()); assert_eq!(opening(Some("p")), Ok(json!(7))); assert_eq!(opening(Some("q")), Ok(json!(1))); assert_eq!(read(&engine, None, "let $a = (on ledger | where amount > $min | select amount), $n = len($a) | on accounts | derive n = $n | select n", json!({"min": 5})).unwrap(), [json!({"n": 1})]); for (text, error) in [("let $a = (on ledger | delete) | on ledger", "only reads"), ("let $a = (on view nope) | on ledger", "view nope is not defined")] { assert!(read(&engine, Some("p"), text, json!({})).unwrap_err().contains(error), "{text}"); } assert!(crate::query::prepare_query("let $a = (on ledger) | on ledger", &serde_json::Map::new()).unwrap_err().contains("query.execute")); assert!(crate::query::reads_place("let $a = (on t) | on u")); let view = run(&engine, None, "define view s | exec | let $s = (on settings | where k == $k) | on ledger | where amount > len($s) | end | end").unwrap(); assert_eq!((view["parameters"].clone(), view["reads"].clone()), (json!(["k"]), json!(["ledger", "settings"]))); }
    #[test] fn a_place_computes_a_materialized_view_from_other_materialized_views() { let engine = engine(); run(&engine, None, "define view m | materialized | exec | on ledger | group | by scenario | sum amount as total | end | end | end").unwrap(); run(&engine, None, "define view both | materialized | exec | on view m | union | on view m | end | end | end").unwrap(); rows(&engine, r#"on ledger | insert {_place: "p", amount: 7, scenario: "actual"}"#).unwrap(); assert_eq!(read(&engine, Some("p"), "on view both | select total", json!({})).unwrap(), [json!({"total": 7}), json!({"total": 7})]); assert_eq!(read(&engine, None, "on view both | select total", json!({})).unwrap().len(), 2); }
    #[test] fn materialized_views_keep_their_rows_per_scope_until_a_read_collection_changes() { let engine = engine(); run(&engine, None, "define view m | materialized | exec | on ledger | group | by scenario | sum amount as total | end | end | end").unwrap(); let total = |place| { let prepared = prepare(&engine, place, None, "on view m | select total", &serde_json::Map::new()).unwrap(); assert!(prepared.contains(CACHE_PREFIX), "{prepared}"); (prepared.clone(), rows(&engine, &prepared).unwrap().iter().map(|row| row["total"].clone()).collect::<Vec<_>>()) }; let (first, totals) = total(None); assert_eq!(totals, [json!(10)]); assert_eq!(total(None), (first.clone(), totals)); rows(&engine, r#"on ledger | insert {account: "602", amount: 5, scenario: "actual"}"#).unwrap(); let (second, totals) = total(None); assert_ne!(first, second); assert_eq!(totals, [json!(15)]); rows(&engine, r#"on accounts | insert {account: "603"}"#).unwrap(); assert_eq!(total(None).0, second); rows(&engine, r#"on ledger | insert {_place: "p", amount: 7, scenario: "actual"}"#).unwrap(); assert_eq!(read(&engine, Some("p"), "on view m | select total", json!({})).unwrap(), [json!({"total": 7})]); assert_eq!(rows(&engine, &format!("on {CACHE_COLLECTION} | count as n")).unwrap(), [json!({"n": 2})]); assert!(run(&engine, None, "define view w | materialized | exec | on ledger | where amount > $min | end | end").unwrap_err().contains("takes no parameter")); }
    #[test] fn a_place_keeps_its_cache_while_only_other_places_write() { let engine = engine(); run(&engine, None, "define view m | materialized | exec | on ledger | group | by scenario | sum amount as total | end | end | end").unwrap(); let write = |place: Option<&str>, amount: i64| { let planned = engine.plan(&pipeline(&format!("on ledger | insert {{amount: {amount}, scenario: \"actual\"}}")).unwrap()).unwrap(); match place { Some(place) => engine.execute_physical_scoped(planned.physical(), &DocumentScope::for_place(place)), None => engine.execute_physical(planned.physical()) }.unwrap(); }; let cache = |place| prepare(&engine, Some(place), None, "on view m", &serde_json::Map::new()).unwrap(); let total = |place| rows(&engine, &prepare(&engine, Some(place), None, "on view m | select total", &serde_json::Map::new()).unwrap()).unwrap(); write(Some("p"), 1); write(Some("q"), 2); let (p, q) = (cache("p"), cache("q")); write(Some("q"), 3); assert_eq!(cache("p"), p); assert_ne!(cache("q"), q); assert_eq!(total("q"), [json!({"total": 5})]); write(Some("p"), 4); assert_ne!(cache("p"), p); assert_eq!(total("p"), [json!({"total": 5})]); let (p, q) = (cache("p"), cache("q")); write(None, 8); assert!(cache("p") != p && cache("q") != q); }
    #[test] fn explain_writes_nothing_and_a_place_reads_only_its_views() { let engine = engine(); assert_eq!(Statement::parse("define view v | exec | on ledger | end | end").unwrap().unwrap().run(&engine, Some("p"), "ann", true).unwrap()["version"], 1); assert!(states(&engine).is_empty()); for place in ["p", "q"] { run(&engine, Some(place), &format!("define view {place} | exec | on ledger | end | end")).unwrap(); } let read = |text: &str| engine.execute_physical_scoped(engine.plan(&pipeline(text).unwrap()).unwrap().physical(), &DocumentScope::new("p", "app")).map(|output| output.rows().len()); assert_eq!(read("on _views | where state == \"active\"").unwrap(), 1); assert!(read("on _views | set state = \"x\"").is_err()); }
}
