#![cfg_attr(rustfmt, rustfmt_skip)]
//! The sources of a Place's content, each giving one part of its index. They read the Place through a [`Reader`] (read-only
//! queries within the reader's access, the bytes of a file) and what every search reads first: its declared schema
//! (`place.schema` over every App instance, whoever sees them: the hits are filtered by the reader's Apps) and the list of
//! its files. Content to come (mails …) is one more source here.
//!
//! - **Catalog**: each App instance by its name and the texts of its definition (titles, labels, descriptions, aliases,
//!   vocabulary: « chiffre d'affaires » finds the Apps that show it), each table by its name and label.
//! - **Collection**: the distinct text values of each text field of a collection (`group | by`), with their row count.
//! - **Files**: the name of each file and folder, with its path; a folder's scope covers what it holds.
//! - **Content**: the text of one file (`extract.rs`) in passages, read again when the file changes.

use std::collections::{BTreeMap, HashMap};

use serde_json::{json, Value as JsonValue};

use super::{extract, Entry, Origin, Part};
use crate::helpers::JsonFields;

/// What the sources read a Place through.
pub trait Reader {
    /// The rows of a read-only query within the Place, under the reader's access.
    fn rows(&mut self, query: String) -> Result<Vec<JsonValue>, String>;
    /// The bytes of the file `remote_id` of the App instance `instance_id`.
    fn content(&mut self, instance_id: &str, remote_id: &str) -> Result<Vec<u8>, String>;
}

/// What every source of a Place starts from: its id, its schema, its App instances with their App (`_app_instances` and
/// `_apps` records), its files (`_files` rows, [`files_query`]).
#[derive(Debug, Clone, Copy)]
pub struct Place<'a> { pub id: &'a str, pub schema: &'a JsonValue, pub apps: &'a [(JsonValue, JsonValue)], pub files: &'a [JsonValue] }

/// The query listing the files of a Place, for [`Place::files`].
#[must_use]
pub fn files_query(place_id: &str) -> String {
    crate::helpers::quoted!("on _files | where _place == {} | select fileId, parentId, name, kind, _app_instance, remoteId, size, etag, modifiedAt", place_id)
}

/// One file whose text is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileText { pub file_id: String, pub instance_id: String, pub remote_id: String, pub name: String, pub path: String, pub scope: Vec<String>, pub folders: JsonValue, pub stamp: JsonValue }

/// One source of a Place's content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source { Catalog, Collection(String), Files, Content(Box<FileText>) }

/// What tells that the part of a source still holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fresh<'a> {
    /// Nothing: it is read at every search (the schema, read anyway).
    Never,
    /// The change token of this collection, for the reader's row filters when `filtered`.
    Collection { name: &'a str, filtered: bool },
    /// This stamp of the file (its size, etag and time).
    Stamp(&'a JsonValue),
}

impl Source {
    /// The sources of `place`: its catalog, its collections, its files, the text of each readable file.
    #[must_use]
    pub fn of(place: &Place<'_>) -> Vec<Self> {
        let mut out = vec![Self::Catalog];
        out.extend(place.schema["collections"].as_array().into_iter().flatten().filter_map(|collection| collection.str_field("name")).filter(|name| crate::view::is_name(name)).map(|name| Self::Collection(name.to_owned())));
        out.push(Self::Files);
        for (file, path, scope, folders) in located(place.files) {
            let (Some(file_id), Some(name), Some(instance_id), Some(remote_id)) = (file.str_field("fileId"), file.str_field("name"), file.str_field("_app_instance"), file.str_field("remoteId")) else { continue };
            if file.str_field("kind") != Some("file") || !extract::readable(name) || file.u64_field("size").is_some_and(|size| size > extract::MAX_BYTES) { continue; }
            let stamp = json!([file["size"], file["etag"], file["modifiedAt"]]);
            out.push(Self::Content(Box::new(FileText { file_id: file_id.into(), instance_id: instance_id.into(), remote_id: remote_id.into(), name: name.into(), path, scope, folders, stamp })));
        }
        out
    }
    /// The key of its part in the index of a Place.
    #[must_use]
    pub fn key(&self) -> String {
        match self { Self::Catalog => "catalog".into(), Self::Collection(name) => format!("collection:{name}"), Self::Files => "files".into(), Self::Content(file) => format!("content:{}", file.file_id) }
    }
    #[must_use]
    pub fn fresh(&self) -> Fresh<'_> {
        match self { Self::Catalog => Fresh::Never, Self::Collection(name) => Fresh::Collection { name, filtered: true }, Self::Files => Fresh::Collection { name: "_files", filtered: false }, Self::Content(file) => Fresh::Stamp(&file.stamp) }
    }

    /// Its part of the index.
    pub fn read(&self, place: &Place<'_>, reader: &mut dyn Reader) -> Result<Part, String> {
        match self {
            Self::Catalog => Ok(catalog(place.schema, place.apps)),
            Self::Collection(name) => collection(place.schema, name, reader),
            Self::Files => Ok(files(place.files)),
            Self::Content(file) => Ok(content(file, reader)),
        }
    }
}

/// The keys of an App definition whose texts tell what it shows (a text, a list of them, or one per language).
const TEXTS: &[&str] = &["title", "subtitle", "label", "labels", "description", "tagline", "text", "aliases", "vocabulary", "name", "label_fr", "label_en"];

/// The texts of an App definition under [`TEXTS`], each once, in their order (its queries are not texts).
fn texts(value: &JsonValue, out: &mut Vec<String>) {
    fn strings(value: &JsonValue, out: &mut Vec<String>) {
        match value {
            JsonValue::String(text) => { let text = text.trim(); if !text.is_empty() && !out.iter().any(|known| known == text) { out.push(text.to_owned()); } }
            JsonValue::Array(items) => items.iter().for_each(|item| strings(item, out)),
            JsonValue::Object(languages) => languages.values().filter(|value| value.is_string() || value.is_array()).for_each(|item| strings(item, out)),
            _ => {}
        }
    }
    match value {
        JsonValue::Object(object) => for (key, item) in object {
            if TEXTS.contains(&key.as_str()) && !item.is_object() || TEXTS.contains(&key.as_str()) && item.as_object().is_some_and(|languages| languages.keys().all(|key| key.len() == 2)) { strings(item, out); } else { texts(item, out); }
        },
        JsonValue::Array(items) => items.iter().for_each(|item| texts(item, out)),
        _ => {}
    }
}

fn catalog(schema: &JsonValue, apps: &[(JsonValue, JsonValue)]) -> Part {
    let (mut origins, mut entries) = (Vec::new(), Vec::new());
    for instance in schema["instances"].as_array().into_iter().flatten() {
        let instance_id = instance.string_field("instanceId");
        let name = instance.str_field("name").unwrap_or(&instance_id).to_owned();
        origins.push(Origin { reference: json!({"instanceId": instance_id, "appId": instance["appId"]}), scope: vec![format!("app:{instance_id}")] });
        // Its name, then what its definition shows: a hit answers the words around what was found.
        let mut shown = vec![name.clone()];
        if let Some((_, app)) = apps.iter().find(|(known, _)| known.str_field("instanceId") == Some(instance_id.as_str())) { texts(&app["definition"], &mut shown); }
        entries.push(Entry { kind: "app", text: shown.join(" · "), count: 0, origin: origins.len() as u32 - 1, own: Some(json!({"name": name})) });
    }
    for table in schema["tables"].as_array().into_iter().flatten() {
        let (instance_id, collection, name) = (table.string_field("instanceId"), table.string_field("collection"), table.string_field("table"));
        origins.push(Origin { reference: json!({"instanceId": instance_id, "table": name, "collection": collection}), scope: vec![format!("app:{instance_id}"), format!("collection:{collection}")] });
        let text = table.str_field("label").map_or(name.clone(), |label| format!("{label} ({name})"));
        entries.push(Entry { kind: "table", text, count: 0, origin: origins.len() as u32 - 1, own: None });
    }
    Part::new(origins, entries)
}

/// The text fields of a collection, with their label (a text, or one per language): declared `text`, or with no type an
/// axis or attribute.
fn text_fields(collection: &JsonValue) -> Vec<(String, JsonValue)> {
    collection["fields"].as_array().into_iter().flatten().filter(|field| match field.str_field("type") {
        Some(kind) => kind == "text",
        None => matches!(field.str_field("role"), Some("axis" | "attribute")),
    }).filter_map(|field| Some((field.str_field("name").filter(|name| crate::view::is_name(name))?.to_owned(), field["label"].clone()))).collect()
}

fn collection(schema: &JsonValue, name: &str, reader: &mut dyn Reader) -> Result<Part, String> {
    let Some(declared) = schema["collections"].as_array().into_iter().flatten().find(|collection| collection.str_field("name") == Some(name)) else { return Ok(Part::default()) };
    let instances: Vec<String> = declared["tables"].as_array().into_iter().flatten().map(|table| format!("app:{}", table.string_field("instanceId"))).collect();
    // The label of the table that owns the collection, to tell where a value lies.
    let owner = &declared["owner"];
    let table = schema["tables"].as_array().into_iter().flatten().find(|table| table["instanceId"] == owner["instanceId"] && table["table"] == owner["table"]).map_or(JsonValue::Null, |table| table["label"].clone());
    let (mut origins, mut entries) = (Vec::new(), Vec::new());
    for (field, label) in text_fields(declared) {
        let mut scope = instances.clone();
        scope.extend([format!("collection:{name}"), format!("field:{name}.{field}")]);
        origins.push(Origin { reference: json!({"collection": name, "table": table, "field": field, "label": label}), scope });
        for row in reader.rows(format!("on {name} | where exists({field}) | group | by {field} as v | count as n | end"))? {
            let Some(text) = row.str_field("v").map(str::trim).filter(|text| !text.is_empty()) else { continue };
            entries.push(Entry { kind: "value", text: text.to_owned(), count: row.u64_field("n").unwrap_or(0), origin: origins.len() as u32 - 1, own: None });
        }
    }
    Ok(Part::new(origins, entries))
}

/// Each file with its path, its scope (its App instance, then the folders above it; a folder is in its own scope) and the
/// folders above it from the root (`{fileId, name}`, to open it where it lies).
fn located(files: &[JsonValue]) -> Vec<(&JsonValue, String, Vec<String>, JsonValue)> {
    let parents: HashMap<&str, (&str, Option<&str>)> = files.iter().filter_map(|file| Some((file.str_field("fileId")?, (file.str_field("name")?, file.str_field("parentId"))))).collect();
    files.iter().filter_map(|file| {
        let (file_id, name, instance_id) = (file.str_field("fileId")?, file.str_field("name")?, file.str_field("_app_instance")?);
        // The folders above it, the nearest first (a cycle stops the walk).
        let (mut path, mut scope, mut folders, mut parent) = (vec![name], vec![format!("app:{instance_id}")], Vec::new(), file.str_field("parentId"));
        while let Some((id, (folder, above))) = parent.and_then(|id| Some((id, parents.get(id)?))) {
            if scope.len() > 64 { break; }
            path.push(folder); scope.push(format!("folder:{id}")); folders.push(json!({"fileId": id, "name": folder})); parent = *above;
        }
        if file.str_field("kind") == Some("directory") { scope.push(format!("folder:{file_id}")); }
        path.reverse(); folders.reverse();
        Some((file, path.join("/"), scope, JsonValue::Array(folders)))
    }).collect()
}

fn files(files: &[JsonValue]) -> Part {
    let (mut origins, mut entries, mut by_scope) = (Vec::new(), Vec::new(), BTreeMap::new());
    for (file, path, scope, folders) in located(files) {
        let instance_id = scope[0].trim_start_matches("app:").to_owned();
        let origin = *by_scope.entry(scope.clone()).or_insert_with(|| { origins.push(Origin { reference: json!({"instanceId": instance_id}), scope }); origins.len() as u32 - 1 });
        let kind = if file.str_field("kind") == Some("directory") { "folder" } else { "file" };
        entries.push(Entry { kind, text: file.string_field("name"), count: 0, origin, own: Some(json!({"fileId": file["fileId"], "path": path, "folders": folders, "size": file["size"], "modifiedAt": file["modifiedAt"]})) });
    }
    Part::new(origins, entries)
}

/// The text of a file in passages; a file that cannot be read has none (its name is still found).
fn content(file: &FileText, reader: &mut dyn Reader) -> Part {
    let Some(text) = reader.content(&file.instance_id, &file.remote_id).ok().and_then(|bytes| extract::text(&file.name, &bytes)) else { return Part::default() };
    let origins = vec![Origin { reference: json!({"instanceId": file.instance_id, "fileId": file.file_id, "path": file.path, "folders": file.folders}), scope: file.scope.clone() }];
    let entries = extract::passages(&text).into_iter().enumerate().map(|(at, passage)| Entry { kind: "passage", text: passage, count: 0, origin: 0, own: Some(json!({"passage": at})) }).collect();
    Part::new(origins, entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture(Vec<String>);
    impl Reader for Fixture {
        fn rows(&mut self, query: String) -> Result<Vec<JsonValue>, String> { self.0.push(query.clone()); Ok(if query.contains("by label") { vec![json!({"v": "T.V.A. collectée", "n": 3}), json!({"v": "", "n": 1})] } else { vec![json!({"v": 445_710, "n": 1})] }) }
        fn content(&mut self, _: &str, remote_id: &str) -> Result<Vec<u8>, String> { if remote_id == "r-x" { Ok(format!("{} Le montant de la TVA collectée est reporté ligne 08. {}", "préambule ".repeat(150), "suite ".repeat(150)).into_bytes()) } else { Err("gone".into()) } }
    }

    #[test] fn sources_read_the_catalog_text_fields_file_paths_and_file_texts() { let schema = json!({"instances": [{"instanceId": "i1", "appId": "system.finance", "name": "Finance"}], "tables": [{"instanceId": "i1", "table": "balance", "collection": "balance", "label": "Balance"}], "collections": [{"name": "balance", "tables": [{"instanceId": "i1"}], "fields": [{"name": "label", "type": "text"}, {"name": "amount", "role": "measure"}, {"name": "account", "role": "axis"}]}]}); let files = [json!({"fileId": "d", "name": "Compta 2022", "kind": "directory", "_app_instance": "f", "remoteId": "r-d"}), json!({"fileId": "x", "parentId": "d", "name": "Déclaration TVA.txt", "kind": "file", "_app_instance": "f", "remoteId": "r-x", "size": 10}), json!({"fileId": "y", "parentId": "d", "name": "photo.jpg", "kind": "file", "_app_instance": "f", "remoteId": "r-y"}), json!({"fileId": "z", "name": "perdu.md", "kind": "file", "_app_instance": "f", "remoteId": "r-z"})]; let place = Place { id: "p", schema: &schema, apps: &[], files: &files }; let sources = Source::of(&place); assert_eq!(sources.iter().map(Source::key).collect::<Vec<_>>(), ["catalog", "collection:balance", "files", "content:x", "content:z"]); let mut reader = Fixture(Vec::new()); let part = sources[1].read(&place, &mut reader).unwrap(); assert_eq!((reader.0.len(), part.len()), (2, 1)); let hits = crate::search::search(&[&part], "tva", &["field:balance.label".into()], &|_| true, 5); assert_eq!((hits[0].entry.count, hits[0].origin.scope.contains(&"app:i1".to_owned())), (3, true)); let names = sources[2].read(&place, &mut reader).unwrap(); let hit = crate::search::search(&[&names], "declaration", &["folder:d".into()], &|_| true, 5)[0].to_json(); assert_eq!((hit["kind"].clone(), hit["path"].clone(), hit["folders"].clone()), (json!("file"), json!("Compta 2022/Déclaration TVA.txt"), json!([{"fileId": "d", "name": "Compta 2022"}]))); let text = sources[3].read(&place, &mut reader).unwrap(); assert!(sources[4].read(&place, &mut reader).unwrap().is_empty()); let hit = crate::search::search(&[&text], "ligne 08", &["folder:d".into()], &|_| true, 5)[0].to_json(); assert_eq!((hit["kind"].clone(), hit["path"].clone(), hit["passage"].clone()), (json!("passage"), json!("Compta 2022/Déclaration TVA.txt"), json!(1))); assert!(hit["text"].as_str().unwrap().contains("reporté ligne 08") && hit["text"].as_str().unwrap().len() < 400, "{hit}"); assert_eq!(sources[3].fresh(), Fresh::Stamp(&json!([10, null, null]))); assert_eq!(found(&catalog(&schema, &[]), "balance"), ["Balance (balance)"]); let apps = [(json!({"instanceId": "i1"}), json!({"definition": {"description": "Chiffre d'affaires, marge et taux de marge", "store": {"tagline": {"fr": "Activité", "en": "Activity"}}, "queryViews": [{"query": "on ventes | where label == \"Factures\""}], "views": {"full": [{"title": "Encours clients", "labels": ["Variation du chiffre d'affaires"]}]}}}))]; let catalog = catalog(&schema, &apps); let hit = crate::search::search(&[&catalog], "chiffre d'affaires", &[], &|_| true, 5)[0].to_json(); assert_eq!((hit["kind"].clone(), hit["name"].clone(), hit["text"].clone()), (json!("app"), json!("Finance"), json!("Finance · Chiffre d'affaires, marge et taux de marge · Activity · Activité · Variation du chiffre d'affaires · Encours clients"))); assert!(crate::search::search(&[&catalog], "factures", &[], &|_| true, 5).is_empty()); }
    fn found(part: &Part, text: &str) -> Vec<String> { crate::search::search(&[part], text, &[], &|_| true, 5).iter().map(|hit| hit.entry.text.clone()).collect() }
}
