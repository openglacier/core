#![cfg_attr(rustfmt, rustfmt_skip)]
//! The native data worker (feature `native-import`): what `python -m glacierdb.data_worker` does, without Python.
//! `ogd data-worker` reads one JSON request on stdin (`operation`: `analyze`, `import`/`transform`, `preview`; `file`;
//! `mapping`; `batch`) and answers on stdout with the same JSON: the analysis, or the documents of an import (one line
//! `{"documents": [...]}` per batch with `batch`, then the summary). It runs in a process of its own, under memory and
//! processor limits (`ogd`), so that a file that breaks it never breaks the Core.
//!
//! Beyond the Python worker: workbooks are told by their content (xlsb and ods too), several tables per sheet, merged
//! cells and group headers, empty rows and footer rows (« Total », notes) left out, a profile on 100,000 rows. See
//! ideas/data-import-rust.md.
mod convert;
mod excel;
mod profile;
mod scalar;
mod text;

use crate::helpers::JsonFields;
use crate::model::{Document, Value};
use crate::query::{evaluate_document, parse_expression, Expression};
use convert::{convert, Format};
use scalar::{column_names, header_row, normalize_text, regex, Cell, HEADER_FOLLOWING, HEADER_SCAN};
use serde_json::{json, Map, Value as JsonValue};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::Path;

const PREVIEW_ROWS: usize = 12;
/// Rows the analysis profiles (`OG_DATA_WORKER_PROFILE_ROWS`, the Python worker read about 1,000); it counts them all.
fn profile_rows() -> usize { crate::helpers::env_text("OG_DATA_WORKER_PROFILE_ROWS").and_then(|rows| rows.parse().ok()).unwrap_or(100_000) }
/// A batch of a streamed import: this many documents at most, or about this many bytes of JSON.
const BATCH_ROWS: usize = 5000;
const BATCH_BYTES: usize = 1 << 20;
const MAX_WARNINGS: usize = 200;
/// Footer rows held back at most: past them, the rows were data after all.
const MAX_FOOTERS: usize = 1000;

/// A table of the file: its name (the sheet, or the file), the row of its header, its columns and its data rows.
pub(crate) struct Table { pub name: String, pub header: usize, pub columns: Vec<String>, pub rows: Box<dyn Iterator<Item = Result<Vec<Cell>, String>>>, pub hidden: bool, pub titles: Vec<String> }

/// The lines written above a header (« Balance au 31/07/2025 », « Export du … »): the filled cells of each row, the last ten.
pub(crate) fn title_lines<'a>(rows: impl Iterator<Item = &'a [Cell]>) -> Vec<String> {
    let lines: Vec<String> = rows.map(|row| row.iter().map(Cell::normalized).filter(|value| !value.is_empty()).collect::<Vec<_>>().join(" ")).filter(|line| !line.is_empty()).collect();
    lines[lines.len().saturating_sub(10)..].to_vec()
}

/// The data rows of a table without its empty rows, nor its footer rows: the rows after the last data row whose first
/// cell says « Total », « Sous-total »…, or holding one or two cells saying « Source », « Note »… (`footers` once read).
struct Rows { rows: Box<dyn Iterator<Item = Result<Vec<Cell>, String>>>, held: VecDeque<Vec<Cell>>, ready: VecDeque<Vec<Cell>>, footers: Vec<String> }

impl Iterator for Rows {
    type Item = Result<Vec<Cell>, String>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.ready.pop_front() { return Some(Ok(row)); }
            match self.rows.next() {
                None => { self.footers = self.held.drain(..).filter_map(|row| row.iter().map(Cell::normalized).find(|value| !value.is_empty())).collect(); return None; }
                Some(Err(error)) => return Some(Err(error)),
                Some(Ok(row)) if row.iter().all(Cell::is_empty) => {}
                Some(Ok(row)) if footer(&row) && self.held.len() < MAX_FOOTERS => self.held.push_back(row),
                Some(Ok(row)) => { self.ready.extend(self.held.drain(..)); self.ready.push_back(row); }
            }
        }
    }
}

fn footer(row: &[Cell]) -> bool {
    // Most rows start with a number or a code: only a first cell starting with a letter or `*` is looked at.
    let Some(Cell::Text(first)) = row.iter().find(|cell| !cell.is_empty()) else { return false };
    if !scalar::py_strip(first).starts_with(|c: char| c.is_alphabetic() || c == '*') { return false; }
    let first = scalar::casefold(&normalize_text(first));
    regex!(r"^(?:total|totaux|sous[- ]?totaux?|sub[- ]?totals?|grand[- ]total|totale?s? général)\b").is_match(&first)
        || row.iter().filter(|cell| !cell.is_empty()).count() <= 2 && regex!(r"^(?:sources?|notes?|remarques?|n\.\s?b\b|nb\s?:|\*)").is_match(&first)
}

/// The table of a text file (named after the file): its rows streamed, its header among the first 40.
fn text_table(path: &Path) -> Result<Table, String> {
    let mut reader = text::TextRows::open(path).map_err(|error| format!("cannot read the file: {error}"))?;
    let mut head = Vec::new();
    while head.len() < HEADER_SCAN + HEADER_FOLLOWING { match reader.next_row().map_err(|error| error.to_string())? { Some(row) => head.push(row), None => break } }
    let header = header_row(&head.iter().map(|row| row.iter().map(|value| normalize_text(value)).collect()).collect::<Vec<_>>());
    let columns = head.get(header).map(|row| column_names(&row.iter().cloned().map(Cell::Text).collect::<Vec<_>>())).unwrap_or_default();
    let rest = head.split_off((header + 1).min(head.len()));
    let cells: Vec<Vec<Cell>> = head.iter().take(header).map(|row| row.iter().cloned().map(Cell::Text).collect()).collect();
    let titles = title_lines(cells.iter().map(Vec::as_slice));
    let rows = rest.into_iter().map(Ok).chain(std::iter::from_fn(move || reader.next_row().transpose())).map(|row| row.map(|row| row.into_iter().map(Cell::Text).collect()).map_err(|error| error.to_string()));
    let name = path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default();
    Ok(Table { name, header, columns, rows: Box::new(rows), hidden: false, titles })
}

/// The tables of the file, a workbook or a text file whatever its name says; with `wanted`, the table of that name only,
/// else the one holding most of the `sources` columns.
fn tables(path: &Path, wanted: Option<&str>, sources: &[&str]) -> Result<Vec<(Table, Rows)>, String> {
    let tables = if excel::is_workbook(path) { excel::tables(path, wanted, sources)? } else { vec![text_table(path)?] };
    Ok(tables.into_iter().map(|mut table| { let rows = Rows { rows: std::mem::replace(&mut table.rows, Box::new(std::iter::empty())), held: VecDeque::new(), ready: VecDeque::new(), footers: Vec::new() }; (table, rows) }).collect())
}

fn extension(path: &Path) -> String { path.extension().map(|extension| extension.to_string_lossy().to_lowercase()).unwrap_or_default() }

/// `analyze`: per table its header row, columns, fingerprint, row count, profile and first rows; and the fingerprint of
/// the file (its tables' in any order).
pub(crate) fn analyze(path: &Path) -> Result<JsonValue, String> {
    let (mut out, profiled) = (Vec::new(), profile_rows());
    for (table, mut rows) in tables(path, None, &[])? {
        let mut profiles: Vec<profile::Profile> = table.columns.iter().map(|_| profile::Profile::new()).collect();
        let (mut preview, mut count) = (Vec::new(), 0usize);
        for row in &mut rows {
            let row = row?;
            if count < profiled {
                let values: Vec<String> = row.iter().map(Cell::normalized).collect();
                values.iter().zip(&mut profiles).for_each(|(value, profile)| profile.add(value));
                if count < PREVIEW_ROWS { preview.push(table.columns.iter().cloned().zip(values.into_iter().map(JsonValue::String)).collect::<Map<_, _>>()); }
            }
            count += 1;
        }
        let mut item = json!({"name": table.name, "headerRow": table.header + 1, "columns": table.columns, "fingerprint": scalar::schema_fingerprint(&table.columns), "rowCount": count, "sampled": count > profiled,
            "profile": table.columns.iter().zip(&profiles).map(|(column, profile)| profile.finish(column)).collect::<Vec<_>>(), "preview": preview});
        if !rows.footers.is_empty() { item["footerRows"] = json!(rows.footers); }
        if table.hidden { item["hidden"] = true.into(); }
        if !table.titles.is_empty() { item["titleRows"] = json!(table.titles); }
        out.push(item);
    }
    let mut fingerprints: Vec<&str> = out.iter().filter_map(|table| table.str_field("fingerprint")).collect(); fingerprints.sort_unstable();
    Ok(json!({"status": "analyzed", "format": extension(path), "fingerprint": scalar::sha1_hex(fingerprints.join("|").as_bytes()), "tables": out}))
}

/// What could not be read, the first 200.
#[derive(Default)]
struct Warnings(Vec<JsonValue>);
impl Warnings { fn push(&mut self, warning: JsonValue) { if self.0.len() < MAX_WARNINGS { self.0.push(warning); } } }

/// Python's `str()` of a JSON value of the mapping.
fn py_text(value: &JsonValue) -> String {
    match value { JsonValue::String(text) => text.clone(), JsonValue::Null => "None".into(), JsonValue::Bool(value) => if *value { "True" } else { "False" }.into(), JsonValue::Number(number) => number.as_f64().filter(|_| number.is_f64()).map_or_else(|| number.to_string(), scalar::py_repr), other => other.to_string() }
}

fn falsy(value: &JsonValue) -> bool { match value { JsonValue::Null => true, JsonValue::Bool(value) => !value, JsonValue::Number(number) => number.as_f64() == Some(0.0), JsonValue::String(text) => text.is_empty(), JsonValue::Array(items) => items.is_empty(), JsonValue::Object(fields) => fields.is_empty() } }

fn target(field: &JsonValue) -> Option<String> { field.get("target").filter(|target| !falsy(target)).map(py_text) }

/// The name a column takes in a formula expression when its own is not a name of the language: lower case, without
/// accents, `_` for the rest (`Code article` → `code_article`, `U. Vente` → `u_vente`, `2025` → `_2025`).
pub(crate) fn formula_name(column: &str) -> String {
    use unicode_normalization::UnicodeNormalization;
    let mut out = String::new();
    for c in column.nfd().filter(|c| !unicode_normalization::char::is_combining_mark(*c)).flat_map(char::to_lowercase) {
        if crate::helpers::is_identifier_continue(c) { out.push(c); } else if !out.ends_with('_') { out.push('_'); }
    }
    let out = out.trim_matches('_').to_owned();
    if out.starts_with(|c: char| c.is_ascii_digit()) { format!("_{out}") } else if out.is_empty() { "_".into() } else { out }
}

/// A cell as a formula expression reads it: a number when it reads as one (« 1 234,56 », « (12) »), but a code written
/// with its leading zeros (« 01 », « 010178 ») stays text; a date of a workbook as its epoch milliseconds.
fn formula_value(cell: &Cell) -> Value {
    let number = |value: f64| if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 { Value::signed(value as i64) } else { Value::float(value).unwrap_or(Value::Null) };
    match cell {
        Cell::Int(value) => Value::signed(*value),
        Cell::Float(value) => number(*value),
        Cell::Bool(value) => Value::Bool(*value),
        Cell::Stamp(stamp) => Value::signed(stamp.days * 86_400_000 + stamp.nanos / 1_000_000),
        Cell::Missing => Value::Null,
        Cell::Text(text) => {
            let text = scalar::py_strip(text);
            if text.is_empty() { return Value::Null; }
            let code = text.starts_with('0') && text[1..].starts_with(|c: char| c.is_ascii_digit());
            match convert::number(text).filter(|_| !code) { Some(value) => number(value), None => Value::string(text) }
        }
        other => other.py_str().map_or(Value::Null, |text| Value::string(text.as_str())),
    }
}

/// The value of a formula expression converted as a mapped column would be (`transform`): a date as its month (epoch
/// milliseconds of its first day) or its day, a text read under the field's format.
fn formula_typed(value: JsonValue, transform: Option<&str>, format: &Format) -> JsonValue {
    use crate::query::dates::{self, DateUnit};
    let millis = |value: &JsonValue| value.as_f64().filter(|value| value.is_finite()).map(|value| value as i64);
    match (transform.unwrap_or(""), &value) {
        (_, JsonValue::Null) | ("" | "string", _) => value,
        ("month", JsonValue::Number(_)) => millis(&value).and_then(|value| dates::trunc(value, DateUnit::Month).ok()).map_or(JsonValue::Null, JsonValue::from),
        ("date", JsonValue::Number(_)) => millis(&value).and_then(|value| dates::format(value, "%F").ok()).map_or(JsonValue::Null, JsonValue::from),
        ("datetime", JsonValue::Number(_)) => millis(&value).map_or(JsonValue::Null, |value| dates::to_iso(value).into()),
        ("number" | "decimal", JsonValue::Number(_)) => value,
        (transform, JsonValue::String(text)) => convert(&Cell::Text(text.clone()), Some(transform), format),
        (transform, other) => convert(&Cell::Text(py_text(other)), Some(transform), format),
    }
}

/// The documents of the mapped table, one at a time to `each`: the fields converted, then the computed ones (constants,
/// `concat`, formulas: an expression of the language on the columns of the row, or signed terms summed); returns their
/// count. An expression that does not parse stops the import; one that fails on a row leaves its field null, told.
fn documents(path: &Path, mapping: &JsonValue, warnings: &mut Warnings, each: &mut dyn FnMut(JsonValue) -> Result<(), String>) -> Result<usize, String> {
    let wanted = mapping.get("source").and_then(|source| source.get("sheet")).and_then(JsonValue::as_str);
    let sources: Vec<&str> = mapping.array_field("fields").into_iter().flatten().filter_map(|field| field.str_field("source")).collect();
    let Some((table, mut rows)) = tables(path, Some(wanted.unwrap_or("\u{0}")), &sources)?.into_iter().next() else { return Err("no tabular data detected".into()) };
    let column = |source: Option<&JsonValue>| source.and_then(JsonValue::as_str).and_then(|source| table.columns.iter().position(|column| column == source));
    let fields: Vec<(String, Option<usize>, JsonValue, Option<String>, Format)> = mapping.array_field("fields").into_iter().flatten().filter_map(|field| {
        Some((target(field)?, column(field.get("source")), field.get("source").cloned().unwrap_or(JsonValue::Null), field.str_field("transform").map(str::to_owned), Format::parse(field.get("format"))))
    }).collect();
    let computed: Vec<&JsonValue> = mapping.array_field("computed").into_iter().flatten().collect();
    // The expressions, read once; the columns under their own names and, when these are not names of the language, as
    // `formula_name` writes them.
    let mut expressions: Vec<Option<(Expression, Format)>> = Vec::new();
    for field in &computed {
        let expression = match (field.str_field("type"), field.str_field("expression").map(str::trim).filter(|text| !text.is_empty())) {
            (Some("formula"), Some(text)) => Some((parse_expression(text).map_err(|error| format!("formula of {}: {error}", target(field).unwrap_or_default()))?, Format::parse(field.get("format")))),
            _ => None,
        };
        expressions.push(expression);
    }
    let names: Vec<Vec<String>> = table.columns.iter().map(|column| {
        let own = formula_name(column);
        let valid = column.starts_with(crate::helpers::is_identifier_start) && column.chars().all(crate::helpers::is_identifier_continue);
        if valid && own != *column { vec![column.clone(), own] } else if valid { vec![column.clone()] } else { vec![own] }
    }).collect();
    let mut count = 0;
    for row in &mut rows {
        let row = row?; let line = count + 1; let mut doc = Map::new();
        for (target, index, source, transform, format) in &fields {
            match index { Some(index) => { doc.insert(target.clone(), convert(&row[*index], transform.as_deref(), format)); } None => warnings.push(json!({"row": line, "field": source, "message": "source column missing"})) }
        }
        let mut cells: Option<Document> = None;
        for (field, expression) in computed.iter().zip(&expressions) {
            let Some(target) = target(field) else { continue };
            if let Some(value) = field.get("value") { doc.insert(target, value.clone()); continue; }
            if let Some((expression, format)) = expression {
                let cells = cells.get_or_insert_with(|| {
                    let mut cells = Document::new();
                    for (cell, names) in row.iter().zip(&names) { for name in names { cells.entry(name.as_str()).or_insert_with(|| formula_value(cell)); } }
                    cells
                });
                let value = match evaluate_document(expression, cells) {
                    Ok(value) => formula_typed(crate::helpers::value_to_json(&value), field.str_field("transform"), format),
                    Err(error) => { warnings.push(json!({"row": line, "field": target, "message": format!("formula: {error}")})); JsonValue::Null }
                };
                doc.insert(target, value);
                continue;
            }
            match field.str_field("type") {
                Some("concat") => {
                    let separator = field.get("separator").map_or_else(String::new, py_text);
                    let parts: Vec<String> = field.array_field("fields").into_iter().flatten().map(|name| { let value = name.as_str().and_then(|name| doc.get(name)).unwrap_or(&JsonValue::Null); if falsy(value) { String::new() } else { py_text(value) } }).collect();
                    doc.insert(target, parts.join(&separator).into());
                }
                Some("formula") => {
                    let mut total = 0.0;
                    for term in field.array_field("terms").into_iter().flatten() {
                        let sign = if term.get("sign").and_then(JsonValue::as_f64) == Some(-1.0) { -1.0 } else { 1.0 };
                        if let Some(source) = term.get("source") {
                            let Some(index) = column(Some(source)) else { if count == 0 { warnings.push(json!({"row": line, "field": source, "message": "source column missing"})); } continue };
                            let raw = &row[index];
                            let Some(text) = raw.py_str().filter(|text| !scalar::py_strip(text).is_empty()) else { continue };
                            match convert::number_of(raw) { Some(value) => total += sign * value, None => warnings.push(json!({"row": line, "field": source, "message": format!("not a number: {}", text.chars().take(40).collect::<String>())})) }
                        } else {
                            let value = match term.get("value") { None => Some(0.0), Some(JsonValue::Number(value)) => value.as_f64(), Some(JsonValue::Bool(value)) => Some(f64::from(u8::from(*value))), Some(JsonValue::String(text)) => text.trim().parse().ok(), _ => None };
                            if let Some(value) = value { total += sign * value; }
                        }
                    }
                    doc.insert(target, JsonValue::from(total));
                }
                _ => {}
            }
        }
        each(JsonValue::Object(doc))?; count += 1;
    }
    for (offset, label) in rows.footers.iter().enumerate() { warnings.push(json!({"row": count + offset + 1, "message": format!("footer row left out: {}", label.chars().take(40).collect::<String>())})); }
    Ok(count)
}

/// The import as JSON lines: `{"documents": [...]}` per batch, then the summary (`status`, `rowCount`, `warnings`).
fn transform_stream(path: &Path, mapping: &JsonValue, out: &mut dyn Write) -> Result<(), String> {
    let (mut warnings, mut batch, mut size) = (Warnings::default(), Vec::new(), 0);
    let flush = |batch: &mut Vec<String>, size: &mut usize, out: &mut dyn Write| -> Result<(), String> {
        if !batch.is_empty() { writeln!(out, "{{\"documents\":[{}]}}", batch.join(",")).and_then(|()| out.flush()).map_err(|error| error.to_string())?; }
        batch.clear(); *size = 0; Ok(())
    };
    let rows = documents(path, mapping, &mut warnings, &mut |doc| {
        let text = doc.to_string(); size += text.len(); batch.push(text);
        if batch.len() >= BATCH_ROWS || size >= BATCH_BYTES { flush(&mut batch, &mut size, out)?; }
        Ok(())
    })?;
    flush(&mut batch, &mut size, out)?;
    writeln!(out, "{}", json!({"status": "transformed", "rowCount": rows, "warnings": warnings.0})).map_err(|error| error.to_string())
}

fn transform(path: &Path, mapping: &JsonValue, limit: Option<usize>) -> Result<JsonValue, String> {
    let (mut warnings, mut out) = (Warnings::default(), Vec::new());
    let rows = documents(path, mapping, &mut warnings, &mut |doc| { if limit.is_none_or(|limit| out.len() < limit) { out.push(doc); } Ok(()) })?;
    Ok(json!({"status": "transformed", "documents": out, "rowCount": rows, "warnings": warnings.0}))
}

fn run(request: &str, out: &mut dyn Write) -> Result<(), String> {
    let request: JsonValue = serde_json::from_str(request).map_err(|error| format!("invalid request: {error}"))?;
    let path = Path::new(request.str_field("file").ok_or("the request names no file")?);
    let mapping = request.get("mapping").filter(|mapping| !mapping.is_null()).cloned().unwrap_or_else(|| json!({}));
    let result = match request.str_field("operation").unwrap_or_default() {
        "analyze" => analyze(path)?,
        "import" | "transform" if request.get("batch").is_some_and(|batch| !falsy(batch)) => return transform_stream(path, &mapping, out),
        "import" | "transform" => transform(path, &mapping, None)?,
        "preview" => transform(path, &mapping, Some(25))?,
        other => return Err(format!("unsupported operation: {other}")),
    };
    write!(out, "{result}").map_err(|error| error.to_string())
}

/// `ogd data-worker`: the request on stdin, the answer on stdout; a failure, a panic included, as the error line
/// `{"status": "error", "error": {"code": "worker.failed", "message": …}}` after what was written, and exit status 1.
pub fn main() -> i32 {
    let mut request = String::new();
    let stdout = std::io::stdout(); let mut out = std::io::BufWriter::new(stdout.lock());
    let result = std::io::stdin().read_to_string(&mut request).map_err(|error| error.to_string())
        .and_then(|_| std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&request, &mut out))).unwrap_or_else(|panic| Err(format!("the data worker failed: {}", panic.downcast_ref::<String>().map(String::as_str).or_else(|| panic.downcast_ref::<&str>().copied()).unwrap_or("panic")))));
    let code = match result { Ok(()) => 0, Err(message) => { let _ = write!(out, "\n{}", json!({"status": "error", "error": {"code": "worker.failed", "message": message}})); 1 } };
    let _ = out.flush(); code
}

#[cfg(test)]
mod tests;
