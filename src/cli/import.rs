//! `import FILE [COLLECTION]`: streams a CSV or TSV file into a collection.
//!
//! The file is read record by record (quoted fields, doubled quotes, line
//! breaks inside quotes, CRLF, UTF-8 BOM; other bytes read as Latin-1). The
//! first record names the fields. Cells are typed as og-peek does: integers
//! (no leading zero, within ±2⁵³), floats, `true`/`false`, empty = null, text
//! otherwise; with a `;` or tab separator, `12,5` is a float. Rows go out as
//! `on C | load | with merge | chunk [...] | end` requests of about 7 MiB: one
//! Glacier commit per request, past the 16 384 rows that skip the WAL. A reader
//! thread builds the next request while the server writes the current one.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{fs::File, io::{self, BufRead, BufReader}, path::Path, sync::mpsc, thread, time::Instant};

use serde_json::{json, Value};

use super::Result;

const BATCH_BYTES: usize = 7 << 20;
const MAX_SAFE_INTEGER: i64 = 1 << 53;

pub(super) struct ImportArgs { pub path: String, pub collection: String, separator: Option<u8>, text: bool }

/// `FILE [COLLECTION] [--sep C] [--text]`; the collection defaults to the file stem.
pub(super) fn parse_args(input: &str) -> Result<ImportArgs> {
    let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidInput, message);
    let (mut words, mut separator, mut text) = (Vec::new(), None, false);
    let mut parts = split_words(input).into_iter();
    while let Some(word) = parts.next() {
        match word.as_str() {
            "--text" => text = true,
            "--sep" => separator = Some(match parts.next().as_deref() { Some("tab" | "\\t") => b'\t', Some(s) if s.len() == 1 => s.as_bytes()[0], _ => return Err(invalid("--sep expects one character or tab".into()).into()) }),
            _ if word.starts_with("--") => return Err(invalid(format!("unknown import option {word}")).into()),
            _ => words.push(word),
        }
    }
    let [path, rest @ ..] = &words[..] else { return Err(invalid("usage: import FILE [COLLECTION] [--sep C] [--text]".into()).into()) };
    let path = expand_home(path);
    let collection = match rest { [] => collection_name(Path::new(&path).file_stem().and_then(|s| s.to_str()).unwrap_or("import")), [name] => name.clone(), _ => return Err(invalid("import takes FILE [COLLECTION]".into()).into()) };
    if !collection.starts_with(char::is_alphabetic) || !collection.chars().all(crate::helpers::is_identifier_continue) {
        return Err(invalid(format!("{collection:?} is not a collection identifier (a letter first; `_` names are system collections)")).into());
    }
    Ok(ImportArgs { path, collection, separator, text })
}

/// Words split on blanks, with `"…"` or `'…'` keeping spaces in a path.
fn split_words(input: &str) -> Vec<String> {
    let (mut words, mut word, mut quote, mut open) = (Vec::new(), String::new(), None, false);
    for c in input.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), c) => word.push(c),
            (None, '"' | '\'') => { quote = Some(c); open = true }
            (None, c) if c.is_whitespace() => { if open || !word.is_empty() { words.push(std::mem::take(&mut word)) } open = false }
            (None, c) => word.push(c),
        }
    }
    if open || !word.is_empty() { words.push(word) }
    words
}

pub(super) fn expand_home(path: &str) -> String {
    match (path.strip_prefix("~/"), std::env::var("HOME")) { (Some(rest), Ok(home)) => format!("{home}/{rest}"), _ => path.to_owned() }
}

/// A file stem made a collection identifier: other characters become `_`,
/// and a stem that would start with a digit or `_` (system) gets a `t` first.
fn collection_name(stem: &str) -> String {
    let name: String = stem.chars().map(|c| if crate::helpers::is_identifier_continue(c) { c } else { '_' }).collect();
    if name.starts_with(|c: char| c.is_alphabetic()) { name } else { format!("t{name}") }
}

/// One request ready to send, and where the reader was when it closed it.
struct Batch { query: String, bytes_read: u64 }

/// Runs the import over `run` (one request in, its statistics out), reporting
/// each committed request to `progress`. Returns the summary document.
pub(super) fn run(args: &ImportArgs, mut execute: impl FnMut(&str) -> Result<Value>, mut progress: impl FnMut(String) -> Result<()>) -> Result<Value> {
    let file = File::open(&args.path).map_err(|error| io::Error::new(error.kind(), format!("{}: {error}", args.path)))?;
    let total = file.metadata()?.len().max(1);
    let mut reader = Reader::new(BufReader::with_capacity(1 << 20, file));
    let header = reader.record()?.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty file"))?;
    let separator = args.separator.unwrap_or_else(|| detect_separator(&args.path, &reader.raw_header));
    let fields = field_names(&split_record(&header, separator)?);
    let prefix = format!("on {} | load | with merge | chunk [", args.collection);
    let (text, started) = (args.text, Instant::now());
    let (tx, rx) = mpsc::sync_channel::<io::Result<Batch>>(1);
    let producer = thread::spawn(move || {
        let keys: Vec<String> = fields.iter().map(|name| { let mut key = String::new(); push_string(&mut key, name); key + ":" }).collect();
        let (mut query, mut rows, mut cells) = (String::with_capacity(BATCH_BYTES + (1 << 16)), 0u64, Vec::new());
        query.push_str(&prefix);
        loop {
            let record = match reader.record() { Ok(record) => record, Err(error) => { let _ = tx.send(Err(error)); return } };
            let done = record.is_none();
            if let Some(record) = record.filter(|r| !r.iter().all(|b| b.is_ascii_whitespace())) {
                if let Err(error) = split_into(&record, separator, &mut cells) { let _ = tx.send(Err(error)); return }
                query.push_str(if rows == 0 { "{" } else { ",{" });
                let mut first = true;
                for (index, cell) in cells.iter().enumerate() {
                    if !first { query.push(',') }
                    first = false;
                    match keys.get(index) { Some(key) => query.push_str(key), None => query.push_str(&format!("\"column_{}\":", index + 1)) }
                    push_value(&mut query, cell, text, separator != b',');
                }
                query.push('}');
                rows += 1;
            }
            if (done && rows > 0) || query.len() >= BATCH_BYTES {
                query.push_str("] | end");
                let batch = Batch { query: std::mem::replace(&mut query, String::with_capacity(BATCH_BYTES + (1 << 16))), bytes_read: reader.bytes_read };
                if tx.send(Ok(batch)).is_err() { return }
                query.push_str(&prefix);
                rows = 0;
            }
            if done { return }
        }
    });
    let (mut imported, mut requests) = (0u64, 0u64);
    let result = (|| {
        for batch in rx {
            let batch = batch?;
            let statistics = execute(&batch.query)?;
            imported += statistics.get("inserted").and_then(Value::as_u64).unwrap_or(0) + statistics.get("replaced").and_then(Value::as_u64).unwrap_or(0);
            requests += 1;
            let rate = imported as f64 / started.elapsed().as_secs_f64().max(1e-3);
            progress(format!("import {}: {imported} rows ({}%, {rate:.0} rows/s)", args.collection, batch.bytes_read * 100 / total))?;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })();
    let _ = producer.join();
    result?;
    Ok(json!({"collection": args.collection, "imported": imported, "requests": requests, "seconds": (started.elapsed().as_secs_f64() * 1000.0).round() / 1000.0}))
}

/// Tab for `.tsv`/`.tab`, else the most frequent of `, ; \t |` in the header.
fn detect_separator(path: &str, header: &[u8]) -> u8 {
    let extension = Path::new(path).extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase();
    if extension == "tsv" || extension == "tab" { return b'\t' }
    b",;\t|".iter().copied().max_by_key(|s| (header.iter().filter(|b| **b == *s).count(), *s == b',')).unwrap_or(b',')
}

/// Header cells made unique and non-empty: `column_N` for a blank, `name_2` for a repeat.
fn field_names(cells: &[String]) -> Vec<String> {
    let mut names: Vec<String> = Vec::with_capacity(cells.len());
    for (index, cell) in cells.iter().enumerate() {
        let base = if cell.trim().is_empty() { format!("column_{}", index + 1) } else { cell.trim().to_owned() };
        let (mut name, mut n) = (base.clone(), 1);
        while names.contains(&name) { n += 1; name = format!("{base}_{n}") }
        names.push(name);
    }
    names
}

/// The JSON of one cell, typed (see the module comment). An integer that
/// does not fit the wire (±2⁵³) stays text rather than become a rounded float.
fn push_value(out: &mut String, cell: &str, text: bool, decimal_comma: bool) {
    use std::fmt::Write;
    if !text {
        if cell.is_empty() { return out.push_str("null") }
        if cell == "true" || cell == "false" { return out.push_str(cell) }
        let digits = cell.strip_prefix(['-', '+']).unwrap_or(cell);
        let leading_zero = digits.len() > 1 && digits.starts_with('0') && digits.as_bytes()[1].is_ascii_digit();
        if !leading_zero && digits.starts_with(|c: char| c.is_ascii_digit()) {
            if digits.bytes().all(|b| b.is_ascii_digit()) {
                if let Some(n) = cell.parse::<i64>().ok().filter(|n| n.abs() <= MAX_SAFE_INTEGER) { let _ = write!(out, "{n}"); return }
            } else {
                let comma = decimal_comma && cell.matches(',').count() == 1 && !cell.contains('.');
                let parsed = if comma { cell.replace(',', ".").parse::<f64>() } else { cell.parse::<f64>() };
                if let Some(n) = parsed.ok().and_then(serde_json::Number::from_f64) { let _ = write!(out, "{n}"); return }
            }
        }
    }
    push_string(out, cell);
}

/// `value` as a JSON string.
fn push_string(out: &mut String, value: &str) {
    use std::fmt::Write;
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => { let _ = write!(out, "\\u{:04x}", c as u32); }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn split_record(record: &[u8], separator: u8) -> Result<Vec<String>> {
    let mut cells = Vec::new();
    split_into(record, separator, &mut cells)?;
    Ok(cells)
}

/// The cells of one record: quotes removed, `""` read as `"`.
fn split_into(record: &[u8], separator: u8, cells: &mut Vec<String>) -> io::Result<()> {
    cells.clear();
    let record = record.strip_suffix(b"\n").unwrap_or(record);
    let record = record.strip_suffix(b"\r").unwrap_or(record);
    let (mut cell, mut quoted, mut i) = (Vec::new(), false, 0);
    while i <= record.len() {
        let byte = record.get(i).copied();
        match (quoted, byte) {
            (true, Some(b'"')) if record.get(i + 1) == Some(&b'"') => { cell.push(b'"'); i += 1 }
            (true, Some(b'"')) => quoted = false,
            (true, Some(b)) => cell.push(b),
            (true, None) => return Err(io::Error::new(io::ErrorKind::InvalidData, "unterminated quoted field")),
            (false, Some(b'"')) if cell.iter().all(u8::is_ascii_whitespace) => { cell.clear(); quoted = true }
            (false, Some(b)) if b == separator => cells.push(decode(std::mem::take(&mut cell))),
            (false, Some(b)) => cell.push(b),
            (false, None) => cells.push(decode(std::mem::take(&mut cell))),
        }
        i += 1;
    }
    Ok(())
}

/// UTF-8, or Latin-1 for a cell that is not.
fn decode(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).unwrap_or_else(|error| error.into_bytes().into_iter().map(char::from).collect())
}

/// Reads whole records: a line break inside quotes continues the record.
struct Reader<R> { input: R, bytes_read: u64, raw_header: Vec<u8>, first: bool }

impl<R: BufRead> Reader<R> {
    fn new(input: R) -> Self { Self { input, bytes_read: 0, raw_header: Vec::new(), first: true } }

    fn record(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut record = Vec::new();
        loop {
            let read = self.input.read_until(b'\n', &mut record)?;
            self.bytes_read += read as u64;
            if read == 0 { return Ok((!record.is_empty()).then_some(record)) }
            if record.iter().filter(|b| **b == b'"').count() % 2 == 0 { break }
        }
        if std::mem::take(&mut self.first) {
            if record.starts_with(b"\xEF\xBB\xBF") { record.drain(..3); }
            self.raw_header = record.clone();
        }
        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn typed(cell: &str, comma: bool) -> String { let mut s = String::new(); push_value(&mut s, cell, false, comma); s }
    #[test] fn types_cells() { assert_eq!(["12", "-3", "007", "1.5", "1e3", "true", "", "abc", "9007199254740993", "12,5"].map(|c| typed(c, false)), ["12", "-3", "\"007\"", "1.5", "1000.0", "true", "null", "\"abc\"", "\"9007199254740993\"", "\"12,5\""]); assert_eq!(typed("12,5", true), "12.5"); assert_eq!(typed("a\"b\n", false), r#""a\"b\n""#); }
    #[test] fn splits_quoted_records() { assert_eq!(split_record(b"a,\"b,\"\"c\"\"\",,d\r\n", b',').unwrap(), ["a", "b,\"c\"", "", "d"]); assert!(split_record(b"\"open", b',').is_err()); }
    #[test] fn reads_records_across_lines() { let mut r = Reader::new(&b"\xEF\xBB\xBFh1;h2\n\"x\ny\";2\n3;4"[..]); assert_eq!(r.record().unwrap().unwrap(), b"h1;h2\n"); assert_eq!(r.record().unwrap().unwrap(), b"\"x\ny\";2\n"); assert_eq!(r.record().unwrap().unwrap(), b"3;4"); assert!(r.record().unwrap().is_none()); }
    #[test] fn names_and_separators() { assert_eq!(field_names(&["a".into(), "".into(), "a".into()]), ["a", "column_2", "a_2"]); assert_eq!(detect_separator("x.csv", b"a;b;c,d"), b';'); assert_eq!(detect_separator("x.tsv", b"a,b"), b'\t'); assert_eq!(collection_name("2024-sales"), "t2024_sales"); }
    #[test] fn parses_args() { let a = parse_args("'my file.csv' sales --sep ;").unwrap(); assert_eq!((a.path.as_str(), a.collection.as_str(), a.separator), ("my file.csv", "sales", Some(b';'))); assert_eq!(parse_args("/tmp/ventes-2025.tsv").unwrap().collection, "ventes_2025"); assert!(parse_args("a.csv _system").is_err()); }
}
