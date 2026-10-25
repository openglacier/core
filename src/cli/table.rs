//! Query results drawn as a text table.
//!
//! Columns are the top-level fields in order of first appearance; numbers are
//! right-aligned, null is blank, objects and arrays are compact JSON, and a
//! cell wider than `MAX_CELL` characters is cut with `…`. Only the first
//! `MAX_ROWS` rows are kept; the footer says how many were left out.
#![cfg_attr(rustfmt, rustfmt_skip)]
use serde_json::{Map, Value};

pub(super) const MAX_ROWS: usize = 1_000;
const MAX_CELL: usize = 48;

/// Rows collected for one table; `total` counts the rows not kept too.
#[derive(Default)]
pub(super) struct Rows { rows: Vec<Value>, pub total: u64 }

impl Rows {
    pub fn push(&mut self, row: Value) {
        self.total += 1;
        if self.rows.len() < MAX_ROWS { self.rows.push(row) }
    }

    /// The table, then one footer line from the statistics and the elapsed time.
    pub fn render(&self, statistics: &Value, elapsed_ms: f64) -> String {
        let mut out = render_rows(&self.rows);
        let count = |name: &str| statistics.get(name).and_then(Value::as_u64).unwrap_or(0);
        let mut footer = format!("{} row{}", self.total, if self.total == 1 { "" } else { "s" });
        if self.total > self.rows.len() as u64 { footer += &format!(", first {} shown", self.rows.len()) }
        for name in ["inserted", "replaced", "deleted"] { if count(name) > 0 { footer += &format!(", {} {name}", count(name)) } }
        footer += &format!(" ({elapsed_ms:.1} ms)");
        if !out.is_empty() { out.push('\n') }
        out + &footer
    }
}

/// An operation result as a table when it is a list of objects, or an object
/// holding exactly one such list (`{"collections": [...]}`); `None` otherwise.
pub(super) fn render_value(value: &Value) -> Option<String> {
    let rows = match value {
        Value::Array(rows) => rows,
        Value::Object(map) => match map.values().filter(|v| v.is_array()).collect::<Vec<_>>()[..] { [Value::Array(rows)] => rows, _ => return None },
        _ => return None,
    };
    if !rows.iter().all(Value::is_object) { return None }
    let mut out = render_rows(&rows[..rows.len().min(MAX_ROWS)]);
    if !out.is_empty() { out.push('\n') }
    Some(out + &format!("{} row{}", rows.len(), if rows.len() == 1 { "" } else { "s" }))
}

/// Columns in the order ogd sends the fields (sorted by name).
fn render_rows(rows: &[Value]) -> String {
    let empty = Map::new();
    let objects: Vec<&Map<String, Value>> = rows.iter().map(|row| row.as_object().unwrap_or(&empty)).collect();
    let mut columns: Vec<&str> = Vec::new();
    for object in &objects { for key in object.keys() { if !columns.contains(&key.as_str()) { columns.push(key) } } }
    if columns.is_empty() {
        // Scalar rows (a `select` of one value without a name) still show up.
        return rows.iter().map(|row| cell(row).0).collect::<Vec<_>>().join("\n");
    }
    let cells: Vec<Vec<(String, bool)>> = objects.iter().map(|object| columns.iter().map(|c| object.get(*c).map_or((String::new(), false), cell)).collect()).collect();
    let widths: Vec<usize> = columns.iter().enumerate().map(|(i, c)| cells.iter().map(|row| width(&row[i].0)).chain([width(c)]).max().unwrap_or(0)).collect();
    let line = |parts: Vec<String>| parts.join(" │ ").trim_end().to_owned();
    let mut out = vec![line(columns.iter().zip(&widths).map(|(c, w)| pad(c, *w, false)).collect())];
    out.push(widths.iter().map(|w| "─".repeat(*w)).collect::<Vec<_>>().join("─┼─"));
    for row in &cells { out.push(line(row.iter().zip(&widths).map(|((text, right), w)| pad(text, *w, *right)).collect())) }
    out.join("\n")
}

/// The text of one cell, and whether it aligns right (a number).
fn cell(value: &Value) -> (String, bool) {
    let text = match value {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Number(n) => return (n.to_string(), true),
        other => other.to_string(),
    };
    let text: String = text.chars().map(|c| if c.is_control() { '·' } else { c }).collect();
    if width(&text) <= MAX_CELL { return (text, false) }
    (text.chars().take(MAX_CELL - 1).chain(['…']).collect(), false)
}

fn width(text: &str) -> usize { text.chars().count() }

fn pad(text: &str, width: usize, right: bool) -> String {
    let fill = " ".repeat(width.saturating_sub(self::width(text)));
    if right { fill + text } else { format!("{text}{fill}") }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test] fn draws_rows() { let mut rows = Rows::default(); rows.push(json!({"name": "Alice", "age": 31})); rows.push(json!({"name": "Bo", "tags": ["x"], "age": null})); assert_eq!(rows.render(&json!({}), 1.0), "age │ name  │ tags\n────┼───────┼──────\n 31 │ Alice │\n    │ Bo    │ [\"x\"]\n2 rows (1.0 ms)"); }
    #[test] fn tables_operation_lists() { assert_eq!(render_value(&json!({"collections": [{"name": "a"}]})).unwrap(), "name\n────\na\n1 row"); assert!(render_value(&json!({"ok": true})).is_none()); }
    #[test] fn cuts_long_cells() { assert_eq!(width(&cell(&json!("x".repeat(100))).0), MAX_CELL); }
}
