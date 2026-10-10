#![cfg_attr(rustfmt, rustfmt_skip)]
//! Query variables (`let`, `$name`), request parameters and the `explain` prefix.
//!
//! Variables are bound before the query is parsed: each `$name` outside a
//! string literal is replaced by the literal spelling of its value. Values are
//! always written as literals (strings are quoted and escaped), so a value can
//! never inject syntax: `$name` bound to `"x\" | delete"` stays one string.
//!
//! A variable comes from the request parameters or from a leading
//! `let $name = <expression>, …` header, evaluated once per query.

use serde_json::{Map, Value as JsonValue};

use super::syntax::{split_assignment, split_top_level, ScanState};

/// Prepares a query text for parsing: evaluates its leading `let` headers,
/// then binds every `$name` from those variables and from `parameters`.
///
/// ```text
/// let $threshold = 100, $since = parse_date("2026-01-01")
/// | let $double = $threshold * 2
/// | on orders | where price * qty > $threshold and date >= $since
/// ```
///
/// Each `let` expression is evaluated once, before the query runs, and may use
/// request parameters and earlier variables but no document field. An
/// `explain` prefix is kept in front of the prepared query.
///
/// # Errors
///
/// Returns an error for a malformed `let`, a variable declared twice (or
/// shadowing a parameter), a `let` expression that fails or reads a field,
/// and an unbound `$name`.
pub fn prepare_query(query: &str, parameters: &Map<String, JsonValue>) -> Result<String, String> {
    prepare_view(query, parameters, None, Vec::new(), false).map(|(prepared, _)| prepared)
}

/// What a query reads from its Place while it is prepared: the body of a
/// view by name and optional version (`on view`), and the rows of a query
/// (`let $x = (on …)`).
#[derive(Clone, Copy)]
pub struct Sources<'a> {
    pub view: &'a dyn Fn(&str, Option<i64>) -> Result<String, String>,
    pub rows: &'a dyn Fn(&str) -> Result<Vec<JsonValue>, String>,
}

impl std::fmt::Debug for Sources<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Sources")
    }
}

/// Deepest chain of views reading views.
const VIEW_DEPTH: usize = 8;

/// [`prepare_query`], reading each `on view` source and each `let` query from
/// `views`. The view
/// body is prepared with the variables of the query that reads it (its own
/// `let` stay local) and replaces the source; `trail` holds the views being
/// read, to refuse cycles. When `symbolic`, each `$name` left unbound is
/// listed (in order) and reads as `1`, and so does a `let` value that
/// cannot be computed without them: a view body is checked this way.
pub fn prepare_view(query: &str, parameters: &Map<String, JsonValue>, views: Option<Sources<'_>>, mut trail: Vec<String>, symbolic: bool) -> Result<(String, Vec<String>), String> {
    let mut free = Vec::new();
    let prepared = prepare(query, parameters, symbolic.then_some(&mut free), views, &mut trail)?;
    let mut parameters = Vec::new();
    for name in free {
        if !parameters.contains(&name) {
            parameters.push(name);
        }
    }
    Ok((prepared, parameters))
}

fn prepare(query: &str, parameters: &Map<String, JsonValue>, mut free: Option<&mut Vec<String>>, views: Option<Sources<'_>>, trail: &mut Vec<String>) -> Result<String, String> {
    if let Some(inner) = split_explain(query) {
        return prepare(inner, parameters, free, views, trail).map(|prepared| format!("explain {prepared}"));
    }
    // A view statement keeps its body as written: it is bound when read.
    if crate::view::Statement::is_statement(query) {
        return Ok(query.to_owned());
    }
    let mut variables = parameters.clone();
    let mut declared = Vec::new();
    let mut rest = query.trim_start();
    while let Some(header) = strip_keyword(rest, "let") {
        let (declarations, remaining) = split_first_stage(header)?;
        for declaration in split_top_level(declarations, ',').map_err(|message| format!("invalid let: {message}"))? {
            // `$x ?= …` keeps a value already bound (a parameter, or a
            // variable of the query reading the view).
            let Some((name, value)) = evaluate_declaration(declaration, &variables, free.as_deref_mut(), views, trail)? else { continue };
            // A view's own variables may shadow those of the query reading it.
            if declared.contains(&name) || (trail.is_empty() && variables.contains_key(&name)) {
                return Err(format!("variable ${name} is already bound"));
            }
            declared.push(name.clone());
            variables.insert(name, value);
        }
        rest = remaining.trim_start();
    }
    if !rest.contains("view") {
        return bind(rest, &variables, free);
    }
    let mut expanded = String::with_capacity(rest.len());
    let mut copied = 0;
    for range in stage_ranges(rest)? {
        let Some((lookup, name, version, alias)) = view_source(rest[range.clone()].trim())? else { continue };
        let views = views.ok_or("`on view` is read through query.execute")?;
        if trail.iter().any(|known| known == name) || trail.len() >= VIEW_DEPTH {
            return Err(format!("view {name} cannot be read here: views read each other in a cycle or deeper than {VIEW_DEPTH}"));
        }
        trail.push(name.to_owned());
        let body = prepare(&(views.view)(name, version)?, &variables, free.as_deref_mut(), Some(views), trail).map_err(|message| format!("view {name}: {message}"))?;
        trail.pop();
        expanded.push_str(&bind(&rest[copied..range.start], &variables, free.as_deref_mut())?);
        // The view's source keeps its stages: `as <alias>` names it, a
        // lookup reads its collection through them.
        let source_end = stage_ranges(&body)?[0].end;
        let (source, stages) = (body[..source_end].trim(), &body[source_end..]);
        if lookup {
            let collection = source.strip_prefix("on ").or_else(|| source.strip_prefix("from ")).map(str::trim).filter(|collection| collection.starts_with("values") || !collection.contains(char::is_whitespace))
                .ok_or_else(|| format!("view {name} cannot be read by lookup: its source must be a bare `on <collection>` or `on values […]`"))?;
            expanded.push_str(&format!(" lookup {collection} {alias} {stages} "));
        } else if alias.is_empty() || !source.split_whitespace().any(|word| word == "as") {
            expanded.push_str(&format!(" {source} {alias} {stages} "));
        } else {
            return Err(format!("view {name} already names its source; read it without `{alias}`"));
        }
        copied = range.end;
    }
    expanded.push_str(&bind(&rest[copied..], &variables, free)?);
    Ok(expanded)
}

/// `on view <name> [version <n>] [as <alias>]` (`from` too), or `lookup view
/// <name> [version <n>] <rest>`: `(lookup, name, version, alias or rest)`.
fn view_source(stage: &str) -> Result<Option<(bool, &str, Option<i64>, String)>, String> {
    let mut words = stage.split_whitespace();
    let lookup = match (words.next(), words.next()) {
        (Some("on" | "from"), Some("view")) => false,
        (Some("lookup"), Some("view")) => true,
        _ => return Ok(None),
    };
    let Some(name) = words.next().filter(|name| crate::view::is_name(name)) else { return Ok(None) };
    let mut rest = words.collect::<Vec<_>>();
    let version = match rest.as_slice() {
        ["version", version, ..] => Some(version.parse::<i64>().ok().filter(|version| *version > 0).ok_or_else(|| format!("view {name}: version takes a positive integer"))?),
        _ => None,
    };
    rest.drain(..if version.is_some() { 2 } else { 0 });
    match rest.as_slice() {
        [] | ["as", _] => {}
        _ if lookup => {}
        _ => return Err(format!("view {name}: a view source takes only `version <n>` and `as <alias>`, not `{}`", rest.join(" "))),
    }
    Ok(Some((lookup, name, version, rest.join(" "))))
}

/// The length of the bracketed group `text` starts with (`[…]`, `{…}`,
/// `(…)`), strings and nesting included.
pub(crate) fn group_length(text: &str) -> Result<usize, String> {
    let mut state = ScanState::default();
    for (index, character) in text.char_indices() {
        state.consume(character)?;
        if state.is_top_level() {
            return Ok(index + character.len_utf8());
        }
    }
    Err("unclosed bracket".to_owned())
}

/// The query of a `let` value written `(on …)` or `(from …)`.
fn query_expression(expression: &str) -> Option<&str> {
    let inner = expression.trim().strip_prefix('(')?.strip_suffix(')')?.trim();
    (strip_keyword(inner, "on").is_some() || strip_keyword(inner, "from").is_some()).then_some(inner)
}

/// True when `query` reads its Place while it is prepared (`on view`,
/// `lookup view`, `let $x = (on …)`): it is then prepared where the Place
/// can be read, with its parameters.
#[must_use]
pub fn reads_place(query: &str) -> bool {
    let query = split_explain(query).unwrap_or(query);
    !crate::view::Statement::is_statement(query) && (query.contains("(on ") || query.contains("(from ")
        || (query.contains("view") && stage_ranges(query).is_ok_and(|ranges| ranges.into_iter().any(|range| !matches!(view_source(query[range].trim()), Ok(None))))))
}

/// Strips a leading `keyword` followed by whitespace.
fn strip_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let rest = text.strip_prefix(keyword)?;
    rest.starts_with(char::is_whitespace).then_some(rest)
}

/// Splits a `let` header from the rest of the query at its first top-level `|`.
fn split_first_stage(text: &str) -> Result<(&str, &str), String> {
    let ranges = stage_ranges(text).map_err(|message| format!("invalid let: {message}"))?;
    match ranges.get(1) {
        Some(next) => Ok((&text[ranges[0].clone()], &text[next.start..])),
        None => Err("a `let` header must be followed by `|` and the query".to_owned()),
    }
}

/// The byte ranges of the top-level stages of `text`, between the `|` that
/// separate them (`||` is the `or` operator, not a separator).
pub(crate) fn stage_ranges(text: &str) -> Result<Vec<std::ops::Range<usize>>, String> {
    let (mut ranges, mut start, mut state) = (Vec::new(), 0, ScanState::default());
    let mut characters = text.char_indices().peekable();
    while let Some((index, character)) = characters.next() {
        state.consume(character)?;
        if character == '|' && state.is_top_level() {
            if characters.peek().is_some_and(|(_, next)| *next == '|') {
                characters.next();
                continue;
            }
            ranges.push(start..index);
            start = index + 1;
        }
    }
    ranges.push(start..text.len());
    Ok(ranges)
}

/// Evaluates `$name = <expression>` against the variables bound so far.
fn evaluate_declaration(declaration: &str, variables: &Map<String, JsonValue>, mut free: Option<&mut Vec<String>>, views: Option<Sources<'_>>, trail: &mut Vec<String>) -> Result<Option<(String, JsonValue)>, String> {
    let syntax = || format!("invalid let {declaration:?}: expected `$name = <expression>` or `$name ?= <expression>`");
    let (name, expression) = split_assignment(declaration).map_err(|_| syntax())?;
    let (name, optional) = name.trim().strip_suffix('?').map_or((name.trim(), false), |name| (name.trim_end(), true));
    let name = name.strip_prefix('$').ok_or_else(syntax)?;
    if optional && variables.contains_key(name) {
        return Ok(None);
    }
    let mut characters = name.chars();
    if !characters.next().is_some_and(|first| first.is_alphabetic() || first == '_')
        || !characters.all(|character| character.is_alphanumeric() || character == '_')
    {
        return Err(syntax());
    }
    let context = |message: String| format!("let ${name}: {message}");
    // `(on …)`: the rows of a query, read in the Place (what `rows` gives
    // when a view body is checked: no rows, and the collections it reads).
    if let Some(query) = query_expression(expression) {
        let symbolic = free.is_some();
        let prepared = prepare(query, variables, free, views, trail).map_err(context)?;
        let rows = match views {
            Some(views) => (views.rows)(&prepared).map_err(context)?,
            None if symbolic => Vec::new(),
            None => return Err(context("a query in `let` is read through query.execute".to_owned())),
        };
        return Ok(Some((name.to_owned(), JsonValue::Array(rows))));
    }
    let unbound = free.as_ref().map(|free| free.len());
    let bound = bind(expression, variables, free.as_deref_mut()).map_err(context)?;
    let parsed = super::parse_expression(&bound).map_err(|error| context(error.to_string()))?;
    let mut field = None;
    parsed.for_each_field(&mut |path| { field.get_or_insert_with(|| path.to_string()); });
    if let Some(field) = field {
        return Err(context(format!("reads the field {field:?}; a `let` value cannot depend on documents")));
    }
    match super::evaluate_document(&parsed, &crate::Document::new()) {
        Ok(value) => Ok(Some((name.to_owned(), crate::helpers::value_to_json(&value)))),
        Err(_) if free.is_some_and(|free| Some(free.len()) != unbound) => Ok(Some((name.to_owned(), JsonValue::from(1)))),
        Err(error) => Err(context(error.to_string())),
    }
}

/// Replaces every `$name` outside string literals with the literal form of
/// `parameters[name]`.
///
/// # Errors
///
/// Returns an error naming the parameter when a referenced name is not bound,
/// or when a string value holds a control character the language cannot escape.
pub fn bind_parameters(query: &str, parameters: &Map<String, JsonValue>) -> Result<String, String> {
    bind(query, parameters, None)
}

/// [`bind_parameters`]; with `free`, an unbound name is listed there (at
/// each use) and reads as `1`.
fn bind(query: &str, parameters: &Map<String, JsonValue>, mut free: Option<&mut Vec<String>>) -> Result<String, String> {
    let mut output = String::with_capacity(query.len());
    let mut characters = query.char_indices().peekable();
    let mut in_string = false;
    while let Some((index, character)) = characters.next() {
        if in_string {
            output.push(character);
            match character {
                '\\' => {
                    if let Some((_, escaped)) = characters.next() {
                        output.push(escaped);
                    }
                }
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match character {
            '"' => {
                in_string = true;
                output.push(character);
            }
            '$' if characters.peek().is_some_and(|(_, next)| next.is_alphabetic() || *next == '_') => {
                let start = index + 1;
                let mut end = start;
                while let Some((position, next)) = characters.peek().copied() {
                    if !(next.is_alphanumeric() || next == '_') {
                        break;
                    }
                    end = position + next.len_utf8();
                    characters.next();
                }
                let name = &query[start..end];
                let value = match (parameters.get(name), free.as_deref_mut()) {
                    (Some(value), _) => value,
                    // Listed at each use, so a `let` knows it read one.
                    (None, Some(free)) => {
                        free.push(name.to_owned());
                        &JsonValue::Number(1.into())
                    }
                    (None, None) => return Err(format!("variable ${name} is not bound; declare it with `let ${name} = …` or pass it in params")),
                };
                write_literal(&mut output, value).map_err(|message| format!("query parameter ${name}: {message}"))?;
            }
            _ => output.push(character),
        }
    }
    Ok(output)
}

fn write_literal(output: &mut String, value: &JsonValue) -> Result<(), String> {
    match value {
        JsonValue::Null => output.push_str("null"),
        JsonValue::Bool(flag) => output.push_str(if *flag { "true" } else { "false" }),
        JsonValue::Number(number) => output.push_str(&number.to_string()),
        JsonValue::String(text) => write_string(output, text)?,
        JsonValue::Array(items) => {
            output.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                write_literal(output, item)?;
            }
            output.push(']');
        }
        JsonValue::Object(fields) => {
            output.push('{');
            for (index, (key, item)) in fields.iter().enumerate() {
                if index > 0 {
                    output.push_str(", ");
                }
                write_string(output, key)?;
                output.push_str(": ");
                write_literal(output, item)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

/// Writes a string literal using only the escapes the query lexers accept.
fn write_string(output: &mut String, text: &str) -> Result<(), String> {
    output.push('"');
    for character in text.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{0008}' => output.push_str("\\b"),
            '\u{000c}' => output.push_str("\\f"),
            control if (control as u32) < 0x20 => output.push_str(&format!("\\u{:04x}", control as u32)),
            other => output.push(other),
        }
    }
    output.push('"');
    Ok(())
}

/// Splits an optional leading `explain` keyword from a query.
///
/// Returns `Some(query)` without the keyword when the text starts with
/// `explain` followed by whitespace, `None` otherwise.
#[must_use]
pub fn split_explain(query: &str) -> Option<&str> {
    let rest = query.trim_start().strip_prefix("explain")?;
    rest.starts_with(char::is_whitespace).then(|| rest.trim_start())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn bind(query: &str, parameters: JsonValue) -> Result<String, String> {
        bind_parameters(query, parameters.as_object().unwrap())
    }

    #[test] fn binds_every_json_kind_as_a_literal() { let bound = bind(r#"on users | where age >= $min and name in $names and active == $on and nick == $nick | limit $n"#, json!({"min": 18, "names": ["Ann", "B\"o"], "on": true, "nick": null, "n": 5})).unwrap(); assert_eq!(bound, r#"on users | where age >= 18 and name in ["Ann", "B\"o"] and active == true and nick == null | limit 5"#); assert_eq!(bind("on t | insert $doc", json!({"doc": {"a": 1.5, "b": {"c": "x"}}})).unwrap(), r#"on t | insert {"a": 1.5, "b": {"c": "x"}}"#); }
    #[test] fn string_values_cannot_inject_syntax() { let bound = bind("on users | where name == $name", json!({"name": "x\" | delete | where \"a"})).unwrap(); assert_eq!(bound, r#"on users | where name == "x\" | delete | where \"a""#); let ast = crate::query::parse(&bound).unwrap(); assert_eq!(ast.stage_count(), 1); }
    #[test] fn leaves_string_contents_and_lone_dollars_alone() { assert_eq!(bind(r#"on t | where a == "$x \" $y" | where b == $x"#, json!({"x": 1})).unwrap(), r#"on t | where a == "$x \" $y" | where b == 1"#); assert_eq!(bind("on t | where a == $ 1", json!({})).unwrap(), "on t | where a == $ 1"); }
    #[test] fn rejects_unbound_names_and_unescapable_strings() { assert!(bind("on t | where a == $missing", json!({})).unwrap_err().contains("$missing")); assert_eq!(bind("on t | where a == $x", json!({"x": "a\u{0001}"})).unwrap(), r#"on t | where a == "a\u0001""#); }
    fn prepare(query: &str, parameters: JsonValue) -> Result<String, String> {
        prepare_query(query, parameters.as_object().unwrap())
    }

    #[test] fn let_binds_evaluated_variables() { assert_eq!(prepare("let $a = 2 * 3, $b = concat(\"x\", $a) | on t | where n > $a and s == $b", json!({})).unwrap(), r#"on t | where n > 6 and s == "x6""#); assert_eq!(prepare("let $a = 1 | let $b = $a + 1 | on t | limit $b", json!({})).unwrap(), "on t | limit 2"); assert_eq!(prepare("let $d = parse_date(\"1970-01-02\"), $l = [1, 2] | on t | where at >= $d and x in $l", json!({})).unwrap(), "on t | where at >= 86400000 and x in [1, 2]"); }
    #[test] fn let_combines_with_parameters_and_explain() { assert_eq!(prepare("let $max = $min * 2 | on t | where n between $min and $max", json!({"min": 5})).unwrap(), "on t | where n between 5 and 10"); assert_eq!(prepare("explain let $n = 3 | on t | limit $n", json!({})).unwrap(), "explain on t | limit 3"); assert_eq!(prepare("let $s = \"a|b\" | on t | where s == $s || false", json!({})).unwrap(), r#"on t | where s == "a|b" || false"#); }
    #[test] fn let_rejects_invalid_declarations() { for (query, fragment) in [ ("let $a = 1, $a = 2 | on t", "already bound"), ("let $min = 1 | on t", "already bound"), ("let $a = age | on t", "cannot depend on documents"), ("let $a = 1 / 0 | on t", "division by zero"), ("let a = 1 | on t", "expected `$name"), ("let $a == 1 | on t", "expected `$name"), ("let $a = 1", "followed by `|`"), ("let $a = 1 | on t | where x == $b", "$b is not bound") ] { let error = prepare(query, json!({"min": 0})).unwrap_err(); assert!(error.contains(fragment), "{query}: {error}"); } assert_eq!(prepare("on letters | limit 1", json!({})).unwrap(), "on letters | limit 1"); }
    fn prepare_symbolic(query: &str) -> Result<(String, Vec<String>), String> { prepare_view(query, &Map::new(), None, Vec::new(), true) }
    #[test] fn symbolic_preparation_lists_free_variables_and_skips_view_statements() { assert_eq!(prepare_symbolic("let $from = parse_date($day), $n = 2 | on t | where at >= $from and k == $key and x == $day | limit $n").unwrap(), ("on t | where at >= 1 and k == 1 and x == 1 | limit 2".to_owned(), vec!["day".to_owned(), "key".to_owned()])); assert!(prepare_symbolic("let $a = 1 / 0 | on t").unwrap_err().contains("division by zero")); let view = "define view v | exec | on t | where a == $a | end | end"; assert_eq!(prepare(view, json!({})).unwrap(), view); assert_eq!(prepare(&format!("explain {view}"), json!({})).unwrap(), format!("explain {view}")); }
    #[test] fn optional_let_keeps_a_bound_value() { assert_eq!(prepare("let $n ?= 2 | on t | limit $n", json!({})).unwrap(), "on t | limit 2"); assert_eq!(prepare("let $n ?= 2 | on t | limit $n", json!({"n": 5})).unwrap(), "on t | limit 5"); assert_eq!(prepare_symbolic("let $g ?= \"ytd\", $m = $min | on t | where g == $g and m == $m").unwrap(), ("on t | where g == \"ytd\" and m == 1".to_owned(), vec!["min".to_owned()])); assert_eq!(prepare("let $n = 1, $n ?= 2 | on t | limit $n", json!({})).unwrap(), "on t | limit 1"); }
    #[test] fn splits_the_explain_prefix() { assert_eq!(split_explain("explain on users | limit 1"), Some("on users | limit 1")); assert_eq!(split_explain("  explain\non users"), Some("on users")); assert_eq!(split_explain("on explain"), None); assert_eq!(split_explain("explainer"), None); }
}
