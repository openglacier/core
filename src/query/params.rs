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
    if let Some(inner) = split_explain(query) {
        return prepare_query(inner, parameters).map(|prepared| format!("explain {prepared}"));
    }
    let mut variables = parameters.clone();
    let mut rest = query.trim_start();
    while let Some(header) = strip_keyword(rest, "let") {
        let (declarations, remaining) = split_first_stage(header)?;
        for declaration in split_top_level(declarations, ',').map_err(|message| format!("invalid let: {message}"))? {
            let (name, value) = evaluate_declaration(declaration, &variables)?;
            if variables.contains_key(&name) {
                return Err(format!("variable ${name} is already bound"));
            }
            variables.insert(name, value);
        }
        rest = remaining.trim_start();
    }
    bind_parameters(rest, &variables)
}

/// Strips a leading `keyword` followed by whitespace.
fn strip_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let rest = text.strip_prefix(keyword)?;
    rest.starts_with(char::is_whitespace).then_some(rest)
}

/// Splits a `let` header from the rest of the query at its first top-level `|`.
fn split_first_stage(text: &str) -> Result<(&str, &str), String> {
    let mut state = ScanState::default();
    let mut characters = text.char_indices().peekable();
    while let Some((index, character)) = characters.next() {
        state.consume(character).map_err(|message| format!("invalid let: {message}"))?;
        if character == '|' && state.is_top_level() {
            if characters.peek().is_some_and(|(_, next)| *next == '|') {
                characters.next();
                continue;
            }
            return Ok((&text[..index], &text[index + 1..]));
        }
    }
    Err("a `let` header must be followed by `|` and the query".to_owned())
}

/// Evaluates `$name = <expression>` against the variables bound so far.
fn evaluate_declaration(declaration: &str, variables: &Map<String, JsonValue>) -> Result<(String, JsonValue), String> {
    let syntax = || format!("invalid let {declaration:?}: expected `$name = <expression>`");
    let (name, expression) = split_assignment(declaration).map_err(|_| syntax())?;
    let name = name.trim().strip_prefix('$').ok_or_else(syntax)?;
    let mut characters = name.chars();
    if !characters.next().is_some_and(|first| first.is_alphabetic() || first == '_')
        || !characters.all(|character| character.is_alphanumeric() || character == '_')
    {
        return Err(syntax());
    }
    let context = |message: String| format!("let ${name}: {message}");
    let bound = bind_parameters(expression, variables).map_err(context)?;
    let parsed = super::parse_expression(&bound).map_err(|error| context(error.to_string()))?;
    let mut field = None;
    parsed.for_each_field(&mut |path| { field.get_or_insert_with(|| path.to_string()); });
    if let Some(field) = field {
        return Err(context(format!("reads the field {field:?}; a `let` value cannot depend on documents")));
    }
    let value = super::evaluate_document(&parsed, &crate::Document::new()).map_err(|error| context(error.to_string()))?;
    Ok((name.to_owned(), crate::helpers::value_to_json(&value)))
}

/// Replaces every `$name` outside string literals with the literal form of
/// `parameters[name]`.
///
/// # Errors
///
/// Returns an error naming the parameter when a referenced name is not bound,
/// or when a string value holds a control character the language cannot escape.
pub fn bind_parameters(query: &str, parameters: &Map<String, JsonValue>) -> Result<String, String> {
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
                let value = parameters.get(name).ok_or_else(|| format!("variable ${name} is not bound; declare it with `let ${name} = …` or pass it in params"))?;
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
    #[test] fn splits_the_explain_prefix() { assert_eq!(split_explain("explain on users | limit 1"), Some("on users | limit 1")); assert_eq!(split_explain("  explain\non users"), Some("on users")); assert_eq!(split_explain("on explain"), None); assert_eq!(split_explain("explainer"), None); }
}
