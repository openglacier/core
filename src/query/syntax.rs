#![cfg_attr(rustfmt, rustfmt_skip)]

use super::SortDirection;

pub(super) fn parse_sort_item(text: &str) -> Result<(&str, SortDirection), String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("sort key must not be empty".to_owned());
    }

    let mut words = text.split_whitespace().collect::<Vec<_>>();
    let direction = match words.last().copied() {
        Some("asc") => {
            words.pop();
            SortDirection::Ascending
        }
        Some("desc") => {
            words.pop();
            SortDirection::Descending
        }
        Some(last) if last.eq_ignore_ascii_case("asc") || last.eq_ignore_ascii_case("desc") => {
            return Err("sort direction must be lowercase `asc` or `desc`".to_owned());
        }
        _ => SortDirection::Ascending,
    };

    if words.len() != 1 {
        return Err("expected a field path optionally followed by `asc` or `desc`".to_owned());
    }

    Ok((&text[..words[0].len()], direction))
}

pub(super) fn split_top_level(text: &str, separator: char) -> Result<Vec<&str>, String> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut state = ScanState::default();

    for (index, character) in text.char_indices() {
        state.consume(character)?;
        if character == separator && state.is_top_level() {
            let part = text[start..index].trim();
            if part.is_empty() {
                return Err("empty item between separators".to_owned());
            }
            parts.push(part);
            start = index + character.len_utf8();
        }
    }

    state.finish()?;
    let final_part = text[start..].trim();
    if final_part.is_empty() {
        return Err("trailing separator creates an empty item".to_owned());
    }
    parts.push(final_part);
    Ok(parts)
}

pub(super) fn validate_balanced_text(text: &str) -> Result<(), String> {
    let mut state = ScanState::default();
    for character in text.chars() {
        state.consume(character)?;
    }
    state.finish()
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct ScanState {
    parentheses: usize,
    brackets: usize,
    braces: usize,
    quote: Option<char>,
    escaped: bool,
}

impl ScanState {
    pub(super) fn consume(&mut self, character: char) -> Result<(), String> {
        if let Some(quote) = self.quote {
            if self.escaped {
                self.escaped = false;
                return Ok(());
            }
            if character == '\\' {
                self.escaped = true;
            } else if character == quote {
                self.quote = None;
            }
            return Ok(());
        }

        match character {
            '\'' | '"' => self.quote = Some(character),
            '(' => self.parentheses += 1,
            ')' => decrement(&mut self.parentheses, "unexpected `)`")?,
            '[' => self.brackets += 1,
            ']' => decrement(&mut self.brackets, "unexpected `]`")?,
            '{' => self.braces += 1,
            '}' => decrement(&mut self.braces, "unexpected `}`")?,
            _ => {}
        }
        Ok(())
    }

    pub(super) const fn is_top_level(&self) -> bool {
        self.quote.is_none() && self.parentheses == 0 && self.brackets == 0 && self.braces == 0
    }

    pub(super) fn finish(self) -> Result<(), String> {
        if self.quote.is_some() {
            return Err("unterminated quoted string".to_owned());
        }
        if self.parentheses != 0 {
            return Err("unclosed parenthesis".to_owned());
        }
        if self.brackets != 0 {
            return Err("unclosed bracket".to_owned());
        }
        if self.braces != 0 {
            return Err("unclosed brace".to_owned());
        }
        Ok(())
    }
}

fn decrement(value: &mut usize, message: &'static str) -> Result<(), String> {
    if *value == 0 {
        return Err(message.to_owned());
    }
    *value -= 1;
    Ok(())
}

/// Decodes the content of a double-quoted string literal (without the quotes).
///
/// Escapes are those of JSON: `\" \\ \/ \b \f \n \r \t` and `\uXXXX`, where a
/// UTF-16 surrogate pair (`\ud83d\ude00`) forms one character. On error,
/// returns the offending escape character.
pub(super) fn unescape_string(inner: &str) -> Result<String, char> {
    let mut decoded = String::with_capacity(inner.len());
    let mut characters = inner.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            decoded.push(character);
            continue;
        }
        let escaped = characters.next().ok_or('\\')?;
        decoded.push(match escaped {
            '"' | '\\' | '/' => escaped,
            'b' => '\u{0008}',
            'f' => '\u{000c}',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'u' => {
                let high = hex_unit(&mut characters)?;
                let code = if (0xD800..0xDC00).contains(&high) {
                    if characters.next() != Some('\\') || characters.next() != Some('u') {
                        return Err('u');
                    }
                    let low = hex_unit(&mut characters)?;
                    if !(0xDC00..0xE000).contains(&low) {
                        return Err('u');
                    }
                    0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00)
                } else {
                    high
                };
                char::from_u32(code).ok_or('u')?
            }
            other => return Err(other),
        });
    }
    Ok(decoded)
}

/// Reads the four hex digits of a `\u` escape.
fn hex_unit(characters: &mut std::str::Chars<'_>) -> Result<u32, char> {
    let hex = characters.take(4).collect::<String>();
    if hex.len() != 4 || !hex.chars().all(|digit| digit.is_ascii_hexdigit()) {
        return Err('u');
    }
    u32::from_str_radix(&hex, 16).map_err(|_| 'u')
}

/// Splits `field = expression` at the first top-level `=` that is not part
/// of `==`, `!=`, `<=` or `>=`.
pub(super) fn split_assignment(text: &str) -> Result<(&str, &str), String> {
    let mut state = ScanState::default();

    for (index, character) in text.char_indices() {
        state.consume(character)?;

        if character == '=' && state.is_top_level() {
            let previous = text[..index].chars().next_back();
            let next = text[index + character.len_utf8()..].chars().next();

            if matches!(previous, Some('=' | '!' | '<' | '>')) || next == Some('=') {
                continue;
            }

            let field = text[..index].trim();
            let expression = text[index + 1..].trim();

            if field.is_empty() {
                return Err("assignment field must not be empty".to_owned());
            }

            if expression.is_empty() {
                return Err("assignment expression must not be empty".to_owned());
            }

            return Ok((field, expression));
        }
    }

    state.finish()?;
    Err("assignment must contain a top-level `=`".to_owned())
}

/// Splits `text` around the first whitespace-delimited `word` found at the top
/// level (outside strings and brackets). Returns the trimmed parts.
pub(super) fn split_top_level_word<'a>(text: &'a str, word: &str) -> Option<(&'a str, &'a str)> {
    let mut state = ScanState::default();
    let mut previous_is_space = true;
    for (index, character) in text.char_indices() {
        if state.consume(character).is_err() {
            return None;
        }
        if previous_is_space && state.is_top_level() && text[index..].starts_with(word) {
            let end = index + word.len();
            if text[end..].chars().next().is_none_or(char::is_whitespace) {
                return Some((text[..index].trim(), text[end..].trim()));
            }
        }
        previous_is_space = character.is_whitespace();
    }
    None
}

/// Splits a trailing `asc` / `desc` from a sort item whose key may be any expression.
pub(super) fn split_sort_direction(text: &str) -> (&str, SortDirection) {
    let text = text.trim();
    for (suffix, direction) in [("asc", SortDirection::Ascending), ("desc", SortDirection::Descending)] {
        if let Some(key) = text.strip_suffix(suffix) {
            if key.ends_with(char::is_whitespace) {
                return (key.trim_end(), direction);
            }
        }
    }
    (text, SortDirection::Ascending)
}

/// Options of `unwind <field> [with index <field>] [keep empty]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct UnwindSpec<'a> {
    pub(super) field: &'a str,
    /// Field receiving the 0-based position of each item.
    pub(super) index: Option<&'a str>,
    /// Keep rows whose array is empty or null (the field becomes `null`).
    pub(super) keep_empty: bool,
}

impl<'a> UnwindSpec<'a> {
    pub(super) fn parse(text: &'a str) -> Result<Self, String> {
        let syntax = "expected `unwind <field> [with index <field>] [keep empty]`";
        let mut words = text.split_whitespace();
        let field = words.next().ok_or_else(|| syntax.to_owned())?;
        let mut spec = Self { field, index: None, keep_empty: false };
        while let Some(word) = words.next() {
            match (word, words.next()) {
                ("with", Some("index")) if spec.index.is_none() => {
                    spec.index = Some(words.next().ok_or_else(|| syntax.to_owned())?);
                }
                ("keep", Some("empty")) if !spec.keep_empty => spec.keep_empty = true,
                _ => return Err(syntax.to_owned()),
            }
        }
        Ok(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn splits_on_top_level_words_only() { assert_eq!(split_top_level_word("age by [0, 18]", "by"), Some(("age", "[0, 18]"))); assert_eq!(split_top_level_word("f(\"a by b\") by [1]", "by"), Some(("f(\"a by b\")", "[1]"))); assert_eq!(split_top_level_word("baby by x", "by"), Some(("baby", "x"))); assert_eq!(split_top_level_word("nearby", "by"), None); }
    #[test] fn sort_direction_is_a_trailing_word() { assert_eq!(split_sort_direction("price * qty desc"), ("price * qty", SortDirection::Descending)); assert_eq!(split_sort_direction("name"), ("name", SortDirection::Ascending)); assert_eq!(split_sort_direction("misc"), ("misc", SortDirection::Ascending)); }
    #[test] fn unescapes_json_escapes() { assert_eq!(unescape_string(r#"a\"\\\/\n\t"#).unwrap(), "a\"\\/\n\t"); assert_eq!(unescape_string(r"\u0041\u00e9\u0001").unwrap(), "Aé\u{1}"); assert_eq!(unescape_string(r"\ud83d\ude00").unwrap(), "😀"); for bad in [r"\q", r"\u+123", r"\u12", r"\u12zz", r"\ud83d", r"\ud83dx", r"\ude00", "\\"] { assert!(unescape_string(bad).is_err(), "{bad}"); } }
    #[test] fn parses_unwind_options() { assert_eq!(UnwindSpec::parse("tags").unwrap(), UnwindSpec { field: "tags", index: None, keep_empty: false }); assert_eq!(UnwindSpec::parse("tags keep empty with index i").unwrap(), UnwindSpec { field: "tags", index: Some("i"), keep_empty: true }); for bad in ["", "tags with", "tags keep", "tags keep empty keep empty", "tags other"] { assert!(UnwindSpec::parse(bad).is_err(), "{bad}"); } }
}
