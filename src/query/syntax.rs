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
