#![cfg_attr(rustfmt, rustfmt_skip)]
//! Stage separators: `|`, or `;` and line breaks.
//!
//! A query holding a top-level `|` (not `||`) separates its stages with `|`
//! only, and its line breaks are spaces, as always. Any other query separates
//! them with `;` and with line breaks. A line break ends a stage when
//! brackets are balanced, the line does not end on a continuation (`,`, an
//! operator, `and`, `as`…) and the next line starts with a stage word, or with
//! a directive of the block it is in (`by`, `sum`… in `group`). Text between
//! parentheses keeps its own style: it is read on its own (a `let` query).
//!
//! [`normalize`] writes each separator as `|`, one byte for one byte, so the
//! spans of the text it returns are those of the text it was given.

use std::borrow::Cow;
use std::ops::Range;

use super::syntax::ScanState;
use super::STAGE_CATALOG;
use crate::helpers::{is_identifier_continue, is_identifier_start};

/// Where the stages of a query text start and end.
#[derive(Debug, Default)]
pub(crate) struct Layout {
    /// Byte offsets of the separators, one byte each (`|`, `;` or `\n`).
    separators: Vec<usize>,
    /// A final `;`, read as a space.
    trailing: Option<usize>,
    /// True when the stages are separated by `;` and line breaks.
    lines: bool,
}

impl Layout {
    /// The byte ranges of the top-level stages, between the separators.
    pub(crate) fn ranges(&self, text: &str) -> Vec<Range<usize>> {
        let end = self.trailing.unwrap_or(text.len());
        let mut start = 0;
        let mut ranges = Vec::with_capacity(self.separators.len() + 1);
        for &separator in &self.separators {
            ranges.push(start..separator);
            start = separator + 1;
        }
        ranges.push(start..end);
        ranges
    }
}

/// The separators of `text`.
///
/// # Errors
///
/// Returns an error for unbalanced brackets, a `;` in a query separated by
/// `|`, an empty stage between two `;`, and, when stages are separated by
/// lines, a block no `end` closes or an `end` closing no block.
pub(crate) fn layout(text: &str) -> Result<Layout, String> {
    let (pipes, semicolons) = top_level_marks(text)?;
    if pipes.is_empty() {
        return Lines::new(text).scan();
    }
    if let Some(&at) = semicolons.first() {
        return Err(format!("`;` at line {} in a query separated by `|`: separate its stages with `|`, or with `;` and line breaks, not both", line(text, at)));
    }
    Ok(Layout { separators: pipes, trailing: None, lines: false })
}

/// `text` with each stage separator written `|` (and a final `;` written as
/// a space): the text the parser reads, byte for byte as long as `text`.
///
/// # Errors
///
/// [`layout`].
pub fn normalize(text: &str) -> Result<Cow<'_, str>, String> {
    // Unbalanced brackets are left to the parser, which says where.
    if top_level_marks(text).is_err() {
        return Ok(Cow::Borrowed(text));
    }
    let layout = layout(text)?;
    if !layout.lines || (layout.separators.is_empty() && layout.trailing.is_none()) {
        return Ok(Cow::Borrowed(text));
    }
    let mut bytes = text.as_bytes().to_vec();
    for &separator in &layout.separators {
        bytes[separator] = b'|';
    }
    if let Some(trailing) = layout.trailing {
        bytes[trailing] = b' ';
    }
    // Only ASCII `;` and `\n` were replaced by ASCII bytes.
    Ok(Cow::Owned(String::from_utf8(bytes).map_err(|error| error.to_string())?))
}

/// True when `text` separates its stages by `;` and line breaks.
#[must_use]
pub fn separated_by_lines(text: &str) -> bool {
    layout(text).is_ok_and(|layout| layout.lines)
}

/// The top-level single `|` and `;` of `text` (strings and brackets aside,
/// `||` is the `or` operator).
fn top_level_marks(text: &str) -> Result<(Vec<usize>, Vec<usize>), String> {
    let (mut pipes, mut semicolons) = (Vec::new(), Vec::new());
    let mut state = ScanState::default();
    let mut characters = text.char_indices().peekable();
    while let Some((index, character)) = characters.next() {
        state.consume(character)?;
        if !state.is_top_level() {
            continue;
        }
        match character {
            '|' if characters.peek().is_some_and(|(_, next)| *next == '|') => {
                characters.next();
            }
            '|' => pipes.push(index),
            ';' => semicolons.push(index),
            _ => {}
        }
    }
    Ok((pipes, semicolons))
}

/// The line (from 1) of the byte at `offset`.
fn line(text: &str, offset: usize) -> usize {
    text.as_bytes()[..offset.min(text.len())].iter().filter(|byte| **byte == b'\n').count() + 1
}

/// Directives of the blocks whose sub-pipeline is not made of stages.
const GROUP: &[&str] = &["by", "sum", "avg", "min", "max", "list", "median", "stddev", "percentile", "first", "last", "firstby", "lastby", "count"];
const PIVOT: &[&str] = &["rows", "columns", "values", "aggregate"];
const WINDOW: &[&str] = &["partition", "order", "lag", "lead", "cumsum", "first", "last", "row_number"];
const LOAD: &[&str] = &["with", "chunk"];
const VIEW: &[&str] = &["description", "version", "materialized", "exec"];

/// Words a stage cannot end on: the next line goes on with it.
const CONTINUATIONS: &[&str] = &["and", "or", "not", "in", "as", "between", "by", "on", "into", "with", "from", "to", "step", "default", "version", "of", "keep", "index", "view"];

fn is_stage(word: &str) -> bool {
    STAGE_CATALOG.iter().any(|entry| entry.name == word || entry.aliases.contains(&word))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Block { Group, Pivot, Window, Load, Lookup, Union, View, Exec }

/// An open block: the stage that opened it and the first stage inside.
#[derive(Debug)]
struct Open<'a> {
    block: Block,
    word: &'a str,
    at: usize,
    inside: Option<(&'a str, usize)>,
}

#[derive(Clone, Copy, Debug)]
enum Token<'a> {
    Word(&'a str),
    Symbol(char),
    /// A string, a number or a bracketed group.
    Other,
}

#[derive(Debug, Default)]
struct Stage<'a> {
    first: Option<&'a str>,
    start: usize,
    tokens: usize,
    into: bool,
}

/// The scan of a query separated by `;` and line breaks.
struct Lines<'a> {
    text: &'a str,
    separators: Vec<usize>,
    semicolon: Option<usize>,
    blocks: Vec<Open<'a>>,
    stage: Stage<'a>,
    previous: Option<&'a str>,
    last: Option<Token<'a>>,
    newline: Option<usize>,
}

impl<'a> Lines<'a> {
    fn new(text: &'a str) -> Self {
        Self { text, separators: Vec::new(), semicolon: None, blocks: Vec::new(), stage: Stage::default(), previous: None, last: None, newline: None }
    }

    fn scan(mut self) -> Result<Layout, String> {
        let text = self.text;
        let mut characters = text.char_indices().peekable();
        let mut first = true;
        while let Some((index, character)) = characters.next() {
            if character == '\n' {
                self.newline = Some(index);
                continue;
            }
            if character.is_whitespace() {
                continue;
            }
            if character == ';' {
                self.separate(index)?;
                first = false;
                continue;
            }
            let token = if is_identifier_start(character) {
                let mut end = index + character.len_utf8();
                while let Some((at, next)) = characters.next_if(|(_, next)| is_identifier_continue(*next)) {
                    end = at + next.len_utf8();
                }
                Token::Word(&text[index..end])
            } else if matches!(character, '"' | '\'' | '(' | '[' | '{') {
                let mut state = ScanState::default();
                state.consume(character)?;
                while !state.is_top_level() {
                    let Some((_, next)) = characters.next() else { break };
                    state.consume(next)?;
                }
                Token::Other
            } else if matches!(character, ')' | ']' | '}') {
                return Err(format!("unexpected `{character}` at line {}", line(text, index)));
            } else if character.is_ascii_digit() {
                while characters.next_if(|(_, next)| next.is_ascii_alphanumeric() || *next == '.').is_some() {}
                Token::Other
            } else {
                Token::Symbol(character)
            };
            // `explain` is a prefix, not a stage.
            if std::mem::take(&mut first) && matches!(token, Token::Word("explain")) {
                continue;
            }
            self.token(index, token)?;
        }
        self.finish()
    }

    fn token(&mut self, index: usize, token: Token<'a>) -> Result<(), String> {
        if let Some(newline) = self.newline.take() {
            if let Token::Word(word) = token {
                if self.stage.tokens > 0 && !self.continues() && self.starts(word) {
                    self.separators.push(newline);
                    self.end_stage()?;
                }
            }
        }
        if self.stage.tokens == 0 {
            self.stage.first = match token { Token::Word(word) => Some(word), _ => None };
            self.stage.start = index;
            if let (Some(open), Some(word)) = (self.blocks.last_mut(), self.stage.first) {
                open.inside.get_or_insert((word, index));
            }
        } else if matches!(token, Token::Word("into")) {
            self.stage.into = true;
        }
        self.stage.tokens += 1;
        self.last = Some(token);
        Ok(())
    }

    /// A `;` at `index`.
    fn separate(&mut self, index: usize) -> Result<(), String> {
        self.newline = None;
        if self.stage.tokens == 0 {
            return Err(format!("empty stage before `;` at line {}", line(self.text, index)));
        }
        self.separators.push(index);
        self.semicolon = Some(index);
        self.end_stage()
    }

    /// True when the line before the line break goes on with the next one.
    fn continues(&self) -> bool {
        match self.last {
            Some(Token::Symbol(symbol)) => matches!(symbol, ',' | '=' | '+' | '-' | '*' | '/' | '%' | '<' | '>' | '!' | '&' | '|' | '.' | ':' | '$'),
            Some(Token::Word(word)) => CONTINUATIONS.contains(&word),
            _ => false,
        }
    }

    /// The block the current stage opens, if any.
    fn opens(&self, stage: &Stage<'a>) -> Option<Block> {
        let top = self.blocks.last().map(|open| open.block);
        match stage.first? {
            "lookup" | "join" if !stage.into => Some(Block::Lookup),
            "union" => Some(Block::Union),
            "pivot" => Some(Block::Pivot),
            "window" => Some(Block::Window),
            "group" if stage.tokens == 1 => Some(Block::Group),
            "load" if stage.tokens == 1 => Some(Block::Load),
            "exec" if top == Some(Block::View) => Some(Block::Exec),
            "define" if self.blocks.is_empty() && self.previous.is_none() => Some(Block::View),
            _ => None,
        }
    }

    /// True when a line starting with `word` starts a stage, once the
    /// current stage ends.
    fn starts(&self, word: &str) -> bool {
        if word == "end" {
            return true;
        }
        let context = if self.stage.first == Some("end") {
            self.blocks.len().checked_sub(2).map(|index| self.blocks[index].block)
        } else {
            self.opens(&self.stage).or_else(|| self.blocks.last().map(|open| open.block))
        };
        match context {
            Some(Block::Group) => GROUP.contains(&word),
            Some(Block::Pivot) => PIVOT.contains(&word),
            Some(Block::Window) => WINDOW.contains(&word),
            Some(Block::Load) => LOAD.contains(&word),
            Some(Block::View) => VIEW.contains(&word),
            Some(Block::Lookup | Block::Union | Block::Exec) | None => match word {
                // A source starts a stage after `union`, `exec` or a `let` header only.
                "on" | "from" => matches!(self.stage.first, Some("union" | "exec" | "let")),
                "let" => true,
                _ => is_stage(word),
            },
        }
    }

    fn end_stage(&mut self) -> Result<(), String> {
        let stage = std::mem::take(&mut self.stage);
        if stage.first == Some("end") {
            if self.blocks.pop().is_none() {
                return Err(format!("`end` at line {} closes no block", line(self.text, stage.start)));
            }
        } else if let Some(block) = self.opens(&stage) {
            self.blocks.push(Open { block, word: stage.first.unwrap_or_default(), at: stage.start, inside: None });
        }
        self.previous = stage.first.or(self.previous);
        Ok(())
    }

    fn finish(mut self) -> Result<Layout, String> {
        let mut trailing = None;
        if self.stage.tokens > 0 {
            self.end_stage()?;
        } else if self.semicolon.is_some() && self.separators.last() == self.semicolon.as_ref() {
            trailing = self.separators.pop();
        }
        // `define view` without its final `end` is said by the view statement itself.
        if let Some(open) = self.blocks.last().filter(|open| !(open.block == Block::View && self.blocks.len() == 1)) {
            let text = self.text;
            let inside = open.inside.map_or_else(String::new, |(word, at)| format!("; line {} starts a new stage `{word}`", line(text, at)));
            return Err(format!(
                "`{}` at line {} opens a block that no `end` closes{inside}: a line starting with a stage word starts a stage; to go on with the line before, end it with an operator or a comma, or keep a one-line form on one line",
                open.word, line(text, open.at),
            ));
        }
        Ok(Layout { separators: self.separators, trailing, lines: true })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalized(text: &str) -> String {
        normalize(text).unwrap().into_owned()
    }

    #[test] fn a_query_with_a_pipe_is_left_as_written() { for text in ["on t | where a > 1\nand b < 2", "on t\n| where a || b\n| sort a", "let $x = (on u\nwhere a) | on t", "on t | where s == \"a;b\""] { assert_eq!(normalized(text), text); assert!(!separated_by_lines(text)); } }
    #[test] fn semicolons_and_lines_separate_stages() { assert_eq!(normalized("on t; where a > 1; sort a"), "on t| where a > 1| sort a"); assert_eq!(normalized("on t\nwhere a > 1\nsort a desc"), "on t|where a > 1|sort a desc"); assert_eq!(normalized("on t\r\nwhere a\r\n"), "on t\r|where a\r\n"); assert!(separated_by_lines("on t\nwhere a")); }
    #[test] fn a_line_goes_on_after_a_continuation_or_without_a_stage_word() { for (text, stages) in [("on t\nwhere a >\n  count", 2), ("on t\nselect a,\n  first", 2), ("on t\nwhere a\n  and b\n  or c", 2), ("on t\nderive x = a\n  * 2", 2), ("on t as\n  u", 1), ("on values [\n {a: 1},\n {a: 2}\n]\nwhere a > 1", 2), ("on t\nwhere a == $x\n  and $y", 2)] { assert_eq!(layout(text).unwrap().ranges(text).len(), stages, "{text}"); } }
    #[test] fn blocks_read_their_directives() { let text = "on t\ngroup\n  by region\n  sum amount as total\n  count\nend\nsort total desc"; assert_eq!(normalized(text), "on t|group|  by region|  sum amount as total|  count|end|sort total desc"); let pivot = "on t\npivot\n rows r\n columns c\n values v\n aggregate sum\nend"; assert_eq!(layout(pivot).unwrap().ranges(pivot).len(), 7); let window = "on t\nwindow\n partition by r\n order by m desc\n lag v 12 as l\n row_number as n\nend"; assert_eq!(layout(window).unwrap().ranges(window).len(), 7); let load = "on t\nload\n with merge\n chunk [{a: 1}]\nend"; assert_eq!(layout(load).unwrap().ranges(load).len(), 5); }
    #[test] fn sources_start_a_stage_after_union_exec_and_let_only() { let union = "on t\nunion\n  on u\n  where a\nend"; assert_eq!(normalized(union), "on t|union|  on u|  where a|end"); assert_eq!(normalized("let $a = 1, $b = 2\nlet $c = 3\non t\nwhere a == $c"), "let $a = 1, $b = 2|let $c = 3|on t|where a == $c"); assert_eq!(normalized("explain\non t\nwhere a"), "explain\non t|where a"); assert_eq!(normalized("on t\nlookup u\n  on a == b into f\nsort a"), "on t|lookup u\n  on a == b into f|sort a"); }
    #[test] fn a_lookup_reads_its_stages_until_end() { let text = "on users\nlookup orders as o\n  where o.status == \"paid\"\n  into paid\nend\nselect name, paid"; assert_eq!(layout(text).unwrap().ranges(text).len(), 6); assert_eq!(layout("on t\nlookup u on a == b into f\nsort a").unwrap().ranges("on t\nlookup u on a == b into f\nsort a").len(), 3); }
    #[test] fn define_view_reads_its_clauses() { let text = "define view finance.cube\ndescription \"cube\"\nmaterialized\nexec\n  let $m = 1\n  on ledger\n  where month <= $m\nend\nend"; let ranges = layout(text).unwrap().ranges(text); assert_eq!(ranges.iter().map(|range| text[range.clone()].trim()).collect::<Vec<_>>(), ["define view finance.cube", "description \"cube\"", "materialized", "exec", "let $m = 1", "on ledger", "where month <= $m", "end", "end"]); }
    #[test] fn a_final_semicolon_and_blank_lines_are_spaces() { assert_eq!(normalized("on t; where a;"), "on t| where a "); assert_eq!(normalized("on t;\n\nwhere a\n\n"), "on t|\n\nwhere a\n\n"); let ranges = layout("on t;").unwrap().ranges("on t;"); assert_eq!(ranges.len(), 1); assert_eq!(ranges[0], 0..4); assert_eq!(normalized("on t;\n where a"), "on t|\n where a"); }
    #[test] fn refuses_mixed_styles_empty_stages_and_unbalanced_blocks() { assert!(layout("on t; where a | sort a").unwrap_err().contains("not both")); for text in ["; on t", "on t;; where a", "on t; ; where a"] { assert!(layout(text).unwrap_err().contains("empty stage"), "{text}"); } let error = layout("on t\nlookup u on a == b\n  into f\nsort a").unwrap_err(); assert!(error.contains("`lookup` at line 2 opens a block") && error.contains("line 3 starts a new stage `into`"), "{error}"); assert!(layout("on t\nend").unwrap_err().contains("`end` at line 2 closes no block")); assert_eq!(layout("define view a.").unwrap().ranges("define view a.").len(), 1); assert!(layout("define view v\nexec\n  on t\n  group\nend").unwrap_err().contains("`exec` at line 2")); assert_eq!(normalize("on t\nwhere a)").unwrap(), "on t\nwhere a)"); }
    #[test] fn parentheses_keep_their_own_style() { assert_eq!(normalized("let $s = (on settings\n | limit 1)\non ledger"), "let $s = (on settings\n | limit 1)|on ledger"); assert_eq!(normalized("let $s = (on settings; limit 1)\non ledger"), "let $s = (on settings; limit 1)|on ledger"); }
    #[test] fn a_single_stage_is_left_as_written() { for text in ["on users", "on t as of \"2026-01-01\"\n", "on view v\n version 2", "undefine view v", "from _og.operations"] { assert_eq!(normalized(text), text); } }
}
