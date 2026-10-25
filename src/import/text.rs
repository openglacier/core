#![cfg_attr(rustfmt, rustfmt_skip)]
//! Text files read as the Python worker did (`magixlib.read_text_raw`), in a stream: encoding guessed on the first
//! 300 KB, separator chosen on the first 500 lines, fields split tolerantly, fixed width or one column when no separator
//! fits. Corrected on the way: UTF-16 with its byte order mark, a value spanning lines between quotes (when the record
//! then has the width of the table), and extra fields joined back with the separator of the file (Python used `;`).
use super::scalar::py_strip;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, Read, Seek};
use std::path::Path;

const ENCODING_PREFIX: usize = 300_000;
const SEPARATOR_LINES: usize = 500;
const SEPARATORS: [char; 6] = [';', ',', '\t', '|', ':', '^'];
/// Lines a value between quotes may span.
const QUOTED_LINES: usize = 50;
/// A longer line is refused: it is no table, and reading it would take several times its size.
const MAX_LINE: usize = 64 << 20;

enum Decoder { Encoding(encoding_rs::Decoder), Latin1 }

/// The lines of a text file, cut where Python's `str.splitlines` cuts them.
pub(crate) struct Lines { file: File, decoder: Decoder, bytes: Vec<u8>, buffer: String, start: usize, scanned: usize, end: bool }

fn line_break(c: char) -> bool { matches!(c, '\n' | '\r' | '\x0b' | '\x0c' | '\x1c' | '\x1d' | '\x1e' | '\u{85}' | '\u{2028}' | '\u{2029}') }

impl Lines {
    /// UTF-8 (byte order mark dropped) when the first 300 KB read so, UTF-16 by its mark, else Windows-1252, or Latin-1
    /// when a byte Windows-1252 leaves undefined is there, as the Python worker tried them.
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?; let mut prefix = Vec::with_capacity(ENCODING_PREFIX);
        (&mut file).take(ENCODING_PREFIX as u64).read_to_end(&mut prefix)?; file.rewind()?;
        let decoder = if prefix.starts_with(&[0xFF, 0xFE]) { Decoder::Encoding(encoding_rs::UTF_16LE.new_decoder_with_bom_removal()) }
            else if prefix.starts_with(&[0xFE, 0xFF]) { Decoder::Encoding(encoding_rs::UTF_16BE.new_decoder_with_bom_removal()) }
            else if std::str::from_utf8(&prefix).is_ok_and(|_| true) || std::str::from_utf8(&prefix).is_err_and(|error| error.error_len().is_none()) { Decoder::Encoding(encoding_rs::UTF_8.new_decoder_with_bom_removal()) }
            else if !prefix.iter().any(|b| matches!(b, 0x81 | 0x8D | 0x8F | 0x90 | 0x9D)) { Decoder::Encoding(encoding_rs::WINDOWS_1252.new_decoder_without_bom_handling()) }
            else { Decoder::Latin1 };
        Ok(Self { file, decoder, bytes: vec![0; 1 << 16], buffer: String::new(), start: 0, scanned: 0, end: false })
    }

    pub(crate) fn next_line(&mut self) -> io::Result<Option<String>> {
        loop {
            if let Some(offset) = self.buffer[self.scanned..].find(line_break) {
                let at = self.scanned + offset; let c = self.buffer[at..].chars().next().expect("a break");
                if !(c == '\r' && at + 1 == self.buffer.len() && !self.end) {
                    let line = self.buffer[self.start..at].to_owned(); let mut next = at + c.len_utf8();
                    if c == '\r' && self.buffer[next..].starts_with('\n') { next += 1; }
                    self.start = next; self.scanned = next; return Ok(Some(line));
                }
                self.scanned = at;
            } else { self.scanned = self.buffer.len(); }
            if self.end {
                if self.start < self.buffer.len() { let line = self.buffer[self.start..].to_owned(); self.start = self.buffer.len(); self.scanned = self.start; return Ok(Some(line)); }
                return Ok(None);
            }
            self.buffer.drain(..self.start); self.scanned -= self.start; self.start = 0;
            if self.buffer.len() > MAX_LINE { return Err(io::Error::other(format!("a line of the file is longer than {} MiB", MAX_LINE >> 20))); }
            let read = self.file.read(&mut self.bytes)?; self.end = read == 0; let bytes = &self.bytes[..read];
            match &mut self.decoder {
                Decoder::Encoding(decoder) => { self.buffer.reserve(decoder.max_utf8_buffer_length(read).unwrap_or(read * 3)); let _ = decoder.decode_to_string(bytes, &mut self.buffer, self.end); }
                Decoder::Latin1 => self.buffer.extend(bytes.iter().map(|b| char::from(*b))),
            }
        }
    }
}

/// `split_tolerant_line`: the fields of `line`, and whether it ends inside quotes. A quote opens a value only at its
/// start and closes it only before a separator or the end; any other quote is text, and `""` inside quotes is a quote.
pub(crate) fn split(line: &str, separator: char) -> (Vec<String>, bool) {
    let (mut fields, mut current, mut quoted, mut chars) = (Vec::new(), String::new(), false, line.chars().peekable());
    while let Some(c) = chars.next() {
        if c == '"' {
            if quoted {
                match chars.peek() { Some('"') => { current.push('"'); chars.next(); } Some(next) if *next == separator => quoted = false, None => quoted = false, _ => current.push('"') }
            } else if current.is_empty() { quoted = true; } else { current.push('"'); }
        } else if c == separator && !quoted { fields.push(std::mem::take(&mut current)); } else { current.push(c); }
    }
    fields.push(current); (fields, quoted)
}

/// `detect_delimiter`: the separator giving the most stable width over the first 500 lines (most rows of the modal
/// width, then most rows of several fields, then the widest), none when three rows in four hold a single field.
pub(crate) fn separator(lines: &[String]) -> Option<char> {
    let mut best = (None, f64::NEG_INFINITY);
    for separator in SEPARATORS {
        let widths: Vec<usize> = lines.iter().take(SEPARATOR_LINES).filter(|line| !py_strip(line).is_empty()).map(|line| split(line, separator).0.len()).collect();
        if widths.len() < 2 { continue; }
        let several = widths.iter().filter(|width| **width > 1).count() as f64 / widths.len() as f64;
        if several < 0.75 { continue; }
        let mut counts: Vec<(usize, usize)> = Vec::new();
        for width in &widths { match counts.iter_mut().find(|(seen, _)| seen == width) { Some((_, count)) => *count += 1, None => counts.push((*width, 1)) } }
        let (modal, count) = counts.iter().fold((0, 0), |best, item| if item.1 > best.1 { *item } else { best });
        if modal <= 1 { continue; }
        let score = count as f64 / widths.len() as f64 * 100.0 + several * 10.0 + modal.min(500) as f64 * 0.01;
        if score > best.1 { best = (Some(separator), score); }
    }
    best.0
}

/// The columns of a fixed-width file as pandas infers them (`read_fwf`): the spans its first 100 lines fill.
fn fixed_columns(lines: &[String]) -> Vec<(usize, usize)> {
    let blank = |c: char| matches!(c, ' ' | '\t' | '\r' | '\n');
    let mut mask: Vec<bool> = Vec::new();
    for line in lines.iter().take(100) {
        for (index, c) in line.chars().enumerate() {
            if index >= mask.len() { mask.resize(index + 1, false); }
            if !blank(c) { mask[index] = true; }
        }
    }
    let mut columns = Vec::new(); let mut start = None;
    for (index, filled) in mask.iter().chain([false].iter()).enumerate() {
        match (start, filled) { (None, true) => start = Some(index), (Some(begin), false) => { columns.push((begin, index)); start = None; } _ => {} }
    }
    columns
}

fn fixed_fields(line: &str, columns: &[(usize, usize)]) -> Vec<String> {
    let chars: Vec<char> = line.chars().collect();
    columns.iter().map(|(start, end)| chars[(*start).min(chars.len())..(*end).min(chars.len())].iter().collect::<String>().trim_matches([' ', '\t', '\r', '\n']).to_owned()).collect()
}

enum Shape { Separated(char), Fixed(Vec<(usize, usize)>), Single }

/// The rows of a text file, all as wide as its first one.
pub(crate) struct TextRows { lines: Lines, pending: VecDeque<String>, shape: Shape, width: usize }

impl TextRows {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let mut lines = Lines::open(path)?; let mut head = Vec::new();
        while head.len() < SEPARATOR_LINES { match lines.next_line()? { Some(line) => head.push(line), None => break } }
        let shape = match separator(&head) {
            Some(separator) => Shape::Separated(separator),
            None => {
                // Fixed width when at least two columns hold something, else one column per line.
                let columns = fixed_columns(&head);
                let used = (0..columns.len()).filter(|index| head.iter().any(|line| !fixed_fields(line, &columns)[*index].is_empty() && !super::scalar::normalize_text(&fixed_fields(line, &columns)[*index]).is_empty())).count();
                if used > 1 { Shape::Fixed(columns) } else { Shape::Single }
            }
        };
        Ok(Self { lines, pending: head.into(), shape, width: 0 })
    }

    fn line(&mut self) -> io::Result<Option<String>> { match self.pending.pop_front() { Some(line) => Ok(Some(line)), None => self.lines.next_line() } }

    /// The next row: blank lines skipped (but for a single column), a value open between quotes continued on the next
    /// lines while that gives a row of the table's width.
    pub(crate) fn next_row(&mut self) -> io::Result<Option<Vec<String>>> {
        loop {
            let Some(line) = self.line()? else { return Ok(None) };
            let separator = match &self.shape {
                Shape::Single => return Ok(Some(vec![line])),
                Shape::Fixed(columns) => { let fields = fixed_fields(&line, columns); if fields.iter().all(String::is_empty) { continue; } return Ok(Some(fields)); }
                Shape::Separated(separator) => *separator,
            };
            if py_strip(&line).is_empty() { continue; }
            let (mut fields, open) = split(&line, separator);
            if open && fields.len() < self.width { if let Some(joined) = self.continued(line, separator)? { fields = joined; } }
            if self.width == 0 { self.width = fields.len(); }
            if fields.len() < self.width { fields.resize(self.width, String::new()); }
            if fields.len() > self.width { let extra = fields.split_off(self.width - 1); fields.push(extra.join(&separator.to_string())); }
            return Ok(Some(fields));
        }
    }

    fn continued(&mut self, line: String, separator: char) -> io::Result<Option<Vec<String>>> {
        let (mut text, mut used) = (line, Vec::new());
        while used.len() < QUOTED_LINES {
            let Some(next) = self.line()? else { break };
            text.push('\n'); text += &next; used.push(next);
            let (fields, open) = split(&text, separator);
            if fields.len() > self.width || !open && fields.len() != self.width { break; }
            if !open { return Ok(Some(fields)); }
        }
        for line in used.into_iter().rev() { self.pending.push_front(line); }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rows(name: &str, bytes: &[u8]) -> Vec<Vec<String>> {
        let path = std::env::temp_dir().join(format!("og-import-text-{}-{name}", std::process::id())); std::fs::write(&path, bytes).unwrap();
        let mut reader = TextRows::open(&path).unwrap(); let mut out = Vec::new(); while let Some(row) = reader.next_row().unwrap() { out.push(row); }
        std::fs::remove_file(&path).unwrap(); out
    }
    #[test] fn fields_split_tolerantly() { assert_eq!(split(r#"a;"b;c";"d""e";f"g;"h"#, ';'), (vec!["a".into(), "b;c".into(), "d\"e".into(), "f\"g".into(), "h".into()], true)); assert_eq!(split(r#""";x"y"z;"#, ';'), (vec![String::new(), "x\"y\"z".into(), String::new()], false)); }
    #[test] fn the_separator_is_the_most_stable_one() { let lines = |text: &str| text.lines().map(str::to_owned).collect::<Vec<_>>(); assert_eq!(separator(&lines("a;b,c\n1;2,3\n4;5,6")), Some(';')); assert_eq!(separator(&lines("a,b\n1,2\n3,4\n5,6;7")), Some(',')); assert_eq!(separator(&lines("a\tb\tc\n1\t2\t3")), Some('\t')); assert_eq!(separator(&lines("un\ndeux\ntrois")), None); }
    #[test] fn encodings_line_ends_and_quoted_lines_are_read() { assert_eq!(rows("utf8", "\u{feff}Nom;Ville\r\nÉlodie;Sète\r\n\r\nZoé;Agde".as_bytes()), [["Nom", "Ville"], ["Élodie", "Sète"], ["Zoé", "Agde"]]); assert_eq!(rows("cp1252", b"Nom;Montant\n\x80uro;12\nFran\xe7ois;3"), [["Nom", "Montant"], ["€uro", "12"], ["François", "3"]]); assert_eq!(rows("latin1", b"a;b\n\x81;\xe9"), [["a", "b"], ["\u{81}", "é"]]); let utf16: Vec<u8> = [0xFF, 0xFE].into_iter().chain("a\tb\r\n1\t2".encode_utf16().flat_map(u16::to_le_bytes)).collect(); assert_eq!(rows("utf16", &utf16), [["a", "b"], ["1", "2"]]); assert_eq!(rows("quoted", b"id;note;n\n1;\"deux\nlignes\";2\n2;\"ouvert;3\n3;x;4"), [["id", "note", "n"], ["1", "deux\nlignes", "2"], ["2", "ouvert;3", ""], ["3", "x", "4"]]); assert_eq!(rows("extra", b"a,b\n1,2,3\n4,5\n6,7\n8"), [["a", "b"], ["1", "2,3"], ["4", "5"], ["6", "7"], ["8", ""]]); assert_eq!(rows("cr", b"a;b\r1;2\r3;4"), [["a", "b"], ["1", "2"], ["3", "4"]]); }
    #[test] fn fixed_width_and_single_column_files_are_read() { assert_eq!(rows("fixed", b"Code  Ville     Total\nA1    Paris       12\n\nB22   Lyon         3\n"), [["Code", "Ville", "Total"], ["A1", "Paris", "12"], ["B22", "Lyon", "3"]]); assert_eq!(rows("single", b"nom\nAlice\n\nBob"), [["nom"], ["Alice"], [""], ["Bob"]]); }
}
