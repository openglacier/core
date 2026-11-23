//! The terminal of ogcli on Unix, without a library: raw mode, the keys and pastes decoded from the bytes the terminal
//! sends (CSI and SS3 sequences, bracketed paste), its size, the ANSI the line being edited
//! (cursor, history, completion), shared by the REPL (`read_line`) and the workspace (`tui.rs`).
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::{io::{self, Write}, sync::mpsc, time::Duration};

use rustix::{event::{poll, PollFd, PollFlags, Timespec}, termios::{self, OptionalActions, OutputModes, Termios}};

use super::complete::{common_prefix, Completer};

const CLEAR: &str = "\x1b[2J";
const PASTE_ON: &str = "\x1b[?2004h";
const PASTE_OFF: &str = "\x1b[?2004l";
const PASTE_START: &[u8] = b"\x1b[200~";
const PASTE_END: &[u8] = b"\x1b[201~";
/// How long the rest of an escape sequence (or of a paste) may take to come.
const SEQUENCE_WAIT: Duration = Duration::from_millis(30);
const PASTE_WAIT: Duration = Duration::from_secs(1);
const POLL: Duration = Duration::from_millis(50);

/// Columns × rows.
pub(super) fn size() -> io::Result<(u16, u16)> { let size = termios::tcgetwinsize(io::stdout())?; Ok((size.ws_col, size.ws_row)) }

/// The columns a character takes: 0 for marks and controls, 2 for East Asian wide ones and emoji, else 1.
pub(super) fn char_width(character: char) -> usize {
    match u32::from(character) {
        0..=0x1F | 0x7F..=0x9F | 0x200B..=0x200F | 0xFE00..=0xFE0F => 0,
        0x0300..=0x036F | 0x0483..=0x0489 | 0x0591..=0x05BD | 0x0610..=0x061A | 0x064B..=0x065F | 0x0E31 | 0x0E34..=0x0E3A
            | 0x1AB0..=0x1AFF | 0x1DC0..=0x1DFF | 0x20D0..=0x20FF | 0xFE20..=0xFE2F => 0,
        0x1100..=0x115F | 0x2E80..=0x303E | 0x3041..=0x33FF | 0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xA000..=0xA4CF | 0xAC00..=0xD7A3
            | 0xF900..=0xFAFF | 0xFE30..=0xFE4F | 0xFF00..=0xFF60 | 0xFFE0..=0xFFE6 | 0x1F300..=0x1F64F | 0x1F900..=0x1F9FF
            | 0x20000..=0x2FFFD | 0x30000..=0x3FFFD => 2,
        _ => 1,
    }
}

pub(super) fn text_width(text: &str) -> usize { text.chars().map(char_width).sum() }

/// Raw input while it lives (output still turns `\n` into `\r\n`), the terminal as it was afterwards.
#[derive(Debug)]
pub(super) struct Raw(Termios);

impl Raw {
    pub fn enter() -> io::Result<Self> {
        let saved = termios::tcgetattr(io::stdin())?;
        let mut raw = saved.clone();
        raw.make_raw();
        raw.output_modes.insert(OutputModes::OPOST | OutputModes::ONLCR);
        termios::tcsetattr(io::stdin(), OptionalActions::Now, &raw)?;
        Ok(Self(saved))
    }
}

impl Drop for Raw {
    fn drop(&mut self) { let _ = termios::tcsetattr(io::stdin(), OptionalActions::Now, &self.0); }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Key { Char(char), Ctrl(char), Alt(char), Enter, AltEnter, Tab, BackTab, Up, Down, ShiftUp, ShiftDown, Left, Right, Home, End, PageUp, PageDown, Backspace, Delete, Esc }

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Event { Key(Key), Paste(String) }

enum Parsed { Event(Event, usize), Skip(usize), Incomplete }

/// An event at the start of `bytes`; `last` when no more bytes are coming (a lone ESC is then the Esc key).
fn parse(bytes: &[u8], last: bool) -> Parsed {
    let key = |key, used| Parsed::Event(Event::Key(key), used);
    let more = |used| if last { Parsed::Skip(used) } else { Parsed::Incomplete };
    match bytes {
        [] => Parsed::Skip(0),
        [b'\r' | b'\n', ..] => key(Key::Enter, 1),
        [b'\t', ..] => key(Key::Tab, 1),
        [0x7f | 0x08, ..] => key(Key::Backspace, 1),
        [0x1b] => if last { key(Key::Esc, 1) } else { Parsed::Incomplete },
        [0x1b, 0x1b, ..] => key(Key::Esc, 1),
        [0x1b, b'\r', ..] => key(Key::AltEnter, 2),
        [0x1b, b'O'] => more(2),
        [0x1b, b'O', last_byte, ..] => match last_byte { b'A' => key(Key::Up, 3), b'B' => key(Key::Down, 3), b'C' => key(Key::Right, 3), b'D' => key(Key::Left, 3), b'H' => key(Key::Home, 3), b'F' => key(Key::End, 3), _ => Parsed::Skip(3) },
        [0x1b, b'[', rest @ ..] => {
            let Some(end) = rest.iter().position(|b| (0x40..=0x7e).contains(b)) else { return more(bytes.len()) };
            let (params, used) = (std::str::from_utf8(&rest[..end]).unwrap_or(""), end + 3);
            if bytes.starts_with(PASTE_START) {
                return match bytes.windows(PASTE_END.len()).position(|window| window == PASTE_END) {
                    Some(at) => Parsed::Event(Event::Paste(String::from_utf8_lossy(&bytes[PASTE_START.len()..at]).into_owned()), at + PASTE_END.len()),
                    None if last => Parsed::Event(Event::Paste(String::from_utf8_lossy(&bytes[PASTE_START.len()..]).into_owned()), bytes.len()),
                    None => Parsed::Incomplete,
                };
            }
            let mut numbers = params.split(';').map(|number| number.parse::<u32>().unwrap_or(1));
            let (number, shift) = (numbers.next().unwrap_or(1), numbers.next().is_some_and(|modifier| (modifier.saturating_sub(1)) & 1 == 1));
            match (rest[end], number) {
                (b'A', _) => key(if shift { Key::ShiftUp } else { Key::Up }, used), (b'B', _) => key(if shift { Key::ShiftDown } else { Key::Down }, used),
                (b'C', _) => key(Key::Right, used), (b'D', _) => key(Key::Left, used), (b'H', _) => key(Key::Home, used), (b'F', _) => key(Key::End, used), (b'Z', _) => key(Key::BackTab, used),
                (b'~', 1 | 7) => key(Key::Home, used), (b'~', 4 | 8) => key(Key::End, used), (b'~', 3) => key(Key::Delete, used), (b'~', 5) => key(Key::PageUp, used), (b'~', 6) => key(Key::PageDown, used),
                _ => Parsed::Skip(used),
            }
        }
        [0x1b, rest @ ..] => match utf8(rest, last) { Parsed::Event(Event::Key(Key::Char(c)), used) => key(Key::Alt(c), used + 1), Parsed::Skip(_) => key(Key::Esc, 1), other => other },
        [byte @ 0x01..=0x1a, ..] => key(Key::Ctrl(char::from(b'a' + byte - 1)), 1),
        [0x00..=0x1f, ..] => Parsed::Skip(1),
        _ => utf8(bytes, last),
    }
}

/// The character at the start of `bytes`.
fn utf8(bytes: &[u8], last: bool) -> Parsed {
    let length = match bytes[0] { 0x00..=0x7f => 1, 0xc0..=0xdf => 2, 0xe0..=0xef => 3, 0xf0..=0xf7 => 4, _ => return Parsed::Skip(1) };
    if bytes.len() < length { return if last { Parsed::Skip(1) } else { Parsed::Incomplete }; }
    match std::str::from_utf8(&bytes[..length]).ok().and_then(|text| text.chars().next()) { Some(c) => Parsed::Event(Event::Key(Key::Char(c)), length), None => Parsed::Skip(1) }
}

/// The keys typed, read from stdin in raw mode.
#[derive(Debug, Default)]
pub(super) struct Keys(Vec<u8>);

impl Keys {
    /// The next event, or `None` when nothing comes within `wait`.
    pub fn next(&mut self, wait: Duration) -> io::Result<Option<Event>> {
        if self.0.is_empty() && !self.fill(wait)? { return Ok(None); }
        loop {
            let parsed = match parse(&self.0, false) {
                Parsed::Incomplete => { if self.fill(if self.0.starts_with(PASTE_START) { PASTE_WAIT } else { SEQUENCE_WAIT })? { continue; } parse(&self.0, true) }
                parsed => parsed,
            };
            match parsed {
                Parsed::Event(event, used) => { self.0.drain(..used); return Ok(Some(event)); }
                Parsed::Skip(used) => { self.0.drain(..used.max(1).min(self.0.len())); if self.0.is_empty() { return Ok(None); } }
                Parsed::Incomplete => return Ok(None),
            }
        }
    }

    /// Whether bytes came within `wait`.
    fn fill(&mut self, wait: Duration) -> io::Result<bool> {
        let stdin = io::stdin();
        let timeout = Timespec { tv_sec: wait.as_secs() as i64, tv_nsec: i64::from(wait.subsec_nanos()) };
        match poll(&mut [PollFd::new(&stdin, PollFlags::IN)], Some(&timeout)) { Ok(0) | Err(rustix::io::Errno::INTR) => return Ok(false), Ok(_) => {}, Err(error) => return Err(error.into()) }
        let mut chunk = [0_u8; 4096];
        // From the descriptor: what std's buffer would keep, poll would not see.
        let read = rustix::io::read(&stdin, &mut chunk)?;
        self.0.extend_from_slice(&chunk[..read]);
        Ok(read > 0)
    }
}

/// The line being edited: text, cursor (a byte offset), and the lines submitted before, which ↑/↓ browse.
#[derive(Debug, Default)]
pub(super) struct Line { pub text: String, pub cursor: usize, history: Vec<String>, at: Option<usize> }

impl Line {
    pub fn insert(&mut self, text: &str) { self.text.insert_str(self.cursor, text); self.cursor += text.len(); }

    /// Replaces the text, the cursor at its end.
    pub fn set(&mut self, text: String) { self.cursor = text.len(); self.text = text; self.at = None; }

    /// The trimmed text, kept in the history unless empty or the same as the last; the line is emptied.
    pub fn submit(&mut self) -> String {
        let line = std::mem::take(&mut self.text).trim().to_owned();
        (self.cursor, self.at) = (0, None);
        if !line.is_empty() && self.history.last() != Some(&line) { self.history.push(line.clone()); }
        line
    }

    fn before(&self) -> usize { self.text[..self.cursor].char_indices().next_back().map_or(0, |(index, _)| index) }
    fn after(&self) -> usize { self.text[self.cursor..].chars().next().map_or(self.cursor, |c| self.cursor + c.len_utf8()) }

    /// ↑: an older line, from an empty line or while browsing; browsing stops once the shown line is edited.
    pub fn back(&mut self) {
        let at = match self.at.filter(|at| self.history.get(*at) == Some(&self.text)) {
            Some(at) => at.saturating_sub(1),
            None if self.text.is_empty() && !self.history.is_empty() => self.history.len() - 1,
            None => return,
        };
        self.show(Some(at));
    }

    /// ↓: a newer line, then back to an empty line.
    pub fn forward(&mut self) {
        let Some(at) = self.at.filter(|at| self.history.get(*at) == Some(&self.text)) else { return };
        self.show((at + 1 < self.history.len()).then_some(at + 1));
    }

    fn show(&mut self, at: Option<usize>) { self.set(at.map_or_else(String::new, |at| self.history[at].clone())); self.at = at; }

    /// The editing keys: characters, moves, deletions, history; false for any other key.
    pub fn edit(&mut self, key: Key) -> bool {
        match key {
            Key::Char(c) => self.insert(c.encode_utf8(&mut [0; 4])),
            Key::Left => self.cursor = self.before(),
            Key::Right => self.cursor = self.after(),
            Key::Home | Key::Ctrl('a') => self.cursor = 0,
            Key::End | Key::Ctrl('e') => self.cursor = self.text.len(),
            Key::Backspace => { let before = self.before(); self.text.drain(before..self.cursor); self.cursor = before; }
            Key::Delete => { let after = self.after(); self.text.drain(self.cursor..after); }
            Key::Ctrl('u') => { self.text.drain(..self.cursor); self.cursor = 0; }
            Key::Ctrl('k') => self.text.truncate(self.cursor),
            Key::Esc => self.set(String::new()),
            Key::Up => self.back(),
            Key::Down => self.forward(),
            _ => return false,
        }
        true
    }

    /// Completes the word before the cursor: the single candidate, or the prefix every candidate shares. The candidates.
    pub fn complete(&mut self, completer: &Completer) -> Vec<String> {
        let (start, candidates) = completer.complete(&self.text, self.cursor);
        let replacement = match &candidates[..] { [] => return candidates, [only] => only.clone(), many => common_prefix(many).to_owned() };
        if replacement.len() > self.cursor - start || candidates.len() == 1 {
            self.text.replace_range(start..self.cursor, &replacement);
            self.cursor = start + replacement.len();
        }
        candidates
    }
}

/// Reads a line at `prompt`, writing the messages of `printer` above it meanwhile; `None` at the end of the input
/// (Ctrl-D on an empty line). Ctrl-C empties the line.
pub(super) fn read_line(prompt: &str, line: &mut Line, keys: &mut Keys, completer: &Completer, printer: &mpsc::Receiver<String>) -> io::Result<Option<String>> {
    let raw = Raw::enter()?;
    let mut out = io::stdout().lock();
    write!(out, "{PASTE_ON}")?;
    let read = (|| -> io::Result<Option<String>> { loop {
        draw_line(&mut out, prompt, line)?;
        let event = loop {
            let mut printed = false;
            while let Ok(message) = printer.try_recv() { write!(out, "\r\x1b[K{message}\n")?; printed = true; }
            if printed { draw_line(&mut out, prompt, line)?; }
            if let Some(event) = keys.next(POLL)? { break event; }
        };
        match event {
            Event::Paste(text) => line.insert(&text.replace("\r\n", "\n").replace('\r', "\n")),
            Event::Key(Key::Enter) => { writeln!(out)?; return Ok(Some(line.submit())); }
            Event::Key(Key::Ctrl('d')) if line.text.is_empty() => { writeln!(out)?; return Ok(None); }
            Event::Key(Key::Ctrl('d')) => { line.edit(Key::Delete); }
            Event::Key(Key::Ctrl('c')) => { writeln!(out, "^C")?; line.set(String::new()); }
            Event::Key(Key::Ctrl('l')) => write!(out, "{CLEAR}\x1b[H")?,
            Event::Key(Key::Tab) => { let candidates = line.complete(completer); if candidates.len() > 1 { write!(out, "\n{}\n", columns(&candidates, size().map_or(80, |size| size.0)))?; } }
            Event::Key(key) => { line.edit(key); }
        }
    } })();
    let _ = write!(out, "{PASTE_OFF}");
    let _ = out.flush();
    drop(raw);
    read
}

/// The prompt and the line on one row, scrolled sideways to keep the cursor in view (a line break shows as ↵).
fn draw_line(out: &mut impl Write, prompt: &str, line: &Line) -> io::Result<()> {
    let room = usize::from(size().map_or(80, |size| size.0)).saturating_sub(text_width(prompt) + 1).max(1);
    let shown = |text: &str| text.chars().map(|c| if c == '\n' { '↵' } else { c }).collect::<String>();
    let (text, at) = (shown(&line.text), text_width(&shown(&line.text[..line.cursor])));
    let skip = at.saturating_sub(room - 1);
    let (mut column, mut visible) = (0, String::new());
    for c in text.chars() {
        let width = char_width(c);
        if column >= skip && column + width <= skip + room { visible.push(c); }
        column += width;
    }
    write!(out, "\r{prompt}{visible}\x1b[K\r")?;
    let right = text_width(prompt) + at - skip;
    if right > 0 { write!(out, "\x1b[{right}C")?; }
    out.flush()
}

/// The candidates in columns as wide as the longest, as many as `width` holds.
fn columns(candidates: &[String], width: u16) -> String {
    let column = candidates.iter().map(|candidate| text_width(candidate)).max().unwrap_or(0) + 2;
    let per_row = (usize::from(width) / column).max(1);
    candidates.chunks(per_row).map(|row| row.iter().map(|candidate| format!("{candidate}{}", " ".repeat(column - text_width(candidate)))).collect::<String>().trim_end().to_owned()).collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events(bytes: &[u8]) -> Vec<Event> { let (mut out, mut at) = (Vec::new(), 0); while at < bytes.len() { match parse(&bytes[at..], true) { Parsed::Event(event, used) => { out.push(event); at += used; } Parsed::Skip(used) => at += used.max(1), Parsed::Incomplete => break } } out }

    #[test] fn keys_and_sequences_decode() { use Key::*; assert_eq!(events(b"a\xc3\xa9\r\t\x7f\x01\x1b[A\x1b[1;2B\x1bOH\x1b[3~\x1b[5~\x1b[Z\x1b1\x1b\r\x1b"), [Char('a'), Char('é'), Enter, Tab, Backspace, Ctrl('a'), Up, ShiftDown, Home, Delete, PageUp, BackTab, Alt('1'), AltEnter, Esc].map(Event::Key)); assert_eq!(events(b"\x1b[200~on a\nwhere\x1b[201~x"), [Event::Paste("on a\nwhere".into()), Event::Key(Char('x'))]); assert!(matches!(parse(b"\x1b[1;", false), Parsed::Incomplete)); assert!(matches!(parse(b"\x1b[200~abc", false), Parsed::Incomplete)); assert!(matches!(parse(b"\xc3", false), Parsed::Incomplete)); }
    #[test] fn the_line_edits_and_browses_history() { let mut line = Line::default(); line.insert("on a"); assert_eq!(line.submit(), "on a"); line.insert("on bé"); line.edit(Key::Left); line.edit(Key::Backspace); line.edit(Key::Home); line.edit(Key::Delete); assert_eq!((line.text.as_str(), line.cursor), ("n é", 0)); line.edit(Key::Ctrl('k')); line.submit(); line.edit(Key::Up); assert_eq!(line.text, "on a"); line.edit(Key::Down); assert_eq!(line.text, ""); line.insert("x"); line.edit(Key::Up); assert_eq!(line.text, "x"); }
    #[test] fn candidates_fill_columns() { assert_eq!(columns(&["where".into(), "window".into(), "with".into()], 18), "where   window\nwith"); }
}
