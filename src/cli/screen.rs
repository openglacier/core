//! What the terminal workspace (`tui.rs`) draws with: a frame is drawn into a `Buffer` of cells (text, bordered
//! blocks, paragraphs wrapped or scrolled sideways), then `Screen` writes to the terminal the cells that differ from
//! the frame before, with crossterm.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::io::{self, Write};

use crossterm::{
    cursor::{Hide, MoveTo, Show},
    queue,
    style::{Attribute, Print, SetAttribute},
    terminal::{Clear, ClearType},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Rect { pub x: u16, pub y: u16, pub width: u16, pub height: u16 }

impl Rect {
    /// Inside a border.
    pub fn inner(self) -> Self {
        Self { x: self.x.saturating_add(1), y: self.y.saturating_add(1), width: self.width.saturating_sub(2), height: self.height.saturating_sub(2) }
    }

    /// `top` rows, the rest, then `bottom` rows (each cut to what there is).
    pub fn rows(self, top: &[u16], bottom: &[u16]) -> (Vec<Self>, Self, Vec<Self>) {
        let mut y = self.y;
        let end = self.y.saturating_add(self.height);
        let take = |height: u16, y: &mut u16| { let height = height.min(end.saturating_sub(*y)); let row = Self { y: *y, height, ..self }; *y += height; row };
        let tops: Vec<Self> = top.iter().map(|height| take(*height, &mut y)).collect();
        let reserved: u16 = bottom.iter().sum();
        let middle = take(end.saturating_sub(y).saturating_sub(reserved), &mut y);
        let bottoms = bottom.iter().map(|height| take(*height, &mut y)).collect();
        (tops, middle, bottoms)
    }

    /// The left and right halves.
    pub fn columns(self) -> [Self; 2] {
        let left = self.width.div_ceil(2);
        [Self { width: left, ..self }, Self { x: self.x + left, width: self.width - left, ..self }]
    }

    /// The top and bottom halves.
    pub fn halves(self) -> [Self; 2] {
        let top = self.height.div_ceil(2);
        [Self { height: top, ..self }, Self { y: self.y + top, height: self.height - top, ..self }]
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Style { pub bold: bool, pub reversed: bool }

impl Style {
    pub const PLAIN: Self = Self { bold: false, reversed: false };
    pub const BOLD: Self = Self { bold: true, reversed: false };
    pub const SELECTED: Self = Self { bold: true, reversed: true };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Cell { symbol: char, style: Style }

impl Cell {
    const BLANK: Self = Self { symbol: ' ', style: Style::PLAIN };
    /// The right half of a wide character, written by it.
    const TAIL: Self = Self { symbol: '\0', style: Style::PLAIN };
}

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

/// `line` in rows of `width` columns, broken after a space where it can.
fn wrap(line: &str, width: usize) -> Vec<String> {
    let mut rows = Vec::new();
    let (mut row, mut used) = (String::new(), 0);
    for word in line.split_inclusive(' ') {
        let (whole, bare) = (text_width(word), text_width(word.trim_end_matches(' ')));
        if used + bare > width && used > 0 && bare <= width {
            rows.push(std::mem::take(&mut row));
            used = 0;
        }
        if used + bare <= width {
            row.push_str(word);
            used += whole;
            continue;
        }
        for character in word.chars() {
            let columns = char_width(character);
            if used + columns > width && used > 0 {
                // A space at the end of a full row is dropped rather than starting the next one.
                if character == ' ' { continue; }
                rows.push(std::mem::take(&mut row));
                used = 0;
            }
            row.push(character);
            used += columns;
        }
    }
    rows.push(row);
    rows
}

/// A frame: cells, and where the cursor shows (hidden when `None`).
pub(super) struct Buffer { width: u16, height: u16, cells: Vec<Cell>, cursor: Option<(u16, u16)> }

impl Buffer {
    pub fn new(width: u16, height: u16) -> Self {
        Self { width, height, cells: vec![Cell::BLANK; usize::from(width) * usize::from(height)], cursor: None }
    }

    pub fn area(&self) -> Rect { Rect { x: 0, y: 0, width: self.width, height: self.height } }

    pub fn set_cursor(&mut self, x: u16, y: u16) { self.cursor = Some((x, y)); }

    fn set(&mut self, x: u16, y: u16, cell: Cell) {
        if x < self.width && y < self.height { self.cells[usize::from(y) * usize::from(self.width) + usize::from(x)] = cell; }
    }

    /// `text` from `(x, y)` on at most `max` columns, after skipping `skip` columns of it; the columns written.
    pub fn text(&mut self, x: u16, y: u16, max: u16, text: &str, skip: usize, style: Style) -> u16 {
        let (mut column, mut skipped) = (0u16, 0usize);
        for character in text.chars() {
            let character = if character == '\t' { ' ' } else { character };
            let width = char_width(character);
            if width == 0 { continue; }
            if skipped < skip { skipped += width; continue; }
            if usize::from(column) + width > usize::from(max) { break; }
            self.set(x + column, y, Cell { symbol: character, style });
            if width == 2 { self.set(x + column + 1, y, Cell::TAIL); }
            column += width as u16;
        }
        column
    }

    /// One line of styled pieces.
    pub fn line(&mut self, area: Rect, pieces: &[(&str, Style)]) {
        if area.height == 0 { return; }
        let mut x = 0;
        for (text, style) in pieces {
            x += self.text(area.x + x, area.y, area.width - x, text, 0, *style);
        }
    }

    /// A border around `area`, `title` on its top edge.
    pub fn block(&mut self, area: Rect, title: &str, style: Style) {
        if area.width < 2 || area.height < 2 { return; }
        let (right, bottom) = (area.x + area.width - 1, area.y + area.height - 1);
        for x in area.x + 1..right {
            self.set(x, area.y, Cell { symbol: '─', style });
            self.set(x, bottom, Cell { symbol: '─', style });
        }
        for y in area.y + 1..bottom {
            self.set(area.x, y, Cell { symbol: '│', style });
            self.set(right, y, Cell { symbol: '│', style });
        }
        for (x, y, symbol) in [(area.x, area.y, '┌'), (right, area.y, '┐'), (area.x, bottom, '└'), (right, bottom, '┘')] {
            self.set(x, y, Cell { symbol, style });
        }
        self.text(area.x + 1, area.y, area.width - 2, title, 0, style);
    }

    /// `lines` in `area`: wrapped (then `skip` ignored), or cut and scrolled `skip` columns sideways; from the row
    /// `top` of them, or showing the last ones when `None`.
    pub fn paragraph<'a>(&mut self, area: Rect, lines: impl IntoIterator<Item = &'a str>, wrapped: bool, top: Option<usize>, skip: usize) {
        if area.width == 0 || area.height == 0 { return; }
        let rows: Vec<std::borrow::Cow<'a, str>> = if wrapped {
            lines.into_iter().flat_map(|line| wrap(line, usize::from(area.width))).map(std::borrow::Cow::Owned).collect()
        } else {
            lines.into_iter().map(std::borrow::Cow::Borrowed).collect()
        };
        let height = usize::from(area.height);
        let first = top.unwrap_or_else(|| rows.len().saturating_sub(height)).min(rows.len());
        for (index, row) in rows[first..].iter().take(height).enumerate() {
            self.text(area.x, area.y + index as u16, area.width, row, if wrapped { 0 } else { skip }, Style::PLAIN);
        }
    }
}

/// The terminal and the frame it shows.
pub(super) struct Screen { out: io::Stdout, shown: Option<Buffer> }

impl Screen {
    pub fn new(out: io::Stdout) -> Self { Self { out, shown: None } }

    pub fn out(&mut self) -> &mut io::Stdout { &mut self.out }

    /// Writes the cells of `frame` that differ from the frame shown (all of them after a resize).
    pub fn draw(&mut self, frame: Buffer) -> io::Result<()> {
        let shown = self.shown.take().filter(|shown| shown.width == frame.width && shown.height == frame.height);
        if shown.is_none() { queue!(self.out, SetAttribute(Attribute::Reset), Clear(ClearType::All))?; }
        let mut style = Style::PLAIN;
        let mut at: Option<(u16, u16)> = None;
        queue!(self.out, Hide, SetAttribute(Attribute::Reset))?;
        for y in 0..frame.height {
            for x in 0..frame.width {
                let index = usize::from(y) * usize::from(frame.width) + usize::from(x);
                let cell = frame.cells[index];
                // A wide character and the cell it covers change together: comparing cells is enough.
                if cell == Cell::TAIL || shown.as_ref().is_some_and(|shown| shown.cells[index] == cell) {
                    continue;
                }
                if at != Some((x, y)) { queue!(self.out, MoveTo(x, y))?; }
                if cell.style != style {
                    queue!(self.out, SetAttribute(Attribute::Reset))?;
                    if cell.style.bold { queue!(self.out, SetAttribute(Attribute::Bold))?; }
                    if cell.style.reversed { queue!(self.out, SetAttribute(Attribute::Reverse))?; }
                    style = cell.style;
                }
                queue!(self.out, Print(cell.symbol))?;
                at = Some((x + char_width(cell.symbol).max(1) as u16, y));
            }
        }
        queue!(self.out, SetAttribute(Attribute::Reset))?;
        if let Some((x, y)) = frame.cursor { queue!(self.out, MoveTo(x, y), Show)?; }
        self.out.flush()?;
        self.shown = Some(frame);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(buffer: &Buffer) -> Vec<String> { buffer.cells.chunks(usize::from(buffer.width)).map(|row| row.iter().filter(|cell| **cell != Cell::TAIL).map(|cell| cell.symbol).collect::<String>().trim_end().to_owned()).collect() }

    #[test] fn a_block_holds_its_title_and_a_paragraph_wrapped_or_scrolled() { let mut buffer = Buffer::new(12, 5); let area = buffer.area(); buffer.block(area, "job #1 on a long title", Style::BOLD); buffer.paragraph(area.inner(), ["one two three four", "five"], true, None, 0); assert_eq!(rows(&buffer), ["┌job #1 on ┐", "│one two   │", "│three four│", "│five      │", "└──────────┘"]); let mut buffer = Buffer::new(6, 2); let area = buffer.area(); buffer.paragraph(area, ["abcdefghij", "klmnopqrst", "uvwxyz"], false, Some(1), 2); assert_eq!(rows(&buffer), ["mnopqr", "wxyz"]); }
    #[test] fn wide_characters_take_two_columns_and_wrapping_breaks_long_words() { assert_eq!((text_width("日本"), text_width("é"), text_width("e\u{301}")), (4, 1, 1)); let mut buffer = Buffer::new(5, 1); assert_eq!(buffer.text(0, 0, 5, "日本語", 0, Style::PLAIN), 4); assert_eq!(rows(&buffer), ["日本"]); assert_eq!(wrap("abcdefgh ij", 4), ["abcd", "efgh", "ij"]); assert_eq!(wrap("", 4), [""]); }
    #[test] fn rows_and_halves_share_the_area() { let area = Rect { x: 0, y: 0, width: 81, height: 20 }; let (tops, middle, bottoms) = area.rows(&[1], &[1, 3]); assert_eq!((tops[0].height, middle.y, middle.height, bottoms[0].y, bottoms[1].y, bottoms[1].height), (1, 1, 15, 16, 17, 3)); assert_eq!(area.columns().map(|rect| (rect.x, rect.width)), [(0, 41), (41, 40)]); assert_eq!(area.halves().map(|rect| (rect.y, rect.height)), [(0, 10), (10, 10)]); }
}
