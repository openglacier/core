#![cfg_attr(rustfmt, rustfmt_skip)]
//! Workbooks (xlsx, xlsm, xlsb, xls, ods) read with calamine. A sheet holding one table reads as pandas read it: the
//! grid from A1, the header found among its first 40 rows. Beyond that (level 2 of ideas/data-import-rust.md):
//! - the named tables of a sheet (ListObjects) are its tables, with their own header;
//! - a sheet holding several blocks of cells apart (two empty rows or columns between them) gives a table per block, the
//!   largest under the sheet's name; titles and notes on their own are left out;
//! - merged cells give their value to the cells they cover in the header, and down a column;
//! - a group header merged above the header names its columns « 2025 / CA »;
//! - chart sheets are left out, hidden sheets come after the visible ones.
use super::scalar::{column_names, header_row, Cell, Stamp, HEADER_FOLLOWING, HEADER_SCAN};
use super::Table;
use calamine::{Data, Dimensions, Reader, SheetType, SheetVisible, Sheets};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

/// More cells than this in a sheet's grid is refused (a cell in XFD1048576 makes 17 billion).
const MAX_CELLS: usize = 50_000_000;

/// Whether the file is a workbook: a zip (xlsx, xlsm, xlsb, ods) or an OLE compound file (xls), whatever its name.
pub(crate) fn is_workbook(path: &Path) -> bool {
    let mut magic = [0u8; 4];
    File::open(path).and_then(|mut file| file.read_exact(&mut magic)).is_ok() && (magic == *b"PK\x03\x04" || magic == [0xD0, 0xCF, 0x11, 0xE0])
}

fn open(path: &Path) -> Result<Sheets<BufReader<File>>, String> {
    let mut magic = [0u8; 4]; File::open(path).and_then(|mut file| file.read_exact(&mut magic)).map_err(|error| error.to_string())?;
    if magic == [0xD0, 0xCF, 0x11, 0xE0] { return calamine::open_workbook(path).map(Sheets::Xls).map_err(|error| format!("cannot read the workbook: {error}")); }
    calamine::open_workbook(path).map(Sheets::Xlsx).or_else(|_| calamine::open_workbook(path).map(Sheets::Xlsb)).or_else(|_| calamine::open_workbook(path).map(Sheets::Ods))
        .map_err(|error| format!("cannot read the workbook: {error}"))
}

fn cell(data: &Data) -> Cell {
    match data {
        Data::Int(value) => Cell::Int(*value),
        // openpyxl gives a whole number as an int.
        Data::Float(value) if value.fract() == 0.0 && value.abs() < 9.2e18 => Cell::Int(*value as i64),
        Data::Float(value) => Cell::Float(*value),
        Data::String(text) | Data::DateTimeIso(text) | Data::DurationIso(text) => Cell::Text(text.clone()),
        Data::Bool(value) => Cell::Bool(*value),
        Data::DateTime(value) if value.is_duration() => Cell::Span(value.as_f64()),
        Data::DateTime(value) => {
            let (year, month, day, hour, minute, second, milli) = value.to_ymd_hms_milli();
            let nanos = ((i64::from(hour) * 60 + i64::from(minute)) * 60 + i64::from(second)) * 1_000_000_000 + i64::from(milli) * 1_000_000;
            // openpyxl: a time alone below one day; the 29 February 1900 of Excel's calendar is the 28th.
            if (0.0..1.0).contains(&value.as_f64()) { return Cell::Time(Stamp { days: 0, nanos, offset: None }); }
            Stamp::new(i64::from(year), u32::from(month), u32::from(day), nanos).or_else(|| Stamp::new(i64::from(year), u32::from(month), u32::from(day) - 1, nanos)).map_or(Cell::Missing, Cell::Stamp)
        }
        Data::Error(_) => Cell::Missing,
        Data::Empty => Cell::empty(),
    }
}

/// The sheet as pandas reads it: from A1 to its last filled row and column.
fn grid(range: &calamine::Range<Data>) -> Result<Vec<Vec<Cell>>, String> {
    let (mut height, mut width) = (0, 0);
    for (row, column, data) in range.used_cells() { if !matches!(data, Data::Empty) && !matches!(data, Data::String(text) if text.is_empty()) { height = height.max(row + 1); width = width.max(column + 1); } }
    let Some((top, left)) = range.start() else { return Ok(Vec::new()) };
    let (height, width) = (height + top as usize, width + left as usize);
    if height.saturating_mul(width) > MAX_CELLS { return Err(format!("the sheet is too large to read ({height} rows by {width} columns)")); }
    let mut grid = vec![vec![Cell::empty(); width]; height];
    for (row, column, data) in range.used_cells() { let (row, column) = (row + top as usize, column + left as usize); if row < height && column < width { grid[row][column] = cell(data); } }
    Ok(grid)
}

/// Rows `top..bottom` and columns `left..right` of a grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Rect { top: usize, bottom: usize, left: usize, right: usize }

impl Rect {
    fn cells(&self) -> usize { (self.bottom - self.top) * (self.right - self.left) }
    fn filled(&self, grid: &[Vec<Cell>]) -> usize { (self.top..self.bottom).map(|row| grid[row][self.left..self.right].iter().filter(|cell| !cell.is_empty()).count()).sum() }
}

fn trim(grid: &[Vec<Cell>], rect: Rect) -> Option<Rect> {
    let filled = |row: usize, column: usize| !grid[row][column].is_empty();
    let rows: Vec<usize> = (rect.top..rect.bottom).filter(|row| (rect.left..rect.right).any(|column| filled(*row, column))).collect();
    let columns: Vec<usize> = (rect.left..rect.right).filter(|column| (rect.top..rect.bottom).any(|row| filled(row, *column))).collect();
    Some(Rect { top: *rows.first()?, bottom: rows.last()? + 1, left: *columns.first()?, right: columns.last()? + 1 })
}

/// The runs of `filled` separated by two empty entries or more, as `start..end`.
fn bands(filled: &[bool]) -> Vec<(usize, usize)> {
    let (mut out, mut start, mut gap) = (Vec::new(), None, 0);
    for (index, filled) in filled.iter().enumerate() {
        if !filled { gap += 1; continue; }
        match start { None => start = Some(index), Some(begin) if gap >= 2 => { out.push((begin, index - gap)); start = Some(index); } _ => {} }
        gap = 0;
    }
    if let Some(begin) = start { out.push((begin, filled.len() - gap)); }
    out
}

/// The blocks of a sheet, cut along two empty rows or columns, then again within each block. Blocks side by side over
/// the same rows are one table with empty columns in it.
fn blocks(grid: &[Vec<Cell>], rect: Rect, out: &mut Vec<Rect>) {
    let Some(rect) = trim(grid, rect) else { return };
    let rows: Vec<bool> = (rect.top..rect.bottom).map(|row| grid[row][rect.left..rect.right].iter().any(|cell| !cell.is_empty())).collect();
    let row_bands = bands(&rows);
    if row_bands.len() > 1 { for (start, end) in row_bands { blocks(grid, Rect { top: rect.top + start, bottom: rect.top + end, ..rect }, out); } return; }
    let columns: Vec<bool> = (rect.left..rect.right).map(|column| (rect.top..rect.bottom).any(|row| !grid[row][column].is_empty())).collect();
    let parts: Vec<Rect> = bands(&columns).into_iter().filter_map(|(start, end)| trim(grid, Rect { left: rect.left + start, right: rect.left + end, ..rect })).collect();
    if parts.len() > 1 && !parts.iter().all(|part| part.top == parts[0].top && part.bottom == parts[0].bottom) { for part in parts { blocks(grid, part, out); } return; }
    out.push(rect);
}

/// The tables of a sheet's blocks: at least two rows and two columns. A block below another, within its columns and
/// without a header of its own (its first row holds numbers or empty cells), continues it.
fn tables_of(grid: &[Vec<Cell>]) -> Vec<Rect> {
    let mut found = Vec::new();
    if let Some(first) = grid.first() { blocks(grid, Rect { top: 0, bottom: grid.len(), left: 0, right: first.len() }, &mut found); }
    let mut tables: Vec<Rect> = Vec::new();
    for rect in found {
        let titled = || { let values: Vec<String> = grid[rect.top][rect.left..rect.right].iter().map(Cell::normalized).filter(|value| !value.is_empty()).collect(); values.len() >= 2 && values.iter().all(|value| value.chars().any(char::is_alphabetic) && super::convert::number(value).is_none()) };
        match tables.last_mut() {
            Some(last) if rect.top >= last.bottom && rect.left >= last.left && rect.right <= last.right && !titled() => last.bottom = rect.bottom,
            _ => tables.push(rect),
        }
    }
    tables.retain(|rect| rect.bottom - rect.top >= 2 && rect.right - rect.left >= 2 && rect.filled(grid) >= 4);
    tables
}

/// The table in `rect`: merged cells spread, header found (or the first row for a named table), group headers joined.
fn table(grid: &[Vec<Cell>], rect: Rect, merges: &[Dimensions], named: bool, name: String, hidden: bool) -> Table {
    let mut rows: Vec<Vec<Cell>> = grid[rect.top..rect.bottom].iter().map(|row| row[rect.left..rect.right].to_vec()).collect();
    let inside = |row: usize, column: usize| (rect.top..rect.bottom).contains(&row) && (rect.left..rect.right).contains(&column);
    let mut groups = Vec::new();
    for merge in merges {
        let (top, left, bottom, right) = (merge.start.0 as usize, merge.start.1 as usize, merge.end.0 as usize, merge.end.1 as usize);
        if !inside(top, left) || top >= rect.top + HEADER_SCAN + HEADER_FOLLOWING && left != right { continue; }
        let value = grid[top][left].clone();
        for row in top..=bottom.min(rect.bottom - 1) { for column in left..=right.min(rect.right - 1) { rows[row - rect.top][column - rect.left] = value.clone(); } }
        if right > left { groups.push((bottom - rect.top, left - rect.left..=right.min(rect.right - 1) - rect.left)); }
    }
    let header = if named || rows.is_empty() { 0 } else { header_row(&rows.iter().take(HEADER_SCAN + HEADER_FOLLOWING).map(|row| row.iter().map(Cell::normalized).collect()).collect::<Vec<_>>()) };
    let mut names = rows.get(header).cloned().unwrap_or_default();
    if header > 0 {
        for (row, columns) in &groups {
            if *row != header - 1 { continue; }
            for column in columns.clone() {
                let (upper, lower) = (rows[header - 1][column].normalized(), names[column].normalized());
                if !upper.is_empty() && upper != lower { names[column] = Cell::Text(if lower.is_empty() { upper } else { format!("{upper} / {lower}") }); }
            }
        }
    }
    let data = rows.split_off((header + 1).min(rows.len()));
    // The lines above the header: the rows of the sheet over the table, then its own rows before the header.
    let titles = super::title_lines(grid[rect.top.saturating_sub(10)..rect.top].iter().map(Vec::as_slice).chain(rows.iter().take(header).map(Vec::as_slice)));
    Table { name, header: rect.top + header, columns: column_names(&names), rows: Box::new(data.into_iter().map(Ok)), hidden, titles }
}

/// The tables of the workbook, sheet by sheet; with `wanted`, only the table of that name, else the one holding most of
/// the mapping's `sources` columns (the first on a tie), the other sheets dropped as they are read.
pub(crate) fn tables(path: &Path, wanted: Option<&str>, sources: &[&str]) -> Result<Vec<Table>, String> {
    let mut book = open(path)?;
    let mut sheets: Vec<(String, bool)> = book.sheets_metadata().iter().filter(|sheet| sheet.typ == SheetType::WorkSheet).map(|sheet| (sheet.name.clone(), sheet.visible != SheetVisible::Visible)).collect();
    sheets.sort_by_key(|(_, hidden)| *hidden);
    if let Sheets::Xlsx(book) = &mut book { let _ = book.load_tables(); }
    let mut out: Vec<Table> = Vec::new();
    for (sheet, hidden) in sheets {
        let grid = grid(&book.worksheet_range(&sheet).map_err(|error| format!("cannot read the sheet {sheet}: {error}"))?)?;
        let merges = match &mut book { Sheets::Xlsx(book) => book.merge_cells_by_sheet_name(&sheet).unwrap_or_default(), Sheets::Xls(book) => book.merge_cells_by_sheet_name(&sheet).unwrap_or_default(), _ => Vec::new() };
        let mut named: Vec<(String, Rect)> = Vec::new();
        if let Sheets::Xlsx(book) = &mut book {
            let names: Vec<String> = book.table_names_in_sheet(&sheet).into_iter().cloned().collect();
            for table_name in names {
                let Ok(table) = book.table_by_name(&table_name) else { continue };
                if let (Some(start), Some(end)) = (table.data().start(), table.data().end()) {
                    let rect = Rect { top: (start.0 as usize).saturating_sub(1), bottom: (end.0 as usize + 1).min(grid.len()), left: start.1 as usize, right: (end.1 as usize + 1).min(grid.first().map_or(0, Vec::len)) };
                    if rect.top < rect.bottom && rect.left < rect.right { named.push((table_name, rect)); }
                }
            }
        }
        let sheet_tables: Vec<Table> = if !named.is_empty() {
            let single = named.len() == 1;
            named.into_iter().map(|(table_name, rect)| table(&grid, rect, &merges, true, if single { sheet.clone() } else { table_name }, hidden)).collect()
        } else {
            let mut rects = tables_of(&grid);
            if rects.len() <= 1 {
                let whole = Rect { top: 0, bottom: grid.len(), left: 0, right: grid.first().map_or(0, Vec::len) };
                vec![table(&grid, whole, &merges, false, sheet.clone(), hidden)]
            } else {
                let largest = (0..rects.len()).max_by_key(|index| (rects[*index].cells(), std::cmp::Reverse(*index))).unwrap_or(0);
                let main = rects.remove(largest);
                std::iter::once(table(&grid, main, &merges, false, sheet.clone(), hidden))
                    .chain(rects.into_iter().enumerate().map(|(index, rect)| table(&grid, rect, &merges, false, format!("{sheet} ({})", index + 2), hidden))).collect()
            }
        };
        for table in sheet_tables {
            let held = |table: &Table| table.columns.iter().filter(|column| sources.contains(&column.as_str())).count();
            match wanted {
                None => out.push(table),
                Some(wanted) if table.name == wanted => return Ok(vec![table]),
                Some(_) => if out.first().is_none_or(|best| held(&table) > held(best)) { out = vec![table]; },
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn grid(rows: &[&[&str]]) -> Vec<Vec<Cell>> { let width = rows.iter().map(|row| row.len()).max().unwrap_or(0); rows.iter().map(|row| (0..width).map(|index| Cell::Text(row.get(index).copied().unwrap_or("").into())).collect()).collect() }
    fn rects(rows: &[&[&str]]) -> Vec<(usize, usize, usize, usize)> { tables_of(&grid(rows)).into_iter().map(|rect| (rect.top, rect.bottom, rect.left, rect.right)).collect() }
    // Empty columns within one table, and a block without a header of its own below it, keep it one table.
    #[test] fn blocks_apart_are_tables_and_titles_are_not() { let sheet: &[&[&str]] = &[&["Rapport annuel"], &[], &["Client", "CA", "", "", "Mois", "Total"], &["A", "1", "", "", "2025-01", "4"], &["B", "2", "", "", "2025-02", "5"], &[], &[], &["Région", "Effectif"], &["Nord", "3"], &["Sud", "4"], &[], &["Source : interne"]]; assert_eq!(rects(sheet), [(0, 5, 0, 2), (2, 5, 4, 6), (7, 12, 0, 2)]); assert_eq!(rects(&[&["a", "", "", "b"], &["1", "", "", "2"], &["3", "", "", "4"], &[], &[], &["5", "", "", "6"]]), [(0, 6, 0, 4)]); assert_eq!(bands(&[true, false, true, false, false, true, true, false]), [(0, 3), (5, 7)]); }
    #[test] fn merged_cells_fill_the_header_and_group_headers_name_columns() { let sheet = grid(&[&["Client", "2025", "", "2026", ""], &["", "CA", "Marge", "CA", "Marge"], &["A", "1", "2", "3", "4"], &["", "5", "6", "7", "8"]]); let merges = [Dimensions::new((0, 0), (1, 0)), Dimensions::new((0, 1), (0, 2)), Dimensions::new((0, 3), (0, 4)), Dimensions::new((2, 0), (3, 0))]; let mut table = table(&sheet, Rect { top: 0, bottom: 4, left: 0, right: 5 }, &merges, false, "S".into(), false); assert_eq!((table.header, table.columns.clone()), (1, vec!["Client".to_owned(), "2025 / CA".into(), "2025 / Marge".into(), "2026 / CA".into(), "2026 / Marge".into()])); assert_eq!(table.rows.nth(1).unwrap().unwrap()[0], Cell::Text("A".into())); }
    #[test] fn excel_values_are_typed_as_openpyxl_gave_them() { assert_eq!([cell(&Data::Float(3.0)), cell(&Data::Float(2.5)), cell(&Data::Error(calamine::CellErrorType::NA)), cell(&Data::Empty)], [Cell::Int(3), Cell::Float(2.5), Cell::Missing, Cell::empty()]); let date = |value: f64| cell(&Data::DateTime(calamine::ExcelDateTime::new(value, calamine::ExcelDateTimeType::DateTime, false))); assert_eq!((date(45_658.5).normalized(), date(0.75).normalized(), date(60.0).normalized()), ("2025-01-01T12:00:00".into(), "18:00:00".into(), "1900-02-28T00:00:00".into())); }
}
