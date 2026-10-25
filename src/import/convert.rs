#![cfg_attr(rustfmt, rustfmt_skip)]
//! What a mapping makes of a cell (`_convert` of the Python worker): text, number, date, date and time, month, boolean.
//! Dates read as pandas did, with two corrections: a date starting with its year is always year-month-day, and a number
//! in a date column is an Excel serial date (pandas read it as nanoseconds since 1970).
use super::scalar::{casefold, py_strip, regex, Cell, Stamp};
use serde_json::Value as JsonValue;

/// The order of a date written « a/b/c »: day first (the default), month first, or year first (ISO).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Order { DayFirst, MonthFirst, Iso }

/// A field's date format (`_date_format`): an order (`dmy`, `mdy`, `ymd`, `auto`), or a pattern in tokens (`dd/mm/yyyy`,
/// `d.m.yy`, `yyyy-mm-dd hh:mm`, where `mm` after `hh:` is minutes) or in strftime codes (`%d/%m/%Y`).
pub(crate) enum Format { Auto, Order(Order), Pattern(String) }

impl Format {
    pub(crate) fn parse(format: Option<&JsonValue>) -> Self {
        let Some(text) = format.and_then(JsonValue::as_str).map(str::trim).filter(|text| !text.is_empty()) else { return Self::Auto };
        match casefold(text).as_str() {
            "dmy" | "dayfirst" => return Self::Order(Order::DayFirst), "mdy" | "monthfirst" => return Self::Order(Order::MonthFirst),
            "ymd" | "iso" => return Self::Order(Order::Iso), "auto" => return Self::Auto, _ => {}
        }
        if text.contains('%') { return Self::Pattern(text.to_owned()); }
        let (lower, mut out, mut index) = (casefold(text), String::new(), 0);
        let original: Vec<char> = text.chars().collect(); let chars: Vec<char> = lower.chars().collect();
        while index < chars.len() {
            let rest: String = chars[index..].iter().collect();
            if rest.starts_with("mm") && out.ends_with("%H:") { out += "%M"; index += 2; continue; }
            match [("yyyy", "%Y"), ("yy", "%y"), ("dd", "%d"), ("d", "%d"), ("hh", "%H"), ("ss", "%S"), ("mmm", "%b"), ("mm", "%m"), ("m", "%m")].iter().find(|(token, _)| rest.starts_with(token)) {
                Some((token, code)) => { out += code; index += token.len(); }
                None => { out.push(original.get(index).copied().unwrap_or(chars[index])); index += 1; }
            }
        }
        Self::Pattern(out)
    }
}

/// `_convert`: the JSON value of `cell` under `transform`; null when it does not read.
pub(crate) fn convert(cell: &Cell, transform: Option<&str>, format: &Format) -> JsonValue {
    let Some(text) = cell.py_str() else { return JsonValue::Null };
    match transform.unwrap_or("") {
        "" | "string" => JsonValue::String(text),
        "number" | "decimal" => number_of(cell).map_or(JsonValue::Null, JsonValue::from),
        "date" => date(cell, format).map_or(JsonValue::Null, |stamp| stamp.date().into()),
        "datetime" => date(cell, format).map_or(JsonValue::Null, |stamp| stamp.format('T').into()),
        "month" => month(cell, format).map_or(JsonValue::Null, JsonValue::from),
        "boolean" => matches!(casefold(py_strip(&text)).as_str(), "1" | "true" | "yes" | "oui" | "vrai").into(),
        _ => match cell { Cell::Int(value) => (*value).into(), Cell::Float(value) => JsonValue::from(*value), Cell::Bool(value) => (*value).into(), _ => text.into() },
    }
}

pub(crate) fn number_of(cell: &Cell) -> Option<f64> {
    match cell { Cell::Int(value) => Some(*value as f64), Cell::Float(value) => Some(*value).filter(|value| value.is_finite()), other => number(&other.py_str()?) }
}

/// A number written by hand: « 1 234,56 », « 1.234,56 », « 1,234.56 », « 1234.56 € », « (120) » (negative), « 16,2% » (0.162).
pub(crate) fn number(text: &str) -> Option<f64> {
    let text: String = text.chars().filter(|c| !(c.is_whitespace() || matches!(c, '€' | '$' | '£'))).collect();
    let negative = text.starts_with('(') && text.ends_with(')'); let percent = text.ends_with('%');
    let mut text = text.trim_matches(['(', ')', '%']).to_owned();
    let (commas, dots) = (text.matches(',').count(), text.matches('.').count());
    if commas == 1 && dots >= 1 && text.rfind(',') > text.rfind('.') { text = text.replace('.', ""); } else if commas >= 1 && dots == 1 && text.rfind('.') > text.rfind(',') { text = text.replace(',', ""); }
    let text = text.replace(',', ".");
    // Python's float() also takes « 1_000 », and gives infinities and NaN, which no JSON holds.
    let text = if regex!("^[+-]?[0-9]+(_[0-9]+)+(\\.[0-9]+)?$").is_match(&text) { text.replace('_', "") } else { text };
    let value: f64 = text.parse().ok().filter(|value: &f64| value.is_finite())?;
    let value = if negative { -value } else { value };
    Some(if percent { value / 100.0 } else { value })
}

/// The date of a cell: an Excel date as it is, a number as an Excel serial date, a text by its format, else day first when
/// written « 03/02/2025 », else as pandas reads it (ISO, year first, month names).
pub(crate) fn date(cell: &Cell, format: &Format) -> Option<Stamp> {
    match cell {
        Cell::Stamp(stamp) => Some(stamp.clone()),
        Cell::Int(_) | Cell::Float(_) => serial(number_of(cell)?, false),
        Cell::Text(text) => {
            let text = py_strip(text);
            match format {
                Format::Pattern(pattern) => strptime(text, pattern).or_else(|| {
                    let date_part = pattern.split(' ').next().unwrap_or(pattern);
                    [text.split(' ').next(), text.split('T').next()].into_iter().flatten().find_map(|candidate| strptime(candidate, date_part))
                }),
                Format::Order(order) => loose(text, *order),
                Format::Auto => loose(text, if regex!(r"^[0-9]{1,2}[-/.][0-9]{1,2}[-/.][0-9]{2,4}\b").is_match(text) { Order::DayFirst } else { Order::Iso }),
            }
        }
        _ => None,
    }
}

/// A period (12-2018, 2018-12, 201812, 31/12/2018…) as the epoch in milliseconds (UTC) of the first day of its month.
pub(crate) fn month(cell: &Cell, format: &Format) -> Option<i64> {
    let text = cell.py_str()?; let text = py_strip(&text);
    let (year, month) = if let (Format::Pattern(_), _) | (_, Cell::Stamp(_)) = (format, cell) { let (year, month, _) = date(cell, format)?.ymd(); (year, month) }
        else if let Some(found) = regex!("^([0-9]{1,2})[-/. ]([0-9]{4})$").captures(text) { (found[2].parse().ok()?, found[1].parse().ok()?) }
        else if let Some(found) = regex!("^([0-9]{4})[-/. ]?([0-9]{2})$").captures(text) { (found[1].parse().ok()?, found[2].parse().ok()?) }
        else { let (year, month, _) = date(cell, format)?.ymd(); (year, month) };
    (1..=12).contains(&month).then(|| crate::query::dates::days_from_civil(year, month, 1) * 86_400_000)
}

/// An Excel serial date (days since 1899-12-30, or 1904-01-01), rounded to the millisecond as openpyxl does; the
/// 1900 calendar counts a 29 February 1900 that never was, so its first 59 days move by one.
pub(crate) fn serial(value: f64, epoch_1904: bool) -> Option<Stamp> {
    if !value.is_finite() || !(-700_000.0..3_000_000.0).contains(&value) { return None; }
    let (day, fraction) = (value.floor(), value - value.floor()); let millis = (fraction * 86_400_000.0).round() as i64;
    let day = day as i64 + if !epoch_1904 && value > 0.0 && value < 60.0 { 1 } else { 0 } + millis / 86_400_000;
    let epoch = if epoch_1904 { crate::query::dates::days_from_civil(1904, 1, 1) } else { crate::query::dates::days_from_civil(1899, 12, 30) };
    Some(Stamp { days: epoch + day, nanos: millis % 86_400_000 * 1_000_000, offset: None })
}

const MONTHS: [&str; 12] = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];

fn month_name(word: &str) -> Option<u32> {
    let word = word.to_ascii_lowercase(); let word = if word == "sept" { "sep".to_owned() } else { word };
    MONTHS.iter().position(|name| word.len() >= 3 && (word == *name || word.starts_with(name) && ["january", "february", "march", "april", "june", "july", "august", "september", "october", "november", "december"].contains(&word.as_str()))).map(|index| index as u32 + 1)
}

/// A two-digit year as dateutil reads it: within fifty years of the current one.
fn full_year(year: i64, digits: usize) -> i64 {
    if digits > 2 { return year; }
    let now = crate::query::dates::civil_from_days(crate::helpers::now_millis().div_euclid(86_400_000)).0;
    let year = now / 100 * 100 + year;
    if year >= now + 50 { year - 100 } else if year < now - 50 { year + 100 } else { year }
}

/// A time of day after a date: `HH:MM[:SS[.fraction]]`, with an offset (`Z`, `+02:00`).
fn clock(text: &str) -> Option<(i64, Option<String>)> {
    if text.is_empty() { return Some((0, None)); }
    let found = regex!(r"^[T ]?([0-9]{1,2})(?::?([0-9]{2})(?::?([0-9]{2})(?:[.,]([0-9]{1,9}))?)?)?\s*(Z|[+-][0-9]{2}:?[0-9]{2})?$").captures(text)?;
    let part = |index: usize| found.get(index).map_or(0, |part| part.as_str().parse::<i64>().unwrap_or(0));
    let (hour, minute, second) = (part(1), part(2), part(3));
    if hour > 23 || minute > 59 || second > 59 { return None; }
    let fraction = found.get(4).map_or(0, |part| format!("{:0<9}", part.as_str()).parse::<i64>().unwrap_or(0));
    let offset = found.get(5).map(|offset| match offset.as_str() { "Z" => "+00:00".to_owned(), offset => { let digits = offset[1..].replace(':', ""); format!("{}{}:{}", &offset[..1], &digits[..2], &digits[2..]) } });
    Some((((hour * 60 + minute) * 60 + second) * 1_000_000_000 + fraction, offset))
}

fn stamp(year: i64, month: u32, day: u32, time: &str) -> Option<Stamp> {
    let (nanos, offset) = clock(time)?; Some(Stamp { offset, ..Stamp::new(year, month, day, nanos)? })
}

/// A date written without a pattern, `order` saying which of day and month comes first in « a/b/c »; a day and a month
/// that only fit the other way round are swapped, as pandas does.
pub(crate) fn loose(text: &str, order: Order) -> Option<Stamp> {
    if let Some(found) = regex!(r"^([0-9]{4})(?:-([0-9]{1,2})(?:-([0-9]{1,2}))?|([0-9]{2})([0-9]{2}))?((?:[T ][0-9].*)?)$").captures(text) {
        let part = |index: usize, default: u32| found.get(index).map_or(Some(default), |part| part.as_str().parse().ok());
        let (month, day) = if found.get(4).is_some() { (part(4, 1)?, part(5, 1)?) } else { (part(2, 1)?, part(3, 1)?) };
        return stamp(found[1].parse().ok()?, month, day, found.get(6).map_or("", |time| time.as_str()));
    }
    if let Some(found) = regex!(r"^([0-9]{1,4})([-/. ])([0-9]{1,2})([-/. ])([0-9]{1,4})((?:[T ][0-9].*)?)$").captures(text) {
        if found[2] != found[4] { return None; }
        let (first, second, third): (i64, u32, i64) = (found[1].parse().ok()?, found[3].parse().ok()?, found[5].parse().ok()?);
        let time = found.get(6).map_or("", |time| time.as_str());
        if found[1].len() == 4 || first > 31 { return stamp(full_year(first, found[1].len()), second, third as u32, time); }
        let year = full_year(third, found[5].len()); let first = first as u32;
        let (day, month) = if order == Order::DayFirst { (first, second) } else { (second, first) };
        return stamp(year, month, day, time).or_else(|| stamp(year, day, month, time));
    }
    if let Some(found) = regex!(r"^([0-9]{1,2})[-/]([0-9]{4})$").captures(text) { return stamp(found[2].parse().ok()?, found[1].parse().ok()?, 1, ""); }
    // Month names: « 15 Jan 2025 », « Jan 15, 2025 », « January 2025 » (its first day).
    let words: Vec<&str> = text.split(|c: char| c.is_whitespace() || c == ',' || c == '-' || c == '/' || c == '.').filter(|word| !word.is_empty()).collect();
    let month = words.iter().find_map(|word| month_name(word))?;
    let numbers: Vec<&str> = words.iter().copied().filter(|word| month_name(word).is_none()).collect();
    if numbers.iter().any(|word| !word.bytes().all(|b| b.is_ascii_digit())) || numbers.is_empty() || numbers.len() > 2 { return None; }
    let (year, day) = match numbers.as_slice() { [year] if year.len() == 4 => (year.parse().ok()?, 1), [a, b] if b.len() == 4 => (b.parse().ok()?, a.parse().ok()?), [a, b] if a.len() == 4 => (a.parse().ok()?, b.parse().ok()?), _ => return None };
    stamp(year, month, day, "")
}

/// `datetime.strptime` for the codes a format uses: %Y %y %m %d %H %M %S %f %b %B and %%; blanks match any blanks.
pub(crate) fn strptime(text: &str, pattern: &str) -> Option<Stamp> {
    thread_local! { static CACHE: std::cell::RefCell<std::collections::HashMap<String, Option<regex::Regex>>> = Default::default(); }
    let compiled = CACHE.with(|cache| cache.borrow_mut().entry(pattern.to_owned()).or_insert_with(|| {
        let (mut out, mut chars) = (String::from("(?i)^"), pattern.chars().peekable());
        while let Some(c) = chars.next() {
            if c == '%' {
                out += match chars.next()? {
                    'Y' => "(?P<Y>[0-9]{4})", 'y' => "(?P<y>[0-9]{2})", 'm' => "(?P<m>1[0-2]|0[1-9]|[1-9])", 'd' => "(?P<d>3[01]|[12][0-9]|0[1-9]|[1-9]| [1-9])",
                    'H' => "(?P<H>2[0-3]|[01][0-9]|[0-9])", 'M' => "(?P<M>[0-5][0-9]|[0-9])", 'S' => "(?P<S>6[01]|[0-5][0-9]|[0-9])", 'f' => "(?P<f>[0-9]{1,6})",
                    'b' | 'B' | 'h' => "(?P<b>[a-z]+)", '%' => "%", _ => return None,
                };
            } else if c.is_whitespace() { out += r"\s+"; } else { out += &regex::escape(&c.to_string()); }
        }
        regex::Regex::new(&(out + "$")).ok()
    }).clone());
    let found = compiled?.captures(text)?;
    let part = |name: &str| found.name(name).map(|part| part.as_str().trim().parse::<i64>().ok());
    let year = match (part("Y"), part("y")) { (Some(year), _) => year?, (None, Some(year)) => { let year = year?; if year < 69 { 2000 + year } else { 1900 + year } } _ => 1900 };
    let month = match found.name("b") { Some(name) => month_name(name.as_str())?, None => part("m").unwrap_or(Some(1))? as u32 };
    let fraction = found.name("f").map_or(0, |part| format!("{:0<9}", part.as_str()).parse::<i64>().unwrap_or(0));
    let seconds = (part("H").unwrap_or(Some(0))? * 60 + part("M").unwrap_or(Some(0))?) * 60 + part("S").unwrap_or(Some(0))?.min(59);
    Stamp::new(year, month, part("d").unwrap_or(Some(1))? as u32, seconds * 1_000_000_000 + fraction)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn text(value: &str) -> Cell { Cell::Text(value.into()) }
    fn dates(value: &str) -> Vec<String> {
        [None, Some("dmy"), Some("mdy"), Some("ymd"), Some("dd/mm/yyyy"), Some("mm/dd/yyyy"), Some("yyyy-mm-dd hh:mm"), Some("%d.%m.%y")].iter()
            .map(|format| match convert(&text(value), Some("date"), &Format::parse(format.map(JsonValue::from).as_ref())) { JsonValue::String(date) => date, _ => "None".into() }).collect()
    }
    // Each row: the value, then the date it gives with no format, dmy, mdy, ymd, dd/mm/yyyy, mm/dd/yyyy, yyyy-mm-dd hh:mm,
    // %d.%m.%y; the month (epoch ms) and the date and time with no format. As the Python worker gives them (pandas 3.0.5),
    // but for « 2025-1-5 » in dmy (pandas: 1 May) and « 2025-13-01 » (pandas in dmy: 13 January).
    #[test] fn dates_read_in_every_format_as_the_python_worker_did() { for (value, expected, month, datetime) in [ ("2025-01-15", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "2025-01-15", "None"], Some(1_735_689_600_000_i64), Some("2025-01-15T00:00:00")), ("15/01/2025", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-15T00:00:00")), ("01/15/2025", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "2025-01-15", "None", "None"], Some(1_735_689_600_000), Some("2025-01-15T00:00:00")), ("3.2.25", ["2025-02-03", "2025-02-03", "2025-03-02", "2025-03-02", "None", "None", "None", "2025-02-03"], Some(1_738_368_000_000), Some("2025-02-03T00:00:00")), ("3/2/2025", ["2025-02-03", "2025-02-03", "2025-03-02", "2025-03-02", "2025-02-03", "2025-03-02", "None", "None"], Some(1_738_368_000_000), Some("2025-02-03T00:00:00")), ("2025/01/15", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-15T00:00:00")), ("2025-01-15 10:30", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "2025-01-15", "None"], Some(1_735_689_600_000), Some("2025-01-15T10:30:00")), ("2025-01-15T10:30:00Z", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "2025-01-15", "None"], Some(1_735_689_600_000), Some("2025-01-15T10:30:00+00:00")), ("15-01-2025 10:30:15", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-15T10:30:15")), ("Jan 15 2025", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-15T00:00:00")), ("15 January 2025", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-15T00:00:00")), ("January 2025", ["2025-01-01", "2025-01-01", "2025-01-01", "2025-01-01", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-01T00:00:00")), ("2025", ["2025-01-01", "2025-01-01", "2025-01-01", "2025-01-01", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-01T00:00:00")), ("12/2018", ["2018-12-01", "2018-12-01", "2018-12-01", "2018-12-01", "None", "None", "None", "None"], Some(1_543_622_400_000), Some("2018-12-01T00:00:00")), ("2018-12", ["2018-12-01", "2018-12-01", "2018-12-01", "2018-12-01", "None", "None", "None", "None"], Some(1_543_622_400_000), Some("2018-12-01T00:00:00")), ("20250115", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-15T00:00:00")), ("13/13/2025", ["None"; 8], None, None), ("31/02/2025", ["None"; 8], None, None), ("1/2/30", ["2030-02-01", "2030-02-01", "2030-01-02", "2030-01-02", "None", "None", "None", "None"], Some(1_896_134_400_000), Some("2030-02-01T00:00:00")), ("1/2/80", ["1980-02-01", "1980-02-01", "1980-01-02", "1980-01-02", "None", "None", "None", "None"], Some(318_211_200_000), Some("1980-02-01T00:00:00")), ("2025-1-5", ["2025-01-05", "2025-01-05", "2025-01-05", "2025-01-05", "None", "None", "2025-01-05", "None"], Some(1_735_689_600_000), Some("2025-01-05T00:00:00")), ("5.1.2025", ["2025-01-05", "2025-01-05", "2025-05-01", "2025-05-01", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-05T00:00:00")), ("05/01/25", ["2025-01-05", "2025-01-05", "2025-05-01", "2025-05-01", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-05T00:00:00")), ("45000", ["None"; 8], None, None), ("15/01/2025 8:05", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-15T08:05:00")), ("2025-01-15T10:30:00.123456789", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "2025-01-15", "None"], Some(1_735_689_600_000), Some("2025-01-15T10:30:00.123456789")), ("2025-01-15T10:30:00+02:00", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "2025-01-15", "None"], Some(1_735_689_600_000), Some("2025-01-15T10:30:00+02:00")), ("", ["None"; 8], None, None), ("abc", ["None"; 8], None, None), ("Q1 2025", ["None"; 8], None, None), ("2025-13-01", ["None"; 8], None, None), ("02/2025", ["2025-02-01", "2025-02-01", "2025-02-01", "2025-02-01", "None", "None", "None", "None"], Some(1_738_368_000_000), Some("2025-02-01T00:00:00")), ("2.2025", ["None"; 8], Some(1_738_368_000_000), None), ("15.01.2025", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-15T00:00:00")), ("2025.01.15", ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15", "None", "None", "None", "None"], Some(1_735_689_600_000), Some("2025-01-15T00:00:00")), ] { assert_eq!(dates(value), expected, "{value}"); assert_eq!((convert(&text(value), Some("month"), &Format::Auto), convert(&text(value), Some("datetime"), &Format::Auto)), (month.map_or(JsonValue::Null, JsonValue::from), datetime.map_or(JsonValue::Null, JsonValue::from)), "{value}"); } }
    #[test] fn excel_dates_and_serial_numbers_are_dates() { let stamp = Stamp::new(2025, 3, 2, 0).unwrap(); assert_eq!((convert(&Cell::Stamp(stamp.clone()), Some("date"), &Format::Auto), convert(&Cell::Stamp(stamp.clone()), None, &Format::Auto), convert(&Cell::Stamp(stamp), Some("month"), &Format::Auto)), ("2025-03-02".into(), "2025-03-02 00:00:00".into(), 1_740_787_200_000_i64.into())); assert_eq!((serial(45_658.5, false).unwrap().format('T'), serial(1.0, false).unwrap().date(), serial(61.0, false).unwrap().date(), serial(0.0, true).unwrap().date()), ("2025-01-01T12:00:00".into(), "1900-01-01".into(), "1900-03-01".into(), "1904-01-01".into())); assert_eq!(convert(&Cell::Int(45_658), Some("date"), &Format::Auto), "2025-01-01"); assert_eq!(convert(&Cell::Int(202_501), Some("month"), &Format::Auto), 1_735_689_600_000_i64); }
    // As the Python worker: `_convert(value, 'number')` and `str(value)` for the string transform.
    #[test] fn numbers_read_as_the_python_worker_did() { for (value, expected) in [("1 234,56", Some(1234.56)), ("1.234,56", Some(1234.56)), ("1,234.56", Some(1234.56)), ("(120)", Some(-120.0)), ("16,2%", Some(0.162)), ("1234.56 €", Some(1234.56)), ("-5", Some(-5.0)), ("+5", Some(5.0)), (".5", Some(0.5)), ("5.", Some(5.0)), ("1e3", Some(1000.0)), ("1_000", Some(1000.0)), ("12 345", Some(12345.0)), ("1.234", Some(1.234)), ("1,5", Some(1.5)), ("0,0", Some(0.0)), ("  7 ", Some(7.0)), ("$12", Some(12.0)), ("12$", Some(12.0)), ("1\u{a0}000,5", Some(1000.5)), ("1\u{202f}000", Some(1000.0)), ("--5", None), ("1.2.3", None), ("abc", None), ("", None), ("nan", None), ("inf", None)] { assert_eq!(number(value), expected, "{value}"); } assert_eq!((convert(&Cell::Float(2.5), Some("number"), &Format::Auto), convert(&Cell::Int(3), Some("number"), &Format::Auto), convert(&Cell::Bool(true), Some("number"), &Format::Auto), convert(&Cell::Missing, Some("number"), &Format::Auto)), (2.5.into(), 3.0.into(), JsonValue::Null, JsonValue::Null)); assert_eq!((convert(&Cell::Float(2.0), Some("string"), &Format::Auto), convert(&Cell::Int(3), None, &Format::Auto), convert(&Cell::Bool(false), Some("string"), &Format::Auto)), ("2.0".into(), "3".into(), "False".into())); assert_eq!([" Oui", "1", "vrai", "TRUE", "non", "1.0", ""].map(|value| convert(&text(value), Some("boolean"), &Format::Auto)), [true, true, true, true, false, false, false].map(JsonValue::from)); assert_eq!(convert(&Cell::Float(7.0), Some("other"), &Format::Auto), 7.0); }
    #[test] fn patterns_read_tokens_and_strftime_codes() { for (format, value, expected) in [("dd/mm/yyyy", "03/02/2025", "2025-02-03"), ("d.m.yy", "3.2.25", "2025-02-03"), ("mm/dd/yyyy", "02/03/2025", "2025-02-03"), ("%d/%m/%Y", "3/2/2025", "2025-02-03"), ("dd mmm yyyy", "03 feb 2025", "2025-02-03"), ("%d %B %Y", "3 February 2025", "2025-02-03"), ("yyyymmdd", "20250203", "2025-02-03"), ("%y%m%d", "700203", "1970-02-03")] { assert_eq!(convert(&text(value), Some("date"), &Format::parse(Some(&format.into()))), expected, "{format} {value}"); } assert_eq!(convert(&text("2025-02-03 07:45"), Some("datetime"), &Format::parse(Some(&"yyyy-mm-dd hh:mm".into()))), "2025-02-03T07:45:00"); assert_eq!(convert(&text("03/2025"), Some("month"), &Format::parse(Some(&"mm/yyyy".into()))), 1_740_787_200_000_i64); }
}
