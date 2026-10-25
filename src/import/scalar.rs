#![cfg_attr(rustfmt, rustfmt_skip)]
//! Cells and their text as the Python worker wrote them (`magixlib.normalize_scalar`, `str()`, `repr()`): the column
//! fingerprints that find a saved mapping again, and the values of an import, depend on it to the byte.
use sha1::{Digest, Sha1};
use unicode_normalization::UnicodeNormalization;

/// A cell of a table: text for a text file, the typed value of an Excel cell.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Cell { Text(String), Int(i64), Float(f64), Bool(bool), Stamp(Stamp), Time(Stamp), Span(f64), Missing }

impl Cell {
    pub(crate) fn empty() -> Self { Self::Text(String::new()) }
    pub(crate) fn is_empty(&self) -> bool { matches!(self, Self::Text(text) if text.is_empty()) }

    /// Python's `str()` of the value; `None` for a missing one (an Excel error, read as NaN).
    pub(crate) fn py_str(&self) -> Option<String> {
        Some(match self {
            Self::Text(text) => text.clone(),
            Self::Int(value) => value.to_string(),
            Self::Float(value) => py_repr(*value),
            Self::Bool(value) => if *value { "True" } else { "False" }.to_owned(),
            Self::Stamp(stamp) => stamp.format(' '),
            Self::Time(stamp) => stamp.clock(),
            Self::Span(days) => span(*days),
            Self::Missing => return None,
        })
    }

    /// `magixlib.normalize_scalar`: what the analysis shows and the fingerprints hash.
    pub(crate) fn normalized(&self) -> String {
        match self {
            Self::Text(text) => normalize_text(text),
            Self::Int(value) => value.to_string(),
            Self::Float(value) if value.is_nan() => String::new(),
            Self::Float(value) if value.is_infinite() => if *value > 0.0 { "inf" } else { "-inf" }.to_owned(),
            Self::Float(value) => normalize_text(&format_g15(*value)),
            Self::Bool(value) => value.to_string(),
            Self::Stamp(stamp) => stamp.format('T'),
            Self::Missing => String::new(),
            other => normalize_text(&other.py_str().unwrap_or_default()),
        }
    }
}

/// A date and time: days since 1970-01-01, nanoseconds in the day, and the offset it was written with (`+02:00`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Stamp { pub days: i64, pub nanos: i64, pub offset: Option<String> }

impl Stamp {
    pub(crate) fn new(year: i64, month: u32, day: u32, nanos: i64) -> Option<Self> {
        valid_date(year, month, day).then(|| Self { days: crate::query::dates::days_from_civil(year, month, day), nanos, offset: None })
    }
    pub(crate) fn ymd(&self) -> (i64, u32, u32) { crate::query::dates::civil_from_days(self.days) }
    pub(crate) fn date(&self) -> String { let (year, month, day) = self.ymd(); format!("{year:04}-{month:02}-{day:02}") }
    /// The time of day, `HH:MM:SS` with its fraction when there is one (six digits, nine for nanoseconds).
    pub(crate) fn clock(&self) -> String {
        let seconds = self.nanos / 1_000_000_000; let fraction = self.nanos % 1_000_000_000;
        let mut out = format!("{:02}:{:02}:{:02}", seconds / 3600, seconds / 60 % 60, seconds % 60);
        if fraction % 1000 != 0 { out += &format!(".{fraction:09}"); } else if fraction != 0 { out += &format!(".{:06}", fraction / 1000); }
        out
    }
    /// `isoformat()` (`sep` 'T') or `str()` (`sep` ' ') of the Python value.
    pub(crate) fn format(&self, sep: char) -> String { format!("{}{sep}{}{}", self.date(), self.clock(), self.offset.as_deref().unwrap_or("")) }
}

pub(crate) fn valid_date(year: i64, month: u32, day: u32) -> bool {
    let days = match month { 2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29, 2 => 28, 4 | 6 | 9 | 11 => 30, 1..=12 => 31, _ => 0 };
    (1..=9999).contains(&year) && (1..=days).contains(&day)
}

/// Python's `str()` of a `timedelta` of `days` days (an Excel duration).
fn span(days: f64) -> String {
    let millis = (days * 86_400_000.0).round() as i64; let (whole, rest) = (millis.div_euclid(86_400_000), millis.rem_euclid(86_400_000));
    let clock = Stamp { days: 0, nanos: rest * 1_000_000, offset: None }.clock(); let clock = clock.strip_prefix('0').filter(|text| text.starts_with(|c: char| c.is_ascii_digit())).unwrap_or(&clock).to_owned();
    if whole == 0 { clock } else { format!("{whole} day{}, {clock}", if whole.abs() == 1 { "" } else { "s" }) }
}

/// The digits and the decimal exponent of `value` (`d.ddd × 10^exponent`), shortest or with `precision` digits after the first.
fn digits(value: f64, precision: Option<usize>) -> (String, i32) {
    let text = match precision { Some(precision) => format!("{:.*e}", precision, value.abs()), None => format!("{:e}", value.abs()) };
    let (mantissa, exponent) = text.split_once('e').expect("exponent notation");
    (mantissa.replace('.', ""), exponent.parse().expect("exponent"))
}

/// `digits` written out: plainly between 10^low and 10^high, else as Python's `1.5e+16`.
fn written(negative: bool, digits: &str, exponent: i32, low: i32, high: i32, point_zero: bool) -> String {
    let sign = if negative { "-" } else { "" }; let length = digits.len() as i32;
    if exponent < low || exponent >= high {
        let rest = &digits[1..]; return format!("{sign}{}{}{rest}e{}{:02}", &digits[..1], if rest.is_empty() { "" } else { "." }, if exponent < 0 { '-' } else { '+' }, exponent.abs());
    }
    if exponent < 0 { return format!("{sign}0.{}{digits}", "0".repeat((-exponent - 1) as usize)); }
    if exponent + 1 >= length { return format!("{sign}{digits}{}{}", "0".repeat((exponent + 1 - length) as usize), if point_zero { ".0" } else { "" }); }
    format!("{sign}{}.{}", &digits[..=exponent as usize], &digits[exponent as usize + 1..])
}

/// Python's `repr()` of a float: the shortest digits that read back the same.
pub(crate) fn py_repr(value: f64) -> String {
    if value.is_nan() { return "nan".into(); }
    if value.is_infinite() { return if value > 0.0 { "inf" } else { "-inf" }.into(); }
    let (digits, exponent) = digits(value, None); written(value.is_sign_negative(), &digits, exponent, -4, 16, true)
}

/// C's `%.15g`, as `format(value, ".15g")` writes an Excel number.
fn format_g15(value: f64) -> String {
    let (digits, exponent) = digits(value, Some(14)); let digits = digits.trim_end_matches('0'); let digits = if digits.is_empty() { "0" } else { digits };
    written(value.is_sign_negative(), digits, exponent, -4, 15, false)
}

/// Python's `str.isspace`.
pub(crate) fn py_space(c: char) -> bool { c.is_whitespace() || ('\x1c'..='\x1f').contains(&c) }
pub(crate) fn py_strip(text: &str) -> &str { text.trim_matches(py_space) }

/// Python's `str.casefold` for the letters that fold apart from lowercasing (ß, final sigma, ŉ).
pub(crate) fn casefold(text: &str) -> String {
    let lower = text.to_lowercase();
    if lower.contains(['ß', 'ς', 'ŉ']) { lower.replace('ß', "ss").replace('ς', "σ").replace('ŉ', "ʼn") } else { lower }
}

macro_rules! regex { ($pattern:expr) => {{ static RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new($pattern).expect("valid pattern")); &*RE }} }
pub(crate) use regex;

/// `magixlib.normalize_string_scalar`: NFKC, collapsed blanks, null words, booleans, numbers through `Decimal`, ISO dates.
pub(crate) fn normalize_text(text: &str) -> String {
    let text: String = if text.is_ascii() { text.to_owned() } else { text.nfkc().collect() };
    let text = text.replace("\r\n", "\n").replace('\r', "\n");
    let text = regex!("[ \t]+").replace_all(&text, " "); let text = py_strip(&text);
    if text.is_empty() { return String::new(); }
    match casefold(text).as_str() { "nan" | "na" | "n/a" | "null" | "none" | "<na>" => return String::new(), "true" | "vrai" => return "true".into(), "false" | "faux" => return "false".into(), _ => {} }
    let compact = text.replace(['\u{a0}', ' '], "");
    let numeric = if regex!("^[+-]?[0-9]+,[0-9]+$").is_match(&compact) { compact.replace(',', ".") } else if regex!(r"^[+-]?[0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]+)?$").is_match(&compact) { String::new() } else { compact };
    if regex!(r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$").is_match(&numeric) {
        if let Some(number) = decimal(&numeric) { return number; }
    }
    let candidate = text.replace('Z', "+00:00");
    if regex!("^[0-9]{4}-[0-9]{2}-[0-9]{2}$").is_match(&candidate) {
        let parts: Vec<i64> = candidate.split('-').map(|part| part.parse().unwrap_or(0)).collect();
        if valid_date(parts[0], parts[1] as u32, parts[2] as u32) { return candidate; }
    } else if let Some(found) = regex!(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})[T ]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]+))?)?(?:([+-])([0-9]{2}):([0-9]{2}))?$").captures(&candidate) {
        let number = |index: usize| found.get(index).map_or(0, |part| part.as_str().parse::<i64>().unwrap_or(0));
        let micros = found.get(7).map_or(0, |part| format!("{:0<6}", &part.as_str()[..part.len().min(6)]).parse::<i64>().unwrap_or(0));
        let offset = found.get(8).map(|sign| format!("{}{:02}:{:02}", sign.as_str(), number(9), number(10)));
        if number(4) < 24 && number(5) < 60 && number(6) < 60 && number(9) < 24 && number(10) < 60 {
            if let Some(stamp) = Stamp::new(number(1), number(2) as u32, number(3) as u32, ((number(4) * 60 + number(5)) * 60 + number(6)) * 1_000_000_000 + micros * 1000) {
                return Stamp { offset, ..stamp }.format('T');
            }
        }
    }
    text.to_owned()
}

/// `format(Decimal(text).normalize(), "f")` with its trailing zeros dropped: 28 significant digits at most, half to even.
fn decimal(text: &str) -> Option<String> {
    let (negative, text) = match text.strip_prefix('-') { Some(rest) => (true, rest), None => (false, text.strip_prefix('+').unwrap_or(text)) };
    let (mantissa, exponent) = match text.split_once(['e', 'E']) { Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().ok()?), None => (text, 0) };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut digits: Vec<u8> = whole.bytes().chain(fraction.bytes()).map(|b| b - b'0').collect();
    let mut exponent = exponent - fraction.len() as i64;
    let leading = digits.iter().take_while(|d| **d == 0).count(); digits.drain(..leading);
    if digits.is_empty() { return Some("0".into()); }
    if digits.len() > 28 {
        let dropped = digits.split_off(28); exponent += dropped.len() as i64;
        let up = dropped[0] > 5 || dropped[0] == 5 && (dropped[1..].iter().any(|d| *d != 0) || digits[27] % 2 == 1);
        if up { let mut index = digits.len(); loop { if index == 0 { digits.insert(0, 1); break; } index -= 1; if digits[index] == 9 { digits[index] = 0; } else { digits[index] += 1; break; } } }
    }
    while digits.last() == Some(&0) { digits.pop(); exponent += 1; }
    if exponent.abs() > 400 { return None; }
    let text: String = digits.iter().map(|d| char::from(b'0' + d)).collect(); let length = text.len() as i64;
    let plain = if exponent >= 0 { format!("{text}{}", "0".repeat(exponent as usize)) } else if length > -exponent { format!("{}.{}", &text[..(length + exponent) as usize], &text[(length + exponent) as usize..]) } else { format!("0.{}{text}", "0".repeat((-exponent - length) as usize)) };
    Some(if negative { format!("-{plain}") } else { plain })
}

/// `make_unique_columns`: the header cells as column names, `column_N` for an empty one, `_2`, `_3`… for a repeated one.
pub(crate) fn column_names(header: &[Cell]) -> Vec<String> {
    let mut seen = std::collections::HashMap::<String, usize>::new();
    header.iter().enumerate().map(|(index, cell)| {
        let base = column_name(&cell.normalized(), index + 1); let count = seen.entry(casefold(&base)).or_insert(0); *count += 1;
        if *count == 1 { base } else { format!("{base}_{count}") }
    }).collect()
}

fn column_name(normalized: &str, index: usize) -> String { if normalized.is_empty() { format!("column_{index}") } else { normalized.to_owned() } }

/// SHA1 of the schema (`normalize_schema_columns` then `schema_payload`): the columns normalized, folded, deduplicated
/// and sorted, as compact JSON and a newline. The same columns in another order or case give the same fingerprint.
pub(crate) fn schema_fingerprint(columns: &[String]) -> String {
    let schema: std::collections::BTreeSet<String> = columns.iter().enumerate().map(|(index, column)| {
        let name: String = column_name(&normalize_text(column), index + 1).nfkc().collect();
        casefold(py_strip(&regex!("[ \t]+").replace_all(&name, " ")))
    }).filter(|name| !name.is_empty()).collect();
    sha1_hex(format!("{}\n", serde_json::to_string(&schema).expect("strings serialize")).as_bytes())
}

pub(crate) fn sha1_hex(bytes: &[u8]) -> String { crate::helpers::hex(&Sha1::digest(bytes)) }

/// Rows scanned for the header (`HEADER_SCAN_ROWS`), and the rows after a candidate that weigh in its score.
pub(crate) const HEADER_SCAN: usize = 40;
pub(crate) const HEADER_FOLLOWING: usize = 5;

/// `detect_header_row` on normalized rows (all the same width): the first row with the best score among the first 40.
/// A header has at least two cells, distinct ones, with letters, and filled rows below it.
pub(crate) fn header_row(rows: &[Vec<String>]) -> usize {
    let mut best = (0, f64::NEG_INFINITY);
    for index in 0..rows.len().min(HEADER_SCAN) {
        let values = &rows[index]; let filled: Vec<&String> = values.iter().filter(|value| !value.is_empty()).collect();
        let score = if filled.len() < 2 { -1000.0 } else {
            let unique = filled.iter().map(|value| casefold(value)).collect::<std::collections::HashSet<_>>().len();
            let letters = filled.iter().filter(|value| value.chars().any(char::is_alphabetic)).count();
            let following = &rows[index + 1..rows.len().min(index + 1 + HEADER_FOLLOWING)];
            let size = following.len() * values.len();
            let fill = if size == 0 { 0.0 } else { following.iter().flatten().filter(|value| !value.is_empty()).count() as f64 / size as f64 };
            // A header names its columns: its cells do not come back below it in the same column, where a data row's
            // repeat (« SOCIETE » on every row of a small file all in text).
            let own = values.iter().enumerate().filter(|(column, value)| !value.is_empty() && !following.iter().any(|row| row.get(*column).is_some_and(|below| casefold(below) == casefold(value)))).count();
            filled.len() as f64 / values.len().max(1) as f64 * 4.0 + unique as f64 / filled.len() as f64 * 2.0 + letters as f64 / filled.len() as f64 * 2.0 + fill * 2.0 + own as f64 / filled.len() as f64 * 2.0
        };
        if score > best.1 { best = (index, score); }
    }
    best.0
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn text_is_normalized_as_magixlib_does() { for (text, normalized) in [("1,50", "1.5"), ("0,0", "0"), ("-0", "0"), ("1e3", "1000"), ("1.5e-3", "0.0015"), ("00012", "12"), (".5", "0.5"), ("5.", "5"), ("+5", "5"), ("1.2345678901234567890123456789012345", "1.234567890123456789012345679"), ("1.000.000", "1.000.000"), ("12,345", "12.345"), ("1 234,56", "1234.56"), ("1.234,56", "1.234,56"), ("16,2%", "16,2%"), ("１２３", "123"), ("2025-01-15T10:30:00.5", "2025-01-15T10:30:00.500000"), ("2025-01-15 10:30", "2025-01-15T10:30:00"), ("2025-01-15T10:30:00Z", "2025-01-15T10:30:00+00:00"), ("2025-01-15T10:30:00.1234567", "2025-01-15T10:30:00.123456"), ("2025-01-15 24:00", "2025-01-15 24:00"), ("2025-02-30", "2025-02-30"), ("2025-01-05Z", "2025-01-05Z"), ("NaN", ""), (" <NA> ", ""), ("Vrai", "true"), ("FAUX", "false"), ("a\tb  c", "a b c"), ("x\u{a0}y", "x y"), ("ligne\r\nsuite", "ligne\nsuite"), ("Straße", "Straße")] { assert_eq!(normalize_text(text), normalized, "{text}"); } }
    #[test] fn floats_are_written_as_python_does() { for (value, repr, normalized) in [(1.0, "1.0", "1"), (0.1, "0.1", "0.1"), (1e16, "1e+16", "10000000000000000"), (1.5e-5, "1.5e-05", "0.000015"), (1.234_567_890_123_456_8e17, "1.2345678901234568e+17", "123456789012346000"), (0.300_000_000_000_000_04, "0.30000000000000004", "0.3"), (1e-4, "0.0001", "0.0001"), (-2.5, "-2.5", "-2.5"), (1e15, "1000000000000000.0", "1000000000000000"), (123_456_789.0, "123456789.0", "123456789"), (1e22, "1e+22", "10000000000000000000000")] { assert_eq!((py_repr(value).as_str(), Cell::Float(value).normalized().as_str()), (repr, normalized), "{value}"); } }
    #[test] fn cells_print_as_python_values() { let stamp = Stamp::new(2025, 1, 15, 3_723_000_000_000).unwrap(); assert_eq!((Cell::Stamp(stamp.clone()).py_str().unwrap(), Cell::Stamp(stamp).normalized()), ("2025-01-15 01:02:03".into(), "2025-01-15T01:02:03".into())); assert_eq!((Cell::Bool(true).py_str().unwrap(), Cell::Bool(true).normalized(), Cell::Int(7).py_str().unwrap()), ("True".into(), "true".into(), "7".into())); assert_eq!((Cell::Span(1.5).py_str().unwrap(), Cell::Span(0.25).py_str().unwrap(), Cell::Missing.py_str()), ("1 day, 12:00:00".into(), "6:00:00".into(), None)); }
    #[test] fn columns_are_unique_and_fingerprinted_whatever_their_order_and_case() { assert_eq!(column_names(&[Cell::Text("Mois".into()), Cell::empty(), Cell::Text("mois".into()), Cell::Text(" Montant ".into())]), ["Mois", "column_2", "mois_2", "Montant"]); assert_eq!(schema_fingerprint(&["B".into(), "a".into(), "C".into()]), schema_fingerprint(&["A".into(), "c".into(), "b".into()])); assert_eq!(schema_fingerprint(&["a".into(), "b".into()]), sha1_hex(b"[\"a\",\"b\"]\n")); assert_eq!(casefold("STRAẞE ΣΑΣ"), "strasse σασ"); }
    #[test] fn the_header_is_the_filled_distinct_text_row() { let rows: Vec<Vec<String>> = [["Rapport", "", ""], ["", "", ""], ["Client", "Mois", "Montant"], ["A", "2025-01", "12"], ["B", "2025-02", "13"]].iter().map(|row| row.iter().map(|cell| cell.to_string()).collect()).collect(); assert_eq!(header_row(&rows), 2); assert_eq!(header_row(&[]), 0); let small: Vec<Vec<String>> = [["code", "libelle", "parent", "niveau"], ["LP", "Groupe", "", "GROUPE"], ["PLP", "La Perigourdine", "LP", "SOCIETE"], ["PPA", "Alimentaire", "LP", "SOCIETE"], ["PVD", "Vidal", "LP", "SOCIETE"], ["VOL", "Volagrain", "LP", "SOCIETE"]].iter().map(|row| row.iter().map(|cell| cell.to_string()).collect()).collect(); assert_eq!(header_row(&small), 0); }
}
