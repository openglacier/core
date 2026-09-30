#![cfg_attr(rustfmt, rustfmt_skip)]
//! Calendar arithmetic behind the date functions.
//!
//! The language has no date type: a timestamp is a number of milliseconds since
//! the Unix epoch, or an ISO-8601 string. Every computation happens in UTC on
//! the proleptic Gregorian calendar, with no dependency on the host time zone.

const MS_PER_SECOND: i64 = 1_000;
const MS_PER_MINUTE: i64 = 60 * MS_PER_SECOND;
const MS_PER_HOUR: i64 = 60 * MS_PER_MINUTE;
const MS_PER_DAY: i64 = 24 * MS_PER_HOUR;

/// Calendar unit accepted by `date_trunc`, `date_add` and `date_diff`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateUnit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
}

impl DateUnit {
    /// Parses a unit name; the plural spelling is accepted too (`days`).
    pub fn parse(text: &str) -> Result<Self, String> {
        let name = text.strip_suffix('s').filter(|name| *name != "m").unwrap_or(text);
        Ok(match name {
            "year" => Self::Year,
            "quarter" => Self::Quarter,
            "month" => Self::Month,
            "week" => Self::Week,
            "day" => Self::Day,
            "hour" => Self::Hour,
            "minute" => Self::Minute,
            "second" => Self::Second,
            "millisecond" | "ms" => Self::Millisecond,
            _ => return Err(format!(
                "unknown date unit {text:?}; expected year, quarter, month, week, day, hour, minute, second or millisecond"
            )),
        })
    }

    /// Fixed length in milliseconds, for units that do not depend on the calendar.
    const fn fixed_millis(self) -> Option<i64> {
        match self {
            Self::Week => Some(7 * MS_PER_DAY),
            Self::Day => Some(MS_PER_DAY),
            Self::Hour => Some(MS_PER_HOUR),
            Self::Minute => Some(MS_PER_MINUTE),
            Self::Second => Some(MS_PER_SECOND),
            Self::Millisecond => Some(1),
            Self::Year | Self::Quarter | Self::Month => None,
        }
    }

    /// Length in months of a calendar unit.
    const fn months(self) -> Option<i64> {
        match self {
            Self::Year => Some(12),
            Self::Quarter => Some(3),
            Self::Month => Some(1),
            _ => None,
        }
    }
}

/// Broken-down UTC timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DateTime {
    pub year: i64,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub millisecond: u32,
}

impl DateTime {
    /// Splits epoch milliseconds into calendar fields.
    #[must_use]
    pub fn from_millis(millis: i64) -> Self {
        let days = millis.div_euclid(MS_PER_DAY);
        let rest = millis.rem_euclid(MS_PER_DAY);
        let (year, month, day) = civil_from_days(days);
        Self {
            year,
            month,
            day,
            hour: (rest / MS_PER_HOUR) as u32,
            minute: (rest % MS_PER_HOUR / MS_PER_MINUTE) as u32,
            second: (rest % MS_PER_MINUTE / MS_PER_SECOND) as u32,
            millisecond: (rest % MS_PER_SECOND) as u32,
        }
    }

    /// Epoch milliseconds of these calendar fields.
    #[must_use]
    pub fn to_millis(self) -> i64 {
        days_from_civil(self.year, self.month, self.day) * MS_PER_DAY
            + i64::from(self.hour) * MS_PER_HOUR
            + i64::from(self.minute) * MS_PER_MINUTE
            + i64::from(self.second) * MS_PER_SECOND
            + i64::from(self.millisecond)
    }

    fn date_only(self) -> Self {
        Self { hour: 0, minute: 0, second: 0, millisecond: 0, ..self }
    }

    fn days(self) -> i64 {
        days_from_civil(self.year, self.month, self.day)
    }

    /// ISO weekday: 1 = Monday … 7 = Sunday.
    fn iso_weekday(self) -> i64 {
        // 1970-01-01 was a Thursday.
        (self.days() + 3).rem_euclid(7) + 1
    }

    fn day_of_year(self) -> i64 {
        self.days() - days_from_civil(self.year, 1, 1) + 1
    }

    /// ISO-8601 week number (weeks start on Monday, week 1 holds the first Thursday).
    fn iso_week(self) -> i64 {
        let thursday = Self::from_millis((self.days() + 4 - self.iso_weekday()) * MS_PER_DAY);
        (thursday.day_of_year() - 1) / 7 + 1
    }
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
#[must_use]
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Civil date of a number of days since 1970-01-01.
#[must_use]
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * shifted_month + 2) / 5 + 1) as u32;
    let month = if shifted_month < 10 { shifted_month + 3 } else { shifted_month - 9 } as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Parses an ISO-8601 timestamp into epoch milliseconds.
///
/// Accepts `YYYY`, `YYYY-MM`, `YYYY-MM-DD`, optionally followed by `T` (or a
/// space) and `HH:MM[:SS[.fff]]`, then an optional `Z` or `±HH:MM` offset.
/// A timestamp without offset is UTC.
pub fn parse_iso(text: &str) -> Result<i64, String> {
    let invalid = || format!("invalid ISO-8601 date {text:?}");
    let mut cursor = Cursor { bytes: text.trim().as_bytes(), position: 0 };
    let negative = cursor.eat(b'-');
    let year = cursor.digits(4, 6).ok_or_else(invalid)?;
    let year = if negative { -year } else { year };
    let (mut month, mut day) = (1, 1);
    if cursor.eat(b'-') {
        month = cursor.digits(2, 2).ok_or_else(invalid)? as u32;
        if cursor.eat(b'-') {
            day = cursor.digits(2, 2).ok_or_else(invalid)? as u32;
        }
    }
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return Err(invalid());
    }
    let (mut hour, mut minute, mut second, mut millisecond) = (0, 0, 0, 0);
    if cursor.eat(b'T') || cursor.eat(b't') || cursor.eat(b' ') {
        hour = cursor.digits(2, 2).ok_or_else(invalid)? as u32;
        if !cursor.eat(b':') {
            return Err(invalid());
        }
        minute = cursor.digits(2, 2).ok_or_else(invalid)? as u32;
        if cursor.eat(b':') {
            second = cursor.digits(2, 2).ok_or_else(invalid)? as u32;
            if cursor.eat(b'.') || cursor.eat(b',') {
                let start = cursor.position;
                let fraction = cursor.digits(1, 9).ok_or_else(invalid)?;
                let scale = 10_i64.pow((cursor.position - start) as u32);
                millisecond = (fraction * 1_000 / scale) as u32;
            }
        }
    }
    if hour > 23 || minute > 59 || second > 59 {
        return Err(invalid());
    }
    let mut offset = 0;
    if cursor.eat(b'Z') || cursor.eat(b'z') {
    } else if let Some(sign) = cursor.peek().filter(|byte| matches!(byte, b'+' | b'-')) {
        cursor.position += 1;
        let hours = cursor.digits(2, 2).ok_or_else(invalid)?;
        cursor.eat(b':');
        let minutes = cursor.digits(2, 2).ok_or_else(invalid)?;
        offset = (hours * 60 + minutes) * MS_PER_MINUTE * if sign == b'-' { -1 } else { 1 };
    }
    if cursor.position != cursor.bytes.len() {
        return Err(invalid());
    }
    let local = DateTime { year, month, day, hour, minute, second, millisecond }.to_millis();
    Ok(local - offset)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl Cursor<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.position).copied()
    }

    fn eat(&mut self, expected: u8) -> bool {
        let matched = self.peek() == Some(expected);
        self.position += usize::from(matched);
        matched
    }

    /// Reads between `minimum` and `maximum` ASCII digits.
    fn digits(&mut self, minimum: usize, maximum: usize) -> Option<i64> {
        let start = self.position;
        while self.position - start < maximum && self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            self.position += 1;
        }
        let text = std::str::from_utf8(&self.bytes[start..self.position]).ok()?;
        (self.position - start >= minimum).then(|| text.parse().ok()).flatten()
    }
}

/// Formats epoch milliseconds with `strftime`-style directives:
/// `%Y %m %d %H %M %S %L` (milliseconds), `%j` (day of year), `%u` (ISO
/// weekday), `%V` (ISO week), `%F` (`%Y-%m-%d`), `%T` (`%H:%M:%S`) and `%%`.
pub fn format(millis: i64, pattern: &str) -> Result<String, String> {
    let date = DateTime::from_millis(millis);
    let mut output = String::with_capacity(pattern.len() + 16);
    let mut characters = pattern.chars();
    while let Some(character) = characters.next() {
        if character != '%' {
            output.push(character);
            continue;
        }
        match characters.next() {
            Some('Y') => output.push_str(&format_year(date.year)),
            Some('m') => output.push_str(&format!("{:02}", date.month)),
            Some('d') => output.push_str(&format!("{:02}", date.day)),
            Some('H') => output.push_str(&format!("{:02}", date.hour)),
            Some('M') => output.push_str(&format!("{:02}", date.minute)),
            Some('S') => output.push_str(&format!("{:02}", date.second)),
            Some('L') => output.push_str(&format!("{:03}", date.millisecond)),
            Some('j') => output.push_str(&format!("{:03}", date.day_of_year())),
            Some('u') => output.push_str(&date.iso_weekday().to_string()),
            Some('V') => output.push_str(&format!("{:02}", date.iso_week())),
            Some('F') => output.push_str(&format!("{}-{:02}-{:02}", format_year(date.year), date.month, date.day)),
            Some('T') => output.push_str(&format!("{:02}:{:02}:{:02}", date.hour, date.minute, date.second)),
            Some('%') => output.push('%'),
            Some(other) => return Err(format!("unknown date format directive %{other}")),
            None => return Err("date format ends with a lone %".to_owned()),
        }
    }
    Ok(output)
}

fn format_year(year: i64) -> String {
    if year < 0 { format!("-{:04}", -year) } else { format!("{year:04}") }
}

/// Canonical ISO-8601 rendering: `YYYY-MM-DDTHH:MM:SS.sssZ`.
#[must_use]
pub fn to_iso(millis: i64) -> String {
    format(millis, "%FT%T.%LZ").expect("static date format is valid")
}

/// Truncates to the start of the unit containing `millis` (weeks start on Monday).
pub fn trunc(millis: i64, unit: DateUnit) -> i64 {
    let date = DateTime::from_millis(millis);
    match unit {
        DateUnit::Year => DateTime { month: 1, day: 1, ..date.date_only() }.to_millis(),
        DateUnit::Quarter => DateTime { month: (date.month - 1) / 3 * 3 + 1, day: 1, ..date.date_only() }.to_millis(),
        DateUnit::Month => DateTime { day: 1, ..date.date_only() }.to_millis(),
        DateUnit::Week => (date.days() - (date.iso_weekday() - 1)) * MS_PER_DAY,
        fixed => {
            let length = fixed.fixed_millis().expect("non-calendar unit");
            millis - millis.rem_euclid(length)
        }
    }
}

/// Adds `amount` units. Adding months clamps the day to the target month
/// (`2026-01-31` + 1 month is `2026-02-28`).
pub fn add(millis: i64, amount: i64, unit: DateUnit) -> Result<i64, String> {
    let overflow = || "date_add overflows the supported range".to_owned();
    if let Some(length) = unit.fixed_millis() {
        return amount.checked_mul(length).and_then(|delta| millis.checked_add(delta)).ok_or_else(overflow);
    }
    let months = amount.checked_mul(unit.months().expect("calendar unit")).ok_or_else(overflow)?;
    let date = DateTime::from_millis(millis);
    let index = (date.year * 12 + i64::from(date.month) - 1).checked_add(months).ok_or_else(overflow)?;
    let (year, month) = (index.div_euclid(12), index.rem_euclid(12) as u32 + 1);
    Ok(DateTime { year, month, day: date.day.min(days_in_month(year, month)), ..date }.to_millis())
}

/// Number of unit boundaries crossed from `start` to `end` (negative when `end` is earlier).
pub fn diff(start: i64, end: i64, unit: DateUnit) -> i64 {
    if let Some(months) = unit.months() {
        let (from, to) = (DateTime::from_millis(start), DateTime::from_millis(end));
        let index = |date: DateTime| (date.year * 12 + i64::from(date.month) - 1).div_euclid(months);
        return index(to) - index(from);
    }
    let length = unit.fixed_millis().expect("non-calendar unit");
    (trunc(end, unit) - trunc(start, unit)) / length
}

/// Extracts one calendar field.
pub fn part(millis: i64, name: &str) -> Result<i64, String> {
    let date = DateTime::from_millis(millis);
    Ok(match name {
        "year" => date.year,
        "quarter" => i64::from((date.month - 1) / 3 + 1),
        "month" => i64::from(date.month),
        "week" => date.iso_week(),
        "day" => i64::from(date.day),
        "day_of_week" | "weekday" => date.iso_weekday(),
        "day_of_year" => date.day_of_year(),
        "hour" => i64::from(date.hour),
        "minute" => i64::from(date.minute),
        "second" => i64::from(date.second),
        "millisecond" => i64::from(date.millisecond),
        _ => return Err(format!(
            "unknown date part {name:?}; expected year, quarter, month, week, day, day_of_week, day_of_year, hour, minute, second or millisecond"
        )),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iso(text: &str) -> i64 { parse_iso(text).unwrap_or_else(|error| panic!("{error}")) }

    #[test] fn civil_round_trip_covers_leap_years_and_negative_days() { for days in [-719_468, -1, 0, 1, 10_957, 11_016, 20_000, 2_932_896] { let (year, month, day) = civil_from_days(days); assert_eq!(days_from_civil(year, month, day), days); } assert_eq!(civil_from_days(0), (1970, 1, 1)); assert_eq!(civil_from_days(11_016), (2000, 2, 29)); }
    #[test] fn parses_iso_forms_and_offsets() { assert_eq!(iso("1970-01-01"), 0); assert_eq!(iso("1970-01-01T00:00:01.5Z"), 1_500); assert_eq!(iso("2026-01"), iso("2026-01-01T00:00:00Z")); assert_eq!(iso("2026-03-10T10:00:00+02:00"), iso("2026-03-10T08:00:00Z")); assert_eq!(iso("2026-03-10 08:00"), iso("2026-03-10T08:00:00.000Z")); for bad in ["2026-13-01", "2026-02-30", "2026-01-01T25:00", "20260101", "2026-01-01T10", "x"] { assert!(parse_iso(bad).is_err(), "{bad}"); } }
    #[test] fn formats_directives() { let millis = iso("2026-03-09T07:05:03.042Z"); assert_eq!(to_iso(millis), "2026-03-09T07:05:03.042Z"); assert_eq!(format(millis, "%Y/%m/%d %j %u W%V 100%%").unwrap(), "2026/03/09 068 1 W11 100%"); assert!(format(millis, "%q").is_err()); }
    #[test] fn truncates_adds_and_diffs_on_calendar_boundaries() { let millis = iso("2026-08-13T15:42:10Z"); assert_eq!(to_iso(trunc(millis, DateUnit::Quarter)), "2026-07-01T00:00:00.000Z"); assert_eq!(to_iso(trunc(millis, DateUnit::Week)), "2026-08-10T00:00:00.000Z"); assert_eq!(to_iso(trunc(millis, DateUnit::Hour)), "2026-08-13T15:00:00.000Z"); assert_eq!(to_iso(add(iso("2026-01-31"), 1, DateUnit::Month).unwrap()), "2026-02-28T00:00:00.000Z"); assert_eq!(to_iso(add(iso("2024-02-29"), -12, DateUnit::Month).unwrap()), "2023-02-28T00:00:00.000Z"); assert_eq!(diff(iso("2025-12-31T23:59:59Z"), iso("2026-01-01"), DateUnit::Year), 1); assert_eq!(diff(iso("2026-01-01"), iso("2026-01-31T23:00:00Z"), DateUnit::Day), 30); assert_eq!(diff(iso("2026-03-01"), iso("2026-01-15"), DateUnit::Month), -2); }
    #[test] fn extracts_parts() { let millis = iso("2021-01-03T10:20:30.400Z"); assert_eq!(part(millis, "week").unwrap(), 53); assert_eq!(part(millis, "day_of_week").unwrap(), 7); assert_eq!(part(millis, "quarter").unwrap(), 1); assert_eq!(part(millis, "millisecond").unwrap(), 400); assert!(part(millis, "century").is_err()); assert_eq!(DateUnit::parse("days").unwrap(), DateUnit::Day); assert!(DateUnit::parse("fortnight").is_err()); }
}
