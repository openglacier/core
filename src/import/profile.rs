#![cfg_attr(rustfmt, rustfmt_skip)]
//! What a column holds (`_profile` of the Python worker): its type (number, month, date, boolean, text) and its role
//! (measure, axis, attribute, time), counted in one pass over the rows of the analysis.
use super::convert::{date, month, number, Format, Order};
use super::scalar::{casefold, py_strip, regex, Cell};
use serde_json::{json, Value as JsonValue};
use std::collections::HashSet;

const ORDERS: [Order; 3] = [Order::DayFirst, Order::MonthFirst, Order::Iso];

/// The counts behind a column's profile, fed with its normalized values.
#[derive(Default)]
pub(crate) struct Profile { filled: usize, empty: usize, distinct: HashSet<String>, samples: Vec<String>, month_text: usize, months: usize, date_text: usize, year_first: bool, firsts: u32, seconds: u32, dates: [usize; 3], month_starts: [usize; 3], booleans: usize, numbers: usize, coded: bool, integers: bool, years: bool }

impl Profile {
    pub(crate) fn new() -> Self { Self { integers: true, years: true, ..Self::default() } }

    pub(crate) fn add(&mut self, value: &str) {
        let value = py_strip(value);
        if value.is_empty() { self.empty += 1; return; }
        self.filled += 1;
        if self.distinct.insert(value.to_owned()) && self.samples.len() < 5 { self.samples.push(value.to_owned()); }
        let cell = Cell::Text(value.to_owned());
        if regex!(r"^([0-9]{1,2})[-/. ]([0-9]{4})$|^([0-9]{4})[-/. ]([0-9]{1,2})$|^([0-9]{4})(0[1-9]|1[0-2])$").is_match(value) { self.month_text += 1; }
        if month(&cell, &Format::Auto).is_some() { self.months += 1; }
        if regex!(r"^[0-9]{1,4}[-/.][0-9]{1,2}[-/.][0-9]{1,4}([ T][0-9]{1,2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?(Z|[+-][0-9]{2}:?[0-9]{2})?)?$").is_match(value) { self.date_text += 1; }
        if let Some(found) = regex!(r"^([0-9]{1,4})[-/.]([0-9]{1,2})[-/.]([0-9]{1,4})").captures(value) {
            if found[1].len() == 4 { self.year_first = true; } else { self.firsts = self.firsts.max(found[1].parse().unwrap_or(0)); self.seconds = self.seconds.max(found[2].parse().unwrap_or(0)); }
        }
        // The dates in each order, as the order is known only once the whole column is read.
        // A value not starting with a digit reads the same in every order.
        let mut first = None;
        for (index, order) in ORDERS.iter().enumerate() {
            let stamp = if index > 0 && !value.starts_with(|c: char| c.is_ascii_digit()) { first.clone() } else { date(&cell, &Format::Order(*order)) };
            if let Some(stamp) = &stamp { self.dates[index] += 1; if stamp.ymd().2 == 1 { self.month_starts[index] += 1; } }
            if index == 0 { first = stamp; }
        }
        if matches!(casefold(value).as_str(), "true" | "false" | "vrai" | "faux" | "oui" | "non" | "yes" | "no") { self.booleans += 1; }
        let whole = number(value);
        if regex!(r"^[-+(]?\s*\d[\d\s\u{a0}\u{202f}.,]*\)?\s*[%€$£]?$").is_match(value) && number(value.trim_matches(['(', ')', '%', '€', '$', '£', ' '])).is_some() { self.numbers += 1; }
        let mut chars = value.chars();
        if value.chars().count() > 1 && chars.next() == Some('0') && chars.next().is_some_and(|c| c.is_numeric()) { self.coded = true; }
        if !whole.is_some_and(|whole| whole.fract() == 0.0) { self.integers = false; }
        if !whole.is_some_and(|whole| whole.fract() == 0.0 && (1900.0..=2200.0).contains(&whole)) { self.years = false; }
    }

    /// The profile as the analysis gives it: `name`, `filled`, `empty`, `distinct`, `samples`, `type`, `role`, and the
    /// `format` (order) of a date column.
    pub(crate) fn finish(&self, column: &str) -> JsonValue {
        let name = regex!(r"[_\-.]+").replace_all(column, " ");
        let mut out = json!({"name": column, "filled": self.filled, "empty": self.empty, "distinct": self.distinct.len(), "samples": self.samples});
        let (kind, role, format) = self.kind(&name);
        out["type"] = kind.into(); out["role"] = role.into();
        if let Some(format) = format { out["format"] = format.into(); }
        out
    }

    fn kind(&self, name: &str) -> (&'static str, &'static str, Option<&'static str>) {
        let words = |pattern: &regex::Regex| pattern.is_match(name);
        let month_words = words(regex!(r"(?i)\b(mois|month|periode|période|period|tperiode)\b"));
        let code_words = words(regex!(r"(?i)\b(code|id|ref|réf|reference|référence|numero|numéro|num|no|n°|siret|siren|cp|postal|zip|tel|téléphone|phone|iban|compte|account)s?\b"));
        if self.filled == 0 { return ("text", "attribute", None); }
        let share = |count: usize| count as f64 / self.filled as f64;
        if (month_words || share(self.month_text) >= 0.95) && share(self.months) >= 0.95 { return ("month", "time", None); }
        if share(self.date_text) >= 0.95 {
            let (order, format) = if self.year_first { (2, "ymd") } else if self.seconds > 12 && self.firsts <= 12 { (1, "mdy") } else { (0, "dmy") };
            if share(self.dates[order]) >= 0.95 {
                let starts = self.month_starts[order] == self.filled && !words(regex!(r"(?i)\b(date|jour|day|echeance|échéance)\b"));
                return (if starts || month_words { "month" } else { "date" }, "time", Some(format));
            }
        }
        if share(self.booleans) >= 0.98 { return ("boolean", "attribute", None); }
        if share(self.numbers) >= 0.95 {
            // Integer columns named after a business object hold its codes (« Client » 1701000050), not quantities.
            let object_words = words(regex!(r"(?i)\b(client|customer|fournisseur|supplier|vendor|article|produit|product|item|sku|societe|société|company|entite|entité|entity|site|magasin|store|agence|branch|salarie|salarié|employee|matricule|projet|project|commande|order|facture|invoice)s?\b"));
            if self.coded || code_words || self.integers && object_words { return ("text", "axis", None); }
            if words(regex!(r"(?i)\b(annee|année|year|exercice)\b")) && self.years { return ("text", "axis", None); }
            return ("number", "measure", None);
        }
        let distinct = self.distinct.len();
        let repeats = distinct < self.filled && distinct <= 2.max(self.filled / 2) || code_words;
        ("text", if repeats { "axis" } else { "attribute" }, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn profile(name: &str, values: &[&str]) -> (String, String, Option<String>) {
        let mut profile = Profile::new(); values.iter().for_each(|value| profile.add(value));
        let out = profile.finish(name); (out["type"].as_str().unwrap().into(), out["role"].as_str().unwrap().into(), out["format"].as_str().map(str::to_owned))
    }
    fn kind(name: &str, values: &[&str]) -> (String, String) { let (kind, role, _) = profile(name, values); (kind, role) }
    #[test] fn dates_give_their_order_and_months_their_type() { assert_eq!(profile("Date", &["15/01/2025", "03/02/2025", "28/02/2025"]), ("date".into(), "time".into(), Some("dmy".into()))); assert_eq!(profile("Date", &["01/15/2025", "02/28/2025"]), ("date".into(), "time".into(), Some("mdy".into()))); assert_eq!(profile("Echeance", &["2025-01-15", "2025-02-01"]), ("date".into(), "time".into(), Some("ymd".into()))); assert_eq!(profile("Valeur", &["01/01/2025", "01/02/2025", "01/03/2025"]), ("month".into(), "time".into(), Some("dmy".into()))); assert_eq!(kind("tPeriode", &["2025-01", "2025-02"]), ("month".into(), "time".into())); assert_eq!(kind("x", &["12/2018", "01/2019", "202001"]), ("month".into(), "time".into())); }
    #[test] fn numbers_are_measures_unless_codes_or_years() { assert_eq!(kind("Montant", &["1 234,56", "(120)", "16,2%", "7"]), ("number".into(), "measure".into())); assert_eq!(kind("CP", &["75001", "69002"]), ("text".into(), "axis".into())); assert_eq!(kind("Valeur", &["0123", "456"]), ("text".into(), "axis".into())); assert_eq!(kind("Client", &["1701000050", "1701000051"]), ("text".into(), "axis".into())); assert_eq!(kind("Client", &["17.5", "3"]), ("number".into(), "measure".into())); assert_eq!(kind("Année", &["2024", "2025"]), ("text".into(), "axis".into())); assert_eq!(kind("Quantité", &["2024", "2025"]), ("number".into(), "measure".into())); }
    #[test] fn texts_are_axes_when_they_repeat() { assert_eq!(kind("Actif", &["oui", "non", "Oui"]), ("boolean".into(), "attribute".into())); assert_eq!(kind("Région", &["Nord", "Sud", "Nord", "Nord"]), ("text".into(), "axis".into())); assert_eq!(kind("Libellé", &["Achat A", "Achat B", "Achat C"]), ("text".into(), "attribute".into())); assert_eq!(kind("Vide", &["", " "]), ("text".into(), "attribute".into())); let mut profile = Profile::new(); ["b", "a", "b", "", "c", "d", "e", "f"].iter().for_each(|value| profile.add(value)); assert_eq!(profile.finish("x")["samples"], json!(["b", "a", "c", "d", "e"])); }
}
