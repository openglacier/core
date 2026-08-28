//! Source span tracking.

use std::fmt;
use std::ops::Range;

/// Intervalle dans le texte source.
///
/// Les bornes sont exprimées en offsets d'octets UTF-8.
///
/// Un span est toujours valide lorsque :
///
/// ```text
/// start <= end
/// ```
///
/// Un span vide représente une position précise, par exemple la fin du texte.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Span {
    start: usize,
    end: usize,
}

impl Span {
    /// Span vide situé au début du texte.
    pub const EMPTY: Self = Self::new(0, 0);

    /// Construit un span.
    ///
    /// # Panics
    ///
    /// Panique lorsque `start > end`.
    #[must_use]
    #[inline]
    pub const fn new(start: usize, end: usize) -> Self {
        assert!(start <= end, "span start must not exceed span end");

        Self { start, end }
    }

    /// Construit un span vide à une position donnée.
    #[must_use]
    pub const fn at(offset: usize) -> Self {
        Self::new(offset, offset)
    }

    /// Retourne la borne de début.
    #[must_use]
    pub const fn start(self) -> usize {
        self.start
    }

    /// Retourne la borne de fin exclusive.
    #[must_use]
    pub const fn end(self) -> usize {
        self.end
    }

    /// Retourne la longueur du span en octets.
    #[must_use]
    #[inline]
    pub const fn len(self) -> usize {
        self.end - self.start
    }

    /// Indique si le span est vide.
    #[must_use]
    #[inline]
    pub const fn is_empty(self) -> bool {
        self.start == self.end
    }

    /// Indique si l'offset appartient au span.
    ///
    /// La borne de fin est exclusive.
    #[must_use]
    pub const fn contains(self, offset: usize) -> bool {
        self.start <= offset && offset < self.end
    }

    /// Indique si le span contient entièrement un autre span.
    #[must_use]
    pub const fn contains_span(self, other: Self) -> bool {
        self.start <= other.start && other.end <= self.end
    }

    /// Indique si deux spans se chevauchent.
    /// Deux spans adjacents ne se chevauchent pas.
    #[must_use]
    #[allow(
        clippy::suspicious_operation_groupings,
        reason = "standard overlap test for half-open ranges"
    )]
    pub const fn overlaps(self, other: Self) -> bool {
        !self.is_empty() && !other.is_empty() && self.start < other.end && other.start < self.end
    }

    /// Fusionne deux spans dans le plus petit span qui les contient.
    #[must_use]
    pub const fn join(self, other: Self) -> Self {
        Self::new(min(self.start, other.start), max(self.end, other.end))
    }

    /// Étend la borne de fin du span.
    ///
    /// # Panics
    ///
    /// Panique lorsque `end < self.start`.
    #[must_use]
    pub const fn with_end(self, end: usize) -> Self {
        Self::new(self.start, end)
    }

    /// Décale le span d'un nombre d'octets.
    ///
    /// Cette opération est utile lorsqu'un fragment de requête est lexé
    /// séparément puis replacé dans le texte source original.
    ///
    /// # Panics
    ///
    /// Panique en cas de dépassement de `usize`.
    #[must_use]
    pub const fn offset(self, amount: usize) -> Self {
        let start = match self.start.checked_add(amount) {
            Some(value) => value,
            None => panic!("span start overflow"),
        };

        let end = match self.end.checked_add(amount) {
            Some(value) => value,
            None => panic!("span end overflow"),
        };

        Self::new(start, end)
    }

    /// Retourne le span sous forme de range Rust.
    #[must_use]
    pub const fn range(self) -> Range<usize> {
        self.start..self.end
    }

    /// Retourne la tranche du texte couverte par le span.
    ///
    /// Retourne `None` lorsque les bornes ne correspondent pas à une tranche
    /// UTF-8 valide du texte.
    #[must_use]
    pub fn slice(self, source: &str) -> Option<&str> {
        source.get(self.range())
    }
}

impl fmt::Debug for Span {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}..{}", self.start, self.end)
    }
}

impl fmt::Display for Span {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}..{}", self.start, self.end)
    }
}

impl From<Range<usize>> for Span {
    fn from(range: Range<usize>) -> Self {
        Self::new(range.start, range.end)
    }
}

impl From<Span> for Range<usize> {
    fn from(span: Span) -> Self {
        span.range()
    }
}

const fn min(left: usize, right: usize) -> usize {
    if left < right {
        left
    } else {
        right
    }
}

const fn max(left: usize, right: usize) -> usize {
    if left > right {
        left
    } else {
        right
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_span() {
        let span = Span::new(2, 8);

        assert_eq!(span.start(), 2);
        assert_eq!(span.end(), 8);
        assert_eq!(span.len(), 6);
        assert!(!span.is_empty());
    }

    #[test]
    fn creates_empty_span_at_position() {
        let span = Span::at(4);

        assert_eq!(span, Span::new(4, 4));
        assert_eq!(span.len(), 0);
        assert!(span.is_empty());
    }

    #[test]
    #[should_panic(expected = "span start must not exceed span end")]
    fn rejects_reversed_span() {
        let _ = Span::new(8, 2);
    }

    #[test]
    fn contains_offset_with_exclusive_end() {
        let span = Span::new(2, 5);

        assert!(!span.contains(1));
        assert!(span.contains(2));
        assert!(span.contains(4));
        assert!(!span.contains(5));
    }

    #[test]
    fn empty_span_contains_no_offset() {
        let span = Span::at(3);

        assert!(!span.contains(3));
    }

    #[test]
    fn contains_nested_span() {
        let outer = Span::new(2, 10);

        assert!(outer.contains_span(Span::new(4, 8)));
        assert!(outer.contains_span(Span::at(10)));
        assert!(!outer.contains_span(Span::new(1, 8)));
        assert!(!outer.contains_span(Span::new(4, 11)));
    }

    #[test]
    fn detects_overlap() {
        let left = Span::new(2, 6);

        assert!(left.overlaps(Span::new(4, 8)));
        assert!(left.overlaps(Span::new(1, 3)));

        assert!(!left.overlaps(Span::new(6, 8)));
        assert!(!left.overlaps(Span::new(0, 2)));
        assert!(!left.overlaps(Span::at(4)));
    }

    #[test]
    fn joins_disjoint_spans() {
        assert_eq!(Span::new(2, 5).join(Span::new(8, 10)), Span::new(2, 10),);
    }

    #[test]
    fn joins_spans_independently_of_order() {
        let left = Span::new(8, 10);
        let right = Span::new(2, 5);

        assert_eq!(left.join(right), Span::new(2, 10));
        assert_eq!(right.join(left), Span::new(2, 10));
    }

    #[test]
    fn extends_end() {
        assert_eq!(Span::new(2, 5).with_end(8), Span::new(2, 8),);
    }

    #[test]
    fn offsets_span() {
        assert_eq!(Span::new(2, 5).offset(10), Span::new(12, 15),);
    }

    #[test]
    fn converts_to_and_from_range() {
        let span = Span::from(2..5);
        let range: Range<usize> = span.into();

        assert_eq!(span, Span::new(2, 5));
        assert_eq!(range, 2..5);
    }

    #[test]
    fn slices_ascii_source() {
        let source = "from users";
        let span = Span::new(5, 10);

        assert_eq!(span.slice(source), Some("users"));
    }

    #[test]
    fn slices_utf8_source_using_byte_offsets() {
        let source = "où âge";
        let start = source.find("âge").unwrap();
        let end = start + "âge".len();

        assert_eq!(Span::new(start, end).slice(source), Some("âge"),);
    }

    #[test]
    fn invalid_utf8_boundary_returns_none() {
        let source = "é";

        assert_eq!(Span::new(0, 1).slice(source), None);
    }

    #[test]
    fn out_of_bounds_slice_returns_none() {
        assert_eq!(Span::new(0, 20).slice("from users"), None,);
    }

    #[test]
    fn debug_and_display_are_compact() {
        let span = Span::new(2, 8);

        assert_eq!(format!("{span:?}"), "2..8");
        assert_eq!(span.to_string(), "2..8");
    }

    #[test]
    fn default_is_empty_start_span() {
        assert_eq!(Span::default(), Span::EMPTY);
    }

    #[test]
    fn span_remains_compact() {
        assert_eq!(
            std::mem::size_of::<Span>(),
            2 * std::mem::size_of::<usize>(),
        );
    }

    #[test]
    fn spans_overlap() {
        assert!(Span::new(0, 5).overlaps(Span::new(4, 8)));
        assert!(Span::new(4, 8).overlaps(Span::new(0, 5)));
        assert!(!Span::new(0, 5).overlaps(Span::new(5, 8)));
        assert!(!Span::new(0, 0).overlaps(Span::new(0, 5)));
    }
}
