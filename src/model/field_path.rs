//! Validated document field paths.

use std::fmt;
use std::iter::FusedIterator;
use std::slice;
use std::sync::Arc;

use crate::{Document, Error, Result, Value};

/// Segment individuel d'un chemin de champ.
///
/// Un segment est une chaîne UTF-8 immuable et partageable.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldPathSegment(Arc<str>);

impl FieldPathSegment {
    /// Construit un segment sans validation syntaxique.
    ///
    /// Cette méthode est destinée aux composants qui possèdent déjà une
    /// représentation structurée du chemin, par exemple le futur parser.
    ///
    /// Un segment vide reste refusé par [`FieldPath::try_from_segments`].
    #[must_use]
    #[inline]
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    /// Retourne le contenu textuel du segment.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Retourne la chaîne partageable sous-jacente.
    #[must_use]
    pub fn as_arc(&self) -> &Arc<str> {
        &self.0
    }

    /// Consomme le segment et retourne sa chaîne partageable.
    #[must_use]
    pub fn into_arc(self) -> Arc<str> {
        self.0
    }

    /// Retourne une copie possédée du contenu textuel.
    #[must_use]
    pub fn to_owned_string(&self) -> String {
        self.0.to_string()
    }

    /// Indique si le segment est vide.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for FieldPathSegment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("FieldPathSegment")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for FieldPathSegment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for FieldPathSegment {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<&str> for FieldPathSegment {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for FieldPathSegment {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<Arc<str>> for FieldPathSegment {
    fn from(value: Arc<str>) -> Self {
        Self(value)
    }
}

/// Chemin structuré vers une valeur d'un document.
///
/// Un `FieldPath` contient toujours au moins un segment non vide.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldPath {
    segments: Arc<[FieldPathSegment]>,
}

impl FieldPath {
    /// Analyse un chemin utilisant `.` comme séparateur.
    ///
    /// # Errors
    ///
    /// Retourne :
    ///
    /// - [`Error::EmptyFieldPath`] si le chemin est vide ;
    /// - [`Error::EmptyFieldPathSegment`] si un segment est vide.
    ///
    /// # Examples
    ///
    /// ```
    /// use og_core::FieldPath;
    ///
    /// let path = FieldPath::parse("address.city")?;
    ///
    /// assert_eq!(path.len(), 2);
    /// assert_eq!(path.first().as_str(), "address");
    /// assert_eq!(path.last().as_str(), "city");
    ///
    /// # Ok::<(), og_core::Error>(())
    /// ```
    pub fn parse(path: &str) -> Result<Self> {
        if path.is_empty() {
            return Err(Error::EmptyFieldPath);
        }

        Self::try_from_segments(path.split('.').map(FieldPathSegment::from))
    }

    /// Construit un chemin depuis une suite de segments.
    ///
    /// # Errors
    ///
    /// Retourne :
    ///
    /// - [`Error::EmptyFieldPath`] si aucun segment n'est fourni ;
    /// - [`Error::EmptyFieldPathSegment`] si un segment est vide.
    pub fn try_from_segments<I, S>(segments: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<FieldPathSegment>,
    {
        let segments = segments.into_iter().map(Into::into).collect::<Vec<_>>();

        if segments.is_empty() {
            return Err(Error::EmptyFieldPath);
        }

        if let Some((index, _)) = segments
            .iter()
            .enumerate()
            .find(|(_, segment)| segment.is_empty())
        {
            return Err(Error::EmptyFieldPathSegment { index });
        }

        Ok(Self {
            segments: segments.into(),
        })
    }

    /// Retourne le nombre de segments.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Indique si le chemin ne contient aucun segment.
    ///
    /// Un `FieldPath` valide n'est jamais vide. Cette méthode est fournie pour
    /// respecter le contrat habituel associé à `len`.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Retourne le premier segment.
    #[must_use]
    pub fn first(&self) -> &FieldPathSegment {
        self.segments
            .first()
            .expect("a valid field path always contains a segment")
    }

    /// Retourne le dernier segment.
    #[must_use]
    pub fn last(&self) -> &FieldPathSegment {
        self.segments
            .last()
            .expect("a valid field path always contains a segment")
    }

    /// Retourne un segment par son index.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<&FieldPathSegment> {
        self.segments.get(index)
    }

    /// Retourne tous les segments.
    #[must_use]
    pub fn as_segments(&self) -> &[FieldPathSegment] {
        &self.segments
    }

    /// Retourne un itérateur sur les segments.
    pub fn iter(&self) -> Iter<'_> {
        Iter {
            inner: self.segments.iter(),
        }
    }

    /// Retourne le chemin parent.
    ///
    /// Un chemin composé d'un seul segment ne possède pas de parent.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        if self.len() <= 1 {
            return None;
        }

        Some(Self {
            segments: self.segments[..self.len() - 1].to_vec().into(),
        })
    }

    /// Retourne un nouveau chemin avec un segment ajouté à la fin.
    ///
    /// # Errors
    ///
    /// Retourne [`Error::EmptyFieldPathSegment`] si le segment ajouté est vide.
    pub fn joined<S>(&self, segment: S) -> Result<Self>
    where
        S: Into<FieldPathSegment>,
    {
        let segment = segment.into();

        if segment.is_empty() {
            return Err(Error::EmptyFieldPathSegment { index: self.len() });
        }

        let mut segments = self.segments.to_vec();
        segments.push(segment);

        Ok(Self {
            segments: segments.into(),
        })
    }

    /// Concatène deux chemins structurés.
    #[must_use]
    pub fn joined_path(&self, suffix: &FieldPath) -> Self {
        let mut segments = Vec::with_capacity(self.len() + suffix.len());
        segments.extend(self.segments.iter().cloned());
        segments.extend(suffix.segments.iter().cloned());

        Self {
            segments: segments.into(),
        }
    }

    /// Indique si ce chemin commence par le préfixe fourni.
    #[must_use]
    pub fn starts_with(&self, prefix: &FieldPath) -> bool {
        self.as_segments().starts_with(prefix.as_segments())
    }

    /// Retire un préfixe et retourne le suffixe restant.
    ///
    /// Retourne `None` lorsque `prefix` n'est pas un préfixe du chemin ou
    /// lorsqu'il correspond au chemin complet, car un [`FieldPath`] valide ne
    /// peut pas être vide.
    #[must_use]
    pub fn strip_prefix(&self, prefix: &FieldPath) -> Option<Self> {
        if !self.starts_with(prefix) || self.len() == prefix.len() {
            return None;
        }

        Some(Self {
            segments: self.segments[prefix.len()..].to_vec().into(),
        })
    }

    /// Résout le chemin à partir d'un document.
    ///
    /// La résolution s'arrête et retourne [`ResolvedValue::Missing`] lorsque :
    ///
    /// - un champ n'existe pas ;
    /// - un segment intermédiaire désigne une valeur qui n'est pas un objet.
    ///
    /// Une valeur [`Value::Null`] est considérée comme présente lorsqu'elle
    /// termine le chemin.
    #[must_use]
    pub fn resolve<'document>(&self, document: &'document Document) -> ResolvedValue<'document> {
        let mut segments = self.segments.iter();

        let first = segments
            .next()
            .expect("a valid field path always contains a segment");

        let Some(mut current) = document.get(first.as_str()) else {
            return ResolvedValue::Missing;
        };

        for segment in segments {
            let Some(object) = current.as_object() else {
                return ResolvedValue::Missing;
            };

            let Some(value) = object.get(segment.as_str()) else {
                return ResolvedValue::Missing;
            };

            current = value;
        }

        ResolvedValue::Present(current)
    }

    /// Résout le chemin et retourne directement la valeur lorsqu'elle existe.
    ///
    /// Cette méthode est un raccourci pour [`FieldPath::resolve`] suivi de
    /// [`ResolvedValue::into_option`].
    #[must_use]
    pub fn resolve_value<'document>(
        &self,
        document: &'document Document,
    ) -> Option<&'document Value> {
        self.resolve(document).into_option()
    }

    /// Produit la représentation textuelle pointée du chemin.
    ///
    /// Cette méthode n'effectue aucun échappement puisque la syntaxe actuelle
    /// interdit les segments vides mais n'interdit pas explicitement les points
    /// dans les segments construits programmatiquement.
    #[must_use]
    pub fn to_dotted_string(&self) -> String {
        self.to_string()
    }
}

impl fmt::Debug for FieldPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FieldPath")
            .field("segments", &self.segments)
            .finish()
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut segments = self.segments.iter();

        if let Some(first) = segments.next() {
            formatter.write_str(first.as_str())?;
        }

        for segment in segments {
            formatter.write_str(".")?;
            formatter.write_str(segment.as_str())?;
        }

        Ok(())
    }
}

impl TryFrom<&str> for FieldPath {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

impl TryFrom<String> for FieldPath {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        Self::parse(&value)
    }
}

impl From<FieldPathSegment> for FieldPath {
    fn from(segment: FieldPathSegment) -> Self {
        assert!(
            !segment.is_empty(),
            "a field path segment used directly must not be empty",
        );

        Self {
            segments: vec![segment].into(),
        }
    }
}

impl TryFrom<Vec<FieldPathSegment>> for FieldPath {
    type Error = Error;

    fn try_from(segments: Vec<FieldPathSegment>) -> Result<Self> {
        Self::try_from_segments(segments)
    }
}

impl<'a> IntoIterator for &'a FieldPath {
    type Item = &'a FieldPathSegment;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Résultat de la résolution d'un chemin.
///
/// `Missing` n'est pas une variante de [`Value`]. Il représente l'absence
/// structurelle d'une valeur dans un document.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResolvedValue<'document> {
    /// Le chemin désigne une valeur existante.
    Present(&'document Value),

    /// Le chemin ne désigne aucune valeur.
    Missing,
}

impl<'document> ResolvedValue<'document> {
    /// Indique si une valeur a été trouvée.
    #[must_use]
    pub const fn is_present(self) -> bool {
        matches!(self, Self::Present(_))
    }

    /// Indique si le chemin est absent.
    #[must_use]
    pub const fn is_missing(self) -> bool {
        matches!(self, Self::Missing)
    }

    /// Indique si le chemin désigne explicitement `null`.
    ///
    /// Une valeur manquante ne constitue pas une valeur nulle.
    #[must_use]
    pub fn is_null(self) -> bool {
        matches!(self, Self::Present(Value::Null))
    }

    /// Retourne la valeur présente ou `None` si le chemin est absent.
    #[must_use]
    pub const fn into_option(self) -> Option<&'document Value> {
        match self {
            Self::Present(value) => Some(value),
            Self::Missing => None,
        }
    }

    /// Retourne une référence vers la valeur présente.
    #[must_use]
    pub const fn as_value(&self) -> Option<&'document Value> {
        match self {
            Self::Present(value) => Some(*value),
            Self::Missing => None,
        }
    }

    /// Transforme la valeur présente avec une fonction.
    ///
    /// Une valeur manquante reste manquante.
    #[must_use]
    pub fn map<T>(self, operation: impl FnOnce(&'document Value) -> T) -> Option<T> {
        match self {
            Self::Present(value) => Some(operation(value)),
            Self::Missing => None,
        }
    }

    /// Transforme la valeur présente ou retourne une valeur par défaut.
    #[must_use]
    pub fn map_or<T>(self, default: T, operation: impl FnOnce(&'document Value) -> T) -> T {
        match self {
            Self::Present(value) => operation(value),
            Self::Missing => default,
        }
    }

    /// Transforme la valeur présente ou calcule une valeur par défaut.
    #[must_use]
    pub fn map_or_else<T>(
        self,
        default: impl FnOnce() -> T,
        operation: impl FnOnce(&'document Value) -> T,
    ) -> T {
        match self {
            Self::Present(value) => operation(value),
            Self::Missing => default(),
        }
    }

    /// Convertit la résolution en référence résultée.
    pub fn ok_or<E>(self, error: E) -> std::result::Result<&'document Value, E> {
        match self {
            Self::Present(value) => Ok(value),
            Self::Missing => Err(error),
        }
    }

    /// Convertit la résolution en référence résultée avec une erreur paresseuse.
    pub fn ok_or_else<E>(
        self,
        error: impl FnOnce() -> E,
    ) -> std::result::Result<&'document Value, E> {
        match self {
            Self::Present(value) => Ok(value),
            Self::Missing => Err(error()),
        }
    }

    /// Copie la valeur présente dans une résolution possédée.
    #[must_use]
    pub fn cloned(self) -> Option<Value> {
        self.into_option().cloned()
    }
}

impl<'document> From<ResolvedValue<'document>> for Option<&'document Value> {
    fn from(value: ResolvedValue<'document>) -> Self {
        value.into_option()
    }
}

/// Itérateur sur les segments d'un chemin.
#[derive(Debug, Clone)]
pub struct Iter<'a> {
    inner: slice::Iter<'a, FieldPathSegment>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a FieldPathSegment;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back()
    }
}

impl ExactSizeIterator for Iter<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl FusedIterator for Iter<'_> {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn user_document() -> Document {
        let address = Document::from_fields([
            ("city", Value::from("Paris")),
            ("country", Value::from("FR")),
            ("postal_code", Value::Null),
        ]);

        Document::from_fields([
            ("name", Value::from("Tom")),
            ("age", Value::from(21_i64)),
            ("nickname", Value::Null),
            ("address", Value::from(address)),
        ])
    }

    #[test]
    fn segment_preserves_its_text() {
        let segment = FieldPathSegment::new("address");

        assert_eq!(segment.as_str(), "address");
        assert_eq!(segment.to_string(), "address");
    }

    #[test]
    fn cloned_segments_share_their_allocation() {
        let original = FieldPathSegment::new("shared");
        let cloned = original.clone();

        assert!(Arc::ptr_eq(original.as_arc(), cloned.as_arc()));
    }

    #[test]
    fn parse_accepts_a_single_segment() {
        let path = FieldPath::parse("name").expect("a single segment must be valid");

        assert_eq!(path.len(), 1);
        assert_eq!(path.first().as_str(), "name");
        assert_eq!(path.last().as_str(), "name");
        assert_eq!(path.to_string(), "name");
    }

    #[test]
    fn parse_accepts_nested_segments() {
        let path = FieldPath::parse("address.city").expect("the path must be valid");

        assert_eq!(path.len(), 2);
        assert_eq!(path.get(0).map(FieldPathSegment::as_str), Some("address"));
        assert_eq!(path.get(1).map(FieldPathSegment::as_str), Some("city"));
        assert_eq!(path.to_string(), "address.city");
    }

    #[test]
    fn parse_rejects_an_empty_path() {
        let error = FieldPath::parse("").expect_err("an empty path must be rejected");

        assert_eq!(error, Error::EmptyFieldPath);
    }

    #[test]
    fn parse_rejects_an_empty_first_segment() {
        let error = FieldPath::parse(".city").expect_err("an empty first segment must be rejected");

        assert_eq!(error, Error::EmptyFieldPathSegment { index: 0 });
    }

    #[test]
    fn parse_rejects_an_empty_middle_segment() {
        let error = FieldPath::parse("address..city")
            .expect_err("an empty middle segment must be rejected");

        assert_eq!(error, Error::EmptyFieldPathSegment { index: 1 });
    }

    #[test]
    fn parse_rejects_an_empty_last_segment() {
        let error =
            FieldPath::parse("address.").expect_err("an empty last segment must be rejected");

        assert_eq!(error, Error::EmptyFieldPathSegment { index: 1 });
    }

    #[test]
    fn construction_rejects_an_empty_segment_collection() {
        let segments: Vec<FieldPathSegment> = Vec::new();

        let error = FieldPath::try_from_segments(segments)
            .expect_err("an empty collection must be rejected");

        assert_eq!(error, Error::EmptyFieldPath);
    }

    #[test]
    fn construction_rejects_a_programmatic_empty_segment() {
        let error = FieldPath::try_from_segments([
            FieldPathSegment::new("address"),
            FieldPathSegment::new(""),
        ])
        .expect_err("an empty segment must be rejected");

        assert_eq!(error, Error::EmptyFieldPathSegment { index: 1 });
    }

    #[test]
    fn iteration_preserves_segment_order() {
        let path = FieldPath::parse("user.address.city").expect("the path must be valid");

        let segments = path
            .iter()
            .map(FieldPathSegment::as_str)
            .collect::<Vec<_>>();

        assert_eq!(segments, vec!["user", "address", "city"]);
    }

    #[test]
    fn path_navigation_and_composition_are_structural() {
        let address = FieldPath::parse("user.address").expect("valid path");
        let city = address.joined("city").expect("valid segment");

        assert_eq!(city.to_string(), "user.address.city");
        assert_eq!(
            city.parent().map(|path| path.to_string()),
            Some(String::from("user.address")),
        );

        let prefix = FieldPath::parse("user").expect("valid path");
        assert!(city.starts_with(&prefix));
        assert_eq!(
            city.strip_prefix(&prefix).map(|path| path.to_string()),
            Some(String::from("address.city")),
        );

        let suffix = FieldPath::parse("postal.code").expect("valid path");
        assert_eq!(prefix.joined_path(&suffix).to_string(), "user.postal.code",);
    }

    #[test]
    fn single_segment_path_has_no_parent_or_non_empty_suffix() {
        let path = FieldPath::parse("name").expect("valid path");

        assert_eq!(path.parent(), None);
        assert_eq!(path.strip_prefix(&path), None);
    }

    #[test]
    fn joining_an_empty_segment_is_rejected_at_the_new_index() {
        let path = FieldPath::parse("address").expect("valid path");
        let error = path.joined("").expect_err("empty segment must fail");

        assert_eq!(error, Error::EmptyFieldPathSegment { index: 1 });
    }

    #[test]
    fn resolve_finds_a_top_level_value() {
        let document = user_document();
        let path = FieldPath::parse("name").expect("the path must be valid");

        assert_eq!(
            path.resolve(&document),
            ResolvedValue::Present(&Value::from("Tom"))
        );
    }

    #[test]
    fn resolve_finds_a_nested_value() {
        let document = user_document();
        let path = FieldPath::parse("address.city").expect("the path must be valid");

        assert_eq!(
            path.resolve(&document),
            ResolvedValue::Present(&Value::from("Paris"))
        );
    }

    #[test]
    fn resolve_returns_missing_for_an_absent_top_level_field() {
        let document = user_document();
        let path = FieldPath::parse("missing").expect("the path must be valid");

        assert_eq!(path.resolve(&document), ResolvedValue::Missing);
    }

    #[test]
    fn resolve_returns_missing_for_an_absent_nested_field() {
        let document = user_document();
        let path = FieldPath::parse("address.missing").expect("the path must be valid");

        assert_eq!(path.resolve(&document), ResolvedValue::Missing);
    }

    #[test]
    fn resolve_returns_missing_when_an_intermediate_value_is_not_an_object() {
        let document = user_document();
        let path = FieldPath::parse("name.first").expect("the path must be valid");

        assert_eq!(path.resolve(&document), ResolvedValue::Missing);
    }

    #[test]
    fn resolve_distinguishes_null_from_missing() {
        let document = user_document();

        let null_path = FieldPath::parse("nickname").expect("the path must be valid");
        let missing_path = FieldPath::parse("unknown").expect("the path must be valid");

        let null_value = null_path.resolve(&document);
        let missing_value = missing_path.resolve(&document);

        assert_eq!(null_value, ResolvedValue::Present(&Value::Null));
        assert!(null_value.is_present());
        assert!(null_value.is_null());
        assert!(!null_value.is_missing());

        assert_eq!(missing_value, ResolvedValue::Missing);
        assert!(!missing_value.is_present());
        assert!(!missing_value.is_null());
        assert!(missing_value.is_missing());
    }

    #[test]
    fn resolve_detects_nested_null() {
        let document = user_document();
        let path = FieldPath::parse("address.postal_code").expect("the path must be valid");

        let resolved = path.resolve(&document);

        assert!(resolved.is_present());
        assert!(resolved.is_null());
    }

    #[test]
    fn resolve_stops_when_null_is_intermediate() {
        let document = user_document();
        let path = FieldPath::parse("nickname.value").expect("the path must be valid");

        assert_eq!(path.resolve(&document), ResolvedValue::Missing);
    }

    #[test]
    fn resolve_value_returns_an_option() {
        let document = user_document();

        let city = FieldPath::parse("address.city").expect("the path must be valid");
        let missing = FieldPath::parse("address.missing").expect("the path must be valid");

        assert_eq!(city.resolve_value(&document), Some(&Value::from("Paris")));
        assert_eq!(missing.resolve_value(&document), None);
    }

    #[test]
    fn resolved_value_can_be_mapped() {
        let value = Value::from("Paris");
        let resolved = ResolvedValue::Present(&value);

        assert_eq!(resolved.map(|value| value.as_str()), Some(Some("Paris")));
    }

    #[test]
    fn resolved_value_supports_option_like_helpers() {
        let value = Value::from("Paris");
        let present = ResolvedValue::Present(&value);
        let missing = ResolvedValue::Missing;

        assert_eq!(present.map_or(0, |_| 1), 1);
        assert_eq!(missing.map_or(0, |_| 1), 0);
        assert_eq!(present.map_or_else(|| 0, |_| 1), 1);
        assert_eq!(missing.map_or_else(|| 0, |_| 1), 0);

        assert_eq!(present.ok_or("missing"), Ok(&value));
        assert_eq!(missing.ok_or("missing"), Err("missing"));
        assert_eq!(present.cloned(), Some(value.clone()));
        assert_eq!(missing.cloned(), None);
    }

    #[test]
    fn segment_can_form_a_single_segment_path() {
        let path = FieldPath::from(FieldPathSegment::new("name"));

        assert_eq!(path.len(), 1);
        assert_eq!(path.to_string(), "name");
    }

    #[test]
    fn try_from_string_parses_a_path() {
        let path =
            FieldPath::try_from(String::from("address.city")).expect("the path must be valid");

        assert_eq!(path.to_string(), "address.city");
    }

    #[test]
    fn field_path_is_send_and_sync() {
        fn assert_send_and_sync<T: Send + Sync>() {}

        assert_send_and_sync::<FieldPath>();
    }
}
