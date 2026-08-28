//! Shared storage validation and mutation invariants.

use super::{
    CollectionId, DocumentId, DocumentVersion, StorageError, StorageResult, VersionPrecondition,
};

pub(crate) fn validate_collection_id(value: &str) -> StorageResult<()> {
    if value.is_empty() {
        return Err(StorageError::invalid_collection_id(
            value,
            "identifier must not be empty",
        ));
    }

    for (segment_index, segment) in value.split('.').enumerate() {
        if segment.is_empty() {
            return Err(StorageError::invalid_collection_id(
                value,
                format!("segment {segment_index} must not be empty"),
            ));
        }

        let mut characters = segment.char_indices();
        let Some((_, first)) = characters.next() else {
            return Err(StorageError::invalid_collection_id(
                value,
                format!("segment {segment_index} must not be empty"),
            ));
        };

        if !is_identifier_start(first) {
            return Err(StorageError::invalid_collection_id(
                value,
                format!("segment {segment_index} must start with an alphabetic character or '_'"),
            ));
        }

        for (byte_index, character) in characters {
            if !is_identifier_continue(character) {
                return Err(StorageError::invalid_collection_id(
                    value,
                    format!(
                        "invalid character {character:?} at byte index {byte_index} in segment {segment_index}"
                    ),
                ));
            }
        }
    }

    Ok(())
}

pub(crate) fn ensure_version(
    collection: &CollectionId,
    id: &DocumentId,
    actual: DocumentVersion,
    precondition: VersionPrecondition,
) -> StorageResult<()> {
    match precondition {
        VersionPrecondition::Any => Ok(()),
        VersionPrecondition::Exact(expected) if expected == actual => Ok(()),
        VersionPrecondition::Exact(expected) => Err(StorageError::version_conflict(
            collection.clone(),
            id.clone(),
            expected,
            actual,
        )),
    }
}

pub(crate) fn increment_counter(value: u64) -> StorageResult<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| StorageError::backend("storage mutation counter overflow"))
}

pub(crate) fn next_generation(generation: u64) -> StorageResult<u64> {
    generation
        .checked_add(1)
        .ok_or_else(|| StorageError::backend("storage generation overflow"))
}

#[inline]
fn is_identifier_start(character: char) -> bool {
    character == '_' || character.is_alphabetic()
}

#[inline]
fn is_identifier_continue(character: char) -> bool {
    character == '_' || character.is_alphabetic() || character.is_ascii_digit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageErrorKind;

    fn collection(value: &str) -> CollectionId {
        CollectionId::parse(value).unwrap()
    }

    fn document_id(value: &str) -> DocumentId {
        DocumentId::from_test_label(value)
    }

    #[test]
    fn rejects_invalid_collection_segments() {
        assert!(matches!(
            validate_collection_id("users..events").unwrap_err().kind(),
            StorageErrorKind::InvalidCollectionId { .. }
        ));
    }

    #[test]
    fn version_precondition_accepts_current_version() {
        ensure_version(
            &collection("users"),
            &document_id("42"),
            DocumentVersion::new(2),
            VersionPrecondition::Exact(DocumentVersion::new(2)),
        )
        .unwrap();
    }

    #[test]
    fn version_precondition_rejects_stale_version() {
        assert!(matches!(
            ensure_version(
                &collection("users"),
                &document_id("42"),
                DocumentVersion::new(3),
                VersionPrecondition::Exact(DocumentVersion::new(2)),
            )
            .unwrap_err()
            .kind(),
            StorageErrorKind::VersionConflict { .. }
        ));
    }

    #[test]
    fn counters_detect_overflow() {
        assert!(increment_counter(u64::MAX).is_err());
        assert!(next_generation(u64::MAX).is_err());
    }
}
