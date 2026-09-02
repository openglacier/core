//! Place roles and stable sharing permission tokens.
//!
//! A Place is a system-owned usage and security context. The Owner is stored on
//! the `_places` record as the immutable primary Owner. Additional Owners,
//! Residents and Members are represented through the existing `_sharings`
//! collection using stable permission tokens.

use serde::{Deserialize, Serialize};

use crate::access::auth::Principal;

/// Untrusted Place/App instance context requested by a client operation.
///
/// The identifiers are transport data only. They MUST be validated against
/// persisted Place membership and App instance ownership before they are used
/// as an execution scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestedExecutionContext {
    pub place_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_instance_id: Option<String>,
}

/// Trusted execution context built by og-core after validating a request.
///
/// This is deliberately distinct from `Principal`: the principal identifies
/// who is connected, while this value identifies where one operation is
/// allowed to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionContext {
    pub principal: Principal,
    pub place_id: String,
    /// Optional AppInstance sub-scope. `None` means the whole Place.
    pub app_instance_id: Option<String>,
    pub place_role: PlaceRole,
    /// Public access mode used when an anonymous connection enters a public Place.
    pub public_access: Option<PublicAccess>,
}

/// Anonymous access exposed by a public Place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PublicAccess {
    /// Anonymous readers may inspect Place-scoped data but cannot mutate it.
    Readonly,
    /// Anonymous readers may read and mutate Place-scoped data.
    Readwrite,
}

impl PublicAccess {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Readonly => "readonly",
            Self::Readwrite => "readwrite",
        }
    }

    #[must_use]
    pub const fn can_write(self) -> bool {
        matches!(self, Self::Readwrite)
    }

    #[must_use]
    pub const fn place_role(self) -> PlaceRole {
        match self {
            Self::Readonly => PlaceRole::Member,
            Self::Readwrite => PlaceRole::Resident,
        }
    }
}

/// Human-facing access role inside one Place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaceRole {
    /// Full control, including App attachment and Place deletion.
    Owner,
    /// Read/write access within the Place.
    Resident,
    /// Read-only access within the Place.
    Member,
}

impl PlaceRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Resident => "resident",
            Self::Member => "member",
        }
    }

    /// Parses the stable textual representation used by Place access records.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "owner" => Some(Self::Owner),
            "resident" => Some(Self::Resident),
            "member" => Some(Self::Member),
            _ => None,
        }
    }

    /// Returns whether this role permits writes inside the Place.
    #[must_use]
    pub const fn can_write(self) -> bool {
        matches!(self, Self::Owner | Self::Resident)
    }

    /// Returns whether this role permits Place administration.
    #[must_use]
    pub const fn can_manage(self) -> bool {
        matches!(self, Self::Owner)
    }
}

/// Stable sharing token used to grant one role on one Place.
#[must_use]
pub fn sharing_permission(place_id: &str, role: PlaceRole) -> String {
    format!("place:{place_id}:{}", role.as_str())
}

/// Parses one stable Place sharing token.
#[must_use]
pub fn parse_sharing_permission(value: &str) -> Option<(&str, PlaceRole)> {
    let rest = value.strip_prefix("place:")?;
    let (place_id, role) = rest.rsplit_once(':')?;
    if place_id.is_empty() {
        return None;
    }
    let role = PlaceRole::parse(role)?;
    Some((place_id, role))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn place_role_rights_are_monotonic() {
        assert!(PlaceRole::Owner.can_manage());
        assert!(PlaceRole::Owner.can_write());
        assert!(!PlaceRole::Resident.can_manage());
        assert!(PlaceRole::Resident.can_write());
        assert!(!PlaceRole::Member.can_manage());
        assert!(!PlaceRole::Member.can_write());
    }

    #[test]
    fn public_access_maps_to_place_capabilities() {
        assert!(!PublicAccess::Readonly.can_write());
        assert!(PublicAccess::Readwrite.can_write());
        assert_eq!(PublicAccess::Readonly.place_role(), PlaceRole::Member);
        assert_eq!(PublicAccess::Readwrite.place_role(), PlaceRole::Resident);
    }

    #[test]
    fn sharing_tokens_round_trip() {
        let token = sharing_permission("workshop", PlaceRole::Resident);
        assert_eq!(token, "place:workshop:resident");
        assert_eq!(
            parse_sharing_permission(&token),
            Some(("workshop", PlaceRole::Resident))
        );
        let owner_token = sharing_permission("workshop", PlaceRole::Owner);
        assert_eq!(owner_token, "place:workshop:owner");
        assert_eq!(
            parse_sharing_permission(&owner_token),
            Some(("workshop", PlaceRole::Owner))
        );
        assert!(parse_sharing_permission("files.read").is_none());
    }
}
