#![cfg_attr(rustfmt, rustfmt_skip)]
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
    /// The Owner reads as a profile of the Place would (its Apps and filters).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_profile: Option<String>,
}

/// Trusted execution context built by og-core after validating a request.
///
/// This is deliberately distinct from `Principal`: the principal identifies
/// who is connected, while this value identifies where one operation is
/// allowed to execute.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionContext {
    pub principal: Principal,
    pub place_id: String,
    /// Optional `AppInstance` sub-scope. `None` means the whole Place.
    pub app_instance_id: Option<String>,
    /// What grants this execution: a scope role, or the Place public policy.
    pub access: PlaceAccess,
    /// The values a field may hold in the rows the principal reads, when
    /// its Place access restricts it (`place.access.set` with `filters`).
    pub filters: Vec<(String, Vec<crate::Value>)>,
}

impl ExecutionContext {
    /// The document scope of this execution, its filters included.
    #[must_use]
    pub fn scope(&self) -> crate::query::DocumentScope {
        let scope = self.app_instance_id.as_deref().map_or_else(|| crate::query::DocumentScope::for_place(self.place_id.as_str()), |app_instance| crate::query::DocumentScope::new(self.place_id.as_str(), app_instance));
        scope.with_filters(self.filters.iter().cloned())
    }
}

/// The filters of a Place access record or profile: `{"<field>": [<values>], …}`,
/// a key `<collection>:<field>` being strict on that collection (see
/// [`crate::query::DocumentScope::enforce`]); a key that is not a field name,
/// or values that are not an array, are an error.
pub fn access_filters(filters: &serde_json::Value) -> Result<Vec<(String, Vec<crate::Value>)>, String> {
    let Some(filters) = filters.as_object() else { return if filters.is_null() { Ok(Vec::new()) } else { Err("filters must be an object of field: [values]".to_owned()) } };
    filters.iter().map(|(key, values)| {
        let (collection, field) = key.split_once(':').unwrap_or(("c", key));
        if collection.is_empty() || !collection.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
            || !(field.starts_with(|first: char| first.is_ascii_alphabetic()) && field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')) {
            return Err(format!("filter field {key:?} must be a field name"));
        }
        let values = values.as_array().ok_or_else(|| format!("filter {key:?} must list its values"))?;
        Ok((key.clone(), values.iter().map(crate::helpers::json_to_value).collect::<Result<_, _>>().map_err(|error| error.to_string())?))
    }).collect()
}

/// `filters` restricted by `more`: a field both filter keeps the values of both.
#[must_use]
pub fn intersect_filters(mut filters: Vec<(String, Vec<crate::Value>)>, more: Vec<(String, Vec<crate::Value>)>) -> Vec<(String, Vec<crate::Value>)> {
    for (field, values) in more {
        match filters.iter_mut().find(|(kept, _)| *kept == field) {
            Some((_, kept)) => kept.retain(|value| values.contains(value)),
            None => filters.push((field, values)),
        }
    }
    filters
}

/// What grants access to one Place for one principal.
///
/// A scope role and a public policy are two distinct sources: a public policy is never
/// translated into a role. Each operation checks its single action (read or write) here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaceAccess {
    /// Membership of the presented identity (Owner, Resident or Member).
    Role(PlaceRole),
    /// Public policy of the Place, for a principal outside its scope (anonymous included).
    Public(PublicAccess),
}

impl PlaceAccess {
    /// The scope role, when access comes from membership.
    #[must_use]
    pub const fn role(self) -> Option<PlaceRole> {
        match self {
            Self::Role(role) => Some(role),
            Self::Public(_) => None,
        }
    }

    /// The public policy, when access comes from it.
    #[must_use]
    pub const fn public_access(self) -> Option<PublicAccess> {
        match self {
            Self::Role(_) => None,
            Self::Public(access) => Some(access),
        }
    }

    /// Every grant allows reading Place-scoped data.
    #[must_use]
    pub const fn can_read(self) -> bool {
        true
    }

    /// Whether the write action is granted.
    #[must_use]
    pub const fn can_write(self) -> bool {
        match self {
            Self::Role(role) => role.can_write(),
            Self::Public(access) => access.can_write(),
        }
    }

    /// Administration is granted by the Owner role only, never by a public policy.
    #[must_use]
    pub const fn can_manage(self) -> bool {
        match self {
            Self::Role(role) => role.can_manage(),
            Self::Public(_) => false,
        }
    }

    /// Stable label for diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Role(role) => role.as_str(),
            Self::Public(PublicAccess::Readonly) => "public:readonly",
            Self::Public(PublicAccess::Readwrite) => "public:readwrite",
        }
    }
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

    /// Parses the stable textual representation.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "readonly" => Some(Self::Readonly),
            "readwrite" => Some(Self::Readwrite),
            _ => None,
        }
    }

    #[must_use]
    pub const fn can_write(self) -> bool {
        matches!(self, Self::Readwrite)
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

    /// The stronger of two roles.
    #[must_use]
    pub const fn strongest(self, other: Self) -> Self {
        match (self, other) {
            (Self::Owner, _) | (_, Self::Owner) => Self::Owner,
            (Self::Resident, _) | (_, Self::Resident) => Self::Resident,
            _ => Self::Member,
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

    #[test] fn place_role_rights_are_monotonic() { assert!(PlaceRole::Owner.can_manage()); assert!(PlaceRole::Owner.can_write()); assert!(!PlaceRole::Resident.can_manage()); assert!(PlaceRole::Resident.can_write()); assert!(!PlaceRole::Member.can_manage()); assert!(!PlaceRole::Member.can_write()); }
    #[test] fn public_access_maps_to_place_capabilities() { assert!(!PublicAccess::Readonly.can_write()); assert!(PublicAccess::Readwrite.can_write()); }
    #[test] fn place_access_keeps_role_and_public_policy_distinct() { let public_rw = PlaceAccess::Public(PublicAccess::Readwrite); assert!(public_rw.can_read()); assert!(public_rw.can_write()); assert!(!public_rw.can_manage()); assert_eq!(public_rw.role(), None); assert_eq!(public_rw.public_access(), Some(PublicAccess::Readwrite)); let public_ro = PlaceAccess::Public(PublicAccess::Readonly); assert!(public_ro.can_read()); assert!(!public_ro.can_write()); let member = PlaceAccess::Role(PlaceRole::Member); assert!(!member.can_write()); assert_eq!(member.public_access(), None); assert!(PlaceAccess::Role(PlaceRole::Owner).can_manage()); assert_eq!(public_ro.as_str(), "public:readonly"); }
    #[test] fn sharing_tokens_round_trip() { let token = sharing_permission("workshop", PlaceRole::Resident); assert_eq!(token, "place:workshop:resident"); assert_eq!( parse_sharing_permission(&token), Some(("workshop", PlaceRole::Resident)) ); let owner_token = sharing_permission("workshop", PlaceRole::Owner); assert_eq!(owner_token, "place:workshop:owner"); assert_eq!( parse_sharing_permission(&owner_token), Some(("workshop", PlaceRole::Owner)) ); assert!(parse_sharing_permission("files.read").is_none()); }
}
