//! Compact authorization model backed by `_permissions` documents.
#![cfg_attr(rustfmt, rustfmt_skip)]
use crate::{access::auth::Principal, query::parse as parse_query};

/// Runtime authorization mode. Permissive is Compatibility mode: all operations are accepted. In Enforced mode, Every protected operation requires an authenticated principal and a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationMode { Permissive, Enforced, }

impl AuthorizationMode {
    #[must_use]
    pub const fn is_enforced(self) -> bool { matches!(self, Self::Enforced) }
    #[must_use]
    pub const fn as_str(self) -> &'static str { match self { Self::Permissive => "permissive", Self::Enforced => "enforced", } }
}

/// Declares persisted authorization actions once and derives their stable names.
macro_rules! authorization_actions {
    ($($variant:ident => $name:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum AuthorizationAction { $($variant,)+ }
        impl AuthorizationAction {
            pub const ALL: &'static [Self] = &[$(Self::$variant,)+];
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $name,)+ }
            }
        }
    };
}

authorization_actions! {
    QueryRead => "query.read",
    QueryWrite => "query.write",
    EventsSubscribe => "events.subscribe",
    IdentityManage => "identity.manage",
    DeviceManage => "device.manage",
    PermissionManage => "permission.manage",
    SharingManage => "sharing.manage",
    AppManage => "app.manage",
    CollectionsList => "collections.list",
    StorageStats => "storage.stats",
    BackupManage => "backup.manage",
}

/// One authorization check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationRequest {
    pub identity_id: String,
    pub action: AuthorizationAction,
    pub resource: String,
}

impl AuthorizationRequest {
    #[must_use]
    pub fn from_principal( principal: &Principal, action: AuthorizationAction, resource: impl Into<String>, ) -> Option<Self> {
        let Principal::Identity { identity_id, .. } = principal else {
            return None;
        };
        Some(Self {
            identity_id: identity_id.clone(),
            action,
            resource: resource.into(),
        })
    }
}

/// Query access inferred from the parsed pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryAccess { pub collection: String, pub action: AuthorizationAction, }

impl QueryAccess {
    /// Parses one query and classifies it as read or write.
    pub fn analyze(source: &str) -> Result<Self, String> {
        let pipeline = parse_query(source).map_err(|error| error.to_string())?;
        let collection = pipeline
            .source()
            .collection_name(source)
            .ok_or_else(|| "query source collection is unavailable".to_owned())?
            .to_owned();

        let mutating = pipeline.stages().iter().any(|stage| {
            stage
                .name_text(source)
                .map(str::to_ascii_lowercase)
                .is_some_and(|name| matches!(name.as_str(), "insert" | "set" | "delete" | "load"))
        });

        Ok(Self {
            collection,
            action: if mutating {
                AuthorizationAction::QueryWrite
            } else {
                AuthorizationAction::QueryRead
            },
        })
    }

    #[must_use]
    pub fn request_for(&self, principal: &Principal) -> Option<AuthorizationRequest> {
        AuthorizationRequest::from_principal(principal, self.action, self.collection.clone())
    }
}

/// Escapes one string for an internal OG query literal.
#[must_use]
pub fn quote_query_string(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '"' => output.push_str("\\\""),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            other => output.push(other),
        }
    }
    output.push('"');
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn read_only_query_is_classified_as_query_read() { let access = QueryAccess::analyze("on users | where active == true | limit 1").unwrap(); assert_eq!(access.collection, "users"); assert_eq!(access.action, AuthorizationAction::QueryRead); }
    #[test] fn mutating_query_is_classified_as_query_write() { for query in [ "on users | insert {name: Alice}", "on users | set active = true", "on users | delete", "on users | load []", ] { let access = QueryAccess::analyze(query).unwrap(); assert_eq!(access.action, AuthorizationAction::QueryWrite, "{query}"); } }
    #[test] fn anonymous_principal_cannot_build_permission_request() { let access = QueryAccess::analyze("on users").unwrap(); assert!(access.request_for(&Principal::Anonymous).is_none()); }
    #[test] fn authenticated_principal_builds_permission_request() { let access = QueryAccess::analyze("on users").unwrap(); let request = access .request_for(&Principal::Identity { identity_id: "identity-a".to_owned(), device_id: "device-a".to_owned(), }) .unwrap(); assert_eq!(request.identity_id, "identity-a"); assert_eq!(request.action, AuthorizationAction::QueryRead); assert_eq!(request.resource, "users"); }
    #[test] fn app_manage_action_is_stable() { assert_eq!(AuthorizationAction::AppManage.as_str(), "app.manage"); }
    #[test] fn query_string_escaping_is_stable() { assert_eq!(quote_query_string("a\"b\\c"), "\"a\\\"b\\\\c\""); }
}
