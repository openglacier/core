//! Typed operation payloads shared by routing, authorization, and execution.

use serde::Deserialize;

use crate::access::place::{PlaceRole, PublicAccess, RequestedExecutionContext};
use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UncheckedInput;

impl<'de> Deserialize<'de> for UncheckedInput {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _ = serde::de::IgnoredAny::deserialize(deserializer)?;
        Ok(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyInput {}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PermissionGrantInput {
    pub identity_id: String,
    pub action: String,
    pub resource: String,
    #[serde(default)]
    pub created_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PermissionRevokeInput {
    pub identity_id: String,
    pub action: String,
    pub resource: String,
    #[serde(default)]
    pub revoked_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SharingCreateInput {
    pub sharing_id: String,
    pub owner: String,
    pub target: String,
    #[serde(default)]
    pub permissions: Vec<String>,
    #[serde(default = "default_sharing_state")]
    pub state: String,
    #[serde(default)]
    pub created_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SharingUpdateInput {
    pub sharing_id: String,
    #[serde(default)]
    pub permissions: Option<Vec<String>>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub updated_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SharingDeleteInput {
    pub sharing_id: String,
    #[serde(default)]
    pub deleted_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupNameInput {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupRestoreInput {
    pub name: String,
    #[serde(default)]
    pub replace: bool,
}

fn default_sharing_state() -> String {
    "accepted".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlaceCreateInput {
    pub name: String,
    #[serde(default)]
    pub mood: String,
    #[serde(default)]
    pub public_access: Option<PublicAccess>,
    #[serde(default)]
    pub created_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlaceIdInput {
    pub place_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlaceUpdateInput {
    pub place_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub subtitle: Option<String>,
    #[serde(default)]
    pub color_scheme: Option<String>,
    #[serde(default)]
    pub app_order: Option<Vec<String>>,
    #[serde(default)]
    pub updated_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlaceResourceSetInput {
    pub place_id: String,
    #[serde(alias = "nodeIdentityId")]
    pub identity_id: String,
    #[serde(alias = "nodeDeviceId", alias = "nodeId")]
    pub device_id: String,
    pub capability: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub service_role: Option<String>,
    #[serde(default)]
    pub storage_role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlaceResourceRemoveInput {
    pub place_id: String,
    #[serde(alias = "nodeIdentityId")]
    pub identity_id: String,
    #[serde(alias = "nodeDeviceId", alias = "nodeId")]
    pub device_id: String,
    pub capability: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlaceDeleteInput {
    pub place_id: String,
    #[serde(default)]
    pub deleted_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlaceAccessSetInput {
    pub place_id: String,
    pub identity_id: String,
    pub role: PlaceRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlaceAccessRemoveInput {
    pub place_id: String,
    pub identity_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlacePublicSetInput {
    pub place_id: String,
    /// `null` makes the Place private again.
    #[serde(default)]
    pub public_access: Option<PublicAccess>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppCreateInput {
    pub app_id: String,
    /// Optional Place context. When present, the Place owner may create the App
    /// and an instance is attached to that Place atomically from the client perspective.
    #[serde(default)]
    pub place_id: Option<String>,
    pub name: String,
    pub version: String,
    pub definition: serde_json::Value,
    #[serde(default)]
    pub created_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppIdInput {
    pub app_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppUpdateInput {
    pub app_id: String,
    /// Optional Place context used to authorize updates to Apps created by that Place owner.
    #[serde(default)]
    pub place_id: Option<String>,
    pub name: String,
    pub version: String,
    pub definition: serde_json::Value,
    /// Optional maintainer set. Only a Place Owner may change it.
    #[serde(default)]
    pub maintainers: Option<Vec<String>>,
    #[serde(default)]
    pub updated_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppDeleteInput {
    pub app_id: String,
    #[serde(default)]
    pub deleted_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppInstanceCreateInput {
    pub place_id: String,
    pub app_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default)]
    pub created_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppInstanceRemoveInput {
    pub instance_id: String,
    #[serde(default)]
    pub removed_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataAnalyzeInput {
    pub place_id: String,
    pub files_instance_id: String,
    pub file_id: String,
    /// Worker output supplied by the Gateway when the data.import capability
    /// lives on another node. When absent, ogd may execute its local worker.
    #[serde(default)]
    pub worker_result: Option<serde_json::Value>,
    /// Ask the control-plane Core to authorize and resolve the destination
    /// without mutating its local application storage. Used by Gateway when
    /// the Place database provider is a remote Core.
    #[serde(default)]
    pub plan_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataImportInput {
    pub place_id: String,
    pub files_instance_id: String,
    pub file_id: String,
    pub target_instance_id: String,
    pub table: String,
    pub mapping: serde_json::Value,
    #[serde(default)]
    pub mode: Option<String>,
    /// Worker output supplied by the Gateway when transformation is executed
    /// by a dedicated data.import node.
    #[serde(default)]
    pub worker_result: Option<serde_json::Value>,
    /// Ask the control-plane Core to authorize and resolve the destination
    /// without mutating its local application storage. Used by Gateway when
    /// the Place database provider is a remote Core.
    #[serde(default)]
    pub plan_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataWorkerRunInput {
    pub place_id: String,
    pub file_name: String,
    pub size: u64,
    pub operation: String,
    #[serde(default)]
    pub mapping: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataMappingSaveInput {
    pub place_id: String,
    pub fingerprint: String,
    pub name: String,
    pub target_app_id: String,
    pub target_table: String,
    pub definition: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataMappingListInput {
    pub place_id: String,
    #[serde(default)]
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataMappingUpdateInput {
    pub place_id: String,
    pub mapping_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub target_app_id: Option<String>,
    #[serde(default)]
    pub target_table: Option<String>,
    #[serde(default)]
    pub definition: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DataMappingDeleteInput {
    pub place_id: String,
    pub mapping_id: String,
}

fn default_algorithm() -> String {
    "ed25519".to_owned()
}
fn default_public_key_encoding() -> String {
    "spki-der".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryExecuteInput {
    pub query: String,
    #[serde(default)]
    pub context: Option<RequestedExecutionContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QueryContextResolveInput {
    pub place_id: String,
    #[serde(default)]
    pub app_instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthBeginInput {
    pub identity_id: String,
    pub device_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChallengeSignatureInput {
    pub challenge_id: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthEnrollBeginInput {
    pub identity_id: String,
    pub identity_public_key: String,
    pub device_id: String,
    pub device_public_key: String,
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClassicAuthRegisterInput {
    pub identifier: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClassicAuthLoginInput {
    pub identifier: String,
    pub password: String,
    pub device_id: String,
    pub public_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsSubscribeInput {
    #[serde(default)]
    pub types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentityRegisterInput {
    pub identity_id: String,
    pub public_key: String,
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
    #[serde(default = "default_public_key_encoding")]
    pub encoding: String,
    #[serde(default)]
    pub created_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentityOpenInput {
    pub file: String,
    pub password: String,
    #[serde(default)]
    pub client_device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PasswordInput {
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentityRenewInput {
    #[serde(default)]
    pub device_id: Option<String>,
    #[serde(default)]
    pub public_key: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceRegisterInput {
    pub device_id: String,
    pub identity_id: String,
    pub public_key: String,
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
    #[serde(default = "default_public_key_encoding")]
    pub encoding: String,
    #[serde(default)]
    pub created_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceRenameInput {
    pub device_id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeviceRevokeInput {
    pub device_id: String,
    #[serde(default)]
    pub revoked_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileScopeInput {
    pub place_id: String,
    pub instance_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileSyncConfigSetInput {
    pub root: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileSyncSelectionSetInput {
    pub place_id: String,
    pub instance_id: String,
    #[serde(default)]
    pub all: bool,
    #[serde(default)]
    pub folder_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileSyncSelectionRemoveInput {
    pub place_id: String,
    pub instance_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileListInput {
    pub place_id: String,
    pub instance_id: String,
    #[serde(default)]
    pub parent_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileEntryInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileMkdirInput {
    pub place_id: String,
    pub instance_id: String,
    #[serde(default)]
    pub parent_id: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileMoveInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: String,
    #[serde(default)]
    pub parent_id: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileReadInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: String,
    #[serde(default)]
    pub offset: u64,
    #[serde(default)]
    pub length: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileWriteInput {
    pub place_id: String,
    pub instance_id: String,
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileVersionReadInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: String,
    pub version_id: String,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub length: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileVersionInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: String,
    pub version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CollectionsListInput {
    #[serde(default)]
    pub stats: bool,
    #[serde(default)]
    pub place_id: Option<String>,
}

/// Validation and normalization owned by a typed operation payload.
///
/// The wire operation name is supplied by the catalog/router so shared payload
/// types can preserve operation-specific error messages without duplicating the
/// validation rules in the router.
pub trait OperationPayload {
    fn validate(&mut self, operation: &str) -> Result<()>;
}

fn invalid_payload(operation: &str, reason: impl Into<String>) -> Error {
    Error::InvalidOperationPayload {
        operation: operation.to_owned(),
        reason: reason.into(),
    }
}

fn non_empty(operation: &str, field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(invalid_payload(
            operation,
            format!("{field} must not be empty"),
        ));
    }
    Ok(())
}

fn file_scope(operation: &str, place_id: &str, instance_id: &str) -> Result<()> {
    non_empty(operation, "placeId", place_id)?;
    non_empty(operation, "instanceId", instance_id)
}

fn file_entry(operation: &str, place_id: &str, instance_id: &str, file_id: &str) -> Result<()> {
    file_scope(operation, place_id, instance_id)?;
    non_empty(operation, "fileId", file_id)
}

fn file_version(
    operation: &str,
    place_id: &str,
    instance_id: &str,
    file_id: &str,
    version_id: &str,
) -> Result<()> {
    file_entry(operation, place_id, instance_id, file_id)?;
    non_empty(operation, "versionId", version_id)
}

fn normalize_strings(values: Vec<String>) -> Vec<String> {
    let mut values: Vec<_> = values
        .into_iter()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect();
    values.sort();
    values.dedup();
    values
}

macro_rules! validate_fields {
    ($type:ty => $($field:ident : $wire:literal),+ $(,)?) => {
        impl OperationPayload for $type {
            fn validate(&mut self, operation: &str) -> Result<()> {
                $(non_empty(operation, $wire, &self.$field)?;)+
                Ok(())
            }
        }
    };
}

macro_rules! valid_payload {
    ($($type:ty),+ $(,)?) => {$ (
        impl OperationPayload for $type {
            fn validate(&mut self, _operation: &str) -> Result<()> { Ok(()) }
        }
    )+};
}

valid_payload!(UncheckedInput, EmptyInput, EventsSubscribeInput);

impl OperationPayload for CollectionsListInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        if let Some(place_id) = self.place_id.as_ref() {
            non_empty(operation, "placeId", place_id)?;
        }
        Ok(())
    }
}

validate_fields!(AuthBeginInput => identity_id: "identityId", device_id: "deviceId");
validate_fields!(ChallengeSignatureInput => challenge_id: "challengeId", signature: "signature");
validate_fields!(AuthEnrollBeginInput =>
    identity_id: "identityId", identity_public_key: "identityPublicKey",
    device_id: "deviceId", device_public_key: "devicePublicKey"
);
validate_fields!(ClassicAuthRegisterInput => identifier: "identifier", password: "password");
validate_fields!(ClassicAuthLoginInput => identifier: "identifier", password: "password", device_id: "deviceId", public_key: "publicKey");
validate_fields!(IdentityRegisterInput => identity_id: "identityId", public_key: "publicKey");
validate_fields!(PasswordInput => password: "password");
validate_fields!(DeviceRegisterInput => device_id: "deviceId", identity_id: "identityId", public_key: "publicKey");
validate_fields!(DeviceRenameInput => device_id: "deviceId", name: "name");
validate_fields!(DeviceRevokeInput => device_id: "deviceId");
validate_fields!(PermissionGrantInput => identity_id: "identityId", action: "action", resource: "resource");
validate_fields!(PermissionRevokeInput => identity_id: "identityId", action: "action", resource: "resource");
validate_fields!(SharingDeleteInput => sharing_id: "sharingId");
validate_fields!(PlaceCreateInput => name: "name");
validate_fields!(PlaceIdInput => place_id: "placeId");
validate_fields!(PlaceDeleteInput => place_id: "placeId");
impl OperationPayload for PlaceResourceSetInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "identityId", &self.identity_id)?;
        non_empty(operation, "deviceId", &self.device_id)?;
        non_empty(operation, "capability", &self.capability)?;

        for (wire, value) in [
            ("role", self.role.as_mut()),
            ("serviceRole", self.service_role.as_mut()),
            ("storageRole", self.storage_role.as_mut()),
        ] {
            if let Some(value) = value {
                *value = value.trim().to_owned();
                non_empty(operation, wire, value)?;
            }
        }

        if self.role.is_none() && self.service_role.is_none() && self.storage_role.is_none() {
            return Err(invalid_payload(
                operation,
                "one of role, serviceRole or storageRole is required",
            ));
        }

        Ok(())
    }
}
validate_fields!(PlaceResourceRemoveInput => place_id: "placeId", identity_id: "identityId", device_id: "deviceId", capability: "capability");
validate_fields!(PlaceAccessRemoveInput => place_id: "placeId", identity_id: "identityId");
validate_fields!(PlacePublicSetInput => place_id: "placeId");
validate_fields!(AppIdInput => app_id: "appId");
validate_fields!(AppDeleteInput => app_id: "appId");
validate_fields!(AppInstanceCreateInput => place_id: "placeId", app_id: "appId");
validate_fields!(AppInstanceRemoveInput => instance_id: "instanceId");
validate_fields!(BackupNameInput => name: "name");
validate_fields!(BackupRestoreInput => name: "name");

impl OperationPayload for PlaceUpdateInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        if let Some(name) = self.name.as_mut() {
            *name = name.trim().to_owned();
            non_empty(operation, "name", name)?;
        }
        if let Some(title) = self.title.as_mut() {
            *title = title.trim().to_owned();
        }
        if let Some(subtitle) = self.subtitle.as_mut() {
            *subtitle = subtitle.trim().to_owned();
        }
        if let Some(color_scheme) = self.color_scheme.as_mut() {
            *color_scheme = color_scheme.trim().to_ascii_lowercase();
            match color_scheme.as_str() {
                "glacier" | "sage" | "amber" | "graphite" => {}
                _ => {
                    return Err(invalid_payload(
                        operation,
                        "colorScheme must be glacier, sage, amber or graphite",
                    ))
                }
            }
        }
        if let Some(order) = self.app_order.as_mut() {
            order.retain(|value| !value.trim().is_empty());
            let mut seen = std::collections::HashSet::new();
            order.retain(|value| seen.insert(value.clone()));
        }
        Ok(())
    }
}

impl OperationPayload for QueryExecuteInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        if self.query.trim().is_empty() {
            return Err(invalid_payload(operation, "query must not be empty"));
        }
        if let Some(context) = self.context.as_ref() {
            non_empty(operation, "context.placeId", &context.place_id)?;
            if let Some(app_instance_id) = context.app_instance_id.as_ref() {
                non_empty(operation, "context.appInstanceId", app_instance_id)?;
            }
        }
        Ok(())
    }
}

impl OperationPayload for QueryContextResolveInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        if let Some(app_instance_id) = self.app_instance_id.as_ref() {
            non_empty(operation, "appInstanceId", app_instance_id)?;
        }
        Ok(())
    }
}

impl OperationPayload for IdentityOpenInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "file", &self.file)?;
        non_empty(operation, "password", &self.password)?;
        if let Some(device_id) = self.client_device_id.as_deref() {
            non_empty(operation, "clientDeviceId", device_id)?;
        }
        Ok(())
    }
}

impl OperationPayload for IdentityRenewInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        if self.password.as_deref().map_or(true, str::is_empty) {
            non_empty(
                operation,
                "deviceId",
                self.device_id.as_deref().unwrap_or_default(),
            )?;
            non_empty(
                operation,
                "publicKey",
                self.public_key.as_deref().unwrap_or_default(),
            )?;
        }
        Ok(())
    }
}

impl OperationPayload for SharingCreateInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "sharingId", &self.sharing_id)?;
        non_empty(operation, "owner", &self.owner)?;
        non_empty(operation, "target", &self.target)?;
        if self.owner == self.target {
            return Err(invalid_payload(operation, "owner and target must differ"));
        }
        self.permissions = normalize_strings(std::mem::take(&mut self.permissions));
        Ok(())
    }
}

impl OperationPayload for SharingUpdateInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "sharingId", &self.sharing_id)?;
        if self.permissions.is_none() && self.state.as_deref().is_none() {
            return Err(invalid_payload(
                operation,
                "permissions or state is required",
            ));
        }
        self.permissions = self.permissions.take().map(normalize_strings);
        Ok(())
    }
}

impl OperationPayload for PlaceAccessSetInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "identityId", &self.identity_id)?;
        Ok(())
    }
}

macro_rules! app_definition_payload {
    ($type:ty) => {
        impl OperationPayload for $type {
            fn validate(&mut self, operation: &str) -> Result<()> {
                non_empty(operation, "appId", &self.app_id)?;
                non_empty(operation, "name", &self.name)?;
                non_empty(operation, "version", &self.version)?;
                if !self.definition.is_object() {
                    return Err(invalid_payload(operation, "definition must be an object"));
                }
                Ok(())
            }
        }
    };
}
app_definition_payload!(AppCreateInput);
app_definition_payload!(AppUpdateInput);
impl OperationPayload for DataAnalyzeInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "filesInstanceId", &self.files_instance_id)?;
        non_empty(operation, "fileId", &self.file_id)?;
        if self
            .worker_result
            .as_ref()
            .is_some_and(|value| !value.is_object())
        {
            return Err(invalid_payload(operation, "workerResult must be an object"));
        }
        Ok(())
    }
}
impl OperationPayload for DataImportInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "filesInstanceId", &self.files_instance_id)?;
        non_empty(operation, "fileId", &self.file_id)?;
        non_empty(operation, "targetInstanceId", &self.target_instance_id)?;
        non_empty(operation, "table", &self.table)?;
        if !self.mapping.is_object() {
            return Err(invalid_payload(operation, "mapping must be an object"));
        }
        if self
            .worker_result
            .as_ref()
            .is_some_and(|value| !value.is_object())
        {
            return Err(invalid_payload(operation, "workerResult must be an object"));
        }
        Ok(())
    }
}
impl OperationPayload for DataWorkerRunInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "fileName", &self.file_name)?;
        non_empty(operation, "operation", &self.operation)?;
        if self.operation != "analyze" && self.operation != "import" {
            return Err(invalid_payload(
                operation,
                "operation must be analyze or import",
            ));
        }
        if self
            .mapping
            .as_ref()
            .is_some_and(|value| !value.is_object())
        {
            return Err(invalid_payload(operation, "mapping must be an object"));
        }
        Ok(())
    }
}
impl OperationPayload for DataMappingSaveInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "fingerprint", &self.fingerprint)?;
        non_empty(operation, "name", &self.name)?;
        non_empty(operation, "targetAppId", &self.target_app_id)?;
        non_empty(operation, "targetTable", &self.target_table)?;
        if !self.definition.is_object() {
            return Err(invalid_payload(operation, "definition must be an object"));
        }
        Ok(())
    }
}
impl OperationPayload for DataMappingListInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        if let Some(fingerprint) = &self.fingerprint {
            non_empty(operation, "fingerprint", fingerprint)?;
        }
        Ok(())
    }
}

impl OperationPayload for DataMappingUpdateInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "mappingId", &self.mapping_id)?;
        if let Some(name) = &self.name {
            non_empty(operation, "name", name)?;
        }
        if let Some(app_id) = &self.target_app_id {
            non_empty(operation, "targetAppId", app_id)?;
        }
        if let Some(table) = &self.target_table {
            non_empty(operation, "targetTable", table)?;
        }
        if self
            .definition
            .as_ref()
            .is_some_and(|value| !value.is_object())
        {
            return Err(invalid_payload(operation, "definition must be an object"));
        }
        if self.name.is_none()
            && self.target_app_id.is_none()
            && self.target_table.is_none()
            && self.definition.is_none()
        {
            return Err(invalid_payload(
                operation,
                "at least one mapping field must be provided",
            ));
        }
        Ok(())
    }
}

impl OperationPayload for DataMappingDeleteInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "mappingId", &self.mapping_id)
    }
}

impl OperationPayload for FileScopeInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_scope(operation, &self.place_id, &self.instance_id)
    }
}
impl OperationPayload for FileSyncConfigSetInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        self.root = self.root.trim().to_owned();
        non_empty(operation, "root", &self.root)
    }
}
impl OperationPayload for FileSyncSelectionSetInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_scope(operation, &self.place_id, &self.instance_id)?;
        for folder_id in &mut self.folder_ids {
            *folder_id = folder_id.trim().to_owned();
            non_empty(operation, "folderIds", folder_id)?;
        }
        self.folder_ids.sort();
        self.folder_ids.dedup();
        if self.all {
            self.folder_ids.clear();
        } else if self.folder_ids.is_empty() {
            return Err(invalid_payload(
                operation,
                "folderIds must contain at least one folder when all is false",
            ));
        }
        Ok(())
    }
}
impl OperationPayload for FileSyncSelectionRemoveInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_scope(operation, &self.place_id, &self.instance_id)
    }
}
impl OperationPayload for FileListInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_scope(operation, &self.place_id, &self.instance_id)
    }
}
impl OperationPayload for FileEntryInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_entry(operation, &self.place_id, &self.instance_id, &self.file_id)
    }
}
impl OperationPayload for FileMkdirInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_scope(operation, &self.place_id, &self.instance_id)?;
        non_empty(operation, "name", &self.name)
    }
}
impl OperationPayload for FileMoveInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_entry(operation, &self.place_id, &self.instance_id, &self.file_id)?;
        non_empty(operation, "name", &self.name)
    }
}
impl OperationPayload for FileReadInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_entry(operation, &self.place_id, &self.instance_id, &self.file_id)?;
        if matches!(self.length, Some(0)) {
            return Err(invalid_payload(
                operation,
                "length must be greater than zero",
            ));
        }
        Ok(())
    }
}
impl OperationPayload for FileWriteInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_scope(operation, &self.place_id, &self.instance_id)?;
        if self.file_id.as_deref().is_some_and(str::is_empty) {
            return Err(invalid_payload(operation, "fileId must not be empty"));
        }
        if self.parent_id.as_deref().is_some_and(str::is_empty) {
            return Err(invalid_payload(operation, "parentId must not be empty"));
        }
        if self.file_id.is_none() && self.name.as_deref().is_none_or(str::is_empty) {
            return Err(invalid_payload(
                operation,
                "name is required when creating a file",
            ));
        }
        Ok(())
    }
}
impl OperationPayload for FileVersionReadInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_version(
            operation,
            &self.place_id,
            &self.instance_id,
            &self.file_id,
            &self.version_id,
        )
    }
}
impl OperationPayload for FileVersionInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_version(
            operation,
            &self.place_id,
            &self.instance_id,
            &self.file_id,
            &self.version_id,
        )
    }
}
