#![cfg_attr(rustfmt, rustfmt_skip)]
//! Typed operation payloads shared by routing, authorization, and execution.

use serde::{Deserialize, Serialize};

use crate::access::place::{PlaceRole, PublicAccess, RequestedExecutionContext};
use crate::error::{Error, Result};

/// Input payloads: deserialized from camelCase JSON, unknown fields refused.
macro_rules! inputs { ($($(#[$meta:meta])* pub struct $name:ident $body:tt)*) => { $( #[derive(Debug, Clone, PartialEq, Eq, Deserialize)] #[serde(rename_all = "camelCase", deny_unknown_fields)] $(#[$meta])* pub struct $name $body )* } }

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

inputs! {
pub struct PermissionGrantInput {
    pub identity_id: String,
    pub action: String,
    pub resource: String,
    pub created_at: Option<u64>,
}
pub struct PermissionRevokeInput {
    pub identity_id: String,
    pub action: String,
    pub resource: String,
    pub revoked_at: Option<u64>,
}
pub struct SharingCreateInput {
    pub sharing_id: String,
    pub owner: String,
    pub target: String,
    #[serde(default)]
    pub permissions: Vec<String>,
    #[serde(default = "default_sharing_state")]
    pub state: String,
    pub created_at: Option<u64>,
}
pub struct SharingUpdateInput {
    pub sharing_id: String,
    pub permissions: Option<Vec<String>>,
    pub state: Option<String>,
    pub updated_at: Option<u64>,
}
pub struct SharingDeleteInput {
    pub sharing_id: String,
    pub deleted_at: Option<u64>,
}
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupNameInput {
    pub name: String,
}

/// `backup.create`: a full backup (with `history`, also the history the store keeps), or with `parent` a diff
/// from that backup.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupCreateInput {
    pub name: String,
    pub parent: Option<String>,
    #[serde(default)]
    pub history: bool,
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

inputs! {
pub struct PlaceCreateInput {
    pub name: String,
    #[serde(default)]
    pub mood: String,
    pub public_access: Option<PublicAccess>,
    pub created_at: Option<u64>,
}
pub struct PlaceIdInput {
    pub place_id: String,
}
/// `place.search`: `text` searched through what the Place holds, within every key of `scope` (`app:<instanceId>`,
/// `collection:<name>`, `field:<collection>.<field>`, `folder:<fileId>`), at most `limit` hits (20 by default).
pub struct PlaceSearchInput {
    pub place_id: String,
    pub text: String,
    #[serde(default)]
    pub scope: Vec<String>,
    pub limit: Option<usize>,
}
pub struct PlaceUpdateInput {
    pub place_id: String,
    pub name: Option<String>,
    pub title: Option<String>,
    pub subtitle: Option<String>,
    pub color_scheme: Option<String>,
    pub app_order: Option<Vec<String>>,
    /// The Place's Apps in order, some gathered in folders (one level).
    pub app_layout: Option<Vec<AppLayoutEntry>>,
    /// The Place's profiles, all of them (a profile still given to a member stays).
    pub profiles: Option<Vec<PlaceProfile>>,
    pub tile_size: Option<String>,
    pub updated_at: Option<u64>,
}
}

/// One entry of a Place's App layout: an App instance, or a folder of instances.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum AppLayoutEntry {
    Instance(String),
    Folder(AppFolder),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AppFolder {
    pub folder: String,
    pub name: String,
    #[serde(default)]
    pub items: Vec<String>,
}

/// A profile of a Place, as its Owner sets it: the App instances its members
/// see (`None`: all) and the rows they read (`filters` as for Place access).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlaceProfile {
    pub profile_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apps: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filters: Option<serde_json::Value>,
}

pub const APP_FOLDER_NAME_MAX: usize = 60;
pub const APP_FOLDERS_MAX: usize = 50;

impl AppLayoutEntry {
    /// The instances in layout order, folders opened in place.
    pub fn flatten(layout: &[AppLayoutEntry]) -> Vec<String> {
        layout.iter().flat_map(|entry| match entry {
            AppLayoutEntry::Instance(id) => vec![id.clone()],
            AppLayoutEntry::Folder(folder) => folder.items.clone(),
        }).collect()
    }

    /// Keeps only the instances `keep` accepts; folders stay, even empty.
    pub fn retain(layout: &mut Vec<AppLayoutEntry>, mut keep: impl FnMut(&str) -> bool) {
        layout.retain_mut(|entry| match entry {
            AppLayoutEntry::Instance(id) => keep(id),
            AppLayoutEntry::Folder(folder) => { folder.items.retain(|id| keep(id)); true }
        });
    }
}

inputs! {
pub struct PlaceResourceSetInput {
    pub place_id: String,
    #[serde(alias = "nodeIdentityId")]
    pub identity_id: String,
    #[serde(alias = "nodeDeviceId", alias = "nodeId")]
    pub device_id: String,
    pub capability: String,
    pub role: Option<String>,
    pub service_role: Option<String>,
    pub storage_role: Option<String>,
}
pub struct PlaceResourceRemoveInput {
    pub place_id: String,
    #[serde(alias = "nodeIdentityId")]
    pub identity_id: String,
    #[serde(alias = "nodeDeviceId", alias = "nodeId")]
    pub device_id: String,
    pub capability: String,
}
pub struct FabricResourceSetInput {
    pub identity_id: String,
    pub device_id: String,
    pub capability: String,
    pub role: Option<String>,
    pub service_role: Option<String>,
    pub storage_role: Option<String>,
}
pub struct FabricResourceRemoveInput {
    pub identity_id: String,
    pub device_id: String,
    pub capability: String,
}
pub struct PlaceDeleteInput {
    pub place_id: String,
    pub deleted_at: Option<u64>,
}
pub struct PlaceAccessSetInput {
    pub place_id: String,
    pub identity_id: String,
    pub role: PlaceRole,
    /// `{"<field>": [<values>], …}`: the member reads only the rows whose
    /// field, when present, holds one of the values; `{}` lifts them.
    pub filters: Option<serde_json::Value>,
    /// The profile of the Place the member gets; `""` takes it away.
    pub profile_id: Option<String>,
}
pub struct PlaceAccessRemoveInput {
    pub place_id: String,
    pub identity_id: String,
}
pub struct PlacePublicSetInput {
    pub place_id: String,
    /// `null` makes the Place private again.
    pub public_access: Option<PublicAccess>,
}
pub struct AppCreateInput {
    pub app_id: String,
    /// Optional Place context. When present, the Place owner may create the App
    /// and an instance is attached to that Place atomically from the client perspective.
    pub place_id: Option<String>,
    pub name: String,
    pub version: String,
    pub definition: serde_json::Value,
    /// `private` (the default: only its creator and maintainers see and install it) or
    /// `public` (the catalog, everyone); publishing requires the global App permission.
    pub visibility: Option<String>,
    pub created_at: Option<u64>,
}
pub struct AppIdInput {
    pub app_id: String,
}
pub struct AppUpdateInput {
    pub app_id: String,
    /// Optional Place context used to authorize updates to Apps created by that Place owner.
    pub place_id: Option<String>,
    pub name: String,
    pub version: String,
    pub definition: serde_json::Value,
    /// Optional maintainer set. Only a Place Owner may change it.
    pub maintainers: Option<Vec<String>>,
    /// A new `public` or `private` visibility; changing it requires the global App permission.
    pub visibility: Option<String>,
    pub updated_at: Option<u64>,
}
pub struct AppDeleteInput {
    pub app_id: String,
    pub deleted_at: Option<u64>,
}
pub struct AppInstanceCreateInput {
    pub place_id: String,
    pub app_id: String,
    pub name: Option<String>,
    #[serde(default)]
    pub config: serde_json::Value,
    pub created_at: Option<u64>,
}
pub struct AppInstanceRemoveInput {
    pub instance_id: String,
    pub removed_at: Option<u64>,
}
pub struct DataAnalyzeInput {
    pub place_id: String,
    pub files_instance_id: String,
    pub file_id: String,
    /// Worker output supplied by the Gateway when the data.import capability
    /// lives on another node. When absent, ogd may execute its local worker.
    pub worker_result: Option<serde_json::Value>,
    /// Ask the control-plane Core to authorize and resolve the destination
    /// without mutating its local application storage. Used by Gateway when
    /// the Place database provider is a remote Core.
    #[serde(default)]
    pub plan_only: bool,
}
pub struct DataImportInput {
    pub place_id: String,
    #[serde(default)]
    pub files_instance_id: String,
    #[serde(default)]
    pub file_id: String,
    pub target_instance_id: String,
    pub table: String,
    #[serde(default)]
    pub mapping: serde_json::Value,
    pub mode: Option<String>,
    /// Worker output supplied by the Gateway when transformation is executed
    /// by a dedicated data.import node.
    pub worker_result: Option<serde_json::Value>,
    /// Ask the control-plane Core to authorize and resolve the destination
    /// without mutating its local application storage. Used by Gateway when
    /// the Place database provider is a remote Core.
    #[serde(default)]
    pub plan_only: bool,
    /// The staged import to resume (after its checks asked for a
    /// confirmation) or to cancel; absent, the file is read and staged.
    pub import_id: Option<String>,
    /// The checks the caller accepts: their rows no longer stop the import.
    #[serde(default)]
    pub accept: Vec<String>,
    /// Stage and check only, write nothing (the « import? » question).
    #[serde(default)]
    pub check_only: bool,
    /// Drop the staged import `importId`.
    #[serde(default)]
    pub cancel: bool,
    /// Fields of the slice an import replaces (mode `slice`), when the table declares none.
    #[serde(default)]
    pub slice: Vec<String>,
    /// Fields of the key a row is updated by (mode `update`), when the table declares none.
    #[serde(default)]
    pub key: Vec<String>,
    /// The destination the authority Core resolved (`planOnly`: `collection`,
    /// `mode`, `import`), given by the Gateway to the database Core of the
    /// Place, which stages, checks and writes the rows under its delegation.
    pub plan: Option<serde_json::Value>,
    /// A batch of the file's rows to stage under `importId` (a new import
    /// when absent), answered `staged`: the Gateway passes a large file on in
    /// batches as the data worker reads it, then resumes the import.
    pub documents: Option<Vec<serde_json::Value>>,
    /// The data worker's warnings, given back by the resume that ends a
    /// batched import.
    pub warnings: Option<serde_json::Value>,
}
pub struct DataWorkerRunInput {
    pub place_id: String,
    pub file_name: String,
    pub size: u64,
    pub operation: String,
    pub mapping: Option<serde_json::Value>,
}
pub struct DataMappingSaveInput {
    pub place_id: String,
    pub fingerprint: String,
    pub name: String,
    pub target_app_id: String,
    pub target_table: String,
    pub definition: serde_json::Value,
}
pub struct DataMappingListInput {
    pub place_id: String,
    pub fingerprint: Option<String>,
}
pub struct DataMappingUpdateInput {
    pub place_id: String,
    pub mapping_id: String,
    pub name: Option<String>,
    pub target_app_id: Option<String>,
    pub target_table: Option<String>,
    pub definition: Option<serde_json::Value>,
}
pub struct DataMappingDeleteInput {
    pub place_id: String,
    pub mapping_id: String,
}
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
    pub context: Option<RequestedExecutionContext>,
    /// Reject the request if the planned query may mutate storage.
    ///
    /// The canonical wire name is `readOnly` (Gateway/public protocol).
    /// `read_only` remains accepted for compatibility with older direct clients.
    #[serde(default, rename = "readOnly", alias = "read_only")]
    pub read_only: bool,
    /// Values for the `$name` parameters of `query`. They are bound as escaped
    /// literals during validation (with the query's `let` variables), before
    /// authorization and planning see the query; for a query reading views,
    /// once the Place's views are read.
    pub params: Option<serde_json::Map<String, serde_json::Value>>,
}

inputs! {
pub struct QueryContextResolveInput {
    pub place_id: String,
    pub app_instance_id: Option<String>,
    pub as_profile: Option<String>,
}
pub struct AuthBeginInput {
    pub identity_id: String,
    pub device_id: String,
}
pub struct ChallengeSignatureInput {
    pub challenge_id: String,
    pub signature: String,
}
pub struct AuthEnrollBeginInput {
    pub identity_id: String,
    pub identity_public_key: String,
    pub device_id: String,
    pub device_public_key: String,
    pub token: Option<String>,
}
pub struct ClassicAuthRegisterInput {
    pub identifier: String,
    pub password: String,
}
pub struct ClassicAuthLoginInput {
    pub identifier: String,
    pub password: String,
    pub device_id: String,
    pub public_key: String,
}
/// Asks for a password reset code, mailed to the identifier (an email address).
pub struct ClassicAuthResetRequestInput {
    pub identifier: String,
    /// Language of the mail (`fr`, else English).
    pub language: Option<String>,
}
/// Sets a new password with the code received by mail.
pub struct ClassicAuthResetCompleteInput {
    pub identifier: String,
    pub code: String,
    pub password: String,
}
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsSubscribeInput {
    #[serde(default)]
    pub types: Vec<String>,
}

inputs! {
pub struct IdentityRegisterInput {
    pub identity_id: String,
    pub public_key: String,
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
    #[serde(default = "default_public_key_encoding")]
    pub encoding: String,
    pub created_at: Option<u64>,
}
pub struct IdentityOpenInput {
    pub file: String,
    pub password: String,
    pub client_device_id: Option<String>,
}
pub struct PasswordInput {
    pub password: String,
}
pub struct IdentityRenewInput {
    pub device_id: Option<String>,
    pub public_key: Option<String>,
    pub password: Option<String>,
}
pub struct DeviceRegisterInput {
    pub device_id: String,
    pub identity_id: String,
    pub public_key: String,
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
    #[serde(default = "default_public_key_encoding")]
    pub encoding: String,
    pub created_at: Option<u64>,
}
pub struct DeviceRenameInput {
    pub device_id: String,
    pub name: String,
}
pub struct DeviceRevokeInput {
    pub device_id: String,
    pub revoked_at: Option<u64>,
}
/// Authenticates a connection with a machine access token (`machine.access.create`).
pub struct AuthMachineInput {
    pub token: String,
}
/// A durable access for a program (a file connector, a script) to one App instance of a Place.
pub struct MachineAccessCreateInput {
    pub place_id: String,
    pub instance_id: String,
    pub name: String,
    /// `deposit` (list and add files, the default) or `full`.
    pub mode: Option<String>,
}
pub struct MachineAccessListInput {
    pub place_id: Option<String>,
    pub instance_id: Option<String>,
}
pub struct MachineAccessRevokeInput {
    pub access_id: String,
}
/// What a machine access did with the last file it deposited (the Gateway's automatic import), kept on the access.
pub struct MachineAccessReportInput {
    pub file_name: String,
    /// `completed`, `skipped` or `failed`.
    pub status: String,
    pub rows: Option<u64>,
    pub table: Option<String>,
    pub message: Option<String>,
}
/// A pool: one client company, the totals its policies share (`limits`: `users`, `storage`, `places`).
/// A section left out is not changed by `pool.update`; `{}` clears it.
pub struct PoolCreateInput {
    pub label: String,
    pub limits: Option<serde_json::Value>,
}
pub struct PoolUpdateInput {
    pub pool_id: String,
    pub label: Option<String>,
    pub limits: Option<serde_json::Value>,
}
pub struct PoolIdInput {
    pub pool_id: String,
}
/// A policy: the profile of the identities enrolled with one of its tokens, inside a pool.
/// Every section and every key is optional: absent means no limit, or allowed.
pub struct PolicyCreateInput {
    pub pool_id: String,
    pub label: String,
    /// Totals of the policy: `users`, `storage`, `places`.
    pub limits: Option<serde_json::Value>,
    /// Per identity: `devices`, `storage`, `places`.
    pub per_user: Option<serde_json::Value>,
    /// `createPlace`, `publicSharing`, `externalSharing`.
    pub rights: Option<serde_json::Value>,
    /// `included` and `addons`: App ids.
    pub apps: Option<serde_json::Value>,
}
pub struct PolicyUpdateInput {
    pub policy_id: String,
    pub label: Option<String>,
    pub limits: Option<serde_json::Value>,
    pub per_user: Option<serde_json::Value>,
    pub rights: Option<serde_json::Value>,
    pub apps: Option<serde_json::Value>,
}
pub struct PolicyListInput {
    pub pool_id: Option<String>,
}
/// Identities for the admin screen: those of one policy, or all.
pub struct IdentityListInput {
    pub policy_id: Option<String>,
}
/// Stored bytes of the caller (its Places, its pool), or with `all` (policy.manage) of everyone.
pub struct UsageGetInput {
    #[serde(default)]
    pub all: bool,
}
pub struct PolicyIdInput {
    pub policy_id: String,
}
/// An enrollment token of a policy, shown once.
pub struct PolicyTokenCreateInput {
    pub policy_id: String,
    pub name: Option<String>,
}
pub struct PolicyTokenRevokeInput {
    pub token_id: String,
}
/// Activates or deactivates an identity: an inactive identity cannot sign in and takes no policy seat.
pub struct IdentityStateSetInput {
    pub identity_id: String,
    /// `active` or `inactive`.
    pub state: String,
}
/// Moves an identity to a policy; without `policyId`, out of every policy.
pub struct IdentityPolicySetInput {
    pub identity_id: String,
    pub policy_id: Option<String>,
}
pub struct FileScopeInput {
    pub place_id: String,
    pub instance_id: String,
}
pub struct FileSyncConfigSetInput {
    pub root: String,
}
pub struct FileSyncSelectionSetInput {
    pub place_id: String,
    pub instance_id: String,
    #[serde(default)]
    pub all: bool,
    #[serde(default)]
    pub folder_ids: Vec<String>,
}
pub struct FileSyncSelectionRemoveInput {
    pub place_id: String,
    pub instance_id: String,
}
pub struct FileListInput {
    pub place_id: String,
    pub instance_id: String,
    pub parent_id: Option<String>,
}
pub struct FileEntryInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: String,
}
pub struct FileMkdirInput {
    pub place_id: String,
    pub instance_id: String,
    pub parent_id: Option<String>,
    pub name: String,
}
pub struct FileMoveInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: String,
    pub parent_id: Option<String>,
    pub name: String,
}
pub struct FileReadInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: String,
    #[serde(default)]
    pub offset: u64,
    pub length: Option<u64>,
}
pub struct FileWriteInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: Option<String>,
    pub parent_id: Option<String>,
    pub name: Option<String>,
    pub content_type: Option<String>,
    pub size: u64,
}
pub struct FileVersionReadInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: String,
    pub version_id: String,
    pub offset: Option<u64>,
    pub length: Option<u64>,
}
pub struct FileVersionInput {
    pub place_id: String,
    pub instance_id: String,
    pub file_id: String,
    pub version_id: String,
}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LlmToolCallInput {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LlmToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LlmMessageInput {
    pub role: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<LlmToolCallInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum LlmToolChoice {
    #[default]
    Auto,
    None,
    Required,
}

impl LlmToolChoice {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::None => "none",
            Self::Required => "required",
        }
    }
}

inputs! {
pub struct LlmGenerateInput {
    pub messages: Vec<LlmMessageInput>,
    #[serde(default)]
    pub tools: Vec<LlmToolDefinition>,
    #[serde(default)]
    pub tool_choice: LlmToolChoice,
    pub max_tokens: Option<u32>,
    pub seed: Option<u32>,
    /// Optional ephemeral prefix-cache key. The Agent uses this to reuse llama.cpp
    /// prompt state across LLM/tool turns inside one run.
    pub cache_key: Option<String>,
    /// Keeps the prefix cache of `cacheKey` after the run, captured at the end of the system message: the catalog and
    /// rules of a Place, read again by its next requests. Without it the cache lives for the tool turns of one run.
    #[serde(default)]
    pub keep_cache: bool,
    /// GBNF grammar the answer follows (root rule `root`), checked before the run: the App generation sends one built
    /// from the catalog of the Place (wip/APP_MODELE_SEMANTIQUE.md, step 8).
    pub grammar: Option<String>,
    /// Always the most likely token: the same request gives the same answer.
    #[serde(default)]
    pub greedy: bool,
    /// The reasoning of the model for this request (`false`: off), else the policy of the node (`OGD_LLM_THINKING`).
    pub thinking: Option<bool>,
}
pub struct LlmCancelInput {
    pub run_id: u64,
}
/// `llm.embed`: one text or a list of texts (at most 256), normalised to unit length unless `normalize: false`.
pub struct LlmEmbedInput {
    #[serde(deserialize_with = "one_or_many")]
    pub input: Vec<String>,
    #[serde(default = "default_true")]
    pub normalize: bool,
}
}

fn default_true() -> bool { true }

fn one_or_many<'de, D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany { One(String), Many(Vec<String>) }
    Ok(match OneOrMany::deserialize(deserializer)? { OneOrMany::One(text) => vec![text], OneOrMany::Many(texts) => texts })
}

inputs! {
pub struct AgentRunInput {
    pub messages: Vec<LlmMessageInput>,
    pub context: RequestedExecutionContext,
    pub max_steps: Option<u32>,
    pub max_tokens: Option<u32>,
}
/// A request on the data of a Place turned into an App (`agent.app`, wip/APP_MODELE_SEMANTIQUE.md step 9). A follow-up
/// sends the previous request with the action and the intent the previous answer returned.
pub struct AgentAppInput {
    pub context: RequestedExecutionContext,
    pub request: String,
    pub lang: Option<String>,
    pub previous: Option<AgentAppPrevious>,
}
pub struct AgentAppPrevious {
    pub request: String,
    pub action: String,
    pub intent: Option<String>,
}
/// One message submitted through the `mail` provider of a Place (or the system default).
/// Mailboxes are `address` or `Display Name <address>`.
pub struct MailSendInput {
    pub context: RequestedExecutionContext,
    #[serde(default)]
    pub to: Vec<String>,
    #[serde(default)]
    pub cc: Vec<String>,
    #[serde(default)]
    pub bcc: Vec<String>,
    pub reply_to: Option<String>,
    pub subject: String,
    #[serde(default)]
    pub text: String,
    pub html: Option<String>,
}
}

/// Recipients per message, all fields together.
pub const MAIL_MAX_RECIPIENTS: usize = 100;

/// A bare email address in a usable form: `local@domain.tld`, no display name, at most 254 bytes.
#[must_use]
pub fn email_address_valid(address: &str) -> bool {
    let Some((local, domain)) = address.rsplit_once('@') else { return false };
    address.len() <= 254 && local.len() <= 64 && mailbox_address(address) == Some(address)
        && !local.contains('@') && !local.starts_with('.') && !local.ends_with('.') && !local.contains("..")
        && domain.contains('.') && domain.split('.').all(|label| !label.is_empty() && label.len() <= 63 && !label.starts_with('-') && !label.ends_with('-') && label.chars().all(|c| c.is_alphanumeric() || c == '-'))
}

/// Address part of a mailbox, refused when it could inject an SMTP command or a header.
#[must_use]
pub fn mailbox_address(mailbox: &str) -> Option<&str> {
    if mailbox.chars().any(char::is_control) { return None; }
    let address = match mailbox.rsplit_once('<') { Some((_, rest)) => rest.strip_suffix('>')?, None => mailbox }.trim();
    let (local, domain) = address.split_once('@')?;
    (!local.is_empty() && !domain.is_empty() && !address.contains(|c: char| c.is_whitespace() || "<>\"(),;:".contains(c))).then_some(address)
}

inputs! {
pub struct CollectionsListInput {
    #[serde(default)]
    pub stats: bool,
    pub place_id: Option<String>,
    pub app_instance_id: Option<String>,
}
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
    if value.trim().is_empty() { Err(invalid_payload(operation, format!("{field} must not be empty"))) } else { Ok(()) }
}

/// Trims `value` in place, then [`non_empty`].
fn trimmed(operation: &str, field: &str, value: &mut String) -> Result<()> {
    *value = value.trim().to_owned();
    non_empty(operation, field, value)
}

/// At least one role, each trimmed and non-empty.
fn roles(operation: &str, roles: [&mut Option<String>; 3]) -> Result<()> {
    reject_if(operation, roles.iter().all(|role| role.is_none()), "one of role, serviceRole or storageRole is required")?;
    for (wire, role) in ["role", "serviceRole", "storageRole"].into_iter().zip(roles) {
        if let Some(role) = role { trimmed(operation, wire, role)?; }
    }
    Ok(())
}

/// At least one message, each with a trimmed role and no NUL byte.
fn messages(operation: &str, messages: &mut [LlmMessageInput]) -> Result<()> {
    reject_if(operation, messages.is_empty(), "messages must contain at least one message")?;
    for message in messages {
        trimmed(operation, "messages.role", &mut message.role)?;
        reject_if(operation, message.content.contains('\0') || message.role.contains('\0'), "messages must not contain NUL bytes")?;
    }
    Ok(())
}

fn reject_if(operation: &str, condition: bool, reason: &'static str) -> Result<()> {
    if condition { Err(invalid_payload(operation, reason)) } else { Ok(()) }
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

impl OperationPayload for LlmEmbedInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        reject_if(operation, self.input.is_empty() || self.input.len() > 256, "input must hold 1 to 256 texts")?;
        reject_if(operation, self.input.iter().any(|text| text.trim().is_empty()), "every input text must be non-empty")?;
        Ok(())
    }
}

impl OperationPayload for LlmCancelInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        reject_if(operation, self.run_id == 0, "runId must be greater than zero")?;
        Ok(())
    }
}

impl OperationPayload for LlmGenerateInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        messages(operation, &mut self.messages)?;
        for message in &mut self.messages {
            if let Some(tool_call_id) = message.tool_call_id.as_mut() {
                trimmed(operation, "messages.toolCallId", tool_call_id)?;
                reject_if(operation, tool_call_id.contains('\0'), "messages.toolCallId must not contain NUL bytes")?;
            }
            for tool_call in &mut message.tool_calls {
                trimmed(operation, "messages.toolCalls.id", &mut tool_call.id)?;
                trimmed(operation, "messages.toolCalls.name", &mut tool_call.name)?;
                reject_if(operation, tool_call.id.contains('\0') || tool_call.name.contains('\0'), "messages.toolCalls must not contain NUL bytes")?;
                reject_if(operation, !tool_call.arguments.is_object(), "messages.toolCalls.arguments must be an object")?;
            }
        }
        for tool in &mut self.tools {
            trimmed(operation, "tools.name", &mut tool.name)?;
            reject_if(operation, tool.name.contains('\0') || tool.description.contains('\0'), "tools must not contain NUL bytes")?;
            reject_if(operation, !tool.parameters.is_object(), "tools.parameters must be a JSON object")?;
        }
        reject_if(operation, self.tool_choice == LlmToolChoice::Required && self.tools.is_empty(), "toolChoice required requires at least one tool")?;
        reject_if(operation, matches!(self.max_tokens, Some(0)), "maxTokens must be greater than zero")?;
        if let Some(cache_key) = self.cache_key.as_mut() {
            trimmed(operation, "cacheKey", cache_key)?;
            reject_if(operation, cache_key.len() > 160, "cacheKey must not exceed 160 bytes")?;
            reject_if(operation, cache_key.contains('\0'), "cacheKey must not contain NUL bytes")?;
        }
        reject_if(operation, self.keep_cache && self.cache_key.is_none(), "keepCache requires a cacheKey")?;
        if let Some(grammar) = &self.grammar {
            reject_if(operation, grammar.trim().is_empty() || grammar.len() > 256 * 1024, "grammar must hold 1 to 262144 bytes")?;
            reject_if(operation, grammar.contains('\0'), "grammar must not contain NUL bytes")?;
            reject_if(operation, !self.tools.is_empty(), "grammar and tools are exclusive")?;
        }
        Ok(())
    }
}

impl OperationPayload for MailSendInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "context.placeId", &self.context.place_id)?;
        let recipients = self.to.len() + self.cc.len() + self.bcc.len();
        if recipients == 0 || recipients > MAIL_MAX_RECIPIENTS {
            return Err(invalid_payload(operation, format!("to, cc and bcc must hold between 1 and {MAIL_MAX_RECIPIENTS} recipients")));
        }
        for mailbox in self.to.iter_mut().chain(&mut self.cc).chain(&mut self.bcc).chain(&mut self.reply_to) {
            *mailbox = mailbox.trim().to_owned();
            if mailbox_address(mailbox).is_none() {
                return Err(invalid_payload(operation, format!("invalid mailbox {mailbox:?}")));
            }
        }
        reject_if(operation, self.subject.chars().any(char::is_control), "subject must not contain control characters")?;
        reject_if(operation, self.text.contains('\0') || self.html.as_deref().is_some_and(|html| html.contains('\0')), "text and html must not contain NUL bytes")?;
        Ok(())
    }
}

impl OperationPayload for AgentRunInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        messages(operation, &mut self.messages)?;
        non_empty(operation, "context.placeId", &self.context.place_id)?;
        if let Some(app_instance_id) = self.context.app_instance_id.as_ref() {
            non_empty(operation, "context.appInstanceId", app_instance_id)?;
        }
        reject_if(operation, matches!(self.max_steps, Some(0)), "maxSteps must be greater than zero")?;
        reject_if(operation, matches!(self.max_tokens, Some(0)), "maxTokens must be greater than zero")?;
        Ok(())
    }
}

impl OperationPayload for AgentAppInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "context.placeId", &self.context.place_id)?;
        if let Some(app_instance_id) = self.context.app_instance_id.as_ref() { non_empty(operation, "context.appInstanceId", app_instance_id)?; }
        non_empty(operation, "request", self.request.trim())?;
        reject_if(operation, self.request.len() > 4000, "request must be at most 4000 bytes")?;
        reject_if(operation, !matches!(self.lang.as_deref(), None | Some("fr" | "en")), "lang must be fr or en")?;
        if let Some(previous) = self.previous.as_ref() {
            reject_if(operation, previous.request.len() > 4000 || previous.intent.as_ref().is_some_and(|intent| intent.len() > 4000), "previous request and intent must be at most 4000 bytes")?;
        }
        Ok(())
    }
}

impl OperationPayload for CollectionsListInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        if let Some(place_id) = self.place_id.as_ref() {
            non_empty(operation, "placeId", place_id)?;
        }
        if let Some(app_instance_id) = self.app_instance_id.as_ref() {
            reject_if(operation, self.place_id.is_none(), "appInstanceId requires placeId")?;
            non_empty(operation, "appInstanceId", app_instance_id)?;
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
validate_fields!(ClassicAuthResetRequestInput => identifier: "identifier");
validate_fields!(ClassicAuthResetCompleteInput => identifier: "identifier", code: "code", password: "password");
validate_fields!(IdentityRegisterInput => identity_id: "identityId", public_key: "publicKey");
validate_fields!(PasswordInput => password: "password");
validate_fields!(DeviceRegisterInput => device_id: "deviceId", identity_id: "identityId", public_key: "publicKey");
validate_fields!(DeviceRenameInput => device_id: "deviceId", name: "name");
validate_fields!(DeviceRevokeInput => device_id: "deviceId");
validate_fields!(AuthMachineInput => token: "token");
validate_fields!(MachineAccessCreateInput => place_id: "placeId", instance_id: "instanceId", name: "name");
validate_fields!(MachineAccessRevokeInput => access_id: "accessId");
validate_fields!(MachineAccessReportInput => file_name: "fileName", status: "status");
validate_fields!(PoolCreateInput => label: "label");
validate_fields!(PoolIdInput => pool_id: "poolId");
validate_fields!(PolicyCreateInput => pool_id: "poolId", label: "label");
validate_fields!(PolicyIdInput => policy_id: "policyId");
validate_fields!(PolicyTokenCreateInput => policy_id: "policyId");
validate_fields!(PolicyTokenRevokeInput => token_id: "tokenId");
validate_fields!(IdentityStateSetInput => identity_id: "identityId", state: "state");
impl OperationPayload for PoolUpdateInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "poolId", &self.pool_id)?;
        if let Some(label) = self.label.as_ref() { non_empty(operation, "label", label)?; }
        Ok(())
    }
}
impl OperationPayload for PolicyUpdateInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "policyId", &self.policy_id)?;
        if let Some(label) = self.label.as_ref() { non_empty(operation, "label", label)?; }
        Ok(())
    }
}
impl OperationPayload for PolicyListInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        if let Some(pool_id) = self.pool_id.as_ref() { non_empty(operation, "poolId", pool_id)?; }
        Ok(())
    }
}
impl OperationPayload for IdentityListInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        if let Some(policy_id) = self.policy_id.as_ref() { non_empty(operation, "policyId", policy_id)?; }
        Ok(())
    }
}
impl OperationPayload for UsageGetInput {
    fn validate(&mut self, _operation: &str) -> Result<()> { Ok(()) }
}
impl OperationPayload for IdentityPolicySetInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "identityId", &self.identity_id)?;
        if let Some(policy_id) = self.policy_id.as_ref() { non_empty(operation, "policyId", policy_id)?; }
        Ok(())
    }
}
impl OperationPayload for MachineAccessListInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        if let Some(place_id) = self.place_id.as_ref() { non_empty(operation, "placeId", place_id)?; }
        if let Some(instance_id) = self.instance_id.as_ref() {
            reject_if(operation, self.place_id.is_none(), "instanceId requires placeId")?;
            non_empty(operation, "instanceId", instance_id)?;
        }
        Ok(())
    }
}
validate_fields!(PermissionGrantInput => identity_id: "identityId", action: "action", resource: "resource");
validate_fields!(PermissionRevokeInput => identity_id: "identityId", action: "action", resource: "resource");
validate_fields!(SharingDeleteInput => sharing_id: "sharingId");
validate_fields!(PlaceCreateInput => name: "name");
validate_fields!(PlaceIdInput => place_id: "placeId");
validate_fields!(PlaceDeleteInput => place_id: "placeId");
impl OperationPayload for PlaceSearchInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "text", &self.text)?;
        reject_if(operation, self.limit.is_some_and(|limit| limit == 0 || limit > 200), "limit must be 1 to 200")
    }
}
impl OperationPayload for PlaceResourceSetInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "identityId", &self.identity_id)?;
        non_empty(operation, "deviceId", &self.device_id)?;
        non_empty(operation, "capability", &self.capability)?;
        roles(operation, [&mut self.role, &mut self.service_role, &mut self.storage_role])
    }
}
validate_fields!(PlaceResourceRemoveInput => place_id: "placeId", identity_id: "identityId", device_id: "deviceId", capability: "capability");
impl OperationPayload for FabricResourceSetInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "identityId", &self.identity_id)?;
        non_empty(operation, "deviceId", &self.device_id)?;
        non_empty(operation, "capability", &self.capability)?;
        roles(operation, [&mut self.role, &mut self.service_role, &mut self.storage_role])
    }
}
validate_fields!(FabricResourceRemoveInput => identity_id: "identityId", device_id: "deviceId", capability: "capability");

validate_fields!(PlaceAccessRemoveInput => place_id: "placeId", identity_id: "identityId");
validate_fields!(PlacePublicSetInput => place_id: "placeId");
validate_fields!(AppIdInput => app_id: "appId");
validate_fields!(AppDeleteInput => app_id: "appId");
validate_fields!(AppInstanceCreateInput => place_id: "placeId", app_id: "appId");
validate_fields!(AppInstanceRemoveInput => instance_id: "instanceId");
validate_fields!(BackupNameInput => name: "name");
validate_fields!(BackupCreateInput => name: "name");
validate_fields!(BackupRestoreInput => name: "name");

impl OperationPayload for PlaceUpdateInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        if let Some(name) = self.name.as_mut() {
            trimmed(operation, "name", name)?;
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
        if let Some(tile_size) = self.tile_size.as_mut() {
            *tile_size = tile_size.trim().to_ascii_lowercase();
            match tile_size.as_str() {
                "full" | "summary" | "icons" => {}
                _ => {
                    return Err(invalid_payload(
                        operation,
                        "tileSize must be full, summary or icons",
                    ))
                }
            }
        }
        if let Some(order) = self.app_order.as_mut() {
            order.retain(|value| !value.trim().is_empty());
            let mut seen = std::collections::HashSet::new();
            order.retain(|value| seen.insert(value.clone()));
        }
        if let Some(layout) = self.app_layout.as_mut() {
            let mut folders = std::collections::HashSet::new();
            for entry in layout.iter_mut() {
                if let AppLayoutEntry::Folder(folder) = entry {
                    folder.folder = folder.folder.trim().to_owned();
                    folder.name = folder.name.trim().to_owned();
                    reject_if(operation, folder.folder.is_empty() || folder.folder.len() > 64, "a folder id holds 1 to 64 bytes")?;
                    reject_if(operation, folder.name.is_empty() || folder.name.chars().count() > APP_FOLDER_NAME_MAX, "a folder name holds 1 to 60 characters")?;
                    reject_if(operation, !folders.insert(folder.folder.clone()), "folder ids must be unique")?;
                }
            }
            reject_if(operation, folders.len() > APP_FOLDERS_MAX, "a Place holds at most 50 folders")?;
            // An instance appears once, at its first place.
            let mut seen = std::collections::HashSet::new();
            AppLayoutEntry::retain(layout, |value| !value.trim().is_empty() && seen.insert(value.to_owned()));
        }
        if let Some(profiles) = self.profiles.as_mut() {
            let mut ids = std::collections::HashSet::new();
            reject_if(operation, profiles.len() > APP_FOLDERS_MAX, "a Place holds at most 50 profiles")?;
            for profile in profiles.iter_mut() {
                (profile.profile_id, profile.name) = (profile.profile_id.trim().to_owned(), profile.name.trim().to_owned());
                reject_if(operation, profile.profile_id.is_empty() || profile.profile_id.len() > 64 || !ids.insert(profile.profile_id.clone()), "profile ids are unique and hold 1 to 64 bytes")?;
                reject_if(operation, profile.name.is_empty() || profile.name.chars().count() > APP_FOLDER_NAME_MAX, "a profile name holds 1 to 60 characters")?;
                if let Some(filters) = profile.filters.as_ref() { crate::access::place::access_filters(filters).map_err(|message| invalid_payload(operation, &message))?; }
            }
        }
        Ok(())
    }
}

impl OperationPayload for QueryExecuteInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        reject_if(operation, self.query.trim().is_empty(), "query must not be empty")?;
        // Evaluate `let` variables and bind `$name` values before authorization
        // and planning ever see the query; a query reading its Place (views,
        // `let` queries) keeps its parameters until the Place is known.
        let parameters = self.params.take().unwrap_or_default();
        if crate::query::reads_place(&self.query) {
            self.params = Some(parameters);
        } else {
            self.query = crate::query::prepare_query(&self.query, &parameters)
                .map_err(|message| invalid_payload(operation, message))?;
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
        if self.password.as_deref().is_none_or(str::is_empty) {
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
        reject_if(operation, self.owner == self.target, "owner and target must differ")?;
        self.permissions = normalize_strings(std::mem::take(&mut self.permissions));
        Ok(())
    }
}

impl OperationPayload for SharingUpdateInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "sharingId", &self.sharing_id)?;
        reject_if(operation, self.permissions.is_none() && self.state.as_deref().is_none(), "permissions or state is required")?;
        self.permissions = self.permissions.take().map(normalize_strings);
        Ok(())
    }
}

impl OperationPayload for PlaceAccessSetInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        non_empty(operation, "placeId", &self.place_id)?;
        non_empty(operation, "identityId", &self.identity_id)?;
        if let Some(filters) = &self.filters {
            crate::access::place::access_filters(filters)
                .map_err(|message| invalid_payload(operation, &message))?;
        }
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
                reject_if(operation, !self.definition.is_object(), "definition must be an object")?;
                reject_if(operation, self.visibility.as_deref().is_some_and(|visibility| visibility != "public" && visibility != "private"), "visibility must be public or private")?;
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
        // A batch or a resume carries rows already read: no file, no mapping.
        let reads_file = self.import_id.is_none() && self.documents.is_none();
        if reads_file {
            non_empty(operation, "filesInstanceId", &self.files_instance_id)?;
            non_empty(operation, "fileId", &self.file_id)?;
        }
        non_empty(operation, "targetInstanceId", &self.target_instance_id)?;
        non_empty(operation, "table", &self.table)?;
        reject_if(operation, reads_file && !self.mapping.is_object(), "mapping must be an object")?;
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
        reject_if(operation, self.operation != "analyze" && self.operation != "import", "operation must be analyze or import")?;
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
        reject_if(operation, !self.definition.is_object(), "definition must be an object")?;
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
            return Err(invalid_payload(operation, "at least one mapping field must be provided"));
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
            trimmed(operation, "folderIds", folder_id)?;
        }
        self.folder_ids.sort();
        self.folder_ids.dedup();
        reject_if(operation, !self.all && self.folder_ids.is_empty(), "folderIds must contain at least one folder when all is false")?;
        if self.all { self.folder_ids.clear(); }
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
        reject_if(operation, matches!(self.length, Some(0)), "length must be greater than zero")?;
        Ok(())
    }
}
impl OperationPayload for FileWriteInput {
    fn validate(&mut self, operation: &str) -> Result<()> {
        file_scope(operation, &self.place_id, &self.instance_id)?;
        reject_if(operation, self.file_id.as_deref().is_some_and(str::is_empty), "fileId must not be empty")?;
        reject_if(operation, self.parent_id.as_deref().is_some_and(str::is_empty), "parentId must not be empty")?;
        reject_if(operation, self.file_id.is_none() && self.name.as_deref().is_none_or(str::is_empty), "name is required when creating a file")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn generate(extra: serde_json::Value) -> Result<LlmGenerateInput> { let mut body = serde_json::json!({"messages": [{"role": "user", "content": "x"}]}); body.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone()); let mut input: LlmGenerateInput = serde_json::from_value(body).unwrap(); input.validate("llm.generate").map(|()| input) }

    #[test] fn generate_takes_a_grammar_greedy_sampling_reasoning_and_a_kept_cache() { let input = generate(serde_json::json!({"grammar": "root ::= \"a\"", "greedy": true, "thinking": false, "cacheKey": "intent:p", "keepCache": true})).unwrap(); assert_eq!((input.greedy, input.thinking, input.keep_cache), (true, Some(false), true)); assert!(generate(serde_json::json!({"keepCache": true})).is_err()); assert!(generate(serde_json::json!({"grammar": "  "})).is_err()); assert!(generate(serde_json::json!({"grammar": "root ::= \"a\"", "tools": [{"name": "t", "description": "", "parameters": {}}]})).is_err()); }
}
