//! Pools and policies of the commercial mode (`OGD_ENROLLMENT_MODE=policy`, see `wip/POLICIES.md`).
//!
//! An identity enrolls with a token of a policy and takes that policy; a policy belongs to a pool,
//! the client company. Every limit is optional: a key left out means no limit, or allowed. This
//! module holds the admin operations and the checks the enrollment and device paths call.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;
use super::serve::{failed, reply, Handled, Stop};
use crate::operation::{IdentityListInput, IdentityPolicySetInput, IdentityStateSetInput, PolicyCreateInput, PolicyIdInput, PolicyListInput, PolicyTokenCreateInput, PolicyTokenRevokeInput, PolicyUpdateInput, PoolCreateInput, PoolIdInput, PoolUpdateInput};

/// Enrollment tokens of a policy: `ogp_` and 64 hex digits (256 random bits), kept as their SHA-256.
const TOKEN_PREFIX: &str = "ogp_";

/// Allowed keys of each section, and the kind of their values.
const POOL_LIMITS: &[&str] = &["users", "storage", "places"];
const POLICY_LIMITS: &[&str] = &["users", "storage", "places"];
const PER_USER: &[&str] = &["devices", "storage", "places"];
const RIGHTS: &[&str] = &["createPlace", "publicSharing", "externalSharing"];
const APPS: &[&str] = &["included", "addons"];

#[derive(Clone, Copy)]
enum Kind { Count, Flag, Ids }

/// A section checked against its allowed keys: counts are non-negative integers, flags booleans,
/// ids arrays of non-empty strings. `devices` per user is at least 1 (enrollment adds one).
/// A count past 2^32 comes as a float from a MessagePack client (msgpackr writes such integers as
/// float64): a whole float is taken as the integer it holds, and kept as one.
fn section(name: &str, value: &JsonValue, keys: &[&str], kind: Kind) -> Result<JsonValue, String> {
    let Some(object) = value.as_object() else { return Err(format!("{name} must be an object")) };
    let mut checked = object.clone();
    for (key, item) in object {
        if !keys.contains(&key.as_str()) { return Err(format!("{name}.{key} is not a known setting (expected one of: {})", keys.join(", "))); }
        let whole = item.as_u64().or_else(|| item.as_f64().filter(|float| float.fract() == 0.0 && (0.0..9_007_199_254_740_992.0).contains(float)).map(|float| float as u64));
        if let (Kind::Count, Some(whole)) = (kind, whole) {
            checked.insert(key.clone(), serde_json::json!(whole));
        }
        let valid = match kind {
            Kind::Count => whole.is_some_and(|count| key != "devices" || count >= 1),
            Kind::Flag => item.is_boolean(),
            Kind::Ids => item.as_array().is_some_and(|ids| ids.iter().all(|id| id.as_str().is_some_and(|id| !id.trim().is_empty()))),
        };
        if !valid {
            return Err(match kind {
                Kind::Count if key == "devices" => format!("{name}.devices must be an integer of at least 1"),
                Kind::Count => format!("{name}.{key} must be a non-negative integer"),
                Kind::Flag => format!("{name}.{key} must be true or false"),
                Kind::Ids => format!("{name}.{key} must be an array of App ids"),
            });
        }
    }
    Ok(JsonValue::Object(checked))
}

/// The sections of a policy given in a create or update request, checked.
fn policy_sections(id: RequestId, limits: Option<&JsonValue>, per_user: Option<&JsonValue>, rights: Option<&JsonValue>, apps: Option<&JsonValue>) -> Result<Vec<(&'static str, JsonValue)>, Stop> {
    let mut sections = Vec::new();
    for (field, value, keys, kind) in [("limits", limits, POLICY_LIMITS, Kind::Count), ("perUser", per_user, PER_USER, Kind::Count), ("rights", rights, RIGHTS, Kind::Flag), ("apps", apps, APPS, Kind::Ids)] {
        if let Some(value) = value {
            match section(field, value, keys, kind) {
                Ok(value) => sections.push((field, value)),
                Err(message) => reject!(id, "policy.invalid_setting", message),
            }
        }
    }
    Ok(sections)
}

fn label_of(id: RequestId, label: &str) -> Result<String, Stop> {
    let label = label.trim();
    if label.is_empty() || label.len() > 120 { reject!(id, "policy.invalid_label", "a label holds 1 to 120 bytes"); }
    Ok(label.to_owned())
}

fn active_pool(engine: &Engine, id: RequestId, pool_id: &str) -> Result<JsonValue, Stop> {
    match query_first(engine, id, quoted!("on _pools | where poolId == {} and state == \"active\" | limit 1", pool_id))? {
        Some(pool) => Ok(pool),
        None => reject!(id, "pool.not_found", "no active pool has this poolId"),
    }
}

fn active_policy(engine: &Engine, id: RequestId, policy_id: &str) -> Result<JsonValue, Stop> {
    match query_first(engine, id, quoted!("on _policies | where policyId == {} and state == \"active\" | limit 1", policy_id))? {
        Some(policy) => Ok(policy),
        None => reject!(id, "policy.not_found", "no active policy has this policyId"),
    }
}

fn count(engine: &Engine, id: RequestId, query: String) -> Result<u64, Stop> {
    Ok(query_first(engine, id, format!("{query} | count"))?.and_then(|row| row.u64_field("count")).unwrap_or(0))
}

/// Active identities of a policy.
fn policy_users(engine: &Engine, id: RequestId, policy_id: &str) -> Result<u64, Stop> {
    count(engine, id, quoted!("on _identities | where policyId? == {} and state == \"active\"", policy_id))
}

/// Active identities of every active policy of a pool.
fn pool_users(engine: &Engine, id: RequestId, pool_id: &str) -> Result<u64, Stop> {
    let mut total = 0u64;
    for policy in query_documents(engine, id, quoted!("on _policies | where poolId == {} and state == \"active\" | select policyId", pool_id))? {
        total = total.saturating_add(policy_users(engine, id, &policy.string_field("policyId"))?);
    }
    Ok(total)
}

fn limit(document: &JsonValue, section: &str, key: &str) -> Option<u64> {
    document.get(section).and_then(|section| section.get(key)).and_then(JsonValue::as_u64)
}

/// Whether one more identity fits in `policy` and its pool. `already` is an identity already counted
/// in the policy (moving it again takes no seat).
fn check_seat(engine: &Engine, id: RequestId, policy: &JsonValue, already: bool) -> Result<(), Stop> {
    if already { return Ok(()); }
    let policy_id = policy.string_field("policyId");
    if let Some(max) = limit(policy, "limits", "users") {
        let users = policy_users(engine, id, &policy_id)?;
        if users >= max { reject!(id, "policy.users_limit", format!("the policy {:?} holds {users} active users, its limit is {max}", policy.string_field("label"))); }
    }
    let pool = active_pool(engine, id, &policy.string_field("poolId"))?;
    if let Some(max) = limit(&pool, "limits", "users") {
        let users = pool_users(engine, id, &pool.string_field("poolId"))?;
        if users >= max { reject!(id, "pool.users_limit", format!("the pool {:?} holds {users} active users, its limit is {max}", pool.string_field("label"))); }
    }
    Ok(())
}

fn token_hash(token: &str) -> String {
    crate::helpers::hex(&sha2::Sha256::digest(token.as_bytes()))
}

fn new_token() -> Result<String, String> {
    let mut random = [0u8; 32];
    getrandom::fill(&mut random).map_err(|error| error.to_string())?;
    Ok(format!("{TOKEN_PREFIX}{}", crate::helpers::hex(&random)))
}

/// A token as an admin sees it: never the token nor its hash.
fn token_json(record: &JsonValue) -> JsonValue {
    let mut token = serde_json::Map::new();
    for field in ["tokenId", "policyId", "name", "tokenHint", "state", "createdAt", "revokedAt"] {
        if let Some(value) = record.get(field) { token.insert(field.to_owned(), value.clone()); }
    }
    JsonValue::Object(token)
}

// ---- Checks called by the enrollment and device paths ----

/// The policy an enrollment token belongs to, once the token, its policy and its pool are active and
/// a seat is free. Refusals do not tell an unknown token from a revoked one.
pub(super) fn enrollment_policy(engine: &Engine, id: RequestId, token: &str) -> Result<String, Stop> {
    if !token.starts_with(TOKEN_PREFIX) || token.len() > 200 { reject!(id, "auth.enrollment_denied", "identity enrollment is not allowed"); }
    let Some(record) = query_first(engine, id, quoted!("on _policy_tokens | where tokenHash == {} and state == \"active\" | limit 1", &token_hash(token)))? else {
        reject!(id, "auth.enrollment_denied", "identity enrollment is not allowed");
    };
    let policy_id = record.string_field("policyId");
    let Some(policy) = query_first(engine, id, quoted!("on _policies | where policyId == {} and state == \"active\" | limit 1", &policy_id))? else {
        reject!(id, "auth.enrollment_denied", "identity enrollment is not allowed");
    };
    check_seat(engine, id, &policy, false)?;
    Ok(policy_id)
}

/// Checked again when the enrollment completes: another enrollment may have taken the last seat.
pub(super) fn enrollment_seat(engine: &Engine, id: RequestId, policy_id: &str) -> Result<(), Stop> {
    let policy = active_policy(engine, id, policy_id)?;
    check_seat(engine, id, &policy, false)
}

/// Whether `identity_id` may hold one more active device under its policy (`perUser.devices`).
/// An identity without a policy (the platform admin, an open-mode identity) has no limit.
pub(super) fn check_new_device(engine: &Engine, id: RequestId, identity_id: &str) -> Result<(), Stop> {
    let identity = query_first(engine, id, quoted!("on _identities | where identityId == {} | limit 1", identity_id))?;
    let Some(policy_id) = identity.as_ref().and_then(|identity| identity.str_field("policyId")).filter(|policy_id| !policy_id.is_empty()) else { return Ok(()) };
    let Some(policy) = query_first(engine, id, quoted!("on _policies | where policyId == {} | limit 1", policy_id))? else { return Ok(()) };
    let Some(max) = limit(&policy, "perUser", "devices") else { return Ok(()) };
    let devices = count(engine, id, quoted!("on _devices | where identityId == {} and state == \"active\"", identity_id))?;
    if devices >= max { reject!(id, "policy.devices_limit", format!("{devices} active devices, the limit of the policy is {max}: revoke one first")); }
    Ok(())
}

/// Refuses to sign in an identity an admin deactivated (`identity.state.set`). Only the explicit
/// `inactive` state is refused: older records without a state stay usable.
pub(super) fn require_identity_not_inactive(engine: &Engine, id: RequestId, identity_id: &str) -> Result<(), Stop> {
    let inactive = query_first(engine, id, quoted!("on _identities | where identityId == {} and state? == \"inactive\" | limit 1", identity_id))?;
    if inactive.is_some() { reject!(id, "auth.identity_inactive", "this identity has been deactivated"); }
    Ok(())
}

/// The active policy of an identity and its pool; `None` without a policy (the platform admin, an
/// open-mode identity), which no limit or right applies to.
fn policy_chain(engine: &Engine, id: RequestId, identity_id: &str) -> Result<Option<(JsonValue, Option<JsonValue>)>, Stop> {
    let identity = query_first(engine, id, quoted!("on _identities | where identityId == {} | limit 1", identity_id))?;
    let Some(policy_id) = identity.as_ref().map(|identity| identity.string_field("policyId")).filter(|policy_id| !policy_id.is_empty()) else { return Ok(None) };
    let Some(policy) = query_first(engine, id, quoted!("on _policies | where policyId == {} and state == \"active\" | limit 1", &policy_id))? else { return Ok(None) };
    let pool = query_first(engine, id, quoted!("on _pools | where poolId == {} and state == \"active\" | limit 1", &policy.string_field("poolId")))?;
    Ok(Some((policy, pool)))
}

/// A right of the policy: absent means allowed.
fn right(policy: &JsonValue, key: &str) -> bool { policy.get("rights").and_then(|rights| rights.get(key)).and_then(JsonValue::as_bool).unwrap_or(true) }

/// Active Places owned by the identities of a policy (every state: a deactivated user's Places stay).
fn policy_places(engine: &Engine, id: RequestId, policy_id: &str) -> Result<u64, Stop> {
    let mut total = 0u64;
    for identity in query_documents(engine, id, quoted!("on _identities | where policyId? == {} | select identityId", policy_id))? {
        total = total.saturating_add(count(engine, id, quoted!("on _places | where ownerIdentityId == {} and state == \"active\"", &identity.string_field("identityId")))?);
    }
    Ok(total)
}

/// `place.create` under the creator's policy: the `createPlace` and `publicSharing` rights, then the
/// Place limits per user, of the policy and of its pool.
pub(super) fn check_place_create(engine: &Engine, id: RequestId, identity_id: &str, public: bool) -> Result<(), Stop> {
    let Some((policy, pool)) = policy_chain(engine, id, identity_id)? else { return Ok(()) };
    if !right(&policy, "createPlace") { reject!(id, "policy.create_place_denied", format!("the policy {:?} does not allow creating Places", policy.string_field("label"))); }
    if public && !right(&policy, "publicSharing") { reject!(id, "policy.public_sharing_denied", format!("the policy {:?} does not allow public Places", policy.string_field("label"))); }
    if let Some(max) = limit(&policy, "perUser", "places") {
        let places = count(engine, id, quoted!("on _places | where ownerIdentityId == {} and state == \"active\"", identity_id))?;
        if places >= max { reject!(id, "policy.user_places_limit", format!("{places} Places, the limit per user of the policy is {max}: delete one first")); }
    }
    if let Some(max) = limit(&policy, "limits", "places") {
        let places = policy_places(engine, id, &policy.string_field("policyId"))?;
        if places >= max { reject!(id, "policy.places_limit", format!("the policy {:?} holds {places} Places, its limit is {max}", policy.string_field("label"))); }
    }
    if let Some(pool) = pool.as_ref() {
        if let Some(max) = limit(pool, "limits", "places") {
            let mut places = 0u64;
            for other in query_documents(engine, id, quoted!("on _policies | where poolId == {} and state == \"active\" | select policyId", &pool.string_field("poolId")))? {
                places = places.saturating_add(policy_places(engine, id, &other.string_field("policyId"))?);
            }
            if places >= max { reject!(id, "pool.places_limit", format!("the pool {:?} holds {places} Places, its limit is {max}", pool.string_field("label"))); }
        }
    }
    Ok(())
}

/// Opening a Place to the public, under its Owner's policy (`publicSharing`).
pub(super) fn check_public_sharing(engine: &Engine, id: RequestId, owner_identity_id: &str) -> Result<(), Stop> {
    let Some((policy, _)) = policy_chain(engine, id, owner_identity_id)? else { return Ok(()) };
    if !right(&policy, "publicSharing") { reject!(id, "policy.public_sharing_denied", format!("the policy {:?} does not allow public Places", policy.string_field("label"))); }
    Ok(())
}

/// Sharing with `target_identity_id`, under the owner's policy (`externalSharing`): a target outside
/// the owner's pool (another pool, or no policy) is external.
pub(super) fn check_external_sharing(engine: &Engine, id: RequestId, owner_identity_id: &str, target_identity_id: &str) -> Result<(), Stop> {
    let Some((policy, _)) = policy_chain(engine, id, owner_identity_id)? else { return Ok(()) };
    if right(&policy, "externalSharing") { return Ok(()); }
    let pool_id = policy.string_field("poolId");
    let target_pool = policy_chain(engine, id, target_identity_id)?.map(|(policy, _)| policy.string_field("poolId"));
    if target_pool.as_deref() != Some(pool_id.as_str()) { reject!(id, "policy.external_sharing_denied", format!("the policy {:?} allows sharing only inside its pool", policy.string_field("label"))); }
    Ok(())
}

// ---- Apps (step 4): what a policy offers, checked at installation and on writes ----

/// A platform App: built in and without a Store card (Query, Ask, Files, Builder…). Always
/// available, whatever the policy. The built-in Apps with a card (Finance and its reports,
/// Organisation, games) are offered like the published ones.
pub(super) fn app_is_platform(app: &JsonValue) -> bool {
    app.str_field("createdBy") == Some("system") && !app["definition"]["store"].is_object()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Availability { Included, Addon, Hidden }

impl Availability {
    pub(super) const fn as_str(self) -> &'static str {
        match self { Self::Included => "included", Self::Addon => "addon", Self::Hidden => "hidden" }
    }
}

/// The Apps a policy offers, when it restricts them (`apps.included` or `apps.addons` given).
#[derive(Debug)]
pub(super) struct AppOffer { label: String, included: std::collections::BTreeSet<String>, addons: std::collections::BTreeSet<String> }

impl AppOffer {
    /// Platform Apps and private Apps (their creator's own work) are always included.
    pub(super) fn of(&self, app: &JsonValue) -> Availability {
        if app_is_platform(app) || !app_is_public(app) { return Availability::Included; }
        let app_id = app.str_field("appId").unwrap_or_default();
        if self.included.contains(app_id) { Availability::Included } else if self.addons.contains(app_id) { Availability::Addon } else { Availability::Hidden }
    }

    /// Whether the policy includes `app_id` by name (not only as a platform or private App).
    pub(super) fn includes(&self, app_id: &str) -> bool { self.included.contains(app_id) }

    fn from_policy(policy: &JsonValue) -> Option<Self> {
        let apps = policy.get("apps").filter(|apps| apps.is_object())?;
        let ids = |key: &str| apps.get(key).and_then(JsonValue::as_array).map(|list| list.iter().filter_map(JsonValue::as_str).map(str::to_owned).collect::<std::collections::BTreeSet<_>>());
        let (included, addons) = (ids("included"), ids("addons"));
        if included.is_none() && addons.is_none() { return None; }
        Some(Self { label: policy.string_field("label"), included: included.unwrap_or_default(), addons: addons.unwrap_or_default() })
    }
}

/// The App offer of an identity's policy; `None` when nothing restricts its Apps (no policy, or a
/// policy without an `apps` selection: the whole store).
pub(super) fn app_offer(engine: &Engine, id: RequestId, identity_id: &str) -> Result<Option<AppOffer>, Stop> {
    Ok(policy_chain(engine, id, identity_id)?.and_then(|(policy, _)| AppOffer::from_policy(&policy)))
}

/// The Apps an App requires (`definition.requires.apps`), installed with it.
fn required_apps(app: &JsonValue) -> Vec<String> {
    app["definition"]["requires"]["apps"].as_array()
        .map(|list| list.iter().filter_map(JsonValue::as_str).filter(|id| !id.is_empty()).map(str::to_owned).collect()).unwrap_or_default()
}

fn app_name(app: &JsonValue) -> String {
    app.str_field("name").or_else(|| app.str_field("appId")).unwrap_or_default().to_owned()
}

/// `app.instance.create` under the Place Owner's policy (the Owner alone installs): the App is
/// included, and so is every App it requires.
pub(super) fn check_app_install(engine: &Engine, id: RequestId, owner_identity_id: &str, app: &JsonValue) -> Result<(), Stop> {
    let Some(offer) = app_offer(engine, id, owner_identity_id)? else { return Ok(()) };
    match offer.of(app) {
        Availability::Included => {}
        Availability::Addon => reject!(id, "policy.app_addon", format!("{:?} is an add-on of the policy {:?}: an administrator includes it first", app_name(app), offer.label)),
        Availability::Hidden => reject!(id, "policy.app_not_included", format!("{:?} is not included in the policy {:?}", app_name(app), offer.label)),
    }
    for required in required_apps(app) {
        // A requirement missing from the store is reported by the Hub, not here.
        let Some(other) = query_first(engine, id, quoted!("on _apps | where appId == {} and state == \"active\" | limit 1", &required))? else { continue };
        if offer.of(&other) != Availability::Included {
            reject!(id, "policy.app_requirement_not_included", format!("{:?} requires {:?}, which the policy {:?} does not include", app_name(app), app_name(&other), offer.label));
        }
    }
    Ok(())
}

/// Instances of `place_id` whose App its Owner's policy no longer includes: they stay, with their
/// data (still counted), and are read-only. Empty when nothing restricts the Owner's Apps.
pub(super) fn read_only_instances(engine: &Engine, id: RequestId, owner_identity_id: &str, instances: &[JsonValue]) -> Result<std::collections::BTreeSet<String>, Stop> {
    let mut read_only = std::collections::BTreeSet::new();
    let Some(offer) = app_offer(engine, id, owner_identity_id)? else { return Ok(read_only) };
    let mut apps: HashMap<String, Availability> = HashMap::new();
    for instance in instances {
        let (Some(instance_id), Some(app_id)) = (instance.str_field("instanceId"), instance.str_field("appId")) else { continue };
        let availability = match apps.get(app_id) {
            Some(availability) => *availability,
            None => {
                // An App deleted from the store keeps its instances as they were.
                let availability = query_first(engine, id, quoted!("on _apps | where appId == {} and state == \"active\" | limit 1", app_id))?.map_or(Availability::Included, |app| offer.of(&app));
                apps.insert(app_id.to_owned(), availability);
                availability
            }
        };
        if availability != Availability::Included { read_only.insert(instance_id.to_owned()); }
    }
    Ok(read_only)
}

/// A write through an App instance whose App the Place Owner's policy no longer includes.
pub(super) fn check_instance_write(engine: &Engine, id: RequestId, place_id: &str, instance_id: &str) -> Result<(), Stop> {
    let Some(place) = query_first(engine, id, quoted!("on _places | where placeId == {} and state == \"active\" | limit 1", place_id))? else { return Ok(()) };
    let Some(instance) = query_first(engine, id, quoted!("on _app_instances | where instanceId == {} and state == \"active\" | limit 1", instance_id))? else { return Ok(()) };
    if !read_only_instances(engine, id, &place.string_field("ownerIdentityId"), std::slice::from_ref(&instance))?.is_empty() {
        reject!(id, "policy.app_read_only", format!("{:?} is no longer included in the policy of the Place Owner: it is read-only", instance.str_field("name").unwrap_or_else(|| instance.str_field("appId").unwrap_or_default())));
    }
    Ok(())
}

/// The collections an App's tables declare (`model.tables.*.collection`, named in `model.collections`).
fn app_collections(app: &JsonValue) -> std::collections::BTreeSet<String> {
    let model = &app["definition"]["model"];
    model["tables"].as_object().into_iter().flatten().filter_map(|(_, table)| {
        let declared = &model["collections"][table.string_field("collection")];
        declared.as_str().or_else(|| declared.str_field("name")).filter(|name| !name.is_empty()).map(str::to_owned)
    }).collect()
}

/// A write in a Place outside any App instance (a direct query) to `collection`: refused when the
/// collection belongs to an instance its Owner's policy made read-only and no writable instance of
/// the Place declares it (a collection several Apps share stays writable while one of them is included).
pub(super) fn check_collection_write(engine: &Engine, id: RequestId, place_id: &str, collection: &str) -> Result<(), Stop> {
    let Some(place) = query_first(engine, id, quoted!("on _places | where placeId == {} and state == \"active\" | limit 1", place_id))? else { return Ok(()) };
    let owner = place.string_field("ownerIdentityId");
    if app_offer(engine, id, &owner)?.is_none() { return Ok(()); }
    let instances = query_documents(engine, id, quoted!("on _app_instances | where placeId == {} and state == \"active\"", place_id))?;
    let read_only = read_only_instances(engine, id, &owner, &instances)?;
    if read_only.is_empty() { return Ok(()); }
    let (mut locked_by, mut writable) = (None, false);
    let mut apps: HashMap<String, std::collections::BTreeSet<String>> = HashMap::new();
    for instance in &instances {
        let app_id = instance.string_field("appId");
        if !apps.contains_key(&app_id) {
            let collections = query_first(engine, id, quoted!("on _apps | where appId == {} and state == \"active\" | limit 1", &app_id))?.map(|app| app_collections(&app)).unwrap_or_default();
            apps.insert(app_id.clone(), collections);
        }
        if !apps[&app_id].contains(collection) { continue; }
        if read_only.contains(&instance.string_field("instanceId")) { locked_by.get_or_insert_with(|| instance.str_field("name").unwrap_or(&app_id).to_owned()); } else { writable = true; }
    }
    if let (Some(name), false) = (locked_by, writable) {
        reject!(id, "policy.app_read_only", format!("{collection:?} belongs to {name:?}, no longer included in the policy of the Place Owner: it is read-only"));
    }
    Ok(())
}

// ---- Admin operations (permission `policy.manage`; `identity.state.set`: `identity.manage`) ----

pub(super) fn handle_policy_operation(writer: &mut TcpStream, engine: &Engine, principal: &Principal, operation: RoutedOperation) -> Handled {
    match operation {
        RoutedOperation::IdentityStateSet(Routed { id, input: IdentityStateSetInput { identity_id, state } }) => {
            if state != "active" && state != "inactive" { reject!(id, "identity.invalid_state", "an identity state is active or inactive"); }
            if principal_identity_id(principal) == Some(identity_id.as_str()) { reject!(id, "identity.self_state", "an identity cannot change its own state"); }
            let Some(identity) = query_first(engine, id, quoted!("on _identities | where identityId == {} | limit 1", &identity_id))? else {
                reject!(id, "identity.not_found", "no identity has this identityId");
            };
            // Coming back takes a seat again in the policy and its pool.
            let policy_id = identity.string_field("policyId");
            if state == "active" && identity.str_field("state") != Some("active") && !policy_id.is_empty() {
                let policy = active_policy(engine, id, &policy_id)?;
                check_seat(engine, id, &policy, false)?;
            }
            let now = unix_time_millis();
            let stamp = if state == "inactive" { format!(", deactivatedAt = {now}") } else { String::new() };
            query_documents(engine, id, format!("on _identities | where identityId == {} | set state = {}, updatedAt = {now}{stamp}", query_string(&identity_id), query_string(&state)))?;
            // Connections already signed in keep their session until they reconnect.
            publish_durable_event(engine, Audience::identities([identity_id.clone()]), "identity.state.updated", serde_json::json!({"identityId": identity_id, "state": state, "updatedAt": now}));
            Ok(reply(writer, id, serde_json::json!({"identityId": identity_id, "state": state, "updatedAt": now}))?)
        }
        // Identities for the admin screen: those of one policy, else all (people and gateways), with
        // their classic login if any and their active devices.
        RoutedOperation::IdentityList(Routed { id, input: IdentityListInput { policy_id } }) => {
            let query = match policy_id.as_deref() {
                Some(policy_id) => quoted!("on _identities | where policyId? == {} | sort createdAt", policy_id),
                None => "on _identities | sort createdAt".to_owned(),
            };
            let mut identities = Vec::new();
            for identity in query_documents(engine, id, query)? {
                let identity_id = identity.string_field("identityId");
                let login = query_first(engine, id, quoted!("on _credentials | where identityId == {} | limit 1", &identity_id))?.map(|credential| credential.string_field("login"));
                let devices = count(engine, id, quoted!("on _devices | where identityId == {} and state == \"active\"", &identity_id))?;
                let mut entry = serde_json::json!({"identityId": identity_id, "login": login, "devices": devices, "current": principal_identity_id(principal) == Some(identity_id.as_str())});
                for field in ["policyId", "state", "kind", "createdAt", "deactivatedAt"] {
                    if let Some(value) = identity.get(field) { entry[field] = value.clone(); }
                }
                identities.push(entry);
            }
            Ok(reply(writer, id, serde_json::json!({"identities": identities}))?)
        }
        RoutedOperation::PoolCreate(Routed { id, input: PoolCreateInput { label, limits } }) => {
            let label = label_of(id, &label)?;
            let limits = match limits.as_ref().map(|limits| section("limits", limits, POOL_LIMITS, Kind::Count)).transpose() {
                Ok(limits) => limits.unwrap_or_else(|| serde_json::json!({})),
                Err(message) => reject!(id, "policy.invalid_setting", message),
            };
            let pool = serde_json::json!({"poolId": UuidV7Generator::new().next_id().to_string(), "label": label, "limits": limits, "state": "active", "createdAt": unix_time_millis()});
            query_documents(engine, id, insert_query("_pools", &pool))?;
            Ok(reply(writer, id, serde_json::json!({"pool": pool}))?)
        }
        RoutedOperation::PoolUpdate(Routed { id, input: PoolUpdateInput { pool_id, label, limits } }) => {
            active_pool(engine, id, &pool_id)?;
            let mut assignments = vec![format!("updatedAt = {}", unix_time_millis())];
            if let Some(label) = label { assignments.push(format!("label = {}", query_string(&label_of(id, &label)?))); }
            if let Some(limits) = limits {
                match section("limits", &limits, POOL_LIMITS, Kind::Count) {
                    Ok(limits) => assignments.push(format!("limits = {limits}")),
                    Err(message) => reject!(id, "policy.invalid_setting", message),
                }
            }
            query_documents(engine, id, format!("on _pools | where poolId == {} and state == \"active\" | set {}", query_string(&pool_id), assignments.join(", ")))?;
            Ok(reply(writer, id, serde_json::json!({"pool": active_pool(engine, id, &pool_id)?}))?)
        }
        RoutedOperation::PoolList(Routed { id, .. }) => {
            let mut pools = query_documents(engine, id, "on _pools | where state == \"active\" | sort label")?;
            for pool in &mut pools {
                let users = pool_users(engine, id, &pool.string_field("poolId"))?;
                pool["users"] = users.into();
            }
            Ok(reply(writer, id, serde_json::json!({"pools": pools}))?)
        }
        RoutedOperation::PoolDelete(Routed { id, input: PoolIdInput { pool_id } }) => {
            active_pool(engine, id, &pool_id)?;
            if count(engine, id, quoted!("on _policies | where poolId == {} and state == \"active\"", &pool_id))? > 0 {
                reject!(id, "pool.not_empty", "the pool still has active policies: delete them first");
            }
            let deleted_at = unix_time_millis();
            query_documents(engine, id, format!("on _pools | where poolId == {} | set state = \"deleted\", deletedAt = {deleted_at}", query_string(&pool_id)))?;
            Ok(reply(writer, id, serde_json::json!({"poolId": pool_id, "state": "deleted", "deletedAt": deleted_at}))?)
        }
        RoutedOperation::PolicyCreate(Routed { id, input: PolicyCreateInput { pool_id, label, limits, per_user, rights, apps } }) => {
            active_pool(engine, id, &pool_id)?;
            let label = label_of(id, &label)?;
            let mut policy = serde_json::json!({"policyId": UuidV7Generator::new().next_id().to_string(), "poolId": pool_id, "label": label, "limits": {}, "perUser": {}, "rights": {}, "apps": {}, "state": "active", "createdAt": unix_time_millis()});
            for (field, value) in policy_sections(id, limits.as_ref(), per_user.as_ref(), rights.as_ref(), apps.as_ref())? { policy[field] = value; }
            query_documents(engine, id, insert_query("_policies", &policy))?;
            Ok(reply(writer, id, serde_json::json!({"policy": policy}))?)
        }
        RoutedOperation::PolicyUpdate(Routed { id, input: PolicyUpdateInput { policy_id, label, limits, per_user, rights, apps } }) => {
            active_policy(engine, id, &policy_id)?;
            let mut assignments = vec![format!("updatedAt = {}", unix_time_millis())];
            if let Some(label) = label { assignments.push(format!("label = {}", query_string(&label_of(id, &label)?))); }
            for (field, value) in policy_sections(id, limits.as_ref(), per_user.as_ref(), rights.as_ref(), apps.as_ref())? { assignments.push(format!("{field} = {value}")); }
            query_documents(engine, id, format!("on _policies | where policyId == {} and state == \"active\" | set {}", query_string(&policy_id), assignments.join(", ")))?;
            Ok(reply(writer, id, serde_json::json!({"policy": active_policy(engine, id, &policy_id)?}))?)
        }
        RoutedOperation::PolicyList(Routed { id, input: PolicyListInput { pool_id } }) => {
            let filter = pool_id.map(|pool_id| format!(" and poolId == {}", query_string(&pool_id))).unwrap_or_default();
            let mut policies = query_documents(engine, id, format!("on _policies | where state == \"active\"{filter} | sort label"))?;
            for policy in &mut policies {
                let users = policy_users(engine, id, &policy.string_field("policyId"))?;
                policy["users"] = users.into();
            }
            Ok(reply(writer, id, serde_json::json!({"policies": policies}))?)
        }
        RoutedOperation::PolicyDelete(Routed { id, input: PolicyIdInput { policy_id } }) => {
            active_policy(engine, id, &policy_id)?;
            if policy_users(engine, id, &policy_id)? > 0 {
                reject!(id, "policy.in_use", "active identities hold this policy: move them first (identity.policy.set)");
            }
            let deleted_at = unix_time_millis();
            query_documents(engine, id, format!("on _policy_tokens | where policyId == {} and state == \"active\" | set state = \"revoked\", revokedAt = {deleted_at}", query_string(&policy_id)))?;
            query_documents(engine, id, format!("on _policies | where policyId == {} | set state = \"deleted\", deletedAt = {deleted_at}", query_string(&policy_id)))?;
            Ok(reply(writer, id, serde_json::json!({"policyId": policy_id, "state": "deleted", "deletedAt": deleted_at}))?)
        }
        RoutedOperation::PolicyTokenCreate(Routed { id, input: PolicyTokenCreateInput { policy_id, name } }) => {
            active_policy(engine, id, &policy_id)?;
            let name = name.map(|name| name.trim().to_owned()).unwrap_or_default();
            if name.len() > 80 { reject!(id, "policy.invalid_name", "a token name holds at most 80 bytes"); }
            let token = new_token().map_err(failed(id, "policy.token_failed"))?;
            let record = serde_json::json!({
                "tokenId": UuidV7Generator::new().next_id().to_string(), "policyId": policy_id, "name": name,
                "tokenHash": token_hash(&token), "tokenHint": &token[..TOKEN_PREFIX.len() + 6], "state": "active", "createdAt": unix_time_millis(),
            });
            query_documents(engine, id, insert_query("_policy_tokens", &record))?;
            // The token is shown once: only its hash is kept.
            Ok(reply(writer, id, serde_json::json!({"token": token, "record": token_json(&record)}))?)
        }
        RoutedOperation::PolicyTokenList(Routed { id, input: PolicyIdInput { policy_id } }) => {
            let tokens = query_documents(engine, id, quoted!("on _policy_tokens | where policyId == {} | sort createdAt desc", &policy_id))?.iter().map(token_json).collect::<Vec<_>>();
            Ok(reply(writer, id, serde_json::json!({"policyId": policy_id, "tokens": tokens}))?)
        }
        RoutedOperation::PolicyTokenRevoke(Routed { id, input: PolicyTokenRevokeInput { token_id } }) => {
            let selected = quoted!("on _policy_tokens | where tokenId == {} and state == \"active\"", &token_id);
            if count(engine, id, selected.clone())? == 0 { reject!(id, "policy.token_not_found", "no active token has this tokenId"); }
            let revoked_at = unix_time_millis();
            query_documents(engine, id, format!("{selected} | set state = \"revoked\", revokedAt = {revoked_at}"))?;
            Ok(reply(writer, id, serde_json::json!({"tokenId": token_id, "state": "revoked", "revokedAt": revoked_at}))?)
        }
        RoutedOperation::IdentityPolicySet(Routed { id, input: IdentityPolicySetInput { identity_id, policy_id } }) => {
            let Some(identity) = query_first(engine, id, quoted!("on _identities | where identityId == {} | limit 1", &identity_id))? else {
                reject!(id, "identity.not_found", "no identity has this identityId");
            };
            let current = identity.string_field("policyId");
            let assignment = match policy_id.as_deref() {
                Some(policy_id) => {
                    let policy = active_policy(engine, id, policy_id)?;
                    let active = identity.str_field("state") == Some("active");
                    check_seat(engine, id, &policy, !active || current == policy_id)?;
                    format!("policyId = {}", query_string(policy_id))
                }
                None => "policyId = null".to_owned(),
            };
            query_documents(engine, id, format!("on _identities | where identityId == {} | set {assignment}, updatedAt = {}", query_string(&identity_id), unix_time_millis()))?;
            Ok(reply(writer, id, serde_json::json!({"identityId": identity_id, "policyId": policy_id}))?)
        }
        other => super::serve::diverged(other.id()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn sections_accept_known_settings_and_refuse_the_rest() {
        let ok = |value: JsonValue, keys, kind| section("s", &value, keys, kind).is_ok();
        assert!(ok(serde_json::json!({}), POOL_LIMITS, Kind::Count));
        assert!(ok(serde_json::json!({"users": 20, "storage": 100_000_000_000_u64}), POOL_LIMITS, Kind::Count));
        assert!(ok(serde_json::json!({"devices": 3}), PER_USER, Kind::Count));
        // 10 GB as msgpackr sends it (a float64): kept as the integer.
        assert_eq!(section("s", &serde_json::json!({"storage": 10_000_000_000.0_f64}), POOL_LIMITS, Kind::Count).unwrap()["storage"].as_u64(), Some(10_000_000_000));
        assert!(ok(serde_json::json!({"createPlace": false}), RIGHTS, Kind::Flag));
        assert!(ok(serde_json::json!({"included": ["finance"], "addons": []}), APPS, Kind::Ids));
        for (value, keys, kind) in [
            (serde_json::json!({"user": 20}), POOL_LIMITS, Kind::Count),
            (serde_json::json!({"users": -1}), POOL_LIMITS, Kind::Count),
            (serde_json::json!({"users": 1.5}), POOL_LIMITS, Kind::Count),
            (serde_json::json!({"devices": 0}), PER_USER, Kind::Count),
            (serde_json::json!({"createPlace": "no"}), RIGHTS, Kind::Flag),
            (serde_json::json!({"included": [""]}), APPS, Kind::Ids),
            (serde_json::json!([1]), POOL_LIMITS, Kind::Count),
        ] { assert!(section("s", &value, keys, kind).is_err(), "{value}"); }
    }

    /// Seats and devices are counted among identities that mostly have no policy (the admin, older ones).
    #[test] fn seats_and_devices_count_policy_identities_only() {
        let engine = Engine::new(Arc::new(crate::storage::MemoryStorage::new()) as Arc<dyn StorageEngine>);
        let id = RequestId::Number(1);
        for query in [
            insert_query("_pools", serde_json::json!({"poolId": "pool", "label": "Acme", "limits": {"users": 2}, "state": "active"})),
            insert_query("_policies", serde_json::json!({"policyId": "std", "poolId": "pool", "label": "Std", "perUser": {"devices": 1}, "state": "active"})),
            insert_query("_identities", serde_json::json!({"identityId": "admin", "state": "active"})),
            insert_query("_identities", serde_json::json!({"identityId": "a", "policyId": "std", "state": "active"})),
            insert_query("_identities", serde_json::json!({"identityId": "gone", "policyId": "std", "state": "inactive"})),
            insert_query("_devices", serde_json::json!({"deviceId": "a1", "identityId": "a", "state": "active"})),
            insert_query("_devices", serde_json::json!({"deviceId": "admin1", "identityId": "admin", "state": "active"})),
        ] { query_run(&engine, id, query).unwrap(); }
        assert_eq!(policy_users(&engine, id, "std").ok(), Some(1));
        assert_eq!(pool_users(&engine, id, "pool").ok(), Some(1));
        assert!(enrollment_seat(&engine, id, "std").is_ok());
        assert!(check_new_device(&engine, id, "a").is_err());
        assert!(check_new_device(&engine, id, "admin").is_ok());
        query_run(&engine, id, insert_query("_identities", serde_json::json!({"identityId": "b", "policyId": "std", "state": "active"}))).unwrap();
        assert!(enrollment_seat(&engine, id, "std").is_err());
    }

    #[test] fn tokens_are_prefixed_random_and_hashed() {
        let (a, b) = (new_token().unwrap(), new_token().unwrap());
        assert!(a.starts_with(TOKEN_PREFIX) && a.len() == TOKEN_PREFIX.len() + 64 && a != b);
        assert_eq!(token_hash(&a).len(), 64);
        assert_ne!(token_hash(&a), token_hash(&b));
    }

    #[test] fn app_offer_includes_platform_and_private_apps_and_sorts_the_catalog() {
        let app = |app_id: &str, created_by: &str, visibility: &str, store: bool| {
            let mut definition = serde_json::json!({"id": app_id});
            if store { definition["store"] = serde_json::json!({"category": {"en": "Finance"}}); }
            serde_json::json!({"appId": app_id, "name": app_id, "createdBy": created_by, "visibility": visibility, "definition": definition})
        };
        // No selection: the whole store.
        assert!(AppOffer::from_policy(&serde_json::json!({"label": "p"})).is_none());
        assert!(AppOffer::from_policy(&serde_json::json!({"label": "p", "apps": {}})).is_none());
        let offer = AppOffer::from_policy(&serde_json::json!({"label": "p", "apps": {"included": ["system.finance"], "addons": ["system.finance.rh"]}})).expect("a selection restricts");
        let files = app("system.files", "system", "public", false);
        assert!(app_is_platform(&files));
        assert_eq!(offer.of(&files), Availability::Included);
        assert_eq!(offer.of(&app("system.finance", "system", "public", true)), Availability::Included);
        assert_eq!(offer.of(&app("system.finance.rh", "system", "public", true)), Availability::Addon);
        assert_eq!(offer.of(&app("system.finance.bilan", "system", "public", true)), Availability::Hidden);
        assert_eq!(offer.of(&app("acme.crm", "someone", "public", false)), Availability::Hidden);
        assert_eq!(offer.of(&app("acme.mine", "someone", "private", false)), Availability::Included);
        // Only addons given: nothing included but the platform and private Apps.
        let addons_only = AppOffer::from_policy(&serde_json::json!({"apps": {"addons": ["acme.crm"]}})).expect("a selection restricts");
        assert_eq!(addons_only.of(&app("acme.crm", "someone", "public", false)), Availability::Addon);
        assert_eq!(addons_only.of(&app("system.finance", "system", "public", true)), Availability::Hidden);
        assert_eq!(required_apps(&serde_json::json!({"definition": {"requires": {"apps": ["system.finance", ""]}}})), vec!["system.finance".to_owned()]);
    }

    #[test] fn app_collections_follow_the_model() {
        let app = serde_json::json!({"definition": {"model": {
            "collections": {"ventes": "sales_rows", "stock": {"name": "stock_rows"}, "empty": ""},
            "tables": {"a": {"collection": "ventes"}, "b": {"collection": "stock"}, "c": {"collection": "empty"}, "d": {"collection": "unknown"}}}}});
        assert_eq!(app_collections(&app).into_iter().collect::<Vec<_>>(), vec!["sales_rows".to_owned(), "stock_rows".to_owned()]);
        assert!(app_collections(&serde_json::json!({"definition": {}})).is_empty());
    }
}
