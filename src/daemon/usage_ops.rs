//! Stored bytes per Place, Owner, policy and pool (step 2 of POLICIES in `wip/README.md`), shown and never
//! enforced. A worker computes them every `OGD_USAGE_INTERVAL` seconds (300 by default, 0 turns
//! it off): database bytes from the storage (`StorageEngine::usage_by("_place")`, method B on
//! Glacier), file bytes from the sizes in `_files` and `_file_versions`. `usage.get` answers the
//! last result: a user sees their own usage, their Places and their pool against its limits; with
//! `all`, an admin (policy.manage) sees everything.
//!
//! Step 3: the storage limits (`perUser.storage`, the policy's and the pool's `limits.storage`) are
//! checked against the last result. Past a limit, what adds documents or files to the Owner's Places
//! is refused; `set` and `delete` stay allowed. A file of known size is refused when it would cross
//! the limit. The overshoot is what can be written between two computations; changing a limit or a
//! policy, or deleting while over a limit, computes again at once.
#![cfg_attr(rustfmt, rustfmt_skip)]

use std::collections::BTreeMap;
use std::sync::RwLock;

use super::*;
use super::serve::{ensure_permission, reply, require_identity, Handled, Stop};
use crate::operation::UsageGetInput;

const DEFAULT_INTERVAL_SECONDS: u64 = 300;

#[derive(Clone, Copy, Debug, Default)]
struct Bytes { database: u64, files: u64 }

impl Bytes {
    fn add(&mut self, other: Self) { self.database += other.database; self.files += other.files; }
    fn json(self) -> JsonValue { serde_json::json!({"database": self.database, "files": self.files, "total": self.database + self.files}) }
}

#[derive(Debug, Default)]
struct Owned { bytes: Bytes, places: u64 }

/// One computation.
#[derive(Debug, Default)]
struct Usage {
    computed_at: u64,
    duration_ms: u64,
    /// Active Places: name, Owner, bytes.
    places: BTreeMap<String, (String, String, Bytes)>,
    /// Per Owner identity: its policy and what its Places hold.
    identities: BTreeMap<String, (String, Owned)>,
    policies: BTreeMap<String, (JsonValue, Owned)>,
    pools: BTreeMap<String, (JsonValue, Owned)>,
    /// Rows outside any active Place (system collections, deleted Places).
    unattributed: Bytes,
    /// The tightest storage limit of each identity under one.
    headroom: BTreeMap<String, Headroom>,
}

/// Room left under the tightest storage limit of an identity (negative past it).
#[derive(Clone, Debug)]
struct Headroom { remaining: i128, code: &'static str, scope: String, limit: u64, used: u64 }

static LAST: RwLock<Option<Arc<Usage>>> = RwLock::new(None);
static INTERVAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Set to compute again before the interval ends.
static WAKE: (std::sync::Mutex<bool>, std::sync::Condvar) = (std::sync::Mutex::new(false), std::sync::Condvar::new());

/// Computes again now (a limit or a policy changed, or data was deleted past a limit).
pub(super) fn refresh() {
    if let Ok(mut wake) = WAKE.0.lock() { *wake = true; WAKE.1.notify_all(); }
}

fn documents(engine: &Engine, query: &str) -> Vec<JsonValue> {
    // A collection not created yet (no file, no policy) reads as empty.
    query_documents(engine, 0, query).unwrap_or_default()
}

fn compute(engine: &Engine) -> Result<Usage, String> {
    let started = std::time::Instant::now();
    let mut per_place: BTreeMap<String, Bytes> = BTreeMap::new();
    for (place, bytes) in engine.storage().usage_by(PLACE_SCOPE_FIELD).map_err(|error| error.to_string())? {
        per_place.entry(place).or_default().database += bytes;
    }
    for query in ["on _files | where kind == \"file\"", "on _file_versions"] {
        for file in documents(engine, query) {
            per_place.entry(file.string_field(PLACE_SCOPE_FIELD)).or_default().files += file.u64_field("size").unwrap_or(0);
        }
    }
    let mut usage = Usage { computed_at: unix_time_millis(), ..Usage::default() };
    for place in documents(engine, "on _places | where state == \"active\"") {
        let place_id = place.string_field("placeId");
        let bytes = per_place.remove(&place_id).unwrap_or_default();
        usage.places.insert(place_id, (place.string_field("name"), place.string_field("ownerIdentityId"), bytes));
    }
    for bytes in per_place.into_values() { usage.unattributed.add(bytes); }
    for pool in documents(engine, "on _pools | where state == \"active\"") {
        usage.pools.insert(pool.string_field("poolId"), (pool, Owned::default()));
    }
    for policy in documents(engine, "on _policies | where state == \"active\"") {
        usage.policies.insert(policy.string_field("policyId"), (policy, Owned::default()));
    }
    let policy_of: BTreeMap<String, String> = documents(engine, "on _identities").into_iter().map(|identity| (identity.string_field("identityId"), identity.string_field("policyId"))).collect();
    for (_, owner, bytes) in usage.places.values() {
        let policy_id = policy_of.get(owner).cloned().unwrap_or_default();
        let entry = usage.identities.entry(owner.clone()).or_insert_with(|| (policy_id.clone(), Owned::default()));
        entry.1.bytes.add(*bytes);
        entry.1.places += 1;
        if let Some((policy, owned)) = usage.policies.get_mut(&policy_id) {
            owned.bytes.add(*bytes);
            owned.places += 1;
            if let Some((_, owned)) = usage.pools.get_mut(&policy.string_field("poolId")) {
                owned.bytes.add(*bytes);
                owned.places += 1;
            }
        }
    }
    for (identity_id, policy_id) in &policy_of {
        let Some((policy, policy_owned)) = usage.policies.get(policy_id) else { continue };
        let pool = usage.pools.get(&policy.string_field("poolId"));
        let used = usage.identities.get(identity_id).map_or(0, |(_, owned)| owned.bytes.database + owned.bytes.files);
        let total = |owned: &Owned| owned.bytes.database + owned.bytes.files;
        let mut candidates = vec![(storage_limit(policy, "perUser"), used, "policy.user_storage_limit", "your Places".to_owned()), (storage_limit(policy, "limits"), total(policy_owned), "policy.storage_limit", format!("the policy {:?}", policy.string_field("label")))];
        if let Some((pool, pool_owned)) = pool { candidates.push((storage_limit(pool, "limits"), total(pool_owned), "pool.storage_limit", format!("the pool {:?}", pool.string_field("label")))); }
        let tightest = candidates.into_iter().filter_map(|(limit, used, code, scope)| limit.map(|limit| Headroom { remaining: i128::from(limit) - i128::from(used), code, scope, limit, used })).min_by_key(|headroom| headroom.remaining);
        if let Some(headroom) = tightest { usage.headroom.insert(identity_id.clone(), headroom); }
    }
    usage.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(usage)
}

/// Starts the worker unless `OGD_USAGE_INTERVAL` is 0.
pub(super) fn start_usage_worker(engine: Arc<Engine>) {
    let interval = env::var("OGD_USAGE_INTERVAL").map_or(DEFAULT_INTERVAL_SECONDS, |value| value.trim().parse().unwrap_or_else(|_| { eprintln!("ogd: invalid OGD_USAGE_INTERVAL {value:?}, using {DEFAULT_INTERVAL_SECONDS} s"); DEFAULT_INTERVAL_SECONDS }));
    if interval == 0 { return; }
    INTERVAL.store(interval, std::sync::atomic::Ordering::Relaxed);
    thread::Builder::new()
        .name("og-usage".to_owned())
        .spawn(move || loop {
            match compute(&engine) {
                Ok(usage) => {
                    debug::log(DebugTopic::Storage, None, format!("usage computed in {} ms: {} Places", usage.duration_ms, usage.places.len()));
                    if let Ok(mut last) = LAST.write() { *last = Some(Arc::new(usage)); }
                }
                Err(error) => eprintln!("ogd: usage computation failed: {error}"),
            }
            if let Ok(wake) = WAKE.0.lock() {
                if let Ok(mut wake) = WAKE.1.wait_timeout_while(wake, Duration::from_secs(interval), |wake| !*wake).map(|(wake, _)| wake) { *wake = false; }
            }
        })
        .expect("usage worker must start");
}

fn storage_limit(document: &JsonValue, section: &str) -> Option<u64> { document.get(section).and_then(|section| section.get("storage")).and_then(JsonValue::as_u64) }

fn limits(document: &JsonValue, section: &str) -> JsonValue { document.get(section).cloned().unwrap_or_else(|| serde_json::json!({})) }

fn owned_json(owned: &Owned) -> JsonValue {
    let mut value = owned.bytes.json();
    value["places"] = owned.places.into();
    value
}

fn place_json(place_id: &str, (name, owner, bytes): &(String, String, Bytes)) -> JsonValue {
    let mut value = bytes.json();
    value["placeId"] = place_id.into();
    value["name"] = name.as_str().into();
    value["ownerIdentityId"] = owner.as_str().into();
    value
}

fn header(usage: Option<&Usage>) -> JsonValue {
    serde_json::json!({
        "computedAt": usage.map(|usage| usage.computed_at),
        "durationMs": usage.map(|usage| usage.duration_ms),
        "intervalSeconds": INTERVAL.load(std::sync::atomic::Ordering::Relaxed),
    })
}

pub(super) fn handle_usage_get(writer: &mut TcpStream, settings: &ConnectionSettings, engine: &Engine, principal: &Principal, Routed { id, input: UsageGetInput { all } }: Routed<UsageGetInput>) -> Handled {
    let identity_id = require_identity(principal, id, "an authenticated identity is required to read usage")?.to_owned();
    let last = LAST.read().ok().and_then(|last| last.clone());
    let mut answer = header(last.as_deref());
    let Some(usage) = last.as_deref() else {
        // Not computed yet (just started), or turned off.
        return Ok(reply(writer, id, answer)?);
    };
    if all {
        ensure_permission(settings, engine, principal, id, AuthorizationAction::PolicyManage, "_policies")?;
        answer["unattributed"] = usage.unattributed.json();
        answer["pools"] = usage.pools.iter().map(|(pool_id, (pool, owned))| {
            let mut value = owned_json(owned);
            value["poolId"] = pool_id.as_str().into();
            value["label"] = pool.get("label").cloned().unwrap_or_default();
            value["limits"] = limits(pool, "limits");
            value
        }).collect();
        answer["policies"] = usage.policies.iter().map(|(policy_id, (policy, owned))| {
            let mut value = owned_json(owned);
            value["policyId"] = policy_id.as_str().into();
            value["poolId"] = policy.get("poolId").cloned().unwrap_or_default();
            value["label"] = policy.get("label").cloned().unwrap_or_default();
            value["limits"] = limits(policy, "limits");
            value["perUser"] = limits(policy, "perUser");
            value
        }).collect();
        answer["identities"] = usage.identities.iter().map(|(identity_id, (policy_id, owned))| {
            let mut value = owned_json(owned);
            value["identityId"] = identity_id.as_str().into();
            value["policyId"] = if policy_id.is_empty() { JsonValue::Null } else { policy_id.as_str().into() };
            value
        }).collect();
        answer["places"] = usage.places.iter().map(|(place_id, place)| place_json(place_id, place)).collect();
        return Ok(reply(writer, id, answer)?);
    }
    let policy_id = query_first(engine, id, quoted!("on _identities | where identityId == {} | limit 1", &identity_id))?.map(|identity| identity.string_field("policyId")).unwrap_or_default();
    let mut mine = usage.identities.get(&identity_id).map(|(_, owned)| owned_json(owned)).unwrap_or_else(|| owned_json(&Owned::default()));
    mine["identityId"] = identity_id.as_str().into();
    let policy = usage.policies.get(&policy_id).map(|(policy, _)| policy);
    mine["limits"] = policy.map_or_else(|| serde_json::json!({}), |policy| limits(policy, "perUser"));
    answer["identity"] = mine;
    answer["places"] = usage.places.iter().filter(|(_, (_, owner, _))| *owner == identity_id).map(|(place_id, place)| place_json(place_id, place)).collect();
    // The pool as a whole, against its own limits; the other members are not listed.
    if let Some((pool, owned)) = policy.and_then(|policy| usage.pools.get(&policy.string_field("poolId"))) {
        let mut value = owned_json(owned);
        value["poolId"] = pool.get("poolId").cloned().unwrap_or_default();
        value["label"] = pool.get("label").cloned().unwrap_or_default();
        value["limits"] = limits(pool, "limits");
        answer["pool"] = value;
    }
    Ok(reply(writer, id, answer)?)
}

/// The Owner of a Place: from the last result, else (a Place created since) from `_places`.
fn owner_of(engine: &Engine, id: RequestId, usage: &Usage, place_id: &str) -> Result<Option<String>, Stop> {
    if let Some((_, owner, _)) = usage.places.get(place_id) { return Ok(Some(owner.clone())); }
    Ok(query_first(engine, id, quoted!("on _places | where placeId == {} | limit 1", place_id))?.map(|place| place.string_field("ownerIdentityId")))
}

fn size(bytes: u64) -> String {
    let (value, unit) = match bytes {
        0..1_000 => return format!("{bytes} B"),
        1_000..1_000_000 => (bytes as f64 / 1e3, "kB"),
        1_000_000..1_000_000_000 => (bytes as f64 / 1e6, "MB"),
        _ => (bytes as f64 / 1e9, "GB"),
    };
    format!("{value:.1} {unit}")
}

/// Refuses to add documents or files to `place_id` once its Owner is past a storage limit, or, for
/// `incoming` bytes announced in advance, when they would cross it. Without a computed usage (just
/// started, or OGD_USAGE_INTERVAL=0), nothing is refused.
pub(super) fn check_storage(engine: &Engine, id: RequestId, place_id: &str, incoming: u64) -> Result<(), Stop> {
    let Some(usage) = LAST.read().ok().and_then(|last| last.clone()) else { return Ok(()) };
    if usage.headroom.is_empty() { return Ok(()); }
    let Some(owner) = owner_of(engine, id, &usage, place_id)? else { return Ok(()) };
    let Some(headroom) = usage.headroom.get(&owner) else { return Ok(()) };
    if headroom.remaining <= 0 {
        reject!(id, headroom.code, format!("storage limit reached for {}: {} used of {}; delete data or files first", headroom.scope, size(headroom.used), size(headroom.limit)));
    }
    if i128::from(incoming) > headroom.remaining {
        reject!(id, headroom.code, format!("{} would exceed the storage limit of {}: {} used of {}", size(incoming), headroom.scope, size(headroom.used), size(headroom.limit)));
    }
    Ok(())
}

/// After a write that does not add documents in `place_id`: when its Owner is past a limit, the
/// usage is computed again at once, so that deleting lifts the refusal without waiting.
pub(super) fn note_shrink(place_id: &str) {
    let Some(usage) = LAST.read().ok().and_then(|last| last.clone()) else { return };
    let Some((_, owner, _)) = usage.places.get(place_id) else { return };
    if usage.headroom.get(owner).is_some_and(|headroom| headroom.remaining <= 0) { refresh(); }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_rolls_places_up_to_owner_policy_and_pool() {
        let engine = Engine::new(Arc::new(crate::storage::MemoryStorage::new()) as Arc<dyn StorageEngine>);
        for query in [
            "on _pools | insert {\"poolId\":\"pool-1\",\"label\":\"Acme\",\"limits\":{\"storage\":1000},\"state\":\"active\"}",
            "on _policies | insert {\"policyId\":\"policy-1\",\"poolId\":\"pool-1\",\"label\":\"Std\",\"state\":\"active\"}",
            "on _identities | insert {\"identityId\":\"alice\",\"policyId\":\"policy-1\",\"state\":\"active\"}",
            "on _identities | insert {\"identityId\":\"admin\",\"state\":\"active\"}",
            "on _places | insert {\"placeId\":\"p1\",\"name\":\"One\",\"ownerIdentityId\":\"alice\",\"state\":\"active\"}",
            "on _places | insert {\"placeId\":\"p2\",\"name\":\"Two\",\"ownerIdentityId\":\"admin\",\"state\":\"active\"}",
            "on notes | insert {\"text\":\"hello\",\"_place\":\"p1\"}",
            "on notes | insert {\"text\":\"a longer text in the second place\",\"_place\":\"p2\"}",
            "on _files | insert {\"kind\":\"file\",\"size\":40,\"_place\":\"p1\"}",
            "on _files | insert {\"kind\":\"directory\",\"_place\":\"p1\"}",
            "on _file_versions | insert {\"size\":10,\"_place\":\"p1\"}",
        ] {
            query_run(&engine, 0, query).unwrap();
        }
        let usage = compute(&engine).unwrap();
        let (_, _, p1) = &usage.places["p1"];
        assert!(p1.database > 0);
        assert_eq!(p1.files, 50);
        let (_, alice) = &usage.identities["alice"];
        assert_eq!((alice.places, alice.bytes.files), (1, 50));
        assert_eq!(usage.policies["policy-1"].1.bytes.database, p1.database);
        assert_eq!(usage.pools["pool-1"].1.bytes.database, p1.database);
        // The admin has no policy: its Place counts for itself only.
        assert_eq!(usage.identities["admin"].0, "");
        assert_eq!(usage.pools["pool-1"].1.places, 1);
        // System collections have no Place.
        assert!(usage.unattributed.database > 0);
    }

    #[test]
    fn the_tightest_storage_limit_gives_the_headroom() {
        let engine = Engine::new(Arc::new(crate::storage::MemoryStorage::new()) as Arc<dyn StorageEngine>);
        for query in [
            "on _pools | insert {\"poolId\":\"pool-1\",\"label\":\"Acme\",\"limits\":{\"storage\":100000000000},\"state\":\"active\"}",
            "on _policies | insert {\"policyId\":\"policy-1\",\"poolId\":\"pool-1\",\"label\":\"Std\",\"perUser\":{\"storage\":40},\"state\":\"active\"}",
            "on _policies | insert {\"policyId\":\"policy-2\",\"poolId\":\"pool-1\",\"label\":\"Free\",\"state\":\"active\"}",
            "on _identities | insert {\"identityId\":\"alice\",\"policyId\":\"policy-1\",\"state\":\"active\"}",
            "on _identities | insert {\"identityId\":\"bob\",\"policyId\":\"policy-2\",\"state\":\"active\"}",
            "on _identities | insert {\"identityId\":\"admin\",\"state\":\"active\"}",
            "on _places | insert {\"placeId\":\"p1\",\"name\":\"One\",\"ownerIdentityId\":\"alice\",\"state\":\"active\"}",
            "on _files | insert {\"kind\":\"file\",\"size\":50,\"_place\":\"p1\"}",
        ] {
            query_run(&engine, 0, query).unwrap();
        }
        let usage = compute(&engine).unwrap();
        // Alice: 50 bytes of files (and a little database) past her 40 per user.
        let alice = &usage.headroom["alice"];
        assert_eq!((alice.code, alice.limit), ("policy.user_storage_limit", 40));
        assert!(alice.remaining < -10, "{alice:?}");
        // Bob, with no Place, is under the pool's limit only.
        let bob = &usage.headroom["bob"];
        assert_eq!((bob.code, bob.limit, bob.used), ("pool.storage_limit", 100_000_000_000, usage.pools["pool-1"].1.bytes.database + 50));
        // No policy, no limit.
        assert!(!usage.headroom.contains_key("admin"));
    }

}
