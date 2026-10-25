//! Permission, Sharing, Place, App and data import requests.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;
use super::serve::{diverged, ensure_operation_resource_authorized, place_role, reply, require_identity, require_place_owner, write_query_then_publish, Handled};

/// The active App instances of a Place.
#[cfg(feature = "database")]
fn active_instance_ids(engine: &Engine, id: RequestId, place_id: &str) -> Result<std::collections::BTreeSet<String>, QueryResponse> {
    Ok(query_documents(engine, id, quoted!("on _app_instances | where placeId == {} and state == \"active\"", place_id))?
        .iter().filter_map(|instance| instance.str_field("instanceId").map(str::to_owned)).collect())
}

#[cfg(feature = "database")]
pub(super) fn handle_place_operation(writer: &mut TcpStream, settings: &ConnectionSettings, engine: &Engine, authentication: &ConnectionAuth, operation: RoutedOperation) -> Handled {
    let principal = authentication.principal();
    match operation {
        RoutedOperation::PermissionGrant(Routed { id, input: PermissionGrantInput { identity_id, action, resource, created_at }, }) => {
            let created_at = created_at.unwrap_or_else(unix_time_millis);
            let query = insert_query("_permissions", serde_json::json!({"identityId": &identity_id, "action": &action, "resource": &resource, "effect": "allow", "state": "active", "createdAt": created_at}));
            write_query_then_publish(writer, engine, id, query, Audience::identities([identity_id.clone()]), "permission.granted", serde_json::json!({"identityId": identity_id, "action": action, "resource": resource, "createdAt": created_at}))
        }
        RoutedOperation::PermissionRevoke(Routed { id, input: PermissionRevokeInput { identity_id, action, resource, revoked_at }, }) => {
            let revoked_at = revoked_at.unwrap_or_else(unix_time_millis);
            let query = quoted!("on _permissions | where identityId == {} and action == {} and resource == {} and state == \"active\" | set state = \"revoked\", revokedAt = {revoked_at}", &identity_id, &action, &resource);
            write_query_then_publish(writer, engine, id, query, Audience::identities([identity_id.clone()]), "permission.revoked", serde_json::json!({"identityId": identity_id, "action": action, "resource": resource, "revokedAt": revoked_at}))
        }
        RoutedOperation::SharingCreate(Routed { id, input: SharingCreateInput { sharing_id, owner, target, permissions, state, created_at }, }) => {
            let created_at = created_at.unwrap_or_else(unix_time_millis);
            let query = insert_query("_sharings", serde_json::json!({"sharingId": &sharing_id, "owner": &owner, "target": &target, "permissions": &permissions, "state": &state, "createdAt": created_at}));
            write_query_then_publish(writer, engine, id, query, Audience::identities([owner.clone(), target.clone()]), "sharing.created", serde_json::json!({ "sharing": { "sharingId": sharing_id, "owner": owner, "target": target, "permissions": permissions, "state": state, "createdAt": created_at, } }))
        }
        RoutedOperation::SharingUpdate(Routed { id, input: SharingUpdateInput { sharing_id, permissions, state, updated_at }, }) => {
            let sharing = load_sharing(engine, id, &sharing_id)?;
            let updated_at = updated_at.unwrap_or_else(unix_time_millis);
            let mut assignments = Vec::new();
            if let Some(permissions) = permissions.as_ref() {
                assignments.push(format!(
                    "permissions = {}",
                    serde_json::to_string(permissions)
                        .expect("sharing permissions serialize")
                ));
            }
            if let Some(state) = state.as_ref() {
                assignments.push(format!("state = {}", query_string(state)));
            }
            assignments.push(format!("updatedAt = {updated_at}"));
            let query = format!( "on _sharings | where sharingId == {} | set {}", query_string(&sharing_id), assignments.join(", "), );
            write_query_then_publish(writer, engine, id, query, Audience::identities([ sharing.owner.clone(), sharing.target.clone(), ]), "sharing.updated", serde_json::json!({ "sharingId": sharing_id, "owner": sharing.owner, "target": sharing.target, "permissions": permissions, "state": state, "updatedAt": updated_at, }))
        }
        RoutedOperation::SharingDelete(Routed { id, input: SharingDeleteInput { sharing_id, deleted_at }, }) => {
            let sharing = load_sharing(engine, id, &sharing_id)?;
            let deleted_at = deleted_at.unwrap_or_else(unix_time_millis);
            let query = quoted!("on _sharings | where sharingId == {} | delete", &sharing_id);
            write_query_then_publish(writer, engine, id, query, Audience::identities([ sharing.owner.clone(), sharing.target.clone(), ]), "sharing.deleted", serde_json::json!({ "sharingId": sharing_id, "owner": sharing.owner, "target": sharing.target, "deletedAt": deleted_at, }))
        }
        RoutedOperation::PlaceCreate(Routed { id, input: PlaceCreateInput { name, mood, public_access, created_at }, }) => {
            let owner_identity_id = require_identity(principal, id, "an authenticated identity is required to create a Place")?;
            if public_access.is_some() && !settings.allow_public { reject!(id, "place.public_disabled", "public Places are disabled on this Core (OGD_ALLOW_PUBLIC)"); }
            #[cfg(feature = "auth")]
            super::policy_ops::check_place_create(engine, id, owner_identity_id, public_access.is_some())?;
            let place_id = UuidV7Generator::new().next_id().to_string();
            let created_at = created_at.unwrap_or_else(unix_time_millis);
            let mut document = serde_json::json!({"placeId": &place_id, "name": &name, "mood": &mood, "ownerIdentityId": owner_identity_id, "state": "active", "createdAt": created_at});
            if let Some(access) = public_access {
                document["publicAccess"] = access.as_str().into();
            }
            let query = insert_query("_places", document);
            query_documents(engine, id, query)?;
            Ok(reply(writer, id, serde_json::json!({ "placeId": place_id, "name": name, "mood": mood, "ownerIdentityId": owner_identity_id, "role": PlaceRole::Owner.as_str(), "publicAccess": public_access.map(PublicAccess::as_str), "state": "active", "createdAt": created_at, }))?)
        }
        RoutedOperation::PlaceList(Routed { id, .. }) => {
            // Scope + public: the Places of the presented identity (with their role),
            // plus every public Place. Without an identity, only the public Places.
            let places = match principal_identity_id(principal) {
                Some(identity_id) => list_places_for_identity_and_public(engine, id, identity_id)?,
                None => list_public_places(engine, id)?,
            };
            Ok(reply(writer, id, serde_json::json!({ "places": places }))?)
        }
        RoutedOperation::PlaceGet(Routed { id, input: PlaceIdInput { place_id } }) => {
            let place = load_place(engine, id, &place_id)?;
            let access = resolve_place_access_for_principal(engine, id, principal, &place)?;
            Ok(reply(writer, id, place.to_json(access.role()))?)
        }
        RoutedOperation::PlaceUpdate(Routed { id, input: PlaceUpdateInput { place_id, name, title, subtitle, color_scheme, app_order, app_layout, profiles, tile_size, updated_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to update a Place")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only a Place Owner may update its presentation")?;
            let updated_at = updated_at.unwrap_or_else(unix_time_millis);
            let mut assignments = Vec::new();
            if let Some(name) = name.as_ref() { assignments.push(format!("name = {}", query_string(name))); }
            if let Some(title) = title.as_ref() { assignments.push(format!("title = {}", query_string(title))); }
            if let Some(subtitle) = subtitle.as_ref() { assignments.push(format!("subtitle = {}", query_string(subtitle))); }
            if let Some(color_scheme) = color_scheme.as_ref() { assignments.push(format!("colorScheme = {}", query_string(color_scheme))); }
            if let Some(tile_size) = tile_size.as_ref() { assignments.push(format!("tileSize = {}", query_string(tile_size))); }
            // With a layout, appOrder becomes its flattened order (read by older Hubs).
            let app_order = match app_layout.as_ref() {
                Some(layout) => {
                    let mut layout = layout.clone();
                    let placed = active_instance_ids(engine, id, &place_id)?;
                    AppLayoutEntry::retain(&mut layout, |instance| placed.contains(instance));
                    let layout_json = serde_json::to_string(&layout).expect("Place app layout serializes");
                    assignments.push(format!("appLayout = {layout_json}"));
                    Some(AppLayoutEntry::flatten(&layout))
                }
                None => app_order,
            };
            if let Some(app_order) = app_order.as_ref() {
                let order_json = serde_json::to_string(app_order).expect("Place app order serializes");
                assignments.push(format!("appOrder = {order_json}"));
            }
            // A profile given to a member stays; the Apps of a profile are instances of the Place.
            if let Some(mut profiles) = profiles.clone() {
                let given = query_documents(engine, id, quoted!("on _sharings | where state == \"active\" and starts_with(sharingId, {}) and profileId? != null | select profileId", &format!("place-access:{place_id}:")))?;
                if let Some(profile) = given.iter().filter_map(|sharing| sharing.str_field("profileId")).find(|given| !profiles.iter().any(|profile| profile.profile_id == *given)) {
                    reject!(id, "place.profile_in_use", format!("profile {profile:?} is given to members of the Place: give them another one first"));
                }
                let placed = active_instance_ids(engine, id, &place_id)?;
                for apps in profiles.iter_mut().filter_map(|profile| profile.apps.as_mut()) { apps.retain(|instance| placed.contains(instance)); }
                assignments.push(format!("profiles = {}", serde_json::to_string(&profiles).expect("Place profiles serialize")));
            }
            assignments.push(format!("updatedAt = {updated_at}"));
            let query = format!("on _places | where placeId == {} and state == \"active\" | set {}", query_string(&place_id), assignments.join(", "));
            query_documents(engine, id, query)?;
            let updated = load_place(engine, id, &place_id)?;
            let audience = place_audience(engine, id, &place_id)?;
            publish_durable_event(engine, audience, "place.updated", serde_json::json!({
                "placeId": place_id,
                "changed": {
                    "name": name.is_some(), "title": title.is_some(), "subtitle": subtitle.is_some(),
                    "colorScheme": color_scheme.is_some(), "appOrder": app_order.is_some(), "appLayout": app_layout.is_some(), "profiles": profiles.is_some(), "tileSize": tile_size.is_some()
                },
                "updatedAt": updated_at
            }));
            Ok(reply(writer, id, updated.to_json(Some(PlaceRole::Owner)))?)
        }
        RoutedOperation::PlaceDelete(Routed { id, input: PlaceDeleteInput { place_id, deleted_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to delete a Place")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only the Place Owner may delete it")?;
            let deleted_at = deleted_at.unwrap_or_else(unix_time_millis);
            let audience = place_audience(engine, id, &place_id)?;
            query_documents(engine, id, quoted!("on _app_instances | where placeId == {} and state == \"active\" | set state = \"removed\", removedAt = {deleted_at}", &place_id))?;
            query_documents(engine, id, quoted!("on _places | where placeId == {} and state == \"active\" | set state = \"deleted\", deletedAt = {deleted_at}", &place_id))?;
            publish_durable_event(engine, audience, "place.deleted", serde_json::json!({ "placeId": place_id, "deletedAt": deleted_at }));
            Ok(reply(writer, id, serde_json::json!({ "placeId": place_id, "state": "deleted", "deletedAt": deleted_at, }))?)
        }
        RoutedOperation::PlaceAccessList(Routed { id, input: PlaceIdInput { place_id } }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to inspect Place access")?;
            let place = load_place(engine, id, &place_id)?;
            place_role(engine, id, identity_id, &place)?;
            let mut entries = list_place_access(engine, id, &place)?;
            // With classic authentication, each member shows the identifier it signs in with.
            if settings.classic_auth_enabled {
                for entry in &mut entries {
                    let Some(member) = entry.str_field("identityId").map(str::to_owned) else { continue };
                    let credentials = query_documents(engine, id, quoted!("on _credentials | where identityId == {} and state == \"active\" | limit 1", &member))?;
                    if let Some(login) = credentials.first().and_then(|document| document.str_field("login")) { entry["login"] = login.into(); }
                }
            }
            Ok(reply(writer, id, serde_json::json!({ "placeId": place_id, "ownerIdentityId": place.owner_identity_id, "entries": entries, }))?)
        }
        RoutedOperation::PlaceAccessSet(Routed { id, input: PlaceAccessSetInput { place_id, identity_id: target_identity_id, role, filters, profile_id }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to manage Place access")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only the Place Owner may change access")?;
            let target_identity_id = resolve_member_identity(settings, engine, id, &target_identity_id)?;
            if target_identity_id == place.owner_identity_id {
                reject!(id, "place.owner_immutable", "the Place Owner cannot be changed through Place access");
            }
            #[cfg(feature = "auth")]
            super::policy_ops::check_external_sharing(engine, id, &place.owner_identity_id, &target_identity_id)?;

            if let Some(profile) = profile_id.as_deref().filter(|profile| !profile.is_empty() && !place.profiles.iter().any(|known| known.profile_id == *profile)) {
                reject!(id, "place.profile_not_found", format!("{profile:?} is not a profile of this Place"));
            }
            let sharing_id = place_access_sharing_id(&place_id, &target_identity_id);
            let permission = sharing_permission(&place_id, role);
            let existing = load_sharing(engine, id, &sharing_id).ok();
            // The profile kept, given or taken away (`""`).
            let profile_id = match profile_id { Some(profile) => (!profile.is_empty()).then_some(profile), None => query_first(engine, id, quoted!("on _sharings | where sharingId == {} | select profileId", &sharing_id))?.and_then(|sharing| sharing.str_field("profileId").map(str::to_owned)) };

            let query = if let Some(filters) = &filters {
                // An object cannot be `set`: the record is written again, with them.
                query_documents(engine, id, quoted!("on _sharings | where sharingId == {} | delete", &sharing_id))?;
                insert_query("_sharings", serde_json::json!({"sharingId": &sharing_id, "owner": &place.owner_identity_id, "target": &target_identity_id, "permissions": [&permission], "placeRole": role.as_str(), "filters": filters, "profileId": profile_id, "state": "active", "createdAt": unix_time_millis()}))
            } else if existing.is_some() {
                // `set` expressions intentionally only accept scalar expressions today;
                // JSON array literals such as `["place:...:resident"]` are not valid
                // expression RHS values. Keep the Place role as a dedicated scalar field
                // and preserve `permissions` for backward-compatible/general sharings.
                format!(
                    "on _sharings | where sharingId == {} | set placeRole = {}, profileId = {}, state = \"active\", updatedAt = {}",
                    query_string(&sharing_id),
                    query_string(role.as_str()),
                    profile_id.as_deref().map_or_else(|| "null".to_owned(), query_string),
                    unix_time_millis(),
                )
            } else {
                let created_at = unix_time_millis();
                insert_query("_sharings", serde_json::json!({"sharingId": &sharing_id, "owner": &place.owner_identity_id, "target": &target_identity_id, "permissions": [&permission], "placeRole": role.as_str(), "profileId": profile_id, "state": "active", "createdAt": created_at}))
            };

            query_documents(engine, id, query)?;
            let audience = place_audience(engine, id, &place_id)?;
            publish_durable_event(engine, audience, "place.access.updated", serde_json::json!({"placeId": place_id, "identityId": target_identity_id, "role": role.as_str(), "filters": filters, "profileId": profile_id}));
            Ok(reply(writer, id, serde_json::json!({"placeId": place_id, "identityId": target_identity_id, "role": role.as_str(), "filters": filters, "profileId": profile_id, "state": "active"}))?)
        }
        RoutedOperation::PlaceAccessRemove(Routed { id, input: PlaceAccessRemoveInput { place_id, identity_id: target_identity_id }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to manage Place access")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only the Place Owner may change access")?;
            if target_identity_id == place.owner_identity_id {
                reject!(id, "place.owner_immutable", "the Place Owner cannot be removed");
            }

            let sharing_id = place_access_sharing_id(&place_id, &target_identity_id);
            if load_sharing(engine, id, &sharing_id).is_err() {
                reject!(id, "place.access_not_found", "the identity does not have a managed access entry for this Place");
            }

            let audience = place_audience(engine, id, &place_id)?;
            query_documents(engine, id, quoted!("on _sharings | where sharingId == {} | delete", &sharing_id))?;
            publish_durable_event(engine, audience, "place.access.removed", serde_json::json!({ "placeId": place_id, "identityId": target_identity_id, }));
            Ok(reply(writer, id, serde_json::json!({ "placeId": place_id, "identityId": target_identity_id, "state": "removed", }))?)
        }
        RoutedOperation::PlacePublicSet(Routed { id, input: PlacePublicSetInput { place_id, public_access } }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to change public Place access")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only the Place Owner may change public access")?;
            // Making a Place private stays possible when public Places are disabled.
            if public_access.is_some() && !settings.allow_public { reject!(id, "place.public_disabled", "public Places are disabled on this Core (OGD_ALLOW_PUBLIC)"); }
            #[cfg(feature = "auth")]
            if public_access.is_some() { super::policy_ops::check_public_sharing(engine, id, &place.owner_identity_id)?; }
            let value = public_access.map(|access| query_string(access.as_str())).unwrap_or_else(|| "null".to_owned());
            let query = quoted!("on _places | where placeId == {} and state == \"active\" | set publicAccess = {value}", &place_id);
            let audience = place_audience(engine, id, &place_id)?;
            query_documents(engine, id, query)?;
            let updated = serde_json::json!({"placeId": place_id, "publicAccess": public_access.map(PublicAccess::as_str)});
            publish_durable_event(engine, audience, "place.public.updated", updated.clone());
            Ok(reply(writer, id, updated)?)
        }
        RoutedOperation::PlaceResourceList(Routed { id, input: PlaceIdInput { place_id } }) => {
            let place = load_place(engine, id, &place_id)?;
            let access = resolve_place_access_for_principal(engine, id, principal, &place)?;
            let fabric_defaults = load_fabric_defaults(engine, id)?;
            let fabric_default_devices = active_devices_for_assignments(engine, id, &fabric_defaults);
            if let PlaceAccess::Public(public_access) = access {
                // Outside the scope, only the routing a Gateway needs to reach the
                // assigned providers is returned: never the members' Devices.
                return Ok(reply(writer, id, serde_json::json!({
                    "placeId": place_id,
                    "assignments": place.resource_assignments,
                    "eligibleDevices": active_devices_for_assignments(engine, id, &place.resource_assignments),
                    "fabricDefaults": fabric_defaults,
                    "fabricDefaultDevices": fabric_default_devices,
                    "fabricCanManage": false,
                    "placeRole": JsonValue::Null,
                    "publicAccess": public_access.as_str(),
                }))?);
            }
            let access_entries = list_place_access(engine, id, &place)?;
            let mut eligible_identity_ids = vec![place.owner_identity_id.clone()];
            for entry in &access_entries {
                if entry.str_field("state") == Some("active") {
                    if let Some(target) = entry.get("identityId").or_else(|| entry.get("target")).and_then(JsonValue::as_str) {
                        if !eligible_identity_ids.iter().any(|value| value == target) { eligible_identity_ids.push(target.to_owned()); }
                    }
                }
            }
            let mut eligible_devices = Vec::new();
            for identity in &eligible_identity_ids {
                let query = quoted!("on _devices | where identityId == {} and state == \"active\" | select identityId, deviceId, publicKey | sort deviceId", identity);
                eligible_devices.extend(query_documents(engine, id, query).unwrap_or_default());
            }
            let fabric_can_manage = !settings.authorization_mode.is_enforced()
                || authorize_connection(engine, principal, AuthorizationAction::FabricManage, "_fabric_resources");
            Ok(reply(writer, id, serde_json::json!({
                "placeId": place_id,
                "assignments": place.resource_assignments,
                "eligibleDevices": eligible_devices,
                "fabricDefaults": fabric_defaults,
                "fabricDefaultDevices": fabric_default_devices,
                "fabricCanManage": fabric_can_manage,
                "placeRole": access.role().map(PlaceRole::as_str),
                "publicAccess": JsonValue::Null,
            }))?)
        }
        RoutedOperation::PlaceResourceSet(Routed { id, input: PlaceResourceSetInput { place_id, identity_id: node_identity_id, device_id: node_device_id, capability, role, service_role, storage_role } }) => {
            let owner_identity_id = require_identity(principal, id, "an authenticated identity is required to manage Place resources")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, owner_identity_id, &place, "only a Place Owner may assign resource nodes")?;
            if resolve_place_role(engine, id, &node_identity_id, &place)?.is_none() {
                reject!(id, "place.resource.identity_not_attached", "the node identity must first be attached to the Place");
            }
            // A resource Node is a concrete registered Device of that Identity.
            // Loading the credential validates both ownership and active device state.
            let _node_device = load_device_credential(engine, id, &node_identity_id, &node_device_id)?;
            if capability == "auth" {
                reject!(id, "place.resource.auth_fabric_level", "auth is fabric-level and cannot be assigned to a Place");
            }
            if !place_resource_capability_allowed(&capability) {
                reject!(id, "place.resource.invalid_capability", "invalid resource capability");
            }
            let assignments = upsert_resource_assignment(
                place.resource_assignments.clone(), owner_identity_id, &node_identity_id, &node_device_id, &capability,
                role.as_deref(), service_role.as_deref(), storage_role.as_deref(),
            ).map_err(|error| super::serve::request_error(id, format!("place.{}", error.code), error.message))?;
            save_place_resources(writer, engine, id, &place_id, assignments)
        }
        RoutedOperation::PlaceResourceRemove(Routed { id, input: PlaceResourceRemoveInput { place_id, identity_id: node_identity_id, device_id: node_device_id, capability } }) => {
            let owner_identity_id = require_identity(principal, id, "an authenticated identity is required to manage Place resources")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, owner_identity_id, &place, "only a Place Owner may remove resource assignments")?;
            let assignments = remove_resource_assignment(
                place.resource_assignments.clone(), &node_identity_id, &node_device_id, &capability,
            );
            save_place_resources(writer, engine, id, &place_id, assignments)
        }
        other => diverged(other.id()),
    }
}

#[cfg(feature = "database")]
/// The Identity a Place is shared with: its id, or with classic authentication the identifier it signs in with.
/// Whether `principal` may install an App kept for compatibility on another Place: the Owner's policy includes it by
/// name, or the identity manages Apps (`app.manage`, which every identity has when authorization is not enforced).
pub(super) fn compatibility_app_installable(enforced: bool, policy_mode: bool, engine: &Engine, id: RequestId, principal: &Principal, app: &JsonValue) -> Result<bool, super::serve::Stop> {
    let app_id = app.str_field("appId").unwrap_or_default();
    if !enforced || authorize_connection(engine, principal, AuthorizationAction::AppManage, app_id) { return Ok(true); }
    #[cfg(feature = "auth")]
    if policy_mode {
        if let Some(identity_id) = principal_identity_id(principal) {
            if super::policy_ops::app_offer(engine, id, identity_id)?.is_some_and(|offer| offer.includes(app_id)) { return Ok(true); }
        }
    }
    #[cfg(not(feature = "auth"))]
    let _ = (id, policy_mode);
    Ok(false)
}

fn resolve_member_identity(settings: &ConnectionSettings, engine: &Engine, id: RequestId, target: &str) -> Result<String, super::serve::Stop> {
    let target = target.trim();
    if !settings.classic_auth_enabled { return Ok(target.to_owned()); }
    if !query_documents(engine, id, quoted!("on _identities | where identityId == {} | limit 1", target))?.is_empty() { return Ok(target.to_owned()); }
    let credentials = query_documents(engine, id, quoted!("on _credentials | where loginNormalized == {} and state == \"active\" | limit 1", &target.to_lowercase()))?;
    match credentials.first().and_then(|document| document.str_field("identityId")) {
        Some(identity_id) => Ok(identity_id.to_owned()),
        None => reject!(id, "place.identity_not_found", "no Identity has this id or classic identifier"),
    }
}

fn save_place_resources(writer: &mut TcpStream, engine: &Engine, id: RequestId, place_id: &str, assignments: Vec<JsonValue>) -> Handled {
    let encoded = serde_json::to_string(&assignments).expect("resource assignments serialize");
    query_documents(engine, id, format!(
        "on _places | where placeId == {} and state == \"active\" | set resourceAssignments = {}, updatedAt = {}",
        query_string(place_id), encoded, unix_time_millis()
    ))?;
    let audience = place_audience(engine, id, place_id)?;
    publish_durable_event(engine, audience, "place.resources.updated", serde_json::json!({ "placeId": place_id }));
    Ok(reply(writer, id, serde_json::json!({ "placeId": place_id, "assignments": assignments }))?)
}

/// The Place of a data request and the caller's grant, which must allow writing.
#[cfg(feature = "database")]
fn writable_place(engine: &Engine, id: RequestId, principal: &Principal, place_id: &str, message: &str) -> Result<PlaceAccess, super::serve::Stop> {
    let place = load_place(engine, id, place_id)?;
    let access = resolve_place_access_for_principal(engine, id, principal, &place)?;
    if !access.can_write() { reject!(id, "authorization.denied", message); }
    Ok(access)
}

/// How long an import waits for its confirmation, and how many rows of a failed check it shows.
#[cfg(feature = "database")]
const STAGED_FOR: u64 = 7 * 24 * 3600 * 1000;
#[cfg(feature = "database")]
const CHECK_ROWS: usize = 100;

/// The analysis or import result given by the caller, or computed by the local data worker.
#[cfg(feature = "database")]
fn data_worker_result(settings: &ConnectionSettings, engine: &Engine, id: RequestId, worker_result: Option<JsonValue>, place_id: &str, files_instance_id: &str, file_id: &str, operation: &str, mapping: Option<&JsonValue>, unavailable: &str) -> Result<JsonValue, super::serve::Stop> {
    if let Some(result) = worker_result {
        Ok(result)
    } else if settings.service_capabilities.contains(ServiceCapability::DataImport) {
        Ok(run_data_worker_for_file(settings, engine, id, place_id, files_instance_id, file_id, operation, mapping)?)
    } else {
        reject!(id, "capability.unavailable", unavailable);
    }
}

#[cfg(feature = "database")]
pub(super) fn handle_app_operation(writer: &mut TcpStream, settings: &ConnectionSettings, engine: &Engine, authentication: &ConnectionAuth, delegation: Option<&super::GatewayDelegation>, operation: RoutedOperation) -> Handled {
    let principal = authentication.principal();
    match operation {
        RoutedOperation::QueryContextResolve(Routed { id, input: QueryContextResolveInput { place_id, app_instance_id, as_profile } }) => {
            let context = resolve_query_execution_context(engine, id, principal, RequestedExecutionContext { place_id, app_instance_id, as_profile })?;
            reply(writer, id, serde_json::json!({
                "placeId": context.place_id,
                "appInstanceId": context.app_instance_id,
                "placeRole": context.access.role().map(PlaceRole::as_str),
                "publicAccess": context.access.public_access().map(PublicAccess::as_str),
                "filters": context.filters.iter().map(|(field, values)| (field.clone(), JsonValue::Array(values.iter().map(crate::helpers::value_to_json).collect()))).collect::<serde_json::Map<_, _>>(),
            }))?;
        }
        RoutedOperation::AppCreate(Routed { id, input: AppCreateInput { app_id, place_id, name, version, definition, visibility, created_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to create an App")?;
            if let Some(ref place_id) = place_id {
                let place = load_place(engine, id, place_id)?;
                require_place_owner(engine, id, identity_id, &place, "only the Place Owner may create an App in this Place")?;
            } else {
                ensure_operation_resource_authorized(settings, engine, principal, id, OperationKind::AppCreate, &app_id)?;
            }
            let visibility = visibility.unwrap_or_else(|| "private".to_owned());
            if place_id.is_some() && visibility == "public" {
                ensure_operation_resource_authorized(settings, engine, principal, id, OperationKind::AppCreate, &app_id)?;
            }
            if let Err(message) = validate_app_definition_model(&definition) {
                reject!(id, "app.invalid_definition", message);
            }
            if app_record_exists(engine, id, &app_id)? {
                reject!(id, "app.already_exists", "an App with this id already exists");
            }
            let created_at = created_at.unwrap_or_else(unix_time_millis);
            let mut document = serde_json::json!({"appId": &app_id, "name": &name, "version": &version, "definition": &definition, "createdBy": identity_id, "visibility": &visibility, "state": "active", "createdAt": created_at});
            if let Some(owner_place_id) = place_id.as_ref() {
                document["ownerPlaceId"] = owner_place_id.as_str().into();
                document["maintainersJson"] = "[]".into();
            }
            query_documents(engine, id, insert_query("_apps", document.clone()))?;

            let instance_id = if let Some(ref place_id) = place_id {
                let instance_id = UuidV7Generator::new().next_id().to_string();
                let query = insert_query("_app_instances", serde_json::json!({"instanceId": &instance_id, "placeId": place_id, "appId": &app_id, "name": &name, "config": {}, "state": "active", "createdAt": created_at}));
                query_documents(engine, id, query)?;
                Some(instance_id)
            } else { None };

            let audience = app_audience(engine, id, &document)?;
            publish_durable_event(engine, audience, "app.created", serde_json::json!({"appId": app_id, "placeId": place_id, "instanceId": instance_id, "name": name, "version": version, "visibility": visibility, "createdBy": identity_id, "createdAt": created_at}));
            reply(writer, id, serde_json::json!({"appId": app_id, "placeId": place_id, "instanceId": instance_id, "name": name, "version": version, "definition": definition, "visibility": visibility, "createdBy": identity_id, "state": "active", "createdAt": created_at}))?;
        }
        // Public Apps, private ones the identity created or maintains, and those of the Places it reads.
        RoutedOperation::AppList(Routed { id, .. }) => {
            #[allow(unused_mut)]
            let mut apps = scoped_apps(engine, id, principal)?;
            // Policy mode: platform Apps are marked, and under a policy that selects its Apps each
            // App says what it offers (`offer`: included, addon); the hidden ones are left out
            // unless a Place the identity reads uses them, which needs them to render.
            #[cfg(feature = "auth")]
            if settings.enrollment_mode.is_policy() {
                let offer = match principal_identity_id(principal) { Some(identity_id) => super::policy_ops::app_offer(engine, id, identity_id)?, None => None };
                let mut placed = None;
                let mut kept = Vec::with_capacity(apps.len());
                for mut app in apps {
                    if super::policy_ops::app_is_platform(&app) { app["platform"] = true.into(); }
                    if let Some(offer) = offer.as_ref() {
                        let availability = offer.of(&app);
                        if availability == super::policy_ops::Availability::Hidden {
                            if placed.is_none() { placed = Some(placed_app_ids(engine, id, principal)?); }
                            if !app.str_field("appId").is_some_and(|app_id| placed.as_ref().is_some_and(|placed| placed.contains(app_id))) { continue; }
                        }
                        app["offer"] = availability.as_str().into();
                    }
                    kept.push(app);
                }
                apps = kept;
            }
            // An App kept for compatibility is an add-on to whoever cannot install it on a new Place.
            for app in &mut apps {
                if super::app_in_compatibility(app) && app.str_field("offer") != Some("hidden") && !compatibility_app_installable(settings.authorization_mode.is_enforced(), settings.enrollment_mode.is_policy(), engine, id, principal, app)? {
                    app["offer"] = "addon".into();
                }
            }
            reply(writer, id, serde_json::json!({ "apps": apps }))?;
        }
        RoutedOperation::AppGet(Routed { id, input: AppIdInput { app_id } }) => {
            let app = load_app_definition(engine, id, &app_id)?;
            if !app_readable(engine, id, principal, &app, &mut None)? {
                reject!(id, "app.not_found", "App was not found");
            }
            reply(writer, id, with_app_visibility(app))?;
        }
        RoutedOperation::AppUpdate(Routed { id, input: AppUpdateInput { app_id, place_id, name, version, definition, maintainers, visibility, updated_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to update an App")?;
            if let Some(ref place_id) = place_id {
                let place = load_place(engine, id, place_id)?;
                let role = place_role(engine, id, identity_id, &place)?;
                let app = load_app_definition(engine, id, &app_id)?;
                let links = query_documents(engine, id, quoted!("on _app_instances | where placeId == {} and appId == {} and state == \"active\" | limit 1", place_id, &app_id))?;
                if links.is_empty() {
                    reject!(id, "app.not_in_place", "the App is not attached to this Place");
                }
                let owner_place_id = app.str_field("ownerPlaceId");
                if owner_place_id.is_some_and(|owner| owner != place_id) {
                    reject!(id, "app.update_forbidden", "this Place does not own the App definition");
                }
                if !role.can_manage() && !app_identity_is_maintainer(&app, identity_id) {
                    reject!(id, "app.update_forbidden", "App management requires a Place Owner or App Maintainer");
                }
                if owner_place_id.is_none() && role.can_manage() {
                    let claim = quoted!("on _apps | where appId == {} and state == \"active\" | set ownerPlaceId = {}, maintainersJson = {}", &app_id, place_id, "[]");
                    query_documents(engine, id, claim)?;
                }
            } else {
                ensure_operation_resource_authorized(settings, engine, principal, id, OperationKind::AppUpdate, &app_id)?;
            }
            if let Some(ref requested_maintainers) = maintainers {
                let Some(ref managing_place_id) = place_id else {
                    reject!(id, "app.maintainers_requires_place", "maintainers can only be changed with a Place context");
                };
                let mut normalized = requested_maintainers.iter().map(|value| value.trim()).filter(|value| !value.is_empty()).map(str::to_owned).collect::<Vec<_>>();
                normalized.sort(); normalized.dedup();
                let current_app = load_app_definition(engine, id, &app_id)?;
                let mut current = app_maintainers(&current_app); current.sort(); current.dedup();
                if normalized != current {
                    let place = load_place(engine, id, managing_place_id)?;
                    if !place_role(engine, id, identity_id, &place)?.can_manage() {
                        reject!(id, "app.maintainers_forbidden", "only a Place Owner may change App maintainers");
                    }
                    let serialized = serde_json::to_string(&normalized).expect("maintainers serialize");
                    let maintainers_query = quoted!("on _apps | where appId == {} and state == \"active\" | set maintainersJson = {}", &app_id, &serialized);
                    query_documents(engine, id, maintainers_query)?;
                }
            }
            if let Err(message) = validate_app_definition_model(&definition) {
                reject!(id, "app.invalid_definition", message);
            }
            let app = load_app_definition(engine, id, &app_id)?;
            let was_public = app_is_public(&app);
            let visibility = visibility.unwrap_or_else(|| if was_public { "public" } else { "private" }.to_owned());
            if (visibility == "public") != was_public {
                ensure_operation_resource_authorized(settings, engine, principal, id, OperationKind::AppUpdate, &app_id)?;
            }
            let updated_at = updated_at.unwrap_or_else(unix_time_millis);
            let definition_json =
                serde_json::to_string(&definition).expect("App definition serializes");
            let query = quoted!("on _apps | where appId == {} and state == \"active\" | set name = {}, version = {}, definition = {definition_json}, visibility = {}, updatedBy = {}, updatedAt = {updated_at}", &app_id, &name, &version, &visibility, identity_id);
            query_documents(engine, id, query)?;
            // A withdrawn App is announced to everyone, who no longer lists it.
            let audience = if was_public { Audience::Global } else { app_audience(engine, id, &load_app_definition(engine, id, &app_id)?)? };
            publish_durable_event(engine, audience, "app.updated", serde_json::json!({"appId": app_id, "name": name, "version": version, "visibility": visibility, "updatedBy": identity_id, "updatedAt": updated_at}));
            reply(writer, id, serde_json::json!({"appId": app_id, "name": name, "version": version, "definition": definition, "visibility": visibility, "updatedBy": identity_id, "state": "active", "updatedAt": updated_at}))?;
        }
        RoutedOperation::AppDelete(Routed { id, input: AppDeleteInput { app_id, deleted_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to delete an App")?;
            ensure_operation_resource_authorized(settings, engine, principal, id, OperationKind::AppDelete, &app_id)?;
            let audience = app_audience(engine, id, &load_app_definition(engine, id, &app_id)?)?;
            let instances = query_documents(engine, id, quoted!("on _app_instances | where appId == {} and state == \"active\" | limit 1", &app_id))?;
            if !instances.is_empty() {
                reject!(id, "app.in_use", "the App still has active instances");
            }
            let deleted_at = deleted_at.unwrap_or_else(unix_time_millis);
            let query = quoted!("on _apps | where appId == {} and state == \"active\" | set state = \"deleted\", deletedBy = {}, deletedAt = {deleted_at}", &app_id, identity_id);
            query_documents(engine, id, query)?;
            publish_durable_event(engine, audience, "app.deleted", serde_json::json!({"appId": app_id, "deletedBy": identity_id, "deletedAt": deleted_at}));
            reply(writer, id, serde_json::json!({"appId": app_id, "state": "deleted", "deletedBy": identity_id, "deletedAt": deleted_at}))?;
        }
        RoutedOperation::AppInstanceCreate(Routed { id, input: AppInstanceCreateInput { place_id, app_id, name, config, created_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to attach an App")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only the Place Owner may attach an App")?;
            let app = load_app_definition(engine, id, &app_id)?;
            if !app_installable_by(&app, identity_id) {
                reject!(id, "app.private", "this App is private to its creator and maintainers");
            }
            if super::app_in_compatibility(&app) && !compatibility_app_installable(settings.authorization_mode.is_enforced(), settings.enrollment_mode.is_policy(), engine, id, principal, &app)? {
                reject!(id, "app.compatibility", format!("{:?} is kept for compatibility: an administrator installs it on another Place", app.str_field("name").or_else(|| app.str_field("appId")).unwrap_or_default()));
            }
            #[cfg(feature = "auth")]
            if settings.enrollment_mode.is_policy() { super::policy_ops::check_app_install(engine, id, identity_id, &app)?; }
            let instance_id = UuidV7Generator::new().next_id().to_string();
            let created_at = created_at.unwrap_or_else(unix_time_millis);
            let mut document = serde_json::json!({"instanceId": &instance_id, "placeId": &place_id, "appId": &app_id, "config": &config, "state": "active", "createdAt": created_at});
            if let Some(name) = name.as_deref() {
                document["name"] = name.into();
            }
            let query = insert_query("_app_instances", document);
            query_documents(engine, id, query)?;
            let audience = place_audience(engine, id, &place_id)?;
            publish_durable_event(engine, audience, "app.instance.created", serde_json::json!({
                "instanceId": instance_id, "placeId": place_id, "appId": app_id
            }));
            reply(writer, id, serde_json::json!({ "instanceId": instance_id, "placeId": place_id, "appId": app_id, "name": name, "config": config, "state": "active", "createdAt": created_at, }))?;
        }
        RoutedOperation::AppInstanceList(Routed { id, input: PlaceIdInput { place_id } }) => {
            let place = load_place(engine, id, &place_id)?;
            let access = resolve_place_access_for_principal(engine, id, principal, &place)?;
            let (apps, _) = member_view(engine, id, principal, &place, access, None)?;
            let mut documents = Vec::new();
            for document in query_documents(engine, id, quoted!("on _app_instances | where placeId == {} and state == \"active\" | sort createdAt", &place_id))? {
                if app_instance_visible(engine, id, principal, &access, apps.as_deref(), &document)? {
                    documents.push(document);
                }
            }
            // Instances of an App the Owner's policy no longer includes are read-only.
            #[cfg(feature = "auth")]
            if settings.enrollment_mode.is_policy() {
                let read_only = super::policy_ops::read_only_instances(engine, id, &place.owner_identity_id, &documents)?;
                for document in &mut documents {
                    if document.str_field("instanceId").is_some_and(|instance_id| read_only.contains(instance_id)) { document["readOnly"] = true.into(); }
                }
            }
            reply(writer, id, serde_json::json!({"placeId": place_id, "role": access.role().map(PlaceRole::as_str), "publicAccess": access.public_access().map(PublicAccess::as_str), "instances": documents}))?;
        }
        RoutedOperation::PlaceSchema(Routed { id, input: PlaceIdInput { place_id } }) => {
            reply(writer, id, place_schema_for(engine, id, principal, &place_id)?)?;
        }
        RoutedOperation::PlaceSearch(Routed { id, input }) => {
            #[cfg(feature = "search")]
            reply(writer, id, place_search_for(engine, Some(&settings.files_path), id, principal, delegation, &input)?)?;
            #[cfg(not(feature = "search"))]
            { let _ = input; reject!(id, "place.search_unavailable", "ogd was built without the search feature"); }
        }
        RoutedOperation::AppInstanceRemove(Routed { id, input: AppInstanceRemoveInput { instance_id, removed_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to remove an App")?;
            let instance = load_app_instance(engine, id, &instance_id)?;
            let place = load_place(engine, id, &instance.place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only the Place Owner may remove an App")?;
            let removed_at = removed_at.unwrap_or_else(unix_time_millis);
            let query = quoted!("on _app_instances | where instanceId == {} and state == \"active\" | set state = \"removed\", removedAt = {removed_at}", &instance_id);
            query_documents(engine, id, query)?;
            // The removed instance leaves the Place's layout; its folder stays.
            if let Some(mut layout) = place.app_layout.clone() {
                AppLayoutEntry::retain(&mut layout, |instance| instance != instance_id);
                let layout_json = serde_json::to_string(&layout).expect("Place app layout serializes");
                let order_json = serde_json::to_string(&AppLayoutEntry::flatten(&layout)).expect("Place app order serializes");
                query_documents(engine, id, format!("on _places | where placeId == {} and state == \"active\" | set appLayout = {layout_json}, appOrder = {order_json}", query_string(&instance.place_id)))?;
            }
            let audience = place_audience(engine, id, &instance.place_id)?;
            publish_durable_event(engine, audience, "app.instance.removed", serde_json::json!({
                "instanceId": instance_id, "placeId": instance.place_id
            }));
            reply(writer, id, serde_json::json!({ "instanceId": instance_id, "placeId": instance.place_id, "state": "removed", "removedAt": removed_at, }))?;
        }
        RoutedOperation::DataAnalyze(Routed { id, input: DataAnalyzeInput { place_id, files_instance_id, file_id, worker_result, .. } }) => {
            writable_place(engine, id, principal, &place_id, "data analysis requires write access to the Place")?;
            let mut response = data_worker_result(settings, engine, id, worker_result, &place_id, &files_instance_id, &file_id, "analyze", None, "data.analyze requires a data.import worker result on this database node")?;
            let fingerprint = response.str_field("fingerprint").unwrap_or_default();
            let mappings = if fingerprint.is_empty() { Vec::new() } else { query_documents(engine, id, quoted!("on _data_mappings | where placeId == {} and fingerprint == {} and state == \"active\" | limit 5", &place_id, fingerprint))? };
            if let Some(object) = response.as_object_mut() { object.insert("recognized".into(), JsonValue::Bool(!mappings.is_empty())); object.insert("mappings".into(), JsonValue::Array(mappings)); }
            reply(writer, id, response)?;
        }
        RoutedOperation::DataMappingSave(Routed { id, input: DataMappingSaveInput { place_id, fingerprint, name, target_app_id, target_table, definition } }) => {
            writable_place(engine, id, principal, &place_id, "saving a Mapping requires write access to the Place")?;
            let mapping_id = UuidV7Generator::new().next_id().to_string(); let now = unix_time_millis();
            query_documents(engine, id, insert_query("_data_mappings", serde_json::json!({"mappingId": &mapping_id, "placeId": &place_id, "fingerprint": &fingerprint, "name": &name, "targetAppId": &target_app_id, "targetTable": &target_table, "definition": &definition, "createdBy": &principal_actor(principal), "state": "active", "createdAt": now})))?;
            reply(writer, id, serde_json::json!({"mappingId": mapping_id, "fingerprint": fingerprint, "name": name, "targetAppId": target_app_id, "targetTable": target_table, "definition": definition}))?;
        }
        RoutedOperation::DataMappingList(Routed { id, input: DataMappingListInput { place_id, fingerprint } }) => {
            let place = load_place(engine, id, &place_id)?;
            resolve_place_access_for_principal(engine, id, principal, &place)?;
            let fingerprint_filter = fingerprint.as_deref().map(|value| format!(" and fingerprint == {}", query_string(value))).unwrap_or_default();
            let mappings = query_documents(engine, id, format!("on _data_mappings | where placeId == {}{} and state == \"active\"", query_string(&place_id), fingerprint_filter))?;
            reply(writer, id, serde_json::json!({"placeId": place_id, "mappings": mappings}))?;
        }
        RoutedOperation::DataMappingUpdate(Routed { id, input: DataMappingUpdateInput { place_id, mapping_id, name, target_app_id, target_table, definition } }) => {
            writable_place(engine, id, principal, &place_id, "updating a Mapping requires write access to the Place")?;
            let selected = quoted!("on _data_mappings | where mappingId == {} and placeId == {} and state == \"active\"", &mapping_id, &place_id);
            if query_documents(engine, id, format!("{selected} | limit 1"))?.is_empty() { reject!(id, "data.mapping_not_found", "mapping was not found in this Place"); }
            let mut assignments = Vec::new();
            if let Some(value) = name { assignments.push(format!("name = {}", query_string(&value))); }
            if let Some(value) = target_app_id { assignments.push(format!("targetAppId = {}", query_string(&value))); }
            if let Some(value) = target_table { assignments.push(format!("targetTable = {}", query_string(&value))); }
            if let Some(value) = definition { assignments.push(format!("definition = {}", serde_json::to_string(&value).expect("mapping definition serializes"))); }
            assignments.push(format!("updatedBy = {}", query_string(&principal_actor(principal)))); assignments.push(format!("updatedAt = {}", unix_time_millis()));
            query_documents(engine, id, format!("{selected} | set {}", assignments.join(", ")))?;
            let mapping = query_documents(engine, id, format!("{selected} | limit 1"))?.into_iter().next();
            reply(writer, id, serde_json::json!({"mapping": mapping}))?;
        }
        RoutedOperation::DataMappingDelete(Routed { id, input: DataMappingDeleteInput { place_id, mapping_id } }) => {
            writable_place(engine, id, principal, &place_id, "deleting a Mapping requires write access to the Place")?;
            let selected = quoted!("on _data_mappings | where mappingId == {} and placeId == {} and state == \"active\"", &mapping_id, &place_id);
            if query_documents(engine, id, format!("{selected} | limit 1"))?.is_empty() { reject!(id, "data.mapping_not_found", "mapping was not found in this Place"); }
            let deleted_at = unix_time_millis();
            query_documents(engine, id, format!("{selected} | set state = \"deleted\", deletedBy = {}, deletedAt = {}", query_string(&principal_actor(principal)), deleted_at))?;
            reply(writer, id, serde_json::json!({"mappingId": mapping_id, "placeId": place_id, "state": "deleted", "deletedAt": deleted_at}))?;
        }
        RoutedOperation::DataImport(Routed { id, input }) => {
            let (place_id, files_instance_id, file_id, mapping) = (input.place_id.clone(), input.files_instance_id.clone(), input.file_id.clone(), input.mapping.clone());
            let response = data_import(engine, principal, delegation, id, input, |stage| {
                if !settings.service_capabilities.contains(ServiceCapability::DataImport) {
                    reject!(id, "capability.unavailable", "data.import requires a data.import worker result on this database node");
                }
                run_data_worker_import_file(settings, engine, id, &place_id, &files_instance_id, &file_id, Some(&mapping), stage)
            })?;
            reply(writer, id, response)?;
        }
        other => return diverged(other.id()),
    }
    Ok(())
}

/// `data.import`: stages the rows of the file (`worker` reads it, unless the Gateway gave them), runs the checks the App
/// declares for the table, and writes the rows once none stops them; else answers `confirm` (or `blocked`) with the rows of
/// each check, and the caller resumes (`importId`, `accept`) or cancels. In Gateway fabric mode the authority Core only
/// authorizes the import and resolves its destination (`planOnly`); the database Core of the Place runs it (`plan`).
#[cfg(feature = "database")]
pub(super) fn data_import(engine: &Engine, principal: &Principal, delegation: Option<&super::GatewayDelegation>, id: RequestId, input: DataImportInput, worker: impl FnOnce(&mut dyn FnMut(Vec<JsonValue>) -> Result<(), super::serve::Stop>) -> Result<JsonValue, super::serve::Stop>) -> Result<JsonValue, super::serve::Stop> {
    let DataImportInput { place_id, target_instance_id, table, mapping, mode, worker_result, plan_only, import_id, accept, check_only, cancel, plan, slice, key, documents, warnings: given_warnings, .. } = input;
    let (given_rows, given_result_warnings) = (worker_result.as_ref().and_then(|result| result.array_field("documents")).map(Vec::len), worker_result.as_ref().and_then(|result| result.get("warnings")).cloned());
    // The rows of the file, given to `stage` batch by batch: the result the Gateway gave whole, else the local data
    // worker's; the outcome carries the worker's warnings.
    let read_file = |stage: &mut dyn FnMut(Vec<JsonValue>) -> Result<(), super::serve::Stop>| match worker_result {
        Some(mut result) => {
            if let Some(JsonValue::Array(rows)) = result.get_mut("documents").map(JsonValue::take) { stage(rows)?; }
            Ok(result)
        }
        None => worker(stage),
    };
    // Past a storage limit, the Place takes no import (its size is known only once the rows are read).
    #[cfg(feature = "auth")]
    if !cancel { super::usage_ops::check_storage(engine, id, &place_id, 0)?; }
    // An App its Place Owner's policy no longer includes takes no import (step 4 of wip/POLICIES.md).
    #[cfg(feature = "auth")]
    if !cancel { super::policy_ops::check_instance_write(engine, id, &place_id, &target_instance_id)?; }
    let (collection, mode, declared) = match plan {
        Some(plan) => {
            let delegated = delegation.is_some_and(|delegated| delegated.capability == "database" && delegated.place_id.as_deref() == Some(place_id.as_str()) && delegated.app_instance_id.as_deref() == Some(target_instance_id.as_str()) && delegated.access().is_some_and(PlaceAccess::can_write));
            if !delegated { reject!(id, "authorization.denied", "a planned data import requires the Gateway's write delegation for its App instance"); }
            let Some(collection) = plan["collection"].as_str().filter(|collection| crate::view::is_name(collection)) else { reject!(id, "data.invalid_target", "the import plan names no collection") };
            (collection.to_owned(), plan["mode"].as_str().map(str::to_owned), plan["import"].clone())
        }
        None => {
            let access = writable_place(engine, id, principal, &place_id, "data import requires write access to the Place")?;
            let instance_docs = query_documents(engine, id, quoted!("on _app_instances | where instanceId == {} and placeId == {} and state == \"active\" | limit 1", &target_instance_id, &place_id))?;
            let Some(instance_doc) = instance_docs.first() else { reject!(id, "app.instance_not_found", "target App instance was not found in this Place"); };
            let app_id = instance_doc.str_field("appId").unwrap_or_default(); if app_id.is_empty() { reject!(id, "app.invalid_instance_record", "target App instance has no appId"); }
            let app = load_app_definition(engine, id, app_id)?;
            let Some(collection) = resolve_app_table_collection(&app, &table) else { reject!(id, "data.invalid_target", "target table is not declared by the App") };
            let mode = mode.as_deref().or_else(|| mapping["import"]["mode"].as_str()).map(str::to_owned);
            let declared = app["definition"]["model"]["tables"][&table]["import"].clone();
            if plan_only {
                // The rows are counted when the Gateway gave them; a batched import has not read them yet.
                let rows = given_rows;
                return Ok(serde_json::json!({"status": "planned", "placeId": place_id, "appInstanceId": target_instance_id, "placeRole": access.role().map(PlaceRole::as_str), "publicAccess": access.public_access().map(PublicAccess::as_str), "collection": collection, "mode": mode, "import": declared, "rows": rows, "warnings": given_result_warnings.unwrap_or(JsonValue::Array(vec![]))}));
            }
            (collection, mode, declared)
        }
    };
    // What the import does to the rows already stored (wip/IMPORT_MODES.md):
    //   append: nothing; update: a row replaces the stored one of the same key; slice: the stored rows of each slice the
    //   file holds (the same values of the slice fields) give way to it; replace_all: the table is emptied first.
    // The table declares its fields (`key`, `slice`, formerly `replace`) and may restrict the modes (`modes`) and name the
    // default one (`mode`); a table that declares no fields takes them from the request.
    let names = |declared: &JsonValue, given: &[String]| -> Vec<String> {
        let own = declared.as_array().into_iter().flatten().filter_map(JsonValue::as_str).map(str::to_owned).collect::<Vec<_>>();
        (if own.is_empty() { given.to_vec() } else { own }).into_iter().filter(|field| crate::view::is_name(field)).collect()
    };
    let slice_fields = names(if declared["slice"].is_array() { &declared["slice"] } else { &declared["replace"] }, &slice);
    let key_fields = names(&declared["key"], &key);
    let mode = match mode.as_deref().or_else(|| declared["mode"].as_str()) {
        Some("replace") | Some("replace_all") => "replace_all",
        Some("update") | Some("sync") => "update",
        Some("slice") | Some("rotation") => "slice",
        Some("append") => "append",
        Some(_) => reject!(id, "data.import_mode_unknown", "import modes are append, update, slice and replace_all"),
        // No mode asked: a table that declares a slice replaces it (reimporting a month replaces it).
        None => if declared["slice"].is_array() || declared["replace"].is_array() { "slice" } else { "append" },
    }.to_owned();
    if declared["modes"].as_array().is_some_and(|allowed| !allowed.iter().any(|allowed| allowed.as_str() == Some(mode.as_str()))) {
        reject!(id, "data.import_mode_refused", "this table does not accept this import mode");
    }
    if mode == "slice" && slice_fields.is_empty() { reject!(id, "data.import_fields_required", "mode slice needs the fields of the slice (`slice`)"); }
    if mode == "update" && key_fields.is_empty() { reject!(id, "data.import_fields_required", "mode update needs the fields of the key (`key`)"); }
    // The rows wait in `<collection>.staged`, in the scope of the target instance (DocumentScope stays the single
    // authority for _place/_app_instance), until the checks the App declares for the table pass or are accepted.
    let staged = format!("{collection}.staged");
    let scope = crate::view::scope_of(&place_id, Some(&target_instance_id));
    let failed = |error: String| super::serve::request_error(id, "data.import_failed", error);
    let run = |query: &str| crate::view::execute(engine, Some(&scope), query, false).map_err(failed);
    let (import_id, warnings) = match import_id {
        Some(import_id) if documents.is_none() => {
            let this = format!("on {staged} | where import_id == {}", JsonValue::from(import_id.as_str()));
            if run(&format!("{this} | limit 1"))?.is_empty() { reject!(id, "data.import_not_found", "no import with this importId waits in this App instance"); }
            if cancel {
                run(&format!("{this} | delete"))?;
                return Ok(serde_json::json!({"status": "cancelled", "importId": import_id}));
            }
            (import_id, given_warnings.unwrap_or(JsonValue::Array(vec![])))
        }
        // A new import, or one more batch of a batched one: the rows wait under one importId, a batch at a time.
        import_id => {
            let now = unix_time_millis();
            let import_id = match import_id {
                Some(import_id) => import_id,
                None => {
                    // An import nobody confirmed or cancelled goes after a week.
                    run(&format!("on {staged} | where staged_at < {} | delete", now.saturating_sub(STAGED_FOR)))?;
                    UuidV7Generator::new().next_id().to_string()
                }
            };
            let mut stage = |documents: Vec<JsonValue>| -> Result<(), super::serve::Stop> {
                if documents.iter().any(|document| !document.is_object()) {
                    reject!(id, "data.invalid_worker_output", "data worker returned a non-object document");
                }
                let rows = documents.into_iter().map(|mut document| { let fields = document.as_object_mut().expect("checked above"); fields.insert("import_id".into(), import_id.clone().into()); fields.insert("staged_at".into(), now.into()); document }).collect::<Vec<_>>();
                if !rows.is_empty() { run(&format!("on {staged} | insert {}", JsonValue::Array(rows)))?; }
                Ok(())
            };
            match documents {
                Some(documents) => {
                    let rows = documents.len();
                    stage(documents)?;
                    return Ok(serde_json::json!({"status": "staged", "importId": import_id, "rows": rows}));
                }
                None => {
                    let outcome = read_file(&mut stage)?;
                    (import_id, outcome.get("warnings").cloned().unwrap_or(JsonValue::Array(vec![])))
                }
            }
        }
    };
    // Each check reads the staged rows (`$import`); its rows stop the import until the caller accepts it, a blocking one for good.
    let parameters = serde_json::Map::from_iter([("import".to_owned(), JsonValue::from(import_id.as_str()))]);
    // Read as the App reads the Place (its views, their caches); `after` writes in the scope of the target instance.
    let read = |query: &str| crate::view::query(engine, &place_id, None, query, &parameters).map_err(failed);
    let summary = match declared["summary"].as_str() { Some(query) => JsonValue::Array(read(query)?), None => JsonValue::Null };
    let mut checks = Vec::new();
    for check in declared["checks"].as_array().into_iter().flatten() {
        let mut rows = read(&format!("{} | limit {}", check["query"].as_str().unwrap_or_default(), CHECK_ROWS + 1))?;
        if rows.is_empty() { continue; }
        let (more, blocking) = (rows.len() > CHECK_ROWS, check["blocking"] == true);
        rows.truncate(CHECK_ROWS);
        checks.push(serde_json::json!({"id": check["id"], "message": check["message"], "blocking": blocking, "accepted": !blocking && accept.iter().any(|accepted| check["id"] == accepted.as_str()), "rows": rows, "more": more}));
    }
    let status = if checks.iter().any(|check| check["blocking"] == true) { "blocked" } else if checks.iter().any(|check| check["accepted"] == false) { "confirm" } else if check_only { "checked" } else { "completed" };
    // The stored rows the import replaces, and for a slice how many slices the file holds: told before anything is written.
    let this_import = format!("on {staged} | where import_id == {}", JsonValue::from(import_id.as_str()));
    let matching = |fields: &[String]| { let same = fields.iter().map(|field| format!("s.{field}? == t.{field}? and ")).collect::<String>(); format!("on {collection} as t | lookup {staged} as s | where {same}s.import_id == {} | first | into same | end | where len(same) > 0", JsonValue::from(import_id.as_str())) };
    let stored = match mode.as_str() { "replace_all" => Some(format!("on {collection}")), "slice" => Some(matching(&slice_fields)), "update" => Some(matching(&key_fields)), _ => None };
    let count = |query: String| -> Result<u64, super::serve::Stop> { Ok(run(&format!("{query} | count as n"))?.first().and_then(|row| row["n"].as_u64()).unwrap_or(0)) };
    let replaced = match &stored { Some(query) => count(query.clone())?, None => 0 };
    let slices = if mode == "slice" { Some(count(format!("{this_import} | derive k = 0 | group | by {} | min k as k | end", slice_fields.join(", ")))?) } else { None };
    let mut response = serde_json::json!({"status": status, "importId": import_id, "checks": checks, "summary": summary, "mode": mode, "warnings": warnings, "replaces": {"rows": replaced, "slices": slices, "fields": match mode.as_str() { "slice" => slice_fields.clone(), "update" => key_fields.clone(), _ => Vec::new() }}});
    if status == "completed" {
        let this = format!("on {staged} | where import_id == {}", JsonValue::from(import_id.as_str()));
        let rows = run(&format!("{this} | count as n"))?.first().and_then(|row| row["n"].as_u64()).unwrap_or(0);
        let replace = stored.map(|query| format!("{query} | delete"));
        let writes = replace.into_iter().chain([format!("{this} | drop staged_at | into {collection}")]).chain(declared["after"].as_array().into_iter().flatten().filter_map(JsonValue::as_str).map(str::to_owned)).chain([format!("{this} | delete")]).collect::<Vec<_>>();
        // All or nothing: each write reads the rows as they were before the import.
        crate::view::write_together(engine, &place_id, Some(&target_instance_id), &writes, &parameters).map_err(failed)?;
        // The collection written, that the Hub reads again only the sources reading it; none when `after` writes others.
        let written = declared["after"].as_array().is_none_or(Vec::is_empty).then_some(collection.as_str());
        if let Ok(audience) = place_audience(engine, id, &place_id) {
            publish_durable_event(engine, audience, "app.data.changed", serde_json::json!({"placeId": place_id, "appInstanceId": target_instance_id, "table": table, "collection": written, "rows": rows}));
        }
        response["rows"] = rows.into();
        response["collection"] = written.into();
    }
    Ok(response)
}

/// `place.schema` as `principal` reads it: the Apps of the Place it sees, merged (`place_schema`).
pub(super) fn place_schema_for(engine: &Engine, id: RequestId, principal: &Principal, place_id: &str) -> Result<JsonValue, QueryResponse> {
    let place = load_place(engine, id, place_id)?;
    let access = resolve_place_access_for_principal(engine, id, principal, &place)?;
    let (apps, _) = member_view(engine, id, principal, &place, access, None)?;
    let mut installed = Vec::new();
    for instance in query_documents(engine, id, quoted!("on _app_instances | where placeId == {} and state == \"active\" | sort createdAt", place_id))? {
        if app_instance_visible(engine, id, principal, &access, apps.as_deref(), &instance)? {
            if let Some(app) = instance.str_field("appId").and_then(|app_id| load_app_definition(engine, id, app_id).ok()) { installed.push((instance, app)); }
        }
    }
    let mut schema = place_schema(&installed);
    schema["placeId"] = place_id.into(); schema["role"] = access.role().map(PlaceRole::as_str).into();
    Ok(schema)
}

/// `place.search` as `principal` reads it (`src/search/`): the parts of the Place's index that no longer hold (their
/// collection's change token moved, their file changed) are read again, within the reader's access (its row filters make
/// parts of their own); the hits are those of the App instances it sees. File texts not read yet past [`CONTENT_BUDGET`]
/// wait for the next search (`pending`). `files` is where the native Files store lives (`None`: no file text).
#[cfg(feature = "search")]
pub(super) fn place_search_for(engine: &Engine, files: Option<&std::path::Path>, id: RequestId, principal: &Principal, delegation: Option<&super::GatewayDelegation>, input: &PlaceSearchInput) -> Result<JsonValue, QueryResponse> {
    use std::{collections::HashMap, io::Read, sync::{Arc, Mutex, OnceLock}};
    use crate::search::{search, sources::{files_query, Fresh, Place, Reader, Source}, Part};
    /// The file texts one search reads at most for this long.
    const CONTENT_BUDGET: Duration = Duration::from_secs(3);
    static PARTS: OnceLock<Mutex<HashMap<(String, String, String), (JsonValue, Arc<Part>)>>> = OnceLock::new();
    static FILES: OnceLock<Mutex<HashMap<String, (JsonValue, Arc<Vec<JsonValue>>)>>> = OnceLock::new();
    struct Daemon<'a> { engine: &'a Engine, files: Option<&'a std::path::Path>, context: &'a ExecutionContext, id: RequestId }
    impl Reader for Daemon<'_> {
        fn rows(&mut self, query: String) -> Result<Vec<JsonValue>, String> {
            let scoped = !query.starts_with("on _");
            match execute_request_scoped(self.engine, QueryRequest::new(self.id, query), scoped.then_some(self.context)) {
                QueryResponse::Ok { documents, .. } => Ok(documents),
                QueryResponse::Error { error, .. } => Err(format!("{}: {}", error.code, error.message)),
            }
        }
        fn content(&mut self, instance_id: &str, remote_id: &str) -> Result<Vec<u8>, String> {
            let root = self.files.ok_or("no Files store")?.join(file_scope_component(&self.context.place_id)).join(file_scope_component(instance_id));
            let store = crate::files::NativeFileStore::new(StoreId::from(DEFAULT_FILES_STORE_ID), root).map_err(|error| error.to_string())?;
            let mut bytes = Vec::new();
            store.read(remote_id, None).map_err(|error| error.to_string())?.take(crate::search::extract::MAX_BYTES).read_to_end(&mut bytes).map_err(|error| error.to_string())?;
            Ok(bytes)
        }
    }
    let started = Instant::now();
    // Through a Gateway, the member's grant on the database of the Place (its role and row filters), as query.execute.
    let delegated = delegation.filter(|value| value.capability == "database" && value.place_id.as_deref() == Some(input.place_id.as_str()))
        .and_then(|value| value.access().map(|access| ExecutionContext { principal: value.principal.clone(), place_id: input.place_id.clone(), app_instance_id: None, access, filters: value.filters.clone() }));
    let context = match delegated { Some(context) => context, None => resolve_query_execution_context(engine, id, principal, RequestedExecutionContext { place_id: input.place_id.clone(), app_instance_id: None, as_profile: None })? };
    let mut installed = Vec::new();
    let mut visible = std::collections::BTreeSet::new();
    // The Apps of the reader's profile, where the Place is known (through a Gateway, the database Core may not hold it).
    let apps = match load_place(engine, id, &input.place_id) { Ok(place) => member_view(engine, id, &context.principal, &place, context.access, None)?.0, Err(_) => None };
    for instance in query_documents(engine, id, quoted!("on _app_instances | where placeId == {} and state == \"active\" | sort createdAt", &input.place_id))? {
        if app_instance_visible(engine, id, &context.principal, &context.access, apps.as_deref(), &instance)? { visible.insert(format!("app:{}", instance.string_field("instanceId"))); }
        if let Some(app) = instance.str_field("appId").and_then(|app_id| load_app_definition(engine, id, app_id).ok()) { installed.push((instance, app)); }
    }
    let schema = place_schema(&installed);
    let filters = serde_json::to_string(&context.filters.iter().map(|(field, values)| (field, values.iter().map(|value| format!("{value:?}")).collect::<Vec<_>>())).collect::<Vec<_>>()).unwrap_or_default();
    let failed = |message: String| QueryResponse::request_error(id, "place.search_failed", message);
    let mut reader = Daemon { engine, files, context: &context, id };
    // The files of the Place, listed again when `_files` changed.
    let token = engine.change_token("_files", Some(&input.place_id));
    let kept = token.as_ref().and_then(|token| lock_unpoisoned(FILES.get_or_init(Default::default)).get(&input.place_id).filter(|(known, _)| known == token).map(|(_, files)| files.clone()));
    let listed = match kept { Some(files) => files, None => Arc::new(reader.rows(files_query(&input.place_id)).map_err(failed)?) };
    if let Some(token) = token { lock_unpoisoned(FILES.get_or_init(Default::default)).insert(input.place_id.clone(), (token, listed.clone())); }
    let place = Place { id: &input.place_id, schema: &schema, apps: &installed, files: &listed };
    let sources = Source::of(&place);
    let (mut parts, mut read, mut pending, mut reading) = (Vec::new(), 0, 0, Duration::ZERO);
    for source in &sources {
        let (token, filtered) = match source.fresh() {
            Fresh::Never => (None, false),
            Fresh::Collection { name, filtered } => (engine.change_token(name, Some(&input.place_id)), filtered),
            Fresh::Stamp(stamp) => (Some(stamp.clone()), false),
        };
        let key = (input.place_id.clone(), source.key(), if filtered { filters.clone() } else { String::new() });
        let kept = token.as_ref().and_then(|token| lock_unpoisoned(PARTS.get_or_init(Default::default)).get(&key).filter(|(known, _)| known == token).map(|(_, part)| part.clone()));
        let part = match kept {
            Some(part) => part,
            None if matches!(source, Source::Content(_)) && reading > CONTENT_BUDGET => { pending += 1; continue; }
            None => {
                let begun = Instant::now();
                let part = Arc::new(source.read(&place, &mut reader).map_err(failed)?);
                if matches!(source, Source::Content(_)) { reading += begun.elapsed(); }
                read += 1;
                if let Some(token) = token { lock_unpoisoned(PARTS.get_or_init(Default::default)).insert(key, (token, part.clone())); }
                part
            }
        };
        parts.push(part);
    }
    // The parts of sources the Place no longer has leave.
    let keys: std::collections::BTreeSet<String> = sources.iter().map(Source::key).collect();
    lock_unpoisoned(PARTS.get_or_init(Default::default)).retain(|(place, source, _), _| place != &input.place_id || keys.contains(source));
    let built = started.elapsed();
    let hits = search(&parts.iter().map(Arc::as_ref).collect::<Vec<_>>(), &input.text, &input.scope, &|origin| !origin.scope.iter().any(|key| key.starts_with("app:")) || origin.scope.iter().any(|key| visible.contains(key)), input.limit.unwrap_or(20));
    Ok(serde_json::json!({"placeId": input.place_id, "results": hits.iter().map(|hit| hit.to_json()).collect::<Vec<_>>(), "entries": parts.iter().map(|part| part.len()).sum::<usize>(), "partsRead": read, "pending": pending, "readMs": built.as_millis() as u64, "searchMs": (started.elapsed() - built).as_millis() as u64}))
}
