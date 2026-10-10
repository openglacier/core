//! Permission, Sharing, Place, App and data import requests.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;
use super::serve::{diverged, ensure_operation_resource_authorized, place_role, reply, require_identity, require_place_owner, write_query_then_publish, Handled};

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
        RoutedOperation::PlaceUpdate(Routed { id, input: PlaceUpdateInput { place_id, name, title, subtitle, color_scheme, app_order, updated_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to update a Place")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only a Place Owner may update its presentation")?;
            let updated_at = updated_at.unwrap_or_else(unix_time_millis);
            let mut assignments = Vec::new();
            if let Some(name) = name.as_ref() { assignments.push(format!("name = {}", query_string(name))); }
            if let Some(title) = title.as_ref() { assignments.push(format!("title = {}", query_string(title))); }
            if let Some(subtitle) = subtitle.as_ref() { assignments.push(format!("subtitle = {}", query_string(subtitle))); }
            if let Some(color_scheme) = color_scheme.as_ref() { assignments.push(format!("colorScheme = {}", query_string(color_scheme))); }
            if let Some(app_order) = app_order.as_ref() {
                let order_json = serde_json::to_string(app_order).expect("Place app order serializes");
                assignments.push(format!("appOrder = {order_json}"));
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
                    "colorScheme": color_scheme.is_some(), "appOrder": app_order.is_some()
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
            let entries = list_place_access(engine, id, &place)?;
            Ok(reply(writer, id, serde_json::json!({ "placeId": place_id, "ownerIdentityId": place.owner_identity_id, "entries": entries, }))?)
        }
        RoutedOperation::PlaceAccessSet(Routed { id, input: PlaceAccessSetInput { place_id, identity_id: target_identity_id, role }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to manage Place access")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only the Place Owner may change access")?;
            if target_identity_id == place.owner_identity_id {
                reject!(id, "place.owner_immutable", "the Place Owner cannot be changed through Place access");
            }

            let sharing_id = place_access_sharing_id(&place_id, &target_identity_id);
            let permission = sharing_permission(&place_id, role);
            let existing = load_sharing(engine, id, &sharing_id).ok();

            let query = if existing.is_some() {
                // `set` expressions intentionally only accept scalar expressions today;
                // JSON array literals such as `["place:...:resident"]` are not valid
                // expression RHS values. Keep the Place role as a dedicated scalar field
                // and preserve `permissions` for backward-compatible/general sharings.
                format!(
                    "on _sharings | where sharingId == {} | set placeRole = {}, state = \"active\", updatedAt = {}",
                    query_string(&sharing_id),
                    query_string(role.as_str()),
                    unix_time_millis(),
                )
            } else {
                let created_at = unix_time_millis();
                insert_query("_sharings", serde_json::json!({"sharingId": &sharing_id, "owner": &place.owner_identity_id, "target": &target_identity_id, "permissions": [&permission], "placeRole": role.as_str(), "state": "active", "createdAt": created_at}))
            };

            query_documents(engine, id, query)?;
            let audience = place_audience(engine, id, &place_id)?;
            publish_durable_event(engine, audience, "place.access.updated", serde_json::json!({"placeId": place_id, "identityId": target_identity_id, "role": role.as_str()}));
            Ok(reply(writer, id, serde_json::json!({"placeId": place_id, "identityId": target_identity_id, "role": role.as_str(), "state": "active"}))?)
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
        RoutedOperation::QueryContextResolve(Routed { id, input: QueryContextResolveInput { place_id, app_instance_id } }) => {
            let context = resolve_query_execution_context(engine, id, principal, RequestedExecutionContext { place_id, app_instance_id })?;
            reply(writer, id, serde_json::json!({
                "placeId": context.place_id,
                "appInstanceId": context.app_instance_id,
                "placeRole": context.access.role().map(PlaceRole::as_str),
                "publicAccess": context.access.public_access().map(PublicAccess::as_str),
            }))?;
        }
        RoutedOperation::AppCreate(Routed { id, input: AppCreateInput { app_id, place_id, name, version, definition, created_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to create an App")?;
            if let Some(ref place_id) = place_id {
                let place = load_place(engine, id, place_id)?;
                require_place_owner(engine, id, identity_id, &place, "only the Place Owner may create an App in this Place")?;
            } else {
                ensure_operation_resource_authorized(settings, engine, principal, id, OperationKind::AppCreate, &app_id)?;
            }
            if let Err(message) = validate_app_definition_model(&definition) {
                reject!(id, "app.invalid_definition", message);
            }
            if app_record_exists(engine, id, &app_id)? {
                reject!(id, "app.already_exists", "an App with this id already exists");
            }
            let created_at = created_at.unwrap_or_else(unix_time_millis);
            let mut document = serde_json::json!({"appId": &app_id, "name": &name, "version": &version, "definition": &definition, "createdBy": identity_id, "state": "active", "createdAt": created_at});
            if let Some(owner_place_id) = place_id.as_ref() {
                document["ownerPlaceId"] = owner_place_id.as_str().into();
                document["maintainersJson"] = "[]".into();
            }
            let query = insert_query("_apps", document);
            query_documents(engine, id, query)?;

            let instance_id = if let Some(ref place_id) = place_id {
                let instance_id = UuidV7Generator::new().next_id().to_string();
                let query = insert_query("_app_instances", serde_json::json!({"instanceId": &instance_id, "placeId": place_id, "appId": &app_id, "name": &name, "config": {}, "state": "active", "createdAt": created_at}));
                query_documents(engine, id, query)?;
                Some(instance_id)
            } else { None };

            publish_durable_event(engine, Audience::Global, "app.created", serde_json::json!({"appId": app_id, "placeId": place_id, "instanceId": instance_id, "name": name, "version": version, "createdBy": identity_id, "createdAt": created_at}));
            reply(writer, id, serde_json::json!({"appId": app_id, "placeId": place_id, "instanceId": instance_id, "name": name, "version": version, "definition": definition, "createdBy": identity_id, "state": "active", "createdAt": created_at}))?;
        }
        RoutedOperation::AppList(Routed { id, .. }) => {
            let documents = query_documents(engine, id, "on _apps | where state == \"active\" | sort name")?;
            // Without an identity under enforcement, reading an App definition is a read
            // on a public Place where it is instantiated: only those Apps are listed.
            let documents = if settings.authorization_mode.is_enforced() && principal_identity_id(principal).is_none() {
                let readable = public_place_app_ids(engine, id)?;
                documents.into_iter().filter(|document| document.str_field("appId").is_some_and(|app_id| readable.contains(app_id))).collect()
            } else { documents };
            reply(writer, id, serde_json::json!({ "apps": documents }))?;
        }
        RoutedOperation::AppGet(Routed { id, input: AppIdInput { app_id } }) => {
            if settings.authorization_mode.is_enforced() && principal_identity_id(principal).is_none()
                && !public_place_app_ids(engine, id)?.contains(&app_id) {
                reject!(id, "authorization.required", "an authenticated identity is required to read an App outside public Places");
            }
            reply(writer, id, load_app_definition(engine, id, &app_id)?)?;
        }
        RoutedOperation::AppUpdate(Routed { id, input: AppUpdateInput { app_id, place_id, name, version, definition, maintainers, updated_at }, }) => {
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
            load_app_definition(engine, id, &app_id)?;
            let updated_at = updated_at.unwrap_or_else(unix_time_millis);
            let definition_json =
                serde_json::to_string(&definition).expect("App definition serializes");
            let query = quoted!("on _apps | where appId == {} and state == \"active\" | set name = {}, version = {}, definition = {definition_json}, updatedBy = {}, updatedAt = {updated_at}", &app_id, &name, &version, identity_id);
            query_documents(engine, id, query)?;
            publish_durable_event(engine, Audience::Global, "app.updated", serde_json::json!({"appId": app_id, "name": name, "version": version, "updatedBy": identity_id, "updatedAt": updated_at}));
            reply(writer, id, serde_json::json!({"appId": app_id, "name": name, "version": version, "definition": definition, "updatedBy": identity_id, "state": "active", "updatedAt": updated_at}))?;
        }
        RoutedOperation::AppDelete(Routed { id, input: AppDeleteInput { app_id, deleted_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to delete an App")?;
            ensure_operation_resource_authorized(settings, engine, principal, id, OperationKind::AppDelete, &app_id)?;
            load_app_definition(engine, id, &app_id)?;
            let instances = query_documents(engine, id, quoted!("on _app_instances | where appId == {} and state == \"active\" | limit 1", &app_id))?;
            if !instances.is_empty() {
                reject!(id, "app.in_use", "the App still has active instances");
            }
            let deleted_at = deleted_at.unwrap_or_else(unix_time_millis);
            let query = quoted!("on _apps | where appId == {} and state == \"active\" | set state = \"deleted\", deletedBy = {}, deletedAt = {deleted_at}", &app_id, identity_id);
            query_documents(engine, id, query)?;
            publish_durable_event(engine, Audience::Global, "app.deleted", serde_json::json!({"appId": app_id, "deletedBy": identity_id, "deletedAt": deleted_at}));
            reply(writer, id, serde_json::json!({"appId": app_id, "state": "deleted", "deletedBy": identity_id, "deletedAt": deleted_at}))?;
        }
        RoutedOperation::AppInstanceCreate(Routed { id, input: AppInstanceCreateInput { place_id, app_id, name, config, created_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to attach an App")?;
            let place = load_place(engine, id, &place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only the Place Owner may attach an App")?;
            load_app_definition(engine, id, &app_id)?;
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
            let mut documents = Vec::new();
            for document in query_documents(engine, id, quoted!("on _app_instances | where placeId == {} and state == \"active\" | sort createdAt", &place_id))? {
                if app_instance_visible(engine, id, principal, &access, &document)? {
                    documents.push(document);
                }
            }
            reply(writer, id, serde_json::json!({"placeId": place_id, "role": access.role().map(PlaceRole::as_str), "publicAccess": access.public_access().map(PublicAccess::as_str), "instances": documents}))?;
        }
        RoutedOperation::AppInstanceRemove(Routed { id, input: AppInstanceRemoveInput { instance_id, removed_at }, }) => {
            let identity_id = require_identity(principal, id, "an authenticated identity is required to remove an App")?;
            let instance = load_app_instance(engine, id, &instance_id)?;
            let place = load_place(engine, id, &instance.place_id)?;
            require_place_owner(engine, id, identity_id, &place, "only the Place Owner may remove an App")?;
            let removed_at = removed_at.unwrap_or_else(unix_time_millis);
            let query = quoted!("on _app_instances | where instanceId == {} and state == \"active\" | set state = \"removed\", removedAt = {removed_at}", &instance_id);
            query_documents(engine, id, query)?;
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
            let response = data_import(engine, principal, delegation, id, input, || data_worker_result(settings, engine, id, None, &place_id, &files_instance_id, &file_id, "import", Some(&mapping), "data.import requires a data.import worker result on this database node"))?;
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
pub(super) fn data_import(engine: &Engine, principal: &Principal, delegation: Option<&super::GatewayDelegation>, id: RequestId, input: DataImportInput, worker: impl FnOnce() -> Result<JsonValue, super::serve::Stop>) -> Result<JsonValue, super::serve::Stop> {
    let DataImportInput { place_id, target_instance_id, table, mapping, mode, worker_result, plan_only, import_id, accept, check_only, cancel, plan, .. } = input;
    let worker = || match worker_result { Some(result) => Ok(result), None => worker() };
    let (collection, mode, declared) = match plan {
        Some(plan) => {
            let delegated = delegation.is_some_and(|delegated| delegated.capability == "database" && delegated.place_id.as_deref() == Some(place_id.as_str()) && delegated.app_instance_id.as_deref() == Some(target_instance_id.as_str()) && delegated.access().is_some_and(PlaceAccess::can_write));
            if !delegated { reject!(id, "authorization.denied", "a planned data import requires the Gateway's write delegation for its App instance"); }
            let Some(collection) = plan["collection"].as_str().filter(|collection| crate::view::is_name(collection)) else { reject!(id, "data.invalid_target", "the import plan names no collection") };
            (collection.to_owned(), plan["mode"].as_str().unwrap_or("append").to_owned(), plan["import"].clone())
        }
        None => {
            let access = writable_place(engine, id, principal, &place_id, "data import requires write access to the Place")?;
            let instance_docs = query_documents(engine, id, quoted!("on _app_instances | where instanceId == {} and placeId == {} and state == \"active\" | limit 1", &target_instance_id, &place_id))?;
            let Some(instance_doc) = instance_docs.first() else { reject!(id, "app.instance_not_found", "target App instance was not found in this Place"); };
            let app_id = instance_doc.str_field("appId").unwrap_or_default(); if app_id.is_empty() { reject!(id, "app.invalid_instance_record", "target App instance has no appId"); }
            let app = load_app_definition(engine, id, app_id)?;
            let Some(collection) = resolve_app_table_collection(&app, &table) else { reject!(id, "data.invalid_target", "target table is not declared by the App") };
            let mode = mode.as_deref().or_else(|| mapping.get("import").and_then(|v| v.get("mode")).and_then(JsonValue::as_str)).unwrap_or("append").to_owned();
            let declared = app["definition"]["model"]["tables"][&table]["import"].clone();
            if plan_only {
                let result = if import_id.is_none() { worker()? } else { JsonValue::Null };
                let rows = result.array_field("documents").map(Vec::len);
                return Ok(serde_json::json!({"status": "planned", "placeId": place_id, "appInstanceId": target_instance_id, "placeRole": access.role().map(PlaceRole::as_str), "publicAccess": access.public_access().map(PublicAccess::as_str), "collection": collection, "mode": mode, "import": declared, "rows": rows, "warnings": result.get("warnings").cloned().unwrap_or(JsonValue::Array(vec![]))}));
            }
            (collection, mode, declared)
        }
    };
    // The rows wait in `<collection>.staged`, in the scope of the target instance (DocumentScope stays the single
    // authority for _place/_app_instance), until the checks the App declares for the table pass or are accepted.
    let staged = format!("{collection}.staged");
    let scope = crate::view::scope_of(&place_id, Some(&target_instance_id));
    let failed = |error: String| super::serve::request_error(id, "data.import_failed", error);
    let run = |query: &str| crate::view::execute(engine, Some(&scope), query, false).map_err(failed);
    let (import_id, warnings) = match import_id {
        Some(import_id) => {
            let this = format!("on {staged} | where import_id == {}", JsonValue::from(import_id.as_str()));
            if run(&format!("{this} | limit 1"))?.is_empty() { reject!(id, "data.import_not_found", "no import with this importId waits in this App instance"); }
            if cancel {
                run(&format!("{this} | delete"))?;
                return Ok(serde_json::json!({"status": "cancelled", "importId": import_id}));
            }
            (import_id, JsonValue::Array(vec![]))
        }
        None => {
            let result = worker()?;
            let documents = result.array_field("documents").cloned().unwrap_or_default();
            if documents.iter().any(|document| !document.is_object()) {
                reject!(id, "data.invalid_worker_output", "data worker returned a non-object document");
            }
            let warnings = result.get("warnings").cloned().unwrap_or(JsonValue::Array(vec![]));
            let (import_id, now) = (UuidV7Generator::new().next_id().to_string(), unix_time_millis());
            // An import nobody confirmed or cancelled goes after a week.
            run(&format!("on {staged} | where staged_at < {} | delete", now.saturating_sub(STAGED_FOR)))?;
            let rows = documents.into_iter().map(|mut document| { let fields = document.as_object_mut().expect("checked above"); fields.insert("import_id".into(), import_id.clone().into()); fields.insert("staged_at".into(), now.into()); document }).collect::<Vec<_>>();
            if !rows.is_empty() { run(&format!("on {staged} | insert {}", JsonValue::Array(rows)))?; }
            (import_id, warnings)
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
    let mut response = serde_json::json!({"status": status, "importId": import_id, "checks": checks, "summary": summary, "mode": mode, "warnings": warnings});
    if status == "completed" {
        let this = format!("on {staged} | where import_id == {}", JsonValue::from(import_id.as_str()));
        let rows = run(&format!("{this} | count as n"))?.first().and_then(|row| row["n"].as_u64()).unwrap_or(0);
        // `replace`: the stored rows sharing these fields with a staged row (an entity's months) give way to it.
        let same = declared["replace"].as_array().into_iter().flatten().filter_map(JsonValue::as_str).filter(|field| crate::view::is_name(field)).map(|field| format!("s.{field}? == t.{field}? and ")).collect::<String>();
        let replace = match mode.as_str() {
            "replace" | "replace_all" => Some(format!("on {collection} | delete")),
            _ => (!same.is_empty()).then(|| format!("on {collection} as t | lookup {staged} as s | where {same}s.import_id == {} | first | into same | end | where len(same) > 0 | delete", JsonValue::from(import_id.as_str()))),
        };
        let writes = replace.into_iter().chain([format!("{this} | drop staged_at | into {collection}")]).chain(declared["after"].as_array().into_iter().flatten().filter_map(JsonValue::as_str).map(str::to_owned)).chain([format!("{this} | delete")]).collect::<Vec<_>>();
        // All or nothing: each write reads the rows as they were before the import.
        crate::view::write_together(engine, &place_id, Some(&target_instance_id), &writes, &parameters).map_err(failed)?;
        if let Ok(audience) = place_audience(engine, id, &place_id) {
            publish_durable_event(engine, audience, "app.data.changed", serde_json::json!({"placeId": place_id, "appInstanceId": target_instance_id, "table": table, "rows": rows}));
        }
        response["rows"] = rows.into();
    }
    Ok(response)
}
