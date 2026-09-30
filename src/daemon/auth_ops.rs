//! Authentication, Identity and Device requests.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;
use super::serve::{auth_failed, authenticated_device, diverged, ensure_operation_resource_authorized, failed, reply, require_identity, write_query_then_publish, Connection, Handled};

#[cfg(feature = "auth")]
pub(super) fn handle_authentication_operation(connection: &mut Connection, settings: &ConnectionSettings, engine: &Engine, event_engine: &EventEngine, operation: RoutedOperation) -> Handled {
    let connection_id = connection.id;
    let authentication = &mut connection.authentication;
    let writer = &mut connection.writer;
    match operation {
        RoutedOperation::AuthBegin(Routed { id, input: AuthBeginInput { identity_id, device_id } }) => {
            debug::log( DebugTopic::Auth, Some(connection_id), format!("begin identity={identity_id} device={device_id}"), );
            let credential = load_device_credential(engine, id, &identity_id, &device_id)?;
            let challenge = authentication.begin(&credential, DEFAULT_CHALLENGE_TTL).map_err(auth_failed(id))?;
            reply(writer, id, serde_json::to_value(challenge).expect("challenge serializes"))?;
        }
        RoutedOperation::AuthComplete(Routed { id, input: ChallengeSignatureInput { challenge_id, signature } }) => {
            let Some((identity_id, device_id)) = pending_auth_subject(authentication) else {
                reject!(id, "auth.failed", "no authentication challenge is pending");
            };
            let credential = load_device_credential(engine, id, &identity_id, &device_id)?;
            let principal = authentication.complete(&challenge_id, &signature, &credential).map_err(auth_failed(id))?;
            debug::log( DebugTopic::Auth, Some(connection_id), format!("authenticated principal={principal:?}"), );
            event_engine.publish_global(
                "auth.authenticated",
                serde_json::json!({"identityId": identity_id, "deviceId": device_id}),
            );
            if settings.authenticated_keepalive && connection.subscription.is_none() {
                let subscription_id = connection.subscribe(event_engine.subscribe(vec!["core.heartbeat".to_owned()]))?;
                debug::log( DebugTopic::Events, Some(connection_id), format!("authenticated keepalive subscribed subscription_id={subscription_id}"), );
            }
            reply(&mut connection.writer, id, serde_json::json!({"authenticated": true, "principal": principal}))?;
        }
        RoutedOperation::AuthEnrollBegin(Routed { id, input: AuthEnrollBeginInput { identity_id, identity_public_key, device_id, device_public_key, token } }) => {
            if !settings.enrollment_mode.allows(token.as_deref()) {
                reject!(id, "auth.enrollment_denied", "identity enrollment is not allowed");
            }
            let challenge = authentication.begin_enrollment(
                identity_id,
                identity_public_key,
                device_id,
                device_public_key,
                DEFAULT_CHALLENGE_TTL,
            ).map_err(auth_failed(id))?;
            reply(writer, id, serde_json::to_value(challenge).expect("challenge serializes"))?;
        }
        RoutedOperation::AuthEnrollComplete(Routed { id, input: ChallengeSignatureInput { challenge_id, signature } }) => {
            let enrollment = authentication.complete_enrollment(&challenge_id, &signature).map_err(auth_failed(id))?;
            let created_at = unix_time_millis();
            let identity_query = format!("on _identities | insert {{identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {created_at}}}", query_string(&enrollment.identity_id), query_string(&enrollment.identity_public_key));
            query_documents(engine, id, identity_query)?;
            // Each failed step rolls back the records written before it.
            let rollback_identity = format!( "on _identities | where identityId == {} | delete", query_string(&enrollment.identity_id) );
            let device_query = format!("on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {created_at}}}", query_string(&enrollment.device_id), query_string(&enrollment.identity_id), query_string(&enrollment.device_public_key));
            if let Err(response) = query_documents(engine, id, device_query) {
                let _ = execute_request(engine, QueryRequest::new(id, rollback_identity));
                return Err(response.into());
            }

            // Stateless enrollment grants the smallest useful baseline:
            // the new identity may subscribe to its authorized event stream.
            // Broader query/system permissions still require explicit grants.
            let permission_query = enrollment_events_permission_query(&enrollment.identity_id, created_at);
            if let Err(response) = query_documents(engine, id, permission_query) {
                let rollback_device = format!( "on _devices | where deviceId == {} | delete", query_string(&enrollment.device_id) );
                let _ = execute_request(engine, QueryRequest::new(id, rollback_device));
                let _ = execute_request(engine, QueryRequest::new(id, rollback_identity));
                return Err(response.into());
            }

            debug::log( DebugTopic::Permission, Some(connection_id), format!( "grant identity={} action=events.subscribe resource=* source=enrollment", enrollment.identity_id ), );
            publish_durable_event(
                engine,
                Audience::identities([enrollment.identity_id.clone()]),
                "identity.enrolled",
                serde_json::json!({"identityId": enrollment.identity_id, "deviceId": enrollment.device_id, "createdAt": created_at}),
            );
            reply(writer, id, serde_json::json!({"enrolled": true, "identityId": enrollment.identity_id, "deviceId": enrollment.device_id}))?;
        }
        RoutedOperation::AuthClassicRegister(Routed { id, input: ClassicAuthRegisterInput { identifier, password } }) => {
            if !settings.classic_auth_enabled { reject!(id, "auth.classic_disabled", "classic authentication is disabled"); }
            let identity_id = require_identity(authentication.principal(), id, "an authenticated identity is required")?;
            let identifier = identifier.trim();
            if identifier.is_empty() || identifier.len() > 200 { reject!(id, "auth.invalid_identifier", "identifier must contain between 1 and 200 bytes"); }
            if password.len() < CLASSIC_AUTH_MIN_PASSWORD_BYTES { reject!(id, "auth.weak_password", "password must contain at least 12 bytes"); }
            let normalized = identifier.to_lowercase();
            let existing = query_documents(engine, id, format!("on _credentials | where loginNormalized == {} or identityId == {} | limit 1", query_string(&normalized), query_string(identity_id)))?;
            if !existing.is_empty() { reject!(id, "auth.identifier_unavailable", "identifier or identity is already registered"); }
            let (salt, hash) = classic_password_hash(&password).map_err(failed(id, "auth.hash_failed"))?;
            let now = unix_time_millis();
            query_documents(engine, id, format!("on _credentials | insert {{identityId: {}, login: {}, loginNormalized: {}, salt: {}, hash: {}, algorithm: \"argon2id\", memoryKiB: {}, iterations: {}, lanes: {}, state: \"active\", createdAt: {now}}}", query_string(identity_id), query_string(identifier), query_string(&normalized), query_string(&salt), query_string(&hash), CLASSIC_AUTH_MEMORY_KIB, CLASSIC_AUTH_ITERATIONS, CLASSIC_AUTH_LANES))?;
            reply(writer, id, serde_json::json!({"registered": true, "identifier": identifier}))?;
        }
        RoutedOperation::AuthClassicLogin(Routed { id, input: ClassicAuthLoginInput { identifier, password, device_id, public_key } }) => {
            if !settings.classic_auth_enabled { reject!(id, "auth.classic_disabled", "classic authentication is disabled"); }
            let normalized = identifier.trim().to_lowercase();
            if normalized.is_empty() || normalized.len() > 200 { reject!(id, "auth.invalid_credentials", "invalid identifier or password"); }
            if let Some(wait_ms) = classic_login_wait(settings, &normalized) { reject!(id, "auth.rate_limited", format!("retry after {wait_ms}ms")); }
            let documents = query_documents(engine, id, format!("on _credentials | where loginNormalized == {} and state == \"active\" | limit 1", query_string(&normalized)))?;
            let credential = documents.first().and_then(JsonValue::as_object);
            let valid = credential.and_then(|object| Some((object.get("salt")?.as_str()?, object.get("hash")?.as_str()?))).is_some_and(|(salt, hash)| classic_password_verify(&password, salt, hash));
            if !valid {
                if credential.is_none() { let _ = classic_password_hash_with_salt(&password, &[0x47; 16]); }
                classic_login_failure(settings, &normalized);
                reject!(id, "auth.invalid_credentials", "invalid identifier or password");
            }
            classic_login_success(settings, &normalized);
            let identity_id = credential.and_then(|object| object.get("identityId")).and_then(JsonValue::as_str).unwrap_or_default().to_owned();
            if identity_id.is_empty() { reject!(id, "auth.invalid_credentials", "invalid identifier or password"); }
            validate_ed25519_public_key(&public_key).map_err(failed(id, "auth.invalid_public_key"))?;
            let identities = query_documents(engine, id, format!("on _identities | where identityId == {} and state == \"active\" | limit 1", query_string(&identity_id)))?;
            if identities.is_empty() { reject!(id, "auth.invalid_credentials", "invalid identifier or password"); }
            if let Some(existing) = try_load_device_credential(engine, id, &identity_id, &device_id)? {
                if !existing.active || existing.public_key != public_key { reject!(id, "auth.device_mismatch", "device identifier is already registered with different credentials"); }
            } else {
                let collision = query_documents(engine, id, format!("on _devices | where deviceId == {} | limit 1", query_string(&device_id)))?;
                if !collision.is_empty() { reject!(id, "auth.device_mismatch", "device identifier is already registered"); }
                let now = unix_time_millis();
                query_documents(engine, id, format!("on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {now}}}", query_string(&device_id), query_string(&identity_id), query_string(&public_key)))?;
            }
            reply(writer, id, serde_json::json!({"accepted": true, "identityId": identity_id, "deviceId": device_id}))?;
        }
        RoutedOperation::IdentityOpen(Routed { id, input: IdentityOpenInput { file, password, client_device_id } }) => {
            let Ok(bytes) = decode_base64(&file) else { reject!(id, "identity.invalid_file", "identity file must be base64 encoded") };
            let portable = identity_file::decrypt_bytes(&bytes, password.as_bytes()).map_err(failed(id, "identity.open_failed"))?;
            let source = load_device_credential(engine, id, &portable.identity_id, &portable.device_id)?;
            if source.public_key != portable.public_key { reject!(id, "auth.device_mismatch", "portable identity does not match the registered source device"); }
            if !source.active { reject!(id, "auth.device_revoked", "portable identity source device is revoked"); }
            let source_device_id = portable.device_id.clone();
            let target_device_id = client_device_id.filter(|device_id| device_id != &source_device_id).unwrap_or_else(|| source_device_id.clone());
            let (device, enrolled) = if target_device_id == source_device_id {
                (source, false)
            } else if let Some(device) = try_load_device_credential(engine, id, &portable.identity_id, &target_device_id)? {
                (device, false)
            } else {
                let now = unix_time_millis();
                query_documents(engine, id, format!("on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", sourceDeviceId: {}, createdAt: {now}}}", query_string(&target_device_id), query_string(&portable.identity_id), query_string(&portable.public_key), query_string(&source_device_id)))?;
                (DeviceCredential { identity_id: portable.identity_id.clone(), device_id: target_device_id.clone(), public_key: portable.public_key.clone(), algorithm: "ed25519".to_owned(), encoding: "spki-der".to_owned(), active: true }, true)
            };
            let principal = authentication.establish(&device).map_err(auth_failed(id))?;
            reply(writer, id, serde_json::json!({"authenticated": true, "principal": principal, "deviceId": device.device_id, "sourceDeviceId": source_device_id, "enrolled": enrolled}))?;
        }
        RoutedOperation::IdentityGet(Routed { id, input: PasswordInput { password } }) => {
            let identity_id = require_identity(authentication.principal(), id, "an authenticated identity is required")?;
            let portable = IdentityCredential::renew(identity_id).map_err(failed(id, "identity.generate_failed"))?;
            let bytes = identity_file::encrypt_bytes(&portable, password.as_bytes()).map_err(failed(id, "identity.encrypt_failed"))?;
            let now = unix_time_millis();
            query_documents(engine, id, format!( "on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {now}}}", query_string(&portable.device_id), query_string(identity_id), query_string(&portable.public_key), ))?;
            reply(writer, id, serde_json::json!({ "file": encode_base64(&bytes), "fileName": format!("{identity_id}.ogid"), "identityId": identity_id, "deviceId": portable.device_id, }))?;
        }
        RoutedOperation::IdentityRenew(Routed { id, input: IdentityRenewInput { device_id, public_key, password } }) => {
            let (identity_id, current_device_id) = authenticated_device(authentication.principal(), id, "an authenticated identity is required to renew credentials")?;
            let (device_id, public_key, portable_file) = if let Some(password) = password {
                let portable = IdentityCredential::renew(&identity_id).map_err(failed(id, "identity.generate_failed"))?;
                let bytes = identity_file::encrypt_bytes(&portable, password.as_bytes()).map_err(failed(id, "identity.encrypt_failed"))?;
                (portable.device_id, portable.public_key, Some(bytes))
            } else {
                (device_id.unwrap_or_default(), public_key.unwrap_or_default(), None)
            };
            if device_id == current_device_id { reject!(id, "identity.invalid_device", "renewal requires a new device identifier"); }
            validate_ed25519_public_key(&public_key).map_err(failed(id, "identity.invalid_public_key"))?;
            let now = unix_time_millis();
            for query in [
                format!("on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {now}}}", query_string(&device_id), query_string(&identity_id), query_string(&public_key)),
                format!("on _identities | where identityId == {} | set publicKey = {}, algorithm = \"ed25519\", encoding = \"spki-der\", updatedAt = {now}", query_string(&identity_id), query_string(&public_key)),
                format!("on _devices | where deviceId == {} and identityId == {} | set state = \"revoked\", revokedAt = {now}", query_string(&current_device_id), query_string(&identity_id)),
            ] { query_documents(engine, id, query)?; }
            let principal = Principal::Identity { identity_id: identity_id.clone(), device_id: device_id.clone() };
            if portable_file.is_some() {
                let device = DeviceCredential { identity_id: identity_id.clone(), device_id: device_id.clone(), public_key: public_key.clone(), algorithm: "ed25519".to_owned(), encoding: "spki-der".to_owned(), active: true };
                authentication.establish(&device).map_err(auth_failed(id))?;
            }
            publish_durable_event(engine, Audience::identities([identity_id.clone()]), "identity.renewed", serde_json::json!({"identityId": identity_id, "deviceId": device_id, "previousDeviceId": current_device_id, "renewedAt": now}));
            let mut data = serde_json::json!({ "identityId": identity_id, "deviceId": device_id, "previousDeviceId": current_device_id, "renewedAt": now, "principal": principal });
            if let Some(bytes) = portable_file {
                data["file"] = JsonValue::String(encode_base64(&bytes));
                data["fileName"] = JsonValue::String(format!("{}.ogid", identity_id));
            }
            reply(writer, id, data)?;
        }
        RoutedOperation::DeviceIdentify(Routed { id, .. }) => {
            let identity_id = require_identity(authentication.principal(), id, "authentication is required")?;
            let documents = query_documents(engine, id, format!("on _devices | where identityId == {} and state == \"active\" | sort createdAt asc", query_string(identity_id)))?;
            let devices = documents.iter().enumerate().filter_map(|(index, device)| device.str_field("deviceId").map(|device_id| serde_json::json!({"deviceId": device_id, "number": index + 1}))).collect::<Vec<_>>();
            let payload = serde_json::json!({"identityId": identity_id, "expiresAt": unix_time_millis().saturating_add(15_000), "devices": devices});
            event_engine.publish_to(Audience::identities([identity_id.to_owned()]), "device.identify", payload.clone());
            reply(writer, id, payload)?;
        }
        other => return diverged(other.id()),
    }
    Ok(())
}

#[cfg(feature = "auth")]
pub(super) fn handle_identity_operation(writer: &mut TcpStream, settings: &ConnectionSettings, engine: &Engine, event_engine: &EventEngine, authentication: &ConnectionAuth, operation: RoutedOperation) -> Handled {
    let principal = authentication.principal();
    match operation {
        RoutedOperation::IdentityRegister(Routed { id, input: IdentityRegisterInput { identity_id, public_key, algorithm, encoding, created_at } }) => {
            let created_at = created_at.unwrap_or_else(unix_time_millis);
            let query = format!( "on _identities | insert {{identityId: {}, publicKey: {}, algorithm: {}, encoding: {}, state: \"active\", createdAt: {created_at}}}", query_string(&identity_id), query_string(&public_key), query_string(&algorithm), query_string(&encoding), );
            write_query_then_publish(writer, engine, id, query, Audience::identities([identity_id.clone()]), "identity.created", serde_json::json!({"identityId": identity_id, "createdAt": created_at}))
        }
        RoutedOperation::DeviceList(Routed { id, .. }) => {
            let (identity_id, current_device_id) = authenticated_device(principal, id, "authentication is required")?;
            let documents = query_documents(engine, id, format!("on _devices | where identityId == {} | sort createdAt asc", query_string(&identity_id)))?;
            let devices = documents.into_iter().map(|mut device| {
                if let Some(object) = device.as_object_mut() {
                    let current = object.str_field("deviceId") == Some(current_device_id.as_str());
                    object.insert("current".to_owned(), JsonValue::Bool(current));
                }
                device
            }).collect::<Vec<_>>();
            Ok(reply(writer, id, serde_json::json!({"devices": devices}))?)
        }
        RoutedOperation::DeviceRename(Routed { id, input: DeviceRenameInput { device_id, name } }) => {
            let identity_id = require_identity(principal, id, "authentication is required")?;
            let name = name.trim();
            if name.len() > 80 { reject!(id, "device.invalid_name", "device name must not exceed 80 bytes"); }
            if try_load_device_credential(engine, id, identity_id, &device_id)?.is_none() {
                reject!(id, "device.not_found", "device does not belong to the authenticated identity");
            }
            let updated_at = unix_time_millis();
            query_documents(engine, id, format!("on _devices | where deviceId == {} and identityId == {} | set name = {}, updatedAt = {updated_at}", query_string(&device_id), query_string(identity_id), query_string(name)))?;
            let renamed = serde_json::json!({"deviceId": device_id, "name": name, "updatedAt": updated_at});
            publish_durable_event(engine, Audience::identities([identity_id.to_owned()]), "device.renamed", renamed.clone());
            Ok(reply(writer, id, renamed)?)
        }
        RoutedOperation::DeviceRegister(Routed { id, input: DeviceRegisterInput { device_id, identity_id, public_key, algorithm, encoding, created_at } }) => {
            if principal_identity_id(principal) != Some(identity_id.as_str()) {
                ensure_operation_resource_authorized(settings, engine, principal, id, OperationKind::DeviceRegister, "_devices")?;
            }
            let created_at = created_at.unwrap_or_else(unix_time_millis);
            let query = format!( "on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: {}, encoding: {}, state: \"active\", createdAt: {created_at}}}", query_string(&device_id), query_string(&identity_id), query_string(&public_key), query_string(&algorithm), query_string(&encoding), );
            write_query_then_publish(writer, engine, id, query, Audience::identities([identity_id.clone()]), "device.registered", serde_json::json!({"deviceId": device_id, "identityId": identity_id, "createdAt": created_at}))
        }
        RoutedOperation::DeviceRevoke(Routed { id, input: DeviceRevokeInput { device_id, revoked_at } }) => {
            let self_identity = principal_identity_id(principal);
            if matches!(principal, Principal::Identity { device_id: current, .. } if *current == device_id) {
                reject!(id, "device.current_revoke_forbidden", "the current device cannot be revoked; use identity.renew instead");
            }
            let self_owned = match self_identity {
                Some(identity_id) => try_load_device_credential(engine, id, identity_id, &device_id)?.is_some(),
                None => false,
            };
            if !self_owned {
                ensure_operation_resource_authorized(settings, engine, principal, id, OperationKind::DeviceRevoke, "_devices")?;
            }
            let revoked_at = revoked_at.unwrap_or_else(unix_time_millis);
            // A device revoked by its own identity is addressed within that identity.
            let owner = self_identity.filter(|_| self_owned);
            let identity_filter = owner
                .map(|identity_id| format!(" and identityId == {}", query_string(identity_id)))
                .unwrap_or_default();
            let query = format!( "on _devices | where deviceId == {}{} | set state = \"revoked\", revokedAt = {revoked_at}", query_string(&device_id), identity_filter, );
            let audience = owner
                .map(|identity_id| Audience::identities([identity_id.to_owned()]))
                .unwrap_or(Audience::Global);
            let payload = serde_json::json!({"deviceId": device_id, "revokedAt": revoked_at});
            let response = execute_request(engine, QueryRequest::new(id, query));
            let ok = response.is_ok();
            write_response(writer, &response)?;
            if ok {
                publish_durable_event(engine, audience, "device.revoked", payload.clone());
                event_engine.publish_global("fabric.device.revoked", payload);
            }
            Ok(())
        }
        other => diverged(other.id()),
    }
}
