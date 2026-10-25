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
            super::policy_ops::require_identity_not_inactive(engine, id, &identity_id)?;
            let credential = load_device_credential(engine, id, &identity_id, &device_id)?;
            let challenge = authentication.begin(&credential, DEFAULT_CHALLENGE_TTL).map_err(auth_failed(id))?;
            reply(writer, id, serde_json::to_value(challenge).expect("challenge serializes"))?;
        }
        RoutedOperation::AuthComplete(Routed { id, input: ChallengeSignatureInput { challenge_id, signature } }) => {
            let Some((identity_id, device_id)) = pending_auth_subject(authentication) else {
                reject!(id, "auth.failed", "no authentication challenge is pending");
            };
            super::policy_ops::require_identity_not_inactive(engine, id, &identity_id)?;
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
            // Commercial mode: the token names the policy the identity takes, if it has a free seat.
            let policy_id = match (settings.enrollment_mode.is_policy(), token.as_deref()) {
                (true, Some(token)) => Some(super::policy_ops::enrollment_policy(engine, id, token)?),
                _ => None,
            };
            let challenge = authentication.begin_enrollment(
                identity_id,
                identity_public_key,
                device_id,
                device_public_key,
                DEFAULT_CHALLENGE_TTL,
            ).map_err(auth_failed(id))?;
            if let Some(policy_id) = policy_id { authentication.bind_enrollment_policy(policy_id); }
            reply(writer, id, serde_json::to_value(challenge).expect("challenge serializes"))?;
        }
        RoutedOperation::AuthEnrollComplete(Routed { id, input: ChallengeSignatureInput { challenge_id, signature } }) => {
            let enrollment = authentication.complete_enrollment(&challenge_id, &signature).map_err(auth_failed(id))?;
            if settings.enrollment_mode.is_policy() && enrollment.policy_id.is_none() { reject!(id, "auth.enrollment_denied", "identity enrollment is not allowed"); }
            if let Some(policy_id) = enrollment.policy_id.as_deref() { super::policy_ops::enrollment_seat(engine, id, policy_id)?; }
            let created_at = unix_time_millis();
            let mut identity = serde_json::json!({"identityId": &enrollment.identity_id, "publicKey": &enrollment.identity_public_key, "algorithm": "ed25519", "encoding": "spki-der", "state": "active", "createdAt": created_at});
            if let Some(policy_id) = enrollment.policy_id.as_deref() { identity["policyId"] = policy_id.into(); }
            let identity_query = insert_query("_identities", identity);
            query_documents(engine, id, identity_query)?;
            // Each failed step rolls back the records written before it.
            let rollback_identity = quoted!("on _identities | where identityId == {} | delete", &enrollment.identity_id);
            let device_query = insert_query("_devices", serde_json::json!({"deviceId": &enrollment.device_id, "identityId": &enrollment.identity_id, "publicKey": &enrollment.device_public_key, "algorithm": "ed25519", "encoding": "spki-der", "state": "active", "createdAt": created_at}));
            if let Err(response) = query_documents(engine, id, device_query) {
                let _ = query_run(engine, id, rollback_identity);
                return Err(response.into());
            }

            // Stateless enrollment grants the smallest useful baseline:
            // the new identity may subscribe to its authorized event stream.
            // Broader query/system permissions still require explicit grants.
            let permission_query = enrollment_events_permission_query(&enrollment.identity_id, created_at);
            if let Err(response) = query_documents(engine, id, permission_query) {
                let rollback_device = quoted!("on _devices | where deviceId == {} | delete", &enrollment.device_id);
                let _ = query_run(engine, id, rollback_device);
                let _ = query_run(engine, id, rollback_identity);
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
            if settings.classic_auth_email && !email_address_valid(identifier) { reject!(id, "auth.invalid_identifier", "identifier must be a valid email address"); }
            if password.len() < CLASSIC_AUTH_MIN_PASSWORD_BYTES { reject!(id, "auth.weak_password", "password must contain at least 12 bytes"); }
            let normalized = identifier.to_lowercase();
            let existing = query_documents(engine, id, quoted!("on _credentials | where loginNormalized == {} or identityId == {} | limit 1", &normalized, identity_id))?;
            if !existing.is_empty() { reject!(id, "auth.identifier_unavailable", "identifier or identity is already registered"); }
            let (salt, hash) = classic_password_hash(&password).map_err(failed(id, "auth.hash_failed"))?;
            let now = unix_time_millis();
            query_documents(engine, id, insert_query("_credentials", serde_json::json!({"identityId": identity_id, "login": identifier, "loginNormalized": &normalized, "salt": &salt, "hash": &hash, "algorithm": "argon2id", "memoryKiB": CLASSIC_AUTH_MEMORY_KIB, "iterations": CLASSIC_AUTH_ITERATIONS, "lanes": CLASSIC_AUTH_LANES, "state": "active", "createdAt": now})))?;
            reply(writer, id, serde_json::json!({"registered": true, "identifier": identifier}))?;
        }
        RoutedOperation::AuthClassicLogin(Routed { id, input: ClassicAuthLoginInput { identifier, password, device_id, public_key } }) => {
            if !settings.classic_auth_enabled { reject!(id, "auth.classic_disabled", "classic authentication is disabled"); }
            let normalized = identifier.trim().to_lowercase();
            if normalized.is_empty() || normalized.len() > 200 { reject!(id, "auth.invalid_credentials", "invalid identifier or password"); }
            if let Some(wait_ms) = classic_login_wait(settings, &normalized) { reject!(id, "auth.rate_limited", format!("retry after {wait_ms}ms")); }
            let documents = query_documents(engine, id, quoted!("on _credentials | where loginNormalized == {} and state == \"active\" | limit 1", &normalized))?;
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
            let identities = query_documents(engine, id, quoted!("on _identities | where identityId == {} and state == \"active\" | limit 1", &identity_id))?;
            if identities.is_empty() { reject!(id, "auth.invalid_credentials", "invalid identifier or password"); }
            if let Some(existing) = try_load_device_credential(engine, id, &identity_id, &device_id)? {
                if !existing.active || existing.public_key != public_key { reject!(id, "auth.device_mismatch", "device identifier is already registered with different credentials"); }
            } else {
                let collision = query_documents(engine, id, quoted!("on _devices | where deviceId == {} | limit 1", &device_id))?;
                if !collision.is_empty() { reject!(id, "auth.device_mismatch", "device identifier is already registered"); }
                super::policy_ops::check_new_device(engine, id, &identity_id)?;
                let now = unix_time_millis();
                query_documents(engine, id, insert_query("_devices", serde_json::json!({"deviceId": &device_id, "identityId": &identity_id, "publicKey": &public_key, "algorithm": "ed25519", "encoding": "spki-der", "state": "active", "createdAt": now})))?;
            }
            reply(writer, id, serde_json::json!({"accepted": true, "identityId": identity_id, "deviceId": device_id}))?;
        }
        RoutedOperation::AuthClassicResetRequest(Routed { id, input: ClassicAuthResetRequestInput { identifier, language } }) => {
            if !settings.password_reset_available() { reject!(id, "auth.reset_unavailable", "password reset is not available on this Core"); }
            let normalized = identifier.trim().to_lowercase();
            if !email_address_valid(&normalized) { reject!(id, "auth.invalid_identifier", "identifier must be a valid email address"); }
            // The answer never tells whether the address is registered; one mail a minute per address at most.
            let throttle = format!("reset-mail:{normalized}");
            if classic_login_wait(settings, &throttle).is_none() {
                classic_throttle(settings, &throttle, CLASSIC_RESET_MAIL_INTERVAL_MS);
                let documents = query_documents(engine, id, quoted!("on _credentials | where loginNormalized == {} and state == \"active\" | limit 1", &normalized))?;
                let credential = documents.first().and_then(|document| Some((document.str_field("identityId")?.to_owned(), document.str_field("login")?.to_owned())));
                if let Some((identity_id, login)) = credential {
                    let code = classic_reset_code().map_err(failed(id, "auth.reset_failed"))?;
                    let (salt, hash) = classic_password_hash(&code.replace('-', "")).map_err(failed(id, "auth.hash_failed"))?;
                    let now = unix_time_millis();
                    query_documents(engine, id, quoted!("on _credential_resets | where loginNormalized == {} | delete", &normalized))?;
                    query_documents(engine, id, insert_query("_credential_resets", serde_json::json!({"loginNormalized": &normalized, "identityId": identity_id, "salt": salt, "hash": hash, "attempts": 0, "expiresAt": now.saturating_add(CLASSIC_RESET_TTL_MS), "createdAt": now})))?;
                    settings.send_mail_detached(classic_reset_mail(&login, &code, language.as_deref()));
                }
            }
            reply(writer, id, serde_json::json!({"requested": true}))?;
        }
        RoutedOperation::AuthClassicResetComplete(Routed { id, input: ClassicAuthResetCompleteInput { identifier, code, password } }) => {
            if !settings.password_reset_available() { reject!(id, "auth.reset_unavailable", "password reset is not available on this Core"); }
            let normalized = identifier.trim().to_lowercase();
            let throttle = format!("reset-code:{normalized}");
            if let Some(wait_ms) = classic_login_wait(settings, &throttle) { reject!(id, "auth.rate_limited", format!("retry after {wait_ms}ms")); }
            if password.len() < CLASSIC_AUTH_MIN_PASSWORD_BYTES { reject!(id, "auth.weak_password", "password must contain at least 12 bytes"); }
            let code: String = code.chars().filter(char::is_ascii_alphanumeric).map(|c| c.to_ascii_uppercase()).collect();
            let resets = query_documents(engine, id, quoted!("on _credential_resets | where loginNormalized == {} | limit 1", &normalized))?;
            let reset = resets.first();
            let attempts = reset.and_then(|document| document.get("attempts")).and_then(JsonValue::as_u64).unwrap_or(0);
            let live = reset.and_then(|document| document.get("expiresAt")).and_then(JsonValue::as_u64).is_some_and(|expires_at| expires_at > unix_time_millis()) && attempts < CLASSIC_RESET_MAX_ATTEMPTS;
            let valid = live && reset.and_then(|document| Some((document.str_field("salt")?, document.str_field("hash")?))).is_some_and(|(salt, hash)| classic_password_verify(&code, salt, hash));
            let identity_id = reset.and_then(|document| document.str_field("identityId")).unwrap_or_default().to_owned();
            if !valid || identity_id.is_empty() {
                if reset.is_some() { query_documents(engine, id, format!("on _credential_resets | where loginNormalized == {} | set attempts = {}", query_string(&normalized), attempts + 1))?; }
                classic_login_failure(settings, &throttle);
                reject!(id, "auth.reset_invalid", "invalid or expired code");
            }
            let (salt, hash) = classic_password_hash(&password).map_err(failed(id, "auth.hash_failed"))?;
            query_documents(engine, id, format!("on _credentials | where loginNormalized == {} and identityId == {} | set salt = {}, hash = {}, updatedAt = {}", query_string(&normalized), query_string(&identity_id), query_string(&salt), query_string(&hash), unix_time_millis()))?;
            query_documents(engine, id, quoted!("on _credential_resets | where loginNormalized == {} | delete", &normalized))?;
            classic_login_success(settings, &throttle);
            classic_login_success(settings, &normalized);
            reply(writer, id, serde_json::json!({"reset": true}))?;
        }
        RoutedOperation::IdentityOpen(Routed { id, input: IdentityOpenInput { file, password, client_device_id } }) => {
            let Ok(bytes) = decode_base64(&file) else { reject!(id, "identity.invalid_file", "identity file must be base64 encoded") };
            let portable = identity_file::decrypt_bytes(&bytes, password.as_bytes()).map_err(failed(id, "identity.open_failed"))?;
            super::policy_ops::require_identity_not_inactive(engine, id, &portable.identity_id)?;
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
                super::policy_ops::check_new_device(engine, id, &portable.identity_id)?;
                let now = unix_time_millis();
                query_documents(engine, id, insert_query("_devices", serde_json::json!({"deviceId": &target_device_id, "identityId": &portable.identity_id, "publicKey": &portable.public_key, "algorithm": "ed25519", "encoding": "spki-der", "state": "active", "sourceDeviceId": &source_device_id, "createdAt": now})))?;
                (DeviceCredential { identity_id: portable.identity_id.clone(), device_id: target_device_id.clone(), public_key: portable.public_key.clone(), algorithm: "ed25519".to_owned(), encoding: "spki-der".to_owned(), active: true }, true)
            };
            let principal = authentication.establish(&device).map_err(auth_failed(id))?;
            reply(writer, id, serde_json::json!({"authenticated": true, "principal": principal, "deviceId": device.device_id, "sourceDeviceId": source_device_id, "enrolled": enrolled}))?;
        }
        RoutedOperation::IdentityGet(Routed { id, input: PasswordInput { password } }) => {
            let identity_id = require_identity(authentication.principal(), id, "an authenticated identity is required")?;
            super::policy_ops::check_new_device(engine, id, identity_id)?;
            let portable = IdentityCredential::renew(identity_id).map_err(failed(id, "identity.generate_failed"))?;
            let bytes = identity_file::encrypt_bytes(&portable, password.as_bytes()).map_err(failed(id, "identity.encrypt_failed"))?;
            let now = unix_time_millis();
            query_documents(engine, id, insert_query("_devices", serde_json::json!({"deviceId": &portable.device_id, "identityId": identity_id, "publicKey": &portable.public_key, "algorithm": "ed25519", "encoding": "spki-der", "state": "active", "createdAt": now})))?;
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
                insert_query("_devices", serde_json::json!({"deviceId": &device_id, "identityId": &identity_id, "publicKey": &public_key, "algorithm": "ed25519", "encoding": "spki-der", "state": "active", "createdAt": now})),
                quoted!("on _identities | where identityId == {} | set publicKey = {}, algorithm = \"ed25519\", encoding = \"spki-der\", updatedAt = {now}", &identity_id, &public_key),
                quoted!("on _devices | where deviceId == {} and identityId == {} | set state = \"revoked\", revokedAt = {now}", &current_device_id, &identity_id),
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
        RoutedOperation::AuthMachine(Routed { id, input: AuthMachineInput { token } }) => {
            // A connection takes one principal: a machine token never replaces a signed-in identity.
            if !matches!(authentication.principal(), Principal::Anonymous) { reject!(id, "auth.already_authenticated", "this connection is already authenticated"); }
            if !token.starts_with(MACHINE_TOKEN_PREFIX) || token.len() > 200 { reject!(id, "auth.invalid_credentials", "invalid machine access token"); }
            let access = query_first(engine, id, quoted!("on _machine_access | where tokenHash == {} and state == \"active\" | limit 1", &machine_token_hash(&token)))?;
            let Some(access) = access else { reject!(id, "auth.invalid_credentials", "invalid machine access token") };
            let (identity_id, access_id) = (access.string_field("identityId"), access.string_field("accessId"));
            if identity_id.is_empty() || access_id.is_empty() { reject!(id, "auth.invalid_credentials", "invalid machine access token"); }
            // The access acts for its identity: an identity no longer active takes its accesses with it.
            if query_documents(engine, id, quoted!("on _identities | where identityId == {} and state == \"active\" | limit 1", &identity_id))?.is_empty() {
                reject!(id, "auth.invalid_credentials", "invalid machine access token");
            }
            let principal = authentication.establish_machine(identity_id, &access_id);
            debug::log( DebugTopic::Auth, Some(connection_id), format!("machine access authenticated principal={principal:?}"), );
            let now = unix_time_millis();
            if access.u64_field("lastUsedAt").is_none_or(|last| now.saturating_sub(last) >= MACHINE_LAST_USED_STEP_MS) {
                let _ = query_run(engine, id, quoted!("on _machine_access | where accessId == {} | set lastUsedAt = {now}", &access_id));
            }
            reply(writer, id, serde_json::json!({"authenticated": true, "principal": principal, "access": machine_access_json(&access)}))?;
        }
        RoutedOperation::DeviceIdentify(Routed { id, .. }) => {
            let identity_id = require_identity(authentication.principal(), id, "authentication is required")?;
            let documents = query_documents(engine, id, quoted!("on _devices | where identityId == {} and state == \"active\" | sort createdAt asc", identity_id))?;
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
        operation @ (RoutedOperation::PoolCreate(_) | RoutedOperation::PoolUpdate(_) | RoutedOperation::PoolList(_) | RoutedOperation::PoolDelete(_)
            | RoutedOperation::PolicyCreate(_) | RoutedOperation::PolicyUpdate(_) | RoutedOperation::PolicyList(_) | RoutedOperation::PolicyDelete(_)
            | RoutedOperation::PolicyTokenCreate(_) | RoutedOperation::PolicyTokenList(_) | RoutedOperation::PolicyTokenRevoke(_) | RoutedOperation::IdentityPolicySet(_) | RoutedOperation::IdentityStateSet(_) | RoutedOperation::IdentityList(_)) => {
            // A limit, a policy or a seat may have changed: the storage headroom is computed again.
            let changes = matches!(operation, RoutedOperation::PoolUpdate(_) | RoutedOperation::PolicyUpdate(_) | RoutedOperation::IdentityPolicySet(_) | RoutedOperation::IdentityStateSet(_));
            let handled = super::policy_ops::handle_policy_operation(writer, engine, principal, operation);
            if changes && handled.is_ok() { super::usage_ops::refresh(); }
            handled
        }
        RoutedOperation::UsageGet(routed) => super::usage_ops::handle_usage_get(writer, settings, engine, principal, routed),
        RoutedOperation::IdentityRegister(Routed { id, input: IdentityRegisterInput { identity_id, public_key, algorithm, encoding, created_at } }) => {
            // In commercial mode every identity comes from a policy token (auth.enroll.*).
            if settings.enrollment_mode.is_policy() { reject!(id, "identity.register_disabled", "in policy enrollment mode, identities enroll with a policy token"); }
            let created_at = created_at.unwrap_or_else(unix_time_millis);
            let query = insert_query("_identities", serde_json::json!({"identityId": &identity_id, "publicKey": &public_key, "algorithm": &algorithm, "encoding": &encoding, "state": "active", "createdAt": created_at}));
            write_query_then_publish(writer, engine, id, query, Audience::identities([identity_id.clone()]), "identity.created", serde_json::json!({"identityId": identity_id, "createdAt": created_at}))
        }
        RoutedOperation::DeviceList(Routed { id, .. }) => {
            let (identity_id, current_device_id) = authenticated_device(principal, id, "authentication is required")?;
            let documents = query_documents(engine, id, quoted!("on _devices | where identityId == {} | sort createdAt asc", &identity_id))?;
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
            query_documents(engine, id, quoted!("on _devices | where deviceId == {} and identityId == {} | set name = {}, updatedAt = {updated_at}", &device_id, identity_id, name))?;
            let renamed = serde_json::json!({"deviceId": device_id, "name": name, "updatedAt": updated_at});
            publish_durable_event(engine, Audience::identities([identity_id.to_owned()]), "device.renamed", renamed.clone());
            Ok(reply(writer, id, renamed)?)
        }
        RoutedOperation::DeviceRegister(Routed { id, input: DeviceRegisterInput { device_id, identity_id, public_key, algorithm, encoding, created_at } }) => {
            if principal_identity_id(principal) != Some(identity_id.as_str()) {
                ensure_operation_resource_authorized(settings, engine, principal, id, OperationKind::DeviceRegister, "_devices")?;
            }
            super::policy_ops::check_new_device(engine, id, &identity_id)?;
            let created_at = created_at.unwrap_or_else(unix_time_millis);
            let query = insert_query("_devices", serde_json::json!({"deviceId": &device_id, "identityId": &identity_id, "publicKey": &public_key, "algorithm": &algorithm, "encoding": &encoding, "state": "active", "createdAt": created_at}));
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
        RoutedOperation::MachineAccessCreate(Routed { id, input: MachineAccessCreateInput { place_id, instance_id, name, mode } }) => {
            let identity_id = require_signed_in_identity(principal, id)?;
            let name = name.trim();
            if name.is_empty() || name.len() > 80 { reject!(id, "machine.invalid_name", "an access name holds 1 to 80 bytes"); }
            let mode = mode.as_deref().unwrap_or(MACHINE_ACCESS_MODES[0]);
            if !MACHINE_ACCESS_MODES.contains(&mode) { reject!(id, "machine.invalid_mode", "machine access modes are deposit and full"); }
            // The access is created by someone who may write in this App instance of the Place.
            let context = resolve_query_execution_context(engine, id, principal, RequestedExecutionContext { place_id: place_id.clone(), app_instance_id: Some(instance_id.clone()), as_profile: None })?;
            if !context.access.can_write() { reject!(id, "authorization.denied", "a machine access needs write access to the Place"); }
            let token = machine_token().map_err(failed(id, "machine.token_failed"))?;
            let record = serde_json::json!({
                "accessId": UuidV7Generator::new().next_id().to_string(), "identityId": identity_id, "placeId": place_id, "instanceId": instance_id,
                "name": name, "mode": mode, "tokenHash": machine_token_hash(&token), "tokenHint": &token[..MACHINE_TOKEN_PREFIX.len() + 6],
                "state": "active", "createdAt": unix_time_millis(),
            });
            query_documents(engine, id, insert_query("_machine_access", record.clone()))?;
            // The token is shown once: only its hash is kept.
            Ok(reply(writer, id, serde_json::json!({"access": machine_access_json(&record), "token": token}))?)
        }
        RoutedOperation::MachineAccessList(Routed { id, input: MachineAccessListInput { place_id, instance_id } }) => {
            let identity_id = require_signed_in_identity(principal, id)?;
            let mut query = quoted!("on _machine_access | where identityId == {} and state == \"active\"", identity_id);
            if let Some(place_id) = place_id { query.push_str(&format!(" and placeId == {}", query_string(&place_id))); }
            if let Some(instance_id) = instance_id { query.push_str(&format!(" and instanceId == {}", query_string(&instance_id))); }
            let accesses = query_documents(engine, id, format!("{query} | sort createdAt desc"))?.iter().map(machine_access_json).collect::<Vec<_>>();
            Ok(reply(writer, id, serde_json::json!({"accesses": accesses}))?)
        }
        RoutedOperation::MachineAccessRevoke(Routed { id, input: MachineAccessRevokeInput { access_id } }) => {
            let identity_id = require_signed_in_identity(principal, id)?;
            let selected = quoted!("on _machine_access | where accessId == {} and identityId == {} and state == \"active\"", &access_id, identity_id);
            if query_documents(engine, id, format!("{selected} | limit 1"))?.is_empty() { reject!(id, "machine.access_not_found", "no active machine access of yours has this accessId"); }
            let revoked_at = unix_time_millis();
            query_documents(engine, id, format!("{selected} | set state = \"revoked\", revokedAt = {revoked_at}"))?;
            Ok(reply(writer, id, serde_json::json!({"accessId": access_id, "state": "revoked", "revokedAt": revoked_at}))?)
        }
        RoutedOperation::MachineAccessReport(Routed { id, input: MachineAccessReportInput { file_name, status, rows, table, message } }) => {
            // Only the machine itself reports, on its own access: its device is `machine:<accessId>`.
            let (identity_id, access_id) = match principal {
                Principal::Identity { identity_id, device_id } => match device_id.strip_prefix("machine:") {
                    Some(access_id) => (identity_id.as_str(), access_id),
                    None => reject!(id, "authorization.denied", "only a machine access reports its imports"),
                },
                _ => reject!(id, "authorization.denied", "only a machine access reports its imports"),
            };
            if !MACHINE_REPORT_STATUSES.contains(&status.as_str()) { reject!(id, "machine.report_status_invalid", "status is completed, skipped or failed"); }
            let clip = |value: &str, max: usize| value.char_indices().take_while(|(at, _)| *at < max).map(|(_, character)| character).collect::<String>();
            let report = serde_json::json!({"fileName": clip(&file_name, 255), "status": status, "rows": rows, "table": table.as_deref().map(|value| clip(value, 120)), "message": message.as_deref().map(|value| clip(value, 500)), "at": unix_time_millis()});
            let selected = quoted!("on _machine_access | where accessId == {} and identityId == {} and state == \"active\"", access_id, identity_id);
            if query_documents(engine, id, format!("{selected} | limit 1"))?.is_empty() { reject!(id, "machine.access_not_found", "this machine access is no longer active"); }
            let json = serde_json::to_string(&report).expect("machine report serializes");
            query_documents(engine, id, format!("{selected} | set lastImport = {json}"))?;
            Ok(reply(writer, id, serde_json::json!({"accessId": access_id, "lastImport": report}))?)
        }
        other => diverged(other.id()),
    }
}

/// Machine access tokens: `ogm_` and 64 hex digits (256 random bits), kept as their SHA-256.
#[cfg(feature = "auth")]
const MACHINE_TOKEN_PREFIX: &str = "ogm_";
/// `deposit` lists and adds files; `full` also reads, moves and deletes them. The Gateway applies the mode.
#[cfg(feature = "auth")]
const MACHINE_ACCESS_MODES: [&str; 2] = ["deposit", "full"];
/// `lastUsedAt` moves by steps, so a busy connector does not write on every request.
#[cfg(feature = "auth")]
const MACHINE_LAST_USED_STEP_MS: u64 = 60_000;
/// What the Gateway's automatic import of a deposited file came to.
#[cfg(feature = "auth")]
const MACHINE_REPORT_STATUSES: [&str; 3] = ["completed", "skipped", "failed"];

#[cfg(feature = "auth")]
fn machine_token() -> Result<String, String> {
    let mut random = [0u8; 32];
    getrandom::fill(&mut random).map_err(|error| error.to_string())?;
    Ok(format!("{MACHINE_TOKEN_PREFIX}{}", crate::helpers::hex(&random)))
}

#[cfg(feature = "auth")]
fn machine_token_hash(token: &str) -> String {
    crate::helpers::hex(&Sha256::digest(token.as_bytes()))
}

/// A machine access as its owner sees it: never its token nor the token's hash.
#[cfg(feature = "auth")]
fn machine_access_json(record: &JsonValue) -> JsonValue {
    let mut access = serde_json::Map::new();
    for field in ["accessId", "placeId", "instanceId", "name", "mode", "tokenHint", "state", "createdAt", "lastUsedAt", "lastImport"] {
        if let Some(value) = record.get(field) { access.insert(field.to_owned(), value.clone()); }
    }
    JsonValue::Object(access)
}

/// Machine accesses are managed by a person signed in on a device, never by another machine access.
#[cfg(feature = "auth")]
fn require_signed_in_identity(principal: &Principal, id: RequestId) -> Result<&str, super::serve::Stop> {
    if matches!(principal, Principal::Identity { device_id, .. } if device_id.starts_with("machine:")) {
        reject!(id, "authorization.denied", "a machine access cannot manage machine accesses");
    }
    require_identity(principal, id, "authentication is required")
}
