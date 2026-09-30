//! Gateway fabric: node registration, authenticated channels and upstream relays.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;

pub(super) fn normalize_gateway_node_endpoint(value: &str) -> Option<String> {
    fn from_authority(authority: &str, secure: bool) -> Option<String> {
        let authority = authority.trim().trim_end_matches('/');
        if authority.is_empty() { return None; }
        let scheme = if secure { "wss" } else { "ws" };
        Some(format!("{scheme}://{authority}/node"))
    }
    let raw = value.trim();
    if let Some(authority) = raw.strip_prefix("gw+insecure://") {
        return from_authority(authority, false);
    }
    if let Some(authority) = raw.strip_prefix("gw://") {
        return from_authority(authority, true);
    }
    if raw.starts_with("ws://") || raw.starts_with("wss://") {
        let base = raw.trim_end_matches('/');
        if base.ends_with("/node") {
            return Some(base.to_owned());
        }
        if base.ends_with("/peer") || base.ends_with("/ws") {
            let base = base.rsplit_once('/').map(|(head, _)| head).unwrap_or(base);
            return Some(format!("{base}/node"));
        }
        return Some(format!("{base}/node"));
    }
    None
}

pub(super) fn gateway_client_endpoint_from_node(endpoint: &str) -> Option<String> {
    let endpoint = endpoint.trim();
    endpoint.strip_suffix("/node").map(|base| format!("{base}/ws"))
}

pub(super) fn gateway_endpoint(bind: &str) -> Option<String> {
    fn normalize(authority: &str, secure: bool) -> Option<String> {
        let authority = authority.trim().trim_end_matches('/');
        if authority.is_empty() { return None; }
        let scheme = if secure { "wss" } else { "ws" };
        Some(format!("{scheme}://{authority}/node"))
    }
    if let Some(raw) = bind.strip_prefix("gw+insecure://") { return normalize(raw, false); }
    bind.strip_prefix("gw://").and_then(|raw| normalize(raw, true))
}


#[cfg(feature = "fabric")]
pub(super) type NodeWebSocket = WebSocket<MaybeTlsStream<TcpStream>>;

#[cfg(not(feature = "fabric"))]
#[derive(Debug)]
pub(super) struct NodeWebSocket;

#[cfg(feature = "fabric")]
pub(super) fn close_node_websocket(websocket: &mut NodeWebSocket) {
    let _ = websocket.close(None);
}

#[cfg(not(feature = "fabric"))]
pub(super) fn close_node_websocket(_websocket: &mut NodeWebSocket) {}

#[cfg(feature = "fabric")]
pub(super) fn write_node_message(websocket: &mut NodeWebSocket, value: &JsonValue) -> Result<(), String> {
    let payload = rmp_serde::to_vec_named(value).map_err(|error| error.to_string())?;
    websocket.send(WebSocketMessage::Binary(payload.into())).map_err(|error| error.to_string())
}

#[cfg(feature = "fabric")]
pub(super) fn read_node_message(websocket: &mut NodeWebSocket) -> Result<Option<JsonValue>, String> {
    loop {
        match websocket.read() {
            Ok(WebSocketMessage::Binary(payload)) => { return rmp_serde::from_slice::<JsonValue>(&payload).map(Some).map_err(|error| error.to_string()); }
            Ok(WebSocketMessage::Close(_)) => return Ok(None),
            Ok(WebSocketMessage::Ping(payload)) => { websocket.send(WebSocketMessage::Pong(payload)).map_err(|error| error.to_string())?; }
            Ok(WebSocketMessage::Pong(_)) => {}
            Ok(WebSocketMessage::Text(_)) => return Err("node control channel requires binary MessagePack frames".to_owned()),
            Ok(_) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
}

#[cfg(feature = "fabric")]
pub(super) enum NodeMessagePoll {
    Message(JsonValue),
    Closed,
    Idle,
}

#[cfg(feature = "fabric")]
pub(super) fn poll_node_message(websocket: &mut NodeWebSocket) -> Result<NodeMessagePoll, String> {
    loop {
        match websocket.read() {
            Ok(WebSocketMessage::Binary(payload)) => { let message = rmp_serde::from_slice::<JsonValue>(&payload).map_err(|error| error.to_string())?; return Ok(NodeMessagePoll::Message(message)); }
            Ok(WebSocketMessage::Close(_)) => return Ok(NodeMessagePoll::Closed),
            Ok(WebSocketMessage::Ping(payload)) => { websocket.send(WebSocketMessage::Pong(payload)).map_err(|error| error.to_string())?; }
            Ok(WebSocketMessage::Pong(_)) => {}
            Ok(WebSocketMessage::Text(_)) => return Err("node control channel requires binary MessagePack frames".to_owned()),
            Ok(_) => {}
            Err(tungstenite::Error::Io(error)) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => { return Ok(NodeMessagePoll::Idle); }
            Err(error) => return Err(error.to_string()),
        }
    }
}

#[cfg(feature = "fabric")]
pub(super) fn write_gateway_message(websocket: &mut NodeWebSocket, value: &JsonValue) -> Result<(), String> {
    let payload = serde_json::to_string(value).map_err(|error| error.to_string())?;
    websocket.send(WebSocketMessage::Text(payload.into())).map_err(|error| error.to_string())
}

#[cfg(feature = "fabric")]
pub(super) fn read_gateway_message(websocket: &mut NodeWebSocket) -> Result<Option<JsonValue>, String> {
    loop {
        match websocket.read() {
            Ok(WebSocketMessage::Text(payload)) => { return serde_json::from_str::<JsonValue>(&payload).map(Some).map_err(|error| error.to_string()); }
            Ok(WebSocketMessage::Close(_)) => return Ok(None),
            Ok(WebSocketMessage::Ping(payload)) => { websocket.send(WebSocketMessage::Pong(payload)).map_err(|error| error.to_string())?; }
            Ok(WebSocketMessage::Pong(_)) => {}
            Ok(WebSocketMessage::Binary(_)) => return Err("gateway client channel requires JSON text frames".to_owned()),
            Ok(_) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
}

#[cfg(feature = "fabric")]
pub(super) fn gateway_response_for_request(websocket: &mut NodeWebSocket, request: &OperationRequest) -> Result<JsonValue, String> {
    let request_id = serde_json::to_value(request.id).map_err(|error| error.to_string())?;
    let value = serde_json::to_value(request).map_err(|error| error.to_string())?;
    write_gateway_message(websocket, &value)?;
    loop {
        let Some(message) = read_gateway_message(websocket)? else {
            return Err("gateway client channel closed before the response".to_owned());
        };
        if message.str_field("kind") == Some("response")
            && message.get("id") == Some(&request_id)
        {
            return Ok(message);
        }
    }
}

#[cfg(feature = "fabric")]
pub(super) fn connect_authenticated_gateway_client( endpoint: &str, credential: &IdentityCredential, read_timeout: Duration, write_timeout: Duration, ) -> Result<NodeWebSocket, String> {
    let (mut websocket, _) = websocket_connect(endpoint).map_err(|error| error.to_string())?;
    configure_websocket_timeouts(&mut websocket, read_timeout, write_timeout)
        .map_err(|error| format!("cannot configure Gateway client timeouts: {error}"))?;

    // The Gateway attaches a dedicated Core connection before it emits gateway.ready.
    // Waiting for that event keeps the node-side client independent from Gateway startup races.
    loop {
        let Some(message) = read_gateway_message(&mut websocket)? else {
            return Err("gateway client channel closed before gateway.ready".to_owned());
        };
        if message.str_field("kind") == Some("event")
            && message.str_field("type") == Some("gateway.ready")
        {
            break;
        }
    }

    let begin = OperationRequest::new(
        RequestId::Number(1),
        "auth.begin",
        serde_json::json!({
            "identityId": credential.identity_id,
            "deviceId": credential.device_id,
        }),
    );
    let begin_response = gateway_response_for_request(&mut websocket, &begin)?;
    if let Some(error) = begin_response.get("error") {
        return Err(format!("upstream auth.begin rejected: {error}"));
    }
    let challenge_id = begin_response.get("data").and_then(|value| value.get("challengeId")).and_then(JsonValue::as_str)
        .ok_or_else(|| "upstream auth.begin did not return challengeId".to_owned())?;
    let challenge = begin_response.get("data").and_then(|value| value.get("challenge")).and_then(JsonValue::as_str)
        .ok_or_else(|| "upstream auth.begin did not return challenge".to_owned())?;
    let challenge = decode_base64(challenge).map_err(|error| error.to_string())?;
    let complete = OperationRequest::new(
        RequestId::Number(2),
        "auth.complete",
        serde_json::json!({
            "challengeId": challenge_id,
            "signature": credential.sign_base64(&challenge),
        }),
    );
    let complete_response = gateway_response_for_request(&mut websocket, &complete)?;
    if let Some(error) = complete_response.get("error") {
        return Err(format!("upstream auth.complete rejected: {error}"));
    }
    if complete_response.str_field("status") != Some("ok") {
        return Err("upstream authentication did not complete successfully".to_owned());
    }

    Ok(websocket)
}

pub(super) fn gateway_client_endpoints(settings: &ConnectionSettings) -> Vec<String> {
    lock_unpoisoned(&settings.upstream_client_endpoints)
        .clone()
}

pub(super) fn has_upstream_gateway(settings: &ConnectionSettings) -> bool {
    !gateway_client_endpoints(settings).is_empty()
}

#[cfg(feature = "fabric")]
pub(super) fn connect_authenticated_gateway_client_any( settings: &ConnectionSettings, credential: &IdentityCredential, read_timeout: Duration, write_timeout: Duration, ) -> Result<(NodeWebSocket, String), String> {
    let endpoints = gateway_client_endpoints(settings);
    if endpoints.is_empty() {
        return Err("no upstream Gateway client endpoint".to_owned());
    }
    let mut errors = Vec::new();
    for endpoint in endpoints {
        match connect_authenticated_gateway_client(&endpoint, credential, read_timeout, write_timeout) {
            Ok(websocket) => return Ok((websocket, endpoint)),
            Err(error) => errors.push(format!("{endpoint}: {error}")),
        }
    }
    Err(format!("all upstream Gateways failed: {}", errors.join("; ")))
}

#[cfg(not(feature = "fabric"))]
pub(super) fn connect_authenticated_gateway_client_any( _settings: &ConnectionSettings, _credential: &IdentityCredential, _read_timeout: Duration, _write_timeout: Duration, ) -> Result<(NodeWebSocket, String), String> {
    Err("ogd was built without the `fabric` feature".to_owned())
}

pub(super) fn write_json_wire_response(writer: &mut TcpStream, response: &JsonValue) -> Result<(), ConnectionError> {
    let encoded = encode_message(response, MessageKind::Response, MAX_RESPONSE_BYTES)
        .map_err(ConnectionError::Encode)?;
    writer.write_all(&encoded).map_err(ConnectionError::Write)
}

pub(super) fn forward_authority_operation( writer: &mut TcpStream, settings: &ConnectionSettings, principal: &Principal, request: &OperationRequest, ) -> Result<bool, ConnectionError> {
    if request.op.is_empty() || !has_upstream_gateway(settings) {
        return Ok(false);
    }
    let Some(credential) = settings.upstream_identity.as_ref() else {
        return Ok(false);
    };
    let Principal::Identity { identity_id, device_id } = principal else {
        return Ok(false);
    };

    // Never turn a public/anonymous local request into the node Identity upstream.
    // Authority forwarding is allowed only for the same authenticated Device that
    // owns the node credential.
    if identity_id != &credential.identity_id || device_id != &credential.device_id {
        return Ok(false);
    }

    match connect_authenticated_gateway_client_any(
        settings,
        credential,
        settings.read_timeout,
        settings.write_timeout,
    )
    .and_then(|(mut websocket, endpoint)| {
        debug::log(DebugTopic::Gateway, None, format!("forward authority op={} endpoint={endpoint}", request.op));
        let response = gateway_response_for_request(&mut websocket, request);
        close_node_websocket(&mut websocket);
        response
    }) {
        Ok(response) => {
            write_json_wire_response(writer, &response)?;
            Ok(true)
        }
        Err(error) => {
            let response = QueryResponse::request_error(
                request.id,
                "gateway.upstream_failed",
                format!("unable to execute {} on upstream authority: {error}", request.op),
            );
            write_response(writer, &response)?;
            Ok(true)
        }
    }
}

#[cfg(feature = "fabric")]
pub(super) fn configure_websocket_timeouts( websocket: &mut NodeWebSocket, read_timeout: Duration, write_timeout: Duration, ) -> io::Result<()> {
    match websocket.get_mut() {
        MaybeTlsStream::Plain(stream) => {
            stream.set_read_timeout(Some(read_timeout))?;
            stream.set_write_timeout(Some(write_timeout))
        }
        MaybeTlsStream::Rustls(stream) => {
            let stream = stream.get_mut();
            stream.set_read_timeout(Some(read_timeout))?;
            stream.set_write_timeout(Some(write_timeout))
        }
        _ => Ok(()),
    }
}

#[cfg(feature = "fabric")]
pub(super) fn configure_websocket_polling(websocket: &mut NodeWebSocket) -> io::Result<()> {
    let timeout = Some(Duration::from_millis(25));
    match websocket.get_mut() {
        MaybeTlsStream::Plain(stream) => stream.set_read_timeout(timeout),
        MaybeTlsStream::Rustls(stream) => stream.get_mut().set_read_timeout(timeout),
        _ => Ok(()),
    }
}

#[cfg(feature = "fabric")]
pub(super) fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _) = listener.accept()?;
    Ok((server, client))
}

#[cfg(feature = "fabric")]
pub(super) fn bridge_websocket_channel(mut websocket: NodeWebSocket, mut local: TcpStream) -> Result<(), String> {
    local.set_nonblocking(true).map_err(|error| error.to_string())?;
    configure_websocket_polling(&mut websocket).map_err(|error| error.to_string())?;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        loop {
            match local.read(&mut buffer) {
                Ok(0) => { close_node_websocket(&mut websocket); return Ok(()); }
                Ok(bytes) => websocket.send(WebSocketMessage::Binary(buffer[..bytes].to_vec().into())).map_err(|error| error.to_string())?,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error.to_string()),
            }
        }

        match websocket.read() {
            Ok(WebSocketMessage::Binary(payload)) => local.write_all(&payload).map_err(|error| error.to_string())?,
            Ok(WebSocketMessage::Close(_)) => return Ok(()),
            Ok(WebSocketMessage::Ping(payload)) => websocket.send(WebSocketMessage::Pong(payload)).map_err(|error| error.to_string())?,
            Ok(WebSocketMessage::Pong(_)) => {}
            Ok(WebSocketMessage::Text(_)) => return Err("node data channel requires binary frames".to_owned()),
            Ok(_) => {}
            Err(tungstenite::Error::Io(error)) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {}
            Err(error) => return Err(error.to_string()),
        }
        thread::sleep(Duration::from_millis(1));
    }
}

pub(super) fn node_identity_proof(challenge: &str, node_id: &str, identity_id: &str, device_id: &str) -> String {
    format!("og.node.challenge.v1\n{challenge}\n{node_id}\n{identity_id}\n{device_id}")
}

pub(super) fn registered_device_matches(engine: &Engine, identity_id: &str, device_id: &str, public_key: &str) -> bool {
    if identity_id.is_empty() || device_id.is_empty() || public_key.is_empty() { return false; }
    let query = format!(
        "on _devices | where identityId == {} and deviceId == {} and publicKey == {} and state == \"active\" | limit 1",
        query_string(identity_id),
        query_string(device_id),
        query_string(public_key),
    );
    query_has_rows(engine, 0, query)
}

pub(super) fn registered_identity_matches(engine: &Engine, identity_id: &str, public_key: &str) -> bool {
    if identity_id.is_empty() || public_key.is_empty() { return false; }
    let query = format!(
        "on _identities | where identityId == {} and publicKey == {} and state == \"active\" | limit 1",
        query_string(identity_id),
        query_string(public_key),
    );
    query_has_rows(engine, 0, query)
}

#[cfg(feature = "fabric")]
pub(super) fn gateway_random_challenge() -> Result<String, String> {
    let mut bytes = [0u8; 32];
    fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|error| format!("unable to generate Gateway challenge: {error}"))?;
    Ok(encode_base64(&bytes))
}

#[cfg(feature = "fabric")]
pub(super) fn gateway_identity_proof(challenge: &str, identity_id: &str, device_id: &str) -> String {
    format!("og.gateway.challenge.v1\n{challenge}\n{identity_id}\n{device_id}")
}

#[cfg(feature = "fabric")]
pub(super) fn gateway_enrollment_payload(challenge: &str, public_key: &str) -> String {
    format!("og.gateway.enroll.v1\n{challenge}\n{public_key}")
}

#[cfg(feature = "fabric")]
pub(super) fn hmac_sha256_base64(key: &[u8], message: &[u8]) -> String {
    const BLOCK_BYTES: usize = 64;
    let mut normalized = [0u8; BLOCK_BYTES];
    if key.len() > BLOCK_BYTES {
        let digest = Sha256::digest(key);
        normalized[..digest.len()].copy_from_slice(&digest);
    } else {
        normalized[..key.len()].copy_from_slice(key);
    }
    let mut inner_key = [0x36u8; BLOCK_BYTES];
    let mut outer_key = [0x5cu8; BLOCK_BYTES];
    for index in 0..BLOCK_BYTES {
        inner_key[index] ^= normalized[index];
        outer_key[index] ^= normalized[index];
    }
    let mut inner = Sha256::new();
    inner.update(inner_key);
    inner.update(message);
    let inner_digest = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(outer_key);
    outer.update(inner_digest);
    encode_base64(&outer.finalize())
}

pub(super) fn ensure_gateway_device_registration(engine: &Engine, identity_id: &str, device_id: &str, public_key: &str) -> Result<(), String> {
    if registered_identity_matches(engine, identity_id, public_key)
        && registered_device_matches(engine, identity_id, device_id, public_key)
    {
        return Ok(());
    }

    let identity_lookup = format!("on _identities | where identityId == {} | limit 1", query_string(identity_id));
    if query_has_rows(engine, 0, identity_lookup) {
        return Err(format!("Gateway identity {identity_id} conflicts with an existing identity"));
    }
    let device_lookup = format!("on _devices | where deviceId == {} | limit 1", query_string(device_id));
    if query_has_rows(engine, 0, device_lookup) {
        return Err(format!("Gateway device {device_id} conflicts with an existing Device"));
    }

    let created_at = unix_time_millis();
    let identity_insert = format!(
        "on _identities | insert {{identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {created_at}, kind: \"gateway\"}}",
        query_string(identity_id), query_string(public_key),
    );
    if !execute_request(engine, QueryRequest::new(0, identity_insert)).is_ok() {
        return Err("unable to persist Gateway identity".to_owned());
    }
    let device_insert = format!(
        "on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {created_at}, kind: \"gateway\"}}",
        query_string(device_id), query_string(identity_id), query_string(public_key),
    );
    if !execute_request(engine, QueryRequest::new(0, device_insert)).is_ok() {
        let rollback = format!("on _identities | where identityId == {} | delete", query_string(identity_id));
        let _ = execute_request(engine, QueryRequest::new(0, rollback));
        return Err("unable to persist Gateway Device".to_owned());
    }
    Ok(())
}

#[cfg(feature = "fabric")]
pub(super) fn authenticate_gateway_fabric( control: &mut NodeWebSocket, configuration: &Configuration, engine: Option<&Engine>, ) -> Result<(), String> {
    write_node_message(control, &serde_json::json!({"kind": "gateway.probe", "version": 1}))?;
    let hello = read_node_message(control)?
        .ok_or_else(|| "Gateway closed before gateway.hello".to_owned())?;
    if hello.str_field("kind") != Some("gateway.hello")
        || hello.u64_field("version") != Some(1)
    {
        return Err("Gateway did not provide a supported gateway.hello".to_owned());
    }
    let state = hello.str_field("state").unwrap_or_default();
    let public_key = hello.str_field("publicKey").unwrap_or_default();
    validate_ed25519_public_key(public_key).map_err(|error| format!("invalid Gateway public key: {error}"))?;

    if state == "enrolled" {
        let identity_id = hello.str_field("identityId").unwrap_or_default();
        let device_id = hello.str_field("deviceId").unwrap_or_default();
        let engine = engine.ok_or_else(|| "Gateway Device verification requires a local Core trust store in Step 2".to_owned())?;
        if !registered_identity_matches(engine, identity_id, public_key)
            || !registered_device_matches(engine, identity_id, device_id, public_key)
        {
            return Err(format!("Gateway Device {device_id} is unknown, revoked, or has a different key"));
        }
        let challenge = gateway_random_challenge()?;
        write_node_message(control, &serde_json::json!({
            "kind": "gateway.challenge", "version": 1,
            "identityId": identity_id, "deviceId": device_id, "challenge": challenge,
        }))?;
        let proof = read_node_message(control)?.ok_or_else(|| "Gateway closed before gateway.proof".to_owned())?;
        if proof.str_field("kind") != Some("gateway.proof")
            || proof.u64_field("version") != Some(1)
        {
            return Err("Gateway did not answer the Device challenge".to_owned());
        }
        let signature = proof.str_field("signature").unwrap_or_default();
        let payload = gateway_identity_proof(&challenge, identity_id, device_id);
        verify_ed25519(public_key, signature, payload.as_bytes())
            .map_err(|error| format!("Gateway Device signature rejected: {error}"))?;
        write_node_message(control, &serde_json::json!({
            "kind": "gateway.accepted", "version": 1,
            "identityId": identity_id, "deviceId": device_id,
        }))?;
        return Ok(());
    }

    if state != "unenrolled" && state != "enrolling" {
        return Err(format!("unsupported Gateway enrollment state {state:?}"));
    }
    if !configuration.node_capabilities.contains(ServiceCapability::Auth) {
        return Err("Only a Core with the auth capability may enroll an unenrolled Gateway".to_owned());
    }
    let engine = engine.ok_or_else(|| "Gateway enrollment requires the Core auth store".to_owned())?;
    let token = configuration.gateway_token.as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "Gateway enrollment requires OGD_GATEWAY_TOKEN on the enrolling Core".to_owned())?;
    let enrollment_challenge = hello.str_field("enrollmentChallenge").unwrap_or_default();
    if enrollment_challenge.is_empty() {
        return Err("unenrolled Gateway did not provide an enrollment challenge".to_owned());
    }
    let (identity_id, device_id) = if state == "enrolling" {
        let identity_id = hello.string_field("identityId");
        let device_id = hello.string_field("deviceId");
        if identity_id.is_empty() || device_id.is_empty() {
            return Err("pending Gateway enrollment is missing identityId/deviceId".to_owned());
        }
        (identity_id, device_id)
    } else {
        let ids = UuidV7Generator::new();
        (ids.next_id().to_string(), ids.next_id().to_string())
    };
    let bootstrap_proof = hmac_sha256_base64(
        token.as_bytes(),
        gateway_enrollment_payload(enrollment_challenge, public_key).as_bytes(),
    );
    let challenge = gateway_random_challenge()?;
    write_node_message(control, &serde_json::json!({
        "kind": "gateway.enroll", "version": 1,
        "identityId": identity_id, "deviceId": device_id,
        "bootstrapProof": bootstrap_proof,
        "challenge": challenge,
    }))?;
    let proof = read_node_message(control)?.ok_or_else(|| "Gateway closed before gateway.enroll.proof".to_owned())?;
    if proof.str_field("kind") != Some("gateway.enroll.proof")
        || proof.u64_field("version") != Some(1)
        || proof.str_field("identityId") != Some(identity_id.as_str())
        || proof.str_field("deviceId") != Some(device_id.as_str())
        || proof.str_field("publicKey") != Some(public_key)
    {
        return Err("Gateway enrollment proof does not match its assignment".to_owned());
    }
    let signature = proof.str_field("signature").unwrap_or_default();
    let payload = gateway_identity_proof(&challenge, &identity_id, &device_id);
    verify_ed25519(public_key, signature, payload.as_bytes())
        .map_err(|error| format!("Gateway enrollment Device signature rejected: {error}"))?;
    ensure_gateway_device_registration(engine, &identity_id, &device_id, public_key)?;
    write_node_message(control, &serde_json::json!({
        "kind": "gateway.enroll.committed", "version": 1,
        "identityId": identity_id, "deviceId": device_id,
    }))?;
    let ack = read_node_message(control)?.ok_or_else(|| "Gateway closed before gateway.enroll.ack".to_owned())?;
    if ack.str_field("kind") != Some("gateway.enroll.ack")
        || ack.u64_field("version") != Some(1)
        || ack.str_field("identityId") != Some(identity_id.as_str())
        || ack.str_field("deviceId") != Some(device_id.as_str())
    {
        return Err("Gateway enrollment commit was not acknowledged".to_owned());
    }
    Ok(())
}


#[cfg(feature = "fabric")]
pub(super) fn load_gateway_directory(path: &Path) -> Vec<String> {
    let Ok(bytes) = fs::read(path) else { return Vec::new(); };
    let Ok(value) = serde_json::from_slice::<JsonValue>(&bytes) else { return Vec::new(); };
    value.array_field("endpoints")
        .into_iter()
        .flatten()
        .filter_map(JsonValue::as_str)
        .filter_map(normalize_gateway_node_endpoint)
        .collect()
}

#[cfg(feature = "fabric")]
pub(super) fn persist_gateway_directory(path: &Path, endpoints: &HashSet<String>) -> Result<(), String> {
    let mut values = endpoints.iter().cloned().collect::<Vec<_>>();
    values.sort();
    let payload = serde_json::to_vec_pretty(&serde_json::json!({
        "version": 1,
        "endpoints": values,
    })).map_err(|error| error.to_string())?;
    write_file_atomic(path, &path.with_extension("json.next"), |output| output.write_all(&payload))
        .map_err(|error| error.to_string())
}

#[cfg(feature = "fabric")]
pub(super) fn run_gateway_fabric( configuration: Configuration, node_credential: IdentityCredential, engine: Option<Arc<Engine>>, operation_router: Arc<OperationRouter>, event_engine: Arc<EventEngine>, settings: Arc<ConnectionSettings>, ) -> Result<(), DaemonError> {
    let configuration = Arc::new(configuration);
    let known = Arc::new(Mutex::new(HashSet::<String>::new()));
    let (sender, receiver) = mpsc::channel::<String>();

    for endpoint in configuration
        .gateway_endpoints
        .iter()
        .cloned()
        .chain(load_gateway_directory(&configuration.gateway_directory_path))
    {
        let _ = sender.send(endpoint);
    }

    loop {
        let endpoint = receiver
            .recv()
            .map_err(|error| DaemonError::Runtime(format!("Gateway connection manager stopped: {error}")))?;
        let Some(endpoint) = normalize_gateway_node_endpoint(&endpoint) else { continue; };
        {
            let mut known_guard = lock_unpoisoned(&known);
            if !known_guard.insert(endpoint.clone()) { continue; }
            if let Err(error) = persist_gateway_directory(&configuration.gateway_directory_path, &known_guard) {
                eprintln!("ogd gateway directory persist {}: {error}", configuration.gateway_directory_path.display());
            }
            if let Some(client_endpoint) = gateway_client_endpoint_from_node(&endpoint) {
                let mut clients = lock_unpoisoned(&settings.upstream_client_endpoints);
                if !clients.contains(&client_endpoint) { clients.push(client_endpoint); }
            }
        }

        let configuration = Arc::clone(&configuration);
        let credential = node_credential.clone();
        let engine = engine.clone();
        let operation_router = Arc::clone(&operation_router);
        let event_engine = Arc::clone(&event_engine);
        let settings = Arc::clone(&settings);
        let discovery_sender = sender.clone();
        let spawned_endpoint = endpoint.clone();
        let spawned = thread::Builder::new()
            .name(format!("ogd-gateway-{}", endpoint.replace('/', "_").replace(':', "_")))
            .spawn(move || {
                if let Err(error) = run_gateway_node(
                    endpoint,
                    configuration.as_ref(),
                    credential,
                    engine,
                    operation_router,
                    event_engine,
                    settings,
                    Some(discovery_sender),
                ) {
                    eprintln!("ogd gateway worker stopped: {error}");
                }
            });
        if let Err(error) = spawned {
            // Forget the endpoint so that a later discovery retries it.
            eprintln!("ogd gateway worker spawn {spawned_endpoint}: {error}");
            lock_unpoisoned(&known).remove(&spawned_endpoint);
        }
    }
}

#[cfg(not(feature = "fabric"))]
pub(super) fn run_gateway_fabric( _configuration: Configuration, _node_credential: IdentityCredential, _engine: Option<Arc<Engine>>, _operation_router: Arc<OperationRouter>, _event_engine: Arc<EventEngine>, _settings: Arc<ConnectionSettings>, ) -> Result<(), DaemonError> {
    Err(DaemonError::Runtime(
        "Gateway mode requires an ogd build with the `fabric` feature".to_owned(),
    ))
}

#[cfg(feature = "fabric")]
pub(super) fn set_gateway_runtime_state( settings: &ConnectionSettings, endpoint: &str, connected: bool, disconnected_state: &'static str, ) {
    let any_connected = {
        let mut connections = lock_unpoisoned(&settings.gateway_connections);
        connections.insert(endpoint.to_owned(), connected);
        connections.values().any(|value| *value)
    };
    *lock_unpoisoned(&settings.gateway_state) =
        if any_connected { "connected" } else { disconnected_state };
}

#[cfg(feature = "fabric")]
pub(super) fn run_gateway_node( endpoint: String, configuration: &Configuration, node_credential: IdentityCredential, engine: Option<Arc<Engine>>, operation_router: Arc<OperationRouter>, event_engine: Arc<EventEngine>, settings: Arc<ConnectionSettings>, discovery_sender: Option<mpsc::Sender<String>>, ) -> Result<(), DaemonError> {
    let capabilities = configuration.published_capabilities.names();
    let operation_contracts = OPERATION_CATALOG
        .iter()
        .filter(|operation| {
            operation.kind.is_compiled()
                && operation.kind.is_published_by(configuration.published_capabilities)
                && configuration.node_capabilities.contains_all(operation.kind.required_capabilities())
        })
        .map(|operation| {
            (
                operation.name.to_owned(),
                serde_json::json!({
                    "capability": operation.kind.provider_capability().map(ServiceCapability::as_str),
                    "access": operation.access.as_str(),
                    "transport": operation.transport.as_str(),
                    "connection": operation.connection.as_str(),
                }),
            )
        })
        .collect::<serde_json::Map<String, JsonValue>>();
    let revocation_subscription = configuration
        .node_capabilities
        .contains(ServiceCapability::Auth)
        .then(|| event_engine.subscribe(vec!["fabric.device.revoked".to_owned()]));
    let channel_dispatcher = start_gateway_channel_dispatcher(Arc::clone(&settings.connection_admission))?;
    loop {
        set_gateway_runtime_state(&settings, &endpoint, false, "connecting");
        debug::log(DebugTopic::Gateway, None, format!("connecting node fabric endpoint={endpoint}"));
        let (mut control, _) = match websocket_connect(endpoint.as_str()) {
            Ok(connection) => connection,
            Err(error) => {
                set_gateway_runtime_state(&settings, &endpoint, false, "retrying");
                eprintln!("ogd gateway connect {endpoint}: {error}; retrying");
                thread::sleep(Duration::from_secs(2));
                continue;
            }
        };
        if let Err(error) = authenticate_gateway_fabric(&mut control, configuration, engine.as_deref()) {
            set_gateway_runtime_state(&settings, &endpoint, false, "retrying");
            eprintln!("ogd gateway identity {endpoint}: {error}; retrying");
            thread::sleep(Duration::from_secs(2));
            continue;
        }
        // Every Core participating in the fabric is a Node. `deviceId` is the stable
        // routing identifier; identity proof is challenge-based and carries no role.
        let node_id = node_credential.device_id.clone();
        let hello = serde_json::json!({
            "kind": "node.hello",
            "version": NODE_CONTROL_PROTOCOL_VERSION,
            "nodeId": node_id.clone(),
            "instanceId": configuration.instance_id.clone(),
            "identityId": node_credential.identity_id.clone(),
            "deviceId": node_credential.device_id.clone(),
            "publicKey": node_credential.public_key.clone(),
            "nodeVersion": env!("CARGO_PKG_VERSION"),
            "capabilities": capabilities.clone(),
            "operationContracts": operation_contracts.clone(),
            "token": configuration.gateway_token.clone(),
        });
        if let Err(error) = write_node_message(&mut control, &hello) {
            set_gateway_runtime_state(&settings, &endpoint, false, "retrying");
            eprintln!("ogd gateway hello {endpoint}: {error}");
            thread::sleep(Duration::from_secs(2));
            continue;
        }
        let mut accepted = false;
        loop {
            if accepted {
                if let Some(subscription) = revocation_subscription.as_ref() {
                    loop {
                        match subscription.try_recv() {
                            Ok(event) => {
                                let Some(device_id) = event.payload.str_field("deviceId") else { continue; };
                                let notification = serde_json::json!({
                                    "kind": "fabric.device.revoked",
                                    "version": 1,
                                    "eventId": event.id,
                                    "deviceId": device_id,
                                    "revokedAt": event.payload.get("revokedAt").cloned().unwrap_or(JsonValue::Null),
                                });
                                if let Err(error) = write_node_message(&mut control, &notification) {
                                    eprintln!("ogd gateway revocation notification: {error}");
                                    break;
                                }
                            }
                            Err(std::sync::mpsc::TryRecvError::Empty) => break,
                            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                        }
                    }
                }
            }
            let message = if accepted {
                match poll_node_message(&mut control) {
                    Ok(NodeMessagePoll::Message(message)) => message,
                    Ok(NodeMessagePoll::Idle) => continue,
                    Ok(NodeMessagePoll::Closed) => break,
                    Err(error) => { eprintln!("ogd gateway control: {error}"); break; }
                }
            } else {
                match read_node_message(&mut control) {
                    Ok(Some(message)) => message,
                    Ok(None) => break,
                    Err(error) => { eprintln!("ogd gateway control: {error}"); break; }
                }
            };
            match message.str_field("kind") {
                Some("node.challenge") => {
                    if message.u64_field("version") != Some(NODE_CONTROL_PROTOCOL_VERSION) {
                        eprintln!("ogd gateway control: unsupported node challenge version");
                        break;
                    }
                    let Some(challenge) = message.str_field("challenge") else {
                        eprintln!("ogd gateway control: node challenge is missing its nonce");
                        break;
                    };
                    let proof = node_identity_proof(
                        challenge,
                        &node_id,
                        &node_credential.identity_id,
                        &node_credential.device_id,
                    );
                    let response = serde_json::json!({
                        "kind": "node.proof",
                        "version": NODE_CONTROL_PROTOCOL_VERSION,
                        "signature": node_credential.sign_base64(proof.as_bytes()),
                    });
                    if let Err(error) = write_node_message(&mut control, &response) {
                        eprintln!("ogd gateway control proof: {error}");
                        break;
                    }
                }
                Some("node.accepted") => {
                    set_gateway_runtime_state(&settings, &endpoint, true, "retrying");
                    accepted = true;
                    if let Err(error) = configure_websocket_polling(&mut control) {
                        eprintln!("ogd gateway control polling: {error}");
                        break;
                    }
                    debug::log(DebugTopic::Gateway, None, format!("node accepted endpoint={endpoint}"));
                }
                Some("node.rejected") => {
                    let code = message.str_field("code").unwrap_or("NODE_REJECTED");
                    let reason = message.str_field("message").unwrap_or("Gateway rejected this Node");
                    eprintln!("ogd gateway node rejected endpoint={endpoint} code={code}: {reason}");
                    break;
                }
                Some("fabric.gateways") if accepted => {
                    if message.u64_field("version") != Some(1) { continue; }
                    let Some(sender) = discovery_sender.as_ref() else { continue; };
                    for value in message.array_field("endpoints").into_iter().flatten() {
                        let Some(raw) = value.as_str() else { continue; };
                        let Some(discovered) = normalize_gateway_node_endpoint(raw) else { continue; };
                        if discovered == endpoint { continue; }
                        debug::log(DebugTopic::Gateway, None, format!("discovered Gateway endpoint={discovered} via={endpoint}"));
                        let _ = sender.send(discovered);
                    }
                }
                Some("node.device.verify") => {
                    let request_id = message.str_field("requestId").unwrap_or_default();
                    let identity_id = message.str_field("identityId").unwrap_or_default();
                    let device_id = message.str_field("deviceId").unwrap_or_default();
                    let public_key = message.str_field("publicKey").unwrap_or_default();
                    let verified = configuration.node_capabilities.contains(ServiceCapability::Auth)
                        && engine.as_deref().map(|engine| registered_device_matches(engine, identity_id, device_id, public_key)).unwrap_or(false);
                    let response = serde_json::json!({
                        "kind": "node.device.verify.result",
                        "version": NODE_CONTROL_PROTOCOL_VERSION,
                        "requestId": request_id,
                        "verified": verified,
                    });
                    if let Err(error) = write_node_message(&mut control, &response) {
                        eprintln!("ogd gateway device verification response: {error}");
                        break;
                    }
                }
                Some("node.open") => {
                    let Some(channel_id) = message.str_field("channelId").map(str::to_owned) else { continue; };
                    let delegated_identity_id = message.str_field("identityId").map(str::to_owned);
                    let delegated_device_id = message.str_field("deviceId").map(str::to_owned);
                    let delegated_place_id = message.str_field("placeId").map(str::to_owned);
                    let delegated_capability = message.str_field("capability").map(str::to_owned);
                    let delegated_token = message.str_field("delegationToken").map(str::to_owned);
                    if let Some(capability_name) = delegated_capability.as_deref() {
                        let Some(capability) = ServiceCapability::parse(capability_name) else {
                            debug::log( DebugTopic::Gateway, None, format!("rejecting node.open for unknown capability={capability_name}"), );
                            continue;
                        };
                        if !configuration.published_capabilities.contains(capability) {
                            debug::log( DebugTopic::Gateway, None, format!("rejecting node.open for unpublished capability={capability_name}"), );
                            continue;
                        }
                    }
                    let delegated_app_instance_id = message.str_field("appInstanceId").map(str::to_owned);
                    let delegated_place_role = message.str_field("placeRole").and_then(PlaceRole::parse);
                    let delegated_public_access = message.str_field("publicAccess").and_then(PublicAccess::parse);
                    let endpoint = endpoint.clone();
                    let engine = engine.clone();
                    let operation_router = Arc::clone(&operation_router);
                    let event_engine = Arc::clone(&event_engine);
                    let settings = Arc::clone(&settings);
                    let channel_node_id = node_id.clone();
                    let pending_channel_id = channel_id.clone();
                    let open = Box::new(move |permit: ConnectionPermit| {
                        let _permit = permit;
                        let (mut websocket, _) = match websocket_connect(endpoint.as_str()) {
                            Ok(connection) => connection,
                            Err(error) => { eprintln!("ogd gateway channel connect {endpoint}: {error}"); return; }
                        };
                        let hello = serde_json::json!({
                            "kind": "node.channel",
                            "version": 2,
                            "nodeId": channel_node_id,
                            "channelId": channel_id,
                        });
                        if let Err(error) = write_node_message(&mut websocket, &hello) {
                            eprintln!("ogd gateway channel hello: {error}");
                            return;
                        }
                        let (core_stream, bridge_stream) = match tcp_pair() {
                            Ok(pair) => pair,
                            Err(error) => { eprintln!("ogd gateway local channel: {error}"); return; }
                        };
                        let bridge = thread::Builder::new().name("ogd-gateway-ws-bridge".to_owned()).spawn(move || {
                            if let Err(error) = bridge_websocket_channel(websocket, bridge_stream) {
                                eprintln!("ogd gateway websocket bridge: {error}");
                            }
                        });
                        if let Err(error) = bridge {
                            eprintln!("ogd gateway bridge spawn: {error}");
                            return;
                        }
                        let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                        // An identity delegation carries the principal of the connected client. A public
                        // delegation carries no identity: the Authority resolved the Place public
                        // policy for an anonymous client, and the provider executes within it.
                        let delegation = match (delegated_identity_id, delegated_device_id, delegated_capability) {
                            (Some(identity_id), Some(device_id), Some(capability)) => Some(GatewayDelegation {
                                principal: Principal::Identity { identity_id, device_id },
                                place_id: delegated_place_id,
                                capability,
                                app_instance_id: delegated_app_instance_id,
                                place_role: delegated_place_role,
                                public_access: delegated_public_access,
                                token: delegated_token,
                            }),
                            (None, None, Some(capability)) if delegated_place_id.is_some() && delegated_public_access.is_some() => Some(GatewayDelegation {
                                principal: Principal::Anonymous,
                                place_id: delegated_place_id,
                                capability,
                                app_instance_id: delegated_app_instance_id,
                                place_role: None,
                                public_access: delegated_public_access,
                                token: delegated_token,
                            }),
                            _ => None,
                        };
                        if let Err(error) = serve_connection(
                            connection_id, core_stream, engine.as_deref(), &operation_router, &event_engine, &settings, delegation,
                        ) {
                            eprintln!("ogd gateway channel: {error}");
                        }
                    });
                    let pending = PendingGatewayChannel {
                        channel_id: pending_channel_id,
                        deadline: Instant::now() + GATEWAY_CHANNEL_ADMISSION_WAIT,
                        open,
                    };
                    if channel_dispatcher.send(pending).is_err() {
                        eprintln!("ogd gateway channel dispatcher stopped");
                    }
                }
                _ => {}
            }
        }
        set_gateway_runtime_state(&settings, &endpoint, false, "retrying");
        thread::sleep(Duration::from_secs(1));
    }
}

/// Stays under the Gateway's default channel-open timeout
/// (`OG_GATEWAY_NODE_CHANNEL_TIMEOUT_MS`, 5 s): once the Gateway has given up
/// on a channel, opening it would only waste a slot.
#[cfg(feature = "fabric")]
pub(super) const GATEWAY_CHANNEL_ADMISSION_WAIT: Duration = Duration::from_secs(4);

#[cfg(feature = "fabric")]
pub(super) struct PendingGatewayChannel {
    pub(super) channel_id: String,
    pub(super) deadline: Instant,
    pub(super) open: Box<dyn FnOnce(ConnectionPermit) + Send>,
}

/// Admits `node.open` requests in order without blocking the control channel.
/// Each channel runs two threads: the connection and its websocket bridge.
#[cfg(feature = "fabric")]
pub(super) fn start_gateway_channel_dispatcher( admission: Arc<ConnectionAdmission>, ) -> Result<mpsc::Sender<PendingGatewayChannel>, DaemonError> {
    let (sender, receiver) = mpsc::channel::<PendingGatewayChannel>();
    thread::Builder::new().name("ogd-gateway-admission".to_owned()).spawn(move || {
        for pending in receiver {
            let Some(permit) = admission.acquire_until(2, AdmissionPriority::Gateway, Some(pending.deadline)) else {
                eprintln!("ogd gateway channel {}: no memory capacity before the Gateway timeout; dropped", pending.channel_id);
                continue;
            };
            let open = pending.open;
            if let Err(error) = thread::Builder::new().name("ogd-gateway-channel".to_owned()).spawn(move || open(permit)) {
                eprintln!("ogd gateway channel {} spawn: {error}", pending.channel_id);
            }
        }
    }).map_err(DaemonError::SpawnConnectionThread)?;
    Ok(sender)
}

#[cfg(not(feature = "fabric"))]
pub(super) fn read_gateway_message(_websocket: &mut NodeWebSocket) -> Result<Option<JsonValue>, String> {
    Err("ogd was built without the `fabric` feature".to_owned())
}

#[cfg(not(feature = "fabric"))]
pub(super) fn gateway_response_for_request(_websocket: &mut NodeWebSocket, _request: &OperationRequest) -> Result<JsonValue, String> {
    Err("ogd was built without the `fabric` feature".to_owned())
}

#[cfg(not(feature = "fabric"))]
pub(super) fn connect_authenticated_gateway_client( _endpoint: &str, _credential: &IdentityCredential, _read_timeout: Duration, _write_timeout: Duration, ) -> Result<NodeWebSocket, String> {
    Err("ogd was built without the `fabric` feature".to_owned())
}

#[cfg(not(feature = "fabric"))]
pub(super) fn run_gateway_node( _endpoint: String, _configuration: &Configuration, _credential: IdentityCredential, _engine: Option<Arc<Engine>>, _operation_router: Arc<OperationRouter>, _event_engine: Arc<EventEngine>, _settings: Arc<ConnectionSettings>, _discovery_sender: Option<mpsc::Sender<String>>, ) -> Result<(), DaemonError> {
    Err(DaemonError::Runtime(
        "Gateway mode requires an ogd build with the `fabric` feature".to_owned(),
    ))
}

#[cfg(feature = "fabric")]
pub(super) fn gateway_raw_read_to_path(websocket: &mut NodeWebSocket, request: &OperationRequest, destination: &Path) -> Result<u64, String> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let request_id = serde_json::to_value(request.id).map_err(|error| error.to_string())?;
    let value = serde_json::to_value(request).map_err(|error| error.to_string())?;
    write_gateway_message(websocket, &value)?;

    let temporary = sync_temporary_path(destination);
    let mut output: Option<fs::File> = None;
    let mut expected = None::<u64>;
    let mut received = 0u64;
    loop {
        let message = websocket.read().map_err(|error| error.to_string())?;
        match message {
            WebSocketMessage::Text(payload) => {
                let message = serde_json::from_str::<JsonValue>(&payload).map_err(|error| error.to_string())?;
                if message.str_field("kind") != Some("response") || message.get("id") != Some(&request_id) {
                    continue;
                }
                if message.str_field("status") == Some("error") || message.get("error").is_some() {
                    let _ = fs::remove_file(&temporary);
                    return Err(format!("file.read failed upstream: {}", message.get("error").cloned().unwrap_or(JsonValue::Null)));
                }
                if message.str_field("status") == Some("complete") {
                    let Some(expected) = expected else { return Err("file.read completed without a raw header".to_owned()); };
                    if received != expected {
                        let _ = fs::remove_file(&temporary);
                        return Err(format!("file.read ended after {received} of {expected} bytes"));
                    }
                    if let Some(mut file) = output.take() {
                        file.flush().map_err(|error| error.to_string())?;
                        file.sync_all().map_err(|error| error.to_string())?;
                    } else {
                        fs::File::create(&temporary).map_err(|error| error.to_string())?;
                    }
                    replace_sync_file(&temporary, destination)?;
                    return Ok(received);
                }
                if message.get("data").and_then(|data| data.get("stream")).and_then(JsonValue::as_str) == Some("raw") {
                    let bytes = message.get("data").and_then(|data| data.get("bytes")).and_then(JsonValue::as_u64)
                        .ok_or_else(|| "file.read raw header has no byte count".to_owned())?;
                    expected = Some(bytes);
                    output = Some(fs::OpenOptions::new().write(true).create(true).truncate(true).open(&temporary).map_err(|error| error.to_string())?);
                }
            }
            WebSocketMessage::Binary(payload) => {
                let limit = expected.ok_or_else(|| "file.read sent binary data before its header".to_owned())?;
                let next = received.saturating_add(payload.len() as u64);
                if next > limit {
                    let _ = fs::remove_file(&temporary);
                    return Err("file.read sent more bytes than announced".to_owned());
                }
                let file = output.as_mut().ok_or_else(|| "file.read has no local output".to_owned())?;
                file.write_all(&payload).map_err(|error| error.to_string())?;
                received = next;
            }
            WebSocketMessage::Ping(payload) => websocket.send(WebSocketMessage::Pong(payload)).map_err(|error| error.to_string())?,
            WebSocketMessage::Close(_) => {
                let _ = fs::remove_file(&temporary);
                return Err("gateway closed during file.read".to_owned());
            }
            _ => {}
        }
    }
}


#[cfg(feature = "fabric")]
pub(super) fn gateway_raw_write_from_path(websocket: &mut NodeWebSocket, request: &OperationRequest, source: &Path) -> Result<JsonValue, String> {
    // Open the local file before advertising the binary stream size. A native
    // watcher can wake reconciliation while a large file is still being copied
    // into the sync root. If we announce one size and then stream a growing or
    // shrinking path, the receiver can wait forever for bytes that will never
    // arrive (or the stream can bleed into the next request).
    //
    // Holding the open handle also gives this upload one coherent source object.
    let mut input = fs::File::open(source).map_err(|error| error.to_string())?;
    let expected = input.metadata().map_err(|error| error.to_string())?.len();
    let requested = request
        .data.u64_field("size")
        .ok_or_else(|| "file.write request has no byte count".to_owned())?;
    if requested != expected {
        return Err(format!(
            "local file changed before upload started (requested {requested} bytes, now {expected}); retrying on the next filesystem event"
        ));
    }

    let request_id = serde_json::to_value(request.id).map_err(|error| error.to_string())?;
    let value = serde_json::to_value(request).map_err(|error| error.to_string())?;
    write_gateway_message(websocket, &value)?;

    loop {
        let message = websocket.read().map_err(|error| error.to_string())?;
        match message {
            WebSocketMessage::Text(payload) => {
                let message = serde_json::from_str::<JsonValue>(&payload).map_err(|error| error.to_string())?;
                if message.str_field("kind") != Some("response") || message.get("id") != Some(&request_id) {
                    continue;
                }
                if message.str_field("status") == Some("error") || message.get("error").is_some() {
                    return Err(format!("file.write failed upstream: {}", message.get("error").cloned().unwrap_or(JsonValue::Null)));
                }
                let ready = message.get("data")
                    .is_some_and(|data| data.str_field("stream") == Some("raw")
                        && data.bool_field("ready") == Some(true));
                if !ready {
                    continue;
                }
                let announced = message.get("data").and_then(|data| data.get("bytes")).and_then(JsonValue::as_u64)
                    .ok_or_else(|| "file.write raw header has no byte count".to_owned())?;
                if announced != expected {
                    return Err(format!("file.write expected {announced} bytes but local file has {expected}"));
                }

                let mut buffer = vec![0u8; 64 * 1024];
                let mut remaining = expected;
                while remaining > 0 {
                    let limit = usize::try_from(remaining.min(buffer.len() as u64))
                        .map_err(|_| "file.write remaining byte count is invalid".to_owned())?;
                    let count = input.read(&mut buffer[..limit]).map_err(|error| error.to_string())?;
                    if count == 0 {
                        // Abort the raw session explicitly. Otherwise Gateway/Core
                        // legitimately keeps waiting for the announced tail forever.
                        close_node_websocket(websocket);
                        return Err(format!(
                            "local file changed during upload: reached EOF with {remaining} bytes still announced; the upload was aborted"
                        ));
                    }
                    websocket.send(WebSocketMessage::Binary(buffer[..count].to_vec().into()))
                        .map_err(|error| error.to_string())?;
                    remaining = remaining.saturating_sub(count as u64);
                }
                break;
            }
            WebSocketMessage::Ping(payload) => websocket.send(WebSocketMessage::Pong(payload)).map_err(|error| error.to_string())?,
            WebSocketMessage::Close(_) => return Err("gateway closed before file.write became ready".to_owned()),
            _ => {}
        }
    }

    loop {
        let message = websocket.read().map_err(|error| error.to_string())?;
        match message {
            WebSocketMessage::Text(payload) => {
                let message = serde_json::from_str::<JsonValue>(&payload).map_err(|error| error.to_string())?;
                if message.str_field("kind") != Some("response") || message.get("id") != Some(&request_id) {
                    continue;
                }
                if message.str_field("status") == Some("error") || message.get("error").is_some() {
                    return Err(format!("file.write failed upstream: {}", message.get("error").cloned().unwrap_or(JsonValue::Null)));
                }
                // file.write is a two-response BinaryIn operation:
                //   1. status=ok, data={ready:true, stream:"raw", bytes:N}
                //   2. status=ok, data={file:..., bytes:N, stream:"raw"}
                //
                // "complete" is the stream status used by MessageStream (and the
                // Gateway synthesizes one after BinaryOut/file.read). Native Core
                // OperationResponse::new() deliberately uses status="ok", so waiting
                // for "complete" here leaves the sync stuck forever after the first
                // successfully persisted file.
                if message.str_field("status") == Some("ok") {
                    let data = message.get("data").cloned()
                        .ok_or_else(|| "file.write completed without data".to_owned())?;
                    if data.bool_field("ready") == Some(true) {
                        // Defensive: a duplicate readiness response is not terminal.
                        continue;
                    }
                    return Ok(data);
                }
            }
            WebSocketMessage::Ping(payload) => websocket.send(WebSocketMessage::Pong(payload)).map_err(|error| error.to_string())?,
            WebSocketMessage::Close(_) => return Err("gateway closed during file.write".to_owned()),
            _ => {}
        }
    }
}

#[cfg(not(feature = "fabric"))]
pub(super) fn gateway_raw_read_to_path(_websocket: &mut NodeWebSocket, _request: &OperationRequest, _destination: &Path) -> Result<u64, String> {
    Err("ogd was built without the `fabric` feature".to_owned())
}

#[cfg(not(feature = "fabric"))]
pub(super) fn gateway_raw_write_from_path(_websocket: &mut NodeWebSocket, _request: &OperationRequest, _source: &Path) -> Result<JsonValue, String> {
    Err("ogd was built without the `fabric` feature".to_owned())
}

pub(super) fn start_upstream_event_relay( settings: Arc<ConnectionSettings>, event_engine: Arc<EventEngine>, ) -> Option<thread::JoinHandle<()>> {
    if !has_upstream_gateway(&settings)
        || settings.upstream_identity.is_none()
        || !settings.service_capabilities.contains(ServiceCapability::Events)
    {
        return None;
    }

    thread::Builder::new()
        .name("og-upstream-events".to_owned())
        .spawn(move || loop {
            match run_upstream_event_relay_session(&settings, &event_engine) {
                Ok(()) => {}
                Err(error) => {
                    debug::log(DebugTopic::Events, None, format!("upstream event relay reconnecting: {error}"));
                    if let Ok(mut runtime) = settings.file_sync.lock() {
                        runtime.remote_events = "retrying".to_owned();
                    }
                }
            }
            thread::sleep(Duration::from_secs(1));
        })
        .ok()
}

pub(super) fn run_upstream_event_relay_session( settings: &ConnectionSettings, event_engine: &EventEngine, ) -> Result<(), String> {
    let credential = settings
        .upstream_identity
        .as_ref()
        .ok_or_else(|| "no node Identity for upstream event relay".to_owned())?;
    let (mut websocket, _endpoint) = connect_authenticated_gateway_client_any(
        settings,
        credential,
        Duration::from_secs(120),
        settings.write_timeout,
    )?;
    let request = OperationRequest::new(
        RequestId::Number(9_001),
        "events.subscribe",
        serde_json::json!({"types": ["*"]}),
    );
    let response = gateway_response_for_request(&mut websocket, &request)?;
    if response.str_field("status") == Some("error") || response.get("error").is_some() {
        return Err(format!("events.subscribe rejected: {}", response.get("error").cloned().unwrap_or(JsonValue::Null)));
    }
    if let Ok(mut runtime) = settings.file_sync.lock() {
        runtime.remote_events = "connected".to_owned();
        file_sync_signal(&runtime, "remote.events.connected");
    }
    debug::log(DebugTopic::Events, None, "upstream event relay connected");

    loop {
        let Some(message) = read_gateway_message(&mut websocket)? else {
            return Err("Gateway event channel closed".to_owned());
        };
        if message.str_field("kind") != Some("event") {
            continue;
        }
        // Preserve the Core event envelope (id, audience, timestamp, payload).
        // Local consumers such as Desktop and Files sync then observe exactly
        // the same event contract as Hub clients connected to Gateway.
        if let Ok(event) = serde_json::from_value::<crate::CoreEvent>(message) {
            event_engine.publish(event);
        }
    }
}
