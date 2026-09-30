//! One client connection: read requests, route them, answer each one.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;

/// Why a handler stopped before its normal end.
pub(super) enum Stop {
    /// The request is answered with this response (an error, or a failed query).
    Reply(QueryResponse),
    /// The connection itself failed.
    Connection(ConnectionError),
}

impl From<QueryResponse> for Stop {
    fn from(response: QueryResponse) -> Self { Self::Reply(response) }
}

impl From<ConnectionError> for Stop {
    fn from(error: ConnectionError) -> Self { Self::Connection(error) }
}

/// What a handler returns: `Err` holds the answer it stopped on.
pub(super) type Handled = Result<(), Stop>;

pub(super) fn request_error(id: RequestId, code: impl Into<String>, message: impl Into<String>) -> Stop {
    Stop::Reply(QueryResponse::request_error(id, code, message))
}

/// Maps a failure to a request error with `code` and the failure's message.
pub(super) fn failed<E: Display>(id: RequestId, code: &'static str) -> impl FnOnce(E) -> Stop {
    move |error| request_error(id, code, error.to_string())
}

pub(super) fn auth_failed(id: RequestId) -> impl FnOnce(crate::AuthError) -> Stop {
    failed(id, "auth.failed")
}

pub(super) fn store_failed(id: RequestId) -> impl FnOnce(FileStoreError) -> Stop {
    move |error| Stop::Reply(file_store_query_error(id, error))
}

/// A request that reached a handler it does not belong to.
pub(super) fn routing_error(id: RequestId, detail: &str) -> QueryResponse {
    debug::log(DebugTopic::Core, None, format!("routing invariant violated for request {id}: {detail}"));
    QueryResponse::request_error(id, "operation.internal_error", format!("internal routing error: {detail}"))
}

pub(super) fn diverged(id: RequestId) -> Handled {
    Err(routing_error(id, "handler domain and routed variant diverged").into())
}

pub(super) fn reply(writer: &mut TcpStream, id: RequestId, data: JsonValue) -> Result<(), ConnectionError> {
    write_operation_response(writer, &OperationResponse::new(id, data))
}

/// Writes the whole response of `query`, then publishes the event when it succeeded.
pub(super) fn write_query_then_publish(writer: &mut TcpStream, engine: &Engine, id: RequestId, query: String, audience: Audience, event_type: &str, payload: JsonValue) -> Handled {
    let response = execute_request(engine, QueryRequest::new(id, query));
    let ok = response.is_ok();
    write_response(writer, &response)?;
    if ok { publish_durable_event(engine, audience, event_type, payload); }
    Ok(())
}

pub(super) fn require_identity<'a>(principal: &'a Principal, id: RequestId, message: &str) -> Result<&'a str, Stop> {
    principal_identity_id(principal).ok_or_else(|| request_error(id, "authorization.required", message))
}

pub(super) fn authenticated_device(principal: &Principal, id: RequestId, message: &str) -> Result<(String, String), Stop> {
    match principal {
        Principal::Identity { identity_id, device_id } => Ok((identity_id.clone(), device_id.clone())),
        Principal::Anonymous => Err(request_error(id, "authorization.required", message)),
    }
}

pub(super) fn place_role(engine: &Engine, id: RequestId, identity_id: &str, place: &PlaceRecord) -> Result<PlaceRole, Stop> {
    resolve_place_role(engine, id, identity_id, place)?
        .ok_or_else(|| request_error(id, "place.access_denied", "the identity does not have access to this Place"))
}

pub(super) fn require_place_owner(engine: &Engine, id: RequestId, identity_id: &str, place: &PlaceRecord, message: &str) -> Handled {
    if !place_role(engine, id, identity_id, place)?.can_manage() { reject!(id, "place.owner_required", message); }
    Ok(())
}

fn ensure_routed_static_authorized(settings: &ConnectionSettings, engine: Option<&Engine>, principal: &Principal, operation: &RoutedOperation) -> Handled {
    match operation.kind().access() {
        AccessPolicy::Permission { action, resource } => {
            let Some(engine) = engine else {
                reject!(operation.id(), "capability.unavailable", "this operation requires the local database engine");
            };
            ensure_authorized(settings, engine, principal, operation.id(), action, resource)
        }
        AccessPolicy::Public | AccessPolicy::Authenticated | AccessPolicy::Query | AccessPolicy::DynamicPermission(_) => Ok(()),
    }
}

pub(super) fn ensure_operation_resource_authorized(settings: &ConnectionSettings, engine: &Engine, principal: &Principal, id: RequestId, operation: OperationKind, resource: &str) -> Handled {
    let (action, resource) = match operation.access() {
        AccessPolicy::DynamicPermission(action) => (action, resource),
        AccessPolicy::Permission { action, resource } => (action, resource),
        AccessPolicy::Public | AccessPolicy::Authenticated | AccessPolicy::Query => return Ok(()),
    };
    ensure_authorized(settings, engine, principal, id, action, resource)
}

fn ensure_authorized(settings: &ConnectionSettings, engine: &Engine, principal: &Principal, id: RequestId, action: AuthorizationAction, resource: &str) -> Handled {
    if !settings.authorization_mode.is_enforced() || authorize_connection(engine, principal, action, resource) {
        return Ok(());
    }
    Err(authorization_denied(id, principal, action, resource).into())
}

pub(super) fn authorization_denied(id: RequestId, principal: &Principal, action: AuthorizationAction, resource: &str) -> QueryResponse {
    let message = match principal {
        Principal::Anonymous => format!("authentication is required for {} on {:?}", action.as_str(), resource),
        Principal::Identity { identity_id, .. } => format!("identity {:?} is not allowed to perform {} on {:?}", identity_id, action.as_str(), resource),
    };
    QueryResponse::request_error(id, "authorization.denied", message)
}

pub(super) fn place_role_denied(id: RequestId, context: &ExecutionContext, action: AuthorizationAction, resource: &str) -> QueryResponse {
    let message = format!( "Place grant {:?} is not allowed to perform {} on {:?} in Place {:?}", context.access.as_str(), action.as_str(), resource, context.place_id, );
    QueryResponse::request_error(id, "authorization.denied", message)
}

pub(super) fn serve_connection( connection_id: u64, stream: TcpStream, engine: Option<&Engine>, operation_router: &OperationRouter, event_engine: &EventEngine, settings: &ConnectionSettings, delegation: Option<GatewayDelegation>, ) -> Result<(), ConnectionError> {
    // A Gateway-delegated Agent provider channel can legitimately sit idle while
    // another capability (most notably llm.generate) is working. Reusing the
    // ordinary 30s connection read timeout here caused the Core side of a DB/file
    // provider channel to close while Qwen was thinking; the Gateway would then
    // try to reuse that stale channel for the next analytical query and report
    // `gateway.core_request_failed: The og-core connection was interrupted.`
    //
    // Use the dedicated Agent provider timeout on delegated channels as well as
    // on the outbound Agent->Gateway websocket. This keeps both ends of the
    // delegated provider session on the same lifetime policy without weakening
    // the timeout for normal client connections.
    let connection_read_timeout = if delegation.is_some() {
        settings.agent_provider_read_timeout
    } else {
        settings.read_timeout
    };
    stream.set_read_timeout(Some(connection_read_timeout)).map_err(ConnectionError::ConfigureSocket)?;
    stream.set_write_timeout(Some(settings.write_timeout)).map_err(ConnectionError::ConfigureSocket)?;
    stream.set_nodelay(true).map_err(ConnectionError::ConfigureSocket)?;

    let mut connection = Connection {
        id: connection_id,
        writer: stream.try_clone().map_err(ConnectionError::CloneSocket)?,
        reader: BufReader::new(stream),
        subscription: None,
        authentication: delegation.as_ref().map(|value| ConnectionAuth::from_principal(value.principal.clone())).unwrap_or_default(),
    };
    let mut payload = Vec::with_capacity(4096);

    loop {
        drain_events(connection_id, &mut connection.writer, connection.subscription.as_ref(), connection.authentication.principal())?;
        payload.clear();
        // A single large request must not pin up to MAX_REQUEST_BYTES for the
        // rest of the connection: admission assumes a small idle footprint.
        payload.shrink_to(64 * 1024);

        let bytes_read = match read_message(&mut connection.reader, &mut payload) {
            Ok(bytes_read) => bytes_read,
            Err(ConnectionError::Read(error))
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if connection.subscription.is_some() {
                    drain_events(connection_id, &mut connection.writer, connection.subscription.as_ref(), connection.authentication.principal())?;
                    continue;
                }
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        if bytes_read == 0 {
            return Ok(());
        }

        if debug::protocol_enabled() {
            let value = rmp_serde::from_slice::<JsonValue>(&payload)
                .map(debug::redact_json)
                .unwrap_or_else(|error| {
                    serde_json::json!({
                        "bytes": payload.len(),
                        "encoding": "invalid-msgpack",
                        "error": error.to_string(),
                    })
                });
            debug::log( DebugTopic::Protocol, Some(connection_id), format!( "< message bytes={} encoding=msgpack {}", payload.len(), value ), );
        }

        let request_started = Instant::now();
        let decode_started = Instant::now();
        let decoded = decode_operation_request(&payload);
        let decode_us = elapsed_micros(decode_started);
        let mut request_id = None;
        let mut compact = false;
        let wire_started = Instant::now();
        let handled = match decoded {
            Ok(operation_request) => {
                request_id = Some(operation_request.id);
                debug::log( DebugTopic::Router, Some(connection_id), format!( "request id={} op={}", operation_request.id, operation_request.op ), );
                let authority_request = operation_request.clone();
                match operation_router.route(operation_request) {
                    Ok(operation) => handle_operation(&mut connection, settings, engine, event_engine, delegation.as_ref(), &authority_request, operation, &mut compact),
                    Err(error) => Err(QueryResponse::request_error(authority_request.id, error.code(), error.to_string()).into()),
                }
            }
            Err(error) => {
                let code = match &error {
                    ProtocolError::InvalidMessagePackDecode(_) => "protocol.invalid_msgpack",
                    ProtocolError::UnsupportedVersion { .. } => "protocol.unsupported_version",
                    ProtocolError::MessageTooLarge { .. } => "protocol.payload_too_large",
                    ProtocolError::InvalidPayloadLength { .. } => "protocol.invalid_payload_length",
                    ProtocolError::EmptyOperation | ProtocolError::EmptyQuery => {
                        "protocol.invalid_message"
                    }
                    ProtocolError::InvalidMessagePackEncode(_)
                    | ProtocolError::InvalidWireProjection(_)
                    | ProtocolError::InvalidMessageKind { .. } => "protocol.invalid_message",
                    ProtocolError::UnsafeJavaScriptInteger { .. } => {
                        "protocol.unsafe_javascript_integer"
                    }
                };
                debug::log( DebugTopic::Protocol, Some(connection_id), format!("msgpack decode failed error={error}"), );
                Err(QueryResponse::protocol_error(code, error.to_string()).into())
            }
        };
        match handled {
            Ok(()) => {}
            Err(Stop::Reply(response)) => write_response(&mut connection.writer, &response)?,
            Err(Stop::Connection(error)) => return Err(error),
        }
        let wire_us = elapsed_micros(wire_started);
        if debug::timing_enabled() {
            debug::log( DebugTopic::Executor, Some(connection_id), format!( "request_id={} decode_us={decode_us} wire_us={wire_us} total_us={}", request_id.map_or_else(|| "-".to_owned(), |id| id.to_string()), elapsed_micros(request_started) ), );
        }

        if settings.import_metrics && compact {
            eprintln!(
                "ogd_import request_id={} request_bytes={} decode_us={} wire_us={} total_us={}",
                request_id.map_or_else(|| "-".to_owned(), |id| id.to_string()),
                payload.len(),
                decode_us,
                wire_us,
                elapsed_micros(request_started),
            );
        }
    }
}

/// The per-connection state handlers may read or change.
pub(super) struct Connection {
    pub(super) id: u64,
    pub(super) writer: TcpStream,
    pub(super) reader: BufReader<TcpStream>,
    pub(super) subscription: Option<EventSubscription>,
    pub(super) authentication: ConnectionAuth,
}

impl Connection {
    /// Makes the connection wake up often enough to deliver subscribed events.
    pub(super) fn subscribe(&mut self, active: EventSubscription) -> Result<u64, ConnectionError> {
        let subscription_id = active.id();
        self.subscription = Some(active);
        self.reader.get_mut().set_read_timeout(Some(Duration::from_millis(250))).map_err(ConnectionError::ConfigureSocket)?;
        Ok(subscription_id)
    }
}

fn handle_operation(connection: &mut Connection, settings: &ConnectionSettings, engine: Option<&Engine>, event_engine: &EventEngine, delegation: Option<&GatewayDelegation>, authority_request: &OperationRequest, operation: RoutedOperation, compact: &mut bool) -> Handled {
    if let Some(delegation) = delegation {
        if let Some(provider) = operation.kind().provider_capability() {
            let expected_capability = if operation.kind().scope() == OperationScope::Authority {
                ServiceCapability::Auth.as_str()
            } else {
                provider.as_str()
            };
            if expected_capability != delegation.capability {
                reject!(operation.id(), "delegation.capability_mismatch", format!(
                    "delegated capability {:?} cannot execute operation {:?}; expected {:?}",
                    delegation.capability,
                    operation.kind().name(),
                    expected_capability,
                ));
            }
        }
    }
    ensure_routed_static_authorized(settings, engine, connection.authentication.principal(), &operation)?;
    if operation.kind().scope() == OperationScope::Authority
        && forward_authority_operation(&mut connection.writer, settings, connection.authentication.principal(), authority_request)?
    {
        return Ok(());
    }
    // Every routed operation belongs to a compiled provider: a mode whose feature is
    // not compiled falls to the routing error.
    match operation.execution_mode() {
        #[cfg(feature = "database")]
        ExecutionMode::Query => super::query_ops::handle_query_operation(connection, settings, engine.expect("database operation routed without db-engine"), event_engine, delegation, operation, compact),
        #[cfg(feature = "auth")]
        ExecutionMode::Authentication => super::auth_ops::handle_authentication_operation(connection, settings, engine.expect("auth operation routed without db-engine"), event_engine, operation),
        #[cfg(feature = "events")]
        ExecutionMode::Subscription => super::query_ops::handle_subscription_operation(connection, settings, event_engine, operation),
        #[cfg(any(feature = "files", feature = "data-import"))]
        ExecutionMode::File => super::file_ops::handle_file_operation(connection, settings, engine, delegation, operation),
        ExecutionMode::Standard => handle_standard_operation(&mut connection.writer, settings, engine, event_engine, &connection.authentication, delegation, operation),
        #[allow(unreachable_patterns)]
        _ => Err(routing_error(operation.id(), "operation routed without its compiled provider feature").into()),
    }
}

fn handle_standard_operation(writer: &mut TcpStream, settings: &ConnectionSettings, engine: Option<&Engine>, event_engine: &EventEngine, authentication: &ConnectionAuth, delegation: Option<&GatewayDelegation>, operation: RoutedOperation) -> Handled {
    #[cfg(feature = "db-engine")]
    let database = engine.expect("database-backed operation routed without db-engine");
    match operation.handler() {
        HandlerKind::Core => super::core_ops::handle_core_operation(writer, settings, engine, authentication, operation),
        #[cfg(feature = "llm")]
        HandlerKind::Llm => super::core_ops::handle_llm_operation(writer, settings, authentication, delegation, operation),
        #[cfg(feature = "agent")]
        HandlerKind::Agent => super::core_ops::handle_agent_operation(writer, settings, engine, authentication, delegation, operation),
        #[cfg(feature = "database")]
        HandlerKind::Collections | HandlerKind::Storage | HandlerKind::Backup => super::core_ops::handle_database_operation(writer, settings, database, authentication, delegation, operation),
        #[cfg(feature = "auth")]
        HandlerKind::Identity | HandlerKind::Device => super::auth_ops::handle_identity_operation(writer, settings, database, event_engine, authentication, operation),
        #[cfg(feature = "database")]
        HandlerKind::Permission | HandlerKind::Sharing | HandlerKind::Place => super::place_ops::handle_place_operation(writer, settings, database, authentication, operation),
        #[cfg(feature = "database")]
        HandlerKind::App => super::place_ops::handle_app_operation(writer, settings, database, authentication, operation),
        _ => Err(routing_error(operation.id(), "execution mode and handler domain diverged").into()),
    }
}
