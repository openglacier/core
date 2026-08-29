//! OG daemon entry point.
#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{
    env, thread, fs, error::Error, fmt::{self, Display, Formatter}, io::{self, BufReader, Read, Write},
    net::{TcpListener, TcpStream}, path::{Path, PathBuf}, process::{Command, ExitCode, Stdio}, collections::HashMap, sync::{ atomic::{AtomicU64, Ordering}, Arc, Mutex },
    time::{Duration, Instant},
};
use og_core::access::{
    auth::{validate_ed25519_public_key, ConnectionAuth, DeviceCredential, DEFAULT_CHALLENGE_TTL},
    bootstrap::BootstrapAdmin,
    identity_file::{self, IdentityCredential},
    authorization::{
        quote_query_string as quote_authorization_string, AuthorizationAction, AuthorizationMode,
        AuthorizationRequest, QueryAccess,
    },
    place::{
        parse_sharing_permission, sharing_permission, ExecutionContext, PlaceRole, PublicAccess,
        RequestedExecutionContext,
    },
};
use og_core::{
    debug::{self, DebugTopic},
    helpers::{decode_base64, document_to_json, elapsed_micros, encode_base64, unix_time_millis},
    engine::Engine, files::{FileEntry, FileId, FileRange, FileStore, FileStoreEntry, FileStoreError, FileWrite, NativeFileStore, StoreId}, Principal, backup,
    event_engine::{EventEngine, EventSubscription},
    memory::{MemoryClass, MemoryGovernor, MemoryProfileConfig, WorkloadClass},
    operation::{
        decode_operation_request, AccessPolicy, ExecutionMode, ServiceCapability, ServiceCapabilities, AppCreateInput, AuthBeginInput, AuthEnrollBeginInput, ClassicAuthLoginInput, ClassicAuthRegisterInput, ChallengeSignatureInput, DeviceRegisterInput, DeviceRenameInput, DeviceRevokeInput, EventsSubscribeInput, IdentityOpenInput, IdentityRegisterInput, IdentityRenewInput, PasswordInput, QueryExecuteInput, QueryContextResolveInput, AppDeleteInput, AppIdInput,
        AppInstanceCreateInput, AppInstanceRemoveInput, AppUpdateInput, DataAnalyzeInput, DataImportInput, DataWorkerRunInput, DataMappingSaveInput, Audience, BackupNameInput,
        CollectionsListInput, FileEntryInput, FileListInput, FileMkdirInput, FileMoveInput,
        FileReadInput, FileScopeInput, FileVersionInput, FileVersionReadInput, FileWriteInput,
        BackupRestoreInput, HandlerKind, OperationKind, OperationResponse, OperationRouter, PermissionGrantInput,
        PermissionRevokeInput, PlaceAccessRemoveInput, PlaceAccessSetInput, PlaceCreateInput, PlacePublicSetInput,
        PlaceDeleteInput, PlaceIdInput, PlaceUpdateInput, PlaceResourceSetInput, PlaceResourceRemoveInput, Routed, RoutedOperation, SharingCreateInput,
        SharingDeleteInput, SharingUpdateInput,
    },
    protocol::{
        encode_message, ensure_payload_size, MessageKind, ProtocolError, QueryRequest,
        QueryResponse, RequestId, StreamResponse, WireError, LENGTH_PREFIX_BYTES,
        MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, PROTOCOL_VERSION,
    },
    query::{
        parse as parse_query, value_expression_runtime, vcollections, DocumentScope, PlannerPipeline,
        QueryRuntime, QueryRuntimeMaterializationExt, ScanPlanLowerer,
    },
    storage::{GlacierStorage, MemoryStorage, StorageEngine, StorageError, UuidV7Generator},
    Document, Number, Value,
};

use argon2::{Algorithm, Argon2, Params, Version};
use serde::{ ser::{Error as SerializeError, SerializeMap, SerializeSeq}, Deserialize, Serialize, Serializer, };
use serde_json::Value as JsonValue;
use tungstenite::{connect as websocket_connect, Message as WebSocketMessage, WebSocket, stream::MaybeTlsStream};

const DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:7878";
const DEFAULT_NODE_CAPABILITIES: &str = "auth,database,files,events";
const DEFAULT_READ_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_WRITE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_STORAGE_BACKEND: &str = "memory";
const DEFAULT_STORAGE_PATH: &str = "data/ogd.glacier";
const DEFAULT_FILES_STORE_ID: &str = "native";
const DEFAULT_ENROLLMENT_MODE: &str = "open";
const CLASSIC_AUTH_MIN_PASSWORD_BYTES: usize = 12;
const CLASSIC_AUTH_MEMORY_KIB: u32 = 64 * 1024;
const CLASSIC_AUTH_ITERATIONS: u32 = 3;
const CLASSIC_AUTH_LANES: u32 = 1;
const BUILTIN_APPS_JSON: &str = include_str!("../../apps.json");
const SYSTEM_ASSISTANT_APP_ID: &str = "system.assistant";
const SYSTEM_ASSISTANT_APP_NAME: &str = "Assistant";
const SYSTEM_ASSISTANT_APP_VERSION: &str = "1.0.0";
const SYSTEM_PROJECTS_APP_ID: &str = "system.projects";
const SYSTEM_PROJECTS_APP_NAME: &str = "Projects";
const SYSTEM_PROJECTS_APP_VERSION: &str = "1.0.0";
const SYSTEM_RESOURCES_APP_ID: &str = "system.ressources";
const SYSTEM_RESOURCES_APP_NAME: &str = "Resources";
const SYSTEM_RESOURCES_APP_VERSION: &str = "1.0.0";
const SYSTEM_CALLS_APP_ID: &str = "system.calls";
const SYSTEM_CALLS_APP_NAME: &str = "Calls";
const SYSTEM_CALLS_APP_VERSION: &str = "1.0.0";
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BuiltinApp {
    app_id: String,
    name: String,
    version: String,
    definition: JsonValue,
}

fn main() -> ExitCode { match run() { Ok(()) => ExitCode::SUCCESS, Err(error) => { eprintln!("openglacier: {error}"); ExitCode::FAILURE } } }

fn run() -> Result<(), DaemonError> {
    let configuration = Configuration::from_environment()?;
    let version = env!("CARGO_PKG_VERSION");
    let gateway_endpoint = gateway_endpoint(&configuration.bind_address);
    let listener_bind_address = if gateway_endpoint.is_some() {
        configuration.local_bind_address.as_deref()
    } else {
        Some(configuration.bind_address.as_str())
    };
    let listener = match listener_bind_address {
        Some(address) => Some(TcpListener::bind(address).map_err(|source| DaemonError::Bind {
            address: address.to_owned(),
            source,
        })?),
        None => None,
    };

    let local_address = match (gateway_endpoint.as_ref(), listener.as_ref()) {
        (Some(_), Some(listener)) => format!(
            "outbound {}, local {}",
            configuration.bind_address,
            listener.local_addr().map_err(DaemonError::LocalAddress)?
        ),
        (Some(_), None) => format!("outbound {}", configuration.bind_address),
        (None, Some(listener)) => listener.local_addr().map_err(DaemonError::LocalAddress)?.to_string(),
        (None, None) => unreachable!("standalone ogd always has a listener"),
    };

    let (engine_value, glacier_storage) = build_engine(&configuration)?;
    let engine = Arc::new(engine_value);
    if configuration.node_capabilities.contains(ServiceCapability::Files) {
        fs::create_dir_all(&configuration.files_path).map_err(|source| DaemonError::PrepareStorageDirectory {
            path: configuration.files_path.clone(),
            source,
        })?;
    }
    debug::log(DebugTopic::Core, None, format!(
        "starting storage={} bind={} capabilities={}",
        configuration.storage_backend.as_str(),
        configuration.bind_address,
        configuration.node_capabilities.names().join(","),
    ));
    start_debug_memory_reporter(Arc::clone(&engine));
    let node_credential = if gateway_endpoint.is_some() {
        load_node_identity_credential(&configuration)?
    } else {
        None
    };
    if configuration.node_capabilities.contains(ServiceCapability::Auth) {
        if gateway_endpoint.is_some() {
            if let Some(credential) = node_credential.as_ref() {
                ensure_node_device_credential(&engine, credential)?;
            }
        } else {
            bootstrap_admin_if_needed(&engine, &configuration)?;
        }
    }
    if configuration.node_capabilities.contains(ServiceCapability::Database) {
        bootstrap_apps_if_needed(&engine)?;
    }
    let operation_router = Arc::new(OperationRouter::for_capabilities(configuration.node_capabilities));
    let event_engine = Arc::new(EventEngine::default());
    let _heartbeat = if configuration.node_capabilities.contains(ServiceCapability::Events) {
        configuration.heartbeat_interval.map(|interval| event_engine.start_heartbeat(interval))
    } else {
        None
    };
    if configuration.node_capabilities.contains(ServiceCapability::Events) {
        debug::log( DebugTopic::Events, None, format!( "event engine started capacity=default heartbeat={}", configuration .heartbeat_interval .map(|value| format!("{}ms", value.as_millis())) .unwrap_or_else(|| "disabled".to_owned()) ), );
        event_engine.publish_global("core.started", serde_json::json!({ "daemon": "ogd" }));
        start_event_outbox_worker(Arc::clone(&engine), Arc::clone(&event_engine));
    } else {
        debug::log(DebugTopic::Events, None, "event service disabled by OGD_NODE_CAPABILITIES");
    }
    let connection_settings = Arc::new(ConnectionSettings {
        read_timeout: configuration.read_timeout,
        write_timeout: configuration.write_timeout,
        import_metrics: configuration.import_metrics,
        authorization_mode: configuration.authorization_mode,
        enrollment_mode: configuration.enrollment_mode.clone(),
        classic_auth_enabled: configuration.classic_auth_enabled,
        classic_auth_attempts: Mutex::new(HashMap::new()),
        authenticated_keepalive: configuration.authenticated_keepalive
            && configuration.node_capabilities.contains(ServiceCapability::Events),
        backup_path: configuration.backup_path.clone(),
        instance_id: configuration.instance_id.clone(),
        storage_backend: configuration.storage_backend,
        glacier_storage,
        files_path: configuration.files_path.clone(),
        service_capabilities: configuration.node_capabilities,
    });
    let with_engine = configuration.storage_backend.as_str();
    let with_storage_path = match configuration.storage_backend { StorageBackend::Memory => "virtual".to_owned(), StorageBackend::Glacier => configuration.storage_path.display().to_string(), };
    let with_modules = match configuration.storage_backend {
        _default => {
            let metrics = if configuration.import_metrics { ", metrics enabled" } else { "" };
            let query_debug = if configuration.debug_query { ", query instrumentation enabled" } else { "" };
            let authorization = format!( ", authorization {}{}", configuration.authorization_mode.as_str(), query_debug );
            format!(
                "memory limit {}, profile {}, runtime reserve {}, managed budget {}, planner cache {}, operation budget {}, query budget {}, import budget {}, heavy concurrency {}{}",
                format_bytes(configuration.memory_profile.process_limit_bytes),
                configuration.memory_profile.effective_profile_label(),
                format_bytes(Some(configuration.memory_profile.runtime_reserve_bytes)),
                format_bytes(configuration.memory_profile.managed_budget_bytes),
                format_bytes(Some(configuration.memory_profile.planner_cache_bytes)),
                format_bytes(Some(configuration.memory_profile.operation_budget_bytes)),
                format_bytes(Some(configuration.memory_profile.query_budget_bytes)),
                format_bytes(Some(configuration.memory_profile.import_budget_bytes)),
                format_bytes(Some(configuration.memory_profile.max_concurrent_heavy)),
                format!("{authorization}{metrics}"),
            )
        }
    };
    const MODULE_LINE_COUNT: usize = 14;

    let mut module_lines = std::array::from_fn::<String, MODULE_LINE_COUNT, _>(|_| String::new());

    for (index, module) in with_modules
        .split(',')
        .map(str::trim)
        .filter(|module| !module.is_empty())
        .take(MODULE_LINE_COUNT)
        .enumerate()
    {
        module_lines[index] = format!("+ {:<18}", module);
    }

    let [with_modules1, with_modules2, with_modules3, with_modules4, with_modules5, with_modules6, with_modules7, with_modules8, with_modules9, with_modules10, with_modules11, with_modules12, with_modules13, with_modules14] =
        module_lines;
    println!(
        "                                          
                                          
                                          
               ++++++#########            
           +++++++#############           
        ++++++++######++++++###           
      ++++++++######     +++##+++         
     ++++++  #####          ++++++        openglacier daemon version {version}
    ++++++ #####             ++++++       
   ++++++ #####               ++++++      Copyright (c) 2026 openglacier.org
   +++++ #####                ######      under the MIT licence
  ++++++#####          #############+     
  +++++#####     ###################+     storage {with_engine} ({with_storage_path})
  +++++####    ##########    #######+     transport {local_address}
  ++++####      ####        ########+     
  ++++####                 ####+###*+     {with_modules1}
   +++####                #### ####+      {with_modules2}
   ++####                #####+####+      {with_modules3}
    +####               ##### +#####      {with_modules4}
    +####+             #####+++#####      {with_modules5}
     #####++         ##### ++++#####      {with_modules6}
      ####++++++   ######++++++#####      {with_modules7}
      #######++########+++++++ #####      {with_modules8}
       ##############++++++    #####      {with_modules9}
          ########             #####      {with_modules10}
                                 ###      {with_modules11}
                                          {with_modules12}
                                          {with_modules13}
                                          {with_modules14}
"
    );
    if let Some(gateway_endpoint) = gateway_endpoint {
        if let Some(listener) = listener {
            spawn_listener_thread(
                listener,
                Arc::clone(&engine),
                Arc::clone(&operation_router),
                Arc::clone(&event_engine),
                Arc::clone(&connection_settings),
                "ogd-local-listener",
            )?;
        }
        run_gateway_node(
            gateway_endpoint,
            &configuration,
            node_credential,
            engine,
            operation_router,
            event_engine,
            connection_settings,
        )
    } else {
        let listener = listener.expect("standalone ogd listener");
        for accepted in listener.incoming() {
            match accepted {
                Ok(stream) => {
                    spawn_serving_connection(
                        stream,
                        Arc::clone(&engine),
                        Arc::clone(&operation_router),
                        Arc::clone(&event_engine),
                        Arc::clone(&connection_settings),
                        "ogd-client",
                    )?;
                }
                Err(error) => eprintln!("ogd accept error: {error}"),
            }
        }
        Ok(())
    }
}

fn spawn_listener_thread(
    listener: TcpListener,
    engine: Arc<Engine>,
    operation_router: Arc<OperationRouter>,
    event_engine: Arc<EventEngine>,
    settings: Arc<ConnectionSettings>,
    thread_name: &str,
) -> Result<(), DaemonError> {
    let name = thread_name.to_owned();
    thread::Builder::new().name(name.clone()).spawn(move || {
        for accepted in listener.incoming() {
            match accepted {
                Ok(stream) => {
                    if let Err(error) = spawn_serving_connection(
                        stream,
                        Arc::clone(&engine),
                        Arc::clone(&operation_router),
                        Arc::clone(&event_engine),
                        Arc::clone(&settings),
                        "ogd-local-client",
                    ) {
                        eprintln!("{name}: cannot spawn client connection: {error}");
                    }
                }
                Err(error) => eprintln!("{name} accept error: {error}"),
            }
        }
    }).map_err(DaemonError::SpawnConnectionThread)?;
    Ok(())
}

fn spawn_serving_connection(
    stream: TcpStream,
    engine: Arc<Engine>,
    operation_router: Arc<OperationRouter>,
    event_engine: Arc<EventEngine>,
    settings: Arc<ConnectionSettings>,
    thread_name: &str,
) -> Result<(), DaemonError> {
    let name = thread_name.to_owned();
    thread::Builder::new().name(name).spawn(move || {
        let peer = stream.peer_addr().ok();
        let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        debug::log(
            DebugTopic::Network,
            Some(connection_id),
            format!(
                "connection peer={}",
                peer.map_or_else(|| "unknown".to_owned(), |value| value.to_string())
            ),
        );
        if let Err(error) = serve_connection(
            connection_id,
            stream,
            &engine,
            &operation_router,
            &event_engine,
            &settings,
            None,
        ) {
            debug::log(
                DebugTopic::Network,
                Some(connection_id),
                format!("connection error: {error}"),
            );
            eprintln!("ogd connection: {error}");
        }
        debug::log(DebugTopic::Network, Some(connection_id), "disconnect");
    }).map_err(DaemonError::SpawnConnectionThread)?;
    Ok(())
}


fn gateway_endpoint(bind: &str) -> Option<String> {
    fn normalize(authority: &str, secure: bool) -> Option<String> {
        let authority = authority.trim().trim_end_matches('/');
        if authority.is_empty() { return None; }
        let scheme = if secure { "wss" } else { "ws" };
        Some(format!("{scheme}://{authority}/node"))
    }
    if let Some(raw) = bind.strip_prefix("gw+insecure://") { return normalize(raw, false); }
    bind.strip_prefix("gw://").and_then(|raw| normalize(raw, true))
}

type NodeWebSocket = WebSocket<MaybeTlsStream<TcpStream>>;

fn write_node_message(websocket: &mut NodeWebSocket, value: &JsonValue) -> Result<(), String> {
    let payload = rmp_serde::to_vec_named(value).map_err(|error| error.to_string())?;
    websocket.send(WebSocketMessage::Binary(payload.into())).map_err(|error| error.to_string())
}

fn read_node_message(websocket: &mut NodeWebSocket) -> Result<Option<JsonValue>, String> {
    loop {
        match websocket.read() {
            Ok(WebSocketMessage::Binary(payload)) => {
                return rmp_serde::from_slice::<JsonValue>(&payload).map(Some).map_err(|error| error.to_string());
            }
            Ok(WebSocketMessage::Close(_)) => return Ok(None),
            Ok(WebSocketMessage::Ping(payload)) => {
                websocket.send(WebSocketMessage::Pong(payload)).map_err(|error| error.to_string())?;
            }
            Ok(WebSocketMessage::Pong(_)) => {}
            Ok(WebSocketMessage::Text(_)) => return Err("node control channel requires binary MessagePack frames".to_owned()),
            Ok(_) => {}
            Err(error) => return Err(error.to_string()),
        }
    }
}

fn configure_websocket_polling(websocket: &mut NodeWebSocket) -> io::Result<()> {
    let timeout = Some(Duration::from_millis(25));
    match websocket.get_mut() {
        MaybeTlsStream::Plain(stream) => stream.set_read_timeout(timeout),
        MaybeTlsStream::NativeTls(stream) => stream.get_mut().set_read_timeout(timeout),
        _ => Ok(()),
    }
}

fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let client = TcpStream::connect(address)?;
    let (server, _) = listener.accept()?;
    Ok((server, client))
}

fn bridge_websocket_channel(mut websocket: NodeWebSocket, mut local: TcpStream) -> Result<(), String> {
    local.set_nonblocking(true).map_err(|error| error.to_string())?;
    configure_websocket_polling(&mut websocket).map_err(|error| error.to_string())?;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        loop {
            match local.read(&mut buffer) {
                Ok(0) => { let _ = websocket.close(None); return Ok(()); }
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

fn run_gateway_node(
    endpoint: String,
    configuration: &Configuration,
    node_credential: Option<IdentityCredential>,
    engine: Arc<Engine>,
    operation_router: Arc<OperationRouter>,
    event_engine: Arc<EventEngine>,
    settings: Arc<ConnectionSettings>,
) -> Result<(), DaemonError> {
    let capabilities = configuration.node_capabilities.names();
    loop {
        debug::log(DebugTopic::Gateway, None, format!("connecting node fabric endpoint={endpoint}"));
        let (mut control, _) = match websocket_connect(endpoint.as_str()) {
            Ok(connection) => connection,
            Err(error) => {
                eprintln!("ogd gateway connect {endpoint}: {error}; retrying");
                thread::sleep(Duration::from_secs(2));
                continue;
            }
        };
        let issued_at = unix_time_millis();
        let (node_id, identity_id, device_id, public_key, signature) = if let Some(credential) = node_credential.as_ref() {
            // A Node is a Device of an Identity. The device identifier is therefore
            // the stable Node identifier exposed to the Gateway and governance layer.
            let node_id = credential.device_id.clone();
            let proof = format!("og.node.hello.v1\n{}\n{}\n{}\n{}", node_id, credential.identity_id, credential.device_id, issued_at);
            (node_id, Some(credential.identity_id.clone()), Some(credential.device_id.clone()), Some(credential.public_key.clone()), Some(credential.sign_base64(proof.as_bytes())))
        } else {
            // Legacy/unverified nodes keep instance_id as their transport identifier.
            (configuration.instance_id.clone(), configuration.node_identity.clone(), None, None, None)
        };
        let hello = serde_json::json!({
            "kind": "node.hello",
            "version": 1,
            "nodeId": node_id.clone(),
            "instanceId": configuration.instance_id.clone(),
            "identityId": identity_id,
            "deviceId": device_id,
            "publicKey": public_key,
            "issuedAt": issued_at,
            "signature": signature,
            "nodeVersion": env!("CARGO_PKG_VERSION"),
            "capabilities": capabilities.clone(),
            "role": configuration.node_role.clone(),
            "token": configuration.gateway_token.clone(),
        });
        if let Err(error) = write_node_message(&mut control, &hello) {
            eprintln!("ogd gateway hello {endpoint}: {error}");
            thread::sleep(Duration::from_secs(2));
            continue;
        }
        loop {
            let message = match read_node_message(&mut control) {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(error) => { eprintln!("ogd gateway control: {error}"); break; }
            };
            match message.get("kind").and_then(JsonValue::as_str) {
                Some("node.accepted") => {
                    debug::log(DebugTopic::Gateway, None, format!("node accepted endpoint={endpoint}"));
                }
                Some("node.open") => {
                    let Some(channel_id) = message.get("channelId").and_then(JsonValue::as_str).map(str::to_owned) else { continue; };
                    let delegated_identity_id = message.get("identityId").and_then(JsonValue::as_str).map(str::to_owned);
                    let delegated_device_id = message.get("deviceId").and_then(JsonValue::as_str).map(str::to_owned);
                    let delegated_place_id = message.get("placeId").and_then(JsonValue::as_str).map(str::to_owned);
                    let delegated_capability = message.get("capability").and_then(JsonValue::as_str).map(str::to_owned);
                    let delegated_app_instance_id = message.get("appInstanceId").and_then(JsonValue::as_str).map(str::to_owned);
                    let delegated_place_role = message.get("placeRole").and_then(JsonValue::as_str).and_then(PlaceRole::parse);
                    let endpoint = endpoint.clone();
                    let engine = Arc::clone(&engine);
                    let operation_router = Arc::clone(&operation_router);
                    let event_engine = Arc::clone(&event_engine);
                    let settings = Arc::clone(&settings);
                    let channel_node_id = node_id.clone();
                    thread::Builder::new().name("ogd-gateway-channel".to_owned()).spawn(move || {
                        let (mut websocket, _) = match websocket_connect(endpoint.as_str()) {
                            Ok(connection) => connection,
                            Err(error) => { eprintln!("ogd gateway channel connect {endpoint}: {error}"); return; }
                        };
                        let hello = serde_json::json!({
                            "kind": "node.channel",
                            "version": 1,
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
                        let delegation = match (delegated_identity_id, delegated_device_id, delegated_place_id, delegated_capability) {
                            (Some(identity_id), Some(device_id), Some(place_id), Some(capability)) => Some(GatewayDelegation {
                                principal: Principal::Identity { identity_id, device_id }, place_id, capability,
                                app_instance_id: delegated_app_instance_id, place_role: delegated_place_role,
                            }),
                            _ => None,
                        };
                        if let Err(error) = serve_connection(
                            connection_id, core_stream, &engine, &operation_router, &event_engine, &settings, delegation,
                        ) {
                            eprintln!("ogd gateway channel: {error}");
                        }
                    }).map_err(DaemonError::SpawnConnectionThread)?;
                }
                _ => {}
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn load_node_identity_credential(configuration: &Configuration) -> Result<Option<IdentityCredential>, DaemonError> {
    match (&configuration.node_identity_file, &configuration.node_identity_password) {
        (Some(path), Some(password)) => identity_file::load(path, password.as_bytes())
            .map(Some)
            .map_err(DaemonError::NodeIdentity),
        (Some(_), None) => Err(DaemonError::NodeIdentityPasswordMissing),
        _ => Ok(None),
    }
}

/// Ensures that the credential used by this client/node to authenticate to its master can
/// also authenticate to the local control listener.
///
/// This deliberately creates only the public `_devices` record. It does not bootstrap an
/// identity, grant permissions, create a Place, or persist private key material. Existing
/// records are never rewritten: an unexpected identity/key/state collision is treated as an
/// inconsistent local store and fails startup.
fn ensure_node_device_credential(engine: &Engine, credential: &IdentityCredential) -> Result<(), DaemonError> {
    let lookup = format!(
        "on _devices | where deviceId == {} | limit 1",
        query_string(&credential.device_id),
    );
    let response = execute_request(engine, QueryRequest::new(0, lookup));
    match response {
        QueryResponse::Ok { documents, .. } => {
            if let Some(document) = documents.first() {
                let text = |field: &str| document.get(field).and_then(JsonValue::as_str);
                let expected = text("identityId") == Some(credential.identity_id.as_str())
                    && text("publicKey") == Some(credential.public_key.as_str())
                    && text("algorithm") == Some("ed25519")
                    && text("encoding") == Some("spki-der")
                    && text("state") == Some("active");
                if expected {
                    return Ok(());
                }
                return Err(DaemonError::NodeDeviceCredentialConflict {
                    device_id: credential.device_id.clone(),
                });
            }
        }
        // A fresh client store may not have `_devices` yet. Reads against a missing
        // collection are therefore equivalent to "credential absent" here. The insert
        // below creates/populates the collection through the normal Core query path. If
        // that insert fails, we still surface a real local credential-state error.
        QueryResponse::Error { .. } => {}
    }

    let created_at = unix_time_millis();
    let insert = format!(
        "on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {created_at}}}",
        query_string(&credential.device_id),
        query_string(&credential.identity_id),
        query_string(&credential.public_key),
    );
    if !execute_request(engine, QueryRequest::new(0, insert)).is_ok() {
        return Err(DaemonError::NodeDeviceCredentialState);
    }
    debug::log(
        DebugTopic::Auth,
        None,
        format!(
            "registered local node device credential identity={} device={}",
            credential.identity_id, credential.device_id,
        ),
    );
    Ok(())
}

fn bootstrap_admin_if_needed( engine: &Engine, configuration: &Configuration, ) -> Result<(), DaemonError> {
    let response = execute_request(engine, QueryRequest::new(0, "on _identities | count"));
    let empty = match response {
        QueryResponse::Ok { documents, .. } => documents
            .first()
            .and_then(|value| value.get("count"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) == 0,
        QueryResponse::Error { .. } => true,
    };
    if !empty { return Ok(()); }

    let password = configuration.bootstrap_password()?;
    let admin = BootstrapAdmin::generate().map_err(DaemonError::BootstrapIdentity)?;
    let staged = admin.stage(&configuration.bootstrap_admin_path, &password)
        .map_err(DaemonError::BootstrapIdentity)?;
    for query in admin.registration_queries() {
        if !execute_request(engine, QueryRequest::new(0, query)).is_ok() {
            return Err(DaemonError::BootstrapAdminState);
        }
    }
    BootstrapAdmin::commit(&staged, &configuration.bootstrap_admin_path)
        .map_err(DaemonError::BootstrapIdentity)?;
    Ok(())
}

fn valid_app_model_identifier(value: &str, allow_dots: bool) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else { return false; };
    if !(first == '_' || first.is_ascii_alphabetic()) { return false; }
    chars.all(|character| {
        character == '_'
            || character == '-'
            || (allow_dots && character == '.')
            || character.is_ascii_alphanumeric()
    })
}

fn validate_app_definition_model(definition: &JsonValue) -> Result<(), String> {
    let Some(model) = definition.get("model") else { return Ok(()); };
    let model = model.as_object().ok_or_else(|| "definition.model must be an object".to_owned())?;
    let collections = model.get("collections").and_then(JsonValue::as_object);
    let tables = model.get("tables").and_then(JsonValue::as_object);

    if model.get("collections").is_some() && collections.is_none() {
        return Err("definition.model.collections must be an object".to_owned());
    }
    if model.get("tables").is_some() && tables.is_none() {
        return Err("definition.model.tables must be an object".to_owned());
    }

    let empty = serde_json::Map::new();
    let collections = collections.unwrap_or(&empty);
    for (alias, declaration) in collections {
        if !valid_app_model_identifier(alias, false) {
            return Err(format!("invalid collection alias {alias:?}"));
        }
        let physical_name = declaration.as_str().or_else(|| declaration.get("name").and_then(JsonValue::as_str))
            .ok_or_else(|| format!("collection {alias:?} must declare a name"))?;
        if !valid_app_model_identifier(physical_name, true) {
            return Err(format!("collection {alias:?} has invalid Core name {physical_name:?}"));
        }
    }

    let tables = tables.unwrap_or(&empty);
    for (alias, table) in tables {
        if !valid_app_model_identifier(alias, false) {
            return Err(format!("invalid table alias {alias:?}"));
        }
        let table = table.as_object().ok_or_else(|| format!("table {alias:?} must be an object"))?;
        let collection = table.get("collection").and_then(JsonValue::as_str)
            .ok_or_else(|| format!("table {alias:?} must reference a collection"))?;
        if !collections.contains_key(collection) {
            return Err(format!("table {alias:?} references undeclared collection {collection:?}"));
        }
        if let Some(fields) = table.get("fields") {
            let fields = fields.as_array().ok_or_else(|| format!("table {alias:?}.fields must be an array"))?;
            let mut names = std::collections::BTreeSet::new();
            for field in fields {
                let name = field.get("name").and_then(JsonValue::as_str)
                    .ok_or_else(|| format!("table {alias:?} contains a field without a name"))?;
                if !valid_app_model_identifier(name, false) {
                    return Err(format!("table {alias:?} has invalid field name {name:?}"));
                }
                if !names.insert(name) {
                    return Err(format!("table {alias:?} declares field {name:?} more than once"));
                }
            }
        }
    }
    Ok(())
}

fn bootstrap_apps_if_needed(engine: &Engine) -> Result<(), DaemonError> {
    let mut apps: Vec<BuiltinApp> = serde_json::from_str(BUILTIN_APPS_JSON)
        .expect("embedded apps.json must be valid");
    if !apps.iter().any(|app| app.app_id == SYSTEM_ASSISTANT_APP_ID) {
        apps.push(BuiltinApp {
            app_id: SYSTEM_ASSISTANT_APP_ID.to_owned(),
            name: SYSTEM_ASSISTANT_APP_NAME.to_owned(),
            version: SYSTEM_ASSISTANT_APP_VERSION.to_owned(),
            definition: serde_json::json!({
                "id": SYSTEM_ASSISTANT_APP_ID,
                "name": SYSTEM_ASSISTANT_APP_NAME,
                "version": SYSTEM_ASSISTANT_APP_VERSION,
                "tone": "glacier",
                "hub": { "description": "Ask, explore and work with what this Place knows.", "meta": "Ready when you are", "mark": "○" },
                "kind": "system.assistant",
                "requires": { "capabilities": [] },
                "views": {
                    "tile": [{ "type": "section", "kicker": "Ready when you are", "title": "Assistant", "description": "Ask, explore and work with what this Place knows." }],
                    "full": [{ "type": "section", "kicker": "Assistant", "title": "Assistant", "description": "Ask, explore and work with what this Place knows." }]
                }
            }),
        });
    }
    if !apps.iter().any(|app| app.app_id == SYSTEM_PROJECTS_APP_ID) {
        apps.push(BuiltinApp {
            app_id: SYSTEM_PROJECTS_APP_ID.to_owned(),
            name: SYSTEM_PROJECTS_APP_NAME.to_owned(),
            version: SYSTEM_PROJECTS_APP_VERSION.to_owned(),
            definition: serde_json::json!({
                "id": SYSTEM_PROJECTS_APP_ID,
                "name": SYSTEM_PROJECTS_APP_NAME,
                "version": SYSTEM_PROJECTS_APP_VERSION,
                "tone": "sage",
                "hub": { "description": "Keep ongoing work, missions and shared context together.", "meta": "No active mission", "mark": "□" },
                "kind": "system.projects",
                "requires": { "capabilities": [] },
                "views": {
                    "tile": [{ "type": "section", "kicker": "No active mission", "title": "Projects", "description": "Keep ongoing work, missions and shared context together." }],
                    "full": [{ "type": "section", "kicker": "Projects", "title": "Projects", "description": "Keep ongoing work, missions and shared context together." }]
                }
            }),
        });
    }
    if !apps.iter().any(|app| app.app_id == SYSTEM_RESOURCES_APP_ID) {
        apps.push(BuiltinApp {
            app_id: SYSTEM_RESOURCES_APP_ID.to_owned(),
            name: SYSTEM_RESOURCES_APP_NAME.to_owned(),
            version: SYSTEM_RESOURCES_APP_VERSION.to_owned(),
            definition: serde_json::json!({
                "id": SYSTEM_RESOURCES_APP_ID,
                "name": SYSTEM_RESOURCES_APP_NAME,
                "version": SYSTEM_RESOURCES_APP_VERSION,
                "tone": "glacier",
                "hub": { "description": "See the things, services and systems available here.", "meta": "Connected by trust", "mark": "◇" },
                "kind": "system.ressources",
                "requires": { "capabilities": ["database", "files", "events"] },
                "views": {
                    "tile": [
                        { "type": "metric", "label": "Resources", "value": "Live", "detail": "Nodes & capabilities" }
                    ],
                    "full": [
                        { "type": "section", "kicker": "System", "title": "Resources", "description": "Govern the nodes and capabilities assigned to this Place." },
                        { "type": "section", "kicker": "Database", "title": "Primary & replicas", "description": "Resource assignments are managed by Place Owners." },
                        { "type": "section", "kicker": "Files", "title": "Storage providers", "description": "Choose which node provides file storage for this Place." },
                        { "type": "section", "kicker": "Nodes", "title": "Connected resources", "description": "Live node status will be resolved through the Gateway fabric." }
                    ]
                }
            }),
        });
    }
    if !apps.iter().any(|app| app.app_id == SYSTEM_CALLS_APP_ID) {
        apps.push(BuiltinApp {
            app_id: SYSTEM_CALLS_APP_ID.to_owned(),
            name: SYSTEM_CALLS_APP_NAME.to_owned(),
            version: SYSTEM_CALLS_APP_VERSION.to_owned(),
            definition: serde_json::json!({
                "id": SYSTEM_CALLS_APP_ID,
                "name": SYSTEM_CALLS_APP_NAME,
                "version": SYSTEM_CALLS_APP_VERSION,
                "tone": "glacier",
                "kind": "system.calls",
                "requires": { "capabilities": [] },
                "views": {
                    "tile": [
                        { "type": "metric", "label": "Calls", "value": "Audio · Video", "detail": "Call any OpenGlacier Identity by _id" }
                    ],
                    "full": [
                        { "type": "section", "kicker": "System", "title": "Calls", "description": "Peer-to-peer audio and video calls between OpenGlacier identities." }
                    ]
                }
            }),
        });
    }
    let now = unix_time_millis();
    for app in apps {
        if let Err(message) = validate_app_definition_model(&app.definition) {
            eprintln!("openglacier: invalid built-in App {}: {message}", app.app_id);
            return Err(DaemonError::BootstrapAppsState);
        }
        let app_id = query_string(&app.app_id);
        let active = format!("on _apps | where appId == {app_id} and state == \"active\" | count");
        let active = match execute_request(engine, QueryRequest::new(0, active)) {
            QueryResponse::Ok { documents, .. } => documents.first()
                .and_then(|value| value.get("count"))
                .and_then(JsonValue::as_u64)
                .unwrap_or(0) > 0,
            QueryResponse::Error { .. } => false,
        };
        if active {
            continue;
        }
        let definition = serde_json::to_string(&app.definition)
            .expect("embedded App definition serializes");
        let exists = match app_record_exists(engine, RequestId::Number(0), &app.app_id) {
            Ok(value) => value,
            Err(_) => return Err(DaemonError::BootstrapAppsState),
        };
        let query = if exists {
            format!(
                "on _apps | where appId == {app_id} | set name = {}, version = {}, definition = {definition}, state = \"active\", updatedBy = \"system\", updatedAt = {now}",
                query_string(&app.name), query_string(&app.version),
            )
        } else {
            format!(
                "on _apps | insert {{appId: {app_id}, name: {}, version: {}, definition: {definition}, createdBy: \"system\", state: \"active\", createdAt: {now}}}",
                query_string(&app.name), query_string(&app.version),
            )
        };
        if !execute_request(engine, QueryRequest::new(0, query)).is_ok() {
            return Err(DaemonError::BootstrapAppsState);
        }
    }
    Ok(())
}

fn build_engine( configuration: &Configuration, ) -> Result<(Engine, Option<Arc<GlacierStorage>>), DaemonError> {
    let governor = configuration.memory_limit_bytes.map_or_else(
        MemoryGovernor::unlimited,
        MemoryGovernor::with_process_limit,
    );
    let (storage, glacier_storage) = build_storage(configuration, governor.clone())?;
    let runtime = Arc::new(build_runtime()?);
    let lowerer = Arc::new(ScanPlanLowerer::new());

    Ok((
        Engine::new(storage, runtime, lowerer).with_memory_governor(governor),
        glacier_storage,
    ))
}

fn build_storage( configuration: &Configuration, governor: MemoryGovernor, ) -> Result<(Arc<dyn StorageEngine>, Option<Arc<GlacierStorage>>), DaemonError> {
    match configuration.storage_backend {
        StorageBackend::Memory => Ok((Arc::new(MemoryStorage::new()), None)),
        StorageBackend::Glacier => {
            prepare_storage_directory(&configuration.storage_path)?;
            GlacierStorage::open_governed(&configuration.storage_path, governor)
                .map(|storage| {
                    let storage = Arc::new(storage);
                    let engine_storage: Arc<dyn StorageEngine> = storage.clone();
                    (engine_storage, Some(storage))
                })
                .map_err(|source| DaemonError::OpenStorage {
                    path: configuration.storage_path.clone(),
                    source,
                })
        }
    }
}

fn prepare_storage_directory(path: &Path) -> Result<(), DaemonError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };

    if parent.as_os_str().is_empty() {
        return Ok(());
    }

    fs::create_dir_all(parent).map_err(|source| DaemonError::PrepareStorageDirectory {
        path: parent.to_path_buf(),
        source,
    })
}

fn build_runtime() -> Result<QueryRuntime, DaemonError> {
    value_expression_runtime()
        .map(|runtime| runtime.with_default_materialization("ogd"))
        .map_err(|error| DaemonError::Runtime(error.to_string()))
}

#[derive(Debug, Clone)]
struct GatewayDelegation {
    principal: Principal,
    place_id: String,
    capability: String,
    app_instance_id: Option<String>,
    place_role: Option<PlaceRole>,
}

fn serve_connection( connection_id: u64, stream: TcpStream, engine: &Engine, operation_router: &OperationRouter, event_engine: &EventEngine, settings: &ConnectionSettings, delegation: Option<GatewayDelegation>, ) -> Result<(), ConnectionError> {
    stream.set_read_timeout(Some(settings.read_timeout)).map_err(ConnectionError::ConfigureSocket)?;
    stream.set_write_timeout(Some(settings.write_timeout)).map_err(ConnectionError::ConfigureSocket)?;
    stream.set_nodelay(true).map_err(ConnectionError::ConfigureSocket)?;

    let mut writer = stream.try_clone().map_err(ConnectionError::CloneSocket)?;
    let mut reader = BufReader::new(stream);
    let mut payload = Vec::with_capacity(4096);
    let mut subscription: Option<EventSubscription> = None;
    let mut authentication = delegation.as_ref().map(|value| ConnectionAuth::from_principal(value.principal.clone())).unwrap_or_default();

    loop {
        drain_events(connection_id, &mut writer, subscription.as_ref(), authentication.principal())?;
        payload.clear();

        let bytes_read = match read_message(&mut reader, &mut payload) {
            Ok(bytes_read) => bytes_read,
            Err(ConnectionError::Read(error))
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                if subscription.is_some() {
                    drain_events(connection_id, &mut writer, subscription.as_ref(), authentication.principal())?;
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
            debug::log(
                DebugTopic::Protocol,
                Some(connection_id),
                format!(
                    "< message bytes={} encoding=msgpack {}",
                    payload.len(),
                    value
                ),
            );
        }

        let request_started = Instant::now();
        let decode_started = Instant::now();
        let decoded = decode_operation_request(&payload);
        let decode_us = elapsed_micros(decode_started);
        let mut request_id = None;
        let mut compact = false;
        let wire_started = Instant::now();
        match decoded {
            Ok(operation_request) => {
                request_id = Some(operation_request.id);
                debug::log(
                    DebugTopic::Router,
                    Some(connection_id),
                    format!(
                        "request id={} op={}",
                        operation_request.id, operation_request.op
                    ),
                );
                match operation_router.route(operation_request) {
                    Ok(operation) => {
                        if !ensure_routed_static_authorized(
                            &mut writer, settings, engine, authentication.principal(), &operation,
                        )? { continue; }
                    match operation.execution_mode() {
                        ExecutionMode::Query => {
                            if handle_query_operation(&mut writer, settings, engine, event_engine, &authentication, delegation.as_ref(), connection_id, operation, &mut compact)? { continue; }
                        }
                        ExecutionMode::Authentication => {
                            if handle_authentication_operation(&mut writer, &mut reader, settings, engine, event_engine, &mut authentication, connection_id, &mut subscription, operation)? { continue; }
                        }
                        ExecutionMode::Subscription => {
                            if handle_subscription_operation(&mut writer, &mut reader, settings, event_engine, &authentication, connection_id, &mut subscription, operation)? { continue; }
                        }
                        ExecutionMode::File => {
                            if handle_file_operation(&mut writer, &mut reader, settings, engine, &authentication, delegation.as_ref(), operation)? { continue; }
                        }
                        ExecutionMode::Standard => handle_standard_operation(&mut writer, settings, engine, &authentication, operation)?,
                    }
                    }
                    Err(error) => {
                        let response = QueryResponse::request_error(
                            request_id.expect("a decoded operation has an id"),
                            error.code(),
                            error.to_string(),
                        );
                        write_response(&mut writer, &response)?;
                    }
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
                debug::log(
                    DebugTopic::Protocol,
                    Some(connection_id),
                    format!("msgpack decode failed error={error}"),
                );
                let response = QueryResponse::protocol_error(code, error.to_string());
                write_response(&mut writer, &response)?;
            }
        }
        let wire_us = elapsed_micros(wire_started);
        if debug::timing_enabled() {
            debug::log(
                DebugTopic::Executor,
                Some(connection_id),
                format!(
                    "request_id={} decode_us={decode_us} wire_us={wire_us} total_us={}",
                    request_id.map_or_else(|| "-".to_owned(), |id| id.to_string()),
                    elapsed_micros(request_started)
                ),
            );
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



fn handle_query_operation( mut writer: &mut TcpStream, settings: &ConnectionSettings, engine: &Engine, event_engine: &EventEngine, authentication: &ConnectionAuth, delegation: Option<&GatewayDelegation>, connection_id: u64, operation: RoutedOperation, compact: &mut bool, ) -> Result<bool, ConnectionError> {
    macro_rules! reject_response { ($response:expr)=>{{write_response(&mut writer,&$response)?;return Ok(true);}}; }
    match operation {
                            RoutedOperation::QueryExecute(Routed { id, input: QueryExecuteInput { query, context } }) => {
                            debug::log(
                                DebugTopic::Query,
                                Some(connection_id),
                                format!("id={id} {}", query),
                            );

                            // Place/App instance context is optional for backward compatibility.
                            // When supplied, it is untrusted wire data until og-core resolves it
                            // against the authenticated principal and persisted system records.
                            let execution_context = match context {
                                Some(requested) => {
                                    let delegated_context = delegation.and_then(|value| {
                                        if value.capability == "database"
                                            && value.place_id == requested.place_id
                                            && value.app_instance_id.as_deref() == Some(requested.app_instance_id.as_str())
                                        {
                                            value.place_role.map(|place_role| ExecutionContext {
                                                principal: value.principal.clone(),
                                                place_id: requested.place_id.clone(),
                                                app_instance_id: requested.app_instance_id.clone(),
                                                place_role,
                                                public_access: None,
                                            })
                                        } else { None }
                                    });
                                    match delegated_context {
                                        Some(context) => Some(context),
                                        None => match resolve_query_execution_context(
                                            engine, id, authentication.principal(), requested,
                                            !settings.authorization_mode.is_enforced(),
                                        ) {
                                            Ok(context) => Some(context),
                                            Err(response) => reject_response!(response)
                                        },
                                    }
                                },
                                None => None,
                            };

                            let analyzed_access = QueryAccess::analyze(&query).ok();
                            let scoped_write_collection = analyzed_access.as_ref()
                                .filter(|access| access.action == AuthorizationAction::QueryWrite)
                                .map(|access| access.collection.clone());

                            // The resolved Place role is the capability ceiling for a scoped
                            // App query. Reads are allowed to every Place participant; writes are
                            // limited to Resident and Owner before the scoped grant is considered.
                            if let (Some(context), Some(access)) =
                                (execution_context.as_ref(), analyzed_access.as_ref())
                            {
                                if access.action == AuthorizationAction::QueryWrite
                                    && !context.place_role.can_write()
                                {
                                    write_place_role_denied(
                                        &mut writer,
                                        id,
                                        context,
                                        access.action,
                                        &access.collection,
                                    )?;
                                    event_engine.publish_global(
                                        "authorization.denied",
                                        serde_json::json!({
                                            "requestId": id,
                                            "action": access.action.as_str(),
                                            "resource": access.collection,
                                            "principal": authentication.principal(),
                                            "placeId": context.place_id,
                                            "appInstanceId": context.app_instance_id,
                                            "placeRole": context.place_role.as_str(),
                                        }),
                                    );
                                    return Ok(true);
                                }
                            }

                            if settings.authorization_mode.is_enforced() {
                                let access = match analyzed_access {
                                    Some(access) => access,
                                    None => {
                                        let response =
                                            execute_request(engine, QueryRequest::new(id, query));
                                        write_response(&mut writer, &response)?;
                                        return Ok(true);
                                    }
                                };

                                // A validated Place + AppInstance context is itself the grant for
                                // ordinary App collections. The physical executor applies the
                                // trusted DocumentScope below the query language, so this grant can
                                // never escape the requested Place/App instance. Place roles remain
                                // the capability ceiling: Member=read, Resident/Owner=read+write.
                                // System collections keep the explicit global permission model.
                                let scoped_app_collection = execution_context.is_some()
                                    && !access.collection.starts_with('_');
                                if !scoped_app_collection
                                    && !authorize_connection(
                                        engine,
                                        authentication.principal(),
                                        access.action,
                                        &access.collection,
                                    )
                                {
                                    write_authorization_denied(
                                        &mut writer,
                                        id,
                                        authentication.principal(),
                                        access.action,
                                        &access.collection,
                                    )?;
                                    event_engine.publish_global(
                                        "authorization.denied",
                                        serde_json::json!({
                                            "requestId": id,
                                            "action": access.action.as_str(),
                                            "resource": access.collection,
                                            "principal": authentication.principal(),
                                            "placeId": execution_context.as_ref().map(|context| &context.place_id),
                                            "appInstanceId": execution_context.as_ref().map(|context| &context.app_instance_id),
                                        }),
                                    );
                                    return Ok(true);
                                }
                            }
                            let request = QueryRequest::new(id, query);
                            *compact = request.query.contains("LOAD") || request.query.contains("load");
                            event_engine.publish_global(
                                "query.started",
                                serde_json::json!({
                                    "requestId": id,
                                    "principal": authentication.principal(),
                                    "context": execution_context.as_ref().map(|context| serde_json::json!({
                                        "placeId": context.place_id,
                                        "appInstanceId": context.app_instance_id,
                                        "placeRole": context.place_role.as_str(),
                                        "publicAccess": context.public_access.map(PublicAccess::as_str),
                                    })),
                                }),
                            );
                            // Scoped queries use the blocking executor so the trusted document
                            // scope is applied below query syntax. The legacy streaming path remains
                            // unchanged for unscoped queries.
                            let streaming = if execution_context.is_none() {
                                try_write_streaming_request(engine, &mut writer, &request)
                            } else {
                                Ok(false)
                            };
                            match streaming {
                                Ok(true) => {
                                    event_engine.publish_global(
                                        "query.finished",
                                        serde_json::json!({ "requestId": id }),
                                    );
                                }
                                Ok(false) => {
                                    let response = execute_request_scoped(
                                        engine,
                                        request,
                                        execution_context.as_ref(),
                                    );
                                    let ok = response.is_ok();
                                    write_response(&mut writer, &response)?;
                                    let event_type = if ok { "query.finished" } else { "query.failed" };
                                    event_engine.publish_global(
                                        event_type,
                                        serde_json::json!({ "requestId": id }),
                                    );
                                    if ok {
                                        if let (Some(context), Some(collection)) =
                                            (execution_context.as_ref(), scoped_write_collection.as_ref())
                                        {
                                            let audience = place_audience(engine, id, &context.place_id)
                                                .unwrap_or_else(|_| Audience::Global);
                                            publish_durable_event(
                                                engine,
                                                audience,
                                                "app.data.changed",
                                                serde_json::json!({
                                                    "placeId": context.place_id,
                                                    "appInstanceId": context.app_instance_id,
                                                    "collection": collection,
                                                }),
                                            );
                                        }
                                    }
                                }
                                Err(error) => {
                                    event_engine.publish_global("query.failed", serde_json::json!({ "requestId": id, "message": error.to_string() }));
                                    return Err(error);
                                }
                            }
                        }
                            _ => unreachable!("operation execution mode and routed variant diverged"),
                        }
    Ok(false)
}

fn handle_authentication_operation( mut writer: &mut TcpStream, reader: &mut BufReader<TcpStream>, settings: &ConnectionSettings, engine: &Engine, event_engine: &EventEngine, authentication: &mut ConnectionAuth, connection_id: u64, subscription: &mut Option<EventSubscription>, operation: RoutedOperation, ) -> Result<bool, ConnectionError> {
    macro_rules! reject { ($id:expr,$code:expr,$message:expr)=>{{write_response(&mut writer,&QueryResponse::request_error($id,$code,$message))?;return Ok(true);}}; }
    macro_rules! reject_response { ($response:expr)=>{{write_response(&mut writer,&$response)?;return Ok(true);}}; }
    macro_rules! identity_or_reject { ($id:expr,$message:expr)=>{{let Some(identity_id)=require_identity(&mut writer,authentication.principal(),$id,$message)? else {return Ok(true);};identity_id}}; }
    macro_rules! reply { ($id:expr,$data:expr $(,)?)=>{{write_operation_response(&mut writer,&OperationResponse::new($id,$data))?;}}; }
    macro_rules! query_documents_or_reject { ($id:expr,$query:expr)=>{{match execute_request(engine,QueryRequest::new($id,$query)){QueryResponse::Ok{documents,..}=>documents,response@QueryResponse::Error{..}=>reject_response!(response)}}}; }
    macro_rules! authenticated_device_or_reject { ($id:expr,$message:expr)=>{{match authentication.principal(){Principal::Identity{identity_id,device_id}=>(identity_id.clone(),device_id.clone()),Principal::Anonymous=>reject!($id,"authorization.required",$message)}}}; }
    macro_rules! or_reject { ($result:expr)=>{{match $result{Ok(value)=>value,Err(response)=>reject_response!(response)}}}; }
    macro_rules! execute_query_or_reject { ($id:expr,$query:expr)=>{{let response=execute_request(engine,QueryRequest::new($id,$query));if !response.is_ok(){reject_response!(response);}}}; }
    macro_rules! value_or_reject { ($id:expr,$result:expr,$code:expr)=>{{match $result{Ok(value)=>value,Err(error)=>reject!($id,$code,error.to_string())}}}; }
    match operation {
                            RoutedOperation::AuthBegin(Routed { id, input: AuthBeginInput { identity_id, device_id } }) => {
                            debug::log(
                                DebugTopic::Auth,
                                Some(connection_id),
                                format!("begin identity={identity_id} device={device_id}"),
                            );
                            match load_device_credential(engine, id, &identity_id, &device_id) {
                                Ok(credential) => {
                                    match authentication.begin(&credential, DEFAULT_CHALLENGE_TTL) {
                                        Ok(challenge) => reply!(id, serde_json::to_value(challenge)
                                                    .expect("challenge serializes"),),
                                        Err(error) => write_auth_error(&mut writer, id, error)?,
                                    }
                                }
                                Err(response) => write_response(&mut writer, &response)?,
                            }
                        }
                            RoutedOperation::AuthComplete(Routed { id, input: ChallengeSignatureInput { challenge_id, signature } }) => {
                            let Some((identity_id, device_id)) = pending_auth_subject(&authentication)
                            else {
                                reject!(id, "auth.failed", "no authentication challenge is pending");
                            };
                            match load_device_credential(engine, id, &identity_id, &device_id) {
                                Ok(credential) => match authentication.complete(
                                    &challenge_id,
                                    &signature,
                                    &credential,
                                ) {
                                    Ok(principal) => {
                                        debug::log(
                                            DebugTopic::Auth,
                                            Some(connection_id),
                                            format!("authenticated principal={principal:?}"),
                                        );
                                        event_engine.publish_global(
                                            "auth.authenticated",
                                            serde_json::json!({"identityId": identity_id, "deviceId": device_id}),
                                        );
                                        if settings.authenticated_keepalive && subscription.is_none() {
                                            let active = event_engine
                                                .subscribe(vec!["core.heartbeat".to_owned()]);
                                            let subscription_id = active.id();
                                            *subscription = Some(active);
                                            reader
                                                .get_mut()
                                                .set_read_timeout(Some(Duration::from_millis(250)))
                                                .map_err(ConnectionError::ConfigureSocket)?;
                                            debug::log(
                                                DebugTopic::Events,
                                                Some(connection_id),
                                                format!("authenticated keepalive subscribed subscription_id={subscription_id}"),
                                            );
                                        }
                                        reply!(id, serde_json::json!({"authenticated": true, "principal": principal}),);
                                    }
                                    Err(error) => write_auth_error(&mut writer, id, error)?,
                                },
                                Err(response) => write_response(&mut writer, &response)?,
                            }
                        }
                            RoutedOperation::AuthEnrollBegin(Routed { id, input: AuthEnrollBeginInput { identity_id, identity_public_key, device_id, device_public_key, token } }) => {
                            if !settings.enrollment_mode.allows(token.as_deref()) {
                                reject!(id, "auth.enrollment_denied", "identity enrollment is not allowed");
                            }
                            match authentication.begin_enrollment(
                                identity_id,
                                identity_public_key,
                                device_id,
                                device_public_key,
                                DEFAULT_CHALLENGE_TTL,
                            ) {
                                Ok(challenge) => reply!(id, serde_json::to_value(challenge).expect("challenge serializes"),),
                                Err(error) => write_auth_error(&mut writer, id, error)?,
                            }
                        }
                            RoutedOperation::AuthEnrollComplete(Routed { id, input: ChallengeSignatureInput { challenge_id, signature } }) => match authentication.complete_enrollment(&challenge_id, &signature) {
                            Ok(enrollment) => {
                                let created_at = unix_time_millis();
                                let identity_query = format!("on _identities | insert {{identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {created_at}}}", query_string(&enrollment.identity_id), query_string(&enrollment.identity_public_key));
                                let identity_response =
                                    execute_request(engine, QueryRequest::new(id, identity_query));
                                if !identity_response.is_ok() {
                                    write_response(&mut writer, &identity_response)?;
                                    return Ok(true);
                                }
                                let device_query = format!("on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {created_at}}}", query_string(&enrollment.device_id), query_string(&enrollment.identity_id), query_string(&enrollment.device_public_key));
                                let device_response = execute_request(
                                    engine,
                                    QueryRequest::new(id.clone(), device_query),
                                );
                                if !device_response.is_ok() {
                                    let rollback = format!( "on _identities | where identityId == {} | delete", query_string(&enrollment.identity_id) );
                                    let _ = execute_request(
                                        engine,
                                        QueryRequest::new(id.clone(), rollback),
                                    );
                                    write_response(&mut writer, &device_response)?;
                                    return Ok(true);
                                }

                                // Stateless enrollment grants the smallest useful baseline:
                                // the new identity may subscribe to its authorized event stream.
                                // Broader query/system permissions still require explicit grants.
                                let permission_query = enrollment_events_permission_query(
                                    &enrollment.identity_id,
                                    created_at,
                                );
                                let permission_response = execute_request(
                                    engine,
                                    QueryRequest::new(id.clone(), permission_query),
                                );
                                if !permission_response.is_ok() {
                                    let rollback_device = format!( "on _devices | where deviceId == {} | delete", query_string(&enrollment.device_id) );
                                    let rollback_identity = format!( "on _identities | where identityId == {} | delete", query_string(&enrollment.identity_id) );
                                    let _ = execute_request(
                                        engine,
                                        QueryRequest::new(id.clone(), rollback_device),
                                    );
                                    let _ = execute_request(
                                        engine,
                                        QueryRequest::new(id.clone(), rollback_identity),
                                    );
                                    write_response(&mut writer, &permission_response)?;
                                    return Ok(true);
                                }

                                debug::log(
                                    DebugTopic::Permission,
                                    Some(connection_id),
                                    format!(
                                        "grant identity={} action=events.subscribe resource=* source=enrollment",
                                        enrollment.identity_id
                                    ),
                                );
                                publish_durable_event(
                                    engine,
                                    Audience::identities([enrollment.identity_id.clone()]),
                                    "identity.enrolled",
                                    serde_json::json!({"identityId": enrollment.identity_id, "deviceId": enrollment.device_id, "createdAt": created_at}),
                                );
                                reply!(id, serde_json::json!({"enrolled": true, "identityId": enrollment.identity_id, "deviceId": enrollment.device_id}),);
                            }
                            Err(error) => write_auth_error(&mut writer, id, error)?,
                        },
                            RoutedOperation::AuthClassicRegister(Routed { id, input: ClassicAuthRegisterInput { identifier, password } }) => {
                            if !settings.classic_auth_enabled { reject!(id, "auth.classic_disabled", "classic authentication is disabled"); }
                            let identity_id = identity_or_reject!(id, "an authenticated identity is required");
                            let identifier = identifier.trim();
                            if identifier.is_empty() || identifier.len() > 200 { reject!(id, "auth.invalid_identifier", "identifier must contain between 1 and 200 bytes"); }
                            if password.as_bytes().len() < CLASSIC_AUTH_MIN_PASSWORD_BYTES { reject!(id, "auth.weak_password", "password must contain at least 12 bytes"); }
                            let normalized = identifier.to_lowercase();
                            let existing = query_documents_or_reject!(id, format!("on _credentials | where loginNormalized == {} or identityId == {} | limit 1", query_string(&normalized), query_string(identity_id)));
                            if !existing.is_empty() { reject!(id, "auth.identifier_unavailable", "identifier or identity is already registered"); }
                            let (salt, hash) = match classic_password_hash(&password) { Ok(value) => value, Err(error) => reject!(id, "auth.hash_failed", error) };
                            let now = unix_time_millis();
                            execute_query_or_reject!(id, format!("on _credentials | insert {{identityId: {}, login: {}, loginNormalized: {}, salt: {}, hash: {}, algorithm: \"argon2id\", memoryKiB: {}, iterations: {}, lanes: {}, state: \"active\", createdAt: {now}}}", query_string(identity_id), query_string(identifier), query_string(&normalized), query_string(&salt), query_string(&hash), CLASSIC_AUTH_MEMORY_KIB, CLASSIC_AUTH_ITERATIONS, CLASSIC_AUTH_LANES));
                            reply!(id, serde_json::json!({"registered": true, "identifier": identifier}));
                        }
                            RoutedOperation::AuthClassicLogin(Routed { id, input: ClassicAuthLoginInput { identifier, password, device_id, public_key } }) => {
                            if !settings.classic_auth_enabled { reject!(id, "auth.classic_disabled", "classic authentication is disabled"); }
                            let normalized = identifier.trim().to_lowercase();
                            if normalized.is_empty() || normalized.len() > 200 { reject!(id, "auth.invalid_credentials", "invalid identifier or password"); }
                            if let Some(wait_ms) = classic_login_wait(settings, &normalized) { reject!(id, "auth.rate_limited", format!("retry after {wait_ms}ms")); }
                            let documents = query_documents_or_reject!(id, format!("on _credentials | where loginNormalized == {} and state == \"active\" | limit 1", query_string(&normalized)));
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
                            if let Err(error) = validate_ed25519_public_key(&public_key) { reject!(id, "auth.invalid_public_key", error.to_string()); }
                            let identities = query_documents_or_reject!(id, format!("on _identities | where identityId == {} and state == \"active\" | limit 1", query_string(&identity_id)));
                            if identities.is_empty() { reject!(id, "auth.invalid_credentials", "invalid identifier or password"); }
                            if let Some(existing) = or_reject!(try_load_device_credential(engine, id, &identity_id, &device_id)) {
                                if !existing.active || existing.public_key != public_key { reject!(id, "auth.device_mismatch", "device identifier is already registered with different credentials"); }
                            } else {
                                let collision = query_documents_or_reject!(id, format!("on _devices | where deviceId == {} | limit 1", query_string(&device_id)));
                                if !collision.is_empty() { reject!(id, "auth.device_mismatch", "device identifier is already registered"); }
                                let now = unix_time_millis();
                                execute_query_or_reject!(id, format!("on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {now}}}", query_string(&device_id), query_string(&identity_id), query_string(&public_key)));
                            }
                            reply!(id, serde_json::json!({"accepted": true, "identityId": identity_id, "deviceId": device_id}));
                        }
                            RoutedOperation::IdentityOpen(Routed { id, input: IdentityOpenInput { file, password, client_device_id } }) => {
                            let bytes = match decode_base64(&file) { Ok(bytes) => bytes, Err(_) => reject!(id, "identity.invalid_file", "identity file must be base64 encoded") };
                            let portable = value_or_reject!(id, identity_file::decrypt_bytes(&bytes, password.as_bytes()), "identity.open_failed");
                            let source = or_reject!(load_device_credential(engine, id, &portable.identity_id, &portable.device_id));
                            if source.public_key != portable.public_key { reject!(id, "auth.device_mismatch", "portable identity does not match the registered source device"); }
                            if !source.active { reject!(id, "auth.device_revoked", "portable identity source device is revoked"); }
                            let source_device_id = portable.device_id.clone();
                            let target_device_id = client_device_id.filter(|device_id| device_id != &source_device_id).unwrap_or_else(|| source_device_id.clone());
                            let (device, enrolled) = if target_device_id == source_device_id {
                                (source, false)
                            } else if let Some(device) = or_reject!(try_load_device_credential(engine, id, &portable.identity_id, &target_device_id)) {
                                (device, false)
                            } else {
                                let now = unix_time_millis();
                                execute_query_or_reject!(id, format!("on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", sourceDeviceId: {}, createdAt: {now}}}", query_string(&target_device_id), query_string(&portable.identity_id), query_string(&portable.public_key), query_string(&source_device_id)));
                                (DeviceCredential { identity_id: portable.identity_id.clone(), device_id: target_device_id.clone(), public_key: portable.public_key.clone(), algorithm: "ed25519".to_owned(), encoding: "spki-der".to_owned(), active: true }, true)
                            };
                            match authentication.establish(&device) {
                                Ok(principal) => reply!(id, serde_json::json!({"authenticated": true, "principal": principal, "deviceId": device.device_id, "sourceDeviceId": source_device_id, "enrolled": enrolled})),
                                Err(error) => write_auth_error(&mut writer, id, error)?,
                            }
                        }
                            RoutedOperation::IdentityGet(Routed { id, input: PasswordInput { password } }) => {
                            let identity_id = identity_or_reject!(id, "an authenticated identity is required");
                            let portable = value_or_reject!(id, IdentityCredential::renew(identity_id), "identity.generate_failed");
                            let bytes = value_or_reject!(id, identity_file::encrypt_bytes(&portable, password.as_bytes()), "identity.encrypt_failed");
                            let now = unix_time_millis();
                            execute_query_or_reject!(id, format!( "on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {now}}}", query_string(&portable.device_id), query_string(&identity_id), query_string(&portable.public_key), ));
                            reply!(id, serde_json::json!({ "file": encode_base64(&bytes), "fileName": format!("{identity_id}.ogid"), "identityId": identity_id, "deviceId": portable.device_id, }));
                        }
                            RoutedOperation::IdentityRenew(Routed { id, input: IdentityRenewInput { device_id, public_key, password } }) => {
                            let (identity_id, current_device_id) = authenticated_device_or_reject!(id, "an authenticated identity is required to renew credentials");
                            let (device_id, public_key, portable_file) = if let Some(password) = password {
                                let portable = value_or_reject!(id, IdentityCredential::renew(&identity_id), "identity.generate_failed");
                                let bytes = value_or_reject!(id, identity_file::encrypt_bytes(&portable, password.as_bytes()), "identity.encrypt_failed");
                                (portable.device_id, portable.public_key, Some(bytes))
                            } else {
                                (device_id.unwrap_or_default(), public_key.unwrap_or_default(), None)
                            };
                            if device_id == current_device_id { reject!(id, "identity.invalid_device", "renewal requires a new device identifier"); }
                            if let Err(error) = validate_ed25519_public_key(&public_key) { reject!(id, "identity.invalid_public_key", error.to_string()); }
                            let now = unix_time_millis();
                            for query in [
                                format!("on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: \"ed25519\", encoding: \"spki-der\", state: \"active\", createdAt: {now}}}", query_string(&device_id), query_string(&identity_id), query_string(&public_key)),
                                format!("on _identities | where identityId == {} | set publicKey = {}, algorithm = \"ed25519\", encoding = \"spki-der\", updatedAt = {now}", query_string(&identity_id), query_string(&public_key)),
                                format!("on _devices | where deviceId == {} and identityId == {} | set state = \"revoked\", revokedAt = {now}", query_string(&current_device_id), query_string(&identity_id)),
                            ] { execute_query_or_reject!(id, query); }
                            let principal = Principal::Identity { identity_id: identity_id.clone(), device_id: device_id.clone() };
                            if portable_file.is_some() {
                                let device = DeviceCredential { identity_id: identity_id.clone(), device_id: device_id.clone(), public_key: public_key.clone(), algorithm: "ed25519".to_owned(), encoding: "spki-der".to_owned(), active: true };
                                if let Err(error) = authentication.establish(&device) { write_auth_error(&mut writer, id, error)?; return Ok(true); }
                            }
                            publish_durable_event(engine, Audience::identities([identity_id.clone()]), "identity.renewed", serde_json::json!({"identityId": identity_id, "deviceId": device_id, "previousDeviceId": current_device_id, "renewedAt": now}));
                            let mut data = serde_json::json!({ "identityId": identity_id, "deviceId": device_id, "previousDeviceId": current_device_id, "renewedAt": now, "principal": principal });
                            if let Some(bytes) = portable_file {
                                data["file"] = JsonValue::String(encode_base64(&bytes));
                                data["fileName"] = JsonValue::String(format!("{}.ogid", identity_id));
                            }
                            reply!(id, data);
                        }
                            RoutedOperation::DeviceIdentify(Routed { id, .. }) => {
                            let identity_id = identity_or_reject!(id, "authentication is required");
                            let documents = query_documents_or_reject!(id, format!("on _devices | where identityId == {} and state == \"active\" | sort createdAt asc", query_string(identity_id)));
                            let devices = documents.iter().enumerate().filter_map(|(index, device)| device.get("deviceId").and_then(JsonValue::as_str).map(|device_id| serde_json::json!({"deviceId": device_id, "number": index + 1}))).collect::<Vec<_>>();
                            let payload = serde_json::json!({"identityId": identity_id, "expiresAt": unix_time_millis().saturating_add(15_000), "devices": devices});
                            event_engine.publish_to(Audience::identities([identity_id.to_owned()]), "device.identify", payload.clone());
                            reply!(id, payload);
                        }
                            _ => unreachable!("operation execution mode and routed variant diverged"),
                        }
    Ok(false)
}

fn handle_subscription_operation( mut writer: &mut TcpStream, reader: &mut BufReader<TcpStream>, settings: &ConnectionSettings, event_engine: &EventEngine, authentication: &ConnectionAuth, connection_id: u64, subscription: &mut Option<EventSubscription>, operation: RoutedOperation, ) -> Result<bool, ConnectionError> {
    macro_rules! reply { ($id:expr,$data:expr $(,)?)=>{{write_operation_response(&mut writer,&OperationResponse::new($id,$data))?;}}; }
    match operation {
                            RoutedOperation::EventsSubscribe(Routed { id, input: EventsSubscribeInput { mut types } }) => {
                            if settings.authenticated_keepalive
                                && !matches!(authentication.principal(), Principal::Anonymous)
                            {
                                ensure_authenticated_keepalive_type(&mut types);
                            }
                            debug::log(
                                DebugTopic::Events,
                                Some(connection_id),
                                format!("subscribe types={types:?}"),
                            );
                            let active = event_engine.subscribe(types);
                            let subscription_id = active.id();
                            *subscription = Some(active);
                            reader
                                .get_mut()
                                .set_read_timeout(Some(Duration::from_millis(250)))
                                .map_err(ConnectionError::ConfigureSocket)?;
                            reply!(id, serde_json::json!({ "subscribed": true, "subscriptionId": subscription_id }),);
                            event_engine.publish_global(
                                "core.started",
                                serde_json::json!({ "subscriptionId": subscription_id }),
                            );
                        }
                            _ => unreachable!("operation execution mode and routed variant diverged"),
                        }
    Ok(false)
}

fn handle_file_operation( mut writer: &mut TcpStream, reader: &mut BufReader<TcpStream>, settings: &ConnectionSettings, engine: &Engine, authentication: &ConnectionAuth, delegation: Option<&GatewayDelegation>, operation: RoutedOperation, ) -> Result<bool, ConnectionError> {
    macro_rules! reject { ($id:expr,$code:expr,$message:expr)=>{{write_response(&mut writer,&QueryResponse::request_error($id,$code,$message))?;return Ok(true);}}; }
    macro_rules! reject_response { ($response:expr)=>{{write_response(&mut writer,&$response)?;return Ok(true);}}; }
    macro_rules! or_reject { ($result:expr)=>{{match $result{Ok(value)=>value,Err(response)=>reject_response!(response)}}}; }
    macro_rules! reply { ($id:expr,$data:expr $(,)?)=>{{write_operation_response(&mut writer,&OperationResponse::new($id,$data))?;}}; }
    macro_rules! respond { ($id:expr,$result:expr,$map:expr)=>{{match $result{Ok(value)=>write_operation_response(&mut writer,&OperationResponse::new($id,($map)(value)))?,Err(response)=>write_response(&mut writer,&response)?}}}; }
    macro_rules! ensure_file_access { ($id:expr,$place_id:expr,$instance_id:expr,$write:expr)=>{{
        let delegated = delegation.is_some_and(|value| value.capability == "files" && value.place_id == *$place_id);
        if !delegated {
            if let Err(response)=resolve_file_context(engine,$id,authentication.principal(),!settings.authorization_mode.is_enforced(),$place_id,$instance_id,$write){reject_response!(response);}
        }
    }}; }
    macro_rules! some_or_reject { ($id:expr,$value:expr,$code:expr,$message:expr)=>{{match $value{Some(value)=>value,None=>reject!($id,$code,$message)}}}; }
    macro_rules! file_store_or_reject { ($id:expr,$result:expr)=>{{match $result{Ok(value)=>value,Err(error)=>{write_file_store_error(&mut writer,$id,error)?;return Ok(true);}}}}; }
    match operation {
                            RoutedOperation::DataWorkerRun(Routed { id, input: DataWorkerRunInput { place_id, file_name, size, operation, mapping } }) => {
                            let delegated = delegation.is_some_and(|value| value.capability == "data.import" && value.place_id == place_id);
                            if !delegated {
                                write_response(&mut writer,&QueryResponse::request_error(id,"authorization.denied","data.worker.run requires a delegated data.import channel"))?;
                                return Ok(true);
                            }
                            reply!(id,serde_json::json!({"ready":true,"bytes":size,"stream":"raw"}));
                            writer.flush().map_err(ConnectionError::Write)?;
                            let extension=Path::new(&file_name).extension().and_then(|v|v.to_str()).unwrap_or("dat");
                            let temp=env::temp_dir().join(format!("og-data-{}-{}.{}",std::process::id(),unix_time_millis(),extension));
                            let mut out=fs::File::create(&temp).map_err(ConnectionError::Write)?;
                            let mut limited=reader.by_ref().take(size);
                            let copied=io::copy(&mut limited,&mut out).map_err(ConnectionError::Read)?;
                            if copied!=size {
                                let _=fs::remove_file(&temp);
                                return Err(ConnectionError::Read(io::Error::new(io::ErrorKind::UnexpectedEof,format!("data worker upload ended after {copied} of {size} bytes"))));
                            }
                            drop(out);
                            let result=run_data_worker_for_path(id,&temp,&operation,mapping.as_ref());
                            let _=fs::remove_file(&temp);
                            match result {
                                Ok(value)=>reply!(id,value),
                                Err(response)=>write_response(&mut writer,&response)?,
                            }
                            return Ok(true);
                        }
                            RoutedOperation::FileRead(Routed { id, input: FileReadInput { place_id, instance_id, file_id, offset, length } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, false);
                            let file_store=or_reject!(scoped_native_file_store(settings,id,&place_id,&instance_id));
                            let entry=or_reject!(load_file_entry(engine,id,&place_id,&instance_id,&file_id));
                            let stat=file_store_or_reject!(id,file_store.stat(&entry.remote_id));
                            let total=some_or_reject!(id,stat.metadata.size,"file.not_file","File entry has no byte size");
                            if offset>total || length.is_some_and(|requested|requested>total.saturating_sub(offset)){
                                write_response(&mut writer,&QueryResponse::request_error(id,"file.invalid_range","requested byte range is outside the file"))?;
                                return Ok(true);
                            }
                            let bytes=length.unwrap_or_else(||total.saturating_sub(offset));
                            let range=(offset!=0 || length.is_some()).then(||FileRange::new(offset,length));
                            let mut source=file_store_or_reject!(id,file_store.read(&entry.remote_id,range));
                            reply!(id, serde_json::json!({ "fileId":file_id, "name":entry.name, "contentType":entry.metadata.content_type, "etag":stat.metadata.etag, "size":total, "offset":offset, "bytes":bytes, "stream":"raw" }));
                            writer.flush().map_err(ConnectionError::Write)?;
                            let copied=io::copy(&mut source,&mut writer).map_err(ConnectionError::Write)?;
                            if copied!=bytes{return Err(ConnectionError::Write(io::Error::new(io::ErrorKind::UnexpectedEof,format!("file stream ended after {copied} of {bytes} bytes"))));}
                            writer.flush().map_err(ConnectionError::Write)?;
                        }
                            RoutedOperation::FileWrite(Routed { id, input: FileWriteInput { place_id, instance_id, file_id, parent_id, name, content_type, size } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, true);
                            let file_store=or_reject!(scoped_native_file_store(settings,id,&place_id,&instance_id));
                            let existing=or_reject!(file_id.as_deref().map(|value|load_file_entry(engine,id,&place_id,&instance_id,value)).transpose());
                            if existing.as_ref().is_some_and(|entry|entry.kind!=og_core::files::FileKind::File){
                                write_response(&mut writer,&QueryResponse::request_error(id,"file.not_file","cannot replace a directory with file content"))?;
                                return Ok(true);
                            }
                            let parent=if let Some(existing)=existing.as_ref(){existing.parent_id.as_ref().map(|value|value.as_str().to_owned())}else{parent_id.clone()};
                            let parent_remote=match parent.as_deref().map(|value|load_file_entry(engine,id,&place_id,&instance_id,value)).transpose(){Ok(value)=>value.map(|entry|entry.remote_id),Err(response)=>{write_response(&mut writer,&response)?;return Ok(true);}};
                            let target_name=existing.as_ref().map(|entry|entry.name.as_str()).or(name.as_deref()).expect("router validates create name");
                            let remote_id=existing.as_ref().map(|entry|entry.remote_id.as_str());
                            if let Some(previous)=existing.as_ref(){
                                or_reject!(archive_current_file(engine,settings,id,previous));
                            }
                            reply!(id, serde_json::json!({"ready":true,"bytes":size,"stream":"raw"}));
                            writer.flush().map_err(ConnectionError::Write)?;
                            let mut limited=reader.by_ref().take(size);
                            let target=FileWrite{remote_id,parent_remote_id:parent_remote.as_deref(),name:target_name,content_type:content_type.as_deref(),size:Some(size)};
                            let mut stored=file_store_or_reject!(id,file_store.write(target,&mut limited));
                            stored.metadata.content_type=content_type.clone().or_else(||existing.as_ref().and_then(|entry|entry.metadata.content_type.clone()));
                            let updated=match existing{
                                Some(previous)=>FileEntry{file_id:previous.file_id,store_id:previous.store_id,remote_id:stored.remote_id,parent_id:previous.parent_id,name:previous.name,kind:stored.kind,metadata:stored.metadata,place_id,app_instance_id:instance_id},
                                None=>file_entry_from_store(&place_id,&instance_id,parent_id.as_deref(),stored),
                            };
                            let result=if file_id.is_some(){replace_file_entry(engine,id,&updated)}else{persist_file_entry(engine,id,&updated)};
                            match result{
                                Ok(json)=>reply!(id, serde_json::json!({"file":json,"bytes":size,"stream":"raw"})),
                                Err(response)=>{
                                    if file_id.is_none(){let _=file_store.delete(&updated.remote_id);}
                                    write_response(&mut writer,&response)?;
                                }
                            }
                        }
                            RoutedOperation::FileVersions(Routed { id, input: FileEntryInput { place_id, instance_id, file_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, false);
                            respond!(id, list_file_versions(engine,id,&place_id,&instance_id,&file_id), |versions| serde_json::json!({"versions":versions}));
                        }
                            RoutedOperation::FileVersionRead(Routed { id, input: FileVersionReadInput { place_id, instance_id, file_id, version_id, offset, length } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, false);
                            let version=or_reject!(load_file_version(engine,id,&place_id,&instance_id,&file_id,&version_id));
                            let Some(remote_id)=version.get("remoteId").and_then(JsonValue::as_str) else {write_response(&mut writer,&QueryResponse::request_error(id,"file.invalid_record","version has no remoteId"))?;return Ok(true);};
                            let store=or_reject!(scoped_native_version_store(settings,id,&place_id,&instance_id));
                            let total=version.get("size").and_then(JsonValue::as_u64).unwrap_or(0);
                            let range=if offset.is_some() || length.is_some(){Some(FileRange::new(offset.unwrap_or(0),length))}else{None};
                            let mut source=file_store_or_reject!(id,store.read(remote_id,range));
                            let start=offset.unwrap_or(0);
                            let available=total.saturating_sub(start);
                            let bytes=length.unwrap_or(available).min(available);
                            reply!(id, serde_json::json!({ "stream":"raw","bytes":bytes,"size":total,"offset":offset.unwrap_or(0), "contentType":version.get("contentType").cloned().unwrap_or(JsonValue::Null), "etag":version.get("etag").cloned().unwrap_or(JsonValue::Null), "versionId":version_id,"fileId":file_id }));
                            writer.flush().map_err(ConnectionError::Write)?;
                            io::copy(&mut source,&mut writer).map_err(ConnectionError::Write)?;
                            writer.flush().map_err(ConnectionError::Write)?;
                        }
                            RoutedOperation::FileVersionRestore(Routed { id, input: FileVersionInput { place_id, instance_id, file_id, version_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, true);
                            let current=or_reject!(load_file_entry(engine,id,&place_id,&instance_id,&file_id));
                            let version=or_reject!(load_file_version(engine,id,&place_id,&instance_id,&file_id,&version_id));
                            or_reject!(archive_current_file(engine,settings,id,&current));
                            let Some(version_remote)=version.get("remoteId").and_then(JsonValue::as_str) else {write_response(&mut writer,&QueryResponse::request_error(id,"file.invalid_record","version has no remoteId"))?;return Ok(true);};
                            let versions=or_reject!(scoped_native_version_store(settings,id,&place_id,&instance_id));
                            let current_store=or_reject!(scoped_native_file_store(settings,id,&place_id,&instance_id));
                            let mut source=file_store_or_reject!(id,versions.read(version_remote,None));
                            let size=version.get("size").and_then(JsonValue::as_u64);
                            let content_type=version.get("contentType").and_then(JsonValue::as_str);
                            let stored=file_store_or_reject!(id,current_store.write(FileWrite{remote_id:Some(&current.remote_id),parent_remote_id:None,name:&current.name,content_type,size},&mut source));
                            let updated=FileEntry{file_id:current.file_id,store_id:current.store_id,remote_id:stored.remote_id,parent_id:current.parent_id,name:current.name,kind:stored.kind,metadata:stored.metadata,place_id,app_instance_id:instance_id};
                            match replace_file_entry(engine,id,&updated){
                                Ok(file)=>reply!(id, serde_json::json!({"file":file,"restoredVersionId":version_id})),
                                Err(response)=>write_response(&mut writer,&response)?,
                            }
                        }
                            RoutedOperation::FileVersionDelete(Routed { id, input: FileVersionInput { place_id, instance_id, file_id, version_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, true);
                            let version=or_reject!(load_file_version(engine,id,&place_id,&instance_id,&file_id,&version_id));
                            let Some(remote_id)=version.get("remoteId").and_then(JsonValue::as_str) else {write_response(&mut writer,&QueryResponse::request_error(id,"file.invalid_record","version has no remoteId"))?;return Ok(true);};
                            let store=or_reject!(scoped_native_version_store(settings,id,&place_id,&instance_id));
                            if let Err(error)=store.delete(remote_id){write_file_store_error(&mut writer,id,error)?;return Ok(true);}
                            match delete_file_version_metadata(engine,id,&place_id,&instance_id,&file_id,&version_id){
                                Ok(())=>reply!(id, serde_json::json!({"deleted":true,"versionId":version_id})),
                                Err(response)=>write_response(&mut writer,&response)?,
                            }
                        }
                            RoutedOperation::FileCapabilities(Routed { id, input: FileScopeInput { place_id, instance_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, false);
                            let file_store=or_reject!(scoped_native_file_store(settings,id,&place_id,&instance_id));
                            reply!(id, serde_json::to_value(file_store.capabilities()).expect("capabilities serialize"));
                        }
                            RoutedOperation::FileList(Routed { id, input: FileListInput { place_id, instance_id, parent_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, false);
                            respond!(id, list_file_entries(engine,id,&place_id,&instance_id,parent_id.as_deref()), JsonValue::Array);
                        }
                            RoutedOperation::FileStat(Routed { id, input: FileEntryInput { place_id, instance_id, file_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, false);
                            respond!(id, load_file_entry_json(engine,id,&place_id,&instance_id,&file_id), |entry| entry);
                        }
                            RoutedOperation::FileMkdir(Routed { id, input: FileMkdirInput { place_id, instance_id, parent_id, name } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, true);
                            let file_store=or_reject!(scoped_native_file_store(settings,id,&place_id,&instance_id));
                            let parent_remote=match parent_id.as_deref().map(|parent| load_file_entry(engine,id,&place_id,&instance_id,parent)).transpose() {
                                Ok(value)=>value.map(|entry|entry.remote_id), Err(response)=>{write_response(&mut writer,&response)?;return Ok(true);}
                            };
                            match file_store.mkdir(parent_remote.as_deref(),&name) {
                                Ok(stored)=>{
                                    let entry=file_entry_from_store(&place_id,&instance_id,parent_id.as_deref(),stored);
                                    match persist_file_entry(engine,id,&entry) {
                                        Ok(json)=>reply!(id, json),
                                        Err(response)=>{ let _=file_store.delete(&entry.remote_id); write_response(&mut writer,&response)?; }
                                    }
                                }
                                Err(error)=>write_file_store_error(&mut writer,id,error)?,
                            }
                        }
                            RoutedOperation::FileMove(Routed { id, input: FileMoveInput { place_id, instance_id, file_id, parent_id, name } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, true);
                            let file_store=or_reject!(scoped_native_file_store(settings,id,&place_id,&instance_id));
                            let entry=or_reject!(load_file_entry(engine,id,&place_id,&instance_id,&file_id));
                            let parent_remote=match parent_id.as_deref().map(|parent| load_file_entry(engine,id,&place_id,&instance_id,parent)).transpose(){Ok(v)=>v.map(|e|e.remote_id),Err(r)=>{write_response(&mut writer,&r)?;return Ok(true);}};
                            match file_store.move_entry(&entry.remote_id,parent_remote.as_deref(),&name){
                                Ok(stored)=>{
                                    let updated=FileEntry{file_id:entry.file_id.clone(),store_id:entry.store_id.clone(),remote_id:stored.remote_id,parent_id:parent_id.as_deref().map(FileId::from),name:stored.name,kind:stored.kind,metadata:stored.metadata,place_id:place_id.clone(),app_instance_id:instance_id.clone()};
                                    match replace_file_entry(engine,id,&updated){
                                        Ok(json)=>{
                                            if updated.kind == og_core::files::FileKind::Directory {
                                                if let Err(r)=sync_moved_children(engine,id,&file_store,&place_id,&instance_id,&updated){write_response(&mut writer,&r)?;return Ok(true);}
                                            }
                                            reply!(id, json)
                                        }
                                        Err(r)=>write_response(&mut writer,&r)?
                                    }
                                }
                                Err(error)=>write_file_store_error(&mut writer,id,error)?,
                            }
                        }
                            RoutedOperation::FileCopy(Routed { id, input: FileMoveInput { place_id, instance_id, file_id, parent_id, name } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, true);
                            let file_store=or_reject!(scoped_native_file_store(settings,id,&place_id,&instance_id));
                            let entry=or_reject!(load_file_entry(engine,id,&place_id,&instance_id,&file_id));
                            if entry.kind==og_core::files::FileKind::Directory {
                                match subtree_has_trashed_entries(engine,id,&place_id,&instance_id,&file_id){
                                    Ok(true)=>{
                                        write_response(&mut writer,&QueryResponse::request_error(id,"file.trash_conflict","restore or permanently delete trashed descendants before copying this directory"))?;
                                        return Ok(true);
                                    }
                                    Ok(false)=>{}
                                    Err(response)=>{write_response(&mut writer,&response)?;return Ok(true);}
                                }
                            }
                            let parent_remote=match parent_id.as_deref().map(|parent| load_file_entry(engine,id,&place_id,&instance_id,parent)).transpose(){Ok(v)=>v.map(|e|e.remote_id),Err(r)=>{write_response(&mut writer,&r)?;return Ok(true);}};
                            match file_store.copy(&entry.remote_id,parent_remote.as_deref(),&name){
                                Ok(stored)=>{
                                    let copied=file_entry_from_store(&place_id,&instance_id,parent_id.as_deref(),stored);
                                    match persist_file_entry(engine,id,&copied){
                                        Ok(json)=>{
                                            if copied.kind == og_core::files::FileKind::Directory {
                                                if let Err(r)=persist_copied_children(engine,id,&file_store,&place_id,&instance_id,&copied){let _=file_store.delete(&copied.remote_id);write_response(&mut writer,&r)?;return Ok(true);}
                                            }
                                            reply!(id, json)
                                        }
                                        Err(r)=>{let _=file_store.delete(&copied.remote_id);write_response(&mut writer,&r)?;}
                                    }
                                }
                                Err(error)=>write_file_store_error(&mut writer,id,error)?,
                            }
                        }
                            RoutedOperation::FileDelete(Routed { id, input: FileEntryInput { place_id, instance_id, file_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, true);
                            respond!(id, trash_file_entry(engine,settings,id,&place_id,&instance_id,&file_id), |file| serde_json::json!({"trashed":true,"file":file}));
                        }
                            RoutedOperation::FileTrashList(Routed { id, input: FileScopeInput { place_id, instance_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, false);
                            respond!(id, list_trashed_file_entries(engine,id,&place_id,&instance_id), |entries| serde_json::json!({"entries":entries}));
                        }
                            RoutedOperation::FileRestore(Routed { id, input: FileEntryInput { place_id, instance_id, file_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, true);
                            respond!(id, restore_file_entry(engine,settings,id,&place_id,&instance_id,&file_id), |file| serde_json::json!({"restored":true,"file":file}));
                        }
                            RoutedOperation::FileDeletePermanent(Routed { id, input: FileEntryInput { place_id, instance_id, file_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, true);
                            if !file_is_trashed(engine,id,&place_id,&instance_id,&file_id).unwrap_or(false){
                                write_response(&mut writer,&QueryResponse::request_error(id,"file.not_trashed","Only trashed entries may be permanently deleted"))?;
                                return Ok(true);
                            }
                            match permanently_delete_file_tree(engine,settings,id,&place_id,&instance_id,&file_id){
                                Ok(())=>reply!(id, serde_json::json!({"deleted":true,"fileId":file_id})),
                                Err(response)=>write_response(&mut writer,&response)?,
                            }
                        }
                            RoutedOperation::FileTrashEmpty(Routed { id, input: FileScopeInput { place_id, instance_id } }) => {
                            ensure_file_access!(id, &place_id, &instance_id, true);
                            let entries=or_reject!(list_trashed_file_entries(engine,id,&place_id,&instance_id));
                            let mut deleted=0u64;
                            let mut failed=None;
                            for entry in entries {
                                let Some(file_id)=entry.get("fileId").and_then(JsonValue::as_str) else {continue;};
                                match permanently_delete_file_tree(engine,settings,id,&place_id,&instance_id,file_id){
                                    Ok(())=>deleted=deleted.saturating_add(1),
                                    Err(response)=>{failed=Some(response);break;}
                                }
                            }
                            if let Some(response)=failed{write_response(&mut writer,&response)?;}else{
                                reply!(id, serde_json::json!({"emptied":true,"deleted":deleted}));
                            }
                        }
                            _ => unreachable!("operation execution mode and routed variant diverged"),
                        }
    Ok(false)
}

fn handle_standard_operation( mut writer: &mut TcpStream, settings: &ConnectionSettings, engine: &Engine, authentication: &ConnectionAuth, operation: RoutedOperation, ) -> Result<(), ConnectionError> {
    macro_rules! reject { ($id:expr,$code:expr,$message:expr)=>{{write_response(writer,&QueryResponse::request_error($id,$code,$message))?;return Ok(());}}; }
    macro_rules! reject_response { ($response:expr)=>{{write_response(writer,&$response)?;return Ok(());}}; }
    macro_rules! identity_or_reject { ($id:expr,$message:expr)=>{{let Some(identity_id)=require_identity(writer,authentication.principal(),$id,$message)? else {return Ok(());};identity_id}}; }
    macro_rules! reply { ($id:expr,$data:expr $(,)?)=>{{write_operation_response(writer,&OperationResponse::new($id,$data))?;}}; }
    macro_rules! respond { ($id:expr,$result:expr,$map:expr)=>{{match $result{Ok(value)=>write_operation_response(writer,&OperationResponse::new($id,($map)(value)))?,Err(response)=>write_response(writer,&response)?}}}; }
    macro_rules! or_reject { ($result:expr)=>{{match $result{Ok(value)=>value,Err(response)=>reject_response!(response)}}}; }
    macro_rules! execute_query_or_reject { ($id:expr,$query:expr)=>{{let response=execute_request(engine,QueryRequest::new($id,$query));if !response.is_ok(){reject_response!(response);}}}; }
    macro_rules! query_documents_or_reject { ($id:expr,$query:expr)=>{{match execute_request(engine,QueryRequest::new($id,$query)){QueryResponse::Ok{documents,..}=>documents,response@QueryResponse::Error{..}=>reject_response!(response)}}}; }
    macro_rules! authenticated_device_or_reject { ($id:expr,$message:expr)=>{{match authentication.principal(){Principal::Identity{identity_id,device_id}=>(identity_id.clone(),device_id.clone()),Principal::Anonymous=>reject!($id,"authorization.required",$message)}}}; }
    macro_rules! execute_query_publish { ($id:expr,$query:expr,$audience:expr,$event:expr,$payload:expr $(,)?)=>{{let response=execute_request(engine,QueryRequest::new($id,$query));let ok=response.is_ok();write_response(writer,&response)?;if ok{publish_durable_event(engine,$audience,$event,$payload);}}}; }
    macro_rules! execute_publish_reply { ($id:expr,$query:expr,$audience:expr,$event:expr,$event_payload:expr,$reply:expr $(,)?)=>{{execute_query_or_reject!($id,$query);publish_durable_event(engine,$audience,$event,$event_payload);reply!($id,$reply);}}; }
    macro_rules! place_role_or_reject { ($id:expr,$identity:expr,$place:expr)=>{{match resolve_place_role(engine,$id,$identity,$place){Ok(Some(role))=>role,Ok(None)=>reject!($id,"place.access_denied","the identity does not have access to this Place"),Err(response)=>reject_response!(response)}}}; }
    macro_rules! place_owner_or_reject { ($id:expr,$identity:expr,$place:expr,$message:expr)=>{{let role=place_role_or_reject!($id,$identity,$place);if !role.can_manage(){reject!($id,"place.owner_required",$message);}}}; }
    macro_rules! authorize_resource_or_reject { ($id:expr,$kind:expr,$resource:expr)=>{{if !ensure_operation_resource_authorized(writer,settings,engine,authentication.principal(),$id,$kind,$resource)?{return Ok(());}}}; }

    match operation.handler() {
                            HandlerKind::Core => match operation {
                                RoutedOperation::CoreHealth(Routed { id, .. }) => {
                                reply!(id, serde_json::json!({ "healthy": true, "version": PROTOCOL_VERSION, "authRequired": settings.authorization_mode.is_enforced(), "classicAuthEnabled": settings.classic_auth_enabled, "capabilities": settings.service_capabilities.names(), }),);
                            }
                                RoutedOperation::Ping(Routed { id, .. }) => {
                                reply!(id, serde_json::json!({ "data": "pong", "version": PROTOCOL_VERSION, }),);
                            }
                                _ => unreachable!("handler domain and routed variant diverged"),
                            },
                            HandlerKind::Collections => match operation {
                                RoutedOperation::CollectionsList(Routed { id, input: CollectionsListInput { stats } }) => {
                                let snapshot = match engine.storage().read() {
                                    Ok(snapshot) => snapshot,
                                    Err(error) => {
                                        reject!(id, "collections.failed", error.to_string());
                                    }
                                };
                                let mut collections = Vec::new();
                                match snapshot.collections() {
                                    Ok(names) => {
                                        for name in names {
                                            let mut item = serde_json::json!({"name": name.as_str(), "system": name.as_str().starts_with('_')});
                                            if stats {
                                                if let Ok(count) = snapshot.count(&name) {
                                                    item["documents"] = serde_json::json!(count);
                                                }
                                            }
                                            collections.push(item);
                                        }
                                    }
                                    Err(error) => {
                                        reject!(id, "collections.failed", error.to_string());
                                    }
                                }
                                for name in vcollections::ALL {
                                    if !collections.iter().any(|item| {
                                        item.get("name").and_then(JsonValue::as_str) == Some(name)
                                    }) {
                                        collections.push(serde_json::json!({"name": name, "system": true, "virtual": true}));
                                    }
                                }
                                reply!(id, serde_json::json!({"collections": collections}),);
                            }
                                _ => unreachable!("handler domain and routed variant diverged"),
                            },
                            HandlerKind::Storage => match operation {
                                RoutedOperation::StorageStats(Routed { id, .. }) => {

                                let data = if let Some(storage) = settings.glacier_storage.as_ref() {
                                    let metrics = storage.write_metrics();
                                    let startup = storage.startup_metrics();
                                    let read = storage.read_metrics();
                                    let resident = storage.resident_memory();
                                    let generation = storage.generation().unwrap_or_default();
                                    let documents = storage.document_count().unwrap_or_default();
                                    let store_bytes = storage.store_bytes().unwrap_or_default();
                                    let wal_bytes = storage.wal_bytes().unwrap_or_default();
                                    serde_json::json!({
                                        "backend": "glacier",
                                        "generation": generation,
                                        "documents": documents,
                                        "storeBytes": store_bytes,
                                        "walBytes": wal_bytes,
                                        "startup": startup,
                                        "read": read,
                                        "resident": resident,
                                        "write": metrics,
                                    })
                                } else {
                                    let snapshot = match engine.storage().read() {
                                        Ok(snapshot) => snapshot,
                                        Err(error) => {
                                            reject!(id, "storage.stats_failed", error.to_string());
                                        }
                                    };
                                    let mut documents = 0u64;
                                    let mut collections = 0u64;
                                    if let Ok(names) = snapshot.collections() {
                                        collections = names.len() as u64;
                                        for collection in names {
                                            documents = documents.saturating_add(
                                                snapshot.count(&collection).unwrap_or_default(),
                                            );
                                        }
                                    }
                                    serde_json::json!({
                                        "backend": settings.storage_backend.as_str(),
                                        "collections": collections,
                                        "documents": documents,
                                    })
                                };

                                reply!(id, data);
                            }
                                _ => unreachable!("handler domain and routed variant diverged"),
                            },
                            HandlerKind::Backup => match operation {
                                RoutedOperation::BackupCreate(Routed { id, input: BackupNameInput { name } }) => {
                                let Some(path) = backup_file_path(&settings.backup_path, &name) else {
                                    reject!(id, "backup.invalid_name", "backup name must be a simple file name");
                                };
                                match backup::create(
                                    engine.storage(),
                                    &path,
                                    backup_metadata(&settings.instance_id),
                                ) {
                                    Ok(summary) => reply!(id, serde_json::json!({"name": name, "collections": summary.collections, "documents": summary.documents}),),
                                    Err(error) => write_response(
                                        &mut writer,
                                        &QueryResponse::request_error(
                                            id,
                                            "backup.create_failed",
                                            error.to_string(),
                                        ),
                                    )?,
                                }
                            }
                                RoutedOperation::BackupInspect(Routed { id, input: BackupNameInput { name } }) => {
                                let Some(path) = backup_file_path(&settings.backup_path, &name) else {
                                    reject!(id, "backup.invalid_name", "backup name must be a simple file name");
                                };
                                match backup::inspect(&path) {
                                    Ok(summary) => reply!(id, serde_json::json!({
                                                "name": name,
                                                "format": summary.format,
                                                "version": summary.version,
                                                "createdAt": summary.created_at,
                                                "sizeBytes": summary.size_bytes,
                                                "collections": summary.collections,
                                                "documents": summary.documents,
                                                "source": summary.source,
                                            }),),
                                    Err(error) => write_response(
                                        &mut writer,
                                        &QueryResponse::request_error(
                                            id,
                                            "backup.inspect_failed",
                                            error.to_string(),
                                        ),
                                    )?,
                                }
                            }
                                RoutedOperation::BackupRestore(Routed { id, input: BackupRestoreInput { name, replace } }) => {
                                let Some(path) = backup_file_path(&settings.backup_path, &name) else {
                                    reject!(id, "backup.invalid_name", "backup name must be a simple file name");
                                };
                                match backup::restore(engine.storage(), &path, replace) {
                                    Ok(summary) => reply!(id, serde_json::json!({"restored": true, "name": name, "collections": summary.collections, "documents": summary.documents}),),
                                    Err(error) => write_response(
                                        &mut writer,
                                        &QueryResponse::request_error(
                                            id,
                                            "backup.restore_failed",
                                            error.to_string(),
                                        ),
                                    )?,
                                }
                            }
                                _ => unreachable!("handler domain and routed variant diverged"),
                            },
                            HandlerKind::Identity => match operation {
                                RoutedOperation::IdentityRegister(Routed { id, input: IdentityRegisterInput { identity_id, public_key, algorithm, encoding, created_at } }) => {
                                let created_at = created_at.unwrap_or_else(unix_time_millis);
                                let query = format!( "on _identities | insert {{identityId: {}, publicKey: {}, algorithm: {}, encoding: {}, state: \"active\", createdAt: {created_at}}}", query_string(&identity_id), query_string(&public_key), query_string(&algorithm), query_string(&encoding), );
                                execute_query_publish!( id, query, Audience::identities([identity_id.clone()]), "identity.created", serde_json::json!({"identityId": identity_id, "createdAt": created_at}), );
                            }
                                _ => unreachable!("handler domain and routed variant diverged"),
                            },
                            HandlerKind::Device => match operation {
                                RoutedOperation::DeviceList(Routed { id, .. }) => {
                                let (identity_id, current_device_id) = authenticated_device_or_reject!(id, "authentication is required");
                                let documents = query_documents_or_reject!(id, format!("on _devices | where identityId == {} | sort createdAt asc", query_string(&identity_id)));
                                let devices = documents.into_iter().map(|mut device| {
                                    if let Some(object) = device.as_object_mut() {
                                        let current = object.get("deviceId").and_then(JsonValue::as_str) == Some(current_device_id.as_str());
                                        object.insert("current".to_owned(), JsonValue::Bool(current));
                                    }
                                    device
                                }).collect::<Vec<_>>();
                                reply!(id, serde_json::json!({"devices": devices}));
                            }
                                RoutedOperation::DeviceRename(Routed { id, input: DeviceRenameInput { device_id, name } }) => {
                                let identity_id = identity_or_reject!(id, "authentication is required");
                                let name = name.trim();
                                if name.len() > 80 { reject!(id, "device.invalid_name", "device name must not exceed 80 bytes"); }
                                if or_reject!(try_load_device_credential(engine, id, identity_id, &device_id)).is_none() {
                                    reject!(id, "device.not_found", "device does not belong to the authenticated identity");
                                }
                                let updated_at = unix_time_millis();
                                let query = format!("on _devices | where deviceId == {} and identityId == {} | set name = {}, updatedAt = {updated_at}", query_string(&device_id), query_string(identity_id), query_string(name));
                                execute_publish_reply!(id, query, Audience::identities([identity_id.to_owned()]), "device.renamed", serde_json::json!({"deviceId": device_id, "name": name, "updatedAt": updated_at}), serde_json::json!({"deviceId": device_id, "name": name, "updatedAt": updated_at}));
                            }
                                RoutedOperation::DeviceRegister(Routed { id, input: DeviceRegisterInput { device_id, identity_id, public_key, algorithm, encoding, created_at } }) => {
                                let self_registration =
                                    principal_identity_id(authentication.principal()) == Some(identity_id.as_str());
                                if !self_registration
                                    && !ensure_operation_resource_authorized(
                                        &mut writer,
                                        settings,
                                        engine,
                                        authentication.principal(),
                                        id,
                                        OperationKind::DeviceRegister,
                                        "_devices",
                                    )?
                                {
                                    return Ok(());
                                }
                                let created_at = created_at.unwrap_or_else(unix_time_millis);
                                let query = format!( "on _devices | insert {{deviceId: {}, identityId: {}, publicKey: {}, algorithm: {}, encoding: {}, state: \"active\", createdAt: {created_at}}}", query_string(&device_id), query_string(&identity_id), query_string(&public_key), query_string(&algorithm), query_string(&encoding), );
                                execute_query_publish!( id, query, Audience::identities([identity_id.clone()]), "device.registered", serde_json::json!({"deviceId": device_id, "identityId": identity_id, "createdAt": created_at}), );
                            }
                                RoutedOperation::DeviceRevoke(Routed { id, input: DeviceRevokeInput { device_id, revoked_at } }) => {
                                let self_identity = principal_identity_id(authentication.principal());
                                let current_device = match authentication.principal() {
                                    Principal::Identity { device_id, .. } => Some(device_id.as_str()),
                                    Principal::Anonymous => None,
                                };
                                if current_device == Some(device_id.as_str()) {
                                    write_response(&mut writer, &QueryResponse::request_error(
                                        id,
                                        "device.current_revoke_forbidden",
                                        "the current device cannot be revoked; use identity.renew instead",
                                    ))?;
                                    return Ok(());
                                }
                                let self_owned = if let Some(identity_id) = self_identity {
                                    match try_load_device_credential(engine, id.clone(), identity_id, &device_id) {
                                        Ok(Some(_)) => true,
                                        Ok(None) => false,
                                        Err(response) => { write_response(&mut writer, &response)?; return Ok(()); }
                                    }
                                } else { false };
                                if !self_owned && !ensure_operation_resource_authorized(&mut writer, settings, engine, authentication.principal(), id.clone(), OperationKind::DeviceRevoke, "_devices")? {
                                    return Ok(());
                                }
                                let revoked_at = revoked_at.unwrap_or_else(unix_time_millis);
                                let identity_filter = self_identity.map(|identity_id| format!(" and identityId == {}", query_string(identity_id))).unwrap_or_default();
                                let query = format!( "on _devices | where deviceId == {}{} | set state = \"revoked\", revokedAt = {revoked_at}", query_string(&device_id), identity_filter, );
                                let audience = self_identity
                                    .map(|identity_id| Audience::identities([identity_id.to_owned()]))
                                    .unwrap_or(Audience::Global);
                                execute_query_publish!( id, query, audience, "device.revoked", serde_json::json!({"deviceId": device_id, "revokedAt": revoked_at}), );
                            }
                                _ => unreachable!("handler domain and routed variant diverged"),
                            },
                            HandlerKind::Permission => match operation {
                                RoutedOperation::PermissionGrant(Routed { id, input: PermissionGrantInput { identity_id, action, resource, created_at }, }) => {
                                let created_at = created_at.unwrap_or_else(unix_time_millis);
                                let query = format!( "on _permissions | insert {{identityId: {}, action: {}, resource: {}, effect: \"allow\", state: \"active\", createdAt: {created_at}}}", query_string(&identity_id), query_string(&action), query_string(&resource), );
                                execute_query_publish!( id, query, Audience::identities([identity_id.clone()]), "permission.granted", serde_json::json!({"identityId": identity_id, "action": action, "resource": resource, "createdAt": created_at}), );
                            }
                                RoutedOperation::PermissionRevoke(Routed { id, input: PermissionRevokeInput { identity_id, action, resource, revoked_at }, }) => {
                                let revoked_at = revoked_at.unwrap_or_else(unix_time_millis);
                                let query = format!( "on _permissions | where identityId == {} and action == {} and resource == {} and state == \"active\" | set state = \"revoked\", revokedAt = {revoked_at}", query_string(&identity_id), query_string(&action), query_string(&resource), );
                                execute_query_publish!( id, query, Audience::identities([identity_id.clone()]), "permission.revoked", serde_json::json!({"identityId": identity_id, "action": action, "resource": resource, "revokedAt": revoked_at}), );
                            }
                                _ => unreachable!("handler domain and routed variant diverged"),
                            },
                            HandlerKind::Sharing => match operation {
                                RoutedOperation::SharingCreate(Routed { id, input: SharingCreateInput { sharing_id, owner, target, permissions, state, created_at }, }) => {
                                let created_at = created_at.unwrap_or_else(unix_time_millis);
                                let permissions_json = serde_json::to_string(&permissions)
                                    .expect("sharing permissions serialize");
                                let query = format!( "on _sharings | insert {{sharingId: {}, owner: {}, target: {}, permissions: {permissions_json}, state: {}, createdAt: {created_at}}}", query_string(&sharing_id), query_string(&owner), query_string(&target), query_string(&state), );
                                execute_query_publish!( id, query, Audience::identities([owner.clone(), target.clone()]), "sharing.created", serde_json::json!({ "sharing": { "sharingId": sharing_id, "owner": owner, "target": target, "permissions": permissions, "state": state, "createdAt": created_at, } }), );
                            }
                                RoutedOperation::SharingUpdate(Routed { id, input: SharingUpdateInput { sharing_id, permissions, state, updated_at }, }) => {
                                let sharing = or_reject!(load_sharing(engine, id, &sharing_id));
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
                                execute_query_publish!( id, query, Audience::identities([ sharing.owner.clone(), sharing.target.clone(), ]), "sharing.updated", serde_json::json!({ "sharingId": sharing_id, "owner": sharing.owner, "target": sharing.target, "permissions": permissions, "state": state, "updatedAt": updated_at, }), );
                            }
                                RoutedOperation::SharingDelete(Routed { id, input: SharingDeleteInput { sharing_id, deleted_at }, }) => {
                                let sharing = or_reject!(load_sharing(engine, id, &sharing_id));
                                let deleted_at = deleted_at.unwrap_or_else(unix_time_millis);
                                let query = format!( "on _sharings | where sharingId == {} | delete", query_string(&sharing_id), );
                                execute_query_publish!( id, query, Audience::identities([ sharing.owner.clone(), sharing.target.clone(), ]), "sharing.deleted", serde_json::json!({ "sharingId": sharing_id, "owner": sharing.owner, "target": sharing.target, "deletedAt": deleted_at, }), );
                            }
                                _ => unreachable!("handler domain and routed variant diverged"),
                            },
                            HandlerKind::Place => match operation {
                                RoutedOperation::PlaceCreate(Routed { id, input: PlaceCreateInput { name, mood, public_access, created_at }, }) => {
                                let owner_identity_id = identity_or_reject!(id, "an authenticated identity is required to create a Place");
                                let place_id = UuidV7Generator::new().next_id().to_string();
                                let created_at = created_at.unwrap_or_else(unix_time_millis);
                                let public_access_field = public_access.map(|access| format!(", publicAccess: {}", query_string(access.as_str()))).unwrap_or_default();
                                let query = format!( "on _places | insert {{placeId: {}, name: {}, mood: {}, ownerIdentityId: {}, state: \"active\", createdAt: {created_at}{public_access_field}}}", query_string(&place_id), query_string(&name), query_string(&mood), query_string(owner_identity_id), );
                                execute_query_or_reject!(id, query);
                                reply!(id, serde_json::json!({ "placeId": place_id, "name": name, "mood": mood, "ownerIdentityId": owner_identity_id, "role": PlaceRole::Owner.as_str(), "publicAccess": public_access.map(PublicAccess::as_str), "state": "active", "createdAt": created_at, }),);
                            }
                                RoutedOperation::PlaceList(Routed { id, .. }) => {
                                if settings.authorization_mode.is_enforced() {
                                    let identity_id = identity_or_reject!(id, "an authenticated identity is required to list Places");
                                    respond!(id, list_places_for_identity(engine, id, identity_id), |places| serde_json::json!({ "places": places }));
                                } else {
                                    respond!(id, list_public_places(engine, id), |places| serde_json::json!({ "places": places }));
                                }
                            }
                                RoutedOperation::PlaceGet(Routed { id, input: PlaceIdInput { place_id } }) => {
                                let place = or_reject!(load_place(engine, id, &place_id));
                                let role = or_reject!(resolve_place_access_for_principal(engine, id, authentication.principal(), &place, !settings.authorization_mode.is_enforced()));
                                let public_role = matches!(authentication.principal(), Principal::Anonymous);
                                reply!(id, place.to_json((!public_role).then_some(role)));
                            }
                            RoutedOperation::PlaceUpdate(Routed { id, input: PlaceUpdateInput { place_id, name, title, subtitle, color_scheme, app_order, updated_at }, }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to update a Place");
                                let place = or_reject!(load_place(engine, id, &place_id));
                                place_owner_or_reject!(id, identity_id, &place, "only a Place Owner may update its presentation");
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
                                execute_query_or_reject!(id, query);
                                let updated = or_reject!(load_place(engine, id, &place_id));
                                let audience = or_reject!(place_audience(engine, id, &place_id));
                                publish_durable_event(engine, audience, "place.updated", serde_json::json!({
                                    "placeId": place_id,
                                    "changed": {
                                        "name": name.is_some(), "title": title.is_some(), "subtitle": subtitle.is_some(),
                                        "colorScheme": color_scheme.is_some(), "appOrder": app_order.is_some()
                                    },
                                    "updatedAt": updated_at
                                }));
                                reply!(id, updated.to_json(Some(PlaceRole::Owner)));
                            }
                                RoutedOperation::PlaceDelete(Routed { id, input: PlaceDeleteInput { place_id, deleted_at }, }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to delete a Place");
                                let place = or_reject!(load_place(engine, id, &place_id));
                                place_owner_or_reject!(id, identity_id, &place, "only the Place Owner may delete it");
                                let deleted_at = deleted_at.unwrap_or_else(unix_time_millis);
                                let audience = or_reject!(place_audience(engine, id, &place_id));
                                let detach_query = format!( "on _app_instances | where placeId == {} and state == \"active\" | set state = \"removed\", removedAt = {deleted_at}", query_string(&place_id), );
                                let detach_response =
                                    execute_request(engine, QueryRequest::new(id, detach_query));
                                if !detach_response.is_ok() {
                                    write_response(&mut writer, &detach_response)?;
                                    return Ok(());
                                }
                                let query = format!( "on _places | where placeId == {} and state == \"active\" | set state = \"deleted\", deletedAt = {deleted_at}", query_string(&place_id), );
                                execute_query_or_reject!(id, query);
                                publish_durable_event(engine, audience, "place.deleted", serde_json::json!({ "placeId": place_id, "deletedAt": deleted_at }));
                                reply!(id, serde_json::json!({ "placeId": place_id, "state": "deleted", "deletedAt": deleted_at, }),);
                            }
                                RoutedOperation::PlaceAccessList(Routed { id, input: PlaceIdInput { place_id } }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to inspect Place access");
                                let place = or_reject!(load_place(engine, id, &place_id));
                                place_role_or_reject!(id, identity_id, &place);
                                respond!(id, list_place_access(engine, id, &place), |entries| serde_json::json!({ "placeId": place_id, "ownerIdentityId": place.owner_identity_id, "entries": entries, }));
                            }
                                RoutedOperation::PlaceAccessSet(Routed { id, input: PlaceAccessSetInput { place_id, identity_id: target_identity_id, role }, }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to manage Place access");
                                let place = or_reject!(load_place(engine, id, &place_id));
                                place_owner_or_reject!(id, identity_id, &place, "only the Place Owner may change access");
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
                                    format!(
                                        "on _sharings | insert {{sharingId: {}, owner: {}, target: {}, permissions: {}, placeRole: {}, state: \"active\", createdAt: {created_at}}}",
                                        query_string(&sharing_id),
                                        query_string(&place.owner_identity_id),
                                        query_string(&target_identity_id),
                                        serde_json::to_string(&vec![permission.clone()])
                                            .expect("Place sharing permissions serialize"),
                                        query_string(role.as_str()),
                                    )
                                };

                                execute_query_or_reject!(id, query);
                                let audience = or_reject!(place_audience(engine, id, &place_id));
                                publish_durable_event(engine, audience, "place.access.updated", serde_json::json!({"placeId": place_id, "identityId": target_identity_id, "role": role.as_str()}));
                                reply!(id, serde_json::json!({"placeId": place_id, "identityId": target_identity_id, "role": role.as_str(), "state": "active"}));
                            }
                                RoutedOperation::PlaceAccessRemove(Routed { id, input: PlaceAccessRemoveInput { place_id, identity_id: target_identity_id }, }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to manage Place access");
                                let place = or_reject!(load_place(engine, id, &place_id));
                                place_owner_or_reject!(id, identity_id, &place, "only the Place Owner may change access");
                                if target_identity_id == place.owner_identity_id {
                                    reject!(id, "place.owner_immutable", "the Place Owner cannot be removed");
                                }

                                let sharing_id = place_access_sharing_id(&place_id, &target_identity_id);
                                if load_sharing(engine, id, &sharing_id).is_err() {
                                    reject!(id, "place.access_not_found", "the identity does not have a managed access entry for this Place");
                                }

                                let audience = or_reject!(place_audience(engine, id, &place_id));
                                let query = format!( "on _sharings | where sharingId == {} | delete", query_string(&sharing_id), );
                                execute_query_or_reject!(id, query);

                                publish_durable_event(
                                    engine,
                                    audience,
                                    "place.access.removed",
                                    serde_json::json!({
                                        "placeId": place_id,
                                        "identityId": target_identity_id,
                                    }),
                                );

                                reply!(id, serde_json::json!({ "placeId": place_id, "identityId": target_identity_id, "state": "removed", }),);
                            }
                                RoutedOperation::PlacePublicSet(Routed { id, input: PlacePublicSetInput { place_id, public_access } }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to change public Place access");
                                let place = or_reject!(load_place(engine, id, &place_id));
                                place_owner_or_reject!(id, identity_id, &place, "only the Place Owner may change public access");
                                let value = public_access.map(|access| query_string(access.as_str())).unwrap_or_else(|| "null".to_owned());
                                let query = format!("on _places | where placeId == {} and state == \"active\" | set publicAccess = {value}", query_string(&place_id));
                                let audience = or_reject!(place_audience(engine, id, &place_id));
                                execute_publish_reply!(id, query, audience, "place.public.updated", serde_json::json!({"placeId": place_id, "publicAccess": public_access.map(PublicAccess::as_str)}), serde_json::json!({"placeId": place_id, "publicAccess": public_access.map(PublicAccess::as_str)}));
                            }
                                RoutedOperation::PlaceResourceList(Routed { id, input: PlaceIdInput { place_id } }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to inspect Place resources");
                                let place = or_reject!(load_place(engine, id, &place_id));
                                place_role_or_reject!(id, identity_id, &place);
                                let access_entries = or_reject!(list_place_access(engine, id, &place));
                                let mut eligible_identity_ids = vec![place.owner_identity_id.clone()];
                                for entry in &access_entries {
                                    if entry.get("state").and_then(JsonValue::as_str) == Some("active") {
                                        if let Some(target) = entry.get("identityId").or_else(|| entry.get("target")).and_then(JsonValue::as_str) {
                                            if !eligible_identity_ids.iter().any(|value| value == target) { eligible_identity_ids.push(target.to_owned()); }
                                        }
                                    }
                                }
                                let mut eligible_devices = Vec::new();
                                for identity in &eligible_identity_ids {
                                    let query = format!("on _devices | where identityId == {} and state == \"active\" | select identityId, deviceId | sort deviceId", query_string(identity));
                                    if let QueryResponse::Ok { documents, .. } = execute_request(engine, QueryRequest::new(id, query)) {
                                        eligible_devices.extend(documents);
                                    }
                                }
                                reply!(id, serde_json::json!({
                                    "placeId": place_id,
                                    "assignments": place.resource_assignments,
                                    "eligibleDevices": eligible_devices,
                                }));
                            }
                            RoutedOperation::PlaceResourceSet(Routed { id, input: PlaceResourceSetInput { place_id, identity_id: node_identity_id, device_id: node_device_id, capability, role } }) => {
                                let owner_identity_id = identity_or_reject!(id, "an authenticated identity is required to manage Place resources");
                                let place = or_reject!(load_place(engine, id, &place_id));
                                place_owner_or_reject!(id, owner_identity_id, &place, "only a Place Owner may assign resource nodes");
                                if or_reject!(resolve_place_role(engine, id, &node_identity_id, &place)).is_none() {
                                    reject!(id, "place.resource.identity_not_attached", "the node identity must first be attached to the Place");
                                }
                                // A resource Node is a concrete registered Device of that Identity.
                                // Loading the credential validates both ownership and active device state.
                                let _node_device = or_reject!(load_device_credential(engine, id, &node_identity_id, &node_device_id));
                                if !valid_resource_capability(&capability) {
                                    reject!(id, "place.resource.invalid_capability", "invalid resource capability");
                                }
                                if !matches!(role.as_str(), "primary" | "replica" | "provider") {
                                    reject!(id, "place.resource.invalid_role", "resource role must be primary, replica or provider");
                                }
                                let mut assignments = place.resource_assignments.clone();
                                assignments.retain(|entry| {
                                    let same_identity = entry.get("identityId").or_else(|| entry.get("nodeIdentityId")).and_then(JsonValue::as_str) == Some(node_identity_id.as_str());
                                    let stored_device = entry.get("deviceId").or_else(|| entry.get("nodeDeviceId")).or_else(|| entry.get("nodeId")).and_then(JsonValue::as_str);
                                    let same_device = stored_device.is_none() || stored_device == Some(node_device_id.as_str());
                                    !(same_identity && same_device && entry.get("capability").and_then(JsonValue::as_str) == Some(capability.as_str()))
                                });
                                if role == "primary" {
                                    assignments.retain(|entry| {
                                        entry.get("capability").and_then(JsonValue::as_str) != Some(capability.as_str())
                                            || entry.get("role").and_then(JsonValue::as_str) != Some("primary")
                                    });
                                }
                                assignments.push(serde_json::json!({
                                    "identityId": node_identity_id,
                                    "deviceId": node_device_id,
                                    "capability": capability,
                                    "role": role,
                                    "assignedBy": owner_identity_id,
                                    "assignedAt": unix_time_millis(),
                                }));
                                let encoded = serde_json::to_string(&assignments).expect("resource assignments serialize");
                                execute_query_or_reject!(id, format!(
                                    "on _places | where placeId == {} and state == \"active\" | set resourceAssignments = {}, updatedAt = {}",
                                    query_string(&place_id), encoded, unix_time_millis()
                                ));
                                let audience = or_reject!(place_audience(engine, id, &place_id));
                                publish_durable_event(engine, audience, "place.resources.updated", serde_json::json!({ "placeId": place_id }));
                                reply!(id, serde_json::json!({ "placeId": place_id, "assignments": assignments }));
                            }
                            RoutedOperation::PlaceResourceRemove(Routed { id, input: PlaceResourceRemoveInput { place_id, identity_id: node_identity_id, device_id: node_device_id, capability } }) => {
                                let owner_identity_id = identity_or_reject!(id, "an authenticated identity is required to manage Place resources");
                                let place = or_reject!(load_place(engine, id, &place_id));
                                place_owner_or_reject!(id, owner_identity_id, &place, "only a Place Owner may remove resource assignments");
                                let mut assignments = place.resource_assignments.clone();
                                assignments.retain(|entry| {
                                    let same_identity = entry.get("identityId").or_else(|| entry.get("nodeIdentityId")).and_then(JsonValue::as_str) == Some(node_identity_id.as_str());
                                    let stored_device = entry.get("deviceId").or_else(|| entry.get("nodeDeviceId")).or_else(|| entry.get("nodeId")).and_then(JsonValue::as_str);
                                    let same_device = stored_device.is_none() || stored_device == Some(node_device_id.as_str());
                                    !(same_identity && same_device && entry.get("capability").and_then(JsonValue::as_str) == Some(capability.as_str()))
                                });
                                let encoded = serde_json::to_string(&assignments).expect("resource assignments serialize");
                                execute_query_or_reject!(id, format!(
                                    "on _places | where placeId == {} and state == \"active\" | set resourceAssignments = {}, updatedAt = {}",
                                    query_string(&place_id), encoded, unix_time_millis()
                                ));
                                let audience = or_reject!(place_audience(engine, id, &place_id));
                                publish_durable_event(engine, audience, "place.resources.updated", serde_json::json!({ "placeId": place_id }));
                                reply!(id, serde_json::json!({ "placeId": place_id, "assignments": assignments }));
                            }
                                _ => unreachable!("handler domain and routed variant diverged"),
                            },
                            HandlerKind::App => match operation {
                            RoutedOperation::QueryContextResolve(Routed { id, input: QueryContextResolveInput { place_id, app_instance_id } }) => {
                                let context = match resolve_query_execution_context(
                                    engine, id, authentication.principal(),
                                    RequestedExecutionContext { place_id, app_instance_id },
                                    !settings.authorization_mode.is_enforced(),
                                ) {
                                    Ok(context) => context,
                                    Err(response) => reject_response!(response),
                                };
                                reply!(id, serde_json::json!({
                                    "placeId": context.place_id,
                                    "appInstanceId": context.app_instance_id,
                                    "placeRole": context.place_role.as_str(),
                                }));
                            }
                            RoutedOperation::AppCreate(Routed { id, input: AppCreateInput { app_id, place_id, name, version, definition, created_at }, }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to create an App");
                                if let Some(ref place_id) = place_id {
                                    let place = or_reject!(load_place(engine, id, place_id));
                                    place_owner_or_reject!(id, identity_id, &place, "only the Place Owner may create an App in this Place");
                                } else {
                                    authorize_resource_or_reject!(id, OperationKind::AppCreate, &app_id);
                                }
                                if let Err(message) = validate_app_definition_model(&definition) {
                                    reject!(id, "app.invalid_definition", message);
                                }
                                match app_record_exists(engine, id, &app_id) {
                                    Ok(true) => reject!(id, "app.already_exists", "an App with this id already exists"),
                                    Ok(false) => {}
                                    Err(response) => reject_response!(response)
                                }
                                let created_at = created_at.unwrap_or_else(unix_time_millis);
                                let definition_json = serde_json::to_string(&definition).expect("App definition serializes");
                                let ownership_fields = place_id.as_ref().map(|owner_place_id| format!(", ownerPlaceId: {}, maintainersJson: {}", query_string(owner_place_id), query_string("[]"))).unwrap_or_default();
                                let query = format!( "on _apps | insert {{appId: {}, name: {}, version: {}, definition: {definition_json}, createdBy: {}, state: \"active\", createdAt: {created_at}{ownership_fields}}}", query_string(&app_id), query_string(&name), query_string(&version), query_string(identity_id), );
                                execute_query_or_reject!(id, query);

                                let instance_id = if let Some(ref place_id) = place_id {
                                    let instance_id = UuidV7Generator::new().next_id().to_string();
                                    let query = format!( "on _app_instances | insert {{instanceId: {}, placeId: {}, appId: {}, name: {}, config: {{}}, state: \"active\", createdAt: {created_at}}}", query_string(&instance_id), query_string(place_id), query_string(&app_id), query_string(&name), );
                                    execute_query_or_reject!(id, query);
                                    Some(instance_id)
                                } else { None };

                                publish_durable_event(engine, Audience::Global, "app.created", serde_json::json!({"appId": app_id, "placeId": place_id, "instanceId": instance_id, "name": name, "version": version, "createdBy": identity_id, "createdAt": created_at}));
                                reply!(id, serde_json::json!({"appId": app_id, "placeId": place_id, "instanceId": instance_id, "name": name, "version": version, "definition": definition, "createdBy": identity_id, "state": "active", "createdAt": created_at}));
                            }
                                RoutedOperation::AppList(Routed { id, .. }) => {
                                if settings.authorization_mode.is_enforced() { identity_or_reject!(id, "an authenticated identity is required to list Apps"); }
                                let documents = query_documents_or_reject!(id, "on _apps | where state == \"active\" | sort name");
                                reply!(id, serde_json::json!({ "apps": documents }));
                            }
                                RoutedOperation::AppGet(Routed { id, input: AppIdInput { app_id } }) => {
                                if settings.authorization_mode.is_enforced() { identity_or_reject!(id, "an authenticated identity is required to read an App"); }
                                reply!(id, or_reject!(load_app_definition(engine, id, &app_id)));
                            }
                                RoutedOperation::AppUpdate(Routed { id, input: AppUpdateInput { app_id, place_id, name, version, definition, maintainers, updated_at }, }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to update an App");
                                if let Some(ref place_id) = place_id {
                                    let place = or_reject!(load_place(engine, id, place_id));
                                    let role = place_role_or_reject!(id, identity_id, &place);
                                    let app = or_reject!(load_app_definition(engine, id, &app_id));
                                    let links = query_documents_or_reject!(id, format!(
                                        "on _app_instances | where placeId == {} and appId == {} and state == \"active\" | limit 1",
                                        query_string(place_id), query_string(&app_id),
                                    ));
                                    if links.is_empty() {
                                        reject!(id, "app.not_in_place", "the App is not attached to this Place");
                                    }
                                    let owner_place_id = app.get("ownerPlaceId").and_then(JsonValue::as_str);
                                    if owner_place_id.is_some_and(|owner| owner != place_id) {
                                        reject!(id, "app.update_forbidden", "this Place does not own the App definition");
                                    }
                                    if !role.can_manage() && !app_identity_is_maintainer(&app, identity_id) {
                                        reject!(id, "app.update_forbidden", "App management requires a Place Owner or App Maintainer");
                                    }
                                    if owner_place_id.is_none() && role.can_manage() {
                                        let claim = format!("on _apps | where appId == {} and state == \"active\" | set ownerPlaceId = {}, maintainersJson = {}", query_string(&app_id), query_string(place_id), query_string("[]"));
                                        execute_query_or_reject!(id, claim);
                                    }
                                } else {
                                    authorize_resource_or_reject!(id, OperationKind::AppUpdate, &app_id);
                                }
                                if let Some(ref requested_maintainers) = maintainers {
                                    let Some(ref managing_place_id) = place_id else {
                                        reject!(id, "app.maintainers_requires_place", "maintainers can only be changed with a Place context");
                                    };
                                    let mut normalized = requested_maintainers.iter().map(|value| value.trim()).filter(|value| !value.is_empty()).map(str::to_owned).collect::<Vec<_>>();
                                    normalized.sort(); normalized.dedup();
                                    let current_app = or_reject!(load_app_definition(engine, id, &app_id));
                                    let mut current = app_maintainers(&current_app); current.sort(); current.dedup();
                                    if normalized != current {
                                        let place = or_reject!(load_place(engine, id, managing_place_id));
                                        let role = place_role_or_reject!(id, identity_id, &place);
                                        if !role.can_manage() {
                                            reject!(id, "app.maintainers_forbidden", "only a Place Owner may change App maintainers");
                                        }
                                        let serialized = serde_json::to_string(&normalized).expect("maintainers serialize");
                                        let maintainers_query = format!("on _apps | where appId == {} and state == \"active\" | set maintainersJson = {}", query_string(&app_id), query_string(&serialized));
                                        execute_query_or_reject!(id, maintainers_query);
                                    }
                                }
                                if let Err(message) = validate_app_definition_model(&definition) {
                                    reject!(id, "app.invalid_definition", message);
                                }
                                let _ = or_reject!(load_app_definition(engine, id, &app_id));
                                let updated_at = updated_at.unwrap_or_else(unix_time_millis);
                                let definition_json =
                                    serde_json::to_string(&definition).expect("App definition serializes");
                                let query = format!( "on _apps | where appId == {} and state == \"active\" | set name = {}, version = {}, definition = {definition_json}, updatedBy = {}, updatedAt = {updated_at}", query_string(&app_id), query_string(&name), query_string(&version), query_string(identity_id), );
                                execute_publish_reply!( id, query, Audience::Global, "app.updated", serde_json::json!({"appId": app_id, "name": name, "version": version, "updatedBy": identity_id, "updatedAt": updated_at}), serde_json::json!({"appId": app_id, "name": name, "version": version, "definition": definition, "updatedBy": identity_id, "state": "active", "updatedAt": updated_at}), );
                            }
                                RoutedOperation::AppDelete(Routed { id, input: AppDeleteInput { app_id, deleted_at }, }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to delete an App");
                                authorize_resource_or_reject!(id, OperationKind::AppDelete, &app_id);
                                let _ = or_reject!(load_app_definition(engine, id, &app_id));
                                let instances = query_documents_or_reject!(id, format!(
                                    "on _app_instances | where appId == {} and state == \"active\" | limit 1",
                                    query_string(&app_id),
                                ));
                                if !instances.is_empty() {
                                    reject!(id, "app.in_use", "the App still has active instances");
                                }
                                let deleted_at = deleted_at.unwrap_or_else(unix_time_millis);
                                let query = format!( "on _apps | where appId == {} and state == \"active\" | set state = \"deleted\", deletedBy = {}, deletedAt = {deleted_at}", query_string(&app_id), query_string(identity_id), );
                                execute_publish_reply!( id, query, Audience::Global, "app.deleted", serde_json::json!({"appId": app_id, "deletedBy": identity_id, "deletedAt": deleted_at}), serde_json::json!({"appId": app_id, "state": "deleted", "deletedBy": identity_id, "deletedAt": deleted_at}), );
                            }
                                RoutedOperation::AppInstanceCreate(Routed { id, input: AppInstanceCreateInput { place_id, app_id, name, config, created_at }, }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to attach an App");
                                let place = or_reject!(load_place(engine, id, &place_id));
                                place_owner_or_reject!(id, identity_id, &place, "only the Place Owner may attach an App");
                                let _ = or_reject!(load_app_definition(engine, id, &app_id));
                                let instance_id = UuidV7Generator::new().next_id().to_string();
                                let created_at = created_at.unwrap_or_else(unix_time_millis);
                                let config_json =
                                    serde_json::to_string(&config).expect("App config serializes");
                                let name_field = name
                                    .as_deref()
                                    .map(|value| format!(", name: {}", query_string(value)))
                                    .unwrap_or_default();
                                let query = format!( "on _app_instances | insert {{instanceId: {}, placeId: {}, appId: {}{name_field}, config: {config_json}, state: \"active\", createdAt: {created_at}}}", query_string(&instance_id), query_string(&place_id), query_string(&app_id), );
                                execute_query_or_reject!(id, query);
                                let audience = or_reject!(place_audience(engine, id, &place_id));
                                publish_durable_event(engine, audience, "app.instance.created", serde_json::json!({
                                    "instanceId": instance_id, "placeId": place_id, "appId": app_id
                                }));
                                reply!(id, serde_json::json!({ "instanceId": instance_id, "placeId": place_id, "appId": app_id, "name": name, "config": config, "state": "active", "createdAt": created_at, }),);
                            }
                                RoutedOperation::AppInstanceList(Routed { id, input: PlaceIdInput { place_id } }) => {
                                let place = or_reject!(load_place(engine, id, &place_id));
                                let role = or_reject!(resolve_place_access_for_principal(engine, id, authentication.principal(), &place, !settings.authorization_mode.is_enforced()));
                                let documents = query_documents_or_reject!(id, format!(
                                    "on _app_instances | where placeId == {} and state == \"active\" | sort createdAt",
                                    query_string(&place_id),
                                ));
                                reply!(id, serde_json::json!({"placeId": place_id, "role": role.as_str(), "instances": documents}));
                            }
                                RoutedOperation::AppInstanceRemove(Routed { id, input: AppInstanceRemoveInput { instance_id, removed_at }, }) => {
                                let identity_id = identity_or_reject!(id, "an authenticated identity is required to remove an App");
                                let instance = or_reject!(load_app_instance(engine, id, &instance_id));
                                let place = or_reject!(load_place(engine, id, &instance.place_id));
                                place_owner_or_reject!(id, identity_id, &place, "only the Place Owner may remove an App");
                                let removed_at = removed_at.unwrap_or_else(unix_time_millis);
                                let query = format!( "on _app_instances | where instanceId == {} and state == \"active\" | set state = \"removed\", removedAt = {removed_at}", query_string(&instance_id), );
                                execute_query_or_reject!(id, query);
                                let audience = or_reject!(place_audience(engine, id, &instance.place_id));
                                publish_durable_event(engine, audience, "app.instance.removed", serde_json::json!({
                                    "instanceId": instance_id, "placeId": instance.place_id
                                }));
                                reply!(id, serde_json::json!({ "instanceId": instance_id, "placeId": instance.place_id, "state": "removed", "removedAt": removed_at, }),);
                            }
                                RoutedOperation::DataAnalyze(Routed { id, input: DataAnalyzeInput { place_id, files_instance_id, file_id, worker_result, .. } }) => {
                                    let identity_id=identity_or_reject!(id,"an authenticated identity is required to analyze data");
                                    let place=or_reject!(load_place(engine,id,&place_id)); let role=place_role_or_reject!(id,identity_id,&place);
                                    if !role.can_write(){reject!(id,"authorization.denied","data analysis requires write access to the Place");}
                                    let result=if let Some(result)=worker_result {
                                        result
                                    } else if settings.service_capabilities.contains(ServiceCapability::DataImport) {
                                        or_reject!(run_data_worker_for_file(settings,engine,id,&place_id,&files_instance_id,&file_id,"analyze",None))
                                    } else {
                                        reject!(id,"capability.unavailable","data.analyze requires a data.import worker result on this database node");
                                    };
                                    let fingerprint=result.get("fingerprint").and_then(JsonValue::as_str).unwrap_or_default();
                                    let mappings=if fingerprint.is_empty(){Vec::new()}else{query_documents_or_reject!(id,format!("on _data_mappings | where fingerprint == {} and state == \"active\" | limit 5",query_string(fingerprint)))};
                                    let mut response=result; if let Some(object)=response.as_object_mut(){object.insert("recognized".into(),JsonValue::Bool(!mappings.is_empty())); object.insert("mappings".into(),JsonValue::Array(mappings));}
                                    reply!(id,response);
                                }
                                RoutedOperation::DataMappingSave(Routed { id, input: DataMappingSaveInput { place_id, fingerprint, name, target_app_id, target_table, definition } }) => {
                                    let identity_id=identity_or_reject!(id,"an authenticated identity is required to save a mapping");
                                    let place=or_reject!(load_place(engine,id,&place_id)); let role=place_role_or_reject!(id,identity_id,&place);
                                    if !role.can_write(){reject!(id,"authorization.denied","saving a Mapping requires write access to the Place");}
                                    let mapping_id=UuidV7Generator::new().next_id().to_string(); let now=unix_time_millis();
                                    let query=format!("on _data_mappings | insert {{mappingId:{},placeId:{},fingerprint:{},name:{},targetAppId:{},targetTable:{},definition:{},createdBy:{},state:\"active\",createdAt:{}}}",query_string(&mapping_id),query_string(&place_id),query_string(&fingerprint),query_string(&name),query_string(&target_app_id),query_string(&target_table),serde_json::to_string(&definition).expect("mapping serializes"),query_string(identity_id),now);
                                    execute_query_or_reject!(id,query); reply!(id,serde_json::json!({"mappingId":mapping_id,"fingerprint":fingerprint,"name":name,"targetAppId":target_app_id,"targetTable":target_table,"definition":definition}));
                                }
                                RoutedOperation::DataImport(Routed { id, input: DataImportInput { place_id, files_instance_id, file_id, target_instance_id, table, mapping, mode, worker_result, plan_only } }) => {
                                    let identity_id=identity_or_reject!(id,"an authenticated identity is required to import data");
                                    let place=or_reject!(load_place(engine,id,&place_id)); let role=place_role_or_reject!(id,identity_id,&place);
                                    if !role.can_write(){reject!(id,"authorization.denied","data import requires write access to the Place");}
                                    let instance_docs=query_documents_or_reject!(id,format!("on _app_instances | where instanceId == {} and placeId == {} and state == \"active\" | limit 1",query_string(&target_instance_id),query_string(&place_id)));
                                    let Some(instance_doc)=instance_docs.first() else {reject!(id,"app.instance_not_found","target App instance was not found in this Place");};
                                    let app_id=instance_doc.get("appId").and_then(JsonValue::as_str).unwrap_or_default(); if app_id.is_empty(){reject!(id,"app.invalid_instance_record","target App instance has no appId");}
                                    let app=or_reject!(load_app_definition(engine,id,app_id)); let collection=match resolve_app_table_collection(&app,&table){Some(v)=>v,None=>reject!(id,"data.invalid_target","target table is not declared by the App")};
                                    let result=if let Some(result)=worker_result {
                                        result
                                    } else if settings.service_capabilities.contains(ServiceCapability::DataImport) {
                                        or_reject!(run_data_worker_for_file(settings,engine,id,&place_id,&files_instance_id,&file_id,"import",Some(&mapping)))
                                    } else {
                                        reject!(id,"capability.unavailable","data.import requires a data.import worker result on this database node");
                                    };
                                    let documents=result.get("documents").and_then(JsonValue::as_array).cloned().unwrap_or_default();
                                    if documents.iter().any(|document| !document.is_object()) {
                                        reject!(id,"data.invalid_worker_output","data worker returned a non-object document");
                                    }
                                    let mode=mode.as_deref().or_else(||mapping.get("import").and_then(|v|v.get("mode")).and_then(JsonValue::as_str)).unwrap_or("append");

                                    // In Gateway fabric mode the master is the control plane only. It
                                    // authorizes Place/AppInstance/table here, but the Gateway executes
                                    // the resulting scoped mutations on the selected database provider.
                                    if plan_only {
                                        reply!(id,serde_json::json!({
                                            "status":"planned",
                                            "placeId":place_id,
                                            "appInstanceId":target_instance_id,
                                            "placeRole":role.as_str(),
                                            "collection":collection,
                                            "mode":mode,
                                            "rows":documents.len(),
                                            "warnings":result.get("warnings").cloned().unwrap_or(JsonValue::Array(vec![]))
                                        }));
                                        return Ok(());
                                    }

                                    // data.import is already authorized against the Place and the target
                                    // AppInstance above. Persist through the normal scoped query executor so
                                    // DocumentScope remains the single authority for _place/_app_instance.
                                    //
                                    // Do NOT use the row-local `load` stage here: `on collection | load ...`
                                    // transforms rows already present in the source collection and therefore
                                    // inserts nothing when the collection is empty. The dedicated streaming
                                    // load path is not scoped yet, so V1 deliberately uses scoped inserts.
                                    let import_context=ExecutionContext{
                                        principal:authentication.principal().clone(),
                                        place_id:place_id.clone(),
                                        app_instance_id:target_instance_id.clone(),
                                        place_role:role,
                                        public_access:None,
                                    };

                                    if mode=="replace" || mode=="replace_all" {
                                        let response=execute_request_scoped(
                                            engine,
                                            QueryRequest::new(id,format!("on {} | delete",collection)),
                                            Some(&import_context),
                                        );
                                        if !response.is_ok(){reject_response!(response);}
                                    }

                                    let mut imported=0usize;
                                    for document in documents {
                                        if !document.is_object(){
                                            reject!(id,"data.invalid_worker_output","data worker returned a non-object document");
                                        }
                                        let query=format!(
                                            "on {} | insert {}",
                                            collection,
                                            serde_json::to_string(&document).expect("worker document serializes"),
                                        );
                                        let response=execute_request_scoped(
                                            engine,
                                            QueryRequest::new(id,query),
                                            Some(&import_context),
                                        );
                                        if !response.is_ok(){reject_response!(response);}
                                        imported=imported.saturating_add(1);
                                    }
                                    let audience = or_reject!(place_audience(engine, id, &place_id));
                                    publish_durable_event(engine, audience, "app.data.changed", serde_json::json!({
                                        "placeId": place_id, "appInstanceId": target_instance_id, "table": table, "rows": imported
                                    }));
                                    reply!(id,serde_json::json!({"status":"completed","rows":imported,"mode":mode,"warnings":result.get("warnings").cloned().unwrap_or(JsonValue::Array(vec![]))}));
                                }
                                _ => unreachable!("handler domain and routed variant diverged"),
                            },
                            _ => unreachable!("execution mode and handler domain diverged"),
                        }
    Ok(())
}

fn ensure_authenticated_keepalive_type(types: &mut Vec<String>) {
    if !types
        .iter()
        .any(|value| value == "*" || value == "core.*" || value == "core.heartbeat")
    {
        types.push("core.heartbeat".to_owned());
    }
}

fn drain_events( connection_id: u64, writer: &mut TcpStream, subscription: Option<&EventSubscription>, principal: &Principal, ) -> Result<(), ConnectionError> {
    let Some(subscription) = subscription else {
        return Ok(());
    };
    loop {
        match subscription.try_recv() {
            Ok(event) if event_visible_to(&event.audience, principal) => {
                write_event(connection_id, writer, &event)?;
            }
            Ok(_) => {}
            Err(std::sync::mpsc::TryRecvError::Empty) => return Ok(()),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => return Ok(()),
        }
    }
}

fn event_visible_to(audience: &Audience, principal: &Principal) -> bool {
    match audience {
        Audience::Global => true,
        Audience::Identity { identity_id } => principal_identity_id(principal) == Some(identity_id.as_str()),
        Audience::Identities { identity_ids } => principal_identity_id(principal)
            .is_some_and(|identity_id| identity_ids.iter().any(|candidate| candidate == identity_id)),
    }
}

fn write_event( connection_id: u64, writer: &mut TcpStream, event: &og_core::CoreEvent, ) -> Result<(), ConnectionError> {
    debug::log(
        DebugTopic::Events,
        Some(connection_id),
        format!("deliver type={} event_id={}", event.event_type, event.id),
    );
    if debug::protocol_enabled() {
        let value = serde_json::to_value(event)
            .map(debug::redact_json)
            .unwrap_or(JsonValue::Null);
        debug::log(
            DebugTopic::Protocol,
            Some(connection_id),
            format!("> event {}", value),
        );
    }
    let encoded = encode_message(event, MessageKind::Response, MAX_RESPONSE_BYTES)
        .map_err(ConnectionError::Encode)?;
    writer.write_all(&encoded).map_err(ConnectionError::Write)
}

fn write_operation_response( writer: &mut TcpStream, response: &OperationResponse, ) -> Result<(), ConnectionError> {
    if debug::protocol_enabled() {
        let value = serde_json::to_value(response)
            .map(debug::redact_json)
            .unwrap_or(JsonValue::Null);
        debug::log(DebugTopic::Protocol, None, format!("> response {}", value));
    }
    let encoded = encode_message(response, MessageKind::Response, MAX_RESPONSE_BYTES)
        .map_err(ConnectionError::Encode)?;
    writer.write_all(&encoded).map_err(ConnectionError::Write)
}

fn pending_auth_subject(authentication: &ConnectionAuth) -> Option<(String, String)> {
    // The challenge subject stays private to ConnectionAuth; auth.complete reloads the
    // only device requested by the client and ConnectionAuth verifies the binding.
    // The wire request carries no identity/device again, avoiding substitution.
    authentication.pending_subject()
}

fn try_load_device_credential( engine: &Engine, request_id: RequestId, identity_id: &str, device_id: &str, ) -> Result<Option<DeviceCredential>, QueryResponse> {
    let query = format!( "on _devices | where deviceId == {} and identityId == {} | limit 1", query_string(device_id), query_string(identity_id), );
    let response = execute_request(engine, QueryRequest::new(request_id.clone(), query));
    match response {
        QueryResponse::Ok { documents, .. } => {
            let Some(document) = documents.into_iter().next() else {
                return Ok(None);
            };
            let text = |field: &str| {
                document
                    .get(field)
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned)
            };
            let credential = DeviceCredential {
                identity_id: text("identityId").unwrap_or_default(),
                device_id: text("deviceId").unwrap_or_default(),
                public_key: text("publicKey").unwrap_or_default(),
                algorithm: text("algorithm").unwrap_or_default(),
                encoding: text("encoding").unwrap_or_default(),
                active: document.get("state").and_then(JsonValue::as_str) == Some("active"),
            };
            if credential.identity_id.is_empty()
                || credential.device_id.is_empty()
                || credential.public_key.is_empty()
            {
                return Err(QueryResponse::request_error(
                    request_id,
                    "auth.invalid_device_record",
                    "device credential is incomplete",
                ));
            }
            Ok(Some(credential))
        }
        error @ QueryResponse::Error { .. } => Err(error),
    }
}

fn load_device_credential( engine: &Engine, request_id: RequestId, identity_id: &str, device_id: &str, ) -> Result<DeviceCredential, QueryResponse> {
    match try_load_device_credential(engine, request_id.clone(), identity_id, device_id)? {
        Some(credential) => Ok(credential),
        None => Err(QueryResponse::request_error(
            request_id,
            "auth.device_not_found",
            "active device credential was not found",
        )),
    }
}

fn valid_resource_capability(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

#[derive(Debug, Clone)]
struct PlaceRecord {
    place_id: String,
    name: String,
    mood: String,
    title: String,
    subtitle: String,
    color_scheme: String,
    app_order: Vec<String>,
    resource_assignments: Vec<JsonValue>,
    owner_identity_id: String,
    public_access: Option<PublicAccess>,
    created_at: Option<u64>,
}

impl PlaceRecord {
    fn to_json(&self, role: Option<PlaceRole>) -> JsonValue {
        serde_json::json!({
            "placeId": self.place_id.clone(),
            "name": self.name.clone(),
            "mood": self.mood.clone(),
            "title": self.title.clone(),
            "subtitle": self.subtitle.clone(),
            "colorScheme": self.color_scheme.clone(),
            "appOrder": self.app_order.clone(),
            "resourceAssignments": self.resource_assignments.clone(),
            "ownerIdentityId": self.owner_identity_id.clone(),
            "role": role.map(PlaceRole::as_str),
            "publicAccess": self.public_access.map(PublicAccess::as_str),
            "state": "active",
            "createdAt": self.created_at,
        })
    }
}

#[derive(Debug, Clone)]
struct AppInstanceRecord {
    place_id: String,
}

fn principal_identity_id(principal: &Principal) -> Option<&str> {
    match principal {
        Principal::Identity { identity_id, .. } => Some(identity_id.as_str()),
        Principal::Anonymous => None,
    }
}

fn load_place( engine: &Engine, request_id: RequestId, place_id: &str, ) -> Result<PlaceRecord, QueryResponse> {
    let query = format!( "on _places | where placeId == {} and state == \"active\" | limit 1", query_string(place_id), );
    match execute_request(engine, QueryRequest::new(request_id, query)) {
        QueryResponse::Ok { documents, .. } => {
            let Some(document) = documents.into_iter().next() else {
                return Err(QueryResponse::request_error(
                    request_id,
                    "place.not_found",
                    "Place was not found",
                ));
            };
            let place_id = document
                .get("placeId")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            let name = document
                .get("name")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            let mood = document
                .get("mood")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            let title = document.get("title").and_then(JsonValue::as_str).unwrap_or_default().to_owned();
            let subtitle = document.get("subtitle").and_then(JsonValue::as_str).unwrap_or_default().to_owned();
            let color_scheme = document.get("colorScheme").and_then(JsonValue::as_str).unwrap_or("glacier").to_owned();
            let app_order = document.get("appOrder").and_then(JsonValue::as_array).map(|items| items.iter().filter_map(JsonValue::as_str).map(str::to_owned).collect()).unwrap_or_default();
            let resource_assignments = document.get("resourceAssignments").and_then(JsonValue::as_array).cloned().unwrap_or_default();
            let owner_identity_id = document
                .get("ownerIdentityId")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            let public_access = match document.get("publicAccess").and_then(JsonValue::as_str) {
                None => None,
                Some("readonly") => Some(PublicAccess::Readonly),
                Some("readwrite") => Some(PublicAccess::Readwrite),
                Some(_) => {
                    return Err(QueryResponse::request_error(
                        request_id,
                        "place.invalid_record",
                        "Place publicAccess must be readonly or readwrite",
                    ));
                }
            };
            let created_at = document.get("createdAt").and_then(JsonValue::as_u64);
            if place_id.is_empty() || name.is_empty() || owner_identity_id.is_empty() {
                return Err(QueryResponse::request_error(
                    request_id,
                    "place.invalid_record",
                    "Place record is incomplete",
                ));
            }
            Ok(PlaceRecord {
                place_id,
                name,
                mood,
                title,
                subtitle,
                color_scheme,
                app_order,
                resource_assignments,
                owner_identity_id,
                public_access,
                created_at,
            })
        }
        error @ QueryResponse::Error { .. } => Err(error),
    }
}

fn app_record_exists( engine: &Engine, request_id: RequestId, app_id: &str, ) -> Result<bool, QueryResponse> {
    let query = format!( "on _apps | where appId == {} | limit 1", query_string(app_id), );
    match execute_request(engine, QueryRequest::new(request_id, query)) {
        QueryResponse::Ok { documents, .. } => Ok(!documents.is_empty()),
        error @ QueryResponse::Error { .. } => Err(error),
    }
}

fn app_maintainers(app: &JsonValue) -> Vec<String> {
    if let Some(values) = app.get("maintainers").and_then(JsonValue::as_array) {
        return values.iter().filter_map(JsonValue::as_str).map(str::to_owned).collect();
    }
    app.get("maintainersJson")
        .and_then(JsonValue::as_str)
        .and_then(|serialized| serde_json::from_str::<Vec<String>>(serialized).ok())
        .unwrap_or_default()
}

fn app_identity_is_maintainer(app: &JsonValue, identity_id: &str) -> bool {
    app_maintainers(app).iter().any(|value| value == identity_id)
}

fn load_app_definition( engine: &Engine, request_id: RequestId, app_id: &str, ) -> Result<JsonValue, QueryResponse> {
    let query = format!( "on _apps | where appId == {} and state == \"active\" | limit 1", query_string(app_id), );
    match execute_request(engine, QueryRequest::new(request_id, query)) {
        QueryResponse::Ok { documents, .. } => {
            let Some(document) = documents.into_iter().next() else {
                return Err(QueryResponse::request_error(
                    request_id,
                    "app.not_found",
                    "App was not found",
                ));
            };
            Ok(document)
        }
        error @ QueryResponse::Error { .. } => Err(error),
    }
}

fn load_app_instance( engine: &Engine, request_id: RequestId, instance_id: &str, ) -> Result<AppInstanceRecord, QueryResponse> {
    let query = format!( "on _app_instances | where instanceId == {} and state == \"active\" | limit 1", query_string(instance_id), );
    match execute_request(engine, QueryRequest::new(request_id, query)) {
        QueryResponse::Ok { documents, .. } => {
            let Some(document) = documents.into_iter().next() else {
                return Err(QueryResponse::request_error(
                    request_id,
                    "app.instance_not_found",
                    "App instance was not found",
                ));
            };
            let place_id = document
                .get("placeId")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            if place_id.is_empty() {
                return Err(QueryResponse::request_error(
                    request_id,
                    "app.invalid_instance_record",
                    "App instance has no valid Place",
                ));
            }
            Ok(AppInstanceRecord { place_id })
        }
        error @ QueryResponse::Error { .. } => Err(error),
    }
}

fn place_access_sharing_id(place_id: &str, identity_id: &str) -> String {
    format!("place-access:{place_id}:{identity_id}")
}

fn list_place_access( engine: &Engine, request_id: RequestId, place: &PlaceRecord, ) -> Result<Vec<serde_json::Value>, QueryResponse> {
    let query = format!( "on _sharings | where owner == {} | select sharingId, target, permissions, placeRole, state | sort target", query_string(&place.owner_identity_id), );
    match execute_request(engine, QueryRequest::new(request_id, query)) {
        QueryResponse::Ok { documents, .. } => {
            use std::collections::BTreeMap;
            let mut entries: BTreeMap<String, PlaceRole> = BTreeMap::new();
            for document in documents {
                let state = document
                    .get("state")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default();
                if state != "accepted" && state != "active" {
                    continue;
                }
                let Some(target) = document.get("target").and_then(JsonValue::as_str) else {
                    continue;
                };
                // `placeRole` is a scalar optimization used by managed Place access
                // records. It is only scoped by the stable sharingId. Without checking
                // that id, an access entry for another Place owned by the same identity
                // would leak into this Place's access list.
                let expected_sharing_id = place_access_sharing_id(&place.place_id, target);
                if document.get("sharingId").and_then(JsonValue::as_str) == Some(expected_sharing_id.as_str()) {
                    if let Some(candidate) = document
                        .get("placeRole")
                        .and_then(JsonValue::as_str)
                        .and_then(PlaceRole::parse)
                    {
                        entries.insert(target.to_owned(), candidate);
                        continue;
                    }
                }
                let Some(permissions) = document.get("permissions").and_then(JsonValue::as_array)
                else {
                    continue;
                };
                for permission in permissions {
                    let Some(permission) = permission.as_str() else {
                        continue;
                    };
                    let Some((place_id, candidate)) = parse_sharing_permission(permission) else {
                        continue;
                    };
                    if place_id != place.place_id {
                        continue;
                    }
                    let current = entries.get(target).copied();
                    let selected = match (current, candidate) {
                        (Some(PlaceRole::Owner), _) | (_, PlaceRole::Owner) => PlaceRole::Owner,
                        (Some(PlaceRole::Resident), _) | (_, PlaceRole::Resident) => PlaceRole::Resident,
                        _ => PlaceRole::Member,
                    };
                    entries.insert(target.to_owned(), selected);
                }
            }
            Ok(entries
                .into_iter()
                .map(|(identity_id, role)| {
                    serde_json::json!({
                        "identityId": identity_id,
                        "role": role.as_str(),
                        "state": "active",
                    })
                })
                .collect())
        }
        error @ QueryResponse::Error { .. } => Err(error),
    }
}

fn place_audience(
    engine: &Engine,
    request_id: RequestId,
    place_id: &str,
) -> Result<Audience, QueryResponse> {
    let place = load_place(engine, request_id, place_id)?;
    let mut identities = vec![place.owner_identity_id.clone()];
    for entry in list_place_access(engine, request_id, &place)? {
        if let Some(identity_id) = entry.get("identityId").and_then(JsonValue::as_str) {
            identities.push(identity_id.to_owned());
        }
    }
    Ok(Audience::identities(identities))
}

fn resolve_place_role(
    engine: &Engine,
    request_id: RequestId,
    identity_id: &str,
    place: &PlaceRecord,
) -> Result<Option<PlaceRole>, QueryResponse> {
    if place.owner_identity_id == identity_id {
        return Ok(Some(PlaceRole::Owner));
    }

    let query = format!( "on _sharings | where owner == {} and target == {} | select sharingId, permissions, placeRole, state", query_string(&place.owner_identity_id), query_string(identity_id), );
    match execute_request(engine, QueryRequest::new(request_id, query)) {
        QueryResponse::Ok { documents, .. } => {
            let mut role = None;
            for document in documents {
                let state = document
                    .get("state")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default();
                if state != "accepted" && state != "active" {
                    continue;
                }
                // A scalar `placeRole` belongs to one managed Place access record.
                // Scope it by sharingId so a grant on another Place cannot authorize
                // this Place merely because owner and target identities are the same.
                let expected_sharing_id = place_access_sharing_id(&place.place_id, identity_id);
                if document.get("sharingId").and_then(JsonValue::as_str) == Some(expected_sharing_id.as_str()) {
                    if let Some(candidate) = document
                        .get("placeRole")
                        .and_then(JsonValue::as_str)
                        .and_then(PlaceRole::parse)
                    {
                        role = Some(candidate);
                        continue;
                    }
                }
                let Some(permissions) = document.get("permissions").and_then(JsonValue::as_array)
                else {
                    continue;
                };
                for permission in permissions {
                    let Some(permission) = permission.as_str() else {
                        continue;
                    };
                    let Some((place_id, candidate)) = parse_sharing_permission(permission) else {
                        continue;
                    };
                    if place_id != place.place_id {
                        continue;
                    }
                    role = match (role, candidate) {
                        (Some(PlaceRole::Owner), _) | (_, PlaceRole::Owner) => Some(PlaceRole::Owner),
                        (Some(PlaceRole::Resident), _) | (_, PlaceRole::Resident) => Some(PlaceRole::Resident),
                        _ => Some(PlaceRole::Member),
                    };
                }
            }
            Ok(role)
        }
        error @ QueryResponse::Error { .. } => Err(error),
    }
}

fn resolve_place_access_for_principal(
    engine: &Engine,
    request_id: RequestId,
    principal: &Principal,
    place: &PlaceRecord,
    allow_public: bool,
) -> Result<PlaceRole, QueryResponse> {
    if let Some(identity_id) = principal_identity_id(principal) {
        return resolve_place_role(engine, request_id, identity_id, place)?.ok_or_else(|| {
            QueryResponse::request_error(
                request_id,
                "authorization.denied",
                format!("identity {:?} has no access to Place {:?}", identity_id, place.place_id),
            )
        });
    }

    if allow_public {
        if let Some(access) = place.public_access {
            return Ok(access.place_role());
        }
        return Err(QueryResponse::request_error(
            request_id,
            "place.not_public",
            "Place is not publicly accessible",
        ));
    }

    Err(QueryResponse::request_error(
        request_id,
        "authorization.required",
        "authentication is required to access this Place",
    ))
}

fn list_public_places(engine: &Engine, request_id: RequestId) -> Result<Vec<JsonValue>, QueryResponse> {
    let places = match execute_request(
        engine,
        QueryRequest::new(request_id, "on _places | where state == \"active\" | sort createdAt"),
    ) {
        QueryResponse::Ok { documents, .. } => documents,
        error @ QueryResponse::Error { .. } => return Err(error),
    };
    let mut visible = Vec::new();
    for document in places {
        let Some(place_id) = document.get("placeId").and_then(JsonValue::as_str) else { continue; };
        let place = match load_place(engine, request_id, place_id) { Ok(place) => place, Err(_) => continue };
        if place.public_access.is_some() { visible.push(place.to_json(None)); }
    }
    Ok(visible)
}

fn resolve_query_execution_context(
    engine: &Engine,
    request_id: RequestId,
    principal: &Principal,
    requested: RequestedExecutionContext,
    allow_public: bool,
) -> Result<ExecutionContext, QueryResponse> {
    let place = load_place(engine, request_id, &requested.place_id)?;
    let place_role = resolve_place_access_for_principal(
        engine,
        request_id,
        principal,
        &place,
        allow_public,
    )?;

    let instance = load_app_instance(engine, request_id, &requested.app_instance_id)?;
    if instance.place_id != requested.place_id {
        return Err(QueryResponse::request_error(
            request_id,
            "app.instance_place_mismatch",
            format!(
                "App instance {:?} does not belong to Place {:?}",
                requested.app_instance_id, requested.place_id
            ),
        ));
    }

    Ok(ExecutionContext {
        principal: principal.clone(),
        place_id: requested.place_id,
        app_instance_id: requested.app_instance_id,
        place_role,
        public_access: if matches!(principal, Principal::Anonymous) { place.public_access } else { None },
    })
}

fn list_places_for_identity(
    engine: &Engine,
    request_id: RequestId,
    identity_id: &str,
) -> Result<Vec<JsonValue>, QueryResponse> {
    let places = match execute_request(
        engine,
        QueryRequest::new(
            request_id,
            "on _places | where state == \"active\" | sort createdAt",
        ),
    ) {
        QueryResponse::Ok { documents, .. } => documents,
        error @ QueryResponse::Error { .. } => return Err(error),
    };

    let mut visible = Vec::new();
    for document in places {
        let place_id = document
            .get("placeId")
            .and_then(JsonValue::as_str)
            .unwrap_or_default()
            .to_owned();
        if place_id.is_empty() {
            continue;
        }
        let place = match load_place(engine, request_id, &place_id) {
            Ok(place) => place,
            Err(_) => continue,
        };
        if let Some(role) = resolve_place_role(engine, request_id, identity_id, &place)? {
            visible.push(place.to_json(Some(role)));
        }
    }
    Ok(visible)
}

#[derive(Debug, Clone)]
struct SharingRecord {
    owner: String,
    target: String,
}

fn load_sharing(
    engine: &Engine,
    request_id: RequestId,
    sharing_id: &str,
) -> Result<SharingRecord, QueryResponse> {
    let query = format!( "on _sharings | where sharingId == {} | limit 1", query_string(sharing_id), );
    match execute_request(engine, QueryRequest::new(request_id, query)) {
        QueryResponse::Ok { documents, .. } => {
            let Some(document) = documents.into_iter().next() else {
                return Err(QueryResponse::request_error(
                    request_id,
                    "sharing.not_found",
                    "sharing relation was not found",
                ));
            };
            let owner = document
                .get("owner")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            let target = document
                .get("target")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .to_owned();
            if owner.is_empty() || target.is_empty() {
                return Err(QueryResponse::request_error(
                    request_id,
                    "sharing.invalid_record",
                    "sharing relation has no valid owner or target",
                ));
            }
            Ok(SharingRecord { owner, target })
        }
        error @ QueryResponse::Error { .. } => Err(error),
    }
}

fn write_auth_error(
    writer: &mut TcpStream,
    request_id: RequestId,
    error: og_core::AuthError,
) -> Result<(), ConnectionError> {
    write_response(
        writer,
        &QueryResponse::request_error(request_id, "auth.failed", error.to_string()),
    )
}

fn query_string(value: &str) -> String {
    serde_json::to_string(value).expect("serializing a Rust string to JSON cannot fail")
}

fn read_message(
    reader: &mut BufReader<TcpStream>,
    payload: &mut Vec<u8>,
) -> Result<usize, ConnectionError> {
    let mut header = [0_u8; LENGTH_PREFIX_BYTES];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(0),
        Err(error) => return Err(ConnectionError::Read(error)),
    }

    let length = u32::from_be_bytes(header) as usize;
    ensure_payload_size(MessageKind::Request, length, MAX_REQUEST_BYTES)
        .map_err(ConnectionError::Protocol)?;

    payload.resize(length, 0);
    reader.read_exact(payload).map_err(ConnectionError::Read)?;
    Ok(length)
}

fn try_write_streaming_request(
    engine: &Engine,
    writer: &mut TcpStream,
    request: &QueryRequest,
) -> Result<bool, ConnectionError> {
    let started = Instant::now();
    let parse_started = Instant::now();
    let pipeline = match parse_pipeline(&request.query) {
        Ok(pipeline) => pipeline,
        Err(_) => return Ok(false),
    };
    let parse_us = elapsed_micros(parse_started);
    let plan_started = Instant::now();
    let planned = match engine.plan_cached(&request.query, &pipeline) {
        Ok(planned) => planned,
        Err(_) => return Ok(false),
    };
    let plan_us = elapsed_micros(plan_started);
    if planned
        .physical()
        .source()
        .collection()
        .as_str()
        .starts_with('_')
    {
        return Ok(false);
    }
    let memory_streaming = planned.physical().is_memory_streaming();
    let terminal_count = planned
        .physical()
        .operators()
        .last()
        .and_then(og_core::query::PhysicalOperator::count_alias)
        .is_some();
    let governed_blocking = engine.supports_governed_blocking_streaming(planned.physical());
    if !memory_streaming && !governed_blocking {
        if matches!(
            planned.physical().memory_execution_mode(),
            og_core::query::MemoryExecutionMode::GovernedBlocking
        ) {
            write_streaming_error(
                writer,
                request.id,
                "query.execution_failed",
                "query plan contains a blocking stage sequence without a bounded executor; materialized fallback is forbidden by the memory contract",
            )?;
            return Ok(true);
        }
        return Ok(false);
    }
    let workload = if governed_blocking {
        WorkloadClass::Query
    } else {
        WorkloadClass::Streaming
    };
    let requested_budget = if governed_blocking {
        engine.memory_governor().profile().query_budget_bytes
    } else {
        0
    };
    let _operation_permit = match engine.memory_governor().admit(workload, requested_budget) {
        Ok(permit) => permit,
        Err(error) => {
            write_streaming_error(
                writer,
                request.id,
                "query.memory_admission_rejected",
                error.to_string(),
            )?;
            return Ok(true);
        }
    };
    let _network_reservation = match engine
        .memory_governor()
        .reserve(MemoryClass::Network, MAX_RESPONSE_BYTES)
    {
        Ok(reservation) => reservation,
        Err(error) => {
            write_streaming_error(
                writer,
                request.id,
                "query.memory_limit_exceeded",
                error.to_string(),
            )?;
            return Ok(true);
        }
    };
    let mut response_buffer = Vec::with_capacity(MAX_RESPONSE_BYTES + 1);
    let execute_started = Instant::now();
    let mut materialize_us = 0_u64;
    let mut streamed_documents = 0_u64;
    let mut write_document = |id: &og_core::storage::DocumentId, document: &Document| {
        let encode_started = Instant::now();
        if terminal_count {
            let response = BorrowedPlainDocumentResponse {
                kind: "response",
                status: "partial",
                version: PROTOCOL_VERSION,
                id: request.id,
                data: BorrowedDocument(document),
            };
            write_message_buffered(writer, &response, &mut response_buffer)
        } else {
            let response = BorrowedDocumentResponse {
                kind: "response",
                status: "partial",
                version: PROTOCOL_VERSION,
                id: request.id,
                data: BorrowedDocumentWithId { id, document },
            };
            write_message_buffered(writer, &response, &mut response_buffer)
        }
        .map_err(|error| {
            og_core::engine::EngineError::execution(og_core::query::ExecutionError::evaluation(
                error.to_string(),
            ))
        })?;
        streamed_documents = streamed_documents.saturating_add(1);
        materialize_us = materialize_us.saturating_add(elapsed_micros(encode_started));
        Ok(())
    };
    let streamed = if memory_streaming {
        engine.stream_read_pipeline(planned.physical(), &mut |stored| {
            write_document(stored.id(), stored.document())
        })
    } else {
        engine.stream_governed_blocking_pipeline(planned.physical(), &mut |row| {
            write_document(row.id(), row.document())
        })
    };
    drop(write_document);
    let statistics = match streamed {
        Ok(Some(statistics)) => statistics,
        Ok(None) => {
            write_streaming_error(
                writer,
                request.id,
                "query.execution_failed",
                "streaming plan unexpectedly fell back to materialized execution",
            )?;
            return Ok(true);
        }
        Err(error) => {
            write_streaming_error(
                writer,
                request.id,
                "query.execution_failed",
                error.to_string(),
            )?;
            return Ok(true);
        }
    };
    let execute_us = elapsed_micros(execute_started);
    write_stream_response_buffered(
        writer,
        &StreamResponse::complete(
            request.id,
            Some(serde_json::json!({
                "scanned": statistics.scanned(),
                "filtered": statistics.filtered(),
                "returned": statistics.returned(),
                "inserted": statistics.inserted(),
                "replaced": statistics.replaced(),
                "deleted": statistics.deleted(),
                "strategies": statistics.strategies().iter().map(|strategy| strategy.as_str()).collect::<Vec<_>>(),
                "committed": false,
                "compact": false,
                "streamed": true,
                "timings_us": {
                    "parse": parse_us, "plan": plan_us, "execute": execute_us,
                    "materialize_response": materialize_us,
                    "total_before_wire_encode": elapsed_micros(started),
                }
            })),
        ),
        &mut response_buffer,
    )?;
    writer.flush().map_err(ConnectionError::Write)?;
    Ok(true)
}

fn write_streaming_error(
    writer: &mut TcpStream,
    request_id: RequestId,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Result<(), ConnectionError> {
    let mut buffer = Vec::with_capacity(512);
    write_stream_response_buffered(
        writer,
        &StreamResponse::error(Some(request_id), WireError::new(code, message)),
        &mut buffer,
    )?;
    writer.flush().map_err(ConnectionError::Write)
}

fn enrollment_events_permission_query(identity_id: &str, created_at: u64) -> String {
    format!(
        r#"on _permissions | insert {{identityId: {}, action: "events.subscribe", resource: "*", effect: "allow", state: "active", createdAt: {created_at}}}"#,
        query_string(identity_id)
    )
}



fn file_scope_component(value:&str)->String{
    let mut encoded=String::with_capacity(value.len()*2);
    const HEX:&[u8;16]=b"0123456789abcdef";
    for byte in value.bytes(){
        encoded.push(char::from(HEX[usize::from(byte>>4)]));
        encoded.push(char::from(HEX[usize::from(byte&0x0f)]));
    }
    encoded
}


fn resolve_app_table_collection(app:&JsonValue, table:&str)->Option<String>{
    let definition=app.get("definition")?; let model=definition.get("model")?; let table_def=model.get("tables")?.get(table)?; let collection_alias=table_def.get("collection")?.as_str()?; let declaration=model.get("collections")?.get(collection_alias)?;
    declaration.as_str().or_else(||declaration.get("name").and_then(JsonValue::as_str)).map(str::to_owned)
}

fn run_data_worker_for_path(id:RequestId,path:&Path,operation:&str,mapping:Option<&JsonValue>)->Result<JsonValue,QueryResponse>{
    let python=env::var("OG_DATA_WORKER_PYTHON").unwrap_or_else(|_|"python3".to_owned());
    let module=env::var("OG_DATA_WORKER_MODULE").unwrap_or_else(|_|"openglacier.data_worker".to_owned());
    let mut child=Command::new(python).args(["-m",module.as_str()]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|e|QueryResponse::request_error(id,"worker.unavailable",format!("cannot start data worker: {e}")))?;
    let request=serde_json::json!({"operation":operation,"file":path.to_string_lossy(),"mapping":mapping});
    if let Some(mut stdin)=child.stdin.take(){serde_json::to_writer(&mut stdin,&request).map_err(|e|QueryResponse::request_error(id,"worker.protocol_error",e.to_string()))?;}
    let output=child.wait_with_output().map_err(|e|QueryResponse::request_error(id,"worker.failed",e.to_string()))?;
    let value:JsonValue=serde_json::from_slice(&output.stdout).map_err(|e|QueryResponse::request_error(id,"worker.protocol_error",format!("invalid worker JSON: {e}; stderr={}",String::from_utf8_lossy(&output.stderr))))?;
    if !output.status.success() || value.get("status").and_then(JsonValue::as_str)==Some("error"){return Err(QueryResponse::request_error(id,"worker.failed",value.get("error").and_then(|v|v.get("message")).and_then(JsonValue::as_str).unwrap_or("data worker failed")));}
    Ok(value)
}

fn run_data_worker_for_file(settings:&ConnectionSettings,engine:&Engine,id:RequestId,place_id:&str,files_instance_id:&str,file_id:&str,operation:&str,mapping:Option<&JsonValue>)->Result<JsonValue,QueryResponse>{
    let _context=resolve_file_context(engine,id,&Principal::Anonymous,true,place_id,files_instance_id,false).ok(); // actual caller authorization is checked before this helper
    let store=scoped_native_file_store(settings,id,place_id,files_instance_id)?; let entry=load_file_entry(engine,id,place_id,files_instance_id,file_id)?;
    let mut source=store.read(&entry.remote_id,None).map_err(|e|QueryResponse::request_error(id,"file.store_error",e.to_string()))?;
    let extension=Path::new(&entry.name).extension().and_then(|v|v.to_str()).unwrap_or("dat"); let temp=env::temp_dir().join(format!("og-data-{}-{}.{}",std::process::id(),unix_time_millis(),extension));
    {let mut out=fs::File::create(&temp).map_err(|e|QueryResponse::request_error(id,"worker.io_error",e.to_string()))?; io::copy(&mut source,&mut out).map_err(|e|QueryResponse::request_error(id,"worker.io_error",e.to_string()))?;}
    let result=run_data_worker_for_path(id,&temp,operation,mapping);
    let _=fs::remove_file(&temp);
    result
}

fn scoped_native_file_store(
    settings:&ConnectionSettings,
    id:RequestId,
    place_id:&str,
    instance_id:&str,
)->Result<NativeFileStore,QueryResponse>{
    let root=settings.files_path
        .join(file_scope_component(place_id))
        .join(file_scope_component(instance_id));
    NativeFileStore::new(StoreId::from(DEFAULT_FILES_STORE_ID),root)
        .map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))
}


fn scoped_native_version_store(
    settings:&ConnectionSettings,
    id:RequestId,
    place_id:&str,
    instance_id:&str,
)->Result<NativeFileStore,QueryResponse>{
    let root=settings.files_path
        .join(".versions")
        .join(file_scope_component(place_id))
        .join(file_scope_component(instance_id));
    NativeFileStore::new(StoreId::from(DEFAULT_FILES_STORE_ID),root)
        .map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))
}

static NEXT_FILE_VERSION_ID: AtomicU64 = AtomicU64::new(1);

fn file_version_json(
    version_id:&str,file:&FileEntry,remote_id:&str,metadata:&og_core::files::FileMetadata,created_at:u64
)->JsonValue{
    serde_json::json!({
        "versionId":version_id,
        "fileId":file.file_id.as_str(),
        "storeId":file.store_id.as_str(),
        "remoteId":remote_id,
        "name":file.name,
        "size":metadata.size,
        "contentType":metadata.content_type,
        "etag":metadata.etag,
        "createdAt":created_at,
        "_place":file.place_id,
        "_app_instance":file.app_instance_id,
    })
}

fn list_file_versions(
    engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str
)->Result<Vec<JsonValue>,QueryResponse>{
    let query=format!( "on _file_versions | where _place == {} and _app_instance == {} and fileId == {} | sort createdAt desc", query_string(place_id),query_string(instance_id),query_string(file_id) );
    match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{documents,..}=>Ok(documents),error@QueryResponse::Error{..}=>Err(error)}
}

fn load_file_version(
    engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str,version_id:&str
)->Result<JsonValue,QueryResponse>{
    let query=format!( "on _file_versions | where _place == {} and _app_instance == {} and fileId == {} and versionId == {} | limit 1", query_string(place_id),query_string(instance_id),query_string(file_id),query_string(version_id) );
    match execute_request(engine,QueryRequest::new(id,query)){
        QueryResponse::Ok{documents,..}=>documents.into_iter().next().ok_or_else(||QueryResponse::request_error(id,"file.version_not_found","File version was not found")),
        error@QueryResponse::Error{..}=>Err(error),
    }
}

fn delete_file_version_metadata(
    engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str,version_id:&str
)->Result<(),QueryResponse>{
    let query=format!( "on _file_versions | where _place == {} and _app_instance == {} and fileId == {} and versionId == {} | delete", query_string(place_id),query_string(instance_id),query_string(file_id),query_string(version_id) );
    match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{..}=>Ok(()),error@QueryResponse::Error{..}=>Err(error)}
}

fn archive_current_file(
    engine:&Engine,settings:&ConnectionSettings,id:RequestId,file:&FileEntry
)->Result<JsonValue,QueryResponse>{
    let source=scoped_native_file_store(settings,id,&file.place_id,&file.app_instance_id)?;
    let versions=scoped_native_version_store(settings,id,&file.place_id,&file.app_instance_id)?;
    let created_at=unix_time_millis();
    let sequence=NEXT_FILE_VERSION_ID.fetch_add(1,Ordering::Relaxed);
    let version_id=format!("version-{created_at}-{sequence}");
    let version_name=version_id.clone();
    let mut reader=source.read(&file.remote_id,None).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    let mut stored=versions.write(
        FileWrite{remote_id:None,parent_remote_id:None,name:&version_name,content_type:file.metadata.content_type.as_deref(),size:file.metadata.size},
        &mut reader
    ).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    stored.metadata.content_type=file.metadata.content_type.clone();
    let json=file_version_json(&version_id,file,&stored.remote_id,&stored.metadata,created_at);
    let query=format!("on _file_versions | insert {}",serde_json::to_string(&json).expect("file version serializes"));
    match execute_request(engine,QueryRequest::new(id,query)){
        QueryResponse::Ok{..}=>Ok(json),
        error@QueryResponse::Error{..}=>{let _=versions.delete(&stored.remote_id);Err(error)}
    }
}

static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(1);

fn resolve_file_context(
    engine:&Engine, id:RequestId, principal:&Principal, allow_public:bool, place_id:&str, instance_id:&str, write:bool
)->Result<ExecutionContext,QueryResponse>{
    let context=resolve_query_execution_context(engine,id,principal,RequestedExecutionContext{place_id:place_id.to_owned(),app_instance_id:instance_id.to_owned()},allow_public)?;
    if write && !context.place_role.can_write(){
        return Err(QueryResponse::request_error(id,"authorization.denied","Place access is read-only for Files"));
    }
    Ok(context)
}

fn file_entry_json(entry:&FileEntry)->JsonValue{
    let mut value=serde_json::json!({
        "fileId":entry.file_id.as_str(),"storeId":entry.store_id.as_str(),"remoteId":entry.remote_id,
         "name":entry.name,"kind":entry.kind.as_str(),"_place":entry.place_id,"_app_instance":entry.app_instance_id,
        "parentId":entry.parent_id.as_ref().map(|parent|parent.as_str()),
    });
    let object=value.as_object_mut().expect("file entry object");
    if let Some(size)=entry.metadata.size{object.insert("size".into(),JsonValue::from(size));}
    if let Some(v)=&entry.metadata.content_type{object.insert("contentType".into(),JsonValue::String(v.clone()));}
    if let Some(v)=&entry.metadata.etag{object.insert("etag".into(),JsonValue::String(v.clone()));}
    if let Some(v)=entry.metadata.created_at{object.insert("createdAt".into(),JsonValue::from(v));}
    if let Some(v)=entry.metadata.modified_at{object.insert("modifiedAt".into(),JsonValue::from(v));}
    value
}

fn json_to_file_entry(value:&JsonValue)->Result<FileEntry,&'static str>{
    let object=value.as_object().ok_or("file metadata is not an object")?;
    let string=|key| object.get(key).and_then(JsonValue::as_str).map(str::to_owned);
    let kind=match string("kind").as_deref(){Some("file")=>og_core::files::FileKind::File,Some("directory")=>og_core::files::FileKind::Directory,_=>return Err("invalid file kind")};
    Ok(FileEntry{
        file_id:FileId::from(string("fileId").ok_or("missing fileId")?),
        store_id:StoreId::from(string("storeId").ok_or("missing storeId")?),
        remote_id:string("remoteId").ok_or("missing remoteId")?,
        parent_id:string("parentId").map(FileId::from),
        name:string("name").ok_or("missing name")?,kind,
        metadata:og_core::files::FileMetadata{size:object.get("size").and_then(JsonValue::as_u64),content_type:string("contentType"),etag:string("etag"),created_at:object.get("createdAt").and_then(JsonValue::as_u64),modified_at:object.get("modifiedAt").and_then(JsonValue::as_u64)},
        place_id:string("_place").ok_or("missing _place")?,app_instance_id:string("_app_instance").ok_or("missing _app_instance")?,
    })
}

fn load_file_entry_json(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str)->Result<JsonValue,QueryResponse>{
    let query=format!("on _files | where _place == {} and _app_instance == {} and fileId == {} | limit 1",query_string(place_id),query_string(instance_id),query_string(file_id));
    match execute_request(engine,QueryRequest::new(id,query)){
        QueryResponse::Ok{documents,..}=>documents.into_iter().next().ok_or_else(||QueryResponse::request_error(id,"file.not_found","File entry was not found")),
        error@QueryResponse::Error{..}=>Err(error),
    }
}
fn load_file_entry(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str)->Result<FileEntry,QueryResponse>{
    let json=load_file_entry_json(engine,id,place_id,instance_id,file_id)?;
    json_to_file_entry(&json).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))
}
fn list_file_children_raw(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,parent_id:Option<&str>)->Result<Vec<JsonValue>,QueryResponse>{
    let parent=parent_id.map_or_else(||"parentId == null".to_owned(),|value|format!("parentId == {}",query_string(value)));
    let query=format!("on _files | where _place == {} and _app_instance == {} and {parent} | sort name",query_string(place_id),query_string(instance_id));
    match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{documents,..}=>Ok(documents),error@QueryResponse::Error{..}=>Err(error)}
}

fn list_file_entries(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,parent_id:Option<&str>)->Result<Vec<JsonValue>,QueryResponse>{
    Ok(list_file_children_raw(engine,id,place_id,instance_id,parent_id)?
        .into_iter()
        .filter(|document|document.get("trashed").and_then(JsonValue::as_bool)!=Some(true))
        .collect())
}

fn list_trashed_file_entries(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str)->Result<Vec<JsonValue>,QueryResponse>{
    let query=format!("on _files | where _place == {} and _app_instance == {}",query_string(place_id),query_string(instance_id));
    match execute_request(engine,QueryRequest::new(id,query)){
        QueryResponse::Ok{documents,..}=>{
            let trashed_ids=documents.iter()
                .filter(|document|document.get("trashed").and_then(JsonValue::as_bool)==Some(true))
                .filter_map(|document|document.get("fileId").and_then(JsonValue::as_str))
                .map(str::to_owned)
                .collect::<std::collections::BTreeSet<_>>();
            let mut entries=documents.into_iter()
                .filter(|document|{
                    if document.get("trashed").and_then(JsonValue::as_bool)!=Some(true){return false;}
                    match document.get("parentId").and_then(JsonValue::as_str){
                        Some(parent)=>!trashed_ids.contains(parent),
                        None=>true,
                    }
                })
                .collect::<Vec<_>>();
            entries.sort_by_key(|document|std::cmp::Reverse(document.get("trashedAt").and_then(JsonValue::as_u64).unwrap_or(0)));
            Ok(entries)
        }
        error@QueryResponse::Error{..}=>Err(error),
    }
}
fn file_entry_from_store(place_id:&str,instance_id:&str,parent_id:Option<&str>,stored:FileStoreEntry)->FileEntry{
    let sequence=NEXT_FILE_ID.fetch_add(1,Ordering::Relaxed);
    FileEntry{file_id:FileId::from(format!("file-{}-{sequence}",unix_time_millis())),store_id:StoreId::from(DEFAULT_FILES_STORE_ID),remote_id:stored.remote_id,parent_id:parent_id.map(FileId::from),name:stored.name,kind:stored.kind,metadata:stored.metadata,place_id:place_id.to_owned(),app_instance_id:instance_id.to_owned()}
}
fn persist_file_entry(engine:&Engine,id:RequestId,entry:&FileEntry)->Result<JsonValue,QueryResponse>{
    let json=file_entry_json(entry);
    let query=format!("on _files | insert {}",serde_json::to_string(&json).expect("file metadata serializes"));
    match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{..}=>Ok(json),error@QueryResponse::Error{..}=>Err(error)}
}
fn replace_file_entry(engine:&Engine,id:RequestId,entry:&FileEntry)->Result<JsonValue,QueryResponse>{
    let delete=format!("on _files | where _place == {} and _app_instance == {} and fileId == {} | delete",query_string(&entry.place_id),query_string(&entry.app_instance_id),query_string(entry.file_id.as_str()));
    match execute_request(engine,QueryRequest::new(id,delete)){QueryResponse::Ok{..}=>persist_file_entry(engine,id,entry),error@QueryResponse::Error{..}=>Err(error)}
}
fn delete_file_metadata(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str)->Result<(),QueryResponse>{
    let query=format!("on _files | where _place == {} and _app_instance == {} and fileId == {} | delete",query_string(place_id),query_string(instance_id),query_string(file_id));
    match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{..}=>Ok(()),error@QueryResponse::Error{..}=>Err(error)}
}


fn replace_file_json(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str,json:&JsonValue)->Result<JsonValue,QueryResponse>{
    let delete=format!("on _files | where _place == {} and _app_instance == {} and fileId == {} | delete",query_string(place_id),query_string(instance_id),query_string(file_id));
    match execute_request(engine,QueryRequest::new(id,delete)){
        QueryResponse::Ok{..}=>{
            let query=format!("on _files | insert {}",serde_json::to_string(json).expect("file metadata serializes"));
            match execute_request(engine,QueryRequest::new(id,query)){QueryResponse::Ok{..}=>Ok(json.clone()),error@QueryResponse::Error{..}=>Err(error)}
        }
        error@QueryResponse::Error{..}=>Err(error),
    }
}

const INTERNAL_TRASH_DIR: &str = ".og-trash";

fn file_store_query_error(id:RequestId,error:FileStoreError)->QueryResponse{
    let code=match error{
        FileStoreError::NotFound=>"file.not_found",
        FileStoreError::AlreadyExists=>"file.already_exists",
        FileStoreError::InvalidName|FileStoreError::InvalidRemoteId=>"file.invalid",
        FileStoreError::NotDirectory=>"file.not_directory",
        FileStoreError::NotFile=>"file.not_file",
        FileStoreError::InvalidRange=>"file.invalid_range",
        FileStoreError::Unsupported(_)=>"file.unsupported",
        _=>"file.store_error",
    };
    QueryResponse::request_error(id,code,error.to_string())
}

fn ensure_file_trash_root(store:&dyn FileStore,id:RequestId)->Result<String,QueryResponse>{
    let entries=store.list(None).map_err(|error|file_store_query_error(id,error))?;
    if let Some(entry)=entries.into_iter().find(|entry|entry.name==INTERNAL_TRASH_DIR){
        if entry.kind!=og_core::files::FileKind::Directory{
            return Err(QueryResponse::request_error(id,"file.trash_invalid","internal trash path is not a directory"));
        }
        return Ok(entry.remote_id);
    }
    store.mkdir(None,INTERNAL_TRASH_DIR)
        .map(|entry|entry.remote_id)
        .map_err(|error|file_store_query_error(id,error))
}

fn trash_file_entry(engine:&Engine,settings:&ConnectionSettings,id:RequestId,place_id:&str,instance_id:&str,file_id:&str)->Result<JsonValue,QueryResponse>{
    let mut json=load_file_entry_json(engine,id,place_id,instance_id,file_id)?;
    if json.get("trashed").and_then(JsonValue::as_bool)==Some(true){return Ok(json);}
    let entry=json_to_file_entry(&json).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))?;
    let store=scoped_native_file_store(settings,id,place_id,instance_id)?;
    let trash_root=ensure_file_trash_root(&store,id)?;
    let stored=store.move_entry(&entry.remote_id,Some(&trash_root),entry.file_id.as_str())
        .map_err(|error|file_store_query_error(id,error))?;

    let parent=json.get("parentId").cloned().unwrap_or(JsonValue::Null);
    let object=json.as_object_mut().ok_or_else(||QueryResponse::request_error(id,"file.invalid_record","File metadata is not an object"))?;
    object.insert("remoteId".to_owned(),JsonValue::String(stored.remote_id.clone()));
    object.insert("trashed".to_owned(),JsonValue::Bool(true));
    object.insert("trashedAt".to_owned(),JsonValue::from(unix_time_millis()));
    object.insert("trashedParentId".to_owned(),parent);
    if let Some(etag)=stored.metadata.etag.clone(){object.insert("etag".to_owned(),JsonValue::String(etag));}
    if let Some(modified)=stored.metadata.modified_at{object.insert("modifiedAt".to_owned(),JsonValue::from(modified));}

    let persisted=replace_file_json(engine,id,place_id,instance_id,file_id,&json)?;
    if entry.kind==og_core::files::FileKind::Directory{
        let moved=json_to_file_entry(&persisted).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))?;
        sync_moved_children(engine,id,&store,place_id,instance_id,&moved)?;
    }
    Ok(persisted)
}

fn file_is_trashed(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,file_id:&str)->Result<bool,QueryResponse>{
    Ok(load_file_entry_json(engine,id,place_id,instance_id,file_id)?
        .get("trashed").and_then(JsonValue::as_bool)==Some(true))
}

fn restore_file_entry(
    engine:&Engine,settings:&ConnectionSettings,id:RequestId,place_id:&str,instance_id:&str,file_id:&str
)->Result<JsonValue,QueryResponse>{
    let mut json=load_file_entry_json(engine,id,place_id,instance_id,file_id)?;
    if json.get("trashed").and_then(JsonValue::as_bool)!=Some(true){
        return Err(QueryResponse::request_error(id,"file.not_trashed","File entry is not in the trash"));
    }

    let entry=json_to_file_entry(&json).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))?;
    let old_parent=json.get("trashedParentId").and_then(JsonValue::as_str).map(str::to_owned);
    let parent=match old_parent.as_deref(){
        Some(parent_id)=>load_file_entry(engine,id,place_id,instance_id,parent_id)
            .ok()
            .filter(|_|load_file_entry_json(engine,id,place_id,instance_id,parent_id)
                .ok()
                .and_then(|parent|parent.get("trashed").and_then(JsonValue::as_bool))!=Some(true)),
        None=>None,
    };
    let target_parent_id=parent.as_ref().map(|parent|parent.file_id.as_str().to_owned());
    let target_parent_remote=parent.as_ref().map(|parent|parent.remote_id.as_str());
    let store=scoped_native_file_store(settings,id,place_id,instance_id)?;
    let stored=store.move_entry(&entry.remote_id,target_parent_remote,&entry.name)
        .map_err(|error|file_store_query_error(id,error))?;

    if let Some(object)=json.as_object_mut(){
        object.insert("remoteId".to_owned(),JsonValue::String(stored.remote_id.clone()));
        object.insert("parentId".to_owned(),target_parent_id.map(JsonValue::String).unwrap_or(JsonValue::Null));
        object.insert("trashed".to_owned(),JsonValue::Bool(false));
        object.remove("trashedAt");
        object.remove("trashedParentId");
        if let Some(etag)=stored.metadata.etag.clone(){object.insert("etag".to_owned(),JsonValue::String(etag));}
        if let Some(modified)=stored.metadata.modified_at{object.insert("modifiedAt".to_owned(),JsonValue::from(modified));}
    }
    let persisted=replace_file_json(engine,id,place_id,instance_id,file_id,&json)?;
    if entry.kind==og_core::files::FileKind::Directory{
        let moved=json_to_file_entry(&persisted).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))?;
        sync_moved_children(engine,id,&store,place_id,instance_id,&moved)?;
    }
    Ok(persisted)
}

fn purge_file_versions(
    engine:&Engine,settings:&ConnectionSettings,id:RequestId,place_id:&str,instance_id:&str,file_id:&str
)->Result<(),QueryResponse>{
    let versions=list_file_versions(engine,id,place_id,instance_id,file_id)?;
    if versions.is_empty(){return Ok(());}
    let store=scoped_native_version_store(settings,id,place_id,instance_id)?;
    for version in versions {
        if let Some(remote_id)=version.get("remoteId").and_then(JsonValue::as_str){
            let _=store.delete(remote_id);
        }
        if let Some(version_id)=version.get("versionId").and_then(JsonValue::as_str){
            delete_file_version_metadata(engine,id,place_id,instance_id,file_id,version_id)?;
        }
    }
    Ok(())
}

fn permanently_delete_file_tree(
    engine:&Engine,settings:&ConnectionSettings,id:RequestId,place_id:&str,instance_id:&str,file_id:&str
)->Result<(),QueryResponse>{
    let entry=load_file_entry(engine,id,place_id,instance_id,file_id)?;
    for child in child_file_entries(engine,id,place_id,instance_id,file_id)? {
        permanently_delete_file_tree(engine,settings,id,place_id,instance_id,child.file_id.as_str())?;
    }
    purge_file_versions(engine,settings,id,place_id,instance_id,file_id)?;
    let store=scoped_native_file_store(settings,id,place_id,instance_id)?;
    if entry.kind==og_core::files::FileKind::Directory {
        // Children were removed first; delete handles the now-empty tree too.
        store.delete(&entry.remote_id).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    } else {
        store.delete(&entry.remote_id).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    }
    delete_file_metadata(engine,id,place_id,instance_id,file_id)
}

fn child_file_entries(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,parent_id:&str)->Result<Vec<FileEntry>,QueryResponse>{
    list_file_children_raw(engine,id,place_id,instance_id,Some(parent_id))?.into_iter().map(|value|json_to_file_entry(&value).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))).collect()
}
fn subtree_has_trashed_entries(engine:&Engine,id:RequestId,place_id:&str,instance_id:&str,parent_id:&str)->Result<bool,QueryResponse>{
    for child in list_file_children_raw(engine,id,place_id,instance_id,Some(parent_id))? {
        if child.get("trashed").and_then(JsonValue::as_bool)==Some(true){return Ok(true);}
        if child.get("kind").and_then(JsonValue::as_str)==Some("directory") {
            if let Some(file_id)=child.get("fileId").and_then(JsonValue::as_str) {
                if subtree_has_trashed_entries(engine,id,place_id,instance_id,file_id)?{return Ok(true);}
            }
        }
    }
    Ok(false)
}

fn sync_moved_children(engine:&Engine,id:RequestId,store:&dyn FileStore,place_id:&str,instance_id:&str,parent:&FileEntry)->Result<(),QueryResponse>{
    let persisted=list_file_children_raw(engine,id,place_id,instance_id,Some(parent.file_id.as_str()))?;
    let remote=store.list(Some(&parent.remote_id)).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    for mut json in persisted {
        let child=json_to_file_entry(&json).map_err(|message|QueryResponse::request_error(id,"file.invalid_record",message))?;
        let Some(found)=remote.iter().find(|candidate|candidate.name==child.name) else {
            return Err(QueryResponse::request_error(id,"file.store_mismatch","provider child is missing after move"));
        };
        if let Some(object)=json.as_object_mut(){
            object.insert("remoteId".to_owned(),JsonValue::String(found.remote_id.clone()));
            if let Some(etag)=&found.metadata.etag{object.insert("etag".to_owned(),JsonValue::String(etag.clone()));}
            if let Some(modified)=found.metadata.modified_at{object.insert("modifiedAt".to_owned(),JsonValue::from(modified));}
        }
        replace_file_json(engine,id,place_id,instance_id,child.file_id.as_str(),&json)?;
        if child.kind==og_core::files::FileKind::Directory{
            let updated=FileEntry{
                file_id:child.file_id,store_id:child.store_id,remote_id:found.remote_id.clone(),
                parent_id:child.parent_id,name:child.name,kind:child.kind,metadata:found.metadata.clone(),
                place_id:place_id.to_owned(),app_instance_id:instance_id.to_owned()
            };
            sync_moved_children(engine,id,store,place_id,instance_id,&updated)?;
        }
    }
    Ok(())
}
fn persist_copied_children(engine:&Engine,id:RequestId,store:&dyn FileStore,place_id:&str,instance_id:&str,parent:&FileEntry)->Result<(),QueryResponse>{
    let remote=store.list(Some(&parent.remote_id)).map_err(|error|QueryResponse::request_error(id,"file.store_error",error.to_string()))?;
    for stored in remote {
        let child=file_entry_from_store(place_id,instance_id,Some(parent.file_id.as_str()),stored);
        persist_file_entry(engine,id,&child)?;
        if child.kind==og_core::files::FileKind::Directory{persist_copied_children(engine,id,store,place_id,instance_id,&child)?;}
    }
    Ok(())
}

fn write_file_store_error(writer:&mut TcpStream,id:RequestId,error:FileStoreError)->Result<(),ConnectionError>{
    write_response(writer,&file_store_query_error(id,error))
}

fn require_identity<'a>(
    writer: &mut TcpStream,
    principal: &'a og_core::access::auth::Principal,
    id: RequestId,
    message: &str,
) -> Result<Option<&'a str>, ConnectionError> {
    let Some(identity_id) = principal_identity_id(principal) else {
        write_response(writer, &QueryResponse::request_error(id, "authorization.required", message))?;
        return Ok(None);
    };
    Ok(Some(identity_id))
}

fn ensure_routed_static_authorized(
    writer: &mut TcpStream,
    settings: &ConnectionSettings,
    engine: &Engine,
    principal: &og_core::access::auth::Principal,
    operation: &RoutedOperation,
) -> Result<bool, ConnectionError> {
    match operation.kind().access() {
        AccessPolicy::Permission { action, resource } =>
            ensure_authorized(writer, settings, engine, principal, operation.id(), action, resource),
        AccessPolicy::Public
        | AccessPolicy::Authenticated
        | AccessPolicy::Query
        | AccessPolicy::DynamicPermission(_) => Ok(true),
    }
}

fn ensure_operation_resource_authorized(
    writer: &mut TcpStream,
    settings: &ConnectionSettings,
    engine: &Engine,
    principal: &og_core::access::auth::Principal,
    id: RequestId,
    operation: OperationKind,
    resource: &str,
) -> Result<bool, ConnectionError> {
    match operation.access() {
        AccessPolicy::DynamicPermission(action) =>
            ensure_authorized(writer, settings, engine, principal, id, action, resource),
        AccessPolicy::Permission { action, resource } =>
            ensure_authorized(writer, settings, engine, principal, id, action, resource),
        AccessPolicy::Public | AccessPolicy::Authenticated | AccessPolicy::Query => Ok(true),
    }
}

fn ensure_authorized(
    writer: &mut TcpStream,
    settings: &ConnectionSettings,
    engine: &Engine,
    principal: &og_core::access::auth::Principal,
    id: RequestId,
    action: AuthorizationAction,
    resource: &str,
) -> Result<bool, ConnectionError> {
    if !settings.authorization_mode.is_enforced() || authorize_connection(engine, principal, action, resource) {
        return Ok(true);
    }
    write_authorization_denied(writer, id, principal, action, resource)?;
    Ok(false)
}

fn authorize_connection(
    engine: &Engine,
    principal: &og_core::access::auth::Principal,
    action: AuthorizationAction,
    resource: &str,
) -> bool {
    let Some(request) = AuthorizationRequest::from_principal(principal, action, resource) else {
        return false;
    };
    permission_exists(engine, &request)
}

fn permission_exists(engine: &Engine, request: &AuthorizationRequest) -> bool {
    let identity = quote_authorization_string(&request.identity_id);
    let action = quote_authorization_string(request.action.as_str());
    let resource = quote_authorization_string(&request.resource);
    let query = format!( "on _permissions | where identityId == {identity} and state == \"active\" and effect == \"allow\" and (action == {action} or action == \"*\") and (resource == {resource} or resource == \"*\") | limit 1" );
    matches!(
        execute_request(engine, QueryRequest::new(0, query)),
        QueryResponse::Ok { documents, .. } if !documents.is_empty()
    )
}

fn write_authorization_denied(
    writer: &mut TcpStream,
    id: RequestId,
    principal: &og_core::access::auth::Principal,
    action: AuthorizationAction,
    resource: &str,
) -> Result<(), ConnectionError> {
    let message = match principal {
        og_core::access::auth::Principal::Anonymous => format!(
            "authentication is required for {} on {:?}",
            action.as_str(),
            resource
        ),
        og_core::access::auth::Principal::Identity { identity_id, .. } => format!(
            "identity {:?} is not allowed to perform {} on {:?}",
            identity_id,
            action.as_str(),
            resource
        ),
    };
    write_response(
        writer,
        &QueryResponse::request_error(id, "authorization.denied", message),
    )
}

fn write_place_role_denied(
    writer: &mut TcpStream,
    id: RequestId,
    context: &ExecutionContext,
    action: AuthorizationAction,
    resource: &str,
) -> Result<(), ConnectionError> {
    let message = format!( "Place role {:?} is not allowed to perform {} on {:?} in Place {:?}", context.place_role.as_str(), action.as_str(), resource, context.place_id, );
    write_response(
        writer,
        &QueryResponse::request_error(id, "authorization.denied", message),
    )
}

fn publish_durable_event(
    engine: &Engine,
    audience: Audience,
    event_type: &str,
    payload: JsonValue,
) -> bool {
    static NEXT_EVENT_ID: AtomicU64 = AtomicU64::new(1);
    let created_at = unix_time_millis();
    let sequence = NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed);
    let event_id = format!("event-{created_at}-{}-{sequence}", std::process::id());
    let audience_json = serde_json::to_string(&audience).expect("audience serializes");
    let payload_json = serde_json::to_string(&payload).expect("payload serializes");
    let query = format!( "on _event_outbox | insert {{eventId: {}, type: {}, audience: {audience_json}, payload: {payload_json}, state: \"pending\", attempts: 0, availableAt: {created_at}, createdAt: {created_at}}}", query_string(&event_id), query_string(event_type), );
    let stored = execute_request(engine, QueryRequest::new(0, query)).is_ok();
    debug::log(
        DebugTopic::Events,
        None,
        format!(
            "outbox {} type={event_type} event_id={event_id}",
            if stored { "pending" } else { "failed" }
        ),
    );
    stored
}

fn start_event_outbox_worker(engine: Arc<Engine>, events: Arc<EventEngine>) {
    thread::Builder::new().name("og-event-outbox".to_owned()).spawn(move || loop {
        let now = unix_time_millis();
        let query = format!("on _event_outbox | where state == \"pending\" and availableAt <= {now} | sort createdAt | limit 16");
        let response = execute_request(&engine, QueryRequest::new(0, query));
        if let QueryResponse::Ok { documents, .. } = response {
            for document in documents {
                let estimate = serde_json::to_vec(&document).map_or(4096, |bytes| bytes.len().saturating_add(1024));
                let Ok(_permit) = engine.memory_governor().reserve(MemoryClass::Indexing, estimate) else {
                    debug::log(DebugTopic::Events, None, format!("outbox deferred reason=memory estimate={estimate}"));
                    break;
                };
                let Some(event_id) = document.get("eventId").and_then(JsonValue::as_str) else { continue; };
                let Some(event_type) = document.get("type").and_then(JsonValue::as_str) else { continue; };
                let audience = document.get("audience").cloned().and_then(|value| serde_json::from_value::<Audience>(value).ok()).unwrap_or(Audience::Global);
                let payload = document.get("payload").cloned().unwrap_or(JsonValue::Null);
                let event = og_core::CoreEvent::new(event_id, event_type, audience, unix_time_millis(), payload);
                if events.publish(event) {
                    debug::log(DebugTopic::Events, None, format!("publish type={event_type} event_id={event_id}"));
                    let delivered = format!("on _event_outbox | where eventId == {} | set state = \"delivered\", deliveredAt = {}", query_string(event_id), unix_time_millis());
                    let _ = execute_request(&engine, QueryRequest::new(0, delivered));
                } else {
                    debug::log(DebugTopic::Events, None, format!("retry type={event_type} event_id={event_id} reason=queue_full"));
                    let retry = format!("on _event_outbox | where eventId == {} | set attempts = attempts + 1, availableAt = {}", query_string(event_id), unix_time_millis().saturating_add(250));
                    let _ = execute_request(&engine, QueryRequest::new(0, retry));
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }).expect("event outbox worker must start");
}

fn start_debug_memory_reporter(engine: Arc<Engine>) {
    if !debug::memory_enabled() {
        return;
    }
    thread::Builder::new()
        .name("og-debug-memory".to_owned())
        .spawn(move || loop {
            let snapshot = engine.memory_governor().snapshot();
            let process = snapshot.process.map_or_else(
                || "rss=unavailable".to_owned(),
                |process| {
                    format!(
                        "rss={} anonymous={} unmanaged={}",
                        format_bytes(Some(process.rss_bytes)),
                        format_bytes(Some(process.anonymous_bytes)),
                        format_bytes(Some(process.unmanaged_bytes))
                    )
                },
            );
            debug::log(
                DebugTopic::Memory,
                None,
                format!(
                    "{process} governed={} peak={} reservations={} rejected={}",
                    format_bytes(Some(snapshot.current_bytes)),
                    format_bytes(Some(snapshot.peak_bytes)),
                    snapshot.active_reservations,
                    snapshot.failed_reservations
                ),
            );
            thread::sleep(Duration::from_secs(1));
        })
        .expect("debug memory reporter must start");
}

fn execute_request(engine: &Engine, request: QueryRequest) -> QueryResponse {
    execute_request_scoped(engine, request, None)
}

fn execute_request_scoped(
    engine: &Engine,
    request: QueryRequest,
    context: Option<&ExecutionContext>,
) -> QueryResponse {
    let started = Instant::now();
    let parse_started = Instant::now();
    let pipeline = match parse_pipeline(&request.query) {
        Ok(pipeline) => pipeline,
        Err(error) => {
            return QueryResponse::request_error(request.id, "query.invalid", error.to_string());
        }
    };
    let parse_us = elapsed_micros(parse_started);

    let plan_started = Instant::now();
    let planned = match engine.plan_cached(&request.query, &pipeline) {
        Ok(planned) => planned,
        Err(error) => {
            return QueryResponse::request_error(
                request.id,
                "query.execution_failed",
                error.to_string(),
            );
        }
    };
    let plan_us = elapsed_micros(plan_started);
    let compact = planned.physical().is_streaming_load();
    let system_query = planned
        .physical()
        .source()
        .collection()
        .as_str()
        .starts_with('_');
    let workload = if system_query {
        WorkloadClass::Streaming
    } else if compact {
        WorkloadClass::Import
    } else {
        WorkloadClass::Query
    };
    let requested_budget = if system_query {
        0
    } else if compact {
        engine.memory_governor().profile().import_budget_bytes
    } else {
        engine.memory_governor().profile().query_budget_bytes
    };
    let _operation_permit = match engine.memory_governor().admit(workload, requested_budget) {
        Ok(permit) => permit,
        Err(error) => {
            return QueryResponse::request_error(
                request.id,
                "query.memory_admission_rejected",
                error.to_string(),
            )
        }
    };

    let execute_started = Instant::now();
    let execution = if let Some(context) = context {
        let scope = DocumentScope::new(context.place_id.as_str(), context.app_instance_id.as_str());
        engine.execute_physical_scoped(planned.physical(), &scope)
    } else if compact {
        engine.execute_physical_compact(planned.physical())
    } else {
        engine.execute_physical(planned.physical())
    };
    let execute_us = elapsed_micros(execute_started);

    match execution {
        Ok(output) => {
            let encode_started = Instant::now();
            let documents = if compact {
                Vec::new()
            } else {
                output
                    .rows()
                    .iter()
                    .map(|row| execution_row_to_json(row, planned.physical()))
                    .collect::<Vec<_>>()
            };
            let materialize_us = elapsed_micros(encode_started);
            let statistics = output.statistics();

            QueryResponse::success(
                request.id,
                documents,
                Some(serde_json::json!({
                    "scanned": statistics.scanned(),
                    "filtered": statistics.filtered(),
                    "returned": statistics.returned(),
                    "inserted": statistics.inserted(),
                    "replaced": statistics.replaced(),
                    "deleted": statistics.deleted(),
                    "strategies": statistics.strategies().iter().map(|strategy| strategy.as_str()).collect::<Vec<_>>(),
                    "committed": output.committed(),
                    "compact": compact,
                    "timings_us": {
                        "parse": parse_us,
                        "plan": plan_us,
                        "execute": execute_us,
                        "materialize_response": materialize_us,
                        "total_before_wire_encode": elapsed_micros(started),
                    },
                })),
            )
        }
        Err(error) => {
            QueryResponse::request_error(request.id, "query.execution_failed", error.to_string())
        }
    }
}

#[inline]
fn execution_row_to_json(
    row: &og_core::query::ExecutionRow,
    plan: &og_core::query::PhysicalPlan,
) -> JsonValue {
    let mut value = document_to_json(row.document());
    if exposes_document_id(plan)
        && !matches!(row.origin(), og_core::query::ExecutionRowOrigin::Synthetic)
    {
        if let JsonValue::Object(object) = &mut value {
            object.insert("_id".to_owned(), JsonValue::String(row.id().to_string()));
        }
    }
    value
}

fn exposes_document_id(plan: &og_core::query::PhysicalPlan) -> bool {
    plan.operators().iter().all(|operator| {
        matches!(
            operator,
            og_core::query::PhysicalOperator::Filter { .. }
                | og_core::query::PhysicalOperator::Union { .. }
                | og_core::query::PhysicalOperator::Limit { .. }
                | og_core::query::PhysicalOperator::Skip { .. }
                | og_core::query::PhysicalOperator::Sort { .. }
                | og_core::query::PhysicalOperator::Distinct { .. }
        )
    })
}

#[derive(Serialize)]
struct BorrowedPlainDocumentResponse<'a> {
    kind: &'static str,
    status: &'static str,
    version: u16,
    id: RequestId,
    data: BorrowedDocument<'a>,
}

struct BorrowedDocument<'a>(&'a Document);

impl Serialize for BorrowedDocument<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in self.0.iter() {
            map.serialize_entry(name.as_str(), &BorrowedValue(value))?;
        }
        map.end()
    }
}

#[derive(Serialize)]
struct BorrowedDocumentResponse<'a> {
    kind: &'static str,
    status: &'static str,
    version: u16,
    id: RequestId,
    data: BorrowedDocumentWithId<'a>,
}

struct BorrowedDocumentWithId<'a> {
    id: &'a og_core::storage::DocumentId,
    document: &'a Document,
}

impl Serialize for BorrowedDocumentWithId<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.document.len() + 1))?;
        map.serialize_entry("_id", &BorrowedDocumentId(self.id))?;
        for (name, value) in self.document.iter() {
            map.serialize_entry(name.as_str(), &BorrowedValue(value))?;
        }
        map.end()
    }
}

struct BorrowedDocumentId<'a>(&'a og_core::storage::DocumentId);

impl Serialize for BorrowedDocumentId<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self.0)
    }
}

struct BorrowedValue<'a>(&'a Value);

impl Serialize for BorrowedValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(value) => serializer.serialize_bool(*value),
            Value::Number(number) => match number {
                Number::Signed(value) => {
                    og_core::protocol::serialize_js_safe_i64(value, serializer)
                }
                Number::Unsigned(value) => {
                    og_core::protocol::serialize_js_safe_u64(value, serializer)
                }
                Number::Float(value) => serializer.serialize_f64(*value),
                _ => Err(S::Error::custom("unsupported numeric representation")),
            },
            Value::String(value) => serializer.serialize_str(value),
            Value::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values.iter() {
                    sequence.serialize_element(&BorrowedValue(value))?;
                }
                sequence.end()
            }
            Value::Object(document) => {
                let mut map = serializer.serialize_map(Some(document.len()))?;
                for (name, value) in document.iter() {
                    map.serialize_entry(name.as_str(), &BorrowedValue(value))?;
                }
                map.end()
            }
        }
    }
}

fn parse_pipeline(source: &str) -> Result<PlannerPipeline, QueryTextError> {
    let ast = parse_query(source).map_err(|error| QueryTextError::Parse(error.to_string()))?;
    PlannerPipeline::from_ast(source, &ast)
        .map_err(|error| QueryTextError::Planning(error.to_string()))
}

fn write_response(writer: &mut TcpStream, response: &QueryResponse) -> Result<(), ConnectionError> {
    match response {
        QueryResponse::Ok {
            id,
            documents,
            statistics,
            ..
        } => {
            for document in documents {
                write_stream_response(writer, &StreamResponse::partial(*id, document.clone()))?;
            }
            write_stream_response(writer, &StreamResponse::complete(*id, statistics.clone()))?;
        }
        QueryResponse::Error { id, error, .. } => {
            write_stream_response(writer, &StreamResponse::error(*id, error.clone()))?;
        }
    }
    writer.flush().map_err(ConnectionError::Write)
}

fn write_stream_response(
    writer: &mut TcpStream,
    response: &StreamResponse,
) -> Result<(), ConnectionError> {
    let encoded =
        og_core::protocol::encode_stream_response(response).map_err(ConnectionError::Encode)?;
    writer.write_all(&encoded).map_err(ConnectionError::Write)
}

fn write_stream_response_buffered(
    writer: &mut TcpStream,
    response: &StreamResponse,
    buffer: &mut Vec<u8>,
) -> Result<(), ConnectionError> {
    let encoded =
        og_core::protocol::encode_stream_response(response).map_err(ConnectionError::Encode)?;
    buffer.clear();
    buffer.extend_from_slice(&encoded);
    writer.write_all(buffer).map_err(ConnectionError::Write)
}

fn write_message_buffered<T: Serialize>(
    writer: &mut TcpStream,
    value: &T,
    buffer: &mut Vec<u8>,
) -> Result<(), ConnectionError> {
    let payload = rmp_serde::to_vec_named(value)
        .map_err(|error| ConnectionError::Encode(ProtocolError::InvalidMessagePackEncode(error)))?;
    buffer.clear();
    buffer.resize(LENGTH_PREFIX_BYTES, 0);
    buffer.extend_from_slice(&payload);
    let payload_len = payload.len();
    ensure_payload_size(MessageKind::Response, payload_len, MAX_RESPONSE_BYTES)
        .map_err(ConnectionError::Encode)?;
    let length = u32::try_from(payload_len).map_err(|_| {
        ConnectionError::Encode(ProtocolError::MessageTooLarge {
            kind: MessageKind::Response,
            actual: payload_len,
            maximum: MAX_RESPONSE_BYTES,
        })
    })?;
    buffer[..LENGTH_PREFIX_BYTES].copy_from_slice(&length.to_be_bytes());
    writer.write_all(buffer).map_err(ConnectionError::Write)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StorageBackend {
    Memory,
    Glacier,
}

impl StorageBackend {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Glacier => "glacier",
        }
    }

    fn parse(value: &str) -> Result<Self, DaemonError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "memory" => Ok(Self::Memory),
            "glacier" => Ok(Self::Glacier),
            _ => Err(DaemonError::InvalidStorageBackend {
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone)]
enum EnrollmentMode {
    Closed,
    Open,
    Token(String),
}
impl EnrollmentMode {
    fn from_environment() -> Result<Self, DaemonError> {
        let mode =
            env::var("OGD_ENROLLMENT_MODE").unwrap_or_else(|_| DEFAULT_ENROLLMENT_MODE.to_owned());
        match mode.trim().to_ascii_lowercase().as_str() {
            "closed" => Ok(Self::Closed),
            "open" => Ok(Self::Open),
            "token" => {
                let token = env::var("OGD_ENROLLMENT_TOKEN").map_err(|source| {
                    DaemonError::Environment {
                        name: "OGD_ENROLLMENT_TOKEN",
                        source,
                    }
                })?;
                if token.is_empty() {
                    return Err(DaemonError::InvalidStorageBackend {
                        value: "OGD_ENROLLMENT_TOKEN must not be empty".to_owned(),
                    });
                }
                Ok(Self::Token(token))
            }
            _ => Err(DaemonError::InvalidStorageBackend {
                value: format!("invalid OGD_ENROLLMENT_MODE {mode:?}"),
            }),
        }
    }
    fn allows(&self, token: Option<&str>) -> bool {
        match self {
            Self::Closed => false,
            Self::Open => true,
            Self::Token(expected) => {
                token.is_some_and(|value| constant_time_eq(value.as_bytes(), expected.as_bytes()))
            }
        }
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

#[derive(Debug)]
struct Configuration {
    bind_address: String,
    local_bind_address: Option<String>,
    read_timeout: Duration,
    write_timeout: Duration,
    storage_backend: StorageBackend,
    storage_path: PathBuf,
    files_path: PathBuf,
    import_metrics: bool,
    debug_query: bool,
    authorization_mode: AuthorizationMode,
    enrollment_mode: EnrollmentMode,
    classic_auth_enabled: bool,
    memory_limit_bytes: Option<usize>,
    memory_profile: MemoryProfileConfig,
    bootstrap_admin_path: PathBuf,
    bootstrap_password_file: Option<PathBuf>,
    bootstrap_password: Option<String>,
    backup_path: PathBuf,
    instance_id: String,
    heartbeat_interval: Option<Duration>,
    authenticated_keepalive: bool,
    node_identity: Option<String>,
    node_identity_file: Option<PathBuf>,
    node_identity_password: Option<String>,
    node_capabilities: ServiceCapabilities,
    node_role: String,
    gateway_token: Option<String>,
}

impl Configuration {
    fn from_environment() -> Result<Self, DaemonError> {
        let bind_address = env::var("OGD_BIND").unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_owned());
        let local_bind_address = env::var("OGD_LOCAL_BIND").ok().filter(|value| !value.trim().is_empty());
        let read_timeout = duration_from_environment("OGD_READ_TIMEOUT_MS", DEFAULT_READ_TIMEOUT_MS)?;
        let write_timeout = duration_from_environment("OGD_WRITE_TIMEOUT_MS", DEFAULT_WRITE_TIMEOUT_MS)?;
        let storage_backend = StorageBackend::parse( &env::var("OGD_STORAGE").unwrap_or_else(|_| DEFAULT_STORAGE_BACKEND.to_owned()), )?;
        let storage_path = PathBuf::from( env::var("OGD_STORAGE_PATH").unwrap_or_else(|_| DEFAULT_STORAGE_PATH.to_owned()), );
        let files_path = env::var("OGD_FILES_PATH").map(PathBuf::from).unwrap_or_else(|_| {
            storage_path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| Path::new(".")).join("files")
        });
        let import_metrics = boolean_from_environment("OGD_IMPORT_METRICS", false)?;
        let debug_query = boolean_from_environment("OGD_DEBUG_QUERY", false)?;
        let authorization_mode = if boolean_from_environment("OGD_AUTH_REQUIRED", false)? { AuthorizationMode::Enforced } else { AuthorizationMode::Permissive };
        let enrollment_mode = EnrollmentMode::from_environment()?;
        let classic_auth_enabled = boolean_from_environment("OGD_CLASSIC_AUTH_ENABLED", false)?;
        let memory_limit_bytes = optional_bytes_from_environment("OGD_MEMORY_LIMIT")?;
        let memory_profile = memory_limit_bytes.map_or_else( MemoryProfileConfig::unlimited, MemoryProfileConfig::for_limit, );
        let bootstrap_admin_path = env::var("OGD_BOOTSTRAP_ADMIN_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                storage_path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."))
                    .join("bootstrap/admin.ogid")
            });
        let bootstrap_password_file = env::var("OGD_BOOTSTRAP_PASSWORD_FILE").ok().map(PathBuf::from);
        let bootstrap_password = env::var("OGD_BOOTSTRAP_PASSWORD").ok();
        let backup_path = env::var("OGD_BACKUP_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                storage_path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."))
                    .join("backups")
            });
        fs::create_dir_all(&backup_path).map_err(|source| {
            DaemonError::PrepareStorageDirectory {
                path: backup_path.clone(),
                source,
            }
        })?;
        let instance_id = load_or_create_instance_id(&storage_path)?;
        let heartbeat_enabled = boolean_from_environment("OGD_HEARTBEAT_ENABLED", true)?;
        let heartbeat_interval = heartbeat_enabled .then(|| duration_from_environment("OGD_HEARTBEAT_INTERVAL_MS", 5_000)) .transpose()?;
        let authenticated_keepalive = boolean_from_environment("OGD_AUTH_KEEPALIVE_ENABLED", true)?;
        let node_identity = env::var("OGD_NODE_IDENTITY").ok().filter(|value| !value.trim().is_empty());
        let node_identity_file = env::var("OGD_NODE_IDENTITY_FILE").ok().filter(|value| !value.trim().is_empty()).map(PathBuf::from);
        let node_identity_password = env::var("OGD_NODE_IDENTITY_PASSWORD").ok().filter(|value| !value.is_empty());
        let node_capabilities_raw = env::var("OGD_NODE_CAPABILITIES")
            .unwrap_or_else(|_| DEFAULT_NODE_CAPABILITIES.to_owned());
        let node_capabilities = ServiceCapabilities::from_names(
            node_capabilities_raw.split(',').map(str::trim).filter(|value| !value.is_empty())
        ).map_err(DaemonError::Runtime)?;
        let node_role = env::var("OGD_NODE_ROLE").unwrap_or_else(|_| "master".to_owned()).trim().to_ascii_lowercase();
        if !matches!(node_role.as_str(), "master" | "node") {
            return Err(DaemonError::Runtime(format!("OGD_NODE_ROLE must be master or node; got {node_role:?}")));
        }
        let gateway_token = env::var("OGD_GATEWAY_TOKEN").ok().filter(|value| !value.is_empty());

        Ok(Self {
            bind_address,
            local_bind_address,
            read_timeout,
            write_timeout,
            storage_backend,
            storage_path,
            files_path,
            import_metrics,
            debug_query,
            authorization_mode,
            enrollment_mode,
            classic_auth_enabled,
            memory_limit_bytes,
            memory_profile,
            bootstrap_admin_path,
            bootstrap_password_file,
            bootstrap_password,
            backup_path,
            instance_id,
            heartbeat_interval,
            authenticated_keepalive,
            node_identity,
            node_identity_file,
            node_identity_password,
            node_capabilities,
            node_role,
            gateway_token,
        })
    }
}

impl Configuration {
    fn bootstrap_password(&self) -> Result<Vec<u8>, DaemonError> {
        if let Some(path) = &self.bootstrap_password_file {
            let mut password = fs::read(path).map_err(DaemonError::BootstrapAdmin)?;
            while password.last().is_some_and(|byte| matches!(byte, b'\n' | b'\r')) { password.pop(); }
            if !password.is_empty() { return Ok(password); }
        }
        self.bootstrap_password
            .as_ref()
            .filter(|value| !value.is_empty())
            .map(|value| value.as_bytes().to_vec())
            .ok_or(DaemonError::BootstrapPasswordMissing)
    }
}

fn load_or_create_instance_id(storage_path: &Path) -> Result<String, DaemonError> {
    let parent = storage_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| DaemonError::PrepareStorageDirectory {
        path: parent.to_path_buf(),
        source,
    })?;
    let path = parent.join("instance.id");
    if let Ok(value) = fs::read_to_string(&path) {
        if !value.trim().is_empty() {
            return Ok(value.trim().to_owned());
        }
    }
    let instance_id = UuidV7Generator::new().next_id().to_string();
    fs::write(&path, format!("{instance_id}\n")).map_err(|source| {
        DaemonError::PrepareStorageDirectory {
            path: path.clone(),
            source,
        }
    })?;
    Ok(instance_id)
}

fn backup_metadata(instance_id: &str) -> backup::BackupMetadata {
    backup::BackupMetadata {
        created_at: unix_time_millis(),
        source: backup::BackupSource {
            instance_id: instance_id.to_owned(),
            hostname: env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_owned()),
            platform: env::consts::OS.to_owned(),
            arch: env::consts::ARCH.to_owned(),
            core_version: env!("CARGO_PKG_VERSION").to_owned(),
        },
    }
}

fn backup_file_path(root: &Path, name: &str) -> Option<PathBuf> {
    let candidate = Path::new(name);
    if candidate.file_name()?.to_str()? != name || candidate.components().count() != 1 {
        return None;
    }
    let filename = if name.ends_with(".ogb") {
        name.to_owned()
    } else {
        format!("{name}.ogb")
    };
    Some(root.join(filename))
}

fn format_bytes(bytes: Option<usize>) -> String { const MIB: f64 = 1024.0 * 1024.0; const GIB: f64 = 1024.0 * MIB; match bytes { None => "unlimited".to_owned(), Some(value) if value as f64 >= GIB => format!("{:.2} GiB", value as f64 / GIB), Some(value) => format!("{:.2} MiB", value as f64 / MIB), } }
fn optional_bytes_from_environment(name: &'static str) -> Result<Option<usize>, DaemonError> { match env::var(name) { Ok(value) => parse_byte_size(name, &value).map(Some), Err(env::VarError::NotPresent) => Ok(None), Err(source) => Err(DaemonError::Environment { name, source }), } }
fn parse_byte_size(name: &'static str, value: &str) -> Result<usize, DaemonError> { const KIB: u128 = 1024; const MIB: u128 = 1024 * KIB; const GIB: u128 = 1024 * MIB; let invalid = || DaemonError::InvalidByteSize { name, value: value.to_owned(), }; let trimmed = value.trim(); if trimmed.is_empty() { return Err(invalid()); } let split = trimmed .find(|character: char| !(character.is_ascii_digit() || character == '.')) .unwrap_or(trimmed.len()); let (number, suffix) = trimmed.split_at(split); if number.is_empty() || number == "." || number.matches('.').count() > 1 { return Err(invalid()); } let (whole, fraction) = number.split_once('.').unwrap_or((number, "")); let whole = if whole.is_empty() { 0 } else { whole.parse::<u128>().map_err(|_| invalid())? }; let fraction_value = if fraction.is_empty() { 0 } else { fraction.parse::<u128>().map_err(|_| invalid())? }; let fraction_scale = 10_u128 .checked_pow(u32::try_from(fraction.len()).map_err(|_| invalid())?) .ok_or_else(invalid)?; let multiplier = match suffix.trim().to_ascii_lowercase().as_str() { "" | "m" | "mb" | "mib" => MIB, "b" => 1, "k" | "kb" | "kib" => KIB, "g" | "gb" | "gib" => GIB, _ => return Err(invalid()), }; let integral_bytes = whole.checked_mul(multiplier).ok_or_else(invalid)?; let fractional_bytes = fraction_value .checked_mul(multiplier) .and_then(|bytes| bytes.checked_div(fraction_scale)) .ok_or_else(invalid)?; let bytes = integral_bytes .checked_add(fractional_bytes) .filter(|bytes| *bytes > 0) .ok_or_else(invalid)?; usize::try_from(bytes).map_err(|_| invalid()) }
fn boolean_from_environment(name: &'static str, default: bool) -> Result<bool, DaemonError> { match env::var(name) { Ok(value) => match value.trim().to_ascii_lowercase().as_str() { "1" | "true" | "yes" | "on" => Ok(true), "0" | "false" | "no" | "off" => Ok(false), _ => Err(DaemonError::InvalidBoolean { name, value }), }, Err(env::VarError::NotPresent) => Ok(default), Err(source) => Err(DaemonError::Environment { name, source }), } }
fn duration_from_environment( name: &'static str, default_milliseconds: u64, ) -> Result<Duration, DaemonError> { let value = match env::var(name) { Ok(value) => value, Err(env::VarError::NotPresent) => { return Ok(Duration::from_millis(default_milliseconds)); } Err(source) => { return Err(DaemonError::Environment { name, source }); } }; let milliseconds = value .parse::<u64>() .map_err(|source| DaemonError::InvalidDuration { name, value: value.clone(), source, })?; if milliseconds == 0 { return Err(DaemonError::ZeroDuration { name }); } Ok(Duration::from_millis(milliseconds)) }

#[derive(Debug, Clone, Copy)]
struct ClassicLoginAttempt { failures: u32, retry_at: u64 }

fn classic_password_hash(password: &str) -> Result<(String, String), String> {
    let mut salt = [0u8; 16];
    fs::File::open("/dev/urandom").and_then(|mut file| file.read_exact(&mut salt)).map_err(|error| error.to_string())?;
    let hash = classic_password_hash_with_salt(password, &salt)?;
    Ok((encode_base64(&salt), encode_base64(&hash)))
}

fn classic_password_hash_with_salt(password: &str, salt: &[u8]) -> Result<[u8; 32], String> {
    let params = Params::new(CLASSIC_AUTH_MEMORY_KIB, CLASSIC_AUTH_ITERATIONS, CLASSIC_AUTH_LANES, Some(32)).map_err(|error| error.to_string())?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut output = [0u8; 32];
    argon.hash_password_into(password.as_bytes(), salt, &mut output).map_err(|error| error.to_string())?;
    Ok(output)
}

fn classic_password_verify(password: &str, salt: &str, expected: &str) -> bool {
    let Ok(salt) = decode_base64(salt) else { return false; };
    let Ok(expected) = decode_base64(expected) else { return false; };
    let Ok(actual) = classic_password_hash_with_salt(password, &salt) else { return false; };
    constant_time_eq(&actual, &expected)
}

fn classic_login_wait(settings: &ConnectionSettings, identifier: &str) -> Option<u64> {
    let attempts = settings.classic_auth_attempts.lock().ok()?;
    let retry_at = attempts.get(identifier)?.retry_at;
    let now = unix_time_millis();
    (retry_at > now).then_some(retry_at - now)
}

fn classic_login_failure(settings: &ConnectionSettings, identifier: &str) {
    if let Ok(mut attempts) = settings.classic_auth_attempts.lock() {
        let entry = attempts.entry(identifier.to_owned()).or_insert(ClassicLoginAttempt { failures: 0, retry_at: 0 });
        entry.failures = entry.failures.saturating_add(1);
        if entry.failures >= 5 {
            let delay = 1_000u64.saturating_mul(1u64 << entry.failures.saturating_sub(5).min(5));
            entry.retry_at = unix_time_millis().saturating_add(delay.min(30_000));
        }
    }
}

fn classic_login_success(settings: &ConnectionSettings, identifier: &str) {
    if let Ok(mut attempts) = settings.classic_auth_attempts.lock() { attempts.remove(identifier); }
}

#[derive(Debug)]
struct ConnectionSettings {
    read_timeout: Duration,
    write_timeout: Duration,
    import_metrics: bool,
    authorization_mode: AuthorizationMode,
    enrollment_mode: EnrollmentMode,
    classic_auth_enabled: bool,
    classic_auth_attempts: Mutex<HashMap<String, ClassicLoginAttempt>>,
    authenticated_keepalive: bool,
    backup_path: PathBuf,
    instance_id: String,
    storage_backend: StorageBackend,
    glacier_storage: Option<Arc<GlacierStorage>>,
    files_path: PathBuf,
    service_capabilities: ServiceCapabilities,
}

#[derive(Debug)]
enum QueryTextError {
    Parse(String),
    Planning(String),
}

impl Display for QueryTextError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(error) | Self::Planning(error) => formatter.write_str(error),
        }
    }
}

impl Error for QueryTextError {
    #[inline]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

#[derive(Debug)]
enum ConnectionError {
    ConfigureSocket(io::Error),
    CloneSocket(io::Error),
    Read(io::Error),
    Write(io::Error),
    Encode(og_core::protocol::ProtocolError),
    Protocol(og_core::protocol::ProtocolError),
}

impl Display for ConnectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigureSocket(error) => {
                write!(formatter, "cannot configure socket: {error}")
            }
            Self::CloneSocket(error) => {
                write!(formatter, "cannot clone socket: {error}")
            }
            Self::Read(error) => {
                write!(formatter, "cannot read request: {error}")
            }
            Self::Write(error) => {
                write!(formatter, "cannot write response: {error}")
            }
            Self::Encode(error) => {
                write!(formatter, "cannot encode response: {error}")
            }
            Self::Protocol(error) => {
                write!(formatter, "protocol error: {error}")
            }
        }
    }
}

impl Error for ConnectionError {
    #[inline]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigureSocket(error)
            | Self::CloneSocket(error)
            | Self::Read(error)
            | Self::Write(error) => Some(error),
            Self::Encode(error) | Self::Protocol(error) => Some(error),
        }
    }
}

#[derive(Debug)]
enum DaemonError {
    BootstrapAdmin(io::Error),
    BootstrapIdentity(og_core::access::identity_file::IdentityFileError),
    NodeIdentity(og_core::access::identity_file::IdentityFileError),
    NodeIdentityPasswordMissing,
    NodeDeviceCredentialState,
    NodeDeviceCredentialConflict { device_id: String },
    BootstrapPasswordMissing,
    BootstrapAdminState,
    BootstrapAppsState,
    Environment {
        name: &'static str,
        source: env::VarError,
    },
    InvalidDuration {
        name: &'static str,
        value: String,
        source: std::num::ParseIntError,
    },
    InvalidBoolean {
        name: &'static str,
        value: String,
    },
    InvalidByteSize {
        name: &'static str,
        value: String,
    },
    ZeroDuration {
        name: &'static str,
    },
    InvalidStorageBackend {
        value: String,
    },
    PrepareStorageDirectory {
        path: PathBuf,
        source: io::Error,
    },
    OpenStorage {
        path: PathBuf,
        source: StorageError,
    },
    Bind {
        address: String,
        source: io::Error,
    },
    LocalAddress(io::Error),
    SpawnConnectionThread(io::Error),
    Runtime(String),
}

impl Display for DaemonError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::BootstrapAdmin(source) => {
                write!(formatter, "cannot create bootstrap administrator: {source}")
            }
            Self::BootstrapIdentity(source) => write!(formatter, "cannot create encrypted bootstrap identity: {source}"),
            Self::NodeIdentity(source) => write!(formatter, "cannot load node identity: {source}"),
            Self::NodeIdentityPasswordMissing => formatter.write_str("OGD_NODE_IDENTITY_FILE requires OGD_NODE_IDENTITY_PASSWORD"),
            Self::NodeDeviceCredentialState => formatter.write_str("cannot persist local node device credential"),
            Self::NodeDeviceCredentialConflict { device_id } => write!(formatter, "local device credential conflicts with configured node identity for device `{device_id}`"),
            Self::BootstrapPasswordMissing => formatter.write_str("bootstrap identity requires OGD_BOOTSTRAP_PASSWORD_FILE or OGD_BOOTSTRAP_PASSWORD"),
            Self::BootstrapAdminState => {
                formatter.write_str("cannot persist bootstrap administrator")
            }
            Self::BootstrapAppsState => formatter.write_str("cannot persist built-in Apps from apps.json"),
            Self::Environment { name, source } => {
                write!(formatter, "cannot read {name}: {source}")
            }
            Self::InvalidDuration {
                name,
                value,
                source,
            } => write!(
                formatter,
                "{name} must be a positive integer in milliseconds; got `{value}`: {source}"
            ),
            Self::InvalidBoolean { name, value } => write!(
                formatter,
                "{name} must be one of 1/0, true/false, yes/no, on/off; got `{value}`"
            ),
            Self::InvalidByteSize { name, value } => write!(
                formatter,
                "{name} must be a positive byte size such as 268435456, 256M or 2G; got `{value}`"
            ),
            Self::ZeroDuration { name } => {
                write!(formatter, "{name} must be greater than zero")
            }
            Self::InvalidStorageBackend { value } => write!(
                formatter,
                "OGD_STORAGE must be `memory` or `glacier`; got `{value}`"
            ),
            Self::PrepareStorageDirectory { path, source } => write!(
                formatter,
                "cannot create storage directory {}: {source}",
                path.display()
            ),
            Self::OpenStorage { path, source } => write!(
                formatter,
                "cannot open storage at {}: {source}",
                path.display()
            ),
            Self::Bind { address, source } => {
                write!(formatter, "cannot bind {address}: {source}")
            }
            Self::LocalAddress(source) => {
                write!(formatter, "cannot read listening address: {source}")
            }
            Self::SpawnConnectionThread(source) => {
                write!(formatter, "cannot spawn connection thread: {source}")
            }
            Self::Runtime(message) => write!(formatter, "cannot build query runtime: {message}"),
        }
    }
}

impl Error for DaemonError {
    #[inline]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::BootstrapAdmin(source) => Some(source),
            Self::BootstrapIdentity(source) | Self::NodeIdentity(source) => Some(source),
            Self::BootstrapPasswordMissing => None,
            Self::Environment { source, .. } => Some(source),
            Self::InvalidDuration { source, .. } => Some(source),
            Self::PrepareStorageDirectory { source, .. } => Some(source),
            Self::OpenStorage { source, .. } => Some(source),
            Self::Bind { source, .. } => Some(source),
            Self::LocalAddress(source) => Some(source),
            Self::SpawnConnectionThread(source) => Some(source),
            Self::BootstrapAdminState
            | Self::BootstrapAppsState
            | Self::InvalidBoolean { .. }
            | Self::InvalidByteSize { .. }
            | Self::ZeroDuration { .. }
            | Self::InvalidStorageBackend { .. }
            | Self::Runtime(_)
            | Self::NodeIdentityPasswordMissing
            | Self::NodeDeviceCredentialState
            | Self::NodeDeviceCredentialConflict { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use og_core::storage::CollectionId;
    #[test] fn borrowed_partial_document_encodes_large_js_safe_integers_as_numbers() { let document = Document::from_fields([ ("created_at", Value::from(1_785_680_802_608_u64)), ("small", Value::from(42_u64)), ("negative", Value::from(-5_000_000_000_i64)), ]); let response = BorrowedPlainDocumentResponse { kind: "response", status: "partial", version: PROTOCOL_VERSION, id: RequestId::string("query-1").unwrap(), data: BorrowedDocument(&document), }; let payload = rmp_serde::to_vec_named(&response).unwrap(); let decoded: JsonValue = rmp_serde::from_slice(&payload).unwrap(); assert!(decoded["data"]["created_at"].is_f64()); assert!(decoded["data"]["negative"].is_f64()); assert!(decoded["data"]["small"].is_u64()); }
    #[test] fn enrollment_grants_event_subscription_permission() { let query = enrollment_events_permission_query("identity-a", 42); assert_eq!( query, r#"on _permissions | insert {identityId: "identity-a", action: "events.subscribe", resource: "*", effect: "allow", state: "active", createdAt: 42}"# ); }
    #[test] fn wildcard_permission_authorizes_events_subscribe() { let storage = MemoryStorage::new(); let storage: Arc<dyn StorageEngine> = Arc::new(storage); let runtime = Arc::new(build_runtime().expect("runtime builds")); let lowerer = Arc::new(ScanPlanLowerer::new()); let engine = Engine::new(storage, runtime, lowerer); let insert = execute_request( &engine, QueryRequest::new( 1, r#"on _permissions | insert {identityId: "identity-a", action: "*", resource: "*", effect: "allow", state: "active"}"#, ), ); assert!(insert.is_ok()); let request = AuthorizationRequest { identity_id: "identity-a".to_owned(), action: AuthorizationAction::EventsSubscribe, resource: "*".to_owned(), }; assert!(permission_exists(&engine, &request)); }
    #[test] fn execute_request_uses_the_planner_cache() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let runtime = Arc::new(build_runtime().expect("runtime builds")); let lowerer = Arc::new(ScanPlanLowerer::new()); let engine = Engine::new(storage, runtime, lowerer); let query = "from users"; let first = execute_request(&engine, QueryRequest::new(1, query)); assert!(matches!(first, QueryResponse::Ok { .. })); assert_eq!(engine.planner_cache_stats().misses, 1); assert_eq!(engine.planner_cache_stats().hits, 0); let second = execute_request(&engine, QueryRequest::new(2, query)); assert!(matches!(second, QueryResponse::Ok { .. })); assert_eq!(engine.planner_cache_stats().misses, 1); assert_eq!(engine.planner_cache_stats().hits, 1); }
    #[test] fn raw_query_results_include_document_id() { let storage = MemoryStorage::new(); let collection = CollectionId::parse("users").unwrap(); let id = og_core::storage::UuidV7Generator::new().next_id(); let document = Arc::new(Document::from_fields([("name", Value::string("Alice"))])); let mut transaction = storage.begin().unwrap(); transaction.insert(&collection, id, document).unwrap(); transaction.commit().unwrap(); let storage: Arc<dyn StorageEngine> = Arc::new(storage); let runtime = Arc::new(build_runtime().expect("runtime builds")); let lowerer = Arc::new(ScanPlanLowerer::new()); let engine = Engine::new(storage, runtime, lowerer); let response = execute_request(&engine, QueryRequest::new(1, "from users")); let QueryResponse::Ok { documents, .. } = response else { panic!("query must succeed"); }; assert_eq!(documents.len(), 1); assert_eq!( documents[0].get("_id"), Some(&JsonValue::String(id.to_string())) ); assert_eq!( documents[0].get("name"), Some(&JsonValue::String("Alice".to_owned())) ); }
    #[test] fn transformed_plans_do_not_expose_document_id() { let source = og_core::query::PhysicalSource::collection_scan(CollectionId::parse("users").unwrap()); let plan = og_core::query::PhysicalPlan::new( source, [og_core::query::PhysicalOperator::select([ og_core::query::ExpressionFieldPath::new(["name"]).unwrap(), ]) .unwrap()], ) .unwrap(); assert!(!exposes_document_id(&plan)); }
    #[test] fn parses_flexible_memory_limit_values() { let mib = 1024 * 1024; let gib = 1024 * mib; assert_eq!(parse_byte_size("TEST", "512M").unwrap(), 512 * mib); assert_eq!(parse_byte_size("TEST", "850m").unwrap(), 850 * mib); assert_eq!(parse_byte_size("TEST", "1G").unwrap(), gib); assert_eq!(parse_byte_size("TEST", "2GiB").unwrap(), 2 * gib); assert_eq!(parse_byte_size("TEST", "1.5G").unwrap(), 1536 * mib); assert_eq!(parse_byte_size("TEST", "512").unwrap(), 512 * mib); assert_eq!(parse_byte_size("TEST", "0.5G").unwrap(), 512 * mib); assert_eq!(parse_byte_size("TEST", "1024KiB").unwrap(), mib); }
    #[test] fn rejects_invalid_memory_limit_values() { for value in ["", "0", ".", "1.2.3G", "12T", "-1G"] { assert!(parse_byte_size("TEST", value).is_err(), "{value}"); } }
    #[test] fn parses_supported_storage_backends() { assert_eq!( StorageBackend::parse("memory").expect("memory"), StorageBackend::Memory ); assert_eq!( StorageBackend::parse("glacier").expect("glacier"), StorageBackend::Glacier ); assert_eq!( StorageBackend::parse(" GLACIER ").expect("normalized glacier"), StorageBackend::Glacier ); assert_eq!(StorageBackend::Memory.as_str(), "memory"); assert_eq!(StorageBackend::Glacier.as_str(), "glacier"); assert!(StorageBackend::parse("redb").is_err()); }
    #[test] fn rejects_unknown_storage_backend() { let error = StorageBackend::parse("sqlite").expect_err("must reject unknown backend"); assert!(matches!( error, DaemonError::InvalidStorageBackend { value } if value == "sqlite" )); }
    #[test] fn adapts_official_ast_to_planner_pipeline() { let source = "from users | where active == true | inspect verbose"; let pipeline = parse_pipeline(source).expect("pipeline parses"); assert_eq!(pipeline.source(), "users"); assert_eq!(pipeline.len(), 2); assert_eq!(pipeline.stages()[0].name().as_str(), "where"); assert_eq!(pipeline.stages()[0].arguments(), "active == true"); assert_eq!(pipeline.stages()[1].name().as_str(), "inspect"); assert_eq!(pipeline.stages()[1].arguments(), "verbose"); }
    #[test] fn preserves_qualified_collection_name() { let pipeline = parse_pipeline("from tenant.analytics.events | where active == true") .expect("pipeline parses"); assert_eq!(pipeline.source(), "tenant.analytics.events"); }
    #[test] fn preserves_stage_spans_from_official_ast() { let source = "from users | where active == true"; let pipeline = parse_pipeline(source).expect("pipeline parses"); let stage = &pipeline.stages()[0]; assert_eq!(stage.span(), og_core::query::Span::new(11, source.len())); }
    #[test] fn rejects_query_without_source() { let error = parse_pipeline("where active == true").expect_err("must fail"); assert!(matches!(error, QueryTextError::Parse(_))); }
    #[test] fn rejects_empty_pipeline_segments() { let error = parse_pipeline("from users || where active").expect_err("must fail"); assert!(matches!(error, QueryTextError::Parse(_))); }
    #[test] fn accepts_identifier_stage_for_planner_validation() { let pipeline = parse_pipeline("from users | limit 10").expect("syntax parses"); assert_eq!(pipeline.len(), 1); assert_eq!(pipeline.stages()[0].name().as_str(), "limit"); assert_eq!(pipeline.stages()[0].arguments(), "10"); }
    #[test] fn preserves_compound_stage_subpipelines() { let source = "from sales | pivot | rows region | columns month | values amount | aggregate sum | end"; let pipeline = parse_pipeline(source).expect("compound pipeline parses"); let stage = &pipeline.stages()[0]; assert_eq!(stage.name().as_str(), "pivot"); assert!(stage.is_compound()); assert_eq!(stage.subpipeline().expect("pivot body").len(), 4); }
    #[test] fn authenticated_keepalive_filter_is_added_once() { let mut types = vec!["sharing.*".to_owned()]; ensure_authenticated_keepalive_type(&mut types); ensure_authenticated_keepalive_type(&mut types); assert_eq!( types, vec!["sharing.*".to_owned(), "core.heartbeat".to_owned()] ); }
    #[test] fn authenticated_keepalive_filter_respects_wildcards() { for mut types in [ vec!["*".to_owned()], vec!["core.*".to_owned()], vec!["core.heartbeat".to_owned()], ] { ensure_authenticated_keepalive_type(&mut types); assert_eq!(types.len(), 1); } }
}
