//! OpenGlacier daemon runtime.
#![cfg_attr(rustfmt, rustfmt_skip)]
#![allow(unused)]

mod admission;
mod frames;

/// Answers the request with an error and stops its handler.
macro_rules! reject {
    ($id:expr, $code:expr, $message:expr $(,)?) => {
        return Err($crate::daemon::serve::request_error($id, $code, $message))
    };
}

mod serve;
mod query_ops;
mod auth_ops;
mod core_ops;
mod place_ops;
mod file_ops;
mod gateway;
#[cfg(feature = "agent")]
mod agent;
mod file_sync;

use admission::{AdmissionPriority, ConnectionAdmission, ConnectionPermit};
use serve::{routing_error, serve_connection};
use file_ops::*;
use gateway::*;
#[cfg(feature = "agent")]
use agent::*;
use file_sync::*;
use std::{
    env, thread, fs, error::Error, fmt::{self, Display, Formatter}, io::{self, BufReader, Read, Write},
    net::{TcpListener, TcpStream}, path::{Path, PathBuf}, process::{Command, Stdio}, collections::{HashMap, HashSet}, sync::{ atomic::{AtomicBool, AtomicU64, Ordering}, mpsc, Arc, Condvar, Mutex, OnceLock },
    time::{Duration, Instant},
};
use crate::access::{
    auth::{validate_ed25519_public_key, verify_ed25519, ConnectionAuth, DeviceCredential, DEFAULT_CHALLENGE_TTL},
    bootstrap::BootstrapAdmin,
    identity_file::{self, IdentityCredential},
    authorization::{
        AuthorizationAction, AuthorizationMode,
        AuthorizationRequest, QueryAccess,
    },
    place::{
        parse_sharing_permission, sharing_permission, ExecutionContext, PlaceAccess, PlaceRole, PublicAccess,
        RequestedExecutionContext,
    },
};
use crate::{
    build as build_profile,
    debug::{self, DebugTopic},
    helpers::{decode_base64, document_to_json, elapsed_micros, encode_base64, fnv1a64_continue, insert_query, query_string, quoted, JsonFields, lock_unpoisoned, system_time_millis, unix_time_millis, write_file_atomic, APP_INSTANCE_SCOPE_FIELD, FNV1A64_OFFSET, PLACE_SCOPE_FIELD},
    engine::Engine, files::{FileEntry, FileId, FileStore, FileStoreEntry, FileStoreError, FileSyncConfig, FileSyncEntryState, FileSyncIndex, FileSyncIndexEntry, FileSyncSelectionMode, FileWrite, NativeFileStore, StoreId, APP_FILES_DIRECTORY, PRIMARY_APPS_COLLISION_NAME, file_sync_projection_component, file_sync_projection_suffix}, Principal, backup,
    event_engine::{EventEngine, EventSubscription},
    memory::{MemoryClass, MemoryGovernor, MemoryProfileConfig, WorkloadClass},
    operation::{
        decode_operation_request, AccessPolicy, ExecutionMode, ServiceCapability, ServiceCapabilities, AppCreateInput, AuthBeginInput, AuthEnrollBeginInput, ClassicAuthLoginInput, ClassicAuthRegisterInput, ChallengeSignatureInput, DeviceRegisterInput, DeviceRenameInput, DeviceRevokeInput, IdentityOpenInput, IdentityRegisterInput, IdentityRenewInput, PasswordInput, QueryExecuteInput, QueryContextResolveInput, AppDeleteInput, AppIdInput,
        AppInstanceCreateInput, AppInstanceRemoveInput, AppUpdateInput, DataAnalyzeInput, DataImportInput, DataMappingSaveInput, DataMappingListInput, DataMappingUpdateInput, DataMappingDeleteInput, Audience, BackupNameInput,
        CollectionsListInput,
        BackupRestoreInput, HandlerKind, OperationKind, OperationRequest, OperationResponse, OperationRouter, OperationScope, PermissionGrantInput,
        PermissionRevokeInput, PlaceAccessRemoveInput, PlaceAccessSetInput, PlaceCreateInput, PlacePublicSetInput,
        PlaceDeleteInput, PlaceIdInput, PlaceUpdateInput, PlaceResourceSetInput, PlaceResourceRemoveInput, FabricResourceSetInput, FabricResourceRemoveInput, Routed, RoutedOperation, SharingCreateInput,
        SharingDeleteInput, SharingUpdateInput, OPERATION_CATALOG,
    },
    protocol::{
        encode_message, ensure_payload_size, MessageKind, ProtocolError, QueryRequest,
        QueryResponse, RequestId, StreamResponse, WireError, LENGTH_PREFIX_BYTES,
        MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, PROTOCOL_VERSION,
    },
    query::{
        parse as parse_query, vcollections, DocumentScope, PlannerPipeline,
    },
    storage::{GlacierStorage, MemoryStorage, ScanOptions, StorageEngine, StorageError, StorageRead, UuidV7Generator},
    Document, Number, Value,
};

#[cfg(feature = "files")]
use crate::{
    files::FileRange,
    operation::{
        FileEntryInput, FileListInput, FileMkdirInput, FileMoveInput, FileReadInput,
        FileScopeInput, FileSyncConfigSetInput, FileSyncSelectionRemoveInput,
        FileSyncSelectionSetInput, FileVersionInput, FileVersionReadInput, FileWriteInput,
    },
};
#[cfg(feature = "events")]
use crate::operation::EventsSubscribeInput;
#[cfg(feature = "data-import")]
use crate::operation::DataWorkerRunInput;
#[cfg(feature = "llm")]
use crate::service::llm::{LlmGenerationEvent, LlmService};
#[cfg(feature = "agent")]
use crate::service::agent::{AgentCapabilityInvoker, AgentCapabilityResponse, AgentError, AgentService};

#[cfg(feature = "auth")]
use argon2::{Algorithm, Argon2, Params, Version};
#[cfg(feature = "fabric")]
use sha2::{Digest, Sha256};
use serde::{ ser::{Error as SerializeError, SerializeMap, SerializeSeq}, Deserialize, Serialize, Serializer, };
use serde_json::Value as JsonValue;
#[cfg(feature = "fabric")]
use tungstenite::{connect as websocket_connect, Message as WebSocketMessage, WebSocket, stream::MaybeTlsStream};
#[cfg(feature = "files-watch")]
use notify::{RecommendedWatcher, RecursiveMode, Watcher};

const DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:7878";
#[cfg(feature = "fabric")]
const NODE_CONTROL_PROTOCOL_VERSION: u64 = 3;
const DEFAULT_READ_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_WRITE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_AGENT_PROVIDER_READ_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_STORAGE_BACKEND: &str = "memory";
const DEFAULT_STORAGE_PATH: &str = "data/ogd.glacier";
const DEFAULT_FILES_STORE_ID: &str = "native";
const DEFAULT_ENROLLMENT_MODE: &str = "open";
const CLASSIC_AUTH_MIN_PASSWORD_BYTES: usize = 12;
const CLASSIC_AUTH_MEMORY_KIB: u32 = 64 * 1024;
const CLASSIC_AUTH_ITERATIONS: u32 = 3;
const CLASSIC_AUTH_LANES: u32 = 1;
const BUILTIN_APPS_JSON: &str = include_str!("../../apps.json");
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
static EVENT_OUTBOX_WAKE: OnceLock<EventOutboxWake> = OnceLock::new();
static EVENT_OUTBOX_ID_GENERATOR: OnceLock<UuidV7Generator> = OnceLock::new();
const EVENT_OUTBOX_RETRY_DELAY: Duration = Duration::from_millis(250);
const EVENT_OUTBOX_MAX_IDLE_RECOVERY: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct EventOutboxWake {
    generation: Mutex<u64>,
    changed: Condvar,
}

impl EventOutboxWake {
    fn new() -> Self {
        Self { generation: Mutex::new(0), changed: Condvar::new() }
    }

    fn notify(&self) {
        let mut generation = self.generation.lock().expect("event outbox wake lock poisoned");
        *generation = (*generation).wrapping_add(1);
        self.changed.notify_one();
    }

    fn wait(&self, observed: &mut u64, timeout: Option<Duration>) {
        let mut generation = self.generation.lock().expect("event outbox wake lock poisoned");
        if *generation != *observed {
            *observed = *generation;
            return;
        }
        generation = match timeout {
            Some(timeout) => {
                let (generation, _) = self.changed
                    .wait_timeout(generation, timeout)
                    .expect("event outbox wake lock poisoned");
                generation
            }
            None => self.changed.wait(generation).expect("event outbox wake lock poisoned"),
        };
        *observed = *generation;
    }
}

fn event_outbox_wake() -> &'static EventOutboxWake {
    EVENT_OUTBOX_WAKE.get_or_init(EventOutboxWake::new)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BuiltinApp {
    app_id: String,
    name: String,
    version: String,
    definition: JsonValue,
}

pub fn run() -> Result<(), DaemonError> {
    let configuration = Configuration::from_environment()?;
    let version = env!("CARGO_PKG_VERSION");
    let fabric_enabled = !configuration.gateway_endpoints.is_empty();
    if fabric_enabled && !build_profile::has_fabric() {
        return Err(DaemonError::Runtime(
            "Gateway mode requires an ogd build with the `fabric` feature".to_owned(),
        ));
    }
    let listener_bind_address = if fabric_enabled {
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

    let local_address = match (fabric_enabled, listener.as_ref()) {
        (true, Some(listener)) => format!(
            "outbound [{}], local {}",
            configuration.gateway_endpoints.join(","),
            listener.local_addr().map_err(DaemonError::LocalAddress)?
        ),
        (true, None) => format!("outbound [{}]", configuration.gateway_endpoints.join(",")),
        (false, Some(listener)) => listener.local_addr().map_err(DaemonError::LocalAddress)?.to_string(),
        (false, None) => unreachable!("standalone ogd always has a listener"),
    };

    #[cfg(feature = "db-engine")]
    let (engine, glacier_storage) = {
        let (engine_value, glacier_storage) = build_engine(&configuration)?;
        (Some(Arc::new(engine_value)), glacier_storage)
    };
    #[cfg(not(feature = "db-engine"))]
    let (engine, glacier_storage): (Option<Arc<Engine>>, Option<Arc<GlacierStorage>>) = (None, None);
    if configuration.node_capabilities.contains(ServiceCapability::Files) {
        fs::create_dir_all(&configuration.files_path).map_err(|source| DaemonError::PrepareStorageDirectory {
            path: configuration.files_path.clone(),
            source,
        })?;
        if let Some(sync_root) = configuration.files_sync_root.as_ref() {
            fs::create_dir_all(sync_root).map_err(|source| DaemonError::PrepareStorageDirectory {
                path: sync_root.clone(),
                source,
            })?;
            debug::log( DebugTopic::Core, None, format!("Files sync root configured at {}", sync_root.display()), );
        }
    }
    debug::log(DebugTopic::Core, None, format!( "starting storage={} bind={} enabled={} published={} compiled={}", if build_profile::has_db_engine() { configuration.storage_backend.as_str() } else { "disabled" }, configuration.bind_address, configuration.node_capabilities.names().join(","), configuration.published_capabilities.names().join(","), ServiceCapabilities::compiled().names().join(","), ));
    if let Some(engine) = engine.as_ref() {
        start_debug_memory_reporter(Arc::clone(engine));
    }
    let node_credential = if fabric_enabled {
        Some(load_node_identity_credential(&configuration)?)
    } else {
        None
    };
    if configuration.node_capabilities.contains(ServiceCapability::Auth) {
        let engine = engine.as_ref().expect("auth capability requires the db-engine feature");
        // Bootstrap is a local Core concern and must not depend on whether the fabric
        // transport is inbound or outbound. Once _identities is populated this is a no-op,
        // so normal reboots do not need admin.ogid or the bootstrap password.
        bootstrap_admin_if_needed(engine, &configuration)?;
        if fabric_enabled {
            let credential = node_credential.as_ref().expect("gateway nodes require an identity credential");
            ensure_node_device_credential(engine, credential)?;
            bootstrap_fabric_defaults_if_needed(engine, credential, configuration.published_capabilities)?;
            if configuration.node_capabilities.contains(ServiceCapability::Events) {
                ensure_node_events_permission(engine, credential);
            }
        }
    }
    if configuration.node_capabilities.contains(ServiceCapability::Database) {
        let engine = engine.as_ref().expect("database capability requires the db-engine feature");
        bootstrap_apps_if_needed(engine)?;
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
        let engine = engine.as_ref().expect("events capability requires the db-engine feature");
        start_event_outbox_worker(Arc::clone(engine), Arc::clone(&event_engine));
    } else {
        debug::log(DebugTopic::Events, None, "event service disabled by OGD_NODE_CAPABILITIES");
    }
    #[cfg(feature = "llm")]
    let llm_service = if configuration.node_capabilities.contains(ServiceCapability::Llm) {
        Some(Arc::new(LlmService::from_environment().map_err(|error| {
            DaemonError::Runtime(format!("unable to initialize llm provider: {error}"))
        })?))
    } else {
        None
    };
    #[cfg(feature = "agent")]
    let agent_service = if configuration.node_capabilities.contains(ServiceCapability::Agent) {
        Some(Arc::new(AgentService::from_environment().map_err(|error| {
            DaemonError::Runtime(format!("unable to initialize agent provider: {error}"))
        })?))
    } else {
        None
    };
    let connection_settings = Arc::new(ConnectionSettings {
        read_timeout: configuration.read_timeout,
        write_timeout: configuration.write_timeout,
        agent_provider_read_timeout: configuration.agent_provider_read_timeout,
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
        file_sync: Mutex::new(if configuration.node_capabilities.contains(ServiceCapability::Files) {
            load_file_sync_runtime(&configuration)?
        } else {
            FileSyncRuntime::disabled(&configuration)
        }),
        file_sync_run: Mutex::new(()),
        file_sync_applying: Arc::new(AtomicBool::new(false)),
        file_sync_watcher_dirty: Arc::new(AtomicBool::new(false)),
        service_capabilities: configuration.node_capabilities,
        published_capabilities: configuration.published_capabilities,
        #[cfg(feature = "llm")]
        llm_service,
        #[cfg(feature = "agent")]
        agent_service,
        upstream: configuration.gateway_endpoints.first().cloned(),
        upstream_client_endpoints: Mutex::new(
            configuration.gateway_endpoints.iter()
                .filter_map(|endpoint| gateway_client_endpoint_from_node(endpoint))
                .collect()
        ),
        upstream_identity: node_credential.clone(),
        gateway_state: Mutex::new(if fabric_enabled { "connecting" } else { "standalone" }),
        gateway_connections: Mutex::new(HashMap::new()),
        connection_admission: Arc::new(ConnectionAdmission::new(engine.as_ref().map_or_else(
            || configuration.memory_limit_bytes.map_or_else(MemoryGovernor::unlimited, MemoryGovernor::with_process_limit),
            |engine| engine.memory_governor().clone(),
        ))),
    });
    let _file_sync_worker = if configuration.node_capabilities.contains(ServiceCapability::Files) {
        let engine = engine.as_ref().expect("files capability requires the db-engine feature");
        start_file_sync_worker(Arc::clone(&connection_settings), Arc::clone(engine), Arc::clone(&event_engine))
    } else {
        None
    };
    let _upstream_event_relay = start_upstream_event_relay(Arc::clone(&connection_settings), Arc::clone(&event_engine));
    let with_engine = engine.as_ref().map_or("none", |_| configuration.storage_backend.as_str());
    let with_storage_path = if engine.is_some() {
        match configuration.storage_backend {
            StorageBackend::Memory => "virtual".to_owned(),
            StorageBackend::Glacier => configuration.storage_path.display().to_string(),
        }
    } else {
        "disabled".to_owned()
    };
    let with_modules = {
        let metrics = if configuration.import_metrics {
            ", metrics enabled"
        } else {
            ""
        };
        let query_debug = if configuration.debug_query {
            ", query instrumentation enabled"
        } else {
            ""
        };
        let authorization = format!(
            ", authorization {}{}",
            configuration.authorization_mode.as_str(),
            query_debug
        );

        format!(
            "memory limit {}, profile {}, runtime reserve {}, managed budget {}, planner cache {}, operation budget {}, query budget {}, import budget {}, heavy concurrency {}{}{}",
            format_bytes(configuration.memory_profile.process_limit_bytes),
            configuration.memory_profile.effective_profile_label(),
            format_bytes(Some(configuration.memory_profile.runtime_reserve_bytes)),
            format_bytes(configuration.memory_profile.managed_budget_bytes),
            format_bytes(Some(configuration.memory_profile.planner_cache_bytes)),
            format_bytes(Some(configuration.memory_profile.operation_budget_bytes)),
            format_bytes(Some(configuration.memory_profile.query_budget_bytes)),
            format_bytes(Some(configuration.memory_profile.import_budget_bytes)),
            format_bytes(Some(configuration.memory_profile.max_concurrent_heavy)),
            authorization,
            metrics,
        )
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
    println!("
    \x1b[1mopenglacier core\x1b[0m - \x1b[3mantaa palaa\x1b[0m
    \x1b[2mv{version}\x1b[0m

    \x1b[1mRuntime\x1b[0m
    storage      {with_engine} ({with_storage_path})
    transport    {local_address}

    \x1b[1mModules\x1b[0m
    {with_modules1}
    {with_modules2}
    {with_modules3}
    {with_modules4}
    {with_modules5}
    {with_modules6}
    {with_modules7}
    {with_modules8}
    {with_modules9}
    {with_modules10}
    {with_modules11}
    {with_modules12}
    {with_modules13}
    {with_modules14}

    \x1b[2mCopyright (c) 2023-2026 openglacier.org · MIT License\x1b[0m
    "
    );

    if fabric_enabled {
        if let Some(listener) = listener {
            spawn_listener_thread(
                listener,
                engine.clone(),
                Arc::clone(&operation_router),
                Arc::clone(&event_engine),
                Arc::clone(&connection_settings),
                "ogd-local-listener",
            )?;
        }
        run_gateway_fabric(
            configuration,
            node_credential.expect("gateway nodes require an identity credential"),
            engine,
            operation_router,
            event_engine,
            connection_settings,
        )
    } else {
        let listener = listener.expect("standalone ogd listener");
        accept_connections(&listener, &engine, &operation_router, &event_engine, &connection_settings, "ogd", "ogd-client");
        Ok(())
    }
}

fn spawn_listener_thread( listener: TcpListener, engine: Option<Arc<Engine>>, operation_router: Arc<OperationRouter>, event_engine: Arc<EventEngine>, settings: Arc<ConnectionSettings>, thread_name: &str, ) -> Result<(), DaemonError> {
    let name = thread_name.to_owned();
    thread::Builder::new().name(name.clone()).spawn(move || {
        accept_connections(&listener, &engine, &operation_router, &event_engine, &settings, &name, "ogd-local-client");
    }).map_err(DaemonError::SpawnConnectionThread)?;
    Ok(())
}

/// Serves a listener forever. Admission happens before `accept`, so clients
/// that exceed the memory-derived capacity wait in the kernel backlog.
fn accept_connections( listener: &TcpListener, engine: &Option<Arc<Engine>>, operation_router: &Arc<OperationRouter>, event_engine: &Arc<EventEngine>, settings: &Arc<ConnectionSettings>, listener_name: &str, connection_name: &str, ) {
    loop {
        let permit = settings.connection_admission.acquire(1, AdmissionPriority::Local);
        match listener.accept() {
            Ok((stream, _)) => {
                if let Err(error) = spawn_serving_connection(
                    stream,
                    permit,
                    engine.clone(),
                    Arc::clone(operation_router),
                    Arc::clone(event_engine),
                    Arc::clone(settings),
                    connection_name,
                ) {
                    eprintln!("{listener_name}: cannot spawn client connection: {error}");
                }
            }
            Err(error) => {
                eprintln!("{listener_name} accept error: {error}");
                // Descriptor exhaustion fails every accept: do not spin on it.
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn spawn_serving_connection( stream: TcpStream, permit: ConnectionPermit, engine: Option<Arc<Engine>>, operation_router: Arc<OperationRouter>, event_engine: Arc<EventEngine>, settings: Arc<ConnectionSettings>, thread_name: &str, ) -> Result<(), DaemonError> {
    let name = thread_name.to_owned();
    thread::Builder::new().name(name).spawn(move || {
        let _permit = permit;
        let peer = stream.peer_addr().ok();
        let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        debug::log( DebugTopic::Network, Some(connection_id), format!( "connection peer={}", peer.map_or_else(|| "unknown".to_owned(), |value| value.to_string()) ), );
        if let Err(error) = serve_connection(
            connection_id,
            stream,
            engine.as_deref(),
            &operation_router,
            &event_engine,
            &settings,
            None,
        ) {
            debug::log( DebugTopic::Network, Some(connection_id), format!("connection error: {error}"), );
            eprintln!("ogd connection: {error}");
        }
        debug::log(DebugTopic::Network, Some(connection_id), "disconnect");
    }).map_err(DaemonError::SpawnConnectionThread)?;
    Ok(())
}



fn load_node_identity_credential(configuration: &Configuration) -> Result<IdentityCredential, DaemonError> {
    match (&configuration.node_identity_file, &configuration.node_identity_password) {
        (Some(path), Some(password)) => identity_file::load(path, password.as_bytes())
            .map_err(DaemonError::NodeIdentity),
        (Some(_), None) => Err(DaemonError::NodeIdentityPasswordMissing),
        (None, _) => Err(DaemonError::NodeIdentityMissing),
    }
}

/// Ensures that the credential used by this Node to join the fabric can
/// also authenticate to the local control listener.
///
/// This deliberately creates only the public `_devices` record. It does not bootstrap an
/// identity, grant permissions, create a Place, or persist private key material. Existing
/// records are never rewritten: an unexpected identity/key/state collision is treated as an
/// inconsistent local store and fails startup.
fn ensure_node_device_credential(engine: &Engine, credential: &IdentityCredential) -> Result<(), DaemonError> {
    let lookup = quoted!("on _devices | where deviceId == {} | limit 1", &credential.device_id);
    // A fresh client store may not have `_devices` yet. Reads against a missing
    // collection are therefore equivalent to "credential absent" here. The insert
    // below creates/populates the collection through the normal Core query path. If
    // that insert fails, we still surface a real local credential-state error.
    if let Ok(Some(document)) = query_first(engine, 0, lookup) {
        let expected = document.str_field("identityId") == Some(credential.identity_id.as_str())
            && document.str_field("publicKey") == Some(credential.public_key.as_str())
            && document.str_field("algorithm") == Some("ed25519")
            && document.str_field("encoding") == Some("spki-der")
            && document.str_field("state") == Some("active");
        if expected {
            return Ok(());
        }
        return Err(DaemonError::NodeDeviceCredentialConflict { device_id: credential.device_id.clone() });
    }

    let created_at = unix_time_millis();
    let insert = insert_query("_devices", serde_json::json!({"deviceId": &credential.device_id, "identityId": &credential.identity_id, "publicKey": &credential.public_key, "algorithm": "ed25519", "encoding": "spki-der", "state": "active", "createdAt": created_at}));
    if !query_run(engine, 0, insert).is_ok() {
        return Err(DaemonError::NodeDeviceCredentialState);
    }
    debug::log( DebugTopic::Auth, None, format!( "registered local node device credential identity={} device={}", credential.identity_id, credential.device_id, ), );
    Ok(())
}

fn ensure_node_events_permission(engine: &Engine, credential: &IdentityCredential) {
    let identity = query_string(&credential.identity_id);
    let lookup = format!(
        r#"on _permissions | where identityId == {identity} and state == "active" and effect == "allow" and (action == "events.subscribe" or action == "*") and (resource == "*") | limit 1"#
    );
    if query_has_rows(engine, 0, lookup) {
        return;
    }

    let query = enrollment_events_permission_query(&credential.identity_id, unix_time_millis());
    if query_run(engine, 0, query).is_ok() {
        debug::log( DebugTopic::Auth, None, format!( "grant local node identity={} action=events.subscribe resource=* source=node-bootstrap", credential.identity_id ), );
    } else {
        debug::log( DebugTopic::Auth, None, format!( "cannot grant local node identity={} action=events.subscribe resource=*", credential.identity_id ), );
    }
}

const FABRIC_DEFAULTS_ID: &str = "defaults";

fn bootstrap_fabric_defaults_if_needed( engine: &Engine, credential: &crate::access::identity_file::IdentityCredential, published: ServiceCapabilities, ) -> Result<(), DaemonError> {
    let query = quoted!("on _fabric_resources | where fabricId == {} and state == \"active\" | limit 1", FABRIC_DEFAULTS_ID);
    if query_has_rows(engine, 0, query) {
        return Ok(());
    }

    let now = unix_time_millis();
    let assignments: Vec<JsonValue> = published
        .names()
        .into_iter()
        .filter(|capability| *capability != "auth")
        .map(|capability| {
            if capability == "files" {
                serde_json::json!({
                    "identityId": &credential.identity_id,
                    "deviceId": &credential.device_id,
                    "capability": capability,
                    "role": "primary",
                    "serviceRole": "primary",
                    "storageRole": "provider",
                    "assignedBy": &credential.identity_id,
                    "assignedAt": now,
                })
            } else {
                serde_json::json!({
                    "identityId": &credential.identity_id,
                    "deviceId": &credential.device_id,
                    "capability": capability,
                    "role": "provider",
                    "assignedBy": &credential.identity_id,
                    "assignedAt": now,
                })
            }
        })
        .collect();
    let insert = insert_query("_fabric_resources", serde_json::json!({"fabricId": FABRIC_DEFAULTS_ID, "resourceAssignments": assignments, "state": "active", "createdAt": now, "updatedAt": now}));
    if !query_run(engine, 0, insert).is_ok() {
        return Err(DaemonError::BootstrapFabricResourcesState);
    }
    debug::log( DebugTopic::Gateway, None, format!( "seed fabric defaults device={} capabilities={}", credential.device_id, assignments .iter() .filter_map(|assignment| assignment.str_field("capability")) .collect::<Vec<_>>() .join(",") ), );
    Ok(())
}

fn bootstrap_admin_if_needed( engine: &Engine, configuration: &Configuration, ) -> Result<(), DaemonError> {
    let count = query_first(engine, 0, "on _identities | count").ok().flatten().and_then(|value| value.u64_field("count"));
    if count.unwrap_or(0) != 0 { return Ok(()); }

    let password = configuration.bootstrap_password()?;
    let admin = BootstrapAdmin::generate().map_err(DaemonError::BootstrapIdentity)?;
    let staged = admin.stage(&configuration.bootstrap_admin_path, &password)
        .map_err(DaemonError::BootstrapIdentity)?;
    for query in admin.registration_queries() {
        if !query_run(engine, 0, query).is_ok() {
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
    let collections = model.object_field("collections");
    let tables = model.object_field("tables");

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
        let physical_name = declaration.as_str().or_else(|| declaration.str_field("name"))
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
        let collection = table.str_field("collection")
            .ok_or_else(|| format!("table {alias:?} must reference a collection"))?;
        if !collections.contains_key(collection) {
            return Err(format!("table {alias:?} references undeclared collection {collection:?}"));
        }
        if let Some(fields) = table.get("fields") {
            let fields = fields.as_array().ok_or_else(|| format!("table {alias:?}.fields must be an array"))?;
            let mut names = std::collections::BTreeSet::new();
            for field in fields {
                let name = field.str_field("name")
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
    let apps: Vec<BuiltinApp> = serde_json::from_str(BUILTIN_APPS_JSON)
        .expect("embedded apps.json must be valid");
    bootstrap_apps(engine, &apps)
}

fn bootstrap_apps(engine: &Engine, apps: &[BuiltinApp]) -> Result<(), DaemonError> {
    let now = unix_time_millis();
    for app in apps {
        if let Err(message) = validate_app_definition_model(&app.definition) {
            eprintln!("openglacier: invalid built-in App {}: {message}", app.app_id);
            return Err(DaemonError::BootstrapAppsState);
        }

        let app_id = query_string(&app.app_id);
        let definition = serde_json::to_string(&app.definition)
            .expect("embedded App definition serializes");
        let exists = match app_record_exists(engine, RequestId::Number(0), &app.app_id) {
            Ok(value) => value,
            Err(_) => return Err(DaemonError::BootstrapAppsState),
        };
        let query = if exists {
            quoted!("on _apps | where appId == {app_id} | set name = {}, version = {}, definition = {definition}, state = \"active\", updatedBy = \"system\", updatedAt = {now}", &app.name, &app.version)
        } else {
            insert_query("_apps", serde_json::json!({"appId": &app.app_id, "name": &app.name, "version": &app.version, "definition": &app.definition, "createdBy": "system", "state": "active", "createdAt": now}))
        };
        if !query_run(engine, 0, query).is_ok() {
            return Err(DaemonError::BootstrapAppsState);
        }
        let views = app.definition.get("queryViews").and_then(JsonValue::as_array).map_or(&[][..], Vec::as_slice);
        if let Err(message) = crate::view::deliver(engine, &app.app_id, views) {
            eprintln!("openglacier: invalid built-in App {}: {message}", app.app_id);
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
    Ok((
        Engine::new(storage).with_memory_governor(governor),
        glacier_storage,
    ))
}

fn build_storage( configuration: &Configuration, governor: MemoryGovernor, ) -> Result<(Arc<dyn StorageEngine>, Option<Arc<GlacierStorage>>), DaemonError> {
    match configuration.storage_backend {
        StorageBackend::Memory if configuration.storage_history => Ok((Arc::new(MemoryStorage::with_history()), None)),
        StorageBackend::Memory => Ok((Arc::new(MemoryStorage::new()), None)),
        StorageBackend::Glacier => {
            prepare_storage_directory(&configuration.storage_path)?;
            GlacierStorage::open_governed(&configuration.storage_path, governor)
                .and_then(|storage| {
                    if configuration.storage_history {
                        storage.record_history()?;
                    }
                    Ok(storage)
                })
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

#[derive(Debug, Clone)]
struct GatewayDelegation {
    principal: Principal,
    place_id: Option<String>,
    capability: String,
    app_instance_id: Option<String>,
    place_role: Option<PlaceRole>,
    public_access: Option<PublicAccess>,
    token: Option<String>,
}

impl GatewayDelegation {
    /// The Place grant resolved by the Authority, when the Gateway transmitted one.
    fn access(&self) -> Option<PlaceAccess> {
        self.place_role
            .map(PlaceAccess::Role)
            .or_else(|| self.public_access.map(PlaceAccess::Public))
    }
}

#[cfg(feature = "llm")]
fn llm_run_owner(authentication: &ConnectionAuth, delegation: Option<&GatewayDelegation>) -> String {
    let principal = match authentication.principal() {
        Principal::Anonymous => "anonymous".to_owned(),
        Principal::Identity { identity_id, device_id } => format!("{identity_id}/{device_id}"),
    };
    match delegation {
        Some(value) => format!(
            "{principal}|{}|{}",
            value.place_id.as_deref().unwrap_or(""),
            value.app_instance_id.as_deref().unwrap_or("")
        ),
        None => principal,
    }
}



#[cfg(any(feature = "events", test))]
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

fn write_event( connection_id: u64, writer: &mut TcpStream, event: &crate::CoreEvent, ) -> Result<(), ConnectionError> {
    debug::log( DebugTopic::Events, Some(connection_id), format!("deliver type={} event_id={}", event.event_type, event.id), );
    if debug::protocol_enabled() {
        let value = serde_json::to_value(event)
            .map(debug::redact_json)
            .unwrap_or(JsonValue::Null);
        debug::log( DebugTopic::Protocol, Some(connection_id), format!("> event {}", value), );
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
    let query = quoted!("on _devices | where deviceId == {} and identityId == {} | limit 1", device_id, identity_id);
    let Some(document) = query_first(engine, request_id, query)? else {
        return Ok(None);
    };
    let credential = DeviceCredential {
        identity_id: document.string_field("identityId"),
        device_id: document.string_field("deviceId"),
        public_key: document.string_field("publicKey"),
        algorithm: document.string_field("algorithm"),
        encoding: document.string_field("encoding"),
        active: document.str_field("state") == Some("active"),
    };
    if credential.identity_id.is_empty() || credential.device_id.is_empty() || credential.public_key.is_empty() {
        return Err(QueryResponse::request_error(request_id, "auth.invalid_device_record", "device credential is incomplete"));
    }
    Ok(Some(credential))
}

fn load_device_credential( engine: &Engine, request_id: RequestId, identity_id: &str, device_id: &str, ) -> Result<DeviceCredential, QueryResponse> {
    match try_load_device_credential(engine, request_id, identity_id, device_id)? {
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

fn place_resource_capability_allowed(value: &str) -> bool {
    valid_resource_capability(value) && value != "auth"
}


fn resource_assignment_service_role(entry: &JsonValue) -> Option<&str> {
    if let Some(role) = entry.str_field("serviceRole") {
        return match role {
            "primary" | "replica" => Some(role),
            _ => None,
        };
    }
    match entry.str_field("role") {
        Some(role @ ("primary" | "replica")) => Some(role),
        _ => None,
    }
}

fn resource_assignment_storage_role(entry: &JsonValue) -> Option<&str> {
    if let Some(role) = entry.str_field("storageRole") {
        return match role {
            "provider" | "sync" => Some(role),
            _ => None,
        };
    }
    match entry.str_field("role") {
        Some(role @ ("provider" | "sync")) => Some(role),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
struct ResourceAssignmentError {
    code: &'static str,
    message: &'static str,
}

fn upsert_resource_assignment( mut assignments: Vec<JsonValue>, assigned_by: &str, identity_id: &str, device_id: &str, capability: &str, role: Option<&str>, service_role: Option<&str>, storage_role: Option<&str>, ) -> Result<Vec<JsonValue>, ResourceAssignmentError> {
    let existing = assignments.iter().find(|entry| {
        let same_identity = entry.get("identityId").or_else(|| entry.get("nodeIdentityId")).and_then(JsonValue::as_str) == Some(identity_id);
        let stored_device = entry.get("deviceId").or_else(|| entry.get("nodeDeviceId")).or_else(|| entry.get("nodeId")).and_then(JsonValue::as_str);
        let same_device = stored_device.is_none() || stored_device == Some(device_id);
        same_identity && same_device && entry.str_field("capability") == Some(capability)
    }).cloned();

    assignments.retain(|entry| {
        let same_identity = entry.get("identityId").or_else(|| entry.get("nodeIdentityId")).and_then(JsonValue::as_str) == Some(identity_id);
        let stored_device = entry.get("deviceId").or_else(|| entry.get("nodeDeviceId")).or_else(|| entry.get("nodeId")).and_then(JsonValue::as_str);
        let same_device = stored_device.is_none() || stored_device == Some(device_id);
        !(same_identity && same_device && entry.str_field("capability") == Some(capability))
    });

    if capability == "files" {
        let mut next_service_role = existing.as_ref().and_then(resource_assignment_service_role).map(str::to_owned);
        let mut next_storage_role = existing.as_ref().and_then(resource_assignment_storage_role).map(str::to_owned);

        if let Some(role) = role {
            match role {
                "primary" | "replica" => next_service_role = Some(role.to_owned()),
                "provider" | "sync" => next_storage_role = Some(role.to_owned()),
                _ => return Err(ResourceAssignmentError { code: "resource.invalid_role", message: "files role must be primary, replica, provider or sync" }),
            }
        }
        if let Some(service_role) = service_role {
            match service_role {
                "none" => next_service_role = None,
                "primary" | "replica" => next_service_role = Some(service_role.to_owned()),
                _ => return Err(ResourceAssignmentError { code: "resource.invalid_service_role", message: "files serviceRole must be none, primary or replica" }),
            }
        }
        if let Some(storage_role) = storage_role {
            match storage_role {
                "none" => next_storage_role = None,
                "provider" | "sync" => next_storage_role = Some(storage_role.to_owned()),
                _ => return Err(ResourceAssignmentError { code: "resource.invalid_storage_role", message: "files storageRole must be none, provider or sync" }),
            }
        }
        if next_service_role.as_deref() == Some("primary") {
            assignments.retain(|entry| {
                entry.str_field("capability") != Some("files")
                    || resource_assignment_service_role(entry) != Some("primary")
            });
        }
        if next_service_role.is_some() || next_storage_role.is_some() {
            let compatibility_role = next_service_role.as_deref()
                .or(next_storage_role.as_deref())
                .expect("files assignment has at least one role");
            assignments.push(serde_json::json!({
                "identityId": identity_id,
                "deviceId": device_id,
                "capability": capability,
                "role": compatibility_role,
                "serviceRole": next_service_role,
                "storageRole": next_storage_role,
                "assignedBy": assigned_by,
                "assignedAt": unix_time_millis(),
            }));
        }
        return Ok(assignments);
    }

    if service_role.is_some() || storage_role.is_some() {
        return Err(ResourceAssignmentError { code: "resource.invalid_role_dimensions", message: "serviceRole and storageRole are only supported for files" });
    }
    let Some(role) = role else {
        return Err(ResourceAssignmentError { code: "resource.role_required", message: "role is required for non-files resources" });
    };
    if !matches!(role, "primary" | "replica" | "provider") {
        return Err(ResourceAssignmentError { code: "resource.invalid_role", message: "resource role must be primary, replica or provider" });
    }
    if role == "primary" {
        assignments.retain(|entry| {
            entry.str_field("capability") != Some(capability)
                || entry.str_field("role") != Some("primary")
        });
    }
    assignments.push(serde_json::json!({
        "identityId": identity_id,
        "deviceId": device_id,
        "capability": capability,
        "role": role,
        "assignedBy": assigned_by,
        "assignedAt": unix_time_millis(),
    }));
    Ok(assignments)
}

fn remove_resource_assignment( mut assignments: Vec<JsonValue>, identity_id: &str, device_id: &str, capability: &str, ) -> Vec<JsonValue> {
    assignments.retain(|entry| {
        let same_identity = entry.get("identityId").or_else(|| entry.get("nodeIdentityId")).and_then(JsonValue::as_str) == Some(identity_id);
        let stored_device = entry.get("deviceId").or_else(|| entry.get("nodeDeviceId")).or_else(|| entry.get("nodeId")).and_then(JsonValue::as_str);
        let same_device = stored_device.is_none() || stored_device == Some(device_id);
        !(same_identity && same_device && entry.str_field("capability") == Some(capability))
    });
    assignments
}

fn load_fabric_defaults(engine: &Engine, request_id: RequestId) -> Result<Vec<JsonValue>, QueryResponse> {
    let query = quoted!("on _fabric_resources | where fabricId == {} and state == \"active\" | limit 1", FABRIC_DEFAULTS_ID);
    // Older stores legitimately do not have this collection yet. Treat that
    // as "no defaults"; the first fabric.resource.set will create the record.
    let document = query_first(engine, request_id, query).ok().flatten();
    Ok(document.and_then(|document| document.array_field("resourceAssignments").cloned()).unwrap_or_default())
}

fn save_fabric_defaults(engine: &Engine, request_id: RequestId, assignments: &[JsonValue]) -> Result<(), QueryResponse> {
    let encoded = serde_json::to_string(assignments).expect("fabric resource assignments serialize");
    let now = unix_time_millis();
    let lookup = quoted!("on _fabric_resources | where fabricId == {} and state == \"active\" | limit 1", FABRIC_DEFAULTS_ID);
    let query = if query_has_rows(engine, request_id, lookup) {
        quoted!("on _fabric_resources | where fabricId == {} and state == \"active\" | set resourceAssignments = {encoded}, updatedAt = {now}", FABRIC_DEFAULTS_ID)
    } else {
        insert_query("_fabric_resources", serde_json::json!({"fabricId": FABRIC_DEFAULTS_ID, "resourceAssignments": assignments, "state": "active", "createdAt": now, "updatedAt": now}))
    };
    query_documents(engine, request_id, query).map(drop)
}

fn active_devices_for_assignments(engine: &Engine, request_id: RequestId, assignments: &[JsonValue]) -> Vec<JsonValue> {
    let mut devices = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for assignment in assignments {
        let Some(identity_id) = assignment.get("identityId").or_else(|| assignment.get("nodeIdentityId")).and_then(JsonValue::as_str) else { continue; };
        let Some(device_id) = assignment.get("deviceId").or_else(|| assignment.get("nodeDeviceId")).or_else(|| assignment.get("nodeId")).and_then(JsonValue::as_str) else { continue; };
        if !seen.insert((identity_id.to_owned(), device_id.to_owned())) { continue; }
        let query = quoted!("on _devices | where identityId == {} and deviceId == {} and state == \"active\" | select identityId, deviceId, publicKey | limit 1", identity_id, device_id);
        devices.extend(query_documents(engine, request_id, query).unwrap_or_default());
    }
    devices
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
    document: JsonValue,
}

/// Stable attribution for records written by a principal (`anonymous` without identity).
fn principal_actor(principal: &Principal) -> String {
    principal_identity_id(principal).unwrap_or("anonymous").to_owned()
}

fn principal_identity_id(principal: &Principal) -> Option<&str> {
    match principal {
        Principal::Identity { identity_id, .. } => Some(identity_id.as_str()),
        Principal::Anonymous => None,
    }
}

fn load_place( engine: &Engine, request_id: RequestId, place_id: &str, ) -> Result<PlaceRecord, QueryResponse> {
    let query = quoted!("on _places | where placeId == {} and state == \"active\" | limit 1", place_id);
    let Some(document) = query_first(engine, request_id, query)? else {
        return Err(QueryResponse::request_error(
            request_id,
            "place.not_found",
            "Place was not found",
        ));
    };
    let place_id = document.string_field("placeId");
    let name = document.string_field("name");
    let mood = document.string_field("mood");
    let title = document.string_field("title");
    let subtitle = document.string_field("subtitle");
    let color_scheme = document.str_field("colorScheme").unwrap_or("glacier").to_owned();
    let app_order = document.array_field("appOrder").map(|items| items.iter().filter_map(JsonValue::as_str).map(str::to_owned).collect()).unwrap_or_default();
    let resource_assignments = document.array_field("resourceAssignments").cloned().unwrap_or_default();
    let owner_identity_id = document.string_field("ownerIdentityId");
    let public_access = match document.str_field("publicAccess") {
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
    let created_at = document.u64_field("createdAt");
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

fn app_record_exists( engine: &Engine, request_id: RequestId, app_id: &str, ) -> Result<bool, QueryResponse> {
    let query = quoted!("on _apps | where appId == {} | limit 1", app_id);
    query_documents(engine, request_id, query).map(|documents| !documents.is_empty())
}

fn app_maintainers(app: &JsonValue) -> Vec<String> {
    if let Some(values) = app.array_field("maintainers") {
        return values.iter().filter_map(JsonValue::as_str).map(str::to_owned).collect();
    }
    app.str_field("maintainersJson")
        .and_then(|serialized| serde_json::from_str::<Vec<String>>(serialized).ok())
        .unwrap_or_default()
}

fn app_identity_is_maintainer(app: &JsonValue, identity_id: &str) -> bool {
    app_maintainers(app).iter().any(|value| value == identity_id)
}

fn load_app_definition( engine: &Engine, request_id: RequestId, app_id: &str, ) -> Result<JsonValue, QueryResponse> {
    let query = quoted!("on _apps | where appId == {} and state == \"active\" | limit 1", app_id);
    let Some(document) = query_first(engine, request_id, query)? else {
        return Err(QueryResponse::request_error(
            request_id,
            "app.not_found",
            "App was not found",
        ));
    };
    Ok(document)
}

fn load_app_instance( engine: &Engine, request_id: RequestId, instance_id: &str, ) -> Result<AppInstanceRecord, QueryResponse> {
    let query = quoted!("on _app_instances | where instanceId == {} and state == \"active\" | limit 1", instance_id);
    let Some(document) = query_first(engine, request_id, query)? else {
        return Err(QueryResponse::request_error(
            request_id,
            "app.instance_not_found",
            "App instance was not found",
        ));
    };
    let place_id = document.string_field("placeId");
    if place_id.is_empty() {
        return Err(QueryResponse::request_error(
            request_id,
            "app.invalid_instance_record",
            "App instance has no valid Place",
        ));
    }
    Ok(AppInstanceRecord { place_id, document })
}

/// Whether `principal`, with `access` to its Place, sees the App instance
/// `instance` (C12): the roles of its `config.visibleTo`, else of its App's
/// `access.visibleTo`; none listed, everyone who reads the Place. The Owner
/// and the App's maintainers always see it; a public reader without a role
/// does not see a restricted App.
fn app_instance_visible( engine: &Engine, request_id: RequestId, principal: &Principal, access: &PlaceAccess, instance: &JsonValue, ) -> Result<bool, QueryResponse> {
    let roles = |value: Option<&JsonValue>| value.and_then(JsonValue::as_array).map(|roles| roles.iter().filter_map(JsonValue::as_str).filter_map(PlaceRole::parse).collect::<Vec<_>>());
    let app = match instance.str_field("appId") {
        Some(app_id) => load_app_definition(engine, request_id, app_id).ok(),
        None => None,
    };
    let visible_to = roles(instance.pointer("/config/visibleTo")).or_else(|| roles(app.as_ref().and_then(|app| app.pointer("/definition/access/visibleTo")))).unwrap_or_default();
    Ok(visible_to.is_empty()
        || access.role().is_some_and(|role| role == PlaceRole::Owner || visible_to.contains(&role))
        || app.as_ref().zip(principal_identity_id(principal)).is_some_and(|(app, identity_id)| app_identity_is_maintainer(app, identity_id)))
}

fn place_access_sharing_id(place_id: &str, identity_id: &str) -> String {
    format!("place-access:{place_id}:{identity_id}")
}

fn list_place_access( engine: &Engine, request_id: RequestId, place: &PlaceRecord, ) -> Result<Vec<serde_json::Value>, QueryResponse> {
    let query = quoted!("on _sharings | where owner == {} | select sharingId, target, permissions, placeRole, state | sort target", &place.owner_identity_id);
    let documents = query_documents(engine, request_id, query)?;
    use std::collections::BTreeMap;
    let mut entries: BTreeMap<String, PlaceRole> = BTreeMap::new();
    for document in documents {
        let state = document.str_field("state").unwrap_or_default();
        if state != "accepted" && state != "active" {
            continue;
        }
        let Some(target) = document.str_field("target") else {
            continue;
        };
        // `placeRole` is a scalar optimization used by managed Place access
        // records. It is only scoped by the stable sharingId. Without checking
        // that id, an access entry for another Place owned by the same identity
        // would leak into this Place's access list.
        let expected_sharing_id = place_access_sharing_id(&place.place_id, target);
        if document.str_field("sharingId") == Some(expected_sharing_id.as_str()) {
            if let Some(candidate) = document.str_field("placeRole")
                .and_then(PlaceRole::parse)
            {
                entries.insert(target.to_owned(), candidate);
                continue;
            }
        }
        let Some(permissions) = document.array_field("permissions")
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

fn place_audience( engine: &Engine, request_id: RequestId, place_id: &str, ) -> Result<Audience, QueryResponse> {
    let place = load_place(engine, request_id, place_id)?;
    let mut identities = vec![place.owner_identity_id.clone()];
    for entry in list_place_access(engine, request_id, &place)? {
        if let Some(identity_id) = entry.str_field("identityId") {
            identities.push(identity_id.to_owned());
        }
    }
    Ok(Audience::identities(identities))
}

fn resolve_place_role( engine: &Engine, request_id: RequestId, identity_id: &str, place: &PlaceRecord, ) -> Result<Option<PlaceRole>, QueryResponse> {
    if place.owner_identity_id == identity_id {
        return Ok(Some(PlaceRole::Owner));
    }
    let query = quoted!("on _sharings | where owner == {} and target == {} | select sharingId, permissions, placeRole, state", &place.owner_identity_id, identity_id);
    let documents = query_documents(engine, request_id, query)?;
    let mut role = None;
    for document in documents {
        let state = document.str_field("state").unwrap_or_default();
        if state != "accepted" && state != "active" {
            continue;
        }
        // A scalar `placeRole` belongs to one managed Place access record.
        // Scope it by sharingId so a grant on another Place cannot authorize
        // this Place merely because owner and target identities are the same.
        let expected_sharing_id = place_access_sharing_id(&place.place_id, identity_id);
        if document.str_field("sharingId") == Some(expected_sharing_id.as_str()) {
            if let Some(candidate) = document.str_field("placeRole")
                .and_then(PlaceRole::parse)
            {
                role = Some(candidate);
                continue;
            }
        }
        let Some(permissions) = document.array_field("permissions")
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

/// Resolves what grants `principal` access to `place`: its scope role first, otherwise the
/// Place public policy. The public policy applies whatever the authorization mode and is
/// never translated into a role.
fn resolve_place_access_for_principal( engine: &Engine, request_id: RequestId, principal: &Principal, place: &PlaceRecord, ) -> Result<PlaceAccess, QueryResponse> {
    if let Some(identity_id) = principal_identity_id(principal) {
        if let Some(role) = resolve_place_role(engine, request_id, identity_id, place)? {
            return Ok(PlaceAccess::Role(role));
        }
        if let Some(access) = place.public_access {
            return Ok(PlaceAccess::Public(access));
        }
        return Err(QueryResponse::request_error(
            request_id,
            "authorization.denied",
            format!("identity {:?} has no access to Place {:?}", identity_id, place.place_id),
        ));
    }

    if let Some(access) = place.public_access {
        return Ok(PlaceAccess::Public(access));
    }

    Err(QueryResponse::request_error(
        request_id,
        "authorization.required",
        "authentication is required to access this Place",
    ))
}

/// App identifiers instantiated in at least one public Place.
fn public_place_app_ids(engine: &Engine, request_id: RequestId) -> Result<std::collections::BTreeSet<String>, QueryResponse> {
    let mut app_ids = std::collections::BTreeSet::new();
    for place in list_public_places(engine, request_id)? {
        let Some(place_id) = place.str_field("placeId") else { continue; };
        let query = quoted!("on _app_instances | where placeId == {} and state == \"active\" | select appId", place_id);
        let documents = query_documents(engine, request_id, query)?;
        app_ids.extend(documents.iter().filter_map(|document| document.str_field("appId").map(str::to_owned)));
    }
    Ok(app_ids)
}

fn list_public_places(engine: &Engine, request_id: RequestId) -> Result<Vec<JsonValue>, QueryResponse> {
    let places = query_documents(engine, request_id, "on _places | where state == \"active\" | sort createdAt")?;
    let mut visible = Vec::new();
    for document in places {
        let Some(place_id) = document.str_field("placeId") else { continue; };
        let place = match load_place(engine, request_id, place_id) { Ok(place) => place, Err(_) => continue };
        if place.public_access.is_some() { visible.push(place.to_json(None)); }
    }
    Ok(visible)
}

fn list_storage_collections( snapshot: &dyn StorageRead, stats: bool, place_id: Option<&str>, app_instance_id: Option<&str>, ) -> Result<Vec<JsonValue>, StorageError> {
    let mut collections = Vec::new();
    for name in snapshot.collections()? {
        if let Some(place_id) = place_id {
            if name.as_str().starts_with('_') {
                continue;
            }
            let expected_place = Value::from(place_id);
            let expected_app_instance = app_instance_id.map(Value::from);
            let mut documents = 0u64;
            snapshot.scan_each(
                &name,
                ScanOptions::default(),
                &mut |stored| {
                    let document = stored.document();
                    let place_matches = document.get(PLACE_SCOPE_FIELD) == Some(&expected_place);
                    let app_matches = expected_app_instance.as_ref().is_none_or(|expected| {
                        document.get(APP_INSTANCE_SCOPE_FIELD) == Some(expected)
                    });
                    if place_matches && app_matches {
                        documents = documents.saturating_add(1);
                        return Ok(stats);
                    }
                    Ok(true)
                },
            )?;
            if documents == 0 {
                continue;
            }
            let mut item = serde_json::json!({"name": name.as_str(), "system": false});
            if stats {
                item["documents"] = serde_json::json!(documents);
            }
            collections.push(item);
            continue;
        }

        let mut item = serde_json::json!({
            "name": name.as_str(),
            "system": name.as_str().starts_with('_'),
        });
        if stats {
            if let Ok(count) = snapshot.count(&name) {
                item["documents"] = serde_json::json!(count);
            }
        }
        collections.push(item);
    }

    if place_id.is_none() {
        for name in vcollections::ALL {
            if !collections.iter().any(|item| {
                item.str_field("name") == Some(name)
            }) {
                collections.push(serde_json::json!({
                    "name": name,
                    "system": true,
                    "virtual": true,
                }));
            }
        }
    }

    Ok(collections)
}

fn resolve_query_execution_context( engine: &Engine, request_id: RequestId, principal: &Principal, requested: RequestedExecutionContext, ) -> Result<ExecutionContext, QueryResponse> {
    let place = load_place(engine, request_id, &requested.place_id)?;
    let access = resolve_place_access_for_principal(engine, request_id, principal, &place)?;

    if let Some(app_instance_id) = requested.app_instance_id.as_deref() {
        let instance = load_app_instance(engine, request_id, app_instance_id)?;
        if !app_instance_visible(engine, request_id, principal, &access, &instance.document)? {
            return Err(QueryResponse::request_error(request_id, "app.instance_forbidden", "this App is not open to your role in this Place"));
        }
        if instance.place_id != requested.place_id {
            return Err(QueryResponse::request_error(
                request_id,
                "app.instance_place_mismatch",
                format!(
                    "App instance {:?} does not belong to Place {:?}",
                    app_instance_id, requested.place_id
                ),
            ));
        }
    }

    Ok(ExecutionContext {
        principal: principal.clone(),
        place_id: requested.place_id,
        app_instance_id: requested.app_instance_id,
        access,
    })
}

/// Places in the identity scope (with their role) and public Places, in creation order.
/// A public Place outside the scope carries no role: its `publicAccess` is the only grant.
fn list_places_for_identity_and_public( engine: &Engine, request_id: RequestId, identity_id: &str, ) -> Result<Vec<JsonValue>, QueryResponse> {
    let places = query_documents(engine, request_id, "on _places | where state == \"active\" | sort createdAt")?;
    let mut visible = Vec::new();
    for document in places {
        let Some(place_id) = document.str_field("placeId") else { continue; };
        let place = match load_place(engine, request_id, place_id) { Ok(place) => place, Err(_) => continue };
        match resolve_place_role(engine, request_id, identity_id, &place)? {
            Some(role) => visible.push(place.to_json(Some(role))),
            None if place.public_access.is_some() => visible.push(place.to_json(None)),
            None => {}
        }
    }
    Ok(visible)
}

#[derive(Debug, Clone)]
struct SharingRecord {
    owner: String,
    target: String,
}

fn load_sharing( engine: &Engine, request_id: RequestId, sharing_id: &str, ) -> Result<SharingRecord, QueryResponse> {
    let query = quoted!("on _sharings | where sharingId == {} | limit 1", sharing_id);
    let Some(document) = query_first(engine, request_id, query)? else {
        return Err(QueryResponse::request_error(
            request_id,
            "sharing.not_found",
            "sharing relation was not found",
        ));
    };
    let owner = document.string_field("owner");
    let target = document.string_field("target");
    if owner.is_empty() || target.is_empty() {
        return Err(QueryResponse::request_error(
            request_id,
            "sharing.invalid_record",
            "sharing relation has no valid owner or target",
        ));
    }
    Ok(SharingRecord { owner, target })
}

fn read_message( reader: &mut BufReader<TcpStream>, payload: &mut Vec<u8>, ) -> Result<usize, ConnectionError> {
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

/// Streams a query's rows when its plan allows it; `Ok(false)` leaves the
/// request to a whole response ([`Engine::execute_physical`]): writes and
/// system collections. Under a Place/App `scope`, the plan reads only the rows
/// of that scope ([`Engine::plan_scoped`]).
fn try_write_streaming_request( engine: &Engine, writer: &mut TcpStream, request: &QueryRequest, scope: Option<(&str, Option<&str>)>, ) -> Result<bool, ConnectionError> {
    let started = Instant::now();
    let parse_started = Instant::now();
    let pipeline = match parse_pipeline(&request.query) {
        Ok(pipeline) => pipeline,
        Err(_) => return Ok(false),
    };
    let parse_us = elapsed_micros(parse_started);
    let plan_started = Instant::now();
    let planned = match scope {
        None => engine.plan_cached(&request.query, &pipeline),
        Some((place, app_instance)) => match engine.plan_scoped(&pipeline, place, app_instance) {
            Ok(Some(planned)) => Ok(planned),
            Ok(None) => return Ok(false),
            Err(error) => Err(error),
        },
    };
    let planned = match planned {
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
        .and_then(crate::query::PhysicalOperator::count_alias)
        .is_some();
    let governed_blocking = engine.supports_governed_blocking_streaming(planned.physical());
    if !memory_streaming && !governed_blocking {
        if scope.is_none() && matches!(
            planned.physical().memory_execution_mode(),
            crate::query::MemoryExecutionMode::GovernedBlocking
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
    // Stages reserve their memory as it grows, see `QueryMemory`.
    let requested_budget = if governed_blocking { crate::memory::QUERY_ADMISSION_BYTES } else { 0 };
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
        .reserve(MemoryClass::Network, MAX_RESPONSE_BYTES + frames::FLUSH_BYTES)
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
    let mut frames = frames::FrameWriter::new(writer);
    let execute_started = Instant::now();
    let mut materialize_us = 0_u64;
    let mut streamed_documents = 0_u64;
    let mut write_document = |id: &crate::storage::DocumentId, document: &Document| {
        let encode_started = Instant::now();
        if terminal_count {
            let response = BorrowedPlainDocumentResponse {
                kind: "response",
                status: "partial",
                version: PROTOCOL_VERSION,
                id: request.id,
                data: BorrowedDocument(document),
            };
            frames.push(&response)
        } else {
            let response = BorrowedDocumentResponse {
                kind: "response",
                status: "partial",
                version: PROTOCOL_VERSION,
                id: request.id,
                data: BorrowedDocumentWithId { id, document },
            };
            frames.push(&response)
        }
        .map_err(|error| {
            crate::engine::EngineError::execution(crate::query::ExecutionError::evaluation(
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

    let statistics = match streamed {
        Ok(Some(statistics)) => statistics,
        Ok(None) => {
            let error = WireError::new("query.execution_failed", "streaming plan unexpectedly fell back to materialized execution");
            frames.finish(&StreamResponse::error(Some(request.id), error))?;
            writer.flush().map_err(ConnectionError::Write)?;
            return Ok(true);
        }
        Err(error) => {
            frames.finish(&StreamResponse::error(Some(request.id), WireError::new("query.execution_failed", error.to_string())))?;
            writer.flush().map_err(ConnectionError::Write)?;
            return Ok(true);
        }
    };
    let execute_us = elapsed_micros(execute_started);
    frames.finish(&StreamResponse::complete(
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
    ))?;
    writer.flush().map_err(ConnectionError::Write)?;
    Ok(true)
}

fn write_streaming_error( writer: &mut TcpStream, request_id: RequestId, code: impl Into<String>, message: impl Into<String>, ) -> Result<(), ConnectionError> {
    write_stream_response(writer, &StreamResponse::error(Some(request_id), WireError::new(code, message)))?;
    writer.flush().map_err(ConnectionError::Write)
}

fn enrollment_events_permission_query(identity_id: &str, created_at: u64) -> String {
    insert_query("_permissions", serde_json::json!({"identityId": identity_id, "action": "events.subscribe", "resource": "*", "effect": "allow", "state": "active", "createdAt": created_at}))
}


fn authorize_connection( engine: &Engine, principal: &crate::access::auth::Principal, action: AuthorizationAction, resource: &str, ) -> bool {
    let Some(request) = AuthorizationRequest::from_principal(principal, action, resource) else {
        return false;
    };
    permission_exists(engine, &request)
}

fn permission_exists(engine: &Engine, request: &AuthorizationRequest) -> bool {
    let identity = query_string(&request.identity_id);
    let action = query_string(request.action.as_str());
    let resource = query_string(&request.resource);
    let query = format!( "on _permissions | where identityId == {identity} and state == \"active\" and effect == \"allow\" and (action == {action} or action == \"*\") and (resource == {resource} or resource == \"*\") | limit 1" );
    query_has_rows(engine, 0, query)
}

fn publish_durable_event( engine: &Engine, audience: Audience, event_type: &str, payload: JsonValue, ) -> bool {
    let created_at = unix_time_millis();
    let event_id = EVENT_OUTBOX_ID_GENERATOR
        .get_or_init(UuidV7Generator::new)
        .next_id()
        .to_string();
    let query = insert_query("_event_outbox", serde_json::json!({"_id": &event_id, "eventId": &event_id, "type": event_type, "audience": &audience, "payload": &payload, "state": "pending", "attempts": 0, "availableAt": created_at, "createdAt": created_at}));
    let stored = query_run(engine, 0, query).is_ok();
    if stored {
        event_outbox_wake().notify();
    }
    debug::log( DebugTopic::Events, None, format!( "outbox {} type={event_type} event_id={event_id}", if stored { "pending" } else { "failed" } ), );
    stored
}

fn start_event_outbox_worker(engine: Arc<Engine>, events: Arc<EventEngine>) {
    thread::Builder::new()
        .name("og-event-outbox".to_owned())
        .spawn(move || {
            // Old versions retained delivered rows forever. New deliveries are
            // deleted immediately; this one-time sweep bounds legacy growth.
            let _ = query_run(&engine, 0, "on _event_outbox | where state == \"delivered\" | delete");

            let wake = event_outbox_wake();
            let mut observed_generation = *wake
                .generation
                .lock()
                .expect("event outbox wake lock poisoned");

            loop {
                let next_wake = drain_event_outbox(&engine, &events);
                wake.wait(&mut observed_generation, next_wake);
            }
        })
        .expect("event outbox worker must start");
}

fn drain_event_outbox(engine: &Engine, events: &EventEngine) -> Option<Duration> {
    loop {
        // Event-driven: query on startup, insertion, or retry deadline instead
        // of polling the database every 100 ms while idle.
        let Ok(documents) = query_documents(engine, 0, "on _event_outbox | where state == \"pending\" | sort availableAt | limit 16") else {
            return Some(EVENT_OUTBOX_MAX_IDLE_RECOVERY);
        };
        if documents.is_empty() {
            return None;
        }

        let now = unix_time_millis();
        let mut processed = 0usize;
        let mut next_retry: Option<Duration> = None;

        for document in documents {
            let available_at = document.u64_field("availableAt").unwrap_or(now);
            if available_at > now {
                let delay = Duration::from_millis(available_at.saturating_sub(now));
                next_retry = Some(next_retry.map_or(delay, |current| current.min(delay)));
                break;
            }

            let Some(row_id) = document.str_field("_id") else {
                debug::log(DebugTopic::Events, None, "outbox invalid row missing _id");
                return Some(EVENT_OUTBOX_MAX_IDLE_RECOVERY);
            };
            let event_id = document.str_field("eventId").unwrap_or(row_id);
            let Some(event_type) = document.str_field("type") else {
                let discard = quoted!("on _event_outbox | where _id == {} | delete", row_id);
                let _ = query_run(engine, 0, discard);
                processed += 1;
                continue;
            };

            let estimate = serde_json::to_vec(&document)
                .map_or(4096, |bytes| bytes.len().saturating_add(1024));
            let Ok(_permit) = engine.memory_governor().reserve(MemoryClass::Indexing, estimate) else {
                debug::log( DebugTopic::Events, None, format!("outbox deferred reason=memory estimate={estimate}"), );
                return Some(EVENT_OUTBOX_RETRY_DELAY);
            };
            let audience = document
                .get("audience")
                .cloned()
                .and_then(|value| serde_json::from_value::<Audience>(value).ok())
                .unwrap_or(Audience::Global);
            let payload = document.get("payload").cloned().unwrap_or(JsonValue::Null);
            let event = crate::CoreEvent::new(
                event_id,
                event_type,
                audience,
                unix_time_millis(),
                payload,
            );

            if events.publish(event) {
                debug::log( DebugTopic::Events, None, format!("publish type={event_type} event_id={event_id}"), );
                let delivered = quoted!("on _event_outbox | where _id == {} | delete", row_id);
                if !query_run(engine, 0, delivered).is_ok() {
                    // A transient delete failure can expose this row again; the
                    // EventEngine suppresses a second delivery of the same id.
                    return Some(EVENT_OUTBOX_RETRY_DELAY);
                }
            } else {
                debug::log( DebugTopic::Events, None, format!("retry type={event_type} event_id={event_id} reason=queue_full"), );
                let retry_at = unix_time_millis().saturating_add(
                    u64::try_from(EVENT_OUTBOX_RETRY_DELAY.as_millis()).unwrap_or(u64::MAX),
                );
                let retry = format!(
                    "on _event_outbox | where _id == {} | set attempts = attempts + 1, availableAt = {}",
                    query_string(row_id),
                    retry_at,
                );
                let _ = query_run(engine, 0, retry);
                next_retry = Some(EVENT_OUTBOX_RETRY_DELAY);
            }
            processed += 1;
        }

        if let Some(delay) = next_retry {
            return Some(delay);
        }
        if processed == 0 {
            return Some(EVENT_OUTBOX_MAX_IDLE_RECOVERY);
        }
        // Continue immediately: there may be more than 16 due rows. Once empty,
        // the worker blocks on the Condvar until a producer wakes it.
    }
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
            debug::log( DebugTopic::Memory, None, format!( "{process} governed={} peak={} reservations={} rejected={}", format_bytes(Some(snapshot.current_bytes)), format_bytes(Some(snapshot.peak_bytes)), snapshot.active_reservations, snapshot.failed_reservations ), );
            thread::sleep(Duration::from_secs(1));
        })
        .expect("debug memory reporter must start");
}

fn execute_request(engine: &Engine, request: QueryRequest) -> QueryResponse {
    execute_request_scoped(engine, request, None)
}

/// Runs `query`, returning its documents or the error response.
fn query_documents(engine: &Engine, request_id: impl Into<RequestId>, query: impl Into<String>) -> Result<Vec<JsonValue>, QueryResponse> {
    match execute_request(engine, QueryRequest::new(request_id, query)) {
        QueryResponse::Ok { documents, .. } => Ok(documents),
        error @ QueryResponse::Error { .. } => Err(error),
    }
}

/// Runs `query`, returning its first document if any.
fn query_first(engine: &Engine, request_id: impl Into<RequestId>, query: impl Into<String>) -> Result<Option<JsonValue>, QueryResponse> {
    query_documents(engine, request_id, query).map(|documents| documents.into_iter().next())
}

/// Runs `query`, keeping only whether it succeeded.
fn query_run(engine: &Engine, request_id: impl Into<RequestId>, query: impl Into<String>) -> Result<(), QueryResponse> {
    query_documents(engine, request_id, query).map(drop)
}

/// True when `query` succeeds and returns at least one document.
fn query_has_rows(engine: &Engine, request_id: impl Into<RequestId>, query: impl Into<String>) -> bool {
    query_documents(engine, request_id, query).is_ok_and(|documents| !documents.is_empty())
}

fn execute_request_scoped( engine: &Engine, request: QueryRequest, context: Option<&ExecutionContext>, ) -> QueryResponse {
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
        crate::memory::QUERY_ADMISSION_BYTES
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
        let scope = match context.app_instance_id.as_deref() {
            Some(app_instance_id) => DocumentScope::new(context.place_id.as_str(), app_instance_id),
            None => DocumentScope::for_place(context.place_id.as_str()),
        };
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
fn execution_row_to_json( row: &crate::query::ExecutionRow, plan: &crate::query::PhysicalPlan, ) -> JsonValue {
    let mut value = document_to_json(row.document());
    if exposes_document_id(plan)
        && !matches!(row.origin(), crate::query::ExecutionRowOrigin::Synthetic)
    {
        if let JsonValue::Object(object) = &mut value {
            object.insert("_id".to_owned(), JsonValue::String(row.id().to_string()));
        }
    }
    value
}

fn exposes_document_id(plan: &crate::query::PhysicalPlan) -> bool {
    plan.operators().iter().all(|operator| {
        matches!(
            operator,
            crate::query::PhysicalOperator::Filter { .. }
                | crate::query::PhysicalOperator::Union { .. }
                | crate::query::PhysicalOperator::Limit { .. }
                | crate::query::PhysicalOperator::Skip { .. }
                | crate::query::PhysicalOperator::Sort { .. }
                | crate::query::PhysicalOperator::Distinct { .. }
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
    id: &'a crate::storage::DocumentId,
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

struct BorrowedDocumentId<'a>(&'a crate::storage::DocumentId);

impl Serialize for BorrowedDocumentId<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(std::str::from_utf8(&self.0.hyphenated()).map_err(serde::ser::Error::custom)?)
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
                    crate::protocol::serialize_js_safe_i64(value, serializer)
                }
                Number::Unsigned(value) => {
                    crate::protocol::serialize_js_safe_u64(value, serializer)
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

/// Plans `query` without running it and returns the plan description.
fn explain_request(engine: &Engine, id: RequestId, query: &str) -> QueryResponse {
    let pipeline = match parse_pipeline(query) {
        Ok(pipeline) => pipeline,
        Err(error) => return QueryResponse::request_error(id, "query.invalid", error.to_string()),
    };
    match engine.explain(&pipeline) {
        Ok(plan) => QueryResponse::success(id, vec![crate::helpers::value_to_json(&crate::Value::object(plan))], None),
        Err(error) => QueryResponse::request_error(id, "query.execution_failed", error.to_string()),
    }
}

fn parse_pipeline(source: &str) -> Result<PlannerPipeline, QueryTextError> {
    let ast = parse_query(source).map_err(|error| QueryTextError::Parse(error.to_string()))?;
    PlannerPipeline::from_ast(source, &ast)
        .map_err(|error| QueryTextError::Planning(error.to_string()))
}

/// Answers a request whose routing reached a handler that cannot serve it.
///
/// This is an internal invariant violation (routing table and handlers
/// disagree, or a feature is missing from the build). The request fails with
/// `operation.internal_error`; the connection and the daemon keep running.
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

fn write_stream_response( writer: &mut TcpStream, response: &StreamResponse, ) -> Result<(), ConnectionError> {
    let encoded =
        crate::protocol::encode_stream_response(response).map_err(ConnectionError::Encode)?;
    writer.write_all(&encoded).map_err(ConnectionError::Write)
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
    /// Stable public name. The enrollment token itself is never exposed.
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::Token(_) => "token",
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

#[derive(Debug, Clone)]
struct Configuration {
    bind_address: String,
    local_bind_address: Option<String>,
    gateway_endpoints: Vec<String>,
    gateway_directory_path: PathBuf,
    read_timeout: Duration,
    write_timeout: Duration,
    agent_provider_read_timeout: Duration,
    storage_backend: StorageBackend,
    storage_path: PathBuf,
    /// `OGD_HISTORY=on`: keep history (`as of`, `history`) and record commit times and authors.
    storage_history: bool,
    files_path: PathBuf,
    files_sync_root: Option<PathBuf>,
    files_sync_config_path: PathBuf,
    files_sync_index_path: PathBuf,
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
    node_identity_file: Option<PathBuf>,
    node_identity_password: Option<String>,
    node_capabilities: ServiceCapabilities,
    published_capabilities: ServiceCapabilities,
    #[cfg_attr(not(feature = "fabric"), allow(dead_code))]
    gateway_token: Option<String>,
}

impl Configuration {
    fn from_environment() -> Result<Self, DaemonError> {
        let bind_address = env::var("OGD_BIND").unwrap_or_else(|_| DEFAULT_BIND_ADDRESS.to_owned());
        let local_bind_address = env::var("OGD_LOCAL_BIND").ok().filter(|value| !value.trim().is_empty());
        let read_timeout = duration_from_environment("OGD_READ_TIMEOUT_MS", DEFAULT_READ_TIMEOUT_MS)?;
        let write_timeout = duration_from_environment("OGD_WRITE_TIMEOUT_MS", DEFAULT_WRITE_TIMEOUT_MS)?;
        let agent_provider_read_timeout = duration_from_environment(
            "OGD_AGENT_PROVIDER_READ_TIMEOUT_MS",
            DEFAULT_AGENT_PROVIDER_READ_TIMEOUT_MS,
        )?;
        let storage_backend = if build_profile::has_db_engine() {
            StorageBackend::parse(&env::var("OGD_STORAGE").unwrap_or_else(|_| DEFAULT_STORAGE_BACKEND.to_owned()))?
        } else {
            StorageBackend::Memory
        };
        let storage_path = PathBuf::from( env::var("OGD_STORAGE_PATH").unwrap_or_else(|_| DEFAULT_STORAGE_PATH.to_owned()), );
        let storage_history = env::var("OGD_HISTORY").is_ok_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "on" | "1" | "true" | "yes"));
        let gateway_endpoints = match env::var("OGD_GATEWAYS") {
            Ok(raw) => {
                let mut endpoints = Vec::new();
                for item in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
                    let endpoint = normalize_gateway_node_endpoint(item)
                        .ok_or_else(|| DaemonError::Runtime(format!("Invalid OGD_GATEWAYS endpoint: {item}")))?;
                    if !endpoints.contains(&endpoint) { endpoints.push(endpoint); }
                }
                endpoints
            }
            Err(env::VarError::NotPresent) => gateway_endpoint(&bind_address).into_iter().collect(),
            Err(source) => return Err(DaemonError::Environment { name: "OGD_GATEWAYS", source }),
        };
        let gateway_directory_path = env::var("OGD_GATEWAY_DIRECTORY_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                storage_path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."))
                    .join("sync/gateways.json")
            });
        let files_path = env::var("OGD_FILES_PATH").map(PathBuf::from).unwrap_or_else(|_| {
            storage_path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| Path::new(".")).join("files")
        });
        let files_sync_root = env::var("OGD_FILES_SYNC_ROOT")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        let sync_state_dir = storage_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .join("sync");
        let files_sync_config_path = env::var("OGD_FILES_SYNC_CONFIG_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| sync_state_dir.join("files.json"));
        let files_sync_index_path = env::var("OGD_FILES_SYNC_INDEX_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| sync_state_dir.join("files-index.json"));
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
        if build_profile::has_db_engine() {
            fs::create_dir_all(&backup_path).map_err(|source| {
                DaemonError::PrepareStorageDirectory {
                    path: backup_path.clone(),
                    source,
                }
            })?;
        }
        let instance_id = load_or_create_instance_id(&storage_path)?;
        let heartbeat_enabled = boolean_from_environment("OGD_HEARTBEAT_ENABLED", true)?;
        let heartbeat_interval = heartbeat_enabled .then(|| duration_from_environment("OGD_HEARTBEAT_INTERVAL_MS", 5_000)) .transpose()?;
        let authenticated_keepalive = boolean_from_environment("OGD_AUTH_KEEPALIVE_ENABLED", true)?;
        let node_identity_file = env::var("OGD_NODE_IDENTITY_FILE").ok().filter(|value| !value.trim().is_empty()).map(PathBuf::from);
        let node_identity_password = env::var("OGD_NODE_IDENTITY_PASSWORD").ok().filter(|value| !value.is_empty());
        let compiled_capabilities = ServiceCapabilities::compiled();
        let node_capabilities = match env::var("OGD_NODE_CAPABILITIES") {
            Ok(raw) => {
                let requested = ServiceCapabilities::from_names(
                    raw.split(',').map(str::trim).filter(|value| !value.is_empty())
                ).map_err(DaemonError::Runtime)?;
                let missing = requested.missing_from(compiled_capabilities);
                if !missing.is_empty() {
                    return Err(DaemonError::Runtime(format!(
                        "OGD_NODE_CAPABILITIES requests capabilities not compiled into this ogd build: {} (compiled: {})",
                        missing.names().join(","),
                        compiled_capabilities.names().join(","),
                    )));
                }
                requested
            }
            Err(env::VarError::NotPresent) => ServiceCapabilities::default_enabled(),
            Err(source) => return Err(DaemonError::Environment { name: "OGD_NODE_CAPABILITIES", source }),
        };
        let published_capabilities = match env::var("OGD_NODE_PUBLISHED_CAPABILITIES") {
            Ok(raw) => {
                let requested = ServiceCapabilities::from_names(
                    raw.split(',').map(str::trim).filter(|value| !value.is_empty())
                ).map_err(DaemonError::Runtime)?;
                let not_enabled = requested.missing_from(node_capabilities);
                if !not_enabled.is_empty() {
                    return Err(DaemonError::Runtime(format!(
                        "OGD_NODE_PUBLISHED_CAPABILITIES may only publish enabled capabilities; not enabled: {} (enabled: {})",
                        not_enabled.names().join(","),
                        node_capabilities.names().join(","),
                    )));
                }
                requested
            }
            Err(env::VarError::NotPresent) => node_capabilities,
            Err(source) => return Err(DaemonError::Environment { name: "OGD_NODE_PUBLISHED_CAPABILITIES", source }),
        };
        let gateway_token = env::var("OGD_GATEWAY_TOKEN").ok().filter(|value| !value.is_empty());

        Ok(Self {
            bind_address,
            local_bind_address,
            gateway_endpoints,
            gateway_directory_path,
            read_timeout,
            write_timeout,
            agent_provider_read_timeout,
            storage_backend,
            storage_path,
            storage_history,
            files_path,
            files_sync_root,
            files_sync_config_path,
            files_sync_index_path,
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
            node_identity_file,
            node_identity_password,
            node_capabilities,
            published_capabilities,
            gateway_token,
        })
    }
}

impl Configuration {
    fn bootstrap_password(&self) -> Result<Vec<u8>, DaemonError> {
        if let Some(path) = &self.bootstrap_password_file {
            let mut password = fs::read(path).map_err(DaemonError::BootstrapAdmin)?;
            while password.last().is_some_and(|byte| matches!(byte, b'\n' | b'\r')) {
                password.pop();
            }
            if !password.is_empty() {
                return Ok(password);
            }
        }

        Ok(self.bootstrap_password
            .as_ref()
            .filter(|value| !value.is_empty())
            .map(|value| value.as_bytes().to_vec())
            .unwrap_or_else(|| b"openglacierd".to_vec()))
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

#[cfg(feature = "auth")]
fn classic_password_hash(password: &str) -> Result<(String, String), String> {
    let mut salt = [0u8; 16];
    fs::File::open("/dev/urandom").and_then(|mut file| file.read_exact(&mut salt)).map_err(|error| error.to_string())?;
    let hash = classic_password_hash_with_salt(password, &salt)?;
    Ok((encode_base64(&salt), encode_base64(&hash)))
}

#[cfg(feature = "auth")]
fn classic_password_hash_with_salt(password: &str, salt: &[u8]) -> Result<[u8; 32], String> {
    let params = Params::new(CLASSIC_AUTH_MEMORY_KIB, CLASSIC_AUTH_ITERATIONS, CLASSIC_AUTH_LANES, Some(32)).map_err(|error| error.to_string())?;
    let argon = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut output = [0u8; 32];
    argon.hash_password_into(password.as_bytes(), salt, &mut output).map_err(|error| error.to_string())?;
    Ok(output)
}

#[cfg(feature = "auth")]
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
    agent_provider_read_timeout: Duration,
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
    file_sync: Mutex<FileSyncRuntime>,
    file_sync_run: Mutex<()>,
    file_sync_applying: Arc<AtomicBool>,
    file_sync_watcher_dirty: Arc<AtomicBool>,
    service_capabilities: ServiceCapabilities,
    published_capabilities: ServiceCapabilities,
    #[cfg(feature = "llm")]
    llm_service: Option<Arc<LlmService>>,
    #[cfg(feature = "agent")]
    agent_service: Option<Arc<AgentService>>,
    upstream: Option<String>,
    upstream_client_endpoints: Mutex<Vec<String>>,
    upstream_identity: Option<IdentityCredential>,
    gateway_state: Mutex<&'static str>,
    gateway_connections: Mutex<HashMap<String, bool>>,
    connection_admission: Arc<ConnectionAdmission>,
}

#[derive(Debug)]
enum QueryTextError { Parse(String), Planning(String), }

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
    Encode(crate::protocol::ProtocolError),
    Protocol(crate::protocol::ProtocolError),
}

impl Display for ConnectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigureSocket(error) => { write!(formatter, "cannot configure socket: {error}") }
            Self::CloneSocket(error) => { write!(formatter, "cannot clone socket: {error}") }
            Self::Read(error) => { write!(formatter, "cannot read request: {error}") }
            Self::Write(error) => { write!(formatter, "cannot write response: {error}") }
            Self::Encode(error) => { write!(formatter, "cannot encode response: {error}") }
            Self::Protocol(error) => { write!(formatter, "protocol error: {error}") }
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
pub enum DaemonError {
    BootstrapAdmin(io::Error),
    BootstrapIdentity(crate::access::identity_file::IdentityFileError),
    NodeIdentity(crate::access::identity_file::IdentityFileError),
    NodeIdentityMissing,
    NodeIdentityPasswordMissing,
    NodeDeviceCredentialState,
    NodeDeviceCredentialConflict { device_id: String },
    BootstrapPasswordMissing,
    BootstrapAdminState,
    BootstrapAppsState,
    BootstrapFabricResourcesState,
    Environment {name: &'static str, source: env::VarError},
    InvalidDuration {name: &'static str, value: String, source: std::num::ParseIntError},
    InvalidBoolean {name: &'static str, value: String},
    InvalidByteSize {name: &'static str, value: String},
    ZeroDuration {name: &'static str},
    InvalidStorageBackend {value: String},
    PrepareStorageDirectory {path: PathBuf, source: io::Error},
    OpenStorage {path: PathBuf, source: StorageError},
    Bind {address: String, source: io::Error},
    LocalAddress(io::Error),
    SpawnConnectionThread(io::Error),
    Runtime(String),
}

impl Display for DaemonError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::BootstrapAdmin(source) => { write!(formatter, "cannot create bootstrap administrator: {source}")}
            Self::BootstrapIdentity(source) => write!(formatter, "cannot create encrypted bootstrap identity: {source}"),
            Self::NodeIdentity(source) => write!(formatter, "cannot load node identity: {source}"),
            Self::NodeIdentityMissing => formatter.write_str("Gateway mode requires OGD_NODE_IDENTITY_FILE and OGD_NODE_IDENTITY_PASSWORD"),
            Self::NodeIdentityPasswordMissing => formatter.write_str("OGD_NODE_IDENTITY_FILE requires OGD_NODE_IDENTITY_PASSWORD"),
            Self::NodeDeviceCredentialState => formatter.write_str("cannot persist local node device credential"),
            Self::NodeDeviceCredentialConflict { device_id } => write!(formatter, "local device credential conflicts with configured node identity for device `{device_id}`"),
            Self::BootstrapPasswordMissing => formatter.write_str("bootstrap identity requires OGD_BOOTSTRAP_PASSWORD_FILE or OGD_BOOTSTRAP_PASSWORD"),
            Self::BootstrapAdminState => { formatter.write_str("cannot persist bootstrap administrator") }
            Self::BootstrapAppsState => formatter.write_str("cannot persist built-in Apps from apps.json"),
            Self::BootstrapFabricResourcesState => formatter.write_str("cannot persist bootstrap fabric resource defaults"),
            Self::Environment { name, source } => { write!(formatter, "cannot read {name}: {source}") }
            Self::InvalidDuration {name, value, source} => write!(formatter, "{name} must be a positive integer in milliseconds; got `{value}`: {source}"),
            Self::InvalidBoolean { name, value } => write!(formatter, "{name} must be one of 1/0, true/false, yes/no, on/off; got `{value}`"),
            Self::InvalidByteSize { name, value } => write!(formatter, "{name} must be a positive byte size such as 268435456, 256M or 2G; got `{value}`"),
            Self::ZeroDuration { name } => { write!(formatter, "{name} must be greater than zero")}
            Self::InvalidStorageBackend { value } => write!(formatter, "OGD_STORAGE must be `memory` or `glacier`; got `{value}`"),
            Self::PrepareStorageDirectory { path, source } => write!(formatter, "cannot create storage directory {}: {source}", path.display()),
            Self::OpenStorage { path, source } => write!(formatter, "cannot open storage at {}: {source}", path.display()),
            Self::Bind { address, source } => { write!(formatter, "cannot bind {address}: {source}")}
            Self::LocalAddress(source) => { write!(formatter, "cannot read listening address: {source}")}
            Self::SpawnConnectionThread(source) => { write!(formatter, "cannot spawn connection thread: {source}")}
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
            | Self::BootstrapFabricResourcesState
            | Self::InvalidBoolean { .. }
            | Self::InvalidByteSize { .. }
            | Self::ZeroDuration { .. }
            | Self::InvalidStorageBackend { .. }
            | Self::Runtime(_)
            | Self::NodeIdentityMissing
            | Self::NodeIdentityPasswordMissing
            | Self::NodeDeviceCredentialState
            | Self::NodeDeviceCredentialConflict { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::CollectionId;

    #[test] fn routing_errors_answer_the_request_and_keep_the_connection() { let listener = TcpListener::bind("127.0.0.1:0").unwrap(); let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap(); let (mut server, _) = listener.accept().unwrap(); write_response(&mut server, &routing_error(RequestId::Number(7), "handler domain and routed variant diverged")).unwrap(); write_response(&mut server, &QueryResponse::success(8, Vec::new(), None)).unwrap(); let mut read = || { let mut length = [0_u8; LENGTH_PREFIX_BYTES]; client.read_exact(&mut length).unwrap(); let mut payload = vec![0_u8; u32::from_be_bytes(length) as usize]; client.read_exact(&mut payload).unwrap(); crate::protocol::decode_stream_response(&payload).unwrap() }; let StreamResponse::Error { id, error, .. } = read() else { panic!("expected an error response") }; assert_eq!(id, Some(RequestId::Number(7))); assert_eq!(error.code, "operation.internal_error"); assert!(error.message.contains("diverged")); assert!(matches!(read(), StreamResponse::Complete { id: RequestId::Number(8), .. })); }

    fn public_places_test_engine() -> Engine {
        let engine = app_bootstrap_test_engine();
        for query in [
            "on _places | insert {placeId: \"private\", name: \"Private\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 1}",
            "on _places | insert {placeId: \"public-ro\", name: \"Public RO\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 2, publicAccess: \"readonly\"}",
            "on _places | insert {placeId: \"public-rw\", name: \"Public RW\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 3, publicAccess: \"readwrite\"}",
            "on _app_instances | insert {instanceId: \"i-public\", placeId: \"public-ro\", appId: \"app.public\", state: \"active\", createdAt: 1}",
            "on _app_instances | insert {instanceId: \"i-private\", placeId: \"private\", appId: \"app.private\", state: \"active\", createdAt: 2}",
        ] {
            assert!(execute_request(&engine, QueryRequest::new(RequestId::Number(1), query)).is_ok(), "{query}");
        }
        engine
    }

    fn identity(identity_id: &str) -> Principal {
        Principal::Identity { identity_id: identity_id.to_owned(), device_id: format!("{identity_id}-device") }
    }

    #[test] fn public_policy_grants_access_without_translating_it_into_a_role() { let engine = public_places_test_engine(); let access = |principal: &Principal, place_id: &str| { let place = load_place(&engine, RequestId::Number(1), place_id).expect("place loads"); resolve_place_access_for_principal(&engine, RequestId::Number(1), principal, &place) }; assert_eq!(access(&Principal::Anonymous, "public-ro").ok(), Some(PlaceAccess::Public(PublicAccess::Readonly))); assert_eq!(access(&Principal::Anonymous, "public-rw").ok(), Some(PlaceAccess::Public(PublicAccess::Readwrite))); assert!(access(&Principal::Anonymous, "private").is_err()); assert_eq!(access(&identity("stranger"), "public-rw").ok(), Some(PlaceAccess::Public(PublicAccess::Readwrite))); assert!(access(&identity("stranger"), "private").is_err()); assert_eq!(access(&identity("owner"), "public-ro").ok(), Some(PlaceAccess::Role(PlaceRole::Owner))); }

    #[test] fn apps_are_open_only_to_their_roles() { let engine = public_places_test_engine(); for query in ["on _places | insert {placeId: \"p\", name: \"P\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 4}", "on _apps | insert {appId: \"app.fin\", state: \"active\", definition: {access: {visibleTo: [\"resident\"]}}, maintainersJson: \"[\\\"maint\\\"]\"}", "on _app_instances | insert {instanceId: \"i-fin\", placeId: \"p\", appId: \"app.fin\", state: \"active\", createdAt: 3}", "on _app_instances | insert {instanceId: \"i-cfg\", placeId: \"p\", appId: \"app.fin\", config: {visibleTo: [\"member\"]}, state: \"active\", createdAt: 4}", "on _app_instances | insert {instanceId: \"i-open\", placeId: \"p\", appId: \"app.open\", state: \"active\", createdAt: 5}"].map(str::to_owned).into_iter().chain([("res", "resident"), ("mem", "member"), ("maint", "member")].map(|(target, role)| format!("on _sharings | insert {{sharingId: \"place-access:p:{target}\", owner: \"owner\", target: \"{target}\", placeRole: \"{role}\", state: \"active\"}}"))) { assert!(execute_request(&engine, QueryRequest::new(RequestId::Number(1), query.clone())).is_ok(), "{query}"); } let open = |identity_id: &str, instance: &str| resolve_query_execution_context(&engine, RequestId::Number(1), &identity(identity_id), RequestedExecutionContext { place_id: "p".to_owned(), app_instance_id: Some(instance.to_owned()) }).is_ok(); assert_eq!(["owner", "res", "mem", "maint"].map(|who| (open(who, "i-fin"), open(who, "i-cfg"), open(who, "i-open"))), [(true, true, true), (true, false, true), (false, true, true), (true, true, true)]); }
    #[test] fn place_list_returns_scope_plus_public_places() { let engine = public_places_test_engine(); let ids = |places: Vec<JsonValue>| places.iter().filter_map(|place| place.str_field("placeId").map(str::to_owned)).collect::<Vec<_>>(); assert_eq!(ids(list_public_places(&engine, RequestId::Number(1)).unwrap()), vec!["public-ro", "public-rw"]); let stranger = list_places_for_identity_and_public(&engine, RequestId::Number(1), "stranger").unwrap(); assert_eq!(ids(stranger.clone()), vec!["public-ro", "public-rw"]); assert!(stranger.iter().all(|place| place.get("role").is_some_and(JsonValue::is_null))); let owner = list_places_for_identity_and_public(&engine, RequestId::Number(1), "owner").unwrap(); assert_eq!(ids(owner.clone()), vec!["private", "public-ro", "public-rw"]); assert!(owner.iter().all(|place| place.str_field("role") == Some("owner"))); }

    #[test] fn public_app_definitions_are_limited_to_public_places() { let engine = public_places_test_engine(); let app_ids = public_place_app_ids(&engine, RequestId::Number(1)).unwrap(); assert!(app_ids.contains("app.public")); assert!(!app_ids.contains("app.private")); }

    #[test] fn files_context_keeps_public_read_only_places_read_only() { let engine = public_places_test_engine(); { let (place_id, instance_id) = ("public-ro", "i-public"); assert!(resolve_file_context(&engine, RequestId::Number(1), &Principal::Anonymous, place_id, instance_id, false).is_ok()); assert!(resolve_file_context(&engine, RequestId::Number(1), &Principal::Anonymous, place_id, instance_id, true).is_err()); } }

    fn app_bootstrap_test_engine() -> Engine {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        Engine::new(storage)
    }

    #[cfg(feature = "fabric")] #[test] fn gateway_endpoint_normalization_supports_seeds_and_discovery_hints() { assert_eq!( normalize_gateway_node_endpoint("gw+insecure://127.0.0.1:3000").as_deref(), Some("ws://127.0.0.1:3000/node") ); assert_eq!( normalize_gateway_node_endpoint("gw://example.test").as_deref(), Some("wss://example.test/node") ); assert_eq!( normalize_gateway_node_endpoint("ws://127.0.0.1:3001").as_deref(), Some("ws://127.0.0.1:3001/node") ); assert_eq!( normalize_gateway_node_endpoint("ws://127.0.0.1:3002/peer").as_deref(), Some("ws://127.0.0.1:3002/node") ); assert_eq!( gateway_client_endpoint_from_node("ws://127.0.0.1:3000/node").as_deref(), Some("ws://127.0.0.1:3000/ws") ); }
    #[test] fn node_identity_proof_is_bound_to_gateway_challenge_and_device() { let proof = node_identity_proof("nonce-a", "device-a", "identity-a", "device-a"); assert_eq!(proof, "og.node.challenge.v1\nnonce-a\ndevice-a\nidentity-a\ndevice-a"); assert_ne!(proof, node_identity_proof("nonce-b", "device-a", "identity-a", "device-a")); assert_ne!(proof, node_identity_proof("nonce-a", "device-b", "identity-a", "device-b")); }
    #[test] fn fabric_device_verification_requires_known_active_exact_key() { let engine = app_bootstrap_test_engine(); let inserted = execute_request(&engine, QueryRequest::new(1, r#"on _devices | insert {identityId: "identity-a", deviceId: "device-a", publicKey: "key-a", state: "active"}"#)); assert!(inserted.is_ok()); assert!(registered_device_matches(&engine, "identity-a", "device-a", "key-a")); assert!(!registered_device_matches(&engine, "identity-a", "device-a", "key-b")); assert!(!registered_device_matches(&engine, "identity-a", "device-unknown", "key-a")); let revoked = execute_request(&engine, QueryRequest::new(2, r#"on _devices | where deviceId == "device-a" | set state = "revoked""#)); assert!(revoked.is_ok()); assert!(!registered_device_matches(&engine, "identity-a", "device-a", "key-a")); }
    #[test] fn gateway_device_registration_is_idempotent_and_exact() { let engine = app_bootstrap_test_engine(); ensure_gateway_device_registration(&engine, "gateway-identity", "gateway-device", "gateway-key") .expect("Gateway enrollment persists"); ensure_gateway_device_registration(&engine, "gateway-identity", "gateway-device", "gateway-key") .expect("Gateway enrollment is idempotent"); assert!(registered_identity_matches(&engine, "gateway-identity", "gateway-key")); assert!(registered_device_matches(&engine, "gateway-identity", "gateway-device", "gateway-key")); assert!(!registered_device_matches(&engine, "gateway-identity", "gateway-device", "other-key")); }
    #[cfg(feature = "fabric")] #[test] fn gateway_enrollment_hmac_matches_sha256_reference_vector() { assert_eq!( hmac_sha256_base64(b"key", b"The quick brown fox jumps over the lazy dog"), "97yD9DBThCSxMpjmqm+xQ+9NWaFJRhdZl0edvC0aPNg=" ); }
    #[test] fn app_bootstrap_with_empty_manifest_is_a_noop() { let engine = app_bootstrap_test_engine(); bootstrap_apps(&engine, &[]).expect("empty App manifest bootstraps"); assert!(!app_record_exists(&engine, RequestId::Number(1), "system.files") .expect("App lookup succeeds")); }
    #[test] fn app_bootstrap_reconciles_managed_apps_without_touching_unmanaged_apps() { let engine = app_bootstrap_test_engine(); let v1 = BuiltinApp { app_id: "system.test".to_owned(), name: "System test".to_owned(), version: "1.0.0".to_owned(), definition: serde_json::json!({ "id": "system.test", "name": "System test", "version": "1.0.0" }), }; bootstrap_apps(&engine, std::slice::from_ref(&v1)).expect("initial App bootstrap succeeds"); let drifted = execute_request( &engine, QueryRequest::new( 2, r#"on _apps | where appId == "system.test" | set name = "Drifted", version = "0.9.0", state = "deleted""#, ), ); assert!(drifted.is_ok()); let custom = execute_request( &engine, QueryRequest::new( 3, r#"on _apps | insert {appId: "custom.test", name: "Custom", version: "7.0.0", definition: {id: "custom.test", name: "Custom", version: "7.0.0"}, createdBy: "user", state: "active", createdAt: 1}"#, ), ); assert!(custom.is_ok()); let v2 = BuiltinApp { app_id: "system.test".to_owned(), name: "System test v2".to_owned(), version: "2.0.0".to_owned(), definition: serde_json::json!({ "id": "system.test", "name": "System test v2", "version": "2.0.0" }), }; bootstrap_apps(&engine, std::slice::from_ref(&v2)).expect("App reconciliation succeeds"); let response = execute_request( &engine, QueryRequest::new(4, r#"on _apps | where appId == "system.test" | limit 1"#), ); let QueryResponse::Ok { documents, .. } = response else { panic!("managed App lookup must succeed"); }; let managed = documents.first().expect("managed App exists"); assert_eq!(managed.str_field("state"), Some("active")); assert_eq!(managed.str_field("name"), Some("System test v2")); assert_eq!(managed.str_field("version"), Some("2.0.0")); assert_eq!(managed.str_field("updatedBy"), Some("system")); assert_eq!(managed.get("definition"), Some(&v2.definition)); let response = execute_request( &engine, QueryRequest::new(5, r#"on _apps | where appId == "custom.test" | limit 1"#), ); let QueryResponse::Ok { documents, .. } = response else { panic!("custom App lookup must succeed"); }; let unmanaged = documents.first().expect("custom App exists"); assert_eq!(unmanaged.str_field("state"), Some("active")); assert_eq!(unmanaged.str_field("name"), Some("Custom")); assert_eq!(unmanaged.str_field("version"), Some("7.0.0")); assert_eq!(unmanaged.str_field("createdBy"), Some("user")); assert!(unmanaged.get("updatedBy").is_none()); }
    #[test] fn place_scoped_collection_listing_filters_and_counts_by_place() { let engine = app_bootstrap_test_engine(); for (id, collection, place_id) in [ (20, "shared_items", "place-a"), (21, "shared_items", "place-a"), (22, "shared_items", "place-b"), (23, "other_items", "place-b"), ] { let response = execute_request( &engine, QueryRequest::new( id, quoted!("on {collection} | insert {{_place: {}, name: \"item\"}}", place_id), ), ); assert!(response.is_ok()); } let snapshot = engine.storage().read().expect("snapshot opens"); let collections = list_storage_collections(snapshot.as_ref(), true, Some("place-a"), None) .expect("Place collections list succeeds"); assert_eq!(collections.len(), 1); assert_eq!(collections[0].str_field("name"), Some("shared_items")); assert_eq!(collections[0].u64_field("documents"), Some(2)); }
    #[test] fn fabric_imports_run_on_the_database_core_under_the_gateway_delegation() { let engine = app_bootstrap_test_engine(); bootstrap_apps_if_needed(&engine).expect("built-in Apps bootstrap"); let declared = serde_json::json!({"checks": [{"id": "big", "message": "big", "query": "on l.staged | where import_id == $import and v > 5"}], "replace": ["k"], "after": ["on l.staged | where import_id == $import | select k | into l_log"]}); for query in ["on _places | insert {placeId: \"p\", name: \"P\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 1}".to_owned(), insert_query("_apps", serde_json::json!({"appId": "app.l", "state": "active", "definition": {"model": {"tables": {"t": {"collection": "l", "import": declared}}, "collections": {"l": "l"}}}})), "on _app_instances | insert {instanceId: \"i\", placeId: \"p\", appId: \"app.l\", state: \"active\", createdAt: 2}".to_owned(), "on l | insert {_place: \"p\", _app_instance: \"i\", k: 1, v: 0}".to_owned()] { query_run(&engine, 1, query).unwrap(); } let rows = serde_json::json!({"documents": [{"k": 1, "v": 9}, {"k": 2, "v": 1}]}); let call = |delegation: Option<&GatewayDelegation>, mut input: serde_json::Value| { for (key, value) in [("placeId", "p"), ("targetInstanceId", "i"), ("table", "t")] { input[key] = value.into(); } super::place_ops::data_import(&engine, &identity("owner"), delegation, RequestId::Number(2), serde_json::from_value(input).unwrap(), || unreachable!()).map_err(|stop| match stop { super::serve::Stop::Reply(response) => format!("{response:?}"), super::serve::Stop::Connection(_) => String::new() }) }; let plan = call(None, serde_json::json!({"workerResult": rows, "planOnly": true})).unwrap(); assert_eq!((plan["status"].clone(), plan["collection"].clone(), plan["rows"].clone(), plan["import"].clone()), (serde_json::json!("planned"), serde_json::json!("l"), serde_json::json!(2), declared)); let planned = serde_json::json!({"workerResult": rows, "plan": plan}); assert!(call(None, planned.clone()).unwrap_err().contains("authorization.denied")); let delegation = |capability: &str, role| GatewayDelegation { principal: identity("gateway"), place_id: Some("p".into()), capability: capability.into(), app_instance_id: Some("i".into()), place_role: Some(role), public_access: None, token: None }; for refused in [delegation("files", PlaceRole::Owner), delegation("database", PlaceRole::Member)] { assert!(call(Some(&refused), planned.clone()).unwrap_err().contains("authorization.denied")); } let gateway = delegation("database", PlaceRole::Owner); let first = call(Some(&gateway), planned).unwrap(); assert_eq!((first["status"].clone(), first["checks"][0]["rows"].as_array().unwrap().len()), (serde_json::json!("confirm"), 1)); let done = call(Some(&gateway), serde_json::json!({"importId": first["importId"], "accept": ["big"], "plan": plan})).unwrap(); let rows = |query: &str| query_documents(&engine, 3, query).unwrap(); assert_eq!((done["status"].clone(), rows("on l | sort k | select k, v"), rows("on l_log | sort k | select k"), rows("on l.staged | count as n")), (serde_json::json!("completed"), vec![serde_json::json!({"k": 1, "v": 9}), serde_json::json!({"k": 2, "v": 1})], vec![serde_json::json!({"k": 1}), serde_json::json!({"k": 2})], vec![serde_json::json!({"n": 0})])); }
    #[test] fn place_only_query_scope_spans_instances_without_leaking_other_places() { let engine = app_bootstrap_test_engine(); let context = |place_id: &str, app_instance_id: Option<&str>| ExecutionContext { principal: Principal::Anonymous, place_id: place_id.to_owned(), app_instance_id: app_instance_id.map(str::to_owned), access: PlaceAccess::Role(PlaceRole::Owner), }; for (id, place_id, app_instance_id, name) in [ (10, "place-a", "app-1", "one"), (11, "place-a", "app-2", "two"), (12, "place-b", "app-3", "other"), ] { let response = execute_request_scoped( &engine, QueryRequest::new(id, quoted!("on studio_scope_test | insert {{name: {}}}", name)), Some(&context(place_id, Some(app_instance_id))), ); assert!(response.is_ok()); } let place_response = execute_request_scoped( &engine, QueryRequest::new(13, "on studio_scope_test | sort name"), Some(&context("place-a", None)), ); let QueryResponse::Ok { documents, .. } = place_response else { panic!("Place scoped query must succeed"); }; assert_eq!(documents.len(), 2); assert_eq!(documents[0].str_field("name"), Some("one")); assert_eq!(documents[1].str_field("name"), Some("two")); assert!(documents.iter().all(|document| document.str_field("_place") == Some("place-a"))); let app_response = execute_request_scoped( &engine, QueryRequest::new(14, "on studio_scope_test | sort name"), Some(&context("place-a", Some("app-1"))), ); let QueryResponse::Ok { documents, .. } = app_response else { panic!("AppInstance scoped query must succeed"); }; assert_eq!(documents.len(), 1); assert_eq!(documents[0].str_field("name"), Some("one")); }
    #[test] fn file_sync_place_root_projection_is_definition_driven() { let declared = serde_json::json!({ "appId": "custom.files", "definition": { "id": "custom.files", "kind": "custom.anything", "files": {"projection": "place-root"} } }); assert!(file_sync_app_place_root_projection(&declared)); let serialized = serde_json::json!({ "definition": r#"{"files":{"projection":"place-root"}}"# }); assert!(file_sync_app_place_root_projection(&serialized)); let legacy_identity_only = serde_json::json!({ "appId": "system.files", "definition": {"kind": "system.files"} }); assert!(!file_sync_app_place_root_projection(&legacy_identity_only)); assert!(!file_sync_app_place_root_projection(&serde_json::json!({ "definition": {"files": {"projection": "app"}} }))); }
    #[test] fn borrowed_partial_document_encodes_large_js_safe_integers_as_numbers() { let document = Document::from_fields([ ("created_at", Value::from(1_785_680_802_608_u64)), ("small", Value::from(42_u64)), ("negative", Value::from(-5_000_000_000_i64)), ]); let response = BorrowedPlainDocumentResponse { kind: "response", status: "partial", version: PROTOCOL_VERSION, id: RequestId::string("query-1").unwrap(), data: BorrowedDocument(&document), }; let payload = rmp_serde::to_vec_named(&response).unwrap(); let decoded: JsonValue = rmp_serde::from_slice(&payload).unwrap(); assert!(decoded["data"]["created_at"].is_f64()); assert!(decoded["data"]["negative"].is_f64()); assert!(decoded["data"]["small"].is_u64()); }
    #[test] fn enrollment_grants_event_subscription_permission() { let query = enrollment_events_permission_query("identity-a", 42); let document: JsonValue = serde_json::from_str(query.strip_prefix("on _permissions | insert ").expect("an insert into _permissions")).unwrap(); assert_eq!( document, serde_json::json!({"identityId": "identity-a", "action": "events.subscribe", "resource": "*", "effect": "allow", "state": "active", "createdAt": 42}) ); }
    #[test] fn wildcard_permission_authorizes_events_subscribe() { let storage = MemoryStorage::new(); let storage: Arc<dyn StorageEngine> = Arc::new(storage); let engine = Engine::new(storage); let insert = execute_request( &engine, QueryRequest::new( 1, r#"on _permissions | insert {identityId: "identity-a", action: "*", resource: "*", effect: "allow", state: "active"}"#, ), ); assert!(insert.is_ok()); let request = AuthorizationRequest { identity_id: "identity-a".to_owned(), action: AuthorizationAction::EventsSubscribe, resource: "*".to_owned(), }; assert!(permission_exists(&engine, &request)); }
    #[test] fn execute_request_uses_the_planner_cache() { let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new()); let engine = Engine::new(storage); let query = "from users"; let first = execute_request(&engine, QueryRequest::new(1, query)); assert!(matches!(first, QueryResponse::Ok { .. })); assert_eq!(engine.planner_cache_stats().misses, 1); assert_eq!(engine.planner_cache_stats().hits, 0); let second = execute_request(&engine, QueryRequest::new(2, query)); assert!(matches!(second, QueryResponse::Ok { .. })); assert_eq!(engine.planner_cache_stats().misses, 1); assert_eq!(engine.planner_cache_stats().hits, 1); }
    #[test] fn raw_query_results_include_document_id() { let storage = MemoryStorage::new(); let collection = CollectionId::parse("users").unwrap(); let id = crate::storage::UuidV7Generator::new().next_id(); let document = Arc::new(Document::from_fields([("name", Value::string("Alice"))])); let mut transaction = storage.begin().unwrap(); transaction.insert(&collection, id, document).unwrap(); transaction.commit().unwrap(); let storage: Arc<dyn StorageEngine> = Arc::new(storage); let engine = Engine::new(storage); let response = execute_request(&engine, QueryRequest::new(1, "from users")); let QueryResponse::Ok { documents, .. } = response else { panic!("query must succeed"); }; assert_eq!(documents.len(), 1); assert_eq!( documents[0].get("_id"), Some(&JsonValue::String(id.to_string())) ); assert_eq!( documents[0].get("name"), Some(&JsonValue::String("Alice".to_owned())) ); }
    #[test] fn transformed_plans_do_not_expose_document_id() { let source = crate::query::PhysicalSource::collection_scan(CollectionId::parse("users").unwrap()); let plan = crate::query::PhysicalPlan::new( source, [crate::query::PhysicalOperator::select([ crate::query::ExpressionFieldPath::new(["name"]).unwrap(), ]) .unwrap()], ) .unwrap(); assert!(!exposes_document_id(&plan)); }
    #[test] fn parses_flexible_memory_limit_values() { let mib = 1024 * 1024; let gib = 1024 * mib; assert_eq!(parse_byte_size("TEST", "512M").unwrap(), 512 * mib); assert_eq!(parse_byte_size("TEST", "850m").unwrap(), 850 * mib); assert_eq!(parse_byte_size("TEST", "1G").unwrap(), gib); assert_eq!(parse_byte_size("TEST", "2GiB").unwrap(), 2 * gib); assert_eq!(parse_byte_size("TEST", "1.5G").unwrap(), 1536 * mib); assert_eq!(parse_byte_size("TEST", "512").unwrap(), 512 * mib); assert_eq!(parse_byte_size("TEST", "0.5G").unwrap(), 512 * mib); assert_eq!(parse_byte_size("TEST", "1024KiB").unwrap(), mib); }
    #[test] fn rejects_invalid_memory_limit_values() { for value in ["", "0", ".", "1.2.3G", "12T", "-1G"] { assert!(parse_byte_size("TEST", value).is_err(), "{value}"); } }
    #[test] fn parses_supported_storage_backends() { assert_eq!( StorageBackend::parse("memory").expect("memory"), StorageBackend::Memory ); assert_eq!( StorageBackend::parse("glacier").expect("glacier"), StorageBackend::Glacier ); assert_eq!( StorageBackend::parse(" GLACIER ").expect("normalized glacier"), StorageBackend::Glacier ); assert_eq!(StorageBackend::Memory.as_str(), "memory"); assert_eq!(StorageBackend::Glacier.as_str(), "glacier"); assert!(StorageBackend::parse("glacier6").is_err()); assert!(StorageBackend::parse("redb").is_err()); }
    #[test] fn rejects_unknown_storage_backend() { let error = StorageBackend::parse("sqlite").expect_err("must reject unknown backend"); assert!(matches!( error, DaemonError::InvalidStorageBackend { value } if value == "sqlite" )); }
    #[test] fn adapts_official_ast_to_planner_pipeline() { let source = "from users | where active == true | inspect verbose"; let pipeline = parse_pipeline(source).expect("pipeline parses"); assert_eq!(pipeline.source(), "users"); assert_eq!(pipeline.len(), 2); assert_eq!(pipeline.stages()[0].name().as_str(), "where"); assert_eq!(pipeline.stages()[0].arguments(), "active == true"); assert_eq!(pipeline.stages()[1].name().as_str(), "inspect"); assert_eq!(pipeline.stages()[1].arguments(), "verbose"); }
    #[test] fn preserves_qualified_collection_name() { let pipeline = parse_pipeline("from tenant.analytics.events | where active == true") .expect("pipeline parses"); assert_eq!(pipeline.source(), "tenant.analytics.events"); }
    #[test] fn preserves_stage_spans_from_official_ast() { let source = "from users | where active == true"; let pipeline = parse_pipeline(source).expect("pipeline parses"); let stage = &pipeline.stages()[0]; assert_eq!(stage.span(), crate::query::Span::new(11, source.len())); }
    #[test] fn rejects_query_without_source() { let error = parse_pipeline("where active == true").expect_err("must fail"); assert!(matches!(error, QueryTextError::Parse(_))); }
    #[test] fn rejects_empty_pipeline_segments() { let error = parse_pipeline("from users || where active").expect_err("must fail"); assert!(matches!(error, QueryTextError::Parse(_))); }
    #[test] fn accepts_identifier_stage_for_planner_validation() { let pipeline = parse_pipeline("from users | limit 10").expect("syntax parses"); assert_eq!(pipeline.len(), 1); assert_eq!(pipeline.stages()[0].name().as_str(), "limit"); assert_eq!(pipeline.stages()[0].arguments(), "10"); }
    #[test] fn preserves_compound_stage_subpipelines() { let source = "from sales | pivot | rows region | columns month | values amount | aggregate sum | end"; let pipeline = parse_pipeline(source).expect("compound pipeline parses"); let stage = &pipeline.stages()[0]; assert_eq!(stage.name().as_str(), "pivot"); assert!(stage.is_compound()); assert_eq!(stage.subpipeline().expect("pivot body").len(), 4); }
    #[test] fn authenticated_keepalive_filter_is_added_once() { let mut types = vec!["sharing.*".to_owned()]; ensure_authenticated_keepalive_type(&mut types); ensure_authenticated_keepalive_type(&mut types); assert_eq!( types, vec!["sharing.*".to_owned(), "core.heartbeat".to_owned()] ); }
    #[test] fn authenticated_keepalive_filter_respects_wildcards() { for mut types in [ vec!["*".to_owned()], vec!["core.*".to_owned()], vec!["core.heartbeat".to_owned()], ] { ensure_authenticated_keepalive_type(&mut types); assert_eq!(types.len(), 1); } }
    #[test] fn auth_is_fabric_level_not_place_assignable() { assert!(valid_resource_capability("auth")); assert!(!place_resource_capability_allowed("auth")); assert!(place_resource_capability_allowed("files")); assert!(place_resource_capability_allowed("custom.capability")); }
    #[test] fn fabric_default_store_is_created_and_updated_explicitly() { let engine = app_bootstrap_test_engine(); let first = vec![serde_json::json!({ "identityId": "identity-a", "deviceId": "device-a", "capability": "database", "role": "provider" })]; save_fabric_defaults(&engine, 1.into(), &first).expect("first fabric default persists"); assert_eq!(load_fabric_defaults(&engine, 2.into()).unwrap(), first); let second = vec![serde_json::json!({ "identityId": "identity-b", "deviceId": "device-b", "capability": "database", "role": "provider" })]; save_fabric_defaults(&engine, 3.into(), &second).expect("fabric default updates"); assert_eq!(load_fabric_defaults(&engine, 4.into()).unwrap(), second); }
    #[test] fn shared_resource_assignment_logic_keeps_files_axes_orthogonal() { let assignments = upsert_resource_assignment( Vec::new(), "admin", "identity-a", "device-a", "files", None, Some("primary"), Some("provider"), ).expect("files default is valid"); assert_eq!(assignments.len(), 1); assert_eq!(resource_assignment_service_role(&assignments[0]), Some("primary")); assert_eq!(resource_assignment_storage_role(&assignments[0]), Some("provider")); let updated = upsert_resource_assignment( assignments, "admin", "identity-a", "device-a", "files", None, Some("replica"), None, ).expect("service role can change without erasing storage role"); assert_eq!(resource_assignment_service_role(&updated[0]), Some("replica")); assert_eq!(resource_assignment_storage_role(&updated[0]), Some("provider")); }
}
