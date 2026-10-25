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
#[cfg(feature = "auth")]
mod policy_ops;
#[cfg(feature = "auth")]
mod usage_ops;
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
    helpers::{decode_base64, document_to_json, elapsed_micros, encode_base64, env_text, env_var, flag, fnv1a64_continue, insert_query, query_string, quoted, JsonFields, lock_unpoisoned, system_time_millis, unix_time_millis, write_file_atomic, APP_INSTANCE_SCOPE_FIELD, FNV1A64_OFFSET, PLACE_SCOPE_FIELD},
    engine::{results::{Lookup, Rows}, Engine}, files::{FileEntry, FileId, FileStore, FileStoreEntry, FileStoreError, FileSyncConfig, FileSyncEntryState, FileSyncIndex, FileSyncIndexEntry, FileSyncSelectionMode, FileWrite, NativeFileStore, StoreId, APP_FILES_DIRECTORY, PRIMARY_APPS_COLLISION_NAME, file_sync_projection_component, file_sync_projection_suffix}, Principal, backup,
    event_engine::{EventEngine, EventSubscription},
    memory::{MemoryClass, MemoryGovernor, MemoryProfileConfig, WorkloadClass},
    operation::{
        decode_operation_request, AccessPolicy, ExecutionMode, ServiceCapability, ServiceCapabilities, AppCreateInput, AuthBeginInput, AuthEnrollBeginInput, ClassicAuthLoginInput, ClassicAuthRegisterInput, ClassicAuthResetRequestInput, ClassicAuthResetCompleteInput, email_address_valid, MailSendInput, ChallengeSignatureInput, DeviceRegisterInput, DeviceRenameInput, DeviceRevokeInput, AuthMachineInput, MachineAccessCreateInput, MachineAccessListInput, MachineAccessRevokeInput, MachineAccessReportInput, IdentityOpenInput, IdentityRegisterInput, IdentityRenewInput, PasswordInput, QueryExecuteInput, QueryContextResolveInput, AppDeleteInput, AppIdInput,
        AppInstanceCreateInput, AppInstanceRemoveInput, AppUpdateInput, DataAnalyzeInput, DataImportInput, DataMappingSaveInput, DataMappingListInput, DataMappingUpdateInput, DataMappingDeleteInput, Audience, BackupCreateInput, BackupNameInput,
        CollectionsListInput,
        BackupRestoreInput, HandlerKind, OperationKind, OperationRequest, OperationResponse, OperationRouter, OperationScope, PermissionGrantInput,
        PermissionRevokeInput, PlaceAccessRemoveInput, PlaceAccessSetInput, PlaceCreateInput, PlacePublicSetInput,
        PlaceDeleteInput, PlaceIdInput, PlaceSearchInput, PlaceUpdateInput, AppLayoutEntry, PlaceProfile, PlaceResourceSetInput, PlaceResourceRemoveInput, FabricResourceSetInput, FabricResourceRemoveInput, Routed, RoutedOperation, SharingCreateInput,
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
#[cfg(feature = "mailsend")]
use crate::service::mail::MailService;
#[cfg(feature = "agent")]
use crate::service::agent::{AgentCapabilityInvoker, AgentCapabilityResponse, AgentError, AgentService};

#[cfg(feature = "auth")]
use argon2::{Algorithm, Argon2, Params, Version};
#[cfg(any(feature = "fabric", feature = "identity"))]
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
const DEFAULT_QUERY_WAIT_MS: u64 = 60_000;
const DEFAULT_AGENT_PROVIDER_READ_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_STORAGE_BACKEND: &str = "memory";
const DEFAULT_STORAGE_PATH: &str = "data/ogd.glacier";
const DEFAULT_FILES_STORE_ID: &str = "native";
const DEFAULT_ENROLLMENT_MODE: &str = "open";
const CLASSIC_AUTH_MIN_PASSWORD_BYTES: usize = 12;
/// A password reset code lives 30 minutes, allows 5 tries, and one mail per address goes out a minute at most.
const CLASSIC_RESET_TTL_MS: u64 = 30 * 60 * 1000;
const CLASSIC_RESET_MAX_ATTEMPTS: u64 = 5;
const CLASSIC_RESET_MAIL_INTERVAL_MS: u64 = 60 * 1000;
const CLASSIC_AUTH_MEMORY_KIB: u32 = 64 * 1024;
const CLASSIC_AUTH_ITERATIONS: u32 = 3;
const CLASSIC_AUTH_LANES: u32 = 1;
/// The public base Apps, built in: the node uses them when its installed set (`OGD_APPS_PATH`) cannot be read.
const EMBEDDED_APPS_JSON: &str = include_str!("../../apps_public.json");
/// Where the packages install the base Apps (`openglacier-core-apps`, or a set that replaces it).
const DEFAULT_APPS_PATH: &str = "/usr/share/openglacier/apps/apps.json";
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
        let mut generation = lock_unpoisoned(&self.generation);
        *generation = (*generation).wrapping_add(1);
        self.changed.notify_one();
    }

    fn wait(&self, observed: &mut u64, timeout: Option<Duration>) {
        let mut generation = lock_unpoisoned(&self.generation);
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
        bootstrap_apps_if_needed(engine, &configuration.apps_path)?;
        #[cfg(feature = "auth")]
        if configuration.node_capabilities.contains(ServiceCapability::Auth) {
            usage_ops::start_usage_worker(Arc::clone(engine));
        }
    }
    let operation_router = Arc::new(OperationRouter::for_capabilities(configuration.node_capabilities));
    let event_engine = Arc::new(EventEngine::default());
    let _heartbeat = if configuration.node_capabilities.contains(ServiceCapability::Events) {
        configuration.heartbeat_interval.map(|interval| event_engine.start_heartbeat(interval))
    } else {
        None
    };
    // A redefined view: the materialized views kept in a cache that read it are computed again.
    if let Some(engine) = engine.as_ref() {
        crate::view::start_refresher(Arc::clone(engine));
    }
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
    #[cfg(feature = "mailsend")]
    let mail_service = if configuration.node_capabilities.contains(ServiceCapability::Mail) {
        Some(Arc::new(MailService::from_environment().map_err(|error| {
            DaemonError::Runtime(format!("unable to initialize mail provider: {error}"))
        })?))
    } else {
        None
    };
    let query_trace = configuration.query_trace.as_ref().map(|path| std::fs::OpenOptions::new().create(true).append(true).open(path).map(Mutex::new).map_err(|error| DaemonError::Runtime(format!("OGD_QUERY_TRACE {}: {error}", path.display())))).transpose()?;
    let connection_settings = Arc::new(ConnectionSettings {
        query_trace,
        read_timeout: configuration.read_timeout,
        write_timeout: configuration.write_timeout,
        agent_provider_read_timeout: configuration.agent_provider_read_timeout,
        import_metrics: configuration.import_metrics,
        authorization_mode: configuration.authorization_mode,
        enrollment_mode: configuration.enrollment_mode.clone(),
        classic_auth_enabled: configuration.classic_auth_enabled,
        allow_public: configuration.allow_public,
        classic_auth_email: configuration.classic_auth_email,
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
        #[cfg(feature = "mailsend")]
        mail_service,
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
            "memory limit {}, profile {}, runtime reserve {}, managed budget {}, planner cache {}, result cache {}, operation budget {}, query budget {}, import budget {}, query slots {} (light {}), query wait {} ms{}{}",
            format_bytes(configuration.memory_profile.process_limit_bytes),
            configuration.memory_profile.effective_profile_label(),
            format_bytes(Some(configuration.memory_profile.runtime_reserve_bytes)),
            format_bytes(configuration.memory_profile.managed_budget_bytes),
            format_bytes(Some(configuration.memory_profile.planner_cache_bytes)),
            if configuration.result_cache { format_bytes(Some(configuration.memory_profile.result_cache_bytes)) } else { "off".to_owned() },
            format_bytes(Some(configuration.memory_profile.operation_budget_bytes)),
            format_bytes(Some(configuration.memory_profile.query_budget_bytes)),
            format_bytes(Some(configuration.memory_profile.import_budget_bytes)),
            configuration.query_slots,
            configuration.light_slots,
            configuration.query_wait.as_millis(),
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

pub(crate) fn validate_app_definition_model(definition: &JsonValue) -> Result<(), String> {
    for view in definition.array_field("queryViews").map_or(&[][..], Vec::as_slice) {
        let owner = format!("view {:?}", view.str_field("name").unwrap_or_default());
        validate_model_fields(&owner, view.get("fields"))?;
        validate_model_measures(&owner, view.get("measures"))?;
    }
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
        let owner = format!("table {alias:?}");
        validate_model_fields(&owner, table.get("fields"))?;
        validate_model_measures(&owner, table.get("measures"))?;
        if table.get("provides").is_some_and(|provides| !provides.as_str().is_some_and(|name| valid_app_model_identifier(name, false))) {
            return Err(format!("{owner} has an invalid provides"));
        }
    }
    Ok(())
}

/// Checks the fields of a table or a query view (unique identifiers and the semantic keys of og-hub/REPORT_RUNTIME.md,
/// plus `role`, `dimension`, `values`, `indicator` and `vocabulary`) and returns their names.
fn validate_model_fields(owner: &str, fields: Option<&JsonValue>) -> Result<std::collections::BTreeSet<String>, String> {
    let mut names = std::collections::BTreeSet::new();
    let Some(fields) = fields else { return Ok(names); };
    let fields = fields.as_array().ok_or_else(|| format!("{owner}.fields must be an array"))?;
    for field in fields {
        let name = field.str_field("name").ok_or_else(|| format!("{owner} contains a field without a name"))?;
        if !valid_app_model_identifier(name, false) {
            return Err(format!("{owner} has invalid field name {name:?}"));
        }
        if !names.insert(name.to_owned()) {
            return Err(format!("{owner} declares field {name:?} more than once"));
        }
    }
    for field in fields {
        let owner = format!("{owner} field {:?}", field.string_field("name"));
        validate_model_semantics(&owner, field)?;
        let one_of = |key: &str, allowed: &[&str]| field.get(key).map_or(Ok(()), |value| match value.as_str() {
            Some(value) if allowed.contains(&value) => Ok(()),
            _ => Err(format!("{owner} has an invalid {key} (expected one of {})", allowed.join(", "))),
        });
        one_of("role", &["measure", "axis", "attribute"])?;
        one_of("aggregate", &["sum", "last", "avg", "min", "max", "count"])?;
        // A ratio being written in the Builder may still lack its terms; a term it names must exist.
        for key in ["num", "den"] {
            if field.str_field(key).is_some_and(|term| !term.is_empty() && !names.contains(term)) {
                return Err(format!("{owner}.{key} must name a declared field"));
            }
        }
        if field.get("dimension").is_some_and(|value| !value.as_str().is_some_and(|name| valid_app_model_identifier(name, false))) {
            return Err(format!("{owner} has an invalid dimension"));
        }
        // `indicator`: the finance indicator a measure details (`ventes.revenue` details `ca`), read by the App compiler.
        if field.get("indicator").is_some_and(|value| field.str_field("role") == Some("axis") || !value.as_str().is_some_and(|name| valid_app_model_identifier(name, false))) {
            return Err(format!("{owner} has an invalid indicator"));
        }
        if let Some(values) = field.get("values") {
            if !["source", "code"].iter().all(|key| values.str_field(key).is_some_and(|name| valid_app_model_identifier(name, true))) {
                return Err(format!("{owner}.values must name a source and a code field"));
            }
        }
    }
    Ok(names)
}

/// Checks `measures`: `{"from": "<view>"}`, a view giving one row per measure (`id`, labels, `unit`, `good`, `vocabulary`).
fn validate_model_measures(owner: &str, measures: Option<&JsonValue>) -> Result<(), String> {
    match measures.map(|measures| measures.get("from").and_then(JsonValue::as_str)) {
        None => Ok(()),
        Some(Some(view)) if valid_app_model_identifier(view, true) => Ok(()),
        Some(_) => Err(format!("{owner}.measures must be {{\"from\": \"<view>\"}}")),
    }
}

/// Semantic keys checked on every field: `unit` (text), `good` (`up` or `down`), `vocabulary` (`{"fr": ["…"], …}`).
fn validate_model_semantics(owner: &str, item: &JsonValue) -> Result<(), String> {
    if item.get("unit").is_some_and(|unit| !unit.is_string()) {
        return Err(format!("{owner} has an invalid unit"));
    }
    if item.get("good").is_some_and(|good| !matches!(good.as_str(), Some("up" | "down"))) {
        return Err(format!("{owner} has an invalid good (expected up or down)"));
    }
    let words = |list: &JsonValue| list.as_array().is_some_and(|words| words.iter().all(|word| word.as_str().is_some_and(|word| !word.trim().is_empty())));
    if item.get("vocabulary").is_some_and(|vocabulary| !vocabulary.as_object().is_some_and(|languages| languages.values().all(words))) {
        return Err(format!("{owner}.vocabulary must map languages to lists of words"));
    }
    Ok(())
}

/// The declared schema of a Place from its visible App instances and their `_apps` records (wip/APP_MODELE_SEMANTIQUE.md):
/// tables per instance (their rows are scoped to it), query views per App, the referentials tables provide, and each
/// collection with the tables over it, its owner (the table that imports into it, else the first) and their fields merged,
/// the owner's first; each instance tells the fields its blocks show (`shows`). The observed layer is `collections.list`,
/// on the node that holds the data.
pub(crate) fn place_schema(installed: &[(JsonValue, JsonValue)]) -> JsonValue {
    let (mut instances, mut tables, mut views, mut referentials, mut templates) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut collections: Vec<(String, Vec<JsonValue>, Option<JsonValue>, Vec<JsonValue>)> = Vec::new();
    let mut seen_views = std::collections::BTreeSet::new();
    for (instance, app) in installed {
        let (instance_id, app_id, definition) = (instance.string_field("instanceId"), app.string_field("appId"), &app["definition"]);
        // The fields the App's blocks show, tile first then full view, in their order: what the App deems important.
        fn shown<'a>(value: &'a JsonValue, out: &mut Vec<&'a str>) {
            match value {
                JsonValue::Object(object) => for (key, item) in object {
                    match (key.as_str(), item) {
                        ("field", JsonValue::String(name)) => out.push(name),
                        ("fields", JsonValue::Array(names)) => out.extend(names.iter().filter_map(JsonValue::as_str)),
                        _ => shown(item, out),
                    }
                },
                JsonValue::Array(items) => items.iter().for_each(|item| shown(item, out)),
                _ => {}
            }
        }
        let mut shows = Vec::new();
        for part in ["tile", "full"] { shown(&definition["views"][part], &mut shows); }
        let mut seen = std::collections::BTreeSet::new();
        shows.retain(|name| seen.insert(*name));
        instances.push(serde_json::json!({"instanceId": instance_id, "appId": app_id, "name": app["name"], "version": app["version"], "shows": shows}));
        // The blocks an App offers as templates (`template: {name, label, vocabulary}`), with the sources they read: a
        // generated App reuses them (`modele:<name>`, service/compile.rs). The first App to name one keeps it.
        fn offered<'a>(value: &'a JsonValue, out: &mut Vec<&'a JsonValue>) {
            match value {
                JsonValue::Object(object) => if object.get("template").is_some_and(JsonValue::is_object) { out.push(value) } else { object.values().for_each(|item| offered(item, out)) },
                JsonValue::Array(items) => items.iter().for_each(|item| offered(item, out)),
                _ => {}
            }
        }
        let mut blocks = Vec::new();
        offered(&definition["views"]["full"], &mut blocks);
        for block in blocks {
            let template = &block["template"];
            let name = template.string_field("name");
            if name.is_empty() || templates.iter().any(|known: &JsonValue| known["name"] == name.as_str()) { continue; }
            let mut node = block.clone();
            node.as_object_mut().map(|object| object.remove("template"));
            let sources: serde_json::Map<String, JsonValue> = block.str_field("source").and_then(|source| Some((source.to_owned(), definition["data"].get(source)?.clone()))).into_iter().collect();
            templates.push(serde_json::json!({"name": name, "appId": app_id, "instanceId": instance_id, "label": template["label"], "vocabulary": template["vocabulary"], "block": node, "sources": sources}));
        }
        let model = &definition["model"];
        for (alias, table) in model["tables"].as_object().into_iter().flatten() {
            let declared = &model["collections"][table.string_field("collection")];
            let collection = declared.as_str().or_else(|| declared.str_field("name")).unwrap_or_default().to_owned();
            let fields = table.array_field("fields").cloned().unwrap_or_default();
            let at = serde_json::json!({"instanceId": instance_id, "table": alias});
            tables.push(serde_json::json!({"instanceId": instance_id, "appId": app_id, "table": alias, "collection": collection, "label": table["label"],
                "provides": table["provides"], "imports": table.get("import").is_some(), "analytics": table["analytics"], "fields": fields}));
            if let Some(name) = table.str_field("provides") {
                referentials.push(serde_json::json!({"name": name, "instanceId": instance_id, "table": alias, "collection": collection}));
            }
            let index = collections.iter().position(|entry| entry.0 == collection).unwrap_or_else(|| { collections.push((collection.clone(), Vec::new(), None, Vec::new())); collections.len() - 1 });
            let entry = &mut collections[index];
            let owns = entry.2.is_none() || (table.get("import").is_some() && !entry.1.iter().any(|other| other["imports"] == true));
            if owns { entry.3.retain(|field| !fields.iter().any(|own| own["name"] == field["name"])); entry.3.splice(0..0, fields.iter().cloned()); entry.2 = Some(at.clone()); }
            else { let added = fields.iter().filter(|field| !entry.3.iter().any(|known| known["name"] == field["name"])).cloned().collect::<Vec<_>>(); entry.3.extend(added); }
            entry.1.push(serde_json::json!({"instanceId": instance_id, "table": alias, "imports": table.get("import").is_some()}));
        }
        for view in definition.array_field("queryViews").into_iter().flatten() {
            if seen_views.insert(view.string_field("name")) {
                views.push(serde_json::json!({"appId": app_id, "name": view["name"], "description": view["description"], "fields": view["fields"], "measures": view["measures"]}));
            }
        }
    }
    let collections = collections.into_iter().map(|(name, tables, owner, fields)| serde_json::json!({"name": name, "tables": tables, "owner": owner, "fields": fields})).collect::<Vec<_>>();
    serde_json::json!({"instances": instances, "tables": tables, "views": views, "referentials": referentials, "collections": collections, "templates": templates})
}

fn bootstrap_apps_if_needed(engine: &Engine, apps_path: &Path) -> Result<(), DaemonError> {
    bootstrap_apps(engine, &base_apps(apps_path))
}

/// The base Apps of this node: the set installed at `path`, or the public ones built in when that set is missing,
/// is not valid JSON or holds an invalid App (the node says so and starts).
fn base_apps(path: &Path) -> Vec<BuiltinApp> {
    read_base_apps(path).unwrap_or_else(|reason| {
        eprintln!("openglacier: base Apps {}: {reason}; starting with the public Apps built in", path.display());
        serde_json::from_str(EMBEDDED_APPS_JSON).expect("embedded apps_public.json must be valid")
    })
}

fn read_base_apps(path: &Path) -> Result<Vec<BuiltinApp>, String> {
    let text = fs::read_to_string(path).map_err(|error| if error.kind() == std::io::ErrorKind::NotFound { "not found".to_owned() } else { error.to_string() })?;
    let apps: Vec<BuiltinApp> = serde_json::from_str(&text).map_err(|error| format!("not a valid App list: {error}"))?;
    for app in &apps {
        validate_app_definition_model(&app.definition).map_err(|message| format!("invalid App {}: {message}", app.app_id))?;
    }
    Ok(apps)
}

/// An App an earlier set of base Apps installed (`createdBy: "system"`) that this one no longer carries stays, with
/// its last definition and its instances, in the compatibility category (`compatibility: true`): the Places that have
/// it go on using it, and only an administrator installs it on another one. Back in the set, it leaves the category.
fn keep_left_apps_for_compatibility(engine: &Engine, apps: &[BuiltinApp], now: u64) -> Result<(), DaemonError> {
    let carried: HashSet<&str> = apps.iter().map(|app| app.app_id.as_str()).collect();
    let managed = query_documents(engine, 0, "on _apps | where createdBy == \"system\" and state == \"active\" | select appId, compatibility")
        .map_err(|_| DaemonError::BootstrapAppsState)?;
    for app in managed {
        let Some(app_id) = app.str_field("appId") else { continue };
        if carried.contains(app_id) || app_in_compatibility(&app) { continue; }
        query_run(engine, 0, quoted!("on _apps | where appId == {} and state == \"active\" | set compatibility = true, updatedBy = \"system\", updatedAt = {now}", app_id))
            .map_err(|_| DaemonError::BootstrapAppsState)?;
        eprintln!("openglacier: App {app_id} is no longer in the base Apps: kept for compatibility");
    }
    Ok(())
}

/// An App kept for compatibility (`keep_left_apps_for_compatibility`).
fn app_in_compatibility(app: &JsonValue) -> bool {
    app.get("compatibility").and_then(JsonValue::as_bool) == Some(true)
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
            quoted!("on _apps | where appId == {app_id} | set name = {}, version = {}, definition = {definition}, state = \"active\", visibility = \"public\", compatibility = false, updatedBy = \"system\", updatedAt = {now}", &app.name, &app.version)
        } else {
            insert_query("_apps", serde_json::json!({"appId": &app.app_id, "name": &app.name, "version": &app.version, "definition": &app.definition, "createdBy": "system", "visibility": "public", "state": "active", "createdAt": now}))
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
    keep_left_apps_for_compatibility(engine, apps, now)
}

fn build_engine( configuration: &Configuration, ) -> Result<(Engine, Option<Arc<GlacierStorage>>), DaemonError> {
    let governor = configuration.memory_limit_bytes.map_or_else(
        MemoryGovernor::unlimited,
        MemoryGovernor::with_process_limit,
    );
    governor.set_query_slots(configuration.query_slots, configuration.light_slots, configuration.query_wait);
    let (storage, glacier_storage) = build_storage(configuration)?;
    let engine = Engine::new(storage).with_memory_governor(governor);
    Ok((if configuration.result_cache { engine } else { engine.without_result_cache() }, glacier_storage))
}

fn build_storage( configuration: &Configuration, ) -> Result<(Arc<dyn StorageEngine>, Option<Arc<GlacierStorage>>), DaemonError> {
    match configuration.storage_backend {
        StorageBackend::Memory if configuration.storage_history => Ok((Arc::new(MemoryStorage::with_history()), None)),
        StorageBackend::Memory => Ok((Arc::new(MemoryStorage::new()), None)),
        StorageBackend::Glacier => {
            prepare_storage_directory(&configuration.storage_path)?;
            GlacierStorage::open(&configuration.storage_path)
                .and_then(|storage| {
                    if configuration.storage_history {
                        storage.record_history()?;
                    } else if configuration.storage_journal {
                        storage.record_journal()?;
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
    /// Filters of the principal's Place access, resolved by the Authority.
    filters: Vec<(String, Vec<crate::Value>)>,
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
    let encoded = match encode_message(response, MessageKind::Response, MAX_RESPONSE_BYTES) {
        // A reply over the protocol limit answers its request with an error rather than leaving it unanswered.
        Err(error @ ProtocolError::MessageTooLarge { .. }) => return write_response(writer, &QueryResponse::request_error(response.id, "protocol.payload_too_large", error.to_string())),
        encoded => encoded.map_err(ConnectionError::Encode)?,
    };
    writer.write_all(&encoded).map_err(ConnectionError::Write)
}

/// The encoded response, or past the size of a message an error of the same request (`id`): its caller learns it at
/// once instead of waiting for an answer that never leaves.
fn encode_response_or_error<T: serde::Serialize>(response: &T, id: Option<RequestId>) -> Result<Vec<u8>, ConnectionError> {
    match encode_message(response, MessageKind::Response, MAX_RESPONSE_BYTES) {
        Ok(encoded) => Ok(encoded),
        Err(ProtocolError::MessageTooLarge { actual, maximum, .. }) => {
            let error = StreamResponse::error(id, WireError::new("protocol.payload_too_large", format!("response message is {actual} bytes; maximum is {maximum} bytes")));
            crate::protocol::encode_stream_response(&error).map_err(ConnectionError::Encode)
        }
        Err(error) => Err(ConnectionError::Encode(error)),
    }
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

/// The identity and the device of a resource assignment, under their current or former names.
fn assignment_node(entry: &JsonValue) -> (Option<&str>, Option<&str>) {
    (entry.str_field("identityId").or_else(|| entry.str_field("nodeIdentityId")), entry.str_field("deviceId").or_else(|| entry.str_field("nodeDeviceId")).or_else(|| entry.str_field("nodeId")))
}

/// Whether `entry` assigns `capability` to the device (or to any device of the identity, when it names none).
fn assigned_to(entry: &JsonValue, identity_id: &str, device_id: &str, capability: &str) -> bool {
    let (identity, device) = assignment_node(entry);
    identity == Some(identity_id) && device.is_none_or(|device| device == device_id) && entry.str_field("capability") == Some(capability)
}

/// Whether `entry` assigns the Files sync of its Place to this device.
fn syncs_files(entry: &JsonValue, identity_id: &str, device_id: &str) -> bool {
    assignment_node(entry) == (Some(identity_id), Some(device_id)) && entry.str_field("capability") == Some("files") && resource_assignment_storage_role(entry) == Some("sync")
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
    let existing = assignments.iter().find(|entry| assigned_to(entry, identity_id, device_id, capability)).cloned();
    assignments.retain(|entry| !assigned_to(entry, identity_id, device_id, capability));

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
    assignments.retain(|entry| !assigned_to(entry, identity_id, device_id, capability));
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
        let (Some(identity_id), Some(device_id)) = assignment_node(assignment) else { continue; };
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
    app_layout: Option<Vec<AppLayoutEntry>>,
    profiles: Vec<PlaceProfile>,
    tile_size: String,
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
            "appLayout": self.app_layout.clone(),
            "profiles": (role == Some(PlaceRole::Owner)).then(|| self.profiles.clone()),
            "tileSize": self.tile_size.clone(),
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
    // A layout written by an older shape is ignored rather than refusing the Place.
    let app_layout = document.get("appLayout").and_then(|value| serde_json::from_value::<Vec<AppLayoutEntry>>(value.clone()).ok());
    let profiles = document.get("profiles").and_then(|value| serde_json::from_value(value.clone()).ok()).unwrap_or_default();
    let tile_size = document.str_field("tileSize").unwrap_or("full").to_owned();
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
        app_layout,
        profiles,
        tile_size,
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
/// does not see a restricted App. `apps`, those of the reader's profile,
/// narrows it further.
fn app_instance_visible( engine: &Engine, request_id: RequestId, principal: &Principal, access: &PlaceAccess, apps: Option<&[String]>, instance: &JsonValue, ) -> Result<bool, QueryResponse> {
    let roles = |value: Option<&JsonValue>| value.and_then(JsonValue::as_array).map(|roles| roles.iter().filter_map(JsonValue::as_str).filter_map(PlaceRole::parse).collect::<Vec<_>>());
    let app = match instance.str_field("appId") {
        Some(app_id) => load_app_definition(engine, request_id, app_id).ok(),
        None => None,
    };
    let visible_to = roles(instance.pointer("/config/visibleTo")).or_else(|| roles(app.as_ref().and_then(|app| app.pointer("/definition/access/visibleTo")))).unwrap_or_default();
    let profiled = apps.is_none_or(|apps| apps.iter().any(|app| instance.str_field("instanceId") == Some(app.as_str())));
    Ok(profiled && (visible_to.is_empty() || access.role().is_some_and(|role| role == PlaceRole::Owner || visible_to.contains(&role)))
        || app.as_ref().zip(principal_identity_id(principal)).is_some_and(|(app, identity_id)| app_identity_is_maintainer(app, identity_id)))
}

/// What `principal` sees of `place` beyond its role: the App instances of its
/// profile (`None`: all) and its row filters, those of the profile its Place
/// access names restricted by the access's own. The Owner has none, unless it
/// reads `as_profile` to see what a profile shows.
fn member_view( engine: &Engine, request_id: RequestId, principal: &Principal, place: &PlaceRecord, access: PlaceAccess, as_profile: Option<&str>, ) -> Result<(Option<Vec<String>>, Vec<(String, Vec<crate::Value>)>), QueryResponse> {
    let invalid = |message: String| QueryResponse::request_error(request_id, "place.access_invalid", message);
    let (profile_id, own) = match (principal_identity_id(principal), access.role()) {
        (_, Some(PlaceRole::Owner)) => (as_profile.map(str::to_owned), JsonValue::Null),
        (Some(identity_id), Some(_)) => {
            let sharing = query_first(engine, request_id, quoted!("on _sharings | where sharingId == {} | select filters, profileId", &place_access_sharing_id(&place.place_id, identity_id)))?.unwrap_or_default();
            (sharing.str_field("profileId").map(str::to_owned), sharing.get("filters").cloned().unwrap_or_default())
        }
        _ => (None, JsonValue::Null),
    };
    let profile = profile_id.map(|profile_id| place.profiles.iter().find(|profile| profile.profile_id == profile_id).ok_or_else(|| invalid(format!("{profile_id:?} is not a profile of this Place")))).transpose()?;
    let filters = crate::access::place::access_filters(profile.and_then(|profile| profile.filters.as_ref()).unwrap_or(&JsonValue::Null)).map_err(invalid)?;
    Ok((profile.and_then(|profile| profile.apps.clone()), crate::access::place::intersect_filters(filters, crate::access::place::access_filters(&own).map_err(invalid)?)))
}

/// The tables of an App's model, with the collection each is stored in (named in `model.collections`).
fn app_tables(app: &JsonValue) -> impl Iterator<Item = (String, &JsonValue)> {
    let model = &app["definition"]["model"];
    model["tables"].as_object().into_iter().flatten().filter_map(move |(_, table)| {
        let declared = &model["collections"][table.string_field("collection")];
        declared.as_str().or_else(|| declared.str_field("name")).filter(|name| !name.is_empty()).map(|name| (name.to_owned(), table))
    })
}

/// `filters` made strict (`<collection>:<field>`) on each collection whose
/// table, in an App of the Place, declares the field: a row there must hold it.
fn strict_filters( engine: &Engine, request_id: RequestId, place_id: &str, filters: Vec<(String, Vec<crate::Value>)>, ) -> Result<Vec<(String, Vec<crate::Value>)>, QueryResponse> {
    if filters.is_empty() { return Ok(filters); }
    let mut strict = Vec::new();
    for instance in query_documents(engine, request_id, quoted!("on _app_instances | where placeId == {} and state == \"active\"", place_id))? {
        let Some(app) = instance.str_field("appId").and_then(|app_id| load_app_definition(engine, request_id, app_id).ok()) else { continue };
        for (collection, table) in app_tables(&app) {
            let declares = |field: &str| table["fields"].as_array().is_some_and(|fields| fields.iter().any(|declared| declared.str_field("name") == Some(field)));
            strict.extend(filters.iter().filter(|(field, _)| !field.contains(':') && declares(field)).map(|(field, values)| (format!("{collection}:{field}"), values.clone())));
        }
    }
    Ok(crate::access::place::intersect_filters(filters, strict))
}

fn place_access_sharing_id(place_id: &str, identity_id: &str) -> String {
    format!("place-access:{place_id}:{identity_id}")
}

/// The role an accepted `_sharings` record grants `target` on `place_id`, and
/// whether it is the scalar `placeRole` of a managed Place access record,
/// which replaces earlier grants. That scalar is scoped by its sharingId: a
/// grant on another Place of the same owner and target must not leak here.
fn sharing_role(document: &JsonValue, place_id: &str, target: &str) -> Option<(PlaceRole, bool)> {
    if !matches!(document.str_field("state"), Some("accepted" | "active")) {
        return None;
    }
    if document.str_field("sharingId") == Some(place_access_sharing_id(place_id, target).as_str()) {
        if let Some(role) = document.str_field("placeRole").and_then(PlaceRole::parse) {
            return Some((role, true));
        }
    }
    document.array_field("permissions")?.iter().filter_map(JsonValue::as_str).filter_map(parse_sharing_permission)
        .filter(|(id, _)| *id == place_id).map(|(_, role)| role).reduce(PlaceRole::strongest).map(|role| (role, false))
}

fn list_place_access( engine: &Engine, request_id: RequestId, place: &PlaceRecord, ) -> Result<Vec<serde_json::Value>, QueryResponse> {
    let query = quoted!("on _sharings | where owner == {} | select sharingId, target, permissions, placeRole, state, filters, profileId | sort target", &place.owner_identity_id);
    let documents = query_documents(engine, request_id, query)?;
    use std::collections::BTreeMap;
    let (mut entries, mut filters, mut profiles): (BTreeMap<String, PlaceRole>, BTreeMap<String, JsonValue>, BTreeMap<String, JsonValue>) = (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
    for document in documents {
        let Some(target) = document.str_field("target") else { continue };
        match sharing_role(&document, &place.place_id, target) {
            Some((role, true)) => {
                entries.insert(target.to_owned(), role);
                filters.extend(document.get("filters").filter(|filters| !filters.is_null()).map(|value| (target.to_owned(), value.clone())));
                profiles.extend(document.get("profileId").filter(|profile| !profile.is_null()).map(|value| (target.to_owned(), value.clone())));
            }
            Some((role, false)) => { let role = entries.get(target).map_or(role, |current| current.strongest(role)); entries.insert(target.to_owned(), role); }
            None => {}
        }
    }
    Ok(entries
        .into_iter()
        .map(|(identity_id, role)| {
            serde_json::json!({
                "filters": filters.get(&identity_id),
                "profileId": profiles.get(&identity_id),
                "identityId": identity_id,
                "role": role.as_str(),
                "state": "active",
            })
        })
        .collect())
}

fn place_audience( engine: &Engine, request_id: RequestId, place_id: &str, ) -> Result<Audience, QueryResponse> {
    place_identities(engine, request_id, place_id).map(Audience::identities)
}

fn place_identities(engine: &Engine, request_id: RequestId, place_id: &str) -> Result<Vec<String>, QueryResponse> {
    let place = load_place(engine, request_id, place_id)?;
    let mut identities = vec![place.owner_identity_id.clone()];
    for entry in list_place_access(engine, request_id, &place)? {
        if let Some(identity_id) = entry.str_field("identityId") {
            identities.push(identity_id.to_owned());
        }
    }
    Ok(identities)
}

fn resolve_place_role( engine: &Engine, request_id: RequestId, identity_id: &str, place: &PlaceRecord, ) -> Result<Option<PlaceRole>, QueryResponse> {
    if place.owner_identity_id == identity_id {
        return Ok(Some(PlaceRole::Owner));
    }
    let query = quoted!("on _sharings | where owner == {} and target == {} | select sharingId, permissions, placeRole, state", &place.owner_identity_id, identity_id);
    let documents = query_documents(engine, request_id, query)?;
    let mut role = None;
    for document in documents {
        role = match sharing_role(&document, &place.place_id, identity_id) {
            Some((granted, true)) => Some(granted),
            Some((granted, false)) => Some(role.map_or(granted, |role: PlaceRole| role.strongest(granted))),
            None => role,
        };
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

/// Whether `app` is in the public catalog: listed and installable by everyone. A created
/// App is `private` unless published; before the field, only the built-in Apps were public.
fn app_is_public(app: &JsonValue) -> bool {
    app.str_field("visibility").map_or(app.str_field("createdBy") == Some("system"), |visibility| visibility == "public")
}

/// Whether `identity_id` may install `app`: a public App, or a private one it created or maintains.
fn app_installable_by(app: &JsonValue, identity_id: &str) -> bool {
    app_is_public(app) || app.str_field("createdBy") == Some(identity_id) || app_identity_is_maintainer(app, identity_id)
}

/// Whether `principal` reads `app`: it may install it, or the App is instantiated in a Place
/// it reads (a role, or a public Place). `placed` caches those App ids across calls.
fn app_readable(engine: &Engine, request_id: RequestId, principal: &Principal, app: &JsonValue, placed: &mut Option<std::collections::BTreeSet<String>>) -> Result<bool, QueryResponse> {
    if app_is_public(app) || principal_identity_id(principal).is_some_and(|identity_id| app_installable_by(app, identity_id)) { return Ok(true); }
    if placed.is_none() { *placed = Some(placed_app_ids(engine, request_id, principal)?); }
    Ok(app.str_field("appId").is_some_and(|app_id| placed.as_ref().is_some_and(|placed| placed.contains(app_id))))
}

/// Active Apps `principal` reads, sorted by name, each with its effective `visibility`.
fn scoped_apps(engine: &Engine, request_id: RequestId, principal: &Principal) -> Result<Vec<JsonValue>, QueryResponse> {
    let mut placed = None;
    let mut apps = Vec::new();
    for app in query_documents(engine, request_id, "on _apps | where state == \"active\" | sort name")? {
        if app_readable(engine, request_id, principal, &app, &mut placed)? { apps.push(with_app_visibility(app)); }
    }
    Ok(apps)
}

fn with_app_visibility(mut app: JsonValue) -> JsonValue {
    app["visibility"] = if app_is_public(&app) { "public" } else { "private" }.into();
    app
}

/// App identifiers instantiated in a Place `principal` reads.
fn placed_app_ids(engine: &Engine, request_id: RequestId, principal: &Principal) -> Result<std::collections::BTreeSet<String>, QueryResponse> {
    let (mut app_ids, mut readable) = (std::collections::BTreeSet::new(), HashMap::<String, bool>::new());
    for instance in query_documents(engine, request_id, "on _app_instances | where state == \"active\" | select placeId, appId")? {
        let (Some(place_id), Some(app_id)) = (instance.str_field("placeId"), instance.str_field("appId")) else { continue; };
        if app_ids.contains(app_id) { continue; }
        let reads = *readable.entry(place_id.to_owned()).or_insert_with(|| load_place(engine, request_id, place_id).is_ok_and(|place| resolve_place_access_for_principal(engine, request_id, principal, &place).is_ok()));
        if reads { app_ids.insert(app_id.to_owned()); }
    }
    Ok(app_ids)
}

/// Who hears about `app` changes: everyone for a public App, otherwise its creator, its
/// maintainers and the members of the Places where it is instantiated.
fn app_audience(engine: &Engine, request_id: RequestId, app: &JsonValue) -> Result<Audience, QueryResponse> {
    if app_is_public(app) { return Ok(Audience::Global); }
    let mut identities = app_maintainers(app);
    identities.extend(app.str_field("createdBy").map(str::to_owned));
    let app_id = app.str_field("appId").unwrap_or_default();
    for instance in query_documents(engine, request_id, quoted!("on _app_instances | where appId == {} and state == \"active\" | select placeId", app_id))? {
        if let Some(place_id) = instance.str_field("placeId") { identities.extend(place_identities(engine, request_id, place_id)?); }
    }
    Ok(Audience::identities(identities))
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

/// The collections `collections.list` shows. Under a Place (and App
/// instance), only those holding some of its rows, each counted by a read in
/// that scope, which reads the scope columns alone ([`place_documents`]).
fn list_storage_collections( engine: &Engine, stats: bool, place_id: Option<&str>, app_instance_id: Option<&str>, ) -> Result<Vec<JsonValue>, String> {
    let snapshot = engine.storage().read().map_err(|error| error.to_string())?;
    let mut collections = Vec::new();
    for name in snapshot.collections().map_err(|error| error.to_string())? {
        if let Some(place_id) = place_id {
            if name.as_str().starts_with('_') {
                continue;
            }
            let documents = place_documents(engine, name.as_str(), place_id, app_instance_id, stats)?;
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

/// The rows of `collection` in the scope of `place` (and `app_instance`):
/// all of them with `all`, else 1 when it holds some. A scoped `count`
/// reads only the scope columns instead of every document, and its result
/// is kept in the result cache under the collection's change token for the
/// Place: the hub's screens listing the same Place at once share one count,
/// and a write of the Place counts again.
fn place_documents(engine: &Engine, collection: &str, place: &str, app_instance: Option<&str>, all: bool) -> Result<u64, String> {
    let text = if all { format!("on {collection} | count as n") } else { format!("on {collection} | first | count as n") };
    let key = engine.change_token(collection, Some(place))
        .map(|token| format!("collections.list\n{place}\n{}\n{token}\n{text}", app_instance.unwrap_or_default()));
    let count = |values: &[JsonValue]| values.first().and_then(|row| row.u64_field("n")).unwrap_or(0);
    let lead = match key.map(|key| engine.results().begin(key)) {
        Some(Lookup::Hit(rows)) => return Ok(count(&rows.values)),
        Some(Lookup::Lead(lead)) => Some(lead),
        Some(Lookup::Run) | None => None,
    };
    let values = crate::view::execute(engine, Some(&crate::view::scope_of(place, app_instance)), &text, true)?;
    if let Some(lead) = lead {
        let mut rows = Rows::new(false);
        let limit = engine.results().entry_limit();
        lead.finish(values.iter().all(|value| rows.push_value(value.clone(), limit)).then_some(rows));
    }
    Ok(count(&values))
}

fn resolve_query_execution_context( engine: &Engine, request_id: RequestId, principal: &Principal, requested: RequestedExecutionContext, ) -> Result<ExecutionContext, QueryResponse> {
    let place = load_place(engine, request_id, &requested.place_id)?;
    let access = resolve_place_access_for_principal(engine, request_id, principal, &place)?;

    let (apps, filters) = member_view(engine, request_id, principal, &place, access, requested.as_profile.as_deref())?;
    if let Some(app_instance_id) = requested.app_instance_id.as_deref() {
        let instance = load_app_instance(engine, request_id, app_instance_id)?;
        if !app_instance_visible(engine, request_id, principal, &access, apps.as_deref(), &instance.document)? {
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

    let filters = strict_filters(engine, request_id, &place.place_id, filters)?;
    Ok(ExecutionContext {
        principal: principal.clone(),
        place_id: requested.place_id,
        app_instance_id: requested.app_instance_id,
        access,
        filters,
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
/// of that scope ([`Engine::plan_scoped`]). The rows sent are copied into
/// `capture` when it holds some, left `None` when the read fails or they
/// pass the result cache's entry limit.
fn try_write_streaming_request( engine: &Engine, writer: &mut TcpStream, request: &QueryRequest, scope: Option<&DocumentScope>, capture: &mut Option<Rows>, ) -> Result<bool, ConnectionError> {
    let mut kept = capture.take();
    let limit = engine.results().entry_limit();
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
        Some(scope) => match engine.plan_scoped(&pipeline, scope) {
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
    // A plan whose blocking stages hold a few rows is a light operation.
    let heavy = governed_blocking && !planned.physical().is_small_state();
    let workload = if heavy { WorkloadClass::Query } else { WorkloadClass::Streaming };
    let _operation_permit = match engine.memory_governor().admit(workload, &request.query) {
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
        .reserve_waiting(MemoryClass::Network, MAX_RESPONSE_BYTES + frames::FLUSH_BYTES)
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
        if kept.as_mut().is_some_and(|rows| !rows.push((!terminal_count).then_some(*id), document.clone(), limit)) {
            kept = None;
        }
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
    let statistics = serde_json::json!({
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
        });
    frames.finish(&StreamResponse::complete(request.id, Some(statistics.clone())))?;
    writer.flush().map_err(ConnectionError::Write)?;
    *capture = kept.map(|rows| rows.with_statistics(Some(statistics)));
    Ok(true)
}

/// Sends rows kept by the result cache as the answer to request `id`, as
/// their read sent them, its statistics marked `cached`.
fn write_kept_rows(writer: &mut TcpStream, id: RequestId, rows: &Rows) -> Result<(), ConnectionError> {
    let mut statistics = rows.statistics.clone();
    if let Some(serde_json::Value::Object(statistics)) = statistics.as_mut() {
        statistics.insert("cached".to_owned(), serde_json::Value::Bool(true));
    }
    if !rows.streamed {
        for value in &rows.values {
            write_stream_response(writer, &StreamResponse::partial(id, value.clone()))?;
        }
        write_stream_response(writer, &StreamResponse::complete(id, statistics))?;
        return writer.flush().map_err(ConnectionError::Write);
    }
    let mut frames = frames::FrameWriter::new(writer);
    for (document_id, document) in &rows.rows {
        match document_id {
            Some(document_id) => frames.push(&BorrowedDocumentResponse { kind: "response", status: "partial", version: PROTOCOL_VERSION, id, data: BorrowedDocumentWithId { id: document_id, document } }),
            None => frames.push(&BorrowedPlainDocumentResponse { kind: "response", status: "partial", version: PROTOCOL_VERSION, id, data: BorrowedDocument(document) }),
        }?;
    }
    frames.finish(&StreamResponse::complete(id, statistics))?;
    writer.flush().map_err(ConnectionError::Write)
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
    let workload = if compact {
        WorkloadClass::Import
    } else if planned.physical().is_memory_streaming() || planned.physical().is_small_state() {
        WorkloadClass::Streaming
    } else {
        WorkloadClass::Query
    };
    // System collections (`_memory`, ...) stay reachable while queries queue.
    let _operation_permit = match (!system_query).then(|| engine.memory_governor().admit(workload, &request.query)).transpose() {
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
        engine.execute_physical_scoped(planned.physical(), &context.scope())
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
    /// Commercial mode: each enrollment presents a token of a policy (`_policy_tokens`).
    Policy,
}
impl EnrollmentMode {
    fn from_environment() -> Result<Self, DaemonError> {
        let mode =
            env::var("OGD_ENROLLMENT_MODE").unwrap_or_else(|_| DEFAULT_ENROLLMENT_MODE.to_owned());
        match mode.trim().to_ascii_lowercase().as_str() {
            "closed" => Ok(Self::Closed),
            "open" => Ok(Self::Open),
            "policy" => Ok(Self::Policy),
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
            Self::Policy => "policy",
        }
    }
    fn allows(&self, token: Option<&str>) -> bool {
        match self {
            Self::Closed => false,
            Self::Open => true,
            Self::Token(expected) => {
                token.is_some_and(|value| constant_time_eq(value.as_bytes(), expected.as_bytes()))
            }
            // The token's policy is checked against the store by the enrollment handler.
            Self::Policy => token.is_some(),
        }
    }
    const fn is_policy(&self) -> bool { matches!(self, Self::Policy) }
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
    /// `OGD_HISTORY=journal`: keep only the commits since the last full backup, for backup diffs (glacier).
    storage_journal: bool,
    /// The base Apps installed on this node (`OGD_APPS_PATH`, default `DEFAULT_APPS_PATH`).
    apps_path: PathBuf,
    files_path: PathBuf,
    files_sync_root: Option<PathBuf>,
    files_sync_config_path: PathBuf,
    files_sync_index_path: PathBuf,
    import_metrics: bool,
    debug_query: bool,
    /// Keeps the rows of repeated Place reads (`OGD_RESULT_CACHE`, on by default).
    result_cache: bool,
    /// One JSON line per `query.execute` (`OGD_QUERY_TRACE`: a file path).
    query_trace: Option<PathBuf>,
    authorization_mode: AuthorizationMode,
    enrollment_mode: EnrollmentMode,
    classic_auth_enabled: bool,
    /// Whether an Owner may open a Place to the public (`OGD_ALLOW_PUBLIC`, on by default).
    allow_public: bool,
    /// Classic identifiers are email addresses (`OGD_CLASSIC_AUTH_EMAIL`, off by default); with the
    /// `mail` capability on this node, a forgotten password is reset with a code sent to them.
    classic_auth_email: bool,
    memory_limit_bytes: Option<usize>,
    memory_profile: MemoryProfileConfig,
    query_slots: usize,
    light_slots: usize,
    query_wait: Duration,
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
        let local_bind_address = env_text("OGD_LOCAL_BIND");
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
        let history_mode = env::var("OGD_HISTORY").map(|value| value.trim().to_ascii_lowercase()).unwrap_or_default();
        let storage_history = matches!(history_mode.as_str(), "on" | "1" | "true" | "yes");
        let storage_journal = history_mode == "journal";
        let apps_path = env_text("OGD_APPS_PATH").map_or_else(|| PathBuf::from(DEFAULT_APPS_PATH), PathBuf::from);
        let gateway_endpoints = match environment("OGD_GATEWAYS")? {
            Some(raw) => {
                let mut endpoints = Vec::new();
                for item in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
                    let endpoint = normalize_gateway_node_endpoint(item)
                        .ok_or_else(|| DaemonError::Runtime(format!("Invalid OGD_GATEWAYS endpoint: {item}")))?;
                    if !endpoints.contains(&endpoint) { endpoints.push(endpoint); }
                }
                endpoints
            }
            None => gateway_endpoint(&bind_address).into_iter().collect(),
        };
        // Paths beside the store, unless the environment names them.
        let data_dir = storage_path.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or_else(|| Path::new(".")).to_path_buf();
        let path = |name: &str, beside: &str| env::var(name).map_or_else(|_| data_dir.join(beside), PathBuf::from);
        let gateway_directory_path = path("OGD_GATEWAY_DIRECTORY_PATH", "sync/gateways.json");
        let files_path = path("OGD_FILES_PATH", "files");
        let files_sync_root = env_text("OGD_FILES_SYNC_ROOT").map(PathBuf::from);
        let files_sync_config_path = path("OGD_FILES_SYNC_CONFIG_PATH", "sync/files.json");
        let files_sync_index_path = path("OGD_FILES_SYNC_INDEX_PATH", "sync/files-index.json");
        let import_metrics = boolean_from_environment("OGD_IMPORT_METRICS", false)?;
        let debug_query = boolean_from_environment("OGD_DEBUG_QUERY", false)?;
        let result_cache = boolean_from_environment("OGD_RESULT_CACHE", true)?;
        let query_trace = env_text("OGD_QUERY_TRACE").map(PathBuf::from);
        let authorization_mode = if boolean_from_environment("OGD_AUTH_REQUIRED", false)? { AuthorizationMode::Enforced } else { AuthorizationMode::Permissive };
        let enrollment_mode = EnrollmentMode::from_environment()?;
        let classic_auth_enabled = boolean_from_environment("OGD_CLASSIC_AUTH_ENABLED", false)?;
        let allow_public = boolean_from_environment("OGD_ALLOW_PUBLIC", true)?;
        let classic_auth_email = boolean_from_environment("OGD_CLASSIC_AUTH_EMAIL", false)?;
        let memory_limit_bytes = optional_bytes_from_environment("OGD_MEMORY_LIMIT")?;
        let memory_profile = memory_limit_bytes.map_or_else( MemoryProfileConfig::unlimited, MemoryProfileConfig::for_limit, );
        // Queries run at most one per hardware thread; the others wait their turn.
        let query_slots = count_from_environment("OGD_QUERY_SLOTS", thread::available_parallelism().map_or(4, usize::from))?;
        let light_slots = count_from_environment("OGD_QUERY_LIGHT_SLOTS", query_slots)?;
        let query_wait = duration_from_environment("OGD_QUERY_WAIT_MS", DEFAULT_QUERY_WAIT_MS)?;
        // After it, a running query gives its slot to new ones, mostly short.
        let bootstrap_admin_path = path("OGD_BOOTSTRAP_ADMIN_PATH", "bootstrap/admin.ogid");
        let bootstrap_password_file = env::var("OGD_BOOTSTRAP_PASSWORD_FILE").ok().map(PathBuf::from);
        let bootstrap_password = env::var("OGD_BOOTSTRAP_PASSWORD").ok();
        let backup_path = path("OGD_BACKUP_PATH", "backups");
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
        let node_identity_file = env_text("OGD_NODE_IDENTITY_FILE").map(PathBuf::from);
        let node_identity_password = env::var("OGD_NODE_IDENTITY_PASSWORD").ok().filter(|value| !value.is_empty());
        let compiled_capabilities = ServiceCapabilities::compiled();
        // The capabilities a node enables (`OGD_NODE_CAPABILITIES`, compiled ones only) and publishes (enabled ones only).
        let capabilities = |name: &'static str, default: ServiceCapabilities, within: ServiceCapabilities, refused: &str, label: &str| -> Result<ServiceCapabilities, DaemonError> {
            let Some(raw) = environment(name)? else { return Ok(default) };
            let requested = ServiceCapabilities::from_names(raw.split(',').map(str::trim).filter(|value| !value.is_empty())).map_err(DaemonError::Runtime)?;
            let missing = requested.missing_from(within);
            if missing.is_empty() { Ok(requested) } else { Err(DaemonError::Runtime(format!("{name} {refused}: {} ({label}: {})", missing.names().join(","), within.names().join(",")))) }
        };
        let node_capabilities = capabilities("OGD_NODE_CAPABILITIES", ServiceCapabilities::default_enabled(), compiled_capabilities, "requests capabilities not compiled into this ogd build", "compiled")?;
        let published_capabilities = capabilities("OGD_NODE_PUBLISHED_CAPABILITIES", node_capabilities, node_capabilities, "may only publish enabled capabilities; not enabled", "enabled")?;
        let gateway_token = env_text("OGD_GATEWAY_TOKEN");

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
            storage_journal,
            apps_path,
            files_path,
            files_sync_root,
            files_sync_config_path,
            files_sync_index_path,
            import_metrics,
            debug_query,
            result_cache,
            query_trace,
            authorization_mode,
            enrollment_mode,
            classic_auth_enabled,
            allow_public,
            classic_auth_email,
            memory_limit_bytes,
            memory_profile,
            query_slots,
            light_slots,
            query_wait,
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
fn environment(name: &'static str) -> Result<Option<String>, DaemonError> { env_var(name).map_err(|source| DaemonError::Environment { name, source }) }
fn optional_bytes_from_environment(name: &'static str) -> Result<Option<usize>, DaemonError> { environment(name)?.map(|value| parse_byte_size(name, &value)).transpose() }
fn parse_byte_size(name: &'static str, value: &str) -> Result<usize, DaemonError> { const KIB: u128 = 1024; const MIB: u128 = 1024 * KIB; const GIB: u128 = 1024 * MIB; let invalid = || DaemonError::InvalidByteSize { name, value: value.to_owned(), }; let trimmed = value.trim(); if trimmed.is_empty() { return Err(invalid()); } let split = trimmed .find(|character: char| !(character.is_ascii_digit() || character == '.')) .unwrap_or(trimmed.len()); let (number, suffix) = trimmed.split_at(split); if number.is_empty() || number == "." || number.matches('.').count() > 1 { return Err(invalid()); } let (whole, fraction) = number.split_once('.').unwrap_or((number, "")); let whole = if whole.is_empty() { 0 } else { whole.parse::<u128>().map_err(|_| invalid())? }; let fraction_value = if fraction.is_empty() { 0 } else { fraction.parse::<u128>().map_err(|_| invalid())? }; let fraction_scale = 10_u128 .checked_pow(u32::try_from(fraction.len()).map_err(|_| invalid())?) .ok_or_else(invalid)?; let multiplier = match suffix.trim().to_ascii_lowercase().as_str() { "" | "m" | "mb" | "mib" => MIB, "b" => 1, "k" | "kb" | "kib" => KIB, "g" | "gb" | "gib" => GIB, _ => return Err(invalid()), }; let integral_bytes = whole.checked_mul(multiplier).ok_or_else(invalid)?; let fractional_bytes = fraction_value .checked_mul(multiplier) .and_then(|bytes| bytes.checked_div(fraction_scale)) .ok_or_else(invalid)?; let bytes = integral_bytes .checked_add(fractional_bytes) .filter(|bytes| *bytes > 0) .ok_or_else(invalid)?; usize::try_from(bytes).map_err(|_| invalid()) }
fn boolean_from_environment(name: &'static str, default: bool) -> Result<bool, DaemonError> { environment(name)?.map_or(Ok(default), |value| flag(&value).ok_or(DaemonError::InvalidBoolean { name, value })) }
fn count_from_environment(name: &'static str, default: usize) -> Result<usize, DaemonError> { environment(name)?.map_or(Ok(default), |value| value.trim().parse().ok().filter(|count| *count > 0).ok_or(DaemonError::InvalidCount { name, value })) }
fn duration_from_environment(name: &'static str, default_milliseconds: u64) -> Result<Duration, DaemonError> { let Some(value) = environment(name)? else { return Ok(Duration::from_millis(default_milliseconds)) }; let milliseconds = value.parse::<u64>().map_err(|source| DaemonError::InvalidDuration { name, value: value.clone(), source })?; if milliseconds == 0 { return Err(DaemonError::ZeroDuration { name }); } Ok(Duration::from_millis(milliseconds)) }

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

/// Holds `key` back for `delay_ms` (one reset mail per address and minute).
fn classic_throttle(settings: &ConnectionSettings, key: &str, delay_ms: u64) {
    if let Ok(mut attempts) = settings.classic_auth_attempts.lock() {
        attempts.insert(key.to_owned(), ClassicLoginAttempt { failures: 0, retry_at: unix_time_millis().saturating_add(delay_ms) });
    }
}

/// A reset code of 8 characters without look-alikes (about 40 bits), shown as `XXXX-XXXX`.
fn classic_reset_code() -> Result<String, String> {
    const ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";
    let mut code = String::with_capacity(9);
    let mut random = [0u8; 64];
    fs::File::open("/dev/urandom").and_then(|mut file| file.read_exact(&mut random)).map_err(|error| error.to_string())?;
    // Rejection sampling keeps every character equally likely.
    let limit = 256 - 256 % ALPHABET.len();
    for byte in random.iter().filter(|byte| usize::from(**byte) < limit).take(8) {
        if code.len() == 4 { code.push('-'); }
        code.push(char::from(ALPHABET[usize::from(*byte) % ALPHABET.len()]));
    }
    if code.len() == 9 { Ok(code) } else { Err("not enough random bytes".to_owned()) }
}

fn classic_reset_mail(to: &str, code: &str, language: Option<&str>) -> MailSendInput {
    let french = language.is_some_and(|language| language.to_ascii_lowercase().starts_with("fr"));
    let (subject, text) = if french {
        ("Réinitialisation de votre mot de passe", format!("Bonjour,\n\nVoici votre code pour choisir un nouveau mot de passe : {code}\n\nIl est valable 30 minutes. Si vous n'êtes pas à l'origine de cette demande, ignorez ce message : votre mot de passe reste inchangé.\n"))
    } else {
        ("Reset your password", format!("Hello,\n\nHere is your code to choose a new password: {code}\n\nIt is valid for 30 minutes. If you did not ask for it, ignore this message: your password stays unchanged.\n"))
    };
    MailSendInput { context: RequestedExecutionContext { place_id: String::new(), app_instance_id: None, as_profile: None }, to: vec![to.to_owned()], cc: Vec::new(), bcc: Vec::new(), reply_to: None, subject: subject.to_owned(), text, html: None }
}

fn classic_login_success(settings: &ConnectionSettings, identifier: &str) {
    if let Ok(mut attempts) = settings.classic_auth_attempts.lock() { attempts.remove(identifier); }
}

impl ConnectionSettings {
    /// A forgotten classic password is reset by mail: classic auth with email identifiers and the mail service here.
    fn password_reset_available(&self) -> bool { self.classic_auth_enabled && self.classic_auth_email && self.mail_enabled() }

    #[cfg(feature = "mailsend")]
    fn mail_enabled(&self) -> bool { self.mail_service.is_some() }

    #[cfg(not(feature = "mailsend"))]
    #[allow(clippy::unused_self)]
    fn mail_enabled(&self) -> bool { false }

    /// Sent apart: a slow SMTP server neither holds the connection nor tells a registered address.
    #[cfg(feature = "mailsend")]
    fn send_mail_detached(&self, mail: MailSendInput) {
        let Some(service) = self.mail_service.clone() else { return };
        thread::spawn(move || if let Err(error) = service.send(&mail) { eprintln!("ogd: mail to {:?} failed: {error} ({})", mail.to, error.code()); });
    }

    #[cfg(not(feature = "mailsend"))]
    #[allow(clippy::unused_self, clippy::needless_pass_by_value)]
    fn send_mail_detached(&self, _mail: MailSendInput) {}
}

#[derive(Debug)]
struct ConnectionSettings {
    /// `OGD_QUERY_TRACE`, opened for appending.
    query_trace: Option<Mutex<std::fs::File>>,
    read_timeout: Duration,
    write_timeout: Duration,
    agent_provider_read_timeout: Duration,
    import_metrics: bool,
    authorization_mode: AuthorizationMode,
    enrollment_mode: EnrollmentMode,
    classic_auth_enabled: bool,
    allow_public: bool,
    classic_auth_email: bool,
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
    #[cfg(feature = "mailsend")]
    mail_service: Option<Arc<MailService>>,
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
    InvalidCount {name: &'static str, value: String},
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
            Self::InvalidCount {name, value} => write!(formatter, "{name} must be a positive integer; got `{value}`"),
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
            | Self::InvalidCount { .. }
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

    #[test] fn a_reply_over_the_protocol_limit_answers_its_request_with_an_error() { let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap(); let mut writer = TcpStream::connect(listener.local_addr().unwrap()).unwrap(); let mut reader = listener.accept().unwrap().0; write_operation_response(&mut writer, &OperationResponse::new(RequestId::Number(7), serde_json::json!({"rows": "x".repeat(MAX_RESPONSE_BYTES)}))).unwrap(); let mut header = [0u8; LENGTH_PREFIX_BYTES]; reader.read_exact(&mut header).unwrap(); let mut payload = vec![0u8; u32::from_be_bytes(header) as usize]; reader.read_exact(&mut payload).unwrap(); let reply: JsonValue = rmp_serde::from_slice(&payload).unwrap(); assert_eq!((reply["id"].clone(), reply["error"]["code"].clone()), (serde_json::json!(7), serde_json::json!("protocol.payload_too_large"))); }

    #[test] fn classic_email_identifiers_are_bare_addresses_with_a_domain() { for valid in ["ana@example.com", "a.b+tag@sub.example.fr", "Zoé@exemple.fr"] { assert!(email_address_valid(valid), "{valid}"); } for invalid in ["ana", "ana@", "@example.com", "ana@localhost", "ana@example..com", ".ana@example.com", "ana.@example.com", "a..b@example.com", "Ana <ana@example.com>", "ana @example.com", "ana@-example.com", "ana@example.com\r\nBcc: x@y.z", "a@b@example.com"] { assert!(!email_address_valid(invalid), "{invalid}"); } }
    #[test] fn password_reset_codes_are_eight_unambiguous_characters() { let codes = (0..64).map(|_| classic_reset_code().unwrap()).collect::<std::collections::HashSet<_>>(); assert!(codes.len() > 60); for code in &codes { assert_eq!((code.len(), &code[4..5]), (9, "-")); assert!(code.chars().filter(|c| *c != '-').all(|c| "ABCDEFGHJKMNPQRSTUVWXYZ23456789".contains(c))); } }
    #[test] fn password_reset_mails_go_to_the_login_in_its_language() { let mail = classic_reset_mail("ana@example.com", "ABCD-EFGH", Some("fr-FR")); assert_eq!((mail.to.as_slice(), mail.subject.as_str()), (["ana@example.com".to_owned()].as_slice(), "Réinitialisation de votre mot de passe")); assert!(mail.text.contains("ABCD-EFGH")); assert!(classic_reset_mail("ana@example.com", "X", None).subject.starts_with("Reset")); }

    #[test] fn app_model_semantics_accept_roles_ratios_axes_vocabulary_and_definition_views() { let definition = serde_json::json!({"model": {"collections": {"ventes": "ventes"}, "tables": {"ventes": {"collection": "ventes", "provides": "sales", "fields": [{"name": "revenue", "type": "number", "role": "measure", "indicator": "ca", "aggregate": "sum", "unit": "amount", "good": "up", "vocabulary": {"fr": ["CA facturé"], "en": ["sales"]}}, {"name": "margin", "type": "number", "aggregate": "count"}, {"name": "tm", "type": "ratio", "num": "margin", "den": "revenue", "unit": "percent", "vocabulary": {"fr": ["taux de marge"]}}, {"name": "draft", "type": "ratio", "num": "", "den": ""}, {"name": "entity", "type": "text", "role": "axis", "dimension": "org"}, {"name": "family", "type": "text", "role": "axis", "values": {"source": "ventes_articles", "code": "family"}}]}}}, "queryViews": [{"name": "finance.indicators", "query": "on finance", "fields": [{"name": "entity", "role": "axis", "dimension": "org"}], "measures": {"from": "finance.indicators.definitions"}}]}); assert_eq!(validate_app_definition_model(&definition), Ok(())); let apps: Vec<BuiltinApp> = serde_json::from_str(ALL_APPS_JSON).unwrap(); assert!(apps.iter().all(|app| validate_app_definition_model(&app.definition).is_ok())); }

    #[test] fn place_schema_merges_the_tables_over_one_collection_and_lists_referentials_and_views_once() { let app = |id: &str, tables: serde_json::Value, views: serde_json::Value| serde_json::json!({"appId": id, "name": id, "version": "1", "definition": {"model": {"collections": {"v": "ventes", "o": {"name": "org"}}, "tables": tables}, "queryViews": views}}); let instance = |id: &str, app: &str| serde_json::json!({"instanceId": id, "appId": app}); let standard = app("system.sales", serde_json::json!({"ventes": {"collection": "v", "import": {}, "fields": [{"name": "revenue", "role": "measure"}, {"name": "entity", "role": "axis", "dimension": "org"}]}}), serde_json::json!([{"name": "sales.by_client", "query": "on ventes", "fields": [{"name": "client", "role": "axis"}]}])); let custom = app("app.crm", serde_json::json!({"mine": {"collection": "v", "fields": [{"name": "revenue", "role": "attribute"}, {"name": "rep", "role": "axis"}]}, "org": {"collection": "o", "provides": "org", "fields": [{"name": "code"}]}}), serde_json::json!([])); let schema = place_schema(&[(instance("i-crm", "app.crm"), custom.clone()), (instance("i-sales", "system.sales"), standard.clone()), (instance("i-sales-2", "system.sales"), standard)]); assert_eq!(schema["instances"].as_array().unwrap().len(), 3); assert_eq!(place_schema(&[(instance("i", "a"), serde_json::json!({"appId": "a", "definition": {"views": {"tile": [{"type": "kpi", "fields": ["ca", "marge"]}], "full": [{"type": "pages", "pages": [{"children": [{"field": "tm"}, {"fields": ["ca"]}]}]}]}}}))])["instances"][0]["shows"], serde_json::json!(["ca", "marge", "tm"])); assert_eq!(schema["tables"].as_array().unwrap().len(), 4); assert_eq!(schema["views"], serde_json::json!([{"appId": "system.sales", "name": "sales.by_client", "description": null, "fields": [{"name": "client", "role": "axis"}], "measures": null}])); assert_eq!(schema["referentials"], serde_json::json!([{"name": "org", "instanceId": "i-crm", "table": "org", "collection": "org"}])); let ventes = &schema["collections"][0]; assert_eq!((ventes["name"].clone(), ventes["owner"].clone(), ventes["tables"].as_array().unwrap().len()), (serde_json::json!("ventes"), serde_json::json!({"instanceId": "i-sales", "table": "ventes"}), 3)); assert_eq!(ventes["fields"], serde_json::json!([{"name": "revenue", "role": "measure"}, {"name": "entity", "role": "axis", "dimension": "org"}, {"name": "rep", "role": "axis"}])); assert_eq!(crate::operation::OperationKind::PlaceSchema.scope(), crate::operation::OperationScope::Authority); }

    #[test] fn place_schema_of_the_finance_apps_carries_their_axes_ratios_and_indicator_definitions() { let apps: Vec<serde_json::Value> = serde_json::from_str(ALL_APPS_JSON).unwrap(); let installed = apps.iter().filter(|app| app["appId"] == "system.finance" || app["appId"] == "system.finance.activite").map(|app| (serde_json::json!({"instanceId": app["appId"], "appId": app["appId"]}), app.clone())).collect::<Vec<_>>(); let schema = place_schema(&installed); let all = place_schema(&apps.iter().map(|app| (serde_json::json!({"instanceId": app["appId"], "appId": app["appId"]}), app.clone())).collect::<Vec<_>>()); let templates = all["templates"].as_array().unwrap(); assert_eq!(templates.iter().map(|template| template["name"].as_str().unwrap()).collect::<Vec<_>>(), ["creances.clientaging", "fournisseurs.supplieraging", "compte.statement", "stocks.stockrotation", "synthese.kpis", "tresorerie.cashflow"]); let statement = templates.iter().find(|template| template["name"] == "compte.statement").unwrap(); assert_eq!((statement["block"]["source"].clone(), statement["block"].get("template"), statement["sources"]["statement"]["view"].clone()), (serde_json::json!("statement"), None, serde_json::json!("finance.statement"))); let view = schema["views"].as_array().unwrap().iter().find(|view| view["name"] == "finance.indicators").unwrap(); assert_eq!(view["measures"], serde_json::json!({"from": "finance.indicators.definitions"})); assert!(view.get("query").is_none()); let ventes = schema["collections"].as_array().unwrap().iter().find(|collection| collection["name"] == "ventes").unwrap(); let field = |name: &str| ventes["fields"].as_array().unwrap().iter().find(|field| field["name"] == name).unwrap().clone(); assert_eq!((field("entity")["dimension"].clone(), field("tm")["type"].clone(), field("tm")["num"].clone()), (serde_json::json!("org"), serde_json::json!("ratio"), serde_json::json!("margin"))); }

    #[test] fn app_model_semantics_refuse_unknown_roles_dangling_ratios_and_bad_vocabulary() { let with = |table: serde_json::Value, views: serde_json::Value| validate_app_definition_model(&serde_json::json!({"model": {"collections": {"c": "c"}, "tables": {"t": table}}, "queryViews": views})).unwrap_err(); let table = |fields: serde_json::Value, provides: serde_json::Value| with(serde_json::json!({"collection": "c", "provides": provides, "fields": [{"name": "a", "type": "number"}, {"name": "b", "type": "number"}, fields]}), serde_json::json!([])); let field = |field: serde_json::Value| table(field, serde_json::json!("p")); assert!(field(serde_json::json!({"name": "x", "role": "metric"})).contains("invalid role")); assert!(field(serde_json::json!({"name": "x", "aggregate": "median"})).contains("invalid aggregate")); assert!(field(serde_json::json!({"name": "r", "type": "ratio", "num": "a", "den": "z"})).contains("field \"r\".den must name a declared field")); assert!(field(serde_json::json!({"name": "a"})).contains("declares field \"a\" more than once")); assert!(field(serde_json::json!({"name": "x", "vocabulary": {"fr": "taux"}})).contains("vocabulary must map")); assert!(field(serde_json::json!({"name": "x", "good": "high"})).contains("invalid good")); assert!(field(serde_json::json!({"name": "x", "values": {"source": "s"}})).contains("values must name")); assert!(table(serde_json::json!({"name": "x"}), serde_json::json!("bad name")).contains("invalid provides")); assert!(with(serde_json::json!({"collection": "c", "measures": [{"name": "m"}]}), serde_json::json!([])).contains("measures must be")); assert!(with(serde_json::json!({"collection": "c"}), serde_json::json!([{"name": "v", "measures": {"from": 3}}])).contains("view \"v\".measures must be")); }

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

    #[test] fn apps_are_open_only_to_their_roles() { let engine = public_places_test_engine(); for query in ["on _places | insert {placeId: \"p\", name: \"P\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 4}", "on _apps | insert {appId: \"app.fin\", state: \"active\", definition: {access: {visibleTo: [\"resident\"]}}, maintainersJson: \"[\\\"maint\\\"]\"}", "on _app_instances | insert {instanceId: \"i-fin\", placeId: \"p\", appId: \"app.fin\", state: \"active\", createdAt: 3}", "on _app_instances | insert {instanceId: \"i-cfg\", placeId: \"p\", appId: \"app.fin\", config: {visibleTo: [\"member\"]}, state: \"active\", createdAt: 4}", "on _app_instances | insert {instanceId: \"i-open\", placeId: \"p\", appId: \"app.open\", state: \"active\", createdAt: 5}"].map(str::to_owned).into_iter().chain([("res", "resident"), ("mem", "member"), ("maint", "member")].map(|(target, role)| format!("on _sharings | insert {{sharingId: \"place-access:p:{target}\", owner: \"owner\", target: \"{target}\", placeRole: \"{role}\", state: \"active\"}}"))) { assert!(execute_request(&engine, QueryRequest::new(RequestId::Number(1), query.clone())).is_ok(), "{query}"); } let open = |identity_id: &str, instance: &str| resolve_query_execution_context(&engine, RequestId::Number(1), &identity(identity_id), RequestedExecutionContext { place_id: "p".to_owned(), app_instance_id: Some(instance.to_owned()), as_profile: None }).is_ok(); assert_eq!(["owner", "res", "mem", "maint"].map(|who| (open(who, "i-fin"), open(who, "i-cfg"), open(who, "i-open"))), [(true, true, true), (true, false, true), (false, true, true), (true, true, true)]); }
    #[test] fn place_list_returns_scope_plus_public_places() { let engine = public_places_test_engine(); let ids = |places: Vec<JsonValue>| places.iter().filter_map(|place| place.str_field("placeId").map(str::to_owned)).collect::<Vec<_>>(); assert_eq!(ids(list_public_places(&engine, RequestId::Number(1)).unwrap()), vec!["public-ro", "public-rw"]); let stranger = list_places_for_identity_and_public(&engine, RequestId::Number(1), "stranger").unwrap(); assert_eq!(ids(stranger.clone()), vec!["public-ro", "public-rw"]); assert!(stranger.iter().all(|place| place.get("role").is_some_and(JsonValue::is_null))); let owner = list_places_for_identity_and_public(&engine, RequestId::Number(1), "owner").unwrap(); assert_eq!(ids(owner.clone()), vec!["private", "public-ro", "public-rw"]); assert!(owner.iter().all(|place| place.str_field("role") == Some("owner"))); }

    #[test] fn custom_apps_stay_in_the_scope_of_their_creator() { let engine = public_places_test_engine(); for query in ["on _apps | insert {appId: \"app.public\", createdBy: \"author\", visibility: \"public\", state: \"active\"}", "on _apps | insert {appId: \"app.private\", createdBy: \"creator\", visibility: \"private\", state: \"active\"}", "on _apps | insert {appId: \"app.legacy\", createdBy: \"creator\", state: \"active\"}", "on _apps | insert {appId: \"system.files\", createdBy: \"system\", state: \"active\"}", "on _apps | insert {appId: \"app.maintained\", createdBy: \"author\", visibility: \"private\", maintainersJson: \"[\\\"maint\\\"]\", state: \"active\"}", "on _sharings | insert {sharingId: \"place-access:private:member\", owner: \"owner\", target: \"member\", placeRole: \"member\", state: \"active\"}"] { assert!(execute_request(&engine, QueryRequest::new(RequestId::Number(1), query)).is_ok(), "{query}"); } let listed = |principal: &Principal| { let mut apps = scoped_apps(&engine, RequestId::Number(1), principal).unwrap().iter().map(|app| format!("{}:{}", app["appId"].as_str().unwrap(), app["visibility"].as_str().unwrap())).collect::<Vec<_>>(); apps.sort(); apps.join(" ") }; assert_eq!(["creator", "maint", "owner", "member", "stranger"].map(|who| listed(&identity(who))), ["app.legacy:private app.private:private app.public:public system.files:public", "app.maintained:private app.public:public system.files:public", "app.private:private app.public:public system.files:public", "app.private:private app.public:public system.files:public", "app.public:public system.files:public"]); assert_eq!(listed(&Principal::Anonymous), "app.public:public system.files:public"); let app = |app_id: &str| load_app_definition(&engine, RequestId::Number(1), app_id).unwrap(); assert_eq!([("creator", "app.private"), ("creator", "app.legacy"), ("owner", "app.private"), ("maint", "app.maintained"), ("author", "app.maintained"), ("stranger", "app.legacy"), ("stranger", "system.files")].map(|(who, app_id)| app_installable_by(&app(app_id), who)), [true, true, false, true, true, false, true]); assert_eq!(app_audience(&engine, RequestId::Number(1), &app("app.private")).unwrap(), Audience::identities(["creator", "owner", "member"].map(str::to_owned))); assert_eq!(app_audience(&engine, RequestId::Number(1), &app("app.public")).unwrap(), Audience::Global); }

    #[test] fn files_context_keeps_public_read_only_places_read_only() { let engine = public_places_test_engine(); { let (place_id, instance_id) = ("public-ro", "i-public"); assert!(resolve_file_context(&engine, RequestId::Number(1), &Principal::Anonymous, place_id, instance_id, false).is_ok()); assert!(resolve_file_context(&engine, RequestId::Number(1), &Principal::Anonymous, place_id, instance_id, true).is_err()); } }

    fn app_bootstrap_test_engine() -> Engine {
        let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        Engine::new(storage)
    }

    /// Every App of the repository (`apps.json`), as a node with that set installed has them.
    const ALL_APPS_JSON: &str = include_str!("../../apps.json");

    fn bootstrap_all_apps(engine: &Engine) -> Result<(), DaemonError> {
        bootstrap_apps(engine, &serde_json::from_str::<Vec<BuiltinApp>>(ALL_APPS_JSON).expect("apps.json is valid"))
    }

    fn test_app(app_id: &str) -> BuiltinApp {
        BuiltinApp { app_id: app_id.to_owned(), name: app_id.to_owned(), version: "1.0.0".to_owned(), definition: serde_json::json!({"id": app_id, "name": app_id, "version": "1.0.0"}) }
    }

    fn app_record(engine: &Engine, app_id: &str) -> JsonValue {
        query_first(engine, 0, quoted!("on _apps | where appId == {} | limit 1", app_id)).unwrap().expect("App record")
    }

    #[test] fn base_apps_come_from_the_installed_set_or_else_from_the_public_apps_built_in() {
        let directory = std::env::temp_dir().join(format!("og-base-apps-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let ids = |apps: Vec<BuiltinApp>| apps.into_iter().map(|app| app.app_id).collect::<Vec<_>>();
        let public = ids(serde_json::from_str(EMBEDDED_APPS_JSON).unwrap());
        assert_eq!(public, ["system.query", "system.files", "system.projects", "system.ressources", "system.calls"]);
        let set = directory.join("apps.json");
        std::fs::write(&set, ALL_APPS_JSON).unwrap();
        let all = ids(serde_json::from_str(ALL_APPS_JSON).unwrap());
        assert!(all.iter().any(|id| id == "system.finance"));
        assert_eq!(ids(base_apps(&set)), all);
        assert_eq!(ids(base_apps(&directory.join("missing.json"))), public);
        std::fs::write(&set, "[{\"appId\": ").unwrap();
        assert_eq!(ids(base_apps(&set)), public);
        std::fs::write(&set, r#"[{"appId": "x.bad", "name": "Bad", "version": "1.0.0", "definition": {"model": {"tables": []}}}]"#).unwrap();
        assert!(read_base_apps(&set).unwrap_err().contains("x.bad"));
        assert_eq!(ids(base_apps(&set)), public);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test] fn an_app_leaving_the_base_set_is_kept_for_compatibility_with_its_instances_until_it_comes_back() {
        let engine = app_bootstrap_test_engine();
        bootstrap_apps(&engine, &[test_app("system.kept"), test_app("system.left")]).unwrap();
        query_run(&engine, 1, "on _app_instances | insert {instanceId: \"i\", placeId: \"p\", appId: \"system.left\", state: \"active\", createdAt: 1}").unwrap();
        query_run(&engine, 1, r#"on _apps | insert {appId: "user.app", name: "U", version: "1.0.0", definition: {id: "user.app"}, createdBy: "someone", state: "active", createdAt: 1}"#).unwrap();
        bootstrap_apps(&engine, &[test_app("system.kept")]).unwrap();
        let left = app_record(&engine, "system.left");
        assert_eq!((left.str_field("state"), app_in_compatibility(&left), left.get("definition")), (Some("active"), true, Some(&test_app("system.left").definition)));
        assert!(!app_in_compatibility(&app_record(&engine, "system.kept")));
        assert!(!app_in_compatibility(&app_record(&engine, "user.app")), "an App the base set never installed is not the set's to keep");
        assert_eq!(query_documents(&engine, 2, "on _app_instances | where appId == \"system.left\" and state == \"active\" | count as n").unwrap(), vec![serde_json::json!({"n": 1})]);
        bootstrap_apps(&engine, &[test_app("system.kept")]).unwrap();
        assert!(app_in_compatibility(&app_record(&engine, "system.left")));
        bootstrap_apps(&engine, &[test_app("system.kept"), test_app("system.left")]).unwrap();
        assert!(!app_in_compatibility(&app_record(&engine, "system.left")));
    }

    #[test] fn an_app_kept_for_compatibility_is_installed_again_by_an_app_manager_or_a_policy_that_includes_it() {
        let engine = app_bootstrap_test_engine();
        bootstrap_apps(&engine, &[test_app("system.left")]).unwrap();
        bootstrap_apps(&engine, &[]).unwrap();
        let app = app_record(&engine, "system.left");
        let installable = |enforced: bool, policy: bool, who: &str| place_ops::compatibility_app_installable(enforced, policy, &engine, RequestId::Number(3), &identity(who), &app).unwrap_or_else(|_| panic!("installability"));
        assert!(installable(false, false, "owner"), "without enforced authorization every identity manages Apps");
        assert!(!installable(true, false, "owner"));
        query_run(&engine, 4, insert_query("_permissions", serde_json::json!({"identityId": "admin", "action": "app.manage", "resource": "*", "effect": "allow", "state": "active", "createdAt": 1}))).unwrap();
        assert!(installable(true, false, "admin"));
        for query in [r#"on _pools | insert {poolId: "pool", label: "P", state: "active"}"#, r#"on _policies | insert {policyId: "pol", poolId: "pool", label: "Std", apps: {included: ["system.left"]}, state: "active"}"#, r#"on _identities | insert {identityId: "member", policyId: "pol", state: "active"}"#] { query_run(&engine, 5, query).unwrap(); }
        assert_eq!(installable(true, true, "member"), cfg!(feature = "auth"));
        assert!(!installable(true, false, "member"), "outside policy mode a policy includes nothing");
    }

    #[cfg(feature = "fabric")] #[test] fn gateway_endpoint_normalization_supports_seeds_and_discovery_hints() { assert_eq!( normalize_gateway_node_endpoint("gw+insecure://127.0.0.1:3000").as_deref(), Some("ws://127.0.0.1:3000/node") ); assert_eq!( normalize_gateway_node_endpoint("gw://example.test").as_deref(), Some("wss://example.test/node") ); assert_eq!( normalize_gateway_node_endpoint("ws://127.0.0.1:3001").as_deref(), Some("ws://127.0.0.1:3001/node") ); assert_eq!( normalize_gateway_node_endpoint("ws://127.0.0.1:3002/peer").as_deref(), Some("ws://127.0.0.1:3002/node") ); assert_eq!( gateway_client_endpoint_from_node("ws://127.0.0.1:3000/node").as_deref(), Some("ws://127.0.0.1:3000/ws") ); }
    #[test] fn node_identity_proof_is_bound_to_gateway_challenge_and_device() { let proof = node_identity_proof("nonce-a", "device-a", "identity-a", "device-a"); assert_eq!(proof, "og.node.challenge.v1\nnonce-a\ndevice-a\nidentity-a\ndevice-a"); assert_ne!(proof, node_identity_proof("nonce-b", "device-a", "identity-a", "device-a")); assert_ne!(proof, node_identity_proof("nonce-a", "device-b", "identity-a", "device-b")); }
    #[test] fn fabric_device_verification_requires_known_active_exact_key() { let engine = app_bootstrap_test_engine(); let inserted = execute_request(&engine, QueryRequest::new(1, r#"on _devices | insert {identityId: "identity-a", deviceId: "device-a", publicKey: "key-a", state: "active"}"#)); assert!(inserted.is_ok()); assert!(registered_device_matches(&engine, "identity-a", "device-a", "key-a")); assert!(!registered_device_matches(&engine, "identity-a", "device-a", "key-b")); assert!(!registered_device_matches(&engine, "identity-a", "device-unknown", "key-a")); let revoked = execute_request(&engine, QueryRequest::new(2, r#"on _devices | where deviceId == "device-a" | set state = "revoked""#)); assert!(revoked.is_ok()); assert!(!registered_device_matches(&engine, "identity-a", "device-a", "key-a")); }
    #[test] fn gateway_device_registration_is_idempotent_and_exact() { let engine = app_bootstrap_test_engine(); ensure_gateway_device_registration(&engine, "gateway-identity", "gateway-device", "gateway-key") .expect("Gateway enrollment persists"); ensure_gateway_device_registration(&engine, "gateway-identity", "gateway-device", "gateway-key") .expect("Gateway enrollment is idempotent"); assert!(registered_identity_matches(&engine, "gateway-identity", "gateway-key")); assert!(registered_device_matches(&engine, "gateway-identity", "gateway-device", "gateway-key")); assert!(!registered_device_matches(&engine, "gateway-identity", "gateway-device", "other-key")); }
    #[cfg(feature = "fabric")] #[test] fn gateway_enrollment_hmac_matches_sha256_reference_vector() { assert_eq!( hmac_sha256_base64(b"key", b"The quick brown fox jumps over the lazy dog"), "97yD9DBThCSxMpjmqm+xQ+9NWaFJRhdZl0edvC0aPNg=" ); }
    #[test] fn app_bootstrap_with_empty_manifest_is_a_noop() { let engine = app_bootstrap_test_engine(); bootstrap_apps(&engine, &[]).expect("empty App manifest bootstraps"); assert!(!app_record_exists(&engine, RequestId::Number(1), "system.files") .expect("App lookup succeeds")); }
    #[test] fn app_bootstrap_reconciles_managed_apps_without_touching_unmanaged_apps() { let engine = app_bootstrap_test_engine(); let v1 = BuiltinApp { app_id: "system.test".to_owned(), name: "System test".to_owned(), version: "1.0.0".to_owned(), definition: serde_json::json!({ "id": "system.test", "name": "System test", "version": "1.0.0" }), }; bootstrap_apps(&engine, std::slice::from_ref(&v1)).expect("initial App bootstrap succeeds"); let drifted = execute_request( &engine, QueryRequest::new( 2, r#"on _apps | where appId == "system.test" | set name = "Drifted", version = "0.9.0", state = "deleted""#, ), ); assert!(drifted.is_ok()); let custom = execute_request( &engine, QueryRequest::new( 3, r#"on _apps | insert {appId: "custom.test", name: "Custom", version: "7.0.0", definition: {id: "custom.test", name: "Custom", version: "7.0.0"}, createdBy: "user", state: "active", createdAt: 1}"#, ), ); assert!(custom.is_ok()); let v2 = BuiltinApp { app_id: "system.test".to_owned(), name: "System test v2".to_owned(), version: "2.0.0".to_owned(), definition: serde_json::json!({ "id": "system.test", "name": "System test v2", "version": "2.0.0" }), }; bootstrap_apps(&engine, std::slice::from_ref(&v2)).expect("App reconciliation succeeds"); let response = execute_request( &engine, QueryRequest::new(4, r#"on _apps | where appId == "system.test" | limit 1"#), ); let QueryResponse::Ok { documents, .. } = response else { panic!("managed App lookup must succeed"); }; let managed = documents.first().expect("managed App exists"); assert_eq!(managed.str_field("state"), Some("active")); assert_eq!(managed.str_field("name"), Some("System test v2")); assert_eq!(managed.str_field("version"), Some("2.0.0")); assert_eq!(managed.str_field("updatedBy"), Some("system")); assert_eq!(managed.get("definition"), Some(&v2.definition)); let response = execute_request( &engine, QueryRequest::new(5, r#"on _apps | where appId == "custom.test" | limit 1"#), ); let QueryResponse::Ok { documents, .. } = response else { panic!("custom App lookup must succeed"); }; let unmanaged = documents.first().expect("custom App exists"); assert_eq!(unmanaged.str_field("state"), Some("active")); assert_eq!(unmanaged.str_field("name"), Some("Custom")); assert_eq!(unmanaged.str_field("version"), Some("7.0.0")); assert_eq!(unmanaged.str_field("createdBy"), Some("user")); assert!(unmanaged.get("updatedBy").is_none()); }
    #[test] fn place_scoped_collection_listing_counts_again_after_a_write_of_the_place() { let engine = app_bootstrap_test_engine(); let insert = |id: u64, place: &str| assert!(execute_request(&engine, QueryRequest::new(id, quoted!("on items | insert {{_place: {}, name: \"item\"}}", place))).is_ok()); for id in 0..3 { insert(id, "place-a"); } insert(9, "place-b"); let documents = |place: &str| list_storage_collections(&engine, true, Some(place), None).unwrap().first().and_then(|item| item.u64_field("documents")); assert_eq!(documents("place-a"), Some(3)); assert_eq!(documents("place-a"), Some(3)); insert(10, "place-a"); assert_eq!(documents("place-a"), Some(4)); assert_eq!(documents("place-b"), Some(1)); assert!(list_storage_collections(&engine, true, Some("place-c"), None).unwrap().is_empty()); let names = list_storage_collections(&engine, false, Some("place-b"), None).unwrap(); assert_eq!(names.len(), 1); assert_eq!(names[0].str_field("name"), Some("items")); assert!(names[0].get("documents").is_none()); }
    /// `cargo test --release place_listing_cost -- --ignored --nocapture`: the cost of a Place listing on Glacier.
    #[test] #[ignore] fn place_listing_cost() { let directory = std::env::temp_dir().join(format!("og-place-listing-{}", std::process::id())); let _ = std::fs::remove_dir_all(&directory); std::fs::create_dir_all(&directory).unwrap(); let storage = Arc::new(GlacierStorage::open(directory.join("store.glacier")).unwrap()); let collection = CollectionId::parse("finance").unwrap(); let generator = UuidV7Generator::new(); for chunk in 0..4 { let mut transaction = storage.begin().unwrap(); for row in 0..50_000u64 { let mut document = crate::Document::new(); document.insert("_place", if row % 4 == 0 { "other" } else { "place-a" }); for field in 0..20 { document.insert(format!("f{field}").as_str(), format!("valeur {field} {row} {chunk}").as_str()); } document.insert("amount", (row as i64) * 3); transaction.insert(&collection, generator.next_id(), Arc::new(document)).unwrap(); } transaction.commit().unwrap(); } let engine = Engine::new(storage.clone()); let started = Instant::now(); let mut old = 0u64; let expected = Value::from("place-a"); engine.storage().read().unwrap().scan_each(&collection, ScanOptions::default(), &mut |stored| { if stored.document().get(PLACE_SCOPE_FIELD) == Some(&expected) { old += 1; } Ok(true) }).unwrap(); let old_time = started.elapsed(); let started = Instant::now(); let first = place_documents(&engine, "finance", "place-a", None, true).unwrap(); let new_time = started.elapsed(); let started = Instant::now(); let again = place_documents(&engine, "finance", "place-a", None, true).unwrap(); let cached_time = started.elapsed(); eprintln!("old scan: {old} in {old_time:?}; scoped count: {first} in {new_time:?}; again: {again} in {cached_time:?}"); assert_eq!((old, first, again), (150_000, 150_000, 150_000)); drop(engine); drop(storage); let _ = std::fs::remove_dir_all(&directory); }
    #[test] fn place_scoped_collection_listing_filters_and_counts_by_place() { let engine = app_bootstrap_test_engine(); for (id, collection, place_id) in [ (20, "shared_items", "place-a"), (21, "shared_items", "place-a"), (22, "shared_items", "place-b"), (23, "other_items", "place-b"), ] { let response = execute_request( &engine, QueryRequest::new( id, quoted!("on {collection} | insert {{_place: {}, name: \"item\"}}", place_id), ), ); assert!(response.is_ok()); } let collections = list_storage_collections(&engine, true, Some("place-a"), None) .expect("Place collections list succeeds"); assert_eq!(collections.len(), 1); assert_eq!(collections[0].str_field("name"), Some("shared_items")); assert_eq!(collections[0].u64_field("documents"), Some(2)); }
    #[test] fn organisation_import_flags_unknown_parents_and_levels_before_writing() { let engine = app_bootstrap_test_engine(); bootstrap_all_apps(&engine).expect("base Apps bootstrap"); for query in ["on _places | insert {placeId: \"p\", name: \"P\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 1}", "on _app_instances | insert {instanceId: \"i\", placeId: \"p\", appId: \"system.organisation\", state: \"active\", createdAt: 2}", "on org_levels | insert {_place: \"p\", _app_instance: \"i\", code: \"societe\", rank: 1}", "on org_levels | insert {_place: \"p\", _app_instance: \"i\", code: \"site\", rank: 2}", "on org | insert {_place: \"p\", _app_instance: \"i\", code: \"S1\", level: \"societe\", parent: \"\"}"] { query_run(&engine, 1, query).unwrap(); } let call = |input: serde_json::Value| { let mut input = input; for (key, value) in [("placeId", "p"), ("targetInstanceId", "i"), ("table", "org")] { input[key] = value.into(); } super::place_ops::data_import(&engine, &identity("owner"), None, RequestId::Number(2), serde_json::from_value(input).unwrap(), |_| unreachable!()).map_err(|stop| match stop { super::serve::Stop::Reply(response) => format!("{response:?}"), super::serve::Stop::Connection(_) => String::new() }) }; let rows = serde_json::json!({"documents": [{"code": "L1", "level": "site", "parent": "S1"}, {"code": "L2", "level": "site", "parent": "L1"}, {"code": "L3", "level": "site", "parent": "S9"}, {"code": "S2", "level": "region", "parent": ""}]}); let first = call(serde_json::json!({"workerResult": rows, "mode": "update"})).unwrap(); let flagged = |id: &str| first["checks"].as_array().unwrap().iter().find(|check| check["id"] == id).map(|check| check["rows"].as_array().unwrap().iter().map(|row| row["code"].clone()).collect::<Vec<_>>()); assert_eq!(first["status"], "confirm"); assert_eq!(flagged("parents"), Some(vec![serde_json::json!("L3")])); assert_eq!(flagged("levels"), Some(vec![serde_json::json!("S2")])); }

    // Whatever stops the data worker (killed after a batch, a crash without output, an answer that is not JSON, a
    // command that does not start) fails the request with an error, and the Core goes on.
    #[test] fn a_data_worker_that_dies_fails_its_request_not_the_core() { let error = |stop: super::serve::Stop| match stop { super::serve::Stop::Reply(QueryResponse::Error { error, .. }) => (error.code, error.message), _ => panic!("not a reply") }; let sh = |script: &str| { let mut command = std::process::Command::new("sh"); command.args(["-c", script]); command }; let (import, mut batches) = (serde_json::json!({"operation": "import", "file": "x", "batch": true}), Vec::new()); let killed = super::file_ops::data_worker_import(RequestId::Number(1), &mut sh(r#"cat >/dev/null; echo '{"documents":[{"a":1}]}'; kill -9 $$"#), &import, &mut |documents| { batches.push(documents); Ok(()) }).map_err(error).unwrap_err(); assert_eq!((killed.0.as_str(), killed.1.contains("signal: 9"), batches.len()), ("worker.failed", true, 1)); let crashed = super::file_ops::data_worker_import(RequestId::Number(1), &mut sh("cat >/dev/null; echo 'thread panicked: out of memory' >&2; exit 101"), &import, &mut |_| Ok(())).map_err(error).unwrap_err(); assert_eq!((crashed.0.as_str(), crashed.1.contains("out of memory")), ("worker.failed", true)); let garbage = super::file_ops::data_worker_import(RequestId::Number(1), &mut sh("cat >/dev/null; echo '{not json'"), &import, &mut |_| Ok(())).map_err(error).unwrap_err(); assert_eq!(garbage.0, "worker.protocol_error"); let analysis = serde_json::json!({"operation": "analyze", "file": "x"}); for (script, code) in [("cat >/dev/null; kill -9 $$", "worker.failed"), ("cat >/dev/null; printf '{\"status\":\"analy'", "worker.protocol_error"), ("exit 0", "worker.protocol_error")] { assert_eq!(super::file_ops::data_worker_result(RequestId::Number(1), &mut sh(script), &analysis).map_err(|response| error(response.into())).unwrap_err().0, code, "{script}"); } assert_eq!(super::file_ops::data_worker_result(RequestId::Number(1), &mut std::process::Command::new("/nonexistent/worker"), &analysis).map_err(|response| error(response.into())).unwrap_err().0, "worker.unavailable"); }
    #[test] fn a_batched_import_stages_each_batch_then_writes_them_all_on_resume() { let engine = app_bootstrap_test_engine(); bootstrap_all_apps(&engine).expect("base Apps bootstrap"); for query in ["on _places | insert {placeId: \"p\", name: \"P\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 1}".to_owned(), insert_query("_apps", serde_json::json!({"appId": "app.l", "state": "active", "definition": {"model": {"tables": {"t": {"collection": "l"}}, "collections": {"l": "l"}}}})), "on _app_instances | insert {instanceId: \"i\", placeId: \"p\", appId: \"app.l\", state: \"active\", createdAt: 2}".to_owned()] { query_run(&engine, 1, query).unwrap(); } let call = |mut input: serde_json::Value| { for (key, value) in [("placeId", "p"), ("targetInstanceId", "i"), ("table", "t")] { input[key] = value.into(); } super::place_ops::data_import(&engine, &identity("owner"), None, RequestId::Number(2), serde_json::from_value(input).unwrap(), |_| unreachable!()).unwrap_or_else(|_| panic!("data.import refused")) }; let count = |collection: &str| query_documents(&engine, 3, format!("on {collection} | count as n")).unwrap()[0]["n"].clone(); let first = call(serde_json::json!({"documents": [{"k": 1}]})); assert_eq!((first["status"].clone(), first["rows"].clone()), (serde_json::json!("staged"), serde_json::json!(1))); let import = first["importId"].clone(); assert_eq!(call(serde_json::json!({"documents": [{"k": 2}, {"k": 3}], "importId": import}))["rows"], 2); assert_eq!((count("l.staged"), count("l")), (serde_json::json!(3), serde_json::json!(0))); let done = call(serde_json::json!({"importId": import, "warnings": [{"row": 2, "message": "w"}]})); assert_eq!((done["status"].clone(), done["rows"].clone(), done["warnings"].clone(), done["collection"].clone()), (serde_json::json!("completed"), serde_json::json!(3), serde_json::json!([{"row": 2, "message": "w"}]), serde_json::json!("l"))); assert_eq!((count("l.staged"), count("l")), (serde_json::json!(0), serde_json::json!(3))); }
    #[test] fn fabric_imports_run_on_the_database_core_under_the_gateway_delegation() { let engine = app_bootstrap_test_engine(); bootstrap_all_apps(&engine).expect("base Apps bootstrap"); let declared = serde_json::json!({"checks": [{"id": "big", "message": "big", "query": "on l.staged | where import_id == $import and v > 5"}], "replace": ["k"], "after": ["on l.staged | where import_id == $import | select k | into l_log"]}); for query in ["on _places | insert {placeId: \"p\", name: \"P\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 1}".to_owned(), insert_query("_apps", serde_json::json!({"appId": "app.l", "state": "active", "definition": {"model": {"tables": {"t": {"collection": "l", "import": declared}}, "collections": {"l": "l"}}}})), "on _app_instances | insert {instanceId: \"i\", placeId: \"p\", appId: \"app.l\", state: \"active\", createdAt: 2}".to_owned(), "on l | insert {_place: \"p\", _app_instance: \"i\", k: 1, v: 0}".to_owned()] { query_run(&engine, 1, query).unwrap(); } let rows = serde_json::json!({"documents": [{"k": 1, "v": 9}, {"k": 2, "v": 1}]}); let call = |delegation: Option<&GatewayDelegation>, mut input: serde_json::Value| { for (key, value) in [("placeId", "p"), ("targetInstanceId", "i"), ("table", "t")] { input[key] = value.into(); } super::place_ops::data_import(&engine, &identity("owner"), delegation, RequestId::Number(2), serde_json::from_value(input).unwrap(), |_| unreachable!()).map_err(|stop| match stop { super::serve::Stop::Reply(response) => format!("{response:?}"), super::serve::Stop::Connection(_) => String::new() }) }; let plan = call(None, serde_json::json!({"workerResult": rows, "planOnly": true})).unwrap(); let bare = call(None, serde_json::json!({"planOnly": true})).unwrap(); assert_eq!((bare["collection"].clone(), bare["rows"].clone()), (serde_json::json!("l"), serde_json::Value::Null)); assert_eq!((plan["status"].clone(), plan["collection"].clone(), plan["rows"].clone(), plan["import"].clone()), (serde_json::json!("planned"), serde_json::json!("l"), serde_json::json!(2), declared)); let planned = serde_json::json!({"workerResult": rows, "plan": plan}); assert!(call(None, planned.clone()).unwrap_err().contains("authorization.denied")); let delegation = |capability: &str, role| GatewayDelegation { filters: Vec::new(), principal: identity("gateway"), place_id: Some("p".into()), capability: capability.into(), app_instance_id: Some("i".into()), place_role: Some(role), public_access: None, token: None }; for refused in [delegation("files", PlaceRole::Owner), delegation("database", PlaceRole::Member)] { assert!(call(Some(&refused), planned.clone()).unwrap_err().contains("authorization.denied")); } let gateway = delegation("database", PlaceRole::Owner); let first = call(Some(&gateway), planned).unwrap(); assert_eq!((first["status"].clone(), first["checks"][0]["rows"].as_array().unwrap().len()), (serde_json::json!("confirm"), 1)); let done = call(Some(&gateway), serde_json::json!({"importId": first["importId"], "accept": ["big"], "plan": plan})).unwrap(); let rows = |query: &str| query_documents(&engine, 3, query).unwrap(); assert_eq!((done["status"].clone(), done["collection"].clone(), rows("on l | sort k | select k, v"), rows("on l_log | sort k | select k"), rows("on l.staged | count as n")), (serde_json::json!("completed"), serde_json::Value::Null, vec![serde_json::json!({"k": 1, "v": 9}), serde_json::json!({"k": 2, "v": 1})], vec![serde_json::json!({"k": 1}), serde_json::json!({"k": 2})], vec![serde_json::json!({"n": 0})])); }
    #[cfg(feature = "search")]
    #[test] fn place_search_finds_data_and_files_within_what_the_reader_sees() { let engine = app_bootstrap_test_engine(); let app = |id: &str, collection: &str, field: serde_json::Value, access: serde_json::Value| insert_query("_apps", serde_json::json!({"appId": id, "name": id, "state": "active", "definition": {"access": access, "model": {"collections": {"c": collection}, "tables": {collection: {"collection": "c", "label": collection, "fields": [field]}}}}})); for query in ["on _places | insert {placeId: \"p\", name: \"P\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 1}".to_owned(), app("app.fin", "comptes", serde_json::json!({"name": "label", "type": "text"}), serde_json::json!({"visibleTo": ["resident"]})), app("app.open", "ventes", serde_json::json!({"name": "client", "role": "axis"}), serde_json::json!({})), "on _app_instances | insert [{instanceId: \"i-fin\", placeId: \"p\", appId: \"app.fin\", state: \"active\", createdAt: 2}, {instanceId: \"i-open\", placeId: \"p\", appId: \"app.open\", state: \"active\", createdAt: 3}]".to_owned(), insert_query("_sharings", serde_json::json!({"sharingId": place_access_sharing_id("p", "m"), "owner": "owner", "target": "m", "permissions": [sharing_permission("p", PlaceRole::Member)], "placeRole": "member", "filters": {"entity": ["BU1"]}, "state": "active"})), "on comptes | insert [{_place: \"p\", _app_instance: \"i-fin\", label: \"T.V.A. collectée\", entity: \"BU1\"}, {_place: \"q\", _app_instance: \"i-q\", label: \"TVA ailleurs\"}]".to_owned(), "on ventes | insert [{_place: \"p\", _app_instance: \"i-open\", client: \"CASTORAMA\", entity: \"BU1\"}, {_place: \"p\", _app_instance: \"i-open\", client: \"CASTORAMA\", entity: \"BU1\"}, {_place: \"p\", _app_instance: \"i-open\", client: \"LEROY\", entity: \"BU2\"}]".to_owned(), "on _files | insert [{_place: \"p\", _app_instance: \"i-open\", fileId: \"d\", name: \"Compta\", kind: \"directory\"}, {_place: \"p\", _app_instance: \"i-open\", fileId: \"x\", parentId: \"d\", name: \"TVA 2022.pdf\", kind: \"file\"}, {_place: \"p\", _app_instance: \"i-open\", fileId: \"n\", parentId: \"d\", name: \"notes.md\", kind: \"file\", remoteId: \"notes.md\", size: 1}, {_place: \"q\", _app_instance: \"i-q\", fileId: \"y\", name: \"TVA.pdf\", kind: \"file\"}]".to_owned()] { query_run(&engine, 1, query).unwrap(); } let files = std::env::temp_dir().join(format!("og-place-search-{}", std::process::id())); let notes = files.join(file_scope_component("p")).join(file_scope_component("i-open")).join("notes.md"); std::fs::create_dir_all(notes.parent().unwrap()).unwrap(); std::fs::write(&notes, "Relevé des écritures de régularisation du trimestre.").unwrap(); let search = |who: &str, text: &str, scope: &[&str]| super::place_ops::place_search_for(&engine, Some(&files), RequestId::Number(2), &identity(who), None, &serde_json::from_value(serde_json::json!({"placeId": "p", "text": text, "scope": scope})).unwrap()).unwrap(); let found = |answer: &serde_json::Value| answer["results"].as_array().unwrap().iter().map(|hit| format!("{}:{}", hit["kind"].as_str().unwrap(), hit.get("path").and_then(|path| path.as_str()).unwrap_or_else(|| hit["text"].as_str().unwrap()))).collect::<Vec<_>>(); let first = search("owner", "tva", &[]); assert_eq!(found(&first), ["value:T.V.A. collectée", "file:Compta/TVA 2022.pdf"]); assert_eq!((first["partsRead"].clone(), search("owner", "tva", &[])["partsRead"].clone()), (serde_json::json!(5), serde_json::json!(1))); assert_eq!(found(&search("owner", "regularisation", &[])), ["passage:Compta/notes.md"]); std::fs::write(&notes, "Lettrage des comptes clients.").unwrap(); query_run(&engine, 1, "on _files | where fileId == \"n\" | set size = 2").unwrap(); let changed = search("owner", "lettrage", &[]); assert_eq!((found(&changed), changed["partsRead"].clone()), (vec!["passage:Compta/notes.md".to_owned()], serde_json::json!(3))); assert!(search("owner", "regularisation", &[])["results"].as_array().unwrap().is_empty()); assert_eq!(found(&search("owner", "castorma", &[])), ["value:CASTORAMA"]); assert_eq!(search("owner", "castorama", &[])["results"][0]["count"], 2); assert_eq!(found(&search("owner", "tva", &["folder:d"])), ["file:Compta/TVA 2022.pdf"]); assert_eq!(found(&search("m", "tva", &[])), ["file:Compta/TVA 2022.pdf"]); assert_eq!((found(&search("owner", "leroy", &[])), found(&search("m", "leroy", &[]))), (vec!["value:LEROY".to_owned()], vec![])); query_run(&engine, 1, "on ventes | insert {_place: \"p\", _app_instance: \"i-open\", client: \"LEROY MERLIN\", entity: \"BU1\"}").unwrap(); let again = search("m", "leroy", &[]); assert_eq!((found(&again), again["partsRead"].clone()), (vec!["value:LEROY MERLIN".to_owned()], serde_json::json!(2))); let gateway = GatewayDelegation { filters: vec![("entity".to_owned(), vec![crate::Value::from("BU1")])], principal: identity("m"), place_id: Some("p".into()), capability: "database".into(), app_instance_id: Some("i-open".into()), place_role: Some(PlaceRole::Member), public_access: None, token: None }; let through = |text: &str| super::place_ops::place_search_for(&engine, Some(&files), RequestId::Number(3), &identity("gateway"), Some(&gateway), &serde_json::from_value(serde_json::json!({"placeId": "p", "text": text})).unwrap()).unwrap(); assert_eq!((found(&through("leroy")), found(&through("castorama"))), (vec!["value:LEROY MERLIN".to_owned()], vec!["value:CASTORAMA".to_owned()])); let _ = std::fs::remove_dir_all(&files); }
    #[test] fn place_profiles_give_their_apps_and_strict_filters_to_reads_writes_and_views() { let engine = app_bootstrap_test_engine(); for query in [insert_query("_places", serde_json::json!({"placeId": "p", "name": "P", "mood": "", "ownerIdentityId": "owner", "state": "active", "createdAt": 1, "profiles": [{"profileId": "bu1", "name": "BU1", "apps": ["i-v"], "filters": {"entity": ["BU1"]}}]})), insert_query("_apps", serde_json::json!({"appId": "app.v", "name": "V", "state": "active", "definition": {"model": {"collections": {"v": "ventes"}, "tables": {"ventes": {"collection": "v", "fields": [{"name": "entity", "role": "axis"}]}}}}})), "on _app_instances | insert [{instanceId: \"i-v\", placeId: \"p\", appId: \"app.v\", state: \"active\", createdAt: 2}, {instanceId: \"i-o\", placeId: \"p\", appId: \"app.open\", state: \"active\", createdAt: 3}]".to_owned(), insert_query("_sharings", serde_json::json!({"sharingId": place_access_sharing_id("p", "m"), "owner": "owner", "target": "m", "permissions": [sharing_permission("p", PlaceRole::Resident)], "placeRole": "resident", "profileId": "bu1", "state": "active"})), "on ventes | insert [{_place: \"p\", entity: \"BU1\", v: 1}, {_place: \"p\", entity: \"BU2\", v: 2}, {_place: \"p\", v: 3}]".to_owned(), "on notes | insert [{_place: \"p\", entity: \"BU2\", v: 4}, {_place: \"p\", v: 5}]".to_owned()] { query_run(&engine, 1, query).unwrap(); } let context = |who: &str, instance: Option<&str>, as_profile: Option<&str>| resolve_query_execution_context(&engine, RequestId::Number(2), &identity(who), RequestedExecutionContext { place_id: "p".into(), app_instance_id: instance.map(str::to_owned), as_profile: as_profile.map(str::to_owned) }); let run = |context: &ExecutionContext, query: &str| { let prepared = crate::view::prepare(&engine, Some(&context.scope()), query, &serde_json::Map::new()).unwrap(); match execute_request_scoped(&engine, QueryRequest::new(3, prepared), Some(context)) { QueryResponse::Ok { documents, .. } => Ok(documents.iter().map(|document| document["v"].as_i64().unwrap_or(-1)).collect::<Vec<_>>()), QueryResponse::Error { error, .. } => Err(format!("{error:?}")), other => panic!("{other:?}") } }; let (member, owner, preview) = (context("m", None, None).unwrap(), context("owner", None, None).unwrap(), context("owner", None, Some("bu1")).unwrap()); assert_eq!(member.filters, [("entity".to_owned(), vec![crate::Value::from("BU1")]), ("ventes:entity".to_owned(), vec![crate::Value::from("BU1")])]); assert_eq!(preview.filters, member.filters); assert!(owner.filters.is_empty() && context("m", None, Some("none")).is_ok()); for context in [&member, &preview] { assert_eq!((run(context, "on ventes | sort v"), run(context, "on notes | sort v")), (Ok(vec![1]), Ok(vec![5]))); } assert_eq!(run(&owner, "on ventes | sort v"), Ok(vec![1, 2, 3])); assert_eq!([("m", "i-v", None), ("m", "i-o", None), ("owner", "i-o", None), ("owner", "i-o", Some("bu1"))].map(|(who, instance, as_profile)| context(who, Some(instance), as_profile).is_ok()), [true, false, true, false]); for refused in ["on ventes | insert {entity: \"BU2\", v: 6}", "on ventes | insert {v: 6}", "on ventes | where v == 1 | set entity = \"BU2\"", "on notes | insert {entity: \"BU2\", v: 6}"] { assert!(run(&member, refused).is_err_and(|error| error.contains("outside your access")), "{refused}"); } assert_eq!(run(&member, "on notes | insert {v: 7}"), Ok(vec![7])); crate::view::deliver(&engine, "app.v", &[serde_json::json!({"name": "tot", "query": "on ventes | group | by \"all\" as k | sum v as v | end", "materialized": true})]).unwrap(); assert_eq!((run(&member, "on view tot"), run(&owner, "on view tot"), run(&member, "on view tot")), (Ok(vec![1]), Ok(vec![6]), Ok(vec![1]))); }
    #[test] fn place_access_filters_restrict_every_read_of_a_member() { let engine = app_bootstrap_test_engine(); for query in ["on _places | insert {placeId: \"p\", name: \"P\", mood: \"\", ownerIdentityId: \"owner\", state: \"active\", createdAt: 1}".to_owned(), insert_query("_sharings", serde_json::json!({"sharingId": place_access_sharing_id("p", "m"), "owner": "owner", "target": "m", "permissions": [sharing_permission("p", PlaceRole::Member)], "placeRole": "member", "filters": {"entity": ["BU1"]}, "state": "active"})), "on finance | insert [{_place: \"p\", entity: \"BU1\", v: 1}, {_place: \"p\", entity: \"BU2\", v: 2}, {_place: \"p\", v: 3}, {_place: \"q\", entity: \"BU1\", v: 4}]".to_owned()] { query_run(&engine, 1, query).unwrap(); } let context = |identity_id: &str| resolve_query_execution_context(&engine, RequestId::Number(2), &identity(identity_id), RequestedExecutionContext { place_id: "p".into(), app_instance_id: None, as_profile: None }).unwrap(); let values = |context: &ExecutionContext, query: &str| { let prepared = crate::view::prepare(&engine, Some(&context.scope()), query, &serde_json::Map::new()).unwrap(); match execute_request_scoped(&engine, QueryRequest::new(3, prepared), Some(context)) { QueryResponse::Ok { documents, .. } => documents.iter().map(|document| document["v"].as_i64().unwrap()).collect::<Vec<_>>(), other => panic!("{other:?}") } }; let (member, owner) = (context("m"), context("owner")); assert_eq!(member.filters, [("entity".to_owned(), vec![crate::Value::from("BU1")])]); assert_eq!(values(&member, "on finance | sort v"), [1, 3]); assert_eq!(values(&owner, "on finance | sort v"), [1, 2, 3]); assert_eq!(values(&member, "on finance | union | on finance | end | lookup finance as o on v == v into same | where len(same) == 1 | sort v"), [1, 1, 3, 3]); assert_eq!(values(&member, "let $all = (on finance | count as n) | on finance | derive v = get(at($all, 0), \"n\") | limit 1"), [2]); assert!(crate::access::place::access_filters(&serde_json::json!({"entity or 1": ["x"]})).is_err() && crate::access::place::access_filters(&serde_json::json!({"entity": "x"})).is_err()); }
    #[cfg(feature = "database")] #[test] fn a_place_read_keeps_its_result_key_until_its_place_writes_what_it_reads() { let engine = app_bootstrap_test_engine(); let context = |place_id: &str, filters: Vec<(String, Vec<Value>)>| ExecutionContext { principal: Principal::Anonymous, place_id: place_id.to_owned(), app_instance_id: None, access: PlaceAccess::Role(PlaceRole::Owner), filters }; let write = |place_id: &str, query: &str| assert!(execute_request_scoped(&engine, QueryRequest::new(1, query), Some(&context(place_id, Vec::new()))).is_ok()); write("p", "on ledger | insert {v: 1}"); let key = |query: &str| super::query_ops::result_key(&engine, query, &context("p", Vec::new())); let read = "on ledger | lookup notes as n on v == v into same | count"; let first = key(read).unwrap(); assert_eq!(key(read), Some(first.clone())); write("q", "on ledger | insert {v: 2}"); write("q", "on notes | insert {v: 2}"); assert_eq!(key(read), Some(first.clone())); write("p", "on notes | insert {v: 1}"); let second = key(read).unwrap(); assert_ne!(first, second); assert_ne!(super::query_ops::result_key(&engine, read, &context("p", vec![("entity".to_owned(), vec![Value::from("BU1")])])), Some(second)); assert_eq!(key("on ledger | insert {v: 3}"), None); assert_eq!(key("on ledger | where v < now()"), None); }
    #[test] fn place_only_query_scope_spans_instances_without_leaking_other_places() { let engine = app_bootstrap_test_engine(); let context = |place_id: &str, app_instance_id: Option<&str>| ExecutionContext { principal: Principal::Anonymous, place_id: place_id.to_owned(), app_instance_id: app_instance_id.map(str::to_owned), access: PlaceAccess::Role(PlaceRole::Owner), filters: Vec::new() }; for (id, place_id, app_instance_id, name) in [ (10, "place-a", "app-1", "one"), (11, "place-a", "app-2", "two"), (12, "place-b", "app-3", "other"), ] { let response = execute_request_scoped( &engine, QueryRequest::new(id, quoted!("on studio_scope_test | insert {{name: {}}}", name)), Some(&context(place_id, Some(app_instance_id))), ); assert!(response.is_ok()); } let place_response = execute_request_scoped( &engine, QueryRequest::new(13, "on studio_scope_test | sort name"), Some(&context("place-a", None)), ); let QueryResponse::Ok { documents, .. } = place_response else { panic!("Place scoped query must succeed"); }; assert_eq!(documents.len(), 2); assert_eq!(documents[0].str_field("name"), Some("one")); assert_eq!(documents[1].str_field("name"), Some("two")); assert!(documents.iter().all(|document| document.str_field("_place") == Some("place-a"))); let app_response = execute_request_scoped( &engine, QueryRequest::new(14, "on studio_scope_test | sort name"), Some(&context("place-a", Some("app-1"))), ); let QueryResponse::Ok { documents, .. } = app_response else { panic!("AppInstance scoped query must succeed"); }; assert_eq!(documents.len(), 1); assert_eq!(documents[0].str_field("name"), Some("one")); }
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
