//! Core, Fabric, LLM, Agent and database administration requests.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;
use super::serve::{diverged, ensure_operation_resource_authorized, failed, reply, require_identity, routing_error, Handled, Stop};

pub(super) fn handle_core_operation(writer: &mut TcpStream, settings: &ConnectionSettings, engine: Option<&Engine>, authentication: &ConnectionAuth, operation: RoutedOperation) -> Handled {
    match operation {
        RoutedOperation::CoreHealth(Routed { id, .. }) => {
            reply(writer, id, serde_json::json!({ "healthy": true, "version": PROTOCOL_VERSION, "authRequired": settings.authorization_mode.is_enforced(), "classicAuthEnabled": settings.classic_auth_enabled, "capabilities": settings.service_capabilities.names(), "publishedCapabilities": settings.published_capabilities.names(), "compiledCapabilities": ServiceCapabilities::compiled().names(), "buildFeatures": build_profile::feature_names(), }))?;
        }
        RoutedOperation::CoreDiscover(Routed { id, .. }) => {
            // Public hello: the Core options a client needs before choosing its entry flow.
            reply(writer, id, serde_json::json!({ "version": PROTOCOL_VERSION, "authRequired": settings.authorization_mode.is_enforced(), "enrollmentMode": settings.enrollment_mode.as_str(), "classicAuthEnabled": settings.classic_auth_enabled, "capabilities": settings.service_capabilities.names(), "publishedCapabilities": settings.published_capabilities.names(), }))?;
        }
        RoutedOperation::CoreOperations(Routed { id, .. }) => {
            let operations = OPERATION_CATALOG
                .iter()
                .filter(|operation| operation.kind.is_compiled() && settings.service_capabilities.contains_all(operation.kind.required_capabilities()))
                .map(|operation| serde_json::json!({
                    "name": operation.name,
                    "capability": operation.kind.provider_capability().map(ServiceCapability::as_str),
                    "access": operation.access.as_str(),
                    "transport": operation.transport.as_str(),
                    "connection": operation.connection.as_str(),
                }))
                .collect::<Vec<_>>();
            reply(writer, id, serde_json::json!({ "operations": operations }))?;
        }
        RoutedOperation::NodeStatus(Routed { id, .. }) => {
            let state = *lock_unpoisoned(&settings.gateway_state);
            reply(writer, id, serde_json::json!({ "upstream": settings.upstream.as_deref(), "state": state, "capabilities": settings.service_capabilities.names(), "publishedCapabilities": settings.published_capabilities.names(), "compiledCapabilities": ServiceCapabilities::compiled().names(), "buildFeatures": build_profile::feature_names(), }))?;
        }
        RoutedOperation::Ping(Routed { id, .. }) => {
            reply(writer, id, serde_json::json!({ "data": "pong", "version": PROTOCOL_VERSION, }))?;
        }
        #[cfg(feature = "db-engine")]
        operation @ (RoutedOperation::FabricResourceList(_) | RoutedOperation::FabricResourceSet(_) | RoutedOperation::FabricResourceRemove(_)) =>
            return handle_fabric_resource_operation(writer, settings, engine.expect("database-backed operation routed without db-engine"), authentication, operation),
        other => return Err(routing_error(other.id(), "core operation routed without its compiled implementation").into()),
    }
    Ok(())
}

#[cfg(feature = "db-engine")]
fn handle_fabric_resource_operation(writer: &mut TcpStream, settings: &ConnectionSettings, engine: &Engine, authentication: &ConnectionAuth, operation: RoutedOperation) -> Handled {
    let principal = authentication.principal();
    let (id, assignments, capability) = match operation {
        RoutedOperation::FabricResourceList(Routed { id, .. }) => {
            require_identity(principal, id, "an authenticated identity is required to inspect Fabric defaults")?;
            let assignments = load_fabric_defaults(engine, id)?;
            let eligible_devices = active_devices_for_assignments(engine, id, &assignments);
            let can_manage = !settings.authorization_mode.is_enforced()
                || authorize_connection(engine, principal, AuthorizationAction::FabricManage, "_fabric_resources");
            reply(writer, id, serde_json::json!({
                "fabricId": FABRIC_DEFAULTS_ID,
                "assignments": assignments,
                "eligibleDevices": eligible_devices,
                "canManage": can_manage,
            }))?;
            return Ok(());
        }
        RoutedOperation::FabricResourceSet(Routed { id, input: FabricResourceSetInput { identity_id: node_identity_id, device_id: node_device_id, capability, role, service_role, storage_role } }) => {
            let assigned_by = require_identity(principal, id, "an authenticated identity is required to manage Fabric defaults")?;
            if !valid_resource_capability(&capability) {
                reject!(id, "fabric.resource.invalid_capability", "invalid resource capability");
            }
            if capability == "auth" {
                reject!(id, "fabric.resource.auth_reserved", "auth authorities are managed by Fabric trust and cannot be configured as a resource default");
            }
            let _node_device = load_device_credential(engine, id, &node_identity_id, &node_device_id)?;
            let current = load_fabric_defaults(engine, id)?;
            let assignments = upsert_resource_assignment(
                current, assigned_by, &node_identity_id, &node_device_id, &capability,
                role.as_deref(), service_role.as_deref(), storage_role.as_deref(),
            ).map_err(|error| super::serve::request_error(id, format!("fabric.{}", error.code), error.message))?;
            (id, assignments, capability)
        }
        RoutedOperation::FabricResourceRemove(Routed { id, input: FabricResourceRemoveInput { identity_id: node_identity_id, device_id: node_device_id, capability } }) => {
            require_identity(principal, id, "an authenticated identity is required to manage Fabric defaults")?;
            if capability == "auth" {
                reject!(id, "fabric.resource.auth_reserved", "auth authorities are managed by Fabric trust and cannot be configured as a resource default");
            }
            let current = load_fabric_defaults(engine, id)?;
            (id, remove_resource_assignment(current, &node_identity_id, &node_device_id, &capability), capability)
        }
        other => return diverged(other.id()),
    };
    save_fabric_defaults(engine, id, &assignments)?;
    publish_durable_event(engine, Audience::Global, "fabric.resources.updated", serde_json::json!({"fabricId": FABRIC_DEFAULTS_ID, "capability": capability}));
    Ok(reply(writer, id, serde_json::json!({ "fabricId": FABRIC_DEFAULTS_ID, "assignments": assignments }))?)
}

/// Streams `result` to its end: the terminal frame, or the error of a failed run.
/// A write failure met while streaming wins over the run's own outcome.
#[cfg(any(feature = "llm", feature = "agent"))]
fn finish_stream<T, E: fmt::Display>(writer: &mut TcpStream, id: RequestId, stream_error: Option<ConnectionError>, result: Result<T, E>, code: impl FnOnce(&E) -> String, complete: impl FnOnce(T) -> JsonValue) -> Handled {
    if let Some(error) = stream_error { return Err(error.into()); }
    let response = match result {
        Ok(value) => StreamResponse::complete(id, Some(complete(value))),
        Err(error) => StreamResponse::error(Some(id), WireError::new(code(&error), error.to_string())),
    };
    Ok(write_stream_response(writer, &response)?)
}

/// Writes one partial frame from inside a streaming callback, keeping the first failure.
#[cfg(any(feature = "llm", feature = "agent"))]
fn stream_partial(writer: &mut TcpStream, id: RequestId, partial: JsonValue, stream_error: &mut Option<ConnectionError>) -> bool {
    match write_stream_response(writer, &StreamResponse::partial(id, partial)) {
        Ok(()) => true,
        Err(error) => {
            *stream_error = Some(error);
            false
        }
    }
}

#[cfg(feature = "llm")]
pub(super) fn handle_llm_operation(writer: &mut TcpStream, settings: &ConnectionSettings, authentication: &ConnectionAuth, delegation: Option<&GatewayDelegation>, operation: RoutedOperation) -> Handled {
    let service = settings.llm_service.as_ref()
        .expect("llm operation routed without an enabled llm provider");
    match operation {
        RoutedOperation::LlmStatus(Routed { id, .. }) => {
            reply(writer, id, serde_json::to_value(service.status()).expect("llm status serializes"))?;
        }
        RoutedOperation::LlmGenerate(Routed { id, input }) => {
            let owner = llm_run_owner(authentication, delegation);
            let mut run = match service.schedule(owner) {
                Ok(run) => run,
                Err(error) => {
                    write_stream_response(writer, &StreamResponse::error(Some(id), WireError::new(error.code(), error.to_string())))?;
                    return Ok(());
                }
            };
            let run_id = run.run_id();
            write_stream_response(writer, &StreamResponse::partial(id, serde_json::json!({
                "type": "run",
                "runId": run_id,
                "state": run.initial_state(),
            })))?;
            let mut stream_error = None;
            let result = service.generate(&mut run, &input, |event| {
                let partial = match event {
                    LlmGenerationEvent::Text(text) => serde_json::json!({
                        "type":"token",
                        "runId":run_id,
                        "text":text
                    }),
                    LlmGenerationEvent::ToolCall { id: tool_call_id, name, arguments } => serde_json::json!({
                        "type":"toolCall",
                        "runId":run_id,
                        "id":tool_call_id,
                        "name":name,
                        "arguments":arguments
                    }),
                };
                stream_partial(writer, id, partial, &mut stream_error)
            });
            return finish_stream(writer, id, stream_error, result, |error| error.code().to_owned(), |statistics| serde_json::json!({
                "runId": run_id,
                "promptTokens": statistics.prompt_tokens,
                "cachedPromptTokens": statistics.cached_prompt_tokens,
                "completionTokens": statistics.completion_tokens,
                "elapsedMs": statistics.elapsed_ms,
                "finishReason": statistics.finish_reason,
            }));
        }
        RoutedOperation::LlmCancel(Routed { id, input }) => {
            let owner = llm_run_owner(authentication, delegation);
            let cancelled = service.cancel(input.run_id, &owner);
            reply(writer, id, serde_json::json!({
                "runId": input.run_id,
                "cancelled": cancelled,
            }))?;
        }
        other => return diverged(other.id()),
    }
    Ok(())
}

#[cfg(feature = "agent")]
pub(super) fn handle_agent_operation(writer: &mut TcpStream, settings: &ConnectionSettings, engine: Option<&Engine>, authentication: &ConnectionAuth, delegation: Option<&GatewayDelegation>, operation: RoutedOperation) -> Handled {
    let service = settings.agent_service.as_ref()
        .expect("agent operation routed without an enabled agent provider");
    let (id, input) = match operation {
        RoutedOperation::AgentStatus(Routed { id, .. }) => {
            return Ok(reply(writer, id, serde_json::to_value(service.status()).expect("agent status serializes"))?);
        }
        RoutedOperation::AgentRun(Routed { id, input }) => (id, input),
        other => return diverged(other.id()),
    };
    let requested = &input.context;
    let execution_context = if let Some(delegated) = delegation {
        if delegated.capability != "agent"
            || delegated.place_id.as_deref() != Some(requested.place_id.as_str())
            || delegated.app_instance_id.as_deref() != requested.app_instance_id.as_deref()
        {
            reject!(id, "agent.context_mismatch", "agent.run context does not match the Gateway delegation");
        }
        let Some(place_role) = delegated.place_role else {
            reject!(id, "agent.context_missing_role", "agent.run requires a delegated Place role");
        };
        ExecutionContext {
            principal: delegated.principal.clone(),
            place_id: requested.place_id.clone(),
            app_instance_id: requested.app_instance_id.clone(),
            access: PlaceAccess::Role(place_role),
        }
    } else {
        let Some(engine) = engine else {
            reject!(id, "agent.context_unavailable", "standalone agent.run requires a local database engine to validate its execution context");
        };
        resolve_query_execution_context(engine, id, authentication.principal(), requested.clone())?
    };

    let mut invoker = if let Some(delegated) = delegation {
        let Some(token) = delegated.token.as_deref() else {
            reject!(id, "agent.gateway_delegation_missing", "Gateway did not provide an Agent sub-call delegation token");
        };
        #[cfg(feature = "fabric")]
        match GatewayAgentInvoker::connect(settings, token) {
            Ok(invoker) => OgdAgentInvoker::Gateway(invoker),
            Err(error) => {
                write_stream_response(writer, &StreamResponse::error(Some(id), WireError::new(error.code(), error.to_string())))?;
                return Ok(());
            }
        }
        #[cfg(not(feature = "fabric"))]
        return Err(routing_error(id, "Gateway-delegated agent.run requires the fabric feature").into());
    } else {
        let principal = match authentication.principal() {
            Principal::Anonymous => "anonymous".to_owned(),
            Principal::Identity { identity_id, device_id } => {
                format!("{identity_id}/{device_id}")
            }
        };
        let owner = format!(
            "agent:{principal}|{}|{}",
            execution_context.place_id,
            execution_context.app_instance_id.as_deref().unwrap_or(""),
        );
        OgdAgentInvoker::Local(LocalAgentInvoker {
            settings,
            engine,
            context: &execution_context,
            owner,
        })
    };

    write_stream_response(writer, &StreamResponse::partial(id, serde_json::json!({
        "type":"run",
        "state":"running",
        "stateless":true,
    })))?;
    let mut stream_error = None;
    let result = service.run(&input, &mut invoker, |event| stream_partial(writer, id, event, &mut stream_error));
    finish_stream(writer, id, stream_error, result, |error| error.code().to_owned(), |statistics| serde_json::to_value(statistics).expect("agent statistics serialize"))
}

#[cfg(feature = "database")]
pub(super) fn handle_database_operation(writer: &mut TcpStream, settings: &ConnectionSettings, engine: &Engine, authentication: &ConnectionAuth, delegation: Option<&GatewayDelegation>, operation: RoutedOperation) -> Handled {
    match operation {
        RoutedOperation::CollectionsList(Routed { id, input: CollectionsListInput { stats, place_id, app_instance_id } }) => {
            if let Some(place_id) = place_id.as_deref() {
                let delegated = delegation.is_some_and(|value| {
                    value.capability == "database"
                        && value.place_id.as_deref() == Some(place_id)
                        && value.app_instance_id.as_deref() == app_instance_id.as_deref()
                });
                if !delegated {
                    resolve_query_execution_context(
                        engine,
                        id,
                        authentication.principal(),
                        RequestedExecutionContext {
                            place_id: place_id.to_owned(),
                            app_instance_id: app_instance_id.clone(),
                        },
                    )?;
                }
            } else {
                ensure_operation_resource_authorized(settings, engine, authentication.principal(), id, OperationKind::CollectionsList, "*")?;
            }

            let snapshot = engine.storage().read().map_err(failed(id, "collections.failed"))?;
            let collections = list_storage_collections(
                snapshot.as_ref(), stats, place_id.as_deref(), app_instance_id.as_deref(),
            ).map_err(failed(id, "collections.failed"))?;
            reply(writer, id, serde_json::json!({"collections": collections}))?;
        }
        RoutedOperation::StorageStats(Routed { id, .. }) => {
            let data = if let Some(storage) = settings.glacier_storage.as_ref() {
                serde_json::json!({
                    "backend": "glacier",
                    "generation": storage.generation().unwrap_or_default(),
                    "documents": storage.document_count().unwrap_or_default(),
                    "storeBytes": storage.store_bytes().unwrap_or_default(),
                    "walBytes": storage.wal_bytes().unwrap_or_default(),
                    "startup": storage.startup_metrics(),
                    "read": storage.read_metrics(),
                    "resident": storage.resident_memory(),
                    "write": storage.write_metrics(),
                })
            } else {
                let snapshot = engine.storage().read().map_err(failed(id, "storage.stats_failed"))?;
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
            reply(writer, id, data)?;
        }
        RoutedOperation::BackupCreate(Routed { id, input: BackupNameInput { name } }) => {
            let path = backup_path(settings, id, &name)?;
            let summary = backup::create(engine.storage(), &path, backup_metadata(&settings.instance_id)).map_err(failed(id, "backup.create_failed"))?;
            reply(writer, id, serde_json::json!({"name": name, "collections": summary.collections, "documents": summary.documents}))?;
        }
        RoutedOperation::BackupInspect(Routed { id, input: BackupNameInput { name } }) => {
            let path = backup_path(settings, id, &name)?;
            let summary = backup::inspect(&path).map_err(failed(id, "backup.inspect_failed"))?;
            reply(writer, id, serde_json::json!({
                "name": name,
                "format": summary.format,
                "version": summary.version,
                "createdAt": summary.created_at,
                "sizeBytes": summary.size_bytes,
                "collections": summary.collections,
                "documents": summary.documents,
                "source": summary.source,
            }))?;
        }
        RoutedOperation::BackupRestore(Routed { id, input: BackupRestoreInput { name, replace } }) => {
            let path = backup_path(settings, id, &name)?;
            let summary = backup::restore(engine.storage(), &path, replace).map_err(failed(id, "backup.restore_failed"))?;
            let payload = serde_json::json!({"name": name, "replace": replace, "collections": summary.collections, "documents": summary.documents});
            publish_durable_event(engine, Audience::Global, "backup.restored", payload);
            reply(writer, id, serde_json::json!({"restored": true, "name": name, "collections": summary.collections, "documents": summary.documents}))?;
        }
        other => return diverged(other.id()),
    }
    Ok(())
}

#[cfg(feature = "database")]
fn backup_path(settings: &ConnectionSettings, id: RequestId, name: &str) -> Result<PathBuf, Stop> {
    backup_file_path(&settings.backup_path, name)
        .ok_or_else(|| super::serve::request_error(id, "backup.invalid_name", "backup name must be a simple file name"))
}
