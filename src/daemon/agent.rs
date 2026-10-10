//! Agent capability invokers: local calls, or sub-calls through the Gateway.
#![cfg_attr(rustfmt, rustfmt_skip)]

use super::*;

#[cfg(all(feature = "agent", feature = "fabric"))]
pub(super) fn connect_delegated_gateway_client( endpoint: &str, token: &str, read_timeout: Duration, write_timeout: Duration, ) -> Result<NodeWebSocket, AgentError> {
    let (mut websocket, _) = websocket_connect(endpoint)
        .map_err(|error| AgentError::capability("gateway.delegation.open", error.to_string()))?;
    configure_websocket_timeouts(&mut websocket, read_timeout, write_timeout)
        .map_err(|error| AgentError::capability("gateway.delegation.open", error.to_string()))?;

    loop {
        let Some(message) = read_gateway_message(&mut websocket)
            .map_err(|error| AgentError::capability("gateway.delegation.open", error))?
        else {
            return Err(AgentError::capability(
                "gateway.delegation.open",
                "Gateway closed before gateway.ready",
            ));
        };
        if message.str_field("kind") == Some("event")
            && message.str_field("type") == Some("gateway.ready")
        {
            break;
        }
    }

    let request = OperationRequest::new(
        RequestId::Number(1),
        "gateway.delegation.open",
        serde_json::json!({"token": token}),
    );
    let response = gateway_response_for_request(&mut websocket, &request)
        .map_err(|error| AgentError::capability("gateway.delegation.open", error))?;
    if let Some(error) = response.get("error") {
        return Err(AgentError::capability(
            "gateway.delegation.open",
            error.to_string(),
        ));
    }
    if response.str_field("status") != Some("ok") {
        return Err(AgentError::capability(
            "gateway.delegation.open",
            "Gateway did not accept the delegated Agent session",
        ));
    }
    Ok(websocket)
}

#[cfg(all(feature = "agent", feature = "fabric"))]
pub(super) fn connect_delegated_gateway_client_any( settings: &ConnectionSettings, token: &str, ) -> Result<NodeWebSocket, AgentError> {
    let endpoints = gateway_client_endpoints(settings);
    if endpoints.is_empty() {
        return Err(AgentError::capability(
            "gateway.delegation.open",
            "Gateway client endpoint is unavailable",
        ));
    }
    let mut last_error = None;
    for endpoint in endpoints {
        match connect_delegated_gateway_client(
            &endpoint,
            token,
            settings.agent_provider_read_timeout,
            settings.write_timeout,
        ) {
            Ok(websocket) => return Ok(websocket),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| AgentError::capability(
        "gateway.delegation.open",
        "No Gateway accepted the delegated Agent session",
    )))
}

#[cfg(feature = "agent")]
pub(super) struct LocalAgentInvoker<'a> {
    pub(super) settings: &'a ConnectionSettings,
    pub(super) engine: Option<&'a Engine>,
    pub(super) context: &'a ExecutionContext,
    pub(super) owner: String,
}

#[cfg(feature = "agent")]
impl fmt::Debug for LocalAgentInvoker<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalAgentInvoker")
            .field("place_id", &self.context.place_id)
            .field("app_instance_id", &self.context.app_instance_id)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "agent")]
impl AgentCapabilityInvoker for LocalAgentInvoker<'_> {
    fn invoke( &mut self, operation: &str, data: JsonValue, ) -> Result<AgentCapabilityResponse, AgentError> {
        match operation {
            "llm.generate" => {
                #[cfg(feature = "llm")]
                {
                    if !self.settings.service_capabilities.contains(ServiceCapability::Llm) {
                        return Err(AgentError::capability(operation, "local llm capability is disabled"));
                    }
                    let service = self.settings.llm_service.as_ref()
                        .ok_or_else(|| AgentError::capability(operation, "local llm provider is unavailable"))?;
                    let input = serde_json::from_value(data)
                        .map_err(|error| AgentError::capability(operation, error.to_string()))?;
                    let mut run = service.schedule(self.owner.clone())
                        .map_err(|error| AgentError::capability(operation, error.to_string()))?;
                    let run_id = run.run_id();
                    let mut partials = Vec::new();
                    let statistics = service.generate(&mut run, &input, |event| {
                        partials.push(match event {
                            LlmGenerationEvent::Text(text) => serde_json::json!({
                                "type":"token",
                                "runId":run_id,
                                "text":text,
                            }),
                            LlmGenerationEvent::ToolCall { id, name, arguments } => serde_json::json!({
                                "type":"toolCall",
                                "runId":run_id,
                                "id":id,
                                "name":name,
                                "arguments":arguments,
                            }),
                        });
                        true
                    }).map_err(|error| AgentError::capability(operation, error.to_string()))?;
                    return Ok(AgentCapabilityResponse::stream(
                        partials,
                        Some(serde_json::json!({
                            "runId":run_id,
                            "promptTokens":statistics.prompt_tokens,
                            "cachedPromptTokens":statistics.cached_prompt_tokens,
                            "completionTokens":statistics.completion_tokens,
                            "elapsedMs":statistics.elapsed_ms,
                            "finishReason":statistics.finish_reason,
                        })),
                    ));
                }
                #[cfg(not(feature = "llm"))]
                {
                    let _ = data;
                    Err(AgentError::capability(operation, "ogd was built without the llm feature"))
                }
            }
            "collections.list" => {
                if !self.settings.service_capabilities.contains(ServiceCapability::Database) {
                    return Err(AgentError::capability(operation, "local database capability is disabled"));
                }
                let engine = self.engine
                    .ok_or_else(|| AgentError::capability(operation, "local database engine is unavailable"))?;
                let snapshot = engine.storage().read()
                    .map_err(|error| AgentError::capability(operation, error.to_string()))?;
                let collections = list_storage_collections(
                    snapshot.as_ref(),
                    false,
                    Some(&self.context.place_id),
                    self.context.app_instance_id.as_deref(),
                ).map_err(|error| AgentError::capability(operation, error.to_string()))?;
                Ok(AgentCapabilityResponse::message(
                    serde_json::json!({"collections":collections}),
                ))
            }
            "query.execute" => {
                if !self.settings.service_capabilities.contains(ServiceCapability::Database) {
                    return Err(AgentError::capability(operation, "local database capability is disabled"));
                }
                let engine = self.engine
                    .ok_or_else(|| AgentError::capability(operation, "local database engine is unavailable"))?;
                let query = data.str_field("query")
                    .ok_or_else(|| AgentError::capability(operation, "query is required"))?;
                let query = crate::query::prepare_query(query, &serde_json::Map::new())
                    .map_err(|message| AgentError::capability(operation, message))?;
                let query = query.as_str();
                if data.bool_field("readOnly") != Some(true) {
                    return Err(AgentError::capability(operation, "Agent database queries must set readOnly=true"));
                }
                let pipeline = parse_pipeline(query)
                    .map_err(|error| AgentError::capability(operation, error.to_string()))?;
                let plan = crate::query::Planner::new().plan(&pipeline)
                    .map_err(|error| AgentError::capability(operation, error.to_string()))?;
                if plan.is_write() {
                    return Err(AgentError::capability(operation, "read-only Agent query attempted to mutate storage"));
                }
                let access = QueryAccess::analyze(query)
                    .map_err(|error| AgentError::capability(operation, error))?;
                if access.collection.starts_with('_') {
                    return Err(AgentError::capability(operation, "Agent queries cannot access system collections"));
                }
                match execute_request_scoped(
                    engine,
                    QueryRequest::new(0, query),
                    Some(self.context),
                ) {
                    QueryResponse::Ok { documents, statistics, .. } => {
                        Ok(AgentCapabilityResponse::stream(documents, statistics))
                    }
                    QueryResponse::Error { error, .. } => {
                        Err(AgentError::capability(operation, format!("{}: {}", error.code, error.message)))
                    }
                }
            }
            "file.list" => {
                #[cfg(feature = "files")]
                {
                    if !self.settings.service_capabilities.contains(ServiceCapability::Files) {
                        return Err(AgentError::capability(operation, "local files capability is disabled"));
                    }
                    let engine = self.engine
                        .ok_or_else(|| AgentError::capability(operation, "local files database is unavailable"))?;
                    let instance_id = self.context.app_instance_id.as_deref()
                        .ok_or_else(|| AgentError::capability(operation, "files.list requires an AppInstance scope"))?;
                    let parent_id = data.str_field("parentId");
                    let entries = list_file_entries(
                        engine,
                        RequestId::Number(0),
                        &self.context.place_id,
                        instance_id,
                        parent_id,
                    ).map_err(|response| AgentError::capability(operation, format!("{response:?}")))?;
                    return Ok(AgentCapabilityResponse::message(JsonValue::Array(entries)));
                }
                #[cfg(not(feature = "files"))]
                {
                    let _ = data;
                    Err(AgentError::capability(operation, "ogd was built without the files feature"))
                }
            }
            _ => Err(AgentError::capability(operation, "operation is not exposed to the Agent")),
        }
    }
}

#[cfg(all(feature = "agent", feature = "fabric"))]
pub(super) struct GatewayAgentInvoker {
    pub(super) websocket: NodeWebSocket,
    pub(super) next_id: u64,
}

#[cfg(all(feature = "agent", feature = "fabric"))]
impl fmt::Debug for GatewayAgentInvoker {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayAgentInvoker")
            .field("next_id", &self.next_id)
            .finish_non_exhaustive()
    }
}

#[cfg(all(feature = "agent", feature = "fabric"))]
impl GatewayAgentInvoker {
    pub(super) fn connect(settings: &ConnectionSettings, token: &str) -> Result<Self, AgentError> {
        let websocket = connect_delegated_gateway_client_any(settings, token)?;
        Ok(Self { websocket, next_id: 2 })
    }
}

#[cfg(all(feature = "agent", feature = "fabric"))]
impl AgentCapabilityInvoker for GatewayAgentInvoker {
    fn invoke( &mut self, operation: &str, data: JsonValue, ) -> Result<AgentCapabilityResponse, AgentError> {
        let id = RequestId::Number(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        let request_id = serde_json::to_value(id)
            .map_err(|error| AgentError::capability(operation, error.to_string()))?;
        let request = OperationRequest::new(id, operation, data);
        let value = serde_json::to_value(&request)
            .map_err(|error| AgentError::capability(operation, error.to_string()))?;
        write_gateway_message(&mut self.websocket, &value)
            .map_err(|error| AgentError::capability(operation, error))?;

        let mut partials = Vec::new();
        loop {
            let Some(message) = read_gateway_message(&mut self.websocket)
                .map_err(|error| AgentError::capability(operation, error))?
            else {
                return Err(AgentError::capability(operation, "Gateway closed before capability response completed"));
            };
            if message.str_field("kind") != Some("response")
                || message.get("id") != Some(&request_id)
            {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(AgentError::capability(operation, error.to_string()));
            }
            match message.str_field("status") {
                Some("partial") => {
                    if let Some(data) = message.get("data") {
                        partials.push(data.clone());
                    }
                }
                Some("complete") => {
                    return Ok(AgentCapabilityResponse::stream(
                        partials,
                        message.get("statistics").cloned().filter(|value| !value.is_null()),
                    ));
                }
                Some("ok") => {
                    return Ok(AgentCapabilityResponse::message(
                        message.get("data").cloned().unwrap_or(JsonValue::Null),
                    ));
                }
                Some(other) => {
                    return Err(AgentError::capability(operation, format!("unexpected Gateway response status {other:?}")));
                }
                None => {
                    return Err(AgentError::capability(operation, "Gateway response has no status"));
                }
            }
        }
    }
}

#[cfg(feature = "agent")]
pub(super) enum OgdAgentInvoker<'a> {
    Local(LocalAgentInvoker<'a>),
    #[cfg(feature = "fabric")]
    Gateway(GatewayAgentInvoker),
}

#[cfg(feature = "agent")]
impl fmt::Debug for OgdAgentInvoker<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(value) => value.fmt(formatter),
            #[cfg(feature = "fabric")]
            Self::Gateway(value) => value.fmt(formatter),
        }
    }
}

#[cfg(feature = "agent")]
impl AgentCapabilityInvoker for OgdAgentInvoker<'_> {
    fn invoke( &mut self, operation: &str, data: JsonValue, ) -> Result<AgentCapabilityResponse, AgentError> {
        let provider = match self {
            Self::Local(_) => "local",
            #[cfg(feature = "fabric")]
            Self::Gateway(_) => "gateway",
        };
        let response = match self {
            Self::Local(value) => value.invoke(operation, data),
            #[cfg(feature = "fabric")]
            Self::Gateway(value) => value.invoke(operation, data),
        }?;
        if operation == "llm.generate" && debug::enabled(DebugTopic::Agent) {
            let mut output = String::new();
            let mut tool_calls = Vec::new();
            for partial in &response.partials {
                match partial.str_field("type") {
                    Some("token") => {
                        if let Some(text) = partial.str_field("text") {
                            output.push_str(text);
                        }
                    }
                    Some("toolCall") => tool_calls.push(partial.clone()),
                    _ => {}
                }
            }
            debug::log( DebugTopic::Agent, None, format!( "llm.generate provider={provider} output={output:?} toolCalls={tool_calls:?}" ), );
        }
        Ok(response)
    }
}
