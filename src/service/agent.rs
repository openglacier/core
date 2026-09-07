#![cfg_attr(rustfmt, rustfmt_skip)]
//! Stateless Agent orchestration over OpenGlacier capabilities.
//!
//! The Agent owns the reasoning/tool loop, but deliberately does not own any
//! backend. LLM, database and files access are provided by the host through
//! `AgentCapabilityInvoker`, which lets standalone ogd use local capabilities
//! while a fabric node delegates the same operations back through Gateway.

use std::{
    env,
    error::Error,
    fmt::{self, Display, Formatter},
};

use serde::Serialize;
use serde_json::{json, Value as JsonValue};

use crate::{
    access::authorization::QueryAccess,
    operation::{AgentRunInput, LlmMessageInput},
    query::{parse as parse_query, Planner},
};

const DEFAULT_MAX_STEPS: u32 = 8;
const MAX_MAX_STEPS: u32 = 32;
const DEFAULT_LLM_MAX_TOKENS: u32 = 512;
const DEFAULT_MAX_TOOL_BYTES: usize = 32 * 1024;
const MAX_MAX_TOOL_BYTES: usize = 1024 * 1024;

const SYSTEM_PROMPT: &str = r#"You are the OpenGlacier Agent. You are stateless: only the messages in this request and tool results in this run exist.

You can use these tools:
- database.collections {}: list application collections visible in the inherited execution scope.
- database.query {"query":"..."}: execute one READ-ONLY OpenGlacier query in the inherited execution scope. Never query system collections whose name starts with "_".
- files.list {"parentId":null}: list files/folders in the inherited AppInstance. parentId may be null or a file id returned by an earlier files.list.

The Place/AppInstance scope is inherited from the request and is never a tool argument. Never invent or request a placeId, appInstanceId or instanceId.

When you need a tool, output exactly one JSON object and nothing else:
{"type":"tool","name":"database.query","arguments":{"query":"on collection | limit 10"}}

When you are ready to answer, output exactly one JSON object and nothing else:
{"type":"final","content":"your answer"}

Do not wrap these JSON objects in Markdown fences. Use at most one tool per turn. If a tool fails, use the error as information and either try a safer alternative or finish honestly."#;

/// Process-wide Agent configuration. It contains limits only; no conversation
/// or run state is retained by the service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentConfig {
    pub max_steps: u32,
    pub llm_max_tokens: u32,
    pub max_tool_bytes: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            max_steps: DEFAULT_MAX_STEPS,
            llm_max_tokens: DEFAULT_LLM_MAX_TOKENS,
            max_tool_bytes: DEFAULT_MAX_TOOL_BYTES,
        }
    }
}

impl AgentConfig {
    pub fn from_environment() -> Result<Self, AgentError> {
        let mut config = Self::default();
        if let Some(value) = env_u32("OGD_AGENT_MAX_STEPS")? {
            if value == 0 || value > MAX_MAX_STEPS {
                return Err(AgentError::Configuration(format!(
                    "OGD_AGENT_MAX_STEPS must be between 1 and {MAX_MAX_STEPS}"
                )));
            }
            config.max_steps = value;
        }
        if let Some(value) = env_u32("OGD_AGENT_LLM_MAX_TOKENS")? {
            if value == 0 {
                return Err(AgentError::Configuration(
                    "OGD_AGENT_LLM_MAX_TOKENS must be greater than zero".to_owned(),
                ));
            }
            config.llm_max_tokens = value;
        }
        if let Some(value) = env_usize("OGD_AGENT_MAX_TOOL_BYTES")? {
            if value == 0 || value > MAX_MAX_TOOL_BYTES {
                return Err(AgentError::Configuration(format!(
                    "OGD_AGENT_MAX_TOOL_BYTES must be between 1 and {MAX_MAX_TOOL_BYTES}"
                )));
            }
            config.max_tool_bytes = value;
        }
        Ok(config)
    }
}

fn env_u32(name: &str) -> Result<Option<u32>, AgentError> {
    env::var(name)
        .ok()
        .map(|value| {
            value.parse::<u32>().map_err(|error| {
                AgentError::Configuration(format!("{name} must be an unsigned integer: {error}"))
            })
        })
        .transpose()
}

fn env_usize(name: &str) -> Result<Option<usize>, AgentError> {
    env::var(name)
        .ok()
        .map(|value| {
            value.parse::<usize>().map_err(|error| {
                AgentError::Configuration(format!("{name} must be an unsigned integer: {error}"))
            })
        })
        .transpose()
}

/// Current state of the local Agent orchestrator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentStatus {
    pub ready: bool,
    pub state: &'static str,
    pub stateless: bool,
    pub max_steps: u32,
    pub llm_max_tokens: u32,
    pub tools: Vec<&'static str>,
}

/// One capability result normalized for the Agent loop.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentCapabilityResponse {
    pub partials: Vec<JsonValue>,
    pub data: Option<JsonValue>,
    pub statistics: Option<JsonValue>,
}

impl AgentCapabilityResponse {
    #[must_use]
    pub const fn message(data: JsonValue) -> Self {
        Self {
            partials: Vec::new(),
            data: Some(data),
            statistics: None,
        }
    }

    #[must_use]
    pub fn stream(partials: Vec<JsonValue>, statistics: Option<JsonValue>) -> Self {
        Self {
            partials,
            data: None,
            statistics,
        }
    }

    fn tool_value(&self) -> JsonValue {
        if !self.partials.is_empty() {
            json!({
                "items": self.partials.clone(),
                "statistics": self.statistics.clone(),
            })
        } else {
            self.data.clone().unwrap_or(JsonValue::Null)
        }
    }

    fn generated_text(&self) -> String {
        let mut output = String::new();
        for partial in &self.partials {
            if partial.get("type").and_then(JsonValue::as_str) == Some("token") {
                if let Some(text) = partial.get("text").and_then(JsonValue::as_str) {
                    output.push_str(text);
                }
            }
        }
        output
    }
}

/// Host-provided capability invocation surface.
///
/// The Agent never chooses local vs remote providers; the ogd host supplies an
/// implementation that preserves the inherited execution/delegation scope.
pub trait AgentCapabilityInvoker {
    fn invoke(
        &mut self,
        operation: &str,
        data: JsonValue,
    ) -> Result<AgentCapabilityResponse, AgentError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentError {
    Configuration(String),
    Capability { operation: String, message: String },
    InvalidToolCall(String),
    ToolDenied(String),
    StepLimit(u32),
    Cancelled,
}

impl AgentError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Configuration(_) => "agent.configuration",
            Self::Capability { .. } => "agent.capability_failed",
            Self::InvalidToolCall(_) => "agent.invalid_tool_call",
            Self::ToolDenied(_) => "agent.tool_denied",
            Self::StepLimit(_) => "agent.step_limit",
            Self::Cancelled => "agent.cancelled",
        }
    }

    #[must_use]
    pub fn capability(operation: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Capability {
            operation: operation.into(),
            message: message.into(),
        }
    }
}

impl Display for AgentError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message)
            | Self::InvalidToolCall(message)
            | Self::ToolDenied(message) => formatter.write_str(message),
            Self::Capability { operation, message } => {
                write!(formatter, "capability {operation} failed: {message}")
            }
            Self::StepLimit(limit) => write!(formatter, "agent exceeded the {limit}-step limit"),
            Self::Cancelled => formatter.write_str("agent run was cancelled by the response sink"),
        }
    }
}

impl Error for AgentError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRunStats {
    pub steps: u32,
    pub llm_calls: u32,
    pub tool_calls: u32,
}

#[derive(Debug, Clone)]
pub struct AgentService {
    config: AgentConfig,
}

impl AgentService {
    pub fn from_environment() -> Result<Self, AgentError> {
        Ok(Self {
            config: AgentConfig::from_environment()?,
        })
    }

    #[must_use]
    pub fn status(&self) -> AgentStatus {
        AgentStatus {
            ready: true,
            state: "ready",
            stateless: true,
            max_steps: self.config.max_steps,
            llm_max_tokens: self.config.llm_max_tokens,
            tools: vec!["database.collections", "database.query", "files.list"],
        }
    }

    pub fn run<F>(
        &self,
        input: &AgentRunInput,
        invoker: &mut dyn AgentCapabilityInvoker,
        mut emit: F,
    ) -> Result<AgentRunStats, AgentError>
    where
        F: FnMut(JsonValue) -> bool,
    {
        let max_steps = input.max_steps.unwrap_or(self.config.max_steps);
        if max_steps == 0 || max_steps > self.config.max_steps {
            return Err(AgentError::Configuration(format!(
                "maxSteps must be between 1 and {}",
                self.config.max_steps
            )));
        }
        let max_tokens = input.max_tokens.unwrap_or(self.config.llm_max_tokens);
        if max_tokens == 0 {
            return Err(AgentError::Configuration(
                "maxTokens must be greater than zero".to_owned(),
            ));
        }

        let mut messages = Vec::with_capacity(input.messages.len() + 1 + max_steps as usize * 2);
        messages.push(LlmMessageInput {
            role: "system".to_owned(),
            content: SYSTEM_PROMPT.to_owned(),
        });
        messages.extend(input.messages.iter().cloned());

        let mut stats = AgentRunStats {
            steps: 0,
            llm_calls: 0,
            tool_calls: 0,
        };

        for step in 1..=max_steps {
            stats.steps = step;
            stats.llm_calls = stats.llm_calls.saturating_add(1);
            emit_or_cancel(&mut emit, json!({"type":"step","step":step,"phase":"llm"}))?;

            let llm_response = invoker.invoke(
                "llm.generate",
                json!({
                    "messages": &messages,
                    "maxTokens": max_tokens,
                }),
            )?;
            let generated = llm_response.generated_text();
            if generated.trim().is_empty() {
                return Err(AgentError::capability(
                    "llm.generate",
                    "provider completed without emitting text tokens",
                ));
            }

            match parse_directive(&generated)? {
                AgentDirective::Final(content) => {
                    emit_or_cancel(
                        &mut emit,
                        json!({
                            "type":"message",
                            "role":"assistant",
                            "content":content,
                        }),
                    )?;
                    return Ok(stats);
                }
                AgentDirective::Tool { name, arguments } => {
                    stats.tool_calls = stats.tool_calls.saturating_add(1);
                    emit_or_cancel(
                        &mut emit,
                        json!({
                            "type":"toolCall",
                            "step":step,
                            "name":name,
                            "arguments":arguments,
                        }),
                    )?;

                    let (operation, data) = tool_operation(&name, arguments)?;
                    let response = invoker.invoke(operation, data)?;
                    let tool_value = response.tool_value();
                    let tool_text = bounded_tool_text(&tool_value, self.config.max_tool_bytes);
                    emit_or_cancel(
                        &mut emit,
                        json!({
                            "type":"toolResult",
                            "step":step,
                            "name":name,
                            "result": serde_json::from_str::<JsonValue>(&tool_text)
                                .unwrap_or_else(|_| JsonValue::String(tool_text.clone())),
                        }),
                    )?;

                    messages.push(LlmMessageInput {
                        role: "assistant".to_owned(),
                        content: generated,
                    });
                    messages.push(LlmMessageInput {
                        role: "user".to_owned(),
                        content: format!(
                            "Tool result for {name}:\n{tool_text}\nContinue with exactly one tool JSON object or one final JSON object."
                        ),
                    });
                }
            }
        }

        Err(AgentError::StepLimit(max_steps))
    }
}

fn emit_or_cancel<F>(emit: &mut F, value: JsonValue) -> Result<(), AgentError>
where
    F: FnMut(JsonValue) -> bool,
{
    if emit(value) {
        Ok(())
    } else {
        Err(AgentError::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum AgentDirective {
    Tool { name: String, arguments: JsonValue },
    Final(String),
}

fn parse_directive(source: &str) -> Result<AgentDirective, AgentError> {
    let trimmed = strip_json_fence(source.trim());
    let Ok(value) = serde_json::from_str::<JsonValue>(trimmed) else {
        // Compatibility fallback for small instruct models that answer directly
        // instead of following the JSON envelope. It is safe because no tool is
        // invoked unless a valid explicit tool object is parsed.
        return Ok(AgentDirective::Final(source.trim().to_owned()));
    };
    let kind = value
        .get("type")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    match kind {
        "final" => value
            .get("content")
            .and_then(JsonValue::as_str)
            .map(|content| AgentDirective::Final(content.to_owned()))
            .ok_or_else(|| {
                AgentError::InvalidToolCall("final directive requires string content".to_owned())
            }),
        "tool" | "tool_call" => {
            let name = value
                .get("name")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    AgentError::InvalidToolCall("tool directive requires name".to_owned())
                })?;
            let arguments = value.get("arguments").cloned().unwrap_or_else(|| json!({}));
            if !arguments.is_object() {
                return Err(AgentError::InvalidToolCall(
                    "tool arguments must be an object".to_owned(),
                ));
            }
            Ok(AgentDirective::Tool {
                name: name.to_owned(),
                arguments,
            })
        }
        _ => Ok(AgentDirective::Final(source.trim().to_owned())),
    }
}

fn strip_json_fence(source: &str) -> &str {
    let Some(after) = source.strip_prefix("```json") else {
        let Some(after) = source.strip_prefix("```") else {
            return source;
        };
        return after.strip_suffix("```").unwrap_or(after).trim();
    };
    after.strip_suffix("```").unwrap_or(after).trim()
}

fn tool_operation(
    name: &str,
    arguments: JsonValue,
) -> Result<(&'static str, JsonValue), AgentError> {
    match name {
        "database.collections" => Ok(("collections.list", json!({"stats": false}))),
        "database.query" => {
            let query = arguments
                .get("query")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| {
                    AgentError::InvalidToolCall(
                        "database.query requires arguments.query".to_owned(),
                    )
                })?
                .trim();
            if query.is_empty() {
                return Err(AgentError::InvalidToolCall(
                    "database.query query must not be empty".to_owned(),
                ));
            }
            validate_read_only_query(query)?;
            Ok(("query.execute", json!({"query": query, "readOnly": true})))
        }
        "files.list" => {
            let parent_id = arguments
                .get("parentId")
                .cloned()
                .unwrap_or(JsonValue::Null);
            if !parent_id.is_null() && !parent_id.is_string() {
                return Err(AgentError::InvalidToolCall(
                    "files.list parentId must be null or a string".to_owned(),
                ));
            }
            Ok(("file.list", json!({"parentId": parent_id})))
        }
        other => Err(AgentError::ToolDenied(format!(
            "tool {other:?} is not available to the Agent"
        ))),
    }
}

fn validate_read_only_query(query: &str) -> Result<(), AgentError> {
    let access = QueryAccess::analyze(query)
        .map_err(|error| AgentError::InvalidToolCall(format!("invalid database query: {error}")))?;
    if access.collection.starts_with('_') {
        return Err(AgentError::ToolDenied(
            "database.query cannot access system collections".to_owned(),
        ));
    }
    let ast = parse_query(query)
        .map_err(|error| AgentError::InvalidToolCall(format!("invalid database query: {error}")))?;
    let plan = Planner::new()
        .plan_ast(query, &ast)
        .map_err(|error| AgentError::InvalidToolCall(format!("invalid database query: {error}")))?;
    if !plan.is_read_only() {
        return Err(AgentError::ToolDenied(
            "database.query accepts read-only queries only".to_owned(),
        ));
    }
    Ok(())
}

fn bounded_tool_text(value: &JsonValue, max_bytes: usize) -> String {
    let serialized = serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned());
    if serialized.len() <= max_bytes {
        return serialized;
    }
    let mut end = max_bytes.min(serialized.len());
    while !serialized.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    serde_json::to_string(&json!({
        "truncated": true,
        "originalBytes": serialized.len(),
        "preview": &serialized[..end],
    }))
    .expect("bounded tool result serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct FakeInvoker {
        calls: Vec<String>,
        llm_outputs: Vec<String>,
    }

    impl AgentCapabilityInvoker for FakeInvoker {
        fn invoke(
            &mut self,
            operation: &str,
            _data: JsonValue,
        ) -> Result<AgentCapabilityResponse, AgentError> {
            self.calls.push(operation.to_owned());
            match operation {
                "llm.generate" => {
                    let output = self.llm_outputs.remove(0);
                    Ok(AgentCapabilityResponse::stream(
                        vec![json!({"type":"token","text":output})],
                        None,
                    ))
                }
                "query.execute" => Ok(AgentCapabilityResponse::stream(
                    vec![json!({"name":"Ada"})],
                    None,
                )),
                _ => Ok(AgentCapabilityResponse::message(json!({}))),
            }
        }
    }

    fn request() -> AgentRunInput {
        AgentRunInput {
            messages: vec![LlmMessageInput {
                role: "user".to_owned(),
                content: "Who is there?".to_owned(),
            }],
            context: crate::RequestedExecutionContext {
                place_id: "place-1".to_owned(),
                app_instance_id: Some("app-1".to_owned()),
            },
            max_steps: None,
            max_tokens: None,
        }
    }

    #[test] fn status_is_ready_and_stateless() { let service = AgentService { config: AgentConfig::default(), }; let status = service.status(); assert!(status.ready); assert!(status.stateless); assert!(status.tools.contains(&"database.query")); }

    #[test] fn direct_answer_needs_only_one_llm_call() { let service = AgentService { config: AgentConfig::default(), }; let mut invoker = FakeInvoker { calls: Vec::new(), llm_outputs: vec![r#"{"type":"final","content":"Hello"}"#.to_owned()], }; let mut events = Vec::new(); let stats = service .run(&request(), &mut invoker, |event| { events.push(event); true }) .unwrap(); assert_eq!(stats.llm_calls, 1); assert_eq!(stats.tool_calls, 0); assert_eq!(invoker.calls, vec!["llm.generate"]); assert!(events .iter() .any(|event| event.get("type").and_then(JsonValue::as_str) == Some("message"))); }

    #[test] fn read_only_query_tool_round_trips_through_capability_invoker() { let service = AgentService { config: AgentConfig::default(), }; let mut invoker = FakeInvoker { calls: Vec::new(), llm_outputs: vec![ r#"{"type":"tool","name":"database.query","arguments":{"query":"on people | limit 1"}}"#.to_owned(), r#"{"type":"final","content":"Ada"}"#.to_owned(), ], }; let stats = service.run(&request(), &mut invoker, |_| true).unwrap(); assert_eq!(stats.llm_calls, 2); assert_eq!(stats.tool_calls, 1); assert_eq!( invoker.calls, vec!["llm.generate", "query.execute", "llm.generate"] ); }

    #[test] fn mutating_query_is_denied_before_capability_call() { let error = tool_operation("database.query", json!({"query":"on people | delete"})).unwrap_err(); assert!(matches!(error, AgentError::ToolDenied(_))); }

    #[test] fn malformed_model_envelope_is_safe_plain_final_text() { assert_eq!( parse_directive("hello").unwrap(), AgentDirective::Final("hello".to_owned()) ); }
}
