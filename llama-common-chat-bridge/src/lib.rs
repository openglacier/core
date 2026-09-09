use std::{
    error::Error as StdError,
    ffi::{CStr, CString},
    fmt::{self, Display, Formatter},
    os::raw::{c_char, c_int},
    ptr,
};

use serde::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeError(String);

impl BridgeError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl Display for BridgeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl StdError for BridgeError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CommonChatToolChoice {
    Auto = 0,
    None = 1,
    Required = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum CommonChatThinkingMode {
    Auto = 0,
    On = 1,
    Off = 2,
}

impl CommonChatThinkingMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::On => "on",
            Self::Off => "off",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommonChatPlan {
    pub prompt: String,
    pub grammar: String,
    pub grammar_lazy: bool,
    pub grammar_needs_prefill: bool,
    pub generation_prompt: String,
    pub trigger_patterns: Vec<String>,
    pub trigger_tokens: Vec<i32>,
    pub additional_stops: Vec<String>,
    pub parser: String,
    pub format: i32,
    pub reasoning_format: i32,
    pub supports_thinking: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommonChatTurn {
    pub content: String,
    pub tool_calls: Vec<CommonChatToolCall>,
}

#[derive(Debug, Deserialize)]
pub struct CommonChatToolCall {
    pub id: String,
    pub name: String,
    pub arguments: JsonValue,
}

pub fn apply(
    chat_template: &str,
    bos_token: &str,
    eos_token: &str,
    messages_json: &str,
    tools_json: &str,
    tool_choice: CommonChatToolChoice,
    thinking_mode: CommonChatThinkingMode,
) -> Result<CommonChatPlan, BridgeError> {
    let template = cstring("chat template", chat_template)?;
    let bos = cstring("BOS token", bos_token)?;
    let eos = cstring("EOS token", eos_token)?;
    let messages = cstring("messages", messages_json)?;
    let tools = cstring("tools", tools_json)?;
    let json = call_bridge(|out, error| unsafe {
        og_llama_common_chat_apply(
            template.as_ptr(),
            bos.as_ptr(),
            eos.as_ptr(),
            messages.as_ptr(),
            tools.as_ptr(),
            tool_choice as c_int,
            thinking_mode as c_int,
            out,
            error,
        )
    })?;
    serde_json::from_str(&json).map_err(|error| {
        BridgeError::new(format!(
            "llama.cpp common/chat returned an invalid plan: {error}"
        ))
    })
}

pub fn parse(plan: &CommonChatPlan, generated: &str) -> Result<CommonChatTurn, BridgeError> {
    let generated = cstring("generated text", generated)?;
    let generation_prompt = cstring("generation prompt", &plan.generation_prompt)?;
    let parser = cstring("chat parser", &plan.parser)?;
    let json = call_bridge(|out, error| unsafe {
        og_llama_common_chat_parse(
            generated.as_ptr(),
            plan.format,
            plan.reasoning_format,
            generation_prompt.as_ptr(),
            parser.as_ptr(),
            out,
            error,
        )
    })?;
    serde_json::from_str(&json).map_err(|error| {
        BridgeError::new(format!(
            "llama.cpp common/chat returned an invalid parsed turn: {error}"
        ))
    })
}

fn cstring(label: &str, value: &str) -> Result<CString, BridgeError> {
    CString::new(value).map_err(|_| BridgeError::new(format!("{label} must not contain NUL bytes")))
}

fn call_bridge<F>(call: F) -> Result<String, BridgeError>
where
    F: FnOnce(*mut *mut c_char, *mut *mut c_char) -> c_int,
{
    let mut out = ptr::null_mut();
    let mut error = ptr::null_mut();
    let status = call(&mut out, &mut error);
    let error_text = take_string(error);
    if status != 0 {
        if !out.is_null() {
            unsafe { og_llama_common_chat_string_free(out) };
        }
        return Err(BridgeError::new(error_text.unwrap_or_else(|| {
            format!("llama.cpp common/chat bridge failed with status {status}")
        })));
    }
    if out.is_null() {
        return Err(BridgeError::new(
            "llama.cpp common/chat bridge returned no result",
        ));
    }
    Ok(take_string(out).expect("non-null bridge string"))
}

fn take_string(value: *mut c_char) -> Option<String> {
    if value.is_null() {
        return None;
    }
    let text = unsafe { CStr::from_ptr(value) }
        .to_string_lossy()
        .into_owned();
    unsafe { og_llama_common_chat_string_free(value) };
    Some(text)
}

unsafe extern "C" {
    fn og_llama_common_chat_apply(
        chat_template: *const c_char,
        bos_token: *const c_char,
        eos_token: *const c_char,
        messages_json: *const c_char,
        tools_json: *const c_char,
        tool_choice: c_int,
        thinking_mode: c_int,
        out_json: *mut *mut c_char,
        out_error: *mut *mut c_char,
    ) -> c_int;

    fn og_llama_common_chat_parse(
        generated: *const c_char,
        format: c_int,
        reasoning_format: c_int,
        generation_prompt: *const c_char,
        parser_source: *const c_char,
        out_json: *mut *mut c_char,
        out_error: *mut *mut c_char,
    ) -> c_int;

    fn og_llama_common_chat_string_free(value: *mut c_char);
}
