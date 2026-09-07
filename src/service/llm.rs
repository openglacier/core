#![cfg_attr(rustfmt, rustfmt_skip)]
//! Local LLM capability backed by llama.cpp / GGUF.

mod config;
mod runtime;
mod scheduler;

use std::fmt;

use serde::Serialize;

use crate::operation::LlmGenerateInput;

pub use config::{LlmConfig, LlmConfigError};
use runtime::LlmRuntime;
pub use runtime::{LlmError, LlmGenerationStats};
pub use scheduler::ScheduledRun as LlmRun;

pub struct LlmService {
    config: LlmConfig,
    runtime: Option<LlmRuntime>,
}

impl fmt::Debug for LlmService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LlmService")
            .field("config", &self.config)
            .field("ready", &self.runtime.is_some())
            .finish()
    }
}

impl LlmService {
    pub fn from_environment() -> Result<Self, LlmError> {
        let config =
            LlmConfig::from_environment().map_err(|error| LlmError::Runtime(error.to_string()))?;
        let runtime = if config.model_path.is_some() {
            Some(LlmRuntime::load(&config)?)
        } else {
            None
        };
        Ok(Self { config, runtime })
    }

    #[must_use]
    pub fn status(&self) -> LlmStatus {
        match self.runtime.as_ref() {
            Some(runtime) => LlmStatus {
                ready: true,
                state: "ready",
                backend: "llama.cpp",
                model: Some(runtime.model_name().to_owned()),
                context_size: Some(runtime.context_size()),
                max_tokens: self.config.max_tokens,
                max_parallel: runtime.max_parallel(),
                queue_size: runtime.queue_size(),
                active_requests: runtime.active_requests(),
                queued_requests: runtime.queued_requests(),
            },
            None => LlmStatus {
                ready: false,
                state: "unconfigured",
                backend: "llama.cpp",
                model: None,
                context_size: Some(self.config.context_size.get()),
                max_tokens: self.config.max_tokens,
                max_parallel: self.config.max_parallel,
                queue_size: self.config.queue_size,
                active_requests: 0,
                queued_requests: 0,
            },
        }
    }

    pub fn schedule(&self, owner: String) -> Result<LlmRun, LlmError> {
        self.runtime
            .as_ref()
            .ok_or(LlmError::Unconfigured)?
            .schedule(owner)
    }

    pub fn generate<F>(
        &self,
        run: &mut LlmRun,
        request: &LlmGenerateInput,
        emit: F,
    ) -> Result<LlmGenerationStats, LlmError>
    where
        F: FnMut(&str) -> bool,
    {
        self.runtime
            .as_ref()
            .ok_or(LlmError::Unconfigured)?
            .generate(run, request, emit)
    }

    #[must_use]
    pub fn cancel(&self, run_id: u64, owner: &str) -> bool {
        self.runtime
            .as_ref()
            .is_some_and(|runtime| runtime.cancel(run_id, owner))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LlmStatus {
    pub ready: bool,
    pub state: &'static str,
    pub backend: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_size: Option<u32>,
    pub max_tokens: u32,
    pub max_parallel: u32,
    pub queue_size: u32,
    pub active_requests: usize,
    pub queued_requests: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn status_shape_is_stable_for_unconfigured_service() { let service = LlmService { config: LlmConfig { model_path: None, context_size: std::num::NonZeroU32::new(4096).unwrap(), threads: None, max_tokens: 512, seed: 1234, max_parallel: 1, queue_size: 16, chat_template: None, }, runtime: None, }; let status = service.status(); assert!(!status.ready); assert_eq!(status.state, "unconfigured"); assert_eq!(status.backend, "llama.cpp"); }
}
