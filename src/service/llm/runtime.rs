use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    path::Path,
    time::Instant,
};

use encoding_rs::UTF_8;
use llama_cpp_2::{
    context::params::LlamaContextParams,
    llama_backend::LlamaBackend,
    llama_batch::LlamaBatch,
    model::{params::LlamaModelParams, AddBos, LlamaChatMessage, LlamaChatTemplate, LlamaModel},
    sampling::LlamaSampler,
};

use crate::operation::LlmGenerateInput;

use super::{
    config::LlmConfig,
    scheduler::{LlmScheduler, ScheduledRun},
};

pub struct LlmRuntime {
    backend: LlamaBackend,
    model: LlamaModel,
    template: LlamaChatTemplate,
    model_name: String,
    context_size: std::num::NonZeroU32,
    threads: Option<i32>,
    max_tokens: u32,
    seed: u32,
    add_bos: AddBos,
    scheduler: LlmScheduler,
}

impl LlmRuntime {
    pub fn load(config: &LlmConfig) -> Result<Self, LlmError> {
        let path = config.model_path.as_ref().ok_or(LlmError::Unconfigured)?;
        if !path.is_file() {
            return Err(LlmError::Model(format!(
                "GGUF model does not exist or is not a file: {}",
                path.display()
            )));
        }
        let backend = LlamaBackend::init().map_err(|error| LlmError::Backend(error.to_string()))?;
        let model = LlamaModel::load_from_file(&backend, path, &LlamaModelParams::default())
            .map_err(|error| LlmError::Model(error.to_string()))?;
        let template = match config.chat_template.as_deref() {
            Some(template) => LlamaChatTemplate::new(template)
                .map_err(|error| LlmError::Template(error.to_string()))?,
            None => model.chat_template(None).map_err(|error| {
                LlmError::Template(format!(
                    "model has no usable default chat template ({error}); set OGD_LLM_CHAT_TEMPLATE"
                ))
            })?,
        };
        let add_bos = model
            .meta_val_str("tokenizer.ggml.add_bos_token")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .map_or(AddBos::Always, |enabled| {
                if enabled {
                    AddBos::Always
                } else {
                    AddBos::Never
                }
            });
        Ok(Self {
            backend,
            model,
            template,
            model_name: model_name(path),
            context_size: config.context_size,
            threads: config.threads,
            max_tokens: config.max_tokens,
            seed: config.seed,
            add_bos,
            scheduler: LlmScheduler::new(config.max_parallel, config.queue_size),
        })
    }

    pub fn model_name(&self) -> &str {
        &self.model_name
    }

    pub const fn context_size(&self) -> u32 {
        self.context_size.get()
    }

    #[must_use]
    pub fn active_requests(&self) -> usize {
        self.scheduler.active_requests()
    }

    #[must_use]
    pub fn queued_requests(&self) -> usize {
        self.scheduler.queued_requests()
    }

    #[must_use]
    pub fn max_parallel(&self) -> u32 {
        self.scheduler.max_parallel()
    }

    #[must_use]
    pub fn queue_size(&self) -> u32 {
        self.scheduler.queue_size()
    }

    pub fn schedule(&self, owner: String) -> Result<ScheduledRun, LlmError> {
        self.scheduler.schedule(owner)
    }

    #[must_use]
    pub fn cancel(&self, run_id: u64, owner: &str) -> bool {
        self.scheduler.cancel(run_id, owner)
    }

    pub fn generate<F>(
        &self,
        run: &mut ScheduledRun,
        request: &LlmGenerateInput,
        mut emit: F,
    ) -> Result<LlmGenerationStats, LlmError>
    where
        F: FnMut(&str) -> bool,
    {
        let started = Instant::now();
        if !run.wait_until_active()? {
            return Ok(LlmGenerationStats {
                prompt_tokens: 0,
                completion_tokens: 0,
                elapsed_ms: elapsed_ms(started),
                finish_reason: "cancelled",
            });
        }
        if run.is_cancelled() {
            return Ok(LlmGenerationStats {
                prompt_tokens: 0,
                completion_tokens: 0,
                elapsed_ms: elapsed_ms(started),
                finish_reason: "cancelled",
            });
        }

        let messages = request
            .messages
            .iter()
            .map(|message| {
                LlamaChatMessage::new(message.role.clone(), message.content.clone())
                    .map_err(|error| LlmError::Request(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let prompt = self
            .model
            .apply_chat_template(&self.template, &messages, true)
            .map_err(|error| LlmError::Template(error.to_string()))?;
        let prompt_tokens = self
            .model
            .str_to_token(&prompt, self.add_bos)
            .map_err(|error| LlmError::Request(error.to_string()))?;
        if prompt_tokens.is_empty() {
            return Err(LlmError::Request(
                "chat template produced an empty prompt".to_owned(),
            ));
        }

        let max_tokens = request.max_tokens.unwrap_or(self.max_tokens);
        if max_tokens > self.max_tokens {
            return Err(LlmError::Request(format!(
                "maxTokens {max_tokens} exceeds OGD_LLM_MAX_TOKENS {}",
                self.max_tokens
            )));
        }
        let context_size =
            usize::try_from(self.context_size.get()).expect("u32 context size fits usize");
        let required = prompt_tokens
            .len()
            .saturating_add(usize::try_from(max_tokens).expect("u32 max tokens fits usize"));
        if required > context_size {
            return Err(LlmError::Request(format!(
                "prompt plus generation budget requires {required} tokens but context size is {context_size}"
            )));
        }

        let prompt_batch_size = u32::try_from(prompt_tokens.len())
            .map_err(|_| LlmError::Request("prompt token count exceeds u32::MAX".to_owned()))?;
        let mut context_params = LlamaContextParams::default()
            .with_n_ctx(Some(self.context_size))
            .with_n_batch(prompt_batch_size.max(1));
        if let Some(threads) = self.threads {
            context_params = context_params
                .with_n_threads(threads)
                .with_n_threads_batch(threads);
        }
        let mut context = self
            .model
            .new_context(&self.backend, context_params)
            .map_err(|error| LlmError::Runtime(error.to_string()))?;
        let mut next_position = i32::try_from(prompt_tokens.len())
            .map_err(|_| LlmError::Request("prompt token count exceeds i32::MAX".to_owned()))?;

        let mut batch = LlamaBatch::new(prompt_tokens.len().max(1), 1);
        batch
            .add_sequence(&prompt_tokens, 0, false)
            .map_err(|error| LlmError::Runtime(error.to_string()))?;
        context
            .decode(&mut batch)
            .map_err(|error| LlmError::Runtime(error.to_string()))?;

        let seed = request.seed.unwrap_or(self.seed);
        let mut sampler = LlamaSampler::chain_simple([
            LlamaSampler::temp(0.7),
            LlamaSampler::top_p(0.9, 1),
            LlamaSampler::dist(seed),
        ]);
        let mut decoder = UTF_8.new_decoder();
        let mut completion_tokens = 0_u32;
        let mut finish_reason = "length";

        for _ in 0..max_tokens {
            if run.is_cancelled() {
                finish_reason = "cancelled";
                break;
            }
            let token = sampler.sample(&context, batch.n_tokens() - 1);
            sampler.accept(token);
            if self.model.is_eog_token(token) {
                finish_reason = "stop";
                break;
            }
            let piece = self
                .model
                .token_to_piece(token, &mut decoder, true, None)
                .map_err(|error| LlmError::Runtime(error.to_string()))?;
            completion_tokens = completion_tokens.saturating_add(1);
            if !piece.is_empty() && !emit(&piece) {
                finish_reason = "consumer_disconnected";
                break;
            }
            batch.clear();
            batch
                .add(token, next_position, &[0], true)
                .map_err(|error| LlmError::Runtime(error.to_string()))?;
            next_position = next_position.saturating_add(1);
            context
                .decode(&mut batch)
                .map_err(|error| LlmError::Runtime(error.to_string()))?;
        }

        Ok(LlmGenerationStats {
            prompt_tokens: prompt_tokens.len() as u64,
            completion_tokens: u64::from(completion_tokens),
            elapsed_ms: elapsed_ms(started),
            finish_reason,
        })
    }
}

fn model_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map_or_else(|| path.display().to_string(), |value| value.to_owned())
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LlmGenerationStats {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub elapsed_ms: u64,
    pub finish_reason: &'static str,
}

#[derive(Debug)]
pub enum LlmError {
    Unconfigured,
    Busy(String),
    Backend(String),
    Model(String),
    Template(String),
    Request(String),
    Runtime(String),
}

impl LlmError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Unconfigured => "llm.unconfigured",
            Self::Busy(_) => "llm.queue_full",
            Self::Backend(_) => "llm.backend_failed",
            Self::Model(_) => "llm.model_failed",
            Self::Template(_) => "llm.template_failed",
            Self::Request(_) => "llm.invalid_request",
            Self::Runtime(_) => "llm.generation_failed",
        }
    }
}

impl Display for LlmError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unconfigured => {
                formatter.write_str("LLM provider is unconfigured; set OGD_LLM_MODEL")
            }
            Self::Busy(message)
            | Self::Backend(message)
            | Self::Model(message)
            | Self::Template(message)
            | Self::Request(message)
            | Self::Runtime(message) => formatter.write_str(message),
        }
    }
}

impl Error for LlmError {}
