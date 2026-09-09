use std::{
    collections::HashMap,
    error::Error,
    fmt::{self, Display, Formatter},
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};

use encoding_rs::UTF_8;
use llama_cpp_2::{
    list_llama_ggml_backend_devices,
    context::params::LlamaContextParams,
    context::session::{LlamaStateSeqFlags, SeqState},
    llama_backend::LlamaBackend,
    llama_batch::LlamaBatch,
    model::{params::LlamaModelParams, AddBos, LlamaChatMessage, LlamaChatTemplate, LlamaModel},
    sampling::LlamaSampler,
    token::LlamaToken,
};
use og_llama_common_chat::{
    self as common_chat, CommonChatPlan, CommonChatThinkingMode, CommonChatToolChoice,
};
use serde_json::{json, Value as JsonValue};

use crate::{
    debug::{self, DebugTopic},
    operation::{LlmGenerateInput, LlmMessageInput, LlmToolChoice, LlmToolDefinition},
};

use super::{
    config::{LlmConfig, LlmDevice, LlmGpuLayers, LlmThinking},
    scheduler::{LlmScheduler, ScheduledRun},
};

const MIN_PREFIX_CACHE_TOKENS: usize = 128;

pub struct LlmRuntime {
    backend: LlamaBackend,
    model: LlamaModel,
    template: LlamaChatTemplate,
    tool_template: Option<LlamaChatTemplate>,
    model_name: String,
    context_size: std::num::NonZeroU32,
    threads: Option<i32>,
    max_tokens: u32,
    seed: u32,
    add_bos: AddBos,
    gpu_offload: bool,
    thinking: LlmThinking,
    prefix_cache: Mutex<PromptPrefixCache>,
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
        let (model_params, device) = model_params_for_config(&backend, config)?;
        log_device_selection(&backend, config, device.as_ref());
        let model = LlamaModel::load_from_file(&backend, path, &model_params)
            .map_err(|error| LlmError::Model(error.to_string()))?;
        eprintln!(
            "[llm] model.layers={} gpuLayers.requested={}",
            model.n_layer(),
            config.gpu_layers.label()
        );
        let (template, tool_template) = match config.chat_template.as_deref() {
            Some(template) => (
                LlamaChatTemplate::new(template)
                    .map_err(|error| LlmError::Template(error.to_string()))?,
                None,
            ),
            None => (
                model.chat_template(None).map_err(|error| {
                    LlmError::Template(format!(
                        "model has no usable default chat template ({error}); set OGD_LLM_CHAT_TEMPLATE"
                    ))
                })?,
                model.chat_template(Some("tool_use")).ok(),
            ),
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
        let prefix_cache_max_bytes = usize::try_from(config.prefix_cache_max_bytes)
            .unwrap_or(usize::MAX);
        eprintln!(
            "[llm] prefixCache.max={} MiB",
            bytes_to_mib(prefix_cache_max_bytes)
        );
        eprintln!(
            "[llm] thinking.requested={} autoPolicy=template-native-without-tools,off-with-tools",
            config.thinking.as_str()
        );
        Ok(Self {
            backend,
            model,
            template,
            tool_template,
            model_name: model_name(path),
            context_size: config.context_size,
            threads: config.threads,
            max_tokens: config.max_tokens,
            seed: config.seed,
            add_bos,
            gpu_offload: device.is_some(),
            thinking: config.thinking,
            prefix_cache: Mutex::new(PromptPrefixCache::new(prefix_cache_max_bytes)),
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
        F: FnMut(LlmGenerationEvent) -> bool,
    {
        let started = Instant::now();
        if !run.wait_until_active()? {
            return Ok(LlmGenerationStats {
                prompt_tokens: 0,
                cached_prompt_tokens: 0,
                completion_tokens: 0,
                elapsed_ms: elapsed_ms(started),
                finish_reason: "cancelled",
            });
        }
        if run.is_cancelled() {
            return Ok(LlmGenerationStats {
                prompt_tokens: 0,
                cached_prompt_tokens: 0,
                completion_tokens: 0,
                elapsed_ms: elapsed_ms(started),
                finish_reason: "cancelled",
            });
        }

        let use_common_chat = request_uses_tools(request)
            || !matches!(self.thinking, LlmThinking::Auto);
        let native_plan = if use_common_chat {
            Some(self.common_chat_plan(request)?)
        } else {
            None
        };
        let prompt = if let Some(plan) = native_plan.as_ref() {
            plan.prompt.clone()
        } else {
            let messages = request
                .messages
                .iter()
                .map(|message| {
                    LlamaChatMessage::new(message.role.clone(), message.content.clone())
                        .map_err(|error| LlmError::Request(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.model
                .apply_chat_template(&self.template, &messages, true)
                .map_err(|error| LlmError::Template(error.to_string()))?
        };

        let prompt_add_bos = if native_plan.is_some() && matches!(self.add_bos, AddBos::Always) {
            let bos = self.special_token_piece(self.model.token_bos());
            if !bos.is_empty() && prompt.starts_with(&bos) {
                AddBos::Never
            } else {
                self.add_bos
            }
        } else {
            self.add_bos
        };
        let prompt_tokens = self
            .model
            .str_to_token(&prompt, prompt_add_bos)
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

        let cache_capture_allowed = request.cache_key.is_some()
            && request.tool_choice != LlmToolChoice::None
            && prompt_tokens.len() >= MIN_PREFIX_CACHE_TOKENS;
        let cache_plan = match request.cache_key.as_deref() {
            Some(cache_key) => match self.prefix_cache.lock() {
                Ok(mut cache) => cache.plan(cache_key, &prompt_tokens, cache_capture_allowed),
                Err(_) => PrefixCachePlan::Disabled,
            },
            None => PrefixCachePlan::Disabled,
        };

        let prompt_batch_size = u32::try_from(prompt_tokens.len())
            .map_err(|_| LlmError::Request("prompt token count exceeds u32::MAX".to_owned()))?;
        let make_context = || {
            let mut context_params = LlamaContextParams::default()
                .with_n_ctx(Some(self.context_size))
                .with_n_batch(prompt_batch_size.max(1));
            if !self.gpu_offload {
                context_params = context_params
                    .with_offload_kqv(false)
                    .with_op_offload(false);
            }
            if let Some(threads) = self.threads {
                context_params = context_params
                    .with_n_threads(threads)
                    .with_n_threads_batch(threads);
            }
            self.model
                .new_context(&self.backend, context_params)
                .map_err(|error| LlmError::Runtime(error.to_string()))
        };
        let mut context = make_context()?;
        let mut next_position = i32::try_from(prompt_tokens.len())
            .map_err(|_| LlmError::Request("prompt token count exceeds i32::MAX".to_owned()))?;

        let mut batch = LlamaBatch::new(prompt_tokens.len().max(1), 1);
        let mut cached_prompt_tokens = 0usize;
        let mut cache_hit = false;
        let mut pending_cache_state: Option<(usize, Arc<SeqState>)> = None;

        match cache_plan {
            PrefixCachePlan::Disabled => {
                add_tokens_at(&mut batch, &prompt_tokens, 0)?;
                context
                    .decode(&mut batch)
                    .map_err(|error| LlmError::Runtime(error.to_string()))?;
            }
            PrefixCachePlan::Miss {
                capture_tokens,
                common_prefix_tokens,
            } => {
                if debug::enabled(DebugTopic::Llm) && request.cache_key.is_some() {
                    debug::log(
                        DebugTopic::Llm,
                        None,
                        format!(
                            "prefix cache miss commonPrefixTokens={common_prefix_tokens} captureTokens={capture_tokens} promptTokens={}",
                            prompt_tokens.len(),
                        ),
                    );
                }

                if capture_tokens > 0 && capture_tokens < prompt_tokens.len() {
                    add_tokens_at(&mut batch, &prompt_tokens[..capture_tokens], 0)?;
                    context
                        .decode(&mut batch)
                        .map_err(|error| LlmError::Runtime(error.to_string()))?;
                    match capture_sequence_state(&context) {
                        Ok(state) => pending_cache_state = Some((capture_tokens, state)),
                        Err(error) if debug::enabled(DebugTopic::Llm) => {
                            debug::log(
                                DebugTopic::Llm,
                                None,
                                format!("prefix cache snapshot failed: {error}"),
                            );
                        }
                        Err(_) => {}
                    }

                    batch.clear();
                    add_tokens_at(
                        &mut batch,
                        &prompt_tokens[capture_tokens..],
                        capture_tokens,
                    )?;
                    context
                        .decode(&mut batch)
                        .map_err(|error| LlmError::Runtime(error.to_string()))?;
                } else {
                    add_tokens_at(&mut batch, &prompt_tokens, 0)?;
                    context
                        .decode(&mut batch)
                        .map_err(|error| LlmError::Runtime(error.to_string()))?;
                    if capture_tokens == prompt_tokens.len() && capture_tokens > 0 {
                        match capture_sequence_state(&context) {
                            Ok(state) => pending_cache_state = Some((capture_tokens, state)),
                            Err(error) if debug::enabled(DebugTopic::Llm) => {
                                debug::log(
                                    DebugTopic::Llm,
                                    None,
                                    format!("prefix cache snapshot failed: {error}"),
                                );
                            }
                            Err(_) => {}
                        }
                    }
                }
            }
            PrefixCachePlan::Hit {
                prefix_tokens,
                state,
                state_bytes,
            } => {
                match context.state_seq_set(state.as_ref(), 0) {
                    Ok(()) => {
                        cached_prompt_tokens = prefix_tokens;
                        cache_hit = true;
                        add_tokens_at(
                            &mut batch,
                            &prompt_tokens[prefix_tokens..],
                            prefix_tokens,
                        )?;
                        context
                            .decode(&mut batch)
                            .map_err(|error| LlmError::Runtime(error.to_string()))?;
                        if debug::enabled(DebugTopic::Llm) {
                            debug::log(
                                DebugTopic::Llm,
                                None,
                                format!(
                                    "prefix cache hit reusedTokens={prefix_tokens} suffixTokens={} stateMiB={} promptTokens={}",
                                    prompt_tokens.len().saturating_sub(prefix_tokens),
                                    bytes_to_mib(state_bytes),
                                    prompt_tokens.len(),
                                ),
                            );
                        }
                    }
                    Err(error) => {
                        if debug::enabled(DebugTopic::Llm) {
                            debug::log(
                                DebugTopic::Llm,
                                None,
                                format!(
                                    "prefix cache restore failed; rebuilding prompt: {error}"
                                ),
                            );
                        }
                        if let Some(cache_key) = request.cache_key.as_deref() {
                            if let Ok(mut cache) = self.prefix_cache.lock() {
                                cache.remove(cache_key);
                            }
                        }
                        context = make_context()?;
                        batch.clear();
                        add_tokens_at(&mut batch, &prompt_tokens, 0)?;
                        context
                            .decode(&mut batch)
                            .map_err(|error| LlmError::Runtime(error.to_string()))?;
                        if cache_capture_allowed {
                            match capture_sequence_state(&context) {
                                Ok(state) => {
                                    pending_cache_state = Some((prompt_tokens.len(), state));
                                }
                                Err(snapshot_error) if debug::enabled(DebugTopic::Llm) => {
                                    debug::log(
                                        DebugTopic::Llm,
                                        None,
                                        format!(
                                            "prefix cache fallback snapshot failed: {snapshot_error}"
                                        ),
                                    );
                                }
                                Err(_) => {}
                            }
                        }
                    }
                }
            }
        }

        let seed = request.seed.unwrap_or(self.seed);
        let mut sampler_chain = Vec::with_capacity(4);
        if let Some(plan) = native_plan.as_ref() {
            if !plan.grammar.is_empty() {
                // llama.cpp b10200 tool grammars are unsafe in-process for the
                // Qwen3 JSON_NATIVE/per-call format used here. We have observed
                // `GGML_ASSERT(!stacks.empty())` both when a lazy AUTO grammar
                // is triggered and immediately after installing a non-lazy
                // REQUIRED grammar. Either path aborts the whole daemon rather
                // than returning an error.
                //
                // Keep common_chat for prompt rendering and, critically, for
                // parsing native `<tool_call>` output, but never install its
                // tool grammar in the sampler. Tool choice remains part of the
                // rendered chat template; the Agent additionally narrows the
                // exposed tool set for explicit intents. This trades token-level
                // grammar enforcement for process safety until llama.cpp is
                // upgraded to a version where these grammars are safe.
                if debug::enabled(DebugTopic::Llm) {
                    debug::log(
                        DebugTopic::Llm,
                        None,
                        format!(
                            "common/chat tool grammar disabled for process safety; toolChoice={} grammarLazy={} relying on native output + parser",
                            request.tool_choice.as_str(),
                            plan.grammar_lazy,
                        ),
                    );
                }
            }
        }
        sampler_chain.extend([
            LlamaSampler::temp(0.7),
            LlamaSampler::top_p(0.9, 1),
            LlamaSampler::dist(seed),
        ]);
        let mut sampler = LlamaSampler::chain_simple(sampler_chain);
        let mut decoder = UTF_8.new_decoder();
        let mut completion_tokens = 0_u32;
        let mut finish_reason = "length";
        let mut generated = String::new();
        let mut generated_tool_calls = false;

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

            if let Some(plan) = native_plan.as_ref() {
                generated.push_str(&piece);
                if let Some(stop_at) = first_stop(&generated, &plan.additional_stops) {
                    generated.truncate(stop_at);
                    finish_reason = "stop";
                    break;
                }
            } else if !piece.is_empty() && !emit(LlmGenerationEvent::Text(piece)) {
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

        if let Some(plan) = native_plan.as_ref() {
            if finish_reason != "cancelled" && finish_reason != "consumer_disconnected" {
                if debug::enabled(DebugTopic::Llm) {
                    debug::log(
                        DebugTopic::Llm,
                        None,
                        format!("common/chat raw generation={generated:?}"),
                    );
                }
                let turn = common_chat::parse(plan, &generated)
                    .map_err(|error| LlmError::Runtime(error.to_string()))?;
                generated_tool_calls = !turn.tool_calls.is_empty();
                if debug::enabled(DebugTopic::Llm) {
                    debug::log(
                        DebugTopic::Llm,
                        None,
                        format!("common/chat parsed turn={turn:?}"),
                    );
                }
                if !turn.content.is_empty()
                    && !emit(LlmGenerationEvent::Text(turn.content))
                {
                    finish_reason = "consumer_disconnected";
                }
                if finish_reason != "consumer_disconnected" {
                    for (index, call) in turn.tool_calls.into_iter().enumerate() {
                        if call.name.trim().is_empty() {
                            return Err(LlmError::Runtime(
                                "llama.cpp parsed a tool call without a name".to_owned(),
                            ));
                        }
                        if !call.arguments.is_object() {
                            return Err(LlmError::Runtime(
                                "llama.cpp parsed non-object tool arguments".to_owned(),
                            ));
                        }
                        let id = if call.id.trim().is_empty() {
                            format!("call-{}", index + 1)
                        } else {
                            call.id
                        };
                        if !emit(LlmGenerationEvent::ToolCall {
                            id,
                            name: call.name,
                            arguments: call.arguments,
                        }) {
                            finish_reason = "consumer_disconnected";
                            break;
                        }
                    }
                }
            }
        }

        if let Some(cache_key) = request.cache_key.as_deref() {
            let keep_cache = generated_tool_calls
                && finish_reason != "cancelled"
                && finish_reason != "consumer_disconnected";
            if let Ok(mut cache) = self.prefix_cache.lock() {
                if keep_cache {
                    if let Some((prefix_tokens, state)) = pending_cache_state {
                        let result = cache.store(
                            cache_key.to_owned(),
                            prompt_tokens.clone(),
                            prefix_tokens,
                            state,
                        );
                        if debug::enabled(DebugTopic::Llm) {
                            debug::log(
                                DebugTopic::Llm,
                                None,
                                if result.stored {
                                    format!(
                                        "prefix cache stored prefixTokens={prefix_tokens} stateMiB={} evicted={} totalMiB={}",
                                        bytes_to_mib(result.state_bytes),
                                        result.evicted,
                                        bytes_to_mib(cache.bytes),
                                    )
                                } else {
                                    format!(
                                        "prefix cache skipped stateMiB={} exceedsMaxMiB={}",
                                        bytes_to_mib(result.state_bytes),
                                        bytes_to_mib(cache.max_bytes),
                                    )
                                },
                            );
                        }
                    } else if cache_hit {
                        cache.update_reference(cache_key, prompt_tokens.clone());
                    }
                } else {
                    let removed = cache.remove(cache_key);
                    if removed && debug::enabled(DebugTopic::Llm) {
                        debug::log(
                            DebugTopic::Llm,
                            None,
                            "prefix cache released at end of tool run",
                        );
                    }
                }
            }
        }

        Ok(LlmGenerationStats {
            prompt_tokens: prompt_tokens.len() as u64,
            cached_prompt_tokens: cached_prompt_tokens as u64,
            completion_tokens: u64::from(completion_tokens),
            elapsed_ms: elapsed_ms(started),
            finish_reason,
        })
    }

    fn common_chat_plan(&self, request: &LlmGenerateInput) -> Result<CommonChatPlan, LlmError> {
        let messages_json = oaicompat_messages_json(&request.messages)?;
        let tools_json = oaicompat_tools_json(&request.tools)?;
        let template = if request.tools.is_empty() {
            &self.template
        } else {
            self.tool_template.as_ref().unwrap_or(&self.template)
        };
        let template = template
            .to_str()
            .map_err(|error| LlmError::Template(error.to_string()))?;
        let bos = self.special_token_piece(self.model.token_bos());
        let eos = self.special_token_piece(self.model.token_eos());
        let thinking_mode = self.common_chat_thinking_mode(request);
        let plan = common_chat::apply(
            template,
            &bos,
            &eos,
            &messages_json,
            &tools_json,
            common_chat_tool_choice(request.tool_choice),
            thinking_mode,
        )
        .map_err(|error| LlmError::Template(error.to_string()))?;
        if debug::enabled(DebugTopic::Llm) {
            let tool_names = request
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>();
            debug::log(
                DebugTopic::Llm,
                None,
                format!(
                    "common/chat plan toolChoice={} thinkingRequested={} thinkingEffective={} supportsThinking={} tools={tool_names:?} format={} reasoningFormat={} grammarLazy={} grammarNeedsPrefill={} generationPrompt={:?} triggerPatterns={:?} triggerTokens={:?} additionalStops={:?}",
                    request.tool_choice.as_str(),
                    self.thinking.as_str(),
                    thinking_mode.as_str(),
                    plan.supports_thinking,
                    plan.format,
                    plan.reasoning_format,
                    plan.grammar_lazy,
                    plan.grammar_needs_prefill,
                    plan.generation_prompt,
                    plan.trigger_patterns,
                    plan.trigger_tokens,
                    plan.additional_stops,
                ),
            );
        }
        Ok(plan)
    }

    fn common_chat_thinking_mode(&self, request: &LlmGenerateInput) -> CommonChatThinkingMode {
        match self.thinking {
            LlmThinking::On => CommonChatThinkingMode::On,
            LlmThinking::Off => CommonChatThinkingMode::Off,
            LlmThinking::Auto => {
                if request_uses_tools(request) {
                    CommonChatThinkingMode::Off
                } else {
                    CommonChatThinkingMode::Auto
                }
            }
        }
    }

    fn special_token_piece(&self, token: LlamaToken) -> String {
        let mut decoder = UTF_8.new_decoder();
        self.model
            .token_to_piece(token, &mut decoder, true, None)
            .unwrap_or_default()
    }
}

fn common_chat_tool_choice(choice: LlmToolChoice) -> CommonChatToolChoice {
    match choice {
        LlmToolChoice::Auto => CommonChatToolChoice::Auto,
        LlmToolChoice::None => CommonChatToolChoice::None,
        LlmToolChoice::Required => CommonChatToolChoice::Required,
    }
}

fn request_uses_tools(request: &LlmGenerateInput) -> bool {
    !request.tools.is_empty()
        || request.messages.iter().any(|message| {
            !message.tool_calls.is_empty()
                || message.tool_call_id.is_some()
                || message.role.eq_ignore_ascii_case("tool")
        })
}

fn oaicompat_messages_json(messages: &[LlmMessageInput]) -> Result<String, LlmError> {
    let messages = messages
        .iter()
        .map(|message| {
            let mut value = json!({
                "role": message.role,
                "content": message.content,
            });
            if let Some(tool_call_id) = message.tool_call_id.as_ref() {
                value["tool_call_id"] = JsonValue::String(tool_call_id.clone());
            }
            if !message.tool_calls.is_empty() {
                value["tool_calls"] = JsonValue::Array(
                    message
                        .tool_calls
                        .iter()
                        .map(|tool_call| {
                            Ok(json!({
                                "id": tool_call.id,
                                "type": "function",
                                "function": {
                                    "name": tool_call.name,
                                    "arguments": serde_json::to_string(&tool_call.arguments)
                                        .map_err(|error| LlmError::Request(error.to_string()))?,
                                }
                            }))
                        })
                        .collect::<Result<Vec<_>, LlmError>>()?,
                );
            }
            Ok(value)
        })
        .collect::<Result<Vec<_>, LlmError>>()?;
    serde_json::to_string(&messages).map_err(|error| LlmError::Request(error.to_string()))
}

fn oaicompat_tools_json(tools: &[LlmToolDefinition]) -> Result<String, LlmError> {
    let tools = tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                }
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&tools).map_err(|error| LlmError::Request(error.to_string()))
}

fn first_stop(generated: &str, stops: &[String]) -> Option<usize> {
    stops
        .iter()
        .filter(|stop| !stop.is_empty())
        .filter_map(|stop| generated.find(stop))
        .min()
}


#[derive(Debug)]
struct PromptPrefixCache {
    entries: HashMap<String, PromptPrefixCacheEntry>,
    bytes: usize,
    max_bytes: usize,
    clock: u64,
}

#[derive(Debug)]
struct PromptPrefixCacheEntry {
    /// Full prompt from the most recent request using this key. This is kept
    /// only to discover how far the stable prefix moved when compaction or
    /// tool exposure changes the rendered prompt.
    reference_tokens: Vec<LlamaToken>,
    /// Number of tokens represented by `state`. The snapshot can be shorter
    /// than `reference_tokens`: after the first mismatch we intentionally keep
    /// the stable common prefix instead of snapshotting the mutable tail.
    prefix_tokens: usize,
    state: Arc<SeqState>,
    state_bytes: usize,
    last_used: u64,
}

#[derive(Debug)]
enum PrefixCachePlan {
    Disabled,
    Miss {
        capture_tokens: usize,
        common_prefix_tokens: usize,
    },
    Hit {
        prefix_tokens: usize,
        state: Arc<SeqState>,
        state_bytes: usize,
    },
}

#[derive(Debug, Clone, Copy)]
struct PrefixCacheStoreResult {
    stored: bool,
    state_bytes: usize,
    evicted: usize,
}

impl PromptPrefixCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            bytes: 0,
            max_bytes,
            clock: 0,
        }
    }

    fn plan(
        &mut self,
        key: &str,
        prompt_tokens: &[LlamaToken],
        allow_capture: bool,
    ) -> PrefixCachePlan {
        if self.max_bytes == 0 {
            return PrefixCachePlan::Disabled;
        }
        self.clock = self.clock.wrapping_add(1);
        let now = self.clock;

        let Some(entry) = self.entries.get(key) else {
            return PrefixCachePlan::Miss {
                capture_tokens: cache_capture_length(prompt_tokens.len(), 0, allow_capture),
                common_prefix_tokens: 0,
            };
        };
        let common_prefix_tokens = common_prefix_len(&entry.reference_tokens, prompt_tokens);
        let can_restore = entry.prefix_tokens >= MIN_PREFIX_CACHE_TOKENS
            && entry.prefix_tokens < prompt_tokens.len()
            && entry.prefix_tokens <= common_prefix_tokens;
        if can_restore {
            let entry = self.entries.get_mut(key).expect("prefix cache entry still exists");
            entry.last_used = now;
            return PrefixCachePlan::Hit {
                prefix_tokens: entry.prefix_tokens,
                state: Arc::clone(&entry.state),
                state_bytes: entry.state_bytes,
            };
        }

        self.remove(key);
        PrefixCachePlan::Miss {
            capture_tokens: cache_capture_length(
                prompt_tokens.len(),
                common_prefix_tokens,
                allow_capture,
            ),
            common_prefix_tokens,
        }
    }

    fn store(
        &mut self,
        key: String,
        reference_tokens: Vec<LlamaToken>,
        prefix_tokens: usize,
        state: Arc<SeqState>,
    ) -> PrefixCacheStoreResult {
        let state_bytes = state.byte_len();
        self.remove(&key);
        if self.max_bytes == 0 || state_bytes > self.max_bytes {
            return PrefixCacheStoreResult {
                stored: false,
                state_bytes,
                evicted: 0,
            };
        }

        let mut evicted = 0usize;
        while self.bytes.saturating_add(state_bytes) > self.max_bytes {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            let Some(oldest) = oldest else { break; };
            self.remove(&oldest);
            evicted = evicted.saturating_add(1);
        }

        self.clock = self.clock.wrapping_add(1);
        self.bytes = self.bytes.saturating_add(state_bytes);
        self.entries.insert(
            key,
            PromptPrefixCacheEntry {
                reference_tokens,
                prefix_tokens,
                state,
                state_bytes,
                last_used: self.clock,
            },
        );
        PrefixCacheStoreResult {
            stored: true,
            state_bytes,
            evicted,
        }
    }

    fn update_reference(&mut self, key: &str, reference_tokens: Vec<LlamaToken>) {
        self.clock = self.clock.wrapping_add(1);
        if let Some(entry) = self.entries.get_mut(key) {
            entry.reference_tokens = reference_tokens;
            entry.last_used = self.clock;
        }
    }

    fn remove(&mut self, key: &str) -> bool {
        let Some(entry) = self.entries.remove(key) else {
            return false;
        };
        self.bytes = self.bytes.saturating_sub(entry.state_bytes);
        true
    }
}

fn cache_capture_length(
    prompt_tokens: usize,
    common_prefix_tokens: usize,
    allow_capture: bool,
) -> usize {
    if !allow_capture || prompt_tokens < MIN_PREFIX_CACHE_TOKENS {
        return 0;
    }
    if common_prefix_tokens >= MIN_PREFIX_CACHE_TOKENS
        && common_prefix_tokens < prompt_tokens
    {
        common_prefix_tokens
    } else {
        prompt_tokens
    }
}

fn common_prefix_len(left: &[LlamaToken], right: &[LlamaToken]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

fn capture_sequence_state(
    context: &llama_cpp_2::context::LlamaContext<'_>,
) -> Result<Arc<SeqState>, String> {
    context
        .state_seq_get(0, LlamaStateSeqFlags::empty())
        .map(Arc::new)
        .map_err(|error| error.to_string())
}

fn add_tokens_at(
    batch: &mut LlamaBatch<'_>,
    tokens: &[LlamaToken],
    start_position: usize,
) -> Result<(), LlmError> {
    for (offset, token) in tokens.iter().enumerate() {
        let absolute = start_position
            .checked_add(offset)
            .ok_or_else(|| LlmError::Request("prompt position overflow".to_owned()))?;
        let position = i32::try_from(absolute)
            .map_err(|_| LlmError::Request("prompt position exceeds i32::MAX".to_owned()))?;
        let logits = offset.saturating_add(1) == tokens.len();
        batch
            .add(*token, position, &[0], logits)
            .map_err(|error| LlmError::Runtime(error.to_string()))?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct SelectedGpu {
    index: usize,
    name: String,
    description: String,
    backend: String,
    memory_total: usize,
    memory_free: usize,
}

fn model_params_for_config(
    backend: &LlamaBackend,
    config: &LlmConfig,
) -> Result<(LlamaModelParams, Option<SelectedGpu>), LlmError> {
    let mut params = LlamaModelParams::default();
    let cuda_device = list_llama_ggml_backend_devices()
        .into_iter()
        .find(|device| device.backend.eq_ignore_ascii_case("cuda"));
    let supports_gpu_offload = backend.supports_gpu_offload();

    let selected = match config.device {
        LlmDevice::Cpu => {
            params = params.with_n_gpu_layers(0);
            None
        }
        LlmDevice::Auto => {
            if supports_gpu_offload {
                if let Some(device) = cuda_device.as_ref() {
                    params = params
                        .with_devices(&[device.index])
                        .map_err(|error| LlmError::Backend(format!("cannot select CUDA device: {error}")))?;
                    Some(selected_gpu(device))
                } else {
                    params = params.with_n_gpu_layers(0);
                    None
                }
            } else {
                params = params.with_n_gpu_layers(0);
                None
            }
        }
        LlmDevice::Cuda => {
            if !supports_gpu_offload {
                let build_hint = if cfg!(feature = "cuda") {
                    "the CUDA backend is compiled in; check the NVIDIA driver and container runtime"
                } else {
                    "build og-core with the `cuda` feature, or provide a dynamic CUDA backend"
                };
                return Err(LlmError::Backend(format!(
                    "OGD_LLM_DEVICE=cuda requested, but llama.cpp reports no usable GPU offload backend; {build_hint}"
                )));
            }
            let device = cuda_device.as_ref().ok_or_else(|| {
                let build_hint = if cfg!(feature = "cuda") {
                    "check CUDA visibility and the NVIDIA container runtime"
                } else {
                    "build og-core with the `cuda` feature"
                };
                LlmError::Backend(format!(
                    "OGD_LLM_DEVICE=cuda requested, but no CUDA device is visible to llama.cpp; {build_hint}"
                ))
            })?;
            params = params
                .with_devices(&[device.index])
                .map_err(|error| LlmError::Backend(format!("cannot select CUDA device: {error}")))?;
            Some(selected_gpu(device))
        }
    };

    if selected.is_some() {
        params = match config.gpu_layers {
            LlmGpuLayers::Auto => params,
            LlmGpuLayers::All => params.with_n_gpu_layers(u32::MAX),
            LlmGpuLayers::Count(count) => params.with_n_gpu_layers(count),
        };
    }

    Ok((params, selected))
}

fn selected_gpu(device: &llama_cpp_2::LlamaBackendDevice) -> SelectedGpu {
    SelectedGpu {
        index: device.index,
        name: device.name.clone(),
        description: device.description.clone(),
        backend: device.backend.clone(),
        memory_total: device.memory_total,
        memory_free: device.memory_free,
    }
}

fn log_device_selection(
    backend: &LlamaBackend,
    config: &LlmConfig,
    selected: Option<&SelectedGpu>,
) {
    eprintln!(
        "[llm] backend.cuda.compiled={} backend.gpuOffload={}",
        cfg!(feature = "cuda"),
        backend.supports_gpu_offload()
    );
    eprintln!("[llm] device.requested={}", config.device.as_str());
    match selected {
        Some(device) => {
            eprintln!(
                "[llm] device.selected=cuda:{} backend={} name={:?} description={:?}",
                device.index, device.backend, device.name, device.description
            );
            eprintln!(
                "[llm] gpu.vram.total={} MiB gpu.vram.free={} MiB gpuLayers.requested={}",
                bytes_to_mib(device.memory_total),
                bytes_to_mib(device.memory_free),
                config.gpu_layers.label()
            );
        }
        None => {
            eprintln!("[llm] device.selected=cpu");
            if !matches!(config.gpu_layers, LlmGpuLayers::Auto) {
                eprintln!(
                    "[llm] gpuLayers.requested={} ignored because CPU is selected",
                    config.gpu_layers.label()
                );
            }
        }
    }
}

fn bytes_to_mib(bytes: usize) -> usize {
    bytes / (1024 * 1024)
}

fn model_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map_or_else(|| path.display().to_string(), |value| value.to_owned())
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

#[derive(Debug, Clone, PartialEq)]
pub enum LlmGenerationEvent {
    Text(String),
    ToolCall {
        id: String,
        name: String,
        arguments: JsonValue,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LlmGenerationStats {
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
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
            Self::Unconfigured => formatter.write_str("LLM service is not configured"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_prefix_stops_at_first_difference() {
        let left = [
            LlamaToken::new(1),
            LlamaToken::new(2),
            LlamaToken::new(3),
            LlamaToken::new(4),
        ];
        let right = [
            LlamaToken::new(1),
            LlamaToken::new(2),
            LlamaToken::new(9),
            LlamaToken::new(4),
        ];
        assert_eq!(common_prefix_len(&left, &right), 2);
    }

    #[test]
    fn cache_capture_prefers_stable_common_prefix() {
        assert_eq!(cache_capture_length(1000, 0, true), 1000);
        assert_eq!(cache_capture_length(1000, 650, true), 650);
        assert_eq!(cache_capture_length(1000, 64, true), 1000);
        assert_eq!(cache_capture_length(1000, 650, false), 0);
    }
}
