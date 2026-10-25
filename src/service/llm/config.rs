#![cfg_attr(rustfmt, rustfmt_skip)]
use std::{
    env,
    error::Error,
    fmt::{self, Display, Formatter},
    num::NonZeroU32,
    path::PathBuf,
};

const DEFAULT_CONTEXT_SIZE: u32 = 4096;
const DEFAULT_MAX_TOKENS: u32 = 512;
const DEFAULT_SEED: u32 = 1234;
const DEFAULT_MAX_PARALLEL: u32 = 1;
const DEFAULT_QUEUE_SIZE: u32 = 16;
/// Room for the prefixes of a Place (routing and intent): the state of a 31B dense model at 4k tokens is 1.1 GiB.
const DEFAULT_PREFIX_CACHE_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const _: () = assert!(DEFAULT_CONTEXT_SIZE > 0 && DEFAULT_MAX_TOKENS > 0 && DEFAULT_MAX_PARALLEL > 0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmDevice {
    Auto,
    Cpu,
    Cuda,
}

impl LlmDevice {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Cpu => "cpu",
            Self::Cuda => "cuda",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmGpuLayers {
    Auto,
    All,
    Count(u32),
}

impl LlmGpuLayers {
    pub fn label(self) -> String {
        match self {
            Self::Auto => "auto".to_owned(),
            Self::All => "all".to_owned(),
            Self::Count(value) => value.to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmThinking {
    Auto,
    On,
    Off,
}

impl LlmThinking {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::On => "on",
            Self::Off => "off",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmConfig {
    pub model_path: Option<PathBuf>,
    pub context_size: NonZeroU32,
    pub threads: Option<i32>,
    pub max_tokens: u32,
    pub seed: u32,
    pub max_parallel: u32,
    pub queue_size: u32,
    pub chat_template: Option<String>,
    pub device: LlmDevice,
    pub gpu_layers: LlmGpuLayers,
    /// Thinking policy for chat-template rendering. `auto` preserves the
    /// existing tool-safe behavior: native/template behavior without tools and
    /// no-think for tool-oriented turns. `on` and `off` force the mode for all
    /// generations through llama.cpp common/chat.
    pub thinking: LlmThinking,
    /// Maximum host-memory budget used by ephemeral llama.cpp sequence-state
    /// snapshots for intra-Agent prompt-prefix reuse. Set to 0 to disable.
    pub prefix_cache_max_bytes: u64,
    /// Pooling of `llm.embed` (`OGD_LLM_POOLING`: mean, cls, last or none); `None` keeps the model's own.
    pub pooling: Option<LlmPooling>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmPooling { None, Mean, Cls, Last }

impl LlmPooling {
    pub const fn as_str(self) -> &'static str { match self { Self::None => "none", Self::Mean => "mean", Self::Cls => "cls", Self::Last => "last" } }
}

impl LlmConfig {
    pub fn from_environment() -> Result<Self, LlmConfigError> {
        let pooling = [("", None), ("model", None), ("none", Some(LlmPooling::None)), ("mean", Some(LlmPooling::Mean)), ("cls", Some(LlmPooling::Cls)), ("last", Some(LlmPooling::Last))];
        let gpu_layers = match var("OGD_LLM_GPU_LAYERS")? {
            None => LlmGpuLayers::Auto,
            Some(value) => match value.trim().to_ascii_lowercase().as_str() {
                "auto" => LlmGpuLayers::Auto, "all" => LlmGpuLayers::All,
                layers => LlmGpuLayers::Count(layers.parse().map_err(|source| LlmConfigError::Integer { name: "OGD_LLM_GPU_LAYERS", value: value.clone(), source })?),
            },
        };
        Ok(Self {
            model_path: crate::helpers::env_text("OGD_LLM_MODEL").map(PathBuf::from),
            context_size: NonZeroU32::new(positive("OGD_LLM_CONTEXT_SIZE", DEFAULT_CONTEXT_SIZE)?).unwrap_or(NonZeroU32::MIN),
            // Every core by default: llama.cpp's own default (4, and one busy core measured with a 35B partly on CPU)
            // leaves the CPU part of a model idle.
            threads: match var("OGD_LLM_THREADS")? { Some(_) => Some(positive("OGD_LLM_THREADS", 1)?), None => std::thread::available_parallelism().ok().and_then(|cores| i32::try_from(cores.get()).ok()) },
            max_tokens: positive("OGD_LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS)?,
            seed: integer("OGD_LLM_SEED", DEFAULT_SEED)?,
            max_parallel: positive("OGD_LLM_MAX_PARALLEL", DEFAULT_MAX_PARALLEL)?,
            queue_size: integer("OGD_LLM_QUEUE_SIZE", DEFAULT_QUEUE_SIZE)?,
            chat_template: crate::helpers::env_text("OGD_LLM_CHAT_TEMPLATE"),
            device: choice("OGD_LLM_DEVICE", LlmDevice::Auto, &[("auto", LlmDevice::Auto), ("cpu", LlmDevice::Cpu), ("cuda", LlmDevice::Cuda)], "auto, cpu or cuda")?,
            gpu_layers,
            thinking: choice("OGD_LLM_THINKING", LlmThinking::Auto, &[("auto", LlmThinking::Auto), ("on", LlmThinking::On), ("true", LlmThinking::On), ("1", LlmThinking::On), ("off", LlmThinking::Off), ("false", LlmThinking::Off), ("0", LlmThinking::Off)], "auto, on or off")?,
            prefix_cache_max_bytes: integer("OGD_LLM_PREFIX_CACHE_MAX_BYTES", DEFAULT_PREFIX_CACHE_MAX_BYTES)?,
            pooling: choice("OGD_LLM_POOLING", None, &pooling, "model, mean, cls, last or none")?,
        })
    }
}

fn var(name: &'static str) -> Result<Option<String>, LlmConfigError> { crate::helpers::env_var(name).map_err(|source| LlmConfigError::Environment { name, source }) }

/// An integer setting, `default` when it is unset.
fn integer<T: std::str::FromStr<Err = std::num::ParseIntError>>(name: &'static str, default: T) -> Result<T, LlmConfigError> {
    var(name)?.map_or(Ok(default), |value| value.parse().map_err(|source| LlmConfigError::Integer { name, value, source }))
}

/// An integer setting greater than zero.
fn positive<T: std::str::FromStr<Err = std::num::ParseIntError> + PartialOrd + Default>(name: &'static str, default: T) -> Result<T, LlmConfigError> {
    let value = integer(name, default)?;
    if value <= T::default() { return Err(LlmConfigError::Invalid(format!("{name} must be greater than zero"))); }
    Ok(value)
}

/// A setting among `choices` (in any case), `default` when it is unset.
fn choice<T: Copy>(name: &'static str, default: T, choices: &[(&str, T)], expected: &str) -> Result<T, LlmConfigError> {
    let Some(value) = var(name)? else { return Ok(default) };
    let key = value.trim().to_ascii_lowercase();
    choices.iter().find(|(word, _)| *word == key).map(|(_, choice)| *choice).ok_or_else(|| LlmConfigError::Invalid(format!("{name} must be one of {expected}; got {value:?}")))
}

#[derive(Debug)]
pub enum LlmConfigError {
    Environment {
        name: &'static str,
        source: env::VarError,
    },
    Integer {
        name: &'static str,
        value: String,
        source: std::num::ParseIntError,
    },
    Invalid(String),
}

impl Display for LlmConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Environment { name, source } => write!(formatter, "cannot read {name}: {source}"),
            Self::Integer {
                name,
                value,
                source,
            } => write!(formatter, "invalid {name} value {value:?}: {source}"),
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl Error for LlmConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Environment { source, .. } => Some(source),
            Self::Integer { source, .. } => Some(source),
            Self::Invalid(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn thinking_labels_are_stable() { assert_eq!(LlmThinking::Auto.as_str(), "auto"); assert_eq!(LlmThinking::On.as_str(), "on"); assert_eq!(LlmThinking::Off.as_str(), "off"); }
}
