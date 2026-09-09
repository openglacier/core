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
const DEFAULT_PREFIX_CACHE_MAX_BYTES: u64 = 1024 * 1024 * 1024;

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
}

impl LlmConfig {
    pub fn from_environment() -> Result<Self, LlmConfigError> {
        let model_path = env::var("OGD_LLM_MODEL")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        let context_size = parse_u32("OGD_LLM_CONTEXT_SIZE", DEFAULT_CONTEXT_SIZE)?;
        let context_size = NonZeroU32::new(context_size).ok_or_else(|| {
            LlmConfigError::Invalid("OGD_LLM_CONTEXT_SIZE must be greater than zero".to_owned())
        })?;
        let threads = match env::var("OGD_LLM_THREADS") {
            Ok(value) => {
                let parsed = value
                    .parse::<i32>()
                    .map_err(|source| LlmConfigError::Integer {
                        name: "OGD_LLM_THREADS",
                        value,
                        source,
                    })?;
                if parsed <= 0 {
                    return Err(LlmConfigError::Invalid(
                        "OGD_LLM_THREADS must be greater than zero".to_owned(),
                    ));
                }
                Some(parsed)
            }
            Err(env::VarError::NotPresent) => None,
            Err(source) => {
                return Err(LlmConfigError::Environment {
                    name: "OGD_LLM_THREADS",
                    source,
                })
            }
        };
        let max_tokens = parse_u32("OGD_LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS)?;
        if max_tokens == 0 {
            return Err(LlmConfigError::Invalid(
                "OGD_LLM_MAX_TOKENS must be greater than zero".to_owned(),
            ));
        }
        let seed = parse_u32("OGD_LLM_SEED", DEFAULT_SEED)?;
        let max_parallel = parse_u32("OGD_LLM_MAX_PARALLEL", DEFAULT_MAX_PARALLEL)?;
        if max_parallel == 0 {
            return Err(LlmConfigError::Invalid(
                "OGD_LLM_MAX_PARALLEL must be greater than zero".to_owned(),
            ));
        }
        let queue_size = parse_u32("OGD_LLM_QUEUE_SIZE", DEFAULT_QUEUE_SIZE)?;
        let device = parse_device()?;
        let gpu_layers = parse_gpu_layers()?;
        let thinking = parse_thinking()?;
        let prefix_cache_max_bytes = parse_u64(
            "OGD_LLM_PREFIX_CACHE_MAX_BYTES",
            DEFAULT_PREFIX_CACHE_MAX_BYTES,
        )?;
        let chat_template = env::var("OGD_LLM_CHAT_TEMPLATE")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        Ok(Self {
            model_path,
            context_size,
            threads,
            max_tokens,
            seed,
            max_parallel,
            queue_size,
            chat_template,
            device,
            gpu_layers,
            thinking,
            prefix_cache_max_bytes,
        })
    }
}

fn parse_device() -> Result<LlmDevice, LlmConfigError> {
    match env::var("OGD_LLM_DEVICE") {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(LlmDevice::Auto),
            "cpu" => Ok(LlmDevice::Cpu),
            "cuda" => Ok(LlmDevice::Cuda),
            _ => Err(LlmConfigError::Invalid(format!(
                "OGD_LLM_DEVICE must be one of auto, cpu or cuda; got {value:?}"
            ))),
        },
        Err(env::VarError::NotPresent) => Ok(LlmDevice::Auto),
        Err(source) => Err(LlmConfigError::Environment {
            name: "OGD_LLM_DEVICE",
            source,
        }),
    }
}

fn parse_gpu_layers() -> Result<LlmGpuLayers, LlmConfigError> {
    match env::var("OGD_LLM_GPU_LAYERS") {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.eq_ignore_ascii_case("auto") {
                return Ok(LlmGpuLayers::Auto);
            }
            if trimmed.eq_ignore_ascii_case("all") {
                return Ok(LlmGpuLayers::All);
            }
            trimmed
                .parse::<u32>()
                .map(LlmGpuLayers::Count)
                .map_err(|source| LlmConfigError::Integer {
                    name: "OGD_LLM_GPU_LAYERS",
                    value,
                    source,
                })
        }
        Err(env::VarError::NotPresent) => Ok(LlmGpuLayers::Auto),
        Err(source) => Err(LlmConfigError::Environment {
            name: "OGD_LLM_GPU_LAYERS",
            source,
        }),
    }
}


fn parse_thinking() -> Result<LlmThinking, LlmConfigError> {
    match env::var("OGD_LLM_THINKING") {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(LlmThinking::Auto),
            "on" | "true" | "1" => Ok(LlmThinking::On),
            "off" | "false" | "0" => Ok(LlmThinking::Off),
            _ => Err(LlmConfigError::Invalid(format!(
                "OGD_LLM_THINKING must be one of auto, on or off; got {value:?}"
            ))),
        },
        Err(env::VarError::NotPresent) => Ok(LlmThinking::Auto),
        Err(source) => Err(LlmConfigError::Environment {
            name: "OGD_LLM_THINKING",
            source,
        }),
    }
}


fn parse_u64(name: &'static str, default: u64) -> Result<u64, LlmConfigError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|source| LlmConfigError::Integer {
                name,
                value,
                source,
            }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(LlmConfigError::Environment { name, source }),
    }
}

fn parse_u32(name: &'static str, default: u32) -> Result<u32, LlmConfigError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u32>()
            .map_err(|source| LlmConfigError::Integer {
                name,
                value,
                source,
            }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(LlmConfigError::Environment { name, source }),
    }
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

    #[test] fn defaults_are_bounded_and_non_zero() { assert!(DEFAULT_CONTEXT_SIZE > 0); assert!(DEFAULT_MAX_TOKENS > 0); assert!(DEFAULT_MAX_PARALLEL > 0); }
    #[test] fn thinking_labels_are_stable() { assert_eq!(LlmThinking::Auto.as_str(), "auto"); assert_eq!(LlmThinking::On.as_str(), "on"); assert_eq!(LlmThinking::Off.as_str(), "off"); }
}
