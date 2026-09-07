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
        })
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
}
