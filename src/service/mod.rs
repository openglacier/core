//! Optional service implementations hosted by `ogd`.
//!
//! Service modules are compile-time extensions. Their public OG capability
//! contract remains declared in `operation`; runtime configuration decides
//! whether a compiled service is enabled and published.

#[cfg(feature = "agent")]
pub mod agent;

#[cfg(feature = "llm")]
pub mod llm;
