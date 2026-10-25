#![cfg_attr(rustfmt, rustfmt_skip)]
//! Optional service implementations hosted by `ogd`.
//!
//! Service modules are compile-time extensions. Their public OG capability
//! contract remains declared in `operation`; runtime configuration decides
//! whether a compiled service is enabled and published.

#[cfg(feature = "agent")]
pub mod agent;

/// Index of the values of a Place (pure code, used by the App generation).
pub mod values;

/// Compiler of an intent into an App (pure code, used by the App generation).
pub mod compile;

/// The deterministic decisions of the routing: absent data, follow-ups (pure code, used by the App generation).
pub mod route;

/// Generation of an App from a request: catalog, prompts, grammar, values (pure code; run by the agent).
pub mod generate;

#[cfg(feature = "llm")]
pub mod llm;

#[cfg(feature = "mailsend")]
pub mod mail;
