//! Public Glacier storage engine: a directory of column segments
//! (format: `backend/GLACIER.md`).
#![cfg_attr(rustfmt, rustfmt_skip)]

/// Glacier storage: a directory of column segments.
pub type GlacierStorage = super::GlacierBackend;
