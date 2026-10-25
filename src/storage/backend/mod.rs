//! Physical storage backends: each implements
//! [`StorageEngine`](crate::storage::StorageEngine) directly.
#![cfg_attr(rustfmt, rustfmt_skip)]
pub mod glacier;
pub mod memory;
