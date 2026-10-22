//! ffsync library crate: chunk-level file synchronisation primitives.
//!
//! The real functionality lives in the `chunking` and `reconstruction`
//! modules; the binary in `src/main.rs` is currently a manual benchmark
//! harness on top of them.

pub mod chunking;
pub mod index;
pub mod protocol;
pub mod reconstruction;
pub mod transport;
