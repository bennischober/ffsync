//! ffsync library crate: chunk-level file synchronisation primitives.
//!
//! The data plane lives in `chunking` and `reconstruction`, the wire
//! protocol in `protocol`, tree scanning/triage in `tree`, the persistent
//! chunk index in `index`, and transport security in `transport`. The
//! binary in `src/main.rs` is the `ffsync` CLI (send/receive/trust/
//! untrust/fingerprints); the benchmark harness is `src/bin/bench.rs`.

pub mod chunking;
pub mod index;
pub mod protocol;
pub mod reconstruction;
pub mod transport;
pub mod tree;
