//! ffsync wire protocol v0.1 (`concept/Protocol v0.1.md`).
//!
//! Layered as:
//! - [`framing`]: the raw frame envelope (type byte + length + payload)
//! - [`message`]: typed frames (HELLO, MANIFEST_CHUNK, PLAN_*, DATA, ...)
//! - [`sync`]: the two-peer state machines (sender and receiver)

pub mod framing;
pub mod message;
pub mod sync;

/// Protocol magic sent in the HELLO payload.
pub const MAGIC: [u8; 4] = *b"FFSY";
/// Protocol version implemented by this crate.
pub const VERSION: u16 = 1;
