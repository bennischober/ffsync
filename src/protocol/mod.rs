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
///
/// v2 (v0.2): HELLO gains `file_mtime_nanos` (u32) — the §4 fast path must
/// compare sub-second mtimes; whole-second comparison let a same-second
/// same-size rewrite pass as "in sync" (D15 follow-up). v1 peers are
/// rejected by the version check.
pub const VERSION: u16 = 2;
