//! ffsync wire protocol v0.1 (`concept/Protocol v0.1.md`).
//!
//! Layered as:
//! - [`framing`]: the raw frame envelope (type byte + length + payload)
//! - [`message`]: typed frames (HELLO, MANIFEST_CHUNK, PLAN_*, DATA, ...)
//! - [`sync`]: the two-peer state machines (sender and receiver)

pub mod framing;
pub mod message;
pub mod sync;
pub mod tree_sync;

/// Protocol magic sent in the HELLO payload.
pub const MAGIC: [u8; 4] = *b"FFSY";
/// Protocol version implemented by this crate.
///
/// v2 (v0.2): HELLO gains `file_mtime_nanos` (u32) - the §4 fast path must
/// compare sub-second mtimes; whole-second comparison let a same-second
/// same-size rewrite pass as "in sync" (D15 follow-up). v1 peers are
/// rejected by the version check.
///
/// v3 (v0.3): tree sync (D17, §16) - TREE_BEGIN/TREE_END envelope, mtime
/// preservation on commit, empty-file convergence fix. v2 peers reject the
/// unknown TREE_BEGIN frame loudly (strict framing, §3), and the HELLO
/// version check rejects them before any tree state exists.
///
/// v4 (v0.7): TREE_BEGIN gains flag bit 4 `metadata` (§16.9, D18) - each
/// tree entry then carries a `mode u32` (POSIX mode low bits). The earlier
/// additive extensions (§16.7 excludes, §16.8 parallel, §17 QUIC) shipped
/// under v3 without a bump because they changed no existing layout; this
/// one changes per-entry layout under a flag, so it takes the bump. v3
/// peers fail loudly on the version check (and would reject the unknown
/// flag bit anyway - both mechanisms agree).
pub const VERSION: u16 = 4;
