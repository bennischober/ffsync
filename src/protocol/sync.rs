//! The two-peer sync state machines (`concept/Protocol v0.1.md` §5–§8).
//!
//! Both sides are plain functions over a generic duplex stream so the same
//! code runs over TCP, over an in-memory duplex pair, or inside tests.
//!
//! Flow (lazy mode, v0.1):
//! 1. both peers send HELLO (§4), validate, optionally take the
//!    size+mtime fast path (D11),
//! 2. receiver streams its manifest (§5); sender scans its own file
//!    concurrently and consumes the manifest into a hash set,
//! 3. sender diffs and streams the plan (§6, D12 exact-match),
//! 4. sender streams DATA frames for remote chunks (§7),
//! 5. receiver verifies the temp file root against VERIFY and commits (§8).

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use blake3::Hash;

use crate::chunking::{
    Blake3Hasher, Chunk, ChunkEvent, FastCDCChunker, PipelineConfig, cdc_params_for_size,
    chunk_root, process_file, sane_cdc_params,
};
use crate::index::{FileIndex, load_index, load_index_with_cdc};
use crate::protocol::framing::{self, FrameError};
use crate::protocol::message::{
    self, ErrorCode, FLAG_RESUMING, FLAG_TRUST_TIMES, Hello, ManifestChunk, Message, MessageError,
    PlanEntry, Role,
};
use crate::reconstruction::{ChunkSource, FileExt, ReconstructionTask, Reconstructor};

// Reusable per-thread zstd decompression context.
//
// `zstd::bulk::decompress` builds and drops a fresh context per call; the
// receiver decompresses one chunk at a time on its own thread, so a
// thread-local amortizes the context across the whole session.
thread_local! {
    static DECOMPRESSOR: std::cell::RefCell<zstd::bulk::Decompressor<'static>> =
        std::cell::RefCell::new(
            zstd::bulk::Decompressor::new()
                .expect("zstd decompressor context allocation failed"),
        );
}

/// Where a locally available chunk lives (D10 union manifest).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalOrigin {
    /// In the partially written temp file (newest state, temp-first claim).
    Temp { offset: u64, length: usize },
    /// In the receiver's old file.
    OldFile { offset: u64, length: usize },
}

/// Errors from the sync state machines.
#[derive(Debug)]
pub enum SyncError {
    /// The peer sent an ERROR frame; connection is dead.
    Peer(ErrorCode, String),
    /// A local I/O problem.
    Io(io::Error),
    /// A protocol violation (bad frames, wrong order, wrong roles).
    Protocol(String),
    /// The stream ended unexpectedly mid-session.
    Eof,
    /// Final verification failed; the temp file was removed.
    VerificationFailed,
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::Peer(code, msg) => write!(f, "peer error {:?}: {}", code, msg),
            SyncError::Io(e) => write!(f, "io error: {}", e),
            SyncError::Protocol(msg) => write!(f, "protocol violation: {}", msg),
            SyncError::Eof => write!(f, "connection closed unexpectedly"),
            SyncError::VerificationFailed => write!(f, "verification failed"),
        }
    }
}

impl From<io::Error> for SyncError {
    fn from(e: io::Error) -> Self {
        SyncError::Io(e)
    }
}

impl From<FrameError> for SyncError {
    fn from(e: FrameError) -> Self {
        match e {
            FrameError::Eof => SyncError::Eof,
            FrameError::Truncated => SyncError::Protocol("truncated frame".into()),
            FrameError::Io(e) => SyncError::Io(e),
        }
    }
}

/// Progress events emitted during a sync (§11): the CLI displays these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// Scanning own file: (bytes hashed so far, file size).
    Scanning(u64, u64),
    /// Warm start (D15): chunk list loaded from a validated persistent
    /// index; payload is the number of chunks reused. No file contents
    /// were read.
    WarmStart(usize),
    /// Diffing against the peer's manifest.
    Diffing,
    /// Transferring: (payload bytes sent so far, payload total).
    Transferring(u64, u64),
    /// Receiving data: (payload bytes written so far, payload total).
    Receiving(u64, u64),
    /// Verifying the reconstructed file.
    Verifying,
    /// Sync completed without any transfer (fast path or empty plan).
    NothingToDo,
    /// Dry-run (§7.1): the sync would change the target; nothing was
    /// written. Reported to the sender as NOT_MODIFIED + DRY_RUN_ACK.
    WouldChange,
    /// An empty file was created (or truncated to empty) and committed —
    /// real work (a state change on disk) even though no bytes moved.
    /// Distinguished from `NothingToDo` so the display never calls a
    /// just-created file "up to date" (§16.3 empty-file conversations).
    CreatedEmpty,
}

/// Everything the sender needs to know about the file being sent.
pub struct SendRequest {
    pub path: PathBuf,
    /// Peer-reachable path/name reported in HELLO (display only).
    pub display_path: String,
    /// mtime to report; `None` reads it from the file.
    pub mtime: Option<SystemTime>,
    /// Disable the size+mtime fast path (rsync `--checksum` semantics, D11).
    pub force_checksum: bool,
    /// Eager diff (§9.2, D8): accept pre-plan DATA buffering. Both peers
    /// must opt in; the sender fails loudly when the receiver declines.
    pub eager: bool,
    /// Dry-run (§7.1): preview only — the receiver executes nothing, writes
    /// nothing, deletes nothing, and reports what would happen. The sender
    /// offers the mode in HELLO; a peer that does not implement it fails
    /// the session loudly (old peers stream their manifest after the plan,
    /// which the dry-run sender detects before any DATA is requested).
    pub dry_run: bool,
    /// Tree mode (v3, D17): the sync runs inside a tree session. Changes
    /// stream-positional behavior — the fast path does NOT close the
    /// connection after NOT_MODIFIED (the tree session continues with the
    /// next file), and the receiver's success paths apply the sender's
    /// mtime before COMMIT_ACK (§16.6).
    pub tree: bool,
}

/// The receiver's view of what to build.
#[derive(Clone)]
pub struct ReceiveRequest {
    pub path: PathBuf,
    /// The file we already have (used as the manifest source); may not exist.
    pub local_source: PathBuf,
    pub force_checksum: bool,
    /// Tree mode (v3, D17): see [`SendRequest::tree`]. In tree mode the
    /// receiver also applies the sender's mtime at commit (§16.6) and does
    /// NOT drain-for-close after the conversation (the session continues).
    pub tree: bool,
    /// Tree mode only: the sender's mtime to apply at commit (from the
    /// per-file HELLO). `None` in single-file mode (mtime is left alone).
    pub apply_mtime: Option<(u64, u32)>,
    /// Tree mode only (v4, §16.9): the sender's POSIX mode bits (low 12
    /// bits) to apply at commit, BEFORE the mtime write (the D15 index
    /// records the post-metadata mtime). `None`/`Some(0)` applies nothing:
    /// the sender had no metadata section or no mode concept on its
    /// platform (the 0 = absent convention, same as mtime (0, 0)).
    pub apply_mode: Option<u32>,
    /// Eager diff (§9.2, D8): accept pre-plan DATA buffering (64 MiB cap).
    /// Both peers must opt in; the sender fails loudly on decline.
    pub eager: bool,
}

/// Outcome of a completed sync.
#[derive(Debug)]
pub enum SyncOutcome {
    /// Receiver converged; nothing was transferred.
    InSync,
    /// Receiver converged; delta bytes were transferred.
    Transferred { bytes: u64, chunks: usize },
    /// Dry-run (§7.1): the sync WOULD have changed the target; nothing was
    /// written. Sender and receiver both report it for the same session.
    WouldChange,
}

fn unix_secs(t: Option<SystemTime>) -> u64 {
    t.and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub(crate) fn send_error_and_finish(
    out: &mut (impl Write + ?Sized),
    code: ErrorCode,
    msg: &str,
) -> SyncError {
    let frame = message::encode_error(code, msg);
    let _ = frame.and_then(|f| f.encode(out));
    let _ = out.flush();
    // Give the peer a moment to read the ERROR frame before the stream
    // drops (drain_for_close only reads; the peer's ERROR read happens
    // after we flushed). Returning immediately is fine here because ERROR
    // paths are terminal failures anyway; the receiver-side read of ERROR
    // happens before its own writes in every flow.
    SyncError::Peer(code, msg.to_string())
}

/// Drains the stream until the peer closes, with a byte bound as a safety
/// net. Used before a clean exit so the peer can read our final frame
/// without a TCP reset racing it. Returns when the peer closes its side or
/// the bound is reached.
fn drain_for_close(stream: &mut impl Read) -> io::Result<()> {
    let mut sink = [0u8; 8192];
    let mut total = 0usize;
    loop {
        match stream.read(&mut sink) {
            Ok(0) => return Ok(()), // peer closed: they got our frame
            Ok(n) => {
                total += n;
                if total > 64 * 1024 * 1024 {
                    // Peer keeps sending beyond any sane manifest; stop.
                    return Ok(());
                }
            }
            Err(e) if e.kind() == io::ErrorKind::ConnectionReset => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

/// Validates the received HELLO against ours (§4). Returns the peer's HELLO
/// or a terminal error. On rejection the caller must send ERROR before
/// closing; `validate_hello` only classifies, it does not write (it has no
/// access to the stream).
fn validate_hello(peer: &Hello, ours_role: Role) -> Result<Hello, SyncError> {
    if peer.version != super::VERSION {
        // Classified as Peer(Version) so callers send ERROR(Version) on the
        // wire — the old peer must learn WHY it was rejected, not just see a
        // hangup (found by the version-mismatch loopback test: the v0.1
        // code wrote the ERROR frame into a throwaway buffer).
        return Err(SyncError::Peer(
            ErrorCode::Version,
            format!(
                "unsupported protocol version {} (this peer speaks {})",
                peer.version,
                super::VERSION
            ),
        ));
    }
    if peer.role == ours_role {
        return Err(SyncError::Protocol(format!(
            "role conflict: both peers are {:?}",
            ours_role
        )));
    }
    // §9.2: the eager bit is now KNOWN (D8 implemented this build); whether
    // the session runs eager is decided per-side after validation — the
    // sender compares its own offer against the receiver's mirror (a
    // receiver that did not opt in fails the session loudly there), and
    // the receiver buffers pre-plan DATA only when IT opted in. An unknown
    // bit would still be rejected at decode (strict codec).
    Ok(peer.clone())
}

impl From<MessageError> for SyncError {
    fn from(e: MessageError) -> Self {
        match e {
            MessageError::Frame(FrameError::Eof) => SyncError::Eof,
            MessageError::Frame(FrameError::Truncated) => {
                SyncError::Protocol("truncated frame".into())
            }
            MessageError::Frame(FrameError::Io(e)) => SyncError::Io(e),
            MessageError::PeerError(code, msg) => SyncError::Peer(code, msg),
            MessageError::UnexpectedType(t) => {
                SyncError::Protocol(format!("unexpected frame type 0x{:02X}", t))
            }
            MessageError::Malformed(why) => {
                SyncError::Protocol(format!("malformed frame: {}", why))
            }
        }
    }
}

/// Runs the sender side of a sync over `stream`.
///
/// `stream` must be a full-duplex ordered byte stream; reads consume what the
/// peer writes and vice versa. Single-threaded by design (v0.1 lazy mode).
pub fn send<S>(
    stream: &mut S,
    request: &SendRequest,
    pipeline: &PipelineConfig,
    progress: &mut dyn FnMut(Progress),
) -> Result<SyncOutcome, SyncError>
where
    S: Read + Write,
{
    // ---- Scan own file first (we are the sender; our chunk list defines
    // the plan, and we need the file size for HELLO). ----
    // Warm start (D15): if a valid index exists for our file (size + mtime
    // match), the chunk list and root come from the index without reading
    // file contents. Otherwise: full cold scan, as always.
    let file = File::open(&request.path)?;
    let file_size = file.metadata()?.len();
    let mtime = request
        .mtime
        .or_else(|| file.metadata().ok().and_then(|m| m.modified().ok()));
    drop(file);
    // Sub-second mtime for HELLO (v0.2): whole-second comparisons let a
    // same-second same-size rewrite fast-path as "in sync".
    let (mtime_secs, mtime_nanos) = mtime
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| (d.as_secs(), d.subsec_nanos()))
        .unwrap_or((0, 0));

    progress(Progress::Scanning(0, file_size));
    // D19: the CDC parameters are a pure function of the file size, chosen
    // by the sender and announced in HELLO. The warm start serves its chunk
    // list only when the stored parameters match these (a foreign layout
    // could never reproduce the root).
    let cdc = cdc_params_for_size(file_size);
    let cdc_u32 = (cdc.0 as u32, cdc.1 as u32, cdc.2 as u32);
    let warm = if request.force_checksum {
        // Checksum mode must derive the root from a real scan (D15: the
        // index is never evidence that scanning can be skipped).
        None
    } else {
        load_index_with_cdc(&request.path, cdc_u32)
    };
    let warmed = warm.is_some();
    let (source_chunks, root) = match warm {
        Some(idx) => {
            progress(Progress::WarmStart(idx.chunks.len()));
            (idx.chunks, idx.root)
        }
        None => {
            let (_scan_stats, chunks) =
                scan_with_progress(&request.path, pipeline, cdc, |hashed| {
                    progress(Progress::Scanning(hashed, file_size))
                })?;
            let root = chunk_root(&chunks);
            (chunks, root)
        }
    };
    progress(Progress::Diffing);

    // ---- HELLO ----
    let hello_flags = (if request.force_checksum {
        0
    } else {
        FLAG_TRUST_TIMES
    }) | if request.eager {
        message::FLAG_EAGER
    } else {
        0
    } | if request.dry_run {
        message::FLAG_DRY_RUN
    } else {
        0
    };
    let hello = Hello {
        version: super::VERSION,
        flags: hello_flags,
        role: Role::Sender,
        cdc_min: cdc.0 as u32,
        cdc_avg: cdc.1 as u32,
        cdc_max: cdc.2 as u32,
        file_size,
        file_mtime: mtime_secs,
        file_mtime_nanos: mtime_nanos,
        path: request.display_path.clone(),
    };
    let trust_own = (hello.flags & FLAG_TRUST_TIMES) != 0;
    let hello_size = hello.file_size;
    let hello_mtime = hello.file_mtime;
    let hello_mtime_nanos = hello.file_mtime_nanos;
    hello.into_frame()?.encode(stream)?;
    stream.flush()?;

    let peer = message::read_typed::<Hello>(stream)?;
    let peer = match validate_hello(&peer, Role::Sender) {
        Ok(h) => h,
        Err(SyncError::Protocol(msg)) => {
            return Err(send_error_and_finish(stream, ErrorCode::RoleConflict, &msg));
        }
        Err(SyncError::Peer(code, msg)) => {
            // Version rejection: the old peer must get an explicit ERROR.
            return Err(send_error_and_finish(stream, code, &msg));
        }
        Err(other) => return Err(other),
    };

    // ---- Fast path (§4): size+mtime equal and trust_times agreed ----
    // §7.1 dry-run: the receiver accepts the preview with a marker frame
    // before the manifest exchange. An old peer streams manifest frames
    // here instead — abort before any plan exists; nothing can be mutated.
    if request.dry_run {
        let marker = framing::read_frame(stream)?;
        match marker.ftype {
            framing::T_DRY_RUN_ACK => {}
            framing::T_MANIFEST_CHUNK | framing::T_MANIFEST_DONE => {
                let _ = send_error_and_finish(
                    stream,
                    ErrorCode::Aborted,
                    "peer does not support --dry-run (no DRY_RUN_ACK after HELLO); aborted before any transfer",
                );
                return Err(SyncError::Protocol(
                    "peer does not support --dry-run (old ffsync?)".into(),
                ));
            }
            framing::T_ERROR => {
                let (code, msg) = message::read_error_payload(&marker)?;
                return Err(SyncError::Peer(code, msg));
            }
            other => {
                return Err(SyncError::Protocol(format!(
                    "expected DRY_RUN_ACK or manifest, got 0x{:02X}",
                    other
                )));
            }
        }
    }
    // §9.2: eager mode is active iff the receiver mirrored the offer. A
    // receiver that implements it consumes interleaved DATA during the
    // manifest exchange; one that does not (old peer, or a receiver that
    // declined) never would — proceeding eagerly would deadlock it at
    // PLAN_DONE (its data loop starts only after the plan), so the
    // session fails loudly instead of silently degrading.
    let eager_active = request.eager && peer.flags & message::FLAG_EAGER != 0;
    if request.eager && !eager_active {
        let _ = send_error_and_finish(
            stream,
            ErrorCode::Version,
            "peer did not accept eager diff (its HELLO lacks flag bit 1)",
        );
        return Err(SyncError::Peer(
            ErrorCode::Version,
            "peer did not accept eager diff (its HELLO lacks flag bit 1); upgrade the peer or drop --eager".into(),
        ));
    }

    // Sub-second mtime matters (v0.2, D11/D15): whole-second comparison let a
    // same-second rewrite with unchanged size fast-path as "in sync" — a
    // silent wrong-content success (found by the warm-start tests). rsync
    // compares sub-second mtimes for the same reason.
    let trust = trust_own && (peer.flags & FLAG_TRUST_TIMES) != 0;
    if trust
        && hello_size == peer.file_size
        && hello_mtime != 0
        && hello_mtime == peer.file_mtime
        && hello_mtime_nanos == peer.file_mtime_nanos
    {
        message::encode_empty(framing::T_NOT_MODIFIED)?.encode(stream)?;
        stream.flush()?;
        if request.tree {
            // Tree mode (§16.4): the session continues with the next file —
            // do NOT drain or close. The receiver reads NOT_MODIFIED in its
            // plan loop and returns; the stream position is back in lockstep
            // (receiver sends nothing on this path).
            progress(Progress::NothingToDo);
            return Ok(SyncOutcome::InSync);
        }
        // Single-file mode: exiting now would close the socket while the
        // receiver may still be writing manifest frames, and the resulting
        // RST can discard the receiver's buffered NOT_MODIFIED before it
        // reads it (the classic TCP reset race). Draining until the peer
        // closes (or a bound is hit) lets the receiver read the frame
        // cleanly.
        let _ = drain_for_close(stream);
        progress(Progress::NothingToDo);
        return Ok(SyncOutcome::InSync);
    }
    if request.tree {
        // Tree mode (§16.4): tell the receiver the conversation proceeds.
        // Without this the receiver would have to race its manifest writes
        // against a possible fast-path NOT_MODIFIED (single-file mode
        // tolerates that; inside a session a broken stream would kill the
        // whole tree sync).
        message::encode_empty(framing::T_FILE_SYNC)?.encode(stream)?;
        stream.flush()?;
    }

    // ---- Receive the manifest (§5) ----
    // On resume the manifest is the union of temp and old-file chunks
    // (D10); the receiver resolves which file to read from per hash. The
    // sender needs hash -> offset for diffing AND the raw chunk list (in
    // arrival order, duplicates kept - identical periodic content repeats
    // hashes and the map must not collapse the layout) for the
    // layout-reproduction check below.
    let mut manifest: HashMap<Hash, u64> = HashMap::new();
    let mut manifest_chunks: Vec<(Hash, u64, u32)> = Vec::new();
    // §9.2 eager state: the sender's position gate (§6.2) — a chunk is
    // offered once manifest consumption has passed chunk.offset + 2*cdc_avg
    // without a match (or the manifest is done); elsewhere a late match
    // still rescues the chunk. Over-send is bounded near modification
    // regions, exactly like the boundary-shift fallback.
    let mut eager_file: Option<File> = None;
    let mut eager_compressor: Option<zstd::bulk::Compressor<'static>> = None;
    let mut eager_sent: u64 = 0;
    let mut eager_scan: usize = 0; // next un-offered source chunk (offset-sorted)
    let mut eager_offered: std::collections::HashSet<Hash> = std::collections::HashSet::new();
    let mut offer_eager_chunk = |chunk: &Chunk,
                                 manifest: &HashMap<Hash, u64>,
                                 consumed_upto: u64,
                                 manifest_done: bool,
                                 stream: &mut dyn Write|
     -> Result<u64, SyncError> {
        if !eager_active {
            return Ok(0);
        }
        let covered = manifest.contains_key(&chunk.hash);
        if covered || (!manifest_done && chunk.offset + 2 * cdc.1 as u64 > consumed_upto) {
            return Ok(0); // still rescuable, or already covered
        }
        let file = eager_file.get_or_insert_with(|| {
            File::open(&request.path).expect("sender file open for eager DATA")
        });
        let compressor = eager_compressor.get_or_insert_with(|| {
            zstd::bulk::Compressor::new(3).expect("zstd context for eager DATA")
        });
        let mut buf = Vec::with_capacity(chunk.length);
        use std::io::{Read as _, Seek as _};
        file.seek(io::SeekFrom::Start(chunk.offset))?;
        buf.resize(chunk.length, 0);
        file.read_exact(&mut buf)?;
        let compressed = compressor.compress(&buf)?;
        message::encode_data(chunk.hash, &compressed)?.encode(stream)?;
        stream.flush()?;
        eager_offered.insert(chunk.hash);
        Ok(chunk.length as u64)
    };
    loop {
        let frame = framing::read_frame(stream)?;
        match frame.ftype {
            framing::T_MANIFEST_CHUNK => {
                let entry = ManifestChunk::from_frame(frame)
                    .map_err(|e| SyncError::Protocol(format!("bad manifest chunk: {}", e)))?;
                manifest.insert(entry.hash, entry.offset);
                manifest_chunks.push((entry.hash, entry.offset, entry.length));
                let consumed = entry.offset + entry.length as u64;
                for chunk in source_chunks[eager_scan..]
                    .iter()
                    .take_while(|c| c.offset < consumed)
                {
                    eager_sent += offer_eager_chunk(chunk, &manifest, consumed, false, stream)?;
                }
                eager_scan = eager_scan
                    + source_chunks[eager_scan..]
                        .iter()
                        .take_while(|c| c.offset < consumed)
                        .count();
            }
            framing::T_MANIFEST_DONE => {
                for chunk in &source_chunks[eager_scan..] {
                    eager_sent += offer_eager_chunk(chunk, &manifest, 0, true, stream)?;
                }
                break;
            }
            other => {
                return Err(send_error_and_finish(
                    stream,
                    ErrorCode::BadFrame,
                    &format!("expected manifest frame, got 0x{:02X}", other),
                ));
            }
        }
    }

    // ---- Plan generation (§6, D12 exact match) ----
    // For every source chunk: if its hash is in the receiver's manifest,
    // PLAN_LOCAL (the receiver reads the bytes from wherever its manifest
    // says — temp or old file — and verifies them against `hash`);
    // otherwise PLAN_REMOTE.
    let mut plan: Vec<PlanEntry> = Vec::with_capacity(source_chunks.len());
    let mut remote_total: u64 = 0;
    for chunk in &source_chunks {
        match manifest.get(&chunk.hash) {
            Some(peer_offset) => plan.push(PlanEntry::Local {
                target_offset: chunk.offset,
                length: chunk.length as u32,
                source_offset: *peer_offset,
                hash: chunk.hash,
            }),
            None => {
                plan.push(PlanEntry::Remote {
                    target_offset: chunk.offset,
                    length: chunk.length as u32,
                    hash: chunk.hash,
                });
                remote_total += chunk.length as u64;
            }
        }
    }

    // Layout-reproduction check (mirrors the receiver's decision, §8): an
    // all-Local plan whose target spans equal the receiver's raw chunk spans
    // (same count, same offsets, same lengths — hash equality implies length
    // equality) means the receiver's file is already chunked exactly like
    // ours: same CDC parameters, same root. The receiver will skip the
    // rewrite and report InSync, so the sender reports InSync too (identical
    // files under --checksum, including periodic content whose repeated
    // hashes collapse in the diff map but not in the positional list — the
    // comparison therefore uses the positional list, not the collapsed map).
    // Any other all-Local plan (Locals dedup'd across positions — a prefix
    // or shifted region) executes on the receiver and reports Transferred
    // with 0 wire bytes.
    let layout_reproduced = !plan.is_empty()
        && plan.len() == manifest_chunks.len()
        && plan
            .iter()
            .zip(manifest_chunks.iter())
            .all(|(e, (_, off, len))| match e {
                PlanEntry::Local {
                    target_offset,
                    length,
                    ..
                } => target_offset == off && length == len,
                _ => false,
            });

    for entry in &plan {
        entry.into_frame()?.encode(stream)?;
    }
    message::encode_empty(framing::T_PLAN_DONE)?.encode(stream)?;
    stream.flush()?;

    // ---- Data transfer (§7) ----
    // Chunks are hashed from the mmap'd pipeline, but for v0.1 we re-read the
    // chunk bytes from the file. zstd-compress each chunk payload.
    // §7.1: in dry-run mode the receiver has already answered (NOT_MODIFIED
    // + DRY_RUN_ACK) — skip DATA and VERIFY entirely; read the report.
    // §9.2: eagerly offered chunks are ALREADY on the wire (sent during the
    // manifest exchange); the plan loop re-sends only what the gate withheld
    // (late-rescuable chunks the manifest eventually missed). The eager
    // counter is part of the transfer progress.
    let mut sent: u64 = 0;
    let mut sent_chunks: usize = 0;
    if remote_total > 0 && !request.dry_run {
        sent = eager_sent;
        let mut file = File::open(&request.path)?;
        let mut chunk_buf = Vec::new();
        // One zstd context reused across all chunks: bulk::compress would
        // create (and destroy) a fresh context per chunk, which shows up as
        // allocation churn on many-small-chunk transfers.
        let mut compressor = zstd::bulk::Compressor::new(3)?;
        for entry in &plan {
            let (target_offset, length, hash) = match entry {
                PlanEntry::Remote {
                    target_offset,
                    length,
                    hash,
                } => (*target_offset, *length as usize, *hash),
                _ => continue,
            };
            // §9.2: this chunk's bytes are already on the wire (offered
            // eagerly during the manifest exchange) and buffered by the
            // receiver — re-sending would corrupt the frame stream the
            // receiver reads at VERIFY.
            if eager_offered.contains(&hash) {
                continue;
            }
            chunk_buf.clear();
            chunk_buf.resize(length, 0);
            use std::io::{Read as _, Seek as _};
            file.seek(io::SeekFrom::Start(target_offset))?;
            file.read_exact(&mut chunk_buf)?;
            let compressed = compressor.compress(&chunk_buf)?;
            message::encode_data(hash, &compressed)?.encode(stream)?;
            sent += length as u64;
            sent_chunks += 1;
            progress(Progress::Transferring(sent, remote_total));
        }
        stream.flush()?;
    }

    // ---- VERIFY + COMMIT_ACK (§8) ----
    // §7.1: in dry-run mode VERIFY is skipped with the data phase — the
    // receiver answers the plan itself (NOT_MODIFIED + DRY_RUN_ACK when
    // the sync would change something, COMMIT_ACK when it would not).
    if !request.dry_run {
        message::encode_verify(root)?.encode(stream)?;
        stream.flush()?;
    }

    // The receiver answers COMMIT_ACK (committed), or in dry-run mode
    // (§7.1) NOT_MODIFIED + DRY_RUN_ACK (would change — nothing was
    // written), or ERROR.
    let frame = framing::read_frame(stream)?;
    match frame.ftype {
        framing::T_COMMIT_ACK => {
            // §7.1: in dry-run mode COMMIT_ACK is the receiver's "nothing
            // would change" report (no temp was created, no index is
            // written). Normal mode: commit succeeded — persist our chunk
            // list for the next warm start (D15). The source is verified
            // content at this point — the receiver hash-verified the
            // reconstruction against this exact chunk list.
            if !request.dry_run {
                let index = FileIndex::from_scan(
                    source_chunks,
                    file_size,
                    crate::index::stat_mtime(&request.path).unwrap_or((0, 0)),
                    cdc_u32,
                );
                let _ = crate::index::write_index(&request.path, &index);
            }
            // InSync is for plans that changed nothing on the receiver: the
            // zero-entry plan, or an all-Local plan that reproduced the
            // receiver's chunk layout (no rewrite). Any executed plan —
            // including an all-Local one that repositioned chunks — reports
            // Transferred (with 0 wire bytes when no Remote entry existed).
            if plan.is_empty() || layout_reproduced {
                Ok(SyncOutcome::InSync)
            } else {
                Ok(SyncOutcome::Transferred {
                    bytes: sent,
                    chunks: sent_chunks,
                })
            }
        }
        framing::T_NOT_MODIFIED => {
            // §7.1: the receiver accepted the dry-run offer and reports a
            // would-change; the DRY_RUN_ACK frame is the formal accept.
            let ack = framing::read_frame(stream)?;
            if ack.ftype != framing::T_DRY_RUN_ACK {
                return Err(SyncError::Protocol(format!(
                    "expected DRY_RUN_ACK after NOT_MODIFIED, got 0x{:02X}",
                    ack.ftype
                )));
            }
            if request.dry_run {
                progress(Progress::WouldChange);
                return Ok(SyncOutcome::WouldChange);
            }
            // Not our request: a receiver that should have committed sent
            // NOT_MODIFIED instead (old-peer contract violation).
            Err(SyncError::Protocol(
                "receiver sent NOT_MODIFIED where a commit was expected".into(),
            ))
        }
        framing::T_ERROR => {
            let (code, msg) = message::read_error_payload(&frame)?;
            // Poison-loop guard (D15): VERIFY failed on the receiver. If we
            // warm-started, our index may have lied about the file's content
            // (mtime-preserving edits defeat stat validation). Drop it so the
            // next attempt cold-scans; a wrong index must never fail a sync
            // twice. Any other error (or a cold scan) leaves the index alone.
            if warmed && code == ErrorCode::HashMismatch {
                crate::index::remove_index(&request.path);
            }
            Err(SyncError::Peer(code, msg))
        }
        other => Err(SyncError::Protocol(format!(
            "expected COMMIT_ACK, got 0x{:02X}",
            other
        ))),
    }
}

/// Runs the receiver side of a sync over `stream`.
///
/// Resume (D10): if a temp file from a previous crashed attempt exists, the
/// receiver re-scans it and unions its chunks with the old file's chunks
/// (temp-first: a hash found in the temp is claimed from the temp). The
/// HELLO carries `FLAG_RESUMING` so the sender knows the manifest may
/// describe two files; plan offsets are unambiguous because PLAN_LOCAL now
/// carries the chunk hash and the receiver resolves the source per hash.
pub fn receive<S>(
    stream: &mut S,
    request: &ReceiveRequest,
    pipeline: &PipelineConfig,
    progress: &mut dyn FnMut(Progress),
) -> Result<SyncOutcome, SyncError>
where
    S: Read + Write,
{
    let temp_path = Reconstructor::temp_path_for(&request.path);
    let resuming = temp_path.exists();

    // Orphan-index cleanup (D15): an index whose target file no longer
    // exists is a leftover from a deleted file. Best-effort, only our own
    // naming convention (`<target>.ffsync-index`, where <target> may itself
    // contain dots — strip the literal suffix, not the last extension).
    if let Some(parent) = request.path.parent()
        && let Ok(entries) = std::fs::read_dir(parent)
    {
        for entry in entries.flatten() {
            let p = entry.path();
            let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(base) = name.strip_suffix(".ffsync-index") else {
                continue;
            };
            if base.is_empty() {
                continue;
            }
            let target = parent.join(base);
            if !target.exists() {
                let _ = std::fs::remove_file(&p);
            }
        }
    }

    // ---- Metadata for HELLO (scanning happens after the handshake, D19:
    // the manifest scan uses the sender's CDC parameters, which we learn
    // from its HELLO). ----
    let local_meta = std::fs::metadata(&request.local_source).ok();
    let local_size = local_meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let local_mtime = local_meta.as_ref().and_then(|m| m.modified().ok());

    // ---- HELLO ----
    let mut flags = if request.force_checksum {
        0
    } else {
        FLAG_TRUST_TIMES
    };
    if resuming {
        flags |= FLAG_RESUMING;
    }
    // §9.2: accept the eager offer iff the CLI asked for it (the receiver
    // must OPT IN too — buffering pre-plan DATA is a real resource
    // commitment). Mirroring without opt-in would surprise a receiver
    // operator; both sides opt in, the bit goes active, and a sender
    // offering without a willing receiver fails loudly (see the sender).
    if request.eager {
        flags |= message::FLAG_EAGER;
    }
    // §7.1: dry-run acceptance is a marker frame (T_DRY_RUN_ACK) sent
    // right after the peer's HELLO is validated — not a HELLO bit (both
    // HELLOs are on the wire before either is read).
    let hello = Hello {
        version: super::VERSION,
        flags,
        role: Role::Receiver,
        // Decorative until the sender's values arrive (§4.3: the sender's
        // parameters are authoritative); kept for symmetric encoding.
        cdc_min: cdc_params_for_size(local_size).0 as u32,
        cdc_avg: cdc_params_for_size(local_size).1 as u32,
        cdc_max: cdc_params_for_size(local_size).2 as u32,
        file_size: local_size,
        file_mtime: unix_secs(local_mtime),
        file_mtime_nanos: local_mtime
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.subsec_nanos())
            .unwrap_or(0),
        path: request
            .path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
    };
    hello.into_frame()?.encode(stream)?;
    stream.flush()?;

    let peer = message::read_typed::<Hello>(stream)?;
    let peer = match validate_hello(&peer, Role::Receiver) {
        Ok(h) => h,
        Err(SyncError::Protocol(msg)) => {
            return Err(send_error_and_finish(stream, ErrorCode::RoleConflict, &msg));
        }
        Err(SyncError::Peer(code, msg)) => {
            // Version rejection: the old peer must get an explicit ERROR.
            return Err(send_error_and_finish(stream, code, &msg));
        }
        Err(other) => return Err(other),
    };
    // §7.1: accept the dry-run offer with a marker frame, before manifest
    // or plan traffic. An old peer (bit ignored on decode) never reads it
    // here — it waits for our manifest, so the marker buffers and our
    // manifest streams on; its plan loop then sees the marker as an
    // unexpected frame and fails loudly BEFORE any transfer. A peer that
    // implements §7.1 consumes the marker before its manifest read.
    if peer.flags & message::FLAG_DRY_RUN != 0 {
        message::encode_empty(framing::T_DRY_RUN_ACK)?.encode(stream)?;
        stream.flush()?;
    }
    // Tree mode (§16.6): the sender's HELLO mtime is what the receiver must
    // carry after commit — fill the per-conversation apply_mtime here.
    let peer_mtime = if request.tree {
        Some((peer.file_mtime, peer.file_mtime_nanos))
    } else {
        None
    };
    let request = ReceiveRequest {
        apply_mtime: request.apply_mtime.or(peer_mtime),
        eager: request.eager,
        ..(*request).clone()
    };

    // ---- CDC parameters for this conversation (D19): the sender's values
    // govern (§4.3). Nonsense values fall back to our size-derived policy —
    // a hostile peer must not steer our chunker. ----
    let cdc = if sane_cdc_params((peer.cdc_min, peer.cdc_avg, peer.cdc_max)) {
        (
            peer.cdc_min as usize,
            peer.cdc_avg as usize,
            peer.cdc_max as usize,
        )
    } else {
        cdc_params_for_size(local_size)
    };
    let cdc_u32 = (cdc.0 as u32, cdc.1 as u32, cdc.2 as u32);

    // ---- Scan our local file for the manifest (§5), now that the
    // parameters are known. Warm start (D15): a valid index (size + mtime +
    // parameter match) supplies the chunk list and root without reading
    // file contents. In --checksum mode the index is never used to skip
    // the scan (its root would claim content identity without reading),
    // per D15. ----
    let warm_idx = if local_size > 0 && !request.force_checksum {
        // D19: parameters are not known yet (the sender's HELLO decides);
        // load without parameter validation here — the check happens after
        // the handshake, before the warm list is actually used.
        load_index(&request.local_source)
    } else {
        None
    };
    let (local_chunks, local_root) = match warm_idx {
        Some(idx) if idx.validate(&request.local_source, cdc_u32).is_some() => {
            progress(Progress::WarmStart(idx.chunks.len()));
            (idx.chunks, Some(idx.root))
        }
        _ => {
            if local_size > 0 {
                progress(Progress::Scanning(0, local_size));
                let (_stats, chunks) =
                    scan_with_progress(&request.local_source, pipeline, cdc, |hashed| {
                        progress(Progress::Scanning(hashed, local_size))
                    })?;
                let root = chunk_root(&chunks);
                (chunks, Some(root))
            } else {
                (Vec::new(), None)
            }
        }
    };

    // ---- Resume manifest (D10): union of temp chunks and old-file chunks,
    // temp-first. The temp file holds the sender's content (partial copy
    // from the interrupted session), so it scans with the sender's
    // parameters (D19). ----
    let temp_chunks: Vec<Chunk> = if resuming {
        let tsize = std::fs::metadata(&temp_path)?.len();
        progress(Progress::Scanning(0, tsize));
        let (_stats, chunks) = scan_with_progress(&temp_path, pipeline, cdc, |hashed| {
            progress(Progress::Scanning(hashed, tsize))
        })?;
        chunks
    } else {
        Vec::new()
    };
    // hash -> which file to read the chunk from (Temp beats OldFile: the
    // temp content is the newest state; old-file entries only fill gaps).
    let mut manifest_map: HashMap<Hash, LocalOrigin> = HashMap::new();
    for c in &temp_chunks {
        manifest_map.entry(c.hash).or_insert(LocalOrigin::Temp {
            offset: c.offset,
            length: c.length,
        });
    }
    for c in &local_chunks {
        manifest_map.entry(c.hash).or_insert(LocalOrigin::OldFile {
            offset: c.offset,
            length: c.length,
        });
    }

    // ---- Fast path (§4): the sender may answer NOT_MODIFIED right after
    // the HELLO exchange when size+mtime match and both sides trust times.
    //
    // Tree mode (§16.4): the sender makes the branch explicit — the next
    // frame is NOT_MODIFIED (skip; done with this file, session continues)
    // or FILE_SYNC (proceed; stream our manifest). Read exactly one frame
    // here: no write races, no broken-pipe tolerance.
    //
    // Single-file mode: there is no FILE_SYNC frame (connection end is the
    // session end). The sender never sends plan frames before MANIFEST_DONE,
    // so reading one frame here would block; instead we stream the manifest
    // and tolerate write failures (a fast-path sender exits right after
    // reading our HELLO, closing the stream — the BrokenPipe the loopback
    // test caught), handling NOT_MODIFIED in the plan loop. ----
    if request.tree {
        let frame = framing::read_frame(stream)?;
        match frame.ftype {
            framing::T_NOT_MODIFIED => {
                // The sender's mtime is our file's mtime already (that is
                // why it skipped us); nothing to do, stay in the session.
                progress(Progress::NothingToDo);
                return Ok(SyncOutcome::InSync);
            }
            framing::T_FILE_SYNC => {} // conversation proceeds
            framing::T_ERROR => {
                let (code, msg) = message::read_error_payload(&frame)?;
                return Err(SyncError::Peer(code, msg));
            }
            other => {
                return Err(send_error_and_finish(
                    stream,
                    ErrorCode::BadFrame,
                    &format!("expected NOT_MODIFIED or FILE_SYNC, got 0x{:02X}", other),
                ));
            }
        }
    }

    // ---- Stream our manifest (§5) ----
    // The union manifest (D10): temp chunks first, then old-file chunks
    // whose hashes the temp does not already cover. With no temp file this
    // is just the old file's manifest. Tolerate write failures: a sender
    // that took the size+mtime fast path exits right after reading our
    // HELLO, so our manifest frames may hit a closed stream. Any such
    // failure means the next readable frame is NOT_MODIFIED (handled in the
    // plan loop) — or nothing, in which case the plan loop surfaces the
    // real error.
    // §9.2: an eager session interleaves DATA frames into the manifest
    // stream (the sender's position gate, §6.2). Buffer them by hash;
    // the plan's Remote arm consumes from the buffer instead of the wire.
    // Cap: 64 MiB (§12 spirit) — overflow is a loud ERROR + lazy retry,
    // never a spill file.
    let eager_active = request.eager && peer.flags & message::FLAG_EAGER != 0;
    let mut eager_buf: HashMap<Hash, Vec<u8>> = HashMap::new();
    let mut eager_bytes: usize = 0;
    const EAGER_BUF_CAP: usize = 64 * 1024 * 1024;
    for chunk in &temp_chunks {
        let frame = message::ManifestChunk {
            hash: chunk.hash,
            offset: chunk.offset,
            length: chunk.length as u32,
        }
        .into_frame()?;
        if frame.encode(stream).and_then(|_| stream.flush()).is_err() {
            break;
        }
    }
    let covered: HashSet<Hash> = temp_chunks.iter().map(|c| c.hash).collect();
    for chunk in &local_chunks {
        if covered.contains(&chunk.hash) {
            continue;
        }
        let frame = message::ManifestChunk {
            hash: chunk.hash,
            offset: chunk.offset,
            length: chunk.length as u32,
        }
        .into_frame()?;
        if frame.encode(stream).and_then(|_| stream.flush()).is_err() {
            break;
        }
    }
    let manifest_done = message::encode_empty(framing::T_MANIFEST_DONE)?;
    if manifest_done
        .encode(stream)
        .and_then(|_| stream.flush())
        .is_err()
    {
        // Sender is gone; the plan loop will read NOT_MODIFIED or EOF.
    }
    let _ = stream.flush();

    // ---- Receive plan (§6) ----
    let mut plan: Vec<PlanEntry> = Vec::new();
    let mut remote_total: u64 = 0;
    loop {
        let frame = framing::read_frame(stream)?;
        match frame.ftype {
            framing::T_MANIFEST_CHUNK | framing::T_MANIFEST_DONE => {
                // §9.2: manifest frames after our DONE cannot happen in a
                // well-formed session... but in eager mode the SENDER may
                // legitimately still send DATA (offered chunks) AFTER our
                // manifest finished — handled below; manifest frames here
                // are a protocol error either way.
                return Err(send_error_and_finish(
                    stream,
                    ErrorCode::BadFrame,
                    "unexpected manifest frame after MANIFEST_DONE",
                ));
            }
            framing::T_DATA if eager_active => {
                // The frame is already read; parse (hash, payload) directly
                // (read_data would try to read ANOTHER frame).
                if frame.payload.len() < 32 {
                    return Err(send_error_and_finish(
                        stream,
                        ErrorCode::BadFrame,
                        "eager DATA frame too short",
                    ));
                }
                let mut h = [0u8; 32];
                h.copy_from_slice(&frame.payload[..32]);
                let hash = Hash::from_bytes(h);
                let compressed = frame.payload[32..].to_vec();
                let len = compressed.len();
                if eager_bytes + len > EAGER_BUF_CAP {
                    return Err(send_error_and_finish(
                        stream,
                        ErrorCode::BadFrame,
                        "eager buffer overflow (64 MiB cap, §9.2); retry without --eager",
                    ));
                }
                eager_bytes += len;
                eager_buf.insert(hash, compressed);
            }
            framing::T_PLAN_LOCAL | framing::T_PLAN_REMOTE => {
                let entry = PlanEntry::from_frame(frame)
                    .map_err(|e| SyncError::Protocol(format!("bad plan entry: {}", e)))?;
                if let PlanEntry::Remote { length, .. } = entry {
                    remote_total += length as u64;
                }
                plan.push(entry);
            }
            framing::T_PLAN_DONE => break,
            framing::T_NOT_MODIFIED => {
                // Sender took the size+mtime fast path. If we trust it, done.
                progress(Progress::NothingToDo);
                return Ok(SyncOutcome::InSync);
            }
            framing::T_ERROR => {
                let (code, msg) = message::read_error_payload(&frame)?;
                return Err(SyncError::Peer(code, msg));
            }
            other => {
                return Err(send_error_and_finish(
                    stream,
                    ErrorCode::BadFrame,
                    &format!("expected plan frame, got 0x{:02X}", other),
                ));
            }
        }
    }
    // §7.1 dry-run: accept the offer only in the hoisted preview branch
    // below (which never writes anything). Plans with entries are answered
    // there — a preview that streams the whole file first would defeat the
    // point. A non-dry-run peer never sees this branch.
    let dry_run = peer.flags & message::FLAG_DRY_RUN != 0;

    // §7.1 dry-run (hoisted, before any temp preparation): decide whether
    // the sync WOULD change the target, answer, and return — nothing is
    // written, no temp is created, and the sender (which offered the mode)
    // skips its DATA/VERIFY phases and reads the report. Answers:
    // would-change → NOT_MODIFIED + DRY_RUN_ACK; nothing-would-change →
    // COMMIT_ACK (same frame the normal empty-plan path answers).
    // Would-change cases: a plan that would execute (any Remote, or an
    // all-Local plan that does not reproduce the receiver's layout —
    // those reposition bytes), or a zero-size sender file (truncate or
    // create — both are state changes; an identical empty file would have
    // taken the §4 fast path before any plan existed). Old peers were
    // already detected at the marker: they stream a manifest where a §7.1
    // peer sends DRY_RUN_ACK, so nothing reaches this branch unless both
    // peers implement the mode.
    let plan_reproduces_local_layout = !plan.is_empty()
        && plan.iter().all(|e| matches!(e, PlanEntry::Local { .. }))
        && plan.len() == local_chunks.len()
        && plan.iter().zip(local_chunks.iter()).all(|(e, c)| match e {
            PlanEntry::Local {
                target_offset,
                length,
                ..
            } => *target_offset == c.offset && *length as usize == c.length,
            _ => false,
        });
    if dry_run {
        let would_change =
            peer.file_size == 0 || (!plan.is_empty() && !plan_reproduces_local_layout);
        if would_change {
            message::encode_empty(framing::T_NOT_MODIFIED)?.encode(stream)?;
            stream.flush()?;
            message::encode_empty(framing::T_DRY_RUN_ACK)?.encode(stream)?;
            stream.flush()?;
            progress(Progress::WouldChange);
            return Ok(SyncOutcome::WouldChange);
        }
        // Would NOT change (empty plan, or an all-Local plan that already
        // reproduces the receiver's layout): answer COMMIT_ACK like the
        // normal empty-plan path — the dry-run sender reads it as InSync.
        message::encode_empty(framing::T_COMMIT_ACK)?.encode(stream)?;
        stream.flush()?;
        progress(Progress::NothingToDo);
        return Ok(SyncOutcome::InSync);
    }

    // ---- Prepare the temp file (§8.1) ----
    let target_size = peer.file_size;
    // Decide whether a rewrite is needed WITHOUT the sender's VERIFY root
    // (§8 keeps VERIFY after the data phase on the wire — moving it before
    // the plan would break v0.2 ↔ v0.3 single-file compat, and the receiver
    // cannot read it early without desyncing the frame order).
    //
    // Plans that must execute:
    // - anything with a Remote entry (bytes cross the wire), and
    // - ALL-LOCAL plans whose Local spans do not exactly reproduce the
    //   receiver's own chunk list: Locals are dedup'd by hash, not position,
    //   so a receiver whose file contains the sender's chunk bytes elsewhere
    //   (a prefix, a shifted region) still needs chunks written at the
    //   sender's offsets (found by the static-binary smoke test: sender
    //   2 MiB = pattern × 8192, receiver 1 MiB = pattern × 4096 — every
    //   chunk hash matched, the plan was [Local, Local], and the old
    //   "no Remote ⇒ in sync" shortcut errored HashMismatch instead of
    //   reconstructing).
    //
    // An all-Local plan covering the receiver's file exactly as it is chunked
    // means the file already carries the sender's chunk layout — same CDC
    // parameters on both sides produce the same root — so writing would be a
    // no-op rewrite: report InSync without touching the file (the
    // --checksum identical-files case). VERIFY still guards every path.
    let plan_reproduces_local_layout = !plan.is_empty()
        && plan.iter().all(|e| matches!(e, PlanEntry::Local { .. }))
        && plan.len() == local_chunks.len()
        && plan.iter().zip(local_chunks.iter()).all(|(e, c)| match e {
            PlanEntry::Local {
                target_offset,
                length,
                ..
            } => *target_offset == c.offset && *length as usize == c.length,
            _ => false,
        });
    if !plan.is_empty() && !plan_reproduces_local_layout {
        let reconstructor = if resuming {
            // D10: never truncate — the temp content IS the resume state.
            Reconstructor::open_existing(&request.path, &request.local_source, target_size)?
        } else {
            Reconstructor::new(&request.path, &request.local_source, target_size)?
        };

        // ---- Execute plan entries in order ----
        // Locals are resolved per-hash from the union manifest (temp first,
        // then old file) and verified against the plan's expected hash (D12:
        // a stale manifest entry can cost an ERROR, never silent corruption;
        // the sender-side check also protects against offset drift between
        // the two files' chunkings).
        let mut scratch: Vec<u8> = Vec::new();
        let mut received: u64 = 0;
        // D7: cache the per-file read handles across plan entries — the old
        // code re-opened the source file for every Local entry (thousands
        // of opens per sync on large files). Both handles are read-only and
        // may coexist with the reconstructor's temp write handle.
        let mut temp_read: Option<File> = None;
        let mut local_read: Option<File> = None;
        for entry in &plan {
            match *entry {
                PlanEntry::Local {
                    target_offset,
                    length,
                    source_offset: _,
                    hash,
                } => {
                    let origin = manifest_map.get(&hash).ok_or_else(|| {
                        send_error_and_finish(
                            stream,
                            ErrorCode::BadFrame,
                            "plan references a chunk not in our manifest",
                        )
                    })?;
                    let offset = match origin {
                        LocalOrigin::Temp { offset, .. } => *offset,
                        LocalOrigin::OldFile { offset, .. } => *offset,
                    };
                    // Spec §12 resource limits: writes stay inside the
                    // declared file size.
                    if target_offset + length as u64 > target_size {
                        return Err(send_error_and_finish(
                            stream,
                            ErrorCode::BadFrame,
                            "plan chunk extends beyond the declared file size",
                        ));
                    }
                    let source_file = match origin {
                        LocalOrigin::Temp { .. } => {
                            if temp_read.is_none() {
                                temp_read = Some(File::open(&temp_path)?);
                            }
                            temp_read.as_mut().expect("temp read handle just opened")
                        }
                        LocalOrigin::OldFile { .. } => {
                            if local_read.is_none() {
                                local_read = Some(File::open(&request.local_source)?);
                            }
                            local_read.as_mut().expect("local read handle just opened")
                        }
                    };
                    scratch.clear();
                    scratch.resize(length as usize, 0);
                    source_file.read_at(&mut scratch[..length as usize], offset)?;
                    // Verify the read content (D12 exact-match guarantee).
                    let actual = blake3::hash(&scratch[..length as usize]);
                    if actual != hash {
                        // Stale manifest entry: this chunk's content differs
                        // from what we promised. Delete the stale temp and
                        // fail — the connection is dead anyway, and the next
                        // attempt starts clean. If we warm-started (D15),
                        // also drop the index: it lied about content and
                        // must not poison the retry (same guard as the
                        // sender's HashMismatch handling). The index belongs
                        // to `local_source` — the file the warm manifest
                        // claims to describe — not necessarily the target.
                        if matches!(origin, LocalOrigin::Temp { .. }) {
                            let _ = std::fs::remove_file(&temp_path);
                        }
                        crate::index::remove_index(&request.local_source);
                        return Err(send_error_and_finish(
                            stream,
                            ErrorCode::HashMismatch,
                            "local chunk content does not match its hash",
                        ));
                    }
                    let task = ReconstructionTask {
                        target_offset,
                        source: ChunkSource {
                            offset: 0,
                            length: length as usize,
                        },
                    };
                    // `scratch` holds the verified bytes; write them straight
                    // from it (no clone — the old `&mut buf, Option<&data>`
                    // signature forced a full-chunk copy per Local entry).
                    reconstructor.write_chunk(&task, &scratch)?;
                }
                PlanEntry::Remote {
                    target_offset,
                    length,
                    hash,
                } => {
                    // §9.2: an eagerly offered chunk is already in the
                    // buffer (consumed during the manifest exchange); only
                    // chunks the sender declared via the plan but never
                    // offered (not eager-active, or offered-late) come from
                    // the wire now.
                    let compressed = match eager_buf.get(&hash) {
                        Some(c) => c.clone(),
                        None => {
                            let (data_hash, compressed) = message::read_data(stream)?;
                            if data_hash != hash {
                                return Err(send_error_and_finish(
                                    stream,
                                    ErrorCode::BadFrame,
                                    "DATA hash does not match plan",
                                ));
                            }
                            compressed
                        }
                    };
                    // One zstd context reused across all chunks (see the
                    // sender's data phase for the reasoning).
                    let decompressed = DECOMPRESSOR.with(|d| {
                        let mut decompressor = d.borrow_mut();
                        decompressor.decompress(&compressed, length as usize)
                    })?;
                    if decompressed.len() != length as usize {
                        return Err(send_error_and_finish(
                            stream,
                            ErrorCode::BadFrame,
                            "decompressed length mismatch",
                        ));
                    }
                    // Spec §12 resource limits: chunk data must never
                    // overflow the sender's declared file size.
                    if target_offset + length as u64 > target_size {
                        return Err(send_error_and_finish(
                            stream,
                            ErrorCode::BadFrame,
                            "plan chunk extends beyond the declared file size",
                        ));
                    }
                    let task = ReconstructionTask {
                        target_offset,
                        source: ChunkSource {
                            offset: 0,
                            length: length as usize,
                        },
                    };
                    reconstructor.write_chunk(&task, &decompressed)?;
                    received += length as u64;
                    progress(Progress::Receiving(received, remote_total));
                }
            }
        }

        // ---- VERIFY (§8) ----
        progress(Progress::Verifying);
        let verify_root = message::read_verify(stream)?;
        // Verification (§8.2): re-scan the temp file with the sender's CDC
        // parameters (D19) and compute the chunk-tree root. With identical
        // parameters, the receiver's chunking of correct chunk data
        // reproduces the sender's chunk list, so the roots match.
        let temp_chunks = scan_file_plain(&temp_path, pipeline, cdc)?;
        let temp_root = chunk_root(&temp_chunks);
        if temp_root != verify_root {
            let _ = std::fs::remove_file(&temp_path);
            return Err(send_error_and_finish(
                stream,
                ErrorCode::HashMismatch,
                "reconstructed file hash mismatch",
            ));
        }
        // Hash verified; commit. Reconstruct the Reconstructor state: the
        // temp file exists and is verified — flush it (§8.3 durability:
        // the rename is journaled, the data pages are not; renaming
        // without fsync can leave a renamed-but-empty target after a
        // crash on some filesystems) and rename atomically. Order is
        // load-bearing (§16.9): sync → rename → mode → mtime → stat →
        // index.
        reconstructor.sync_temp()?;
        clear_readonly_best_effort(&request.path);
        std::fs::rename(&temp_path, &request.path)?;
        fsync_parent_dir_best_effort(&request.path);
        apply_commit_mode(&request.path, request.apply_mode)?;
        apply_commit_mtime(&request)?;
        // Persist the verified state for the next warm start (D15): the
        // verified temp scan is exactly the index content. The index
        // records the mtime AFTER it was set to the sender's (§16.6), so
        // both peers' indexes agree and the next tree sync's triage skips.
        let _ = crate::index::write_index(
            &request.path,
            &FileIndex::from_scan(
                temp_chunks,
                target_size,
                crate::index::stat_mtime(&request.path).unwrap_or((0, 0)),
                cdc_u32,
            ),
        );
        message::encode_empty(framing::T_COMMIT_ACK)?.encode(stream)?;
        stream.flush()?;
        return Ok(SyncOutcome::Transferred {
            bytes: received,
            // Every executed entry wrote something (Locals reposition content,
            // Remotes carry wire bytes) — count them all.
            chunks: plan.len(),
        });
    }

    // ---- Zero-entry plan (§8) or an all-Local plan that already reproduces
    // the receiver's chunk layout: nothing to write. The sender still sends
    // VERIFY; compare against the root of our current state (temp when
    // resuming, else the old file). In dry-run mode (§7.1) a would-change
    // plan is answered NOT_MODIFIED + DRY_RUN_ACK instead of executing —
    // nothing is ever written. ----
    let verify_root = message::read_verify(stream)?;
    // v3 (§16): a zero-size sender file means "converge to empty" — truncate
    // (or drop a resume temp) and verify the empty root. Must be checked
    // BEFORE the state comparison: a non-empty local state would otherwise
    // take the HashMismatch path below (v0.2 behavior), making empty sender
    // files unconvergeable. Empty files are common in trees (§16.2), so this
    // is the load-bearing empty-file fix.
    //
    // The empty root is verified explicitly: target_size and verify_root
    // come from the same sender state, so a mismatch is a protocol
    // violation — and without this check a sender claiming size 0 could
    // truncate receiver files while never matching any root.
    if target_size == 0 {
        if verify_root != chunk_root(&[]) {
            return Err(send_error_and_finish(
                stream,
                ErrorCode::BadFrame,
                "sender reports zero size but a non-empty root",
            ));
        }
        if resuming {
            // A stale temp cannot contain anything the sender wants (its
            // file is empty); drop it before truncating.
            let _ = std::fs::remove_file(&temp_path);
        }
        // Truncate the target to empty (create if absent), carry the
        // sender's mtime, and persist the empty index (D15).
        clear_readonly_best_effort(&request.path);
        std::fs::File::create(&request.path)?.set_len(0)?;
        apply_commit_mode(&request.path, request.apply_mode)?;
        apply_commit_mtime(&request)?;
        let _ = crate::index::write_index(
            &request.path,
            &FileIndex::from_scan(
                Vec::new(),
                0,
                crate::index::stat_mtime(&request.path).unwrap_or((0, 0)),
                cdc_u32,
            ),
        );
        message::encode_empty(framing::T_COMMIT_ACK)?.encode(stream)?;
        stream.flush()?;
        progress(Progress::CreatedEmpty);
        return Ok(SyncOutcome::Transferred {
            bytes: 0,
            chunks: 0,
        });
    }
    let (state_root, state_desc, state_chunks): (Option<Hash>, &str, Vec<Chunk>) = if resuming {
        let chunks = scan_file_plain(&temp_path, pipeline, cdc)?;
        (Some(chunk_root(&chunks)), "temp", chunks)
    } else {
        (local_root, "old file", Vec::new())
    };
    match state_root {
        Some(root) if root == verify_root => {
            if resuming {
                // The previous attempt actually completed the data; commit it.
                // Durability (§8.3): flush before the rename (see the
                // executed-plan commit above).
                let reconstructor = Reconstructor::open_existing(
                    &request.path,
                    &request.local_source,
                    target_size,
                )?;
                reconstructor.sync_temp()?;
                drop(reconstructor);
                clear_readonly_best_effort(&request.path);
                std::fs::rename(&temp_path, &request.path)?;
                fsync_parent_dir_best_effort(&request.path);
                apply_commit_mode(&request.path, request.apply_mode)?;
                apply_commit_mtime(&request)?;
                // Persist the committed state (D15): the temp scan above IS
                // the verified chunk list — reuse it instead of re-scanning.
                // (Found while landing D19: this site re-scanned the temp
                // path AFTER renaming it away, so `unwrap_or_default` fed an
                // empty index after every successful resume commit — the
                // warm start then served nothing.)
                let _ = crate::index::write_index(
                    &request.path,
                    &FileIndex::from_scan(
                        state_chunks,
                        target_size,
                        crate::index::stat_mtime(&request.path).unwrap_or((0, 0)),
                        cdc_u32,
                    ),
                );
            } else if request.local_source == request.path {
                // Old file already matched (no transfer needed): persist its
                // index so the NEXT sync warms (D15). The scan already ran;
                // writing costs one file of bytes. Only when the file we
                // scanned IS the target: with --from <other-file> the index
                // would claim the target's identity from another file's
                // content (size/mtime of `local_source` keyed to `path`),
                // and the next warm start would serve the wrong chunk list.
                apply_commit_mode(&request.path, request.apply_mode)?;
                apply_commit_mtime(&request)?;
                let _ = crate::index::write_index(
                    &request.path,
                    &FileIndex::from_scan(
                        local_chunks.clone(),
                        local_size,
                        crate::index::stat_mtime(&request.local_source).unwrap_or((0, 0)),
                        cdc_u32,
                    ),
                );
            }
            message::encode_empty(framing::T_COMMIT_ACK)?.encode(stream)?;
            stream.flush()?;
            progress(Progress::NothingToDo);
            Ok(SyncOutcome::InSync)
        }
        Some(_) => Err(send_error_and_finish(
            stream,
            ErrorCode::HashMismatch,
            &format!(
                "content differs despite empty plan (stale {} manifest?)",
                state_desc
            ),
        )),
        None => {
            // We have no local file but the plan was empty. v3 (§16): this
            // is legitimate when the sender's file is EMPTY (an empty chunk
            // list yields an empty plan) and the receiver's state is a
            // non-empty old file or temp — the correct convergence is to
            // truncate the target to zero bytes and verify the empty root.
            // (v0.2 errored here; an empty sender file could never converge
            // a non-empty receiver file — found by the tree tests, where
            // empty files are common.)
            if target_size == 0 {
                if resuming {
                    std::fs::remove_file(&temp_path).ok();
                }
                // Truncate the target to empty (or create it).
                clear_readonly_best_effort(&request.path);
                std::fs::File::create(&request.path)?.set_len(0)?;
                apply_commit_mode(&request.path, request.apply_mode)?;
                apply_commit_mtime(&request)?;
                let _ = crate::index::write_index(
                    &request.path,
                    &FileIndex::from_scan(
                        Vec::new(),
                        0,
                        crate::index::stat_mtime(&request.path).unwrap_or((0, 0)),
                        cdc_u32,
                    ),
                );
                message::encode_empty(framing::T_COMMIT_ACK)?.encode(stream)?;
                stream.flush()?;
                progress(Progress::CreatedEmpty);
                return Ok(SyncOutcome::Transferred {
                    bytes: 0,
                    chunks: 0,
                });
            }
            // Otherwise: the sender thinks we have everything but we have
            // nothing — our manifest said so, contradiction; protocol error.
            Err(send_error_and_finish(
                stream,
                ErrorCode::BadFrame,
                "empty plan without a local file",
            ))
        }
    }
}

/// Windows: a read-only target cannot be replaced by rename nor truncated
/// by create(write) (os error 5). Clear the attribute best-effort before
/// the commit write — the write-enable-wins direction of §16.9: a verified
/// sync must not fail because a stale attribute blocks the commit, and
/// apply_commit_mode re-asserts read-only afterwards when the sender's
/// mode says so. No-op elsewhere (unix rename ignores the file's own mode).
#[cfg(windows)]
fn clear_readonly_best_effort(path: &Path) {
    if let Ok(meta) = std::fs::metadata(path)
        && meta.is_file()
    {
        let mut perms = meta.permissions();
        if perms.readonly() {
            // Windows-only helper: clearing the attribute here is the
            // write-enable direction of §16.9 (clippy's Unix warning about
            // `set_readonly(false)` does not apply — this code never runs
            // on unix).
            #[allow(clippy::permissions_set_readonly_false)]
            perms.set_readonly(false);
            let _ = std::fs::set_permissions(path, perms);
        }
    }
}

#[cfg(not(windows))]
fn clear_readonly_best_effort(_path: &Path) {}

/// Durability (§8.3, unix strict form): after the atomic rename, flush the
/// parent directory entry so the rename itself survives a crash. On Linux
/// the file-data fsync before the rename (Reconstructor::sync_temp) makes
/// the content durable; the rename is journaled, but a directory fsync is
/// the strict POSIX guarantee for the new name. Windows needs no equivalent
/// (NTFS metadata journaling covers the rename).
#[cfg(unix)]
fn fsync_parent_dir_best_effort(path: &Path) {
    if let Some(parent) = path.parent()
        && let Ok(dir) = std::fs::File::open(parent)
    {
        let _ = dir.sync_all();
    }
}

#[cfg(not(unix))]
fn fsync_parent_dir_best_effort(_path: &Path) {}

/// Applies the sender's POSIX mode bits to the committed target (§16.9,
/// tree mode only, BEFORE the mtime write — the D15 index must record the
/// post-metadata mtime). Best-effort like mtime: a failure must not fail a
/// verified sync, but it will cause the file's mode to diverge until the
/// next sync of that file applies it again.
///
/// Unix: fchmod-equivalent with the masked `0o7777` bits (exact).
/// Windows: the no-write bits (`0o200`/`0o020`/`0o002`) map to setting the
/// read-only attribute; any write-enabled bit clears it. This is lossy by
/// design — an NTFS attribute cannot express read-denial per class, and
/// write-enable winning is the safe direction (a read-only file that should
/// be writable would otherwise break every later sync's writes).
fn apply_commit_mode(path: &Path, mode: Option<u32>) -> io::Result<()> {
    let Some(bits) = mode.filter(|m| *m != 0) else {
        return Ok(()); // no metadata section, or sender has no mode concept
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let f = std::fs::OpenOptions::new().write(true).open(path)?;
        f.set_permissions(std::fs::Permissions::from_mode(bits))?;
        Ok(())
    }
    #[cfg(windows)]
    {
        let write_denied = bits & 0o222 != 0;
        let mut perms = std::fs::metadata(path)?.permissions();
        perms.set_readonly(write_denied);
        std::fs::set_permissions(path, perms)?;
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, bits);
        Ok(())
    }
}

/// Applies the sender's mtime to the committed target file (§16.6, tree
/// mode only): the receiver's file must carry the sender's mtime so the
/// next tree sync's metadata triage skips it, and both peers' D15 indexes
/// record the same mtime. Best-effort: a failure to set mtime must not
/// fail a verified sync — but it WILL cause the next tree sync to re-check
/// this file (its triage metadata will differ), which is self-healing.
fn apply_commit_mtime(request: &ReceiveRequest) -> io::Result<()> {
    let Some((secs, nanos)) = request.apply_mtime else {
        return Ok(());
    };
    if secs == 0 && nanos == 0 {
        return Ok(()); // sender had no mtime; leave ours
    }
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&request.path)?;
    f.set_modified(UNIX_EPOCH + std::time::Duration::new(secs, nanos))
}

/// Scans a file with progress callbacks bridged from the pipeline events.
/// The CDC parameters are explicit (D19): the sender scans with its
/// size-derived policy, the receiver with the sender's HELLO values.
fn scan_with_progress(
    path: &Path,
    pipeline: &PipelineConfig,
    cdc: (usize, usize, usize),
    mut on_progress: impl FnMut(u64),
) -> Result<(u64, Vec<Chunk>), SyncError> {
    let rx = process_file(
        path,
        move || FastCDCChunker::new(cdc.0, cdc.1, cdc.2),
        Blake3Hasher,
        pipeline,
    )?;
    let mut hashed: u64 = 0;
    let mut chunks = Vec::new();
    loop {
        match rx.recv() {
            Ok(ChunkEvent::Chunk(chunk)) => {
                hashed += chunk.length as u64;
                chunks.push(chunk);
                on_progress(hashed);
            }
            Ok(ChunkEvent::Done) => {}
            Err(_) => break,
        }
    }
    // CRITICAL: hash workers complete chunks concurrently, so arrival order
    // is nondeterministic (worker-completion order, not file order). The
    // chunk-tree root is order-sensitive; sort by offset — chunks partition
    // the file, so offsets are unique and this restores canonical file
    // order regardless of worker count. Without this, roots differ between
    // scans of identical data and every verification fails (or worse, a
    // root over a permuted list is "verified" against itself only by luck).
    chunks.sort_by_key(|c| c.offset);
    Ok((hashed, chunks))
}

/// Scans a file without progress reporting (verification re-scan).
fn scan_file_plain(
    path: &Path,
    pipeline: &PipelineConfig,
    cdc: (usize, usize, usize),
) -> Result<Vec<Chunk>, SyncError> {
    let (_, chunks) = scan_with_progress(path, pipeline, cdc, |_| {})?;
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_accepts_eager_bit_decline_is_session_level() {
        // §9.2 (D8 implemented): the eager bit is KNOWN and passes HELLO
        // validation; whether the session runs eager is decided per-side
        // (the sender checks the receiver's mirror, the receiver gates its
        // buffer on its own opt-in). Decline is a session-level loud error,
        // not a decode error.
        let peer = Hello {
            version: super::super::VERSION,
            flags: message::FLAG_EAGER,
            role: Role::Receiver,
            cdc_min: 16384,
            cdc_avg: 65536,
            cdc_max: 262144,
            file_size: 1,
            file_mtime: 0,
            file_mtime_nanos: 0,
            path: "x".into(),
        };
        assert!(validate_hello(&peer, Role::Sender).is_ok());
    }
}
