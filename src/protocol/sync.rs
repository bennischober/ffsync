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
    Blake3Hasher, CHUNK_AVG, CHUNK_MAX, CHUNK_MIN, Chunk, ChunkEvent, FastCDCChunker,
    PipelineConfig, chunk_root, process_file,
};
use crate::protocol::framing::{self, FrameError};
use crate::protocol::message::{
    self, ErrorCode, FLAG_RESUMING, FLAG_TRUST_TIMES, Hello, ManifestChunk, Message, MessageError,
    PlanEntry, Role,
};
use crate::reconstruction::{ChunkSource, FileExt, ReconstructionTask, Reconstructor};

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
}

/// The receiver's view of what to build.
pub struct ReceiveRequest {
    pub path: PathBuf,
    /// The file we already have (used as the manifest source); may not exist.
    pub local_source: PathBuf,
    pub force_checksum: bool,
}

/// Outcome of a completed sync.
#[derive(Debug)]
pub enum SyncOutcome {
    /// Receiver converged; nothing was transferred.
    InSync,
    /// Receiver converged; delta bytes were transferred.
    Transferred { bytes: u64, chunks: usize },
}

fn unix_secs(t: Option<SystemTime>) -> u64 {
    t.and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn send_error_and_finish(out: &mut impl Write, code: ErrorCode, msg: &str) -> SyncError {
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
/// or a terminal error.
fn validate_hello(peer: &Hello, ours_role: Role) -> Result<Hello, SyncError> {
    if peer.version != super::VERSION {
        return Err(send_error_and_finish(
            &mut Vec::new(), // replaced below; see helper
            ErrorCode::Version,
            &format!("unsupported version {}", peer.version),
        ));
    }
    if peer.role == ours_role {
        return Err(SyncError::Protocol(format!(
            "role conflict: both peers are {:?}",
            ours_role
        )));
    }
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
    let file = File::open(&request.path)?;
    let file_size = file.metadata()?.len();
    let mtime = request
        .mtime
        .or_else(|| file.metadata().ok().and_then(|m| m.modified().ok()));
    drop(file);

    progress(Progress::Scanning(0, file_size));
    let (_scan_stats, source_chunks) = scan_with_progress(&request.path, pipeline, |hashed| {
        progress(Progress::Scanning(hashed, file_size))
    })?;
    let root = chunk_root(&source_chunks);
    progress(Progress::Diffing);

    // ---- HELLO ----
    let hello_flags = if request.force_checksum {
        0
    } else {
        FLAG_TRUST_TIMES
    };
    let hello = Hello {
        version: super::VERSION,
        flags: hello_flags,
        role: Role::Sender,
        cdc_min: CHUNK_MIN as u32,
        cdc_avg: CHUNK_AVG as u32,
        cdc_max: CHUNK_MAX as u32,
        file_size,
        file_mtime: unix_secs(mtime),
        path: request.display_path.clone(),
    };
    let trust_own = (hello.flags & FLAG_TRUST_TIMES) != 0;
    let hello_size = hello.file_size;
    let hello_mtime = hello.file_mtime;
    hello.into_frame()?.encode(stream)?;
    stream.flush()?;

    let peer = message::read_typed::<Hello>(stream)?;
    let peer = match validate_hello(&peer, Role::Sender) {
        Ok(h) => h,
        Err(SyncError::Protocol(msg)) => {
            return Err(send_error_and_finish(stream, ErrorCode::RoleConflict, &msg));
        }
        Err(other) => return Err(other),
    };

    // ---- Fast path (§4): size+mtime equal and trust_times agreed ----
    let trust = trust_own && (peer.flags & FLAG_TRUST_TIMES) != 0;
    if trust && hello_size == peer.file_size && hello_mtime != 0 && hello_mtime == peer.file_mtime {
        message::encode_empty(framing::T_NOT_MODIFIED)?.encode(stream)?;
        stream.flush()?;
        // TCP note: exiting now would close the socket while the receiver
        // may still be writing manifest frames, and the resulting RST can
        // discard the receiver's buffered NOT_MODIFIED before it reads it
        // (the classic TCP reset race). Draining until the peer closes (or
        // a bound is hit) lets the receiver read the frame cleanly.
        let _ = drain_for_close(stream);
        progress(Progress::NothingToDo);
        return Ok(SyncOutcome::InSync);
    }

    // ---- Receive the manifest (§5) ----
    // On resume the manifest is the union of temp and old-file chunks
    // (D10); the receiver resolves which file to read from per hash. The
    // sender only needs the hash set for diffing.
    let mut manifest: HashMap<Hash, u64> = HashMap::new();
    loop {
        let frame = framing::read_frame(stream)?;
        match frame.ftype {
            framing::T_MANIFEST_CHUNK => {
                let entry = ManifestChunk::from_frame(frame)
                    .map_err(|e| SyncError::Protocol(format!("bad manifest chunk: {}", e)))?;
                manifest.insert(entry.hash, entry.offset);
            }
            framing::T_MANIFEST_DONE => break,
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

    for entry in &plan {
        entry.into_frame()?.encode(stream)?;
    }
    message::encode_empty(framing::T_PLAN_DONE)?.encode(stream)?;
    stream.flush()?;

    // ---- Data transfer (§7) ----
    // Chunks are hashed from the mmap'd pipeline, but for v0.1 we re-read the
    // chunk bytes from the file. zstd-compress each chunk payload.
    let mut sent: u64 = 0;
    let mut sent_chunks: usize = 0;
    if remote_total > 0 {
        let mut file = File::open(&request.path)?;
        let mut chunk_buf = Vec::new();
        for entry in &plan {
            let (target_offset, length, hash) = match entry {
                PlanEntry::Remote {
                    target_offset,
                    length,
                    hash,
                } => (*target_offset, *length as usize, *hash),
                _ => continue,
            };
            chunk_buf.clear();
            chunk_buf.resize(length, 0);
            use std::io::{Read as _, Seek as _};
            file.seek(io::SeekFrom::Start(target_offset))?;
            file.read_exact(&mut chunk_buf)?;
            let compressed = zstd::bulk::compress(&chunk_buf, 3)?;
            message::encode_data(hash, &compressed)?.encode(stream)?;
            sent += length as u64;
            sent_chunks += 1;
            progress(Progress::Transferring(sent, remote_total));
        }
        stream.flush()?;
    }

    // ---- VERIFY + COMMIT_ACK (§8) ----
    message::encode_verify(root)?.encode(stream)?;
    stream.flush()?;

    // The receiver answers COMMIT_ACK or NOT_MODIFIED (empty plan) or ERROR.
    let frame = framing::read_frame(stream)?;
    match frame.ftype {
        framing::T_COMMIT_ACK => {
            if remote_total > 0 {
                Ok(SyncOutcome::Transferred {
                    bytes: sent,
                    chunks: sent_chunks,
                })
            } else {
                Ok(SyncOutcome::InSync)
            }
        }
        framing::T_ERROR => {
            let (code, msg) = message::read_error_payload(&frame)?;
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

    // ---- Scan our local file for the manifest (§5). ----
    let local_meta = std::fs::metadata(&request.local_source).ok();
    let local_size = local_meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let local_mtime = local_meta.as_ref().and_then(|m| m.modified().ok());

    let (local_chunks, local_root) = if local_size > 0 {
        progress(Progress::Scanning(0, local_size));
        let (_stats, chunks) = scan_with_progress(&request.local_source, pipeline, |hashed| {
            progress(Progress::Scanning(hashed, local_size))
        })?;
        let root = chunk_root(&chunks);
        (chunks, Some(root))
    } else {
        (Vec::new(), None)
    };

    // ---- Resume manifest (D10): union of temp chunks and old-file chunks,
    // temp-first. Offsets in the manifest are resolved per (hash -> file)
    // below when the plan references them. ----
    let temp_chunks: Vec<Chunk> = if resuming {
        let tsize = std::fs::metadata(&temp_path)?.len();
        progress(Progress::Scanning(0, tsize));
        let (_stats, chunks) = scan_with_progress(&temp_path, pipeline, |hashed| {
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

    // ---- HELLO ----
    let mut flags = if request.force_checksum {
        0
    } else {
        FLAG_TRUST_TIMES
    };
    if resuming {
        flags |= FLAG_RESUMING;
    }
    let hello = Hello {
        version: super::VERSION,
        flags,
        role: Role::Receiver,
        cdc_min: CHUNK_MIN as u32,
        cdc_avg: CHUNK_AVG as u32,
        cdc_max: CHUNK_MAX as u32,
        file_size: local_size,
        file_mtime: unix_secs(local_mtime),
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
        Err(other) => return Err(other),
    };

    // ---- Fast path (§4): the sender may immediately answer NOT_MODIFIED
    // when size+mtime match and both sides trust times. Note the sender
    // sends its HELLO first and ours second, so a fast-path sender exits
    // right after reading our HELLO: the next frame here is either
    // NOT_MODIFIED or the sender's demand for our manifest (i.e. anything
    // else means the sender did NOT fast-path). ----
    // We must check this BEFORE streaming our manifest, otherwise the
    // sender's exit races our writes (the BrokenPipe the loopback test
    // caught). The check is a peek at the next frame's type: read it now,
    // and if it is not NOT_MODIFIED, process it as the start of the normal
    // flow (the plan loop consumes plan frames; the manifest sender only
    // starts writing its plan after MANIFEST_DONE, so an early plan frame
    // cannot arrive while our manifest is still streaming... except the
    // sender diffs only after MANIFEST_DONE, so the first frame after
    // HELLO from a non-fast-path sender is nothing — it waits. Reading one
    // frame here would block. Therefore: do NOT read; rely on the sender
    // never sending plan frames before MANIFEST_DONE and just stream the
    // manifest. The NOT_MODIFIED frame arrives *while* we stream, and the
    // plan loop below handles it.)
    //
    // BUT: the sender closes after NOT_MODIFIED (one-shot HELLO exchange),
    // so our subsequent manifest writes hit a dead peer and fail with
    // BrokenPipe — which is *harmless* (the sync is done). We catch that
    // case in the plan loop instead: the first frame we read after our
    // manifest is either NOT_MODIFIED (already consumed... no, the plan
    // loop reads it), or an ERROR, or plan frames.
    //
    // Conclusion: stream the manifest, tolerate write failures during it
    // (broken pipe means the sender fast-pathed), and handle NOT_MODIFIED
    // in the plan loop (already implemented).

    // ---- Stream our manifest (§5) ----
    // The union manifest (D10): temp chunks first, then old-file chunks
    // whose hashes the temp does not already cover. With no temp file this
    // is just the old file's manifest. Tolerate write failures: a sender
    // that took the size+mtime fast path exits right after reading our
    // HELLO, so our manifest frames may hit a closed stream. Any such
    // failure means the next readable frame is NOT_MODIFIED (handled in the
    // plan loop) — or nothing, in which case the plan loop surfaces the
    // real error.
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

    // ---- Prepare the temp file (§8.1) ----
    let target_size = peer.file_size;
    if remote_total > 0 || plan.iter().any(|e| matches!(e, PlanEntry::Remote { .. })) {
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
                    let (path, offset) = match origin {
                        LocalOrigin::Temp { offset, .. } => (&temp_path, *offset),
                        LocalOrigin::OldFile { offset, .. } => (&request.local_source, *offset),
                    };
                    let source_file = File::open(path)?;
                    scratch.clear();
                    scratch.resize(length as usize, 0);
                    source_file.read_at(&mut scratch[..length as usize], offset)?;
                    // Verify the read content (D12 exact-match guarantee).
                    let actual = blake3::hash(&scratch[..length as usize]);
                    if actual != hash {
                        // Stale manifest entry: this chunk's content differs
                        // from what we promised. Delete the stale temp and
                        // fail — the connection is dead anyway, and the next
                        // attempt starts clean.
                        if matches!(origin, LocalOrigin::Temp { .. }) {
                            let _ = std::fs::remove_file(&temp_path);
                        }
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
                            is_local: false, // data is already in `scratch`
                        },
                    };
                    let payload = scratch.clone();
                    reconstructor.write_chunk(&task, &mut scratch, Some(&payload))?;
                }
                PlanEntry::Remote {
                    target_offset,
                    length,
                    hash,
                } => {
                    let (data_hash, compressed) = message::read_data(stream)?;
                    if data_hash != hash {
                        return Err(send_error_and_finish(
                            stream,
                            ErrorCode::BadFrame,
                            "DATA hash does not match plan",
                        ));
                    }
                    let decompressed = zstd::bulk::decompress(&compressed, length as usize)?;
                    if decompressed.len() != length as usize {
                        return Err(send_error_and_finish(
                            stream,
                            ErrorCode::BadFrame,
                            "decompressed length mismatch",
                        ));
                    }
                    let task = ReconstructionTask {
                        target_offset,
                        source: ChunkSource {
                            offset: 0,
                            length: length as usize,
                            is_local: false,
                        },
                    };
                    reconstructor.write_chunk(&task, &mut scratch, Some(&decompressed))?;
                    received += length as u64;
                    progress(Progress::Receiving(received, remote_total));
                }
            }
        }

        // ---- VERIFY (§8) ----
        progress(Progress::Verifying);
        let verify_root = message::read_verify(stream)?;
        // Verification (§8.2): re-scan the temp file with the same CDC
        // parameters and compute the chunk-tree root. With identical
        // parameters, the receiver's chunking of correct chunk data
        // reproduces the sender's chunk list, so the roots match.
        let temp_path = request.path.with_extension("ffsync-tmp");
        let temp_chunks = scan_file_plain(&temp_path, pipeline)?;
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
        // temp file exists and is verified — rename atomically.
        std::fs::rename(&temp_path, &request.path)?;
        message::encode_empty(framing::T_COMMIT_ACK)?.encode(stream)?;
        stream.flush()?;
        return Ok(SyncOutcome::Transferred {
            bytes: received,
            chunks: plan
                .iter()
                .filter(|e| matches!(e, PlanEntry::Remote { .. }))
                .count(),
        });
    }

    // ---- Empty plan (§8 already-in-sync): the sender still sends VERIFY;
    // compare against the root of our current state. On resume the temp file
    // is the newest state (a previous attempt may have died after the last
    // DATA byte), so prefer it when present. ----
    let verify_root = message::read_verify(stream)?;
    let (state_root, state_desc): (Option<Hash>, &str) = if resuming {
        let chunks = scan_file_plain(&temp_path, pipeline)?;
        (Some(chunk_root(&chunks)), "temp")
    } else {
        (local_root, "old file")
    };
    match state_root {
        Some(root) if root == verify_root => {
            if resuming {
                // The previous attempt actually completed the data; commit it.
                std::fs::rename(&temp_path, &request.path)?;
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
            // We have no local file but the plan was empty: the sender thinks
            // we have everything. That is only possible if our manifest said
            // so — contradiction; treat as protocol error.
            Err(send_error_and_finish(
                stream,
                ErrorCode::BadFrame,
                "empty plan without a local file",
            ))
        }
    }
}

/// Scans a file with progress callbacks bridged from the pipeline events.
fn scan_with_progress(
    path: &Path,
    pipeline: &PipelineConfig,
    mut on_progress: impl FnMut(u64),
) -> Result<(u64, Vec<Chunk>), SyncError> {
    let rx = process_file(
        path,
        move || FastCDCChunker::new(CHUNK_MIN, CHUNK_AVG, CHUNK_MAX),
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
fn scan_file_plain(path: &Path, pipeline: &PipelineConfig) -> Result<Vec<Chunk>, SyncError> {
    let (_, chunks) = scan_with_progress(path, pipeline, |_| {})?;
    Ok(chunks)
}
