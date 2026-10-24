//! Tree session state machines (D17, protocol v3 §16; parallel v4 §16.8):
//! orchestrate the per-file sync machines over one or more connections
//! inside a TREE_BEGIN / TREE_END envelope.
//!
//! Shape of a sequential session (both directions of the envelope, one
//! connection):
//!
//! ```text
//! sender                                receiver
//! -------                               --------
//! TREE_BEGIN(manifest, flags)  ────────────────►
//!                              ◄──────────────── TREE_BEGIN(manifest, flags)
//! [triage: local, deterministic]
//! for file in sync/transfer set:
//!     HELLO {rel_path}         ────────────────►
//!                              ◄──────────────── HELLO {rel_path}
//!     NOT_MODIFIED | FILE_SYNC ────────────────►
//!     ...per-file §5–§8, unchanged...
//!                              ◄──────────────── COMMIT_ACK
//! TREE_END (empty)             ────────────────►
//!                              ◄──────────────── TREE_END(delete report)
//! ```
//!
//! A parallel session (§16.8, `--parallel K`) runs the same envelope on
//! connection 0 (control) and distributes conversations round-robin over
//! K−1 worker connections; each worker announces itself with TREE_WORKER
//! and names each conversation with TREE_FILE (membership, not order).
//!
//! Determinism contract (§16.4): both peers compute the same action list
//! from the same two manifests, so the receiver knows exactly which files
//! will have conversations and fails loudly on any unexpected HELLO.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::protocol::framing::{self};
use crate::protocol::message::{self, ErrorCode, MessageError, TreeBegin, TreeEnd, TreeEntryWire};
use crate::protocol::sync::{
    self, Progress, ReceiveRequest, SendRequest, SyncError, SyncOutcome, send_error_and_finish,
};
use crate::tree::{self, EntryKind, TREE_FLAG_CHECKSUM, TREE_FLAG_NO_DELETE, TreeManifest, Triage};

/// Maximum worker connection count (§16.8): 2..=16 total connections.
pub const MAX_PARALLEL: u16 = 16;

/// Events a parallel session's connection threads report to the control
/// thread (§16.8) — the progress callback is not Send, so per-file activity
/// funnels through a channel.
enum ConvEvent {
    /// A conversation started (rel path; read by the drain loop to emit
    /// FileStart — the field is used in debug contexts only).
    Start(#[allow(dead_code)] String),
    /// Per-file progress from the state machine.
    Progress(String, Progress),
    /// A conversation finished with its outcome (rel path kept for the
    /// drain loop's symmetric handling; outcome is what aggregates).
    Done(#[allow(dead_code)] String, SyncOutcome),
}

/// Clones the pipeline config (worker threads need their own value; the
/// config is a cheap plain-data struct).
fn pipeline_clone(p: &crate::chunking::PipelineConfig) -> crate::chunking::PipelineConfig {
    crate::chunking::PipelineConfig {
        read_buffer_size: p.read_buffer_size,
        chunk_queue_size: p.chunk_queue_size,
        hash_workers: p.hash_workers,
    }
}

/// Tree-level progress events (bridged to the CLI display).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeProgress {
    /// Scanning the local tree (stat-only; no file contents are read).
    Scanning,
    /// A per-file conversation started: (index, total files to sync).
    FileStart(usize, usize),
    /// Per-file progress from the state machine, wrapped with the file's
    /// tree-relative path (the CLI prefixes its display line with it —
    /// hashing a 70 GiB file inside a tree must stay visible).
    FileProgress {
        rel_path: String,
        progress: Progress,
    },
    /// A per-file conversation finished: (index, total files to sync).
    FileDone(usize, usize),
    /// Deleting receiver-only files: (deleted so far, bytes so far).
    Deleting(usize, u64),
}

/// Everything the sender needs for a tree session.
pub struct TreeSendRequest {
    /// The local tree root to send.
    pub root: PathBuf,
    /// Disable the per-file metadata fast path tree-wide (D11/D17).
    pub force_checksum: bool,
    /// Do not delete receiver-only files (§16.5 opt-out).
    pub no_delete: bool,
    /// Exclude patterns (§16.7). The sender's patterns govern BOTH sides'
    /// scans (they travel in TREE_BEGIN); invalid patterns fail the session
    /// before any wire traffic.
    pub excludes: Vec<String>,
    /// Total connection count for a parallel session (§16.8): 1 = the
    /// sequential v3 flow on `stream` only; K ≥ 2 opens K−1 worker
    /// connections through `connect_worker` after TREE_BEGIN. Values >16
    /// are rejected (§16.8 range).
    pub parallel: u16,
    /// §16.10 dry-run: preview session — the receiver runs triage only,
    /// reports what would change (conversation count, transfer bytes,
    /// deletion count), and never writes. Old peers reject the unknown
    /// TREE_BEGIN flag bit loudly (strict decode, §16.2).
    pub dry_run: bool,
    /// Opens worker connection `index` (1..K−1) to the peer. The CLI
    /// supplies the closure (TCP connect + TLS); the state machine stays
    /// transport-agnostic. Called from multiple worker threads — Arc-shared,
    /// hence `Send + Sync`.
    pub connect_worker: std::sync::Arc<dyn Fn(u16) -> io::Result<Box<dyn ReadWrite>> + Send + Sync>,
}

/// Everything the receiver needs for a tree session.
pub struct TreeReceiveRequest {
    /// The local tree root to converge toward the sender's.
    pub root: PathBuf,
    pub force_checksum: bool,
    pub no_delete: bool,
    /// The receiver's own exclude patterns (§16.7): they never touch
    /// manifests (the sender's do that) — they only protect paths from the
    /// delete pass (§16.5).
    pub excludes: Vec<String>,
    /// Accepts a worker connection from the peer (the receiver is the
    /// listening side; §16.8). Called K−1 times after the control
    /// connection's TREE_BEGIN is read and K is known; the connection is
    /// placed by its announced TREE_WORKER index, so any arrival order is
    /// valid. Arc-shared with the session's worker threads.
    pub accept_worker: std::sync::Arc<dyn Fn() -> io::Result<Box<dyn ReadWrite>> + Send + Sync>,
    /// §16.10 dry-run (mirrored from the sender's TREE_BEGIN): the receiver
    /// runs triage only — no conversations, no deletes, no writes; it
    /// reports what would happen instead.
    pub dry_run: bool,
}

/// Object-safe read+write stream (worker connections come back as these).
/// `Send` because each worker connection lives on its own thread.
pub trait ReadWrite: Read + Write + Send {}
impl<T: Read + Write + Send> ReadWrite for T {}

/// Outcome of a completed tree session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeOutcome {
    /// Per-file outcomes in conversation order.
    pub files: usize,
    /// Files that arrived via transfer (delta bytes moved).
    pub transferred_bytes: u64,
    /// Files already in sync (fast path or verified equal).
    pub in_sync: usize,
    /// Files deleted on the receiver (0 when `no_delete` or sender role).
    pub deleted: u32,
    pub deleted_bytes: u64,
}

/// Encodes a [`TreeManifest`] into the wire form.
fn manifest_to_wire(m: &TreeManifest) -> Vec<TreeEntryWire> {
    m.entries()
        .map(|e| TreeEntryWire {
            rel_path: e.rel_path.clone(),
            kind: e.kind as u8,
            size: e.size,
            mtime_secs: e.mtime_secs,
            mtime_nanos: e.mtime_nanos,
            mode: e.mode,
        })
        .collect()
}

/// Decodes the wire form into a [`TreeManifest`], validating every path
/// (§16.2: no traversal, wire separators, kind byte).
pub fn manifest_from_wire(wire: &[TreeEntryWire]) -> Result<TreeManifest, SyncError> {
    let mut m = TreeManifest::new();
    for e in wire {
        tree::validate_rel_path(&e.rel_path)
            .map_err(|why| SyncError::Protocol(format!("bad tree path: {why}")))?;
        let kind = EntryKind::from_u8(e.kind)
            .ok_or_else(|| SyncError::Protocol(format!("bad tree entry kind {}", e.kind)))?;
        if kind == EntryKind::Dir && e.size != 0 {
            return Err(SyncError::Protocol(format!(
                "tree dir entry {} carries a size",
                e.rel_path
            )));
        }
        m.insert(crate::tree::TreeEntry {
            rel_path: e.rel_path.clone(),
            kind,
            size: e.size,
            mtime_secs: e.mtime_secs,
            mtime_nanos: e.mtime_nanos,
            mode: e.mode,
        });
    }
    Ok(m)
}

fn read_tree_begin(stream: &mut impl Read) -> Result<(TreeBegin, TreeManifest), SyncError> {
    let frame = framing::read_frame(stream)?;
    let begin = TreeBegin::from_frame(frame)?;
    let manifest = manifest_from_wire(&begin.entries)?;
    Ok((begin, manifest))
}

/// The sender's mode for one conversation (§16.9): `Some(mode)` only when
/// the session carries the metadata section AND the sender's manifest has
/// the entry with a nonzero mode (the 0 = absent convention — applying 0
/// would mean "everything denied" on unix, so it is skipped).
fn sender_mode_of(peer_manifest: &TreeManifest, rel: &str, metadata: bool) -> Option<u32> {
    if !metadata {
        return None;
    }
    peer_manifest.get(rel).map(|e| e.mode).filter(|m| *m != 0)
}

/// The sender's per-path modes for the parallel service loop (§16.9).
fn sender_modes_of(
    peer_manifest: &TreeManifest,
    metadata: bool,
) -> std::collections::HashMap<String, u32> {
    if !metadata {
        return std::collections::HashMap::new();
    }
    peer_manifest
        .entries()
        .map(|e| (e.rel_path.clone(), e.mode))
        .filter(|(_, m)| *m != 0)
        .collect()
}

fn send_tree_begin(
    stream: &mut impl Write,
    flags: u16,
    parallel: u16,
    excludes: &[String],
    manifest: &TreeManifest,
) -> Result<(), SyncError> {
    TreeBegin {
        flags,
        parallel,
        excludes: excludes.to_vec(),
        entries: manifest_to_wire(manifest),
    }
    .into_frame()?
    .encode(stream)?;
    stream.flush()?;
    Ok(())
}

/// Builds the receiver-side filesystem path for a relative tree path.
/// The rel path was validated at manifest decode (no `..`, no backslash),
/// so a plain join cannot escape the root.
fn abs_path(root: &Path, rel: &str) -> PathBuf {
    root.join(rel.replace('/', std::path::MAIN_SEPARATOR_STR))
}

/// Creates all parent directories for a target path (tree-relative mkdir -p).
fn ensure_parent_dir(target: &Path) -> io::Result<()> {
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Runs one sender-side per-file conversation on `stream` (§16.4): the
/// vanished-file check, HELLO with the file's mtime, then the §5–§8 state
/// machine. Used by the sequential loop and by worker threads alike.
fn run_sender_conversation<S>(
    stream: &mut S,
    root: &Path,
    rel: &str,
    checksum: bool,
    pipeline: &crate::chunking::PipelineConfig,
    progress: &mut dyn FnMut(Progress),
) -> Result<SyncOutcome, SyncError>
where
    S: Read + Write,
{
    let path = abs_path(root, rel);
    let meta = std::fs::metadata(&path).map_err(|e| {
        let err = SyncError::Protocol(format!("sender file vanished: {rel}: {e}"));
        // Announce the abort: the peer is waiting for the per-file
        // HELLO and must fail fast instead of timing out.
        let _ = send_error_and_finish(stream, ErrorCode::Aborted, &err.to_string());
        err
    })?;
    let mtime = meta.modified().ok();

    let send_req = SendRequest {
        path,
        display_path: rel.to_string(),
        mtime,
        force_checksum: checksum,
        tree: true,
        dry_run: false, // per-file conversations never preview (§16.10 gates the session)
        eager: false,
    };
    // The sender's mode needs no wire field of its own: the receiver reads
    // it from the TREE_BEGIN manifest entry for this rel_path (§16.9) —
    // the manifest is the per-session source of truth for metadata.
    let outcome = match sync::send(stream, &send_req, pipeline, progress) {
        Ok(o) => o,
        Err(e) => {
            // Announce the abort (best effort — the peer may be the one
            // that caused it and is already gone).
            let _ = send_error_and_finish(stream, ErrorCode::Aborted, &e.to_string());
            return Err(e);
        }
    };
    Ok(outcome)
}

/// Runs the sender side of a tree session over `stream`.
pub fn tree_send<S>(
    stream: &mut S,
    request: &TreeSendRequest,
    pipeline: &crate::chunking::PipelineConfig,
    progress: &mut dyn FnMut(TreeProgress),
) -> Result<TreeOutcome, SyncError>
where
    S: Read + Write,
{
    // ---- Scan our tree (§16.1) ----
    progress(TreeProgress::Scanning);
    let excludes = tree::ExcludeSet::from_patterns(&request.excludes)
        .map_err(|e| SyncError::Protocol(format!("bad exclude pattern: {e}")))?;
    let manifest = TreeManifest::scan_with_excludes(&request.root, &excludes).map_err(|e| {
        SyncError::Protocol(format!(
            "cannot scan tree {}: {}",
            request.root.display(),
            e
        ))
    })?;

    // §16.8: a parallel session announces its connection count in
    // TREE_BEGIN (flag bit 3 + parallel u16). Sequential sessions keep the
    // exact v3 wire layout (flag absent, no field).
    // §16.9: the metadata section rides whenever the sender's platform
    // produces mode values. A Windows sender (modes are always 0) sends no
    // section — its entries would carry no information — while a unix
    // sender always does, so a unix→Windows sync transfers exec bits (which
    // map to the read-only attribute) and Windows→unix syncs mode 0 (apply
    // nothing). The flag is sender-authoritative; the receiver mirrors it.
    // §16.7 v0.7.1: the negation bit rides iff any pattern re-includes —
    // peers that cannot evaluate negations reject the session loudly.
    let metadata = cfg!(unix);
    let negation = excludes.has_negations();
    let parallel = request.parallel;
    let flags = if request.force_checksum {
        TREE_FLAG_CHECKSUM
    } else {
        0
    } | if request.no_delete {
        TREE_FLAG_NO_DELETE
    } else {
        0
    } | if !request.excludes.is_empty() {
        message::TREE_FLAG_EXCLUDES
    } else {
        0
    } | if parallel > 1 {
        message::TREE_FLAG_PARALLEL
    } else {
        0
    } | if metadata {
        message::TREE_FLAG_METADATA
    } else {
        0
    } | if negation {
        message::TREE_FLAG_NEGATION
    } else {
        0
    } | if request.dry_run {
        crate::tree::TREE_FLAG_DRY_RUN
    } else {
        0
    };

    // ---- TREE_BEGIN exchange ----
    send_tree_begin(stream, flags, parallel, &request.excludes, &manifest)?;
    let (peer_begin, peer_manifest) = read_tree_begin(stream)?;
    // The receiver mirrors K (§16.8: sender-authoritative); a mismatch is a
    // protocol error (it would desync the worker setup).
    if peer_begin.parallel != parallel {
        return Err(SyncError::Protocol(format!(
            "peer replied with parallel={} (we announced {parallel})",
            peer_begin.parallel
        )));
    }
    let checksum = request.force_checksum || (peer_begin.flags & TREE_FLAG_CHECKSUM) != 0;
    // no_delete on the sender only suppresses the RECEIVER's deletes; the
    // sender never deletes. The receiver combines both sides' flags (see
    // tree_receive); here the variable is only used for the record.
    let _ = request.no_delete || (peer_begin.flags & TREE_FLAG_NO_DELETE) != 0;

    // ---- Triage (local, §16.3) ----
    // The sender's conversation set: files to Sync/Transfer, plus kind
    // conflicts where the sender has a FILE (the receiver removes its dir
    // first, then the conversation writes the file). Sender-only DIRS and
    // conflicts where the sender has a DIR need no conversation — the
    // receiver applies them locally.
    let mut actions: Vec<String> = Vec::new();
    for entry in manifest.entries() {
        let action = tree::triage(
            &entry.rel_path,
            Some(entry),
            peer_manifest.get(&entry.rel_path),
            checksum,
        );
        match action {
            Triage::Sync | Triage::Transfer => actions.push(entry.rel_path.clone()),
            Triage::Conflict {
                sender_kind: EntryKind::File,
                ..
            } => actions.push(entry.rel_path.clone()),
            _ => {}
        }
    }
    // Sort by rel_path for deterministic order (§16.4).
    actions.sort();

    // ---- §16.10 dry-run: triage-only preview. The receiver mirrors the
    // flag, so it runs no conversations and deletes nothing; the sender
    // skips its conversation phase and goes straight to TREE_END, whose
    // reply carries the receiver's would-delete report. The preview's
    // would-change numbers come from the local triage: |actions|
    // conversations would run, and the bytes they would move are the
    // sender-side estimate (transferred_bytes stays 0 — no conversation
    // ran to measure real bytes).
    if request.dry_run {
        let would_transfer: u64 = actions
            .iter()
            .filter_map(|rel| {
                manifest.get(rel).map(|e| {
                    peer_manifest.get(rel).map_or(e.size, |p| {
                        if p.size == e.size && p.mtime_secs == e.mtime_secs {
                            0
                        } else {
                            e.size
                        }
                    })
                })
            })
            .sum();
        message::TreeEnd::default().into_frame()?.encode(stream)?;
        stream.flush()?;
        let frame = framing::read_frame(stream)?;
        match frame.ftype {
            framing::T_TREE_END => {
                let report = TreeEnd::from_frame(frame)?;
                return Ok(TreeOutcome {
                    files: actions.len(),
                    transferred_bytes: would_transfer,
                    in_sync: 0,
                    deleted: report.deleted_count,
                    deleted_bytes: report.deleted_bytes,
                });
            }
            framing::T_ERROR => {
                let (code, msg) = message::read_error_payload(&frame)?;
                return Err(SyncError::Peer(code, msg));
            }
            other => {
                return Err(SyncError::Protocol(format!(
                    "expected TREE_END, got 0x{:02X}",
                    other
                )));
            }
        }
    }

    let total = actions.len();
    let mut transferred_bytes = 0u64;
    let mut in_sync = 0usize;

    // ---- Per-file conversations (§16.4 sequential, §16.8 parallel) ----
    if request.parallel <= 1 {
        for (idx, rel) in actions.iter().enumerate() {
            progress(TreeProgress::FileStart(idx, total));
            let outcome = run_sender_conversation(
                stream,
                &request.root,
                rel,
                checksum,
                pipeline,
                &mut |p| {
                    progress(TreeProgress::FileProgress {
                        rel_path: rel.clone(),
                        progress: p,
                    })
                },
            )?;
            match outcome {
                SyncOutcome::InSync => in_sync += 1,
                SyncOutcome::Transferred { bytes, .. } => transferred_bytes += bytes,
                SyncOutcome::WouldChange => {} // unreachable: dry-run sessions skip the conversation phase
            }
            progress(TreeProgress::FileDone(idx, total));
        }
    } else {
        let k = request.parallel;
        if !(2..=MAX_PARALLEL).contains(&k) {
            return Err(SyncError::Protocol(format!(
                "parallel={k} outside 2..={MAX_PARALLEL} (§16.8)"
            )));
        }
        // Round-robin partition (deterministic, §16.8): conversation i →
        // connection i mod K. Connection 0 (the control stream) carries
        // partition 0 inline; partitions 1..K run on worker connections.
        let mut partitions: Vec<Vec<String>> = vec![Vec::new(); k as usize];
        for (i, rel) in actions.iter().enumerate() {
            partitions[i % k as usize].push(rel.clone());
        }
        // Shared progress funnel: worker threads cannot call the non-Send
        // progress closure; they send (rel_path, Progress) events the
        // control thread drains after joining.
        let (progress_tx, progress_rx) = std::sync::mpsc::channel::<(String, Progress)>();
        // Every worker index 1..K announces itself — even with an empty
        // partition — because the receiver expects exactly that set
        // (§16.8); it connects, announces, and immediately closes when it
        // has nothing to do.
        let mut handles = Vec::new();
        let mut worker_errors: Vec<Option<SyncError>> = (0..k as usize).map(|_| None).collect();
        for w in 1..k {
            let connect = std::sync::Arc::clone(&request.connect_worker);
            let root = request.root.clone();
            let part = partitions[w as usize].clone();
            let pipeline = pipeline_clone(pipeline);
            let tx = progress_tx.clone();
            let connect_index = w;
            handles.push(std::thread::spawn(move || {
                // Connect + announce + ACK; every later step stops early on
                // an error (§16.8: stop before the NEXT conversation).
                let mut worker_stream: Box<dyn ReadWrite> = match connect(connect_index) {
                    Ok(s) => s,
                    Err(e) => return (Vec::new(), Some(SyncError::Io(e))),
                };
                if let Err(e) = (|| -> Result<(), SyncError> {
                    message::TreeWorker {
                        worker_index: connect_index,
                    }
                    .into_frame()?
                    .encode(&mut worker_stream)?;
                    worker_stream.flush()?;
                    let frame = framing::read_frame(&mut worker_stream)?;
                    if frame.ftype != framing::T_TREE_WORKER {
                        return Err(SyncError::Protocol(format!(
                            "expected TREE_WORKER ack, got 0x{:02X}",
                            frame.ftype
                        )));
                    }
                    Ok(())
                })() {
                    return (Vec::new(), Some(e));
                }
                let mut outcomes = Vec::new();
                for rel in &part {
                    let _ = message::TreeFile {
                        rel_path: rel.clone(),
                    }
                    .into_frame()
                    .and_then(|f| f.encode(&mut worker_stream));
                    let _ = worker_stream.flush();
                    let rel_c = rel.clone();
                    match run_sender_conversation(
                        &mut worker_stream,
                        &root,
                        rel,
                        checksum,
                        &pipeline,
                        &mut |p| {
                            let _ = tx.send((rel_c.clone(), p));
                        },
                    ) {
                        Ok(o) => outcomes.push(o),
                        Err(e) => {
                            let _ = send_error_and_finish(
                                &mut worker_stream,
                                ErrorCode::Aborted,
                                &e.to_string(),
                            );
                            return (outcomes, Some(e)); // stop before the NEXT conversation
                        }
                    }
                }
                (outcomes, None)
            }));
        }

        // Control connection: partition 0 inline. In parallel mode every
        // conversation is announced with TREE_FILE — on ALL connections,
        // control included (the receiver validates membership uniformly).
        let mut control_error: Option<SyncError> = None;
        for (idx, rel) in partitions[0].iter().enumerate() {
            progress(TreeProgress::FileStart(idx, total));
            let _ = message::TreeFile {
                rel_path: rel.clone(),
            }
            .into_frame()
            .and_then(|f| f.encode(stream));
            stream.flush()?;
            let outcome = match run_sender_conversation(
                stream,
                &request.root,
                rel,
                checksum,
                pipeline,
                &mut |p| {
                    progress(TreeProgress::FileProgress {
                        rel_path: rel.clone(),
                        progress: p,
                    })
                },
            ) {
                Ok(o) => o,
                Err(e) => {
                    let _ = send_error_and_finish(stream, ErrorCode::Aborted, &e.to_string());
                    control_error = Some(e);
                    break;
                }
            };
            match outcome {
                SyncOutcome::InSync => in_sync += 1,
                SyncOutcome::Transferred { bytes, .. } => transferred_bytes += bytes,
                SyncOutcome::WouldChange => {} // unreachable: dry-run sessions skip the conversation phase
            }
            progress(TreeProgress::FileDone(idx, total));
        }

        // Join workers; aggregate outcomes. First error wins (§16.8: any
        // failure fails the session).
        for (w, handle) in handles.into_iter().enumerate() {
            let (outcomes, err) = handle
                .join()
                .map_err(|_| SyncError::Protocol("worker thread panicked".to_string()))?;
            let w = w + 1; // partitions skip index 0
            for o in outcomes {
                match o {
                    SyncOutcome::InSync => in_sync += 1,
                    SyncOutcome::Transferred { bytes, .. } => transferred_bytes += bytes,
                    SyncOutcome::WouldChange => {} // unreachable: dry-run sessions skip the conversation phase
                }
            }
            worker_errors[w] = err;
        }
        drop(progress_tx);
        // Final drain (workers are joined; the channel is closed once empty).
        while let Ok((rel_path, p)) = progress_rx.try_recv() {
            progress(TreeProgress::FileProgress {
                rel_path,
                progress: p,
            });
        }
        if let Some(e) = worker_errors.into_iter().flatten().next() {
            return Err(e);
        }
        if let Some(e) = control_error {
            return Err(e);
        }
    }

    // ---- TREE_END (§16.5): transfers done; the receiver now reads our
    // barrier, applies deletions, and replies with the delete report.
    message::TreeEnd::default().into_frame()?.encode(stream)?;
    stream.flush()?;

    // The receiver replies with the delete report; it may also have hit an
    // ERROR during its last conversation or its delete pass — read and
    // classify.
    let frame = framing::read_frame(stream)?;
    match frame.ftype {
        framing::T_TREE_END => {
            let report = TreeEnd::from_frame(frame)?;
            Ok(TreeOutcome {
                files: total,
                transferred_bytes,
                in_sync,
                deleted: report.deleted_count,
                deleted_bytes: report.deleted_bytes,
            })
        }
        framing::T_ERROR => {
            let (code, msg) = message::read_error_payload(&frame)?;
            Err(SyncError::Peer(code, msg))
        }
        other => Err(SyncError::Protocol(format!(
            "expected TREE_END, got 0x{:02X}",
            other
        ))),
    }
}

/// Runs the receiver side of a tree session over `stream`.
pub fn tree_receive<S>(
    stream: &mut S,
    request: &TreeReceiveRequest,
    pipeline: &crate::chunking::PipelineConfig,
    progress: &mut dyn FnMut(TreeProgress),
) -> Result<TreeOutcome, SyncError>
where
    S: Read + Write,
{
    // ---- TREE_BEGIN exchange (receiver second) ----
    // Read the sender's envelope FIRST: its exclude patterns govern BOTH
    // sides' scans (§16.7) — our manifest must be filtered with them before
    // it goes on the wire, or the two triages diverge (§16.4 contract).
    // No deadlock: the sender writes its TREE_BEGIN before blocking on ours.
    progress(TreeProgress::Scanning);
    let (peer_begin, peer_manifest) = read_tree_begin(stream)?;
    let sender_excludes = tree::ExcludeSet::from_patterns(&peer_begin.excludes)
        .map_err(|e| SyncError::Protocol(format!("peer sent bad exclude pattern: {e}")))?;
    let manifest =
        TreeManifest::scan_with_excludes(&request.root, &sender_excludes).map_err(|e| {
            SyncError::Protocol(format!(
                "cannot scan tree {}: {}",
                request.root.display(),
                e
            ))
        })?;

    // §16.8: mirror the sender's K in our TREE_BEGIN (the flag bit carries
    // the field; the value is sender-authoritative). K=1 keeps the exact
    // v3 layout.
    let parallel = peer_begin.parallel;
    // §16.9: mirror the sender's metadata flag — triage inputs must be
    // identical on both peers, so the mode section is all-or-nothing.
    // §16.7 v0.7.1: mirror the negation bit — the sender's patterns govern
    // both scans, so the evaluation model (first-match-wins) must be
    // announced and agreed, not inferred.
    let metadata = (peer_begin.flags & message::TREE_FLAG_METADATA) != 0;
    let negation = (peer_begin.flags & message::TREE_FLAG_NEGATION) != 0;
    // §16.10: mirror the sender's dry-run bit — the session is a preview
    // or it is not, on both peers.
    let dry_run = (peer_begin.flags & crate::tree::TREE_FLAG_DRY_RUN) != 0;
    let flags = if request.force_checksum {
        TREE_FLAG_CHECKSUM
    } else {
        0
    } | if request.no_delete {
        TREE_FLAG_NO_DELETE
    } else {
        0
    } | if parallel > 1 {
        message::TREE_FLAG_PARALLEL
    } else {
        0
    } | if metadata {
        message::TREE_FLAG_METADATA
    } else {
        0
    } | if negation {
        message::TREE_FLAG_NEGATION
    } else {
        0
    } | if dry_run {
        crate::tree::TREE_FLAG_DRY_RUN
    } else {
        0
    };
    // The receiver's own patterns never touch manifests (§16.7); they only
    // guard the delete pass below. They are NOT advertised on the wire.
    let own_excludes = tree::ExcludeSet::from_patterns(&request.excludes)
        .map_err(|e| SyncError::Protocol(format!("bad exclude pattern: {e}")))?;

    send_tree_begin(stream, flags, parallel, &[], &manifest)?;
    let checksum = request.force_checksum || (peer_begin.flags & TREE_FLAG_CHECKSUM) != 0;
    let no_delete = request.no_delete || (peer_begin.flags & TREE_FLAG_NO_DELETE) != 0;
    let mut conversation_paths: Vec<String> = Vec::new();
    let mut deletions: Vec<(String, u64)> = Vec::new();

    // Every path in the union of both manifests, in canonical order.
    let mut all_paths: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for e in manifest.entries() {
        all_paths.insert(e.rel_path.clone());
    }
    for e in peer_manifest.entries() {
        all_paths.insert(e.rel_path.clone());
    }

    for rel in &all_paths {
        let action = tree::triage(rel, peer_manifest.get(rel), manifest.get(rel), checksum);
        // Local apply errors during triage are announced to the peer (best
        // effort) — it sits in its own triage/conversation loop and must
        // fail fast rather than time out.
        macro_rules! announce {
            ($err:expr) => {{
                let err = $err;
                let _ = send_error_and_finish(stream, ErrorCode::Aborted, &err.to_string());
                return Err(err);
            }};
        }
        match action {
            Triage::Skip => {}
            Triage::Sync | Triage::Transfer => conversation_paths.push(rel.clone()),
            Triage::CreateDir => {
                // Sender-only dir: create it now (before conversations — a
                // conversation file may land inside it). §16.10: counted,
                // not created, in dry-run.
                if dry_run {
                    continue;
                }
                let p = abs_path(&request.root, rel);
                if let Err(e) = std::fs::create_dir_all(&p) {
                    announce!(SyncError::Protocol(format!("cannot create dir {rel}: {e}")));
                }
            }
            Triage::Delete => {
                // Receiver-only: delete after transfers (§16.5). Also covers
                // receiver-only dirs (the sender's tree is authoritative).
                if let Some(entry) = manifest.get(rel) {
                    if entry.kind == EntryKind::File {
                        deletions.push((rel.clone(), entry.size));
                    } else {
                        // Empty dir removal is part of the delete pass; it
                        // carries no bytes.
                        deletions.push((rel.clone(), 0));
                    }
                }
            }
            Triage::Conflict { sender_kind, .. } => {
                // The sender's kind wins (§16.3). If the sender has a file,
                // the conversation covers it; the receiver's conflicting
                // entry must be removed first (done in the apply step). If
                // the sender has a dir, remove the receiver's file and
                // create the dir.
                match sender_kind {
                    EntryKind::File => {
                        // Remove the receiver's dir at that path before the
                        // conversation can write the file there. §16.10:
                        // nothing is removed in dry-run.
                        if !dry_run {
                            let p = abs_path(&request.root, rel);
                            if p.is_dir()
                                && let Err(e) = std::fs::remove_dir_all(&p)
                            {
                                announce!(SyncError::Protocol(format!(
                                    "cannot replace dir {rel} with file: {e}"
                                )));
                            }
                        }
                        conversation_paths.push(rel.clone());
                    }
                    EntryKind::Dir => {
                        // §16.10: nothing is removed or created in dry-run.
                        if !dry_run {
                            let p = abs_path(&request.root, rel);
                            if p.is_file()
                                && let Err(e) = std::fs::remove_file(&p)
                            {
                                announce!(SyncError::Protocol(format!(
                                    "cannot replace file {rel} with dir: {e}"
                                )));
                            }
                            if let Err(e) = std::fs::create_dir_all(&p) {
                                announce!(SyncError::Protocol(format!(
                                    "cannot create dir {rel}: {e}"
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    // ---- Per-file conversations (§16.4 sequential, §16.8 parallel) ----
    // §16.10 dry-run: the receiver runs triage only. No conversations run,
    // no deletes apply; the sender (mirrored flag) skips its conversation
    // phase and sends TREE_END directly, whose reply reports what would be
    // deleted. The dry-run conversation count is the preview.
    // True when the parallel control loop already consumed the sender's
    // TREE_END (§16.8): the §16.5 barrier read below must then be skipped.
    let mut sender_end_consumed = false;
    let k = parallel;
    let total = conversation_paths.len();
    let mut transferred_bytes = 0u64;
    let mut in_sync = 0usize;
    if !dry_run {
        // The expected-conversation set for parallel sessions: each TREE_FILE
        // must name a path from here, exactly once (§16.8 membership-not-order).
        let mut remaining: std::collections::HashSet<String> =
            conversation_paths.iter().cloned().collect();

        // Runs one receiver-side conversation on the given stream. Shared by
        // the sequential loop and worker threads (§16.8). Grouped args struct
        // (clippy too_many_arguments) — a plain data bundle.
        struct ConvCtx<'a> {
            root: &'a Path,
            checksum: bool,
            pipeline: &'a crate::chunking::PipelineConfig,
        }
        fn run_one<S: Read + Write>(
            ctx: &ConvCtx<'_>,
            stream: &mut S,
            rel: &str,
            idx: usize,
            total: usize,
            sender_mode: Option<u32>,
            progress: &mut dyn FnMut(TreeProgress),
        ) -> Result<(SyncOutcome, String), SyncError> {
            let ConvCtx {
                root,
                checksum,
                pipeline,
            } = *ctx;
            progress(TreeProgress::FileStart(idx, total));
            let path = abs_path(root, rel);
            if let Err(e) = ensure_parent_dir(&path) {
                let err = SyncError::Protocol(format!("cannot create parent dirs for {rel}: {e}"));
                let _ = send_error_and_finish(stream, ErrorCode::Aborted, &err.to_string());
                return Err(err);
            }

            // The per-file HELLO must arrive now (the sender initiates).
            // We answer with our own HELLO inside sync::receive; our request
            // points at the file's local state (target path; local_source is
            // the same file — tree mode has no --from concept).
            let recv_req = ReceiveRequest {
                path: path.clone(),
                local_source: path.clone(),
                force_checksum: checksum,
                tree: true,
                apply_mtime: None, // filled by sync::receive from the sender's HELLO
                // §16.9: the sender's mode from the TREE_BEGIN manifest entry.
                // None (apply nothing) when the session has no metadata section
                // or the entry is absent — the lookup is by rel_path.
                apply_mode: sender_mode,
                eager: false,
            };
            let rel_c = rel.to_string();
            let outcome = match sync::receive(stream, &recv_req, pipeline, &mut |p| {
                progress(TreeProgress::FileProgress {
                    rel_path: rel_c.clone(),
                    progress: p,
                });
            }) {
                Ok(o) => o,
                Err(e) => {
                    // Announce the abort (best effort): the sender may still be
                    // reading its next conversation's reply and must fail fast
                    // instead of timing out.
                    let _ = send_error_and_finish(stream, ErrorCode::Aborted, &e.to_string());
                    return Err(e);
                }
            };
            progress(TreeProgress::FileDone(idx, total));
            Ok((outcome, rel.to_string()))
        }

        if k <= 1 {
            let ctx = ConvCtx {
                root: &request.root,
                checksum,
                pipeline,
            };
            for (idx, rel) in conversation_paths.iter().enumerate() {
                let sender_mode = sender_mode_of(&peer_manifest, rel, metadata);
                let (outcome, _) = run_one(&ctx, stream, rel, idx, total, sender_mode, progress)?;
                match outcome {
                    SyncOutcome::InSync => in_sync += 1,
                    SyncOutcome::Transferred { bytes, .. } => transferred_bytes += bytes,
                    SyncOutcome::WouldChange => {} // unreachable: dry-run sessions skip the conversation phase
                }
                remaining.remove(rel);
            }
        } else {
            // Parallel (§16.8): accept K−1 workers (any order), each announces
            // itself with TREE_WORKER and is ACKed. Then every connection runs
            // its own service loop — read TREE_FILE, validate membership
            // (expected set, exactly once), run that conversation to
            // completion, repeat — one native thread per connection, so an
            // idle connection never blocks another (blocking I/O, no async
            // runtime, per D13's constraint). The control connection's loop is
            // the control thread itself: it reads TREE_FILEs (serving each)
            // until the sender's TREE_END arrives there, joins the workers,
            // then decides success/failure for the whole session.
            let mut worker_streams: Vec<Option<Box<dyn ReadWrite>>> =
                (0..k as usize).map(|_| None).collect();
            // Workers connect in ARBITRARY order (the sender's threads race):
            // accept any connection, read its TREE_WORKER, and place it by the
            // ANNOUNCED index — then verify the set is exactly {1..K−1} (§16.8:
            // "expects exactly the set {1..K−1}, in any arrival order").
            for _ in 1..k {
                let (mut s, announced) = {
                    let s = (request.accept_worker)().map_err(|e| {
                        let err = SyncError::Io(e);
                        let _ = send_error_and_finish(
                            stream,
                            ErrorCode::Aborted,
                            &format!("cannot accept worker: {err}"),
                        );
                        err
                    })?;
                    let mut s = s;
                    let frame = framing::read_frame(&mut *s)?;
                    let announced =
                        message::TreeWorker::from_frame(frame).map_err(|e| match e {
                            MessageError::PeerError(code, msg) => SyncError::Peer(code, msg),
                            other => SyncError::Protocol(format!("bad TREE_WORKER: {other}")),
                        })?;
                    (s, announced)
                };
                let w = announced.worker_index;
                if w == 0 || w >= k {
                    let err =
                        SyncError::Protocol(format!("worker announced index {w} (valid: 1..{k})"));
                    let _ = send_error_and_finish(&mut *s, ErrorCode::Aborted, &err.to_string());
                    return Err(err);
                }
                if worker_streams[w as usize].is_some() {
                    let err =
                        SyncError::Protocol(format!("duplicate worker announcement for index {w}"));
                    let _ = send_error_and_finish(&mut *s, ErrorCode::Aborted, &err.to_string());
                    return Err(err);
                }
                message::TreeWorker { worker_index: w }
                    .into_frame()?
                    .encode(&mut *s)?;
                s.flush()?;
                worker_streams[w as usize] = Some(s);
            }

            // Shared coordination state:
            // - `remaining`: the expected conversation set (membership check,
            //   exactly once per path).
            // - `first_error` + `cancel`: any thread's failure stops the others
            //   before their NEXT conversation (§16.8 failure semantics).
            // - events: (Start/Progress/Done) funnel to the control thread,
            //   which owns the progress callback (it is not Send).
            let remaining = std::sync::Arc::new(std::sync::Mutex::new(remaining));
            let first_error = std::sync::Arc::new(std::sync::Mutex::new(None::<SyncError>));
            let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (event_tx, event_rx) = std::sync::mpsc::channel::<ConvEvent>();

            // Serves one connection end-to-end. Used for worker threads; the
            // control connection runs the same loop inline (it must also read
            // the sender's TREE_END there, and one stream = one reader).
            struct Shared {
                remaining: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
                cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
                first_error: std::sync::Arc<std::sync::Mutex<Option<SyncError>>>,
                root: PathBuf,
                checksum: bool,
                /// §16.9: the sender's per-entry modes (empty when the session
                /// has no metadata section).
                sender_modes: std::collections::HashMap<String, u32>,
                pipeline: crate::chunking::PipelineConfig,
            }
            impl Shared {
                fn record_error(&self, err: SyncError) {
                    let mut first = self.first_error.lock().unwrap();
                    if first.is_none() {
                        *first = Some(err);
                    }
                    self.cancel
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
            let shared = std::sync::Arc::new(Shared {
                remaining: std::sync::Arc::clone(&remaining),
                cancel: std::sync::Arc::clone(&cancel),
                first_error: std::sync::Arc::clone(&first_error),
                root: request.root.clone(),
                checksum,
                sender_modes: sender_modes_of(&peer_manifest, metadata),
                pipeline: pipeline_clone(pipeline),
            });
            let serve_connection = {
                let shared = std::sync::Arc::clone(&shared);
                move |mut s: Box<dyn ReadWrite>, event_tx: std::sync::mpsc::Sender<ConvEvent>| {
                    loop {
                        if shared.cancel.load(std::sync::atomic::Ordering::Relaxed) {
                            return; // some connection failed; stop before the next conversation
                        }
                        let frame = match framing::read_frame(&mut *s) {
                            Ok(f) => f,
                            // The sender closed this connection: its partition is
                            // over (success or failure decided via first_error and
                            // the remaining set after the joins).
                            Err(framing::FrameError::Eof) => return,
                            Err(e) => {
                                shared.record_error(SyncError::from(e));
                                return;
                            }
                        };
                        let named = match message::TreeFile::from_frame(frame) {
                            Ok(n) => n,
                            Err(MessageError::PeerError(code, msg)) => {
                                shared.record_error(SyncError::Peer(code, msg));
                                return;
                            }
                            Err(other) => {
                                shared.record_error(SyncError::Protocol(format!(
                                    "bad TREE_FILE: {other}"
                                )));
                                return;
                            }
                        };
                        // Membership check (§16.8): exactly once, from the set.
                        let taken = shared.remaining.lock().unwrap().remove(&named.rel_path);
                        if !taken {
                            let err = SyncError::Protocol(format!(
                                "unexpected conversation: {} (not in the expected set, or already done)",
                                named.rel_path
                            ));
                            let _ = send_error_and_finish(
                                &mut *s,
                                ErrorCode::Aborted,
                                &err.to_string(),
                            );
                            shared.record_error(err);
                            return;
                        }
                        let rel = named.rel_path.clone();
                        let _ = event_tx.send(ConvEvent::Start(rel.clone()));
                        let sender_mode = shared.sender_modes.get(&rel).copied();
                        let outcome = run_one(
                            &ConvCtx {
                                root: &shared.root,
                                checksum: shared.checksum,
                                pipeline: &shared.pipeline,
                            },
                            &mut s,
                            &rel,
                            0,
                            0,
                            sender_mode,
                            &mut |p| {
                                if let TreeProgress::FileProgress { rel_path, progress } = p {
                                    let _ = event_tx.send(ConvEvent::Progress(rel_path, progress));
                                }
                            },
                        );
                        match outcome {
                            Ok((o, _)) => {
                                let _ = event_tx.send(ConvEvent::Done(rel, o));
                            }
                            Err(e) => {
                                let _ = send_error_and_finish(
                                    &mut *s,
                                    ErrorCode::Aborted,
                                    &e.to_string(),
                                );
                                shared.record_error(e);
                                return;
                            }
                        }
                    }
                }
            };

            // Worker service threads.
            let mut handles = Vec::new();
            for slot in worker_streams.into_iter().flatten() {
                let serve = serve_connection.clone();
                let tx = event_tx.clone();
                handles.push(std::thread::spawn(move || {
                    serve(slot, tx);
                }));
            }
            drop(event_tx); // the control thread's copy; Done events keep flowing from workers

            // Control connection: the same service loop inline. The loop ends
            // when the sender's TREE_END arrives here — which the sender sends
            // only after ALL its conversations (partitions + joined workers)
            // succeeded, so reaching TREE_END means every conversation it knows
            // of finished; the membership set must be empty then.
            let mut idx = 0usize;
            let mut session_error: Option<SyncError> = None;
            let mut saw_tree_end = false;
            loop {
                // Forward whatever workers reported while we were blocked.
                while let Ok(ev) = event_rx.try_recv() {
                    match ev {
                        ConvEvent::Start(_) => {
                            progress(TreeProgress::FileStart(idx, total));
                        }
                        ConvEvent::Progress(rel_path, p) => {
                            progress(TreeProgress::FileProgress {
                                rel_path,
                                progress: p,
                            });
                        }
                        ConvEvent::Done(_, o) => {
                            progress(TreeProgress::FileDone(idx, total));
                            idx += 1;
                            match o {
                                SyncOutcome::InSync => in_sync += 1,
                                SyncOutcome::Transferred { bytes, .. } => {
                                    transferred_bytes += bytes
                                }
                                SyncOutcome::WouldChange => {} // unreachable: dry-run sessions skip the conversation phase
                            }
                        }
                    }
                }
                if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                    break; // a worker failed; stop serving (the error read below)
                }
                let frame = match framing::read_frame(stream) {
                    Ok(f) => f,
                    Err(framing::FrameError::Eof) => {
                        // Sender dropped the control stream without TREE_END:
                        // it saw a failure (its ERROR frame is either already
                        // consumed above or the connection reset first).
                        break;
                    }
                    Err(e) => {
                        session_error = Some(SyncError::from(e));
                        break;
                    }
                };
                match frame.ftype {
                    framing::T_TREE_FILE => {
                        let named = match message::TreeFile::from_frame(frame) {
                            Ok(n) => n,
                            Err(MessageError::PeerError(code, msg)) => {
                                session_error = Some(SyncError::Peer(code, msg));
                                break;
                            }
                            Err(other) => {
                                session_error =
                                    Some(SyncError::Protocol(format!("bad TREE_FILE: {other}")));
                                break;
                            }
                        };
                        let taken = remaining.lock().unwrap().remove(&named.rel_path);
                        if !taken {
                            session_error = Some(SyncError::Protocol(format!(
                                "unexpected conversation: {} (not in the expected set, or already done)",
                                named.rel_path
                            )));
                            break;
                        }
                        let rel = named.rel_path.clone();
                        let sender_mode =
                            peer_manifest.get(&rel).filter(|_| metadata).map(|e| e.mode);
                        let outcome = run_one(
                            &ConvCtx {
                                root: &request.root,
                                checksum,
                                pipeline,
                            },
                            stream,
                            &rel,
                            idx,
                            total,
                            sender_mode,
                            &mut |tp| progress(tp),
                        );
                        idx += 1;
                        match outcome {
                            Ok((SyncOutcome::InSync, _)) => in_sync += 1,
                            Ok((SyncOutcome::Transferred { bytes, .. }, _)) => {
                                transferred_bytes += bytes
                            }

                            Ok((SyncOutcome::WouldChange, _)) => {}
                            Err(e) => {
                                let _ = send_error_and_finish(
                                    stream,
                                    ErrorCode::Aborted,
                                    &e.to_string(),
                                );
                                session_error = Some(e);
                                break;
                            }
                        }
                    }
                    framing::T_TREE_END => {
                        saw_tree_end = true;
                        break;
                    }
                    framing::T_ERROR => {
                        session_error = Some(match message::read_error_payload(&frame) {
                            Ok((code, msg)) => SyncError::Peer(code, msg),
                            Err(e) => SyncError::Protocol(format!("bad ERROR frame: {e}")),
                        });
                        break;
                    }
                    other => {
                        session_error = Some(SyncError::Protocol(format!(
                            "expected TREE_FILE or TREE_END on the control connection, got 0x{:02X}",
                            other
                        )));
                        break;
                    }
                }
            }

            // Join worker threads, then drain the rest of the event stream.
            for h in handles {
                if h.join().is_err() && session_error.is_none() {
                    session_error = Some(SyncError::Protocol("worker thread panicked".into()));
                }
            }
            while let Ok(ev) = event_rx.try_recv() {
                match ev {
                    ConvEvent::Start(_) => {
                        progress(TreeProgress::FileStart(idx, total));
                    }
                    ConvEvent::Progress(rel_path, p) => {
                        progress(TreeProgress::FileProgress {
                            rel_path,
                            progress: p,
                        });
                    }
                    ConvEvent::Done(_, o) => {
                        progress(TreeProgress::FileDone(idx, total));
                        idx += 1;
                        match o {
                            SyncOutcome::InSync => in_sync += 1,
                            SyncOutcome::Transferred { bytes, .. } => transferred_bytes += bytes,
                            SyncOutcome::WouldChange => {} // unreachable: dry-run sessions skip the conversation phase
                        }
                    }
                }
            }

            // Decide the session: error recorded anywhere wins; then TREE_END
            // must have arrived AND every expected conversation must have run.
            let recorded = first_error.lock().unwrap().take();
            let has_error = session_error.is_some() || recorded.is_some();
            if !has_error {
                if !saw_tree_end {
                    return Err(SyncError::Protocol(
                        "sender ended the control connection without TREE_END".to_string(),
                    ));
                }
                let pending = remaining.lock().unwrap().len();
                if pending > 0 {
                    return Err(SyncError::Protocol(format!(
                        "sender's TREE_END arrived with {pending} expected conversations never announced"
                    )));
                }
                // Success. The control loop already consumed the sender's
                // TREE_END (that is how it knew to stop serving), so the §16.5
                // barrier read below must be skipped — the delete report is
                // still sent from here. Gate the barrier read on a flag.
                sender_end_consumed = true;
            } else {
                let err = session_error
                    .or(recorded)
                    .unwrap_or_else(|| SyncError::Protocol("session failed".to_string()));
                return Err(err);
            }
        }
    } // end of the §16.10 dry-run gate: the conversation phase runs only
    // when !dry_run; in dry-run the delete pass below becomes the
    // would-delete report.

    // ---- TREE_END (§16.5): the sender's empty TREE_END is the "transfers
    // done" barrier. Read it BEFORE applying deletions (a sender aborting
    // mid-session must leave the receiver's tree intact apart from synced
    // files), then reply with the delete report.
    // NOTE: the sender's TREE_END is already in flight the moment its last
    // conversation's COMMIT_ACK was sent; by the time we reach here it is
    // buffered locally, so this read does not round-trip.
    // In a parallel session the control loop already consumed it (§16.8).
    if !sender_end_consumed {
        let sender_end_frame = framing::read_frame(stream)?;
        match sender_end_frame.ftype {
            framing::T_TREE_END => {
                let end = TreeEnd::from_frame(sender_end_frame)?;
                if end.deleted_count != 0 || end.deleted_bytes != 0 {
                    return Err(SyncError::Protocol(
                        "sender TREE_END must be empty (delete report is receiver→sender)"
                            .to_string(),
                    ));
                }
            }
            framing::T_ERROR => {
                let (code, msg) = message::read_error_payload(&sender_end_frame)?;
                return Err(SyncError::Peer(code, msg));
            }
            other => {
                return Err(SyncError::Protocol(format!(
                    "expected TREE_END barrier, got 0x{:02X}",
                    other
                )));
            }
        }
    }

    // ---- Deletions (§16.5): only after every conversation succeeded ----
    // Exclude protection (§16.7): paths matching the receiver's own patterns
    // — directly or via an excluded ancestor — are never deleted, even
    // though the (sender-filtered) manifests marked them receiver-only.
    // §16.10 dry-run: nothing is removed — the counts below become the
    // would-delete report.
    let mut deleted_count: u32 = 0;
    let mut deleted_bytes: u64 = 0;
    if !no_delete && !deletions.is_empty() {
        // Deepest paths first so directories empty out before removal.
        deletions.sort_by(|a, b| b.0.cmp(&a.0));
        for (i, (rel, size)) in deletions.iter().enumerate() {
            let kind = manifest.get(rel).map(|e| e.kind).unwrap_or(EntryKind::File);
            if own_excludes.matches_path_or_ancestor(rel, kind) {
                continue;
            }
            if dry_run {
                deleted_count += 1;
                deleted_bytes += size;
                progress(TreeProgress::Deleting(i + 1, deleted_bytes));
                continue;
            }
            let p = abs_path(&request.root, rel);
            let res = if p.is_dir() {
                std::fs::remove_dir(&p)
            } else {
                std::fs::remove_file(&p)
            };
            match res {
                Ok(()) => {
                    deleted_count += 1;
                    deleted_bytes += size;
                    // Remove the orphaned D15 index of a deleted file.
                    if p.is_file() {
                        crate::index::remove_index(&p);
                    }
                }
                // Already gone — or its ancestor became a file when a kind
                // conflict was resolved this session (the conflict removal
                // destroyed the subtree, §16.3). On Linux that surfaces as
                // ENOTDIR, on Windows as ERROR_PATH_NOT_FOUND; either way
                // nothing is left to delete at this path.
                Err(e)
                    if e.kind() == io::ErrorKind::NotFound
                        || e.kind() == io::ErrorKind::NotADirectory => {}
                Err(e) => {
                    return Err(SyncError::Protocol(format!("cannot delete {rel}: {e}")));
                }
            }
            progress(TreeProgress::Deleting(i + 1, deleted_bytes));
        }
    }

    // ---- TREE_END with the delete report (§16.5) ----
    TreeEnd {
        deleted_count,
        deleted_bytes,
    }
    .into_frame()?
    .encode(stream)?;
    stream.flush()?;

    Ok(TreeOutcome {
        files: total,
        transferred_bytes,
        in_sync,
        deleted: deleted_count,
        deleted_bytes,
    })
}
