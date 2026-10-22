//! Tree session state machines (D17, protocol v3 §16): orchestrate the
//! per-file sync machines over one connection inside a TREE_BEGIN /
//! TREE_END envelope.
//!
//! Shape of a session (both directions of the envelope, one connection):
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
//! Determinism contract (§16.4): both peers compute the same action list
//! from the same two manifests, so the receiver knows exactly which files
//! will have conversations and fails loudly on any unexpected HELLO.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::protocol::framing::{self};
use crate::protocol::message::{self, ErrorCode, TreeBegin, TreeEnd, TreeEntryWire};
use crate::protocol::sync::{
    self, Progress, ReceiveRequest, SendRequest, SyncError, SyncOutcome, send_error_and_finish,
};
use crate::tree::{self, EntryKind, TREE_FLAG_CHECKSUM, TREE_FLAG_NO_DELETE, TreeManifest, Triage};

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
}

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

fn send_tree_begin(
    stream: &mut impl Write,
    flags: u16,
    excludes: &[String],
    manifest: &TreeManifest,
) -> Result<(), SyncError> {
    TreeBegin {
        flags,
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
    };

    // ---- TREE_BEGIN exchange ----
    send_tree_begin(stream, flags, &request.excludes, &manifest)?;
    let (peer_begin, peer_manifest) = read_tree_begin(stream)?;
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

    let total = actions.len();
    let mut transferred_bytes = 0u64;
    let mut in_sync = 0usize;

    // ---- Per-file conversations (§16.4) ----
    for (idx, rel) in actions.iter().enumerate() {
        progress(TreeProgress::FileStart(idx, total));
        let path = abs_path(&request.root, rel);
        let meta = std::fs::metadata(&path).map_err(|e| {
            let err = SyncError::Protocol(format!("sender file vanished: {rel}: {e}"));
            // Announce the abort: the peer is waiting for the next per-file
            // HELLO and must fail fast instead of timing out.
            let _ = send_error_and_finish(stream, ErrorCode::Aborted, &err.to_string());
            err
        })?;
        let mtime = meta.modified().ok();

        let send_req = SendRequest {
            path: path.clone(),
            display_path: rel.clone(),
            mtime,
            force_checksum: checksum,
            tree: true,
        };
        let rel_for_progress = rel.clone();
        let mut file_progress = |p: Progress| {
            progress(TreeProgress::FileProgress {
                rel_path: rel_for_progress.clone(),
                progress: p,
            });
        };
        let outcome = match sync::send(stream, &send_req, pipeline, &mut file_progress) {
            Ok(o) => o,
            Err(e) => {
                // Announce the abort (best effort — the peer may be the one
                // that caused it and is already gone).
                let _ = send_error_and_finish(stream, ErrorCode::Aborted, &e.to_string());
                return Err(e);
            }
        };
        match outcome {
            SyncOutcome::InSync => in_sync += 1,
            SyncOutcome::Transferred { bytes, .. } => transferred_bytes += bytes,
        }
        progress(TreeProgress::FileDone(idx, total));
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

    let flags = if request.force_checksum {
        TREE_FLAG_CHECKSUM
    } else {
        0
    } | if request.no_delete {
        TREE_FLAG_NO_DELETE
    } else {
        0
    };
    // The receiver's own patterns never touch manifests (§16.7); they only
    // guard the delete pass below. They are NOT advertised on the wire.
    let own_excludes = tree::ExcludeSet::from_patterns(&request.excludes)
        .map_err(|e| SyncError::Protocol(format!("bad exclude pattern: {e}")))?;

    send_tree_begin(stream, flags, &[], &manifest)?;
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
                // conversation file may land inside it).
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
                        // conversation can write the file there.
                        let p = abs_path(&request.root, rel);
                        if p.is_dir()
                            && let Err(e) = std::fs::remove_dir_all(&p)
                        {
                            announce!(SyncError::Protocol(format!(
                                "cannot replace dir {rel} with file: {e}"
                            )));
                        }
                        conversation_paths.push(rel.clone());
                    }
                    EntryKind::Dir => {
                        let p = abs_path(&request.root, rel);
                        if p.is_file()
                            && let Err(e) = std::fs::remove_file(&p)
                        {
                            announce!(SyncError::Protocol(format!(
                                "cannot replace file {rel} with dir: {e}"
                            )));
                        }
                        if let Err(e) = std::fs::create_dir_all(&p) {
                            announce!(SyncError::Protocol(format!("cannot create dir {rel}: {e}")));
                        }
                    }
                }
            }
        }
    }

    // ---- Per-file conversations (§16.4): expect exactly the sender's set ----
    let total = conversation_paths.len();
    let mut transferred_bytes = 0u64;
    let mut in_sync = 0usize;

    for (idx, rel) in conversation_paths.iter().enumerate() {
        progress(TreeProgress::FileStart(idx, total));
        let path = abs_path(&request.root, rel);
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
        };
        let rel_for_progress = rel.clone();
        let mut file_progress = |p: Progress| {
            progress(TreeProgress::FileProgress {
                rel_path: rel_for_progress.clone(),
                progress: p,
            });
        };
        let outcome = match sync::receive(stream, &recv_req, pipeline, &mut file_progress) {
            Ok(o) => o,
            Err(e) => {
                // Announce the abort (best effort): the sender may still be
                // reading its next conversation's reply and must fail fast
                // instead of timing out.
                let _ = send_error_and_finish(stream, ErrorCode::Aborted, &e.to_string());
                return Err(e);
            }
        };
        match outcome {
            SyncOutcome::InSync => in_sync += 1,
            SyncOutcome::Transferred { bytes, .. } => transferred_bytes += bytes,
        }
        progress(TreeProgress::FileDone(idx, total));
    }

    // ---- TREE_END (§16.5): the sender's empty TREE_END is the "transfers
    // done" barrier. Read it BEFORE applying deletions (a sender aborting
    // mid-session must leave the receiver's tree intact apart from synced
    // files), then reply with the delete report.
    // NOTE: the sender's TREE_END is already in flight the moment its last
    // conversation's COMMIT_ACK was sent; by the time we reach here it is
    // buffered locally, so this read does not round-trip.
    let sender_end_frame = framing::read_frame(stream)?;
    match sender_end_frame.ftype {
        framing::T_TREE_END => {
            let end = TreeEnd::from_frame(sender_end_frame)?;
            if end.deleted_count != 0 || end.deleted_bytes != 0 {
                return Err(SyncError::Protocol(
                    "sender TREE_END must be empty (delete report is receiver→sender)".to_string(),
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

    // ---- Deletions (§16.5): only after every conversation succeeded ----
    // Exclude protection (§16.7): paths matching the receiver's own patterns
    // — directly or via an excluded ancestor — are never deleted, even
    // though the (sender-filtered) manifests marked them receiver-only.
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
