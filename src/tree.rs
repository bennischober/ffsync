//! Directory-tree sync (D17): tree scan, wire manifest codec, and triage.
//!
//! A tree session (protocol v3, `concept/Protocol v0.1.md` §16) exchanges
//! both peers' tree manifests, triages locally and deterministically, then
//! runs the existing per-file state machines sequentially inside a
//! TREE_BEGIN/TREE_END envelope.
//!
//! Everything here is pure computation over metadata — no I/O beyond the
//! initial scan, no protocol frames — so triage rules are exhaustively
//! unit-testable.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Kind of a tree entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File = 0,
    Dir = 1,
}

impl EntryKind {
    pub(crate) fn from_u8(v: u8) -> Option<EntryKind> {
        match v {
            0 => Some(EntryKind::File),
            1 => Some(EntryKind::Dir),
            _ => None,
        }
    }
}

/// One entry of the tree manifest: relative path + the metadata the triage
/// decides on. Directories exist in the manifest only to create empty dirs
/// on the receiver (a dir with files is implied by its files).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    /// `/`-separated path relative to the synced root, no `..` components.
    pub rel_path: String,
    pub kind: EntryKind,
    pub size: u64,
    /// Unix seconds (0 if absent) — same convention as the per-file HELLO.
    pub mtime_secs: u64,
    pub mtime_nanos: u32,
}

impl TreeEntry {
    /// Scans one filesystem path into an entry (absolute or root-relative
    /// input; the caller supplies the relative path).
    pub fn from_metadata(rel_path: String, kind: EntryKind, meta: &std::fs::Metadata) -> Self {
        let mtime = meta.modified().ok();
        let (mtime_secs, mtime_nanos) = mtime
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| (d.as_secs(), d.subsec_nanos()))
            .unwrap_or((0, 0));
        Self {
            rel_path,
            kind,
            size: if kind == EntryKind::File {
                meta.len()
            } else {
                0
            },
            mtime_secs,
            mtime_nanos,
        }
    }
}

/// A whole-tree manifest, sorted by rel_path (byte order — the wire format's
/// canonical order, §16.2). The BTreeMap keyed by rel_path is the in-memory
/// form; the wire form is the sorted entry list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TreeManifest {
    entries: BTreeMap<String, TreeEntry>,
}

impl TreeManifest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts an entry, replacing any previous entry for the same path.
    pub fn insert(&mut self, entry: TreeEntry) {
        self.entries.insert(entry.rel_path.clone(), entry);
    }

    pub fn get(&self, rel_path: &str) -> Option<&TreeEntry> {
        self.entries.get(rel_path)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Entries in canonical (sorted) order.
    pub fn entries(&self) -> impl Iterator<Item = &TreeEntry> {
        self.entries.values()
    }

    /// Total file bytes (for progress and the delete report).
    pub fn total_file_bytes(&self) -> u64 {
        self.entries
            .values()
            .filter(|e| e.kind == EntryKind::File)
            .map(|e| e.size)
            .sum()
    }

    /// Scans a directory tree rooted at `root` into a manifest. Follows
    /// symlinks (a symlink is recorded as its target type; a dangling
    /// symlink is skipped with a warning — D17, recorded not silent).
    /// The returned error is only for fatal I/O on the root itself.
    pub fn scan(root: &Path) -> io::Result<TreeManifest> {
        let mut manifest = TreeManifest::new();
        let root_meta = std::fs::metadata(root)?;
        if !root_meta.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("tree root is not a directory: {}", root.display()),
            ));
        }
        Self::scan_dir(root, "", &mut manifest)?;
        Ok(manifest)
    }

    fn scan_dir(dir: &Path, prefix: &str, out: &mut TreeManifest) -> io::Result<()> {
        let mut entries: Vec<(String, PathBuf, std::fs::Metadata)> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let meta = match entry.metadata() {
                Ok(m) => m,
                // A file deleted between read_dir and metadata: skip, the
                // next sync sees a consistent state. Anything else is fatal.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            let rel = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            entries.push((rel, entry.path(), meta));
        }
        // Deterministic order regardless of filesystem enumeration order.
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        for (rel, path, meta) in entries {
            if meta.is_dir() {
                out.insert(TreeEntry::from_metadata(rel.clone(), EntryKind::Dir, &meta));
                Self::scan_dir(&path, &rel, out)?;
            } else if meta.is_file() {
                // D15/D17: the sidecar index and the temp file are ffsync's
                // own bookkeeping, never tree content. The index filename is
                // `<target>.ffsync-index` (suffix APPENDED — see index.rs);
                // the temp is `<target-with-ext>.ffsync-tmp`. Both are
                // excluded from tree manifests: syncing them would (a) sync
                // our bookkeeping to the peer and (b) desync on every run
                // (an index written at commit changes mtime → re-transfer
                // loop).
                if rel.ends_with(".ffsync-index") || rel.ends_with(".ffsync-tmp") {
                    continue;
                }
                out.insert(TreeEntry::from_metadata(rel, EntryKind::File, &meta));
            } else {
                // Symlink to something else (fifo, socket, ...): skip with a
                // warning (D17: recorded, not silent).
                eprintln!("warning: skipping non-regular file: {}", path.display());
            }
        }
        Ok(())
    }
}

/// Tree-level flags carried by TREE_BEGIN (§16.2).
pub const TREE_FLAG_CHECKSUM: u16 = 1 << 0; // D11 tree-wide: no fast path
pub const TREE_FLAG_NO_DELETE: u16 = 1 << 1; // §16.5 opt-out

/// What the receiver should do with one path (§16.3). Computed identically
/// on both sides from the same two manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Triage {
    /// Metadata matches (and no `checksum` flag): nothing to do.
    Skip,
    /// Exists on both sides as files, metadata differs: per-file conversation.
    Sync,
    /// Sender-only file: per-file conversation; receiver starts from nothing.
    Transfer,
    /// Sender-only directory: the receiver creates it locally (no
    /// conversation — a directory carries no content).
    CreateDir,
    /// Receiver-only file: deferred deletion (§16.5), gated on `no_delete`.
    Delete,
    /// File on one side, dir on the other. The sender's kind wins: the
    /// receiver removes its own entry of the other kind and applies the
    /// sender's. Carries the sender's kind for the receiver to enforce.
    Conflict {
        sender_kind: EntryKind,
        /// Also delete files/dirs under the receiver's conflicting entry.
        remove_receiver_entry: bool,
    },
}

impl fmt::Display for Triage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Triage::Skip => write!(f, "skip"),
            Triage::Sync => write!(f, "sync"),
            Triage::Transfer => write!(f, "transfer"),
            Triage::CreateDir => write!(f, "create-dir"),
            Triage::Delete => write!(f, "delete"),
            Triage::Conflict { .. } => write!(f, "conflict"),
        }
    }
}

/// The triage of one path, given both sides' entries (`None` = absent).
/// Pure function — identical on both peers (§16.4's determinism contract).
pub fn triage(
    _rel_path: &str,
    sender: Option<&TreeEntry>,
    receiver: Option<&TreeEntry>,
    checksum: bool,
) -> Triage {
    match (sender, receiver) {
        (None, None) => Triage::Skip, // not in either tree; unreachable via manifests
        (Some(s), None) => match s.kind {
            EntryKind::File => Triage::Transfer,
            // Sender-only dir: the receiver creates it (empty dirs are
            // first-class tree members, §16.2/§16.3).
            EntryKind::Dir => Triage::CreateDir,
        },
        (None, Some(_r)) => {
            // Receiver-only content: file deleted; a receiver-only empty dir
            // is also removed (the sender's tree is authoritative), but an
            // empty dir carries no data — its removal is part of the same
            // delete pass.
            Triage::Delete
        }
        (Some(s), Some(r)) => {
            match (s.kind, r.kind) {
                (EntryKind::Dir, EntryKind::Dir) => Triage::Skip,
                (EntryKind::File, EntryKind::File) => {
                    if !checksum
                        && s.size == r.size
                        && s.mtime_secs == r.mtime_secs
                        && s.mtime_nanos == r.mtime_nanos
                    {
                        Triage::Skip
                    } else {
                        Triage::Sync
                    }
                }
                // Kind conflict: the sender's kind wins (§16.3).
                _ => Triage::Conflict {
                    sender_kind: s.kind,
                    remove_receiver_entry: true,
                },
            }
        }
    }
}

/// Validates a relative path per §16.2: no empty components, no `..`, no
/// leading/trailing slash, no backslash (Windows is source-side too; the
/// wire form is always `/`-separated).
pub fn validate_rel_path(p: &str) -> Result<(), String> {
    if p.is_empty() {
        return Err("empty path".to_string());
    }
    if p.starts_with('/') || p.ends_with('/') {
        return Err(format!("leading or trailing slash: {p:?}"));
    }
    if p.contains('\\') {
        return Err(format!(
            "backslash in path (wire form is /-separated): {p:?}"
        ));
    }
    if p.split('/').any(|c| c.is_empty() || c == "." || c == "..") {
        return Err(format!("empty, '.', or '..' path component: {p:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(path: &str, kind: EntryKind, size: u64, secs: u64, nanos: u32) -> TreeEntry {
        TreeEntry {
            rel_path: path.to_string(),
            kind,
            size,
            mtime_secs: secs,
            mtime_nanos: nanos,
        }
    }

    #[test]
    fn triage_metadata_match_skips() {
        let s = entry("a.bin", EntryKind::File, 100, 1600000000, 500);
        let r = entry("a.bin", EntryKind::File, 100, 1600000000, 500);
        assert_eq!(triage("a.bin", Some(&s), Some(&r), false), Triage::Skip);
        // checksum mode forces Sync despite metadata equality (D11).
        assert_eq!(triage("a.bin", Some(&s), Some(&r), true), Triage::Sync);
    }

    #[test]
    fn triage_size_or_mtime_difference_syncs() {
        let s = entry("a.bin", EntryKind::File, 100, 1600000000, 500);
        // Size differs.
        let r1 = entry("a.bin", EntryKind::File, 101, 1600000000, 500);
        assert_eq!(triage("a.bin", Some(&s), Some(&r1), false), Triage::Sync);
        // Only sub-second mtime differs (the v0.2 lesson, now in triage).
        let r2 = entry("a.bin", EntryKind::File, 100, 1600000000, 501);
        assert_eq!(triage("a.bin", Some(&s), Some(&r2), false), Triage::Sync);
        // Whole-second equal, nanos equal, but secs differ.
        let r3 = entry("a.bin", EntryKind::File, 100, 1600000001, 500);
        assert_eq!(triage("a.bin", Some(&s), Some(&r3), false), Triage::Sync);
    }

    #[test]
    fn triage_one_sided_and_kind_conflicts() {
        let f = entry("x", EntryKind::File, 5, 1, 0);
        let d = entry("x", EntryKind::Dir, 0, 1, 0);
        // Sender-only file → transfer; sender-only dir → create on receiver.
        assert_eq!(triage("x", Some(&f), None, false), Triage::Transfer);
        assert_eq!(triage("x", Some(&d), None, false), Triage::CreateDir);
        // Receiver-only anything → delete (sender authoritative).
        assert_eq!(triage("x", None, Some(&f), false), Triage::Delete);
        assert_eq!(triage("x", None, Some(&d), false), Triage::Delete);
        // Kind conflict: sender's kind wins.
        assert_eq!(
            triage("x", Some(&f), Some(&d), false),
            Triage::Conflict {
                sender_kind: EntryKind::File,
                remove_receiver_entry: true
            }
        );
        assert_eq!(
            triage("x", Some(&d), Some(&f), false),
            Triage::Conflict {
                sender_kind: EntryKind::Dir,
                remove_receiver_entry: true
            }
        );
    }

    #[test]
    fn rel_path_validation() {
        assert!(validate_rel_path("a").is_ok());
        assert!(validate_rel_path("a/b/c.txt").is_ok());
        assert!(validate_rel_path("").is_err());
        assert!(validate_rel_path("/abs").is_err());
        assert!(validate_rel_path("a/").is_err());
        assert!(validate_rel_path("a//b").is_err());
        assert!(validate_rel_path("a/../b").is_err());
        assert!(validate_rel_path("a/./b").is_err());
        assert!(validate_rel_path("..").is_err());
        assert!(validate_rel_path("a\\b").is_err());
    }

    #[test]
    fn manifest_scan_and_canonical_order() {
        let dir = std::env::temp_dir().join(format!("ffsync_tree_scan_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("sub/deep")).unwrap();
        std::fs::create_dir_all(dir.join("empty_dir")).unwrap();
        std::fs::write(dir.join("b.txt"), b"bbb").unwrap();
        std::fs::write(dir.join("a.txt"), b"a").unwrap();
        std::fs::write(dir.join("sub/one.bin"), b"1111").unwrap();
        std::fs::write(dir.join("sub/deep/two.bin"), b"22").unwrap();

        let m = TreeManifest::scan(&dir).unwrap();
        let paths: Vec<&str> = m.entries().map(|e| e.rel_path.as_str()).collect();
        // Sorted, with dirs and empty_dir present.
        assert_eq!(
            paths,
            vec![
                "a.txt",
                "b.txt",
                "empty_dir",
                "sub",
                "sub/deep",
                "sub/deep/two.bin",
                "sub/one.bin"
            ]
        );
        // File sizes recorded.
        assert_eq!(m.get("a.txt").unwrap().size, 1);
        assert_eq!(m.get("sub/one.bin").unwrap().size, 4);
        assert_eq!(m.get("empty_dir").unwrap().kind, EntryKind::Dir);
        // Total bytes: 1 + 3 + 4 + 2.
        assert_eq!(m.total_file_bytes(), 10);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scan_rejects_non_directory_root() {
        let f = std::env::temp_dir().join(format!("ffsync_tree_notadir_{}", std::process::id()));
        std::fs::write(&f, b"x").unwrap();
        assert!(TreeManifest::scan(&f).is_err());
        let _ = std::fs::remove_file(&f);
    }
}
