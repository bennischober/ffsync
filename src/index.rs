//! Persistent per-file chunk index (D15): the warm-start state.
//!
//! One sidecar file per synced target, `<target>.ffsync-index`, storing what
//! the last successful sync knew about the file: identity (size, mtime,
//! chunk-tree root) and the full ordered chunk list — the same
//! `(hash, offset, length)` triples the wire manifest uses.
//!
//! Invariants (D15):
//! - Written only from verified state: the receiver writes at commit (after
//!   VERIFY + rename), the sender after a full scan (content is the source
//!   of truth on the sender side by definition).
//! - Validated, never trusted: a warm start requires size AND mtime to match
//!   the index; otherwise the index is treated as missing (cold scan).
//! - Never used as write evidence without hash verification (PLAN_LOCAL's
//!   hash field covers reads on the receiver; D12).

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use blake3::Hash;

use crate::chunking::{Chunk, chunk_root};

/// Index format magic + version: `FFSYIX01`. A mismatch means the file is
/// not an index or was written by an incompatible version — treat as missing
/// (no migration machinery in v0.2; a format bump rewrites indexes at the
/// next commit).
const MAGIC: [u8; 8] = *b"FFSYIX01";

/// Fixed-size header: magic (8) + file size (8) + mtime secs (8) + mtime
/// subsec nanos (4) + root (32) + chunk count (8) = 68 bytes.
const HEADER_LEN: usize = 8 + 8 + 8 + 4 + 32 + 8;

/// One chunk entry: hash 32 B + offset u64 + length u32 = 44 bytes (the
/// wire-manifest layout, D15).
const ENTRY_LEN: usize = 32 + 8 + 4;

/// The persisted state of one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileIndex {
    /// File size the index was written for.
    pub file_size: u64,
    /// File mtime (unix seconds + subsec nanos) at the time the index was
    /// written. Nanosecond granularity matters: whole-second mtimes make two
    /// writes within the same second indistinguishable, so a stale index
    /// would validate against modified content (found by the warm-start
    /// loopback test — the mtime convention must match `--checksum`
    /// semantics, D11/D15).
    pub file_mtime: (u64, u32),
    /// Chunk-tree root of the indexed content.
    pub root: Hash,
    /// Ordered chunk list (canonical file order — the root is
    /// order-sensitive, see `chunking::types::chunk_root`).
    pub chunks: Vec<Chunk>,
}

/// Reads a file's mtime as (unix secs, subsec nanos), or `None` if
/// unavailable.
pub fn stat_mtime(path: &Path) -> Option<(u64, u32)> {
    let meta = std::fs::metadata(path).ok()?;
    let d = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    Some((d.as_secs(), d.subsec_nanos()))
}

impl FileIndex {
    /// Builds an index from a scanned chunk list and the file's metadata.
    pub fn from_scan(chunks: Vec<Chunk>, file_size: u64, file_mtime: (u64, u32)) -> Self {
        let root = chunk_root(&chunks);
        Self {
            file_size,
            file_mtime,
            root,
            chunks,
        }
    }

    /// Serializes to bytes (little-endian, hand-rolled per D1/D15).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.chunks.len() * ENTRY_LEN);
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&self.file_size.to_le_bytes());
        out.extend_from_slice(&self.file_mtime.0.to_le_bytes());
        out.extend_from_slice(&self.file_mtime.1.to_le_bytes());
        out.extend_from_slice(self.root.as_bytes());
        out.extend_from_slice(&(self.chunks.len() as u64).to_le_bytes());
        for c in &self.chunks {
            out.extend_from_slice(c.hash.as_bytes());
            out.extend_from_slice(&c.offset.to_le_bytes());
            out.extend_from_slice(&(c.length as u32).to_le_bytes());
        }
        out
    }

    /// Parses an index from bytes. Returns `None` on any structural problem
    /// (bad magic, truncation, trailing garbage, root mismatch) — callers
    /// treat the index as missing, never as partially valid.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN || bytes[0..8] != MAGIC {
            return None;
        }
        let file_size = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        let secs = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        let nanos = u32::from_le_bytes(bytes[24..28].try_into().ok()?);
        let root = Hash::from_bytes(bytes[28..60].try_into().ok()?);
        let count = u64::from_le_bytes(bytes[60..68].try_into().ok()?);
        // Overflow guard: count × 44 must fit the remaining bytes exactly.
        if count > (bytes.len() - HEADER_LEN) as u64 / ENTRY_LEN as u64 {
            return None;
        }
        let mut chunks = Vec::with_capacity(count as usize);
        let mut pos = HEADER_LEN;
        for _ in 0..count {
            let hash = Hash::from_bytes(bytes[pos..pos + 32].try_into().ok()?);
            let offset = u64::from_le_bytes(bytes[pos + 32..pos + 40].try_into().ok()?);
            let length = u32::from_le_bytes(bytes[pos + 40..pos + 44].try_into().ok()?);
            chunks.push(Chunk {
                offset,
                length: length as usize,
                hash,
            });
            pos += ENTRY_LEN;
        }
        if pos != bytes.len() {
            return None; // trailing garbage
        }
        // Self-integrity check (D15): the stored root must equal the root
        // derived from the decoded chunk list. This catches corruption in
        // the root *or* any chunk entry at load time — O(chunk count) of
        // in-memory hashing, no file I/O — so a corrupt index is reliably
        // refused rather than silently mis-serving warm starts. (Detection
        // is not strictly required for correctness — plan reads are
        // hash-verified and VERIFY is the oracle — but a warm start that
        // fails late is expensive; failing at load is free.)
        if chunk_root(&chunks) != root {
            return None;
        }
        Some(Self {
            file_size,
            file_mtime: (secs, nanos),
            root,
            chunks,
        })
    }

    /// Validates the index against the file's current stat. Size must match
    /// exactly and mtime must match at nanosecond granularity. Any mismatch
    /// or missing file ⇒ `None` (cold start).
    pub fn validate(&self, path: &Path) -> Option<()> {
        let meta = std::fs::metadata(path).ok()?;
        if meta.len() != self.file_size {
            return None;
        }
        if stat_mtime(path)? != self.file_mtime {
            return None;
        }
        Some(())
    }
}

/// The sidecar index path for a target path (single source of truth for the
/// naming convention, like `Reconstructor::temp_path_for`). The suffix is
/// *appended*, not substituted for the last extension: `a.mkv` becomes
/// `a.mkv.ffsync-index`, so the target name is recoverable from the index
/// name by stripping the suffix — required by the orphan cleanup, and the
/// convention the concept doc originally described (D6's deviation note
/// applies only to the temp file, which needs no reverse mapping).
pub fn index_path_for(target_path: &Path) -> PathBuf {
    let mut name = target_path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".ffsync-index");
    target_path.with_file_name(name)
}

/// Writes the index atomically: temp file + rename (same pattern as the
/// reconstruction commit, D6). A crash mid-write leaves the old index (or a
/// `<path>.ffsync-index-tmp` remnant, harmless).
pub fn write_index(path: &Path, index: &FileIndex) -> io::Result<()> {
    let idx_path = index_path_for(path);
    let tmp_path = {
        let mut name = idx_path
            .file_name()
            .map(|n| n.to_os_string())
            .unwrap_or_default();
        name.push("-tmp");
        idx_path.with_file_name(name)
    };
    let bytes = index.encode();
    {
        let mut f = File::create(&tmp_path)?;
        f.write_all(&bytes)?;
        f.sync_all().ok(); // durability of the resume/warm state is cheap here
    }
    std::fs::rename(&tmp_path, &idx_path)
}

/// Loads and returns the index for `path` if it exists, parses cleanly, and
/// validates against the file's current stat. Any failure ⇒ `None`
/// (warm start refused, cold scan). Best-effort removal of a corrupt index
/// is NOT done here — the next successful commit overwrites it anyway.
pub fn load_index(path: &Path) -> Option<FileIndex> {
    let idx_path = index_path_for(path);
    let mut f = File::open(&idx_path).ok()?;
    let mut bytes = Vec::new();
    f.read_to_end(&mut bytes).ok()?;
    let index = FileIndex::decode(&bytes)?;
    index.validate(path)?;
    Some(index)
}

/// Removes the index for a target (called when a sync replaces or removes
/// the target state, and by orphan cleanup).
pub fn remove_index(path: &Path) {
    let _ = std::fs::remove_file(index_path_for(path));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_chunks(n: usize) -> Vec<Chunk> {
        (0..n)
            .map(|i| Chunk {
                offset: (i * 256) as u64,
                length: 256,
                hash: blake3::hash(format!("chunk-{i}").as_bytes()),
            })
            .collect()
    }

    #[test]
    fn index_round_trip() {
        let chunks = sample_chunks(7);
        let idx = FileIndex::from_scan(chunks, 7 * 256, (1_700_000_000, 0));
        let bytes = idx.encode();
        assert_eq!(bytes.len(), HEADER_LEN + 7 * ENTRY_LEN);
        let back = FileIndex::decode(&bytes).expect("decode");
        assert_eq!(back, idx);
    }

    #[test]
    fn index_decode_rejects_garbage() {
        assert!(FileIndex::decode(b"").is_none());
        assert!(FileIndex::decode(b"nonsense").is_none());
        let idx = FileIndex::from_scan(sample_chunks(3), 768, (42, 0));
        let mut bytes = idx.encode();
        // Truncation.
        bytes.truncate(bytes.len() - 10);
        assert!(FileIndex::decode(&bytes).is_none());
        // Trailing garbage.
        let mut bytes = idx.encode();
        bytes.extend_from_slice(&[0u8; 3]);
        assert!(FileIndex::decode(&bytes).is_none());
        // Wrong magic.
        let mut bytes = idx.encode();
        bytes[0] = b'X';
        assert!(FileIndex::decode(&bytes).is_none());
        // Chunk count beyond the payload.
        let mut bytes = idx.encode();
        bytes[56..64].copy_from_slice(&999u64.to_le_bytes());
        assert!(FileIndex::decode(&bytes).is_none());
    }

    #[test]
    fn index_validate_checks_size_and_mtime() {
        let dir = std::env::temp_dir().join(format!("ffsync_idx_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("file.bin");
        std::fs::write(&path, vec![0u8; 1000]).unwrap();

        let mtime = crate::index::stat_mtime(&path).unwrap();
        let idx = FileIndex::from_scan(sample_chunks(4), 1000, mtime);
        assert!(idx.validate(&path).is_some(), "matching stat validates");

        // Size mismatch.
        std::fs::write(&path, vec![0u8; 999]).unwrap();
        assert!(idx.validate(&path).is_none(), "size mismatch refuses");

        // Missing file.
        std::fs::remove_file(&path).unwrap();
        assert!(idx.validate(&path).is_none(), "missing file refuses");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn index_write_and_load_round_trip() {
        let dir = std::env::temp_dir().join(format!("ffsync_idx_test2_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("file.bin");
        std::fs::write(&path, vec![7u8; 512]).unwrap();
        let mtime = crate::index::stat_mtime(&path).unwrap();

        let idx = FileIndex::from_scan(sample_chunks(2), 512, mtime);
        write_index(&path, &idx).unwrap();
        let loaded = load_index(&path).expect("valid index loads");
        assert_eq!(loaded, idx);

        // Corrupt the index on disk: load must refuse (not panic).
        let idx_path = index_path_for(&path);
        let mut bytes = std::fs::read(&idx_path).unwrap();
        bytes[40] ^= 0xFF; // inside the root hash
        std::fs::write(&idx_path, &bytes).unwrap();
        assert!(load_index(&path).is_none(), "corrupt index refuses");

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
