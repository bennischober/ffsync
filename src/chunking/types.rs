use blake3::Hash;

/// Represents a chunk with its metadata
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub offset: u64,
    pub length: usize,
    pub hash: Hash,
}

/// Result of chunking operation
pub enum ChunkEvent {
    /// A chunk was processed (metadata only, no data copy)
    Chunk(Chunk),
    /// Chunking completed
    Done,
}

/// The root hash of a file under ffsync's chunk-tree definition:
/// `BLAKE3(CHUNK_ROOT_PREFIX || chunk_hashes_in_file_order)` where
/// `CHUNK_ROOT_PREFIX` is the 32-byte personalization below. This is NOT the
/// standard BLAKE3 file hash (whose internal tree is over fixed 1024-byte
/// blocks, not CDC chunk boundaries); it is a single BLAKE3 hash over the
/// ordered chunk-hash list, domain-separated from every other BLAKE3 use in
/// this codebase.
///
/// Property: two files with equal root hashes are (collision-assumption)
/// chunk-identical. The root is therefore simultaneously the change-detection
/// identity, the end-to-end verification oracle, and (with a persisted chunk
/// index) the file identity for warm starts (D5, D11).
pub const CHUNK_ROOT_PREFIX: [u8; 32] = *b"ffsync chunk tree root v1\0\0\0\0\0\0\0";

/// Computes the chunk-tree root hash from the ordered chunk list of a file.
pub fn chunk_root(chunks: &[Chunk]) -> Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&CHUNK_ROOT_PREFIX);
    for chunk in chunks {
        hasher.update(chunk.hash.as_bytes());
    }
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_root_is_order_sensitive_and_prefix_bound() {
        let c1 = Chunk {
            offset: 0,
            length: 4,
            hash: blake3::hash(b"aaaa"),
        };
        let c2 = Chunk {
            offset: 4,
            length: 4,
            hash: blake3::hash(b"bbbb"),
        };
        let ab = chunk_root(&[c1.clone(), c2.clone()]);
        let ba = chunk_root(&[c2.clone(), c1.clone()]);
        // Order matters.
        assert_ne!(ab, ba);
        // The same chunk list gives the same root.
        assert_eq!(chunk_root(&[c1.clone(), c2.clone()]), ab);
        // An empty chunk list has a defined root (domain-separated).
        let _empty = chunk_root(&[]);
        // Root differs from the plain BLAKE3 hash of the concatenated hashes
        // (prefix actually applies).
        let mut concat = Vec::new();
        concat.extend_from_slice(c1.hash.as_bytes());
        concat.extend_from_slice(c2.hash.as_bytes());
        assert_ne!(chunk_root(&[c1, c2]), blake3::hash(&concat));
    }
}
