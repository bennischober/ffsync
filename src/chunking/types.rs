use blake3::Hash;

/// Represents a chunk with its metadata
#[derive(Debug, Clone)]
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
