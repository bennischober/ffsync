use blake3::Hash;
use std::io;

/// Trait for different hashing strategies
pub trait Hasher: Send + Sync {
    /// Hash a slice of bytes in memory
    fn hash(&self, data: &[u8]) -> Hash;

    /// Hash a stream of bytes from a reader
    fn hash_reader(&self, reader: &mut dyn io::Read) -> io::Result<Hash>;
}

/// BLAKE3 hasher
pub struct Blake3Hasher;

impl Hasher for Blake3Hasher {
    fn hash(&self, data: &[u8]) -> Hash {
        blake3::hash(data)
    }

    fn hash_reader(&self, reader: &mut dyn io::Read) -> io::Result<Hash> {
        let mut hasher = blake3::Hasher::new();
        // std::io::copy is highly optimized; it uses a specialized buffer
        // and pipes the reader directly into the writer (the hasher).
        io::copy(reader, &mut hasher)?;
        Ok(hasher.finalize())
    }
}
