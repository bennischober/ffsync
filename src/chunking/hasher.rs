use blake3::Hash;

/// Trait for different hashing strategies
pub trait Hasher: Send + Sync {
    fn hash(&self, data: &[u8]) -> Hash;
}

/// BLAKE3 hasher
pub struct Blake3Hasher;

impl Hasher for Blake3Hasher {
    fn hash(&self, data: &[u8]) -> Hash {
        blake3::hash(data)
    }
}
