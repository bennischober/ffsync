use blake3::Hash;

/// Trait for different hashing strategies
pub trait Hasher: Send + Sync {
    fn hash(&self, data: &[u8]) -> Hash;
}

/// BLAKE3 hasher
pub struct Blake3Hasher;

impl Hasher for Blake3Hasher {
    fn hash(&self, data: &[u8]) -> Hash {
        if data.len() > 512 * 1024 {
            let mut hasher = blake3::Hasher::new();
            hasher.update_rayon(data);
            hasher.finalize()
        } else {
            blake3::hash(data)
        }
    }
}
