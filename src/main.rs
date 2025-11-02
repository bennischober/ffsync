use blake3::{Hasher, Hash};
use rand::Rng;

fn generate_random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..size).map(|_| rng.random()).collect()
}

fn hash_data(data: &[u8]) -> Hash {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

// make chunk_size a default parameter with 1024 * 1024 bytes (1 MB)
fn create_chunks(data: &[u8], chunk_size: usize) -> Vec<&[u8]> {
    data.chunks(chunk_size).collect()
}

fn compare_hashes(hash_a: &Hash, hash_b: &Hash) -> bool {
    hash_a == hash_b
}

fn main() {
    let data_a = generate_random_data(1024 * 1024); // 1 MB of random data
    let data_b = [data_a.clone(), generate_random_data(2 * 1024 * 1024)].concat(); // File B = File A + 2 MB random data

    let max_chunk_size = 1024 * 1024; // 1 MB
    let data_a_chunks = create_chunks(&data_a, max_chunk_size);
    let data_b_chunks = create_chunks(&data_b, max_chunk_size);

    let mut unequal_chunks: Vec<usize> = Vec::new();
    let mut not_common_data: Vec<usize> = Vec::new();

    // Compare chunks that exist in both files
    let min_len = data_a_chunks.len().min(data_b_chunks.len());
    for i in 0..min_len {
        let chunk_a = &data_a_chunks[i];
        let chunk_b = &data_b_chunks[i];

        println!("Chunk {}: File A size: {}, File B size: {}", i, chunk_a.len(), chunk_b.len());

        let hash_a = hash_data(&chunk_a);
        let hash_b = hash_data(&chunk_b);

        let are_equal = compare_hashes(&hash_a, &hash_b);
        if !are_equal {
            unequal_chunks.push(i);
        }
        println!("Are hashes equal? {}", are_equal);
    }

    // Add remaining chunks from File B if it's longer
    if data_b_chunks.len() > data_a_chunks.len() {
        for i in min_len..data_b_chunks.len() {
            not_common_data.push(i);
        }
    }

    println!("Total unequal data size from File A: {}", unequal_chunks.len());
    println!("Total not common data size from File B: {}", not_common_data.len());
}
