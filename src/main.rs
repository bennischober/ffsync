mod chunking;

use tokio::fs::{File, OpenOptions};
use chunking::{chunk_and_hash, ChunkConfig, ChunkEvent};

fn create_large_file() -> std::io::Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;
    use rand::Rng;

    let mut rng = rand::rng();

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open("large_file.bin")?;

    let file_size = 10 * 1024 * 1024;
    let data: Vec<u8> = (0..file_size).map(|_| rng.random()).collect();

    file.write_all(&data)?;

    let mut partial_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open("partial_file.bin")?;

    partial_file.write_all(&data[..5 * 1024 * 1024])?;
    Ok(())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // let result = create_large_file();

    let file = File::open("large_file.bin").await?;

    let config = ChunkConfig::default();
    let start_time = tokio::time::Instant::now();
    let mut rx = chunk_and_hash(file, config, 100).await?;

    while let Some(event) = rx.recv().await {
        match event {
            ChunkEvent::Chunk(chunk) => {
                // println!("Chunk at offset {}: {} bytes, hash: {:?}",
                //          chunk.offset, chunk.length, chunk.hash);
            }
            ChunkEvent::Done => {
                let duration = start_time.elapsed();
                println!("Chunking finished in: {:?}", duration);
                break;
            }
        }
    }

    Ok(())
}
