mod chunking;
use crate::chunking::*;
use std::time::Instant;

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

    let path = std::path::Path::new("D:\\Video\\LORD OF WAR\\title_t00.mkv");
    let config = PipelineConfig::default();

    // // Test Fixed-size chunking
    // {
    //     let start = Instant::now();
    //     let hasher = Blake3Hasher;
    //     let rx = process_file(
    //         path,
    //         || FixedChunker::new(700 * 1024), // Factory closure
    //         hasher,
    //         &config
    //     )?;
    //
    //     let mut count = 0;
    //     while let Ok(event) = rx.recv() {
    //         if matches!(event, ChunkEvent::Chunk(_)) {
    //             count += 1;
    //         }
    //     }
    //
    //     println!("Fixed (700KB): {} chunks in {:?}", count, start.elapsed());
    // }

    // Test FastCDC chunking
    {
        let start = Instant::now();
        let rx = process_file(
            path,
            || FastCDCChunker::new(256 * 1024, 1024 * 1024, 4 * 1024 * 1024),
            Blake3Hasher,
            &config
        )?;

        let mut count = 0;
        while let Ok(event) = rx.recv() {
            if matches!(event, ChunkEvent::Chunk(_)) {
                count += 1;
            }
        }

        println!("FastCDC: {} chunks in {:?}", count, start.elapsed());
    }

    Ok(())
}
