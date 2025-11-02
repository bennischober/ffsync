use blake3::Hash;
use fastcdc::v2020::AsyncStreamCDC;
use futures::StreamExt;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use std::io;

use super::types::{Chunk, ChunkEvent};

pub struct ChunkConfig {
    pub min_size: u32,
    pub avg_size: u32,
    pub max_size: u32,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            min_size: 256 * 1024,      // 256 KB
            avg_size: 1024 * 1024,     // 1 MB
            max_size: 4 * 1024 * 1024, // 4 MB
        }
    }
}

pub async fn chunk_and_hash<R>(
    reader: R,
    config: ChunkConfig,
    buffer_size: usize,
) -> io::Result<mpsc::Receiver<ChunkEvent>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(buffer_size);

    tokio::spawn(async move {
        let mut chunker = AsyncStreamCDC::new(
            reader,
            config.min_size,
            config.avg_size,
            config.max_size,
        );

        let mut offset = 0u64;
        let stream = chunker.as_stream();
        tokio::pin!(stream);

        while let Some(result) = stream.next().await {
            match result {
                Ok(chunk_data) => {
                    let length = chunk_data.data.len(); // Access .data field
                    let data = chunk_data.data; // Extract the Vec<u8>

                    let hash = tokio::task::spawn_blocking(move || {
                        hash_chunk(&data)
                    })
                        .await
                        .expect("Hash task panicked");

                    let chunk = Chunk {
                        offset,
                        length,
                        hash,
                    };

                    if tx.send(ChunkEvent::Chunk(chunk)).await.is_err() {
                        break;
                    }

                    offset += length as u64;
                }
                Err(e) => {
                    eprintln!("Chunking error: {}", e);
                    break;
                }
            }
        }

        let _ = tx.send(ChunkEvent::Done).await;
    });

    Ok(rx)
}

fn hash_chunk(data: &[u8]) -> Hash {
    blake3::hash(data)
}
