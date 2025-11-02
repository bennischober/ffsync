use crossbeam::channel;
use memmap2::Mmap;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use super::chunker::Chunker;
use super::hasher::Hasher;
use super::types::{Chunk, ChunkEvent};

pub struct PipelineConfig {
    pub read_buffer_size: usize,  // Size of read chunks from mmap
    pub chunk_queue_size: usize,
    pub hash_workers: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            read_buffer_size: 128 * 1024 * 1024, // Process 128MB at a time
            chunk_queue_size: 1000,
            hash_workers: num_cpus::get(),
        }
    }
}

struct ChunkWork {
    offset: u64,
    data: Vec<u8>,
}

pub fn process_file<C, H>(
    path: &Path,
    mut chunker: C,
    hasher: H,
    config: &PipelineConfig,
) -> io::Result<channel::Receiver<ChunkEvent>>
where
    C: Chunker + 'static,
    H: Hasher + 'static,
{
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mmap = Arc::new(mmap);

    let (chunk_tx, chunk_rx) = channel::bounded(config.chunk_queue_size);
    let (result_tx, result_rx) = channel::bounded(config.chunk_queue_size);

    // Chunking thread (single-threaded, fast sequential access)
    let mmap_clone = Arc::clone(&mmap);
    let read_buffer_size = config.read_buffer_size;
    thread::spawn(move || {
        let mut global_offset = 0u64;
        let total_size = mmap_clone.len();

        // Process file in large chunks for better cache locality
        for buffer_start in (0..total_size).step_by(read_buffer_size) {
            let buffer_end = (buffer_start + read_buffer_size).min(total_size);
            let buffer = &mmap_clone[buffer_start..buffer_end];

            // Chunk this buffer
            for boundary in chunker.chunk(buffer) {
                let chunk_offset = global_offset + boundary.offset as u64;
                let chunk_data = buffer[boundary.offset..boundary.offset + boundary.length].to_vec();

                if chunk_tx.send(ChunkWork {
                    offset: chunk_offset,
                    data: chunk_data,
                }).is_err() {
                    return; // Receiver dropped
                }
            }

            global_offset += buffer.len() as u64;
        }

        drop(chunk_tx); // Signal completion
    });

    // Parallel hashing workers
    let hasher = Arc::new(hasher);
    let hash_workers = config.hash_workers;

    for _ in 0..hash_workers {
        let chunk_rx = chunk_rx.clone();
        let result_tx = result_tx.clone();
        let hasher = Arc::clone(&hasher);

        thread::spawn(move || {
            while let Ok(work) = chunk_rx.recv() {
                let hash = hasher.hash(&work.data);

                let chunk = Chunk {
                    offset: work.offset,
                    length: work.data.len(),
                    hash,
                };

                if result_tx.send(ChunkEvent::Chunk(chunk)).is_err() {
                    break;
                }
            }
        });
    }

    // Drop original senders so receivers know when done
    drop(chunk_rx);

    // Completion monitor thread
    thread::spawn(move || {
        drop(result_tx);
    });

    Ok(result_rx)
}
