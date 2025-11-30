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
    pub read_buffer_size: usize,
    pub chunk_queue_size: usize,
    pub hash_workers: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        let cores = num_cpus::get();
        Self {
            read_buffer_size: 256 * 1024 * 1024,
            chunk_queue_size: 2000,
            hash_workers: cores,
        }
    }
}

struct ChunkWork {
    offset: u64,
    length: usize,
}

pub fn process_file<C, H>(
    path: &Path,
    chunker_factory: impl Fn() -> C + Send + Sync + 'static,
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

    let total_size = mmap.len();
    let chunk_threads = config.hash_workers;
    let region_size = config.read_buffer_size;

    for thread_id in 0..chunk_threads {
        let mmap_clone = Arc::clone(&mmap);
        let chunk_tx = chunk_tx.clone();
        let mut chunker = chunker_factory();

        thread::spawn(move || {
            let start_offset = thread_id * region_size;

            for buffer_start in (start_offset..total_size).step_by(region_size * chunk_threads) {
                let buffer_end = (buffer_start + region_size).min(total_size);
                let buffer = &mmap_clone[buffer_start..buffer_end];

                for boundary in chunker.chunk(buffer) {
                    let chunk_offset = buffer_start as u64 + boundary.offset as u64;

                    if chunk_tx
                        .send(ChunkWork {
                            offset: chunk_offset,
                            length: boundary.length,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });
    }

    drop(chunk_tx);

    let hasher = Arc::new(hasher);
    let hash_workers = config.hash_workers;

    for _ in 0..hash_workers {
        let chunk_rx = chunk_rx.clone();
        let result_tx = result_tx.clone();
        let hasher = Arc::clone(&hasher);
        let mmap_clone = Arc::clone(&mmap);

        thread::spawn(move || {
            while let Ok(work) = chunk_rx.recv() {
                let data =
                    &mmap_clone[work.offset as usize..(work.offset + work.length as u64) as usize];
                let hash = hasher.hash(data);

                let chunk = Chunk {
                    offset: work.offset,
                    length: work.length,
                    hash,
                };

                if result_tx.send(ChunkEvent::Chunk(chunk)).is_err() {
                    return;
                }
            }
        });
    }

    drop(chunk_rx);

    thread::spawn(move || {
        drop(result_tx);
    });

    Ok(result_rx)
}
