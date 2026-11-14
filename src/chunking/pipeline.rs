use crossbeam::channel;
use memmap2::Mmap;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use super::chunker::{Chunker, FastCDCChunker};
use super::hasher::{Blake3Hasher, Hasher};
use super::types::{Chunk, ChunkEvent};

/// Size of the regions the pipeline chunks independently (one chunker per
/// region, regions striped across the chunker threads). The chunker
/// restarts at every region start, so a boundary is forced at each
/// multiple of this size: it is part of the chunk-layout definition, like
/// the CDC parameters, and both peers must use the same value
/// (`concept/Protocol.md` §4, "Region seams"). It is a protocol constant,
/// never negotiated and never configurable.
pub const REGION_SIZE: usize = 256 * 1024 * 1024;

/// Bytes of file per hash worker below which another worker is not worth
/// spawning: BLAKE3 hashes several hundred MiB/s per thread, so a thread
/// over less than this spends a large share of its life being created.
const BYTES_PER_HASH_WORKER: usize = 4 * 1024 * 1024;

/// Pipeline concurrency settings. Only concurrency lives here: nothing in
/// this struct changes the chunk layout.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Ignored. The region size changes where chunks are cut, so it is the
    /// fixed protocol constant [`REGION_SIZE`]; setting a different value
    /// here used to produce chunk lists no peer could match.
    #[deprecated(
        since = "1.4.0",
        note = "ignored: the region size is the protocol constant REGION_SIZE"
    )]
    pub read_buffer_size: usize,
    /// Capacity of the bounded work and result channels.
    pub chunk_queue_size: usize,
    /// Upper bound on hash workers (and on chunker threads) per scan. A
    /// scan uses fewer when the file is small; see [`process_file`].
    pub hash_workers: usize,
}

impl Default for PipelineConfig {
    #[allow(deprecated)] // the field still has to be initialized
    fn default() -> Self {
        let cores = num_cpus::get();
        Self {
            read_buffer_size: REGION_SIZE,
            chunk_queue_size: 2000,
            hash_workers: cores,
        }
    }
}

/// Threads one scan of a `total_size`-byte file uses: `(chunker threads,
/// hash workers)`. Chunker threads beyond the region count would exit
/// without work, and hash workers beyond one per
/// [`BYTES_PER_HASH_WORKER`] cost more to spawn than they save, which
/// added up in tree sessions (every small file paid for 2 × cores
/// threads). The counts never affect the chunk layout: regions are fixed.
fn thread_counts(total_size: usize, max_workers: usize) -> (usize, usize) {
    let max_workers = max_workers.max(1);
    let regions = total_size.div_ceil(REGION_SIZE).max(1);
    let hash = total_size.div_ceil(BYTES_PER_HASH_WORKER).max(1);
    (regions.min(max_workers), hash.min(max_workers))
}

struct ChunkWork {
    offset: u64,
    length: usize,
}

/// Scans a file through the pipeline and returns all chunks in canonical
/// file order (sorted by offset).
///
/// Hash workers complete chunks concurrently, so the channel's arrival
/// order is worker-completion order, not file order. Every consumer that
/// needs a deterministic chunk list (chunk-tree roots, delta diffing,
/// reconstruction plans) must use this ordering. Offsets are unique because
/// chunks partition the file.
pub fn scan_file_ordered(
    path: &Path,
    config: &PipelineConfig,
    cdc: (usize, usize, usize),
) -> io::Result<(u64, Vec<Chunk>)> {
    let (min, avg, max) = cdc;
    let rx = process_file(
        path,
        move || FastCDCChunker::new(min, avg, max),
        Blake3Hasher,
        config,
    )?;
    let mut hashed: u64 = 0;
    let mut chunks = Vec::new();
    // Disconnect (all hash workers finished) ends the scan.
    while let Ok(ChunkEvent::Chunk(chunk)) = rx.recv() {
        hashed += chunk.length as u64;
        chunks.push(chunk);
    }
    chunks.sort_by_key(|c| c.offset);
    Ok((hashed, chunks))
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
    // SAFETY: the file is opened read-only and nothing in ffsync writes it
    // for the lifetime of the mapping: the pipeline holds the only
    // reference, hash workers just slice it, and (receiving) commits go
    // through a separate temp file. Another process can still change it,
    // and unix offers no way to forbid that. A concurrent in-place write
    // only changes the bytes we hash (the receiver's final hash check then
    // fails the sync). A truncation below a page we then touch raises
    // SIGBUS on Linux and kills the process; nothing is committed and the
    // target is untouched. Windows refuses the truncation instead (os
    // error 1224, ERROR_USER_MAPPED_FILE). Both reproduced 2026-11-08.
    // This is a known limitation (M1: README, root AGENTS.md gotchas);
    // closing it means replacing the mmap with positional reads.
    let mmap = unsafe { Mmap::map(&file)? };
    let mmap = Arc::new(mmap);

    let (chunk_tx, chunk_rx) = channel::bounded(config.chunk_queue_size);
    let (result_tx, result_rx) = channel::bounded(config.chunk_queue_size);

    let total_size = mmap.len();
    let (chunk_threads, hash_workers) = thread_counts(total_size, config.hash_workers);
    let region_size = REGION_SIZE;

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
    // Only the workers' clones remain: the receiver disconnects exactly
    // when the last worker finishes (no helper thread needed).
    drop(result_tx);

    Ok(result_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_files_get_one_thread_of_each_kind() {
        assert_eq!(thread_counts(0, 16), (1, 1));
        assert_eq!(thread_counts(4096, 16), (1, 1));
        assert_eq!(thread_counts(BYTES_PER_HASH_WORKER, 16), (1, 1));
    }

    #[test]
    fn thread_counts_grow_with_size_up_to_the_cap() {
        assert_eq!(thread_counts(BYTES_PER_HASH_WORKER + 1, 16), (1, 2));
        assert_eq!(thread_counts(64 * 1024 * 1024, 16), (1, 16));
        assert_eq!(thread_counts(64 * 1024 * 1024, 4), (1, 4));
        assert_eq!(thread_counts(3 * REGION_SIZE, 16), (3, 16));
        assert_eq!(thread_counts(100 * REGION_SIZE, 16), (16, 16));
        // A zero cap still scans.
        assert_eq!(thread_counts(4096, 0), (1, 1));
    }
}
