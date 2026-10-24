use crate::chunking::Hasher;
use crate::reconstruction::io_ext::FileExt;
use blake3::Hash;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct ChunkSource {
    pub offset: u64, // Where in the SOURCE to read from
    pub length: usize,
}

pub struct ReconstructionTask {
    pub target_offset: u64, // Where to write in the NEW temp file
    pub source: ChunkSource,
}

pub struct Reconstructor {
    temp_file: File,
    local_source_path: PathBuf,
    /// Lazily opened read handle on `local_source_path` (D7): copying many
    /// local chunks must not re-open the file per chunk (tens of thousands
    /// of opens on large files). `RefCell` keeps the public methods
    /// `&self`-compatible; a Reconstructor is used from one thread per
    /// session, so `Sync` is not required.
    source_file: std::cell::RefCell<Option<File>>,
}

impl Reconstructor {
    /// Creates a new reconstructor.
    /// Opens the temporary file and pre-allocates it (Sparse file).
    pub fn new(target_path: &Path, local_source_path: &Path, final_size: u64) -> io::Result<Self> {
        let temp_path = target_path.with_extension("ffsync-tmp");

        let file = OpenOptions::new()
            .read(true) // Needed for final verification
            .write(true)
            .create(true)
            .truncate(true) // Overwrite if partial exists (for now)
            .open(&temp_path)?;

        // Pre-allocate file size. This is crucial for performance and ensuring
        // we don't run out of disk space halfway through.
        file.set_len(final_size)?;

        Ok(Self {
            temp_file: file,
            local_source_path: local_source_path.to_path_buf(),
            source_file: std::cell::RefCell::new(None),
        })
    }

    /// Opens an existing temp file for resume (D10): no truncation, no
    /// creation. The caller must have verified the temp file exists; its
    /// content is the resume state. Size is ensured (extends with zeros if
    /// a crash happened mid-pre-allocation; a no-op otherwise).
    pub fn open_existing(
        target_path: &Path,
        local_source_path: &Path,
        final_size: u64,
    ) -> io::Result<Self> {
        let temp_path = target_path.with_extension("ffsync-tmp");
        let file = OpenOptions::new().read(true).write(true).open(&temp_path)?;
        if file.metadata()?.len() < final_size {
            file.set_len(final_size)?;
        }
        Ok(Self {
            temp_file: file,
            local_source_path: local_source_path.to_path_buf(),
            source_file: std::cell::RefCell::new(None),
        })
    }

    /// The temp file path for a given target path (single source of truth
    /// for the naming convention).
    pub fn temp_path_for(target_path: &Path) -> PathBuf {
        target_path.with_extension("ffsync-tmp")
    }

    /// Durability (§8.3): flush the temp file's data before the atomic
    /// rename. The rename is journaled, the data pages are not — without
    /// this a crash between rename and page-cache flush can leave the
    /// target renamed-but-empty on some filesystems. Cost is one flush of
    /// already-written pages.
    pub fn sync_temp(&self) -> io::Result<()> {
        self.temp_file.sync_all()
    }

    /// Process a single chunk whose bytes are already in memory.
    ///
    /// This is the path the sync state machines use: the receiver has
    /// either decompressed network data or verified local bytes in a
    /// buffer, and only needs them placed at `task.target_offset`.
    pub fn write_chunk(&self, task: &ReconstructionTask, data: &[u8]) -> io::Result<()> {
        self.temp_file.write_at(data, task.target_offset)?;
        Ok(())
    }

    /// Process a single chunk by copying it from the local source file.
    ///
    /// The bytes are read through `scratch_buffer` (resized as needed) so
    /// repeated calls do not reallocate. The source file handle is opened
    /// once and cached (D7) — it is read-only, so the cached handle never
    /// conflicts with the temp file's write handle on either platform.
    /// Bench and integration tests use this to simulate the locals-first
    /// write order without a peer; the protocol itself resolves local
    /// chunks itself because it must verify each chunk's hash before
    /// writing (D12).
    pub fn copy_local_chunk(
        &self,
        task: &ReconstructionTask,
        scratch_buffer: &mut Vec<u8>,
    ) -> io::Result<()> {
        if self.source_file.borrow().is_none() {
            *self.source_file.borrow_mut() = Some(File::open(&self.local_source_path)?);
        }
        if scratch_buffer.len() < task.source.length {
            scratch_buffer.resize(task.source.length, 0);
        }
        let slice = &mut scratch_buffer[0..task.source.length];
        {
            let mut guard = self.source_file.borrow_mut();
            let source_file = guard.as_mut().expect("source handle was just cached above");
            source_file.read_at(slice, task.source.offset)?;
        }
        self.temp_file.write_at(slice, task.target_offset)?;
        Ok(())
    }

    /// Finalize: verify the reconstructed file's chunk-tree root and rename.
    ///
    /// `expected_root` is the sender's `chunk_root` over its ordered chunk
    /// list. Verifying requires re-scanning the temp file with the same CDC
    /// parameters and computing the root over that chunk list — the chunk
    /// tree is *the* verification identity (see `chunking::chunk_root`).
    pub fn finalize_chunk_root(
        self,
        expected_root: Hash,
        final_path: &Path,
        pipeline: &crate::chunking::PipelineConfig,
        cdc: (usize, usize, usize),
    ) -> io::Result<()> {
        use crate::chunking::{Blake3Hasher, ChunkEvent, FastCDCChunker, process_file};

        let temp_path = final_path.with_extension("ffsync-tmp");
        let rx = process_file(
            &temp_path,
            move || FastCDCChunker::new(cdc.0, cdc.1, cdc.2),
            Blake3Hasher,
            pipeline,
        )?;
        let mut chunks = Vec::new();
        while let Ok(event) = rx.recv() {
            if let ChunkEvent::Chunk(c) = event {
                chunks.push(c);
            }
        }
        // Canonical order (offsets are unique; hash workers race).
        chunks.sort_by_key(|c| c.offset);
        let actual_root = crate::chunking::chunk_root(&chunks);

        if actual_root != expected_root {
            let _ = std::fs::remove_file(&temp_path);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Reconstructed chunk-root mismatch",
            ));
        }

        // Durability (§8.3): flush before the rename.
        self.temp_file.sync_all()?;
        drop(self.temp_file);
        std::fs::rename(temp_path, final_path)?;
        Ok(())
    }

    /// Legacy finalize using the standard BLAKE3 file hash.
    pub fn finalize<H: Hasher>(
        mut self,
        hasher_impl: &H,
        expected_hash: Hash,
        final_path: &Path,
    ) -> io::Result<()> {
        self.temp_file.seek(SeekFrom::Start(0))?;

        // This keeps the abstraction clean. The Reconstructor doesn't need to know
        // *how* to hash, just that it needs to hash this file handle.
        let final_hash = hasher_impl.hash_reader(&mut self.temp_file)?;

        if final_hash != expected_hash {
            // Explicitly delete the corrupted temp file
            // (Use metadata to get the path, or reconstruct it)
            let _ = std::fs::remove_file(final_path.with_extension("ffsync-tmp"));
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Reconstructed hash mismatch",
            ));
        }

        // Durability (§8.3): flush before the rename.
        self.temp_file.sync_all()?;
        drop(self.temp_file);

        std::fs::rename(final_path.with_extension("ffsync-tmp"), final_path)?;

        Ok(())
    }
}
