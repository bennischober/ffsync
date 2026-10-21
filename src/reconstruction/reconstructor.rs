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
    pub is_local: bool, // True = Local File, False = Payload provided
}

pub struct ReconstructionTask {
    pub target_offset: u64, // Where to write in the NEW temp file
    pub source: ChunkSource,
}

pub struct Reconstructor {
    temp_file: File,
    local_source_path: PathBuf,
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
        })
    }

    /// Process a single chunk.
    ///
    /// `scratch_buffer`: A mutable buffer provided by the caller to avoid allocations.
    /// `network_data`: If the source is remote, this contains the bytes.
    pub fn write_chunk(
        &self,
        task: &ReconstructionTask,
        scratch_buffer: &mut Vec<u8>,
        network_data: Option<&[u8]>,
    ) -> io::Result<()> {
        let data_to_write = if task.source.is_local {
            // CASE A: Local Copy (Source -> Scratch -> Dest)
            // We open a fresh handle or use FileExt to read safely
            // Note: Opening a file handle is relatively cheap, but for millions of chunks
            // we might want to keep a thread-local handle open. For now, let's open it
            // here to ensure 100% thread safety without `Send` issues on the struct.
            let source_file = File::open(&self.local_source_path)?;

            // Resize scratch buffer to fit data
            if scratch_buffer.len() < task.source.length {
                scratch_buffer.resize(task.source.length, 0);
            }
            let slice = &mut scratch_buffer[0..task.source.length];

            // Atomic read from offset
            source_file.read_at(slice, task.source.offset)?;
            slice
        } else {
            // CASE B: Network Data (Network Buffer -> Dest)
            network_data.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "Network chunk missing data")
            })?
        };

        // Atomic write to offset
        self.temp_file.write_at(data_to_write, task.target_offset)?;

        Ok(())
    }

    /// Finalize: Hash the result, verify, and rename.
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

        drop(self.temp_file);

        std::fs::rename(final_path.with_extension("ffsync-tmp"), final_path)?;

        Ok(())
    }
}
