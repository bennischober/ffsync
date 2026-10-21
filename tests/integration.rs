#[cfg(test)]
mod tests {
    use ffsync::chunking::{Blake3Hasher, ChunkEvent, FixedChunker, PipelineConfig, process_file};
    use ffsync::reconstruction::{ChunkSource, ReconstructionTask, Reconstructor};
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    fn create_dummy_file(path: &str, size: usize, pattern: u8) {
        let mut f = File::create(path).unwrap();
        let data = vec![pattern; size];
        f.write_all(&data).unwrap();
    }

    #[test]
    fn test_reconstruction() {
        let v1_path = "test_v1.bin";
        let v2_path = "test_v2.bin";
        let rec_path = "test_v2_reconstructed.bin";

        // 1. Create Source File (V1) - 10MB of 'A's
        create_dummy_file(v1_path, 10 * 1024 * 1024, b'A');

        // 2. Create Target File (V2) - 10MB, but middle 1MB is 'B's (Modification)
        let mut v2_data = vec![b'A'; 10 * 1024 * 1024];
        for i in 4 * 1024 * 1024..5 * 1024 * 1024 {
            v2_data[i] = b'B';
        }
        {
            let mut f = File::create(v2_path).unwrap();
            f.write_all(&v2_data).unwrap();
        }

        // 3. Scan V2 to get the "Target" state (what we want to end up with)
        // In real life, the source sends us this list of chunks.
        let config = PipelineConfig::default();
        let hasher = Blake3Hasher;
        let chunk_rx = process_file(
            Path::new(v2_path),
            || FixedChunker::new(1024 * 1024), // 1MB chunks
            hasher,
            &config,
        )
        .unwrap();

        // Collect all chunks from V2
        let mut target_chunks = Vec::new();
        while let Ok(event) = chunk_rx.recv() {
            if let ChunkEvent::Chunk(c) = event {
                target_chunks.push(c);
            }
        }

        // Calculate the expected root hash of V2
        let v2_hash = blake3::hash(&v2_data);

        // 4. Initialize Reconstructor
        // We are trying to build `rec_path` using `v1_path` as our local cache
        let mut reconstructor = Reconstructor::new(
            Path::new(rec_path),
            Path::new(v1_path),
            v2_data.len() as u64,
        )
        .unwrap();

        // 5. Execute Plan
        // "A" chunks are local (in V1), "B" chunks are network (new data)
        let mut scratch = Vec::with_capacity(1024 * 1024);

        for chunk in target_chunks {
            // Determine if we have this chunk in V1 (Simulation logic)
            // In V1, all data is 'A'.
            // If the chunk hash corresponds to a block of 'A's, we use local.
            // If it corresponds to 'B's, we simulate network.

            // Hash of a 1MB block of 'A's
            let hash_a = blake3::hash(&vec![b'A'; chunk.length]);

            let task = if chunk.hash == hash_a {
                // We found it locally!
                // (In this simple test, offset in V1 is same as V2 for 'A' parts)
                ReconstructionTask {
                    target_offset: chunk.offset,
                    source: ChunkSource {
                        offset: chunk.offset,
                        length: chunk.length,
                        is_local: true,
                    },
                }
            } else {
                // Must be the 'B' block. Simulate network download.
                ReconstructionTask {
                    target_offset: chunk.offset,
                    source: ChunkSource {
                        offset: 0,
                        length: chunk.length,
                        is_local: false,
                    },
                }
            };

            // Execute write
            let network_data = if !task.source.is_local {
                Some(&v2_data[chunk.offset as usize..(chunk.offset as usize + chunk.length)])
            } else {
                None
            };

            reconstructor
                .write_chunk(&task, &mut scratch, network_data)
                .unwrap();
        }

        // 6. Finalize and Verify
        let hasher_trait = Blake3Hasher;
        reconstructor
            .finalize(&hasher_trait, v2_hash, Path::new(rec_path))
            .unwrap();

        // 7. Cleanup
        std::fs::remove_file(v1_path).ok();
        std::fs::remove_file(v2_path).ok();
        std::fs::remove_file(rec_path).ok();
    }
}
