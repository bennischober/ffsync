/// FastCDC lower bound of the default configuration.
pub const CHUNK_MIN: usize = 64 * 1024;
/// FastCDC average target of the default configuration.
pub const CHUNK_AVG: usize = 256 * 1024;
/// FastCDC upper bound of the default configuration.
pub const CHUNK_MAX: usize = 1024 * 1024;

/// Trait for different chunking strategies
pub trait Chunker: Send {
    fn chunk<'a>(&'a mut self, data: &'a [u8]) -> Box<dyn Iterator<Item = ChunkBoundary> + 'a>;
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkBoundary {
    pub offset: usize,
    pub length: usize,
}

/// Fixed-size chunking (for comparison/benchmarking)
pub struct FixedChunker {
    chunk_size: usize,
}

impl FixedChunker {
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }
}

impl Chunker for FixedChunker {
    fn chunk<'a>(&'a mut self, data: &'a [u8]) -> Box<dyn Iterator<Item = ChunkBoundary> + 'a> {
        let chunk_size = self.chunk_size;
        Box::new(data.chunks(chunk_size).scan(0, move |offset, chunk| {
            let boundary = ChunkBoundary {
                offset: *offset,
                length: chunk.len(),
            };
            *offset += chunk.len();
            Some(boundary)
        }))
    }
}

/// FastCDC-based chunking (content-defined)
pub struct FastCDCChunker {
    min_size: usize,
    avg_size: usize,
    max_size: usize,
}

impl FastCDCChunker {
    /// Creates a FastCDC chunker. All three sizes must be even (fastcdc 5.0
    /// scans in two-byte steps and asserts this; odd values would silently
    /// shift boundaries by one byte).
    pub fn new(min_size: usize, avg_size: usize, max_size: usize) -> Self {
        debug_assert!(
            min_size.is_multiple_of(2) && avg_size.is_multiple_of(2) && max_size.is_multiple_of(2),
            "fastcdc requires even chunk sizes"
        );
        Self {
            min_size,
            avg_size,
            max_size,
        }
    }
}

impl Chunker for FastCDCChunker {
    fn chunk<'a>(&'a mut self, data: &'a [u8]) -> Box<dyn Iterator<Item = ChunkBoundary> + 'a> {
        Box::new(
            fastcdc::v2020::FastCDC::new(data, self.min_size, self.avg_size, self.max_size).map(
                |entry| ChunkBoundary {
                    offset: entry.offset,
                    length: entry.length,
                },
            ),
        )
    }
}
