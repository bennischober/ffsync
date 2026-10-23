/// FastCDC lower bound of the default configuration.
pub const CHUNK_MIN: usize = 64 * 1024;
/// FastCDC average target of the default configuration.
pub const CHUNK_AVG: usize = 256 * 1024;
/// FastCDC upper bound of the default configuration.
pub const CHUNK_MAX: usize = 1024 * 1024;

/// Files below this size chunk with the small-file configuration (D19).
/// The size-ladder sweep (`evaluation/chunking.md`, 2026-10-23) puts the
/// crossover here: below 16 MiB the smaller config wins unambiguously
/// (rust-src 8 MiB: 18.1% → 4.1% delta; tos.webm 16 MiB: 8.6% → 3.8%), at
/// 32 MiB the difference is noise, and chunk counts stay trivial at the
/// boundary (~200 chunks ≈ 9 KiB manifest/index per file).
pub const SMALL_FILE_BOUNDARY: u64 = 16 * 1024 * 1024;
/// Small-file configuration (D19): the F1-sweep best point.
pub const SMALL_CHUNK_MIN: usize = 16 * 1024;
/// Small-file average target.
pub const SMALL_CHUNK_AVG: usize = 64 * 1024;
/// Small-file upper bound.
pub const SMALL_CHUNK_MAX: usize = 256 * 1024;

/// The CDC parameters for a file of `size` bytes (D19): a pure function of
/// the size, evaluated by the sender before scanning and sent in HELLO
/// (`cdc_min/cdc_avg/cdc_max`) — the receiver honors the wire values, so
/// this policy only decides what the sender sends. Files at or above the
/// boundary chunk exactly as the shipped defaults always have, which keeps
/// every existing large-file index valid.
pub fn cdc_params_for_size(size: u64) -> (usize, usize, usize) {
    if size < SMALL_FILE_BOUNDARY {
        (SMALL_CHUNK_MIN, SMALL_CHUNK_AVG, SMALL_CHUNK_MAX)
    } else {
        (CHUNK_MIN, CHUNK_AVG, CHUNK_MAX)
    }
}

/// Sanity-checks peer-supplied CDC parameters (HELLO): usable only when
/// ordered and non-degenerate. Nonsense values fall back to `fallback`
/// instead of trusting the wire — a hostile peer must not be able to steer
/// our chunker into a pathological configuration.
pub fn sane_cdc_params(p: (u32, u32, u32)) -> bool {
    let (min, avg, max) = (p.0 as usize, p.1 as usize, p.2 as usize);
    min > 0 && min <= avg && avg <= max && max <= 64 * 1024 * 1024
}

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
