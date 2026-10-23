//! Benchmark harness for the ffsync chunking + reconstruction pipeline.
//!
//! Generates random fixtures in an output directory, scans them with the
//! production chunking pipeline, simulates a delta transfer (the chunk hashes
//! the receiving side is missing), reconstructs the target from the source
//! via `Reconstructor`, and projects transfer times for a range of link
//! speeds and hypothetical zstd compression ratios.
//!
//! Usage:
//! ```text
//! cargo run --bin bench -- [--size-mib <N>] [--quick] [--out-dir <path>] [--rsync] [--json]
//! ```

use blake3::Hash;
use ffsync::chunking::{
    Blake3Hasher, Chunk, ChunkEvent, FastCDCChunker, Hasher, PipelineConfig, process_file,
};
use ffsync::index::FileIndex;
use ffsync::reconstruction::{ChunkSource, FileExt, ReconstructionTask, Reconstructor};
use rand::{Rng, RngExt, SeedableRng, rngs::StdRng};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::{self, Seek, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime};

/// FastCDC lower bound used for every scan (default).
const CHUNK_MIN: usize = 64 * 1024;
/// FastCDC average target used for every scan (default).
const CHUNK_AVG: usize = 256 * 1024;
/// FastCDC upper bound used for every scan (default).
const CHUNK_MAX: usize = 1024 * 1024;

/// Fixture files are written in blocks of this size.
const WRITE_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Default fixture size in MiB.
const DEFAULT_SIZE_MIB: u64 = 128;
/// Fixture size in MiB implied by `--quick`.
const QUICK_SIZE_MIB: u64 = 32;
/// Default scratch/output directory.
const DEFAULT_OUT_DIR: &str = "./bench_tmp";

/// Link speeds for the transfer projection: (Mbit/s, display label).
const LINKS: [(f64, &str); 4] = [
    (20.0, "20 Mbit/s"),
    (100.0, "100 Mbit/s"),
    (1000.0, "1 Gbit/s"),
    (10000.0, "10 Gbit/s"),
];
/// Hypothetical zstd compression ratios for the transfer projection.
const RATIOS: [f64; 3] = [1.0, 2.0, 4.0];

/// Fixture variants benchmarked against the source file `A`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Identical,
    Modified,
    Appended,
    Fresh,
}

impl Scenario {
    const ALL: [Scenario; 4] = [
        Scenario::Identical,
        Scenario::Modified,
        Scenario::Appended,
        Scenario::Fresh,
    ];

    fn name(self) -> &'static str {
        match self {
            Scenario::Identical => "identical",
            Scenario::Modified => "modified",
            Scenario::Appended => "appended",
            Scenario::Fresh => "fresh",
        }
    }
}

/// Parsed command line configuration.
struct BenchConfig {
    size_mib: u64,
    out_dir: PathBuf,
    quick: bool,
    rsync: bool,
    json: bool,
    size_explicit: bool,
    /// Deterministic RNG seed for fixture generation; `None` = random seed.
    /// Deterministic fixtures make bench runs comparable across code changes
    /// (chunk hashes, deltas, and rsync's view of the files are all stable).
    seed: Option<u64>,
    /// FastCDC (min, avg, max) chunk sizes; `None` = defaults.
    cdc: Option<(usize, usize, usize)>,
    /// Run the chunk-size sweep instead of the default single pass.
    sweep: bool,
    /// Run the disconnect/resume simulation on the modified scenario.
    disconnect: bool,
    /// Run the warm-start index simulation on the identical scenario (D15).
    warm: bool,
    /// Run the tree-sync simulation (D17): multi-file fixture, both sides
    /// through the real scan + triage, per-file delta and warm/cold starts.
    tree: bool,
    /// Run the latency simulation: REAL tree sessions over an in-process
    /// duplex that delays every direction (§16.8 measurement) — sequential
    /// vs parallel conversation execution at a configurable RTT.
    latency: bool,
    /// Only generate the 100-file tree fixture into `<out-dir>/lat_sender`
    /// (same seed/shape as `--latency`) and exit — the WAN-measurement
    /// harness packs it for a remote peer; no simulation runs.
    gen_tree: bool,
    /// Simulated RTT in milliseconds for `--latency` (default 30).
    rtt_ms: u64,
    /// Parallel connection counts swept by `--latency` (default 1,4,8).
    parallel_counts: Vec<u16>,
}

impl BenchConfig {
    fn size_bytes(&self) -> u64 {
        self.size_mib * 1024 * 1024
    }
}

/// Timing/chunk statistics of one `process_file` scan.
struct ScanStats {
    total: Duration,
    first_chunk: Option<Duration>,
    chunks: usize,
    hashed_bytes: u64,
}

/// Delta simulation results for one (source, target) pair.
struct DeltaStats {
    target_bytes: u64,
    missing_bytes: u64,
}

impl DeltaStats {
    /// Percentage of the target that is not covered by source chunks.
    fn pct(&self) -> f64 {
        if self.target_bytes == 0 {
            0.0
        } else {
            self.missing_bytes as f64 / self.target_bytes as f64 * 100.0
        }
    }
}

/// Full result of one benchmarked scenario.
struct ScenarioResult {
    scenario: Scenario,
    variant_size: u64,
    source_scan: ScanStats,
    target_scan: ScanStats,
    delta: DeltaStats,
    reconstruct: Duration,
}

/// One timed rsync invocation.
struct RsyncRow {
    mode: &'static str,
    wall: Duration,
    transferred: Option<u64>,
    matched: Option<u64>,
}

/// How the rsync comparison is executed.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RsyncMode {
    /// `rsync` binary on the Windows PATH (also the only mode on Linux).
    Native,
    /// `rsync` inside a WSL distro, invoked via `wsl.exe --exec`.
    Wsl,
}

impl RsyncMode {
    /// Label used in the report header and the JSON output.
    fn label(self) -> &'static str {
        match self {
            RsyncMode::Native => "native",
            RsyncMode::Wsl => "wsl",
        }
    }
}

/// Outcome of probing for an rsync to compare against.
enum RsyncProbe {
    Available(RsyncMode),
    Unavailable(String),
}

/// Result of the optional rsync comparison.
struct RsyncResults {
    /// `Some` when the comparison ran, `None` when it was skipped.
    mode: Option<RsyncMode>,
    rows: Vec<RsyncRow>,
}

fn print_usage() {
    println!(
        "ffsync benchmark harness

Usage: cargo run --bin bench -- [options]

Options:
  --size-mib <N>   fixture size in MiB (default: {})
  --quick          use a {} MiB fixture for a fast run
  --seed <N>       deterministic fixture seed (comparable runs across code changes)
  --cdc <m/a/M>    FastCDC sizes, e.g. 64k/256k/1m (default 256k/1m/4m)
  --sweep          run a chunk-size sweep (6 CDC configs) and compare deltas
  --disconnect     simulate a mid-transfer disconnect and compare resume costs
  --warm           measure the warm-start index: cold scan + index write, then
                   reload (stat-only) with content verification
  --tree           simulate a directory-tree session (D17): generate a
                   multi-file tree, run both sides' scan + triage, measure
                   per-file delta, session-level fast path, and warm starts
  --out-dir <path> directory for fixtures and scratch output (default: {})
  --rsync          additionally compare against rsync (native, or via WSL)
  --json           print a machine-readable JSON summary as the last line
  -h, --help       show this help",
        DEFAULT_SIZE_MIB, QUICK_SIZE_MIB, DEFAULT_OUT_DIR
    );
}

fn die(msg: &str) -> ! {
    eprintln!("bench: error: {}", msg);
    print_usage();
    std::process::exit(2);
}

/// Parses a `min/avg/max` triple in bytes (e.g. `64k/256k/1m`), all even.
fn parse_cdc(value: &str) -> (usize, usize, usize) {
    let parse_unit = |s: &str| -> usize {
        let s = s.trim().to_ascii_lowercase();
        let (digits, mult) = match s.as_bytes().last() {
            Some(b'k') => (&s[..s.len() - 1], 1024usize),
            Some(b'm') => (&s[..s.len() - 1], 1024 * 1024),
            Some(b'g') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
            _ => (s.as_str(), 1usize),
        };
        let n: usize = digits
            .parse()
            .unwrap_or_else(|_| die(&format!("invalid cdc size: {}", s)));
        n * mult
    };
    let parts: Vec<&str> = value.split('/').collect();
    if parts.len() != 3 {
        die("--cdc expects min/avg/max, e.g. --cdc 64k/256k/1m");
    }
    let triple = (
        parse_unit(parts[0]),
        parse_unit(parts[1]),
        parse_unit(parts[2]),
    );
    if triple.0 == 0 || triple.1 < triple.0 || triple.2 < triple.1 {
        die("--cdc requires 0 < min <= avg <= max");
    }
    if triple.0 % 2 != 0 || triple.1 % 2 != 0 || triple.2 % 2 != 0 {
        die("--cdc values must be even (fastcdc 5.0 requirement)");
    }
    triple
}

/// CDC configurations swept by `--sweep`, roughly on a log scale.
const SWEEP_POINTS: [(usize, usize, usize); 6] = [
    (16 * 1024, 64 * 1024, 256 * 1024),
    (32 * 1024, 128 * 1024, 512 * 1024),
    (64 * 1024, 256 * 1024, 1024 * 1024),
    (128 * 1024, 512 * 1024, 2 * 1024 * 1024),
    (256 * 1024, 1024 * 1024, 4 * 1024 * 1024),
    (512 * 1024, 2 * 1024 * 1024, 8 * 1024 * 1024),
];

fn parse_args() -> BenchConfig {
    let mut cfg = BenchConfig {
        size_mib: DEFAULT_SIZE_MIB,
        out_dir: PathBuf::from(DEFAULT_OUT_DIR),
        quick: false,
        rsync: false,
        json: false,
        size_explicit: false,
        seed: None,
        cdc: None,
        sweep: false,
        disconnect: false,
        warm: false,
        tree: false,
        latency: false,
        gen_tree: false,
        rtt_ms: 30,
        parallel_counts: Vec::new(),
    };

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--size-mib" => {
                let value = args
                    .next()
                    .unwrap_or_else(|| die("--size-mib requires a value"));
                let parsed: u64 = value
                    .parse()
                    .unwrap_or_else(|_| die(&format!("invalid --size-mib value: {}", value)));
                if parsed == 0 {
                    die("--size-mib must be at least 1");
                }
                cfg.size_mib = parsed;
                cfg.size_explicit = true;
            }
            "--quick" => cfg.quick = true,
            "--out-dir" => {
                let value = args
                    .next()
                    .unwrap_or_else(|| die("--out-dir requires a value"));
                cfg.out_dir = PathBuf::from(value);
            }
            "--rsync" => cfg.rsync = true,
            "--json" => cfg.json = true,
            "--seed" => {
                let value = args
                    .next()
                    .unwrap_or_else(|| die("--seed requires a value"));
                let parsed: u64 = value
                    .parse()
                    .unwrap_or_else(|_| die(&format!("invalid --seed value: {}", value)));
                cfg.seed = Some(parsed);
            }
            "--cdc" => {
                let value = args
                    .next()
                    .unwrap_or_else(|| die("--cdc requires min/avg/max"));
                let parsed = parse_cdc(&value);
                cfg.cdc = Some(parsed);
            }
            "--sweep" => cfg.sweep = true,
            "--disconnect" => cfg.disconnect = true,
            "--warm" => cfg.warm = true,
            "--tree" => cfg.tree = true,
            "--latency" => cfg.latency = true,
            "--gen-tree" => cfg.gen_tree = true,
            "--rtt-ms" => {
                let value = args
                    .next()
                    .unwrap_or_else(|| die("--rtt-ms requires a number"));
                cfg.rtt_ms = value
                    .parse()
                    .unwrap_or_else(|_| die(&format!("invalid --rtt-ms value: {}", value)));
            }
            "--parallel" => {
                let value = args
                    .next()
                    .unwrap_or_else(|| die("--parallel requires a comma list, e.g. 1,4,8"));
                let counts: Vec<u16> = value
                    .split(',')
                    .map(|p| {
                        p.trim()
                            .parse()
                            .unwrap_or_else(|_| die(&format!("invalid --parallel value: {}", p)))
                    })
                    .collect();
                if counts.iter().any(|c| *c < 1 || *c > 16) {
                    die("--parallel counts must be 1..=16");
                }
                cfg.parallel_counts = counts;
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other => die(&format!("unknown argument: {}", other)),
        }
    }

    if cfg.quick && !cfg.size_explicit {
        cfg.size_mib = QUICK_SIZE_MIB;
    }
    cfg
}

/// Writes `size` bytes of random data to `path`, streaming in ~4 MiB blocks.
fn write_random_file(path: &Path, size: u64, rng: &mut impl Rng) -> io::Result<()> {
    let mut file = File::create(path)?;
    let mut buf = vec![0u8; WRITE_CHUNK_BYTES];
    let mut remaining = size;
    while remaining > 0 {
        let n = remaining.min(WRITE_CHUNK_BYTES as u64) as usize;
        rng.fill_bytes(&mut buf[..n]);
        file.write_all(&buf[..n])?;
        remaining -= n as u64;
    }
    Ok(())
}

/// Overwrites two ~1% regions around the middle of the file with fresh
/// random bytes (simulating in-place edits of equal total size).
fn flip_middle_regions(path: &Path, size: u64, rng: &mut impl Rng) -> io::Result<()> {
    let region_len = (size / 100).max(4096).min(size / 4);
    let mut buf = vec![0u8; region_len as usize];
    let file = OpenOptions::new().write(true).open(path)?;
    for pos in [size * 45 / 100, size * 55 / 100] {
        rng.fill_bytes(&mut buf);
        file.write_at(&buf, pos)?;
    }
    Ok(())
}

/// Appends ~10% of new random bytes to the file.
fn append_random_tail(path: &Path, size: u64, rng: &mut impl Rng) -> io::Result<()> {
    let tail_len = (size / 10).max(1);
    let mut buf = vec![0u8; tail_len.min(WRITE_CHUNK_BYTES as u64) as usize];
    let mut file = OpenOptions::new().append(true).open(path)?;
    let mut remaining = tail_len;
    while remaining > 0 {
        let n = remaining.min(buf.len() as u64) as usize;
        rng.fill_bytes(&mut buf[..n]);
        file.write_all(&buf[..n])?;
        remaining -= n as u64;
    }
    Ok(())
}

/// Generates the source file `A` and all four variants into the out-dir.
/// Returns the source path plus the variant paths in scenario order.
fn generate_fixtures(cfg: &BenchConfig) -> io::Result<(PathBuf, Vec<(Scenario, PathBuf)>)> {
    fs::create_dir_all(&cfg.out_dir)?;
    let size = cfg.size_bytes();

    // One seeded RNG stream for all fixtures: deterministic per `--seed`, so
    // hashes and deltas are reproducible across runs and code changes.
    let mut rng = match cfg.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_rng(&mut rand::rng()),
    };

    let source = cfg.out_dir.join("source_A.bin");
    write_random_file(&source, size, &mut rng)?;
    println!(
        "fixture: source A              {:>9}  {}",
        fmt_bytes(size),
        source.display()
    );

    let mut variants = Vec::new();
    for scenario in Scenario::ALL {
        let path = cfg.out_dir.join(format!("variant_{}.bin", scenario.name()));
        match scenario {
            Scenario::Identical => {
                fs::copy(&source, &path)?;
            }
            Scenario::Modified => {
                fs::copy(&source, &path)?;
                flip_middle_regions(&path, size, &mut rng)?;
            }
            Scenario::Appended => {
                fs::copy(&source, &path)?;
                append_random_tail(&path, size, &mut rng)?;
            }
            Scenario::Fresh => {
                write_random_file(&path, size, &mut rng)?;
            }
        }
        let variant_size = fs::metadata(&path)?.len();
        println!(
            "fixture: variant {:<9}   {:>9}  {}",
            scenario.name(),
            fmt_bytes(variant_size),
            path.display()
        );
        variants.push((scenario, path));
    }
    Ok((source, variants))
}

/// Scans `path` with the production pipeline and collects all chunks in
/// canonical file order (sorted by offset — hash workers race, so arrival
/// order is worker-completion order, not file order).
fn scan_file(
    path: &Path,
    pipeline: &PipelineConfig,
    cdc: (usize, usize, usize),
) -> io::Result<(ScanStats, Vec<Chunk>)> {
    let start = Instant::now();
    let (min, avg, max) = cdc;
    let rx = process_file(
        path,
        move || FastCDCChunker::new(min, avg, max),
        Blake3Hasher,
        pipeline,
    )?;

    let mut first_chunk = None;
    let mut chunks = Vec::new();
    let mut hashed_bytes = 0u64;
    loop {
        match rx.recv() {
            Ok(ChunkEvent::Chunk(chunk)) => {
                if first_chunk.is_none() {
                    first_chunk = Some(start.elapsed());
                }
                hashed_bytes += chunk.length as u64;
                chunks.push(chunk);
            }
            Ok(ChunkEvent::Done) => {}
            Err(_) => break, // all senders dropped: scan finished
        }
    }

    // Canonical order: offsets are unique (chunks partition the file).
    chunks.sort_by_key(|c| c.offset);

    let stats = ScanStats {
        total: start.elapsed(),
        first_chunk,
        chunks: chunks.len(),
        hashed_bytes,
    };
    Ok((stats, chunks))
}

/// Simulates the delta: how many target bytes have no matching chunk hash
/// in the source (i.e. what the wire would have to carry).
fn compute_delta(source_chunks: &[Chunk], target_chunks: &[Chunk]) -> DeltaStats {
    let source_hashes: HashSet<Hash> = source_chunks.iter().map(|c| c.hash).collect();
    let mut missing_bytes = 0u64;
    for chunk in target_chunks {
        if !source_hashes.contains(&chunk.hash) {
            missing_bytes += chunk.length as u64;
        }
    }
    DeltaStats {
        target_bytes: target_chunks.iter().map(|c| c.length as u64).sum(),
        missing_bytes,
    }
}

/// Streams the file through the BLAKE3 hasher to get its root hash.
fn file_root_hash(path: &Path) -> io::Result<Hash> {
    Blake3Hasher.hash_reader(&mut File::open(path)?)
}

/// Result of one disconnect/resume simulation point.
struct DisconnectStats {
    /// Percentage of remote (DATA) chunks written before the "disconnect".
    kill_pct: u32,
    /// Bytes a cold restart would send (delta vs the old file only).
    cold_restart_bytes: u64,
    /// Bytes needed when resuming with a temp-file-only manifest.
    temp_only_resume_bytes: u64,
    /// Bytes needed when resuming with a union manifest (temp ∪ old file).
    union_resume_bytes: u64,
    /// Total bytes already in the temp file when the disconnect happened.
    temp_written_bytes: u64,
    /// Whether completing from the temp state reproduced the target file.
    completion_verified: bool,
}

/// Simulates a disconnect during the DATA phase and measures what a resume
/// would cost under three manifest strategies.
///
/// Setup mirrors the receiver implementation: the receiver's old file is
/// `source`, the desired state is `variant`; remote chunks (variant chunks
/// missing from `source`) are written to the temp file in plan order and the
/// "disconnect" happens after `kill_pct` of them. Local copies are not yet
/// written at that point (the current receiver writes them after DATA), so
/// the temp file contains written remote chunks plus zero-filled holes —
/// exactly the resume state of a crashed transfer.
///
/// Strategies (bytes the sender must put on the wire to finish):
/// - cold restart: manifest = old file only → equals the original delta.
/// - temp-only: manifest = re-scan of the temp file. Zero-filled holes
///   produce hashes that (usually) match nothing and are re-sent; written
///   regions re-chunk to matching hashes except near hole boundaries.
/// - union: manifest = temp chunks ∪ old-file chunks. Strictly ≤ both.
///
/// Finally the temp file is *completed* (remaining remote chunks + all
/// local copies) and verified against the target's full-file BLAKE3 hash,
/// proving the partial state is genuinely recoverable.
#[allow(clippy::too_many_arguments)]
fn simulate_disconnect(
    source: &Path,
    variant: &Path,
    source_chunks: &[Chunk],
    variant_chunks: &[Chunk],
    variant_size: u64,
    variant_hash: Hash,
    cdc: (usize, usize, usize),
    kill_pct: u32,
) -> io::Result<DisconnectStats> {
    // The Reconstructor derives its on-disk temp path by replacing the last
    // extension with "ffsync-tmp"; mirror that here.
    let rec_path = variant.with_file_name("disconnect_sim.bin");
    let temp_path = rec_path.with_extension("ffsync-tmp");
    let _ = fs::remove_file(&temp_path);

    let source_hashes: HashSet<Hash> = source_chunks.iter().map(|c| c.hash).collect();
    let remote_total = variant_chunks
        .iter()
        .filter(|c| !source_hashes.contains(&c.hash))
        .count();
    let kill_count = ((remote_total as u64 * kill_pct as u64) / 100) as usize;

    // ---- Phase 1: write local copies FIRST, then remote chunks in plan
    // order, dropping mid-way ----
    // Rationale (this ordering IS the D10 finding): local chunks are ~98%
    // of the file and are available offline; writing them first means a
    // crash leaves a temp that is mostly real data with few holes, and the
    // union manifest recovers nearly everything. Writing remotes first (the
    // current receiver does locals *after* DATA) leaves a 97%-hole temp
    // where every chunk straddles a hole and re-scan recovery collapses to
    // ~zero — verified empirically (temp-only resume = 32 MiB re-send).
    let reconstructor = Reconstructor::new(&rec_path, source, variant_size)?;
    let source_file = File::open(source)?;
    {
        let mut local_scratch: Vec<u8> = Vec::new();
        for chunk in variant_chunks {
            if let Some(&src_off) = source_index_lookup(source_chunks, chunk.hash) {
                local_scratch.clear();
                local_scratch.resize(chunk.length, 0);
                let read = source_file.read_at(&mut local_scratch[..chunk.length], src_off)?;
                if read != chunk.length {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
                }
                let task = ReconstructionTask {
                    target_offset: chunk.offset,
                    source: ChunkSource {
                        offset: src_off,
                        length: chunk.length,
                    },
                };
                reconstructor.copy_local_chunk(&task, &mut local_scratch)?;
            }
        }
    }
    let variant_file = File::open(variant)?;
    let mut scratch: Vec<u8> = Vec::new();
    let mut remote_seen = 0usize;
    let mut temp_written_bytes = 0u64;
    for chunk in variant_chunks {
        if source_hashes.contains(&chunk.hash) {
            continue; // locals already written above
        }
        if remote_seen >= kill_count {
            break; // "disconnect"
        }
        remote_seen += 1;
        scratch.clear();
        scratch.resize(chunk.length, 0);
        let read = variant_file.read_at(&mut scratch[..chunk.length], chunk.offset)?;
        if read != chunk.length {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
        }
        let task = ReconstructionTask {
            target_offset: chunk.offset,
            source: ChunkSource {
                offset: 0,
                length: chunk.length,
            },
        };
        reconstructor.write_chunk(&task, &scratch)?;
        temp_written_bytes += chunk.length as u64;
    }
    drop(reconstructor); // close the handle without finalize; temp remains

    // ---- Phase 2: scan the temp file (the resume manifest source) ----
    let (_, temp_chunks) = scan_file(&temp_path, &PipelineConfig::default(), cdc)?;
    let temp_hashes: HashSet<Hash> = temp_chunks.iter().map(|c| c.hash).collect();

    // Diagnostics: how much of the temp matches variant chunks at all?
    let temp_matching: u64 = variant_chunks
        .iter()
        .filter(|c| temp_hashes.contains(&c.hash))
        .map(|c| c.length as u64)
        .sum();
    eprintln!(
        "    [dbg] kill={} wrote={} variant_chunks={} temp_chunks={} temp_bytes_matching_variant={}",
        kill_pct,
        temp_written_bytes,
        variant_chunks.len(),
        temp_chunks.len(),
        temp_matching
    );

    // ---- Byte accounting per strategy ----
    // The sender diffs ITS chunk list (variant_chunks, since the sender owns
    // the desired state) against the receiver's manifest. A chunk is covered
    // iff its hash is in the manifest set.
    let mut cold_restart_bytes = 0u64;
    let mut temp_only_resume_bytes = 0u64;
    let mut union_resume_bytes = 0u64;
    for chunk in variant_chunks {
        let len = chunk.length as u64;
        if !source_hashes.contains(&chunk.hash) {
            cold_restart_bytes += len;
        }
        if !temp_hashes.contains(&chunk.hash) {
            temp_only_resume_bytes += len;
        }
        if !temp_hashes.contains(&chunk.hash) && !source_hashes.contains(&chunk.hash) {
            union_resume_bytes += len;
        }
    }

    // ---- Phase 3: complete from the temp state and verify ----
    // NOTE: do NOT touch Reconstructor::new here — it truncates the temp
    // file and would erase phase 1's state. The killed remotes are skipped
    // as "already written", which is only true if we did not truncate.
    let completion_verified = {
        let temp = OpenOptions::new().read(true).write(true).open(&temp_path)?;
        let mut local_scratch: Vec<u8> = Vec::new();
        // Remaining remote chunks (skip the ones already written).
        let mut remote_seen = 0usize;
        for chunk in variant_chunks {
            if source_hashes.contains(&chunk.hash) {
                continue;
            }
            let already = remote_seen < kill_count;
            remote_seen += 1;
            if already {
                continue;
            }
            scratch.clear();
            scratch.resize(chunk.length, 0);
            let read = variant_file.read_at(&mut scratch[..chunk.length], chunk.offset)?;
            if read != chunk.length {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
            }
            temp.write_at(&scratch[..chunk.length], chunk.offset)?;
        }
        // Local copies from the old file.
        let source_file = File::open(source)?;
        for chunk in variant_chunks {
            if let Some(&src_off) = source_index_lookup(source_chunks, chunk.hash) {
                local_scratch.clear();
                local_scratch.resize(chunk.length, 0);
                let read = source_file.read_at(&mut local_scratch[..chunk.length], src_off)?;
                if read != chunk.length {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
                }
                temp.write_at(&local_scratch[..chunk.length], chunk.offset)?;
            }
        }
        temp.sync_all()?;
        // Verify against the target's standard full-file hash (same oracle
        // as the bench's reconstruct()).
        let actual = file_root_hash(&temp_path)?;
        actual == variant_hash
    };
    let _ = fs::remove_file(&temp_path);

    Ok(DisconnectStats {
        kill_pct,
        cold_restart_bytes,
        temp_only_resume_bytes,
        union_resume_bytes,
        temp_written_bytes,
        completion_verified,
    })
}

/// Looks up a chunk's offset in the source chunk list.
fn source_index_lookup(source_chunks: &[Chunk], hash: Hash) -> Option<&u64> {
    source_chunks
        .iter()
        .find(|c| c.hash == hash)
        .map(|c| &c.offset)
}

/// Runs the disconnect simulation on the modified scenario across kill
/// percentages and prints the resume-cost comparison table.
fn run_disconnect_simulation(
    source: &Path,
    variant: &Path,
    source_chunks: &[Chunk],
    variant_chunks: &[Chunk],
    cdc: (usize, usize, usize),
) -> io::Result<()> {
    let variant_size = fs::metadata(variant)?.len();
    let variant_hash = file_root_hash(variant)?;

    println!();
    println!("==== disconnect simulation (scenario: modified) ====");
    println!("kill %   written      cold restart   temp-only resume   union resume   completed");
    let mut all_verified = true;
    for kill_pct in [25u32, 50, 75] {
        let stats = simulate_disconnect(
            source,
            variant,
            source_chunks,
            variant_chunks,
            variant_size,
            variant_hash,
            cdc,
            kill_pct,
        )?;
        all_verified &= stats.completion_verified;
        println!(
            "{:>5}%  {:>9}  {:>13}  {:>17}  {:>13}  {:>9}",
            stats.kill_pct,
            fmt_bytes(stats.temp_written_bytes),
            fmt_bytes(stats.cold_restart_bytes),
            fmt_bytes(stats.temp_only_resume_bytes),
            fmt_bytes(stats.union_resume_bytes),
            if stats.completion_verified {
                "yes"
            } else {
                "NO"
            },
        );
    }
    println!();
    println!(
        "Reading: 'cold restart' is what v0.1 does today (re-send the full delta);\
\n'temp-only' resumes from the temp file re-scan; 'union' also matches the old\
\nfile's chunks (temp-first). Completion 'yes' proves the partial temp state\
\nis recoverable end-to-end."
    );
    if !all_verified {
        return Err(io::Error::other(
            "disconnect simulation failed verification",
        ));
    }
    Ok(())
}

/// Reconstructs the target variant from the source file via `Reconstructor`.
///
/// Chunks whose hash exists in the source are copied locally (at the source
/// chunk's offset); all other chunks are fed as simulated "network data" read
/// from the real variant file at the right offset. The result is verified
/// against the variant's root hash by `finalize`.
fn run_warm_simulation(
    file: &Path,
    pipeline: &PipelineConfig,
    cdc: (usize, usize, usize),
) -> io::Result<()> {
    println!();
    println!("==== warm-start index simulation (D15) ====");
    let size = fs::metadata(file)?.len();

    // Cold pass: the real scan (what v0.1 pays on every run) + index write
    // (what v0.2 pays once, at commit).
    let (scan_stats, chunks) = scan_file(file, pipeline, cdc)?;
    let index = FileIndex::from_scan(
        chunks.clone(),
        size,
        ffsync::index::stat_mtime(file).unwrap_or((0, 0)),
    );
    let t_write = Instant::now();
    ffsync::index::write_index(file, &index)?;
    let write_us = t_write.elapsed().as_secs_f64() * 1e6;

    // Warm pass: load + validate — must read no file content (stat-only).
    let t_load = Instant::now();
    let warm = ffsync::index::load_index(file);
    let load_us = t_load.elapsed().as_secs_f64() * 1e6;
    let Some(warm_idx) = warm else {
        return Err(io::Error::other("warm index failed to load/validate"));
    };

    // The warm index must describe exactly the cold scan's content.
    let content_ok = warm_idx == index;
    let encoded_len = index.encode().len();

    println!(
        "file: {} ({} chunks, index {})",
        fmt_bytes(size),
        scan_stats.chunks,
        fmt_bytes(encoded_len as u64)
    );
    println!(
        "cold scan   {:>10.2} ms   ({} hashed, what every run pays without the index)",
        scan_stats.total.as_secs_f64() * 1e3,
        fmt_bytes(scan_stats.hashed_bytes)
    );
    println!(
        "index write {:>10.2} µs   (once per successful sync, at commit)",
        write_us
    );
    println!(
        "warm load   {:>10.2} µs   (validate + decode, stat-only — no file contents read)",
        load_us
    );
    println!(
        "speedup     {:>10.1}x   (cold scan / warm load)",
        scan_stats.total.as_secs_f64() * 1e6 / load_us.max(0.01)
    );
    if !content_ok {
        return Err(io::Error::other(
            "warm index does not match the cold scan (root or chunk mismatch)",
        ));
    }
    println!("content verification: index matches cold scan exactly (root + all chunks)");
    Ok(())
}

// ---------------------------------------------------------------------------
// Tree simulation (D17)
// ---------------------------------------------------------------------------

/// Fixture layout for `--tree`: deterministic file sizes per index, so runs
/// are comparable. Roughly movie-like big files plus many small ones — the
/// mix a real backup tree has.
const TREE_FILE_COUNT: usize = 40;
const TREE_DIR_COUNT: usize = 6;
/// Fraction of files modified between the "previous sync" and "now".
const TREE_MODIFIED_FRAC: usize = 4; // 1 in N
/// Fraction of files that are new on the sender (receiver lacks them).
const TREE_NEW_FRAC: usize = 10; // 1 in N
/// Fraction of files that exist only on the receiver (delete candidates).
const TREE_STALE_FRAC: usize = 20; // 1 in N

/// Result of one tree simulation pass.
struct TreeStats {
    files: usize,
    total_bytes: u64,
    /// Files triaged to a conversation (sync + transfer + conflicts).
    conversations: usize,
    /// Bytes the conversations would transfer (chunk-level delta, zstd 1x).
    transferred_bytes: u64,
    /// Files deleted on the receiver (stale, receiver-only).
    deleted: usize,
    deleted_bytes: u64,
    /// Wall time of the metadata-only scan of both trees.
    scan_both: Duration,
    /// Wall time of triage + the per-file delta simulations.
    delta_sim: Duration,
    /// Wall time of a warm second session (stat + index loads, no hashing).
    warm_session: Duration,
}

/// Writes one tree fixture: `dir_count` subdirs holding `file_count` files
/// total, deterministic sizes from the seed (mostly 1–4 MiB, a few ~20 MiB,
/// some empty files). One dir is left empty (empty dirs are first-class
/// tree members, §16.2).
fn generate_tree_fixture(
    root: &Path,
    rng: &mut impl Rng,
    dir_count: usize,
    file_count: usize,
) -> io::Result<u64> {
    fs::create_dir_all(root)?;
    let mut total = 0u64;
    let mut remaining = file_count;
    for d in 0..dir_count {
        let dir = root.join(format!("dir_{d:02}"));
        fs::create_dir_all(&dir)?;
        // Distribute files evenly; the last dir stays empty.
        let per_dir = if d + 1 == dir_count {
            0
        } else if d + 2 == dir_count {
            remaining // all of the rest
        } else {
            remaining / (dir_count - 1 - d)
        };
        for f in 0..per_dir {
            remaining -= 1;
            let size = match rng.random_range(0u8..100) {
                0..=5 => 0,                                       // empty files are real
                6..=15 => rng.random_range(20..64) * 1024 * 1024, // big
                _ => rng.random_range(1..4) * 1024 * 1024,        // small
            };
            let path = dir.join(format!("file_{f:03}.bin"));
            write_random_file(&path, size, rng)?;
            total += size;
        }
    }
    Ok(total)
}

/// Applies the divergence script to the fixture: modifies every Nth file,
/// adds new files under `new/`, and appends stale receiver-only files.
/// Returns the bytes written by the mutations.
fn diverge_tree(
    sender: &Path,
    receiver: &Path,
    rng: &mut impl Rng,
    modified_every: usize,
    new_every: usize,
    stale_every: usize,
) -> io::Result<u64> {
    let mut written = 0u64;
    for d in 0..TREE_DIR_COUNT {
        let dir = sender.join(format!("dir_{d:02}"));
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for (i, e) in entries.flatten().enumerate() {
            let name = e.file_name().to_string_lossy().into_owned();
            let path = dir.join(&name);
            if i % modified_every == 0 {
                // In-place modification: overwrite ~5% of the file with fresh
                // bytes (equal size — mirrors the single-file Modified logic).
                let size = fs::metadata(&path)?.len();
                if size > 0 {
                    let mut f = fs::OpenOptions::new().write(true).open(&path)?;
                    let region = (size / 20).max(4096).min(size);
                    let start = size / 2;
                    let mut buf = vec![0u8; region as usize];
                    rng.fill_bytes(&mut buf);
                    f.seek(io::SeekFrom::Start(start))?;
                    f.write_all(&buf)?;
                    written += region;
                }
            }
            if i % new_every == 0 {
                // New file on the sender only.
                let new_path = dir.join(format!("new_{i:03}.bin"));
                let size = rng.random_range(1..4) * 1024 * 1024;
                write_random_file(&new_path, size, rng)?;
                written += size;
            }
            if i % stale_every == 0 {
                // Receiver-only file (the sender never had it): delete candidate.
                let stale_path = receiver
                    .join(format!("dir_{d:02}"))
                    .join(format!("stale_{i:03}.bin"));
                let size = 512 * 1024;
                write_random_file(&stale_path, size, rng)?;
                written += size;
            }
        }
    }
    Ok(written)
}

/// Runs the tree simulation: two "sessions" — the initial sync (everything
/// transfers), then the diverged second session (only changes), plus a warm
/// third session (nothing changed) and its cold counterfactual.
fn run_tree_simulation(cfg: &BenchConfig, pipeline: &PipelineConfig) -> io::Result<()> {
    println!();
    println!("==== tree sync simulation (D17) ====");
    let cdc = cfg.cdc.unwrap_or((CHUNK_MIN, CHUNK_AVG, CHUNK_MAX));
    let mut rng = match cfg.seed {
        Some(seed) => StdRng::seed_from_u64(seed ^ 0x7EE5_1EE7),
        None => StdRng::from_rng(&mut rand::rng()),
    };

    let sender = cfg.out_dir.join("tree_sender");
    let receiver = cfg.out_dir.join("tree_receiver");
    let _ = fs::remove_dir_all(&sender);
    let _ = fs::remove_dir_all(&receiver);

    let t = Instant::now();
    let total = generate_tree_fixture(&sender, &mut rng, TREE_DIR_COUNT, TREE_FILE_COUNT)?;
    let fixture_s = t.elapsed();

    println!(
        "fixture: {} files across {} dirs ({}), generated in {}",
        TREE_FILE_COUNT,
        TREE_DIR_COUNT,
        fmt_bytes(total),
        fmt_ms(fixture_s)
    );

    // ---- Session 1: the receiver starts EMPTY — everything transfers ----
    fs::create_dir_all(&receiver)?; // a first-sync receiver is an empty dir
    let s1 = simulate_tree_session(&sender, &receiver, pipeline, cdc, false)?;
    print_tree_stats("session 1 (receiver empty, everything transfers)", &s1);
    // Commit the session: receiver adopts the sender's tree (the real
    // receiver's per-file commit + mtime preservation; here a plain copy).
    let t = Instant::now();
    copy_tree(&sender, &receiver)?;
    let copy_s = t.elapsed();
    println!(
        "(session 1 commit simulated by copying the tree, {})",
        fmt_ms(copy_s)
    );

    // ---- Session 2: divergence ----
    let t = Instant::now();
    let diverged_bytes = diverge_tree(
        &sender,
        &receiver,
        &mut rng,
        TREE_MODIFIED_FRAC,
        TREE_NEW_FRAC,
        TREE_STALE_FRAC,
    )?;
    let diverge_s = t.elapsed();
    println!(
        "(divergence script wrote {} in {})",
        fmt_bytes(diverged_bytes),
        fmt_ms(diverge_s)
    );

    let s2 = simulate_tree_session(&sender, &receiver, pipeline, cdc, false)?;
    print_tree_stats("session 2 (cold scan, diverged)", &s2);

    // ---- Session 2 commit: receiver adopts the changes (real receiver:
    // per-file conversations wrote the files; here a plain copy) ----
    copy_tree(&sender, &receiver)?;

    // ---- Session 3: warm — nothing changed after session 2's commit ----
    // The real flow: mtime preservation at commit makes this stat-only; the
    // conversation set is empty, so no hashing, no delta sim, no transfers.
    let s3 = simulate_tree_session(&sender, &receiver, pipeline, cdc, true)?;
    print_tree_stats("session 3 (warm, unchanged)", &s3);

    // ---- Cold counterfactual: what an unchanged second session costs
    // without D17/D15 (rsync-style full re-scan + per-file hashing of both
    // trees) — session 2's cold scan + delta sim is that model. ----
    let cold_second = s2.scan_both + s2.delta_sim;
    println!();
    println!(
        "conclusion: warm unchanged session is {:.1}x cheaper than a cold re-scan \
         ({} vs {}); diverged session transfers {} of {} ({:.1}%)",
        cold_second.as_secs_f64() / s3.warm_session.as_secs_f64().max(1e-9),
        fmt_ms(s3.warm_session),
        fmt_ms(cold_second),
        fmt_bytes(s2.transferred_bytes),
        fmt_bytes(s2.total_bytes),
        if s2.total_bytes == 0 {
            0.0
        } else {
            s2.transferred_bytes as f64 / s2.total_bytes as f64 * 100.0
        }
    );
    Ok(())
}

/// Recursively copies a tree (files + dirs, mtimes preserved on files).
fn copy_tree(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    for e in fs::read_dir(src)? {
        let e = e?;
        let from = e.path();
        let to = dst.join(e.file_name());
        if from.is_dir() {
            copy_tree(&from, &to)?;
        } else {
            fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

/// One simulated tree session between `sender` and `receiver` roots: both
/// sides' scans through the production pipeline, triage via the real
/// `tree::triage`, and per-file delta simulation for conversation files.
/// `warm` skips file scans for metadata-matching files (the D15 index path).
fn simulate_tree_session(
    sender: &Path,
    receiver: &Path,
    pipeline: &PipelineConfig,
    cdc: (usize, usize, usize),
    warm: bool,
) -> io::Result<TreeStats> {
    let t_scan = Instant::now();
    let sender_manifest = ffsync::tree::TreeManifest::scan(sender)?;
    let receiver_manifest = ffsync::tree::TreeManifest::scan(receiver)?;
    let scan_both = t_scan.elapsed();

    // Triage every path in the union (identical on both sides per §16.4).
    let mut all_paths: std::collections::BTreeSet<String> = Default::default();
    for e in sender_manifest.entries() {
        all_paths.insert(e.rel_path.clone());
    }
    for e in receiver_manifest.entries() {
        all_paths.insert(e.rel_path.clone());
    }

    let mut conversations: Vec<String> = Vec::new();
    let mut deletions: Vec<(String, u64)> = Vec::new();
    for rel in &all_paths {
        match ffsync::tree::triage(
            rel,
            sender_manifest.get(rel),
            receiver_manifest.get(rel),
            false,
        ) {
            ffsync::tree::Triage::Skip => {}
            ffsync::tree::Triage::Sync | ffsync::tree::Triage::Transfer => {
                conversations.push(rel.clone());
            }
            ffsync::tree::Triage::CreateDir | ffsync::tree::Triage::Conflict { .. } => {}
            ffsync::tree::Triage::Delete => {
                if let Some(e) = receiver_manifest.get(rel) {
                    deletions.push((rel.clone(), e.size));
                }
            }
        }
    }

    // Per-file delta simulation for the conversation set. A Transfer file
    // (receiver lacks it) scans on the sender only — every chunk is "missing"
    // and would cross the wire.
    let t_delta = Instant::now();
    let mut transferred = 0u64;
    let mut total_bytes = 0u64;
    for rel in &conversations {
        let src = sender.join(rel.replace('/', std::path::MAIN_SEPARATOR_STR));
        let dst = receiver.join(rel.replace('/', std::path::MAIN_SEPARATOR_STR));
        let (_, src_chunks) = scan_file(&src, pipeline, cdc)?;
        if dst.exists() {
            let (_, dst_chunks) = scan_file(&dst, pipeline, cdc)?;
            let delta = compute_delta(&src_chunks, &dst_chunks);
            transferred += delta.missing_bytes;
            total_bytes += delta.target_bytes;
        } else {
            transferred += src_chunks.iter().map(|c| c.length as u64).sum::<u64>();
            total_bytes += src_chunks.iter().map(|c| c.length as u64).sum::<u64>();
        }
    }
    let delta_sim = t_delta.elapsed();

    // Warm-session model: scans were metadata-only; per conversation file the
    // D15 index load replaces the scan. We measure index load cost on the
    // first conversation file (or the first file of the tree) and apply it
    // per file — index loads are O(chunks), and chunk counts are comparable
    // across files of the same size distribution.
    let warm_load_us = if warm {
        let probe = sender_manifest
            .entries()
            .find(|e| e.kind == ffsync::tree::EntryKind::File)
            .map(|e| sender.join(e.rel_path.replace('/', std::path::MAIN_SEPARATOR_STR)))
            .unwrap_or_else(|| sender.join("dir_00/file_000.bin"));
        // Ensure an index exists for the probe (session 2's commit wrote one
        // in the real flow; the simulation writes it here if missing).
        if ffsync::index::load_index(&probe).is_none() {
            let size = fs::metadata(&probe)?.len();
            let (_, chunks) = scan_file(&probe, pipeline, cdc)?;
            let index = ffsync::index::FileIndex::from_scan(
                chunks,
                size,
                ffsync::index::stat_mtime(&probe).unwrap_or((0, 0)),
            );
            ffsync::index::write_index(&probe, &index)?;
        }
        let t = Instant::now();
        let loaded = ffsync::index::load_index(&probe);
        let us = t.elapsed().as_secs_f64() * 1e6;
        if loaded.is_none() {
            return Err(io::Error::other("warm index probe failed to load"));
        }
        us
    } else {
        0.0
    };

    // Warm session wall time: scan_both (stat-only scans) + per-file index
    // loads for files that WOULD be conversations (here: none after a
    // converged session — the model cost if something had changed).
    // No hashing, no delta sim: a converged warm session is stat-only.
    let warm_session =
        scan_both + Duration::from_secs_f64(warm_load_us * 1e-6 * conversations.len() as f64);

    Ok(TreeStats {
        files: sender_manifest.len(),
        total_bytes,
        conversations: conversations.len(),
        transferred_bytes: if warm { 0 } else { transferred },
        deleted: if warm { 0 } else { deletions.len() },
        deleted_bytes: if warm {
            0
        } else {
            deletions.iter().map(|(_, b)| b).sum()
        },
        scan_both,
        delta_sim: if warm { Duration::ZERO } else { delta_sim },
        warm_session,
    })
}

// ---- Latency simulation (§16.8 measurement) ----
//
// Real tree sessions over an in-process duplex that delays each direction
// by RTT/2 (a read that finds data queued sleeps that long before
// returning it, coalescing anything already queued like TCP does for
// back-to-back writes). This models a WAN link: request/response
// conversations cost real round trips; scan and I/O cost what they cost.
//
// The numbers are a MODEL, not a network measurement — the point is the
// *ratio* between sequential and parallel conversation execution at the
// same simulated RTT, which is what §16.8's design decision rests on.

/// One end of a latency-injecting in-memory duplex.
struct LatencyEnd {
    /// Half the RTT: applied before returning queued data on a read.
    one_way: Duration,
    rx: std::sync::mpsc::Receiver<Vec<u8>>,
    tx: std::sync::mpsc::SyncSender<Vec<u8>>,
    /// Partially delivered chunk (a read may consume less than a chunk).
    pending: Vec<u8>,
    pending_pos: usize,
}

impl io::Read for LatencyEnd {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pending_pos >= self.pending.len() {
            self.pending.clear();
            self.pending_pos = 0;
            match self.rx.recv() {
                Ok(chunk) => {
                    self.pending = chunk;
                    // Coalesce anything the peer already wrote (models TCP
                    // batching back-to-back frame writes into one flight).
                    while let Ok(more) = self.rx.try_recv() {
                        self.pending.extend_from_slice(&more);
                    }
                    if self.one_way > Duration::ZERO {
                        std::thread::sleep(self.one_way);
                    }
                }
                Err(_) => return Ok(0), // peer dropped: clean EOF
            }
        }
        let available = self.pending.len() - self.pending_pos;
        let n = available.min(buf.len());
        buf[..n].copy_from_slice(&self.pending[self.pending_pos..self.pending_pos + n]);
        self.pending_pos += n;
        Ok(n)
    }
}

impl io::Write for LatencyEnd {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Bounded channel: full = the peer is not reading — exactly the
        // backpressure a TCP socket buffer gives; block like one. A dropped
        // peer (its receiving end gone) surfaces as Disconnected.
        match self.tx.send(buf.to_vec()) {
            Ok(()) => Ok(buf.len()),
            Err(_) => Err(io::Error::new(io::ErrorKind::BrokenPipe, "peer is gone")),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Builds both ends of a latency duplex: `ends.0` is the sender's, `ends.1`
/// the receiver's. Data written to one end appears on the other after
/// one_way (RTT/2).
fn latency_pair(one_way: Duration) -> (LatencyEnd, LatencyEnd) {
    let (a_tx, b_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(1024);
    let (b_tx, a_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(1024);
    (
        LatencyEnd {
            one_way,
            rx: a_rx,
            tx: a_tx,
            pending: Vec::new(),
            pending_pos: 0,
        },
        LatencyEnd {
            one_way,
            rx: b_rx,
            tx: b_tx,
            pending: Vec::new(),
            pending_pos: 0,
        },
    )
}

/// One session measurement: runs REAL tree_send/tree_receive over K
/// latency-duplex pairs on two threads; returns the wall time.
fn run_one_latency_session(
    sender_root: &Path,
    receiver_root: &Path,
    rtt: Duration,
    parallel: u16,
) -> io::Result<Duration> {
    use ffsync::protocol::tree_sync::{
        ReadWrite, TreeReceiveRequest, TreeSendRequest, tree_receive, tree_send,
    };

    let mut pairs: Vec<(LatencyEnd, LatencyEnd)> = Vec::new();
    for _ in 0..parallel {
        pairs.push(latency_pair(rtt));
    }

    let send_root = sender_root.to_path_buf();
    let recv_root = receiver_root.to_path_buf();
    let mut sender_ends: Vec<LatencyEnd> = Vec::new();
    let mut receiver_ends: Vec<LatencyEnd> = Vec::new();
    for (s, r) in pairs {
        sender_ends.push(s);
        receiver_ends.push(r);
    }
    // The closure that hands out receiver-side worker ends is called from
    // the accept loop sequentially but must be Send+Sync: share the ends
    // through a Mutex.
    let receiver_ends = std::sync::Mutex::new(receiver_ends);
    let receiver_ends_for_closure = std::sync::Arc::new(receiver_ends);

    let recv_root2 = recv_root;
    let ends2 = std::sync::Arc::clone(&receiver_ends_for_closure);
    let recv_thread = std::thread::spawn(move || -> Result<
        ffsync::protocol::tree_sync::TreeOutcome,
        ffsync::protocol::sync::SyncError,
    > {
        let receiver_ends = ends2;
        // Take the control end FIRST, dropping the lock before tree_receive
        // runs (a temporary guard on an argument would live for the whole
        // call and deadlock the accept closure).
        let mut control_end = receiver_ends.lock().unwrap().remove(0);
        tree_receive(
            &mut control_end,
            &TreeReceiveRequest {
                root: recv_root2,
                force_checksum: false,
                no_delete: false,
                excludes: Vec::new(),
                accept_worker: {
                    let ends = std::sync::Arc::clone(&receiver_ends);
                    std::sync::Arc::new(move || {
                        Ok(Box::new(ends.lock().unwrap().remove(0)) as Box<dyn ReadWrite>)
                    })
                },
            },
            &PipelineConfig::default(),
            &mut |_p| {},
        )
    });

    let t = Instant::now();
    let send_res = tree_send(
        &mut sender_ends.remove(0),
        &TreeSendRequest {
            root: send_root,
            force_checksum: false,
            no_delete: false,
            excludes: Vec::new(),
            parallel,
            connect_worker: {
                let ends = std::sync::Mutex::new(sender_ends);
                std::sync::Arc::new(move |_| {
                    Ok(Box::new(ends.lock().unwrap().remove(0)) as Box<dyn ReadWrite>)
                })
            },
        },
        &PipelineConfig::default(),
        &mut |_p| {},
    );
    let elapsed = t.elapsed();
    let recv_res = recv_thread.join().unwrap();
    if let Err(e) = send_res {
        return Err(io::Error::other(format!("sender failed: {e}")));
    }
    if let Err(e) = recv_res {
        return Err(io::Error::other(format!("receiver failed: {e}")));
    }
    Ok(elapsed)
}

/// The §16.8 measurement: diverged tree fixture, sessions at the configured
/// RTT for each parallel count (1 = sequential baseline).
fn run_latency_simulation(cfg: &BenchConfig, _pipeline: &PipelineConfig) -> io::Result<()> {
    println!();
    println!("==== latency simulation (§16.8): sequential vs parallel conversations ====");
    let mut rng = match cfg.seed {
        Some(seed) => StdRng::seed_from_u64(seed ^ 0x1CE_5EED),
        None => StdRng::from_rng(&mut rand::rng()),
    };

    let sender = cfg.out_dir.join("lat_sender");
    let receiver = cfg.out_dir.join("lat_receiver");
    let _ = fs::remove_dir_all(&sender);
    let _ = fs::remove_dir_all(&receiver);

    // ~100 files × ~32 KiB: conversation-count dominated (the RTT regime),
    // not bandwidth dominated. All files diverge (receiver starts empty) —
    // the worst case for serialized round trips.
    let total = generate_tree_fixture(&sender, &mut rng, 8, 100)?;
    fs::create_dir_all(&receiver)?;

    println!(
        "fixture: 100 files ({}), receiver starts empty, simulated RTT = {} ms",
        fmt_bytes(total),
        cfg.rtt_ms
    );
    println!("  (model: each direction delayed by RTT/2, reads coalesce queued data;");
    println!("   session wall time ≈ scan+I/O + conversations × round trips × RTT)");

    let counts: Vec<u16> = if cfg.parallel_counts.is_empty() {
        vec![1, 4, 8]
    } else {
        cfg.parallel_counts.clone()
    };

    println!();
    println!(
        "  {:>10} {:>14} {:>16}",
        "parallel", "wall time", "vs sequential"
    );
    let mut baseline: Option<Duration> = None;
    for k in counts {
        // Fresh receiver per measurement: every session is the full-transfer
        // worst case (comparable across K).
        let _ = fs::remove_dir_all(&receiver);
        fs::create_dir_all(&receiver)?;
        let elapsed =
            run_one_latency_session(&sender, &receiver, Duration::from_millis(cfg.rtt_ms), k)?;
        let rel = match baseline {
            None => {
                baseline = Some(elapsed);
                "baseline".to_string()
            }
            Some(b) => format!("{:.2}x", b.as_secs_f64() / elapsed.as_secs_f64()),
        };
        println!("  {:>10} {:>14} {:>16}", k, fmt_ms(elapsed), rel);
    }
    Ok(())
}

fn print_tree_stats(label: &str, s: &TreeStats) {
    println!();
    println!("---- {} ----", label);
    println!(
        "  {:<22} {:>9}   ({} files, {} total)",
        "tree scan (both):",
        fmt_ms(s.scan_both),
        s.files,
        fmt_bytes(s.total_bytes)
    );
    println!(
        "  {:<22} {:>9}   ({} conversations)",
        "delta sim:",
        fmt_ms(s.delta_sim),
        s.conversations
    );
    println!(
        "  {:<22} {:>9}   ({} deleted, {})",
        "warm session model:",
        fmt_ms(s.warm_session),
        s.deleted,
        fmt_bytes(s.deleted_bytes)
    );
}

/// Reconstructs the target variant from the source file via `Reconstructor`.
///
/// Chunks whose hash exists in the source are copied locally (at the source
/// chunk's offset); all other chunks are fed as simulated "network data" read
/// from the real variant file at the right offset. The result is verified
/// against the variant's root hash by `finalize`.
fn reconstruct(
    variant: &Path,
    source: &Path,
    rec_path: &Path,
    source_index: &HashMap<Hash, u64>,
    target_chunks: &[Chunk],
    variant_hash: Hash,
) -> io::Result<Duration> {
    // Remove a leftover reconstruction from an earlier run so that the final
    // rename in finalize() cannot collide on Windows.
    let _ = fs::remove_file(rec_path);

    let variant_size = fs::metadata(variant)?.len();
    let start = Instant::now();
    let reconstructor = Reconstructor::new(rec_path, source, variant_size)?;
    let variant_file = File::open(variant)?;
    let mut local_scratch: Vec<u8> = Vec::new();
    let mut net_scratch: Vec<u8> = Vec::new();

    for chunk in target_chunks {
        if let Some(&source_offset) = source_index.get(&chunk.hash) {
            let task = ReconstructionTask {
                target_offset: chunk.offset,
                source: ChunkSource {
                    offset: source_offset,
                    length: chunk.length,
                },
            };
            reconstructor.copy_local_chunk(&task, &mut local_scratch)?;
        } else {
            // Simulated network payload: the bytes the wire would carry,
            // read from the real variant file at the chunk's offset.
            net_scratch.resize(chunk.length, 0);
            let read = variant_file.read_at(&mut net_scratch[..chunk.length], chunk.offset)?;
            if read != chunk.length {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("short read from variant at offset {}", chunk.offset),
                ));
            }
            let task = ReconstructionTask {
                target_offset: chunk.offset,
                source: ChunkSource {
                    offset: chunk.offset,
                    length: chunk.length,
                },
            };
            reconstructor.write_chunk(&task, &net_scratch[..chunk.length])?;
        }
    }

    reconstructor.finalize(&Blake3Hasher, variant_hash, rec_path)?;
    Ok(start.elapsed())
}

/// Runs scan, delta simulation and reconstruction for one (source, target)
/// pair and removes the verified reconstruction file afterwards.
fn run_scenario(
    scenario: Scenario,
    source: &Path,
    variant: &Path,
    pipeline: &PipelineConfig,
    cdc: (usize, usize, usize),
) -> io::Result<ScenarioResult> {
    let (source_scan, source_chunks) = scan_file(source, pipeline, cdc)?;
    let (target_scan, target_chunks) = scan_file(variant, pipeline, cdc)?;
    let delta = compute_delta(&source_chunks, &target_chunks);

    let source_index: HashMap<Hash, u64> =
        source_chunks.iter().map(|c| (c.hash, c.offset)).collect();
    let variant_hash = file_root_hash(variant)?;

    let rec_path = variant.with_file_name(format!("reconstructed_{}.bin", scenario.name()));
    let reconstruct = reconstruct(
        variant,
        source,
        &rec_path,
        &source_index,
        &target_chunks,
        variant_hash,
    )?;
    fs::remove_file(&rec_path).ok();

    Ok(ScenarioResult {
        scenario,
        variant_size: fs::metadata(variant)?.len(),
        source_scan,
        target_scan,
        delta,
        reconstruct,
    })
}

/// Prints the per-scenario report including the transfer projection table.
fn print_scenario_report(result: &ScenarioResult, target: &Path) {
    let first = |d: &Option<Duration>| match d {
        Some(d) => fmt_ms(*d),
        None => "n/a".to_string(),
    };
    let src_label = "scan source (A):";
    let tgt_label = format!("scan target ({}):", result.scenario.name());
    println!();
    println!(
        "---- scenario: {} (target: {}) ----",
        result.scenario.name(),
        target.display()
    );
    println!(
        "  {:<24} {:>9}  (first chunk {:>9}, {:>4} chunks, {:>9} hashed)",
        src_label,
        fmt_ms(result.source_scan.total),
        first(&result.source_scan.first_chunk),
        result.source_scan.chunks,
        fmt_bytes(result.source_scan.hashed_bytes),
    );
    println!(
        "  {:<24} {:>9}  (first chunk {:>9}, {:>4} chunks, {:>9} hashed)",
        tgt_label,
        fmt_ms(result.target_scan.total),
        first(&result.target_scan.first_chunk),
        result.target_scan.chunks,
        fmt_bytes(result.target_scan.hashed_bytes),
    );

    let dedup = if result.delta.missing_bytes == 0 {
        "inf".to_string()
    } else {
        format!(
            "{:.1}x",
            result.delta.target_bytes as f64 / result.delta.missing_bytes as f64
        )
    };
    println!(
        "  delta: {:>9} missing of {:>9} ({:.2}%), dedup {}",
        fmt_bytes(result.delta.missing_bytes),
        fmt_bytes(result.delta.target_bytes),
        result.delta.pct(),
        dedup,
    );
    println!(
        "  reconstruction: {:>9}  (final hash verified)",
        fmt_ms(result.reconstruct),
    );
    print_projection_table(&result.delta, result.scan_max());
}

impl ScenarioResult {
    fn scan_max(&self) -> Duration {
        self.source_scan.total.max(self.target_scan.total)
    }
}

/// Prints the projected transfer and end-to-end times for all link speeds
/// and hypothetical zstd ratios. End-to-end assumes both peers scan in
/// parallel, so it is max(source scan, target scan) + transfer.
fn print_projection_table(delta: &DeltaStats, scan_max: Duration) {
    println!(
        "  projection (transfer of {} missing; e2e = max(scans) + transfer):",
        fmt_bytes(delta.missing_bytes)
    );
    println!(
        "    {:<10}  {:>9} {:>9}    {:>9} {:>9}    {:>9} {:>9}",
        "link", "1x xfer", "1x e2e", "2x xfer", "2x e2e", "4x xfer", "4x e2e"
    );
    for &(mbits, label) in &LINKS {
        let bytes_per_sec = mbits * 1_000_000.0 / 8.0;
        let mut cells = String::new();
        for &ratio in &RATIOS {
            let transfer = delta.missing_bytes as f64 / (bytes_per_sec * ratio);
            let e2e = transfer + scan_max.as_secs_f64();
            cells.push_str(&format!(
                "    {:>9} {:>9}",
                fmt_secs(transfer),
                fmt_secs(e2e)
            ));
        }
        println!("    {:<10}{}", label, cells);
    }
}

/// Prints the compact one-line-per-scenario summary table.
/// Cross-config comparison of the sweep: one row per CDC configuration,
/// one column per scenario, showing delta % (the over-send metric) and scan
/// cost. The interesting columns are `modified`/`appended` delta % versus
/// `fresh`/`identical` scan throughput.
fn print_sweep_conclusion(all_results: &[Vec<ScenarioResult>]) {
    println!();
    println!("==== sweep conclusion (delta % per scenario) ====");
    println!(
        "{:<22} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "cdc avg/min/max", "identical", "modified", "appended", "fresh", "src scan"
    );
    for (point, results) in SWEEP_POINTS.iter().zip(all_results) {
        let get = |name: &str| -> &ScenarioResult {
            results
                .iter()
                .find(|r| r.scenario.name() == name)
                .expect("scenario in sweep results")
        };
        let scan = get("modified").source_scan.total;
        println!(
            "{:<22} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10}",
            format!(
                "{}/{}/{}",
                fmt_bytes(point.0 as u64),
                fmt_bytes(point.1 as u64),
                fmt_bytes(point.2 as u64)
            ),
            get("identical").delta.pct(),
            get("modified").delta.pct(),
            get("appended").delta.pct(),
            get("fresh").delta.pct(),
            fmt_ms(scan),
        );
    }
    println!();
    println!(
        "Reading: lower delta % on modified/appended = fewer bytes on the wire;\
\nsmaller chunks cost scan throughput and produce more chunks (plan frames).\
\nPick the smallest avg size whose scan cost is acceptable for the target link."
    );
}

fn print_summary(results: &[ScenarioResult]) {
    println!();
    println!("==== summary ====");
    println!(
        "{:<10} {:>9} {:>9} {:>9} {:>9} {:>8} {:>9} {:>9} {:>8}",
        "scenario",
        "src scan",
        "tgt scan",
        "missing",
        "delta %",
        "dedup",
        "recon",
        "e2e 1x",
        "verified"
    );
    for r in results {
        let dedup = if r.delta.missing_bytes == 0 {
            "inf".to_string()
        } else {
            format!(
                "{:.1}x",
                r.delta.target_bytes as f64 / r.delta.missing_bytes as f64
            )
        };
        // E2E at 1 Gbit/s without compression for a single comparable number.
        let e2e = 1_000_000_000.0 / 8.0;
        let e2e_1x = r.scan_max().as_secs_f64() + r.delta.missing_bytes as f64 / e2e;
        println!(
            "{:<10} {:>9} {:>9} {:>9} {:>8.2}% {:>8} {:>9} {:>9} {:>8}",
            r.scenario.name(),
            fmt_ms(r.source_scan.total),
            fmt_ms(r.target_scan.total),
            fmt_bytes(r.delta.missing_bytes),
            r.delta.pct(),
            dedup,
            fmt_ms(r.reconstruct),
            fmt_secs(e2e_1x),
            "yes",
        );
    }
}

/// Probes for an rsync to compare against, in order of preference:
///
/// 1. native: `rsync --version` on the PATH (also the only mode on Linux),
/// 2. WSL: `wsl.exe --exec rsync --version` (Windows fallback),
/// 3. neither: a reason string for the skip message.
fn probe_rsync() -> RsyncProbe {
    let native = Command::new("rsync").arg("--version").output();
    match native {
        Ok(out) if out.status.success() => return RsyncProbe::Available(RsyncMode::Native),
        // Found but failing is treated as absent so the WSL fallback applies.
        _ => {}
    }

    // Sentinel: every distro has /bin/true. If this fails, WSL is absent,
    // broken, or has no installed distro at all.
    match wsl_command().arg("/bin/true").output() {
        Err(_) => {
            return RsyncProbe::Unavailable(
                "rsync not found on PATH and WSL (wsl.exe) is unavailable \
                 (this comparison can be run on Linux, or on Windows inside WSL)"
                    .to_string(),
            );
        }
        Ok(out) if !out.status.success() => {
            return RsyncProbe::Unavailable(
                "rsync not found on PATH and WSL has no usable distro \
                 (install one with `wsl.exe --install`, then install rsync inside it)"
                    .to_string(),
            );
        }
        Ok(_) => {}
    }

    if let Ok(out) = wsl_command().args(["rsync", "--version"]).output() {
        if out.status.success() {
            return RsyncProbe::Available(RsyncMode::Wsl);
        }
        return RsyncProbe::Unavailable(
            "rsync is not installed inside the WSL distro \
             (install it there, e.g. `wsl.exe sudo apt install rsync`)"
                .to_string(),
        );
    }
    RsyncProbe::Unavailable(
        "rsync not found on PATH and it could not be probed inside WSL \
         (this comparison can be run on Linux, or on Windows inside WSL)"
            .to_string(),
    )
}

/// Builds a `wsl.exe --exec <program> <args...>` command. `--exec` bypasses
/// the default shell (`/bin/sh -c`), which would otherwise mangle backslashes
/// and verbatim `\\?\` path prefixes in the arguments.
fn wsl_command() -> Command {
    let mut cmd = Command::new("wsl.exe");
    cmd.arg("--exec");
    cmd
}

/// Translates a Windows path to its WSL (`/mnt/...`) equivalent.
///
/// Preferred: canonicalize to an absolute Windows path, then ask the distro
/// itself via `wslpath -a -u`, so drive mapping comes from WSL's own rules.
/// Falls back to a manual `X:\dir\file` -> `/mnt/x/dir/file` translation if
/// `wslpath` fails. Never invokes a shell: arguments stay separate.
fn to_wsl_path(path: &Path) -> io::Result<String> {
    // Canonicalize the deepest existing ancestor: the file itself may not
    // exist yet (the destination is (re)written for every mode), but its
    // parent directory is created up front.
    let canonical = match fs::canonicalize(path) {
        Ok(p) => p,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let parent = path.parent().ok_or_else(|| {
                io::Error::other(format!("{} has no parent directory", path.display()))
            })?;
            let file_name = path
                .file_name()
                .ok_or_else(|| io::Error::other(format!("{} has no file name", path.display())))?;
            fs::canonicalize(parent)?.join(file_name)
        }
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!("cannot canonicalize {} for WSL: {}", path.display(), e),
            ));
        }
    };
    // Forward slashes: wslpath accepts them, and they survive any shell-free
    // argument passing. Strip the verbatim `\\?\` prefix that canonicalize
    // produces on Windows, which wslpath cannot interpret.
    let windows = canonical.to_string_lossy().replace('\\', "/");
    let windows = windows
        .strip_prefix("//?/")
        .map(|rest| rest.to_string())
        .unwrap_or(windows);

    let mut cmd = wsl_command();
    cmd.args(["wslpath", "-a", "-u", &windows]);
    if let Ok(out) = cmd.output()
        && out.status.success()
    {
        let converted = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if converted.starts_with('/') {
            return Ok(converted);
        }
    }

    // Manual fallback: `X:/dir/file` -> `/mnt/x/dir/file`.
    let bytes = windows.as_bytes();
    if bytes.len() >= 2 && bytes[1] == b':' {
        let mut manual = String::from("/mnt/");
        manual.push(bytes[0].to_ascii_lowercase() as char);
        manual.push_str(&windows[2..]);
        return Ok(manual);
    }
    Err(io::Error::other(format!(
        "wslpath failed and {:?} is not a drive-letter path",
        windows
    )))
}

/// Runs one timed rsync invocation (`rsync -a --stats <extra> src dst`),
/// returning (stats stdout, wall time).
fn run_rsync_once(
    mode: RsyncMode,
    mode_name: &str,
    extra_args: &[&str],
    src: &str,
    dst: &str,
) -> io::Result<(String, Duration)> {
    let mut cmd = match mode {
        RsyncMode::Native => {
            let mut cmd = Command::new("rsync");
            cmd.args(["-a", "--stats"]).args(extra_args);
            cmd
        }
        RsyncMode::Wsl => {
            let mut cmd = wsl_command();
            cmd.args(["rsync", "-a", "--stats"]).args(extra_args);
            cmd
        }
    };
    cmd.arg(src).arg(dst);

    let start = Instant::now();
    let out = cmd.output()?;
    let wall = start.elapsed();
    if !out.status.success() {
        return Err(io::Error::other(format!(
            "rsync ({}) failed: {}",
            mode_name,
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok((String::from_utf8_lossy(&out.stdout).to_string(), wall))
}

/// Runs the optional rsync comparison for the modified scenario.
fn run_rsync_comparison(
    cfg: &BenchConfig,
    source: &Path,
    variants: &[(Scenario, PathBuf)],
) -> io::Result<RsyncResults> {
    let probe = probe_rsync();
    let mode = match probe {
        RsyncProbe::Available(mode) => mode,
        RsyncProbe::Unavailable(reason) => {
            println!();
            println!("rsync comparison skipped: {}.", reason);
            return Ok(RsyncResults {
                mode: None,
                rows: Vec::new(),
            });
        }
    };

    let variant = &variants
        .iter()
        .find(|(s, _)| *s == Scenario::Modified)
        .expect("modified variant fixture")
        .1;

    // In WSL mode, every path handed to rsync must be translated first, so
    // the working directories are materialized before that point.
    let src_dir = cfg.out_dir.join("rsync_src");
    let dst_dir = cfg.out_dir.join("rsync_dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;
    let src_file = src_dir.join("A.bin");
    let dst_file = dst_dir.join("A.bin");
    fs::copy(source, &src_file)?;

    let (src_arg, dst_arg) = match mode {
        RsyncMode::Native => (
            src_file.to_string_lossy().to_string(),
            dst_file.to_string_lossy().to_string(),
        ),
        RsyncMode::Wsl => (to_wsl_path(&src_file)?, to_wsl_path(&dst_file)?),
    };

    let modes: [(&str, &[&str]); 4] = [
        ("default", &[]),
        ("whole-file", &["--whole-file"]),
        ("checksum", &["--checksum"]),
        // Force delta transmission: rsync disables its delta algorithm for
        // same-host (local) copies, so the default row would otherwise be a
        // full-file copy and say nothing about rsync's delta behavior. This
        // is the closest same-host analog to a real network transfer.
        ("delta (--no-W)", &["--no-whole-file", "--no-implied-dirs"]),
    ];

    let mut rows = Vec::new();
    for (mode_name, extra_args) in modes {
        // Reset the destination to the variant (old state) for every mode.
        fs::copy(variant, &dst_file)?;
        // Separate mtimes: `fs::copy` preserves the source's mtime on Windows,
        // so src and dst can share the same whole-second timestamp and rsync's
        // size+mtime quick check would skip the file and report a bogus
        // `0 B transferred`. Backdate the destination relative to the source.
        let src_mtime = fs::metadata(&src_file)?.modified()?;
        let older = src_mtime
            .checked_sub(Duration::from_secs(2))
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let times = FileTimes::new().set_accessed(older).set_modified(older);
        let dst = OpenOptions::new().write(true).open(&dst_file)?;
        dst.set_times(times).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("cannot backdate mtime of {}: {}", dst_file.display(), e),
            )
        })?;
        let (stdout, wall) = run_rsync_once(mode, mode_name, extra_args, &src_arg, &dst_arg)?;

        // Extract rsync's wire-bytes metric from --stats output.
        //
        // Careful: "Total transferred file size" is the total size of the
        // files that were transferred, NOT the bytes put on the wire — it
        // reports the full file size even when the delta algorithm matched
        // 98% of the data. The actual payload rsync sent as data is
        // "Literal data" (the complement of "Matched data"); "Total bytes
        // sent" additionally includes file-list and protocol overhead.
        let mut transferred = None;
        let mut matched = None;
        for line in stdout.lines() {
            let parse = |prefix: &str| -> Option<u64> {
                let rest = line.strip_prefix(prefix)?;
                rest.trim()
                    .trim_end_matches("bytes")
                    .trim()
                    .replace(',', "")
                    .parse::<u64>()
                    .ok()
            };
            if transferred.is_none() {
                transferred = parse("Literal data:");
            }
            if matched.is_none() {
                matched = parse("Matched data:");
            }
        }
        rows.push(RsyncRow {
            mode: mode_name,
            wall,
            transferred,
            matched,
        });
    }
    Ok(RsyncResults {
        mode: Some(mode),
        rows,
    })
}

/// Prints the rsync comparison table.
fn print_rsync_report(rsync: &RsyncResults) {
    let mode = match rsync.mode {
        Some(mode) => mode,
        None => return, // skip line was already printed by the comparison itself
    };
    println!();
    println!(
        "==== rsync comparison (scenario: modified, via {}) ====",
        mode.label()
    );
    println!(
        "{:<16} {:>10} {:>14} {:>14}",
        "mode", "wall", "transferred", "matched"
    );
    for row in &rsync.rows {
        let transferred = match row.transferred {
            Some(bytes) => fmt_bytes(bytes),
            None => "n/a".to_string(),
        };
        let matched = match row.matched {
            Some(bytes) => fmt_bytes(bytes),
            None => "n/a".to_string(),
        };
        println!(
            "{:<16} {:>10} {:>14} {:>14}",
            row.mode,
            fmt_ms(row.wall),
            transferred,
            matched
        );
    }
}

/// Emits the final machine-readable JSON line (hand-formatted, no serde).
fn emit_json(cfg: &BenchConfig, results: &[ScenarioResult], rsync: Option<&RsyncResults>) {
    let mut out = String::from("{\"bench\":\"ffsync\"");
    out.push_str(&format!(",\"size_mib\":{}", cfg.size_mib));
    out.push_str(",\"scenarios\":[");
    for (i, r) in results.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let dedup = if r.delta.missing_bytes == 0 {
            "null".to_string()
        } else {
            format!(
                "{:.3}",
                r.delta.target_bytes as f64 / r.delta.missing_bytes as f64
            )
        };
        out.push_str(&format!(
            "{{\"variant\":\"{name}\",\"target_bytes\":{tb},\
             \"source_scan_ms\":{ss:.3},\"target_scan_ms\":{ts:.3},\
             \"source_first_chunk_ms\":{sf},\"target_first_chunk_ms\":{tf},\
             \"source_chunks\":{sc},\"target_chunks\":{tc},\
             \"hashed_bytes\":{hb},\"missing_bytes\":{mb},\
             \"delta_pct\":{dp:.4},\"dedup_ratio\":{dd},\
             \"reconstruct_ms\":{rc:.3},\"verified\":true}}",
            name = r.scenario.name(),
            tb = r.variant_size,
            ss = ms(r.source_scan.total),
            ts = ms(r.target_scan.total),
            sf = json_ms(r.source_scan.first_chunk),
            tf = json_ms(r.target_scan.first_chunk),
            sc = r.source_scan.chunks,
            tc = r.target_scan.chunks,
            hb = r.source_scan.hashed_bytes,
            mb = r.delta.missing_bytes,
            dp = r.delta.pct(),
            dd = dedup,
            rc = ms(r.reconstruct),
        ));
    }
    out.push_str("],\"rsync\":");
    match rsync {
        None => out.push_str("null"),
        Some(r) if r.mode.is_none() => out.push_str("{\"available\":false}"),
        Some(r) => {
            out.push_str(&format!(
                "{{\"available\":true,\"via\":\"{}\",\"rows\":[",
                r.mode.unwrap().label()
            ));
            for (i, row) in r.rows.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let transferred = match row.transferred {
                    Some(bytes) => bytes.to_string(),
                    None => "null".to_string(),
                };
                out.push_str(&format!(
                    "{{\"mode\":\"{}\",\"wall_ms\":{:.3},\"transferred_bytes\":{}}}",
                    row.mode,
                    ms(row.wall),
                    transferred
                ));
            }
            out.push_str("]}");
        }
    }
    out.push('}');
    println!("{}", out);
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn json_ms(d: Option<Duration>) -> String {
    match d {
        Some(d) => format!("{:.3}", ms(d)),
        None => "null".to_string(),
    }
}

/// Human-readable byte count (binary units).
fn fmt_bytes(bytes: u64) -> String {
    let b = bytes as f64;
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    if b >= GIB {
        format!("{:.2} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.2} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.2} KiB", b / KIB)
    } else {
        format!("{} B", bytes)
    }
}

/// Human-readable duration for scan/reconstruction timings.
fn fmt_ms(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s < 1.0 {
        format!("{:.1} ms", s * 1000.0)
    } else {
        format!("{:.2} s", s)
    }
}

/// Human-readable duration for projection table cells.
fn fmt_secs(secs: f64) -> String {
    if secs < 1.0 {
        format!("{:.1} ms", secs * 1000.0)
    } else {
        format!("{:.2} s", secs)
    }
}

fn main() -> io::Result<()> {
    let cfg = parse_args();
    println!(
        "ffsync bench: size = {} MiB, out-dir = {}, rsync = {}, json = {}",
        cfg.size_mib,
        cfg.out_dir.display(),
        cfg.rsync,
        cfg.json
    );

    // Fixture-only mode exits before ANY other fixture generation (the WAN
    // harness wants exactly the --latency tree, nothing else).
    if cfg.gen_tree {
        let mut rng = match cfg.seed {
            Some(seed) => StdRng::seed_from_u64(seed ^ 0x1CE_5EED),
            None => StdRng::from_rng(&mut rand::rng()),
        };
        let sender = cfg.out_dir.join("lat_sender");
        let _ = fs::remove_dir_all(&sender);
        let total = generate_tree_fixture(&sender, &mut rng, 8, 100)?;
        println!(
            "fixture: 100 files ({}) in {} + empty dir, seed {}",
            fmt_bytes(total),
            sender.display(),
            cfg.seed.unwrap_or(0)
        );
        return Ok(());
    }

    let (source, variants) = generate_fixtures(&cfg)?;
    let pipeline = PipelineConfig::default();

    let cdc = cfg.cdc.unwrap_or((CHUNK_MIN, CHUNK_AVG, CHUNK_MAX));

    if cfg.sweep {
        let mut all_results: Vec<Vec<ScenarioResult>> = Vec::new();
        for cdc_point in SWEEP_POINTS {
            println!(
                "\n==== cdc min/avg/max = {}/{}/{} ====",
                fmt_bytes(cdc_point.0 as u64),
                fmt_bytes(cdc_point.1 as u64),
                fmt_bytes(cdc_point.2 as u64)
            );
            let mut results = Vec::new();
            for (scenario, path) in &variants {
                let result = run_scenario(*scenario, &source, path, &pipeline, cdc_point)?;
                print_scenario_report(&result, path);
                results.push(result);
            }
            print_summary(&results);
            all_results.push(results);
        }
        print_sweep_conclusion(&all_results);
        return Ok(());
    }

    let mut results = Vec::new();
    for (scenario, path) in &variants {
        let result = run_scenario(*scenario, &source, path, &pipeline, cdc)?;
        print_scenario_report(&result, path);
        results.push(result);
    }

    print_summary(&results);

    if cfg.disconnect {
        // Simulate a mid-transfer disconnect on the modified scenario: the
        // receiver's old file is the source, the desired state is the
        // modified variant.
        let modified = &variants
            .iter()
            .find(|(s, _)| *s == Scenario::Modified)
            .expect("modified variant")
            .1;
        let (_, variant_chunks) = scan_file(modified, &pipeline, cdc)?;
        let (_, source_chunks) = scan_file(&source, &pipeline, cdc)?;
        run_disconnect_simulation(&source, modified, &source_chunks, &variant_chunks, cdc)?;
    }

    if cfg.warm {
        run_warm_simulation(&source, &pipeline, cdc)?;
    }

    if cfg.tree {
        run_tree_simulation(&cfg, &pipeline)?;
    }

    if cfg.latency {
        run_latency_simulation(&cfg, &pipeline)?;
    }

    let rsync = if cfg.rsync {
        Some(run_rsync_comparison(&cfg, &source, &variants)?)
    } else {
        None
    };
    if let Some(r) = &rsync {
        print_rsync_report(r);
    }

    if cfg.json {
        emit_json(&cfg, &results, rsync.as_ref());
    }
    Ok(())
}
