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
use ffsync::reconstruction::{ChunkSource, FileExt, ReconstructionTask, Reconstructor};
use rand::RngCore;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

/// FastCDC lower bound used for every scan.
const CHUNK_MIN: u32 = 256 * 1024;
/// FastCDC average target used for every scan.
const CHUNK_AVG: u32 = 1024 * 1024;
/// FastCDC upper bound used for every scan.
const CHUNK_MAX: u32 = 4 * 1024 * 1024;

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
}

/// Result of the optional rsync comparison.
struct RsyncResults {
    available: bool,
    rows: Vec<RsyncRow>,
}

fn print_usage() {
    println!(
        "ffsync benchmark harness

Usage: cargo run --bin bench -- [options]

Options:
  --size-mib <N>   fixture size in MiB (default: {})
  --quick          use a {} MiB fixture for a fast run
  --out-dir <path> directory for fixtures and scratch output (default: {})
  --rsync          additionally compare against rsync (needs rsync on PATH)
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

fn parse_args() -> BenchConfig {
    let mut cfg = BenchConfig {
        size_mib: DEFAULT_SIZE_MIB,
        out_dir: PathBuf::from(DEFAULT_OUT_DIR),
        quick: false,
        rsync: false,
        json: false,
        size_explicit: false,
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
fn write_random_file(path: &Path, size: u64) -> io::Result<()> {
    let mut file = File::create(path)?;
    let mut rng = rand::rng();
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
fn flip_middle_regions(path: &Path, size: u64) -> io::Result<()> {
    let region_len = (size / 100).max(4096).min(size / 4);
    let mut rng = rand::rng();
    let mut buf = vec![0u8; region_len as usize];
    let file = OpenOptions::new().write(true).open(path)?;
    for pos in [size * 45 / 100, size * 55 / 100] {
        rng.fill_bytes(&mut buf);
        file.write_at(&buf, pos)?;
    }
    Ok(())
}

/// Appends ~10% of new random bytes to the file.
fn append_random_tail(path: &Path, size: u64) -> io::Result<()> {
    let tail_len = (size / 10).max(1);
    let mut rng = rand::rng();
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

    let source = cfg.out_dir.join("source_A.bin");
    write_random_file(&source, size)?;
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
                flip_middle_regions(&path, size)?;
            }
            Scenario::Appended => {
                fs::copy(&source, &path)?;
                append_random_tail(&path, size)?;
            }
            Scenario::Fresh => {
                write_random_file(&path, size)?;
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

/// Scans `path` with the production pipeline and collects all chunks.
fn scan_file(path: &Path, pipeline: &PipelineConfig) -> io::Result<(ScanStats, Vec<Chunk>)> {
    let start = Instant::now();
    let rx = process_file(
        path,
        || FastCDCChunker::new(CHUNK_MIN, CHUNK_AVG, CHUNK_MAX),
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
        let task = if let Some(&source_offset) = source_index.get(&chunk.hash) {
            ReconstructionTask {
                target_offset: chunk.offset,
                source: ChunkSource {
                    offset: source_offset,
                    length: chunk.length,
                    is_local: true,
                },
            }
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
            ReconstructionTask {
                target_offset: chunk.offset,
                source: ChunkSource {
                    offset: chunk.offset,
                    length: chunk.length,
                    is_local: false,
                },
            }
        };

        let network_data = if task.source.is_local {
            None
        } else {
            Some(&net_scratch[..chunk.length])
        };
        reconstructor.write_chunk(&task, &mut local_scratch, network_data)?;
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
) -> io::Result<ScenarioResult> {
    let (source_scan, source_chunks) = scan_file(source, pipeline)?;
    let (target_scan, target_chunks) = scan_file(variant, pipeline)?;
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

/// Runs the optional rsync comparison for the modified scenario.
fn run_rsync_comparison(
    cfg: &BenchConfig,
    source: &Path,
    variants: &[(Scenario, PathBuf)],
) -> io::Result<RsyncResults> {
    // Probe for rsync; on plain Windows it is typically absent.
    let probe = Command::new("rsync").arg("--version").output();
    if probe.is_err() || !probe.as_ref().unwrap().status.success() {
        println!();
        println!(
            "rsync comparison skipped: rsync not found on PATH. \
             This comparison must be run on Linux/WSL \
             (e.g. `cargo run --bin bench -- --quick --rsync` inside WSL)."
        );
        return Ok(RsyncResults {
            available: false,
            rows: Vec::new(),
        });
    }

    let variant = &variants
        .iter()
        .find(|(s, _)| *s == Scenario::Modified)
        .expect("modified variant fixture")
        .1;

    let src_dir = cfg.out_dir.join("rsync_src");
    let dst_dir = cfg.out_dir.join("rsync_dst");
    fs::create_dir_all(&src_dir)?;
    fs::create_dir_all(&dst_dir)?;
    let src_file = src_dir.join("A.bin");
    let dst_file = dst_dir.join("A.bin");
    fs::copy(source, &src_file)?;

    let modes: [(&str, &[&str]); 3] = [
        ("default", &[]),
        ("whole-file", &["--whole-file"]),
        ("checksum", &["--checksum"]),
    ];

    let mut rows = Vec::new();
    for (mode, extra_args) in modes {
        // Reset the destination to the variant (old state) for every mode.
        fs::copy(variant, &dst_file)?;
        let start = Instant::now();
        let out = Command::new("rsync")
            .arg("-a")
            .arg("--stats")
            .args(extra_args)
            .arg(&src_file)
            .arg(&dst_file)
            .output()?;
        let wall = start.elapsed();
        if !out.status.success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "rsync ({}) failed: {}",
                    mode,
                    String::from_utf8_lossy(&out.stderr).trim()
                ),
            ));
        }

        // Best-effort extraction of the transferred size from --stats output.
        let mut transferred = None;
        let stdout = String::from_utf8_lossy(&out.stdout);
        for line in stdout.lines() {
            if let Some(rest) = line.strip_prefix("Total transferred file size:") {
                let digits = rest
                    .trim()
                    .trim_end_matches("bytes")
                    .trim()
                    .replace(',', "");
                transferred = digits.parse::<u64>().ok();
            }
        }
        rows.push(RsyncRow {
            mode,
            wall,
            transferred,
        });
    }
    Ok(RsyncResults {
        available: true,
        rows,
    })
}

/// Prints the rsync comparison table.
fn print_rsync_report(rsync: &RsyncResults) {
    if !rsync.available {
        return; // skip line was already printed by the comparison itself
    }
    println!();
    println!("==== rsync comparison (scenario: modified) ====");
    println!("{:<12} {:>10} {:>14}", "mode", "wall", "transferred");
    for row in &rsync.rows {
        let transferred = match row.transferred {
            Some(bytes) => fmt_bytes(bytes),
            None => "n/a".to_string(),
        };
        println!(
            "{:<12} {:>10} {:>14}",
            row.mode,
            fmt_ms(row.wall),
            transferred
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
        Some(r) if !r.available => out.push_str("{\"available\":false}"),
        Some(r) => {
            out.push_str("{\"available\":true,\"rows\":[");
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

    let (source, variants) = generate_fixtures(&cfg)?;
    let pipeline = PipelineConfig::default();

    let mut results = Vec::new();
    for (scenario, path) in &variants {
        let result = run_scenario(*scenario, &source, path, &pipeline)?;
        print_scenario_report(&result, path);
        results.push(result);
    }

    print_summary(&results);

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
