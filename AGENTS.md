# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## What this project is

`ffsync` is a file synchronization tool written in Rust that syncs data at the
chunk level to minimize transfer and I/O, similar in spirit to rsync. As of
v0.1.0, single-file sync between two peers over TCP works end-to-end: parallel
chunking/hashing pipeline, file reconstruction, manifest/plan exchange,
zstd-compressed transfer, chunk-root verification, mtime/size fast path, and
resume after mid-transfer disconnects. Not implemented yet: directory/multi-file
sync, the persistent chunk index (warm restarts), and encryption/TLS.

Design documents (read before making architectural changes):

- `concept/Positioning.md` — why this project exists, the niche, the decision
  framework (link speed decides what matters), and the rejected
  "build around rsync" alternatives
- `concept/Design Decisions.md` — recorded decisions with reasoning and
  alternatives; add new decisions there, do not silently change old ones
- `concept/Protocol v0.1.md` — the v0.1 wire protocol: framing, handshake,
  manifest/plan/data flow, verification, resume. The implementation follows
  it; deviations are protocol bugs unless the spec is amended first
- `concept/Deep Research Digest.md` — what the two deep-research runs
  (architecture + P2P connectivity, saved alongside as markdown) got right,
  and what we rejected
- `concept/File Synchronisation Workflow.md` — the four-phase sync pipeline
  (shallow → medium → deep diff, then pipelined transfer and reconstruction)
- `concept/File Reconstruction.md` — out-of-place reconstruction with a
  pre-allocated temp file and atomic rename
- `evaluation/chunking.md` — measured performance of chunking approaches and
  the reasoning behind the current pipeline design

## Commands

| Command | Purpose |
| --- | --- |
| `cargo build` | Build the binary |
| `cargo check --tests` | Fast compile check including tests |
| `cargo test` | Run unit and integration tests (`tests/integration.rs`) |
| `cargo run` | Run the CLI (`ffsync send/receive`, see `--help`) |
| `cargo run --bin bench` | Run the benchmark harness (fixture variants, delta simulation, reconstruction, link-speed projections; `--quick`, `--size-mib N`, `--seed N`, `--rsync`, `--json`) |
| `cargo fmt` | Format (default rustfmt style; no `rustfmt.toml`) |
| `cargo clippy` | Lint (no `clippy.toml`) |
| `cargo audit` | Check `Cargo.lock` against RustSec advisories (see D14 in `concept/Design Decisions.md`) |

CI (`.github/workflows/`) runs on push to `main` and on PRs, on
Windows **and** Linux: `cargo fmt --check`, `cargo clippy --all-targets
-- -D warnings`, `cargo test`, `cargo audit` — keep all four green
locally before pushing. A weekly scheduled workflow re-runs `cargo audit`
so advisories published between pushes are caught (D14).

Toolchain: current stable Rust (1.99.x as of 2026-10), edition 2024. The crate
is a lib + two bins (`ffsync`, `bench`); tests import via `ffsync::chunking` /
`ffsync::reconstruction`. Run `cargo audit` whenever dependencies change; on
fastcdc upgrades additionally fingerprint chunk boundaries before/after with
`cargo run --release --bin bench -- --quick --seed 42` (identical chunk counts
and missing-byte counts required — boundaries are wire-protocol-relevant).

The `--rsync` bench flag compares against rsync when available; on Windows it
falls back to WSL (`wsl.exe` + `wslpath` translation). Paths are passed as
separate args (never through a shell) so spaces are safe.

The crate may be mid-refactor and fail to compile. Before starting work, run
`cargo check --tests` to establish a baseline so you know which errors are
pre-existing.

## Architecture

- `src/lib.rs` — lib target exposing `chunking` and `reconstruction` (the
  integration test and the bench binary import through it).
- `src/main.rs` — binary entry point; currently a scratch/benchmark harness,
  not a stable API.
- `src/bin/bench.rs` — the benchmark harness: generates fixture variants
  (identical / modified / appended / fresh), scans both sides through the real
  pipeline, simulates the delta against a hash set, reconstructs the target
  through the real `Reconstructor` with hash verification, and projects
  transfer times across link speeds and zstd ratios. Optional `--rsync`
  comparison (native or via WSL). Zero dependencies beyond the crate's own.
- `src/chunking/` — the core data pipeline. The public API is re-exported from
  `mod.rs`.
  - `pipeline.rs` — `process_file()` is the entry point: it memory-maps the
    file (`memmap2`), spawns one chunker thread per core (each striding over
    256 MiB regions), and pushes `(offset, length)` work items through bounded
    `crossbeam` channels to hash-worker threads, which slice the mmap directly.
    Returns a `Receiver<ChunkEvent>`.
  - `chunker.rs` — `Chunker` trait with two implementations: `FixedChunker`
    (baseline) and `FastCDCChunker` (content-defined, via `fastcdc::v2020`).
  - `hasher.rs` — `Hasher` trait; `Blake3Hasher` (in-memory and streaming).
  - `types.rs` — `Chunk` (offset, length, BLAKE3 hash) and `ChunkEvent`.
- `src/index.rs` — the persistent per-file chunk index (D15, warm starts):
  hand-rolled binary codec (magic `FFSYIX01`, 68-byte header, 44-byte chunk
  entries), load-time self-verification (root re-derived from the chunk
  list), size + nanosecond-mtime validation, atomic temp+rename writes, and
  the `index_path_for` naming convention (suffix **appended**, never
  `with_extension` — the orphan cleanup recovers target names by stripping
  the literal suffix).
- `src/reconstruction/` — writes a new file out-of-place from chunk sources.
  - `reconstructor.rs` — `Reconstructor`: pre-allocates a temp file, writes
    each chunk at its target offset (local copy from the old file, or remote
    payload), then verifies the final BLAKE3 hash and atomically renames.
  - `io_ext.rs` — `FileExt` trait for positional I/O (`read_at`/`write_at`)
    with separate `#[cfg(unix)]` and `#[cfg(windows)]` implementations.
- `tests/integration.rs` — end-to-end test: reconstruct a modified file from
  an older local version plus simulated "network" chunks.

## Conventions

- Conventional Commits for commit messages.
- Doc comments (`///`) on public traits, structs, and functions.
- Swappable behavior goes behind traits (`Chunker`, `Hasher`, `FileExt`); add
  new strategies as implementations instead of hardcoding or adding enums with
  match arms everywhere.
- Keep submodules private and re-export the intended public API explicitly in
  each `mod.rs`.
- Prefer native threads + `crossbeam` channels over async/tokio for the data
  plane. The tokio runtime only wraps `main`; measurements in
  `evaluation/chunking.md` showed async reads were roughly 5x slower than the
  mmap + native-thread pipeline.

## Gotchas (non-obvious invariants)

- Chunk data is never copied or sent through channels — only
  `(offset, length)`. Hash workers index into the mmap themselves. Don't
  reintroduce data copies.
- `process_file` takes a `chunker_factory` closure because each worker thread
  needs its own stateful chunker instance; the `Hasher` is shared via `Arc`
  because it is stateless (`Send + Sync`). Preserve this split.
- The mmap is created with `unsafe { Mmap::map }`; the underlying file must not
  be modified while the pipeline holds the mapping.
- The reconstructor's temp path is derived with
  `Path::with_extension("ffsync-tmp")`, which *replaces* the last extension
  (`a.mkv` → `a.ffsync-tmp`). This differs from the `<.file.ext.ffsync-tmp>`
  naming in the concept doc (recorded as D6 in `concept/Design Decisions.md`).
  The index path does the OPPOSITE (appends `.ffsync-index`) because the
  orphan cleanup must recover the target name by stripping the suffix; never
  "fix" one to match the other.
- Sub-second mtime is load-bearing in two places: the index validation and
  the §4 wire fast path (HELLO `file_mtime_nanos`, protocol v2). Whole-second
  comparisons let a same-second same-size rewrite pass as unchanged — a
  silent wrong-content sync. Any new metadata-based fast path must compare
  at the finest available granularity.
- Windows is the primary development platform; keep platform-specific I/O
  behind `FileExt` cfg branches.
- Never commit generated artifacts. `.gitignore` covers `*.bin` and
  `/bench_tmp/` (bench fixtures); `.opencode/` holds local-only configuration
  and credentials.

## Roadmap (from the concept docs)

Not yet implemented: metadata scan and triage (Phase 1), file-level BLAKE3
root-hash exchange (Phase 2), live chunk diffing and `SyncPlan` generation
(Phase 3), and the network transfer/protocol layer including Zstd compression
(Phase 4). See `concept/Design Decisions.md` D5 (checksum-based change
detection), D8 (link-adaptive diff strategy), and D10 (resume state, open) for
the design questions the protocol work must answer.
