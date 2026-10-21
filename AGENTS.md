# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## What this project is

`ffsync` is a file synchronization tool written in Rust that syncs data at the
chunk level to minimize transfer and I/O, similar in spirit to rsync. It is in
an early prototype stage: the chunking/hashing pipeline, file reconstruction,
and a benchmark harness exist; metadata exchange, sync-plan generation, and the
network protocol are not implemented yet.

Design documents (read before making architectural changes):

- `concept/Positioning.md` — why this project exists, the niche, the decision
  framework (link speed decides what matters), and the rejected
  "build around rsync" alternatives
- `concept/Design Decisions.md` — recorded decisions with reasoning and
  alternatives; add new decisions there, do not silently change old ones
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
| `cargo run` | Run `main.rs` (currently a manual benchmark harness with a hardcoded local file path) |
| `cargo run --bin bench` | Run the benchmark harness (fixture variants, delta simulation, reconstruction, link-speed projections; `--quick`, `--size-mib N`, `--rsync`, `--json`) |
| `cargo fmt` | Format (default rustfmt style; no `rustfmt.toml`) |
| `cargo clippy` | Lint (no `clippy.toml`) |

Toolchain: current stable Rust, edition 2024. The crate is a lib + two bins
(`ffsync`, `bench`); tests import via `ffsync::chunking` / `ffsync::reconstruction`.

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
