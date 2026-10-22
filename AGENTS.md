# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## What this project is

`ffsync` is a file synchronization tool written in Rust that syncs data at the
chunk level to minimize transfer and I/O, similar in spirit to rsync. As of
v0.3 (unreleased), single-file sync **and directory-tree sync** between two
peers over TCP work end-to-end: parallel chunking/hashing pipeline, file
reconstruction, manifest/plan exchange, zstd-compressed transfer,
chunk-root verification, mtime/size fast path (sub-second mtimes),
resume after mid-transfer disconnects, the persistent chunk index for warm
starts (D15), TLS transport with mutual TOFU fingerprint pinning (D16), and
tree sync (D17: protocol v3 — TREE_BEGIN/TREE_END envelope, sequential
per-file conversations, mtime preservation on commit, deletes deferred to
session end, `--no-delete` opt-out). Not implemented yet: excludes/pattern
matching, parallel per-file streams (QUIC, D13), permissions/xattrs/symlink
policy (D9).

Design documents (read before making architectural changes):

- `concept/Positioning.md` — why this project exists, the niche, the decision
  framework (link speed decides what matters), and the rejected
  "build around rsync" alternatives
- `concept/Design Decisions.md` — recorded decisions with reasoning and
  alternatives; add new decisions there, do not silently change old ones
- `concept/Protocol v0.1.md` — the wire protocol (named for its first
  release; it tracks the protocol across versions — currently v2: HELLO
  gained `file_mtime_nanos`): framing, handshake, manifest/plan/data flow,
  verification, resume. The implementation follows it; deviations are
  protocol bugs unless the spec is amended first
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
| `cargo run --bin bench` | Run the benchmark harness (fixture variants, delta simulation, reconstruction, link-speed projections; `--quick`, `--size-mib N`, `--seed N`, `--rsync`, `--disconnect`, `--warm`, `--json`) |
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

- `src/lib.rs` — lib target; exposes `chunking`, `index`, `protocol`,
  `reconstruction`, and `transport` (tests and the bench binary import
  through it).
- `src/main.rs` — the `ffsync` CLI (`send`/`receive`/`trust`/`untrust`/
  `fingerprints`): argument parsing, progress display, TCP setup, TLS
  wiring, and error reporting. Thin by design; the sync logic lives in
  `protocol::sync`.
- `src/bin/bench.rs` — the benchmark harness: generates fixture variants
  (identical / modified / appended / fresh), scans both sides through the real
  pipeline, simulates the delta against a hash set, reconstructs the target
  through the real `Reconstructor` with hash verification, and projects
  transfer times across link speeds and zstd ratios. Optional `--rsync`
  comparison (native or via WSL), `--disconnect` resume simulation, `--warm`
  warm-start index measurement. Zero dependencies beyond the crate's own.
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
- `src/protocol/` — the wire protocol. `framing.rs` (length-prefixed frames),
  `message.rs` (HELLO/manifest/plan/DATA/VERIFY/… codecs), `sync.rs` (the
  sender/receiver state machines: manifest exchange, exact-hash diffing (D12),
  zstd delta transfer, locals-first write order, mtime/size fast path,
  resume (D10), warm-start integration (D15)), and `tree_sync.rs` (the v3
  tree session orchestrators: TREE_BEGIN/TREE_END envelope, deterministic
  triage on both sides, sequential per-file conversations, deferred
  deletions). State machines are generic over `S: Read + Write` —
  transport-agnostic by design.
- `src/tree.rs` — the tree layer (D17): metadata-only directory scan
  (sorted, ffsync sidecar files excluded), relative-path validation
  (no traversal, wire paths always `/`-separated), the `triage` pure
  function (skip/sync/transfer/create-dir/delete/kind-conflict), and
  the tree manifest type. Pure computation — no protocol frames here.
- `src/transport.rs` — transport security (D16): rustls TLS 1.3 as a thin
  wrapper. `Identity` (self-signed cert + PKCS#8 key, raw-DER codec in
  `identity.bin`, created on first run), SHA-256 fingerprint codec
  (`AA:BB:…`), `PinPolicy` (`Pinned(set)` vs `FirstUse`), and
  `client_session`/`server_session` — each completes the TLS handshake
  eagerly and returns the presented fingerprint; the CLI applies the TOFU
  decision (prompt or `--trust`) *after* the handshake, before any sync
  data. The verifier never makes trust decisions by itself. Pins live in
  `<state-dir>/pins.conf` (one fingerprint per line, atomic rewrite).
  `--plaintext` bypasses TLS entirely.
- `tests/loopback.rs` — two-peer integration tests over in-memory duplex
  streams: identical/modified/fresh/fast-path/disconnect-resume plus the
  warm-start, stale-index, and orphan-cleanup scenarios.
- `tests/tls.rs` — TLS transport tests over real TCP loopback: pin-match
  converges, third-party cert rejected by the client, unpinned client
  rejected by the pinned receiver (error names the attacker's
  fingerprint), FirstUse handshake + sync, pins-file codec round trip.
- `tests/tree.rs` — tree-session tests over real TCP loopback: converge a
  mixed tree (nested dirs, empty files/dirs), second session skips
  everything (mtime preservation fast path), modified/new-only deltas,
  deletes after success, `--no-delete`, `--checksum`, empty-file
  convergence, kind conflicts, v2-flow peers rejecting TREE_BEGIN,
  wire path traversal refused, vanished sender files fail closed.
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
  plane — there is no tokio in the tree anymore (removed in v0.1; the
  measurement in `evaluation/chunking.md` showed async reads ~5x slower than
  the mmap + native-thread pipeline). Keep it that way unless a transport
  (QUIC) genuinely requires an async runtime at the edges; re-evaluate the
  D13 decision when that comes up.

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
- mtime preservation on commit (§16.6, v3) is what makes tree syncs skip:
  the receiver sets the target's mtime to the sender's *before* writing the
  D15 index (order: rename → set_modified → stat → write_index). Reversing
  the order leaves the index recording the wrong mtime and every subsequent
  tree sync re-checks the file.
- Tree manifests must exclude `*.ffsync-index` / `*.ffsync-tmp` — they are
  ffsync's own bookkeeping; syncing them would sync internal state and
  desync warm starts (an index written at commit changes mtime → re-sync
  loop). The scan filter in `src/tree.rs` is the single place this lives.
- Tree triage must be deterministic and identical on both peers (same two
  manifests, same flags — the receiver derives its flags from the sender's
  TREE_BEGIN). Any nondeterminism desyncs the per-file conversation
  sequence; the receiver fails loudly on the first unexpected HELLO.
- Tree sessions make the fast-path branch explicit (FILE_SYNC frame);
  single-file mode still relies on the drain-after-NOT_MODIFIED dance.
  Do not "simplify" one to match the other — they differ because the
  session continues after a tree conversation but ends after a single-file
  one.
- Windows is the primary development platform; keep platform-specific I/O
  behind `FileExt` cfg branches.
- The identity file (`identity.bin`) holds the private key: `0600` on unix,
  inherited user-profile ACLs on Windows; corrupt identity files are
  refused loudly (never silently regenerated — that would break every
  peer's pin). Pins are a set, not per-name entries; any pinned fingerprint
  may connect in either role.
- Never commit generated artifacts. `.gitignore` covers `*.bin` and
  `/bench_tmp/` (bench fixtures); `.opencode/` holds local-only configuration
  and credentials.

## Roadmap (from the concept docs)

Implemented: single-file sync end-to-end (Phases 2–4 of the workflow doc:
root-hash identity, chunk diffing and plan generation, transfer + zstd),
resume, the persistent chunk index (D15), transport security (D16:
rustls TLS 1.3, mutual TOFU fingerprint pinning), and directory-tree sync
(D17, protocol v3: Phase 1 metadata triage + per-file conversations,
mtime preservation, deferred deletes). Not yet implemented: excludes/
pattern matching, parallel per-file streams — QUIC (D13 amendment:
deferred until measurement shows parallel streams worthwhile), and
permissions/xattrs/symlinks as first-class metadata (D9). See
`concept/Design Decisions.md` D5 (checksum-based change detection) and
D8 (link-adaptive diff strategy) for the design questions the pipelining
step must answer.
