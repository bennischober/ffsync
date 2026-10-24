# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

**Generated:** 2026-10-23 · **Commit:** 953666a · **Branch:** main

## What this project is

`ffsync` is a file synchronization tool written in Rust that syncs data at the
chunk level to minimize transfer and I/O, similar in spirit to rsync. As of
v0.8.0, single-file sync **and directory-tree sync** between two peers over
TCP or QUIC work end-to-end: parallel chunking/hashing pipeline, file
reconstruction, manifest/plan exchange, zstd-compressed transfer,
chunk-root verification, mtime/size fast path (sub-second mtimes),
resume after mid-transfer disconnects, the persistent chunk index for warm
starts (D15), TLS transport with mutual TOFU fingerprint pinning (D16),
tree sync (D17: protocol v3 — TREE_BEGIN/TREE_END envelope, sequential
per-file conversations, mtime preservation on commit, deletes deferred to
session end, `--no-delete` opt-out), tree-mode excludes (§16.7:
`--exclude`/`--exclude-from`, sender's patterns rule the session,
receiver's patterns guard the delete pass), parallel per-file
conversations (§16.8, D13 step 1: `--parallel K` over K connections,
measured 3.4×–6.1× at 30 ms RTT), the QUIC transport (`--quic`, §17,
D13 step 2: one QUIC connection per session, tree conversations ride
streams, same identity/pinning), and POSIX mode-bit preservation on commit
(§16.9, v4/D18 — chmod-only changes are deliberately NOT synced; mode rides
the size+mtime fast path, is sender-authoritative, and Windows senders send
no section). The metadata remainder is closed by decision (D9 amendment,
2026-10-23): symlink-as-entry, hardlinks, and xattrs stay out of scope with
recorded reasoning — see `concept/Design Decisions.md` D9 and
`evaluation/ship-readiness.md`.

Per-directory knowledge bases: `src/chunking/AGENTS.md`,
`src/protocol/AGENTS.md`, and `src/transport/AGENTS.md` hold
subtree-specific conventions and invariants — consult them when working in
those directories.

Design documents (read before making architectural changes):

- `concept/Positioning.md` — why this project exists, the niche, the decision
  framework (link speed decides what matters), and the rejected
  "build around rsync" alternatives
- `concept/Design Decisions.md` — recorded decisions with reasoning and
  alternatives; add new decisions there, do not silently change old ones
- `concept/Protocol v0.1.md` — the wire protocol (named for its first
  release; it tracks the protocol across versions — currently v4: v3 added
  tree sync §16, v4 added the metadata/mode section §16.9): framing,
  handshake, manifest/plan/data flow, verification, resume, tree sync,
  QUIC. The implementation follows it; deviations are protocol bugs unless
  the spec is amended first. Commit durability (§8.3): the temp file is
  flushed before every rename (`Reconstructor::sync_temp`, 2026-10-23).
  Dry-run (D20, §7.1/§16.10): zero-mutation preview — HELLO flag bit 3,
  DRY_RUN_ACK (0x11) marker, tree would-delete report. Eager diff (D8,
  §9.2): --eager streams DATA during the manifest exchange behind the
  §6.2 position gate (over-send 0 in paired runs; both peers opt in,
  decline fails loudly). HELLO flag decode rejects unknown bits (strict
  codec).
- `concept/Deep Research Digest.md` — what the two deep-research runs
  (architecture + P2P connectivity, saved alongside as markdown) got right,
  and what we rejected; its follow-ups F1–F4 are all closed
- `concept/File Synchronisation Workflow.md` — the four-phase sync pipeline
  (shallow → medium → deep diff, then pipelined transfer and reconstruction)
- `concept/File Reconstruction.md` — out-of-place reconstruction with a
  pre-allocated temp file and atomic rename
- `concept/P2P Connection Challenge Resolution.md` — raw NAT-traversal
  research (STUN/TURN/ICE); the basis for keeping P2P out of scope (D9)
- `concept/Designing a High-Performance, Resilient File Synchronization
  Protocol in Rust_ ….md` — raw architecture research (QUIC, BLAKE3, CDC,
  zstd) that fed the design docs
- `evaluation/chunking.md` — measured performance of chunking approaches and
  the reasoning behind the current pipeline design
- `evaluation/memory.md` — measured memory footprint of the sync paths
  (heap is O(chunks) ≈ 0.05% of file size; trees stay flat with file count)
- `evaluation/parallel.md` — `--parallel K` scaling (model + real-TCP +
  real-WAN)
- `evaluation/comparison.md` — loopback comparison vs rsync/croc/sendme plus
  the threat-model table
- `evaluation/rsync.md` — the chunk-size sweep (F1) that picked the
  64k/256k/1m defaults, and rsync delta comparison
- `evaluation/ship-readiness.md` — the feature-gap and readiness
  assessment (2026-10-23): verdict, pre-ship blockers, and the order to 1.0

## Commands

| Command | Purpose |
| --- | --- |
| `cargo build` | Build the binary |
| `cargo check --tests` | Fast compile check including tests |
| `cargo test` | Run unit and integration tests (six test files: `integration`, `loopback`, `tree`, `tls`, `quic`, `soak`) |
| `cargo run` | Run the CLI (`ffsync send/receive`, see `--help`) |
| `cargo run --bin bench` | Run the benchmark harness (fixture variants, delta simulation, reconstruction, link-speed projections; `--quick`, `--size-mib N`, `--seed N`, `--cdc m/a/M`, `--sweep`, `--rsync`, `--disconnect`, `--warm`, `--tree`, `--latency` (+ `--rtt-ms`, `--parallel K,...`), `--gen-tree`, `--out-dir P`, `--json`) |
| `cargo fmt` | Format (default rustfmt style; no `rustfmt.toml`) |
| `cargo clippy` | Lint (no `clippy.toml`) |
| `cargo audit` | Check `Cargo.lock` against RustSec advisories (see D14 in `concept/Design Decisions.md`) |
| `cargo test --test soak -- --ignored` | Run the leak-guard soak test (100 sessions under a counting allocator; excluded from normal runs and CI) |

CI (`.github/workflows/`) runs on push to `main` and on PRs, on
Windows **and** Linux: `cargo fmt --check`, `cargo clippy --all-targets
-- -D warnings`, `cargo test`, plus an MSRV job (`cargo check
--all-targets` on Rust 1.88) and `cargo audit` — keep them green locally
before pushing. A weekly scheduled workflow re-runs `cargo audit` so
advisories published between pushes are caught (D14). `release.yml` (tag
pushes) verifies the tag against `Cargo.toml`, runs the same gates, then
`cargo publish --locked` and builds 4-platform artifacts;
`wan-measure.yml` is a manual-dispatch WAN benchmark workflow (it uses
`bench --gen-tree`).

Toolchain: current stable Rust (1.99.x as of 2026-10), edition 2024,
`rust-version = 1.88` (dependency-driven: rcgen 0.14 / time 0.3 need 1.88;
CI enforces it as a separate msrv job). The crate is a lib + two bins
(`ffsync`, `bench`); tests import via `ffsync::chunking` /
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
  `reconstruction`, `transport`, and `tree` (tests and the bench binary
  import through it).
- `src/main.rs` — the `ffsync` CLI (`send`/`receive`/`trust`/`untrust`/
  `fingerprints`): argument parsing, progress display, TCP/TLS/QUIC setup,
  the TOFU decision (prompt or `--trust`), and error reporting (~1300
  lines — tree glue lives here too, but the sync logic itself stays in
  `protocol::sync` / `protocol::tree_sync`). `--dry-run` (D20, §7.1/§16.10)
  previews a session with zero mutation: receiver answers the plan with
  NOT_MODIFIED + DRY_RUN_ACK (would-change) or COMMIT_ACK (nothing would
  change), no temp file, no index; tree mode runs triage-only with a
  would-delete report.
- `src/bin/bench.rs` — the benchmark harness: generates fixture variants
  (identical / modified / appended / fresh), scans both sides through the real
  pipeline, simulates the delta against a hash set, reconstructs the target
  through the real `Reconstructor` with hash verification, and projects
  transfer times across link speeds and zstd ratios. Optional `--rsync`
  comparison (native or via WSL), `--disconnect` resume simulation, `--warm`
  warm-start index measurement, `--tree`/`--latency` tree-session runs,
  `--gen-tree` fixture emission (used by `wan-measure.yml`), `--cdc m/a/M`
  + `--sweep` CDC experiments, `--out-dir` fixture location. Zero
  dependencies beyond the crate's own.
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
  hand-rolled binary codec (magic `FFSYIX02` since D19, 80-byte header
  carrying size + mtime + the CDC parameters that produced the chunk list,
  44-byte chunk entries), load-time self-verification (root re-derived from
  the chunk list), size + nanosecond-mtime + CDC-parameter validation, atomic
  temp+rename writes, and the `index_path_for` naming convention (suffix
  **appended**, never `with_extension` — the orphan cleanup recovers target
  names by stripping the literal suffix).
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
  deletions; plus the v4 parallel sessions of §16.8 — `--parallel K`
  spreads conversations over K connections, one native thread per
  connection, membership-validated via TREE_FILE). State machines are
  generic over `S: Read + Write` — transport-agnostic by design. `mod.rs`
  carries `MAGIC` and `VERSION` (currently 4: v3 tree sync, v4
  metadata/mode section).
- `src/tree.rs` — the tree layer (D17): metadata-only directory scan
  (sorted, ffsync sidecar files excluded, optional exclude patterns with
  subtree pruning — §16.7), relative-path validation (no traversal, wire
  paths always `/`-separated), the hand-rolled glob matcher
  (`ExcludeSet`/`ExcludePattern`), the `triage` pure function
  (skip/sync/transfer/create-dir/delete/kind-conflict), and the tree
  manifest type. Pure computation — no protocol frames here.
- `src/transport/` — the transport layer. `mod.rs` is transport security
  (D16): rustls TLS 1.3 as a thin wrapper. `Identity` (self-signed cert +
  PKCS#8 key, raw-DER codec in `identity.bin`, created on first run),
  SHA-256 fingerprint codec (`AA:BB:…`), `PinPolicy` (`Pinned(set)` vs
  `FirstUse`), and `client_session`/`server_session` — each completes the
  TLS handshake eagerly and returns the presented fingerprint; the CLI
  applies the TOFU decision (prompt or `--trust`) *after* the handshake,
  before any sync data. The verifier never makes trust decisions by
  itself. Pins live in `<state-dir>/pins.conf` (one fingerprint per line,
  atomic rewrite). `--plaintext` bypasses TLS entirely (TCP only).
  `quic.rs` is the QUIC carrier (D13 step 2, §17): quinn + the same
  identity/pins, `QuicStream` adapts quinn streams to sync
  `Read + Write + Send` (block_on at the boundary), `QuicSession` owns the
  connection (worker conversations = extra streams); tokio lives here and
  nowhere else (D3 edge exception).
- `tests/loopback.rs` — two-peer integration tests over in-memory duplex
  streams: identical/modified/fresh/fast-path/disconnect-resume plus the
  warm-start, stale-index, and orphan-cleanup scenarios.
- `tests/soak.rs` — leak guard (ignored by default): 100 sequential
  sessions in one process under a counting global allocator, asserting net
  live heap stays flat; run with `cargo test --test soak -- --ignored`.
- `tests/tls.rs` — TLS transport tests over real TCP loopback: pin-match
  converges, third-party cert rejected by the client, unpinned client
  rejected by the pinned receiver (error names the attacker's
  fingerprint), FirstUse handshake + sync, pins-file codec round trip.
- `tests/quic.rs` — QUIC transport tests over real UDP loopback (§17):
  single-file sync converges, pin-match + MITM rejected by the client,
  unpinned client rejected by the pinned receiver, tree convergence,
  parallel sessions over K streams (§16.8 on one connection), deletes
  after success.
- `tests/tree.rs` — tree-session tests over real TCP loopback: converge a
  mixed tree (nested dirs, empty files/dirs), second session skips
  everything (mtime preservation fast path), modified/new-only deltas,
  deletes after success, `--no-delete`, `--checksum`, empty-file
  convergence, kind conflicts, v2-flow peers rejecting TREE_BEGIN,
  wire path traversal refused, vanished sender files fail closed,
  parallel sessions (§16.8: convergence, sequential-fallback rejection,
  fail-closed on worker error).
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
  plane. The one registered exception (D3 amendment, 2026-10-23) is the
  QUIC transport edge: `src/transport/quic.rs` owns a small tokio runtime
  because quinn is an async library; everything it returns is sync
  (`QuicStream: Read + Write + Send` via block_on at the boundary), and
  `ffsync::protocol` MUST NOT depend on tokio — new transport code keeps
  that split (async at the edge, sync at the seam). The async-file-I/O
  measurement (`evaluation/chunking.md`, ~5× slower) still rules out any
  async in the chunking/reconstruction paths.
- No rustfmt.toml / clippy.toml / toolchain pin files exist: default
  rustfmt and clippy behavior is authoritative, and lint enforcement
  happens via CI (`cargo clippy --all-targets -- -D warnings`; no
  crate-level `#![deny]`/`#![forbid]`).
- The crate's only `unsafe` block is the mmap at
  `src/chunking/pipeline.rs:82` — keep it that way, and add the missing
  `SAFETY:` comment when you touch it.
- `src/lib.rs`'s crate-level doc comment is stale (calls main.rs a
  benchmark harness) — fix on touch.
- `.opencode/` holds tracked agent tooling (a git-commit-message skill);
  credentials under it are untracked and must stay that way.

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
- Mode bits (§16.9, v4/D18) extend that ordering: rename → **apply_mode** →
  set_modified → stat → write_index. Mode is never a triage input (a
  chmod-only change is not synced — the same trade as rsync's fast path);
  it rides conversations the size+mtime triage already starts. The
  metadata flag is sender-authoritative and mirrored by the receiver
  (triage inputs must be identical on both peers); a Windows sender sends
  no section (synthesized modes would corrupt unix exec bits). On Windows,
  read-only targets block rename/truncate (os error 5) —
  `clear_readonly_best_effort` runs before every commit write and
  `apply_commit_mode` re-asserts read-only after.
- CDC parameters (D19, v0.8.0) are a pure function of the SENDER's file
  size (`cdc_params_for_size`: < 16 MiB → 16k/64k/256k, else 64k/256k/1m)
  and travel authoritative in HELLO's `cdc_min/avg/max` — the receiver
  MUST chunk its manifest scan and the VERIFY re-scan with the peer's
  values (different parameters produce different chunk layouts; roots
  could never agree otherwise). Nonsense peer values fall back to the
  local policy (hostile-peer guard). The receiver scans AFTER the
  handshake for exactly this reason — do not "optimize" the scan back
  before the HELLO exchange. The D15 index (FFSYIX02) records the
  parameters and serves a warm start only on a parameter match; the
  receiver-side `load_index` (without parameters) loads but the warm list
  is gated on `validate(path, cdc)` after the handshake. Files ≥ 16 MiB
  chunk byte-identically to the pre-D19 defaults (existing large-file
  indexes stay valid) — keep that promise in any policy change, and run
  bench fingerprints per config.
- The tree scan never recurses into directory symlinks/junctions (an
  ancestor junction on Windows needs no privilege and was a plausible
  infinite loop); file symlinks are followed, dangling ones warn+skip.
  Classification goes through symlink metadata, not the target's.
- Tree manifests must exclude `*.ffsync-index` / `*.ffsync-tmp` — they are
  ffsync's own bookkeeping; syncing them would sync internal state and
  desync warm starts (an index written at commit changes mtime → re-sync
  loop). The scan filter in `src/tree.rs` is the single place this lives.
- Tree triage must be deterministic and identical on both peers (same two
  manifests, same flags — the receiver derives its flags from the sender's
  TREE_BEGIN). Any nondeterminism desyncs the per-file conversation
  sequence; the receiver fails loudly on the first unexpected HELLO.
- Excludes (§16.7) follow the same contract, with a twist: the receiver
  must read the sender's TREE_BEGIN *before* scanning its own tree (the
  sender's patterns govern both scans). The receiver's own patterns never
  touch manifests — they only guard its delete pass. Never "optimize" the
  receiver into scanning first; that deadlocks nothing but desyncs triage.
- Tree sessions make the fast-path branch explicit (FILE_SYNC frame);
  single-file mode still relies on the drain-after-NOT_MODIFIED dance.
  Do not "simplify" one to match the other — they differ because the
  session continues after a tree conversation but ends after a single-file
  one.
- Parallel sessions (§16.8) replace wire ORDER with membership: every
  conversation (control connection included) is announced by a TREE_FILE
  frame and validated against the expected set, exactly once. Do not
  "optimize" the membership check away or reintroduce order-based
  expectations — the sender's worker threads race, and arrival order is
  arbitrary on every connection. The receiver's control loop consumes the
  sender's TREE_END itself (that is how it stops serving partition 0), so
  the sequential §16.5 barrier read is gated on `sender_end_consumed` —
  removing that gate deadlocks the sender on its TREE_END reply.
- Worker connections authenticate with the SAME pin policy as the control
  connection; the CLI's connect/accept closures handle that, and the
  state machines stay transport-agnostic (the closures are injected).
  Over QUIC (§17) the same closures open/accept extra streams on the one
  session connection — the membership protocol is unchanged.
- QUIC teardown is NOT TCP teardown (§17.6): closing a QUIC connection
  discards unACKed *stream* data (RFC 9000 §10.2.2), while TCP `close()`
  flushes. A final frame that is ANSWERED (TREE_END report, COMMIT_ACK)
  is only safe when the writer holds the connection until the reply/close
  is observed — the CLI's `wait_for_peer_close` guards both receive paths
  and the tree sender. Dropping a `QuicStream` auto-finishes (FIN), which
  is what the receiver's Eof path expects; a dropped `RecvStream` sends
  STOP_SENDING, so never rely on reading a stream after its `QuicStream`
  was dropped. quinn's `Endpoint::connect`/`Incoming::accept` and
  `Endpoint::new` all require a tokio runtime context (they spawn the
  driver) — route them through the module's `block_on`, and give
  `client_session` its bounded handshake timeout (quinn retransmits
  Initials forever; a dead peer must fail loudly, §17.5).
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
rustls TLS 1.3, mutual TOFU fingerprint pinning), directory-tree sync
(D17, protocol v3: Phase 1 metadata triage + per-file conversations,
mtime preservation, deferred deletes), tree-mode excludes (§16.7), and
parallel per-file conversations (§16.8, D13 step 1: `--parallel K` over
K connections — measured 3.4×–6.1× on a 100-file full transfer at
30 ms RTT), and the QUIC transport (§17, D13 step 2: `--quic`, one
connection per session, conversations ride streams), and POSIX mode-bit
preservation (§16.9, v4/D18). The D9 metadata remainder (symlink-as-entry,
hardlinks, xattrs) is closed by decision with recorded reasoning (D9
amendment, 2026-10-23). See
`concept/Design Decisions.md` D5 (checksum-based change detection) and
D8 (link-adaptive diff strategy) for the design questions the pipelining
step must answer.
