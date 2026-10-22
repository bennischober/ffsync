# ffsync

**Fast chunk-level file sync for big files and fast links.** rsync's behavior —
minimal bytes on the wire, resumable, verified — with a modern engine: parallel
content-defined chunking, BLAKE3, warm restarts, no daemon.

> **Status: v0.1.0 — first working implementation.** Single-file sync between
> two peers over TCP works end-to-end: parallel scan, manifest/plan exchange,
> zstd-compressed delta transfer, chunk-root verification, atomic commit,
> mtime/size fast path, and **resume after mid-transfer disconnects**.
> Directory sync, encryption, and the persistent index are next.

## Why

rsync is battle-tested, but its per-file scan is single-threaded: the sender
computes a rolling checksum over every byte on one core. On large files that
means a long "cold start" of disk-grinding, network-silent hashing before the
first byte moves — and it pays that cost again on every run.

ffsync attacks exactly that:

- **Parallel scan** — memory-mapped I/O, one chunker thread per core, BLAKE3
  (SIMD) hash workers. Measured ~3.8x faster cold start than rsync on a 72.7 GiB
  file, disk-bound rather than CPU-bound (~320 MB/s vs ~85 MB/s effective).
- **Content-defined chunking** — FastCDC boundaries instead of fixed blocks, so
  chunk identity survives insertions/deletions, and the BLAKE3 root hash doubles
  as change detection and end-to-end verification for free.
- **Warm restarts (planned)** — chunks are content-addressed, so a persisted
  chunk index turns *repeated* syncs into near-zero-scan operations. rsync
  rescans everything from scratch every run; this is the core product insight.
- **Resumable by construction (planned)** — the receiver's chunk manifest *is*
  the resume state; a disconnect wastes at most one in-flight chunk.

Where ffsync matters — and where it doesn't — is a link-speed question:
sync time ≈ max(scan on both peers, transfer). On slow links, transfer
dominates and rsync is fine. On **fast links (LAN, 1–10 GbE) with big files
and repeated syncs**, the scan dominates and ffsync's engine is the win. The
full reasoning lives in [`concept/Positioning.md`](concept/Positioning.md).

## Using it

Two terminals on the same machine (or LAN — replace `127.0.0.1` with the
receiver's address):

```bash
# Receiver: listen and converge ./big-file.bin from its current state
ffsync receive ./big-file.bin 7788

# Sender: push the (newer) file to the receiver
ffsync send ./big-file.bin 127.0.0.1:7788
```

Flags: `--checksum` disables the mtime/size fast path (rsync `-c` semantics —
always compare chunk roots); receiver-side `--from <old-file>` reads the local
chunks from a different file than the target. If the connection drops
mid-transfer, the receiver keeps its partial state; rerunning the same two
commands **resumes** — the receiver re-scans its temp file, unions it with the
old file, and only the genuinely missing chunks cross the wire.

Progress is single-line: `hashing → diffing → sending/receiving → verifying`,
with bytes, throughput, and elapsed time per phase. A scan-bound sync says so
instead of being silently disk-busy/network-idle — that silence in rsync is
why this project exists.

## First numbers

Benchmark (`cargo run --release --bin bench -- --quick --seed 42`, 32 MiB,
release build; all reconstructions hash-verified):

```
scenario    src scan  tgt scan   missing   delta %    recon    e2e 1x
identical    23.5 ms   23.6 ms       0 B     0.00%   44.9 ms   23.6 ms
modified     23.4 ms   23.6 ms  3.60 MiB    11.24%   41.8 ms   53.8 ms
appended     23.1 ms   25.5 ms  3.24 MiB     9.21%   45.2 ms   52.7 ms
fresh        23.5 ms   23.4 ms 32.00 MiB   100.00%   37.9 ms  292.0 ms
```

An 11% modification sends 11% of the bytes; identical files cost zero transfer
bytes and one parallel scan. For comparison, on the same modified scenario
rsync's delta algorithm (`--no-whole-file` over WSL, same fixtures) sent
**667 KiB** — its fine-grained fixed blocks win on delta ratio for small
localized edits; ffsync's current ~1 MiB average chunks trade some of that for
scan parallelism (see `evaluation/rsync.md`, and the chunk-size sweep planned
in `concept/Deep Research Digest.md`).

## Building and benchmarking

```bash
cargo build --release
cargo test
cargo run --release --bin bench -- --quick          # fast run (32 MiB)
cargo run --release --bin bench -- --size-mib 1024  # 1 GiB, stresses the pipeline
cargo run --release --bin bench -- --quick --rsync  # also time rsync (native or WSL)
cargo audit                                          # dependency advisory check
```

## Project layout

- `src/chunking/` — the pipeline: `process_file()` memory-maps a file, chunks
  it (Fixed/FastCDC) on worker threads, hashes chunks (BLAKE3) on more workers,
  all connected by bounded crossbeam channels. Chunks travel as
  `(offset, length)` — never copied. `scan_file_ordered()` returns chunks in
  canonical file order (the chunk-tree root is order-sensitive).
- `src/reconstruction/` — out-of-place reconstruction: pre-allocated temp file,
  positional writes at arbitrary offsets, final chunk-root verification, atomic
  rename. `open_existing()` reopens a temp without truncating (resume).
- `src/protocol/` — the v0.1 wire protocol: framing, typed messages, and the
  sender/receiver state machines (`send()`/`receive()` over any duplex stream).
- `src/bin/bench.rs` — the benchmark harness described above.
- `concept/` — design docs: positioning, decision log (D1–D14), wire protocol
  draft, and the original concept notes.
- `evaluation/` — measured results: chunking approaches, rsync comparison.

## Design docs

The interesting content is the reasoning, and it's all recorded:

- [`concept/Positioning.md`](concept/Positioning.md) — why this exists, the
  niche, why "build around rsync" doesn't work
- [`concept/Design Decisions.md`](concept/Design Decisions.md) — D1–D14 with
  alternatives and rejected options
- [`concept/Protocol v0.1.md`](concept/Protocol%20v0.1.md) — the wire protocol
  the first implementation will follow
- [`concept/Deep Research Digest.md`](concept/Deep%20Research%20Digest.md) —
  what the deep-research runs got right, and what we rejected

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
