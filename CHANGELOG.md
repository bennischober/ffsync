# Changelog

All notable changes to ffsync are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Persistent chunk index (warm starts, D15):** each synced file gets a
  sidecar `<file>.ffsync-index` (size, mtime, chunk-tree root, ordered chunk
  list) written only from verified state at commit time. On the next sync, a
  peer whose index validates (size + mtime at nanosecond granularity) skips
  its file scan entirely — repeated syncs of unchanged data become stat-only
  operations. Corrupt indexes are refused at load (the decoder re-derives the
  chunk-tree root from the chunk list and rejects mismatches); a
  stale-but-validating index fails the sync loudly and is invalidated, so a
  retry cold-scans and recovers — never a poison loop, never silent
  corruption.

### Fixed

- The mtime/size fast path now compares sub-second mtimes. Whole-second
  comparison let a same-second rewrite of a same-size file pass as "already
  in sync" without transferring — a silent wrong-content no-op (protocol
  version bumped to 2; HELLO gained `file_mtime_nanos`). Found by the
  warm-start tests.

## [0.1.0] - 2026-10-22

First working implementation: single-file sync between two peers over TCP.

### Added

- Chunking pipeline: memory-mapped files, FastCDC content-defined chunking
  (64 KiB / 256 KiB / 1 MiB defaults, picked by measurement), parallel BLAKE3
  hashing with one chunker thread per core; chunks travel as `(offset, length)`
  and are never copied.
- Out-of-place reconstruction: pre-allocated temp file, positional writes at
  arbitrary offsets, chunk-tree-root verification, atomic rename commit.
- Wire protocol v0.1: length-prefixed framing with 11 frame types (HELLO,
  manifest, plan, DATA, VERIFY, COMMIT_ACK, NOT_MODIFIED, ERROR), hand-rolled
  binary encoding — no protobuf.
- Sender/receiver state machines over any duplex stream: manifest exchange,
  exact-hash diffing and plan generation (D12), zstd-compressed delta
  transfer, locals-first write order (D10), mtime/size fast path with
  `--checksum` escape hatch (D11).
- Resume after mid-transfer disconnects: the temp file is the resume state;
  on reconnect the receiver re-scans it and streams a union manifest
  (temp chunks first, then old-file chunks); PLAN_LOCAL carries the chunk
  hash so local reads are verified against the expected content.
- CLI: `ffsync send <file> <host:port>` and `ffsync receive <file> <port>`
  with single-line progress display (scan, diff, transfer, verify phases).
- Benchmark harness (`--bin bench`): fixture variants (identical, modified,
  appended, fresh), link-speed projections, `--sweep` chunk-size sweep,
  `--disconnect` resume simulation, optional rsync comparison (`--rsync`,
  native or WSL), `--json` output.
- Loopback integration tests (two peers over in-memory duplex): identical,
  modified, fresh, fast-path, and disconnect-resume scenarios; plus protocol
  unit tests and an end-to-end reconstruction test. 21 tests total.
- I/O timeouts on the TCP streams and receiver-side resource limits (plan
  chunks must stay inside the declared file size).

### Notes

- Windows is the primary development platform; positional I/O is
  `#[cfg]`-split between Unix and Windows.
- The chunk-tree root (file identity for verification and change detection)
  is a domain-separated BLAKE3 over the ordered chunk-hash list — not the
  standard BLAKE3 file hash (its internal tree is over fixed 1024-byte
  blocks, not CDC chunk boundaries).
- rsync remains faster on delta ratio for small localized edits (fine-grained
  fixed blocks); ffsync's advantage is the parallel scan (~3.8x cold start on
  the 72.7 GiB reference file) and resumability.

[0.1.0]: https://github.com/bennischober/ffsync/releases/tag/v0.1.0
