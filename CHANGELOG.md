# Changelog

All notable changes to ffsync are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Directory-tree sync (D17, protocol v3):** pass a directory to
  `ffsync send` / `ffsync receive` and the *contents* of the directory sync
  into the destination directory (`dir` and `dir/` mean the same thing —
  no rsync trailing-slash semantics). Both peers exchange metadata-only
  tree manifests (relative path, kind, size, mtime), triage locally and
  identically, then run the proven per-file machinery sequentially over
  one connection for exactly the files that need it. Matching files are
  skipped without a byte of file I/O; nested and empty directories are
  created; kind conflicts (file vs. dir on the same path) resolve with the
  sender's kind winning.
- **`--no-delete` (tree mode):** by default the receiver deletes files that
  exist only locally — the sender's tree is authoritative, like
  `rsync --delete`. The flag keeps them. Deletions always apply only
  after every transfer succeeded: a failed session never destroys data.
- **`bench --tree`:** a tree-session simulation in the benchmark harness
  (multi-file fixture, both sides through the production scan pipeline,
  real triage, per-file delta measurement, warm vs cold session model).
- **Wire protocol v3:** new frames TREE_BEGIN (0x0C), TREE_END (0x0D),
  FILE_SYNC (0x0E). v0.2 peers reject v3 loudly (strict framing + version
  check); single-file sync between v0.3 peers is unchanged apart from the
  two behaviors below.

### Changed

- **mtime preservation on commit (breaking-ish, §16.6):** after a verified
  transfer the receiver sets the target file's mtime to the sender's.
  Without this, tree syncs would re-check every synced file forever; with
  it, an unchanged tree syncs metadata-only. Single-file syncs benefit the
  same way (the next run fast-paths instead of re-verifying). Matches
  rsync's default `--times` behavior.
- **Empty sender files now converge (bug fix):** a zero-byte sender file
  used to fail with `HashMismatch` against a non-empty receiver file; it
  now truncates the target to empty (verified against the empty root).

### Fixed

- Tree manifests exclude ffsync's own bookkeeping (`*.ffsync-index`,
  `*.ffsync-tmp`); syncing them would have synced internal state and
  desynced warm starts.
- Receiver-only directories are deleted (deepest-first) with their files;
  directories the sender still has survive even when empty on the
  receiver.

## [0.2.0] - 2026-10-22

### Added

- **Transport security (D16):** TLS 1.3 (rustls, ring provider) is on by
  default for both `send` and `receive`; `--plaintext` opts out (LAN use or
  inside an SSH tunnel). Each peer generates a self-signed certificate on
  first run (stored in the ffsync state directory) and peers authenticate
  *mutually* by pinning the certificate's SHA-256 fingerprint: first contact
  shows the fingerprint and asks `trust this peer? [y/N]` (or pass
  `--trust <fingerprint>` non-interactively); every later sync verifies
  silently. A non-matching fingerprint is a loud, terminal rejection before
  any sync data flows. Manage pins with `ffsync trust <fingerprint>`,
  `ffsync untrust <fingerprint>`, `ffsync fingerprints` (one plain-text
  `pins.conf` in the state dir; no usernames, no passwords, no secrets on
  disk — the SSH `known_hosts` mental model, mutual by default).
- **Persistent chunk index (warm starts, D15):** each synced file gets a
  sidecar `<file>.ffsync-index` (size, mtime, chunk-tree root, ordered chunk
  list) written only from verified state at commit time. On the next sync, a
  peer whose index validates (size + mtime at nanosecond granularity) skips
  its file scan entirely - repeated syncs of unchanged data become stat-only
  operations. Corrupt indexes are refused at load (the decoder re-derives the
  chunk-tree root from the chunk list and rejects mismatches); a
  stale-but-validating index fails the sync loudly and is invalidated, so a
  retry cold-scans and recovers - never a poison loop, never silent
  corruption.

### Fixed

- The mtime/size fast path now compares sub-second mtimes. Whole-second
  comparison let a same-second rewrite of a same-size file pass as "already
  in sync" without transferring — a silent wrong-content no-op (protocol
  version bumped to 2; HELLO gained `file_mtime_nanos`). Found by the
  warm-start tests.

### Changed

- CLI polish: `--help` and `--version` (also per-command, `-h`/`-V`), unknown
  flags are now rejected on `send` (a typo'd `--checkum` used to be silently
  ignored, changing sync semantics), pre-flight errors with actionable hints
  (missing source, missing local source, connection refused), an outcome line
  with effective throughput and wall time, `receive <port> 0` binds an
  OS-assigned port and prints it (for scripted runs), and bracketed IPv6
  `[host]:port` parsing on `send` (receive stays IPv4-only for now).
  Sub-second durations render as milliseconds.

## [0.2.0] - 2026-10-22

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

[0.2.0]: https://github.com/bennischober/ffsync/releases/tag/v0.2.0
[0.1.0]: https://github.com/bennischober/ffsync/releases/tag/v0.1.0
