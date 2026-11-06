# Changelog

All notable changes to ffsync are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `chunking::REGION_SIZE`: the 256 MiB region size as a public protocol constant.

### Fixed

- **`receive --from <old>` could leave the target unwritten.** When the old file already matched the sender's file (same size and mtime, or same content under `--checksum`), both peers reported InSync and the target was never written. The receiver's HELLO now describes its target, and a `--from` old file never takes the no-rewrite shortcut.
- **Resume after the sender's file shifted.** A leftover temp whose chunks belong at other offsets (bytes inserted or removed between attempts) failed its own read verification, because an earlier write overwrote a chunk that a later one still needed. The attempt was wasted and the temp deleted. Moved temp chunks are now stashed before any write, so the resume converges in one attempt. Temp chunks that are already in place are verified and no longer rewritten.
- A complete resume temp no longer makes the sender report InSync while the receiver commits the temp. Both now report Transferred.

### Changed

- **Scanning small files uses far fewer threads.** Every file scan started one chunker thread and one hash worker per core, so a `--parallel` tree of small files spawned about 2×cores threads per file on every connection. Thread counts now scale with the file size (one chunker per 256 MiB region, one hash worker per 4 MiB), capped at the configured worker count. Chunk layouts are unchanged. In a local measurement, scanning 2000 files of 8 KiB each took 0.5 s instead of 3.5 s.
- The D22 rename census loads and validates each source index once per conversation instead of twice.

### Deprecated

- `PipelineConfig::read_buffer_size` is ignored. The region size is part of the chunk layout (a chunk boundary is forced at every multiple of it), so a non-default value produced chunks that no other peer could match. It is now the fixed protocol constant `chunking::REGION_SIZE`. The field will be removed in 2.0.

## [1.3.0] - 2026-11-06

A hardening and performance release from a full code review. The wire protocol stays at v4 and 1.2.x peers interoperate. One on-disk change: resume temps are now named `<file>.ffsync-tmp`, so a partial transfer left by 1.2.x restarts instead of resuming (its old temp is cleaned up as an orphan).

### Added

- **`receive --bind <ip>`** (default `0.0.0.0`; `--bind ::` for IPv6). The listening line prints the address it actually bound. The QUIC client resolves DNS names and IPv6 literals and binds the matching address family, so it can reach IPv6 peers.
- `protocol::BufStream`: 64 KiB read and write buffers on every connection (control and workers, TCP/TLS/QUIC). Small frames go out as one write instead of three.
- Library additions: `tree::mtimes_match`, `tree::is_sidecar`, `tree::case_fold`, `tree::same_file`, `index::remove_orphan_indexes`, `Reconstructor::set_final_len`, `message::split_data_payload`, and the provided `FileExt` methods `read_full_at` / `read_exact_at` / `write_all_at`.

### Fixed

- **Data loss on case-only renames (D23).** On a case-insensitive receiver (NTFS, default APFS), renaming `README.md` → `Readme.md` (or a directory `docs` → `Docs`) synced the file and then deleted it through its old spelling in the delete pass. The receiver now keeps any target that is the same filesystem entry as a sender path, renames case-variant directories to the sender's spelling, and warns about sender paths that differ only in case.
- **`--eager` deadlock on real transports.** The eager sender wrote DATA while the receiver was still streaming its manifest, which hung large modified files over TCP. Offers are now prepared during the manifest exchange and sent after MANIFEST_DONE, capped at the receiver's 64 MiB buffer.
- **Hostile-peer input.** A HELLO with out-of-range CDC parameters could panic the receiver in release builds. Plan entries are now bounds-checked on arrival, before any allocation is sized from them (length, overflowing offsets, entry count). Sender memory during the manifest exchange stays O(own chunks) whatever the peer streams.
- **Commit metadata (§16.6/§16.9, D18).**
  - Windows receivers marked *writable* sender modes read-only (the mapping was inverted).
  - A read-only sender mode failed the sync: the mode was applied before the mtime, so the mtime write was refused. The order is now mtime, then mode, and both writes are best-effort.
  - Pre-1970 mtimes re-synced on every session.
  - Empty files are committed through temp + rename like every other file.
- **Identity and pins.**
  - Any read error on `identity.bin` other than "not found" (permission denied, sharing violation) silently regenerated the key and broke every peer's pin. It is now reported instead.
  - On unix the key file is created `0600`; it was briefly world-readable.
  - Racing first runs agree on a single identity.
  - `pins.conf` rewrites are durable (fsynced).
  - `untrust` keeps comments and labels.
- **Parallel worker connections (§16.8)** are pinned to the control connection's peer. Another pinned peer can no longer inject conversations into a running session.
- **Parallel temp collision.** `a.mkv` and `a.mp4` shared one temp (`a.ffsync-tmp`), so concurrent workers overwrote each other's temp. The temp suffix is now appended (`a.mkv.ffsync-tmp`), like the index suffix.
- **Tree delete pass.**
  - A directory holding orphan sidecars or excluded files failed the session after a complete transfer. Such directories are now kept instead.
  - A real failure in the delete pass now reports `ERROR` instead of a dangling TREE_END.
  - The delete guard no longer protects a *file* named like an excluded directory pattern.
- **Stuck resumes.**
  - A leftover temp no longer blocks a target that already matches.
  - A resume after the sender's file *shrank* trims the temp before VERIFY.
  - The receiver writes all Local chunks before any Remote chunk (D10 locals-first), so a disconnect leaves a temp that is mostly real bytes.
- **Linux → Windows tree syncs re-checked every file.** NTFS stores mtimes in 100 ns ticks, so comparisons now happen at 100 ns granularity. The D15 index keeps its exact compare.
- `*.ffsync-index-tmp` files (from a killed index writer) leaked into tree manifests and aborted the next session.
- `--dry-run` single-file receives no longer remove orphan indexes (zero-mutation contract), and the dry-run report counts same-second rewrites.
- The index decoder refuses chunk lists that do not partition the file.
- Short positional reads and writes are retried instead of trusted.
- Non-UTF-8 names are skipped with a warning on both peers instead of being renamed lossily.
- The D22 rename census is deterministic and never offers a file that has a conversation in flight.
- The layout-reproduction (InSync) checks compare chunk hashes, and both peers report the same outcome when resume or census entries were streamed.

### Changed

- **Parallel tree sender:** it now pulls from one shared work queue instead of fixed round-robin partitions, so one large file no longer idles the other K−1 connections. Progress counts are session-wide and forwarded live.
- **Performance:**
  - The rename census lookup is O(same-size files) instead of O(files) per conversation.
  - Receiver workers share the peer manifest instead of deep-copying it.
  - Orphan-index cleanup runs once per directory instead of once per file.
  - DATA payloads reuse the frame buffer.
  - The exclude matcher no longer allocates per pattern.

### Deprecated

- `ChunkEvent::Done`: it was never sent, and completion is the channel disconnecting. It will be removed in 2.0.

## [1.2.0] - 2026-10-24

### Added

- **Rename detection (D22):** a file renamed (or moved) within a synced tree is served from the receiver's own bytes — the census of same-size indexed files makes the renamed file's plan all-Local, moving **zero wire bytes** (bench cell: 3.00 MiB → 0 B for a renamed file). No wire change: census sources are receiver-local claims, hash-verified at read; the delete pass removes the old name and its index. Wrong claims cost an ERROR, never corruption.

- **Warm-started --from syncs (D21):** the receiver now indexes the old file (--from <file>) at commit — a repeated --from sync against the same old file skips the receiver's full scan (measured 238.6x on the 128 MiB fixture: 116.5 ms cold scan -> 0.5 ms warm load). The "index snapshot for fresh machines" idea was analyzed and rejected (an index carries no bytes; see D21 in the design ledger). Rename detection (cross-file chunk manifests) is recorded as a future wire feature.

## [1.1.0] - 2026-10-24

### Added

- **Hardening regressions in CI (PR #15):** Windows long-path tree sync (>260 chars) and a real-socket dry-run E2E test run on every push; unix commits fsync the target's parent directory after the rename (strict POSIX durability).

- **Eager diff in tree sessions (D8, §16.11):** `--eager` now extends to tree sync: TREE_BEGIN flag bit 7, mirrored only on the receiver's own opt-in; sequential and `--parallel` conversations all run eager, a declining receiver converges lazily. Fixed on the way: eager DATA now emits Transferring progress (the display/kill logic saw 0 bytes during the pre-plan stream).
- **Eager diff (D8, §9.2):** `--eager` (single-file, both peers) streams chunk DATA during the manifest exchange, bounded by the §6.2 position gate: over-send 0 bytes in every paired bench run, wall time −18–33% at 0 ms RTT and −12–18% at 30 ms RTT (128 MiB modified fixture, 3 paired runs each). A peer that declines the offer fails the session loudly.

## [1.0.0] - 2026-10-24

### Added

- **Dry-run preview (D20):** `ffsync send --dry-run` previews a session with zero mutation — rsync `-n` semantics. Single-file: the receiver answers the plan with NOT_MODIFIED + DRY_RUN_ACK (would change) or COMMIT_ACK (nothing would change), never creating a temp file or index; the sender skips DATA and VERIFY. Tree: TREE_BEGIN flag bit 6 runs triage only and the delete report counts what would be removed. Peers without the feature (≤0.8.0) reject the offer loudly before anything moves.
- **Commit durability (§8.3):** every commit flushes the temp file before the atomic rename (`Reconstructor::sync_temp` plus both protocol commit sites) — a crash can no longer leave a renamed-but-empty target.
- **Strict HELLO flag decoding:** unknown flag bits are rejected at decode (matching TREE_BEGIN's long-standing policy), closing the last silent-bit-tolerance hole.

### Changed

- **D7 flaw 1 — reconstruction I/O:** the reconstructor and the single-file receive loop cache their read-only source handles instead of re-opening the file per chunk: 32 MiB reconstruction ~105 ms → ~50 ms median, 1 GiB −10–15% (Windows bench host, NTFS, seeded fixtures).
- **D7 flaw 2 — coalesced writes:** implemented, measured, and rejected — no gain (NTFS positional writes are already ~µs-cheap; the run-buffer memcpy outweighs the syscall saving). Reverted; the ledger records the data-backed barrier a retry must clear.

### Fixed

- Documentation drift across spec (§3 frame table, §4 flag register, §9.2 eager design, §14 extension-point status), README (status, protocol version, decision-ledger range, known limitations), `src/lib.rs` crate doc, and the bench `--help` string (missing flags, wrong CDC default).
- The mmap `unsafe` block carries its `SAFETY:` justification.
- A flaky dry-run loopback test (same-size files with 100 ns-resolution NTFS mtimes could legitimately take the fast path).

### Removed

- Dead code: `message::write_frame` and `Reconstructor::finalize_chunk_root` (−54 lines; the real verification lives inline in `sync::receive`).

### Notes

- The wire protocol stays at VERSION 4: 1.0.0 is the stability milestone (the 1.x compatibility commitment), not a wire change. Remaining roadmap: D8 eager mode (designed, gated on a bench cell) and D7 flaw 2 (open with a documented retry barrier).

## [0.8.0] - 2026-10-23

### Added

- **Small-file adaptive chunking (D19):** the CDC parameters are now a pure function of the file size — below 16 MiB files chunk with 16 KiB/64 KiB/256 KiB (the F1-sweep best point), at or above with the shipped 64 KiB/256 KiB/1 MiB. Localized edits to small files stop dragging whole large chunks to the wire: the size-ladder sweep (`evaluation/chunking.md`) measured 18.09% → 4.14% delta on an 8 MiB source archive and 8.62% → 3.76% on 16 MiB compressed video, while large files stay byte-identically chunked (existing large-file indexes remain valid). The sender announces the actual parameters in HELLO's existing `cdc_min/avg/max` fields (wire layout unchanged — they were decorative until now, §4.3); the receiver scans its manifest and the VERIFY re-scan with the sender's values (roots can only agree on identical parameters). Nonsense peer values fall back to the local policy. The D15 chunk index (now `FFSYIX02`, +12-byte header) records the parameters that produced its chunk list and serves a warm start only on a parameter match; older `FFSYIX01` indexes are treated as missing state and rewritten at the next commit.

### Fixed

- **Resume commits wrote an empty index:** the resume commit path re-scanned the temp file *after* renaming it away, so every successful resumed sync persisted an empty chunk list and the next sync never warm-started. The verified temp scan is now reused for the index (regression test added).

## [0.7.1] - 2026-10-23

### Added

- **Exclude negation (`!pattern`, §16.7 v0.7.1):** a leading `!` turns an exclude pattern into a re-include; patterns are evaluated in the order given and the first match decides (rsync's model). Identical to the previous any-match behavior for sets without negations, so existing invocations are unchanged. `!!foo` matches a literal `!foo`; the class spelling for a literal `!` member is `[]!]`. The known caveat stands: a negation cannot re-include inside a pruned directory — exclude the children (`dir/*`) instead of the directory (`dir/`), negation first. Wire: TREE_BEGIN flag bit 5, no version bump; sessions without negations are byte-identical to v4 and interoperate with any peer (a peer that cannot evaluate negations rejects the session loudly instead of misreading the pattern).

## [0.7.0] - 2026-10-23

### Added

- **POSIX mode bits in tree sync (§16.9, D18; protocol v3 → v4):** the sender's permission bits (rwxrwxrwx + setuid/setgid/sticky) ride the tree manifest under a new flag and are applied at commit — unix exact (exec bits converge), Windows via the read-only attribute (no-write bits set it, any write-enable clears it; the lossy direction is documented). Mode is never a triage input: a chmod-only change is not synced (the same trade rsync's fast path makes); metadata rides the conversations the size+mtime triage already starts, so the deterministic-triage invariant is untouched. Single-file mode is unchanged; the D15 index codec is unchanged. Old peers fail loudly on the version check (and would reject the unknown flag bit anyway).

### Fixed

- **Directory symlinks/junctions are no longer recursed into during the tree scan.** The scan classified entries by their *target's* metadata; on Windows an NTFS junction needs no privileges, carries the directory attribute, and may point at an ancestor — a plausible infinite recursion/hang. File symlinks are still followed (synced as their target's content); dangling links are skipped with a warning (recorded, not silent).
- **A read-only target file on Windows could not be synced over** (rename and truncate both fail with os error 5). The attribute is cleared best-effort before every commit write; a sender's read-only mode is re-asserted after, so read-only state still converges when the wire carries it.

## [0.6.0] - 2026-10-23

### Added

- **QUIC transport (`--quic`; §17, D13 step 2):** an opt-in transport beside TCP. One QUIC connection carries the whole session — in tree mode the §16.8 worker conversations ride QUIC streams instead of separate TCP connections (K connections become K streams; one TLS 1.3 handshake instead of K). The same D16 identity and pins file are used for both transports (a pinned peer may connect over either); QUIC has no plaintext mode, so `--plaintext` + `--quic` is rejected loudly. TCP stays the default; QUIC targets WAN-shaped paths (stream-level loss recovery instead of TCP head-of-line blocking, connection migration for changing addresses). QUIC requires UDP; a blocked path fails with a bounded handshake timeout naming the fallback. The sync state machines stay synchronous (tokio lives at the transport edge only, D3); the wire bytes are unchanged, so no protocol VERSION bump.

### Fixed

- **QUIC teardown standoff (§17.6):** the CLI's tree sender held its session for the receiver's close while the receiver held it for the sender's close — every QUIC session paid the full 10 s teardown bound. Delivery proof is reading the final reply (stream order guarantees everything before it arrived); the sender now closes right after, and the receiver's bounded wait observes that close in milliseconds. Found by timing whole CLI sessions in the loopback tool comparison, not by the library tests (they drive the state machines directly).

### Documented

- **Loopback tool comparison** (`evaluation/comparison.md`): ffsync (TCP/QUIC) vs rsync 3.1.3 (daemon + pipe) vs croc 11.5.5 vs sendme 0.37.1 on the 100-file / 423 MiB seed-42 tree, scenarios fresh / delta / noop, every cell SHA-256-tree-verified. ffsync's noop cell is 30× faster than rsync's full walk (the D15 warm-start index); QUIC ≈ TCP on loopback; sendme pays ~124 s fixed overhead per transfer in this version.
- **Real-content chunking results** (`evaluation/chunking.md`): the CDC / delta measurements repeated on real media and code (545 MiB VP9 video, 284 MiB gzipped source tree, H.264 clip, prose). On large real files the shipped defaults match the smaller sweep config within noise with 4× fewer chunks; CDC boundary stability holds on video and gzip streams; the small-file penalty is quantified (1.2 MiB text file: one edit drags 64.56% to the wire at the defaults, 23.10% at 16k/64k/256k) and registered as a possible adaptive follow-up, not scheduled.

## [0.5.0] - 2026-10-22

### Added

- **Parallel tree sessions (`--parallel <K>`; §16.8, D13 step 1):** a tree session can run its per-file conversations over K connections at once (2..=16) instead of sequentially on one. The sender partitions its conversation set round-robin; every conversation is announced with a new `TREE_FILE` frame and the receiver validates membership (each path exactly once) instead of wire order. Per-file frames are byte-identical to v3; sequential sessions (default) keep the exact v3 wire layout, so v0.4 peers interoperate unchanged. Worker connections authenticate with the same TLS + fingerprint pinning as the control connection; a failed conversation fails the session and the receiver deletes nothing (fail-closed, as before). Measured with the new `bench --latency` harness (simulated 30 ms RTT, 100-file full transfer): sequential 21.0 s → 6.25 s at K=4 (3.36×) → 3.46 s at K=8 (6.07×). Documented in `evaluation/parallel.md` with the model caveats (a latency model, not a network measurement — trust the K-ratio, not the absolute seconds).

### Changed

- **Friendly release asset names:** release executables are now named `ffsync-vX.Y.Z-windows-x64.zip` / `ffsync-vX.Y.Z-linux-x64.tar.gz` instead of the Rust target triples (`…-x86_64-pc-windows-msvc.zip`, `…-x86_64-unknown-linux-musl.tar.gz`); `.sha256` sidecars follow the archive name. Added `linux-arm64` (aarch64 musl) and opportunistic `windows-arm64` (aarch64 MSVC) builds on GitHub's native arm64 runners.

## [0.4.0] - 2026-10-22

### Added

- **Tree-mode excludes (`--exclude <pat>`, `--exclude-from <file>`; §16.7):** exclude patterns remove paths from a tree session entirely — never transferred, never deleted, never reported (rsync's exclude model). Grammar: `*`/`?` never cross `/`, `**` as a whole segment matches any depth, `[a-z]`/`[!a-z]` classes, a pattern containing `/` is anchored to the tree root, without it matches the basename at any depth, trailing `/` restricts to directories. The **sender's patterns rule the session** (they travel in TREE_BEGIN under a new flag bit, so both peers filter identically and triage stays deterministic); the receiver's own patterns only protect matching paths (and their subtrees) from deletion. An excluded directory is pruned with its whole subtree; no negation syntax (re-includes may come later). Excluded dirs are pruned during scan, so the receiver creates excluded directories it lacks only when the dir itself is not excluded.

### Changed

- **Allocation hygiene in the transfer path:** the zstd compressor/decompressor context is now created once per session and reused across chunks (`zstd::bulk::compress`/`decompress` built a fresh context per chunk), and the receiver no longer clones the full chunk buffer per Local plan entry (the reconstructor's `write_chunk` now takes the data slice directly; `copy_local_chunk` is the explicit read-from-source variant). No wire change.
- New ignored soak tests (`cargo test --test soak -- --ignored`) run 100 sequential sessions in one process under a counting global allocator and fail if net live heap grows — the leak guard for library embedders.

## [0.3.1] - 2026-10-22

### Fixed

- **All-Local plans now execute (single-file sync bug):** when the receiver's file contains every chunk hash of the sender's file at *different positions* (a prefix or shifted region — e.g. sender `pattern × 8192`, receiver `pattern × 4096`), the plan has no Remote entries and the old "nothing to send ⇒ in sync" shortcut errored `HashMismatch` instead of reconstructing. Chunk dedup is by hash, not position; such plans now execute (0 bytes on the wire, chunks repositioned locally). Identical files still report `up to date`.

### Changed

- Release executables are built with a **static CRT** on Windows (`crt-static`): the shipped `ffsync.exe` no longer requires the VC++ redistributable.
- **Tag-driven release workflow** (`.github/workflows/release.yml`): pushing a `v*` tag runs the four gates + a tag↔version guard, publishes to crates.io, builds Windows/Linux executables with SHA-256 sidecars, and attaches them to the GitHub release.

## [0.3.0] - 2026-10-22

### Added

- **Directory-tree sync (D17, protocol v3):** pass a directory to `ffsync send` / `ffsync receive` and the *contents* of the directory sync into the destination directory (`dir` and `dir/` mean the same thing — no rsync trailing-slash semantics). Both peers exchange metadata-only tree manifests (relative path, kind, size, mtime), triage locally and identically, then run the proven per-file machinery sequentially over one connection for exactly the files that need it. Matching files are skipped without a byte of file I/O; nested and empty directories are created; kind conflicts (file vs. dir on the same path) resolve with the sender's kind winning.
- **`--no-delete` (tree mode):** by default the receiver deletes files that exist only locally — the sender's tree is authoritative, like `rsync --delete`. The flag keeps them. Deletions always apply only after every transfer succeeded: a failed session never destroys data.
- **`bench --tree`:** a tree-session simulation in the benchmark harness (multi-file fixture, both sides through the production scan pipeline, real triage, per-file delta measurement, warm vs cold session model).
- **Wire protocol v3:** new frames TREE_BEGIN (0x0C), TREE_END (0x0D), FILE_SYNC (0x0E). v0.2 peers reject v3 loudly (strict framing + version check); single-file sync between v0.3 peers is unchanged apart from the two behaviors below.

### Changed

- **mtime preservation on commit (breaking-ish, §16.6):** after a verified transfer the receiver sets the target file's mtime to the sender's. Without this, tree syncs would re-check every synced file forever; with it, an unchanged tree syncs metadata-only. Single-file syncs benefit the same way (the next run fast-paths instead of re-verifying). Matches rsync's default `--times` behavior.
- **Empty sender files now converge (bug fix):** a zero-byte sender file used to fail with `HashMismatch` against a non-empty receiver file; it now truncates the target to empty (verified against the empty root).

### Fixed

- Tree manifests exclude ffsync's own bookkeeping (`*.ffsync-index`, `*.ffsync-tmp`); syncing them would have synced internal state and desynced warm starts.
- Receiver-only directories are deleted (deepest-first) with their files; directories the sender still has survive even when empty on the receiver.

## [0.2.0] - 2026-10-22

### Added

- **Transport security (D16):** TLS 1.3 (rustls, ring provider) is on by default for both `send` and `receive`; `--plaintext` opts out (LAN use or inside an SSH tunnel). Each peer generates a self-signed certificate on first run (stored in the ffsync state directory) and peers authenticate *mutually* by pinning the certificate's SHA-256 fingerprint: first contact shows the fingerprint and asks `trust this peer? [y/N]` (or pass `--trust <fingerprint>` non-interactively); every later sync verifies silently. A non-matching fingerprint is a loud, terminal rejection before any sync data flows. Manage pins with `ffsync trust <fingerprint>`, `ffsync untrust <fingerprint>`, `ffsync fingerprints` (one plain-text `pins.conf` in the state dir; no usernames, no passwords, no secrets on disk — the SSH `known_hosts` mental model, mutual by default).
- **Persistent chunk index (warm starts, D15):** each synced file gets a sidecar `<file>.ffsync-index` (size, mtime, chunk-tree root, ordered chunk list) written only from verified state at commit time. On the next sync, a peer whose index validates (size + mtime at nanosecond granularity) skips its file scan entirely - repeated syncs of unchanged data become stat-only operations. Corrupt indexes are refused at load (the decoder re-derives the chunk-tree root from the chunk list and rejects mismatches); a stale-but-validating index fails the sync loudly and is invalidated, so a retry cold-scans and recovers - never a poison loop, never silent corruption.

### Fixed

- The mtime/size fast path now compares sub-second mtimes. Whole-second comparison let a same-second rewrite of a same-size file pass as "already in sync" without transferring — a silent wrong-content no-op (protocol version bumped to 2; HELLO gained `file_mtime_nanos`). Found by the warm-start tests.

### Changed

- CLI polish: `--help` and `--version` (also per-command, `-h`/`-V`), unknown flags are now rejected on `send` (a typo'd `--checkum` used to be silently ignored, changing sync semantics), pre-flight errors with actionable hints (missing source, missing local source, connection refused), an outcome line with effective throughput and wall time, `receive <port> 0` binds an OS-assigned port and prints it (for scripted runs), and bracketed IPv6 `[host]:port` parsing on `send` (receive stays IPv4-only for now). Sub-second durations render as milliseconds.

## [0.1.0] - 2026-10-22

First working implementation: single-file sync between two peers over TCP.

### Added

- Chunking pipeline: memory-mapped files, FastCDC content-defined chunking (64 KiB / 256 KiB / 1 MiB defaults, picked by measurement), parallel BLAKE3 hashing with one chunker thread per core; chunks travel as `(offset, length)` and are never copied.
- Out-of-place reconstruction: pre-allocated temp file, positional writes at arbitrary offsets, chunk-tree-root verification, atomic rename commit.
- Wire protocol v0.1: length-prefixed framing with 11 frame types (HELLO, manifest, plan, DATA, VERIFY, COMMIT_ACK, NOT_MODIFIED, ERROR), hand-rolled binary encoding — no protobuf.
- Sender/receiver state machines over any duplex stream: manifest exchange, exact-hash diffing and plan generation (D12), zstd-compressed delta transfer, locals-first write order (D10), mtime/size fast path with `--checksum` escape hatch (D11).
- Resume after mid-transfer disconnects: the temp file is the resume state; on reconnect the receiver re-scans it and streams a union manifest (temp chunks first, then old-file chunks); PLAN_LOCAL carries the chunk hash so local reads are verified against the expected content.
- CLI: `ffsync send <file> <host:port>` and `ffsync receive <file> <port>` with single-line progress display (scan, diff, transfer, verify phases).
- Benchmark harness (`--bin bench`): fixture variants (identical, modified, appended, fresh), link-speed projections, `--sweep` chunk-size sweep, `--disconnect` resume simulation, optional rsync comparison (`--rsync`, native or WSL), `--json` output.
- Loopback integration tests (two peers over in-memory duplex): identical, modified, fresh, fast-path, and disconnect-resume scenarios; plus protocol unit tests and an end-to-end reconstruction test. 21 tests total.
- I/O timeouts on the TCP streams and receiver-side resource limits (plan chunks must stay inside the declared file size).

### Notes

- Windows is the primary development platform; positional I/O is `#[cfg]`-split between Unix and Windows.
- The chunk-tree root (file identity for verification and change detection) is a domain-separated BLAKE3 over the ordered chunk-hash list — not the standard BLAKE3 file hash (its internal tree is over fixed 1024-byte blocks, not CDC chunk boundaries).
- rsync remains faster on delta ratio for small localized edits (fine-grained fixed blocks); ffsync's advantage is the parallel scan (~3.8x cold start on the 72.7 GiB reference file) and resumability.

[Unreleased]: https://github.com/bennischober/ffsync/compare/v1.3.0...HEAD
[1.3.0]: https://github.com/bennischober/ffsync/releases/tag/v1.3.0
[1.2.0]: https://github.com/bennischober/ffsync/releases/tag/v1.2.0
[1.1.0]: https://github.com/bennischober/ffsync/releases/tag/v1.1.0
[1.0.0]: https://github.com/bennischober/ffsync/releases/tag/v1.0.0
[0.8.0]: https://github.com/bennischober/ffsync/releases/tag/v0.8.0
[0.7.1]: https://github.com/bennischober/ffsync/releases/tag/v0.7.1
[0.7.0]: https://github.com/bennischober/ffsync/releases/tag/v0.7.0
[0.6.0]: https://github.com/bennischober/ffsync/releases/tag/v0.6.0
[0.5.0]: https://github.com/bennischober/ffsync/releases/tag/v0.5.0
[0.4.0]: https://github.com/bennischober/ffsync/releases/tag/v0.4.0
[0.3.1]: https://github.com/bennischober/ffsync/releases/tag/v0.3.1
[0.3.0]: https://github.com/bennischober/ffsync/releases/tag/v0.3.0
[0.2.0]: https://github.com/bennischober/ffsync/releases/tag/v0.2.0
[0.1.0]: https://github.com/bennischober/ffsync/releases/tag/v0.1.0
