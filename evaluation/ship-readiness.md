# Ship-readiness assessment (2026-10-23)

Scope: a feature-gap and readiness pass over the code as of protocol v4 /
v0.8.0, done alongside the D8 design pass and the D9 closure. Every claim
below was checked against the code in this pass; line references are
evidence, not decoration.

## Verdict

**Good to ship for its niche — with one correctness fix first.** The niche
is two-peer, on-demand sync of big files over fast links (Positioning.md).
The core loop is complete, verified end-to-end, and tested on both primary
platforms. One gap between spec and code (durability of the commit, §8.3)
should be closed before calling it 1.0; two cheap hygiene items should ride
along. Everything else is either deliberately out of scope (D9, documented)
or roadmap work with a recorded decision.

## What works (verified this pass)

- **Core loop end-to-end**: scan (parallel mmap pipeline) → manifest →
  plan → zstd transfer → reconstruction → chunk-root verify → atomic
  rename. Tests: 46 unit, integration, loopback (16), tls (5), quic (5),
  tree (23) — green on Windows and Linux CI.
- **Warm starts (D15)**: validated index, poison-loop guard, orphan
  cleanup, parameter-gated (D19).
- **Tree sync (D17/v3/v4)**: deterministic triage, excludes with negation
  (first-match-wins), parallel sessions (membership protocol), QUIC
  carrier (§17), mode bits (D18), mtime preservation ordering.
- **Resume (D10)**: receiver-authoritative, union manifest, verified by
  disconnect simulation.
- **Security (D16)**: TLS 1.3 + QUIC, mutual TOFU pinning, strict codecs
  (unknown types/flags fail loudly), hostile-peer guards
  (`sane_cdc_params`, §12 resource limits, path-traversal refusal).

## Pre-ship blockers

### 1. The commit is not durable — spec §8.3 is violated (correctness)

**Claim:** the receiver renames the verified temp file over the target
without ever `fsync`-ing it.

**Evidence:** `src/reconstruction/reconstructor.rs:192` (legacy
`finalize`: root/hash check → `drop(self.temp_file)` → `rename`),
`finalize_chunk_root` (same pattern), and both commit sites in
`src/protocol/sync.rs` (`std::fs::rename(&temp_path, &request.path)?`
directly after the VERIFY check). The only `sync_all()` in the crate is
the index writer (`src/index.rs:226`) — the *committed file itself* never
gets one.

**Why it matters:** spec §8.3 says "Match → fsync, atomic rename, send
COMMIT_ACK". Until the rename, the temp file's data lives in the OS page
cache. A power loss (or, on some filesystems, a crash) after the rename
can leave the target with correct metadata but zero/unwritten content —
the classic ext4 rename-reorder failure, and NTFS metadata journaling has
the same shape. The verification we just paid for protects against wrong
bytes, not against *missing* bytes. The spec promised the fsync; the code
must follow the spec (repo doctrine: deviations are protocol bugs).

**Fix (small):** `temp_file.sync_all()` after the VERIFY match, before the
rename, at both commit sites (and in `finalize`/`finalize_chunk_root`).
Cost: one flush of already-written pages, ~ms on the bench host. Optionally
`sync_all` the parent directory after rename on unix for full POSIX
durability (Linux renames are journaled; a directory fsync is the strict
form — Windows needs no equivalent for this guarantee).

## Recommended before 1.0 (cheap hygiene)

### 2. `FLAG_EAGER` is silently ignored (protocol hygiene)

`src/protocol/message.rs:35` defines `FLAG_EAGER` ("reserved, must be 0"),
but HELLO decoding never rejects a peer that sets it — the bit is silently
dropped. The spec (§4, updated this pass) mandates loud rejection while
eager mode is unimplemented: two peers that disagree on DATA timing are a
desync hazard, and silent-bit-tolerance is exactly what the strict-codec
doctrine forbids everywhere else. Fix: reject `flags & FLAG_EAGER != 0` in
`validate_hello` with `ERROR(Version)` (or BadFrame) until §9.2 ships.

### 3. CLI usage string is missing six real flags (UX)

`src/main.rs`'s usage block lists `--checksum`/`--no-delete` (send) and
`--from` (receive) — but not `--exclude`, `--exclude-from`, `--plaintext`,
`--quic`, `--trust <fp>`, `--parallel K`. All six are parsed and shipped;
a new user reading `ffsync --help` cannot discover tree excludes or the
QUIC transport. (The bench harness had the same drift; fixed in PR #4.)
Fix: complete the usage text — no code change.

## Feature gaps vs. the niche (assessed, not all actionable)

| Gap | Assessment |
|-----|------------|
| **Dry-run / preview** (`rsync -n`) | The one genuinely missing UX feature. `tree::triage` is a pure function — a `--dry-run` that prints the triage table (skip/sync/transfer/delete, per-path) without touching the wire is nearly free and directly answers "what would this do to my data?" before the first real sync. Recommended next feature. |
| Bandwidth limit (`--bwlimit`) | Deferred: QUIC/TCP congestion windows already bound harm; niche users park big syncs off-hours. Record as "accepted gap" unless asked. |
| Chmod-only changes don't sync | Deliberate trade (D18/§16.9, same as rsync's fast path) — documented, not a gap. |
| Symlinks/hardlinks/xattrs | Closed with reasoning (D9 amendment, 2026-10-23) — scope reaffirmation, not debt. |
| Daemon / continuous sync | Out of scope since D9 (Syncthing's job) — correct for this niche. |
| mtime-only `--size-only`-style modes | Out of niche; the metadata fast path already covers the common case. |
| Windows long paths (>260 chars) | Unverified this pass. Windows is the primary platform — worth one dedicated test (`\\\\?\\` prefix handling through the tree scan + commit) before 1.0. Not assumed broken; flagged for verification. |
| Open-file sync on Windows (AV lock, media player) | Rename over an open file fails with a sharing violation — loud error, retry works. Known limitation worth a README sentence, not a fix. |
| Hash truncation (§14.5) | Reserved with explicit analysis required — correct to leave. |

## Known security posture (honest limits)

- **TOFU is trust-on-first-use**: a man-in-the-middle present at first
  contact can present its own fingerprint; the mitigation is the
  out-of-band compare (documented in README and enforced by the prompt).
  This is the rsync-over-ssh-first-connection model, chosen deliberately
  (D16); pins make every later session safe.
- **`--plaintext` is plaintext**: LAN/SSH-tunnel use only, by design.
- The hostile-peer guards cover the evaluated surfaces (chunker steering,
  resource limits, traversal, stale-manifest cost model). No gap found in
  this pass.

## Suggested order to 1.0

1. **fsync commit fix** (blocker 1) + eager-bit rejection (item 2) +
   usage string (item 3) — one small PR, fully testable.
2. **`--dry-run`** (the one missing feature worth building).
3. **Windows long-path verification test** (or a finding that it already
   works — either outcome is cheap).
4. Then 1.0: the tool does what its docs promise, on both primary
   platforms, with every deviation from the spec either fixed or recorded.
