# Ship-readiness assessment (2026-10-23, updated after the 1.0-prep work)

Scope: a feature-gap and readiness pass over the code as of protocol v4 /
v0.8.0, done alongside the D8 design pass and the D9 closure. Every claim
below was checked against the code in this pass; line references are
evidence, not decoration.

**Update (same day, after the 1.0-prep implementation):** blocker 1 (fsync)
is FIXED; item 2 (eager-bit rejection) shipped in PR #7; item 3 (usage
string) was re-checked and found already complete — the original claim was
wrong (the six flags are all documented in USAGE); `--dry-run` (D20) is
IMPLEMENTED (single-file + tree, loopback tests + real-TCP E2E showing zero
mutation); Windows long paths are VERIFIED working (315/317-char paths,
byte-identical sync, no `\\?\` handling needed). What remains before 1.0:
the open D8/D7 items (roadmap, not blockers) and a release decision.

**Update (2026-10-24, post-1.0):** v1.0.0 released (tag `v1.0.0`); the
hardening regressions (long-path, real-socket dry-run, parent-dir fsync)
landed in PR #15. **D8 eager mode is IMPLEMENTED and merged** (single-file
PR #16, tree sessions incl. `--parallel` PR #18) after its doctrine cell
measured over-send 0 B in every paired run and eager wall time −18–33% at
0 ms RTT / −12–18% at 30 ms RTT — §9.2/§16.11 carry the numbers, and the
`FLAG_EAGER` hygiene item below is resolved the right way (the bit is
real: opt-in offer, mirrored only on the receiver's own opt-in, loud
single-file decline, lazy tree fallback). The gap table's "Daemon /
continuous sync" and "Bandwidth limit" rows remain accepted gaps.

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

### 2. ~~`FLAG_EAGER` is silently ignored (protocol hygiene)~~ RESOLVED (2026-10-24)

The bit is now real (§9.2/§16.11): the sender offers with `--eager`, the
receiver mirrors only on its own opt-in, eager activates when both peers
set it. Single-file decline fails loudly; tree decline falls back to lazy.
Measured before implementation (D8 doctrine): over-send 0 B, wall time
−18–33% (0 ms RTT) / −12–18% (30 ms RTT).
but HELLO decoding never rejects a peer that sets it — the bit is silently
dropped. The spec (§4, updated this pass) mandated loud rejection while
eager mode was unimplemented — that rejection shipped in PR #7 and the
mode itself in PRs #16/#18 (see the 2026-10-24 update above); the
historical analysis stands as the reasoning for the strict interim
behavior.

### 3. CLI usage string (UX) — CORRECTION: already complete

The original claim that six flags were missing was **wrong** — re-reading
`src/main.rs`'s USAGE block shows all of them documented (`--exclude`,
`--exclude-from`, `--plaintext`, `--quic`, `--trust`, `--parallel`),
including the tree-mode restrictions. Recorded as a lesson: the claim was
made from the scan report without re-reading the file (the same class of
drift the scans exist to catch, in the scan output itself).

## Feature gaps vs. the niche (assessed, not all actionable)

| Gap | Assessment |
|-----|------------|
| **Dry-run / preview** (`rsync -n`) | **IMPLEMENTED (D20, this day)**: single-file (HELLO flag + DRY_RUN_ACK marker, would-change/no-change answers, no temp created, no index written) and tree (TREE_BEGIN bit 6, triage-only, would-delete report). Verified zero-mutation by loopback tests and a real-TCP E2E (stale file survives the preview). |
| Bandwidth limit (`--bwlimit`) | Deferred: QUIC/TCP congestion windows already bound harm; niche users park big syncs off-hours. Record as "accepted gap" unless asked. |
| **Eager diff (D8)** | **IMPLEMENTED (2026-10-24, PRs #16/#18)**: `--eager` single-file and tree (incl. `--parallel`), position-gated (over-send 0 B in every paired run), 18–33% faster at 0 ms RTT / 12–18% at 30 ms. Spec §9.2/§16.11. |
| Chmod-only changes don't sync | Deliberate trade (D18/§16.9, same as rsync's fast path) — documented, not a gap. |
| Symlinks/hardlinks/xattrs | Closed with reasoning (D9 amendment, 2026-10-23) — scope reaffirmation, not debt. |
| Daemon / continuous sync | Out of scope since D9 (Syncthing's job) — correct for this niche. |
| mtime-only `--size-only`-style modes | Out of niche; the metadata fast path already covers the common case. |
| Windows long paths (>260 chars) | **VERIFIED WORKING (this day)**: an E2E tree sync with a 315/317-char file path over real TCP converged byte-identically (SHA-256 match); no `\\?\` handling needed in the tree scan or commit path. |
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

1. **~~fsync commit fix~~ DONE** + ~~eager-bit rejection~~ DONE (PR #7) +
   ~~usage string~~ was already complete (corrected above).
2. **~~`--dry-run`~~ DONE (D20)**.
3. **~~Windows long-path verification~~ DONE (works)**.
4. **~~1.0~~ DONE: released v1.0.0 (2026-10-24).**

## Outlook (2026-10-24): what might come, and what deliberately won't

The core is stable and the feature space is intentionally small — the
product IS the small portable executable. Anything below must pay rent in
that currency (no daemon, no config, no runtime deps, no binary bloat).
Ranked by expected value; everything here is a hypothesis to be decided
by data (the D8 doctrine), not a commitment:

1. **Real-WAN measurement (wan-measure.yml, ready but never run).** The
   3.4×–6.1× `--parallel` numbers and the eager numbers are RTT-modeled
   and loopback-measured; one real uplink run would (a) validate the
   model, (b) give the README honest WAN numbers, (c) possibly reorder
   the next items. Cheapest high-information item; blocked only on the
   four-tunnel setup.
2. **Batched/parallel reconstruction (D7 flaw 2).** Evaluated 2026-10-23
   and rejected on data (coalescing loses on chunk locality); the retry
   shape is vectored I/O or parallel disjoint-offset writes. Only matters
   on fat pipes (≥10 GbE / localhost-class); revisit when a real workload
   shows reconstruction as the bottleneck (the bench already separates
   the phase, so the evidence will show up by itself).
3. **macOS release legs + a Homebrew tap.** The only distribution gap
   that maps to a real user group (macOS has no static-binary story
   today; `cargo install` works but needs a toolchain). Two matrix legs
   (`aarch64-apple-darwin`, `x86_64-apple-darwin`) + a tap repo.
   Add when the first macOS user shows up — not before.
4. **Directory-level mtime fast path.** Trees re-triage on every run
   (cheap, but it walks every path); a directory-mtime short-circuit
   ("no file in this subtree changed" is common in real trees) could make
   huge read-only subtrees O(1). Risky (mtime semantics on dirs differ
   per platform); needs a bench cell first — same doctrine as D8.
5. **Chunk-index reuse ACROSS trees (warm-start second-order).** The D15
   index is per-file; a receiver that synced a tree once has per-file
   indexes, but a NEW receiver (fresh machine) starts cold. A
   receiver-side index snapshot (one file describing the whole tree) that
   a peer can request would turn "first sync to a new machine over a slow
   link" into a delta from the old machine's index. This is the biggest
   remaining UX gap vs Syncthing-style setups — and the only one in this
   list that adds a wire feature.
6. **`--bwlimit`** stays accepted-gap unless a user asks; the congestion
   window already bounds harm.

Deliberately NOT on the list (positioning doc, D9): daemon/continuous
sync (Syncthing's job), cloud remotes (rclone's job), xattrs/hardlinks,
GUI, config files. The niche is on-demand two-peer delta sync; every
addition that blurs that is a feature for a different tool.
