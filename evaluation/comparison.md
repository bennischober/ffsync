# Loopback comparison: ffsync vs rsync vs croc vs sendme

Measured 2026-10-23, Windows 11 host (16 cores, NVMe, NTFS), release builds.
All peers on the same machine (loopback): wall times measure the *pipelines*,
not any network. This complements `rsync.md` (delta-ratio comparison on
single files) and `parallel.md` (simulated-latency parallel cells); the
contribution here is whole-tree wall times against the two common
end-to-end transfer tools, plus ffsync's own TCP vs QUIC carriers.

## Tools and versions

| Tool | Version | Transport used | Config notes |
|---|---|---|---|
| ffsync | 0.5.0 (this repo) | TCP loopback; QUIC (quinn, UDP loopback) | TLS pinned both sides (`--trust`), tree mode |
| rsync | 3.1.3 (Ubuntu 20.04 in WSL2) | daemon mode: TCP loopback *inside WSL*, ext4; pipe mode: local copy | `-a --delete`, daemon on port 8730, module over ext4 (its native FS) |
| croc | 11.5.5 | TCP loopback via self-hosted relay (localhost:9009) | classic mode (persisted consent), `--no-local --yes --overwrite` |
| sendme | 0.37.1 | iroh direct (QUIC-based, `--relay disabled`) | receive wall excludes the sender-side import (measured separately) |

## Fixture and scenarios

`bench --gen-tree --seed 42`: 100 files in 8 directories + 1 empty dir,
423.0 MiB (sizes random per seed: 1–4 MiB small, 20–64 MiB big, ~6% empty
files). Identical bytes for every tool.

- **A fresh** — receiver empty, full transfer. Two iterations.
- **B delta** — receiver keeps A; sender diverged (every 4th file in sorted
  order: in-place ~5% region rewrite + one new 1–3 MiB file; 25 modified,
  25 new, 2.6 MiB rewritten, 472 MiB total) + 8 receiver-only 1 MiB files
  (delete pass). ffsync and rsync receive the delta; **croc/sendme have no
  delta mode — their B cells are fresh re-receives** (marked in the table).
- **C noop** — same transfer again, nothing changed (metadata fast path).

Verification: SHA-256 over the sorted `rel:size:sha256` manifest of every
real file (`.ffsync-index`/`.ffsync-tmp` excluded), computed identically on
the Windows and WSL sides (implementations cross-checked byte-for-byte).
**Every cell below verified OK against the expected tree** — no silent
mismatches.

## Results (wall time, seconds; best per scenario bold)

| Cell | ffsync TCP | ffsync QUIC | rsync daemon | rsync pipe | croc | sendme |
|---|---|---|---|---|---|---|
| A fresh, iter 1 | 3.61 | 4.78 | 2.29 | **1.19** | 4.41 | 123.72 |
| A fresh, iter 2 | 2.62 | 3.81 | 2.37 | **1.16** | 3.15 | 123.76 |
| B delta | 1.74 / 1.17 | 1.92 / 1.35 | 1.71 / 1.76 | **0.33 / 0.32** | 5.79 / 3.95* | 123.87 / 124.41* |
| C noop | 0.04 / 0.03 | 0.03 / 0.03 | 1.31 / 1.29 | **0.09 / 0.10** | — | — |

\* croc/sendme B rows are fresh transfers (they have no delta mode), so
their B numbers compare against other tools' A cells, not B.

## Reading the numbers honestly

- **rsync pipe wins every scenario.** It is a single-process local copy on
  its native filesystem with no TLS, no framing, and no hash verification
  of the result (rsync verifies per-block checksums during transfer, but
  the pipe row is `--whole-file`-equivalent for local transfers —
  see `rsync.md` trap 1). It is also the *only* cell that does not do
  end-to-end BLAKE3 verification like ffsync, and the only one measured in
  WSL/ext4 (its native home) rather than NTFS. Treat it as the
  "no network, no security, no verification" floor.
- **rsync daemon ≈ ffsync TCP** on fresh transfers (2.3 vs 3.6 s): the
  daemon path adds a real TCP loopback connection and rsync's own protocol
  — and still beats ffsync by ~1 s, most of which is ffsync's TLS
  handshake pair + BLAKE3 verification of 423 MiB on the receiver. On the
  delta cell they are equal (1.7 s). ffsync's noop (0.03 s) beats rsync's
  (1.3 s) because the warm-start index (D15) skips the scan entirely;
  rsync still walks the full file list and stats every file.
- **QUIC ≈ TCP** on loopback for full transfers (4.8 vs 3.6 s; ~1 s apart),
  and identical on noop. The gap is UDP loopback + per-call `block_on`
  cost, not congestion control. On lossy/latency-path networks QUIC is
  expected to close or invert this (untested here — see the WAN caveat
  below). **A teardown bug was found and fixed while measuring this
  table**: the sender used to hold the connection for the receiver's
  close (§17.6) while the receiver did the same — a symmetric standoff
  that burned the full 10 s teardown bound on every CLI QUIC session
  (tree and single-file). The library tests never caught it because they
  exercise the state machines directly, not the CLI's §17.6 waits.
- **croc** is in ffsync's ballpark for fresh transfers (3.2–5.8 s
  including its relay setup on every cell — the relay is started fresh per
  cell and torn down after; a warm relay would shave ~1–2 s). It zips
  nothing by default, compresses with zstd, and does no delta transfer.
- **sendme is the outlier (124 s per cell, 423 MiB ≈ 3.4 MiB/s).** Every
  cell pays a fixed ~120 s regardless of scenario — consistent with iroh's
  connection establishment + per-file import/reconstruction overhead, not
  throughput (the transfer itself is QUIC on loopback). The import phase
  is excluded from the receive wall and still costs this much; no
  configuration tried (`--relay disabled`, default relay) changed it.
  Recorded as-is: this is a loopback measurement of the tool's fixed
  overhead, not a claim about iroh in general.
- **Wire volume** (sender-reported): ffsync A transfers 423.0 MiB (chunk
  dedup has nothing to dedup on a fresh tree); B transfers **61.4 MiB**
  (2.6 MiB rewritten + 47 MiB of new files + boundary-shift over-send,
  ~13% of tree size). rsync's B stats (via `--stats`) show a similar
  magnitude (`Total transferred file size` is not wire bytes — trap 2 in
  `rsync.md`; its `sent` bytes row in the raw log is the comparable one).
  croc/sendme re-transfer the full 472 MiB in B.

## Methodology traps (each cost a run to learn)

1. **ffsync writes `.ffsync-index` sidecars next to source files (D15).**
   A shared sender fixture accumulates 100 extra files after one ffsync
   run — every tool must get its own pristine copy, and digests must
   exclude the sidecars.
2. **ffsync tree receive nests the tree under `<root>/<source-leaf>`.**
   Digest the nested path, not the receive root.
3. **`Get-ChildItem -Recurse` streams lazily** — diverging a tree while
   enumerating it picks up the diverger's own new files. Materialize the
   file list first.
4. **rsync's quick check is size+mtime at 1-second granularity.** A
   divergence applied sub-second after fixture creation is legitimately
   skipped. Real edits advance mtime; the harness stamps +1 h to mirror
   that. (`rsync.md` trap 3, rediscovered in tree form.)
5. **rsync client `host::module` defaults to port 873** — the daemon on
   8730 needs an explicit `--port`.
6. **croc prints its banner to stderr**; readiness checks must read both
   streams.
7. **WSL2 kills daemons when the launching `wsl.exe` exits** — start and
   kill the rsync daemon inside each cell's single bash call.

## Threat-model / security comparison (structural, not measured)

| Property | ffsync | rsync | croc | sendme |
|---|---|---|---|---|
| Transport encryption | TLS 1.3 always (TCP: rustls; QUIC: quinn) | none (needs SSH tunnel) | PAKE over TLS (NaCl) | TLS 1.3 (iroh) |
| Peer authentication | mutual fingerprint pinning (TOFU, D16) | none / SSH keys | code phrase (PAKE) | ticket (bearer token) |
| Delta transfer | yes (CDC chunks) | yes (fixed blocks) | no | no (content-addressed dedup across transfers via blob store) |
| Resume after disconnect | yes (D10) | yes (`--partial`) | yes (same-code retry) | no (restart) |
| Receiver-side verification | full BLAKE3 root per file | per-block during transfer | SHA256 per file | BLAKE3 per blob |
| Delete propagation | yes, deferred to session end | `--delete` | no | no |

## Conclusions for the roadmap

1. **The §17.6 sender fix must land before the release** — it is a
   user-visible 10 s penalty on every QUIC session, found only because
   this comparison timed whole CLI sessions instead of library calls.
2. ffsync's niche (fast scans + warm restarts + verified deltas) shows up
   exactly where expected: the noop cell (0.03 s vs rsync's 1.3 s full
   walk) and the delta cell with verification included. The fresh-transfer
   deficit vs rsync daemon (~1.3 s) is mostly TLS + full-content
   verification — the price of D16 and correctness, worth stating rather
   than optimizing away.
3. rsync remains the delta-ratio reference (`rsync.md`); croc is the
   human-workflow competitor (code phrase, no setup); sendme competes on
   zero-config end-to-end encryption but pays ~2 minutes of fixed overhead
   per transfer in this version.
4. WAN numbers are still missing: the planned real-WAN run died on the
   tester's DS-Lite line (inbound IPv4 structurally impossible; confirmed
   via 54 external probe nodes, see the wan-measure workflow history).
   Loopback TCP-vs-QUIC is transport-neutral enough for the carrier
   comparison, but latency-dominated parallel-scaling claims still rest on
   `parallel.md`'s simulated RTT.

## Reproduction

Scripts kept out of the repo (they hardcode local paths); the essential
shape is reproducible from this document plus `bench --gen-tree`:
fixture generation, the divergence rule (every 4th file sorted, 5%
in-place region + 1–3 MiB new file, +1 h mtime stamp), the digest recipe
(sorted `rel:size:sha256`, LF-joined, SHA-256 of that text, sidecars
excluded), and the per-tool flags in the versions table above.
