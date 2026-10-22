# rsync comparison notes

First real head-to-head numbers, from the bench harness (`--rsync`, via WSL:
Ubuntu 20.04, rsync 3.1.3, reading fixtures through `/mnt/c`).

## F1: chunk-size sweep (2026-10-22)

Sweep across CDC configurations on the 32 MiB seeded fixtures
(`--sweep --seed 42`; delta % = share of the target file the sender must
transmit):

| min/avg/max | modified | appended | src scan |
|---|---|---|---|
| 16k / 64k / 256k | **2.35%** | 9.21% | 28.1 ms |
| 32k / 128k / 512k | 4.24% | 9.21% | 28.1 ms |
| 64k / 256k / 1m | 4.79% | 9.21% | 27.4 ms |
| 128k / 512k / 2m | 6.11% | 10.62% | 24.3 ms |
| 256k / 1m / 4m (old default) | 11.24% | 9.21% | 23.9 ms |
| 512k / 2m / 8m | 14.34% | 13.72% | 28.7 ms |

Findings:

- Delta % on `modified` scales roughly linearly with chunk size (2.35% at
  64k avg vs 11.24% at 1m avg) — the boundary-shift over-send (D12) dominates
  the delta at large chunks. Smaller is dramatically better here.
- Scan cost is *flat* across the sweep (24–29 ms for 32 MiB): the pipeline is
  not chunk-size-bound at these sizes. The page-cache caveat applies; the GiB
  run should be re-checked, but nothing suggests a cliff.
- `appended` is insensitive to chunk size at ≤1m avg (append-only content
  re-chunks identically after the boundary) and degrades at ≥512k avg.
- **v0.1 defaults picked: 64 KiB / 256 KiB / 1 MiB.** Rationale: ~2.3x less
  over-send than the old default on localized modifications at essentially
  no scan cost, chunk counts stay modest (≈1 chunk per 256 KiB ⇒ a 72 GiB
  file yields ~288k chunks; manifest and plan traffic remain small), and the
  max of 1 MiB caps reconstruction scratch buffers. The deep-research
  recommendation of 32–256 KiB average was directionally right; 256 KiB is
  the sweet spot between over-send and per-chunk overhead in this sweep.

Caveats: 32 MiB is page-cache resident; sweep fixtures are random data
(worst case for compression, best case for CDC boundary stability). Real
media files chunk differently — worth re-running on a real file sample.

## Measurement traps (learned the hard way)

1. **Same-host rsync disables delta transmission.** For local copies rsync
   behaves like `--whole-file` regardless of flags; `-vv` prints
   `delta-transmission disabled for local transfer or --whole-file`. The only
   way to observe rsync's delta behavior on one machine is
   `--no-whole-file` (reported as `delta-transmission enabled`). The bench
   therefore includes a `delta (--no-W)` row.
2. **`Total transferred file size` is not wire bytes.** It is the total size
   of the files transferred — the full file size even when 98% of data was
   matched locally. rsync's wire payload is `Literal data` (the complement of
   `Matched data`); `Total bytes sent` adds file-list and protocol overhead.
   The harness parses `Literal data`.
3. **mtime equality short-circuits everything.** `fs::copy` (CopyFileEx)
   preserves the source mtime; identical size+mtime makes rsync's quick check
   skip the file entirely (`0 B` "transferred" that really means "not
   examined"). The bench backdates the destination mtime before each run.

## First numbers (32 MiB, ~2 × 1% contiguous modification)

| Approach | Wire bytes | Notes |
|---|---|---|
| rsync default / whole-file / checksum | 32.00 MiB | full copy (local transfer, no delta) |
| rsync `delta (--no-W)` | **667.44 KiB** | matched 31.35 MiB (98.07%) |
| ffsync pipeline (FastCDC 256 KiB / 1 MiB / 4 MiB) | **3.13 MiB** | 9.79% missing, dedup 10.2x |

## Interpretation

- On delta ratio for small localized modifications, **rsync's fine-grained
  fixed blocks win**: its block size for a 32 MiB file is a few KiB, so a
  small modification touches only the few blocks containing it. Our ~1 MiB
  average CDC chunks pay the boundary-shift over-send (D12) around each
  modified region — with 4 MiB max chunks, that is up to ~2 × 4 MiB per
  region. This is exactly the trade-off F1 (chunk-size sweep) was registered
  to explore; the data says it matters and smaller average chunks should
  tighten our delta considerably.
- The comparison is **not** apples-to-apples on speed: rsync's 667 KiB came
  after a single-threaded full-file scan (its checksum row took ~0.8 s for
  32 MiB; extrapolated to 72.7 GiB that is the ~15 min cold start from
  `Positioning.md`), while our scan is parallel and disk-bound. The niche
  claim was never "smaller deltas than rsync" — it is "fast scans, warm
  restarts, resumable transfers". Delta ratio at small modifications is a
  dimension where rsync remains strong and where we should improve via chunk
  size, not by abandoning CDC.
- Caveats: `/mnt/c` 9P I/O penalizes rsync's read throughput (fair delta
  ratio, unfair wall times); 32 MiB fits in page cache; rsync 3.1.3 is
  older than current 3.3.x. Treat wall times as indicative, delta ratios as
  structural.
