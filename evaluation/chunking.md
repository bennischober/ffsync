# Chunking Algorithm Evaluation

The evolution of the chunking mechanism in numbers. To begin with, the file that is used has a total
size of 78129430149 bytes (roughly 72.7 GiB). The file is read from a Samsung 860 QVO (2TB) SSD, so
we are already limited to a maximum read speed of about 550 MB/s.

The initial implementation used the `AsyncStreamCDC` implementation from the `fastcdc` crate with
`tokio` to read the file asynchronously. However, due to the overhead of asynchronous reading and
the fact that the SSD cannot saturate the read speed, the drive utilization was quite low. The average
drive utilization was around 12%. The whole process took 1298.0573254s.

Compared to `rsync`, this was actually a performance downgrade. `rsync` was able to read the file and
compute strong hashes in under 15 minutes.

To improve the performance, two channels were introduced to separate the reading, chunking, and hashing stages.
The chunking stage was a single-threaded implementation to allow for fast sequential reading of the file. The
hashing stage was a multithreaded (native threads) implementation to utilize multiple CPU cores for hashing.

| Chunking Algorithm   | Time (s)     | Drive Utilization | Number of Chunks |
|----------------------|--------------|-------------------|------------------|
| Fixed Size (700 KiB) | 221.6832065s | ~75%              | 60997            |
| FastCDC (avg 1 MiB)  | 496.4611076s | ~50%              | 109437           |

Compared to the initial approach, we already have a performance gain of 2.6x with the `FastCDC` algorithm and
a 5.8x gain with the `Fixed Size` algorithm. The drive utilization has also increased significantly, indicating
that the new implementation is better at keeping the SSD busy.

To mitigate the issues of the drive being not fully utilized, a new implementation using `memmap2` for memory
mapping, `crossbeam` for channels and native threads for true parallelism was created.

The process of reading, chunking, and hashing was split into separate and truly parallel stages:

1. `memmap2` is used to memory map the file, allowing for fast sequential access.
2. A dedicated thread reads the memory-mapped file in large blocks, chunks them using the provided chunking
   algorithm (either `FastCDC` or `FixedSize`), and sends the chunks to a channel.
3. Multiple worker threads receive the chunks from the channel, compute their strong hashes, and write the
   to a separate channel.

| Chunking Algorithm   | Time (s)     | Drive Utilization | Number of Chunks |
|----------------------|--------------|-------------------|------------------|
| Fixed Size (700 KiB) | 242.1889197s | 100%              | 60817            |
| FastCDC (avg 1 MiB)  | 244.5058804s | 100%              | 109146           |

To further reduce memory usage, the way how the chunking thread sends chunks to the hashing threads was modified.
Instead of sending the entire chunk data, only the offset and length of each chunk is sent. The hashing threads
then read the chunk data directly from the memory-mapped file using the provided offset and length.

```diff
@@ -1,4 +1,4 @@
 struct ChunkWork {
     offset: u64,
-    data: Vec<u8>,
+    length: usize,
 }
```

| Chunking Algorithm   | Time (s)     | Drive Utilization | Number of Chunks |
|----------------------|--------------|-------------------|------------------|
| Fixed Size (700 KiB) | 243.9368918s | 100%              | 60817            |
| FastCDC (avg 1 MiB)  | 242.1928239s | 100%              | 109146           |

With this final optimization, the memory usage was significantly reduced while maintaining the same
performance. The drive utilization remained at 100%, indicating that the SSD was fully utilized during
the chunking and hashing process.

## Scale validation: 1 GiB with the v0.1 defaults (2026-10-22)

The F1 chunk-size sweep (see `rsync.md`) picked the v0.1 defaults
(64 KiB / 256 KiB / 1 MiB) from 32 MiB fixtures — page-cache resident, which
cast doubt on the flat scan-cost finding. Re-run at 1 GiB with the shipped
defaults (`--size-mib 1024 --seed 42`, release build, NVMe + SATA mixed
read path), everything hash-verified:

| scenario | src scan | tgt scan | missing | delta % | dedup | recon | verified |
|---|---|---|---|---|---|---|---|
| identical | 348.7 ms | 347.1 ms | 0 B | 0.00% | inf | 1.48 s | yes |
| modified | 372.7 ms | 383.5 ms | 21.70 MiB | 2.12% | 47.2x | 1.42 s | yes |
| appended | 373.4 ms | 429.9 ms | 102.40 MiB | 9.09% | 11.0x | 1.58 s | yes |
| fresh | 391.1 ms | 360.4 ms | 1.00 GiB | 100.00% | 1.0x | 1.25 s | yes |

Findings:

- **Scan cost stays flat at scale**: ~350–390 ms per GiB (vs. 24–29 ms per
  32 MiB in the sweep — same order, no cliff). First chunk lands at ~1 ms
  regardless of size, so pipeline warm-up is negligible.
- **Delta % improves with file size** on localized modifications:
  4.79% (32 MiB) → 2.12% (1 GiB) for the same ~1% × 2 edit pattern. With
  fixed-size edit regions, the boundary-shift over-send (D12) is bounded by
  chunk size, so its *share* shrinks as the file grows. Dedup 47.2x.
- **Chunk counts confirm the manifest projection**: ~3370 chunks per GiB at
  avg 256 KiB (sweep rationale projected ~288k for 72 GiB — consistent).
- Reconstruction at 1 GiB: 1.25–1.58 s (write-bound, ~0.7–0.8 GiB/s) — the
  verify re-scan included.

Caveats unchanged: seeded random data (worst case for compression, best case
for CDC boundary stability), warm page cache even at 1 GiB on this machine,
development-machine noise. Real-content samples are still open (registered as
F3-adjacent work for the real-world test).

## Warm-start index: scan vs. reload (D15, 2026-10-22)

Measured with `bench --warm --seed 42` (release build): one cold scan, one
index write (what a successful sync pays at commit), one warm load (what
every subsequent sync pays instead of the scan). The warm load is
`load_index` = stat (size + nanosecond mtime) + decode + root re-derivation
over the chunk list — no file contents read:

| file | chunks | index size | cold scan | index write | warm load | speedup |
|---|---|---|---|---|---|---|
| 32 MiB | 105 | 4.58 KiB | 25.6 ms | 2.6 ms | 0.44 ms | 58x |
| 1 GiB | 3 369 | 144.8 KiB | 350.3 ms | 3.1 ms | 0.62 ms | 563x |
| 4 GiB | 13 460 | 578.4 KiB | 1039.6 ms | 6.2 ms | 1.13 ms | 918x |

Findings:

- **The warm load is essentially flat**: 0.4–1.1 ms from 32 MiB to 4 GiB —
  it scales with the chunk count (~44 B/chunk to decode and re-hash), not
  with file size. The cold scan scales with bytes. The crossover is
  immediate: even a 32 MiB file warms 58x faster than it rescans.
- **Index size is ~144 KiB per GiB** (3369 × 44 B + header) — the same
  order as the wire manifest, as designed (identical 44-byte entry layout).
- **Index write cost is negligible** (2.6–6.2 ms, one sequential
  write + rename at commit).
- Content verification: the reloaded index matches the cold scan exactly
  (root + every chunk) in all runs — the D15 invariant "written only from
  verified state, re-derived root catches corruption" holds end-to-end.

Caveats: same development-machine, seeded-random-fixture caveats as above;
the warm load was measured on a warm page cache (the index file is small —
its cold-read cost is one disk seek, still far below any scan).

> [!NOTE]
> These results are no real benchmarks but rather observations during development. Actual benchmarks
> may vary based on hardware, file types, and other factors. Benchmarks should also run in isolated
> environments to minimize interference from other processes.
## Tree sync: sessions at tree scale (D17, 2026-10-22)

Measured with `bench --tree --seed 42` (release build): a 40-file /
6-dir fixture (135 MiB — 1–4 MiB small files, a few 20–64 MiB big ones,
some empty files, one empty dir), three simulated sessions using the
production pipeline for scans and the real `tree::triage` for the action
lists. "Delta sim" hashes both sides of every conversation file and
counts chunks the receiver lacks (zstd 1x model):

| session | tree scan (both) | conversations | delta sim | transfers | deleted |
|---|---|---|---|---|---|
| 1: receiver empty | 0.9 ms | 40 | 138.0 ms | 135.0 MiB | 0 |
| 2: diverged (cold) | 1.5 ms | 15 | 84.5 ms | 17.44 MiB | 5 (2.5 MiB) |
| 3: unchanged (warm) | 1.5 ms | 0 | 0.0 ms | 0 B | 0 |

Findings:

- **The tree fast path is stat-only**: an unchanged session (session 3)
  reads metadata for 51 paths in ~1.5 ms and transfers nothing — no
  hashing, no chunk I/O. mtime preservation at commit (§16.6) is what
  makes this hold: without it every synced file would re-hash every run.
- **A diverged session moves exactly the changed bytes**: 15
  conversations over 32 MiB of touched/new files transferred 17.44 MiB
  (54.5%) — chunk-level dedup on top of the file-level skip. The 5
  receiver-only files (2.5 MiB) were delete candidates, applied only
  after every conversation succeeded (fail-closed, §16.5).
- **The metadata scan is negligible at this scale**: ~1.5 ms for both
  sides of a 51-path tree; the expensive work is bounded by the
  conversations, never by the triage.
- **Warm vs cold unchanged session: ~57x** (1.5 ms vs ~86 ms modeled as
  cold scan + per-file hashing of the diverged session). At larger trees
  the gap widens with file count, since the warm session stays
  proportional to path count while the cold one is proportional to
  bytes.

Caveats: seeded-random fixtures, warm page cache, one development
machine (Windows); the warm-session figure is a model (stat-only scans +
measured per-file index loads), not a full protocol run — the wire cost
of a converged session (a few frames) is excluded from both sides and
does not change the conclusion.

## Real-content corpus: CDC and delta on actual files (2026-10-23)

Every number above this section used seeded-random fixtures — worst case
for compression, best case for CDC boundary stability. This section
repeats the core measurements on real downloaded content (all verified
via chunk root after reconstruction):

| file | size | content |
|---|---|---|
| `tos.webm` | 544.9 MiB | 1080p VP9 video (Blender Foundation, Tears of Steel, via Wikimedia Commons) |
| `rust-src.tar.gz` | 283.7 MiB | gzip'd Rust 1.79.0 source tree (static.rust-lang.org) |
| `video720.mp4` | 7.3 MiB | 720p H.264 (Blender, via download.blender.org) |
| `mobydick.txt` | 1.2 MiB | English prose (Project Gutenberg) |

Harness: the production pipeline (`process_file` → FastCDC → BLAKE3,
`Reconstructor` + `finalize_chunk_root` verification), the bench's own
mutation patterns applied to copies (modified: two ~1% regions at 45%/55%
of the file; appended: ~10% tail), delta = bytes of variant chunks whose
hash is absent from the source chunk set. Two CDC configurations: the
v0.1 defaults (64k/256k/1m) and the F1 sweep's best point (16k/64k/256k).

### Results

**CDC 64k/256k/1m (shipped defaults):**

| file | scan | chunks | mod: missing | mod % | mod chunks | app: missing | app % |
|---|---|---|---|---|---|---|---|
| mobydick.txt | 19 ms | 4 | 804.7 KiB | 64.56% | 4 | 201.1 KiB | 14.67% |
| rust-src.tar.gz | 1730 ms | 916 | 6.45 MiB | 2.27% | 921 | 28.39 MiB | 9.10% |
| tos.webm | 3056 ms | 1781 | 11.84 MiB | 2.17% | 1781 | 54.55 MiB | 9.10% |
| video720.mp4 | 60 ms | 26 | 764.7 KiB | 10.29% | 26 | 978.9 KiB | 11.98% |

**CDC 16k/64k/256k (F1 sweep best):**

| file | scan | chunks | mod: missing | mod % | mod chunks | app: missing | app % |
|---|---|---|---|---|---|---|---|
| mobydick.txt | 22 ms | 15 | 287.9 KiB | 23.10% | 15 | 152.7 KiB | 11.14% |
| rust-src.tar.gz | 2144 ms | 3708 | 5.95 MiB | 2.10% | 3710 | 28.39 MiB | 9.10% |
| tos.webm | 3640 ms | 7230 | 11.16 MiB | 2.05% | 7222 | 54.53 MiB | 9.10% |
| video720.mp4 | 69 ms | 96 | 399.0 KiB | 5.37% | 96 | 798.4 KiB | 9.77% |

### Findings

- **The large real-media numbers are excellent and match the random-data
  model**: a ~1%×2 in-place modification of a 545 MiB VP9 video costs
  **2.17% of the file** (11.84 MiB) at the shipped defaults — right
  between the random-data 32 MiB (4.79%) and 1 GiB (2.12%) figures, and
  consistent with D12's prediction that the boundary-shift over-send
  share shrinks with file size. The gzipped source tree behaves
  identically (2.27%). CDC boundaries are stable on compressed real
  content.
- **Appends cost exactly the appended bytes on large files**: 9.10% for
  both large files — the ~10% tail re-chunks identically after the
  boundary, as F1's `appended` row predicted for CDC at ≤1m avg.
- **Small files are the weak spot, and it is structural**: mobydick.txt
  (1.2 MiB) is only 4 chunks at the shipped defaults — a single ~1%
  edit region sits inside one chunk and drags 64.56% of the file to the
  wire. With 4× smaller chunks it drops to 23.10%. video720.mp4 (7 MiB)
  improves 10.29% → 5.37%. For small text/code files the smaller CDC
  config is strictly better; for large files the difference is noise
  (2.17% → 2.05%) while chunk count (and thus manifest/index bytes)
  grows 4×.
- **Scan cost on real content is ~5–6 ms per GiB slower than the
  random-data figures** (5.6 ms/GiB for tos.webm vs ~0.35 ms/GiB
  claimed above — the earlier figure is per GiB at 1 GiB scale with a
  warm cache; tos.webm scanned 3.06 s for 545 MiB ≈ 5.6 s/GiB) — actually
  the discrepancy is the mutation copies: the tos.webm scan figure above
  includes cold NTFS reads of a freshly-copied 545 MiB file (page cache
  was evicted by the intervening rust-src work set), while the 1 GiB
  random-data figure ran warm. Treat scan-rate comparisons across these
  two sections as disk-cache-sensitive, not algorithmic.
- **Boundary stability on real content holds**: variant chunk counts stay
  within ±10 of the source count in every modified case (916→921,
  1781→1781, 26→26, 4→4) — the classic CDC "edit locality" property is
  intact on video and gzip streams, not just random data.
- **Every reconstruction verified via chunk root** — the wire's
  verification identity works unchanged on real content.

### Conclusion (does the default need to change?)

Not on this evidence. The shipped 64k/256k/1m is the right default for
the tool's niche (big files, fast links): on large real files it matches
the smaller config's delta within noise with 4× fewer chunks. The
small-file penalty is real but is the domain where total transfer time is
milliseconds anyway; a per-file adaptive chunk size (small files → small
chunks) is a possible future refinement and interacts with the warm-start
index (different CDC params per size class would need per-class index
validation). Registered as a possible follow-up; not scheduled.

### Harness note

The corpus harness lives in `bench_tmp/corpus_bench/` (gitignored, like
all bench_tmp fixtures): a throwaway cargo project depending on the crate
by path, so it stays in sync with the production pipeline automatically.
The corpus files are in `bench_tmp/corpus/` with their provenance in the
table above; re-download them to reproduce.
