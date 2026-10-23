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
validation). Registered as F4 — full scope and measurement plan in
"Scope: small-file adaptive chunking (F4)" below.

## Scope: small-file adaptive chunking (F4, 2026-10-23)

The corpus data (above) shows the last known sync-efficiency gap: chunk
count scales with file size, so small files have few, large chunks — and
one localized edit lands inside a single chunk and drags it whole to the
wire. Mobydick (1.2 MiB, 4 chunks at the shipped defaults): 64.56% delta
for a ~1% edit; at the F1-sweep config (16k/64k/256k) it is 23.10%.
video720.mp4 (7.3 MiB): 10.29% → 5.37%. Large files: noise (tos.webm
2.17% → 2.05%). The F2 experiment sharpened the diagnosis: this is a
**chunk-size effect, not a chunker effect** (MinCDC's mobydick win came
from its smaller measured average, 103 KiB vs 311 KiB). So the fix is
not a chunker swap (F2 closed) — it is choosing CDC parameters from the
file size.

### What the codebase already gives us (checked 2026-10-23)

- **The wire negotiation exists and is unused.** HELLO carries
  `cdc_min/cdc_avg/cdc_max` (§4), and the spec already says "the
  sender's (min, avg, max) are authoritative; the receiver SHOULD use
  the same values" (§4.3). But the implementation sends the shipped
  constants on both sides (`sync.rs` HELLO construction) and hardcodes
  the constants in every `scan_with_progress` call site — neither peer
  ever reads the peer's values. The fields are decorative today.
- **Per-file HELLO**: each conversation (single-file mode, and each
  per-file conversation in tree mode) opens with a fresh HELLO, so
  parameters can vary per file without any tree-layer change.
- **The metadata fast path is chunk-free** (size + ns-mtime only), so
  adaptive parameters cannot touch the skip logic.
- **D15 index**: stores the chunk list + root + (size, mtime) but *not*
  the CDC parameters that produced the list (68-byte header, magic
  `FFSYIX01`, no version field).

### Design shape (proposed)

**Parameters are a pure function of the sender's file size.** The sender
derives `(min, avg, max) = f(file_size)` before scanning, puts the real
values in its HELLO (they already ride the wire), and scans with them.
The receiver uses the peer's HELLO values everywhere it scans:

1. its own manifest scan (better hash-match rate — boundary agreement),
2. the temp-file re-scan for VERIFY (§8.2) — **mandatory** to match, or
   the roots can never agree,
3. the resume re-scan (D10) — same reasoning.

No new frames, no flag bits, no version bump: the fields already exist;
their meaning sharpens from decorative to authoritative. This is the
protocol keeping a promise it already made on paper (§4.3's SHOULD).

**The policy (draft, to be fixed by measurement):** size ≥ 8 MiB → the
shipped defaults (64k/256k/1m); below → the F1-sweep config
(16k/64k/256k). Two classes only — the corpus has no data between 7 MiB
and 284 MiB to justify more, and each class boundary is a one-time
re-chunk cost for files crossing it (see below). The threshold needs a
sweep before it is fixed: synthetic sizes at 8/16/32/64 MiB through the
corpus harness, both configs, pick the crossover where the smaller
config's advantage disappears and its chunk-count/manifest cost starts
to dominate.

### Costs and open questions

1. **The class-boundary cliff.** A file that grows or shrinks across the
   threshold between syncs changes chunking entirely: every chunk hash
   changes, the diff sees nothing, the file re-transfers fully once.
   Self-healing at the next commit (the index re-writes with the new
   layout). Bounded and one-time per crossing; acceptable, but must be
   documented (rsync's `--whole-file` boundary behaves similarly).
2. **D15 index must record the parameters.** The warm start serves the
   stored chunk list; if the session's parameters differ from the ones
   that produced the stored list, hash-match rates degrade (correct but
   wasteful) — and worse, the sender's root and the served root can
   diverge for identical content, so the NOT_MODIFIED-adjacent root
   comparison must never mix layouts. Cleanest: extend the header with
   the three parameters (12 bytes) and bump the magic to `FFSYIX02`;
   a warm start serves only when the stored parameters equal the
   session's (else cold scan — correct, simple, and the orphan-cleanup
   naming convention is untouched). This is the same discipline as the
   chunker-swap rule (boundaries are wire-protocol-relevant).
3. **Which size drives the receiver's parameters?** The *sender's*
   file size (from HELLO), not the receiver's own — the sender's chunk
   layout defines the plan and VERIFY. Both peers then scan identical
   content with identical parameters. (Deriving the function on both
   sides from the size would also work, but sending the values is more
   robust — no "both sides must run the same policy version" invariant.)
4. **The fast-path/warm interaction stays intact**: the fast path
   (metadata-only) fires before any chunking; a warm index whose
   parameters match the session's is layout-identical to a fresh scan.
5. **Boundary-fingerprint discipline**: after the change, files above
   the threshold must chunk *byte-identically* to the shipped defaults
   (existing large-file indexes stay valid); files below move to the
   smaller config — the bench fingerprint runs must therefore be
   per-config, not global.

### Measurement plan (before implementation)

1. Corpus harness at intermediate sizes: 8/16/32/64 MiB (synthetic,
   both mutation patterns) × both configs → pick the threshold where
   small-chunk advantage vanishes. Expect the crossover between 8 and
   64 MiB (video720 still wins 1.9× at 7.3 MiB; tos.webm is noise at
   545 MiB; nothing between is measured).
2. Manifest/index-bytes accounting per config at the candidate
   threshold (44 B/chunk on the wire and in the index): confirm the
   small-file classes do not inflate tree-mode manifests meaningfully
   (a 1000-file tree of ~100 KiB files at 16/64k/256k ≈ 44 B × 7
   chunks × 1000 ≈ 300 KiB total — fine).
3. Implement behind the existing HELLO fields + index magic bump;
   re-run the bench fingerprint per config; full local gates; Docker
   Linux leg.

### Honest sizing of the win

Small files are milliseconds on loopback; the delta improvement is
visible only on slow links or many-file trees with per-file round trips
already dominating. This closes the *last known delta-efficiency gap*
(the corpus conclusion's registered follow-up) — it is a 0.8.0-scale
polish, not a game changer: mobydick 64.6% → 23.1% wire bytes (2.8×),
video720 10.3% → 5.4% (1.9×), large files unchanged. The implementation
is small (thread HELLO params through the scan sites, index header
bump, policy function); the measurement to place the threshold is most
of the work.

### Harness note

The corpus harness lives in `bench_tmp/corpus_bench/` (gitignored, like
all bench_tmp fixtures): a throwaway cargo project depending on the crate
by path, so it stays in sync with the production pipeline automatically.
The corpus files are in `bench_tmp/corpus/` with their provenance in the
table above; re-download them to reproduce.

## Alternative chunkers on our scenarios (F2, 2026-10-23)

The vendor benchmarks for SeqCDC / MinCDC / MothCDC measure **corpus dedup
savings at ~8 KiB chunks on VM images, source archives, and Wikipedia
text** — not ffsync's workload (pairwise delta between two versions of one
file at ~256 KiB chunks, on media). This section re-benchmarks them on our
corpus with our metrics, at matched average chunk size. The mutation
patterns, corpus, and delta definition are identical to the section above
(two ~1% middle regions at 45%/55%; ~10% tail; delta = target chunk bytes
whose hash the source lacks).

Configs (measured average chunk size in parentheses; the vendor chunkers
have no avg parameter — MinCDC's distribution is uniform in [min, max]):

- `fastcdc 64/256k/1m (shipped)` — the baseline (313 KiB)
- `fastcdc 16/64k/256k` — the sweep's best point (77 KiB)
- `fastcdc 192/256/320k (narrow ctrl)` — control separating "narrow range"
  from "uniform distribution" as the cause of any delta movement (296 KiB)
- `mincdc 64/448k (uniform)` — uniform, matched to shipped avg (278 KiB)
- `mincdc 192/320k (narrow)` — vendor-recommended narrow shape (256 KiB)
- `mothcdc 64/448k` — MinCDC + caterpillar coalescing (278 KiB)
- `seqcdc thr=5 224/448k` — SeqCDC's best shot at our scale (226 KiB)
- `seqcdc thr=32 64/448k` — shipped-threshold sensitivity probe (447 KiB)

Chunking throughput (MiB/s, single thread, in memory, boundary generation
only — hashing untimed, matching the vendor benches' methodology):

| file | fcdc shipped | fcdc narrow | mincdc wide | mincdc narrow | mothcdc | seqcdc t5 | seqcdc t32 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| rust-src.tar.gz | 2793 | 6329 | 9754 | 24078 | 9797 | (142086)* | 2720 |
| tos.webm | 2826 | 6291 | 10146 | 24418 | 10036 | (158014)* | 2723 |
| video720.mp4 | 3019 | 6367 | 9717 | 19679 | 11639 | (166799)* | 2833 |

\* Not a real scan speed. SeqCDC's cut rule is `restart + min + first
rising byte-run of threshold length` with no content anchoring — on
high-entropy data a rising run appears almost immediately after `min`, so
the boundary lands at `min + ~2 KB` jitter (measured avg 226 KiB) and the
scanner only *examines* ~1% of the bytes. That is near-fixed-size chunking
at min, and the delta numbers below prove it is not achieving FastCDC-like
locality. At thr=32 the rising run essentially never occurs and cuts land
at max (447 KiB = fixed-size chunking), giving honest FastCDC-comparable
throughput (2720 MiB/s).

Delta % (what the wire would carry; lower is better):

| file | fcdc shipped | fcdc narrow | mincdc wide | mincdc narrow | mothcdc | seqcdc t5 | seqcdc t32 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| rust-src mod | 2.27 | 3.11 | 2.39 | 2.91 | 2.39 | 13.99 | 2.47 |
| tos.webm mod | 2.17 | 3.00 | 2.14 | 2.41 | 2.14 | 5.93 | 2.17 |
| video720 mod | 10.29 | 52.16 | 12.94 | 15.08 | 12.94 | 48.92 | 12.06 |
| mobydick mod | 64.56 | 51.35 | 18.43 | 31.14 | 18.43 | 36.72 | 35.94 |
| appends (all) | 9.10 | 9.11 | 9.14 | 9.13 | 9.14 | 9.10 | 9.12 |

Chunk counts (tos.webm, 545 MiB): fastcdc 1781, mincdc wide 2001, mincdc
narrow 2177, mothcdc 2001, seqcdc t5 2460, seqcdc t32 1246.

Appends: on large files every algorithm and config costs exactly the
appended bytes (~9.1%) — the universal CDC property on tail growth,
confirmed across all eight configs. On the 1.2 MiB file the appended-chunk
boundary shift dominates (FastCDC 14.67%, MinCDC 36.93%) — again the
small-file chunk-size effect from finding 4, not a chunker-quality
difference.

### Findings

1. **MinCDC is the only real finding**: 3.6× FastCDC's throughput
   (9.7–10.1 GiB/s vs 2.8) at **equal delta quality** on large media (2.14%
   vs 2.17% on tos.webm, within noise of each other and of the shipped
   default). Its uniform distribution does not measurably change edit
   locality; the narrow-range control shows the narrow configs' delta
   regressions come from the range, not the distribution.
2. **MothCDC is byte-identical to MinCDC on our data** — records == chunks
   on every real file (caterpillar fires only on the synthetic zero file:
   1024 chunks → 1 record). Its extra speed claim (~15.7 GiB/s on zeros)
   applies to a data shape ffsync does not target; on our data it is MinCDC
   with no advantage. Correct per its own docs ("no-op on data with no
   runs").
3. **SeqCDC fails the sync metric** on high-entropy content: 2.7–6× worse
   delta on the movie (5.93% vs 2.17%) and the archive (13.99% vs 2.27%),
   and the video720 mod case (48.92% vs 10.29%) shows the predicted
   cascade: its cut decision is relative to the chunk *start*, so an
   in-place edit re-anchors every subsequent boundary. Its throughput
   number is an artifact of not reading most of the data (see table note).
4. **Small files**: MinCDC's uniform distribution *halves* mobydick's delta
   (18.43% vs 64.56%) — but this is the same small-file effect already
   documented above (few chunks = one boundary shift dominates); mincdc's
   smaller average (103 KiB measured, vs FastCDC's 311 KiB skew at this
   file size) is the cause, and the `fastcdc 16/64k/256k` config got the
   same improvement (23.10%) with a proportionally smaller chunk size. Not
   a chunker win — a chunk-size win, already registered as a possible
   adaptive-chunking follow-up.
5. **Chunk counts** at matched average are comparable (mincdc 2001 vs
   fastcdc 1781 on tos.webm; the vendor tables' "FastCDC makes fewer
   records" reflected their 8 KiB targets, not our scale).

### Verdict (F2 closed)

No integration. The throughput argument evaporates end-to-end: our scan is
disk-bound cold (100% drive utilization measured above) and the scan is a
few percent of sync wall time (comparison.md cells); MinCDC's 3.6× would
save ~0.1 s on the measured loopback cells. The delta argument goes the
wrong way for SeqCDC and is a wash for MinCDC. MinCDC remains the tracked
candidate **if** a fast-NVMe/warm-rescan scenario ever becomes the
bottleneck (its correctness story is solid: SIMD argmin with a scalar
oracle, property-tested against streaming equivalence) — but the swap
would still need the boundary-fingerprint + D15 index-version discipline
from the top of this document. SeqCDC and MothCDC are ruled out on our
metrics.

### Harness note (cdc_exp)

The experiment harness lives in `bench_tmp/cdc_exp/` (gitignored): a
throwaway cargo project depending on `ffsync` by path plus
`mothcdc 0.7.2` / `seq_chunking 0.1.0`. It reuses corpus_bench's mutation
patterns and delta definition; reconstruction is exercised via the real
`Reconstructor` on files < 32 MiB (disk path is chunker-independent). The
vendor chunkers have no avg parameter; configs are matched on the
*measured* average chunk size, reported per config above. Run:
`cargo run --release` inside `bench_tmp/cdc_exp`.
