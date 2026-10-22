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

> [!NOTE]
> These results are no real benchmarks but rather observations during development. Actual benchmarks
> may vary based on hardware, file types, and other factors. Benchmarks should also run in isolated
> environments to minimize interference from other processes.