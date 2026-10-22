# Memory Footprint Evaluation

Measured 2026-10-22, release build (v0.3.0 + the all-Local fix), Windows,
private bytes = heap the process actually owns (working set is dominated by
mmap file pages — file-backed, evictable, shared with the OS page cache, so
not a heap cost). Sampling every 200 ms over live CLI-to-CLI syncs.

## Peak private memory (heap) per sync role

| Scenario | Sender | Receiver |
|---|---|---|
| 1 GiB file, identical (fast path) | 1.9 MiB | 1.7 MiB |
| 1 GiB file, 8 MiB modified (4 096 chunks) | 2.9 MiB | 7.4 MiB |
| Tree, 200 files × 256 KiB (50 MiB) | 4.8 MiB | 2.8 MiB |
| Tree, 2 000 files × 256 KiB (500 MiB) | 4.7 MiB | 4.5 MiB |

## Findings

- **State is O(chunks), not O(bytes)** — the core design claim, now
  measured. A 1 GiB sync runs on ~3–7 MiB of heap because chunk data is
  never copied: the pipeline mmaps the file and hash workers slice it in
  place; channels carry `(offset, length)` (16 B) with backpressure, never
  content; the receiver writes via positional I/O from the mmap or a reused
  scratch buffer. The D15 index adds ~44 B/chunk on disk, not in RAM.
- **Tree memory does not grow with tree size**: 10× the files (200 → 2 000)
  moved receiver peak only 2.8 → 4.5 MiB. Per-file conversations are
  sequential and their per-file structures are dropped between files; the
  only tree-scale structures are the manifests themselves (~90 B/path).
- **Chunk-count stress**: 1 GiB at 16 KiB average chunks (53 821 chunks —
  16× denser than the default 256 KiB) holds the full chunk list in a
  ~3 MiB Vec; with manifest map + plan the structures stay in the
  low-tens of MiB at chunking configurations far more aggressive than the
  defaults. Scaling with file size at fixed chunking is linear in chunk
  count (~130 B/chunk across all structures) ≈ 0.05% of file size.
- **Working set vs heap**: the sender's peak WS (~750 MiB for 1 GiB) is the
  mmap's file-backed pages — evictable under memory pressure, no private
  cost. On 64-bit targets a 70 GiB movie maps fine (lazy paging; the
  striding chunker threads only fault what they hash). 32-bit targets are
  out of scope by design (Windows-primary, 64-bit).

## Leak posture

Each ffsync process runs exactly one sync and exits; classic long-lived
leaks cannot accumulate. Within a run, all hot buffers are reused
(`clear` + `resize` on scratch Vecs), channels are bounded, and the mmap is
dropped with the pipeline. The repeated-sync check (private bytes stable
across fresh processes at 1.7–7.4 MiB depending on role) shows no
run-to-run growth mechanism.

## Caveats

Development machine, warm page cache, Windows-only measurements; Linux
numbers expected in the same order (same structures, musl/glibc allocator
differences are noise at these sizes). The bench harness itself is not
measured — it holds full chunk sets deliberately (it is a measurement tool,
not the shipped sync path).
