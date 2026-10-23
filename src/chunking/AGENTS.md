# chunking — the data pipeline

Splitting and hashing file content: mmap-backed parallel pipeline producing
`(offset, length, BLAKE3 hash)` chunks. Chunk boundaries are the protocol's
delta unit — this module is wire-protocol-relevant (D2, D19).

## STRUCTURE
- `pipeline.rs` — `process_file()` (71): memory-maps the file, one chunker
  thread per core striding 256 MiB regions, hash workers slicing the mmap,
  bounded crossbeam channels, returns `Receiver<ChunkEvent>`.
  `scan_file_ordered()` (43) wraps it with offset-sorted output.
- `chunker.rs` — `Chunker` trait (46); `FixedChunker` (57, baseline) and
  `FastCDCChunker` (82, `fastcdc::v2020`). `cdc_params_for_size()` (28) and
  `sane_cdc_params()` (40): CDC parameters as a pure function of file size,
  hostile-peer guard.
- `hasher.rs` — `Hasher` trait (5); `Blake3Hasher` (14, in-memory + streaming).
- `types.rs` — `Chunk` (5), `ChunkEvent` (12), `chunk_root()` (34): BLAKE3
  root over the chunk list (verification anchor).
- `mod.rs` — curated re-exports; keep the public surface here.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| New chunking strategy | implement `Chunker` in `chunker.rs` | re-export via `mod.rs` |
| Pipeline concurrency | `PipelineConfig` (`pipeline.rs:13`) | |
| CDC policy | `cdc_params_for_size` / `sane_cdc_params` | see ANTI-PATTERNS |
| Chunk integrity | `chunk_root` (`types.rs`) | shared with the D15 index |

## CONVENTIONS
- Work items are `(offset, length)` only — hash workers index the mmap
  themselves. Never put chunk bytes through a channel.
- `process_file` takes a `chunker_factory` closure (each worker thread needs
  its own stateful chunker); the `Hasher` is shared as `Arc` (stateless,
  `Send + Sync`). Preserve this split.
- Threads + crossbeam only — no tokio (D3; async file I/O measured ~5x
  slower, `evaluation/chunking.md`).

## ANTI-PATTERNS (THIS PROJECT)
- Modifying the file while the pipeline holds the mapping. The mmap is
  `unsafe { Mmap::map }` at `pipeline.rs:82` — the crate's only unsafe block
  (known drift: no `SAFETY:` comment at the site yet).
- Hardcoding CDC parameters: they come from `cdc_params_for_size` only (the
  sender-size policy and its HELLO wire contract are the root AGENTS.md's
  CDC gotcha). Files >= 16 MiB must chunk byte-identically to the pre-D19
  defaults — existing large-file indexes depend on it.
- Upgrading fastcdc without the bench fingerprint check: before/after
  `cargo run --release --bin bench -- --quick --seed 42` must produce
  identical chunk counts and missing-byte counts (boundaries are
  wire-protocol-relevant).

## HOTSPOTS
- `pipeline.rs` and `chunker.rs` are the perf-sensitive path; measure against
  the baselines recorded in `evaluation/chunking.md` (mmap/threads evolution,
  warm-index speedups, F4 adaptive-chunking sweep).
- Unit tests are inline (`#[cfg(test)]` in `types.rs`); integration coverage
  rides `tests/loopback.rs` and the bench harness.
