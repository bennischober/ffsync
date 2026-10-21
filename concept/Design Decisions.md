# Design Decisions

Recorded decisions, their reasoning, and the alternatives considered. This is
the long-term memory of the project: new contributors (human or AI) should be
able to reconstruct *why* the tool looks the way it does without re-deriving the
arguments. Add entries chronologically; never delete, only supersede with a new
entry.

## D1 — Own the protocol; do not build on rsync

**Decision:** ffsync implements its own chunk-level sync protocol instead of
wrapping or extending rsync.

**Reasoning:** rsync's transport and its delta algorithm are one thing — there
is no interface to feed it externally computed chunk hashes. All realistic
wrapper variants (pre-hash + `--whole-file`, librsync, fpsync, patching) fail
for the reasons in `Positioning.md`. The parts we would want from rsync
(crypto, compression, atomicity) are all available as off-the-shelf components.

**Alternative rejected:** "Speed up cold start ourselves, use rsync as
transport" — not composable.

## D2 — CDC + BLAKE3 instead of rsync-style rolling checksums

**Decision:** Content-defined chunking (FastCDC) with BLAKE3 content hashes is
the matching mechanism, not offset-based fixed blocks with rolling checksums.

**Reasoning:**
- Parallelizable: CDC boundaries can be computed in independent regions, and
  BLAKE3 is SIMD/many-core accelerated. Measured ~3.8x faster cold start than
  rsync on the same file, disk-bound rather than CPU-bound
  (`evaluation/chunking.md`).
- Content addressing makes chunk identity position-independent, which rsync's
  offset-based block matching cannot do. Consequences:
  - Resume after disconnect is nearly free: the receiver's chunk manifest *is*
    the resume state. rsync `--partial` only handles append-style transfers.
  - The BLAKE3 root hash doubles as change-detection (see D5) and end-to-end
    verification with no extra pass.

**Trade-off accepted:** after the first modified byte, chunk boundaries shift
between the two versions until the rolling window re-synchronizes (~max chunk
size). Offset-based speculation ("stream the head of the file, it is probably
identical") is therefore unsound; only hash matching is correct. rsync lives
with the same property; it is inherent to delta sync, not a design flaw.

## D3 — Threads + crossbeam, not tokio, for the data plane

**Decision:** chunking/hashing/reconstruction run on native threads with
crossbeam channels. tokio only wraps `main`.

**Reasoning:** measured in `evaluation/chunking.md`: the initial async
implementation took 1298 s for the 72.7 GiB file with ~12% drive utilization;
the mmap + native-thread pipeline took ~242 s at 100% utilization. Async reads
were ~5x slower for this workload. tokio remains in the dependency tree for the
future network layer, where it fits.

**Consequence for the protocol:** the wire layer must bridge native-thread
pipelines into async I/O at the edges, not pull the data plane into async.

## D4 — Chunks are metadata-only over the wire and in memory

**Decision:** chunk data is never copied or sent through channels — only
`(offset, length)`. Hash workers index the mmap directly. Hashes are content
addresses; data is only moved when the transfer layer needs it.

**Reasoning:** sending whole `Vec<u8>` chunks through the queue ballooned
memory for no gain (see the diff in `evaluation/chunking.md`). This invariant
also keeps the door open for zero-copy sends from the mmap during transfer.

## D5 — Checksum-based change detection is a first-class mode

**Decision:** the BLAKE3 root hash of a file serves simultaneously as (a) the
Phase-2 identity check, (b) the end-to-end integrity verification after
reconstruction, and (c) a "trust mode" equivalent of rsync's `-c` /
`--checksum` flag. Progress and phase reporting (chunks hashed, bytes matched
vs. missing, effective throughput) is part of this decision: the pipeline
already produces these signals, and the tool must surface them — silent
hashing was the original motivation for this project.

**Reasoning:** rsync's default mtime/size change detection is a heuristic that
can miss changes (or force pointless transfers). rsync users pay a full extra
scan for `-c`. Because ffsync's root hash falls out of the chunking pipeline
for free (BLAKE3's Merkle tree gives the root while chunking), full-content
change detection costs nothing extra. Planned CLI semantics:

- default: mtime/size fast path when metadata matches (Phase 1 buckets), root
  hash when it does not
- `--checksum`-style mode: always compare root hashes, ignore metadata

**Open detail:** persisting root hashes/chunk manifests per file is what makes
repeated syncs free (warm start), but a stale index that claims "I have this
chunk" breaks reconstruction. Validation strategy (mtime heuristics vs. cheap
re-verify vs. trusting finalize-time verification) is an open design question —
see `Positioning.md` and the roadmap.

## D6 — Out-of-place reconstruction with atomic commit

**Decision:** reconstruction writes to `<target>.ffsync-tmp`, pre-allocates the
full size, writes chunks at arbitrary offsets in any order, verifies the root
hash, then atomically renames.

**Reasoning:** from `File Reconstruction.md`. Out-of-order writes are safe
because every chunk lands at a pre-known offset; the final hash is the
correctness oracle; the rename is the atomicity boundary. On hash mismatch the
temp file is deleted and the original untouched.

**Known deviation from the concept doc:** the temp path is derived with
`Path::with_extension("ffsync-tmp")`, which *replaces* the last extension
(`a.mkv` → `a.ffsync-tmp`) rather than producing the dotfile
`.a.mkv.ffsync-tmp` from the concept doc. Accepted for now; revisit when the
network layer lands (a hidden-file convention may be preferable).

## D7 — Reconstruction I/O will need batching and parallelism (known flaw)

**Decision:** accept the current single-threaded `Reconstructor` for now; do
not optimize before the benchmark shows it matters.

**Reasoning:** two separate flaws are known:
1. `write_chunk` opens the local source file once *per chunk* (~60k opens for
   72 GiB at 1 MiB chunks). Fixable with a cached handle; the `Send` concern
   that motivated the per-chunk open can be solved with a thread-local or
   caller-owned handle.
2. Writes are serialized on one thread. Adjacent chunks should coalesce into
   contiguous range writes; disjoint-offset writes parallelize safely.

The harness measures reconstruction time separately, so this decision gets
revisited on data, not vibes. On slow links it is irrelevant; on fat pipes it
becomes the next bottleneck after the scan is fixed.

## D8 — The link speed is a protocol parameter

**Decision:** the diff strategy must adapt to the link, not be fixed.

**Reasoning:** two extreme strategies exist:
- **Lazy diff:** receiver completes its scan, sends the full chunk list, sender
  replies with exactly the missing chunks. Zero wasted bytes, one scan-length
  latency before any transfer.
- **Eager diff:** hashes stream both ways while scanning; the sender queues
  chunks the moment they are missing from the receiver's streamed set. Bytes
  start moving earlier, but a late-arriving receiver hash may match something
  already sent — a bounded over-send.

On a slow link, over-sending 10% of 72 GiB is fatal → lazy. On a fat pipe,
redundant bytes are nearly free → eager. The protocol design (open) must expose
this as a knob, not pick one globally.

## D9 — Scope discipline: what ffsync deliberately does not build

**Decision:** v0.1 target is a single file pair between two peers over TCP with
zstd compression. Explicitly out of scope until the core is proven:

- metadata/permissions/xattrs/symlinks/hardlinks
- directory recursion (Phase 1) and the full four-phase workflow
- auth (use rustls or an SSH tunnel), key management
- daemon mode / continuous sync (that is Syncthing's job)

**Reasoning:** `Positioning.md`; the scary surface of "building your own
rsync" is entirely in the deferred list. The core loop (scan → diff → transfer
→ reconstruct → verify) is a few hundred lines on top of the existing pipeline.

## D10 — Resume state: open question (receiver-authoritative is the leaning)

**Status:** undecided, leaning receiver-authoritative.

The receiver's chunk manifest is the natural resume state (see D2). But the
Phase 3 concept has *both* peers streaming hashes from unfinished scans, so a
mid-phase disconnect leaves partial state on both ends. Options:

- **Receiver-authoritative:** receiver persists its manifest; sender re-scans
  on reconnect. One source of truth, simpler staleness reasoning, slower
  reconnects.
- **Symmetric:** both peers keep manifests. Faster reconnects, but a two-party
  staleness problem.

A mid-transfer disconnect simulation (planned harness extension) should be
built before deciding.
