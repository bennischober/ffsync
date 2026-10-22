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

## D13 — Transport plan: plain TCP in v0.1, QUIC as the v0.2 default

**Decision:** v0.1 runs over one plain TCP stream (plaintext, LAN or inside an
SSH tunnel). QUIC — not raw TCP hardening — is the default v0.2 transport plan.

**Reasoning:** QUIC solves problems v0.1 does not have (packet-loss
resilience on lossy WANs via per-stream recovery, 0-RTT reconnects, connection
migration across network changes) and its UDP basis is also the foundation for
NAT traversal in the peer-to-peer direction. The deep-research analysis
(`Deep Research Digest.md`, document 2) quantifies what direct-only
connectivity costs: ~20–25% of internet peer pairs need a relay (symmetric
NATs), so v0.1's TCP/SSH-tunnel scope keeps that entire second project out
until the core engine is proven. Deferring QUIC is a scope decision, not a
rejection; connection migration alone makes it the right fit for the
"friend over the internet" scenario v0.1 cannot serve.

**Wire mapping:** the framing in `concept/Protocol v0.1.md` §3 is
transport-agnostic by construction; §14.4 reserves parallel streams, and the
digest's F3 registers the v0.2 evaluation (quinn vs. iroh vs. ant-quic).

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

## D10 — Resume state: receiver-authoritative, locals-first write order

**Status:** decided on simulation data (2026-10-22).

**Decision:**
1. **Receiver-authoritative:** after a disconnect, the sender keeps no state
   and re-scans its file; the receiver's on-disk state is the single source
   of truth for resume.
2. **The receiver writes local chunk copies BEFORE remote chunks.** This
   ordering is not an implementation detail — it is what makes resume work
   at all: local chunks are ~98% of the file and available offline, so a
   crash leaves a temp file that is mostly real data. The disconnect
   simulation measured the alternatives on the 32 MiB modified scenario:
   - remotes-first (current v0.1 receiver): temp is ~97% zero-filled holes;
     every chunk straddles a hole; the temp re-scan matched **zero** variant
     chunks → resume re-sends the full delta. Resume worthless.
   - locals-first: temp re-scan matches 30–32 MiB; with the union manifest
     (temp chunks ∪ old-file chunks), resume needs **796 KiB at 50–75%
     killed** vs. 1.53 MiB cold restart — a ~2x saving at half the transfer,
     and completion verified end-to-end at every kill rate (25/50/75%).
3. Resume uses the **union manifest**: on reconnect the receiver re-scans
   the temp file and unions it with its old-file manifest. The temp-first
   offset claim keeps each hash mapped to exactly one file. Zero-filled
   holes produce hashes that match nothing, costing at most a re-send of
   hole-adjacent chunks — never corruption (final verification is the
   oracle, unchanged).
4. A write-journal (recording exact written extents) would beat re-scan
   slightly, but re-scan reuses one code path and needs no new state
   format; adopted only if the simulation shows re-scan cost mattering at
   GiB scale.

**Why the old "open question" framing was wrong:** the options were framed
as receiver-manifest vs. symmetric-manifest state. The simulation showed the
real variable is *what is on disk at crash time* — write ordering — which no
manifest design can fix retroactively.

## D11 — mtime/size fast path, with a checksum-only trust mode

**Decision:** by default, sync skips files whose (size, mtime) match on both
peers — rsync's standard semantics, including its mtime trust caveats (FAT
2-second granularity, network filesystem clocks). An explicit `--checksum`-style
mode ignores metadata entirely and always compares BLAKE3 root hashes.

**Reasoning:** the fast path makes "nothing changed" syncs cost zero file I/O,
which matters for repeated syncs and directory syncs later. Because the root
hash falls out of the chunking pipeline for free (D5), the paranoid mode is
also free — users pay only in scan time, never in an extra pass.

**Wire mapping:** a `trust_times` flag in the protocol handshake
(`concept/Protocol v0.1.md` §4). Protocol-level framing decisions (little-
endian framing, exact-match rule) are recorded there and in D12.

## D12 — Exact (hash, length) matching for local chunk reuse

**Decision:** a sender-side chunk is reusable locally on the receiver only on
an exact `(hash, length)` match against a manifest entry. The receiver never
sub-slices or merges manifest entries; chunks that do not match exactly fall
back to remote transfer.

**Reasoning:** content inside a chunk cannot be verified without re-hashing it,
and sub-slicing would reintroduce rsync-style offset matching through the back
door — complexity with no measurable win at megabyte chunk sizes.

**Accepted cost:** after a modification, CDC boundaries shift for roughly one
max-chunk-size until the rolling window re-synchronizes; chunks in that window
over-send even though the bytes exist locally. The over-send is bounded
(~2 × max_chunk_size near modification regions) and correctness is unaffected;
identical CDC parameters on both peers maximize local reuse.

## D14 — Supply-chain hygiene and dependency policy

**Decision:** dependencies are updated promptly (same-week for security
advisories), the toolchain tracks current stable, and `cargo audit` (or an
equivalent OSV check) is part of any dependency work. FastCDC chunk-boundary
behavior is fingerprinted (deterministic `--seed` bench run) before and after
any fastcdc upgrade, because two peers must agree on chunk boundaries for
their hashes to interoperate (D12).

**Reasoning:** the August 2026 crates.io incident (compromised maintainer
account poisoned `arrayref@0.3.10` + two sibling crates with a build-time
dropper, DPRK-linked infrastructure) showed the realistic attack class:
build scripts executing during an innocent `cargo update`. ffsync's exposure
was checked during that response — our tree had `arrayref 0.3.9` (last safe
version) and none of the dropper crates. The hygiene rules derived from that
incident:

- **Build scripts are the attack surface.** Audit them whenever the tree
  changes shape: `cargo tree -e build` must stay small and explicable
  (currently: effectively none — no `build.rs` anywhere in the graph).
- **Lockfile is committed** (`Cargo.lock`); dependency bumps ship as their own
  commits so an audit diff is one commit wide.
- **Prefer fewer, well-audited deps.** `blake3` (dropped `arrayref` in 1.8.7),
  `fastcdc`, `rand`, `tokio`, `memmap2`, `crossbeam`, `num_cpus` — all
  mainstream; resist adding anything exotic (the gRPC/tonic/prost rejection
  in D1's context was also a supply-chain decision, not just complexity).
- **Known advisories in our history** (for the record): RUSTSEC-2026-0097
  (rand unsound with custom logger; fixed by 0.9.3+, we moved to 0.10.3),
  RUSTSEC-2026-0186 (memmap2 unchecked pointer offset; fixed 0.9.11+, we
  moved to 0.9.12), RUSTSEC-2026-0007 (bytes, transitive via tokio; fixed
  1.11.1), RUSTSEC-2026-0204 (crossbeam-epoch; fixed 0.9.20). All resolved
  in the 2026-10 dependency pass; `cargo audit` exits clean.

**Chunking-stability verification:** fastcdc 3.2.1 → 5.0.0 was fingerprinted
before/after with seeded fixtures (30/30 chunks identical, byte-identical
missing-byte counts on all scenarios, all reconstructions verified) —
boundary behavior unchanged. This check is mandatory for every future
fastcdc upgrade; a boundary change would be a wire-protocol compatibility
break (D12) and must ship with a protocol version bump.

