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

> **Amendment (2026-10, v0.2 work):** QUIC is *deferred past* v0.2; the
> v0.2 security layer is TLS-over-TCP per D16. Two developments changed the
> calculus: (1) the persistent index (D15) landed first, making repeated
> single-file syncs cheap over plain TCP, so the QUIC motivation shifted
> entirely to multi-file sessions (parallel streams, migration) — which do
> not exist yet; (2) the implementation survey (verified 2026-10): quinn is
> pure Rust on rustls + tokio; quiche (Cloudflare) hard-wires BoringSSL (C,
> cmake build) — rejected on D14 grounds; a moq-dev fork with a rustls
> backend is not upstreamable. **Plan:** rustls-over-TCP now (same crypto
> design D16 specifies; works for 1:1 file sync indefinitely), revisit
> quinn when directory sync exists — at that point tokio enters only at the
> transport edges (the mmap/thread data plane stays native, per the
> measured 5x regression of async I/O in `evaluation/chunking.md`), and
> parallel per-file QUIC streams become the optimization worth building.
>
> **Second amendment (2026-10-22, v0.5 work):** the parallel-streams half
> of D13 landed WITHOUT QUIC and without tokio (§16.8, `--parallel K`):
> K connections between the same peers, one native thread per connection,
> each running the byte-identical v3 per-file flow; the conversation set
> is membership-validated (TREE_FILE frames) instead of order-validated.
> Measurement first, per this decision's own discipline — `bench
> --latency` (in-process duplex, each direction delayed RTT/2, 100-file
> full transfer, receiver empty): sequential 21.0 s → 6.25 s at K=4
> (3.36×) → 3.46 s at K=8 (6.07×) at 30 ms simulated RTT. The dead air is
> exactly the serialized round trips the amendment predicted. QUIC
> (quinn) remains deferred as *step 2*: the parallel-session layer is
> transport-agnostic (worker connections are injected closures), so a
> quinn stream substitutes a connection without touching tree_sync's
> logic. The tokio-at-the-edges question moves with it.
>
> **Third amendment (2026-10-23, v0.6 work):** step 2 lands — QUIC
> (quinn) as an opt-in transport beside TCP, spec §17. The evidence
> ladder from `evaluation/parallel.md` settled what step 1 left open:
> parallel conversations are worth their carrier (real-TCP and real-WAN
> cells), K=4 is the practical sweet spot, and the remaining transport
> wins are exactly QUIC's: K handshakes collapse to one, stream-level
> loss recovery replaces the TCP head-of-line block, connection
> migration covers the mobile-peer case. **Tokio enters the tree at the
> edges only** — the D3 re-evaluation this decision pre-registered:
> quinn is async, so `transport::quic` owns a small runtime driving
> endpoint machinery; the public seam stays synchronous (a
> `QuicStream: Read + Write + Send` adapter, block_on at the boundary);
> `ffsync::protocol` must not depend on tokio; the data plane (mmap,
> native threads, crossbeam) is untouched — the async-file-I/O
> regression of v0.1 (`evaluation/chunking.md`, ~5×) is NOT repeated.
> QUIC has no plaintext mode (TLS 1.3 is transport-mandated), so
> `--plaintext` stays TCP-only; the D16 TOFU pinning model maps 1:1
> onto quinn's TLS stack (same identity, same pins set, both
> transports). TCP remains the default; `--quic` is per-invocation.

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

## D15 — Persistent chunk index: sidecar files, content-verified, written at commit

**Decision (v0.2):** each synced file gets a sidecar index file
`<target>.ffsync-index` storing the state that made the last successful sync
work: the file's identity (size, mtime at commit, chunk-tree root) and its
full ordered chunk list (`(hash, offset, length)` entries, the same 44-byte
layout as the wire manifest). Both sides write their index at commit time
(sender: after scanning its source, gated on VERIFY success; receiver: after
the atomic rename). The index is *validated, not trusted*: a warm start uses
the index only after (a) the file's size matches the index and (b) the mtime
matches; if either check fails — or the index is missing/corrupt — the peer
falls back to a full cold scan, exactly as today. Additionally, the
receiving side never *writes* from the index without verifying against the
plan's expected hash (already the case via PLAN_LOCAL's hash field, D12), so
a stale index costs re-sends, never corruption. The end-to-end VERIFY (§8)
remains the final oracle in all cases.

**Index format:** plain binary, little-endian, no serde: 8-byte magic
(`FFSYIX01`), file size u64, mtime u64 (unix secs) **+ u32 sub-second nanos**
(the nanoseconds are load-bearing: whole-second mtimes let a same-second
rewrite validate a stale index and silently sync old content — found by the
warm-start loopback test; the same finding forced sub-second mtimes onto the
§4 wire fast path, see below), chunk-tree root 32 B, chunk count u64, then
`count ×` (hash 32 B, offset u64, length u32). Same hand-rolled style as the
wire framing (D1). `decode` self-verifies: it recomputes `chunk_root` from
the decoded chunk list and refuses on mismatch, so corruption in the root or
any entry is caught at load time (O(chunk count) in-memory hashing, no I/O).
Corrupt/truncated index ⇒ treat as missing.

**Index format v2 (`FFSYIX02`, v0.8.0/D19):** 12 bytes appended to the
header: the CDC parameters (min u32, avg u32, max u32) that produced the
chunk list. A warm start serves the stored chunk list only when the stored
parameters equal the session's (the sender's size-derived values); a
mismatch falls back to a cold scan — correct (chunk hashes are content
hashes regardless of layout) but wasteful, so the index prefers honesty
over stale service. A v1 index (no parameters stored) never validates
against a v2 reader — the magic differs, so old indexes are simply
missing state and are rewritten at the next commit. The self-verification
re-derives the root from the chunk list unchanged.

**Reasoning:** the positioning doc calls the persistent index the core product
insight — cold starts repeat, and rsync is amnesiac. The measured scan cost
(~350–390 ms/GiB, `evaluation/chunking.md`) bounds what the index saves: on
repeated syncs of unchanged data, both peers go from a full scan to a stat +
mtime/size check, and a *changed* file still needs its cold scan (the index
of the old version is stale by definition — but the *sender* can keep the old
index to skip... nothing; see Accepted costs). The index also carries Phase-2
style root-hash change detection forward: mtime/size match ⇒ presumed
unchanged (D11); index root can answer "would this sync do anything?" without
reading file contents.

**Alternatives considered:**
- *One global database (SQLite/sled) per user/repo* — rejected for v0.2:
  a new dependency class (D14 supply-chain bias toward few deps), a
  concurrency model to design, and a migration story; sidecar files are
  crash-atomic by construction (rename at commit), travel with the file, and
  need zero new deps. Revisit when directory sync (tree index) lands — the
  sidecar-per-file design degrades to thousands of tiny files at tree scale,
  which is the known trigger to switch to a single per-directory database.
- *Journal the index during transfer* — rejected: the D10 simulation showed
  re-scan-from-content beats journals for the temp; same argument applies
  here. The index is derived state; only write it from a verified state.
- *Trust the index without validation* — rejected: D5's open question (stale
  index claims break reconstruction) is answered by validation + PLAN_LOCAL
  hash verification + VERIFY; "trust finalize-time verification alone" would
  silently accept wrong local reads that cancel out — the hash-verified local
  read closes that hole cheaply.

**Accepted costs:** index files double the inode count per synced file
(and appear in directory listings — D6's hidden-file question is now resolved
for the index: `<target>.ffsync-index` sibling — **appended**, not
`with_extension` (which replaces the last extension and would make the
target name unrecoverable from the index name; the orphan cleanup must
strip the literal suffix to find the target — the `with_extension` variant
deleted valid indexes at startup, found by tests), same non-hidden style as the
temp); a deleted target leaves an orphan index (cleanup rule: on receive,
best-effort delete of `*.ffsync-index` files whose target is missing in the
same directory); and mtime-only validation means an mtime-preserving
in-place edit defeats the warm start — the same caveat as rsync's quick
check and D11. In `--checksum` mode the index is never used as evidence that
a scan can be skipped (the root must come from the scan); warm-start chunk
reads themselves are always hash-verified against the plan, so both modes
are safe.

**Wire mapping:** the index itself is peer-local, but the tests forced a
protocol change: HELLO v2 adds `file_mtime_nanos` (u32), and the §4 fast
path compares sub-second mtimes. v0.1's whole-second comparison let a
same-second same-size rewrite fast-path as "in sync" — a silent
wrong-content no-op that the index work only surfaced because the index
tests rewrite files within the test second. Bumped `VERSION` to 2 (v0.1
peers are rejected by version check; single-binary deployments upgrade
atomically). Self-healing rule: an index that lies (stale-but-validating)
must fail the sync loudly (VERIFY / plan-read hash verification) and be
invalidated on that failure — sender on `ERROR(HashMismatch)`, receiver on
local-read mismatch — so a retry cold-scans and recovers; no poison loop.
A future manifest-delta optimization (send only changed entries) is
explicitly deferred; v0.2 keeps manifest streaming as-is.

**Validation plan:** bench gains a `--warm` scenario: sync twice, assert the
second run performs zero chunk-I/O on unchanged data (stat-only) and still
hash-verifies; loopback test: warm receiver + changed file falls back to cold
scan correctly; corrupt-index test: truncated index behaves as missing.

## D16 — Transport security: rustls TLS 1.3 with self-signed certs, mutual auth by TOFU fingerprint pinning, no PSK, no SSH embedding

**Decision (v0.2 security layer):** encryption and peer authentication ride
on **rustls (TLS 1.3)** as a thin wrapper around the existing TCP stream —
the sync state machines stay untouched (they are generic over
`Read + Write`, and `rustls::StreamOwned` *is* `Read + Write`). Both peers
generate a **self-signed certificate on first run** (`rcgen`, stored in the
ffsync state directory) and authenticate each other by **pinning the
certificate's SHA-256 fingerprint** — trust on first use (TOFU):

1. First connection: each side shows its fingerprint; the user confirms the
   peer's fingerprint matches out-of-band (in person, phone, signal —
   whatever they'd use to exchange a key). `ffsync trust <peer-name>`
   stores it. This is the "fingerprint compare" step SSH users already
   know — same mental model, no new concepts.
2. Every later connection: the presented fingerprint must match the pinned
   one, **on both sides** (mutual auth via a rustls client-cert verifier on
   the server and a custom server-cert verifier on the client). A mismatch
   is a loud, terminal rejection — the same "fail loudly" contract as every
   other ffsync error.

Connection-level identity is all the auth there is: **no usernames, no
passwords, no secrets file** (rsync's daemon model — `auth users` +
`secrets file` — was studied and rejected: plaintext secrets on disk,
challenge-response to reimplement, and it answers a question ffsync does
not have: multi-user serving. ffsync peers are 1:1, person-to-person).

**Why not the alternatives:**
- *Plain TCP + handshake MAC (PSK over our own framing)* — rejected: rolling
  our own channel crypto is exactly the class of mistake TLS exists to
  prevent (nonce reuse, truncation, renegotiation bugs). The one legit
  PSK-in-TLS path is unavailable: rustls does not support external PSKs
  (open issue since 2018, verified 2026-10). A hand-rolled Noise construction
  (snow) would buy smaller dependency weight at the cost of an unaudited
  crypto hand-rolled layer — the opposite trade of rustls, which is the
  Rust ecosystem's audited, mainstream default (D14 bias).
- *Embedding SSH (libssh2)* — rejected: C dependency on the primary Windows
  platform, subprocess-based piping is un-scriptable, and SSH key management
  (agents, known_hosts formats) is a second config surface ffsync would
  have to explain. Users who want SSH can still tunnel plain-TCP ffsync
  through it; that path stays supported by design (transport-agnostic
  state machines).
- *Web PKI (Let's Encrypt, real CAs)* — rejected for peers: ffsync peers
  usually have no domain name; ACME/DNS machinery is setup burden with zero
  benefit when the trust anchor is "the friend's fingerprint", not "a CA
  vouches". Self-signed + pinning is strictly simpler and honest about the
  trust model.
- *QUIC now* — rejected for v0.2 (see D13 amendment below): quinn brings
  tokio back into the tree for one stream of payload; QUIC's wins
  (migration, parallel streams) pay off at directory-sync scale, not for
  single-file syncs. TLS-over-TCP ships the same crypto with a fraction of
  the surface.

**Threat model (what this does and does not stop):**
- Stops: passive observers (encryption), active MITM (pinning — a MITM
  presents a different cert and is rejected), unauthenticated writers (a
  peer without the pinned fingerprint cannot complete a handshake, let
  alone overwrite a file), replay of an old session (TLS 1.3 nonce +
  transcript binding; ffsync's VERIFY oracle additionally pins the payload
  to content).
- Does not stop: a compromised *pinned* peer (it is authorized by
  definition), a user who blindly confirms the first fingerprint (TOFU's
  known weakest link — mitigated by making the fingerprint prominent and
  by `ffsync trust` being an explicit command), and attacks on the host
  itself. This is the same envelope rsync-over-ssh provides, minus the
  multi-user machinery.

**UI contract (rsync-easy, verified against rsync's actual model):** rsync
daemon auth = config file + `auth users` + a plaintext secrets file
(challenge-response); rsync-over-ssh = ssh keys + known_hosts. ffsync's
answer keeps the one part users actually like — "check the fingerprint
once, never again" — and drops everything else:

```
# receiver (first ever run): generates identity, prints fingerprint
$ ffsync receive big-file.bin 7788
  identity: 3f9a…c21e (created, stored in ffsync state dir)
  waiting for peer …
# sender (first ever run): same, then both users compare out-of-band
$ ffsync send big-file.bin 192.168.1.20:7788
  peer fingerprint: 3f9a…c21e — trust? [y/N] y      # interactive TOFU
  (non-interactive: --trust <fingerprint> or ffsync trust <name> <fp>)
# every later sync: no prompts, pinned fingerprints verified silently
```

`ffsync trust <fingerprint>` / `ffsync untrust <fingerprint>` /
`ffsync fingerprints` manage pins in the state dir (`~/.ffsync/` /
`%APPDATA%\ffsync\`, one plain-text `pins.conf`, one fingerprint per line,
`#` comments, no secrets — fingerprints are public). Pins are a **set**,
not per-name entries: any pinned fingerprint may connect, in either role
(names would be per-host bookkeeping with nothing to enforce — a 1:1 peer
set has no "user" concept to separate). No config file, no daemon config,
no users: two commands and a one-time compare. Setup cost vs
rsync-over-ssh: comparable; vs rsync-daemon: strictly less (no plaintext
secrets file, no module config); security: strictly more (mutual, pinning
beats password-over-TLS challenge-response). *(Amended at implementation
2026-10-22: `trust` takes the fingerprint, not a name; storage is a
hand-rolled text file, not TOML — D1's no-serde bias; set semantics
recorded.)*

**Wire mapping:** none — TLS wraps the TCP stream before frame 1; HELLO
and everything above are unchanged (protocol version stays 2; the framing
was transport-agnostic by construction, §14.3). Port: TLS is the default
on the same port scheme; `--plaintext` flag keeps LAN/SSH-tunnel use
explicit. Fingerprint = SHA-256 over the DER-encoded self-signed cert,
formatted like SSH's (`AA:BB:…`, uppercase hex pairs). The identity is
stored raw-DER in `<state-dir>/identity.bin` (4-byte LE cert length,
cert DER, PKCS#8 key DER; atomic temp+rename; `0600` on unix) — no PEM
parsing anywhere.

**Validation plan (as implemented):** loopback tests over real TCP
(`tests/tls.rs`): pin-match on both sides → sync converges; client pinning
a third-party cert → loud client-side rejection; unpinned client vs pinned
receiver → server-side refusal naming the attacker's fingerprint; FirstUse
on both sides → handshake + sync succeed (the CLI's post-handshake prompt
is a CLI concern, not a transport one — the verifier never makes trust
decisions by itself, it only *records* what was presented). One deviation
from the plan as written: the tests run over real TCP loopback, not an
in-memory rustls pair — same code path the CLI uses, and it exercises the
eager-handshake contract. Live two-process check (done): interactive TOFU
prompt on the sender, pre-pinned receiver via `ffsync trust`, silent
second sync, attacker rejected on both sides.

**Accepted costs:** a new dependency class (rustls + rcgen + ring — the
D14 ledger grew from 10 to 12 direct crates, recorded in the deps commit);
the receiver scans its local file *before* the TLS handshake completes
(no data leaves before authentication, but a rejected peer costs a wasted
scan — fixing that needs a pre-handshake identity exchange, which defeats
TLS; accepted); certs never expire practically (self-signed, rcgen
defaults, regenerated on demand); TOFU's known weakest-link property
(documented above); Windows state-dir conventions need care (`%APPDATA%`,
not the temp dir).

## D17 — Directory sync: one session, sequential per-file conversations, tree protocol v3

**Decision (v0.3 scope):** ffsync learns to sync a **directory tree**
between two peers, reusing the proven per-file engine. The design keeps
single-file sync as the special case it already is, and adds a thin
orchestration layer:

- **One connection, many conversations.** A tree session is a single
  transport (TCP or TLS, D16) over which the peers run the *existing*
  per-file state machines sequentially, one file at a time, with a
  directory-level envelope around them. No connection pool, no parallel
  streams (D13's QUIC answer stays: revisit when measurement demands it),
  no interleaving of per-file conversations. Sequential keeps the per-file
  state machines verbatim (they are one-shot, and their NOT_MODIFIED /
  drain semantics are stream-positional) and makes resume, progress, and
  error attribution trivially per-file.
- **Tree envelope (protocol v3):** a `TREE_BEGIN` / `TREE_END` pair of
  frames wraps the session; between them, peers exchange a metadata
  scan of the tree (relative path, size, mtime secs + nanos), reconcile
  it into an action list (see triage below), and then execute per-file
  conversations in a deterministic order. The per-file HELLO carries the
  relative path (it already has a `path` field) and the tree flags in its
  reserved flag bits.
- **Triage happens on metadata, files are proven by roots (D5, D11):**
  matching (path, size, mtime secs+nanos) on both sides → skipped, unless
  `--checksum` disables the fast path tree-wide (D11 semantics, unchanged).
  Everything else → per-file sync (which itself proves identity by root
  hash; the "medium diff" of the workflow doc is absorbed by the per-file
  machine's VERIFY, which is strictly stronger).
- **Direction and deletions:** a tree sync is **one-directional**
  (sender's tree is authoritative, like `rsync -r --delete` semantics made
  explicit). The receiver deletes files present locally but absent from
  the sender's manifest, **after** all transfers succeeded, and only files
  inside the synced subtree. Deletes are deferred to the end so a failed
  transfer never destroys data (fail-closed, same contract as single-file
  sync). `--no-delete` opts out (rsync flag name, familiar semantics).
- **mtime preservation on commit:** after reconstruction, the receiver
  sets the target file's mtime to the sender's (std `File::set_modified`,
  verified to work on this toolchain). Without this, every synced file
  changes mtime and the next tree sync would re-transfer everything —
  the fast path would never fire. This is a wire-visible change: the
  receiver's COMMIT_ACK now happens *after* mtime was set, so both peers'
  warm indexes (D15) record the same mtime. (Single-file sync does NOT
  gain this silently: it changes observable behavior; it lands as part of
  the v3 protocol bump, and the single-file path adopts it under the same
  flag.)
- **Empty directories:** created on the receiver (existence, not content,
  is what a tree is); empty files are synced as zero-byte files (their
  chunk list is empty, root is the domain-separated empty root — the
  codec already handles both). The pre-existing gap that an empty sender
  file cannot converge a non-empty receiver file is fixed in v3 (the
  empty-plan branch truncates to zero and verifies the empty root instead
  of erroring).
- **Symlinks, permissions, xattrs, hardlinks:** still out of scope (D9).
  A symlink inside the tree is synced as its target type resolved
  (follow), or skipped with a warning if dangling — recorded, not silent.

**Reasoning:** the workflow doc's Phase 1–3 (shallow/medium/deep diff) map
onto this as: Phase 1 = tree metadata triage (new, cheap); Phase 2 is
absorbed into per-file VERIFY (root hash comparison — strictly stronger
than the doc's per-file root exchange, and it falls out of the existing
machine); Phase 3–4 = the existing per-file plan/transfer/reconstruction,
unchanged. The sequential model deliberately trades parallel-file
throughput for correctness-first simplicity: one file's failure leaves a
clean, resumable session (its temp file), exactly like single-file sync
today.

**Alternatives rejected:**
- *Parallel per-file conversations* — the per-file machines are one-shot
  and stream-positional (NOT_MODIFIED drain logic assumes conversation
  end); parallelizing needs a conversation multiplexer, which is a new
  protocol layer with real correctness risk, for a win that only matters
  on very high-BDP links with many large files. Revisit with QUIC (D13).
- *One mega-manifest of all chunks in the tree* — collapses per-file
  identity (a chunk is only meaningful as part of a file's ordered chunk
  list, and verification is per-file root); also recreates the memory
  problem for large trees. Per-file conversations keep identity clean.
- *Receiver-derived plans* (§13.5) — orthogonal to tree sync, unchanged.
- *Bidirectional merge* — that is Syncthing's job (D9); ffsync is
  explicitly one-directional with the sender authoritative. A reverse
  sync is just another run with swapped roles.

**Wire mapping (v3):** protocol version 2 → 3. New frame types
`TREE_BEGIN` (0x0C) / `TREE_END` (0x0D): TREE_BEGIN carries the sender's
tree manifest (entry count, then per entry: path, size, mtime secs+nanos,
flags) plus the shared tree flags (`--checksum`, `--no-delete`).
Per-file conversations then run unchanged inside the envelope; the
per-file HELLO `path` field carries the relative path and `file_size`
stays authoritative for that file. TREE_END carries the per-file outcome
summary; the receiver sends the final TREE_END with the delete report
after applying deletions. v2 peers receiving TREE_BEGIN reject it as an
unknown frame (v2 framing is strict) — version negotiation in HELLO
makes this a clean, loud rejection.

**Validation plan:** unit tests for triage and the tree manifest codec;
loopback tests (`tests/tree.rs`) over TCP + TLS: converge a tree of
mixed-size files, empty files, empty dirs, nested dirs; deletions applied
only after success and only with delete enabled; mtime preservation makes
the second sync hit the per-file fast path for every file; a mid-tree
failure leaves earlier files synced and the failed file resumable;
`--checksum` disables the tree fast path; v2 peer rejects TREE_BEGIN
loudly. Live two-process check over TLS, as with D15/D16.

**Accepted costs:** sequential per-file conversations serialize scan and
transfer (no overlap *across* files, only within one file — the pipeline's
parallelism is per-file); the tree manifest is memory-resident on both
sides (bounded by file count × ~100 bytes per entry; 100k files ≈ 10 MB —
fine, and the per-directory database note in D15 stays the scale trigger);
deletes are a trust decision deferred to session end (a receiver that
trusts the sender's fingerprint already trusts it with writes).

## D18 — File metadata, first-class at last: POSIX mode bits, symlink scan fix (D9 amendment)

**Decision (2026-10-23, 0.7.0 scope).** D9's "metadata out of scope until
the core is proven" condition is met — the core is proven end-to-end
(single file, trees, resume, warm starts, TLS, QUIC, excludes, parallel).
The one metadata axis that matters for ffsync's niche is adopted, the
rest stays out:

- **In: POSIX mode bits** (low 12 bits: rwxrwxrwx + setuid/setgid/sticky)
  for tree sync, riding TREE_BEGIN entries under a new flag bit (§16.9).
  Rationale: executable bits and read-only state are the metadata that
  actually breaks a cross-machine sync of scripts/binaries/trees; xattrs
  are platform-fragmented with little sync value here; hardlinks break
  the one-path-one-chunk-list identity model (D15 index, per-file
  verification); symlink-as-entry needs Windows privileges and adds a
  target-validation traversal surface.
- **Out (unchanged): xattrs, hardlinks, symlink-as-entry, ACLs.**
- **Single-file mode unchanged** — metadata rides the tree manifest only;
  documented asymmetry, HELLO layout untouched.
- **Triage unchanged in shape:** mode differences do not trigger sync;
  metadata rides conversations the existing (size, mtime) triage already
  starts. A chmod-only tree change is not synced — the same trade rsync's
  fast path makes, chosen deliberately to preserve the
  deterministic-triage invariant (both peers derive identical
  conversations from identical manifests; adding mode to the skip rule
  without adding it to both manifests' *conversation semantics* would
  split that). `--checksum` mode applies metadata on every file.
- **Symlink scan fix (the real bug this decision found):** `scan_dir`
  recursed on `metadata().is_dir()` — on Windows an NTFS junction needs
  no privileges, carries the directory attribute, and may point at an
  ancestor, so a junction loop was a plausible infinite recursion/hang.
  Fix: the scan reads each entry's *symlink* metadata (`symlink_metadata`),
  follows file symlinks to their target content (today's documented
  behavior), and never recurses into directory symlinks/junctions — skip
  with a warning, recorded not silent. This fix is chunker- and
  version-independent and lands regardless of the metadata feature.
- **Apply order is load-bearing:** mode write → mtime write (§16.6) →
  stat → D15 index write. The index must record the post-metadata mtime
  or every metadata-synced file re-syncs on the next run (the §16.6
  ordering rule extended one step earlier).
- **Wire:** TREE_BEGIN flag bit 4 `metadata`; when set, each entry gains
  `mode u32` (st_mode low bits masked `0o7777`, 0 where the platform has
  no POSIX modes — Windows maps no-write bits to the read-only attribute,
  write-enable always wins, lossy direction documented). Protocol v3 → v4
  (the flag-gated field changes per-entry layout; unknown flags were
  already rejected, so old peers fail loudly — the migration path the
  protocol prefers). The D15 index codec is unchanged (nothing in it
  consumes mode; see §16.9).

**Alternatives rejected:**
- *Read-only-bit-only* (cross-platform, no unix value — exec bits are
  the actual need).
- *Mode as a triage trigger* (breaks deterministic triage / turns a
  chmod into a metadata-only sync class; see reasoning above).
- *Sync symlink entries* (privilege surface on Windows; traversal
  validation; niche).
- *Bump only the index codec, not VERSION* — rejected because per-entry
  wire layout changed under a flag; the flag mechanism is safe for
  *unknown* senders only when the layout is unknown, and here old peers
  would parse a metadata-flagged TREE_BEGIN with wrong entry sizes if
  VERSION still said 3/4. Loud mismatch is the protocol's failure mode
  of choice.

**Validation plan:** tree loopback tests gain mode assertions (unix:
chmod'd file converges; setuid bit round-trips; Windows: read-only
attribute maps both ways); scan tests for junction/dir-symlink
non-recursion and dangling-symlink warning; v3-peer-rejects-v5-HELLO
test; the full §16.6-style mtime-preservation regression suite re-run.

## D19 — Small-file adaptive chunking: sender-derived CDC parameters from file size, HELLO-authoritative, index records its parameters

**Decision (v0.8.0):** the CDC parameters stop being compile-time
constants and become a pure function of the sender's file size:

| file size | CDC (min/avg/max) | source |
|---|---|---|
| ≥ 16 MiB | 64 KiB / 256 KiB / 1 MiB | shipped defaults (unchanged) |
| < 16 MiB | 16 KiB / 64 KiB / 256 KiB | F1-sweep best point |

The sender derives the parameters before scanning and sends the *actual
values* in HELLO (`cdc_min/cdc_avg/cdc_max` — the fields have been on the
wire since v0.1, but until now both sides sent the constants and neither
read the peer's; §4.3's "sender's are authoritative" was aspirational).
The receiver uses the peer's values for the VERIFY re-scan (§8.2 —
mandatory: different parameters produce different chunk layouts and the
roots could never agree) and for its own manifest scan (maximizes hash
matches). No new frames, no flag bits, no VERSION bump: the layout is
unchanged; only the semantics of already-present fields sharpen, and the
policy is a pure function both peers could re-derive (the wire values
still govern — no "same policy version" invariant needed).

**Threshold evidence** (`evaluation/chunking.md`, size-ladder sweep,
2026-10-23; truncated real content, corpus mutation patterns, delta =
missing-chunk share): below 16 MiB the smaller config wins on average —
rust-src 8 MiB: 18.09% → 4.14% (4.4×), 16 MiB: 5.13% → 2.70%; tos.webm
16 MiB: 8.62% → 3.76%; the one regression (tos.webm 8 MiB, 8.94% →
10.18%) is compressed media too dense in cut points even for the smaller
config at that scale. At 32 MiB the difference is noise (2.6–2.9% both,
mixed direction); at 64 MiB noise (2.4–2.8%). 16 MiB is the last rung
where the win is unambiguous and the cost — chunk count 4× (206–209
chunks at the boundary, ~9 KiB manifest, ~9 KiB index per file) — is
still trivial. The `appended` pattern is config-insensitive (~9–10%
everywhere; the tail shifts one boundary), confirming the effect is an
edit-locality property, not a generic "smaller is better".

**Alternatives considered:**
- *Continuous scaling* (`avg = clamp(size/16, 16 KiB, 256 KiB)`) —
  rejected: unbounded parameter variety makes every D15 index a potential
  layout mismatch and the policy unauditable; two classes bound the cliff
  to one crossing per file lifetime.
- *More classes* (32k/128k/512k as a middle gear) — rejected: the sweep
  shows 32k/128k/512k never wins clearly (it trails 16k below 16 MiB and
  ties the shipped config above) — it adds a cliff without a rung where
  it is the best choice.
- *Derive the policy independently on both peers from file size* —
  rejected in favor of sending the values: the sender's layout defines
  plan and VERIFY; making the receiver consume the wire values (which it
  already receives) removes any requirement that both binaries share a
  policy version. The policy then only decides what the *sender* sends.
- *Chunker swap for small files (MinCDC's uniform distribution)* —
  already closed by F2: the mobydick effect is chunk-size, not
  distribution; the 16k config achieves the same improvement within the
  existing chunker.

**Accepted costs:**
- *The class-boundary cliff:* a file that crosses 16 MiB between syncs
  re-chunks entirely; the diff sees nothing familiar and the file
  re-transfers once. Self-healing at the next commit (index rewrites with
  the new layout). One crossing per file lifetime in practice.
- *Index codec bump* (`FFSYIX01` → `FFSYIX02`, +12 B header: min/avg/max
  u32 each): a warm start serves its chunk list only when the stored
  parameters equal the session's; otherwise cold scan (correct but
  wasteful — the honest choice over serving a foreign layout). Existing
  v1 indexes are treated as missing state and rewritten at the next
  commit; no migration machinery (same rule as every codec change so
  far).
- *Large-file fingerprints must not move:* files ≥ 16 MiB chunk
  byte-identically to the shipped defaults, so every existing large-file
  index stays valid. The bench fingerprint discipline runs per-config
  from now on.

**Wire mapping:** none. HELLO's `cdc_min/cdc_avg/cdc_max` become
authoritative (§4.3 amended: SHOULD → MUST for the VERIFY re-scan, SHOULD
for the manifest scan); §8.2 notes that the root identity is
parameter-bound. The worked example (§15) already says "both use
identical CDC params" — now they are the sender's, announced.

**Validation plan:** unit tests for the policy function (boundary at
16 MiB inclusive/exclusive); loopback: small-file session converges with
non-default parameters (assert the HELLO carried 16k/64k/256k), warm
start serves a matching-parameter index and cold-scans a mismatched one
(v1 index treated as missing); bench fingerprint per config (large-file
chunk counts unchanged vs the shipped defaults); full gates on Windows +
Docker Linux.

