# Deep Research Digest

Provenance: two Gemini deep-research runs from the project's inception phase
(November 2025), saved as markdown in this folder:

- `Designing a High-Performance, Resilient File Synchronization Protocol in
  Rust_ A QUIC, BLAKE3, and CDC Approach.md` — overall architecture
  ("RustSync"): transport, hashing, chunking, index structures, workflow.
- `P2P Connection Challenge Resolution.md` — P2P connectivity: NAT traversal,
  signaling, relay fallback.

This document digests them into the current decision framework: what they
validate, what they challenge, and what we adopt / defer / reject. The two
source documents remain the primary sources; this digest is the working copy.

## Document 1: Architecture ("RustSync")

### Where it converges with our design

Independent research reaching the same conclusions is the strongest validation
we have. Convergent points:

- **BLAKE3 + CDC + zstd** as the core stack (our D2, D4; protocol §7).
- **Concurrent scanning on both peers** with early, pipelined data transfer
  (our workflow Phase 1–4, protocol §5, §9.2).
- **Per-chunk BLAKE3 verification** so resume never re-hashes partial data —
  "the receiver never needs to re-hash any partial file data upon resumption"
  is exactly our D2/D10 argument.
- **Merkle/tree-structured hashing for free**: BLAKE3's tree mode yields the
  root hash while chunking (our D5), which their design also relies on.
- **Atomicity and compression choices** (zstd, level tuning) match ours.

### Where it diverges — and our verdicts

| Their proposal | Our verdict |
|---|---|
| **QUIC from day one** (HOL-blocking elimination, 0-RTT, connection migration) | **Defer to v0.2.** Their WAN-loss-resilience argument is real (single-flow TCP degrades badly at 1–5% loss), but v0.1 targets LAN/SSH-tunnel use where TCP is fine. Recorded as D13. The strong point we adopt: connection migration (Wi-Fi↔hotspot) is exactly the "friend over the internet" scenario, so QUIC is the default v0.2 transport plan, not an afterthought. |
| **gRPC/protobuf framing** (schema safety, streaming, cross-language) | **Reject for v0.1.** Our frame table is 11 fixed types over a length-prefixed stream — protobuf adds two heavy dependencies (tonic/prost) for a contract we can hold in one Rust enum. Revisit only if a third-party client ever matters. |
| **Chunk sizes 32–256 KiB average** ("larger" by their framing, balancing metadata volume vs. dedup) | **Open empirical question — they challenge our 1 MiB.** Their concern is metadata volume for archival-scale dedup; ours is different: with avg 1 MiB / max 4 MiB, the boundary-shift over-send bound (D12, ~2×max = 8 MiB per modification region) is coarse. Smaller chunks tighten it at the cost of ~4× more chunks (hash-finalize overhead, plan frames, manifest bytes — all still tiny at single-file scale). Needs a bench parameter sweep, not an opinion. Registered as follow-up F1. |
| **SeqCDC** (hashless CDC, 1.5–3.1× throughput vs. FastCDC, Waterloo Middleware '24) | **Note for later.** No mature Rust implementation; our `Chunker` trait makes swapping cheap when one appears. Follow-up F2. |
| **Prolly-tree index sync** (Merkle traversal to localize divergence) | **Adopt the idea for tree sync (Phase 1 / protocol §14.6), not now.** Their design leads with directory trees; we deliberately scoped single-file first (D9). When tree sync arrives, a Merkle index over per-file trees is the right shape and the root-hash fast path (D11) generalizes to it naturally. |
| **Mid-chunk resume** (`StreamChunkRequest` with start byte offset) | **Reject for v0.1.** Requires the sender to keep per-chunk stream state. Our chunk-granularity resume (§10) wastes at most one partially sent chunk (~avg chunk size) per interruption — negligible at our chunk sizes. |
| **0-RTT session resumption** | **Adopt with a caveat for v0.2.** 0-RTT data is replayable; fine for read-only/idempotent sync steps, must not carry mutating commands without anti-replay handling. |

## Document 2: P2P connectivity ("P2P Connection Challenge Resolution")

### The numbers that matter

- UDP hole punching succeeds in ~82% of compatible peer pairs (vs. ~64% for
  TCP punching).
- Direct connections (host + STUN-reflexive) cover ~75–80% of real-world
  sessions; the remaining **20–25% need a TURN relay** (symmetric NATs and
  restrictive firewalls).
- Free public TURN servers are effectively nonexistent — relay fallback means
  owning or renting infrastructure.
- NAT mappings expire in tens of seconds without traffic → keep-alives are
  mandatory for idle sync sessions.

### Our verdict

This document is the best argument *for* our D9 scope discipline — and the
best roadmap for when we lift it.

- **v0.1 stays direct-connection-only** (TCP, user supplies reachability:
  LAN, port forward, or SSH tunnel). The doc shows NAT traversal is not a
  feature, it's a second project: rendezvous/signaling server, candidate
  exchange, coordinated punching, relay fallback, keep-alives. Building it in
  v0.1 would delay the core engine for connectivity we don't need on LAN.
- **We adopt their honesty requirement:** document that direct-only v0.1 will
  fail to connect for roughly a fifth of internet-peer pairs, and say so in
  the README/CLI when the network layer ships.
- **v0.2+ path:** use an integrated Rust P2P-QUIC crate rather than assembling
  STUN/TURN/ICE by hand. The doc recommends `ant-quic`; the other strong
  candidate (not covered by the doc) is **iroh** (n0-computer): quinn-based
  hole punching + relay fallback, and — notably — it already uses BLAKE3 for
  node identity, which fits our stack. Evaluate both when v0.2 starts
  (follow-up F3). Relay fallback remains a real infrastructure cost either
  way; the doc's three-tier blueprint (peers / rendezvous / TURN) is the
  shape to hold in mind.

## Registered follow-ups

- **F1 — Chunk-size sweep in the bench:** parameterize CDC min/avg/max
  (blocked until the bench harness WSL work lands), measure over-send vs.
  chunk size on the modified scenario, then revisit D12's cost statement and
  possibly the default config.
- **F2 — Track SeqCDC:** watch for a Rust implementation; benchmark against
  `fastcdc` when one exists. (Update 2026-10-23: the trigger has fired —
  a young implementation exists, `seq-chunking` 0.1.0 on crates.io /
  `puntakana/seqcdc-rs`, MIT, single release, minimal adoption; MinCDC
  (`orlp/mincdc`) and mothcdc are further candidates. The benchmark
  belongs in a standalone experiment crate first: chunk boundaries are
  wire-protocol-relevant, so a chunker swap must pass the boundary
  fingerprint + delta discipline of `evaluation/chunking.md` before it
  can ship behind the `Chunker` trait.)
  (Update 2026-10-23, same day: **F2 closed — no integration.** The
  experiment ran (`evaluation/chunking.md`, "Alternative chunkers on our
  scenarios"): MinCDC is 3.6× faster than FastCDC at equal delta quality
  but scan time is disk-bound cold and a few percent of sync wall time;
  SeqCDC fails the sync metric outright (relative cut rule re-anchors
  boundaries after in-place edits — 2.7–6× worse delta on media); MothCDC
  is MinCDC byte-identical on our data (caterpillar inert without
  repeated regions). MinCDC stays the tracked candidate only if warm/fast
  rescans ever become the bottleneck.)
- **F3 — v0.2 transport evaluation:** QUIC via quinn/iroh/ant-quic against
  the criteria in D13 (loss resilience, migration, relay fallback, BLAKE3
  synergy, maintenance health). (Update 2026-10-23: **F3 closed — QUIC
  shipped** in 0.6.0, §17 of the protocol spec; quinn won on maintenance
  health and BLAKE3 synergy. One connection per session, conversations ride
  streams, same identity/pinning as TLS.)
- **F4 — Small-file adaptive chunking:** choose CDC parameters from the
  file size (small files → smaller chunks; the corpus shows mobydick
  64.6% → 23.1% delta, video720 10.3% → 5.4%, large files noise — a
  chunk-size effect, not a chunker effect, per F2). The wire already
  carries per-file CDC parameters in HELLO (§4.3 makes the sender's
  authoritative); the implementation hardcodes constants on both sides
  and never reads the peer's values, so the change threads the existing
  fields through the scan sites + records parameters in the D15 index
  (magic bump). Full scope and measurement plan in
  `evaluation/chunking.md`, "Scope: small-file adaptive chunking (F4)".
  (Registered 2026-10-23; not scheduled.)
