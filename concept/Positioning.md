# Positioning: Why ffsync exists

This document captures the reasoning for building ffsync in a world where rsync,
rclone, and Syncthing already exist. It is the "why" behind the project; the
"how" lives in `File Synchronisation Workflow.md` and `File Reconstruction.md`.

## The origin story

The project started with a concrete failure: exchanging one large file with a
friend over the internet, using rsync. The transfer needed several attempts, and
every attempt paid the same "cold start": minutes of hashing before a single
byte moved. Five runs accumulated almost an hour of combined hashing time.

The tell-tale sign was the resource profile: the disk was working hard while the
network was silent. The tool gave almost no visibility into this, even in
verbose mode. The conclusion that started this project: *it should not take that
long to just hash a file and then send it.*

## What is actually slow in rsync

rsync's delta algorithm is single-threaded per file. The sender computes a
rolling checksum over every byte, plus a strong (MD5) hash per candidate block,
on one core, with no SIMD story. The pipeline shape (generator / sender /
receiver as separate processes) already overlaps scan and transfer — the shape
is not the problem. The problem is that the scan itself cannot use more than
one core.

Measured during development on a 72.7 GiB file (Samsung 860 QVO, ~550 MB/s):

- rsync: ~15 minutes (~85 MB/s effective — far below what the drive can do)
- ffsync pipeline: ~242 seconds (~320 MB/s — the drive is the limit, see
  `evaluation/chunking.md`)

That is a ~3.8x advantage on the cold start, and it widens on faster hardware
because BLAKE3 is SIMD-accelerated while rsync's checksum work is not.

## The decision framework: link speed decides what matters

Sync wall time ≈ max(scan time on both peers, transfer time), overlapped. Which
term dominates flips every design decision:

| | Slow link (residential, 10–50 Mbit) | Fast link (LAN, 1–10 GbE, datacenter) |
|---|---|---|
| Dominant cost | Transfer (10–50x everything else) | Scan / hashing |
| Parallel hashing | Irrelevant — one core can hash faster than the link drains | The whole win |
| Warm-start chunk index | The only thing that helps repeated syncs | Also helps |
| rsync | Perfectly adequate | Painfully scan-bound |

ffsync is therefore a *fast-link, big-file, repeated-sync* tool: workstations,
media, datasets, LAN replication. On a slow link with a one-off transfer, rsync
is the right tool and ffsync adds nothing. Being honest about this niche is a
design constraint, not a limitation.

The benchmark harness (`cargo run --bin bench`) projects exactly this crossover
for each scenario and link speed.

## Why "build around rsync" does not work

The tempting option — speed up cold-start hashing ourselves and use rsync as
the transport — fails for a structural reason: **rsync's transport *is* its
delta algorithm.** The wire protocol and the rolling-hash block matching are one
thing. There is no interface to hand rsync a set of chunk hashes computed by a
different pipeline.

The realistic rsync-based variants and why they fall short:

| Variant | What happens | Verdict |
|---|---|---|
| Pre-hash both sides, skip if identical, else rsync | rsync still does its slow scan for anything that differs | Only helps the trivial case |
| Pre-hash + `rsync --whole-file` | Full copy, no delta | Loses bandwidth on "similar but modified" — the core case |
| librsync | Same serial scan, embeddable | No gain |
| fpsync / parallel rsync instances | Parallelizes across files | Useless for one huge file |
| Patching rsync | The matching loop is the heart of a 25-year-old C codebase | Not a realistic project |

So the real choice is: accept rsync's cold start, or own a deliberately tiny
protocol. What ffsync does *not* rebuild (the boring, battle-tested parts):

- Auth/encryption: rustls, or tunnel over SSH
- Compression: zstd
- Atomicity: temp file + verify + atomic rename (already implemented)
- Metadata, permissions, xattrs, symlinks, hardlinks: deferred, possibly forever
- File discovery (Phase 1): deferred; v0.1 syncs one explicitly named file pair

## The niche is open — but fragmented

The technique stack (content-defined chunking + content-addressed chunks +
strong hash) is proven at scale — restic, bup, casync, ostree, Syncthing all use
it. But each existing tool took one quadrant of the feature space:

| Tool | Delta transfer | Resume/reconnect | Peer-to-peer push | On-demand CLI |
|---|---|---|---|---|
| rsync | yes | yes (`--partial`) | yes | yes |
| rclone | **no** (open issue #3488 since 2018) | partial, cloud-dependent | via remotes | yes |
| Syncthing | yes | yes | yes | no — background daemon, key exchange, constant presence |
| croc / wormhole | no | yes | yes | yes |
| restic / casync / Plakar | yes (CDC) | n/a | no — archival, single-writer | no |

The empty intersection: **on-demand, interactive, chunk-delta sync between two
peers** — rsync's interaction model built on restic-class internals. Nobody
fires up Syncthing to send a friend a 70 GiB file once; nobody gets deltas from
rclone; restic does not do interactive two-peer sync.

rclone's maintainers know exactly what they are missing (issue #3488) and still
have not built it — cloud-remote architecture makes delta sync awkward, and
rclone's demand is scattered across use cases that mostly do not need it.

## Why incumbents do not just adopt the new algorithms

rsync *does* adopt drop-in improvements (rsync 3.2.x added xxHash as a checksum
option). What it never did is parallelize its matching loop — because that
cannot be bolted on: it requires changing the algorithm class (rolling hash →
CDC) and the wire protocol, i.e. a breaking change in a tool whose entire value
is being preinstalled everywhere for 25 years.

The ecosystem pattern: incumbents freeze because their value is ubiquity and
trust; newcomers absorb the new technology and displace the underserved niche.
Nobody rewrote rsync — restic, Syncthing, zstd, and BLAKE3 displaced it at its
edges. AI accelerates this by compressing the newcomer's iteration loop, not by
making rewrites of trust-sensitive software safe. A sync tool that is 90%
correct is worth less than zero; trust is accumulated through edge cases
(partial writes, fsync semantics, filesystem quirks) that no amount of generated
code shortcuts.

## The thesis, in one sentence

> Send a big file to a peer like rsync would — minimal bytes, resumable,
> verified — but with a modern engine: parallel CDC + BLAKE3, warm restarts,
> no daemon, no ceremony.

Every clause maps to a gap in the table above. The benchmark harness exists to
keep the claims honest.
