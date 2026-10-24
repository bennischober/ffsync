# Wire Protocol

Specification for the ffsync wire protocol. Written for v0.1 (protocol
version 1: **one file pair, two peers, one TCP connection**); it tracks the
protocol across versions — changes ship with a `VERSION` bump and are
recorded in the relevant sections. Current version: **4** (v0.2: HELLO
gained `file_mtime_nanos`, see §3/§4; v0.3 adds the tree envelope, see
§16; v0.4/v0.5 work extends TREE_BEGIN additively under flag bits —
§16.7 excludes, §16.8 parallel sessions — without a VERSION bump, since
unknown flag bits are rejected and the default layouts are unchanged;
v0.6 adds §17, the QUIC carrier, also without a bump; v0.7 adds §16.9,
per-entry POSIX mode bits under a flag, with a VERSION bump — see §16.9).
§17 specifies the QUIC transport (D13 step 2): a carrier change under
the same wire bytes, no VERSION bump.

## 1. Scope and non-goals

**In scope (v0.1, extended in v0.2):**

- One explicitly named file pair per invocation (`ffsync send <file> <peer>` /
  `ffsync receive <file>` — exact CLI shape open, see §14).
- Roles: the peer with the desired ("source") content is the **sender**, the
  peer wanting to converge is the **receiver**. Roles are assigned by the CLI,
  not negotiated.
- Transport: one ordered, reliable byte stream (TCP). Plaintext for v0.1 —
  intended for LAN use or inside an SSH tunnel. TLS (rustls) is planned; see
  D13 and §14.
- Compression: zstd on chunk-data frames only.
- Resume after disconnect (receiver-authoritative, D10).
- Progress/observability derived from protocol flow (§11).
- v0.2: peer-local persistent chunk index (warm starts, D15) — §14.1; HELLO
  sub-second mtime (protocol v2).
- v0.3: directory-tree sync (D17) — one connection, sequential per-file
  conversations inside a TREE_BEGIN/TREE_END envelope (§16); mtime
  preservation on commit.

**Out of scope, per D9:**

- Metadata beyond (path, size, mtime): no xattrs, no hardlinks, no
  symlink-as-entry. (v0.3 follows file symlinks to their target type;
  dangling ones are skipped with a warning — recorded, not silent.
  v0.7 amends this once: tree sync carries POSIX mode bits under a flag
  — §16.9 — and directory symlinks/junctions are no longer recursed
  into; both still recorded, not silent.)
- Bidirectional merge / continuous sync (that is Syncthing's job; D9, D17).

## 2. Model

### 2.1 Content addressing is the only source of truth

Every chunk is identified by its 32-byte BLAKE3 hash. The file is identified by
its **chunk-tree root**: `BLAKE3(CHUNK_ROOT_PREFIX || chunk_hashes_in_file_order)`
with a 32-byte domain-separated prefix — *not* the standard BLAKE3 file hash,
whose internal Merkle tree is over fixed 1024-byte blocks rather than CDC chunk
boundaries (a standard root could not be computed from the chunk list and would
require a second full read of the file). The chunk root is computed from the
chunk list the pipeline already produces; it is order-sensitive, so all scans
must produce chunks in canonical file order (sorted by offset — hash workers
complete chunks concurrently and arrival order is not file order).

Property: two files with equal root hashes are (collision-assumption)
chunk-identical. The root is therefore simultaneously the change-detection
identity (D11), the end-to-end verification oracle (§8), and the file identity
for warm starts (§14.1). Offsets are *descriptive*, never *authoritative*.

### 2.2 One ordered wire, unordered storage

The wire is a single ordered TCP stream carrying typed, interleaved frames.
"Out-of-order" applies to **storage**, not the wire: the receiver writes chunks
to the temp file at arbitrary offsets in arrival order (D6). There is no
per-chunk parallelism on the wire in v0.1 (that is an extension for long fat
networks, §14.4).

### 2.3 The manifest describes "bytes I can offer locally"

Both roles use one message shape: the **manifest** — a list of
`(hash, offset, length)` entries describing byte ranges the *sender of the
manifest* can serve locally. Normally this is the receiver's current file. On
resume it is the receiver's partially built temp file (§10). The sender's
diff logic never needs to know *which* file a manifest describes — only that
those bytes are available at those offsets.

## 3. Framing

```
Frame := type: u8 | payload_len: u32 (LE) | payload: [payload_len]
```

All integers are **little-endian** (deliberate: native Rust order, no
conversion cost; network-byte-order tradition is not worth the code). Maximum
payload: 64 MiB. A frame must be dispatched on its type byte only; unknown
types are a protocol error (forward compatibility is by version negotiation,
not by tolerating unknown frames).

| Type  | Name           | Dir     | Payload |
|-------|----------------|---------|---------|
| 0x01  | HELLO          | both    | magic `FFSY` (4B), version u16 (=4), flags u16 (§4), role u8 (1=sender, 2=receiver), cdc_min u32, cdc_avg u32, cdc_max u32, file_size u64, file_mtime u64 (unix secs, 0 if absent), file_mtime_nanos u32 (sub-second part, 0 if absent; v0.2), path (u16 len + UTF-8) |
| 0x02  | MANIFEST_CHUNK | recv→send | hash 32B + offset u64 + length u32 (44 B) |
| 0x03  | MANIFEST_DONE  | recv→send | empty |
| 0x04  | PLAN_LOCAL     | send→recv | target_offset u64 + length u32 + source_offset u64 + hash 32B (52 B) |
| 0x05  | PLAN_REMOTE    | send→recv | target_offset u64 + length u32 + hash 32B (44 B) |
| 0x06  | PLAN_DONE      | send→recv | empty |
| 0x07  | DATA           | send→recv | hash 32B + zstd-compressed chunk bytes |
| 0x08  | VERIFY         | send→recv | root_hash 32B |
| 0x09  | COMMIT_ACK     | recv→send | empty |
| 0x0A  | NOT_MODIFIED   | both    | empty |
| 0x0B  | ERROR          | both    | code u16 + message (u16 len + UTF-8) |
| 0x0C  | TREE_BEGIN     | send→recv | flags u16 (§16.2), [parallel u16 when bit 3], [pattern section when bit 2], entry_count u64, then entry_count × tree entry (§16.2; per-entry `mode u32` when bit 4) |
| 0x0D  | TREE_END       | both    | deleted_count u32 + deleted_bytes u64 (recv→send, after deletes; send→recv variant is empty); v0.3 |
| 0x0E  | FILE_SYNC      | send→recv | empty; tree sessions only: "this file needs a conversation" (§16.4) — its absence is NOT_MODIFIED |
| 0x0F  | TREE_WORKER    | both    | sender→recv: worker_index u16 (1..K-1); recv→sender: ACK (empty) (§16.8) |
| 0x10  | TREE_FILE      | send→recv | rel path (u16 len + UTF-8) (§16.8) |

Error codes: 1=version, 2=role conflict, 3=bad frame, 4=hash mismatch,
5=io, 6=aborted. Any ERROR frame is terminal: the sender of ERROR closes the
connection after flushing; the peer must treat the transfer as failed.

## 4. Handshake and negotiation

1. Both peers connect and immediately send HELLO.
2. Validate: magic, version equality (else ERROR 1), role conflict
   (both 1 or both 2 → ERROR 2).
3. CDC parameters: the **sender's** (min, avg, max) are authoritative; the
   receiver **MUST** use them for the temp-file re-scan (§8.2 — different
   parameters produce different chunk layouts and the roots could never
   agree) and **SHOULD** use them for its own manifest scan (same-boundary
   scans maximize hash matches; a mismatch degrades to over-send, never
   corruption — the offsets in a manifest are descriptive, §3). The sender
   picks the parameters as a pure function of its file size (small-file
   adaptive chunking, D19: below 16 MiB the F1-sweep config
   16k/64k/256k, otherwise the shipped 64k/256k/1m — both peers can
   re-derive the policy, but the values on the wire are what governs). A
   receiver may keep scanning with its own parameters for its manifest
   when its file differs wildly in size; VERIFY correctness is unaffected
   as long as the *re-scan* uses the sender's parameters.
4. Fast path: if `flags.trust_times` is set (default on, D14) **and**
   `file_size`, `file_mtime` **and `file_mtime_nanos`** are equal on both
   sides, the sender sends NOT_MODIFIED and closes. Both peers are done
   without reading a single file byte. This is rsync's default semantics; it
   trusts mtime+size and can be wrong in the same ways rsync can (2-second
   FAT mtime granularity, network filesystem clocks, mtime-preserving edits).
   The nanosecond field exists because v0.1's whole-second comparison let a
   same-second same-size rewrite pass as "in sync" — a silent wrong-content
   no-op (v0.2, found by the D15 index tests; modern rsync compares
   sub-second mtimes for the same reason). `--checksum` mode clears this
   flag and forces full verification.

Flags (HELLO): bit 0 `trust_times`, bit 1 `eager` (§9.2 — designed below;
still must be 0 on the wire until §9.2 is implemented, and a peer MUST
reject a HELLO that sets it), bit 2 `resuming` (informational, §10), bit 3
`dry_run` (§7.1, implemented in v0.8 work: the sender offers a preview
session; the receiver accepts with the DRY_RUN_ACK marker before any
manifest/plan traffic). Unknown bits are rejected at decode (strict
codec).

## 5. Scan and manifest exchange (cold start)

After the handshake, both peers scan concurrently (this is the pipelined
`process_file` pipeline):

- **Receiver:** streams one MANIFEST_CHUNK per chunk of its file as the
  pipeline emits them, then MANIFEST_DONE. It does *not* need to finish before
  the sender starts anything except diffing (lazy mode).
- **Sender:** accumulates its own chunk list and root hash. It consumes the
  receiver's manifest stream into a `HashSet<Hash>` (offsets are ignored for
  diffing, D4).

The sender's diff may only start after MANIFEST_DONE. TCP backpressure makes
the ordering self-enforcing: the sender cannot have diffed a manifest it has
not fully received.

## 6. Plan generation

The sender maps each of its chunks to an action:

- Chunk hash ∈ receiver's manifest set → **PLAN_LOCAL**: the receiver already
  has these bytes; the plan entry carries the receiver's own offset for that
  hash (target_offset = sender chunk offset, length = sender chunk length,
  source_offset = manifest offset for that hash).
- Otherwise → **PLAN_REMOTE**: sender must transmit the bytes (target_offset,
  length, hash).

The sender streams PLAN_LOCAL / PLAN_REMOTE frames as it diffs, then PLAN_DONE.

### 6.1 Exact-match rule (D12)

A local match requires **(hash, length) exact match** against a manifest entry.
PLAN_LOCAL carries the chunk hash so the receiver can verify the local read
against the expected content: a stale manifest entry (content changed since
the manifest was computed) costs a clean ERROR — never silent corruption. On
resume the hash also disambiguates *which* local file an entry refers to (see
§10). The receiver never sub-slices a manifest entry, because content inside a
chunk cannot be verified without re-hashing it.

### 6.2 Boundary-shift fallback

After a modification, CDC boundaries shift between the two versions for
roughly one max-chunk-size until the rolling window re-synchronizes. In that
window, sender chunks may not match any receiver manifest entry exactly even
though the bytes exist locally. The plan therefore falls back to PLAN_REMOTE
for those chunks. Consequence: over-send is bounded near modification regions
by ~2 × max_chunk_size; correctness is unaffected. Using identical CDC
parameters on both sides (§4.3) maximizes local reuse.

## 7. Data transfer (lazy)

After PLAN_DONE, the sender streams one DATA frame per PLAN_REMOTE hash,
compressed with zstd (level 3 default). Because the wire is ordered, the
receiver has already seen the PLAN_REMOTE entry for every DATA hash when it
arrives — it can write immediately to `target_offset`, no buffering needed.

### 7.1 Dry-run preview (v0.8 work, additive)

`--dry-run` (send side) asks for a preview: the receiver executes nothing,
writes nothing, deletes nothing, and reports what WOULD happen. The wire
contract (no VERSION bump — additive frame + flag, same precedent as
TREE_WORKER/TREE_FILE):

1. The sender's HELLO carries flag bit 3 (`FLAG_DRY_RUN`).
2. A receiver implementing the mode sends an empty `DRY_RUN_ACK` (0x11)
   marker immediately after reading the HELLO — before any manifest or plan
   traffic.
3. **Old-peer contract:** a peer that ignores the bit on decode never reads
   the marker — it waits for a manifest, so the marker buffers and the
   receiver's manifest streams on normally; its plan loop then sees the
   marker as an unexpected frame and fails LOUDLY (strict decode). Nothing
   is transferred or written at any point.
4. A dry-run sender reads the marker, then the flow is normal through the
   plan. The receiver answers the PLAN itself:
   - would-change (any PLAN_REMOTE entry, or a non-layout-identical Local
     plan, or a zero-size sender file) → `NOT_MODIFIED` + `DRY_RUN_ACK`
     (would-change report). The sender skips DATA and VERIFY entirely,
     reads the report, returns `WouldChange`.
   - nothing-would-change (empty plan with matching state, or an all-Local
     plan reproducing the receiver's layout exactly) → `COMMIT_ACK` as
     usual; the sender returns `InSync`. No index is written in dry-run.
5. Tree sessions (§16.10) carry the preview as a TREE_BEGIN flag bit; the
   delete pass counts instead of removing.

The preview's zero-mutation guarantee is structural: no temp file is
created on the receiver in the would-change path, and the sender requests
no DATA.

**Receiver write order (D10, empirically decided):** the receiver executes
plan entries in order, resolving each PLAN_LOCAL chunk per hash from its union
manifest (temp file before old file) and writing it *before* receiving the
DATA frames. Local chunks are the bulk of the file and available offline, so a
crash mid-DATA leaves a temp that is mostly real data — the precondition that
makes §10 resume work. (The disconnect simulation measured the alternative:
remotes-first leaves a ~97%-hole temp where every chunk straddles a hole and
the re-scan matches nothing; locals-first recovers 30–32 of 32 MiB.)

Receivers handle duplicate DATA hashes by ignoring repeats (can only happen
with future eager mode or retries).

**End of transfer discipline:** the receiver knows the transfer is complete
when (a) PLAN_DONE has been seen, (b) every PLAN_REMOTE hash has a DATA frame,
and (c) VERIFY has arrived. It must not finalize before (a)–(c) all hold.

## 8. Verification and commit

1. Receiver pre-allocates the temp file (`<target>.ffsync-tmp`, D6) to the
   sender's `file_size` at the start of plan execution.
2. On completion (§7), the receiver computes the BLAKE3 root hash of the temp
   file and compares it to the VERIFY frame.
3. Match → fsync, atomic rename over the target path, send COMMIT_ACK. Both
   peers are done.
4. Mismatch → delete the temp file, send ERROR 4. The original file is
   untouched. Retry is the sender's decision (fresh connection).

**CDC parameters in verification (v4, D19):** the root is computed over the
chunk list produced by *one* parameter set — the sender's. The receiver's
temp re-scan therefore uses the sender's HELLO parameters
(`cdc_min/cdc_avg/cdc_max`), not its own defaults; two differently-cut
chunkings of identical content yield different roots, so a receiver that
ignores the sender's parameters can never verify. (Before D19 both peers
hardcoded the shipped constants, which is why this was invisible.)

**Already-in-sync edge case:** if the sender's plan is empty (no chunks at
all) the sender still sends VERIFY. The receiver then hashes its *current*
file (it already has the root from its own scan — free) and compares: match →
COMMIT_ACK without ever creating a temp file; mismatch → the receiver's file
content does not match what its manifest claimed (corrupted index/trust
assumption) → ERROR 4. This makes "files identical despite metadata mismatch"
discoverable without any transfer, and it is the hook the future index plugs
into (§14.1).

**All-Local plans (v3, found by the static-binary smoke test):** an empty plan
is *not* the only zero-wire-bytes shape. A receiver whose file contains the
sender's chunk bytes at *different positions* (a prefix or shifted region of
the sender's file) yields an all-Local plan — Locals are dedup'd by hash, not
by position. The receiver must then **execute** the plan (write chunks at the
sender's offsets from wherever its manifest holds them) and both peers report
the executed plan (Transferred, 0 wire bytes). The receiver skips the rewrite
only when the plan's Local spans exactly reproduce the receiver's own chunk
layout (same count, same offsets, same lengths — identical files under
`--checksum`, including periodic content whose repeated hashes collapse in
the diff map; the sender mirrors the check against the raw manifest chunk
list). VERIFY guards every path regardless.

## 9. Diff strategy (D8) — lazy now, eager reserved

### 9.1 Lazy (v0.1, default)

Receiver finishes its scan before the sender sends anything. Zero wasted
bytes, one scan-length of latency before the first data byte. Right for slow
links, where over-sending is fatal.

### 9.2 Eager (designed, not yet implemented — flag bit 1)

Sender diffs against the *partial* manifest set and streams DATA for hashes
confirmed missing so far. Bytes start moving during the scan. A late manifest
entry may match an already-sent chunk → bounded over-send. Requires the
receiver to buffer pre-plan DATA by hash (the §7 no-buffering guarantee is a
lazy-mode property).

**The knob is a HELLO handshake, not a configuration guess.** The sender
sets flag bit 1 (`eager`) as an OFFER (`--eager` CLI flag, default off);
the receiver mirrors the bit in its HELLO iff it implements eager mode.
Eager is active iff both peers set the bit.

**Compatibility (loud-failure doctrine).** A peer that receives a HELLO
with `eager` set must reject it with ERROR(Version) while it does not
implement §9.2 — the two peers would otherwise disagree on DATA timing,
which is a desync hazard. An old receiver never sees early DATA (nothing
is sent at HELLO time), so the fallback to lazy is silent and free; only
the offer itself is rejected loudly. Once both peers implement the mode,
the bit is active.

**Over-send is bounded by position, not by hope.** The sender declares a
chunk missing only when its manifest consumption has advanced past
`chunk.offset + 2×cdc_avg` without seeing the chunk's hash — or the
manifest is done. This is exactly the §6.2 boundary-shift condition;
elsewhere a late manifest match still rescues the chunk. Eager's extra
cost over lazy is therefore bounded by ~2×cdc_avg per chunk inside
modified regions (same constant as the boundary-shift fallback).

**Receiver buffering, bounded.** Pre-plan DATA is buffered by hash (the
sender dedups by hash before offering; duplicate DATA hashes are ignored
as today) under a fixed 64 MiB cap (§12 spirit). Overflow is a loud ERROR
and a lazy retry with a clean temp — never a spill file, never a silent
degradation. (This resolves §13 question 2.)

**Ordering and resume.** Writes happen in arrival order under eager —
D10's locals-first ordering is a lazy-mode property (§7), and resume is
unaffected because the temp re-scan is content-addressed (§10).

**Status.** Implemented (2026-10-24, post-1.0). The bench cell
(`bench --eager --rtt-ms {0,30} --seed 42`) measures paired lazy-vs-eager
sessions over the RTT-modeled duplex; over-send was 0 bytes in every run
(the §6.2 position gate rescues late matches exactly as designed), and
eager cut wall time 18–33% at 0 ms RTT and 12–18% at 30 ms RTT (medians
455→368 ms and 596→488 ms on the 128 MiB modified fixture, 3 paired runs
each). The knob is `--eager` on both peers; a declining peer fails the
session loudly (the sender checks the receiver's HELLO mirror). Wire
contract notes: eager offers are deduplicated by hash (repeated content
chunks are offered once; the receiver's by-hash buffer is a cache that
serves every plan entry with that hash), and the sender's data phase
skips hashes already offered (re-sending would corrupt the frame stream
the receiver reads at VERIFY).

## 10. Resume (D10, decided and implemented)

Resume is **not a separate protocol phase** — it reuses the normal flow with a
different manifest source. Decided on simulation data (see D10 in
`Design Decisions.md`); receiver-authoritative:

1. Receiver persists no manifest file: the **temp file's content is the
   resume state** (locals-first write order per §7 makes it mostly real
   data after a crash).
2. On reconnect, the receiver checks for a temp file. If present (HELLO
   carries `FLAG_RESUMING`): it **re-scans the temp file** and streams the
   **union manifest** — temp chunks first, then old-file chunks the temp
   does not cover. If absent: cold start, manifest of the old file.
3. The sender diffs its source against whatever manifest it receives,
   exactly as in §5–7. PLAN_LOCAL entries carry the hash; the receiver
   resolves each local source per hash (temp before old file) and verifies
   the read against it. The sender needs no state from the previous
   attempt.
4. Verification (§8) is unchanged and is the final oracle: a stale or wrong
   manifest can cost re-sends, never corruption.
5. **Empty plan on resume:** if the previous attempt died after the last
   DATA byte, the reconnect plan is empty but the temp is complete; the
   receiver compares the temp's root against VERIFY and commits it (atomic
   rename) on match.

**Why re-scan the temp instead of journaling written offsets:** the temp
file's content *is* the state, re-scanning reuses one code path, and CDC
re-chunking of correct chunk data reproduces matching hashes except in
boundary-shift regions (same bounded over-send as §6.2). A write-journal is a
later optimization, not v0.1 (§13).

## 11. Progress and observability

No dedicated progress frames. Every phase's progress is derivable from frame
flow, which was a design requirement (silent hashing was the project's
origin, D5):

- Scan: MANIFEST_CHUNK count vs. expected chunk count (receiver knows its file
  size; chunks per byte ≈ 1/avg_size).
- Diff: plan entries received vs. sender's file size covered.
- Transfer: DATA bytes decompressed vs. total PLAN_REMOTE length.
- The CLI SHOULD display: current phase, bytes hashed / total, bytes missing /
  total, effective throughput per phase. A peer that is scan-bound says so:
  "hashing (disk-bound), network idle" is the exact failure mode this tool
  exists to eliminate and must never be silent again.

## 12. Errors and resource limits

- Any ERROR frame: terminal, both sides close. Documented codes in §3.
- Truncated connections: both sides treat as failure; receiver keeps temp +
  manifest (that is the resume point); sender keeps nothing.
- Frames larger than 64 MiB payload: protocol error.
- Receiver MUST enforce: DATA total ≤ PLAN_REMOTE total; temp file never
  exceeds sender's file_size. A sender violating these is malicious or broken
  → ERROR 3.

## 13. Open questions

1. **Index staleness policy** (§14.1): exact trust rules for cross-run
   manifests; relationship to mtime granularity problems.
2. **Eager-mode buffer cap** (§9.2): resolved by design — fixed 64 MiB cap,
   overflow = loud ERROR + lazy retry (§9.2). Implementation of eager mode
   itself remains open, gated on the eager-mode bench cell.
3. **Resume write-journal** vs. temp re-scan: at what file sizes does the
   re-scan cost matter?
4. **zstd batching:** per-chunk frames waste some compression ratio at ~1 MiB
   average chunk size; batching multiple chunks per frame trades ratio against
   pipelining granularity. Measure before deciding.
5. **Plan bandwidth symmetry:** the plan is sender-computed (§6). A
   receiver-derived plan (sender sends only its manifest) would halve control
   traffic but move work and complexity to the receiver. Revisit if plan frames
   ever show up in profiles (they will not for typical chunk sizes).
6. **Parallel data streams** for long fat networks (§14.4): how many, how
   scheduled.

## 14. Extension points — status

1. **Persistent index:** IMPLEMENTED (D15, v0.4 — `FFSYIX02` sidecar
   files; the INDEX-frame idea was dropped in favor of the on-disk
   sidecar, same trust model: size+mtime+CDC-parameter validation).
2. **Eager diff:** DESIGNED, not implemented (§9.2; gated on the
   eager-mode bench cell — D8's doctrine).
3. **TLS:** IMPLEMENTED (D16, v0.5 — rustls TLS 1.3, mutual TOFU
   fingerprint pinning; QUIC followed in v0.6, §17).
4. **Parallel chunk streams:** IMPLEMENTED (v0.7, §16.8 — per-file
   conversations over K TCP connections; on QUIC they ride streams,
   §17).
5. **Hash truncation:** reserved, not implemented. BLAKE3 supports
   shorter outputs; truncating wire hashes saves bandwidth at a
   quantifiable collision cost. Only with explicit analysis; the wire
   always sends 32 bytes.
6. **Tree sync:** implemented in v0.3 — §16 (D17), including the
   parallel per-file streams item above.

## 15. Worked example (3-chunk toy file)

Sender file S = [A][B][C] (3 chunks, hashes hA/hB/hC). Receiver file R = [A][X]
(hA/hX). Both use identical CDC params.

```
S→R: HELLO {role=sender, size=3 chunks, mtime=T1}
R→S: HELLO {role=receiver, size=2 chunks, mtime=T2}
     (fast path not taken: size differs)
R→S: MANIFEST_CHUNK {hA, off 0, len 1}      # R scans, streams
R→S: MANIFEST_CHUNK {hX, off 1, len 1}
R→S: MANIFEST_DONE
S→R: PLAN_LOCAL  {target 0, len 1, src 0}   # hA present in R
S→R: PLAN_REMOTE {target 1, len 1, hB}      # hB missing
S→R: PLAN_REMOTE {target 2, len 1, hC}      # hC missing
S→R: PLAN_DONE
S→R: DATA {hB, zstd(bytes-of-B)}
S→R: DATA {hC, zstd(bytes-of-C)}
S→R: VERIFY {root(S)}
R:   writes B→offset 1, C→offset 2 of temp; local-copies A→offset 0;
     root(temp) == root(S) → fsync, rename, COMMIT_ACK
R→S: COMMIT_ACK
```

Same flow with R = [A][B][C] but mtime differs: plan comes back empty, VERIFY
carries root(S), receiver compares against its own root → match → COMMIT_ACK
with no temp file ever created.

## 16. Tree sync (v0.3, D17)

One connection, many per-file conversations. The per-file protocol (§4–§8) is
unchanged; the tree layer wraps and sequences it.

### 16.1 Session shape

```
S→R: TREE_BEGIN   (tree manifest + tree flags)
R→S: TREE_BEGIN   (receiver's tree manifest, same framing)
      both sides triage (§16.3) — no wire traffic
      for each file needing sync (deterministic order, §16.4):
        S→R: per-file HELLO (path = relative path, §4)
        ... per-file protocol §5–§8 unchanged ...
      (deletions, §16.5)
S→R: TREE_END     (empty payload)
R→S: TREE_END     (deleted_count, deleted_bytes)
```

The sender's TREE_BEGIN is authoritative for direction: the sender's tree is
the desired state, the receiver converges toward it (`rsync -r --delete`
semantics made explicit; `--no-delete` via tree flags, §16.2). Both peers
send their full tree manifest before any per-file conversation starts —
triage is local, deterministic on both sides from the same two manifests.

A v2 peer receiving TREE_BEGIN fails with an unknown-frame ERROR (v2 framing
is strict, §3) — the version check in HELLO makes this a clean rejection
before any tree state exists. Protocol version 3 is only advertised by
peers implementing this section.

### 16.2 Tree manifest and flags

TREE_BEGIN payload: `entry_count u64`, then `entry_count` tree entries:

```
TreeEntry := rel_path (u16 len + UTF-8) | kind u8 (0=file, 1=dir)
           | size u64 | mtime u64 (unix secs, 0 if absent)
           | mtime_nanos u32 (0 if absent)
```

Entries are sorted by `rel_path` (byte order) — deterministic iteration on
both sides without negotiation. `kind=1` (dir) entries carry size=0 and
exist only to create empty directories on the receiver (a directory with
files needs no entry; its files imply the path). Paths are relative to the
synced root, `/`-separated regardless of platform, and must not contain
`..` components (protocol error otherwise — path traversal is a protocol
violation, not a filesystem concern).

Tree flags (in TREE_BEGIN, u16): bit 0 `checksum` (D11 tree-wide: no
metadata fast path on any file), bit 1 `no_delete` (§16.5).

### 16.3 Triage (local, no wire traffic)

Both peers, after exchanging TREE_BEGINs, compute locally:

- **skip**: same rel_path on both sides, both files, same size, same
  mtime secs + nanos (and neither side set `checksum`) — exactly the §4
  fast-path rule, applied per file from metadata alone. Also: same path
  and both dirs.
- **sync**: same path, both files, size or mtime differs (or `checksum`
  set) → per-file conversation (its VERIFY is the medium diff; the
  workflow doc's Phase 2 is absorbed there).
- **transfer**: file exists on sender only → per-file conversation with an
  empty receiver file (the receiver creates it; empty-plan handling of a
  zero-byte sender file truncates a non-empty receiver file to empty —
  fixed in v3, previously an ERROR).
- **delete**: file exists on receiver only → deferred to §16.5.
- **conflict**: file on one side, dir on the other → the dir side is
  removed and the file synced (the sender's tree is authoritative in both
  directions of this asymmetry: sender-file/receiver-dir deletes the dir,
  sender-dir/receiver-file deletes the file, then creates the dir if
  needed). Files inside a conflicted dir are covered by the delete rule.

### 16.4 Execution order

Per-file conversations run in the sender's manifest order (sorted by
rel_path). Skipping happens on both sides independently — the sender
initiates a conversation only for files in its `sync`/`transfer` set; the
receiver expects conversations exactly for the same set (computed from the
same two manifests), so no per-file negotiation is needed. A receiver that
disagrees (it must not, given deterministic triage) fails loudly on the
first unexpected HELLO.

Because the sender's fast-path decision (size+mtime equal, both trust
times) is known *before* it would demand the receiver's manifest, tree
sessions make the branch explicit: after the per-file HELLO exchange, the
sender sends either NOT_MODIFIED (skip; conversation over, no manifest
streamed) or FILE_SYNC (empty; conversation proceeds — the receiver then
streams its manifest as usual). The receiver reads exactly one frame at
that point; no write races, no broken-pipe tolerance needed inside a
session. Single-file sync keeps its original drain-based flow (§4) —
there, connection end *is* the session end.

Within one file's conversation, everything is unchanged from §5–§8 —
including resume (a mid-tree failure leaves that file's temp file; the
next session's triage sees the file still differing and the per-file
resume logic takes over), warm indexes (D15), and the fast path.

**Manifest sources (D22, receiver-side rename detection):** the
conversation manifest's meaning — "I have these chunks" — is
source-agnostic by the D10 union-manifest precedent (temp first, then
old-file). A receiver MAY add further local sources: when a file is
missing locally (a rename moved it elsewhere), the chunks of its
**orphan D15 index** (an index whose target file no longer exists) are
offered as census sources, size-filtered to the conversation file's
HELLO size, at lower precedence than temp/old-file. Every census claim
is hash-verified at read like any other manifest source (D12): a stale
or wrong claim costs an ERROR, never corruption. The census never
touches triage inputs; the delete pass removes the old path and its
consumed index (the existing orphan cleanup). No new wire format, no
flag — the sources are the receiver's local business.

Deletions happen **after** all conversations succeeded. A failed
conversation aborts the session (ERROR, terminal per §3) before any
delete is applied — fail-closed: a failed transfer never destroys data.

**ERROR at any read point (v3):** a peer that aborts mid-session sends
ERROR (code `Aborted`) before dropping the stream, at whatever point it
dies — instead of letting the peer time out. Receivers that expect a
HELLO (or any typed frame) must classify an incoming ERROR as the peer's
abort reason, not as a framing violation. A peer MAY still die without
announcing (process kill, network cut); the socket timeout remains the
last line of defense, which is why it exists and stays generous. Delete
targets destroyed as a side effect of conflict resolution (§16.3) are
tolerated as already-gone (`NotFound`/`NotADirectory`) during the delete
pass.

### 16.5 Deletions

After the last per-file COMMIT_ACK, the sender sends TREE_END (empty).
The receiver deletes every file in its `delete` set (skipped when
`no_delete` is set), then removes now-empty directories bottom-up
(within the synced subtree only), then replies TREE_END with
`deleted_count u32 + deleted_bytes u64` for the summary line. The sender
reads the reply and exits; the receiver exits after sending. The sender's
TREE_END doubles as the "transfers done, please finalize" barrier — the
receiver must not delete anything before it (a sender aborting
mid-session leaves the receiver's tree intact apart from synced files).

Delete safety is bounded by the same trust decision as every write in the
session: the receiver already pinned the sender's fingerprint (D16).

### 16.6 mtime preservation (v3, load-bearing for trees)

On every commit (COMMIT_ACK path), the receiver sets the target file's
mtime to the sender's HELLO `file_mtime`/`file_mtime_nanos` **before**
sending COMMIT_ACK, using `File::set_modified` (std, all platforms). The
sender's HELLO mtime is read before its scan starts (unchanged §4). Both
peers then record the same mtime in their D15 indexes, so the next tree
sync's triage (§16.3) skips the file — the tree fast path fires without
reading a byte. Without this, every synced file's mtime would change at
commit and no tree sync would ever skip.

Single-file sync adopts the same behavior under protocol v3 (observable
change: the receiver's file now carries the sender's mtime after a sync;
this matches rsync's default `--times` behavior and is what users expect
from a sync tool — recorded in D17).

### 16.7 Excludes (v3, `--exclude` / `--exclude-from`)

Excludes remove paths from a tree session entirely: an excluded path is
invisible to triage on both peers — it is never transferred, never
deleted, never reported. This is rsync's model: exclude patterns filter
what the session sees, and (because receiver-only paths that are excluded
never enter the receiver's manifest either) excluded files on the receiver
are protected from deletion.

**Ownership — the sender's patterns rule.** Exclude patterns travel in the
sender's TREE_BEGIN; the receiver applies exactly those to its own scan.
The receiver's own patterns (if any) never touch manifests — they only
protect paths from the receiver's delete pass (§16.5), so both peers
triage identically from the same two filtered manifests (§16.4's
determinism contract is untouched).

**Wire form.** TREE_BEGIN gains flag bit 2 `excludes`. When set, the
payload after `flags u16` carries the pattern section *before*
`entry_count u64`:

```
pattern_count u32
pattern_count × pattern (u16 len + UTF-8)
entry_count u64
entry_count × TreeEntry (§16.2)
```

A peer that did not send `excludes` but receives the flag must fail
(Malformed): the two peers would filter differently, desyncing the
per-file conversation sequence — this is a loud protocol error by design.
Peers that never set the flag parse exactly the §16.2 layout.

**Pattern grammar** (matched against the wire rel path, always
`/`-separated; case-sensitive; no backslash escapes — a `\` in a pattern
is a parse error):

- A pattern containing `/` anywhere except a trailing one is **anchored**:
  it matches from the tree root (`/build` matches only the root entry
  `build`; `doc/*.md` only files directly in `doc/`).
- A pattern without `/` matches the **basename** of any entry at any depth
  (`*.tmp`, `node_modules`).
- `*` matches any run of characters except `/`; `?` matches exactly one
  character except `/`; `[abc]`, `[a-z]`, `[!a-z]` are character classes
  within a segment (an unterminated `[` is literal).
- `**` as a *complete segment* matches any number of segments, including
  zero (`logs/**/*.log` matches `logs/a.log` and `logs/a/b/a.log`;
  `a/**/b` matches `a/b`). `**` inside a segment collapses to `*`.
- A trailing `/` restricts the pattern to **directories** (`build/`).

**Scanning rule (both peers):** an excluded directory is pruned — its
whole subtree never enters the manifest (a negation under a pruned
directory cannot re-include anything — see the grammar note below; to
re-include inside an excluded directory, exclude its children
(`target/*`) instead of the directory itself (`target/`)). An excluded
file is skipped. The scan-sidecar filter (`*.ffsync-index` /
`*.ffsync-tmp`, §16.2) takes precedence and is not user-visible.

**Delete-pass protection (receiver).** A path is skipped by the delete
pass when the path itself matches one of the receiver's patterns, or when
any of its ancestor directories matches one (an excluded directory
protects its whole subtree from deletion). Conflict resolution (§16.3) is
not gated on excludes — the sender's kind change is explicit and
authoritative.

**Negation (v0.7.1, first-match-wins).** A pattern whose first character
is `!` is a **re-include**: it removes paths from the exclusion set
rather than adding them. This upgrades the evaluation model from
any-match to rsync's **ordered first-match-wins**: patterns are checked
in the order given (CLI flag order; `--exclude-from` lines in file
order, at the position of the flag), and the first pattern that matches
a path decides — exclude, or (negation) include. A path matching no
pattern is included. For a set without negations this is exactly the
old any-match behavior (the first matching pattern is an exclude, as
they all are), so existing pattern sets and old peers' expectations are
unchanged. A leading `!` in a literal filename is written `!!foo`
(strip one `!`, the rest is the pattern — there are no backslash
escapes); the class spelling for a literal `!` member is `[]!]`
(POSIX: `]` first in a class is literal).

- **Ordering is the wire's job too:** patterns travel as an ordered
  list; both peers evaluate the same order. The receiver's own patterns
  (delete-pass guard) follow the same rule locally.
- **Delete-pass protection** inherits the semantics through the verdict:
  an ancestor directory protects its subtree from deletion iff its
  first-match verdict is *exclude* — a negated ancestor is not an
  exclusion and therefore protects nothing.
- **Wire form:** TREE_BEGIN gains flag bit 5 `negation`, set by the
  sender iff any of its patterns starts with `!`. No payload change (the
  pattern section layout is unchanged; only the interpretation of the
  strings is flag-gated). No VERSION bump: no existing layout changes
  and the default (no negations, no bit) is byte-identical to before.
  The bit's purpose is compatibility: a v3/v4 peer that receives a
  negation-carrying session rejects the unknown flag bit loudly
  (Malformed) instead of parsing `!foo` as a literal basename pattern —
  the two peers would filter differently and desync the conversation
  sequence, so the loud failure is the correct one. A session without
  negations interoperates with any peer.

### 16.8 Parallel per-file conversations (v4 flag, D13 step 1)

A tree session's conversations are independent of each other: each is a
complete §4–§8 state machine over one path. Sequential execution costs
~3 round trips per conversation (HELLO, manifest→plan, VERIFY→COMMIT_ACK)
— on a WAN, a tree with many changed files spends most of its wall time
waiting. §14.4 reserved parallel streams for exactly this.

**K connections, one conversation at a time each.** The session runs over
K connections total: connection 0 is the *control* connection (the one
TREE_BEGIN/TREE_END and the delete pass live on), connections 1..K−1 are
*worker* connections. Each connection carries at most one conversation at
a time; the per-file frames themselves are byte-identical to §4–§8. The
sender partitions its conversation set round-robin over the sorted list
(conversation *i* → connection *i* mod K — deterministic, reproducible).

**Wire form.** TREE_BEGIN gains flag bit 3 `parallel`. When set, the
payload carries `parallel u16` (K, the total connection count) right
after `flags` and before the optional excludes section:

```
flags u16
parallel u16      (only when flag bit 3 set; 2..=16)
[pattern section] (only when flag bit 2 set)
entry_count u64 ...
```

`parallel` absent (flag unset) means K=1 — the §16.2 layout, unchanged.
**A peer MUST reject unknown flag bits** (Malformed) — flag-gated fields
are only safe if unknown bits are errors. A v3 peer receiving a parallel
TREE_BEGIN misparses and fails loudly at TREE_BEGIN, before any tree
state exists (same rejection semantics as v2↔v3); a v4 peer with
`--parallel 1` interoperates with v3 peers fully. K is
sender-authoritative; the receiver mirrors the value in its own
TREE_BEGIN. Valid range 2..=16 (a session announcing 0, 1, or >16 is a
protocol error).

**New frames (worker connections only).**

```
S→R: TREE_WORKER (worker_index u16, 1..K-1)   on each worker connection
R→S: TREE_WORKER_ACK (empty)
S→R: TREE_FILE (rel path, u16 len + UTF-8)    before each conversation
      ... §4–§8 per-file conversation, unchanged ...
```

The sender opens its worker connections AFTER sending TREE_BEGIN on the
control connection (the receiver accepts them after reading TREE_BEGIN
and scanning its own tree — no deadlock: each side's blocking step is
unblocked by the peer's next step). Workers connect in ARBITRARY order
(the sender's threads race); the receiver accepts any connection, reads
its TREE_WORKER, places it by the ANNOUNCED index, and verifies the set
is exactly {1..K−1} with no duplicates. Each worker is ACKed before the
conversation phase starts.

**Membership, not order (§16.4 amendment for parallel sessions).** In a
parallel session the receiver does NOT expect conversations in manifest
order. Every conversation — on worker connections AND the control
connection — is named by a preceding TREE_FILE; the receiver checks the
path is in its expected conversation set and removes it — every path
exactly once. The control connection's service loop is the receiver's
main thread: it reads TREE_FILEs (serving each) until the sender's
TREE_END arrives THERE — in a parallel session the control connection
carries partition 0's conversations plus that TREE_END, so the §16.5
barrier read is already satisfied when the conversations phase ends (the
receiver then joins its worker threads, requires the membership set to
be empty, and proceeds to deletions). At session end the set must be
empty (a worker that died leaves it non-empty, but then the session has
already failed with that worker's error). Within one connection,
conversations still run in the sender's sorted partition order.

**Failure semantics (fail-closed, unchanged in spirit).** A failed
conversation aborts its connection (ERROR `Aborted`, §16.4). Workers stop
before *starting* the next conversation once any worker has failed
(in-flight conversations finish). The sender then reports ERROR on the
control connection; the receiver applies deletions only after ALL
conversations succeeded — a failed session never destroys data. Tree
flags, excludes, resume, warm indexes, and mtime preservation are
per-conversation concerns and work unchanged on any connection.

**Not async.** This is native threads + blocking I/O per connection - 
no async runtime enters the tree (the D13 amendment's constraint). QUIC
(D13 step 2) can later substitute the transport: a QUIC stream is
`Read + Write`, so worker connections map onto streams without touching
this layer again. That substitution is specified in §17.

### 16.10 Dry-run preview (v0.8 work)

`--dry-run` in tree mode is a TREE_BEGIN flag bit (bit 6,
`TREE_FLAG_DRY_RUN`), mirrored by the receiver like every session flag:
the sender announces it, the receiver mirrors it, and both run the normal
TREE_BEGIN exchange and triage. Then:

- The sender skips the per-file conversations entirely and goes straight
  to its TREE_END barrier; the receiver skips its conversation loop and
  its TREE_END barrier read consumes the sender's frame as usual (§16.8
  membership discipline is untouched).
- The receiver's delete pass runs in count-only mode: excluded and
  protected paths are skipped exactly as in a real session, everything
  else is counted (deepest-first order preserved), and the TREE_END
  delete report carries the WOULD-delete numbers. Nothing is removed,
  no directories are created, no kind-conflict cleanup happens.
- The sender's preview outcome reports the conversation count from its
  own triage (files that WOULD transfer), a sender-side byte estimate,
  and the receiver's would-delete report.

Old peers reject the unknown bit loudly at TREE_BEGIN decode (§16.2
strict flag policy) — the same failure shape as every other flag-gated
feature.

### 16.11 Eager diff in tree sessions (v1.0.x work, D8 extended)

`--eager` in tree mode is a TREE_BEGIN flag bit (bit 7,
`TREE_FLAG_EAGER`), announced by the sender. The receiver mirrors the bit
in its own TREE_BEGIN **iff it also opted in with its own `--eager`** —
unlike the mirrored metadata/negation/dry-run bits, eager is an opt-in,
not a session property: a receiver must never accept pre-plan DATA it
did not agree to buffer. Eager is active for the whole session iff both
peers set the bit; every per-file conversation (sequential §16.4 and
parallel §16.8 workers alike) then runs the single-file §9.2 machinery
inside the normal conversation frame order — the position gate, the
hash-keyed pre-plan DATA buffer with its 64 MiB cap, and the
offer-dedup contract (§9.2) are reused unchanged; no new frame types.

A declining receiver converges lazily (silent fallback, same as the
single-file offer), so mixed-version pairs and opt-out pairs stay
compatible. The flag joins `TREE_FLAG_KNOWN`; old peers reject it loudly
at decode — correct, since a peer that cannot parse bit 7 must not be
asked to interpret conversations that ride it.

### 16.9 File metadata: POSIX mode bits (v4, D9 amendment)

**Scope decision (D9 amendment, 2026-10-23).** Tree sync gains one metadata
axis: the POSIX mode bits (permissions + setuid/setgid/sticky). Xattrs and
hardlinks stay out of scope (xattrs: platform-fragmented semantics for
little sync value here; hardlinks: a linked file would need the same
chunk list under two paths, breaking the one-path-one-chunk-list identity
model that D15 indexes and per-file verification are built on).
Single-file mode is unchanged (metadata rides the tree manifest only —
the asymmetry is documented, and HELLO keeps its layout).

**Wire format (additive, flag-gated — the §16.7/§16.8 pattern).**
TREE_BEGIN flag bit 4 `metadata`: when set, each tree entry in this
TREE_BEGIN carries an additional `mode u32` field:

```
TreeEntry (metadata flag) := ...existing fields... | mode u32
```

`mode u32` carries the low 12 bits of the POSIX mode (rwxrwxrwx +
setuid/setgid/sticky, `0o7777` mask), encoded as the raw st_mode low bits
— i.e. the value `unix::fs::MetadataExt::mode()` masked with `0o7777`,
or 0 where the platform has no such concept. Flag bit 4 joins
`TREE_FLAG_KNOWN`; decoders reject it when unknown (old receivers fail
loudly against new senders — correct, because triage inputs must be
identical on both peers).

**Semantics.**

- **Scan:** `mode` is read at scan time on every platform; on Windows and
  any platform without POSIX modes it is 0.
- **Triage:** mode differences do NOT trigger sync by themselves. Triage
  inputs stay (path, kind, size, mtime) — adding mode to the skip rule
  would make a chmod-only change invisible to the skip decision but
  visible to the metadata pass, splitting the deterministic-triage
  invariant. Instead: mode rides along with every conversation the
  existing triage already starts, and a **chmod-only tree change**
  (same size+mtime everywhere, differing modes) is simply not synced —
  same trade rsync's default fast path makes. The metadata is best-effort
  convergence, not a change detector. (`--checksum` still forces full
  per-file conversations, which then also apply metadata.)
- **Apply:** after a file's reconstruction succeeds and before the mtime
  preservation write (§16.6), the receiver applies the sender's mode:
  on unix, `fchmod`-equivalent via `std::os::unix::fs::PermissionsExt`
  (mode masked `0o7777`); on Windows, the no-write bits
  (`0o222` for owner/group/other) map to clearing the read-only
  attribute — write-enable always wins (an attribute cannot express
  read-denial; documenting the lossy direction is the honest choice).
  Directories: mode applies to created directories at creation time;
  pre-existing directories keep their mode (the receiver's directory is
  not replaced — matching the "dir with files needs no entry" rule).
- **Order is load-bearing:** mode write → mtime write → stat → D15 index
  write. The index must record the post-metadata mtime or every
  metadata-synced file re-syncs (§16.6's ordering rule, extended one
  step earlier).
- **Index (D15): no change.** The index stays (chunk list, root, size,
  mtime): triage never reads mode (chmod-only changes are accepted as
  not-synced — the §16.3 invariant), the sender's manifest mode comes
  from a fresh metadata scan at session start (never from the index —
  the tree scan is metadata-only and always current), and the per-file
  warm path serves chunks, not metadata. Applying mode before mtime
  (above) keeps the index's recorded mtime correct without mode ever
  touching the index. (An earlier draft of this section bumped the index
  codec for a mode field; the implementation review dropped it — nothing
  consumes it.)
- **Determinism:** both peers derive the same triage from the same two
  manifests; the metadata section changes only what rides *with* a
  conversation, never *which* conversations exist. The §16.3 invariant
  is untouched.

**Symlinks (D9 amendment — policy, no wire change).** The §1 behavior is
restated and sharpened: symlinks to files are followed and synced as
their target type; symlinks to directories are followed but **never
recursed into as directories during scan** (the scan reads the link's
own metadata, sees "symlink", and skips it with a warning) — this closes
the junction/junk-symlink recursion hazard on Windows, where a junction
needs no privileges and can point at an ancestor. Dangling symlinks are
skipped with a warning (unchanged). Sync-as-entry (link + target on the
wire) remains out of scope: Windows symlink creation needs privilege or
developer mode, and target validation would add a traversal surface for
a feature ffsync's niche (big-file sync) does not need.

**Version:** protocol v3 → **v4** (this flag-gated field changes the
per-entry layout, so it takes the bump; the earlier additive extensions
— §16.7 excludes, §16.8 parallel, §17 QUIC — shipped without one because
they changed no existing layout. Unknown flag bits were already
rejected, making the migration path the loud-failure one the protocol
prefers).

## 17. QUIC transport (D13 step 2, v0.6 work)

QUIC substitutes the transport underneath the existing session shapes —
it changes **how bytes move**, not **which bytes move**. Every frame,
every handshake, every state machine in §4–§16 is unchanged: a QUIC
stream is an ordered, reliable byte stream, exactly the §2 assumption
framing and TLS-over-TCP already make. This section specifies the
mapping, the security model, and what actually differs.

### 17.1 Relationship to the session shapes

- **Single-file sessions (§4–§8) and sequential tree sessions (§16.1–
  16.6):** one QUIC *connection*, one bidirectional *stream* per
  conversation (single-file: exactly one stream). The stream plays the
  role the TLS-over-TCP stream plays today.
- **Parallel sessions (§16.8):** one QUIC connection; conversation
  *i*'s stream plays the role of connection *i*. K=1 is the sequential
  shape. The TREE_WORKER/TREE_FILE membership protocol is UNCHANGED —
  the sender still announces every conversation, the receiver still
  validates each announced path exactly once; what was "the worker
  connects" becomes "the worker opens a stream." A v4 peer over QUIC
  interoperates with a v4 TCP peer (the wire bytes are identical; only
  the carrier differs), so no VERSION bump and no new flag bits.
- **Streams are cheap** (no handshake, no kernel socket): the §16.8
  cap K=2..=16 remains the wire-protocol cap, but implementations MAY
  run more conversations than they have streams in flight; this spec
  keeps one stream per conversation partition (deterministic, matches
  the TCP shape 1:1).

### 17.2 Security model (D16 mapping)

QUIC in Rust (quinn) mandates TLS 1.3 **inside the transport** — there
is no plaintext QUIC. The D16 model maps directly, replacing the
TLS-over-TCP layer rather than stacking on it:

- **Identity:** the same self-signed certificate (raw-DER
  `identity.bin`, rcgen) is the QUIC endpoint's certificate. One
  identity per peer, same file, same codec.
- **Mutual TOFU pinning:** the QUIC client verifies the server cert's
  SHA-256 fingerprint against the pins set (Pinned or FirstUse), and
  the QUIC *server* requires client certificates and pins them the
  same way. The presented fingerprint is surfaced to the CLI after the
  handshake completes, exactly like `transport::client_session` /
  `server_session` do today — the verifier never makes trust
  decisions by itself.
- **`--plaintext` semantics:** QUIC has no plaintext mode. `--plaintext`
  on a QUIC invocation is an error ("QUIC is always encrypted; use TCP
  for plaintext") — plaintext remains a TCP-only concern (LAN/SSH).
- **Fingerprint format, pins file, `trust`/`untrust`/`fingerprints`
  commands: unchanged.** Pins are a set, not per-transport entries; a
  pinned fingerprint may connect over TCP or QUIC in either role.

### 17.3 Runtime boundary (D3 amendment)

tokio enters the tree at the edges only: quinn is an async library,
so `transport::quic` owns a small multi-thread runtime that drives
endpoint machinery (connection state, retransmit timers, keep-alives).
The public seam stays synchronous — a `QuicStream` adapter implements
`Read + Write + Send` by driving quinn futures to completion
(`block_on` at the adapter boundary), so `protocol::sync` and
`protocol::tree_sync` remain 100% sync, and the data plane (mmap,
chunker/hash worker threads, crossbeam channels, reconstructor)
remains native-thread and runtime-free. `ffsync::protocol` MUST NOT
depend on tokio; only `transport` and the CLI edge do. The §16.8
worker model is unchanged: one native thread per conversation, each
driving its stream through the sync adapter.

### 17.4 What QUIC actually buys (and what it does not)

- **One handshake instead of K.** A parallel session over TCP pays K
  full rustls handshakes (§16.8 measured this as part of the K>8
  flattening); QUIC pays one connection handshake, streams are nearly
  free.
- **Stream-level loss recovery.** A lost packet delays only its own
  stream's data, not every conversation behind a TCP head-of-line
  block. The netem blind spot from `evaluation/parallel.md` (shared
  bottleneck + loss) is *solved by the transport* here.
- **Connection migration.** A peer whose address changes (Wi-Fi →
  hotspot, NAT rebinding) keeps the connection alive — the "friend
  over the internet" case, impossible over TCP. This is the headline
  feature; it requires both peers on QUIC.
- **What it does not buy:** no throughput magic on clean paths (TCP at
  100 Mbit/s+ is rarely the bottleneck for ffsync's chunk sizes), and
  no LAN benefit (RTT is already ~0; §16.8's LAN caveat applies).
  QUIC is a WAN-shape transport; over TCP and QUIC both being
  available, the peers negotiate nothing — the CLI flags decide
  (`--quic` sender-side; a QUIC receiver listens on UDP and speaks
  QUIC to any peer that connects).

### 17.5 Interoperability and rollout

- **Discovery:** a QUIC receiver binds UDP port N and a TCP receiver
  binds TCP port N — the sender's flag picks the transport, there is
  no wire negotiation step (a QUIC sender connecting to a TCP-only
  peer fails at the UDP level, loudly: no QUIC handshake response).
- **v0.4/v0.5 peers:** no interaction — QUIC peers cannot talk to
  TCP-only peers (different transport), but the *protocol version*
  over QUIC is the current one; a future TCP peer of the same version
  interchanges identical bytes.
- **UDP blocked networks:** common in corporate/captive networks. The
  sender reports the handshake timeout with a hint ("QUIC needs UDP;
  try TCP (default) if the path blocks UDP"). ffsync always keeps the
  TCP transport; QUIC is opt-in per invocation, not a default.

### 17.6 Connection teardown (the one real behavioral difference)

TCP's `close()` flushes buffered data before FIN; QUIC's connection
close is **abrupt with respect to buffered stream data**: when a peer
closes the connection (or it is dropped), data accepted by
`SendStream::write` but not yet ACKed by the peer MAY be discarded
(RFC 9000 §10.2.2). Every protocol-level stream end is explicit here
(per-conversation FIN, ERROR frames), so the frame flows themselves
are unaffected — but a final frame that is *answered* (TREE_END
report, COMMIT_ACK) is only safely delivered if the writer holds the
connection until the reader's reply or close is observed. The
implementation therefore:

- keeps the session's `quinn::Connection` handle alive until after the
  last reply is read (senders: TREE_END report; receivers: the
  post-commit drain),
- receivers wait (bounded) for the peer to close before exiting, so a
  sender's final frames are ACKed rather than lost to a fast process
  exit, and
- treats stream FIN (`finish()` + drop, like TCP close) as the
  per-conversation end — the receiver's Eof path needs nothing extra.

