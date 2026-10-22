# Wire Protocol

Specification for the ffsync wire protocol. Written for v0.1 (protocol
version 1: **one file pair, two peers, one TCP connection**); it tracks the
protocol across versions — changes ship with a `VERSION` bump and are
recorded in the relevant sections. Current version: **3** (v0.2: HELLO
gained `file_mtime_nanos`, see §3/§4; v0.3 adds the tree envelope, see
§16). This document is implementation-facing: it should contain everything
needed to build the sync without re-deriving design arguments. The
reasoning lives in `Positioning.md` and `Design Decisions.md` (referenced
as D-numbers).

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

- Metadata beyond (path, size, mtime): no permissions, xattrs, symlinks,
  hardlinks. (v0.3 follows symlinks to their target type; dangling ones are
  skipped with a warning — recorded, not silent.)
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
| 0x01  | HELLO          | both    | magic `FFSY` (4B), version u16 (=3), flags u16 (§4), role u8 (1=sender, 2=receiver), cdc_min u32, cdc_avg u32, cdc_max u32, file_size u64, file_mtime u64 (unix secs, 0 if absent), file_mtime_nanos u32 (sub-second part, 0 if absent; v0.2), path (u16 len + UTF-8) |
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
| 0x0C  | TREE_BEGIN     | send→recv | entry_count u64, then entry_count × tree entry (§16.2); v0.3 |
| 0x0D  | TREE_END       | both    | deleted_count u32 + deleted_bytes u64 (recv→send, after deletes; send→recv variant is empty); v0.3 |
| 0x0E  | FILE_SYNC      | send→recv | empty; tree sessions only: "this file needs a conversation" (§16.4) — its absence is NOT_MODIFIED |

Error codes: 1=version, 2=role conflict, 3=bad frame, 4=hash mismatch,
5=io, 6=aborted. Any ERROR frame is terminal: the sender of ERROR closes the
connection after flushing; the peer must treat the transfer as failed.

## 4. Handshake and negotiation

1. Both peers connect and immediately send HELLO.
2. Validate: magic, version equality (else ERROR 1), role conflict
   (both 1 or both 2 → ERROR 2).
3. CDC parameters: the **sender's** (min, avg, max) are authoritative; the
   receiver SHOULD use the same values when chunking its own file. This is a
   bandwidth optimization, not a correctness requirement — see §8.2.
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

Flags (HELLO): bit 0 `trust_times`, bit 1 `eager` (§9.2, reserved, must be 0
in v0.1), bit 2 `resuming` (informational, §10).

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

### 9.2 Eager (reserved, flag bit 1)

Sender diffs against the *partial* manifest set and streams DATA for hashes
confirmed missing so far. Bytes start moving during the scan. A late manifest
entry may match an already-sent chunk → bounded over-send. Requires the
receiver to buffer DATA by hash until its PLAN_REMOTE arrives (the §7
no-buffering guarantee is a lazy-mode property). Buffer memory cap is an open
question (§13). Do not implement in v0.1.

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
2. **Eager-mode buffer cap** (§9.2): memory bound for by-hash buffering.
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

## 14. Extension points (reserved, not implemented)

1. **Persistent index (v0.2):** peers may answer HELLO with an INDEX frame
   carrying a manifest + root hash from a previous run, valid only if the
   on-disk file's current size+mtime still match what the index recorded
   (cheap stat; same trust level as the §4 fast path). Turns repeated syncs
   into near-zero-scan operations — the core product insight
   (`Positioning.md`).
2. **Eager diff** (§9.2).
3. **TLS** via rustls (decided: D16 — self-signed certs, mutual TOFU
   fingerprint pinning, wraps the stream before frame 1; framing unchanged).
   See also the D13 amendment for the QUIC sequencing.
4. **Parallel chunk streams** (multiple TCP connections or QUIC streams) to
   beat single-flow window limits on high-BDP paths.
5. **Hash truncation:** BLAKE3 supports shorter outputs; truncating wire hashes
   saves bandwidth at a quantifiable collision cost. Only with explicit
   analysis; v0.1 always sends 32 bytes.
6. **Tree sync:** implemented in v0.3 — §16 (D17). Open remains: parallel
   per-file streams for high-BDP paths (needs QUIC or a conversation
   multiplexer; deferred with D13's QUIC answer).

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
