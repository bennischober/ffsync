# Wire Protocol v0.1

Draft specification for the smallest useful ffsync sync: **one file pair,
two peers, one TCP connection**. This document is implementation-facing: it
should contain everything needed to build the first working sync without
re-deriving design arguments. The reasoning lives in `Positioning.md` and
`Design Decisions.md` (referenced as D-numbers).

## 1. Scope and non-goals

**In scope (v0.1):**

- One explicitly named file pair per invocation (`ffsync send <file> <peer>` /
  `ffsync receive <file>` — exact CLI shape open, see §14).
- Roles: the peer with the desired ("source") content is the **sender**, the
  peer wanting to converge is the **receiver**. Roles are assigned by the CLI,
  not negotiated.
- Transport: one ordered, reliable byte stream (TCP). Plaintext for v0.1 —
  intended for LAN use or inside an SSH tunnel. TLS (rustls) is v0.2.
- Compression: zstd on chunk-data frames only.
- Resume after disconnect (receiver-authoritative, D10).
- Progress/observability derived from protocol flow (§11).

**Out of scope (v0.1), per D9:**

- Directory recursion, deletes, renames (tree sync is Phase 1 concept work).
- Metadata beyond (path, size, mtime): no permissions, xattrs, symlinks,
  hardlinks.
- Authentication, encryption, key exchange.
- Daemon/continuous mode.
- Persistent cross-run chunk index — **extension point only** (§14.1). The
  resume manifest (§10) deliberately uses the same format so the index can be
  added without protocol changes.

## 2. Model

### 2.1 Content addressing is the only source of truth

Every chunk is identified by its 32-byte BLAKE3 hash. The file is identified by
its BLAKE3 **root hash** (BLAKE3's Merkle tree yields the root for free while
chunking — see D5). Offsets are *descriptive*, never *authoritative*: two files
with the same root hash are identical regardless of what any metadata claims.

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
| 0x01  | HELLO          | both    | magic `FFSY` (4B), version u16 (=1), flags u16 (§4), role u8 (1=sender, 2=receiver), cdc_min u32, cdc_avg u32, cdc_max u32, file_size u64, file_mtime u64 (unix secs, 0 if absent), path (u16 len + UTF-8) |
| 0x02  | MANIFEST_CHUNK | recv→send | hash 32B + offset u64 + length u32 (44 B) |
| 0x03  | MANIFEST_DONE  | recv→send | empty |
| 0x04  | PLAN_LOCAL     | send→recv | target_offset u64 + length u32 + source_offset u64 (20 B) |
| 0x05  | PLAN_REMOTE    | send→recv | target_offset u64 + length u32 + hash 32B (44 B) |
| 0x06  | PLAN_DONE      | send→recv | empty |
| 0x07  | DATA           | send→recv | hash 32B + zstd-compressed chunk bytes |
| 0x08  | VERIFY         | send→recv | root_hash 32B |
| 0x09  | COMMIT_ACK     | recv→send | empty |
| 0x0A  | NOT_MODIFIED   | both    | empty |
| 0x0B  | ERROR          | both    | code u16 + message (u16 len + UTF-8) |

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
   `file_size` and `file_mtime` are equal on both sides, the sender sends
   NOT_MODIFIED and closes. Both peers are done without reading a single file
   byte. This is rsync's default semantics; it trusts mtime+size and can be
   wrong in the same ways rsync can (2-second FAT mtime granularity, network
   filesystem clocks). `--checksum` mode clears this flag and forces full
   verification.

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
The receiver's manifest entry is used verbatim as the local source range — the
receiver never sub-slices a manifest entry, because content inside a chunk
cannot be verified without re-hashing it.

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

**Already-in-sync edge case:** if the sender's plan is empty (every chunk
matched the manifest) the sender still sends VERIFY. The receiver then hashes
its *current* file (it already has the root from its own scan — free) and
compares: match → NOT_MODIFIED-style done via COMMIT_ACK without ever creating
a temp file; mismatch → the receiver's file content does not match what its
manifest claimed (corrupted index/trust assumption) → ERROR 4. This makes
"files identical despite metadata mismatch" discoverable without any transfer,
and it is the hook the future index plugs into (§14.1).

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

## 10. Resume (D10, receiver-authoritative)

Resume is **not a separate protocol phase** — it reuses the normal flow with a
different manifest source:

1. Receiver persists its manifest (same 44-byte entry format) to
   `<target>.ffsync-tmp.ffsync-manifest` as it scans, and keeps it after a
   disconnect.
2. On reconnect, the receiver checks for a temp file + manifest. If present:
   it **re-scans the temp file** and sends *that* manifest (offsets are
   temp-relative), with `flags.resuming` set in HELLO. If absent: cold start,
   manifest of the current file.
3. The sender diffs its source against whatever manifest it receives, exactly
   as in §5–7. PLAN_LOCAL offsets then point into the temp file; the receiver
   reads local sources from temp (and may consult its old file as well — its
   choice, invisible to the sender).
4. Verification (§8) is unchanged and is the final oracle: a stale or wrong
   manifest can cost re-sends, never corruption.

**Why re-scan the temp file instead of journaling written offsets:** the temp
file's content *is* the state, re-scanning reuses one code path, and CDC
re-chunking of correct chunk data reproduces matching hashes except in
boundary-shift regions (same bounded over-send as §6.2). A write-journal is a
later optimization, not v0.1 (§13).

Receiver-authoritative means: after a disconnect, the sender keeps no state and
must re-scan its own file. That is the accepted cost (D10); the future index
(§14.1) removes it.

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
3. **TLS** via rustls; the framing is TLS-agnostic.
4. **Parallel chunk streams** (multiple TCP connections or QUIC streams) to
   beat single-flow window limits on high-BDP paths.
5. **Hash truncation:** BLAKE3 supports shorter outputs; truncating wire hashes
   saves bandwidth at a quantifiable collision cost. Only with explicit
   analysis; v0.1 always sends 32 bytes.
6. **Tree sync:** Phase 1 metadata reconciliation from
   `File Synchronisation Workflow.md` layered above per-file syncs.

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
